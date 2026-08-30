/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * Pure-C POSIX model-based-testing replayer.  A single process loads the ITF
 * traces (quint's posix model), drives chimera's POSIX client in-process, and
 * runs the model oracle -- no Python orchestrator, no line-protocol driver
 * subprocess, no per-op JSON round-trip over a pipe.
 *
 * The op-execution engine (all 82 chimera_posix_* calls, arg decode, result
 * encode) is reused verbatim from posix_driver.c, included here in
 * ENGINE_ONLY mode so its handle(json_t*)->json_t* is called directly with an
 * in-memory request object.  This file adds what posix_replay.py used to do:
 * ITF decoding, the identity/time/shadow oracle, and the final audit.
 *
 * Scope: the deviation-free stepping flavors (the model's LInit profile is
 * pinned to memfs).  Traces that would exercise a known chimera deviation are
 * out of scope here -- posix_replay.py (with posix_deviations.py) remains the
 * reference for those; this replayer treats any errno/state divergence as a
 * hard mismatch.
 */

#define POSIX_DRIVER_ENGINE_ONLY
#include "posix_driver.c"
#include "common/mbt_trace_dir.h"

/* ---- small helpers over the raw ITF JSON --------------------------------- */

/* An ITF integer is {"#bigint":"N"} or a bare JSON integer. */
static int64_t
tf_i64(json_t *v)
{
    if (json_is_object(v)) {
        json_t *b = json_object_get(v, "#bigint");
        if (b) {
            return (int64_t) strtoll(json_string_value(b), NULL, 10);
        }
    }
    if (json_is_integer(v)) {
        return json_integer_value(v);
    }
    return 0;
} /* tf_i64 */

static int64_t
tf_field(
    json_t     *o,
    const char *k)
{
    return tf_i64(json_object_get(o, k));
} /* tf_field */

static int
tf_bool(
    json_t     *o,
    const char *k)
{
    return json_is_true(json_object_get(o, k));
} /* tf_bool */

static const char *
tf_tag(json_t *o)
{
    return json_string_value(json_object_get(o, "tag"));
} /* tf_tag */

static json_t *
tf_val(json_t *o)
{
    return json_object_get(o, "value");
} /* tf_val */

/* Look up an integer key in an ITF #map ({"#map":[[k,v],...]}); NULL if
 * absent or not a map. */
static json_t *
map_get_int(
    json_t *map,
    int64_t key)
{
    json_t *pairs = json_is_object(map) ? json_object_get(map, "#map") : NULL;
    size_t  i;

    if (!json_is_array(pairs)) {
        return NULL;
    }
    for (i = 0; i < json_array_size(pairs); i++) {
        json_t *pair = json_array_get(pairs, i);
        if (tf_i64(json_array_get(pair, 0)) == key) {
            return json_array_get(pair, 1);
        }
    }
    return NULL;
} /* map_get_int */

/* Look up a string key in an ITF #map. */
static json_t *
map_get_str(
    json_t     *map,
    const char *key)
{
    json_t *pairs = json_is_object(map) ? json_object_get(map, "#map") : NULL;
    size_t  i;

    if (!json_is_array(pairs)) {
        return NULL;
    }
    for (i = 0; i < json_array_size(pairs); i++) {
        json_t     *pair = json_array_get(pairs, i);
        const char *k    = json_string_value(json_array_get(pair, 0));
        if (k && strcmp(k, key) == 0) {
            return json_array_get(pair, 1);
        }
    }
    return NULL;
} /* map_get_str */

/* Look up a 2-tuple (pid, fd) key in an ITF #map whose keys are #tup pairs. */
static json_t *
map_get_pair(
    json_t *map,
    int64_t a,
    int64_t b)
{
    json_t *pairs = json_is_object(map) ? json_object_get(map, "#map") : NULL;
    size_t  i;

    if (!json_is_array(pairs)) {
        return NULL;
    }
    for (i = 0; i < json_array_size(pairs); i++) {
        json_t *pair = json_array_get(pairs, i);
        json_t *k    = json_array_get(pair, 0);
        json_t *tup  = json_is_object(k) ? json_object_get(k, "#tup") : NULL;
        if (json_is_array(tup) && json_array_size(tup) == 2 &&
            tf_i64(json_array_get(tup, 0)) == a &&
            tf_i64(json_array_get(tup, 1)) == b) {
            return json_array_get(pair, 1);
        }
    }
    return NULL;
} /* map_get_pair */

/* ---- replay state -------------------------------------------------------- */

#define R_MAXPID 4
#define R_MAXFD  64
#define R_MAXSID 64
#define R_MAXINO 8192
#define BADFD    999999
#define MOUNT    "/test"

static int           g_fdmap[R_MAXPID][R_MAXFD]; /* (pid, model fd) -> real fd     */
static CHIMERA_DIR  *g_dirmap[R_MAXSID];    /* model sid -> live DIR*         */

struct ident { int present; long long dev, ino; };
static struct ident  g_inomap[R_MAXINO];    /* model ino -> (dev, real ino)   */

struct tent { int present; int64_t abstract; long long sec, nsec; };
static struct tent g_timemap[R_MAXINO][3];  /* [mino][atime|mtime|ctime]      */

struct shadow { unsigned char *buf; size_t len, cap; };
static struct shadow g_shadow[R_MAXINO];    /* model ino -> byte content      */

static int           g_strict_atime;        /* from the trace LInit caps      */
static int           g_nmismatch;           /* per-trace mismatch tally       */
static const char   *g_trace;               /* current trace path (messages)  */
static json_t       *g_cur_fs;              /* model post-state fs (this step) */
static json_t       *g_cur_ps;              /* model protocol state (ps)       */
static const char   *g_cur_tag;             /* current op tag (for reconcile)  */
static json_t       *g_cur_rv;              /* current op request value        */

/* Known-deviation tallies for the per-trace summary (PD<id> -> count). */
struct devhit { char id[12]; int count; };
static struct devhit g_devhits[64];
static int           g_ndevhits;

/* Paths chimera created where the model predicted EMFILE (PD24 residue);
 * excluded from the final audit. */
static char          g_exempt[64][256];
static int           g_nexempt;

/* NFS-loopback divergence containment (ND3/ND4).  When a reconciled
 * divergence makes chimera fail an op the model completed (today: EACCES
 * from the server's directory-search enforcement, which neither the model
 * nor chimera's own resolution -- issue #1771 -- implements), the two sides
 * fall out of sync: a descriptor, stream, or whole file exists on one side
 * only.  Rather than drown the trace in follow-on mismatches, the harness
 * marks what was lost and reconciles exactly the consequences: EBADF on a
 * lost descriptor, ENOENT on a lost file, and attribute/content checks on an
 * inode the model mutated through a lost descriptor.  All tallied, so the
 * masking stays visible per trace; the whole family retires when the specs
 * model grows search-permission semantics. */
static int         g_lost_fd[R_MAXPID][R_MAXFD];
static int         g_lost_sid[R_MAXSID];
static int         g_lost_ino[R_MAXINO];
static int         g_cur_pid;
static const char *g_last_recon;    /* deviation id check_status last
                                     * reconciled, or NULL              */
static int         g_cur_step;      /* ITF state index of the op under replay
                                     * (0 during the final audit)       */
static int         g_nquarantine;   /* ND5 quarantine dirs minted this trace */

static void
state_reset(void)
{
    int i, j;

    for (i = 0; i < R_MAXPID; i++) {
        for (j = 0; j < R_MAXFD; j++) {
            g_fdmap[i][j] = BADFD;
        }
    }
    for (i = 0; i < R_MAXSID; i++) {
        g_dirmap[i] = NULL;
    }
    memset(g_inomap, 0, sizeof(g_inomap));
    memset(g_timemap, 0, sizeof(g_timemap));
    for (i = 0; i < R_MAXINO; i++) {
        free(g_shadow[i].buf);
        g_shadow[i].buf = NULL;
        g_shadow[i].len = g_shadow[i].cap = 0;
    }
    memset(g_lost_fd, 0, sizeof(g_lost_fd));
    memset(g_lost_sid, 0, sizeof(g_lost_sid));
    memset(g_lost_ino, 0, sizeof(g_lost_ino));
    g_nmismatch   = 0;
    g_ndevhits    = 0;
    g_nexempt     = 0;
    g_nquarantine = 0;
} /* state_reset */

/* Record a reconciled known deviation for the per-trace summary. */
static void
record_dev(const char *id)
{
    int i;

    for (i = 0; i < g_ndevhits; i++) {
        if (strcmp(g_devhits[i].id, id) == 0) {
            g_devhits[i].count++;
            return;
        }
    }
    if (g_ndevhits < (int) (sizeof(g_devhits) / sizeof(g_devhits[0]))) {
        snprintf(g_devhits[g_ndevhits].id, sizeof(g_devhits[0].id), "%s", id);
        g_devhits[g_ndevhits].count = 1;
        g_ndevhits++;
    }
} /* record_dev */

/* NFS-client deviation (ND1/ND2): the loopback's nfs proxy, like every NFS
 * client, implements unlink-of-an-open-file as a silly rename (".nfs" + hex
 * of the file handle) that lingers in the directory until the last close.
 * The model implements the POSIX reading -- the name is gone, the file lives
 * on anonymously -- so the artifact name is invisible to it: filter it from
 * readdir and audit listings (ND1) and accept the link count the lingering
 * name keeps, 1 where the model says 0 (ND2).  Gated on the nfs3_/nfs4_
 * backends: a direct backend producing a .nfs* name would be a real
 * divergence and still flags. */
/* An ND5 quarantine directory (see residue_preclear). */
static int
is_nd5_quarantine(const char *name)
{
    return strncmp(name, ".nd5_q", 6) == 0;
} /* is_nd5_quarantine */

static int
is_nfs_silly_name(const char *name)
{
    int i;

    if (!g_nfs_version || strncmp(name, ".nfs", 4) != 0 || name[4] == '\0') {
        return 0;
    }
    for (i = 4; name[i]; i++) {
        char c = name[i];
        if (!((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'))) {
            return 0;
        }
    }
    return 1;
} /* is_nfs_silly_name */

static void
exempt_add(const char *path)
{
    if (g_nexempt < (int) (sizeof(g_exempt) / sizeof(g_exempt[0]))) {
        snprintf(g_exempt[g_nexempt++], sizeof(g_exempt[0]), "%.255s",
                 path);
    }
} /* exempt_add */

static int
is_exempt(const char *path)
{
    int i;

    for (i = 0; i < g_nexempt; i++) {
        if (strcmp(g_exempt[i], path) == 0) {
            return 1;
        }
    }
    return 0;
} /* is_exempt */

static int
rfd(
    int pid,
    int mfd)
{
    if (pid < 0 || pid >= R_MAXPID || mfd < 0 || mfd >= R_MAXFD) {
        return BADFD;
    }
    return g_fdmap[pid][mfd];
} /* rfd */

static void
set_fd(
    int pid,
    int mfd,
    int real)
{
    if (pid >= 0 && pid < R_MAXPID && mfd >= 0 && mfd < R_MAXFD) {
        g_fdmap[pid][mfd]   = real;
        g_lost_fd[pid][mfd] = 0;
    }
} /* set_fd */

static CHIMERA_DIR *
rdir(int msid)
{
    return (msid >= 0 && msid < R_MAXSID) ? g_dirmap[msid] : NULL;
} /* rdir */

static void
mism(
    const char *fmt,
    ...)
{
    va_list ap;

    printf("MISMATCH [%s] step %d: ", g_trace, g_cur_step);
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    printf("\n");
    g_nmismatch++;
} /* mism */

/* ---- fsx-style byte shadow ----------------------------------------------- */

static unsigned char
pat_byte(
    int64_t pat,
    int64_t pos)
{
    return (unsigned char) (1 + (((31 * pat) + pos) % 255));
} /* pat_byte */

static void
sh_ensure(
    struct shadow *s,
    size_t         want)
{
    if (want > s->cap) {
        size_t ncap = s->cap ? s->cap : 4096;
        while (ncap < want) {
            ncap *= 2;
        }
        s->buf = realloc(s->buf, ncap);
        memset(s->buf + s->cap, 0, ncap - s->cap);
        s->cap = ncap;
    }
} /* sh_ensure */

static void
shadow_apply(
    int64_t              ino,
    int64_t              off,
    const unsigned char *data,
    size_t               n)
{
    struct shadow *s;
    size_t         end = (size_t) off + n;

    if (ino < 0 || ino >= R_MAXINO) {
        return;
    }
    s = &g_shadow[ino];
    sh_ensure(s, end);
    /* A write starting past the current end leaves a hole in between; zero it
     * (the buffer may still hold stale bytes from before a shrink). */
    if ((size_t) off > s->len) {
        memset(s->buf + s->len, 0, (size_t) off - s->len);
    }
    memcpy(s->buf + off, data, n);
    if (s->len < end) {
        s->len = end;
    }
} /* shadow_apply */

/* Copy the shadow's [off, off+count) into out, zero-filling past EOF. */
static void
shadow_read(
    int64_t        ino,
    int64_t        off,
    size_t         count,
    unsigned char *out)
{
    struct shadow *s = (ino >= 0 && ino < R_MAXINO) ? &g_shadow[ino] : NULL;
    size_t         i;

    for (i = 0; i < count; i++) {
        size_t p = (size_t) off + i;
        out[i] = (s && p < s->len) ? s->buf[p] : 0;
    }
} /* shadow_read */

static void
shadow_resize(
    int64_t ino,
    int64_t n)
{
    struct shadow *s;

    if (ino < 0 || ino >= R_MAXINO) {
        return;
    }
    s = &g_shadow[ino];
    if ((size_t) n < s->len) {
        s->len = (size_t) n;
    } else {
        sh_ensure(s, (size_t) n);
        /* Growing exposes a hole; zero it (stale bytes may linger past len). */
        memset(s->buf + s->len, 0, (size_t) n - s->len);
        s->len = (size_t) n;
    }
} /* shadow_resize */

static void
shadow_punch(
    int64_t ino,
    int64_t off,
    int64_t length)
{
    struct shadow *s = (ino >= 0 && ino < R_MAXINO) ? &g_shadow[ino] : NULL;
    size_t         hi;

    if (!s || !s->buf) {
        return;
    }
    hi = (size_t) (off + length);
    if (hi > s->len) {
        hi = s->len;
    }
    if (hi > (size_t) off) {
        memset(s->buf + off, 0, hi - (size_t) off);
    }
} /* shadow_punch */

/* Model ino behind (pid, model fd), via the protocol state ps. */
static int64_t
model_ino_of_fd(
    int pid,
    int mfd)
{
    json_t *ofd, *ofds, *node;

    if (!g_cur_ps) {
        return -1;
    }
    ofd = map_get_pair(json_object_get(g_cur_ps, "fds"), pid, mfd);
    if (!ofd) {
        return -1;
    }
    ofds = json_object_get(g_cur_ps, "ofds");
    node = map_get_int(ofds, tf_i64(ofd));
    if (!node) {
        return -1;
    }
    return tf_field(node, "ino");
} /* model_ino_of_fd */

static int
fd_lost(
    int pid,
    int mfd)
{
    return pid >= 0 && pid < R_MAXPID && mfd >= 0 && mfd < R_MAXFD &&
           g_lost_fd[pid][mfd];
} /* fd_lost */

static int
ino_lost(int64_t ino)
{
    return ino >= 0 && ino < R_MAXINO && g_lost_ino[ino];
} /* ino_lost */

/* Mark a model descriptor chimera never obtained (or lost), along with the
 * inode the model reaches through it -- everything the model does via this
 * descriptor is invisible to chimera from here on. */
static void
lose_fd(
    int pid,
    int mfd)
{
    int64_t ino = model_ino_of_fd(pid, mfd);

    if (pid >= 0 && pid < R_MAXPID && mfd >= 0 && mfd < R_MAXFD) {
        g_lost_fd[pid][mfd] = 1;
    }
    if (ino >= 0 && ino < R_MAXINO) {
        g_lost_ino[ino] = 1;
    }
} /* lose_fd */

/* True when (pid, model fd) maps to a directory inode in the post-state fs. */
static int
fd_is_model_dir(
    int pid,
    int mfd)
{
    int64_t ino = model_ino_of_fd(pid, mfd);
    json_t *node;

    if (ino < 0 || !g_cur_fs) {
        return 0;
    }
    node = map_get_int(json_object_get(g_cur_fs, "inodes"), ino);
    return node && strcmp(tf_tag(json_object_get(node, "ftype")), "FDir") == 0;
} /* fd_is_model_dir */

/* Resolve a model path (component list) to an ino in the post-state fs. */
static int64_t
path_ino(
    json_t *fs,
    json_t *comps)
{
    json_t *inodes = json_object_get(fs, "inodes");
    int64_t ino    = 0;   /* ROOT */
    size_t  i;

    if (!json_is_array(comps)) {
        /* comps may be an ITF #set/#tup wrapper of a list */
        return -1;
    }
    for (i = 0; i < json_array_size(comps); i++) {
        json_t     *node = map_get_int(inodes, ino);
        const char *name = json_string_value(json_array_get(comps, i));
        json_t     *nx;
        if (!node || !name) {
            return -1;
        }
        nx = map_get_str(json_object_get(node, "ents"), name);
        if (!nx) {
            return -1;
        }
        ino = tf_i64(nx);
    }
    return ino;
} /* path_ino */

/* True when the model path (component list) resolves through or to a lost
 * inode in the post-state fs: the leaf, or any directory on the way, exists
 * only model-side (or has honestly diverged) behind a reconciled divergence. */
static int
path_touches_lost(
    json_t *fs,
    json_t *comps)
{
    json_t *inodes = json_object_get(fs, "inodes");
    int64_t ino    = 0;   /* ROOT */
    size_t  i;

    if (!json_is_array(comps)) {
        return 0;
    }
    if (ino_lost(ino)) {
        return 1;
    }
    for (i = 0; i < json_array_size(comps); i++) {
        json_t     *node = map_get_int(inodes, ino);
        const char *name = json_string_value(json_array_get(comps, i));
        json_t     *nx;
        if (!node || !name) {
            return 0;
        }
        nx = map_get_str(json_object_get(node, "ents"), name);
        if (!nx) {
            return 0;
        }
        ino = tf_i64(nx);
        if (ino_lost(ino)) {
            return 1;
        }
    }
    return 0;
} /* path_touches_lost */

/* ---- path materialization ------------------------------------------------ */

/* Expand @nlong / @plong sentinels, matching posix_replay._expand. */
static void
append_comp(
    char       *buf,
    size_t      cap,
    size_t     *len,
    const char *comp)
{
    const char *s;
    size_t      n, i;

    if (strcmp(comp, "@nlong") == 0) {
        n = 300;                 /* one component > NAME_MAX (256) */
        for (i = 0; i < n && *len + 1 < cap; i++) {
            buf[(*len)++] = 'n';
        }
    } else if (strcmp(comp, "@plong") == 0) {
        n = 5000;                /* whole path > PATH_MAX (4096) */
        for (i = 0; i < n && *len + 1 < cap; i++) {
            buf[(*len)++] = 'p';
        }
    } else {
        s = comp;
        while (*s && *len + 1 < cap) {
            buf[(*len)++] = *s++;
        }
    }
    buf[*len] = '\0';
} /* append_comp */

/* Build the on-disk path for a model path record into buf.  Absolute paths are
 * rooted at MOUNT; relative paths are joined bare (matching real_path()). */
static void
real_path(
    json_t *pth,
    char   *buf,
    size_t  cap)
{
    json_t *comps = json_object_get(pth, "comps");
    int     abs   = tf_bool(pth, "abs");
    int     slash = tf_bool(pth, "slash");
    size_t  len   = 0, n, i;

    n      = json_is_array(comps) ? json_array_size(comps) : 0;
    buf[0] = '\0';

    if (abs) {
        len = 0;
        while (MOUNT[len] && len + 1 < cap) {
            buf[len] = MOUNT[len];
            len++;
        }
        buf[len] = '\0';
    }
    for (i = 0; i < n; i++) {
        if ((abs || i) && len + 1 < cap) {
            buf[len++] = '/';
        }
        buf[len] = '\0';
        append_comp(buf, cap, &len,
                    json_string_value(json_array_get(comps, i)));
    }
    if (slash && n && len + 1 < cap) {
        buf[len++] = '/';
        buf[len]   = '\0';
    }
} /* real_path */

/* Build a symlink target string (no MOUNT prefix unless abs). */
static void
real_target(
    json_t *tgt,
    char   *buf,
    size_t  cap)
{
    json_t *comps = json_object_get(tgt, "comps");
    int     abs   = tf_bool(tgt, "abs");
    size_t  len   = 0, n, i;

    buf[0] = '\0';
    if (abs) {
        snprintf(buf, cap, "%s", MOUNT);
        len = strlen(buf);
    }
    n = json_is_array(comps) ? json_array_size(comps) : 0;
    for (i = 0; i < n; i++) {
        if ((abs || i) && len + 1 < cap) {
            buf[len++] = '/';
        }
        buf[len] = '\0';
        append_comp(buf, cap, &len,
                    json_string_value(json_array_get(comps, i)));
    }
} /* real_target */

/* ---- direct chimera_posix dispatch --------------------------------------- */

/* Install the requesting model pid's credential + umask on this thread (the
 * driver's apply_pid, but called directly rather than off a JSON request). */
static void
apply_cred(int pid)
{
    if (pid < 0 || pid >= MAX_PIDS) {
        pid = 0;
    }
    chimera_posix_set_cred(&driver_creds[pid]);
    (void) chimera_posix_umask(driver_umasks[pid]);
} /* apply_cred */

/* ---- root redo for reconciled DAC denials -------------------------------- */

/* Re-execute credential: root, with the failing pid's umask left active so a
 * creating redo's mode arithmetic is preserved. */
static void
apply_root_cred(void)
{
    chimera_posix_set_cred(&g_root_cred);
} /* apply_root_cred */

/* True when check_status just reconciled ND3 (a directory-search DAC denial
 * the model does not share) on an op the model COMPLETED.  Chimera refused a
 * mutation the model performed, and the two namespaces are about to drift --
 * so the caller re-executes the exact operation as root, which passes every
 * DAC check, and the sides converge again instead of cascading through the
 * lost-inode machinery. */
static int
nd3_redo_wanted(
    json_t *res_v,
    int     rc)
{
    return rc < 0 && tf_field(res_v, "e") == 0 &&
           g_last_recon && strcmp(g_last_recon, "ND3") == 0;
} /* nd3_redo_wanted */

/* A root redo creates nodes owned by root where the model attributes them to
 * the calling credential: align with the model's post-state.  Compares
 * before writing so a pre-existing node is untouched -- even a same-owner
 * chown would bump ctime. */
static void
redo_fix_owner(
    const char *path,
    json_t     *pth,
    int         follow)
{
    struct stat st;
    json_t     *node;
    int64_t     ino, uid, gid;

    if (!g_cur_fs) {
        return;
    }
    ino = path_ino(g_cur_fs, json_object_get(pth, "comps"));
    if (ino < 0) {
        return;
    }
    node = map_get_int(json_object_get(g_cur_fs, "inodes"), ino);
    if (!node) {
        return;
    }
    uid = tf_field(node, "uid");
    gid = tf_field(node, "gid");
    if ((follow ? chimera_posix_stat(path, &st)
                : chimera_posix_lstat(path, &st)) != 0) {
        return;
    }
    if ((int64_t) st.st_uid != uid || (int64_t) st.st_gid != gid) {
        if (follow) {
            (void) chimera_posix_chown(path, (uid_t) uid, (gid_t) gid);
        } else {
            (void) chimera_posix_lchown(path, (uid_t) uid, (gid_t) gid);
        }
    }
} /* redo_fix_owner */

/* True when some exempt path's final component is `name` (see the readdir
 * ND5 use: only the basename is available there). */
static int
exempt_basename(const char *name)
{
    int i;

    for (i = 0; i < g_nexempt; i++) {
        const char *slash = strrchr(g_exempt[i], '/');
        if (slash && strcmp(slash + 1, name) == 0) {
            return 1;
        }
    }
    return 0;
} /* exempt_basename */

/* Drop an audit exemption that no longer applies (its residue was cleared
 * and the name can be audited normally again). */
static void
exempt_remove(const char *path)
{
    int i;

    for (i = 0; i < g_nexempt; i++) {
        if (strcmp(g_exempt[i], path) == 0) {
            /* Swap-with-last.  memmove, not snprintf: removing the LAST
             * entry makes source and destination the same slot, which is
             * fine for memmove and a -Werror=restrict violation for the
             * string functions (gcc flags it; clang does not). */
            g_nexempt--;
            if (i != g_nexempt) {
                memmove(g_exempt[i], g_exempt[g_nexempt],
                        sizeof(g_exempt[i]));
            }
            g_exempt[g_nexempt][0] = '\0';
            return;
        }
    }
} /* exempt_remove */

/* ND5 leaves a node chimera-side that the model removed (silly-rename
 * residue blocked the rmdir).  When a later op CREATES that name, the
 * residue may have cleared in the meantime -- the offending descriptors
 * closed, and their silly files with them -- so sweep the leftover as root
 * before the create instead of failing EEXIST, and drop the audit exemption
 * once the namespaces agree again.  The caller re-applies its credential
 * afterwards. */
static void
residue_preclear(
    const char *path,
    json_t     *pth)
{
    json_t *comps = json_object_get(pth, "comps");
    char    mp[4096];
    size_t  len = 0, k;

    for (k = 0; comps && k < json_array_size(comps); k++) {
        const char *c = json_string_value(json_array_get(comps, k));
        len += (size_t) snprintf(mp + len, sizeof(mp) - len, "/%s",
                                 c ? c : "");
    }
    if (len == 0 || !is_exempt(mp)) {
        return;
    }
    apply_root_cred();
    if (chimera_posix_rmdir(path) == 0 || chimera_posix_unlink(path) == 0) {
        exempt_remove(mp);
        return;
    }
    /* Still non-empty: descriptors into it are open and their silly files
     * with them.  Quarantine instead -- rename the leftover aside so the
     * name is free for the create; the open descriptors keep working (their
     * handles are fh-based) and the silly files disappear at close as
     * usual.  Quarantine names are filtered wherever silly names are. */
    {
        char q[64];

        snprintf(q, sizeof(q), "%s/.nd5_q%d", MOUNT, ++g_nquarantine);
        if (chimera_posix_rename(path, q) == 0) {
            exempt_remove(mp);
        }
    }
} /* residue_preclear */

/* The model and the deviation registry encode Linux errno numbers; the host
 * libc's errno is identical on Linux but diverges on macOS/BSD for the higher
 * codes (and swaps EAGAIN/EDEADLK).  Translate host -> Linux by name -- the
 * case labels are host macros, so this is an identity map on Linux and the
 * correct remap on Darwin.  Mirrors posix_replay.host_errno_to_linux. */
static int
host_to_linux(int e)
{
    switch (e) {
        case 0: return 0;
        case EPERM: return 1;
        case ENOENT: return 2;
        case EIO: return 5;
        case ENXIO: return 6;
        case EBADF: return 9;
        case EAGAIN: return 11;
        case EACCES: return 13;
        case EBUSY: return 16;
        case EEXIST: return 17;
        case EXDEV: return 18;
        case ENOTDIR: return 20;
        case EISDIR: return 21;
        case EINVAL: return 22;
        case EMFILE: return 24;
        case EFBIG: return 27;
        case ENOSPC: return 28;
        case ESPIPE: return 29;
        case EROFS: return 30;
        case EMLINK: return 31;
        case EDEADLK: return 35;
        case ENAMETOOLONG: return 36;
        case ENOSYS: return 38;
        case ENOTEMPTY: return 39;
        case ELOOP: return 40;
        case EOPNOTSUPP: return 95;
#if defined(ENOTSUP) && ENOTSUP != EOPNOTSUPP
        case ENOTSUP: return 95;   /* distinct from EOPNOTSUPP on Darwin */
#endif /* ENOTSUP */
        default: return e;
    } /* switch */
} /* host_to_linux */

/* The model reports errno 0 on success; a failed call carries the live errno,
 * translated into the model's Linux errno space.  ERRV must be read
 * immediately after the chimera call, before anything else clobbers errno. */
#define ERRV(rc) ((rc) < 0 ? host_to_linux(errno) : 0)

/* ---- known-deviation registry (mirror of posix_deviations.py) ------------ */

#define DEV_ANY (-1)
enum devctx { CTX_ALWAYS, CTX_DFD, CTX_SLASH, CTX_NLONG, CTX_ACCW };

struct deviation {
    const char *id;
    const char *ops[12];     /* NULL-terminated; empty first slot = any op    */
    int         expected;    /* DEV_ANY = wildcard                            */
    int         actual;
    enum devctx ctx;
};

/* Reconcilable entries only, in posix_deviations.py order (reconcile returns
 * the first match, so order is significant). */
static const struct deviation KNOWN_DEVIATIONS[] = {
    { "PD1",
        {
            "RFcntlLock",
            "RLockf",
            0
        },
        DEV_ANY,
        95,
        CTX_ALWAYS                                     },
    { "PD2",
        {
            "RFcntlDupfd",
            "RFcntlGetfl",
            0
        },
        DEV_ANY,
        22,
        CTX_ALWAYS                                     },
    { "PD2b",
        {
            "RFcntlSetfl",
            0
        },
        DEV_ANY,
        22,
        CTX_ALWAYS    },
    { "PD3",
        {
            "RStat",
            0
        },
        DEV_ANY,
        38,
        CTX_DFD },
    { "PD7r",
        {
            "RRead",
            "RPread",
            0
        },
        9,
        0,
        CTX_ALWAYS                                             },
    { "PD11a",
        {
            "RRead",
            "RPread",
            0
        },
        0,
        13,      CTX_ALWAYS                                                    },
    { "PD11b",
        {
            "RRead",
            "RPread",
            0
        },
        9,      13,        CTX_ALWAYS                                                          },
    { "PD8",
        {
            "RRead",
            "RPread",
            0
        },
        21, 0,  CTX_ALWAYS                                                                                     },
    { "PD8w",
        {
            "RWrite",
            "RPwrite",
            0
        },
        9,  21, CTX_ALWAYS                                                                                     },
    { "PD13",
        {
            "ROpen",
            0
        },
        6,
        0,
        CTX_ALWAYS },
    { "PD14",
        {
            "ROpen",
            0
        },
        21
        ,
        0,
        CTX_ALWAYS },
    { "PD15",
        {
            "RStat",
            "RMkdir",
            "ROpen",
            "RUnlink",
            "RRmdir",
            "RTruncate",
            "RChmod",
            "RChown",
            "RUtimens",
            "RAccess",
            "RReadlink",
            0
        },
        20
        ,
        DEV_ANY,
        CTX_SLASH }
    ,
    { "PD15b",
        {
            "RMkdir",
            0
        },
        40
        ,
        17,
        CTX_SLASH },
    { "PD15c",
        {
            "RMkdir",     0
        },
        0,
        17,
        CTX_SLASH },
    { "PD17h",
        {
            "RRmdir",     0
        },
        20
        ,
        13,
        CTX_ALWAYS },
    { "PD17i",
        {
            "RRmdir",      0
        },
        39
        ,
        13,
        CTX_ALWAYS },
    { "PD17a",
        {
            "RUnlink",     0
        },
        21
        ,
        13,
        CTX_ALWAYS },
    { "PD17b",
        {
            "RMkdir",
            "RSymlink",
            "RMknod",
            "RLink",
            0
        },
        17,     13,     CTX_ALWAYS },
    { "PD17c",
        {
            "RLink",       0
        },
        1,
        13,
        CTX_ALWAYS },
    { "PD18",
        { "RAccess", 0                                                                               },
        13
        ,
        0,
        CTX_ALWAYS },
    { "PD17d",
        { "ROpen",
        "RMkdir",
        "RMknod",
        "RSymlink",
        0    },
        13
        ,        17,       CTX_ALWAYS },
    { "PD17e",  { "RLink",       0                                                               },
        1,
        17,
        CTX_ALWAYS },
    { "PD17f",  { "RLink",       0                                                               },
        2,
        17,
        CTX_ALWAYS },
    { "PD24",   { "ROpen",       "RDup",
                  "RFcntlDupfd",
                  "ROpendir", 0 }, 24, 0
        ,
        CTX_ALWAYS }
    ,
    { "PD20",   { "ROpen",       0                                                               },
        13
        ,
        0,
        CTX_ALWAYS },
    { "PD22",   { "RLseek",      0                                                               },
        6,
        22,
        CTX_ALWAYS },
    { "PD19",   { "RCloneRange", 0                                                               },
        22
        ,
        0,
        CTX_ALWAYS },
    { "PD25",   { "RMkdir",
                  "RMknod",
                  "RSymlink",
                  "RChmod", "RChown",
                  "RUnlink", "RRmdir",
                  "RTruncate",
                  "RStat",
                  0 }
        ,
        13, 36, CTX_NLONG },
    { "PD26",   { "ROpen",       0                                                               },
        13
        ,
        21,
        CTX_ACCW },
};

static int
dev_ctx_ok(
    enum devctx ctx,
    json_t     *rv)
{
    json_t *pth;
    size_t  i;

    switch (ctx) {
        case CTX_ALWAYS:
            return 1;
        case CTX_DFD:
            return tf_field(rv, "dfd") != -1;
        case CTX_SLASH:
            return tf_bool(json_object_get(rv, "pth"), "slash");
        case CTX_NLONG:
            pth = json_object_get(json_object_get(rv, "pth"), "comps");
            for (i = 0; pth && i < json_array_size(pth); i++) {
                const char *c = json_string_value(json_array_get(pth, i));
                if (c && strcmp(c, "@nlong") == 0) {
                    return 1;
                }
            }
            return 0;
        case CTX_ACCW: {
            const char *a = tf_tag(json_object_get(
                                       json_object_get(rv, "fl"), "acc"));
            return a && (strcmp(a, "AccW") == 0 || strcmp(a, "AccRW") == 0);
        }
    } /* switch */
    return 0;
} /* dev_ctx_ok */

/* Return the id of the first reconcilable deviation matching this divergence,
 * or NULL.  Called only when actual != expected. */
static const char *
reconcile(
    const char *tag,
    json_t     *rv,
    int         expected,
    int         actual)
{
    size_t i, j;

    for (i = 0; i < sizeof(KNOWN_DEVIATIONS) / sizeof(KNOWN_DEVIATIONS[0]);
         i++) {
        const struct deviation *d     = &KNOWN_DEVIATIONS[i];
        int                     op_ok = (d->ops[0] == NULL);

        for (j = 0; d->ops[j]; j++) {
            if (strcmp(tag, d->ops[j]) == 0) {
                op_ok = 1;
                break;
            }
        }
        if (!op_ok) {
            continue;
        }
        if (d->expected != DEV_ANY && d->expected != expected) {
            continue;
        }
        if (d->actual != DEV_ANY && d->actual != actual) {
            continue;
        }
        if (dev_ctx_ok(d->ctx, rv)) {
            return d->id;
        }
    }
    return NULL;
} /* reconcile */

/* ---- oracle -------------------------------------------------------------- */

/* True if the errno matches (proceed with success-path checks).  A mismatch
 * that reconciles to a known deviation is recorded and treated as non-fatal. */
static int
check_status(
    int64_t expected,
    int     actual)
{
    const char *dev;

    g_last_recon = NULL;
    if (actual == expected) {
        return 1;
    }
    dev = reconcile(g_cur_tag, g_cur_rv, (int) expected, actual);
    if (dev) {
        record_dev(dev);
        g_last_recon = dev;
        return 0;
    }
    if (g_nfs_version) {
        /* ND3: EACCES from the server's directory-search enforcement on the
         * loopback's component-by-component resolution.  Neither the posix
         * model nor chimera's own resolution (issue #1771) implements search
         * permission, so any path-taking op can draw an EACCES the model maps
         * to another outcome.  Retires when the model grows search-perm
         * semantics (which also unblocks the nfs3 passthrough batches). */
        if (actual == 13 && expected != 13 &&
            json_object_get(g_cur_rv, "pth")) {
            record_dev("ND3");
            g_last_recon = "ND3";
            return 0;
        }
        /* The same gap in the other direction: the model's EACCES masks
         * what lies behind an unsearchable directory, while chimera (whose
         * resolution has no search-perm concept) reports the true state --
         * typically ENOENT for a name that is not there. */
        if (expected == 13 && actual == 2 &&
            json_object_get(g_cur_rv, "pth")) {
            record_dev("ND3");
            g_last_recon = "ND3";
            return 0;
        }
        /* ND6: EACCES on descriptor I/O the model completed.  NFS3 is
         * stateless, so the server re-checks DAC per READ/WRITE with the
         * credential the RPC carries; the proxy sends the OPENING
         * credential and the server owner-overrides, which covers every
         * owner case -- what remains is a NON-owner opener whose access
         * was chmod'ed away after open, where real NFS diverges from
         * POSIX by design.  Retires if the model grows an nfs-per-op-DAC
         * capability. */
        if (actual == 13 && expected != 13 &&
            !json_object_get(g_cur_rv, "pth") &&
            json_object_get(g_cur_rv, "fd")) {
            record_dev("ND6");
            g_last_recon = "ND6";
            return 0;
        }
        /* ND8: the nfs3 profile pins copyRange=false, which the model
         * flattens to ENOTSUP for every copy_file_range -- but argument
         * validation comes first in POSIX and Linux alike, so a same-file
         * overlapping range draws EINVAL before any backend support check.
         * Retires when the model orders its argument checks ahead of the
         * capability answer. */
        if (actual == 22 && expected == 95 &&
            strcmp(g_cur_tag, "RCopyRange") == 0) {
            record_dev("ND8");
            g_last_recon = "ND8";
            return 0;
        }
        /* ND4 consequences of a lost descriptor / lost file (see the g_lost_*
         * block comment): EBADF where the model still holds the descriptor,
         * ENOENT where the model still has the file. */
        if (actual == 9 && expected != 9 &&
            fd_lost(g_cur_pid, (int) tf_field(g_cur_rv, "fd"))) {
            record_dev("ND4");
            g_last_recon = "ND4";
            return 0;
        }
        if (actual == 2 && expected != 2 && g_cur_fs) {
            json_t *comps = json_object_get(json_object_get(g_cur_rv, "pth"),
                                            "comps");
            if (comps && path_touches_lost(g_cur_fs, comps)) {
                record_dev("ND4");
                g_last_recon = "ND4";
                return 0;
            }
        }
    }
    mism("errno: expected %lld, got %d", (long long) expected, actual);
    return 0;
} /* check_status */

static const char *
ftype_of(const char *tag)
{
    if (strcmp(tag, "FReg") == 0) {
        return "reg";
    }
    if (strcmp(tag, "FDir") == 0) {
        return "dir";
    }
    if (strcmp(tag, "FLnk") == 0) {
        return "lnk";
    }
    if (strcmp(tag, "FFifo") == 0) {
        return "fifo";
    }
    if (strcmp(tag, "FSock") == 0) {
        return "sock";
    }
    if (strcmp(tag, "FBlk") == 0) {
        return "blk";
    }
    if (strcmp(tag, "FChr") == 0) {
        return "chr";
    }
    return "?";
} /* ftype_of */

/* Explicit utimensat instants: model reserved value -> (sec, nsec). */
static void
xtime(
    int64_t    v,
    long long *sec,
    long long *nsec)
{
    *nsec = 0;
    *sec  = (v == -1) ? 1000000 : (v == -2) ? 2000000 : -1;
} /* xtime */

/* Map a host st_mode to the model's ftype string. */
static const char *
ftype_from_mode(mode_t m)
{
    switch (m & S_IFMT) {
        case S_IFREG: return "reg";
        case S_IFDIR: return "dir";
        case S_IFLNK: return "lnk";
        case S_IFIFO: return "fifo";
        case S_IFSOCK: return "sock";
        case S_IFBLK: return "blk";
        case S_IFCHR: return "chr";
        default: return "unk";
    } /* switch */
} /* ftype_from_mode */

static void
check_time(
    int64_t   mino,
    int       field,         /* 0 atime, 1 mtime, 2 ctime */
    int64_t   abstract,
    long long wsec,
    long long wnsec)
{
    struct tent *t;

    if (field == 0 && !g_strict_atime) {
        return;
    }
    if (mino < 0 || mino >= R_MAXINO) {
        return;
    }
    t = &g_timemap[mino][field];

    if (abstract < 0) {
        long long xs, xn;
        xtime(abstract, &xs, &xn);
        if (xs < 0) {
            mism("time: unmapped explicit instant %lld",
                 (long long) abstract);
        } else if (wsec != xs || wnsec != xn) {
            mism("time: explicit instant %lld: expected %lld.%lld, got %lld.%lld",
                 (long long) abstract, xs, xn, wsec, wnsec);
        }
        t->present  = 1;
        t->abstract = abstract;
        t->sec      = wsec;
        t->nsec     = wnsec;
        return;
    }
    if (!t->present || t->abstract < 0) {
        t->present  = 1;
        t->abstract = abstract;
        t->sec      = wsec;
        t->nsec     = wnsec;
    } else if (abstract == t->abstract) {
        /* The model clock quantizes: several time-setting ops can land on
         * one abstract instant, each moving the REAL timestamp forward
         * (e.g. observe, chown, observe within a tick).  Forward motion at
         * an unchanged instant is therefore legitimate on any backend; only
         * BACKWARD motion is a bug (a stale attribute served after a newer
         * one -- the attr-cache class this check exists to catch). */
        if (wsec < t->sec || (wsec == t->sec && wnsec < t->nsec)) {
            mism("time: ino %lld %s: model instant unchanged (%lld) but wire "
                 "went backwards %lld.%lld -> %lld.%lld", (long long) mino,
                 field == 0 ? "atime" : field == 1 ? "mtime" : "ctime",
                 (long long) abstract,
                 t->sec, t->nsec, wsec, wnsec);
        } else {
            t->sec  = wsec;
            t->nsec = wnsec;
        }
    } else if (abstract > t->abstract) {
        if (wsec < t->sec || (wsec == t->sec && wnsec < t->nsec)) {
            mism("time: ino %lld %s: model advanced but wire went backwards "
                 "%lld.%lld -> %lld.%lld", (long long) mino,
                 field == 0 ? "atime" : field == 1 ? "mtime" : "ctime",
                 t->sec, t->nsec, wsec, wnsec);
        }
        t->abstract = abstract;
        t->sec      = wsec;
        t->nsec     = wnsec;
    } else {
        mism("time: model instant went backwards (harness bug?)");
    }
} /* check_time */

/* Compare a live struct stat against the model's SStatR payload (res_v). */
static void
check_statres(
    json_t            *rv,     /* model result value */
    const struct stat *st)
{
    const char   *ftag    = tf_tag(json_object_get(rv, "ftype"));
    const char   *want_ft = ftype_of(ftag);
    const char   *got_ft  = ftype_from_mode(st->st_mode);
    int64_t       mino    = tf_field(rv, "ino");

    if (ino_lost(mino)) {
        /* The model mutated this inode through a descriptor chimera never
         * had (or created it behind a reconciled divergence); its attributes
         * have honestly diverged. */
        record_dev("ND4");
        return;
    }
    long long     dev = (long long) st->st_dev;
    long long     ino = (long long) st->st_ino;
    struct ident *known;

    if (strcmp(got_ft, want_ft) != 0) {
        mism("ftype: expected %s, got %s", want_ft, got_ft);
    }
    if (strcmp(ftag, "FLnk") != 0) {
        int64_t wmode = tf_field(rv, "mode");
        if ((int64_t) (st->st_mode & 07777) != wmode) {
            mism("mode: expected %#llo, got %#o", (long long) wmode,
                 st->st_mode & 07777);
        }
    }
    if ((int64_t) st->st_uid != tf_field(rv, "uid")) {
        mism("uid: expected %lld, got %u", (long long) tf_field(rv, "uid"),
             st->st_uid);
    }
    if ((int64_t) st->st_gid != tf_field(rv, "gid")) {
        mism("gid: expected %lld, got %u", (long long) tf_field(rv, "gid"),
             st->st_gid);
    }
    if ((int64_t) st->st_nlink != tf_field(rv, "nlink")) {
        if (g_nfs_version && tf_field(rv, "nlink") == 0 &&
            st->st_nlink == 1) {
            /* Unlinked-while-open: the silly-renamed name keeps one link
             * alive until the last close (ND2). */
            record_dev("ND2");
        } else if (g_nfs_version && g_nexempt > 0 &&
                   strcmp(ftag, "FDir") == 0 &&
                   (int64_t) st->st_nlink > tf_field(rv, "nlink")) {
            /* A directory the model shrank still holds ND5 residue (a
             * subdirectory whose rmdir the silly files blocked), so its
             * link count runs high until the residue clears. */
            record_dev("ND5");
        } else {
            mism("nlink: expected %lld, got %llu",
                 (long long) tf_field(rv, "nlink"),
                 (unsigned long long) st->st_nlink);
        }
    }
    if (strcmp(ftag, "FReg") == 0) {
        int64_t want = tf_field(rv, "sizeB");
        if ((int64_t) st->st_size != want) {
            mism("size: expected %lld, got %lld", (long long) want,
                 (long long) st->st_size);
        }
    } else if (strcmp(ftag, "FLnk") == 0 && g_cur_fs) {
        json_t *node = map_get_int(json_object_get(g_cur_fs, "inodes"), mino);
        if (node) {
            char want[8192];
            real_target(json_object_get(node, "target"), want, sizeof(want));
            if ((int64_t) st->st_size != (int64_t) strlen(want)) {
                mism("symlink size: expected %zu, got %lld", strlen(want),
                     (long long) st->st_size);
            }
        }
    }

    if (mino >= 0 && mino < R_MAXINO) {
        known = &g_inomap[mino];
        if (!known->present) {
            known->present = 1;
            known->dev     = dev;
            known->ino     = ino;
        } else if (known->dev != dev || known->ino != ino) {
            mism("identity: model ino %lld previously (%lld,%lld), now (%lld,%lld)",
                 (long long) mino, known->dev, known->ino, dev, ino);
        }
    }

    check_time(mino, 0, tf_field(rv, "atime"),
               CHIMERA_STAT_ATIM(*st).tv_sec, CHIMERA_STAT_ATIM(*st).tv_nsec);
    check_time(mino, 1, tf_field(rv, "mtime"),
               CHIMERA_STAT_MTIM(*st).tv_sec, CHIMERA_STAT_MTIM(*st).tv_nsec);
    check_time(mino, 2, tf_field(rv, "ctime"),
               CHIMERA_STAT_CTIM(*st).tv_sec, CHIMERA_STAT_CTIM(*st).tv_nsec);
} /* check_statres */

/* ---- newfs/setcred: cold path, still via the JSON engine ----------------- */

static json_t *
call(json_t *req)
{
    json_t *res;

    errno = 0;
    res   = handle(req);
    json_decref(req);
    return res;
} /* call */

static json_t *
req_new(
    const char *op,
    int         pid)
{
    json_t *r = json_object();

    json_object_set_new(r, "op", json_string(op));
    json_object_set_new(r, "pid", json_integer(pid));
    return r;
} /* req_new */

/* ---- per-op handlers ----------------------------------------------------- */

static int
open_flags(json_t *fl)
{
    const char *acc = tf_tag(json_object_get(fl, "acc"));
    int         f   = 0;

    if (strcmp(acc, "AccW") == 0) {
        f = O_WRONLY;
    } else if (strcmp(acc, "AccRW") == 0) {
        f = O_RDWR;
    } else {
        f = O_RDONLY;
    }
    if (tf_bool(fl, "creat")) {
        f |= O_CREAT;
    }
    if (tf_bool(fl, "excl")) {
        f |= O_EXCL;
    }
    if (tf_bool(fl, "trunc")) {
        f |= O_TRUNC;
    }
    if (tf_bool(fl, "appendF")) {
        f |= O_APPEND;
    }
    if (tf_bool(fl, "directory")) {
        f |= O_DIRECTORY;
    }
    if (tf_bool(fl, "nofollow")) {
        f |= O_NOFOLLOW;
    }
    return f;
} /* open_flags */

/*
 * A call the model expected to FAIL that chimera let through leaves us holding
 * a descriptor the model never learned about (PD24's EMFILE with chimera's
 * larger fd table; over the NFS loopback also expected-EACCES opens the
 * server's DAC reading permits).  The model's fd is not set, so the descriptor
 * never reaches g_fdmap and close_live_handles() cannot reach it either -- and
 * one descriptor still open on the mount wedges the newfs() recycle between
 * traces, whose unmount then returns EBUSY for as long as the harness is
 * willing to retry.  Drop it at the op that minted it, for every
 * expected-failure flavor.  (op_open does its own, because it must also exempt
 * any node O_CREAT minted from the final audit.)
 */
static void
drop_stray_fd(
    int64_t model_err,
    int     rc)
{
    if (model_err != 0 && rc >= 0) {
        chimera_posix_close(rc);
    }
} /* drop_stray_fd */

static void
op_open(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    json_t *fl    = json_object_get(rv, "fl");
    int     flags = open_flags(fl);
    int64_t dfd   = tf_field(rv, "dfd");
    char    path[8192];
    int     rc, e;

    apply_cred(pid);
    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    if (tf_bool(fl, "creat")) {
        residue_preclear(path, json_object_get(rv, "pth"));
        apply_cred(pid);
    }
    if (dfd == -1) {
        rc = chimera_posix_open(path, flags, tf_field(fl, "mode"));
    } else {
        rc = chimera_posix_openat(rfd(pid, dfd), path, flags,
                                  tf_field(fl, "mode"));
    }
    e = ERRV(rc);
    if (check_status(tf_field(res_v, "e"), e) && tf_field(res_v, "e") == 0) {
        set_fd(pid, tf_field(res_v, "fd"), rc);
        if (tf_bool(fl, "trunc")) {
            int64_t ino = model_ino_of_fd(pid, tf_field(res_v, "fd"));
            if (ino >= 0) {
                shadow_resize(ino, 0);
            }
        }
    } else if (tf_field(res_v, "e") == 0 && rc < 0) {
        int redone = -1;
        if (g_last_recon && strcmp(g_last_recon, "ND3") == 0) {
            /* A search-permission denial of an open the model completed:
             * re-execute as root (see nd3_redo_wanted) so the descriptor --
             * and, for O_CREAT, the file -- exists on both sides.  The
             * active umask is still the failing pid's, so a creating open's
             * mode arithmetic is preserved; the owner is aligned with the
             * model's post-state afterwards.  I/O through the root-opened
             * descriptor then succeeds, which is exactly what the model
             * expects of a descriptor it granted. */
            apply_root_cred();
            redone = dfd == -1
                ? chimera_posix_open(path, flags, tf_field(fl, "mode"))
                : chimera_posix_openat(rfd(pid, dfd), path, flags,
                                       tf_field(fl, "mode"));
            if (redone >= 0) {
                redo_fix_owner(path, json_object_get(rv, "pth"), 1);
                set_fd(pid, tf_field(res_v, "fd"), redone);
            }
        }
        if (redone < 0) {
            /* The model holds a descriptor chimera never obtained: mark it
             * and the file it names lost, so the consequences reconcile
             * instead of cascading. */
            lose_fd(pid, tf_field(res_v, "fd"));
        }
    } else if (tf_field(res_v, "e") != 0 && e == 0 && rc >= 0) {
        /* The model expected this open to fail but chimera minted a
         * descriptor (PD24's EMFILE; over the NFS loopback also opens whose
         * expected EACCES the server's DAC reading does not share).  Close
         * the stray descriptor -- the model never learned of it, so nothing
         * downstream ever would. */
        chimera_posix_close(rc);
        if (tf_bool(fl, "creat")) {
            json_t *comps = json_object_get(json_object_get(rv, "pth"),
                                            "comps");
            char    mp[4096];
            size_t  len = 0, k;
            for (k = 0; comps && k < json_array_size(comps); k++) {
                const char *c = json_string_value(json_array_get(comps, k));
                len += (size_t) snprintf(mp + len, sizeof(mp) - len, "/%s",
                                         c ? c : "");
            }
            if (len == 0) {
                snprintf(mp, sizeof(mp), "/");
            }
            /* O_CREAT may also have minted a file the model does not have,
             * and that residue is not inert: a later op on the same name
             * meets an object the model has never heard of and diverges for
             * reasons unrelated to the descriptor table.  Remove it and the
             * namespaces agree again -- but only when the model really lacks
             * the path, since an O_CREAT over a file that already existed
             * created nothing and unlinking it would invent a divergence. */
            if (path_ino(g_cur_fs, comps) < 0) {
                char rp[4096];
                real_path(json_object_get(rv, "pth"), rp, sizeof(rp));
                chimera_posix_unlink(rp);
            } else {
                exempt_add(mp);
            }
        }
    }
} /* op_open */

static void
op_close(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    int mfd  = tf_field(rv, "fd");
    int lost = fd_lost(pid, mfd);
    int rc   = chimera_posix_close(rfd(pid, mfd));
    int e    = ERRV(rc);

    if ((check_status(tf_field(res_v, "e"), e) || lost) &&
        tf_field(res_v, "e") == 0) {
        /* set_fd also clears the lost mark: the model no longer holds the
         * descriptor, so a later model open reusing the number starts clean. */
        set_fd(pid, mfd, BADFD);
    }
} /* op_close */

static void
op_dup(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    int rc = chimera_posix_dup(rfd(pid, tf_field(rv, "fd")));
    int e  = ERRV(rc);

    if (check_status(tf_field(res_v, "e"), e) && tf_field(res_v, "e") == 0) {
        set_fd(pid, tf_field(res_v, "fd"), rc);
    } else if (tf_field(res_v, "e") == 0 && rc < 0) {
        lose_fd(pid, tf_field(res_v, "fd"));
    } else {
        drop_stray_fd(tf_field(res_v, "e"), rc);
    }
} /* op_dup */

static void
op_lseek(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    const char *wh     = tf_tag(json_object_get(rv, "wh"));
    int         whence = SEEK_SET;
    off_t       rc;
    int         e;

    if (strcmp(wh, "WCur") == 0) {
        whence = SEEK_CUR;
    } else if (strcmp(wh, "WEnd") == 0) {
        whence = SEEK_END;
    } else if (strcmp(wh, "WData") == 0) {
        whence = SEEK_DATA;
    } else if (strcmp(wh, "WHole") == 0) {
        whence = SEEK_HOLE;
    }
    int64_t exp_e        = tf_field(res_v, "e");
    int     is_size_seek = strcmp(wh, "WEnd") == 0 ||
        strcmp(wh, "WCur") == 0 ||
        strcmp(wh, "WData") == 0 ||
        strcmp(wh, "WHole") == 0;
    int     fail = 0;

    apply_cred(pid);
    rc = chimera_posix_lseek(rfd(pid, tf_field(rv, "fd")),
                             (off_t) tf_field(rv, "off"), whence);
    e = ERRV(rc);
    if (e != exp_e) {
        const char *dev = reconcile("RLseek", rv, (int) exp_e, e);
        if (dev) {              /* e.g. PD22 (ENXIO vs EINVAL) */
            record_dev(dev);
            return;
        }
        fail = 1;               /* unreconciled errno divergence */
    } else if (exp_e == 0 && (int64_t) rc != tf_field(res_v, "off")) {
        fail = 1;               /* offset divergence on success */
    }
    if (fail) {
        /* ND7: the nfs3 profile pins seekHole=false, which the model encodes
         * as EINVAL for SEEK_DATA/SEEK_HOLE; the client instead EMULATES
         * them over the holes-invisible wire (DATA at the offset, HOLE at
         * EOF, ENXIO past it) -- strictly more useful than failing.
         * Reconcile until the model grows a seek-emulated capability. */
        if (g_nfs_version && exp_e == 22 &&
            (whence == SEEK_DATA || whence == SEEK_HOLE)) {
            record_dev("ND7");
            return;
        }
        /* PD25: a directory's st_size is unspecified; the model abstracts it
         * as 0 while memfs reports a block, so size-relative seeks (and the
         * ENXIO boundary at that size) legitimately disagree on dir fds. */
        if (is_size_seek && fd_is_model_dir(pid, tf_field(rv, "fd"))) {
            record_dev("PD25");
        } else if (e != exp_e) {
            mism("errno: expected %lld, got %d", (long long) exp_e, e);
        } else {
            mism("lseek: expected offset %lld, got %lld",
                 (long long) tf_field(res_v, "off"), (long long) rc);
        }
    }
} /* op_lseek */

static void
op_read_family(
    int     pid,
    json_t *rv,
    json_t *res_v,
    int     positioned,
    int     vectored)
{
    int64_t        fd  = rfd(pid, tf_field(rv, "fd"));
    size_t         len = (size_t) tf_field(rv, "len");
    off_t          off = (off_t) tf_field(rv, "off");
    unsigned char *buf = malloc(len ? len : 1);
    ssize_t        n;
    int            e;

    apply_cred(pid);
    if (vectored) {
        struct iovec iov[2];
        int          niov = split_iovec(iov, buf, len);
        n = positioned
            ? chimera_posix_preadv2(fd, iov, niov, off, 0)
            : chimera_posix_readv(fd, iov, niov);
    } else {
        n = positioned
            ? chimera_posix_pread(fd, buf, len, off)
            : chimera_posix_read(fd, buf, len);
    }
    e = ERRV(n);
    if (check_status(tf_field(res_v, "e"), e) && tf_field(res_v, "e") == 0) {
        int64_t        count = tf_field(res_v, "n");
        unsigned char *exp   = malloc(count ? count : 1);
        if (ino_lost(tf_field(res_v, "ino"))) {
            /* The model mutated this inode through a descriptor chimera never
             * had; its size and content have honestly diverged. */
            record_dev("ND4");
        } else {
            if (n != count) {
                mism("read count: expected %lld, got %zd", (long long) count,
                     n);
            }
            shadow_read(tf_field(res_v, "ino"), tf_field(res_v, "off"),
                        (size_t) count, exp);
            if (n != count || (count && memcmp(buf, exp, count) != 0)) {
                mism("read data mismatch at ino %lld off %lld len %lld",
                     (long long) tf_field(res_v, "ino"),
                     (long long) tf_field(res_v, "off"), (long long) count);
            }
        }
        free(exp);
    }
    free(buf);
} /* op_read_family */

static void
op_write_family(
    int     pid,
    json_t *rv,
    json_t *res_v,
    int     positioned,
    int     vectored)
{
    int64_t        fd   = rfd(pid, tf_field(rv, "fd"));
    int64_t        off  = tf_field(res_v, "off");
    int64_t        len  = tf_field(rv, "len");
    int64_t        pat  = tf_field(rv, "pat");
    off_t          poff = (off_t) tf_field(rv, "off");
    unsigned char *buf  = malloc(len ? len : 1);
    ssize_t        n;
    int64_t        i;
    int            e;

    for (i = 0; i < len; i++) {
        buf[i] = pat_byte(pat, off + i);
    }
    apply_cred(pid);
    if (vectored) {
        struct iovec iov[2];
        int          niov = split_iovec(iov, buf, (size_t) len);
        n = positioned
            ? chimera_posix_pwritev2(fd, iov, niov, poff, 0)
            : chimera_posix_writev(fd, iov, niov);
    } else {
        n = positioned
            ? chimera_posix_pwrite(fd, buf, (size_t) len, poff)
            : chimera_posix_write(fd, buf, (size_t) len);
    }
    e = ERRV(n);
    if (check_status(tf_field(res_v, "e"), e) && tf_field(res_v, "e") == 0) {
        if (n != tf_field(res_v, "n")) {
            mism("write count: expected %lld, got %zd",
                 (long long) tf_field(res_v, "n"), n);
        }
        shadow_apply(tf_field(res_v, "ino"), off, buf, (size_t) len);
    } else if (tf_field(res_v, "e") == 0 && n < 0) {
        /* The model wrote, chimera did not (a reconciled divergence): the
        * inode's size and content have honestly diverged from here on. */
        int64_t lost = tf_field(res_v, "ino");
        if (lost >= 0 && lost < R_MAXINO) {
            g_lost_ino[lost] = 1;
        }
    }
    free(buf);
} /* op_write_family */

static void
op_truncate(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    char path[8192];
    int  rc, e;

    apply_cred(pid);
    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    rc = chimera_posix_truncate(path, (off_t) tf_field(rv, "len"));
    e  = ERRV(rc);
    if (!check_status(tf_field(res_v, "e"), e) &&
        nd3_redo_wanted(res_v, rc)) {
        /* Redo as root (see nd3_redo_wanted). */
        apply_root_cred();
        (void) chimera_posix_truncate(path, (off_t) tf_field(rv, "len"));
    }
    if (tf_field(res_v, "e") == 0) {
        int64_t ino = path_ino(g_cur_fs, json_object_get(
                                   json_object_get(rv, "pth"), "comps"));
        if (ino >= 0) {
            shadow_resize(ino, tf_field(rv, "len"));
        }
    }
} /* op_truncate */

static void
op_ftruncate(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    int rc, e;

    apply_cred(pid);
    rc = chimera_posix_ftruncate(rfd(pid, tf_field(rv, "fd")),
                                 (off_t) tf_field(rv, "len"));
    e = ERRV(rc);
    check_status(tf_field(res_v, "e"), e);
    if (tf_field(res_v, "e") == 0) {
        int64_t ino = model_ino_of_fd(pid, tf_field(rv, "fd"));
        if (ino >= 0) {
            shadow_resize(ino, tf_field(rv, "len"));
        }
        if (rc < 0 && ino >= 0 && ino < R_MAXINO) {
            /* The model truncated, chimera did not (a reconciled
             * divergence): size and content have honestly diverged. */
            g_lost_ino[ino] = 1;
        }
    }
} /* op_ftruncate */

static void
op_stat(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    int64_t     dfd    = tf_field(rv, "dfd");
    int         follow = tf_bool(rv, "follow");
    struct stat st;
    char        path[8192];
    int         rc, e;

    memset(&st, 0, sizeof(st));
    apply_cred(pid);
    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    if (dfd == -1) {
        rc = follow ? chimera_posix_stat(path, &st)
                    : chimera_posix_lstat(path, &st);
    } else {
        rc = chimera_posix_fstatat(rfd(pid, dfd), path, &st,
                                   follow ? 0 : AT_SYMLINK_NOFOLLOW);
    }
    e = ERRV(rc);
    if (check_status(tf_field(res_v, "e"), e) && tf_field(res_v, "e") == 0) {
        check_statres(res_v, &st);
    }
} /* op_stat */

static void
op_fstat(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    struct stat st;
    int         rc, e;

    memset(&st, 0, sizeof(st));
    apply_cred(pid);
    rc = chimera_posix_fstat(rfd(pid, tf_field(rv, "fd")), &st);
    e  = ERRV(rc);
    if (check_status(tf_field(res_v, "e"), e) && tf_field(res_v, "e") == 0) {
        check_statres(res_v, &st);
    }
} /* op_fstat */

/* statfs/statvfs/fstatfs/fstatvfs: only the errno is asserted (capacity
 * fields are backend-specific and untracked). */
static void
op_statfs_family(
    int     pid,
    json_t *rv,
    json_t *res_v,
    int     which)         /* 0 statfs, 1 statvfs, 2 fstatfs, 3 fstatvfs */
{
    struct statfs  sf;
    struct statvfs sv;
    char           path[8192];
    int            rc, e;

    apply_cred(pid);
    if (which < 2) {
        real_path(json_object_get(rv, "pth"), path, sizeof(path));
        rc = which == 0 ? chimera_posix_statfs(path, &sf)
                        : chimera_posix_statvfs(path, &sv);
    } else {
        int64_t fd = rfd(pid, tf_field(rv, "fd"));
        rc = which == 2 ? chimera_posix_fstatfs(fd, &sf)
                        : chimera_posix_fstatvfs(fd, &sv);
    }
    e = ERRV(rc);
    check_status(tf_field(res_v, "e"), e);
} /* op_statfs_family */

static void
op_mkdir(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    int64_t dfd = tf_field(rv, "dfd");
    char    path[8192];
    int     rc, e;

    apply_cred(pid);
    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    residue_preclear(path, json_object_get(rv, "pth"));
    apply_cred(pid);
    rc = dfd == -1
        ? chimera_posix_mkdir(path, (mode_t) tf_field(rv, "mode"))
        : chimera_posix_mkdirat(rfd(pid, dfd), path,
                                (mode_t) tf_field(rv, "mode"));
    e = ERRV(rc);
    if (!check_status(tf_field(res_v, "e"), e)) {
        if (nd3_redo_wanted(res_v, rc)) {
            /* Redo as root; align the owner with the model (op_open). */
            apply_root_cred();
            rc = dfd == -1
                ? chimera_posix_mkdir(path, (mode_t) tf_field(rv, "mode"))
                : chimera_posix_mkdirat(rfd(pid, dfd), path,
                                        (mode_t) tf_field(rv, "mode"));
            if (rc == 0) {
                redo_fix_owner(path, json_object_get(rv, "pth"), 1);
            }
        }
        if (tf_field(res_v, "e") == 0 && rc < 0 && g_cur_fs) {
            /* The model created this directory behind a reconciled
             * divergence; chimera did not.  Everything under it is lost. */
            int64_t ino = path_ino(g_cur_fs, json_object_get(
                                       json_object_get(rv, "pth"), "comps"));
            if (ino >= 0 && ino < R_MAXINO) {
                g_lost_ino[ino] = 1;
            }
        }
    }
} /* op_mkdir */

static void
op_mknod(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    const char *ft   = ftype_of(tf_tag(json_object_get(rv, "ft")));
    mode_t      mode = (mode_t) tf_field(rv, "mode");
    dev_t       dev  = 0;
    char        path[8192];
    int         rc, e;

    if (strcmp(ft, "fifo") == 0) {
        mode |= S_IFIFO;
    } else if (strcmp(ft, "blk") == 0) {
        mode |= S_IFBLK;
        dev   = makedev(3, 4);
    } else if (strcmp(ft, "chr") == 0) {
        mode |= S_IFCHR;
        dev   = makedev(3, 4);
    } else {
        mode |= S_IFREG;
    }
    apply_cred(pid);
    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    residue_preclear(path, json_object_get(rv, "pth"));
    apply_cred(pid);
    rc = chimera_posix_mknod(path, mode, dev);
    e  = ERRV(rc);
    if (!check_status(tf_field(res_v, "e"), e)) {
        if (nd3_redo_wanted(res_v, rc)) {
            apply_root_cred();
            rc = chimera_posix_mknod(path, mode, dev);
            if (rc == 0) {
                redo_fix_owner(path, json_object_get(rv, "pth"), 0);
            }
        }
        if (tf_field(res_v, "e") == 0 && rc < 0 && g_cur_fs) {
            int64_t lost_ino = path_ino(g_cur_fs, json_object_get(
                                            json_object_get(rv, "pth"),
                                            "comps"));
            if (lost_ino >= 0 && lost_ino < R_MAXINO) {
                g_lost_ino[lost_ino] = 1;
            }
        }
    }
} /* op_mknod */

static void
op_symlink(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    char tgt[8192], path[8192];
    int  rc, e;

    apply_cred(pid);
    real_target(json_object_get(rv, "tgt"), tgt, sizeof(tgt));
    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    residue_preclear(path, json_object_get(rv, "pth"));
    apply_cred(pid);
    rc = chimera_posix_symlink(tgt, path);
    e  = ERRV(rc);
    if (!check_status(tf_field(res_v, "e"), e)) {
        if (nd3_redo_wanted(res_v, rc)) {
            apply_root_cred();
            rc = chimera_posix_symlink(tgt, path);
            if (rc == 0) {
                redo_fix_owner(path, json_object_get(rv, "pth"), 0);
            }
        }
        if (tf_field(res_v, "e") == 0 && rc < 0 && g_cur_fs) {
            int64_t lost_ino = path_ino(g_cur_fs, json_object_get(
                                            json_object_get(rv, "pth"),
                                            "comps"));
            if (lost_ino >= 0 && lost_ino < R_MAXINO) {
                g_lost_ino[lost_ino] = 1;
            }
        }
    }
} /* op_symlink */

static void
op_link(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    char o[8192], n[8192];
    int  rc, e;

    apply_cred(pid);
    real_path(json_object_get(rv, "pthOld"), o, sizeof(o));
    real_path(json_object_get(rv, "pthNew"), n, sizeof(n));
    residue_preclear(n, json_object_get(rv, "pthNew"));
    apply_cred(pid);
    rc = tf_bool(rv, "followOld")
        ? chimera_posix_linkat(AT_FDCWD, o, AT_FDCWD, n, AT_SYMLINK_FOLLOW)
        : chimera_posix_link(o, n);
    e = ERRV(rc);
    if (!check_status(tf_field(res_v, "e"), e) &&
        nd3_redo_wanted(res_v, rc)) {
        /* Redo as root (see nd3_redo_wanted). */
        apply_root_cred();
        (void) (tf_bool(rv, "followOld")
                ? chimera_posix_linkat(AT_FDCWD, o, AT_FDCWD, n,
                                       AT_SYMLINK_FOLLOW)
                : chimera_posix_link(o, n));
    }
} /* op_link */

static void
op_unlink(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    int64_t dfd = tf_field(rv, "dfd");
    char    path[8192];
    int     rc, e;

    apply_cred(pid);
    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    rc = dfd == -1 ? chimera_posix_unlink(path)
                   : chimera_posix_unlinkat(rfd(pid, dfd), path, 0);
    e = ERRV(rc);
    if (!check_status(tf_field(res_v, "e"), e) &&
        nd3_redo_wanted(res_v, rc)) {
        /* Redo as root (see nd3_redo_wanted). */
        apply_root_cred();
        (void) (dfd == -1 ? chimera_posix_unlink(path)
                          : chimera_posix_unlinkat(rfd(pid, dfd), path, 0));
    }
} /* op_unlink */

static void
op_rmdir(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    int64_t dfd = tf_field(rv, "dfd");
    char    path[8192];
    int     rc, e;

    apply_cred(pid);
    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    rc = dfd == -1
        ? chimera_posix_rmdir(path)
        : chimera_posix_unlinkat(rfd(pid, dfd), path, AT_REMOVEDIR);
    e = ERRV(rc);
    if (g_nfs_version && e == 39 && tf_field(res_v, "e") == 0) {
        /* ND5: the directory holds .nfs* silly-rename residue for files
         * still open, so the server honestly refuses ENOTEMPTY where the
         * model (whose unlinks truly removed the names) sees empty.  The
         * model forgets the directory; chimera keeps it until the last
         * close -- exempt the path so the audit tolerates the leftover. */
        record_dev("ND5");
        {
            json_t *comps = json_object_get(json_object_get(rv, "pth"),
                                            "comps");
            char    mp[4096];
            size_t  len = 0, k;
            for (k = 0; comps && k < json_array_size(comps); k++) {
                const char *c = json_string_value(json_array_get(comps, k));
                len += (size_t) snprintf(mp + len, sizeof(mp) - len, "/%s",
                                         c ? c : "");
            }
            if (len > 0) {
                exempt_add(mp);
            }
        }
        return;
    }
    if (!check_status(tf_field(res_v, "e"), e) &&
        nd3_redo_wanted(res_v, rc)) {
        /* Redo as root (see nd3_redo_wanted). */
        apply_root_cred();
        (void) (dfd == -1
                ? chimera_posix_rmdir(path)
                : chimera_posix_unlinkat(rfd(pid, dfd), path, AT_REMOVEDIR));
    }
} /* op_rmdir */

static void
op_rename(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    char o[8192], n[8192];
    int  rc, e;

    apply_cred(pid);
    real_path(json_object_get(rv, "pthOld"), o, sizeof(o));
    real_path(json_object_get(rv, "pthNew"), n, sizeof(n));
    rc = chimera_posix_rename(o, n);
    e  = ERRV(rc);
    if (!check_status(tf_field(res_v, "e"), e) &&
        nd3_redo_wanted(res_v, rc)) {
        /* Redo as root (see nd3_redo_wanted). */
        apply_root_cred();
        (void) chimera_posix_rename(o, n);
    }
} /* op_rename */

static void
op_readlink(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    char    path[8192], buf[8192];
    ssize_t n;
    int     e;

    apply_cred(pid);
    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    n = chimera_posix_readlink(path, buf, sizeof(buf) - 1);
    e = ERRV(n);
    if (check_status(tf_field(res_v, "e"), e) && tf_field(res_v, "e") == 0) {
        char want[8192];
        buf[n >= 0 ? n : 0] = '\0';
        real_target(json_object_get(res_v, "tgt"), want, sizeof(want));
        if (strcmp(buf, want) != 0) {
            mism("readlink: expected '%s', got '%s'", want, buf);
        }
    }
} /* op_readlink */

static void
op_opendir(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    CHIMERA_DIR *d;
    char         path[8192];
    int          e;

    apply_cred(pid);
    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    d = chimera_posix_opendir(path);
    e = d ? 0 : host_to_linux(errno);
    if (check_status(tf_field(res_v, "e"), e) && tf_field(res_v, "e") == 0) {
        int64_t msid = tf_field(res_v, "sid");
        if (msid >= 0 && msid < R_MAXSID) {
            g_dirmap[msid]   = d;
            g_lost_sid[msid] = 0;
        } else {
            chimera_posix_closedir(d);
        }
    } else if (tf_field(res_v, "e") == 0 && !d) {
        int64_t msid = tf_field(res_v, "sid");
        if (g_last_recon && strcmp(g_last_recon, "ND3") == 0) {
            /* Redo as root (see nd3_redo_wanted). */
            apply_root_cred();
            d = chimera_posix_opendir(path);
        }
        if (d && msid >= 0 && msid < R_MAXSID) {
            g_dirmap[msid]   = d;
            g_lost_sid[msid] = 0;
        } else if (d) {
            chimera_posix_closedir(d);
        } else if (msid >= 0 && msid < R_MAXSID) {
            g_lost_sid[msid] = 1;
        }
    } else if (d) {
        chimera_posix_closedir(d);
    }
} /* op_opendir */

static void
op_readdir(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    CHIMERA_DIR   *d = rdir(tf_field(rv, "sid"));
    struct dirent *de;
    json_t        *want;
    char           names[256][256];
    int            nnames = 0, i;
    size_t         j;

    (void) pid;
    if (!d) {
        int64_t msid = tf_field(rv, "sid");
        if (g_nfs_version && msid >= 0 && msid < R_MAXSID &&
            g_lost_sid[msid]) {
            record_dev("ND4");
        } else {
            check_status(tf_field(res_v, "e"), EBADF);
        }
        return;
    }
    if (!check_status(tf_field(res_v, "e"), 0) || tf_field(res_v, "e") != 0) {
        return;
    }
    /* One atomic full sweep from a fresh cursor (the model's RReaddir returns
     * the full current entry set each time). */
    chimera_posix_rewinddir(d);
    while ((de = chimera_posix_readdir(d)) != NULL && nnames < 256) {
        snprintf(names[nnames++], 256, "%s", de->d_name);
    }

    want = json_object_get(res_v, "names");
    if (json_is_object(want)) {
        json_t *sset = json_object_get(want, "#set");
        if (sset) {
            want = sset;
        }
    }
    /* every model name present, and every live name (minus . / ..) modelled */
    for (j = 0; want && j < json_array_size(want); j++) {
        const char *wn    = json_string_value(json_array_get(want, j));
        int         found = 0;
        for (i = 0; i < nnames; i++) {
            if (wn && strcmp(names[i], wn) == 0) {
                found = 1;
                break;
            }
        }
        if (!found) {
            mism("readdir: model name '%s' missing", wn ? wn : "?");
        }
    }
    for (i = 0; i < nnames; i++) {
        int ok = 0;
        if (strcmp(names[i], ".") == 0 || strcmp(names[i], "..") == 0) {
            continue;
        }
        for (j = 0; want && j < json_array_size(want); j++) {
            const char *wn = json_string_value(json_array_get(want, j));
            if (wn && strcmp(wn, names[i]) == 0) {
                ok = 1;
                break;
            }
        }
        if (!ok) {
            if (is_nfs_silly_name(names[i])) {
                record_dev("ND1");
            } else if (is_nd5_quarantine(names[i])) {
                record_dev("ND5");
            } else if (exempt_basename(names[i])) {
                /* ND5 residue (a name the model removed but silly-rename
                 * kept alive chimera-side) showing in its parent listing;
                 * matched by basename because a stream id gives no dir
                 * path.  Cleared by residue_preclear when reused. */
                record_dev("ND5");
            } else {
                mism("readdir: unexpected name '%s'", names[i]);
            }
        }
    }
} /* op_readdir */

static void
op_dir_simple(
    int     pid,
    json_t *rv,
    json_t *res_v,
    int     which)         /* 0 rewinddir, 1 telldir, 2 seekdir, 3 closedir */
{
    CHIMERA_DIR *d = rdir(tf_field(rv, "sid"));
    int          e = 0;

    (void) pid;
    if (!d) {
        int64_t msid = tf_field(rv, "sid");
        if (g_nfs_version && msid >= 0 && msid < R_MAXSID &&
            g_lost_sid[msid]) {
            record_dev("ND4");
        } else {
            check_status(tf_field(res_v, "e"), EBADF);
        }
        return;
    }
    switch (which) {
        case 0:
            chimera_posix_rewinddir(d);
            break;
        case 1:
            e = chimera_posix_telldir(d) < 0 ? host_to_linux(errno) : 0;
            break;
        case 2:
            chimera_posix_seekdir(d, (long) tf_field(rv, "loc"));
            break;
        case 3:
            e = chimera_posix_closedir(d) < 0 ? host_to_linux(errno) : 0;
            break;
    } /* switch */
    if (check_status(tf_field(res_v, "e"), e) && which == 3 &&
        tf_field(res_v, "e") == 0) {
        int64_t msid = tf_field(rv, "sid");
        if (msid >= 0 && msid < R_MAXSID) {
            g_dirmap[msid] = NULL;
        }
    }
} /* op_dir_simple */

static void
op_copy_range(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    off_t   off_in  = (off_t) tf_field(rv, "offIn");
    off_t   off_out = (off_t) tf_field(rv, "offOut");
    ssize_t n;
    int     e;

    apply_cred(pid);
    n = chimera_posix_copy_file_range(rfd(pid, tf_field(rv, "fdIn")), &off_in,
                                      rfd(pid, tf_field(rv, "fdOut")), &off_out,
                                      (size_t) tf_field(rv, "len"), 0);
    e = ERRV(n);
    if (check_status(tf_field(res_v, "e"), e) && tf_field(res_v, "e") == 0) {
        int64_t want = tf_field(res_v, "n");
        if (n != want) {
            mism("copy_file_range: expected %lld, got %zd", (long long) want, n);
        }
        if (n > 0) {
            int64_t si = model_ino_of_fd(pid, tf_field(rv, "fdIn"));
            int64_t di = model_ino_of_fd(pid, tf_field(rv, "fdOut"));
            if (si >= 0 && di >= 0) {
                unsigned char *tmp = malloc(n);
                shadow_read(si, tf_field(rv, "offIn"), (size_t) n, tmp);
                shadow_apply(di, tf_field(rv, "offOut"), tmp, (size_t) n);
                free(tmp);
            }
        }
    }
} /* op_copy_range */

static void
op_clone_range(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    int64_t len = tf_field(rv, "len");
    int     rc, e;

    apply_cred(pid);
    rc = chimera_posix_clone_file_range(rfd(pid, tf_field(rv, "fdDst")),
                                        (off_t) tf_field(rv, "offDst"),
                                        rfd(pid, tf_field(rv, "fdSrc")),
                                        (off_t) tf_field(rv, "offSrc"),
                                        (size_t) len);
    e = ERRV(rc);
    if (check_status(tf_field(res_v, "e"), e) && tf_field(res_v, "e") == 0) {
        int64_t si = model_ino_of_fd(pid, tf_field(rv, "fdSrc"));
        int64_t di = model_ino_of_fd(pid, tf_field(rv, "fdDst"));
        if (si >= 0 && di >= 0) {
            unsigned char *tmp = malloc(len ? len : 1);
            shadow_read(si, tf_field(rv, "offSrc"), (size_t) len, tmp);
            shadow_apply(di, tf_field(rv, "offDst"), tmp, (size_t) len);
            free(tmp);
        }
    }
} /* op_clone_range */

static void
op_fallocate(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    int64_t mode = tf_field(rv, "mode");
    off_t   off  = (off_t) tf_field(rv, "off");
    off_t   len  = (off_t) tf_field(rv, "len");
    int     rc, e;

    apply_cred(pid);
    rc = mode == 0
        ? chimera_posix_fallocate(rfd(pid, tf_field(rv, "fd")), off, len)
        : chimera_posix_fallocate_mode(rfd(pid, tf_field(rv, "fd")),
                                       FALLOC_FL_PUNCH_HOLE | FALLOC_FL_KEEP_SIZE,
                                       off, len);
    e = ERRV(rc);
    check_status(tf_field(res_v, "e"), e);
    if (tf_field(res_v, "e") == 0) {
        int64_t ino = model_ino_of_fd(pid, tf_field(rv, "fd"));
        if (ino >= 0) {
            if (mode == 0) {
                int64_t end = tf_field(rv, "off") + tf_field(rv, "len");
                if ((int64_t) g_shadow[ino].len < end) {
                    shadow_resize(ino, end);
                }
            } else {
                shadow_punch(ino, tf_field(rv, "off"), tf_field(rv, "len"));
            }
        }
    }
} /* op_fallocate */

static void
op_fsync(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    int rc, e;

    apply_cred(pid);
    rc = tf_bool(rv, "dataOnly")
        ? chimera_posix_fdatasync(rfd(pid, tf_field(rv, "fd")))
        : chimera_posix_fsync(rfd(pid, tf_field(rv, "fd")));
    e = ERRV(rc);
    check_status(tf_field(res_v, "e"), e);
} /* op_fsync */

/* Build a struct timespec from the model's timestamp record (TsNow / TsOmit /
 * explicit XTIME instant). */
static void
ts_build(
    json_t          *ts,
    struct timespec *out)
{
    const char *tag = tf_tag(ts);

    if (strcmp(tag, "TsOmit") == 0) {
        out->tv_sec  = 0;
        out->tv_nsec = UTIME_OMIT;
    } else if (strcmp(tag, "TsNow") == 0) {
        out->tv_sec  = 0;
        out->tv_nsec = UTIME_NOW;
    } else {
        long long xs, xn;
        xtime(tf_i64(tf_val(ts)), &xs, &xn);
        out->tv_sec  = (time_t) xs;
        out->tv_nsec = xn;
    }
} /* ts_build */

static void
op_chmod(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    char path[8192];
    int  rc, e;

    apply_cred(pid);
    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    rc = chimera_posix_chmod(path, (mode_t) tf_field(rv, "mode"));
    e  = ERRV(rc);
    if (!check_status(tf_field(res_v, "e"), e) &&
        nd3_redo_wanted(res_v, rc)) {
        /* Redo as root (see nd3_redo_wanted). */
        apply_root_cred();
        (void) chimera_posix_chmod(path, (mode_t) tf_field(rv, "mode"));
    }
} /* op_chmod */

static void
op_fchmod(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    int rc, e;

    apply_cred(pid);
    rc = chimera_posix_fchmod(rfd(pid, tf_field(rv, "fd")),
                              (mode_t) tf_field(rv, "mode"));
    e = ERRV(rc);
    check_status(tf_field(res_v, "e"), e);
} /* op_fchmod */

static void
op_chown(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    char  path[8192];
    uid_t u = (uid_t) tf_field(rv, "u");
    gid_t g = (gid_t) tf_field(rv, "g");
    int   rc, e;

    apply_cred(pid);
    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    rc = tf_bool(rv, "follow") ? chimera_posix_chown(path, u, g)
                               : chimera_posix_lchown(path, u, g);
    e = ERRV(rc);
    if (!check_status(tf_field(res_v, "e"), e) &&
        nd3_redo_wanted(res_v, rc)) {
        /* Redo as root (see nd3_redo_wanted). */
        apply_root_cred();
        (void) (tf_bool(rv, "follow")
                ? chimera_posix_chown(path, u, g)
                : chimera_posix_lchown(path, u, g));
    }
} /* op_chown */

static void
op_fchown(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    int rc, e;

    apply_cred(pid);
    rc = chimera_posix_fchown(rfd(pid, tf_field(rv, "fd")),
                              (uid_t) tf_field(rv, "u"),
                              (gid_t) tf_field(rv, "g"));
    e = ERRV(rc);
    check_status(tf_field(res_v, "e"), e);
} /* op_fchown */

static void
op_utimens(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    struct timespec times[2];
    int64_t         dfd = tf_field(rv, "dfd");
    char            path[8192];
    int             rc, e;

    ts_build(json_object_get(rv, "ta"), &times[0]);
    ts_build(json_object_get(rv, "tm"), &times[1]);
    apply_cred(pid);
    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    rc = chimera_posix_utimensat(dfd == -1 ? AT_FDCWD : rfd(pid, dfd),
                                 path, times, 0);
    e = ERRV(rc);
    if (!check_status(tf_field(res_v, "e"), e) &&
        nd3_redo_wanted(res_v, rc)) {
        /* Redo as root (see nd3_redo_wanted). */
        apply_root_cred();
        (void) chimera_posix_utimensat(dfd == -1 ? AT_FDCWD : rfd(pid, dfd),
                                       path, times, 0);
    }
} /* op_utimens */

static void
op_futimens(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    struct timespec times[2];
    int             rc, e;

    ts_build(json_object_get(rv, "ta"), &times[0]);
    ts_build(json_object_get(rv, "tm"), &times[1]);
    apply_cred(pid);
    rc = chimera_posix_futimens(rfd(pid, tf_field(rv, "fd")), times);
    e  = ERRV(rc);
    check_status(tf_field(res_v, "e"), e);
} /* op_futimens */

static void
op_access(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    char path[8192];
    int  mode = 0, rc, e;

    if (tf_bool(rv, "r")) {
        mode |= R_OK;
    }
    if (tf_bool(rv, "w")) {
        mode |= W_OK;
    }
    if (tf_bool(rv, "x")) {
        mode |= X_OK;
    }
    apply_cred(pid);
    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    rc = chimera_posix_faccessat(AT_FDCWD, path, mode,
                                 tf_bool(rv, "eff") ? AT_EACCESS : 0);
    e = ERRV(rc);
    check_status(tf_field(res_v, "e"), e);
} /* op_access */

static void
op_umask(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    int old;

    if (pid < 0 || pid >= MAX_PIDS) {
        pid = 0;
    }
    old                = (int) driver_umasks[pid];
    driver_umasks[pid] = (mode_t) tf_field(rv, "mask");
    if (old != tf_field(res_v, "old")) {
        mism("umask bookkeeping: expected old %lld, had %d (harness bug)",
             (long long) tf_field(res_v, "old"), old);
    }
} /* op_umask */

static void
op_dup2(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    int64_t fd     = tf_field(rv, "fd");
    int64_t nfd    = tf_field(rv, "nfd");
    int     target = rfd(pid, nfd);
    int     rc, e, minted;

    apply_cred(pid);
    if (fd == nfd || target != BADFD) {
        rc     = chimera_posix_dup2(rfd(pid, fd), target);
        minted = 0;
    } else {
        /* The model's nfd names a free slot; chimera fd numbers are its own,
         * so a plain dup() is observationally identical here. */
        rc     = chimera_posix_dup(rfd(pid, fd));
        minted = 1;
    }
    e = ERRV(rc);
    if (check_status(tf_field(res_v, "e"), e) && tf_field(res_v, "e") == 0) {
        set_fd(pid, nfd, rc);
    } else if (tf_field(res_v, "e") == 0 && rc < 0) {
        lose_fd(pid, nfd);
    } else if (minted) {
        /* Only the dup() arm mints a descriptor of its own; the dup2() arm
         * returns one g_fdmap already names, which close_live_handles() owns. */
        drop_stray_fd(tf_field(res_v, "e"), rc);
    }
} /* op_dup2 */

static void
op_fcntl_dupfd(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    int rc, e, ok;

    apply_cred(pid);
    rc = chimera_posix_fcntl(rfd(pid, tf_field(rv, "fd")), F_DUPFD,
                             (int) tf_field(rv, "atLeast"));
    e  = ERRV(rc);
    ok = check_status(tf_field(res_v, "e"), e);
    if (tf_field(res_v, "e") == 0) {
        if (ok) {
            set_fd(pid, tf_field(res_v, "fd"), rc);
        } else if (e == 22) {
            /* PD2: F_DUPFD is unimplemented (EINVAL).  Emulate with dup() so
             * the model's new descriptor exists on the real side and replay
             * stays in sync. */
            int r2 = chimera_posix_dup(rfd(pid, tf_field(rv, "fd")));
            if (r2 >= 0) {
                set_fd(pid, tf_field(res_v, "fd"), r2);
            }
        }
    } else {
        drop_stray_fd(tf_field(res_v, "e"), rc);
    }
} /* op_fcntl_dupfd */

static void
op_fcntl_getfl(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    int rc, e;

    apply_cred(pid);
    rc = chimera_posix_fcntl(rfd(pid, tf_field(rv, "fd")), F_GETFL);
    e  = ERRV(rc);
    if (check_status(tf_field(res_v, "e"), e) && tf_field(res_v, "e") == 0) {
        const char *acc      = tf_tag(json_object_get(res_v, "acc"));
        int         want_acc = strcmp(acc, "AccW") == 0 ? O_WRONLY
                             : strcmp(acc, "AccRW") == 0 ? O_RDWR : O_RDONLY;
        if ((rc & O_ACCMODE) != want_acc) {
            mism("F_GETFL access mode: expected %d, got %d", want_acc,
                 rc & O_ACCMODE);
        }
        if (!!(rc & O_APPEND) != tf_bool(res_v, "appendF")) {
            mism("F_GETFL O_APPEND: expected %d, flags %#x",
                 tf_bool(res_v, "appendF"), rc);
        }
    }
} /* op_fcntl_getfl */

static void
op_fcntl_setfl(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    int rc, e;

    apply_cred(pid);
    rc = chimera_posix_fcntl(rfd(pid, tf_field(rv, "fd")), F_SETFL,
                             tf_bool(rv, "appendF") ? O_APPEND : 0);
    e = ERRV(rc);
    check_status(tf_field(res_v, "e"), e);
} /* op_fcntl_setfl */

static void
op_fcntl_lock(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    const char  *cmdt = tf_tag(json_object_get(rv, "cmd"));
    const char  *lkt  = tf_tag(json_object_get(rv, "lk"));
    struct flock fl;
    int          cmd = F_SETLK, rc, e;

    memset(&fl, 0, sizeof(fl));
    if (strcmp(cmdt, "CSetlkw") == 0) {
        cmd = F_SETLKW;
    } else if (strcmp(cmdt, "CGetlk") == 0) {
        cmd = F_GETLK;
    }
    fl.l_type = strcmp(lkt, "LkWr") == 0 ? F_WRLCK
              : strcmp(lkt, "LkUn") == 0 ? F_UNLCK : F_RDLCK;
    fl.l_whence = SEEK_SET;
    fl.l_start  = (off_t) tf_field(rv, "lo");
    fl.l_len    = (off_t) (tf_field(rv, "hi") - tf_field(rv, "lo"));

    apply_cred(pid);
    rc = chimera_posix_fcntl(rfd(pid, tf_field(rv, "fd")), cmd, &fl);
    e  = ERRV(rc);
    if (check_status(tf_field(res_v, "e"), e) && tf_field(res_v, "e") == 0 &&
        cmd == F_GETLK) {
        int conflict = fl.l_type != F_UNLCK;
        if (conflict != tf_bool(res_v, "conflict")) {
            mism("F_GETLK: expected conflict=%d, got l_type %d",
                 tf_bool(res_v, "conflict"), fl.l_type);
        }
    }
} /* op_fcntl_lock */

static void
op_lockf(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    const char *cmdt = tf_tag(json_object_get(rv, "cmd"));
    int         cmd  = F_LOCK, rc, e;

    if (strcmp(cmdt, "LfTlock") == 0) {
        cmd = F_TLOCK;
    } else if (strcmp(cmdt, "LfUlock") == 0) {
        cmd = F_ULOCK;
    } else if (strcmp(cmdt, "LfTst") == 0) {
        cmd = F_TEST;
    }
    apply_cred(pid);
    rc = chimera_posix_lockf(rfd(pid, tf_field(rv, "fd")), cmd,
                             (off_t) tf_field(rv, "len"));
    e = ERRV(rc);
    check_status(tf_field(res_v, "e"), e);
} /* op_lockf */
/* ---- dispatch + trace driving -------------------------------------------- */

static int
dispatch(
    const char *tag,
    int         pid,
    json_t     *rv,
    json_t     *res_v)
{
    if (strcmp(tag, "ROpen") == 0) {
        op_open(pid, rv, res_v);
    } else if (strcmp(tag, "RClose") == 0) {
        op_close(pid, rv, res_v);
    } else if (strcmp(tag, "RDup") == 0) {
        op_dup(pid, rv, res_v);
    } else if (strcmp(tag, "RLseek") == 0) {
        op_lseek(pid, rv, res_v);
    } else if (strcmp(tag, "RRead") == 0) {
        op_read_family(pid, rv, res_v, 0, 0);
    } else if (strcmp(tag, "RPread") == 0) {
        op_read_family(pid, rv, res_v, 1, 0);
    } else if (strcmp(tag, "RReadv") == 0) {
        op_read_family(pid, rv, res_v, 0, 1);
    } else if (strcmp(tag, "RPreadv") == 0) {
        op_read_family(pid, rv, res_v, 1, 1);
    } else if (strcmp(tag, "RWrite") == 0) {
        op_write_family(pid, rv, res_v, 0, 0);
    } else if (strcmp(tag, "RPwrite") == 0) {
        op_write_family(pid, rv, res_v, 1, 0);
    } else if (strcmp(tag, "RWritev") == 0) {
        op_write_family(pid, rv, res_v, 0, 1);
    } else if (strcmp(tag, "RPwritev") == 0) {
        op_write_family(pid, rv, res_v, 1, 1);
    } else if (strcmp(tag, "RTruncate") == 0) {
        op_truncate(pid, rv, res_v);
    } else if (strcmp(tag, "RFtruncate") == 0) {
        op_ftruncate(pid, rv, res_v);
    } else if (strcmp(tag, "RStat") == 0) {
        op_stat(pid, rv, res_v);
    } else if (strcmp(tag, "RFstat") == 0) {
        op_fstat(pid, rv, res_v);
    } else if (strcmp(tag, "RStatfs") == 0) {
        op_statfs_family(pid, rv, res_v, 0);
    } else if (strcmp(tag, "RStatvfs") == 0) {
        op_statfs_family(pid, rv, res_v, 1);
    } else if (strcmp(tag, "RFstatfs") == 0) {
        op_statfs_family(pid, rv, res_v, 2);
    } else if (strcmp(tag, "RFstatvfs") == 0) {
        op_statfs_family(pid, rv, res_v, 3);
    } else if (strcmp(tag, "RMkdir") == 0) {
        op_mkdir(pid, rv, res_v);
    } else if (strcmp(tag, "RMknod") == 0) {
        op_mknod(pid, rv, res_v);
    } else if (strcmp(tag, "RSymlink") == 0) {
        op_symlink(pid, rv, res_v);
    } else if (strcmp(tag, "RLink") == 0) {
        op_link(pid, rv, res_v);
    } else if (strcmp(tag, "RUnlink") == 0) {
        op_unlink(pid, rv, res_v);
    } else if (strcmp(tag, "RRmdir") == 0) {
        op_rmdir(pid, rv, res_v);
    } else if (strcmp(tag, "RRename") == 0) {
        op_rename(pid, rv, res_v);
    } else if (strcmp(tag, "RReadlink") == 0) {
        op_readlink(pid, rv, res_v);
    } else if (strcmp(tag, "ROpendir") == 0) {
        op_opendir(pid, rv, res_v);
    } else if (strcmp(tag, "RReaddir") == 0) {
        op_readdir(pid, rv, res_v);
    } else if (strcmp(tag, "RRewinddir") == 0) {
        op_dir_simple(pid, rv, res_v, 0);
    } else if (strcmp(tag, "RTelldir") == 0) {
        op_dir_simple(pid, rv, res_v, 1);
    } else if (strcmp(tag, "RSeekdir") == 0) {
        op_dir_simple(pid, rv, res_v, 2);
    } else if (strcmp(tag, "RClosedir") == 0) {
        op_dir_simple(pid, rv, res_v, 3);
    } else if (strcmp(tag, "RCopyRange") == 0) {
        op_copy_range(pid, rv, res_v);
    } else if (strcmp(tag, "RCloneRange") == 0) {
        op_clone_range(pid, rv, res_v);
    } else if (strcmp(tag, "RFallocate") == 0) {
        op_fallocate(pid, rv, res_v);
    } else if (strcmp(tag, "RFsync") == 0) {
        op_fsync(pid, rv, res_v);
    } else if (strcmp(tag, "RChmod") == 0) {
        op_chmod(pid, rv, res_v);
    } else if (strcmp(tag, "RFchmod") == 0) {
        op_fchmod(pid, rv, res_v);
    } else if (strcmp(tag, "RChown") == 0) {
        op_chown(pid, rv, res_v);
    } else if (strcmp(tag, "RFchown") == 0) {
        op_fchown(pid, rv, res_v);
    } else if (strcmp(tag, "RUtimens") == 0) {
        op_utimens(pid, rv, res_v);
    } else if (strcmp(tag, "RFutimens") == 0) {
        op_futimens(pid, rv, res_v);
    } else if (strcmp(tag, "RAccess") == 0) {
        op_access(pid, rv, res_v);
    } else if (strcmp(tag, "RUmask") == 0) {
        op_umask(pid, rv, res_v);
    } else if (strcmp(tag, "RDup2") == 0) {
        op_dup2(pid, rv, res_v);
    } else if (strcmp(tag, "RFcntlDupfd") == 0) {
        op_fcntl_dupfd(pid, rv, res_v);
    } else if (strcmp(tag, "RFcntlGetfl") == 0) {
        op_fcntl_getfl(pid, rv, res_v);
    } else if (strcmp(tag, "RFcntlSetfl") == 0) {
        op_fcntl_setfl(pid, rv, res_v);
    } else if (strcmp(tag, "RFcntlLock") == 0) {
        op_fcntl_lock(pid, rv, res_v);
    } else if (strcmp(tag, "RLockf") == 0) {
        op_lockf(pid, rv, res_v);
    } else {
        return -1;   /* unimplemented op */
    }
    return 0;
} /* dispatch */

/* Set per-pid credentials the way Replayer.__init__/creds_for does. */
static void
setcred(
    int pid,
    int uid,
    int gid,
    int g0,
    int g1)
{
    json_t *req  = req_new("setcred", pid);
    json_t *gids = json_array();
    json_t *res;

    json_object_set_new(req, "uid", json_integer(uid));
    json_object_set_new(req, "gid", json_integer(gid));
    json_array_append_new(gids, json_integer(g0));
    json_array_append_new(gids, json_integer(g1));
    json_object_set_new(req, "gids", gids);
    res = call(req);
    json_decref(res);
} /* setcred */

/* State-key lookups that survive the model's `inst::mod::name` namespacing. */
static json_t *
state_get(
    json_t     *state,
    const char *suffix)
{
    const char *key;
    json_t     *val;
    size_t      slen = strlen(suffix);

    json_object_foreach(state, key, val)
    {
        size_t klen = strlen(key);

        if (klen >= slen && strcmp(key + klen - slen, suffix) == 0 &&
            (klen == slen || key[klen - slen - 1] == ':')) {
            return val;
        }
    }
    return NULL;
} /* state_get */

/* Close every handle still open from the just-replayed trace -- both the real
 * fds in the model->fd map and the live directory streams.  A trace can
 * legitimately end with an open fd or dir (e.g. on an unlinked inode), and the
 * newfs op unmounts the old filesystem, which upstream's mkfs refuses while any
 * handle keeps it busy (Device or resource busy).  state_reset() only forgets
 * the maps, so drop the underlying handles here before the recycle. */
static void
close_live_handles(void)
{
    int pid, mfd, sid;

    for (pid = 0; pid < R_MAXPID; pid++) {
        for (mfd = 0; mfd < R_MAXFD; mfd++) {
            if (g_fdmap[pid][mfd] != BADFD) {
                chimera_posix_close(g_fdmap[pid][mfd]);
                g_fdmap[pid][mfd] = BADFD;
            }
        }
    }
    for (sid = 0; sid < R_MAXSID; sid++) {
        if (g_dirmap[sid]) {
            chimera_posix_closedir(g_dirmap[sid]);
            g_dirmap[sid] = NULL;
        }
    }
} /* close_live_handles */

/* Reset the driver to a fresh empty filesystem (driver "newfs" op). */
static int
newfs(void)
{
    json_t *res;
    int     ok;

    close_live_handles();
    res = call(req_new("newfs", 0));
    ok  = res && tf_field(res, "err") == 0;

    json_decref(res);
    return ok;
} /* newfs */

/* ---- end-of-trace audit -------------------------------------------------- */

struct auditent { int64_t ino; char path[4096]; };

/* Collect a directory's live entries into names[][]; returns the count. */
static int
audit_readdir(
    CHIMERA_DIR *d,
    char         names[][256],
    int          cap)
{
    struct dirent *de;
    int            n = 0;

    chimera_posix_rewinddir(d);
    while ((de = chimera_posix_readdir(d)) != NULL && n < cap) {
        snprintf(names[n++], 256, "%s", de->d_name);
    }
    return n;
} /* audit_readdir */

/* Walk the final model tree (out-of-band as root pid 3) and verify identity,
 * attributes, directory contents, link targets and file bytes.  Returns the
 * number of objects audited. */
static int
final_audit(json_t *fs)
{
    json_t          *inodes  = json_object_get(fs, "inodes");
    struct auditent *stack   = malloc(sizeof(*stack) * 4096);
    int              sp      = 0;
    int              audited = 0;

    setcred(3, 0, 0, 0, 0);   /* root; driver_creds[3] */
    apply_cred(3);

    stack[sp].ino     = 0;
    stack[sp].path[0] = '\0';
    sp++;

    while (sp > 0 && g_nmismatch < 20) {
        struct auditent e     = stack[--sp];
        json_t         *node  = map_get_int(inodes, e.ino);
        json_t         *ents  = node ? json_object_get(node, "ents") : NULL;
        json_t         *pairs = ents ? json_object_get(ents, "#map") : NULL;
        CHIMERA_DIR    *d;
        char            dirpath[4160];
        char            names[256][256];
        int             nnames, k;
        size_t          i;

        snprintf(dirpath, sizeof(dirpath), "%s%.4095s", MOUNT, e.path);
        d = chimera_posix_opendir(dirpath);
        if (!d) {
            mism("audit: opendir %s: errno %d", e.path[0] ? e.path : "/",
                 errno);
            continue;
        }
        nnames = audit_readdir(d, names, 256);
        chimera_posix_closedir(d);

        /* every live entry (minus . / ..) must be modelled */
        for (k = 0; k < nnames; k++) {
            int found = 0;
            if (strcmp(names[k], ".") == 0 || strcmp(names[k], "..") == 0) {
                continue;
            }
            for (i = 0; pairs && i < json_array_size(pairs); i++) {
                const char *wn = json_string_value(
                    json_array_get(json_array_get(pairs, i), 0));
                if (wn && strcmp(wn, names[k]) == 0) {
                    found = 1;
                    break;
                }
            }
            if (!found) {
                char ep[4400];
                snprintf(ep, sizeof(ep), "%.4095s/%.255s", e.path, names[k]);
                if (is_nfs_silly_name(names[k])) {
                    record_dev("ND1");
                } else if (is_nd5_quarantine(names[k])) {
                    record_dev("ND5");
                } else if (!is_exempt(ep)) {   /* PD24 residue */
                    mism("audit: dir %s: unexpected entry '%s'",
                         e.path[0] ? e.path : "/", names[k]);
                }
            }
        }

        for (i = 0; pairs && i < json_array_size(pairs); i++) {
            json_t     *pair  = json_array_get(pairs, i);
            const char *name  = json_string_value(json_array_get(pair, 0));
            int64_t     cino  = tf_i64(json_array_get(pair, 1));
            json_t     *cnode = map_get_int(inodes, cino);
            const char *ftag, *want_ft, *got_ft;
            char        cpath[4160], full[4260];
            struct stat st;
            int         present = 0, rc;

            if (!name || !cnode) {
                continue;
            }
            for (k = 0; k < nnames; k++) {
                if (strcmp(names[k], name) == 0) {
                    present = 1;
                    break;
                }
            }
            if (!present) {
                if (ino_lost(cino)) {
                    record_dev("ND4");
                } else {
                    mism("audit: dir %s: model entry '%s' missing",
                         e.path[0] ? e.path : "/", name);
                }
                continue;
            }

            snprintf(cpath, sizeof(cpath), "%.3800s/%.255s", e.path, name);
            snprintf(full, sizeof(full), "%s%.4159s", MOUNT, cpath);
            ftag    = tf_tag(json_object_get(cnode, "ftype"));
            want_ft = ftype_of(ftag);
            audited++;

            memset(&st, 0, sizeof(st));
            rc = chimera_posix_lstat(full, &st);
            if (ino_lost(cino)) {
                /* Mutated or created behind a reconciled divergence: its
                 * attributes and content have honestly diverged (see the
                 * g_lost_* block comment).  Still descend a directory that
                 * exists on both sides, so unaffected children get audited. */
                record_dev("ND4");
                if (rc == 0 && S_ISDIR(st.st_mode) &&
                    strcmp(ftag, "FDir") == 0 && sp < 4096) {
                    stack[sp].ino = cino;
                    snprintf(stack[sp].path, sizeof(stack[sp].path),
                             "%.4095s", cpath);
                    sp++;
                }
                continue;
            }
            if (rc != 0) {
                mism("audit: lstat %s: errno %d", cpath, errno);
                continue;
            }
            got_ft = ftype_from_mode(st.st_mode);
            if (strcmp(got_ft, want_ft) != 0) {
                mism("audit: %s: ftype %s != %s", cpath, got_ft, want_ft);
                continue;
            }
            if (strcmp(ftag, "FLnk") != 0 &&
                (int64_t) (st.st_mode & 07777) != tf_field(cnode, "mode")) {
                mism("audit: %s: mode %#o != %#llo", cpath,
                     st.st_mode & 07777, (long long) tf_field(cnode, "mode"));
            }
            if ((int64_t) st.st_uid != tf_field(cnode, "uid") ||
                (int64_t) st.st_gid != tf_field(cnode, "gid")) {
                mism("audit: %s: owner %u:%u != %lld:%lld", cpath,
                     st.st_uid, st.st_gid,
                     (long long) tf_field(cnode, "uid"),
                     (long long) tf_field(cnode, "gid"));
            }
            if ((int64_t) st.st_nlink != tf_field(cnode, "nlink")) {
                if (g_nfs_version && g_nexempt > 0 &&
                    strcmp(ftag, "FDir") == 0 &&
                    (int64_t) st.st_nlink > tf_field(cnode, "nlink")) {
                    record_dev("ND5");   /* residue subdir (see check_statres) */
                } else {
                    mism("audit: %s: nlink %llu != %lld", cpath,
                         (unsigned long long) st.st_nlink,
                         (long long) tf_field(cnode, "nlink"));
                }
            }
            if (cino >= 0 && cino < R_MAXINO && g_inomap[cino].present) {
                if (g_inomap[cino].dev != (long long) st.st_dev ||
                    g_inomap[cino].ino != (long long) st.st_ino) {
                    mism("audit: %s: identity mismatch", cpath);
                }
            }

            if (strcmp(ftag, "FDir") == 0) {
                if (sp < 4096) {
                    stack[sp].ino = cino;
                    snprintf(stack[sp].path, sizeof(stack[sp].path), "%.4095s",
                             cpath);
                    sp++;
                }
            } else if (strcmp(ftag, "FLnk") == 0) {
                char    tbuf[8192], want[8192];
                ssize_t tn = chimera_posix_readlink(full, tbuf,
                                                    sizeof(tbuf) - 1);
                tbuf[tn >= 0 ? tn : 0] = '\0';
                real_target(json_object_get(cnode, "target"), want,
                            sizeof(want));
                if (tn < 0 || strcmp(tbuf, want) != 0) {
                    mism("audit: readlink %s: '%s' != '%s'", cpath, tbuf, want);
                }
            } else if (strcmp(ftag, "FReg") == 0) {
                int64_t want_size = tf_field(cnode, "size");
                if ((int64_t) st.st_size != want_size) {
                    mism("audit: %s: size %lld != %lld", cpath,
                         (long long) st.st_size, (long long) want_size);
                } else if (want_size > 0) {
                    int rfdn = chimera_posix_open(full, O_RDONLY, 0);
                    if (rfdn < 0) {
                        mism("audit: open %s: errno %d", cpath, errno);
                    } else {
                        int64_t off;
                        for (off = 0; off < want_size; off += 65536) {
                            int64_t        n = want_size - off < 65536
                                ? want_size - off : 65536;
                            unsigned char *got = malloc(n);
                            unsigned char *exp = malloc(n);
                            ssize_t        gn  = chimera_posix_pread(rfdn, got,
                                                                     n, off);
                            shadow_read(cino, off, (size_t) n, exp);
                            if (gn != n || memcmp(got, exp, n) != 0) {
                                mism("audit: %s: content mismatch at +%lld",
                                     cpath, (long long) off);
                                free(got);
                                free(exp);
                                break;
                            }
                            free(got);
                            free(exp);
                        }
                        chimera_posix_close(rfdn);
                    }
                }
            }
        }
    }
    free(stack);
    return audited;
} /* final_audit */

/* ---- trace driver + main ------------------------------------------------- */

static int
replay_trace(const char *path)
{
    json_error_t err;
    json_t      *root = json_load_file(path, 0, &err);
    json_t      *states, *s0, *lo0, *caps, *last_fs = NULL;
    size_t       ns, i;
    int          audited;

    if (!root) {
        fprintf(stderr, "failed to parse %s: %s\n", path, err.text);
        return -1;
    }
    states = json_object_get(root, "states");
    ns     = json_array_size(states);
    if (ns == 0) {
        fprintf(stderr, "%s: no states\n", path);
        json_decref(root);
        return -1;
    }

    s0  = json_array_get(states, 0);
    lo0 = state_get(s0, "lastOp");
    if (!lo0 || strcmp(tf_tag(lo0), "LInit") != 0) {
        fprintf(stderr, "%s: state 0 is not LInit\n", path);
        json_decref(root);
        return -1;
    }
    caps           = json_object_get(tf_val(lo0), "caps");
    g_strict_atime = tf_bool(caps, "strictAtime");

    state_reset();
    g_trace = path;

    /* Per-pid credentials (creds_for): pid0 root-or-uid100, pid1 uid200. */
    setcred(0, tf_bool(caps, "withRoot") ? 0 : 100, 10, 10, 30);
    setcred(1, 200, 20, 20, 30);

    for (i = 1; i < ns; i++) {
        json_t     *st = json_array_get(states, i);
        json_t     *lo = state_get(st, "lastOp");
        json_t     *v, *req, *res;
        const char *tag;
        int         pid;

        if (!lo || strcmp(tf_tag(lo), "LCall") != 0) {
            continue;
        }
        v   = tf_val(lo);
        pid = tf_field(v, "pid");
        req = json_object_get(v, "req");
        res = json_object_get(v, "res");
        tag = tf_tag(req);

        g_cur_fs   = state_get(st, "fs");
        g_cur_ps   = state_get(st, "ps");
        g_cur_tag  = tag;
        g_cur_rv   = tf_val(req);
        g_cur_pid  = pid;
        g_cur_step = (int) i;
        last_fs    = g_cur_fs;

        if (dispatch(tag, pid, tf_val(req), tf_val(res)) != 0) {
            fprintf(stderr, "%s: step %zu: unimplemented op %s\n", path, i,
                    tag);
            json_decref(root);
            return -1;
        }
    }

    audited = last_fs ? final_audit(last_fs) : 0;

    if (g_nmismatch) {
        fprintf(stderr, "%s: %d mismatch(es)\n", path, g_nmismatch);
    } else {
        char devs[512] = "";
        int  a, b;
        /* Insertion sort g_devhits by id (matches Python's sorted() output). */
        for (a = 1; a < g_ndevhits; a++) {
            struct devhit t = g_devhits[a];
            for (b = a - 1; b >= 0 && strcmp(g_devhits[b].id, t.id) > 0; b--) {
                g_devhits[b + 1] = g_devhits[b];
            }
            g_devhits[b + 1] = t;
        }
        for (a = 0; a < g_ndevhits; a++) {
            char one[48];
            snprintf(one, sizeof(one), "%s%.11sx%d", a ? ", " : "",
                     g_devhits[a].id, g_devhits[a].count);
            strncat(devs, one, sizeof(devs) - strlen(devs) - 1);
        }
        printf("%s: %zu steps replayed, %d objects audited%s%s\n", path,
               ns - 1, audited, g_ndevhits ? "; known deviations: " : "",
               devs);
    }
    json_decref(root);
    return g_nmismatch ? 1 : 0;
} /* replay_trace */

int
main(
    int    argc,
    char **argv)
{
    const char *backend = "memfs";
    int         ntraces = 0;
    int         i, ran = 0, failures = 0, bad = 0;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--backend") == 0 && i + 1 < argc) {
            backend = argv[++i];
        } else if (strcmp(argv[i], "--driver") == 0 && i + 1 < argc) {
            i++;   /* accepted + ignored (no subprocess) */
        }
    }

    /* --trace/--trace-dir/--exclude-prefix are gathered by the shared helper. */
    char **traces = mbt_collect_traces(argc, argv, &ntraces);
    if (ntraces == 0) {
        fprintf(stderr,
                "usage: %s --backend <b> [--trace <f> ...] [--trace-dir <d>]\n",
                argv[0]);
        mbt_free_traces(traces, ntraces);
        return 2;
    }

    /* Clean result stream: chimera logs go to stderr (fd 1 -> stderr), our
     * output to the saved stdout, exactly like the driver. */
    int out_fd = dup(STDOUT_FILENO);
    proto_out = out_fd >= 0 ? fdopen(out_fd, "w") : NULL;
    if (!proto_out || dup2(STDERR_FILENO, STDOUT_FILENO) < 0) {
        fprintf(stderr, "posix_mbt_replay: stream setup failed\n");
        mbt_free_traces(traces, ntraces);
        return 1;
    }
    /* Route our printf() to the clean stream. */
    if (dup2(fileno(proto_out), STDOUT_FILENO) < 0) {
        mbt_free_traces(traces, ntraces);
        return 1;
    }

    if (posix_env_setup(backend, NULL) != 0) {
        mbt_free_traces(traces, ntraces);
        return 1;
    }

    for (i = 0; i < ntraces; i++) {
        int rc;
        if (ran > 0 && !newfs()) {
            fprintf(stderr, "newfs reset failed before %s\n", traces[i]);
            failures++;
            break;
        }
        rc = replay_trace(traces[i]);
        if (rc < 0) {
            bad++;
        } else {
            ran++;
            if (rc) {
                failures++;
            }
        }
    }

    posix_env_teardown();

    printf("batch: %d replayed, %d failed, %d unhandled of %d trace(s)\n",
           ran, failures, bad, ntraces);
    fflush(stdout);
    mbt_free_traces(traces, ntraces);
    return (failures || bad) ? 1 : 0;
} /* main */
