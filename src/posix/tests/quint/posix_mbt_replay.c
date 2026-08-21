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
static int           g_sidmap[R_MAXSID];    /* model sid       -> real sid    */

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
        g_sidmap[i] = -1;
    }
    memset(g_inomap, 0, sizeof(g_inomap));
    memset(g_timemap, 0, sizeof(g_timemap));
    for (i = 0; i < R_MAXINO; i++) {
        free(g_shadow[i].buf);
        g_shadow[i].buf = NULL;
        g_shadow[i].len = g_shadow[i].cap = 0;
    }
    g_nmismatch = 0;
} /* state_reset */

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
        g_fdmap[pid][mfd] = real;
    }
} /* set_fd */

static int
rsid(int msid)
{
    return (msid >= 0 && msid < R_MAXSID) ? g_sidmap[msid] : -1;
} /* rsid */

static void
mism(
    const char *fmt,
    ...)
{
    va_list ap;

    printf("MISMATCH [%s] ", g_trace);
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

/* ---- oracle -------------------------------------------------------------- */

/* True if the errno matches (proceed with success-path checks). */
static int
check_status(
    int64_t expected,
    json_t *res)
{
    int64_t actual = tf_field(res, "err");

    if (actual == expected) {
        return 1;
    }
    mism("errno: expected %lld, got %lld", (long long) expected,
         (long long) actual);
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

static void
check_time(
    int64_t mino,
    int     field,           /* 0 atime, 1 mtime, 2 ctime */
    int64_t abstract,
    json_t *wire_arr)
{
    long long    wsec  = tf_i64(json_array_get(wire_arr, 0));
    long long    wnsec = tf_i64(json_array_get(wire_arr, 1));
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
        if (wsec != t->sec || wnsec != t->nsec) {
            mism("time: model instant unchanged (%lld) but wire moved "
                 "%lld.%lld -> %lld.%lld", (long long) abstract,
                 t->sec, t->nsec, wsec, wnsec);
        }
    } else if (abstract > t->abstract) {
        if (wsec < t->sec || (wsec == t->sec && wnsec < t->nsec)) {
            mism("time: model advanced but wire went backwards "
                 "%lld.%lld -> %lld.%lld", t->sec, t->nsec, wsec, wnsec);
        }
        t->abstract = abstract;
        t->sec      = wsec;
        t->nsec     = wnsec;
    } else {
        mism("time: model instant went backwards (harness bug?)");
    }
} /* check_time */

/* Compare a stat reply against the model's SStatR payload (res_v). */
static void
check_statres(
    json_t *rv,      /* model result value */
    json_t *res)     /* driver reply */
{
    json_t       *ftype   = json_object_get(rv, "ftype");
    const char   *ftag    = tf_tag(ftype);
    const char   *want_ft = ftype_of(ftag);
    const char   *got_ft  = json_string_value(json_object_get(res, "ftype"));
    int64_t       mino    = tf_field(rv, "ino");
    long long     dev, ino;
    struct ident *known;

    if (!got_ft || strcmp(got_ft, want_ft) != 0) {
        mism("ftype: expected %s, got %s", want_ft, got_ft ? got_ft : "(nil)");
    }
    if (strcmp(ftag, "FLnk") != 0) {
        int64_t wmode = tf_field(rv, "mode");
        int64_t gmode = tf_field(res, "mode");
        if (gmode != wmode) {
            mism("mode: expected %#llo, got %#llo",
                 (long long) wmode, (long long) gmode);
        }
    }
    if (tf_field(res, "uid") != tf_field(rv, "uid")) {
        mism("uid: expected %lld, got %lld", (long long) tf_field(rv, "uid"),
             (long long) tf_field(res, "uid"));
    }
    if (tf_field(res, "gid") != tf_field(rv, "gid")) {
        mism("gid: expected %lld, got %lld", (long long) tf_field(rv, "gid"),
             (long long) tf_field(res, "gid"));
    }
    if (tf_field(res, "nlink") != tf_field(rv, "nlink")) {
        mism("nlink: expected %lld, got %lld",
             (long long) tf_field(rv, "nlink"),
             (long long) tf_field(res, "nlink"));
    }
    if (strcmp(ftag, "FReg") == 0) {
        int64_t want = tf_field(rv, "sizeB");
        if (tf_field(res, "size") != want) {
            mism("size: expected %lld, got %lld", (long long) want,
                 (long long) tf_field(res, "size"));
        }
    } else if (strcmp(ftag, "FLnk") == 0 && g_cur_fs) {
        json_t *node = map_get_int(json_object_get(g_cur_fs, "inodes"), mino);
        if (node) {
            char want[8192];
            real_target(json_object_get(node, "target"), want, sizeof(want));
            if (tf_field(res, "size") != (int64_t) strlen(want)) {
                mism("symlink size: expected %zu, got %lld", strlen(want),
                     (long long) tf_field(res, "size"));
            }
        }
    }

    dev = tf_field(res, "dev");
    ino = tf_field(res, "ino");
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

    check_time(mino, 0, tf_field(rv, "atime"), json_object_get(res, "atime"));
    check_time(mino, 1, tf_field(rv, "mtime"), json_object_get(res, "mtime"));
    check_time(mino, 2, tf_field(rv, "ctime"), json_object_get(res, "ctime"));
} /* check_statres */

/* ---- request building + in-process dispatch ------------------------------ */

/* Call the reused driver engine with a freshly built request object.  Returns
 * the reply (caller decrefs).  `req` is consumed. */
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
    json_t *req, *res;

    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    if (dfd == -1) {
        req = req_new("open", pid);
    } else {
        req = req_new("openat", pid);
        json_object_set_new(req, "dirfd", json_integer(rfd(pid, dfd)));
    }
    json_object_set_new(req, "path", json_string(path));
    json_object_set_new(req, "flags", json_integer(flags));
    json_object_set_new(req, "mode", json_integer(tf_field(fl, "mode")));
    res = call(req);

    if (check_status(tf_field(res_v, "e"), res) && tf_field(res_v, "e") == 0) {
        set_fd(pid, tf_field(res_v, "fd"), tf_field(res, "ret"));
        if (tf_bool(fl, "trunc")) {
            int64_t ino = model_ino_of_fd(pid, tf_field(res_v, "fd"));
            if (ino >= 0) {
                shadow_resize(ino, 0);
            }
        }
    }
    json_decref(res);
} /* op_open */

static void
op_close(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    json_t *req = req_new("close", pid);
    json_t *res;

    json_object_set_new(req, "fd", json_integer(rfd(pid, tf_field(rv, "fd"))));
    res = call(req);
    if (check_status(tf_field(res_v, "e"), res) && tf_field(res_v, "e") == 0) {
        set_fd(pid, tf_field(rv, "fd"), BADFD);
    }
    json_decref(res);
} /* op_close */

static void
op_dup(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    json_t *req = req_new("dup", pid);
    json_t *res;

    json_object_set_new(req, "fd", json_integer(rfd(pid, tf_field(rv, "fd"))));
    res = call(req);
    if (check_status(tf_field(res_v, "e"), res) && tf_field(res_v, "e") == 0) {
        set_fd(pid, tf_field(res_v, "fd"), tf_field(res, "ret"));
    }
    json_decref(res);
} /* op_dup */

static void
op_lseek(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    const char *whmap[] = { 0 };
    const char *wh      = tf_tag(json_object_get(rv, "wh"));
    const char *w       = "set";
    json_t     *req     = req_new("lseek", pid);
    json_t     *res;

    (void) whmap;
    if (strcmp(wh, "WCur") == 0) {
        w = "cur";
    } else if (strcmp(wh, "WEnd") == 0) {
        w = "end";
    } else if (strcmp(wh, "WData") == 0) {
        w = "data";
    } else if (strcmp(wh, "WHole") == 0) {
        w = "hole";
    }
    json_object_set_new(req, "fd", json_integer(rfd(pid, tf_field(rv, "fd"))));
    json_object_set_new(req, "off", json_integer(tf_field(rv, "off")));
    json_object_set_new(req, "whence", json_string(w));
    res = call(req);
    if (check_status(tf_field(res_v, "e"), res) && tf_field(res_v, "e") == 0) {
        int64_t want = tf_field(res_v, "off");
        if (tf_field(res, "ret") != want) {
            mism("lseek: expected offset %lld, got %lld", (long long) want,
                 (long long) tf_field(res, "ret"));
        }
    }
    json_decref(res);
} /* op_lseek */

/* Shared read verify for read/pread/readv/preadv. */
static void
check_read(
    json_t *res_v,
    json_t *res)
{
    if (check_status(tf_field(res_v, "e"), res) && tf_field(res_v, "e") == 0) {
        int64_t        count = tf_field(res_v, "n");
        const char    *b64   = json_string_value(json_object_get(res, "data"));
        unsigned char *got   = malloc(count > 0 ? count : 1);
        unsigned char *exp   = malloc(count > 0 ? count : 1);
        int            gl    = b64_decode(b64 ? b64 : "", got,
                                          count > 0 ? count : 1);

        if (tf_field(res, "ret") != count) {
            mism("read count: expected %lld, got %lld", (long long) count,
                 (long long) tf_field(res, "ret"));
        }
        shadow_read(tf_field(res_v, "ino"), tf_field(res_v, "off"),
                    (size_t) count, exp);
        if (gl != count || memcmp(got, exp, count) != 0) {
            mism("read data mismatch at ino %lld off %lld len %lld",
                 (long long) tf_field(res_v, "ino"),
                 (long long) tf_field(res_v, "off"), (long long) count);
        }
        free(got);
        free(exp);
    }
} /* check_read */

static void
op_read_family(
    int         pid,
    json_t     *rv,
    json_t     *res_v,
    const char *op,
    int         positioned)
{
    json_t *req = req_new(op, pid);
    json_t *res;

    json_object_set_new(req, "fd", json_integer(rfd(pid, tf_field(rv, "fd"))));
    json_object_set_new(req, "len", json_integer(tf_field(rv, "len")));
    if (positioned) {
        json_object_set_new(req, "off", json_integer(tf_field(rv, "off")));
    }
    res = call(req);
    check_read(res_v, res);
    json_decref(res);
} /* op_read_family */

static void
op_write_family(
    int         pid,
    json_t     *rv,
    json_t     *res_v,
    const char *op,
    int         positioned)
{
    int64_t        off  = tf_field(res_v, "off");
    int64_t        len  = tf_field(rv, "len");
    int64_t        pat  = tf_field(rv, "pat");
    unsigned char *data = malloc(len > 0 ? len : 1);
    char          *enc;
    json_t        *req, *res;
    int64_t        i;

    for (i = 0; i < len; i++) {
        data[i] = pat_byte(pat, off + i);
    }
    enc = b64_encode(data, (size_t) len);
    req = req_new(op, pid);
    json_object_set_new(req, "fd", json_integer(rfd(pid, tf_field(rv, "fd"))));
    json_object_set_new(req, "data", json_string(enc ? enc : ""));
    if (positioned) {
        json_object_set_new(req, "off", json_integer(tf_field(rv, "off")));
    }
    res = call(req);
    if (check_status(tf_field(res_v, "e"), res) && tf_field(res_v, "e") == 0) {
        if (tf_field(res, "ret") != tf_field(res_v, "n")) {
            mism("write count: expected %lld, got %lld",
                 (long long) tf_field(res_v, "n"),
                 (long long) tf_field(res, "ret"));
        }
        shadow_apply(tf_field(res_v, "ino"), off, data, (size_t) len);
    }
    free(data);
    free(enc);
    json_decref(res);
} /* op_write_family */

static void
op_truncate(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    char    path[8192];
    json_t *req = req_new("truncate", pid);
    json_t *res;

    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    json_object_set_new(req, "path", json_string(path));
    json_object_set_new(req, "len", json_integer(tf_field(rv, "len")));
    res = call(req);
    check_status(tf_field(res_v, "e"), res);
    if (tf_field(res_v, "e") == 0) {
        int64_t ino = path_ino(g_cur_fs, json_object_get(
                                   json_object_get(rv, "pth"), "comps"));
        if (ino >= 0) {
            shadow_resize(ino, tf_field(rv, "len"));
        }
    }
    json_decref(res);
} /* op_truncate */

static void
op_ftruncate(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    json_t *req = req_new("ftruncate", pid);
    json_t *res;

    json_object_set_new(req, "fd", json_integer(rfd(pid, tf_field(rv, "fd"))));
    json_object_set_new(req, "len", json_integer(tf_field(rv, "len")));
    res = call(req);
    check_status(tf_field(res_v, "e"), res);
    if (tf_field(res_v, "e") == 0) {
        int64_t ino = model_ino_of_fd(pid, tf_field(rv, "fd"));
        if (ino >= 0) {
            shadow_resize(ino, tf_field(rv, "len"));
        }
    }
    json_decref(res);
} /* op_ftruncate */

static void
op_stat(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    int64_t dfd = tf_field(rv, "dfd");
    char    path[8192];
    json_t *req, *res;

    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    if (dfd == -1) {
        req = req_new("stat", pid);
    } else {
        req = req_new("fstatat", pid);
        json_object_set_new(req, "dirfd", json_integer(rfd(pid, dfd)));
    }
    json_object_set_new(req, "path", json_string(path));
    json_object_set_new(req, "follow", json_boolean(tf_bool(rv, "follow")));
    res = call(req);
    if (check_status(tf_field(res_v, "e"), res) && tf_field(res_v, "e") == 0) {
        check_statres(res_v, res);
    }
    json_decref(res);
} /* op_stat */

static void
op_fstat(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    json_t *req = req_new("fstat", pid);
    json_t *res;

    json_object_set_new(req, "fd", json_integer(rfd(pid, tf_field(rv, "fd"))));
    res = call(req);
    if (check_status(tf_field(res_v, "e"), res) && tf_field(res_v, "e") == 0) {
        check_statres(res_v, res);
    }
    json_decref(res);
} /* op_fstat */

/* statfs/statvfs/fstatfs/fstatvfs: only the errno is asserted. */
static void
op_statfs_family(
    int         pid,
    json_t     *rv,
    json_t     *res_v,
    const char *op,
    int         byfd)
{
    json_t *req = req_new(op, pid);
    json_t *res;

    if (byfd) {
        json_object_set_new(req, "fd",
                            json_integer(rfd(pid, tf_field(rv, "fd"))));
    } else {
        char path[8192];
        real_path(json_object_get(rv, "pth"), path, sizeof(path));
        json_object_set_new(req, "path", json_string(path));
    }
    res = call(req);
    check_status(tf_field(res_v, "e"), res);
    json_decref(res);
} /* op_statfs_family */

static void
op_mkdir(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    int64_t dfd = tf_field(rv, "dfd");
    char    path[8192];
    json_t *req, *res;

    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    if (dfd == -1) {
        req = req_new("mkdir", pid);
    } else {
        req = req_new("mkdirat", pid);
        json_object_set_new(req, "dirfd", json_integer(rfd(pid, dfd)));
    }
    json_object_set_new(req, "path", json_string(path));
    json_object_set_new(req, "mode", json_integer(tf_field(rv, "mode")));
    res = call(req);
    check_status(tf_field(res_v, "e"), res);
    json_decref(res);
} /* op_mkdir */

static void
op_mknod(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    const char *ft = ftype_of(tf_tag(json_object_get(rv, "ft")));
    char        path[8192];
    json_t     *req = req_new("mknod", pid);
    json_t     *res;

    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    json_object_set_new(req, "path", json_string(path));
    json_object_set_new(req, "mode", json_integer(tf_field(rv, "mode")));
    json_object_set_new(req, "ftype", json_string(ft));
    res = call(req);
    check_status(tf_field(res_v, "e"), res);
    json_decref(res);
} /* op_mknod */

static void
op_symlink(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    char    tgt[8192], path[8192];
    json_t *req = req_new("symlink", pid);
    json_t *res;

    real_target(json_object_get(rv, "tgt"), tgt, sizeof(tgt));
    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    json_object_set_new(req, "target", json_string(tgt));
    json_object_set_new(req, "path", json_string(path));
    res = call(req);
    check_status(tf_field(res_v, "e"), res);
    json_decref(res);
} /* op_symlink */

static void
op_link(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    char    o[8192], n[8192];
    json_t *req = req_new("link", pid);
    json_t *res;

    real_path(json_object_get(rv, "pthOld"), o, sizeof(o));
    real_path(json_object_get(rv, "pthNew"), n, sizeof(n));
    json_object_set_new(req, "old", json_string(o));
    json_object_set_new(req, "new", json_string(n));
    json_object_set_new(req, "follow", json_boolean(tf_bool(rv, "followOld")));
    res = call(req);
    check_status(tf_field(res_v, "e"), res);
    json_decref(res);
} /* op_link */

static void
op_unlink(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    int64_t dfd = tf_field(rv, "dfd");
    char    path[8192];
    json_t *req, *res;

    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    if (dfd == -1) {
        req = req_new("unlink", pid);
    } else {
        req = req_new("unlinkat", pid);
        json_object_set_new(req, "dirfd", json_integer(rfd(pid, dfd)));
        json_object_set_new(req, "rmdir", json_false());
    }
    json_object_set_new(req, "path", json_string(path));
    res = call(req);
    check_status(tf_field(res_v, "e"), res);
    json_decref(res);
} /* op_unlink */

static void
op_rmdir(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    int64_t dfd = tf_field(rv, "dfd");
    char    path[8192];
    json_t *req, *res;

    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    if (dfd == -1) {
        req = req_new("rmdir", pid);
    } else {
        req = req_new("unlinkat", pid);
        json_object_set_new(req, "dirfd", json_integer(rfd(pid, dfd)));
        json_object_set_new(req, "rmdir", json_true());
    }
    json_object_set_new(req, "path", json_string(path));
    res = call(req);
    check_status(tf_field(res_v, "e"), res);
    json_decref(res);
} /* op_rmdir */

static void
op_rename(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    char    o[8192], n[8192];
    json_t *req = req_new("rename", pid);
    json_t *res;

    real_path(json_object_get(rv, "pthOld"), o, sizeof(o));
    real_path(json_object_get(rv, "pthNew"), n, sizeof(n));
    json_object_set_new(req, "old", json_string(o));
    json_object_set_new(req, "new", json_string(n));
    res = call(req);
    check_status(tf_field(res_v, "e"), res);
    json_decref(res);
} /* op_rename */

static void
op_readlink(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    char    path[8192];
    json_t *req = req_new("readlink", pid);
    json_t *res;

    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    json_object_set_new(req, "path", json_string(path));
    res = call(req);
    if (check_status(tf_field(res_v, "e"), res) && tf_field(res_v, "e") == 0) {
        char        want[8192];
        const char *got = json_string_value(json_object_get(res, "target"));
        real_target(json_object_get(res_v, "tgt"), want, sizeof(want));
        if (!got || strcmp(got, want) != 0) {
            mism("readlink: expected '%s', got '%s'", want, got ? got : "(nil)");
        }
    }
    json_decref(res);
} /* op_readlink */

static void
op_opendir(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    char    path[8192];
    json_t *req = req_new("opendir", pid);
    json_t *res;

    real_path(json_object_get(rv, "pth"), path, sizeof(path));
    json_object_set_new(req, "path", json_string(path));
    res = call(req);
    if (check_status(tf_field(res_v, "e"), res) && tf_field(res_v, "e") == 0) {
        int64_t msid = tf_field(res_v, "sid");
        if (msid >= 0 && msid < R_MAXSID) {
            g_sidmap[msid] = tf_field(res, "ret");
        }
    }
    json_decref(res);
} /* op_opendir */

static void
op_readdir(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    json_t *req = req_new("readdir", pid);
    json_t *res, *names, *want;
    size_t  i, j;

    json_object_set_new(req, "sid", json_integer(rsid(tf_field(rv, "sid"))));
    res = call(req);
    if (check_status(tf_field(res_v, "e"), res) && tf_field(res_v, "e") == 0) {
        names = json_object_get(res, "names");                 /* got names   */
        want  = json_object_get(res_v, "names");               /* model #set  */
        if (json_is_object(want)) {
            json_t *s = json_object_get(want, "#set");
            if (s) {
                want = s;
            }
        }
        /* every model name must be present, and every present name (minus
         * . / ..) must be in the model set: compare as sets */
        for (i = 0; want && i < json_array_size(want); i++) {
            const char *wn    = json_string_value(json_array_get(want, i));
            int         found = 0;
            for (j = 0; names && j < json_array_size(names); j++) {
                const char *gn = json_string_value(json_array_get(names, j));
                if (gn && wn && strcmp(gn, wn) == 0) {
                    found = 1;
                    break;
                }
            }
            if (!found) {
                mism("readdir: model name '%s' missing", wn ? wn : "?");
            }
        }
        for (j = 0; names && j < json_array_size(names); j++) {
            const char *gn = json_string_value(json_array_get(names, j));
            int         ok;
            if (!gn || strcmp(gn, ".") == 0 || strcmp(gn, "..") == 0) {
                continue;
            }
            ok = 0;
            for (i = 0; want && i < json_array_size(want); i++) {
                const char *wn = json_string_value(json_array_get(want, i));
                if (wn && strcmp(wn, gn) == 0) {
                    ok = 1;
                    break;
                }
            }
            if (!ok) {
                mism("readdir: unexpected name '%s'", gn);
            }
        }
    }
    json_decref(res);
} /* op_readdir */

static void
op_dir_simple(
    int         pid,
    json_t     *rv,
    json_t     *res_v,
    const char *op)
{
    json_t *req = req_new(op, pid);
    json_t *res;

    json_object_set_new(req, "sid", json_integer(rsid(tf_field(rv, "sid"))));
    if (strcmp(op, "seekdir") == 0) {
        json_object_set_new(req, "loc", json_integer(tf_field(rv, "loc")));
    }
    res = call(req);
    check_status(tf_field(res_v, "e"), res);
    if (strcmp(op, "closedir") == 0 && tf_field(res_v, "e") == 0) {
        int64_t msid = tf_field(rv, "sid");
        if (msid >= 0 && msid < R_MAXSID) {
            g_sidmap[msid] = -1;
        }
    }
    json_decref(res);
} /* op_dir_simple */

static void
op_copy_range(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    json_t *req = req_new("copy_range", pid);
    json_t *res;

    json_object_set_new(req, "fd_in",
                        json_integer(rfd(pid, tf_field(rv, "fdIn"))));
    json_object_set_new(req, "off_in", json_integer(tf_field(rv, "offIn")));
    json_object_set_new(req, "fd_out",
                        json_integer(rfd(pid, tf_field(rv, "fdOut"))));
    json_object_set_new(req, "off_out", json_integer(tf_field(rv, "offOut")));
    json_object_set_new(req, "len", json_integer(tf_field(rv, "len")));
    res = call(req);
    if (check_status(tf_field(res_v, "e"), res) && tf_field(res_v, "e") == 0) {
        int64_t n = tf_field(res_v, "n");
        if (tf_field(res, "ret") != n) {
            mism("copy_file_range: expected %lld, got %lld", (long long) n,
                 (long long) tf_field(res, "ret"));
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
    json_decref(res);
} /* op_copy_range */

static void
op_clone_range(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    json_t *req = req_new("clone_range", pid);
    json_t *res;

    json_object_set_new(req, "dst_fd",
                        json_integer(rfd(pid, tf_field(rv, "fdDst"))));
    json_object_set_new(req, "dst_off", json_integer(tf_field(rv, "offDst")));
    json_object_set_new(req, "src_fd",
                        json_integer(rfd(pid, tf_field(rv, "fdSrc"))));
    json_object_set_new(req, "src_off", json_integer(tf_field(rv, "offSrc")));
    json_object_set_new(req, "len", json_integer(tf_field(rv, "len")));
    res = call(req);
    if (check_status(tf_field(res_v, "e"), res) && tf_field(res_v, "e") == 0) {
        int64_t si  = model_ino_of_fd(pid, tf_field(rv, "fdSrc"));
        int64_t di  = model_ino_of_fd(pid, tf_field(rv, "fdDst"));
        int64_t len = tf_field(rv, "len");
        if (si >= 0 && di >= 0) {
            unsigned char *tmp = malloc(len > 0 ? len : 1);
            shadow_read(si, tf_field(rv, "offSrc"), (size_t) len, tmp);
            shadow_apply(di, tf_field(rv, "offDst"), tmp, (size_t) len);
            free(tmp);
        }
    }
    json_decref(res);
} /* op_clone_range */

static void
op_fallocate(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    int64_t mode = tf_field(rv, "mode");
    json_t *req  = req_new("fallocate", pid);
    json_t *res;

    json_object_set_new(req, "fd", json_integer(rfd(pid, tf_field(rv, "fd"))));
    json_object_set_new(req, "mode", json_integer(mode));
    json_object_set_new(req, "off", json_integer(tf_field(rv, "off")));
    json_object_set_new(req, "len", json_integer(tf_field(rv, "len")));
    res = call(req);
    check_status(tf_field(res_v, "e"), res);
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
    json_decref(res);
} /* op_fallocate */

static void
op_fsync(
    int     pid,
    json_t *rv,
    json_t *res_v)
{
    const char *op  = tf_bool(rv, "dataOnly") ? "fdatasync" : "fsync";
    json_t     *req = req_new(op, pid);
    json_t     *res;

    json_object_set_new(req, "fd", json_integer(rfd(pid, tf_field(rv, "fd"))));
    res = call(req);
    check_status(tf_field(res_v, "e"), res);
    json_decref(res);
} /* op_fsync */

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
        op_read_family(pid, rv, res_v, "read", 0);
    } else if (strcmp(tag, "RPread") == 0) {
        op_read_family(pid, rv, res_v, "pread", 1);
    } else if (strcmp(tag, "RReadv") == 0) {
        op_read_family(pid, rv, res_v, "readv", 0);
    } else if (strcmp(tag, "RPreadv") == 0) {
        op_read_family(pid, rv, res_v, "preadv", 1);
    } else if (strcmp(tag, "RWrite") == 0) {
        op_write_family(pid, rv, res_v, "write", 0);
    } else if (strcmp(tag, "RPwrite") == 0) {
        op_write_family(pid, rv, res_v, "pwrite", 1);
    } else if (strcmp(tag, "RWritev") == 0) {
        op_write_family(pid, rv, res_v, "writev", 0);
    } else if (strcmp(tag, "RPwritev") == 0) {
        op_write_family(pid, rv, res_v, "pwritev", 1);
    } else if (strcmp(tag, "RTruncate") == 0) {
        op_truncate(pid, rv, res_v);
    } else if (strcmp(tag, "RFtruncate") == 0) {
        op_ftruncate(pid, rv, res_v);
    } else if (strcmp(tag, "RStat") == 0) {
        op_stat(pid, rv, res_v);
    } else if (strcmp(tag, "RFstat") == 0) {
        op_fstat(pid, rv, res_v);
    } else if (strcmp(tag, "RStatfs") == 0) {
        op_statfs_family(pid, rv, res_v, "statfs", 0);
    } else if (strcmp(tag, "RStatvfs") == 0) {
        op_statfs_family(pid, rv, res_v, "statvfs", 0);
    } else if (strcmp(tag, "RFstatfs") == 0) {
        op_statfs_family(pid, rv, res_v, "fstatfs", 1);
    } else if (strcmp(tag, "RFstatvfs") == 0) {
        op_statfs_family(pid, rv, res_v, "fstatvfs", 1);
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
        op_dir_simple(pid, rv, res_v, "rewinddir");
    } else if (strcmp(tag, "RTelldir") == 0) {
        op_dir_simple(pid, rv, res_v, "telldir");
    } else if (strcmp(tag, "RSeekdir") == 0) {
        op_dir_simple(pid, rv, res_v, "seekdir");
    } else if (strcmp(tag, "RClosedir") == 0) {
        op_dir_simple(pid, rv, res_v, "closedir");
    } else if (strcmp(tag, "RCopyRange") == 0) {
        op_copy_range(pid, rv, res_v);
    } else if (strcmp(tag, "RCloneRange") == 0) {
        op_clone_range(pid, rv, res_v);
    } else if (strcmp(tag, "RFallocate") == 0) {
        op_fallocate(pid, rv, res_v);
    } else if (strcmp(tag, "RFsync") == 0) {
        op_fsync(pid, rv, res_v);
    } else {
        return -1;   /* unimplemented op (out of the deviation-free scope) */
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

/* Reset the driver to a fresh empty filesystem (driver "newfs" op). */
static int
newfs(void)
{
    json_t *res = call(req_new("newfs", 0));
    int     ok  = res && tf_field(res, "err") == 0;

    json_decref(res);
    return ok;
} /* newfs */

/* ---- end-of-trace audit -------------------------------------------------- */

struct auditent { int64_t ino; char path[4096]; };

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

    setcred(3, 0, 0, 0, 0);   /* gids ignored; root */

    stack[sp].ino     = 0;
    stack[sp].path[0] = '\0';
    sp++;

    while (sp > 0 && g_nmismatch < 20) {
        struct auditent e     = stack[--sp];
        json_t         *node  = map_get_int(inodes, e.ino);
        json_t         *ents  = node ? json_object_get(node, "ents") : NULL;
        json_t         *pairs = ents ? json_object_get(ents, "#map") : NULL;
        json_t         *req, *res, *names;
        char            dirpath[4160];
        int             sid;
        size_t          i, j;

        snprintf(dirpath, sizeof(dirpath), "%s%.4095s", MOUNT, e.path);

        req = req_new("opendir", 3);
        json_object_set_new(req, "path", json_string(dirpath));
        res = call(req);
        if (tf_field(res, "err") != 0) {
            mism("audit: opendir %s: errno %lld", e.path[0] ? e.path : "/",
                 (long long) tf_field(res, "err"));
            json_decref(res);
            continue;
        }
        sid = tf_field(res, "ret");
        json_decref(res);

        req = req_new("readdir", 3);
        json_object_set_new(req, "sid", json_integer(sid));
        res   = call(req);
        names = json_incref(json_object_get(res, "names"));
        json_decref(res);

        req = req_new("closedir", 3);
        json_object_set_new(req, "sid", json_integer(sid));
        json_decref(call(req));

        /* every live entry (minus . / ..) must be in the model, and vice
         * versa */
        for (j = 0; names && j < json_array_size(names); j++) {
            const char *gn = json_string_value(json_array_get(names, j));
            int         ok = 0;
            if (!gn || strcmp(gn, ".") == 0 || strcmp(gn, "..") == 0) {
                continue;
            }
            for (i = 0; pairs && i < json_array_size(pairs); i++) {
                const char *wn = json_string_value(
                    json_array_get(json_array_get(pairs, i), 0));
                if (wn && strcmp(wn, gn) == 0) {
                    ok = 1;
                    break;
                }
            }
            if (!ok) {
                mism("audit: dir %s: unexpected entry '%s'",
                     e.path[0] ? e.path : "/", gn);
            }
        }

        for (i = 0; pairs && i < json_array_size(pairs); i++) {
            json_t     *pair  = json_array_get(pairs, i);
            const char *name  = json_string_value(json_array_get(pair, 0));
            int64_t     cino  = tf_i64(json_array_get(pair, 1));
            json_t     *cnode = map_get_int(inodes, cino);
            const char *ftag;
            const char *want_ft, *got_ft;
            char        cpath[4160], full[4260];
            int         present = 0;

            if (!name || !cnode) {
                continue;
            }
            for (j = 0; names && j < json_array_size(names); j++) {
                const char *gn = json_string_value(json_array_get(names, j));
                if (gn && strcmp(gn, name) == 0) {
                    present = 1;
                    break;
                }
            }
            if (!present) {
                mism("audit: dir %s: model entry '%s' missing",
                     e.path[0] ? e.path : "/", name);
                continue;
            }

            snprintf(cpath, sizeof(cpath), "%.3800s/%.255s", e.path, name);
            snprintf(full, sizeof(full), "%s%.4159s", MOUNT, cpath);
            ftag    = tf_tag(json_object_get(cnode, "ftype"));
            want_ft = ftype_of(ftag);
            audited++;

            req = req_new("stat", 3);
            json_object_set_new(req, "path", json_string(full));
            json_object_set_new(req, "follow", json_false());
            res = call(req);
            if (tf_field(res, "err") != 0) {
                mism("audit: lstat %s: errno %lld", cpath,
                     (long long) tf_field(res, "err"));
                json_decref(res);
                continue;
            }
            got_ft = json_string_value(json_object_get(res, "ftype"));
            if (!got_ft || strcmp(got_ft, want_ft) != 0) {
                mism("audit: %s: ftype %s != %s", cpath,
                     got_ft ? got_ft : "(nil)", want_ft);
                json_decref(res);
                continue;
            }
            if (strcmp(ftag, "FLnk") != 0 &&
                tf_field(res, "mode") != tf_field(cnode, "mode")) {
                mism("audit: %s: mode %#llo != %#llo", cpath,
                     (long long) tf_field(res, "mode"),
                     (long long) tf_field(cnode, "mode"));
            }
            if (tf_field(res, "uid") != tf_field(cnode, "uid") ||
                tf_field(res, "gid") != tf_field(cnode, "gid")) {
                mism("audit: %s: owner %lld:%lld != %lld:%lld", cpath,
                     (long long) tf_field(res, "uid"),
                     (long long) tf_field(res, "gid"),
                     (long long) tf_field(cnode, "uid"),
                     (long long) tf_field(cnode, "gid"));
            }
            if (tf_field(res, "nlink") != tf_field(cnode, "nlink")) {
                mism("audit: %s: nlink %lld != %lld", cpath,
                     (long long) tf_field(res, "nlink"),
                     (long long) tf_field(cnode, "nlink"));
            }
            if (cino >= 0 && cino < R_MAXINO && g_inomap[cino].present) {
                if (g_inomap[cino].dev != tf_field(res, "dev") ||
                    g_inomap[cino].ino != tf_field(res, "ino")) {
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
                json_t *rl = req_new("readlink", 3);
                char    want[8192];
                json_t *rr;
                json_object_set_new(rl, "path", json_string(full));
                rr = call(rl);
                real_target(json_object_get(cnode, "target"), want,
                            sizeof(want));
                {
                    const char *got = json_string_value(
                        json_object_get(rr, "target"));
                    if (tf_field(rr, "err") != 0 || !got ||
                        strcmp(got, want) != 0) {
                        mism("audit: readlink %s: '%s' != '%s'", cpath,
                             got ? got : "(nil)", want);
                    }
                }
                json_decref(rr);
            } else if (strcmp(ftag, "FReg") == 0) {
                int64_t want_size = tf_field(cnode, "size");
                if (tf_field(res, "size") != want_size) {
                    mism("audit: %s: size %lld != %lld", cpath,
                         (long long) tf_field(res, "size"),
                         (long long) want_size);
                } else if (want_size > 0) {
                    json_t *of = req_new("open", 3);
                    json_t *ofr;
                    json_object_set_new(of, "path", json_string(full));
                    json_object_set_new(of, "flags", json_integer(O_RDONLY));
                    json_object_set_new(of, "mode", json_integer(0));
                    ofr = call(of);
                    if (tf_field(ofr, "err") != 0) {
                        mism("audit: open %s: errno %lld", cpath,
                             (long long) tf_field(ofr, "err"));
                    } else {
                        int     rfdn = tf_field(ofr, "ret");
                        int64_t off;
                        for (off = 0; off < want_size; off += 65536) {
                            int64_t        n = want_size - off < 65536 ?
                                want_size - off : 65536;
                            json_t        *pr = req_new("pread", 3);
                            json_t        *prr;
                            unsigned char *got = malloc(n);
                            unsigned char *exp = malloc(n);
                            const char    *b64;
                            int            gl;
                            json_object_set_new(pr, "fd", json_integer(rfdn));
                            json_object_set_new(pr, "off", json_integer(off));
                            json_object_set_new(pr, "len", json_integer(n));
                            prr = call(pr);
                            b64 = json_string_value(
                                json_object_get(prr, "data"));
                            gl = b64_decode(b64 ? b64 : "", got, n);
                            shadow_read(cino, off, (size_t) n, exp);
                            if (gl != n || memcmp(got, exp, n) != 0) {
                                mism("audit: %s: content mismatch at +%lld",
                                     cpath, (long long) off);
                                free(got);
                                free(exp);
                                json_decref(prr);
                                break;
                            }
                            free(got);
                            free(exp);
                            json_decref(prr);
                        }
                        {
                            json_t *cl = req_new("close", 3);
                            json_object_set_new(cl, "fd", json_integer(rfdn));
                            json_decref(call(cl));
                        }
                    }
                    json_decref(ofr);
                }
            }
            json_decref(res);
        }
        json_decref(names);
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

        g_cur_fs = state_get(st, "fs");
        g_cur_ps = state_get(st, "ps");
        last_fs  = g_cur_fs;

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
        printf("%s: %zu steps replayed, %d objects audited\n", path, ns - 1,
               audited);
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
    const char *traces[1024];
    int         ntraces = 0;
    int         i, ran = 0, failures = 0, bad = 0;

    for (i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--backend") == 0 && i + 1 < argc) {
            backend = argv[++i];
        } else if (strcmp(argv[i], "--trace") == 0 && i + 1 < argc) {
            if (ntraces < (int) (sizeof(traces) / sizeof(traces[0]))) {
                traces[ntraces++] = argv[++i];
            }
        } else if (strcmp(argv[i], "--driver") == 0 && i + 1 < argc) {
            i++;   /* accepted + ignored (no subprocess) */
        }
    }
    if (ntraces == 0) {
        fprintf(stderr, "usage: %s --backend <b> --trace <f> [--trace ...]\n",
                argv[0]);
        return 2;
    }

    /* Clean result stream: chimera logs go to stderr (fd 1 -> stderr), our
     * output to the saved stdout, exactly like the driver. */
    proto_out = fdopen(dup(STDOUT_FILENO), "w");
    if (!proto_out || dup2(STDERR_FILENO, STDOUT_FILENO) < 0) {
        fprintf(stderr, "posix_mbt_replay: stream setup failed\n");
        return 1;
    }
    /* Route our printf() to the clean stream. */
    if (dup2(fileno(proto_out), STDOUT_FILENO) < 0) {
        return 1;
    }

    if (posix_env_setup(backend, NULL) != 0) {
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
    return (failures || bad) ? 1 : 0;
} /* main */
