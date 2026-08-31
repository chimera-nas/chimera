// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Replay a Quint-generated NFSv4 ITF trace against an IN-PROCESS chimera
 * server over the libevpl inproc transport.
 *
 * Each state's `lastOp` is either the initial `LInit {minor, caps}` or
 * one `LCompound {tag, ops, status, results}` (see nfs4.qnt /
 * DESIGN-NFS4.md).  The harness builds each compound from the model ops
 * through the generated COMPOUND stubs, sends it at the trace's
 * minorversion, and compares the server's per-op results and compound
 * status against the model's expectations.
 *
 * Identity indirection maintained here (model values are abstract):
 *   - ino        -> nfs_fh4, learned from GETFH replies
 *   - clientid   -> wire clientid, from SETCLIENTID/EXCHANGE_ID replies
 *   - sessionid  -> wire sessionid, from CREATE_SESSION replies
 *   - stateid    -> wire `other`, from OPEN/LOCK/delegation replies;
 *                   stateid seqids are predicted by the model exactly
 *   - change     -> never predicted; per-ino consistency map
 *   - block s    -> BLOCK_SIZE bytes, symbol 0 = zeroes, s > 0 = 0x40+s
 *   - lock byte  -> wire byte offset; top of the lock space = to-EOF
 *
 * The 4.1 SEQUENCE replay check compares a summary of every
 * oracle-relevant reply field (statuses, stateids, counts, change
 * values, payload hashes) instead of the raw reply bytes, which the
 * generated client does not expose.  (The retired python harness --
 * nfs4_replay.py, in git history -- compared raw bytes.)
 */

#include <getopt.h>
#include <jansson.h>
#include <sys/time.h>

#include "nfs3_mbt_common.h"
#include "common/mbt_trace_dir.h"
#include "common/mbt_watchdog.h"

#define V4_BLOCK_SIZE       8192
#define V4_LOCK_BYTES       8
#define V4_TO_EOF           0xffffffffffffffffULL

#define V4_MAX_INOS         4096
#define V4_MAX_CLIENTS      32
#define V4_MAX_SESS         32
#define V4_MAX_SLOTS        8
#define V4_MAX_SIDS         512
#define V4_MAX_TOKS         1024
#define V4_MAX_CHG          8192
#define V4_MAX_OPS          16
#define V4_MAX_MISM         16
#define V4_MISM_LEN         512
#define V4_MAX_NAMES        32
#define V4_NAME_LEN         128
#define V4_HISTORY          6
#define V4_DATA_ARENA       (4 << 20)

/* READ_PLUS: the model asks for at most a handful of blocks, and a
 * conforming server returns at most one segment per block. */
#define V4_RP_MAX_SEGS      16
#define V4_IA_MAX_HINTS     8
#define V4_RETRY_DELAY_MAX  40

/* layouttype4 values the harness can ask for.  The generated XDR names only
 * the NFSv4.1-files type (RFC 8881 12.2.5), which chimera does not serve; the
 * two it does are RFC 8435 flex-files and RFC 5663 block, spelled out here the
 * same way src/server/nfs/nfs4_pnfs.c spells them. */
#define V4_LAYOUT_FILES     0x1
#define V4_LAYOUT_BLOCK     0x3
#define V4_LAYOUT_FLEX      0x4
#define V4_LAYOUT_SCSI      0x5

/* Offset of ffds_deviceid inside an ff_layout4 loc_body (RFC 8435 5.1):
 * ffl_stripe_unit (8) + ffl_mirrors<> count (4) + ffm_data_servers<> count (4). */
#define V4_FF_DEVICEID_OFF  16

#define E_DELAY             10008
#define E_DENIED            10010
#define E_NOTSUPP           10004
#define E_LOCKS_HELD        10037
#define E_LAYOUTUNAVAILABLE 10059
#define E_NOMATCHING_LAYOUT 10060
#define V4_ERR_SYMLINK      10029

/* The layout type this run drives.  Fixed for the process: which one a server
 * serves is a property of the MDS backend (flex-files for the orchestrated
 * backends, block/SCSI for a layout-sourcing one), not of a trace. */
static uint32_t g_layout_type = V4_LAYOUT_FLEX;

/* ---- mismatch accumulator ------------------------------------------------ */

struct mism {
    int  n;
    char msg[V4_MAX_MISM][V4_MISM_LEN];
};

static void
mism_add(
    struct mism *m,
    const char  *fmt,
    ...) __attribute__((format(printf, 2, 3)));

static void
mism_add(
    struct mism *m,
    const char  *fmt,
    ...)
{
    va_list ap;

    if (m->n >= V4_MAX_MISM) {
        return;
    }
    va_start(ap, fmt);
    vsnprintf(m->msg[m->n], V4_MISM_LEN, fmt, ap);
    va_end(ap);
    m->n++;
} /* mism_add */

/* ---- ITF decoding helpers (same shapes as nfs3_mbt_replay.c) ------------- */

static int64_t
itf_i64(json_t *v)
{
    json_t *big;

    if (json_is_integer(v)) {
        return json_integer_value(v);
    }
    if (json_is_object(v)) {
        big = json_object_get(v, "#bigint");
        if (big && json_is_string(big)) {
            return strtoll(json_string_value(big), NULL, 10);
        }
    }
    fprintf(stderr, "trace format error: expected integer\n");
    exit(2);
} /* itf_i64 */

static int
itf_bool(json_t *v)
{
    if (!v || !json_is_boolean(v)) {
        fprintf(stderr, "trace format error: expected bool\n");
        exit(2);
    }
    return json_is_true(v);
} /* itf_bool */

static json_t *
itf_seq(json_t *v)
{
    json_t *set;

    if (json_is_array(v)) {
        return v;
    }
    if (json_is_object(v)) {
        set = json_object_get(v, "#set");
        if (set && json_is_array(set)) {
            return set;
        }
    }
    fprintf(stderr, "trace format error: expected list/set\n");
    exit(2);
} /* itf_seq */

static int64_t
jf_i64(
    json_t     *op,
    const char *key)
{
    json_t *v = json_object_get(op, key);

    if (!v) {
        fprintf(stderr, "trace format error: missing field '%s'\n", key);
        exit(2);
    }
    return itf_i64(v);
} /* jf_i64 */

static const char *
jf_str(
    json_t     *op,
    const char *key)
{
    json_t *v = json_object_get(op, key);

    if (!v || !json_is_string(v)) {
        fprintf(stderr, "trace format error: field '%s' not a string\n", key);
        exit(2);
    }
    return json_string_value(v);
} /* jf_str */

static int
jf_bool(
    json_t     *op,
    const char *key)
{
    json_t *v = json_object_get(op, key);

    if (!v || !json_is_boolean(v)) {
        fprintf(stderr, "trace format error: field '%s' not a bool\n", key);
        exit(2);
    }
    return json_is_true(v);
} /* jf_bool */

static const char *
jf_tag(json_t *v)
{
    json_t *tag = v ? json_object_get(v, "tag") : NULL;

    if (!tag || !json_is_string(tag)) {
        fprintf(stderr, "trace format error: value has no tag\n");
        exit(2);
    }
    return json_string_value(tag);
} /* jf_tag */

static json_t *
jf_val(json_t *v)
{
    return json_object_get(v, "value");
} /* jf_val */


/* Every attribute chimera advertises as supported, minus the two write-only
 * *_SET attributes and RDATTR_ERROR (meaningful only inside READDIR).  Shared
 * by RGetattrWide, RReaddir and the wide VERIFY/NVERIFY variants: asking for
 * the server's whole attribute vocabulary is what drives
 * chimera_nfs4_marshall_attrs, which each of those three ops compiles its own
 * static copy of. */
static void
v4_wide_attr_mask(uint32_t *m)
{
    m[0] = (1U << FATTR4_SUPPORTED_ATTRS) |
        (1U << FATTR4_TYPE) | (1U << FATTR4_FH_EXPIRE_TYPE) |
        (1U << FATTR4_CHANGE) | (1U << FATTR4_SIZE) |
        (1U << FATTR4_LINK_SUPPORT) | (1U << FATTR4_SYMLINK_SUPPORT) |
        (1U << FATTR4_NAMED_ATTR) | (1U << FATTR4_FSID) |
        (1U << FATTR4_UNIQUE_HANDLES) | (1U << FATTR4_LEASE_TIME) |
        (1U << FATTR4_ACL) | (1U << FATTR4_ACLSUPPORT) |
        (1U << FATTR4_ARCHIVE) | (1U << FATTR4_CANSETTIME) |
        (1U << FATTR4_CASE_INSENSITIVE) | (1U << FATTR4_CASE_PRESERVING) |
        (1U << FATTR4_CHOWN_RESTRICTED) | (1U << FATTR4_FILEHANDLE) |
        (1U << FATTR4_FILEID) | (1U << FATTR4_FILES_AVAIL) |
        (1U << FATTR4_FILES_FREE) | (1U << FATTR4_FILES_TOTAL) |
        (1U << FATTR4_MAXNAME) | (1U << FATTR4_MAXREAD) |
        (1U << FATTR4_MAXWRITE);
    m[1] = (1U << (FATTR4_MODE - 32)) |
        (1U << (FATTR4_NUMLINKS - 32)) | (1U << (FATTR4_OWNER - 32)) |
        (1U << (FATTR4_OWNER_GROUP - 32)) | (1U << (FATTR4_RAWDEV - 32)) |
        (1U << (FATTR4_SPACE_AVAIL - 32)) | (1U << (FATTR4_SPACE_FREE - 32)) |
        (1U << (FATTR4_SPACE_TOTAL - 32)) | (1U << (FATTR4_SPACE_USED - 32)) |
        (1U << (FATTR4_TIME_ACCESS - 32)) |
        (1U << (FATTR4_TIME_METADATA - 32)) |
        (1U << (FATTR4_TIME_MODIFY - 32));
} /* v4_wide_attr_mask */

/* ---- known-deviation registry (see DEVIATIONS-NFS4.md) ------------------- */

enum v4_dev {
    DEV_LOOKUPP_PSEUDOROOT = 0,   /* D4-1 */
    DEV_ACCESS_NO_EXECUTE,        /* D4-2 */
    DEV_SYMLINK_MODE_0755,        /* D4-4 */
    DEV_LOOKUPP_SYMLINK,          /* D4-7 */
    DEV_COARSE_TYPE_ERR,          /* D4-15 */
    DEV_READLINK_DIR_INVAL,       /* D4-16 */
    DEV_COUNT,
};

static const char *v4_dev_ids[DEV_COUNT] = {
    "D4-1-lookupp-pseudoroot",
    "D4-2-access-no-execute",
    "D4-4-symlink-mode-0755",
    "D4-7-lookupp-symlink",
    "D4-15-coarse-type-error",
    "D4-16-readlink-dir-inval",
};

#define E_WRONG_TYPE 10083

/* ---- per-op reply summary ------------------------------------------------ */

/* Everything the oracle consumes from one op's reply, copied out of the
 * rpc2-owned decode inside the compound callback.  `fnv` digests the
 * copied fields (and payload bytes) for the SEQUENCE replay-cache
 * comparison. */
struct v4_res {
    uint32_t        status;
    uint64_t        fnv;

    struct mbt_fh   fh;               /* GETFH */

    /* GETATTR (the five requested attrs) */
    int             has_attrs;
    uint32_t        a_type, a_mode, a_nlink;
    uint64_t        a_size, a_change;

    char            target[V4_NAME_LEN]; /* READLINK */
    uint32_t        target_len;

    uint32_t        supported, access; /* ACCESS */

    int             nnames;           /* READDIR / LISTXATTRS */
    char            names[V4_MAX_NAMES][V4_NAME_LEN];
    int             names_overflow;
    int             eof;

    int             has_cinfo;        /* CREATE/REMOVE/LINK/OPEN/... */
    int             cinfo_atomic;
    uint64_t        cinfo_before, cinfo_after;
    int             has_cinfo2;       /* RENAME target dir */
    int             cinfo2_atomic;
    uint64_t        cinfo2_before, cinfo2_after;

    uint64_t        clientid;         /* SETCLIENTID / EXCHANGE_ID */
    uint8_t         confirm[8];
    uint32_t        sequenceid, flags; /* EXCHANGE_ID */
    uint8_t         sessionid[16];    /* CREATE_SESSION */
    uint32_t        fore_slots;       /* CREATE_SESSION: ca_maxrequests */

    int             has_sid;          /* OPEN/LOCK/LOCKU/... result stateid */
    struct stateid4 sid;
    uint32_t        rflags;           /* OPEN */
    uint32_t        deleg_type;
    struct stateid4 deleg_sid;

    uint8_t         denied_owner[64]; /* LOCK/LOCKT DENIED */
    uint32_t        denied_owner_len;
    int             has_denied;

    uint32_t        count;            /* WRITE / COPY */
    uint8_t         verf[8];          /* WRITE / COMMIT */

    uint8_t        *data;             /* READ payload (compound arena) */
    uint32_t        data_len;

    uint64_t        offset;           /* SEEK */

    /* READ_PLUS segments, as returned.  The server may legally answer with
     * fewer segments than cover the request (the client re-issues from the
     * last byte returned), so the SReadPlus arm checks them as a prefix of
     * the model's classification rather than for equality.  DATA bytes are
     * copied into the compound arena: the reply buffers do not outlive the
     * decode. */
    int             rp_present;
    int             rp_nsegs;
    int             rp_bad;           /* unusable segment list */
    struct {
        int      is_data;
        uint64_t offset;
        uint64_t length;
        uint8_t *data;                /* arena copy; DATA segments only */
    }        rp_segs[V4_RP_MAX_SEGS];

    uint32_t        ia_nhints;        /* IO_ADVISE hints the server honored */
    uint32_t        ia_hints[V4_IA_MAX_HINTS];

    char            xvalue[V4_NAME_LEN]; /* GETXATTR */
    uint32_t        xvalue_len;

    int             nsegs;            /* LAYOUTGET */
    struct {
        uint64_t offset, length;
    }        segs[4];
    uint8_t         deviceid[16];
    int             has_deviceid;
    int             lr_present;       /* LAYOUTRETURN */
    int             has_newsize;      /* LAYOUTCOMMIT */
    uint64_t        newsize;
};

struct v4_reply {
    int           done;
    int           rpc_err;
    uint32_t      status;             /* compound status */
    int           nres;
    struct v4_res res[V4_MAX_OPS];
};

/* Cached summary for the 4.1 SEQUENCE replay contract. */
struct v4_cached {
    int      valid;
    uint32_t status;
    int      nres;
    struct {
        uint32_t status;
        uint64_t fnv;
    }        op[V4_MAX_OPS];
};

/* ---- oracle -------------------------------------------------------------- */

struct v4_chg {
    int64_t  ino;
    int64_t  abstract;
    uint64_t wire;
};

struct v4_hist {
    int      idx;
    char     opl[256];
    uint32_t status;
    char     stats[128];
};

struct oracle {
    struct mbt_env        *env;
    int                    verbose;
    int                    minor;

    struct mbt_fh          fh[V4_MAX_INOS];       /* ino -> filehandle */

    uint64_t               clientid[V4_MAX_CLIENTS];
    uint8_t                clientid_known[V4_MAX_CLIENTS];

    uint64_t               confirm_clientid[V4_MAX_TOKS];
    uint8_t                confirm_verf[V4_MAX_TOKS][8];
    uint8_t                confirm_known[V4_MAX_TOKS];

    uint8_t                sess[V4_MAX_SESS][16];
    uint8_t                sess_known[V4_MAX_SESS];
    int                    sess_client[V4_MAX_SESS]; /* model sess -> client */

    /* Fore-channel slots the server granted, and a private sequence id on
     * the retry slot (see the DELAY retry in run_compound). */
    uint32_t               sess_slots[V4_MAX_SESS];
    uint32_t               retry_seq[V4_MAX_SESS];

    uint8_t                sid_other[V4_MAX_SIDS][12];
    uint8_t                sid_known[V4_MAX_SIDS];
    uint64_t               sid_clientid[V4_MAX_SIDS]; /* wire clientid of maker */
    uint8_t                sid_clientid_known[V4_MAX_SIDS];

    struct v4_chg          chg[V4_MAX_CHG];
    int                    nchg;

    struct v4_cached cache[V4_MAX_SESS][V4_MAX_SLOTS];

    int                    have_write_verf;
    uint8_t                write_verf[8];

    uint8_t                deviceid[16];
    int                    has_deviceid;

    /* Per-model-client connections (a model client
    * owns its own connection, like a real one). */
    struct evpl_rpc2_conn *conns[V4_MAX_CLIENTS];

    /* CB_RECALL observations (stateid others seen on any backchannel). */
    uint8_t                recalls[64][12];
    int                    nrecalls;

    int                    dev_hits[DEV_COUNT];

    /* caps reconciliation */
    const char            *mandatory[8];
    int                    nmandatory;
    int                    skip;
    char                   skip_feature[64];
    char                   skip_detail[256];

    uint32_t               status_dev; /* accepted compound status deviation */
    uint64_t               cur_open_clientid;
    int                    cur_open_clientid_known;

    struct v4_hist         history[V4_HISTORY];
    int                    nhist;

    uint8_t               *arena;     /* per-compound payload copy space */
    uint32_t               arena_used;
    uint8_t               *scratch;   /* expectation expansion */
};

static uint64_t
fnv64(
    uint64_t    h,
    const void *data,
    size_t      len)
{
    const uint8_t *p = data;
    size_t         i;

    if (h == 0) {
        h = 0xcbf29ce484222325ULL;
    }
    for (i = 0; i < len; i++) {
        h ^= p[i];
        h *= 0x100000001b3ULL;
    }
    return h;
} /* fnv64 */

static void
block_fill(
    int64_t  sym,
    uint8_t *out)
{
    memset(out, sym == 0 ? 0 : (int) (0x40 + sym), V4_BLOCK_SIZE);
} /* block_fill */

static void
lock_range(
    int64_t   lo,
    int64_t   hi,
    uint64_t *off,
    uint64_t *len)
{
    if (hi >= V4_LOCK_BYTES) {
        *off = (uint64_t) lo;
        *len = V4_TO_EOF;
    } else {
        *off = (uint64_t) lo;
        *len = (uint64_t) (hi - lo);
    }
} /* lock_range */

/* ---- caps / deviations --------------------------------------------------- */

static int
caps_mismatch(
    struct oracle *o,
    struct mism   *m,
    const char    *feature,
    const char    *fmt,
    ...) __attribute__((format(printf, 4, 5)));

static int
caps_mismatch(
    struct oracle *o,
    struct mism   *m,
    const char    *feature,
    const char    *fmt,
    ...)
{
    va_list ap;
    char    detail[256];
    int     i;

    va_start(ap, fmt);
    vsnprintf(detail, sizeof(detail), fmt, ap);
    va_end(ap);

    for (i = 0; i < o->nmandatory; i++) {
        if (strcmp(o->mandatory[i], feature) == 0) {
            mism_add(m, "MANDATORY capability [%s] mismatch: %s",
                     feature, detail);
            return 1;
        }
    }
    o->skip = 1;
    snprintf(o->skip_feature, sizeof(o->skip_feature), "%s", feature);
    snprintf(o->skip_detail, sizeof(o->skip_detail), "%s", detail);
    return 0;
} /* caps_mismatch */

/* ---- identity helpers ---------------------------------------------------- */

static const struct mbt_fh *
real_fh(
    struct oracle *o,
    int64_t        ino,
    struct mism   *m)
{
    if (ino < 0 || ino >= V4_MAX_INOS || !o->fh[ino].has) {
        mism_add(m, "model ino %" PRId64 " has no learned filehandle", ino);
        return NULL;
    }
    return &o->fh[ino];
} /* real_fh */

static void
learn_fh(
    struct oracle       *o,
    int64_t              ino,
    const struct mbt_fh *fh,
    struct mism         *m)
{
    if (ino < 0 || ino >= V4_MAX_INOS) {
        mism_add(m, "model ino %" PRId64 " out of range", ino);
        return;
    }
    if (!o->fh[ino].has) {
        o->fh[ino] = *fh;
    } else if (!mbt_fh_eq(&o->fh[ino], fh)) {
        mism_add(m, "ino %" PRId64 ": filehandle changed", ino);
    }
} /* learn_fh */

static int
sid_of(
    struct oracle *o,
    int64_t        sid,
    uint8_t       *other,
    struct mism   *m)
{
    if (sid < 0 || sid >= V4_MAX_SIDS || !o->sid_known[sid]) {
        mism_add(m, "model stateid %" PRId64 " has no learned other", sid);
        return -1;
    }
    memcpy(other, o->sid_other[sid], 12);
    return 0;
} /* sid_of */

static void
learn_sid(
    struct oracle         *o,
    int64_t                sid,
    const struct stateid4 *wire,
    struct mism           *m,
    int64_t                expect_seq, /* < 0: no expectation */
    const char            *what)
{
    int64_t osid;

    if (sid < 0 || sid >= V4_MAX_SIDS) {
        mism_add(m, "%s: model sid %" PRId64 " out of range", what, sid);
        return;
    }
    if (!o->sid_known[sid]) {
        for (osid = 0; osid < V4_MAX_SIDS; osid++) {
            if (o->sid_known[osid] &&
                memcmp(o->sid_other[osid], wire->other, 12) == 0) {
                mism_add(m, "%s: other of model sid %" PRId64
                         " collides with model sid %" PRId64,
                         what, sid, osid);
            }
        }
        memcpy(o->sid_other[sid], wire->other, 12);
        o->sid_known[sid] = 1;
    } else if (memcmp(o->sid_other[sid], wire->other, 12) != 0) {
        mism_add(m, "%s: model sid %" PRId64 " other changed", what, sid);
    }
    if (expect_seq >= 0 && wire->seqid != (uint32_t) expect_seq) {
        mism_add(m, "%s: seqid: expected %" PRId64 ", got %u",
                 what, expect_seq, wire->seqid);
    }
} /* learn_sid */

static void
check_change(
    struct oracle *o,
    int64_t        ino,
    int64_t        abstract,
    uint64_t       wire,
    struct mism   *m,
    const char    *what)
{
    int i;

    if (ino < 0) {
        return;
    }
    for (i = 0; i < o->nchg; i++) {
        if (o->chg[i].ino == ino && o->chg[i].abstract == abstract) {
            if (o->chg[i].wire != wire) {
                mism_add(m, "%s: ino %" PRId64 " change: abstract %" PRId64
                         " previously %#" PRIx64 ", now %#" PRIx64,
                         what, ino, abstract, o->chg[i].wire, wire);
            }
            return;
        }
    }
    for (i = 0; i < o->nchg; i++) {
        if (o->chg[i].ino == ino && o->chg[i].wire == wire) {
            mism_add(m, "%s: ino %" PRId64 " change %#" PRIx64 " unchanged "
                     "on the wire but the model mutated the object "
                     "(abstract %" PRId64 " vs %" PRId64 ")",
                     what, ino, wire, o->chg[i].abstract, abstract);
            return;
        }
    }
    if (o->nchg < V4_MAX_CHG) {
        o->chg[o->nchg].ino      = ino;
        o->chg[o->nchg].abstract = abstract;
        o->chg[o->nchg].wire     = wire;
        o->nchg++;
    }
} /* check_change */

static void
check_cinfo(
    struct oracle *o,
    int64_t        ino,
    json_t        *exp,
    int            atomic,
    uint64_t       before,
    uint64_t       after,
    struct mism   *m,
    const char    *what)
{
    char sub[80];

    if (atomic) {
        snprintf(sub, sizeof(sub), "%s.before", what);
        check_change(o, ino, jf_i64(exp, "before"), before, m, sub);
    }
    snprintf(sub, sizeof(sub), "%s.after", what);
    check_change(o, ino, jf_i64(exp, "after"), after, m, sub);
} /* check_cinfo */

/* ---- CB_COMPOUND recorder (backchannel) ---------------------------------- */

static struct oracle *g_recall_oracle;

/* Bumped once per trace so every trace's SETCLIENTID/EXCHANGE_ID owner id is
 * globally distinct: when many traces share one long-lived server (batched
 * replay), each must register as a brand-new client so it gets a fresh
 * clientid/session/slot state rather than colliding with a prior trace's. */
static uint64_t       g_owner_epoch;

/*
 * CB_NULL.  The server probes a client's callback path with one before it will
 * use the path (nfs4_cb_ensure_probe), and it holds a reference on the channel
 * until the probe completes -- so a harness that never answers leaves the
 * channel pinned for the life of the process.  pNFS is the first MBT profile to
 * open a callback path at all (LAYOUTGET opens one so the layout can later be
 * recalled), which is why this went unnoticed while delegations stayed off.
 */
static void
v4_cb_null(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct oracle *o = private_data ? private_data : g_recall_oracle;
    int            rc;

    (void) conn;
    (void) cred;

    if (!o) {
        return;
    }

    rc = o->env->nfs_v4_cb.send_reply_CB_NULL(evpl, NULL, encoding);
    (void) rc;
} /* v4_cb_null */

static void
v4_cb_compound(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct CB_COMPOUND4args   *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct oracle         *o = private_data ? private_data : g_recall_oracle;
    struct CB_COMPOUND4res res;
    struct nfs_cb_resop4  *resarray;
    uint32_t               i;
    int                    rc;
    nfsstat4               cb_status = NFS4_OK;

    (void) conn;
    (void) cred;

    if (!o) {
        return;   /* no oracle bound to this backchannel; cannot reply */
    }

    memset(&res, 0, sizeof(res));
    res.tag.len  = 0;
    res.tag.data = NULL;

    resarray = xdr_dbuf_alloc_space(
        sizeof(*resarray) * (args->num_argarray ? args->num_argarray : 1),
        encoding->dbuf);
    if (!resarray) {
        res.status = NFS4ERR_RESOURCE;
        rc         = o->env->nfs_v4_cb.send_reply_CB_COMPOUND(evpl, NULL,
                                                              &res, encoding);
        (void) rc;
        return;
    }

    for (i = 0; i < args->num_argarray; i++) {
        struct nfs_cb_argop4 *argop = &args->argarray[i];
        struct nfs_cb_resop4 *resop = &resarray[i];

        memset(resop, 0, sizeof(*resop));
        resop->resop = argop->argop;

        switch (argop->argop) {
            case OP_CB_SEQUENCE:
                memcpy(resop->opcbsequence.csr_resok4.csr_sessionid,
                       argop->opcbsequence.csa_sessionid, 16);
                resop->opcbsequence.csr_resok4.csr_sequenceid =
                    argop->opcbsequence.csa_sequenceid;
                resop->opcbsequence.csr_resok4.csr_slotid =
                    argop->opcbsequence.csa_slotid;
                resop->opcbsequence.csr_resok4.csr_highest_slotid =
                    argop->opcbsequence.csa_highest_slotid;
                resop->opcbsequence.csr_resok4.csr_target_highest_slotid =
                    argop->opcbsequence.csa_highest_slotid;
                resop->opcbsequence.csr_status = NFS4_OK;
                break;
            case OP_CB_RECALL:
                if (o && o->nrecalls <
                    (int) (sizeof(o->recalls) / sizeof(o->recalls[0]))) {
                    memcpy(o->recalls[o->nrecalls++],
                           argop->opcbrecall.stateid.other, 12);
                }
                resop->opcbrecall.status = NFS4_OK;
                break;
            case OP_CB_LAYOUTRECALL:
                /* Decline the recall.  NFS4_OK would promise a LAYOUTRETURN,
                 * and there is nobody here to send one: the trace is fixed and
                 * this thread is blocked inside the very compound whose
                 * SETATTR triggered the recall, so the server would wait for a
                 * return that can never arrive.  NFS4ERR_NOMATCHING_LAYOUT is
                 * also the honest answer -- a replayer holds no layout state
                 * of its own -- and it makes the server revoke the layout and
                 * resume the deferred op (RFC 8881 12.5.5), which is what the
                 * pNFS model profiles encode.  Both the op status and the
                 * compound status carry it; the server reads the latter. */
                if (o && o->nrecalls <
                    (int) (sizeof(o->recalls) / sizeof(o->recalls[0]))) {
                    memcpy(o->recalls[o->nrecalls++],
                           argop->opcblayoutrecall.clora_recall.lor_layout.
                           lor_stateid.other, 12);
                }
                resop->opcblayoutrecall.clorr_status =
                    E_NOMATCHING_LAYOUT;
                cb_status = E_NOMATCHING_LAYOUT;
                break;
            default:
                resop->opcbrecall.status = NFS4ERR_NOTSUPP;
                cb_status                = NFS4ERR_NOTSUPP;
                break;
        } /* switch */
    }

    res.status       = cb_status;
    res.num_resarray = args->num_argarray;
    res.resarray     = resarray;

    rc = o->env->nfs_v4_cb.send_reply_CB_COMPOUND(evpl, NULL, &res, encoding);
    (void) rc;
} /* v4_cb_compound */

/* ---- per-model-client connections ---------------------------------------- */

static struct evpl_rpc2_conn *
conn_for(
    struct oracle *o,
    int64_t        model_client)
{
    struct evpl_rpc2_program *cb_programs[1];
    struct evpl_endpoint     *ep;

    if (model_client < 0) {
        return o->env->nfs_conn;
    }
    if (model_client >= V4_MAX_CLIENTS) {
        fprintf(stderr, "model client %" PRId64 " out of range\n",
                model_client);
        exit(2);
    }
    if (!o->conns[model_client]) {
        cb_programs[0] = &o->env->nfs_v4_cb.rpc2;
        ep             = chimera_tcp_flavor_endpoint_create(CHIMERA_TCP_FLAVOR_INPROC,
                                                            "127.0.0.1", 2049);
        o->conns[model_client] =
            evpl_rpc2_client_connect(o->env->rpc2_thread, EVPL_STREAM_INPROC,
                                     ep, cb_programs, 1, o);
        if (!o->conns[model_client]) {
            fprintf(stderr, "failed to open connection for model client "
                    "%" PRId64 "\n", model_client);
            exit(2);
        }
    }
    return o->conns[model_client];
} /* conn_for */

/* The model client whose connection this compound belongs to, or -1 for
 * filehandle-only compounds (mirror of Replayer.compound_client). */
static int64_t
compound_client(
    struct oracle *o,
    json_t        *ops,
    json_t        *results)
{
    size_t  i;
    json_t *op;
    json_t *res;

    if (json_array_size(ops) > 0) {
        op = json_array_get(ops, 0);
        if (strcmp(jf_tag(op), "RSequence") == 0) {
            int64_t sess = jf_i64(jf_val(op), "sess");

            if (sess >= 0 && sess < V4_MAX_SESS && o->sess_known[sess]) {
                return o->sess_client[sess];
            }
            return -1;
        }
    }
    json_array_foreach(ops, i, op)
    {
        const char *t = jf_tag(op);
        json_t     *v = jf_val(op);

        res = json_array_get(results, i);
        if ((strcmp(t, "RExchangeId") == 0 ||
             strcmp(t, "RSetclientid") == 0) && res &&
            itf_i64(json_object_get(jf_val(res), "st")) == NFS4_OK) {
            return jf_i64(jf_val(res), "client");
        }
        if (strcmp(t, "RSetclientidConfirm") == 0 ||
            strcmp(t, "RCreateSession") == 0 ||
            strcmp(t, "RRenew") == 0 ||
            strcmp(t, "RDestroyClientid") == 0) {
            if (json_is_object(v) && json_object_get(v, "client")) {
                return jf_i64(v, "client");
            }
            return itf_i64(v);
        }
        if (strcmp(t, "RDestroySession") == 0) {
            int64_t sess = itf_i64(v);

            if (sess >= 0 && sess < V4_MAX_SESS && o->sess_known[sess]) {
                return o->sess_client[sess];
            }
            return -1;
        }
        if (strcmp(t, "ROpen") == 0 ||
            strcmp(t, "RReleaseLockowner") == 0) {
            return jf_i64(v, "client");
        }
    }
    return -1;
} /* compound_client */

/* ---- op encoding --------------------------------------------------------- */

/* Per-op stable storage for pointers the argop carries until the send
 * completes: owner strings, verifiers, attr blobs, WRITE iovecs. */
struct v4_argscratch {
    char              owner[64];
    uint8_t           verf[8];
    uint32_t          bitmap[2];
    uint8_t           blob[64];
    uint32_t          attr_req[2];
    struct evpl_iovec wiov;
    int               wiov_used;
    char              xval[64];
    uint32_t          ia_hint;               /* IO_ADVISE request hint */
    uint8_t           ws_pattern[V4_BLOCK_SIZE]; /* WRITE_SAME ADB pattern */
    /* Materialized component names.  Two, because RENAME carries two at once. */
    char              nbuf[2][300];
};


/*
 * Materialize a model component name onto the wire.
 *
 * Ordinary names travel as themselves.  The hostile sentinels expand to byte
 * strings a JSON trace cannot carry literally -- an embedded NUL, invalid
 * UTF-8, 256 bytes -- which is the same abstract-sentinel trick the POSIX
 * suite uses for its ENAMETOOLONG cases.  Kept in lockstep with nameStatus()
 * in nfs4_ops.qnt, which predicts what chimera_nfs4_validate_name() answers
 * for each (nfs4_procs.h):
 *
 *   NEMPTY  ""                  -> NFS4ERR_INVAL        (len == 0)
 *   NLONG   256 * 'x'           -> NFS4ERR_NAMETOOLONG  (len > 255)
 *   NDOT    "."                 -> NFS4ERR_BADNAME
 *   NDOTDOT ".."                -> NFS4ERR_BADNAME
 *   NSLASH  "a/b"               -> NFS4ERR_BADCHAR      (path separator)
 *   NNUL    "a\0b"              -> NFS4ERR_BADCHAR      (embedded NUL)
 *   NUTF8   "\x80"              -> NFS4ERR_INVAL        (stray continuation)
 *   NUTF8B  "\xC0\x80"          -> NFS4ERR_INVAL        (overlong 2-byte)
 *   NUTF8C  "\xE0\x80\x80"      -> NFS4ERR_INVAL        (overlong 3-byte)
 */
static void
v4_expand_name(
    const char  *sname,
    char        *buf,
    const char **out,
    uint32_t    *outlen)
{
    /* Adjacent string literals, not one literal: a C hex escape is greedy, so
     * "\xE6\x97\xA5" would parse as a single overlong escape. */
    /* *INDENT-OFF* */
    struct { const char *tag; const char *bytes; uint32_t len; } map[] = {
        /* --- rejected by chimera_nfs4_validate_name ------------------ */
        { "NEMPTY",  "",                                          0
        },
        { "NDOT",    ".",                                         1
        },
        { "NDOTDOT", "..",                                        2
        },
        { "NSLASH",  "a/b",                                       3
        },
        { "NNUL",    "a\0b",                                      3
        },
        /* --- rejected by chimera_nfs4_utf8_valid, one per branch ----- */
        { "NUTF8",   "\x80",                                      1
        },                                                                                         /* stray continuation */
        { "NUTF8B",  "\xC0" "\x80",                               2
        },                                                                                         /* overlong 2-byte    */
        { "NUTF8C",  "\xE0" "\x80" "\x80",                        3
        },                                                                                         /* overlong 3-byte    */
        { "NUSUR",   "\xED" "\xA0" "\x80",                        3
        },                                                                                         /* surrogate U+D800   */
        { "NUNCH",   "\xEF" "\xBF" "\xBF",                        3
        },                                                                                         /* non-char U+FFFF    */
        { "NU4OV",   "\xF0" "\x8F" "\xBF" "\xBF",                 4
        },                                                                                         /* overlong 4-byte    */
        { "NU4HI",   "\xF4" "\x90" "\x80" "\x80",                 4
        },                                                                                         /* above U+10FFFF     */
        { "NUF5",    "\xF5" "\x80" "\x80" "\x80",                 4
        },                                                                                         /* c > 0xF4           */
        { "NUTRUNC", "\xE6" "\x97",                               2
        },                                                                                         /* truncated 3-byte   */
        { "NUBADC",  "\xC3" "A",                                  2
        },                                                                                         /* bad continuation   */
        /* --- multi-character names.  Every sentinel above is a single
         * character, so the validator's loop (for i < len, with the inner
         * continuation-byte while) only ever ran one iteration and never a
         * transition between widths.  These do. --- */
        { "NUMIXB",  "a" "\xC3" "\xA9" "\x80",                    4
        },                                                                                         /* ok,ok,stray cont  */
        { "NUMIXT",  "a" "\xE6" "\x97",                           3
        },                                                                                         /* ok, then truncated*/
        { "NUMIXS",  "\xC3" "\xA9" "\xED" "\xA0" "\x80",          5
        },                                                                                         /* ok, surrogate */
        /* --- ACCEPTED: the multi-byte success paths, which every name
         * the model generated until now (pure ASCII) skipped entirely --- */
        { "NUMIX",   "a" "\xC3" "\xA9" "\xE6" "\x97" "\xA5"
          "\xF0" "\x9D" "\x84" "\x9E",  10 },            /* 1+2+3+4 widths  */
        { "NUREP",   "\xC3" "\xA9" "\xC3" "\xA9" "\xC3" "\xA9",   6
        },                                                                                         /* loop x3 */
        { "NU2",     "\xC3" "\xA9",                               2
        },                                                                                         /* U+00E9  e-acute    */
        { "NU3",     "\xE6" "\x97" "\xA5",                        3
        },                                                                                         /* U+65E5  CJK        */
        { "NU4",     "\xF0" "\x9D" "\x84" "\x9E",                 4
        },                                                                                         /* U+1D11E clef       */
        { "NU3E",    "\xE0" "\xA0" "\x80",                        3
        },                                                                                         /* U+0800  3-byte min */
        { "NU3S",    "\xED" "\x9F" "\xBF",                        3
        },                                                                                         /* U+D7FF just below
                                                                                                    * the surrogates    */
        { "NU4M",    "\xF4" "\x8F" "\xBF" "\xBF",                 4
        },                                                                                         /* U+10FFFF max       */
    };
    /* *INDENT-ON* */
    unsigned i;

    if (strcmp(sname, "NLONG") == 0) {
        memset(buf, 'x', 256);
        *out    = buf;
        *outlen = 256;
        return;
    }
    for (i = 0; i < sizeof(map) / sizeof(map[0]); i++) {
        if (strcmp(sname, map[i].tag) == 0) {
            memcpy(buf, map[i].bytes, map[i].len);
            *out    = buf;
            *outlen = map[i].len;
            return;
        }
    }
    *out    = sname;
    *outlen = (uint32_t) strlen(sname);
} /* v4_expand_name */


/* Compare a server-returned directory entry against a model name.  Model names
 * may be sentinels (see v4_expand_name), so the expected side has to be
 * materialized before comparing -- the server returns the real bytes, e.g. the
 * two bytes of U+00E9 rather than the string "NU2". */
static int
v4_name_eq(
    const char *wire,
    const char *model_name)
{
    char        buf[300];
    const char *want;
    uint32_t    wantlen;

    v4_expand_name(model_name, buf, &want, &wantlen);
    return strlen(wire) == wantlen && memcmp(wire, want, wantlen) == 0;
} /* v4_name_eq */

static void
pack_be32(
    uint8_t *p,
    uint32_t v)
{
    p[0] = (uint8_t) (v >> 24);
    p[1] = (uint8_t) (v >> 16);
    p[2] = (uint8_t) (v >> 8);
    p[3] = (uint8_t) v;
} /* pack_be32 */

static void
pack_be64(
    uint8_t *p,
    uint64_t v)
{
    pack_be32(p, (uint32_t) (v >> 32));
    pack_be32(p + 4, (uint32_t) v);
} /* pack_be64 */

/* fattr4 restricted to what the model sets (mode and/or size), packed in
 * ascending attribute order. */
static void
pack_fattr(
    struct fattr4        *f,
    struct v4_argscratch *s,
    int                   mode,   /* < 0: omit */
    int64_t               size)   /* < 0: omit */
{
    uint32_t len = 0;

    s->bitmap[0] = 0;
    s->bitmap[1] = 0;
    if (size >= 0) {
        s->bitmap[0] |= 1U << FATTR4_SIZE;
        pack_be64(s->blob + len, (uint64_t) size);
        len += 8;
    }
    if (mode >= 0) {
        s->bitmap[1] |= 1U << (FATTR4_MODE - 32);
        pack_be32(s->blob + len, (uint32_t) mode);
        len += 4;
    }
    f->num_attrmask   = mode >= 0 ? 2 : (size >= 0 ? 1 : 0);
    f->attrmask       = s->bitmap;
    f->attr_vals.data = s->blob;
    f->attr_vals.len  = len;
} /* pack_fattr */

/*
 * SETATTR carrying every writable attribute chimera accepts except SIZE and
 * ACL: mode, owner, owner_group and both settable times.  Values are packed in
 * ascending attribute-bit order (RFC 7530 §5) -- MODE(33), OWNER(36),
 * OWNER_GROUP(37), TIME_ACCESS_SET(40), TIME_MODIFY_SET(42).
 *
 * The model treats this as exactly a mode-only SETATTR, so the extra
 * attributes must not move anything it tracks: owner and group are "0", the
 * identity the harness already runs as, and both times use
 * SET_TO_SERVER_TIME4 (discriminant 0, no nfstime4 body).  The point is
 * SETATTR's own copy of chimera_nfs4_unmarshall_attrs, which a mode/size-only
 * model leaves largely unexecuted.
 */
static void
pack_fattr_wide(
    struct fattr4        *f,
    struct v4_argscratch *s,
    int                   mode)
{
    uint32_t len = 0;
    int      i;

    s->bitmap[0] = 0;
    s->bitmap[1] = (1U << (FATTR4_MODE - 32)) |
        (1U << (FATTR4_OWNER - 32)) |
        (1U << (FATTR4_OWNER_GROUP - 32)) |
        (1U << (FATTR4_TIME_ACCESS_SET - 32)) |
        (1U << (FATTR4_TIME_MODIFY_SET - 32));

    pack_be32(s->blob + len, (uint32_t) mode);
    len += 4;

    /* owner, then owner_group: utf8str_mixed, length-prefixed and padded to a
     * 4-byte boundary.  chimera converts the text with strtoul(). */
    for (i = 0; i < 2; i++) {
        pack_be32(s->blob + len, 1);
        len             += 4;
        s->blob[len]     = '0';
        s->blob[len + 1] = 0;
        s->blob[len + 2] = 0;
        s->blob[len + 3] = 0;
        len             += 4;
    }

    /* settime4 x2: SET_TO_SERVER_TIME4 carries no timestamp. */
    pack_be32(s->blob + len, 0);
    len += 4;
    pack_be32(s->blob + len, 0);
    len += 4;

    f->num_attrmask   = 2;
    f->attrmask       = s->bitmap;
    f->attr_vals.data = s->blob;
    f->attr_vals.len  = len;
} /* pack_fattr_wide */


static void
set_stateid(
    struct stateid4 *sid,
    uint32_t         seq,
    const uint8_t   *other)
{
    sid->seqid = seq;
    memcpy(sid->other, other, 12);
} /* set_stateid */

static const uint8_t v4_anon_other[12]   = { 0 };
static const uint8_t v4_bypass_other[12] = {
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xff, 0xff, 0xff
};

/* SelAnon / SelBypass / SelSid{argSeq, sid} -> wire stateid. */
static int
resolve_sel(
    struct oracle   *o,
    json_t          *sel,
    struct stateid4 *out,
    struct mism     *m)
{
    const char *tag = jf_tag(sel);
    uint8_t     other[12];
    json_t     *r;

    if (strcmp(tag, "SelAnon") == 0) {
        set_stateid(out, 0, v4_anon_other);
        return 0;
    }
    if (strcmp(tag, "SelBypass") == 0) {
        set_stateid(out, 0xffffffff, v4_bypass_other);
        return 0;
    }
    r = jf_val(sel);
    if (sid_of(o, jf_i64(r, "sid"), other, m) < 0) {
        return -1;
    }
    set_stateid(out, (uint32_t) jf_i64(r, "argSeq"), other);
    return 0;
} /* resolve_sel */

static int64_t
wire_clientid(
    struct oracle *o,
    int64_t        client,
    uint64_t      *out,
    struct mism   *m)
{
    if (client < 0 || client >= V4_MAX_CLIENTS ||
        !o->clientid_known[client]) {
        mism_add(m, "model client %" PRId64 " has no wire clientid", client);
        return -1;
    }
    *out = o->clientid[client];
    return 0;
} /* wire_clientid */

static nfs_ftype4
v4_ftype_wire(const char *tag)
{
    if (strcmp(tag, "FReg") == 0) {
        return NF4REG;
    }
    if (strcmp(tag, "FDir") == 0) {
        return NF4DIR;
    }
    if (strcmp(tag, "FLnk") == 0) {
        return NF4LNK;
    }
    if (strcmp(tag, "FFifo") == 0) {
        return NF4FIFO;
    }
    if (strcmp(tag, "FSock") == 0) {
        return NF4SOCK;
    }
    if (strcmp(tag, "FBlk") == 0) {
        return NF4BLK;
    }
    if (strcmp(tag, "FChr") == 0) {
        return NF4CHR;
    }
    fprintf(stderr, "trace format error: unknown ftype %s\n", tag);
    exit(2);
} /* v4_ftype_wire */

/* Encode one model op into argop.  Returns 0, or -1 with a mism recorded
 * (unknown identity -- an earlier divergence). */
static int
encode_op(
    struct oracle        *o,
    json_t               *op,
    struct nfs_argop4    *a,
    struct v4_argscratch *s,
    struct mism          *m)
{
    const char          *tag = jf_tag(op);
    json_t              *v   = jf_val(op);
    const struct mbt_fh *fh;
    uint64_t             cid;
    uint8_t              other[12];
    uint64_t             off64, len64;

    memset(a, 0, sizeof(*a));
    memset(s, 0, sizeof(*s));

    if (strcmp(tag, "RPutrootfh") == 0) {
        /* The model's ROOT is the export root; chimera's PUTROOTFH lands
         * on the pseudo-fs root, so substitute the export root handle
         * resolved at mount time. */
        a->argop               = OP_PUTFH;
        a->opputfh.object.data = (void *) o->fh[0].data;
        a->opputfh.object.len  = o->fh[0].len;
        return 0;
    }
    if (strcmp(tag, "RPutfh") == 0) {
        fh = real_fh(o, itf_i64(v), m);
        if (!fh) {
            return -1;
        }
        a->argop               = OP_PUTFH;
        a->opputfh.object.data = (void *) fh->data;
        a->opputfh.object.len  = fh->len;
        return 0;
    }
    if (strcmp(tag, "RGetfh") == 0) {
        a->argop = OP_GETFH;
        return 0;
    }
    if (strcmp(tag, "RSavefh") == 0) {
        a->argop = OP_SAVEFH;
        return 0;
    }
    if (strcmp(tag, "RRestorefh") == 0) {
        a->argop = OP_RESTOREFH;
        return 0;
    }
    if (strcmp(tag, "RLookup") == 0) {
        a->argop = OP_LOOKUP;
        v4_expand_name(json_string_value(v), s->nbuf[0],
                       (const char **) &a->oplookup.objname.data,
                       &a->oplookup.objname.len);
        return 0;
    }
    if (strcmp(tag, "RSecinfo") == 0) {
        a->argop = OP_SECINFO;
        v4_expand_name(json_string_value(v), s->nbuf[0],
                       (const char **) &a->opsecinfo.name.data,
                       &a->opsecinfo.name.len);
        return 0;
    }
    if (strcmp(tag, "RLookupp") == 0) {
        a->argop = OP_LOOKUPP;
        return 0;
    }
    if (strcmp(tag, "RGetattr") == 0) {
        a->argop       = OP_GETATTR;
        s->attr_req[0] = (1U << FATTR4_TYPE) | (1U << FATTR4_CHANGE) |
            (1U << FATTR4_SIZE);
        s->attr_req[1] = (1U << (FATTR4_MODE - 32)) |
            (1U << (FATTR4_NUMLINKS - 32));
        a->opgetattr.attr_request     = s->attr_req;
        a->opgetattr.num_attr_request = 2;
        return 0;
    }
    if (strcmp(tag, "RGetattrWide") == 0) {
        /* See v4_wide_attr_mask: the model predicts the status and nothing
         * else (SGetattrWide has no comparator arm), so this exists purely to
         * run chimera's attribute marshaller over its whole vocabulary. */
        a->argop = OP_GETATTR;
        v4_wide_attr_mask(s->attr_req);
        a->opgetattr.attr_request     = s->attr_req;
        a->opgetattr.num_attr_request = 2;
        return 0;
    }
    if (strcmp(tag, "RReadlink") == 0) {
        a->argop = OP_READLINK;
        return 0;
    }
    if (strcmp(tag, "RAccess") == 0) {
        a->argop           = OP_ACCESS;
        a->opaccess.access = (uint32_t) itf_i64(v);
        return 0;
    }
    if (strcmp(tag, "RReaddir") == 0) {
        a->argop            = OP_READDIR;
        a->opreaddir.cookie = 0;
        memset(a->opreaddir.cookieverf, 0, sizeof(a->opreaddir.cookieverf));
        a->opreaddir.dircount = 65536;
        a->opreaddir.maxcount = 1048576;
        /* Ask for the server's whole attribute vocabulary, not just FILEID:
         * READDIR compiles its own copy of chimera_nfs4_marshall_attrs and
         * marshals it per directory entry, and the harness compares only
         * names and eof -- so this is free coverage of that copy. */
        v4_wide_attr_mask(s->attr_req);
        a->opreaddir.attr_request     = s->attr_req;
        a->opreaddir.num_attr_request = 2;
        return 0;
    }
    if (strcmp(tag, "RCreate") == 0) {
        const char *ft = jf_tag(json_object_get(v, "ctype"));

        a->argop                 = OP_CREATE;
        a->opcreate.objtype.type = v4_ftype_wire(ft);
        if (strcmp(ft, "FLnk") == 0) {
            const char *t = jf_str(v, "target");

            a->opcreate.objtype.linkdata.data = (void *) t;
            a->opcreate.objtype.linkdata.len  = (uint32_t) strlen(t);
        } else if (strcmp(ft, "FBlk") == 0 || strcmp(ft, "FChr") == 0) {
            a->opcreate.objtype.devdata.specdata1 = 0;
            a->opcreate.objtype.devdata.specdata2 = 0;
        }
        v4_expand_name(jf_str(v, "name"), s->nbuf[0],
                       (const char **) &a->opcreate.objname.data,
                       &a->opcreate.objname.len);
        pack_fattr(&a->opcreate.createattrs, s, (int) jf_i64(v, "mode"), -1);
        return 0;
    }
    if (strcmp(tag, "RRemove") == 0) {
        a->argop = OP_REMOVE;
        v4_expand_name(json_string_value(v), s->nbuf[0],
                       (const char **) &a->opremove.target.data,
                       &a->opremove.target.len);
        return 0;
    }
    if (strcmp(tag, "RRename") == 0) {
        a->argop = OP_RENAME;
        v4_expand_name(jf_str(v, "oldname"), s->nbuf[0],
                       (const char **) &a->oprename.oldname.data,
                       &a->oprename.oldname.len);
        v4_expand_name(jf_str(v, "newname"), s->nbuf[1],
                       (const char **) &a->oprename.newname.data,
                       &a->oprename.newname.len);
        return 0;
    }
    if (strcmp(tag, "RLink") == 0) {
        a->argop = OP_LINK;
        v4_expand_name(json_string_value(v), s->nbuf[0],
                       (const char **) &a->oplink.newname.data,
                       &a->oplink.newname.len);
        return 0;
    }
    if (strcmp(tag, "RSetattrWide") == 0) {
        a->argop = OP_SETATTR;
        memset(&a->opsetattr.stateid, 0, sizeof(a->opsetattr.stateid));
        pack_fattr_wide(&a->opsetattr.obj_attributes, s, (int) itf_i64(v));
        return 0;
    }
    if (strcmp(tag, "RSetattr") == 0) {
        int64_t size_blk = jf_i64(v, "sizeBlocks");
        int64_t mode     = jf_i64(v, "mode");

        a->argop = OP_SETATTR;
        if (resolve_sel(o, json_object_get(v, "sel"),
                        &a->opsetattr.stateid, m) < 0) {
            return -1;
        }
        pack_fattr(&a->opsetattr.obj_attributes, s,
                   mode < 0 ? -1 : (int) mode,
                   size_blk < 0 ? -1 : size_blk * V4_BLOCK_SIZE);
        return 0;
    }
    if (strcmp(tag, "RVerifyWide") == 0 ||
        strcmp(tag, "RNverifyWide") == 0) {
        int            nv = tag[1] == 'N';
        struct fattr4 *f  = nv ? &a->opnverify.obj_attributes
                               : &a->opverify.obj_attributes;

        /* Wide mask, empty value blob.  chimera marshals every requested
         * attribute before comparing, so this exercises VERIFY's own copy of
         * chimera_nfs4_marshall_attrs; the length guard
         * (out_len == attr_vals.len) then makes the comparison differ
         * deterministically, which is what lets the model predict the answer
         * without knowing a single attribute value. */
        a->argop = nv ? OP_NVERIFY : OP_VERIFY;
        v4_wide_attr_mask(s->bitmap);
        /* VERIFY's own supported set omits FATTR4_RAWDEV, though GETATTR
         * marshals it happily -- an asymmetry in chimera, recorded as an open
         * finding in DEVIATIONS-NFS4.md.  Requesting it here would answer
         * NFS4ERR_ATTRNOTSUPP before the marshaller ever runs, defeating the
         * point of the op, so drop the bit until chimera is fixed. */
        s->bitmap[1]     &= ~(1U << (FATTR4_RAWDEV - 32));
        f->attrmask       = s->bitmap;
        f->num_attrmask   = 2;
        f->attr_vals.data = s->blob;
        f->attr_vals.len  = 0;
        return 0;
    }
    if (strcmp(tag, "RVerifySize") == 0 ||
        strcmp(tag, "RNverifySize") == 0) {
        int nv = tag[1] == 'N';

        a->argop = nv ? OP_NVERIFY : OP_VERIFY;
        pack_fattr(nv ? &a->opnverify.obj_attributes
                      : &a->opverify.obj_attributes, s, -1,
                   itf_i64(v) * V4_BLOCK_SIZE);
        return 0;
    }
    if (strcmp(tag, "RSetclientid") == 0) {
        a->argop = OP_SETCLIENTID;
        pack_be64(s->verf, (uint64_t) jf_i64(v, "verfSym"));
        memcpy(a->opsetclientid.client.verifier, s->verf, 8);
        snprintf(s->owner, sizeof(s->owner), "qmbt-client-%llu-%" PRId64,
                 (unsigned long long) g_owner_epoch, jf_i64(v, "ownerSym"));
        a->opsetclientid.client.id.data                      = s->owner;
        a->opsetclientid.client.id.len                       = (uint32_t) strlen(s->owner);
        a->opsetclientid.callback.cb_program                 = 0x40000000;
        a->opsetclientid.callback.cb_location.na_r_netid.str = "tcp";
        a->opsetclientid.callback.cb_location.na_r_netid.len = 3;
        a->opsetclientid.callback.cb_location.na_r_addr.str  = "0.0.0.0.0.0";
        a->opsetclientid.callback.cb_location.na_r_addr.len  = 11;
        a->opsetclientid.callback_ident                      = 1;
        return 0;
    }
    if (strcmp(tag, "RSetclientidConfirm") == 0) {
        int64_t tok = jf_i64(v, "tok");

        if (tok < 0 || tok >= V4_MAX_TOKS || !o->confirm_known[tok]) {
            mism_add(m, "confirm token %" PRId64 " unknown", tok);
            return -1;
        }
        a->argop                          = OP_SETCLIENTID_CONFIRM;
        a->opsetclientid_confirm.clientid = o->confirm_clientid[tok];
        memcpy(a->opsetclientid_confirm.setclientid_confirm,
               o->confirm_verf[tok], 8);
        return 0;
    }
    if (strcmp(tag, "RRenew") == 0) {
        if (wire_clientid(o, itf_i64(v), &cid, m) < 0) {
            return -1;
        }
        a->argop            = OP_RENEW;
        a->oprenew.clientid = cid;
        return 0;
    }
    if (strcmp(tag, "RReleaseLockowner") == 0) {
        if (wire_clientid(o, jf_i64(v, "client"), &cid, m) < 0) {
            return -1;
        }
        a->argop                                   = OP_RELEASE_LOCKOWNER;
        a->oprelease_lockowner.lock_owner.clientid = cid;
        snprintf(s->owner, sizeof(s->owner), "lo%" PRId64,
                 jf_i64(v, "lockOwner"));
        a->oprelease_lockowner.lock_owner.owner.data = s->owner;
        a->oprelease_lockowner.lock_owner.owner.len  =
            (uint32_t) strlen(s->owner);
        return 0;
    }
    if (strcmp(tag, "RExchangeId") == 0) {
        a->argop = OP_EXCHANGE_ID;
        pack_be64(s->verf, (uint64_t) jf_i64(v, "verfSym"));
        memcpy(a->opexchange_id.eia_clientowner.co_verifier, s->verf, 8);
        snprintf(s->owner, sizeof(s->owner), "qmbt-client-%llu-%" PRId64,
                 (unsigned long long) g_owner_epoch, jf_i64(v, "ownerSym"));
        a->opexchange_id.eia_clientowner.co_ownerid.data = s->owner;
        a->opexchange_id.eia_clientowner.co_ownerid.len  =
            (uint32_t) strlen(s->owner);
        a->opexchange_id.eia_flags                 = 0;
        a->opexchange_id.eia_state_protect.spa_how = SP4_NONE;
        a->opexchange_id.num_eia_client_impl_id    = 0;
        return 0;
    }
    if (strcmp(tag, "RCreateSession") == 0) {
        struct channel_attrs4 chan = {
            .ca_headerpadsize          = 0,
            .ca_maxrequestsize         = 1048576,
            .ca_maxresponsesize        = 1048576,
            .ca_maxresponsesize_cached = 65536,
            .ca_maxoperations          = 16,
            .ca_maxrequests            = 32,
            .num_ca_rdma_ird           = 0,
        };

        if (wire_clientid(o, jf_i64(v, "client"), &cid, m) < 0) {
            return -1;
        }
        a->argop                         = OP_CREATE_SESSION;
        a->opcreate_session.csa_clientid = cid;
        a->opcreate_session.csa_sequence = (uint32_t) jf_i64(v, "csaSeq");
        a->opcreate_session.csa_flags    = jf_bool(v, "backChan")
            ? CREATE_SESSION4_FLAG_CONN_BACK_CHAN : 0;
        a->opcreate_session.csa_fore_chan_attrs = chan;
        a->opcreate_session.csa_back_chan_attrs = chan;
        a->opcreate_session.csa_cb_program      = 0x40000000;
        a->opcreate_session.num_csa_sec_parms   = 1;
        {
            static struct callback_sec_parms4 sec = { .cb_secflavor = 0 };

            a->opcreate_session.csa_sec_parms = &sec;
        }
        return 0;
    }
    if (strcmp(tag, "RSequence") == 0) {
        int64_t sess = jf_i64(v, "sess");

        if (sess < 0 || sess >= V4_MAX_SESS || !o->sess_known[sess]) {
            mism_add(m, "model session %" PRId64 " unknown", sess);
            return -1;
        }
        a->argop = OP_SEQUENCE;
        memcpy(a->opsequence.sa_sessionid, o->sess[sess], 16);
        a->opsequence.sa_sequenceid     = (uint32_t) jf_i64(v, "seq");
        a->opsequence.sa_slotid         = (uint32_t) jf_i64(v, "slot");
        a->opsequence.sa_highest_slotid = 31;
        a->opsequence.sa_cachethis      = jf_bool(v, "cacheThis");
        return 0;
    }
    if (strcmp(tag, "RDestroySession") == 0) {
        int64_t sess = itf_i64(v);

        if (sess < 0 || sess >= V4_MAX_SESS || !o->sess_known[sess]) {
            mism_add(m, "model session %" PRId64 " unknown", sess);
            return -1;
        }
        a->argop = OP_DESTROY_SESSION;
        memcpy(a->opdestroy_session.dsa_sessionid, o->sess[sess], 16);
        return 0;
    }
    if (strcmp(tag, "RBindConnToSession") == 0) {
        int64_t sess = jf_i64(v, "sess");

        if (sess < 0 || sess >= V4_MAX_SESS || !o->sess_known[sess]) {
            mism_add(m, "model session %" PRId64 " unknown", sess);
            return -1;
        }
        a->argop = OP_BIND_CONN_TO_SESSION;
        memcpy(a->opbind_conn_to_session.bctsa_sessid, o->sess[sess], 16);
        a->opbind_conn_to_session.bctsa_dir =
            (uint32_t) jf_i64(v, "dir");
        a->opbind_conn_to_session.bctsa_use_conn_in_rdma_mode = 0;
        return 0;
    }
    if (strcmp(tag, "RDestroyClientid") == 0) {
        if (wire_clientid(o, itf_i64(v), &cid, m) < 0) {
            return -1;
        }
        a->argop                           = OP_DESTROY_CLIENTID;
        a->opdestroy_clientid.dca_clientid = cid;
        return 0;
    }
    if (strcmp(tag, "RReclaimComplete") == 0) {
        a->argop                         = OP_RECLAIM_COMPLETE;
        a->opreclaim_complete.rca_one_fs = 0;
        return 0;
    }
    if (strcmp(tag, "RFreeStateid") == 0) {
        if (sid_of(o, jf_i64(v, "sid"), other, m) < 0) {
            return -1;
        }
        a->argop = OP_FREE_STATEID;
        set_stateid(&a->opfree_stateid.fsa_stateid,
                    (uint32_t) jf_i64(v, "argSeq"), other);
        return 0;
    }
    if (strcmp(tag, "ROpen") == 0) {
        json_t     *how   = json_object_get(v, "how");
        json_t     *claim = json_object_get(v, "claim");
        const char *htag;

        if (wire_clientid(o, jf_i64(v, "client"), &cid, m) < 0) {
            return -1;
        }
        a->argop                 = OP_OPEN;
        a->opopen.seqid          = (uint32_t) jf_i64(v, "oseq");
        a->opopen.share_access   = (uint32_t) jf_i64(v, "access");
        a->opopen.share_deny     = (uint32_t) jf_i64(v, "deny");
        a->opopen.owner.clientid = cid;
        snprintf(s->owner, sizeof(s->owner), "oo%" PRId64,
                 jf_i64(v, "owner"));
        a->opopen.owner.owner.data = s->owner;
        a->opopen.owner.owner.len  = (uint32_t) strlen(s->owner);

        htag = jf_tag(how);
        if (strcmp(htag, "HNoCreate") == 0) {
            a->opopen.openhow.opentype = OPEN4_NOCREATE;
        } else {
            json_t *hv = jf_val(how);

            a->opopen.openhow.opentype = OPEN4_CREATE;
            if (strcmp(htag, "HUnchecked") == 0) {
                a->opopen.openhow.how.mode = UNCHECKED4;
                pack_fattr(&a->opopen.openhow.how.createattrs, s,
                           (int) jf_i64(hv, "mode"),
                           jf_bool(hv, "truncate") ? 0 : -1);
            } else if (strcmp(htag, "HGuarded") == 0) {
                a->opopen.openhow.how.mode = GUARDED4;
                pack_fattr(&a->opopen.openhow.how.createattrs, s,
                           (int) jf_i64(hv, "mode"), -1);
            } else if (strcmp(htag, "HExclusive") == 0) {
                a->opopen.openhow.how.mode = EXCLUSIVE4;
                pack_be64(s->verf, (uint64_t) jf_i64(hv, "verf"));
                memcpy(a->opopen.openhow.how.createverf, s->verf, 8);
            } else {
                fprintf(stderr, "unknown open how %s\n", htag);
                exit(2);
            }
        }
        if (strcmp(jf_tag(claim), "OCNull") == 0) {
            const char *name = json_string_value(jf_val(claim));

            a->opopen.claim.claim = CLAIM_NULL;
            v4_expand_name(name, s->nbuf[0],
                           (const char **) &a->opopen.claim.file.data,
                           &a->opopen.claim.file.len);
        } else {
            a->opopen.claim.claim = CLAIM_FH;
        }
        return 0;
    }
    if (strcmp(tag, "ROpenConfirm") == 0) {
        if (sid_of(o, jf_i64(v, "sid"), other, m) < 0) {
            return -1;
        }
        a->argop = OP_OPEN_CONFIRM;
        set_stateid(&a->opopen_confirm.open_stateid, 0, other);
        a->opopen_confirm.seqid = (uint32_t) jf_i64(v, "oseq");
        return 0;
    }
    if (strcmp(tag, "ROpenDowngrade") == 0) {
        if (sid_of(o, jf_i64(v, "sid"), other, m) < 0) {
            return -1;
        }
        a->argop = OP_OPEN_DOWNGRADE;
        set_stateid(&a->opopen_downgrade.open_stateid,
                    (uint32_t) jf_i64(v, "argSeq"), other);
        a->opopen_downgrade.seqid        = (uint32_t) jf_i64(v, "oseq");
        a->opopen_downgrade.share_access = (uint32_t) jf_i64(v, "access");
        a->opopen_downgrade.share_deny   = (uint32_t) jf_i64(v, "deny");
        return 0;
    }
    if (strcmp(tag, "RClose") == 0) {
        if (sid_of(o, jf_i64(v, "sid"), other, m) < 0) {
            return -1;
        }
        a->argop         = OP_CLOSE;
        a->opclose.seqid = (uint32_t) jf_i64(v, "oseq");
        set_stateid(&a->opclose.open_stateid,
                    (uint32_t) jf_i64(v, "argSeq"), other);
        return 0;
    }
    if (strcmp(tag, "RRead") == 0) {
        a->argop = OP_READ;
        if (resolve_sel(o, json_object_get(v, "sel"),
                        &a->opread.stateid, m) < 0) {
            return -1;
        }
        a->opread.offset = (uint64_t) jf_i64(v, "off") * V4_BLOCK_SIZE;
        a->opread.count  = (uint32_t) (jf_i64(v, "len") * V4_BLOCK_SIZE);
        return 0;
    }
    if (strcmp(tag, "RWrite") == 0) {
        int64_t  nblk = jf_i64(v, "len");
        uint32_t len  = (uint32_t) (nblk * V4_BLOCK_SIZE);
        int64_t  i;

        a->argop = OP_WRITE;
        if (resolve_sel(o, json_object_get(v, "sel"),
                        &a->opwrite.stateid, m) < 0) {
            return -1;
        }
        a->opwrite.offset = (uint64_t) jf_i64(v, "off") * V4_BLOCK_SIZE;
        a->opwrite.stable = (stable_how4) jf_i64(v, "stable");
        if (evpl_iovec_alloc(o->env->evpl, len, 4096, 1, 0, &s->wiov) < 0) {
            fprintf(stderr, "evpl_iovec_alloc(%u) failed\n", len);
            exit(2);
        }
        for (i = 0; i < nblk; i++) {
            block_fill(jf_i64(v, "pat"),
                       (uint8_t *) s->wiov.data + i * V4_BLOCK_SIZE);
        }
        s->wiov.length         = len;
        s->wiov_used           = 1;
        a->opwrite.data.iov    = &s->wiov;
        a->opwrite.data.niov   = 1;
        a->opwrite.data.length = len;
        return 0;
    }
    if (strcmp(tag, "RCommit") == 0) {
        a->argop           = OP_COMMIT;
        a->opcommit.offset = 0;
        a->opcommit.count  = 0;
        return 0;
    }
    if (strcmp(tag, "RLock") == 0) {
        lock_range(jf_i64(v, "lo"), jf_i64(v, "hi"), &off64, &len64);
        a->argop           = OP_LOCK;
        a->oplock.locktype = jf_bool(v, "wr") ? WRITE_LT : READ_LT;
        a->oplock.reclaim  = 0;
        a->oplock.offset   = off64;
        a->oplock.length   = len64;
        if (jf_bool(v, "newOwner")) {
            int64_t open_sid = jf_i64(v, "openSid");

            if (sid_of(o, open_sid, other, m) < 0) {
                return -1;
            }
            /* open_to_lock_owner's clientid: the lock owner belongs to
             * the open's client, learned from the open stateid's maker;
             * fall back to the sole known clientid. */
            if (open_sid >= 0 && open_sid < V4_MAX_SIDS &&
                o->sid_clientid_known[open_sid]) {
                cid = o->sid_clientid[open_sid];
            } else {
                int64_t c, found = -1, nfound = 0;

                for (c = 0; c < V4_MAX_CLIENTS; c++) {
                    if (o->clientid_known[c]) {
                        found = c;
                        nfound++;
                    }
                }
                if (nfound != 1) {
                    mism_add(m, "cannot determine clientid for open sid "
                             "%" PRId64, open_sid);
                    return -1;
                }
                cid = o->clientid[found];
            }
            a->oplock.locker.new_lock_owner        = 1;
            a->oplock.locker.open_owner.open_seqid =
                (uint32_t) jf_i64(v, "oseq");
            set_stateid(&a->oplock.locker.open_owner.open_stateid, 0,
                        other);
            a->oplock.locker.open_owner.lock_seqid =
                (uint32_t) jf_i64(v, "lseq");
            a->oplock.locker.open_owner.lock_owner.clientid = cid;
            snprintf(s->owner, sizeof(s->owner), "lo%" PRId64,
                     jf_i64(v, "lockOwner"));
            a->oplock.locker.open_owner.lock_owner.owner.data = s->owner;
            a->oplock.locker.open_owner.lock_owner.owner.len  =
                (uint32_t) strlen(s->owner);
        } else {
            if (sid_of(o, jf_i64(v, "lockSid"), other, m) < 0) {
                return -1;
            }
            a->oplock.locker.new_lock_owner = 0;
            set_stateid(&a->oplock.locker.lock_owner.lock_stateid, 0,
                        other);
            a->oplock.locker.lock_owner.lock_seqid =
                (uint32_t) jf_i64(v, "lseq");
        }
        return 0;
    }
    if (strcmp(tag, "RLockt") == 0) {
        lock_range(jf_i64(v, "lo"), jf_i64(v, "hi"), &off64, &len64);
        if (wire_clientid(o, jf_i64(v, "client"), &cid, m) < 0) {
            return -1;
        }
        a->argop                  = OP_LOCKT;
        a->oplockt.locktype       = jf_bool(v, "wr") ? WRITE_LT : READ_LT;
        a->oplockt.offset         = off64;
        a->oplockt.length         = len64;
        a->oplockt.owner.clientid = cid;
        snprintf(s->owner, sizeof(s->owner), "lo%" PRId64,
                 jf_i64(v, "lockOwner"));
        a->oplockt.owner.owner.data = s->owner;
        a->oplockt.owner.owner.len  = (uint32_t) strlen(s->owner);
        return 0;
    }
    if (strcmp(tag, "RLocku") == 0) {
        lock_range(jf_i64(v, "lo"), jf_i64(v, "hi"), &off64, &len64);
        if (sid_of(o, jf_i64(v, "sid"), other, m) < 0) {
            return -1;
        }
        a->argop            = OP_LOCKU;
        a->oplocku.locktype = WRITE_LT;
        a->oplocku.seqid    = (uint32_t) jf_i64(v, "lseq");
        set_stateid(&a->oplocku.lock_stateid,
                    (uint32_t) jf_i64(v, "argSeq"), other);
        a->oplocku.offset = off64;
        a->oplocku.length = len64;
        return 0;
    }
    if (strcmp(tag, "RDelegreturn") == 0) {
        if (sid_of(o, jf_i64(v, "sid"), other, m) < 0) {
            return -1;
        }
        a->argop = OP_DELEGRETURN;
        set_stateid(&a->opdelegreturn.deleg_stateid,
                    (uint32_t) jf_i64(v, "argSeq"), other);
        return 0;
    }
    if (strcmp(tag, "RAllocate") == 0 || strcmp(tag, "RDeallocate") == 0) {
        struct stateid4 sid;

        if (resolve_sel(o, json_object_get(v, "sel"), &sid, m) < 0) {
            return -1;
        }
        off64 = (uint64_t) jf_i64(v, "off") * V4_BLOCK_SIZE;
        len64 = (uint64_t) jf_i64(v, "len") * V4_BLOCK_SIZE;
        if (tag[1] == 'A') {
            a->argop                 = OP_ALLOCATE;
            a->opallocate.aa_stateid = sid;
            a->opallocate.aa_offset  = off64;
            a->opallocate.aa_length  = len64;
        } else {
            a->argop                   = OP_DEALLOCATE;
            a->opdeallocate.da_stateid = sid;
            a->opdeallocate.da_offset  = off64;
            a->opdeallocate.da_length  = len64;
        }
        return 0;
    }
    if (strcmp(tag, "RSeek") == 0) {
        a->argop = OP_SEEK;
        if (resolve_sel(o, json_object_get(v, "sel"),
                        &a->opseek.sa_stateid, m) < 0) {
            return -1;
        }
        a->opseek.sa_offset = (uint64_t) jf_i64(v, "off") * V4_BLOCK_SIZE;
        a->opseek.sa_what   = jf_bool(v, "whatData")
            ? NFS4_CONTENT_DATA : NFS4_CONTENT_HOLE;
        return 0;
    }
    if (strcmp(tag, "RCopy") == 0) {
        a->argop = OP_COPY;
        if (resolve_sel(o, json_object_get(v, "srcSel"),
                        &a->opcopy.ca_src_stateid, m) < 0 ||
            resolve_sel(o, json_object_get(v, "dstSel"),
                        &a->opcopy.ca_dst_stateid, m) < 0) {
            return -1;
        }
        a->opcopy.ca_src_offset = (uint64_t) jf_i64(v, "srcOff") *
            V4_BLOCK_SIZE;
        a->opcopy.ca_dst_offset = (uint64_t) jf_i64(v, "dstOff") *
            V4_BLOCK_SIZE;
        a->opcopy.ca_count = (uint64_t) jf_i64(v, "cnt") *
            V4_BLOCK_SIZE;
        a->opcopy.ca_consecutive       = 1;
        a->opcopy.ca_synchronous       = 1;
        a->opcopy.num_ca_source_server = 0;
        return 0;
    }
    if (strcmp(tag, "RReadPlus") == 0) {
        a->argop = OP_READ_PLUS;
        if (resolve_sel(o, json_object_get(v, "sel"),
                        &a->opread_plus.rpa_stateid, m) < 0) {
            return -1;
        }
        a->opread_plus.rpa_offset = (uint64_t) jf_i64(v, "off") *
            V4_BLOCK_SIZE;
        a->opread_plus.rpa_count = (uint32_t) (jf_i64(v, "len") *
                                               V4_BLOCK_SIZE);
        return 0;
    }
    if (strcmp(tag, "RIoAdvise") == 0) {
        a->argop = OP_IO_ADVISE;
        if (resolve_sel(o, json_object_get(v, "sel"),
                        &a->opio_advise.iaa_stateid, m) < 0) {
            return -1;
        }
        a->opio_advise.iaa_offset = (uint64_t) jf_i64(v, "off") *
            V4_BLOCK_SIZE;
        a->opio_advise.iaa_count = (uint64_t) jf_i64(v, "len") *
            V4_BLOCK_SIZE;
        /* One hint per request: the model draws an io_advise_type4 bit
         * number and the server may honor any subset (here, none). */
        s->ia_hint                   = (uint32_t) jf_i64(v, "hint");
        a->opio_advise.num_iaa_hints = 1;
        a->opio_advise.iaa_hints     = &s->ia_hint;
        return 0;
    }
    if (strcmp(tag, "RWriteSame") == 0) {
        int64_t nblk = jf_i64(v, "blocks");

        a->argop = OP_WRITE_SAME;
        if (resolve_sel(o, json_object_get(v, "sel"),
                        &a->opwrite_same.wsa_stateid, m) < 0) {
            return -1;
        }
        a->opwrite_same.wsa_stable = FILE_SYNC4;
        /* One ADB block == one model block, so the pattern is a whole block
         * and the model's block count is the ADB block count. */
        block_fill(jf_i64(v, "pat"), s->ws_pattern);
        a->opwrite_same.wsa_adb.adb_offset = (uint64_t)
            jf_i64(v, "off") * V4_BLOCK_SIZE;
        a->opwrite_same.wsa_adb.adb_block_size  = V4_BLOCK_SIZE;
        a->opwrite_same.wsa_adb.adb_block_count = (uint64_t) nblk;
        /* NFS4_UINT64_MAX == "no per-block-number stamping"; anything else
         * selects the union arm the server rejects with UNION_NOTSUPP. */
        a->opwrite_same.wsa_adb.adb_reloff_blocknum =
            jf_bool(v, "blocknum") ? 0 : NFS4_UINT64_MAX;
        a->opwrite_same.wsa_adb.adb_block_num      = 0;
        a->opwrite_same.wsa_adb.adb_reloff_pattern = 0;
        a->opwrite_same.wsa_adb.adb_pattern.data   = s->ws_pattern;
        a->opwrite_same.wsa_adb.adb_pattern.len    = V4_BLOCK_SIZE;
        return 0;
    }
    if (strcmp(tag, "RClone") == 0) {
        a->argop = OP_CLONE;
        if (resolve_sel(o, json_object_get(v, "srcSel"),
                        &a->opclone.cl_src_stateid, m) < 0 ||
            resolve_sel(o, json_object_get(v, "dstSel"),
                        &a->opclone.cl_dst_stateid, m) < 0) {
            return -1;
        }
        a->opclone.cl_src_offset = (uint64_t) jf_i64(v, "srcOff") *
            V4_BLOCK_SIZE;
        a->opclone.cl_dst_offset = (uint64_t) jf_i64(v, "dstOff") *
            V4_BLOCK_SIZE;
        a->opclone.cl_count = (uint64_t) jf_i64(v, "cnt") * V4_BLOCK_SIZE;
        return 0;
    }
    if (strcmp(tag, "RGetxattr") == 0) {
        a->argop                    = OP_GETXATTR;
        a->opgetxattr.gxa_name.data = (void *) json_string_value(v);
        a->opgetxattr.gxa_name.len  =
            (uint32_t) strlen(json_string_value(v));
        return 0;
    }
    if (strcmp(tag, "RSetxattr") == 0) {
        const char *how = jf_tag(json_object_get(v, "how"));

        a->argop                 = OP_SETXATTR;
        a->opsetxattr.sxa_option = strcmp(how, "XsCreate") == 0 ? 1
            : (strcmp(how, "XsReplace") == 0 ? 2 : 0);
        a->opsetxattr.sxa_key.data = (void *) jf_str(v, "name");
        a->opsetxattr.sxa_key.len  =
            (uint32_t) strlen(jf_str(v, "name"));
        snprintf(s->xval, sizeof(s->xval), "xattr-value-%" PRId64,
                 jf_i64(v, "sym"));
        a->opsetxattr.sxa_value.data = s->xval;
        a->opsetxattr.sxa_value.len  = (uint32_t) strlen(s->xval);
        return 0;
    }
    if (strcmp(tag, "RListxattrs") == 0) {
        a->argop                     = OP_LISTXATTRS;
        a->oplistxattrs.lxa_cookie   = 0;
        a->oplistxattrs.lxa_maxcount = 1048576;
        return 0;
    }
    if (strcmp(tag, "RRemovexattr") == 0) {
        a->argop                       = OP_REMOVEXATTR;
        a->opremovexattr.rxa_name.data = (void *) json_string_value(v);
        a->opremovexattr.rxa_name.len  =
            (uint32_t) strlen(json_string_value(v));
        return 0;
    }
    if (strcmp(tag, "RLayoutget") == 0) {
        a->argop                                = OP_LAYOUTGET;
        a->oplayoutget.loga_signal_layout_avail = 0;
        a->oplayoutget.loga_layout_type         = g_layout_type;
        a->oplayoutget.loga_iomode              = jf_bool(v, "rw")
            ? LAYOUTIOMODE4_RW : LAYOUTIOMODE4_READ;
        a->oplayoutget.loga_offset = (uint64_t) jf_i64(v, "lo") *
            V4_BLOCK_SIZE;
        a->oplayoutget.loga_length = (uint64_t) (jf_i64(v, "hi") -
                                                 jf_i64(v, "lo")) *
            V4_BLOCK_SIZE;
        a->oplayoutget.loga_minlength = 1;
        if (resolve_sel(o, json_object_get(v, "sel"),
                        &a->oplayoutget.loga_stateid, m) < 0) {
            return -1;
        }
        a->oplayoutget.loga_maxcount = 1048576;
        return 0;
    }
    if (strcmp(tag, "RLayoutreturn") == 0) {
        if (sid_of(o, jf_i64(v, "sid"), other, m) < 0) {
            return -1;
        }
        a->argop                                          = OP_LAYOUTRETURN;
        a->oplayoutreturn.lora_reclaim                    = 0;
        a->oplayoutreturn.lora_layout_type                = g_layout_type;
        a->oplayoutreturn.lora_iomode                     = LAYOUTIOMODE4_ANY;
        a->oplayoutreturn.lora_layoutreturn.lr_returntype =
            LAYOUTRETURN4_FILE;
        a->oplayoutreturn.lora_layoutreturn.lr_layout.lrf_offset =
            (uint64_t) jf_i64(v, "lo") * V4_BLOCK_SIZE;
        a->oplayoutreturn.lora_layoutreturn.lr_layout.lrf_length =
            (uint64_t) (jf_i64(v, "hi") - jf_i64(v, "lo")) * V4_BLOCK_SIZE;
        set_stateid(&a->oplayoutreturn.lora_layoutreturn.lr_layout.
                    lrf_stateid, (uint32_t) jf_i64(v, "argSeq"), other);
        a->oplayoutreturn.lora_layoutreturn.lr_layout.lrf_body.len = 0;
        return 0;
    }
    if (strcmp(tag, "RLayoutcommit") == 0) {
        if (sid_of(o, jf_i64(v, "sid"), other, m) < 0) {
            return -1;
        }
        a->argop                      = OP_LAYOUTCOMMIT;
        a->oplayoutcommit.loca_offset = (uint64_t) jf_i64(v, "lo") *
            V4_BLOCK_SIZE;
        a->oplayoutcommit.loca_length = (uint64_t) (jf_i64(v, "hi") -
                                                    jf_i64(v, "lo")) *
            V4_BLOCK_SIZE;
        a->oplayoutcommit.loca_reclaim = 0;
        set_stateid(&a->oplayoutcommit.loca_stateid, 0, other);
        a->oplayoutcommit.loca_last_write_offset.no_newoffset = 1;
        a->oplayoutcommit.loca_last_write_offset.no_offset    =
            (uint64_t) jf_i64(v, "hi") * V4_BLOCK_SIZE - 1;
        a->oplayoutcommit.loca_time_modify.nt_timechanged = 0;
        a->oplayoutcommit.loca_layoutupdate.lou_type      = g_layout_type;
        a->oplayoutcommit.loca_layoutupdate.lou_body.len  = 0;
        return 0;
    }
    if (strcmp(tag, "RGetdeviceinfo") == 0) {
        a->argop = OP_GETDEVICEINFO;
        memcpy(a->opgetdeviceinfo.gdia_device_id,
               o->has_deviceid ? o->deviceid : (const uint8_t[16]) { 0 },
               16);
        a->opgetdeviceinfo.gdia_layout_type      = g_layout_type;
        a->opgetdeviceinfo.gdia_maxcount         = 1048576;
        a->opgetdeviceinfo.num_gdia_notify_types = 0;
        return 0;
    }
    fprintf(stderr, "no encoder for %s\n", tag);
    exit(2);
} /* encode_op */

/* ---- reply decode --------------------------------------------------------- */

/* Parse the fattr4 attr_vals blob for the five attrs the harness
 * requests, in ascending attribute order. */
static void
decode_fattr(
    const struct fattr4 *f,
    struct v4_res       *r)
{
    const uint8_t *p   = f->attr_vals.data;
    const uint8_t *end = p + f->attr_vals.len;
    uint32_t       w0  = f->num_attrmask > 0 ? f->attrmask[0] : 0;
    uint32_t       w1  = f->num_attrmask > 1 ? f->attrmask[1] : 0;

#define TAKE32(dst) do { if (p + 4 > end) { return; } \
                         (dst)                      = ((uint32_t) p[0] << 24) | (p[1] << 16) | \
                             (p[2] << 8) | p[3]; p += 4; } while (0)
#define TAKE64(dst) do { uint32_t hi_, lo_; TAKE32(hi_); TAKE32(lo_); \
                         (dst) = ((uint64_t) hi_ << 32) | lo_; } while (0)

    r->has_attrs = 1;
    if (w0 & (1U << FATTR4_TYPE)) {
        TAKE32(r->a_type);
    }
    if (w0 & (1U << FATTR4_CHANGE)) {
        TAKE64(r->a_change);
    }
    if (w0 & (1U << FATTR4_SIZE)) {
        TAKE64(r->a_size);
    }
    if (w1 & (1U << (FATTR4_MODE - 32))) {
        TAKE32(r->a_mode);
    }
    if (w1 & (1U << (FATTR4_NUMLINKS - 32))) {
        TAKE32(r->a_nlink);
    }
#undef TAKE32
#undef TAKE64
} /* decode_fattr */

static void
copy_cinfo(
    struct v4_res             *r,
    const struct change_info4 *ci,
    int                        second)
{
    if (second) {
        r->has_cinfo2    = 1;
        r->cinfo2_atomic = ci->atomic != 0;
        r->cinfo2_before = ci->before;
        r->cinfo2_after  = ci->after;
    } else {
        r->has_cinfo    = 1;
        r->cinfo_atomic = ci->atomic != 0;
        r->cinfo_before = ci->before;
        r->cinfo_after  = ci->after;
    }
} /* copy_cinfo */

struct v4_call_ctx {
    struct oracle   *o;
    struct v4_reply *rep;
};

/* Copy every oracle-relevant field of one resop into the flat summary,
 * folding the copied values into the op's fnv digest. */
static void
decode_resop(
    struct oracle           *o,
    const struct nfs_resop4 *rop,
    struct v4_res           *r,
    struct evpl             *evpl)
{
    uint32_t st = 0;
    int      i;

    memset(r, 0, sizeof(*r));

    switch (rop->resop) {
        case OP_ACCESS:
            st = rop->opaccess.status;
            if (st == NFS4_OK) {
                r->supported = rop->opaccess.resok4.supported;
                r->access    = rop->opaccess.resok4.access;
            }
            break;
        case OP_CLOSE:
            st = rop->opclose.status;
            if (st == NFS4_OK) {
                r->has_sid = 1;
                r->sid     = rop->opclose.open_stateid;
            }
            break;
        case OP_COMMIT:
            st = rop->opcommit.status;
            if (st == NFS4_OK) {
                memcpy(r->verf, rop->opcommit.resok4.writeverf, 8);
            }
            break;
        case OP_CREATE:
            st = rop->opcreate.status;
            if (st == NFS4_OK) {
                copy_cinfo(r, &rop->opcreate.resok4.cinfo, 0);
            }
            break;
        case OP_DELEGRETURN:
            st = rop->opdelegreturn.status;
            break;
        case OP_GETATTR:
            st = rop->opgetattr.status;
            if (st == NFS4_OK) {
                decode_fattr(&rop->opgetattr.resok4.obj_attributes, r);
            }
            break;
        case OP_GETFH:
            st = rop->opgetfh.status;
            if (st == NFS4_OK) {
                mbt_copy_fh(&r->fh, &rop->opgetfh.resok4.object);
            }
            break;
        case OP_LINK:
            st = rop->oplink.status;
            if (st == NFS4_OK) {
                copy_cinfo(r, &rop->oplink.resok4.cinfo, 0);
            }
            break;
        case OP_LOCK:
            st = rop->oplock.status;
            if (st == NFS4_OK) {
                r->has_sid = 1;
                r->sid     = rop->oplock.resok4.lock_stateid;
            } else if (st == E_DENIED) {
                r->has_denied       = 1;
                r->denied_owner_len =
                    rop->oplock.denied.owner.owner.len <
                    sizeof(r->denied_owner)
                    ? rop->oplock.denied.owner.owner.len
                    : sizeof(r->denied_owner);
                memcpy(r->denied_owner,
                       rop->oplock.denied.owner.owner.data,
                       r->denied_owner_len);
            }
            break;
        case OP_LOCKT:
            st = rop->oplockt.status;
            if (st == E_DENIED) {
                r->has_denied       = 1;
                r->denied_owner_len =
                    rop->oplockt.denied.owner.owner.len <
                    sizeof(r->denied_owner)
                    ? rop->oplockt.denied.owner.owner.len
                    : sizeof(r->denied_owner);
                memcpy(r->denied_owner,
                       rop->oplockt.denied.owner.owner.data,
                       r->denied_owner_len);
            }
            break;
        case OP_LOCKU:
            st = rop->oplocku.status;
            if (st == NFS4_OK) {
                r->has_sid = 1;
                r->sid     = rop->oplocku.lock_stateid;
            }
            break;
        case OP_LOOKUP:
            st = rop->oplookup.status;
            break;
        case OP_SECINFO:
            st = rop->opsecinfo.status;
            break;
        case OP_LOOKUPP:
            st = rop->oplookupp.status;
            break;
        case OP_NVERIFY:
            st = rop->opnverify.status;
            break;
        case OP_VERIFY:
            st = rop->opverify.status;
            break;
        case OP_OPEN:
            st = rop->opopen.status;
            if (st == NFS4_OK) {
                const struct OPEN4resok *ok = &rop->opopen.resok4;

                r->has_sid = 1;
                r->sid     = ok->stateid;
                copy_cinfo(r, &ok->cinfo, 0);
                r->rflags     = ok->rflags;
                r->deleg_type = ok->delegation.delegation_type;
                if (r->deleg_type == OPEN_DELEGATE_READ) {
                    r->deleg_sid = ok->delegation.read.stateid;
                } else if (r->deleg_type == OPEN_DELEGATE_WRITE) {
                    r->deleg_sid = ok->delegation.write.stateid;
                }
            }
            break;
        case OP_OPEN_CONFIRM:
            st = rop->opopen_confirm.status;
            if (st == NFS4_OK) {
                r->has_sid = 1;
                r->sid     = rop->opopen_confirm.resok4.open_stateid;
            }
            break;
        case OP_OPEN_DOWNGRADE:
            st = rop->opopen_downgrade.status;
            if (st == NFS4_OK) {
                r->has_sid = 1;
                r->sid     = rop->opopen_downgrade.resok4.open_stateid;
            }
            break;
        case OP_PUTFH:
            st = rop->opputfh.status;
            break;
        case OP_PUTROOTFH:
            st = rop->opputrootfh.status;
            break;
        case OP_READ:
            st = rop->opread.status;
            if (st == NFS4_OK) {
                uint32_t off = 0;
                uint32_t n;

                r->eof  = rop->opread.resok4.eof != 0;
                r->data = o->arena + o->arena_used;
                for (i = 0; i < rop->opread.resok4.data.niov; i++) {
                    n = rop->opread.resok4.data.iov[i].length;
                    if (o->arena_used + off + n > V4_DATA_ARENA) {
                        n = V4_DATA_ARENA - o->arena_used - off;
                    }
                    memcpy(r->data + off,
                           rop->opread.resok4.data.iov[i].data, n);
                    off += n;
                    evpl_iovec_release(evpl,
                                       &rop->opread.resok4.data.iov[i]);
                }
                r->data_len    = off;
                o->arena_used += off;
            }
            break;
        case OP_READDIR:
            st = rop->opreaddir.status;
            if (st == NFS4_OK) {
                const struct entry4 *e;

                r->eof = rop->opreaddir.resok4.reply.eof != 0;
                for (e = rop->opreaddir.resok4.reply.entries; e;
                     e = e->nextentry) {
                    if (r->nnames >= V4_MAX_NAMES) {
                        r->names_overflow = 1;
                        break;
                    }
                    uint32_t n = e->name.len < V4_NAME_LEN - 1
                        ? e->name.len : V4_NAME_LEN - 1;

                    memcpy(r->names[r->nnames], e->name.data, n);
                    r->names[r->nnames][n] = '\0';
                    r->nnames++;
                }
            }
            break;
        case OP_READLINK:
            st = rop->opreadlink.status;
            if (st == NFS4_OK) {
                uint32_t n = rop->opreadlink.resok4.link.len <
                    V4_NAME_LEN - 1 ? rop->opreadlink.resok4.link.len
                    : V4_NAME_LEN - 1;

                memcpy(r->target, rop->opreadlink.resok4.link.data, n);
                r->target[n]  = '\0';
                r->target_len = n;
            }
            break;
        case OP_REMOVE:
            st = rop->opremove.status;
            if (st == NFS4_OK) {
                copy_cinfo(r, &rop->opremove.resok4.cinfo, 0);
            }
            break;
        case OP_RENAME:
            st = rop->oprename.status;
            if (st == NFS4_OK) {
                copy_cinfo(r, &rop->oprename.resok4.source_cinfo, 0);
                copy_cinfo(r, &rop->oprename.resok4.target_cinfo, 1);
            }
            break;
        case OP_RENEW:
            st = rop->oprenew.status;
            break;
        case OP_RESTOREFH:
            st = rop->oprestorefh.status;
            break;
        case OP_SAVEFH:
            st = rop->opsavefh.status;
            break;
        case OP_SETATTR:
            st = rop->opsetattr.status;
            break;
        case OP_SETCLIENTID:
            st = rop->opsetclientid.status;
            if (st == NFS4_OK) {
                r->clientid = rop->opsetclientid.resok4.clientid;
                memcpy(r->confirm,
                       rop->opsetclientid.resok4.setclientid_confirm, 8);
            }
            break;
        case OP_SETCLIENTID_CONFIRM:
            st = rop->opsetclientid_confirm.status;
            break;
        case OP_WRITE:
            st = rop->opwrite.status;
            if (st == NFS4_OK) {
                r->count = rop->opwrite.resok4.count;
                memcpy(r->verf, rop->opwrite.resok4.writeverf, 8);
            }
            break;
        case OP_RELEASE_LOCKOWNER:
            st = rop->oprelease_lockowner.status;
            break;
        case OP_EXCHANGE_ID:
            st = rop->opexchange_id.eir_status;
            if (st == NFS4_OK) {
                r->clientid   = rop->opexchange_id.eir_resok4.eir_clientid;
                r->sequenceid = rop->opexchange_id.eir_resok4.eir_sequenceid;
                r->flags      = rop->opexchange_id.eir_resok4.eir_flags;
            }
            break;
        case OP_CREATE_SESSION:
            st = rop->opcreate_session.csr_status;
            if (st == NFS4_OK) {
                memcpy(r->sessionid,
                       rop->opcreate_session.csr_resok4.csr_sessionid, 16);
                r->fore_slots = rop->opcreate_session.csr_resok4.
                    csr_fore_chan_attrs.ca_maxrequests;
            }
            break;
        case OP_BIND_CONN_TO_SESSION:
            st = rop->opbind_conn_to_session.bctsr_status;
            break;
        case OP_DESTROY_SESSION:
            st = rop->opdestroy_session.dsr_status;
            break;
        case OP_DESTROY_CLIENTID:
            st = rop->opdestroy_clientid.dcr_status;
            break;
        case OP_FREE_STATEID:
            st = rop->opfree_stateid.fsr_status;
            break;
        case OP_RECLAIM_COMPLETE:
            st = rop->opreclaim_complete.rcr_status;
            break;
        case OP_SEQUENCE:
            st = rop->opsequence.sr_status;
            break;
        case OP_ALLOCATE:
            st = rop->opallocate.ar_status;
            break;
        case OP_DEALLOCATE:
            st = rop->opdeallocate.dr_status;
            break;
        case OP_SEEK:
            st = rop->opseek.sa_status;
            if (st == NFS4_OK) {
                r->eof    = rop->opseek.resok4.sr_eof != 0;
                r->offset = rop->opseek.resok4.sr_offset;
            }
            break;
        case OP_READ_PLUS:
            st = rop->opread_plus.rp_status;
            if (st == NFS4_OK) {
                uint32_t nc = rop->opread_plus.rp_resok4.num_rpr_contents;
                uint32_t ci;

                r->rp_present = 1;
                r->eof        = rop->opread_plus.rp_resok4.rpr_eof != 0;
                if (nc > V4_RP_MAX_SEGS) {
                    r->rp_bad = 1;
                    break;
                }
                for (ci = 0; ci < nc; ci++) {
                    struct read_plus_content *c =
                        &rop->opread_plus.rp_resok4.rpr_contents[ci];
                    int                       j = r->rp_nsegs;

                    if (c->rpc_content == NFS4_CONTENT_DATA) {
                        uint32_t n = c->rpc_data.d_data.len;

                        if (o->arena_used + n > V4_DATA_ARENA) {
                            r->rp_bad = 1;
                            break;
                        }
                        r->rp_segs[j].is_data = 1;
                        r->rp_segs[j].offset  = c->rpc_data.d_offset;
                        r->rp_segs[j].length  = n;
                        r->rp_segs[j].data    = o->arena + o->arena_used;
                        memcpy(r->rp_segs[j].data, c->rpc_data.d_data.data, n);
                        o->arena_used += n;
                    } else if (c->rpc_content == NFS4_CONTENT_HOLE) {
                        r->rp_segs[j].is_data = 0;
                        r->rp_segs[j].offset  = c->rpc_hole.di_offset;
                        r->rp_segs[j].length  = c->rpc_hole.di_length;
                        r->rp_segs[j].data    = NULL;
                    } else {
                        r->rp_bad = 1;
                        break;
                    }
                    r->rp_nsegs++;
                }
            }
            break;
        case OP_IO_ADVISE:
            st = rop->opio_advise.ior_status;
            if (st == NFS4_OK) {
                uint32_t hi;

                r->ia_nhints = rop->opio_advise.resok4.num_ior_hints;
                for (hi = 0; hi < r->ia_nhints &&
                     hi < V4_IA_MAX_HINTS; hi++) {
                    r->ia_hints[hi] = rop->opio_advise.resok4.ior_hints[hi];
                }
            }
            break;
        case OP_WRITE_SAME:
            st = rop->opwrite_same.wsr_status;
            if (st == NFS4_OK) {
                r->count = (uint32_t) rop->opwrite_same.resok4.wr_count;
            }
            break;
        case OP_CLONE:
            st = rop->opclone.cl_status;
            break;
        case OP_COPY:
            st = rop->opcopy.cr_status;
            if (st == NFS4_OK) {
                r->count = (uint32_t)
                    rop->opcopy.cr_resok4.cr_response.wr_count;
            }
            break;
        case OP_GETXATTR:
            st = rop->opgetxattr.gxr_status;
            if (st == NFS4_OK) {
                uint32_t n = rop->opgetxattr.gxr_value.len <
                    V4_NAME_LEN - 1 ? rop->opgetxattr.gxr_value.len
                    : V4_NAME_LEN - 1;

                memcpy(r->xvalue, rop->opgetxattr.gxr_value.data, n);
                r->xvalue[n]  = '\0';
                r->xvalue_len = n;
            }
            break;
        case OP_SETXATTR:
            st = rop->opsetxattr.sxr_status;
            break;
        case OP_LISTXATTRS:
            st = rop->oplistxattrs.lxr_status;
            if (st == NFS4_OK) {
                const struct LISTXATTRS4resok *ok =
                    &rop->oplistxattrs.lxr_value;
                uint32_t                       j;

                r->eof = ok->lxr_eof != 0;
                for (j = 0; j < ok->num_lxr_names; j++) {
                    if (r->nnames >= V4_MAX_NAMES) {
                        r->names_overflow = 1;
                        break;
                    }
                    uint32_t n = ok->lxr_names[j].xn_name.len < V4_NAME_LEN - 1
                        ? ok->lxr_names[j].xn_name.len : V4_NAME_LEN - 1;

                    memcpy(r->names[r->nnames], ok->lxr_names[j].xn_name.data, n);
                    r->names[r->nnames][n] = '\0';
                    r->nnames++;
                }
            }
            break;
        case OP_REMOVEXATTR:
            st = rop->opremovexattr.rxr_status;
            break;
        case OP_LAYOUTGET:
            st = rop->oplayoutget.logr_status;
            if (st == NFS4_OK) {
                const struct LAYOUTGET4resok *ok =
                    &rop->oplayoutget.logr_resok4;
                uint32_t                      j;

                r->has_sid = 1;
                r->sid     = ok->logr_stateid;
                for (j = 0; j < ok->num_logr_layout && j < 4; j++) {
                    r->segs[r->nsegs].offset = ok->logr_layout[j].lo_offset;
                    r->segs[r->nsegs].length = ok->logr_layout[j].lo_length;
                    r->nsegs++;
                }
                if (ok->num_logr_layout > 0) {
                    const xdr_opaque *body =
                        &ok->logr_layout[0].lo_content.loc_body;
                    /* Where the deviceid sits in the body depends on the
                     * layout type: an ff_layout4 carries it inside its first
                     * ff_data_server4 (RFC 8435 5.1); a pnfs_block_layout4
                     * opens with the extent count and then bex_vol_id
                     * (RFC 5663 2.3.1).  A client has to parse it out either
                     * way to have anything to hand GETDEVICEINFO. */
                    uint32_t          off = (g_layout_type == V4_LAYOUT_FLEX)
                        ? V4_FF_DEVICEID_OFF : 4;

                    if (body->len >= off + 16) {
                        memcpy(r->deviceid, body->data + off, 16);
                        r->has_deviceid = 1;
                    }
                }
            }
            break;
        case OP_LAYOUTRETURN:
            st = rop->oplayoutreturn.lorr_status;
            if (st == NFS4_OK) {
                r->lr_present =
                    rop->oplayoutreturn.lorr_stateid.lrs_present != 0;
                if (r->lr_present) {
                    r->has_sid = 1;
                    r->sid     = rop->oplayoutreturn.lorr_stateid.lrs_stateid;
                }
            }
            break;
        case OP_LAYOUTCOMMIT:
            st = rop->oplayoutcommit.locr_status;
            if (st == NFS4_OK) {
                r->has_newsize = rop->oplayoutcommit.locr_resok4.locr_newsize.
                    ns_sizechanged != 0;
                if (r->has_newsize) {
                    r->newsize = rop->oplayoutcommit.locr_resok4.locr_newsize.
                        ns_size;
                }
            }
            break;
        case OP_GETDEVICEINFO:
            st = rop->opgetdeviceinfo.gdir_status;
            break;
        default:
            fprintf(stderr, "no decoder for resop %d\n", rop->resop);
            exit(2);
    } /* switch */

    r->status = st;

    /* Digest for the SEQUENCE replay-cache comparison. */
    r->fnv = fnv64(0, &r->status, sizeof(r->status));
    r->fnv = fnv64(r->fnv, &r->sid, sizeof(r->sid));
    r->fnv = fnv64(r->fnv, &r->a_change, sizeof(r->a_change));
    r->fnv = fnv64(r->fnv, &r->a_size, sizeof(r->a_size));
    r->fnv = fnv64(r->fnv, &r->count, sizeof(r->count));
    r->fnv = fnv64(r->fnv, r->verf, sizeof(r->verf));
    r->fnv = fnv64(r->fnv, &r->cinfo_after, sizeof(r->cinfo_after));
    r->fnv = fnv64(r->fnv, &r->clientid, sizeof(r->clientid));
    r->fnv = fnv64(r->fnv, r->sessionid, sizeof(r->sessionid));
    r->fnv = fnv64(r->fnv, &r->eof, sizeof(r->eof));
    r->fnv = fnv64(r->fnv, &r->offset, sizeof(r->offset));
    if (r->data_len) {
        r->fnv = fnv64(r->fnv, r->data, r->data_len);
    }
    for (i = 0; i < r->nnames; i++) {
        r->fnv = fnv64(r->fnv, r->names[i], strlen(r->names[i]));
    }
    if (r->fh.has) {
        r->fnv = fnv64(r->fnv, r->fh.data, r->fh.len);
    }
} /* decode_resop */

static void
v4_compound_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *reply,
    int                          status,
    void                        *private_data)
{
    struct v4_call_ctx *ctx = private_data;
    uint32_t            i;

    ctx->rep->rpc_err = status;
    if (status == 0) {
        ctx->rep->status = reply->status;
        ctx->rep->nres   = (int) reply->num_resarray;
        if (ctx->rep->nres > V4_MAX_OPS) {
            ctx->rep->nres = V4_MAX_OPS;
        }
        for (i = 0; i < (uint32_t) ctx->rep->nres; i++) {
            decode_resop(ctx->o, &reply->resarray[i], &ctx->rep->res[i],
                         evpl);
        }
    }
    ctx->rep->done = 1;
} /* v4_compound_cb */

/* ---- result checking ------------------------------------------------------ */

struct v4_ctx {
    int64_t cur;      /* current-FH ino per the model's own threading */
    int64_t saved;
    int     abort;
};

static void
check_attrs(
    struct oracle *o,
    json_t        *exp,
    struct v4_res *r,
    int64_t        ino,
    struct mism   *m)
{
    const char *ft   = jf_tag(json_object_get(exp, "ftype"));
    uint32_t    want = (uint32_t) v4_ftype_wire(ft);

    if (r->a_type != want) {
        mism_add(m, "getattr.type: expected %u (%s), got %u",
                 want, ft, r->a_type);
    }
    if (r->a_mode != (uint32_t) jf_i64(exp, "mode")) {
        if (strcmp(ft, "FLnk") == 0 && r->a_mode == 0755) {
            o->dev_hits[DEV_SYMLINK_MODE_0755]++;
        } else {
            mism_add(m, "getattr.mode: expected %#o, got %#o",
                     (unsigned) jf_i64(exp, "mode"), r->a_mode);
        }
    }
    if (r->a_nlink != (uint32_t) jf_i64(exp, "nlink")) {
        mism_add(m, "getattr.numlinks: expected %" PRId64 ", got %u",
                 jf_i64(exp, "nlink"), r->a_nlink);
    }
    if (strcmp(ft, "FReg") == 0) {
        uint64_t want_size = (uint64_t) jf_i64(exp, "sizeBlocks") *
            V4_BLOCK_SIZE;

        if (r->a_size != want_size) {
            mism_add(m, "getattr.size: expected %" PRIu64 ", got %" PRIu64,
                     want_size, r->a_size);
        }
    }
    check_change(o, ino, jf_i64(exp, "change"), r->a_change, m,
                 "getattr.change");
} /* check_attrs */

static void
check_deleg(
    struct oracle *o,
    json_t        *exp,
    struct v4_res *r,
    struct mism   *m)
{
    const char *etag = jf_tag(exp);
    uint32_t    want;

    if (strcmp(etag, "DNone") == 0) {
        if (r->deleg_type != 0) {
            caps_mismatch(o, m,
                          r->deleg_type == 1 ? "readDeleg" : "writeDeleg",
                          "trace assumes no delegation, server granted "
                          "type %u", r->deleg_type);
        }
        return;
    }
    want = strcmp(etag, "DRead") == 0 ? 1 : 2;
    if (r->deleg_type == 0) {
        caps_mismatch(o, m, want == 1 ? "readDeleg" : "writeDeleg",
                      "trace assumes a delegation grant, server granted "
                      "none");
        return;
    }
    if (r->deleg_type != want) {
        mism_add(m, "open.delegation: expected type %u, got %u",
                 want, r->deleg_type);
        return;
    }
    learn_sid(o, jf_i64(jf_val(exp), "sid"), &r->deleg_sid, m, 1,
              "deleg.stateid");
} /* check_deleg */

#define SPARSE_TAG(t)   (strcmp(t, "SAllocate") == 0 || \
                         strcmp(t, "SDeallocate") == 0 || \
                         strcmp(t, "SSeek") == 0)
/* Ops that run the shared regular-file type gate (see D4-15). */
/* ...of which these four now answer the per-type split directly
 * (chimera_nfs4_data_nonreg_status), so their symlink arm must match
 * exactly rather than fall through the D4-15 acceptance. */
#define DATAGATE_TAG(t) (strcmp(t, "SRead") == 0 || \
                         strcmp(t, "SWrite") == 0 || \
                         strcmp(t, "SCommit") == 0 || \
                         strcmp(t, "SLockt") == 0)
#define TYPEGATE_TAG(t) (strcmp(t, "SRead") == 0 || \
                         strcmp(t, "SWrite") == 0 || \
                         strcmp(t, "SCommit") == 0 || \
                         strcmp(t, "SLockt") == 0 || \
                         strcmp(t, "SSetattr") == 0 || \
                         SPARSE_TAG(t))
#define XATTR_TAG(t)    (strcmp(t, "SGetxattr") == 0 || \
                         strcmp(t, "SSetxattr") == 0 || \
                         strcmp(t, "SListxattrs") == 0 || \
                         strcmp(t, "SRemovexattr") == 0)
#define PNFS_TAG(t)     (strcmp(t, "SLayoutget") == 0 || \
                         strcmp(t, "SLayoutreturn") == 0 || \
                         strcmp(t, "SLayoutcommit") == 0 || \
                         strcmp(t, "SGetdeviceinfo") == 0)

/* Divergence in status: capability reconciliation or mismatch.  Mirrors
 * Replayer.classify_status_mismatch. */
static void
classify_status_mismatch(
    struct oracle *o,
    const char    *tag,
    uint32_t       est,
    uint32_t       ast,
    struct mism   *m)
{
    int has_notsupp = est == E_NOTSUPP || ast == E_NOTSUPP;

    if (SPARSE_TAG(tag) && has_notsupp) {
        caps_mismatch(o, m, "sparse", "%s: expected %u, got %u",
                      tag, est, ast);
        return;
    }
    if (strcmp(tag, "SCopy") == 0 && has_notsupp) {
        caps_mismatch(o, m, "copy", "%s: expected %u, got %u",
                      tag, est, ast);
        return;
    }
    /* The RFC 7862 data ops are per-backend capabilities (CAP_READ_PLUS,
     * CAP_WRITE_SAME, CAP_CLONE_RANGE): a backend without one answers
     * NFS4ERR_NOTSUPP, which is a profile mismatch rather than a defect
     * unless the instance pinned the feature FeatMandatory. */
    if (strcmp(tag, "SReadPlus") == 0 && has_notsupp) {
        caps_mismatch(o, m, "readPlus", "%s: expected %u, got %u",
                      tag, est, ast);
        return;
    }
    if (strcmp(tag, "SIoAdvise") == 0 && has_notsupp) {
        caps_mismatch(o, m, "ioAdvise", "%s: expected %u, got %u",
                      tag, est, ast);
        return;
    }
    if (strcmp(tag, "SWriteSame") == 0 && has_notsupp) {
        caps_mismatch(o, m, "writeSame", "%s: expected %u, got %u",
                      tag, est, ast);
        return;
    }
    if (strcmp(tag, "SClone") == 0 && has_notsupp) {
        caps_mismatch(o, m, "clone", "%s: expected %u, got %u",
                      tag, est, ast);
        return;
    }
    if (XATTR_TAG(tag) && has_notsupp) {
        caps_mismatch(o, m, "xattr", "%s: expected %u, got %u",
                      tag, est, ast);
        return;
    }
    if (PNFS_TAG(tag) &&
        (has_notsupp || est == E_LAYOUTUNAVAILABLE ||
         ast == E_LAYOUTUNAVAILABLE)) {
        caps_mismatch(o, m, "pnfs", "%s: expected %u, got %u",
                      tag, est, ast);
        return;
    }
    /* D4-7: wherever a path operation meets a symlink where a directory was
    * required, chimera answers the more specific NFS4ERR_SYMLINK and the
    * model predicts the generic NFS4ERR_NOTDIR.  RFC 7530 Table 7 lists both
    * for every op that can hit it -- LOOKUPP and READDIR on a symlink cfh,
    * and OPEN/CREATE/REMOVE/RENAME/LINK on a symlink *parent* -- so either is
    * conformant.  Matched on the (NOTDIR, SYMLINK) status pair rather than an
    * op list: the pair itself is the deviation, and enumerating tags would
    * silently miss each new op the generator learns to aim at a symlink. */
    if (est == NFS4ERR_NOTDIR && ast == V4_ERR_SYMLINK) {
        o->dev_hits[DEV_LOOKUPP_SYMLINK]++;
        o->status_dev = ast;
        return;
    }
    /* D4-15: chimera answers the coarse 4.0-style type error on every
     * data-path and size-setting op -- NFS4ERR_ISDIR for a directory,
     * NFS4ERR_INVAL for every other non-regular object, in all minor
     * versions -- where the model predicts the per-type split (SYMLINK for a
     * symlink; WRONG_TYPE for a special file, and for a directory on a 4.1+
     * SETATTR(size)).  Both are conformant: RFC 7530 Table 7 lists SYMLINK
     * *and* INVAL for READ/WRITE/COMMIT on a symlink cfh (R-CORE-91/96/97),
     * and RFC 8881 15.1.2.9 makes WRONG_TYPE the more specific successor of
     * INVAL (R-ATTR-25 marks the exact choice server-specific).  The model
     * stays RFC-first and reconciles here, exactly as D4-7 does. */
    /* D4-16: READLINK on a *directory* answers NFS4ERR_INVAL where the model
    * predicts NFS4ERR_ISDIR.  Here the model is right and chimera is not --
    * RFC 7530 §16.25.4/.5 Table 7 (rfc-notes R-CORE-86) and RFC 8881
    * §18.24.3 both make a directory cfh ISDIR, with INVAL reserved for the
    * other non-symlink types.  It is registered rather than fixed because
    * pynfs RDLK2d (st_readlink.testDir) asserts INVAL for a directory, so the
    * RFC-correct answer fails the legacy suite: chimera stays bug-compatible
    * with pynfs on purpose and the model tolerates it here.  Retire this row
    * and restore the split in nfs4_proc_readlink.c if pynfs is corrected. */
    if (strcmp(tag, "SReadlink") == 0 && est == NFS4ERR_ISDIR &&
        ast == NFS4ERR_INVAL) {
        o->dev_hits[DEV_READLINK_DIR_INVAL]++;
        o->status_dev = ast;
        return;
    }
    if (TYPEGATE_TAG(tag) &&
        (est == V4_ERR_SYMLINK || est == E_WRONG_TYPE) &&
        !(DATAGATE_TAG(tag) && est == V4_ERR_SYMLINK) &&
        (ast == NFS4ERR_INVAL ||
         (ast == NFS4ERR_ISDIR && strcmp(tag, "SSetattr") == 0))) {
        o->dev_hits[DEV_COARSE_TYPE_ERR]++;
        o->status_dev = ast;
        return;
    }
    if (strcmp(tag, "SClose") == 0 &&
        ((est == NFS4_OK && ast == E_LOCKS_HELD) ||
         (est == E_LOCKS_HELD && ast == NFS4_OK))) {
        caps_mismatch(o, m, "closeFreesLocks", "expected %u, got %u",
                      est, ast);
        return;
    }
    if (strcmp(tag, "SOpen") == 0 && est == E_DELAY && ast == NFS4_OK) {
        caps_mismatch(o, m, "conflictDelays",
                      "model expected NFS4ERR_DELAY during recall, server "
                      "completed the op");
        return;
    }
    mism_add(m, "%s: status: expected %u, got %u", tag, est, ast);
} /* classify_status_mismatch */

/* Compare one expected OpRes against the wire result summary. */
static void
check_result(
    struct oracle *o,
    json_t        *exp,
    json_t        *req,
    struct v4_res *r,
    struct v4_ctx *ctx,
    struct mism   *m)
{
    const char *tag = jf_tag(exp);
    json_t     *v   = jf_val(exp);
    uint32_t    est = (uint32_t) jf_i64(v, "st");
    uint32_t    ast = r->status;

    if (est != ast) {
        if (strcmp(tag, "SLookupp") == 0 && est == NFS4ERR_NOENT &&
            ast == NFS4_OK && ctx->cur == 0) {
            o->dev_hits[DEV_LOOKUPP_PSEUDOROOT]++;
            ctx->abort = 1;
            return;
        }
        classify_status_mismatch(o, tag, est, ast, m);
        return;
    }

    if (ast != NFS4_OK) {
        /* Error statuses matched; DENIED bodies still carry payload. */
        if ((strcmp(tag, "SLock") == 0 || strcmp(tag, "SLockt") == 0) &&
            ast == E_DENIED && r->has_denied) {
            char want[64];

            snprintf(want, sizeof(want), "lo%" PRId64,
                     jf_i64(v, "deniedOwner"));
            if (strlen(want) != r->denied_owner_len ||
                memcmp(want, r->denied_owner, r->denied_owner_len) != 0) {
                mism_add(m, "%s: denied owner: expected %s, got %.*s",
                         tag, want, (int) r->denied_owner_len,
                         (const char *) r->denied_owner);
            }
        }
        return;
    }

    if (strcmp(tag, "SGetfh") == 0) {
        learn_fh(o, jf_i64(v, "ino"), &r->fh, m);
        ctx->cur = jf_i64(v, "ino");
    } else if (strcmp(tag, "SLookup") == 0) {
        ctx->cur = jf_i64(v, "child");
    } else if (strcmp(tag, "SLookupp") == 0) {
        ctx->cur = jf_i64(v, "parent");
    } else if (strcmp(tag, "SGetattr") == 0) {
        check_attrs(o, json_object_get(v, "attrs"), r, ctx->cur, m);
    } else if (strcmp(tag, "SReadlink") == 0) {
        const char *want = jf_str(v, "target");

        if (strlen(want) != r->target_len ||
            memcmp(want, r->target, r->target_len) != 0) {
            mism_add(m, "readlink: expected '%s', got '%.*s'",
                     want, (int) r->target_len, r->target);
        }
    } else if (strcmp(tag, "SAccess") == 0) {
        uint32_t esup = (uint32_t) jf_i64(v, "supported");
        uint32_t eacc = (uint32_t) jf_i64(v, "access");

        if (r->supported == esup && r->access == eacc) {
            /* exact */
        } else if ((r->supported == (esup & 0x1f) &&
                    r->access == (eacc & 0x1f)) ||
                   (r->supported == (esup & 0x2d) &&
                    r->access == (eacc & 0x2d))) {
            /* Chimera restricts supported/access to type-applicable
             * bits (dirs: no EXECUTE; files: no LOOKUP/DELETE). */
            o->dev_hits[DEV_ACCESS_NO_EXECUTE]++;
        } else {
            if (r->supported != esup) {
                mism_add(m, "access.supported: expected %#x, got %#x",
                         esup, r->supported);
            }
            if (r->access != eacc) {
                mism_add(m, "access.access: expected %#x, got %#x",
                         eacc, r->access);
            }
        }
    } else if (strcmp(tag, "SReaddir") == 0) {
        json_t *names = itf_seq(json_object_get(v, "names"));
        json_t *jn;
        size_t  i;
        int     j;

        if (r->names_overflow) {
            mism_add(m, "readdir: more than %d entries", V4_MAX_NAMES);
            return;
        }
        for (j = 0; j < r->nnames; j++) {
            for (i = (size_t) j + 1; i < (size_t) r->nnames; i++) {
                if (strcmp(r->names[j], r->names[i]) == 0) {
                    mism_add(m, "readdir: duplicate entry '%s'",
                             r->names[j]);
                }
            }
        }
        json_array_foreach(names, i, jn)
        {
            int found = 0;

            for (j = 0; j < r->nnames; j++) {
                if (v4_name_eq(r->names[j], json_string_value(jn))) {
                    found = 1;
                    break;
                }
            }
            if (!found) {
                mism_add(m, "readdir: expected entry '%s' missing",
                         json_string_value(jn));
            }
        }
        for (j = 0; j < r->nnames; j++) {
            int found = 0;

            json_array_foreach(names, i, jn)
            {
                if (v4_name_eq(r->names[j], json_string_value(jn))) {
                    found = 1;
                    break;
                }
            }
            if (!found) {
                mism_add(m, "readdir: unexpected entry '%s'", r->names[j]);
            }
        }
        if (!r->eof) {
            mism_add(m, "readdir: eof unset on full listing");
        }
    } else if (strcmp(tag, "SCreate") == 0) {
        check_cinfo(o, ctx->cur, json_object_get(v, "cinfo"),
                    r->cinfo_atomic, r->cinfo_before, r->cinfo_after, m,
                    "create.cinfo");
        ctx->cur = jf_i64(v, "ino");
    } else if (strcmp(tag, "SRemove") == 0) {
        check_cinfo(o, ctx->cur, json_object_get(v, "cinfo"),
                    r->cinfo_atomic, r->cinfo_before, r->cinfo_after, m,
                    "remove.cinfo");
    } else if (strcmp(tag, "SRename") == 0) {
        check_cinfo(o, ctx->saved, json_object_get(v, "cinfoS"),
                    r->cinfo_atomic, r->cinfo_before, r->cinfo_after, m,
                    "rename.cinfoS");
        check_cinfo(o, ctx->cur, json_object_get(v, "cinfoT"),
                    r->cinfo2_atomic, r->cinfo2_before, r->cinfo2_after, m,
                    "rename.cinfoT");
    } else if (strcmp(tag, "SLink") == 0) {
        check_cinfo(o, ctx->cur, json_object_get(v, "cinfo"),
                    r->cinfo_atomic, r->cinfo_before, r->cinfo_after, m,
                    "link.cinfo");
    } else if (strcmp(tag, "SSetclientid") == 0) {
        int64_t tok    = jf_i64(v, "tok");
        int64_t client = jf_i64(v, "client");

        if (tok >= 0 && tok < V4_MAX_TOKS) {
            o->confirm_clientid[tok] = r->clientid;
            memcpy(o->confirm_verf[tok], r->confirm, 8);
            o->confirm_known[tok] = 1;
        }
        if (client >= 0 && client < V4_MAX_CLIENTS) {
            o->clientid[client]       = r->clientid;
            o->clientid_known[client] = 1;
        }
    } else if (strcmp(tag, "SExchangeId") == 0) {
        int64_t client = jf_i64(v, "client");
        int     wire_conf, wire_pnfs;

        if (client >= 0 && client < V4_MAX_CLIENTS) {
            if (o->clientid_known[client] &&
                o->clientid[client] != r->clientid) {
                mism_add(m, "exchange_id: model client %" PRId64 " wire "
                         "clientid changed", client);
            }
            o->clientid[client]       = r->clientid;
            o->clientid_known[client] = 1;
        }
        /* eir_sequenceid is meaningful only for a newly registered
         * (unconfirmed) client ID (RFC 8881 18.35.3). */
        if (!jf_bool(v, "confirmedR") &&
            r->sequenceid != (uint32_t) jf_i64(v, "csSeq")) {
            mism_add(m, "exchange_id.sequenceid: expected %" PRId64
                     ", got %u", jf_i64(v, "csSeq"), r->sequenceid);
        }
        wire_conf = (r->flags & EXCHGID4_FLAG_CONFIRMED_R) != 0;
        if (wire_conf != jf_bool(v, "confirmedR")) {
            mism_add(m, "exchange_id CONFIRMED_R: expected %d, got %d",
                     jf_bool(v, "confirmedR"), wire_conf);
        }
        wire_pnfs = (r->flags & EXCHGID4_FLAG_USE_PNFS_MDS) != 0;
        if (wire_pnfs != jf_bool(v, "pnfsMds")) {
            caps_mismatch(o, m, "pnfs",
                          "trace assumes pnfsMds=%d, server advertises %d",
                          jf_bool(v, "pnfsMds"), wire_pnfs);
        }
    } else if (strcmp(tag, "SCreateSession") == 0) {
        int64_t sess = jf_i64(v, "sess");

        if (sess >= 0 && sess < V4_MAX_SESS) {
            memcpy(o->sess[sess], r->sessionid, 16);
            o->sess_known[sess] = 1;
            o->sess_slots[sess] = r->fore_slots;
            /* A brand-new session's slots are all unused, and an unused
             * slot's first request must carry sequence id 1. */
            o->retry_seq[sess] = 0;
            if (req && strcmp(jf_tag(req), "RCreateSession") == 0) {
                o->sess_client[sess] =
                    (int) jf_i64(jf_val(req), "client");
            }
        }
    } else if (strcmp(tag, "SSequence") == 0) {
        /* cache bookkeeping happens in run_compound */
    } else if (strcmp(tag, "SOpen") == 0) {
        int64_t sid  = jf_i64(v, "sid");
        int     need = (r->rflags & OPEN4_RESULT_CONFIRM) != 0;

        learn_sid(o, sid, &r->sid, m, jf_i64(v, "seq"), "open.stateid");
        if (sid >= 0 && sid < V4_MAX_SIDS && o->cur_open_clientid_known) {
            o->sid_clientid[sid]       = o->cur_open_clientid;
            o->sid_clientid_known[sid] = 1;
        }
        if (need != jf_bool(v, "needConfirm")) {
            mism_add(m, "open.rflags CONFIRM: expected %d, got %d",
                     jf_bool(v, "needConfirm"), need);
        }
        check_cinfo(o, ctx->cur, json_object_get(v, "cinfo"),
                    r->cinfo_atomic, r->cinfo_before, r->cinfo_after, m,
                    "open.cinfo");
        check_deleg(o, json_object_get(v, "deleg"), r, m);
    } else if (strcmp(tag, "SOpenConfirm") == 0 ||
               strcmp(tag, "SOpenDowngrade") == 0 ||
               strcmp(tag, "SLocku") == 0) {
        if (r->sid.seqid != (uint32_t) jf_i64(v, "seq")) {
            mism_add(m, "%s.seqid: expected %" PRId64 ", got %u",
                     tag, jf_i64(v, "seq"), r->sid.seqid);
        }
    } else if (strcmp(tag, "SRead") == 0) {
        json_t  *blocks = itf_seq(json_object_get(v, "blocks"));
        json_t  *jb;
        size_t   i;
        uint32_t want_len = (uint32_t) (jf_i64(v, "count") * V4_BLOCK_SIZE);

        if (r->eof != jf_bool(v, "eof")) {
            mism_add(m, "read.eof: expected %d, got %d",
                     jf_bool(v, "eof"), r->eof);
        }
        if (r->data_len != want_len) {
            mism_add(m, "read.count: expected %u, got %u",
                     want_len, r->data_len);
        }
        json_array_foreach(blocks, i, jb)
        {
            block_fill(itf_i64(jb), o->scratch + i * V4_BLOCK_SIZE);
        }
        if (r->data_len == want_len &&
            (uint32_t) json_array_size(blocks) * V4_BLOCK_SIZE ==
            want_len && memcmp(r->data, o->scratch, want_len) != 0) {
            uint32_t off;

            mism_add(m, "read data mismatch");
            for (off = 0; off + V4_BLOCK_SIZE <= want_len;
                 off += V4_BLOCK_SIZE) {
                if (memcmp(r->data + off, o->scratch + off,
                           V4_BLOCK_SIZE) != 0) {
                    mism_add(m, "first differing block %u: expected byte "
                             "%#x, got byte %#x", off / V4_BLOCK_SIZE,
                             o->scratch[off], r->data[off]);
                    break;
                }
            }
        }
    } else if (strcmp(tag, "SReadPlus") == 0) {
        /* The model predicts the DATA/HOLE classification and contents of the
         * whole requested range.  A server may legally return a shorter
         * prefix -- rpr_contents is an array and the client re-issues from
         * the last byte returned -- so the segments are validated as a
         * contiguous, block-aligned prefix starting at the requested offset,
         * and only the blocks actually returned are compared. */
        json_t  *blocks  = itf_seq(json_object_get(v, "blocks"));
        json_t  *isdata  = itf_seq(json_object_get(v, "isData"));
        uint64_t req_off = req ? (uint64_t) jf_i64(jf_val(req), "off") *
            V4_BLOCK_SIZE : 0;
        uint64_t want   = req_off;
        int      nmodel = (int) json_array_size(blocks);
        int      got    = 0;
        int      i;

        if (r->rp_bad) {
            mism_add(m, "read_plus: unusable segment list");
        }
        for (i = 0; i < r->rp_nsegs && !r->rp_bad; i++) {
            uint64_t soff = r->rp_segs[i].offset;
            uint64_t slen = r->rp_segs[i].length;
            uint64_t b;

            if (soff != want) {
                mism_add(m, "read_plus seg %d: offset %llu, expected %llu "
                         "(segments must tile the range from the requested "
                         "offset)", i, (unsigned long long) soff,
                         (unsigned long long) want);
                break;
            }
            if (slen == 0 || (slen % V4_BLOCK_SIZE) != 0) {
                mism_add(m, "read_plus seg %d: length %llu is not a whole "
                         "number of %u-byte blocks", i,
                         (unsigned long long) slen, V4_BLOCK_SIZE);
                break;
            }
            for (b = 0; b < slen / V4_BLOCK_SIZE; b++) {
                int is_data;

                if (got >= nmodel) {
                    mism_add(m, "read_plus: returned %d blocks, model "
                             "predicts only %d", got + 1, nmodel);
                    break;
                }
                is_data = itf_bool(json_array_get(isdata, (size_t) got));
                if (r->rp_segs[i].is_data != is_data) {
                    mism_add(m, "read_plus block %d: expected %s, got %s",
                             got, is_data ? "DATA" : "HOLE",
                             r->rp_segs[i].is_data ? "DATA" : "HOLE");
                } else if (r->rp_segs[i].is_data) {
                    block_fill(itf_i64(json_array_get(blocks, (size_t) got)),
                               o->scratch);
                    if (memcmp(r->rp_segs[i].data + b * V4_BLOCK_SIZE,
                               o->scratch, V4_BLOCK_SIZE) != 0) {
                        mism_add(m, "read_plus block %d: data mismatch "
                                 "(expected byte %#x, got byte %#x)", got,
                                 o->scratch[0],
                                 r->rp_segs[i].data[b * V4_BLOCK_SIZE]);
                    }
                }
                got++;
            }
            want += slen;
        }
        /* A short answer is legal, but it must make progress: returning no
        * segment at all while the model still has blocks to report, without
        * claiming EOF, leaves the client with nowhere to re-issue from. */
        if (!r->rp_bad && got == 0 && nmodel > 0 && !r->eof) {
            mism_add(m, "read_plus: no segment returned for %d predicted "
                     "blocks and eof is unset", nmodel);
        }
        if (!r->rp_bad && got < nmodel && r->eof) {
            mism_add(m, "read_plus: eof set after %d of %d predicted blocks",
                     got, nmodel);
        }
        if (!r->rp_bad && got == nmodel && r->eof != jf_bool(v, "eof")) {
            mism_add(m, "read_plus.eof: expected %d, got %d",
                     jf_bool(v, "eof"), r->eof);
        }
    } else if (strcmp(tag, "SIoAdvise") == 0) {
        /* IO_ADVISE is advisory: the server may honor none of the hints, but
         * it must not invent one the client did not ask for (RFC 7862
         * 15.5). */
        uint32_t asked = req ? (uint32_t) jf_i64(jf_val(req), "hint") : 0;
        uint32_t hi;

        for (hi = 0; hi < r->ia_nhints && hi < V4_IA_MAX_HINTS; hi++) {
            if (r->ia_hints[hi] != 0 && r->ia_hints[hi] != asked) {
                mism_add(m, "io_advise: honored hint %u was not requested "
                         "(asked for %u)", r->ia_hints[hi], asked);
            }
        }
    } else if (strcmp(tag, "SWriteSame") == 0) {
        uint32_t want = (uint32_t) (jf_i64(v, "count") * V4_BLOCK_SIZE);

        if (r->count != want) {
            mism_add(m, "write_same.count: expected %u, got %u", want,
                     r->count);
        }
    } else if (strcmp(tag, "SClone") == 0) {
        /* CLONE reports status only; its data effect is checked by the
         * subsequent READ/READ_PLUS the generator emits against the model's
         * updated filesystem. */
        (void) 0;
    } else if (strcmp(tag, "SWrite") == 0) {
        uint32_t want = (uint32_t) (jf_i64(v, "count") * V4_BLOCK_SIZE);

        if (r->count != want) {
            mism_add(m, "write.count: expected %u, got %u", want,
                     r->count);
        }
        if (!o->have_write_verf) {
            memcpy(o->write_verf, r->verf, 8);
            o->have_write_verf = 1;
        } else if (memcmp(o->write_verf, r->verf, 8) != 0) {
            mism_add(m, "write verifier changed mid-run");
        }
    } else if (strcmp(tag, "SCommit") == 0) {
        if (o->have_write_verf && memcmp(o->write_verf, r->verf, 8) != 0) {
            mism_add(m, "commit verifier differs from write verifier");
        }
    } else if (strcmp(tag, "SLock") == 0) {
        learn_sid(o, jf_i64(v, "sid"), &r->sid, m, jf_i64(v, "seq"),
                  "lock.stateid");
    } else if (strcmp(tag, "SLayoutget") == 0) {
        learn_sid(o, jf_i64(v, "sid"), &r->sid, m, jf_i64(v, "seq"),
                  "layout.stateid");
        if (r->nsegs == 0) {
            mism_add(m, "layoutget: empty segment list");
        } else {
            uint64_t lo = r->segs[0].offset;
            uint64_t hi = 0;
            int      j;

            for (j = 0; j < r->nsegs; j++) {
                uint64_t top = r->segs[j].offset +
                    (r->segs[j].length > V4_TO_EOF - r->segs[j].offset
                     ? V4_TO_EOF - r->segs[j].offset : r->segs[j].length);

                if (r->segs[j].offset < lo) {
                    lo = r->segs[j].offset;
                }
                if (top > hi) {
                    hi = top;
                }
            }
            if (lo > (uint64_t) jf_i64(v, "lo") * V4_BLOCK_SIZE ||
                hi < (uint64_t) jf_i64(v, "hi") * V4_BLOCK_SIZE) {
                mism_add(m, "layoutget: segments do not cover the "
                         "requested range");
            }
            if (r->has_deviceid) {
                memcpy(o->deviceid, r->deviceid, 16);
                o->has_deviceid = 1;
            }
        }
    } else if (strcmp(tag, "SLayoutreturn") == 0) {
        if (r->lr_present != jf_bool(v, "present")) {
            mism_add(m, "layoutreturn.present: expected %d, got %d",
                     jf_bool(v, "present"), r->lr_present);
        } else if (r->lr_present &&
                   r->sid.seqid != (uint32_t) jf_i64(v, "seq")) {
            mism_add(m, "layoutreturn.seqid: expected %" PRId64
                     ", got %u", jf_i64(v, "seq"), r->sid.seqid);
        }
    } else if (strcmp(tag, "SLayoutcommit") == 0) {
        if (r->has_newsize &&
            r->newsize != (uint64_t) jf_i64(v, "newSizeBlocks") *
            V4_BLOCK_SIZE) {
            mism_add(m, "layoutcommit.newsize: expected %" PRId64
                     ", got %" PRIu64,
                     jf_i64(v, "newSizeBlocks") * V4_BLOCK_SIZE,
                     r->newsize);
        }
    } else if (strcmp(tag, "SSeek") == 0) {
        if (r->eof != jf_bool(v, "eof")) {
            mism_add(m, "seek.eof: expected %d, got %d",
                     jf_bool(v, "eof"), r->eof);
        }
        if (r->offset != (uint64_t) jf_i64(v, "offset") * V4_BLOCK_SIZE) {
            mism_add(m, "seek.offset: expected %" PRId64 ", got %" PRIu64,
                     jf_i64(v, "offset") * V4_BLOCK_SIZE, r->offset);
        }
    } else if (strcmp(tag, "SCopy") == 0) {
        uint32_t want = (uint32_t) (jf_i64(v, "copied") * V4_BLOCK_SIZE);

        if (r->count != want) {
            mism_add(m, "copy.count: expected %u, got %u", want, r->count);
        }
    } else if (strcmp(tag, "SGetxattr") == 0) {
        char want[64];

        snprintf(want, sizeof(want), "xattr-value-%" PRId64,
                 jf_i64(v, "sym"));
        if (strlen(want) != r->xvalue_len ||
            memcmp(want, r->xvalue, r->xvalue_len) != 0) {
            mism_add(m, "getxattr: expected '%s', got '%.*s'",
                     want, (int) r->xvalue_len, r->xvalue);
        }
    } else if (strcmp(tag, "SListxattrs") == 0) {
        json_t *names = itf_seq(json_object_get(v, "names"));
        json_t *jn;
        size_t  i;
        int     j;

        json_array_foreach(names, i, jn)
        {
            int found = 0;

            for (j = 0; j < r->nnames; j++) {
                if (strcmp(r->names[j], json_string_value(jn)) == 0) {
                    found = 1;
                    break;
                }
            }
            if (!found) {
                mism_add(m, "listxattrs: expected '%s' missing",
                         json_string_value(jn));
            }
        }
        for (j = 0; j < r->nnames; j++) {
            int found = 0;

            json_array_foreach(names, i, jn)
            {
                if (strcmp(r->names[j], json_string_value(jn)) == 0) {
                    found = 1;
                    break;
                }
            }
            if (!found) {
                mism_add(m, "listxattrs: unexpected '%s'", r->names[j]);
            }
        }
    }
    /* All remaining tags are status-only. */
} /* check_result */

/* ---- compound driver ------------------------------------------------------ */

static double
now_seconds(void)
{
    struct timeval tv;

    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1e6;
} /* now_seconds */

static void
await_recalls(
    struct oracle *o,
    json_t        *sids,
    struct mism   *m)
{
    uint8_t want[16][12];
    int     nwant = 0;
    size_t  i;
    json_t *js;
    double  deadline = now_seconds() + 3.0;
    int     j, k, missing;

    json_array_foreach(itf_seq(sids), i, js)
    {
        int64_t sid = itf_i64(js);

        if (sid >= 0 && sid < V4_MAX_SIDS && o->sid_known[sid] &&
            nwant < 16) {
            memcpy(want[nwant++], o->sid_other[sid], 12);
        }
    }

    for (;;) {
        missing = 0;
        for (j = 0; j < nwant; j++) {
            int seen = 0;

            for (k = 0; k < o->nrecalls; k++) {
                if (memcmp(o->recalls[k], want[j], 12) == 0) {
                    seen = 1;
                    break;
                }
            }
            if (!seen) {
                missing++;
            }
        }
        if (!missing || now_seconds() >= deadline) {
            break;
        }
        evpl_continue(o->env->evpl);
    }
    if (missing) {
        mism_add(m, "CB_RECALL not observed for %d delegation stateid(s)",
                 missing);
    }
} /* await_recalls */

static int
run_compound(
    struct oracle *o,
    int            idx,
    json_t        *lab,
    struct mism   *m)
{
    json_t              *ops        = json_object_get(lab, "ops");
    json_t              *results    = json_object_get(lab, "results");
    uint32_t             exp_status = (uint32_t) jf_i64(lab, "status");
    int                  nops       = (int) json_array_size(ops);
    struct nfs_argop4    argarray[V4_MAX_OPS];
    struct v4_argscratch scratch[V4_MAX_OPS];
    struct v4_reply      rep;
    struct v4_reply      first = { 0 };
    struct v4_call_ctx   cctx  = { .o = o, .rep = &rep };
    struct COMPOUND4args args;
    struct v4_ctx        ctx = { .cur = -1, .saved = -1, .abort = 0 };
    char                 tagbuf[32];
    char                 tagexp[300];
    json_t              *op;
    json_t              *seq_req = NULL;
    json_t              *seq_exp = NULL;
    size_t               i;
    int                  attempt;
    int                  n;
    uint32_t             retry_slot = 0;
    int                  retry_sess = 0;
    int                  has_seq    = 0;

    o->status_dev              = 0;
    o->cur_open_clientid_known = 0;
    o->arena_used              = 0;

    if (nops > V4_MAX_OPS) {
        mism_add(m, "compound has %d ops (harness cap %d)", nops,
                 V4_MAX_OPS);
        return -1;
    }

    json_array_foreach(ops, i, op)
    {
        if (strcmp(jf_tag(op), "ROpen") == 0) {
            int64_t client = jf_i64(jf_val(op), "client");

            if (client >= 0 && client < V4_MAX_CLIENTS &&
                o->clientid_known[client]) {
                o->cur_open_clientid       = o->clientid[client];
                o->cur_open_clientid_known = 1;
            }
        }
    }

    /* Encode every op BEFORE sending (an unknown identity is an earlier
     * divergence anchored to this step). */
    json_array_foreach(ops, i, op)
    {
        if (encode_op(o, op, &argarray[i], &scratch[i], m) < 0) {
            return -1;
        }
    }

    memset(&args, 0, sizeof(args));
    {
        /* The model may override the wire tag with a sentinel (see runTagged
         * in nfs4.qnt): chimera validates the COMPOUND tag as a utf8str_cs
         * before running any operation, so a malformed one fails the whole
         * compound with an empty result array.  "" means use the default
         * identity tag. */
        const char *tn = jf_str(lab, "tagName");

        if (tn && tn[0]) {
            const char *tb;
            uint32_t    tl;

            v4_expand_name(tn, tagexp, &tb, &tl);
            args.tag.data = (void *) tb;
            args.tag.len  = tl;
        } else {
            snprintf(tagbuf, sizeof(tagbuf), "t%" PRId64, jf_i64(lab, "tag"));
            args.tag.data = tagbuf;
            args.tag.len  = (uint32_t) strlen(tagbuf);
        }
    }
    args.minorversion = (uint32_t) o->minor;
    args.argarray     = argarray;
    args.num_argarray = (uint32_t) nops;

    struct evpl_rpc2_conn *conn =
        conn_for(o, compound_client(o, ops, results));

    /* A 4.1+ compound leads with SEQUENCE.  Pick the session's highest
     * granted slot for DELAY retries: the model only ever drives slot 0, so
     * anything above it is ours to use.  Slot 0 means "no safe retry slot"
     * (a 4.0 compound, or a session that granted a single slot). */
    if (nops > 0 && strcmp(jf_tag(json_array_get(ops, 0)), "RSequence") == 0) {
        int64_t rsess = jf_i64(jf_val(json_array_get(ops, 0)), "sess");

        has_seq = 1;
        if (rsess >= 0 && rsess < V4_MAX_SESS && o->sess_known[rsess] &&
            o->sess_slots[rsess] >= 2) {
            retry_sess = (int) rsess;
            retry_slot = o->sess_slots[rsess] - 1;
        }
    }

    /* Send; retry when the server reports transient DELAY the model did
     * not predict.
     *
     * A 4.1+ retry may not reuse the model's (slot, sequence id).  That pair
     * names a slot in the session reply cache, and RFC 8881 2.10.6.2 reserves
     * reusing it for retransmitting a request whose reply was lost -- so the
     * server answers a verbatim resend from the cache, which is exactly what
     * it is required to do.  Resending the same compound therefore replays
     * the cached DELAY forever instead of re-running the operation: the retry
     * loop could never make progress, and every DELAY became a divergence
     * after the retry budget ran out.
     *
     * Retry on a slot the model never uses, with a sequence id of our own, so
     * the retry actually executes and the model's slot keeps the sequence the
     * trace assigned it.  The model's slot still holds the first attempt's
     * reply on the server, so the reply-cache bookkeeping below records that
     * one rather than the retry's. */
    for (attempt = 0; attempt < V4_RETRY_DELAY_MAX; attempt++) {
        int unexpected_delay = 0;

        if (attempt > 0 && has_seq && retry_slot > 0) {
            argarray[0].opsequence.sa_slotid     = retry_slot;
            argarray[0].opsequence.sa_sequenceid = ++o->retry_seq[retry_sess];
            argarray[0].opsequence.sa_cachethis  = 0;
        }

        memset(&rep, 0, sizeof(rep));
        o->arena_used = 0;
        o->env->nfs_v4.send_call_NFSPROC4_COMPOUND(&o->env->nfs_v4.rpc2,
                                                   o->env->evpl, conn,
                                                   &o->env->cred, &args,
                                                   0, 0, NULL, 0, 0,
                                                   v4_compound_cb, &cctx);
        while (!rep.done) {
            evpl_continue(o->env->evpl);
        }
        if (rep.rpc_err != 0) {
            fprintf(stderr, "rpc2 transport error %d\n", rep.rpc_err);
            exit(3);
        }
        for (n = 0; n < rep.nres; n++) {
            uint32_t est = n < (int) json_array_size(results)
                ? (uint32_t) jf_i64(jf_val(json_array_get(results, n)),
                                    "st")
                : NFS4_OK;

            if (rep.res[n].status == E_DELAY && est != E_DELAY) {
                unexpected_delay = 1;
            }
        }
        if (attempt == 0) {
            first = rep;
        }
        if (!unexpected_delay) {
            break;
        }
        if (has_seq && retry_slot == 0) {
            /* A 4.1+ compound with nowhere safe to retry: resending it
             * verbatim would only replay the cached reply.  A 4.0 compound
             * has no reply cache, so resending it re-runs the work. */
            break;
        }
        usleep(100000);
    }
    if (attempt == V4_RETRY_DELAY_MAX) {
        mism_add(m, "server kept returning NFS4ERR_DELAY after %d retries",
                 V4_RETRY_DELAY_MAX);
    }

    /* 4.1 SEQUENCE replay contract: compare against our own summary of
     * the original reply (see the header comment for how this differs
     * from a raw-byte comparison). */
    if (nops > 0 && strcmp(jf_tag(json_array_get(ops, 0)),
                           "RSequence") == 0) {
        seq_req = jf_val(json_array_get(ops, 0));
    }
    if (json_array_size(results) > 0 &&
        strcmp(jf_tag(json_array_get(results, 0)), "SSequence") == 0) {
        seq_exp = jf_val(json_array_get(results, 0));
    }
    if (seq_req && seq_exp && jf_bool(seq_exp, "replay") &&
        (uint32_t) jf_i64(seq_exp, "st") == NFS4_OK) {
        int64_t sess = jf_i64(seq_req, "sess");
        int64_t slot = jf_i64(seq_req, "slot");

        if (sess < 0 || sess >= V4_MAX_SESS || slot < 0 ||
            slot >= V4_MAX_SLOTS || !o->cache[sess][slot].valid) {
            mism_add(m, "replay step but no cached original reply");
        } else {
            struct v4_cached *c = &o->cache[sess][slot];

            if (c->status != rep.status || c->nres != rep.nres) {
                mism_add(m, "SEQUENCE replay: reply differs from the "
                         "original (reply cache violation): "
                         "original status %u nres %d, replay status %u nres %d",
                         c->status, c->nres, rep.status, rep.nres);
            } else {
                for (n = 0; n < rep.nres; n++) {
                    if (c->op[n].status != rep.res[n].status ||
                        c->op[n].fnv != rep.res[n].fnv) {
                        mism_add(m, "SEQUENCE replay: op %d differs from "
                                 "the original (reply cache violation)",
                                 n);
                        break;
                    }
                }
            }
        }
        return m->n ? -1 : 0;
    }

    n = rep.nres < (int) json_array_size(results)
        ? rep.nres : (int) json_array_size(results);
    for (i = 0; i < (size_t) n; i++) {
        const char *eop = i < (size_t) nops
            ? jf_tag(json_array_get(ops, i)) : "?";

        if (strcmp(eop, "RPutfh") == 0) {
            ctx.cur = itf_i64(jf_val(json_array_get(ops, i)));
        } else if (strcmp(eop, "RPutrootfh") == 0) {
            ctx.cur = 0;
        } else if (strcmp(eop, "RSecinfo") == 0) {
            /* SECINFO consumes the current filehandle (RFC 7530 16.31.3), so
             * drop the tracked cfh exactly as the model does -- any following
             * op is expected to answer NFS4ERR_NOFILEHANDLE. */
            ctx.cur = -1;
        } else if (strcmp(eop, "RSavefh") == 0) {
            ctx.saved = ctx.cur;
        } else if (strcmp(eop, "RRestorefh") == 0) {
            ctx.cur = ctx.saved;
        }
        check_result(o, json_array_get(results, i),
                     i < (size_t) nops ? json_array_get(ops, i) : NULL,
                     &rep.res[i], &ctx, m);
        /* A capability mismatch (o->skip) abandons the trace, but keep
         * consuming this compound's remaining results so their filehandles and
         * stateids are still learned -- the teardown sweep needs the GETFH of
         * an OPEN that skipped on a missing delegation to close that (validly
         * opened) file.  run_trace honours o->skip before any mismatch, so the
         * comparisons below are ignored.  A hard abort still stops. */
        if (ctx.abort) {
            break;
        }
    }

    if (!ctx.abort && !o->skip &&
        rep.nres != (int) json_array_size(results)) {
        mism_add(m, "result count: model expected %d results, server "
                 "returned %d", (int) json_array_size(results),
                 rep.nres);
    }
    if (!ctx.abort && !o->skip && rep.status != exp_status &&
        rep.status != o->status_dev) {
        mism_add(m, "compound status: expected %u, got %u",
                 exp_status, rep.status);
    }

    /* Record the reply summary for future SEQUENCE replays of this
     * slot. */
    if (seq_req && first.nres > 0 && first.res[0].status == NFS4_OK) {
        int64_t sess = jf_i64(seq_req, "sess");
        int64_t slot = jf_i64(seq_req, "slot");

        if (sess >= 0 && sess < V4_MAX_SESS && slot >= 0 &&
            slot < V4_MAX_SLOTS) {
            struct v4_cached *c = &o->cache[sess][slot];

            /* The first attempt is what the server cached against the
             * model's slot: a retry runs on a different one. */
            c->valid  = 1;
            c->status = first.status;
            c->nres   = first.nres;
            for (n = 0; n < first.nres; n++) {
                c->op[n].status = first.res[n].status;
                c->op[n].fnv    = first.res[n].fnv;
            }
        }
    }

    /* Recall observations promised by this compound. */
    if (!o->skip) {
        json_t *res;

        json_array_foreach(results, i, res)
        {
            if (strcmp(jf_tag(res), "SOpen") == 0) {
                json_t *recalls = json_object_get(jf_val(res), "recalls");

                if (recalls && json_array_size(itf_seq(recalls)) > 0) {
                    await_recalls(o, recalls, m);
                }
            }
        }
    }

    /* History entry (also drives --verbose). */
    {
        struct v4_hist *h;
        size_t          used  = 0;
        size_t          sused = 0;

        if (o->nhist == V4_HISTORY) {
            memmove(&o->history[0], &o->history[1],
                    sizeof(o->history[0]) * (V4_HISTORY - 1));
            o->nhist--;
        }
        h           = &o->history[o->nhist++];
        h->idx      = idx;
        h->opl[0]   = '\0';
        h->stats[0] = '\0';
        json_array_foreach(ops, i, op)
        {
            used += snprintf(h->opl + used, sizeof(h->opl) - used, "%s%s",
                             i ? "," : "", jf_tag(op) + 1);
            if (used >= sizeof(h->opl)) {
                break;
            }
        }
        for (n = 0; n < rep.nres && sused < sizeof(h->stats); n++) {
            sused += snprintf(h->stats + sused, sizeof(h->stats) - sused,
                              "%s%u", n ? "," : "", rep.res[n].status);
        }
        h->status = rep.status;
        if (o->verbose) {
            printf("  [%4d] %s -> %u [%s]\n", idx, h->opl, h->status,
                   h->stats);
        }
    }

    return m->n ? -1 : 0;
} /* run_compound */

/* ---- trace driver --------------------------------------------------------- */

static void
report_divergence(
    const char    *trace_path,
    struct oracle *o,
    int            step,
    json_t        *lab,
    struct mism   *m)
{
    json_t *ops     = json_object_get(lab, "ops");
    json_t *results = json_object_get(lab, "results");
    json_t *e;
    size_t  i;
    int     j;
    char   *dump;

    fprintf(stderr, "\n=== DIVERGENCE in %s ===\n", trace_path);
    fprintf(stderr, "step %d:\n", step);
    json_array_foreach(ops, i, e)
    {
        dump = json_dumps(jf_val(e) ?: e, JSON_COMPACT | JSON_ENCODE_ANY);
        fprintf(stderr, "    op[%zu] %s: %s\n", i, jf_tag(e),
                dump ?: "<?>");
        free(dump);
    }
    fprintf(stderr, "  expected status %" PRId64 "; expected results:\n",
            jf_i64(lab, "status"));
    json_array_foreach(results, i, e)
    {
        dump = json_dumps(jf_val(e) ?: e, JSON_COMPACT | JSON_ENCODE_ANY);
        fprintf(stderr, "    res[%zu] %s: %s\n", i, jf_tag(e),
                dump ?: "<?>");
        free(dump);
    }
    for (j = 0; j < m->n; j++) {
        fprintf(stderr, "  MISMATCH: %s\n", m->msg[j]);
    }
    fprintf(stderr, "\nlast compounds before failure:\n");
    for (j = 0; j < o->nhist; j++) {
        fprintf(stderr, "  [%4d] %s -> %u [%s]\n", o->history[j].idx,
                o->history[j].opl, o->history[j].status,
                o->history[j].stats);
    }
} /* report_divergence */

/* ---- teardown: replay the closes a trace left implicit ------------------- */

/* Send one already-encoded compound and wait for the reply; the teardown is
 * best-effort (the fs is about to be torn down), so the result is ignored. */
static void
v4_send_raw(
    struct oracle         *o,
    struct evpl_rpc2_conn *conn,
    struct nfs_argop4     *argarray,
    int                    nops)
{
    struct v4_reply      rep  = { 0 };
    struct v4_call_ctx   cctx = { .o = o, .rep = &rep };
    struct COMPOUND4args args = { 0 };

    args.tag.data     = "teardown";
    args.tag.len      = 8;
    args.minorversion = (uint32_t) o->minor;
    args.argarray     = argarray;
    args.num_argarray = (uint32_t) nops;

    o->arena_used = 0;
    o->env->nfs_v4.send_call_NFSPROC4_COMPOUND(&o->env->nfs_v4.rpc2,
                                               o->env->evpl, conn,
                                               &o->env->cred, &args,
                                               0, 0, NULL, 0, 0,
                                               v4_compound_cb, &cctx);
    while (!rep.done) {
        evpl_continue(o->env->evpl);
    }
} /* v4_send_raw */

/* Locate the sdb map field of a state (its key may be module-qualified). */
static json_t *
v4_sdb_field(
    json_t     *sdb,
    const char *name)
{
    return json_object_get(json_object_get(sdb, name), "#map");
} /* v4_sdb_field */

/* A trace can stop with opens still live.  Upstream's rmfs -- and the umount
 * that precedes it -- refuse a filesystem whose VFS handles are still
 * referenced, and the server is RFC 8881 §18.50.3 strict: DESTROY_CLIENTID
 * returns NFS4ERR_CLIENTID_BUSY while a clientid holds any leased state.  So,
 * exactly as a real client does before it tears its mount down, replay the
 * closes the trace left implicit: for every open still live in the final model
 * state, PUTFH its file and CLOSE it.  4.1+ wraps each batch in a SEQUENCE on
 * an otherwise-unused slot (seq 1), which needs no slot-sequence bookkeeping;
 * 4.0 carries the open-owner's next seqid.  Once the opens are gone the
 * filesystem is unreferenced and the recycle (and, for a real client,
 * DESTROY_CLIENTID) succeeds.  Locks ride on their open and go with it; the
 * memfs corpus grants no delegations. */
static void
v4_close_dangling_opens(
    struct oracle *o,
    json_t        *final_state)
{
    json_t     *sdb = NULL;
    json_t     *opens;
    json_t     *oo40;
    const char *k;
    json_t     *v;
    int         client;
    uint32_t    oseq_next[V4_MAX_SIDS];   /* 4.0 open-owner -> next seqid */
    int         oi;

    json_object_foreach(final_state, k, v)
    {
        const char *base = strrchr(k, ':');

        if (strcmp(base ? base + 1 : k, "sdb") == 0) {
            sdb = v;
            break;
        }
    }
    if (!sdb) {
        return;
    }
    opens = v4_sdb_field(sdb, "opens");
    if (!opens || json_array_size(opens) == 0) {
        return;
    }
    json_t *sessions = v4_sdb_field(sdb, "sessions");

    oo40 = v4_sdb_field(sdb, "oo40");   /* 4.0 open-owner seqids; seeded per client */
    (void) oi;

    for (client = 0; client < V4_MAX_CLIENTS; client++) {
        struct nfs_argop4 arg[V4_MAX_OPS];
        int               n    = 0;
        int               sess = -1;
        size_t            i;
        json_t           *pair;

        if (!o->clientid_known[client]) {
            continue;
        }
        if (o->minor >= 1) {
            /* Use the client's *live* session from the final state -- the trace
             * may have churned sessions, and the oracle keeps destroyed ones
             * mapped (a stale one would make SEQUENCE fail NFS4ERR_BADSESSION). */
            size_t  si;
            json_t *spair;

            sess = -1;
            json_array_foreach(sessions, si, spair)
            {
                if (jf_i64(json_array_get(spair, 1), "client") == client) {
                    sess = (int) itf_i64(json_array_get(spair, 0));
                    break;
                }
            }
            if (sess < 0 || sess >= V4_MAX_SESS || !o->sess_known[sess]) {
                continue;   /* no live session to carry the compound */
            }
        } else {
            /* Seed this client's open-owner seqids: oo40's key is the
             * (client, owner) tuple and its record carries the last seqid. */
            size_t  oi2;
            json_t *op40;

            for (oi = 0; oi < V4_MAX_SIDS; oi++) {
                oseq_next[oi] = 1;
            }
            json_array_foreach(oo40, oi2, op40)
            {
                json_t *tup = json_object_get(json_array_get(op40, 0), "#tup");
                int64_t oc, ow;

                if (!tup) {
                    continue;
                }
                oc = itf_i64(json_array_get(tup, 0));
                ow = itf_i64(json_array_get(tup, 1));
                if (oc == client && ow >= 0 && ow < V4_MAX_SIDS) {
                    oseq_next[ow] = (uint32_t)
                        (jf_i64(json_array_get(op40, 1), "seqid") + 1);
                }
            }
        }

        json_array_foreach(opens, i, pair)
        {
            json_t              *rec = json_array_get(pair, 1);
            int64_t              sid = itf_i64(json_array_get(pair, 0));
            int64_t              file;
            int64_t              seq;
            int64_t              owner;
            const struct mbt_fh *fh;
            uint8_t              other[12];
            struct mism          m = { 0 };

            if (jf_i64(rec, "client") != client) {
                continue;
            }
            file  = jf_i64(rec, "file");
            seq   = jf_i64(rec, "seq");
            owner = jf_i64(rec, "owner");
            fh    = real_fh(o, file, &m);
            if (!fh || sid_of(o, sid, other, &m) < 0) {
                continue;
            }

            /* Flush and restart the batch (with a fresh SEQUENCE) if a
             * PUTFH+CLOSE pair would overflow the compound op cap. */
            if (n + 2 > V4_MAX_OPS) {
                v4_send_raw(o, conn_for(o, client), arg, n);
                n = 0;
            }
            if (o->minor >= 1 && n == 0) {
                arg[n].argop = OP_SEQUENCE;
                memcpy(arg[n].opsequence.sa_sessionid, o->sess[sess], 16);
                arg[n].opsequence.sa_sequenceid     = 1;
                arg[n].opsequence.sa_slotid         = 1;
                arg[n].opsequence.sa_highest_slotid = 1;
                arg[n].opsequence.sa_cachethis      = 0;
                n++;
            }

            arg[n].argop               = OP_PUTFH;
            arg[n].opputfh.object.data = (void *) fh->data;
            arg[n].opputfh.object.len  = fh->len;
            n++;

            arg[n].argop         = OP_CLOSE;
            arg[n].opclose.seqid = (o->minor >= 1 || owner < 0 ||
                                    owner >= V4_MAX_SIDS)
                ? 0 : oseq_next[owner]++;
            set_stateid(&arg[n].opclose.open_stateid, (uint32_t) seq, other);
            n++;
        }

        if (n > (o->minor >= 1 ? 1 : 0)) {
            v4_send_raw(o, conn_for(o, client), arg, n);
        }
    }
} /* v4_close_dangling_opens */

static int
run_trace(
    struct mbt_env *env,
    const char     *fsname,
    const char     *trace_path,
    const char    **mandatory,
    int             nmandatory,
    int             verbose,
    int             dry_run)
{
    json_error_t   jerr;

    json_t        *root;
    json_t        *states;
    json_t        *state;
    json_t        *lastop;
    json_t        *init;
    struct oracle *o;
    size_t         nstates;
    size_t         idx;
    size_t         sweep_idx = 0;   /* state to close opens from (stop point) */
    int            minor;
    int            failed = 0;
    int            c;
    struct mism    m;
    const char    *k;
    json_t        *jv;

    root = json_load_file(trace_path, 0, &jerr);
    if (!root) {
        fprintf(stderr, "%s: JSON parse error: %s (line %d)\n",
                trace_path, jerr.text, jerr.line);
        return 1;
    }
    states = json_object_get(root, "states");
    if (!states || !json_is_array(states)) {
        fprintf(stderr, "%s: not an ITF trace\n", trace_path);
        json_decref(root);
        return 1;
    }
    nstates = json_array_size(states);

    /* Each state's lastOp key may be module-qualified. */
    lastop = NULL;
    state  = json_array_get(states, 0);
    json_object_foreach(state, k, jv)
    {
        const char *base = strrchr(k, ':');

        if (strcmp(base ? base + 1 : k, "lastOp") == 0) {
            lastop = jv;
        }
    }
    if (!lastop || strcmp(jf_tag(lastop), "LInit") != 0) {
        fprintf(stderr, "%s: first state is not LInit\n", trace_path);
        json_decref(root);
        return 1;
    }
    init  = jf_val(lastop);
    minor = (int) jf_i64(init, "minor");

    /* Delegations are a server-init setting, so they are fixed once for the
     * whole batch in main() (off for the memfs corpus; the Deleg instances
     * skip on the capability mismatch), not toggled per trace here. */

    if (dry_run) {
        printf("%s: %zu compounds, minor %d, format OK\n",
               trace_path, nstates - 1, minor);
        json_decref(root);
        return 0;
    }

    mbt_watchdog_arm(150);

    o = calloc(1, sizeof(*o));

    mbt_env_fs_setup(env, fsname);

    /* The backchannel recorder (registered once in main) dispatches to the
     * oracle for the trace currently replaying.  Bump the owner epoch so this
     * trace's clients register fresh against the long-lived server. */
    g_recall_oracle = o;
    g_owner_epoch++;

    o->env        = env;
    o->minor      = minor;
    o->verbose    = verbose;
    o->nmandatory = nmandatory;
    for (c = 0; c < nmandatory && c < 8; c++) {
        o->mandatory[c] = mandatory[c];
    }
    o->arena   = malloc(V4_DATA_ARENA);
    o->scratch = malloc(V4_DATA_ARENA);

    /* Resolve the export root with a minorversion-0 compound (4.1
     * forbids non-session compounds beyond a tiny op set). */
    {
        struct nfs_argop4    argarray[3];
        struct COMPOUND4args args;
        struct v4_reply      rep;
        struct v4_call_ctx   cctx = { .o = o, .rep = &rep };

        memset(&rep, 0, sizeof(rep));
        memset(&args, 0, sizeof(args));
        memset(argarray, 0, sizeof(argarray));
        argarray[0].argop = OP_PUTROOTFH;
        argarray[1].argop = OP_LOOKUP;
        /* The export is named after this trace's filesystem (see
         * mbt_env_fs_setup), so a mount left stuck by an earlier diverging
         * trace cannot be picked up here by mistake. */
        argarray[1].oplookup.objname.data = (void *) fsname;
        argarray[1].oplookup.objname.len  = (uint32_t) strlen(fsname);
        argarray[2].argop                 = OP_GETFH;
        args.minorversion                 = 0;
        args.argarray                     = argarray;
        args.num_argarray                 = 3;

        o->env->nfs_v4.send_call_NFSPROC4_COMPOUND(&o->env->nfs_v4.rpc2,
                                                   env->evpl,
                                                   env->nfs_conn,
                                                   &env->cred, &args,
                                                   0, 0, NULL, 0, 0,
                                                   v4_compound_cb, &cctx);
        while (!rep.done) {
            evpl_continue(env->evpl);
        }
        if (rep.rpc_err != 0 || rep.status != NFS4_OK || rep.nres < 3 ||
            !rep.res[2].fh.has) {
            fprintf(stderr, "%s: cannot resolve export root: %u\n",
                    trace_path, rep.status);
            failed = 1;
            goto out;
        }
        o->fh[0] = rep.res[2].fh;
    }

    sweep_idx = nstates - 1;
    for (idx = 1; idx < nstates; idx++) {
        json_t *lab;

        state  = json_array_get(states, idx);
        lastop = NULL;
        json_object_foreach(state, k, jv)
        {
            const char *base = strrchr(k, ':');

            if (strcmp(base ? base + 1 : k, "lastOp") == 0) {
                lastop = jv;
            }
        }
        if (!lastop || strcmp(jf_tag(lastop), "LCompound") != 0) {
            fprintf(stderr, "%s: step %zu: unexpected state shape\n",
                    trace_path, idx);
            failed = 1;
            goto out;
        }
        lab = jf_val(lastop);

        /* The v4 step label is the whole compound object, not a tag, so
         * the trace and step index are what name the position. */
        mbt_watchdog_at(trace_path, (int) idx, "compound");
        memset(&m, 0, sizeof(m));
        run_compound(o, (int) idx, lab, &m);
        if (o->skip) {
            printf("%s: SKIP (not applicable): capability mismatch "
                   "[%s]: %s\n", trace_path, o->skip_feature,
                   o->skip_detail);
            failed    = 77;
            sweep_idx = idx;   /* replay stopped here; close from this state */
            goto out;
        }
        if (m.n) {
            report_divergence(trace_path, o, (int) idx, lab, &m);
            failed    = 1;
            sweep_idx = idx;
            goto out;
        }
    }

    {
        char   devs[256] = "";
        size_t used      = 0;
        int    first     = 1;

        for (c = 0; c < DEV_COUNT; c++) {
            if (o->dev_hits[c]) {
                used += snprintf(devs + used, sizeof(devs) - used,
                                 "%s%s x%d", first ? "; known deviations: "
                                 : ", ", v4_dev_ids[c], o->dev_hits[c]);
                first = 0;
                if (used >= sizeof(devs)) {
                    break;
                }
            }
        }
        printf("%s: %zu compounds replayed, minor %d%s\n",
               trace_path, nstates - 1, minor, devs);
    }

 out:
    /* Replay the closes the trace left implicit (a bounded random walk can stop
     * mid-open) so the filesystem is unreferenced when it is recycled; do it
     * while the client connections are still up. */
    if (states && nstates > 0) {
        v4_close_dangling_opens(o, json_array_get(states, sweep_idx));
    }
    for (c = 0; c < V4_MAX_CLIENTS; c++) {
        if (o->conns[c]) {
            evpl_rpc2_client_disconnect(env->rpc2_thread, o->conns[c]);
        }
    }
    mbt_env_fs_teardown(env, fsname);
    free(o->arena);
    free(o->scratch);
    free(o);
    json_decref(root);
    mbt_watchdog_disarm();
    return failed;
} /* run_trace */

int
main(
    int    argc,
    char **argv)
{
    /* *INDENT-OFF* */
    /* uncrustify's column alignment does not converge on this initializer --
     * each pass widens the trailing brace column of the entry it just moved --
     * so the table is aligned by hand and left alone. */
    static struct option long_options[] = {
        { "trace",          required_argument, 0, 't' },
        { "trace-dir",      required_argument, 0, 'D' },
        { "exclude-prefix", required_argument, 0, 'X' },
        { "mandatory",      required_argument, 0, 'M' },
        { "backend",        required_argument, 0, 'b' },
        { "pnfs",           required_argument, 0, 'p' },
        { "dry-run",        no_argument,       0, 'n' },
        { "verbose",        no_argument,       0, 'v' },
        { 0,                0,                 0, 0   },
    };
    /* *INDENT-ON* */
    char              **traces;
    const char         *mandatory[8];
    int                 ntraces    = 0;
    int                 nmandatory = 0;
    int                 dry_run    = 0;
    int                 verbose    = 0;
    int                 rc;
    int                 failures = 0;
    int                 skips    = 0;
    int                 c;
    int                 i;
    struct mbt_env      env;
    const char         *backend = "memfs";
    struct mbt_env_opts opts    = {
        .disable_caches = 1,
        /* Pin memfs's block size to the model's block granularity so sub-block
         * holes line up with SEEK_HOLE/SEEK_DATA (DEVIATIONS-NFS4.md round 8).
         * Delegations stay off: the memfs corpus's Deleg instances skip on the
         * capability mismatch, and every runnable trace is delegation-free. */
        .memfs_config   = "{\"block_size\": 8192}",
    };

    /* --trace/--trace-dir/--exclude-prefix are gathered from raw argv by the
     * shared helper; getopt only recognizes them so it does not error. */
    traces = mbt_collect_traces(argc, argv, &ntraces);

    while ((c = getopt_long(argc, argv, "t:D:X:M:b:p:nv", long_options,
                            NULL)) != -1) {
        switch (c) {
            case 't':
            case 'D':
            case 'X':
                break;   /* handled by mbt_collect_traces */
            case 'M':
                if (nmandatory < 8) {
                    mandatory[nmandatory++] = optarg;
                }
                break;
            case 'b':
                backend = optarg;
                break;
            case 'n':
                dry_run = 1;
                break;
            case 'v':
                verbose = 1;
                break;
            case 'p':
                /* Run the server as a pNFS metadata server with this many
                 * in-process data servers.  0 (the default) leaves pNFS off,
                 * which is what the plain corpus expects. */
                opts.pnfs_num_ds = atoi(optarg);
                if (opts.pnfs_num_ds < 0 || opts.pnfs_num_ds > MBT_MAX_DS) {
                    fprintf(stderr, "%s: --pnfs takes 0..%d data servers\n",
                            argv[0], MBT_MAX_DS);
                    mbt_free_traces(traces, ntraces);
                    return 2;
                }
                break;
            default:
                fprintf(stderr,
                        "usage: %s [--trace FILE ...] [--trace-dir DIR] "
                        "[--backend memfs|diskfs|cairn] [--pnfs N] "
                        "[--mandatory CAP] [--dry-run] [--verbose]\n",
                        argv[0]);
                mbt_free_traces(traces, ntraces);
                return 2;
        } /* switch */
    }

    if (ntraces == 0) {
        fprintf(stderr, "%s: at least one --trace or --trace-dir is required\n",
                argv[0]);
        mbt_free_traces(traces, ntraces);
        return 2;
    }

    /* Open the server + client once and amortize that (dominant) cost across
     * the corpus; each trace gets a fresh, uniquely-named memfs and a fresh
     * client identity (g_owner_epoch).  The backchannel recorder is registered
     * once here (it dispatches to the trace-local oracle via g_recall_oracle). */
    /* memfs_config only reaches the memfs module; diskfs and cairn
     * self-provision their scratch under the env's session dir. */
    opts.module = backend;

    if (!dry_run) {
        mbt_env_open_opts(&env, &opts);
        env.nfs_v4_cb.recv_call_CB_COMPOUND = v4_cb_compound;
        env.nfs_v4_cb.recv_call_CB_NULL     = v4_cb_null;
    }

    for (i = 0; i < ntraces; i++) {
        char fsname[32];

        snprintf(fsname, sizeof(fsname), "fs_%d", i);
        rc = run_trace(dry_run ? NULL : &env, fsname, traces[i], mandatory,
                       nmandatory, verbose, dry_run);
        if (rc == 77) {
            skips++;
        } else if (rc) {
            failures++;
        }
    }

    if (!dry_run) {
        mbt_env_stop(&env);
    }

    mbt_free_traces(traces, ntraces);

    if (failures) {
        return 1;
    }
    if (skips == ntraces) {
        return 77;
    }
    return 0;
} /* main */
