// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Replay a Quint-generated ITF trace against an IN-PROCESS chimera NFS3
 * server over the libevpl inproc transport.
 *
 * Each state of the trace carries a `lastOp` record naming the RPC the
 * model issued and the reply the server must produce (see nfs3.qnt).  This
 * harness embeds a chimera server backed by memfs, obtains the export root
 * file handle via MOUNTv3, then replays every step through the rpc2 client
 * shim in nfs3_mbt_common.h, comparing the server's actual reply against
 * the model's expectation.  Any mismatch is reported as a divergence with
 * full context and fails the run.
 *
 * Model-to-wire mapping maintained here:
 *   - model Fid     -> real nfs_fh3, learned from LOOKUP/CREATE/MKDIR
 *                      replies (byte-compared once known)
 *   - model block i -> block_size bytes at offset i * block_size; block
 *                      symbol 0 is a hole (zero bytes), symbol s > 0 is
 *                      block_size repetitions of byte 0x40 + s
 *   - fileid        -> not predicted; checked for consistency (a fid must
 *                      always report the same fileid; live fids distinct)
 *
 * This began as a line-faithful port of the retired python harness
 * (replay.py, in git history), whose independent hand-rolled XDR
 * validated the wire encoding this harness and the server both derive
 * from the same generator.
 */

#include <getopt.h>
#include <jansson.h>

#include "nfs3_mbt_common.h"
#include "common/mbt_trace_dir.h"
#include "common/mbt_watchdog.h"

#define MBT_MAX_FIDS           16384
#define MBT_MAX_MISM           16
#define MBT_MISM_LEN           512
#define MBT_HISTORY            10

/* Expected constant replies, established empirically against the daemon
 * (probe of 2026-08-08) and pinned here as regression checks. */
#define MBT_FSINFO_RTMAX       1048576
#define MBT_FSINFO_RTPREF      1048576
#define MBT_FSINFO_RTMULT      4096
#define MBT_FSINFO_WTMAX       1048576
#define MBT_FSINFO_WTPREF      1048576
#define MBT_FSINFO_WTMULT      4096
#define MBT_FSINFO_DTPREF      65536
#define MBT_FSINFO_MAXFILESIZE 0xffffffffffffffffULL
#define MBT_FSINFO_PROPERTIES  0x1b
#define MBT_PATHCONF_LINKMAX   0xffffffffU
#define MBT_PATHCONF_NAME_MAX  255

struct mism {
    int  n;
    char msg[MBT_MAX_MISM][MBT_MISM_LEN];
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

    if (m->n >= MBT_MAX_MISM) {
        return;
    }
    va_start(ap, fmt);
    vsnprintf(m->msg[m->n], MBT_MISM_LEN, fmt, ap);
    va_end(ap);
    m->n++;
} /* mism_add */

struct hist_ent {
    int      idx;
    char     tag[32];
    uint32_t status;
    char    *op_dump;   /* json_dumps of the op record, owned */
};

struct oracle {
    struct mbt_env *env;
    int             block_size;
    int             verbose;

    struct mbt_fh   root_fh;
    struct mbt_fh  *fh;             /* [MBT_MAX_FIDS], .has = learned */
    uint64_t       *fileid;
    uint8_t        *fileid_known;
    int64_t         max_fid_seen;

    int             attr_checks;
    int             attr_skips;
    int             have_write_verf;
    uint8_t         write_verf[NFS3_WRITEVERFSIZE];

    struct hist_ent history[MBT_HISTORY];
    int             nhist;

    uint8_t        *scratch;        /* WRITE pattern / READ expectation */
};

/* ---- ITF decoding helpers (jansson) -------------------------------------- */

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
    fprintf(stderr, "trace format error: expected integer, got %s\n",
            json_dumps(v, JSON_ENCODE_ANY) ?: "<null>");
    exit(2);
} /* itf_i64 */

/* A Quint set arrives as {"#set": [...]}; lists arrive as plain arrays. */
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
op_i64(
    json_t     *op,
    const char *key)
{
    json_t *v = json_object_get(op, key);

    if (!v) {
        fprintf(stderr, "trace format error: op missing field '%s'\n", key);
        exit(2);
    }
    return itf_i64(v);
} /* op_i64 */

static const char *
op_str(
    json_t     *op,
    const char *key)
{
    json_t *v = json_object_get(op, key);

    if (!v || !json_is_string(v)) {
        fprintf(stderr, "trace format error: op field '%s' not a string\n",
                key);
        exit(2);
    }
    return json_string_value(v);
} /* op_str */

static int
op_bool(
    json_t     *op,
    const char *key)
{
    json_t *v = json_object_get(op, key);

    if (!v || !json_is_boolean(v)) {
        fprintf(stderr, "trace format error: op field '%s' not a bool\n", key);
        exit(2);
    }
    return json_is_true(v);
} /* op_bool */

/* The tag of a sum-type field like op["cmode"] = {"tag": "Exclusive", ...} */
static const char *
op_tag(
    json_t     *op,
    const char *key)
{
    json_t *v   = json_object_get(op, key);
    json_t *tag = v ? json_object_get(v, "tag") : NULL;

    if (!tag || !json_is_string(tag)) {
        fprintf(stderr, "trace format error: op field '%s' has no tag\n", key);
        exit(2);
    }
    return json_string_value(tag);
} /* op_tag */

/* fs is the model's post-state: {"#map": [[fid, node], ...]}.  Returns the
 * node record for fid, or NULL if the fid is not in the post-state. */
static json_t *
fs_node(
    json_t *fs,
    int64_t fid)
{
    json_t *pairs = json_object_get(fs, "#map");
    json_t *pair;
    size_t  i;

    if (!pairs) {
        fprintf(stderr, "trace format error: fs is not a #map\n");
        exit(2);
    }
    json_array_foreach(pairs, i, pair)
    {
        if (itf_i64(json_array_get(pair, 0)) == fid) {
            return json_array_get(pair, 1);
        }
    }
    return NULL;
} /* fs_node */

/* node["ents"] name -> fid lookup; returns -1 if absent. */
static int64_t
ents_fid(
    json_t     *node,
    const char *name)
{
    json_t *pairs = json_object_get(json_object_get(node, "ents"), "#map");
    json_t *pair;
    size_t  i;

    json_array_foreach(pairs, i, pair)
    {
        if (strcmp(json_string_value(json_array_get(pair, 0)), name) == 0) {
            return itf_i64(json_array_get(pair, 1));
        }
    }
    return -1;
} /* ents_fid */

static ftype3
ftype_wire(const char *tag)
{
    if (strcmp(tag, "TReg") == 0) {
        return NF3REG;
    }
    if (strcmp(tag, "TDir") == 0) {
        return NF3DIR;
    }
    if (strcmp(tag, "TLnk") == 0) {
        return NF3LNK;
    }
    if (strcmp(tag, "TFifo") == 0) {
        return NF3FIFO;
    }
    if (strcmp(tag, "TSock") == 0) {
        return NF3SOCK;
    }
    fprintf(stderr, "trace format error: unknown ftype %s\n", tag);
    exit(2);
} /* ftype_wire */

/* ---- known-deviation registry -------------------------------------------- */

/* Registry of known chimera deviations from RFC 1813 (see DEVIATIONS.md):
 * the model always encodes the RFC-correct reply; a *status-only* divergence
 * listed here is recorded and tolerated instead of failing the replay.  A
 * tolerated deviation skips the OK-path checks (learn_fh/check_attrs), so it
 * cannot desync a stateful replay: the model leaves fs unchanged on the error
 * it asserts, and chimera's differing status is not accompanied by a state
 * change the model would miss.  New entries take the shape
 * { .id, .op_tag, .expected_status, .actual_status }.
 *
 * The entries below are backend-conditioned in practice by being self-selecting
 * on the actual status: the backend that answers RFC-correctly matches
 * `expected` exactly (reconcile is never consulted), and only the deviating
 * backend hits the tolerated pair.  Each is annotated with which backend
 * family deviates. */
struct deviation {
    const char *id;
    const char *op_tag;
    uint32_t    expected_status;
    uint32_t    actual_status;
};

/* *INDENT-OFF* */
/* uncrustify 0.78.1 oscillates on the aligned initializer columns below; pin a
 * stable manual alignment. */
static const struct deviation known_deviations[] = {
    /* EXCLUSIVE-create same-verifier retry (RFC 1813 3.3.8): the model asserts
     * the idempotent OK the RFC recommends.  chimera stores the create verifier
     * in the file's atime/mtime (the RFC's own agreed-upon mechanism); on the
     * passthrough backends (linux/io_uring) an intervening READ of the file
     * bumps atime in the kernel, so a later same-verifier retry no longer
     * matches and returns EXIST.  RFC idempotency is only guaranteed for a true
     * retransmit, so this is permitted; the mkfs backends do not touch atime on
     * read and return the OK the model asserts. */
    { "exclusive-create-retry-exist", "OCreate",  0, 17 },

    /* CREATE over an existing name in a directory the caller cannot SEARCH
     * (execute): the model requires search permission to resolve the name at
     * all and asserts ACCES ahead of any existence/type result (POSIX path
     * resolution).  The passthrough backends enforce this in the kernel and
     * match ACCES directly.  The mkfs backends (memfs/diskfs/cairn) omit the
     * search check on the existing-entry path -- chimera bug #1771 -- and leak
     * the entry by returning its type-based reply instead: EISDIR over a
     * directory, EXIST over another non-regular object, or OK (the existing
     * regular file) for an UNCHECKED create.  Tracked for fix in #1771; when
     * fixed, delete these three entries. */
    { "create-search-perm-1771",      "OCreate", 13, 21 },
    { "create-search-perm-1771",      "OCreate", 13, 17 },
    { "create-search-perm-1771",      "OCreate", 13,  0 },
};
/* *INDENT-ON* */

static const struct deviation *
reconcile(
    const char *tag,
    uint32_t    expected,
    uint32_t    actual)
{
    size_t i;

    for (i = 0;
         i < sizeof(known_deviations) / sizeof(known_deviations[0]);
         i++) {
        const struct deviation *dev = &known_deviations[i];

        if (strcmp(dev->op_tag, tag) == 0 &&
            dev->expected_status == expected &&
            dev->actual_status == actual) {
            return dev;
        }
    }
    return NULL;
} /* reconcile */

/* RFC 1813 does not mandate error precedence when more than one error condition
 * applies to a single request.  The model commits to one choice (which the
 * mkfs backends mirror); the linux/io_uring passthrough backends follow the
 * host kernel's order, which is equally valid.  Accept those standard-permitted
 * alternatives so a legitimate precedence difference is not a divergence.  This
 * is distinct from a chimera deviation (reconcile above): neither answer is
 * wrong. */
static int
status_precedence_ok(
    const char *tag,
    uint32_t    expected,
    uint32_t    actual)
{
    /* LINK whose source is a directory onto an already-existing name: the
     * model reports ISDIR (21, source is a directory); Linux reports EXIST
     * (17), having checked target existence first. */
    if (strcmp(tag, "OLink") == 0 && expected == 21 && actual == 17) {
        return 1;
    }
    /* RENAME onto a non-empty directory: the model reports ISDIR (21, target
     * type conflict); Linux reports NOTEMPTY (66), having checked emptiness
     * first. */
    if (strcmp(tag, "ORename") == 0 && expected == 21 && actual == 66) {
        return 1;
    }
    return 0;
} /* status_precedence_ok */

/* ---- oracle checks (ports of the Replayer methods) ----------------------- */

static void
fh_hex(
    const struct mbt_fh *fh,
    char                *out,
    size_t               out_size)
{
    uint32_t i;
    size_t   n = 0;

    for (i = 0; i < fh->len && n + 3 < out_size; i++) {
        n += snprintf(out + n, out_size - n, "%02x", fh->data[i]);
    }
    out[n] = '\0';
} /* fh_hex */

/* Missing fid means a harness bug or an earlier divergence; the step
 * cannot even be sent, so it fails via mism and a NULL return. */
static const struct mbt_fh *
real_fh(
    struct oracle *o,
    int64_t        fid,
    struct mism   *m)
{
    if (fid < 0 || fid >= MBT_MAX_FIDS || !o->fh[fid].has) {
        mism_add(m, "model fid %" PRId64 " has no learned file handle "
                 "(harness bug or earlier divergence)", fid);
        return NULL;
    }
    return &o->fh[fid];
} /* real_fh */

static void
learn_fh(
    struct oracle       *o,
    int64_t              fid,
    const struct mbt_fh *fh,
    struct mism         *m)
{
    char hex_known[2 * NFS3_FHSIZE + 1];
    char hex_new[2 * NFS3_FHSIZE + 1];

    if (!fh->has) {
        mism_add(m, "server returned no file handle for fid %" PRId64
                 " (handle_follows=0)", fid);
        return;
    }
    if (fid < 0 || fid >= MBT_MAX_FIDS) {
        mism_add(m, "fid %" PRId64 " out of range", fid);
        return;
    }
    if (fid > o->max_fid_seen) {
        o->max_fid_seen = fid;
    }
    if (!o->fh[fid].has) {
        o->fh[fid] = *fh;
    } else if (!mbt_fh_eq(&o->fh[fid], fh)) {
        fh_hex(&o->fh[fid], hex_known, sizeof(hex_known));
        fh_hex(fh, hex_new, sizeof(hex_new));
        mism_add(m, "fid %" PRId64 ": file handle changed: was %s, now %s",
                 fid, hex_known, hex_new);
    }
} /* learn_fh */

/* Compare a returned fattr3 against the model's post-state node. */
static void
check_attrs(
    struct oracle         *o,
    int64_t                fid,
    const struct mbt_attr *attrs,
    json_t                *post_fs,
    struct mism           *m,
    const char            *what)
{
    json_t     *node;
    const char *ftype;
    ftype3      wire_type;
    int64_t     expect;
    int64_t     other;

    if (!attrs->has) {
        o->attr_skips++;
        return;
    }
    o->attr_checks++;

    node = fs_node(post_fs, fid);
    if (!node) {
        mism_add(m, "%s: fid %" PRId64 " not in model post-state", what, fid);
        return;
    }

    ftype     = op_tag(node, "ftype");
    wire_type = ftype_wire(ftype);
    if (attrs->a.type != wire_type) {
        mism_add(m, "%s: type: expected %d (%s), got %d",
                 what, wire_type, ftype, attrs->a.type);
    }

    /* The standard leaves two modes unspecified, so conformant backends
     * legitimately differ and the mode must not be asserted:
     *   - Symlink permission bits (POSIX): Linux forces 0777, memfs keeps 0755.
     *   - An exclusive-created regular file (RFC 1813 3.3.8): its mode is
     *     undefined until the client's follow-up SETATTR, which the model marks
     *     by a non-zero exclusive verifier (xverf); memfs defaults 0644, the
     *     linux/io_uring backends 0600.
     * The model keeps a concrete mode for its own DAC evaluation either way. */
    expect = op_i64(node, "mode");
    if (strcmp(ftype, "TLnk") != 0 && op_i64(node, "xverf") == 0 &&
        (attrs->a.mode & 07777) != (uint32_t) expect) {
        mism_add(m, "%s: mode: expected %#o, got %#o",
                 what, (unsigned) expect, attrs->a.mode & 07777);
    }

    if (strcmp(ftype, "TDir") != 0) {
        expect = op_i64(node, "nlink");
        if (attrs->a.nlink != (uint32_t) expect) {
            mism_add(m, "%s: nlink: expected %" PRId64 ", got %u",
                     what, expect, attrs->a.nlink);
        }
    }

    if (strcmp(ftype, "TReg") == 0) {
        expect = (int64_t) json_array_size(json_object_get(node, "data")) *
            o->block_size;
        if (attrs->a.size != (uint64_t) expect) {
            mism_add(m, "%s: size: expected %" PRId64 ", got %" PRIu64,
                     what, expect, attrs->a.size);
        }
    } else if (strcmp(ftype, "TLnk") == 0) {
        expect = (int64_t) strlen(op_str(node, "target"));
        if (attrs->a.size != (uint64_t) expect) {
            mism_add(m, "%s: symlink size: expected %" PRId64
                     ", got %" PRIu64, what, expect, attrs->a.size);
        }
    }

    if (fid > o->max_fid_seen) {
        o->max_fid_seen = fid;
    }
    if (!o->fileid_known[fid]) {
        for (other = 0; other <= o->max_fid_seen; other++) {
            if (other != fid && o->fileid_known[other] &&
                o->fileid[other] == attrs->a.fileid &&
                fs_node(post_fs, other) != NULL) {
                mism_add(m, "%s: fileid %" PRIu64 " of fid %" PRId64
                         " collides with live fid %" PRId64,
                         what, attrs->a.fileid, fid, other);
            }
        }
        o->fileid[fid]       = attrs->a.fileid;
        o->fileid_known[fid] = 1;
    } else if (o->fileid[fid] != attrs->a.fileid) {
        mism_add(m, "%s: fileid: fid %" PRId64 " previously reported "
                 "%" PRIu64 ", now %" PRIu64,
                 what, fid, o->fileid[fid], attrs->a.fileid);
    }
} /* check_attrs */

/* True if the reply status matches (proceed with OK-path checks).  A
 * registered deviation is tolerated; an unregistered mismatch is a hard
 * failure.  Mirrors Replayer.check_status. */
static int
check_status(
    struct oracle *o,
    const char    *tag,
    uint32_t       expected,
    uint32_t       actual,
    struct mism   *m)
{
    (void) o;
    if (actual == expected) {
        return 1;
    }
    if (status_precedence_ok(tag, expected, actual)) {
        return 0;
    }
    if (reconcile(tag, expected, actual) != NULL) {
        return 0;
    }
    mism_add(m, "status: expected %u, got %u", expected, actual);
    return 0;
} /* check_status */

static void
block_bytes(
    struct oracle *o,
    int64_t        sym,
    uint8_t       *out)
{
    memset(out, sym == 0 ? 0 : (int) (0x40 + sym), o->block_size);
} /* block_bytes */

/* ---- per-procedure handlers ---------------------------------------------- */

static void
op_lookup(
    struct oracle *o,
    json_t        *op,
    json_t        *post_fs,
    struct mism   *m)
{
    const struct mbt_fh *dir = real_fh(o, op_i64(op, "dir"), m);
    const char          *name;
    struct mbt_result   *res;
    uint32_t             expected = (uint32_t) op_i64(op, "status");

    if (!dir) {
        return;
    }
    name = op_str(op, "name");
    res  = mbt_lookup(o->env, dir, name, (uint32_t) strlen(name));
    if (check_status(o, "OLookup", expected, res->status, m) &&
        expected == NFS3_OK) {
        learn_fh(o, op_i64(op, "child"), &res->obj_fh, m);
        check_attrs(o, op_i64(op, "child"), &res->obj_attrs, post_fs, m,
                    "obj_attrs");
    }
} /* op_lookup */

static void
op_getattr(
    struct oracle *o,
    json_t        *op,
    json_t        *post_fs,
    struct mism   *m)
{
    const struct mbt_fh *fh = real_fh(o, op_i64(op, "obj"), m);
    struct mbt_result   *res;
    uint32_t             expected = (uint32_t) op_i64(op, "status");

    if (!fh) {
        return;
    }
    res = mbt_getattr(o->env, fh);
    if (check_status(o, "OGetattr", expected, res->status, m) &&
        expected == NFS3_OK) {
        check_attrs(o, op_i64(op, "obj"), &res->obj_attrs, post_fs, m,
                    "attrs");
    }
} /* op_getattr */

static void
op_stalegetattr(
    struct oracle *o,
    json_t        *op,
    json_t        *post_fs,
    struct mism   *m)
{
    const struct mbt_fh *fh = real_fh(o, op_i64(op, "obj"), m);
    struct mbt_result   *res;

    (void) post_fs;
    if (!fh) {
        return;
    }
    res = mbt_getattr(o->env, fh);
    check_status(o, "OStaleGetattr", (uint32_t) op_i64(op, "status"),
                 res->status, m);
} /* op_stalegetattr */

static void
op_create(
    struct oracle *o,
    json_t        *op,
    json_t        *post_fs,
    struct mism   *m)
{
    const struct mbt_fh *dir = real_fh(o, op_i64(op, "dir"), m);
    const char          *name;
    const char          *cmode = op_tag(op, "cmode");
    struct mbt_result   *res;
    uint32_t             expected = (uint32_t) op_i64(op, "status");
    uint8_t              verf[NFS3_CREATEVERFSIZE];
    uint64_t             v;
    int                  i;

    if (!dir) {
        return;
    }
    name = op_str(op, "name");
    if (strcmp(cmode, "Exclusive") == 0) {
        v = (uint64_t) op_i64(op, "verf");
        for (i = 0; i < 8; i++) {
            verf[i] = (uint8_t) (v >> (56 - 8 * i));
        }
        res = mbt_create(o->env, dir, name, (uint32_t) strlen(name),
                         EXCLUSIVE, -1, verf);
    } else {
        res = mbt_create(o->env, dir, name, (uint32_t) strlen(name),
                         strcmp(cmode, "Guarded") == 0 ? GUARDED : UNCHECKED,
                         (int) op_i64(op, "mode"), NULL);
    }
    if (check_status(o, "OCreate", expected, res->status, m) &&
        expected == NFS3_OK) {
        learn_fh(o, op_i64(op, "obj"), &res->obj_fh, m);
        check_attrs(o, op_i64(op, "obj"), &res->obj_attrs, post_fs, m,
                    "obj_attrs");
    }
} /* op_create */

static void
op_setattr(
    struct oracle *o,
    json_t        *op,
    json_t        *post_fs,
    struct mism   *m)
{
    const struct mbt_fh *fh = real_fh(o, op_i64(op, "obj"), m);
    struct mbt_result   *res;
    struct nfstime3      guard;
    struct nfstime3     *guardp     = NULL;
    uint32_t             expected   = (uint32_t) op_i64(op, "status");
    int64_t              guard_kind = op_i64(op, "guard");
    int64_t              mode       = op_i64(op, "mode");
    int64_t              size_blk   = op_i64(op, "sizeBlocks");

    if (!fh) {
        return;
    }
    if (guard_kind == 1) {
        /* A matching guard needs the object's live ctime; fetch it with an
         * auxiliary GETATTR (not part of the modeled sequence). */
        res = mbt_getattr(o->env, fh);
        if (res->status != NFS3_OK) {
            mism_add(m, "pre-guard GETATTR failed: %u", res->status);
            return;
        }
        guard  = res->obj_attrs.a.ctime;
        guardp = &guard;
    } else if (guard_kind == 2) {
        guard.seconds  = 1;
        guard.nseconds = 1;
        guardp         = &guard;
    }
    res = mbt_setattr(o->env, fh,
                      mode < 0 ? -1 : (int) mode,
                      size_blk < 0 ? -1
                                   : (int64_t) size_blk * o->block_size,
                      guardp);
    if (check_status(o, "OSetattr", expected, res->status, m) &&
        expected == NFS3_OK) {
        check_attrs(o, op_i64(op, "obj"), &res->wcc_after, post_fs, m,
                    "wcc.after");
    }
} /* op_setattr */

static void
op_access(
    struct oracle *o,
    json_t        *op,
    json_t        *post_fs,
    struct mism   *m)
{
    const struct mbt_fh *fh = real_fh(o, op_i64(op, "obj"), m);
    struct mbt_result   *res;
    uint32_t             expected = (uint32_t) op_i64(op, "status");

    if (!fh) {
        return;
    }
    res = mbt_access(o->env, fh, (uint32_t) op_i64(op, "mask"));
    if (check_status(o, "OAccess", expected, res->status, m) &&
        expected == NFS3_OK) {
        uint32_t eacc = (uint32_t) op_i64(op, "access");

        /* Tolerate chimera's root/AUTH_NONE DAC override granting
         * ACCESS3_EXECUTE (0x20) on a file with no execute mode bit -- the
         * mirror of the NFSv4 D4-19 case.  ACCESS is advisory (RFC 1813
         * 3.3.4), the Linux server and NFS-Ganesha withhold exec-override
         * absent an x bit, and every other bit matches. */
        int      exec_overgrant = (eacc & 0x20) == 0 &&
            res->access == (eacc | 0x20);

        if (res->access != eacc && !exec_overgrant) {
            mism_add(m, "access: expected %#x, got %#x",
                     (unsigned) eacc, res->access);
        }
        check_attrs(o, op_i64(op, "obj"), &res->obj_attrs, post_fs, m,
                    "obj_attrs");
    }
} /* op_access */

static void
op_symlink(
    struct oracle *o,
    json_t        *op,
    json_t        *post_fs,
    struct mism   *m)
{
    const struct mbt_fh *dir = real_fh(o, op_i64(op, "dir"), m);
    const char          *name;
    struct mbt_result   *res;
    uint32_t             expected = (uint32_t) op_i64(op, "status");

    if (!dir) {
        return;
    }
    name = op_str(op, "name");
    /* Request mode 0777; the model expects the server to ignore it and
     * store 0755 (see symlinkNode in nfs3.qnt). */
    res = mbt_symlink(o->env, dir, name, (uint32_t) strlen(name),
                      op_str(op, "target"), 0777);
    if (check_status(o, "OSymlink", expected, res->status, m) &&
        expected == NFS3_OK) {
        learn_fh(o, op_i64(op, "obj"), &res->obj_fh, m);
        check_attrs(o, op_i64(op, "obj"), &res->obj_attrs, post_fs, m,
                    "obj_attrs");
    }
} /* op_symlink */

static void
op_readlink(
    struct oracle *o,
    json_t        *op,
    json_t        *post_fs,
    struct mism   *m)
{
    const struct mbt_fh *fh = real_fh(o, op_i64(op, "obj"), m);
    struct mbt_result   *res;
    uint32_t             expected = (uint32_t) op_i64(op, "status");
    const char          *target;

    if (!fh) {
        return;
    }
    res = mbt_readlink(o->env, fh);
    if (check_status(o, "OReadlink", expected, res->status, m) &&
        expected == NFS3_OK) {
        target = op_str(op, "target");
        if (strlen(target) != res->target_len ||
            memcmp(target, res->target, res->target_len) != 0) {
            mism_add(m, "readlink: expected '%s', got '%.*s'",
                     target, (int) res->target_len, res->target);
        }
        check_attrs(o, op_i64(op, "obj"), &res->obj_attrs, post_fs, m,
                    "obj_attrs");
    }
} /* op_readlink */

static void
op_mknod(
    struct oracle *o,
    json_t        *op,
    json_t        *post_fs,
    struct mism   *m)
{
    const struct mbt_fh *dir = real_fh(o, op_i64(op, "dir"), m);
    const char          *name;
    struct mbt_result   *res;
    uint32_t             expected = (uint32_t) op_i64(op, "status");

    if (!dir) {
        return;
    }
    name = op_str(op, "name");
    res  = mbt_mknod(o->env, dir, name, (uint32_t) strlen(name),
                     ftype_wire(op_tag(op, "ftype")),
                     (int) op_i64(op, "mode"));
    if (check_status(o, "OMknod", expected, res->status, m) &&
        expected == NFS3_OK) {
        learn_fh(o, op_i64(op, "obj"), &res->obj_fh, m);
        check_attrs(o, op_i64(op, "obj"), &res->obj_attrs, post_fs, m,
                    "obj_attrs");
    }
} /* op_mknod */

static void
op_rename(
    struct oracle *o,
    json_t        *op,
    json_t        *post_fs,
    struct mism   *m)
{
    const struct mbt_fh *from_dir = real_fh(o, op_i64(op, "fromDir"), m);
    const struct mbt_fh *to_dir   = real_fh(o, op_i64(op, "toDir"), m);
    const char          *from_name;
    const char          *to_name;
    struct mbt_result   *res;

    (void) post_fs;
    if (!from_dir || !to_dir) {
        return;
    }
    from_name = op_str(op, "fromName");
    to_name   = op_str(op, "toName");
    res       = mbt_rename(o->env,
                           from_dir, from_name, (uint32_t) strlen(from_name),
                           to_dir, to_name, (uint32_t) strlen(to_name));
    check_status(o, "ORename", (uint32_t) op_i64(op, "status"),
                 res->status, m);
} /* op_rename */

static void
op_link(
    struct oracle *o,
    json_t        *op,
    json_t        *post_fs,
    struct mism   *m)
{
    const struct mbt_fh *obj = real_fh(o, op_i64(op, "obj"), m);
    const struct mbt_fh *dir = real_fh(o, op_i64(op, "dir"), m);
    const char          *name;
    struct mbt_result   *res;
    uint32_t             expected = (uint32_t) op_i64(op, "status");

    if (!obj || !dir) {
        return;
    }
    name = op_str(op, "name");
    res  = mbt_link(o->env, obj, dir, name, (uint32_t) strlen(name));
    if (check_status(o, "OLink", expected, res->status, m) &&
        expected == NFS3_OK) {
        check_attrs(o, op_i64(op, "obj"), &res->obj_attrs, post_fs, m,
                    "file_attributes");
    }
} /* op_link */

static void
op_readdir(
    struct oracle *o,
    json_t        *op,
    json_t        *post_fs,
    struct mism   *m)
{
    const struct mbt_fh *dir = real_fh(o, op_i64(op, "dir"), m);
    struct mbt_result   *res;
    uint32_t             expected = (uint32_t) op_i64(op, "status");
    int                  plus     = op_bool(op, "plus");

    json_t              *names = itf_seq(json_object_get(op, "names"));
    json_t              *dir_node;
    json_t              *jname;
    size_t               i;
    int                  j;
    int64_t              fid;

    if (!dir) {
        return;
    }
    res = plus ? mbt_readdirplus(o->env, dir) : mbt_readdir(o->env, dir);
    if (!check_status(o, "OReaddir", expected, res->status, m) ||
        expected != NFS3_OK) {
        return;
    }

    if (res->entries_overflow) {
        mism_add(m, "readdir: more than %d entries returned",
                 MBT_MAX_ENTRIES);
        return;
    }

    for (j = 0; j < res->nentries; j++) {
        for (i = (size_t) j + 1; i < (size_t) res->nentries; i++) {
            if (strcmp(res->entries[j].name, res->entries[i].name) == 0) {
                mism_add(m, "readdir: duplicate entry '%s'",
                         res->entries[j].name);
            }
        }
    }

    /* chimera emits "." and ".." (probed 2026-08-08): expected set is the
     * model's names plus the two dot entries; compare both directions,
     * dot entries included -- a listing missing "." or ".." is wrong. */
    for (i = 0; i < 2; i++) {
        const char *dot   = i ? ".." : ".";
        int         found = 0;

        for (j = 0; j < res->nentries; j++) {
            if (strcmp(res->entries[j].name, dot) == 0) {
                found = 1;
                break;
            }
        }
        if (!found) {
            mism_add(m, "readdir: expected entry '%s' missing", dot);
        }
    }
    json_array_foreach(names, i, jname)
    {
        const char *want  = json_string_value(jname);
        int         found = 0;

        for (j = 0; j < res->nentries; j++) {
            if (strcmp(res->entries[j].name, want) == 0) {
                found = 1;
                break;
            }
        }
        if (!found) {
            mism_add(m, "readdir: expected entry '%s' missing", want);
        }
    }
    for (j = 0; j < res->nentries; j++) {
        const char *got = res->entries[j].name;
        int         found;

        if (strcmp(got, ".") == 0 || strcmp(got, "..") == 0) {
            continue;
        }
        found = 0;
        json_array_foreach(names, i, jname)
        {
            if (strcmp(json_string_value(jname), got) == 0) {
                found = 1;
                break;
            }
        }
        if (!found) {
            mism_add(m, "readdir: unexpected entry '%s'", got);
        }
    }

    if (!res->eof) {
        mism_add(m, "readdir: eof not set on single-shot full listing");
    }

    dir_node = fs_node(post_fs, op_i64(op, "dir"));
    if (!dir_node) {
        mism_add(m, "readdir: dir fid %" PRId64 " not in model post-state",
                 op_i64(op, "dir"));
        return;
    }

    for (j = 0; j < res->nentries; j++) {
        struct mbt_entry *e = &res->entries[j];

        if (strcmp(e->name, ".") == 0) {
            fid = op_i64(op, "dir");
        } else if (strcmp(e->name, "..") == 0) {
            continue;
        } else {
            fid = ents_fid(dir_node, e->name);
            if (fid < 0) {
                continue;
            }
        }
        if (fid >= MBT_MAX_FIDS) {
            continue;
        }
        if (fid > o->max_fid_seen) {
            o->max_fid_seen = fid;
        }
        if (!o->fileid_known[fid]) {
            o->fileid[fid]       = e->fileid;
            o->fileid_known[fid] = 1;
        } else if (o->fileid[fid] != e->fileid) {
            mism_add(m, "readdir: entry '%s': fileid of fid %" PRId64
                     " previously %" PRIu64 ", now %" PRIu64,
                     e->name, fid, o->fileid[fid], e->fileid);
        }
        if (plus && strcmp(e->name, ".") != 0) {
            char what[MBT_NAME_MAX + 16];

            learn_fh(o, fid, &e->fh, m);
            snprintf(what, sizeof(what), "readdirplus[%s]", e->name);
            check_attrs(o, fid, &e->attrs, post_fs, m, what);
        }
    }
} /* op_readdir */

static void
check_verf(
    struct oracle *o,
    const uint8_t *verf,
    const char    *what,
    struct mism   *m)
{
    if (!o->have_write_verf) {
        memcpy(o->write_verf, verf, NFS3_WRITEVERFSIZE);
        o->have_write_verf = 1;
    } else if (memcmp(o->write_verf, verf, NFS3_WRITEVERFSIZE) != 0) {
        mism_add(m, "%s: verifier changed: %02x%02x%02x%02x%02x%02x%02x%02x"
                 " -> %02x%02x%02x%02x%02x%02x%02x%02x", what,
                 o->write_verf[0], o->write_verf[1], o->write_verf[2],
                 o->write_verf[3], o->write_verf[4], o->write_verf[5],
                 o->write_verf[6], o->write_verf[7],
                 verf[0], verf[1], verf[2], verf[3],
                 verf[4], verf[5], verf[6], verf[7]);
    }
} /* check_verf */

static void
op_commit(
    struct oracle *o,
    json_t        *op,
    json_t        *post_fs,
    struct mism   *m)
{
    const struct mbt_fh *fh = real_fh(o, op_i64(op, "file"), m);
    struct mbt_result   *res;
    uint32_t             expected = (uint32_t) op_i64(op, "status");

    (void) post_fs;
    if (!fh) {
        return;
    }
    res = mbt_commit(o->env, fh);
    if (check_status(o, "OCommit", expected, res->status, m) &&
        expected == NFS3_OK) {
        check_verf(o, res->verf, "commit", m);
    }
} /* op_commit */

static void
op_fsstat(
    struct oracle *o,
    json_t        *op,
    json_t        *post_fs,
    struct mism   *m)
{
    struct mbt_result *res;
    uint32_t           expected = (uint32_t) op_i64(op, "status");

    (void) post_fs;
    res = mbt_fsstat(o->env, &o->root_fh);
    if (!check_status(o, "OFsstat", expected, res->status, m) ||
        expected != NFS3_OK) {
        return;
    }
    /* The model explicitly does not model space-used (nfs3.qnt: "space-used
     * are not modeled"), and RFC 1813 leaves the FSSTAT totals
     * implementation-defined -- different backends legitimately report
     * different capacities (memfs/cairn a fixed synthetic size, diskfs its
     * real device geometry, and free/avail shift as data is written).  Assert
     * only the ordering invariant the protocol guarantees, not a size. */
    if (res->tbytes == 0 || res->fbytes > res->tbytes ||
        res->abytes > res->fbytes) {
        mism_add(m, "fsstat bytes invariant (avail<=free<=total, total>0) "
                 "violated: t=%" PRIu64 " f=%" PRIu64 " a=%" PRIu64,
                 res->tbytes, res->fbytes, res->abytes);
    }
    /* Likewise the file-slot counts are implementation-defined and unmodeled;
     * RFC 1813 even lets a server report 0 for "unknown", so assert only the
     * ordering (no total>0 requirement, unlike bytes). */
    if (res->ffiles > res->tfiles || res->afiles > res->ffiles) {
        mism_add(m, "fsstat files invariant (avail<=free<=total) violated: "
                 "t=%" PRIu64 " f=%" PRIu64 " a=%" PRIu64,
                 res->tfiles, res->ffiles, res->afiles);
    }
    if (res->invarsec != 0) {
        mism_add(m, "invarsec: expected 0, got %u", res->invarsec);
    }
} /* op_fsstat */

static void
op_fsinfo(
    struct oracle *o,
    json_t        *op,
    json_t        *post_fs,
    struct mism   *m)
{
    struct mbt_result *res;
    uint32_t           expected = (uint32_t) op_i64(op, "status");

    (void) post_fs;
    res = mbt_fsinfo(o->env, &o->root_fh);
    if (!check_status(o, "OFsinfo", expected, res->status, m) ||
        expected != NFS3_OK) {
        return;
    }
    if (res->rtmax != MBT_FSINFO_RTMAX || res->rtpref != MBT_FSINFO_RTPREF ||
        res->rtmult != MBT_FSINFO_RTMULT) {
        mism_add(m, "fsinfo rt: got max=%u pref=%u mult=%u",
                 res->rtmax, res->rtpref, res->rtmult);
    }
    if (res->wtmax != MBT_FSINFO_WTMAX || res->wtpref != MBT_FSINFO_WTPREF ||
        res->wtmult != MBT_FSINFO_WTMULT) {
        mism_add(m, "fsinfo wt: got max=%u pref=%u mult=%u",
                 res->wtmax, res->wtpref, res->wtmult);
    }
    if (res->dtpref != MBT_FSINFO_DTPREF) {
        mism_add(m, "dtpref: expected %u, got %u",
                 MBT_FSINFO_DTPREF, res->dtpref);
    }
    if (res->maxfilesize != MBT_FSINFO_MAXFILESIZE) {
        mism_add(m, "maxfilesize: got %" PRIu64, res->maxfilesize);
    }
    if (res->time_delta.seconds != 0 || res->time_delta.nseconds != 1) {
        mism_add(m, "time_delta: expected (0,1), got (%u,%u)",
                 res->time_delta.seconds, res->time_delta.nseconds);
    }
    if (res->properties != MBT_FSINFO_PROPERTIES) {
        mism_add(m, "properties: expected %#x, got %#x",
                 MBT_FSINFO_PROPERTIES, res->properties);
    }
} /* op_fsinfo */

static void
op_pathconf(
    struct oracle *o,
    json_t        *op,
    json_t        *post_fs,
    struct mism   *m)
{
    const struct mbt_fh *fh = real_fh(o, op_i64(op, "obj"), m);
    struct mbt_result   *res;
    uint32_t             expected = (uint32_t) op_i64(op, "status");

    (void) post_fs;
    if (!fh) {
        return;
    }
    res = mbt_pathconf(o->env, fh);
    if (!check_status(o, "OPathconf", expected, res->status, m) ||
        expected != NFS3_OK) {
        return;
    }
    if (res->linkmax != MBT_PATHCONF_LINKMAX) {
        mism_add(m, "linkmax: expected %#x, got %#x",
                 MBT_PATHCONF_LINKMAX, res->linkmax);
    }
    if (res->name_max != MBT_PATHCONF_NAME_MAX) {
        mism_add(m, "name_max: expected %u, got %u",
                 MBT_PATHCONF_NAME_MAX, res->name_max);
    }
    if (!res->no_trunc || !res->chown_restricted) {
        mism_add(m, "pathconf flags: no_trunc=%d chown_restricted=%d",
                 res->no_trunc, res->chown_restricted);
    }
    if (res->case_insensitive || !res->case_preserving) {
        mism_add(m, "pathconf case flags: insensitive=%d preserving=%d",
                 res->case_insensitive, res->case_preserving);
    }
} /* op_pathconf */

static void
op_mkdir(
    struct oracle *o,
    json_t        *op,
    json_t        *post_fs,
    struct mism   *m)
{
    const struct mbt_fh *dir = real_fh(o, op_i64(op, "dir"), m);
    const char          *name;
    struct mbt_result   *res;
    uint32_t             expected = (uint32_t) op_i64(op, "status");

    if (!dir) {
        return;
    }
    name = op_str(op, "name");
    res  = mbt_mkdir(o->env, dir, name, (uint32_t) strlen(name),
                     (int) op_i64(op, "mode"));
    if (check_status(o, "OMkdir", expected, res->status, m) &&
        expected == NFS3_OK) {
        learn_fh(o, op_i64(op, "obj"), &res->obj_fh, m);
        check_attrs(o, op_i64(op, "obj"), &res->obj_attrs, post_fs, m,
                    "obj_attrs");
    }
} /* op_mkdir */

static void
op_write(
    struct oracle *o,
    json_t        *op,
    json_t        *post_fs,
    struct mism   *m)
{
    const struct mbt_fh *fh = real_fh(o, op_i64(op, "file"), m);
    struct mbt_result   *res;
    uint32_t             expected = (uint32_t) op_i64(op, "status");
    int64_t              count    = op_i64(op, "count");
    int64_t              stable   = op_i64(op, "stable");
    uint32_t             len      = (uint32_t) (count * o->block_size);
    int64_t              i;

    if (!fh) {
        return;
    }
    if (len > MBT_MAX_DATA) {
        mism_add(m, "write: %u bytes exceeds harness scratch", len);
        return;
    }
    for (i = 0; i < count; i++) {
        block_bytes(o, op_i64(op, "pat"), o->scratch + i * o->block_size);
    }
    res = mbt_write(o->env, fh,
                    (uint64_t) op_i64(op, "offset") * o->block_size,
                    o->scratch, len, (stable_how) stable);
    if (check_status(o, "OWrite", expected, res->status, m) &&
        expected == NFS3_OK) {
        if (res->count != len) {
            mism_add(m, "count: expected %u, got %u", len, res->count);
        }
        if ((int64_t) res->committed < stable) {
            mism_add(m, "committed %u weaker than requested stability "
                     "%" PRId64, res->committed, stable);
        }
        check_verf(o, res->verf, "write", m);
        check_attrs(o, op_i64(op, "file"), &res->wcc_after, post_fs, m,
                    "wcc.after");
    }
} /* op_write */

static void
op_read(
    struct oracle *o,
    json_t        *op,
    json_t        *post_fs,
    struct mism   *m)
{
    const struct mbt_fh *fh = real_fh(o, op_i64(op, "file"), m);
    struct mbt_result   *res;
    uint32_t             expected = (uint32_t) op_i64(op, "status");

    json_t              *blocks;
    json_t              *jblk;
    size_t               i;
    uint32_t             expect_len;

    if (!fh) {
        return;
    }
    res = mbt_read(o->env, fh,
                   (uint64_t) op_i64(op, "offset") * o->block_size,
                   (uint32_t) (op_i64(op, "count") * o->block_size));
    if (check_status(o, "ORead", expected, res->status, m) &&
        expected == NFS3_OK) {
        blocks     = itf_seq(json_object_get(op, "blocks"));
        expect_len = (uint32_t) (json_array_size(blocks) * o->block_size);
        if (expect_len > MBT_MAX_DATA) {
            mism_add(m, "read: expectation exceeds harness scratch");
            return;
        }
        json_array_foreach(blocks, i, jblk)
        {
            block_bytes(o, itf_i64(jblk), o->scratch + i * o->block_size);
        }
        if (res->count != expect_len) {
            mism_add(m, "count: expected %u, got %u",
                     expect_len, res->count);
        }
        if (res->eof != op_bool(op, "eof")) {
            mism_add(m, "eof: expected %d, got %d",
                     op_bool(op, "eof"), res->eof);
        }
        if (res->data_len != expect_len ||
            memcmp(res->data, o->scratch, expect_len) != 0) {
            uint32_t off;

            mism_add(m, "data mismatch: expected %u bytes, got %u bytes",
                     expect_len, res->data_len);
            for (off = 0;
                 off + o->block_size <= expect_len &&
                 off + o->block_size <= res->data_len;
                 off += o->block_size) {
                if (memcmp(res->data + off, o->scratch + off,
                           o->block_size) != 0) {
                    mism_add(m, "first differing block %u: expected byte "
                             "%#x, got byte %#x", off / o->block_size,
                             o->scratch[off], res->data[off]);
                    break;
                }
            }
        }
        check_attrs(o, op_i64(op, "file"), &res->obj_attrs, post_fs, m,
                    "file_attributes");
    }
} /* op_read */

static void
op_remove(
    struct oracle *o,
    json_t        *op,
    json_t        *post_fs,
    struct mism   *m)
{
    const struct mbt_fh *dir = real_fh(o, op_i64(op, "dir"), m);
    const char          *name;
    struct mbt_result   *res;

    (void) post_fs;
    if (!dir) {
        return;
    }
    name = op_str(op, "name");
    res  = mbt_remove(o->env, dir, name, (uint32_t) strlen(name));
    check_status(o, "ORemove", (uint32_t) op_i64(op, "status"),
                 res->status, m);
} /* op_remove */

static void
op_rmdir(
    struct oracle *o,
    json_t        *op,
    json_t        *post_fs,
    struct mism   *m)
{
    const struct mbt_fh *dir = real_fh(o, op_i64(op, "dir"), m);
    const char          *name;
    struct mbt_result   *res;

    (void) post_fs;
    if (!dir) {
        return;
    }
    name = op_str(op, "name");
    res  = mbt_rmdir(o->env, dir, name, (uint32_t) strlen(name));
    check_status(o, "ORmdir", (uint32_t) op_i64(op, "status"),
                 res->status, m);
} /* op_rmdir */

/* ---- dispatch ------------------------------------------------------------- */

typedef void (*op_handler_t)(
    struct oracle *o,
    json_t        *op,
    json_t        *post_fs,
    struct mism   *m);

/* *INDENT-OFF* */
static const struct {
    const char  *tag;
    op_handler_t fn;
} handlers[] = {
    { "OLookup",       op_lookup       },
    { "OGetattr",      op_getattr      },
    { "OStaleGetattr", op_stalegetattr },
    { "OSetattr",      op_setattr      },
    { "OAccess",       op_access       },
    { "OCreate",       op_create       },
    { "OMkdir",        op_mkdir        },
    { "OSymlink",      op_symlink      },
    { "OReadlink",     op_readlink     },
    { "OMknod",        op_mknod        },
    { "OWrite",        op_write        },
    { "ORead",         op_read         },
    { "ORemove",       op_remove       },
    { "ORmdir",        op_rmdir        },
    { "ORename",       op_rename       },
    { "OLink",         op_link         },
    { "OReaddir",      op_readdir      },
    { "OCommit",       op_commit       },
    { "OFsstat",       op_fsstat       },
    { "OFsinfo",       op_fsinfo       },
    { "OPathconf",     op_pathconf     },
};
/* *INDENT-ON* */

static op_handler_t
find_handler(const char *tag)
{
    size_t i;

    for (i = 0; i < sizeof(handlers) / sizeof(handlers[0]); i++) {
        if (strcmp(handlers[i].tag, tag) == 0) {
            return handlers[i].fn;
        }
    }
    return NULL;
} /* find_handler */

/*
 * Drive each operation under the credential the model chose for it.  The DAC
 * layer of nfs3.qnt tags the permission-sensitive ops (OCreate/OWrite/ORead)
 * with a `cred` (uid/gid/supplementary gids); replaying those under AUTH_SYS
 * with that identity is what lets the server's access checks reproduce the
 * model's predicted EACCES/EPERM.  Ops without a `cred` (namespace/metadata
 * setup) run as root, matching the model, which mints them as ROOT_CRED.  The
 * gids pointer references file-static storage that outlives the synchronous
 * RPC (evpl_rpc2_cred keeps externally-managed gid storage).
 */
static uint32_t mbt_cred_gids[64];

static void
mbt_set_cred(
    struct mbt_env *env,
    json_t         *op)
{
    json_t *cred = json_object_get(op, "cred");
    json_t *gids;
    size_t  n, i, cap = sizeof(mbt_cred_gids) / sizeof(mbt_cred_gids[0]);

    env->cred.flavor = EVPL_RPC2_AUTH_SYS;

    if (!cred) {
        env->cred.authsys.uid      = 0;
        env->cred.authsys.gid      = 0;
        env->cred.authsys.num_gids = 0;
        env->cred.authsys.gids     = NULL;
        return;
    }

    env->cred.authsys.uid = (uint32_t) op_i64(cred, "uid");
    env->cred.authsys.gid = (uint32_t) op_i64(cred, "gid");

    gids = json_object_get(cred, "gids");
    n    = 0;
    if (gids) {
        json_t *seq = itf_seq(gids);
        n = json_array_size(seq);
        if (n > cap) {
            n = cap;
        }
        for (i = 0; i < n; i++) {
            mbt_cred_gids[i] = (uint32_t) itf_i64(json_array_get(seq, i));
        }
    }
    env->cred.authsys.num_gids = (uint32_t) n;
    env->cred.authsys.gids     = n ? mbt_cred_gids : NULL;
} /* mbt_set_cred */

/* ---- replay driver -------------------------------------------------------- */

static void
history_push(
    struct oracle *o,
    int            idx,
    const char    *tag,
    json_t        *op,
    uint32_t       status)
{
    struct hist_ent *e;

    if (o->nhist == MBT_HISTORY) {
        free(o->history[0].op_dump);
        memmove(&o->history[0], &o->history[1],
                sizeof(o->history[0]) * (MBT_HISTORY - 1));
        o->nhist--;
    }
    e      = &o->history[o->nhist++];
    e->idx = idx;
    snprintf(e->tag, sizeof(e->tag), "%s", tag);
    e->status  = status;
    e->op_dump = json_dumps(op, JSON_COMPACT | JSON_ENCODE_ANY);
} /* history_push */

static void
report_divergence(
    const char    *trace_path,
    struct oracle *o,
    int            step,
    const char    *tag,
    json_t        *op,
    struct mism   *m)
{
    char    hex[2 * NFS3_FHSIZE + 1];
    char   *dump;
    int     i;
    int64_t fid;

    fprintf(stderr, "\n=== DIVERGENCE in %s ===\n", trace_path);
    dump = json_dumps(op, JSON_COMPACT | JSON_ENCODE_ANY);
    fprintf(stderr, "step %d: %s args/expectation: %s\n",
            step, tag, dump ?: "<?>");
    free(dump);
    for (i = 0; i < m->n; i++) {
        fprintf(stderr, "  MISMATCH: %s\n", m->msg[i]);
    }
    fprintf(stderr, "\nlast operations before failure:\n");
    for (i = 0; i < o->nhist; i++) {
        fprintf(stderr, "  [%4d] %s %s -> %u\n", o->history[i].idx,
                o->history[i].tag, o->history[i].op_dump ?: "<?>",
                o->history[i].status);
    }
    fprintf(stderr, "\nfid -> file handle map:\n");
    for (fid = 0; fid <= o->max_fid_seen; fid++) {
        if (o->fh[fid].has) {
            fh_hex(&o->fh[fid], hex, sizeof(hex));
            fprintf(stderr, "  %" PRId64 ": %s\n", fid, hex);
        }
    }
} /* report_divergence */

static int
run_trace(
    struct mbt_env *env,
    const char     *trace_path,
    const char     *fsname,
    int             block_size,
    double          max_attr_skip_rate,
    int             verbose,
    int             dry_run)
{
    json_error_t   jerr;

    json_t        *root;
    json_t        *states;
    json_t        *state;
    json_t        *last_op;
    json_t        *op;
    json_t        *fs;
    const char    *tag;
    op_handler_t   fn;
    struct oracle *o;
    size_t         idx;
    size_t         nstates;
    int            failed = 0;
    double         rate;
    int            total;
    struct mism    m;

    root = json_load_file(trace_path, 0, &jerr);
    if (!root) {
        fprintf(stderr, "%s: JSON parse error: %s (line %d)\n",
                trace_path, jerr.text, jerr.line);
        return 1;
    }
    states = json_object_get(root, "states");
    if (!states || !json_is_array(states) ||
        !json_object_get(root, "vars")) {
        fprintf(stderr, "%s: not an ITF trace\n", trace_path);
        json_decref(root);
        return 1;
    }
    nstates = json_array_size(states);

    if (dry_run) {
        printf("%s: %zu steps, format OK\n", trace_path, nstates - 1);
        json_decref(root);
        return 0;
    }

    /* Backstop for a server deadlock: with everything in one process a
     * hung reply spins in mbt_call_wait forever; SIGALRM's default
     * disposition kills the test with a nonzero status. */
    mbt_watchdog_arm(120);

    o = calloc(1, sizeof(*o));
    mbt_env_fs_setup(env, fsname);

    o->env          = env;
    o->block_size   = block_size;
    o->verbose      = verbose;
    o->fh           = calloc(MBT_MAX_FIDS, sizeof(*o->fh));
    o->fileid       = calloc(MBT_MAX_FIDS, sizeof(*o->fileid));
    o->fileid_known = calloc(MBT_MAX_FIDS, sizeof(*o->fileid_known));
    o->scratch      = malloc(MBT_MAX_DATA);

    char               mntpath[80];

    snprintf(mntpath, sizeof(mntpath), "/%s", fsname);

    struct mbt_result *res = mbt_mnt(env, mntpath);

    if (res->rpc_err != 0 || res->status != MNT3_OK || !res->obj_fh.has) {
        fprintf(stderr, "%s: MNT %s failed: rpc_err=%d status=%u\n",
                trace_path, mntpath, res->rpc_err, res->status);
        failed = 1;
        goto out;
    }
    o->root_fh = res->obj_fh;
    o->fh[0]   = res->obj_fh;    /* model fid 0 is the export root */
    mbt_null(env);

    for (idx = 1; idx < nstates; idx++) {
        state   = json_array_get(states, idx);
        last_op = json_object_get(state, "lastOp");
        fs      = json_object_get(state, "fs");
        if (!last_op || !fs) {
            fprintf(stderr, "%s: state %zu missing lastOp/fs\n",
                    trace_path, idx);
            failed = 1;
            goto out;
        }
        tag = json_string_value(json_object_get(last_op, "tag"));
        op  = json_object_get(last_op, "value");
        fn  = find_handler(tag ?: "");
        if (!fn) {
            fprintf(stderr, "%s: step %zu: no handler for %s\n",
                    trace_path, idx, tag ?: "<?>");
            failed = 1;
            goto out;
        }

        memset(&m, 0, sizeof(m));
        mbt_watchdog_at(trace_path, (int) idx, tag);
        mbt_set_cred(env, op);
        fn(o, op, fs, &m);
        history_push(o, (int) idx, tag, op, env->res.status);
        if (verbose) {
            printf("  [%4zu] %s -> %u\n", idx, tag, env->res.status);
        }
        if (m.n) {
            report_divergence(trace_path, o, (int) idx, tag, op, &m);
            failed = 1;
            goto out;
        }
    }

    total = o->attr_checks + o->attr_skips;
    rate  = total ? (double) o->attr_skips / total : 0.0;
    if (rate > max_attr_skip_rate) {
        fprintf(stderr, "%s: attribute skip rate %.0f%% exceeds %.0f%% "
                "(%d of %d replies had attributes_follow=0)\n",
                trace_path, rate * 100, max_attr_skip_rate * 100,
                o->attr_skips, total);
        failed = 1;
        goto out;
    }

    printf("%s: %zu steps replayed, %d attribute checks (%d skipped)\n",
           trace_path, nstates - 1, o->attr_checks, o->attr_skips);

 out:
    mbt_env_fs_teardown(env, fsname);
    while (o->nhist) {
        free(o->history[--o->nhist].op_dump);
    }
    free(o->fh);
    free(o->fileid);
    free(o->fileid_known);
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
    static struct option long_options[] = {
        { "trace",              required_argument,              0,
          't'                                                                           },
        { "trace-dir",          required_argument,              0,
          'D'                                                                           },
        { "exclude-prefix",     required_argument,              0,
          'X'                                                                           },
        { "block-size",         required_argument,              0,
          'b'                                                                                                         },
        { "max-attr-skip-rate", required_argument,              0,
          'r'                                                                                                                                      },
        { "dry-run",            no_argument,                    0,
          'n'                                                                                                                                                                   },
        { "verbose",            no_argument,                    0,
          'v'                                                                                                                                                                                                },
        { "backend",            required_argument,              0,
          'B'                                                                           },
        { "rdma",               no_argument,                    0,
          'R'                                                                           },
        { 0,                    0,                              0,                             0 },
    };
    char               **traces;
    int                  ntraces            = 0;
    int                  block_size         = 8192;
    double               max_attr_skip_rate = 0.1;
    int                  dry_run            = 0;
    int                  verbose            = 0;
    const char          *backend            = "memfs";
    int                  rdma               = 0;
    int                  failures           = 0;
    int                  c;
    int                  i;
    struct mbt_env       env;

    /* Neutralize the host umask so passthrough backends (linux/io_uring) apply
     * client-sent modes verbatim and the export root keeps its 0777.  The mkfs
     * backends store modes directly and are unaffected. */
    /* Line-buffer stdout so a crash cannot swallow the progress output.
     * These drivers print one line per trace, and both that and chimera's log
     * (which defaults to stdout) are block-buffered when stdout is a pipe --
     * which it always is under ctest.  glibc's abort() does not flush stdio,
     * so on Linux an aborting run loses everything since the last 4 KB
     * boundary, including the line naming the trace that was executing and
     * the fatal log message itself.  That is exactly what made a CI abort
     * here undiagnosable from its artifacts. */
    setvbuf(stdout, NULL, _IOLBF, 0);

    umask(0);

    /* --trace/--trace-dir/--exclude-prefix are gathered from the raw argv by
     * the shared helper; getopt only needs to recognize them so it does not
     * error, and skips them here (the 't'/'D'/'X' cases). */
    traces = mbt_collect_traces(argc, argv, &ntraces);

    while ((c = getopt_long(argc, argv, "t:D:X:b:r:nvB:R", long_options,
                            NULL)) != -1) {
        switch (c) {
            case 't':
            case 'D':
            case 'X':
                break;   /* handled by mbt_collect_traces */
            case 'b':
                block_size = atoi(optarg);
                break;
            case 'r':
                max_attr_skip_rate = atof(optarg);
                break;
            case 'n':
                dry_run = 1;
                break;
            case 'v':
                verbose = 1;
                break;
            case 'B':
                backend = optarg;
                break;
            case 'R':
                rdma = 1;
                break;
            default:
                fprintf(stderr,
                        "usage: %s [--trace FILE ...] [--trace-dir DIR] "
                        "[--block-size N] [--max-attr-skip-rate F] "
                        "[--backend memfs|diskfs|cairn|linux|io_uring] "
                        "[--rdma] [--dry-run] [--verbose]\n", argv[0]);
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
     * every trace; each trace gets a fresh, uniquely-named memfs for
     * isolation.  A single-fs backend (linux/io_uring) would instead clear the
     * backing directory between traces. */
    if (!dry_run) {
        struct mbt_env_opts opts = { .module = backend, .rdma = rdma };
        mbt_env_open_opts(&env, &opts);
    }

    for (i = 0; i < ntraces; i++) {
        char fsname[32];

        snprintf(fsname, sizeof(fsname), "fs_%d", i);
        failures += run_trace(dry_run ? NULL : &env, traces[i], fsname,
                              block_size, max_attr_skip_rate, verbose, dry_run);
    }

    if (!dry_run) {
        mbt_env_stop(&env);
    }

    mbt_free_traces(traces, ntraces);
    return failures ? 1 : 0;
} /* main */
