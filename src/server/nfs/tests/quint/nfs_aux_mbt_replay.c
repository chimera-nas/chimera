// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Replay a Quint-generated ITF trace of the AUXILIARY NFS protocols --
 * portmap/rpcbind, MOUNT, NLM and NSM -- against an in-process chimera
 * server over the libevpl inproc transport.
 *
 * Each state of the trace carries a `lastOp` naming the RPC the model
 * issued and the reply the server must produce (see the nfsaux model in
 * ext/specs/quint/nfsaux).  This harness embeds a chimera server backed by
 * memfs, stands up the namespace the model assumes, then replays every step
 * through the rpc2 client shim in nfs_aux_mbt_common.h and compares.
 *
 * Model-to-wire mapping maintained here:
 *   - model file 0/1     -> the handles of /share/f0 and /share/f1, learned
 *                           by LOOKUP after the harness creates them
 *   - model file -1      -> a deliberately malformed handle, so the
 *                           STALE_FH path is reachable
 *   - model caller "cN"  -> "<trace>-cN": see reset_state() for why the
 *                           name is qualified per trace
 *   - model wireLen 0/-1 -> the two wire spellings of to-EOF, 0 and
 *                           0xffffffffffffffff
 *   - PROC_UNAVAIL       -> the model's RUnavail: the reply a procedure the
 *                           server does not register produces
 *
 * ISOLATION.  The nfs3 suite gets a fresh filesystem per trace and that is
 * enough, because NFSv3 is stateless.  These protocols are not: the lock
 * table, the client table, the NSM monitor list and the NSM state number
 * all live on the server and outlive any filesystem.  Three measures make a
 * trace independent of its predecessors, and reset_state() carries the
 * details.
 */

#include <getopt.h>
#include <jansson.h>

#include "nfs_aux_mbt_common.h"
#include "common/mbt_watchdog.h"
#include "common/mbt_trace_dir.h"

#define AUX_MAX_MISM     16
#define AUX_MISM_LEN     512
#define AUX_HISTORY      10

/* RFC 5531 accept_stat: the RPC ran but the procedure is not registered. */
#define RPC_PROC_UNAVAIL 3

/* The export the per-trace filesystem is published under, plus a second one
 * over the same directory so MNT has an export list to disambiguate and
 * MOUNTPROC3_EXPORT has more than one row to order. */
#define AUX_EXPORT       "/share"
#define AUX_EXPORT2      "/share2"

/* Must match UADDR_HOST in the model's profile. */
#define AUX_UADDR_HOST   "127.0.0.1"

/* Must match MOUNT_HOST: the mount table records the connection's peer
 * address with the port stripped, which under inproc is this literal. */
#define AUX_MOUNT_HOST   "inproc"

/* The model's caller names, in the order their per-trace qualified forms
 * are built.  Every one is FREE_ALL'd at teardown. */
static const char *aux_callers[] = { "c1", "c2", "c3" };
#define AUX_NCALLERS     (int) (sizeof(aux_callers) / sizeof(aux_callers[0]))

struct mism {
    int  n;
    char msg[AUX_MAX_MISM][AUX_MISM_LEN];
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

    if (m->n >= AUX_MAX_MISM) {
        return;
    }
    va_start(ap, fmt);
    vsnprintf(m->msg[m->n], AUX_MISM_LEN, fmt, ap);
    va_end(ap);
    m->n++;
} /* mism_add */

struct hist_ent {
    int   idx;
    char  tag[32];
    char *op_dump;   /* json_dumps of the op record, owned */
};

struct oracle {
    struct mbt_env *env;
    int             verbose;
    int             trace_seq;

    struct mbt_fh   root_fh;
    struct mbt_fh   file_fh[2];
    struct mbt_fh   stale_fh;

    /* The NSM state number is server-global and only ever advances, so a
     * trace cannot assume it starts where the model starts.  Read it once
     * and shift every predicted state by the difference. */
    int32_t         nsm_base;

    /* "<seq>-<name>" for each model caller name. */
    char            caller[AUX_NCALLERS][MBT_AUX_NAME_MAX];

    struct hist_ent history[AUX_HISTORY];
    int             nhist;
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
    return 0;
} /* itf_i64 */

static json_t *
itf_seq(json_t *v)
{
    json_t *inner;

    if (json_is_array(v)) {
        return v;
    }
    if (json_is_object(v)) {
        inner = json_object_get(v, "#set");
        if (inner) {
            return inner;
        }
        inner = json_object_get(v, "#list");
        if (inner) {
            return inner;
        }
    }
    return NULL;
} /* itf_seq */

static int64_t
op_i64(
    json_t     *op,
    const char *key)
{
    return itf_i64(json_object_get(op, key));
} /* op_i64 */

static const char *
op_str(
    json_t     *op,
    const char *key)
{
    json_t *v = json_object_get(op, key);

    return (v && json_is_string(v)) ? json_string_value(v) : "";
} /* op_str */

static int
op_bool(
    json_t     *op,
    const char *key)
{
    json_t *v = json_object_get(op, key);

    return v && json_is_true(v);
} /* op_bool */

/* A tagged sum value: { "tag": ..., "value": ... }. */
static const char *
jf_tag(json_t *v)
{
    json_t *t = v ? json_object_get(v, "tag") : NULL;

    return (t && json_is_string(t)) ? json_string_value(t) : "";
} /* jf_tag */

static json_t *
jf_val(json_t *v)
{
    return v ? json_object_get(v, "value") : NULL;
} /* jf_val */

static json_t *
op_field(
    json_t     *op,
    const char *key)
{
    return json_object_get(op, key);
} /* op_field */

/* ---- model -> wire ------------------------------------------------------- */

/* Model wireLen: a positive value is itself, 0 and -1 are the two wire
 * spellings of to-EOF (l_len == 0 and l_len == 0xffffffffffffffff). */
static uint64_t
wire_len(int64_t model_len)
{
    if (model_len < 0) {
        return MBT_NLM_LEN_EOF;
    }
    return (uint64_t) model_len;
} /* wire_len */

/* The server stores to-EOF as POSIX length 0 and reports it back as the NLM
 * sentinel, so a holder's predicted l_len collapses both spellings. */
static uint64_t
holder_len(int64_t model_len)
{
    if (model_len <= 0) {
        return MBT_NLM_LEN_EOF;
    }
    return (uint64_t) model_len;
} /* holder_len */

static const struct mbt_fh *
fh_of(
    struct oracle *o,
    int64_t        file)
{
    if (file < 0 || file > 1) {
        return &o->stale_fh;
    }
    return &o->file_fh[file];
} /* fh_of */

static const char *
caller_of(
    struct oracle *o,
    const char    *model_name)
{
    int i;

    for (i = 0; i < AUX_NCALLERS; i++) {
        if (strcmp(aux_callers[i], model_name) == 0) {
            return o->caller[i];
        }
    }
    return model_name;
} /* caller_of */

/* The owner handle is a model integer; two bytes carry every value the
 * model uses and keep the wire form small. */
static uint32_t
oh_bytes(
    int64_t  oh,
    uint8_t *buf)
{
    buf[0] = (uint8_t) (oh & 0xff);
    buf[1] = (uint8_t) ((oh >> 8) & 0xff);
    return 2;
} /* oh_bytes */

/* One fixed cookie for every request: the protocol requires the server to
 * echo it, which is itself worth checking. */
static const uint8_t aux_cookie[] = { 0xc0, 0xff, 0xee };
#define AUX_COOKIE_LEN (uint32_t) sizeof(aux_cookie)

/* ---- reply checking ------------------------------------------------------ */

/*
 * Every handler starts here.  `reply` is the model's Reply sum value: when
 * it is RUnavail the server must have refused the call with PROC_UNAVAIL and
 * nothing else is compared; otherwise the call must have run.  Returns 1
 * when the caller should go on to compare the payload.
 */
static int
check_gate(
    struct oracle *o,
    json_t        *reply,
    struct mism   *m)
{
    const char *tag = jf_tag(reply);
    int         err = o->env->res.rpc_err;

    if (strcmp(tag, "RUnavail") == 0) {
        if (err != RPC_PROC_UNAVAIL) {
            mism_add(m, "expected PROC_UNAVAIL (%d), got rpc status %d",
                     RPC_PROC_UNAVAIL, err);
        }
        return 0;
    }

    if (err != 0) {
        mism_add(m, "expected the call to run, got rpc status %d", err);
        return 0;
    }
    return 1;
} /* check_gate */

static void
check_u32(
    struct mism *m,
    const char  *what,
    uint32_t     got,
    uint32_t     want)
{
    if (got != want) {
        mism_add(m, "%s: got %u, want %u", what, got, want);
    }
} /* check_u32 */

static void
check_u64(
    struct mism *m,
    const char  *what,
    uint64_t     got,
    uint64_t     want)
{
    if (got != want) {
        mism_add(m, "%s: got %llu, want %llu", what,
                 (unsigned long long) got, (unsigned long long) want);
    }
} /* check_u64 */

static void
check_str(
    struct mism *m,
    const char  *what,
    const char  *got,
    const char  *want)
{
    if (strcmp(got, want) != 0) {
        mism_add(m, "%s: got '%s', want '%s'", what, got, want);
    }
} /* check_str */

/* The universal address a port is reported under: "<host>.<hi>.<lo>", and
 * the empty string when the service is not registered. */
static void
uaddr_of(
    char    *out,
    size_t   cap,
    uint32_t port)
{
    if (port == 0) {
        out[0] = '\0';
        return;
    }
    snprintf(out, cap, AUX_UADDR_HOST ".%u.%u", port >> 8, port & 0xff);
} /* uaddr_of */

static void
check_cookie(
    struct oracle *o,
    struct mism   *m)
{
    struct mbt_aux_result *r = &mbt_aux(o->env)->r;

    if (r->cookie_len != AUX_COOKIE_LEN ||
        memcmp(r->cookie, aux_cookie, AUX_COOKIE_LEN) != 0) {
        mism_add(m, "cookie not echoed: got %u bytes", r->cookie_len);
    }
} /* check_cookie */

/* ---- portmap / rpcbind --------------------------------------------------- */

typedef void (*op_handler_t)(
    struct oracle *o,
    json_t        *op,
    struct mism   *m);

static void
op_pm_null(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    mbt_pm_null(o->env);
    check_gate(o, op_field(op, "reply"), m);
} /* op_pm_null */

static void
op_pm_getport(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    json_t                *reply = op_field(op, "reply");
    struct mbt_aux_result *r;

    r = mbt_pm_getport(o->env, (uint32_t) op_i64(op, "prog"),
                       (uint32_t) op_i64(op, "vers"),
                       (uint32_t) op_i64(op, "prot"));
    if (!check_gate(o, reply, m)) {
        return;
    }
    check_u32(m, "GETPORT port", r->port,
              (uint32_t) itf_i64(jf_val(reply)));
} /* op_pm_getport */

static void
op_pm_set(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    json_t                *reply = op_field(op, "reply");
    json_t                *map   = op_field(op, "map");
    struct mbt_aux_result *r;

    r = mbt_pm_set(o->env, (uint32_t) op_i64(map, "prog"),
                   (uint32_t) op_i64(map, "vers"),
                   (uint32_t) op_i64(map, "prot"),
                   (uint32_t) op_i64(map, "port"),
                   op_bool(op, "unset"));
    if (!check_gate(o, reply, m)) {
        return;
    }
    check_u32(m, "SET/UNSET result", (uint32_t) r->boolres,
              json_is_true(jf_val(reply)) ? 1u : 0u);
} /* op_pm_set */

/* Compare a reply's mapping list against the model's service table. */
static void
check_maps(
    struct mbt_aux_result *r,
    json_t                *want,
    struct mism           *m)
{
    size_t n = json_array_size(want);
    size_t i;

    if ((size_t) r->nmaps != n) {
        mism_add(m, "DUMP entry count: got %d, want %zu", r->nmaps, n);
        return;
    }
    for (i = 0; i < n; i++) {
        json_t *e = json_array_get(want, i);

        if (r->maps[i].prog != (uint32_t) itf_i64(json_object_get(e, "prog")) ||
            r->maps[i].vers != (uint32_t) itf_i64(json_object_get(e, "vers")) ||
            r->maps[i].prot != (uint32_t) itf_i64(json_object_get(e, "prot")) ||
            r->maps[i].port != (uint32_t) itf_i64(json_object_get(e, "port"))) {
            mism_add(m, "DUMP[%zu]: got %u.%u.%u.%u, want %lld.%lld.%lld.%lld",
                     i, r->maps[i].prog, r->maps[i].vers, r->maps[i].prot,
                     r->maps[i].port,
                     (long long) itf_i64(json_object_get(e, "prog")),
                     (long long) itf_i64(json_object_get(e, "vers")),
                     (long long) itf_i64(json_object_get(e, "prot")),
                     (long long) itf_i64(json_object_get(e, "port")));
        }
    }
} /* check_maps */

static void
op_pm_dump(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    json_t                *reply = op_field(op, "reply");
    struct mbt_aux_result *r     = mbt_pm_dump(o->env);

    if (!check_gate(o, reply, m)) {
        return;
    }
    check_maps(r, jf_val(reply), m);
} /* op_pm_dump */

static void
op_pm_callit(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    mbt_pm_callit(o->env, (uint32_t) op_i64(op, "prog"),
                  (uint32_t) op_i64(op, "vers"),
                  (uint32_t) op_i64(op, "cproc"));
    /* Only the refusal is predictable; a server that implements CALLIT
     * returns the proxied service's own result, which is that protocol's
     * business.  check_gate is the whole check. */
    check_gate(o, op_field(op, "reply"), m);
} /* op_pm_callit */

static const char *
netid_str(json_t *netid)
{
    const char *tag = jf_tag(netid);

    if (strcmp(tag, "NetTcp") == 0) {
        return "tcp";
    }
    if (strcmp(tag, "NetUdp") == 0) {
        return "udp";
    }
    if (strcmp(tag, "NetTcp6") == 0) {
        return "tcp6";
    }
    if (strcmp(tag, "NetUdp6") == 0) {
        return "udp6";
    }
    return "local";
} /* netid_str */

static void
check_uaddr(
    struct mbt_aux_result *r,
    json_t                *reply,
    struct mism           *m)
{
    json_t  *u    = jf_val(reply);
    uint32_t port = (uint32_t) itf_i64(json_object_get(u, "port"));
    char     want[MBT_AUX_UADDR_MAX];

    uaddr_of(want, sizeof(want), port);
    check_str(m, "universal address", r->uaddr, want);
} /* check_uaddr */

static void
op_rb_getaddr(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    json_t                *reply = op_field(op, "reply");
    struct mbt_aux_result *r;

    r = mbt_rb_getaddr(o->env, (int) op_i64(op, "rbvers"),
                       (uint32_t) op_i64(op, "prog"),
                       (uint32_t) op_i64(op, "vers"),
                       netid_str(op_field(op, "netid")));
    if (!check_gate(o, reply, m)) {
        return;
    }
    check_uaddr(r, reply, m);
} /* op_rb_getaddr */

static void
op_rb_getversaddr(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    json_t                *reply = op_field(op, "reply");
    struct mbt_aux_result *r;

    r = mbt_rb_getversaddr(o->env, (uint32_t) op_i64(op, "prog"),
                           (uint32_t) op_i64(op, "vers"),
                           netid_str(op_field(op, "netid")));
    if (!check_gate(o, reply, m)) {
        return;
    }
    check_uaddr(r, reply, m);
} /* op_rb_getversaddr */

static void
op_rb_dump(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    json_t                *reply = op_field(op, "reply");
    struct mbt_aux_result *r;
    json_t                *want;
    size_t                 n, i;

    r = mbt_rb_dump(o->env, (int) op_i64(op, "rbvers"));
    if (!check_gate(o, reply, m)) {
        return;
    }
    want = jf_val(reply);
    n    = json_array_size(want);
    if ((size_t) r->nrpcb != n) {
        mism_add(m, "rpcbind DUMP entry count: got %d, want %zu", r->nrpcb, n);
        return;
    }
    for (i = 0; i < n; i++) {
        json_t  *e    = json_array_get(want, i);
        uint32_t prog = (uint32_t) itf_i64(json_object_get(e, "prog"));
        uint32_t vers = (uint32_t) itf_i64(json_object_get(e, "vers"));
        uint32_t port = (uint32_t) itf_i64(json_object_get(e, "port"));
        char     addr[MBT_AUX_UADDR_MAX];
        char     label[64];

        uaddr_of(addr, sizeof(addr), port);
        snprintf(label, sizeof(label), "rpcbind DUMP[%zu] prog", i);
        check_u32(m, label, r->rpcb[i].prog, prog);
        snprintf(label, sizeof(label), "rpcbind DUMP[%zu] vers", i);
        check_u32(m, label, r->rpcb[i].vers, vers);
        snprintf(label, sizeof(label), "rpcbind DUMP[%zu] addr", i);
        check_str(m, label, r->rpcb[i].addr, addr);
        /* Every row is served over TCP and no ownership is tracked. */
        snprintf(label, sizeof(label), "rpcbind DUMP[%zu] netid", i);
        check_str(m, label, r->rpcb[i].netid, "tcp");
        snprintf(label, sizeof(label), "rpcbind DUMP[%zu] owner", i);
        check_str(m, label, r->rpcb[i].owner, "");
    }
} /* op_rb_dump */

static void
op_rb_gettime(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    mbt_rb_gettime(o->env, (int) op_i64(op, "rbvers"));
    /* The reply is a wall clock; only the accept_stat is an oracle. */
    check_gate(o, op_field(op, "reply"), m);
} /* op_rb_gettime */

/* ---- MOUNT --------------------------------------------------------------- */

static void
op_mnt_null(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    mbt_mount_null(o->env);
    check_gate(o, op_field(op, "reply"), m);
} /* op_mnt_null */

static void
op_mnt(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    json_t                *reply = op_field(op, "reply");
    const char            *path  = op_str(op, "path");
    const char            *tag;
    struct mbt_aux_result *r;
    json_t                *want;
    size_t                 n, i;

    r = mbt_mount_mnt(o->env, path);
    if (!check_gate(o, reply, m)) {
        return;
    }

    tag = jf_tag(reply);
    if (strcmp(tag, "RMntErr") == 0) {
        check_u32(m, "MNT status", o->env->res.status,
                  (uint32_t) itf_i64(jf_val(reply)));
        return;
    }

    check_u32(m, "MNT status", o->env->res.status, MNT3_OK);
    if (o->env->res.status != MNT3_OK) {
        return;
    }
    if (!o->env->res.obj_fh.has) {
        mism_add(m, "MNT succeeded without a file handle");
    }

    want = jf_val(reply);
    n    = json_array_size(want);
    if ((size_t) r->nauth != n) {
        mism_add(m, "MNT auth flavor count: got %d, want %zu", r->nauth, n);
        return;
    }
    for (i = 0; i < n; i++) {
        char label[48];

        snprintf(label, sizeof(label), "MNT auth flavor[%zu]", i);
        check_u32(m, label, (uint32_t) r->auth[i],
                  (uint32_t) itf_i64(json_array_get(want, i)));
    }
} /* op_mnt */

static void
op_mnt_dump(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    json_t                *reply = op_field(op, "reply");
    struct mbt_aux_result *r     = mbt_mount_dump(o->env);
    json_t                *want;
    size_t                 n, i;

    if (!check_gate(o, reply, m)) {
        return;
    }
    want = jf_val(reply);
    n    = json_array_size(want);
    if ((size_t) r->nmounts != n) {
        mism_add(m, "mount table size: got %d, want %zu", r->nmounts, n);
        return;
    }
    for (i = 0; i < n; i++) {
        json_t *e = json_array_get(want, i);
        char    label[64];

        snprintf(label, sizeof(label), "mount table[%zu] host", i);
        check_str(m, label, r->mounts[i].host, AUX_MOUNT_HOST);
        snprintf(label, sizeof(label), "mount table[%zu] dir", i);
        check_str(m, label, r->mounts[i].dir,
                  json_string_value(json_object_get(e, "dir")) ?: "");
    }
} /* op_mnt_dump */

static void
op_mnt_umnt(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    mbt_mount_umnt(o->env, op_str(op, "path"));
    check_gate(o, op_field(op, "reply"), m);
} /* op_mnt_umnt */

static void
op_mnt_umntall(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    mbt_mount_umntall(o->env);
    check_gate(o, op_field(op, "reply"), m);
} /* op_mnt_umntall */

static void
op_mnt_export(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    json_t                *reply = op_field(op, "reply");
    struct mbt_aux_result *r     = mbt_mount_export(o->env);
    json_t                *want;
    size_t                 n, i;

    if (!check_gate(o, reply, m)) {
        return;
    }
    want = jf_val(reply);
    n    = json_array_size(want);
    if ((size_t) r->nexports != n) {
        mism_add(m, "export list size: got %d, want %zu", r->nexports, n);
        return;
    }
    for (i = 0; i < n; i++) {
        char label[48];

        snprintf(label, sizeof(label), "export[%zu]", i);
        check_str(m, label, r->exports[i].dir,
                  json_string_value(json_array_get(want, i)) ?: "");
        if (r->exports[i].ngroups != 0) {
            mism_add(m, "export[%zu] carries %d group(s); none expected", i,
                     r->exports[i].ngroups);
        }
    }
} /* op_mnt_export */

/* ---- NLM ----------------------------------------------------------------- */

/* Decode a model Lock record into the wire arguments. */
struct aux_lock {
    const struct mbt_fh *fh;
    const char          *caller;
    uint8_t              oh[4];
    uint32_t             oh_len;
    int32_t              svid;
    uint64_t             offset;
    uint64_t             length;
    int                  excl;
};

static void
decode_lock(
    struct oracle   *o,
    json_t          *jl,
    struct aux_lock *out)
{
    json_t *owner = json_object_get(jl, "owner");

    out->fh     = fh_of(o, op_i64(jl, "file"));
    out->caller = caller_of(o, op_str(owner, "caller"));
    out->oh_len = oh_bytes(op_i64(owner, "oh"), out->oh);
    out->svid   = (int32_t) op_i64(owner, "svid");
    out->offset = (uint64_t) op_i64(jl, "offset");
    out->length = wire_len(op_i64(jl, "wireLen"));
    out->excl   = op_bool(jl, "excl");
} /* decode_lock */

/*
 * An NLM request can provoke messages the server sends BACK to the client:
 * an NLMPROC4_*_RES reporting the outcome of an asynchronous request.  The
 * model's `asyncs` list says exactly which, and it is not always one --
 * cancelling a queued NLMPROC4_LOCK_MSG produces both the CANCEL_RES that
 * answers the cancel and the LOCK_RES that retires the blocked LOCK.
 *
 * The two are not ordered with respect to each other, so the comparison is
 * a multiset match: each expected (proc, status) has to be consumed by a
 * distinct received message, and nothing may be left over.
 */
static void
check_asyncs(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    struct mbt_aux *a      = mbt_aux(o->env);
    json_t         *asyncs = itf_seq(op_field(op, "asyncs"));
    size_t          want   = asyncs ? json_array_size(asyncs) : 0;
    int             taken[MBT_AUX_MAX_ASYNC];
    size_t          i;
    int             j;

    if (want == 0) {
        /* Wait out a short window rather than declaring success at once, so
         * a server that answers when it should not is caught instead of
         * being silently tolerated. */
        mbt_aux_drain_us(o->env, 50000);
        if (a->nasync != 0) {
            mism_add(m, "expected no asynchronous message, got %d (proc %d)",
                     a->nasync, a->async[0].proc);
        }
        return;
    }

    mbt_aux_drain_for(o->env, (int) want, 2000000);
    if ((size_t) a->nasync != want) {
        mism_add(m, "expected %zu asynchronous message(s), got %d", want,
                 a->nasync);
        return;
    }

    memset(taken, 0, sizeof(taken));
    for (i = 0; i < want; i++) {
        json_t  *e     = json_array_get(asyncs, i);
        int      proc  = (int) itf_i64(json_object_get(e, "proc"));
        uint32_t stat  = (uint32_t) itf_i64(json_object_get(e, "stat"));
        int      found = 0;

        for (j = 0; j < a->nasync; j++) {
            if (taken[j] || a->async[j].proc != proc ||
                a->async[j].stat != stat) {
                continue;
            }
            taken[j] = 1;
            found    = 1;
            if (a->async[j].cookie_len != AUX_COOKIE_LEN ||
                memcmp(a->async[j].cookie, aux_cookie, AUX_COOKIE_LEN) != 0) {
                mism_add(m, "asynchronous message (proc %d) did not echo the "
                         "cookie", proc);
            }
            break;
        }
        if (!found) {
            mism_add(m, "no asynchronous message with proc %d status %u "
                     "arrived", proc, stat);
        }
    }
} /* check_asyncs */

/* The synchronous shape: an nlm4_res carrying a status and the cookie. */
static void
check_nlm_res(
    struct oracle *o,
    json_t        *reply,
    struct mism   *m)
{
    struct mbt_aux_result *r = &mbt_aux(o->env)->r;

    check_u32(m, "NLM status", r->nlm_stat,
              (uint32_t) itf_i64(jf_val(reply)));
    check_cookie(o, m);
} /* check_nlm_res */

static void
op_nlm_null(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    mbt_nlm_null(o->env);
    check_gate(o, op_field(op, "reply"), m);
} /* op_nlm_null */

static void
op_nlm_test(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    json_t                *reply = op_field(op, "reply");
    int                    msg   = op_bool(op, "msg");
    struct aux_lock        l;
    struct mbt_aux_result *r;
    json_t                *v, *holders;
    uint32_t               want_stat;
    size_t                 n, i;
    int                    matched;

    decode_lock(o, op_field(op, "lock"), &l);
    if (msg) {
    }
    r = mbt_nlm_test(o->env, msg, l.caller, l.fh, l.oh, l.oh_len, l.svid,
                     l.excl, l.offset, l.length, aux_cookie, AUX_COOKIE_LEN);
    if (!check_gate(o, reply, m)) {
        return;
    }
    if (msg) {
        check_asyncs(o, op, m);
        return;
    }

    v         = jf_val(reply);
    want_stat = (uint32_t) itf_i64(json_object_get(v, "stat"));
    check_u32(m, "TEST status", r->nlm_stat, want_stat);
    check_cookie(o, m);

    if (r->nlm_stat != NLM4_DENIED || want_stat != NLM4_DENIED) {
        return;
    }

    /*
     * Which conflicting holder a server names when several conflict is not
     * determined by the protocol, so the model reports the candidates and
     * the check is membership.  The holder's owner identity is separately
     * pinned: a server whose arbitration does not retain the requester
     * reports zero for both svid and oh, which is what this profile expects.
     */
    holders = itf_seq(json_object_get(v, "holders"));
    n       = holders ? json_array_size(holders) : 0;
    matched = 0;
    for (i = 0; i < n; i++) {
        json_t  *h    = json_array_get(holders, i);
        uint64_t off  = (uint64_t) op_i64(h, "offset");
        uint64_t len  = holder_len(op_i64(h, "wireLen"));
        int      excl = op_bool(h, "excl");

        if (r->holder_offset == off && r->holder_length == len &&
            r->holder_exclusive == excl) {
            matched = 1;
            break;
        }
    }
    if (!matched) {
        mism_add(m, "TEST holder [%llu,%llu) excl=%d matches none of the %zu "
                 "conflicting locks the model holds",
                 (unsigned long long) r->holder_offset,
                 (unsigned long long) r->holder_length,
                 r->holder_exclusive, n);
    }
    check_u32(m, "TEST holder svid", (uint32_t) r->holder_svid, 0);
    check_u32(m, "TEST holder oh length", r->holder_oh_len, 0);
} /* op_nlm_test */

static void
op_nlm_lock(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    json_t         *reply = op_field(op, "reply");
    int             nproc = (int) op_i64(op, "nproc");
    struct aux_lock l;

    decode_lock(o, op_field(op, "lock"), &l);
    if (nproc == 7) {
    }
    mbt_nlm_lock(o->env, nproc, l.caller, l.fh, l.oh, l.oh_len, l.svid,
                 l.excl, op_bool(op, "block"), op_bool(op, "reclaim"), 0,
                 l.offset, l.length, aux_cookie, AUX_COOKIE_LEN);
    if (!check_gate(o, reply, m)) {
        return;
    }
    if (nproc == 7) {
        check_asyncs(o, op, m);
        return;
    }
    check_nlm_res(o, reply, m);
} /* op_nlm_lock */

static void
op_nlm_cancel(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    json_t         *reply = op_field(op, "reply");
    int             msg   = op_bool(op, "msg");
    struct aux_lock l;

    decode_lock(o, op_field(op, "lock"), &l);
    mbt_nlm_cancel(o->env, msg, l.caller, l.fh, l.oh, l.oh_len, l.svid,
                   l.excl, 1, l.offset, l.length, aux_cookie,
                   AUX_COOKIE_LEN);
    if (!check_gate(o, reply, m)) {
        return;
    }
    if (!msg) {
        check_nlm_res(o, reply, m);
    }
    check_asyncs(o, op, m);
} /* op_nlm_cancel */

static void
op_nlm_unlock(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    json_t         *reply = op_field(op, "reply");
    int             msg   = op_bool(op, "msg");
    struct aux_lock l;

    decode_lock(o, op_field(op, "lock"), &l);
    if (msg) {
    }
    mbt_nlm_unlock(o->env, msg, l.caller, l.fh, l.oh, l.oh_len, l.svid,
                   l.offset, l.length, aux_cookie, AUX_COOKIE_LEN);
    if (!check_gate(o, reply, m)) {
        return;
    }
    if (msg) {
        check_asyncs(o, op, m);
        return;
    }
    check_nlm_res(o, reply, m);
} /* op_nlm_unlock */

static void
op_nlm_granted(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    json_t         *reply = op_field(op, "reply");
    int             msg   = op_bool(op, "msg");
    struct aux_lock l;

    decode_lock(o, op_field(op, "lock"), &l);
    if (msg) {
    }
    mbt_nlm_granted(o->env, msg, l.caller, l.fh, l.oh, l.oh_len, l.svid,
                    l.excl, l.offset, l.length, aux_cookie, AUX_COOKIE_LEN);
    if (!check_gate(o, reply, m)) {
        return;
    }
    if (msg) {
        check_asyncs(o, op, m);
        return;
    }
    check_nlm_res(o, reply, m);
} /* op_nlm_granted */

static void
op_nlm_res(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    mbt_nlm_send_res(o->env, (int) op_i64(op, "nproc"),
                     (uint32_t) op_i64(op, "stat"), aux_cookie,
                     AUX_COOKIE_LEN);
    check_gate(o, op_field(op, "reply"), m);
} /* op_nlm_res */

static void
op_nlm_reserved(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    mbt_nlm_reserved(o->env, (int) op_i64(op, "nproc"));
    check_gate(o, op_field(op, "reply"), m);
} /* op_nlm_reserved */

static void
op_nlm_share(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    json_t                *reply = op_field(op, "reply");
    json_t                *sh    = op_field(op, "share");
    struct mbt_aux_result *r;
    uint8_t                oh[4];
    uint32_t               oh_len = oh_bytes(op_i64(sh, "oh"), oh);
    json_t                *v;

    r = mbt_nlm_share(o->env, op_bool(op, "unshare"),
                      caller_of(o, op_str(sh, "caller")),
                      fh_of(o, op_i64(sh, "file")), oh, oh_len,
                      (int) op_i64(sh, "mode"), (int) op_i64(sh, "access"),
                      0, aux_cookie, AUX_COOKIE_LEN);
    if (!check_gate(o, reply, m)) {
        return;
    }
    v = jf_val(reply);
    check_u32(m, "SHARE status", r->nlm_stat,
              (uint32_t) itf_i64(json_object_get(v, "stat")));
    check_u32(m, "SHARE sequence", (uint32_t) r->sequence,
              (uint32_t) itf_i64(json_object_get(v, "sequence")));
    check_cookie(o, m);
} /* op_nlm_share */

static void
op_nlm_free_all(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    mbt_nlm_free_all(o->env, caller_of(o, op_str(op, "name")), 1);
    check_gate(o, op_field(op, "reply"), m);
} /* op_nlm_free_all */

/* ---- NSM ----------------------------------------------------------------- */

/* The model predicts a state number relative to 1; the live server's
 * counter is global and only advances, so shift by the base read at trace
 * start. */
static void
check_sm_state(
    struct oracle *o,
    struct mism   *m,
    int32_t        got,
    int64_t        want_model)
{
    int32_t want = (int32_t) (want_model - 1) + o->nsm_base;

    if (got != want) {
        mism_add(m, "NSM state number: got %d, want %d (model %lld, base %d)",
                 got, want, (long long) want_model, o->nsm_base);
    }
} /* check_sm_state */

static void
op_sm_null(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    mbt_sm_null(o->env);
    check_gate(o, op_field(op, "reply"), m);
} /* op_sm_null */

static void
op_sm_stat(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    json_t                *reply = op_field(op, "reply");
    struct mbt_aux_result *r;
    json_t                *v;

    r = mbt_sm_stat(o->env, caller_of(o, op_str(op, "host")));
    if (!check_gate(o, reply, m)) {
        return;
    }
    v = jf_val(reply);
    check_u32(m, "SM_STAT res", (uint32_t) r->sm_res,
              (uint32_t) itf_i64(json_object_get(v, "res")));
    check_sm_state(o, m, r->sm_state, itf_i64(json_object_get(v, "state")));
} /* op_sm_stat */

static void
op_sm_mon(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    json_t                *reply = op_field(op, "reply");
    struct mbt_aux_result *r;
    json_t                *v;

    r = mbt_sm_mon(o->env, caller_of(o, op_str(op, "host")), "quintmbt",
                   100021, 4, 24);
    if (!check_gate(o, reply, m)) {
        return;
    }
    v = jf_val(reply);
    check_u32(m, "SM_MON res", (uint32_t) r->sm_res,
              (uint32_t) itf_i64(json_object_get(v, "res")));
    check_sm_state(o, m, r->sm_state, itf_i64(json_object_get(v, "state")));
} /* op_sm_mon */

static void
op_sm_unmon(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    json_t                *reply = op_field(op, "reply");
    struct mbt_aux_result *r;

    r = mbt_sm_unmon(o->env, caller_of(o, op_str(op, "host")), "quintmbt");
    if (!check_gate(o, reply, m)) {
        return;
    }
    check_sm_state(o, m, r->sm_state, itf_i64(jf_val(reply)));
} /* op_sm_unmon */

static void
op_sm_unmon_all(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    json_t                *reply = op_field(op, "reply");
    struct mbt_aux_result *r;

    r = mbt_sm_unmon_all(o->env, caller_of(o, op_str(op, "name")));
    if (!check_gate(o, reply, m)) {
        return;
    }
    check_sm_state(o, m, r->sm_state, itf_i64(jf_val(reply)));
} /* op_sm_unmon_all */

static void
op_sm_simu_crash(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    mbt_sm_simu_crash(o->env);
    check_gate(o, op_field(op, "reply"), m);
} /* op_sm_simu_crash */

static void
op_sm_notify(
    struct oracle *o,
    json_t        *op,
    struct mism   *m)
{
    mbt_sm_notify(o->env, caller_of(o, op_str(op, "host")),
                  (int32_t) op_i64(op, "state"));
    check_gate(o, op_field(op, "reply"), m);
} /* op_sm_notify */

/* ---- dispatch ------------------------------------------------------------ */

/* *INDENT-OFF* -- uncrustify oscillates on this table's alignment. */
static const struct {
    const char  *tag;
    op_handler_t fn;
} handlers[] = {
    { "OPmNull",        op_pm_null         },
    { "OPmGetport",     op_pm_getport      },
    { "OPmSet",         op_pm_set          },
    { "OPmDump",        op_pm_dump         },
    { "OPmCallit",      op_pm_callit       },
    { "ORbGetaddr",     op_rb_getaddr      },
    { "ORbDump",        op_rb_dump         },
    { "ORbGettime",     op_rb_gettime      },
    { "ORbGetversaddr", op_rb_getversaddr  },
    { "OMntNull",       op_mnt_null        },
    { "OMnt",           op_mnt             },
    { "OMntDump",       op_mnt_dump        },
    { "OMntUmnt",       op_mnt_umnt        },
    { "OMntUmntAll",    op_mnt_umntall     },
    { "OMntExport",     op_mnt_export      },
    { "ONlmNull",       op_nlm_null        },
    { "ONlmTestOp",     op_nlm_test        },
    { "ONlmLock",       op_nlm_lock        },
    { "ONlmCancel",     op_nlm_cancel      },
    { "ONlmUnlock",     op_nlm_unlock      },
    { "ONlmGranted",    op_nlm_granted     },
    { "ONlmRes",        op_nlm_res         },
    { "ONlmReserved",   op_nlm_reserved    },
    { "ONlmShare",      op_nlm_share       },
    { "ONlmFreeAll",    op_nlm_free_all    },
    { "OSmNull",        op_sm_null         },
    { "OSmStat",        op_sm_stat         },
    { "OSmMon",         op_sm_mon          },
    { "OSmUnmon",       op_sm_unmon        },
    { "OSmUnmonAll",    op_sm_unmon_all    },
    { "OSmSimuCrash",   op_sm_simu_crash   },
    { "OSmNotify",      op_sm_notify       },
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

/* ---- per-trace setup / teardown ------------------------------------------ */

/*
 * Build the namespace and the identity space each trace assumes.
 *
 * The filesystem is fresh (a uniquely named memfs, mounted and exported by
 * the caller), but the LOCK MANAGER and STATUS MONITOR are not: their
 * tables are server-global and outlive any filesystem.  Three measures make
 * a trace independent of its predecessors:
 *
 *   1. Caller names are qualified with the trace's sequence number, so the
 *      NLM client table and the NSM monitor list start empty for the names
 *      this trace uses.  That matters beyond tidiness: a server keeps a
 *      client's table entry after its last lock is released, and SM_NOTIFY
 *      takes a different path for a name it has seen before.
 *   2. Every caller is FREE_ALL'd at teardown, which releases its locks and
 *      unmonitors it.  Without that a leftover lock would hold an open
 *      handle on the filesystem and rmfs would never succeed.
 *   3. The NSM state number only ever advances, so the trace records the
 *      value it starts at and every prediction is shifted by it.
 *
 * The mount table IS reset directly (UMNTALL), because it is keyed by the
 * client's address, which every trace shares.
 */
static int
trace_setup(
    struct oracle *o,
    const char    *trace_path)
{
    struct mbt_env        *env = o->env;
    struct mbt_result     *res;
    struct mbt_aux_result *ar;
    int                    i;
    char                   name[8];

    for (i = 0; i < AUX_NCALLERS; i++) {
        snprintf(o->caller[i], sizeof(o->caller[i]), "t%d-%s", o->trace_seq,
                 aux_callers[i]);
    }

    if (chimera_server_create_export(env->server, AUX_EXPORT2, AUX_EXPORT, 0,
                                     NULL) != 0) {
        fprintf(stderr, "%s: failed to create the %s export\n", trace_path,
                AUX_EXPORT2);
        return 1;
    }

    res = mbt_mnt(env, AUX_EXPORT);
    if (res->rpc_err != 0 || res->status != MNT3_OK || !res->obj_fh.has) {
        fprintf(stderr, "%s: MNT %s failed: rpc_err=%d status=%u\n",
                trace_path, AUX_EXPORT, res->rpc_err, res->status);
        return 1;
    }
    o->root_fh = res->obj_fh;

    /* The objects the model's path universe and file universe name. */
    mbt_mkdir(env, &o->root_fh, "d", 1, 0755);
    for (i = 0; i < 2; i++) {
        snprintf(name, sizeof(name), "f%d", i);
        mbt_create(env, &o->root_fh, name, (uint32_t) strlen(name), UNCHECKED,
                   0644, NULL);
        res = mbt_lookup(env, &o->root_fh, name, (uint32_t) strlen(name));
        if (res->status != 0 || !res->obj_fh.has) {
            fprintf(stderr, "%s: LOOKUP %s failed: %u\n", trace_path, name,
                    res->status);
            return 1;
        }
        o->file_fh[i] = res->obj_fh;
    }

    /* A handle of the right shape that decodes to nothing. */
    memset(&o->stale_fh, 0, sizeof(o->stale_fh));
    o->stale_fh.has = 1;
    o->stale_fh.len = 16;
    memset(o->stale_fh.data, 0x5a, o->stale_fh.len);

    /* The MNT above left a row behind; the model starts with none. */
    mbt_mount_umntall(env);

    ar = mbt_sm_stat(env, "quintmbt");
    if (env->res.rpc_err != 0) {
        fprintf(stderr, "%s: SM_STAT failed: %d\n", trace_path,
                env->res.rpc_err);
        return 1;
    }
    o->nsm_base = ar->sm_state;

    mbt_aux_async_reset(env);
    return 0;
} /* trace_setup */

static void
trace_teardown(struct oracle *o)
{
    int i;

    for (i = 0; i < AUX_NCALLERS; i++) {
        mbt_nlm_free_all(o->env, o->caller[i], 1);
    }
    mbt_mount_umntall(o->env);
    chimera_server_remove_export(o->env->server, AUX_EXPORT2);
} /* trace_teardown */

/* ---- replay driver -------------------------------------------------------- */

static void
history_push(
    struct oracle *o,
    int            idx,
    const char    *tag,
    json_t        *op)
{
    struct hist_ent *e;

    if (o->nhist == AUX_HISTORY) {
        free(o->history[0].op_dump);
        memmove(&o->history[0], &o->history[1],
                sizeof(o->history[0]) * (AUX_HISTORY - 1));
        o->nhist--;
    }
    e      = &o->history[o->nhist++];
    e->idx = idx;
    snprintf(e->tag, sizeof(e->tag), "%s", tag);
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
    char *dump;
    int   i;

    fprintf(stderr, "\n=== DIVERGENCE in %s ===\n", trace_path);
    dump = json_dumps(op, JSON_COMPACT | JSON_ENCODE_ANY);
    fprintf(stderr, "step %d: %s args/expectation: %s\n", step, tag,
            dump ?: "<?>");
    free(dump);
    for (i = 0; i < m->n; i++) {
        fprintf(stderr, "  MISMATCH: %s\n", m->msg[i]);
    }
    fprintf(stderr, "\nlast operations before failure:\n");
    for (i = 0; i < o->nhist; i++) {
        fprintf(stderr, "  [%4d] %s %s\n", o->history[i].idx,
                o->history[i].tag, o->history[i].op_dump ?: "<?>");
    }
} /* report_divergence */

/*
 * Per-step state cross-check (--paranoid).
 *
 * Comparing replies alone localizes a divergence only when it happens to be
 * visible in the very next reply.  A lock the server did not grant (or did
 * not release) is invisible until something later collides with it, by which
 * point the trace has moved on and the report names the wrong step.
 *
 * This walks the model's own `held` list -- the ITF carries it -- and asks
 * the server, with a TEST from an owner that appears nowhere in the trace,
 * whether each file's ranges are free.  The predicted answer is computed
 * here from the model's held set with the same rule the model uses, so a
 * mismatch pins the exact step at which the two lock tables parted.
 *
 * It costs an RPC per probe range per step, so it is off by default and
 * exists for narrowing a divergence rather than for the routine run.
 */
static const uint64_t paranoid_offsets[] = { 0, 8, 16 };
static const uint64_t paranoid_lengths[] = { 8, 16, MBT_NLM_LEN_EOF };

static int
model_conflicts(
    json_t  *held,
    int64_t  file,
    uint64_t off,
    uint64_t len)
{
    size_t n = held ? json_array_size(held) : 0;
    size_t i;

    /* The probe is exclusive, so any overlapping lock conflicts.  Lengths
     * are compared in the POSIX convention the model stores: 0 (and the
     * model's -1 spelling) mean to-EOF. */
    for (i = 0; i < n; i++) {
        json_t  *l = json_array_get(held, i);
        int64_t  hlen;
        uint64_t hoff, hend, pend;

        if (op_i64(l, "file") != file) {
            continue;
        }
        hoff = (uint64_t) op_i64(l, "offset");
        hlen = op_i64(l, "wireLen");
        hend = (hlen <= 0) ? UINT64_MAX : hoff + (uint64_t) hlen;
        pend = (len == MBT_NLM_LEN_EOF) ? UINT64_MAX : off + len;
        if (hoff < pend && off < hend) {
            return 1;
        }
    }
    return 0;
} /* model_conflicts */

static void
check_state(
    struct oracle *o,
    json_t        *state,
    struct mism   *m)
{
    static const uint8_t probe_oh[] = { 0xff, 0xff };
    json_t              *held       = NULL;
    const char          *k;
    json_t              *v;
    size_t               fi, oi, li;

    json_object_foreach(state, k, v)
    {
        const char *base = strrchr(k, ':');

        if (strcmp(base ? base + 1 : k, "held") == 0) {
            held = itf_seq(v);
        }
    }

    for (fi = 0; fi < 2; fi++) {
        for (oi = 0; oi < sizeof(paranoid_offsets) / sizeof(uint64_t); oi++) {
            for (li = 0; li < sizeof(paranoid_lengths) / sizeof(uint64_t);
                 li++) {
                uint64_t               off  = paranoid_offsets[oi];
                uint64_t               len  = paranoid_lengths[li];
                int                    want = model_conflicts(held, (int64_t) fi, off, len);
                struct mbt_aux_result *r;

                r = mbt_nlm_test(o->env, 0, "probe-owner", &o->file_fh[fi],
                                 probe_oh, sizeof(probe_oh), 0x7fff, 1, off,
                                 len, aux_cookie, AUX_COOKIE_LEN);
                if ((r->nlm_stat == NLM4_DENIED) != want) {
                    mism_add(m, "state check: f%zu [%llu,%llu) is %s on the "
                             "server but %s in the model", fi,
                             (unsigned long long) off,
                             (unsigned long long) len,
                             r->nlm_stat == NLM4_DENIED ? "held" : "free",
                             want ? "held" : "free");
                }
            }
        }
    }
} /* check_state */

/* Each state's lastOp key is module-qualified by the instance name. */
static json_t *
state_lastop(json_t *state)
{
    const char *k;
    json_t     *v;

    json_object_foreach(state, k, v)
    {
        const char *base = strrchr(k, ':');

        if (strcmp(base ? base + 1 : k, "lastOp") == 0) {
            return v;
        }
    }
    return NULL;
} /* state_lastop */

static int paranoid;

static int
run_trace(
    struct mbt_env *env,
    const char     *trace_path,
    const char     *fsname,
    int             trace_seq,
    int             verbose,
    int             dry_run)
{
    json_error_t   jerr;
    json_t        *root;
    json_t        *states;
    json_t        *state;
    json_t        *last_op;
    json_t        *op;
    const char    *tag;
    op_handler_t   fn;
    struct oracle *o;
    size_t         idx, nstates;
    int            failed = 0;
    struct mism    m;

    root = json_load_file(trace_path, 0, &jerr);
    if (!root) {
        fprintf(stderr, "%s: JSON parse error: %s (line %d)\n", trace_path,
                jerr.text, jerr.line);
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

    /* Backstop for a server deadlock: with everything in one process a hung
     * reply spins in mbt_call_wait forever.  The watchdog names the trace and
     * step it caught before letting SIGALRM kill the test, so a hang in CI
     * arrives with the position attached rather than as a bare signal. */
    mbt_watchdog_arm(180);
    mbt_watchdog_at(trace_path, -1, "setup");

    o = calloc(1, sizeof(*o));
    /* The aux harness asserts against a fixed "/share" export -- its MNT
     * cases include deliberate near-misses like "/shareXX" -- so it mounts
     * under that name rather than the per-trace one the batch replayers
     * use.  The filesystem itself is still per-trace. */
    mbt_env_fs_setup_as(env, fsname, "share");

    o->env       = env;
    o->verbose   = verbose;
    o->trace_seq = trace_seq;

    if (trace_setup(o, trace_path)) {
        failed = 1;
        goto out;
    }

    for (idx = 1; idx < nstates; idx++) {
        state   = json_array_get(states, idx);
        last_op = state_lastop(state);
        if (!last_op) {
            fprintf(stderr, "%s: state %zu has no lastOp\n", trace_path, idx);
            failed = 1;
            goto out;
        }
        tag = jf_tag(last_op);
        op  = jf_val(last_op);
        fn  = find_handler(tag);
        if (!fn) {
            fprintf(stderr, "%s: step %zu: no handler for %s\n", trace_path,
                    idx, tag);
            failed = 1;
            goto out;
        }

        memset(&m, 0, sizeof(m));
        /* The asynchronous log is cleared once per step, here rather than in
         * each handler, so every op is judged only on what its own call
         * provoked. */
        mbt_watchdog_at(trace_path, (int) idx, tag);
        mbt_aux_async_reset(env);
        fn(o, op, &m);
        if (!m.n && paranoid) {
            check_state(o, state, &m);
        }
        history_push(o, (int) idx, tag, op);
        if (verbose) {
            printf("  [%4zu] %s\n", idx, tag);
        }
        if (m.n) {
            report_divergence(trace_path, o, (int) idx, tag, op, &m);
            failed = 1;
            goto out;
        }
    }

    printf("%s: %zu steps replayed\n", trace_path, nstates - 1);

 out:
    trace_teardown(o);
    mbt_env_fs_teardown_as(env, fsname, "share");
    while (o->nhist) {
        free(o->history[--o->nhist].op_dump);
    }
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
        { "trace",          required_argument,          0,
          't'                                                                   },
        { "trace-dir",      required_argument,          0,
          'D'                                                                                             },
        { "exclude-prefix", required_argument,          0,
          'X'                                                                                                                      },
        { "dry-run",        no_argument,                0,
          'n'                                                                                                                                               },
        { "verbose",        no_argument,                0,
          'v'                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        },
        { "paranoid",       no_argument,                0,
          'p'                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 },
        { "backend",        required_argument,          0,
          'B'                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          },
        { "rdma",           no_argument,                0,
          'R'                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          },
        { 0,                0,                          0,                         0 },
    };
    char               **traces;
    int                  ntraces  = 0;
    int                  dry_run  = 0;
    int                  verbose  = 0;
    const char          *backend  = "memfs";
    int                  rdma     = 0;
    int                  failures = 0;
    int                  c, i;
    struct mbt_env       env;
    struct mbt_env_opts  opts;

    /* Line-buffer stdout so a crash cannot swallow the progress output.
     * These drivers print one line per trace, and both that and chimera's log
     * (which defaults to stdout) are block-buffered when stdout is a pipe --
     * which it always is under ctest.  glibc's abort() does not flush stdio,
     * so on Linux an aborting run loses everything since the last 4 KB
     * boundary, including the line naming the trace that was executing and
     * the fatal log message itself.  That is exactly what made a CI abort
     * here undiagnosable from its artifacts. */
    setvbuf(stdout, NULL, _IOLBF, 0);

    traces = mbt_collect_traces(argc, argv, &ntraces);

    while ((c = getopt_long(argc, argv, "t:D:X:nvpB:R", long_options,
                            NULL)) != -1) {
        switch (c) {
            case 't':
            case 'D':
            case 'X':
                break;   /* handled by mbt_collect_traces */
            case 'n':
                dry_run = 1;
                break;
            case 'v':
                verbose = 1;
                break;
            case 'p':
                paranoid = 1;
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
                        "[--backend memfs|diskfs|cairn|linux|io_uring] "
                        "[--rdma] [--dry-run] "
                        "[--verbose] [--paranoid]\n", argv[0]);
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

    if (!dry_run) {
        memset(&opts, 0, sizeof(opts));
        opts.module = backend;
        opts.rdma   = rdma;
        /* The universal addresses rpcbind reports are built from this rather
         * than from the connection's local address, which under inproc is a
         * service name rather than an IP. */
        opts.portmap_hostname = AUX_UADDR_HOST;
        /* Exact attribute comparison cannot tolerate a stale cache, and the
         * per-trace namespace setup re-creates the same names each time. */
        opts.disable_caches = 1;
        mbt_aux_env_open(&env, &opts);
    }

    for (i = 0; i < ntraces; i++) {
        char fsname[32];

        snprintf(fsname, sizeof(fsname), "fs_%d", i);
        failures += run_trace(dry_run ? NULL : &env, traces[i], fsname, i,
                              verbose, dry_run);
    }

    if (!dry_run) {
        mbt_env_stop(&env);
    }

    mbt_free_traces(traces, ntraces);
    return failures ? 1 : 0;
} /* main */
