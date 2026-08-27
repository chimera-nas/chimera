// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * Client half of the auxiliary-NFS model-based test harness: the RPC
 * wrappers for the four protocols that surround NFSv3 -- portmap/rpcbind
 * (100000 v2/v3/v4), MOUNT (100005 v3), NLM (100021 v4) and NSM
 * (100024 v1).
 *
 * It layers on nfs3_mbt_common.h, which owns the embedded server and the
 * rpc2 client thread; setting mbt_env_opts.aux makes that header connect
 * the three extra inproc services (111 / 32803 / 32765) and export NLM_V4
 * on the lock-manager connection so the server can call NLMPROC4_*_RES
 * back at us.
 *
 * Every wrapper follows the nfs3 harness's shape: fire the generated
 * send_call_* stub, pump evpl_continue() until the reply callback lands,
 * and copy the oracle-relevant reply fields out of the rpc2-owned decode
 * into the caller-owned struct mbt_aux_result.  Unlike the nfs3 wrappers
 * these use mbt_call_wait_soft(): the suite deliberately calls procedures
 * chimera does not implement and PROC_UNAVAIL is a modeled answer, not a
 * harness failure.
 *
 * Two things are asynchronous and therefore accumulate in a separate,
 * never-cleared log (struct mbt_aux.async):
 *   - NLMPROC4_*_RES calls the server makes in response to an
 *     NLMPROC4_*_MSG, and
 *   - NLMPROC4_GRANTED calls from the out-of-band grant engine when a
 *     blocking lock that was answered NLM4_BLOCKED is finally granted.
 * The replayer drains that log at defined points rather than inside the
 * call that provoked it.
 */

#include "nfs3_mbt_common.h"

#define MBT_AUX_MAX_MAPS    32
#define MBT_AUX_MAX_MOUNTS  32
#define MBT_AUX_MAX_EXPORTS 16
#define MBT_AUX_MAX_ASYNC   64
#define MBT_AUX_UADDR_MAX   80
#define MBT_AUX_NAME_MAX    128
#define MBT_AUX_COOKIE_MAX  64

/* nlm4.x: UINT64_MAX in l_len means "to end of file". */
#define MBT_NLM_LEN_EOF     UINT64_MAX

struct mbt_pmap_entry {
    uint32_t prog, vers, prot, port;
};

struct mbt_rpcb_entry {
    uint32_t prog, vers;
    char     netid[16];
    char     addr[MBT_AUX_UADDR_MAX];
    char     owner[MBT_AUX_NAME_MAX];
};

struct mbt_mount_entry {
    char host[MBT_AUX_NAME_MAX];
    char dir[MBT_AUX_NAME_MAX];
};

struct mbt_export_entry {
    char dir[MBT_AUX_NAME_MAX];
    int  ngroups;
    char groups[4][MBT_AUX_NAME_MAX];
};

/* One asynchronous message the SERVER sent us on the NLM connection. */
struct mbt_async_ev {
    int      proc;              /* NLMPROC4_* of the received call */
    uint32_t stat;              /* nlm4_stats carried by it */
    uint32_t cookie_len;
    uint8_t  cookie[MBT_AUX_COOKIE_MAX];
};

/* One auxiliary RPC's oracle-relevant reply fields.  Cleared per call. */
struct mbt_aux_result {
    /* ---- portmap / rpcbind ---- */
    uint32_t                port;            /* PMAPPROC_GETPORT */
    int                     boolres;         /* SET / UNSET */
    int                     nmaps;           /* PMAPPROC_DUMP */
    struct mbt_pmap_entry   maps[MBT_AUX_MAX_MAPS];
    int                     nrpcb;           /* rpcbproc_dump / RPCBPROC_DUMP */
    struct mbt_rpcb_entry   rpcb[MBT_AUX_MAX_MAPS];
    char                    uaddr[MBT_AUX_UADDR_MAX];  /* GETADDR */
    uint32_t                uaddr_len;

    /* ---- MOUNT ---- */
    int                     nauth;           /* MNT auth_flavors */
    int32_t                 auth[8];
    int                     nmounts;         /* MOUNTPROC3_DUMP */
    struct mbt_mount_entry  mounts[MBT_AUX_MAX_MOUNTS];
    int                     nexports;        /* MOUNTPROC3_EXPORT */
    struct mbt_export_entry exports[MBT_AUX_MAX_EXPORTS];

    /* ---- NLM ---- */
    uint32_t                nlm_stat;
    uint32_t                cookie_len;
    uint8_t                 cookie[MBT_AUX_COOKIE_MAX];
    int                     holder_exclusive;
    int32_t                 holder_svid;
    uint32_t                holder_oh_len;
    uint64_t                holder_offset;
    uint64_t                holder_length;
    int32_t                 sequence;        /* nlm4_shareres */

    /* ---- NSM ---- */
    int32_t                 sm_res;          /* res: STAT_SUCC / STAT_FAIL */
    int32_t                 sm_state;
};

/* Drain deadline.  The timer is FIRST so the callback can cast it straight
 * back to the context. */
struct mbt_aux_drain_ctx {
    struct evpl_timer timer;
    volatile int      ticks;
};

struct mbt_aux {
    struct mbt_aux_result r;

    /* Server-initiated messages, accumulated across calls. */
    int                   nasync;
    int                   async_overflow;
    struct mbt_async_ev   async[MBT_AUX_MAX_ASYNC];
};

static inline struct mbt_aux *
mbt_aux(struct mbt_env *env)
{
    return env->aux;
} /* mbt_aux */

static inline struct mbt_aux_result *
mbt_aux_begin(struct mbt_env *env)
{
    struct mbt_aux *a = env->aux;

    mbt_call_begin(env);
    memset(&a->r, 0, sizeof(a->r));
    return &a->r;
} /* mbt_aux_begin */

static inline void
mbt_aux_copy_str(
    char             *dst,
    size_t            cap,
    const xdr_string *src)
{
    size_t n = src->len < cap - 1 ? src->len : cap - 1;

    if (n) {
        memcpy(dst, src->str, n);
    }
    dst[n] = '\0';
} /* mbt_aux_copy_str */

static inline void
mbt_aux_copy_cookie(
    uint8_t          *dst,
    uint32_t         *dst_len,
    const xdr_opaque *src)
{
    uint32_t n = src->len < MBT_AUX_COOKIE_MAX ? src->len : MBT_AUX_COOKIE_MAX;

    if (n) {
        memcpy(dst, src->data, n);
    }
    *dst_len = n;
} /* mbt_aux_copy_cookie */

/* ======================================================================
 * PORTMAP v2 (100000.2)
 * =================================================================== */

static void
mbt_pm_null_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    env->res.done    = 1;
} /* mbt_pm_null_cb */

static inline struct mbt_aux_result *
mbt_pm_null(struct mbt_env *env)
{
    mbt_aux_begin(env);
    env->pm_v2.send_call_PMAPPROC_NULL(&env->pm_v2.rpc2, env->evpl,
                                       env->portmap_conn, &env->cred,
                                       0, 0, NULL, 0, 0, mbt_pm_null_cb, env);
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_pm_null */

static void
mbt_pm_getport_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    uint32_t                     reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    if (status == 0) {
        ((struct mbt_aux *) env->aux)->r.port = reply;
    }
    env->res.done = 1;
} /* mbt_pm_getport_cb */

static inline struct mbt_aux_result *
mbt_pm_getport(
    struct mbt_env *env,
    uint32_t        prog,
    uint32_t        vers,
    uint32_t        prot)
{
    struct mapping args;

    mbt_aux_begin(env);
    args.prog = prog;
    args.vers = vers;
    args.prot = prot;
    args.port = 0;
    env->pm_v2.send_call_PMAPPROC_GETPORT(&env->pm_v2.rpc2, env->evpl,
                                          env->portmap_conn, &env->cred, &args,
                                          0, 0, NULL, 0, 0, mbt_pm_getport_cb,
                                          env);
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_pm_getport */

static void
mbt_pm_set_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    uint32_t                     reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    if (status == 0) {
        ((struct mbt_aux *) env->aux)->r.boolres = reply ? 1 : 0;
    }
    env->res.done = 1;
} /* mbt_pm_set_cb */

/* PMAPPROC_SET (1) and PMAPPROC_UNSET (2): chimera registers neither, so
 * these exist to pin the PROC_UNAVAIL answer the model predicts. */
static inline struct mbt_aux_result *
mbt_pm_set(
    struct mbt_env *env,
    uint32_t        prog,
    uint32_t        vers,
    uint32_t        prot,
    uint32_t        port,
    int             unset)
{
    struct mapping args;

    mbt_aux_begin(env);
    args.prog = prog;
    args.vers = vers;
    args.prot = prot;
    args.port = port;
    if (unset) {
        env->pm_v2.send_call_PMAPPROC_UNSET(&env->pm_v2.rpc2, env->evpl,
                                            env->portmap_conn, &env->cred,
                                            &args, 0, 0, NULL, 0, 0,
                                            mbt_pm_set_cb, env);
    } else {
        env->pm_v2.send_call_PMAPPROC_SET(&env->pm_v2.rpc2, env->evpl,
                                          env->portmap_conn, &env->cred, &args,
                                          0, 0, NULL, 0, 0, mbt_pm_set_cb, env);
    }
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_pm_set */

static void
mbt_pm_dump_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct pmapdumpres          *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env        *env = private_data;
    struct mbt_aux_result *r   = &((struct mbt_aux *) env->aux)->r;
    struct pmaplist       *cur;

    env->res.rpc_err = status;
    if (status == 0) {
        for (cur = reply->maps; cur && r->nmaps < MBT_AUX_MAX_MAPS;
             cur = cur->next) {
            r->maps[r->nmaps].prog = cur->map.prog;
            r->maps[r->nmaps].vers = cur->map.vers;
            r->maps[r->nmaps].prot = cur->map.prot;
            r->maps[r->nmaps].port = cur->map.port;
            r->nmaps++;
        }
    }
    env->res.done = 1;
} /* mbt_pm_dump_cb */

static inline struct mbt_aux_result *
mbt_pm_dump(struct mbt_env *env)
{
    mbt_aux_begin(env);
    env->pm_v2.send_call_PMAPPROC_DUMP(&env->pm_v2.rpc2, env->evpl,
                                       env->portmap_conn, &env->cred,
                                       0, 0, NULL, 0, 0, mbt_pm_dump_cb, env);
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_pm_dump */

static void
mbt_pm_callit_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct call_result          *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    if (status == 0) {
        ((struct mbt_aux *) env->aux)->r.port = reply->port;
    }
    env->res.done = 1;
} /* mbt_pm_callit_cb */

/* PMAPPROC_CALLIT (5): the indirect-call proxy.  Unregistered in chimera. */
static inline struct mbt_aux_result *
mbt_pm_callit(
    struct mbt_env *env,
    uint32_t        prog,
    uint32_t        vers,
    uint32_t        proc)
{
    struct call_args args;

    mbt_aux_begin(env);
    args.prog      = prog;
    args.vers      = vers;
    args.proc      = proc;
    args.args.len  = 0;
    args.args.data = NULL;
    env->pm_v2.send_call_PMAPPROC_CALLIT(&env->pm_v2.rpc2, env->evpl,
                                         env->portmap_conn, &env->cred, &args,
                                         0, 0, NULL, 0, 0, mbt_pm_callit_cb,
                                         env);
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_pm_callit */

/* ======================================================================
 * RPCBIND v3 / v4 (100000.3 / 100000.4)
 * =================================================================== */

static void
mbt_rb_getaddr_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    xdr_string                  *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env        *env = private_data;
    struct mbt_aux_result *r   = &((struct mbt_aux *) env->aux)->r;

    env->res.rpc_err = status;
    if (status == 0) {
        mbt_aux_copy_str(r->uaddr, sizeof(r->uaddr), reply);
        r->uaddr_len = reply->len;
    }
    env->res.done = 1;
} /* mbt_rb_getaddr_cb */

/* GETADDR against rpcbind version `vers` (3 or 4). */
static inline struct mbt_aux_result *
mbt_rb_getaddr(
    struct mbt_env *env,
    int             vers,
    uint32_t        prog,
    uint32_t        pvers,
    const char     *netid)
{
    struct rpcb args;

    mbt_aux_begin(env);
    args.r_prog = prog;
    args.r_vers = pvers;
    xdr_set_str_static(&args, r_netid, netid, (uint32_t) strlen(netid));
    xdr_set_str_static(&args, r_addr, "", 0);
    xdr_set_str_static(&args, r_owner, "", 0);

    if (vers == 3) {
        env->pm_v3.send_call_rpcbproc_getaddr(&env->pm_v3.rpc2, env->evpl,
                                              env->portmap_conn, &env->cred,
                                              &args, 0, 0, NULL, 0, 0,
                                              mbt_rb_getaddr_cb, env);
    } else {
        env->pm_v4.send_call_RPCBPROC_GETADDR(&env->pm_v4.rpc2, env->evpl,
                                              env->portmap_conn, &env->cred,
                                              &args, 0, 0, NULL, 0, 0,
                                              mbt_rb_getaddr_cb, env);
    }
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_rb_getaddr */

static void
mbt_rb_dump_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct rpcbdumpres          *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env        *env = private_data;
    struct mbt_aux_result *r   = &((struct mbt_aux *) env->aux)->r;
    struct rp__list       *cur;

    env->res.rpc_err = status;
    if (status == 0) {
        for (cur = reply->maps; cur && r->nrpcb < MBT_AUX_MAX_MAPS;
             cur = cur->next) {
            struct mbt_rpcb_entry *e = &r->rpcb[r->nrpcb];

            e->prog = cur->rpcb_map.r_prog;
            e->vers = cur->rpcb_map.r_vers;
            mbt_aux_copy_str(e->netid, sizeof(e->netid), &cur->rpcb_map.r_netid);
            mbt_aux_copy_str(e->addr, sizeof(e->addr), &cur->rpcb_map.r_addr);
            mbt_aux_copy_str(e->owner, sizeof(e->owner), &cur->rpcb_map.r_owner);
            r->nrpcb++;
        }
    }
    env->res.done = 1;
} /* mbt_rb_dump_cb */

static inline struct mbt_aux_result *
mbt_rb_dump(
    struct mbt_env *env,
    int             vers)
{
    mbt_aux_begin(env);
    if (vers == 3) {
        env->pm_v3.send_call_rpcbproc_dump(&env->pm_v3.rpc2, env->evpl,
                                           env->portmap_conn, &env->cred,
                                           0, 0, NULL, 0, 0, mbt_rb_dump_cb,
                                           env);
    } else {
        env->pm_v4.send_call_RPCBPROC_DUMP(&env->pm_v4.rpc2, env->evpl,
                                           env->portmap_conn, &env->cred,
                                           0, 0, NULL, 0, 0, mbt_rb_dump_cb,
                                           env);
    }
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_rb_dump */

static void
mbt_rb_gettime_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    uint32_t                     reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    if (status == 0) {
        ((struct mbt_aux *) env->aux)->r.port = reply;
    }
    env->res.done = 1;
} /* mbt_rb_gettime_cb */

/* GETTIME (proc 6) -- unregistered in chimera; pins PROC_UNAVAIL. */
static inline struct mbt_aux_result *
mbt_rb_gettime(
    struct mbt_env *env,
    int             vers)
{
    mbt_aux_begin(env);
    if (vers == 3) {
        env->pm_v3.send_call_rpcbproc_gettime(&env->pm_v3.rpc2, env->evpl,
                                              env->portmap_conn, &env->cred,
                                              0, 0, NULL, 0, 0,
                                              mbt_rb_gettime_cb, env);
    } else {
        env->pm_v4.send_call_RPCBPROC_GETTIME(&env->pm_v4.rpc2, env->evpl,
                                              env->portmap_conn, &env->cred,
                                              0, 0, NULL, 0, 0,
                                              mbt_rb_gettime_cb, env);
    }
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_rb_gettime */

/* GETVERSADDR (v4 proc 9) -- unregistered in chimera. */
static inline struct mbt_aux_result *
mbt_rb_getversaddr(
    struct mbt_env *env,
    uint32_t        prog,
    uint32_t        pvers,
    const char     *netid)
{
    struct rpcb args;

    mbt_aux_begin(env);
    args.r_prog = prog;
    args.r_vers = pvers;
    xdr_set_str_static(&args, r_netid, netid, (uint32_t) strlen(netid));
    xdr_set_str_static(&args, r_addr, "", 0);
    xdr_set_str_static(&args, r_owner, "", 0);
    env->pm_v4.send_call_RPCBPROC_GETVERSADDR(&env->pm_v4.rpc2, env->evpl,
                                              env->portmap_conn, &env->cred,
                                              &args, 0, 0, NULL, 0, 0,
                                              mbt_rb_getaddr_cb, env);
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_rb_getversaddr */

/* ======================================================================
 * MOUNT v3 (100005.3)
 * =================================================================== */

static void
mbt_aux_mnt_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct mountres3            *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env        *env = private_data;
    struct mbt_aux_result *r   = &((struct mbt_aux *) env->aux)->r;
    int                    i;

    env->res.rpc_err = status;
    if (status == 0) {
        env->res.status = reply->fhs_status;
        if (reply->fhs_status == MNT3_OK) {
            mbt_copy_fh(&env->res.obj_fh, &reply->mountinfo.fhandle);
            r->nauth = reply->mountinfo.num_auth_flavors;
            if (r->nauth > (int) (sizeof(r->auth) / sizeof(r->auth[0]))) {
                r->nauth = sizeof(r->auth) / sizeof(r->auth[0]);
            }
            for (i = 0; i < r->nauth; i++) {
                r->auth[i] = reply->mountinfo.auth_flavors[i];
            }
        }
    }
    env->res.done = 1;
} /* mbt_aux_mnt_cb */

/* MOUNTPROC3_MNT with the auth_flavors list copied out as well as the
 * handle (mbt_mnt in the nfs3 harness keeps only the handle). */
static inline struct mbt_aux_result *
mbt_mount_mnt(
    struct mbt_env *env,
    const char     *path)
{
    struct mountarg3 args;

    mbt_aux_begin(env);
    xdr_set_str_static(&args, path, path, (uint32_t) strlen(path));
    env->mount_v3.send_call_MOUNTPROC3_MNT(&env->mount_v3.rpc2, env->evpl,
                                           env->mount_conn, &env->cred, &args,
                                           0, 0, NULL, 0, 0, mbt_aux_mnt_cb,
                                           env);
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_mount_mnt */

static void
mbt_void_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    int                          status,
    void                        *private_data)
{
    struct mbt_env *env = private_data;

    env->res.rpc_err = status;
    env->res.done    = 1;
} /* mbt_void_cb */

static inline struct mbt_aux_result *
mbt_mount_null(struct mbt_env *env)
{
    mbt_aux_begin(env);
    env->mount_v3.send_call_MOUNTPROC3_NULL(&env->mount_v3.rpc2, env->evpl,
                                            env->mount_conn, &env->cred,
                                            0, 0, NULL, 0, 0, mbt_void_cb, env);
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_mount_null */

static void
mbt_mount_dump_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct mountdumpres         *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env        *env = private_data;
    struct mbt_aux_result *r   = &((struct mbt_aux *) env->aux)->r;
    struct mountbody      *cur;

    env->res.rpc_err = status;
    if (status == 0) {
        for (cur = reply->mounts; cur && r->nmounts < MBT_AUX_MAX_MOUNTS;
             cur = cur->ml_next) {
            mbt_aux_copy_str(r->mounts[r->nmounts].host,
                             sizeof(r->mounts[0].host), &cur->ml_hostname);
            mbt_aux_copy_str(r->mounts[r->nmounts].dir,
                             sizeof(r->mounts[0].dir), &cur->ml_directory);
            r->nmounts++;
        }
    }
    env->res.done = 1;
} /* mbt_mount_dump_cb */

static inline struct mbt_aux_result *
mbt_mount_dump(struct mbt_env *env)
{
    mbt_aux_begin(env);
    env->mount_v3.send_call_MOUNTPROC3_DUMP(&env->mount_v3.rpc2, env->evpl,
                                            env->mount_conn, &env->cred,
                                            0, 0, NULL, 0, 0, mbt_mount_dump_cb,
                                            env);
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_mount_dump */

static inline struct mbt_aux_result *
mbt_mount_umnt(
    struct mbt_env *env,
    const char     *path)
{
    struct mountarg3 args;

    mbt_aux_begin(env);
    xdr_set_str_static(&args, path, path, (uint32_t) strlen(path));
    env->mount_v3.send_call_MOUNTPROC3_UMNT(&env->mount_v3.rpc2, env->evpl,
                                            env->mount_conn, &env->cred, &args,
                                            0, 0, NULL, 0, 0, mbt_void_cb, env);
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_mount_umnt */

static inline struct mbt_aux_result *
mbt_mount_umntall(struct mbt_env *env)
{
    mbt_aux_begin(env);
    env->mount_v3.send_call_MOUNTPROC3_UMNTALL(&env->mount_v3.rpc2, env->evpl,
                                               env->mount_conn, &env->cred,
                                               0, 0, NULL, 0, 0, mbt_void_cb,
                                               env);
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_mount_umntall */

static void
mbt_mount_export_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct exportres            *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env        *env = private_data;
    struct mbt_aux_result *r   = &((struct mbt_aux *) env->aux)->r;
    struct exportnode     *cur;
    struct groupnode      *g;

    env->res.rpc_err = status;
    if (status == 0) {
        for (cur = reply->exports; cur && r->nexports < MBT_AUX_MAX_EXPORTS;
             cur = cur->ex_next) {
            struct mbt_export_entry *e = &r->exports[r->nexports];

            mbt_aux_copy_str(e->dir, sizeof(e->dir), &cur->ex_dir);
            for (g = cur->ex_groups; g && e->ngroups < 4; g = g->nextgroup) {
                mbt_aux_copy_str(e->groups[e->ngroups], sizeof(e->groups[0]),
                                 &g->name);
                e->ngroups++;
            }
            r->nexports++;
        }
    }
    env->res.done = 1;
} /* mbt_mount_export_cb */

static inline struct mbt_aux_result *
mbt_mount_export(struct mbt_env *env)
{
    mbt_aux_begin(env);
    env->mount_v3.send_call_MOUNTPROC3_EXPORT(&env->mount_v3.rpc2, env->evpl,
                                              env->mount_conn, &env->cred,
                                              0, 0, NULL, 0, 0,
                                              mbt_mount_export_cb, env);
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_mount_export */

/* ======================================================================
 * NLM v4 (100021.4)
 * =================================================================== */

/* Argument scratch: the generated stubs marshal from these buffers inside
 * the send_call, and every wrapper here is synchronous, so one static set
 * per call site is enough. */
struct mbt_nlm_args {
    char    caller[MBT_AUX_NAME_MAX];
    uint8_t oh[MBT_AUX_COOKIE_MAX];
    uint8_t cookie[MBT_AUX_COOKIE_MAX];
};

/* Fill an nlm4_lock from the model-level identity + range. */
static inline void
mbt_nlm_fill_lock(
    struct nlm4_lock    *alock,
    struct mbt_nlm_args *scratch,
    const char          *caller,
    const struct mbt_fh *fh,
    const uint8_t       *oh,
    uint32_t             oh_len,
    int32_t              svid,
    uint64_t             offset,
    uint64_t             length)
{
    snprintf(scratch->caller, sizeof(scratch->caller), "%s", caller);
    if (oh_len > MBT_AUX_COOKIE_MAX) {
        oh_len = MBT_AUX_COOKIE_MAX;
    }
    if (oh_len) {
        memcpy(scratch->oh, oh, oh_len);
    }

    xdr_set_str_static(alock, caller_name, scratch->caller,
                       (uint32_t) strlen(scratch->caller));
    alock->fh.len   = fh->len;
    alock->fh.data  = (uint8_t *) fh->data;
    alock->oh.len   = oh_len;
    alock->oh.data  = scratch->oh;
    alock->svid     = svid;
    alock->l_offset = offset;
    alock->l_len    = length;
} /* mbt_nlm_fill_lock */

static inline void
mbt_nlm_fill_cookie(
    xdr_opaque          *cookie,
    struct mbt_nlm_args *scratch,
    const uint8_t       *bytes,
    uint32_t             len)
{
    if (len > MBT_AUX_COOKIE_MAX) {
        len = MBT_AUX_COOKIE_MAX;
    }
    if (len) {
        memcpy(scratch->cookie, bytes, len);
    }
    cookie->len  = len;
    cookie->data = len ? scratch->cookie : NULL;
} /* mbt_nlm_fill_cookie */

static void
mbt_nlm_res_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct nlm4_res             *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env        *env = private_data;
    struct mbt_aux_result *r   = &((struct mbt_aux *) env->aux)->r;

    env->res.rpc_err = status;
    if (status == 0) {
        r->nlm_stat = reply->stat;
        mbt_aux_copy_cookie(r->cookie, &r->cookie_len, &reply->cookie);
    }
    env->res.done = 1;
} /* mbt_nlm_res_cb */

static void
mbt_nlm_testres_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct nlm4_testres         *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env        *env = private_data;
    struct mbt_aux_result *r   = &((struct mbt_aux *) env->aux)->r;

    env->res.rpc_err = status;
    if (status == 0) {
        r->nlm_stat = reply->test_stat.stat;
        mbt_aux_copy_cookie(r->cookie, &r->cookie_len, &reply->cookie);
        if (reply->test_stat.stat == NLM4_DENIED) {
            r->holder_exclusive = reply->test_stat.holder.exclusive ? 1 : 0;
            r->holder_svid      = reply->test_stat.holder.svid;
            r->holder_oh_len    = reply->test_stat.holder.oh.len;
            r->holder_offset    = reply->test_stat.holder.l_offset;
            r->holder_length    = reply->test_stat.holder.l_len;
        }
    }
    env->res.done = 1;
} /* mbt_nlm_testres_cb */

static void
mbt_nlm_shareres_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct nlm4_shareres        *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env        *env = private_data;
    struct mbt_aux_result *r   = &((struct mbt_aux *) env->aux)->r;

    env->res.rpc_err = status;
    if (status == 0) {
        r->nlm_stat = reply->stat;
        r->sequence = reply->sequence;
        mbt_aux_copy_cookie(r->cookie, &r->cookie_len, &reply->cookie);
    }
    env->res.done = 1;
} /* mbt_nlm_shareres_cb */

static inline struct mbt_aux_result *
mbt_nlm_null(struct mbt_env *env)
{
    mbt_aux_begin(env);
    env->nlm_v4.send_call_NLMPROC4_NULL(&env->nlm_v4.rpc2, env->evpl,
                                        env->nlm_conn, &env->cred,
                                        0, 0, NULL, 0, 0, mbt_void_cb, env);
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_nlm_null */

/* NLMPROC4_TEST (1) or NLMPROC4_TEST_MSG (6) when msg is set. */
static inline struct mbt_aux_result *
mbt_nlm_test(
    struct mbt_env      *env,
    int                  msg,
    const char          *caller,
    const struct mbt_fh *fh,
    const uint8_t       *oh,
    uint32_t             oh_len,
    int32_t              svid,
    int                  exclusive,
    uint64_t             offset,
    uint64_t             length,
    const uint8_t       *cookie,
    uint32_t             cookie_len)
{
    static struct mbt_nlm_args scratch;
    struct nlm4_testargs       args;

    mbt_aux_begin(env);
    mbt_nlm_fill_cookie(&args.cookie, &scratch, cookie, cookie_len);
    args.exclusive = exclusive ? true : false;
    mbt_nlm_fill_lock(&args.alock, &scratch, caller, fh, oh, oh_len, svid,
                      offset, length);

    if (msg) {
        env->nlm_v4.send_call_NLMPROC4_TEST_MSG(&env->nlm_v4.rpc2, env->evpl,
                                                env->nlm_conn, &env->cred,
                                                &args, 0, 0, NULL, 0, 0,
                                                mbt_void_cb, env);
    } else {
        env->nlm_v4.send_call_NLMPROC4_TEST(&env->nlm_v4.rpc2, env->evpl,
                                            env->nlm_conn, &env->cred, &args,
                                            0, 0, NULL, 0, 0,
                                            mbt_nlm_testres_cb, env);
    }
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_nlm_test */

/* NLMPROC4_LOCK (2), NLMPROC4_LOCK_MSG (7) or NLMPROC4_NM_LOCK (22). */
static inline struct mbt_aux_result *
mbt_nlm_lock(
    struct mbt_env      *env,
    int                  proc,
    const char          *caller,
    const struct mbt_fh *fh,
    const uint8_t       *oh,
    uint32_t             oh_len,
    int32_t              svid,
    int                  exclusive,
    int                  block,
    int                  reclaim,
    int32_t              state,
    uint64_t             offset,
    uint64_t             length,
    const uint8_t       *cookie,
    uint32_t             cookie_len)
{
    static struct mbt_nlm_args scratch;
    struct nlm4_lockargs       args;

    mbt_aux_begin(env);
    mbt_nlm_fill_cookie(&args.cookie, &scratch, cookie, cookie_len);
    args.block     = block ? true : false;
    args.exclusive = exclusive ? true : false;
    args.reclaim   = reclaim ? true : false;
    args.state     = state;
    mbt_nlm_fill_lock(&args.alock, &scratch, caller, fh, oh, oh_len, svid,
                      offset, length);

    if (proc == 7) {
        env->nlm_v4.send_call_NLMPROC4_LOCK_MSG(&env->nlm_v4.rpc2, env->evpl,
                                                env->nlm_conn, &env->cred,
                                                &args, 0, 0, NULL, 0, 0,
                                                mbt_void_cb, env);
    } else if (proc == 22) {
        env->nlm_v4.send_call_NLMPROC4_NM_LOCK(&env->nlm_v4.rpc2, env->evpl,
                                               env->nlm_conn, &env->cred,
                                               &args, 0, 0, NULL, 0, 0,
                                               mbt_nlm_res_cb, env);
    } else {
        env->nlm_v4.send_call_NLMPROC4_LOCK(&env->nlm_v4.rpc2, env->evpl,
                                            env->nlm_conn, &env->cred, &args,
                                            0, 0, NULL, 0, 0, mbt_nlm_res_cb,
                                            env);
    }
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_nlm_lock */

/* NLMPROC4_CANCEL (3) or NLMPROC4_CANCEL_MSG (8). */
static inline struct mbt_aux_result *
mbt_nlm_cancel(
    struct mbt_env      *env,
    int                  msg,
    const char          *caller,
    const struct mbt_fh *fh,
    const uint8_t       *oh,
    uint32_t             oh_len,
    int32_t              svid,
    int                  exclusive,
    int                  block,
    uint64_t             offset,
    uint64_t             length,
    const uint8_t       *cookie,
    uint32_t             cookie_len)
{
    static struct mbt_nlm_args scratch;
    struct nlm4_cancargs       args;

    mbt_aux_begin(env);
    mbt_nlm_fill_cookie(&args.cookie, &scratch, cookie, cookie_len);
    args.block     = block ? true : false;
    args.exclusive = exclusive ? true : false;
    mbt_nlm_fill_lock(&args.alock, &scratch, caller, fh, oh, oh_len, svid,
                      offset, length);

    if (msg) {
        env->nlm_v4.send_call_NLMPROC4_CANCEL_MSG(&env->nlm_v4.rpc2, env->evpl,
                                                  env->nlm_conn, &env->cred,
                                                  &args, 0, 0, NULL, 0, 0,
                                                  mbt_void_cb, env);
    } else {
        env->nlm_v4.send_call_NLMPROC4_CANCEL(&env->nlm_v4.rpc2, env->evpl,
                                              env->nlm_conn, &env->cred, &args,
                                              0, 0, NULL, 0, 0, mbt_nlm_res_cb,
                                              env);
    }
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_nlm_cancel */

/* NLMPROC4_UNLOCK (4) or NLMPROC4_UNLOCK_MSG (9). */
static inline struct mbt_aux_result *
mbt_nlm_unlock(
    struct mbt_env      *env,
    int                  msg,
    const char          *caller,
    const struct mbt_fh *fh,
    const uint8_t       *oh,
    uint32_t             oh_len,
    int32_t              svid,
    uint64_t             offset,
    uint64_t             length,
    const uint8_t       *cookie,
    uint32_t             cookie_len)
{
    static struct mbt_nlm_args scratch;
    struct nlm4_unlockargs     args;

    mbt_aux_begin(env);
    mbt_nlm_fill_cookie(&args.cookie, &scratch, cookie, cookie_len);
    mbt_nlm_fill_lock(&args.alock, &scratch, caller, fh, oh, oh_len, svid,
                      offset, length);

    if (msg) {
        env->nlm_v4.send_call_NLMPROC4_UNLOCK_MSG(&env->nlm_v4.rpc2, env->evpl,
                                                  env->nlm_conn, &env->cred,
                                                  &args, 0, 0, NULL, 0, 0,
                                                  mbt_void_cb, env);
    } else {
        env->nlm_v4.send_call_NLMPROC4_UNLOCK(&env->nlm_v4.rpc2, env->evpl,
                                              env->nlm_conn, &env->cred, &args,
                                              0, 0, NULL, 0, 0, mbt_nlm_res_cb,
                                              env);
    }
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_nlm_unlock */

/* NLMPROC4_GRANTED (5) or NLMPROC4_GRANTED_MSG (10): the client-callback
 * direction of the protocol, which a server also has to answer. */
static inline struct mbt_aux_result *
mbt_nlm_granted(
    struct mbt_env      *env,
    int                  msg,
    const char          *caller,
    const struct mbt_fh *fh,
    const uint8_t       *oh,
    uint32_t             oh_len,
    int32_t              svid,
    int                  exclusive,
    uint64_t             offset,
    uint64_t             length,
    const uint8_t       *cookie,
    uint32_t             cookie_len)
{
    static struct mbt_nlm_args scratch;
    struct nlm4_testargs       args;

    mbt_aux_begin(env);
    mbt_nlm_fill_cookie(&args.cookie, &scratch, cookie, cookie_len);
    args.exclusive = exclusive ? true : false;
    mbt_nlm_fill_lock(&args.alock, &scratch, caller, fh, oh, oh_len, svid,
                      offset, length);

    if (msg) {
        env->nlm_v4.send_call_NLMPROC4_GRANTED_MSG(&env->nlm_v4.rpc2, env->evpl,
                                                   env->nlm_conn, &env->cred,
                                                   &args, 0, 0, NULL, 0, 0,
                                                   mbt_void_cb, env);
    } else {
        env->nlm_v4.send_call_NLMPROC4_GRANTED(&env->nlm_v4.rpc2, env->evpl,
                                               env->nlm_conn, &env->cred,
                                               &args, 0, 0, NULL, 0, 0,
                                               mbt_nlm_res_cb, env);
    }
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_nlm_granted */

/* The *_RES half (procs 11-15): a client reporting an asynchronous result
 * to the server, which acks with a void reply. */
static inline struct mbt_aux_result *
mbt_nlm_send_res(
    struct mbt_env *env,
    int             proc,
    uint32_t        stat,
    const uint8_t  *cookie,
    uint32_t        cookie_len)
{
    static struct mbt_nlm_args scratch;
    struct nlm4_res            res;
    struct nlm4_testres        tres;

    mbt_aux_begin(env);

    if (proc == 11) {
        mbt_nlm_fill_cookie(&tres.cookie, &scratch, cookie, cookie_len);
        tres.test_stat.stat = stat;
        /* nlm4_testrply is a union on the status: NLM4_DENIED carries a
         * holder, every other arm is void.  The holder has to be filled in
         * even though nothing reads it -- marshalling a netobj from
         * uninitialised memory produces a message that never goes out, and
         * a send_call whose marshal fails never fires its callback, so the
         * caller waits forever rather than seeing an error. */
        if (stat == NLM4_DENIED) {
            tres.test_stat.holder.exclusive = true;
            tres.test_stat.holder.svid      = 1;
            tres.test_stat.holder.oh.len    = 0;
            tres.test_stat.holder.oh.data   = NULL;
            tres.test_stat.holder.l_offset  = 0;
            tres.test_stat.holder.l_len     = 0;
        }
        env->nlm_v4.send_call_NLMPROC4_TEST_RES(&env->nlm_v4.rpc2, env->evpl,
                                                env->nlm_conn, &env->cred,
                                                &tres, 0, 0, NULL, 0, 0,
                                                mbt_void_cb, env);
        mbt_call_wait_soft(env);
        return &mbt_aux(env)->r;
    }

    mbt_nlm_fill_cookie(&res.cookie, &scratch, cookie, cookie_len);
    res.stat = stat;

    switch (proc) {
        case 12:
            env->nlm_v4.send_call_NLMPROC4_LOCK_RES(&env->nlm_v4.rpc2,
                                                    env->evpl, env->nlm_conn,
                                                    &env->cred, &res, 0, 0,
                                                    NULL, 0, 0, mbt_void_cb,
                                                    env);
            break;
        case 13:
            env->nlm_v4.send_call_NLMPROC4_CANCEL_RES(&env->nlm_v4.rpc2,
                                                      env->evpl, env->nlm_conn,
                                                      &env->cred, &res, 0, 0,
                                                      NULL, 0, 0, mbt_void_cb,
                                                      env);
            break;
        case 14:
            env->nlm_v4.send_call_NLMPROC4_UNLOCK_RES(&env->nlm_v4.rpc2,
                                                      env->evpl, env->nlm_conn,
                                                      &env->cred, &res, 0, 0,
                                                      NULL, 0, 0, mbt_void_cb,
                                                      env);
            break;
        default:
            env->nlm_v4.send_call_NLMPROC4_GRANTED_RES(&env->nlm_v4.rpc2,
                                                       env->evpl, env->nlm_conn,
                                                       &env->cred, &res, 0, 0,
                                                       NULL, 0, 0, mbt_void_cb,
                                                       env);
            break;
    } /* switch */

    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_nlm_send_res */

/* The four reserved slots (16-19): chimera acks them, which is what the
 * model pins -- an unimplemented slot would be PROC_UNAVAIL instead. */
static inline struct mbt_aux_result *
mbt_nlm_reserved(
    struct mbt_env *env,
    int             proc)
{
    mbt_aux_begin(env);
    switch (proc) {
        case 16:
            env->nlm_v4.send_call_NLMPROC4_RESERVED_16(&env->nlm_v4.rpc2,
                                                       env->evpl, env->nlm_conn,
                                                       &env->cred, 0, 0, NULL,
                                                       0, 0, mbt_void_cb, env);
            break;
        case 17:
            env->nlm_v4.send_call_NLMPROC4_RESERVED_17(&env->nlm_v4.rpc2,
                                                       env->evpl, env->nlm_conn,
                                                       &env->cred, 0, 0, NULL,
                                                       0, 0, mbt_void_cb, env);
            break;
        case 18:
            env->nlm_v4.send_call_NLMPROC4_RESERVED_18(&env->nlm_v4.rpc2,
                                                       env->evpl, env->nlm_conn,
                                                       &env->cred, 0, 0, NULL,
                                                       0, 0, mbt_void_cb, env);
            break;
        default:
            env->nlm_v4.send_call_NLMPROC4_RESERVED_19(&env->nlm_v4.rpc2,
                                                       env->evpl, env->nlm_conn,
                                                       &env->cred, 0, 0, NULL,
                                                       0, 0, mbt_void_cb, env);
            break;
    } /* switch */
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_nlm_reserved */

/* NLMPROC4_SHARE (20) / NLMPROC4_UNSHARE (21): the DOS share-mode half. */
static inline struct mbt_aux_result *
mbt_nlm_share(
    struct mbt_env      *env,
    int                  unshare,
    const char          *caller,
    const struct mbt_fh *fh,
    const uint8_t       *oh,
    uint32_t             oh_len,
    int                  mode,
    int                  access,
    int                  reclaim,
    const uint8_t       *cookie,
    uint32_t             cookie_len)
{
    static struct mbt_nlm_args scratch;
    struct nlm4_shareargs      args;

    mbt_aux_begin(env);
    mbt_nlm_fill_cookie(&args.cookie, &scratch, cookie, cookie_len);
    args.reclaim = reclaim ? true : false;

    snprintf(scratch.caller, sizeof(scratch.caller), "%s", caller);
    if (oh_len > MBT_AUX_COOKIE_MAX) {
        oh_len = MBT_AUX_COOKIE_MAX;
    }
    if (oh_len) {
        memcpy(scratch.oh, oh, oh_len);
    }
    xdr_set_str_static(&args.share, caller_name, scratch.caller,
                       (uint32_t) strlen(scratch.caller));
    args.share.fh.len  = fh->len;
    args.share.fh.data = (uint8_t *) fh->data;
    args.share.oh.len  = oh_len;
    args.share.oh.data = scratch.oh;
    args.share.mode    = mode;
    args.share.access  = access;

    if (unshare) {
        env->nlm_v4.send_call_NLMPROC4_UNSHARE(&env->nlm_v4.rpc2, env->evpl,
                                               env->nlm_conn, &env->cred,
                                               &args, 0, 0, NULL, 0, 0,
                                               mbt_nlm_shareres_cb, env);
    } else {
        env->nlm_v4.send_call_NLMPROC4_SHARE(&env->nlm_v4.rpc2, env->evpl,
                                             env->nlm_conn, &env->cred, &args,
                                             0, 0, NULL, 0, 0,
                                             mbt_nlm_shareres_cb, env);
    }
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_nlm_share */

/* NLMPROC4_FREE_ALL (23). */
static inline struct mbt_aux_result *
mbt_nlm_free_all(
    struct mbt_env *env,
    const char     *name,
    int32_t         state)
{
    static struct mbt_nlm_args scratch;
    struct nlm4_notify         args;

    mbt_aux_begin(env);
    snprintf(scratch.caller, sizeof(scratch.caller), "%s", name);
    xdr_set_str_static(&args, name, scratch.caller,
                       (uint32_t) strlen(scratch.caller));
    args.state = state;
    env->nlm_v4.send_call_NLMPROC4_FREE_ALL(&env->nlm_v4.rpc2, env->evpl,
                                            env->nlm_conn, &env->cred, &args,
                                            0, 0, NULL, 0, 0, mbt_void_cb, env);
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_nlm_free_all */

/* ---- server-initiated NLM messages (the async half) -------------------- */

static inline void
mbt_aux_async_push(
    struct mbt_env   *env,
    int               proc,
    uint32_t          stat,
    const xdr_opaque *cookie)
{
    struct mbt_aux *a = env->aux;

    if (a->nasync >= MBT_AUX_MAX_ASYNC) {
        a->async_overflow = 1;
        return;
    }
    a->async[a->nasync].proc = proc;
    a->async[a->nasync].stat = stat;
    mbt_aux_copy_cookie(a->async[a->nasync].cookie,
                        &a->async[a->nasync].cookie_len, cookie);
    a->nasync++;
} /* mbt_aux_async_push */

#define MBT_AUX_DEFINE_RES_RECV(NAME, PROC)                               \
        static void                                                           \
        mbt_aux_recv_ ## NAME(                                                \
            struct evpl *evpl,                                  \
            struct evpl_rpc2_conn *conn,                                  \
            struct evpl_rpc2_cred *cred,                                  \
            struct nlm4_res *args,                                  \
            struct evpl_rpc2_encoding *encoding,                              \
            void *private_data)                          \
        {                                                                     \
            struct mbt_env *env = private_data;                               \
            int             rc;                                               \
                                                                          \
            mbt_aux_async_push(env, PROC, args->stat, &args->cookie);          \
            rc = env->nlm_v4.send_reply_ ## NAME(evpl, NULL, encoding);        \
            if (rc) {                                                         \
                fprintf(stderr, "failed to ack " #NAME "\n");                  \
                exit(3);                                                      \
            }                                                                 \
        }

MBT_AUX_DEFINE_RES_RECV(NLMPROC4_LOCK_RES, 12)
MBT_AUX_DEFINE_RES_RECV(NLMPROC4_CANCEL_RES, 13)
MBT_AUX_DEFINE_RES_RECV(NLMPROC4_UNLOCK_RES, 14)
MBT_AUX_DEFINE_RES_RECV(NLMPROC4_GRANTED_RES, 15)

static void
mbt_aux_recv_NLMPROC4_TEST_RES(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_testres       *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct mbt_env *env = private_data;
    int             rc;

    mbt_aux_async_push(env, 11, args->test_stat.stat, &args->cookie);
    rc = env->nlm_v4.send_reply_NLMPROC4_TEST_RES(evpl, NULL, encoding);
    if (rc) {
        fprintf(stderr, "failed to ack NLMPROC4_TEST_RES\n");
        exit(3);
    }
} /* mbt_aux_recv_NLMPROC4_TEST_RES */

/* The out-of-band grant engine delivers a deferred blocking lock with a
 * GRANTED *call* rather than a *_RES.  It arrives on a connection the
 * server opened to us, but the program is the same, so record it here. */
static void
mbt_aux_recv_NLMPROC4_GRANTED(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_testargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct mbt_env *env = private_data;
    struct nlm4_res res;
    int             rc;

    mbt_aux_async_push(env, 5, NLM4_GRANTED, &args->cookie);

    res.cookie = args->cookie;
    res.stat   = NLM4_GRANTED;
    rc         = env->nlm_v4.send_reply_NLMPROC4_GRANTED(evpl, NULL, &res,
                                                         encoding);
    if (rc) {
        fprintf(stderr, "failed to ack NLMPROC4_GRANTED\n");
        exit(3);
    }
} /* mbt_aux_recv_NLMPROC4_GRANTED */

static void
mbt_aux_recv_NLMPROC4_GRANTED_MSG(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_testargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct mbt_env *env = private_data;
    int             rc;

    mbt_aux_async_push(env, 10, NLM4_GRANTED, &args->cookie);
    rc = env->nlm_v4.send_reply_NLMPROC4_GRANTED_MSG(evpl, NULL, encoding);
    if (rc) {
        fprintf(stderr, "failed to ack NLMPROC4_GRANTED_MSG\n");
        exit(3);
    }
} /* mbt_aux_recv_NLMPROC4_GRANTED_MSG */

/* ======================================================================
 * NSM / sm_inter v1 (100024.1)
 * =================================================================== */

static void
mbt_sm_statres_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct sm_stat_res          *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env        *env = private_data;
    struct mbt_aux_result *r   = &((struct mbt_aux *) env->aux)->r;

    env->res.rpc_err = status;
    if (status == 0) {
        r->sm_res   = reply->res_stat;
        r->sm_state = reply->state;
    }
    env->res.done = 1;
} /* mbt_sm_statres_cb */

static void
mbt_sm_stat_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct sm_stat              *reply,
    int                          status,
    void                        *private_data)
{
    struct mbt_env        *env = private_data;
    struct mbt_aux_result *r   = &((struct mbt_aux *) env->aux)->r;

    env->res.rpc_err = status;
    if (status == 0) {
        r->sm_state = reply->state;
    }
    env->res.done = 1;
} /* mbt_sm_stat_cb */

static inline struct mbt_aux_result *
mbt_sm_null(struct mbt_env *env)
{
    mbt_aux_begin(env);
    env->nsm_v1.send_call_SM_NULL(&env->nsm_v1.rpc2, env->evpl, env->nsm_conn,
                                  &env->cred, 0, 0, NULL, 0, 0, mbt_void_cb,
                                  env);
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_sm_null */

/* Scratch for the NSM string arguments, same contract as mbt_nlm_args. */
struct mbt_sm_args {
    char mon[MBT_AUX_NAME_MAX];
    char my[MBT_AUX_NAME_MAX];
};

static inline struct mbt_aux_result *
mbt_sm_stat(
    struct mbt_env *env,
    const char     *host)
{
    static struct mbt_sm_args scratch;
    struct sm_name            args;

    mbt_aux_begin(env);
    snprintf(scratch.mon, sizeof(scratch.mon), "%s", host);
    xdr_set_str_static(&args, mon_name, scratch.mon,
                       (uint32_t) strlen(scratch.mon));
    env->nsm_v1.send_call_SM_STAT(&env->nsm_v1.rpc2, env->evpl, env->nsm_conn,
                                  &env->cred, &args, 0, 0, NULL, 0, 0,
                                  mbt_sm_statres_cb, env);
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_sm_stat */

static inline struct mbt_aux_result *
mbt_sm_mon(
    struct mbt_env *env,
    const char     *host,
    const char     *my_name,
    int32_t         prog,
    int32_t         vers,
    int32_t         proc)
{
    static struct mbt_sm_args scratch;
    struct mon                args;

    mbt_aux_begin(env);
    snprintf(scratch.mon, sizeof(scratch.mon), "%s", host);
    snprintf(scratch.my, sizeof(scratch.my), "%s", my_name);
    xdr_set_str_static(&args.id, mon_name, scratch.mon,
                       (uint32_t) strlen(scratch.mon));
    xdr_set_str_static(&args.id.id, my_name, scratch.my,
                       (uint32_t) strlen(scratch.my));
    args.id.id.my_prog = prog;
    args.id.id.my_vers = vers;
    args.id.id.my_proc = proc;
    memset(args.priv, 0, sizeof(args.priv));

    env->nsm_v1.send_call_SM_MON(&env->nsm_v1.rpc2, env->evpl, env->nsm_conn,
                                 &env->cred, &args, 0, 0, NULL, 0, 0,
                                 mbt_sm_statres_cb, env);
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_sm_mon */

static inline struct mbt_aux_result *
mbt_sm_unmon(
    struct mbt_env *env,
    const char     *host,
    const char     *my_name)
{
    static struct mbt_sm_args scratch;
    struct mon_id             args;

    mbt_aux_begin(env);
    snprintf(scratch.mon, sizeof(scratch.mon), "%s", host);
    snprintf(scratch.my, sizeof(scratch.my), "%s", my_name);
    xdr_set_str_static(&args, mon_name, scratch.mon,
                       (uint32_t) strlen(scratch.mon));
    xdr_set_str_static(&args.id, my_name, scratch.my,
                       (uint32_t) strlen(scratch.my));
    args.id.my_prog = 100021;
    args.id.my_vers = 4;
    args.id.my_proc = 24;

    env->nsm_v1.send_call_SM_UNMON(&env->nsm_v1.rpc2, env->evpl, env->nsm_conn,
                                   &env->cred, &args, 0, 0, NULL, 0, 0,
                                   mbt_sm_stat_cb, env);
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_sm_unmon */

static inline struct mbt_aux_result *
mbt_sm_unmon_all(
    struct mbt_env *env,
    const char     *my_name)
{
    static struct mbt_sm_args scratch;
    struct my_id              args;

    mbt_aux_begin(env);
    snprintf(scratch.my, sizeof(scratch.my), "%s", my_name);
    xdr_set_str_static(&args, my_name, scratch.my,
                       (uint32_t) strlen(scratch.my));
    args.my_prog = 100021;
    args.my_vers = 4;
    args.my_proc = 24;

    env->nsm_v1.send_call_SM_UNMON_ALL(&env->nsm_v1.rpc2, env->evpl,
                                       env->nsm_conn, &env->cred, &args,
                                       0, 0, NULL, 0, 0, mbt_sm_stat_cb, env);
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_sm_unmon_all */

static inline struct mbt_aux_result *
mbt_sm_simu_crash(struct mbt_env *env)
{
    mbt_aux_begin(env);
    env->nsm_v1.send_call_SM_SIMU_CRASH(&env->nsm_v1.rpc2, env->evpl,
                                        env->nsm_conn, &env->cred,
                                        0, 0, NULL, 0, 0, mbt_void_cb, env);
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_sm_simu_crash */

static inline struct mbt_aux_result *
mbt_sm_notify(
    struct mbt_env *env,
    const char     *host,
    int32_t         state)
{
    static struct mbt_sm_args scratch;
    struct stat_chge          args;

    mbt_aux_begin(env);
    snprintf(scratch.mon, sizeof(scratch.mon), "%s", host);
    xdr_set_str_static(&args, mon_name, scratch.mon,
                       (uint32_t) strlen(scratch.mon));
    args.state = state;

    env->nsm_v1.send_call_SM_NOTIFY(&env->nsm_v1.rpc2, env->evpl, env->nsm_conn,
                                    &env->cred, &args, 0, 0, NULL, 0, 0,
                                    mbt_void_cb, env);
    mbt_call_wait_soft(env);
    return &mbt_aux(env)->r;
} /* mbt_sm_notify */

/* ======================================================================
 * Lifecycle
 * =================================================================== */

/* Open the aux harness: the nfs3 env with the auxiliary services connected,
 * plus this header's async receive handlers installed on NLM_V4. */
static inline void
mbt_aux_env_open(
    struct mbt_env      *env,
    struct mbt_env_opts *opts)
{
    static struct mbt_aux aux_storage;

    opts->aux = 1;
    mbt_env_open_opts(env, opts);

    memset(&aux_storage, 0, sizeof(aux_storage));
    env->aux = &aux_storage;

    env->nlm_v4.recv_call_NLMPROC4_TEST_RES    = mbt_aux_recv_NLMPROC4_TEST_RES;
    env->nlm_v4.recv_call_NLMPROC4_LOCK_RES    = mbt_aux_recv_NLMPROC4_LOCK_RES;
    env->nlm_v4.recv_call_NLMPROC4_CANCEL_RES  = mbt_aux_recv_NLMPROC4_CANCEL_RES;
    env->nlm_v4.recv_call_NLMPROC4_UNLOCK_RES  = mbt_aux_recv_NLMPROC4_UNLOCK_RES;
    env->nlm_v4.recv_call_NLMPROC4_GRANTED_RES = mbt_aux_recv_NLMPROC4_GRANTED_RES;
    env->nlm_v4.recv_call_NLMPROC4_GRANTED     = mbt_aux_recv_NLMPROC4_GRANTED;
    env->nlm_v4.recv_call_NLMPROC4_GRANTED_MSG = mbt_aux_recv_NLMPROC4_GRANTED_MSG;
} /* mbt_aux_env_open */

/* Pump the client loop until the server's asynchronous messages have been
 * dispatched.
 *
 * evpl_continue() BLOCKS until something happens, so a fixed spin count
 * would wedge the moment the last pending event has been consumed.  A
 * PERIODIC timer bounds it: a one-shot does not, because it is popped
 * before its callback runs and the very evpl_continue() that fired it then
 * waits with no deadline left.  A periodic timer is re-armed before the
 * wait, so every pass is bounded and the caller's flag is re-read at a
 * known cadence.  The timer is removed before returning.
 */
#define MBT_AUX_DRAIN_TICK_US 5000

static void
mbt_aux_drain_timer_cb(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    (void) evpl;
    ((struct mbt_aux_drain_ctx *) timer)->ticks++;
} /* mbt_aux_drain_timer_cb */

/* Drain until `want` asynchronous messages have arrived, or the deadline
 * passes.  want == 0 always waits the full timeout (the "nothing should
 * come back" case).  Returns the number actually seen. */
static inline int
mbt_aux_drain_for(
    struct mbt_env *env,
    int             want,
    uint64_t        timeout_us)
{
    struct mbt_aux          *a     = env->aux;
    int                      limit = (int) (timeout_us /
                                            MBT_AUX_DRAIN_TICK_US) + 1;
    struct mbt_aux_drain_ctx ctx;

    ctx.ticks = 0;
    evpl_add_timer(env->evpl, &ctx.timer, mbt_aux_drain_timer_cb,
                   MBT_AUX_DRAIN_TICK_US);
    while (ctx.ticks < limit && (want == 0 || a->nasync < want)) {
        evpl_continue(env->evpl);
    }
    evpl_remove_timer(env->evpl, &ctx.timer);
    return a->nasync;
} /* mbt_aux_drain_for */

/* Wait out a fixed window with nothing expected back. */
static inline void
mbt_aux_drain_us(
    struct mbt_env *env,
    uint64_t        timeout_us)
{
    mbt_aux_drain_for(env, 0, timeout_us);
} /* mbt_aux_drain_us */

static inline void
mbt_aux_async_reset(struct mbt_env *env)
{
    struct mbt_aux *a = env->aux;

    a->nasync         = 0;
    a->async_overflow = 0;
} /* mbt_aux_async_reset */
