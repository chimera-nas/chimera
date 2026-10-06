// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/compound_retry.h"
#include <fcntl.h>
#include <string.h>
#include <stdlib.h>
#include "common/thread.h"
#include <xxhash.h>

#include "nfs_common.h"
#include "nfs_internal.h"
#include "nfs_nlm.h"
#include "common/format.h"
#include "nfs_nlm_state.h"
#include "nfs_nlm_range.h"
#include "nfs_nlm_granted.h"
#include "nfs_nsm.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_claim.h"
#include "vfs/vfs_compound.h"

/*
 * The FH opens below are internal lock-bookkeeping opens (the client already
 * opened the file via NFS), not user data access, so they run with a system
 * credential rather than re-evaluating DAC.  AUTH_NONE is the engine's
 * privileged sentinel: chimera_vfs_gate_needed() short-circuits it, so the open
 * is not access-gated.  Must be non-NULL -- the gate dereferences cred->flavor
 * (passing NULL crashes on backends that do not delegate DAC, e.g. memfs).
 * File-scope const so its lifetime spans the asynchronous open.
 */
static const struct chimera_vfs_cred nlm_system_cred = {
    .flavor = CHIMERA_VFS_AUTH_NONE,
    .uid    = 0,
    .gid    = 0,
    .ngids  = 0,
};

/* Peer IP of an NLM connection with the ephemeral port stripped, written as a
 * C-string into out (size bytes).  Handles both "ipv4:port" and "[ipv6]:port".
 * Used to record where a lock-holder's statd can be reached (NSM monitor). */
static void
nlm_conn_peer_addr(
    struct evpl_rpc2_conn *conn,
    char                  *out,
    int                    size)
{
    char  *p;
    size_t n;

    evpl_rpc2_conn_get_remote_address(conn, out, size);

    if (out[0] == '[') {
        p = strchr(out, ']');
        n = p ? (size_t) (p - (out + 1)) : strlen(out);
        memmove(out, out + 1, n);
        out[n] = '\0';
    } else {
        p = strrchr(out, ':');
        if (p) {
            *p = '\0';
        }
    }
} /* nlm_conn_peer_addr */

/* Convert NLM length (UINT64_MAX == to-EOF) to POSIX length (0 == to-EOF) */
#define NLM_TO_POSIX_LEN(l)     ((l) == UINT64_MAX ? 0 : (l))

/* NLM keeps its internal byte-range length in the POSIX convention (0 == to
 * EOF), but the VFS range layer uses UINT64_MAX for to-EOF (length 0 is a real
 * zero-byte range).  Translate when handing a length to a VFS claim/probe. */
#define NLM_POSIX_LEN_TO_VFS(l) ((l) == 0 ? UINT64_MAX : (l))

/* Accepted entries anchor backend opens only. All byte-range geometry and
 * admission live in the VFS lock domain, including partial unlock/downgrade. */
static bool
nlm_same_owner_file(
    const struct nlm_lock_entry *a,
    const struct nlm_lock_entry *b)
{
    return a->svid == b->svid && a->oh_len == b->oh_len && a->vfh_len == b->vfh_len &&
           !memcmp(a->oh, b->oh, a->oh_len) && !memcmp(a->vfh, b->vfh, a->vfh_len);
} /* nlm_same_owner_file */

/* Map NLM caller_name (hostname) to a claim owner.client_key. */
static inline uint64_t
nlm_owner_client_key(const char *hostname)
{
    return XXH3_64bits(hostname, strlen(hostname));
} /* nlm_owner_client_key */

/* Map NLM (oh_bytes, svid) to a claim owner.owner_lo.  Two LOCK ops
 * with the same (hostname, oh, svid) tuple will produce the same owner
 * identity and therefore coalesce in the claim core's admission predicate. */
static inline uint64_t
nlm_owner_owner_lo(
    const uint8_t *oh,
    uint32_t       oh_len,
    int32_t        svid)
{
    uint8_t buf[LM_MAXSTRLEN + sizeof(int32_t)];

    if (oh_len > LM_MAXSTRLEN) {
        oh_len = LM_MAXSTRLEN;
    }
    if (oh_len > 0) {
        memcpy(buf, oh, oh_len);
    }
    memcpy(buf + oh_len, &svid, sizeof(svid));
    return XXH3_64bits(buf, oh_len + sizeof(svid));
} /* nlm_owner_owner_lo */

/*
 * The *_RES half of NLM is fire-and-forget: the server reports the result of
 * an asynchronous (*_MSG) request and has no use for whatever the client
 * answers.  It still has to hand the generated stub a reply callback, because
 * the reply dispatcher invokes it unconditionally -- passing NULL there
 * crashes the server the moment a client acknowledges the call, which every
 * conforming RPC client does.
 */
static void
nlm4_res_sent_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    int                          status,
    void                        *private_data)
{
    (void) evpl;
    (void) verf;
    (void) private_data;

    if (status) {
        chimera_nfs_debug("NLM asynchronous result was not acknowledged: %d",
                          status);
    }
} /* nlm4_res_sent_cb */

/* -------------------------------------------------------------------------
 * Helper: deliver an nlm4_res outcome for whichever procedure asked for it.
 *
 * The synchronous procedures answer on their own RPC.  The asynchronous ones
 * have already acknowledged the request with a void reply, so their outcome
 * travels as a *_RES CALL back to the client -- which is why this needs the
 * connection as well as the encoding.  An unhandled proc here would drop the
 * outcome silently and leave the client waiting forever, so the default arm
 * is loud rather than a no-op.
 * ---------------------------------------------------------------------- */
static void
nlm4_send_res(
    struct chimera_server_nfs_shared *shared,
    struct evpl                      *evpl,
    struct evpl_rpc2_conn            *conn,
    struct evpl_rpc2_encoding        *encoding,
    const xdr_opaque                 *cookie,
    nlm4_stats                        stat,
    int                               proc)
{
    struct nlm4_res res;
    int             rc = 0;

    res.cookie.len  = cookie ? cookie->len : 0;
    res.cookie.data = cookie ? cookie->data : NULL;
    res.stat        = stat;

    switch (proc) {
        case 2:
            rc = shared->nlm_v4.send_reply_NLMPROC4_LOCK(evpl, NULL, &res, encoding);
            break;
        case 3:
            rc = shared->nlm_v4.send_reply_NLMPROC4_CANCEL(evpl, NULL, &res, encoding);
            break;
        case 4:
            rc = shared->nlm_v4.send_reply_NLMPROC4_UNLOCK(evpl, NULL, &res, encoding);
            break;
        case 5:
            rc = shared->nlm_v4.send_reply_NLMPROC4_GRANTED(evpl, NULL, &res, encoding);
            break;
        case 17:   /* NLMPROC4_LOCK_MSG   -> NLMPROC4_LOCK_RES   */
            shared->nlm_v4.send_call_NLMPROC4_LOCK_RES(&shared->nlm_v4.rpc2, evpl, conn, NULL, &res, 0, 0, NULL, 0, 0,
                                                       nlm4_res_sent_cb, NULL);
            break;
        case 18:   /* NLMPROC4_UNLOCK_MSG -> NLMPROC4_UNLOCK_RES */
            shared->nlm_v4.send_call_NLMPROC4_UNLOCK_RES(&shared->nlm_v4.rpc2, evpl, conn, NULL, &res, 0, 0, NULL, 0, 0,
                                                         nlm4_res_sent_cb, NULL);
            break;
        case 19:   /* NLMPROC4_CANCEL_MSG -> NLMPROC4_CANCEL_RES */
            shared->nlm_v4.send_call_NLMPROC4_CANCEL_RES(&shared->nlm_v4.rpc2, evpl, conn, NULL, &res, 0, 0, NULL, 0, 0,
                                                         nlm4_res_sent_cb, NULL);
            break;
        case 22:
            rc = shared->nlm_v4.send_reply_NLMPROC4_NM_LOCK(evpl, NULL, &res, encoding);
            break;
        default:
            chimera_nfs_abort_if(1, "No NLM result delivery for proc %d", proc);
            break;
    } /* switch */
    chimera_nfs_abort_if(rc, "Failed to send NLM res reply");
} /* nlm4_send_res */

/* -------------------------------------------------------------------------
 * TEST procedure callbacks
 * ---------------------------------------------------------------------- */

struct nlm_test_ctx {
    struct chimera_server_nfs_thread *thread;
    struct evpl                      *evpl;
    struct evpl_rpc2_encoding        *encoding;
    struct evpl_rpc2_conn            *conn;
    /* The probe, built before the run and read by the executor while the
     * CLAIM_TEST op runs.  Never inserted, so nothing keeps a pointer to it
     * afterwards -- but it has to outlive the submission, which the build's
     * stack frame would not. */
    struct chimera_vfs_claim          probe;
    xdr_opaque                        cookie;
    uint8_t                           cookie_buf[LM_MAXSTRLEN];
    uint64_t                          offset;
    uint64_t                          length;
    bool                              exclusive;
    int                               proc; /* 1=TEST, 16=TEST_MSG */
    /* caller identity for NLM-state conflict check */
    char                              caller_name[LM_MAXSTRLEN + 1];
    uint8_t                           oh[LM_MAXSTRLEN];
    uint32_t                          oh_len;
    int32_t                           svid;
    uint8_t                           fh[NFS4_FHSIZE];
    uint32_t                          fh_len;
};

/* PUTFH seats the object, OPEN_CURRENT opens it, and the CLAIM_TEST asks the
 * question.  The open is a PATH open because a probe reads only the file
 * handle -- and because a data open of a FIFO blocks, which is the whole
 * reason the compound's probe ops want one. */
#define NLM_TEST_OP_PROBE 2

static void
chimera_nfs_nlm4_test_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nlm_test_ctx                  *ctx      = private_data;
    struct chimera_server_nfs_thread     *thread   = ctx->thread;
    struct chimera_server_nfs_shared     *shared   = thread->shared;
    struct evpl                          *evpl     = ctx->evpl;
    struct evpl_rpc2_encoding            *encoding = ctx->encoding;
    const struct chimera_vfs_compound_op *vop;
    enum chimera_vfs_error                error_code;
    struct nlm4_testres                   res;
    int                                   rc;

    res.cookie.len  = ctx->cookie.len;
    res.cookie.data = ctx->cookie.data;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs_debug("NLM TEST failed: error %d -> NLM4_STALE_FH",
                          error_code);
        res.test_stat.stat = NLM4_STALE_FH;
        goto send_reply;
    }

    /* A denial is an ANSWER, so the op succeeded and the run went on -- the
     * probe never fails for being refused.  A file nobody holds a claim on
     * answers GRANTED without the executor having to invent the "no state
     * means no holders" case this used to make for itself. */
    vop = chimera_vfs_compound_op(compound, NLM_TEST_OP_PROBE);

    if (vop->claim_result == CHIMERA_CLAIM_GRANTED) {
        res.test_stat.stat = NLM4_GRANTED;
    } else {
        res.test_stat.stat = NLM4_DENIED;
        /* WRITE_LT iff the holder's used bits include a write-implying
         * capability (a write delegation reports exclusive though it holds
         * no LW). */
        res.test_stat.holder.exclusive = (vop->conflict.used &
                                          (CHIMERA_CLAIM_W |
                                           CHIMERA_CLAIM_CW |
                                           CHIMERA_CLAIM_LW)) != 0;
        res.test_stat.holder.svid     = 0;  /* not exposed by the claim core */
        res.test_stat.holder.oh.len   = 0;
        res.test_stat.holder.oh.data  = NULL;
        res.test_stat.holder.l_offset = vop->conflict.offset;
        /* conflict.length uses the VFS to-EOF sentinel (UINT64_MAX); on the
         * wire a to-EOF holder is l_len 0 (see nfs_nlm_range.h). */
        res.test_stat.holder.l_len = nlm_vfs_len_to_wire(vop->conflict.length);
    }

 send_reply:
    chimera_vfs_compound_free(compound);

    chimera_nfs_debug("NLM TEST cb: stat=%d", res.test_stat.stat);

    if (ctx->proc == 16) {
        shared->nlm_v4.send_call_NLMPROC4_TEST_RES(&shared->nlm_v4.rpc2, evpl,
                                                   ctx->conn, NULL, &res, 0, 0, NULL, 0, 0,
                                                   nlm4_res_sent_cb, NULL);
    } else {
        rc = shared->nlm_v4.send_reply_NLMPROC4_TEST(evpl, NULL, &res, encoding);
        chimera_nfs_abort_if(rc, "Failed to send NLM TEST reply");
    }
    thread->nlm_active--;
    free(ctx);
} /* chimera_nfs_nlm4_test_complete */

/* -------------------------------------------------------------------------
 * LOCK procedure callbacks
 * ---------------------------------------------------------------------- */

struct nlm_lock_ctx {
    struct chimera_server_nfs_thread *thread;
    struct evpl                      *evpl;
    struct evpl_rpc2_encoding        *encoding;
    struct evpl_rpc2_conn            *conn;
    xdr_opaque                        cookie;
    uint8_t                           cookie_buf[LM_MAXSTRLEN];
    struct nlm_lock_entry            *entry;
    struct nlm_client                *client; /* owning client for pending cleanup */
    bool                              block;
    bool                              nm_lock; /* non-monitored: skip persistence */
    int                               proc;    /* LOCK/NM_LOCK/UNLOCK, including MSG */
    struct chimera_vfs_lock_request   lock;
    struct chimera_vfs_open_handle   *borrowed;
    uint8_t                           vfh[CHIMERA_VFS_FH_SIZE];
    uint32_t                          vfh_len;
    /* Set by the blocked-notify callback when the acquire queued (deferred):
     * the immediate NLM4_BLOCKED interim has been sent and the eventual grant
     * must be delivered out-of-band via an NLM_GRANTED callback rather than on
     * the (already-completed) original LOCK RPC. */
    bool                              was_blocked;
    /* Client IP (no port), captured at request time so the out-of-band GRANTED
     * callback can portmap-resolve the client's NLM service. */
    char                              client_addr[80];
};

/* Snapshot a self-contained grant job from the now-granted lock entry.
 *
 * CALLER MUST HOLD nlm_state.mutex.  The entry is only guaranteed to exist
 * while it is held: a client reap (FREE_ALL, SM_NOTIFY, or the last
 * connection dropping) takes every entry off client->locks under the mutex
 * and frees them as soon as it drops it, so reading the entry afterwards is
 * a use-after-free.  The job is a pure value copy, so once it is filled in
 * nothing downstream aliases lock state. */
static void
chimera_nfs_nlm4_build_grant_locked(
    struct nlm_lock_ctx      *ctx,
    struct nlm_grant_request *reqp)
{
    struct nlm_lock_entry   *entry = ctx->entry;
    struct nlm_grant_request req;

    memset(&req, 0, sizeof(req));
    snprintf(req.client_addr, sizeof(req.client_addr), "%s", ctx->client_addr);
    snprintf(req.caller_name, sizeof(req.caller_name), "%s",
             ctx->client->hostname);
    req.cookie_len = ctx->cookie.len < LM_MAXSTRLEN ? ctx->cookie.len : LM_MAXSTRLEN;
    if (req.cookie_len) {
        memcpy(req.cookie, ctx->cookie.data, req.cookie_len);
    }
    req.fh_len = entry->fh_len < NFS4_FHSIZE ? entry->fh_len : NFS4_FHSIZE;
    memcpy(req.fh, entry->fh, req.fh_len);
    req.oh_len = entry->oh_len < LM_MAXSTRLEN ? entry->oh_len : LM_MAXSTRLEN;
    memcpy(req.oh, entry->oh, req.oh_len);
    req.svid   = entry->svid;
    req.offset = entry->offset;
    /* entry->length is the stored POSIX length (0 == to EOF); the callback
     * must carry the wire form, which is also 0.  Linux lockd decodes
     * UINT64_MAX from a nonzero offset as a wrapped, empty range and answers
     * the grant with NLM_DENIED, leaving the waiter blocked for good (see
     * nfs_nlm_range.h). */
    req.length    = nlm_posix_len_to_wire(entry->length);
    req.exclusive = entry->exclusive ? 1 : 0;

    *reqp = req;
} /* chimera_nfs_nlm4_build_grant_locked */

/* Hand a snapshot built above to the outbound NLM_GRANTED engine.  Caller
 * must NOT hold nlm_state.mutex. */
static void
chimera_nfs_nlm4_submit_grant(
    struct nlm_lock_ctx      *ctx,
    struct nlm_grant_request *req)
{
    struct chimera_server_nfs_shared *shared = ctx->thread->shared;
    struct nlm_granter               *granter;

    /* Lazily create the granter (idempotent).  Done under nlm_state.mutex so two
     * threads cannot both create one. */
    evpl_mutex_lock(&shared->nlm_state.mutex);
    granter = nlm_granter_get_or_create(shared);
    evpl_mutex_unlock(&shared->nlm_state.mutex);

    nlm_granter_submit(granter, req);
} /* chimera_nfs_nlm4_submit_grant */

/* Explicit coordination hook when typed LOCK admission queues on a conflict.  Sends the RFC 1813 / XNFS NLM4_BLOCKED interim
 * immediately and records that the eventual grant must be delivered via an
 * out-of-band NLM_GRANTED callback (not on this RPC). */
static void
chimera_nfs_nlm4_lock_blocked_cb(void *private_data)
{
    struct nlm_lock_ctx              *ctx      = private_data;
    struct chimera_server_nfs_thread *thread   = ctx->thread;
    struct chimera_server_nfs_shared *shared   = thread->shared;
    struct evpl                      *evpl     = ctx->evpl;
    struct evpl_rpc2_encoding        *encoding = ctx->encoding;
    struct nlm4_res                   res;

    evpl_mutex_lock(&shared->nlm_state.mutex);
    bool                              reaped       = ctx->entry->reaped;
    bool                              disconnected = ctx->entry->disconnected;
    evpl_mutex_unlock(&shared->nlm_state.mutex);
    if (reaped || ctx->was_blocked) {
        return; /* Retry may park again; one interim per logical RPC. */
    }
    ctx->was_blocked = true;
    if (disconnected) {
        return; /* Another connection may still keep this client alive. */
    }

    res.cookie.len  = ctx->cookie.len;
    res.cookie.data = ctx->cookie.data;
    res.stat        = NLM4_BLOCKED;

    chimera_nfs_debug("NLM LOCK: blocking lock queued -> NLM4_BLOCKED (proc %d)",
                      ctx->proc);

    /* LOCK and non-monitored NM_LOCK both honor the wire block flag. */
    nlm4_send_res(shared, evpl, ctx->conn, encoding, &res.cookie, NLM4_BLOCKED, ctx->proc);
    if (ctx->proc != 17) {
        ctx->encoding = NULL; /* The BLOCKED reply consumed this request. */
    }
} /* chimera_nfs_nlm4_lock_blocked_cb */

/* The VFS has already accepted and published its journal. Pending registry
 * entries remain pinned until this worker removes them under the same mutex
 * used by CANCEL/recovery. Nothing runs from a foreign claim-core callback. */
static void
chimera_nfs_nlm4_lock_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nlm_lock_ctx              *ctx    = private_data;
    struct chimera_server_nfs_thread *thread = ctx->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nlm_lock_entry            *entry = ctx->entry, *old, *next, *retired = NULL;
    struct nlm_grant_request          grant;
    enum chimera_vfs_error            status = chimera_vfs_compound_status(compound);
    bool                              deliver = false, monitor = false, keep = false;
    bool                              reaped;

    evpl_mutex_lock(&shared->nlm_state.mutex);
    reaped = entry->reaped;
    bool                              disconnected = entry->disconnected;
    DL_DELETE(ctx->client->locks, entry);
    entry->compound = NULL;
    if (status == CHIMERA_VFS_OK && !reaped) {
        bool held = chimera_vfs_lock_domain_has_locks(ctx->client->domain,
                                                      ctx->vfh, ctx->vfh_len, &ctx->lock.owner);
        DL_FOREACH_SAFE(ctx->client->locks, old, next)
        {
            if (!old->pending && nlm_same_owner_file(old, entry) && (!held || !entry->unlock)) {
                DL_DELETE(ctx->client->locks, old);
                old->next = retired;
                retired   = old;
            }
        }
        if (!entry->unlock) {
            if (held) {
                entry->handle  = chimera_vfs_compound_take_handle(compound, 2);
                entry->pending = false;
                DL_APPEND(ctx->client->locks, entry);
                keep = true;
            }
            monitor = !ctx->nm_lock;
            if (ctx->was_blocked) {
                chimera_nfs_nlm4_build_grant_locked(ctx, &grant);
                deliver = true;
            }
        }
    }
    evpl_mutex_unlock(&shared->nlm_state.mutex);
    while (retired) {
        old     = retired;
        retired = old->next;
        chimera_vfs_release(thread->vfs_thread, old->handle);
        nlm_lock_entry_free(old);
    }
    chimera_vfs_compound_free(compound);
    if (ctx->borrowed) {
        chimera_vfs_release(thread->vfs_thread, ctx->borrowed);
    }
    if (!keep) {
        nlm_lock_entry_free(entry);
    }
    if (monitor) {
        nsm_monitor(thread, ctx->client->hostname, ctx->client_addr);
    }
    if (deliver) {
        chimera_nfs_nlm4_submit_grant(ctx, &grant);
    } else if (!disconnected && !(ctx->was_blocked && ctx->proc != 17) &&
               (!reaped || ctx->proc == 2 || ctx->proc == 22 || ctx->proc == 4)) {
        /* Recovery may cut off OPEN before a synchronous LOCK sent BLOCKED.
         * Complete that original RPC; a disconnected encoding must stay unused. */
        if (reaped) {
            status = CHIMERA_VFS_EINTR;
        }
        nlm4_stats stat = status == CHIMERA_VFS_OK ? NLM4_GRANTED :
            (status == CHIMERA_VFS_ENOSPC ? NLM4_DENIED_NOLOCKS :
             (status == CHIMERA_VFS_ESTALE || status == CHIMERA_VFS_ENOENT ? NLM4_STALE_FH : NLM4_DENIED));
        nlm4_send_res(shared, ctx->evpl, ctx->conn, ctx->encoding, &ctx->cookie, stat, ctx->proc);
    }
    thread->nlm_active--;
    free(ctx);
} /* chimera_nfs_nlm4_lock_complete */

void
chimera_nfs_nlm4_thread_init(struct chimera_server_nfs_thread *thread)
{
    thread->nlm_active = 0;
} /* chimera_nfs_nlm4_thread_init */

void
chimera_nfs_nlm4_thread_destroy(struct chimera_server_nfs_thread *thread)
{
    struct nlm_client *client, *next;

    /* Stop retired requests before the RPC/VFS worker they complete on dies. */
    nlm_state_shutdown(&thread->shared->nlm_state);
    while (thread->nlm_active) {
        evpl_continue(thread->evpl);
    }
    HASH_ITER(hh, thread->shared->nlm_state.clients, client, next)
    {
        nlm_client_release_all_locks(&thread->shared->nlm_state, client,
                                     thread->vfs_thread, thread->vfs->vfs_state, NULL);
    }
} /* chimera_nfs_nlm4_thread_destroy */

void
chimera_nfs_nlm4_null(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    chimera_nfs_debug("NLM NULL");

    rc = shared->nlm_v4.send_reply_NLMPROC4_NULL(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM NULL reply");
} /* chimera_nfs_nlm4_null */

static void
chimera_nfs_nlm4_do_test(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_testargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data,
    int                        proc)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nlm_test_ctx              *ctx;
    struct chimera_vfs_compound      *compound;
    struct chimera_claim_owner        owner;
    uint8_t                           vfh[CHIMERA_VFS_FH_SIZE];
    int                               vfh_len;
    uint16_t                          vexp;

    chimera_nfs_debug("NLM TEST: caller='%.*s' fh_len=%u offset=%lu len=%lu exclusive=%d",
                      (int) args->alock.caller_name.len, args->alock.caller_name.str,
                      (unsigned) args->alock.fh.len,
                      (unsigned long) args->alock.l_offset,
                      (unsigned long) args->alock.l_len,
                      (int) args->exclusive);

    /* The wire handle is wrapped (export id + signing MAC); recover the inner
     * VFS handle for the open below.  NLM keeps the client's wrapped bytes for
     * lock identity, but the VFS layer needs the unwrapped handle. */
    if (chimera_nfs_fh_unwrap(args->alock.fh.data, args->alock.fh.len, &vexp,
                              vfh, &vfh_len, shared->fh_key, shared->fh_sign) !=
        CHIMERA_NFS_FH_OK) {
        struct nlm4_testres err_res;
        err_res.cookie.len     = args->cookie.len;
        err_res.cookie.data    = args->cookie.data;
        err_res.test_stat.stat = NLM4_STALE_FH;
        if (proc == 16) {
            shared->nlm_v4.send_call_NLMPROC4_TEST_RES(&shared->nlm_v4.rpc2, evpl, conn, NULL, &err_res, 0, 0, NULL, 0,
                                                       0, nlm4_res_sent_cb,
                                                       NULL);
        } else {
            int rc = shared->nlm_v4.send_reply_NLMPROC4_TEST(evpl, NULL, &err_res, encoding);
            chimera_nfs_abort_if(rc, "Failed to send NLM TEST stale-fh reply");
        }
        return;
    }

    ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        struct nlm4_testres err_res;
        err_res.cookie.len     = args->cookie.len;
        err_res.cookie.data    = args->cookie.data;
        err_res.test_stat.stat = NLM4_DENIED_NOLOCKS;
        if (proc == 16) {
            shared->nlm_v4.send_call_NLMPROC4_TEST_RES(&shared->nlm_v4.rpc2, evpl, conn, NULL, &err_res, 0, 0, NULL, 0,
                                                       0, nlm4_res_sent_cb,
                                                       NULL);
        } else {
            int oom_rc = shared->nlm_v4.send_reply_NLMPROC4_TEST(evpl, NULL, &err_res, encoding);
            chimera_nfs_abort_if(oom_rc, "Failed to send NLM TEST OOM reply");
        }
        return;
    }
    ctx->thread    = thread;
    ctx->evpl      = evpl;
    ctx->encoding  = encoding;
    ctx->conn      = conn;
    ctx->proc      = proc;
    ctx->offset    = args->alock.l_offset;
    ctx->length    = nlm_wire_len_to_posix(args->alock.l_len);
    ctx->exclusive = args->exclusive;
    ctx->svid      = args->alock.svid;

    if (args->alock.caller_name.str && args->alock.caller_name.len > 0) {
        size_t nlen = args->alock.caller_name.len < LM_MAXSTRLEN
                      ? args->alock.caller_name.len
                      : LM_MAXSTRLEN;
        memcpy(ctx->caller_name, args->alock.caller_name.str, nlen);
        ctx->caller_name[nlen] = '\0';
    }

    if (args->alock.oh.len > 0) {
        ctx->oh_len = args->alock.oh.len < LM_MAXSTRLEN
                      ? args->alock.oh.len
                      : LM_MAXSTRLEN;
        memcpy(ctx->oh, args->alock.oh.data, ctx->oh_len);
    }

    if (args->alock.fh.len > 0) {
        ctx->fh_len = args->alock.fh.len < NFS4_FHSIZE
                      ? args->alock.fh.len
                      : NFS4_FHSIZE;
        memcpy(ctx->fh, args->alock.fh.data, ctx->fh_len);
    }

    if (args->cookie.len > 0 && args->cookie.len <= LM_MAXSTRLEN) {
        ctx->cookie.len  = args->cookie.len;
        ctx->cookie.data = ctx->cookie_buf;
        memcpy(ctx->cookie_buf, args->cookie.data, args->cookie.len);
    }

    memset(&owner, 0, sizeof(owner));
    owner.proto      = CHIMERA_CLAIM_PROTO_NLM;
    owner.client_key = nlm_owner_client_key(ctx->caller_name);
    owner.owner_lo   = nlm_owner_owner_lo(ctx->oh, ctx->oh_len, ctx->svid);

    chimera_vfs_claim_init_range(&ctx->probe, ctx->exclusive, /* smb */ false,
                                 ctx->offset,
                                 NLM_POSIX_LEN_TO_VFS(ctx->length),
                                 &owner);

    compound = chimera_vfs_compound_alloc(thread->vfs_thread,
                                          &nlm_system_cred);

    chimera_vfs_compound_add_putfh(compound, vfh, vfh_len);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH, 0);
    chimera_vfs_compound_add_claim_test(compound, &ctx->probe, 0);

    thread->nlm_active++;
    chimera_frontend_compound_submit(compound, chimera_nfs_nlm4_test_complete, ctx);
} /* chimera_nfs_nlm4_do_test */

void
chimera_nfs_nlm4_test(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_testargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    chimera_nfs_nlm4_do_test(evpl, conn, cred, args, encoding, private_data, 1);
} /* chimera_nfs_nlm4_test */
static void
chimera_nfs_nlm4_do_lock(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *rpc_cred,
    struct nlm4_lockargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data,
    bool                       nm_lock,
    int                        proc)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct chimera_vfs_compound      *compound;
    struct nlm_lock_entry            *entry;
    struct nlm_lock_ctx              *ctx;
    struct nlm_client                *client;
    char                              safe_hostname[LM_MAXSTRLEN + 1];
    size_t                            hn_len;
    uint8_t                           vfh[CHIMERA_VFS_FH_SIZE];
    int                               vfh_len;
    uint16_t                          vexp;

    /* NUL-terminate the XDR caller_name for use as a C-string hash key */
    hn_len = args->alock.caller_name.len < LM_MAXSTRLEN
             ? args->alock.caller_name.len : LM_MAXSTRLEN;
    memcpy(safe_hostname, args->alock.caller_name.str, hn_len);
    safe_hostname[hn_len] = '\0';

    chimera_nfs_debug("NLM LOCK: caller='%s' fh_len=%u offset=%lu len=%lu exclusive=%d block=%d reclaim=%d nm=%d",
                      safe_hostname,
                      (unsigned) args->alock.fh.len,
                      (unsigned long) args->alock.l_offset,
                      (unsigned long) args->alock.l_len,
                      (int) args->exclusive, (int) args->block, (int) args->reclaim, (int) nm_lock);
    {
        char ohhex[2 * 64 + 1];
        format_hex(ohhex, sizeof(ohhex), args->alock.oh.data,
                   args->alock.oh.len > 64 ? 64 : args->alock.oh.len);
        chimera_nfs_debug("NLM LOCK: oh=%s svid=%d", ohhex, args->alock.svid);
    }

    /* Recover the inner VFS handle from the wrapped wire handle for the open
    * below (lock identity keeps the client's wrapped bytes in entry->fh). */
    if (chimera_nfs_fh_unwrap(args->alock.fh.data, args->alock.fh.len, &vexp,
                              vfh, &vfh_len, shared->fh_key, shared->fh_sign) !=
        CHIMERA_NFS_FH_OK) {
        nlm4_send_res(shared, evpl, conn, encoding, &args->cookie, (proc == 4 || proc == 18) ? NLM4_GRANTED :
                      NLM4_STALE_FH, proc);
        return;
    }

    /* Build the in-flight lock entry (handle filled in by open callback) */
    entry = nlm_lock_entry_alloc();
    if (!entry) {
        nlm4_send_res(shared, evpl, conn, encoding, &args->cookie, NLM4_DENIED_NOLOCKS, proc);
        return;
    }
    memcpy(entry->vfh, vfh, vfh_len);
    entry->vfh_len = vfh_len;
    entry->fh_len  = args->alock.fh.len < NFS4_FHSIZE ? args->alock.fh.len : NFS4_FHSIZE;
    memcpy(entry->fh, args->alock.fh.data, entry->fh_len);
    entry->oh_len = args->alock.oh.len < LM_MAXSTRLEN ? args->alock.oh.len : LM_MAXSTRLEN;
    memcpy(entry->oh, args->alock.oh.data, entry->oh_len);
    entry->svid      = args->alock.svid;
    entry->offset    = args->alock.l_offset;
    entry->length    = nlm_wire_len_to_posix(args->alock.l_len);
    entry->exclusive = args->exclusive;
    entry->handle    = NULL;
    entry->pending   = true;
    entry->conn      = conn;
    entry->unlock    = proc == 4 || proc == 18;

    ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        nlm_lock_entry_free(entry);
        nlm4_send_res(shared, evpl, conn, encoding, &args->cookie, NLM4_DENIED_NOLOCKS, proc);
        return;
    }
    ctx->thread   = thread;
    ctx->evpl     = evpl;
    ctx->encoding = encoding;
    ctx->conn     = conn;
    ctx->entry    = entry;
    ctx->block    = args->block;
    ctx->nm_lock  = nm_lock;
    ctx->proc     = proc;
    /* Capture the client IP now (the conn outlives the request, but the grant
     * callback runs on a different thread and must not touch core-thread conn
     * state); the out-of-band NLM_GRANTED engine portmap-resolves it. */
    nlm_conn_peer_addr(conn, ctx->client_addr, sizeof(ctx->client_addr));

    if (args->cookie.len > 0 && args->cookie.len <= LM_MAXSTRLEN) {
        ctx->cookie.len  = args->cookie.len;
        ctx->cookie.data = ctx->cookie_buf;
        memcpy(ctx->cookie_buf, args->cookie.data, args->cookie.len);
    }

    /* Reserve generation and register the compound under the recovery mutex.
     * The VFS performs conflict checks; this list protects request lifetime. */
    evpl_mutex_lock(&shared->nlm_state.mutex);

    if (shared->nlm_state.stopping) {
        evpl_mutex_unlock(&shared->nlm_state.mutex);
        nlm_lock_entry_free(entry);
        free(ctx);
        nlm4_send_res(shared, evpl, conn, encoding, &args->cookie, NLM4_DENIED_NOLOCKS, proc);
        return;
    }

    /* Grace period check: only reclaim locks are accepted during grace */
    if (!entry->unlock && nlm_state_in_grace(&shared->nlm_state) && !args->reclaim) {
        evpl_mutex_unlock(&shared->nlm_state.mutex);
        chimera_nfs_debug("NLM LOCK: rejected -> server in grace period");
        nlm4_send_res(shared, evpl, conn, encoding, &ctx->cookie, NLM4_DENIED_GRACE_PERIOD, proc);
        nlm_lock_entry_free(entry);
        free(ctx);
        return;
    }

    /* Look up or create per-client state */
    if (entry->unlock) {
        HASH_FIND_STR(shared->nlm_state.clients, safe_hostname, client);
        if (!client) {
            evpl_mutex_unlock(&shared->nlm_state.mutex);
            nlm_lock_entry_free(entry);
            free(ctx);
            nlm4_send_res(shared, evpl, conn, encoding, &args->cookie, NLM4_GRANTED, proc);
            return;
        }
    } else {
        client = nlm_client_lookup_or_create(&shared->nlm_state, safe_hostname);
    }
    if (!client) {
        evpl_mutex_unlock(&shared->nlm_state.mutex);
        nlm_lock_entry_free(entry);
        free(ctx);
        nlm4_send_res(shared, evpl, conn, encoding, &args->cookie, NLM4_DENIED_NOLOCKS, proc);
        return;
    }
    ctx->client = client;

    memset(&ctx->lock, 0, sizeof(ctx->lock));
    ctx->lock.owner.proto      = CHIMERA_CLAIM_PROTO_NLM;
    ctx->lock.owner.client_key = nlm_owner_client_key(client->hostname);
    ctx->lock.owner.owner_lo   = nlm_owner_owner_lo(entry->oh, entry->oh_len, entry->svid);
    ctx->lock.type             = entry->unlock ? CHIMERA_VFS_LOCK_UNLOCK : entry->exclusive ? CHIMERA_VFS_LOCK_WRITE :
        CHIMERA_VFS_LOCK_READ;
    ctx->lock.whence = SEEK_SET;
    ctx->lock.offset = entry->offset;
    ctx->lock.length = NLM_POSIX_LEN_TO_VFS(entry->length);
    /* Existing NLM geometry saturates overflowing intervals to EOF. */
    if (ctx->lock.length > UINT64_MAX - ctx->lock.offset) {
        ctx->lock.length = UINT64_MAX;
    }
    ctx->lock.wait           = ctx->block;
    ctx->lock.fail_on_recall = !ctx->block;
    ctx->lock.on_wait        = chimera_nfs_nlm4_lock_blocked_cb;
    ctx->lock.wait_private   = ctx;
    memcpy(ctx->vfh, vfh, vfh_len);
    ctx->vfh_len         = vfh_len;
    ctx->lock.generation = shared->nlm_state.stopping ? 0 :
        chimera_vfs_lock_domain_admit_fh(client->domain, vfh, vfh_len, &ctx->lock.owner);
    if (!ctx->lock.generation) {
        evpl_mutex_unlock(&shared->nlm_state.mutex);
        nlm_lock_entry_free(entry);
        free(ctx);
        nlm4_send_res(shared, evpl, conn, encoding, &args->cookie, NLM4_DENIED_NOLOCKS, proc);
        return;
    }
    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &nlm_system_cred);
    if (entry->unlock) {
        struct nlm_lock_entry *held;
        DL_FOREACH(client->locks, held)
        {
            if (!held->pending && nlm_same_owner_file(held, entry)) {
                ctx->borrowed = held->handle;
                chimera_vfs_dup_handle(thread->vfs_thread, ctx->borrowed);
                break;
            }
        }
    }
    entry->lock_index = ctx->borrowed ?
        chimera_vfs_compound_add_lock_change(compound, client->domain, ctx->borrowed, &ctx->lock) :
        chimera_vfs_compound_add_lock_change_fh(compound, client->domain, vfh, vfh_len, &ctx->lock);
    entry->compound = compound;
    DL_APPEND(client->locks, entry);
    thread->nlm_active++;
    evpl_mutex_unlock(&shared->nlm_state.mutex);

    /* Associate this connection with the client so the disconnect handler
     * knows whose locks to release.  Increment conn_count once per conn
     * (only on the first LOCK that sets private_data for this connection). */
    if (!entry->unlock && !evpl_rpc2_conn_get_private_data(conn)) {
        evpl_rpc2_conn_set_private_data(conn, client);
        evpl_mutex_lock(&shared->nlm_state.mutex);
        client->conn_count++;
        evpl_mutex_unlock(&shared->nlm_state.mutex);
    }

    chimera_frontend_compound_submit(compound, chimera_nfs_nlm4_lock_complete, ctx);
} /* chimera_nfs_nlm4_do_lock */

void
chimera_nfs_nlm4_lock(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_lockargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    chimera_nfs_nlm4_do_lock(evpl, conn, cred, args, encoding, private_data,
                             false, 2);
} /* chimera_nfs_nlm4_lock */

static void
chimera_nfs_nlm4_do_cancel(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_cancargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data,
    int                        proc)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nlm_client                *client;
    struct nlm_lock_entry            *entry, *match;
    char                              safe_hostname[LM_MAXSTRLEN + 1];
    size_t                            hn_len;
    uint64_t                          want_length;

    chimera_nfs_debug("NLM CANCEL: caller='%.*s' fh_len=%u offset=%lu len=%lu",
                      (int) args->alock.caller_name.len, args->alock.caller_name.str,
                      (unsigned) args->alock.fh.len,
                      (unsigned long) args->alock.l_offset,
                      (unsigned long) args->alock.l_len);

    hn_len = args->alock.caller_name.len < LM_MAXSTRLEN
             ? args->alock.caller_name.len : LM_MAXSTRLEN;
    memcpy(safe_hostname, args->alock.caller_name.str, hn_len);
    safe_hostname[hn_len] = '\0';

    want_length = nlm_wire_len_to_posix(args->alock.l_len);

    evpl_mutex_lock(&shared->nlm_state.mutex);
    HASH_FIND_STR(shared->nlm_state.clients, safe_hostname, client);
    match = NULL;
    if (client) {
        DL_FOREACH(client->locks, entry)
        {
            /* Only pending entries can be cancelled; granted entries
             * require UNLOCK to release.  CANCEL identifies the specific
             * outstanding blocked LOCK request, which per RFC 1813 (struct
             * nlm4_cancargs / nlm4_lock) is keyed by its mode as well as
             * owner+range, so match the exact
             * (oh, svid, fh, range, exclusive) tuple. */
            if (!entry->pending || entry->unlock || entry->reaped) {
                continue;
            }
            if (entry->oh_len    != args->alock.oh.len  ||
                entry->fh_len    != args->alock.fh.len  ||
                entry->svid      != args->alock.svid    ||
                entry->offset    != args->alock.l_offset ||
                entry->length    != want_length          ||
                entry->exclusive != args->exclusive      ||
                memcmp(entry->oh, args->alock.oh.data, entry->oh_len) != 0 ||
                memcmp(entry->fh, args->alock.fh.data, entry->fh_len) != 0) {
                continue;
            }
            match = entry;
            break;
        }
    }

    if (match && match->lock_index >= 0) {
        chimera_vfs_compound_lock_cancel(match->compound, match->lock_index);
    }
    evpl_mutex_unlock(&shared->nlm_state.mutex);
    /* NLM CANCEL is idempotent, including the grant-won race. Only UNLOCK
     * removes an already accepted lock. Cancellation never calls back inline. */
    nlm4_send_res(shared, evpl, conn, encoding, &args->cookie, NLM4_GRANTED, proc);
} /* chimera_nfs_nlm4_do_cancel */

void
chimera_nfs_nlm4_cancel(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_cancargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    chimera_nfs_nlm4_do_cancel(evpl, conn, cred, args, encoding, private_data,
                               3);
} /* chimera_nfs_nlm4_cancel */

static void
chimera_nfs_nlm4_do_unlock(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_unlockargs    *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data,
    int                        proc)
{
    struct nlm4_lockargs lock = { 0 };

    lock.cookie = args->cookie;
    lock.alock  = args->alock;
    chimera_nfs_nlm4_do_lock(evpl, conn, cred, &lock, encoding, private_data, true, proc);
} /* chimera_nfs_nlm4_do_unlock */

void
chimera_nfs_nlm4_unlock(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_unlockargs    *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    chimera_nfs_nlm4_do_unlock(evpl, conn, cred, args, encoding, private_data, 4);
} /* chimera_nfs_nlm4_unlock */

void
chimera_nfs_nlm4_granted(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_testargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nlm4_res                   res;
    int                               rc;

    chimera_nfs_debug("NLM GRANTED received");

    res.cookie.len  = args->cookie.len;
    res.cookie.data = args->cookie.data;
    res.stat        = NLM4_GRANTED;

    rc = shared->nlm_v4.send_reply_NLMPROC4_GRANTED(evpl, NULL, &res, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM GRANTED reply");
} /* chimera_nfs_nlm4_granted */

void
chimera_nfs_nlm4_test_msg(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_testargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    chimera_nfs_debug("NLM TEST_MSG received");

    rc = shared->nlm_v4.send_reply_NLMPROC4_TEST_MSG(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM TEST_MSG reply");
    /* Process async: result delivered via send_call_NLMPROC4_TEST_RES */
    chimera_nfs_nlm4_do_test(evpl, conn, cred, args, NULL, private_data, 16);
} /* chimera_nfs_nlm4_test_msg */

void
chimera_nfs_nlm4_lock_msg(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_lockargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    chimera_nfs_debug("NLM LOCK_MSG received");

    rc = shared->nlm_v4.send_reply_NLMPROC4_LOCK_MSG(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM LOCK_MSG reply");
    /* Process async: result delivered via send_call_NLMPROC4_LOCK_RES */
    chimera_nfs_nlm4_do_lock(evpl, conn, cred, args, NULL, private_data, false, 17);
} /* chimera_nfs_nlm4_lock_msg */

void
chimera_nfs_nlm4_cancel_msg(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_cancargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    chimera_nfs_debug("NLM CANCEL_MSG received");

    rc = shared->nlm_v4.send_reply_NLMPROC4_CANCEL_MSG(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM CANCEL_MSG reply");

    /* RFC 1813 gives the asynchronous form the same meaning as the
     * synchronous one, so it does the same work -- withdrawing a queued
     * blocking LOCK -- and reports the outcome through NLMPROC4_CANCEL_RES.
     * Acknowledging without cancelling would leave the client's blocking
     * LOCK queued for a range it has given up on. */
    chimera_nfs_nlm4_do_cancel(evpl, conn, cred, args, NULL, private_data, 19);
} /* chimera_nfs_nlm4_cancel_msg */

void
chimera_nfs_nlm4_unlock_msg(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_unlockargs    *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    chimera_nfs_debug("NLM UNLOCK_MSG received");

    rc = shared->nlm_v4.send_reply_NLMPROC4_UNLOCK_MSG(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM UNLOCK_MSG reply");
    /* Process async: result delivered via send_call_NLMPROC4_UNLOCK_RES */
    chimera_nfs_nlm4_do_unlock(evpl, conn, cred, args, NULL, private_data, 18);
} /* chimera_nfs_nlm4_unlock_msg */

void
chimera_nfs_nlm4_granted_msg(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_testargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    chimera_nfs_debug("NLM GRANTED_MSG received");

    rc = shared->nlm_v4.send_reply_NLMPROC4_GRANTED_MSG(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM GRANTED_MSG reply");
} /* chimera_nfs_nlm4_granted_msg */

void
chimera_nfs_nlm4_test_res(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_testres       *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    chimera_nfs_debug("NLM TEST_RES received");

    rc = shared->nlm_v4.send_reply_NLMPROC4_TEST_RES(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM TEST_RES reply");
} /* chimera_nfs_nlm4_test_res */

void
chimera_nfs_nlm4_lock_res(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_res           *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    chimera_nfs_debug("NLM LOCK_RES received");

    rc = shared->nlm_v4.send_reply_NLMPROC4_LOCK_RES(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM LOCK_RES reply");
} /* chimera_nfs_nlm4_lock_res */

void
chimera_nfs_nlm4_cancel_res(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_res           *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    chimera_nfs_debug("NLM CANCEL_RES received");

    rc = shared->nlm_v4.send_reply_NLMPROC4_CANCEL_RES(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM CANCEL_RES reply");
} /* chimera_nfs_nlm4_cancel_res */

void
chimera_nfs_nlm4_unlock_res(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_res           *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    chimera_nfs_debug("NLM UNLOCK_RES received");

    rc = shared->nlm_v4.send_reply_NLMPROC4_UNLOCK_RES(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM UNLOCK_RES reply");
} /* chimera_nfs_nlm4_unlock_res */

void
chimera_nfs_nlm4_granted_res(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_res           *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    chimera_nfs_debug("NLM GRANTED_RES received");

    rc = shared->nlm_v4.send_reply_NLMPROC4_GRANTED_RES(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM GRANTED_RES reply");
} /* chimera_nfs_nlm4_granted_res */

void
chimera_nfs_nlm4_reserved_16(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    chimera_nfs_debug("Received NLM reserved procedure 16");
    rc = shared->nlm_v4.send_reply_NLMPROC4_RESERVED_16(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM reserved 16 reply");
} /* chimera_nfs_nlm4_reserved_16 */

void
chimera_nfs_nlm4_reserved_17(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    chimera_nfs_debug("Received NLM reserved procedure 17");
    rc = shared->nlm_v4.send_reply_NLMPROC4_RESERVED_17(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM reserved 17 reply");
} /* chimera_nfs_nlm4_reserved_17 */

void
chimera_nfs_nlm4_reserved_18(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    chimera_nfs_debug("Received NLM reserved procedure 18");
    rc = shared->nlm_v4.send_reply_NLMPROC4_RESERVED_18(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM reserved 18 reply");
} /* chimera_nfs_nlm4_reserved_18 */

void
chimera_nfs_nlm4_reserved_19(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    int                               rc;

    chimera_nfs_debug("Received NLM reserved procedure 19");
    rc = shared->nlm_v4.send_reply_NLMPROC4_RESERVED_19(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM reserved 19 reply");
} /* chimera_nfs_nlm4_reserved_19 */

void
chimera_nfs_nlm4_share(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_shareargs     *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nlm4_shareres              res;
    int                               rc;

    chimera_nfs_debug("NLM SHARE: caller='%.*s' mode=%d access=%d",
                      (int) args->share.caller_name.len, args->share.caller_name.str,
                      (int) args->share.mode, (int) args->share.access);

    /* DOS share modes are advisory and rarely used by modern clients.
     * Accept all share requests without enforcement. */
    res.cookie.len  = args->cookie.len;
    res.cookie.data = args->cookie.data;
    res.stat        = NLM4_GRANTED;
    res.sequence    = 0;

    rc = shared->nlm_v4.send_reply_NLMPROC4_SHARE(evpl, NULL, &res, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM SHARE reply");
} /* chimera_nfs_nlm4_share */

void
chimera_nfs_nlm4_unshare(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_shareargs     *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nlm4_shareres              res;
    int                               rc;

    chimera_nfs_debug("NLM UNSHARE: caller='%.*s'",
                      (int) args->share.caller_name.len, args->share.caller_name.str);

    res.cookie.len  = args->cookie.len;
    res.cookie.data = args->cookie.data;
    res.stat        = NLM4_GRANTED;
    res.sequence    = 0;

    rc = shared->nlm_v4.send_reply_NLMPROC4_UNSHARE(evpl, NULL, &res, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM UNSHARE reply");
} /* chimera_nfs_nlm4_unshare */

void
chimera_nfs_nlm4_nm_lock(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_lockargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    chimera_nfs_nlm4_do_lock(evpl, conn, cred, args, encoding, private_data,
                             true, 22);
} /* chimera_nfs_nlm4_nm_lock */

void
chimera_nfs_nlm4_free_all(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_notify        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nlm_client                *client;
    struct chimera_vfs_cred           anon_cred;
    char                              safe_hostname[LM_MAXSTRLEN + 1];
    size_t                            hn_len;
    int                               rc;

    /* NUL-terminate the XDR name for use as a C-string hash key */
    hn_len = args->name.len < LM_MAXSTRLEN ? args->name.len : LM_MAXSTRLEN;
    memcpy(safe_hostname, args->name.str, hn_len);
    safe_hostname[hn_len] = '\0';

    chimera_nfs_debug("Received NLM FREE_ALL for client '%s'", safe_hostname);
    /* Locate under the lock, release outside it (release_all takes the lock
     * itself and pumps the VFS queue, which can re-enter the NLM callback). */
    evpl_mutex_lock(&shared->nlm_state.mutex);
    HASH_FIND_STR(shared->nlm_state.clients, safe_hostname, client);
    evpl_mutex_unlock(&shared->nlm_state.mutex);
    if (client) {
        chimera_vfs_cred_init_anonymous(&anon_cred,
                                        CHIMERA_VFS_ANON_UID,
                                        CHIMERA_VFS_ANON_GID);
        nlm_client_release_all_locks(&shared->nlm_state, client,
                                     thread->vfs_thread,
                                     thread->vfs->vfs_state,
                                     &anon_cred);
    }
    if (client) {
        nlm_state_remove_client_file(&shared->nlm_state, safe_hostname);
        /* All of this client's locks are gone; stop monitoring it for reboot. */
        nsm_unmonitor(thread, safe_hostname);
    }

    rc = shared->nlm_v4.send_reply_NLMPROC4_FREE_ALL(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM FREE_ALL reply");
} /* chimera_nfs_nlm4_free_all */
