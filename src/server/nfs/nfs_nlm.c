// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <stdlib.h>
#include <pthread.h>
#include <xxhash.h>

#include "nfs_common.h"
#include "nfs_internal.h"
#include "nfs_nlm.h"
#include "nfs_nlm_state.h"
#include "nfs_nsm.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_state.h"

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
 * zero-byte range).  Translate when handing a length to a VFS lease/probe. */
#define NLM_POSIX_LEN_TO_VFS(l) ((l) == 0 ? UINT64_MAX : (l))

/* Map NLM caller_name (hostname) to a vfs_state owner.client_key. */
static inline uint64_t
nlm_owner_client_key(const char *hostname)
{
    return XXH3_64bits(hostname, strlen(hostname));
} /* nlm_owner_client_key */

/* Map NLM (oh_bytes, svid) to a vfs_state owner.owner_lo.  Two LOCK ops
 * with the same (hostname, oh, svid) tuple will produce the same owner
 * identity and therefore coalesce in the vfs_state conflict matrix. */
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

/* -------------------------------------------------------------------------
 * Helper: send a simple nlm4_res reply
 * ---------------------------------------------------------------------- */
static void
nlm4_send_res(
    struct chimera_server_nfs_shared *shared,
    struct evpl                      *evpl,
    struct evpl_rpc2_encoding        *encoding,
    const xdr_opaque                 *cookie,
    nlm4_stats                        stat,
    int                               proc)
{
    struct nlm4_res res;
    int             rc;

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
        case 22:
            rc = shared->nlm_v4.send_reply_NLMPROC4_NM_LOCK(evpl, NULL, &res, encoding);
            break;
        default:
            rc = 0;
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
    struct chimera_vfs_open_handle   *handle;
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

static void
chimera_nfs_nlm4_test_open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nlm_test_ctx              *ctx       = private_data;
    struct chimera_server_nfs_thread *thread    = ctx->thread;
    struct chimera_server_nfs_shared *shared    = thread->shared;
    struct evpl                      *evpl      = ctx->evpl;
    struct evpl_rpc2_encoding        *encoding  = ctx->encoding;
    struct chimera_vfs_state         *vfs_state = thread->vfs->vfs_state;
    struct chimera_vfs_file_state    *file_state;
    struct chimera_vfs_lease          probe;
    struct chimera_vfs_lease         *conflict = NULL;
    enum chimera_vfs_lease_result     result;
    struct nlm4_testres               res;
    int                               rc;

    res.cookie.len  = ctx->cookie.len;
    res.cookie.data = ctx->cookie.data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs_debug("NLM TEST open failed: error %d -> NLM4_STALE_FH", error_code);
        res.test_stat.stat = NLM4_STALE_FH;
        goto send_reply;
    }

    file_state = chimera_vfs_state_get(vfs_state,
                                       handle->fh, handle->fh_len,
                                       handle->fh_hash, false);

    if (!file_state) {
        /* No state on this file means no leases held — TEST grants. */
        chimera_vfs_release(thread->vfs_thread, handle);
        res.test_stat.stat = NLM4_GRANTED;
        goto send_reply;
    }

    memset(&probe, 0, sizeof(probe));
    probe.kind         = CHIMERA_VFS_LEASE_RANGE;
    probe.mode.granted = ctx->exclusive
                             ? CHIMERA_VFS_LEASE_MODE_W
                             : CHIMERA_VFS_LEASE_MODE_R;
    probe.offset           = ctx->offset;
    probe.length           = NLM_POSIX_LEN_TO_VFS(ctx->length);
    probe.owner.protocol   = CHIMERA_VFS_LEASE_PROTO_NLM;
    probe.owner.client_key = nlm_owner_client_key(ctx->caller_name);
    probe.owner.owner_lo   = nlm_owner_owner_lo(ctx->oh, ctx->oh_len, ctx->svid);

    result = chimera_vfs_lease_test(file_state, &probe, &conflict);

    if (result == CHIMERA_VFS_LEASE_GRANTED) {
        res.test_stat.stat = NLM4_GRANTED;
    } else {
        uint64_t conflict_len;
        res.test_stat.stat             = NLM4_DENIED;
        res.test_stat.holder.exclusive = conflict && (conflict->mode.granted &
                                                      CHIMERA_VFS_LEASE_MODE_W);
        res.test_stat.holder.svid     = 0;  /* not exposed by vfs_state */
        res.test_stat.holder.oh.len   = 0;
        res.test_stat.holder.oh.data  = NULL;
        res.test_stat.holder.l_offset = conflict ? conflict->offset : 0;
        /* conflict->length already uses UINT64_MAX for a to-EOF holder, which
         * is exactly the NLM to-EOF sentinel. */
        conflict_len               = conflict ? conflict->length : UINT64_MAX;
        res.test_stat.holder.l_len = conflict_len;
    }

    chimera_vfs_state_put(vfs_state, file_state);
    chimera_vfs_release(thread->vfs_thread, handle);

 send_reply:
    chimera_nfs_debug("NLM TEST cb: stat=%d", res.test_stat.stat);

    if (ctx->proc == 16) {
        shared->nlm_v4.send_call_NLMPROC4_TEST_RES(&shared->nlm_v4.rpc2, evpl,
                                                   ctx->conn, NULL, &res, 0, 0, NULL, 0, 0,
                                                   NULL, NULL);
    } else {
        rc = shared->nlm_v4.send_reply_NLMPROC4_TEST(evpl, NULL, &res, encoding);
        chimera_nfs_abort_if(rc, "Failed to send NLM TEST reply");
    }
    free(ctx);
} /* chimera_nfs_nlm4_test_open_cb */

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
    int                               proc;    /* 2=LOCK, 22=NM_LOCK */
};

static void
chimera_nfs_nlm4_lock_acquire_cb(
    enum chimera_vfs_lease_result result,
    struct chimera_vfs_lease     *granted,
    struct chimera_vfs_lease     *conflict,
    void                         *private_data)
{
    struct nlm_lock_ctx              *ctx       = private_data;
    struct chimera_server_nfs_thread *thread    = ctx->thread;
    struct chimera_server_nfs_shared *shared    = thread->shared;
    struct evpl                      *evpl      = ctx->evpl;
    struct evpl_rpc2_encoding        *encoding  = ctx->encoding;
    struct chimera_vfs_state         *vfs_state = thread->vfs->vfs_state;
    struct nlm_lock_entry            *entry     = ctx->entry;
    struct nlm4_res                   res;
    int                               rc = 0;

    (void) conflict;
    (void) granted;

    res.cookie.len  = ctx->cookie.len;
    res.cookie.data = ctx->cookie.data;

    if (result == CHIMERA_VFS_LEASE_GRANTED) {
        pthread_mutex_lock(&shared->nlm_state.mutex);
        entry->pending        = false;
        entry->lease_inserted = true;
        pthread_mutex_unlock(&shared->nlm_state.mutex);
        res.stat = NLM4_GRANTED;

        /* Monitor the lock holder so we can SM_NOTIFY it to reclaim if we
        * reboot.  NM_LOCK is explicitly non-monitored, so it opts out. */
        if (!ctx->nm_lock) {
            char addr[80];

            nlm_conn_peer_addr(ctx->conn, addr, sizeof(addr));
            nsm_monitor(thread, ctx->client->hostname, addr);
        }
    } else {
        /* DENIED or wait=false-with-BREAKING: drop the entry. */
        pthread_mutex_lock(&shared->nlm_state.mutex);
        DL_DELETE(ctx->client->locks, entry);
        pthread_mutex_unlock(&shared->nlm_state.mutex);
        chimera_vfs_state_put(vfs_state, entry->file_state);
        entry->file_state = NULL;
        chimera_vfs_release(thread->vfs_thread, entry->handle);
        nlm_lock_entry_free(entry);
        res.stat = NLM4_DENIED;
    }

    chimera_nfs_debug("NLM LOCK cb: result=%d stat=%d block=%d", result, res.stat, ctx->block);

    switch (ctx->proc) {
        case 2:
            rc = shared->nlm_v4.send_reply_NLMPROC4_LOCK(evpl, NULL, &res, encoding);
            break;
        case 17:
            shared->nlm_v4.send_call_NLMPROC4_LOCK_RES(&shared->nlm_v4.rpc2, evpl, ctx->conn, NULL, &res, 0, 0, NULL, 0,
                                                       0, NULL,
                                                       NULL);
            break;
        default:
            rc = shared->nlm_v4.send_reply_NLMPROC4_NM_LOCK(evpl, NULL, &res, encoding);
            break;
    } /* switch */
    chimera_nfs_abort_if(rc, "Failed to send NLM LOCK reply");

    free(ctx);
} /* chimera_nfs_nlm4_lock_acquire_cb */

static void
chimera_nfs_nlm4_lock_open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nlm_lock_ctx              *ctx       = private_data;
    struct chimera_server_nfs_thread *thread    = ctx->thread;
    struct chimera_server_nfs_shared *shared    = thread->shared;
    struct evpl                      *evpl      = ctx->evpl;
    struct evpl_rpc2_encoding        *encoding  = ctx->encoding;
    struct chimera_vfs_state         *vfs_state = thread->vfs->vfs_state;
    struct nlm_lock_entry            *entry     = ctx->entry;
    struct nlm4_res                   res;
    int                               rc = 0;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs_debug("NLM LOCK open failed: error %d -> NLM4_STALE_FH", error_code);
        /* Remove the pending sentinel that was pre-inserted in do_lock */
        pthread_mutex_lock(&shared->nlm_state.mutex);
        DL_DELETE(ctx->client->locks, entry);
        pthread_mutex_unlock(&shared->nlm_state.mutex);
        nlm_lock_entry_free(entry);
        res.cookie.len  = ctx->cookie.len;
        res.cookie.data = ctx->cookie.data;
        res.stat        = NLM4_STALE_FH;
        switch (ctx->proc) {
            case 2:
                rc = shared->nlm_v4.send_reply_NLMPROC4_LOCK(evpl, NULL, &res, encoding);
                break;
            case 17:
                shared->nlm_v4.send_call_NLMPROC4_LOCK_RES(&shared->nlm_v4.rpc2, evpl, ctx->conn, NULL, &res, 0, 0, NULL
                                                           , 0, 0,
                                                           NULL, NULL);
                break;
            default:
                rc = shared->nlm_v4.send_reply_NLMPROC4_NM_LOCK(evpl, NULL, &res, encoding);
                break;
        } /* switch */
        chimera_nfs_abort_if(rc, "Failed to send NLM LOCK reply");
        free(ctx);
        return;
    }

    entry->handle = handle;

    entry->file_state = chimera_vfs_state_get(vfs_state,
                                              handle->fh, handle->fh_len,
                                              handle->fh_hash, true);

    if (!entry->file_state) {
        pthread_mutex_lock(&shared->nlm_state.mutex);
        DL_DELETE(ctx->client->locks, entry);
        pthread_mutex_unlock(&shared->nlm_state.mutex);
        chimera_vfs_release(thread->vfs_thread, handle);
        nlm_lock_entry_free(entry);
        res.cookie.len  = ctx->cookie.len;
        res.cookie.data = ctx->cookie.data;
        res.stat        = NLM4_DENIED_NOLOCKS;
        switch (ctx->proc) {
            case 2:
                rc = shared->nlm_v4.send_reply_NLMPROC4_LOCK(evpl, NULL, &res, encoding);
                break;
            case 17:
                shared->nlm_v4.send_call_NLMPROC4_LOCK_RES(&shared->nlm_v4.rpc2, evpl, ctx->conn, NULL, &res, 0, 0, NULL
                                                           , 0, 0,
                                                           NULL, NULL);
                break;
            default:
                rc = shared->nlm_v4.send_reply_NLMPROC4_NM_LOCK(evpl, NULL, &res, encoding);
                break;
        } /* switch */
        chimera_nfs_abort_if(rc, "Failed to send NLM LOCK OOM reply");
        free(ctx);
        return;
    }

    entry->lease.kind         = CHIMERA_VFS_LEASE_RANGE;
    entry->lease.mode.granted = entry->exclusive
                                    ? CHIMERA_VFS_LEASE_MODE_W
                                    : CHIMERA_VFS_LEASE_MODE_R;
    entry->lease.offset           = entry->offset;
    entry->lease.length           = NLM_POSIX_LEN_TO_VFS(entry->length);
    entry->lease.owner.protocol   = CHIMERA_VFS_LEASE_PROTO_NLM;
    entry->lease.owner.client_key = nlm_owner_client_key(ctx->client->hostname);
    entry->lease.owner.owner_lo   = nlm_owner_owner_lo(entry->oh, entry->oh_len,
                                                       entry->svid);

    /* wait=ctx->block: blocking LOCK rides out cross-protocol breaks;
     * non-blocking LOCK returns DENIED on any conflict.  Note: even with
     * wait=true, same-protocol byte-range overlap returns DENIED
     * synchronously — BREAKING only applies to caching leases. */
    chimera_vfs_lease_acquire(vfs_state, entry->file_state,
                              &entry->lease, &entry->ticket, ctx->block,
                              chimera_nfs_nlm4_lock_acquire_cb, ctx);
} /* chimera_nfs_nlm4_lock_open_cb */

/* -------------------------------------------------------------------------
 * UNLOCK procedure callbacks
 * ---------------------------------------------------------------------- */

/* UNLOCK is fully synchronous in vfs_state mode — lease_release and
 * chimera_vfs_release are both sync — so no separate ctx/cb is needed. */

/* =========================================================================
 * Public procedure handlers
 * ====================================================================== */

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

    chimera_nfs_debug("NLM TEST: caller='%.*s' fh_len=%u offset=%lu len=%lu exclusive=%d",
                      (int) args->alock.caller_name.len, args->alock.caller_name.str,
                      (unsigned) args->alock.fh.len,
                      (unsigned long) args->alock.l_offset,
                      (unsigned long) args->alock.l_len,
                      (int) args->exclusive);

    ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        struct nlm4_testres err_res;
        err_res.cookie.len     = args->cookie.len;
        err_res.cookie.data    = args->cookie.data;
        err_res.test_stat.stat = NLM4_DENIED_NOLOCKS;
        if (proc == 16) {
            shared->nlm_v4.send_call_NLMPROC4_TEST_RES(&shared->nlm_v4.rpc2, evpl, conn, NULL, &err_res, 0, 0, NULL, 0,
                                                       0, NULL,
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
    ctx->length    = NLM_TO_POSIX_LEN(args->alock.l_len);
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

    /* Conflict detection is delegated to vfs_state — the test_open_cb
     * does the lookup and probe synchronously once the FH is validated. */
    chimera_vfs_open_fh(thread->vfs_thread, NULL,
                        args->alock.fh.data,
                        args->alock.fh.len,
                        CHIMERA_VFS_OPEN_INFERRED,
                        chimera_nfs_nlm4_test_open_cb,
                        ctx);
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
    struct nlm_lock_entry            *entry;
    struct nlm_lock_ctx              *ctx;
    struct nlm_client                *client;
    struct nlm4_res                   res;
    char                              safe_hostname[LM_MAXSTRLEN + 1];
    size_t                            hn_len;
    int                               rc = 0;

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

    /* Build the in-flight lock entry (handle filled in by open callback) */
    entry = nlm_lock_entry_alloc();
    if (!entry) {
        nlm4_send_res(shared, evpl, encoding, &args->cookie, NLM4_DENIED_NOLOCKS, proc);
        return;
    }
    entry->fh_len = args->alock.fh.len < NFS4_FHSIZE ? args->alock.fh.len : NFS4_FHSIZE;
    memcpy(entry->fh, args->alock.fh.data, entry->fh_len);
    entry->oh_len = args->alock.oh.len < LM_MAXSTRLEN ? args->alock.oh.len : LM_MAXSTRLEN;
    memcpy(entry->oh, args->alock.oh.data, entry->oh_len);
    entry->svid      = args->alock.svid;
    entry->offset    = args->alock.l_offset;
    entry->length    = NLM_TO_POSIX_LEN(args->alock.l_len);
    entry->exclusive = args->exclusive;
    entry->handle    = NULL;
    entry->pending   = true;

    ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        nlm_lock_entry_free(entry);
        nlm4_send_res(shared, evpl, encoding, &args->cookie, NLM4_DENIED_NOLOCKS, proc);
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

    if (args->cookie.len > 0 && args->cookie.len <= LM_MAXSTRLEN) {
        ctx->cookie.len  = args->cookie.len;
        ctx->cookie.data = ctx->cookie_buf;
        memcpy(ctx->cookie_buf, args->cookie.data, args->cookie.len);
    }

    /* Single mutex section: grace check, conflict check, client lookup, and
    * pre-insertion of the pending sentinel -- all under one lock to eliminate
    * the TOCTOU window between conflict detection and VFS lock acquisition.
    * The pending entry is visible to concurrent LOCK requests immediately,
    * so a second client cannot slip through while this one is in-flight. */
    pthread_mutex_lock(&shared->nlm_state.mutex);

    /* Grace period check: only reclaim locks are accepted during grace */
    if (nlm_state_in_grace(&shared->nlm_state) && !args->reclaim) {
        pthread_mutex_unlock(&shared->nlm_state.mutex);
        chimera_nfs_debug("NLM LOCK: rejected -> server in grace period");
        res.cookie.len  = ctx->cookie.len;
        res.cookie.data = ctx->cookie.data;
        res.stat        = NLM4_DENIED_GRACE_PERIOD;
        switch (proc) {
            case 2:
                rc = shared->nlm_v4.send_reply_NLMPROC4_LOCK(evpl, NULL, &res, encoding);
                break;
            case 17:
                shared->nlm_v4.send_call_NLMPROC4_LOCK_RES(&shared->nlm_v4.rpc2, evpl, conn, NULL, &res, 0, 0, NULL, 0,
                                                           0, NULL,
                                                           NULL);
                break;
            default:
                rc = shared->nlm_v4.send_reply_NLMPROC4_NM_LOCK(evpl, NULL, &res, encoding);
                break;
        } /* switch */
        chimera_nfs_abort_if(rc, "Failed to send NLM LOCK grace reply");
        nlm_lock_entry_free(entry);
        free(ctx);
        return;
    }

    /* Look up or create per-client state */
    client = nlm_client_lookup_or_create(&shared->nlm_state, safe_hostname);
    if (!client) {
        pthread_mutex_unlock(&shared->nlm_state.mutex);
        nlm_lock_entry_free(entry);
        free(ctx);
        nlm4_send_res(shared, evpl, encoding, &args->cookie, NLM4_DENIED_NOLOCKS, proc);
        return;
    }
    ctx->client = client;

    /* Conflict detection is delegated to vfs_state — handled inside
     * chimera_nfs_nlm4_lock_open_cb / lock_acquire_cb. */

    /* Reject if an identical confirmed lock already exists for this owner.
     * Two simultaneous LOCK requests from the same owner could both reach
     * vfs_state before either has been linked; the in-flight `pending`
     * sentinel inserted below catches that, but we also short-circuit
     * idempotent re-LOCKs of an already-held range to NLM4_GRANTED. */
    if (nlm_client_find_lock(client,
                             entry->oh, entry->oh_len,
                             entry->svid,
                             entry->fh, entry->fh_len,
                             entry->offset, entry->length)) {
        pthread_mutex_unlock(&shared->nlm_state.mutex);
        chimera_nfs_debug("NLM LOCK: duplicate lock request -> NLM4_GRANTED (idempotent)");
        res.cookie.len  = ctx->cookie.len;
        res.cookie.data = ctx->cookie.data;
        res.stat        = NLM4_GRANTED;
        switch (ctx->proc) {
            case 2:
                rc = shared->nlm_v4.send_reply_NLMPROC4_LOCK(evpl, NULL, &res, encoding);
                break;
            case 17:
                shared->nlm_v4.send_call_NLMPROC4_LOCK_RES(&shared->nlm_v4.rpc2, evpl, ctx->conn, NULL, &res, 0, 0, NULL
                                                           , 0, 0,
                                                           NULL, NULL);
                break;
            default:
                rc = shared->nlm_v4.send_reply_NLMPROC4_NM_LOCK(evpl, NULL, &res, encoding);
                break;
        } /* switch */
        chimera_nfs_abort_if(rc, "Failed to send NLM LOCK duplicate reply");
        nlm_lock_entry_free(entry);
        free(ctx);
        return;
    }

    /* Pre-insert the pending entry so concurrent requests see it immediately */
    DL_APPEND(client->locks, entry);

    pthread_mutex_unlock(&shared->nlm_state.mutex);

    /* Associate this connection with the client so the disconnect handler
     * knows whose locks to release.  Increment conn_count once per conn
     * (only on the first LOCK that sets private_data for this connection). */
    if (!evpl_rpc2_conn_get_private_data(conn)) {
        evpl_rpc2_conn_set_private_data(conn, client);
        pthread_mutex_lock(&shared->nlm_state.mutex);
        client->conn_count++;
        pthread_mutex_unlock(&shared->nlm_state.mutex);
    }

    chimera_vfs_open_fh(thread->vfs_thread, NULL,
                        args->alock.fh.data,
                        args->alock.fh.len,
                        CHIMERA_VFS_OPEN_INFERRED,
                        chimera_nfs_nlm4_lock_open_cb,
                        ctx);
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

void
chimera_nfs_nlm4_cancel(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct nlm4_cancargs      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread    = private_data;
    struct chimera_server_nfs_shared *shared    = thread->shared;
    struct chimera_vfs_state         *vfs_state = thread->vfs->vfs_state;
    struct nlm_client                *client;
    struct nlm_lock_entry            *entry, *match;
    struct nlm4_res                   res;
    char                              safe_hostname[LM_MAXSTRLEN + 1];
    size_t                            hn_len;
    uint64_t                          want_length;
    bool                              cancelled       = false;
    void                             *ctx_for_lock_cb = NULL;
    int                               rc;

    chimera_nfs_debug("NLM CANCEL: caller='%.*s' fh_len=%u offset=%lu len=%lu",
                      (int) args->alock.caller_name.len, args->alock.caller_name.str,
                      (unsigned) args->alock.fh.len,
                      (unsigned long) args->alock.l_offset,
                      (unsigned long) args->alock.l_len);

    res.cookie.len  = args->cookie.len;
    res.cookie.data = args->cookie.data;
    res.stat        = NLM4_GRANTED;

    hn_len = args->alock.caller_name.len < LM_MAXSTRLEN
             ? args->alock.caller_name.len : LM_MAXSTRLEN;
    memcpy(safe_hostname, args->alock.caller_name.str, hn_len);
    safe_hostname[hn_len] = '\0';

    want_length = NLM_TO_POSIX_LEN(args->alock.l_len);

    pthread_mutex_lock(&shared->nlm_state.mutex);
    HASH_FIND_STR(shared->nlm_state.clients, safe_hostname, client);
    match = NULL;
    if (client) {
        DL_FOREACH(client->locks, entry)
        {
            /* Only pending entries can be cancelled; granted entries
             * require UNLOCK to release.  Match the exact (oh, svid,
             * fh, range) tuple. */
            if (!entry->pending) {
                continue;
            }
            if (entry->oh_len  != args->alock.oh.len  ||
                entry->fh_len  != args->alock.fh.len  ||
                entry->svid    != args->alock.svid    ||
                entry->offset  != args->alock.l_offset ||
                entry->length  != want_length          ||
                memcmp(entry->oh, args->alock.oh.data, entry->oh_len) != 0 ||
                memcmp(entry->fh, args->alock.fh.data, entry->fh_len) != 0) {
                continue;
            }
            match = entry;
            break;
        }
    }
    pthread_mutex_unlock(&shared->nlm_state.mutex);

    if (match && match->file_state) {
        /* The entry is queued inside vfs_state waiting on a break.  Try
         * to dequeue it; if we win the race against the in-flight cb,
         * synthesize a DENIED completion so the original LOCK reply is
         * still sent (the protocol layer's cb tears down the entry). */
        cancelled       = chimera_vfs_lease_acquire_cancel(vfs_state, &match->ticket);
        ctx_for_lock_cb = match->ticket.private_data;
    }

    rc = shared->nlm_v4.send_reply_NLMPROC4_CANCEL(evpl, NULL, &res, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM CANCEL reply");

    if (cancelled && ctx_for_lock_cb) {
        chimera_nfs_nlm4_lock_acquire_cb(CHIMERA_VFS_LEASE_DENIED, NULL, NULL,
                                         ctx_for_lock_cb);
    }
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
    struct chimera_server_nfs_thread *thread    = private_data;
    struct chimera_server_nfs_shared *shared    = thread->shared;
    struct chimera_vfs_state         *vfs_state = thread->vfs->vfs_state;
    struct nlm_client                *client;
    struct nlm_lock_entry            *entry;
    struct nlm4_res                   res;
    char                              safe_hostname[LM_MAXSTRLEN + 1];
    size_t                            hn_len;
    int                               rc;

    /* NUL-terminate the XDR caller_name for use as a C-string hash key */
    hn_len = args->alock.caller_name.len < LM_MAXSTRLEN
             ? args->alock.caller_name.len : LM_MAXSTRLEN;
    memcpy(safe_hostname, args->alock.caller_name.str, hn_len);
    safe_hostname[hn_len] = '\0';

    chimera_nfs_debug("NLM UNLOCK: caller='%s' fh_len=%u offset=%lu len=%lu",
                      safe_hostname,
                      (unsigned) args->alock.fh.len,
                      (unsigned long) args->alock.l_offset,
                      (unsigned long) args->alock.l_len);

    pthread_mutex_lock(&shared->nlm_state.mutex);
    HASH_FIND_STR(shared->nlm_state.clients, safe_hostname, client);
    if (!client) {
        /* Unknown client -- nothing to unlock */
        pthread_mutex_unlock(&shared->nlm_state.mutex);
        chimera_nfs_debug("NLM UNLOCK: unknown client '%s' -> NLM4_GRANTED", safe_hostname);
        res.cookie.len  = args->cookie.len;
        res.cookie.data = args->cookie.data;
        res.stat        = NLM4_GRANTED;
        if (proc == 18) {
            shared->nlm_v4.send_call_NLMPROC4_UNLOCK_RES(&shared->nlm_v4.rpc2, evpl, conn, NULL, &res, 0, 0, NULL, 0, 0,
                                                         NULL,
                                                         NULL);
        } else {
            rc = shared->nlm_v4.send_reply_NLMPROC4_UNLOCK(evpl, NULL, &res, encoding);
            chimera_nfs_abort_if(rc, "Failed to send NLM UNLOCK reply");
        }
        return;
    }

    entry = nlm_client_find_lock(client,
                                 args->alock.oh.data,
                                 args->alock.oh.len,
                                 args->alock.svid,
                                 args->alock.fh.data,
                                 args->alock.fh.len,
                                 args->alock.l_offset,
                                 NLM_TO_POSIX_LEN(args->alock.l_len));
    if (!entry) {
        pthread_mutex_unlock(&shared->nlm_state.mutex);
        chimera_nfs_debug("NLM UNLOCK: lock entry not found -> NLM4_GRANTED");
        /* Lock not found -- treat as already unlocked */
        res.cookie.len  = args->cookie.len;
        res.cookie.data = args->cookie.data;
        res.stat        = NLM4_GRANTED;
        if (proc == 18) {
            shared->nlm_v4.send_call_NLMPROC4_UNLOCK_RES(&shared->nlm_v4.rpc2, evpl, conn, NULL, &res, 0, 0, NULL, 0, 0,
                                                         NULL,
                                                         NULL);
        } else {
            rc = shared->nlm_v4.send_reply_NLMPROC4_UNLOCK(evpl, NULL, &res, encoding);
            chimera_nfs_abort_if(rc, "Failed to send NLM UNLOCK reply");
        }
        return;
    }

    /* Take exclusive ownership of the entry by removing it from the list
     * while still holding the mutex, preventing concurrent FREE_ALL or
     * disconnect handlers from freeing it before the release path runs. */
    DL_DELETE(client->locks, entry);
    pthread_mutex_unlock(&shared->nlm_state.mutex);

    /* Release the vfs_state lease (sync), put the file_state ref (sync),
     * and close the handle (sync).  RFC 1813 requires NLM4_GRANTED. */
    if (entry->lease_inserted) {
        chimera_vfs_lease_release(vfs_state, entry->file_state, &entry->lease);
        entry->lease_inserted = false;
    }
    if (entry->file_state) {
        chimera_vfs_state_put(vfs_state, entry->file_state);
        entry->file_state = NULL;
    }
    if (entry->handle) {
        chimera_vfs_release(thread->vfs_thread, entry->handle);
    }
    nlm_lock_entry_free(entry);

    res.cookie.len  = args->cookie.len;
    res.cookie.data = args->cookie.data;
    res.stat        = NLM4_GRANTED;

    if (proc == 18) {
        shared->nlm_v4.send_call_NLMPROC4_UNLOCK_RES(&shared->nlm_v4.rpc2, evpl, conn, NULL, &res, 0, 0, NULL, 0, 0,
                                                     NULL,
                                                     NULL);
    } else {
        rc = shared->nlm_v4.send_reply_NLMPROC4_UNLOCK(evpl, NULL, &res, encoding);
        chimera_nfs_abort_if(rc, "Failed to send NLM UNLOCK reply");
    }
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

    struct nlm4_res                   res;

    chimera_nfs_debug("NLM CANCEL_MSG received");

    rc = shared->nlm_v4.send_reply_NLMPROC4_CANCEL_MSG(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM CANCEL_MSG reply");
    /* No pending-lock queue, so there is nothing to cancel.  Reply granted. */
    res.cookie.len  = args->cookie.len;
    res.cookie.data = args->cookie.data;
    res.stat        = NLM4_GRANTED;
    shared->nlm_v4.send_call_NLMPROC4_CANCEL_RES(&shared->nlm_v4.rpc2, evpl, conn, NULL, &res, 0, 0, NULL, 0, 0, NULL,
                                                 NULL);
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
    pthread_mutex_lock(&shared->nlm_state.mutex);
    HASH_FIND_STR(shared->nlm_state.clients, safe_hostname, client);
    if (client) {
        chimera_vfs_cred_init_anonymous(&anon_cred,
                                        CHIMERA_VFS_ANON_UID,
                                        CHIMERA_VFS_ANON_GID);
        nlm_client_release_all_locks(&shared->nlm_state, client,
                                     thread->vfs_thread,
                                     thread->vfs->vfs_state,
                                     &anon_cred);
    }
    pthread_mutex_unlock(&shared->nlm_state.mutex);
    /* Remove on-disk state outside the mutex to avoid blocking I/O under lock */
    if (client) {
        nlm_state_remove_client_file(&shared->nlm_state, safe_hostname);
        /* All of this client's locks are gone; stop monitoring it for reboot. */
        nsm_unmonitor(thread, safe_hostname);
    }

    rc = shared->nlm_v4.send_reply_NLMPROC4_FREE_ALL(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM FREE_ALL reply");
} /* chimera_nfs_nlm4_free_all */
