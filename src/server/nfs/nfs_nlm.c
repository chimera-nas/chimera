// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <stdlib.h>
#include <pthread.h>

#include "nfs_common.h"
#include "nfs_internal.h"
#include "nfs_nlm.h"
#include "nfs_nlm_state.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

/* Convert NLM length (UINT64_MAX == to-EOF) to POSIX length (0 == to-EOF) */
#define NLM_TO_POSIX_LEN(l) ((l) == UINT64_MAX ? 0 : (l))

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
chimera_nfs_nlm4_test_lock_cb(
    enum chimera_vfs_error error_code,
    uint32_t               conflict_type,
    uint64_t               conflict_offset,
    uint64_t               conflict_length,
    pid_t                  conflict_pid,
    void                  *private_data)
{
    struct nlm_test_ctx              *ctx      = private_data;
    struct chimera_server_nfs_thread *thread   = ctx->thread;
    struct chimera_server_nfs_shared *shared   = thread->shared;
    struct evpl                      *evpl     = ctx->evpl;
    struct evpl_rpc2_encoding        *encoding = ctx->encoding;
    struct nlm4_testres               res;
    int                               rc;

    res.cookie.len     = ctx->cookie.len;
    res.cookie.data    = ctx->cookie.data;
    res.test_stat.stat = NLM4_DENIED_NOLOCKS;

    if (error_code == CHIMERA_VFS_OK) {
        res.test_stat.stat = NLM4_GRANTED;
    } else if (error_code == CHIMERA_VFS_EACCES || error_code == CHIMERA_VFS_EAGAIN) {
        res.test_stat.stat             = NLM4_DENIED;
        res.test_stat.holder.exclusive = (conflict_type == CHIMERA_VFS_LOCK_WRITE);
        res.test_stat.holder.svid      = conflict_pid;
        res.test_stat.holder.oh.len    = 0;
        res.test_stat.holder.oh.data   = NULL;
        res.test_stat.holder.l_offset  = conflict_offset;
        res.test_stat.holder.l_len     = (conflict_length == 0) ? UINT64_MAX : conflict_length;
    }

    chimera_nfs_debug("NLM TEST lock cb: vfs_error=%d stat=%d", error_code, res.test_stat.stat);

    /* Release the open handle; TEST does not hold the lock */
    chimera_vfs_release(thread->vfs_thread, ctx->handle);

    if (ctx->proc == 16) {
        shared->nlm_v4.send_call_NLMPROC4_TEST_RES(&shared->nlm_v4.rpc2, evpl, ctx->conn, NULL, &res, 0, 0, 0, NULL,
                                                   NULL);
    } else {
        rc = shared->nlm_v4.send_reply_NLMPROC4_TEST(evpl, NULL, &res, encoding);
        chimera_nfs_abort_if(rc, "Failed to send NLM TEST reply");
    }

    free(ctx);
} /* chimera_nfs_nlm4_test_lock_cb */

static void
chimera_nfs_nlm4_test_open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nlm_test_ctx              *ctx      = private_data;
    struct chimera_server_nfs_thread *thread   = ctx->thread;
    struct chimera_server_nfs_shared *shared   = thread->shared;
    struct evpl                      *evpl     = ctx->evpl;
    struct evpl_rpc2_encoding        *encoding = ctx->encoding;
    struct nlm4_testres               res;
    int                               rc;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs_debug("NLM TEST open failed: error %d -> NLM4_STALE_FH", error_code);
        res.cookie.len     = ctx->cookie.len;
        res.cookie.data    = ctx->cookie.data;
        res.test_stat.stat = NLM4_STALE_FH;
        if (ctx->proc == 16) {
            shared->nlm_v4.send_call_NLMPROC4_TEST_RES(&shared->nlm_v4.rpc2, evpl, ctx->conn, NULL, &res, 0, 0, 0, NULL,
                                                       NULL);
        } else {
            rc = shared->nlm_v4.send_reply_NLMPROC4_TEST(evpl, NULL, &res, encoding);
            chimera_nfs_abort_if(rc, "Failed to send NLM TEST reply");
        }
        free(ctx);
        return;
    }

    /* TEST probes whether the requested lock would conflict.
     * Store handle in ctx; released inside the lock callback. */
    ctx->handle = handle;

    chimera_vfs_lock(thread->vfs_thread, NULL,
                     handle,
                     SEEK_SET,
                     ctx->offset,
                     ctx->length,
                     ctx->exclusive ? CHIMERA_VFS_LOCK_WRITE : CHIMERA_VFS_LOCK_READ,
                     CHIMERA_VFS_LOCK_TEST,
                     chimera_nfs_nlm4_test_lock_cb,
                     ctx);
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
chimera_nfs_nlm4_lock_vfs_cb(
    enum chimera_vfs_error error_code,
    uint32_t               conflict_type,
    uint64_t               conflict_offset,
    uint64_t               conflict_length,
    pid_t                  conflict_pid,
    void                  *private_data)
{
    struct nlm_lock_ctx              *ctx      = private_data;
    struct chimera_server_nfs_thread *thread   = ctx->thread;
    struct chimera_server_nfs_shared *shared   = thread->shared;
    struct evpl                      *evpl     = ctx->evpl;
    struct evpl_rpc2_encoding        *encoding = ctx->encoding;
    struct nlm_lock_entry            *entry    = ctx->entry;
    struct nlm4_res                   res;
    int                               rc = 0;

    res.cookie.len  = ctx->cookie.len;
    res.cookie.data = ctx->cookie.data;

    if (error_code == CHIMERA_VFS_OK) {
        /* Lock acquired -- mark entry confirmed, then persist outside the
         * mutex (nlm_state_persist_client takes its own lock internally). */
        pthread_mutex_lock(&shared->nlm_state.mutex);
        entry->pending = false;
        pthread_mutex_unlock(&shared->nlm_state.mutex);
        if (!ctx->nm_lock) {
            nlm_state_persist_client(&shared->nlm_state, ctx->client);
        }

        res.stat = NLM4_GRANTED;
    } else if (error_code == CHIMERA_VFS_EACCES || error_code == CHIMERA_VFS_EAGAIN) {
        /* Lock conflict -- remove the pending sentinel and reject */
        pthread_mutex_lock(&shared->nlm_state.mutex);
        DL_DELETE(ctx->client->locks, entry);
        pthread_mutex_unlock(&shared->nlm_state.mutex);
        chimera_vfs_release(thread->vfs_thread, entry->handle);
        nlm_lock_entry_free(entry);
        res.stat = ctx->block ? NLM4_BLOCKED : NLM4_DENIED;
    } else {
        /* Other VFS error -- remove the pending sentinel */
        pthread_mutex_lock(&shared->nlm_state.mutex);
        DL_DELETE(ctx->client->locks, entry);
        pthread_mutex_unlock(&shared->nlm_state.mutex);
        chimera_vfs_release(thread->vfs_thread, entry->handle);
        nlm_lock_entry_free(entry);
        res.stat = NLM4_DENIED_NOLOCKS;
    }

    chimera_nfs_debug("NLM LOCK vfs cb: vfs_error=%d stat=%d block=%d", error_code, res.stat, ctx->block);

    switch (ctx->proc) {
        case 2:
            rc = shared->nlm_v4.send_reply_NLMPROC4_LOCK(evpl, NULL, &res, encoding);
            break;
        case 17:
            shared->nlm_v4.send_call_NLMPROC4_LOCK_RES(&shared->nlm_v4.rpc2, evpl, ctx->conn, NULL, &res, 0, 0, 0, NULL,
                                                       NULL);
            break;
        default:
            rc = shared->nlm_v4.send_reply_NLMPROC4_NM_LOCK(evpl, NULL, &res, encoding);
            break;
    } /* switch */
    chimera_nfs_abort_if(rc, "Failed to send NLM LOCK reply");

    free(ctx);
} /* chimera_nfs_nlm4_lock_vfs_cb */

static void
chimera_nfs_nlm4_lock_open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nlm_lock_ctx              *ctx      = private_data;
    struct chimera_server_nfs_thread *thread   = ctx->thread;
    struct chimera_server_nfs_shared *shared   = thread->shared;
    struct evpl                      *evpl     = ctx->evpl;
    struct evpl_rpc2_encoding        *encoding = ctx->encoding;
    struct nlm_lock_entry            *entry    = ctx->entry;
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
                shared->nlm_v4.send_call_NLMPROC4_LOCK_RES(&shared->nlm_v4.rpc2, evpl, ctx->conn, NULL, &res, 0, 0, 0,
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

    chimera_vfs_lock(thread->vfs_thread, NULL,
                     handle,
                     SEEK_SET,
                     entry->offset,
                     entry->length,
                     entry->exclusive ? CHIMERA_VFS_LOCK_WRITE : CHIMERA_VFS_LOCK_READ,
                     0,
                     chimera_nfs_nlm4_lock_vfs_cb,
                     ctx);
} /* chimera_nfs_nlm4_lock_open_cb */

/* -------------------------------------------------------------------------
 * UNLOCK procedure callbacks
 * ---------------------------------------------------------------------- */

struct nlm_unlock_ctx {
    struct chimera_server_nfs_thread *thread;
    struct evpl                      *evpl;
    struct evpl_rpc2_encoding        *encoding;
    struct evpl_rpc2_conn            *conn;
    xdr_opaque                        cookie;
    uint8_t                           cookie_buf[LM_MAXSTRLEN];
    struct nlm_lock_entry            *entry;
    struct nlm_client                *client;
    int                               proc; /* 4=UNLOCK, 18=UNLOCK_MSG */
};

static void
chimera_nfs_nlm4_unlock_vfs_cb(
    enum chimera_vfs_error error_code,
    uint32_t               conflict_type,
    uint64_t               conflict_offset,
    uint64_t               conflict_length,
    pid_t                  conflict_pid,
    void                  *private_data)
{
    struct nlm_unlock_ctx            *ctx      = private_data;
    struct chimera_server_nfs_thread *thread   = ctx->thread;
    struct chimera_server_nfs_shared *shared   = thread->shared;
    struct evpl                      *evpl     = ctx->evpl;
    struct evpl_rpc2_encoding        *encoding = ctx->encoding;
    struct nlm_lock_entry            *entry    = ctx->entry;
    struct nlm4_res                   res;
    int                               rc;

    if (error_code != CHIMERA_VFS_OK) {
        /* Per RFC 1813, UNLOCK always returns NLM4_GRANTED regardless of
         * VFS outcome, but log unexpected errors for diagnostics. */
        chimera_nfs_debug("NLM UNLOCK vfs cb: VFS error %d -- replying NLM4_GRANTED per RFC",
                          error_code);
    } else {
        chimera_nfs_debug("NLM UNLOCK vfs cb: ok");
    }

    chimera_vfs_release(thread->vfs_thread, entry->handle);

    /* Entry was already removed from client->locks in chimera_nfs_nlm4_unlock
     * to take exclusive ownership before releasing the mutex.
     * Persist without holding the mutex (the function takes its own lock). */
    nlm_state_persist_client(&shared->nlm_state, ctx->client);

    nlm_lock_entry_free(entry);

    res.cookie.len  = ctx->cookie.len;
    res.cookie.data = ctx->cookie.data;
    res.stat        = NLM4_GRANTED;

    if (ctx->proc == 18) {
        shared->nlm_v4.send_call_NLMPROC4_UNLOCK_RES(&shared->nlm_v4.rpc2, evpl, ctx->conn, NULL, &res, 0, 0, 0, NULL,
                                                     NULL);
    } else {
        rc = shared->nlm_v4.send_reply_NLMPROC4_UNLOCK(evpl, NULL, &res, encoding);
        chimera_nfs_abort_if(rc, "Failed to send NLM UNLOCK reply");
    }

    free(ctx);
} /* chimera_nfs_nlm4_unlock_vfs_cb */

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
    struct nlm_lock_entry            *conflict;
    struct nlm4_testres               res;
    uint8_t                           conflict_oh[LM_MAXSTRLEN];
    uint32_t                          conflict_oh_len = 0;
    int                               rc;

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
            shared->nlm_v4.send_call_NLMPROC4_TEST_RES(&shared->nlm_v4.rpc2, evpl, conn, NULL, &err_res, 0, 0, 0, NULL,
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

    /* Check the in-memory NLM state for conflicts first.
     * This is necessary for in-process servers where all lock operations
     * share the same OS process, making kernel F_GETLK ineffective for
     * detecting cross-connection conflicts. */
    pthread_mutex_lock(&shared->nlm_state.mutex);
    conflict = nlm_state_find_conflict(&shared->nlm_state,
                                       ctx->fh, ctx->fh_len,
                                       ctx->caller_name,
                                       ctx->oh, ctx->oh_len,
                                       ctx->svid,
                                       ctx->offset, ctx->length,
                                       ctx->exclusive);
    if (conflict) {
        chimera_nfs_debug("NLM TEST: in-memory conflict found -> NLM4_DENIED");
        /* Copy conflict->oh before releasing the mutex: the entry can be freed
         * by another thread as soon as we unlock, making the pointer stale. */
        conflict_oh_len = conflict->oh_len;
        memcpy(conflict_oh, conflict->oh, conflict->oh_len);
        res.cookie.len                 = ctx->cookie.len;
        res.cookie.data                = ctx->cookie.data;
        res.test_stat.stat             = NLM4_DENIED;
        res.test_stat.holder.exclusive = conflict->exclusive;
        res.test_stat.holder.svid      = conflict->svid;
        res.test_stat.holder.oh.len    = conflict_oh_len;
        res.test_stat.holder.oh.data   = conflict_oh;
        res.test_stat.holder.l_offset  = conflict->offset;
        res.test_stat.holder.l_len     = (conflict->length == 0)
                                          ? UINT64_MAX
                                          : conflict->length;
        pthread_mutex_unlock(&shared->nlm_state.mutex);
        if (ctx->proc == 16) {
            shared->nlm_v4.send_call_NLMPROC4_TEST_RES(&shared->nlm_v4.rpc2, evpl, ctx->conn, NULL, &res, 0, 0, 0, NULL,
                                                       NULL);
        } else {
            rc = shared->nlm_v4.send_reply_NLMPROC4_TEST(evpl, NULL, &res, encoding);
            chimera_nfs_abort_if(rc, "Failed to send NLM TEST reply");
        }
        free(ctx);
        return;
    }
    pthread_mutex_unlock(&shared->nlm_state.mutex);

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
    struct nlm_lock_entry            *conflict;
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
                shared->nlm_v4.send_call_NLMPROC4_LOCK_RES(&shared->nlm_v4.rpc2, evpl, conn, NULL, &res, 0, 0, 0, NULL,
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

    /* Check in-memory state for conflicts before attempting the VFS lock.
     * Required for in-process servers where all NLM connections share the
     * same OS PID, making the kernel's F_SETLK unable to detect conflicts
     * between different NLM clients. */
    conflict = nlm_state_find_conflict(&shared->nlm_state,
                                       entry->fh, entry->fh_len,
                                       client->hostname,
                                       entry->oh, entry->oh_len,
                                       entry->svid,
                                       entry->offset, entry->length,
                                       entry->exclusive);
    if (conflict) {
        pthread_mutex_unlock(&shared->nlm_state.mutex);
        /* Always reply NLM4_DENIED: there is no pending-lock queue, so
         * NLM4_BLOCKED would strand clients waiting for a GRANTED_MSG
         * callback that will never arrive. */
        chimera_nfs_debug("NLM LOCK: in-memory conflict -> NLM4_DENIED");
        res.cookie.len  = ctx->cookie.len;
        res.cookie.data = ctx->cookie.data;
        res.stat        = NLM4_DENIED;
        switch (ctx->proc) {
            case 2:
                rc = shared->nlm_v4.send_reply_NLMPROC4_LOCK(evpl, NULL, &res, encoding);
                break;
            case 17:
                shared->nlm_v4.send_call_NLMPROC4_LOCK_RES(&shared->nlm_v4.rpc2, evpl, ctx->conn, NULL, &res, 0, 0, 0,
                                                           NULL, NULL);
                break;
            default:
                rc = shared->nlm_v4.send_reply_NLMPROC4_NM_LOCK(evpl, NULL, &res, encoding);
                break;
        } /* switch */
        chimera_nfs_abort_if(rc, "Failed to send NLM LOCK conflict reply");
        nlm_lock_entry_free(entry);
        free(ctx);
        return;
    }

    /* Reject if an identical confirmed lock already exists for this owner.
     * Two simultaneous LOCK requests from the same owner both bypass the
     * self-exclusion check in nlm_state_find_conflict; this guard prevents
     * a duplicate confirmed entry from slipping in. */
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
                shared->nlm_v4.send_call_NLMPROC4_LOCK_RES(&shared->nlm_v4.rpc2, evpl, ctx->conn, NULL, &res, 0, 0, 0,
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
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nlm4_res                   res;
    int                               rc;

    chimera_nfs_debug("NLM CANCEL: caller='%.*s' -> NLM4_GRANTED (no pending queue)",
                      (int) args->alock.caller_name.len, args->alock.caller_name.str);

    /* Phase 1: no pending-lock queue, so there is nothing to cancel.
     * Blocking clients that received NLM4_BLOCKED will retry. */
    res.cookie.len  = args->cookie.len;
    res.cookie.data = args->cookie.data;
    res.stat        = NLM4_GRANTED;

    rc = shared->nlm_v4.send_reply_NLMPROC4_CANCEL(evpl, NULL, &res, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM CANCEL reply");
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
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nlm_client                *client;
    struct nlm_lock_entry            *entry;
    struct nlm_unlock_ctx            *ctx;
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
            shared->nlm_v4.send_call_NLMPROC4_UNLOCK_RES(&shared->nlm_v4.rpc2, evpl, conn, NULL, &res, 0, 0, 0, NULL,
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
            shared->nlm_v4.send_call_NLMPROC4_UNLOCK_RES(&shared->nlm_v4.rpc2, evpl, conn, NULL, &res, 0, 0, 0, NULL,
                                                         NULL);
        } else {
            rc = shared->nlm_v4.send_reply_NLMPROC4_UNLOCK(evpl, NULL, &res, encoding);
            chimera_nfs_abort_if(rc, "Failed to send NLM UNLOCK reply");
        }
        return;
    }

    /* Take exclusive ownership of the entry by removing it from the list
     * while still holding the mutex, preventing concurrent FREE_ALL or
     * disconnect handlers from freeing it before the VFS call completes. */
    DL_DELETE(client->locks, entry);
    pthread_mutex_unlock(&shared->nlm_state.mutex);

    ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        /* Re-insert entry so state remains consistent, then reply with error */
        pthread_mutex_lock(&shared->nlm_state.mutex);
        DL_APPEND(client->locks, entry);
        pthread_mutex_unlock(&shared->nlm_state.mutex);
        res.cookie.len  = args->cookie.len;
        res.cookie.data = args->cookie.data;
        res.stat        = NLM4_DENIED_NOLOCKS;
        if (proc == 18) {
            shared->nlm_v4.send_call_NLMPROC4_UNLOCK_RES(&shared->nlm_v4.rpc2, evpl, conn, NULL, &res, 0, 0, 0, NULL,
                                                         NULL);
        } else {
            rc = shared->nlm_v4.send_reply_NLMPROC4_UNLOCK(evpl, NULL, &res, encoding);
            chimera_nfs_abort_if(rc, "Failed to send NLM UNLOCK reply");
        }
        return;
    }
    ctx->thread   = thread;
    ctx->evpl     = evpl;
    ctx->encoding = encoding;
    ctx->conn     = conn;
    ctx->entry    = entry;
    ctx->client   = client;
    ctx->proc     = proc;

    if (args->cookie.len > 0 && args->cookie.len <= LM_MAXSTRLEN) {
        ctx->cookie.len  = args->cookie.len;
        ctx->cookie.data = ctx->cookie_buf;
        memcpy(ctx->cookie_buf, args->cookie.data, args->cookie.len);
    }

    chimera_vfs_lock(thread->vfs_thread, NULL,
                     entry->handle,
                     SEEK_SET,
                     entry->offset,
                     entry->length,
                     CHIMERA_VFS_LOCK_UNLOCK,
                     0,
                     chimera_nfs_nlm4_unlock_vfs_cb,
                     ctx);
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
    shared->nlm_v4.send_call_NLMPROC4_CANCEL_RES(&shared->nlm_v4.rpc2, evpl, conn, NULL, &res, 0, 0, 0, NULL, NULL);
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
                                     thread->vfs_thread, &anon_cred);
    }
    pthread_mutex_unlock(&shared->nlm_state.mutex);
    /* Remove on-disk state outside the mutex to avoid blocking I/O under lock */
    if (client) {
        nlm_state_remove_client_file(&shared->nlm_state, safe_hostname);
    }

    rc = shared->nlm_v4.send_reply_NLMPROC4_FREE_ALL(evpl, NULL, encoding);
    chimera_nfs_abort_if(rc, "Failed to send NLM FREE_ALL reply");
} /* chimera_nfs_nlm4_free_all */
