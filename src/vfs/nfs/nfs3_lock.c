// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs_common/nfs3_status.h"

/*
 * Context stored in request->plugin_data for the duration of the NLM call.
 * The oh_val field provides a request-lifetime home for the owner-handle
 * value: NLM passes oh by pointer into an async RPC, so it must not live
 * on the stack of chimera_nfs3_do_lock.
 */
struct nfs3_lock_ctx {
    struct chimera_nfs_thread               *nfs_thread;
    struct chimera_nfs_shared               *shared;
    struct chimera_nfs_client_server_thread *server_thread;
    uint64_t                                 oh_val;
};

static void chimera_nfs3_do_lock(
    struct chimera_nfs_thread               *thread,
    struct chimera_nfs_shared               *shared,
    struct chimera_nfs_client_server_thread *server_thread,
    struct chimera_vfs_request              *request);

static void
chimera_nfs3_lock_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct nlm4_res             *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request *request = private_data;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    switch (res->stat) {
        case NLM4_GRANTED:
            request->status = CHIMERA_VFS_OK;
            break;
        case NLM4_BLOCKED:
            /* Server has no pending queue; caller should retry */
            request->status = CHIMERA_VFS_EAGAIN;
            break;
        case NLM4_DENIED:
            request->status = CHIMERA_VFS_EACCES;
            break;
        case NLM4_STALE_FH:
            request->status = CHIMERA_VFS_ESTALE;
            break;
        default:
            request->status = CHIMERA_VFS_EFAULT;
            break;
    } /* switch */

    request->complete(request);
} /* chimera_nfs3_lock_callback */

static void
chimera_nfs3_unlock_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct nlm4_res             *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request *request = private_data;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    request->status = (res->stat == NLM4_GRANTED) ? CHIMERA_VFS_OK : CHIMERA_VFS_EFAULT;
    request->complete(request);
} /* chimera_nfs3_unlock_callback */

static void
chimera_nfs3_test_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct nlm4_testres         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct nlm4_holder         *holder;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    switch (res->test_stat.stat) {
        case NLM4_GRANTED:
            request->lock.r_conflict_type = CHIMERA_VFS_LOCK_UNLOCK;
            request->status               = CHIMERA_VFS_OK;
            break;
        case NLM4_DENIED:
            holder                        = &res->test_stat.holder;
            request->lock.r_conflict_type = holder->exclusive
                                              ? CHIMERA_VFS_LOCK_WRITE
                                              : CHIMERA_VFS_LOCK_READ;
            request->lock.r_conflict_offset = holder->l_offset;
            request->lock.r_conflict_length = holder->l_len;
            request->lock.r_conflict_pid    = holder->svid;
            request->status                 = CHIMERA_VFS_OK;
            break;
        default:
            request->status = CHIMERA_VFS_EFAULT;
            break;
    } /* switch */

    request->complete(request);
} /* chimera_nfs3_test_callback */

static void
chimera_nfs3_do_lock(
    struct chimera_nfs_thread               *thread,
    struct chimera_nfs_shared               *shared,
    struct chimera_nfs_client_server_thread *server_thread,
    struct chimera_vfs_request              *request)
{
    struct nfs3_lock_ctx *ctx = request->plugin_data;
    struct evpl_rpc2_cred rpc2_cred;
    uint8_t              *fh;
    int                   fhlen;
    uint64_t              nlm_len;
    int                   exclusive;

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    /* Store oh_val in request-lifetime ctx: NLM passes oh by pointer into
     * an async RPC call, so it must not live on this stack frame. */
    ctx->oh_val = (uint64_t) (uintptr_t) request->lock.handle;
    nlm_len     = (request->lock.length == 0) ? UINT64_MAX : request->lock.length;

    if (request->lock.lock_type == CHIMERA_VFS_LOCK_UNLOCK) {
        struct nlm4_unlockargs args;

        memset(&args, 0, sizeof(args));
        args.alock.caller_name.str = (char *) request->thread->vfs->machine_name;
        args.alock.caller_name.len = request->thread->vfs->machine_name_len;
        args.alock.fh.data         = fh;
        args.alock.fh.len          = fhlen;
        args.alock.oh.data         = &ctx->oh_val;
        args.alock.oh.len          = sizeof(ctx->oh_val);
        args.alock.svid            = 0;
        args.alock.l_offset        = request->lock.offset;
        args.alock.l_len           = nlm_len;

        shared->nlm_v4.send_call_NLMPROC4_UNLOCK(&shared->nlm_v4.rpc2, thread->evpl,
                                                 server_thread->nlm_conn, &rpc2_cred,
                                                 &args, 0, 0, 0,
                                                 chimera_nfs3_unlock_callback, request);

    } else if (request->lock.flags & CHIMERA_VFS_LOCK_TEST) {
        struct nlm4_testargs args;

        exclusive = (request->lock.lock_type == CHIMERA_VFS_LOCK_WRITE);

        memset(&args, 0, sizeof(args));
        args.exclusive             = exclusive;
        args.alock.caller_name.str = (char *) request->thread->vfs->machine_name;
        args.alock.caller_name.len = request->thread->vfs->machine_name_len;
        args.alock.fh.data         = fh;
        args.alock.fh.len          = fhlen;
        args.alock.oh.data         = &ctx->oh_val;
        args.alock.oh.len          = sizeof(ctx->oh_val);
        args.alock.svid            = 0;
        args.alock.l_offset        = request->lock.offset;
        args.alock.l_len           = nlm_len;

        shared->nlm_v4.send_call_NLMPROC4_TEST(&shared->nlm_v4.rpc2, thread->evpl,
                                               server_thread->nlm_conn, &rpc2_cred,
                                               &args, 0, 0, 0,
                                               chimera_nfs3_test_callback, request);

    } else {
        struct nlm4_lockargs args;

        exclusive = (request->lock.lock_type == CHIMERA_VFS_LOCK_WRITE);

        memset(&args, 0, sizeof(args));
        args.block                 = (request->lock.flags & CHIMERA_VFS_LOCK_WAIT) ? 1 : 0;
        args.exclusive             = exclusive;
        args.reclaim               = 0;
        args.state                 = 0;
        args.alock.caller_name.str = (char *) request->thread->vfs->machine_name;
        args.alock.caller_name.len = request->thread->vfs->machine_name_len;
        args.alock.fh.data         = fh;
        args.alock.fh.len          = fhlen;
        args.alock.oh.data         = &ctx->oh_val;
        args.alock.oh.len          = sizeof(ctx->oh_val);
        args.alock.svid            = 0;
        args.alock.l_offset        = request->lock.offset;
        args.alock.l_len           = nlm_len;

        shared->nlm_v4.send_call_NLMPROC4_LOCK(&shared->nlm_v4.rpc2, thread->evpl,
                                               server_thread->nlm_conn, &rpc2_cred,
                                               &args, 0, 0, 0,
                                               chimera_nfs3_lock_callback, request);
    }
} /* chimera_nfs3_do_lock */

static void
chimera_nfs3_lock_getattr_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct GETATTR3res          *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request              *request       = private_data;
    struct nfs3_lock_ctx                    *ctx           = request->plugin_data;
    struct chimera_nfs_thread               *thread        = ctx->nfs_thread;
    struct chimera_nfs_shared               *shared        = ctx->shared;
    struct chimera_nfs_client_server_thread *server_thread = ctx->server_thread;
    int64_t                                  signed_offset;
    uint64_t                                 file_size;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    if (res->status != NFS3_OK) {
        request->status = nfs3_client_status_to_chimera_vfs_error(res->status);
        request->complete(request);
        return;
    }

    file_size     = res->resok.obj_attributes.size;
    signed_offset = (int64_t) file_size + (int64_t) request->lock.offset;

    if (signed_offset < 0) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    request->lock.offset = (uint64_t) signed_offset;
    request->lock.whence = SEEK_SET;

    chimera_nfs3_do_lock(thread, shared, server_thread, request);
} /* chimera_nfs3_lock_getattr_callback */

void
chimera_nfs3_lock(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread;
    struct nfs3_lock_ctx                    *ctx;
    struct GETATTR3args                      getattr_args;
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;

    server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh, request->fh_len);

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    if (!server_thread->nlm_conn) {
        request->status = CHIMERA_VFS_ENOTSUP;
        request->complete(request);
        return;
    }

    /* Always initialize ctx: chimera_nfs3_do_lock reads ctx->oh_val from
     * plugin_data regardless of whence.  SEEK_CUR is normalized to SEEK_SET
     * by the POSIX layer (posix_fcntl.c) before reaching here, so only
     * SEEK_SET and SEEK_END need to be handled. */
    ctx                = request->plugin_data;
    ctx->nfs_thread    = thread;
    ctx->shared        = shared;
    ctx->server_thread = server_thread;

    if (request->lock.whence == SEEK_END) {

        chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

        getattr_args.object.data.data = fh;
        getattr_args.object.data.len  = fhlen;

        chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                                   request->thread->vfs->machine_name,
                                   request->thread->vfs->machine_name_len);

        shared->nfs_v3.send_call_NFSPROC3_GETATTR(&shared->nfs_v3.rpc2, thread->evpl,
                                                  server_thread->nfs_conn, &rpc2_cred,
                                                  &getattr_args, 0, 0, 0,
                                                  chimera_nfs3_lock_getattr_callback, request);
    } else {
        chimera_nfs3_do_lock(thread, shared, server_thread, request);
    }
} /* chimera_nfs3_lock */
