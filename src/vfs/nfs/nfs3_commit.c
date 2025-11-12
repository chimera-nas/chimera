// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"

struct chimera_nfs3_commit_ctx {
    struct chimera_nfs_thread *thread;
    int                dirty;
};

static void
chimera_nfs3_commit_callback(
    struct evpl       *evpl,
    struct COMMIT3res *res,
    int                status,
    void              *private_data)
{
    struct chimera_vfs_request    *request     = private_data;
    struct chimera_nfs_client_open_handle *open_handle = (struct chimera_nfs_client_open_handle *) request->close.vfs_private;
    struct chimera_nfs3_commit_ctx        *ctx         = request->plugin_data;

    chimera_nfs_thread_open_handle_free(ctx->thread, open_handle);

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    if (res->status != NFS3_OK) {
        chimera_nfs3_get_wcc_data(&request->write.r_pre_attr, &request->write.r_post_attr, &res->resfail.file_wcc);
        request->status = nfs3_client_status_to_chimera_vfs_error(res->status);
        request->complete(request);
        return;
    }

    chimera_nfs3_get_wcc_data(&request->write.r_pre_attr, &request->write.r_post_attr, &res->resok.file_wcc);

    open_handle->dirty -= ctx->dirty;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* nfs3_close_callback */
void
chimera_nfs3_commit(
    struct chimera_nfs_thread          *thread,
    struct chimera_nfs_shared          *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh, request->fh_len);
    struct chimera_nfs3_commit_ctx          *ctx           = private_data;
    struct chimera_nfs_client_open_handle   *open_handle   = (struct chimera_nfs_client_open_handle *) request->commit.handle->
        vfs_private;
    struct COMMIT3args               args;
    uint8_t                         *fh;
    int                              fhlen;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    ctx         = request->plugin_data;
    ctx->thread = thread;
    ctx->dirty  = open_handle->dirty;

    args.file.data.data = fh;
    args.file.data.len  = fhlen;

    shared->nfs_v3.send_call_NFSPROC3_COMMIT(&shared->nfs_v3.rpc2, thread->evpl, server_thread->nfs_conn, &args,
                                             chimera_nfs3_commit_callback, request);
    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs3_commit */

