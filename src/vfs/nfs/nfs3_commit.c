// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs3_open_state.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"

struct chimera_nfs3_commit_ctx {
    struct chimera_nfs3_open_state *open_state;
    int                             dirty_count; /* Count captured before commit */
};

static void
chimera_nfs3_commit_callback(
    struct evpl       *evpl,
    struct COMMIT3res *res,
    int                status,
    void              *private_data)
{
    struct chimera_vfs_request     *request = private_data;
    struct chimera_nfs3_commit_ctx *ctx     = request->plugin_data;

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

    /* Clear the dirty count we captured before commit.
     * If writes happened during the commit, they added to the counter,
     * so after subtracting we'll still see those new writes. */
    if (ctx->open_state && ctx->dirty_count > 0) {
        chimera_nfs3_open_state_clear_dirty(ctx->open_state, ctx->dirty_count);
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs3_commit_callback */

void
chimera_nfs3_commit(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct chimera_nfs3_commit_ctx          *ctx;
    struct COMMIT3args                       args;
    uint8_t                                 *fh;
    int                                      fhlen;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    /* Initialize context and capture dirty count before commit */
    ctx              = request->plugin_data;
    ctx->open_state  = (struct chimera_nfs3_open_state *) request->commit.handle->vfs_private;
    ctx->dirty_count = ctx->open_state ? chimera_nfs3_open_state_get_dirty(ctx->open_state) : 0;

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    args.file.data.data = fh;
    args.file.data.len  = fhlen;

    shared->nfs_v3.send_call_NFSPROC3_COMMIT(&shared->nfs_v3.rpc2, thread->evpl, server_thread->nfs_conn, &args,
                                             0, 0, 0, chimera_nfs3_commit_callback, request);
} /* chimera_nfs3_commit */

