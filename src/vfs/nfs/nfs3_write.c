// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs3_open_state.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"

struct chimera_nfs3_write_ctx {
    struct chimera_nfs_shared      *shared;
    struct chimera_nfs3_open_state *open_state;
};

static void
chimera_nfs3_write_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct WRITE3res            *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct chimera_nfs3_write_ctx *ctx     = request->plugin_data;

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

    /* Mark file as dirty if the write was not fully committed to stable storage */
    if (res->resok.committed != FILE_SYNC && ctx->open_state) {
        chimera_nfs3_open_state_mark_dirty(ctx->open_state);
    }

    request->write.r_sync   = res->resok.committed;
    request->write.r_length = res->resok.count;
    request->status         = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs3_write_callback */

void
chimera_nfs3_write(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct chimera_nfs3_write_ctx           *ctx;
    struct WRITE3args                        args;
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    /* Initialize context for dirty tracking in callback */
    ctx             = request->plugin_data;
    ctx->shared     = shared;
    ctx->open_state = (struct chimera_nfs3_open_state *) request->write.handle->vfs_private;

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    args.file.data.data = fh;
    args.file.data.len  = fhlen;
    args.offset         = request->write.offset;
    args.count          = request->write.length;
    args.stable         = request->write.sync ? FILE_SYNC : UNSTABLE;
    args.data.iov       = request->write.iov;
    args.data.niov      = request->write.niov;
    args.data.length    = request->write.length;

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    shared->nfs_v3.send_call_NFSPROC3_WRITE(&shared->nfs_v3.rpc2, thread->evpl, server_thread->nfs_conn, &rpc2_cred,
                                            &args, 1, 0, 0, chimera_nfs3_write_callback, request);
} /* chimera_nfs3_write */
