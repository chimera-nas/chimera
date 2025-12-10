// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"

static void
chimera_nfs3_write_callback(
    struct evpl      *evpl,
    struct WRITE3res *res,
    int               status,
    void             *private_data)
{
    struct chimera_vfs_request *request = private_data;

    /* XXX should not be needed */
    for (int i = 0; i < request->write.niov; i++) {
        evpl_iovec_release(&request->write.iov[i]);
    }

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
    struct chimera_nfs_client_open_handle   *open_handle;
    struct WRITE3args                        args;
    uint8_t                                 *fh;
    int                                      fhlen;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    open_handle = (struct chimera_nfs_client_open_handle *) request->write.handle->vfs_private;

    if (!request->write.sync) {
        open_handle->dirty++;
    }

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    args.file.data.data = fh;
    args.file.data.len  = fhlen;
    args.offset         = request->write.offset;
    args.count          = request->write.length;
    args.stable         = request->write.sync ? FILE_SYNC : UNSTABLE;
    args.data.iov       = request->write.iov;
    args.data.niov      = request->write.niov;
    args.data.length    = request->write.length;

    shared->nfs_v3.send_call_NFSPROC3_WRITE(&shared->nfs_v3.rpc2, thread->evpl, server_thread->nfs_conn, &args,
                                            1, 0, 0, chimera_nfs3_write_callback, request);
} /* chimera_nfs3_write */

