// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs_common/nfs3_attr.h"
#include "nfs_common/nfs3_status.h"

static void
chimera_nfs3_readlink_callback(
    struct evpl         *evpl,
    struct READLINK3res *res,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request *request = private_data;

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


    request->readlink.r_target_length = res->resok.data.len;

    if (request->readlink.r_target_length > request->readlink.target_maxlength) {
        request->readlink.r_target_length = request->readlink.target_maxlength;
    }

    memcpy(request->readlink.r_target, res->resok.data.str, request->readlink.r_target_length);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs3_readlink_callback */
void
chimera_nfs3_readlink(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct READLINK3args                     args;
    uint8_t                                 *fh;
    int                                      fhlen;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    args.symlink.data.data = fh;
    args.symlink.data.len  = fhlen;

    shared->nfs_v3.send_call_NFSPROC3_READLINK(&shared->nfs_v3.rpc2, thread->evpl, server_thread->nfs_conn, &args,
                                               0, 0, 0, chimera_nfs3_readlink_callback, request);
} /* chimera_nfs3_readlink */

