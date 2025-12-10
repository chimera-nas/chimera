// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"

static void
chimera_nfs3_getattr_callback(
    struct evpl        *evpl,
    struct GETATTR3res *res,
    int                 status,
    void               *private_data)
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

    chimera_nfs3_unmarshall_attrs(&res->resok.obj_attributes, &request->getattr.r_attr);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs3_getattr_callback */

void
chimera_nfs3_getattr(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct GETATTR3args                      args;
    uint8_t                                 *fh;
    int                                      fhlen;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    args.object.data.data = fh;
    args.object.data.len  = fhlen;

    shared->nfs_v3.send_call_NFSPROC3_GETATTR(&shared->nfs_v3.rpc2, thread->evpl, server_thread->nfs_conn, &args,
                                              0, 0, 0, chimera_nfs3_getattr_callback, request);

} /* chimera_nfs3_getattr */

