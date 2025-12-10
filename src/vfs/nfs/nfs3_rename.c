// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs_common/nfs3_attr.h"
#include "nfs_common/nfs3_status.h"

static void
chimera_nfs3_rename_callback(
    struct evpl       *evpl,
    struct RENAME3res *res,
    int                status,
    void              *private_data)
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

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* chimera_nfs3_rename_callback */

void
chimera_nfs3_rename(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct RENAME3args                       args;
    uint8_t                                 *old_fh, *new_fh;
    int                                      old_fhlen, new_fhlen;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    chimera_nfs3_map_fh(request->fh, request->fh_len, &old_fh, &old_fhlen);
    chimera_nfs3_map_fh(request->rename.new_fh, request->rename.new_fhlen, &new_fh, &new_fhlen);

    args.from.dir.data.data = old_fh;
    args.from.dir.data.len  = old_fhlen;
    args.from.name.str      = (char *) request->rename.name;
    args.from.name.len      = request->rename.namelen;

    args.to.dir.data.data = new_fh;
    args.to.dir.data.len  = new_fhlen;
    args.to.name.str      = (char *) request->rename.new_name;
    args.to.name.len      = request->rename.new_namelen;

    shared->nfs_v3.send_call_NFSPROC3_RENAME(&shared->nfs_v3.rpc2, thread->evpl, server_thread->nfs_conn, &args,
                                             0, 0, 0, chimera_nfs3_rename_callback, request);
} /* chimera_nfs3_rename */

