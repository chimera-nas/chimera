// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs_common/nfs3_attr.h"
#include "nfs_common/nfs3_status.h"

static void
chimera_nfs3_remove_callback(
    struct evpl       *evpl,
    struct REMOVE3res *res,
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

        chimera_nfs3_get_wcc_data(&request->remove.r_dir_pre_attr,
                                  &request->remove.r_dir_post_attr,
                                  &res->resfail.dir_wcc);

        request->status = nfs3_client_status_to_chimera_vfs_error(res->status);
        request->complete(request);
        return;
    }

    chimera_nfs3_get_wcc_data(&request->remove.r_dir_pre_attr,
                              &request->remove.r_dir_post_attr,
                              &res->resok.dir_wcc);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs3_remove_callback */

void
chimera_nfs3_remove(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct REMOVE3args                       args;
    uint8_t                                 *fh;
    int                                      fhlen;

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    args.object.dir.data.data = fh;
    args.object.dir.data.len  = fhlen;
    args.object.name.str      = (char *) request->remove.name;
    args.object.name.len      = request->remove.namelen;

    shared->nfs_v3.send_call_NFSPROC3_REMOVE(&shared->nfs_v3.rpc2, thread->evpl, server_thread->nfs_conn, &args,
                                             0, 0, 0,
                                             chimera_nfs3_remove_callback, request);
} /* chimera_nfs3_remove */

