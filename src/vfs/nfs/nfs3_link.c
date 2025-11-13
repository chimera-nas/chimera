// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs_common/nfs3_attr.h"
#include "nfs_common/nfs3_status.h"

static void
chimera_nfs3_link_callback(
    struct evpl     *evpl,
    struct LINK3res *res,
    int              status,
    void            *private_data)
{
    struct chimera_vfs_request *request = private_data;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    if (res->status != NFS3_OK) {
        if (res->resfail.file_attributes.attributes_follow) {
            chimera_nfs3_unmarshall_attrs(&res->resfail.file_attributes.attributes, &request->link.r_attr);
        }

        chimera_nfs3_get_wcc_data(&request->link.r_dir_pre_attr, &request->link.r_dir_post_attr, &res->resfail.
                                  linkdir_wcc);

        request->status = nfs3_client_status_to_chimera_vfs_error(res->status);
        request->complete(request);
        return;
    }

    if (res->resok.file_attributes.attributes_follow) {
        chimera_nfs3_unmarshall_attrs(&res->resok.file_attributes.attributes, &request->link.r_attr);
    }

    chimera_nfs3_get_wcc_data(&request->link.r_dir_pre_attr, &request->link.r_dir_post_attr, &res->resok.linkdir_wcc);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs3_link_callback */

void
chimera_nfs3_link(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct LINK3args                         args;
    uint8_t                                 *fh, *dir_fh;
    int                                      fhlen, dir_fhlen;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);
    chimera_nfs3_map_fh(request->link.dir_fh, request->link.dir_fhlen, &dir_fh, &dir_fhlen);

    args.file.data.data     = fh;
    args.file.data.len      = fhlen;
    args.link.dir.data.data = dir_fh;
    args.link.dir.data.len  = dir_fhlen;
    args.link.name.str      = (char *) request->link.name;
    args.link.name.len      = request->link.namelen;

    shared->nfs_v3.send_call_NFSPROC3_LINK(&shared->nfs_v3.rpc2, thread->evpl, server_thread->nfs_conn, &args,
                                           chimera_nfs3_link_callback, request);
} /* chimera_nfs3_link */

