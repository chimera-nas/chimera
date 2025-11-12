// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs_common/nfs3_attr.h"
#include "nfs_common/nfs3_status.h"

static void
nfs3_mkdir_callback(
    struct evpl      *evpl,
    struct MKDIR3res *res,
    int               status,
    void             *private_data)
{
    struct chimera_vfs_request *request = private_data;

    if (status != NFS3_OK) {
        request->status = nfs3_client_status_to_chimera_vfs_error(status);
        request->complete(request);
        return;
    }


    if (res->resok.obj_attributes.attributes_follow) {
        chimera_nfs3_unmarshall_attrs(&res->resok.obj_attributes.attributes, &request->mkdir.r_attr);
    }

    chimera_nfs3_get_wcc_data(&request->mkdir.r_dir_pre_attr, &request->mkdir.r_dir_post_attr, &res->resok.dir_wcc);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* nfs3_mkdir_callback */

void
nfs3_mkdir(
    struct nfs_thread          *thread,
    struct nfs_shared          *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct nfs_client_server_thread *server_thread = nfs_thread_get_server_thread(thread, request->fh, request->fh_len);
    struct MKDIR3args                args;
    uint8_t                         *fh;
    int                              fhlen;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    args.where.dir.data.data = fh;
    args.where.dir.data.len  = fhlen;
    args.where.name.str      = (char *) request->mkdir.name;
    args.where.name.len      = request->mkdir.name_len;

    chimera_nfs_va_to_sattr3(&args.attributes, request->mkdir.set_attr);

    shared->nfs_v3.send_call_NFSPROC3_MKDIR(&shared->nfs_v3.rpc2, thread->evpl, server_thread->nfs_conn, &args,
                                            nfs3_mkdir_callback, request);
} /* nfs3_mkdir */

