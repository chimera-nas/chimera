// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs_common/nfs3_attr.h"
#include "nfs_common/nfs3_status.h"

struct chimera_nfs3_symlink_ctx {
    struct chimera_nfs_client_server *server;
};

static void
chimera_nfs3_symlink_callback(
    struct evpl        *evpl,
    struct SYMLINK3res *res,
    int                 status,
    void               *private_data)
{
    struct chimera_vfs_request *request = private_data;
    struct chimera_nfs3_symlink_ctx    *ctx     = request->plugin_data;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    if (res->status != NFS3_OK) {

        chimera_nfs3_get_wcc_data(&request->symlink.r_dir_pre_attr, &request->symlink.r_dir_post_attr, &res->resfail.
                                  dir_wcc);

        request->status = nfs3_client_status_to_chimera_vfs_error(res->status);
        request->complete(request);
        return;
    }

    chimera_nfs3_get_wcc_data(&request->symlink.r_dir_pre_attr, &request->symlink.r_dir_post_attr, &res->resok.dir_wcc);

    if (res->resok.obj.handle_follows) {
        chimera_nfs3_unmarshall_fh(&res->resok.obj.handle, ctx->server->index, &request->symlink.r_attr);
    }

    if (res->resok.obj_attributes.attributes_follow) {
        chimera_nfs3_unmarshall_attrs(&res->resok.obj_attributes.attributes, &request->symlink.r_attr);
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* nfs3_symlink_callback */


void
chimera_nfs3_symlink(
    struct chimera_nfs_thread          *thread,
    struct chimera_nfs_shared          *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh, request->fh_len);
    struct SYMLINK3args              args;
    struct chimera_nfs3_symlink_ctx         *ctx;
    uint8_t                         *fh;
    int                              fhlen;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    args.where.dir.data.data = fh;
    args.where.dir.data.len  = fhlen;
    args.where.name.str      = (char *) request->symlink.name;
    args.where.name.len      = request->symlink.namelen;

    args.symlink.symlink_data.str = (char *) request->symlink.target;
    args.symlink.symlink_data.len = request->symlink.targetlen;

    chimera_nfs_va_to_sattr3(&args.symlink.symlink_attributes, request->symlink.set_attr);

    ctx         = request->plugin_data;
    ctx->server = server_thread->server;

    shared->nfs_v3.send_call_NFSPROC3_SYMLINK(&shared->nfs_v3.rpc2, thread->evpl, server_thread->nfs_conn, &args,
                                              chimera_nfs3_symlink_callback, request);
} /* chimera_nfs3_symlink */

