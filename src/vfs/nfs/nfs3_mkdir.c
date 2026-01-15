// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs_common/nfs3_attr.h"
#include "nfs_common/nfs3_status.h"

struct chimera_nfs3_mkdir_ctx {
    struct chimera_nfs_client_server *server;
};

static void
chimera_nfs3_mkdir_callback(
    struct evpl      *evpl,
    struct MKDIR3res *res,
    int               status,
    void             *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct chimera_nfs3_mkdir_ctx *ctx     = request->plugin_data;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    if (res->status != NFS3_OK) {

        chimera_nfs3_get_wcc_data(&request->mkdir.r_dir_pre_attr,
                                  &request->mkdir.r_dir_post_attr,
                                  &res->resfail.dir_wcc);

        request->status = nfs3_client_status_to_chimera_vfs_error(res->status);
        request->complete(request);
        return;
    }

    chimera_nfs3_unmarshall_fh(&res->resok.obj.handle, ctx->server->index, request->fh, &request->mkdir.r_attr);

    if (res->resok.obj_attributes.attributes_follow) {
        chimera_nfs3_unmarshall_attrs(&res->resok.obj_attributes.attributes, &request->mkdir.r_attr);
    }

    chimera_nfs3_get_wcc_data(&request->mkdir.r_dir_pre_attr, &request->mkdir.r_dir_post_attr, &res->resok.dir_wcc);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs3_mkdir_callback */

void
chimera_nfs3_mkdir(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct chimera_nfs3_mkdir_ctx           *ctx;
    struct MKDIR3args                        args;
    uint8_t                                 *fh;
    int                                      fhlen;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    ctx         = request->plugin_data;
    ctx->server = server_thread->server;

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    args.where.dir.data.data = fh;
    args.where.dir.data.len  = fhlen;
    args.where.name.str      = (char *) request->mkdir.name;
    args.where.name.len      = request->mkdir.name_len;

    chimera_nfs_va_to_sattr3(&args.attributes, request->mkdir.set_attr);

    shared->nfs_v3.send_call_NFSPROC3_MKDIR(&shared->nfs_v3.rpc2, thread->evpl, server_thread->nfs_conn, &args,
                                            0, 0, 0, chimera_nfs3_mkdir_callback, request);
} /* chimera_nfs3_mkdir */

