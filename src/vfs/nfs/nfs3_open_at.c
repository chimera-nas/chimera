// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_error.h"

struct chimera_nfs3_open_at_ctx {
    struct chimera_nfs_thread        *thread;
    struct chimera_nfs_client_server *server;
};

static void
chimera_nfs3_open_at_lookup_callback(
    struct evpl       *evpl,
    struct LOOKUP3res *res,
    int                status,
    void              *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct chimera_nfs3_open_at_ctx       *ctx     = request->plugin_data;
    struct chimera_nfs_client_open_handle *open_handle;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    if (res->status != NFS3_OK) {
        if (res->resfail.dir_attributes.attributes_follow) {
            chimera_nfs3_unmarshall_attrs(&res->resfail.dir_attributes.attributes, &request->open_at.r_dir_pre_attr);
            chimera_nfs3_unmarshall_attrs(&res->resfail.dir_attributes.attributes, &request->open_at.r_dir_post_attr);
        }

        request->status = nfs3_client_status_to_chimera_vfs_error(res->status);
        request->complete(request);
        return;
    }

    if (res->resok.obj_attributes.attributes_follow) {
        chimera_nfs3_unmarshall_attrs(&res->resok.obj_attributes.attributes, &request->open_at.r_attr);
    }

    if (res->resok.dir_attributes.attributes_follow) {
        chimera_nfs3_unmarshall_attrs(&res->resok.dir_attributes.attributes, &request->open_at.r_dir_pre_attr);
        chimera_nfs3_unmarshall_attrs(&res->resok.dir_attributes.attributes, &request->open_at.r_dir_post_attr);
    }

    chimera_nfs3_unmarshall_fh(&res->resok.object, ctx->server->index, &request->open_at.r_attr);

    open_handle        = chimera_nfs_thread_open_handle_alloc(ctx->thread);
    open_handle->dirty = 0;

    request->open_at.r_vfs_private = (uint64_t) open_handle;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* chimera_nfs3_open_at_lookup_callback */

static void
chimera_nfs3_open_at_create_callback(
    struct evpl       *evpl,
    struct CREATE3res *res,
    int                status,
    void              *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct chimera_nfs3_open_at_ctx       *ctx     = request->plugin_data;
    struct chimera_nfs_client_open_handle *open_handle;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    if (res->status != NFS3_OK) {

        chimera_nfs3_get_wcc_data(&request->open_at.r_dir_pre_attr,
                                  &request->open_at.r_dir_post_attr,
                                  &res->resfail.dir_wcc);

        request->status = nfs3_client_status_to_chimera_vfs_error(res->status);
        request->complete(request);
        return;
    }

    if (res->resok.obj_attributes.attributes_follow) {
        chimera_nfs3_unmarshall_attrs(&res->resok.obj_attributes.attributes, &request->open_at.r_attr);
    }

    chimera_nfs3_unmarshall_fh(&res->resok.obj.handle, ctx->server->index, &request->open_at.r_attr);

    chimera_nfs3_get_wcc_data(&request->open_at.r_dir_pre_attr, &request->open_at.r_dir_post_attr, &res->resok.dir_wcc);

    open_handle        = chimera_nfs_thread_open_handle_alloc(ctx->thread);
    open_handle->dirty = 0;

    request->open_at.r_vfs_private = (uint64_t) open_handle;


    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* chimera_nfs3_open_at_create_callback */

void
chimera_nfs3_open_at(
    struct chimera_nfs_thread          *thread,
    struct chimera_nfs_shared          *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh, request->fh_len);
    struct chimera_nfs3_open_at_ctx         *ctx;
    struct LOOKUP3args               lookup_args;
    struct CREATE3args               create_args;
    uint8_t                         *fh;
    int                              fhlen;

    ctx = request->plugin_data;

    ctx->thread = thread;
    ctx->server = server_thread->server;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    if (request->open_at.flags & CHIMERA_VFS_OPEN_CREATE) {

        create_args.where.dir.data.data = fh;
        create_args.where.dir.data.len  = fhlen;
        create_args.where.name.str      = (char *) request->open_at.name;
        create_args.where.name.len      = request->open_at.namelen;
        create_args.how.mode            = UNCHECKED;

        chimera_nfs_va_to_sattr3(&create_args.how.obj_attributes, request->open_at.set_attr);

        shared->nfs_v3.send_call_NFSPROC3_CREATE(&shared->nfs_v3.rpc2, thread->evpl, server_thread->nfs_conn, &
                                                 create_args, chimera_nfs3_open_at_create_callback, request);
    } else {
        chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

        lookup_args.what.dir.data.data = fh;
        lookup_args.what.dir.data.len  = fhlen;
        lookup_args.what.name.str      = (char *) request->open_at.name;
        lookup_args.what.name.len      = request->open_at.namelen;

        shared->nfs_v3.send_call_NFSPROC3_LOOKUP(&shared->nfs_v3.rpc2, thread->evpl, server_thread->nfs_conn, &
                                                 lookup_args,
                                                 chimera_nfs3_open_at_lookup_callback, request);

    }
} /* chimera_nfs3_open_at */

