// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs3_open_state.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/sdk/vfs_error.h"

struct chimera_nfs3_open_at_ctx {
    struct chimera_nfs_thread        *thread;
    struct chimera_nfs_client_server *server;
};

static void chimera_nfs3_open_at_lookup_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct LOOKUP3res           *res,
    int                          status,
    void                        *private_data);

/*
 * Resolve the name with LOOKUP and complete the open from what it finds.
 *
 * Used for a plain open, and to recover the object behind an UNCHECKED
 * CREATE that the server answered NFS3ERR_EXIST: the POSIX rules the layer
 * above applies -- ELOOP on a symlink under O_NOFOLLOW, EISDIR on a
 * directory, restarting resolution on a symlink it must follow -- all need
 * the object's type, and NFS3ERR_EXIST does not carry it.
 */
static void
chimera_nfs3_open_at_send_lookup(
    struct chimera_nfs_thread  *thread,
    struct chimera_vfs_request *request)
{
    struct chimera_nfs_client_server_thread *server_thread =
        chimera_nfs_thread_get_server_thread(thread, request->fh,
                                             request->fh_len);
    struct chimera_nfs_shared               *shared = thread->shared;
    struct LOOKUP3args                       lookup_args;
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    lookup_args.what.dir.data.data = fh;
    lookup_args.what.dir.data.len  = fhlen;
    lookup_args.what.name.str      = (char *) request->open_at.name;
    lookup_args.what.name.len      = request->open_at.namelen;

    shared->nfs_v3.send_call_NFSPROC3_LOOKUP(&shared->nfs_v3.rpc2,
                                             thread->evpl,
                                             server_thread->nfs_conn,
                                             &rpc2_cred, &lookup_args,
                                             0, 0, NULL, 0, 0,
                                             chimera_nfs3_open_at_lookup_callback,
                                             request);
} /* chimera_nfs3_open_at_send_lookup */

static void
chimera_nfs3_open_at_lookup_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct LOOKUP3res           *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request      *request = private_data;
    struct chimera_nfs3_open_at_ctx *ctx     = request->plugin_data;
    struct chimera_nfs3_open_state  *state;

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

    if (chimera_nfs3_unmarshall_fh(&res->resok.object, ctx->server->index, request->fh, &request->open_at.r_attr) != 0)
    {
        request->status = CHIMERA_VFS_EOVERFLOW;
        request->complete(request);
        return;
    }

    /* Allocate open state for dirty tracking and silly rename support.
     * Skip for inferred opens (use synthetic handles which don't call close).
     * Always allocate for non-inferred opens since open_at always inserts fresh. */
    if (!(request->open_at.flags & CHIMERA_VFS_OPEN_INFERRED)) {
        state = chimera_nfs3_open_state_alloc();

        if (!state) {
            request->status = CHIMERA_VFS_EFAULT;
            request->complete(request);
            return;
        }

        state->server_index            = ctx->server->index;
        request->open_at.r_vfs_private = (uint64_t) state;
    } else {
        request->open_at.r_vfs_private = 0;
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* chimera_nfs3_open_at_lookup_callback */

static void
chimera_nfs3_open_at_create_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct CREATE3res           *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request      *request = private_data;
    struct chimera_nfs3_open_at_ctx *ctx     = request->plugin_data;
    struct chimera_nfs3_open_state  *state;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    if (res->status != NFS3_OK) {

        chimera_nfs3_get_wcc_data(&request->open_at.r_dir_pre_attr,
                                  &request->open_at.r_dir_post_attr,
                                  &res->resfail.dir_wcc);

        /* An UNCHECKED create over a name already held by a non-regular
         * object is NFS3ERR_EXIST on the wire (RFC 1813 3.3.8: CREATE
         * materializes a regular file and neither follows a symlink nor
         * unlinks what is there).  That is the right answer to send, but it
         * is not the answer POSIX gives the caller: open(O_CREAT) on a
         * symlink follows it, or fails ELOOP under O_NOFOLLOW.  The type the
         * decision needs is exactly what the error dropped, so go and get it
         * -- one LOOKUP, only on this path, and the checks above the VFS then
         * see the same object a create-on-existing used to hand them.
         *
         * Not for GUARDED (O_EXCL), where EEXIST is the caller's answer. */
        if (res->status == NFS3ERR_EXIST &&
            !(request->open_at.flags & CHIMERA_VFS_OPEN_EXCLUSIVE)) {
            chimera_nfs3_open_at_send_lookup(ctx->thread, request);
            return;
        }

        request->status = nfs3_client_status_to_chimera_vfs_error(res->status);
        request->complete(request);
        return;
    }

    if (res->resok.obj_attributes.attributes_follow) {
        chimera_nfs3_unmarshall_attrs(&res->resok.obj_attributes.attributes, &request->open_at.r_attr);
    }

    /* NFS3 CREATE (UNCHECKED) returns an existing object rather than creating
     * one; if that object is a symlink and the caller asked for O_NOFOLLOW
     * (a data open, not O_PATH), POSIX open(2) must fail with ELOOP.  NFS3 has
     * no atomic open and the server's CREATE does not see the open flags, so
     * the client enforces this here (mirrors the memfs open_at path). */
    if ((request->open_at.flags & CHIMERA_VFS_OPEN_NOFOLLOW) &&
        !(request->open_at.flags & CHIMERA_VFS_OPEN_PATH) &&
        (request->open_at.r_attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        S_ISLNK(request->open_at.r_attr.va_mode)) {
        request->status = CHIMERA_VFS_ELOOP;
        request->complete(request);
        return;
    }

    if (chimera_nfs3_unmarshall_fh(&res->resok.obj.handle, ctx->server->index, request->fh, &request->open_at.r_attr) !=
        0) {
        request->status = CHIMERA_VFS_EOVERFLOW;
        request->complete(request);
        return;
    }

    chimera_nfs3_get_wcc_data(&request->open_at.r_dir_pre_attr, &request->open_at.r_dir_post_attr, &res->resok.dir_wcc);

    /* Allocate open state for dirty tracking and silly rename support.
     * Skip for inferred opens (use synthetic handles which don't call close).
     * Always allocate for non-inferred opens since open_at always inserts fresh. */
    if (!(request->open_at.flags & CHIMERA_VFS_OPEN_INFERRED)) {
        state = chimera_nfs3_open_state_alloc();

        if (!state) {
            request->status = CHIMERA_VFS_EFAULT;
            request->complete(request);
            return;
        }

        state->server_index            = ctx->server->index;
        request->open_at.r_vfs_private = (uint64_t) state;
    } else {
        request->open_at.r_vfs_private = 0;
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* chimera_nfs3_open_at_create_callback */

void
chimera_nfs3_open_at(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh,
                                                                                                  request->fh_len);
    struct chimera_nfs3_open_at_ctx         *ctx;
    struct CREATE3args                       create_args;
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;

    ctx = request->plugin_data;

    ctx->thread = thread;
    ctx->server = server_thread->server;

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    if (request->open_at.flags & CHIMERA_VFS_OPEN_CREATE) {

        create_args.where.dir.data.data = fh;
        create_args.where.dir.data.len  = fhlen;
        create_args.where.name.str      = (char *) request->open_at.name;
        create_args.where.name.len      = request->open_at.namelen;
        create_args.how.mode            = (request->open_at.flags & CHIMERA_VFS_OPEN_EXCLUSIVE) ? GUARDED : UNCHECKED;

        chimera_nfs_va_to_sattr3(&create_args.how.obj_attributes, request->open_at.set_attr);

        shared->nfs_v3.send_call_NFSPROC3_CREATE(&shared->nfs_v3.rpc2, thread->evpl, server_thread->nfs_conn, &rpc2_cred
                                                 ,
                                                 &create_args, 0, 0, NULL, 0, 0, chimera_nfs3_open_at_create_callback,
                                                 request);
    } else {
        chimera_nfs3_open_at_send_lookup(thread, request);
    }
} /* chimera_nfs3_open_at */

