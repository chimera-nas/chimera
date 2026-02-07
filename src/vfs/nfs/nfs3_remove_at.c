// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs3_open_state.h"
#include "nfs_common/nfs3_attr.h"
#include "nfs_common/nfs3_status.h"
#include "vfs/vfs_open_cache.h"
#include "vfs/vfs_internal.h"

/*
 * NFS3 Remove with Silly Rename Support
 *
 * Silly rename is only checked when the caller provides the child file handle
 * (child_fh). This happens when the caller is the local client library, which
 * looks up the child before calling remove.
 *
 * When child_fh is NULL (e.g., from NFS server serving external clients),
 * we skip silly rename handling entirely and proceed with normal remove.
 * External clients manage their own silly renames.
 */

struct chimera_nfs3_remove_ctx {
    struct chimera_nfs_thread        *thread;
    struct chimera_nfs_shared        *shared;
    struct chimera_nfs_client_server *server;
    char                              silly_name[5 + CHIMERA_VFS_FH_SIZE * 2 + 1];
    int                               silly_name_len;
};

static void
chimera_nfs3_remove_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct REMOVE3res           *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request *request = private_data;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    if (res->status != NFS3_OK) {

        chimera_nfs3_get_wcc_data(&request->remove_at.r_dir_pre_attr,
                                  &request->remove_at.r_dir_post_attr,
                                  &res->resfail.dir_wcc);

        request->status = nfs3_client_status_to_chimera_vfs_error(res->status);
        request->complete(request);
        return;
    }

    chimera_nfs3_get_wcc_data(&request->remove_at.r_dir_pre_attr,
                              &request->remove_at.r_dir_post_attr,
                              &res->resok.dir_wcc);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs3_remove_callback */

static void
chimera_nfs3_remove_rename_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct RENAME3res           *res,
    int                          status,
    void                        *private_data)
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

    /* Silly rename succeeded - from the caller's perspective, the file is removed */
    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs3_remove_rename_callback */

static void
chimera_nfs3_remove_do_silly_rename(
    struct chimera_vfs_request     *request,
    struct chimera_nfs3_remove_ctx *ctx)
{
    struct chimera_nfs_client_server_thread *server_thread;
    struct RENAME3args                       args;
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *dir_fh;
    int                                      dir_fhlen;

    server_thread = chimera_nfs_thread_get_server_thread(ctx->thread, request->fh, request->fh_len);

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    chimera_nfs3_map_fh(request->fh, request->fh_len, &dir_fh, &dir_fhlen);

    /* Rename from original name to silly name in the same directory */
    args.from.dir.data.data = dir_fh;
    args.from.dir.data.len  = dir_fhlen;
    args.from.name.str      = (char *) request->remove_at.name;
    args.from.name.len      = request->remove_at.namelen;

    args.to.dir.data.data = dir_fh;
    args.to.dir.data.len  = dir_fhlen;
    args.to.name.str      = ctx->silly_name;
    args.to.name.len      = ctx->silly_name_len;

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    ctx->shared->nfs_v3.send_call_NFSPROC3_RENAME(&ctx->shared->nfs_v3.rpc2, ctx->thread->evpl,
                                                  server_thread->nfs_conn, &rpc2_cred, &args,
                                                  0, 0, 0,
                                                  chimera_nfs3_remove_rename_callback, request);
} /* chimera_nfs3_remove_do_silly_rename */

static void
chimera_nfs3_remove_do_remove(
    struct chimera_vfs_request     *request,
    struct chimera_nfs3_remove_ctx *ctx)
{
    struct chimera_nfs_client_server_thread *server_thread;
    struct REMOVE3args                       args;
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *dir_fh;
    int                                      dir_fhlen;

    server_thread = chimera_nfs_thread_get_server_thread(ctx->thread, request->fh, request->fh_len);

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    chimera_nfs3_map_fh(request->fh, request->fh_len, &dir_fh, &dir_fhlen);

    args.object.dir.data.data = dir_fh;
    args.object.dir.data.len  = dir_fhlen;
    args.object.name.str      = (char *) request->remove_at.name;
    args.object.name.len      = request->remove_at.namelen;

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    ctx->shared->nfs_v3.send_call_NFSPROC3_REMOVE(&ctx->shared->nfs_v3.rpc2, ctx->thread->evpl,
                                                  server_thread->nfs_conn, &rpc2_cred, &args,
                                                  0, 0, 0,
                                                  chimera_nfs3_remove_callback, request);
} /* chimera_nfs3_remove_do_remove */

void
chimera_nfs3_remove_at(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread;
    struct chimera_nfs3_remove_ctx          *ctx;
    struct chimera_vfs_open_handle          *handle;
    struct chimera_nfs3_open_state          *state;
    struct vfs_open_cache                   *cache;
    uint64_t                                 fh_hash;
    int                                      rc;

    server_thread = chimera_nfs_thread_get_server_thread(thread, request->fh, request->fh_len);

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    /* Initialize context */
    ctx         = request->plugin_data;
    ctx->thread = thread;
    ctx->shared = shared;
    ctx->server = server_thread->server;

    /*
     * If no child FH provided, skip silly rename handling entirely.
     * This happens when the caller is an NFS server serving external clients
     * who manage their own silly renames.
     */
    if (!request->remove_at.child_fh || request->remove_at.child_fh_len == 0) {
        chimera_nfs3_remove_do_remove(request, ctx);
        return;
    }

    /*
     * Child FH provided - check if file is open and needs silly rename.
     * The child FH was obtained by the caller (client library) before
     * calling remove, so we can check the open cache directly.
     */
    cache   = request->thread->vfs->vfs_open_file_cache;
    fh_hash = chimera_vfs_hash(request->remove_at.child_fh, request->remove_at.child_fh_len);
    handle  = chimera_vfs_open_cache_lookup_ref(cache, request->remove_at.child_fh,
                                                request->remove_at.child_fh_len, fh_hash);

    if (!handle) {
        /* File is not open, proceed with normal remove */
        chimera_nfs3_remove_do_remove(request, ctx);
        return;
    }

    /* File is open - get NFS3 state and mark for silly rename */
    state = (struct chimera_nfs3_open_state *) handle->vfs_private;

    if (!state) {
        /* No NFS3 state attached (shouldn't happen but handle gracefully) */
        chimera_vfs_open_cache_release(request->thread, cache, handle, 0);
        chimera_nfs3_remove_do_remove(request, ctx);
        return;
    }

    rc = chimera_nfs3_open_state_mark_silly(state, request->fh, request->fh_len, request->cred);

    /* Release the handle ref - we're done with it */
    chimera_vfs_open_cache_release(request->thread, cache, handle, 0);

    if (rc == -1) {
        /* Already silly renamed - this shouldn't happen normally, but just succeed */
        chimera_nfsclient_debug("Remove: file already silly renamed");
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    /* Successfully marked for silly rename - generate silly name and rename */
    ctx->silly_name_len = chimera_nfs3_silly_name_from_fh(request->remove_at.child_fh,
                                                          request->remove_at.child_fh_len,
                                                          ctx->silly_name, sizeof(ctx->silly_name));

    chimera_nfs3_remove_do_silly_rename(request, ctx);
} /* chimera_nfs3_remove_at */
