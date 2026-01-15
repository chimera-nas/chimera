// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs3_open_state.h"
#include "nfs_common/nfs3_attr.h"
#include "nfs_common/nfs3_status.h"
#include "vfs/vfs_open_cache.h"
#include "vfs/vfs_internal.h"

/*
 * NFS3 Rename with Silly Rename Support
 *
 * When target_fh is provided and the target file is open, we first
 * hard link the target to a silly name (.nfsXXX), then perform the
 * actual rename. This preserves the target file for open handles
 * while maintaining atomicity of the rename operation.
 *
 * When target_fh is NULL (e.g., from NFS server or when target doesn't exist),
 * we skip silly rename handling and proceed with normal rename.
 */

struct chimera_nfs3_rename_ctx {
    struct chimera_nfs_thread        *thread;
    struct chimera_nfs_shared        *shared;
    struct chimera_nfs_client_server *server;
    char                              silly_name[5 + CHIMERA_VFS_FH_SIZE * 2 + 1];
    int                               silly_name_len;
};

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

static void
chimera_nfs3_rename_do_rename(
    struct chimera_vfs_request     *request,
    struct chimera_nfs3_rename_ctx *ctx)
{
    struct chimera_nfs_client_server_thread *server_thread;
    struct RENAME3args                       args;
    uint8_t                                 *old_fh, *new_fh;
    int                                      old_fhlen, new_fhlen;

    server_thread = chimera_nfs_thread_get_server_thread(ctx->thread, request->fh, request->fh_len);

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

    ctx->shared->nfs_v3.send_call_NFSPROC3_RENAME(&ctx->shared->nfs_v3.rpc2, ctx->thread->evpl,
                                                  server_thread->nfs_conn, &args,
                                                  0, 0, 0,
                                                  chimera_nfs3_rename_callback, request);
} /* chimera_nfs3_rename_do_rename */

static void
chimera_nfs3_rename_link_callback(
    struct evpl     *evpl,
    struct LINK3res *res,
    int              status,
    void            *private_data)
{
    struct chimera_vfs_request     *request = private_data;
    struct chimera_nfs3_rename_ctx *ctx     = request->plugin_data;

    if (unlikely(status)) {
        request->status = CHIMERA_VFS_EFAULT;
        request->complete(request);
        return;
    }

    if (res->status != NFS3_OK) {
        /* Hard link failed - still try to do the rename.
         * The worst case is the open file becomes inaccessible. */
        chimera_nfsclient_debug("Rename: hard link for silly rename failed with %d, proceeding anyway",
                                res->status);
    }

    /* Now do the actual rename */
    chimera_nfs3_rename_do_rename(request, ctx);

} /* chimera_nfs3_rename_link_callback */

static void
chimera_nfs3_rename_do_silly_link(
    struct chimera_vfs_request     *request,
    struct chimera_nfs3_rename_ctx *ctx)
{
    struct chimera_nfs_client_server_thread *server_thread;
    struct LINK3args                         args;
    uint8_t                                 *target_fh, *dir_fh;
    int                                      target_fhlen, dir_fhlen;

    server_thread = chimera_nfs_thread_get_server_thread(ctx->thread, request->fh, request->fh_len);

    if (!server_thread) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    /* Map file handles - target_fh is the file being overwritten, new_fh is the dest directory */
    chimera_nfs3_map_fh(request->rename.target_fh, request->rename.target_fh_len, &target_fh, &target_fhlen);
    chimera_nfs3_map_fh(request->rename.new_fh, request->rename.new_fhlen, &dir_fh, &dir_fhlen);

    /* Link the target file to the silly name in the same directory */
    args.file.data.data     = target_fh;
    args.file.data.len      = target_fhlen;
    args.link.dir.data.data = dir_fh;
    args.link.dir.data.len  = dir_fhlen;
    args.link.name.str      = ctx->silly_name;
    args.link.name.len      = ctx->silly_name_len;

    ctx->shared->nfs_v3.send_call_NFSPROC3_LINK(&ctx->shared->nfs_v3.rpc2, ctx->thread->evpl,
                                                server_thread->nfs_conn, &args,
                                                0, 0, 0,
                                                chimera_nfs3_rename_link_callback, request);
} /* chimera_nfs3_rename_do_silly_link */

void
chimera_nfs3_rename(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_server_thread *server_thread;
    struct chimera_nfs3_rename_ctx          *ctx;
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
     * If no target FH provided, skip silly rename handling entirely.
     * This happens when:
     * - The caller is an NFS server serving external clients
     * - The target file doesn't exist
     */
    if (!request->rename.target_fh || request->rename.target_fh_len == 0) {
        chimera_nfs3_rename_do_rename(request, ctx);
        return;
    }

    /*
     * Target FH provided - check if file is open and needs silly rename.
     */
    cache   = request->thread->vfs->vfs_open_file_cache;
    fh_hash = chimera_vfs_hash(request->rename.target_fh, request->rename.target_fh_len);
    handle  = chimera_vfs_open_cache_lookup_ref(cache, request->rename.target_fh,
                                                request->rename.target_fh_len, fh_hash);

    if (!handle) {
        /* File is not open, proceed with normal rename */
        chimera_nfs3_rename_do_rename(request, ctx);
        return;
    }

    /* File is open - get NFS3 state and mark for silly rename */
    state = (struct chimera_nfs3_open_state *) handle->vfs_private;

    if (!state) {
        /* No NFS3 state attached (shouldn't happen but handle gracefully) */
        chimera_vfs_open_cache_release(request->thread, cache, handle, 0);
        chimera_nfs3_rename_do_rename(request, ctx);
        return;
    }

    /* Mark the state as silly renamed so close will remove the silly file */
    rc = chimera_nfs3_open_state_mark_silly(state, request->rename.new_fh, request->rename.new_fhlen);

    /* Release the handle ref - we're done with it */
    chimera_vfs_open_cache_release(request->thread, cache, handle, 0);

    if (rc == -1) {
        /* Already silly renamed - just do the rename */
        chimera_nfsclient_debug("Rename: target file already silly renamed");
        chimera_nfs3_rename_do_rename(request, ctx);
        return;
    }

    /* Generate silly name from target FH and do hard link + rename */
    ctx->silly_name_len = chimera_nfs3_silly_name_from_fh(request->rename.target_fh,
                                                          request->rename.target_fh_len,
                                                          ctx->silly_name, sizeof(ctx->silly_name));

    chimera_nfs3_rename_do_silly_link(request, ctx);
} /* chimera_nfs3_rename */
