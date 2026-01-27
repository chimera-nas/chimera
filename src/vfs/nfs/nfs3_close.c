// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"
#include "nfs3_open_state.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"

struct chimera_nfs3_close_ctx {
    struct chimera_nfs_thread *thread;
    struct chimera_nfs_shared *shared;
    int                        was_last;
    int                        dirty;
    int                        silly_renamed;
    uint8_t                    dir_fh[CHIMERA_VFS_FH_SIZE]; /* Copied from open state before release */
    int                        dir_fh_len;
    struct chimera_vfs_cred    silly_remove_cred; /* Cred from REMOVE that triggered silly rename */
};

static void
chimera_nfs3_close_complete(struct chimera_vfs_request *request)
{
    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs3_close_complete */

static void
chimera_nfs3_close_remove_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct REMOVE3res           *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request *request = private_data;

    /* We don't fail the close if the silly remove fails - the file is already
     * closed from the client's perspective. Just log and continue. */
    if (unlikely(status)) {
        chimera_nfsclient_error("Silly remove failed with transport error");
    } else if (res->status != NFS3_OK) {
        chimera_nfsclient_error("Silly remove failed with NFS status %d", res->status);
    }

    chimera_nfs3_close_complete(request);
} /* chimera_nfs3_close_remove_callback */

static void
chimera_nfs3_close_do_silly_remove(
    struct chimera_vfs_request    *request,
    struct chimera_nfs3_close_ctx *ctx)
{
    struct chimera_nfs_client_server_thread *server_thread;
    struct REMOVE3args                       args;
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;
    char                                     silly_name[5 + CHIMERA_VFS_FH_SIZE * 2 + 1];
    int                                      silly_name_len;

    server_thread = chimera_nfs_thread_get_server_thread(ctx->thread, ctx->dir_fh, ctx->dir_fh_len);

    if (!server_thread) {
        /* Can't find server thread, just complete */
        chimera_nfsclient_error("Silly remove: cannot get server thread");
        chimera_nfs3_close_complete(request);
        return;
    }

    /* Construct silly name from file handle */
    silly_name_len = chimera_nfs3_silly_name_from_fh(request->fh, request->fh_len,
                                                     silly_name, sizeof(silly_name));

    chimera_nfs3_map_fh(ctx->dir_fh, ctx->dir_fh_len, &fh, &fhlen);

    args.object.dir.data.data = fh;
    args.object.dir.data.len  = fhlen;
    args.object.name.str      = silly_name;
    args.object.name.len      = silly_name_len;

    chimera_nfs_init_rpc2_cred(&rpc2_cred, &ctx->silly_remove_cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    ctx->shared->nfs_v3.send_call_NFSPROC3_REMOVE(&ctx->shared->nfs_v3.rpc2, ctx->thread->evpl,
                                                  server_thread->nfs_conn, &rpc2_cred, &args,
                                                  0, 0, 0,
                                                  chimera_nfs3_close_remove_callback, request);
} /* chimera_nfs3_close_do_silly_remove */

static void
chimera_nfs3_close_commit_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMMIT3res           *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct chimera_nfs3_close_ctx *ctx     = request->plugin_data;

    /* We don't fail the close if commit fails - data may be lost but the
     * close operation itself should succeed. Log the error. */
    if (unlikely(status)) {
        chimera_nfsclient_error("Close commit failed with transport error");
    } else if (res->status != NFS3_OK) {
        chimera_nfsclient_error("Close commit failed with NFS status %d", res->status);
    }

    /* If this was the last reference and the file was silly renamed, remove it */
    if (ctx->was_last && ctx->silly_renamed) {
        chimera_nfs3_close_do_silly_remove(request, ctx);
        return;
    }

    chimera_nfs3_close_complete(request);
} /* chimera_nfs3_close_commit_callback */

static void
chimera_nfs3_close_do_commit(
    struct chimera_vfs_request    *request,
    struct chimera_nfs3_close_ctx *ctx)
{
    struct chimera_nfs_client_server_thread *server_thread;
    struct COMMIT3args                       args;
    struct evpl_rpc2_cred                    rpc2_cred;
    uint8_t                                 *fh;
    int                                      fhlen;

    server_thread = chimera_nfs_thread_get_server_thread(ctx->thread, request->fh, request->fh_len);

    if (!server_thread) {
        /* Can't find server thread, skip commit and continue */
        chimera_nfsclient_error("Close commit: cannot get server thread");
        if (ctx->was_last && ctx->silly_renamed) {
            chimera_nfs3_close_do_silly_remove(request, ctx);
            return;
        }
        chimera_nfs3_close_complete(request);
        return;
    }

    chimera_nfs3_map_fh(request->fh, request->fh_len, &fh, &fhlen);

    args.file.data.data = fh;
    args.file.data.len  = fhlen;
    args.offset         = 0;
    args.count          = 0; /* Commit entire file */

    chimera_nfs_init_rpc2_cred(&rpc2_cred, request->cred,
                               request->thread->vfs->machine_name,
                               request->thread->vfs->machine_name_len);

    ctx->shared->nfs_v3.send_call_NFSPROC3_COMMIT(&ctx->shared->nfs_v3.rpc2, ctx->thread->evpl,
                                                  server_thread->nfs_conn, &rpc2_cred, &args,
                                                  0, 0, 0,
                                                  chimera_nfs3_close_commit_callback, request);
} /* chimera_nfs3_close_do_commit */

void
chimera_nfs3_close(
    struct chimera_nfs_thread  *thread,
    struct chimera_nfs_shared  *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs3_close_ctx  *ctx;
    struct chimera_nfs3_open_state *open_state;

    ctx = request->plugin_data;

    ctx->thread = thread;
    ctx->shared = shared;

    open_state = (struct chimera_nfs3_open_state *) request->close.vfs_private;

    /* Handle case where no open state was tracked (shouldn't happen but not fatal) */
    if (!open_state) {
        chimera_nfsclient_debug("Close: no open state tracked");
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    /* Extract state info before freeing. Copy dir_fh since we need it for
     * async silly remove after state is freed. */
    ctx->dirty         = chimera_nfs3_open_state_get_dirty(open_state);
    ctx->silly_renamed = open_state->silly_renamed;
    ctx->was_last      = 1; /* Each open handle has its own state, always last */

    if (ctx->silly_renamed) {
        ctx->dir_fh_len = open_state->dir_fh_len;
        memcpy(ctx->dir_fh, open_state->dir_fh, open_state->dir_fh_len);
        ctx->silly_remove_cred = open_state->silly_remove_cred;
    }

    /* Free the open state */
    chimera_nfs3_open_state_free(open_state);

    /* If dirty data exists, issue commit first */
    if (ctx->dirty) {
        chimera_nfs3_close_do_commit(request, ctx);
        return;
    }

    /* If the file was silly renamed, remove it */
    if (ctx->silly_renamed) {
        chimera_nfs3_close_do_silly_remove(request, ctx);
        return;
    }

    /* Nothing to do, close complete */
    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* chimera_nfs3_close */
