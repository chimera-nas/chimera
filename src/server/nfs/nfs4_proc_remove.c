// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <xxhash.h>

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "server/server.h"
#include "nfs_internal.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_state.h"
#include "vfs/vfs_pnfs.h"

/*
 * REMOVE.  When pNFS is enabled the target may be a flex-files file whose data
 * lives in a backing file on a data server; removing the last link must also
 * delete that backing file or it leaks.  The pNFS layout state is an opaque
 * attribute on the file (CHIMERA_VFS_ATTR_PNFS_LAYOUT, {deviceid, backing-fh}),
 * so we LOOKUP the target first to capture it, remove the MDS file, then -- if
 * it was the last link to a pNFS-backed file -- delete the backing file on the
 * steered data server (best effort: the MDS namespace entry is already gone).
 */
struct nfs4_remove_ctx {
    struct nfs_request             *req;
    struct chimera_vfs_open_handle *parent_handle;
    struct chimera_vfs_open_handle *ds_root_handle;
    uint8_t                         deviceid[CHIMERA_VFS_DEVICEID_SIZE];
    uint64_t                        fileid;
    int                             have_layout;
    char                            backing_name[24];
};

static void
nfs4_remove_finish(
    struct nfs4_remove_ctx *ctx,
    nfsstat4                status)
{
    struct nfs_request *req = ctx->req;
    struct REMOVE4res  *res = &req->res_compound.resarray[req->index].opremove;

    if (ctx->ds_root_handle) {
        chimera_vfs_release(req->thread->vfs_thread, ctx->ds_root_handle);
        ctx->ds_root_handle = NULL;
    }
    if (ctx->parent_handle) {
        chimera_vfs_release(req->thread->vfs_thread, ctx->parent_handle);
        ctx->parent_handle = NULL;
    }

    res->status = status;
    chimera_nfs4_compound_complete(req, status);
} /* nfs4_remove_finish */

static void
nfs4_remove_ds_backing_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    if (error_code != CHIMERA_VFS_OK) {
        chimera_nfs_error(
            "pNFS: failed to delete data-server backing file (err=%d); space leaked",
            error_code);
    }

    /* Best effort: the MDS file is already gone, so a failure to delete the
     * backing file only leaks data-server space; do not fail the REMOVE. */
    nfs4_remove_finish(private_data, NFS4_OK);
} /* nfs4_remove_ds_backing_complete */

static void
nfs4_remove_ds_root_opened(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs4_remove_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        nfs4_remove_finish(ctx, NFS4_OK);
        return;
    }

    ctx->ds_root_handle = handle;

    snprintf(ctx->backing_name, sizeof(ctx->backing_name), "%016lx", ctx->fileid);

    chimera_nfs_info("pNFS: deleting data-server backing file %s for removed file",
                     ctx->backing_name);

    chimera_vfs_remove_at(ctx->req->thread->vfs_thread, &ctx->req->cred,
                          ctx->ds_root_handle,
                          ctx->backing_name, strlen(ctx->backing_name),
                          NULL, 0, 0, 0,
                          nfs4_remove_ds_backing_complete, ctx);
} /* nfs4_remove_ds_root_opened */

static void
nfs4_remove_mds_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs4_remove_ctx      *ctx = private_data;
    struct chimera_vfs          *vfs = ctx->req->thread->shared->vfs;
    const struct chimera_vfs_ds *ds;

    if (error_code != CHIMERA_VFS_OK) {
        nfs4_remove_finish(ctx, chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    if (!ctx->have_layout) {
        nfs4_remove_finish(ctx, NFS4_OK);
        return;
    }

    ds = chimera_vfs_pnfs_find_device(vfs, ctx->deviceid);
    if (!ds || ds->root_fh_len == 0) {
        nfs4_remove_finish(ctx, NFS4_OK);
        return;
    }

    chimera_vfs_open_fh(ctx->req->thread->vfs_thread, &ctx->req->cred, NULL,
                        ds->root_fh, ds->root_fh_len,
                        CHIMERA_VFS_OPEN_DIRECTORY | CHIMERA_VFS_OPEN_INFERRED,
                        nfs4_remove_ds_root_opened, ctx);
} /* nfs4_remove_mds_complete */

static void
nfs4_remove_mds(struct nfs4_remove_ctx *ctx)
{
    struct nfs_request *req  = ctx->req;
    struct REMOVE4args *args = &req->args_compound->argarray[req->index].opremove;

    chimera_vfs_remove_at(req->thread->vfs_thread, &req->cred,
                          ctx->parent_handle,
                          args->target.data, args->target.len,
                          NULL, 0, 0, 0,
                          nfs4_remove_mds_complete, ctx);
} /* nfs4_remove_mds */

static void
nfs4_remove_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct nfs4_remove_ctx *ctx = private_data;
    struct nfs_request     *req = ctx->req;

    /* RFC 7530 §10.4.5: a REMOVE of a delegated file must recall the
     * delegation first.  We looked the target up to learn its FH; if a caching
     * lease is held, kick the recall and tell the client to retry
     * (NFS4ERR_DELAY).  The retry finds no lease and the remove proceeds. */
    if (chimera_server_config_get_nfs4_delegations(req->thread->shared->config) &&
        error_code == CHIMERA_VFS_OK &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        uint64_t fh_hash = XXH3_64bits(attr->va_fh, attr->va_fh_len) & INT64_MAX;

        if (chimera_vfs_state_break_caching(req->thread->vfs->vfs_state,
                                            attr->va_fh, attr->va_fh_len,
                                            fh_hash)) {
            nfs4_remove_finish(ctx, NFS4ERR_DELAY);
            return;
        }
    }

    /* Capture the pNFS layout iff this is the last link to the file; a file
     * with remaining hard links must keep its backing data. */
    if (error_code == CHIMERA_VFS_OK &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_PNFS_LAYOUT) &&
        attr->va_pnfs_len >= CHIMERA_VFS_DEVICEID_SIZE &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_NLINK) && attr->va_nlink == 1) {
        memcpy(ctx->deviceid, attr->va_pnfs, CHIMERA_VFS_DEVICEID_SIZE);
        ctx->fileid      = (attr->va_set_mask & CHIMERA_VFS_ATTR_INUM) ? attr->va_ino : 0;
        ctx->have_layout = 1;
    }

    nfs4_remove_mds(ctx);
} /* nfs4_remove_lookup_complete */

static void
chimera_nfs4_remove_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *parent_handle,
    void                           *private_data)
{
    struct nfs4_remove_ctx *ctx  = private_data;
    struct nfs_request     *req  = ctx->req;
    struct REMOVE4args     *args = &req->args_compound->argarray[req->index].opremove;

    if (error_code != CHIMERA_VFS_OK) {
        nfs4_remove_finish(ctx, chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    ctx->parent_handle = parent_handle;

    /* Look the victim up first when delegations OR pNFS are in play: the FH lets
     * us recall a delegation (RFC 7530 §10.4.5) before the remove, and the pNFS
     * attrs let us delete its data-server backing file afterwards.  Skip the
     * extra LOOKUP entirely otherwise (have_layout stays 0). */
    if (chimera_server_config_get_nfs4_delegations(req->thread->shared->config) ||
        chimera_vfs_pnfs_enabled(req->thread->shared->vfs)) {
        chimera_vfs_lookup_at(req->thread->vfs_thread, &req->cred, NULL,
                              parent_handle,
                              args->target.data, args->target.len,
                              CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_PNFS_LAYOUT |
                              CHIMERA_VFS_ATTR_NLINK | CHIMERA_VFS_ATTR_INUM,
                              0,
                              nfs4_remove_lookup_complete, ctx);
    } else {
        nfs4_remove_mds(ctx);
    }
} /* chimera_nfs4_remove_open_callback */

void
chimera_nfs4_remove(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct REMOVE4args     *args = &argop->opremove;
    struct REMOVE4res      *res  = &resop->opremove;
    struct nfs4_remove_ctx *ctx;

    req->handle = NULL;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    res->status = chimera_nfs4_validate_name(&args->target);

    if (res->status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    ctx = xdr_dbuf_alloc_space(sizeof(*ctx), req->encoding->dbuf);
    chimera_nfs_abort_if(ctx == NULL, "Failed to allocate space");
    memset(ctx, 0, sizeof(*ctx));
    ctx->req = req;

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred, NULL,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_nfs4_remove_open_callback,
                        ctx);
} /* chimera_nfs4_remove */
