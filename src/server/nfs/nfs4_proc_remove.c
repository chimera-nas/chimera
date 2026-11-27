// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <xxhash.h>

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_attr.h"
#include "nfs4_named_attr.h"
#include "server/server.h"
#include "nfs_internal.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_claim.h"

/* REMOVE of a name inside a named-attribute directory deletes the named stream
 * of that name from the base file. */
static void
chimera_nfs4_remove_stream_complete(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data)
{
    struct nfs_request *req = private_data;
    struct REMOVE4res  *res = &req->res_compound.resarray[req->index].opremove;

    chimera_vfs_release(req->thread->vfs_thread, req->handle);
    req->handle = NULL;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* change_info4 is non-atomic and best-effort here (mirrors the ordinary
     * REMOVE path, which also leaves it zeroed). */
    res->resok4.cinfo.atomic = 0;
    res->resok4.cinfo.before = 0;
    res->resok4.cinfo.after  = 0;

    res->status = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_remove_stream_complete */

static void
chimera_nfs4_remove_attrdir_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req  = private_data;
    struct REMOVE4args *args = &req->args_compound->argarray[req->index].opremove;
    struct REMOVE4res  *res  = &req->res_compound.resarray[req->index].opremove;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    req->handle = handle;

    chimera_vfs_remove_stream(req->thread->vfs_thread, &req->cred,
                              handle,
                              args->target.data, args->target.len,
                              chimera_nfs4_remove_stream_complete,
                              req);
} /* chimera_nfs4_remove_attrdir_open_callback */

/*
 * REMOVE.  With delegations enabled the target is looked up first: its FH lets
 * a delegation be recalled before the remove (RFC 7530 §10.4.4).  The handle
 * found is passed to the VFS remove so it does not look the name up again.  The
 * VFS does everything else a last-link remove needs, for a pNFS file and for
 * every protocol alike.
 */
struct nfs4_remove_ctx {
    struct nfs_request             *req;
    struct chimera_vfs_open_handle *parent_handle;
    uint8_t                         child_fh[CHIMERA_VFS_FH_SIZE];
    int                             child_fh_len;
};

static void
nfs4_remove_finish(
    struct nfs4_remove_ctx *ctx,
    nfsstat4                status)
{
    struct nfs_request *req = ctx->req;
    struct REMOVE4res  *res = &req->res_compound.resarray[req->index].opremove;

    if (ctx->parent_handle) {
        chimera_vfs_release(req->thread->vfs_thread, ctx->parent_handle);
        ctx->parent_handle = NULL;
    }

    res->status = status;
    chimera_nfs4_compound_complete(req, status);
} /* nfs4_remove_finish */

static void
nfs4_remove_mds_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs4_remove_ctx *ctx = private_data;
    struct REMOVE4res      *res =
        &ctx->req->res_compound.resarray[ctx->req->index].opremove;

    if (error_code != CHIMERA_VFS_OK) {
        nfs4_remove_finish(ctx, chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    /* change_info4 for the parent directory (RFC 7530 §16.25.5), from its
     * change attribute pre/post the unlink. */
    chimera_nfs4_set_changeinfo(&res->resok4.cinfo, pre_attr, post_attr);

    nfs4_remove_finish(ctx, NFS4_OK);
} /* nfs4_remove_mds_complete */

static void
nfs4_remove_mds(struct nfs4_remove_ctx *ctx)
{
    struct nfs_request *req  = ctx->req;
    struct REMOVE4args *args = &req->args_compound->argarray[req->index].opremove;

    chimera_vfs_remove_at(req->thread->vfs_thread, &req->cred,
                          ctx->parent_handle,
                          args->target.data, args->target.len,
                          ctx->child_fh_len ? ctx->child_fh : NULL, ctx->child_fh_len, 0,
                          CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME,
                          CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME,
                          NULL,
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

    if (error_code == CHIMERA_VFS_OK &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        uint64_t fh_hash = XXH3_64bits(attr->va_fh, attr->va_fh_len) & INT64_MAX;

        /* RFC 7530 §10.4.4: a REMOVE of a delegated file must recall the
         * delegation first; if a caching lease is held, kick the recall and
         * tell the client to retry (NFS4ERR_DELAY).  The retry finds no lease
         * and the remove proceeds. */
        if (chimera_vfs_claim_break_caching(req->thread->vfs->vfs_state,
                                            attr->va_fh, attr->va_fh_len,
                                            fh_hash)) {
            nfs4_remove_finish(ctx, NFS4ERR_DELAY);
            return;
        }

        if (attr->va_fh_len <= sizeof(ctx->child_fh)) {
            memcpy(ctx->child_fh, attr->va_fh, attr->va_fh_len);
            ctx->child_fh_len = attr->va_fh_len;
        }
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

    if (chimera_server_config_get_nfs4_delegations(req->thread->shared->config)) {
        chimera_vfs_lookup_at(req->thread->vfs_thread, &req->cred,
                              parent_handle,
                              args->target.data, args->target.len,
                              CHIMERA_VFS_ATTR_FH, 0,
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

    /* REMOVE inside a named-attribute directory: drop the named stream. */
    if (chimera_nfs4_fh_is_attrdir(req->fh, req->fhlen)) {
        const uint8_t *base;
        int            base_len;

        chimera_nfs4_attrdir_base(req->fh, req->fhlen, &base, &base_len);

        chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                            base, base_len,
                            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                            chimera_nfs4_remove_attrdir_open_callback,
                            req);
        return;
    }

    ctx = xdr_dbuf_alloc_space(sizeof(*ctx), req->encoding->dbuf);
    chimera_nfs_abort_if(ctx == NULL, "Failed to allocate space");
    memset(ctx, 0, sizeof(*ctx));
    ctx->req = req;

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_nfs4_remove_open_callback,
                        ctx);
} /* chimera_nfs4_remove */
