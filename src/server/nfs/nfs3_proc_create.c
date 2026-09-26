// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <fcntl.h>
#include <string.h>

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_internal_procs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"

#include "nfs3_compound.h"

static void
chimera_nfs3_create_prepare(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs3_compound *ctx = private_data;

    /* An empty protocol name must not become compound OPEN-current. */
    if (!ctx->req->args_create->where.name.len) {
        *status = CHIMERA_VFS_EINVAL;
    }
} /* chimera_nfs3_create_prepare */

static void
chimera_nfs3_create_verify(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs3_compound                 *ctx  = private_data;
    const struct CREATE3args             *args = ctx->req->args_create;
    const struct chimera_vfs_compound_op *op   = chimera_vfs_compound_op(compound, index);
    uint32_t                              atime, mtime;

    if (args->how.mode != EXCLUSIVE || !op->existed) {
        return;
    }
    /* A failed reopen still means the exclusive name collision lost. */
    if (*status != CHIMERA_VFS_OK) {
        *status = CHIMERA_VFS_EEXIST;
        return;
    }
    memcpy(&atime, args->how.verf, sizeof(atime));
    memcpy(&mtime, args->how.verf + sizeof(atime), sizeof(mtime));
    if (op->attr.va_atime.tv_sec != atime || op->attr.va_mtime.tv_sec != mtime) {
        *status = CHIMERA_VFS_EEXIST;
    }
} /* chimera_nfs3_create_verify */

static void
chimera_nfs3_create_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs3_compound                 *ctx = private_data;

    if (nfs3_compound_retry(ctx)) {
        return;
    }
    struct nfs_request                   *req = ctx->req;
    const struct chimera_vfs_compound_op *op  = nfs3_compound_result(ctx);
    struct CREATE3res                     res = { 0 };
    int                                   rc;
    res.status = nfs3_compound_status(ctx);
    if (res.status == NFS3_OK) {
        if (op->attr.va_set_mask & CHIMERA_VFS_ATTR_FH) {
            uint8_t wire[CHIMERA_NFS_FH_MAX];
            int     wirelen;
            chimera_nfs_fh_encode(req, op->out_handle->fh, op->out_handle->fh_len, wire, &wirelen);
            res.resok.obj.handle_follows = 1;
            rc                           = xdr_dbuf_opaque_copy(&res.resok.obj.handle.data, wire, wirelen, req->encoding
                                                                ->dbuf);
            chimera_nfs_abort_if(rc, "Failed to copy opaque");
        }
        chimera_nfs3_set_post_op_attr(&res.resok.obj_attributes, &op->attr);
        chimera_nfs3_set_wcc_data(&res.resok.dir_wcc, &op->dir_pre_attr, &op->dir_post_attr);
    } else {
        chimera_nfs3_set_wcc_data(&res.resfail.dir_wcc, &op->dir_pre_attr, &op->dir_post_attr);
    }
    rc = req->thread->shared->nfs_v3.send_reply_NFSPROC3_CREATE(req->thread->evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
    nfs3_compound_free(ctx);
    nfs_request_free(req->thread, req);
} /* chimera_nfs3_create_complete */

void
chimera_nfs3_create(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct CREATE3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct CREATE3res                 res;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_create(req, args);
    nfs3_trace_create(req, args);

    req->args_create = args;

    res.status = chimera_nfs3_decode_fh(req, args->where.dir.data.data, args->where.dir.data.len);
    if (res.status == NFS3_OK) {
        res.status = chimera_nfs3_check_rofs(req, req->export_id);
    }
    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_CREATE(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    struct nfs3_compound        *ctx = nfs3_compound_alloc(req,
                                                           CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                                                           CHIMERA_VFS_OPEN_DIRECTORY);
    struct chimera_vfs_compound *compound = ctx->compound;
    struct chimera_vfs_attrs     attrs    = { 0 };
    unsigned int                 flags    = CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_INFERRED;
    uint32_t                     opts     = 0;
    switch (args->how.mode) {
        case UNCHECKED:
            chimera_nfs3_sattr3_to_va(&attrs, &args->how.obj_attributes);
            flags |= CHIMERA_VFS_OPEN_CREATE_REGULAR;
            break;
        case GUARDED:
            chimera_nfs3_sattr3_to_va(&attrs, &args->how.obj_attributes);
            flags |= CHIMERA_VFS_OPEN_EXCLUSIVE;
            break;
        case EXCLUSIVE: {
            uint32_t lo, hi;
            flags            |= CHIMERA_VFS_OPEN_EXCLUSIVE;
            opts              = CHIMERA_VFS_COMPOUND_OPEN_EXCLUSIVE_RETRY;
            attrs.va_set_mask = CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME | CHIMERA_VFS_ATTR_MODE;
            attrs.va_mode     = 0600;
            memcpy(&lo, args->how.verf, sizeof(lo));
            memcpy(&hi, args->how.verf + sizeof(lo), sizeof(hi));
            attrs.va_atime.tv_sec = lo;
            attrs.va_mtime.tv_sec = hi;
            break;
        }
    } /* switch */
    ctx->result = chimera_vfs_compound_add_open(compound, args->where.name.str, args->where.name.len, flags, opts, &
                                                attrs, CHIMERA_NFS3_ATTR_MASK | CHIMERA_VFS_ATTR_FH, 0, 0);
    chimera_vfs_compound_set_result_masks(compound, ctx->result, CHIMERA_NFS3_ATTR_MASK | CHIMERA_VFS_ATTR_FH,
                                          CHIMERA_NFS3_ATTR_WCC_MASK, CHIMERA_NFS3_ATTR_MASK);
    chimera_vfs_compound_set_op_callbacks(compound, ctx->result, chimera_nfs3_create_prepare,
                                          chimera_nfs3_create_verify, ctx);
    chimera_vfs_compound_submit(compound, chimera_nfs3_create_complete, ctx);
} /* chimera_nfs3_create */
