// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_internal_procs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"
#include "nfs3_compound.h"

static void
chimera_nfs3_setattr_guard(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs3_compound           *ctx  = private_data;
    const struct SETATTR3args      *args = ctx->req->args_setattr;
    const struct chimera_vfs_attrs *attr = &chimera_vfs_compound_op(compound, index)->attr;

    if (*status == CHIMERA_VFS_OK &&
        (!(attr->va_set_mask & CHIMERA_VFS_ATTR_CTIME) ||
         attr->va_ctime.tv_sec != args->guard.obj_ctime.seconds ||
         attr->va_ctime.tv_nsec != args->guard.obj_ctime.nseconds)) {
        ctx->protocol_status = NFS3ERR_NOT_SYNC;
        *status              = CHIMERA_VFS_EINVAL;
    }
} /* chimera_nfs3_setattr_guard */

static void
chimera_nfs3_setattr_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs3_compound                 *ctx = private_data;

    if (nfs3_compound_retry(ctx)) {
        return;
    }
    struct nfs_request                   *req    = ctx->req;
    const struct chimera_vfs_compound_op *op     = nfs3_compound_result(ctx);
    const struct chimera_vfs_compound_op *opened = chimera_vfs_compound_op(compound, 1);
    struct SETATTR3res                    res    = { 0 };
    res.status = nfs3_compound_status(ctx);
    /* A size-setting open can reject nonregular objects before SETATTR. */
    if (chimera_vfs_compound_finish_status(compound) == CHIMERA_VFS_OK &&
        req->args_setattr->new_attributes.size.set_it &&
        (opened->status == CHIMERA_VFS_ELOOP || opened->status == CHIMERA_VFS_ENXIO)) {
        res.status = NFS3ERR_INVAL;
    }
    if (res.status == NFS3_OK) {
        chimera_nfs3_set_wcc_data(&res.resok.obj_wcc, &op->dir_pre_attr, &op->dir_post_attr);
    } else {
        chimera_nfs3_set_wcc_data(&res.resfail.obj_wcc, &op->dir_pre_attr, &op->dir_post_attr);
    }
    int rc = req->thread->shared->nfs_v3.send_reply_NFSPROC3_SETATTR(req->thread->evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
    nfs3_compound_free(ctx);
    nfs_request_free(req->thread, req);
} /* chimera_nfs3_setattr_complete */

void
chimera_nfs3_setattr(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct SETATTR3args       *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct SETATTR3res                res;
    unsigned int                      open_flags;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_setattr(req, args);
    nfs3_trace_setattr(req, args);

    req->args_setattr = args;

    res.status = chimera_nfs3_decode_fh(req, args->object.data.data, args->object.data.len);
    if (res.status == NFS3_OK) {
        res.status = chimera_nfs3_check_rofs(req, req->export_id);
    }
    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_SETATTR(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    /*
     * Use a real file handle instead of OPEN_PATH when setting size, because
     * ftruncate() requires a real file descriptor, not an O_PATH handle.
     */
    if (args->new_attributes.size.set_it) {
        open_flags = CHIMERA_VFS_OPEN_INFERRED;
    } else {
        open_flags = CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH;
    }

    struct nfs3_compound        *ctx      = nfs3_compound_alloc(req, open_flags);
    struct chimera_vfs_compound *compound = ctx->compound;
    struct chimera_vfs_attrs     attrs    = { 0 };
    chimera_nfs3_sattr3_to_va(&attrs, &args->new_attributes);
    if (args->guard.check) {
        int guard = chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_CTIME);
        chimera_vfs_compound_set_op_callbacks(compound, guard, NULL, chimera_nfs3_setattr_guard, ctx);
    }
    ctx->result = chimera_vfs_compound_add_setattr(compound, NULL, &attrs, 0, CHIMERA_NFS3_ATTR_MASK);
    chimera_vfs_compound_set_result_masks(compound, ctx->result, 0, CHIMERA_NFS3_ATTR_WCC_MASK, CHIMERA_NFS3_ATTR_MASK);
    chimera_vfs_compound_submit(compound, chimera_nfs3_setattr_complete, ctx);
} /* chimera_nfs3_setattr */
