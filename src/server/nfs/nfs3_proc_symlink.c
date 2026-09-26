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
chimera_nfs3_symlink_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs3_compound                 *ctx = private_data;

    if (nfs3_compound_retry(ctx)) {
        return;
    }
    struct nfs_request                   *req             = ctx->req;
    const struct chimera_vfs_compound_op *op              = nfs3_compound_result(ctx);
    const struct chimera_vfs_attrs       *r_attr          = &op->attr;
    const struct chimera_vfs_attrs       *r_dir_pre_attr  = &op->dir_pre_attr;
    const struct chimera_vfs_attrs       *r_dir_post_attr = &op->dir_post_attr;

    struct chimera_server_nfs_thread     *thread = req->thread;
    struct chimera_server_nfs_shared     *shared = thread->shared;
    struct evpl                          *evpl   = thread->evpl;
    struct SYMLINK3res                    res;
    int                                   rc;

    res.status = nfs3_compound_status(ctx);

    if (res.status == NFS3_OK) {
        if (r_attr->va_set_mask & CHIMERA_VFS_ATTR_FH) {
            uint8_t wire[CHIMERA_NFS_FH_MAX];
            int     wirelen;

            chimera_nfs_fh_encode(req, r_attr->va_fh, r_attr->va_fh_len, wire, &wirelen);
            res.resok.obj.handle_follows = 1;
            rc                           = xdr_dbuf_opaque_copy(&res.resok.obj.handle.data,
                                                                wire,
                                                                wirelen,
                                                                req->encoding->dbuf);
            chimera_nfs_abort_if(rc, "Failed to copy opaque");
        } else {
            res.resok.obj.handle_follows = 0;
        }

        chimera_nfs3_set_post_op_attr(&res.resok.obj_attributes, r_attr);
        chimera_nfs3_set_wcc_data(&res.resok.dir_wcc, r_dir_pre_attr, r_dir_post_attr);
    } else {
        chimera_nfs3_set_wcc_data(&res.resfail.dir_wcc, r_dir_pre_attr, r_dir_post_attr);
    }


    rc = shared->nfs_v3.send_reply_NFSPROC3_SYMLINK(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs3_compound_free(ctx);
    nfs_request_free(thread, req);
} /* chimera_nfs3_symlink_complete */

void
chimera_nfs3_symlink(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct SYMLINK3args       *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct SYMLINK3res                res;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_symlink(req, args);
    nfs3_trace_symlink(req, args);
    req->args_symlink = args;

    res.status = chimera_nfs3_decode_fh(req, args->where.dir.data.data, args->where.dir.data.len);
    if (res.status == NFS3_OK) {
        res.status = chimera_nfs3_check_rofs(req, req->export_id);
    }
    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_SYMLINK(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    struct nfs3_compound        *ctx = nfs3_compound_alloc(req, CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                                                           CHIMERA_VFS_OPEN_DIRECTORY);
    struct chimera_vfs_compound *compound = ctx->compound;
    struct chimera_vfs_attrs     attrs    = { 0 };
    chimera_nfs3_sattr3_to_va(&attrs, &args->symlink.symlink_attributes);
    ctx->result = chimera_vfs_compound_add_create(compound, CHIMERA_VFS_COMPOUND_CREATE_SYMLINK, args->where.name.str,
                                                  args->where.name.len, args->symlink.symlink_data.str, args->symlink.
                                                  symlink_data.len, &attrs, CHIMERA_NFS3_ATTR_MASK | CHIMERA_VFS_ATTR_FH
                                                  , 0, 0);
    chimera_vfs_compound_set_result_masks(compound, ctx->result, CHIMERA_NFS3_ATTR_MASK | CHIMERA_VFS_ATTR_FH,
                                          CHIMERA_NFS3_ATTR_WCC_MASK | CHIMERA_VFS_ATTR_ATOMIC, CHIMERA_NFS3_ATTR_MASK);
    chimera_vfs_compound_submit(compound, chimera_nfs3_symlink_complete, ctx);
} /* chimera_nfs3_symlink */
