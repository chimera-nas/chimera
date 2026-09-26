// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "server/server.h"
#include "vfs/vfs_procs.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"

#include "nfs3_compound.h"

static void
chimera_nfs3_rename_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs3_compound                 *ctx = private_data;

    if (nfs3_compound_retry(ctx)) {
        return;
    }
    struct nfs_request                   *req               = ctx->req;
    const struct chimera_vfs_compound_op *op                = nfs3_compound_result(ctx);
    const struct chimera_vfs_attrs       *fromdir_pre_attr  = &op->from_dir_pre_attr;
    const struct chimera_vfs_attrs       *fromdir_post_attr = &op->from_dir_post_attr;
    const struct chimera_vfs_attrs       *todir_pre_attr    = &op->dir_pre_attr;
    const struct chimera_vfs_attrs       *todir_post_attr   = &op->dir_post_attr;

    struct chimera_server_nfs_thread     *thread = req->thread;
    struct chimera_server_nfs_shared     *shared = thread->shared;
    struct evpl                          *evpl   = thread->evpl;
    struct RENAME3res                     res;
    int                                   rc;

    res.status = nfs3_compound_status(ctx);

    if (res.status == NFS3_OK) {
        chimera_nfs3_set_wcc_data(&res.resok.fromdir_wcc, fromdir_pre_attr, fromdir_post_attr);
        chimera_nfs3_set_wcc_data(&res.resok.todir_wcc, todir_pre_attr, todir_post_attr);
    } else {
        chimera_nfs3_set_wcc_data(&res.resfail.fromdir_wcc, fromdir_pre_attr, fromdir_post_attr);
        chimera_nfs3_set_wcc_data(&res.resfail.todir_wcc, todir_pre_attr, todir_post_attr);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_RENAME(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs3_compound_free(ctx);
    nfs_request_free(thread, req);
} /* chimera_nfs3_rename_complete */

void
chimera_nfs3_rename(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct RENAME3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct RENAME3res                 res;
    uint16_t                          todir_export_id = 0;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_rename(req, args);
    nfs3_trace_rename(req, args);

    /* Decode both directory handles: the source sets the request export (and
     * squash); the destination is authenticated into req->saved_fh and layers
     * its own export's sec= and squash policy on top.  Both must outlive this
     * (async) call, so they go in the request, not on the stack. */
    res.status = chimera_nfs3_decode_fh(req, args->from.dir.data.data, args->from.dir.data.len);
    if (res.status == NFS3_OK) {
        res.status = chimera_nfs3_decode_fh2(req, args->to.dir.data.data,
                                             args->to.dir.data.len, &todir_export_id);
    }
    /* Both directories are mutated, so both exports must be writable. */
    if (res.status == NFS3_OK) {
        res.status = chimera_nfs3_check_rofs2(req, todir_export_id);
    }
    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_RENAME(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    req->args_rename = args;

    struct nfs3_compound        *ctx      = nfs3_compound_alloc(req, 0);
    struct chimera_vfs_compound *compound = ctx->compound;
    chimera_vfs_compound_add_savefh(compound);
    chimera_vfs_compound_add_putfh(compound, req->saved_fh, req->saved_fhlen);
    ctx->result = chimera_vfs_compound_add_rename(compound, args->from.name.str, args->from.name.len, args->to.name.str,
                                                  args->to.name.len, CHIMERA_VFS_REMOVE_RECALL);
    chimera_vfs_compound_set_result_masks(compound, ctx->result, CHIMERA_NFS3_ATTR_MASK, CHIMERA_NFS3_ATTR_WCC_MASK,
                                          CHIMERA_NFS3_ATTR_MASK);
    chimera_vfs_compound_submit(compound, chimera_nfs3_rename_complete, ctx);
} /* chimera_nfs3_rename */
