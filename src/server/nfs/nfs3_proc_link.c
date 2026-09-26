// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_internal_procs.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"
#include "nfs3_compound.h"

static void
chimera_nfs3_link_complete(
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
    struct LINK3res                       res;
    int                                   rc;

    res.status = nfs3_compound_status(ctx);

    if (res.status == NFS3_OK) {
        chimera_nfs3_set_post_op_attr(&res.resok.file_attributes, r_attr);
        chimera_nfs3_set_wcc_data(&res.resok.linkdir_wcc, r_dir_pre_attr, r_dir_post_attr);
    } else {
        chimera_nfs3_set_post_op_attr(&res.resfail.file_attributes, r_attr);
        chimera_nfs3_set_wcc_data(&res.resfail.linkdir_wcc, r_dir_pre_attr, r_dir_post_attr);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_LINK(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs3_compound_free(ctx);
    nfs_request_free(thread, req);
} /* chimera_nfs3_link_complete */

void
chimera_nfs3_link(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct LINK3args          *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct LINK3res                   res;
    uint16_t                          linkdir_export_id = 0;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_link(req, args);
    nfs3_trace_link(req, args);

    /* Decode the existing-file handle (sets the request export + squash) and
     * the target-directory handle (authenticated into req->saved_fh, layering
     * its own export's sec= and squash policy on top).  The target handle must
     * outlive this (async) call, so it lives in the request rather than on the
     * stack (saved_fh is unused by NFSv3). */
    res.status = chimera_nfs3_decode_fh(req, args->file.data.data, args->file.data.len);
    if (res.status == NFS3_OK) {
        res.status = chimera_nfs3_decode_fh2(req, args->link.dir.data.data,
                                             args->link.dir.data.len, &linkdir_export_id);
    }
    /* The link directory gains an entry and the file's nlink changes, so
     * both exports must be writable. */
    if (res.status == NFS3_OK) {
        res.status = chimera_nfs3_check_rofs2(req, linkdir_export_id);
    }
    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_LINK(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    struct nfs3_compound        *ctx      = nfs3_compound_alloc(req, 0);
    struct chimera_vfs_compound *compound = ctx->compound;
    chimera_vfs_compound_add_savefh(compound);
    chimera_vfs_compound_add_putfh(compound, req->saved_fh, req->saved_fhlen);
    ctx->result = chimera_vfs_compound_add_link(compound, args->link.name.str, args->link.name.len,
                                                CHIMERA_NFS3_ATTR_MASK, 0, 0);
    chimera_vfs_compound_set_result_masks(compound, ctx->result, CHIMERA_NFS3_ATTR_MASK, CHIMERA_NFS3_ATTR_WCC_MASK,
                                          CHIMERA_NFS3_ATTR_MASK);
    chimera_vfs_compound_submit(compound, chimera_nfs3_link_complete, ctx);
} /* chimera_nfs3_link */
