// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_internal.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs.h"
#include "vfs/vfs_internal_procs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"
#include "nfs3_compound.h"

static void
chimera_nfs3_commit_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs3_compound                 *ctx = private_data;

    if (nfs3_compound_retry(ctx)) {
        return;
    }
    struct nfs_request                   *req       = ctx->req;
    const struct chimera_vfs_compound_op *op        = nfs3_compound_result(ctx);
    const struct chimera_vfs_attrs       *pre_attr  = &op->dir_pre_attr;
    const struct chimera_vfs_attrs       *post_attr = &op->dir_post_attr;

    struct chimera_server_nfs_thread     *thread = req->thread;
    struct chimera_server_nfs_shared     *shared = thread->shared;
    struct evpl                          *evpl   = thread->evpl;
    struct COMMIT3res                     res;
    int                                   rc;

    res.status = nfs3_compound_status(ctx);

    if (res.status == NFS3_OK) {
        chimera_nfs3_set_wcc_data(&res.resok.file_wcc, pre_attr, post_attr);

        memcpy(res.resok.verf,
               &shared->nfs_verifier,
               sizeof(res.resok.verf));
    } else {
        chimera_nfs3_set_wcc_data(&res.resfail.file_wcc, pre_attr, post_attr);
    }


    rc = shared->nfs_v3.send_reply_NFSPROC3_COMMIT(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs3_compound_free(ctx);
    nfs_request_free(thread, req);
} /* chimera_nfs3_commit_complete */

void
chimera_nfs3_commit(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct COMMIT3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct COMMIT3res                 res;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_commit(req, args);
    nfs3_trace_commit(req, args);

    req->args_commit = args;

    res.status = chimera_nfs3_decode_fh(req, args->file.data.data, args->file.data.len);
    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_COMMIT(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    struct nfs3_compound        *ctx      = nfs3_compound_alloc(req, CHIMERA_VFS_OPEN_INFERRED);
    struct chimera_vfs_compound *compound = ctx->compound;
    ctx->result = chimera_vfs_compound_add_commit(compound, args->offset, args->count, CHIMERA_NFS3_ATTR_WCC_MASK, 0);
    chimera_vfs_compound_set_result_masks(compound, ctx->result, CHIMERA_NFS3_ATTR_MASK, CHIMERA_NFS3_ATTR_WCC_MASK,
                                          CHIMERA_NFS3_ATTR_MASK);
    chimera_vfs_compound_submit(compound, chimera_nfs3_commit_complete, ctx);
} /* chimera_nfs3_commit */
