// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_internal_procs.h"
#include "nfs3_compound.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"

/*
 * PUTFH, OPEN, GETATTR.  The open the handler used to make by hand is now the
 * sequence's, and the handle goes with it -- nothing here releases one.
 */
static void
chimera_nfs3_getattr_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs3_compound                 *ctx = private_data;

    if (nfs3_compound_retry(ctx)) {
        return;
    }
    struct nfs_request                   *req = ctx->req;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_attrs              result_attr;
    const struct chimera_vfs_attrs       *attr;
    struct chimera_server_nfs_thread     *thread = req->thread;
    struct chimera_server_nfs_shared     *shared = thread->shared;
    struct evpl                          *evpl   = thread->evpl;
    struct GETATTR3res                    res;
    int                                   rc;
    enum chimera_vfs_error                status;

    status = chimera_vfs_compound_status(compound);

    memset(&result_attr, 0, sizeof(result_attr));

    /* Out of the sequence before it is freed: a freed sequence is recycled
     * and reset. */
    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);
        result_attr = op->attr;
    }

    nfs3_compound_free(ctx);

    attr = &result_attr;

    memset(&res, 0, sizeof(res));

    res.status = chimera_vfs_error_to_nfsstat3(status);

    if (res.status == NFS3_OK) {
        chimera_nfs3_marshall_attrs(attr, &res.resok.obj_attributes);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_GETATTR(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_getattr_sequence_complete */

void
chimera_nfs3_getattr(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct GETATTR3args       *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct chimera_vfs_compound      *compound;
    struct GETATTR3res                res;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);

    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_getattr(req, args);
    nfs3_trace_getattr(req, args);

    req->args_getattr = args;

    res.status = chimera_nfs3_decode_fh(req, args->object.data.data, args->object.data.len);
    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_GETATTR(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);
    struct nfs3_compound *ctx = calloc(1, sizeof(*ctx));
    chimera_nfs_abort_if(!ctx, "NFS3 compound context allocation failed");
    ctx->req      = req;
    ctx->compound = compound;

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH, 0);
    chimera_vfs_compound_add_getattr(compound, CHIMERA_NFS3_ATTR_MASK);

    chimera_vfs_compound_submit(compound,
                                chimera_nfs3_getattr_sequence_complete, ctx);
} /* chimera_nfs3_getattr */
