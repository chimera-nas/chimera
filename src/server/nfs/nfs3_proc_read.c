// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"

#include "nfs3_compound.h"

static void
chimera_nfs3_read_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs3_compound                 *ctx = private_data;

    if (nfs3_compound_retry(ctx)) {
        return;
    }
    struct nfs_request                   *req  = ctx->req;
    const struct chimera_vfs_compound_op *op   = nfs3_compound_result(ctx);
    const struct chimera_vfs_attrs       *attr = &op->attr;
    uint32_t                              count = op->read_len, eof = op->eof_read;
    struct evpl_iovec                    *iov  = op->iov;
    int                                   niov = op->niov;

    struct chimera_server_nfs_thread     *thread = req->thread;
    struct chimera_server_nfs_shared     *shared = thread->shared;
    struct evpl                          *evpl   = thread->evpl;
    struct READ3res                       res;
    int                                   rc;

    res.status = nfs3_compound_status(ctx);

    if (res.status == NFS3_OK) {
        res.resok.count       = count;
        res.resok.eof         = eof;
        res.resok.data.length = count;
        res.resok.data.iov    = iov;
        res.resok.data.niov   = niov;

        chimera_nfs3_set_post_op_attr(&res.resok.file_attributes, attr);
    } else {
        chimera_nfs3_set_post_op_attr(&res.resfail.file_attributes, attr);
    }

    if (res.status == NFS3_OK) {
        chimera_vfs_compound_take_iov(compound, ctx->result, &res.resok.data.iov, &res.resok.data.niov);
    }
    rc = shared->nfs_v3.send_reply_NFSPROC3_READ(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");


    nfs3_compound_free(ctx);
    nfs_request_free(thread, req);
} /* chimera_nfs3_read_complete */

void
chimera_nfs3_read(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct READ3args          *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct READ3res                   res;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_read(req, args);
    nfs3_trace_read(req, args);

    req->args_read = args;

    res.status = chimera_nfs3_decode_fh(req, args->file.data.data, args->file.data.len);
    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_READ(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    /* RFC 1813 3.3.6: a count above rtmax is served as a short read rather than
     * an error.  The clamp is load-bearing -- the read dispatch sizes its iovec
     * allocation from this count and aborts if the allocation fails, so an
     * unclamped wire count would take the daemon down. */
    if (args->count > CHIMERA_NFS3_MAX_XFER) {
        args->count = CHIMERA_NFS3_MAX_XFER;
    }

    struct nfs3_compound        *ctx      = nfs3_compound_alloc(req, CHIMERA_VFS_OPEN_INFERRED);
    struct chimera_vfs_compound *compound = ctx->compound;
    struct evpl_iovec           *iov      = xdr_dbuf_alloc_space(sizeof(*iov) * 256, req->encoding->dbuf);
    chimera_nfs_abort_if(!iov, "NFS3 READ descriptor allocation failed");
    ctx->result = chimera_vfs_compound_add_read(compound, NULL, args->offset, args->count, iov, 256, NULL);
    chimera_vfs_compound_set_result_masks(compound, ctx->result, CHIMERA_NFS3_ATTR_MASK, CHIMERA_NFS3_ATTR_WCC_MASK,
                                          CHIMERA_NFS3_ATTR_MASK);
    chimera_vfs_compound_submit(compound, chimera_nfs3_read_complete, ctx);
} /* chimera_nfs3_read */
