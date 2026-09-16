// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"

static void
chimera_nfs3_read_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct READ3res                   res;
    int                               rc;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

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

    rc = shared->nfs_v3.send_reply_NFSPROC3_READ(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    /* The open belonged to the sequence and went with it. */

    nfs_request_free(thread, req);
} /* chimera_nfs3_read_complete */

/*
 * PUTFH, OPEN, READ.  The data iovecs are TAKEN from the sequence: they are the
 * reply's payload and have to outlive the sequence that produced them.
 */
static void
chimera_nfs3_read_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_attrs              attr;
    struct evpl_iovec                    *iov = NULL;
    enum chimera_vfs_error                status;
    uint32_t                              idx, count = 0, eof = 0;
    int                                   niov = 0;

    status = chimera_vfs_compound_status(compound);

    memset(&attr, 0, sizeof(attr));

    idx = chimera_vfs_compound_num_ops(compound) - 1;
    op  = chimera_vfs_compound_op(compound, idx);

    attr = op->attr;

    if (status == CHIMERA_VFS_OK) {
        count = op->read_len;
        eof   = op->eof_read;
        chimera_vfs_compound_take_iov(compound, idx, &iov, &niov);
    }

    chimera_vfs_compound_free(compound);

    chimera_nfs3_read_complete(status, count, eof, iov, niov, &attr, req);
} /* chimera_nfs3_read_sequence_complete */

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
    struct chimera_vfs_compound      *compound;
    struct evpl_iovec                *iov;
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

    iov = xdr_dbuf_alloc_space(sizeof(*iov) * 256, req->encoding->dbuf);
    chimera_nfs_abort_if(iov == NULL, "Failed to allocate space");

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED, 0);
    chimera_vfs_compound_add_read(compound, NULL,
                                  args->offset, args->count,
                                  iov, 256,
                                  CHIMERA_NFS3_ATTR_MASK, NULL);

    chimera_vfs_compound_submit(compound,
                                chimera_nfs3_read_sequence_complete, req);
} /* chimera_nfs3_read */
