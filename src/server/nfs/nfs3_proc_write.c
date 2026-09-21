// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "nfs_internal.h"
#include "vfs/vfs.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"

static void
chimera_nfs3_write_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct WRITE3args                *args   = req->args_write;
    struct WRITE3res                  res;
    int                               rc;

    /* Release write iovecs here on the server thread, not in VFS backend.
     * The iovecs were allocated on this thread and must be released here
     * to avoid cross-thread access to non-atomic refcounts.
     */
    evpl_iovecs_release(evpl, args->data.iov, args->data.niov);

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        res.resok.count = length;

        /* Report the durability actually achieved (the backend may make data
         * durable but defer metadata): sync is one of UNSTABLE/DATA_SYNC/
         * FILE_SYNC (0/1/2), which match the stable_how enum directly. */
        res.resok.committed = sync;

        memcpy(res.resok.verf,
               &shared->nfs_verifier,
               sizeof(res.resok.verf));

        chimera_nfs3_set_wcc_data(&res.resok.file_wcc, pre_attr, post_attr);
    } else {
        chimera_nfs3_set_wcc_data(&res.resfail.file_wcc, pre_attr, post_attr);
    }

    /* The open belonged to the sequence and went with it. */

    rc = shared->nfs_v3.send_reply_NFSPROC3_WRITE(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_write_complete */

/*
 * PUTFH, OPEN, WRITE.  The wcc pair comes from the WRITE itself: the two
 * readings have to bracket the write atomically, which a GETATTR either side of
 * it in the same sequence would not.
 */
static void
chimera_nfs3_write_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_attrs              pre_attr, post_attr;
    enum chimera_vfs_error                status;
    uint32_t                              length = 0, sync = 0;

    status = chimera_vfs_compound_status(compound);

    op = chimera_vfs_compound_op(compound,
                                 chimera_vfs_compound_num_ops(compound) - 1);

    /* Both arms of WRITE3res carry file_wcc, so these are read whatever the
     * status; an unfilled va_set_mask is what makes wcc_data say nothing. */
    pre_attr  = op->pre_attr;
    post_attr = op->attr;

    if (status == CHIMERA_VFS_OK) {
        length = op->written;
        sync   = op->committed;
    }

    chimera_vfs_compound_free(compound);

    chimera_nfs3_write_complete(status, length, sync, &pre_attr, &post_attr,
                                req);
} /* chimera_nfs3_write_sequence_complete */

void
chimera_nfs3_write(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct WRITE3args         *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct chimera_vfs_compound      *compound;
    struct WRITE3res                  res;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_write(req, args);
    nfs3_trace_write(req, args);

    req->args_write = args;

    /* Transfer ownership of write iovecs from the RPC2 message to prevent
     * msg_free from double-releasing (args->data.iov points to msg->read_chunk.iov
     * via XDR zerocopy). The iovecs will be released in the write completion
     * callback on this server thread, not in the VFS backend (which may run on
     * a different delegation thread). Must be done before any error paths.
     */
    evpl_rpc2_encoding_take_read_chunk(req->encoding, NULL, NULL);

    res.status = chimera_nfs3_decode_fh(req, args->file.data.data, args->file.data.len);
    if (res.status == NFS3_OK) {
        res.status = chimera_nfs3_check_rofs(req, req->export_id);
    }

    /* count and the opaque data<> are independent wire fields, so a client can
     * claim more bytes than it supplied.  The backends drive an iovec cursor
     * over the supplied iovecs for count bytes and abort() once the source is
     * exhausted, so a disagreeing pair has to be refused here. */
    if (res.status == NFS3_OK && args->count > args->data.length) {
        res.status = NFS3ERR_INVAL;
    }

    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        chimera_nfs3_set_wcc_data(&res.resfail.file_wcc, NULL, NULL);
        rc = shared->nfs_v3.send_reply_NFSPROC3_WRITE(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

        /* Iovecs were already taken from the message above, so release them
         * here since the VFS will never be invoked.
         */
        evpl_iovecs_release(evpl, args->data.iov, args->data.niov);
        nfs_request_free(thread, req);
        return;
    }

    /* RFC 1813 3.3.7: a count above wtmax is served as a short write, with the
     * reply reporting how much was actually written. */
    if (args->count > CHIMERA_NFS3_MAX_XFER) {
        args->count = CHIMERA_NFS3_MAX_XFER;
    }

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED, 0);
    chimera_vfs_compound_add_write(compound, NULL,
                                   args->offset, args->count, args->stable,
                                   args->data.iov, args->data.niov,
                                   CHIMERA_NFS3_ATTR_WCC_MASK,
                                   CHIMERA_NFS3_ATTR_MASK,
                                   NULL);

    chimera_vfs_compound_submit(compound,
                                chimera_nfs3_write_sequence_complete, req);
} /* chimera_nfs3_write */
