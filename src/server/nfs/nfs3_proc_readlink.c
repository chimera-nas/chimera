// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"

static void
chimera_nfs3_readlink_complete(
    enum chimera_vfs_error    error_code,
    int                       targetlen,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct READLINK3res              *res;
    int                               rc;

    res = &req->res_readlink;

    res->status = chimera_vfs_error_to_nfsstat3(error_code);

    /* READLINK is only valid on a symlink (RFC 1813 §3.3.5); enforce at the
     * protocol layer rather than relying on every backend to map a non-symlink
     * to EINVAL.  The type comes from the attrs already fetched, so no extra
     * round-trip. */
    if (res->status == NFS3_OK && attr &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        (attr->va_mode & S_IFMT) != S_IFLNK) {
        res->status = NFS3ERR_INVAL;
    }

    if (res->status == NFS3_OK) {
        chimera_nfs3_set_post_op_attr(&res->resok.symlink_attributes, attr);
        res->resok.data.len = targetlen;
    } else {
        chimera_nfs3_set_post_op_attr(&res->resfail.symlink_attributes, attr);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_READLINK(evpl, NULL, res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");


    nfs_request_free(thread, req);
} /* chimera_nfs3_readlink_complete */



/*
 * PUTFH, OPEN, GETATTR, READLINK -- four ops, ONE open.  READLINK itself
 * fetches no attributes, and NFS3 needs them twice over: for the post-op
 * symlink attributes, and for the S_ISLNK check RFC 1813 3.3.5 requires.  A
 * GETATTR ahead of it on the same current object costs nothing but a slot,
 * because the open is already there.
 */
static void
chimera_nfs3_readlink_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    struct READLINK3res                  *res = &req->res_readlink;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_attrs              attr;
    enum chimera_vfs_error                status;
    uint32_t                              last;
    int                                   targetlen = 0;

    status = chimera_vfs_compound_status(compound);

    memset(&attr, 0, sizeof(attr));

    /* Everything out of the sequence before it is freed and recycled. */
    if (status == CHIMERA_VFS_OK) {
        last = chimera_vfs_compound_num_ops(compound) - 1;

        attr = chimera_vfs_compound_op(compound, last - 1)->attr;

        op = chimera_vfs_compound_op(compound, last);

        targetlen = (int) op->target_len;

        if (targetlen > (int) res->resok.data.len) {
            targetlen = (int) res->resok.data.len;
        }

        if (targetlen > 0 && op->target) {
            memcpy(res->resok.data.str, op->target, targetlen);
        }
    }

    chimera_vfs_compound_free(compound);

    chimera_nfs3_readlink_complete(status, targetlen,
                                   status == CHIMERA_VFS_OK ? &attr : NULL,
                                   req);
} /* chimera_nfs3_readlink_sequence_complete */


void
chimera_nfs3_readlink(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct READLINK3args      *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct chimera_vfs_compound      *compound;
    struct READLINK3res              *res;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_readlink(req, args);
    nfs3_trace_readlink(req, args);
    res = &req->res_readlink;

    res->resok.data.len = 4096;
    res->resok.data.str = xdr_dbuf_alloc_space(4096, encoding->dbuf);

    req->args_readlink = args;

    res->status = chimera_nfs3_decode_fh(req, args->symlink.data.data, args->symlink.data.len);
    if (res->status != NFS3_OK) {
        nfsstat3 fh_status = res->status;
        memset(res, 0, sizeof(*res));
        res->status = fh_status;
        rc          = shared->nfs_v3.send_reply_NFSPROC3_READLINK(evpl, NULL, res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound, CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH, 0);
    chimera_vfs_compound_add_getattr(compound, CHIMERA_NFS3_ATTR_MASK);
    chimera_vfs_compound_add_readlink(compound);

    chimera_vfs_compound_submit(compound,
                                chimera_nfs3_readlink_sequence_complete, req);

} /* chimera_nfs3_readlink */
