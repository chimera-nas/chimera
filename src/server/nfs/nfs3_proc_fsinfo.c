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
chimera_nfs3_fsinfo_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct FSINFO3res                 res;
    uint64_t                          max_xfer = CHIMERA_NFS3_MAX_XFER;
    int                               rc;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        chimera_nfs3_set_post_op_attr(&res.resok.obj_attributes, attr);

        res.resok.maxfilesize         = UINT64_MAX;
        res.resok.time_delta.seconds  = 0;
        res.resok.time_delta.nseconds = 1;
        res.resok.rtmax               = max_xfer;
        res.resok.rtpref              = max_xfer;
        res.resok.rtmult              = 4096;
        res.resok.wtmax               = max_xfer;
        res.resok.wtpref              = max_xfer;
        res.resok.wtmult              = 4096;
        res.resok.dtpref              = 64 * 1024;
        res.resok.properties          = FSF3_LINK | FSF3_SYMLINK |
            FSF3_HOMOGENEOUS | FSF3_CANSETTIME;
    } else {
        chimera_nfs3_set_post_op_attr(&res.resfail.obj_attributes, attr);
    }


    rc = shared->nfs_v3.send_reply_NFSPROC3_FSINFO(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_fsinfo_complete */


/*
 * PUTFH, OPEN, GETATTR.  The open the handler used to make by hand belongs to
 * the sequence now, and goes with it -- nothing here releases a handle.
 */
static void
chimera_nfs3_fsinfo_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_attrs              attr;
    enum chimera_vfs_error                status;

    status = chimera_vfs_compound_status(compound);

    memset(&attr, 0, sizeof(attr));

    /* Taken out before the free: a freed sequence is recycled and reset. */
    if (status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(compound,
                                     chimera_vfs_compound_num_ops(compound) - 1);
        attr = op->attr;
    }

    chimera_vfs_compound_free(compound);

    chimera_nfs3_fsinfo_complete(status,
                                 status == CHIMERA_VFS_OK ? &attr : NULL,
                                 req);
} /* chimera_nfs3_fsinfo_sequence_complete */


void
chimera_nfs3_fsinfo(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct FSINFO3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct chimera_vfs_compound      *compound;
    struct FSINFO3res                 res;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_fsinfo(req, args);
    nfs3_trace_fsinfo(req, args);

    req->args_fsinfo = args;

    res.status = chimera_nfs3_decode_fh(req, args->fsroot.data.data, args->fsroot.data.len);
    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_FSINFO(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound, CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH, 0);
    chimera_vfs_compound_add_getattr(compound, CHIMERA_NFS3_ATTR_MASK);

    chimera_vfs_compound_submit(compound,
                                chimera_nfs3_fsinfo_sequence_complete, req);
} /* chimera_nfs3_fsinfo */
