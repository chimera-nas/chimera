// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_internal.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs.h"
#include "vfs/vfs_compound.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"
static void
chimera_nfs3_commit_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct COMMIT3res                 res;
    int                               rc;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        chimera_nfs3_set_wcc_data(&res.resok.file_wcc, pre_attr, post_attr);

        memcpy(res.resok.verf,
               &shared->nfs_verifier,
               sizeof(res.resok.verf));
    } else {
        chimera_nfs3_set_wcc_data(&res.resfail.file_wcc, pre_attr, post_attr);
    }

    /* The open belonged to the sequence and went with it. */

    rc = shared->nfs_v3.send_reply_NFSPROC3_COMMIT(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_commit_complete */

/*
 * PUTFH, OPEN, COMMIT.  COMMIT3res carries file_wcc on both arms, and the pair
 * has to bracket the flush, so it comes from the COMMIT rather than from
 * getattrs around it.
 */
static void
chimera_nfs3_commit_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_attrs              pre_attr, post_attr;
    enum chimera_vfs_error                status;

    status = chimera_vfs_compound_status(compound);

    op = chimera_vfs_compound_op(compound,
                                 chimera_vfs_compound_num_ops(compound) - 1);

    pre_attr  = op->pre_attr;
    post_attr = op->attr;

    chimera_vfs_compound_free(compound);

    chimera_nfs3_commit_complete(status, &pre_attr, &post_attr, req);
} /* chimera_nfs3_commit_sequence_complete */

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
    struct chimera_vfs_compound      *compound;
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

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED, 0);
    chimera_vfs_compound_add_commit(compound,
                                    args->offset, args->count,
                                    CHIMERA_NFS3_ATTR_WCC_MASK,
                                    CHIMERA_NFS3_ATTR_MASK);

    chimera_vfs_compound_submit(compound,
                                chimera_nfs3_commit_sequence_complete, req);

} /* chimera_nfs3_commit */
