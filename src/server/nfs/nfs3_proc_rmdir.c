// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "server/server.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"
#include "vfs/vfs_compound.h"

static void
chimera_nfs3_rmdir_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct RMDIR3res                  res;
    int                               rc;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        chimera_nfs3_set_wcc_data(&res.resok.dir_wcc, pre_attr, post_attr);
    } else {
        chimera_nfs3_set_wcc_data(&res.resfail.dir_wcc, pre_attr, post_attr);
    }

    /* The open belonged to the sequence and went with it. */

    rc = shared->nfs_v3.send_reply_NFSPROC3_RMDIR(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_rmdir_complete */

/*
 * PUTFH, OPEN, REMOVE.  child_fh is left NULL: when a caching protocol is
 * enabled the VFS resolves the victim's FH itself and recalls any
 * delegation/oplock/lease on it before the unlink.
 */
static void
chimera_nfs3_rmdir_sequence_complete(
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

    /* Both arms carry dir_wcc, so these are read whatever the status. */
    pre_attr  = op->dir_pre_attr;
    post_attr = op->dir_post_attr;

    chimera_vfs_compound_free(compound);

    chimera_nfs3_rmdir_complete(status, &pre_attr, &post_attr, req);
} /* chimera_nfs3_rmdir_sequence_complete */

void
chimera_nfs3_rmdir(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct RMDIR3args         *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct chimera_vfs_compound      *compound;
    struct RMDIR3res                  res;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_rmdir(req, args);
    nfs3_trace_rmdir(req, args);

    req->args_rmdir = args;

    res.status = chimera_nfs3_decode_fh(req, args->object.dir.data.data, args->object.dir.data.len);
    if (res.status == NFS3_OK) {
        res.status = chimera_nfs3_check_rofs(req, req->export_id);
    }
    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_RMDIR(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED |
                                          CHIMERA_VFS_OPEN_PATH |
                                          CHIMERA_VFS_OPEN_DIRECTORY, 0);
    chimera_vfs_compound_add_remove(compound,
                                    args->object.name.str,
                                    args->object.name.len,
                                    CHIMERA_VFS_REMOVE_ISDIR | CHIMERA_VFS_REMOVE_RECALL,
                                    CHIMERA_NFS3_ATTR_WCC_MASK,
                                    CHIMERA_NFS3_ATTR_MASK);

    chimera_vfs_compound_submit(compound,
                                chimera_nfs3_rmdir_sequence_complete, req);
} /* chimera_nfs3_rmdir */
