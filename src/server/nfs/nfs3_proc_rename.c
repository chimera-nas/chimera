// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "server/server.h"
#include "vfs/vfs_procs.h"
#include "nfs3_dump.h"
#include "nfs3_trace.h"

static void
chimera_nfs3_rename_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *fromdir_pre_attr,
    struct chimera_vfs_attrs *fromdir_post_attr,
    struct chimera_vfs_attrs *todir_pre_attr,
    struct chimera_vfs_attrs *todir_post_attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct RENAME3res                 res;
    int                               rc;

    res.status = chimera_vfs_error_to_nfsstat3(
        error_code);

    if (res.status == NFS3_OK) {
        chimera_nfs3_set_wcc_data(&res.resok.fromdir_wcc, fromdir_pre_attr, fromdir_post_attr);
        chimera_nfs3_set_wcc_data(&res.resok.todir_wcc, todir_pre_attr, todir_post_attr);
    } else {
        chimera_nfs3_set_wcc_data(&res.resfail.fromdir_wcc, fromdir_pre_attr, fromdir_post_attr);
        chimera_nfs3_set_wcc_data(&res.resfail.todir_wcc, todir_pre_attr, todir_post_attr);
    }

    rc = shared->nfs_v3.send_reply_NFSPROC3_RENAME(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_mkdir_complete */

/* Issue the rename.  target_fh is left NULL: when a caching protocol is enabled
 * the VFS resolves the clobbered destination's FH itself and recalls any
 * delegation/lease on it -- and on the renamed source -- before the rename. */
static void
chimera_nfs3_rename_dispatch(struct nfs_request *req)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct RENAME3args               *args   = req->args_rename;

    /* req->fh / req->saved_fh are the decoded+authenticated source / dest
     * directory handles (set in chimera_nfs3_rename below). */
    chimera_vfs_rename_at(thread->vfs_thread,
                          &req->cred,
                          req->fh,
                          req->fhlen,
                          args->from.name.str,
                          args->from.name.len,
                          req->saved_fh,
                          req->saved_fhlen,
                          args->to.name.str,
                          args->to.name.len,
                          NULL,
                          0,
                          CHIMERA_VFS_REMOVE_RECALL,
                          CHIMERA_NFS3_ATTR_WCC_MASK | CHIMERA_VFS_ATTR_ATOMIC,
                          CHIMERA_NFS3_ATTR_MASK,
                          NULL,
                          NULL,
                          chimera_nfs3_rename_complete,
                          req);
} /* chimera_nfs3_rename_dispatch */

void
chimera_nfs3_rename(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct RENAME3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct RENAME3res                 res;
    uint16_t                          todir_export_id = 0;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_rename(req, args);
    nfs3_trace_rename(req, args);

    /* Decode both directory handles: the source sets the request export (and
     * squash); the destination is authenticated into req->saved_fh and layers
     * its own export's sec= and squash policy on top.  Both must outlive this
     * (async) call, so they go in the request, not on the stack. */
    res.status = chimera_nfs3_decode_fh(req, args->from.dir.data.data, args->from.dir.data.len);
    if (res.status == NFS3_OK) {
        res.status = chimera_nfs3_decode_fh2(req, args->to.dir.data.data,
                                             args->to.dir.data.len, &todir_export_id);
    }
    /* Both directories are mutated, so both exports must be writable. */
    if (res.status == NFS3_OK) {
        res.status = chimera_nfs3_check_rofs2(req, todir_export_id);
    }
    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_RENAME(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    req->args_rename = args;

    chimera_nfs3_rename_dispatch(req);
} /* chimera_nfs3_rename */
