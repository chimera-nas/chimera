// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "server/server.h"
#include "vfs/vfs_procs.h"
#include "nfs3_dump.h"

/* See chimera_nfs3_remove.c: when a caching protocol is enabled, an NFSv3
 * RENAME that overwrites an existing destination must recall any delegation/
 * lease held on the clobbered target before it is replaced (#1071). */
static inline int
chimera_nfs3_recall_needed(struct chimera_server_nfs_thread *thread)
{
    return chimera_server_config_get_nfs4_delegations(thread->shared->config) ||
           chimera_server_config_get_smb_leases(thread->shared->config) ||
           chimera_server_config_get_smb_oplocks(thread->shared->config);
} /* chimera_nfs3_recall_needed */

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

/* Issue the rename.  target_fh (when the destination name already exists) lets
 * the VFS recall any delegation/lease on the clobbered file before it is
 * replaced.  The VFS recalls the source itself. */
static void
chimera_nfs3_rename_dispatch(
    struct nfs_request *req,
    const uint8_t      *target_fh,
    int                 target_fh_len)
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
                          target_fh,
                          target_fh_len,
                          CHIMERA_NFS3_ATTR_WCC_MASK | CHIMERA_VFS_ATTR_ATOMIC,
                          CHIMERA_NFS3_ATTR_MASK,
                          NULL,
                          NULL,
                          chimera_nfs3_rename_complete,
                          req);
} /* chimera_nfs3_rename_dispatch */

static void
chimera_nfs3_rename_target_lookup_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;

    /* Destination exists: hand its FH to rename_at so the VFS recalls any
     * delegation/lease on the doomed target before it is replaced (#1071).
     * No destination (e.g. ENOENT) is the common case -- proceed with no
     * target recall.  Stash the FH in req->rename_target_fh (req->fh /
     * req->saved_fh hold the source / dest directory handles) so it survives
     * the async rename. */
    if (error_code == CHIMERA_VFS_OK &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        memcpy(req->rename_target_fh, attr->va_fh, attr->va_fh_len);
        req->rename_target_fhlen = attr->va_fh_len;
        chimera_nfs3_rename_dispatch(req, req->rename_target_fh,
                                     req->rename_target_fhlen);
    } else {
        chimera_nfs3_rename_dispatch(req, NULL, 0);
    }
} /* chimera_nfs3_rename_target_lookup_callback */

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
    uint16_t                          todir_export_id;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_rename(req, args);

    /* Decode both directory handles: the source sets the request export (and
     * squash); the destination is authenticated into req->saved_fh.  Both must
     * outlive this (async) call, so they go in the request, not on the stack. */
    res.status = chimera_nfs3_decode_fh(req, args->from.dir.data.data, args->from.dir.data.len);
    if (res.status != NFS3_OK ||
        chimera_nfs_fh_unwrap(args->to.dir.data.data, args->to.dir.data.len,
                              &todir_export_id, req->saved_fh, &req->saved_fhlen,
                              shared->fh_key, shared->fh_sign) != CHIMERA_NFS_FH_OK) {
        nfsstat3 fh_status = res.status != NFS3_OK ? res.status : NFS3ERR_BADHANDLE;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_RENAME(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    req->args_rename = args;

    /* Resolve the destination name first (in the decoded dest dir) so a
     * delegation/lease on a clobbered target is recalled before the rename.
     * Skip the lookup when no caching protocol is enabled (no holder can
     * exist). */
    if (chimera_nfs3_recall_needed(thread)) {
        chimera_vfs_lookup(thread->vfs_thread, &req->cred,
                           req->saved_fh,
                           req->saved_fhlen,
                           args->to.name.str,
                           args->to.name.len,
                           CHIMERA_VFS_ATTR_FH,
                           0,
                           chimera_nfs3_rename_target_lookup_callback,
                           req);
    } else {
        chimera_nfs3_rename_dispatch(req, NULL, 0);
    }
} /* chimera_nfs3_rename */
