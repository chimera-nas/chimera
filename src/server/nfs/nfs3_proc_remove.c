// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "server/server.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"

/* True when a cross-protocol caching holder (NFSv4 delegation, SMB lease, or
 * SMB oplock) could exist on the shared store.  An NFSv3 REMOVE/RMDIR of an
 * object another protocol holds must recall that holder before the unlink
 * (RFC 7530 §10.4.5 / MS-SMB2 break-before-mutate, #1070); when no caching
 * protocol is enabled the extra LOOKUP needed to learn the victim FH is
 * skipped. */
static inline int
chimera_nfs3_recall_needed(struct chimera_server_nfs_thread *thread)
{
    return chimera_server_config_get_nfs4_delegations(thread->shared->config) ||
           chimera_server_config_get_smb_leases(thread->shared->config) ||
           chimera_server_config_get_smb_oplocks(thread->shared->config);
} /* chimera_nfs3_recall_needed */

static void
chimera_nfs3_remove_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct REMOVE3res                 res;
    int                               rc;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        chimera_nfs3_set_wcc_data(&res.resok.dir_wcc, pre_attr, post_attr);
    } else {
        chimera_nfs3_set_wcc_data(&res.resfail.dir_wcc, pre_attr, post_attr);
    }

    chimera_vfs_release(thread->vfs_thread, req->handle);

    rc = shared->nfs_v3.send_reply_NFSPROC3_REMOVE(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_remove_complete */

/* Issue the unlink.  child_fh (when known) lets the VFS recall any
 * delegation/oplock/lease on the victim before it is removed. */
static void
chimera_nfs3_remove_dispatch(
    struct nfs_request *req,
    const uint8_t      *child_fh,
    int                 child_fh_len)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct REMOVE3args               *args   = req->args_remove;

    chimera_vfs_remove_at(thread->vfs_thread, &req->cred,
                          req->handle,
                          args->object.name.str,
                          args->object.name.len,
                          child_fh,
                          child_fh_len,
                          CHIMERA_NFS3_ATTR_WCC_MASK,
                          CHIMERA_NFS3_ATTR_MASK,
                          NULL,
                          chimera_nfs3_remove_complete,
                          req);
} /* chimera_nfs3_remove_dispatch */

static void
chimera_nfs3_remove_lookup_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;

    /* Hand the victim's FH to remove_at so the VFS recalls any delegation/
     * oplock/lease held on it before the unlink (#1070).  A failed lookup is
     * not fatal here -- let remove_at produce the authoritative error.  Stash
     * the FH in req->fh (unused by NFSv3 procs) so it stays valid across the
     * async remove. */
    if (error_code == CHIMERA_VFS_OK &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        memcpy(req->fh, attr->va_fh, attr->va_fh_len);
        req->fhlen = attr->va_fh_len;
        chimera_nfs3_remove_dispatch(req, req->fh, req->fhlen);
    } else {
        chimera_nfs3_remove_dispatch(req, NULL, 0);
    }
} /* chimera_nfs3_remove_lookup_callback */

static void
chimera_nfs3_remove_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct REMOVE3args               *args   = req->args_remove;
    struct REMOVE3res                 res;
    int                               rc;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        /* Resolve the victim FH first so a cross-protocol caching holder is
         * recalled before the unlink.  Skip the extra LOOKUP when no caching
         * protocol is enabled (no holder can exist). */
        if (chimera_nfs3_recall_needed(thread)) {
            chimera_vfs_lookup_at(thread->vfs_thread, &req->cred,
                                  handle,
                                  args->object.name.str,
                                  args->object.name.len,
                                  CHIMERA_VFS_ATTR_FH,
                                  0,
                                  chimera_nfs3_remove_lookup_callback,
                                  req);
        } else {
            chimera_nfs3_remove_dispatch(req, NULL, 0);
        }
    } else {
        res.status = chimera_vfs_error_to_nfsstat3(error_code);
        chimera_nfs3_set_wcc_data(&res.resfail.dir_wcc, NULL, NULL);
        rc = shared->nfs_v3.send_reply_NFSPROC3_REMOVE(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_remove_open_callback */

void
chimera_nfs3_remove(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct REMOVE3args        *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct REMOVE3res                 res;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_remove(req, args);

    req->args_remove = args;

    res.status = chimera_nfs3_decode_fh(req, args->object.dir.data.data, args->object.dir.data.len);
    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_REMOVE(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_nfs3_remove_open_callback,
                        req);
} /* chimera_nfs3_remove */
