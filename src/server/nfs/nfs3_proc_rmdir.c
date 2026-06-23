// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "server/server.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "vfs/vfs_procs.h"

/* See chimera_nfs3_remove.c: a directory removed via NFSv3 may be held under an
 * SMB directory lease (or, rarely, a delegation); recall the holder before the
 * rmdir when a caching protocol is enabled (#1070). */
static inline int
chimera_nfs3_recall_needed(struct chimera_server_nfs_thread *thread)
{
    return chimera_server_config_get_nfs4_delegations(thread->shared->config) ||
           chimera_server_config_get_smb_leases(thread->shared->config) ||
           chimera_server_config_get_smb_oplocks(thread->shared->config);
} /* chimera_nfs3_recall_needed */

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

    chimera_vfs_release(thread->vfs_thread, req->handle);

    rc = shared->nfs_v3.send_reply_NFSPROC3_RMDIR(evpl, NULL, &res, req->encoding);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_rmdir_complete */

/* Issue the rmdir.  child_fh (when known) lets the VFS recall a directory
 * lease/delegation on the victim before it is removed. */
static void
chimera_nfs3_rmdir_dispatch(
    struct nfs_request *req,
    const uint8_t      *child_fh,
    int                 child_fh_len)
{
    struct chimera_server_nfs_thread *thread = req->thread;
    struct RMDIR3args                *args   = req->args_rmdir;

    chimera_vfs_remove_at(thread->vfs_thread, &req->cred,
                          req->handle,
                          args->object.name.str,
                          args->object.name.len,
                          child_fh,
                          child_fh_len,
                          CHIMERA_NFS3_ATTR_WCC_MASK,
                          CHIMERA_NFS3_ATTR_MASK,
                          NULL,
                          chimera_nfs3_rmdir_complete,
                          req);
} /* chimera_nfs3_rmdir_dispatch */

static void
chimera_nfs3_rmdir_lookup_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;

    if (error_code == CHIMERA_VFS_OK &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        memcpy(req->fh, attr->va_fh, attr->va_fh_len);
        req->fhlen = attr->va_fh_len;
        chimera_nfs3_rmdir_dispatch(req, req->fh, req->fhlen);
    } else {
        chimera_nfs3_rmdir_dispatch(req, NULL, 0);
    }
} /* chimera_nfs3_rmdir_lookup_callback */

static void
chimera_nfs3_rmdir_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct RMDIR3args                *args   = req->args_rmdir;
    struct RMDIR3res                  res;
    int                               rc;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        if (chimera_nfs3_recall_needed(thread)) {
            chimera_vfs_lookup_at(thread->vfs_thread, &req->cred,
                                  handle,
                                  args->object.name.str,
                                  args->object.name.len,
                                  CHIMERA_VFS_ATTR_FH,
                                  0,
                                  chimera_nfs3_rmdir_lookup_callback,
                                  req);
        } else {
            chimera_nfs3_rmdir_dispatch(req, NULL, 0);
        }
    } else {
        res.status = chimera_vfs_error_to_nfsstat3(error_code);
        chimera_nfs3_set_wcc_data(&res.resfail.dir_wcc, NULL, NULL);
        rc = shared->nfs_v3.send_reply_NFSPROC3_RMDIR(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_rmdir_open_callback */

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
    struct RMDIR3res                  res;
    int                               rc;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred_req(req, cred);

    nfs3_dump_rmdir(req, args);

    req->args_rmdir = args;

    res.status = chimera_nfs3_decode_fh(req, args->object.dir.data.data, args->object.dir.data.len);
    if (res.status != NFS3_OK) {
        nfsstat3 fh_status = res.status;
        memset(&res, 0, sizeof(res));
        res.status = fh_status;
        rc         = shared->nfs_v3.send_reply_NFSPROC3_RMDIR(evpl, NULL, &res, req->encoding);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
        return;
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_nfs3_rmdir_open_callback,
                        req);
} /* chimera_nfs3_rmdir */
