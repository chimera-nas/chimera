// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
#include "vfs/vfs_procs.h"

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

        chimera_vfs_remove_at(thread->vfs_thread, &req->cred,
                              handle,
                              args->object.name.str,
                              args->object.name.len,
                              NULL,
                              0,
                              CHIMERA_NFS3_ATTR_WCC_MASK,
                              CHIMERA_NFS3_ATTR_MASK,
                              chimera_nfs3_rmdir_complete,
                              req);
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
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, encoding);
    chimera_nfs_map_cred(&req->cred, cred);

    nfs3_dump_rmdir(req, args);

    req->args_rmdir = args;

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        args->object.dir.data.data,
                        args->object.dir.data.len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_nfs3_rmdir_open_callback,
                        req);
} /* chimera_nfs3_rmdir */
