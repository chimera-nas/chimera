// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs3_attr.h"
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
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct RMDIR3res                  res;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        chimera_nfs3_set_wcc_data(&res.resok.dir_wcc, pre_attr, post_attr);
    } else {
        chimera_nfs3_set_wcc_data(&res.resfail.dir_wcc, pre_attr, post_attr);
    }

    chimera_vfs_release(thread->vfs_thread, req->handle);

    shared->nfs_v3.send_reply_NFSPROC3_RMDIR(evpl, &res, msg);

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
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct RMDIR3args                *args   = req->args_rmdir;
    struct RMDIR3res                  res;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        chimera_vfs_remove(thread->vfs_thread,
                           handle,
                           args->object.name.str,
                           args->object.name.len,
                           CHIMERA_NFS3_ATTR_WCC_MASK,
                           CHIMERA_NFS3_ATTR_MASK,
                           chimera_nfs3_rmdir_complete,
                           req);
    } else {
        res.status = chimera_vfs_error_to_nfsstat3(error_code);
        chimera_nfs3_set_wcc_data(&res.resfail.dir_wcc, NULL, NULL);
        shared->nfs_v3.send_reply_NFSPROC3_RMDIR(evpl, &res, msg);
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_rmdir_open_callback */

void
chimera_nfs3_rmdir(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct RMDIR3args     *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, msg);

    nfs3_dump_rmdir(req, args);

    req->args_rmdir = args;

    chimera_vfs_open(thread->vfs_thread,
                     args->object.dir.data.data,
                     args->object.dir.data.len,
                     CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                     chimera_nfs3_rmdir_open_callback,
                     req);
} /* chimera_nfs3_rmdir */
