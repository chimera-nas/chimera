// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs3_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
static void
chimera_nfs3_fsstat_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct FSSTAT3res                 res;
    int                               rc;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if ((attr->va_set_mask & CHIMERA_NFS3_FSSTAT_MASK) != CHIMERA_NFS3_FSSTAT_MASK) {
        res.status = NFS3ERR_NOTSUPP;
    }

    if (res.status == NFS3_OK) {
        chimera_nfs3_set_post_op_attr(&res.resok.obj_attributes, attr);

        res.resok.tbytes   = attr->va_fs_space_total;
        res.resok.fbytes   = attr->va_fs_space_free;
        res.resok.abytes   = attr->va_fs_space_used;
        res.resok.tfiles   = attr->va_fs_files_total;
        res.resok.ffiles   = attr->va_fs_files_free;
        res.resok.afiles   = attr->va_fs_files_avail;
        res.resok.invarsec = 0;
    } else {
        chimera_nfs3_set_post_op_attr(&res.resfail.obj_attributes, attr);
    }

    chimera_vfs_release(thread->vfs_thread, req->handle);

    rc = shared->nfs_v3.send_reply_NFSPROC3_FSSTAT(evpl, &res, msg);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_fsstat_complete */

static void
chimera_nfs3_fsstat_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct FSSTAT3res                 res;
    int                               rc;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        chimera_vfs_getattr(thread->vfs_thread,
                            handle,
                            CHIMERA_NFS3_FSSTAT_MASK,
                            chimera_nfs3_fsstat_complete,
                            req);
    } else {
        res.status = chimera_vfs_error_to_nfsstat3(error_code);
        chimera_nfs3_set_post_op_attr(&res.resfail.obj_attributes, NULL);
        rc = shared->nfs_v3.send_reply_NFSPROC3_FSSTAT(evpl, &res, msg);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_fsstat_open_callback */

void
chimera_nfs3_fsstat(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct FSSTAT3args    *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, msg);

    nfs3_dump_fsstat(req, args);

    req->args_fsstat = args;

    chimera_vfs_open(thread->vfs_thread,
                     args->fsroot.data.data,
                     args->fsroot.data.len,
                     CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                     chimera_nfs3_fsstat_open_callback,
                     req);
} /* chimera_nfs3_fsstat */
