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
chimera_nfs3_symlink_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *r_attr,
    struct chimera_vfs_attrs *r_dir_pre_attr,
    struct chimera_vfs_attrs *r_dir_post_attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct SYMLINK3res                res;
    int                               rc;

    res.status = chimera_vfs_error_to_nfsstat3(
        error_code);

    if (res.status == NFS3_OK) {
        if (r_attr->va_set_mask & CHIMERA_VFS_ATTR_FH) {
            res.resok.obj.handle_follows = 1;
            rc                           = xdr_dbuf_opaque_copy(&res.resok.obj.handle.data,
                                                                r_attr->va_fh,
                                                                r_attr->va_fh_len,
                                                                msg->dbuf);
            chimera_nfs_abort_if(rc, "Failed to copy opaque");
        } else {
            res.resok.obj.handle_follows = 0;
        }

        chimera_nfs3_set_post_op_attr(&res.resok.obj_attributes, r_attr);
        chimera_nfs3_set_wcc_data(&res.resok.dir_wcc, r_dir_pre_attr, r_dir_post_attr);
    } else {
        chimera_nfs3_set_wcc_data(&res.resfail.dir_wcc, r_dir_pre_attr, r_dir_post_attr);
    }

    chimera_vfs_release(thread->vfs_thread, req->handle);

    rc = shared->nfs_v3.send_reply_NFSPROC3_SYMLINK(evpl, &res, msg);
    chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");

    nfs_request_free(thread, req);
} /* chimera_nfs3_mkdir_complete */


static void
chimera_nfs3_symlink_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct SYMLINK3args              *args   = req->args_symlink;
    struct SYMLINK3res                res;
    struct chimera_vfs_attrs         *attr;
    int                               rc;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;
        attr        = xdr_dbuf_alloc_space(sizeof(*attr), msg->dbuf);
        chimera_nfs_abort_if(attr == NULL, "Failed to allocate space");

        chimera_nfs3_sattr3_to_va(attr, &args->symlink.symlink_attributes);

        chimera_vfs_symlink(
            thread->vfs_thread,
            handle,
            args->where.name.str,
            args->where.name.len,
            args->symlink.symlink_data.str,
            args->symlink.symlink_data.len,
            CHIMERA_VFS_ATTR_FH |      CHIMERA_NFS3_ATTR_MASK,
            CHIMERA_NFS3_ATTR_WCC_MASK,
            CHIMERA_NFS3_ATTR_MASK,
            chimera_nfs3_symlink_complete,
            req);

    } else {
        res.status = chimera_vfs_error_to_nfsstat3(error_code);
        chimera_nfs3_set_wcc_data(&res.resfail.dir_wcc, NULL, NULL);
        rc = shared->nfs_v3.send_reply_NFSPROC3_SYMLINK(evpl, &res, msg);
        chimera_nfs_abort_if(rc, "Failed to send RPC2 reply");
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_mkdir_open_callback */
void
chimera_nfs3_symlink(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct SYMLINK3args   *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, msg);

    nfs3_dump_symlink(req, args);
    req->args_symlink = args;

    chimera_vfs_open(thread->vfs_thread,
                     args->where.dir.data.data,
                     args->where.dir.data.len,
                     CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                     chimera_nfs3_symlink_open_callback,
                     req);
} /* chimera_nfs3_symlink */
