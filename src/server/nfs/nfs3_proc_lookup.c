// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs3_procs.h"
#include "nfs_common/nfs3_status.h"
#include "nfs_common/nfs3_attr.h"
#include "nfs_internal.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
static void
chimera_nfs3_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct LOOKUP3res                 res;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {

        chimera_nfs_abort_if(!(attr->va_set_mask & CHIMERA_VFS_ATTR_FH),
                             "NFS3 lookup: no file handle was returned");

        xdr_dbuf_opaque_copy(&res.resok.object.data,
                             attr->va_fh,
                             attr->va_fh_len,
                             msg->dbuf);

        chimera_nfs3_set_post_op_attr(&res.resok.obj_attributes, attr);
        chimera_nfs3_set_post_op_attr(&res.resok.dir_attributes, dir_attr);
    } else {
        chimera_nfs3_set_post_op_attr(&res.resfail.dir_attributes, dir_attr);
    }

    chimera_vfs_release(thread->vfs_thread, req->handle);

    shared->nfs_v3.send_reply_NFSPROC3_LOOKUP(evpl, &res, msg);


    nfs_request_free(thread, req);
} /* chimera_nfs3_lookup_complete */

static void
chimera_nfs3_lookup_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct LOOKUP3args               *args   = req->args_lookup;
    struct LOOKUP3res                 res;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        chimera_vfs_lookup(thread->vfs_thread,
                           handle,
                           args->what.name.str,
                           args->what.name.len,
                           CHIMERA_VFS_ATTR_FH | CHIMERA_NFS3_ATTR_MASK,
                           CHIMERA_NFS3_ATTR_MASK,
                           chimera_nfs3_lookup_complete,
                           req);
    } else {
        res.status =
            chimera_vfs_error_to_nfsstat3(error_code);
        res.resfail.dir_attributes.attributes_follow = 0;
        shared->nfs_v3.send_reply_NFSPROC3_LOOKUP(evpl, &res, msg);
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_lookup_open_callback */


void
chimera_nfs3_lookup(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct LOOKUP3args    *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, msg);

    nfs3_dump_lookup(req, args);

    req->args_lookup = args;

    chimera_vfs_open(thread->vfs_thread,
                     args->what.dir.data.data,
                     args->what.dir.data.len,
                     CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                     chimera_nfs3_lookup_open_callback,
                     req);

} /* chimera_nfs3_lookup */
