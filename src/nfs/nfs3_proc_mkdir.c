#include <fcntl.h>

#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs3_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"

static void
chimera_nfs3_mkdir_complete(
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
    struct MKDIR3res                  res;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        if (r_attr->va_set_mask & CHIMERA_VFS_ATTR_FH) {
            res.resok.obj.handle_follows = 1;
            xdr_dbuf_opaque_copy(&res.resok.obj.handle.data,
                                 r_attr->va_fh,
                                 r_attr->va_fh_len,
                                 msg->dbuf);
        } else {
            res.resok.obj.handle_follows = 0;
        }

        chimera_nfs3_set_post_op_attr(&res.resok.obj_attributes, r_attr);
        chimera_nfs3_set_wcc_data(&res.resok.dir_wcc, r_dir_pre_attr, r_dir_post_attr);
    } else {
        chimera_nfs3_set_wcc_data(&res.resfail.dir_wcc, r_dir_pre_attr, r_dir_post_attr);
    }

    chimera_vfs_release(thread->vfs_thread, req->handle);

    shared->nfs_v3.send_reply_NFSPROC3_MKDIR(evpl, &res, msg);

    nfs_request_free(thread, req);
} /* chimera_nfs3_mkdir_complete */

static void
chimera_nfs3_mkdir_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct MKDIR3args                *args   = req->args_mkdir;
    struct MKDIR3res                  res;
    struct chimera_vfs_attrs         *attr;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        xdr_dbuf_alloc_space(attr, sizeof(*attr), msg->dbuf);

        chimera_nfs3_sattr3_to_va(attr, &args->attributes);

        chimera_vfs_mkdir(thread->vfs_thread,
                          handle,
                          args->where.name.str,
                          args->where.name.len,
                          attr,
                          CHIMERA_NFS3_ATTR_MASK | CHIMERA_VFS_ATTR_FH,
                          CHIMERA_NFS3_ATTR_MASK | CHIMERA_VFS_ATTR_ATOMIC,
                          CHIMERA_NFS3_ATTR_MASK,
                          chimera_nfs3_mkdir_complete,
                          req);
    } else {
        res.status = chimera_vfs_error_to_nfsstat3(error_code);
        chimera_nfs3_set_wcc_data(&res.resfail.dir_wcc, NULL, NULL);
        shared->nfs_v3.send_reply_NFSPROC3_MKDIR(evpl, &res, msg);
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_mkdir_open_callback */

void
chimera_nfs3_mkdir(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct MKDIR3args     *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, msg);

    nfs3_dump_mkdir(req, args);

    req->args_mkdir = args;

    chimera_vfs_open(thread->vfs_thread,
                     args->where.dir.data.data,
                     args->where.dir.data.len,
                     CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                     chimera_nfs3_mkdir_open_callback,
                     req);
} /* chimera_nfs3_mkdir */
