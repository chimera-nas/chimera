#include <fcntl.h>

#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs3_attr.h"
#include "vfs/vfs_procs.h"

static void
chimera_nfs3_create_open_at_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_attr,
    void                           *private_data)
{
    struct nfs_request               *req           = private_data;
    struct chimera_server_nfs_thread *thread        = req->thread;
    struct chimera_server_nfs_shared *shared        = thread->shared;
    struct chimera_vfs_open_handle   *parent_handle = req->handle;
    struct evpl                      *evpl          = thread->evpl;
    struct evpl_rpc2_msg             *msg           = req->msg;
    struct CREATE3res                 res;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {

        if (attr->va_mask & CHIMERA_VFS_ATTR_FH) {
            res.resok.obj.handle_follows = 1;
            xdr_dbuf_opaque_copy(&res.resok.obj.handle.data,
                                 handle->fh,
                                 handle->fh_len,
                                 msg->dbuf);

        } else {
            res.resok.obj.handle_follows = 0;
        }

        if ((attr->va_mask & CHIMERA_NFS3_ATTR_MASK) == CHIMERA_NFS3_ATTR_MASK) {
            res.resok.obj_attributes.attributes_follow = 1;
            chimera_nfs3_marshall_attrs(attr, &res.resok.obj_attributes.attributes);
        } else {
            res.resok.obj_attributes.attributes_follow = 0;
        }

        res.resok.dir_wcc.before.attributes_follow = 0;

        if ((dir_attr->va_mask & CHIMERA_NFS3_ATTR_MASK) == CHIMERA_NFS3_ATTR_MASK) {
            res.resok.dir_wcc.after.attributes_follow = 1;
            chimera_nfs3_marshall_attrs(dir_attr, &res.resok.dir_wcc.after.attributes);
        } else {
            res.resok.dir_wcc.after.attributes_follow = 0;
        }

    }

    chimera_vfs_release(thread->vfs, parent_handle);
    chimera_vfs_release(thread->vfs, handle);

    shared->nfs_v3.send_reply_NFSPROC3_CREATE(evpl, &res, msg);
    nfs_request_free(thread, req);
} /* chimera_nfs3_create_open_at_complete */

static void
chimera_nfs3_create_open_at_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *parent_handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct CREATE3args               *args   = req->args_create;
    uint32_t                          open_flags;

    if (error_code != CHIMERA_VFS_OK) {
        struct CREATE3res res;
        res.status = chimera_vfs_error_to_nfsstat3(error_code);
        thread->shared->nfs_v3.send_reply_NFSPROC3_CREATE(thread->evpl, &res, req->msg);
        nfs_request_free(thread, req);
        return;
    }

    req->handle = parent_handle;

    open_flags = CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_RDWR;

    chimera_vfs_open_at(thread->vfs,
                        parent_handle,
                        args->where.name.str,
                        args->where.name.len,
                        open_flags,
                        S_IWUSR | S_IRUSR,
                        CHIMERA_NFS3_ATTR_MASK | CHIMERA_VFS_ATTR_FH,
                        chimera_nfs3_create_open_at_complete,
                        req);
} /* chimera_nfs3_create_open_complete */

void
chimera_nfs3_create(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct CREATE3args    *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req              = nfs_request_alloc(thread, conn, msg);
    req->args_create = args;

    chimera_vfs_open(thread->vfs,
                     args->where.dir.data.data,
                     args->where.dir.data.len,
                     CHIMERA_VFS_OPEN_RDONLY,
                     chimera_nfs3_create_open_at_parent_complete,
                     req);
} /* chimera_nfs3_create */
