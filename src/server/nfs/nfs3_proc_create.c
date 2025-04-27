#include <fcntl.h>

#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs3_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"
static void
chimera_nfs3_create_open_at_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
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

        if (attr->va_set_mask & CHIMERA_VFS_ATTR_FH) {
            res.resok.obj.handle_follows = 1;
            xdr_dbuf_opaque_copy(&res.resok.obj.handle.data,
                                 handle->fh,
                                 handle->fh_len,
                                 msg->dbuf);

        } else {
            res.resok.obj.handle_follows = 0;
        }

        chimera_nfs3_set_post_op_attr(&res.resok.obj_attributes, attr);
        chimera_nfs3_set_wcc_data(&res.resok.dir_wcc, dir_pre_attr, dir_post_attr);

        chimera_vfs_release(thread->vfs_thread, handle);
    }

    chimera_vfs_release(thread->vfs_thread, parent_handle);

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
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct CREATE3args               *args   = req->args_create;
    struct chimera_vfs_attrs         *attr;

    if (error_code != CHIMERA_VFS_OK) {
        struct CREATE3res res;
        res.status = chimera_vfs_error_to_nfsstat3(error_code);
        chimera_nfs3_set_wcc_data(&res.resfail.dir_wcc, NULL, NULL);
        thread->shared->nfs_v3.send_reply_NFSPROC3_CREATE(thread->evpl, &res, req->msg);
        nfs_request_free(thread, req);
        return;
    }

    req->handle = parent_handle;

    xdr_dbuf_alloc_space(attr, sizeof(*attr), msg->dbuf);

    attr->va_req_mask = 0;

    switch (args->how.mode) {
        case UNCHECKED:
            chimera_nfs3_sattr3_to_va(attr, &args->how.obj_attributes);
            break;
        case GUARDED:
            chimera_nfs3_sattr3_to_va(attr, &args->how.obj_attributes);
            break;
        case EXCLUSIVE:
            break;
    } /* switch */

    chimera_vfs_open_at(thread->vfs_thread,
                        parent_handle,
                        args->where.name.str,
                        args->where.name.len,
                        CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_INFERRED,
                        attr,
                        CHIMERA_NFS3_ATTR_MASK | CHIMERA_VFS_ATTR_FH,
                        CHIMERA_NFS3_ATTR_WCC_MASK,
                        CHIMERA_NFS3_ATTR_MASK,
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

    req = nfs_request_alloc(thread, conn, msg);

    nfs3_dump_create(req, args);

    req->args_create = args;

    chimera_vfs_open(thread->vfs_thread,
                     args->where.dir.data.data,
                     args->where.dir.data.len,
                     CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                     chimera_nfs3_create_open_at_parent_complete,
                     req);
} /* chimera_nfs3_create */
