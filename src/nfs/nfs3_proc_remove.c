#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs3_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"

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
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct REMOVE3res                 res;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        if (pre_attr->va_mask & CHIMERA_NFS3_ATTR_MASK) {
            res.resok.dir_wcc.before.attributes_follow = 1;
            chimera_nfs3_marshall_wcc_attrs(pre_attr,
                                            &res.resok.dir_wcc.before.attributes);
        } else {
            res.resok.dir_wcc.before.attributes_follow = 0;
        }

        if (post_attr->va_mask & CHIMERA_NFS3_ATTR_MASK) {
            res.resok.dir_wcc.after.attributes_follow = 1;
            chimera_nfs3_marshall_attrs(post_attr,
                                        &res.resok.dir_wcc.after.attributes);
        } else {
            res.resok.dir_wcc.after.attributes_follow = 0;
        }
    } else {
        res.resfail.dir_wcc.before.attributes_follow = 0;
        res.resfail.dir_wcc.after.attributes_follow  = 0;
    }

    chimera_vfs_release(thread->vfs_thread, req->handle);

    shared->nfs_v3.send_reply_NFSPROC3_REMOVE(evpl, &res, msg);

    nfs_request_free(thread, req);
} /* chimera_nfs3_remove_complete */

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
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct REMOVE3args               *args   = req->args_remove;
    struct REMOVE3res                 res;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        chimera_vfs_remove(thread->vfs_thread,
                           handle,
                           args->object.name.str,
                           args->object.name.len,
                           CHIMERA_NFS3_ATTR_MASK,
                           chimera_nfs3_remove_complete,
                           req);
    } else {
        res.status                                   = chimera_vfs_error_to_nfsstat3(error_code);
        res.resfail.dir_wcc.before.attributes_follow = 0;
        res.resfail.dir_wcc.after.attributes_follow  = 0;
        shared->nfs_v3.send_reply_NFSPROC3_REMOVE(evpl, &res, msg);
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_remove_open_callback */

void
chimera_nfs3_remove(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct REMOVE3args    *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, msg);

    nfs3_dump_remove(req, args);

    req->args_remove = args;

    chimera_vfs_open(thread->vfs_thread,
                     args->object.dir.data.data,
                     args->object.dir.data.len,
                     CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                     chimera_nfs3_remove_open_callback,
                     req);
} /* chimera_nfs3_remove */
