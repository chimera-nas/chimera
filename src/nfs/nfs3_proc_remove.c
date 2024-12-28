#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "vfs/vfs_procs.h"

static void
chimera_nfs3_remove_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct REMOVE3res                 res;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        res.resok.dir_wcc.before.attributes_follow = 0;
        res.resok.dir_wcc.after.attributes_follow  = 0;
    }

    chimera_vfs_release(thread->vfs, req->handle);

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

        chimera_vfs_remove(thread->vfs,
                           handle,
                           args->object.name.str,
                           args->object.name.len,
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

    req->args_remove = args;

    chimera_vfs_open(thread->vfs,
                     args->object.dir.data.data,
                     args->object.dir.data.len,
                     CHIMERA_VFS_OPEN_RDONLY,
                     chimera_nfs3_remove_open_callback,
                     req);
} /* chimera_nfs3_remove */
