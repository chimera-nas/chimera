#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "vfs/vfs_open_cache.h"
#include "nfs3_dump.h"
#include "vfs/vfs_procs.h"

static void
chimera_nfs3_rmdir_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct RMDIR3res                  res;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        res.resok.dir_wcc.before.attributes_follow = 0;
        res.resok.dir_wcc.after.attributes_follow  = 0;
    }

    chimera_vfs_open_cache_release(thread->vfs->vfs_open_file_cache, req->handle);

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
                           chimera_nfs3_rmdir_complete,
                           req);
    } else {
        res.status                                   = chimera_vfs_error_to_nfsstat3(error_code);
        res.resfail.dir_wcc.before.attributes_follow = 0;
        res.resfail.dir_wcc.after.attributes_follow  = 0;
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
                     CHIMERA_VFS_OPEN_RDONLY,
                     chimera_nfs3_rmdir_open_callback,
                     req);
} /* chimera_nfs3_rmdir */
