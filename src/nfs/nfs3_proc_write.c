#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs_internal.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"

static void
chimera_nfs3_write_complete(
    enum chimera_vfs_error error_code,
    uint32_t               length,
    void                  *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct WRITE3res                  res;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        res.resok.count                             = length;
        res.resok.committed                         = 0;
        res.resok.file_wcc.before.attributes_follow = 0;
        res.resok.file_wcc.after.attributes_follow  = 0;
    }

    shared->nfs_v3.send_reply_NFSPROC3_WRITE(evpl, &res, msg);

    nfs_request_free(thread, req);
} /* chimera_nfs3_write_complete */

static void
chimera_nfs3_write_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct WRITE3args                *args   = req->args_write;
    struct WRITE3res                  res;

    if (error_code == CHIMERA_VFS_OK) {

        nfs3_open_cache_insert(&shared->nfs3_open_cache, handle);

        chimera_vfs_write(thread->vfs,
                          handle,
                          args->offset,
                          args->count,
                          args->data.iov,
                          args->data.niov,
                          chimera_nfs3_write_complete,
                          req);
    } else {
        res.status = chimera_vfs_error_to_nfsstat3(error_code);
        shared->nfs_v3.send_reply_NFSPROC3_WRITE(evpl, &res, msg);
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_read_open_callback */
void
chimera_nfs3_write(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct WRITE3args     *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct chimera_vfs_open_handle   *handle;

    handle = nfs3_open_cache_lookup(&shared->nfs3_open_cache,
                                    args->file.data.data,
                                    args->file.data.len);

    req = nfs_request_alloc(thread, conn, msg);

    req->args_write = args;

    if (!handle) {
        fprintf(stderr, "Opening file for write\n");
        chimera_vfs_open(thread->vfs,
                         args->file.data.data,
                         args->file.data.len,
                         CHIMERA_VFS_OPEN_RDWR,
                         chimera_nfs3_write_open_callback,
                         req);
        return;
    }

    chimera_vfs_write(thread->vfs,
                      handle,
                      args->offset,
                      args->count,
                      args->data.iov,
                      args->data.niov,
                      chimera_nfs3_write_complete,
                      req);
} /* chimera_nfs3_write */
