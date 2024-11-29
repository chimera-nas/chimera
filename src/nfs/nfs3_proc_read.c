#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs_internal.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"

static void
chimera_nfs3_read_complete(
    enum chimera_vfs_error error_code,
    uint32_t               count,
    uint32_t               eof,
    struct evpl_iovec     *iov,
    int                    niov,
    void                  *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct READ3res                   res;
    struct evpl_iovec                *iov_copy;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        res.resok.count = count;
        res.resok.eof   = eof;

        res.resok.file_attributes.attributes_follow = 0;

        iov_copy         = msg->dbuf->buffer + msg->dbuf->used;
        msg->dbuf->used += niov * sizeof(*iov);
        memcpy(iov_copy, iov, niov * sizeof(*iov));

        res.resok.data.length = count;
        res.resok.data.iov    = iov_copy;
        res.resok.data.niov   = niov;

        chimera_nfs_debug("read %d bytes into %d iovs", count, niov);
    }

    shared->nfs_v3.send_reply_NFSPROC3_READ(evpl, &res, msg);

    nfs_request_free(thread, req);
} /* chimera_nfs3_read_complete */

static void
chimera_nfs3_read_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct READ3res                   res;

    if (error_code == CHIMERA_VFS_OK) {
        chimera_nfs_debug("opened for read module %s fd %d",
                          handle->vfs_module->name,
                          (int) handle->vfs_private);

        chimera_vfs_read(thread->vfs,
                         handle,
                         req->args_read->offset,
                         req->args_read->count,
                         chimera_nfs3_read_complete,
                         req);
    } else {
        res.status = chimera_vfs_error_to_nfsstat3(error_code);
        shared->nfs_v3.send_reply_NFSPROC3_READ(evpl, &res, msg);
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_read_open_callback */

void
chimera_nfs3_read(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct READ3args      *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs_request               *req;
    struct chimera_vfs_open_handle   *handle;

    req = nfs_request_alloc(thread, conn, msg);

    req->args_read = args;

    handle = nfs3_open_cache_lookup(&shared->nfs3_open_cache,
                                    args->file.data.data,
                                    args->file.data.len);

    if (!handle) {
        chimera_nfs_debug("opening file for read");
        chimera_vfs_open(thread->vfs,
                         args->file.data.data,
                         args->file.data.len,
                         CHIMERA_VFS_OPEN_RDONLY,
                         chimera_nfs3_read_open_callback,
                         req);
        return;
    }

    chimera_vfs_read(thread->vfs,
                     handle,
                     args->offset,
                     args->count,
                     chimera_nfs3_read_complete,
                     req);
} /* chimera_nfs3_read */
