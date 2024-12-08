#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs3_attr.h"
#include "nfs_internal.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"

static void
chimera_nfs3_read_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct READ3res                   res;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        res.resok.count = count;
        res.resok.eof   = eof;

        if ((attr->va_mask & CHIMERA_NFS3_ATTR_MASK) == CHIMERA_NFS3_ATTR_MASK)
        {
            res.resok.file_attributes.attributes_follow = 1;
            chimera_nfs3_marshall_attrs(attr,
                                        &res.resok.file_attributes.attributes);
        } else {
            res.resok.file_attributes.attributes_follow = 0;
        }

        res.resok.data.length = count;
        res.resok.data.iov    = iov;
        res.resok.data.niov   = niov;
    } else {
        res.resfail.file_attributes.attributes_follow = 0;
    }

    shared->nfs_v3.send_reply_NFSPROC3_READ(evpl, &res, msg);

    chimera_vfs_close(thread->vfs, req->handle);

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
    struct READ3args                 *args   = req->args_read;
    struct READ3res                   res;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        chimera_vfs_read(thread->vfs,
                         handle,
                         args->offset,
                         args->count,
                         CHIMERA_NFS3_ATTR_MASK,
                         chimera_nfs3_read_complete,
                         req);
    } else {
        res.status =
            chimera_vfs_error_to_nfsstat3(error_code);
        res.resfail.file_attributes.attributes_follow = 0;
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
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, msg);

    req->args_read = args;

    chimera_vfs_open(thread->vfs,
                     args->file.data.data,
                     args->file.data.len,
                     CHIMERA_VFS_OPEN_RDWR,
                     chimera_nfs3_read_open_callback,
                     req);
} /* chimera_nfs3_read */
