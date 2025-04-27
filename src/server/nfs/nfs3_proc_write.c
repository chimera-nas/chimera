#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs3_attr.h"
#include "nfs_internal.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "nfs3_dump.h"

static void
chimera_nfs3_write_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct WRITE3res                  res;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        res.resok.count = length;

        if (sync) {
            res.resok.committed = FILE_SYNC;
        } else {
            res.resok.committed = UNSTABLE;
        }

        memcpy(res.resok.verf,
               &shared->nfs_verifier,
               sizeof(res.resok.verf));

        chimera_nfs3_set_wcc_data(&res.resok.file_wcc, pre_attr, post_attr);
    } else {
        chimera_nfs3_set_wcc_data(&res.resfail.file_wcc, pre_attr, post_attr);
    }

    chimera_vfs_release(thread->vfs_thread, req->handle);

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

        req->handle = handle;
        chimera_vfs_write(thread->vfs_thread,
                          handle,
                          args->offset,
                          args->count,
                          (args->stable != UNSTABLE),
                          CHIMERA_NFS3_ATTR_WCC_MASK,
                          CHIMERA_NFS3_ATTR_MASK,
                          args->data.iov,
                          args->data.niov,
                          chimera_nfs3_write_complete,
                          req);
    } else {
        res.status =
            chimera_vfs_error_to_nfsstat3(error_code);
        chimera_nfs3_set_wcc_data(&res.resfail.file_wcc, NULL, NULL);
        shared->nfs_v3.send_reply_NFSPROC3_WRITE(evpl, &res, msg);
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_write_open_callback */

void
chimera_nfs3_write(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct WRITE3args     *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, msg);

    nfs3_dump_write(req, args);

    req->args_write = args;

    chimera_vfs_open(thread->vfs_thread,
                     args->file.data.data,
                     args->file.data.len,
                     CHIMERA_VFS_OPEN_INFERRED,
                     chimera_nfs3_write_open_callback,
                     req);
} /* chimera_nfs3_write */
