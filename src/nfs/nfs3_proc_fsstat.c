#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs3_attr.h"
#include "vfs/vfs_procs.h"

static void
chimera_nfs3_fsstat_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct FSSTAT3res                 res;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if ((attr->va_mask & CHIMERA_NFS3_FSSTAT_MASK) != CHIMERA_NFS3_FSSTAT_MASK) {
        res.status = NFS3ERR_NOTSUPP;
    }

    if (res.status == NFS3_OK) {
        res.resok.obj_attributes.attributes_follow = 0;

        res.resok.tbytes   = attr->va_space_total;
        res.resok.fbytes   = attr->va_space_free;
        res.resok.abytes   = attr->va_space_used;
        res.resok.tfiles   = attr->va_files_total;
        res.resok.ffiles   = attr->va_files_free;
        res.resok.afiles   = attr->va_files_used;
        res.resok.invarsec = 0;
    }

    chimera_vfs_release(thread->vfs, req->handle);

    shared->nfs_v3.send_reply_NFSPROC3_FSSTAT(evpl, &res, msg);

    nfs_request_free(thread, req);
} /* chimera_nfs3_fsstat_complete */

static void
chimera_nfs3_fsstat_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct FSSTAT3res                 res;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        chimera_vfs_getattr(thread->vfs,
                            handle,
                            CHIMERA_NFS3_FSSTAT_MASK,
                            chimera_nfs3_fsstat_complete,
                            req);
    } else {
        res.status = chimera_vfs_error_to_nfsstat3(error_code);
        shared->nfs_v3.send_reply_NFSPROC3_FSSTAT(evpl, &res, msg);
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_fsstat_open_callback */

void
chimera_nfs3_fsstat(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct FSSTAT3args    *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req              = nfs_request_alloc(thread, conn, msg);
    req->args_fsstat = args;

    chimera_vfs_open(thread->vfs,
                     args->fsroot.data.data,
                     args->fsroot.data.len,
                     CHIMERA_VFS_OPEN_RDWR,
                     chimera_nfs3_fsstat_open_callback,
                     req);
} /* chimera_nfs3_fsstat */
