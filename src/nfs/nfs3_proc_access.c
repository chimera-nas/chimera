#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs3_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_open_cache.h"
#include "nfs3_dump.h"

static void
chimera_nfs3_access_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct ACCESS3args               *args   = req->args_access;
    struct ACCESS3res                 res;

    res.status = chimera_vfs_error_to_nfsstat3(error_code);

    if (res.status == NFS3_OK) {
        if ((attr->va_mask & CHIMERA_NFS3_ATTR_MASK) == CHIMERA_NFS3_ATTR_MASK) {
            res.resok.obj_attributes.attributes_follow = 1;
            chimera_nfs3_marshall_attrs(attr, &res.resok.obj_attributes.attributes);
        } else {
            res.resok.obj_attributes.attributes_follow = 0;
        }

        res.resok.access = 0;

        if (args->access & ACCESS3_READ) {
            if (attr->va_mode & S_IRUSR) {
                res.resok.access |= ACCESS3_READ;
            }
        }

        if (args->access & (ACCESS3_DELETE | ACCESS3_MODIFY | ACCESS3_EXTEND)) {
            if (attr->va_mode & S_IWUSR) {
                if (args->access & ACCESS3_DELETE) {
                    res.resok.access |= ACCESS3_DELETE;
                }
                if (args->access & ACCESS3_MODIFY) {
                    res.resok.access |= ACCESS3_MODIFY;
                }
                if (args->access & ACCESS3_EXTEND) {
                    res.resok.access |= ACCESS3_EXTEND;
                }
            }
        }

        if (args->access & (ACCESS3_EXECUTE | ACCESS3_LOOKUP)) {
            if (attr->va_mode & S_IXUSR) {
                res.resok.access |= ACCESS3_EXECUTE;
            }
            if (attr->va_mode & S_IXUSR) {
                res.resok.access |= ACCESS3_LOOKUP;
            }
        }
    }

    chimera_vfs_open_cache_release(thread->vfs->vfs_open_file_cache, req->handle);

    shared->nfs_v3.send_reply_NFSPROC3_ACCESS(evpl, &res, msg);

    nfs_request_free(thread, req);
} /* chimera_nfs3_access_complete */

static void
chimera_nfs3_access_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct ACCESS3res                 res;

    if (error_code == CHIMERA_VFS_OK) {
        req->handle = handle;

        chimera_vfs_getattr(thread->vfs_thread,
                            handle,
                            CHIMERA_NFS3_ATTR_MASK,
                            chimera_nfs3_access_complete,
                            req);
    } else {
        res.status = chimera_vfs_error_to_nfsstat3(error_code);
        shared->nfs_v3.send_reply_NFSPROC3_ACCESS(evpl, &res, msg);
        nfs_request_free(thread, req);
    }
} /* chimera_nfs3_access_open_callback */

void
chimera_nfs3_access(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct ACCESS3args    *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, msg);

    nfs3_dump_access(req, args);

    req->args_access = args;

    chimera_vfs_open(thread->vfs_thread,
                     args->object.data.data,
                     args->object.data.len,
                     CHIMERA_VFS_OPEN_RDONLY,
                     chimera_nfs3_access_open_callback,
                     req);
} /* chimera_nfs3_access */
