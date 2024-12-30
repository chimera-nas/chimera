#include "nfs3_procs.h"
#include "nfs3_status.h"
#include "nfs3_attr.h"
#include "vfs/vfs_procs.h"
#include "nfs3_dump.h"

static void
chimera_nfs3_symlink_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *r_attr,
    struct chimera_vfs_attrs *r_dir_attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl                      *evpl   = thread->evpl;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct SYMLINK3res                res;

    res.status = chimera_vfs_error_to_nfsstat3(
        error_code);

    if (res.status == NFS3_OK) {
        if (r_attr->va_mask & CHIMERA_VFS_ATTR_FH) {
            res.resok.obj.handle_follows = 1;
            xdr_dbuf_opaque_copy(&res.resok.obj.handle.data,
                                 r_attr->va_fh,
                                 r_attr->va_fh_len,
                                 msg->dbuf);
        } else {
            res.resok.obj.handle_follows = 0;
        }

        if ((r_attr->va_mask & CHIMERA_NFS3_ATTR_MASK) == CHIMERA_NFS3_ATTR_MASK
            ) {
            res.resok.obj_attributes.attributes_follow = 1;
            chimera_nfs3_marshall_attrs(r_attr,
                                        &res.resok.obj_attributes.attributes);
        } else {
            res.resok.obj_attributes.attributes_follow = 0;
        }

        res.resok.dir_wcc.before.attributes_follow = 0;

        if ((r_dir_attr->va_mask & CHIMERA_NFS3_ATTR_MASK) ==
            CHIMERA_NFS3_ATTR_MASK) {
            res.resok.dir_wcc.after.attributes_follow = 1;
            chimera_nfs3_marshall_attrs(r_dir_attr,
                                        &res.resok.dir_wcc.after.attributes);
        } else {
            res.resok.dir_wcc.after.attributes_follow = 0;
        }
    }

    shared->nfs_v3.send_reply_NFSPROC3_SYMLINK(evpl, &res, msg);

    nfs_request_free(thread, req);
} /* chimera_nfs3_mkdir_complete */
void
chimera_nfs3_symlink(
    struct evpl           *evpl,
    struct evpl_rpc2_conn *conn,
    struct SYMLINK3args   *args,
    struct evpl_rpc2_msg  *msg,
    void                  *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs_request               *req;

    req = nfs_request_alloc(thread, conn, msg);

    nfs3_dump_symlink(req, args);

    chimera_vfs_symlink(thread->vfs,
                        args->where.dir.data.data,
                        args->where.dir.data.len,
                        args->where.name.str,
                        args->where.name.len,
                        args->symlink.symlink_data.str,
                        args->symlink.symlink_data.len,
                        CHIMERA_VFS_ATTR_FH | CHIMERA_NFS3_ATTR_MASK,
                        chimera_nfs3_symlink_complete,
                        req);
} /* chimera_nfs3_symlink */
