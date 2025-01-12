#include "nfs4_procs.h"
#include "nfs4_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "nfs4_status.h"

static void
chimera_nfs4_create_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;
    struct CREATE4res  *res = &req->res_compound.resarray[req->index].opcreate;

    res->status = NFS4_OK;

    chimera_vfs_release(req->thread->vfs_thread, req->handle);

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_create_complete */

static void
chimera_nfs4_create_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct evpl_rpc2_msg             *msg    = req->msg;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct CREATE4args               *args;
    struct CREATE4res                *res;
    struct chimera_vfs_attrs         *attr;

    args = &req->args_compound->argarray[req->index].opcreate;
    res  = &req->res_compound.resarray[req->index].opcreate;

    xdr_dbuf_alloc_space(attr, sizeof(*attr), msg->dbuf);

    chimera_nfs4_unmarshall_attrs(attr,
                                  args->createattrs.num_attrmask,
                                  args->createattrs.attrmask,
                                  args->createattrs.attr_vals.data,
                                  args->createattrs.attr_vals.len);

    xdr_dbuf_alloc_space(res->resok4.attrset, sizeof(uint32_t) * 4, msg->dbuf);
    res->resok4.num_attrset = chimera_nfs4_mask2attr(attr, res->resok4.attrset, 4);

    if (error_code == CHIMERA_VFS_OK) {

        switch (args->objtype.type) {
            case NF4REG:
                break;
            case NF4DIR:
                chimera_vfs_mkdir(thread->vfs_thread,
                                  handle,
                                  args->objname.data,
                                  args->objname.len,
                                  attr,
                                  0,
                                  0,
                                  0,
                                  chimera_nfs4_create_complete,
                                  req);
                break;
            case NF4BLK:
                break;
            case NF4CHR:
                break;
            case NF4LNK:
                break;
            case NF4SOCK:
                break;
            case NF4FIFO:
                break;
            case NF4ATTRDIR:
                break;
            case NF4NAMEDATTR:
                break;
            default:
                chimera_nfs4_compound_complete(req,
                                               NFS4ERR_BADTYPE);
                chimera_vfs_release(thread->vfs_thread, req->handle);
                return;
        } /* switch */
    } else {
        chimera_nfs4_compound_complete(req,
                                       chimera_nfs4_errno_to_nfsstat4(error_code));
    }
} /* chimera_nfs3_mkdir_open_callback */


void
chimera_nfs4_create(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    chimera_vfs_open(thread->vfs_thread,
                     req->fh,
                     req->fhlen,
                     CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                     chimera_nfs4_create_open_callback,
                     req);

} /* chimera_nfs4_create */
