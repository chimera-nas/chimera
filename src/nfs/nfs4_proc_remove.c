#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
static void
chimera_nfs4_remove_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct REMOVE4res                *res    = &req->res_compound.resarray[req->index].opremove;

    if (error_code == CHIMERA_VFS_OK) {
        res->status = NFS4_OK;
    } else {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
    }

    chimera_vfs_release(thread->vfs_thread, req->handle);

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_remove_complete */

static void
chimera_nfs4_remove_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *parent_handle,
    void                           *private_data)
{
    struct nfs_request *req  = private_data;
    struct REMOVE4args *args = &req->args_compound->argarray[req->index].opremove;

    req->handle = parent_handle;

    if (error_code != NFS4_OK) {
        struct REMOVE4res *res = &req->res_compound.resarray[req->index].opremove;
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    chimera_vfs_remove(req->thread->vfs_thread,
                       parent_handle,
                       args->target.data,
                       args->target.len,
                       0,
                       0,
                       chimera_nfs4_remove_complete,
                       req);


} /* chimera_nfs4_open_complete */


void
chimera_nfs4_remove(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    chimera_vfs_open(thread->vfs_thread,
                     req->fh,
                     req->fhlen,
                     CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                     chimera_nfs4_remove_open_callback,
                     req);

} /* chimera_nfs4_create */
