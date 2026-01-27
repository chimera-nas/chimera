// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
static void
chimera_nfs4_commit_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;
    struct COMMIT4res  *res = &req->res_compound.resarray[req->index].opcommit;

    if (error_code == CHIMERA_VFS_OK) {
        res->status = NFS4_OK;

        memcpy(res->resok4.writeverf,
               &req->thread->shared->nfs_verifier,
               sizeof(res->resok4.writeverf));
    } else {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
    }

    chimera_vfs_release(req->thread->vfs_thread, req->handle);

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_commit_complete */

static void
chimera_nfs4_commit_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *file_handle,
    void                           *private_data)
{
    struct nfs_request *req  = private_data;
    struct COMMIT4args *args = &req->args_compound->argarray[req->index].opcommit;
    struct COMMIT4res  *res  = &req->res_compound.resarray[req->index].opcommit;

    req->handle = file_handle;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    chimera_vfs_commit(req->thread->vfs_thread, &req->cred,
                       file_handle,
                       args->offset,
                       args->count,
                       0, 0,
                       chimera_nfs4_commit_complete,
                       req);
} /* chimera_nfs4_commit_open_callback */

void
chimera_nfs4_commit(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    chimera_vfs_open(thread->vfs_thread, &req->cred,
                     req->fh,
                     req->fhlen,
                     CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                     chimera_nfs4_commit_open_callback,
                     req);
} /* chimera_nfs4_commit */