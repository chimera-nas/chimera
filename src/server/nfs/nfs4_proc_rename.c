// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "nfs4_status.h"

static void
chimera_nfs4_rename_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs_request *req = private_data;
    struct RENAME4res  *res = &req->res_compound.resarray[req->index].oprename;

    res->status = NFS4_OK;

    chimera_vfs_release(req->thread->vfs_thread, req->handle);

    chimera_nfs4_compound_complete(req, res->status);
} /* chimera_nfs4_rename_complete */

static void
chimera_nfs4_rename_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request               *req    = private_data;
    struct chimera_server_nfs_thread *thread = req->thread;
    struct RENAME4args               *args;
    struct RENAME4res                *res;

    args = &req->args_compound->argarray[req->index].oprename;
    res  = &req->res_compound.resarray[req->index].oprename;

    if (error_code != CHIMERA_VFS_OK) {
        res->status = NFS4ERR_IO;
        chimera_nfs4_compound_complete(req,
                                       chimera_nfs4_errno_to_nfsstat4(error_code));
        return;
    }

    req->handle = handle;

    chimera_vfs_rename(
        thread->vfs_thread,
        req->saved_fh,
        req->saved_fhlen,
        args->oldname.data,
        args->oldname.len,
        req->fh,
        req->fhlen,
        args->newname.data,
        args->newname.len,
        chimera_nfs4_rename_complete,
        req);

} /* chimera_nfs4_rename_open_callback */


void
chimera_nfs4_rename(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    chimera_vfs_open(thread->vfs_thread,
                     req->fh,
                     req->fhlen,
                     CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                     chimera_nfs4_rename_open_callback,
                     req);

} /* chimera_nfs4_create */