// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_attr.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "nfs4_status.h"

static void
chimera_nfs4_rename_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *fromdir_pre_attr,
    struct chimera_vfs_attrs *fromdir_post_attr,
    struct chimera_vfs_attrs *todir_pre_attr,
    struct chimera_vfs_attrs *todir_post_attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;
    struct RENAME4res  *res = &req->res_compound.resarray[req->index].oprename;
    nfsstat4            status;

    if (error_code != CHIMERA_VFS_OK) {
        status      = chimera_nfs4_errno_to_nfsstat4(error_code);
        res->status = status;
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        chimera_nfs4_compound_complete(req, status);
        return;
    }

    res->status = NFS4_OK;

    chimera_nfs4_set_changeinfo(&res->resok4.source_cinfo, fromdir_pre_attr, fromdir_post_attr);
    chimera_nfs4_set_changeinfo(&res->resok4.target_cinfo, todir_pre_attr, todir_post_attr);

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
        &req->cred,
        req->saved_fh,
        req->saved_fhlen,
        args->oldname.data,
        args->oldname.len,
        req->fh,
        req->fhlen,
        args->newname.data,
        args->newname.len,
        NULL,
        0,
        CHIMERA_VFS_ATTR_MTIME,
        CHIMERA_VFS_ATTR_MTIME,
        chimera_nfs4_rename_complete,
        req);

} /* chimera_nfs4_rename_open_callback */


static nfsstat4
chimera_nfs4_check_name(const xdr_opaque *name)
{
    if (name->len == 0) {
        return NFS4ERR_INVAL;
    }

    if ((name->len == 1 && ((const char *) name->data)[0] == '.') ||
        (name->len == 2 && ((const char *) name->data)[0] == '.' &&
         ((const char *) name->data)[1] == '.')) {
        return NFS4ERR_BADNAME;
    }

    return NFS4_OK;
} /* chimera_nfs4_check_name */

void
chimera_nfs4_rename(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct RENAME4args *args = &argop->oprename;
    struct RENAME4res  *res  = &resop->oprename;
    nfsstat4            status;

    status = chimera_nfs4_check_name(&args->oldname);

    if (status == NFS4_OK) {
        status = chimera_nfs4_check_name(&args->newname);
    }

    if (status != NFS4_OK) {
        res->status = status;
        chimera_nfs4_compound_complete(req, status);
        return;
    }

    chimera_vfs_open(thread->vfs_thread, &req->cred,
                     req->fh,
                     req->fhlen,
                     CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                     chimera_nfs4_rename_open_callback,
                     req);

} /* chimera_nfs4_create */