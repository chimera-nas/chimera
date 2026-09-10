// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/sdk/vfs_xattr_name.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

/*
 * Fill a SETXATTR4 result's change_info from the object's ctime either side of
 * the change.  Shared by the per-op path below and by the VFS-compound path,
 * which carries the same pair of ctimes back from the sequence.
 */
void
chimera_nfs4_setxattr_fill(
    struct SETXATTR4res   *res,
    const struct timespec *pre_ctime,
    const struct timespec *post_ctime)
{
    res->sxr_info.atomic = 1;
    res->sxr_info.before = pre_ctime->tv_sec * 1000000000ULL +
        pre_ctime->tv_nsec;
    res->sxr_info.after = post_ctime->tv_sec * 1000000000ULL +
        post_ctime->tv_nsec;
} /* chimera_nfs4_setxattr_fill */

static void
chimera_nfs4_setxattr_complete(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data)
{
    struct nfs_request  *req = private_data;
    struct SETXATTR4res *res = &req->res_compound.resarray[req->index].opsetxattr;

    if (error_code == CHIMERA_VFS_OK) {
        res->sxr_status = NFS4_OK;
        chimera_nfs4_setxattr_fill(res, &pre_attr->va_ctime,
                                   &post_attr->va_ctime);
    } else {
        res->sxr_status = chimera_nfs4_errno_to_nfsstat4(error_code);
    }

    chimera_vfs_release(req->thread->vfs_thread, req->handle);
    chimera_nfs4_compound_complete(req, res->sxr_status);
} /* chimera_nfs4_setxattr_complete */

static void
chimera_nfs4_setxattr_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request   *req  = private_data;
    struct SETXATTR4args *args = &req->args_compound->argarray[req->index].opsetxattr;
    struct SETXATTR4res  *res  = &req->res_compound.resarray[req->index].opsetxattr;
    char                 *name;
    int                   namelen;

    if (error_code != CHIMERA_VFS_OK) {
        res->sxr_status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->sxr_status);
        return;
    }

    req->handle = handle;

    res->sxr_status = chimera_nfs4_xattr_stage_name(req, args->sxa_key.data,
                                                    args->sxa_key.len,
                                                    &name, &namelen);
    if (res->sxr_status != NFS4_OK) {
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        chimera_nfs4_compound_complete(req, res->sxr_status);
        return;
    }

    chimera_vfs_set_xattr(req->thread->vfs_thread, &req->cred,
                          handle,
                          args->sxa_option,
                          name,
                          namelen,
                          args->sxa_value.data,
                          args->sxa_value.len,
                          chimera_nfs4_setxattr_complete,
                          req);
} /* chimera_nfs4_setxattr_open_callback */

void
chimera_nfs4_setxattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct SETXATTR4args *args = &argop->opsetxattr;
    struct SETXATTR4res  *res  = &resop->opsetxattr;

    if (req->fhlen == 0) {
        res->sxr_status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->sxr_status);
        return;
    }

    /* RFC 8276 §8.3: only the three defined option values are valid. */
    if (args->sxa_option != SETXATTR4_EITHER &&
        args->sxa_option != SETXATTR4_CREATE &&
        args->sxa_option != SETXATTR4_REPLACE) {
        res->sxr_status = NFS4ERR_INVAL;
        chimera_nfs4_compound_complete(req, res->sxr_status);
        return;
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED,
                        chimera_nfs4_setxattr_open_callback,
                        req);
} /* chimera_nfs4_setxattr */
