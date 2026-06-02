// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <xxhash.h>

#include "nfs4_procs.h"
#include "nfs4_attr.h"
#include "server/server.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_state.h"
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
chimera_nfs4_rename_do(struct nfs_request *req)
{
    struct RENAME4args *args = &req->args_compound->argarray[req->index].oprename;

    chimera_vfs_rename_at(
        req->thread->vfs_thread,
        &req->cred, NULL,
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
} /* chimera_nfs4_rename_do */

/* Recall a delegation found on `attr` (if any); returns true if a recall was
 * kicked and the caller should fail with NFS4ERR_DELAY. */
static bool
chimera_nfs4_rename_recall(
    struct nfs_request       *req,
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr)
{
    if (error_code == CHIMERA_VFS_OK &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        uint64_t fh_hash = XXH3_64bits(attr->va_fh, attr->va_fh_len) & INT64_MAX;

        return chimera_vfs_state_break_caching(req->thread->vfs->vfs_state,
                                               attr->va_fh, attr->va_fh_len,
                                               fh_hash);
    }
    return false;
} /* chimera_nfs4_rename_recall */

static void
chimera_nfs4_rename_delay(struct nfs_request *req)
{
    struct RENAME4res *res = &req->res_compound.resarray[req->index].oprename;

    chimera_vfs_release(req->thread->vfs_thread, req->handle);
    res->status = NFS4ERR_DELAY;
    chimera_nfs4_compound_complete(req, NFS4ERR_DELAY);
} /* chimera_nfs4_rename_delay */

/* RFC 7530 §10.4.5: a RENAME must recall a delegation on the file being
 * renamed over (the target name), if one exists, before proceeding. */
static void
chimera_nfs4_rename_recall_target(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;

    if (chimera_nfs4_rename_recall(req, error_code, attr)) {
        chimera_nfs4_rename_delay(req);
        return;
    }
    chimera_nfs4_rename_do(req);
} /* chimera_nfs4_rename_recall_target */

/* Recall a delegation on the rename source, then check the target. */
static void
chimera_nfs4_rename_recall_source(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct nfs_request *req  = private_data;
    struct RENAME4args *args = &req->args_compound->argarray[req->index].oprename;

    if (chimera_nfs4_rename_recall(req, error_code, attr)) {
        chimera_nfs4_rename_delay(req);
        return;
    }

    chimera_vfs_lookup(req->thread->vfs_thread, &req->cred, NULL,
                       req->fh, req->fhlen,
                       args->newname.data, args->newname.len,
                       CHIMERA_VFS_ATTR_FH, 0,
                       chimera_nfs4_rename_recall_target, req);
} /* chimera_nfs4_rename_recall_source */

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

    /* When delegations are enabled, recall any delegation on the source file
     * and on a file being renamed over before performing the rename. */
    if (chimera_server_config_get_nfs4_delegations(thread->shared->config)) {
        chimera_vfs_lookup(thread->vfs_thread, &req->cred, NULL,
                           req->saved_fh, req->saved_fhlen,
                           args->oldname.data, args->oldname.len,
                           CHIMERA_VFS_ATTR_FH, 0,
                           chimera_nfs4_rename_recall_source, req);
        return;
    }

    chimera_nfs4_rename_do(req);
} /* chimera_nfs4_rename_open_callback */



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

    if (req->fhlen == 0 || req->saved_fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    status = chimera_nfs4_validate_name(&args->oldname);

    if (status == NFS4_OK) {
        status = chimera_nfs4_validate_name(&args->newname);
    }

    if (status != NFS4_OK) {
        res->status = status;
        chimera_nfs4_compound_complete(req, status);
        return;
    }

    chimera_vfs_open_fh(thread->vfs_thread, &req->cred, NULL,
                        req->fh,
                        req->fhlen,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_nfs4_rename_open_callback,
                        req);

} /* chimera_nfs4_create */