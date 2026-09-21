// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/sdk/vfs_xattr_name.h"
#include "vfs/vfs_compound.h"

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

/* PUTFH, OPEN_CURRENT, SETXATTR: the change is op 2 of the run. */
#define NFS4_SETXATTR_OP_SETXATTR 2

static void
chimera_nfs4_setxattr_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    struct SETXATTR4res                  *res = &req->res_compound.resarray[req->index].opsetxattr;
    const struct chimera_vfs_compound_op *xop;
    enum chimera_vfs_error                error_code;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code == CHIMERA_VFS_OK) {
        /* The ctimes either side of the change, which the op sampled atomically
         * with it. */
        xop = chimera_vfs_compound_op(compound,
                                      NFS4_SETXATTR_OP_SETXATTR);
        res->sxr_status = NFS4_OK;
        chimera_nfs4_setxattr_fill(res, &xop->pre_ctime, &xop->post_ctime);
    } else {
        res->sxr_status = chimera_nfs4_errno_to_nfsstat4(error_code);
    }

    chimera_vfs_compound_free(compound);

    chimera_nfs4_compound_complete(req, res->sxr_status);
} /* chimera_nfs4_setxattr_complete */

void
chimera_nfs4_setxattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct SETXATTR4args        *args = &argop->opsetxattr;
    struct SETXATTR4res         *res  = &resop->opsetxattr;
    struct chimera_vfs_compound *compound;
    char                        *name;
    int                          namelen;

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

    /* The name is staged before the sequence rather than after the open,
     * because the adder takes it -- which is where the VFS-compound path
     * stages it too. */
    res->sxr_status = chimera_nfs4_xattr_stage_name(req, args->sxa_key.data,
                                                    args->sxa_key.len,
                                                    &name, &namelen);

    if (res->sxr_status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, res->sxr_status);
        return;
    }

    req->handle = NULL;

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED, 0);
    chimera_vfs_compound_add_setxattr(compound, args->sxa_option,
                                      name, namelen,
                                      args->sxa_value.data,
                                      args->sxa_value.len);

    chimera_vfs_compound_submit(compound, chimera_nfs4_setxattr_complete, req);
} /* chimera_nfs4_setxattr */
