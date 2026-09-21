// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/sdk/vfs_xattr_name.h"
#include "vfs/vfs_compound.h"

/*
 * Fill a REMOVEXATTR4 result's change_info from the object's ctime either side
 * of the removal.  Shared by the per-op path below and by the VFS-compound
 * path, which carries the same pair of ctimes back from the sequence.
 */
void
chimera_nfs4_removexattr_fill(
    struct REMOVEXATTR4res *res,
    const struct timespec  *pre_ctime,
    const struct timespec  *post_ctime)
{
    res->rxr_info.atomic = 1;
    res->rxr_info.before = pre_ctime->tv_sec * 1000000000ULL +
        pre_ctime->tv_nsec;
    res->rxr_info.after = post_ctime->tv_sec * 1000000000ULL +
        post_ctime->tv_nsec;
} /* chimera_nfs4_removexattr_fill */

/* PUTFH, OPEN_CURRENT, REMOVEXATTR: the removal is op 2 of the run. */
#define NFS4_REMOVEXATTR_OP_REMOVEXATTR 2

static void
chimera_nfs4_removexattr_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs_request                   *req = private_data;
    struct REMOVEXATTR4res               *res = &req->res_compound.resarray[req->index].opremovexattr;
    const struct chimera_vfs_compound_op *xop;
    enum chimera_vfs_error                error_code;

    error_code = chimera_vfs_compound_status(compound);

    if (error_code == CHIMERA_VFS_OK) {
        /* The ctimes either side of the removal, which the op sampled
         * atomically with it. */
        xop = chimera_vfs_compound_op(compound,
                                      NFS4_REMOVEXATTR_OP_REMOVEXATTR);
        res->rxr_status = NFS4_OK;
        chimera_nfs4_removexattr_fill(res, &xop->pre_ctime, &xop->post_ctime);
    } else {
        res->rxr_status = chimera_nfs4_errno_to_nfsstat4(error_code);
    }

    chimera_vfs_compound_free(compound);

    chimera_nfs4_compound_complete(req, res->rxr_status);
} /* chimera_nfs4_removexattr_complete */

void
chimera_nfs4_removexattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct REMOVEXATTR4args     *args = &argop->opremovexattr;
    struct REMOVEXATTR4res      *res  = &resop->opremovexattr;
    struct chimera_vfs_compound *compound;
    char                        *name;
    int                          namelen;

    if (req->fhlen == 0) {
        res->rxr_status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->rxr_status);
        return;
    }

    /* The name is staged before the sequence rather than after the open,
     * because the adder takes it -- which is where the VFS-compound path
     * stages it too. */
    res->rxr_status = chimera_nfs4_xattr_stage_name(req, args->rxa_name.data,
                                                    args->rxa_name.len,
                                                    &name, &namelen);

    if (res->rxr_status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, res->rxr_status);
        return;
    }

    req->handle = NULL;

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    chimera_vfs_compound_add_putfh(compound, req->fh, req->fhlen);
    chimera_vfs_compound_add_open_current(compound,
                                          CHIMERA_VFS_OPEN_INFERRED, 0);
    chimera_vfs_compound_add_removexattr(compound, name, namelen);

    chimera_vfs_compound_submit(compound, chimera_nfs4_removexattr_complete,
                                req);
} /* chimera_nfs4_removexattr */
