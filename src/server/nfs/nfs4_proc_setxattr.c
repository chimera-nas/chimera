// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"

/*
 * Fill a SETXATTR4 result's change_info from the object's ctime either side of
 * the change, as captured by the compound operation.
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

void
chimera_nfs4_setxattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct SETXATTR4res *res = &resop->opsetxattr;

    if (req->fhlen == 0) {
        res->sxr_status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->sxr_status);
        return;
    }

    if (argop->opsetxattr.sxa_option != SETXATTR4_EITHER &&
        argop->opsetxattr.sxa_option != SETXATTR4_CREATE &&
        argop->opsetxattr.sxa_option != SETXATTR4_REPLACE) {
        res->sxr_status = NFS4ERR_INVAL;
        chimera_nfs4_compound_complete(req, res->sxr_status);
        return;
    }

    res->sxr_status = chimera_nfs4_xattr_validate_name(argop->opsetxattr.sxa_key.len);
    if (res->sxr_status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, res->sxr_status);
        return;
    }

    chimera_nfs4_compound_single(thread, req);
} /* chimera_nfs4_setxattr */
