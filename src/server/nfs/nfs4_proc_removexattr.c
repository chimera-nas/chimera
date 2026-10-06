// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"

/*
 * Fill a REMOVEXATTR4 result's change_info from the object's ctime either side
 * of the removal, as captured by the compound operation.
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

void
chimera_nfs4_removexattr(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct REMOVEXATTR4res *res = &resop->opremovexattr;

    if (req->fhlen == 0) {
        res->rxr_status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->rxr_status);
        return;
    }

    res->rxr_status = chimera_nfs4_xattr_validate_name(argop->opremovexattr.rxa_name.len);
    if (res->rxr_status != NFS4_OK) {
        chimera_nfs4_compound_complete(req, res->rxr_status);
        return;
    }

    chimera_nfs4_compound_single(thread, req);
} /* chimera_nfs4_removexattr */
