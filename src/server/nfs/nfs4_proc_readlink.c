// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"

/* Copy the compound-owned link target into the accepted reply. */
nfsstat4
chimera_nfs4_readlink_fill(
    struct nfs_request  *req,
    struct READLINK4res *res,
    const char          *target,
    uint32_t             target_len)
{
    res->resok4.link.data = xdr_dbuf_alloc_space(4096, req->encoding->dbuf);

    if (!res->resok4.link.data) {
        return NFS4ERR_RESOURCE;
    }

    if (target_len > 4096) {
        target_len = 4096;
    }
    memcpy(res->resok4.link.data, target, target_len);
    res->resok4.link.len = target_len;

    return NFS4_OK;
} /* chimera_nfs4_readlink_fill */

void
chimera_nfs4_readlink(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct READLINK4res *res = &resop->opreadlink;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    chimera_nfs4_compound_single(thread, req);
} /* chimera_nfs4_readlink */
