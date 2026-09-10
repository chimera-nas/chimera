// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"

/*
 * Copy `fh` into the request's saved slot, along with the export it belongs to
 * (RESTOREFH restores the squash policy with the handle).  Shared by the per-op
 * path below and by the VFS-compound path, which saves whatever object the
 * sequence had current when the SAVEFH ran rather than req->fh.
 */
void
chimera_nfs4_savefh_apply(
    struct nfs_request *req,
    const uint8_t      *fh,
    int                 fhlen)
{
    memcpy(req->saved_fh, fh, fhlen);
    req->saved_fhlen     = fhlen;
    req->saved_export_id = req->export_id;
} /* chimera_nfs4_savefh_apply */

void
chimera_nfs4_savefh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct  SAVEFH4res *res = &resop->opsavefh;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    res->status = NFS4_OK;

    chimera_nfs4_savefh_apply(req, req->fh, req->fhlen);

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_savefh */
