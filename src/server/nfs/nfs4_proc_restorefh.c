// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"

void
chimera_nfs4_restorefh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct RESTOREFH4res *res = &resop->oprestorefh;

    if (req->saved_fhlen == 0) {
        res->status = NFS4ERR_RESTOREFH;
        chimera_nfs4_compound_complete(req, NFS4ERR_RESTOREFH);
        return;
    }

    memcpy(req->fh, req->saved_fh, req->saved_fhlen);
    req->fhlen = req->saved_fhlen;

    /* Restoring the saved handle also restores its export and re-derives the
     * squash from the original credential (the saved export may differ from the
     * current one). */
    req->export_id = req->saved_export_id;
    req->cred      = req->orig_cred;
    chimera_nfs_squash_cred(&req->cred,
                            chimera_nfs_get_export_by_id(req->thread->shared,
                                                         req->export_id));

    res->status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_restorefh */
