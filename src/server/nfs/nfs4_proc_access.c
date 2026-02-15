// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"

void
chimera_nfs4_access(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct ACCESS4args *args = &argop->opaccess;
    struct ACCESS4res  *res  = &resop->opaccess;

    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, NFS4ERR_NOFILEHANDLE);
        return;
    }

    res->status = NFS4_OK;

    res->resok4.supported = args->access;
    res->resok4.access    = args->access;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_access */
