// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_attr.h"

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

    chimera_nfs4_compound_single(thread, req);
} /* chimera_nfs4_rename */
