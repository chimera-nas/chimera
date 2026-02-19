// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"

void
chimera_nfs4_putrootfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct PUTROOTFH4res *res = &resop->opputrootfh;
    uint32_t              fhlen;

    nfs4_root_get_fh(req->fh, &fhlen);
    req->fhlen = fhlen;

    res->status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_putrootfh */
