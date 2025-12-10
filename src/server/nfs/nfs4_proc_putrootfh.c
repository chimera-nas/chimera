// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "vfs/vfs_procs.h"

void
chimera_nfs4_putrootfh(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct PUTROOTFH4res *res = &resop->opputrootfh;

    req->fh[0] = CHIMERA_VFS_FH_MAGIC_ROOT;
    req->fhlen = 1;

    res->status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_putrootfh */