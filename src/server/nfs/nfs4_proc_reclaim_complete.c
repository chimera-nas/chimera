// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "evpl/evpl_rpc2.h"

void
chimera_nfs4_reclaim_complete(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct RECLAIM_COMPLETE4res *res = &resop->opreclaim_complete;

    res->rcr_status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_reclaim_complete */