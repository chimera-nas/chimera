// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_recovery.h"
#include "evpl/evpl_rpc2.h"

void
chimera_nfs4_reclaim_complete(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct RECLAIM_COMPLETE4res *res = &resop->opreclaim_complete;

    /* RFC 8881 §18.51 / RFC 7530 §10.2.5: client signals end of reclaim
     * activity.  Drop the per-client recovery marker.  When persistence
     * lands and the to_reclaim hash is populated at boot, the
     * pending_reclaim counter hitting zero will also end the grace window
     * early via nfs_recovery_sweep_once. */
    nfs_recovery_reclaim_complete(
        &thread->shared->nfs4_recovery,
        req->session ? req->session->client_unified : NULL);

    res->rcr_status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_reclaim_complete */
