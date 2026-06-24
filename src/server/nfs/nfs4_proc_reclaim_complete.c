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
    struct RECLAIM_COMPLETE4args *args = &argop->opreclaim_complete;
    struct RECLAIM_COMPLETE4res  *res  = &resop->opreclaim_complete;

    /* RFC 8881 §18.51.4: a second *global* (rca_one_fs == FALSE)
     * RECLAIM_COMPLETE for the same client is NFS4ERR_COMPLETE_ALREADY.  The
     * per-filesystem variant (rca_one_fs == TRUE) does not set that flag. */
    if (!args->rca_one_fs && req->session &&
        nfs4_client_mark_reclaim_complete(&thread->shared->nfs4_shared_clients,
                                          req->session->nfs4_session_clientid)) {
        res->rcr_status = NFS4ERR_COMPLETE_ALREADY;
        chimera_nfs4_compound_complete(req, res->rcr_status);
        return;
    }

    /* RFC 8881 §18.51.3: rca_one_fs == TRUE signals the client finished
     * reclaim for a SINGLE file system after migration -- it is NOT the global
     * end-of-reclaim and must not retire the client's whole reclaim obligation
     * or decrement the server-wide pending_reclaim counter (which would end the
     * grace window early for everyone).  Only the global form (rca_one_fs ==
     * FALSE) drops the recovery marker.  Chimera does not track per-fs reclaim
     * (no fs_locations/migration yet), so the per-fs variant is a no-op. */
    if (!args->rca_one_fs) {
        nfs_recovery_reclaim_complete(
            &thread->shared->nfs4_recovery,
            req->session ? req->session->client_unified : NULL);
    }

    res->rcr_status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_reclaim_complete */
