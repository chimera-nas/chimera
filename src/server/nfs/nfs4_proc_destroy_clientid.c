// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_session.h"
#include "evpl/evpl_rpc2.h"

void
chimera_nfs4_destroy_clientid(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct DESTROY_CLIENTID4args     *args   = &argop->opdestroy_clientid;
    struct DESTROY_CLIENTID4res      *res    = &resop->opdestroy_clientid;

    /* RFC 8881 §18.50.3: when preceded by SEQUENCE, DESTROY_CLIENTID must be
     * the final operation; otherwise it must be the sole operation. */
    if (req->seen_sequence) {
        if (req->index != req->args_compound->num_argarray - 1) {
            res->dcr_status = NFS4ERR_NOT_ONLY_OP;
            chimera_nfs4_compound_complete(req, res->dcr_status);
            return;
        }
    } else if (req->args_compound->num_argarray != 1) {
        res->dcr_status = NFS4ERR_NOT_ONLY_OP;
        chimera_nfs4_compound_complete(req, res->dcr_status);
        return;
    }

    /* NFS4ERR_STALE_CLIENTID for an unknown clientid, NFS4ERR_CLIENTID_BUSY
     * if it still owns sessions, else tear the record (and its unified state
     * hierarchy) down. */
    res->dcr_status = nfs4_client_destroy_clientid(&shared->nfs4_shared_clients,
                                                   &shared->nfs4_state_table,
                                                   thread->vfs_thread,
                                                   args->dca_clientid);

    chimera_nfs4_compound_complete(req, res->dcr_status);
} /* chimera_nfs4_destroy_clientid */
