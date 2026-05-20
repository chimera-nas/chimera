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

    /* Pass state_table + vfs_thread so nfs4_client_unregister can tear
     * down the unified state hierarchy (owners, states, dup'd VFS
     * handles) along with the nfs4_client record.  Previously this leaked
     * the unified hierarchy on every DESTROY_CLIENTID. */
    nfs4_client_unregister(&shared->nfs4_shared_clients,
                           &shared->nfs4_state_table,
                           thread->vfs_thread,
                           args->dca_clientid);

    res->dcr_status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_destroy_clientid */
