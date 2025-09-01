// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "evpl/evpl_rpc2.h"

void
chimera_nfs4_destroy_session(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct DESTROY_SESSION4args      *args   = &argop->opdestroy_session;
    struct DESTROY_SESSION4res       *res    = &resop->opdestroy_session;


    nfs4_destroy_session(
        &shared->nfs4_shared_clients,
        args->dsa_sessionid);


    res->dsr_status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_setclientid */