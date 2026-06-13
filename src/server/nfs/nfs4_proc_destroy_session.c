// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_session.h"
#include "nfs4_drc.h"
#include "server/server.h"
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
    struct nfs4_session              *session;
    struct nfs4_session              *bound;

    /* RFC 8881 §18.37.3: when preceded by SEQUENCE, DESTROY_SESSION must be
     * the final operation; otherwise it must be the sole operation. */
    if (req->seen_sequence) {
        if (req->index != req->args_compound->num_argarray - 1) {
            res->dsr_status = NFS4ERR_NOT_ONLY_OP;
            chimera_nfs4_compound_complete(req, res->dsr_status);
            return;
        }
    } else if (req->args_compound->num_argarray != 1) {
        res->dsr_status = NFS4ERR_NOT_ONLY_OP;
        chimera_nfs4_compound_complete(req, res->dsr_status);
        return;
    }

    session = nfs4_session_lookup(&shared->nfs4_shared_clients,
                                  args->dsa_sessionid);
    if (session == NULL) {
        res->dsr_status = NFS4ERR_BADSESSION;
        chimera_nfs4_compound_complete(req, res->dsr_status);
        return;
    }

    bound = evpl_rpc2_conn_get_private_data(req->conn);
    if (!nfs4_session_is_live(bound) || bound != session) {
        nfs4_session_put(session);
        res->dsr_status = NFS4ERR_CONN_NOT_BOUND_TO_SESSION;
        chimera_nfs4_compound_complete(req, res->dsr_status);
        return;
    }

    nfs4_session_put(session);

    nfs4_destroy_session(&shared->nfs4_shared_clients, args->dsa_sessionid);

    /* Drop this session's persisted metadata + reply entries so a restart does
     * not resurrect a session the client explicitly tore down. */
    if (chimera_server_config_get_nfs4_drc(shared->config)) {
        nfs4_drc_forget_session(thread->vfs_thread, shared->node_id,
                                args->dsa_sessionid);
    }

    res->dsr_status = NFS4_OK;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_setclientid */
