// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_session.h"
#include "evpl/evpl_rpc2.h"

/*
 * SET_SSV (RFC 8881 §18.47): the client sets (XORs in) the shared secret value
 * the server negotiated at EXCHANGE_ID time when SP4_SSV state protection was
 * requested.  The SSV underpins the RPCSEC_GSS SSV mechanism used to integrity-
 * or privacy-protect the state-protected operations.
 *
 * chimera declines SP4_SSV at EXCHANGE_ID (see nfs4_set_state_protect, which
 * falls back to SP4_NONE because the SSV-backed GSS credential is not
 * enforced), so a conforming client never negotiates SSV and never reaches
 * SET_SSV.  A client that issues it anyway is told so: §18.47.3 makes SET_SSV
 * NFS4ERR_INVAL unless the client opted for SP4_SSV, which is what the
 * negotiated mode recorded on the client record (nfs4_client_sp_how) says.
 * Full SSV support (storing the SSV, computing the ssr_digest HMAC, binding
 * RPCSEC_GSS credentials) is deferred; until it exists no client reaches the
 * success path below.
 */
void
chimera_nfs4_set_ssv(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct SET_SSV4args              *args   = &argop->opset_ssv;
    struct SET_SSV4res               *res    = &resop->opset_ssv;
    bool                              ssv_negotiated;

    /* SET_SSV is only meaningful inside a session (RFC 8881 §18.47.3). */
    if (!req->session) {
        res->ssr_status = NFS4ERR_OP_NOT_IN_SESSION;
        chimera_nfs4_compound_complete(req, res->ssr_status);
        return;
    }

    /* RFC 8881 §18.47.3: SET_SSV MUST NOT be used by a client that did not opt
     * for SP4_SSV state protection at EXCHANGE_ID; the server returns
     * NFS4ERR_INVAL. */
    {
        struct nfs4_client *c;

        pthread_mutex_lock(&shared->nfs4_shared_clients.nfs4_ct_lock);
        HASH_FIND(nfs4_client_hh_by_id,
                  shared->nfs4_shared_clients.nfs4_ct_clients_by_id,
                  &req->session->nfs4_session_clientid,
                  sizeof(req->session->nfs4_session_clientid), c);
        ssv_negotiated = c && c->nfs4_client_sp_how == SP4_SSV;
        pthread_mutex_unlock(&shared->nfs4_shared_clients.nfs4_ct_lock);
    }

    if (!ssv_negotiated) {
        res->ssr_status = NFS4ERR_INVAL;
        chimera_nfs4_compound_complete(req, res->ssr_status);
        return;
    }

    /* A zero-length SSV contribution is malformed. */
    if (args->ssa_ssv.len == 0) {
        res->ssr_status = NFS4ERR_INVAL;
        chimera_nfs4_compound_complete(req, res->ssr_status);
        return;
    }

    res->ssr_status                 = NFS4_OK;
    res->ssr_resok4.ssr_digest.len  = 0;
    res->ssr_resok4.ssr_digest.data = NULL;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_set_ssv */
