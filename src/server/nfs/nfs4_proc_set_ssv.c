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
 * SET_SSV.  For any client that issues it anyway this remains a benign no-op:
 * we accept the contribution and complete the exchange.  ssr_digest is the
 * server's proof-of-knowledge over the SEQUENCE reply; it is returned empty
 * because no SSV is in effect (clients that do not verify it -- e.g. pynfs
 * EID50 -- are unaffected).  Full SSV enforcement (storing the SSV, computing
 * the HMAC digest, binding RPCSEC_GSS credentials) is deferred.
 */
void
chimera_nfs4_set_ssv(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct SET_SSV4args *args = &argop->opset_ssv;
    struct SET_SSV4res  *res  = &resop->opset_ssv;

    (void) thread;

    /* SET_SSV is only meaningful inside a session (RFC 8881 §18.47.3). */
    if (!req->session) {
        res->ssr_status = NFS4ERR_OP_NOT_IN_SESSION;
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
