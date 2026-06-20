// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "nfs4_callback.h"
#include "nfs4_drc.h"
#include "evpl/evpl_rpc2.h"

/*
 * BIND_CONN_TO_SESSION (RFC 8881 §18.34 / §2.10.3.1).  Associates the
 * connection this request arrived on with an existing session, in the fore
 * channel, the back channel, or both.
 *
 * The fore-channel association is normally implicit: SEQUENCE already calls
 * nfs4_session_bind_conn() on every request (see nfs4_proc_sequence.c), so a
 * client doing nconnect-style trunking never needs this op for the fore
 * channel.  The reason this op matters is the *back* channel: the callback
 * channel used for delegation recalls is pinned to whichever connection
 * carried CREATE_SESSION with CREATE_SESSION4_FLAG_CONN_BACK_CHAN.  When that
 * connection drops, a client re-establishes callbacks by binding the back
 * channel to a surviving connection here -- the same repoint + recall re-drive
 * that CREATE_SESSION performs (see nfs4_proc_create_session.c).
 *
 * The op carries no cb_program / sec_parms, so (unlike CREATE_SESSION /
 * BACKCHANNEL_CTL) it does not change the callback program or credentials --
 * those were established earlier; we only repoint the transport.
 *
 * Chimera multiplexes both channels over a single TCP connection, so binding
 * either direction makes both available at no extra cost; the _OR_BOTH
 * variants are therefore honored as "both" (matching Linux nfsd).
 */
void
chimera_nfs4_bind_conn_to_session(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct BIND_CONN_TO_SESSION4args *args = &argop->opbind_conn_to_session;
    struct BIND_CONN_TO_SESSION4res  *res  = &resop->opbind_conn_to_session;
    struct nfs4_session              *session;
    bool                              bind_fore, bind_back;
    channel_dir_from_server4          reply_dir;

    session = nfs4_session_lookup(&thread->shared->nfs4_shared_clients,
                                  args->bctsa_sessid);
    if (!session) {
        /* Lazily reconstruct a failed-over client's session from the shared KV
         * store (NFS4ERR_DELAY = retry shortly); see nfs4_proc_sequence.c. */
        nfsstat4 hs = (nfs4_drc_session_hydrate(thread, args->bctsa_sessid) ==
                       NFS4_DRC_HYDRATE_INFLIGHT) ? NFS4ERR_DELAY
                                                  : NFS4ERR_BADSESSION;
        res->bctsr_status = hs;
        chimera_nfs4_compound_complete(req, hs);
        return;
    }

    /* SP4_MACH_CRED: only the bound machine credential may bind a connection
     * to the session (RFC 8881 §2.10.8.3). */
    {
        const struct nfs4_client_principal p = {
            .flavor          = req->principal_flavor,
            .uid             = req->principal_uid,
            .gid             = req->principal_gid,
            .machinename     = req->principal_machinename,
            .machinename_len = req->principal_machinename_len,
        };
        if (!nfs4_client_mach_cred_ok(session->nfs4_session_client, &p)) {
            nfs4_session_put(session);
            res->bctsr_status = NFS4ERR_WRONG_CRED;
            chimera_nfs4_compound_complete(req, NFS4ERR_WRONG_CRED);
            return;
        }
    }

    switch (args->bctsa_dir) {
        case CDFC4_FORE:
            bind_fore = true;
            bind_back = false;
            reply_dir = CDFS4_FORE;
            break;
        case CDFC4_BACK:
            bind_fore = false;
            bind_back = true;
            reply_dir = CDFS4_BACK;
            break;
        case CDFC4_FORE_OR_BOTH:
        case CDFC4_BACK_OR_BOTH:
            bind_fore = true;
            bind_back = true;
            reply_dir = CDFS4_BOTH;
            break;
        default:
            nfs4_session_put(session);
            res->bctsr_status = NFS4ERR_INVAL;
            chimera_nfs4_compound_complete(req, NFS4ERR_INVAL);
            return;
    } /* switch */

    /* Bind the conn to the session (idempotent; takes the conn's own +1 ref
     * unless it is already bound to this session).  This holds a ref before we
     * drop the lookup ref below, mirroring SEQUENCE. */
    if (bind_fore || bind_back) {
        nfs4_session_bind_conn(req->conn, session);
    }

    if (bind_back) {
        /* Repoint the backchannel transport to this connection and re-drive any
         * delegation recall that was stuck with no live backchannel (RFC 8881
         * §20.4.1; the same path CREATE_SESSION uses for DSESS9003). */
        session->nfs4_session_backchannel_conn = req->conn;
        /* This compound runs on req->conn's owner thread; record it so
         * cross-thread callbacks marshal their send to the new owner. */
        session->nfs4_session_backchannel_owner = thread;
        nfs4_cb_resend_recalls_on_rebind(thread, session->client_unified, req);
    }

    /* Drop the +1 from nfs4_session_lookup; the conn's ref keeps the session
     * alive for the rest of this compound. */
    nfs4_session_put(session);

    res->bctsr_status = NFS4_OK;
    memcpy(res->bctsr_resok4.bctsr_sessid, session->nfs4_session_id,
           NFS4_SESSIONID_SIZE);
    res->bctsr_resok4.bctsr_dir = reply_dir;
    /* We never run an RDMA backchannel; honestly decline RDMA mode. */
    res->bctsr_resok4.bctsr_use_conn_in_rdma_mode = false;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_bind_conn_to_session */
