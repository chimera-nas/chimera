// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "nfs4_callback.h"

/*
 * BACKCHANNEL_CTL (RFC 8881 §18.33).  Lets a client change the callback
 * program number and/or the RPC auth the server uses on the session's
 * backchannel after CREATE_SESSION.  The new values replace the old ones
 * (§18.33.3): they go on the client's callback path, which the next callback
 * reads its credentials from, and on the session, so a backchannel bound to it
 * later (BIND_CONN_TO_SESSION) keeps them rather than the ones CREATE_SESSION
 * gave.  An open channel copied the program when it was opened, so when this
 * op arrives on the session's backchannel connection the channel is rebuilt
 * there, as BIND_CONN_TO_SESSION does, and the next callback goes to the new
 * program.  Arriving on another connection, it is not rebuilt: the rebuilt
 * channel would be owned by this thread and send on a connection another
 * thread owns.  The new program then takes effect at the next rebind.
 */
void
chimera_nfs4_backchannel_ctl(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct BACKCHANNEL_CTL4args *args = &argop->opbackchannel_ctl;
    struct BACKCHANNEL_CTL4res  *res  = &resop->opbackchannel_ctl;
    struct nfs4_session         *session;
    uint32_t                     flavor, uid, gid;

    if (!req->session || !req->session->client_unified) {
        res->bcr_status = NFS4ERR_OP_NOT_IN_SESSION;
        chimera_nfs4_compound_complete(req, res->bcr_status);
        return;
    }

    session = req->session;
    nfs4_cb_sec_from_parms(args->bca_sec_parms, args->num_bca_sec_parms,
                           &flavor, &uid, &gid);

    nfs4_client_set_cb_sec(&thread->shared->nfs4_shared_clients,
                           session->client_unified->client_id,
                           args->bca_cb_program, flavor, uid, gid);

    if (args->bca_cb_program) {
        session->nfs4_session_cb_program = args->bca_cb_program;
    }
    session->nfs4_session_cb_sec_flavor = flavor;
    session->nfs4_session_cb_sec_uid    = uid;
    session->nfs4_session_cb_sec_gid    = gid;

    if (session->nfs4_session_backchannel_conn &&
        session->nfs4_session_backchannel_conn == req->conn) {
        nfs4_cb_resend_recalls_on_rebind(thread, session->client_unified, req);
    }

    res->bcr_status = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_backchannel_ctl */
