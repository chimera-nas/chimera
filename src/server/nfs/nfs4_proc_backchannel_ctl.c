// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_session.h"
#include "nfs4_state.h"

/*
 * BACKCHANNEL_CTL (RFC 8881 §18.33).  Lets a client change the callback
 * program number and/or the RPC auth the server uses on the session's
 * backchannel after CREATE_SESSION.  We update the client's callback path so
 * subsequent CB_RECALL / CB_GETATTR use the new program and credentials.
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

    if (!req->session || !req->session->client_unified) {
        res->bcr_status = NFS4ERR_OP_NOT_IN_SESSION;
        chimera_nfs4_compound_complete(req, res->bcr_status);
        return;
    }

    {
        uint64_t clientid = req->session->client_unified->client_id;
        uint32_t flavor   = AUTH_NONE;
        uint32_t uid      = 0, gid = 0;

        if (args->num_bca_sec_parms > 0) {
            struct callback_sec_parms4 *sp = &args->bca_sec_parms[0];

            flavor = sp->cb_secflavor;
            if (flavor == AUTH_SYS) {
                uid = sp->cbsp_sys_cred.uid;
                gid = sp->cbsp_sys_cred.gid;
            }
        }

        nfs4_client_set_cb_sec(&thread->shared->nfs4_shared_clients,
                               clientid, args->bca_cb_program,
                               flavor, uid, gid);
    }

    res->bcr_status = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_backchannel_ctl */
