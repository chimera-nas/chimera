// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "nfs4_registration.h"

void
chimera_nfs4_setclientid(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct SETCLIENTID4args *args = &argop->opsetclientid;
    struct nfs4_reg_args     ra;

    /* RFC 7530 §16.33: record (or update) an unconfirmed client record.  The
     * clientid is not usable until SETCLIENTID_CONFIRM, so no session is
     * created here -- that happens at confirm time.  A fresh clientid comes
     * from the client ID allocator; the reply is sent once it answers
     * (nfs4_registration.h). */
    memset(&ra, 0, sizeof(ra));
    ra.kind                      = NFS4_REG_SETCLIENTID;
    ra.owner                     = args->client.id.data;
    ra.owner_len                 = args->client.id.len;
    ra.verifier                  = *(uint64_t *) args->client.verifier;
    ra.principal.flavor          = req->principal_flavor;
    ra.principal.uid             = req->principal_uid;
    ra.principal.gid             = req->principal_gid;
    ra.principal.machinename     = req->principal_machinename;
    ra.principal.machinename_len = req->principal_machinename_len;
    ra.minorversion              = req->minorversion;
    ra.cb_program                = args->callback.cb_program;
    ra.cb_ident                  = args->callback_ident;
    ra.cb_netid                  = args->callback.cb_location.na_r_netid.str;
    ra.cb_netid_len              = args->callback.cb_location.na_r_netid.len;
    ra.cb_addr                   = args->callback.cb_location.na_r_addr.str;
    ra.cb_addr_len               = args->callback.cb_location.na_r_addr.len;

    nfs4_registration_run(thread, req, argop, resop, &ra);
} /* chimera_nfs4_setclientid */

void
chimera_nfs4_setclientid_reply(
    struct chimera_server_nfs_thread     *thread,
    struct nfs_request                   *req,
    struct nfs_argop4                    *argop,
    struct nfs_resop4                    *resop,
    const struct nfs4_setclientid_result *scid)
{
    struct SETCLIENTID4args          *args   = &argop->opsetclientid;
    struct SETCLIENTID4res           *res    = &resop->opsetclientid;
    struct chimera_server_nfs_shared *shared = thread->shared;

    if (scid->status != NFS4_OK) {
        res->status = scid->status;
        /* The NFS4ERR_CLID_INUSE arm of SETCLIENTID4res carries a client_using
         * (netaddr4) that the marshaller encodes even on error, so it must be
         * initialised.  We do not track the conflicting client's address. */
        if (scid->status == NFS4ERR_CLID_INUSE) {
            res->client_using.na_r_netid.len = 0;
            res->client_using.na_r_netid.str = (char *) "";
            res->client_using.na_r_addr.len  = 0;
            res->client_using.na_r_addr.str  = (char *) "";
        }
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* RFC 7530 §16.33: capture the callback path (cb_client4) so the server
     * can deliver CB_RECALL once it grants a delegation.  Staged, not applied:
     * §16.33.5 says the update "is only confirmed if followed up by a
     * SETCLIENTID_CONFIRM", which is why the RFC can call a stray SETCLIENTID
     * carrying stale callback info harmless.  Applying it here let an
     * unconfirmed (or replayed) request retarget a confirmed client's callback
     * path and tear down its working back-channel; the confirm handler commits
     * it instead.  A coalesced duplicate carries identical callback arguments,
     * so staging them again is a no-op. */
    nfs4_client_stage_cb_path(&shared->nfs4_shared_clients,
                              scid->clientid,
                              args->callback.cb_program,
                              args->callback_ident,
                              args->callback.cb_location.na_r_netid.str,
                              args->callback.cb_location.na_r_netid.len,
                              args->callback.cb_location.na_r_addr.str,
                              args->callback.cb_location.na_r_addr.len);

    res->status          = NFS4_OK;
    res->resok4.clientid = scid->clientid;
    memcpy(res->resok4.setclientid_confirm, scid->confirm,
           sizeof(res->resok4.setclientid_confirm));

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_setclientid_reply */
