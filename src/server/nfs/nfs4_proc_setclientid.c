// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_session.h"
#include "nfs4_state.h"

void
chimera_nfs4_setclientid(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct SETCLIENTID4args          *args   = &argop->opsetclientid;
    struct SETCLIENTID4res           *res    = &resop->opsetclientid;
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct nfs4_client_principal      principal;
    struct nfs4_setclientid_result    scid;

    principal.flavor          = req->principal_flavor;
    principal.uid             = req->principal_uid;
    principal.gid             = req->principal_gid;
    principal.machinename     = req->principal_machinename;
    principal.machinename_len = req->principal_machinename_len;

    /* RFC 7530 §16.33: record (or update) an unconfirmed client record.  The
     * clientid is not usable until SETCLIENTID_CONFIRM, so no session is
     * created here -- that happens at confirm time. */
    nfs4_client_setclientid(&shared->nfs4_shared_clients,
                            args->client.id.data,
                            args->client.id.len,
                            *(uint64_t *) args->client.verifier,
                            &principal,
                            req->minorversion,
                            &scid);

    /* A replaced unconfirmed record's state hierarchy is torn down outside the
     * table lock. */
    if (scid.destroy_unified) {
        nfs_client_destroy(scid.destroy_unified,
                           &shared->nfs4_state_table,
                           thread->vfs_thread);
    }

    if (scid.status != NFS4_OK) {
        res->status = scid.status;
        /* The NFS4ERR_CLID_INUSE arm of SETCLIENTID4res carries a client_using
         * (netaddr4) that the marshaller encodes even on error, so it must be
         * initialised.  We do not track the conflicting client's address. */
        if (scid.status == NFS4ERR_CLID_INUSE) {
            res->client_using.na_r_netid.len = 0;
            res->client_using.na_r_netid.str = (char *) "";
            res->client_using.na_r_addr.len  = 0;
            res->client_using.na_r_addr.str  = (char *) "";
        }
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    res->status          = NFS4_OK;
    res->resok4.clientid = scid.clientid;
    memcpy(res->resok4.setclientid_confirm, scid.confirm,
           sizeof(res->resok4.setclientid_confirm));

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_setclientid */
