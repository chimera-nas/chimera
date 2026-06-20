// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_session.h"
#include "nfs4_recovery.h"
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

    /* RFC 8881 §18.50.3: when preceded by SEQUENCE, DESTROY_CLIENTID must be
     * the final operation; otherwise it must be the sole operation. */
    if (req->seen_sequence) {
        if (req->index != req->args_compound->num_argarray - 1) {
            res->dcr_status = NFS4ERR_NOT_ONLY_OP;
            chimera_nfs4_compound_complete(req, res->dcr_status);
            return;
        }
    } else if (req->args_compound->num_argarray != 1) {
        res->dcr_status = NFS4ERR_NOT_ONLY_OP;
        chimera_nfs4_compound_complete(req, res->dcr_status);
        return;
    }

    /* Capture the client's owner string before teardown so we can delete its
     * persistent recovery record once the destroy succeeds (the nfs4_client is
     * freed inside nfs4_client_destroy_clientid). */
    uint8_t  owner[NFS4_OPAQUE_LIMIT];
    uint16_t owner_len  = 0;
    bool     wrong_cred = false;
    {
        struct nfs4_client                *c;
        const struct nfs4_client_principal p = {
            .flavor          = req->principal_flavor,
            .uid             = req->principal_uid,
            .gid             = req->principal_gid,
            .machinename     = req->principal_machinename,
            .machinename_len = req->principal_machinename_len,
        };

        pthread_mutex_lock(&shared->nfs4_shared_clients.nfs4_ct_lock);
        HASH_FIND(nfs4_client_hh_by_id,
                  shared->nfs4_shared_clients.nfs4_ct_clients_by_id,
                  &args->dca_clientid, sizeof(args->dca_clientid), c);
        if (c) {
            /* SP4_MACH_CRED: only the bound machine credential may destroy
             * this client (RFC 8881 §2.10.8.3). */
            if (!nfs4_client_mach_cred_ok(c, &p)) {
                wrong_cred = true;
            } else {
                owner_len = c->nfs4_client_owner_len;
                memcpy(owner, c->nfs4_client_owner, owner_len);
            }
        }
        pthread_mutex_unlock(&shared->nfs4_shared_clients.nfs4_ct_lock);
    }

    if (wrong_cred) {
        res->dcr_status = NFS4ERR_WRONG_CRED;
        chimera_nfs4_compound_complete(req, res->dcr_status);
        return;
    }

    /* NFS4ERR_STALE_CLIENTID for an unknown clientid, NFS4ERR_CLIENTID_BUSY
     * if it still owns sessions, else tear the record (and its unified state
     * hierarchy) down. */
    res->dcr_status = nfs4_client_destroy_clientid(&shared->nfs4_shared_clients,
                                                   &shared->nfs4_state_table,
                                                   thread->vfs_thread,
                                                   args->dca_clientid);

    if (res->dcr_status == NFS4_OK && owner_len) {
        nfs_recovery_forget(thread->vfs_thread, &shared->nfs4_recovery,
                            owner, owner_len);
    }

    chimera_nfs4_compound_complete(req, res->dcr_status);
} /* chimera_nfs4_destroy_clientid */
