// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_session.h"
#include "nfs4_state.h"

/*
 * RFC 7530 §16.41: RELEASE_LOCKOWNER notifies the server that the client is
 * done with a lock-owner.  If the lock-owner still holds byte-range locks the
 * request must be refused with NFS4ERR_LOCKS_HELD; otherwise it succeeds.
 */
void
chimera_nfs4_release_lockowner(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct RELEASE_LOCKOWNER4args *args   = &argop->oprelease_lockowner;
    struct RELEASE_LOCKOWNER4res  *res    = &resop->oprelease_lockowner;
    struct nfs_client             *client =
        req->session ? req->session->client_unified : NULL;
    struct nfs_lock_owner         *lo;
    struct nfs_lock_state         *ls;
    bool                           held = false;

    if (!client || args->lock_owner.clientid != client->client_id) {
        res->status = NFS4ERR_STALE_CLIENTID;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    pthread_mutex_lock(&client->lock);

    HASH_FIND(hh, client->lock_owners_by_str,
              args->lock_owner.owner.data, args->lock_owner.owner.len, lo);

    if (lo) {
        pthread_mutex_lock(&lo->lock);
        for (ls = lo->states; ls; ls = ls->next_in_owner) {
            if (ls->range_leases != NULL) {
                held = true;
                break;
            }
        }
        pthread_mutex_unlock(&lo->lock);
    }

    pthread_mutex_unlock(&client->lock);

    res->status = held ? NFS4ERR_LOCKS_HELD : NFS4_OK;
    chimera_nfs4_compound_complete(req, res->status);
} /* chimera_nfs4_release_lockowner */
