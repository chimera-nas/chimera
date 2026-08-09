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
    struct nfs_state_table        *table      = &thread->shared->nfs4_state_table;
    struct chimera_vfs_thread     *vfs_thread = thread->vfs_thread;
    struct nfs_lock_owner         *lo;
    struct nfs_lock_state         *ls;
    bool                           held    = false;
    bool                           release = false;

    if (!client || args->lock_owner.clientid != client->client_id) {
        res->status = NFS4ERR_STALE_CLIENTID;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* RFC 7530 §9.5: RELEASE_LOCKOWNER is a clientid-bearing operation and
     * renews all of the client's leases. */
    nfs_client_touch(client);

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

        /* RFC 7530 §16.37: on success the server discards the lock-owner and
         * all state it anchors, so the client may reuse the owner string for a
         * fresh lock-owner (and a later new_lock_owner=TRUE OPEN-to-lock is not
         * rejected as a duplicate -- see R-LOCK-6 in nfs4_proc_lock.c).
         * Unpublish it here while holding client->lock; keep the hash-slot ref
         * and tear the states down below, outside the client lock. */
        if (!held) {
            HASH_DELETE(hh, client->lock_owners_by_str, lo);
            release = true;
        }
    }

    pthread_mutex_unlock(&client->lock);

    if (release) {
        /* Drain the (now rangeless) lock stateids.  nfs_lock_state_destroy
         * unlinks each entry under lo->lock, so re-read the head each pass
         * rather than iterate a list being mutated underneath us. */
        for ( ; ;) {
            pthread_mutex_lock(&lo->lock);
            ls = lo->states;
            pthread_mutex_unlock(&lo->lock);
            if (!ls) {
                break;
            }
            nfs_lock_state_destroy(ls, table, vfs_thread);
        }
        nfs_lock_owner_put(lo);
    }

    res->status = held ? NFS4ERR_LOCKS_HELD : NFS4_OK;
    chimera_nfs4_compound_complete(req, res->status);
} /* chimera_nfs4_release_lockowner */
