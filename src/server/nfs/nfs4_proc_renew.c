// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_session.h"
#include "nfs4_state.h"

/* RFC 7530 §16.28: RENEW refreshes the lease for the given client. Chimera
 * does enforce lease expiry (nfs4_lease.c sweeps expired leases at ~1 Hz), so
 * RENEW returns NFS4ERR_STALE_CLIENTID for an unknown client and
 * NFS4ERR_EXPIRED when the client's lease has been lost to a conflicting
 * acquirer.
 *
 * One exception: if the client holds delegations but its callback path has
 * been found unusable (a CB_RECALL failed), RENEW returns NFS4ERR_CB_PATH_DOWN
 * (RFC 7530 §16.28.4) so the client knows to re-establish the callback path;
 * the server retains the delegations meanwhile. */
void
chimera_nfs4_renew(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct RENEW4args   *args = &argop->oprenew;
    struct RENEW4res    *res  = &resop->oprenew;
    struct nfs4_session *session;

    session = nfs4_session_find_by_clientid(&thread->shared->nfs4_shared_clients,
                                            args->clientid);
    if (!session || !session->client_unified) {
        res->status = NFS4ERR_STALE_CLIENTID;
    } else {
        struct nfs_client *client = session->client_unified;

        if (client->expired ||
            atomic_load_explicit(&client->reclaim_pending,
                                 memory_order_acquire)) {
            /* A reclaimed courtesy client has lost its lease to a conflicting
             * acquirer (RFC 7530 §16.30). */
            res->status = NFS4ERR_EXPIRED;
        } else {
            res->status = NFS4_OK;
            nfs_client_touch(client);

            if (atomic_load_explicit(&client->cb_path.cb_state,
                                     memory_order_acquire) == NFS4_CB_DOWN) {
                bool has_deleg;

                pthread_mutex_lock(&client->lock);
                has_deleg = (client->delegations != NULL);
                pthread_mutex_unlock(&client->lock);

                if (has_deleg) {
                    res->status = NFS4ERR_CB_PATH_DOWN;
                }
            }
        }
    }

    if (session) {
        nfs4_session_put(session);
    }

    chimera_nfs4_compound_complete(req, res->status);
} /* chimera_nfs4_renew */
