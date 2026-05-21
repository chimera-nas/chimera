// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_session.h"
#include "nfs4_state.h"

/* RFC 7530 §16.30: RENEW refreshes the lease for the given client. Chimera
 * does not currently enforce lease expiry, so this is a no-op that always
 * succeeds. Tests that require true lease-expiry semantics (e.g. pynfs
 * testExpired) will not behave correctly until real lease tracking lands.
 *
 * One exception: if the client holds delegations but its callback path has
 * been found unusable (a CB_RECALL failed), RENEW returns NFS4ERR_CB_PATH_DOWN
 * (RFC 7530 §16.30.4) so the client knows to re-establish the callback path;
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

    res->status = NFS4_OK;

    session = nfs4_session_find_by_clientid(&thread->shared->nfs4_shared_clients,
                                            args->clientid);
    if (session) {
        struct nfs_client *client = session->client_unified;

        if (client &&
            atomic_load_explicit(&client->cb_path.cb_state,
                                 memory_order_acquire) == NFS4_CB_DOWN) {
            bool has_deleg;

            pthread_mutex_lock(&client->lock);
            has_deleg = (client->delegations != NULL);
            pthread_mutex_unlock(&client->lock);

            if (has_deleg) {
                res->status = NFS4ERR_CB_PATH_DOWN;
            }
        }
        nfs4_session_put(session);
    }

    chimera_nfs4_compound_complete(req, res->status);
} /* chimera_nfs4_renew */
