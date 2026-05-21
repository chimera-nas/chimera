// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "vfs/vfs_state.h"

/*
 * DELEGRETURN (RFC 7530 §16.6 / RFC 8881 §18.5).  The client returns a
 * delegation -- voluntarily or in response to a CB_RECALL.  We release the
 * backing vfs_state CACHING lease (which lets any conflicting acquirer that
 * triggered a recall make progress) and tear the delegation state down.
 */
void
chimera_nfs4_delegreturn(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct DELEGRETURN4args  *args      = &argop->opdelegreturn;
    struct DELEGRETURN4res   *res       = &resop->opdelegreturn;
    struct nfs_state_table   *table     = &thread->shared->nfs4_state_table;
    struct chimera_vfs_state *vfs_state = thread->vfs->vfs_state;
    void                     *state_void;
    uint8_t                   state_type;
    struct nfs_delegation    *deleg;
    nfsstat4                  status;

    status = nfs_state_table_acquire(table,
                                     &args->deleg_stateid,
                                     NFS4_SLOT_TYPE_DELEG,
                                     &state_void,
                                     &state_type);

    if (status != NFS4_OK) {
        res->status = status;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    deleg = state_void;

    /* Release the lease now so a conflicting open/IO awaiting the recall can
     * proceed on its next attempt.  Clearing lease_held first keeps the
     * delegation cleanup (and any racing recall completion) from touching it
     * again.  All of these run on this client's connection thread. */
    if (deleg->lease_held) {
        chimera_vfs_lease_release(vfs_state, deleg->file_state, &deleg->lease);
        deleg->lease_held = false;
    }
    atomic_store_explicit(&deleg->cb_recall_state, NFS4_DELEG_RETURNED,
                          memory_order_release);

    /* Destroy under the acquire-ref (pins it across teardown), then drop the
     * ref; final cleanup (state_put on file_state, free) runs on the last
     * ref, deferred if an in-flight recall still holds one. */
    nfs_delegation_destroy(deleg, table, thread->vfs_thread);
    nfs_state_table_release(table, deleg, NFS4_SLOT_TYPE_DELEG,
                            thread->vfs_thread);

    res->status = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_delegreturn */

/*
 * DELEGPURGE (RFC 7530 §16.5).  Purges delegations the client may reclaim
 * after its own reboot (CLAIM_DELEGATE_PREV).  This server does not persist
 * delegations across a client reboot, so there is never anything to purge --
 * acknowledge success.
 */
void
chimera_nfs4_delegpurge(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct DELEGPURGE4res *res = &resop->opdelegpurge;

    (void) thread;
    (void) argop;

    res->status = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_delegpurge */
