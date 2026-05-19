// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
void
chimera_nfs4_close(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct CLOSE4args      *args   = &argop->opclose;
    struct CLOSE4res       *res    = &resop->opclose;
    struct nfs_state_table *table  = &thread->shared->nfs4_state_table;
    bool                    is_v40 = (req->minorversion == 0);
    void                   *state_void;
    uint8_t                 state_type;
    nfsstat4                status;
    uint32_t                client_short_id;

    status = nfs_state_table_acquire(table,
                                     &args->open_stateid,
                                     NFS4_SLOT_TYPE_OPEN,
                                     &state_void,
                                     &state_type);

    if (status != NFS4_OK) {
        res->status = status;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    struct nfs_open_state *open_state = state_void;
    struct nfs_open_owner *owner      = open_state->owner;

    if (is_v40 && owner) {
        pthread_mutex_lock(&owner->lock);

        int seqid_class = nfs4_owner_seqid_classify(owner->seqid,
                                                    &owner->replay,
                                                    args->seqid);

        if (seqid_class == NFS4_SEQID_REPLAY) {
            /* RFC 7530 §9.1.7: return cached reply.  Replay does NOT
             * destroy the state -- the original CLOSE already did that
             * (or, if it didn't, our re-execution would have side
             * effects we must avoid). */
            res->status       = owner->replay.status;
            res->open_stateid = owner->replay.stateid;
            pthread_mutex_unlock(&owner->lock);
            nfs_state_table_release(table, open_state, NFS4_SLOT_TYPE_OPEN,
                                    thread->vfs_thread);
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }

        if (seqid_class != NFS4_SEQID_NEW) {
            pthread_mutex_unlock(&owner->lock);
            nfs_state_table_release(table, open_state, NFS4_SLOT_TYPE_OPEN,
                                    thread->vfs_thread);
            res->status = NFS4ERR_BAD_SEQID;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }

        pthread_mutex_unlock(&owner->lock);
    }

    /* Bump stateid.seqid per RFC 7530 §16.2.4, then encode the returned
     * stateid before destroying.  Destroy cascades through any lock_states
     * rooted on this open. */
    open_state->seqid += 1;
    client_short_id    = owner ? (uint32_t) owner->client->client_id : 0;

    nfs4_stateid_encode(&res->open_stateid, open_state->seqid,
                        NFS4_STATEID_TYPE_OPEN,
                        open_state->shard, open_state->slot_idx,
                        open_state->generation, client_short_id);

    /* Record reply BEFORE destroying the state -- after destroy, the owner
     * may also be torn down by client teardown.  In practice the owner
     * persists until DESTROY_CLIENTID / expiry, so recording here keeps
     * the cached stateid available for any retransmit. */
    if (is_v40 && owner) {
        pthread_mutex_lock(&owner->lock);
        owner->seqid = args->seqid;
        nfs4_replay_record(&owner->replay, args->seqid, OP_CLOSE,
                           NFS4_OK, &res->open_stateid);
        pthread_mutex_unlock(&owner->lock);
    }

    /* Drop the acquire ref, then destroy.  If concurrent READ/WRITE still
     * hold an acquire-ref, destroy marks the state and cleanup runs on the
     * last release; otherwise destroy releases the handle here. */
    nfs_state_table_release(table, open_state, NFS4_SLOT_TYPE_OPEN,
                            thread->vfs_thread);
    nfs_open_state_destroy(open_state, table, thread->vfs_thread);

    res->status = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_close */
