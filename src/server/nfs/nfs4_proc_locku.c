// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

static void
chimera_nfs4_locku_complete(
    enum chimera_vfs_error error_code,
    uint32_t               conflict_type,
    uint64_t               conflict_offset,
    uint64_t               conflict_length,
    pid_t                  conflict_pid,
    void                  *private_data)
{
    struct nfs_request     *req        = private_data;
    struct LOCKU4args      *args       = &req->args_compound->argarray[req->index].oplocku;
    struct LOCKU4res       *res        = &req->res_compound.resarray[req->index].oplocku;
    struct nfs_state_table *table      = &req->thread->shared->nfs4_state_table;
    struct nfs_lock_state  *lock_state = req->nfs_state_ref;
    struct nfs_lock_owner  *lock_owner = lock_state->lock_owner;
    bool                    is_v40     = (req->minorversion == 0);
    uint32_t                client_short_id;

    if (error_code != CHIMERA_VFS_OK) {
        nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                                req->thread->vfs_thread);
        req->nfs_state_ref = NULL;
        res->status        = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    lock_state->seqid += 1;
    client_short_id    = (uint32_t) lock_owner->client->client_id;
    nfs4_stateid_encode(&res->lock_stateid, lock_state->seqid,
                        NFS4_STATEID_TYPE_LOCK,
                        lock_state->shard, lock_state->slot_idx,
                        lock_state->generation, client_short_id);
    res->status = NFS4_OK;

    /* RFC 7530 §9.1.7: record cached reply for the lock_owner. */
    if (is_v40 && lock_owner) {
        pthread_mutex_lock(&lock_owner->lock);
        lock_owner->seqid = args->seqid;
        nfs4_replay_record(&lock_owner->replay, args->seqid, OP_LOCKU,
                           NFS4_OK, &res->lock_stateid);
        pthread_mutex_unlock(&lock_owner->lock);
    }

    nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                            req->thread->vfs_thread);
    req->nfs_state_ref = NULL;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_locku_complete */

void
chimera_nfs4_locku(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct LOCKU4args              *args   = &argop->oplocku;
    struct LOCKU4res               *res    = &resop->oplocku;
    struct nfs_state_table         *table  = &thread->shared->nfs4_state_table;
    bool                            is_v40 = (req->minorversion == 0);
    void                           *state_void;
    uint8_t                         state_type;
    struct nfs_lock_state          *lock_state;
    struct nfs_lock_owner          *lock_owner;
    struct chimera_vfs_open_handle *handle;
    uint64_t                        vfs_length;
    nfsstat4                        status;

    /* RFC 7530 §16.12.3: current filehandle must be set */
    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    status = nfs_state_table_acquire(table, &args->lock_stateid,
                                     NFS4_SLOT_TYPE_LOCK,
                                     &state_void, &state_type);
    if (status != NFS4_OK) {
        res->status = status;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    lock_state          = state_void;
    lock_owner          = lock_state->lock_owner;
    handle              = lock_state->handle;
    req->nfs_state_ref  = lock_state;
    req->nfs_state_type = NFS4_SLOT_TYPE_LOCK;

    if (is_v40 && lock_owner) {
        pthread_mutex_lock(&lock_owner->lock);
        int seqid_class = nfs4_owner_seqid_classify(lock_owner->seqid,
                                                    &lock_owner->replay,
                                                    args->seqid);
        if (seqid_class == NFS4_SEQID_REPLAY) {
            res->status       = lock_owner->replay.status;
            res->lock_stateid = lock_owner->replay.stateid;
            pthread_mutex_unlock(&lock_owner->lock);
            nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                                    thread->vfs_thread);
            req->nfs_state_ref = NULL;
            chimera_nfs4_compound_complete(req, NFS4_OK);
            return;
        }
        if (seqid_class != NFS4_SEQID_NEW) {
            pthread_mutex_unlock(&lock_owner->lock);
            nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                                    thread->vfs_thread);
            req->nfs_state_ref = NULL;
            res->status        = NFS4ERR_BAD_SEQID;
            chimera_nfs4_compound_complete(req, NFS4_OK);
            return;
        }
        pthread_mutex_unlock(&lock_owner->lock);
    }

    /* RFC 7530 §16.12.4: same length rules as LOCK */
    if (args->length == 0 ||
        (args->length != UINT64_MAX && args->offset > UINT64_MAX - args->length)) {
        nfs_state_table_release(table, lock_state, NFS4_SLOT_TYPE_LOCK,
                                thread->vfs_thread);
        req->nfs_state_ref = NULL;
        res->status        = NFS4ERR_INVAL;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* NFS uses UINT64_MAX to mean "to end of file"; POSIX fcntl uses 0. */
    vfs_length = args->length == UINT64_MAX ? 0 : args->length;

    chimera_vfs_lock(thread->vfs_thread, &req->cred,
                     handle,
                     SEEK_SET,
                     args->offset,
                     vfs_length,
                     CHIMERA_VFS_LOCK_UNLOCK,
                     0,
                     chimera_nfs4_locku_complete,
                     req);
} /* chimera_nfs4_locku */
