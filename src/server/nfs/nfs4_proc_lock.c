// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_session.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

static void
chimera_nfs4_lock_complete(
    enum chimera_vfs_error error_code,
    uint32_t               conflict_type,
    uint64_t               conflict_offset,
    uint64_t               conflict_length,
    pid_t                  conflict_pid,
    void                  *private_data)
{
    struct nfs_request             *req        = private_data;
    struct LOCK4args               *args       = &req->args_compound->argarray[req->index].oplock;
    struct LOCK4res                *res        = &req->res_compound.resarray[req->index].oplock;
    struct nfs4_session            *session    = req->session;
    struct nfs4_state              *lock_state = req->nfs4_state;
    struct chimera_vfs_open_handle *unused;

    if (error_code == CHIMERA_VFS_OK) {
        /* RFC 7530 Section 9.1.3: seqid starts at 1 for a new lock stateid;
         * only increment when modifying an existing lock state. */
        if (!args->locker.new_lock_owner) {
            lock_state->nfs4_state_id.seqid++;
            nfs4_session_release_state(session, lock_state);
        }
        res->status              = NFS4_OK;
        res->resok4.lock_stateid = lock_state->nfs4_state_id;
        chimera_nfs4_compound_complete(req, NFS4_OK);
        return;
    }

    /* Lock conflict - free the slot if we just allocated it */
    if (error_code == CHIMERA_VFS_EACCES || error_code == CHIMERA_VFS_EAGAIN) {
        if (args->locker.new_lock_owner) {
            lock_state->nfs4_state_handle = NULL;
            nfs4_session_free_slot(session, lock_state, &unused);
        } else {
            nfs4_session_release_state(session, lock_state);
        }
        res->status          = NFS4ERR_DENIED;
        res->denied.offset   = conflict_offset;
        res->denied.length   = conflict_length;
        res->denied.locktype = (conflict_type == CHIMERA_VFS_LOCK_READ) ?
            READ_LT : WRITE_LT;
        /* The VFS layer does not expose the conflicting lock's NFS owner;
         * return a zeroed owner rather than the requester's clientid. */
        res->denied.owner.clientid   = 0;
        res->denied.owner.owner.len  = 0;
        res->denied.owner.owner.data = NULL;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* Other error - clean up allocated or acquired state */
    if (args->locker.new_lock_owner) {
        lock_state->nfs4_state_handle = NULL;
        nfs4_session_free_slot(session, lock_state, &unused);
    } else {
        nfs4_session_release_state(session, lock_state);
    }

    res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
    chimera_nfs4_compound_complete(req, res->status);
} /* chimera_nfs4_lock_complete */

void
chimera_nfs4_lock(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct LOCK4args               *args = &argop->oplock;
    struct LOCK4res                *res  = &resop->oplock;
    struct nfs4_session            *session;
    struct nfs4_state              *open_state;
    struct nfs4_state              *lock_state;
    struct chimera_vfs_open_handle *handle;
    struct chimera_vfs_open_handle *unused;
    uint32_t                        lock_type;

    /* RFC 7530 Section 16.10.3: current filehandle must be set */
    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    if (args->locktype == READ_LT || args->locktype == READW_LT) {
        lock_type = CHIMERA_VFS_LOCK_READ;
    } else {
        lock_type = CHIMERA_VFS_LOCK_WRITE;
    }

    if (args->locker.new_lock_owner) {
        /* First lock for this open - validate open stateid, allocate lock state */
        session = nfs4_resolve_session(
            req->session, req->conn,
            &thread->shared->nfs4_shared_clients,
            &args->locker.open_owner.open_stateid);

        if (!session) {
            res->status = NFS4ERR_BAD_STATEID;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }

        req->session = session;

        if (nfs4_session_acquire_state(session,
                                       &args->locker.open_owner.open_stateid,
                                       &open_state,
                                       &handle) != NFS4_OK) {
            res->status = NFS4ERR_BAD_STATEID;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }

        if (open_state->nfs4_state_type != NFS4_STATE_TYPE_OPEN) {
            nfs4_session_release_state(session, open_state);
            res->status = NFS4ERR_BAD_STATEID;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }

        /* Allocate a lock state that borrows the open handle */
        lock_state                         = nfs4_session_alloc_slot(session);
        lock_state->nfs4_state_type        = NFS4_STATE_TYPE_LOCK;
        lock_state->nfs4_state_handle      = handle;
        lock_state->nfs4_state_parent_slot = *(uint32_t *) open_state->nfs4_state_id.other;

        req->nfs4_state = lock_state;

        /* Release the open state acquire ref - handle stays alive while open is active */
        nfs4_session_release_state(session, open_state);

    } else {
        /* Subsequent lock - use existing lock stateid */
        session = nfs4_resolve_session(
            req->session, req->conn,
            &thread->shared->nfs4_shared_clients,
            &args->locker.lock_owner.lock_stateid);

        if (!session) {
            res->status = NFS4ERR_BAD_STATEID;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }

        req->session = session;

        if (nfs4_session_acquire_state(session,
                                       &args->locker.lock_owner.lock_stateid,
                                       &lock_state,
                                       &handle) != NFS4_OK) {
            res->status = NFS4ERR_BAD_STATEID;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }

        if (lock_state->nfs4_state_type != NFS4_STATE_TYPE_LOCK) {
            nfs4_session_release_state(session, lock_state);
            res->status = NFS4ERR_BAD_STATEID;
            chimera_nfs4_compound_complete(req, res->status);
            return;
        }

        req->nfs4_state = lock_state;
    }

    /* RFC 7530 Section 16.10.4: length must not be zero; if not the all-ones
     * "to-EOF" sentinel, offset+length must not exceed UINT64_MAX. */
    if (args->length == 0 ||
        (args->length != UINT64_MAX && args->offset > UINT64_MAX - args->length)) {
        if (args->locker.new_lock_owner) {
            lock_state->nfs4_state_handle = NULL;
            nfs4_session_free_slot(session, lock_state, &unused);
        } else {
            nfs4_session_release_state(session, lock_state);
        }
        res->status = NFS4ERR_INVAL;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    /* NFS uses UINT64_MAX to mean "to end of file"; POSIX fcntl uses 0. */
    chimera_vfs_lock(thread->vfs_thread, &req->cred,
                     handle,
                     SEEK_SET,
                     args->offset,
                     args->length == UINT64_MAX ? 0 : args->length,
                     lock_type,
                     0,
                     chimera_nfs4_lock_complete,
                     req);
} /* chimera_nfs4_lock */
