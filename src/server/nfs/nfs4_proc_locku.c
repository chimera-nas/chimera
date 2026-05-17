// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_session.h"
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
    struct nfs_request *req        = private_data;
    struct LOCKU4res   *res        = &req->res_compound.resarray[req->index].oplocku;
    struct nfs4_state  *lock_state = req->nfs4_state;

    if (error_code != CHIMERA_VFS_OK) {
        nfs4_session_release_state(req->session, lock_state);
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    lock_state->nfs4_state_id.seqid++;
    res->status       = NFS4_OK;
    res->lock_stateid = lock_state->nfs4_state_id;
    nfs4_session_release_state(req->session, lock_state);

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_locku_complete */

void
chimera_nfs4_locku(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct LOCKU4args              *args = &argop->oplocku;
    struct LOCKU4res               *res  = &resop->oplocku;
    struct nfs4_session            *session;
    struct nfs4_state              *lock_state;
    struct chimera_vfs_open_handle *handle;
    uint64_t                        vfs_length;

    /* RFC 7530 Section 16.12.3: current filehandle must be set */
    if (req->fhlen == 0) {
        res->status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    session = nfs4_resolve_session(
        req->session, req->conn,
        &thread->shared->nfs4_shared_clients,
        &args->lock_stateid);

    if (!session) {
        res->status = NFS4ERR_BAD_STATEID;
        chimera_nfs4_compound_complete(req, res->status);
        return;
    }

    req->session = session;

    if (nfs4_session_acquire_state(session,
                                   &args->lock_stateid,
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

    /* RFC 7530 Section 16.12.4: same length rules as LOCK */
    if (args->length == 0 ||
        (args->length != UINT64_MAX && args->offset > UINT64_MAX - args->length)) {
        nfs4_session_release_state(session, lock_state);
        res->status = NFS4ERR_INVAL;
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
