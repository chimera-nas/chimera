// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_state.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

static void
chimera_nfs4_seek_complete(
    enum chimera_vfs_error error_code,
    int                    sr_eof,
    uint64_t               sr_offset,
    void                  *private_data)
{
    struct nfs_request *req = private_data;
    struct SEEK4res    *res = &req->res_compound.resarray[req->index].opseek;

    if (error_code == CHIMERA_VFS_OK) {
        res->sa_status        = NFS4_OK;
        res->resok4.sr_eof    = sr_eof;
        res->resok4.sr_offset = sr_offset;
    } else {
        res->sa_status = chimera_nfs4_errno_to_nfsstat4(error_code);
    }

    if (req->nfs_state_ref) {
        nfs_state_table_release(&req->thread->shared->nfs4_state_table,
                                req->nfs_state_ref, req->nfs_state_type,
                                req->thread->vfs_thread);
        req->nfs_state_ref = NULL;
    } else if (req->handle) {
        /* Special stateid: release the on-the-fly handle we opened. */
        chimera_vfs_release(req->thread->vfs_thread, req->handle);
        req->handle = NULL;
    }

    chimera_nfs4_compound_complete(req, res->sa_status);
} /* chimera_nfs4_seek_complete */

static void
chimera_nfs4_seek_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct nfs_request *req  = private_data;
    struct SEEK4args   *args = &req->args_compound->argarray[req->index].opseek;
    struct SEEK4res    *res  = &req->res_compound.resarray[req->index].opseek;

    if (error_code != CHIMERA_VFS_OK) {
        res->sa_status = chimera_nfs4_errno_to_nfsstat4(error_code);
        chimera_nfs4_compound_complete(req, res->sa_status);
        return;
    }

    req->handle = handle;

    chimera_vfs_seek(req->thread->vfs_thread, &req->cred,
                     handle,
                     args->sa_offset,
                     args->sa_what,
                     chimera_nfs4_seek_complete,
                     req);
} /* chimera_nfs4_seek_open_callback */

void
chimera_nfs4_seek(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct SEEK4args               *args  = &argop->opseek;
    struct SEEK4res                *res   = &resop->opseek;
    struct nfs_state_table         *table = &thread->shared->nfs4_state_table;
    void                           *state_void;
    uint8_t                         state_type;
    struct chimera_vfs_open_handle *state_handle;
    nfsstat4                        status;

    req->nfs_state_ref = NULL;
    req->handle        = NULL;

    /* NFS4.1 current-stateid substitution (RFC 8881 §16.2.3.1.2). */
    chimera_nfs4_resolve_current_stateid(req, &args->sa_stateid);

    /*
     * RFC 8881 §8.2.3 requires SEEK to honor the special stateids.  These carry
     * no state-table entry, so open the current FH on the fly instead of
     * consulting the state table.
     */
    if (nfs4_stateid_is_special(&args->sa_stateid)) {
        if (req->fhlen == 0) {
            res->sa_status = NFS4ERR_NOFILEHANDLE;
            chimera_nfs4_compound_complete(req, res->sa_status);
            return;
        }

        chimera_vfs_open_fh(thread->vfs_thread, &req->cred, NULL,
                            req->fh,
                            req->fhlen,
                            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_READ_ONLY,
                            chimera_nfs4_seek_open_callback,
                            req);
        return;
    }

    status = nfs_state_table_acquire(table, &args->sa_stateid, 0,
                                     &state_void, &state_type);
    if (status != NFS4_OK) {
        res->sa_status = status;
        chimera_nfs4_compound_complete(req, res->sa_status);
        return;
    }

    if (state_type == NFS4_SLOT_TYPE_OPEN) {
        state_handle = ((struct nfs_open_state *) state_void)->handle;
    } else {
        state_handle = ((struct nfs_lock_state *) state_void)->handle;
    }

    req->nfs_state_ref  = state_void;
    req->nfs_state_type = state_type;

    chimera_vfs_seek(thread->vfs_thread, &req->cred,
                     state_handle,
                     args->sa_offset,
                     args->sa_what,
                     chimera_nfs4_seek_complete,
                     req);
} /* chimera_nfs4_seek */
