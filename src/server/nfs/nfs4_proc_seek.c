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

    nfs_state_table_release(&req->thread->shared->nfs4_state_table,
                            req->nfs_state_ref, req->nfs_state_type,
                            req->thread->vfs_thread);
    req->nfs_state_ref = NULL;

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_seek_complete */

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

    status = nfs_state_table_acquire(table, &args->sa_stateid, 0,
                                     &state_void, &state_type);
    if (status != NFS4_OK) {
        res->sa_status = status;
        chimera_nfs4_compound_complete(req, NFS4_OK);
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
