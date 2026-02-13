// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"

static void
chimera_nfs4_deallocate_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request             *req = private_data;
    struct DEALLOCATE4res          *res = &req->res_compound.resarray[req->index].opdeallocate;
    struct chimera_vfs_open_handle *deferred;

    if (error_code == CHIMERA_VFS_OK) {
        res->dr_status = NFS4_OK;
    } else {
        res->dr_status = chimera_nfs4_errno_to_nfsstat4(error_code);
    }

    deferred = nfs4_session_release_state(req->session, req->nfs4_state);
    if (deferred) {
        chimera_vfs_release(req->thread->vfs_thread, deferred);
    }

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_deallocate_complete */

void
chimera_nfs4_deallocate(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct DEALLOCATE4args         *args    = &argop->opdeallocate;
    struct DEALLOCATE4res          *res     = &resop->opdeallocate;
    struct nfs4_session            *session = req->session;
    struct nfs4_state              *state;
    struct chimera_vfs_open_handle *state_handle;

    if (!session) {
        res->dr_status = NFS4ERR_BAD_STATEID;
        chimera_nfs4_compound_complete(req, NFS4_OK);
        return;
    }

    if (nfs4_session_acquire_state(session, &args->da_stateid,
                                   &state, &state_handle) != NFS4_OK) {
        res->dr_status = NFS4ERR_BAD_STATEID;
        chimera_nfs4_compound_complete(req, NFS4_OK);
        return;
    }

    req->nfs4_state = state;

    chimera_vfs_allocate(thread->vfs_thread, &req->cred,
                         state_handle,
                         args->da_offset,
                         args->da_length,
                         CHIMERA_VFS_ALLOCATE_DEALLOCATE,
                         0, 0,
                         chimera_nfs4_deallocate_complete,
                         req);
} /* chimera_nfs4_deallocate */
