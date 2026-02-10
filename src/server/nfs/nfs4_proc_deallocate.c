// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_procs.h"

static void
chimera_nfs4_deallocate_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request    *req = private_data;
    struct DEALLOCATE4res *res = &req->res_compound.resarray[req->index].opdeallocate;

    if (error_code == CHIMERA_VFS_OK) {
        res->dr_status = NFS4_OK;
    } else {
        res->dr_status = chimera_nfs4_errno_to_nfsstat4(error_code);
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
    struct DEALLOCATE4args *args    = &argop->opdeallocate;
    struct nfs4_session    *session = req->session;
    struct nfs4_state      *state;

    state = nfs4_session_get_state(session, &args->da_stateid);

    chimera_vfs_allocate(thread->vfs_thread, &req->cred,
                         state->nfs4_state_handle,
                         args->da_offset,
                         args->da_length,
                         CHIMERA_VFS_ALLOCATE_DEALLOCATE,
                         0, 0,
                         chimera_nfs4_deallocate_complete,
                         req);
} /* chimera_nfs4_deallocate */
