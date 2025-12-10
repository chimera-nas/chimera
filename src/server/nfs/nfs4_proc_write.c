// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_procs.h"

static void
chimera_nfs4_write_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct nfs_request *req = private_data;
    struct WRITE4res   *res = &req->res_compound.resarray[req->index].opwrite;

    if (error_code == CHIMERA_VFS_OK) {
        res->status           = NFS4_OK;
        res->resok4.count     = length;
        res->resok4.committed = sync ? FILE_SYNC4 : UNSTABLE4;

        memcpy(res->resok4.writeverf,
               &req->thread->shared->nfs_verifier,
               sizeof(res->resok4.writeverf));
    } else {
        res->status = chimera_nfs4_errno_to_nfsstat4(error_code);
    }

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs3_write_complete */

void
chimera_nfs4_write(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct WRITE4args   *args    = &argop->opwrite;
    struct nfs4_session *session = req->session;
    struct nfs4_state   *state;

    state = nfs4_session_get_state(session, &args->stateid);

    chimera_vfs_write(thread->vfs_thread,
                      state->nfs4_state_handle,
                      args->offset,
                      args->data.length,
                      (args->stable != UNSTABLE4),
                      0,
                      0,
                      args->data.iov,
                      args->data.niov,
                      chimera_nfs4_write_complete,
                      req);
} /* chimera_nfs4_write */
