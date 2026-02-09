// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "vfs/vfs_procs.h"

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

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_seek_complete */

void
chimera_nfs4_seek(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct SEEK4args    *args    = &argop->opseek;
    struct nfs4_session *session = req->session;
    struct nfs4_state   *state;

    state = nfs4_session_get_state(session, &args->sa_stateid);

    chimera_vfs_seek(thread->vfs_thread, &req->cred,
                     state->nfs4_state_handle,
                     args->sa_offset,
                     args->sa_what,
                     chimera_nfs4_seek_complete,
                     req);
} /* chimera_nfs4_seek */
