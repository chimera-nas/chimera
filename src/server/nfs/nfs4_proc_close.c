// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_session.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
void
chimera_nfs4_close(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct CLOSE4args   *args    = &argop->opclose;
    struct CLOSE4res    *res     = &resop->opclose;
    struct nfs4_session *session = req->session;
    struct nfs4_state   *state;

    state = nfs4_session_get_state(session, &args->open_stateid);

    if (state) {

        res->open_stateid = state->nfs4_state_id;

        nfs4_session_free_slot(session, state);

        chimera_vfs_release(thread->vfs_thread, state->nfs4_state_handle);

        res->status = NFS4_OK;
    } else {
        res->status = NFS4ERR_BAD_STATEID;
    }

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_close */