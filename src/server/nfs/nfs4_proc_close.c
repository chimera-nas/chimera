// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
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
    struct CLOSE4args              *args    = &argop->opclose;
    struct CLOSE4res               *res     = &resop->opclose;
    struct nfs4_session            *session = nfs4_resolve_session(
        req->session, &args->open_stateid,
        &thread->shared->nfs4_shared_clients);
    struct nfs4_state              *state;
    struct chimera_vfs_open_handle *handle;

    if (!session) {
        res->status = NFS4ERR_BAD_STATEID;
        chimera_nfs4_compound_complete(req, NFS4_OK);
        return;
    }

    if (!req->session) {
        req->session = session;
        evpl_rpc2_conn_set_private_data(req->conn, session);
    }

    state = nfs4_session_get_state(session, &args->open_stateid);

    handle = nfs4_session_free_slot(session, state);

    if (handle) {
        res->open_stateid = state->nfs4_state_id;
        chimera_vfs_release(thread->vfs_thread, handle);
        res->status = NFS4_OK;
    } else {
        res->status = NFS4ERR_BAD_STATEID;
    }

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_close */
