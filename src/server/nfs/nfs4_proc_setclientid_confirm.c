// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * RFC 7530 §16.34: SETCLIENTID_CONFIRM
 *
 * Confirms the unconfirmed client record named by {clientid, verifier} that a
 * prior SETCLIENTID created.  On success the clientid becomes usable, so the
 * implicit NFSv4.0 session (how non-SEQUENCE ops resolve the client) is
 * created and bound to the connection here -- not at SETCLIENTID, so that an
 * unconfirmed clientid cannot be used.  A client reboot purges the superseded
 * record's state inside nfs4_client_setclientid_confirm.
 */

#include <string.h>

#include "nfs4_procs.h"
#include "nfs4_recovery.h"
#include "nfs4_session.h"
#include "nfs4_state.h"

void
chimera_nfs4_setclientid_confirm(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct SETCLIENTID_CONFIRM4args  *args            = &argop->opsetclientid_confirm;
    struct chimera_server_nfs_shared *shared          = thread->shared;
    struct nfs_client                *destroy_unified = NULL;
    struct nfs4_session              *session;
    nfsstat4                          status;

    status = nfs4_client_setclientid_confirm(&shared->nfs4_shared_clients,
                                             args->clientid,
                                             args->setclientid_confirm,
                                             &destroy_unified);

    /* A superseded (rebooted) client's state hierarchy is torn down outside
     * the table lock. */
    if (destroy_unified) {
        nfs_client_destroy(destroy_unified,
                           &shared->nfs4_state_table,
                           thread->vfs_thread);
    }

    if (status != NFS4_OK) {
        resop->opsetclientid_confirm.status = status;
        chimera_nfs4_compound_complete(req, status);
        return;
    }

    /* The clientid is now confirmed and usable.  Ensure it has an implicit
     * (NFS4.0) session and bind it to this connection so subsequent
     * non-SEQUENCE operations resolve the client. */
    session = nfs4_session_find_by_clientid(&shared->nfs4_shared_clients,
                                            args->clientid);
    if (!session) {
        session = nfs4_create_session(&shared->nfs4_shared_clients,
                                      args->clientid, 1, 0, 0, NULL, NULL);
    }

    if (session) {
        struct nfs_client *uc = session->client_unified;

        nfs4_session_bind_conn(req->conn, session);
        req->session = session;
        nfs4_session_put(session);

        if (uc) {
            uc->confirmed = 1;
            nfs_recovery_persist(&thread->shared->nfs4_recovery, uc);
        }
    }

    resop->opsetclientid_confirm.status = NFS4_OK;
    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_setclientid_confirm */
