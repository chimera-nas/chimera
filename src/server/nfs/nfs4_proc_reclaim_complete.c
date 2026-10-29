// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_recovery.h"
#include "nfs4_drc.h"
#include "evpl/evpl_rpc2.h"

/* RFC 8881 §18.51.4: a second *global* RECLAIM_COMPLETE for the same client is
 * NFS4ERR_COMPLETE_ALREADY.  The first retires the client's recovery record,
 * which may end the grace window early once every client has reclaimed. */
static nfsstat4
chimera_nfs4_reclaim_complete_mark(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    if (nfs4_client_mark_reclaim_complete(&thread->shared->nfs4_shared_clients,
                                          req->session->nfs4_session_clientid)) {
        return NFS4ERR_COMPLETE_ALREADY;
    }
    nfs_recovery_reclaim_complete(&thread->shared->nfs4_recovery,
                                  req->session->client_unified);
    return NFS4_OK;
} /* chimera_nfs4_reclaim_complete_mark */

static void
chimera_nfs4_reclaim_complete_persisted(
    bool  ok,
    void *arg)
{
    struct nfs_request          *req = arg;
    struct RECLAIM_COMPLETE4res *res =
        &req->res_compound.resarray[req->index].opreclaim_complete;

    /* The bit only spares a client that later fails over a second
     * RECLAIM_COMPLETE, which RESTART_RECLAIM_NEEDED asks for anyway.  Holding
     * it with NFS4ERR_DELAY instead would keep the per-client OPEN gate shut
     * for as long as the store is failing. */
    if (!ok) {
        chimera_nfs_error("RECLAIM_COMPLETE: could not record it on the session "
                          "record of clientid %lx; marking it complete here only",
                          (unsigned long) req->session->nfs4_session_clientid);
    }
    res->rcr_status = chimera_nfs4_reclaim_complete_mark(req->thread, req);
    chimera_nfs4_compound_complete(req, res->rcr_status);
} /* chimera_nfs4_reclaim_complete_persisted */

void
chimera_nfs4_reclaim_complete(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct RECLAIM_COMPLETE4args *args = &argop->opreclaim_complete;
    struct RECLAIM_COMPLETE4res  *res  = &resop->opreclaim_complete;

    /* RFC 8881 §18.51.3: the per-filesystem form names its file system by the
     * current filehandle, so rca_one_fs == TRUE with none established is
     * NFS4ERR_NOFILEHANDLE.  The global form takes no filehandle at all. */
    if (args->rca_one_fs && req->fhlen == 0) {
        res->rcr_status = NFS4ERR_NOFILEHANDLE;
        chimera_nfs4_compound_complete(req, res->rcr_status);
        return;
    }

    /* RFC 8881 §18.51.3: rca_one_fs == TRUE signals the client finished
     * reclaim for a SINGLE file system after migration -- it is NOT the global
     * end-of-reclaim and must not retire the client's whole reclaim obligation
     * or decrement the server-wide pending_reclaim counter (which would end the
     * grace window early for everyone).  Chimera does not track per-fs reclaim
     * (no fs_locations/migration yet), so the per-fs variant is a no-op. */
    if (args->rca_one_fs || !req->session) {
        res->rcr_status = NFS4_OK;
        chimera_nfs4_compound_complete(req, NFS4_OK);
        return;
    }

    /* On a persisted session, record the completion on the session record
     * before marking it or answering: a node the client later fails over to
     * then knows not to hold its non-reclaim OPENs back for a second one
     * (nfs4_client_recovered). */
    if (req->session->nfs4_session_persist &&
        !thread->shared->nfs4_recovery.persistence_disabled) {
        if (nfs4_client_reclaim_complete(&thread->shared->nfs4_shared_clients,
                                         req->session->nfs4_session_clientid)) {
            res->rcr_status = NFS4ERR_COMPLETE_ALREADY;
            chimera_nfs4_compound_complete(req, res->rcr_status);
            return;
        }
        nfs4_drc_persist_reclaim_complete(thread->vfs_thread, req->session,
                                          chimera_nfs4_reclaim_complete_persisted,
                                          req);
        return;
    }

    res->rcr_status = chimera_nfs4_reclaim_complete_mark(thread, req);
    chimera_nfs4_compound_complete(req, res->rcr_status);
} /* chimera_nfs4_reclaim_complete */
