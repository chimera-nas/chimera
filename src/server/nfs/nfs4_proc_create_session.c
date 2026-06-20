// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs4_procs.h"
#include "nfs4_session.h"
#include "nfs4_callback.h"
#include "nfs4_drc.h"
#include "nfs4_recovery.h"
#include "server/server.h"

/* Flag bits a client may set in csa_flags (RFC 8881 §18.36.3). */
#define NFS4_CS_VALID_FLAGS                                  \
        (CREATE_SESSION4_FLAG_PERSIST |                          \
         CREATE_SESSION4_FLAG_CONN_BACK_CHAN |                   \
         CREATE_SESSION4_FLAG_CONN_RDMA)

/* Smallest ca_maxrequestsize / ca_maxresponsesize that could ever carry a
 * request/reply (a bare SEQUENCE op plus COMPOUND framing).  Below this the
 * server returns NFS4ERR_TOOSMALL per RFC 8881 §18.36.3. */
#define NFS4_CS_MIN_CHAN_SIZE 64u

void
chimera_nfs4_create_session(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    struct chimera_server_nfs_shared *shared = thread->shared;
    struct evpl_rpc2_conn            *conn   = req->conn;
    struct CREATE_SESSION4args       *args   = &argop->opcreate_session;
    struct CREATE_SESSION4res        *res    = &resop->opcreate_session;
    struct nfs4_session              *session;
    struct nfs4_client_principal      principal;
    struct nfs4_cs_classify           cs;
    struct channel_attrs4             clamped_fore;
    uint32_t                          flags = 0;
    uint32_t                          replay_max_slots;
    uint32_t                          replay_maxresp_cached;
    uint32_t                          server_max_slots;

    /* RFC 8881 §18.36.3: outside a session CREATE_SESSION must be the only op. */
    if (!req->seen_sequence && req->args_compound->num_argarray != 1) {
        res->csr_status = NFS4ERR_NOT_ONLY_OP;
        chimera_nfs4_compound_complete(req, res->csr_status);
        return;
    }

    /* Hold off until the persistent cold-start reconstruction has finished, so
     * a reclaimed session/client is restored before the client re-creates one
     * (NFS4ERR_DELAY = retry shortly). */
    if (chimera_server_config_get_nfs4_drc(shared->config) &&
        nfs_recovery_loading(&shared->nfs4_recovery)) {
        res->csr_status = NFS4ERR_DELAY;
        chimera_nfs4_compound_complete(req, res->csr_status);
        return;
    }

    /* Undefined csa_flags bits are rejected. */
    if (args->csa_flags & ~NFS4_CS_VALID_FLAGS) {
        res->csr_status = NFS4ERR_INVAL;
        chimera_nfs4_compound_complete(req, res->csr_status);
        return;
    }

    /* ca_rdma_ird is XDR array<1>; more than one element is malformed. */
    if (args->csa_fore_chan_attrs.num_ca_rdma_ird > 1 ||
        args->csa_back_chan_attrs.num_ca_rdma_ird > 1) {
        res->csr_status = NFS4ERR_BADXDR;
        chimera_nfs4_compound_complete(req, res->csr_status);
        return;
    }

    /* A maxrequestsize/maxresponsesize too small to ever carry a
     * request/reply is NFS4ERR_TOOSMALL. */
    if (args->csa_fore_chan_attrs.ca_maxresponsesize < NFS4_CS_MIN_CHAN_SIZE ||
        args->csa_back_chan_attrs.ca_maxresponsesize < NFS4_CS_MIN_CHAN_SIZE ||
        args->csa_fore_chan_attrs.ca_maxrequestsize < NFS4_CS_MIN_CHAN_SIZE ||
        args->csa_back_chan_attrs.ca_maxrequestsize < NFS4_CS_MIN_CHAN_SIZE) {
        res->csr_status = NFS4ERR_TOOSMALL;
        chimera_nfs4_compound_complete(req, res->csr_status);
        return;
    }

    if (args->csa_flags & CREATE_SESSION4_FLAG_CONN_BACK_CHAN) {
        flags |= CREATE_SESSION4_FLAG_CONN_BACK_CHAN;
    }

    /* RFC 8881 §18.36.3: echo CREATE_SESSION4_FLAG_PERSIST only if the server
     * will honor it.  We do when nfs4_drc is enabled (the per-slot reply cache
     * is written through to the KV store and reloaded across a restart). */
    if ((args->csa_flags & CREATE_SESSION4_FLAG_PERSIST) &&
        chimera_server_config_get_nfs4_drc(shared->config)) {
        flags |= CREATE_SESSION4_FLAG_PERSIST;
    }

    /* RFC 8881 §18.36.4 sequencing + reply cache. */
    principal.flavor          = req->principal_flavor;
    principal.uid             = req->principal_uid;
    principal.gid             = req->principal_gid;
    principal.machinename     = req->principal_machinename;
    principal.machinename_len = req->principal_machinename_len;

    /* SP4_MACH_CRED: CREATE_SESSION for this client must use the bound machine
     * credential (RFC 8881 §2.10.8.3). */
    {
        struct nfs4_client *c;
        bool                wrong_cred = false;

        pthread_mutex_lock(&shared->nfs4_shared_clients.nfs4_ct_lock);
        HASH_FIND(nfs4_client_hh_by_id,
                  shared->nfs4_shared_clients.nfs4_ct_clients_by_id,
                  &args->csa_clientid, sizeof(args->csa_clientid), c);
        if (c && !nfs4_client_mach_cred_ok(c, &principal)) {
            wrong_cred = true;
        }
        pthread_mutex_unlock(&shared->nfs4_shared_clients.nfs4_ct_lock);

        if (wrong_cred) {
            res->csr_status = NFS4ERR_WRONG_CRED;
            chimera_nfs4_compound_complete(req, NFS4ERR_WRONG_CRED);
            return;
        }
    }

    nfs4_client_create_session_classify(&shared->nfs4_shared_clients,
                                        args->csa_clientid,
                                        args->csa_sequence,
                                        &principal,
                                        &cs);

    if (cs.action == NFS4_CS_ERROR) {
        res->csr_status = cs.status;
        chimera_nfs4_compound_complete(req, cs.status);
        return;
    }

    if (cs.action == NFS4_CS_REPLAY) {
        res->csr_status = cs.status;
        if (cs.status == NFS4_OK) {
            memcpy(res->csr_resok4.csr_sessionid, cs.sessionid,
                   sizeof(res->csr_resok4.csr_sessionid));
            res->csr_resok4.csr_sequence        = cs.sequence;
            res->csr_resok4.csr_flags           = cs.flags;
            res->csr_resok4.csr_fore_chan_attrs = cs.fore;
            res->csr_resok4.csr_back_chan_attrs = cs.back;
        }
        chimera_nfs4_compound_complete(req, cs.status);
        return;
    }

    /* NFS4_CS_NEW.  RFC 8881 §18.36.3: an as-yet-unconfirmed record may only be
     * confirmed by the same principal that created it (EXCHANGE_ID); a
     * different principal is a collision.  Cache the error so a retransmit
     * replays it. */
    if (!cs.confirmed && !cs.principal_ok) {
        res->csr_status = NFS4ERR_CLID_INUSE;
        nfs4_client_create_session_cache(&shared->nfs4_shared_clients,
                                         args->csa_clientid,
                                         args->csa_sequence,
                                         NFS4ERR_CLID_INUSE,
                                         NULL, 0, 0, NULL, NULL);
        chimera_nfs4_compound_complete(req, res->csr_status);
        return;
    }

    /* RFC 5661 18.36.4: the server may negotiate ca_maxrequests and
     * ca_maxresponsesize_cached downward and return the effective values
     * to the client. */
    clamped_fore = args->csa_fore_chan_attrs;

    /* Server cap on fore-channel slots is configurable (server.nfs4_session_slots);
     * fall back to the compiled-in default if it is unset/invalid. */
    server_max_slots = (uint32_t) chimera_server_config_get_nfs4_session_slots(shared->config);
    if (server_max_slots == 0) {
        server_max_slots = NFS4_MAX_REPLY_CACHE_SLOTS;
    }

    replay_max_slots = clamped_fore.ca_maxrequests;
    if (replay_max_slots == 0 ||
        replay_max_slots > server_max_slots) {
        replay_max_slots = server_max_slots;
    }
    clamped_fore.ca_maxrequests = replay_max_slots;

    replay_maxresp_cached = clamped_fore.ca_maxresponsesize_cached;
    if (replay_maxresp_cached > NFS4_MAX_CACHED_RESPONSE_SIZE) {
        replay_maxresp_cached = NFS4_MAX_CACHED_RESPONSE_SIZE;
    }
    clamped_fore.ca_maxresponsesize_cached = replay_maxresp_cached;

    session = nfs4_create_session(
        &shared->nfs4_shared_clients,
        args->csa_clientid,
        0,
        replay_max_slots,
        replay_maxresp_cached,
        &clamped_fore,
        &args->csa_back_chan_attrs,
        NULL);

    if (!session) {
        res->csr_status = NFS4ERR_STALE_CLIENTID;
        chimera_nfs4_compound_complete(req, NFS4ERR_STALE_CLIENTID);
        return;
    }

    /* RFC 8881 §18.36.3: a successful CREATE_SESSION confirms the client
     * record.  If this record superseded an earlier one (client reboot, see
     * nfs4_client_exchange_id), confirmation tears the old record and its
     * sessions down; release its state hierarchy outside the table lock. */
    {
        struct nfs_client *superseded = NULL;

        nfs4_client_confirm(&shared->nfs4_shared_clients,
                            args->csa_clientid, &superseded);

        if (superseded) {
            nfs_client_destroy(superseded,
                               &shared->nfs4_state_table,
                               thread->vfs_thread,
                               false);
        }
    }

    nfs4_session_bind_conn(conn, session);
    req->session                  = session;
    session->nfs4_session_persist = (flags & CREATE_SESSION4_FLAG_PERSIST) != 0;

    /* Persist this session's metadata so a restart can reconstruct it with the
     * same sessionid (and its owning client), letting a persistent client's
     * retransmits replay from the reloaded reply cache. */
    if (session->nfs4_session_persist) {
        nfs4_drc_persist_session(thread->vfs_thread, session, &principal,
                                 args->csa_cb_program, flags);
    }

    /* RFC 8881 §18.36: when the client asks to use this connection as the
     * backchannel, record it together with the callback program number so the
     * server can deliver CB_RECALL for delegations over the fore conn.  Also
     * stamp the callback path on the unified client.  The backchannel carries
     * the session's own minorversion: a 4.2 client rejects a CB_COMPOUND
     * tagged minorversion 1 with NFS4ERR_MINOR_VERS_MISMATCH.  No separate
     * netid/addr -- the backchannel is the fore conn. */
    if (args->csa_flags & CREATE_SESSION4_FLAG_CONN_BACK_CHAN) {
        session->nfs4_session_cb_program       = args->csa_cb_program;
        session->nfs4_session_backchannel_conn = conn;
        /* This compound runs on conn's owner thread; record it so cross-thread
         * callbacks (CB_RECALL / CB_LAYOUTRECALL) marshal their send here. */
        session->nfs4_session_backchannel_owner = thread;

        nfs4_client_set_cb_path(&shared->nfs4_shared_clients,
                                args->csa_clientid,
                                args->csa_cb_program,
                                0,
                                req->minorversion,
                                NULL, 0, NULL, 0);

        /* Record the callback auth the client wants the server to use
         * (RFC 8881 §18.36.3 csa_sec_parms).  Use the first parameter; for
         * AUTH_SYS carry its uid/gid onto the callback path. */
        if (args->num_csa_sec_parms > 0) {
            struct callback_sec_parms4 *sp = &args->csa_sec_parms[0];
            uint32_t                    uid = 0, gid = 0;

            if (sp->cb_secflavor == AUTH_SYS) {
                uid = sp->cbsp_sys_cred.uid;
                gid = sp->cbsp_sys_cred.gid;
            }
            nfs4_client_set_cb_sec(&shared->nfs4_shared_clients,
                                   args->csa_clientid, 0,
                                   sp->cb_secflavor, uid, gid);
        }

        /* If this client destroyed a session while a delegation recall was
         * still outstanding, re-drive that recall over the freshly-bound
         * backchannel (RFC 8881 §20.4.1; pynfs DSESS9003).  No-op unless a
         * recall is actually pending. */
        nfs4_cb_resend_recalls_on_rebind(thread, session->client_unified, req);
    }

    res->csr_status = NFS4_OK;
    memcpy(res->csr_resok4.csr_sessionid, session->nfs4_session_id, sizeof(res->csr_resok4.csr_sessionid));
    /* RFC 8881 §18.36.4: csr_sequence MUST equal the request's csa_sequence. */
    res->csr_resok4.csr_sequence = args->csa_sequence;
    res->csr_resok4.csr_flags    = flags;

    res->csr_resok4.csr_fore_chan_attrs = session->nfs4_session_fore_attrs;
    res->csr_resok4.csr_back_chan_attrs = session->nfs4_session_back_attrs;

    /* Cache the reply for retransmit detection (RFC 8881 §18.36.4). */
    nfs4_client_create_session_cache(&shared->nfs4_shared_clients,
                                     args->csa_clientid,
                                     args->csa_sequence,
                                     NFS4_OK,
                                     session->nfs4_session_id,
                                     args->csa_sequence,
                                     flags,
                                     &session->nfs4_session_fore_attrs,
                                     &session->nfs4_session_back_attrs);

    /* Drop the +1 ref returned by nfs4_create_session; the conn now holds
     * its own ref via bind_conn, and the hash holds the other ref. */
    nfs4_session_put(session);

    chimera_nfs4_compound_complete(req, NFS4_OK);
} /* chimera_nfs4_setclientid */