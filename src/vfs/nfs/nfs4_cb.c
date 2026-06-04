// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * NFSv4.1 client back channel (callback channel) -- the RECEIVING side.
 *
 * In NFSv4.1 the back channel rides the fore-channel connection: after
 * CREATE_SESSION binds it (csa_flags |= CONN_BACK_CHAN, csa_cb_program set), the
 * server sends CB_COMPOUND RPC *calls* to the client over that connection.  The
 * client must serve them: it registers the NFS_V4_CB program on the connection
 * so rpc2 dispatches incoming CB_COMPOUND to chimera_nfs4_cb_compound(), which
 * processes CB_SEQUENCE (back-channel slot/seqid) and the recall ops and replies.
 *
 * Because the back channel must live on a connection the server can keep
 * reaching, the session that carries it is established on a dedicated long-lived
 * control connection (see nfs4_cb_control_*), not the transient mount connection.
 *
 * The only recall this client cares about today is CB_LAYOUTRECALL (pNFS).  On
 * main no layouts are held, so it answers NFS4ERR_NOMATCHING_LAYOUT; the pNFS
 * client overrides chimera_nfs4_cb_layoutrecall() to find and return the layout.
 */

#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <time.h>

#include "nfs_internal.h"
#include "common/macros.h"

/*
 * Locate the session a CB_SEQUENCE refers to by its sessionid.  The client keeps
 * one session per server; iterate the (small) server table.  Returns the owning
 * server, or NULL if the sessionid is unknown (-> NFS4ERR_BADSESSION).
 */
static struct chimera_nfs_client_server *
chimera_nfs4_cb_find_server(
    struct chimera_nfs_shared *shared,
    const uint8_t             *sessionid)
{
    struct chimera_nfs_client_server   *server;
    struct chimera_nfs4_client_session *session;
    int                                 i;

    pthread_mutex_lock(&shared->lock);
    for (i = 0; i < shared->max_servers; i++) {
        server  = shared->servers[i];
        session = server ? server->nfs4_session : NULL;
        if (session &&
            memcmp(session->sessionid, sessionid, NFS4_SESSIONID_SIZE) == 0) {
            pthread_mutex_unlock(&shared->lock);
            return server;
        }
    }
    pthread_mutex_unlock(&shared->lock);
    return NULL;
} /* chimera_nfs4_cb_find_server */

/*
 * Handle a CB_LAYOUTRECALL.  Weak default: the client holds no layouts, so
 * report NFS4ERR_NOMATCHING_LAYOUT and let the server stop tracking it.  The
 * pNFS client provides a strong definition that finds the matching layout,
 * fences further DS I/O, and returns it via LAYOUTRETURN.
 */
nfsstat4 __attribute__((weak))
chimera_nfs4_cb_layoutrecall(
    struct chimera_nfs_shared        *shared,
    struct chimera_nfs_client_server *server,
    struct CB_LAYOUTRECALL4args      *args)
{
    (void) shared;
    (void) server;
    (void) args;
    return NFS4ERR_NOMATCHING_LAYOUT;
} /* chimera_nfs4_cb_layoutrecall */

/*
 * recv_call_CB_COMPOUND: rpc2 dispatches an incoming CB_COMPOUND here (on the
 * control thread that owns the back-channel connection).  Process CB_SEQUENCE
 * first (RFC 8881 §20.9 -- it MUST be the first op), then each recall op, build
 * the result array, and reply.
 */
void
chimera_nfs4_cb_compound(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_cred     *cred,
    struct CB_COMPOUND4args   *args,
    struct evpl_rpc2_encoding *encoding,
    void                      *private_data)
{
    struct chimera_nfs_shared        *shared = private_data;
    struct chimera_nfs_client_server *server = NULL;
    struct CB_COMPOUND4res            res;
    struct nfs_cb_resop4             *resarray;
    nfsstat4                          compound_status = NFS4_OK;
    uint32_t                          i;
    int                               rc;

    (void) conn;
    (void) cred;

    memset(&res, 0, sizeof(res));
    res.tag.len  = 0;
    res.tag.data = NULL;

    if (args->num_argarray == 0) {
        res.status       = NFS4_OK;
        res.num_resarray = 0;
        res.resarray     = NULL;
        rc               = shared->nfs_v4_cb.send_reply_CB_COMPOUND(evpl, NULL, &res, encoding);
        (void) rc;
        return;
    }

    resarray = xdr_dbuf_alloc_space(sizeof(*resarray) * args->num_argarray,
                                    encoding->dbuf);
    if (unlikely(resarray == NULL)) {
        res.status       = NFS4ERR_RESOURCE;
        res.num_resarray = 0;
        res.resarray     = NULL;
        rc               = shared->nfs_v4_cb.send_reply_CB_COMPOUND(evpl, NULL, &res, encoding);
        (void) rc;
        return;
    }

    for (i = 0; i < args->num_argarray; i++) {
        struct nfs_cb_argop4 *argop = &args->argarray[i];
        struct nfs_cb_resop4 *resop = &resarray[i];

        resop->resop = argop->argop;

        /* CB_SEQUENCE must be the first operation in a 4.1 CB_COMPOUND. */
        if (i == 0) {
            if (argop->argop != OP_CB_SEQUENCE) {
                resop->resop                   = OP_CB_SEQUENCE;
                resop->opcbsequence.csr_status = NFS4ERR_OP_NOT_IN_SESSION;
                compound_status                = NFS4ERR_OP_NOT_IN_SESSION;
                res.num_resarray               = 1;
                break;
            }

            server = chimera_nfs4_cb_find_server(shared,
                                                 argop->opcbsequence.csa_sessionid);
            if (!server) {
                resop->opcbsequence.csr_status = NFS4ERR_BADSESSION;
                compound_status                = NFS4ERR_BADSESSION;
                res.num_resarray               = 1;
                break;
            }

            /* Accept the server-driven back-channel slot.  We run a single slot
             * (slotid 0) with no reply cache, echoing the server's sequenceid;
             * the server drives the seqid and rarely retransmits. */
            memcpy(resop->opcbsequence.csr_resok4.csr_sessionid,
                   argop->opcbsequence.csa_sessionid, NFS4_SESSIONID_SIZE);
            resop->opcbsequence.csr_resok4.csr_sequenceid            = argop->opcbsequence.csa_sequenceid;
            resop->opcbsequence.csr_resok4.csr_slotid                = argop->opcbsequence.csa_slotid;
            resop->opcbsequence.csr_resok4.csr_highest_slotid        = 0;
            resop->opcbsequence.csr_resok4.csr_target_highest_slotid = 0;
            resop->opcbsequence.csr_status                           = NFS4_OK;
            continue;
        }

        switch (argop->argop) {
            case OP_CB_LAYOUTRECALL:
                resop->opcblayoutrecall.clorr_status =
                    chimera_nfs4_cb_layoutrecall(shared, server, &argop->opcblayoutrecall);
                if (resop->opcblayoutrecall.clorr_status != NFS4_OK) {
                    compound_status = resop->opcblayoutrecall.clorr_status;
                }
                break;

            case OP_CB_RECALL:
                /* This client takes no delegations, so it never holds one. */
                resop->opcbrecall.status = NFS4ERR_BADHANDLE;
                compound_status          = NFS4ERR_BADHANDLE;
                break;

            default:
                /* Other callbacks (notify, recall_any, ...) are not supported. */
                resop->resop              = OP_CB_ILLEGAL;
                resop->opcbillegal.status = NFS4ERR_NOTSUPP;
                compound_status           = NFS4ERR_NOTSUPP;
                break;
        } /* switch */

        /* RFC 8881 §20: stop at the first failed op. */
        if (compound_status != NFS4_OK) {
            res.num_resarray = i + 1;
            break;
        }

        res.num_resarray = i + 1;
    }

    res.status   = compound_status;
    res.resarray = resarray;

    rc = shared->nfs_v4_cb.send_reply_CB_COMPOUND(evpl, NULL, &res, encoding);
    (void) rc;
} /* chimera_nfs4_cb_compound */

/*
 * ===========================================================================
 * Back-channel control thread
 * ===========================================================================
 *
 * The server pins a session's back channel to the connection CREATE_SESSION was
 * sent on (it never re-binds on SEQUENCE), and drops it when that connection
 * dies.  The mount runs on a transient evpl thread, so the session must instead
 * be created on a long-lived connection.  A single dedicated control thread owns
 * one persistent connection per server (server->cb_conn): it runs EXCHANGE_ID +
 * CREATE_SESSION (csa_flags |= CONN_BACK_CHAN, csa_cb_program = the NFS_V4_CB
 * program) on it, publishes server->nfs4_session, and -- because cb_conn
 * registers the NFS_V4_CB server program -- serves the resulting CB_COMPOUND
 * traffic.  Data threads bind their own connections to the same session via
 * bind-on-SEQUENCE, so only establishment is cross-thread.
 *
 * Mount threads hand a request off via chimera_nfs4_cb_establish_session():
 * push a work item on shared->cb_establish_queue and ring shared->cb_doorbell.
 * When the control thread finishes (or fails) it rings the item's own resume
 * doorbell, registered on the originating evpl, which resumes the mount.
 */

/* Forward declarations for the control-thread establishment chain. */
static void chimera_nfs4_cb_exchange_id(
    struct chimera_nfs4_cb_establish *item);
static void chimera_nfs4_cb_create_session(
    struct chimera_nfs4_cb_establish *item);

/* Signal the originating thread that establishment finished (status 0) or
 * failed (errno).  After this the control thread must not touch the item; the
 * originating thread owns and frees it. */
static void
chimera_nfs4_cb_establish_done(
    struct chimera_nfs4_cb_establish *item,
    int                               status)
{
    item->status = status;
    evpl_ring_doorbell(&item->resume_db);
} /* chimera_nfs4_cb_establish_done */

/*
 * CREATE_SESSION reply (control thread).  Record the granted sessionid + slot
 * count, publish the session onto the server (under shared->lock so data
 * threads and the CB receive path observe a fully-formed session), and resume
 * the mount.
 */
static void
chimera_nfs4_cb_create_session_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_nfs4_cb_establish   *item    = private_data;
    struct chimera_nfs_client_server   *server  = item->server_thread->server;
    struct chimera_nfs_shared          *shared  = server->shared;
    struct chimera_nfs4_client_session *session = item->session;
    struct CREATE_SESSION4resok        *cs_res;

    (void) evpl;
    (void) verf;

    if (status != 0 || res->status != NFS4_OK ||
        res->num_resarray < 1 ||
        res->resarray[0].opcreate_session.csr_status != NFS4_OK) {
        chimera_nfsclient_error("NFS4 back-channel CREATE_SESSION failed (rpc=%d compound=%d)",
                                status, res ? res->status : -1);
        pthread_mutex_destroy(&session->lock);
        free(session);
        item->session = NULL;
        chimera_nfs4_cb_establish_done(item, EIO);
        return;
    }

    cs_res = &res->resarray[0].opcreate_session.csr_resok4;

    memcpy(session->sessionid, cs_res->csr_sessionid, NFS4_SESSIONID_SIZE);
    session->max_slots      = cs_res->csr_fore_chan_attrs.ca_maxrequests;
    session->next_unclaimed = 0;
    session->overflow_rr    = 0;

    pthread_mutex_lock(&shared->lock);
    server->nfs4_session = session;
    pthread_mutex_unlock(&shared->lock);

    chimera_nfsclient_info(
        "NFS4 back-channel session established, clientid=%lu max_slots=%u",
        (unsigned long) session->clientid, session->max_slots);

    chimera_nfs4_cb_establish_done(item, 0);
} /* chimera_nfs4_cb_create_session_callback */

/*
 * Send CREATE_SESSION on the control connection (control thread).  Requests the
 * back channel be bound to this connection (CONN_BACK_CHAN) with the client's
 * NFS_V4_CB program number, so the server delivers CB_COMPOUND here.
 */
static void
chimera_nfs4_cb_create_session(struct chimera_nfs4_cb_establish *item)
{
    struct chimera_nfs_client_server   *server  = item->server_thread->server;
    struct chimera_nfs_shared          *shared  = server->shared;
    struct chimera_nfs4_client_session *session = item->session;
    struct COMPOUND4args                args;
    struct nfs_argop4                   argarray[1];
    struct CREATE_SESSION4args         *cs_args;

    memset(&args, 0, sizeof(args));
    args.tag.len      = 0;
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 1;

    argarray[0].argop = OP_CREATE_SESSION;
    cs_args           = &argarray[0].opcreate_session;

    cs_args->csa_clientid = session->clientid;
    cs_args->csa_sequence = 1;
    cs_args->csa_flags    = CREATE_SESSION4_FLAG_CONN_BACK_CHAN;

    cs_args->csa_fore_chan_attrs.ca_headerpadsize          = 0;
    cs_args->csa_fore_chan_attrs.ca_maxrequestsize         = 1024 * 1024;
    cs_args->csa_fore_chan_attrs.ca_maxresponsesize        = 1024 * 1024;
    cs_args->csa_fore_chan_attrs.ca_maxresponsesize_cached = 0;
    cs_args->csa_fore_chan_attrs.ca_maxoperations          = 64;
    cs_args->csa_fore_chan_attrs.ca_maxrequests            = 64;
    cs_args->csa_fore_chan_attrs.ca_rdma_ird               = NULL;
    cs_args->csa_fore_chan_attrs.num_ca_rdma_ird           = 0;

    /* Back channel: a single slot is enough -- the server serialises recalls. */
    cs_args->csa_back_chan_attrs.ca_headerpadsize          = 0;
    cs_args->csa_back_chan_attrs.ca_maxrequestsize         = 4096;
    cs_args->csa_back_chan_attrs.ca_maxresponsesize        = 4096;
    cs_args->csa_back_chan_attrs.ca_maxresponsesize_cached = 0;
    cs_args->csa_back_chan_attrs.ca_maxoperations          = 8;
    cs_args->csa_back_chan_attrs.ca_maxrequests            = 1;
    cs_args->csa_back_chan_attrs.ca_rdma_ird               = NULL;
    cs_args->csa_back_chan_attrs.num_ca_rdma_ird           = 0;

    cs_args->csa_cb_program    = shared->nfs_v4_cb.rpc2.program;
    cs_args->csa_sec_parms     = NULL;
    cs_args->num_csa_sec_parms = 0;

    shared->nfs_v4.send_call_NFSPROC4_COMPOUND(
        &shared->nfs_v4.rpc2,
        shared->cb_evpl,
        server->cb_conn,
        NULL,
        &args,
        0, 0, NULL, 0, 0,
        chimera_nfs4_cb_create_session_callback,
        item);
} /* chimera_nfs4_cb_create_session */

/*
 * EXCHANGE_ID reply (control thread).  Allocate the session, stash the clientid,
 * and proceed to CREATE_SESSION.  The session is not published onto the server
 * until CREATE_SESSION fills in its sessionid.
 */
static void
chimera_nfs4_cb_exchange_id_callback(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_nfs4_cb_establish   *item = private_data;
    struct chimera_nfs4_client_session *session;

    (void) evpl;
    (void) verf;

    if (status != 0 || res->status != NFS4_OK ||
        res->num_resarray < 1 ||
        res->resarray[0].opexchange_id.eir_status != NFS4_OK) {
        chimera_nfsclient_error("NFS4 back-channel EXCHANGE_ID failed (rpc=%d compound=%d)",
                                status, res ? res->status : -1);
        chimera_nfs4_cb_establish_done(item, EIO);
        return;
    }

    session = calloc(1, sizeof(*session));
    pthread_mutex_init(&session->lock, NULL);
    session->clientid = res->resarray[0].opexchange_id.eir_resok4.eir_clientid;

    item->session = session;

    chimera_nfs4_cb_create_session(item);
} /* chimera_nfs4_cb_exchange_id_callback */

/*
 * Send EXCHANGE_ID on the control connection (control thread).  The client owner
 * (verifier + ownerid) is stable per server, so a reconnect yields the same
 * clientid family.
 */
static void
chimera_nfs4_cb_exchange_id(struct chimera_nfs4_cb_establish *item)
{
    struct chimera_nfs_client_server *server = item->server_thread->server;
    struct chimera_nfs_shared        *shared = server->shared;
    struct COMPOUND4args              args;
    struct nfs_argop4                 argarray[1];
    struct EXCHANGE_ID4args          *eid_args;
    struct timespec                   now;

    memset(&args, 0, sizeof(args));
    args.tag.len      = 0;
    args.minorversion = 1;
    args.argarray     = argarray;
    args.num_argarray = 1;

    argarray[0].argop = OP_EXCHANGE_ID;
    eid_args          = &argarray[0].opexchange_id;

    clock_gettime(CLOCK_REALTIME, &now);
    memcpy(server->nfs4_verifier, &now.tv_sec, sizeof(now.tv_sec));
    memcpy(eid_args->eia_clientowner.co_verifier, server->nfs4_verifier,
           NFS4_VERIFIER_SIZE);

    server->nfs4_owner_id_len = snprintf(server->nfs4_owner_id,
                                         sizeof(server->nfs4_owner_id),
                                         "chimera-%s-%d", server->hostname, getpid());
    eid_args->eia_clientowner.co_ownerid.data = (uint8_t *) server->nfs4_owner_id;
    eid_args->eia_clientowner.co_ownerid.len  = server->nfs4_owner_id_len;

    eid_args->eia_flags                 = EXCHGID4_FLAG_USE_NON_PNFS;
    eid_args->eia_state_protect.spa_how = SP4_NONE;
    eid_args->eia_client_impl_id        = NULL;
    eid_args->num_eia_client_impl_id    = 0;

    shared->nfs_v4.send_call_NFSPROC4_COMPOUND(
        &shared->nfs_v4.rpc2,
        shared->cb_evpl,
        server->cb_conn,
        NULL,
        &args,
        0, 0, NULL, 0, 0,
        chimera_nfs4_cb_exchange_id_callback,
        item);
} /* chimera_nfs4_cb_exchange_id */

/*
 * Control-thread doorbell: drain the establishment queue.  For each item ensure
 * the server's persistent connection exists (registering the NFS_V4_CB server
 * program so incoming CB_COMPOUND dispatches to chimera_nfs4_cb_compound), then
 * start EXCHANGE_ID.  A session already established by an earlier item resumes
 * immediately.
 */
static void
chimera_nfs4_cb_control_doorbell(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_nfs_shared        *shared =
        container_of(doorbell, struct chimera_nfs_shared, cb_doorbell);
    struct chimera_nfs4_cb_establish *queue, *item;
    struct chimera_nfs_client_server *server;
    struct evpl_rpc2_program         *cb_programs[1];

    (void) evpl;

    pthread_mutex_lock(&shared->cb_lock);
    queue                      = shared->cb_establish_queue;
    shared->cb_establish_queue = NULL;
    pthread_mutex_unlock(&shared->cb_lock);

    cb_programs[0] = &shared->nfs_v4_cb.rpc2;

    while (queue) {
        item       = queue;
        queue      = queue->next;
        item->next = NULL;
        server     = item->server_thread->server;

        if (server->nfs4_session) {
            /* Another mount of this server already established the session. */
            chimera_nfs4_cb_establish_done(item, 0);
            continue;
        }

        if (!server->cb_conn) {
            enum evpl_protocol_id proto = server->use_rdma
                                          ? server->rdma_protocol
                                          : shared->tcp_protocol;

            server->cb_conn = evpl_rpc2_client_connect(shared->cb_rpc2_thread,
                                                       proto,
                                                       server->nfs_endpoint,
                                                       cb_programs, 1, shared);
            if (!server->cb_conn) {
                chimera_nfsclient_error("NFS4 back-channel connect to %s failed",
                                        server->hostname);
                chimera_nfs4_cb_establish_done(item, EIO);
                continue;
            }
        }

        chimera_nfs4_cb_exchange_id(item);
    }
} /* chimera_nfs4_cb_control_doorbell */

/*
 * Control-thread rpc2 notify.  On disconnect, drop the dead cb_conn so the next
 * establishment reconnects.  Full session recovery (re-CREATE_SESSION, in-flight
 * migration) is deferred to the disconnect-recovery effort.
 */
static void
chimera_nfs4_cb_control_notify(
    struct evpl_rpc2_thread *thread,
    struct evpl_rpc2_conn   *conn,
    struct evpl_rpc2_notify *notify,
    void                    *private_data)
{
    struct chimera_nfs_shared        *shared = private_data;
    struct chimera_nfs_client_server *server;
    int                               i;

    (void) thread;

    if (notify->notify_type != EVPL_RPC2_NOTIFY_DISCONNECTED) {
        return;
    }

    pthread_mutex_lock(&shared->lock);
    for (i = 0; i < shared->max_servers; i++) {
        server = shared->servers[i];
        if (server && server->cb_conn == conn) {
            server->cb_conn = NULL;
            break;
        }
    }
    pthread_mutex_unlock(&shared->lock);
} /* chimera_nfs4_cb_control_notify */

/* Control-thread init (runs on the control thread's evpl before
 * evpl_thread_create returns): create its rpc2 thread and arm the doorbell. */
static void *
chimera_nfs4_cb_control_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_nfs_shared *shared = private_data;
    struct evpl_rpc2_program  *programs[6];

    programs[0] = &shared->mount_v3.rpc2;
    programs[1] = &shared->portmap_v2.rpc2;
    programs[2] = &shared->nfs_v3.rpc2;
    programs[3] = &shared->nfs_v4.rpc2;
    programs[4] = &shared->nfs_v4_cb.rpc2;
    programs[5] = &shared->nlm_v4.rpc2;

    shared->cb_evpl        = evpl;
    shared->cb_rpc2_thread = evpl_rpc2_thread_init(evpl, programs, 6,
                                                   chimera_nfs4_cb_control_notify, shared);

    evpl_add_doorbell(evpl, &shared->cb_doorbell, chimera_nfs4_cb_control_doorbell);

    return shared;
} /* chimera_nfs4_cb_control_init */

static void
chimera_nfs4_cb_control_shutdown(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_nfs_shared *shared = private_data;

    evpl_remove_doorbell(evpl, &shared->cb_doorbell);

    if (shared->cb_rpc2_thread) {
        evpl_rpc2_thread_destroy(shared->cb_rpc2_thread);
        shared->cb_rpc2_thread = NULL;
    }
} /* chimera_nfs4_cb_control_shutdown */

void
chimera_nfs4_cb_control_stop(struct chimera_nfs_shared *shared)
{
    /* cb_started is only ever set under cb_lock; at destroy time all threads
     * are quiesced, so an unlocked read is safe. */
    if (shared->cb_started) {
        evpl_thread_destroy(shared->cb_thread);
        shared->cb_thread  = NULL;
        shared->cb_started = 0;
    }
} /* chimera_nfs4_cb_control_stop */

/*
 * Resume doorbell (originating mount thread).  The control thread rang it once
 * establishment finished; pick up the result, free the item, and either fail
 * the mount or resume it (RECLAIM_COMPLETE + root FH on the mount connection).
 */
static void
chimera_nfs4_cb_resume(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_nfs4_cb_establish        *item =
        container_of(doorbell, struct chimera_nfs4_cb_establish, resume_db);
    struct chimera_nfs_client_server_thread *server_thread = item->server_thread;
    struct chimera_vfs_request              *request       = item->request;
    int                                      status        = item->status;

    evpl_remove_doorbell(evpl, &item->resume_db);
    free(item);

    if (status != 0) {
        request->status = CHIMERA_VFS_EIO;
        request->complete(request);
        return;
    }

    chimera_nfs4_mount_resume_after_session(server_thread, request);
} /* chimera_nfs4_cb_resume */

void
chimera_nfs4_cb_establish_session(
    struct chimera_nfs_client_server_thread *server_thread,
    struct chimera_vfs_request              *request)
{
    struct chimera_nfs_shared        *shared = server_thread->shared;
    struct chimera_nfs4_cb_establish *item   = calloc(1, sizeof(*item));

    item->server_thread = server_thread;
    item->request       = request;
    item->resume_evpl   = server_thread->thread->evpl;

    /* Arm the resume doorbell on our own evpl before publishing the item, so the
     * control thread can never ring it before it is registered. */
    evpl_add_doorbell(item->resume_evpl, &item->resume_db, chimera_nfs4_cb_resume);

    pthread_mutex_lock(&shared->cb_lock);

    /* Lazily start the control thread on first use.  evpl_thread_create blocks
     * until chimera_nfs4_cb_control_init has run (and armed cb_doorbell), so the
     * ring below always lands on a registered doorbell. */
    if (!shared->cb_started) {
        shared->cb_thread = evpl_thread_create(NULL,
                                               chimera_nfs4_cb_control_init,
                                               chimera_nfs4_cb_control_shutdown,
                                               shared);
        shared->cb_started = 1;
    }

    item->next                 = shared->cb_establish_queue;
    shared->cb_establish_queue = item;
    pthread_mutex_unlock(&shared->cb_lock);

    evpl_ring_doorbell(&shared->cb_doorbell);
} /* chimera_nfs4_cb_establish_session */
