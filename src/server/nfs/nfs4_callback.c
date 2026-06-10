// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stddef.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <xxhash.h>

/* portmap_xdr.h (pulled in via nfs_common.h below) #defines these RPC
 * protocol constants, colliding with <netinet/in.h>'s IPPROTO_* enum-macros.
 * This file does not use them, so drop the system macros before the XDR
 * headers redefine them. */
#undef IPPROTO_TCP
#undef IPPROTO_UDP

#include "evpl/evpl.h"
#include "evpl/evpl_rpc2.h"

#include "nfs4_callback.h"
#include "nfs4_cb.h"
#include "nfs4_state.h"
#include "nfs4_session.h"
#include "nfs4_procs.h"
#include "nfs_common.h"
#include "nfs_internal.h"
#include "vfs/vfs_state.h"

/*
 * Parse an RFC 1833 universal address "h1.h2.h3.h4.p1.p2" into a host string
 * and a TCP port (port = p1*256 + p2).  Handles IPv4 dotted-quad uaddrs (what
 * NFSv4.0 clients on a v4 transport send).  Returns 0 on success.
 */
static int
nfs4_cb_parse_uaddr(
    const char *uaddr,
    char       *host,
    size_t      hostsz,
    int        *out_port)
{
    char   buf[80];
    char  *last_dot, *second_dot;
    int    hi, lo;
    size_t hostlen;

    if (!uaddr || uaddr[0] == '\0') {
        return -1;
    }

    snprintf(buf, sizeof(buf), "%s", uaddr);

    last_dot = strrchr(buf, '.');
    if (!last_dot) {
        return -1;
    }
    *last_dot = '\0';
    lo        = atoi(last_dot + 1);

    second_dot = strrchr(buf, '.');
    if (!second_dot) {
        return -1;
    }
    *second_dot = '\0';
    hi          = atoi(second_dot + 1);

    hostlen = strlen(buf);
    if (hostlen == 0 || hostlen >= hostsz) {
        return -1;
    }
    memcpy(host, buf, hostlen + 1);
    *out_port = (hi << 8) | lo;
    return 0;
} /* nfs4_cb_parse_uaddr */

/*
 * Probe whether host:port (IPv4) is connectable.  evpl_rpc2_client_connect ->
 * evpl_socket_tcp_connect aborts the process on a synchronous connect() error
 * (e.g. ECONNREFUSED, which loopback returns immediately for a dead port).  A
 * client's callback address is untrusted and may be unreachable, so we must
 * never reach that abort: pre-flight with our own non-blocking socket and only
 * hand off to evpl when the peer is reachable.  Returns true if connectable.
 */
static bool
nfs4_cb_addr_reachable(
    const char *host,
    int         port)
{
    struct sockaddr_in sin;
    int                fd;
    int                rc;
    bool               ok = false;

    memset(&sin, 0, sizeof(sin));
    sin.sin_family = AF_INET;
    sin.sin_port   = htons((uint16_t) port);
    if (inet_pton(AF_INET, host, &sin.sin_addr) != 1) {
        return false;
    }

    fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
    if (fd < 0) {
        return false;
    }

    do {
        rc = connect(fd, (struct sockaddr *) &sin, sizeof(sin));
    } while (rc < 0 && errno == EINTR);

    if (rc == 0) {
        ok = true;
    } else if (errno == EINPROGRESS || errno == EALREADY) {
        /* Connection underway; treat as reachable.  evpl will redo its own
         * connect immediately after, which the listening peer accepts. */
        ok = true;
    }

    close(fd);
    return ok;
} /* nfs4_cb_addr_reachable */

/* ---------------------------------------------------------------------- */
/* CB_NULL liveness probe                                                 */
/* ---------------------------------------------------------------------- */

struct nfs4_cb_probe_ctx {
    struct nfs_client *client;
};

static void
nfs4_cb_null_complete(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    int                          status,
    void                        *private_data)
{
    struct nfs4_cb_probe_ctx *ctx    = private_data;
    struct nfs_client        *client = ctx->client;
    struct nfs_request       *waiters;

    (void) evpl;
    (void) verf;

    if (status == 0) {
        atomic_store_explicit(&client->cb_path.cb_state, NFS4_CB_UP,
                              memory_order_release);
    } else {
        atomic_store_explicit(&client->cb_path.cb_state, NFS4_CB_DOWN,
                              memory_order_release);
        chimera_nfs_error("NFS4 callback path probe failed (status %d) for client %lu",
                          status, client->client_id);
    }

    /* Resume any OPENs that parked while the probe was in flight.  Runs on
     * the channel's owner thread (this completion fires from this thread's
     * evpl), the same thread the OPEN parked from, so the list mutation is
     * race-free without a lock. */
    waiters                       = client->cb_path.probe_waiters;
    client->cb_path.probe_waiters = NULL;
    while (waiters) {
        struct nfs_request *req = waiters;
        waiters         = req->probe_next;
        req->probe_next = NULL;
        chimera_nfs4_open_resume_after_probe(req);
    }

    free(ctx);
} /* nfs4_cb_null_complete */

/*
 * Establish the callback channel for `client` (if not already present) and
 * fire a CB_NULL probe.  Runs on `thread`, which owns the client's fore
 * connection.
 */
/*
 * Resolve the connection to send a callback on, right now.  For 4.0 this is the
 * server-owned outbound conn (NULLed by nfs4_cb_conn_lost() once it drops); for
 * 4.1 it is read live from the session, since the backchannel-carrying fore
 * conn is freed on disconnect and re-bound on reconnect and so must never be
 * cached.  Returns NULL if no usable connection currently exists.  Called only
 * on the channel's owner thread.
 */
static struct evpl_rpc2_conn *
nfs4_cb_chan_conn(struct nfs4_cb_client *chan)
{
    if (chan->owns_conn) {
        return chan->conn;
    }
    return chan->session ? chan->session->nfs4_session_backchannel_conn : NULL;
} /* nfs4_cb_chan_conn */

/* ---------------------------------------------------------------------- */
/* Callback-channel lifetime (refcount)                                   */
/* ---------------------------------------------------------------------- */

/* Take a reference for an about-to-be-issued CB RPC; its completion drops it
 * via nfs4_cb_client_unref_complete().  Caller runs on the owner thread. */
static inline void
nfs4_cb_client_ref(struct nfs4_cb_client *chan)
{
    atomic_fetch_add_explicit(&chan->refcount, 1, memory_order_relaxed);
} /* nfs4_cb_client_ref */

/* Free a callback channel.  MUST run on chan->owner_thread: it releases the
 * 4.1 backchannel session ref and frees the struct whose embedded cb_prog an
 * owner-thread reply dispatch may otherwise still reference.  (The 4.0 conn
 * back-pointer is cleared synchronously in nfs4_cb_path_teardown.) */
static void
nfs4_cb_client_destroy(struct nfs4_cb_client *chan)
{
    if (chan->session) {
        nfs4_session_put(chan->session);
    }
    free(chan);
} /* nfs4_cb_client_destroy */

/* Drop a reference; returns true iff it was the last (the caller must free). */
static inline bool
nfs4_cb_client_unref(struct nfs4_cb_client *chan)
{
    return atomic_fetch_sub_explicit(&chan->refcount, 1,
                                     memory_order_acq_rel) == 1;
} /* nfs4_cb_client_unref */

/* Drop an in-flight CB RPC's reference from its completion (which runs on the
 * channel's owner thread), freeing the channel here if it was the last. */
static void
nfs4_cb_client_unref_complete(struct nfs4_cb_client *chan)
{
    if (chan && nfs4_cb_client_unref(chan)) {
        nfs4_cb_client_destroy(chan);
    }
} /* nfs4_cb_client_unref_complete */

static void
nfs4_cb_channel_open(
    struct chimera_server_nfs_thread *thread,
    struct nfs_client                *client,
    struct nfs_request               *req)
{
    struct nfs4_cb_path      *cb = &client->cb_path;
    struct nfs4_cb_client    *chan;
    struct nfs4_cb_probe_ctx *ctx;

    chan               = calloc(1, sizeof(*chan));
    chan->magic        = NFS4_CB_CLIENT_MAGIC;
    chan->owner_thread = thread;
    chan->cb_path      = cb;
    chan->minorversion = cb->cb_minorversion;
    chan->cb_ident     = cb->cb_ident;

    /* Private program copy carrying the client's transient callback program
     * number; program_data must point at this stable copy for async reply
     * dispatch. */
    chan->cb_prog                   = thread->shared->nfs_v4_cb;
    chan->cb_prog.rpc2.program      = cb->cb_program;
    chan->cb_prog.rpc2.program_data = &chan->cb_prog;
    atomic_init(&chan->cb_seq, 1);
    atomic_init(&chan->refcount, 1); /* the owning cb_path's reference */

    if (cb->cb_minorversion == 0) {
        char                  host[64];
        int                   port = 0;
        struct evpl_endpoint *ep;

        if (nfs4_cb_parse_uaddr(cb->cb_addr, host, sizeof(host), &port) != 0) {
            chimera_nfs_error("NFS4 callback: bad uaddr '%s' for client %lu",
                              cb->cb_addr, client->client_id);
            atomic_store_explicit(&cb->cb_state, NFS4_CB_DOWN,
                                  memory_order_release);
            free(chan);
            return;
        }

        /* A client that does not actually offer a callback service advertises
         * a null address (e.g. "0.0.0.0.0.0" -> 0.0.0.0:0).  Treat that as no
         * callback path -- no probe, no delegation. */
        if (port == 0 || strcmp(host, "0.0.0.0") == 0) {
            atomic_store_explicit(&cb->cb_state, NFS4_CB_DOWN,
                                  memory_order_release);
            free(chan);
            return;
        }

        /* Guard against evpl's fatal abort on a refused/unreachable callback
         * address (the client's cb addr is untrusted). */
        if (!nfs4_cb_addr_reachable(host, port)) {
            atomic_store_explicit(&cb->cb_state, NFS4_CB_DOWN,
                                  memory_order_release);
            free(chan);
            return;
        }

        ep         = evpl_endpoint_create(host, port);
        chan->conn = evpl_rpc2_client_connect(thread->rpc2_thread,
                                              EVPL_STREAM_SOCKET_TCP,
                                              ep, NULL, 0, NULL);
        chan->owns_conn = 1;

        if (!chan->conn) {
            chimera_nfs_error("NFS4 callback: connect to %s:%d failed", host, port);
            atomic_store_explicit(&cb->cb_state, NFS4_CB_DOWN,
                                  memory_order_release);
            free(chan);
            return;
        }

        /* Tag the conn so the shared rpc2 disconnect notify can find this
         * channel and invalidate it if the conn drops (see nfs4_cb_conn_lost). */
        evpl_rpc2_conn_set_private_data(chan->conn, chan);
    } else {
        /* 4.1+: the backchannel rides the session's fore conn.  Hold a ref on
         * the session and re-read the live conn at send time rather than
         * caching it, since the fore conn is freed on disconnect. */
        struct nfs4_session *session = req ? req->session : NULL;

        if (!session || !session->nfs4_session_backchannel_conn) {
            atomic_store_explicit(&cb->cb_state, NFS4_CB_DOWN,
                                  memory_order_release);
            free(chan);
            return;
        }
        nfs4_session_get(session);
        chan->session   = session;
        chan->owns_conn = 0;
        memcpy(chan->sessionid, session->nfs4_session_id, NFS4_SESSIONID_SIZE);
    }

    cb->cb_client = chan;

    if (cb->cb_minorversion >= 1) {
        /* 4.1+: the backchannel rides the session's fore connection and is
         * already bound at CREATE_SESSION (RFC 8881 §2.10.3.1), so the server
         * may deliver callbacks on it immediately -- no CB_NULL round trip is
         * required to validate it.  Mark the path usable now so the client's
         * very first delegation-wanting OPEN is served, rather than racing an
         * asynchronous probe.  Avoiding the per-session probe also keeps
         * many-client workloads cheap (pynfs COUR7 opens 1000 sessions; a
         * CB_NULL on each would swamp the single-threaded callback path).  A
         * backchannel that turns out to be dead is caught by the first real
         * CB_RECALL, which drives the path to NFS4_CB_DOWN and revokes. */
        atomic_store_explicit(&cb->cb_state, NFS4_CB_UP, memory_order_release);
        return;
    }

    atomic_store_explicit(&cb->cb_state, NFS4_CB_PROBING, memory_order_release);

    ctx         = calloc(1, sizeof(*ctx));
    ctx->client = client;

    chan->cb_prog.send_call_CB_NULL(&chan->cb_prog.rpc2,
                                    thread->evpl,
                                    nfs4_cb_chan_conn(chan),
                                    NULL,
                                    0, 0, NULL, 0, 0,
                                    nfs4_cb_null_complete,
                                    ctx);
} /* nfs4_cb_channel_open */

/*
 * Internal driver.  Returns the post-action cb_state so callers can decide
 * what to do with it; also reports (via *triggered_out) whether THIS call
 * was the one that flipped UNINIT->PROBING.
 */
static uint8_t
nfs4_cb_probe_drive(
    struct chimera_server_nfs_thread *thread,
    struct nfs_client                *client,
    struct nfs_request               *req,
    bool                             *triggered_out)
{
    uint8_t state;

    if (triggered_out) {
        *triggered_out = false;
    }

    state = atomic_load_explicit(&client->cb_path.cb_state,
                                 memory_order_acquire);

    if (state != NFS4_CB_UNINIT) {
        return state;
    }

    /* No callback program captured at all -> client never asked to be able
     * to receive callbacks; report DOWN so callers skip delegation. */
    if (client->cb_path.cb_program == 0) {
        return NFS4_CB_DOWN;
    }

    /* Claim the one-time channel setup: only the thread that flips
     * UNINIT->PROBING opens the channel.  A concurrent OPEN that loses the
     * race sees the post-CAS state and reacts to that instead. */
    {
        uint8_t expect = NFS4_CB_UNINIT;

        if (!atomic_compare_exchange_strong_explicit(
                &client->cb_path.cb_state, &expect, NFS4_CB_PROBING,
                memory_order_acq_rel, memory_order_acquire)) {
            return expect;
        }
    }

    if (triggered_out) {
        *triggered_out = true;
    }

    nfs4_cb_channel_open(thread, client, req);

    /* 4.1+ binds the backchannel and marks the path UP synchronously (no
     * CB_NULL round trip).  4.0 fired an asynchronous CB_NULL and is still
     * PROBING here. */
    return atomic_load_explicit(&client->cb_path.cb_state,
                                memory_order_acquire);
} /* nfs4_cb_probe_drive */

bool
nfs4_cb_ensure_probe(
    struct chimera_server_nfs_thread *thread,
    struct nfs_client                *client,
    struct nfs_request               *req)
{
    if (!client) {
        return false;
    }
    return nfs4_cb_probe_drive(thread, client, req, NULL) == NFS4_CB_UP;
} /* nfs4_cb_ensure_probe */

enum nfs4_cb_grant_decision
nfs4_cb_grant_probe(
    struct chimera_server_nfs_thread *thread,
    struct nfs_client                *client,
    struct nfs_request               *req)
{
    uint8_t state;
    bool triggered = false;

    if (!client) {
        return NFS4_CB_GRANT_NO_PATH;
    }

    state = nfs4_cb_probe_drive(thread, client, req, &triggered);

    switch (state) {
        case NFS4_CB_UP:
            return NFS4_CB_GRANT_PATH_UP;
        case NFS4_CB_PROBING:
            /* If THIS call kicked the probe, preserve the historical
             * behavior: the kicking OPEN proceeds without a delegation.
             * (This is how chimera has always behaved on 4.0 -- the very
             * first delegation-wanting OPEN never gets a delegation, and a
             * follow-up OPEN is the one that lands it.  Changing that for
             * the kicking OPEN itself would also start handing out
             * delegations on OPENs that historically did not get them,
             * which some pynfs tests rely on -- e.g. DELEG15d sets up an
             * undelegated file via a plain c.create_file before requesting
             * a delegation on a different file.)  Only OPENs that find the
             * probe already in flight defer. */
            return triggered ? NFS4_CB_GRANT_NO_PATH : NFS4_CB_GRANT_DEFER;
        case NFS4_CB_DOWN:
        default:
            return NFS4_CB_GRANT_NO_PATH;
    } /* switch */
} /* nfs4_cb_grant_probe */

void
nfs4_cb_probe_park(
    struct nfs_client  *client,
    struct nfs_request *req)
{
    /* probe_waiters is owned by chan->owner_thread; the OPEN handler runs on
     * the same thread, and the only producer of completions (nfs4_cb_null_
     * complete) runs there too, so no lock is needed.  LIFO is fine -- the
     * order in which deferred OPENs resume doesn't affect correctness. */
    req->probe_next               = client->cb_path.probe_waiters;
    client->cb_path.probe_waiters = req;
} /* nfs4_cb_probe_park */

/* ---------------------------------------------------------------------- */
/* CB_RECALL                                                              */
/* ---------------------------------------------------------------------- */

struct nfs4_cb_recall_ctx {
    struct nfs_delegation            *deleg;
    struct chimera_server_nfs_thread *thread;
    struct nfs4_cb_client            *chan; /* ref held until this completes */
};

static void nfs4_cb_recall_enqueue(
    struct nfs_delegation *deleg);

/* Backchannel CB_RECALL retransmit (BADSESSION race).  A client that has just
 * CREATE_SESSION'd a new session may not have finished registering its
 * backchannel when the server re-drives a pending recall over it, so it rejects
 * the recall's CB_SEQUENCE with NFS4ERR_BADSESSION -- without consuming the
 * slot.  We reset the slot sequence and retransmit after a short delay until the
 * session becomes usable, bounded so a genuinely unreachable client still falls
 * through to the vfs_state recall deadline / lease-expiry revocation. */
#define NFS4_CB_RECALL_RETRY_DELAY_US 250000  /* 250ms between retransmits */
#define NFS4_CB_RECALL_RETRY_MAX      50      /* ~12.5s, under the 15s deadline */

struct nfs4_cb_recall_retry {
    struct evpl_timer                 timer;
    struct nfs_delegation            *deleg; /* ref held until the timer fires */
    struct chimera_server_nfs_thread *thread;
};

/* One-shot timer (owner thread): retransmit the recall over the client's
 * current callback channel. */
static void
nfs4_cb_recall_retry_fire(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct nfs4_cb_recall_retry      *r =
        (struct nfs4_cb_recall_retry *) ((char *) timer -
                                         offsetof(struct nfs4_cb_recall_retry, timer));
    struct nfs_delegation            *deleg  = r->deleg;
    struct chimera_server_nfs_thread *thread = r->thread;

    (void) evpl;

    atomic_store_explicit(&deleg->cb_recall_state, NFS4_DELEG_RECALLING,
                          memory_order_release);
    nfs4_cb_recall_enqueue(deleg);

    nfs_state_table_release(&thread->shared->nfs4_state_table, deleg,
                            NFS4_SLOT_TYPE_DELEG, thread->vfs_thread);
    free(r);
} /* nfs4_cb_recall_retry_fire */

static void
nfs4_cb_recall_complete(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct CB_COMPOUND4res      *reply,
    int                          status,
    void                        *private_data)
{
    struct nfs4_cb_recall_ctx        *ctx    = private_data;
    struct nfs_delegation            *deleg  = ctx->deleg;
    struct chimera_server_nfs_thread *thread = ctx->thread;

    (void) evpl;
    (void) verf;

    /* An RPC-level failure or a callback compound error means this recall
     * attempt did not land.  Do NOT revoke the delegation here: a 4.1 client may
     * have lost its backchannel (e.g. it destroyed the session mid-recall) and
     * will present a new one via CREATE_SESSION, at which point the recall is
     * re-driven (see nfs4_cb_resend_recalls_on_rebind).  Move the delegation
     * back to ACTIVE so the rebind path (and any fresh conflict) can re-arm the
     * one-shot recall.  The backing lease stays BREAKING with its bounded recall
     * deadline, so a stuck delegation is still revoked by the vfs_state deadline
     * (a conflicting acquirer's retry) or by client lease expiry -- nothing
     * leaks. */
    if (status != 0 || (reply && reply->status != NFS4_OK)) {
        bool    badsession = (status == 0 && reply &&
                              reply->status == NFS4ERR_BADSESSION);
        bool    same_chan = (ctx->chan == deleg->client->cb_path.cb_client);
        uint8_t expected  = NFS4_DELEG_RECALLING;

        atomic_compare_exchange_strong_explicit(
            &deleg->cb_recall_state, &expected, NFS4_DELEG_ACTIVE,
            memory_order_acq_rel, memory_order_acquire);

        if (badsession && same_chan && deleg->lease_held &&
            deleg->lease.break_state == CHIMERA_VFS_BREAK_BREAKING &&
            atomic_fetch_add_explicit(&deleg->cb_recall_retries, 1,
                                      memory_order_acq_rel) <
            NFS4_CB_RECALL_RETRY_MAX) {
            /* The client just created this session and rejected the recall's
            * CB_SEQUENCE with BADSESSION before its backchannel was usable --
            * the slot was not consumed.  Reset the slot-0 sequence and
            * retransmit over the same channel after a short delay (the path is
            * fine, the client is just catching up).  Do NOT mark it DOWN. */
            struct nfs4_cb_recall_retry *r = calloc(1, sizeof(*r));

            chimera_nfs_abort_if(r == NULL, "recall retry alloc failed");
            atomic_store_explicit(&ctx->chan->cb_seq, 1, memory_order_release);
            r->deleg  = deleg;
            r->thread = thread;
            atomic_fetch_add_explicit(&deleg->refcount, 1, memory_order_acq_rel);
            evpl_add_oneshot_timer(thread->evpl, &r->timer,
                                   nfs4_cb_recall_retry_fire,
                                   NFS4_CB_RECALL_RETRY_DELAY_US);
        } else if (same_chan) {
            /* Non-retryable failure (or retry budget exhausted): the path is
             * unusable for granting new delegations.  Mark it DOWN only if this
             * channel is still the client's current one -- a stale completion
             * must not clobber a path a rebind already rebuilt UP. */
            atomic_store_explicit(&deleg->client->cb_path.cb_state,
                                  NFS4_CB_DOWN, memory_order_release);
        }
    }

    /* Drop the transient recall ref. */
    nfs_state_table_release(&thread->shared->nfs4_state_table, deleg,
                            NFS4_SLOT_TYPE_DELEG, thread->vfs_thread);

    /* Release this RPC's reference on the callback channel (runs on the
     * owner thread; frees the channel if the path was already torn down). */
    nfs4_cb_client_unref_complete(ctx->chan);
    free(ctx);
} /* nfs4_cb_recall_complete */

/* Build and send the CB_COMPOUND{[CB_SEQUENCE,] CB_RECALL} on the channel.
 * Runs on the channel's owner thread. */
static void
nfs4_cb_recall_send(
    struct chimera_server_nfs_thread *thread,
    struct nfs_delegation            *deleg)
{
    struct nfs4_cb_client     *chan = deleg->client->cb_path.cb_client;
    struct evpl_rpc2_conn     *conn = chan ? nfs4_cb_chan_conn(chan) : NULL;
    struct nfs4_cb_recall_ctx *ctx;
    struct CB_COMPOUND4args    args;
    struct nfs_cb_argop4       ops[2];
    struct stateid4            sid;
    int                        nops = 0;

    if (!conn) {
        /* The backchannel went away before this queued recall could be sent
         * (e.g. the client destroyed the session in the DESTROY/CREATE gap).
         * Do NOT revoke: move the delegation back to ACTIVE so a subsequent
         * CREATE_SESSION rebind re-drives the recall over the new backchannel.
         * The lease stays BREAKING under its recall deadline, so the vfs_state
         * deadline / client lease expiry remain the revocation backstop. */
        uint8_t expected = NFS4_DELEG_RECALLING;

        atomic_compare_exchange_strong_explicit(
            &deleg->cb_recall_state, &expected, NFS4_DELEG_ACTIVE,
            memory_order_acq_rel, memory_order_acquire);

        nfs_state_table_release(&thread->shared->nfs4_state_table, deleg,
                                NFS4_SLOT_TYPE_DELEG, thread->vfs_thread);
        return;
    }

    memset(&args, 0, sizeof(args));
    memset(ops, 0, sizeof(ops));

    /* 4.1+: a CB_SEQUENCE must lead the compound. */
    if (chan->minorversion >= 1) {
        struct CB_SEQUENCE4args *seq = &ops[nops].opcbsequence;
        ops[nops].argop = OP_CB_SEQUENCE;
        memcpy(seq->csa_sessionid, chan->sessionid, NFS4_SESSIONID_SIZE);
        seq->csa_sequenceid = atomic_fetch_add_explicit(&chan->cb_seq, 1,
                                                        memory_order_relaxed);
        seq->csa_slotid                   = 0;
        seq->csa_highest_slotid           = 0;
        seq->csa_cachethis                = 0;
        seq->num_csa_referring_call_lists = 0;
        seq->csa_referring_call_lists     = NULL;
        nops++;
    }

    /* Reconstruct the delegation stateid to recall. */
    nfs4_stateid_encode(&sid, deleg->seqid, NFS4_STATEID_TYPE_DELEG,
                        deleg->shard, deleg->slot_idx, deleg->generation,
                        thread->shared->nfs4_state_table.epoch);

    ops[nops].argop               = OP_CB_RECALL;
    ops[nops].opcbrecall.stateid  = sid;
    ops[nops].opcbrecall.truncate = 0;
    ops[nops].opcbrecall.fh.len   = deleg->fh_len;
    ops[nops].opcbrecall.fh.data  = deleg->fh;
    nops++;

    args.tag.len        = 0;
    args.tag.data       = NULL;
    args.minorversion   = chan->minorversion;
    args.callback_ident = chan->cb_ident;
    args.num_argarray   = nops;
    args.argarray       = ops;

    ctx         = calloc(1, sizeof(*ctx));
    ctx->deleg  = deleg;
    ctx->thread = thread;
    ctx->chan   = chan;
    /* Keep the channel (and its embedded cb_prog) alive until this RPC's
     * reply dispatches, even if the client's lease expires meanwhile. */
    nfs4_cb_client_ref(chan);

    /* Use the RPC auth the client requested for its callback channel
     * (CREATE_SESSION csa_sec_parms / BACKCHANNEL_CTL).  AUTH_NONE -> NULL
     * cred; AUTH_SYS -> the client's uid/gid. */
    {
        struct nfs4_cb_path   *cb = &deleg->client->cb_path;
        struct evpl_rpc2_cred  rpc_cred;
        struct evpl_rpc2_cred *credp         = NULL;
        static const char      machinename[] = "chimera";

        if (cb->cb_sec_flavor == AUTH_SYS) {
            memset(&rpc_cred, 0, sizeof(rpc_cred));
            rpc_cred.flavor                  = EVPL_RPC2_AUTH_SYS;
            rpc_cred.authsys.uid             = cb->cb_sec_uid;
            rpc_cred.authsys.gid             = cb->cb_sec_gid;
            rpc_cred.authsys.num_gids        = 0;
            rpc_cred.authsys.gids            = NULL;
            rpc_cred.authsys.machinename     = machinename;
            rpc_cred.authsys.machinename_len = sizeof(machinename) - 1;
            credp                            = &rpc_cred;
        }

        chan->cb_prog.send_call_CB_COMPOUND(&chan->cb_prog.rpc2,
                                            thread->evpl,
                                            conn,
                                            credp,
                                            &args,
                                            0, 0, NULL, 0, 0,
                                            nfs4_cb_recall_complete,
                                            ctx);
    }
} /* nfs4_cb_recall_send */

/* ---------------------------------------------------------------------- */
/* CB_LAYOUTRECALL (pNFS layout recall)                                   */
/* ---------------------------------------------------------------------- */

#define NFS4_LAYOUT4_FLEX_FILES 0x4 /* RFC 8435; not in the generated XDR */

struct nfs4_cb_layoutrecall_ctx {
    void                   (*done)(
        int   cb_status,
        void *arg);
    void                  *arg;
    struct nfs4_cb_client *chan; /* ref held until this completes */
};

static void
nfs4_cb_layoutrecall_complete(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct CB_COMPOUND4res      *reply,
    int                          status,
    void                        *private_data)
{
    struct nfs4_cb_layoutrecall_ctx *ctx = private_data;

    (void) evpl;
    (void) verf;

    /* cb_status: the callback compound result (NFS4_OK => the client will
     * LAYOUTRETURN); a negative value flags an RPC-level transport failure. */
    ctx->done((status == 0 && reply) ? (int) reply->status : -1, ctx->arg);
    nfs4_cb_client_unref_complete(ctx->chan);
    free(ctx);
} /* nfs4_cb_layoutrecall_complete */

bool
nfs4_cb_layoutrecall(
    struct chimera_server_nfs_thread *thread,
    struct nfs_client *client,
    const uint8_t *fh,
    uint32_t fh_len,
    const struct stateid4 *layout_stateid,
    void ( *done )(int cb_status, void *arg),
    void *arg)
{
    struct nfs4_cb_client           *chan = client->cb_path.cb_client;
    struct evpl_rpc2_conn           *conn = chan ? nfs4_cb_chan_conn(chan) : NULL;
    struct nfs4_cb_layoutrecall_ctx *ctx;
    struct CB_COMPOUND4args          args;
    struct nfs_cb_argop4             ops[2];
    int                              nops = 0;

    if (!conn) {
        return false;
    }

    memset(&args, 0, sizeof(args));
    memset(ops, 0, sizeof(ops));

    /* 4.1+: a CB_SEQUENCE must lead the compound. */
    if (chan->minorversion >= 1) {
        struct CB_SEQUENCE4args *seq = &ops[nops].opcbsequence;
        ops[nops].argop = OP_CB_SEQUENCE;
        memcpy(seq->csa_sessionid, chan->sessionid, NFS4_SESSIONID_SIZE);
        seq->csa_sequenceid = atomic_fetch_add_explicit(&chan->cb_seq, 1,
                                                        memory_order_relaxed);
        seq->csa_slotid                   = 0;
        seq->csa_highest_slotid           = 0;
        seq->csa_cachethis                = 0;
        seq->num_csa_referring_call_lists = 0;
        seq->csa_referring_call_lists     = NULL;
        nops++;
    }

    ops[nops].argop                                                = OP_CB_LAYOUTRECALL;
    ops[nops].opcblayoutrecall.clora_type                          = NFS4_LAYOUT4_FLEX_FILES;
    ops[nops].opcblayoutrecall.clora_iomode                        = LAYOUTIOMODE4_ANY;
    ops[nops].opcblayoutrecall.clora_changed                       = 0;
    ops[nops].opcblayoutrecall.clora_recall.lor_recalltype         = LAYOUTRECALL4_FILE;
    ops[nops].opcblayoutrecall.clora_recall.lor_layout.lor_fh.len  = fh_len;
    ops[nops].opcblayoutrecall.clora_recall.lor_layout.lor_fh.data = (void *) fh;
    ops[nops].opcblayoutrecall.clora_recall.lor_layout.lor_offset  = 0;
    ops[nops].opcblayoutrecall.clora_recall.lor_layout.lor_length  = UINT64_MAX;
    ops[nops].opcblayoutrecall.clora_recall.lor_layout.lor_stateid = *layout_stateid;
    nops++;

    args.tag.len        = 0;
    args.tag.data       = NULL;
    args.minorversion   = chan->minorversion;
    args.callback_ident = chan->cb_ident;
    args.num_argarray   = nops;
    args.argarray       = ops;

    ctx       = calloc(1, sizeof(*ctx));
    ctx->done = done;
    ctx->arg  = arg;
    ctx->chan = chan;
    nfs4_cb_client_ref(chan); /* hold the channel until the reply dispatches */

    /* Same callback RPC auth the channel uses for CB_RECALL. */
    {
        struct nfs4_cb_path   *cb = &client->cb_path;
        struct evpl_rpc2_cred  rpc_cred;
        struct evpl_rpc2_cred *credp         = NULL;
        static const char      machinename[] = "chimera";

        if (cb->cb_sec_flavor == AUTH_SYS) {
            memset(&rpc_cred, 0, sizeof(rpc_cred));
            rpc_cred.flavor                  = EVPL_RPC2_AUTH_SYS;
            rpc_cred.authsys.uid             = cb->cb_sec_uid;
            rpc_cred.authsys.gid             = cb->cb_sec_gid;
            rpc_cred.authsys.num_gids        = 0;
            rpc_cred.authsys.gids            = NULL;
            rpc_cred.authsys.machinename     = machinename;
            rpc_cred.authsys.machinename_len = sizeof(machinename) - 1;
            credp                            = &rpc_cred;
        }

        chan->cb_prog.send_call_CB_COMPOUND(&chan->cb_prog.rpc2,
                                            thread->evpl,
                                            conn,
                                            credp,
                                            &args,
                                            0, 0, NULL, 0, 0,
                                            nfs4_cb_layoutrecall_complete,
                                            ctx);
    }

    return true;
} /* nfs4_cb_layoutrecall */

/* ---------------------------------------------------------------------- */
/* CB_GETATTR (write-delegation attribute query)                          */
/* ---------------------------------------------------------------------- */

#define NFS4_CB_GETATTR_REQUEST    0
#define NFS4_CB_GETATTR_RESPONSE   1

/* A holder that accepts the CB_GETATTR but never answers must not wedge the
 * requester's GETATTR forever; fall back to the server's own attrs after this. */
#define NFS4_CB_GETATTR_TIMEOUT_US (5ULL * 1000 * 1000)

/* Cross-thread CB_GETATTR work item.  A REQUEST item is created on the
 * requester thread, queued to the holder thread, and lives until the CB_GETATTR
 * RPC completes there (it carries the in-flight RPC's private_data and timeout
 * timer).  The result is handed back to the requester as a separate RESPONSE
 * item, so the request item can be freed independently of a late RPC reply. */
struct nfs4_cb_getattr {
    uint8_t                           phase;   /* request (0) / response (1) */
    struct chimera_server_nfs_thread *requester_thread;
    struct nfs_delegation            *deleg;
    void                             *priv;
    nfs4_cb_getattr_resume_t          resume;
    int                               status;
    bool                              got_change;
    bool                              got_size;
    uint64_t                          change;
    uint64_t                          size;
    /* REQUEST item, holder thread only: */
    struct evpl_timer                 timeout_timer;
    bool                              delivered; /* result already handed back */
    struct nfs4_cb_client            *chan;      /* ref held until RPC completes */
    struct nfs4_cb_getattr           *next;
};

/* Hand the result of a CB_GETATTR back to the requester's thread.  Runs on the
 * holder thread; idempotent (whichever of the RPC reply / timeout fires first
 * delivers, the other is a no-op).  A fresh RESPONSE item is queued rather than
 * the REQUEST item `w`, because a timed-out RPC may still complete later and
 * dereference `w`. */
static void
nfs4_cb_getattr_deliver(struct nfs4_cb_getattr *w)
{
    struct chimera_server_nfs_thread *x = w->requester_thread;
    struct nfs4_cb_getattr           *r;

    if (w->delivered) {
        return;
    }
    w->delivered = true;

    r                   = calloc(1, sizeof(*r));
    r->phase            = NFS4_CB_GETATTR_RESPONSE;
    r->requester_thread = x;
    r->deleg            = w->deleg;
    r->priv             = w->priv;
    r->resume           = w->resume;
    r->status           = w->status;
    r->got_change       = w->got_change;
    r->change           = w->change;
    r->got_size         = w->got_size;
    r->size             = w->size;

    pthread_mutex_lock(&x->cb_recall_lock);
    r->next             = x->cb_getattr_queue;
    x->cb_getattr_queue = r;
    pthread_mutex_unlock(&x->cb_recall_lock);

    evpl_ring_doorbell(&x->cb_doorbell);
} /* nfs4_cb_getattr_deliver */

/* One-shot timeout (holder thread): the CB_GETATTR RPC is still in flight, so
 * deliver a fall-back result now.  The request item is freed by the RPC
 * completion if/when it arrives (or by conn teardown), never here. */
static void
nfs4_cb_getattr_timeout(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct nfs4_cb_getattr *w = (struct nfs4_cb_getattr *)
        ((char *) timer - offsetof(struct nfs4_cb_getattr, timeout_timer));

    (void) evpl;

    w->status = -1;
    nfs4_cb_getattr_deliver(w);
} /* nfs4_cb_getattr_timeout */

static void
nfs4_cb_getattr_complete(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct CB_COMPOUND4res      *reply,
    int                          status,
    void                        *private_data)
{
    struct nfs4_cb_getattr *w = private_data;

    (void) verf;

    /* Cancel the timeout (a no-op if it already fired -- see one-shot timer
     * semantics).  Runs on the holder thread, same as the timeout, so the
     * delivered flag below needs no synchronisation. */
    evpl_remove_timer(evpl, &w->timeout_timer);

    if (!w->delivered) {
        w->status = -1;

        if (status == 0 && reply && reply->status == NFS4_OK) {
            for (uint32_t i = 0; i < reply->num_resarray; i++) {
                struct nfs_cb_resop4 *rop = &reply->resarray[i];

                if (rop->resop == OP_CB_GETATTR &&
                    rop->opcbgetattr.status == NFS4_OK) {
                    struct fattr4 *fa  = &rop->opcbgetattr.resok4.obj_attributes;
                    const uint8_t *p   = fa->attr_vals.data;
                    const uint8_t *end = p + fa->attr_vals.len;
                    uint32_t       w0  = (fa->num_attrmask >= 1) ? fa->attrmask[0] : 0;
                    uint64_t       v;

                    /* The holder returns change (bit 3) then size (bit 4). */
                    if ((w0 & (1u << FATTR4_CHANGE)) && p + 8 <= end) {
                        memcpy(&v, p, 8);
                        w->change     = chimera_nfs_hton64(v);
                        w->got_change = true;
                        p            += 8;
                    }
                    if ((w0 & (1u << FATTR4_SIZE)) && p + 8 <= end) {
                        memcpy(&v, p, 8);
                        w->size     = chimera_nfs_hton64(v);
                        w->got_size = true;
                    }
                    w->status = 0;
                    break;
                }
            }
        }

        nfs4_cb_getattr_deliver(w);
    }

    /* The RPC is done; the request item is no longer referenced by the RPC
     * layer, so free it (the result, if any, lives in a separate RESPONSE). */
    nfs4_cb_client_unref_complete(w->chan);
    free(w);
} /* nfs4_cb_getattr_complete */

/* Runs on the delegation holder's thread: send CB_COMPOUND{[CB_SEQUENCE,]
 * CB_GETATTR} to the holder. */
static void
nfs4_cb_getattr_send(
    struct chimera_server_nfs_thread *thread,
    struct nfs4_cb_getattr           *w)
{
    struct nfs_delegation  *deleg = w->deleg;
    struct nfs4_cb_client  *chan  = deleg->client->cb_path.cb_client;
    struct evpl_rpc2_conn  *conn  = chan ? nfs4_cb_chan_conn(chan) : NULL;
    struct CB_COMPOUND4args args;
    struct nfs_cb_argop4    ops[2];
    uint32_t                attr_req = (1u << FATTR4_CHANGE) | (1u << FATTR4_SIZE);
    int                     nops     = 0;

    if (!conn) {
        /* No usable callback path: no RPC is issued, so the request item can
         * be delivered and freed right here. */
        w->status = -1;
        nfs4_cb_getattr_deliver(w);
        free(w);
        return;
    }

    memset(&args, 0, sizeof(args));
    memset(ops, 0, sizeof(ops));

    if (chan->minorversion >= 1) {
        struct CB_SEQUENCE4args *seq = &ops[nops].opcbsequence;
        ops[nops].argop = OP_CB_SEQUENCE;
        memcpy(seq->csa_sessionid, chan->sessionid, NFS4_SESSIONID_SIZE);
        seq->csa_sequenceid = atomic_fetch_add_explicit(&chan->cb_seq, 1,
                                                        memory_order_relaxed);
        seq->csa_slotid                   = 0;
        seq->csa_highest_slotid           = 0;
        seq->csa_cachethis                = 0;
        seq->num_csa_referring_call_lists = 0;
        seq->csa_referring_call_lists     = NULL;
        nops++;
    }

    ops[nops].argop                        = OP_CB_GETATTR;
    ops[nops].opcbgetattr.fh.len           = deleg->fh_len;
    ops[nops].opcbgetattr.fh.data          = deleg->fh;
    ops[nops].opcbgetattr.num_attr_request = 1;
    ops[nops].opcbgetattr.attr_request     = &attr_req;
    nops++;

    args.tag.len        = 0;
    args.tag.data       = NULL;
    args.minorversion   = chan->minorversion;
    args.callback_ident = chan->cb_ident;
    args.num_argarray   = nops;
    args.argarray       = ops;

    {
        struct nfs4_cb_path   *cb = &deleg->client->cb_path;
        struct evpl_rpc2_cred  rpc_cred;
        struct evpl_rpc2_cred *credp         = NULL;
        static const char      machinename[] = "chimera";

        if (cb->cb_sec_flavor == AUTH_SYS) {
            memset(&rpc_cred, 0, sizeof(rpc_cred));
            rpc_cred.flavor                  = EVPL_RPC2_AUTH_SYS;
            rpc_cred.authsys.uid             = cb->cb_sec_uid;
            rpc_cred.authsys.gid             = cb->cb_sec_gid;
            rpc_cred.authsys.machinename     = machinename;
            rpc_cred.authsys.machinename_len = sizeof(machinename) - 1;
            credp                            = &rpc_cred;
        }

        /* Hold the channel alive until this RPC completes.  Set before the
         * send (which may complete synchronously and run the completion that
         * reads w->chan). */
        w->chan = chan;
        nfs4_cb_client_ref(chan);

        /* Arm the timeout before issuing the RPC: if the send completes
         * synchronously (e.g. an immediate conn error), the completion cancels
         * an already-armed timer and frees `w`, so nothing touches `w` after. */
        evpl_add_oneshot_timer(thread->evpl, &w->timeout_timer,
                               nfs4_cb_getattr_timeout,
                               NFS4_CB_GETATTR_TIMEOUT_US);

        chan->cb_prog.send_call_CB_COMPOUND(&chan->cb_prog.rpc2,
                                            thread->evpl,
                                            conn,
                                            credp,
                                            &args,
                                            0, 0, NULL, 0, 0,
                                            nfs4_cb_getattr_complete,
                                            w);
    }
} /* nfs4_cb_getattr_send */

struct nfs_delegation *
nfs4_find_conflicting_write_deleg(
    struct chimera_server_nfs_thread *thread,
    const uint8_t                    *fh,
    uint16_t                          fh_len,
    uint64_t                          querying_client_id)
{
    struct chimera_vfs_state      *vfs_state = thread->vfs->vfs_state;
    uint64_t                       fh_hash;
    struct chimera_vfs_file_state *file;
    struct chimera_vfs_lease      *cur;
    struct nfs_delegation         *deleg = NULL;

    fh_hash = XXH3_64bits(fh, fh_len) & INT64_MAX;

    file = chimera_vfs_state_get(vfs_state, fh, fh_len, fh_hash, false);
    if (!file) {
        return NULL;
    }

    pthread_mutex_lock(&file->lock);
    for (cur = file->caching_leases; cur; cur = cur->next) {
        struct nfs_delegation *d;

        if (cur->owner.protocol != CHIMERA_VFS_LEASE_PROTO_NFSV4) {
            continue;
        }
        if (!(cur->mode.granted & CHIMERA_VFS_LEASE_MODE_W)) {
            continue;
        }
        if (cur->owner.client_key == querying_client_id) {
            continue; /* the querying client's own delegation */
        }
        d = cur->owner.cb_private;
        if (!d || atomic_load_explicit(&d->destroyed, memory_order_acquire)) {
            continue;
        }
        atomic_fetch_add_explicit(&d->refcount, 1, memory_order_acq_rel);
        deleg = d;
        break;
    }
    pthread_mutex_unlock(&file->lock);

    chimera_vfs_state_put(vfs_state, file);
    return deleg;
} /* nfs4_find_conflicting_write_deleg */

void
nfs4_cb_getattr(
    struct chimera_server_nfs_thread *requester_thread,
    struct nfs_delegation            *deleg,
    void                             *priv,
    nfs4_cb_getattr_resume_t          resume)
{
    struct nfs4_cb_client            *chan = deleg->client->cb_path.cb_client;
    struct chimera_server_nfs_thread *holder;
    struct nfs4_cb_getattr           *w;

    if (!chan) {
        /* No callback channel -> can't query; fall back to server attrs. */
        resume(priv, -1, false, 0, false, 0);
        nfs_state_table_release(&requester_thread->shared->nfs4_state_table,
                                deleg, NFS4_SLOT_TYPE_DELEG,
                                requester_thread->vfs_thread);
        return;
    }

    holder = chan->owner_thread;

    w                   = calloc(1, sizeof(*w));
    w->phase            = NFS4_CB_GETATTR_REQUEST;
    w->requester_thread = requester_thread;
    w->deleg            = deleg;
    w->priv             = priv;
    w->resume           = resume;

    pthread_mutex_lock(&holder->cb_recall_lock);
    w->next                  = holder->cb_getattr_queue;
    holder->cb_getattr_queue = w;
    pthread_mutex_unlock(&holder->cb_recall_lock);

    evpl_ring_doorbell(&holder->cb_doorbell);
} /* nfs4_cb_getattr */

/* Owner-thread doorbell handler: drain queued recalls and CB_GETATTR work. */
static void
nfs4_cb_doorbell_drain(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_server_nfs_thread *thread =
        (struct chimera_server_nfs_thread *) ((char *) doorbell -
                                              offsetof(struct chimera_server_nfs_thread, cb_doorbell));
    struct nfs_delegation            *queue;
    struct nfs_layout_state          *lrq;
    struct nfs4_cb_getattr           *gq;
    struct nfs4_cb_client            *tq;

    (void) evpl;

    pthread_mutex_lock(&thread->cb_recall_lock);
    queue                         = thread->cb_recall_queue;
    thread->cb_recall_queue       = NULL;
    lrq                           = thread->cb_layoutrecall_queue;
    thread->cb_layoutrecall_queue = NULL;
    gq                            = thread->cb_getattr_queue;
    thread->cb_getattr_queue      = NULL;
    tq                            = thread->cb_teardown_queue;
    thread->cb_teardown_queue     = NULL;
    pthread_mutex_unlock(&thread->cb_recall_lock);

    while (queue) {
        struct nfs_delegation *deleg = queue;
        queue               = deleg->recall_qnext;
        deleg->recall_qnext = NULL;
        nfs4_cb_recall_send(thread, deleg);
    }

    /* Layout recalls bounced here from another thread: we now own the holder's
     * backchannel conn, so nfs4_cb_recall_holder sends inline.  Drop the ref the
     * producer took to pin the layout across the bounce. */
    while (lrq) {
        struct nfs_layout_state *h = lrq;
        lrq             = h->recall_qnext;
        h->recall_qnext = NULL;
        nfs4_cb_recall_holder(thread, h);
        nfs_layout_state_put(h);
    }

    /* Deferred-op resumes bounced to this (their home) thread by a recall that
     * completed on another thread.  Re-drives the op where its iovecs live. */
    nfs4_cb_drain_resume_queue(thread);

    while (gq) {
        struct nfs4_cb_getattr *w = gq;
        gq      = w->next;
        w->next = NULL;

        if (w->phase == NFS4_CB_GETATTR_REQUEST) {
            /* This thread owns the holder's callback channel. */
            nfs4_cb_getattr_send(thread, w);
        } else {
            /* Back on the requester's thread: deliver the result and release
             * the delegation ref taken for the query. */
            w->resume(w->priv, w->status, w->got_change, w->change,
                      w->got_size, w->size);
            nfs_state_table_release(&thread->shared->nfs4_state_table,
                                    w->deleg, NFS4_SLOT_TYPE_DELEG,
                                    thread->vfs_thread);
            free(w);
        }
    }

    /* Free callback channels whose last reference was dropped off this (their
     * owner) thread.  Done last so any send above sees a consistent state. */
    while (tq) {
        struct nfs4_cb_client *chan = tq;
        tq                  = chan->teardown_next;
        chan->teardown_next = NULL;
        nfs4_cb_client_destroy(chan);
    }
} /* nfs4_cb_doorbell_drain */

/* vfs_state break callback, wired onto every delegation's CACHING lease.
 * Invoked (under file->lock release) when a conflicting acquirer needs the
 * delegation dropped.  Kicks the CB_RECALL; the lease stays BREAKING until
 * the client's DELEGRETURN releases it (or the recall fails and we revoke). */
void
nfs4_delegation_break_cb(
    struct chimera_vfs_lease *lease,
    uint8_t                   needed_mode,
    void                     *private_data)
{
    struct nfs_delegation *deleg = private_data;

    (void) lease;
    (void) needed_mode;

    nfs4_cb_recall(deleg);
} /* nfs4_delegation_break_cb */

/* Marshal a CB_RECALL send for `deleg` to its callback channel's owner thread
 * (the doorbell drain calls nfs4_cb_recall_send).  Takes a transient ref so the
 * delegation survives until the recall completes.  Caller is responsible for the
 * cb_recall_state bookkeeping; this does no CAS, so it can re-send a recall that
 * is already RECALLING (used by the rebind resend path).  No-op if no callback
 * channel currently exists. */
static void
nfs4_cb_recall_enqueue(struct nfs_delegation *deleg)
{
    struct nfs4_cb_client            *chan = deleg->client->cb_path.cb_client;
    struct chimera_server_nfs_thread *owner;

    if (!chan) {
        return;
    }

    owner = chan->owner_thread;

    atomic_fetch_add_explicit(&deleg->refcount, 1, memory_order_acq_rel);

    pthread_mutex_lock(&owner->cb_recall_lock);
    deleg->recall_qnext    = owner->cb_recall_queue;
    owner->cb_recall_queue = deleg;
    pthread_mutex_unlock(&owner->cb_recall_lock);

    evpl_ring_doorbell(&owner->cb_doorbell);
} /* nfs4_cb_recall_enqueue */

void
nfs4_cb_recall(struct nfs_delegation *deleg)
{
    uint8_t expected = NFS4_DELEG_ACTIVE;

    /* Only the first recall on a delegation does work. */
    if (!atomic_compare_exchange_strong_explicit(
            &deleg->cb_recall_state, &expected, NFS4_DELEG_RECALLING,
            memory_order_acq_rel, memory_order_acquire)) {
        return;
    }

    if (!deleg->client->cb_path.cb_client) {
        /* No channel was ever established (delegation should not have been
         * granted, but be defensive): revoke so the breaker proceeds. */
        if (deleg->lease_held) {
            chimera_vfs_lease_revoke(&deleg->lease);
        }
        return;
    }

    nfs4_cb_recall_enqueue(deleg);
} /* nfs4_cb_recall */

void
nfs4_cb_resend_recalls_on_rebind(
    struct chimera_server_nfs_thread *thread,
    struct nfs_client                *client,
    struct nfs_request               *req)
{
    struct nfs_delegation  *deleg;
    struct nfs_delegation **pending  = NULL;
    size_t                  count    = 0;
    size_t                  capacity = 0;

    if (!client) {
        return;
    }

    /* Fast path: collect delegations with an outstanding recall -- a still-
     * BREAKING backing lease that has not been returned.  In the common case --
     * and always when delegations are disabled -- there are none, so this is a
     * cheap scan-and-return.  Pin each survivor with a transient ref so it
     * cannot be freed once we drop client->lock.  We deliberately do NOT filter
     * on cb_recall_state: the prior recall's completion (which moves the deleg
     * back to ACTIVE) runs asynchronously on the channel owner thread and races
     * this CREATE_SESSION, so the deleg may still read RECALLING here -- keying
     * off the lease's BREAK state instead makes the resend independent of that
     * race.  (Reading lease.break_state under client->lock, without the file
     * lock, matches the existing nfs_deleg_recall_timeout_check pattern -- a
     * benign racy read of an enum used only to decide whether to retry.) */
    pthread_mutex_lock(&client->lock);
    LL_FOREACH2(client->delegations, deleg, next_in_client)
    {
        if (atomic_load_explicit(&deleg->cb_recall_state,
                                 memory_order_acquire) == NFS4_DELEG_RETURNED) {
            continue;
        }
        if (!deleg->lease_held ||
            deleg->lease.break_state != CHIMERA_VFS_BREAK_BREAKING) {
            continue;
        }

        if (count == capacity) {
            size_t                  new_cap = capacity ? capacity * 2 : 4;
            struct nfs_delegation **np      =
                realloc(pending, new_cap * sizeof(*np));
            chimera_nfs_abort_if(np == NULL, "recall resend list realloc failed");
            pending  = np;
            capacity = new_cap;
        }
        atomic_fetch_add_explicit(&deleg->refcount, 1, memory_order_acq_rel);
        pending[count++] = deleg;
    }
    pthread_mutex_unlock(&client->lock);

    if (count == 0) {
        free(pending);
        return;
    }

    /* The stale callback channel still references the destroyed session (whose
     * backchannel_conn is now NULL), so it cannot carry a recall.  Tear it down
     * and rebuild against the new session: nfs4_client_set_cb_path already reset
     * the path to NFS4_CB_UNINIT, so ensure_probe binds the new backchannel and
     * (4.1) marks it UP without a CB_NULL round trip.  Done outside client->lock
     * to avoid a client->lock -> cb_recall_lock ordering. */
    if (client->cb_path.cb_client) {
        nfs4_cb_path_teardown(&client->cb_path, false);
    }

    /* Force the path back to UNINIT before rebuilding.  The failed first
     * recall's completion may have stored NFS4_CB_DOWN (it races this rebind),
     * and ensure_probe refuses to (re)build a DOWN path -- which would silently
     * drop the resend.  The teardown above already cleared cb_client, so a later
     * stale completion can no longer match-and-re-DOWN the path. */
    atomic_store_explicit(&client->cb_path.cb_state, NFS4_CB_UNINIT,
                          memory_order_release);

    if (nfs4_cb_ensure_probe(thread, client, req)) {
        /* Re-drive each outstanding recall over the freshly-bound channel.  Use
         * the CAS-free enqueue (not nfs4_cb_recall) and stamp RECALLING
         * directly: the deleg may legitimately already be RECALLING here (the
         * prior recall's completion has not run yet), and the one-shot CAS in
         * nfs4_cb_recall would silently drop the re-send in that case -- exactly
         * the race that left DSESS9003 hanging.  A duplicate CB_RECALL (if the
         * prior one is still in flight) is protocol-legal. */
        for (size_t i = 0; i < count; i++) {
            atomic_store_explicit(&pending[i]->cb_recall_state,
                                  NFS4_DELEG_RECALLING, memory_order_release);
            nfs4_cb_recall_enqueue(pending[i]);
        }
    }
    /* else: no usable backchannel even now -- leave the recalls pending; the
     * vfs_state recall deadline / client lease expiry will revoke. */

    for (size_t i = 0; i < count; i++) {
        nfs_state_table_release(&thread->shared->nfs4_state_table, pending[i],
                                NFS4_SLOT_TYPE_DELEG, thread->vfs_thread);
    }
    free(pending);
} /* nfs4_cb_resend_recalls_on_rebind */

void
nfs4_cb_conn_lost(struct nfs4_cb_client *chan)
{
    /* Runs on the conn's owner thread, which is the only thread that sends on
     * this channel, so clearing the conn here cannot race a send. */
    chan->conn = NULL;
    if (chan->cb_path) {
        atomic_store_explicit(&chan->cb_path->cb_state, NFS4_CB_DOWN,
                              memory_order_release);
    }
} /* nfs4_cb_conn_lost */

void
nfs4_cb_path_teardown(
    struct nfs4_cb_path *cb,
    bool                 synchronous)
{
    struct nfs4_cb_client *chan = cb->cb_client;

    if (!chan) {
        return;
    }
    cb->cb_client = NULL;

    /* 4.0: if the outbound conn is still live, clear its back-pointer so a
     * later disconnect notify does not dereference the (about-to-be-freed)
     * channel.  We do not disconnect it; evpl drains/frees it at thread
     * teardown, and disconnecting here would race that teardown.  Skipped at
     * server shutdown: the rpc2 thread (and this conn) has already been freed
     * by the threadpool teardown, so chan->conn is stale. */
    if (!synchronous && chan->owns_conn && chan->conn) {
        evpl_rpc2_conn_set_private_data(chan->conn, NULL);
    }
    chan->cb_path = NULL;

    /* Drop the owning path's reference.  If CB RPCs are still in flight they
     * hold the remaining references, and their completions (which run on the
     * owner thread) free the channel once the last reply has dispatched.  If
     * we are the last reference we may be running on a different thread (the
     * lease sweeper), so hand the free to the owner thread via its cb_doorbell
     * rather than freeing here, where it could race in-flight reply handling
     * on the owner thread.  The 4.1 session ref is released by
     * nfs4_cb_client_destroy() at the actual free. */
    if (nfs4_cb_client_unref(chan)) {
        if (synchronous) {
            /* Server shutdown: the threadpool is already gone, so the owner
             * thread no longer exists to drain a marshaled free, and no
             * concurrent reply dispatch or queued recall can race us (we run
             * single-threaded on the main thread).  Free in-line. */
            nfs4_cb_client_destroy(chan);
        } else {
            struct chimera_server_nfs_thread *owner = chan->owner_thread;

            pthread_mutex_lock(&owner->cb_recall_lock);
            chan->teardown_next      = owner->cb_teardown_queue;
            owner->cb_teardown_queue = chan;
            pthread_mutex_unlock(&owner->cb_recall_lock);

            evpl_ring_doorbell(&owner->cb_doorbell);
        }
    }
} /* nfs4_cb_path_teardown */

/* ---------------------------------------------------------------------- */
/* Per-thread setup                                                       */
/* ---------------------------------------------------------------------- */

void
nfs4_cb_thread_init(struct chimera_server_nfs_thread *thread)
{
    pthread_mutex_init(&thread->cb_recall_lock, NULL);
    thread->cb_recall_queue   = NULL;
    thread->cb_getattr_queue  = NULL;
    thread->cb_teardown_queue = NULL;
    evpl_add_doorbell(thread->evpl, &thread->cb_doorbell, nfs4_cb_doorbell_drain);
    thread->cb_doorbell_armed = 1;
} /* nfs4_cb_thread_init */

void
nfs4_cb_thread_destroy(struct chimera_server_nfs_thread *thread)
{
    if (thread->cb_doorbell_armed) {
        evpl_remove_doorbell(thread->evpl, &thread->cb_doorbell);
        thread->cb_doorbell_armed = 0;
    }
    pthread_mutex_destroy(&thread->cb_recall_lock);
} /* nfs4_cb_thread_destroy */
