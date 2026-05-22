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

/* portmap_xdr.h (pulled in via nfs_common.h below) #defines these RPC
 * protocol constants, colliding with <netinet/in.h>'s IPPROTO_* enum-macros.
 * This file does not use them, so drop the system macros before the XDR
 * headers redefine them. */
#undef IPPROTO_TCP
#undef IPPROTO_UDP

#include "evpl/evpl.h"
#include "evpl/evpl_rpc2.h"

#include "nfs4_callback.h"
#include "nfs4_state.h"
#include "nfs4_session.h"
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

    rc = connect(fd, (struct sockaddr *) &sin, sizeof(sin));
    if (rc == 0) {
        ok = true;
    } else if (errno == EINPROGRESS) {
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

    free(ctx);
} /* nfs4_cb_null_complete */

/*
 * Establish the callback channel for `client` (if not already present) and
 * fire a CB_NULL probe.  Runs on `thread`, which owns the client's fore
 * connection.
 */
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
    chan->owner_thread = thread;
    chan->minorversion = cb->cb_minorversion;
    chan->cb_ident     = cb->cb_ident;

    /* Private program copy carrying the client's transient callback program
     * number; program_data must point at this stable copy for async reply
     * dispatch. */
    chan->cb_prog                   = thread->shared->nfs_v4_cb;
    chan->cb_prog.rpc2.program      = cb->cb_program;
    chan->cb_prog.rpc2.program_data = &chan->cb_prog;
    atomic_init(&chan->cb_seq, 1);

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
    } else {
        /* 4.1+: the backchannel rides the session's fore conn. */
        struct nfs4_session *session = req ? req->session : NULL;

        if (!session || !session->nfs4_session_backchannel_conn) {
            atomic_store_explicit(&cb->cb_state, NFS4_CB_DOWN,
                                  memory_order_release);
            free(chan);
            return;
        }
        chan->conn      = session->nfs4_session_backchannel_conn;
        chan->owns_conn = 0;
        memcpy(chan->sessionid, session->nfs4_session_id, NFS4_SESSIONID_SIZE);
    }

    cb->cb_client = chan;
    atomic_store_explicit(&cb->cb_state, NFS4_CB_PROBING, memory_order_release);

    ctx         = calloc(1, sizeof(*ctx));
    ctx->client = client;

    chan->cb_prog.send_call_CB_NULL(&chan->cb_prog.rpc2,
                                    thread->evpl,
                                    chan->conn,
                                    NULL,
                                    0, 0, 0,
                                    nfs4_cb_null_complete,
                                    ctx);
} /* nfs4_cb_channel_open */

bool
nfs4_cb_ensure_probe(
    struct chimera_server_nfs_thread *thread,
    struct nfs_client                *client,
    struct nfs_request               *req)
{
    uint8_t state;

    if (!client) {
        return false;
    }

    state = atomic_load_explicit(&client->cb_path.cb_state, memory_order_acquire);

    switch (state) {
        case NFS4_CB_UP:
            return true;
        case NFS4_CB_PROBING:
        case NFS4_CB_DOWN:
            return false;
        case NFS4_CB_UNINIT:
        default:
            /* No callback program captured at all -> client never asked to be
             * able to receive callbacks; cannot delegate. */
            if (client->cb_path.cb_program == 0) {
                return false;
            }
            nfs4_cb_channel_open(thread, client, req);
            return false;
    } /* switch */
} /* nfs4_cb_ensure_probe */

/* ---------------------------------------------------------------------- */
/* CB_RECALL                                                              */
/* ---------------------------------------------------------------------- */

struct nfs4_cb_recall_ctx {
    struct nfs_delegation            *deleg;
    struct chimera_server_nfs_thread *thread;
};

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

    /* RPC-level failure or a callback compound error means the client cannot
     * (or will not) return the delegation in-band.  Revoke the backing lease
     * so the conflicting acquirer makes progress; the delegation stateid is
     * left to be torn down by DELEGRETURN/close/expiry. */
    if (status != 0 || (reply && reply->status != NFS4_OK)) {
        if (deleg->lease_held) {
            chimera_vfs_lease_revoke(&deleg->lease);
        }
        atomic_store_explicit(&deleg->client->cb_path.cb_state, NFS4_CB_DOWN,
                              memory_order_release);
    }

    /* Drop the transient recall ref. */
    nfs_state_table_release(&thread->shared->nfs4_state_table, deleg,
                            NFS4_SLOT_TYPE_DELEG, thread->vfs_thread);
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
    struct nfs4_cb_recall_ctx *ctx;
    struct CB_COMPOUND4args    args;
    struct nfs_cb_argop4       ops[2];
    struct stateid4            sid;
    int                        nops = 0;

    if (!chan || !chan->conn) {
        /* No path: revoke so the conflicting acquirer proceeds, drop ref. */
        if (deleg->lease_held) {
            chimera_vfs_lease_revoke(&deleg->lease);
        }
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
                                            chan->conn,
                                            credp,
                                            &args,
                                            0, 0, 0,
                                            nfs4_cb_recall_complete,
                                            ctx);
    }
} /* nfs4_cb_recall_send */

/* Owner-thread doorbell handler: drain queued recalls and send them. */
static void
nfs4_cb_doorbell_drain(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_server_nfs_thread *thread =
        (struct chimera_server_nfs_thread *) ((char *) doorbell -
                                              offsetof(struct chimera_server_nfs_thread, cb_doorbell));
    struct nfs_delegation            *queue;

    (void) evpl;

    pthread_mutex_lock(&thread->cb_recall_lock);
    queue                   = thread->cb_recall_queue;
    thread->cb_recall_queue = NULL;
    pthread_mutex_unlock(&thread->cb_recall_lock);

    while (queue) {
        struct nfs_delegation *deleg = queue;
        queue               = deleg->recall_qnext;
        deleg->recall_qnext = NULL;
        nfs4_cb_recall_send(thread, deleg);
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

void
nfs4_cb_recall(struct nfs_delegation *deleg)
{
    struct nfs4_cb_client            *chan;
    struct chimera_server_nfs_thread *owner;
    uint8_t                           expected = NFS4_DELEG_ACTIVE;

    /* Only the first recall on a delegation does work. */
    if (!atomic_compare_exchange_strong_explicit(
            &deleg->cb_recall_state, &expected, NFS4_DELEG_RECALLING,
            memory_order_acq_rel, memory_order_acquire)) {
        return;
    }

    chan = deleg->client->cb_path.cb_client;
    if (!chan) {
        /* No channel was ever established (delegation should not have been
         * granted, but be defensive): revoke so the breaker proceeds. */
        if (deleg->lease_held) {
            chimera_vfs_lease_revoke(&deleg->lease);
        }
        return;
    }

    owner = chan->owner_thread;

    /* Take a transient ref so the delegation survives until the recall
     * completes on the owner thread. */
    atomic_fetch_add_explicit(&deleg->refcount, 1, memory_order_acq_rel);

    pthread_mutex_lock(&owner->cb_recall_lock);
    deleg->recall_qnext    = owner->cb_recall_queue;
    owner->cb_recall_queue = deleg;
    pthread_mutex_unlock(&owner->cb_recall_lock);

    evpl_ring_doorbell(&owner->cb_doorbell);
} /* nfs4_cb_recall */

void
nfs4_cb_path_teardown(struct nfs4_cb_path *cb)
{
    struct nfs4_cb_client *chan = cb->cb_client;

    if (!chan) {
        return;
    }
    cb->cb_client = NULL;

    /* Free only the channel bookkeeping.  The outbound 4.0 connection is owned
     * by its evpl thread and is drained/freed by evpl at thread teardown;
     * disconnecting it here would race that teardown (the conn may already be
     * gone).  4.1 channels borrow the session's fore conn and never own it. */
    free(chan);
} /* nfs4_cb_path_teardown */

/* ---------------------------------------------------------------------- */
/* Per-thread setup                                                       */
/* ---------------------------------------------------------------------- */

void
nfs4_cb_thread_init(struct chimera_server_nfs_thread *thread)
{
    pthread_mutex_init(&thread->cb_recall_lock, NULL);
    thread->cb_recall_queue = NULL;
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
