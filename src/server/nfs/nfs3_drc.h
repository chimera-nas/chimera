// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "common/thread.h"
#include <stdatomic.h>
#include <stdint.h>

#include <uthash.h>

/* nfs3_xdr.h defines xdr_iovec (used in orig_dispatch below) and then pulls in
 * evpl_rpc2_program.h, which uses it. */
#include "nfs3_xdr.h"

struct chimera_server_nfs_thread;
struct chimera_server_nfs_shared;
struct evpl;
struct evpl_rpc2_conn;
struct evpl_rpc2_encoding;
struct evpl_rpc2_cred;

/*
 * NFSv3 duplicate-request cache (DRC), gated by server.nfs3_drc (default off).
 *
 * NFSv3 has no sessions or client ids, so -- following the classic server DRC
 * pattern -- a request is identified by {client address, xid, procedure,
 * request checksum}.  The address has its ephemeral port stripped so it stays
 * stable across the reconnect a retransmit rides in on; the checksum guards
 * against a reused xid mapping to a different call (a hit then means the same
 * client re-presenting a byte-identical request).  The checksum covers the
 * caller's credential too, because the address is shared by everything behind
 * a NAT or on a multi-user host and two users' identical requests would
 * otherwise be one key.
 *
 * Only non-idempotent procedures are cached (CREATE/MKDIR/REMOVE/RENAME/...);
 * re-executing an idempotent op is harmless, so those bypass the cache.
 *
 * The cache is installed as a wrapper around the generated NFS_V3 call
 * dispatcher (see nfs3_drc_install): on a hit it replays the cached reply and
 * skips execution; on a miss it arms the rpc2 reply-capture hook and forwards
 * to the real dispatcher.  When kv_module is persistent the captured reply is
 * also written through to the KV store keyed by the same identity, and a
 * cold-start scan repopulates the in-memory cache, so a retransmit that arrives
 * after a server restart still replays instead of re-executing.
 */

/* Longest client address string a key embeds (see CHIMERA_KV_NFS3_ADDR_MAX). */
#define NFS3_DRC_ADDR_MAX       48u

/* Per-entry reply cap and total in-memory (and therefore on-KV) byte budget. */
#define NFS3_DRC_MAX_REPLY_SIZE (64u * 1024u)
#define NFS3_DRC_MAX_BYTES      (4u * 1024u * 1024u)

/* Fixed-layout identity used as the uthash key.  memset to zero before filling
 * so the trailing pad and any unused addr bytes hash deterministically. */
struct nfs3_drc_keybuf {
    uint8_t  addr[NFS3_DRC_ADDR_MAX];
    uint8_t  addr_len;
    uint8_t  pad[3];
    uint32_t proc;
    uint32_t xid;
    uint64_t cksum;
};

/* Ceilings on the two kinds of state a request in progress pins.  An INPROG
 * entry holds no reply bytes, so the byte budget above does not bound them;
 * a held waiter pins a whole request (its dbuf and decoded arguments). */
#define NFS3_DRC_MAX_INPROG   16384u
#define NFS3_DRC_MAX_HELD     4096u

/* How long past the NFSv4 lease time a retransmit may be held.  An NFSv3
 * REMOVE can wait out an NFSv4 delegation recall, which lasts up to a lease,
 * and one held retransmit should normally cover all of it. */
#define NFS3_DRC_HOLD_SLACK_S 10u

enum nfs3_drc_state {
    /* The reply is cached; a retransmit replays it. */
    NFS3_DRC_DONE   = 0,
    /* The original is still executing.  No reply exists yet, and a
     * retransmit must neither re-execute it nor be answered from nothing:
     * it is held as a waiter, or dropped for the client to send again. */
    NFS3_DRC_INPROG = 1,
};

/* A cached reply.  Shared by its entry and every waiter answered from it, so a
 * completion hands it to any number of waiters without copying it -- and
 * without allocating under the cache lock -- and an entry evicted while a
 * waiter still has it queued does not pull it out from under the waiter. */
struct nfs3_drc_reply {
    atomic_uint refs;
    uint32_t    len;
    uint8_t     data[];
};

struct nfs3_drc_waiter;
struct nfs3_drc_delay_ctx;

struct nfs3_drc_entry {
    struct nfs3_drc_keybuf  key;
    struct nfs3_drc_reply  *reply;   /* DONE only */
    uint64_t                ts;      /* INPROG: start time; DONE: capture time (ns) */
    enum nfs3_drc_state state;
    /* INPROG only.  The connection the original arrived on, compared and never
     * dereferenced: the original's reply already goes there, so a retransmit
     * on it is redundant.  The original's request keeps the connection alive
     * (and so the pointer unique) for as long as the entry is in progress. */
    const void             *origin_conn;
    struct nfs3_drc_waiter *waiters; /* INPROG only: held retransmits, one per connection */
    UT_hash_handle          hh;
};

enum nfs3_drc_waiter_state {
    /* Linked on its entry, waiting for the original's reply. */
    NFS3_DRC_WAITER_HELD   = 0,
    /* Detached from its entry and queued to its own thread's mailbox, with a
     * reply to send or (reply == NULL) to be discarded. */
    NFS3_DRC_WAITER_QUEUED = 1,
};

/*
 * A retransmit held while its original executes.
 *
 * Bump-allocated from the held request's own dbuf, so it lives exactly as long
 * as the request: it ends when its thread replies or discards the request, and
 * must not be touched after that.  Only the thread that received the request
 * (the owner of its connection) ever answers, discards or frees it; another
 * thread completing the original only moves it, under the cache lock, from its
 * entry to the owner's mailbox and rings the owner's doorbell.
 */
struct nfs3_drc_waiter {
    struct chimera_server_nfs_thread *thread;
    struct evpl_rpc2_conn            *conn;     /* identity only once queued */
    struct evpl_rpc2_encoding        *encoding;
    struct nfs3_drc_entry            *entry;    /* HELD only */
    struct nfs3_drc_reply            *reply;    /* QUEUED: a reference, or NULL */
    uint64_t                          deadline; /* ns, nfs_lease_now_ns() clock */
    enum nfs3_drc_waiter_state state;
    struct nfs3_drc_waiter           *prev, *next;   /* entry->waiters */
    struct nfs3_drc_waiter           *tprev, *tnext; /* thread->drc_held */
    struct nfs3_drc_waiter           *qnext; /* thread->drc_mbox */
};

/* What the cache did with a request, for chimera_nfs3_drc{event=...}.  Every
 * event is counted under the cache lock. */
enum nfs3_drc_event {
    NFS3_DRC_EV_RESERVED = 0,                /* first arrival, entry now in progress */
    NFS3_DRC_EV_UNTRACKED,                   /* executed without an entry (ceiling) */
    NFS3_DRC_EV_REPLAYED,                    /* answered from a cached reply */
    NFS3_DRC_EV_HELD,                        /* retransmit held on an entry in progress */
    NFS3_DRC_EV_ANSWERED,                    /* held retransmit given the original's reply */
    NFS3_DRC_EV_ABANDONED,                   /* original ended with no cacheable reply */
    NFS3_DRC_EV_DROP_ORIGIN_CONN,    /* retransmit on the original's connection */
    NFS3_DRC_EV_DROP_ALREADY_WAITING,/* its connection already has a waiter */
    NFS3_DRC_EV_DROP_CEILING,                /* NFS3_DRC_MAX_HELD reached */
    NFS3_DRC_EV_DROP_NO_WAITER,      /* no room for a waiter in its dbuf */
    NFS3_DRC_EV_DROP_DEADLINE,       /* held past its deadline */
    NFS3_DRC_EV_DROP_DISCONNECT,     /* its connection closed while held */
    NFS3_DRC_EV_DROP_ABANDONED,      /* its original was abandoned */
    NFS3_DRC_EV_DROP_SHUTDOWN,       /* the server thread stopped */
    NFS3_DRC_EV_COUNT
};

struct prometheus_metrics;
struct prometheus_counter;
struct prometheus_counter_series;
struct prometheus_counter_instance;
struct prometheus_gauge;
struct prometheus_gauge_series;
struct prometheus_gauge_instance;

struct nfs3_drc_metrics {
    struct prometheus_metrics          *registry;
    struct prometheus_counter          *counter;
    struct prometheus_counter_series   *series[NFS3_DRC_EV_COUNT];
    struct prometheus_counter_instance *events[NFS3_DRC_EV_COUNT];
    struct prometheus_gauge            *gauge;
    struct prometheus_gauge_series     *inprog_series;
    struct prometheus_gauge_series     *held_series;
    struct prometheus_gauge_instance   *inprog;
    struct prometheus_gauge_instance   *held;
};

/* One per client address this instance has hydrated from the KV store (loaded
 * that client's whole reply band on first contact).  Once an address is here,
 * an in-memory miss for it is DEFINITIVE -- no per-op KV read.  Keyed by the
 * zero-padded addr + addr_len. */
struct nfs3_drc_hydra {
    uint8_t        addr[NFS3_DRC_ADDR_MAX];
    uint8_t        addr_len;
    uint8_t        pad[3];
    UT_hash_handle hh;
};

/* The connectionless duplicate-request cache.  Address keying is inherent to
 * NFSv3, which has no client identity of its own; NFSv4.0 does have one and
 * keys its cache per connection instead (nfs4_v40_drc.{c,h}). */
struct nfs3_drc {
    evpl_mutex_t            lock;
    struct nfs3_drc_entry  *table;           /* uthash, FIFO eviction order */
    struct nfs3_drc_hydra  *hydrated;        /* uthash: addrs hydrated from KV  */
    uint64_t                bytes;
    uint32_t                inprog;          /* entries in NFS3_DRC_INPROG       */
    uint32_t                held;            /* waiters in NFS3_DRC_WAITER_HELD  */
    uint64_t                hold_ns;         /* how long a waiter may be held    */
    uint64_t                last_stuck_report; /* ns; rate-limits the stuck log   */
    /* Event counts, kept under the lock; mirrored into `metrics` when the
     * server has a registry, and read directly by the unit tests. */
    uint64_t                events[NFS3_DRC_EV_COUNT];
    struct nfs3_drc_metrics metrics;
    /* Test only (CHIMERA_NFS3_DRC_TEST_DELAY_MS): park every request the cache
     * executes this long before running it, so a retransmit can be made to
     * land while its original is deterministically still in progress. */
    uint32_t                test_delay_ms;
    uint8_t                 kv_type;         /* CHIMERA_KV_TYPE_NFS3_REPLY         */
    int                     persistence_disabled;
    /* The generated dispatcher we wrap; NULL until installed. */
    int                     (*orig_dispatch)(
        struct evpl               *evpl,
        struct evpl_rpc2_conn     *conn,
        struct evpl_rpc2_encoding *encoding,
        uint32_t                   proc,
        void                      *program_data,
        struct evpl_rpc2_cred     *cred,
        xdr_iovec                 *iov,
        int                        niov,
        int                        length,
        void                      *private_data);
};

/* Wrap the NFS_V3 program's call dispatcher with the DRC (call once at server
 * init, after NFS_V3_init, only when nfs3_drc is enabled). */
void
nfs3_drc_install(
    struct chimera_server_nfs_shared *shared);

/* How long a retransmit may be held: the NFSv4 lease time plus
 * NFS3_DRC_HOLD_SLACK_S.  Call once the lease time is known, before the
 * server threads start. */
void
nfs3_drc_set_lease_time(
    struct nfs3_drc *drc,
    uint32_t         lease_time_s);

/* Initialize / tear down the in-memory cache.  kv_type picks the KV band. */
void
nfs3_drc_init(
    struct nfs3_drc *drc,
    uint8_t          kv_type);

void
nfs3_drc_destroy(
    struct nfs3_drc *drc);

/* Per-thread waiter machinery: the mailbox doorbell other threads ring when an
 * original this thread's waiters are held on completes, and the timer that
 * drops waiters held past their deadline.  A no-op unless the cache is
 * installed.  destroy discards every waiter the thread still holds. */
void
nfs3_drc_thread_init(
    struct chimera_server_nfs_thread *thread);

void
nfs3_drc_thread_destroy(
    struct chimera_server_nfs_thread *thread);

/* Discard every waiter held for `conn`.  Called from the rpc2 DISCONNECTED
 * notification on the connection's own thread, before rpc2 frees the
 * connection -- after that a held request could no longer be answered or
 * released safely. */
void
nfs3_drc_conn_close(
    struct chimera_server_nfs_thread *thread,
    struct evpl_rpc2_conn            *conn);

/* ----------------------------------------------------------------------- *
*  Connectionless-DRC core.  Some of this (the checksum in particular) is   *
*  protocol-agnostic and reused by the NFSv4.0 per-connection cache.        *
* ----------------------------------------------------------------------- */

/* Serve one cacheable request through `drc`: replay from cache, or (when this
 * client's band is not yet hydrated) load it from the KV store first, then
 * replay-or-execute.  The caller has already built `key` (proc + xid +
 * checksum + client address) and decided the op is cacheable.  Returns the
 * dispatch result (0 = handled, possibly after an async hydrate). */
int
nfs3_drc_serve(
    struct nfs3_drc              *drc,
    const struct nfs3_drc_keybuf *key,
    struct evpl                  *evpl,
    struct evpl_rpc2_conn        *conn,
    struct evpl_rpc2_encoding    *encoding,
    uint32_t                      proc,
    void                         *program_data,
    struct evpl_rpc2_cred        *cred,
    xdr_iovec                    *iov,
    int                           niov,
    int                           length,
    void                         *private_data);

/* 64-bit FNV-1a over a request's iovecs -- the key's checksum field. */
uint64_t
nfs3_drc_checksum_iov(
    const xdr_iovec *iov,
    int              niov);

/* Fold the caller's identity into a checksum.  The iovecs above begin after the
 * RPC header and so exclude the credential; without this, two users' identical
 * requests share a key and one is answered from the other's entry.  Shared with
 * the NFSv4.0 cache, which has the same gap for the same reason. */
uint64_t
nfs3_drc_checksum_cred(
    uint64_t                     h,
    const struct evpl_rpc2_cred *cred);

/* Source IP with the ephemeral port stripped; writes addr, returns its len. */
uint8_t
nfs3_drc_client_addr(
    struct evpl_rpc2_conn *conn,
    uint8_t               *out);

/* ----------------------------------------------------------------------- *
*  Exposed for unit tests (test_nfs_persist).                             *
* ----------------------------------------------------------------------- */

/* 64-bit FNV-1a over a request body -- the key's checksum field. */
uint64_t
nfs3_drc_checksum(
    const void *data,
    uint32_t    len);

/* Is this NFSv3 procedure number non-idempotent (and therefore cached)? */
int
nfs3_drc_proc_cacheable(
    uint32_t proc);

/* Reply value layout: magic(4) ts(8) len(4) body[len].  serialize returns
 * bytes written (0 on overflow); parse points data at the in-buffer body. */
#define NFS3_DRC_VALUE_HDR_LEN 16u

uint32_t
nfs3_drc_value_serialize(
    uint8_t    *buf,
    uint32_t    buf_size,
    uint64_t    ts,
    const void *body,
    uint32_t    body_len);

int
nfs3_drc_value_parse(
    const uint8_t  *buf,
    uint32_t        len,
    uint64_t       *out_ts,
    const uint8_t **out_body,
    uint32_t       *out_body_len);

/* In-memory cache primitives (lock taken internally). */

/* Insert a completed reply, as the cold-start KV reload does.  An entry that
 * already exists -- cached or in progress -- is kept: the first reply for an
 * identity is the one every retransmit gets. */
void
nfs3_drc_cache_insert(
    struct nfs3_drc              *drc,
    const struct nfs3_drc_keybuf *key,
    const void                   *body,
    uint32_t                      body_len,
    uint64_t                      ts);

/* Returns 1 and fills out_buf (malloc'd, caller frees) + out_len on a hit.
 * An entry still in progress is not a hit. */
int
nfs3_drc_cache_lookup(
    struct nfs3_drc              *drc,
    const struct nfs3_drc_keybuf *key,
    uint8_t                     **out_buf,
    uint32_t                     *out_len);

enum nfs3_drc_reserve_result {
    /* No entry: one was created in progress for the caller, who must now
     * execute the request and end it -- the request-end hook abandons the
     * entry if no reply was captured by then. */
    NFS3_DRC_RESERVED  = 0,
    /* No entry, and none created (too many in progress): execute untracked. */
    NFS3_DRC_UNTRACKED = 1,
    /* Cached: *out_reply holds a reference to the reply to replay, which the
     * caller drops with nfs3_drc_reply_put. */
    NFS3_DRC_HIT       = 2,
    /* In progress, and the caller's waiter is now held on it. */
    NFS3_DRC_HELD      = 3,
    /* In progress, and the request must go unanswered: it arrived on the
     * original's own connection, its connection already has a waiter, the
     * held ceiling is reached, or no waiter was given. */
    NFS3_DRC_DROP      = 4,
};

/* A reply holding a copy of body, with one reference (NULL on allocation
 * failure), and the drop of one reference. */
struct nfs3_drc_reply *
nfs3_drc_reply_alloc(
    const void *body,
    uint32_t    body_len);

void
nfs3_drc_reply_put(
    struct nfs3_drc_reply *reply);

/*
 * Look up `key` and, in the same critical section, act on what is there:
 * reserve it, replay it, or hold `waiter` on it.  Doing all three under one
 * lock is what closes the window #1375 describes -- a lookup that missed and
 * an insert after the handler ran left a retransmit nothing to find.
 *
 * `conn` is the connection the request arrived on, used as an identity only.
 * `waiter` (may be NULL, which turns HELD into DROP) must have its thread,
 * conn and encoding filled in; the cache fills the rest.
 */
enum nfs3_drc_reserve_result
nfs3_drc_cache_reserve(
    struct nfs3_drc              *drc,
    const struct nfs3_drc_keybuf *key,
    const void                   *conn,
    struct nfs3_drc_waiter       *waiter,
    struct nfs3_drc_reply       **out_reply);

/* The original's reply was captured.  Fill its in-progress entry and hand the
 * reply to every held waiter; a cached entry is left alone (the first reply
 * wins).  The cache takes its own references to `reply`; the caller keeps
 * its one.  Returns 1 if the reply was stored -- and so should be persisted --
 * and 0 if an earlier one is kept.  Evicted keys are reported as by the KV
 * write-through. */
int
nfs3_drc_cache_complete(
    struct nfs3_drc              *drc,
    const struct nfs3_drc_keybuf *key,
    struct nfs3_drc_reply        *reply,
    uint64_t                      ts,
    struct nfs3_drc_keybuf       *evicted,
    int                           max_evicted,
    int                          *nevicted);

/* The original ended without a reply that can be cached (it was rejected
* before its handler ran, its reply could not be captured, or it was
* discarded).  Drop its in-progress entry and release every waiter held on it
* unanswered: they were never going to receive a cached reply, and executing
* one of them instead could repeat a change the original already made. */
void
nfs3_drc_cache_abandon(
    struct nfs3_drc              *drc,
    const struct nfs3_drc_keybuf *key);
