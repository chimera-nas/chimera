// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <pthread.h>
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
 * client re-presenting a byte-identical request).
 *
 * Only non-idempotent procedures are cached (CREATE/MKDIR/REMOVE/RENAME/...);
 * re-executing an idempotent op is harmless, so those bypass the cache.
 *
 * Two further conditions gate a replay, because {address, xid, checksum} alone
 * cannot tell a retransmit from a different client's first-ever request when
 * both come from one host:
 *
 *   - Connection origin.  An entry records the connection whose reply it
 *     captured.  It replays to that connection, or -- once that connection is
 *     gone -- to any connection from the same address.  A second, concurrently
 *     live connection never replays another's reply, so a real mutation can no
 *     longer be acknowledged and discarded.  Records loaded from the KV store
 *     have no live origin and stay replayable, which is what keeps a client's
 *     reply band useful after it reconnects to another instance.
 *   - Age.  A retransmit follows its call by seconds; an entry older than
 *     NFS3_DRC_MAX_AGE_NS is treated as a miss rather than answering a fresh
 *     request with an arbitrarily old reply.
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

/* How long a captured reply stays eligible for replay.  A retransmit follows
 * its original call within the client's RPC timeout (seconds), so 120s leaves
 * generous headroom while bounding how stale a replayed reply can be.  Entries
 * are still inserted regardless of age -- only lookups are gated -- so a band
 * hydrated from the KV store after a restart is unaffected. */
#define NFS3_DRC_MAX_AGE_NS     (120ULL * 1000000000ULL)

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

struct nfs3_drc_entry {
    struct nfs3_drc_keybuf key;
    uint8_t               *buf;     /* cached procedure-result body */
    uint32_t               len;
    uint64_t               ts;      /* capture time (monotonic ns) */
    /* Connection this reply was captured on, and the generation that connection
     * held at the time (see nfs3_drc_conn).  origin_gen 0 means "no live origin"
     * and replays to anyone.
     *
     * Deliberately payload rather than part of the hash key.  Keying on the
     * connection would give each one its own entry, but a reconnecting client
     * would then miss its own record entirely -- and answering a retransmit that
     * arrives on a new connection is the whole point of an address-keyed cache.
     * The cost is that two live connections at one address issuing byte-
     * identical requests at the same xid share one entry, so the later capture
     * takes ownership and the earlier connection loses its replay protection.
     * That direction is safe: it degrades to re-executing a retransmit, never to
     * answering one connection with another's reply. */
    const void            *origin_conn;
    uint64_t               origin_gen;
    UT_hash_handle         hh;
};

/* One connection currently open on this server.  The generation disambiguates a
 * recycled pointer: a new connection landing on a freed conn's address gets a
 * fresh generation, so an entry naming the old {conn, gen} still reads as
 * origin-gone rather than as this connection's own reply. */
struct nfs3_drc_conn {
    const void    *conn;
    uint64_t       gen;
    UT_hash_handle hh;
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

/* One connectionless duplicate-request cache.  NFSv3 and NFSv4.0 each get an
 * instance; they share all the machinery below and differ only in kv_type (the
 * KV band) and their protocol-specific dispatch wrapper. */
struct nfs3_drc {
    pthread_mutex_t        lock;
    struct nfs3_drc_entry *table;            /* uthash, FIFO eviction order */
    struct nfs3_drc_hydra *hydrated;         /* uthash: addrs hydrated from KV  */
    struct nfs3_drc_conn  *live_conns;       /* uthash: connections now open    */
    uint64_t               next_conn_gen;    /* monotonic; never hands out 0    */
    uint64_t               bytes;
    uint8_t                kv_type;          /* CHIMERA_KV_TYPE_NFS{3,4_V40}_REPLY */
    int                    persistence_disabled;
    /* The generated dispatcher we wrap; NULL until installed. */
    int                    (*orig_dispatch)(
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

/* Initialize / tear down the in-memory cache.  kv_type picks the KV band. */
void
nfs3_drc_init(
    struct nfs3_drc *drc,
    uint8_t          kv_type);

void
nfs3_drc_destroy(
    struct nfs3_drc *drc);

/* Connection lifecycle, driven from the NFS server's rpc2 notify callback.  Both
 * are no-ops on a DRC that was never installed, so a disabled cache costs
 * nothing per connection.  A conn the DRC never saw opened reads as "not live",
 * which is the safe direction: entries it captured stay replayable. */
void
nfs3_drc_conn_open(
    struct nfs3_drc *drc,
    const void      *conn);

void
nfs3_drc_conn_close(
    struct nfs3_drc *drc,
    const void      *conn);

/* ----------------------------------------------------------------------- *
*  Shared connectionless-DRC core, used by the NFSv3 and NFSv4.0 adapters. *
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

/* In-memory cache primitives (lock taken internally).  origin_conn is the
 * connection whose reply this is; pass NULL for a record loaded from the KV
 * store, which has no live origin and stays replayable by any connection. */
void
nfs3_drc_cache_insert(
    struct nfs3_drc              *drc,
    const struct nfs3_drc_keybuf *key,
    const void                   *body,
    uint32_t                      body_len,
    uint64_t                      ts,
    const void                   *origin_conn);

/* Returns 1 and fills out_buf (malloc'd, caller frees) + out_len on a hit.
 * `conn` is the connection asking; an entry captured on a different, still-live
 * connection, or one older than NFS3_DRC_MAX_AGE_NS, reads as a miss. */
int
nfs3_drc_cache_lookup(
    struct nfs3_drc              *drc,
    const struct nfs3_drc_keybuf *key,
    const void                   *conn,
    uint8_t                     **out_buf,
    uint32_t                     *out_len);
