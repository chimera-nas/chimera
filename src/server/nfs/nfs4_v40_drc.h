// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <pthread.h>
#include <stdbool.h>
#include <stdint.h>

#include <uthash.h>

/* nfs3_xdr.h defines xdr_iovec (used in orig_dispatch below) and then pulls in
 * evpl_rpc2_program.h, which uses it. */
#include "nfs3_xdr.h"

struct chimera_server_nfs_shared;
struct evpl;
struct evpl_rpc2_conn;
struct evpl_rpc2_encoding;
struct evpl_rpc2_cred;
struct COMPOUND4args;

/*
 * NFSv4.0 duplicate-request cache.
 *
 * NFSv4.0 has no sessions, so a server reply cache is its only exactly-once
 * mechanism for the non-idempotent COMPOUNDs that the RFC 7530 Section 9.1.7
 * per-owner replay cache does not cover.  RFC 7530 requires one outright for
 * SETCLIENTID (Section 16.33.5: "Since SETCLIENTID is a non-idempotent
 * operation, let us assume that the server is implementing the duplicate
 * request cache").
 *
 * A request is identified by {connection, xid, request checksum}.  NFSv4.0 runs
 * over a connection-oriented transport, so a retransmit arrives on the
 * connection that sent the original; the connection is therefore the correct
 * scope, and it brings two properties an address-keyed cache cannot have:
 *
 *   - Isolation.  A connection belongs to exactly one client, so no amount of
 *     address sharing (NAT, containers, a multi-user host) can let one client be
 *     answered from another's entry.
 *   - A natural lifetime.  Entries are freed when their connection closes, so a
 *     reply can never be replayed arbitrarily long after capture and no age
 *     bound is needed.  A recycled `conn` pointer finds an empty cache rather
 *     than a stale one, so no generation counter is needed either.
 *
 * The checksum guards against a client reusing an xid on one connection for a
 * genuinely different request: same xid, different bytes reads as a miss.
 *
 * Cross-connection replay is deliberately not attempted.  A 4.0 client that
 * reconnects must re-establish through SETCLIENTID / SETCLIENTID_CONFIRM
 * anyway, and the seqid-sequenced state operations keep their own per-owner
 * replay cache (RFC 7530 Section 9.1.7), which the RFC calls "a more reliable
 * cache of duplicate non-idempotent requests than that of the traditional
 * cache" and which follows the owner across connections.  This cache layers
 * on top of it for the state ops: it answers a same-connection retransmit
 * byte-exact, where the Section 9.1.7 replay is structured and lossy (see
 * nfs4_v40_op_cacheable in the .c).  Keying on the client's address instead --
 * to answer a retransmit that arrives on a new connection -- is what made one
 * client's reply answerable to another; see the history note at the end of
 * this comment.
 *
 * 4.1+ COMPOUNDs (covered by the session reply cache), NULL, and RDMA
 * connections pass straight through.  RDMA is excluded because a cached reply
 * is the TCP on-wire form and the framing would not match.
 *
 * History: cd259a97 keyed this cache on {conn, xid}.  601a8d03 replaced it with
 * an instance of the NFSv3 connectionless DRC, keyed by {client address, xid,
 * proc, checksum}, so that a client's reply band could follow it to another
 * instance on failover.  The source address is the only client identity NFSv3
 * has, but NFSv4.0 has a real one, and substituting the address for it meant a
 * byte-identical COMPOUND at the same xid was answered from the cache no matter
 * which client on that address sent it.  This cache restores connection
 * scoping and keeps the checksum, the operation filter, and the reply filter
 * that cd259a97 lacked.
 */

/* Cached replies per connection, and the largest reply worth caching.  16 slots
 * covers any plausible number of non-idempotent operations a client has in
 * flight at once; a 17th insert evicts the connection's oldest entry. */
#define NFS4_V40_DRC_SLOTS          16u
#define NFS4_V40_DRC_MAX_REPLY_SIZE (64u * 1024u)

/* One cached reply.  `buf` holds the complete captured on-wire reply
 * (record mark + RPC header + body); nfs_drc_send_cached_reply strips it down to
 * the procedure result and regenerates the header for the replaying request. */
struct nfs4_v40_drc_entry {
    uint64_t cksum;
    uint32_t xid;
    uint32_t len;
    uint8_t *buf;   /* NULL when the slot is empty */
};

/* One connection's cache.  Created lazily on the connection's first cacheable
 * COMPOUND -- an idle or 4.1-only connection costs nothing -- and freed whole
 * when the connection disconnects.
 *
 * `conn` is the hash key and an identity only: it is compared, never
 * dereferenced, because the rpc2 layer may already have freed the connection by
 * the time anything looks at it. */
struct nfs4_v40_drc_conn {
    const void               *conn;
    struct nfs4_v40_drc_entry entries[NFS4_V40_DRC_SLOTS];
    uint32_t                  next;     /* FIFO eviction cursor */
    uint32_t                  bytes;
    UT_hash_handle            hh;
};

struct nfs4_v40_drc {
    pthread_mutex_t           lock;
    struct nfs4_v40_drc_conn *conns;   /* uthash, keyed by connection pointer */
    uint64_t                  bytes;   /* summed across all connections */
    /* The generated dispatcher we wrap; NULL until installed. */
    int                       (*orig_dispatch)(
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

void
nfs4_v40_drc_init(
    struct nfs4_v40_drc *drc);

void
nfs4_v40_drc_destroy(
    struct nfs4_v40_drc *drc);

/*
 * Wrap the generated NFS_V4 call dispatcher.  Unconditional: the cache is
 * bounded by the connections that are open, so there is nothing to opt out of,
 * and a server without it would answer a retransmitted CREATE with
 * NFS4ERR_EXIST.  server.nfs4_drc gates NFSv4.1 reply-cache *persistence* and
 * has no bearing here.
 */
void
nfs4_v40_drc_install(
    struct chimera_server_nfs_shared *shared);

/*
 * Drop everything cached for a connection.  Driven from the NFS server's rpc2
 * notify callback on disconnect, before the rpc2 layer frees the connection.
 * A no-op for a connection that never cached anything, and for a DRC that was
 * never installed.
 *
 * There is deliberately no matching open hook: a cache is created on first use,
 * so a connection cannot be missed by a notification that never arrives.
 */
void
nfs4_v40_drc_conn_close(
    struct nfs4_v40_drc *drc,
    const void          *conn);

/*
 * Does this COMPOUND carry an operation that must not be re-executed on a
 * retransmit?  Called after the arguments are decoded, from
 * chimera_nfs4_compound: the dispatcher arms the reply capture before it can see
 * the operations, so this decides whether the capture stays armed.
 *
 * Never captured is also never matched, because the key covers a checksum of
 * the request: a read-only COMPOUND's bytes cannot match a cached mutating
 * one's, barring a checksum collision.
 */
bool
nfs4_v40_drc_compound_cacheable(
    const struct COMPOUND4args *args);

/*
 * Disarm a reply capture this cache armed.  A no-op unless the capture belongs
 * to this cache -- the encoding's capture slot is shared with the NFSv4.1
 * session replay cache (nfs4_replay_arm_capture), so it must not be cleared
 * blindly.
 */
void
nfs4_v40_drc_cancel_capture(
    struct evpl_rpc2_encoding *encoding);

/*
 * Read the COMPOUND4args minorversion straight off the wire, before the request
 * is decoded.  False when it cannot be determined from the first iovec, which
 * the caller must treat as "do not cache" rather than as minorversion 0.
 *
 * Exposed for unit tests (test_nfs_persist): tag_len is attacker-controlled and
 * the offset arithmetic has to stay overflow-free.
 */
bool
nfs4_v40_peek_minorversion(
    const xdr_iovec *iov,
    int              niov,
    uint32_t        *out_mv);

/*
 * In-memory cache primitives (lock taken internally).  Exposed for unit tests;
 * the dispatch path uses them through nfs4_v40_drc_dispatch.  `reply` is a
 * complete on-wire reply as the rpc2 capture hook delivers it.
 */
void
nfs4_v40_drc_cache_insert(
    struct nfs4_v40_drc *drc,
    const void          *conn,
    uint32_t             xid,
    uint64_t             cksum,
    const void          *reply,
    uint32_t             reply_len);

/* Returns 1 and fills out_buf (malloc'd, caller frees) + out_len on a hit. */
int
nfs4_v40_drc_cache_lookup(
    struct nfs4_v40_drc *drc,
    const void          *conn,
    uint32_t             xid,
    uint64_t             cksum,
    uint8_t            **out_buf,
    uint32_t            *out_len);
