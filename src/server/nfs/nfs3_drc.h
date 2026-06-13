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

struct nfs3_drc_entry {
    struct nfs3_drc_keybuf key;
    uint8_t               *buf;     /* cached procedure-result body */
    uint32_t               len;
    uint64_t               ts;      /* capture time (monotonic ticks), diag */
    UT_hash_handle         hh;
};

enum nfs3_drc_load_state {
    NFS3_DRC_LOAD_IDLE = 0,
    NFS3_DRC_LOAD_RUNNING,
    NFS3_DRC_LOAD_READY,
};

struct nfs3_drc {
    pthread_mutex_t        lock;
    struct nfs3_drc_entry *table;            /* uthash, FIFO eviction order */
    uint64_t               bytes;
    uint16_t               node_id;          /* scopes our entries in a shared store */
    int                    persistence_disabled;
    _Atomic int            load_state;
    /* The generated NFS_V3 dispatcher we wrap; NULL until installed. */
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

/* One-shot cold-start reload of persisted reply records into the in-memory
 * cache.  Idempotent; no-op when the cache is disabled or the backend is
 * non-persistent.  Call from per-thread init (a live vfs_thread is required). */
void
nfs3_drc_reload_kickoff(
    struct chimera_server_nfs_thread *thread);

/* Initialize / tear down the in-memory cache. */
void
nfs3_drc_init(
    struct nfs3_drc *drc);

void
nfs3_drc_destroy(
    struct nfs3_drc *drc);

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
void
nfs3_drc_cache_insert(
    struct nfs3_drc              *drc,
    const struct nfs3_drc_keybuf *key,
    const void                   *body,
    uint32_t                      body_len,
    uint64_t                      ts);

/* Returns 1 and fills out_buf (malloc'd, caller frees) + out_len on a hit. */
int
nfs3_drc_cache_lookup(
    struct nfs3_drc              *drc,
    const struct nfs3_drc_keybuf *key,
    uint8_t                     **out_buf,
    uint32_t                     *out_len);
