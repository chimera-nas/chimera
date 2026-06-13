// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "nfs3_drc.h"
#include "nfs_common.h"
#include "nfs_internal.h"
#include "nfs_kv_keys.h"
#include "nfs_drc_reply.h"
#include "nfs4_lease.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "evpl/evpl.h"
#include "evpl/evpl_rpc2.h"
#include "evpl/evpl_rpc2_program.h"

#define NFS3_DRC_VALUE_MAGIC 0x33435244u /* "DRC3" */

/* Most evictions remove a single similarly-sized entry; this caps how many
 * evicted keys one insert reports back for KV cleanup (any beyond it leak in
 * the KV store until the next cold-start reload re-bounds it). */
#define NFS3_DRC_EVICT_MAX   32

/* Standard NFSv3 procedure numbers (the generated dispatcher keys on these). */
enum {
    NFS3PROC_SETATTR = 2,
    NFS3PROC_CREATE  = 8,
    NFS3PROC_MKDIR   = 9,
    NFS3PROC_SYMLINK = 10,
    NFS3PROC_MKNOD   = 11,
    NFS3PROC_REMOVE  = 12,
    NFS3PROC_RMDIR   = 13,
    NFS3PROC_RENAME  = 14,
    NFS3PROC_LINK    = 15,
};

/* ------------------------------------------------------------------ *
*  request identity                                                  *
* ------------------------------------------------------------------ */

int
nfs3_drc_proc_cacheable(uint32_t proc)
{
    switch (proc) {
        case NFS3PROC_SETATTR:
        case NFS3PROC_CREATE:
        case NFS3PROC_MKDIR:
        case NFS3PROC_SYMLINK:
        case NFS3PROC_MKNOD:
        case NFS3PROC_REMOVE:
        case NFS3PROC_RMDIR:
        case NFS3PROC_RENAME:
        case NFS3PROC_LINK:
            return 1;
        default:
            /* Idempotent ops (READ/WRITE/GETATTR/LOOKUP/COMMIT/...) are safe to
             * re-execute on a retransmit, so they bypass the cache. */
            return 0;
    } /* switch */
} /* nfs3_drc_proc_cacheable */

#define NFS3_DRC_FNV_OFFSET 1469598103934665603ULL
#define NFS3_DRC_FNV_PRIME  1099511628211ULL

static inline uint64_t
nfs3_drc_fnv_accum(
    uint64_t       h,
    const uint8_t *p,
    uint32_t       len)
{
    uint32_t i;

    for (i = 0; i < len; i++) {
        h ^= p[i];
        h *= NFS3_DRC_FNV_PRIME;
    }
    return h;
} /* nfs3_drc_fnv_accum */

uint64_t
nfs3_drc_checksum(
    const void *data,
    uint32_t    len)
{
    return nfs3_drc_fnv_accum(NFS3_DRC_FNV_OFFSET, data, len);
} /* nfs3_drc_checksum */

static uint64_t
nfs3_drc_checksum_iov(
    const xdr_iovec *iov,
    int              niov)
{
    uint64_t h = NFS3_DRC_FNV_OFFSET;
    int      i;

    for (i = 0; i < niov; i++) {
        h = nfs3_drc_fnv_accum(h, xdr_iovec_data(&iov[i]), xdr_iovec_len(&iov[i]));
    }
    return h;
} /* nfs3_drc_checksum_iov */

/* Source IP with the ephemeral port stripped (stable across the reconnect a
 * retransmit rides in on).  Returns the address length written into out. */
static uint8_t
nfs3_drc_client_addr(
    struct evpl_rpc2_conn *conn,
    uint8_t               *out)
{
    char   str[80];
    char  *p;
    size_t n;

    evpl_rpc2_conn_get_remote_address(conn, str, sizeof(str));

    if (str[0] == '[') {
        /* "[ipv6]:port" -> the bytes between the brackets. */
        p = strchr(str, ']');
        n = p ? (size_t) (p - (str + 1)) : strlen(str);
        memmove(str, str + 1, n);
    } else {
        /* "ipv4:port" -> strip the trailing ":port". */
        p = strrchr(str, ':');
        n = p ? (size_t) (p - str) : strlen(str);
    }

    if (n > NFS3_DRC_ADDR_MAX) {
        n = NFS3_DRC_ADDR_MAX;
    }
    memcpy(out, str, n);
    return (uint8_t) n;
} /* nfs3_drc_client_addr */

/* ------------------------------------------------------------------ *
*  reply value (de)serialization                                     *
* ------------------------------------------------------------------ */

/* Value layout (LE): magic(4) ts(8) len(4) reply[len]. */
uint32_t
nfs3_drc_value_serialize(
    uint8_t    *buf,
    uint32_t    buf_size,
    uint64_t    ts,
    const void *body,
    uint32_t    body_len)
{
    uint32_t p = 0;

    if (buf_size < NFS3_DRC_VALUE_HDR_LEN + body_len) {
        return 0;
    }
    nfs_kv_put_le32(buf, &p, NFS3_DRC_VALUE_MAGIC);
    nfs_kv_put_le64(buf, &p, ts);
    nfs_kv_put_le32(buf, &p, body_len);
    memcpy(buf + p, body, body_len);
    return p + body_len;
} /* nfs3_drc_value_serialize */

int
nfs3_drc_value_parse(
    const uint8_t  *buf,
    uint32_t        len,
    uint64_t       *out_ts,
    const uint8_t **out_body,
    uint32_t       *out_body_len)
{
    uint32_t body_len;

    if (len < NFS3_DRC_VALUE_HDR_LEN ||
        nfs_kv_le32(buf) != NFS3_DRC_VALUE_MAGIC) {
        return -1;
    }
    *out_ts  = nfs_kv_le64(buf + 4);
    body_len = nfs_kv_le32(buf + 12);
    if (NFS3_DRC_VALUE_HDR_LEN + body_len > len) {
        return -1;
    }
    *out_body     = buf + NFS3_DRC_VALUE_HDR_LEN;
    *out_body_len = body_len;
    return 0;
} /* nfs3_drc_value_parse */

/* ------------------------------------------------------------------ *
*  in-memory cache                                                   *
* ------------------------------------------------------------------ */

/* Caller holds drc->lock.  Returns the number of evicted entries whose keys
 * were copied into evicted[] (for the caller to drop from the KV store). */
static int
nfs3_drc_cache_insert_locked(
    struct nfs3_drc              *drc,
    const struct nfs3_drc_keybuf *key,
    const void                   *body,
    uint32_t                      body_len,
    uint64_t                      ts,
    struct nfs3_drc_keybuf       *evicted,
    int                           max_evicted)
{
    struct nfs3_drc_entry *e;
    int                    nev = 0;

    HASH_FIND(hh, drc->table, key, sizeof(*key), e);
    if (e) {
        /* Last-writer-wins refresh of an existing identity. */
        drc->bytes -= e->len;
        free(e->buf);
        e->buf = malloc(body_len);
        memcpy(e->buf, body, body_len);
        e->len      = body_len;
        e->ts       = ts;
        drc->bytes += body_len;
        return 0;
    }

    /* FIFO eviction: drc->table is the oldest-inserted entry. */
    while (drc->bytes + body_len > NFS3_DRC_MAX_BYTES && drc->table) {
        struct nfs3_drc_entry *old = drc->table;

        if (evicted && nev < max_evicted) {
            evicted[nev++] = old->key;
        }
        HASH_DELETE(hh, drc->table, old);
        drc->bytes -= old->len;
        free(old->buf);
        free(old);
    }

    e      = calloc(1, sizeof(*e));
    e->key = *key;
    e->buf = malloc(body_len);
    memcpy(e->buf, body, body_len);
    e->len = body_len;
    e->ts  = ts;
    HASH_ADD(hh, drc->table, key, sizeof(e->key), e);
    drc->bytes += body_len;

    return nev;
} /* nfs3_drc_cache_insert_locked */

void
nfs3_drc_cache_insert(
    struct nfs3_drc              *drc,
    const struct nfs3_drc_keybuf *key,
    const void                   *body,
    uint32_t                      body_len,
    uint64_t                      ts)
{
    pthread_mutex_lock(&drc->lock);
    nfs3_drc_cache_insert_locked(drc, key, body, body_len, ts, NULL, 0);
    pthread_mutex_unlock(&drc->lock);
} /* nfs3_drc_cache_insert */

int
nfs3_drc_cache_lookup(
    struct nfs3_drc              *drc,
    const struct nfs3_drc_keybuf *key,
    uint8_t                     **out_buf,
    uint32_t                     *out_len)
{
    struct nfs3_drc_entry *e;
    uint8_t               *buf = NULL;
    uint32_t               len = 0;

    pthread_mutex_lock(&drc->lock);
    HASH_FIND(hh, drc->table, key, sizeof(*key), e);
    if (e) {
        len = e->len;
        buf = malloc(len);
        memcpy(buf, e->buf, len);
    }
    pthread_mutex_unlock(&drc->lock);

    if (!buf) {
        return 0;
    }
    *out_buf = buf;
    *out_len = len;
    return 1;
} /* nfs3_drc_cache_lookup */

/* ------------------------------------------------------------------ *
*  KV write-through                                                  *
* ------------------------------------------------------------------ */

struct nfs3_drc_kv_ctx {
    uint8_t  key[CHIMERA_KV_NFS3_REPLY_KEY_MAX];
    uint32_t key_len;
    uint8_t *value;
    uint32_t value_len;
};

static void
nfs3_drc_kv_done(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs3_drc_kv_ctx *ctx = private_data;

    (void) error_code;
    free(ctx->value);
    free(ctx);
} /* nfs3_drc_kv_done */

static void
nfs3_drc_kv_put(
    struct chimera_vfs_thread    *vfs_thread,
    uint16_t                      node_id,
    const struct nfs3_drc_keybuf *key,
    const void                   *body,
    uint32_t                      body_len,
    uint64_t                      ts)
{
    struct nfs3_drc_kv_ctx *ctx = malloc(sizeof(*ctx));

    ctx->key_len = nfs_kv_nfs3_reply_key(ctx->key, node_id, key->addr,
                                         key->addr_len, key->proc, key->xid,
                                         key->cksum);
    ctx->value     = malloc(NFS3_DRC_VALUE_HDR_LEN + body_len);
    ctx->value_len = nfs3_drc_value_serialize(ctx->value,
                                              NFS3_DRC_VALUE_HDR_LEN + body_len,
                                              ts, body, body_len);

    chimera_vfs_put_key(vfs_thread, ctx->key, ctx->key_len,
                        ctx->value, ctx->value_len, nfs3_drc_kv_done, ctx);
} /* nfs3_drc_kv_put */

static void
nfs3_drc_kv_delete(
    struct chimera_vfs_thread    *vfs_thread,
    uint16_t                      node_id,
    const struct nfs3_drc_keybuf *key)
{
    struct nfs3_drc_kv_ctx *ctx = malloc(sizeof(*ctx));

    ctx->value     = NULL;
    ctx->value_len = 0;
    ctx->key_len   = nfs_kv_nfs3_reply_key(ctx->key, node_id, key->addr,
                                           key->addr_len, key->proc, key->xid,
                                           key->cksum);

    chimera_vfs_delete_key(vfs_thread, ctx->key, ctx->key_len,
                           nfs3_drc_kv_done, ctx);
} /* nfs3_drc_kv_delete */

/* ------------------------------------------------------------------ *
*  reply capture (fires from inside send_reply)                      *
* ------------------------------------------------------------------ */

struct nfs3_drc_capture_ctx {
    struct chimera_server_nfs_thread *thread;
    struct nfs3_drc_keybuf            key;
};

static void
nfs3_drc_capture_reply(
    const struct evpl_iovec *iov,
    int                      niov,
    int                      total_length,
    void                    *private_data)
{
    struct nfs3_drc_capture_ctx      *ctx    = private_data;
    struct chimera_server_nfs_thread *thread = ctx->thread;
    struct nfs3_drc                  *drc    = &thread->shared->nfs3_drc;
    struct nfs3_drc_keybuf            evicted[NFS3_DRC_EVICT_MAX];
    uint8_t                          *buf;
    size_t                            offset = 0;
    uint64_t                          ts;
    int                               i, nev;

    if (total_length <= 0 || (uint32_t) total_length > NFS3_DRC_MAX_REPLY_SIZE) {
        return;
    }

    buf = malloc(total_length);
    for (i = 0; i < niov; i++) {
        memcpy(buf + offset, iov[i].data, iov[i].length);
        offset += iov[i].length;
    }

    ts = nfs_lease_now_ns();

    pthread_mutex_lock(&drc->lock);
    nev = nfs3_drc_cache_insert_locked(drc, &ctx->key, buf, total_length, ts,
                                       evicted, NFS3_DRC_EVICT_MAX);
    pthread_mutex_unlock(&drc->lock);

    if (!drc->persistence_disabled) {
        nfs3_drc_kv_put(thread->vfs_thread, drc->node_id, &ctx->key, buf,
                        total_length, ts);
        for (i = 0; i < nev; i++) {
            nfs3_drc_kv_delete(thread->vfs_thread, drc->node_id, &evicted[i]);
        }
    }

    free(buf);
} /* nfs3_drc_capture_reply */

/* ------------------------------------------------------------------ *
*  cold-start reload                                                 *
* ------------------------------------------------------------------ */

struct nfs3_drc_reload_ctx {
    struct chimera_server_nfs_thread *thread;
    uint32_t                          loaded;
    uint8_t                           start[CHIMERA_KV_PREFIX_LEN];
};

static int
nfs3_drc_reload_scan_cb(
    const void *key,
    uint32_t    key_len,
    const void *value,
    uint32_t    value_len,
    void       *private_data)
{
    struct nfs3_drc_reload_ctx       *ctx    = private_data;
    struct chimera_server_nfs_thread *thread = ctx->thread;
    struct nfs3_drc                  *drc    = &thread->shared->nfs3_drc;
    const uint8_t                    *k      = key;
    struct nfs3_drc_keybuf            kb;
    const uint8_t                    *body;
    uint64_t                          ts;
    uint32_t                          p, body_len;
    uint8_t                           addr_len;

    if (key_len < CHIMERA_KV_PREFIX_LEN + 1 ||
        memcmp(k, ctx->start, CHIMERA_KV_PREFIX_LEN) != 0) {
        return 1;  /* left this node's NFSv3 reply band */
    }

    addr_len = k[CHIMERA_KV_PREFIX_LEN];
    p        = CHIMERA_KV_PREFIX_LEN + 1;
    if (addr_len > NFS3_DRC_ADDR_MAX || p + addr_len + 16 > key_len) {
        return 0;  /* skip a malformed key */
    }

    memset(&kb, 0, sizeof(kb));
    memcpy(kb.addr, k + p, addr_len);
    kb.addr_len = addr_len;
    p          += addr_len;
    kb.proc     = nfs_kv_le32(k + p); p += 4;
    kb.xid      = nfs_kv_le32(k + p); p += 4;
    kb.cksum    = nfs_kv_le64(k + p);

    if (nfs3_drc_value_parse(value, value_len, &ts, &body, &body_len) != 0) {
        return 0;  /* skip a corrupt value */
    }

    /* In-memory only here -- deleting KV entries mid-scan could disturb the
     * backend's iteration, and KV size is already bounded by the previous
     * uptime's evict-on-insert. */
    nfs3_drc_cache_insert(drc, &kb, body, body_len, ts);
    ctx->loaded++;
    return 0;
} /* nfs3_drc_reload_scan_cb */

static void
nfs3_drc_reload_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs3_drc_reload_ctx       *ctx    = private_data;
    struct chimera_server_nfs_thread *thread = ctx->thread;

    (void) error_code;

    atomic_store(&thread->shared->nfs3_drc.load_state, NFS3_DRC_LOAD_READY);
    chimera_nfs_info("NFSv3 DRC: cold-start load complete: %u reply record(s) "
                     "reloaded", ctx->loaded);
    free(ctx);
} /* nfs3_drc_reload_complete */

static void
nfs3_drc_reload(struct chimera_server_nfs_thread *thread)
{
    struct nfs3_drc_reload_ctx *ctx = malloc(sizeof(*ctx));

    ctx->thread = thread;
    ctx->loaded = 0;
    nfs_kv_node_prefix(ctx->start, CHIMERA_KV_TYPE_NFS3_REPLY,
                       thread->shared->nfs3_drc.node_id);

    /* No end key + flags 0: key-ordered results, the callback stops when the
     * 5-byte [type,node] prefix changes. */
    chimera_vfs_search_keys(thread->vfs_thread,
                            ctx->start, CHIMERA_KV_PREFIX_LEN,
                            NULL, 0, 0,
                            nfs3_drc_reload_scan_cb,
                            nfs3_drc_reload_complete, ctx);
} /* nfs3_drc_reload */

/* One-shot cold-start reload.  Idempotent (atomic IDLE->RUNNING CAS); no-op
 * unless the cache is installed and the backend is persistent.  Called eagerly
 * from each worker's thread-init so the cache is warm before clients retransmit
 * across a restart, with the dispatch path as a fallback trigger. */
void
nfs3_drc_reload_kickoff(struct chimera_server_nfs_thread *thread)
{
    struct nfs3_drc *drc      = &thread->shared->nfs3_drc;
    int              expected = NFS3_DRC_LOAD_IDLE;

    if (!drc->orig_dispatch || drc->persistence_disabled) {
        return;  /* DRC disabled or non-persistent: nothing to reload */
    }

    if (atomic_compare_exchange_strong(&drc->load_state, &expected,
                                       NFS3_DRC_LOAD_RUNNING)) {
        nfs3_drc_reload(thread);
    }
} /* nfs3_drc_reload_kickoff */

/* ------------------------------------------------------------------ *
*  dispatch wrapper                                                  *
* ------------------------------------------------------------------ */

static int
nfs3_drc_dispatch(
    struct evpl               *evpl,
    struct evpl_rpc2_conn     *conn,
    struct evpl_rpc2_encoding *encoding,
    uint32_t                   proc,
    void                      *program_data,
    struct evpl_rpc2_cred     *cred,
    xdr_iovec                 *iov,
    int                        niov,
    int                        length,
    void                      *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs3_drc                  *drc    = &thread->shared->nfs3_drc;
    struct nfs3_drc_capture_ctx      *cctx;
    struct nfs3_drc_keybuf            key;
    uint8_t                          *cached;
    uint32_t                          cached_len;

    /* Warm the cache from stable storage on the first request of any kind (a
     * live vfs_thread is required, which only requests have).  Idempotent.
     * Lookups serve misses while the reload is in flight -- a miss just
     * re-executes, the pre-feature behavior. */
    nfs3_drc_reload_kickoff(thread);

    /* The cached reply is the TCP on-wire form; RDMA framing differs, so leave
    * RDMA requests (and idempotent procs) to the real dispatcher untouched. */
    if (conn->rdma || !nfs3_drc_proc_cacheable(proc)) {
        return drc->orig_dispatch(evpl, conn, encoding, proc, program_data,
                                  cred, iov, niov, length, private_data);
    }

    memset(&key, 0, sizeof(key));
    key.addr_len = nfs3_drc_client_addr(conn, key.addr);
    key.proc     = proc;
    key.xid      = encoding->xid;
    key.cksum    = nfs3_drc_checksum_iov(iov, niov);

    if (nfs3_drc_cache_lookup(drc, &key, &cached, &cached_len)) {
        int rc = nfs_drc_send_cached_reply(thread, encoding, cached, cached_len);

        free(cached);
        if (rc == 0) {
            return 0;  /* retransmit replayed from cache */
        }
        /* Unparseable cached reply (should not happen for a TCP MSG_ACCEPTED
         * reply): fall through and re-execute. */
    }

    /* Miss: arm the reply-capture hook, then run the real handler. */
    cctx = xdr_dbuf_alloc_space(sizeof(*cctx), encoding->dbuf);
    if (cctx) {
        cctx->thread                    = thread;
        cctx->key                       = key;
        encoding->reply_capture_cb      = nfs3_drc_capture_reply;
        encoding->reply_capture_private = cctx;
    }

    return drc->orig_dispatch(evpl, conn, encoding, proc, program_data,
                              cred, iov, niov, length, private_data);
} /* nfs3_drc_dispatch */

/* ------------------------------------------------------------------ *
*  lifecycle                                                         *
* ------------------------------------------------------------------ */

void
nfs3_drc_init(struct nfs3_drc *drc)
{
    pthread_mutex_init(&drc->lock, NULL);
    drc->table                = NULL;
    drc->bytes                = 0;
    drc->persistence_disabled = 0;
    drc->orig_dispatch        = NULL;
    atomic_store(&drc->load_state, NFS3_DRC_LOAD_IDLE);
} /* nfs3_drc_init */

void
nfs3_drc_destroy(struct nfs3_drc *drc)
{
    struct nfs3_drc_entry *e, *tmp;

    HASH_ITER(hh, drc->table, e, tmp)
    {
        HASH_DELETE(hh, drc->table, e);
        free(e->buf);
        free(e);
    }
    pthread_mutex_destroy(&drc->lock);
} /* nfs3_drc_destroy */

void
nfs3_drc_install(struct chimera_server_nfs_shared *shared)
{
    struct nfs3_drc *drc    = &shared->nfs3_drc;
    const char      *kvname = (shared->vfs && shared->vfs->kv_module) ?
        shared->vfs->kv_module->name : "";

    drc->node_id              = shared->node_id;
    drc->persistence_disabled = (strcmp(kvname, "memkv") == 0);

    if (drc->persistence_disabled) {
        chimera_nfs_info(
            "NFSv3 DRC: KV backend '%s' is non-persistent; the duplicate-"
            "request cache will not survive a server restart", kvname);
    }

    /* Wrap the generated NFS_V3 call dispatcher. */
    drc->orig_dispatch                     = shared->nfs_v3.rpc2.recv_call_dispatch;
    shared->nfs_v3.rpc2.recv_call_dispatch = nfs3_drc_dispatch;
} /* nfs3_drc_install */
