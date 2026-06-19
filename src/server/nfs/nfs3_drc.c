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

uint64_t
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
uint8_t
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
    uint8_t  key[CHIMERA_KV_CONN_REPLY_KEY_MAX];
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
    uint8_t                       kv_type,
    const struct nfs3_drc_keybuf *key,
    const void                   *body,
    uint32_t                      body_len,
    uint64_t                      ts)
{
    struct nfs3_drc_kv_ctx *ctx = malloc(sizeof(*ctx));

    ctx->key_len = nfs_kv_conn_reply_key(ctx->key, kv_type, key->addr,
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
    uint8_t                       kv_type,
    const struct nfs3_drc_keybuf *key)
{
    struct nfs3_drc_kv_ctx *ctx = malloc(sizeof(*ctx));

    ctx->value     = NULL;
    ctx->value_len = 0;
    ctx->key_len   = nfs_kv_conn_reply_key(ctx->key, kv_type, key->addr,
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
    struct nfs3_drc                  *drc;
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
    struct nfs3_drc                  *drc    = ctx->drc;
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
        nfs3_drc_kv_put(thread->vfs_thread, drc->kv_type, &ctx->key, buf,
                        total_length, ts);
        for (i = 0; i < nev; i++) {
            nfs3_drc_kv_delete(thread->vfs_thread, drc->kv_type, &evicted[i]);
        }
    }

    free(buf);
} /* nfs3_drc_capture_reply */

/* ------------------------------------------------------------------ *
*  lazy per-client hydrate                                           *
*                                                                    *
*  A client's reply records are keyed by its address, not by node, so*
*  they follow the client to whatever node it reconnects to.  On the *
*  first cacheable op from a client this instance has not yet seen, we*
*  load that client's whole band from the KV store BEFORE serving the *
*  op (which may itself be the retransmit we need to replay).  Once an*
*  address is hydrated, an in-memory miss is definitive -- no per-op  *
*  KV read.  The set is shared; each un-hydrated op self-defers and   *
*  scans, so the deferred op always resumes on its own thread (no     *
*  cross-thread reply).  Bounded redundant scans during the window are*
*  the price of that simplicity.                                     *
* ------------------------------------------------------------------ */

#define NFS3_DRC_HYDRA_KEYLEN (NFS3_DRC_ADDR_MAX + 4)

static bool
nfs3_drc_addr_hydrated(
    struct nfs3_drc *drc,
    const uint8_t   *addr,
    uint8_t          addr_len)
{
    struct nfs3_drc_hydra keyh, *h;

    memset(&keyh, 0, sizeof(keyh));
    memcpy(keyh.addr, addr, addr_len);
    keyh.addr_len = addr_len;

    pthread_mutex_lock(&drc->lock);
    HASH_FIND(hh, drc->hydrated, keyh.addr, NFS3_DRC_HYDRA_KEYLEN, h);
    pthread_mutex_unlock(&drc->lock);
    return h != NULL;
} /* nfs3_drc_addr_hydrated */

static void
nfs3_drc_addr_mark_hydrated(
    struct nfs3_drc *drc,
    const uint8_t   *addr,
    uint8_t          addr_len)
{
    struct nfs3_drc_hydra keyh, *h;

    memset(&keyh, 0, sizeof(keyh));
    memcpy(keyh.addr, addr, addr_len);
    keyh.addr_len = addr_len;

    pthread_mutex_lock(&drc->lock);
    HASH_FIND(hh, drc->hydrated, keyh.addr, NFS3_DRC_HYDRA_KEYLEN, h);
    if (!h) {
        h  = malloc(sizeof(*h));
        *h = keyh;
        HASH_ADD(hh, drc->hydrated, addr, NFS3_DRC_HYDRA_KEYLEN, h);
    }
    pthread_mutex_unlock(&drc->lock);
} /* nfs3_drc_addr_mark_hydrated */

/* In-memory lookup, then either replay (hit) or arm-capture + run the real
 * handler (miss).  Used directly on the hydrated path and again to resume a
 * deferred op once its client's band has loaded. */
static int
nfs3_drc_lookup_or_forward(
    struct nfs3_drc                  *drc,
    struct chimera_server_nfs_thread *thread,
    const struct nfs3_drc_keybuf     *key,
    struct evpl                      *evpl,
    struct evpl_rpc2_conn            *conn,
    struct evpl_rpc2_encoding        *encoding,
    uint32_t                          proc,
    void                             *program_data,
    struct evpl_rpc2_cred            *cred,
    xdr_iovec                        *iov,
    int                               niov,
    int                               length,
    void                             *private_data)
{
    struct nfs3_drc_capture_ctx *cctx;
    uint8_t                     *cached;
    uint32_t                     cached_len;

    if (nfs3_drc_cache_lookup(drc, key, &cached, &cached_len)) {
        int rc = nfs_drc_send_cached_reply(thread, encoding, cached, cached_len);

        free(cached);
        if (rc == 0) {
            return 0;  /* retransmit replayed from cache */
        }
        /* Unparseable cached reply (should not happen for a TCP MSG_ACCEPTED
         * reply): fall through and re-execute. */
    }

    cctx = xdr_dbuf_alloc_space(sizeof(*cctx), encoding->dbuf);
    if (cctx) {
        cctx->thread                    = thread;
        cctx->drc                       = drc;
        cctx->key                       = *key;
        encoding->reply_capture_cb      = nfs3_drc_capture_reply;
        encoding->reply_capture_private = cctx;
    }

    return drc->orig_dispatch(evpl, conn, encoding, proc, program_data,
                              cred, iov, niov, length, private_data);
} /* nfs3_drc_lookup_or_forward */

/* A deferred request + the in-flight scan that loads its client's band.  The
 * dispatch returned without replying, so the request (and the dbuf its
 * encoding/iov/cred point into) stays alive until we resume it here. */
struct nfs3_drc_hydrate_ctx {
    struct chimera_server_nfs_thread *thread;
    struct nfs3_drc                  *drc;
    struct nfs3_drc_keybuf            key;
    /* Stashed dispatch args to resume. */
    struct evpl                      *evpl;
    struct evpl_rpc2_conn            *conn;
    struct evpl_rpc2_encoding        *encoding;
    uint32_t                          proc;
    void                             *program_data;
    struct evpl_rpc2_cred             cred;   /* shallow copy; ptrs into dbuf */
    int                               has_cred;
    xdr_iovec                        *iov;
    int                               niov;
    int                               length;
    void                             *private_data;
    uint32_t                          loaded;   /* records found for this client */
    uint32_t                          start_len;
    uint8_t                           start[CHIMERA_KV_CONN_ADDR_PREFIX_MAX];
};

static int
nfs3_drc_hydrate_scan_cb(
    const void *key,
    uint32_t    key_len,
    const void *value,
    uint32_t    value_len,
    void       *private_data)
{
    struct nfs3_drc_hydrate_ctx *hc  = private_data;
    struct nfs3_drc             *drc = hc->drc;
    const uint8_t               *k   = key;
    struct nfs3_drc_keybuf       kb;
    const uint8_t               *body;
    uint64_t                     ts;
    uint32_t                     p = hc->start_len, body_len;

    /* Stop when we leave this client's [hdr][addr_len][addr] band. */
    if (key_len < hc->start_len ||
        memcmp(k, hc->start, hc->start_len) != 0) {
        return 1;
    }
    if (p + 16 > key_len) {
        return 0;  /* malformed tail */
    }

    /* Every record in this scan shares the client address; only the
     * proc/xid/cksum tail varies. */
    memset(&kb, 0, sizeof(kb));
    memcpy(kb.addr, hc->key.addr, hc->key.addr_len);
    kb.addr_len = hc->key.addr_len;
    kb.proc     = nfs_kv_le32(k + p); p += 4;
    kb.xid      = nfs_kv_le32(k + p); p += 4;
    kb.cksum    = nfs_kv_le64(k + p);

    if (nfs3_drc_value_parse(value, value_len, &ts, &body, &body_len) != 0) {
        return 0;  /* skip a corrupt value */
    }
    nfs3_drc_cache_insert(drc, &kb, body, body_len, ts);
    hc->loaded++;
    return 0;
} /* nfs3_drc_hydrate_scan_cb */

static void
nfs3_drc_hydrate_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct nfs3_drc_hydrate_ctx *hc  = private_data;
    struct nfs3_drc             *drc = hc->drc;

    (void) error_code;

    nfs3_drc_addr_mark_hydrated(drc, hc->key.addr, hc->key.addr_len);

    /* Only a returning client (one with persisted records) loads anything; a
     * brand-new client hydrates 0 and stays quiet. */
    if (hc->loaded) {
        chimera_nfs_info("conn DRC (type 0x%02x): hydrated %u reply record(s) "
                         "for a returning client", drc->kv_type, hc->loaded);
    }

    nfs3_drc_lookup_or_forward(drc, hc->thread, &hc->key, hc->evpl, hc->conn,
                               hc->encoding, hc->proc, hc->program_data,
                               hc->has_cred ? &hc->cred : NULL,
                               hc->iov, hc->niov, hc->length, hc->private_data);
    free(hc);
} /* nfs3_drc_hydrate_complete */

/* ------------------------------------------------------------------ *
*  shared dispatch core (used by the NFSv3 + NFSv4.0 adapters)        *
* ------------------------------------------------------------------ */

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
    void                         *private_data)
{
    struct chimera_server_nfs_thread *thread = private_data;
    struct nfs3_drc_hydrate_ctx      *hc;

    /* With a non-persistent backend there is nothing to hydrate -- the
     * in-memory cache is the whole truth -- and once a client's band is loaded,
     * an in-memory miss is definitive.  Either way, serve from memory. */
    if (drc->persistence_disabled ||
        nfs3_drc_addr_hydrated(drc, key->addr, key->addr_len)) {
        return nfs3_drc_lookup_or_forward(drc, thread, key, evpl, conn, encoding,
                                          proc, program_data, cred, iov, niov,
                                          length, private_data);
    }

    /* First cacheable op from a client this instance has not hydrated: load its
     * band from the KV store before serving this (possibly-replay) op.  Defer
     * the request -- the dbuf its encoding/iov/cred point into stays alive
     * because we do not reply -- and resume it when the scan completes. */
    hc = malloc(sizeof(*hc));
    if (!hc) {
        return nfs3_drc_lookup_or_forward(drc, thread, key, evpl, conn, encoding,
                                          proc, program_data, cred, iov, niov,
                                          length, private_data);
    }
    hc->thread       = thread;
    hc->drc          = drc;
    hc->key          = *key;
    hc->evpl         = evpl;
    hc->conn         = conn;
    hc->encoding     = encoding;
    hc->proc         = proc;
    hc->program_data = program_data;
    if (cred) {
        hc->cred     = *cred;  /* shallow copy; gids/machinename live in dbuf */
        hc->has_cred = 1;
    } else {
        hc->has_cred = 0;
    }
    hc->iov          = iov;
    hc->niov         = niov;
    hc->length       = length;
    hc->private_data = private_data;
    hc->loaded       = 0;
    hc->start_len    = nfs_kv_conn_addr_prefix(hc->start, drc->kv_type,
                                               key->addr, key->addr_len);

    chimera_vfs_search_keys(thread->vfs_thread, hc->start, hc->start_len,
                            NULL, 0, 0,
                            nfs3_drc_hydrate_scan_cb,
                            nfs3_drc_hydrate_complete, hc);
    return 0;  /* deferred; the reply is sent from nfs3_drc_hydrate_complete */
} /* nfs3_drc_serve */

/* ------------------------------------------------------------------ *
*  NFSv3 dispatch wrapper                                            *
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
    struct nfs3_drc_keybuf            key;

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

    return nfs3_drc_serve(drc, &key, evpl, conn, encoding, proc, program_data,
                          cred, iov, niov, length, private_data);
} /* nfs3_drc_dispatch */

/* ------------------------------------------------------------------ *
*  lifecycle                                                         *
* ------------------------------------------------------------------ */

void
nfs3_drc_init(
    struct nfs3_drc *drc,
    uint8_t          kv_type)
{
    pthread_mutex_init(&drc->lock, NULL);
    drc->table                = NULL;
    drc->hydrated             = NULL;
    drc->bytes                = 0;
    drc->kv_type              = kv_type;
    drc->persistence_disabled = 0;
    drc->orig_dispatch        = NULL;
} /* nfs3_drc_init */

void
nfs3_drc_destroy(struct nfs3_drc *drc)
{
    /* The uthash delete-during-iteration idiom (tmp holds the next entry before
     * the body runs, so freeing e is safe) trips scan-build's use-after-free
     * checker; guard it the same way the other NFS hash-table teardowns do. */
#ifndef __clang_analyzer__
    struct nfs3_drc_entry *e, *tmp;
    struct nfs3_drc_hydra *h, *htmp;

    HASH_ITER(hh, drc->table, e, tmp)
    {
        HASH_DELETE(hh, drc->table, e);
        free(e->buf);
        free(e);
    }
    HASH_ITER(hh, drc->hydrated, h, htmp)
    {
        HASH_DELETE(hh, drc->hydrated, h);
        free(h);
    }
#endif /* ifndef __clang_analyzer__ */
    pthread_mutex_destroy(&drc->lock);
} /* nfs3_drc_destroy */

void
nfs3_drc_install(struct chimera_server_nfs_shared *shared)
{
    struct nfs3_drc *drc    = &shared->nfs3_drc;
    const char      *kvname = (shared->vfs && shared->vfs->kv_module) ?
        shared->vfs->kv_module->name : "";

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
