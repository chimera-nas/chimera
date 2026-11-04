// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/thread.h"
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

/* Bumped from "DRC3" when the cached bytes changed from the whole on-wire
 * message to the RPC reply alone: an entry written by an older build is not
 * readable by this one, and a magic mismatch is already treated as a miss. */
#define NFS3_DRC_VALUE_MAGIC 0x34435244u /* "DRC4" */

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

/*
 * Fold the caller's identity into a request checksum.
 *
 * The iovecs a dispatcher is handed begin after the RPC header, so they do not
 * cover the credential: two users sending byte-identical procedure arguments
 * produce the same checksum.  With the address (NFSv3) or the connection
 * (NFSv4.0) as the only other scope, that is enough for one user's request to
 * be answered from another's entry -- and the second user is told its
 * non-idempotent operation succeeded when nothing of the sort ran.  Two users
 * on one machine, or on one container host behind a NAT, are all it takes.
 *
 * RFC 8881 Section 2.10.6.1.3.1 states the rule for the NFSv4.1 reply cache: a
 * retry "that uses a different principal in the RPC request's credential field
 * that translates to a different user" is a false retry, and if the replier
 * determines the users differ it MUST NOT answer from the cache.  Neither
 * RFC 1813 nor RFC 7530 discusses a reply cache at all, so neither says
 * anything either way -- but the hazard is the same one, and the fix is to make
 * the identity part of what the key compares.
 *
 * The decoded credential is folded rather than the raw bytes, because the
 * AUTH_SYS stamp is free to vary between a call and its own retransmission and
 * folding it would mean nothing ever matched.  Squashing is deliberately not
 * applied first: it happens later, per export, and two credentials that would
 * squash together simply miss and re-execute, which costs work but is never
 * wrong.
 */
uint64_t
nfs3_drc_checksum_cred(
    uint64_t                     h,
    const struct evpl_rpc2_cred *cred)
{
    uint32_t flavor = cred ? cred->flavor : 0;
    uint32_t i;

    h = nfs3_drc_fnv_accum(h, (const uint8_t *) &flavor, sizeof(flavor));

    if (!cred) {
        return h;
    }

    switch (cred->flavor) {
        case EVPL_RPC2_AUTH_SYS:
            h = nfs3_drc_fnv_accum(h, (const uint8_t *) &cred->authsys.uid,
                                   sizeof(cred->authsys.uid));
            h = nfs3_drc_fnv_accum(h, (const uint8_t *) &cred->authsys.gid,
                                   sizeof(cred->authsys.gid));
            for (i = 0; i < cred->authsys.num_gids && cred->authsys.gids; i++) {
                h = nfs3_drc_fnv_accum(h,
                                       (const uint8_t *) &cred->authsys.gids[i],
                                       sizeof(cred->authsys.gids[i]));
            }
            /* The machine name is part of the identity an AUTH_SYS credential
             * asserts: the same uid on two hosts is two users. */
            if (cred->authsys.machinename && cred->authsys.machinename_len > 0) {
                h = nfs3_drc_fnv_accum(h,
                                       (const uint8_t *) cred->authsys.machinename,
                                       (uint32_t) cred->authsys.machinename_len);
            }
            break;

        case EVPL_RPC2_AUTH_RPCSEC_GSS:
            h = nfs3_drc_fnv_accum(h, (const uint8_t *) &cred->gss.service,
                                   sizeof(cred->gss.service));
            if (cred->gss.principal) {
                h = nfs3_drc_fnv_accum(h, (const uint8_t *) cred->gss.principal,
                                       (uint32_t) strlen(cred->gss.principal));
            }
            break;

        default:
            /* AUTH_NONE and anything unrecognised assert no identity beyond
             * the flavor, which is already folded in. */
            break;
    } /* switch */

    return h;
} /* nfs3_drc_checksum_cred */

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

/* Caller holds drc->lock.  Evict cached entries, oldest first, until body_len
 * more bytes fit the budget.  Entries in progress are skipped: they hold no
 * bytes, and evicting one would reopen the window it exists to close.  Returns
 * the number of evicted keys copied into evicted[] (for the caller to drop
 * from the KV store). */
static int
nfs3_drc_evict_locked(
    struct nfs3_drc        *drc,
    uint32_t                body_len,
    struct nfs3_drc_keybuf *evicted,
    int                     max_evicted)
{
    struct nfs3_drc_entry *old, *next;
    int                    nev = 0;

    /* FIFO: drc->table iterates oldest-inserted first. */
    for (old = drc->table;
         old && drc->bytes + body_len > NFS3_DRC_MAX_BYTES;
         old = next) {
        next = old->hh.next;
        if (old->state != NFS3_DRC_DONE) {
            continue;
        }
        if (evicted && nev < max_evicted) {
            evicted[nev++] = old->key;
        }
        HASH_DELETE(hh, drc->table, old);
        drc->bytes -= old->len;
        free(old->buf);
        free(old);
    }
    return nev;
} /* nfs3_drc_evict_locked */

/* Caller holds drc->lock.  Add a cached entry for a key that has none. */
static int
nfs3_drc_add_done_locked(
    struct nfs3_drc              *drc,
    const struct nfs3_drc_keybuf *key,
    const void                   *body,
    uint32_t                      body_len,
    uint64_t                      ts,
    struct nfs3_drc_keybuf       *evicted,
    int                           max_evicted,
    int                          *nevicted)
{
    struct nfs3_drc_entry *e;

    *nevicted = nfs3_drc_evict_locked(drc, body_len, evicted, max_evicted);

    e = calloc(1, sizeof(*e));
    if (!e) {
        return 0;
    }
    e->buf = malloc(body_len);
    if (!e->buf) {
        free(e);
        return 0;
    }
    memcpy(e->buf, body, body_len);
    e->key   = *key;
    e->len   = body_len;
    e->ts    = ts;
    e->state = NFS3_DRC_DONE;
    HASH_ADD(hh, drc->table, key, sizeof(e->key), e);
    drc->bytes += body_len;
    return 1;
} /* nfs3_drc_add_done_locked */

/* Caller holds drc->lock.  Detach every waiter held on `e` and queue it to its
 * own thread's mailbox, carrying a copy of `reply` to send -- or NULL, to be
 * released unanswered.  The doorbell is rung rather than the reply sent here:
 * this runs on whichever thread completed the original (from inside that
 * request's own send_reply, at that), and a held request may only be answered
 * by the thread that owns its connection. */
static void
nfs3_drc_release_waiters_locked(
    struct nfs3_drc       *drc,
    struct nfs3_drc_entry *e,
    const uint8_t         *reply,
    uint32_t               reply_len)
{
    struct nfs3_drc_waiter           *w, *tmp;
    struct chimera_server_nfs_thread *owner;

    DL_FOREACH_SAFE2(e->waiters, w, tmp, next)
    {
        DL_DELETE2(e->waiters, w, prev, next);
        drc->held--;

        w->entry     = NULL;
        w->reply     = NULL;
        w->reply_len = 0;
        if (reply) {
            /* On allocation failure the waiter is released unanswered and its
             * client retransmits -- by then into the cached entry. */
            w->reply = malloc(reply_len);
            if (w->reply) {
                memcpy(w->reply, reply, reply_len);
                w->reply_len = reply_len;
            }
        }
        w->state = NFS3_DRC_WAITER_QUEUED;

        owner = w->thread;
        evpl_mutex_lock(&owner->drc_mbox_lock);
        LL_PREPEND2(owner->drc_mbox, w, qnext);
        evpl_mutex_unlock(&owner->drc_mbox_lock);
        evpl_ring_doorbell(&owner->drc_doorbell);
    }
} /* nfs3_drc_release_waiters_locked */

void
nfs3_drc_cache_insert(
    struct nfs3_drc              *drc,
    const struct nfs3_drc_keybuf *key,
    const void                   *body,
    uint32_t                      body_len,
    uint64_t                      ts)
{
    struct nfs3_drc_entry *e;
    int                    nev;

    evpl_mutex_lock(&drc->lock);
    HASH_FIND(hh, drc->table, key, sizeof(*key), e);
    if (!e) {
        /* Eviction from a reload has nothing to clean up in the KV store: the
         * entries evicted are ones this same scan (or capture) just wrote. */
        (void) nfs3_drc_add_done_locked(drc, key, body, body_len, ts,
                                        NULL, 0, &nev);
    }
    evpl_mutex_unlock(&drc->lock);
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

    evpl_mutex_lock(&drc->lock);
    HASH_FIND(hh, drc->table, key, sizeof(*key), e);
    if (e && e->state == NFS3_DRC_DONE) {
        len = e->len;
        buf = malloc(len);
        if (buf) {
            memcpy(buf, e->buf, len);
        }
    }
    evpl_mutex_unlock(&drc->lock);

    if (!buf) {
        return 0;
    }
    *out_buf = buf;
    *out_len = len;
    return 1;
} /* nfs3_drc_cache_lookup */

enum nfs3_drc_reserve_result
nfs3_drc_cache_reserve(
    struct nfs3_drc              *drc,
    const struct nfs3_drc_keybuf *key,
    struct nfs3_drc_waiter       *waiter,
    uint8_t                     **out_buf,
    uint32_t                     *out_len)
{
    struct nfs3_drc_entry       *e;
    struct nfs3_drc_waiter      *w;
    enum nfs3_drc_reserve_result rc;
    uint64_t now      = nfs_lease_now_ns();
    uint64_t stuck_ns = 0;

    evpl_mutex_lock(&drc->lock);
    HASH_FIND(hh, drc->table, key, sizeof(*key), e);

    if (!e) {
        rc = NFS3_DRC_UNTRACKED;
        if (drc->inprog < NFS3_DRC_MAX_INPROG) {
            e = calloc(1, sizeof(*e));
            if (e) {
                e->key   = *key;
                e->ts    = now;
                e->state = NFS3_DRC_INPROG;
                HASH_ADD(hh, drc->table, key, sizeof(e->key), e);
                drc->inprog++;
                rc = NFS3_DRC_RESERVED;
            }
        }
    } else if (e->state == NFS3_DRC_DONE) {
        rc       = NFS3_DRC_DROP;
        *out_buf = malloc(e->len);
        if (*out_buf) {
            memcpy(*out_buf, e->buf, e->len);
            *out_len = e->len;
            rc       = NFS3_DRC_HIT;
        }
    } else {
        /* In progress.  Hold at most one retransmit per connection: the
         * client listens only on its current connection, and one reply under
         * this XID answers it there, so a second waiter on the same
         * connection is redundant. */
        rc = NFS3_DRC_DROP;
        if (waiter && drc->held < NFS3_DRC_MAX_HELD) {
            DL_FOREACH2(e->waiters, w, next)
            {
                if (w->conn == waiter->conn) {
                    break;
                }
            }
            if (!w) {
                waiter->entry     = e;
                waiter->reply     = NULL;
                waiter->reply_len = 0;
                waiter->deadline  = now + drc->hold_ns;
                waiter->state     = NFS3_DRC_WAITER_HELD;
                waiter->qnext     = NULL;
                DL_APPEND2(e->waiters, waiter, prev, next);
                drc->held++;
                rc = NFS3_DRC_HELD;
            }
        }

        /* An original that is still running long after a client gave up on
         * it is wedged somewhere below.  Make that visible, rate-limited. */
        if (now - e->ts >= 5000000000ULL &&
            now - drc->last_stuck_report >= 10000000000ULL) {
            drc->last_stuck_report = now;
            stuck_ns               = now - e->ts;
        }
    }
    evpl_mutex_unlock(&drc->lock);

    if (stuck_ns) {
        chimera_nfs_error("NFSv3 DRC: a retransmit found its original (proc %u "
                          "xid %u) still in progress after %llu ms",
                          key->proc, key->xid,
                          (unsigned long long) (stuck_ns / 1000000ULL));
    }
    return rc;
} /* nfs3_drc_cache_reserve */

int
nfs3_drc_cache_complete(
    struct nfs3_drc              *drc,
    const struct nfs3_drc_keybuf *key,
    const void                   *body,
    uint32_t                      body_len,
    uint64_t                      ts,
    struct nfs3_drc_keybuf       *evicted,
    int                           max_evicted,
    int                          *nevicted)
{
    struct nfs3_drc_entry *e;
    int                    stored = 0;
    uint8_t               *buf;

    *nevicted = 0;

    evpl_mutex_lock(&drc->lock);
    HASH_FIND(hh, drc->table, key, sizeof(*key), e);

    if (!e) {
        /* Executed untracked (the in-progress ceiling was reached when it
         * arrived): cache it now. */
        stored = nfs3_drc_add_done_locked(drc, key, body, body_len, ts,
                                          evicted, max_evicted, nevicted);
    } else if (e->state == NFS3_DRC_INPROG) {
        buf = malloc(body_len);
        if (buf) {
            /* Room first; the eviction pass skips this entry, which is still
             * in progress while it runs. */
            *nevicted = nfs3_drc_evict_locked(drc, body_len, evicted,
                                              max_evicted);
            memcpy(buf, body, body_len);
            e->buf      = buf;
            e->len      = body_len;
            e->ts       = ts;
            e->state    = NFS3_DRC_DONE;
            drc->bytes += body_len;
            drc->inprog--;
            stored = 1;
            nfs3_drc_release_waiters_locked(drc, e, buf, body_len);
        } else {
            /* Cannot keep the reply: nothing to replay, now or later. */
            nfs3_drc_release_waiters_locked(drc, e, NULL, 0);
            HASH_DELETE(hh, drc->table, e);
            drc->inprog--;
            free(e);
        }
    }
    /* else: already cached.  The first reply for an identity is the one the
     * client may already have seen, so a later one -- from an execution that
     * should never have happened -- must not replace it. */

    evpl_mutex_unlock(&drc->lock);
    return stored;
} /* nfs3_drc_cache_complete */

void
nfs3_drc_cache_abandon(
    struct nfs3_drc              *drc,
    const struct nfs3_drc_keybuf *key)
{
    struct nfs3_drc_entry *e;

    evpl_mutex_lock(&drc->lock);
    HASH_FIND(hh, drc->table, key, sizeof(*key), e);
    if (e && e->state == NFS3_DRC_INPROG) {
        nfs3_drc_release_waiters_locked(drc, e, NULL, 0);
        HASH_DELETE(hh, drc->table, e);
        drc->inprog--;
        free(e);
    }
    evpl_mutex_unlock(&drc->lock);
} /* nfs3_drc_cache_abandon */

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
    /* This request created the key's in-progress entry, and so must end it:
     * with a reply (complete) or without one (abandon). */
    int                               reserved;
};

static void
nfs3_drc_capture_reply(
    const struct evpl_iovec *iov,
    int                      niov,
    int                      total_length,
    uint32_t                 body_offset,
    void                    *private_data)
{
    struct nfs3_drc_capture_ctx      *ctx    = private_data;
    struct chimera_server_nfs_thread *thread = ctx->thread;
    struct nfs3_drc                  *drc    = ctx->drc;
    struct nfs3_drc_keybuf            evicted[NFS3_DRC_EVICT_MAX];
    uint8_t                          *buf;
    uint32_t                          rpc_len;
    uint64_t                          ts;
    int                               i, nev, stored;

    if (total_length <= (int) body_offset ||
        (uint32_t) total_length > NFS3_DRC_MAX_REPLY_SIZE) {
        goto uncached;
    }

    rpc_len = (uint32_t) total_length - body_offset;

    buf = malloc(rpc_len);
    if (!buf) {
        goto uncached;  /* OOM: the reply cannot be kept */
    }

    if (nfs_drc_copy_rpc_reply(iov, niov, body_offset, buf, rpc_len) != rpc_len) {
        free(buf);
        goto uncached;
    }

    ts = nfs_lease_now_ns();

    stored = nfs3_drc_cache_complete(drc, &ctx->key, buf, rpc_len, ts,
                                     evicted, NFS3_DRC_EVICT_MAX, &nev);

    if (!drc->persistence_disabled) {
        if (stored) {
            nfs3_drc_kv_put(thread->vfs_thread, drc->kv_type, &ctx->key, buf,
                            rpc_len, ts);
        }
        for (i = 0; i < nev; i++) {
            nfs3_drc_kv_delete(thread->vfs_thread, drc->kv_type, &evicted[i]);
        }
    }

    free(buf);
    return;

 uncached:
    /* The original ran but its reply cannot be cached, so there is nothing to
     * give the retransmits held on it.  They are released unanswered rather
     * than executed: the original has already made its change. */
    if (ctx->reserved) {
        nfs3_drc_cache_abandon(drc, &ctx->key);
    }
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

    evpl_mutex_lock(&drc->lock);
    HASH_FIND(hh, drc->hydrated, keyh.addr, NFS3_DRC_HYDRA_KEYLEN, h);
    evpl_mutex_unlock(&drc->lock);
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

    evpl_mutex_lock(&drc->lock);
    HASH_FIND(hh, drc->hydrated, keyh.addr, NFS3_DRC_HYDRA_KEYLEN, h);
    if (!h) {
        h  = malloc(sizeof(*h));
        *h = keyh;
        HASH_ADD(hh, drc->hydrated, addr, NFS3_DRC_HYDRA_KEYLEN, h);
    }
    evpl_mutex_unlock(&drc->lock);
} /* nfs3_drc_addr_mark_hydrated */

/* ------------------------------------------------------------------ *
*  execution                                                         *
* ------------------------------------------------------------------ */

/* Test only: a request parked for drc->test_delay_ms before it runs.
 * The connection it arrived on must stay open for the delay -- the parked
 * request is not tracked for disconnect, which is acceptable for a hook that
 * exists to put a retransmit on ANOTHER connection inside the window. */
struct nfs3_drc_delay_ctx {
    struct evpl_timer                 timer;
    struct chimera_server_nfs_thread *thread;
    struct nfs3_drc                  *drc;
    struct nfs3_drc_keybuf            key;
    int                               reserved;
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
};

/* Run the real handler for a request this cache did not answer.  `reserved`
 * says it owns the key's in-progress entry; `deferred` says the dispatch that
 * delivered it has already returned, so a failure here can no longer be handed
 * back to rpc2 to answer and the request must be released instead. */
static int
nfs3_drc_execute(
    struct nfs3_drc                  *drc,
    struct chimera_server_nfs_thread *thread,
    const struct nfs3_drc_keybuf     *key,
    int                               reserved,
    int                               deferred,
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
    int                          rc;

    cctx = xdr_dbuf_alloc_space(sizeof(*cctx), encoding->dbuf);
    if (cctx) {
        cctx->thread                    = thread;
        cctx->drc                       = drc;
        cctx->key                       = *key;
        cctx->reserved                  = reserved;
        encoding->reply_capture_cb      = nfs3_drc_capture_reply;
        encoding->reply_capture_private = cctx;
    } else if (reserved) {
        /* Without a capture nothing would ever end the entry; run it
         * untracked instead, as an exhausted dbuf always has. */
        nfs3_drc_cache_abandon(drc, key);
        reserved = 0;
    }

    rc = drc->orig_dispatch(evpl, conn, encoding, proc, program_data,
                            cred, iov, niov, length, private_data);
    if (rc != 0) {
        /* Rejected before the handler ran (undecodable arguments, say): no
         * reply will be captured.  Retransmits carry the same bytes and would
         * be rejected the same way, so the ones held are released. */
        if (reserved) {
            nfs3_drc_cache_abandon(drc, key);
        }
        if (deferred) {
            evpl_rpc2_discard_request(evpl, encoding);
            rc = 0;
        }
    }
    return rc;
} /* nfs3_drc_execute */

static void
nfs3_drc_delay_fire(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct nfs3_drc_delay_ctx *dc = container_of(timer, struct nfs3_drc_delay_ctx,
                                                 timer);

    (void) evpl;

    nfs3_drc_execute(dc->drc, dc->thread, &dc->key, dc->reserved, 1, dc->evpl,
                     dc->conn,
                     dc->encoding, dc->proc, dc->program_data,
                     dc->has_cred ? &dc->cred : NULL, dc->iov, dc->niov,
                     dc->length, dc->private_data);
    free(dc);
} /* nfs3_drc_delay_fire */

/* Answer, hold, drop or execute one cacheable request.  Used directly on the
 * hydrated path and again (deferred) to resume a request once its client's
 * band has loaded. */
static int
nfs3_drc_lookup_or_forward(
    struct nfs3_drc                  *drc,
    struct chimera_server_nfs_thread *thread,
    const struct nfs3_drc_keybuf     *key,
    int                               deferred,
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
    struct nfs3_drc_waiter      *w = NULL;
    struct nfs3_drc_delay_ctx   *dc;
    uint8_t                     *cached;
    uint32_t                     cached_len;
    enum nfs3_drc_reserve_result res;

    /* A waiter lives in the request's own dbuf; without one (exhausted dbuf,
     * or a thread without the waiter machinery) a retransmit that finds its
     * original in progress is dropped instead of held. */
    if (thread->drc_armed) {
        w = xdr_dbuf_alloc_space(sizeof(*w), encoding->dbuf);
        if (w) {
            memset(w, 0, sizeof(*w));
            w->thread   = thread;
            w->conn     = conn;
            w->encoding = encoding;
        }
    }

    res = nfs3_drc_cache_reserve(drc, key, w, &cached, &cached_len);

    switch (res) {
        case NFS3_DRC_HIT:
            if (nfs_drc_send_cached_reply(thread, encoding, cached,
                                          cached_len) != 0) {
                /* Could not build the replay.  Never re-execute a request
                 * that has a cached reply: let the client retransmit. */
                evpl_rpc2_discard_request(evpl, encoding);
            }
            free(cached);
            return 0;

        case NFS3_DRC_HELD:
            /* Answered later, by this thread, from the original's reply. */
            DL_APPEND2(thread->drc_held, w, tprev, tnext);
            return 0;

        case NFS3_DRC_DROP:
            evpl_rpc2_discard_request(evpl, encoding);
            return 0;

        case NFS3_DRC_RESERVED:
        case NFS3_DRC_UNTRACKED:
        default:
            break;
    } /* switch */

    if (drc->test_delay_ms && !deferred) {
        dc = calloc(1, sizeof(*dc));
        if (dc) {
            dc->thread       = thread;
            dc->drc          = drc;
            dc->key          = *key;
            dc->reserved     = (res == NFS3_DRC_RESERVED);
            dc->evpl         = evpl;
            dc->conn         = conn;
            dc->encoding     = encoding;
            dc->proc         = proc;
            dc->program_data = program_data;
            if (cred) {
                dc->cred     = *cred;
                dc->has_cred = 1;
            }
            dc->iov          = iov;
            dc->niov         = niov;
            dc->length       = length;
            dc->private_data = private_data;
            evpl_add_oneshot_timer(evpl, &dc->timer, nfs3_drc_delay_fire,
                                   (uint64_t) drc->test_delay_ms * 1000);
            return 0;
        }
    }

    return nfs3_drc_execute(drc, thread, key, res == NFS3_DRC_RESERVED,
                            deferred, evpl, conn, encoding, proc, program_data,
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

    nfs3_drc_lookup_or_forward(drc, hc->thread, &hc->key, 1, hc->evpl, hc->conn,
                               hc->encoding, hc->proc, hc->program_data,
                               hc->has_cred ? &hc->cred : NULL,
                               hc->iov, hc->niov, hc->length, hc->private_data);
    free(hc);
} /* nfs3_drc_hydrate_complete */

/* ------------------------------------------------------------------ *
*  held retransmits                                                  *
*                                                                    *
*  Everything here runs on the waiter's own thread, the one that owns *
*  its connection: only that thread may answer or release the held   *
*  request.  Other threads only ever move a waiter from its entry to  *
*  this thread's mailbox (nfs3_drc_release_waiters_locked).           *
* ------------------------------------------------------------------ */

/* Answer (reply != NULL) or release one waiter this thread owns and has
 * already unlinked from its entry, its mailbox and its held list. */
static void
nfs3_drc_waiter_finish(
    struct chimera_server_nfs_thread *thread,
    struct nfs3_drc_waiter           *w)
{
    struct evpl_rpc2_encoding *encoding = w->encoding;
    uint8_t                   *reply    = w->reply;
    uint32_t                   len      = w->reply_len;

    /* w lives in the request's dbuf: nothing below may touch it. */
    if (!reply || nfs_drc_send_cached_reply(thread, encoding, reply, len) != 0) {
        evpl_rpc2_discard_request(thread->evpl, encoding);
    }
    free(reply);
} /* nfs3_drc_waiter_finish */

static void
nfs3_drc_doorbell(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_server_nfs_thread *thread = container_of(doorbell,
                                                            struct chimera_server_nfs_thread,
                                                            drc_doorbell);
    struct nfs3_drc_waiter           *q, *w;

    (void) evpl;

    evpl_mutex_lock(&thread->drc_mbox_lock);
    q                = thread->drc_mbox;
    thread->drc_mbox = NULL;
    evpl_mutex_unlock(&thread->drc_mbox_lock);

    while (q) {
        w = q;
        q = w->qnext;
        DL_DELETE2(thread->drc_held, w, tprev, tnext);
        nfs3_drc_waiter_finish(thread, w);
    }
} /* nfs3_drc_doorbell */

/* Unlink one owned waiter from wherever the cache has it -- its entry if still
 * held, this thread's mailbox if already queued -- and from the held list,
 * then release it unanswered.  With only_if_held, a waiter already queued for
 * an answer is left for the doorbell instead, and 0 is returned. */
static int
nfs3_drc_waiter_drop(
    struct chimera_server_nfs_thread *thread,
    struct nfs3_drc_waiter           *w,
    int                               only_if_held)
{
    struct nfs3_drc *drc = &thread->shared->nfs3_drc;

    evpl_mutex_lock(&drc->lock);
    if (w->state == NFS3_DRC_WAITER_HELD) {
        DL_DELETE2(w->entry->waiters, w, prev, next);
        w->entry = NULL;
        drc->held--;
    } else if (only_if_held) {
        evpl_mutex_unlock(&drc->lock);
        return 0;
    } else {
        /* Queued: pushed under drc->lock, so it is in the mailbox now (the
         * mailbox is only ever drained on this thread, which is here). */
        evpl_mutex_lock(&thread->drc_mbox_lock);
        LL_DELETE2(thread->drc_mbox, w, qnext);
        evpl_mutex_unlock(&thread->drc_mbox_lock);
        free(w->reply);
    }
    evpl_mutex_unlock(&drc->lock);

    DL_DELETE2(thread->drc_held, w, tprev, tnext);
    w->reply = NULL;
    nfs3_drc_waiter_finish(thread, w);
    return 1;
} /* nfs3_drc_waiter_drop */

/* Drop waiters held past their deadline.  Their entry stays in progress, so
 * the client's next retransmit is held again -- or, if the original has
 * finished by then, answered from the cache. */
static void
nfs3_drc_timer(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct chimera_server_nfs_thread *thread = container_of(timer,
                                                            struct chimera_server_nfs_thread,
                                                            drc_timer);
    struct nfs3_drc_waiter           *w, *tmp;
    uint64_t                          now = nfs_lease_now_ns();

    (void) evpl;

    /* The deadline is fixed before a waiter joins this list, by this thread;
     * whether it is still held is decided under the lock. */
    DL_FOREACH_SAFE2(thread->drc_held, w, tmp, tnext)
    {
        if (now >= w->deadline) {
            (void) nfs3_drc_waiter_drop(thread, w, 1);
        }
    }
} /* nfs3_drc_timer */

void
nfs3_drc_conn_close(
    struct chimera_server_nfs_thread *thread,
    struct evpl_rpc2_conn            *conn)
{
    struct nfs3_drc_waiter *w, *tmp;

    if (!thread->drc_armed) {
        return;
    }

    DL_FOREACH_SAFE2(thread->drc_held, w, tmp, tnext)
    {
        if (w->conn == conn) {
            (void) nfs3_drc_waiter_drop(thread, w, 0);
        }
    }
} /* nfs3_drc_conn_close */

void
nfs3_drc_thread_init(struct chimera_server_nfs_thread *thread)
{
    struct nfs3_drc *drc = &thread->shared->nfs3_drc;
    uint64_t         tick_us;

    thread->drc_mbox  = NULL;
    thread->drc_held  = NULL;
    thread->drc_armed = 0;

    if (!drc->orig_dispatch) {
        return;  /* the cache is not installed */
    }

    evpl_mutex_init(&thread->drc_mbox_lock, NULL);
    evpl_add_doorbell(thread->evpl, &thread->drc_doorbell, nfs3_drc_doorbell);

    /* Check deadlines a few times per hold period, and at least once a
     * second. */
    tick_us = drc->hold_ns / 4000;
    if (tick_us == 0) {
        tick_us = 1000;
    } else if (tick_us > 1000000) {
        tick_us = 1000000;
    }
    evpl_add_timer(thread->evpl, &thread->drc_timer, nfs3_drc_timer, tick_us);

    thread->drc_armed = 1;
} /* nfs3_drc_thread_init */

void
nfs3_drc_thread_destroy(struct chimera_server_nfs_thread *thread)
{
    struct nfs3_drc_waiter *w, *tmp;

    if (!thread->drc_armed) {
        return;
    }

    /* Answer what has already been answered, release the rest. */
    nfs3_drc_doorbell(thread->evpl, &thread->drc_doorbell);
    DL_FOREACH_SAFE2(thread->drc_held, w, tmp, tnext)
    {
        (void) nfs3_drc_waiter_drop(thread, w, 0);
    }

    evpl_remove_timer(thread->evpl, &thread->drc_timer);
    evpl_remove_doorbell(thread->evpl, &thread->drc_doorbell);
    evpl_mutex_destroy(&thread->drc_mbox_lock);
    thread->drc_armed = 0;
} /* nfs3_drc_thread_destroy */

/* ------------------------------------------------------------------ *
*  dispatch core                                                     *
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
        return nfs3_drc_lookup_or_forward(drc, thread, key, 0, evpl, conn, encoding,
                                          proc, program_data, cred, iov, niov,
                                          length, private_data);
    }

    /* First cacheable op from a client this instance has not hydrated: load its
     * band from the KV store before serving this (possibly-replay) op.  Defer
     * the request -- the dbuf its encoding/iov/cred point into stays alive
     * because we do not reply -- and resume it when the scan completes. */
    hc = malloc(sizeof(*hc));
    if (!hc) {
        return nfs3_drc_lookup_or_forward(drc, thread, key, 0, evpl, conn, encoding,
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

    /* Idempotent procs are safe to re-execute, so they bypass the cache.
     *
     * RDMA connections used to bypass it too, because the cached reply was the
     * TCP on-wire form and RDMA framing differs.  The cache now stores the RPC
     * reply with the transport framing stripped (nfs3_drc_capture_reply), so a
     * retransmit over RDMA is served like any other -- which it has to be: a
     * duplicate-request cache that quietly disables itself on one transport
     * re-executes non-idempotent operations there. */
    if (!nfs3_drc_proc_cacheable(proc)) {
        return drc->orig_dispatch(evpl, conn, encoding, proc, program_data,
                                  cred, iov, niov, length, private_data);
    }

    memset(&key, 0, sizeof(key));
    key.addr_len = nfs3_drc_client_addr(conn, key.addr);
    key.proc     = proc;
    key.xid      = encoding->xid;
    key.cksum    = nfs3_drc_checksum_cred(nfs3_drc_checksum_iov(iov, niov),
                                          cred);

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
    evpl_mutex_init(&drc->lock, NULL);
    drc->table    = NULL;
    drc->hydrated = NULL;
    drc->bytes    = 0;
    drc->inprog   = 0;
    drc->held     = 0;
    drc->hold_ns  = (uint64_t) (NFS4_LEASE_TIME_DEFAULT_S +
                                NFS3_DRC_HOLD_SLACK_S) * 1000000000ULL;
    drc->last_stuck_report    = 0;
    drc->test_delay_ms        = 0;
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
    evpl_mutex_destroy(&drc->lock);
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

    /* Test only: see struct nfs3_drc. */
    {
        const char *d = getenv("CHIMERA_NFS3_DRC_TEST_DELAY_MS");

        if (d && atoi(d) > 0) {
            drc->test_delay_ms = (uint32_t) atoi(d);
        }
    }

    /* Wrap the generated NFS_V3 call dispatcher. */
    drc->orig_dispatch                     = shared->nfs_v3.rpc2.recv_call_dispatch;
    shared->nfs_v3.rpc2.recv_call_dispatch = nfs3_drc_dispatch;
} /* nfs3_drc_install */

void
nfs3_drc_set_lease_time(
    struct nfs3_drc *drc,
    uint32_t         lease_time_s)
{
    const char *h = getenv("CHIMERA_NFS3_DRC_TEST_HOLD_MS");

    drc->hold_ns = (uint64_t) (lease_time_s + NFS3_DRC_HOLD_SLACK_S) *
        1000000000ULL;

    /* Test only: lets a test watch a held retransmit expire without waiting
     * out a lease. */
    if (h && atoi(h) > 0) {
        drc->hold_ns = (uint64_t) atoi(h) * 1000000ULL;
    }
} /* nfs3_drc_set_lease_time */
