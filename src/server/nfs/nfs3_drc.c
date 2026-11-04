// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/thread.h"
#include <stdatomic.h>
#include <stdlib.h>
#include <string.h>

#include "nfs3_drc.h"
#include "prometheus-c.h"
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

/* Unlink an element from one of this file's doubly linked lists.  utlist's
 * DL_DELETE2 asserts the list is not empty, but assert() is compiled out of
 * release builds; this keeps the invariant checked -- and visible to the
 * static analyzer -- in both. */
#define NFS3_DRC_DL_UNLINK(head, el, prev, next) do {                        \
            chimera_nfs_abort_if(!(head), "NFSv3 DRC: unlink from an empty list"); \
            DL_DELETE2(head, el, prev, next);                                 \
} while (0)

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

struct nfs3_drc_reply *
nfs3_drc_reply_alloc(
    const void *body,
    uint32_t    body_len)
{
    struct nfs3_drc_reply *reply = malloc(sizeof(*reply) + body_len);

    if (!reply) {
        return NULL;
    }
    atomic_init(&reply->refs, 1);
    reply->len = body_len;
    memcpy(reply->data, body, body_len);
    return reply;
} /* nfs3_drc_reply_alloc */

void
nfs3_drc_reply_put(struct nfs3_drc_reply *reply)
{
    if (reply && atomic_fetch_sub_explicit(&reply->refs, 1,
                                           memory_order_acq_rel) == 1) {
        free(reply);
    }
} /* nfs3_drc_reply_put */

static inline struct nfs3_drc_reply *
nfs3_drc_reply_get(struct nfs3_drc_reply *reply)
{
    atomic_fetch_add_explicit(&reply->refs, 1, memory_order_relaxed);
    return reply;
} /* nfs3_drc_reply_get */

/* Caller holds drc->lock. */
static inline void
nfs3_drc_count_locked(
    struct nfs3_drc    *drc,
    enum nfs3_drc_event ev)
{
    drc->events[ev]++;
    if (drc->metrics.events[ev]) {
        prometheus_counter_increment(drc->metrics.events[ev]);
    }
} /* nfs3_drc_count_locked */

/* Caller holds drc->lock.  Publish the in-progress and held totals. */
static inline void
nfs3_drc_gauges_locked(struct nfs3_drc *drc)
{
    if (drc->metrics.inprog) {
        prometheus_gauge_set(drc->metrics.inprog, drc->inprog);
    }
    if (drc->metrics.held) {
        prometheus_gauge_set(drc->metrics.held, drc->held);
    }
} /* nfs3_drc_gauges_locked */

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
        drc->bytes -= old->reply->len;
        nfs3_drc_reply_put(old->reply);
        free(old);
    }
    return nev;
} /* nfs3_drc_evict_locked */

/* Caller holds drc->lock.  Detach every waiter held on `e` and queue it to its
 * own thread's mailbox, carrying a reference to `reply` to send -- or NULL, to
 * be released unanswered and counted as `ev`.  The doorbell is rung rather
 * than the reply sent here: this runs on whichever thread completed the
 * original (from inside that request's own send_reply, at that), and a held
 * request may only be answered by the thread that owns its connection.
 * Nothing here allocates. */
static void
nfs3_drc_release_waiters_locked(
    struct nfs3_drc       *drc,
    struct nfs3_drc_entry *e,
    struct nfs3_drc_reply *reply,
    enum nfs3_drc_event    ev)
{
    struct nfs3_drc_waiter           *w, *tmp;
    struct chimera_server_nfs_thread *owner;

    DL_FOREACH_SAFE2(e->waiters, w, tmp, next)
    {
        NFS3_DRC_DL_UNLINK(e->waiters, w, prev, next);
        drc->held--;

        w->entry = NULL;
        w->reply = reply ? nfs3_drc_reply_get(reply) : NULL;
        w->state = NFS3_DRC_WAITER_QUEUED;
        nfs3_drc_count_locked(drc, reply ? NFS3_DRC_EV_ANSWERED : ev);

        owner = w->thread;
        evpl_mutex_lock(&owner->drc_mbox_lock);
        LL_PREPEND2(owner->drc_mbox, w, qnext);
        evpl_mutex_unlock(&owner->drc_mbox_lock);
        evpl_ring_doorbell(&owner->drc_doorbell);
    }
    nfs3_drc_gauges_locked(drc);
} /* nfs3_drc_release_waiters_locked */

void
nfs3_drc_cache_insert(
    struct nfs3_drc              *drc,
    const struct nfs3_drc_keybuf *key,
    const void                   *body,
    uint32_t                      body_len,
    uint64_t                      ts)
{
    struct nfs3_drc_entry *e, *fresh;
    struct nfs3_drc_reply *reply;

    /* Allocated before the lock, freed below if the key is already taken. */
    reply = nfs3_drc_reply_alloc(body, body_len);
    fresh = calloc(1, sizeof(*fresh));
    if (!reply || !fresh) {
        nfs3_drc_reply_put(reply);
        free(fresh);
        return;
    }

    evpl_mutex_lock(&drc->lock);
    HASH_FIND(hh, drc->table, key, sizeof(*key), e);
    if (!e) {
        /* Eviction from a reload has nothing to clean up in the KV store: the
         * entries evicted are ones this same scan (or capture) just wrote. */
        (void) nfs3_drc_evict_locked(drc, body_len, NULL, 0);
        fresh->key   = *key;
        fresh->reply = reply;
        fresh->ts    = ts;
        fresh->state = NFS3_DRC_DONE;
        HASH_ADD(hh, drc->table, key, sizeof(fresh->key), fresh);
        drc->bytes += body_len;
        reply       = NULL;
        fresh       = NULL;
    }
    evpl_mutex_unlock(&drc->lock);

    nfs3_drc_reply_put(reply);
    free(fresh);
} /* nfs3_drc_cache_insert */

int
nfs3_drc_cache_lookup(
    struct nfs3_drc              *drc,
    const struct nfs3_drc_keybuf *key,
    uint8_t                     **out_buf,
    uint32_t                     *out_len)
{
    struct nfs3_drc_entry *e;
    struct nfs3_drc_reply *reply = NULL;
    uint8_t               *buf;

    evpl_mutex_lock(&drc->lock);
    HASH_FIND(hh, drc->table, key, sizeof(*key), e);
    if (e && e->state == NFS3_DRC_DONE) {
        reply = nfs3_drc_reply_get(e->reply);
    }
    evpl_mutex_unlock(&drc->lock);

    if (!reply) {
        return 0;
    }
    buf = malloc(reply->len);
    if (buf) {
        memcpy(buf, reply->data, reply->len);
        *out_buf = buf;
        *out_len = reply->len;
    }
    nfs3_drc_reply_put(reply);
    return buf ? 1 : 0;
} /* nfs3_drc_cache_lookup */

enum nfs3_drc_reserve_result
nfs3_drc_cache_reserve(
    struct nfs3_drc              *drc,
    const struct nfs3_drc_keybuf *key,
    const void                   *conn,
    struct nfs3_drc_waiter       *waiter,
    struct nfs3_drc_reply       **out_reply)
{
    struct nfs3_drc_entry       *e, *fresh;
    struct nfs3_drc_waiter      *w;
    enum nfs3_drc_reserve_result rc;
    uint64_t now      = nfs_lease_now_ns();
    uint64_t stuck_ns = 0;

    /* Allocated before the lock; freed below unless this request reserves. */
    fresh = calloc(1, sizeof(*fresh));

    evpl_mutex_lock(&drc->lock);
    HASH_FIND(hh, drc->table, key, sizeof(*key), e);

    if (!e) {
        if (fresh && drc->inprog < NFS3_DRC_MAX_INPROG) {
            fresh->key         = *key;
            fresh->ts          = now;
            fresh->state       = NFS3_DRC_INPROG;
            fresh->origin_conn = conn;
            HASH_ADD(hh, drc->table, key, sizeof(fresh->key), fresh);
            fresh = NULL;
            drc->inprog++;
            nfs3_drc_count_locked(drc, NFS3_DRC_EV_RESERVED);
            nfs3_drc_gauges_locked(drc);
            rc = NFS3_DRC_RESERVED;
        } else {
            nfs3_drc_count_locked(drc, NFS3_DRC_EV_UNTRACKED);
            rc = NFS3_DRC_UNTRACKED;
        }
    } else if (e->state == NFS3_DRC_DONE) {
        *out_reply = nfs3_drc_reply_get(e->reply);
        nfs3_drc_count_locked(drc, NFS3_DRC_EV_REPLAYED);
        rc = NFS3_DRC_HIT;
    } else {
        /* In progress: never executed again.  Hold the retransmit unless
         * that adds nothing or cannot be afforded. */
        rc = NFS3_DRC_DROP;
        if (conn == e->origin_conn) {
            /* The original's own reply goes to this very connection. */
            nfs3_drc_count_locked(drc, NFS3_DRC_EV_DROP_ORIGIN_CONN);
        } else if (!waiter) {
            nfs3_drc_count_locked(drc, NFS3_DRC_EV_DROP_NO_WAITER);
        } else {
            /* One reply under this XID on the connection the client is
             * listening on answers it; a second waiter there adds nothing. */
            DL_FOREACH2(e->waiters, w, next)
            {
                if (w->conn == waiter->conn) {
                    break;
                }
            }
            if (w) {
                nfs3_drc_count_locked(drc, NFS3_DRC_EV_DROP_ALREADY_WAITING);
            } else if (drc->held >= NFS3_DRC_MAX_HELD) {
                nfs3_drc_count_locked(drc, NFS3_DRC_EV_DROP_CEILING);
            } else {
                waiter->entry    = e;
                waiter->reply    = NULL;
                waiter->deadline = now + drc->hold_ns;
                waiter->state    = NFS3_DRC_WAITER_HELD;
                waiter->qnext    = NULL;
                DL_APPEND2(e->waiters, waiter, prev, next);
                drc->held++;
                nfs3_drc_count_locked(drc, NFS3_DRC_EV_HELD);
                nfs3_drc_gauges_locked(drc);
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

    free(fresh);

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
    struct nfs3_drc_reply        *reply,
    uint64_t                      ts,
    struct nfs3_drc_keybuf       *evicted,
    int                           max_evicted,
    int                          *nevicted)
{
    struct nfs3_drc_entry *e, *fresh;
    int                    stored = 0;

    *nevicted = 0;

    /* For the untracked case below; allocated before the lock. */
    fresh = calloc(1, sizeof(*fresh));

    evpl_mutex_lock(&drc->lock);
    HASH_FIND(hh, drc->table, key, sizeof(*key), e);

    if (!e) {
        /* Executed untracked (the in-progress ceiling was reached when it
         * arrived): cache it now. */
        if (fresh) {
            *nevicted = nfs3_drc_evict_locked(drc, reply->len, evicted,
                                              max_evicted);
            fresh->key   = *key;
            fresh->reply = nfs3_drc_reply_get(reply);
            fresh->ts    = ts;
            fresh->state = NFS3_DRC_DONE;
            HASH_ADD(hh, drc->table, key, sizeof(fresh->key), fresh);
            drc->bytes += reply->len;
            fresh       = NULL;
            stored      = 1;
        }
    } else if (e->state == NFS3_DRC_INPROG) {
        /* Room first; the eviction pass skips this entry, which is still in
         * progress while it runs. */
        *nevicted = nfs3_drc_evict_locked(drc, reply->len, evicted,
                                          max_evicted);
        e->reply       = nfs3_drc_reply_get(reply);
        e->ts          = ts;
        e->state       = NFS3_DRC_DONE;
        e->origin_conn = NULL;
        /* Eviction is oldest-inserted first, and the entry was inserted when
         * it was reserved: a slow original would complete as the oldest
         * reply in the cache and be the next one evicted, leaving its
         * retransmit to execute again.  Requeue it at the tail, so replies
         * age from when they were produced. */
        HASH_DELETE(hh, drc->table, e);
        HASH_ADD(hh, drc->table, key, sizeof(e->key), e);
        drc->bytes += reply->len;
        drc->inprog--;
        stored = 1;
        nfs3_drc_release_waiters_locked(drc, e, reply, NFS3_DRC_EV_ANSWERED);
    }
    /* else: already cached.  The first reply for an identity is the one the
     * client may already have seen, so a later one -- from an execution that
     * should never have happened -- must not replace it. */

    evpl_mutex_unlock(&drc->lock);

    free(fresh);
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
        nfs3_drc_release_waiters_locked(drc, e, NULL,
                                        NFS3_DRC_EV_DROP_ABANDONED);
        HASH_DELETE(hh, drc->table, e);
        drc->inprog--;
        nfs3_drc_count_locked(drc, NFS3_DRC_EV_ABANDONED);
        nfs3_drc_gauges_locked(drc);
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
*  reply capture and request end                                     *
* ------------------------------------------------------------------ */

/* Armed on every request the cache executes, in the request's dbuf. */
struct nfs3_drc_capture_ctx {
    struct chimera_server_nfs_thread *thread;
    struct nfs3_drc                  *drc;
    struct nfs3_drc_keybuf            key;
    /* This request created the key's in-progress entry. */
    int                               reserved;
    /* Its reply was captured and handed to the cache. */
    int                               completed;
};

/* Fires from inside send_reply, for an accepted SUCCESS reply only -- which
 * includes an NFS error status such as NFS3ERR_NOENT, a reply like any other.
 * It also fires for a reply whose connection has closed: libevpl drops that
 * reply after this, and a retransmit on the client's next connection is owed
 * exactly this answer. */
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
    struct nfs3_drc_reply            *reply;
    uint32_t                          rpc_len;
    uint64_t                          ts;
    int                               i, nev, stored;

    /* Too large to keep, or no memory for it: not completed, so the
     * request-end hook abandons the entry when the request is freed. */
    if (total_length <= (int) body_offset ||
        (uint32_t) total_length > NFS3_DRC_MAX_REPLY_SIZE) {
        return;
    }

    rpc_len = (uint32_t) total_length - body_offset;

    reply = malloc(sizeof(*reply) + rpc_len);
    if (!reply) {
        return;
    }
    atomic_init(&reply->refs, 1);
    reply->len = rpc_len;

    if (nfs_drc_copy_rpc_reply(iov, niov, body_offset, reply->data,
                               rpc_len) != rpc_len) {
        nfs3_drc_reply_put(reply);
        return;
    }

    ts = nfs_lease_now_ns();

    stored = nfs3_drc_cache_complete(drc, &ctx->key, reply, ts,
                                     evicted, NFS3_DRC_EVICT_MAX, &nev);
    ctx->completed = 1;

    if (!drc->persistence_disabled) {
        if (stored) {
            nfs3_drc_kv_put(thread->vfs_thread, drc->kv_type, &ctx->key,
                            reply->data, reply->len, ts);
        }
        for (i = 0; i < nev; i++) {
            nfs3_drc_kv_delete(thread->vfs_thread, drc->kv_type, &evicted[i]);
        }
    }

    nfs3_drc_reply_put(reply);
} /* nfs3_drc_capture_reply */

/* Fires exactly once when the request ends, however it ends (libevpl's
 * request-end hook): replied to, rejected before its handler ran, its reply
 * dropped with a closed connection, or discarded.  An in-progress entry whose
 * original ends without a captured reply is abandoned here -- the one place no
 * path can bypass, so such an entry can never outlive its original. */
static void
nfs3_drc_request_end(void *private_data)
{
    struct nfs3_drc_capture_ctx *ctx = private_data;

    if (ctx->reserved && !ctx->completed) {
        nfs3_drc_cache_abandon(ctx->drc, &ctx->key);
    }
} /* nfs3_drc_request_end */

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

/* Test only: a request parked for drc->test_delay_ms before it runs.  Kept on
 * its thread's drc_parked list, so a disconnect or a thread shutdown cancels
 * it like any other request still waiting on this cache. */
struct nfs3_drc_delay_ctx {
    struct evpl_timer                 timer;
    struct chimera_server_nfs_thread *thread;
    struct nfs3_drc                  *drc;
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
    struct nfs3_drc_delay_ctx        *prev, *next;  /* thread->drc_parked */
};

/* Run the real handler.  `deferred` says the dispatch that delivered the
 * request has already returned, so a rejection can no longer be handed back
 * to rpc2 to answer and the request is released instead.  Either way the
 * request-end hook then abandons an entry the request reserved. */
static int
nfs3_drc_execute(
    struct nfs3_drc           *drc,
    int                        deferred,
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
    int rc;

    rc = drc->orig_dispatch(evpl, conn, encoding, proc, program_data,
                            cred, iov, niov, length, private_data);
    if (rc != 0 && deferred) {
        evpl_rpc2_discard_request(evpl, encoding);
        rc = 0;
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

    NFS3_DRC_DL_UNLINK(dc->thread->drc_parked, dc, prev, next);
    nfs3_drc_execute(dc->drc, 1, dc->evpl, dc->conn, dc->encoding, dc->proc,
                     dc->program_data, dc->has_cred ? &dc->cred : NULL,
                     dc->iov, dc->niov, dc->length, dc->private_data);
    free(dc);
} /* nfs3_drc_delay_fire */

/* Release a parked request unrun.  Its request-end hook abandons the entry it
 * reserved. */
static void
nfs3_drc_delay_cancel(
    struct chimera_server_nfs_thread *thread,
    struct nfs3_drc_delay_ctx        *dc)
{
    evpl_remove_timer(thread->evpl, &dc->timer);
    NFS3_DRC_DL_UNLINK(thread->drc_parked, dc, prev, next);
    evpl_rpc2_discard_request(thread->evpl, dc->encoding);
    free(dc);
} /* nfs3_drc_delay_cancel */

static void
nfs3_drc_timer_arm(
    struct chimera_server_nfs_thread *thread);

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
    struct nfs3_drc_waiter      *w     = NULL;
    struct nfs3_drc_reply       *reply = NULL;
    struct nfs3_drc_capture_ctx *cctx;
    struct nfs3_drc_delay_ctx   *dc;
    enum nfs3_drc_reserve_result res;

    /* A waiter lives in the request's own dbuf (a bump allocation, no malloc);
     * without one (exhausted dbuf, or a thread without the waiter machinery)
     * a retransmit that finds its original in progress is dropped. */
    if (thread->drc_armed) {
        w = xdr_dbuf_alloc_space(sizeof(*w), encoding->dbuf);
        if (w) {
            memset(w, 0, sizeof(*w));
            w->thread   = thread;
            w->conn     = conn;
            w->encoding = encoding;
        }
    }

    res = nfs3_drc_cache_reserve(drc, key, conn, w, &reply);

    switch (res) {
        case NFS3_DRC_HIT:
            if (nfs_drc_send_cached_reply(thread, encoding, reply->data,
                                          reply->len) != 0) {
                /* Could not build the replay.  Never re-execute a request
                 * that has a cached reply: let the client retransmit. */
                evpl_rpc2_discard_request(evpl, encoding);
            }
            nfs3_drc_reply_put(reply);
            return 0;

        case NFS3_DRC_HELD:
            /* Answered later, by this thread, from the original's reply.
             * The cache only holds a waiter it was given. */
            chimera_nfs_abort_if(!w, "NFSv3 DRC: request held without a waiter");
            DL_APPEND2(thread->drc_held, w, tprev, tnext);
            nfs3_drc_timer_arm(thread);
            return 0;

        case NFS3_DRC_DROP:
            evpl_rpc2_discard_request(evpl, encoding);
            return 0;

        case NFS3_DRC_RESERVED:
        case NFS3_DRC_UNTRACKED:
        default:
            break;
    } /* switch */

    /* Executed by this request.  Arm the capture (its reply becomes the
     * cached one) and the request-end hook (an entry it reserved is abandoned
     * if it ends with no reply captured) before anything can end it. */
    cctx = xdr_dbuf_alloc_space(sizeof(*cctx), encoding->dbuf);
    if (cctx) {
        cctx->thread                    = thread;
        cctx->drc                       = drc;
        cctx->key                       = *key;
        cctx->reserved                  = (res == NFS3_DRC_RESERVED);
        cctx->completed                 = 0;
        encoding->reply_capture_cb      = nfs3_drc_capture_reply;
        encoding->reply_capture_private = cctx;
        encoding->request_end_cb        = nfs3_drc_request_end;
        encoding->request_end_private   = cctx;
    } else if (res == NFS3_DRC_RESERVED) {
        /* Nothing could ever end the entry: run untracked, as an exhausted
         * dbuf always has. */
        nfs3_drc_cache_abandon(drc, key);
    }

    if (drc->test_delay_ms && !deferred) {
        dc = calloc(1, sizeof(*dc));
        if (dc) {
            dc->thread       = thread;
            dc->drc          = drc;
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
            DL_APPEND2(thread->drc_parked, dc, prev, next);
            evpl_add_oneshot_timer(evpl, &dc->timer, nfs3_drc_delay_fire,
                                   (uint64_t) drc->test_delay_ms * 1000);
            return 0;
        }
    }

    return nfs3_drc_execute(drc, deferred, evpl, conn, encoding, proc,
                            program_data, cred, iov, niov, length,
                            private_data);
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

/* Answer (with its reply) or release one waiter this thread owns and has
 * already unlinked from its entry, its mailbox and its held list. */
static void
nfs3_drc_waiter_finish(
    struct chimera_server_nfs_thread *thread,
    struct nfs3_drc_waiter           *w)
{
    struct evpl_rpc2_encoding *encoding = w->encoding;
    struct nfs3_drc_reply     *reply    = w->reply;

    /* w lives in the request's dbuf: nothing below may touch it. */
    if (!reply || nfs_drc_send_cached_reply(thread, encoding, reply->data,
                                            reply->len) != 0) {
        evpl_rpc2_discard_request(thread->evpl, encoding);
    }
    nfs3_drc_reply_put(reply);
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
        NFS3_DRC_DL_UNLINK(thread->drc_held, w, tprev, tnext);
        nfs3_drc_waiter_finish(thread, w);
    }
} /* nfs3_drc_doorbell */

/* Unlink one owned waiter from wherever the cache has it -- its entry if still
 * held, this thread's mailbox if already queued -- and from the held list,
 * then release it unanswered, counted as `ev`.  With only_if_held, a waiter
 * already queued for an answer is left for the doorbell instead, and 0 is
 * returned. */
static int
nfs3_drc_waiter_drop(
    struct chimera_server_nfs_thread *thread,
    struct nfs3_drc_waiter           *w,
    int                               only_if_held,
    enum nfs3_drc_event               ev)
{
    struct nfs3_drc *drc = &thread->shared->nfs3_drc;

    evpl_mutex_lock(&drc->lock);
    if (w->state == NFS3_DRC_WAITER_HELD) {
        NFS3_DRC_DL_UNLINK(w->entry->waiters, w, prev, next);
        w->entry = NULL;
        drc->held--;
        nfs3_drc_count_locked(drc, ev);
        nfs3_drc_gauges_locked(drc);
    } else if (only_if_held) {
        evpl_mutex_unlock(&drc->lock);
        return 0;
    } else {
        /* Queued: pushed under drc->lock, so it is in the mailbox now (the
         * mailbox is only ever drained on this thread, which is here). */
        evpl_mutex_lock(&thread->drc_mbox_lock);
        LL_DELETE2(thread->drc_mbox, w, qnext);
        evpl_mutex_unlock(&thread->drc_mbox_lock);
    }
    evpl_mutex_unlock(&drc->lock);

    NFS3_DRC_DL_UNLINK(thread->drc_held, w, tprev, tnext);
    nfs3_drc_reply_put(w->reply);
    w->reply = NULL;
    nfs3_drc_waiter_finish(thread, w);
    return 1;
} /* nfs3_drc_waiter_drop */

static void
nfs3_drc_timer(
    struct evpl       *evpl,
    struct evpl_timer *timer);

/* Arm the deadline check if this thread holds waiters and it is not armed.
 * It runs only while something is held: an idle server has no timer. */
static void
nfs3_drc_timer_arm(struct chimera_server_nfs_thread *thread)
{
    uint64_t tick_us;

    if (thread->drc_timer_armed || !thread->drc_held) {
        return;
    }

    /* A few checks per hold period, and at least one a second. */
    tick_us = thread->shared->nfs3_drc.hold_ns / 4000;
    if (tick_us < 1000) {
        tick_us = 1000;
    } else if (tick_us > 1000000) {
        tick_us = 1000000;
    }
    evpl_add_oneshot_timer(thread->evpl, &thread->drc_timer, nfs3_drc_timer,
                           tick_us);
    thread->drc_timer_armed = 1;
} /* nfs3_drc_timer_arm */

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

    thread->drc_timer_armed = 0;   /* one-shot: already removed */

    /* The deadline is fixed before a waiter joins this list, by this thread;
     * whether it is still held is decided under the lock. */
    DL_FOREACH_SAFE2(thread->drc_held, w, tmp, tnext)
    {
        if (now >= w->deadline) {
            (void) nfs3_drc_waiter_drop(thread, w, 1, NFS3_DRC_EV_DROP_DEADLINE);
        }
    }

    nfs3_drc_timer_arm(thread);
} /* nfs3_drc_timer */

void
nfs3_drc_conn_close(
    struct chimera_server_nfs_thread *thread,
    struct evpl_rpc2_conn            *conn)
{
    struct nfs3_drc_waiter    *w, *tmp;
    struct nfs3_drc_delay_ctx *dc, *dtmp;

    if (!thread->drc_armed) {
        return;
    }

    DL_FOREACH_SAFE2(thread->drc_held, w, tmp, tnext)
    {
        if (w->conn == conn) {
            (void) nfs3_drc_waiter_drop(thread, w, 0,
                                        NFS3_DRC_EV_DROP_DISCONNECT);
        }
    }
    DL_FOREACH_SAFE2(thread->drc_parked, dc, dtmp, next)
    {
        if (dc->conn == conn) {
            nfs3_drc_delay_cancel(thread, dc);
        }
    }
} /* nfs3_drc_conn_close */

void
nfs3_drc_thread_init(struct chimera_server_nfs_thread *thread)
{
    struct nfs3_drc *drc = &thread->shared->nfs3_drc;

    thread->drc_mbox        = NULL;
    thread->drc_held        = NULL;
    thread->drc_parked      = NULL;
    thread->drc_armed       = 0;
    thread->drc_timer_armed = 0;

    if (!drc->orig_dispatch) {
        return;  /* the cache is not installed */
    }

    evpl_mutex_init(&thread->drc_mbox_lock, NULL);
    evpl_add_doorbell(thread->evpl, &thread->drc_doorbell, nfs3_drc_doorbell);
    thread->drc_armed = 1;
} /* nfs3_drc_thread_init */

void
nfs3_drc_thread_destroy(struct chimera_server_nfs_thread *thread)
{
    struct nfs3_drc           *drc = &thread->shared->nfs3_drc;
    struct nfs3_drc_waiter    *w, *tmp;
    struct nfs3_drc_delay_ctx *dc, *dtmp;

    if (!thread->drc_armed) {
        return;
    }

    /* Server stop closes every connection before its threads are torn down,
     * so nfs3_drc_conn_close has normally released everything here already
     * and this finds nothing.  It stays as the guarantee should that order
     * ever change: a request left parked or held at this point could never be
     * answered or freed.
     *
     * Parked requests first: releasing one abandons its entry, which queues
     * that entry's waiters -- possibly this thread's -- for the drain below. */
    DL_FOREACH_SAFE2(thread->drc_parked, dc, dtmp, next)
    {
        nfs3_drc_delay_cancel(thread, dc);
    }

    /* Answer what has already been answered, release the rest. */
    nfs3_drc_doorbell(thread->evpl, &thread->drc_doorbell);
    DL_FOREACH_SAFE2(thread->drc_held, w, tmp, tnext)
    {
        (void) nfs3_drc_waiter_drop(thread, w, 0, NFS3_DRC_EV_DROP_SHUTDOWN);
    }

    if (thread->drc_timer_armed) {
        evpl_remove_timer(thread->evpl, &thread->drc_timer);
        thread->drc_timer_armed = 0;
    }

    /* A completion on another thread queues a waiter of ours and then rings
     * our doorbell, under drc->lock throughout but outside our mailbox lock:
     * the drain above can take that waiter between the two, leaving nothing
     * here to wait on, and the ring would then land on a removed doorbell.
     * Retire it under drc->lock, which waits out a ring in progress.  With
     * every waiter of ours already drained or dropped, no later completion
     * can target this thread. */
    evpl_mutex_lock(&drc->lock);
    evpl_remove_doorbell(thread->evpl, &thread->drc_doorbell);
    evpl_mutex_unlock(&drc->lock);
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
    memset(drc->events, 0, sizeof(drc->events));
    memset(&drc->metrics, 0, sizeof(drc->metrics));
} /* nfs3_drc_init */

/* The event label each counter series carries, by enum nfs3_drc_event. */
static const char *const nfs3_drc_event_names[NFS3_DRC_EV_COUNT] = {
    [NFS3_DRC_EV_RESERVED]             = "reserved",
    [NFS3_DRC_EV_UNTRACKED]            = "untracked",
    [NFS3_DRC_EV_REPLAYED]             = "replayed",
    [NFS3_DRC_EV_HELD]                 = "held",
    [NFS3_DRC_EV_ANSWERED]             = "answered",
    [NFS3_DRC_EV_ABANDONED]            = "abandoned",
    [NFS3_DRC_EV_DROP_ORIGIN_CONN]     = "drop_origin_conn",
    [NFS3_DRC_EV_DROP_ALREADY_WAITING] = "drop_already_waiting",
    [NFS3_DRC_EV_DROP_CEILING]         = "drop_ceiling",
    [NFS3_DRC_EV_DROP_NO_WAITER]       = "drop_no_waiter",
    [NFS3_DRC_EV_DROP_DEADLINE]        = "drop_deadline",
    [NFS3_DRC_EV_DROP_DISCONNECT]      = "drop_disconnect",
    [NFS3_DRC_EV_DROP_ABANDONED]       = "drop_abandoned",
    [NFS3_DRC_EV_DROP_SHUTDOWN]        = "drop_shutdown",
};

/* chimera_nfs3_drc{event=...} counts what the cache did with each request;
 * chimera_nfs3_drc_state{state="in_progress"|"held"} is how many entries are
 * in progress and how many retransmits are held right now.  A growing
 * in_progress with nothing completing is an original wedged below the
 * cache. */
static void
nfs3_drc_metrics_init(
    struct nfs3_drc           *drc,
    struct prometheus_metrics *registry)
{
    struct nfs3_drc_metrics *m = &drc->metrics;
    int                      i;

    if (!registry) {
        return;
    }
    m->registry = registry;
    m->counter  = prometheus_metrics_create_counter(
        registry, "chimera_nfs3_drc",
        "NFSv3 duplicate-request cache: what was done with each request");
    for (i = 0; i < NFS3_DRC_EV_COUNT; i++) {
        m->series[i] = prometheus_counter_create_series(
            m->counter, (const char *[]) { "event" },
            (const char *[]) { nfs3_drc_event_names[i] }, 1);
        m->events[i] = prometheus_counter_series_create_instance(m->series[i]);
    }
    m->gauge = prometheus_metrics_create_gauge(
        registry, "chimera_nfs3_drc_state",
        "NFSv3 duplicate-request cache: entries in progress and retransmits held");
    m->inprog_series = prometheus_gauge_create_series(
        m->gauge, (const char *[]) { "state" },
        (const char *[]) { "in_progress" }, 1);
    m->held_series = prometheus_gauge_create_series(
        m->gauge, (const char *[]) { "state" }, (const char *[]) { "held" }, 1);
    m->inprog = prometheus_gauge_series_create_instance(m->inprog_series);
    m->held   = prometheus_gauge_series_create_instance(m->held_series);
} /* nfs3_drc_metrics_init */

static void
nfs3_drc_metrics_destroy(struct nfs3_drc *drc)
{
    struct nfs3_drc_metrics *m = &drc->metrics;
    int                      i;

    if (!m->registry) {
        return;
    }
    for (i = 0; i < NFS3_DRC_EV_COUNT; i++) {
        prometheus_counter_series_destroy_instance(m->series[i], m->events[i]);
        prometheus_counter_destroy_series(m->counter, m->series[i]);
    }
    prometheus_counter_destroy(m->registry, m->counter);
    prometheus_gauge_series_destroy_instance(m->inprog_series, m->inprog);
    prometheus_gauge_series_destroy_instance(m->held_series, m->held);
    prometheus_gauge_destroy_series(m->gauge, m->inprog_series);
    prometheus_gauge_destroy_series(m->gauge, m->held_series);
    prometheus_gauge_destroy(m->registry, m->gauge);
    memset(m, 0, sizeof(*m));
} /* nfs3_drc_metrics_destroy */

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
        nfs3_drc_reply_put(e->reply);   /* NULL for an entry in progress */
        free(e);
    }
    HASH_ITER(hh, drc->hydrated, h, htmp)
    {
        HASH_DELETE(hh, drc->hydrated, h);
        free(h);
    }
#endif /* ifndef __clang_analyzer__ */
    nfs3_drc_metrics_destroy(drc);
    evpl_mutex_destroy(&drc->lock);
} /* nfs3_drc_destroy */

void
nfs3_drc_install(
    struct chimera_server_nfs_shared *shared,
    int                               persist)
{
    struct nfs3_drc *drc    = &shared->nfs3_drc;
    const char      *kvname = (shared->vfs && shared->vfs->kv_module) ?
        shared->vfs->kv_module->name : "";

    drc->persistence_disabled = (!persist || strcmp(kvname, "memkv") == 0);

    if (!persist) {
        chimera_nfs_info(
            "NFSv3 DRC: nfs3_drc_persist=false; replies are cached in memory only");
    } else if (drc->persistence_disabled) {
        chimera_nfs_info(
            "NFSv3 DRC: KV backend '%s' is non-persistent; the duplicate-"
            "request cache will not survive a server restart", kvname);
    }

    nfs3_drc_metrics_init(drc, shared->metrics);

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
