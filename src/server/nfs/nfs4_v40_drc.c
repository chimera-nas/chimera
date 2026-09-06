// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdbool.h>
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "nfs4_v40_drc.h"
#include "nfs3_drc.h"
#include "nfs_drc_reply.h"
#include "nfs_common.h"
#include "nfs_internal.h"
#include "evpl/evpl.h"
#include "evpl/evpl_rpc2.h"
#include "evpl/evpl_rpc2_program.h"

/* NFSv4 RPC procedure numbers. */
#define NFS4_PROC_COMPOUND 1u

static inline uint32_t
v40_be32(const uint8_t *p)
{
    return ((uint32_t) p[0] << 24) | ((uint32_t) p[1] << 16) |
           ((uint32_t) p[2] << 8) | (uint32_t) p[3];
} /* v40_be32 */

/*
 * Peek the COMPOUND4args minorversion without decoding the whole request.  Wire
 * layout: tag<opaque> (len + bytes padded to 4) then minorversion(u32).  The
 * compound header is virtually always within the first iovec on TCP; if the tag
 * runs past it, we cannot tell -- return false and let the request pass through
 * uncached (safe: a missed cache, never a wrong reply).
 *
 * tag_len is attacker-controlled, so the offset arithmetic is done in 64 bits.
 * In 32-bit arithmetic `4 + tag_len + pad` wraps for tag_len near UINT32_MAX --
 * 0xFFFFFFF5 through 0xFFFFFFF8 land on off == 0xFFFFFFFC, whose `off + 4` wraps
 * to 0 and sails through a 32-bit bounds check, reading ~4GB past the buffer.
 * Widening makes the comparison exact and the guard unconditional.
 */
bool
nfs4_v40_peek_minorversion(
    const xdr_iovec *iov,
    int              niov,
    uint32_t        *out_mv)
{
    const uint8_t *p;
    uint32_t       len, tag_len;
    uint64_t       off;

    if (niov < 1) {
        return false;
    }
    p   = xdr_iovec_data(&iov[0]);
    len = xdr_iovec_len(&iov[0]);
    if (len < 4) {
        return false;
    }
    tag_len = v40_be32(p);
    off     = 4ull + tag_len + ((4u - (tag_len & 3u)) & 3u); /* tag padded to 4 */
    if (off + 4ull > (uint64_t) len) {
        return false;  /* tag spans beyond the first iovec */
    }
    *out_mv = v40_be32(p + off);
    return true;
} /* nfs4_v40_peek_minorversion */

/*
 * Which minorversion-0 operations must not be re-executed on a retransmit.
 *
 * The NFSv3 path answers the same question per procedure in
 * nfs3_drc_proc_cacheable(); a COMPOUND has to be judged by the operations it
 * carries, so this is its equivalent.  The one deliberate exclusion:
 *
 *   - Idempotent ops (LOOKUP, READDIR, GETATTR, READ, ACCESS, COMMIT, ...) are
 *     safe to re-execute, so they never justify an entry.  WRITE is excluded for
 *     the same reason and to match the v3 list: replaying the same bytes at the
 *     same offset is harmless, and WRITE replies are the bulk of the traffic
 *     this cache would otherwise hold.
 *
 * The seqid-sequenced state operations (OPEN, CLOSE, LOCK, LOCKU, OPEN_CONFIRM,
 * OPEN_DOWNGRADE) are included even though they also have the RFC 7530
 * Section 9.1.7 per-owner replay cache (struct nfs4_replay_cache).  The two
 * caches layer rather than overlap:
 *
 *   - This cache answers a same-connection retransmit BYTE-EXACT, before the
 *     compound is decoded.  The Section 9.1.7 cache is structured and lossy --
 *     its OPEN replay reconstructs cinfo/attrset/rflags/delegation as zero
 *     (nfs4_proc_open.c) -- and the retransmit most likely to happen is the
 *     first OPEN of a fresh open-owner, exactly the reply that carried
 *     OPEN4_RESULT_CONFIRM.  A client replayed rflags == 0 skips OPEN_CONFIRM
 *     and desynchronises its seqid bookkeeping for that owner.
 *   - The Section 9.1.7 cache still answers a retransmit that arrives on a NEW
 *     connection, which this cache deliberately does not cover.
 *
 * On a hit here the compound never executes, so the owner's seqid is not
 * advanced twice; that is precisely the Section 9.1.7 rule for r == L (return
 * the stored response, do not process beyond seqid checking).  An erroneous
 * state op (e.g. NFS4ERR_BAD_SEQID) is an RPC-level SUCCESS, so its reply is
 * cached too, and a retransmit sees the same error rather than a re-execution.
 *
 * SETCLIENTID and SETCLIENTID_CONFIRM are here because RFC 7530 Section 16.33.5
 * requires a DRC for them outright: every case in that section mints a fresh
 * setclientid_confirm verifier, so a re-executed SETCLIENTID invalidates the
 * verifier the client received from the reply it lost.
 *
 * This is not NFS4_OP_FLAG_MUTATES from nfs4_op_matrix.h: that flag answers a
 * different question (does the op need a writable export) and includes WRITE.
 *
 * OPENATTR with createdir set does mutate, but chimera has no OPENATTR handler
 * (chimera_nfs4_compound_process falls through to NFS4ERR_NOTSUPP), and a
 * NOTSUPP reply is MSG_ACCEPTED/SUCCESS at the RPC layer, so listing it here
 * would only spend slots caching errors.  Add it when OPENATTR is implemented.
 */
static bool
nfs4_v40_op_cacheable(const struct nfs_argop4 *argop)
{
    switch (argop->argop) {
        case OP_CREATE:
        case OP_REMOVE:
        case OP_RENAME:
        case OP_LINK:
        case OP_SETATTR:
        case OP_SETCLIENTID:
        case OP_SETCLIENTID_CONFIRM:
        /* The seqid-sequenced state ops: byte-exact same-connection replay,
         * layered over the structured Section 9.1.7 per-owner cache (see the
         * comment above). */
        case OP_OPEN:
        case OP_CLOSE:
        case OP_LOCK:
        case OP_LOCKU:
        case OP_OPEN_CONFIRM:
        case OP_OPEN_DOWNGRADE:
        /* Destroys the delegation, so a retransmit would answer BAD_STATEID
         * where the original answered NFS4_OK.  Carries no seqid, so the
         * Section 9.1.7 cache does not cover it. */
        case OP_DELEGRETURN:
            return true;
        default:
            return false;
    } /* switch */
} /* nfs4_v40_op_cacheable */

bool
nfs4_v40_drc_compound_cacheable(const struct COMPOUND4args *args)
{
    uint32_t i;

    for (i = 0; i < args->num_argarray; i++) {
        if (nfs4_v40_op_cacheable(&args->argarray[i])) {
            return true;
        }
    }
    return false;
} /* nfs4_v40_drc_compound_cacheable */

/* ------------------------------------------------------------------ *
*  per-connection cache                                              *
* ------------------------------------------------------------------ */

/* Caller holds drc->lock. */
static struct nfs4_v40_drc_conn *
nfs4_v40_drc_conn_find_locked(
    struct nfs4_v40_drc *drc,
    const void          *conn)
{
    struct nfs4_v40_drc_conn *c;

    HASH_FIND_PTR(drc->conns, &conn, c);
    return c;
} /* nfs4_v40_drc_conn_find_locked */

/* Caller holds drc->lock.  NULL on allocation failure. */
static struct nfs4_v40_drc_conn *
nfs4_v40_drc_conn_get_locked(
    struct nfs4_v40_drc *drc,
    const void          *conn)
{
    struct nfs4_v40_drc_conn *c = nfs4_v40_drc_conn_find_locked(drc, conn);

    if (c) {
        return c;
    }

    c = calloc(1, sizeof(*c));
    if (!c) {
        return NULL;
    }
    c->conn = conn;
    HASH_ADD_PTR(drc->conns, conn, c);
    return c;
} /* nfs4_v40_drc_conn_get_locked */

/* Caller holds drc->lock.  Unlinks and frees the connection's whole cache. */
static void
nfs4_v40_drc_conn_free_locked(
    struct nfs4_v40_drc      *drc,
    struct nfs4_v40_drc_conn *c)
{
    uint32_t i;

    for (i = 0; i < NFS4_V40_DRC_SLOTS; i++) {
        free(c->entries[i].buf);
    }
    drc->bytes -= c->bytes;
    HASH_DEL(drc->conns, c);
    free(c);
} /* nfs4_v40_drc_conn_free_locked */

void
nfs4_v40_drc_cache_insert(
    struct nfs4_v40_drc *drc,
    const void          *conn,
    uint32_t             xid,
    uint64_t             cksum,
    const void          *reply,
    uint32_t             reply_len)
{
    struct nfs4_v40_drc_conn  *c;
    struct nfs4_v40_drc_entry *e;
    uint8_t                   *buf;
    uint32_t                   i;

    if (!conn || !reply_len || reply_len > NFS4_V40_DRC_MAX_REPLY_SIZE) {
        return;
    }

    /* Copy outside the lock; a 64KiB memcpy is not worth serializing. */
    buf = malloc(reply_len);
    if (!buf) {
        return;  /* degrade to a cache miss on the retransmit */
    }
    memcpy(buf, reply, reply_len);

    pthread_mutex_lock(&drc->lock);

    c = nfs4_v40_drc_conn_get_locked(drc, conn);
    if (!c) {
        pthread_mutex_unlock(&drc->lock);
        free(buf);
        return;
    }

    /* A repeat of a request already cached: refresh it in place rather than
     * spending a second slot on the same {xid, cksum}. */
    for (i = 0; i < NFS4_V40_DRC_SLOTS; i++) {
        e = &c->entries[i];
        if (e->buf && e->xid == xid && e->cksum == cksum) {
            drc->bytes -= e->len;
            c->bytes   -= e->len;
            free(e->buf);
            e->buf      = buf;
            e->len      = reply_len;
            drc->bytes += reply_len;
            c->bytes   += reply_len;
            pthread_mutex_unlock(&drc->lock);
            return;
        }
    }

    /* FIFO: claim the slot the cursor points at, evicting whatever is there. */
    e = &c->entries[c->next];
    if (e->buf) {
        drc->bytes -= e->len;
        c->bytes   -= e->len;
        free(e->buf);
    }
    e->xid      = xid;
    e->cksum    = cksum;
    e->buf      = buf;
    e->len      = reply_len;
    c->next     = (c->next + 1) % NFS4_V40_DRC_SLOTS;
    drc->bytes += reply_len;
    c->bytes   += reply_len;

    pthread_mutex_unlock(&drc->lock);
} /* nfs4_v40_drc_cache_insert */

int
nfs4_v40_drc_cache_lookup(
    struct nfs4_v40_drc *drc,
    const void          *conn,
    uint32_t             xid,
    uint64_t             cksum,
    uint8_t            **out_buf,
    uint32_t            *out_len)
{
    struct nfs4_v40_drc_conn  *c;
    struct nfs4_v40_drc_entry *e;
    uint8_t                   *buf = NULL;
    uint32_t                   len = 0;
    uint32_t                   i;

    if (!conn) {
        return 0;
    }

    pthread_mutex_lock(&drc->lock);
    c = nfs4_v40_drc_conn_find_locked(drc, conn);
    if (c) {
        for (i = 0; i < NFS4_V40_DRC_SLOTS; i++) {
            e = &c->entries[i];
            if (e->buf && e->xid == xid && e->cksum == cksum) {
                len = e->len;
                buf = malloc(len);
                if (buf) {
                    memcpy(buf, e->buf, len);
                }
                break;
            }
        }
    }
    pthread_mutex_unlock(&drc->lock);

    if (!buf) {
        return 0;  /* miss, or OOM -- which degrades to re-executing */
    }

    *out_buf = buf;
    *out_len = len;
    return 1;
} /* nfs4_v40_drc_cache_lookup */

void
nfs4_v40_drc_conn_close(
    struct nfs4_v40_drc *drc,
    const void          *conn)
{
    struct nfs4_v40_drc_conn *c;

    if (!conn) {
        return;
    }

    pthread_mutex_lock(&drc->lock);
    c = nfs4_v40_drc_conn_find_locked(drc, conn);
    if (c) {
        nfs4_v40_drc_conn_free_locked(drc, c);
    }
    pthread_mutex_unlock(&drc->lock);
} /* nfs4_v40_drc_conn_close */

/* ------------------------------------------------------------------ *
*  reply capture                                                     *
* ------------------------------------------------------------------ */

struct nfs4_v40_drc_capture_ctx {
    struct nfs4_v40_drc *drc;
    const void          *conn;   /* identity only; never dereferenced */
    uint32_t             xid;
    uint64_t             cksum;
};

static void
nfs4_v40_drc_capture_reply(
    const struct evpl_iovec *iov,
    int                      niov,
    int                      total_length,
    uint32_t                 body_offset,
    void                    *private_data)
{
    struct nfs4_v40_drc_capture_ctx *ctx = private_data;
    uint8_t                         *buf;
    uint32_t                         rpc_len;

    if (total_length <= (int) body_offset ||
        (uint32_t) total_length > NFS4_V40_DRC_MAX_REPLY_SIZE) {
        return;
    }

    /* Store the RPC reply without its transport framing; see
     * nfs_drc_copy_rpc_reply. */
    rpc_len = (uint32_t) total_length - body_offset;

    buf = malloc(rpc_len);
    if (!buf) {
        return;  /* OOM: skip caching this reply (degrade to a cache miss) */
    }

    if (nfs_drc_copy_rpc_reply(iov, niov, body_offset, buf, rpc_len) != rpc_len) {
        free(buf);
        return;
    }

    /* rpc2 runs the capture only for a MSG_ACCEPTED/SUCCESS reply, so a slot is
     * never spent on one that carries no results.  That is also what keeps a
     * COMPOUND that fails XDR decode out of the cache: the generated dispatcher
     * answers GARBAGE_ARGS without ever reaching chimera_nfs4_compound, so
     * nothing disarms the capture, and an unauthenticated peer could otherwise
     * evict every real entry with a stream of malformed requests. */
    nfs4_v40_drc_cache_insert(ctx->drc, ctx->conn, ctx->xid, ctx->cksum,
                              buf, rpc_len);

    free(buf);
} /* nfs4_v40_drc_capture_reply */

void
nfs4_v40_drc_cancel_capture(struct evpl_rpc2_encoding *encoding)
{
    /* Only ours.  The slot is shared with the 4.1 session replay cache
     * (nfs4_replay_arm_capture), which arms later, from the SEQUENCE handler.
     * The capture context is bump-allocated from the request's dbuf, so
     * dropping the pointers is the whole cancellation -- nothing to free. */
    if (encoding && encoding->reply_capture_cb == nfs4_v40_drc_capture_reply) {
        encoding->reply_capture_cb      = NULL;
        encoding->reply_capture_private = NULL;
    }
} /* nfs4_v40_drc_cancel_capture */

/* ------------------------------------------------------------------ *
*  dispatch                                                          *
* ------------------------------------------------------------------ */

static int
nfs4_v40_drc_dispatch(
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
    struct nfs4_v40_drc              *drc    = &thread->shared->v40_drc;
    struct nfs4_v40_drc_capture_ctx  *cctx;
    uint8_t                          *cached;
    uint32_t                          cached_len, mv;
    uint64_t                          cksum;

    /* Only minorversion-0 COMPOUNDs use this cache; NULL and 4.1+ COMPOUNDs
     * (the session reply cache covers those) pass through.
     *
     * RDMA connections used to pass through as well, because the cached reply
     * was the TCP on-wire form and RDMA framing would not match.  The cache now
     * stores the RPC reply with the framing stripped
     * (nfs4_v40_drc_capture_reply), so an RDMA retransmit is served like any
     * other -- it has to be, or a non-idempotent COMPOUND re-executes there. */
    if (proc != NFS4_PROC_COMPOUND ||
        !nfs4_v40_peek_minorversion(iov, niov, &mv) || mv != 0) {
        return drc->orig_dispatch(evpl, conn, encoding, proc, program_data,
                                  cred, iov, niov, length, private_data);
    }

    /* The lookup runs for every 4.0 COMPOUND, read-only ones included, because
     * the operations are not known until the arguments are decoded.  That is
     * safe: only a COMPOUND carrying a non-idempotent operation is ever
     * inserted (chimera_nfs4_compound disarms the capture otherwise, keyed on
     * nfs4_v40_drc_compound_cacheable), and the key covers a checksum of the
     * whole request, so a read-only COMPOUND's bytes cannot match a cached
     * mutating one's.  A read-only request therefore always misses and executes
     * for real.
     *
     * The checksum covers the credential as well as the body: a connection is
     * one client but not necessarily one user, and two users' byte-identical
     * COMPOUNDs are two requests. */
    cksum = nfs3_drc_checksum_cred(nfs3_drc_checksum_iov(iov, niov), cred);

    if (nfs4_v40_drc_cache_lookup(drc, conn, encoding->xid, cksum,
                                  &cached, &cached_len)) {
        int rc = nfs_drc_send_cached_reply(thread, encoding, cached, cached_len);

        free(cached);
        if (rc == 0) {
            return 0;
        }
        /* Unparseable cached reply (should not happen: only SUCCESS replies are
         * inserted).  Fall through and re-execute. */
    }

    /* Miss: arm the capture so this request's reply is cached.  The context is
     * bump-allocated from the request's dbuf, so it lives exactly as long as the
     * request and needs no free.  If the dbuf is exhausted we simply do not
     * cache -- a retransmit then re-executes. */
    cctx = xdr_dbuf_alloc_space(sizeof(*cctx), encoding->dbuf);
    if (cctx) {
        cctx->drc                       = drc;
        cctx->conn                      = conn;
        cctx->xid                       = encoding->xid;
        cctx->cksum                     = cksum;
        encoding->reply_capture_cb      = nfs4_v40_drc_capture_reply;
        encoding->reply_capture_private = cctx;
    }

    return drc->orig_dispatch(evpl, conn, encoding, proc, program_data, cred,
                              iov, niov, length, private_data);
} /* nfs4_v40_drc_dispatch */

/* ------------------------------------------------------------------ *
*  lifecycle                                                         *
* ------------------------------------------------------------------ */

void
nfs4_v40_drc_init(struct nfs4_v40_drc *drc)
{
    pthread_mutex_init(&drc->lock, NULL);
    drc->conns         = NULL;
    drc->bytes         = 0;
    drc->orig_dispatch = NULL;
} /* nfs4_v40_drc_init */

void
nfs4_v40_drc_destroy(struct nfs4_v40_drc *drc)
{
#ifndef __clang_analyzer__
    struct nfs4_v40_drc_conn *c, *tmp;

    /* Teardown is single-threaded (every connection is closed and drained
     * first), so the _locked helper is called without the lock deliberately. */
    HASH_ITER(hh, drc->conns, c, tmp)
    {
        nfs4_v40_drc_conn_free_locked(drc, c);
    }
#endif /* ifndef __clang_analyzer__ */
    pthread_mutex_destroy(&drc->lock);
} /* nfs4_v40_drc_destroy */

void
nfs4_v40_drc_install(struct chimera_server_nfs_shared *shared)
{
    struct nfs4_v40_drc *drc = &shared->v40_drc;

    drc->orig_dispatch                     = shared->nfs_v4.rpc2.recv_call_dispatch;
    shared->nfs_v4.rpc2.recv_call_dispatch = nfs4_v40_drc_dispatch;
} /* nfs4_v40_drc_install */
