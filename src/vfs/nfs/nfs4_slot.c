// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * NFSv4.1 client fore-channel session-slot layer (RFC 8881 §2.10.6).
 *
 * Every NFSv4.1 COMPOUND the client sends carries a SEQUENCE op naming a session
 * slot and that slot's sequenceid.  The protocol allows only ONE outstanding
 * request per slot at a time, and the slot's sequenceid must advance by exactly
 * one per accepted request; sending a second request on a busy slot (or with a
 * mismatched seqid) is rejected with NFS4ERR_SEQ_MISORDERED.  The previous client
 * gave each thread a single slot and merely incremented its seqid per call, so a
 * thread with more than one COMPOUND in flight (pipelined I/O, or back-to-back
 * compounds) collided.
 *
 * This layer fixes that.  The session owns the global slot space (max_slots,
 * granted by CREATE_SESSION).  Each per-thread connection
 * (chimera_nfs_client_server_thread) claims a disjoint block of slot indices
 * once and manages them entirely thread-locally -- a server_thread is touched by
 * exactly one evpl thread, so acquire/release/park/wake need no locking; only
 * the one-time block claim takes session->lock.  When all of a thread's slots
 * are busy, further requests are parked and replayed (same thread) as slots
 * free, never sent on a busy slot.
 */

#include <stdlib.h>

#include "nfs_internal.h"

/* Upper bound on a thread's slot block, so an early claim (when few threads have
 * registered yet) cannot grab the whole pool and starve later threads. */
#define CHIMERA_NFS4_MAX_SLOTS_PER_THREAD 16

/* In-flight wrapper context: what rpc2 carries as private_data so the reply can
 * free the slot and then invoke the caller's real callback. */
struct chimera_nfs4_compound_ctx {
    struct chimera_nfs_client_server_thread *server_thread;
    uint32_t                                 slot_local;   /* index into slots[] */
    uint32_t                                 seqid;        /* seqid we sent       */
    void                                     (*real_cb)(
        struct evpl *,
        const struct evpl_rpc2_verf *,
        struct COMPOUND4res *,
        int,
        void *);
    void                                    *real_private;
    struct chimera_nfs4_compound_ctx        *prev, *next;  /* inflight dll        */
    struct chimera_nfs4_compound_ctx        *fl_next;      /* freelist            */
};

/* A request parked because no slot was free; replayed when one frees. */
struct chimera_nfs4_parked {
    struct chimera_nfs_thread  *thread;
    struct chimera_nfs_shared  *shared;
    struct chimera_vfs_request *request;
    chimera_nfs4_retry_fn       retry_fn;
    void                       *retry_ctx;
    struct chimera_nfs4_parked *next;
};

/* ---- per-thread pools (no locking; single owning evpl thread) ----------- */

static struct chimera_nfs4_compound_ctx *
chimera_nfs4_ctx_alloc(struct chimera_nfs4_slot_table *st)
{
    struct chimera_nfs4_compound_ctx *ctx = st->ctx_freelist;

    if (ctx) {
        st->ctx_freelist = ctx->fl_next;
    } else {
        ctx = calloc(1, sizeof(*ctx));
    }
    return ctx;
} /* chimera_nfs4_ctx_alloc */

static void
chimera_nfs4_ctx_free(
    struct chimera_nfs4_slot_table   *st,
    struct chimera_nfs4_compound_ctx *ctx)
{
    ctx->fl_next     = st->ctx_freelist;
    st->ctx_freelist = ctx;
} /* chimera_nfs4_ctx_free */

static void
chimera_nfs4_inflight_add(
    struct chimera_nfs4_slot_table   *st,
    struct chimera_nfs4_compound_ctx *ctx)
{
    ctx->prev = NULL;
    ctx->next = st->inflight;
    if (st->inflight) {
        st->inflight->prev = ctx;
    }
    st->inflight = ctx;
} /* chimera_nfs4_inflight_add */

static void
chimera_nfs4_inflight_remove(
    struct chimera_nfs4_slot_table   *st,
    struct chimera_nfs4_compound_ctx *ctx)
{
    if (ctx->prev) {
        ctx->prev->next = ctx->next;
    } else {
        st->inflight = ctx->next;
    }
    if (ctx->next) {
        ctx->next->prev = ctx->prev;
    }
    ctx->prev = ctx->next = NULL;
} /* chimera_nfs4_inflight_remove */

static void
chimera_nfs4_park(
    struct chimera_nfs4_slot_table *st,
    struct chimera_nfs_thread      *thread,
    struct chimera_nfs_shared      *shared,
    struct chimera_vfs_request     *request,
    chimera_nfs4_retry_fn           retry_fn,
    void                           *retry_ctx)
{
    struct chimera_nfs4_parked *p = st->parked_freelist;

    if (p) {
        st->parked_freelist = p->next;
    } else {
        p = calloc(1, sizeof(*p));
    }

    p->thread    = thread;
    p->shared    = shared;
    p->request   = request;
    p->retry_fn  = retry_fn;
    p->retry_ctx = retry_ctx;
    p->next      = NULL;

    if (st->wait_tail) {
        st->wait_tail->next = p;
    } else {
        st->wait_head = p;
    }
    st->wait_tail = p;
} /* chimera_nfs4_park */

static struct chimera_nfs4_parked *
chimera_nfs4_park_pop(struct chimera_nfs4_slot_table *st)
{
    struct chimera_nfs4_parked *p = st->wait_head;

    if (p) {
        st->wait_head = p->next;
        if (!st->wait_head) {
            st->wait_tail = NULL;
        }
    }
    return p;
} /* chimera_nfs4_park_pop */

/* ---- slot table block claim + acquire/release --------------------------- */

static void
chimera_nfs4_slot_table_init(
    struct chimera_nfs4_slot_table     *st,
    struct chimera_nfs4_client_session *session,
    struct chimera_nfs_shared          *shared)
{
    uint32_t fair, remaining, takeable, base, n, i;
    int      nthreads, others;

    nthreads = atomic_load(&shared->nfs_thread_count);
    if (nthreads < 1) {
        nthreads = 1;
    }

    /* Fair share of the global slot pool for one thread (>=1), bounded so an
     * early claim does not grab a huge block while few threads have registered. */
    fair = session->max_slots / (uint32_t) nthreads;
    if (fair < 1) {
        fair = 1;
    }
    if (fair > CHIMERA_NFS4_MAX_SLOTS_PER_THREAD) {
        fair = CHIMERA_NFS4_MAX_SLOTS_PER_THREAD;
    }

    pthread_mutex_lock(&session->lock);

    remaining = session->max_slots - session->next_unclaimed;

    /* Invariant: the session must have at least as many slots as client threads,
     * so every thread gets at least one private slot.  If a thread cannot claim
     * even one slot, that invariant is violated -- abort loudly rather than
     * silently aliasing a slot across threads (which breaks one-outstanding-per-
     * slot and yields NFS4ERR_SEQ_MISORDERED).  Provision more slots via the
     * `slots=` mount option and/or server.nfs4_session_slots. */
    chimera_nfsclient_abort_if(remaining == 0,
                               "NFS4 session fore-channel slots exhausted: max_slots=%u but the "
                               "client has %d threads -- every thread needs at least one slot.  "
                               "Raise the `slots=` mount option and/or server.nfs4_session_slots "
                               "above the client thread count.",
                               session->max_slots, nthreads);

    /* Reserve one slot for each thread that has registered but not yet claimed a
     * block, so this (possibly early) claim cannot starve a later thread and
     * trip the abort above even though max_slots > threads. */
    session->claimed_blocks++;
    others = nthreads - (int) session->claimed_blocks;
    if (others < 0) {
        others = 0;
    }
    if ((uint32_t) others >= remaining) {
        takeable = 1;                       /* tight: one slot per remaining thread */
    } else {
        takeable = remaining - (uint32_t) others;
    }

    n                        = fair < takeable ? fair : takeable; /* both >= 1, and n <= remaining */
    base                     = session->next_unclaimed;
    session->next_unclaimed += n;

    pthread_mutex_unlock(&session->lock);

    st->slots      = calloc(n, sizeof(*st->slots));
    st->free_stack = calloc(n, sizeof(*st->free_stack));
    st->num_slots  = n;
    st->free_top   = (int) n;

    for (i = 0; i < n; i++) {
        st->slots[i].global_id = base + i;
        st->slots[i].seqid     = 1;             /* RFC 8881: first request seqid 1 */
        st->slots[i].in_use    = 0;
        st->free_stack[i]      = n - 1 - i;     /* hand out low indices first      */
    }

    st->initialized = 1;
} /* chimera_nfs4_slot_table_init */

/* Returns a free local slot index, or -1 if all owned slots are busy. */
static int
chimera_nfs4_slot_acquire(struct chimera_nfs4_slot_table *st)
{
    uint32_t local;

    if (st->free_top == 0) {
        return -1;
    }

    local                   = st->free_stack[--st->free_top];
    st->slots[local].in_use = 1;
    return (int) local;
} /* chimera_nfs4_slot_acquire */

static void
chimera_nfs4_slot_release(
    struct chimera_nfs4_slot_table *st,
    uint32_t                        local)
{
    st->slots[local].in_use        = 0;
    st->free_stack[st->free_top++] = local;
} /* chimera_nfs4_slot_release */

/* ---- the reply wrapper -------------------------------------------------- */

static void
chimera_nfs4_compound_call_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_nfs4_compound_ctx *ctx  = private_data;
    struct chimera_nfs4_slot_table   *st   = &ctx->server_thread->slots;
    struct chimera_nfs4_slot         *slot = &st->slots[ctx->slot_local];
    struct chimera_nfs4_parked       *p;

    void                              (*real_cb)(
        struct evpl *,
        const struct evpl_rpc2_verf *,
        struct COMPOUND4res *,
        int,
        void *);
    void                             *real_private;

    chimera_nfs4_inflight_remove(st, ctx);

    /* Advance the slot's seqid only when the server accepted the SEQUENCE
    * (RFC 8881 §2.10.6.1).  On a transport error or a SEQUENCE error, leave it
    * so the slot stays replayable; we surface the error to the caller. */
    if (status == 0 && res && res->num_resarray >= 1 &&
        res->resarray[0].opsequence.sr_status == NFS4_OK) {
        slot->seqid = ctx->seqid + 1;
    }

    chimera_nfs4_slot_release(st, ctx->slot_local);

    real_cb      = ctx->real_cb;
    real_private = ctx->real_private;
    chimera_nfs4_ctx_free(st, ctx);

    /* Deliver to the caller first: a follow-on COMPOUND it issues (e.g. a close
     * sending LAYOUTRETURN after LAYOUTCOMMIT) gets the just-freed slot. */
    real_cb(evpl, verf, res, status, real_private);

    /* Then wake parked requests for any slots still free.  Each replay
     * re-dispatches the op, which re-enters chimera_nfs4_compound_call and
     * acquires a slot (re-entrant same-thread send is safe). */
    while (st->free_top > 0 && st->wait_head) {
        p = chimera_nfs4_park_pop(st);
        chimera_nfs4_retry_fn       rf = p->retry_fn;
        struct chimera_nfs_thread  *t  = p->thread;
        struct chimera_nfs_shared  *sh = p->shared;
        struct chimera_vfs_request *rq = p->request;
        void                       *rc = p->retry_ctx;
        p->next             = st->parked_freelist;
        st->parked_freelist = p;
        rf(t, sh, rq, rc);
    }
} /* chimera_nfs4_compound_call_cb */

void
chimera_nfs4_compound_call(
    struct chimera_nfs_thread *thread,
    struct chimera_nfs_shared *shared,
    struct chimera_nfs_client_server_thread *server_thread,
    struct chimera_vfs_request *request,
    struct COMPOUND4args *args,
    const struct evpl_rpc2_cred *cred,
    int ddp,
    int max_rdma_write_chunk,
    struct evpl_iovec *write_chunk_iov,
    int write_chunk_niov,
    int max_rdma_reply_chunk,
    void ( *cb )(struct evpl *, const struct evpl_rpc2_verf *,
                 struct COMPOUND4res *, int, void *),
    void *cb_private,
    chimera_nfs4_retry_fn retry_fn,
    void *retry_ctx)
{
    struct chimera_nfs4_client_session *session = server_thread->server->nfs4_session;
    struct chimera_nfs4_slot_table     *st      = &server_thread->slots;
    struct chimera_nfs4_slot           *slot;
    struct chimera_nfs4_compound_ctx   *ctx;
    int                                 local;

    if (unlikely(!st->initialized)) {
        chimera_nfs4_slot_table_init(st, session, shared);
    }

    local = chimera_nfs4_slot_acquire(st);
    if (local < 0) {
        /* No slot free: park and replay when one frees.  Must not send on a
         * busy slot (that is the NFS4ERR_SEQ_MISORDERED bug). */
        chimera_nfs4_park(st, thread, shared, request, retry_fn, retry_ctx);
        return;
    }

    slot = &st->slots[local];

    args->argarray[0].argop = OP_SEQUENCE;
    memcpy(args->argarray[0].opsequence.sa_sessionid, session->sessionid, NFS4_SESSIONID_SIZE);
    args->argarray[0].opsequence.sa_sequenceid     = slot->seqid;
    args->argarray[0].opsequence.sa_slotid         = slot->global_id;
    args->argarray[0].opsequence.sa_highest_slotid = session->max_slots - 1;
    args->argarray[0].opsequence.sa_cachethis      = 0;

    ctx                = chimera_nfs4_ctx_alloc(st);
    ctx->server_thread = server_thread;
    ctx->slot_local    = (uint32_t) local;
    ctx->seqid         = slot->seqid;
    ctx->real_cb       = cb;
    ctx->real_private  = cb_private;
    chimera_nfs4_inflight_add(st, ctx);

    shared->nfs_v4.send_call_NFSPROC4_COMPOUND(
        &shared->nfs_v4.rpc2,
        thread->evpl,
        server_thread->nfs_conn,
        cred,
        args,
        ddp, max_rdma_write_chunk, write_chunk_iov, write_chunk_niov, max_rdma_reply_chunk,
        chimera_nfs4_compound_call_cb,
        ctx);
} /* chimera_nfs4_compound_call */

void
chimera_nfs4_slot_table_destroy(struct chimera_nfs4_slot_table *st)
{
    struct chimera_nfs4_compound_ctx *ctx;
    struct chimera_nfs4_parked       *p;

    while (st->ctx_freelist) {
        ctx              = st->ctx_freelist;
        st->ctx_freelist = ctx->fl_next;
        free(ctx);
    }
    /* Any still-inflight ctxs are owned by pending replies; on teardown the
     * connection is gone, so just free them. */
    while (st->inflight) {
        ctx          = st->inflight;
        st->inflight = ctx->next;
        free(ctx);
    }
    while (st->parked_freelist) {
        p                   = st->parked_freelist;
        st->parked_freelist = p->next;
        free(p);
    }
    while (st->wait_head) {
        p             = st->wait_head;
        st->wait_head = p->next;
        free(p);
    }
    st->wait_tail = NULL;
    free(st->slots);
    free(st->free_stack);
    st->slots       = NULL;
    st->free_stack  = NULL;
    st->num_slots   = 0;
    st->free_top    = 0;
    st->initialized = 0;
} /* chimera_nfs4_slot_table_destroy */

void
chimera_nfs4_slot_table_reset(
    struct evpl                    *evpl,
    struct chimera_nfs4_slot_table *st)
{
    struct chimera_nfs4_compound_ctx *ctx;
    struct chimera_nfs4_parked       *p;
    uint32_t                          i;

    if (!st->initialized) {
        return;
    }

    /* Error-complete everything that was in flight on the dropped connection.
     * rpc2 already freed the underlying calls without invoking their callbacks,
     * so we must run them here (non-zero status -> the op completes with an
     * error rather than hanging). */
    while (st->inflight) {
        ctx          = st->inflight;
        st->inflight = ctx->next;
        ctx->prev    = ctx->next = NULL;
        ctx->real_cb(evpl, NULL, NULL, 1 /* error */, ctx->real_private);
        chimera_nfs4_ctx_free(st, ctx);
    }

    /* Parked requests never went on the wire; error-complete them too. */
    while ((p = chimera_nfs4_park_pop(st)) != NULL) {
        p->request->status = CHIMERA_VFS_EIO;
        p->request->complete(p->request);
        p->next             = st->parked_freelist;
        st->parked_freelist = p;
    }

    /* All slots are free again.  seqids are left as-is: a lost request the
    * server did process would yield NFS4ERR_SEQ_MISORDERED on the next reuse,
    * which we surface as an error (full recovery is a separate effort). */
    for (i = 0; i < st->num_slots; i++) {
        st->slots[i].in_use = 0;
        st->free_stack[i]   = st->num_slots - 1 - i;
    }
    st->free_top = (int) st->num_slots;
} /* chimera_nfs4_slot_table_reset */
