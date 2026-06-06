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
 * mismatched seqid) is rejected with NFS4ERR_SEQ_MISORDERED.
 *
 * Slots are shared cooperatively across the client's evpl threads.  The session
 * owns the global slot space (max_slots, granted by CREATE_SESSION) as a pool of
 * free slot ids plus a per-id seqid array.  Each per-thread connection keeps:
 *   - a FLOOR of slots claimed once and never returned (>=1, so the thread always
 *     owns a slot and can always make progress -- see the wakeup note below), and
 *   - a thread-local stack of additional BORROWED ids it tops up from / returns to
 *     the shared pool in BATCHES under session->lock.
 * Steady-state acquire/release is a thread-local stack pop/push (no lock, no
 * atomics); the session lock is taken only on a batch borrow/return/floor-claim or
 * when the server changes the slot-use limit -- amortized ~1/REFILL_BATCH, never
 * per operation.  A busy thread can therefore run far more than its even share in
 * flight while idle threads hand slots back.
 *
 * When a thread momentarily has no free owned slot it PARKS the request and
 * replays it from its OWN reply callback as one of its in-flight slots frees.
 * Parked requests are only ever woken from the owning thread's reply callback
 * (there is no cross-thread wakeup), so a thread must always own >=1 slot or a
 * parked request could hang with nothing to wake it.  The floor guarantees this;
 * it requires usable >= live_threads, enforced by a hard abort at floor-claim.
 *
 * Dynamic limit (RFC 8881 §2.10.6.1): the server advertises a target slot-use
 * limit via sr_target_highest_slotid.  The client tracks it as `usable` and drains
 * its working set toward it -- the borrow pool only lends ids < usable and retires
 * higher ids until the target grows again -- while floor ids (always valid, < the
 * negotiated max_slots) keep every thread alive.
 */

#include <stdlib.h>

#include "nfs_internal.h"

/* Slot id a thread keeps for life (never returned except at teardown). */
#define CHIMERA_NFS4_FLOOR_SLOTS  1u
/* Slots grabbed/returned per shared-pool transaction (amortizes the lock). */
#define CHIMERA_NFS4_REFILL_BATCH 8u
#define CHIMERA_NFS4_RETURN_BATCH 8u
/* A thread returns idle slots once it is sitting on more than HIGH_WATER free,
 * draining down to LOW_WATER.  HIGH_WATER > FLOOR + REFILL_BATCH avoids a single
 * borrow immediately tripping a return (thrash). */
#define CHIMERA_NFS4_HIGH_WATER   16
#define CHIMERA_NFS4_LOW_WATER    8
/* Slack above the even per-thread share a thread may borrow, so it can burst into
 * idle capacity without monopolizing the pool. */
#define CHIMERA_NFS4_CAP_SLACK    4u

/* In-flight wrapper context: what rpc2 carries as private_data so the reply can
 * free the slot and then invoke the caller's real callback. */
struct chimera_nfs4_compound_ctx {
    struct chimera_nfs_client_server_thread *server_thread;
    uint32_t                                 slot_id;      /* global slot id sent */
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

/* ---- session-global slot pool (all helpers run under session->lock) ----- */

void
chimera_nfs4_session_pool_init(struct chimera_nfs4_client_session *session)
{
    uint32_t i, n = session->max_slots;

    session->slot_seqid  = calloc(n, sizeof(*session->slot_seqid));
    session->free_ids    = calloc(n, sizeof(*session->free_ids));
    session->retired_ids = calloc(n, sizeof(*session->retired_ids));

    /* All ids start usable and free; push so low ids come off the top first
     * (floors/borrows then cluster at the low end of the id space). */
    for (i = 0; i < n; i++) {
        session->slot_seqid[i] = 1;                 /* RFC 8881: first seqid is 1 */
        session->free_ids[i]   = n - 1 - i;
    }
    session->free_count     = n;
    session->retired_count  = 0;
    session->claimed_floors = 0;
    atomic_store(&session->usable, n);
    atomic_store(&session->target_usable, n);
} /* chimera_nfs4_session_pool_init */

void
chimera_nfs4_session_pool_destroy(struct chimera_nfs4_client_session *session)
{
    free(session->slot_seqid);
    free(session->free_ids);
    free(session->retired_ids);
    session->slot_seqid  = NULL;
    session->free_ids    = NULL;
    session->retired_ids = NULL;
} /* chimera_nfs4_session_pool_destroy */

/* Apply a pending server slot-use-limit change (sr_target).  Shrinking moves
 * now-out-of-range free ids aside to retired_ids; growing brings them back.
 * usable is clamped to >= claimed_floors (we can never reclaim a thread's floor
 * slot) and <= max_slots.  O(pool) but runs only when the target actually
 * changes -- never on the steady-state hot path. */
static void
chimera_nfs4_pool_apply_target(struct chimera_nfs4_client_session *session)
{
    uint32_t want = atomic_load_explicit(&session->target_usable, memory_order_relaxed);
    uint32_t cur  = atomic_load_explicit(&session->usable, memory_order_relaxed);
    uint32_t floor_min, i, w;

    floor_min = session->claimed_floors ? session->claimed_floors : 1;
    if (want < floor_min) {
        want = floor_min;
    }
    if (want > session->max_slots) {
        want = session->max_slots;
    }
    if (want == cur) {
        return;
    }

    if (want < cur) {
        /* Shrink: retire free ids that are now >= want. */
        w = 0;
        for (i = 0; i < session->free_count; i++) {
            uint32_t id = session->free_ids[i];
            if (id < want) {
                session->free_ids[w++] = id;
            } else {
                session->retired_ids[session->retired_count++] = id;
            }
        }
        session->free_count = w;
    } else {
        /* Grow: bring retired ids that are now < want back into circulation. */
        w = 0;
        for (i = 0; i < session->retired_count; i++) {
            uint32_t id = session->retired_ids[i];
            if (id < want) {
                session->free_ids[session->free_count++] = id;
            } else {
                session->retired_ids[w++] = id;
            }
        }
        session->retired_count = w;
    }

    atomic_store_explicit(&session->usable, want, memory_order_relaxed);
} /* chimera_nfs4_pool_apply_target */

/* Borrow up to `want` ids from the shared pool into the thread's local_free,
 * reserving one slot for every live thread that has not yet claimed its floor so
 * a late floor-claim cannot be starved.  Returns the number borrowed. */
static uint32_t
chimera_nfs4_pool_borrow(
    struct chimera_nfs4_client_session *session,
    struct chimera_nfs4_slot_table     *st,
    struct chimera_nfs_shared          *shared,
    uint32_t                            want)
{
    uint32_t reserve, avail, take, i;
    int      live;

    chimera_nfs4_pool_apply_target(session);

    live = atomic_load(&shared->nfs_thread_count);
    if (live < 0) {
        live = 0;
    }
    reserve = (uint32_t) live > session->claimed_floors ?
        (uint32_t) live - session->claimed_floors : 0;

    avail = session->free_count > reserve ? session->free_count - reserve : 0;
    take  = want < avail ? want : avail;

    for (i = 0; i < take; i++) {
        st->local_free[st->local_free_top++] = session->free_ids[--session->free_count];
    }
    st->leased += take;
    return take;
} /* chimera_nfs4_pool_borrow */

/* Return `count` idle ids from the thread's local_free back to the shared pool
 * (or retired, if the limit shrank below them). */
static void
chimera_nfs4_pool_return(
    struct chimera_nfs4_client_session *session,
    struct chimera_nfs4_slot_table     *st,
    uint32_t                            count)
{
    uint32_t usable, i;

    chimera_nfs4_pool_apply_target(session);
    usable = atomic_load_explicit(&session->usable, memory_order_relaxed);

    for (i = 0; i < count; i++) {
        uint32_t id = st->local_free[--st->local_free_top];
        if (id < usable) {
            session->free_ids[session->free_count++] = id;
        } else {
            session->retired_ids[session->retired_count++] = id;
        }
    }
    st->leased -= count;
} /* chimera_nfs4_pool_return */

/* ---- per-thread slot table: floor claim + reset/destroy ----------------- */

static void
chimera_nfs4_slot_table_init(
    struct chimera_nfs4_slot_table     *st,
    struct chimera_nfs4_client_session *session,
    struct chimera_nfs_shared          *shared)
{
    uint32_t i;
    int      live;

    st->local_free     = calloc(session->max_slots, sizeof(*st->local_free));
    st->local_free_top = 0;
    st->leased         = 0;
    st->floor          = 0;
    st->cached_target  = atomic_load(&session->target_usable);

    pthread_mutex_lock(&session->lock);

    chimera_nfs4_pool_apply_target(session);

    live = atomic_load(&shared->nfs_thread_count);
    if (live < 1) {
        live = 1;
    }

    /* Claim the per-thread floor.  Requires the session to have at least as many
     * usable slots as live client threads; if a thread cannot get even one slot,
     * that invariant is violated -- abort loudly (parked requests would have no
     * in-flight slot to wake them).  Raise `slots=` / server.nfs4_session_slots
     * above the client thread count. */
    chimera_nfsclient_abort_if(session->free_count == 0,
                               "NFS4 session fore-channel slots exhausted: max_slots=%u but the "
                               "client has %d threads -- every thread needs at least one slot.  "
                               "Raise the `slots=` mount option and/or server.nfs4_session_slots "
                               "above the client thread count.",
                               session->max_slots, live);

    session->claimed_floors++;
    for (i = 0; i < CHIMERA_NFS4_FLOOR_SLOTS && session->free_count > 0; i++) {
        st->local_free[st->local_free_top++] = session->free_ids[--session->free_count];
        st->leased++;
        st->floor++;
    }

    pthread_mutex_unlock(&session->lock);

    st->initialized = 1;
} /* chimera_nfs4_slot_table_init */

void
chimera_nfs4_slot_table_reset(
    struct evpl                    *evpl,
    struct chimera_nfs4_slot_table *st)
{
    struct chimera_nfs4_compound_ctx *ctx;
    struct chimera_nfs4_parked       *p;

    if (!st->initialized) {
        return;
    }

    /* Error-complete everything that was in flight on the dropped connection.
     * rpc2 already freed the underlying calls without invoking their callbacks,
     * so we run them here (non-zero status -> the op completes with an error
     * rather than hanging).  The slot id returns to this thread's local_free --
     * NOT to the shared pool: the id is still leased to this thread and would be
     * double-issued if another thread borrowed it. */
    while (st->inflight) {
        ctx                                  = st->inflight;
        st->inflight                         = ctx->next;
        ctx->prev                            = ctx->next = NULL;
        st->local_free[st->local_free_top++] = ctx->slot_id;
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

    /* seqids are left as-is: a lost request the server did process would yield
     * NFS4ERR_SEQ_MISORDERED on the next reuse, surfaced as an error (full
     * session recovery is a separate effort). */
} /* chimera_nfs4_slot_table_reset */

void
chimera_nfs4_slot_table_destroy(struct chimera_nfs_client_server_thread *server_thread)
{
    struct chimera_nfs4_slot_table     *st      = &server_thread->slots;
    struct chimera_nfs4_client_session *session =
        server_thread->server ? server_thread->server->nfs4_session : NULL;
    struct chimera_nfs4_compound_ctx   *ctx;
    struct chimera_nfs4_parked         *p;

    if (st->initialized && session) {
        /* Reclaim any still-inflight ctxs (reset normally drained them first);
         * their ids return to local_free, then the whole lease goes back to the
         * shared pool so a torn-down thread does not leak slots. */
        while (st->inflight) {
            ctx                                  = st->inflight;
            st->inflight                         = ctx->next;
            st->local_free[st->local_free_top++] = ctx->slot_id;
        }

        pthread_mutex_lock(&session->lock);
        chimera_nfs4_pool_apply_target(session);
        {
            uint32_t usable = atomic_load_explicit(&session->usable, memory_order_relaxed);
            while (st->local_free_top > 0) {
                uint32_t id = st->local_free[--st->local_free_top];
                if (id < usable) {
                    session->free_ids[session->free_count++] = id;
                } else {
                    session->retired_ids[session->retired_count++] = id;
                }
            }
        }
        if (session->claimed_floors >= st->floor) {
            session->claimed_floors -= st->floor;
        }
        st->leased = 0;
        st->floor  = 0;
        pthread_mutex_unlock(&session->lock);
    }

    while (st->ctx_freelist) {
        ctx              = st->ctx_freelist;
        st->ctx_freelist = ctx->fl_next;
        free(ctx);
    }
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
    while ((p = chimera_nfs4_park_pop(st)) != NULL) {
        free(p);
    }
    st->wait_tail = NULL;
    free(st->local_free);
    st->local_free     = NULL;
    st->local_free_top = 0;
    st->initialized    = 0;
} /* chimera_nfs4_slot_table_destroy */

/* ---- the wrapper + reply handler ---------------------------------------- */

static void
chimera_nfs4_compound_call_cb(
    struct evpl                 *evpl,
    const struct evpl_rpc2_verf *verf,
    struct COMPOUND4res         *res,
    int                          status,
    void                        *private_data)
{
    struct chimera_nfs4_compound_ctx        *ctx           = private_data;
    struct chimera_nfs_client_server_thread *server_thread = ctx->server_thread;
    struct chimera_nfs4_slot_table          *st            = &server_thread->slots;
    struct chimera_nfs4_client_session      *session       = server_thread->server->nfs4_session;
    struct chimera_nfs4_parked              *p;

    void                                     (*real_cb)(
        struct evpl *,
        const struct evpl_rpc2_verf *,
        struct COMPOUND4res *,
        int,
        void *);
    void                                    *real_private;

    chimera_nfs4_inflight_remove(st, ctx);

    /* Advance the slot's seqid only when the server accepted the SEQUENCE
     * (RFC 8881 §2.10.6.1).  On a transport error or a SEQUENCE error, leave it
     * so the slot stays replayable; we surface the error to the caller.  This
     * write happens-before the id can be returned to the shared pool below. */
    if (status == 0 && res && res->num_resarray >= 1 &&
        res->resarray[0].opsequence.sr_status == NFS4_OK) {
        session->slot_seqid[ctx->slot_id] = ctx->seqid + 1;

        /* Track a server-driven slot-use-limit change cheaply (plain compare);
         * the actual pool adjustment is applied under the lock at the next
         * batch borrow/return. */
        uint32_t target = res->resarray[0].opsequence.sr_resok4.sr_target_highest_slotid + 1;
        if (target != st->cached_target) {
            st->cached_target = target;
            atomic_store_explicit(&session->target_usable, target, memory_order_relaxed);
        }
    }

    /* The freed id returns to this thread's local stack (lock-free). */
    st->local_free[st->local_free_top++] = ctx->slot_id;

    /* If we are sitting on plenty of idle slots and have no one waiting, hand a
     * batch back to the shared pool so other threads can use them.  (A high free
     * count already means we are not slot-starved; an empty park FIFO means no
     * pending demand.)  Done before real_cb / the park drain so the lock is never
     * held across a send. */
    if (st->local_free_top > CHIMERA_NFS4_HIGH_WATER && st->wait_head == NULL) {
        uint32_t returnable = (uint32_t) st->local_free_top - CHIMERA_NFS4_LOW_WATER;
        uint32_t keep_floor = st->leased > st->floor ? st->leased - st->floor : 0;
        uint32_t cnt        = returnable < CHIMERA_NFS4_RETURN_BATCH ? returnable : CHIMERA_NFS4_RETURN_BATCH;

        if (cnt > keep_floor) {
            cnt = keep_floor;
        }
        if (cnt > 0) {
            pthread_mutex_lock(&session->lock);
            chimera_nfs4_pool_return(session, st, cnt);
            pthread_mutex_unlock(&session->lock);
        }
    }

    real_cb      = ctx->real_cb;
    real_private = ctx->real_private;
    chimera_nfs4_ctx_free(st, ctx);

    /* Deliver to the caller first: a follow-on COMPOUND it issues (e.g. a close
     * sending LAYOUTRETURN after LAYOUTCOMMIT) gets the just-freed slot. */
    real_cb(evpl, verf, res, status, real_private);

    /* Then wake parked requests for any slots still free.  Each replay
     * re-dispatches the op, which re-enters chimera_nfs4_compound_call and
     * acquires a slot (re-entrant same-thread send is safe). */
    while (st->local_free_top > 0 && st->wait_head) {
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
    struct chimera_nfs4_compound_ctx   *ctx;
    uint32_t                            id, usable;

    if (unlikely(!st->initialized)) {
        chimera_nfs4_slot_table_init(st, session, shared);
    }

    /* Acquire a free owned slot.  Hot path: pop the thread-local stack.  If empty,
     * top up a batch from the shared pool -- but only up to a soft fair cap, so a
     * greedy thread bursts into idle capacity without monopolizing the pool. */
    if (st->local_free_top == 0) {
        int      live  = atomic_load(&shared->nfs_thread_count);
        uint32_t share = atomic_load_explicit(&session->usable, memory_order_relaxed) /
            (uint32_t) (live > 0 ? live : 1);
        uint32_t fair_cap = (share > CHIMERA_NFS4_FLOOR_SLOTS ? share : CHIMERA_NFS4_FLOOR_SLOTS) +
            CHIMERA_NFS4_CAP_SLACK;

        if (st->leased < fair_cap) {
            uint32_t want = fair_cap - st->leased;
            if (want > CHIMERA_NFS4_REFILL_BATCH) {
                want = CHIMERA_NFS4_REFILL_BATCH;
            }
            pthread_mutex_lock(&session->lock);
            chimera_nfs4_pool_borrow(session, st, shared, want);
            pthread_mutex_unlock(&session->lock);
        }

        if (st->local_free_top == 0) {
            /* No slot free and at/over the fair cap (or pool empty): park and
             * replay when one of this thread's in-flight slots frees.  The floor
             * guarantees there is always such an in-flight slot. */
            chimera_nfs4_park(st, thread, shared, request, retry_fn, retry_ctx);
            return;
        }
    }

    id     = st->local_free[--st->local_free_top];
    usable = atomic_load_explicit(&session->usable, memory_order_relaxed);

    args->argarray[0].argop = OP_SEQUENCE;
    memcpy(args->argarray[0].opsequence.sa_sessionid, session->sessionid, NFS4_SESSIONID_SIZE);
    args->argarray[0].opsequence.sa_sequenceid = session->slot_seqid[id];
    args->argarray[0].opsequence.sa_slotid     = id;
    /* Advertise the highest slot id we might use: the live limit, but never below
     * the id we are actually sending on (a floor id may briefly exceed `usable`
     * after a shrink -- still < the negotiated max, so always valid on the wire). */
    args->argarray[0].opsequence.sa_highest_slotid = (id >= usable ? id : usable - 1);
    args->argarray[0].opsequence.sa_cachethis      = 0;

    ctx                = chimera_nfs4_ctx_alloc(st);
    ctx->server_thread = server_thread;
    ctx->slot_id       = id;
    ctx->seqid         = session->slot_seqid[id];
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
