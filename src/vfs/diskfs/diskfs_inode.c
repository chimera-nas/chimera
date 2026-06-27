// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Inode cache: sharded rb-trees of in-memory inodes, logical read/write
 * transaction locks (grant/waiter machinery), LRU and recycling, deferred
 * mtime flushing, and the inode-generation epoch that makes inum reuse safe
 * against stale file handles.
 */

#include "diskfs_internal.h"

/* Forward declarations (definitions below, in call-graph order) */

static inline struct diskfs_inode_waiter *
diskfs_waiter_alloc(
    struct diskfs_thread *thread);

static inline void
diskfs_waiter_free(
    struct diskfs_thread       *thread,
    struct diskfs_inode_waiter *w);

static inline int
diskfs_inode_lock_compatible(
    struct diskfs_inode        *inode,
    enum diskfs_inode_lock_mode mode);

static inline void
diskfs_inode_lock_grant(
    struct diskfs_inode_shard  *shard,
    struct diskfs_inode        *inode,
    enum diskfs_inode_lock_mode mode,
    struct diskfs_txn          *txn);

static void
diskfs_dispatch_grant(
    struct diskfs_inode_waiter *w);

static void
diskfs_inode_acquire_contended(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    uint64_t                    inum,
    uint32_t                    gen,
    enum diskfs_inode_lock_mode mode,
    diskfs_inode_cb_t           cb,
    void                       *private_data);

static void
diskfs_gen_extend_complete(
    struct evpl *evpl,
    int          status,
    void        *private_data);

static inline int
diskfs_mtime_owns_shard(
    const struct diskfs_thread *thread,
    uint32_t                    shard);

static struct diskfs_inode *
diskfs_mtime_flush_pick(
    struct diskfs_thread *thread);

static void
diskfs_mtime_flush_drop_pin(
    struct diskfs_thread *thread,
    struct diskfs_inode  *inode);

static void
diskfs_mtime_flush_committed_cb(
    struct diskfs_txn *txn,
    int                status,
    void              *priv);

static void
diskfs_mtime_flush_acquired_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *priv);


static inline struct diskfs_inode_waiter *
diskfs_waiter_alloc(struct diskfs_thread *thread)
{
    struct diskfs_inode_waiter *w = thread->waiter_free_list;

    if (w) {
        thread->waiter_free_list = w->next;
    } else {
        w = malloc(sizeof(*w));
    }
    return w;
} /* diskfs_waiter_alloc */


static inline void
diskfs_waiter_free(
    struct diskfs_thread       *thread,
    struct diskfs_inode_waiter *w)
{
    w->next                  = thread->waiter_free_list;
    thread->waiter_free_list = w;
} /* diskfs_waiter_free */


/* Caller must hold the inode's shard lock. */
static inline int
diskfs_inode_lock_compatible(
    struct diskfs_inode        *inode,
    enum diskfs_inode_lock_mode mode)
{
    if (mode != DISKFS_INODE_LOCK_READ) {     /* WRITE and WRITE_NOPIN are exclusive */
        return inode->writer == 0 && inode->readers == 0;
    }
    return inode->writer == 0;
} /* diskfs_inode_lock_compatible */


/* Two lock modes conflict unless both are READ.  WRITE and WRITE_NOPIN are both
 * exclusive, so any non-READ mode on either side conflicts. */
static inline int
diskfs_lock_modes_conflict(
    enum diskfs_inode_lock_mode a,
    enum diskfs_inode_lock_mode b)
{
    return a != DISKFS_INODE_LOCK_READ || b != DISKFS_INODE_LOCK_READ;
} /* diskfs_lock_modes_conflict */

/* --- WFG wait-descriptor seqlock (see struct diskfs_txn) -------------------- */

/* Publish/clear this txn's wait edge.  Single-writer by construction (park and
 * grant_pending are under the parking inode's shard lock; the finalize clear is
 * on this txn's own worker, ordered after the grant); the seqlock only protects
 * cross-thread readers from torn reads. */
static inline void
diskfs_wfg_set_wait(
    struct diskfs_txn          *txn,
    uint64_t                    inum,
    uint32_t                    gen,
    enum diskfs_inode_lock_mode mode,
    int                         grant_pending)
{
    uint32_t s = atomic_load_explicit(&txn->wfg_seq, memory_order_relaxed);

    atomic_store_explicit(&txn->wfg_seq, s + 1, memory_order_relaxed); /* odd */
    atomic_thread_fence(memory_order_release);
    txn->wfg_wait_inum     = inum;
    txn->wfg_wait_gen      = gen;
    txn->wfg_wait_mode     = (uint8_t) mode;
    txn->wfg_grant_pending = (uint8_t) grant_pending;
    atomic_store_explicit(&txn->wfg_seq, s + 2, memory_order_release); /* even */
} /* diskfs_wfg_set_wait */

static inline void
diskfs_wfg_clear_wait(struct diskfs_txn *txn)
{
    diskfs_wfg_set_wait(txn, 0, 0, DISKFS_INODE_LOCK_READ, 0);
} /* diskfs_wfg_clear_wait */

/* Lock-free read of a holder txn's wait descriptor.  Returns 1 and fills the
 * out params if the txn is parked on an inode with no pending grant (a live wait
 * edge); returns 0 if it is not waiting or is being granted (a leaf). */
static inline int
diskfs_wfg_read_wait(
    struct diskfs_txn           *txn,
    uint64_t                    *r_inum,
    uint32_t                    *r_gen,
    enum diskfs_inode_lock_mode *r_mode)
{
    for ( ;;) {
        uint32_t s1 = atomic_load_explicit(&txn->wfg_seq, memory_order_acquire);

        if (s1 & 1) {
            continue;                           /* writer mid-update */
        }

        uint64_t inum = txn->wfg_wait_inum;
        uint32_t gen  = txn->wfg_wait_gen;
        uint8_t  mode = txn->wfg_wait_mode;
        uint8_t  gp   = txn->wfg_grant_pending;

        atomic_thread_fence(memory_order_acquire);
        if (atomic_load_explicit(&txn->wfg_seq, memory_order_relaxed) != s1) {
            continue;                           /* torn; retry */
        }

        if (inum == 0 || gp) {
            return 0;                           /* leaf: not waiting / being granted */
        }
        *r_inum = inum;
        *r_gen  = gen;
        *r_mode = (enum diskfs_inode_lock_mode) mode;
        return 1;
    }
} /* diskfs_wfg_read_wait */

/* --- WFG per-inode holder list (pooled per shard, under the shard lock) ----- */

static inline struct diskfs_inode_holder *
diskfs_holder_alloc(struct diskfs_inode_shard *shard)
{
    struct diskfs_inode_holder *h = shard->holder_free_list;

    if (h) {
        shard->holder_free_list = h->next;
    } else {
        h = malloc(sizeof(*h));
    }
    return h;
} /* diskfs_holder_alloc */

static inline void
diskfs_holder_free(
    struct diskfs_inode_shard  *shard,
    struct diskfs_inode_holder *h)
{
    h->next                 = shard->holder_free_list;
    shard->holder_free_list = h;
} /* diskfs_holder_free */

/* Caller holds the inode's shard lock.  Records `txn` as a holder of `inode`. */
static inline void
diskfs_inode_holder_add(
    struct diskfs_inode_shard  *shard,
    struct diskfs_inode        *inode,
    struct diskfs_txn          *txn,
    enum diskfs_inode_lock_mode mode)
{
    struct diskfs_inode_holder *h = diskfs_holder_alloc(shard);

    h->txn         = txn;
    h->mode        = mode;
    h->next        = inode->holders;
    inode->holders = h;
} /* diskfs_inode_holder_add */

/* Caller holds the inode's shard lock.  Removes one holder record for `txn`. */
static inline void
diskfs_inode_holder_remove(
    struct diskfs_inode_shard *shard,
    struct diskfs_inode       *inode,
    struct diskfs_txn         *txn)
{
    struct diskfs_inode_holder **pp = &inode->holders;

    while (*pp) {
        if ((*pp)->txn == txn) {
            struct diskfs_inode_holder *h = *pp;
            *pp = h->next;
            diskfs_holder_free(shard, h);
            return;
        }
        pp = &(*pp)->next;
    }
} /* diskfs_inode_holder_remove */

/* Caller must hold the inode's shard lock.  Grants the lock and records the
 * holder identity for WFG detection. */
static inline void
diskfs_inode_lock_grant(
    struct diskfs_inode_shard  *shard,
    struct diskfs_inode        *inode,
    enum diskfs_inode_lock_mode mode,
    struct diskfs_txn          *txn)
{
    diskfs_inode_holder_add(shard, inode, txn, mode);

    if (mode != DISKFS_INODE_LOCK_READ) {     /* WRITE and WRITE_NOPIN are exclusive */
        inode->writer = 1;
    } else {
        inode->readers++;
    }
} /* diskfs_inode_lock_grant */


/*
 * Hand a granted (or stale-failed) waiter to its owning worker so its
 * continuation runs back on the transaction's own thread.
 */
static void
diskfs_dispatch_grant(struct diskfs_inode_waiter *w)
{
    struct diskfs_thread *worker = w->txn->thread;

    w->dispatched_ns = diskfs_diag_now_ns();

    pthread_mutex_lock(&worker->grant_lock);
    w->next = NULL;
    if (worker->grant_tail) {
        worker->grant_tail->next = w;
    } else {
        worker->grant_head = w;
    }
    worker->grant_tail = w;
    __atomic_store_n(&worker->grant_pending, 1, __ATOMIC_RELEASE);
    pthread_mutex_unlock(&worker->grant_lock);

    evpl_ring_doorbell(&worker->grant_doorbell);
} /* diskfs_dispatch_grant */


/*
 * Drop one held inode lock and grant the lock to compatible FIFO waiters.
 * Safe to call from any thread (worker for read/abort, intent-log thread
 * for write commit); granted waiters are dispatched to their own workers.
 */
void
diskfs_inode_release_one(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    struct diskfs_inode        *inode,
    enum diskfs_inode_lock_mode mode)
{
    struct diskfs_inode_shard  *shard   = diskfs_inode_shard(thread->shared, inode->inum);
    struct diskfs_inode_waiter *granted = NULL;
    struct diskfs_inode_waiter *w;

    pthread_mutex_lock(&shard->lock);

    if (mode != DISKFS_INODE_LOCK_READ) {     /* WRITE and WRITE_NOPIN are exclusive */
        inode->writer = 0;
    } else {
        inode->readers--;
    }
    diskfs_inode_holder_remove(shard, inode, txn);

    while (inode->wait_head) {
        w = inode->wait_head;

        if (w->gen != inode->gen) {
            /* The inode this waiter referenced was freed/replaced.  Fail
             * it with ENOENT rather than handing back a stale inode. */
            inode->wait_head = w->next;
            if (!inode->wait_head) {
                inode->wait_tail = NULL;
            }
            inode->wait_count--;
            w->status = CHIMERA_VFS_ENOENT;
            w->next   = granted;
            granted   = w;
            continue;
        }

        if (!diskfs_inode_lock_compatible(inode, w->mode)) {
            break;
        }

        inode->wait_head = w->next;
        if (!inode->wait_head) {
            inode->wait_tail = NULL;
        }
        inode->wait_count--;
        /* Grant under the shard lock: the waiter becomes a holder *now* (so the
         * WFG detector sees it as a holder, never "neither parked nor holding"),
         * and we publish grant_pending so the detector prunes its now-dead wait
         * edge until its own worker finalizes (clears the descriptor). */
        diskfs_inode_lock_grant(shard, inode, w->mode, w->txn);
        diskfs_wfg_set_wait(w->txn, inode->inum, w->gen, w->mode, 1);
        w->status = CHIMERA_VFS_OK;
        w->next   = granted;
        granted   = w;

        if (w->mode != DISKFS_INODE_LOCK_READ) {
            break;     /* exclusive (WRITE/WRITE_NOPIN): stop granting */
        }
    }

    /* If nobody re-took the lock and the inode is now idle, it becomes a
     * recycle candidate.  (Recycle re-checks evictability, so it's fine that
     * its dinode may not be durable yet.) */
    if (!inode->on_lru && diskfs_inode_idle(inode)) {
        diskfs_inode_lru_push_tail(shard, inode);
    }

    pthread_mutex_unlock(&shard->lock);

    while (granted) {
        w       = granted;
        granted = w->next;
        diskfs_dispatch_grant(w);
    }
} /* diskfs_inode_release_one */


/* ===========================================================================
 * WFG (wait-for-graph) deadlock detection.
 *
 * When an acquire finds an inode lock incompatible, instead of wait-die (which
 * false-aborts on mere contention), we walk the wait-for graph to see whether
 * parking would close a genuine cycle.  Only a real cycle aborts, and the victim
 * is the highest-ts (youngest) *explicit* transaction in the cycle (autocommit
 * ops -- ts==0 -- are never aborted; they cannot retry, and every cycle is
 * guaranteed to contain an explicit txn since only explicit txns acquire out of
 * canonical order).  The graph is sharded: the walk takes one inode-shard lock
 * at a time (never nested), reads each holder's wait edge via its seqlock-
 * published descriptor, and recurses by inum only -- no global lock.
 * =========================================================================== */

/* Real wait chains are a handful of txns deep; these are generous safety caps.
 * Exceeding either is treated as a (conservative) cycle that aborts the
 * requester -- safe and retriable, never a missed deadlock. */
#define DISKFS_WFG_MAX_VISITED 256
#define DISKFS_WFG_MAX_DEPTH   64

struct diskfs_wfg_visited {
    uint64_t inum;
    enum diskfs_inode_lock_mode mode;
};

struct diskfs_wfg_ctx {
    struct diskfs_thread     *thread;
    struct diskfs_txn        *requester;     /* R: the txn trying to acquire */
    int                       nvisited;
    int                       overflow;      /* exceeded a bound -> abort R */
    /* victim = highest-ts explicit member found across all closing paths */
    uint64_t                  vic_ts;
    uint64_t                  vic_inum;       /* inode the victim is parked on */
    /* running max-ts explicit member on the current recursion path */
    uint64_t                  path_ts;
    uint64_t                  path_inum;
    struct diskfs_wfg_visited visited[DISKFS_WFG_MAX_VISITED];
};

/* Does `txn` hold `inum` in a mode conflicting with `mode`?  Reads the txn's own
 * slot set; only ever called on the requester from its own worker thread. */
static int
diskfs_txn_holds_conflicting(
    struct diskfs_txn          *txn,
    uint64_t                    inum,
    enum diskfs_inode_lock_mode mode)
{
    int i;

    for (i = 0; i < txn->num_inodes; i++) {
        if (txn->inodes[i].inode->inum == inum) {
            return diskfs_lock_modes_conflict(txn->inodes[i].mode, mode);
        }
    }
    return 0;
} /* diskfs_txn_holds_conflicting */

static int
diskfs_wfg_seen(
    struct diskfs_wfg_ctx      *ctx,
    uint64_t                    inum,
    enum diskfs_inode_lock_mode mode)
{
    int i;

    for (i = 0; i < ctx->nvisited; i++) {
        if (ctx->visited[i].inum == inum && ctx->visited[i].mode == mode) {
            return 1;
        }
    }
    if (ctx->nvisited < DISKFS_WFG_MAX_VISITED) {
        ctx->visited[ctx->nvisited].inum = inum;
        ctx->visited[ctx->nvisited].mode = mode;
        ctx->nvisited++;
    } else {
        ctx->overflow = 1;
    }
    return 0;
} /* diskfs_wfg_seen */

/* One wait edge snapshotted under a shard lock: a conflicting holder H of the
 * inode being expanded, plus where H is itself parked. */
struct diskfs_wfg_edge {
    uint64_t ts;                           /* H's core.ts (0 = autocommit) */
    uint64_t w_inum;                       /* inode H is parked on */
    uint32_t w_gen;
    enum diskfs_inode_lock_mode w_mode;    /* mode H wants there */
};

/* Consider a member {ts, parked_inum} (a txn on the closing cycle) for victim. */
static inline void
diskfs_wfg_consider_victim(
    struct diskfs_wfg_ctx *ctx,
    uint64_t               ts,
    uint64_t               parked_inum)
{
    if (ts > ctx->vic_ts) {       /* explicit (ts>0) and higher than current */
        ctx->vic_ts   = ts;
        ctx->vic_inum = parked_inum;
    }
} /* diskfs_wfg_consider_victim */

/* Expand inode (inum,gen) wanted in `mode`: returns 1 if a cycle that closes on
 * the requester R is reachable.  Holds no shard lock across recursion. */
static int
diskfs_wfg_dfs(
    struct diskfs_wfg_ctx      *ctx,
    uint64_t                    inum,
    uint32_t                    gen,
    enum diskfs_inode_lock_mode mode,
    int                         depth)
{
    struct diskfs_thread       *thread = ctx->thread;
    struct diskfs_inode_shard  *shard;
    struct diskfs_inode        *inode;
    struct diskfs_inode_holder *h;
    struct diskfs_wfg_edge     *edges;
    int                         nedges = 0, cap, found = 0, i;

    if (depth > DISKFS_WFG_MAX_DEPTH) {
        ctx->overflow = 1;
        return 1;                 /* conservative: treat as a cycle, abort R */
    }
    if (diskfs_wfg_seen(ctx, inum, mode)) {
        return 0;
    }
    if (ctx->overflow) {
        return 1;
    }

    shard = diskfs_inode_shard(thread->shared, inum);
    pthread_mutex_lock(&shard->lock);
    rb_tree_query_exact(&shard->inodes, inum, inum, inode);
    if (!inode || inode->gen != gen) {
        pthread_mutex_unlock(&shard->lock);
        return 0;                 /* recycled / stale -> dead edge */
    }

    cap   = inode->readers + (inode->writer ? 1 : 0) + 1;
    edges = malloc(cap * sizeof(*edges));

    for (h = inode->holders; h; h = h->next) {
        struct diskfs_wfg_edge     *e;
        uint64_t                    w_inum;
        uint32_t                    w_gen;
        enum diskfs_inode_lock_mode w_mode;

        if (!diskfs_lock_modes_conflict(h->mode, mode)) {
            continue;
        }
        if (h->txn == ctx->requester) {
            continue;             /* the requester itself (deeper levels) */
        }
        if (!h->txn) {
            continue;             /* anonymous loader hold: not a txn -> leaf */
        }
        if (!diskfs_wfg_read_wait(h->txn, &w_inum, &w_gen, &w_mode)) {
            continue;             /* H not waiting / being granted -> leaf */
        }
        if (nedges < cap) {
            e         = &edges[nedges++];
            e->ts     = h->txn->core.ts;
            e->w_inum = w_inum;
            e->w_gen  = w_gen;
            e->w_mode = w_mode;
        }
    }
    pthread_mutex_unlock(&shard->lock);

    for (i = 0; i < nedges; i++) {
        struct diskfs_wfg_edge *e = &edges[i];

        if (diskfs_txn_holds_conflicting(ctx->requester, e->w_inum, e->w_mode)) {
            /* H waits on an inode R holds -> cycle closes.  Members are R, the
             * ancestors on the current path, and H itself. */
            found = 1;
            diskfs_wfg_consider_victim(ctx, ctx->path_ts, ctx->path_inum);
            diskfs_wfg_consider_victim(ctx, e->ts, e->w_inum);
            diskfs_wfg_consider_victim(ctx, ctx->requester->core.ts, 0);
            continue;
        }

        /* Recurse, tracking H on the path for victim selection. */
        uint64_t saved_ts   = ctx->path_ts;
        uint64_t saved_inum = ctx->path_inum;
        if (e->ts > ctx->path_ts) {
            ctx->path_ts   = e->ts;
            ctx->path_inum = e->w_inum;
        }
        if (diskfs_wfg_dfs(ctx, e->w_inum, e->w_gen, e->w_mode, depth + 1)) {
            found = 1;
        }
        ctx->path_ts   = saved_ts;
        ctx->path_inum = saved_inum;
    }

    free(edges);
    return found;
} /* diskfs_wfg_dfs */

/* Detect whether requester R parking on (inum,gen) in `mode` would close a
 * cycle.  Returns 1 and fills *vic_ts / *vic_inum (the victim's ts and the inode
 * it is parked on; *vic_inum==0 means the victim is R itself). */
static int
diskfs_wfg_detect(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    uint64_t                    inum,
    uint32_t                    gen,
    enum diskfs_inode_lock_mode mode,
    uint64_t                   *vic_ts,
    uint64_t                   *vic_inum)
{
    struct diskfs_wfg_ctx ctx;
    int                   cycle;

    ctx.thread    = thread;
    ctx.requester = txn;
    ctx.nvisited  = 0;
    ctx.overflow  = 0;
    ctx.vic_ts    = 0;
    ctx.vic_inum  = 0;
    ctx.path_ts   = 0;
    ctx.path_inum = 0;

    cycle = diskfs_wfg_dfs(&ctx, inum, gen, mode, 0);

    if (cycle) {
        if (ctx.overflow || ctx.vic_ts == 0) {
            /* No explicit victim identified (shouldn't happen for a real cycle)
             * or a bound was exceeded -> abort the requester, which is safe and
             * retriable when R is explicit. */
            *vic_ts   = txn->core.ts;
            *vic_inum = 0;
        } else if (ctx.vic_ts == txn->core.ts) {
            *vic_ts   = txn->core.ts;
            *vic_inum = 0;        /* victim is R */
        } else {
            *vic_ts   = ctx.vic_ts;
            *vic_inum = ctx.vic_inum;
        }
    }

    return cycle;
} /* diskfs_wfg_detect */

/* Abort a parked victim transaction: remove its waiter from the inode it is
 * parked on, clear its wait edge, and dispatch its continuation with
 * ETXN_CONFLICT.  Idempotent (a no-op if the waiter is already gone). */
static void
diskfs_wfg_abort_victim(
    struct diskfs_thread *thread,
    uint64_t              vic_ts,
    uint64_t              vic_inum)
{
    struct diskfs_inode_shard  *shard = diskfs_inode_shard(thread->shared, vic_inum);
    struct diskfs_inode        *inode;
    struct diskfs_inode_waiter *w, *prev = NULL, *victim = NULL;

    pthread_mutex_lock(&shard->lock);
    rb_tree_query_exact(&shard->inodes, vic_inum, inum, inode);
    if (inode) {
        for (w = inode->wait_head; w; prev = w, w = w->next) {
            if (w->txn->core.ts == vic_ts) {
                if (prev) {
                    prev->next = w->next;
                } else {
                    inode->wait_head = w->next;
                }
                if (inode->wait_tail == w) {
                    inode->wait_tail = prev;
                }
                inode->wait_count--;
                victim = w;
                break;
            }
        }
        if (victim) {
            diskfs_wfg_clear_wait(victim->txn);   /* edge dead before we release */
        }
    }
    pthread_mutex_unlock(&shard->lock);

    if (victim) {
        victim->status = CHIMERA_VFS_ETXN_CONFLICT;
        diskfs_dispatch_grant(victim);
    }
} /* diskfs_wfg_abort_victim */

/*
 * Contended acquire: the inode lock was incompatible.  Run deadlock detection;
 * on a cycle abort the victim (the requester inline, or a parked txn remotely)
 * and retry; otherwise park a waiter.  Runs on the requester's own worker.
 */
static void
diskfs_inode_acquire_contended(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    uint64_t                    inum,
    uint32_t                    gen,
    enum diskfs_inode_lock_mode mode,
    diskfs_inode_cb_t           cb,
    void                       *private_data)
{
    struct diskfs_inode_shard  *shard = diskfs_inode_shard(thread->shared, inum);
    struct diskfs_inode        *inode;
    struct diskfs_inode_waiter *w;

    for ( ;;) {
        uint64_t vic_ts = 0, vic_inum = 0;
        int      cycle = diskfs_wfg_detect(thread, txn, inum, gen, mode,
                                           &vic_ts, &vic_inum);

        if (cycle) {
            if (vic_inum == 0) {
                /* Victim is the requester. */
                cb(NULL, CHIMERA_VFS_ETXN_CONFLICT, private_data);
                return;
            }
            /* Abort a parked victim; its wait edge is cleared synchronously, so
             * the retry below sees no cycle and parks (the victim's locks free
             * asynchronously as its op unwinds, then grant us). */
            diskfs_wfg_abort_victim(thread, vic_ts, vic_inum);
        }

        pthread_mutex_lock(&shard->lock);
        rb_tree_query_exact(&shard->inodes, inum, inum, inode);
        if (!inode || inode->gen != gen) {
            pthread_mutex_unlock(&shard->lock);
            cb(NULL, CHIMERA_VFS_ENOENT, private_data);
            return;
        }

        if (diskfs_inode_lock_compatible(inode, mode)) {
            diskfs_inode_lock_grant(shard, inode, mode, txn);
            diskfs_inode_lru_unlink(shard, inode);
            pthread_mutex_unlock(&shard->lock);
            diskfs_txn_add_slot(txn, inode, mode);
            if (mode == DISKFS_INODE_LOCK_WRITE) {
                diskfs_inode_finish_write_pin(thread, txn, inode, cb, private_data,
                                              0 /* existing inode */);
            } else {
                cb(inode, CHIMERA_VFS_OK, private_data);
            }
            return;
        }

        if (cycle) {
            /* We aborted a victim but the lock is still held (async unwind):
             * re-detect (the victim's edge is gone, so this converges to park). */
            pthread_mutex_unlock(&shard->lock);
            continue;
        }

        /* No cycle: park a waiter and publish our wait edge. */
        w               = diskfs_waiter_alloc(thread);
        w->txn          = txn;
        w->mode         = mode;
        w->gen          = gen;
        w->cb           = cb;
        w->private_data = private_data;
        w->inode        = inode;
        w->queued_ns    = diskfs_diag_now_ns();
        w->queue_depth  = inode->wait_count + 1;
        w->status       = CHIMERA_VFS_OK;
        w->next         = NULL;

        if (inode->wait_tail) {
            inode->wait_tail->next = w;
        } else {
            inode->wait_head = w;
        }
        inode->wait_tail = w;
        inode->wait_count++;

        diskfs_wfg_set_wait(txn, inum, gen, mode, 0);
        pthread_mutex_unlock(&shard->lock);
        return;
    }
} /* diskfs_inode_acquire_contended */

/*
 * Grant (or enqueue a waiter for) `inode` with the shard lock already held, and
 * release the lock before returning.  Shared by diskfs_inode_acquire (after the
 * rb-tree lookup) and diskfs_inode_acquire_pinned (lookup skipped because an
 * open handle pins the inode).  On a compatible WRITE grant this pins the home
 * block, which may async-load it and defer the callback.  `gen` is recorded on
 * a parked waiter so a later grant can detect a stale generation.
 */
void
diskfs_inode_grant_locked(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    struct diskfs_inode_shard  *shard,
    struct diskfs_inode        *inode,
    uint32_t                    gen,
    enum diskfs_inode_lock_mode mode,
    diskfs_inode_cb_t           cb,
    void                       *private_data)
{
    if (diskfs_inode_lock_compatible(inode, mode)) {
        diskfs_metric_inode_cache(thread, DISKFS_METRIC_INODE_CACHE_HIT);
        diskfs_inode_lock_grant(shard, inode, mode, txn);
        diskfs_inode_lru_unlink(shard, inode);     /* busy now, not a candidate */
        pthread_mutex_unlock(&shard->lock);
        diskfs_txn_add_slot(txn, inode, mode);
        /* The grant no longer eager-faults the home block: a b+tree modify links
        * its inode's root in the descent, attr-only modifiers fault it at
        * commit-prep (diskfs_txn_commit_finish), and read-only/volatile holders
        * (getattr, close) never touch it -- so reporting the grant is enough. */
        cb(inode, CHIMERA_VFS_OK, private_data);
        return;
    }

    /* Incompatible.  Drop the shard lock and resolve via the WFG contended path
     * (deadlock detection, then either abort a victim, park, or -- if the abort
     * frees the lock -- retry). */
    diskfs_metric_inode_cache(thread, DISKFS_METRIC_INODE_CACHE_WAIT);
    pthread_mutex_unlock(&shard->lock);
    diskfs_inode_acquire_contended(thread, txn, inode->inum, gen, mode, cb,
                                   private_data);
} /* diskfs_inode_grant_locked */


void
diskfs_inode_acquire(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    uint64_t                    inum,
    uint32_t                    gen,
    enum diskfs_inode_lock_mode mode,
    diskfs_inode_cb_t           cb,
    void                       *private_data)
{
    struct diskfs_inode_shard *shard;
    struct diskfs_inode       *inode;
    int                        i;

    for (i = 0; i < txn->num_inodes; i++) {
        inode = txn->inodes[i].inode;
        if (inode->inum == inum) {
            if (unlikely(inode->gen != gen)) {
                cb(NULL, CHIMERA_VFS_ENOENT, private_data);
            } else {
                cb(inode, CHIMERA_VFS_OK, private_data);
            }
            return;
        }
    }

    shard = diskfs_inode_shard(thread->shared, inum);
    pthread_mutex_lock(&shard->lock);

    rb_tree_query_exact(&shard->inodes, inum, inum, inode);

    if (unlikely(inode && inode->gen != gen)) {
        /* Cached under a different generation: the handle is stale. */
        diskfs_metric_inode_cache(thread, DISKFS_METRIC_INODE_CACHE_STALE);
        pthread_mutex_unlock(&shard->lock);
        cb(NULL, CHIMERA_VFS_ENOENT, private_data);
        return;
    }

    if (unlikely(!inode)) {
        /* Not resident: either evicted (its dinode is durably home -- eviction
         * only drops CLEAN inodes) or genuinely absent.  Fault it in from disk
         * whenever the inum is within allocated space; the on-disk dinode read
         * validates inum/gen/nlink and yields ENOENT if it isn't really there.
         * (This must not gate on `mounted` -- a freshly-formatted FS evicts
         * too, so a miss is not necessarily ENOENT.) */
        diskfs_metric_inode_cache(thread, DISKFS_METRIC_INODE_CACHE_MISS);
        pthread_mutex_unlock(&shard->lock);
        if (sm_inum_valid(thread->shared->space_map, inum)) {
            diskfs_inode_load(thread, txn, inum, gen, mode, cb, private_data);
        } else {
            cb(NULL, CHIMERA_VFS_ENOENT, private_data);
        }
        return;
    }

    diskfs_inode_grant_locked(thread, txn, shard, inode, gen, mode, cb, private_data);
} /* diskfs_inode_acquire */


/*
 * Acquire the inode lock on an inode already pinned by an open handle (its
 * refcnt was bumped in diskfs_open_fh_inode_cb, so it is resident and will not
 * be freed; gen bumps only on free).  This skips the fh->inum decode and the
 * inode-cache rb-tree lookup -- the hot per-I/O cost on a warm handle -- but
 * still takes the shard lock to serialize the lock-state grant against the
 * concurrent release/completion path.
 */
void
diskfs_inode_acquire_pinned(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    struct diskfs_inode        *inode,
    enum diskfs_inode_lock_mode mode,
    diskfs_inode_cb_t           cb,
    void                       *private_data)
{
    struct diskfs_inode_shard *shard;
    int                        i;

    /* Already locked in this txn: reuse the held grant (matches the txn-slot
     * fast path in diskfs_inode_acquire). */
    for (i = 0; i < txn->num_inodes; i++) {
        if (txn->inodes[i].inode == inode) {
            cb(inode, CHIMERA_VFS_OK, private_data);
            return;
        }
    }

    shard = diskfs_inode_shard(thread->shared, inode->inum);
    pthread_mutex_lock(&shard->lock);
    diskfs_inode_grant_locked(thread, txn, shard, inode, inode->gen, mode, cb,
                              private_data);
} /* diskfs_inode_acquire_pinned */


struct diskfs_inode *
diskfs_inode_load_sync(
    struct diskfs_thread   *thread,
    struct diskfs_mount_io *io,
    uint64_t                inum,
    uint32_t                gen,
    int                     allow_orphan)
{
    struct diskfs_shared      *shared = thread->shared;
    struct diskfs_inode_shard *shard  = diskfs_inode_shard(shared, inum);
    struct diskfs_inode       *inode;
    struct diskfs_dinode      *di;
    uint8_t                    buf[DISKFS_BLOCK_SIZE];
    uint32_t                   dev;
    uint64_t                   off;
    int                        created = 0;

    if (!sm_inum_valid(shared->space_map, inum)) {
        return NULL;
    }

    /* Already resident (e.g. a freshly-bootstrapped root/orphan inode, or a
     * prior fault): return it without touching disk. */
    pthread_mutex_lock(&shard->lock);
    rb_tree_query_exact(&shard->inodes, inum, inum, inode);
    if (inode) {
        int ok = (inode->gen == gen && (inode->nlink != 0 || allow_orphan));

        pthread_mutex_unlock(&shard->lock);
        return ok ? inode : NULL;
    }
    pthread_mutex_unlock(&shard->lock);

    /* Not cached: read the dinode block from disk through the mount-time pump
     * (VFIO-safe).  Safe to read the on-disk image directly -- an inode whose
     * struct is not cached has no in-flight dirty block. */
    off = sm_inum_to_device_offset(shared->space_map, inum, &dev);
    if (diskfs_mount_io_read(io, dev, buf, sizeof(buf), off) != 0) {
        return NULL;
    }

    di = (struct diskfs_dinode *) buf;
    if (di->inum != inum || di->gen != gen ||
        (di->nlink == 0 && !allow_orphan)) {
        return NULL;
    }

    pthread_mutex_lock(&shard->lock);
    rb_tree_query_exact(&shard->inodes, inum, inum, inode);
    if (!inode) {
        created = 1;
        diskfs_inode_cache_recycle_locked(shared, shard);
        inode                 = calloc(1, sizeof(*inode));
        inode->inum           = inum;
        inode->refcnt         = 1;
        inode->gen            = di->gen;
        inode->mode           = di->mode;
        inode->nlink          = di->nlink;
        inode->uid            = di->uid;
        inode->gid            = di->gid;
        inode->rdev           = di->rdev;
        inode->size           = di->size;
        inode->space_used     = di->space_used;
        inode->atime_sec      = di->atime_sec;
        inode->atime_nsec     = di->atime_nsec;
        inode->mtime_sec      = di->mtime_sec;
        inode->mtime_nsec     = di->mtime_nsec;
        inode->ctime_sec      = di->ctime_sec;
        inode->ctime_nsec     = di->ctime_nsec;
        inode->btime_sec      = di->btime_sec;
        inode->btime_nsec     = di->btime_nsec;
        inode->dos_attributes = di->dos_attributes;
        inode->change         = di->change;
        inode->ea_size        = di->ea_size;
        inode->ea_count       = di->ea_count;
        inode->parent_inum    = di->parent_inum;
        inode->parent_gen     = di->parent_gen;
        rb_tree_insert(&shard->inodes, inum, inode);
        shard->ninodes++;
        diskfs_metric_inode_cache(thread, DISKFS_METRIC_INODE_CACHE_LOAD);
    }
    pthread_mutex_unlock(&shard->lock);

    /* Mirror the singleton ACL/pNFS records onto the freshly-constructed
     * inode (the runtime fault path does the same through the async b+tree
     * ops), walking the on-disk tree through the pump. */
    if (created) {
        uint8_t rec[DISKFS_ACL_REC_MAX];
        int     len;

        len = diskfs_bt_lookup_pump(shared, io, buf, &diskfs_acl_key,
                                    rec, sizeof(rec));
        if (len >= 0) {
            diskfs_acl_serial_install(inode, rec, len);
        }

        len = diskfs_bt_lookup_pump(shared, io, buf, &diskfs_pnfs_key,
                                    rec, CHIMERA_VFS_PNFS_LAYOUT_MAX);
        if (len >= 0) {
            inode->pnfs_blob = malloc(len);
            memcpy(inode->pnfs_blob, rec, len);
            inode->pnfs_blob_len = (uint32_t) len;
        }
    }

    /* Seed the inode's home block into the block cache from the disk image we
     * just read.  Claim is_new (no read-back): we overwrite the whole block. */
    {
        struct diskfs_block *blk = diskfs_block_claim(thread, dev, off, 1);

        memcpy(blk->iov.data, buf, DISKFS_BLOCK_SIZE);
        diskfs_block_unpin(thread, blk, DISKFS_BLOCK_CLEAN);
    }
    return inode;
} /* diskfs_inode_load_sync */


static void
diskfs_gen_extend_complete(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct diskfs_gen_extend   *ge     = private_data;
    struct diskfs_thread       *thread = ge->thread;
    struct diskfs_shared       *shared = thread->shared;
    struct diskfs_block_waiter *waiters, *w;

    chimera_diskfs_abort_if(status != 0,
                            "generation-floor superblock write failed: %d",
                            status);

    __atomic_store_n(&shared->gen_floor, ge->new_floor, __ATOMIC_RELEASE);
    __atomic_store_n(&shared->gen_extend_inflight, 0, __ATOMIC_RELEASE);

    evpl_iovec_release(evpl, &ge->iov);
    free(ge);

    /* Resume any allocations that caught up to the old floor. */
    pthread_mutex_lock(&shared->gen_lock);
    waiters          = shared->gen_wait;
    shared->gen_wait = NULL;
    pthread_mutex_unlock(&shared->gen_lock);

    while (waiters) {
        w       = waiters;
        waiters = w->next;
        diskfs_block_waiter_dispatch(thread, w);
    }
} /* diskfs_gen_extend_complete */


/* Persist a new generation floor (current counter + reserve).  The write
* always carries FUA regardless of unsafe_async: re-issuing a generation
* after a crash would let stale file handles resolve to reused inodes. */
void
diskfs_gen_extend(struct diskfs_thread *thread)
{
    struct diskfs_shared     *shared = thread->shared;
    struct diskfs_gen_extend *ge;
    int                       expect = 0;

    if (!__atomic_compare_exchange_n(&shared->gen_extend_inflight, &expect, 1,
                                     0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
        return;     /* one extension in flight at a time */
    }

    ge            = malloc(sizeof(*ge));
    ge->thread    = thread;
    ge->new_floor = __atomic_load_n(&shared->gen_next, __ATOMIC_RELAXED) +
        DISKFS_GEN_RESERVE;

    evpl_iovec_alloc(thread->evpl, SM_SUPERBLOCK_SIZE, SM_SUPERBLOCK_SIZE, 1,
                     0, &ge->iov);
    space_map_fill_superblock(shared->space_map, ge->iov.data, shared->fsid,
                              0 /* dirty */, shared->root_inum,
                              shared->root_gen, 0, ge->new_floor);

    evpl_block_write(thread->evpl, thread->queue[0], &ge->iov, 1,
                     SM_SUPERBLOCK_OFFSET, 1 /* sync */,
                     diskfs_gen_extend_complete, ge);
} /* diskfs_gen_extend */


/*
 * Draw the next inode generation.  Returns 0 with *r_gen set, or SM_AGAIN
 * after parking resume(thread, arg) for the (effectively never taken) case
 * where the counter caught up to the durable floor before an extension
 * landed.  The extension is kicked at half-reserve so allocations normally
 * never block on it.
 */
int
diskfs_gen_alloc(
    struct diskfs_thread *thread,
    uint32_t *r_gen,
    void ( *resume )(struct diskfs_thread *, void *),
    void *arg)
{
    struct diskfs_shared *shared = thread->shared;
    uint64_t              g      = __atomic_fetch_add(&shared->gen_next, 1,
                                                      __ATOMIC_RELAXED);
    uint64_t              floor;

    chimera_diskfs_abort_if(g >= UINT32_MAX,
                            "inode generation space exhausted");

    floor = __atomic_load_n(&shared->gen_floor, __ATOMIC_ACQUIRE);

    if (g + DISKFS_GEN_RESERVE / 2 >= floor) {
        diskfs_gen_extend(thread);
        if (g >= floor) {
            /* The slot is past the durable bound: park until the extension
             * lands (the drawn value is simply wasted; the retry draws a
             * fresh one). */
            struct diskfs_block_waiter *w = diskfs_block_waiter_alloc(thread);

            w->thread = thread;
            w->resume = resume;
            w->arg    = arg;

            pthread_mutex_lock(&shared->gen_lock);
            /* The extension may have landed while we took the lock. */
            if (__atomic_load_n(&shared->gen_floor, __ATOMIC_ACQUIRE) > g) {
                pthread_mutex_unlock(&shared->gen_lock);
                diskfs_block_waiter_free(thread, w);
                *r_gen = (uint32_t) g;
                return 0;
            }
            w->next          = shared->gen_wait;
            shared->gen_wait = w;
            pthread_mutex_unlock(&shared->gen_lock);
            return SM_AGAIN;
        }
    }

    *r_gen = (uint32_t) g;
    return 0;
} /* diskfs_gen_alloc */


/*
 * Runs on a worker thread when another thread has granted it one or more
 * inode locks it was waiting on.  The lock state was already updated by
 * the releasing thread; here we record the slot (on this, the txn's own
 * thread) and resume the parked continuation.
 */
static int
diskfs_grant_drain(struct diskfs_thread *thread)
{
    struct diskfs_inode_waiter *list, *w;
    int                         drained = 0;

    if (!__atomic_load_n(&thread->grant_pending, __ATOMIC_ACQUIRE)) {
        return 0;
    }

    pthread_mutex_lock(&thread->grant_lock);
    list               = thread->grant_head;
    thread->grant_head = NULL;
    thread->grant_tail = NULL;
    __atomic_store_n(&thread->grant_pending, 0, __ATOMIC_RELEASE);
    pthread_mutex_unlock(&thread->grant_lock);

    while (list) {
        diskfs_inode_cb_t    cb;
        void                *private_data;
        struct diskfs_inode *inode;
        int                  status;

        w    = list;
        list = w->next;
        drained++;

        cb           = w->cb;
        private_data = w->private_data;
        inode        = w->inode;
        status       = w->status;

        /* The txn's continuation runs here on its own worker: it is no longer
         * parked, so finalize its WFG wait edge (the grant path set it to
         * grant_pending; a stale/conflict grant leaves it set).  Clearing here
         * also closes the grant window once we install the holder slot. */
        diskfs_wfg_clear_wait(w->txn);

        if (status == CHIMERA_VFS_OK) {
            uint64_t wait_ns = diskfs_diag_now_ns() - w->queued_ns;

            if (wait_ns > 1000000000ULL) {
                uint64_t grant_ns = w->dispatched_ns ?
                    w->dispatched_ns - w->queued_ns : 0;
                uint64_t callback_ns = w->dispatched_ns ?
                    wait_ns - grant_ns : 0;

                chimera_diskfs_error(
                    "inode waiter callback after %llu sec inum=%llu mode=%d queued_depth=%u grant_wait_ms=%llu callback_wait_ms=%llu",
                    (unsigned long long) (wait_ns / 1000000000ULL),
                    inode ? (unsigned long long) inode->inum : 0ULL,
                    w->mode,
                    w->queue_depth,
                    (unsigned long long) (grant_ns / 1000000ULL),
                    (unsigned long long) (callback_ns / 1000000ULL));
            }

            struct diskfs_txn          *wtxn  = w->txn;
            enum diskfs_inode_lock_mode wmode = w->mode;

            diskfs_txn_add_slot(wtxn, inode, wmode);
            diskfs_waiter_free(thread, w);

            /* No eager home-block fault (see diskfs_inode_grant_locked): the
             * descent links a modify's root and commit-prep faults attr-only
             * inodes, so just report the grant. */
            cb(inode, CHIMERA_VFS_OK, private_data);
        } else {
            diskfs_waiter_free(thread, w);
            cb(NULL, status, private_data);
        }
    }
    return drained;
} /* diskfs_grant_drain */


void
diskfs_grant_doorbell_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct diskfs_thread *thread = container_of(doorbell,
                                                struct diskfs_thread,
                                                grant_doorbell);

    (void) evpl;
    diskfs_grant_drain(thread);
} /* diskfs_grant_doorbell_cb */


void
diskfs_grant_poll(
    struct evpl *evpl,
    void        *private_data)
{
    if (diskfs_grant_drain(private_data)) {
        evpl_activity(evpl);
    }
} /* diskfs_grant_poll */


/* ================================================================== */
/* Deferred-mtime flusher: coalesce per-write inode timestamp updates  */
/* ================================================================== */

/* This worker owns inode-cache shards where (shard % num_active_threads) ==
 * thread_id; flushing each shard on a single worker keeps two flushers off the
 * same inode without a cross-thread lock. */
static inline int
diskfs_mtime_owns_shard(
    const struct diskfs_thread *thread,
    uint32_t                    shard)
{
    int n = thread->shared->num_active_threads;

    return (int) (shard % (uint32_t) (n > 0 ? n : 1)) == thread->thread_id;
} /* diskfs_mtime_owns_shard */


/*
 * Claim the oldest deferred-mtime inode from this worker's owned shards that
 * has been dirty at least the coalescing window (or any, when flushing for
 * unmount).  Claiming unlinks it and clears the flag but KEEPS the dirty-pin
 * (now owned by the in-flight flush); a concurrent re-dirty re-queues with a
 * fresh pin.  Round-robins the shard cursor so no shard starves.
 */
static struct diskfs_inode *
diskfs_mtime_flush_pick(struct diskfs_thread *thread)
{
    struct diskfs_shared *shared    = thread->shared;
    uint64_t              period_ns = shared->mtime_defer_us * 1000;
    struct timespec       ts;
    uint64_t              now_ns;
    uint32_t              n;

    if ((uint32_t) thread->thread_id >= DISKFS_INODE_CACHE_SHARDS) {
        return NULL;     /* more workers than shards: this one owns none */
    }

    clock_gettime(CLOCK_REALTIME, &ts);
    now_ns = (uint64_t) ts.tv_sec * 1000000000ULL + ts.tv_nsec;

    for (n = 0; n < DISKFS_INODE_CACHE_SHARDS; n++) {
        uint32_t                   s = thread->mtime_scan_shard;
        struct diskfs_inode_shard *shard;
        struct diskfs_inode       *inode = NULL;

        /* Advance the cursor to this worker's next owned shard. */
        do {
            thread->mtime_scan_shard = (thread->mtime_scan_shard + 1) &
                DISKFS_INODE_CACHE_MASK;
        } while (!diskfs_mtime_owns_shard(thread, thread->mtime_scan_shard));

        if (!diskfs_mtime_owns_shard(thread, s)) {
            continue;
        }

        shard = &shared->inode_cache->shards[s];
        pthread_mutex_lock(&shard->lock);
        if (shard->mdirty_head &&
            (thread->mtime_flush_all ||
             now_ns - shard->mdirty_head->mtime_dirty_since >= period_ns)) {
            inode = shard->mdirty_head;
            diskfs_inode_mtime_unlink_locked(shard, inode);   /* keeps the pin */
        }
        pthread_mutex_unlock(&shard->lock);

        if (inode) {
            return inode;
        }
    }
    return NULL;
} /* diskfs_mtime_flush_pick */


/* Any deferred-mtime work left in this worker's owned shards? (unmount drain) */
int
diskfs_mtime_any_dirty(struct diskfs_thread *thread)
{
    struct diskfs_shared *shared = thread->shared;
    uint32_t              s;

    for (s = 0; s < DISKFS_INODE_CACHE_SHARDS; s++) {
        if (!diskfs_mtime_owns_shard(thread, s)) {
            continue;
        }
        if (shared->inode_cache->shards[s].mdirty_head) {
            return 1;
        }
    }
    return 0;
} /* diskfs_mtime_any_dirty */


/* Drop the dirty-pin on a flushed inode, re-LRUing it if it became idle. */
static void
diskfs_mtime_flush_drop_pin(
    struct diskfs_thread *thread,
    struct diskfs_inode  *inode)
{
    /* May be the last reference to an inode unlinked while its deferred
     * mtime was still queued: ref_drop hands it to the reclaim workers. */
    diskfs_inode_ref_drop(thread, inode);
} /* diskfs_mtime_flush_drop_pin */


static void
diskfs_mtime_flush_committed_cb(
    struct diskfs_txn *txn,
    int                status,
    void              *priv)
{
    struct diskfs_mtime_flush *f      = priv;
    struct diskfs_thread      *thread = f->thread;

    (void) txn;
    (void) status;

    diskfs_metric_mtime(thread, DISKFS_METRIC_MTIME_FLUSHED);
    diskfs_mtime_flush_drop_pin(thread, f->inode);
    free(f);
    thread->mtime_flushing = 0;
    diskfs_mtime_flush_kick(thread);     /* next eligible inode, if any */
} /* diskfs_mtime_flush_committed_cb */


static void
diskfs_mtime_flush_pinned_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *priv)
{
    struct diskfs_mtime_flush *f = priv;

    (void) inode;
    (void) status;
    /* Home block resident (faulted async if it had been evicted); commit
     * serializes the coalesced mtime/ctime into it and logs it. */
    diskfs_txn_commit(f->txn, diskfs_mtime_flush_committed_cb, f);
} /* diskfs_mtime_flush_pinned_cb */


static void
diskfs_mtime_flush_acquired_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *priv)
{
    struct diskfs_mtime_flush *f      = priv;
    struct diskfs_thread      *thread = f->thread;

    if (status != CHIMERA_VFS_OK) {
        /* Should not happen for a pinned inode; release the pin and move on. */
        diskfs_txn_abort(f->txn);
        diskfs_mtime_flush_drop_pin(thread, inode);
        free(f);
        thread->mtime_flushing = 0;
        diskfs_mtime_flush_kick(thread);
        return;
    }

    /* Establish the home block (async-load if it was evicted) before commit, so
     * the commit serializes the coalesced mtime/ctime into it and logs it.
     * Inline when resident; suspends to diskfs_mtime_flush_pinned_cb on a cold
     * reload. */
    diskfs_inode_finish_write_pin(thread, f->txn, inode,
                                  diskfs_mtime_flush_pinned_cb, f, 0);
} /* diskfs_mtime_flush_acquired_cb */


void
diskfs_mtime_flush_kick(struct diskfs_thread *thread)
{
    struct diskfs_mtime_flush *f;
    struct diskfs_inode       *inode;

    if (thread->mtime_flushing) {
        return;     /* one flush txn at a time per worker */
    }

    inode = diskfs_mtime_flush_pick(thread);
    if (!inode) {
        return;     /* nothing ready; the timer re-kicks next tick */
    }

    thread->mtime_flushing = 1;
    f                      = malloc(sizeof(*f));
    f->thread              = thread;
    f->inode               = inode;
    f->txn                 = diskfs_txn_begin(thread, DISKFS_TXN_WRITE, NULL);
    diskfs_inode_acquire_pinned(thread, f->txn, inode, DISKFS_INODE_LOCK_WRITE,
                                diskfs_mtime_flush_acquired_cb, f);
} /* diskfs_mtime_flush_kick */


void
diskfs_mtime_flush_timer_cb(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct diskfs_thread *thread = container_of(timer, struct diskfs_thread, mtime_timer);

    (void) evpl;
    diskfs_mtime_flush_kick(thread);
} /* diskfs_mtime_flush_timer_cb */
