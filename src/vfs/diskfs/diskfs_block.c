// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Block cache: sharded cache of 4 KiB metadata blocks with async fetch
 * (park/resume on miss), pin/unpin, LOGGED->COW forking, eviction, and the
 * cross-thread waiter dispatch (doorbell/deferral) machinery.
 */

#include <execinfo.h>
#include "diskfs_internal.h"

/*
 * Reclaimable-buffer reserve per shard.  A buffer-consuming async claim
 * (miss->recycle, CoW->reclaim) parks when reclaimable (nfree_buffers +
 * lru_count) would drop to/below this; the reserve is what the synchronous
 * (can't-park) recycle/CoW paths draw from, so it only needs to exceed peak
 * concurrent sync consumption (~one buffer per worker thread).  Kept small so
 * the gate parks only near true exhaustion -- a large reserve would park claims
 * (under the inode write lock) while plenty of buffers are still free, inflating
 * inode-lock hold time into a convoy.  The wake path (diskfs_block_buf_wake)
 * releases at most (reclaimable - reserve) waiters so woken claims actually make
 * progress instead of re-parking in a thundering herd. */
#define DISKFS_BLOCK_BUF_RESERVE 1024

/* Diag: a single transaction has grown pathologically large.  Dump the count
 * and the call path (once, at the threshold crossing) so we can see what is
 * bulk-adding blocks -- e.g. an unbounded AG-log condense folding a whole AG
 * log into one txn. */
static int g_giant_bt_budget = 120;     /* cap total backtraces so the log doesn't flood */

/* Diag: distinct (device_id, device_offset) among the txn's block entries, via
 * a temporary open-addressing hash set.  If unique << nblocks, the txn is
 * re-appending the same few blocks (the re-claim/re-journal duplication theory). */
static uint32_t
diskfs_txn_count_unique_blocks(struct diskfs_txn *txn)
{
    struct diskfs_txn_block *tb;
    uint32_t                 cap = 1u << 17;      /* 131072 slots */
    uint32_t                 mask = cap - 1, unique = 0;
    uint64_t                *keys = calloc(cap, sizeof(*keys));

    if (!keys) {
        return 0;
    }
    for (tb = txn->blocks; tb; tb = tb->next) {
        uint64_t k = ((uint64_t) tb->block->device_id << 48) | tb->block->device_offset;
        uint32_t h = (uint32_t) ((k * 0x9E3779B97F4A7C15ULL) >> 32) & mask;
        while (keys[h] && keys[h] != k) {
            h = (h + 1) & mask;
        }
        if (!keys[h]) {
            keys[h] = k;
            unique++;
        }
    }
    free(keys);
    return unique;
} /* diskfs_txn_count_unique_blocks */

void
diskfs_txn_trace_giant(struct diskfs_txn *txn)
{
    void    *frames[40];
    int      n, i;
    char   **syms;
    uint32_t unique;

    if (g_giant_bt_budget <= 0) {
        return;
    }
    g_giant_bt_budget--;

    unique = diskfs_txn_count_unique_blocks(txn);
    n      = backtrace(frames, 40);
    syms   = backtrace_symbols(frames, n);

    /* journal = AG-log journal-block claims; direct = btree-node/inode adds.
     * unique = distinct blocks; reserve_again = RESERVE-phase re-drives.
     * journal>>direct + unique<<nblocks + high reserve_again == the re-journal
     * duplication theory confirmed. */
    chimera_diskfs_error(
        "GIANT-TXN txn=%p type=%d num_inodes=%d nblocks=%u journal=%u direct=%u unique=%u reserve_again=%u -- backtrace:",
        (void *) txn, (int) txn->type, txn->num_inodes,
        txn->nblocks, txn->n_journal, txn->nblocks - txn->n_journal,
        unique, txn->n_reserve_again);
    for (i = 0; i < n; i++) {
        chimera_diskfs_error("  GIANT-TXN bt[%d] %s", i, syms ? syms[i] : "?");
    }
    free(syms);
} /* diskfs_txn_trace_giant */

static int g_dbg_btdone_budget = 80;     /* cap dropped-continuation backtraces */

void
diskfs_bt_done_trace(struct diskfs_bt_op *op)
{
    void  *frames[40];
    int    n, i;
    char **syms;

    if (g_dbg_btdone_budget <= 0) {
        return;
    }
    g_dbg_btdone_budget--;

    n    = backtrace(frames, 40);
    syms = backtrace_symbols(frames, n);

    /* A mutating op whose txn reserve-parked is completing via the done-path
     * (suspended==0) -- the async caller already left, so this op is dropped.
     * If the backtrace contains diskfs_bt_op_resume_cb, it completed on the
     * resume path with suspended still clear (the reserve-park race). */
    chimera_diskfs_error(
        "BT-DONE-DROP op=%p opcode=%d result=%d suspended=%d txn=%p dbg_stage=%d n_reserve_again=%u -- backtrace:",
        (void *) op, (int) op->opcode, op->result, op->suspended,
        (void *) op->txn,
        op->txn ? op->txn->dbg_stage : -1,
        op->txn ? op->txn->n_reserve_again : 0);
    for (i = 0; i < n; i++) {
        chimera_diskfs_error("  BT-DONE-DROP bt[%d] %s", i, syms ? syms[i] : "?");
    }
    free(syms);
} /* diskfs_bt_done_trace */

static int g_dbg_recyc_waiter_budget = 40;     /* cap recycle-with-waiter backtraces */
int        g_dbg_recyc_waiter_count  = 0;      /* total such events (atomic) */

/* Smoking-gun detector: a block being recycled (its slot reused for a new
 * (dev,offset)) still has parked waiters on its wait_head. Those waiters'
 * resume continuations are now lost forever -> the dirent-insert reserve hang.
 * Always counts; backtraces are budgeted so the log doesn't flood. */
static void
diskfs_block_recycle_waiter_trace(struct diskfs_block *blk)
{
    void                       *frames[40];
    int                         n, i, wc = 0;
    char                      **syms;
    struct diskfs_block_waiter *w;

    __atomic_fetch_add(&g_dbg_recyc_waiter_count, 1, __ATOMIC_RELAXED);
    if (g_dbg_recyc_waiter_budget <= 0) {
        return;
    }
    g_dbg_recyc_waiter_budget--;

    for (w = blk->wait_head; w; w = w->next) {
        wc++;
    }
    n    = backtrace(frames, 40);
    syms = backtrace_symbols(frames, n);
    chimera_diskfs_error(
        "RECYC-WAITER blk=%p state=%d dev=%u off=%lu pin=%d on_lru=%d waiters=%d -- recycling a block with parked waiters (lost wakeup!) -- backtrace:",
        (void *) blk, (int) blk->state, blk->device_id,
        (unsigned long) blk->device_offset,
        (int) blk->pin_count, blk->on_lru, wc);
    for (i = 0; i < n; i++) {
        chimera_diskfs_error("  RECYC-WAITER bt[%d] %s", i, syms ? syms[i] : "?");
    }
    free(syms);
} /* diskfs_block_recycle_waiter_trace */

/* Per-phase count of bt ops currently parked (indexed by enum diskfs_bt_phase,
 * 0..5). Read via gdb at a hang to localize which park site loses wakeups. */
int g_dbg_park[6] = { 0 };

/* Called at a park point (op is parking, alive): record the phase and bump its
 * count once. Cleared when the op resumes (diskfs_bt_op_park_clear). */
void
diskfs_bt_op_park_account(struct diskfs_bt_op *op)
{
    if (!op->dbg_park_counted) {
        op->dbg_park_counted = 1;
        op->dbg_park_phase   = op->phase;
        if ((unsigned) op->phase < 6) {
            __atomic_fetch_add(&g_dbg_park[op->phase], 1, __ATOMIC_RELAXED);
        }
    }
} /* diskfs_bt_op_park_account */

/* Called when a parked op is about to resume: drop its park-count. */
void
diskfs_bt_op_park_clear(struct diskfs_bt_op *op)
{
    if (op->dbg_park_counted) {
        op->dbg_park_counted = 0;
        if ((unsigned) op->dbg_park_phase < 6) {
            __atomic_fetch_sub(&g_dbg_park[op->dbg_park_phase], 1, __ATOMIC_RELAXED);
        }
    }
} /* diskfs_bt_op_park_clear */

/* Forward declarations (definitions below, in call-graph order) */

static inline void
diskfs_block_lru_unlink(
    struct diskfs_block_shard *shard,
    struct diskfs_block       *blk);

static struct diskfs_block *
diskfs_block_recycle(
    struct diskfs_thread      *thread,
    struct diskfs_block_shard *shard);

static void
diskfs_block_drain_returned_locked(
    struct diskfs_block_shard *shard);

static void
diskfs_block_drain_clean_locked(
    struct diskfs_block_shard *shard);

static inline void
diskfs_block_assert_iov(
    struct diskfs_thread *thread,
    struct diskfs_block  *blk);

static void
diskfs_bt_resume_drain(
    struct diskfs_thread *thread);

static void
diskfs_block_load_complete(
    struct evpl *evpl,
    int          status,
    void        *private_data);

static inline void
diskfs_bt_op_pin(
    struct diskfs_bt_op       *op,
    struct diskfs_block_shard *shard,
    struct diskfs_block       *blk);

static struct diskfs_block *
diskfs_block_claim_async(
    struct diskfs_thread *thread,
    uint32_t device_id,
    uint64_t device_offset,
    int is_new,
    void ( *resume )(struct diskfs_thread *, void *),
    void *arg);

static void
diskfs_pin_cont_resume(
    struct diskfs_thread *thread,
    void                 *arg);

static struct diskfs_block_buf *
diskfs_block_buf_reclaim_locked(
    struct diskfs_thread      *thread,
    struct diskfs_block_shard *shard);


static inline void
diskfs_block_lru_unlink(
    struct diskfs_block_shard *shard,
    struct diskfs_block       *blk)
{
    if (!blk->on_lru) {
        return;
    }
    if (blk->lru_prev) {
        blk->lru_prev->lru_next = blk->lru_next;
    } else {
        shard->lru_head = blk->lru_next;
    }
    if (blk->lru_next) {
        blk->lru_next->lru_prev = blk->lru_prev;
    } else {
        shard->lru_tail = blk->lru_prev;
    }
    blk->lru_prev = blk->lru_next = NULL;
    blk->on_lru   = 0;
    shard->lru_count--;
} /* diskfs_block_lru_unlink */


static void
diskfs_block_drain_returned_locked(struct diskfs_block_shard *shard)
{
    struct diskfs_block_buf *buf;

    buf = __atomic_exchange_n(&shard->returned_buffers, NULL, __ATOMIC_ACQUIRE);
    while (buf) {
        struct diskfs_block_buf *next = buf->next;

        chimera_diskfs_abort_if(!buf->on_free,
                                "returned diskfs block buffer is not marked free");
        diskfs_block_return_buf_locked(shard, buf);
        buf = next;
    }
} /* diskfs_block_drain_returned_locked */


static void
diskfs_block_drain_clean_locked(struct diskfs_block_shard *shard)
{
    struct diskfs_block *blk;

    blk = __atomic_exchange_n(&shard->clean_head, NULL, __ATOMIC_ACQUIRE);
    while (blk) {
        struct diskfs_block *next = blk->clean_next;

        blk->clean_next = NULL;
        __atomic_store_n(&blk->clean_queued, 0, __ATOMIC_RELEASE);

        if (__atomic_load_n(&blk->state, __ATOMIC_ACQUIRE) == DISKFS_BLOCK_CLEAN &&
            __atomic_load_n(&blk->pin_count, __ATOMIC_ACQUIRE) == 0 && !blk->on_lru) {
            diskfs_block_lru_push_tail(shard, blk);
        }
        blk = next;
    }
} /* diskfs_block_drain_clean_locked */


/*
 * Recycle the least-recently-used CLEAN, unpinned buffer for reuse at a new
 * key.  Caller holds the shard lock.  Returns a buffer with pin_count 0,
 * unlinked from the LRU and removed from its old bucket (a no-op for a free,
 * never-keyed buffer); the caller sets the new key/state and links it into the
 * new bucket.
 *
 * The pool is fixed and never grows or blocks: the cache is provisioned larger
 * than the maximum pinnable set (bounded by the intent log -- see the cache
 * sizing constants), so by the pigeonhole principle the LRU is never empty.
 * An empty LRU means every buffer in this shard is pinned -- a provisioning
 * violation or a leaked pin (and, since the descent that called us holds pins
 * in this shard, the precise self-deadlock condition).  Abort loudly rather
 * than block and hang.
 */
static struct diskfs_block *
diskfs_block_recycle(
    struct diskfs_thread      *thread,
    struct diskfs_block_shard *shard)
{
    struct diskfs_block *blk;
    struct diskfs_block *cur, *prev;
    uint32_t             ob;

    diskfs_block_drain_returned_locked(shard);
    diskfs_block_drain_clean_locked(shard);
    blk = shard->lru_head;

    /*
     * Block-swap model: every block lives on the LRU all the time -- clean,
     * unreferenced recycle candidates AND pinned-dirty blocks still moving
     * through the intent-log pipeline (including retired ones no longer in the
     * hash).  Blocks are (re)inserted at the MRU end in recycle/commit order,
     * which tracks intent-log drain order, so the head is the oldest block and
     * the first to drain.  Take the head only if it is a clean, unreferenced
     * candidate (pin_count == 0 && CLEAN); if even the head is still referenced
     * the whole shard is genuinely busy -- return NULL so the caller parks
     * (async) or treats it as a provisioning violation (sync).  No scan: by
     * drain order the head is the best (and effectively the only) candidate.
     */
    if (!blk ||
        __atomic_load_n(&blk->pin_count, __ATOMIC_ACQUIRE) != 0 ||
        __atomic_load_n(&blk->state, __ATOMIC_ACQUIRE) != DISKFS_BLOCK_CLEAN) {
        return NULL;
    }

    diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_RECYCLE);

    if (blk->wait_head != NULL) {
        diskfs_block_recycle_waiter_trace(blk);
    }

    /* Move the victim to the MRU end: it is about to become a fresh live block,
     * so it must not be reconsidered until it ages back to the head.  It never
     * leaves the LRU (block-swap: a block and its buffer are paired for life). */
    diskfs_block_lru_unlink(shard, blk);
    diskfs_block_lru_push_tail(shard, blk);

    /* Unhook from its current bucket (no-op for a retired/never-keyed block:
     * it is in no chain, so the pointer search simply finds nothing). */
    ob   = diskfs_block_bucket(blk->device_id, blk->device_offset);
    prev = NULL;
    for (cur = shard->buckets[ob]; cur; prev = cur, cur = cur->hash_next) {
        if (cur == blk) {
            if (prev) {
                prev->hash_next = cur->hash_next;
            } else {
                shard->buckets[ob] = cur->hash_next;
            }
            break;
        }
    }
    blk->hash_next = NULL;
    return blk;
} /* diskfs_block_recycle */


/*
 * Block-swap CoW.  The block X keyed at (device_id, device_offset) has its
 * buffer referenced by an un-pushed redo record (buf->refs > 1), so it must stay
 * immutable until the tail-pusher writes it home.  Rather than swap the BUFFER
 * (which orphans a donor block and is what leaked), take a fresh recycle victim
 * Y, copy X's content into Y's OWN buffer, and swap Y into the hash for
 * (device_id, device_offset).  X is retired: removed from the hash but left on
 * the LRU, still holding its own buffer and still pinned by its record(s); once
 * the pusher writes it home and drops the pin it ages back to a normal recycle
 * candidate.  Blocks and buffers stay paired 1:1 -- no bufless donor, no detach,
 * no re-pair.  Caller holds the shard lock.  Returns the new live block Y, or
 * NULL if no recycle victim is available (caller parks / aborts).
 */
static struct diskfs_block *
diskfs_block_cow_swap(
    struct diskfs_thread      *thread,
    struct diskfs_block_shard *shard,
    struct diskfs_block       *x,
    uint32_t                   device_id,
    uint64_t                   device_offset,
    uint32_t                   bucket)
{
    struct diskfs_block  *y;
    struct diskfs_block **pp;

    /* X is the active block being forked and is pinned (refs>1 => a record holds
     * it), so it is never a recycle victim -- but if it happened to sit at the
     * LRU head, recycle would see a pinned head and spuriously park.  Move it to
     * the MRU end first so recycle finds a genuinely idle victim. */
    diskfs_block_lru_unlink(shard, x);
    diskfs_block_lru_push_tail(shard, x);

    y = diskfs_block_recycle(thread, shard);
    if (!y) {
        return NULL;     /* no clean victim at the LRU head -- caller parks */
    }

    /* y is keyless (off its old bucket), on the LRU tail, CLEAN, pin 0. */
    memcpy(y->iov.data, x->iov.data, DISKFS_BLOCK_SIZE);

    /* Retire X out of the hash (it stays on the LRU, pinned, with its buffer). */
    pp = &shard->buckets[bucket];
    while (*pp && *pp != x) {
        pp = &(*pp)->hash_next;
    }
    if (*pp) {
        *pp = x->hash_next;
    }
    x->hash_next = NULL;

    /* Publish Y as the live, writable copy at (device_id, device_offset). */
    y->device_id     = device_id;
    y->device_offset = device_offset;
    __atomic_store_n(&y->seq, __atomic_load_n(&x->seq, __ATOMIC_RELAXED),
                     __ATOMIC_RELEASE);
    y->wait_head           = NULL;
    y->wait_tail           = NULL;
    y->hash_next           = shard->buckets[bucket];
    shard->buckets[bucket] = y;
    __atomic_store_n(&y->state, DISKFS_BLOCK_CLEAN, __ATOMIC_RELEASE);

    diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_COW);
    return y;
} /* diskfs_block_cow_swap */


/*
 * Obtain a buffer for a CoW fork.  Prefer a spare on the free list; otherwise
 * evict the LRU's least-recently-used CLEAN, unpinned block and take its buffer
 * -- exactly like a cache miss recycles -- leaving the donor struct bufless on
 * free_blocks until a freed buffer re-pairs with it (diskfs_block_return_buf_
 * locked).  Returns NULL when every block in the shard is pinned (no victim):
 * the async caller parks and resumes when a buffer/victim frees; a synchronous
 * caller (pre-faulted descent) treats it as the recycle provisioning violation
 * it already is.  Caller holds the shard lock.
 */
/* Unused under the block-swap CoW model (CoW swaps whole blocks via
 * diskfs_block_cow_swap, never buffers); kept for reference, remove later. */
__attribute__((unused))
static struct diskfs_block_buf *
diskfs_block_buf_reclaim_locked(
    struct diskfs_thread      *thread,
    struct diskfs_block_shard *shard)
{
    struct diskfs_block_buf *buf;
    struct diskfs_block     *blk, *cur, *prev;
    uint32_t                 ob;

    diskfs_block_drain_returned_locked(shard);

    buf = shard->free_buffers;
    if (buf) {
        shard->free_buffers = buf->next;
        shard->nfree_buffers--;
        buf->next = NULL;
        __atomic_store_n(&buf->on_free, 0, __ATOMIC_RELEASE);
        __atomic_store_n(&buf->refs, 1, __ATOMIC_RELEASE);
        return buf;
    }

    blk = shard->lru_head;
    if (!blk) {
        return NULL;        /* every block pinned: park (async) / abort (sync) */
    }
    diskfs_block_lru_unlink(shard, blk);

    /* Unhook from its bucket (no-op for an already-keyless struct). */
    ob   = diskfs_block_bucket(blk->device_id, blk->device_offset);
    prev = NULL;
    for (cur = shard->buckets[ob]; cur; prev = cur, cur = cur->hash_next) {
        if (cur == blk) {
            if (prev) {
                prev->hash_next = cur->hash_next;
            } else {
                shard->buckets[ob] = cur->hash_next;
            }
            break;
        }
    }
    blk->hash_next = NULL;

    buf                = blk->buf;
    blk->buf           = NULL;
    blk->iov.data      = NULL;
    blk->free_next     = shard->free_blocks;
    shard->free_blocks = blk;
    shard->n_bufless++;

    buf->next = NULL;
    __atomic_store_n(&buf->on_free, 0, __ATOMIC_RELEASE);
    __atomic_store_n(&buf->refs, 1, __ATOMIC_RELEASE);
    diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_RECYCLE);
    return buf;
} /* diskfs_block_buf_reclaim_locked */


/*
 * Wake CoW forks parked on this shard for want of a buffer.  Called from every
 * path that frees a buffer or unpins a block (so a victim/buffer is now
 * reclaimable), including the cross-thread tail-pusher -- otherwise a shard
 * whose every op is parked would never be drained and would hang.  Wakes one
 * waiter per available resource; a waiter that loses the reclaim race re-parks.
 */
void
diskfs_block_buf_wake(
    struct diskfs_thread      *self,
    struct diskfs_block_shard *shard)
{
    struct diskfs_block_waiter *list = NULL, *w;

    if (!__atomic_load_n(&shard->buf_wait_pending, __ATOMIC_ACQUIRE)) {
        return;
    }

    pthread_mutex_lock(&shard->lock);
    diskfs_block_drain_returned_locked(shard);
    /*
     * Release at most (reclaimable - reserve) waiters: exactly the number that
     * can claim a buffer and clear the gate, so a woken claim makes progress
     * instead of re-contending the shard lock and re-parking.  Without this
     * bound, every freed buffer woke ALL waiters on a hot shard -> thundering
     * herd: dozens of workers spin on the one shard mutex (holding inode write
     * locks the whole time) and re-park, collapsing throughput into a multi-
     * minute inode-lock convoy. */
    {
        uint32_t reclaimable = shard->nfree_buffers + shard->lru_count;
        uint32_t budget      = reclaimable > DISKFS_BLOCK_BUF_RESERVE ?
            reclaimable - DISKFS_BLOCK_BUF_RESERVE : 0;

        while (shard->buf_wait_head && budget > 0) {
            w                    = shard->buf_wait_head;
            shard->buf_wait_head = w->next;
            if (!shard->buf_wait_head) {
                shard->buf_wait_tail = NULL;
            }
            w->next = list;
            list    = w;
            budget--;
        }
    }
    if (!shard->buf_wait_head) {
        __atomic_store_n(&shard->buf_wait_pending, 0, __ATOMIC_RELEASE);
    }
    pthread_mutex_unlock(&shard->lock);

    while (list) {
        w    = list;
        list = w->next;
        diskfs_block_waiter_dispatch(self, w);
    }
} /* diskfs_block_buf_wake */


void
diskfs_block_unpin(
    struct diskfs_thread   *thread,
    struct diskfs_block    *blk,
    enum diskfs_block_state new_state)
{
    /*
     * Drop one pin.  pin_count is the block's outstanding-reference count: every
     * un-home write generation holds a pin from claim until its redo record is
     * pushed home and trimmed (released by the push thread in free_record), and
     * every active read/descent holds one for its duration.  So pin_count == 0
     * is exactly "content is home and nobody is using it" -- the block is then
     * CLEAN and reusable.  A block is therefore marked CLEAN by *whichever* drop
     * takes the count to zero, never before (a concurrent generation or read
     * keeps it pinned and thus DIRTY).  new_state is advisory: we only ever
     * transition to CLEAN, and only at pin==0.
     */
    (void) new_state;
    if (__atomic_sub_fetch(&blk->pin_count, 1, __ATOMIC_ACQ_REL) != 0) {
        return;     /* still referenced -- stays as-is (DIRTY if un-home) */
    }

    /* Took the last pin: home + idle -> publish CLEAN and link onto the LRU
    * under the shard lock, re-checking since a concurrent claim may have
    * re-pinned, and draining deferred clean insertions while we hold it. */
    {
        struct diskfs_block_shard *shard = diskfs_block_shard(thread->shared->block_cache,
                                                              blk->device_id, blk->device_offset);
        pthread_mutex_lock(&shard->lock);
        diskfs_block_drain_clean_locked(shard);
        if (__atomic_load_n(&blk->pin_count, __ATOMIC_ACQUIRE) == 0) {
            __atomic_sub_fetch(&shard->pinned, 1, __ATOMIC_RELAXED);  /* 1->0 */
            __atomic_store_n(&blk->state, DISKFS_BLOCK_CLEAN, __ATOMIC_RELEASE);
            if (!blk->on_lru) {
                diskfs_block_lru_push_tail(shard, blk);
            }
        }
        pthread_mutex_unlock(&shard->lock);

        /* This produced a CLEAN, unpinned LRU victim -- resume any CoW fork
         * parked waiting for a reclaimable buffer (main's park machinery). */
        diskfs_block_buf_wake(thread, shard);
    }
} /* diskfs_block_unpin */


/* Release a descent pin.  Like diskfs_block_unpin: the block becomes CLEAN and
 * LRU-reusable only if this was the last pin (no un-home generation, no other
 * reader still holds it). */
void
diskfs_block_release(
    struct diskfs_thread *thread,
    struct diskfs_block  *blk)
{
    struct diskfs_block_shard *shard = diskfs_block_shard(thread->shared->block_cache,
                                                          blk->device_id, blk->device_offset);

    pthread_mutex_lock(&shard->lock);
    diskfs_block_drain_clean_locked(shard);
    if (__atomic_sub_fetch(&blk->pin_count, 1, __ATOMIC_ACQ_REL) == 0) {
        __atomic_sub_fetch(&shard->pinned, 1, __ATOMIC_RELAXED);  /* 1->0 */
        __atomic_store_n(&blk->state, DISKFS_BLOCK_CLEAN, __ATOMIC_RELEASE);
        if (!blk->on_lru) {
            diskfs_block_lru_push_tail(shard, blk);
        }
    }
    pthread_mutex_unlock(&shard->lock);

    /* Unpinning may have produced an LRU victim a parked CoW fork can reclaim. */
    diskfs_block_buf_wake(thread, shard);
} /* diskfs_block_release */


void
diskfs_block_cache_create(struct diskfs_shared *shared)
{
    struct diskfs_block_cache *cache   = calloc(1, sizeof(*cache));
    uint64_t                   il_size = shared->intent_log_size;
    uint32_t                   min     = (uint32_t) DISKFS_BLOCK_CACHE_MIN_BLOCKS(il_size);
    uint32_t                   total   = shared->block_cache_blocks ?
        shared->block_cache_blocks : (uint32_t) DISKFS_BLOCK_CACHE_DEFAULT_BLOCKS(il_size);
    int                        i;
    uint32_t                   j;
    uint32_t                   extra;

    /* The pool never grows or blocks, so it must clear the maximum pinnable
     * set; floor an under-sized configuration rather than risk the recycle
     * abort under load. */
    if (total < min) {
        total = min;
    }

    cache->shard_cap = total / DISKFS_BLOCK_CACHE_SHARDS;
    if (cache->shard_cap == 0) {
        cache->shard_cap = 1;
    }

    /* No dedicated CoW slush: a fork draws its buffer from the regular LRU
     * (diskfs_block_buf_reclaim_locked) and parks if the shard is fully pinned,
     * so backing buffers are 1:1 with block structs. */
    extra                         = 0;
    cache->buffer_extra_per_shard = extra;
    pthread_mutex_init(&cache->prealloc_lock, NULL);

    for (i = 0; i < DISKFS_BLOCK_CACHE_SHARDS; i++) {
        struct diskfs_block_shard *shard = &cache->shards[i];

        pthread_mutex_init(&shard->lock, NULL);
        shard->buckets = calloc(DISKFS_BLOCK_CACHE_BUCKETS_PER_SHARD,
                                sizeof(struct diskfs_block *));
        shard->pool = calloc(cache->shard_cap, sizeof(struct diskfs_block));

        /* Pre-populate the struct pool: every block starts free (unkeyed, in
         * no bucket) and CLEAN on the LRU, with no buffer yet (iov.data NULL);
         * the iovec is allocated on first use and reused thereafter. */
        for (j = 0; j < cache->shard_cap; j++) {
            struct diskfs_block *blk = &shard->pool[j];

            blk->state = DISKFS_BLOCK_CLEAN;
            diskfs_block_lru_push_tail(shard, blk);
            shard->nblocks++;
        }
    }
    shared->block_cache = cache;
} /* diskfs_block_cache_create */


void
diskfs_block_cache_prealloc(
    struct diskfs_shared *shared,
    struct evpl          *evpl)
{
    struct diskfs_block_cache *cache = shared->block_cache;
    uint32_t                   i, j;

    pthread_mutex_lock(&cache->prealloc_lock);
    if (cache->buffers_ready) {
        pthread_mutex_unlock(&cache->prealloc_lock);
        return;
    }

    for (i = 0; i < DISKFS_BLOCK_CACHE_SHARDS; i++) {
        struct diskfs_block_shard *shard = &cache->shards[i];
        uint32_t                   total = shard->nblocks + cache->buffer_extra_per_shard;

        struct evpl_iovec         *big            = NULL;
        uint32_t                   slices_per_big = 0;
        uint32_t                   slot_in_big    = 0;

        shard->buffers  = calloc(total, sizeof(*shard->buffers));
        shard->nbuffers = total;

        /* Backing GLOBAL buffers: worst case (buffer_size == BLOCK_SIZE) is one
         * big buffer per slice; trimmed to the exact count after carving. */
        shard->global_bufs   = calloc(total ? total : 1, sizeof(*shard->global_bufs));
        shard->n_global_bufs = 0;

        for (j = 0; j < total; j++) {
            struct diskfs_block_buf *buf = &shard->buffers[j];

            if (slot_in_big == 0) {
                /* Carve subsequent slices from a fresh whole GLOBAL buffer. */
                big = &shard->global_bufs[shard->n_global_bufs++];
                evpl_iovec_alloc_global(evpl, big);
                slices_per_big = big->length / DISKFS_BLOCK_SIZE;
                chimera_diskfs_abort_if(slices_per_big == 0,
                                        "evpl buffer_size %u < DISKFS_BLOCK_SIZE %u",
                                        big->length, (unsigned) DISKFS_BLOCK_SIZE);
            }

            /* Non-owning GLOBAL slice into the big buffer: clone_segment copies
             * the (GLOBAL) ref and sets data+offset/length.  The ref incr is a
             * no-op, so the slice takes no atomic and owns nothing -- the big
             * buffer is freed once at teardown, never the slice. */
            evpl_iovec_clone_segment(&buf->iov, big,
                                     slot_in_big * DISKFS_BLOCK_SIZE,
                                     DISKFS_BLOCK_SIZE);

            if (++slot_in_big == slices_per_big) {
                slot_in_big = 0;
            }

            buf->shard          = shard;
            buf->on_free        = 1;
            buf->next           = shard->free_buffers;
            shard->free_buffers = buf;
            shard->nfree_buffers++;
        }

        if (shard->n_global_bufs) {
            shard->global_bufs = realloc(shard->global_bufs,
                                         shard->n_global_bufs *
                                         sizeof(*shard->global_bufs));
        }

        for (j = 0; j < shard->nblocks; j++) {
            struct diskfs_block     *blk = &shard->pool[j];
            struct diskfs_block_buf *buf = diskfs_block_buf_alloc_locked(shard);

            blk->buf = buf;
            blk->iov = buf->iov;
        }
    }

    cache->buffers_ready = 1;
    pthread_mutex_unlock(&cache->prealloc_lock);
} /* diskfs_block_cache_prealloc */


void
diskfs_block_cache_destroy(struct diskfs_shared *shared)
{
    struct diskfs_block_cache *cache = shared->block_cache;
    int                        i;

    if (!cache) {
        return;
    }

    for (i = 0; i < DISKFS_BLOCK_CACHE_SHARDS; i++) {
        struct diskfs_block_shard *shard = &cache->shards[i];
        uint32_t                   j;

        /* Free the backing GLOBAL buffers once each (NULL evpl -> straight to
         * the global allocator, correct at teardown).  The per-block iovecs are
         * non-owning slices into these and must NOT be released individually --
         * doing so would free a whole buffer still backing its sibling slices. */
        for (j = 0; j < shard->n_global_bufs; j++) {
            evpl_iovec_release(NULL, &shard->global_bufs[j]);
        }
        free(shard->global_bufs);
        free(shard->buffers);
        free(shard->pool);
        free(shard->buckets);
        pthread_mutex_destroy(&shard->lock);
    }
    pthread_mutex_destroy(&cache->prealloc_lock);
    free(cache);
    shared->block_cache = NULL;
} /* diskfs_block_cache_destroy */


void
diskfs_block_buf_release(struct diskfs_block_buf *buf)
{
    struct diskfs_block_shard *shard;
    struct diskfs_block_buf   *head;

    chimera_diskfs_abort_if(__atomic_load_n(&buf->refs, __ATOMIC_ACQUIRE) == 0,
                            "diskfs block buffer ref underflow");
    if (__atomic_sub_fetch(&buf->refs, 1, __ATOMIC_ACQ_REL) != 0) {
        return;
    }

    shard = buf->shard;
    chimera_diskfs_abort_if(__atomic_exchange_n(&buf->on_free, 1, __ATOMIC_ACQ_REL),
                            "diskfs block buffer already free");

    do {
        head      = __atomic_load_n(&shard->returned_buffers, __ATOMIC_ACQUIRE);
        buf->next = head;
    } while (!__atomic_compare_exchange_n(&shard->returned_buffers, &head, buf,
                                          0, __ATOMIC_RELEASE, __ATOMIC_ACQUIRE));

    /* The tail-pusher just freed a buffer cross-thread.  Wake any CoW fork
     * parked on this shard -- if every worker op on the shard is parked, nothing
     * else will drain the returned stack and the shard would hang.  self=NULL:
     * the pusher is no waiter's worker, so the wake always rings the doorbell. */
    diskfs_block_buf_wake(NULL, shard);
} /* diskfs_block_buf_release */


/*
 * Ensure a block has a backing buffer.  Buffers are SHARED evpl iovecs so the
 * intent logger and tail-pusher can reference them zero-copy for I/O (and RDMA)
 * and hand the reference across threads.  A recycled block keeps (reuses) its
 * iovec -- a CLEAN block's buffer is referenced only by the cache (refcount 1)
 * -- so an allocation happens only on a never-yet-used pool slot (and on COW).
 */
static inline void
diskfs_block_assert_iov(
    struct diskfs_thread *thread,
    struct diskfs_block  *blk)
{
    (void) thread;
    chimera_diskfs_abort_if(!blk->buf || !blk->iov.data,
                            "diskfs block used before buffer preallocation");
} /* diskfs_block_assert_iov */


/*
 * Find or create the cache entry for (device_id, device_offset) and pin it.
 * On a miss a buffer is obtained from the shard pool (recycling the LRU
 * eviction candidate): is_new (a freshly space-map-allocated block) starts
 * zeroed; otherwise -- always, now that eviction can discard a resident CLEAN
 * block whose content is already home -- the buffer is repopulated from disk so
 * a re-claimed evicted block keeps its contents.  A hit unlinks the block from
 * the LRU (it is now pinned, not a candidate).
 */
struct diskfs_block *
diskfs_block_claim(
    struct diskfs_thread *thread,
    uint32_t              device_id,
    uint64_t              device_offset,
    int                   is_new)
{
    struct diskfs_block_cache *cache  = thread->shared->block_cache;
    uint64_t                   hash   = diskfs_block_hash(device_id, device_offset);
    uint32_t                   sidx   = hash & DISKFS_BLOCK_CACHE_SHARD_MASK;
    uint32_t                   bucket = (hash >> 8) & DISKFS_BLOCK_CACHE_BUCKET_MASK;
    struct diskfs_block_shard *shard  = &cache->shards[sidx];
    struct diskfs_block       *blk;

    pthread_mutex_lock(&shard->lock);

    diskfs_block_drain_returned_locked(shard);
    diskfs_block_drain_clean_locked(shard);
    blk = diskfs_block_lookup_locked(shard, bucket, device_id, device_offset);
    if (!blk) {
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_MISS);
        blk = diskfs_block_recycle(thread, shard);
        /* The synchronous claim cannot park: a NULL here means the LRU head is
         * still referenced, i.e. the shard is genuinely full of in-flight work.
         * This path runs only on pre-faulted, pinned descents, so it should not
         * happen; abort rather than hang. */
        chimera_diskfs_abort_if(!blk,
                                "synchronous block_claim: shard full (LRU head "
                                "still pinned) off=%lu", device_offset);
        blk->device_id     = device_id;
        blk->device_offset = device_offset;
        __atomic_store_n(&blk->state, DISKFS_BLOCK_CLEAN, __ATOMIC_RELEASE);
        __atomic_store_n(&blk->seq, 0, __ATOMIC_RELEASE);
        blk->wait_head = NULL;
        blk->wait_tail = NULL;
        diskfs_block_assert_iov(thread, blk);
        if (is_new) {
            diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_NEW);
        }

        chimera_diskfs_abort_if(!is_new,
                                "synchronous block_claim miss off=%lu -- "
                                "caller must pre-fault or use the async path",
                                device_offset);
        memset(blk->iov.data, 0, DISKFS_BLOCK_SIZE);

        /* recycle left blk on the LRU tail; just key it into the new bucket. */
        blk->hash_next         = shard->buckets[bucket];
        shard->buckets[bucket] = blk;
    } else if (blk->buf->refs > 1) {
        /* CoW (block-swap): the live buffer is still in an un-pushed record.
         * The synchronous claim cannot park, so a NULL fork is a provisioning
         * violation (pre-faulted descent should always leave a clean LRU head). */
        struct diskfs_block *y = diskfs_block_cow_swap(thread, shard, blk,
                                                       device_id, device_offset,
                                                       bucket);
        chimera_diskfs_abort_if(!y,
                                "synchronous CoW: shard full (LRU head still "
                                "pinned) off=%lu", device_offset);
        blk = y;
    } else {
        /* Resident hit (refs == 1).  The block stays on the LRU (block-swap);
         * move it to the MRU end since it is now active. */
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_HIT);
        diskfs_block_lru_unlink(shard, blk);
        diskfs_block_lru_push_tail(shard, blk);
    }

    if (__atomic_add_fetch(&blk->pin_count, 1, __ATOMIC_ACQ_REL) == 1) {
        __atomic_add_fetch(&shard->pinned, 1, __ATOMIC_RELAXED);
    }
    pthread_mutex_unlock(&shard->lock);

    return blk;
} /* diskfs_block_claim */


void *
diskfs_sm_claim_block(
    void    *user,
    uint32_t device_id,
    uint64_t device_offset,
    int      is_new)
{
    struct diskfs_sm_jnl *c = user;
    struct diskfs_block  *blk;

    /* Route the journal-block claim through the async path: a not-resident log
     * block parks this journaling op's continuation (c->resume) and issues the
     * read, returning NULL so the allocator unwinds with SM_AGAIN and the
     * caller re-drives once it loads. */
    blk = diskfs_block_claim_async(c->thread, device_id, device_offset, is_new,
                                   c->resume, c->resume_arg);
    if (!blk) {
        return NULL;
    }

    diskfs_txn_add_block(c->txn, blk);
    c->txn->n_journal++;     /* diag: this add is an AG-log journal-block claim */
    return blk->iov.data;
} /* diskfs_sm_claim_block */


/*
 * Record an ALLOC space-map delta into the current transaction.  Replaces the
 * old per-AG on-disk delta log for allocations: instead of claiming (and
 * possibly parking on) a shared AG-log block, the allocator appends an in-memory
 * node that diskfs_il_write_redo serializes into this txn's redo record.  Purely
 * in-memory -- no block I/O, never parks.  Frees are not recorded here; they
 * ride the redo record from txn->pending_frees.
 */
void
diskfs_sm_record_delta(
    void    *user,
    uint32_t device_id,
    uint32_t ag_index,
    uint64_t device_offset,
    uint64_t length,
    uint32_t op)
{
    struct diskfs_sm_jnl    *c = user;
    struct diskfs_txn_delta *d;

    d                    = malloc(sizeof(*d));
    d->device_id         = device_id;
    d->ag_index          = ag_index;
    d->device_offset     = device_offset;
    d->length            = length;
    d->op                = op;
    d->next              = c->txn->space_deltas;
    c->txn->space_deltas = d;
    c->txn->n_space_deltas++;
} /* diskfs_sm_record_delta */


/* A journaling context for a site that has guaranteed its log blocks are
 * resident (e.g. a pre-reserved b+tree modify): a claim must never park, so a
 * suspend here is a bug. */
void
diskfs_sm_no_suspend(
    struct diskfs_thread *thread,
    void                 *arg)
{
    (void) thread;
    (void) arg;
    chimera_diskfs_abort("space-map journal parked where it must not suspend");
} /* diskfs_sm_no_suspend */


/*
 * Free a device range as part of a transaction.  The FREE delta rides this
 * txn's redo record, but the in-memory free is deferred onto the txn's pending
 * list and applied only once the record is durable (diskfs_txn_apply_frees) or
 * discarded on abort (diskfs_txn_discard_frees).  This is required for metadata
 * blocks (b+tree nodes), which unlike file data are resident + pinned in the
 * block cache: applying the free immediately could hand the range to a
 * concurrent allocation that then claims the stale, still-pinned block.
 * Deferring to commit (block is LOGGED, unpinned by then) makes a re-claim COW
 * cleanly.
 */
void
diskfs_txn_free_space(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    uint32_t              device_id,
    uint64_t              device_offset,
    uint64_t              length)
{
    struct diskfs_txn_free *f;

    (void) thread;

    /* Record the pending free only -- the FREE delta is appended to the txn's
     * redo space-delta list later, in the pre-commit flush
     * (diskfs_txn_flush_free_journals), since diskfs_txn_free_space runs inside
     * the b+tree synchronous modify (merge frees the emptied sibling, teardown
     * frees nodes).  The in-memory free stays deferred to commit
     * (diskfs_txn_apply_frees). */
    f                  = malloc(sizeof(*f));
    f->device_id       = device_id;
    f->device_offset   = device_offset;
    f->length          = length;
    f->journaled       = 0;
    f->next            = txn->pending_frees;
    txn->pending_frees = f;
} /* diskfs_txn_free_space */


/*
 * Append the FREE delta for every pending free recorded during the op's modify
 * onto the txn's redo space-delta list (so it rides this txn's redo record and
 * is replayed on crash).  A pre-commit phase run on the worker before the txn
 * is handed to the intent-log thread.  Purely in-memory now (the allocator no
 * longer journals to a per-AG on-disk log), so it never parks; the SM_AGAIN
 * return is vestigial.  Already-recorded frees are flagged for idempotency.
 */
int
diskfs_txn_flush_free_journals(
    struct diskfs_thread     *thread,
    struct diskfs_txn        *txn,
    struct diskfs_commit_ctx *cctx)
{
    struct diskfs_txn_free *f;

    DISKFS_SM_JNL(jnl, thread, txn, diskfs_commit_resume, cctx);

    for (f = txn->pending_frees; f; f = f->next) {
        if (f->journaled) {
            continue;
        }
        if (space_map_free_journal(thread->shared->space_map, &jnl,
                                   f->device_id, f->device_offset,
                                   f->length) == SM_AGAIN) {
            return SM_AGAIN;
        }
        f->journaled = 1;
    }
    return 0;
} /* diskfs_txn_flush_free_journals */


/* Commit a txn's pending frees -- the ranges become reusable.  Runs on the
 * intent-log thread once the redo record is durable, after the txn's blocks
 * have been unpinned (so a freed metadata block is LOGGED, not DIRTY-pinned). */
void
diskfs_txn_apply_frees(struct diskfs_txn *txn)
{
    struct space_map       *sm = txn->thread->shared->space_map;
    struct diskfs_txn_free *f, *n;

    for (f = txn->pending_frees; f; f = n) {
        n = f->next;
        space_map_free_apply(sm, f->device_id, f->device_offset, f->length);
        free(f);
    }
    txn->pending_frees = NULL;
} /* diskfs_txn_apply_frees */


/* Discard a txn's pending frees without applying (abort): the journaled FREE
 * deltas never become durable, so the ranges stay allocated. */
void
diskfs_txn_discard_frees(struct diskfs_txn *txn)
{
    struct diskfs_txn_free *f, *n;

    for (f = txn->pending_frees; f; f = n) {
        n = f->next;
        free(f);
    }
    txn->pending_frees = NULL;
} /* diskfs_txn_discard_frees */


/* Apply a txn's committed allocations to the in-memory free tree, on the
 * intent-log thread once the redo record is durable (beside diskfs_txn_apply_
 * frees).  The reservation allocator hands out blocks thread-locally without
 * removing them from the tree, so the tree stays == committed state until here
 * -- which is what lets condense snapshot committed-only and never leak an
 * uncommitted reservation tail on crash. */
void
diskfs_txn_apply_allocs(struct diskfs_txn *txn)
{
    struct space_map        *sm = txn->thread->shared->space_map;
    struct diskfs_txn_delta *d;

    for (d = txn->space_deltas; d; d = d->next) {
        if (d->op == SM_AG_LOG_OP_ALLOC) {
            space_map_alloc_apply(sm, d->device_id, d->device_offset, d->length);
        }
    }
} /* diskfs_txn_apply_allocs */


/* Discard a txn's allocations on abort: the redo never becomes durable so the
 * ranges were never marked used (tree untouched), but each bump pinned its
 * claim -- unpin them so the claim can be GC'd. */
void
diskfs_txn_discard_allocs(struct diskfs_txn *txn)
{
    struct space_map        *sm = txn->thread->shared->space_map;
    struct diskfs_txn_delta *d;

    for (d = txn->space_deltas; d; d = d->next) {
        if (d->op == SM_AG_LOG_OP_ALLOC) {
            space_map_alloc_discard(sm, d->device_id, d->device_offset, d->length);
        }
    }
} /* diskfs_txn_discard_allocs */


/*
 * Enqueue a ready waiter on its owning worker's resume queue.  If the waking
 * thread is the waiter's own worker, schedule a deferral (no eventfd);
 * otherwise ring the cross-thread doorbell.
 */
void
diskfs_block_waiter_dispatch(
    struct diskfs_thread       *waker,
    struct diskfs_block_waiter *w)
{
    struct diskfs_thread *worker = w->thread;

    pthread_mutex_lock(&worker->resume_lock);
    w->next = NULL;
    if (worker->resume_tail) {
        worker->resume_tail->next = w;
    } else {
        worker->resume_head = w;
    }
    worker->resume_tail = w;
    __atomic_store_n(&worker->resume_pending, 1, __ATOMIC_RELEASE);
    pthread_mutex_unlock(&worker->resume_lock);

    if (worker == waker) {
        evpl_defer(worker->evpl, &worker->resume_deferral);
    } else {
        evpl_ring_doorbell(&worker->resume_doorbell);
    }
} /* diskfs_block_waiter_dispatch */


/*
 * Block-swap park: a claim found the LRU head still referenced (no recycle
 * victim).  The lock-free pusher no longer wakes anyone, so the worker
 * self-recovers: re-queue this op's continuation onto its own resume/defer
 * queue, which re-runs it next loop iteration (and again, until recycle
 * succeeds).  The pusher discharges blocks lock-free meanwhile, so the LRU head
 * clears and the retry lands -- coverage can't be starved by the retries.
 * Caller holds the shard lock; we drop it before touching the resume queue
 * (lock order: never hold a shard lock under resume_lock).  Returns NULL.
 */
static inline struct diskfs_block *
diskfs_block_defer_retry(
    struct diskfs_thread *thread,
    struct diskfs_block_shard *shard,
    void ( *resume )(struct diskfs_thread *, void *),
    void *arg)
{
    struct diskfs_block_waiter *w = diskfs_block_waiter_alloc(thread);

    w->thread = thread;
    w->resume = resume;
    w->arg    = arg;
    w->next   = NULL;
    pthread_mutex_unlock(&shard->lock);
    diskfs_block_waiter_dispatch(thread, w);   /* self -> evpl_defer, re-runs */
    return NULL;
} /* diskfs_block_defer_retry */


/* Resume trampoline for a parked b+tree op: re-enter its driver. */
void
diskfs_bt_op_resume_cb(
    struct diskfs_thread *thread,
    void                 *arg)
{
    struct diskfs_bt_op *op = arg;

    (void) thread;
    diskfs_bt_op_park_clear(op);     /* resuming -> no longer parked (op alive here) */
    diskfs_bt_run(op);               /* may complete + free op via cb; do NOT touch op after */
} /* diskfs_bt_op_resume_cb */


/* Drain this worker's resume queue, invoking each ready waiter's continuation
 * (which re-enters its driver / request step), then recycling the waiter. */
static void
diskfs_bt_resume_drain(struct diskfs_thread *thread)
{
    struct diskfs_block_waiter *list, *w;

    if (!__atomic_load_n(&thread->resume_pending, __ATOMIC_ACQUIRE)) {
        return;
    }

    pthread_mutex_lock(&thread->resume_lock);
    list                = thread->resume_head;
    thread->resume_head = NULL;
    thread->resume_tail = NULL;
    __atomic_store_n(&thread->resume_pending, 0, __ATOMIC_RELEASE);
    pthread_mutex_unlock(&thread->resume_lock);

    while (list) {
        void  (*resume)(
            struct diskfs_thread *,
            void *);
        void *arg;

        w      = list;
        list   = w->next;
        resume = w->resume;
        arg    = w->arg;
        diskfs_block_waiter_free(thread, w);
        resume(thread, arg);
    }
} /* diskfs_bt_resume_drain */


void
diskfs_bt_resume_doorbell_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct diskfs_thread *thread = container_of(doorbell, struct diskfs_thread,
                                                resume_doorbell);

    (void) evpl;
    diskfs_bt_resume_drain(thread);
} /* diskfs_bt_resume_doorbell_cb */


void
diskfs_bt_resume_deferral_cb(
    struct evpl *evpl,
    void        *private_data)
{
    (void) evpl;
    diskfs_bt_resume_drain(private_data);
} /* diskfs_bt_resume_deferral_cb */

void
diskfs_bt_resume_poll(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_thread *thread = private_data;

    if (__atomic_load_n(&thread->resume_pending, __ATOMIC_ACQUIRE)) {
        diskfs_bt_resume_drain(thread);
        evpl_activity(evpl);
    }
} /* diskfs_bt_resume_poll */


/* Block read completion: data landed directly in blk->iov; mark CLEAN, wake. */
static void
diskfs_block_load_complete(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct diskfs_block_load   *ld    = private_data;
    struct diskfs_block        *blk   = ld->blk;
    struct diskfs_thread       *self  = ld->thread;
    struct diskfs_block_shard  *shard = diskfs_block_shard(self->shared->block_cache,
                                                           blk->device_id, blk->device_offset);
    struct diskfs_block_waiter *waiters, *w;

    (void) evpl;
    chimera_diskfs_abort_if(status != 0, "block read failed off=%lu status=%d",
                            blk->device_offset, status);

    pthread_mutex_lock(&shard->lock);
    __atomic_store_n(&blk->state, DISKFS_BLOCK_CLEAN, __ATOMIC_RELEASE);
    waiters        = blk->wait_head;
    blk->wait_head = NULL;
    blk->wait_tail = NULL;
    pthread_mutex_unlock(&shard->lock);

    diskfs_pending_io_add(self, -1);
    free(ld);

    while (waiters) {
        w       = waiters;
        waiters = w->next;
        diskfs_block_waiter_dispatch(self, w);
    }

    /* Freed a worker-queue slot: let any parked data-I/O requests resume. */
    diskfs_io_resume_waiters(self);
} /* diskfs_block_load_complete */


/*
 * Fetch the block backing a b+tree node for op.  On a resident, valid block
 * the block is returned immediately.  Otherwise the op is parked on the
 * block's waiter list (a read is issued if it is not already in flight) and
 * NULL is returned; the op's driver will be re-entered once the block loads.
 */
/* Pin a block for op's descent (so it can't be evicted while in use) and
 * record it for release at completion.  Caller holds the shard lock. */
static inline void
diskfs_bt_op_pin(
    struct diskfs_bt_op       *op,
    struct diskfs_block_shard *shard,
    struct diskfs_block       *blk)
{
    /* Block-swap: blocks stay on the LRU whether pinned or not.  Move this one
     * to the MRU end since it is now active (recyclers take the unpinned head). */
    diskfs_block_lru_unlink(shard, blk);
    diskfs_block_lru_push_tail(shard, blk);
    if (__atomic_add_fetch(&blk->pin_count, 1, __ATOMIC_ACQ_REL) == 1) {
        __atomic_add_fetch(&shard->pinned, 1, __ATOMIC_RELAXED);
    }
    chimera_diskfs_abort_if(op->npins >= (int) (sizeof(op->pins) / sizeof(op->pins[0])),
                            "b+tree op pin list overflow");
    op->pins[op->npins++] = blk;
} /* diskfs_bt_op_pin */


struct diskfs_block *
diskfs_bt_block_get(
    struct diskfs_bt_op *op,
    uint32_t             device_id,
    uint64_t             device_offset)
{
    struct diskfs_thread      *thread = op->thread;
    struct diskfs_block_cache *cache  = thread->shared->block_cache;
    struct diskfs_block_shard *shard  = diskfs_block_shard(cache, device_id, device_offset);
    uint32_t                   bucket = diskfs_block_bucket(device_id, device_offset);
    struct diskfs_block       *blk;
    struct diskfs_block_load  *ld;
    int                        issue = 0;

    pthread_mutex_lock(&shard->lock);
    blk = diskfs_block_lookup_locked(shard, bucket, device_id, device_offset);
    if (blk && blk->state != DISKFS_BLOCK_LOADING) {
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_HIT);
        diskfs_bt_op_pin(op, shard, blk);
        pthread_mutex_unlock(&shard->lock);
        return blk;
    }

    if (!blk) {
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_MISS);
        blk = diskfs_block_recycle(thread, shard);
        if (!blk) {
            /* No clean victim at the LRU head: defer-retry the descent (the
             * lock-free pusher won't wake us; we re-run via the resume queue). */
            diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_WAIT);
            op->suspended = 1;
            diskfs_bt_op_park_account(op);
            return diskfs_block_defer_retry(thread, shard, diskfs_bt_op_resume_cb, op);
        }
        blk->device_id     = device_id;
        blk->device_offset = device_offset;
        __atomic_store_n(&blk->state, DISKFS_BLOCK_LOADING, __ATOMIC_RELEASE);
        blk->seq               = 0;
        blk->wait_head         = NULL;
        blk->wait_tail         = NULL;
        blk->hash_next         = shard->buckets[bucket];
        shard->buckets[bucket] = blk;
        issue                  = 1;
    } else {
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_WAIT);
    }

    /* Park this op on the block's waiter list (via a continuation that
     * re-enters its driver); it can no longer complete inline, so its result
     * will be delivered via the callback. */
    {
        struct diskfs_block_waiter *w = diskfs_block_waiter_alloc(thread);

        op->suspended = 1;
        diskfs_bt_op_park_account(op);   /* diag: parked at op->phase on a block fault */
        w->thread = op->thread;
        w->resume = diskfs_bt_op_resume_cb;
        w->arg    = op;
        if (blk->wait_tail) {
            blk->wait_tail->next = w;
        } else {
            blk->wait_head = w;
        }
        blk->wait_tail = w;
    }
    pthread_mutex_unlock(&shard->lock);

    if (issue) {
        diskfs_block_assert_iov(thread, blk);
        ld         = malloc(sizeof(*ld));
        ld->blk    = blk;
        ld->thread = thread;
        diskfs_pending_io_add(thread, 1);
        diskfs_metric_block_io(thread, DISKFS_METRIC_IO_READ,
                               DISKFS_METRIC_IO_BTREE, DISKFS_BLOCK_SIZE);
        diskfs_metric_block_io_device(thread, device_id, DISKFS_METRIC_IO_READ,
                                      DISKFS_METRIC_IO_BTREE, DISKFS_BLOCK_SIZE);
        evpl_block_read(thread->evpl, thread->queue[device_id], &blk->iov, 1,
                        device_offset, diskfs_block_load_complete, ld);
    }

    return NULL;
} /* diskfs_bt_block_get */


/*
 * Async, COW-aware block claim for non-bt_op callers.  Behaves like
 * diskfs_block_claim (returns the block pinned; is_new starts from a zeroed
 * buffer; a resident LOGGED buffer is COW-forked) but never reads
 * synchronously: on a miss (or a read already in flight) it parks
 * resume(thread, arg) on the block and returns NULL, and the read is driven on
 * the async evpl_block path.  The caller's continuation re-invokes this once
 * the block has loaded, when it returns the now-resident block inline.
 */
static struct diskfs_block *
diskfs_block_claim_async(
    struct diskfs_thread *thread,
    uint32_t device_id,
    uint64_t device_offset,
    int is_new,
    void ( *resume )(struct diskfs_thread *, void *),
    void *arg)
{
    struct diskfs_block_cache  *cache  = thread->shared->block_cache;
    uint64_t                    hash   = diskfs_block_hash(device_id, device_offset);
    uint32_t                    sidx   = hash & DISKFS_BLOCK_CACHE_SHARD_MASK;
    uint32_t                    bucket = (hash >> 8) & DISKFS_BLOCK_CACHE_BUCKET_MASK;
    struct diskfs_block_shard  *shard  = &cache->shards[sidx];
    struct diskfs_block        *blk;
    struct diskfs_block_waiter *w;
    struct diskfs_block_load   *ld;
    int                         issue = 0;

    pthread_mutex_lock(&shard->lock);

    diskfs_block_drain_returned_locked(shard);
    diskfs_block_drain_clean_locked(shard);
    blk = diskfs_block_lookup_locked(shard, bucket, device_id, device_offset);

    if (blk && __atomic_load_n(&blk->state, __ATOMIC_ACQUIRE) == DISKFS_BLOCK_LOADING) {
        /* A read is already in flight: park and resume when it lands. */
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_WAIT);
        w         = diskfs_block_waiter_alloc(thread);
        w->thread = thread;
        w->resume = resume;
        w->arg    = arg;
        if (blk->wait_tail) {
            blk->wait_tail->next = w;
        } else {
            blk->wait_head = w;
        }
        blk->wait_tail = w;
        pthread_mutex_unlock(&shard->lock);
        return NULL;
    }

    if (!blk) {
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_MISS);
        blk = diskfs_block_recycle(thread, shard);
        if (!blk) {
            /* Block-swap park: LRU head still referenced -- defer-retry (the
             * lock-free pusher won't wake us; re-run via the resume queue). */
            diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_WAIT);
            return diskfs_block_defer_retry(thread, shard, resume, arg);
        }
        blk->device_id     = device_id;
        blk->device_offset = device_offset;
        __atomic_store_n(&blk->seq, 0, __ATOMIC_RELEASE);
        blk->wait_head = NULL;
        blk->wait_tail = NULL;

        if (is_new) {
            diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_NEW);
            __atomic_store_n(&blk->state, DISKFS_BLOCK_CLEAN, __ATOMIC_RELEASE);
            diskfs_block_assert_iov(thread, blk);
            memset(blk->iov.data, 0, DISKFS_BLOCK_SIZE);
            blk->hash_next         = shard->buckets[bucket];
            shard->buckets[bucket] = blk;
            if (__atomic_add_fetch(&blk->pin_count, 1, __ATOMIC_ACQ_REL) == 1) {
                __atomic_add_fetch(&shard->pinned, 1, __ATOMIC_RELAXED);
            }
            pthread_mutex_unlock(&shard->lock);
            return blk;
        }

        /* Miss: publish a LOADING block (recycle left it on the LRU tail; the
         * LOADING state keeps recycle from re-taking it), park, issue the read. */
        __atomic_store_n(&blk->state, DISKFS_BLOCK_LOADING, __ATOMIC_RELEASE);
        blk->hash_next         = shard->buckets[bucket];
        shard->buckets[bucket] = blk;
        w                      = diskfs_block_waiter_alloc(thread);
        w->thread              = thread;
        w->resume              = resume;
        w->arg                 = arg;
        blk->wait_head         = w;
        blk->wait_tail         = w;
        issue                  = 1;
    } else if (blk->buf->refs > 1) {
        /* CoW (block-swap): the live buffer is still referenced by an un-pushed
         * record, so swap a fresh block in for (dev,off) and retire this one onto
         * the LRU (still pinned, still holding its buffer).  No victim -> park. */
        struct diskfs_block *y = diskfs_block_cow_swap(thread, shard, blk,
                                                       device_id, device_offset,
                                                       bucket);
        if (!y) {
            diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_WAIT);
            return diskfs_block_defer_retry(thread, shard, resume, arg);
        }
        blk = y;
    } else {
        /* Resident hit (refs == 1).  Stays on the LRU (block-swap); move it to
         * the MRU end since it is now active. */
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_HIT);
        diskfs_block_lru_unlink(shard, blk);
        diskfs_block_lru_push_tail(shard, blk);
    }

    if (issue) {
        pthread_mutex_unlock(&shard->lock);
        diskfs_block_assert_iov(thread, blk);
        ld         = malloc(sizeof(*ld));
        ld->blk    = blk;
        ld->thread = thread;
        diskfs_pending_io_add(thread, 1);
        diskfs_metric_block_io(thread, DISKFS_METRIC_IO_READ,
                               DISKFS_METRIC_IO_METADATA, DISKFS_BLOCK_SIZE);
        diskfs_metric_block_io_device(thread, device_id, DISKFS_METRIC_IO_READ,
                                      DISKFS_METRIC_IO_METADATA, DISKFS_BLOCK_SIZE);
        evpl_block_read(thread->evpl, thread->queue[device_id], &blk->iov, 1,
                        device_offset, diskfs_block_load_complete, ld);
        return NULL;
    }

    if (__atomic_add_fetch(&blk->pin_count, 1, __ATOMIC_ACQ_REL) == 1) {
        __atomic_add_fetch(&shard->pinned, 1, __ATOMIC_RELAXED);
    }
    pthread_mutex_unlock(&shard->lock);
    return blk;
} /* diskfs_block_claim_async */


void
diskfs_inode_finish_write_pin(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode,
    diskfs_inode_cb_t     cb,
    void                 *private_data,
    int                   is_new)
{
    struct diskfs_block    *blk;
    struct diskfs_pin_cont *c;
    uint32_t                device_id;
    uint64_t                device_offset;

    if (inode->block) {
        cb(inode, CHIMERA_VFS_OK, private_data);
        return;
    }

    device_offset = sm_inum_to_device_offset(thread->shared->space_map,
                                             inode->inum, &device_id);

    c               = malloc(sizeof(*c));
    c->thread       = thread;
    c->txn          = txn;
    c->inode        = inode;
    c->cb           = cb;
    c->private_data = private_data;
    c->is_new       = is_new;

    /* Always async so the pin honors the per-shard pin threshold: when a shard
     * is at its pin cap the claim parks on the buffer-wait queue and resumes
     * once the tail-pusher drains a pin -- this is the backpressure that bounds
     * the in-memory record/pin backlog under a create flood (e.g. SPEC INIT).
     * A fresh inode (is_new) gets a zeroed buffer with no read-back. */
    blk = diskfs_block_claim_async(thread, device_id, device_offset, is_new,
                                   diskfs_pin_cont_resume, c);
    if (!blk) {
        return;     /* suspended; diskfs_pin_cont_resume re-runs when a pin frees */
    }

    free(c);
    inode->block = blk;
    diskfs_txn_add_block(txn, blk);
    if (is_new) {
        /* Initialize the embedded b+tree root (empty leaf) in the new block. */
        diskfs_bt_node_init(blk->iov.data, DISKFS_BT_ROOT_BASE,
                            DISKFS_BT_ROOT_CAP, 0);
    }
    cb(inode, CHIMERA_VFS_OK, private_data);
} /* diskfs_inode_finish_write_pin */


/*
 * Link an inode's already-resident home (root) block as its txn home block and
 * take a txn-lifetime pin on it.  Used by the b+tree descent when it faults the
 * root for a structural modify (insert/remove): the synchronous modify derefs
 * inode->block, and flush_inodes serializes the dinode into it at commit, so the
 * block must outlive the descent op's own (op->pins[]) pin, which is released at
 * op completion -- before commit.  The block is already pin-held by the descent
 * (diskfs_bt_op_pin), so this increment races no recycle; the inode is held
 * under the write lock, so inode->block is owned by this thread.
 */
void
diskfs_inode_link_root(
    struct diskfs_txn   *txn,
    struct diskfs_inode *inode,
    struct diskfs_block *blk)
{
    struct diskfs_block_shard *shard =
        diskfs_block_shard(txn->thread->shared->block_cache,
                           blk->device_id, blk->device_offset);

    if (__atomic_add_fetch(&blk->pin_count, 1, __ATOMIC_ACQ_REL) == 1) {
        __atomic_add_fetch(&shard->pinned, 1, __ATOMIC_RELAXED);
    }
    diskfs_txn_add_block(txn, blk);
    inode->block = blk;
} /* diskfs_inode_link_root */


static void
diskfs_pin_cont_resume(
    struct diskfs_thread *thread,
    void                 *arg)
{
    struct diskfs_pin_cont c = *(struct diskfs_pin_cont *) arg;

    (void) thread;
    free(arg);
    diskfs_inode_finish_write_pin(c.thread, c.txn, c.inode, c.cb,
                                  c.private_data, c.is_new);
} /* diskfs_pin_cont_resume */


/*
 * Ensure this (write-locked) inode's home block is resident and pinned, and
 * attached to the transaction.  Idempotent per inode: the inode caches its
 * block pointer and we only claim/attach once.
 */
void
diskfs_txn_pin_inode_block(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode,
    int                   is_new)
{
    uint32_t device_id;
    uint64_t device_offset;

    if (inode->block) {
        return;     /* already pinned by this txn */
    }

    device_offset = sm_inum_to_device_offset(thread->shared->space_map,
                                             inode->inum, &device_id);

    inode->block = diskfs_block_claim(thread, device_id, device_offset, is_new);
    diskfs_txn_add_block(txn, inode->block);

    if (is_new) {
        /* Initialize the embedded b+tree root (empty leaf) in the new
         * inode block. */
        diskfs_bt_node_init(inode->block->iov.data, DISKFS_BT_ROOT_BASE,
                            DISKFS_BT_ROOT_CAP, 0);
    }
} /* diskfs_txn_pin_inode_block */


/*
 * Inverse of the write-lock home-block pin, for the deferred-mtime path: detach
 * the inode's home block from the txn and unpin it so the txn commits with no
 * durable block (the mtime bump is held in memory and logged later by the
 * coalescing flusher).  The block stays in the cache (CLEAN/evictable) and is
 * re-claimed when something next needs it.  Only valid when the block is clean
 * (the deferred path makes no on-block change), which an in-place overwrite of
 * an already-written extent guarantees.
 */
void
diskfs_txn_drop_inode_block(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode)
{
    struct diskfs_txn_block **pp, *tb;

    if (!inode->block) {
        return;
    }
    for (pp = &txn->blocks; *pp; pp = &(*pp)->next) {
        if ((*pp)->block == inode->block) {
            tb  = *pp;
            *pp = tb->next;
            free(tb);
            break;
        }
    }
    diskfs_block_release(thread, inode->block);
    inode->block = NULL;
} /* diskfs_txn_drop_inode_block */


/* Serialize an inode's durable attributes into the front of its block. */
void
diskfs_inode_flush(struct diskfs_inode *inode)
{
    struct diskfs_dinode *di;

    if (!inode->block) {
        return;
    }

    di = inode->block->iov.data;

    di->inum           = inode->inum;
    di->gen            = inode->gen;
    di->mode           = inode->mode;
    di->nlink          = inode->nlink;
    di->uid            = inode->uid;
    di->gid            = inode->gid;
    di->rdev           = inode->rdev;
    di->size           = inode->size;
    di->space_used     = inode->space_used;
    di->atime_sec      = inode->atime_sec;
    di->mtime_sec      = inode->mtime_sec;
    di->ctime_sec      = inode->ctime_sec;
    di->btime_sec      = inode->btime_sec;
    di->atime_nsec     = inode->atime_nsec;
    di->mtime_nsec     = inode->mtime_nsec;
    di->ctime_nsec     = inode->ctime_nsec;
    di->btime_nsec     = inode->btime_nsec;
    di->dos_attributes = inode->dos_attributes;
    di->change         = inode->change;
    di->ea_size        = inode->ea_size;
    di->ea_count       = inode->ea_count;
    if (S_ISDIR(inode->mode)) {
        di->parent_inum = inode->parent_inum;
        di->parent_gen  = inode->parent_gen;
    }

    inode->block->state = DISKFS_BLOCK_DIRTY;
} /* diskfs_inode_flush */


/*
 * At commit, serialize every write-locked inode into its block buffer.
 * Runs on the worker thread (it owns the live inodes under write lock).
 */
void
diskfs_txn_flush_inodes(struct diskfs_txn *txn)
{
    int i;

    for (i = 0; i < txn->num_inodes; i++) {
        if (txn->inodes[i].mode == DISKFS_INODE_LOCK_WRITE) {
            diskfs_inode_flush(txn->inodes[i].inode);
        }
    }
} /* diskfs_txn_flush_inodes */


/*
 * Unpin all blocks held by this txn, transitioning them to new_state -- except
 * a block a later writer has already COW-forked away from this txn's snapshot
 * (snap_buf != live buf), where only this txn's pin is dropped and the live
 * block's state is left to its new owner.  Also clears each write-locked
 * inode's cached block pointer (valid only while the txn holds the block
 * pinned).  Used at abort (worker, inodes still locked) and at durable commit
 * (intent-log thread).  On the commit path the inodes were already released in
 * diskfs_il_write_redo, so num_inodes is 0 and the clearing loop is a no-op
 * here (the clear happened there, before the locks dropped); only the abort
 * path still does the clearing.
 */
void
diskfs_txn_unpin_blocks(struct diskfs_txn *txn)
{
    struct diskfs_thread    *thread = txn->thread;
    struct diskfs_txn_block *tb     = txn->blocks;
    struct diskfs_txn_block *n;
    int                      i;

    for (i = 0; i < txn->num_inodes; i++) {
        if (txn->inodes[i].mode == DISKFS_INODE_LOCK_WRITE) {
            txn->inodes[i].inode->block = NULL;
        }
    }

    txn->blocks = NULL;
    while (tb) {
        n = tb->next;
        /* Abort: no record was logged, so no tail-push will ever release this
         * txn's claim pin -- drop it here.  The block is home (nothing of ours
         * went un-home), so unpin marks it CLEAN once fully unpinned. */
        diskfs_block_unpin(thread, tb->block, DISKFS_BLOCK_CLEAN);
        free(tb);
        tb = n;
    }
} /* diskfs_txn_unpin_blocks */


/*
 * Commit-path counterpart of diskfs_txn_unpin_blocks, run by the intent-log
 * thread once a record is durable.  It releases the inode->block links and the
 * per-block txn structs but does NOT drop the block pins: each block's claim
 * pin is now held until its redo record is pushed home and trimmed, where the
 * push thread drops it (diskfs_il_free_record -> diskfs_push_unpin_block).  So
 * the IL thread touches none of the block's reference/state fields.
 */
void
diskfs_txn_retire_blocks(struct diskfs_txn *txn)
{
    struct diskfs_txn_block *tb = txn->blocks;
    struct diskfs_txn_block *n;
    int                      i;

    for (i = 0; i < txn->num_inodes; i++) {
        if (txn->inodes[i].mode == DISKFS_INODE_LOCK_WRITE) {
            txn->inodes[i].inode->block = NULL;
        }
    }

    txn->blocks = NULL;
    while (tb) {
        n = tb->next;
        free(tb);
        tb = n;
    }
} /* diskfs_txn_retire_blocks */
