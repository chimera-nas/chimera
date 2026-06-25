// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Transactions and the intent log: txn lifecycle (inode slots, block pins,
 * pending frees), commit (dirty-inode serialization, block snapshots, redo
 * records), the SPSC submit/completion rings, the log-writer thread and the
 * tail-pusher that writes logged blocks home and trims the log.
 */

#include "diskfs_internal.h"

/* Debug: live handle to the intent log + a per-queue CQ dumper (in libevpl),
 * so a wedge can be inspected from gdb with `call dbg_dump_push()`. */
struct diskfs_intent_log *g_dbg_il;
extern void evpl_vfio_queue_dump(
    struct evpl_block_queue *bq);

SYMBOL_EXPORT void
dbg_dump_push(void)
{
    struct diskfs_intent_log *il = g_dbg_il;
    struct diskfs_shared     *shared;
    int                       i;

    if (!il) {
        chimera_diskfs_error("DBG-PUSH: no il");
        return;
    }

    shared = container_of(il, struct diskfs_shared, intent_log);

    chimera_diskfs_error(
        "DBG-PUSH push_outstanding=%d ready=%s push_head=%p redo_inflight=%d log_head=%lu log_tail=%lu used=%lu num_devices=%d",
        il->push_outstanding, il->ready_head ? "NONEMPTY" : "empty",
        (void *) il->push_head, il->redo_inflight,
        (unsigned long) __atomic_load_n(&il->log_head, __ATOMIC_RELAXED),
        (unsigned long) __atomic_load_n(&il->log_tail, __ATOMIC_RELAXED),
        (unsigned long) (__atomic_load_n(&il->log_head, __ATOMIC_RELAXED) -
                         __atomic_load_n(&il->log_tail, __ATOMIC_RELAXED)),
        shared->num_devices);

    if (il->log_queue) {
        evpl_vfio_queue_dump(il->log_queue);    /* redo / commit queue */
    }
    for (i = 0; i < shared->num_devices; i++) {
        if (il->home_queue[i]) {
            evpl_vfio_queue_dump(il->home_queue[i]);
        }
    }
} /* dbg_dump_push */

/* Forward declarations (definitions below, in call-graph order) */

static uint64_t
diskfs_il_contig_free(
    struct diskfs_intent_log *il);

static int
diskfs_il_fits(
    struct diskfs_intent_log *il,
    uint64_t                  reclen);

static uint64_t
diskfs_il_place(
    struct diskfs_intent_log *il,
    uint64_t                  reclen);

static inline uint32_t
diskfs_il_pow2(
    uint32_t v);

static inline uint32_t
diskfs_il_home_hash(
    uint32_t device_id,
    uint64_t device_offset,
    uint32_t mask);

static struct diskfs_pending *
diskfs_pending_lookup(
    struct diskfs_intent_log *il,
    uint32_t                  dev,
    uint64_t                  off);

static struct diskfs_pending *
diskfs_pending_alloc(
    struct diskfs_intent_log *il);

static void
diskfs_pending_insert(
    struct diskfs_intent_log *il,
    struct diskfs_pending    *p);

static void
diskfs_pending_remove(
    struct diskfs_intent_log *il,
    struct diskfs_pending    *p);

static void
diskfs_ready_push(
    struct diskfs_intent_log *il,
    struct diskfs_pending    *p);

static struct diskfs_pending *
diskfs_ready_pop(
    struct diskfs_intent_log *il);

static void
diskfs_il_free_record(
    struct diskfs_intent_log *il,
    struct diskfs_il_record  *rec);

static void
diskfs_push_fold_record(
    struct diskfs_intent_log *il,
    struct diskfs_il_record  *rec);

static int
diskfs_push_record_covered(
    struct diskfs_intent_log *il,
    struct diskfs_il_record  *rec);

static void
diskfs_push_trim(
    struct diskfs_intent_log *il);

static void
diskfs_push_issue(
    struct diskfs_intent_log *il);

static void
diskfs_push_block_cb(
    struct evpl *evpl,
    int          status,
    void        *private_data);

static void
diskfs_il_push_doorbell_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell);

static void
diskfs_il_write_redo(
    struct diskfs_intent_log *il,
    struct diskfs_redo_entry *entries,
    uint32_t                  num_entries,
    uint32_t                  nblocks,
    uint32_t                  num_deltas,
    uint64_t                  end_txn_id);

static uint32_t
diskfs_il_txn_blocks(
    struct diskfs_txn *txn);

static uint64_t
diskfs_il_blocks_reclen(
    uint32_t nblocks,
    uint32_t num_deltas);

static uint64_t
diskfs_il_txn_reclen(
    struct diskfs_txn *txn);

static void
diskfs_intent_log_drain_pending(
    struct diskfs_intent_log *il);

static void
diskfs_il_service_registrations(
    struct diskfs_intent_log *il);

static int
diskfs_il_process_all(
    struct diskfs_intent_log *il);

static int
diskfs_il_has_sq_work(
    struct diskfs_intent_log *il);

static void
diskfs_intent_log_wake_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell);

static void
diskfs_il_sq_poll(
    struct evpl *evpl,
    void        *private_data);

static void
diskfs_il_poll_enter(
    struct evpl *evpl,
    void        *private_data);

static void
diskfs_iq_resume_commit_waiters(
    struct diskfs_thread *thread);


void
diskfs_txn_request_complete_cb(
    struct diskfs_txn *txn,
    int                status,
    void              *private_data)
{
    struct chimera_vfs_request *request = private_data;

    if (txn) {
        txn->dbg_stage = 6;     /* COMPLETE (durable, locks dropped) */
    }
    request->wait_reason = NULL;
    if (status != 0 && request->status == CHIMERA_VFS_OK) {
        request->status = status;
    }
    request->complete(request);
} /* diskfs_txn_request_complete_cb */


/* ------------------------------------------------------------------ */
/* Tail-pusher: write logged blocks to final locations + trim the log  */
/* ------------------------------------------------------------------ */

/*
 * Largest contiguous run available for one record (records never wrap the end
 * of the log region; a short run at the end is simply left unused until the
 * tail laps it).  The log is empty exactly when no record is pending.
 */
/* Commit thread owns log_head; the push thread advances log_tail (trim) and
 * the commit thread reads it here to check space.  head == tail means empty
 * (place() always leaves the wrap gap, so the ring never reads full as empty). */
static uint64_t
diskfs_il_contig_free(struct diskfs_intent_log *il)
{
    uint64_t start = SM_INTENT_LOG_OFFSET;
    uint64_t end   = SM_INTENT_LOG_OFFSET + il->intent_log_size;
    uint64_t head  = il->log_head;     /* commit-owned */
    uint64_t tail  = __atomic_load_n(&il->log_tail, __ATOMIC_ACQUIRE);

    if (head == tail) {
        return il->intent_log_size;     /* empty */
    }
    if (head >= tail) {
        uint64_t run_end   = end - head;
        uint64_t run_start = tail - start;

        return run_end > run_start ? run_end : run_start;
    }
    return tail - head;
} /* diskfs_il_contig_free */


static int
diskfs_il_fits(
    struct diskfs_intent_log *il,
    uint64_t                  reclen)
{
    return diskfs_il_contig_free(il) >= reclen;
} /* diskfs_il_fits */


/* Choose the offset for a record of `reclen` bytes and advance log_head. */
static uint64_t
diskfs_il_place(
    struct diskfs_intent_log *il,
    uint64_t                  reclen)
{
    uint64_t end  = SM_INTENT_LOG_OFFSET + il->intent_log_size;
    uint64_t head = il->log_head;
    uint64_t offset;

    if (head + reclen > end) {
        head = SM_INTENT_LOG_OFFSET;     /* wrap; tail of region unused */
    }
    offset = head;
    __atomic_store_n(&il->log_head, head + reclen, __ATOMIC_RELEASE);
    /* The record now occupies log space until the trim point passes it. */
    __atomic_add_fetch(&il->live_records, 1, __ATOMIC_RELEASE);
    return offset;
} /* diskfs_il_place */


/* ================================================================== */
/* Tail-push thread: per-block coalescing home writes + in-order trim  */
/* ================================================================== */

static inline uint32_t
diskfs_il_pow2(uint32_t v)
{
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    return v + 1;
} /* diskfs_il_pow2 */


/* Hash a home location to a pending-map bucket. */
static inline uint32_t
diskfs_il_home_hash(
    uint32_t device_id,
    uint64_t device_offset,
    uint32_t mask)
{
    uint64_t k = (device_offset / DISKFS_BLOCK_SIZE) ^ ((uint64_t) device_id << 48);

    k *= 0x9E3779B97F4A7C15ULL;
    return (uint32_t) (k >> 32) & mask;
} /* diskfs_il_home_hash */


static struct diskfs_pending *
diskfs_pending_lookup(
    struct diskfs_intent_log *il,
    uint32_t                  dev,
    uint64_t                  off)
{
    struct diskfs_pending *p = il->phash[diskfs_il_home_hash(dev, off, il->phash_mask)];

    for (; p; p = p->hnext) {
        if (p->device_id == dev && p->device_offset == off) {
            return p;
        }
    }
    return NULL;
} /* diskfs_pending_lookup */


static struct diskfs_pending *
diskfs_pending_alloc(struct diskfs_intent_log *il)
{
    struct diskfs_pending *p = il->pfree;

    if (p) {
        il->pfree = p->rnext;
    } else {
        p = malloc(sizeof(*p));
    }
    p->il       = il;
    p->inflight = 0;
    p->on_ready = 0;
    return p;
} /* diskfs_pending_alloc */


static void
diskfs_pending_insert(
    struct diskfs_intent_log *il,
    struct diskfs_pending    *p)
{
    uint32_t b = diskfs_il_home_hash(p->device_id, p->device_offset, il->phash_mask);

    p->hnext     = il->phash[b];
    il->phash[b] = p;
} /* diskfs_pending_insert */


static void
diskfs_pending_remove(
    struct diskfs_intent_log *il,
    struct diskfs_pending    *p)
{
    uint32_t                b  = diskfs_il_home_hash(p->device_id, p->device_offset, il->phash_mask);
    struct diskfs_pending **pp = &il->phash[b];

    while (*pp && *pp != p) {
        pp = &(*pp)->hnext;
    }
    if (*pp) {
        *pp = p->hnext;
    }
    p->rnext  = il->pfree;     /* recycle onto the free list */
    il->pfree = p;
} /* diskfs_pending_remove */


static void
diskfs_ready_push(
    struct diskfs_intent_log *il,
    struct diskfs_pending    *p)
{
    if (p->on_ready) {
        return;
    }
    p->on_ready = 1;
    p->rnext    = NULL;
    if (il->ready_tail) {
        il->ready_tail->rnext = p;
    } else {
        il->ready_head = p;
    }
    il->ready_tail = p;
} /* diskfs_ready_push */


static struct diskfs_pending *
diskfs_ready_pop(struct diskfs_intent_log *il)
{
    struct diskfs_pending *p = il->ready_head;

    if (!p) {
        return NULL;
    }
    il->ready_head = p->rnext;
    if (!il->ready_head) {
        il->ready_tail = NULL;
    }
    p->on_ready = 0;
    p->rnext    = NULL;
    return p;
} /* diskfs_ready_pop */


static void diskfs_push_unpin_block(
    struct diskfs_intent_log *il,
    struct diskfs_block      *blk);

/*
 * Release a record's per-block resources: drop the submitting txn's claim pin
 * on each block (diskfs_push_unpin_block -- the "decrement on flush/skip") and
 * release the record's snapshot ref on each block's buffer (buf->refs).  This is
 * the moment a block stops owing this record an image home, so its pin can fall
 * to zero (-> CLEAN) and its un-home snapshot buffer can return to the pool.
 *
 * Called as soon as the record is COVERED (every block home or superseded) AND
 * inflight-drained (no in-flight home write still reads its images) -- NOT
 * deferred to the in-order log trim.  Deferring was the bug: a hot directory
 * block logged by R1..RN kept N pins + N snapshot buffers alive until all N
 * records trimmed in FIFO order, so one uncovered head record pinned the whole
 * tail of the cache (measured: ~76% of buffers were detached snapshots riding
 * un-trimmed-but-covered records).  Idempotent: the in-order trim path calls it
 * again on free, a no-op once discharged.
 *
 * Reads dev/off from iovs[0]'s header region, so it must run before iovs[0] is
 * released (only diskfs_il_free_record does that, at trim).
 */
static void
diskfs_il_discharge_record(
    struct diskfs_intent_log *il,
    struct diskfs_il_record  *rec)
{
    uint32_t i;

    if (rec->discharged) {
        return;
    }
    rec->discharged = 1;

    for (i = 0; i < rec->num_blocks; i++) {
        /* Drop the snapshot buffer ref BEFORE the obligation pin so that a
         * recycler which observes pin_count == 0 also sees the buffer's refs
         * back to 1 (block-only) -- block-swap keeps a block and its buffer
         * paired, so a recycled block reuses its own buffer.  Use the carried
         * block pointer: after a CoW the block at (dev,off) is a different
         * (live) block; the obligation is on THIS retired one. */
        diskfs_block_buf_release(rec->block_bufs[i]);
        diskfs_push_unpin_block(il, rec->blocks[i]);
    }
} /* diskfs_il_discharge_record */


/* A record is COVERED (uncovered == 0) and inflight-drained: release its
 * resources now rather than waiting for the in-order trim. */
static inline void
diskfs_il_try_discharge(
    struct diskfs_intent_log *il,
    struct diskfs_il_record  *rec)
{
    if (rec->uncovered == 0 && rec->inflight_refs == 0) {
        diskfs_il_discharge_record(il, rec);
    }
} /* diskfs_il_try_discharge */


/* ---------------------------------------------------------------------------
* Stage A: commit-hot-path memory recycling.
*
* The record builder allocated six heap blocks per redo record (the record and
* its three block arrays, the completion ctx and its entry array).  At a few
* hundred thousand commits/s that malloc/free traffic was a large slice of the
* single commit thread.  Recycle the structs -- and their variable-length
* arrays, grown on demand and never shrunk -- through lock-free pools so the
* steady-state hot path never calls malloc().  Each pool is single-consumer
* (the commit thread, which allocates in diskfs_il_write_redo) and single-
* producer (the thread that frees the object: the push thread for records, the
* apply thread for ctxs), so the Treiber pop is ABA-free.
* ------------------------------------------------------------------------- */

static struct diskfs_il_record *
diskfs_il_rec_alloc(
    struct diskfs_intent_log *il,
    uint32_t                  nblocks)
{
    struct diskfs_il_record *rec;

    rec = __atomic_load_n(&il->rec_pool, __ATOMIC_ACQUIRE);
    while (rec &&
           !__atomic_compare_exchange_n(&il->rec_pool, &rec, rec->recycle_next,
                                        0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
    }
    if (!rec) {
        rec = malloc(sizeof(*rec));
        chimera_diskfs_abort_if(!rec, "out of memory allocating IL record");
        rec->blocks_cap = 0;
        rec->iovs       = NULL;
        rec->block_bufs = NULL;
        rec->blocks     = NULL;
    }

    /* Grow the arrays only when this record needs more blocks than any prior
     * use of this struct -- the common (batched) case reuses them untouched.
     * A fresh struct (iovs == NULL) must always allocate, even at nblocks == 0:
     * iovs[0] holds the redo header regardless of the block count. */
    if (nblocks > rec->blocks_cap || !rec->iovs) {
        free(rec->iovs);
        free(rec->block_bufs);
        free(rec->blocks);
        rec->iovs       = malloc((1 + nblocks) * sizeof(*rec->iovs));
        rec->block_bufs = malloc(nblocks * sizeof(*rec->block_bufs));
        rec->blocks     = malloc(nblocks * sizeof(*rec->blocks));
        chimera_diskfs_abort_if(!rec->iovs ||
                                (nblocks && (!rec->block_bufs || !rec->blocks)),
                                "out of memory growing IL record arrays");
        rec->blocks_cap = nblocks;
    }
    return rec;
} /* diskfs_il_rec_alloc */


static void
diskfs_il_rec_recycle(
    struct diskfs_intent_log *il,
    struct diskfs_il_record  *rec)
{
    struct diskfs_il_record *head;

    head = __atomic_load_n(&il->rec_pool, __ATOMIC_ACQUIRE);
    do {
        rec->recycle_next = head;
    } while (!__atomic_compare_exchange_n(&il->rec_pool, &head, rec,
                                          0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE));
} /* diskfs_il_rec_recycle */


static struct diskfs_redo_ctx *
diskfs_il_ctx_alloc(
    struct diskfs_intent_log *il,
    uint32_t                  num_entries)
{
    struct diskfs_redo_ctx *ctx;

    ctx = __atomic_load_n(&il->ctx_pool, __ATOMIC_ACQUIRE);
    while (ctx &&
           !__atomic_compare_exchange_n(&il->ctx_pool, &ctx, ctx->free_next,
                                        0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE)) {
    }
    if (!ctx) {
        ctx = malloc(sizeof(*ctx));
        chimera_diskfs_abort_if(!ctx, "out of memory allocating IL completion ctx");
        ctx->entries_cap = 0;
        ctx->entries     = NULL;
    }
    if (num_entries > ctx->entries_cap) {
        free(ctx->entries);
        ctx->entries = malloc(num_entries * sizeof(*ctx->entries));
        chimera_diskfs_abort_if(!ctx->entries,
                                "out of memory growing IL ctx entries");
        ctx->entries_cap = num_entries;
    }
    return ctx;
} /* diskfs_il_ctx_alloc */


static void
diskfs_il_ctx_recycle(
    struct diskfs_intent_log *il,
    struct diskfs_redo_ctx   *ctx)
{
    struct diskfs_redo_ctx *head;

    head = __atomic_load_n(&il->ctx_pool, __ATOMIC_ACQUIRE);
    do {
        ctx->free_next = head;
    } while (!__atomic_compare_exchange_n(&il->ctx_pool, &head, ctx,
                                          0, __ATOMIC_ACQ_REL, __ATOMIC_ACQUIRE));
} /* diskfs_il_ctx_recycle */


static void
diskfs_il_free_record(
    struct diskfs_intent_log *il,
    struct diskfs_il_record  *rec)
{
    /* Resources are normally already released at coverage; ensure it (no-op if
     * so) before we free the record. */
    diskfs_il_discharge_record(il, rec);

    /* iovs[0] is the materialized header (a real SHARED buffer) -- release it.
     * iovs[1..] are non-owning GLOBAL slices of block-cache buffers (cloned at
     * zero cost in the IL builder); they own nothing of the backing buffer, so
     * release_internal leaves it untouched (its GLOBAL ref-release is inert, and
     * the buffer is freed only at cache teardown) -- but it still frees each
     * clone's trace canary, which evpl_iovec_clone allocates unconditionally. */
    evpl_iovec_release(il->push_evpl, &rec->iovs[0]);
    if (rec->niov > 1) {
        evpl_iovecs_release_internal(il->push_evpl, &rec->iovs[1], rec->niov - 1);
    }

    /* Stage A: recycle the record + its (capacity-tracked) block arrays for the
     * commit thread to reuse instead of freeing them.  The iovs[1..] canaries
     * are dropped above first, so the pooled iovs array carries none into reuse
     * (the builder re-clones, allocating fresh canaries). */
    diskfs_il_rec_recycle(il, rec);
} /* diskfs_il_free_record */


/*
 * Fold one durable record's blocks into the pending map.  Records are folded in
 * log/seq order, so a later record's image of a block supersedes an earlier
 * one (the coalescing win: a block logged by many queued commits is written
 * home once).  A newly-pending block is queued for issue; a block already in
 * flight is left for its completion to re-issue the newer image.
 */
static void
diskfs_push_fold_record(
    struct diskfs_intent_log *il,
    struct diskfs_il_record  *rec)
{
    char    *p = (char *) rec->iovs[0].data + sizeof(struct diskfs_redo_header);
    uint32_t i;

    for (i = 0; i < rec->num_blocks; i++) {
        struct diskfs_redo_block_header *bh = (struct diskfs_redo_block_header *) p;
        struct diskfs_pending           *e;

        p += sizeof(*bh);

        e = diskfs_pending_lookup(il, bh->device_id, bh->device_offset);
        if (!e) {
            e                = diskfs_pending_alloc(il);
            e->device_id     = bh->device_id;
            e->device_offset = bh->device_offset;
            e->seq           = rec->seq;
            e->iov           = &rec->iovs[1 + i];
            e->owner         = rec;
            rec->uncovered++;            /* R now owns this newly-pending block */
            diskfs_pending_insert(il, e);
            diskfs_ready_push(il, e);
        } else {
            /* Newest image supersedes: ownership passes from the old owner
             * (which now needs one fewer block home) to R. */
            struct diskfs_il_record *old_owner = e->owner;

            old_owner->uncovered--;
            e->seq   = rec->seq;
            e->iov   = &rec->iovs[1 + i];
            e->owner = rec;
            rec->uncovered++;
            if (!e->inflight) {
                diskfs_ready_push(il, e);
            }
            /* The old owner no longer owes this block home; if that was its last
             * obligation and nothing is in flight for it, release its resources
             * now instead of waiting for the in-order trim. */
            diskfs_il_try_discharge(il, old_owner);
        }
    }
} /* diskfs_push_fold_record */


/*
 * A redo record holding an image of this block has been pushed home and
 * trimmed (free_record): drop the claim pin the submitting txn took for that
 * generation.  pin_count is the block's outstanding-reference count, so when
 * this is the last pin -- no other un-home generation and no active reader --
 * the block's newest content is durably home and idle, hence CLEAN and
 * LRU-reusable.  Under the shard lock so it serializes against
 * diskfs_block_claim re-pinning/re-dirtying the block.  The block is guaranteed
 * resident: it was pinned (>=1) until this very drop, so it can't have been
 * recycled.
 */
static void
diskfs_push_unpin_block(
    struct diskfs_intent_log *il,
    struct diskfs_block      *blk)
{
    struct diskfs_shared      *shared = container_of(il, struct diskfs_shared, intent_log);
    struct diskfs_block_shard *shard  = diskfs_block_shard(shared->block_cache,
                                                           blk->device_id,
                                                           blk->device_offset);

    /*
     * LOCK-FREE discharge (the point of block-swap).  The block is retired but
     * still on the LRU and pinned; recyclers only ever take the LRU head and
     * require pin_count == 0 && CLEAN (re-checked atomically), so the pusher needs
     * NO shard lock here and NO LRU surgery (the block never leaves the LRU).
     * Drop the obligation pin on the carried block pointer (a (dev,off) lookup
     * would find a different live block after a CoW; the struct is in the fixed
     * pool and never freed, so the pointer is always valid).  On the last pin,
     * publish CLEAN and account the 1->0 transition with relaxed atomics.  No
     * wake: a claim parked for want of a victim self-retries via the worker's
     * resume/defer queue.  Keeping this off the shard lock is what stops the push
     * thread from being starved by claim/CoW lock contention under load -- the
     * feedback collapse the locked version suffered.
     *
     * Note: we never read/touch on_lru or the LRU list here -- a concurrent
     * worker may be moving this block to the MRU end under the shard lock
     * (on_lru toggles 0->1), which is fine: the discharge only adjusts the
     * atomic pin_count/state, and the block stays on the LRU either way. */
    if (__atomic_sub_fetch(&blk->pin_count, 1, __ATOMIC_ACQ_REL) == 0) {
        __atomic_store_n(&blk->state, DISKFS_BLOCK_CLEAN, __ATOMIC_RELEASE);
        __atomic_sub_fetch(&shard->pinned, 1, __ATOMIC_RELAXED);
    }
} /* diskfs_push_unpin_block */


/*
 * Record R is coverable iff every block it logged is either home at its newest
 * seq (absent from pending) or superseded by a later still-logged record
 * (pending seq > R.seq).  pending[X].seq == R.seq means R's own image is still
 * the newest pending and not yet home, so R must wait.
 *
 * rec->uncovered tracks exactly this in O(1): it is the count of blocks for
 * which R is still the newest-pending owner (pending[X].owner == R).  It is
 * raised to num_blocks at fold (R owns every block it just dirtied), and
 * decremented when each such block is either written home (diskfs_push_block_cb)
 * or superseded by a newer record taking ownership (diskfs_push_fold_record).
 * Covered <=> uncovered == 0 -- no per-completion O(num_blocks) re-scan.
 */
static int
diskfs_push_record_covered(
    struct diskfs_intent_log *il,
    struct diskfs_il_record  *rec)
{
    (void) il;
    return rec->uncovered == 0;
} /* diskfs_push_record_covered */


/*
 * Checkpoint frontier gate: a record may be trimmed only once every AG it
 * carries a space delta for has been checkpointed to >= the record's seq (so
 * the delta is folded into a durable on-disk snapshot and losing the record on
 * trim is safe).  The delta section lives in iovs[0] right after the per-block
 * headers (same region diskfs_push_fold_record walks).  Returns 1 if ready;
 * otherwise kicks a background checkpoint for each lagging AG and returns 0
 * (the push thread retries when a checkpoint completes -- see
 * diskfs_il_checkpoint_advanced).  Because a checkpoint stamps ckpt_seq from
 * the durable frontier (near log_head), one checkpoint of a hot AG unblocks all
 * of its in-ring records at once, so this is not per-record work.
 */
static int
diskfs_push_checkpoint_ready(
    struct diskfs_intent_log *il,
    struct diskfs_il_record  *rec)
{
    struct diskfs_shared      *shared = container_of(il, struct diskfs_shared,
                                                     intent_log);
    struct diskfs_redo_header *hdr = (struct diskfs_redo_header *) rec->iovs[0].data;
    char                      *p;
    uint32_t                   i;
    int                        ready = 1;

    /* During shutdown the reclaim workers that service checkpoint CONDENSE jobs
     * are already gone (diskfs_reclaim_destroy runs before the push-thread
     * drain), so kicking a checkpoint here would never complete and the drain
     * would hang forever.  A clean unmount persists the whole space map after
     * the drain (space_map_persist stamped with the final durable_seq) and marks
     * the superblock CLEAN, so no record needs its per-AG snapshot frontier --
     * the log is discarded wholesale and there is nothing to replay.  Trim every
     * covered record unconditionally. */
    if (__atomic_load_n(&il->shutdown, __ATOMIC_ACQUIRE)) {
        return 1;
    }

    if (hdr->num_deltas == 0) {
        return 1;
    }

    p = (char *) rec->iovs[0].data + sizeof(struct diskfs_redo_header) +
        (size_t) rec->num_blocks * sizeof(struct diskfs_redo_block_header);

    for (i = 0; i < hdr->num_deltas; i++) {
        struct diskfs_redo_delta *rd = (struct diskfs_redo_delta *) p;

        p += sizeof(*rd);

        if (space_map_ag_ckpt_seq(shared->space_map, rd->device_id,
                                  rd->ag_index) < rec->seq) {
            ready = 0;
            diskfs_checkpoint_kick(shared, rd->device_id, rd->ag_index);
        }
    }
    return ready;
} /* diskfs_push_checkpoint_ready */


/* The push thread: a background checkpoint advanced some AG's ckpt_seq; retry
 * the trim that may have been blocked on the frontier gate.  Rung from the
 * checkpoint-completion path (any reclaim worker). */
void
diskfs_il_checkpoint_advanced(struct diskfs_intent_log *il)
{
    evpl_ring_doorbell(&il->push_doorbell);
} /* diskfs_il_checkpoint_advanced */


/* Advance the trim point over the contiguous prefix of fully-covered records,
 * freeing each once no in-flight home write still reads its image. */
static void
diskfs_push_trim(struct diskfs_intent_log *il)
{
    int advanced = 0;

    while (il->push_head && diskfs_push_record_covered(il, il->push_head) &&
           diskfs_push_checkpoint_ready(il, il->push_head)) {
        struct diskfs_il_record *rec = il->push_head;

        il->push_head = rec->next;
        if (!il->push_head) {
            il->push_tail = NULL;
        }

        /* Trim point = start of the oldest record we have not retired: the next
         * record in the FIFO, or (FIFO drained) the oldest record the commit
         * thread has handed off but we have not yet consumed.  If neither is
         * known, leave log_tail unchanged (conservative -- the next hand-off
         * advances it). */
        if (il->push_head) {
            __atomic_store_n(&il->log_tail, il->push_head->offset, __ATOMIC_RELEASE);
        } else {
            uint32_t hh = il->handoff_head;
            uint32_t ht = __atomic_load_n(&il->handoff_tail, __ATOMIC_ACQUIRE);
            if (hh != ht) {
                __atomic_store_n(&il->log_tail,
                                 il->handoff[hh & il->handoff_ring_mask]->offset,
                                 __ATOMIC_RELEASE);
            }
        }

        rec->retired = 1;
        if (rec->inflight_refs == 0) {
            diskfs_il_free_record(il, rec);
        }
        /* The trim point is past this record: its log space is reclaimable
         * (home writes read the in-memory image, never the on-disk log).
         * Release-ordered after the log_tail stores above so the commit
         * thread's acquire-load of a zero count also sees the final tail. */
        __atomic_sub_fetch(&il->live_records, 1, __ATOMIC_RELEASE);
        advanced = 1;
    }

    if (advanced) {
        diskfs_il_push_metrics(il);
        /* Freed log space -> resume the commit thread, but only while its
         * doorbell is still live.  The commit thread is destroyed before the
         * push thread (the push thread drains the records it handed off), which
         * closes wake_doorbell's fd; ringing it after that aborts.  commit_alive
         * is cleared before that teardown, and during shutdown the commit thread
         * makes progress by self-pumping, so a skipped wake is harmless. */
        if (__atomic_load_n(&il->commit_alive, __ATOMIC_ACQUIRE)) {
            evpl_ring_doorbell(&il->wake_doorbell);
        }
    }
} /* diskfs_push_trim */


/* Issue ready home writes up to a per-device in-flight cap, one per unique
 * block. */
static void
diskfs_push_issue(struct diskfs_intent_log *il)
{
    for ( ;; ) {
        struct diskfs_pending *e = il->ready_head;   /* peek; don't pop yet */

        if (!e) {
            /* Ran out of ready blocks: the pusher is STARVED (the coverage/fold
             * pipeline isn't feeding it), not in-flight limited. */
            return;
        }

        /* Per-device in-flight cap: if this block's home device already has
         * DISKFS_PUSH_DEV_WATERMARK home writes in flight, the pusher is
         * SATURATED on that device -- park.  A completion on that device
         * re-kicks push_issue (diskfs_push_block_cb).  Peeking the head before
         * popping keeps the gate exact: one block per iteration, so we never
         * commit to a block we then have to park mid-record. */
        if (il->push_outstanding_dev[e->device_id] >= DISKFS_PUSH_DEV_WATERMARK) {
            return;
        }

        e               = diskfs_ready_pop(il);
        e->inflight     = 1;
        e->issued_seq   = e->seq;
        e->issued_owner = e->owner;
        e->owner->inflight_refs++;
        il->push_outstanding++;
        il->push_outstanding_dev[e->device_id]++;
        diskfs_il_push_metrics(il);

        diskfs_metric_il_block_io(il, DISKFS_METRIC_IO_WRITE,
                                  DISKFS_METRIC_IO_TAIL_PUSH, DISKFS_BLOCK_SIZE);
        diskfs_metric_il_block_io_device(il, e->device_id, DISKFS_METRIC_IO_WRITE,
                                         DISKFS_METRIC_IO_TAIL_PUSH, DISKFS_BLOCK_SIZE);
        evpl_block_write(il->push_evpl, il->home_queue[e->device_id], e->iov, 1,
                         e->device_offset, il->sync, diskfs_push_block_cb, e);
    }
} /* diskfs_push_issue */


/* One home write completed (push thread). */
static void
diskfs_push_block_cb(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct diskfs_pending    *e  = private_data;
    struct diskfs_intent_log *il = e->il;
    struct diskfs_il_record  *io = e->issued_owner;

    (void) evpl;
    chimera_diskfs_abort_if(status, "tail-push home write failed: %d", status);

    il->push_outstanding--;
    il->push_outstanding_dev[e->device_id]--;

    /* Release the record the in-flight write read from; free it if it has been
     * retired and no other in-flight write still reads it. */
    if (--io->inflight_refs == 0 && io->retired) {
        diskfs_il_free_record(il, io);
        io = NULL;     /* freed -- do not touch below */
    }

    e->inflight = 0;

    if (e->seq > e->issued_seq) {
        /* A newer image arrived mid-flight: re-issue it (newest lands last).
         * The image just written belonged to the (now superseded) issued owner;
         * its in-flight read just drained, so it may now be fully dischargeable. */
        diskfs_ready_push(il, e);
        if (io) {
            diskfs_il_try_discharge(il, io);
        }
    } else {
        /* Durably home at its newest seq: drop the pending entry.  The owning
         * record needs one fewer block home; once it owes none (and nothing is
         * in flight for it) release its pins + snapshot buffers immediately --
         * the block goes CLEAN now, not at the in-order trim. */
        e->owner->uncovered--;
        diskfs_pending_remove(il, e);
        diskfs_il_try_discharge(il, e->owner);
    }

    diskfs_il_push_metrics(il);
    diskfs_push_issue(il);
    diskfs_push_trim(il);
} /* diskfs_push_block_cb */


/*
 * Push-thread doorbell: drain the hand-off ring into the record FIFO and the
 * pending map, then issue and trim.  Rung by the commit thread after it hands
 * off durable records.
 */
static void
diskfs_il_push_doorbell_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct diskfs_intent_log *il = container_of(doorbell,
                                                struct diskfs_intent_log,
                                                push_doorbell);
    uint32_t                  head = il->handoff_head;
    uint32_t                  tail = __atomic_load_n(&il->handoff_tail, __ATOMIC_ACQUIRE);

    (void) evpl;

    while (head != tail) {
        struct diskfs_il_record *rec = il->handoff[head & il->handoff_ring_mask];

        head++;
        rec->next          = NULL;
        rec->inflight_refs = 0;
        rec->retired       = 0;
        rec->discharged    = 0;
        rec->uncovered     = 0;     /* raised to num_blocks by the fold below */
        if (il->push_tail) {
            il->push_tail->next = rec;
        } else {
            il->push_head = rec;
        }
        il->push_tail = rec;
        diskfs_push_fold_record(il, rec);
    }
    __atomic_store_n(&il->handoff_head, head, __ATOMIC_RELEASE);

    diskfs_push_issue(il);
    diskfs_push_trim(il);
} /* diskfs_il_push_doorbell_cb */


/*
 * Runs on the intent-log thread when a redo record has been written
 * durably.  The transaction's changes are now recoverable, so drop the
 * block pins (-> LOGGED, awaiting tail-push) and the inode locks, then push
 * the completion onto the worker's CQ.
 */
void
diskfs_redo_write_cb(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct diskfs_redo_ctx   *ctx        = private_data;
    struct diskfs_intent_log *il         = ctx->il;
    int                       handed_off = 0;

    (void) evpl;
    chimera_diskfs_abort_if(status, "redo record write failed: %d", status);

    /* One redo block write (one chunk) drained from the log queue.  Resume SQ
     * draining once redo writes bleed back down to the low watermark. */
    if (--il->redo_inflight == DISKFS_COMMIT_LOWAT) {
        evpl_ring_doorbell(&il->wake_doorbell);
    }

    /* One chunk of a possibly multi-chunk journal write landed; the record is
     * durable only when its last chunk completes. */
    if (--ctx->segments > 0) {
        diskfs_il_commit_metrics(il);
        return;
    }

    /* Mark this record done; retire the contiguous completed prefix strictly in
     * submission (== log) order.  Replay stops at the first torn record, so a
     * later record is not recoverable -- nor ACKable -- until every earlier
     * record is durable. */
    il->retire[ctx->retire_idx & DISKFS_RETIRE_RING_MASK].done = 1;

    while (il->retire_head != il->retire_tail &&
           il->retire[il->retire_head & DISKFS_RETIRE_RING_MASK].done) {
        struct diskfs_retire_slot *slot = &il->retire[il->retire_head & DISKFS_RETIRE_RING_MASK];
        struct diskfs_redo_ctx    *rc   = slot->ctx;
        struct diskfs_il_record   *rec  = rc->rec;
        uint32_t                   ht, at;

        /* This record is durably logged; records retire strictly in seq order,
         * so durable_seq advances monotonically.  The per-txn work -- releasing
         * inode->block links + per-block txn structs, applying the committed
         * space-map deltas to the in-memory free map, and ACKing each txn to its
         * worker -- is done off this thread by the apply thread, which advances
         * applied_seq (the frontier a checkpoint actually stamps from).  Inode
         * locks were already released in diskfs_il_write_redo once log order was
         * fixed; the block pins are dropped by the push thread at trim. */
        __atomic_store_n(&il->durable_seq, rec->seq, __ATOMIC_RELEASE);

        /* Stage C: publish the durable watermark (1-past-last txn id of this
         * record).  Records retire in id order, so it is monotonic; a worker
         * polling it ACKs the client for every txn below it.  Release-ordered:
         * the record (hence every grouped txn) is durable before this is seen. */
        __atomic_store_n(&il->durable_wm, rc->end_txn_id, __ATOMIC_RELEASE);

        /* Hand the record image to the push thread (block home writes) and the
         * completion ctx to the apply thread (space-map apply + txn recycle via
         * applied_wm), both in log order.  Each ring is sized larger than the log
         * can ever hold, so neither can fill before the log does. */
        ht = il->handoff_tail;
        chimera_diskfs_abort_if(ht - __atomic_load_n(&il->handoff_head, __ATOMIC_ACQUIRE) >=
                                il->handoff_ring_size, "intent-log hand-off ring overflow");
        il->handoff[ht & il->handoff_ring_mask] = rec;
        __atomic_store_n(&il->handoff_tail, ht + 1, __ATOMIC_RELEASE);

        at = il->apply_tail;
        chimera_diskfs_abort_if(at - __atomic_load_n(&il->apply_head, __ATOMIC_ACQUIRE) >=
                                il->apply_ring_size, "intent-log apply ring overflow");
        il->apply_queue[at & il->apply_ring_mask] = rc;
        __atomic_store_n(&il->apply_tail, at + 1, __ATOMIC_RELEASE);

        handed_off = 1;

        slot->ctx  = NULL;
        slot->done = 0;
        il->retire_head++;
    }

    diskfs_il_commit_metrics(il);

    if (handed_off) {
        evpl_ring_doorbell(&il->push_doorbell);
        evpl_ring_doorbell(&il->apply_doorbell);
    }
} /* diskfs_redo_write_cb */


/*
 * Apply thread (Stage B): drains the durable records the commit thread enqueued
 * and does the per-txn work off the commit hot path -- release the inode->block
 * links + per-block txn structs, apply the committed space-map deltas to the
 * in-memory free map, and ACK each txn to its worker.  Records arrive strictly
 * in seq order, so per-AG apply stays single-owner and in order, and applied_seq
 * advances monotonically -- the checkpoint stamps ckpt_seq from it (durable_seq
 * now leads it, since apply is async).
 */
static void
diskfs_il_apply_doorbell_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct diskfs_intent_log *il = container_of(doorbell,
                                                struct diskfs_intent_log,
                                                apply_doorbell);
    uint32_t                  head = il->apply_head;
    uint32_t                  tail = __atomic_load_n(&il->apply_tail, __ATOMIC_ACQUIRE);

    (void) evpl;

    while (head != tail) {
        struct diskfs_redo_ctx *rc = il->apply_queue[head & il->apply_ring_mask];
        uint32_t                i;

        head++;

        for (i = 0; i < rc->num_entries; i++) {
            struct diskfs_iq_entry *entry = &rc->entries[i].entry;

            diskfs_metric_time_sample(
                il->metrics.txn_latency[DISKFS_METRIC_TXN_SUBMIT_TO_DURABLE],
                &entry->submit_time);
            diskfs_metric_time_sample(
                il->metrics.txn_latency[DISKFS_METRIC_TXN_QUEUE_TO_DURABLE],
                &entry->enqueue_time);

            /* Release the inode->block links + per-block txn structs and apply
             * the committed space-map deltas to the in-memory free map.  NOT the
             * block pins -- each block's claim pin is held until the record is
             * pushed home and trimmed (the push thread drops it).  The txn itself
             * is recycled by its owning worker once applied_wm passes it below. */
            diskfs_txn_retire_blocks(entry->txn);
            diskfs_txn_apply_allocs(entry->txn);
            diskfs_txn_apply_frees(entry->txn);
        }

        /* Deltas applied + txn structs finished with; advance both apply
         * frontiers in log order (monotonic).  applied_seq (record seq) is what a
         * checkpoint stamps ckpt_seq from; applied_wm (txn id) is what each
         * worker recycles its in-flight txns against.  Release-ordered so a
         * worker that observes applied_wm also observes apply being done with the
         * txn (safe to return it to the per-thread free list). */
        __atomic_store_n(&il->applied_seq, rc->seq, __ATOMIC_RELEASE);
        __atomic_store_n(&il->applied_wm, rc->end_txn_id, __ATOMIC_RELEASE);

        diskfs_il_ctx_recycle(il, rc);
    }

    __atomic_store_n(&il->apply_head, head, __ATOMIC_RELEASE);
} /* diskfs_il_apply_doorbell_cb */


/*
 * Build a full-block redo record for one transaction and issue a durable
 * write into the reserved intent-log region.  Runs on the intent-log thread.
 */
static void
diskfs_il_write_redo(
    struct diskfs_intent_log *il,
    struct diskfs_redo_entry *entries,
    uint32_t                  num_entries,
    uint32_t                  nblocks,
    uint32_t                  num_deltas,
    uint64_t                  end_txn_id)
{
    struct diskfs_redo_ctx          *ctx;
    struct diskfs_il_record         *rec;
    struct diskfs_redo_header       *hdr;
    struct diskfs_redo_block_header *bh;
    uint64_t                         hdr_len, reclen, offset;
    uint32_t                         i, e, nd;
    char                            *p;
    int                              niov;

    hdr_len = diskfs_il_hdr_len(nblocks, num_deltas);
    reclen  = diskfs_il_blocks_reclen(nblocks, num_deltas);

    /* Caller guarantees space (diskfs_iq_process_channel checks diskfs_il_fits
     * before consuming SQ entries), so placement always succeeds. */
    offset = diskfs_il_place(il, reclen);

    /* Stage A: draw the record + its block arrays (iovs/block_bufs/blocks,
     * sized >= nblocks and fully populated below) from the recycle pool -- no
     * malloc in steady state.  Block-swap CoW retires the dirtied block out of
     * the hash but keeps the struct alive (on the LRU, pinned, holding its
     * buffer); rec->blocks carries the block pointer so the tail-pusher drops
     * the obligation pin on THIS block (a (dev,off) lookup would find the new
     * live block after a CoW). */
    rec             = diskfs_il_rec_alloc(il, nblocks);
    rec->seq        = il->log_seq;
    rec->offset     = offset;
    rec->reclen     = reclen;
    rec->num_blocks = nblocks;
    rec->niov       = 1 + nblocks;
    rec->next       = NULL;

    /* iovs[0]: materialized header region (redo_header + per-block headers). */
    niov = evpl_iovec_alloc(il->evpl, hdr_len, DISKFS_BLOCK_SIZE, 1,
                            EVPL_IOVEC_FLAG_SHARED, &rec->iovs[0]);
    chimera_diskfs_abort_if(niov != 1, "redo header did not fit in one iovec (%d)", niov);

    ctx              = diskfs_il_ctx_alloc(il, num_entries);
    ctx->il          = il;
    ctx->num_entries = num_entries;
    ctx->rec         = rec;
    ctx->end_txn_id  = end_txn_id;     /* Stage C: durable/applied watermark target */
    memcpy(ctx->entries, entries, num_entries * sizeof(*ctx->entries));

    p               = (char *) rec->iovs[0].data;
    hdr             = (struct diskfs_redo_header *) p;
    hdr->magic      = DISKFS_REDO_MAGIC;
    hdr->csum_lo    = 0;
    hdr->csum_hi    = 0;
    hdr->seq        = il->log_seq++;
    rec->seq        = hdr->seq;
    ctx->seq        = hdr->seq;     /* Stage B: apply thread advances applied_seq from this */
    hdr->tail       = __atomic_load_n(&il->log_tail, __ATOMIC_ACQUIRE);
    hdr->num_blocks = nblocks;
    hdr->reclen     = (uint32_t) reclen;
    hdr->num_deltas = num_deltas;
    hdr->pad0       = 0;
    p              += sizeof(*hdr);

    i = 0;
    for (e = 0; e < num_entries; e++) {
        struct diskfs_txn_block *tb;

        for (tb = entries[e].entry.txn->blocks; tb; tb = tb->next, i++) {
            struct diskfs_block *blk = tb->block;

            bh                = (struct diskfs_redo_block_header *) p;
            bh->device_id     = blk->device_id;
            bh->pad           = 0;
            bh->device_offset = blk->device_offset;
            /* Per-block XXH3-128 computed by the submitting worker at commit
             * (tb->snap_csum); recovery verifies each image against this, so
             * the IL thread no longer hashes the block images itself. */
            bh->block_csum_lo = tb->snap_csum_lo;
            bh->block_csum_hi = tb->snap_csum_hi;
            p                += sizeof(*bh);

            evpl_iovec_clone(&rec->iovs[1 + i], &tb->snap_buf->iov);
            rec->block_bufs[i] = tb->snap_buf;
            rec->blocks[i]     = blk;
        }
    }
    chimera_diskfs_abort_if(i != nblocks,
                            "redo grouped block count changed (%u != %u)", i, nblocks);

    /* Serialize this batch's space-map deltas after the per-block headers.  The
     * allocator hot path no longer journals alloc/free to a per-AG on-disk log;
     * instead each delta rides this redo record (struct diskfs_redo_delta) and
     * is replayed on crash against the owning AG's checkpoint.  Both alloc and
     * (immediate) free deltas live on txn->space_deltas; deferred transactional
     * frees are applied in-memory from txn->pending_frees on durability but ride
     * here too (recorded into space_deltas by the pre-commit free-journal
     * flush). */
    nd = 0;
    for (e = 0; e < num_entries; e++) {
        struct diskfs_txn_delta *d;

        for (d = entries[e].entry.txn->space_deltas; d; d = d->next, nd++) {
            struct diskfs_redo_delta *rd = (struct diskfs_redo_delta *) p;

            rd->device_id     = d->device_id;
            rd->ag_index      = d->ag_index;
            rd->device_offset = d->device_offset;
            rd->length        = d->length;
            rd->op            = d->op;
            rd->pad           = 0;
            p                += sizeof(*rd);
        }
    }
    chimera_diskfs_abort_if(nd != num_deltas,
                            "redo grouped delta count changed (%u != %u)", nd, num_deltas);

    /* Zero the header-region tail padding so the checksum covers deterministic
     * bytes, then stamp the XXH3-128 over the header region only.  Each block
     * image is protected by its own block_csum (stamped above from the worker's
     * commit-time hash), so the IL thread no longer hashes the 4 KiB images --
     * recovery verifies this header csum, then each image against its block_csum
     * (see diskfs_recover_log). */
    {
        char *end = (char *) rec->iovs[0].data + hdr_len;

        if (p < end) {
            memset(p, 0, (size_t) (end - p));
        }
    }
    {
        XXH128_hash_t h = XXH3_128bits(rec->iovs[0].data, hdr_len);

        hdr->csum_lo = h.low64;
        hdr->csum_hi = h.high64;
    }

    ctx->segments = (rec->niov + DISKFS_IL_MAX_IOV - 1) / DISKFS_IL_MAX_IOV;

    /* Reserve this record's retirement-ring slot (in submission/log order) so
     * the completion can retire the contiguous done-prefix in order. */
    ctx->retire_idx                                            = il->retire_tail;
    il->retire[il->retire_tail & DISKFS_RETIRE_RING_MASK].ctx  = ctx;
    il->retire[il->retire_tail & DISKFS_RETIRE_RING_MASK].done = 0;
    il->retire_tail++;

    il->redo_inflight += ctx->segments;     /* one redo block write per chunk below */
    diskfs_il_commit_metrics(il);

    /* Issue the record in <=DISKFS_IL_MAX_IOV-iovec chunks to consecutive
     * offsets (the on-log record is contiguous); all chunks share ctx and the
     * last completion finalizes the record. */
    {
        uint32_t done = 0;
        uint64_t woff = offset;

        while (done < rec->niov) {
            uint32_t cnt   = rec->niov - done;
            uint64_t bytes = 0;
            uint32_t k;

            if (cnt > DISKFS_IL_MAX_IOV) {
                cnt = DISKFS_IL_MAX_IOV;
            }
            for (k = 0; k < cnt; k++) {
                bytes += rec->iovs[done + k].length;
            }

            evpl_block_write(il->evpl, il->log_queue,
                             &rec->iovs[done], cnt, woff, il->sync,
                             diskfs_redo_write_cb, ctx);
            diskfs_metric_il_block_io(il, DISKFS_METRIC_IO_WRITE,
                                      DISKFS_METRIC_IO_INTENT_LOG, bytes);
            diskfs_metric_il_block_io_device(il, SM_INTENT_LOG_DEVICE,
                                             DISKFS_METRIC_IO_WRITE,
                                             DISKFS_METRIC_IO_INTENT_LOG, bytes);
            woff += bytes;
            done += cnt;
        }
    }

    /* The record now owns an independent snapshot of every block (refs held in
     * rec->block_bufs, data cloned into rec->iovs) and its log order is fixed
     * (seq stamped, retire slot reserved, write issued above).  Release each
     * txn's inode locks NOW, in log order, instead of waiting for the record to
     * be durable: a later writer of any of these blocks will COW-fork it
     * (diskfs_block_claim sees buf->refs > 1), preserving this record's image.
     * Only the block pins, deferred frees, and the client ACK still wait for
     * durable (diskfs_redo_write_cb) -- so the hot parent-directory inode lock
     * is no longer held across the intent-log write latency. */
    for (e = 0; e < num_entries; e++) {
        struct diskfs_txn *t = entries[e].entry.txn;
        int                j;

        for (j = 0; j < t->num_inodes; j++) {
            if (t->inodes[j].mode == DISKFS_INODE_LOCK_WRITE) {
                t->inodes[j].inode->block = NULL;
            }
        }
        diskfs_txn_unlock_all(t);
    }
} /* diskfs_il_write_redo */

static uint32_t
diskfs_il_txn_blocks(struct diskfs_txn *txn)
{
    struct diskfs_txn_block *tb;
    uint32_t                 nblocks = 0;

    for (tb = txn->blocks; tb; tb = tb->next) {
        nblocks++;
    }
    return nblocks;
} /* diskfs_il_txn_blocks */


/* Count of space-map deltas this txn carries into its redo record. */
static uint32_t
diskfs_il_txn_deltas(struct diskfs_txn *txn)
{
    return txn->n_space_deltas;
} /* diskfs_il_txn_deltas */


static uint64_t
diskfs_il_blocks_reclen(
    uint32_t nblocks,
    uint32_t num_deltas)
{
    return diskfs_il_hdr_len(nblocks, num_deltas) +
           (uint64_t) nblocks * DISKFS_BLOCK_SIZE;
} /* diskfs_il_blocks_reclen */


/* Padded on-log length of the redo record for one transaction. */
static uint64_t
diskfs_il_txn_reclen(struct diskfs_txn *txn)
{
    return diskfs_il_blocks_reclen(diskfs_il_txn_blocks(txn),
                                   diskfs_il_txn_deltas(txn));
} /* diskfs_il_txn_reclen */


static int
diskfs_iq_process_batch(struct diskfs_intent_log *il)
{
    struct diskfs_redo_entry entries[DISKFS_IL_MAX_IOV];
    uint32_t                 batch_count  = 0;
    uint32_t                 batch_blocks = 0;
    uint32_t                 batch_deltas = 0;
    uint32_t                 i;
    uint64_t                 pos;

    if (il->redo_inflight >= DISKFS_COMMIT_WATERMARK) {
        return 0;
    }

    if (il->retire_tail - il->retire_head >= DISKFS_RETIRE_RING_SIZE) {
        return 0;
    }

    /* Consume the global submission ring strictly in id order.  Slots that are
     * reserved but not yet published (a producer mid-fill) stop the batch --
     * head-of-line by design, since id order IS log order. */
    pos = il->gsq_head;
    while (batch_count < DISKFS_IL_MAX_IOV) {
        struct diskfs_gsq_slot *slot = &il->gsq[pos & DISKFS_GSQ_MASK];
        struct diskfs_txn      *txn;
        uint32_t                nblocks, next_blocks;
        uint32_t                ndeltas, next_deltas;
        uint64_t                reclen;

        if (__atomic_load_n(&slot->turn, __ATOMIC_ACQUIRE) != pos + 1) {
            break;                        /* empty or not yet published */
        }

        txn         = slot->txn;
        nblocks     = diskfs_il_txn_blocks(txn);
        next_blocks = batch_blocks + nblocks;
        ndeltas     = diskfs_il_txn_deltas(txn);
        next_deltas = batch_deltas + ndeltas;

        /* Keep one normal record to one backend write.  A txn larger than the
         * iov cap goes alone via the segmented path. */
        if (batch_count > 0 && 1 + next_blocks > DISKFS_IL_MAX_IOV) {
            break;
        }

        reclen = diskfs_il_blocks_reclen(next_blocks, next_deltas);
        if (!diskfs_il_fits(il, reclen)) {
            if (batch_count > 0) {
                break;
            }
            if (__atomic_load_n(&il->live_records, __ATOMIC_ACQUIRE) != 0) {
                break;
            }
            __atomic_store_n(&il->log_tail, il->log_head, __ATOMIC_RELEASE);
            if (!diskfs_il_fits(il, reclen)) {
                break;
            }
        }

        entries[batch_count].entry.txn          = txn;
        entries[batch_count].entry.enqueue_time = slot->enqueue_time;

        /* Free the slot for reuse (Vyukov consumer publish): the next producer
         * for this slot is at pos + DISKFS_GSQ_SIZE.  txn/enqueue were copied
         * above, so the slot may be overwritten now. */
        __atomic_store_n(&slot->turn, pos + DISKFS_GSQ_SIZE, __ATOMIC_RELEASE);

        batch_count++;
        batch_blocks = next_blocks;
        batch_deltas = next_deltas;
        pos++;

        if (1 + batch_blocks > DISKFS_IL_MAX_IOV) {
            break;                        /* the lone over-cap txn; stop here */
        }
    }

    if (batch_count == 0) {
        return 0;
    }

    for (i = 0; i < batch_count; i++) {
        prometheus_stopwatch_start(&entries[i].entry.submit_time);
        diskfs_metric_time_sample(
            il->metrics.txn_latency[DISKFS_METRIC_TXN_QUEUE_TO_SUBMIT],
            &entries[i].entry.enqueue_time);
    }

    il->gsq_head = pos;     /* == base + batch_count; the record covers [base, pos) */
    diskfs_il_write_redo(il, entries, batch_count, batch_blocks, batch_deltas, pos);
    return 1;
} /* diskfs_iq_process_batch */


void
diskfs_iq_process_channel(struct diskfs_iq_channel *ch)
{
    struct diskfs_intent_log *il = &ch->worker->shared->intent_log;

    while (diskfs_iq_process_batch(il)) {
    }
} /* diskfs_iq_process_channel */

static void
diskfs_intent_log_drain_pending(struct diskfs_intent_log *il)
{
    struct diskfs_iq_channel *head, *ch;

    pthread_mutex_lock(&il->registration_lock);
    head             = il->pending_head;
    il->pending_head = NULL;
    pthread_mutex_unlock(&il->registration_lock);

    while (head) {
        ch               = head;
        head             = ch->next_pending;
        ch->next_pending = NULL;

        chimera_diskfs_abort_if(il->num_channels >= DISKFS_IL_MAX_CHANNELS,
                                "intent log: too many channels (%u >= %u)",
                                il->num_channels, DISKFS_IL_MAX_CHANNELS);
        il->channels[il->num_channels++] = ch;

        __atomic_store_n(&ch->registered, 1, __ATOMIC_RELEASE);
    }
    diskfs_il_commit_metrics(il);
} /* diskfs_intent_log_drain_pending */


/* Drain newly-registered channels into the slot array and compact out any that
 * requested unregistration.  Rare (worker-thread lifecycle), so it stays on the
 * wake-doorbell path rather than the per-iteration poll. */
static void
diskfs_il_service_registrations(struct diskfs_intent_log *il)
{
    uint32_t i;

    /* Clear the dirty flag before we read pending_head / scan for unregisters,
     * so a (un)registration published after this point re-sets it and is picked
     * up on a later poll rather than being lost. */
    __atomic_store_n(&il->reg_dirty, 0, __ATOMIC_SEQ_CST);

    diskfs_intent_log_drain_pending(il);

    /* Unregister pass: compact slots out (swap-with-tail). */
    i = 0;
    while (i < il->num_channels) {
        struct diskfs_iq_channel *ch = il->channels[i];

        if (__atomic_load_n(&ch->unregister_requested, __ATOMIC_ACQUIRE)) {
            uint32_t last = il->num_channels - 1;

            /* The worker frees the channel (and soon its thread struct) the
             * moment unregister_done is set, and a txn still referenced by the
             * global ring or a record dereferences its submitting worker --
             * acting with anything still in flight would be a use-after-free.
             * The worker drains its in-flight ring (commits_inflight to zero,
             * i.e. every txn it submitted recycled) BEFORE setting
             * unregister_requested (release), which this acquire load pairs with,
             * so the cursors below are settled.  Make a violation loud. */
            chimera_diskfs_abort_if(
                ch->inflight_recycle != ch->inflight_tail,
                "intent-log channel unregistered with txns in flight");

            if (i != last) {
                il->channels[i] = il->channels[last];
            }
            il->channels[last] = NULL;
            il->num_channels   = last;
            __atomic_store_n(&ch->unregister_done, 1, __ATOMIC_RELEASE);
            diskfs_il_commit_metrics(il);
            continue;     /* re-process index i (now a different channel) */
        }
        i++;
    }
} /* diskfs_il_service_registrations */


/* Process registered channel SQs into cross-channel redo batches. */
static int
diskfs_il_process_all(struct diskfs_intent_log *il)
{
    /* One redo submission per call: build a SINGLE cross-channel redo record
     * (one txn per channel per round, up to the DISKFS_IL_MAX_IOV size cap or
     * until the SQs run dry) and submit it, then return so the event loop polls
     * the redo CQ and reaps completions before we build the next record.
     *
     * Previously this drained the entire SQ backlog in one heads-down stretch
     * (looping process_batch), so the per-iteration CQ poll could not reap redo
     * completions until the whole stretch finished -- inflating completion
     * latency (the ~1.87ms reap-delay mode) and leaving the device under-driven.
     * Submitting one record per iteration interleaves submit with reap; the
     * caller's evpl_activity keeps us in poll mode so the next record is built
     * on the very next iteration with no sleep in between. */
    return diskfs_iq_process_batch(il);
} /* diskfs_il_process_all */


/* Seq-cst re-scan for the poll-exit wakeup handshake (diskfs_iq_try_submit).
 * gsq_head is commit-private; a producer that has reserved a slot but not yet
 * published it still shows tail != head, so this conservatively reports work
 * (the consumer spins on turn and picks it up momentarily). */
static int
diskfs_il_has_sq_work(struct diskfs_intent_log *il)
{
    return __atomic_load_n(&il->gsq_tail, __ATOMIC_SEQ_CST) != il->gsq_head;
} /* diskfs_il_has_sq_work */


/* The wake doorbell only needs to rouse a sleeping commit thread; once awake,
* diskfs_il_sq_poll discovers SQ work every iteration without it.  Workers ring
* it on channel (un)registration and only when the commit thread is asleep. */
static void
diskfs_intent_log_wake_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct diskfs_intent_log *il = container_of(doorbell,
                                                struct diskfs_intent_log,
                                                wake_doorbell);

    (void) evpl;

    diskfs_il_service_registrations(il);
    diskfs_il_process_all(il);
} /* diskfs_intent_log_wake_cb */


/* Per-iteration SQ poll: discover and process committed transactions without
* waiting for a doorbell.  Scanning an SQ is just two atomic loads, so this is
* cheap when idle; processing work marks activity to keep us in poll mode. */
static void
diskfs_il_sq_poll(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_intent_log *il = private_data;

    /* Pick up channel (un)registrations without waiting for the wake doorbell:
     * while we stay in continuous poll mode under load the doorbell is starved,
     * so a freshly-registered channel would otherwise never enter channels[] and
     * its commits would never be seen.  Gated on a cheap atomic so the common
     * (no-change) case avoids the registration_lock. */
    if (__atomic_load_n(&il->reg_dirty, __ATOMIC_ACQUIRE)) {
        diskfs_il_service_registrations(il);
    }

    if (diskfs_il_process_all(il)) {
        evpl_activity(evpl);
    }
} /* diskfs_il_sq_poll */


/* Entering poll mode -> the commit thread is awake and polling the SQs. */
static void
diskfs_il_poll_enter(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_intent_log *il = private_data;

    (void) evpl;
    __atomic_store_n(&il->awake, 1, __ATOMIC_SEQ_CST);
} /* diskfs_il_poll_enter */


/* Leaving poll mode (about to block).  Publish "asleep", then re-scan once: a
 * submitter that enqueued before observing awake=0 is picked up here; one that
 * enqueues afterward observes awake=0 and rings the wake doorbell.  Dekker-style
 * handshake -- the awake flag and the SQ tails are all seq_cst, so a wakeup can
 * never be lost. */
void
diskfs_il_poll_exit(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_intent_log *il = private_data;

    __atomic_store_n(&il->awake, 0, __ATOMIC_SEQ_CST);

    if (diskfs_il_has_sq_work(il)) {
        __atomic_store_n(&il->awake, 1, __ATOMIC_SEQ_CST);
        diskfs_il_process_all(il);
        evpl_activity(evpl);   /* stay awake; the loop will not block this pass */
    }
} /* diskfs_il_poll_exit */


/* Submit one txn into the global submission ring and record it on this worker's
 * private in-flight ring (for watermark-driven completion).  Returns 1 on
 * success, 0 if either ring is full (the caller then parks the commit).  The
 * client completion fires later from diskfs_iq_drain_cq when the durable
 * watermark passes this txn's id. */
int
diskfs_iq_try_submit(
    struct diskfs_thread  *thread,
    struct diskfs_txn     *txn,
    diskfs_txn_commit_cb_t cb,
    void                  *private_data)
{
    struct diskfs_shared       *shared = thread->shared;
    struct diskfs_intent_log   *il     = &shared->intent_log;
    struct diskfs_iq_channel   *ch     = thread->iq_channel;
    struct diskfs_iq_inflight  *fe;
    struct diskfs_gsq_slot     *slot;
    struct prometheus_stopwatch enqueue;
    uint64_t                    pos;

    /* Worker-private in-flight ring full?  (Bounds this worker's outstanding
     * commits; recycle trails the applied watermark.) */
    if (ch->inflight_tail - ch->inflight_recycle >= DISKFS_IQ_INFLIGHT_SIZE) {
        return 0;
    }

    /* Claim a global-ring slot (Vyukov bounded MPSC enqueue).  pos == the txn's
     * monotonic id and its log order. */
    prometheus_stopwatch_start(&enqueue);
    pos = __atomic_load_n(&il->gsq_tail, __ATOMIC_RELAXED);
    for ( ;; ) {
        uint64_t turn;
        int64_t  diff;

        slot = &il->gsq[pos & DISKFS_GSQ_MASK];
        turn = __atomic_load_n(&slot->turn, __ATOMIC_ACQUIRE);
        diff = (int64_t) (turn - pos);

        if (diff == 0) {
            if (__atomic_compare_exchange_n(&il->gsq_tail, &pos, pos + 1, 1,
                                            __ATOMIC_RELAXED, __ATOMIC_RELAXED)) {
                break;                    /* claimed slot `pos` */
            }
            /* CAS reloaded pos; retry */
        } else if (diff < 0) {
            return 0;                     /* global ring full -> park */
        } else {
            pos = __atomic_load_n(&il->gsq_tail, __ATOMIC_RELAXED);
        }
    }

    slot->txn          = txn;
    slot->enqueue_time = enqueue;
    /* Publish: the commit thread spins on turn==pos+1 before reading the slot. */
    __atomic_store_n(&slot->turn, pos + 1, __ATOMIC_RELEASE);

    /* Record on the worker's in-flight ring so completion can find cb/private by
     * id when the watermark passes. */
    fe               = &ch->inflight[ch->inflight_tail & DISKFS_IQ_INFLIGHT_MASK];
    fe->txn_id       = pos;
    fe->txn          = txn;
    fe->cb           = cb;
    fe->private_data = private_data;
    fe->enqueue_time = enqueue;
    ch->inflight_tail++;

    /* The commit thread polls the global ring every loop iteration while awake,
     * so the wake doorbell is only needed to rouse it once it has slept.  The
     * seq-cst load pairs with the commit thread's diskfs_il_poll_exit handshake. */
    if (!__atomic_load_n(&il->awake, __ATOMIC_SEQ_CST)) {
        evpl_ring_doorbell(&il->wake_doorbell);
    }
    return 1;
} /* diskfs_iq_try_submit */


/* Resume commits parked on the SQ-full FIFO, in order, as space allows.  Called
 * from the CQ doorbell, which fires once the intent-log thread has consumed SQ
 * entries (freeing space) and posted completions on this worker. */
static void
diskfs_iq_resume_commit_waiters(struct diskfs_thread *thread)
{
    struct diskfs_txn *txn;

    while ((txn = thread->commit_wait_head)) {
        if (!diskfs_iq_try_submit(thread, txn, txn->commit_cb, txn->commit_private)) {
            break;     /* SQ full again; the next CQ doorbell resumes us */
        }

        thread->commit_wait_head = txn->commit_wait_next;
        if (!thread->commit_wait_head) {
            thread->commit_wait_tail = NULL;
        }
    }
} /* diskfs_iq_resume_commit_waiters */


/* Complete this worker's in-flight txns against the global watermarks: ACK the
 * client (cb) for every txn whose id is below the durable watermark, then
 * recycle the txn (and drop the poll-mode pin) for every txn whose id is below
 * the applied watermark -- the apply thread has finished with it by then.
 * Finally retry any commits that parked on a full ring.  Returns the number of
 * txns ACKed.  Runs every loop iteration via diskfs_iq_cq_poll while the worker
 * is poll-pinned (a commit outstanding). */
int
diskfs_iq_drain_cq(struct diskfs_iq_channel *ch)
{
    struct diskfs_thread *worker  = ch->worker;
    uint64_t              durable = __atomic_load_n(
        &worker->shared->intent_log.durable_wm, __ATOMIC_ACQUIRE);
    uint64_t              applied = __atomic_load_n(
        &worker->shared->intent_log.applied_wm, __ATOMIC_ACQUIRE);
    int                   drained = 0;

    /* ACK the client for the contiguous prefix now durable (recoverable).  The
     * txn's logical inode locks were already dropped by the commit thread in
     * diskfs_il_write_redo, so this just delivers completion. */
    while (ch->inflight_ack != ch->inflight_tail) {
        struct diskfs_iq_inflight *fe =
            &ch->inflight[ch->inflight_ack & DISKFS_IQ_INFLIGHT_MASK];

        if ((int64_t) (fe->txn_id - durable) >= 0) {
            break;                        /* not yet durable */
        }
        diskfs_metric_time_sample(
            worker->metrics.txn_latency[DISKFS_METRIC_TXN_QUEUE_TO_CALLBACK],
            &fe->enqueue_time);
        fe->cb(fe->txn, 0, fe->private_data);
        ch->inflight_ack++;
        drained++;
    }

    /* Recycle the txn for the prefix now applied -- the apply thread has finished
     * reading txn->blocks/space_deltas/pending_frees, so it is safe to return to
     * this worker's per-thread free list (recycle <= ack always, since applied
     * <= durable).  Drop the poll-mode pin once nothing is outstanding. */
    while (ch->inflight_recycle != ch->inflight_ack) {
        struct diskfs_iq_inflight *fe =
            &ch->inflight[ch->inflight_recycle & DISKFS_IQ_INFLIGHT_MASK];

        if ((int64_t) (fe->txn_id - applied) >= 0) {
            break;                        /* not yet applied */
        }
        diskfs_txn_release(fe->txn);
        ch->inflight_recycle++;
        if (--worker->commits_inflight == 0) {
            evpl_poll_unpin(worker->evpl);
        }
    }

    /* Ring space may have freed, so retry any commits parked on a full ring. */
    diskfs_iq_resume_commit_waiters(worker);
    return drained;
} /* diskfs_iq_drain_cq */


/* Per-iteration completion poll.  While a commit is outstanding the worker is
 * pinned in poll mode (diskfs_txn_commit_finish), so completions are reaped
 * within a loop iteration instead of waiting at the spin boundary. */
void
diskfs_iq_cq_poll(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_iq_channel *ch = private_data;

    if (diskfs_iq_drain_cq(ch)) {
        evpl_activity(evpl);
    }
} /* diskfs_iq_cq_poll */


/*
 * IL pipeline stall watchdog.  Runs on the commit thread on a ~1s timer.  The
 * commit thread produces durable_wm; the apply thread produces applied_wm.  If
 * there is outstanding pipeline work (txns submitted-but-not-durable, durable-
 * but-not-applied, records still queued to apply, or the in-order retire ring
 * not drained) and none of those frontiers have advanced for
 * DISKFS_IL_WD_STALL_TICKS ticks, dump the full pipeline state so a CI hang
 * shows which stage froze (classically: the apply thread wedged, leaving
 * applied_wm stuck below durable_wm).  Read-only on the pipeline.
 */
static void
diskfs_il_watchdog_cb(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct diskfs_intent_log *il = container_of(timer,
                                                struct diskfs_intent_log,
                                                wd_timer);
    uint64_t                  durable_wm  = __atomic_load_n(&il->durable_wm, __ATOMIC_ACQUIRE);
    uint64_t                  applied_wm  = __atomic_load_n(&il->applied_wm, __ATOMIC_ACQUIRE);
    uint64_t                  gsq_tail    = __atomic_load_n(&il->gsq_tail, __ATOMIC_ACQUIRE);
    uint32_t                  apply_head  = __atomic_load_n(&il->apply_head, __ATOMIC_ACQUIRE);
    uint32_t                  apply_tail  = __atomic_load_n(&il->apply_tail, __ATOMIC_ACQUIRE);
    uint64_t                  retire_head = il->retire_head;   /* commit-thread private (we are it) */
    uint64_t                  retire_tail = il->retire_tail;
    int                       apply_stalled, commit_stalled;
    const char               *stage;

    (void) evpl;

    /* Per-stage stall detection, evaluated independently so forward progress in
     * one stage cannot mask a freeze in another.  The apply check is the load-
     * bearing one: the apply thread is a *different* thread, so a wedge there is
     * visible from this (live) commit-thread timer even while the commit thread
     * keeps advancing durable_wm -- apply_head frozen with records still queued
     * (apply_head != apply_tail) is exactly the LAYOUTCOMMIT apply-pipeline
     * hang.  The commit check catches the commit thread failing to drain its
     * claimed slots / retire ring (only observable while it is still looping). */
    apply_stalled = (apply_head != apply_tail) &&
        (apply_head == il->wd_last_apply_head);

    commit_stalled = ((gsq_tail != durable_wm) || (retire_head != retire_tail)) &&
        (durable_wm == il->wd_last_durable_wm) &&
        (retire_head == il->wd_last_retire_head);

    il->wd_last_durable_wm  = durable_wm;
    il->wd_last_applied_wm  = applied_wm;
    il->wd_last_apply_head  = apply_head;
    il->wd_last_retire_head = retire_head;

    if (!apply_stalled && !commit_stalled) {
        il->wd_stall_ticks = 0;
        return;
    }

    il->wd_stall_ticks++;

    if (il->wd_stall_ticks == DISKFS_IL_WD_STALL_TICKS ||
        (il->wd_stall_ticks > DISKFS_IL_WD_STALL_TICKS &&
         (il->wd_stall_ticks % DISKFS_IL_WD_RELOG_TICKS) == 0)) {
        stage = (apply_stalled && commit_stalled) ? "apply+commit" :
            apply_stalled ? "apply" : "commit";
        chimera_diskfs_error(
            "IL pipeline STALLED ~%us [%s]: gsq_tail=%lu durable_wm=%lu applied_wm=%lu "
            "(submitted-not-durable=%lu durable-not-applied=%lu) "
            "retire[head=%lu tail=%lu] apply_ring[head=%u tail=%u] "
            "redo_inflight=%d push_outstanding=%d live_records=%lu log[head=%lu tail=%lu]",
            il->wd_stall_ticks, stage,
            (unsigned long) gsq_tail, (unsigned long) durable_wm, (unsigned long) applied_wm,
            (unsigned long) (gsq_tail - durable_wm), (unsigned long) (durable_wm - applied_wm),
            (unsigned long) retire_head, (unsigned long) retire_tail,
            apply_head, apply_tail,
            il->redo_inflight, il->push_outstanding,
            (unsigned long) il->live_records,
            (unsigned long) __atomic_load_n(&il->log_head, __ATOMIC_ACQUIRE),
            (unsigned long) __atomic_load_n(&il->log_tail, __ATOMIC_ACQUIRE));
    }
} /* diskfs_il_watchdog_cb */


void *
diskfs_intent_log_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_intent_log *il     = private_data;
    struct diskfs_shared     *shared = container_of(il, struct diskfs_shared, intent_log);
    int                       i;

    il->evpl                        = evpl;
    il->intent_log_size             = shared->intent_log_size;
    il->handoff_ring_size           = diskfs_il_pow2((uint32_t) (il->intent_log_size / SM_BLOCK_SIZE) * 2);
    il->handoff_ring_mask           = il->handoff_ring_size - 1;
    il->log_head                    = SM_INTENT_LOG_OFFSET;
    il->log_tail                    = SM_INTENT_LOG_OFFSET;
    il->live_records                = 0;
    il->log_seq                     = 0;
    il->redo_inflight               = 0;
    il->redo_inflight_high_water    = 0;
    il->push_outstanding            = 0;
    il->push_outstanding_high_water = 0;
    il->log_used_bytes_high_water   = 0;
    il->sync                        = !shared->unsafe_async;

    /* In-order retirement ring + cross-thread hand-off ring to the push thread. */
    il->retire      = calloc(DISKFS_RETIRE_RING_SIZE, sizeof(*il->retire));
    il->retire_head = 0;
    il->retire_tail = 0;
    il->handoff     = calloc(il->handoff_ring_size, sizeof(*il->handoff));
    __atomic_store_n(&il->handoff_head, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&il->handoff_tail, 0, __ATOMIC_RELAXED);

    /* Stage B: cross-thread apply ring (commit -> apply thread).  Sized like the
     * hand-off ring (larger than the log can hold) so it cannot overflow. */
    il->apply_ring_size = il->handoff_ring_size;
    il->apply_ring_mask = il->handoff_ring_mask;
    il->apply_queue     = calloc(il->apply_ring_size, sizeof(*il->apply_queue));
    __atomic_store_n(&il->apply_head, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&il->apply_tail, 0, __ATOMIC_RELAXED);

    /* Stage C: global submission ring + completion watermarks.  Allocated here
     * (before the commit thread publishes ready) so it exists before any worker
     * registers and submits.  Vyukov init: slot i's turn starts at i, so the
     * first producer claiming id i (turn==i) proceeds and the consumer at i waits
     * for turn==i+1. */
    {
        uint64_t s;

        il->gsq = calloc(DISKFS_GSQ_SIZE, sizeof(*il->gsq));
        for (s = 0; s < DISKFS_GSQ_SIZE; s++) {
            __atomic_store_n(&il->gsq[s].turn, s, __ATOMIC_RELAXED);
        }
        il->gsq_head = 0;
        __atomic_store_n(&il->gsq_tail, 0, __ATOMIC_RELAXED);
        __atomic_store_n(&il->durable_wm, 0, __ATOMIC_RELAXED);
        __atomic_store_n(&il->applied_wm, 0, __ATOMIC_RELAXED);
    }

    diskfs_intent_log_metrics_init(il);
    diskfs_il_commit_metrics(il);

    /* Redo records are written only to the intent-log device. */
    il->log_queue = shared->devices[SM_INTENT_LOG_DEVICE].bdev ?
        evpl_block_open_queue(evpl, shared->devices[SM_INTENT_LOG_DEVICE].bdev) : NULL;

    (void) i;
    evpl_add_doorbell(evpl, &il->wake_doorbell, diskfs_intent_log_wake_cb);

    /* Poll all channel SQs every loop iteration (cheap atomic loads) so commit
     * pickup never waits for the wake doorbell; the doorbell only rouses us when
     * we have actually gone to sleep.  Start "awake" -- we are about to spin. */
    /* awake must track the loop's poll_mode, which starts at 0 (the thread is
     * event-driven until activity pulls it into poll mode).  Initialising this
     * to 1 would lie -- a submitter would skip the wake doorbell believing we
     * are polling while we are actually asleep, stranding the commit until some
     * unrelated doorbell happens to wake us.  poll_enter/poll_exit own it from
     * here; it is 0 (asleep) until the first poll_enter. */
    __atomic_store_n(&il->awake, 0, __ATOMIC_SEQ_CST);
    __atomic_store_n(&il->reg_dirty, 1, __ATOMIC_SEQ_CST);   /* service any channels registered before we started polling */
    il->sq_poll = evpl_add_poll(evpl, diskfs_il_poll_enter, diskfs_il_poll_exit,
                                diskfs_il_sq_poll, il);

    /* Stall watchdog: ~1s timer that dumps the pipeline frontiers if they stop
     * advancing while work is outstanding (diagnostic for the pNFS LAYOUTCOMMIT
     * apply-pipeline hang).  See diskfs_il_watchdog_cb. */
    il->wd_last_durable_wm  = 0;
    il->wd_last_applied_wm  = 0;
    il->wd_last_retire_head = 0;
    il->wd_last_apply_head  = 0;
    il->wd_stall_ticks      = 0;
    evpl_add_timer(evpl, &il->wd_timer, diskfs_il_watchdog_cb, DISKFS_IL_WD_INTERVAL_US);

    __atomic_store_n(&il->ready, 1, __ATOMIC_RELEASE);
    return il;
} /* diskfs_intent_log_thread_init */


void
diskfs_intent_log_thread_shutdown(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_intent_log *il = private_data;

    /* Workers are gone, so no new SQ work arrives.  Drain every in-flight redo
     * write and retire (hand off, in order) every record to the push thread --
     * which is still running and will flush them home before it is itself shut
     * down (the push thread is destroyed after this one). */
    while (il->redo_inflight || il->retire_head != il->retire_tail) {
        evpl_continue(evpl);
    }

    evpl_remove_timer(evpl, &il->wd_timer);
    evpl_remove_poll(evpl, il->sq_poll);
    evpl_remove_doorbell(evpl, &il->wake_doorbell);
    if (il->log_queue) {
        evpl_block_close_queue(evpl, il->log_queue);
    }
    free(il->retire);
    free(il->gsq);
} /* diskfs_intent_log_thread_shutdown */


void *
diskfs_il_push_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_intent_log *il     = private_data;
    struct diskfs_shared     *shared = container_of(il, struct diskfs_shared, intent_log);
    uint32_t                  cap;
    int                       i;

    il->push_evpl  = evpl;
    il->push_head  = NULL;
    il->push_tail  = NULL;
    il->ready_head = NULL;
    il->ready_tail = NULL;
    il->pfree      = NULL;

    g_dbg_il = il;     /* debug handle for gdb: call dbg_dump_push() at a wedge */

    /* Pending map: one bucket budget per distinct block the log can hold. */
    cap            = diskfs_il_pow2((shared->intent_log_size / DISKFS_BLOCK_SIZE) * 2);
    il->phash_mask = cap - 1;
    il->phash      = calloc(cap, sizeof(*il->phash));

    /* Home writes can target any device. */
    il->home_queue = calloc(shared->num_devices, sizeof(*il->home_queue));
    /* Per-device in-flight home-write counters (gate in diskfs_push_issue). */
    il->push_outstanding_dev = calloc(shared->num_devices,
                                      sizeof(*il->push_outstanding_dev));
    for (i = 0; i < shared->num_devices; i++) {
        il->home_queue[i] = shared->devices[i].bdev ?
            evpl_block_open_queue(evpl, shared->devices[i].bdev) : NULL;
    }

    evpl_add_doorbell(evpl, &il->push_doorbell, diskfs_il_push_doorbell_cb);
    __atomic_store_n(&il->push_ready, 1, __ATOMIC_RELEASE);
    return il;
} /* diskfs_il_push_thread_init */


void
diskfs_il_push_thread_shutdown(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_intent_log *il     = private_data;
    struct diskfs_shared     *shared = container_of(il, struct diskfs_shared, intent_log);
    struct diskfs_pending    *p;
    int                       i;

    /* The commit thread is already gone, so no new hand-offs arrive.  Drain
     * every handed-off record home and trim the log fully (clean unmount => no
     * replay needed). */
    while (il->handoff_head != __atomic_load_n(&il->handoff_tail, __ATOMIC_ACQUIRE) ||
           il->push_head || il->push_outstanding) {
        diskfs_il_push_doorbell_cb(evpl, &il->push_doorbell);
        evpl_continue(evpl);
    }

    evpl_remove_doorbell(evpl, &il->push_doorbell);

    for (i = 0; i < shared->num_devices; i++) {
        if (il->home_queue[i]) {
            evpl_block_close_queue(evpl, il->home_queue[i]);
        }
    }
    free(il->home_queue);
    free(il->push_outstanding_dev);

    while ((p = il->pfree)) {
        il->pfree = p->rnext;
        free(p);
    }
    free(il->phash);
} /* diskfs_il_push_thread_shutdown */


void *
diskfs_il_apply_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_intent_log *il = private_data;

    il->apply_evpl = evpl;
    evpl_add_doorbell(evpl, &il->apply_doorbell, diskfs_il_apply_doorbell_cb);
    __atomic_store_n(&il->apply_ready, 1, __ATOMIC_RELEASE);
    return il;
} /* diskfs_il_apply_thread_init */


void
diskfs_il_apply_thread_shutdown(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_intent_log *il = private_data;

    /* The commit thread is gone, so no new ctxs are enqueued.  Apply every
     * remaining record's deltas and ACK its txns, advancing applied_seq up to
     * the final durable_seq -- so the push thread can finish trimming (its
     * checkpoint frontier reads applied_seq) and the unmount checkpoint persists
     * a complete free map. */
    while (il->apply_head != __atomic_load_n(&il->apply_tail, __ATOMIC_ACQUIRE)) {
        diskfs_il_apply_doorbell_cb(evpl, &il->apply_doorbell);
        evpl_continue(evpl);
    }
    evpl_remove_doorbell(evpl, &il->apply_doorbell);
} /* diskfs_il_apply_thread_shutdown */


/*
 * Commit-prep fault context.  The grant no longer eager-faults the inode home
 * block; a b+tree modify links its inode's root in the descent, but an attr-only
 * modify (setattr, and the nlink/parent change on a link/unlink/rename target)
 * touches no b+tree, so its home block may not be resident at commit.  The
 * commit-prep loop below is the single can't-miss chokepoint that faults any
 * still-unlinked write inode before the dinode flush.
 */
struct diskfs_commit_fault_ctx {
    struct diskfs_txn     *txn;
    diskfs_txn_commit_cb_t cb;
    void                  *private_data;
};

static void
diskfs_commit_fault_resume(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct diskfs_commit_fault_ctx *c   = private_data;
    struct diskfs_txn              *txn = c->txn;
    diskfs_txn_commit_cb_t          cb  = c->cb;
    void                           *pd  = c->private_data;

    (void) inode;
    (void) status;
    free(c);
    /* Re-enter: the loop skips the inode we just linked and faults the next. */
    diskfs_txn_commit_finish(txn, cb, pd);
} /* diskfs_commit_fault_resume */


/*
 * The post-free-flush half of commit: serialize the dirty inodes, snapshot the
 * pinned blocks, and hand the txn to the intent-log thread.  Runs once the
 * deferred FREE deltas are journaled (inline, or resumed via the load).
 */
void
diskfs_txn_commit_finish(
    struct diskfs_txn     *txn,
    diskfs_txn_commit_cb_t cb,
    void                  *private_data)
{
    struct diskfs_thread *thread = txn->thread;
    int                   i;

    /* Commit-prep: fault any write-locked inode whose home block is not yet
     * resident, so the flush below can serialize its dinode.  A no-op for
     * descent-linked inodes, inline for a cache hit, suspending only on a cold
     * reload (diskfs_commit_fault_resume re-enters here when it lands). */
    for (i = 0; i < txn->num_inodes; i++) {
        struct diskfs_inode *in = txn->inodes[i].inode;

        /* Skip deferred-mtime inodes (mtime_dirty): they intentionally commit
         * without their home block (the coalescing flusher logs them later).
         * Everything else write-locked needs its dinode flushed here. */
        if (txn->inodes[i].mode == DISKFS_INODE_LOCK_WRITE && !in->block &&
            !in->mtime_dirty) {
            struct diskfs_commit_fault_ctx *c = malloc(sizeof(*c));

            c->txn          = txn;
            c->cb           = cb;
            c->private_data = private_data;
            diskfs_inode_finish_write_pin(thread, txn, in,
                                          diskfs_commit_fault_resume, c, 0);
            return;
        }
    }

    /* Serialize every dirty inode into its block buffer now, on the worker
     * that owns the live inodes under write lock, before handing the txn
     * (and its pinned blocks) to the intent log thread. */
    diskfs_txn_flush_inodes(txn);

    /* Snapshot each block's buffer (zero-copy ref) while the content is final
     * and the inode locks are still held -- so the redo record captures this
     * txn's committed image, immune to a later COW, and the intent-log thread
     * never has to touch the live block->iov.  The refs are moved into the
     * record by diskfs_il_write_redo.
     *
     * Each clone runs under the block's shard lock: a shared block (an
     * AG-log block pinned dirty by several journaling txns) goes LOGGED as
     * soon as the FIRST txn's record is durable, at which point a concurrent
     * claim COW-forks it -- replacing block->iov under that same lock --
     * while a later txn is still snapshotting.  The COW copies the buffer,
     * so whichever side of the swap the clone sees carries this txn's
     * bytes. */
    {
        struct diskfs_txn_block *tb;
        uint64_t                 blocks = 0;

        for (tb = txn->blocks; tb; tb = tb->next) {
            struct diskfs_block_shard *bshard =
                diskfs_block_shard(thread->shared->block_cache,
                                   tb->block->device_id,
                                   tb->block->device_offset);
            XXH128_hash_t              snap_hash;

            pthread_mutex_lock(&bshard->lock);
            diskfs_block_buf_ref_locked(tb->block->buf);
            tb->snap     = tb->block->iov;
            tb->snap_buf = tb->block->buf;
            pthread_mutex_unlock(&bshard->lock);

            /* Hash the snapshotted image here, on the submitting worker, so the
             * single IL thread never hashes 4 KiB/block -- it just copies this
             * csum into the block header.  The snapshot is frozen (snap_buf's
             * ref pins it; any later writer COW-forks), so hashing after the
             * unlock is safe and keeps it off the shard lock. */
            snap_hash        = XXH3_128bits(tb->snap.data, DISKFS_BLOCK_SIZE);
            tb->snap_csum_lo = snap_hash.low64;
            tb->snap_csum_hi = snap_hash.high64;
            blocks++;
        }
        diskfs_metric_counter_inc(thread->metrics.txn[0]);
        diskfs_metric_counter_add(thread->metrics.txn[1], blocks);
        diskfs_metric_counter_add(thread->metrics.txn[2], blocks * DISKFS_BLOCK_SIZE);
        diskfs_metric_histogram_sample(thread->metrics.txn_blocks, blocks);
        diskfs_metric_histogram_sample(thread->metrics.txn_bytes,
                                       blocks * DISKFS_BLOCK_SIZE);
    }

    /* Hand the txn -> intent log thread via this worker's SQ.  The intent log
     * thread drops the txn's logical inode locks when it processes the entry
     * (see diskfs_iq_process_channel); these are logical locks tracked in the
     * cache, not pthread mutexes, so holding them while the commit is parked
     * on a full SQ cannot deadlock (conflicting ops simply park as waiters).
     * The completion callback fires from the CQ doorbell back on this worker.
     *
     * SQ-full backpressure parks the commit on this worker's FIFO and returns;
     * the CQ doorbell (diskfs_iq_resume_commit_waiters) retries it once the IL
     * thread frees SQ space.  We never spin evpl_continue here -- doing so
     * would re-enter this worker's event loop from within a callback that is
     * itself running under evpl_continue (e.g. the close-thread sweep), which
     * recurses without bound.  If commits are already parked, queue behind them
     * to preserve submission order. */
    /* This commit is entering the intent-log pipeline (submitted now or parked
    * for later).  Pin the worker in poll mode so it keeps draining its CQ
    * every iteration (diskfs_iq_cq_poll) and never sleeps with a commit
    * outstanding; released in diskfs_iq_drain_cq when the commit completes. */
    if (thread->commits_inflight++ == 0) {
        evpl_poll_pin(thread->evpl);
    }

    if (!thread->commit_wait_head &&
        diskfs_iq_try_submit(thread, txn, cb, private_data)) {
        txn->dbg_stage = 3;     /* SUBMITTED to IL */
        return;
    }

    txn->dbg_stage        = 4;     /* PARKED on commit-wait FIFO */
    txn->commit_cb        = cb;
    txn->commit_private   = private_data;
    txn->commit_wait_next = NULL;

    if (thread->commit_wait_tail) {
        thread->commit_wait_tail->commit_wait_next = txn;
    } else {
        thread->commit_wait_head = txn;
    }
    thread->commit_wait_tail = txn;
} /* diskfs_txn_commit_finish */


/* Resume a commit whose pre-commit free-journal flush parked on a log read. */
void
diskfs_commit_resume(
    struct diskfs_thread *thread,
    void                 *arg)
{
    struct diskfs_commit_ctx *c = arg;

    if (diskfs_txn_flush_free_journals(thread, c->txn, c) == SM_AGAIN) {
        return;     /* re-parked; another log block is loading */
    }
    diskfs_txn_commit_finish(c->txn, c->cb, c->private_data);
    free(c);
} /* diskfs_commit_resume */


int
diskfs_recover_rec_cmp(
    const void *a,
    const void *b)
{
    uint64_t sa = ((const struct diskfs_recover_rec *) a)->seq;
    uint64_t sb = ((const struct diskfs_recover_rec *) b)->seq;

    return (sa > sb) - (sa < sb);
} /* diskfs_recover_rec_cmp */
