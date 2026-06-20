// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Block cache: sharded cache of 4 KiB metadata blocks with async fetch
 * (park/resume on miss), pin/unpin, LOGGED->COW forking, eviction, and the
 * cross-thread waiter dispatch (doorbell/deferral) machinery.
 */

#include "diskfs_internal.h"

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
        buf->next           = shard->free_buffers;
        shard->free_buffers = buf;
        shard->nfree_buffers++;
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

    chimera_diskfs_abort_if(!blk,
                            "block cache shard exhausted: every buffer pinned "
                            "(raise block_cache_blocks above the intent-log size, "
                            "or a pin was leaked)");
    diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_RECYCLE);

    diskfs_block_lru_unlink(shard, blk);

    /* Unhook from its current bucket (no-op for a never-keyed free buffer:
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


void
diskfs_block_unpin(
    struct diskfs_thread   *thread,
    struct diskfs_block    *blk,
    enum diskfs_block_state new_state)
{
    struct diskfs_block_shard *shard = diskfs_block_shard(thread->shared->block_cache,
                                                          blk->device_id, blk->device_offset);

    pthread_mutex_lock(&shard->lock);
    diskfs_block_drain_clean_locked(shard);
    __atomic_store_n(&blk->state, new_state, __ATOMIC_RELEASE);
    if (__atomic_sub_fetch(&blk->pin_count, 1, __ATOMIC_ACQ_REL) == 0 &&
        __atomic_load_n(&blk->state, __ATOMIC_ACQUIRE) == DISKFS_BLOCK_CLEAN && !blk->on_lru) {
        diskfs_block_lru_push_tail(shard, blk);
    }
    pthread_mutex_unlock(&shard->lock);
} /* diskfs_block_unpin */


/* Release a descent pin without changing the block's state. */
void
diskfs_block_release(
    struct diskfs_thread *thread,
    struct diskfs_block  *blk)
{
    struct diskfs_block_shard *shard = diskfs_block_shard(thread->shared->block_cache,
                                                          blk->device_id, blk->device_offset);

    pthread_mutex_lock(&shard->lock);
    diskfs_block_drain_clean_locked(shard);
    if (__atomic_sub_fetch(&blk->pin_count, 1, __ATOMIC_ACQ_REL) == 0 &&
        __atomic_load_n(&blk->state, __ATOMIC_ACQUIRE) == DISKFS_BLOCK_CLEAN && !blk->on_lru) {
        diskfs_block_lru_push_tail(shard, blk);
    }
    pthread_mutex_unlock(&shard->lock);
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

    extra                         = cache->shard_cap;
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

        shard->buffers  = calloc(total, sizeof(*shard->buffers));
        shard->nbuffers = total;

        for (j = 0; j < total; j++) {
            struct diskfs_block_buf *buf = &shard->buffers[j];
            int                      niov;

            niov = evpl_iovec_alloc(evpl, DISKFS_BLOCK_SIZE, DISKFS_BLOCK_SIZE, 1,
                                    EVPL_IOVEC_FLAG_SHARED, &buf->iov);
            chimera_diskfs_abort_if(niov != 1,
                                    "diskfs block buffer did not fit in one iovec (%d)",
                                    niov);
            buf->shard          = shard;
            buf->on_free        = 1;
            buf->next           = shard->free_buffers;
            shard->free_buffers = buf;
            shard->nfree_buffers++;
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

        /* Release each block's buffer (NULL evpl -> straight to the global
         * allocator, which is correct at teardown); the structs are one pool
         * array.  At a clean unmount every block is CLEAN (refcount 1). */
        for (j = 0; j < shard->nbuffers; j++) {
            evpl_iovec_release(NULL, &shard->buffers[j].iov);
        }
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
        blk                = diskfs_block_recycle(thread, shard);
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

        /* is_new starts from a zeroed buffer.  A non-resident is_new==0 claim
         * would need a synchronous disk read, which no longer happens: every
         * such caller either finds the block pinned/resident (inode-block pin
         * after the write-lock grant pre-faults it; b+tree modify nodes faulted
         * + pinned by bt_run) or goes through the async path (diskfs_block_
         * claim_async) / the mount-time pump.  A miss here is therefore a bug. */
        chimera_diskfs_abort_if(!is_new,
                                "synchronous block_claim miss off=%lu -- "
                                "caller must pre-fault or use the async path",
                                device_offset);
        memset(blk->iov.data, 0, DISKFS_BLOCK_SIZE);

        blk->hash_next         = shard->buckets[bucket];
        shard->buckets[bucket] = blk;
    } else if (blk->on_lru) {
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_HIT);
        diskfs_block_lru_unlink(shard, blk);
    } else if (__atomic_load_n(&blk->state, __ATOMIC_ACQUIRE) == DISKFS_BLOCK_LOGGED) {
        /* COW: this buffer is still referenced by an un-pushed redo record (and
         * the tail-pusher will write it home), so it must stay immutable.  Fork
         * a private writable copy; the old buffer rides the record to its home
         * and is freed when the pusher releases it.  Done under the shard lock
         * so it serializes against the pusher's LOGGED->CLEAN transition. */
        struct diskfs_block_buf *old = blk->buf;
        struct diskfs_block_buf *new = diskfs_block_buf_alloc_locked(shard);

        memcpy(new->iov.data, old->iov.data, DISKFS_BLOCK_SIZE);
        blk->buf = new;
        blk->iov = new->iov;
        diskfs_block_buf_release_locked(shard, old);
        __atomic_store_n(&blk->state, DISKFS_BLOCK_CLEAN, __ATOMIC_RELEASE);
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_COW);
    } else {
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_HIT);
    }

    __atomic_add_fetch(&blk->pin_count, 1, __ATOMIC_ACQ_REL);
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
    return blk->iov.data;
} /* diskfs_sm_claim_block */


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
 * Free a device range as part of a transaction.  The FREE delta is journaled
 * now (it rides this txn's redo), but the in-memory free is deferred onto the
 * txn's pending list and applied only once the record is durable
 * (diskfs_txn_apply_frees) or discarded on abort (diskfs_txn_discard_frees).
 * This is required for metadata blocks (b+tree nodes), which unlike file data
 * are resident + pinned in the block cache: applying the free immediately
 * could hand the range to a concurrent allocation that then claims the stale,
 * still-pinned block.  Deferring to commit (block is LOGGED, unpinned by then)
 * makes a re-claim COW cleanly.
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

    /* Record the pending free only -- the FREE delta is journaled later, in the
     * suspendable pre-commit flush (diskfs_txn_flush_free_journals).  This is
     * because diskfs_txn_free_space runs inside the b+tree synchronous modify
     * (merge frees the emptied sibling, teardown frees nodes), which cannot
     * suspend on a cold journal block.  The in-memory free stays deferred to
     * commit (diskfs_txn_apply_frees). */
    f                  = malloc(sizeof(*f));
    f->device_id       = device_id;
    f->device_offset   = device_offset;
    f->length          = length;
    f->journaled       = 0;
    f->next            = txn->pending_frees;
    txn->pending_frees = f;
} /* diskfs_txn_free_space */


/*
 * Journal the FREE delta for every pending free recorded during the op's
 * modify.  A suspendable pre-commit phase (run on the worker before the txn is
 * handed to the intent-log thread): the delta write goes through the async
 * journal claim, so on a not-resident log block this parks the request (with
 * diskfs_commit_resume re-driving the commit) and returns SM_AGAIN.  Already-
 * journaled frees are flagged so a resumed flush continues where it left off.
 * Returns 0 when all frees are journaled.
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


/* Resume trampoline for a parked b+tree op: re-enter its driver. */
void
diskfs_bt_op_resume_cb(
    struct diskfs_thread *thread,
    void                 *arg)
{
    (void) thread;
    diskfs_bt_run((struct diskfs_bt_op *) arg);
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
    if (blk->on_lru) {
        diskfs_block_lru_unlink(shard, blk);
    }
    __atomic_add_fetch(&blk->pin_count, 1, __ATOMIC_ACQ_REL);
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
        blk                = diskfs_block_recycle(thread, shard);
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
        w->thread     = op->thread;
        w->resume     = diskfs_bt_op_resume_cb;
        w->arg        = op;
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
        blk                = diskfs_block_recycle(thread, shard);
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
            __atomic_add_fetch(&blk->pin_count, 1, __ATOMIC_ACQ_REL);
            pthread_mutex_unlock(&shard->lock);
            return blk;
        }

        /* Miss: publish a LOADING block, park, and issue the async read. */
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
    } else if (blk->on_lru) {
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_HIT);
        diskfs_block_lru_unlink(shard, blk);
    } else if (__atomic_load_n(&blk->state, __ATOMIC_ACQUIRE) == DISKFS_BLOCK_LOGGED) {
        /* COW (see diskfs_block_claim): fork a private writable copy. */
        struct diskfs_block_buf *old = blk->buf;
        struct diskfs_block_buf *new = diskfs_block_buf_alloc_locked(shard);

        memcpy(new->iov.data, old->iov.data, DISKFS_BLOCK_SIZE);
        blk->buf = new;
        blk->iov = new->iov;
        diskfs_block_buf_release_locked(shard, old);
        __atomic_store_n(&blk->state, DISKFS_BLOCK_CLEAN, __ATOMIC_RELEASE);
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_COW);
    } else {
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_HIT);
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

    __atomic_add_fetch(&blk->pin_count, 1, __ATOMIC_ACQ_REL);
    pthread_mutex_unlock(&shard->lock);
    return blk;
} /* diskfs_block_claim_async */


void
diskfs_inode_finish_write_pin(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode,
    diskfs_inode_cb_t     cb,
    void                 *private_data)
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

    blk = diskfs_block_claim_async(thread, device_id, device_offset, 0,
                                   diskfs_pin_cont_resume, c);
    if (!blk) {
        return;     /* suspended; diskfs_pin_cont_resume re-runs on load */
    }

    free(c);
    inode->block = blk;
    diskfs_txn_add_block(txn, blk);
    cb(inode, CHIMERA_VFS_OK, private_data);
} /* diskfs_inode_finish_write_pin */


static void
diskfs_pin_cont_resume(
    struct diskfs_thread *thread,
    void                 *arg)
{
    struct diskfs_pin_cont c = *(struct diskfs_pin_cont *) arg;

    (void) thread;
    free(arg);
    diskfs_inode_finish_write_pin(c.thread, c.txn, c.inode, c.cb, c.private_data);
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
 * Unpin all blocks held by this txn, transitioning them to new_state.  Also
 * clears each write-locked inode's cached block pointer (it is only valid
 * while the txn holds the block pinned; a later txn re-claims it).  Used at
 * commit (intent-log thread) and abort (worker).  Must run while the txn's
 * inode slots are still populated (before diskfs_txn_unlock_all).
 */
void
diskfs_txn_unpin_blocks(
    struct diskfs_txn      *txn,
    enum diskfs_block_state new_state)
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
        struct diskfs_block *blk = tb->block;

        n = tb->next;
        diskfs_block_unpin(thread, blk, new_state);
        free(tb);
        tb = n;
    }
} /* diskfs_txn_unpin_blocks */
