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
extern void               evpl_vfio_queue_dump(struct evpl_block_queue *bq);

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
    uint32_t                  num_deltas);

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
    uint32_t                  dev,
    uint64_t                  off);

static void
diskfs_il_free_record(
    struct diskfs_intent_log *il,
    struct diskfs_il_record  *rec)
{
    uint32_t i;

    /* This record is pushed home and trimmed.  For each block image it carried:
     *  - drop the submitting txn's claim pin (diskfs_push_unpin_block); this is
     *    the "decrement on flush/skip" -- the record is done whether its image
     *    was written home or superseded by a newer record, and it happens at
     *    record granularity here, after the home write, so it never races ahead
     *    of durability.  The block becomes CLEAN once its last pin drops.
     *  - release the record's snapshot reference on the buffer (buf->refs).
     * Done before releasing iovs[0], whose header region we read dev/off from. */
    {
        char *p = (char *) rec->iovs[0].data + sizeof(struct diskfs_redo_header);

        for (i = 0; i < rec->num_blocks; i++) {
            struct diskfs_redo_block_header *bh = (struct diskfs_redo_block_header *) p;

            p += sizeof(*bh);
            diskfs_push_unpin_block(il, bh->device_id, bh->device_offset);
            diskfs_block_buf_release(rec->block_bufs[i]);
        }
    }

    /* iovs[0] is the materialized header (a real SHARED buffer) -- release it.
     * iovs[1..] are non-owning GLOBAL slices of block-cache buffers (cloned at
     * zero cost in the IL builder); they own nothing, so they are NOT released
     * here -- the backing GLOBAL buffer is freed only at cache teardown. */
    evpl_iovec_release(il->push_evpl, &rec->iovs[0]);
    free(rec->block_bufs);
    free(rec->iovs);
    free(rec);
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
            e->owner->uncovered--;
            e->seq   = rec->seq;
            e->iov   = &rec->iovs[1 + i];
            e->owner = rec;
            rec->uncovered++;
            if (!e->inflight) {
                diskfs_ready_push(il, e);
            }
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
    uint32_t                  dev,
    uint64_t                  off)
{
    struct diskfs_shared      *shared = container_of(il, struct diskfs_shared, intent_log);
    struct diskfs_block_cache *cache  = shared->block_cache;
    struct diskfs_block_shard *shard  = diskfs_block_shard(cache, dev, off);
    uint32_t                   bucket = diskfs_block_bucket(dev, off);
    struct diskfs_block       *blk;

    pthread_mutex_lock(&shard->lock);
    blk = diskfs_block_lookup_locked(shard, bucket, dev, off);
    chimera_diskfs_abort_if(!blk,
                            "tail-push unpin: pinned block dev=%u off=%lu vanished",
                            dev, off);
    if (__atomic_sub_fetch(&blk->pin_count, 1, __ATOMIC_ACQ_REL) == 0) {
        __atomic_store_n(&blk->state, DISKFS_BLOCK_CLEAN, __ATOMIC_RELEASE);
        if (!blk->on_lru) {
            diskfs_block_lru_push_tail(shard, blk);
        }
    }
    pthread_mutex_unlock(&shard->lock);

    /* This pin drop may have produced a CLEAN LRU victim; wake any CoW fork
     * parked on this shard for want of a reclaimable buffer.  Cross-thread
     * (push thread -> worker doorbell), so self == NULL. */
    diskfs_block_buf_wake(NULL, shard);
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
    struct diskfs_redo_header *hdr    = (struct diskfs_redo_header *) rec->iovs[0].data;
    char                      *p;
    uint32_t                   i;
    int                        ready  = 1;

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
                                 il->handoff[hh & DISKFS_HANDOFF_RING_MASK]->offset,
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


/* Issue ready home writes up to the push watermark, one per unique block. */
static void
diskfs_push_issue(struct diskfs_intent_log *il)
{
    while (il->push_outstanding < DISKFS_PUSH_WATERMARK) {
        struct diskfs_pending *e = diskfs_ready_pop(il);

        if (!e) {
            break;
        }

        e->inflight     = 1;
        e->issued_seq   = e->seq;
        e->issued_owner = e->owner;
        e->owner->inflight_refs++;
        il->push_outstanding++;
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

    /* Release the record the in-flight write read from; free it if it has been
     * retired and no other in-flight write still reads it. */
    if (--io->inflight_refs == 0 && io->retired) {
        diskfs_il_free_record(il, io);
    }

    e->inflight = 0;

    if (e->seq > e->issued_seq) {
        /* A newer image arrived mid-flight: re-issue it (newest lands last). */
        diskfs_ready_push(il, e);
    } else {
        /* Durably home at its newest seq: drop the pending entry.  The owning
         * record needs one fewer block home -> may become coverable.  The block
         * is marked CLEAN later, when that record is trimmed and its claim pin
         * is dropped (diskfs_il_free_record -> diskfs_push_unpin_block); a block
         * stays pinned/DIRTY as long as any record still owes it home. */
        e->owner->uncovered--;
        diskfs_pending_remove(il, e);
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
        struct diskfs_il_record *rec = il->handoff[head & DISKFS_HANDOFF_RING_MASK];

        head++;
        rec->next          = NULL;
        rec->inflight_refs = 0;
        rec->retired       = 0;
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
        uint32_t                   ht, i;

        for (i = 0; i < rc->num_entries; i++) {
            struct diskfs_iq_channel *ch    = rc->entries[i].ch;
            struct diskfs_iq_entry   *entry = &rc->entries[i].entry;
            uint32_t                  cq_tail;

            prometheus_stopwatch_start(&entry->durable_time);
            diskfs_metric_time_sample(
                il->metrics.txn_latency[DISKFS_METRIC_TXN_SUBMIT_TO_DURABLE],
                &entry->submit_time);
            diskfs_metric_time_sample(
                il->metrics.txn_latency[DISKFS_METRIC_TXN_QUEUE_TO_DURABLE],
                &entry->enqueue_time);

            /* Record durable & recoverable: release the inode->block links and
             * the per-block txn structs (NOT the block pins -- each block's
             * claim pin is held until this record is pushed home and trimmed,
             * where the push thread drops it; see diskfs_il_free_record).  Then
             * return freed ranges to the allocator and ACK the transaction back
             * to its worker.  Inode locks were already released in
             * diskfs_il_write_redo once this record's log order was fixed, so
             * the hot parent-directory lock is not held across the log write.
             * The IL thread no longer touches any block reference/state field. */
            diskfs_txn_retire_blocks(entry->txn);
            diskfs_txn_apply_allocs(entry->txn);
            diskfs_txn_apply_frees(entry->txn);

            entry->status                                 = 0;
            cq_tail                                       = __atomic_load_n(&ch->cq.tail, __ATOMIC_RELAXED);
            ch->cq.entries[cq_tail & DISKFS_IQ_RING_MASK] = *entry;
            __atomic_store_n(&ch->cq.tail, cq_tail + 1, __ATOMIC_RELEASE);
            ch->cq_inflight--;
        }

        /* This record is durable and all its frees are applied; records retire
         * strictly in seq order, so durable_seq advances monotonically.  A
         * space-map checkpoint stamps its ckpt_seq from this. */
        __atomic_store_n(&il->durable_seq, rec->seq, __ATOMIC_RELEASE);

        /* Hand the durable record to the push thread, in log order.  The
         * hand-off ring is sized larger than the log can ever hold, so it
         * cannot fill before the log does. */
        ht = il->handoff_tail;
        chimera_diskfs_abort_if(ht - __atomic_load_n(&il->handoff_head, __ATOMIC_ACQUIRE) >=
                                DISKFS_HANDOFF_RING_SIZE, "intent-log hand-off ring overflow");
        il->handoff[ht & DISKFS_HANDOFF_RING_MASK] = rec;
        __atomic_store_n(&il->handoff_tail, ht + 1, __ATOMIC_RELEASE);
        handed_off = 1;

        slot->ctx  = NULL;
        slot->done = 0;
        free(rc->entries);
        free(rc);
        il->retire_head++;
    }

    diskfs_il_commit_metrics(il);

    if (handed_off) {
        evpl_ring_doorbell(&il->push_doorbell);
    }
} /* diskfs_redo_write_cb */

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
    uint32_t                  num_deltas)
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

    rec             = malloc(sizeof(*rec));
    rec->seq        = il->log_seq;
    rec->offset     = offset;
    rec->reclen     = reclen;
    rec->num_blocks = nblocks;
    rec->niov       = 1 + nblocks;
    rec->iovs       = malloc(rec->niov * sizeof(struct evpl_iovec));
    rec->block_bufs = calloc(nblocks, sizeof(*rec->block_bufs));
    rec->next       = NULL;

    /* iovs[0]: materialized header region (redo_header + per-block headers). */
    niov = evpl_iovec_alloc(il->evpl, hdr_len, DISKFS_BLOCK_SIZE, 1,
                            EVPL_IOVEC_FLAG_SHARED, &rec->iovs[0]);
    chimera_diskfs_abort_if(niov != 1, "redo header did not fit in one iovec (%d)", niov);

    ctx              = malloc(sizeof(*ctx));
    ctx->il          = il;
    ctx->entries     = malloc(num_entries * sizeof(*ctx->entries));
    ctx->num_entries = num_entries;
    ctx->rec         = rec;
    memcpy(ctx->entries, entries, num_entries * sizeof(*ctx->entries));

    p               = (char *) rec->iovs[0].data;
    hdr             = (struct diskfs_redo_header *) p;
    hdr->magic      = DISKFS_REDO_MAGIC;
    hdr->csum_lo    = 0;
    hdr->csum_hi    = 0;
    hdr->seq        = il->log_seq++;
    rec->seq        = hdr->seq;
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
diskfs_il_blocks_reclen(uint32_t nblocks, uint32_t num_deltas)
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


/* --- TEMP IL throughput instrumentation (collector is single-threaded, so
 * plain counters are safe).  Printed once/sec to the daemon log to find why the
 * redo pipeline runs shallow.  Remove after the investigation. --- */
static uint64_t g_il_calls, g_il_submits, g_il_txns, g_il_gate_redo,
                g_il_gate_retire, g_il_empty, g_il_sum_inflight;
static int      g_il_max_inflight;
static uint64_t g_il_last_print_ns;

static void
diskfs_il_dbg_tick(struct diskfs_intent_log *il)
{
    struct timespec ts;
    uint64_t        now;
    double          dt;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    now = (uint64_t) ts.tv_sec * 1000000000ull + (uint64_t) ts.tv_nsec;
    if (g_il_last_print_ns == 0) {
        g_il_last_print_ns = now;
        return;
    }
    if (now - g_il_last_print_ns < 1000000000ull) {
        return;
    }
    dt = (double) (now - g_il_last_print_ns) / 1e9;
    chimera_diskfs_info(
        "IL/s calls=%.0f submits=%.0f txns/rec=%.1f gate_redo=%.0f "
        "gate_retire=%.0f empty=%.0f avg_inflight=%.1f max_inflight=%d "
        "now_inflight=%d retire_depth=%u",
        g_il_calls / dt, g_il_submits / dt,
        g_il_submits ? (double) g_il_txns / (double) g_il_submits : 0.0,
        g_il_gate_redo / dt, g_il_gate_retire / dt, g_il_empty / dt,
        g_il_submits ? (double) g_il_sum_inflight / (double) g_il_submits : 0.0,
        g_il_max_inflight, il->redo_inflight,
        il->retire_tail - il->retire_head);
    g_il_calls = g_il_submits = g_il_txns = g_il_gate_redo = 0;
    g_il_gate_retire = g_il_empty = g_il_sum_inflight = 0;
    g_il_max_inflight  = 0;
    g_il_last_print_ns = now;
} /* diskfs_il_dbg_tick */

static int
diskfs_iq_process_batch(struct diskfs_intent_log *il)
{
    struct diskfs_redo_entry entries[DISKFS_IL_MAX_IOV];
    uint32_t                 sq_head[DISKFS_IL_MAX_CHANNELS];
    uint32_t                 sq_tail[DISKFS_IL_MAX_CHANNELS];
    uint32_t                 consumed[DISKFS_IL_MAX_CHANNELS];
    uint32_t                 batch_count  = 0;
    uint32_t                 batch_blocks = 0;
    uint32_t                 batch_deltas = 0;
    uint32_t                 start, rounds, pass, i;
    int                      stopped = 0;

    g_il_calls++;
    diskfs_il_dbg_tick(il);

    if (il->redo_inflight >= DISKFS_COMMIT_WATERMARK) {
        g_il_gate_redo++;
        return 0;
    }

    if (il->retire_tail - il->retire_head >= DISKFS_RETIRE_RING_SIZE) {
        g_il_gate_retire++;
        return 0;
    }

    if (il->num_channels == 0) {
        return 0;
    }

    for (i = 0; i < il->num_channels; i++) {
        struct diskfs_iq_channel *ch = il->channels[i];

        sq_head[i]  = __atomic_load_n(&ch->sq.head, __ATOMIC_RELAXED);
        sq_tail[i]  = __atomic_load_n(&ch->sq.tail, __ATOMIC_ACQUIRE);
        consumed[i] = 0;
    }

    start = il->log_seq % il->num_channels;

    for (rounds = 0; !stopped && batch_count < DISKFS_IL_MAX_IOV; rounds++) {
        int took = 0;

        for (pass = 0; pass < il->num_channels && batch_count < DISKFS_IL_MAX_IOV; pass++) {
            uint32_t                  idx = (start + pass) % il->num_channels;
            struct diskfs_iq_channel *ch  = il->channels[idx];
            struct diskfs_iq_entry   *slot;
            uint32_t                  nblocks, next_blocks;
            uint32_t                  ndeltas, next_deltas;
            uint32_t                  cq_tail, cq_head;
            uint64_t                  reclen;

            if (sq_head[idx] + consumed[idx] == sq_tail[idx]) {
                continue;
            }

            slot = &ch->sq.entries[(sq_head[idx] + consumed[idx]) &
                                   DISKFS_IQ_RING_MASK];
            nblocks     = diskfs_il_txn_blocks(slot->txn);
            next_blocks = batch_blocks + nblocks;
            ndeltas     = diskfs_il_txn_deltas(slot->txn);
            next_deltas = batch_deltas + ndeltas;

            /* Keep one normal record to one backend write.  Transactions larger
             * than the iov cap still go alone and use the segmented path. */
            if (batch_count > 0 && 1 + next_blocks > DISKFS_IL_MAX_IOV) {
                stopped = 1;
                break;
            }

            cq_tail = __atomic_load_n(&ch->cq.tail, __ATOMIC_RELAXED);
            cq_head = __atomic_load_n(&ch->cq.head, __ATOMIC_ACQUIRE);
            if ((cq_tail - cq_head) + ch->cq_inflight + consumed[idx] >=
                DISKFS_IQ_RING_SIZE) {
                continue;
            }

            reclen = diskfs_il_blocks_reclen(next_blocks, next_deltas);
            if (!diskfs_il_fits(il, reclen)) {
                if (batch_count > 0) {
                    stopped = 1;
                    break;
                }
                if (__atomic_load_n(&il->live_records, __ATOMIC_ACQUIRE) != 0) {
                    stopped = 1;
                    break;
                }
                __atomic_store_n(&il->log_tail, il->log_head, __ATOMIC_RELEASE);
                if (!diskfs_il_fits(il, reclen)) {
                    stopped = 1;
                    break;
                }
            }

            entries[batch_count].ch    = ch;
            entries[batch_count].entry = *slot;
            batch_count++;
            batch_blocks = next_blocks;
            batch_deltas = next_deltas;
            consumed[idx]++;
            took = 1;

            if (1 + batch_blocks > DISKFS_IL_MAX_IOV) {
                stopped = 1;
                break;
            }
        }

        if (!took) {
            break;
        }

        if (rounds >= DISKFS_IQ_RING_SIZE) {
            break;
        }
    }

    if (batch_count == 0) {
        g_il_empty++;
        return 0;
    }

    g_il_submits++;
    g_il_txns        += batch_count;
    g_il_sum_inflight += (uint64_t) il->redo_inflight;   /* depth at decision time */
    if (il->redo_inflight > g_il_max_inflight) {
        g_il_max_inflight = il->redo_inflight;
    }

    for (i = 0; i < batch_count; i++) {
        entries[i].entry.status = 0;
        prometheus_stopwatch_start(&entries[i].entry.submit_time);
        diskfs_metric_time_sample(
            il->metrics.txn_latency[DISKFS_METRIC_TXN_QUEUE_TO_SUBMIT],
            &entries[i].entry.enqueue_time);
    }

    for (i = 0; i < il->num_channels; i++) {
        if (consumed[i] == 0) {
            continue;
        }
        __atomic_store_n(&il->channels[i]->sq.head,
                         sq_head[i] + consumed[i],
                         __ATOMIC_RELEASE);
        il->channels[i]->cq_inflight += consumed[i];
    }

    diskfs_il_write_redo(il, entries, batch_count, batch_blocks, batch_deltas);
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
             * moment unregister_done is set, and retired txns dereference
             * their submitting thread -- acking with anything still in
             * flight would be a use-after-free.  The worker guarantees
             * quiescence (commits_inflight drained to zero) before
             * requesting unregistration; make a violation loud instead of
             * corrupting memory. */
            chimera_diskfs_abort_if(
                ch->cq_inflight != 0 ||
                __atomic_load_n(&ch->sq.tail, __ATOMIC_ACQUIRE) !=
                __atomic_load_n(&ch->sq.head, __ATOMIC_RELAXED),
                "intent-log channel unregistered with work in flight "
                "(cq_inflight=%u)", ch->cq_inflight);

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


/* Seq-cst re-scan for the poll-exit wakeup handshake (diskfs_iq_try_submit). */
static int
diskfs_il_has_sq_work(struct diskfs_intent_log *il)
{
    uint32_t i;

    for (i = 0; i < il->num_channels; i++) {
        struct diskfs_iq_channel *ch = il->channels[i];

        if (__atomic_load_n(&ch->sq.tail, __ATOMIC_SEQ_CST) !=
            __atomic_load_n(&ch->sq.head, __ATOMIC_SEQ_CST)) {
            return 1;
        }
    }
    return 0;
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


/* Push one txn onto this worker's IL submission queue.  Returns 1 on success,
 * 0 if the SQ is full.  The completion fires later from the CQ doorbell. */
int
diskfs_iq_try_submit(
    struct diskfs_thread  *thread,
    struct diskfs_txn     *txn,
    diskfs_txn_commit_cb_t cb,
    void                  *private_data)
{
    struct diskfs_shared     *shared = thread->shared;
    struct diskfs_iq_channel *ch     = thread->iq_channel;
    struct diskfs_iq_entry   *slot;
    uint32_t                  tail, head;

    tail = __atomic_load_n(&ch->sq.tail, __ATOMIC_RELAXED);
    head = __atomic_load_n(&ch->sq.head, __ATOMIC_ACQUIRE);

    if (tail - head >= DISKFS_IQ_RING_SIZE) {
        return 0;
    }

    slot               = &ch->sq.entries[tail & DISKFS_IQ_RING_MASK];
    slot->txn          = txn;
    slot->cb           = cb;
    slot->private_data = private_data;
    slot->status       = 0;
    prometheus_stopwatch_start(&slot->enqueue_time);

    /* Seq-cst so this store is ordered before the awake load below; pairs with
     * the commit thread's diskfs_il_poll_exit handshake. */
    __atomic_store_n(&ch->sq.tail, tail + 1, __ATOMIC_SEQ_CST);

    /* The commit thread polls every channel's SQ each loop iteration while it is
     * awake, so the wake doorbell is only needed to rouse it once it has gone to
     * sleep.  Skip the eventfd write in the common (awake) case. */
    if (!__atomic_load_n(&shared->intent_log.awake, __ATOMIC_SEQ_CST)) {
        evpl_ring_doorbell(&shared->intent_log.wake_doorbell);
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


/* Drain this worker's completion queue: deliver callbacks, release txns, drop
 * the poll-mode pin once no commits remain outstanding, and resume any commits
 * that parked on a full SQ.  Returns the number of completions drained.  Runs
 * every loop iteration via diskfs_iq_cq_poll (the fast path, while the worker is
 * poll-pinned) and also from the CQ doorbell as a backstop. */
int
diskfs_iq_drain_cq(struct diskfs_iq_channel *ch)
{
    struct diskfs_thread     *worker  = ch->worker;
    struct diskfs_intent_log *il      = &worker->shared->intent_log;
    uint32_t                  head    = __atomic_load_n(&ch->cq.head, __ATOMIC_RELAXED);
    uint32_t                  tail    = __atomic_load_n(&ch->cq.tail, __ATOMIC_ACQUIRE);
    int                       drained = 0;

    while (head != tail) {
        struct diskfs_iq_entry entry = ch->cq.entries[head & DISKFS_IQ_RING_MASK];
        head++;
        drained++;

        diskfs_metric_time_sample(
            worker->metrics.txn_latency[DISKFS_METRIC_TXN_QUEUE_TO_CALLBACK],
            &entry.enqueue_time);
        diskfs_metric_time_sample(
            worker->metrics.txn_latency[DISKFS_METRIC_TXN_DURABLE_TO_CALLBACK],
            &entry.durable_time);

        /* The txn's logical inode locks were already dropped by the intent
         * log thread (diskfs_iq_process_channel); just deliver completion. */
        entry.cb(entry.txn, entry.status, entry.private_data);
        diskfs_txn_release(entry.txn);

        /* Last outstanding commit done -> release the poll-mode pin taken in
         * diskfs_txn_commit_finish so the worker may sleep again when idle. */
        if (--worker->commits_inflight == 0) {
            evpl_poll_unpin(worker->evpl);
        }
    }

    if (drained > 0) {
        __atomic_store_n(&ch->cq.head, head, __ATOMIC_RELEASE);
        /* Freeing CQ space may let the IL resume a channel it deferred; it sees
         * that on its next poll if awake, so only rouse it if it is asleep. */
        if (!__atomic_load_n(&il->awake, __ATOMIC_SEQ_CST)) {
            evpl_ring_doorbell(&il->wake_doorbell);
        }
    }

    /* The IL has now consumed SQ entries (freeing space), so retry any commits
     * that parked on a full SQ.  No-op when none are waiting. */
    diskfs_iq_resume_commit_waiters(worker);
    return drained;
} /* diskfs_iq_drain_cq */


/* Backstop: drain on the doorbell (rarely rung now -- the worker polls the CQ
 * every iteration while pinned). */
void
diskfs_iq_cq_doorbell_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct diskfs_iq_channel *ch = container_of(doorbell,
                                                struct diskfs_iq_channel,
                                                cq_doorbell);

    (void) evpl;
    diskfs_iq_drain_cq(ch);
} /* diskfs_iq_cq_doorbell_cb */


/* Per-iteration completion poll.  While a commit is outstanding the worker is
 * pinned in poll mode (diskfs_txn_commit_finish), so completions are reaped
 * within a loop iteration instead of waiting for the doorbell at the spin
 * boundary. */
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
    il->handoff     = calloc(DISKFS_HANDOFF_RING_SIZE, sizeof(*il->handoff));
    __atomic_store_n(&il->handoff_head, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&il->handoff_tail, 0, __ATOMIC_RELAXED);

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

    evpl_remove_poll(evpl, il->sq_poll);
    evpl_remove_doorbell(evpl, &il->wake_doorbell);
    if (il->log_queue) {
        evpl_block_close_queue(evpl, il->log_queue);
    }
    free(il->retire);
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

    while ((p = il->pfree)) {
        il->pfree = p->rnext;
        free(p);
    }
    free(il->phash);
} /* diskfs_il_push_thread_shutdown */


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
            XXH128_hash_t snap_hash;

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
