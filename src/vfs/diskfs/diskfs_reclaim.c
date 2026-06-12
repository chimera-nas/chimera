// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Background space reclaim: the durable sharded orphan list recording every
 * deleted inode, the incremental drainer that frees extents and destructs
 * b+trees in bounded batches, the dedicated reclaim worker pool, mount-time
 * orphan recovery, and runtime AG-log condensation.
 */

#include "diskfs_internal.h"

/* Forward declarations (definitions below, in call-graph order) */

static void
diskfs_drain_enqueue(
    struct diskfs_thread *thread,
    uint64_t              inum,
    uint32_t              gen);

static void
diskfs_drain_kick(
    struct diskfs_thread *thread);

static void
diskfs_drain_complete(
    struct diskfs_drain *d);

static void
diskfs_drain_acquired_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *priv);

static void
diskfs_drain_begin(
    struct diskfs_drain *d);

static void
diskfs_drain_committed_cb(
    struct diskfs_txn *txn,
    int                status,
    void              *priv);

static void
diskfs_drain_final_cb(
    struct diskfs_txn *txn,
    int                status,
    void              *priv);

static void
diskfs_drain_after_unrecord(
    void *priv);

static void
diskfs_drain_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *priv);

static void
diskfs_drain_looked_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *priv);

static void
diskfs_drain_step(
    struct diskfs_drain *d);

static void
diskfs_reclaim_doorbell_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell);

static void
diskfs_reclaim_submit_job(
    struct diskfs_shared      *shared,
    struct diskfs_reclaim_job *j);

static void
diskfs_reclaim_submit(
    struct diskfs_shared *shared,
    uint64_t              inum,
    uint32_t              gen);

static void *
diskfs_reclaim_thread_init(
    struct evpl *evpl,
    void        *private_data);

static void
diskfs_reclaim_thread_shutdown(
    struct evpl *evpl,
    void        *private_data);

static void
diskfs_condense_finish(
    struct evpl *evpl,
    int          status,
    void        *private_data);

static void
diskfs_condense_body_done(
    struct evpl *evpl,
    int          status,
    void        *private_data);

static void
diskfs_condense_try(
    struct diskfs_condense *c);

static void
diskfs_condense_start(
    struct diskfs_reclaim_worker *w,
    uint32_t                      device_id,
    uint32_t                      ag_index);

static void
diskfs_inode_orphaned_recorded_cb(
    void *priv);

static void
diskfs_orphan_scan_node(
    struct diskfs_thread      *thread,
    struct diskfs_mount_io    *io,
    void                      *buf,
    uint32_t                   base,
    struct diskfs_orphan_ent **arr,
    uint32_t                  *n,
    uint32_t                  *cap);


/* Queue a deleted (nlink==0) large inode for background reclaim.  Its inode
 * struct must stay resident until drained -- nlink==0 keeps A5 from evicting
 * it -- and must NOT be diskfs_inode_free'd by the caller (the drainer does
 * that at the end). */
static void
diskfs_drain_enqueue(
    struct diskfs_thread *thread,
    uint64_t              inum,
    uint32_t              gen)
{
    struct diskfs_drain *d;

    /* Test/safety knob: skip the in-session drain.  The durable orphan entry
     * was already recorded by the unlink, so the next mount's scan reclaims
     * it -- letting a remount deterministically exercise crash-resume. */
    if (unlikely(getenv("DISKFS_DRAIN_DISABLE") != NULL)) {
        return;
    }

    d = calloc(1, sizeof(*d));

    d->thread = thread;
    d->inum   = inum;
    d->gen    = gen;
    if (thread->drain_tail) {
        thread->drain_tail->next = d;
    } else {
        thread->drain_head = d;
    }
    thread->drain_tail = d;
    diskfs_drain_kick(thread);
} /* diskfs_drain_enqueue */


static void
diskfs_drain_kick(struct diskfs_thread *thread)
{
    struct diskfs_drain *d;

    if (thread->draining || !thread->drain_head) {
        return;
    }
    d                  = thread->drain_head;
    thread->drain_head = d->next;
    if (!thread->drain_head) {
        thread->drain_tail = NULL;
    }
    d->next          = NULL;
    thread->draining = 1;
    diskfs_drain_begin(d);
} /* diskfs_drain_kick */


static void
diskfs_drain_complete(struct diskfs_drain *d)
{
    struct diskfs_thread *thread = d->thread;

    free(d);
    thread->draining = 0;
    diskfs_drain_kick(thread);
} /* diskfs_drain_complete */


static void
diskfs_drain_acquired_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *priv)
{
    struct diskfs_drain *d = priv;

    /* The acquire parked behind the unlink's write lock; once granted the
     * inode is durably nlink==0.  If the unlink aborted (or it was already
     * reclaimed in a prior run), it isn't really gone -- skip it. */
    if (status != CHIMERA_VFS_OK || inode->nlink != 0) {
        diskfs_txn_abort(d->txn);
        diskfs_drain_complete(d);
        return;
    }
    d->inode = inode;
    d->batch = 0;
    diskfs_drain_step(d);
} /* diskfs_drain_acquired_cb */


static void
diskfs_drain_begin(struct diskfs_drain *d)
{
    d->txn = diskfs_txn_begin(d->thread, DISKFS_TXN_WRITE);
    diskfs_inode_acquire(d->thread, d->txn, d->inum, d->gen,
                         DISKFS_INODE_LOCK_WRITE, diskfs_drain_acquired_cb, d);
} /* diskfs_drain_begin */


static void
diskfs_drain_committed_cb(
    struct diskfs_txn *txn,
    int                status,
    void              *priv)
{
    struct diskfs_drain *d = priv;

    (void) txn;
    (void) status;
    diskfs_drain_begin(d);     /* next batch: fresh txn + re-acquire */
} /* diskfs_drain_committed_cb */


static void
diskfs_drain_final_cb(
    struct diskfs_txn *txn,
    int                status,
    void              *priv)
{
    struct diskfs_drain       *d      = priv;
    struct diskfs_shared      *shared = d->thread->shared;
    struct diskfs_inode_shard *shard  = diskfs_inode_shard(shared, d->inum);
    struct diskfs_inode       *inode;

    (void) txn;
    (void) status;

    /* The retire txn is durable and its locks are released; drop the dead
     * struct from the cache so it neither accumulates nor lingers to collide
     * with a reallocation of the inum.  Straggling lookups by the old (or
     * any) generation fault from disk and get ENOENT from the tombstone /
     * inum check there. */
    pthread_mutex_lock(&shard->lock);
    rb_tree_query_exact(&shard->inodes, d->inum, inum, inode);
    if (inode && inode->nlink == 0 && inode->refcnt == 0 &&
        !inode->writer && !inode->readers && !inode->wait_head) {
        diskfs_inode_lru_unlink(shard, inode);
        rb_tree_remove(&shard->inodes, &inode->node);
        shard->ninodes--;
        diskfs_inode_struct_free(inode);
    }
    pthread_mutex_unlock(&shard->lock);

    diskfs_drain_complete(d);
} /* diskfs_drain_final_cb */


/* The durable orphan entry is removed; finish retiring the inode in the same
 * transaction and commit: the dinode is logged one last time (gen bumped,
 * nlink 0 -- a tombstone for any straggling pre-reuse fault) and the 4 KiB
 * home block itself is returned to the space map.  Generation reuse is safe:
 * generations are drawn from the global epoch counter, so whatever this inum
 * becomes next carries a generation no file handle has ever seen. */
static void
diskfs_drain_after_unrecord(void *priv)
{
    struct diskfs_drain *d = priv;
    uint32_t             dev;
    uint64_t             off = sm_inum_to_device_offset(d->thread->shared->space_map,
                                                        d->inum, &dev);

    diskfs_txn_pin_inode_block(d->thread, d->txn, d->inode, 0);
    diskfs_inode_return_reservation(d->thread, d->txn, d->inode);
    diskfs_inode_free(d->thread, d->inode);
    diskfs_txn_free_space(d->thread, d->txn, dev, off, DISKFS_BLOCK_SIZE);
    diskfs_txn_commit(d->txn, diskfs_drain_final_cb, d);
} /* diskfs_drain_after_unrecord */


static void
diskfs_drain_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *priv)
{
    struct diskfs_drain *d = priv;

    (void) result;
    diskfs_bt_op_free(d->thread, op);

    if (++d->batch >= DISKFS_DRAIN_BATCH) {
        diskfs_txn_commit(d->txn, diskfs_drain_committed_cb, d);
    } else {
        diskfs_drain_step(d);
    }
} /* diskfs_drain_removed_cb */


static void
diskfs_drain_looked_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *priv)
{
    struct diskfs_drain *d = priv;
    struct diskfs_bt_op *rop;

    if (result < 0) {
        /* Tree empty: remove the durable orphan entry, then retire the inode in
         * the same txn.  Removing the orphan entry last means a crash before
         * this commit just re-drains on the next mount (idempotent); the orphan
         * inode (3) is acquired last (leaf -> no deadlock). */
        diskfs_bt_op_free(d->thread, op);
        diskfs_orphan_op_start(d->thread, d->txn, d->inum, d->gen, 1 /* remove */,
                               diskfs_drain_after_unrecord, d);
        return;
    }

    /* Free a file extent's backing data before removing the record.  The
     * remove reclaims any emptied b+tree node blocks (generic, any entry). */
    if (d->found_key.type == DISKFS_REC_EXTENT) {
        struct diskfs_extent_rec *e = (struct diskfs_extent_rec *) d->recbuf;

        diskfs_thread_free_space(d->thread, d->txn, e->device_id, e->device_offset,
                                 SM_ALIGN_UP(e->length));
    }
    diskfs_bt_op_free(d->thread, op);

    rop = diskfs_bt_op_alloc(d->thread);
    if (diskfs_bt_remove_async(rop, d->thread, d->txn, d->inode, &d->found_key,
                               diskfs_drain_removed_cb, d)) {
        diskfs_drain_removed_cb(rop, rop->result, d);
    }
} /* diskfs_drain_looked_cb */


static void
diskfs_drain_step(struct diskfs_drain *d)
{
    struct diskfs_bt_op *op  = diskfs_bt_op_alloc(d->thread);
    struct diskfs_bt_key key = { .type = 0, .subkey = 0 };   /* min key */

    if (diskfs_bt_lookup_async(op, d->thread, d->inode, DISKFS_BT_OP_LOOKUP_GE,
                               &key, &d->found_key, d->recbuf, sizeof(d->recbuf),
                               diskfs_drain_looked_cb, d)) {
        diskfs_drain_looked_cb(op, op->result, d);
    }
} /* diskfs_drain_step */


/* Pop every queued job and hand it to this worker's drain machinery. */
static void
diskfs_reclaim_doorbell_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct diskfs_reclaim_worker *w = container_of(doorbell,
                                                   struct diskfs_reclaim_worker,
                                                   doorbell);
    struct diskfs_reclaim_job    *jobs, *j;

    (void) evpl;

    pthread_mutex_lock(&w->lock);
    jobs    = w->head;
    w->head = NULL;
    w->tail = NULL;
    pthread_mutex_unlock(&w->lock);

    while (jobs) {
        j    = jobs;
        jobs = j->next;
        if (j->type == DISKFS_RECLAIM_JOB_CONDENSE) {
            diskfs_condense_start(w, j->device_id, j->ag_index);
        } else {
            diskfs_drain_enqueue(w->ctx, j->inum, j->gen);
        }
        free(j);
    }
} /* diskfs_reclaim_doorbell_cb */


/*
 * Queue a deleted (nlink==0, refcnt==0) inode for background reclaim on the
 * worker pool, round-robin.  Safe from any thread.  The durable orphan record
 * already exists (inserted by the unlink/create txn), so a crash before the
 * drain finishes is recovered by the next mount's orphan scan.
 */
static void
diskfs_reclaim_submit_job(
    struct diskfs_shared      *shared,
    struct diskfs_reclaim_job *j)
{
    struct diskfs_reclaim        *r = shared->reclaim;
    struct diskfs_reclaim_worker *w;
    uint32_t                      idx;

    /* Pool tearing down (a worker's own shutdown flush can drop a last
    * reference): skip -- the durable orphan record makes the next mount's
    * scan pick the inode up.  (Condense jobs cannot arrive here during
    * teardown: every journaling request completed before it began.) */
    if (__atomic_load_n(&r->shutdown, __ATOMIC_ACQUIRE)) {
        free(j);
        return;
    }

    idx = __atomic_fetch_add(&r->rr, 1, __ATOMIC_RELAXED) % r->nworkers;
    w   = &r->workers[idx];

    pthread_mutex_lock(&w->lock);
    if (w->tail) {
        w->tail->next = j;
    } else {
        w->head = j;
    }
    w->tail = j;
    pthread_mutex_unlock(&w->lock);

    evpl_ring_doorbell(&w->doorbell);
} /* diskfs_reclaim_submit_job */


static void
diskfs_reclaim_submit(
    struct diskfs_shared *shared,
    uint64_t              inum,
    uint32_t              gen)
{
    struct diskfs_reclaim_job *j = calloc(1, sizeof(*j));

    j->type = DISKFS_RECLAIM_JOB_DRAIN;
    j->inum = inum;
    j->gen  = gen;
    diskfs_reclaim_submit_job(shared, j);
} /* diskfs_reclaim_submit */


static void *
diskfs_reclaim_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_reclaim_worker *w = private_data;

    w->ctx = diskfs_thread_init(evpl, w->shared);
    evpl_add_doorbell(evpl, &w->doorbell, diskfs_reclaim_doorbell_cb);
    __atomic_store_n(&w->ready, 1, __ATOMIC_RELEASE);
    return w;
} /* diskfs_reclaim_thread_init */


static void
diskfs_reclaim_thread_shutdown(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_reclaim_worker *w = private_data;
    struct diskfs_reclaim_job    *j;

    /* Feed any still-queued jobs to the drain machinery first;
     * diskfs_thread_destroy pumps until every drain completes, so unmount
     * finishes the reclaim backlog rather than re-scanning it next mount. */
    diskfs_reclaim_doorbell_cb(evpl, &w->doorbell);

    /* Finish in-flight AG-log condensations (their parked journaling ops
     * belong to requests that cannot complete until the flip). */
    while (w->condenses > 0) {
        evpl_continue(evpl);
    }

    evpl_remove_doorbell(evpl, &w->doorbell);
    diskfs_thread_destroy(w->ctx);

    /* Anything submitted after the doorbell drain is recovered by the next
     * mount's orphan scan (the records are durable). */
    while ((j = w->head)) {
        w->head = j->next;
        free(j);
    }
    pthread_mutex_destroy(&w->lock);
} /* diskfs_reclaim_thread_shutdown */


void
diskfs_reclaim_create(struct diskfs_shared *shared)
{
    struct diskfs_reclaim *r = calloc(1, sizeof(*r));
    uint32_t               i;

    r->nworkers = shared->reclaim_threads ? shared->reclaim_threads
                                          : DISKFS_RECLAIM_THREADS_DEFAULT;
    r->workers = calloc(r->nworkers, sizeof(*r->workers));

    for (i = 0; i < r->nworkers; i++) {
        struct diskfs_reclaim_worker *w = &r->workers[i];

        w->shared = shared;
        pthread_mutex_init(&w->lock, NULL);
        w->thread = evpl_thread_create(NULL, diskfs_reclaim_thread_init,
                                       diskfs_reclaim_thread_shutdown, w);
        while (!__atomic_load_n(&w->ready, __ATOMIC_ACQUIRE)) {
            /* spin briefly; context construction is fast */
        }
    }
    shared->reclaim = r;
} /* diskfs_reclaim_create */


void
diskfs_reclaim_destroy(struct diskfs_shared *shared)
{
    struct diskfs_reclaim *r = shared->reclaim;
    uint32_t               i;

    if (!r) {
        return;
    }
    __atomic_store_n(&r->shutdown, 1, __ATOMIC_RELEASE);
    for (i = 0; i < r->nworkers; i++) {
        evpl_thread_destroy(r->workers[i].thread);
    }
    free(r->workers);
    free(r);
    shared->reclaim = NULL;
} /* diskfs_reclaim_destroy */


static void
diskfs_condense_finish(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct diskfs_condense       *c      = private_data;
    struct diskfs_reclaim_worker *w      = c->worker;
    struct diskfs_shared         *shared = w->shared;
    struct diskfs_ag_wait        *agw    = &shared->agw[c->device_id][c->ag_index];
    struct diskfs_block_waiter   *waiters, *wt;
    uint32_t                      base_count;

    chimera_diskfs_abort_if(status != 0,
                            "AG-log condense header write failed: %d", status);

    base_count = ((struct sm_ag_log_header *) c->scratch)->base_count;
    space_map_condense_commit(shared->space_map, c->device_id, c->ag_index,
                              base_count);

    chimera_diskfs_info("AG %u/%u log condensed (%u free extents)",
                        c->device_id, c->ag_index, base_count);

    /* Re-drive everything parked behind the condensation. */
    pthread_mutex_lock(&agw->lock);
    waiters   = agw->head;
    agw->head = NULL;
    pthread_mutex_unlock(&agw->lock);

    while (waiters) {
        wt      = waiters;
        waiters = wt->next;
        diskfs_block_waiter_dispatch(w->ctx, wt);
    }

    evpl_iovec_release(evpl, &c->hdr);
    if (c->body_len) {
        evpl_iovec_release(evpl, &c->body);
    }
    free(c->scratch);
    w->condenses--;
    free(c);
} /* diskfs_condense_finish */


/* Body blocks durable: write the header block (slot becomes valid). */
static void
diskfs_condense_body_done(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct diskfs_condense       *c = private_data;
    struct diskfs_reclaim_worker *w = c->worker;

    chimera_diskfs_abort_if(status != 0,
                            "AG-log condense body write failed: %d", status);

    memcpy(c->hdr.data, c->scratch, DISKFS_BLOCK_SIZE);
    evpl_block_write(evpl, w->ctx->queue[c->device_id], &c->hdr, 1,
                     c->slot_off, 1 /* FUA */, diskfs_condense_finish, c);
} /* diskfs_condense_body_done */


static void
diskfs_condense_try(struct diskfs_condense *c)
{
    struct diskfs_reclaim_worker *w      = c->worker;
    struct diskfs_shared         *shared = w->shared;
    struct evpl                  *evpl   = w->ctx->evpl;
    uint64_t                      payload, aligned;

    space_map_condense_prepare(shared->space_map, c->device_id,
                               c->ag_index, c->scratch,
                               &c->slot_off, &payload);

    aligned = (payload + DISKFS_BLOCK_SIZE - 1) & ~((uint64_t) DISKFS_BLOCK_SIZE - 1);
    memset(c->scratch + payload, 0, aligned - payload);
    c->body_len = aligned - DISKFS_BLOCK_SIZE;

    /* Header block (written last) makes the slot valid; everything after it
     * goes first. */
    evpl_iovec_alloc(evpl, DISKFS_BLOCK_SIZE, DISKFS_BLOCK_SIZE, 1, 0, &c->hdr);

    if (c->body_len) {
        evpl_iovec_alloc(evpl, c->body_len, DISKFS_BLOCK_SIZE, 1, 0, &c->body);
        memcpy(c->body.data, c->scratch + DISKFS_BLOCK_SIZE, c->body_len);
        evpl_block_write(evpl, w->ctx->queue[c->device_id], &c->body, 1,
                         c->slot_off + DISKFS_BLOCK_SIZE, 1 /* FUA */,
                         diskfs_condense_body_done, c);
    } else {
        diskfs_condense_body_done(evpl, 0, c);
    }
} /* diskfs_condense_try */


static void
diskfs_condense_start(
    struct diskfs_reclaim_worker *w,
    uint32_t                      device_id,
    uint32_t                      ag_index)
{
    struct diskfs_condense *c = calloc(1, sizeof(*c));

    c->worker    = w;
    c->device_id = device_id;
    c->ag_index  = ag_index;
    c->scratch   = malloc(SM_AG_LOG_SLOT_SIZE);
    w->condenses++;

    diskfs_condense_try(c);
} /* diskfs_condense_start */


/* sm_journal bridge (called under the AG lock; see space_map.h). */
void
diskfs_sm_ag_condense(
    void    *user,
    uint32_t device_id,
    uint32_t ag_index)
{
    struct diskfs_sm_jnl      *c = user;
    struct diskfs_reclaim_job *j = calloc(1, sizeof(*j));

    j->type      = DISKFS_RECLAIM_JOB_CONDENSE;
    j->device_id = device_id;
    j->ag_index  = ag_index;
    diskfs_reclaim_submit_job(c->thread->shared, j);
} /* diskfs_sm_ag_condense */


void
diskfs_sm_ag_park(
    void    *user,
    uint32_t device_id,
    uint32_t ag_index)
{
    struct diskfs_sm_jnl       *c      = user;
    struct diskfs_shared       *shared = c->thread->shared;
    struct diskfs_ag_wait      *agw    = &shared->agw[device_id][ag_index];
    struct diskfs_block_waiter *w      = diskfs_block_waiter_alloc(c->thread);

    w->thread = c->thread;
    w->resume = c->resume;
    w->arg    = c->resume_arg;

    pthread_mutex_lock(&agw->lock);
    w->next   = agw->head;
    agw->head = w;
    pthread_mutex_unlock(&agw->lock);
} /* diskfs_sm_ag_park */


static void
diskfs_inode_orphaned_recorded_cb(void *priv)
{
    struct diskfs_inode_orphaned_ctx c     = *(struct diskfs_inode_orphaned_ctx *) priv;
    struct diskfs_inode_shard       *shard =
        diskfs_inode_shard(c.thread->shared, c.inode->inum);
    int                              reclaim;

    free(priv);

    /* Last reference already gone (no open handles, no deferred-mtime pin):
     * hand the inode to the reclaim workers now.  Their drain parks on the
     * inode's write lock, which this txn holds until it is durable, so the
     * burn-down can't start before the orphan record (and the nlink=0 dinode)
     * are recoverable.  Otherwise the final ref drop (close / pin release)
     * submits it.  (A duplicate submit from a racing ref drop is benign: the
     * second drain finds the bumped generation and skips.) */
    pthread_mutex_lock(&shard->lock);
    reclaim = (c.inode->refcnt == 0);
    pthread_mutex_unlock(&shard->lock);

    if (reclaim) {
        diskfs_reclaim_submit(c.thread->shared, c.inode->inum, c.inode->gen);
    }

    c.done(c.priv);
} /* diskfs_inode_orphaned_recorded_cb */


/*
 * Call when `inode`'s nlink just dropped to 0 inside `txn` (which holds its
 * write lock): drop the namespace's base reference, record the inode on the
 * durable orphan list (the record rides this txn's redo, so a crash at any
 * later point is recovered by the next mount's orphan scan), and queue it for
 * background reclaim once nothing references it.  `done(priv)` fires when the
 * record is in place; the caller continues (attr mapping, commit) from there.
 *
 * Invariant: an inode holds a base reference iff nlink > 0, so refcnt of a
 * deleted inode counts only open handles + transient pins, and exactly one
 * ref-drop site observes zero.
 */
void
diskfs_inode_orphaned(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode,
    void (               *done )(void *priv),
    void                 *priv)
{
    struct diskfs_inode_orphaned_ctx *c     = malloc(sizeof(*c));
    struct diskfs_inode_shard        *shard =
        diskfs_inode_shard(thread->shared, inode->inum);

    /* Shard-locked like every other ref drop, so it can't race a concurrent
     * deferred-mtime pin release. */
    pthread_mutex_lock(&shard->lock);
    --inode->refcnt;
    pthread_mutex_unlock(&shard->lock);

    c->thread = thread;
    c->inode  = inode;
    c->done   = done;
    c->priv   = priv;

    diskfs_orphan_op_start(thread, txn, inode->inum, inode->gen, 0 /* insert */,
                           diskfs_inode_orphaned_recorded_cb, c);
} /* diskfs_inode_orphaned */


/* A non-namespace reference (open handle, deferred-mtime pin, commit pin)
 * was dropped; if it was the last one on a deleted inode, reclaim it.  The
 * decrement runs under the inode's shard lock so it can't race the deferred-
 * mtime pin release (which holds no inode lock); a live inode that became
 * idle joins the recycle LRU as before. */
void
diskfs_inode_ref_drop(
    struct diskfs_thread *thread,
    struct diskfs_inode  *inode)
{
    struct diskfs_inode_shard *shard = diskfs_inode_shard(thread->shared,
                                                          inode->inum);
    int                        reclaim;

    pthread_mutex_lock(&shard->lock);
    --inode->refcnt;
    reclaim = (inode->refcnt == 0 && inode->nlink == 0);
    if (diskfs_inode_idle(inode) && !inode->on_lru) {
        diskfs_inode_lru_push_tail(shard, inode);
    }
    pthread_mutex_unlock(&shard->lock);

    if (reclaim) {
        diskfs_reclaim_submit(thread->shared, inode->inum, inode->gen);
    }
} /* diskfs_inode_ref_drop */


/* Collect every orphan entry in the orphan-list inode's b+tree (sync walk;
 * runs once at mount, single-threaded).  Generic recursion over node levels. */
static void
diskfs_orphan_scan_node(
    struct diskfs_thread      *thread,
    struct diskfs_mount_io    *io,
    void                      *buf,
    uint32_t                   base,
    struct diskfs_orphan_ent **arr,
    uint32_t                  *n,
    uint32_t                  *cap)
{
    struct diskfs_bt_node_hdr *h = diskfs_bt_hdr(buf, base);
    int                        i;

    if (h->level > 0) {
        struct diskfs_bt_islot *isl = diskfs_bt_islots(buf, base);

        for (i = 0; i < h->nitems; i++) {
            uint32_t cdev;
            uint64_t coff = sm_inum_to_device_offset(thread->shared->space_map,
                                                     isl[i].child, &cdev);
            /* Read the child node from disk through the pump (the orphan tree
             * is not dirty at mount).  Heap buffer -- recursion depth can reach
             * the tree height, too deep for a stack block per level. */
            uint8_t *cbuf = malloc(DISKFS_BLOCK_SIZE);

            if (diskfs_mount_io_read(io, cdev, cbuf, DISKFS_BLOCK_SIZE, coff) == 0) {
                diskfs_orphan_scan_node(thread, io, cbuf, 0, arr, n, cap);
            }
            free(cbuf);
        }
    } else {
        struct diskfs_bt_lslot *s = diskfs_bt_lslots(buf, base);

        for (i = 0; i < h->nitems; i++) {
            if (s[i].key.type != DISKFS_REC_ORPHAN) {
                continue;
            }
            if (*n == *cap) {
                *cap *= 2;
                *arr  = realloc(*arr, *cap * sizeof(**arr));
            }
            (*arr)[*n].inum = s[i].key.subkey;
            (*arr)[*n].gen  = *(uint32_t *) ((char *) buf + base + s[i].off);
            (*n)++;
        }
    }
} /* diskfs_orphan_scan_node */


void
diskfs_orphan_scan(struct diskfs_thread *thread)
{
    struct diskfs_shared     *shared = thread->shared;
    struct diskfs_inode      *odir;
    struct diskfs_mount_io   *io;
    uint8_t                  *obuf;
    struct diskfs_orphan_ent *arr;
    uint32_t                  n = 0, cap = 16, i;
    uint32_t                  dev;
    uint64_t                  off;

    pthread_mutex_lock(&shared->lock);
    if (shared->orphans_scanned) {
        pthread_mutex_unlock(&shared->lock);
        return;
    }
    shared->orphans_scanned = 1;
    pthread_mutex_unlock(&shared->lock);

    /* All reads here go through a transient evpl pump (VFIO-safe); the orphan
     * tree is not dirty at mount, so reading the on-disk image is correct. */
    io = diskfs_mount_io_open(shared);

    /* Remount fault-in: bootstrap (mkfs) seeds the reserved root inode into
     * cache, but a remount skips bootstrap, so the root lives only on disk.
     * Seed it synchronously here -- the MOUNT op's own walk would otherwise
     * fault it in with an async read that the mount-time context never pumps
     * to completion, hanging the mount.  A freshly bootstrapped FS already has
     * it resident, so this is a cheap cache hit. */
    diskfs_inode_load_sync(thread, io, shared->root_inum, shared->root_gen, 0);

    /* Load each orphan-list shard inode (nlink 1) and read its tree from its
     * home block, collecting every recorded orphan inum + gen. */
    obuf = malloc(DISKFS_BLOCK_SIZE);
    arr  = malloc(cap * sizeof(*arr));
    for (i = 0; i < DISKFS_ORPHAN_SHARDS; i++) {
        uint64_t oinum = DISKFS_ORPHAN_INUM_BASE + i;

        odir = diskfs_inode_load_sync(thread, io, oinum, DISKFS_ORPHAN_GEN, 0);
        if (!odir) {
            continue;     /* not yet created (no orphans possible) */
        }

        off = sm_inum_to_device_offset(shared->space_map, oinum, &dev);
        if (diskfs_mount_io_read(io, dev, obuf, DISKFS_BLOCK_SIZE, off) == 0) {
            diskfs_orphan_scan_node(thread, io, obuf, DISKFS_BT_ROOT_BASE,
                                    &arr, &n, &cap);
        }
    }
    free(obuf);

    for (i = 0; i < n; i++) {
        /* Reload the orphaned (nlink==0) inode into cache, then enqueue it;
         * the drainer resumes its (possibly partially-drained) tree. */
        diskfs_inode_load_sync(thread, io, arr[i].inum, arr[i].gen, 1 /* allow_orphan */);
        diskfs_reclaim_submit(thread->shared, arr[i].inum, arr[i].gen);
    }
    free(arr);
    diskfs_mount_io_close(io);

    if (n) {
        chimera_diskfs_info("orphan recovery: re-enqueued %u inode(s) for drain", n);
    }
} /* diskfs_orphan_scan */


/*
 * Remove a key from an inode's b+tree, maintaining the B+tree invariants:
 * the leaf heap is compacted, parent separators are kept exact, and
 * underflowing non-root nodes borrow/merge with a sibling (propagating up).
 * Returns 1 if removed, 0 if not found.  The descent path and rebalance
 * siblings must already be resident (the async driver faults them in).
 */
int
diskfs_bt_remove_locked(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    struct diskfs_inode        *inode,
    const struct diskfs_bt_key *key)
{
    struct {
        void    *buf;
        uint32_t base;
        int      ci;
    } path[DISKFS_BT_MAX_DEPTH];
    int      depth = 0;
    void    *buf   = inode->block->iov.data;
    uint32_t base  = DISKFS_BT_ROOT_BASE;
    int      idx, exact, j, level;

    /* Descend to the leaf, recording the interior path. */
    for (;; ) {
        struct diskfs_bt_node_hdr *h = diskfs_bt_hdr(buf, base);

        if (h->level == 0) {
            struct diskfs_bt_lslot *sl = diskfs_bt_lslots(buf, base);

            idx = diskfs_bt_leaf_search(buf, base, key, &exact);
            if (!exact) {
                return 0;
            }
            for (j = idx; j < h->nitems - 1; j++) {
                sl[j] = sl[j + 1];
            }
            h->nitems--;
            diskfs_bt_leaf_compact(buf, base, h->capacity);
            break;
        }

        chimera_diskfs_abort_if(depth >= DISKFS_BT_MAX_DEPTH,
                                "b+tree remove: path too deep");
        path[depth].buf  = buf;
        path[depth].base = base;
        path[depth].ci   = diskfs_bt_interior_search(buf, base, key);
        buf              = diskfs_bt_node_for_write(thread, txn,
                                                    diskfs_bt_islots(buf, base)[path[depth].ci].child);
        base = 0;
        depth++;
    }

    if (depth == 0) {
        return 1;     /* the leaf is the embedded root; nothing more to do */
    }

    /* Removing a leaf's minimum changes its subtree min; keep the ancestor
     * separators exact (cascading up through leftmost links). */
    if (idx == 0 && diskfs_bt_hdr(buf, base)->nitems > 0) {
        struct diskfs_bt_key new_min = diskfs_bt_lslots(buf, base)[0].key;

        for (level = depth - 1; level >= 0; level--) {
            int ci = path[level].ci;

            diskfs_bt_islots(path[level].buf, path[level].base)[ci].key = new_min;
            if (ci > 0) {
                break;
            }
        }
    }

    /* Rebalance up the tree from the leaf's parent. */
    if (diskfs_bt_leaf_underflow(buf, base)) {
        int merged = diskfs_bt_rebalance_leaf(thread, txn, path[depth - 1].buf,
                                              path[depth - 1].base, path[depth - 1].ci);

        for (level = depth - 1; merged && level > 0; level--) {
            if (diskfs_bt_interior_underflow(path[level].buf, path[level].base)) {
                merged = diskfs_bt_rebalance_interior(thread, txn,
                                                      path[level - 1].buf,
                                                      path[level - 1].base,
                                                      path[level - 1].ci);
            } else {
                merged = 0;
            }
        }
    }

    diskfs_bt_collapse_root(thread, txn, inode);
    return 1;
} /* diskfs_bt_remove_locked */
