// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * File I/O: extent records over the inode b+tree, per-open-file space
 * reservations, read, write (in-place, redirect and RMW paths),
 * allocate/deallocate (fallocate), seek (SEEK_HOLE/SEEK_DATA) and the
 * COMMIT operation.
 */

#include "diskfs_internal.h"

/* Forward declarations (definitions below, in call-graph order) */

static int
diskfs_ext_remove_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode,
    uint64_t              file_offset,
    diskfs_bt_cb_t        cb,
    void                 *private_data);

static inline struct diskfs_extent *
diskfs_extent_alloc(
    struct diskfs_thread *thread);

static inline void
diskfs_extent_free(
    struct diskfs_thread *thread,
    struct diskfs_extent *extent);

static inline void
diskfs_extent_release(
    struct rb_node *node,
    void           *private_data);

static inline struct diskfs_symlink_target *
diskfs_symlink_target_alloc(
    struct diskfs_thread *thread,
    const char           *data,
    int                   length);

static inline void
diskfs_symlink_target_free(
    struct diskfs_thread         *thread,
    struct diskfs_symlink_target *target);

static int
diskfs_inode_dinode_clean(
    struct diskfs_shared *shared,
    struct diskfs_inode  *inode);

static void
diskfs_inode_load_recs_done(
    struct diskfs_inode_load_ctx *lc);

static inline int
diskfs_write_data_sync(
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request)
{
    (void) request;
    return !shared->unsafe_async;
} /* diskfs_write_data_sync */

static inline uint32_t
diskfs_write_reported_sync(
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request)
{
    (void) shared;
    (void) request;
    return CHIMERA_VFS_WRITE_FILESYNC;
} /* diskfs_write_reported_sync */

static void
diskfs_inode_load_recs_pnfs_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_inode_load_recs_acl_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_inode_load_recs(
    struct diskfs_inode_load_ctx *lc);

static void
diskfs_inode_load_complete(
    struct evpl *evpl,
    int          status,
    void        *private_data);

static void
diskfs_dirent_release(
    struct rb_node *node,
    void           *private_data);

static inline struct diskfs_dirent *
diskfs_dirent_alloc(
    struct diskfs_thread *thread,
    uint64_t              inum,
    uint32_t              gen,
    uint64_t              hash,
    const char           *name,
    int                   name_len);

static void
diskfs_dirent_free(
    struct diskfs_thread *thread,
    struct diskfs_dirent *dirent);

static int
diskfs_io_gate(
    struct diskfs_thread       *thread,
    struct chimera_vfs_request *request,
    void (                     *resume )(struct chimera_vfs_request *));

static inline void
diskfs_io_callback(
    struct evpl *evpl,
    int          status,
    void        *private_data);

static void
diskfs_read_finish(
    struct chimera_vfs_request *request);

static void
diskfs_read_walk_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_read_advance(
    struct chimera_vfs_request *request);

static void
diskfs_read_process(
    struct chimera_vfs_request *request);

static void
diskfs_read_first_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_read_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static void
diskfs_write_phase2_resume(
    struct chimera_vfs_request *request);

static void
diskfs_write_rmw_read_callback(
    struct evpl *evpl,
    int          status,
    void        *private_data);

static void
diskfs_write_phase2(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request);

static void
diskfs_ext_put_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_ext_put_insert(
    struct chimera_vfs_request *request);

static void
diskfs_ext_put_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_ext_put_floor_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_write_finish_map(
    struct chimera_vfs_request *request);

static void
diskfs_write_trim_done(
    struct chimera_vfs_request *request);

static void
diskfs_write_trim_walk_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_write_trim_advance(
    struct chimera_vfs_request *request);

static void
diskfs_write_trim_advance_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_write_trim_spans_before_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_write_trim_spans_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_write_trim_spans_after_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_write_trim_oleft_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_write_trim_oright_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_write_trim_process(
    struct chimera_vfs_request *request);

static void
diskfs_write_trim_first_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_write_trim_start(
    struct chimera_vfs_request *request);

static void
diskfs_write_recon_edge(
    struct chimera_vfs_request *request);

static void
diskfs_write_recon_next(
    struct chimera_vfs_request *request);

static void
diskfs_write_recon_process(
    struct chimera_vfs_request *request);

static void
diskfs_write_recon_advance(
    struct chimera_vfs_request *request);

static void
diskfs_write_recon_walk_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_write_recon_read_cb(
    struct evpl *evpl,
    int          status,
    void        *private_data);

static void
diskfs_write_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static void
diskfs_write_classify_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_write_redirect_alloc(
    struct chimera_vfs_request *request);

static void
diskfs_write_alloc_resume(
    struct diskfs_thread *thread,
    void                 *arg);

static void
diskfs_write_split_tail_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_write_split_finish_tail(
    struct chimera_vfs_request *request);

static void
diskfs_write_split_head_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_write_split_finish_head(
    struct chimera_vfs_request *request);

static void
diskfs_write_split_mid_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_write_split_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_write_split_start(
    struct chimera_vfs_request *request);

static void
diskfs_allocate_finalize(
    struct chimera_vfs_request *request);

static void
diskfs_dealloc_finish(
    struct chimera_vfs_request *request);

static void
diskfs_dealloc_walk_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_dealloc_advance(
    struct chimera_vfs_request *request);

static void
diskfs_dealloc_modify_advance_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_dealloc_modify_finish_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_dealloc_ostart_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_dealloc_oend_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_dealloc_spans_before_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_dealloc_spans_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_dealloc_spans_after_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_dealloc_process(
    struct chimera_vfs_request *request);

static void
diskfs_dealloc_first_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_allocate_alloc_resume(
    struct diskfs_thread *thread,
    void                 *arg);

static void
diskfs_allocate_next(
    struct chimera_vfs_request *request);

static void
diskfs_allocate_do_alloc(
    struct chimera_vfs_request *request);

static void
diskfs_allocate_holeend_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_allocate_floor_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_allocate_reserve_step(
    struct chimera_vfs_request *request);

static void
diskfs_allocate_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static void
diskfs_seek_walk_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_seek_advance(
    struct chimera_vfs_request *request);

static void
diskfs_seek_process(
    struct chimera_vfs_request *request);

static void
diskfs_seek_first_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_seek_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static void
diskfs_commit_acquired_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *priv);


/*
 * Async extent lookups (floor / ceil / next).  Each returns 1 if it completed
 * synchronously, 0 if it suspended (cb fires later); on completion the result
 * is in op->result and the record + found key are in rec_out / op->found_key.
 * Use diskfs_ext_from_op() in the callback to materialize the extent.
 */
int
diskfs_ext_floor_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_inode  *inode,
    uint64_t              file_offset,
    void                 *rec_out,
    uint32_t              rec_cap,
    diskfs_bt_cb_t        cb,
    void                 *private_data)
{
    struct diskfs_bt_key key = diskfs_extent_key(file_offset);
    int                  r;

    r = diskfs_bt_lookup_async(op, thread, inode, DISKFS_BT_OP_LOOKUP_LE,
                               &key, &op->found_key, rec_out, rec_cap, cb, private_data);
    return r;
} /* diskfs_ext_floor_async */


int
diskfs_ext_ceil_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_inode  *inode,
    uint64_t              file_offset,
    void                 *rec_out,
    uint32_t              rec_cap,
    diskfs_bt_cb_t        cb,
    void                 *private_data)
{
    struct diskfs_bt_key key = diskfs_extent_key(file_offset);

    return diskfs_bt_lookup_async(op, thread, inode, DISKFS_BT_OP_LOOKUP_GE,
                                  &key, &op->found_key, rec_out, rec_cap, cb, private_data);
} /* diskfs_ext_ceil_async */


int
diskfs_ext_next_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_inode  *inode,
    uint64_t              after_file_offset,
    void                 *rec_out,
    uint32_t              rec_cap,
    diskfs_bt_cb_t        cb,
    void                 *private_data)
{
    return diskfs_ext_ceil_async(op, thread, inode, after_file_offset + 1,
                                 rec_out, rec_cap, cb, private_data);
} /* diskfs_ext_next_async */


int
diskfs_ext_insert_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode,
    uint64_t              file_offset,
    uint64_t              length,
    uint32_t              device_id,
    uint64_t              device_offset,
    uint32_t              flags,
    diskfs_bt_cb_t        cb,
    void                 *private_data)
{
    struct diskfs_extent_rec rec = {
        .length        = length,
        .device_id     = device_id,
        .flags         = flags,
        .device_offset = device_offset,
    };
    struct diskfs_bt_key     key = diskfs_extent_key(file_offset);

    return diskfs_bt_insert_async(op, thread, txn, inode, &key, &rec, sizeof(rec),
                                  cb, private_data);
} /* diskfs_ext_insert_async */


static int
diskfs_ext_remove_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode,
    uint64_t              file_offset,
    diskfs_bt_cb_t        cb,
    void                 *private_data)
{
    struct diskfs_bt_key key = diskfs_extent_key(file_offset);

    return diskfs_bt_remove_async(op, thread, txn, inode, &key, cb, private_data);
} /* diskfs_ext_remove_async */


static inline struct diskfs_extent *
diskfs_extent_alloc(struct diskfs_thread *thread)
{
    struct diskfs_extent *extent;

    extent = slab_allocator_alloc(thread->allocator, sizeof(struct diskfs_extent));

    return extent;
} /* diskfs_extent_alloc */ /* diskfs_extent_alloc */ /* diskfs_extent_alloc */


static inline void
diskfs_extent_free(
    struct diskfs_thread *thread,
    struct diskfs_extent *extent)
{
    slab_allocator_free(thread->allocator, extent, sizeof(*extent));
} /* diskfs_extent_free */ /* diskfs_extent_free */ /* diskfs_extent_free */


static inline void
diskfs_extent_release(
    struct rb_node *node,
    void           *private_data)
{
    struct diskfs_thread *thread = private_data;
    struct diskfs_extent *extent = container_of(node, struct diskfs_extent, node);

    if (thread) {
        slab_allocator_free(thread->allocator, extent, sizeof(*extent));
    }
} /* diskfs_extent_release */


static inline struct diskfs_symlink_target *
diskfs_symlink_target_alloc(
    struct diskfs_thread *thread,
    const char           *data,
    int                   length)
{
    struct diskfs_symlink_target *target;

    target = slab_allocator_alloc(thread->allocator, sizeof(struct diskfs_symlink_target));

    target->data = slab_allocator_alloc(thread->allocator, length);

    target->length = length;

    memcpy(target->data, data, length);

    return target;
} /* diskfs_symlink_target_alloc */


static inline void
diskfs_symlink_target_free(
    struct diskfs_thread         *thread,
    struct diskfs_symlink_target *target)
{
    slab_allocator_free(thread->allocator, target->data, target->length);
    slab_allocator_free(thread->allocator, target, sizeof(*target));
} /* diskfs_symlink_target_free */


/*
 * An inode struct may be dropped only when its dinode is durably home, so a
 * later fault re-reads current attrs from disk.  True iff the dinode's home
 * block is CLEAN or no longer resident (it was CLEAN when evicted from the
 * block cache, hence on disk).  Caller holds the inode shard lock; this takes
 * the block shard lock (inode->block ordering, never the reverse).
 */
static int
diskfs_inode_dinode_clean(
    struct diskfs_shared *shared,
    struct diskfs_inode  *inode)
{
    uint32_t                   dev;
    uint64_t                   off = sm_inum_to_device_offset(shared->space_map,
                                                              inode->inum, &dev);
    struct diskfs_block_shard *bs     = diskfs_block_shard(shared->block_cache, dev, off);
    uint32_t                   bucket = diskfs_block_bucket(dev, off);
    struct diskfs_block       *blk;
    int                        clean;

    pthread_mutex_lock(&bs->lock);
    blk   = diskfs_block_lookup_locked(bs, bucket, dev, off);
    clean = (blk == NULL || blk->state == DISKFS_BLOCK_CLEAN);
    pthread_mutex_unlock(&bs->lock);
    return clean;
} /* diskfs_inode_dinode_clean */


/*
 * Make room in a shard at/over its cap by evicting one idle, durable inode
 * from the LRU.  Caller holds the shard lock.  The LRU is approximate -- a
 * candidate may have gone busy since it was queued -- so each is re-validated;
 * stale ones are unlinked (self-heal) and dinode-dirty ones skipped.  If none
 * are evictable the pool grows past the cap (bounded by the live working set;
 * the A5b waiter will make this a hard cap).
 */
void
diskfs_inode_cache_recycle_locked(
    struct diskfs_shared      *shared,
    struct diskfs_inode_shard *shard)
{
    struct diskfs_inode *inode, *next;

    if (shard->ninodes < shared->inode_cache->shard_cap) {
        return;
    }

    for (inode = shard->lru_head; inode; inode = next) {
        next = inode->lru_next;

        if (!diskfs_inode_idle(inode)) {
            diskfs_inode_lru_unlink(shard, inode);     /* went busy; self-heal */
            continue;
        }
        if (!diskfs_inode_dinode_clean(shared, inode)) {
            continue;                                  /* not durable yet; skip */
        }

        /* An idle inode (no open handle) has already returned its data-space
         * reservation at its last close; catch a leak before the struct goes. */
        chimera_diskfs_abort_if(inode->space_resv.valid,
                                "evicting inode %lu with a live space reservation",
                                inode->inum);

        diskfs_inode_lru_unlink(shard, inode);
        rb_tree_remove(&shard->inodes, &inode->node);
        shard->ninodes--;
        diskfs_inode_struct_free(inode);
        return;
    }
} /* diskfs_inode_cache_recycle_locked */


/* Record loads done: install the mirrors, drop the loader's exclusive hold
 * (waking any acquirers that parked during the fault) and re-drive the
 * original acquire. */
static void
diskfs_inode_load_recs_done(struct diskfs_inode_load_ctx *lc)
{
    struct diskfs_thread *thread = lc->thread;
    struct diskfs_inode  *inode  = lc->inode;

    if (lc->acl_len >= 0) {
        diskfs_acl_serial_install(inode, lc->acl_rec, lc->acl_len);
    }
    if (lc->pnfs_len >= 0) {
        inode->pnfs_blob = malloc(lc->pnfs_len);
        memcpy(inode->pnfs_blob, lc->pnfs_rec, lc->pnfs_len);
        inode->pnfs_blob_len = (uint32_t) lc->pnfs_len;
    }

    diskfs_inode_release_one(thread, inode, DISKFS_INODE_LOCK_WRITE);

    diskfs_inode_acquire(thread, lc->txn, lc->inum, lc->gen, lc->mode,
                         lc->cb, lc->private_data);
    free(lc);
} /* diskfs_inode_load_recs_done */


static void
diskfs_inode_load_recs_pnfs_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct diskfs_inode_load_ctx *lc = private_data;

    lc->pnfs_len = result;
    diskfs_bt_op_free(lc->thread, op);
    diskfs_inode_load_recs_done(lc);
} /* diskfs_inode_load_recs_pnfs_cb */


static void
diskfs_inode_load_recs_acl_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct diskfs_inode_load_ctx *lc = private_data;

    lc->acl_len = result;
    diskfs_bt_op_free(lc->thread, op);

    op = diskfs_bt_op_alloc(lc->thread);
    if (diskfs_bt_lookup_async(op, lc->thread, lc->inode,
                               DISKFS_BT_OP_LOOKUP_EXACT, &diskfs_pnfs_key,
                               NULL, lc->pnfs_rec, sizeof(lc->pnfs_rec),
                               diskfs_inode_load_recs_pnfs_cb, lc)) {
        diskfs_inode_load_recs_pnfs_cb(op, op->result, lc);
    }
} /* diskfs_inode_load_recs_acl_cb */


static void
diskfs_inode_load_recs(struct diskfs_inode_load_ctx *lc)
{
    struct diskfs_bt_op *op = diskfs_bt_op_alloc(lc->thread);

    if (diskfs_bt_lookup_async(op, lc->thread, lc->inode,
                               DISKFS_BT_OP_LOOKUP_EXACT, &diskfs_acl_key,
                               NULL, lc->acl_rec, sizeof(lc->acl_rec),
                               diskfs_inode_load_recs_acl_cb, lc)) {
        diskfs_inode_load_recs_acl_cb(op, op->result, lc);
    }
} /* diskfs_inode_load_recs */


static void
diskfs_inode_load_complete(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct diskfs_inode_load_ctx *lc     = private_data;
    struct diskfs_thread         *thread = lc->thread;
    struct diskfs_shared         *shared = thread->shared;
    struct diskfs_dinode         *di     = (struct diskfs_dinode *) lc->iov.data;
    struct diskfs_inode_shard    *shard  = diskfs_inode_shard(shared, lc->inum);
    struct diskfs_inode          *inode;

    if (status != 0 || di->inum != lc->inum || di->gen != lc->gen ||
        di->nlink == 0) {
        /* No such inode on disk (or stale generation). */
        evpl_iovec_release(thread->evpl, &lc->iov);
        lc->cb(NULL, CHIMERA_VFS_ENOENT, lc->private_data);
        free(lc);
        return;
    }

    pthread_mutex_lock(&shard->lock);
    rb_tree_query_exact(&shard->inodes, lc->inum, inum, inode);
    if (!inode) {
        diskfs_inode_cache_recycle_locked(shared, shard);
        inode                 = diskfs_inode_struct_new(lc->inum);
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
        /* Publish write-locked, held by this fault: nobody can grant (or
         * modify the tree) until the record loads below finish; concurrent
         * acquirers park as ordinary lock waiters. */
        inode->writer = 1;
        rb_tree_insert(&shard->inodes, inum, inode);
        shard->ninodes++;
        diskfs_metric_inode_cache(thread, DISKFS_METRIC_INODE_CACHE_LOAD);
    } else {
        /* Lost a concurrent fault race: the winner published the inode (and
         * does/did its own record loads).  Just re-drive the acquire. */
        pthread_mutex_unlock(&shard->lock);
        evpl_iovec_release(thread->evpl, &lc->iov);
        diskfs_inode_acquire(thread, lc->txn, lc->inum, lc->gen, lc->mode,
                             lc->cb, lc->private_data);
        free(lc);
        return;
    }
    pthread_mutex_unlock(&shard->lock);

    /* Seed the inode's home block (dinode + embedded b+tree root) into the
     * block cache from the disk image, so the b+tree traversal and inode-block
     * pin find the real contents instead of a zero-created block.  No writer
     * can be modifying it yet -- we hold the inode exclusively.  Claim is_new
     * (no disk read): we already hold the freshly-read image in lc->iov and
     * overwrite the whole block below, so reading it back would be redundant
     * -- and a synchronous read here cannot reach a VFIO device anyway. */
    {
        uint32_t             dev;
        uint64_t             off = sm_inum_to_device_offset(shared->space_map,
                                                            lc->inum, &dev);
        struct diskfs_block *blk = diskfs_block_claim(thread, dev, off, 1);

        memcpy(blk->iov.data, lc->iov.data, DISKFS_BLOCK_SIZE);
        diskfs_block_unpin(thread, blk, DISKFS_BLOCK_CLEAN);
    }

    evpl_iovec_release(thread->evpl, &lc->iov);

    /* Load the ACL/pNFS record mirrors, then release the hold and re-drive
     * the acquire to grant the lock as usual. */
    lc->inode = inode;
    diskfs_inode_load_recs(lc);
} /* diskfs_inode_load_complete */


void
diskfs_inode_load(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    uint64_t                    inum,
    uint32_t                    gen,
    enum diskfs_inode_lock_mode mode,
    diskfs_inode_cb_t           cb,
    void                       *private_data)
{
    struct diskfs_inode_load_ctx *lc = malloc(sizeof(*lc));
    uint32_t                      dev;
    uint64_t                      off;

    off              = sm_inum_to_device_offset(thread->shared->space_map, inum, &dev);
    lc->thread       = thread;
    lc->txn          = txn;
    lc->inum         = inum;
    lc->gen          = gen;
    lc->mode         = mode;
    lc->cb           = cb;
    lc->private_data = private_data;

    evpl_iovec_alloc(thread->evpl, DISKFS_BLOCK_SIZE, DISKFS_BLOCK_SIZE, 1, 0, &lc->iov);
    diskfs_metric_block_io(thread, DISKFS_METRIC_IO_READ,
                           DISKFS_METRIC_IO_INODE, DISKFS_BLOCK_SIZE);
    diskfs_metric_block_io_device(thread, dev, DISKFS_METRIC_IO_READ,
                                  DISKFS_METRIC_IO_INODE, DISKFS_BLOCK_SIZE);
    evpl_block_read(thread->evpl, thread->queue[dev], &lc->iov, 1, off,
                    diskfs_inode_load_complete, lc);
} /* diskfs_inode_load */


void
diskfs_inode_alloc_resume(
    struct diskfs_thread *thread,
    void                 *arg)
{
    struct diskfs_inode_alloc_ctx c = *(struct diskfs_inode_alloc_ctx *) arg;

    (void) thread;
    free(arg);
    diskfs_inode_alloc_async(c.thread, c.txn, c.cb, c.private_data);
} /* diskfs_inode_alloc_resume */


static void
diskfs_dirent_release(
    struct rb_node *node,
    void           *private_data)
{
    struct diskfs_thread *thread = private_data;
    struct diskfs_dirent *dirent = container_of(node, struct diskfs_dirent, node);

    if (thread) {
        slab_allocator_free(thread->allocator, dirent, sizeof(*dirent));
    }
} /* diskfs_dirent_release */


static inline struct diskfs_dirent *
diskfs_dirent_alloc(
    struct diskfs_thread *thread,
    uint64_t              inum,
    uint32_t              gen,
    uint64_t              hash,
    const char           *name,
    int                   name_len)
{
    struct diskfs_dirent *dirent = slab_allocator_alloc(thread->allocator, sizeof(struct diskfs_dirent));

    dirent->inum     = inum;
    dirent->gen      = gen;
    dirent->hash     = hash;
    dirent->name_len = name_len;

    dirent->name = slab_allocator_alloc(thread->allocator, name_len);
    memcpy(dirent->name, name, name_len);

    return dirent;

} /* diskfs_dirent_alloc */


static void
diskfs_dirent_free(
    struct diskfs_thread *thread,
    struct diskfs_dirent *dirent)
{
    slab_allocator_free(thread->allocator, dirent->name, dirent->name_len);
    slab_allocator_free(thread->allocator, dirent, sizeof(*dirent));
} /* diskfs_dirent_free */


void
diskfs_kv_entry_free(
    struct diskfs_thread   *thread,
    struct diskfs_kv_entry *entry)
{
    slab_allocator_free(thread->allocator, entry->key, entry->key_len);
    slab_allocator_free(thread->allocator, entry->value, entry->value_len);
    slab_allocator_free(thread->allocator, entry, sizeof(*entry));
} /* diskfs_kv_entry_free */


void
diskfs_kv_entry_release(
    struct rb_node *node,
    void           *private_data)
{
    struct diskfs_kv_entry *entry = container_of(node, struct diskfs_kv_entry, node);

    free(entry->key);
    free(entry->value);
    free(entry);
} /* diskfs_kv_entry_release */


/*
 * Returns 1 and parks the request if the in-flight data I/O is at the cap (the
 * caller must then return without issuing); 0 if it is clear to submit.  resume
 * re-enters the paused path once a completion drains the queue.
 */
static int
diskfs_io_gate(
    struct diskfs_thread       *thread,
    struct chimera_vfs_request *request,
    void (                     *resume )(struct chimera_vfs_request *))
{
    struct diskfs_request_private *p = request->plugin_data;

    if (thread->pending_io < DISKFS_IO_INFLIGHT_CAP) {
        return 0;
    }

    p->io_resume    = resume;
    p->io_wait_next = NULL;
    if (thread->io_wait_tail) {
        struct diskfs_request_private *tp = thread->io_wait_tail->plugin_data;
        tp->io_wait_next = request;
    } else {
        thread->io_wait_head = request;
    }
    thread->io_wait_tail = request;
    return 1;
} /* diskfs_io_gate */


/* Resume parked requests while the queue has drained below the low watermark. */
void
diskfs_io_resume_waiters(struct diskfs_thread *thread)
{
    while (thread->io_wait_head && thread->pending_io < DISKFS_IO_INFLIGHT_LOWAT) {
        struct chimera_vfs_request    *request = thread->io_wait_head;
        struct diskfs_request_private *p       = request->plugin_data;
        void                           (*resume)(
            struct chimera_vfs_request *) = p->io_resume;

        thread->io_wait_head = p->io_wait_next;
        if (!thread->io_wait_head) {
            thread->io_wait_tail = NULL;
        }
        p->io_wait_next = NULL;
        p->io_resume    = NULL;
        resume(request);
    }
} /* diskfs_io_resume_waiters */


static inline void
diskfs_io_callback(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct chimera_vfs_request    *request        = (struct chimera_vfs_request *) private_data;
    struct diskfs_request_private *diskfs_private = request->plugin_data;
    struct diskfs_thread          *thread         = diskfs_private->thread;

    if (diskfs_private->status == 0 && status) {
        diskfs_private->status = status;
    }

    diskfs_private->pending--;
    diskfs_pending_io_add(thread, -1);

    /* Don't finalize a read whose extent walk is still in progress (parked on
     * the admission gate): its remaining reads have yet to be issued.  The
     * io_reading guard is scoped to reads -- request plugin_data is pooled and
     * not zeroed, and only diskfs_read sets the flag (fresh, per op). */
    if (diskfs_private->pending == 0 &&
        !(diskfs_private->opcode == CHIMERA_VFS_OP_READ && diskfs_private->io_reading)) {
        /* Release the per-chunk device-I/O iovec refs (slices of the
        * VFS-provided read buffers); the VFS core trims and releases
        * request->read.iov itself after the request bounces back. */
        evpl_iovecs_release(thread->evpl, diskfs_private->iov, diskfs_private->niov);

        if (diskfs_private->status != 0) {
            diskfs_op_fail(request, diskfs_private->txn,
                           diskfs_private->status);
        } else {
            diskfs_op_ok(request, diskfs_private->txn);
        }
    }

    /* Queue capacity freed: let any parked requests resume submitting. */
    diskfs_io_resume_waiters(thread);
} /* diskfs_io_callback */


static void
diskfs_read_finish(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_inode           *inode  = p->inode_stash[0];

    if (p->loop_left) {
        evpl_iovec_cursor_zero(&p->rd_cursor, p->loop_left);
    }

    diskfs_map_attrs(thread, &request->read.r_attr, inode);

    /* The extent walk is complete; a now-or-later finalize is safe. */
    p->io_reading = 0;

    if (p->pending == 0) {
        diskfs_op_ok(request, p->txn);
    } else if (p->txn->type == DISKFS_TXN_READ) {
        /* I/O is in flight; drop the inode lock so other ops proceed.  The
         * txn commits from diskfs_io_callback once all reads complete. */
        diskfs_txn_unlock_inode(p->txn, inode);
    }
    /* A relatime atime bump upgraded this read to a WRITE txn: keep the inode
     * locked until the redo is durable (like the write path) -- io_callback
     * runs diskfs_op_ok at pending==0 and the intent-log thread releases the
     * lock once committed. */
} /* diskfs_read_finish */


static void
diskfs_read_walk_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    p->loop_have = diskfs_ext_from_op(op, result, &p->ext_iter);
    diskfs_bt_op_free(p->thread, op);
    diskfs_read_process(request);
} /* diskfs_read_walk_cb */


static void
diskfs_read_advance(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_op           *op     = diskfs_bt_op_alloc(thread);

    if (diskfs_ext_next_async(op, thread, p->inode_stash[0], p->ext_iter.file_offset,
                              p->rec_scratch, sizeof(p->rec_scratch),
                              diskfs_read_walk_cb, request)) {
        diskfs_read_walk_cb(op, op->result, request);
    }
} /* diskfs_read_advance */


static void
diskfs_read_process(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p           = request->plugin_data;
    struct diskfs_thread          *thread      = p->thread;
    struct diskfs_shared          *shared      = thread->shared;
    struct evpl                   *evpl        = thread->evpl;
    struct diskfs_extent          *extent      = &p->ext_iter;
    uint64_t                       read_offset = p->loop_off;
    uint64_t                       read_left   = p->loop_left;
    uint64_t                       aligned_end = p->loop_pos;
    uint64_t                       extent_end, overlap_start, overlap_length, chunk;
    uint32_t                       chunk_niov;
    struct evpl_iovec             *chunk_iov;

    if (!(read_left && p->loop_have && extent->file_offset < aligned_end)) {
        diskfs_read_finish(request);
        return;
    }

    /* Bound in-flight data I/O: park here (state is fully in p) and resume the
     * walk from a completion if the queue is at the cap. */
    if (diskfs_io_gate(thread, request, diskfs_read_process)) {
        return;
    }

    if (read_offset < extent->file_offset) {
        chunk = extent->file_offset - read_offset;
        evpl_iovec_cursor_zero(&p->rd_cursor, chunk);
        read_offset += chunk;
        read_left   -= chunk;
    }

    extent_end     = extent->file_offset + extent->length;
    overlap_start  = read_offset - extent->file_offset;
    overlap_length = extent_end - read_offset;
    if (overlap_length > read_left) {
        overlap_length = read_left;
    }

    if (extent->flags & DISKFS_EXT_UNWRITTEN) {
        /* Space is reserved (fallocate) but was never written: it reads back
         * as zeros, with no device I/O.  Fill the cursor like a hole and skip
         * the read loop. */
        evpl_iovec_cursor_zero(&p->rd_cursor, overlap_length);
        read_offset   += overlap_length;
        read_left     -= overlap_length;
        overlap_length = 0;
    }

    while (overlap_length) {
        uint64_t dev_offset;
        uint32_t dev_pad, total;
        int      pad_niov = 0;

        if (overlap_length > shared->devices[extent->device_id].max_request_size) {
            chunk = shared->devices[extent->device_id].max_request_size;
        } else {
            chunk = overlap_length;
        }

        chunk_iov  = &p->iov[p->niov];
        dev_offset = extent->device_offset + overlap_start;
        dev_pad    = (uint32_t) (dev_offset & 4095ULL);

        if (dev_pad) {
            evpl_iovec_clone_segment(&chunk_iov[0], &thread->pad, 0, dev_pad);
            pad_niov    = 1;
            dev_offset -= dev_pad;
        }

        chunk_niov = evpl_iovec_cursor_move(&p->rd_cursor, &chunk_iov[pad_niov],
                                            32, chunk, 1);
        chunk_niov += pad_niov;

        total = dev_pad + chunk;
        if (total & 4095) {
            evpl_iovec_clone_segment(&chunk_iov[chunk_niov], &thread->pad, 0,
                                     4096 - (total & 4095));
            chunk_niov++;
        }

        p->niov += chunk_niov;
        p->pending++;
        diskfs_pending_io_add(thread, 1);

        diskfs_metric_block_io(thread, DISKFS_METRIC_IO_READ,
                               DISKFS_METRIC_IO_DATA, chunk);
        diskfs_metric_block_io_device(thread, extent->device_id,
                                      DISKFS_METRIC_IO_READ,
                                      DISKFS_METRIC_IO_DATA, chunk);
        evpl_block_read(evpl, thread->queue[extent->device_id], chunk_iov,
                        chunk_niov, dev_offset, diskfs_io_callback, request);

        overlap_length -= chunk;
        overlap_start  += chunk;
        read_offset    += chunk;
        read_left      -= chunk;
    }

    p->loop_off  = read_offset;
    p->loop_left = read_left;

    diskfs_read_advance(request);
} /* diskfs_read_process */


/* First-extent selection for read: floor(read_offset), advancing if it ends
 * at/before read_offset, or the first extent if none. */
static void
diskfs_read_first_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request     = private_data;
    struct diskfs_request_private *p           = request->plugin_data;
    struct diskfs_thread          *thread      = p->thread;
    uint64_t                       read_offset = p->loop_off;
    int                            have        = diskfs_ext_from_op(op, result, &p->ext_iter);

    diskfs_bt_op_free(thread, op);

    if (have && p->ext_iter.file_offset + p->ext_iter.length <= read_offset) {
        diskfs_read_advance(request);
        return;
    }
    if (!have) {
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_ceil_async(op, thread, p->inode_stash[0], 0, p->rec_scratch,
                                  sizeof(p->rec_scratch), diskfs_read_walk_cb,
                                  request)) {
            diskfs_read_walk_cb(op, op->result, request);
        }
        return;
    }

    p->loop_have = 1;
    diskfs_read_process(request);
} /* diskfs_read_first_cb */


static void
diskfs_read_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request        = private_data;
    struct diskfs_request_private *diskfs_private = request->plugin_data;
    struct diskfs_thread          *thread         = diskfs_private->thread;
    uint64_t                       offset, length;
    uint64_t                       aligned_offset, aligned_length;
    uint32_t                       eof = 0;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, diskfs_private->txn, status);
        return;
    }

    if (unlikely(!S_ISREG(inode->mode))) {
        diskfs_op_fail(request, diskfs_private->txn, CHIMERA_VFS_EINVAL);
        return;
    }

    offset = request->read.offset;
    length = request->read.length;

    if (offset >= inode->size) {
        length = 0;
        eof    = 1;
    } else if (length >= inode->size - offset) {
        length = inode->size - offset;
        eof    = 1;
    }

    if (unlikely(length == 0)) {
        diskfs_map_attrs(thread, &request->read.r_attr, inode);
        request->read.r_niov   = 0;
        request->read.r_length = 0;
        request->read.r_eof    = eof;
        diskfs_op_ok(request, diskfs_private->txn);
        return;
    }

    /*
     * relatime atime maintenance (data-returning reads only).  Reads run as a
     * READ txn; if relatime says atime is due for a bump we can't journal it
     * under a read lock, so abort and re-run the whole read under a WRITE txn.
     * On the (rare) re-entry the txn is already WRITE: pin the inode block and
     * stamp atime only (never ctime); the WRITE commit journals it.
     */
    if (diskfs_private->txn->type == DISKFS_TXN_WRITE) {
        struct timespec now;

        chimera_vfs_realtime(&now);
        diskfs_txn_pin_inode_block(thread, diskfs_private->txn, inode, 0);
        inode->atime_sec  = now.tv_sec;
        inode->atime_nsec = now.tv_nsec;
    } else if (!thread->shared->noatime) {
        struct timespec atime = { inode->atime_sec, inode->atime_nsec };
        struct timespec mtime = { inode->mtime_sec, inode->mtime_nsec };
        struct timespec ctime = { inode->ctime_sec, inode->ctime_nsec };
        struct timespec now;

        chimera_vfs_realtime(&now);

        if (chimera_vfs_relatime_needs_update(&atime, &mtime, &ctime, &now)) {
            struct diskfs_inode *pinned =
                (request->read.handle && request->read.handle->vfs_private) ?
                (struct diskfs_inode *) request->read.handle->vfs_private : NULL;

            diskfs_txn_abort(diskfs_private->txn);
            diskfs_private->txn = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);

            if (pinned) {
                diskfs_inode_acquire_pinned(thread, diskfs_private->txn, pinned,
                                            DISKFS_INODE_MODE_FOR_TXN(diskfs_private->txn),
                                            diskfs_read_inode_cb, request);
            } else {
                diskfs_inode_get_fh_async(thread, diskfs_private->txn,
                                          request->fh, request->fh_len,
                                          diskfs_read_inode_cb, request);
            }
            return;
        }
    }

    aligned_offset = offset & ~4095ULL;
    aligned_length = ((offset + length + 4095ULL) & ~4095ULL) - aligned_offset;

    request->read.r_length = length;
    request->read.r_eof    = eof;

    /* The VFS core allocated the 4 KiB-aligned read buffers on the connection
     * thread (diskfs does not advertise CAP_READ_PROVIDES_BUFFERS) and placed
     * them in request->read.iov.  Fill them via the cursor; the VFS core skips
     * the prefix pad and trims to r_length on completion.  Its allocation is
     * sized from the unclamped count, so it always covers our (EOF-clamped)
     * aligned_length. */
    chimera_diskfs_abort_if(request->read.buffers_provided == 0,
                            "diskfs read dispatched without VFS-provided buffers");

    evpl_iovec_cursor_init(&diskfs_private->rd_cursor, request->read.iov,
                           request->read.buffers_provided);

    diskfs_private->inode_stash[0] = inode;
    diskfs_private->loop_off       = aligned_offset;
    diskfs_private->loop_left      = aligned_length;
    diskfs_private->loop_pos       = aligned_offset + aligned_length;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_ext_floor_async(op, thread, inode, aligned_offset, diskfs_private->rec_scratch,
                               sizeof(diskfs_private->rec_scratch), diskfs_read_first_cb,
                               request)) {
        diskfs_read_first_cb(op, op->result, request);
    }
} /* diskfs_read_inode_cb */


void
diskfs_read(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;

    (void) private_data;

    /* Block/SCSI-mode shares keep all file data on remote (pNFS) devices the
     * server can't touch, so inline reads are impossible -- the client must use
     * a layout.  Reject so a non-pNFS read doesn't dereference a NULL device
     * queue. */
    if (unlikely(shared->block_layout || shared->scsi_layout)) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    p->opcode     = request->opcode;
    p->status     = 0;
    p->pending    = 0;
    p->niov       = 0;
    p->thread     = thread;
    p->io_reading = 1;     /* cleared in diskfs_read_finish when the walk ends */
    p->txn        = diskfs_txn_begin(thread, DISKFS_TXN_READ);

    /* Warm-handle fast path: diskfs advertises CAP_OPEN_FILE_REQUIRED, so a read
     * is preceded by a real open that pinned the inode and stashed it in
     * handle->vfs_private.  Reuse it to skip the fh->inum decode + rb-tree
     * lookup.  Fall back to the by-fh resolve for any handle that lacks it. */
    if (request->read.handle && request->read.handle->vfs_private) {
        diskfs_inode_acquire_pinned(thread, p->txn,
                                    (struct diskfs_inode *) request->read.handle->vfs_private,
                                    DISKFS_INODE_MODE_FOR_TXN(p->txn),
                                    diskfs_read_inode_cb, request);
    } else {
        diskfs_inode_get_fh_async(thread, p->txn,
                                  request->fh, request->fh_len,
                                  diskfs_read_inode_cb, request);
    }
} /* diskfs_read */


/* Admission-gate resume trampoline for the write data phase. */
static void
diskfs_write_phase2_resume(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p = request->plugin_data;

    diskfs_write_phase2(p->thread, p->thread->shared, request);
} /* diskfs_write_phase2_resume */


// Callback for RMW prefix/suffix reads
static void
diskfs_write_rmw_read_callback(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct chimera_vfs_request    *request        = private_data;
    struct diskfs_request_private *diskfs_private = request->plugin_data;
    struct diskfs_thread          *thread         = diskfs_private->thread;
    struct diskfs_shared          *shared         = thread->shared;

    if (status && diskfs_private->status == 0) {
        diskfs_private->status = status;
    }

    diskfs_private->pending--;
    diskfs_pending_io_add(thread, -1);

    /* Queue capacity freed: let any parked requests resume submitting. */
    diskfs_io_resume_waiters(thread);

    if (diskfs_private->pending == 0) {
        if (diskfs_private->status) {
            // RMW read failed
            if (diskfs_private->rmw_prefix_iov.data) {
                evpl_iovec_release(thread->evpl, &diskfs_private->rmw_prefix_iov);
            }
            if (diskfs_private->rmw_suffix_iov.data) {
                evpl_iovec_release(thread->evpl, &diskfs_private->rmw_suffix_iov);
            }
            request->status = diskfs_private->status;
            request->complete(request);
            return;
        }

        // All RMW reads complete, proceed to write phase
        diskfs_private->rmw_phase = 2;
        diskfs_write_phase2(thread, shared, request);
    }
} /* diskfs_write_rmw_read_callback */


// Phase 2: Issue actual writes (called after RMW reads complete or if no RMW needed)
static void
diskfs_write_phase2(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request)
{
    struct evpl                   *evpl           = thread->evpl;
    struct diskfs_request_private *diskfs_private = request->plugin_data;
    struct evpl_iovec              write_iov[DISKFS_WRITE_RMW_MAX_IOV];
    int                            write_niov = 0;
    uint64_t                       offset, chunk;
    uint32_t                       left;
    struct evpl_iovec             *chunk_iov;
    int                            chunk_niov;
    struct evpl_iovec_cursor       cursor;
    uint64_t                       write_length = request->write.length;
    uint32_t                       prefix_len   = diskfs_private->rmw_prefix_len;
    uint32_t                       suffix_len   = diskfs_private->rmw_suffix_len;

    /* Bound in-flight data I/O: park before assembling/issuing the write if the
     * queue is at the cap.  We gate at entry (nothing allocated yet), so resume
     * simply re-enters phase2.  The inode lock is held until the txn is durable
     * regardless, so parking here doesn't expose dirty state. */
    if (diskfs_io_gate(thread, request, diskfs_write_phase2_resume)) {
        return;
    }

    if (request->write.niov > DISKFS_WRITE_MAX_IOV) {
        diskfs_op_fail(request, diskfs_private->txn, CHIMERA_VFS_EINVAL);
        return;
    }

    /* Zero-copy fast path: a fully block-aligned overwrite has no RMW prefix or
    * suffix (and therefore no sub-block padding), so the staged buffer would be
    * a byte-for-byte copy of the caller's write data.  When the data is a single
    * block-aligned segment that fits a single device request, hand the caller's
    * iovec straight to the device and skip the per-write staging copy entirely.
    *
    * The single-segment, 4K-aligned gate is required for correctness, not just
    * the win: the device backend (libaio/io_uring O_DIRECT, VFIO) needs each
    * segment block-aligned in address and length.  That holds for an RDMA write,
    * where the data lands in one registered, 4K-aligned buffer -- the same buffer
    * the read path DMAs device reads into.  It does NOT hold for a write whose
    * payload arrived as many unaligned fragments (e.g. an RPC reassembled from
    * TCP record marks); those must take the staging path below, which coalesces
    * them into aligned device blocks.  Feeding raw unaligned segments to the
    * device silently drops the I/O -> the request never completes -> hang.
    *
    * Lifetime: the VFS core retains ownership of request->write.iov and releases
    * it only after this op completes (i.e. after the device write), so borrowing
    * it here is safe.  We do not add it to diskfs_private->iov, so io_callback
    * (which only releases diskfs_private->iov[0..niov]) leaves it untouched. */
    if (prefix_len == 0 && suffix_len == 0 && write_length > 0 &&
        diskfs_private->rmw_aligned_length == write_length &&
        request->write.niov == 1 &&
        (((uintptr_t) request->write.iov[0].data & 4095) == 0) &&
        write_length <= shared->devices[diskfs_private->rmw_device_id].max_request_size) {

        diskfs_private->pending = 1;
        diskfs_private->niov    = 0;

        diskfs_pending_io_add(thread, 1);

        diskfs_metric_block_io(thread, DISKFS_METRIC_IO_WRITE,
                               DISKFS_METRIC_IO_DATA, write_length);
        diskfs_metric_block_io_device(thread, diskfs_private->rmw_device_id,
                                      DISKFS_METRIC_IO_WRITE,
                                      DISKFS_METRIC_IO_DATA, write_length);

        evpl_block_write(evpl,
                         thread->queue[diskfs_private->rmw_device_id],
                         request->write.iov,
                         request->write.niov,
                         diskfs_private->rmw_device_offset,
                         diskfs_write_data_sync(shared, request),
                         diskfs_io_callback,
                         request);
        return;
    }

    // Build the combined write iovec:
    // [prefix (if any)] + [write data] + [suffix (if any)] + [padding to 4KB]

    // Add prefix if present
    if (prefix_len > 0) {
        if (diskfs_private->rmw_prefix_iov.data && diskfs_private->rmw_prefix_valid > 0) {
            // Prefix from existing extent
            uint32_t valid_len = diskfs_private->rmw_prefix_valid;

            if (valid_len > prefix_len) {
                valid_len = prefix_len;
            }

            // Add the valid portion from existing extent
            evpl_iovec_move_segment(&write_iov[write_niov], &diskfs_private->rmw_prefix_iov, 0, valid_len);
            write_niov++;

            // If extent was truncated, remaining prefix bytes should be zeros
            if (valid_len < prefix_len) {
                evpl_iovec_clone_segment(&write_iov[write_niov], &thread->zero, 0, prefix_len - valid_len);
                write_niov++;
            }
        } else {
            // Prefix is zeros (no existing data)
            // Use thread->zero without adding ref - it's persistent
            evpl_iovec_clone_segment(&write_iov[write_niov], &thread->zero, 0, prefix_len);
            write_niov++;
        }
    }

    // Add write data - clone to local array (caller retains ownership and releases).
    for (int i = 0; i < request->write.niov; i++) {
        evpl_iovec_clone(&write_iov[write_niov], &request->write.iov[i]);
        write_niov++;
    }

    // Add suffix if present
    if (suffix_len > 0) {
        if (diskfs_private->rmw_suffix_iov.data && diskfs_private->rmw_suffix_valid > 0) {
            // Suffix from existing extent - extract the portion after write_end
            uint64_t write_end = request->write.offset + write_length;
            // suffix_start is the offset within the read buffer to find write_end's data
            // Normally it's (write_end & 4095), but if we had to adjust because the
            // block started before the extent, we subtract the adjustment
            uint32_t suffix_start = (write_end & 4095) - diskfs_private->rmw_suffix_adjust;
            uint32_t valid_len    = diskfs_private->rmw_suffix_valid;

            if (valid_len > suffix_len) {
                valid_len = suffix_len;
            }

            // Add the valid portion from existing extent
            evpl_iovec_move_segment(&write_iov[write_niov], &diskfs_private->rmw_suffix_iov, suffix_start, valid_len);
            write_niov++;

            // If extent was truncated, remaining suffix bytes should be zeros
            if (valid_len < suffix_len) {
                evpl_iovec_clone_segment(&write_iov[write_niov], &thread->zero, 0, suffix_len - valid_len);
                write_niov++;
            }
        } else {
            // Suffix is zeros (no existing data)
            // Use thread->zero without adding ref - it's persistent
            evpl_iovec_clone_segment(&write_iov[write_niov], &thread->zero, 0, suffix_len);
            write_niov++;
        }
    }

    // Add padding to align to 4KB if needed
    uint64_t total_len = prefix_len + write_length + suffix_len;
    uint32_t padding   = (4096 - (total_len & 4095)) & 4095;

    if (padding > 0) {
        evpl_iovec_clone_segment(&write_iov[write_niov], &thread->zero, 0, padding);
        write_niov++;
    }

    // Reset pending and niov for write phase
    diskfs_private->pending = 0;
    diskfs_private->niov    = 0;

    evpl_iovec_cursor_init(&cursor, write_iov, write_niov);

    offset = 0;
    left   = diskfs_private->rmw_aligned_length;

    while (left) {
        chunk = shared->devices[diskfs_private->rmw_device_id].max_request_size;

        if (left < chunk) {
            chunk = left;
        }

        chunk_iov = &diskfs_private->iov[diskfs_private->niov];

        chunk_niov = evpl_iovec_alloc(evpl, chunk, 4096, 32, 0, chunk_iov);
        if (chunk_niov <= 0) {
            evpl_iovecs_release(evpl, write_iov, write_niov);
            diskfs_op_fail(request, diskfs_private->txn, CHIMERA_VFS_EIO);
            return;
        }

        evpl_iovec_cursor_get_blob(&cursor, chunk_iov->data, chunk_iov->length);
        for (int i = 1; i < chunk_niov; i++) {
            evpl_iovec_cursor_get_blob(&cursor, chunk_iov[i].data, chunk_iov[i].length);
        }

        diskfs_private->niov += chunk_niov;

        diskfs_private->pending++;
        diskfs_pending_io_add(thread, 1);

        diskfs_metric_block_io(thread, DISKFS_METRIC_IO_WRITE,
                               DISKFS_METRIC_IO_DATA, chunk);
        diskfs_metric_block_io_device(thread, diskfs_private->rmw_device_id,
                                      DISKFS_METRIC_IO_WRITE,
                                      DISKFS_METRIC_IO_DATA, chunk);
        evpl_block_write(evpl,
                         thread->queue[diskfs_private->rmw_device_id],
                         chunk_iov,
                         chunk_niov,
                         diskfs_private->rmw_device_offset + offset,
                         diskfs_write_data_sync(shared, request),
                         diskfs_io_callback,
                         request);

        offset += chunk;
        left   -= chunk;
    }

    evpl_iovecs_release(evpl, write_iov, write_niov);

} /* diskfs_write_phase2 */


static void
diskfs_ext_put_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    p->ci_cont(request);
} /* diskfs_ext_put_inserted_cb */


static void
diskfs_ext_put_insert(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_op           *op     = diskfs_bt_op_alloc(thread);

    if (diskfs_ext_insert_async(op, thread, p->txn, p->inode_stash[0],
                                p->ci_off, p->ci_len, p->ci_devid, p->ci_devoff,
                                p->ci_flags, diskfs_ext_put_inserted_cb, request)) {
        diskfs_ext_put_inserted_cb(op, op->result, request);
    }
} /* diskfs_ext_put_insert */


static void
diskfs_ext_put_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_ext_put_insert(request);
} /* diskfs_ext_put_removed_cb */


static void
diskfs_ext_put_floor_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_extent           prev;
    int                            have;

    have = diskfs_ext_from_op(op, result, &prev);
    diskfs_bt_op_free(thread, op);

    if (have && prev.flags == p->ci_flags && prev.device_id == p->ci_devid &&
        prev.file_offset + prev.length == p->ci_off &&
        prev.device_offset + prev.length == p->ci_devoff) {
        /* Contiguous predecessor: widen it (remove then re-insert at its key). */
        p->ci_off    = prev.file_offset;
        p->ci_len   += prev.length;
        p->ci_devoff = prev.device_offset;

        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_remove_async(op, thread, p->txn, p->inode_stash[0],
                                    prev.file_offset, diskfs_ext_put_removed_cb,
                                    request)) {
            diskfs_ext_put_removed_cb(op, op->result, request);
        }
        return;
    }

    diskfs_ext_put_insert(request);
} /* diskfs_ext_put_floor_cb */


void
diskfs_ext_put(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_op           *op;

    if (p->ci_off == 0) {
        diskfs_ext_put_insert(request);     /* nothing precedes offset 0 */
        return;
    }

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_ext_floor_async(op, thread, p->inode_stash[0], p->ci_off - 1,
                               p->rec_scratch, sizeof(p->rec_scratch),
                               diskfs_ext_put_floor_cb, request)) {
        diskfs_ext_put_floor_cb(op, op->result, request);
    }
} /* diskfs_ext_put */


/* Tail shared by every write path (in-place, unwritten-split, redirect):
 * stamp inode metadata, then RMW reads (if any) -> phase2 data write. */
static void
diskfs_write_finish_map(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *diskfs_private = request->plugin_data;
    struct diskfs_thread          *thread         = diskfs_private->thread;
    struct diskfs_shared          *shared         = thread->shared;
    struct evpl                   *evpl           = thread->evpl;
    struct diskfs_inode           *inode          = diskfs_private->inode_stash[0];
    uint64_t                       write_end      = request->write.offset + request->write.length;
    struct timespec                now;
    int                            size_grew = write_end > inode->size;
    int                            deferrable;

    if (size_grew) {
        inode->size       = write_end;
        inode->space_used = (inode->size + 4095) & ~4095;
    }

    clock_gettime(CLOCK_REALTIME, &now);
    inode->mtime_sec  = now.tv_sec;
    inode->mtime_nsec = now.tv_nsec;
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;
    inode->change++;

    /* POSIX kill-priv: a non-privileged write to a regular file clears the
     * set-user-ID bit and the set-group-ID bit (when group-executable).  When
     * the mode actually changes, the inode block must be journaled (the
     * deferred mtime-only path below would drop it from the txn). */
    uint32_t new_mode = chimera_vfs_killpriv_mode(request->cred, inode->mode);
    int      killpriv = (new_mode != inode->mode);
    inode->mode = new_mode;

    diskfs_map_attrs(thread, &request->write.r_post_attr, inode);

    request->write.r_length = request->write.length;

    /*
     * Deferred metadata durability.  For an in-place overwrite of an
     * already-written extent (extent map untouched), no size growth, and a
     * non-FILE_SYNC write, the only inode change is the timestamp bump (already
     * applied to the in-memory inode above, so WCC/GETATTR observe it).  Queue
     * the inode on the coalescing flusher and drop its home block from the txn
     * so this write logs nothing -- the data block is still written FUA below,
     * so data is durable; only mtime/ctime durability is deferred.  Report
     * DATA_SYNC (data durable, metadata deferred).  Everything else (size
     * growth, extent/allocation changes, FILE_SYNC) logs the inode block
     * synchronously as before and reports FILE_SYNC.
     */
    deferrable = diskfs_private->inplace_written &&
        !size_grew &&
        !killpriv &&
        request->write.sync != CHIMERA_VFS_WRITE_FILESYNC &&
        shared->mtime_defer_us > 0;

    if (deferrable) {
        struct diskfs_inode_shard *shard  = diskfs_inode_shard(shared, inode->inum);
        uint64_t                   now_ns = (uint64_t) now.tv_sec * 1000000000ULL + now.tv_nsec;

        pthread_mutex_lock(&shard->lock);
        diskfs_inode_mtime_dirty_locked(shard, inode, now_ns);
        pthread_mutex_unlock(&shard->lock);

        diskfs_txn_drop_inode_block(thread, diskfs_private->txn, inode);
        diskfs_metric_mtime(thread, DISKFS_METRIC_MTIME_DEFERRED);
        request->write.r_sync = diskfs_write_reported_sync(shared, request);
    } else {
        /* Record which gate stopped the deferral (in expression order). */
        if (!diskfs_private->inplace_written) {
            diskfs_metric_mtime(thread, DISKFS_METRIC_MTIME_SKIP_NOT_INPLACE);
        } else if (size_grew) {
            diskfs_metric_mtime(thread, DISKFS_METRIC_MTIME_SKIP_SIZE_GREW);
        } else if (request->write.sync == CHIMERA_VFS_WRITE_FILESYNC) {
            diskfs_metric_mtime(thread, DISKFS_METRIC_MTIME_SKIP_FILESYNC);
        }
        request->write.r_sync = diskfs_write_reported_sync(shared, request);
    }

    /* Do NOT release the inode lock here.  The dirty b+tree/inode blocks are
     * not yet protected by the intent log, so exposing them to another thread
     * (which could read stale state or re-dirty them) is unsafe.  The data I/O
     * below is submitted by this worker, then the txn is handed to the intent
     * log (diskfs_op_ok -> diskfs_txn_commit); the intent-log thread releases
     * the inode locks only once the record is durable (diskfs_redo_write_cb ->
     * diskfs_txn_unlock_all).  The lock is a logical flag, so holding it across
     * async I/O doesn't block the worker -- conflicting ops park as waiters. */

    if (diskfs_private->need_prefix_read || diskfs_private->need_suffix_read) {
        diskfs_private->rmw_phase = 1;

        if (diskfs_private->need_prefix_read) {
            int niov = evpl_iovec_alloc(evpl, 4096, 4096, 1, 0,
                                        &diskfs_private->rmw_prefix_iov);
            if (niov > 0) {
                diskfs_private->pending++;
                diskfs_pending_io_add(thread, 1);
                diskfs_private->rmw_prefix_pending = 1;
                diskfs_metric_block_io(thread, DISKFS_METRIC_IO_READ,
                                       DISKFS_METRIC_IO_RMW, DISKFS_BLOCK_SIZE);
                diskfs_metric_block_io_device(thread,
                                              diskfs_private->prefix_device_id,
                                              DISKFS_METRIC_IO_READ,
                                              DISKFS_METRIC_IO_RMW,
                                              DISKFS_BLOCK_SIZE);
                evpl_block_read(evpl, thread->queue[diskfs_private->prefix_device_id],
                                &diskfs_private->rmw_prefix_iov, 1,
                                diskfs_private->prefix_device_offset,
                                diskfs_write_rmw_read_callback, request);
            }
        }
        if (diskfs_private->need_suffix_read) {
            int niov = evpl_iovec_alloc(evpl, 4096, 4096, 1, 0,
                                        &diskfs_private->rmw_suffix_iov);
            if (niov > 0) {
                diskfs_private->pending++;
                diskfs_pending_io_add(thread, 1);
                diskfs_private->rmw_suffix_pending = 1;
                diskfs_metric_block_io(thread, DISKFS_METRIC_IO_READ,
                                       DISKFS_METRIC_IO_RMW, DISKFS_BLOCK_SIZE);
                diskfs_metric_block_io_device(thread,
                                              diskfs_private->suffix_device_id,
                                              DISKFS_METRIC_IO_READ,
                                              DISKFS_METRIC_IO_RMW,
                                              DISKFS_BLOCK_SIZE);
                evpl_block_read(evpl, thread->queue[diskfs_private->suffix_device_id],
                                &diskfs_private->rmw_suffix_iov, 1,
                                diskfs_private->suffix_device_offset,
                                diskfs_write_rmw_read_callback, request);
            }
        }

        if (diskfs_private->pending == 0) {
            diskfs_private->rmw_phase = 2;
            diskfs_write_phase2(thread, shared, request);
        }
    } else {
        diskfs_private->rmw_phase = 2;
        diskfs_write_phase2(thread, shared, request);
    }
} /* diskfs_write_finish_map */


/* Redirect path: record the freshly-allocated extent (coalescing it with a
 * contiguous predecessor -- e.g. a sequential append), then run the tail. */
static void
diskfs_write_trim_done(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p = request->plugin_data;

    p->ci_off    = p->rmw_aligned_start;
    p->ci_len    = p->rmw_aligned_length;
    p->ci_devid  = (uint32_t) p->rmw_device_id;
    p->ci_devoff = p->rmw_device_offset;
    p->ci_flags  = 0;
    p->ci_cont   = diskfs_write_finish_map;
    diskfs_ext_put(request);
} /* diskfs_write_trim_done */


static void
diskfs_write_trim_walk_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    p->loop_have = diskfs_ext_from_op(op, result, &p->ext_iter);
    diskfs_bt_op_free(p->thread, op);
    diskfs_write_trim_process(request);
} /* diskfs_write_trim_walk_cb */


static void
diskfs_write_trim_advance(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_op           *op     = diskfs_bt_op_alloc(thread);

    if (diskfs_ext_next_async(op, thread, p->inode_stash[0], p->ext_iter.file_offset,
                              p->rec_scratch, sizeof(p->rec_scratch),
                              diskfs_write_trim_walk_cb, request)) {
        diskfs_write_trim_walk_cb(op, op->result, request);
    }
} /* diskfs_write_trim_advance */


static void
diskfs_write_trim_advance_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request *request = private_data;

    (void) result;
    diskfs_bt_op_free(((struct diskfs_request_private *) request->plugin_data)->thread, op);
    diskfs_write_trim_advance(request);
} /* diskfs_write_trim_advance_cb */


/* spans: insert tail -> remove -> insert head -> done. */
static void
diskfs_write_trim_spans_before_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request *request = private_data;

    (void) result;
    diskfs_bt_op_free(((struct diskfs_request_private *) request->plugin_data)->thread, op);
    diskfs_write_trim_done(request);
} /* diskfs_write_trim_spans_before_cb */


static void
diskfs_write_trim_spans_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    uint64_t                       astart  = p->rmw_aligned_start;

    (void) result;
    diskfs_bt_op_free(thread, op);

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_ext_insert_async(op, thread, p->txn, p->inode_stash[0],
                                p->ext_iter.file_offset, astart - p->ext_iter.file_offset,
                                p->ext_iter.device_id, p->ext_iter.device_offset,
                                p->ext_iter.flags,
                                diskfs_write_trim_spans_before_cb, request)) {
        diskfs_write_trim_spans_before_cb(op, op->result, request);
    }
} /* diskfs_write_trim_spans_removed_cb */


static void
diskfs_write_trim_spans_after_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;

    (void) result;
    diskfs_bt_op_free(thread, op);

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_ext_remove_async(op, thread, p->txn, p->inode_stash[0],
                                p->ext_iter.file_offset,
                                diskfs_write_trim_spans_removed_cb, request)) {
        diskfs_write_trim_spans_removed_cb(op, op->result, request);
    }
} /* diskfs_write_trim_spans_after_cb */


/* overlap-left: remove -> reinsert head -> advance. */
static void
diskfs_write_trim_oleft_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    uint64_t                       astart  = p->rmw_aligned_start;

    (void) result;
    diskfs_bt_op_free(thread, op);

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_ext_insert_async(op, thread, p->txn, p->inode_stash[0],
                                p->ext_iter.file_offset, astart - p->ext_iter.file_offset,
                                p->ext_iter.device_id, p->ext_iter.device_offset,
                                p->ext_iter.flags,
                                diskfs_write_trim_advance_cb, request)) {
        diskfs_write_trim_advance_cb(op, op->result, request);
    }
} /* diskfs_write_trim_oleft_removed_cb */


/* overlap-right: remove -> reinsert tail at aligned_end -> done. */
static void
diskfs_write_trim_oright_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    uint64_t                       es      = p->ext_iter.file_offset;
    uint64_t                       ee      = es + p->ext_iter.length;
    uint64_t                       aend    = p->rmw_aligned_start + p->rmw_aligned_length;

    (void) result;
    diskfs_bt_op_free(thread, op);

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_ext_insert_async(op, thread, p->txn, p->inode_stash[0], aend, ee - aend,
                                p->ext_iter.device_id,
                                p->ext_iter.device_offset + (aend - es),
                                p->ext_iter.flags,
                                diskfs_write_trim_spans_before_cb, request)) {
        diskfs_write_trim_spans_before_cb(op, op->result, request);
    }
} /* diskfs_write_trim_oright_removed_cb */


static void
diskfs_write_trim_process(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    uint64_t                       astart = p->rmw_aligned_start;
    uint64_t                       aend   = p->rmw_aligned_start + p->rmw_aligned_length;
    uint64_t                       es, ee;
    struct diskfs_bt_op           *op;

    if (!p->loop_have) {
        diskfs_write_trim_done(request);
        return;
    }

    es = p->ext_iter.file_offset;
    ee = es + p->ext_iter.length;

    if (es >= aend) {
        diskfs_write_trim_done(request);
        return;
    }

    /* The aligned region's data is being redirected to freshly-allocated
     * blocks (rmw_device_offset), so the old device blocks backing the part of
     * this extent that the region covers are now garbage and must be freed --
     * otherwise every overwrite leaks space.  (The in-place paths never reach
     * here; they reuse the existing blocks and free nothing.) */
    if (es >= astart && ee <= aend) {
        /* Completely inside the aligned region: free + remove, then advance. */
        diskfs_thread_free_space(thread, p->txn, p->ext_iter.device_id,
                                 p->ext_iter.device_offset,
                                 SM_ALIGN_UP(p->ext_iter.length));
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_remove_async(op, thread, p->txn, p->inode_stash[0], es,
                                    diskfs_write_trim_advance_cb, request)) {
            diskfs_write_trim_advance_cb(op, op->result, request);
        }
    } else if (es < astart && ee > aend) {
        /* Spans the region: free the covered middle, then insert tail at
         * aligned_end first. */
        diskfs_thread_free_space(thread, p->txn, p->ext_iter.device_id,
                                 p->ext_iter.device_offset + (astart - es),
                                 p->rmw_aligned_length);
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_insert_async(op, thread, p->txn, p->inode_stash[0], aend,
                                    ee - aend, p->ext_iter.device_id,
                                    p->ext_iter.device_offset + (aend - es),
                                    p->ext_iter.flags,
                                    diskfs_write_trim_spans_after_cb, request)) {
            diskfs_write_trim_spans_after_cb(op, op->result, request);
        }
    } else if (es < astart && ee > astart) {
        /* Overlaps the left edge: free the covered tail, remove, reinsert the
         * head. */
        diskfs_thread_free_space(thread, p->txn, p->ext_iter.device_id,
                                 p->ext_iter.device_offset + (astart - es),
                                 ee - astart);
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_remove_async(op, thread, p->txn, p->inode_stash[0], es,
                                    diskfs_write_trim_oleft_removed_cb, request)) {
            diskfs_write_trim_oleft_removed_cb(op, op->result, request);
        }
    } else if (es < aend && ee > aend) {
        /* Starts within, extends past: free the covered head, remove, reinsert
         * tail at aligned_end. */
        diskfs_thread_free_space(thread, p->txn, p->ext_iter.device_id,
                                 p->ext_iter.device_offset, aend - es);
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_remove_async(op, thread, p->txn, p->inode_stash[0], es,
                                    diskfs_write_trim_oright_removed_cb, request)) {
            diskfs_write_trim_oright_removed_cb(op, op->result, request);
        }
    } else {
        /* No overlap (extent before aligned_start): skip. */
        diskfs_write_trim_advance(request);
    }
} /* diskfs_write_trim_process */


static void
diskfs_write_trim_first_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    int                            have    = diskfs_ext_from_op(op, result, &p->ext_iter);

    diskfs_bt_op_free(thread, op);

    if (!have) {
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_ceil_async(op, thread, p->inode_stash[0], 0, p->rec_scratch,
                                  sizeof(p->rec_scratch), diskfs_write_trim_walk_cb,
                                  request)) {
            diskfs_write_trim_walk_cb(op, op->result, request);
        }
        return;
    }

    p->loop_have = 1;
    diskfs_write_trim_process(request);
} /* diskfs_write_trim_first_cb */


static void
diskfs_write_trim_start(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_op           *op     = diskfs_bt_op_alloc(thread);

    if (diskfs_ext_floor_async(op, thread, p->inode_stash[0], p->rmw_aligned_start,
                               p->rec_scratch, sizeof(p->rec_scratch),
                               diskfs_write_trim_first_cb, request)) {
        diskfs_write_trim_first_cb(op, op->result, request);
    }
} /* diskfs_write_trim_start */


/*
 * Redirect-write edge reconstruction.
 *
 * The redirect path (hole / partial / multi-extent write) replaces the
 * block-aligned region [aligned_start, aligned_end) with a single freshly
 * allocated extent.  The two sub-block remainders the write data does not
 * cover -- the prefix [aligned_start, write_start) in the first block and the
 * suffix [write_end, aligned_end) in the last block -- must carry over the
 * file's current contents so the redirect does not disturb bytes outside the
 * written range.
 *
 * An edge block can be fragmented at sub-block boundaries: a DEALLOCATE trims
 * an extent to a raw byte offset, leaving a written piece adjacent to an
 * unwritten piece or a hole inside one block.  A single floor() lookup cannot
 * reconstruct that -- the old code read one extent and zero-filled the rest,
 * dropping any data past that extent's end.  Instead, rebuild each
 * edge block exactly as a read would: walk every extent the block overlaps into
 * a zero-initialized 4 KiB block buffer (written -> device read, unwritten /
 * hole -> left zero).  diskfs_write_phase2 then slices the prefix and suffix
 * bytes it needs out of rmw_prefix_iov / rmw_suffix_iov.  The walk runs before
 * the trim, while the old extents and their backing are still intact.
 */
static void
diskfs_write_recon_walk_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    p->loop_have = diskfs_ext_from_op(op, result, &p->ext_iter);
    diskfs_bt_op_free(p->thread, op);
    diskfs_write_recon_process(request);
} /* diskfs_write_recon_walk_cb */


static void
diskfs_write_recon_advance(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_op           *op     = diskfs_bt_op_alloc(thread);

    if (diskfs_ext_next_async(op, thread, p->inode_stash[0], p->ext_iter.file_offset,
                              p->rec_scratch, sizeof(p->rec_scratch),
                              diskfs_write_recon_walk_cb, request)) {
        diskfs_write_recon_walk_cb(op, op->result, request);
    }
} /* diskfs_write_recon_advance */


static void
diskfs_write_recon_read_cb(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;

    (void) evpl;

    if (status && p->status == 0) {
        p->status = status;
    }

    p->pending--;
    diskfs_pending_io_add(thread, -1);
    diskfs_io_resume_waiters(thread);

    /* Advance only once the walk has issued all its reads and they are done. */
    if (p->pending == 0 && !p->recon_walking) {
        diskfs_write_recon_next(request);
    }
} /* diskfs_write_recon_read_cb */


static void
diskfs_write_recon_process(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p           = request->plugin_data;
    struct diskfs_thread          *thread      = p->thread;
    struct diskfs_shared          *shared      = thread->shared;
    struct evpl                   *evpl        = thread->evpl;
    struct diskfs_extent          *extent      = &p->ext_iter;
    uint64_t                       read_offset = p->loop_off;
    uint64_t                       read_left   = p->loop_left;
    uint64_t                       block_end   = p->loop_pos;
    uint64_t                       extent_end, overlap_start, overlap_length, chunk;
    uint32_t                       chunk_niov;
    struct evpl_iovec             *chunk_iov;

    if (!(read_left && p->loop_have && extent->file_offset < block_end)) {
        /* Walk done for this edge block.  Trailing bytes (no extent) stay zero
         * because the block buffer was zero-initialized. */
        p->recon_walking = 0;
        if (p->pending == 0) {
            diskfs_write_recon_next(request);
        }
        return;
    }

    /* Bound in-flight data I/O; park (state is fully in p) and resume the walk
     * from a completion if the queue is at the cap. */
    if (diskfs_io_gate(thread, request, diskfs_write_recon_process)) {
        return;
    }

    if (read_offset < extent->file_offset) {
        chunk = extent->file_offset - read_offset;
        if (chunk > read_left) {
            chunk = read_left;
        }
        evpl_iovec_cursor_zero(&p->rd_cursor, chunk);
        read_offset += chunk;
        read_left   -= chunk;
    }

    extent_end     = extent->file_offset + extent->length;
    overlap_start  = read_offset - extent->file_offset;
    overlap_length = extent_end - read_offset;
    if (overlap_length > read_left) {
        overlap_length = read_left;
    }

    if (extent->flags & DISKFS_EXT_UNWRITTEN) {
        /* Reserved but never written: reads back as zeros, no device I/O. */
        evpl_iovec_cursor_zero(&p->rd_cursor, overlap_length);
        read_offset   += overlap_length;
        read_left     -= overlap_length;
        overlap_length = 0;
    }

    while (overlap_length) {
        uint64_t dev_offset;
        uint32_t dev_pad, total;
        int      pad_niov = 0;

        if (overlap_length > shared->devices[extent->device_id].max_request_size) {
            chunk = shared->devices[extent->device_id].max_request_size;
        } else {
            chunk = overlap_length;
        }

        chunk_iov  = &p->iov[p->niov];
        dev_offset = extent->device_offset + overlap_start;
        dev_pad    = (uint32_t) (dev_offset & 4095ULL);

        if (dev_pad) {
            evpl_iovec_clone_segment(&chunk_iov[0], &thread->pad, 0, dev_pad);
            pad_niov    = 1;
            dev_offset -= dev_pad;
        }

        chunk_niov = evpl_iovec_cursor_move(&p->rd_cursor, &chunk_iov[pad_niov],
                                            32, chunk, 1);
        chunk_niov += pad_niov;

        total = dev_pad + chunk;
        if (total & 4095) {
            evpl_iovec_clone_segment(&chunk_iov[chunk_niov], &thread->pad, 0,
                                     4096 - (total & 4095));
            chunk_niov++;
        }

        p->niov += chunk_niov;
        p->pending++;
        diskfs_pending_io_add(thread, 1);

        diskfs_metric_block_io(thread, DISKFS_METRIC_IO_READ,
                               DISKFS_METRIC_IO_RMW, chunk);
        diskfs_metric_block_io_device(thread, extent->device_id,
                                      DISKFS_METRIC_IO_READ,
                                      DISKFS_METRIC_IO_RMW, chunk);
        evpl_block_read(evpl, thread->queue[extent->device_id], chunk_iov,
                        chunk_niov, dev_offset, diskfs_write_recon_read_cb, request);

        overlap_length -= chunk;
        overlap_start  += chunk;
        read_offset    += chunk;
        read_left      -= chunk;
    }

    p->loop_off  = read_offset;
    p->loop_left = read_left;

    diskfs_write_recon_advance(request);
} /* diskfs_write_recon_process */


static void
diskfs_write_recon_first_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request     = private_data;
    struct diskfs_request_private *p           = request->plugin_data;
    struct diskfs_thread          *thread      = p->thread;
    uint64_t                       block_start = p->loop_off;
    int                            have        = diskfs_ext_from_op(op, result, &p->ext_iter);

    diskfs_bt_op_free(thread, op);

    if (have && p->ext_iter.file_offset + p->ext_iter.length <= block_start) {
        /* Floor extent ends at/before the block: start at the next extent. */
        diskfs_write_recon_advance(request);
        return;
    }
    if (!have) {
        /* No extent at/before the block start; find the first one after it. */
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_ceil_async(op, thread, p->inode_stash[0], block_start,
                                  p->rec_scratch, sizeof(p->rec_scratch),
                                  diskfs_write_recon_walk_cb, request)) {
            diskfs_write_recon_walk_cb(op, op->result, request);
        }
        return;
    }

    p->loop_have = 1;
    diskfs_write_recon_process(request);
} /* diskfs_write_recon_first_cb */


/* Begin rebuilding the current edge block (p->recon_edge: 0 = prefix first
 * block, 1 = suffix last block). */
static void
diskfs_write_recon_edge(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct evpl                   *evpl   = thread->evpl;
    uint64_t                       aend   = p->rmw_aligned_start + p->rmw_aligned_length;
    struct evpl_iovec             *buf;
    uint64_t                       block_start;
    struct diskfs_bt_op           *op;

    if (p->recon_edge == 0) {
        if (p->rmw_prefix_len == 0) {
            p->recon_edge = 1;
            diskfs_write_recon_edge(request);
            return;
        }
        buf                 = &p->rmw_prefix_iov;
        block_start         = p->rmw_aligned_start;
        p->rmw_prefix_valid = p->rmw_prefix_len;
    } else {
        if (p->rmw_suffix_len == 0) {
            diskfs_write_trim_start(request);
            return;
        }
        buf                  = &p->rmw_suffix_iov;
        block_start          = aend - 4096;
        p->rmw_suffix_valid  = p->rmw_suffix_len;
        p->rmw_suffix_adjust = 0;
    }

    if (evpl_iovec_alloc(evpl, 4096, 4096, 1, 0, buf) <= 0) {
        /* The other edge's buffer (the prefix, when this is the suffix) is
         * already allocated; release it so the failure path leaks nothing. */
        if (p->rmw_prefix_iov.data) {
            evpl_iovec_release(evpl, &p->rmw_prefix_iov);
            p->rmw_prefix_iov.data = NULL;
        }
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EIO);
        return;
    }
    memset(buf->data, 0, 4096);

    evpl_iovec_cursor_init(&p->rd_cursor, buf, 1);
    p->loop_off      = block_start;
    p->loop_left     = 4096;
    p->loop_pos      = block_start + 4096;
    p->loop_have     = 0;
    p->recon_walking = 1;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_ext_floor_async(op, thread, p->inode_stash[0], block_start,
                               p->rec_scratch, sizeof(p->rec_scratch),
                               diskfs_write_recon_first_cb, request)) {
        diskfs_write_recon_first_cb(op, op->result, request);
    }
} /* diskfs_write_recon_edge */


/* An edge block is fully rebuilt (walk done, all reads complete): release the
 * device-read iovec refs and move on (prefix -> suffix -> trim). */
static void
diskfs_write_recon_next(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;

    /* Release the per-chunk device-read iovec refs (slices of the edge buffer);
     * the buffer's own ref stays in rmw_prefix_iov / rmw_suffix_iov until
     * phase2 consumes it. */
    if (p->niov) {
        evpl_iovecs_release(thread->evpl, p->iov, p->niov);
        p->niov = 0;
    }

    if (p->status) {
        if (p->rmw_prefix_iov.data) {
            evpl_iovec_release(thread->evpl, &p->rmw_prefix_iov);
            p->rmw_prefix_iov.data = NULL;
        }
        if (p->rmw_suffix_iov.data) {
            evpl_iovec_release(thread->evpl, &p->rmw_suffix_iov);
            p->rmw_suffix_iov.data = NULL;
        }
        diskfs_op_fail(request, p->txn, p->status);
        return;
    }

    if (p->recon_edge == 0) {
        p->recon_edge = 1;
        diskfs_write_recon_edge(request);
    } else {
        diskfs_write_trim_start(request);
    }
} /* diskfs_write_recon_next */


/*
 * A write is dispatched here with the inode write-locked.  Compute the 4 KiB-
 * aligned region, then classify it (diskfs_write_classify_cb) against the
 * extent at/just-before aligned_start:
 *
 *   - one existing extent fully covers the region -> overwrite IN PLACE:
 *       written   -> no map mutation at all (the hot random-overwrite path:
 *                    no allocate, no free, no b+tree churn);
 *       unwritten -> overwrite in place + split the covered range to written
 *                    (fallocate first-touch);
 *   - otherwise (hole / partial / multi-extent span) -> allocate fresh blocks
 *     and redirect, freeing the old covered blocks (diskfs_write_trim_*).
 *
 * Multi-extent-cover redirects for now; coalescing keeps written runs in a
 * single extent so contiguous regions collapse to the in-place case.
 */
static void
diskfs_write_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request     = private_data;
    struct diskfs_request_private *p           = request->plugin_data;
    struct diskfs_thread          *thread      = p->thread;
    uint64_t                       write_start = request->write.offset;
    uint64_t                       write_end   = write_start + request->write.length;
    uint64_t                       aligned_start, aligned_end;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (unlikely(!S_ISREG(inode->mode))) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EINVAL);
        return;
    }

    diskfs_map_attrs(thread, &request->write.r_pre_attr, inode);

    if (request->write.length == 0) {
        diskfs_map_attrs(thread, &request->write.r_post_attr, inode);

        request->write.r_length = 0;
        request->write.r_sync   = diskfs_write_reported_sync(thread->shared, request);
        diskfs_op_ok(request, p->txn);
        return;
    }

    aligned_start = write_start & ~4095ULL;
    aligned_end   = (write_end + 4095ULL) & ~4095ULL;

    p->rmw_prefix_len     = write_start - aligned_start;
    p->rmw_suffix_len     = aligned_end - write_end;
    p->rmw_aligned_start  = aligned_start;
    p->rmw_aligned_length = aligned_end - aligned_start;
    p->rmw_prefix_valid   = 0;
    p->rmw_suffix_valid   = 0;
    p->rmw_suffix_adjust  = 0;
    p->need_prefix_read   = 0;
    p->need_suffix_read   = 0;
    p->inode_stash[0]     = inode;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_ext_floor_async(op, thread, inode, aligned_start, p->rec_scratch,
                               sizeof(p->rec_scratch), diskfs_write_classify_cb,
                               request)) {
        diskfs_write_classify_cb(op, op->result, request);
    }
} /* diskfs_write_inode_cb */


static void
diskfs_write_classify_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    uint64_t                       astart  = p->rmw_aligned_start;
    uint64_t                       aend    = astart + p->rmw_aligned_length;
    struct diskfs_extent           e;
    int                            have;

    have = diskfs_ext_from_op(op, result, &e);
    diskfs_bt_op_free(thread, op);

    if (have && e.file_offset <= astart && e.file_offset + e.length >= aend) {
        /* Single extent fully covers the region: overwrite its blocks in
         * place at the matching device offset. */
        p->rmw_device_id     = e.device_id;
        p->rmw_device_offset = e.device_offset + (astart - e.file_offset);

        if (e.flags & DISKFS_EXT_UNWRITTEN) {
            /* The un-overwritten bytes of the first/last block are zeros (not
             * stale data), so phase2 zero-fills the prefix/suffix -- no RMW
             * read.  Split the covered range to written. */
            p->ext_iter = e;
            diskfs_write_split_start(request);
        } else {
            /* RMW the partial first/last blocks from these same in-place
             * blocks; the extent map is left untouched -- the only inode change
             * is the timestamp bump, which a non-FILE_SYNC write may defer. */
            p->inplace_written = 1;
            if (p->rmw_prefix_len) {
                p->need_prefix_read     = 1;
                p->prefix_device_id     = e.device_id;
                p->prefix_device_offset = p->rmw_device_offset;
                p->rmw_prefix_valid     = p->rmw_prefix_len;
            }
            if (p->rmw_suffix_len) {
                uint64_t write_end    = request->write.offset + request->write.length;
                uint64_t suffix_block = write_end & ~4095ULL;

                p->need_suffix_read     = 1;
                p->suffix_device_id     = e.device_id;
                p->suffix_device_offset = e.device_offset +
                    (suffix_block - e.file_offset);
                p->rmw_suffix_valid = p->rmw_suffix_len;
            }
            diskfs_write_finish_map(request);
        }
        return;
    }

    /* Hole / partial / multi-extent span: allocate fresh blocks and redirect. */
    diskfs_write_redirect_alloc(request);
} /* diskfs_write_classify_cb */


static void
diskfs_write_redirect_alloc(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    uint64_t                       dev_id, dev_off;
    int                            rc;

    rc = diskfs_inode_alloc_space(thread, p->txn, p->inode_stash[0],
                                  (int64_t) p->rmw_aligned_length,
                                  SM_RESERVATION_MIN,
                                  &dev_id, &dev_off,
                                  diskfs_write_alloc_resume, request);
    if (rc == SM_AGAIN) {
        return;     /* parked; diskfs_write_alloc_resume re-runs */
    }
    if (rc) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOSPC);
        return;
    }

    p->rmw_device_id     = dev_id;
    p->rmw_device_offset = dev_off;

    /* Rebuild the prefix/suffix edge blocks (fragmentation-aware) before the
     * trim frees the old backing, then continue into diskfs_write_trim_start. */
    p->recon_edge = 0;
    diskfs_write_recon_edge(request);
} /* diskfs_write_redirect_alloc */


static void
diskfs_write_alloc_resume(
    struct diskfs_thread *thread,
    void                 *arg)
{
    (void) thread;
    diskfs_write_redirect_alloc((struct chimera_vfs_request *) arg);
} /* diskfs_write_alloc_resume */


/*
 * Unwritten-extent in-place split: the write lands inside a reserved-but-
 * unwritten extent e=[es,ee) (stashed in p->ext_iter).  Its blocks are
 * overwritten in place; re-record the map as
 *     [es,astart) unwritten | [astart,aend) written | [aend,ee) unwritten
 * (head/tail only when non-empty; the written middle reuses e's blocks at
 * rmw_device_offset).  remove -> insert middle -> [head] -> [tail] -> tail.
 */
static void
diskfs_write_split_tail_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_write_finish_map(request);
} /* diskfs_write_split_tail_cb */


static void
diskfs_write_split_finish_tail(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    uint64_t                       es     = p->ext_iter.file_offset;
    uint64_t                       ee     = es + p->ext_iter.length;
    uint64_t                       aend   = p->rmw_aligned_start + p->rmw_aligned_length;
    struct diskfs_bt_op           *op;

    if (ee > aend) {
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_insert_async(op, thread, p->txn, p->inode_stash[0], aend,
                                    ee - aend, p->ext_iter.device_id,
                                    p->ext_iter.device_offset + (aend - es),
                                    DISKFS_EXT_UNWRITTEN,
                                    diskfs_write_split_tail_cb, request)) {
            diskfs_write_split_tail_cb(op, op->result, request);
        }
    } else {
        diskfs_write_finish_map(request);
    }
} /* diskfs_write_split_finish_tail */


static void
diskfs_write_split_head_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_write_split_finish_tail(request);
} /* diskfs_write_split_head_cb */


static void
diskfs_write_split_finish_head(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    uint64_t                       es     = p->ext_iter.file_offset;
    uint64_t                       astart = p->rmw_aligned_start;
    struct diskfs_bt_op           *op;

    if (es < astart) {
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_insert_async(op, thread, p->txn, p->inode_stash[0], es,
                                    astart - es, p->ext_iter.device_id,
                                    p->ext_iter.device_offset,
                                    DISKFS_EXT_UNWRITTEN,
                                    diskfs_write_split_head_cb, request)) {
            diskfs_write_split_head_cb(op, op->result, request);
        }
    } else {
        diskfs_write_split_finish_tail(request);
    }
} /* diskfs_write_split_finish_head */


static void
diskfs_write_split_mid_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_write_split_finish_head(request);
} /* diskfs_write_split_mid_cb */


static void
diskfs_write_split_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;

    (void) result;
    diskfs_bt_op_free(thread, op);

    /* Insert the written middle, reusing e's blocks (rmw_device_offset). */
    op = diskfs_bt_op_alloc(thread);
    if (diskfs_ext_insert_async(op, thread, p->txn, p->inode_stash[0],
                                p->rmw_aligned_start, p->rmw_aligned_length,
                                (uint32_t) p->rmw_device_id, p->rmw_device_offset, 0,
                                diskfs_write_split_mid_cb, request)) {
        diskfs_write_split_mid_cb(op, op->result, request);
    }
} /* diskfs_write_split_removed_cb */


static void
diskfs_write_split_start(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_op           *op     = diskfs_bt_op_alloc(thread);

    if (diskfs_ext_remove_async(op, thread, p->txn, p->inode_stash[0],
                                p->ext_iter.file_offset,
                                diskfs_write_split_removed_cb, request)) {
        diskfs_write_split_removed_cb(op, op->result, request);
    }
} /* diskfs_write_split_start */



void
diskfs_write(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;

    (void) private_data;

    /* Block/SCSI-mode data lives on remote (pNFS) devices: writes go directly
     * from the client to the volume via an RW layout, never inline through the
     * server (which has no handle for those devices). */
    if (unlikely(shared->block_layout || shared->scsi_layout)) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    p->opcode              = request->opcode;
    p->status              = 0;
    p->pending             = 0;
    p->niov                = 0;
    p->thread              = thread;
    p->rmw_phase           = 0;
    p->rmw_prefix_iov.data = NULL;
    p->rmw_suffix_iov.data = NULL;
    p->rmw_prefix_pending  = 0;
    p->rmw_suffix_pending  = 0;
    p->rmw_prefix_valid    = 0;
    p->rmw_suffix_adjust   = 0;
    p->rmw_suffix_valid    = 0;
    p->need_prefix_read    = 0;
    p->need_suffix_read    = 0;
    p->inplace_written     = 0;
    p->txn                 = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);

    /* Warm-handle fast path (see diskfs_read): reuse the inode pinned at open
    * via handle->vfs_private, skipping the by-fh resolve.  The WRITE-mode grant
    * (incl. the home-block pin) is preserved by diskfs_inode_grant_locked. */
    if (request->write.handle && request->write.handle->vfs_private) {
        diskfs_inode_acquire_pinned(thread, p->txn,
                                    (struct diskfs_inode *) request->write.handle->vfs_private,
                                    DISKFS_INODE_MODE_FOR_TXN(p->txn),
                                    diskfs_write_inode_cb, request);
    } else {
        diskfs_inode_get_fh_async(thread, p->txn,
                                  request->fh, request->fh_len,
                                  diskfs_write_inode_cb, request);
    }
} /* diskfs_write */


static void
diskfs_allocate_finalize(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_inode           *inode  = p->inode_stash[0];
    struct timespec                now;

    clock_gettime(CLOCK_REALTIME, &now);
    inode->mtime_sec  = now.tv_sec;
    inode->mtime_nsec = now.tv_nsec;
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;
    inode->change++;

    diskfs_map_attrs(thread, &request->allocate.r_post_attr, inode);
    diskfs_op_ok(request, p->txn);
} /* diskfs_allocate_finalize */


static void
diskfs_dealloc_finish(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p     = request->plugin_data;
    struct diskfs_inode           *inode = p->inode_stash[0];

    inode->space_used = (inode->size + 4095) & ~4095;
    diskfs_allocate_finalize(request);
} /* diskfs_dealloc_finish */


static void
diskfs_dealloc_walk_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    p->loop_have = diskfs_ext_from_op(op, result, &p->ext_iter);
    diskfs_bt_op_free(p->thread, op);
    diskfs_dealloc_process(request);
} /* diskfs_dealloc_walk_cb */


static void
diskfs_dealloc_advance(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_op           *op     = diskfs_bt_op_alloc(thread);

    if (diskfs_ext_next_async(op, thread, p->inode_stash[0], p->ext_iter.file_offset,
                              p->rec_scratch, sizeof(p->rec_scratch),
                              diskfs_dealloc_walk_cb, request)) {
        diskfs_dealloc_walk_cb(op, op->result, request);
    }
} /* diskfs_dealloc_advance */


/* Generic "advance after a single async modify" continuation. */
static void
diskfs_dealloc_modify_advance_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request *request = private_data;

    (void) result;
    diskfs_bt_op_free(((struct diskfs_request_private *) request->plugin_data)->thread, op);
    diskfs_dealloc_advance(request);
} /* diskfs_dealloc_modify_advance_cb */


static void
diskfs_dealloc_modify_finish_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request *request = private_data;

    (void) result;
    diskfs_bt_op_free(((struct diskfs_request_private *) request->plugin_data)->thread, op);
    diskfs_dealloc_finish(request);
} /* diskfs_dealloc_modify_finish_cb */


/* overlap-start: after removing the slot, reinsert the trimmed head. */
static void
diskfs_dealloc_ostart_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;

    (void) result;
    diskfs_bt_op_free(thread, op);

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_ext_insert_async(op, thread, p->txn, p->inode_stash[0],
                                p->ext_iter.file_offset,
                                p->loop_off - p->ext_iter.file_offset,
                                p->ext_iter.device_id, p->ext_iter.device_offset,
                                p->ext_iter.flags,
                                diskfs_dealloc_modify_advance_cb, request)) {
        diskfs_dealloc_modify_advance_cb(op, op->result, request);
    }
} /* diskfs_dealloc_ostart_removed_cb */


/* overlap-end: after removing, reinsert the trimmed tail at hole_end. */
static void
diskfs_dealloc_oend_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    uint64_t                       es      = p->ext_iter.file_offset;
    uint64_t                       ee      = es + p->ext_iter.length;
    uint64_t                       he      = p->loop_left;

    (void) result;
    diskfs_bt_op_free(thread, op);

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_ext_insert_async(op, thread, p->txn, p->inode_stash[0], he, ee - he,
                                p->ext_iter.device_id,
                                p->ext_iter.device_offset + (he - es),
                                p->ext_iter.flags,
                                diskfs_dealloc_modify_finish_cb, request)) {
        diskfs_dealloc_modify_finish_cb(op, op->result, request);
    }
} /* diskfs_dealloc_oend_removed_cb */


/* spans: insert tail -> remove -> insert head -> finish. */
static void
diskfs_dealloc_spans_before_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request *request = private_data;

    (void) result;
    diskfs_bt_op_free(((struct diskfs_request_private *) request->plugin_data)->thread, op);
    diskfs_dealloc_finish(request);
} /* diskfs_dealloc_spans_before_cb */


static void
diskfs_dealloc_spans_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;

    (void) result;
    diskfs_bt_op_free(thread, op);

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_ext_insert_async(op, thread, p->txn, p->inode_stash[0],
                                p->ext_iter.file_offset,
                                p->loop_off - p->ext_iter.file_offset,
                                p->ext_iter.device_id, p->ext_iter.device_offset,
                                p->ext_iter.flags,
                                diskfs_dealloc_spans_before_cb, request)) {
        diskfs_dealloc_spans_before_cb(op, op->result, request);
    }
} /* diskfs_dealloc_spans_removed_cb */


static void
diskfs_dealloc_spans_after_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;

    (void) result;
    diskfs_bt_op_free(thread, op);

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_ext_remove_async(op, thread, p->txn, p->inode_stash[0],
                                p->ext_iter.file_offset,
                                diskfs_dealloc_spans_removed_cb, request)) {
        diskfs_dealloc_spans_removed_cb(op, op->result, request);
    }
} /* diskfs_dealloc_spans_after_cb */


static void
diskfs_dealloc_process(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p          = request->plugin_data;
    struct diskfs_thread          *thread     = p->thread;
    uint64_t                       hole_start = p->loop_off;
    uint64_t                       hole_end   = p->loop_left;
    uint64_t                       es, ee;
    struct diskfs_bt_op           *op;

    if (!p->loop_have) {
        diskfs_dealloc_finish(request);
        return;
    }

    es = p->ext_iter.file_offset;
    ee = es + p->ext_iter.length;

    if (ee <= hole_start) {     /* entirely before the hole: skip */
        diskfs_dealloc_advance(request);
        return;
    }
    if (es >= hole_end) {       /* at/after hole end: done */
        diskfs_dealloc_finish(request);
        return;
    }

    if (es >= hole_start && ee <= hole_end) {
        /* Completely inside the hole: free + remove, then advance. */
        diskfs_thread_free_space(thread, p->txn, p->ext_iter.device_id,
                                 p->ext_iter.device_offset,
                                 SM_ALIGN_UP(p->ext_iter.length));
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_remove_async(op, thread, p->txn, p->inode_stash[0], es,
                                    diskfs_dealloc_modify_advance_cb, request)) {
            diskfs_dealloc_modify_advance_cb(op, op->result, request);
        }
    } else if (es < hole_start && ee > hole_end) {
        /* Spans the hole: free the punched-out middle [hole_start, hole_end) of
         * this extent's backing, then insert tail at hole_end.  The raw range is
         * passed (not block-rounded): space_map_free frees only whole blocks
         * strictly contained, so a sub-block hole edge shared with a kept piece
         * is never freed. */
        diskfs_thread_free_space(thread, p->txn, p->ext_iter.device_id,
                                 p->ext_iter.device_offset + (hole_start - es),
                                 hole_end - hole_start);
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_insert_async(op, thread, p->txn, p->inode_stash[0], hole_end,
                                    ee - hole_end, p->ext_iter.device_id,
                                    p->ext_iter.device_offset + (hole_end - es),
                                    p->ext_iter.flags,
                                    diskfs_dealloc_spans_after_cb, request)) {
            diskfs_dealloc_spans_after_cb(op, op->result, request);
        }
    } else if (es < hole_start) {
        /* Overlaps the hole start: free the punched tail [hole_start, ee) of this
         * extent's backing, then remove + reinsert the kept head [es, hole_start). */
        diskfs_thread_free_space(thread, p->txn, p->ext_iter.device_id,
                                 p->ext_iter.device_offset + (hole_start - es),
                                 ee - hole_start);
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_remove_async(op, thread, p->txn, p->inode_stash[0], es,
                                    diskfs_dealloc_ostart_removed_cb, request)) {
            diskfs_dealloc_ostart_removed_cb(op, op->result, request);
        }
    } else {
        /* Overlaps the hole end: free the punched head [es, hole_end) of this
         * extent's backing, then remove + reinsert the kept tail at hole_end. */
        diskfs_thread_free_space(thread, p->txn, p->ext_iter.device_id,
                                 p->ext_iter.device_offset,
                                 hole_end - es);
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_remove_async(op, thread, p->txn, p->inode_stash[0], es,
                                    diskfs_dealloc_oend_removed_cb, request)) {
            diskfs_dealloc_oend_removed_cb(op, op->result, request);
        }
    }
} /* diskfs_dealloc_process */


static void
diskfs_dealloc_first_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    int                            have    = diskfs_ext_from_op(op, result, &p->ext_iter);

    diskfs_bt_op_free(thread, op);

    if (!have) {
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_ceil_async(op, thread, p->inode_stash[0], 0, p->rec_scratch,
                                  sizeof(p->rec_scratch), diskfs_dealloc_walk_cb,
                                  request)) {
            diskfs_dealloc_walk_cb(op, op->result, request);
        }
        return;
    }

    p->loop_have = 1;
    diskfs_dealloc_process(request);
} /* diskfs_dealloc_first_cb */


static void
diskfs_allocate_alloc_resume(
    struct diskfs_thread *thread,
    void                 *arg)
{
    (void) thread;
    diskfs_allocate_do_alloc((struct chimera_vfs_request *) arg);
} /* diskfs_allocate_alloc_resume */


static void
diskfs_allocate_next(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p = request->plugin_data;

    if (p->loop_off < p->loop_left) {
        diskfs_allocate_do_alloc(request);      /* more of this gap */
    } else {
        diskfs_allocate_reserve_step(request);  /* on to the next gap */
    }
} /* diskfs_allocate_next */


static void
diskfs_allocate_do_alloc(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    uint64_t                       off    = p->loop_off;
    uint64_t                       dev_id, dev_off, chunk;
    int                            rc;

    chunk = p->loop_left - off;
    if (chunk > p->alloc_cap) {
        chunk = p->alloc_cap;
    }

    rc = diskfs_inode_alloc_space(thread, p->txn, p->inode_stash[0],
                                  (int64_t) chunk, 0 /* exact, no retained tail */,
                                  &dev_id, &dev_off,
                                  diskfs_allocate_alloc_resume, request);
    if (rc == SM_AGAIN) {
        return;     /* parked; resume re-drives diskfs_allocate_do_alloc */
    }
    if (rc) {
        /* No single allocation group has `chunk` contiguous free space, but the
         * filesystem as a whole may: a space map reservation is per-AG and
         * all-or-nothing (and failure is a cheap in-memory check, no journal).
         * Halve the cap and retry so a large fallocate spreads across AGs;
         * only a request that can't place even one block is truly ENOSPC. */
        if (chunk > SM_BLOCK_SIZE) {
            p->alloc_cap = chunk >> 1;
            diskfs_allocate_do_alloc(request);
            return;
        }
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOSPC);
        return;
    }

    /* Success: re-probe a large chunk for the next allocation. */
    p->alloc_cap = DISKFS_ALLOCATE_MAX_EXTENT;

    /* Advance before recording: diskfs_ext_put may complete inline and run
     * diskfs_allocate_next, which reads loop_off.  Record the reserved chunk
     * as an unwritten extent, coalescing contiguous chunks into one. */
    p->loop_off  = off + chunk;
    p->ci_off    = off;
    p->ci_len    = chunk;
    p->ci_devid  = (uint32_t) dev_id;
    p->ci_devoff = dev_off;
    p->ci_flags  = DISKFS_EXT_UNWRITTEN;
    p->ci_cont   = diskfs_allocate_next;
    diskfs_ext_put(request);
} /* diskfs_allocate_do_alloc */


static void
diskfs_allocate_holeend_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_extent           next;
    int                            have;

    have = diskfs_ext_from_op(op, result, &next);
    diskfs_bt_op_free(p->thread, op);

    /* The gap runs from loop_off to the next extent, clamped to the end. */
    p->loop_left = p->loop_pos;
    if (have && next.file_offset < p->loop_left) {
        p->loop_left = next.file_offset;
    }
    diskfs_allocate_do_alloc(request);
} /* diskfs_allocate_holeend_cb */


static void
diskfs_allocate_floor_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_extent           e;
    int                            have;

    have = diskfs_ext_from_op(op, result, &e);
    diskfs_bt_op_free(thread, op);

    if (have && e.file_offset + e.length > p->loop_off) {
        /* Already backed (written or unwritten): skip past this extent.  Round
         * up to the next block: an extent that ends mid-block (e.g. a partial
         * last block left by truncate) physically owns the rest of that block,
         * so a gap allocation must start at the next block boundary.  Starting
         * at an unaligned file_offset would pair it with a block-aligned device
         * offset from the space map, breaking the file==device block alignment
         * that diskfs's block I/O relies on. */
        p->loop_off = (e.file_offset + e.length + 4095) & ~4095ULL;
        diskfs_allocate_reserve_step(request);
        return;
    }

    /* Hole at loop_off: find where it ends (next extent at/after loop_off). */
    op = diskfs_bt_op_alloc(thread);
    if (diskfs_ext_ceil_async(op, thread, p->inode_stash[0], p->loop_off,
                              p->rec_scratch, sizeof(p->rec_scratch),
                              diskfs_allocate_holeend_cb, request)) {
        diskfs_allocate_holeend_cb(op, op->result, request);
    }
} /* diskfs_allocate_floor_cb */


static void
diskfs_allocate_reserve_step(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_op           *op;

    if (p->loop_off >= p->loop_pos) {
        /* Reservation complete; extend the logical size if the request did. */
        struct diskfs_inode *inode   = p->inode_stash[0];
        uint64_t             new_end = request->allocate.offset +
            request->allocate.length;

        if (new_end > inode->size) {
            inode->size       = new_end;
            inode->space_used = (inode->size + 4095) & ~4095;
        }
        diskfs_allocate_finalize(request);
        return;
    }

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_ext_floor_async(op, thread, p->inode_stash[0], p->loop_off,
                               p->rec_scratch, sizeof(p->rec_scratch),
                               diskfs_allocate_floor_cb, request)) {
        diskfs_allocate_floor_cb(op, op->result, request);
    }
} /* diskfs_allocate_reserve_step */


static void
diskfs_allocate_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    diskfs_map_attrs(thread, &request->allocate.r_pre_attr, inode);
    p->inode_stash[0] = inode;

    if (request->allocate.flags & CHIMERA_VFS_ALLOCATE_DEALLOCATE) {
        uint64_t hole_start = request->allocate.offset;
        uint64_t hole_end   = hole_start + request->allocate.length;

        if (hole_end > inode->size) {
            hole_end = inode->size;
        }

        if (hole_start < hole_end) {
            p->loop_off  = hole_start;
            p->loop_left = hole_end;

            op = diskfs_bt_op_alloc(thread);
            if (diskfs_ext_floor_async(op, thread, inode, hole_start, p->rec_scratch,
                                       sizeof(p->rec_scratch), diskfs_dealloc_first_cb,
                                       request)) {
                diskfs_dealloc_first_cb(op, op->result, request);
            }
            return;
        }
    } else if (request->allocate.length) {
        /* ALLOCATE: reserve backing space for any gap in the (block-aligned)
         * requested range as UNWRITTEN extents, then extend the size.  The
         * walk drives diskfs_allocate_finalize when it completes. */
        p->loop_off = request->allocate.offset & ~4095ULL;
        p->loop_pos = (request->allocate.offset + request->allocate.length +
                       4095ULL) & ~4095ULL;
        p->alloc_cap = DISKFS_ALLOCATE_MAX_EXTENT;
        diskfs_allocate_reserve_step(request);
        return;
    }

    diskfs_allocate_finalize(request);
} /* diskfs_allocate_inode_cb */


void
diskfs_allocate(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);

    diskfs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              diskfs_allocate_inode_cb, request);
} /* diskfs_allocate */


static void
diskfs_seek_walk_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    p->loop_have = diskfs_ext_from_op(op, result, &p->ext_iter);
    diskfs_bt_op_free(p->thread, op);
    diskfs_seek_process(request);
} /* diskfs_seek_walk_cb */


static void
diskfs_seek_advance(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_op           *op     = diskfs_bt_op_alloc(thread);

    if (diskfs_ext_next_async(op, thread, p->inode_stash[0], p->ext_iter.file_offset,
                              p->rec_scratch, sizeof(p->rec_scratch),
                              diskfs_seek_walk_cb, request)) {
        diskfs_seek_walk_cb(op, op->result, request);
    }
} /* diskfs_seek_advance */


static void
diskfs_seek_process(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_inode           *inode  = p->inode_stash[0];
    uint64_t                       offset = request->seek.offset;
    uint64_t                       extent_end;

    if (request->seek.what == 0) {
        /* SEEK_DATA: first extent whose data covers/follows offset. */
        if (!p->loop_have) {
            /* No data at or beyond the offset: SEEK_DATA fails with NXIO. */
            diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENXIO);
            return;
        }

        extent_end = p->ext_iter.file_offset + p->ext_iter.length;
        if (extent_end > offset) {
            request->seek.r_offset = (p->ext_iter.file_offset > offset) ?
                p->ext_iter.file_offset : offset;
            request->seek.r_eof = 0;
            diskfs_op_ok(request, p->txn);
            return;
        }
        diskfs_seek_advance(request);
    } else {
        /* SEEK_HOLE: first gap from loop_pos forward. */
        if (!p->loop_have) {
            request->seek.r_offset = (p->loop_pos < inode->size) ?
                p->loop_pos : inode->size;
            /* The implicit hole at EOF is reached only once the returned offset
             * meets the logical size; RFC 7862 §11.4.4 wants sr_eof TRUE there
             * (the Linux client surfaces it to lseek).  A gap that starts before
             * the size is a real hole short of EOF. */
            request->seek.r_eof = (request->seek.r_offset >= inode->size);
            diskfs_op_ok(request, p->txn);
            return;
        }

        extent_end = p->ext_iter.file_offset + p->ext_iter.length;
        if (extent_end <= p->loop_pos) {
            diskfs_seek_advance(request);
            return;
        }
        if (p->ext_iter.file_offset > p->loop_pos) {
            request->seek.r_offset = p->loop_pos;
            request->seek.r_eof    = 0;
            diskfs_op_ok(request, p->txn);
            return;
        }
        p->loop_pos = extent_end;
        diskfs_seek_advance(request);
    }

    (void) thread;
} /* diskfs_seek_process */


/* First-extent selection: floor(offset), advancing to the next extent if the
 * floor extent ends at/before offset, or the first extent if none. */
static void
diskfs_seek_first_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    uint64_t                       offset  = request->seek.offset;
    int                            have    = diskfs_ext_from_op(op, result, &p->ext_iter);

    diskfs_bt_op_free(thread, op);

    if (have && p->ext_iter.file_offset + p->ext_iter.length <= offset) {
        diskfs_seek_advance(request);
        return;
    }
    if (!have) {
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_ceil_async(op, thread, p->inode_stash[0], 0, p->rec_scratch,
                                  sizeof(p->rec_scratch), diskfs_seek_walk_cb,
                                  request)) {
            diskfs_seek_walk_cb(op, op->result, request);
        }
        return;
    }

    p->loop_have = 1;
    diskfs_seek_process(request);
} /* diskfs_seek_first_cb */


static void
diskfs_seek_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    uint64_t                       offset  = request->seek.offset;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (offset >= inode->size) {
        /* No data or hole at or beyond EOF: SEEK must fail with NXIO
         * (POSIX lseek ENXIO / RFC 7862 NFS4ERR_NXIO). */
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENXIO);
        return;
    }

    p->inode_stash[0] = inode;
    p->loop_pos       = offset;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_ext_floor_async(op, thread, inode, offset, p->rec_scratch,
                               sizeof(p->rec_scratch), diskfs_seek_first_cb,
                               request)) {
        diskfs_seek_first_cb(op, op->result, request);
    }
} /* diskfs_seek_inode_cb */


void
diskfs_seek(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, DISKFS_TXN_READ);

    diskfs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              diskfs_seek_inode_cb, request);
} /* diskfs_seek */


/*
 * COMMIT.  Data is already durable (written FUA), so COMMIT's only remaining
 * job is to make any deferred mtime/ctime durable for a committing client.  If
 * the file has a pending deferred timestamp, fold its inode block into a write
 * txn so the commit logs it; otherwise (or with the flusher disabled) it is the
 * old inline no-op.  The write lock serializes against the background flusher,
 * so a concurrent flush either completes first (COMMIT then sees it clean) or
 * waits behind COMMIT.
 */
static void
diskfs_commit_acquired_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *priv)
{
    struct chimera_vfs_request    *request = priv;
    struct diskfs_request_private *cp      = request->plugin_data;
    struct diskfs_thread          *thread  = cp->thread;
    struct diskfs_inode_shard     *shard;

    if (status != CHIMERA_VFS_OK) {
        diskfs_txn_abort(cp->txn);
        request->status = status;
        request->complete(request);
        return;
    }

    shard = diskfs_inode_shard(thread->shared, inode->inum);
    pthread_mutex_lock(&shard->lock);
    if (inode->mtime_dirty) {
        diskfs_inode_mtime_unlink_locked(shard, inode);
        /* Drop the dirty-pin; the txn write lock holds the inode.  Never the
         * final reference: the COMMIT's own open handle still holds one, so
         * no reclaim check is needed here. */
        --inode->refcnt;
        pthread_mutex_unlock(&shard->lock);
        diskfs_txn_pin_inode_block(thread, cp->txn, inode, 0);
    } else {
        pthread_mutex_unlock(&shard->lock);
    }

    diskfs_op_ok(request, cp->txn);     /* logs the inode if pinned, else inline */
} /* diskfs_commit_acquired_cb */


void
diskfs_commit(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *cp = request->plugin_data;
    struct diskfs_inode           *warm;

    (void) private_data;
    cp->thread = thread;

    warm = (request->commit.handle && request->commit.handle->vfs_private) ?
        (struct diskfs_inode *) request->commit.handle->vfs_private : NULL;

    if (!warm || shared->mtime_defer_us == 0) {
        cp->txn = diskfs_txn_begin(thread, DISKFS_TXN_READ);
        diskfs_op_ok(request, cp->txn);
        return;
    }

    cp->txn = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);
    diskfs_inode_acquire_pinned(thread, cp->txn, warm, DISKFS_INODE_LOCK_WRITE,
                                diskfs_commit_acquired_cb, request);
} /* diskfs_commit */
