// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Reflink (NFSv4.2 CLONE) for diskfs: true copy-on-write sharing of byte ranges
 * between files.
 *
 * Sharing is tracked by a refcount per shared device range, stored as
 * DISKFS_REC_REFCOUNT records in a statically-reserved refcount inode
 * (DISKFS_REFCOUNT_INUM_BASE).  An extent that participates in sharing carries
 * the DISKFS_EXT_SHARED flag; a write to such an extent must copy-on-write
 * (diskfs_cow_privatize, driven from the write path) and a free of such an
 * extent must decrement instead of releasing the backing space
 * (diskfs_ext_release, called from truncate/unlink/deallocate).
 *
 * Refcount semantics: a device range with a sole owner has NO record (implicit
 * count 1) and is not flagged SHARED.  The first clone creates a record with
 * count 2 and flags both extents SHARED.  Privatizing or freeing a shared
 * extent decrements; at count 1 the record is removed (one owner remains, who
 * keeps the blocks); a decrement that finds no record means this was the last
 * owner and the blocks are freed.
 *
 * Crash consistency: every refcount record update, extent-map change, and the
 * conditional space free all ride the one diskfs txn (redo log), so a crash
 * either keeps or discards the whole clone/CoW atomically.
 */

#include "diskfs_internal.h"
#include <sys/stat.h>

#define DISKFS_COW_CHUNK (1ULL << 20)   /* device-copy chunk for privatization */

static struct diskfs_inode *
diskfs_refcount_inode(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p = request->plugin_data;

    return p->inode_stash[2];
} /* diskfs_refcount_inode */

/* ---------------------------------------------------------------- refcount inc */

static void
diskfs_refcount_inc_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    p->rc_cont(request);
} /* diskfs_refcount_inc_inserted_cb */

static void
diskfs_refcount_inc_insert(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_key           key    = diskfs_refcount_key(p->rc_devid, p->rc_devoff);
    struct diskfs_refcount_rec     rec    = { .length = p->rc_length, .refcount = p->rc_newcount };
    struct diskfs_bt_op           *op     = diskfs_bt_op_alloc(thread);

    if (diskfs_bt_insert_async(op, thread, p->txn, diskfs_refcount_inode(request),
                               &key, &rec, sizeof(rec),
                               diskfs_refcount_inc_inserted_cb, request)) {
        diskfs_refcount_inc_inserted_cb(op, op->result, request);
    }
} /* diskfs_refcount_inc_insert */

static void
diskfs_refcount_inc_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_refcount_inc_insert(request);
} /* diskfs_refcount_inc_removed_cb */

static void
diskfs_refcount_inc_lookup_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    int                            found   = (result >= 0);

    if (found) {
        struct diskfs_refcount_rec *rec = (struct diskfs_refcount_rec *) p->rec_scratch;
        p->rc_newcount = rec->refcount + 1;
    } else {
        p->rc_newcount = 2;     /* sole owner (implicit 1) + this new share */
    }
    diskfs_bt_op_free(thread, op);

    if (found) {
        struct diskfs_bt_key key = diskfs_refcount_key(p->rc_devid, p->rc_devoff);
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_bt_remove_async(op, thread, p->txn, diskfs_refcount_inode(request),
                                   &key, diskfs_refcount_inc_removed_cb, request)) {
            diskfs_refcount_inc_removed_cb(op, op->result, request);
        }
    } else {
        diskfs_refcount_inc_insert(request);
    }
} /* diskfs_refcount_inc_lookup_cb */

void
diskfs_refcount_inc(
    struct chimera_vfs_request *request,
    uint32_t                    device_id,
    uint64_t                    device_offset,
    uint64_t                    length,
    void (                     *cont )(struct chimera_vfs_request *))
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_key           key    = diskfs_refcount_key(device_id, device_offset);
    struct diskfs_bt_op           *op;

    p->rc_devid  = device_id;
    p->rc_devoff = device_offset;
    p->rc_length = length;
    p->rc_cont   = cont;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_bt_lookup_async(op, thread, diskfs_refcount_inode(request),
                               DISKFS_BT_OP_LOOKUP_EXACT, &key, &op->found_key,
                               p->rec_scratch, sizeof(struct diskfs_refcount_rec),
                               diskfs_refcount_inc_lookup_cb, request)) {
        diskfs_refcount_inc_lookup_cb(op, op->result, request);
    }
} /* diskfs_refcount_inc */

/* ---------------------------------------------------------------- refcount dec */

static void
diskfs_refcount_dec_done_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    p->rc_cont(request);
} /* diskfs_refcount_dec_done_cb */

static void
diskfs_refcount_dec_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;

    (void) result;
    diskfs_bt_op_free(thread, op);

    if (p->rc_newcount >= 2) {
        /* Still shared by others: reinsert the decremented record. */
        struct diskfs_bt_key       key = diskfs_refcount_key(p->rc_devid, p->rc_devoff);
        struct diskfs_refcount_rec rec = { .length = p->rc_length, .refcount = p->rc_newcount };
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_bt_insert_async(op, thread, p->txn, diskfs_refcount_inode(request),
                                   &key, &rec, sizeof(rec),
                                   diskfs_refcount_dec_done_cb, request)) {
            diskfs_refcount_dec_done_cb(op, op->result, request);
        }
        return;
    }

    /* Dropped to a single owner: no record (implicit 1), do not free. */
    p->rc_cont(request);
} /* diskfs_refcount_dec_removed_cb */

static void
diskfs_refcount_dec_lookup_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    int                            found   = (result >= 0);

    if (!found) {
        /* No record: this was the last (sole) owner, so the caller frees. */
        diskfs_bt_op_free(thread, op);
        p->rc_should_free = 1;
        p->rc_cont(request);
        return;
    }

    struct diskfs_refcount_rec *rec = (struct diskfs_refcount_rec *) p->rec_scratch;
    p->rc_length      = rec->length;
    p->rc_newcount    = rec->refcount - 1;
    p->rc_should_free = 0;
    diskfs_bt_op_free(thread, op);

    struct diskfs_bt_key        key = diskfs_refcount_key(p->rc_devid, p->rc_devoff);
    op = diskfs_bt_op_alloc(thread);
    if (diskfs_bt_remove_async(op, thread, p->txn, diskfs_refcount_inode(request),
                               &key, diskfs_refcount_dec_removed_cb, request)) {
        diskfs_refcount_dec_removed_cb(op, op->result, request);
    }
} /* diskfs_refcount_dec_lookup_cb */

void
diskfs_refcount_dec(
    struct chimera_vfs_request *request,
    uint32_t                    device_id,
    uint64_t                    device_offset,
    void (                     *cont )(struct chimera_vfs_request *))
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_key           key    = diskfs_refcount_key(device_id, device_offset);
    struct diskfs_bt_op           *op;

    p->rc_devid       = device_id;
    p->rc_devoff      = device_offset;
    p->rc_should_free = 0;
    p->rc_cont        = cont;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_bt_lookup_async(op, thread, diskfs_refcount_inode(request),
                               DISKFS_BT_OP_LOOKUP_EXACT, &key, &op->found_key,
                               p->rec_scratch, sizeof(struct diskfs_refcount_rec),
                               diskfs_refcount_dec_lookup_cb, request)) {
        diskfs_refcount_dec_lookup_cb(op, op->result, request);
    }
} /* diskfs_refcount_dec */

/* ----------------------------------------------- copy-on-write: privatize one */

static void
diskfs_cow_replace_done_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    p->cow_cont(request);
} /* diskfs_cow_replace_done_cb */

/* After the device copy and the refcount decrement: free the old blocks if this
 * was the last owner, then resume. */
static void
diskfs_cow_after_dec(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p = request->plugin_data;

    if (p->rc_should_free) {
        diskfs_thread_free_space(p->thread, p->txn, p->cow_ext.device_id,
                                 p->cow_ext.device_offset,
                                 SM_ALIGN_UP(p->cow_ext.length));
    }
    p->cow_cont(request);
} /* diskfs_cow_after_dec */

/* Old extent removed; insert the private replacement, then drop the refcount. */
static void
diskfs_cow_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);

    diskfs_refcount_dec(request, p->cow_ext.device_id, p->cow_ext.device_offset,
                        diskfs_cow_after_dec);
} /* diskfs_cow_inserted_cb */

static void
diskfs_cow_removed_cb(
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
                                p->cow_ext.file_offset, p->cow_ext.length,
                                p->cow_new_devid, p->cow_new_devoff,
                                p->cow_ext.flags & ~(uint32_t) DISKFS_EXT_SHARED,
                                diskfs_cow_inserted_cb, request)) {
        diskfs_cow_inserted_cb(op, op->result, request);
    }
} /* diskfs_cow_removed_cb */

/* Device copy complete: swap the shared extent record for the private copy. */
static void
diskfs_cow_replace(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_op           *op     = diskfs_bt_op_alloc(thread);

    if (diskfs_ext_remove_async(op, thread, p->txn, p->inode_stash[0],
                                p->cow_ext.file_offset, diskfs_cow_removed_cb,
                                request)) {
        diskfs_cow_removed_cb(op, op->result, request);
    }
} /* diskfs_cow_replace */

/* Chunked device-to-device copy of the shared extent into the freshly allocated
 * private blocks.  Each chunk is read then written before advancing. */
static void diskfs_cow_copy_step(
    struct chimera_vfs_request *request);

static void
diskfs_cow_write_cb(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;

    (void) evpl;
    diskfs_pending_io_add(thread, -1);
    diskfs_io_resume_waiters(thread);

    evpl_iovecs_release(thread->evpl, p->iov, p->niov);
    p->niov = 0;

    if (status) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }
    diskfs_cow_copy_step(request);
} /* diskfs_cow_write_cb */

static void
diskfs_cow_read_cb(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_shared          *shared  = thread->shared;

    (void) evpl;
    diskfs_pending_io_add(thread, -1);
    diskfs_io_resume_waiters(thread);

    if (status) {
        evpl_iovecs_release(thread->evpl, p->iov, p->niov);
        p->niov = 0;
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    /* Write the chunk just read to the new private device range. */
    uint64_t off   = p->cow_copied;
    uint64_t left  = p->cow_ext.length - off;
    uint64_t chunk = left < DISKFS_COW_CHUNK ? left : DISKFS_COW_CHUNK;

    p->cow_copied += chunk;
    diskfs_pending_io_add(thread, 1);
    evpl_block_write(thread->evpl, thread->queue[p->cow_new_devid],
                     p->iov, p->niov,
                     p->cow_new_devoff + off, !shared->unsafe_async,
                     diskfs_cow_write_cb, request);
} /* diskfs_cow_read_cb */

static void
diskfs_cow_copy_step(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_shared          *shared = thread->shared;
    struct evpl                   *evpl   = thread->evpl;
    uint64_t                       off    = p->cow_copied;
    uint64_t                       left   = p->cow_ext.length - off;
    uint64_t                       chunk;

    if (left == 0) {
        diskfs_cow_replace(request);
        return;
    }

    if (diskfs_io_gate(thread, request, diskfs_cow_copy_step)) {
        return;
    }

    chunk = left < DISKFS_COW_CHUNK ? left : DISKFS_COW_CHUNK;

    p->niov = evpl_iovec_alloc(evpl, chunk, 4096, 32, 0, p->iov);
    if (p->niov <= 0) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EIO);
        return;
    }

    diskfs_pending_io_add(thread, 1);
    evpl_block_read(evpl, thread->queue[p->cow_ext.device_id], p->iov, p->niov,
                    p->cow_ext.device_offset + off, diskfs_cow_read_cb, request);
    (void) shared;
} /* diskfs_cow_copy_step */

static void
diskfs_cow_alloc_resume(
    struct diskfs_thread *thread,
    void                 *arg);

static void
diskfs_cow_alloc(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    uint64_t                       dev_id, dev_off;
    int                            rc;

    rc = diskfs_inode_alloc_space(thread, p->txn, p->inode_stash[0],
                                  (int64_t) p->cow_ext.length, SM_RESERVATION_MIN,
                                  &dev_id, &dev_off, diskfs_cow_alloc_resume, request);
    if (rc == SM_AGAIN) {
        return;
    }
    if (rc) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOSPC);
        return;
    }

    p->cow_new_devid  = (uint32_t) dev_id;
    p->cow_new_devoff = dev_off;
    p->cow_copied     = 0;
    diskfs_cow_copy_step(request);
} /* diskfs_cow_alloc */

static void
diskfs_cow_alloc_resume(
    struct diskfs_thread *thread,
    void                 *arg)
{
    (void) thread;
    diskfs_cow_alloc((struct chimera_vfs_request *) arg);
} /* diskfs_cow_alloc_resume */

void
diskfs_cow_privatize(
    struct chimera_vfs_request *request,
    const struct diskfs_extent *ext,
    void (                     *cont )(struct chimera_vfs_request *))
{
    struct diskfs_request_private *p = request->plugin_data;

    p->cow_ext  = *ext;
    p->cow_cont = cont;
    diskfs_cow_alloc(request);
} /* diskfs_cow_privatize */

/* --------------------------------------------- ext_release (truncate/unlink/dealloc)
 *
 * Release one extent's backing space.  A non-shared extent is freed directly; a
 * shared extent decrements its refcount and frees the device range only when it
 * was the last owner.  The refcount inode must already be acquired at
 * inode_stash[2] (the caller acquires it lazily when it meets a shared extent). */

static void
diskfs_ext_release_after_dec(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p = request->plugin_data;

    if (p->rc_should_free) {
        diskfs_thread_free_space(p->thread, p->txn, p->rc_devid, p->rc_devoff,
                                 SM_ALIGN_UP(p->rc_length));
    }
    p->rc_cont(request);
} /* diskfs_ext_release_after_dec */

/* Lazily acquired the refcount inode for a free of a shared extent; retry. */
static void
diskfs_ext_release_acquired_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    p->inode_stash[2] = inode;
    diskfs_ext_release(request, &p->cow_ext, p->cow_cont);
} /* diskfs_ext_release_acquired_cb */

void
diskfs_ext_release(
    struct chimera_vfs_request *request,
    const struct diskfs_extent *ext,
    void (                     *cont )(struct chimera_vfs_request *))
{
    struct diskfs_request_private *p = request->plugin_data;

    if (ext->flags & DISKFS_EXT_SHARED) {
        if (p->inode_stash[2] == NULL) {
            /* Acquire the refcount inode (lock leaf, last), then retry. */
            p->cow_ext  = *ext;
            p->cow_cont = cont;
            diskfs_inode_acquire(p->thread, p->txn, DISKFS_REFCOUNT_INUM_BASE,
                                 DISKFS_REFCOUNT_GEN, DISKFS_INODE_LOCK_WRITE,
                                 diskfs_ext_release_acquired_cb, request);
            return;
        }
        p->rc_cont   = cont;
        p->rc_length = ext->length;
        diskfs_refcount_dec(request, ext->device_id, ext->device_offset,
                            diskfs_ext_release_after_dec);
        return;
    }

    diskfs_thread_free_space(p->thread, p->txn, ext->device_id,
                             ext->device_offset, SM_ALIGN_UP(ext->length));
    cont(request);
} /* diskfs_ext_release */

/* --------------------------------------------------------------------- CLONE */

static void diskfs_clone_walk(
    struct chimera_vfs_request *request);

static void diskfs_clone_walk_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

static void
diskfs_clone_finalize(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_inode           *dst    = p->inode_stash[0];
    uint64_t                       end    = p->clone_dst_base +
        (p->loop_pos - p->clone_src_base);
    struct timespec                now;

    if (end > dst->size) {
        dst->size       = end;
        dst->space_used = (dst->size + 4095) & ~4095;
    }

    clock_gettime(CLOCK_REALTIME, &now);
    dst->mtime_sec  = now.tv_sec;
    dst->mtime_nsec = now.tv_nsec;
    dst->ctime_sec  = now.tv_sec;
    dst->ctime_nsec = now.tv_nsec;

    diskfs_map_attrs(thread, &request->clone_range.r_post_attr, dst);
    diskfs_op_ok(request, p->txn);
} /* diskfs_clone_finalize */

static void
diskfs_clone_advance(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_op           *op     = diskfs_bt_op_alloc(thread);

    /* Step to the extent following the one just processed.  (A plain re-floor of
     * an advanced offset would return the same extent again, since floor is the
     * largest key <= offset.) */
    if (diskfs_ext_next_async(op, thread, p->inode_stash[1], p->ext_iter.file_offset,
                              p->rec_scratch, sizeof(p->rec_scratch),
                              diskfs_clone_walk_cb, request)) {
        diskfs_clone_walk_cb(op, op->result, request);
    }
} /* diskfs_clone_advance */

/* Bump the share count for the overlap, then advance. */
static void
diskfs_clone_share_refcount(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_extent          *e      = &p->ext_iter;
    uint64_t                       ostart = e->file_offset > p->clone_src_base ?
        e->file_offset : p->clone_src_base;
    uint64_t                       oend = (e->file_offset + e->length) < p->loop_pos ?
        (e->file_offset + e->length) : p->loop_pos;
    uint64_t                       odevoff = e->device_offset + (ostart - e->file_offset);

    diskfs_refcount_inc(request, e->device_id, odevoff, oend - ostart,
                        diskfs_clone_advance);
} /* diskfs_clone_share_refcount */

/* Insert the destination extent (shared) for the overlap, then bump refcount. */
static void
diskfs_clone_dst_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_clone_share_refcount(request);
} /* diskfs_clone_dst_inserted_cb */

static void
diskfs_clone_insert_dst(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_extent          *e      = &p->ext_iter;
    uint64_t                       ostart = e->file_offset > p->clone_src_base ?
        e->file_offset : p->clone_src_base;
    uint64_t                       oend = (e->file_offset + e->length) < p->loop_pos ?
        (e->file_offset + e->length) : p->loop_pos;
    uint64_t                       odevoff = e->device_offset + (ostart - e->file_offset);
    uint64_t                       dst_off = p->clone_dst_base + (ostart - p->clone_src_base);
    struct diskfs_bt_op           *op      = diskfs_bt_op_alloc(thread);

    if (diskfs_ext_insert_async(op, thread, p->txn, p->inode_stash[0],
                                dst_off, oend - ostart, e->device_id, odevoff,
                                (e->flags | DISKFS_EXT_SHARED) & ~(uint32_t) DISKFS_EXT_UNWRITTEN,
                                diskfs_clone_dst_inserted_cb, request)) {
        diskfs_clone_dst_inserted_cb(op, op->result, request);
    }
} /* diskfs_clone_insert_dst */

/* Source-side: rewrite e as [head][overlap=SHARED][tail] so the shared piece is
 * its own extent (devoff == the overlap's), then insert the destination. */
static void
diskfs_clone_src_step(
    struct chimera_vfs_request *request,
    int                         stage);

static void
diskfs_clone_src_step_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_clone_src_step(request, (int) p->op_scratch);
} /* diskfs_clone_src_step_cb */

static void
diskfs_clone_src_step(
    struct chimera_vfs_request *request,
    int                         stage)
{
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_extent          *e       = &p->ext_iter;
    uint64_t                       estart  = e->file_offset;
    uint64_t                       eend    = e->file_offset + e->length;
    uint64_t                       ostart  = estart > p->clone_src_base ? estart : p->clone_src_base;
    uint64_t                       oend    = eend < p->loop_pos ? eend : p->loop_pos;
    uint64_t                       odevoff = e->device_offset + (ostart - estart);
    struct diskfs_bt_op           *op;

    p->op_scratch = stage + 1;

    switch (stage) {
        case 0:     /* remove the original source extent */
            op = diskfs_bt_op_alloc(thread);
            if (diskfs_ext_remove_async(op, thread, p->txn, p->inode_stash[1],
                                        estart, diskfs_clone_src_step_cb, request)) {
                diskfs_clone_src_step_cb(op, op->result, request);
            }
            return;
        case 1:     /* head [estart, ostart) keeps the original (non-shared) flags */
            if (ostart > estart) {
                op = diskfs_bt_op_alloc(thread);
                if (diskfs_ext_insert_async(op, thread, p->txn, p->inode_stash[1],
                                            estart, ostart - estart, e->device_id,
                                            e->device_offset, e->flags,
                                            diskfs_clone_src_step_cb, request)) {
                    diskfs_clone_src_step_cb(op, op->result, request);
                }
                return;
            }
            diskfs_clone_src_step(request, 2);
            return;
        case 2:     /* overlap [ostart, oend) becomes SHARED */
            op = diskfs_bt_op_alloc(thread);
            if (diskfs_ext_insert_async(op, thread, p->txn, p->inode_stash[1],
                                        ostart, oend - ostart, e->device_id, odevoff,
                                        e->flags | DISKFS_EXT_SHARED,
                                        diskfs_clone_src_step_cb, request)) {
                diskfs_clone_src_step_cb(op, op->result, request);
            }
            return;
        case 3:     /* tail [oend, eend) keeps the original flags */
            if (oend < eend) {
                op = diskfs_bt_op_alloc(thread);
                if (diskfs_ext_insert_async(op, thread, p->txn, p->inode_stash[1],
                                            oend, eend - oend, e->device_id,
                                            e->device_offset + (oend - estart), e->flags,
                                            diskfs_clone_src_step_cb, request)) {
                    diskfs_clone_src_step_cb(op, op->result, request);
                }
                return;
            }
            diskfs_clone_src_step(request, 4);
            return;
        default:    /* done splitting source; record the destination extent */
            diskfs_clone_insert_dst(request);
            return;
    } /* switch */
} /* diskfs_clone_src_step */

static void
diskfs_clone_walk_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    int                            have;

    have = diskfs_ext_from_op(op, result, &p->ext_iter);
    diskfs_bt_op_free(p->thread, op);

    if (!have || p->ext_iter.file_offset >= p->loop_pos) {
        diskfs_clone_finalize(request);
        return;
    }

    uint64_t estart = p->ext_iter.file_offset;
    uint64_t eend   = estart + p->ext_iter.length;
    uint64_t ostart = estart > p->clone_src_base ? estart : p->clone_src_base;
    uint64_t oend   = eend < p->loop_pos ? eend : p->loop_pos;

    /* Skip holes and reserved-but-unwritten ranges: the destination simply has
     * no extent there and reads as zeros, matching the source. */
    if (ostart >= oend || (p->ext_iter.flags & DISKFS_EXT_UNWRITTEN)) {
        diskfs_clone_advance(request);
        return;
    }

    diskfs_clone_src_step(request, 0);
} /* diskfs_clone_walk_cb */

static void
diskfs_clone_walk(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_op           *op     = diskfs_bt_op_alloc(thread);

    /* Walk the source extents covering [loop_off, loop_pos). */
    if (diskfs_ext_floor_async(op, thread, p->inode_stash[1], p->loop_off,
                               p->rec_scratch, sizeof(p->rec_scratch),
                               diskfs_clone_walk_cb, request)) {
        diskfs_clone_walk_cb(op, op->result, request);
    }
} /* diskfs_clone_walk */

static void
diskfs_clone_rc_acquired_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *src     = (struct diskfs_inode *) request->clone_range.src_handle->vfs_private;
    struct diskfs_inode           *dst     = (struct diskfs_inode *) request->clone_range.dst_handle->vfs_private;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    p->inode_stash[0] = dst;
    p->inode_stash[1] = src;
    p->inode_stash[2] = inode;     /* refcount inode (acquired last) */

    diskfs_map_attrs(p->thread, &request->clone_range.r_pre_attr, dst);

    p->clone_src_base = request->clone_range.src_offset;
    p->clone_dst_base = request->clone_range.dst_offset;
    p->loop_off       = request->clone_range.src_offset;
    p->loop_pos       = request->clone_range.src_offset + request->clone_range.length;

    diskfs_clone_walk(request);
} /* diskfs_clone_rc_acquired_cb */

static void
diskfs_clone_second_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    /* Acquire the refcount inode last (it is a deadlock-free lock leaf: only
     * clone / CoW / shared-free touch it, and always after their file inodes). */
    diskfs_inode_acquire(p->thread, p->txn, DISKFS_REFCOUNT_INUM_BASE,
                         DISKFS_REFCOUNT_GEN, DISKFS_INODE_LOCK_WRITE,
                         diskfs_clone_rc_acquired_cb, request);
} /* diskfs_clone_second_cb */

static void
diskfs_clone_first_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *src     = (struct diskfs_inode *) request->clone_range.src_handle->vfs_private;
    struct diskfs_inode           *dst     = (struct diskfs_inode *) request->clone_range.dst_handle->vfs_private;
    struct diskfs_inode           *second;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    second = (src->inum < dst->inum) ? dst : src;
    diskfs_inode_acquire_pinned(p->thread, p->txn, second,
                                DISKFS_INODE_LOCK_WRITE, diskfs_clone_second_cb,
                                request);
} /* diskfs_clone_first_cb */

void
diskfs_clone_range(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p   = request->plugin_data;
    struct diskfs_inode           *src = (struct diskfs_inode *) request->clone_range.src_handle->vfs_private;
    struct diskfs_inode           *dst = (struct diskfs_inode *) request->clone_range.dst_handle->vfs_private;
    struct diskfs_inode           *first;

    (void) private_data;

    if (unlikely(shared->block_layout || shared->scsi_layout)) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    /* Block-aligned ranges only (reflink shares whole device blocks). */
    if ((request->clone_range.src_offset & 4095) ||
        (request->clone_range.dst_offset & 4095) ||
        (request->clone_range.length & 4095)) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    p->thread  = thread;
    p->status  = 0;
    p->pending = 0;
    p->niov    = 0;
    p->txn     = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);

    if (request->clone_range.length == 0) {
        diskfs_map_attrs(thread, &request->clone_range.r_pre_attr, dst);
        diskfs_map_attrs(thread, &request->clone_range.r_post_attr, dst);
        diskfs_op_ok(request, p->txn);
        return;
    }

    /* Acquire the two file inodes in ascending-inum order (deadlock-free). */
    first = (src->inum < dst->inum) ? src : dst;
    diskfs_inode_acquire_pinned(thread, p->txn, first, DISKFS_INODE_LOCK_WRITE,
                                diskfs_clone_first_cb, request);
} /* diskfs_clone_range */
