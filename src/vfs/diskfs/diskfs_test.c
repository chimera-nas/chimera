// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * White-box test SDK for diskfs -- see diskfs_test.h.  Compiled into the diskfs
 * module library so it can reach the private on-disk-allocator and b+tree
 * structures; exported (SYMBOL_EXPORT) for the model-based-test harness to link.
 */

#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <time.h>

#include "diskfs_internal.h"
#include "diskfs_test.h"
#include "space_map.h"
#include "common/macros.h"
#include "common/rbtree.h"
#include "vfs/vfs.h"
#include "vfs/sdk/vfs_fh_magic.h"

static struct diskfs_shared *
diskfs_test_shared(struct chimera_vfs *vfs)
{
    if (!vfs) {
        return NULL;
    }
    return vfs->module_private[CHIMERA_VFS_FH_MAGIC_DISKFS];
} /* diskfs_test_shared */

/* Lock (or unlock) every AG on every device, in a fixed (device, ag) order, so
 * a snapshot is globally consistent against the background intent-log and
 * reclaim threads (which only ever hold one AG lock at a time). */
static void
diskfs_test_lock_all_ags(
    struct space_map *sm,
    int               lock)
{
    uint32_t d, a;

    for (d = 0; d < sm->num_devices; d++) {
        struct sm_device *dev = &sm->devices[d];

        for (a = 0; a < dev->num_ags; a++) {
            if (lock) {
                pthread_mutex_lock(&dev->ags[a].lock);
            } else {
                pthread_mutex_unlock(&dev->ags[a].lock);
            }
        }
    }
} /* diskfs_test_lock_all_ags */

/* Accumulate one AG's free-extent tree into *out and, when err is non-NULL,
 * validate the tree's structural invariants.  The AG lock must be held.
 * Returns 0, or -1 with a message in err/errlen on a violation. */
static int
diskfs_test_scan_ag(
    struct sm_ag             *ag,
    struct diskfs_test_space *out,
    char                     *err,
    int                       errlen)
{
    struct sm_extent *ext;
    struct sm_claim  *claim;
    uint64_t          tree_free = 0;
    uint64_t          prev_end  = 0;
    int               have_prev = 0;

    rb_tree_first(&ag->free_by_offset, ext);
    while (ext) {
        if (err) {
            if (ext->offset < ag->base_offset ||
                ext->offset + ext->length > ag->base_offset + ag->size) {
                snprintf(err, errlen,
                         "AG %u:%u free extent [%lu,+%lu) out of AG bounds [%lu,+%lu)",
                         ag->device_id, ag->ag_index,
                         (unsigned long) ext->offset, (unsigned long) ext->length,
                         (unsigned long) ag->base_offset, (unsigned long) ag->size);
                return -1;
            }
            if (have_prev && ext->offset < prev_end) {
                snprintf(err, errlen,
                         "AG %u:%u free extents overlap: prev end %lu > next start %lu",
                         ag->device_id, ag->ag_index,
                         (unsigned long) prev_end, (unsigned long) ext->offset);
                return -1;
            }
            if (have_prev && ext->offset == prev_end) {
                snprintf(err, errlen,
                         "AG %u:%u adjacent free extents not coalesced at offset %lu",
                         ag->device_id, ag->ag_index, (unsigned long) prev_end);
                return -1;
            }
        }

        tree_free += ext->length;
        if (ext->length > out->largest_free_extent) {
            out->largest_free_extent = ext->length;
        }
        out->total_free_extents++;

        prev_end  = ext->offset + ext->length;
        have_prev = 1;
        ext       = rb_tree_next(&ag->free_by_offset, ext);
    }

    if (err && tree_free != ag->free_bytes) {
        snprintf(err, errlen,
                 "AG %u:%u free-tree total %lu != ag->free_bytes %lu",
                 ag->device_id, ag->ag_index,
                 (unsigned long) tree_free, (unsigned long) ag->free_bytes);
        return -1;
    }

    for (claim = ag->claims; claim; claim = claim->next) {
        if (err &&
            (claim->base < ag->base_offset ||
             claim->base + claim->len > ag->base_offset + ag->size)) {
            snprintf(err, errlen,
                     "AG %u:%u claim [%lu,+%lu) out of AG bounds",
                     ag->device_id, ag->ag_index,
                     (unsigned long) claim->base, (unsigned long) claim->len);
            return -1;
        }
        out->claim_bytes += claim->len;
    }

    out->tree_free_sum += tree_free;
    out->ag_free_sum   += ag->free_bytes;
    return 0;
} /* diskfs_test_scan_ag */

/* Core: snapshot every AG under the global lock, optionally validating.  err
 * NULL => snapshot only.  Returns 0, or -1 on a validation failure. */
static int
diskfs_test_snapshot(
    struct chimera_vfs       *vfs,
    struct diskfs_test_space *out,
    char                     *err,
    int                       errlen)
{
    struct diskfs_shared *shared = diskfs_test_shared(vfs);
    struct space_map     *sm;
    uint32_t              d, a;
    int                   rc = 0;

    memset(out, 0, sizeof(*out));
    if (!shared || !shared->space_map) {
        if (err) {
            snprintf(err, errlen, "diskfs is not the mounted module");
        }
        return -1;
    }
    sm = shared->space_map;

    out->total_capacity  = sm->total_capacity;
    out->usable_capacity = sm->usable_capacity;
    out->num_devices     = sm->num_devices;

    diskfs_test_lock_all_ags(sm, 1);

    for (d = 0; d < sm->num_devices && rc == 0; d++) {
        struct sm_device *dev = &sm->devices[d];

        out->dev_free_sum += dev->free_bytes;
        out->num_ags      += dev->num_ags;

        for (a = 0; a < dev->num_ags; a++) {
            if (diskfs_test_scan_ag(&dev->ags[a], out, err, errlen) != 0) {
                rc = -1;
                break;
            }
        }
    }

    diskfs_test_lock_all_ags(sm, 0);

    if (rc != 0) {
        return -1;
    }

    if (err) {
        if (out->ag_free_sum != out->dev_free_sum) {
            snprintf(err, errlen,
                     "per-AG free total %lu != per-device counter total %lu "
                     "(reclaim may not have quiesced)",
                     (unsigned long) out->ag_free_sum,
                     (unsigned long) out->dev_free_sum);
            return -1;
        }
        if (out->ag_free_sum > out->usable_capacity) {
            snprintf(err, errlen,
                     "free %lu exceeds usable capacity %lu",
                     (unsigned long) out->ag_free_sum,
                     (unsigned long) out->usable_capacity);
            return -1;
        }
    }

    return 0;
} /* diskfs_test_snapshot */

SYMBOL_EXPORT int
diskfs_test_space(
    struct chimera_vfs       *vfs,
    struct diskfs_test_space *out)
{
    return diskfs_test_snapshot(vfs, out, NULL, 0);
} /* diskfs_test_space */

SYMBOL_EXPORT int
diskfs_test_check(
    struct chimera_vfs *vfs,
    char               *err,
    int                 errlen)
{
    struct diskfs_test_space snap;
    char                     local[256];

    if (!err) {
        err    = local;
        errlen = sizeof(local);
    }
    err[0] = '\0';
    return diskfs_test_snapshot(vfs, &snap, err, errlen);
} /* diskfs_test_check */

/* Fill *out from a raw 4 KiB home-block image (dinode scalars at offset 0, the
 * b+tree root at DISKFS_BT_ROOT_BASE). */
static void
diskfs_test_inode_from_block(
    void                     *buf,
    uint64_t                  inum,
    struct diskfs_test_inode *out)
{
    struct diskfs_dinode      *di = (struct diskfs_dinode *) buf;
    struct diskfs_bt_node_hdr *h  = diskfs_bt_hdr(buf, DISKFS_BT_ROOT_BASE);

    out->inum        = inum;
    out->size        = di->size;
    out->nlink       = di->nlink;
    out->tree_height = (uint16_t) (h->level + 1);
    out->root_nitems = h->nitems;
} /* diskfs_test_inode_from_block */

SYMBOL_EXPORT int
diskfs_test_inode(
    struct chimera_vfs       *vfs,
    uint64_t                  inum,
    struct diskfs_test_inode *out)
{
    struct diskfs_shared      *shared = diskfs_test_shared(vfs);
    struct diskfs_inode_shard *shard;
    struct diskfs_inode       *inode;
    uint32_t                   disk;
    uint64_t                   off;
    uint8_t                    blk[DISKFS_BLOCK_SIZE];
    int                        fd;
    ssize_t                    rc;

    if (!shared || inum == 0) {
        return -1;
    }

    /* Freshest source: a resident inode whose home block is still pinned in the
     * cache (dirty / mid-transaction). */
    shard = diskfs_inode_shard(shared, inum);
    pthread_mutex_lock(&shard->lock);
    rb_tree_query_exact(&shard->inodes, inum, inum, inode);
    if (inode && inode->block) {
        struct diskfs_bt_node_hdr *h =
            diskfs_bt_hdr(inode->block->iov.data, DISKFS_BT_ROOT_BASE);

        out->inum        = inum;
        out->size        = inode->size;
        out->nlink       = inode->nlink;
        out->tree_height = (uint16_t) (h->level + 1);
        out->root_nitems = h->nitems;
        pthread_mutex_unlock(&shard->lock);
        return 0;
    }
    pthread_mutex_unlock(&shard->lock);

    /* Idle inode: its home block has been returned to the cache, so read the
     * on-disk image directly from the backing device file.  This can lag the
     * newest logged image by the tail-push delay, so it is exact only once the
     * push frontier has caught up (call diskfs_test_await_reclaim first for a
     * point-in-time-stable read). */
    if (!shared->space_map || !sm_inum_valid(shared->space_map, inum)) {
        return -1;
    }
    off = sm_inum_to_device_offset(shared->space_map, inum, &disk);
    if ((int) disk >= shared->num_devices || !shared->device_paths ||
        !shared->device_paths[disk]) {
        return -1;
    }
    fd = open(shared->device_paths[disk], O_RDONLY);
    if (fd < 0) {
        return -1;
    }
    rc = pread(fd, blk, sizeof(blk), (off_t) off);
    close(fd);
    if (rc != (ssize_t) sizeof(blk)) {
        return -1;
    }
    diskfs_test_inode_from_block(blk, inum, out);
    return 0;
} /* diskfs_test_inode */

/* Are all reclaim workers idle (no queued jobs, no condense in flight)? */
static int
diskfs_test_reclaim_idle(struct diskfs_shared *shared)
{
    struct diskfs_reclaim *r = shared->reclaim;
    uint32_t               i;
    int                    idle = 1;

    if (!r) {
        return 1;
    }
    for (i = 0; i < r->nworkers; i++) {
        struct diskfs_reclaim_worker *w = &r->workers[i];

        pthread_mutex_lock(&w->lock);
        if (w->head != NULL || w->condenses != 0) {
            idle = 0;
        }
        pthread_mutex_unlock(&w->lock);
        if (!idle) {
            break;
        }
    }
    return idle;
} /* diskfs_test_reclaim_idle */

static uint64_t
diskfs_test_now_ms(void)
{
    struct timespec ts;

    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (uint64_t) ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
} /* diskfs_test_now_ms */

SYMBOL_EXPORT int
diskfs_test_await_reclaim(
    struct chimera_vfs *vfs,
    struct evpl        *evpl,
    int                 max_ms)
{
    struct diskfs_shared *shared = diskfs_test_shared(vfs);
    uint64_t              deadline;
    int                   stable = 0;

    if (!shared) {
        return 0;
    }
    deadline = diskfs_test_now_ms() + (uint64_t) max_ms;

    /* Quiesced == workers idle AND the apply frontier has caught the durable
     * frontier (every freed range returned to the map), observed stable across
     * a few consecutive polls so a momentary lull between two reclaim batches is
     * not mistaken for completion. */
    while (diskfs_test_now_ms() < deadline) {
        uint64_t applied = __atomic_load_n(&shared->intent_log.applied_seq,
                                           __ATOMIC_ACQUIRE);
        uint64_t durable = __atomic_load_n(&shared->intent_log.durable_seq,
                                           __ATOMIC_ACQUIRE);

        if (diskfs_test_reclaim_idle(shared) && applied == durable) {
            if (++stable >= 3) {
                return 1;
            }
        } else {
            stable = 0;
        }

        if (evpl) {
            evpl_continue(evpl);
        }
        usleep(200);
    }
    return 0;
} /* diskfs_test_await_reclaim */

SYMBOL_EXPORT void
diskfs_test_crash(struct chimera_vfs *vfs)
{
    struct diskfs_shared *shared = diskfs_test_shared(vfs);

    if (!shared) {
        return;
    }
    /* Mark the module so the NORMAL chimera_vfs_destroy -> diskfs_destroy tears
     * it down as a crash (skipping the free-map persist + CLEAN stamp).  Doing
     * it there rather than freeing shared here is what keeps the VFS's own
     * internal threads (RCU / close-thread), whose diskfs_thread_destroy still
     * dereferences shared, from touching freed memory. */
    __atomic_store_n(&shared->test_crash, 1, __ATOMIC_RELEASE);
} /* diskfs_test_crash */
