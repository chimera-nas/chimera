// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#define _GNU_SOURCE
#include <stdint.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <assert.h>
#include <fcntl.h>
#include <unistd.h>

#include "common/rbtree.h"
#include "common/logging.h"

#include "space_map.h"

#ifndef container_of
#define container_of(ptr, type, member) \
        ((type *) ((char *) (ptr) - offsetof(type, member)))
#endif /* ifndef container_of */

#define sm_abort_if(cond, ...) \
        chimera_abort_if(cond, "space_map", __FILE__, __LINE__, __VA_ARGS__)

#define sm_info(...) \
        chimera_info("space_map", __FILE__, __LINE__, __VA_ARGS__)

/*
 * Journal stubs.  These are the future intent-log hook points; calls land
 * here AFTER the in-memory state has been updated.  For now the records go
 * nowhere; replacing the body with a journal append is the next step.
 */
static inline void
sm_journal_record_alloc(
    uint32_t device_id,
    uint64_t offset,
    uint64_t length)
{
    (void) device_id;
    (void) offset;
    (void) length;
} /* sm_journal_record_alloc */

static inline void
sm_journal_record_free(
    uint32_t device_id,
    uint64_t offset,
    uint64_t length)
{
    (void) device_id;
    (void) offset;
    (void) length;
} /* sm_journal_record_free */

static struct sm_extent *
sm_extent_new(
    uint64_t offset,
    uint64_t length)
{
    struct sm_extent *ext = calloc(1, sizeof(*ext));

    ext->offset = offset;
    ext->length = length;
    return ext;
} /* sm_extent_new */

static void
sm_extent_release(
    struct rb_node *node,
    void           *private_data)
{
    struct sm_extent *ext = container_of(node, struct sm_extent, node);

    (void) private_data;
    free(ext);
} /* sm_extent_release */

/*
 * Initialize an AG.
 *
 * Layout within the AG (offsets are absolute device offsets):
 *
 *   [base_offset, base_offset + pre_log_reserved)    -- global reservations
 *                                                       (e.g. superblock,
 *                                                       intent log on AG 0
 *                                                       of device 0)
 *   [log_offset,  log_offset + SM_AG_LOG_SIZE)       -- this AG's space-map log
 *   [data_base,   data_base + post_log_reserved)     -- format-time reservations
 *                                                       at the start of the
 *                                                       data region (e.g. the
 *                                                       block_idx==1 inode
 *                                                       slot on AG 0 of disk 0
 *                                                       so root lands at
 *                                                       inum 2)
 *   [free_start,  base_offset + size)                -- allocatable data range
 *
 * `data_base` == `log_offset + SM_AG_LOG_SIZE`.
 */
static void
sm_ag_init(
    struct sm_ag *ag,
    uint32_t      device_id,
    uint32_t      ag_index,
    uint64_t      base_offset,
    uint64_t      size,
    uint64_t      pre_log_reserved,
    uint64_t      post_log_reserved)
{
    struct sm_extent *initial;
    uint64_t          total_reserved;

    ag->device_id       = device_id;
    ag->ag_index        = ag_index;
    ag->base_offset     = base_offset;
    ag->size            = size;
    ag->log_offset      = base_offset + pre_log_reserved;
    ag->log_size        = SM_AG_LOG_SIZE;
    ag->log_slot        = 0;
    ag->log_generation  = 0;
    ag->log_base_count  = 0;
    ag->log_delta_count = 0;

    pthread_mutex_init(&ag->lock, NULL);
    rb_tree_init(&ag->free_by_offset);

    total_reserved = pre_log_reserved + SM_AG_LOG_SIZE + post_log_reserved;

    if (total_reserved >= size) {
        /* The AG is too small to hold even its own log.  This shouldn't
         * happen for any reasonable AG size; if it does, the AG has zero
         * usable data range. */
        ag->free_bytes = 0;
        return;
    }

    initial = sm_extent_new(base_offset + total_reserved,
                            size - total_reserved);

    rb_tree_insert(&ag->free_by_offset, offset, initial);
    ag->free_bytes = size - total_reserved;
} /* sm_ag_init */

static void
sm_ag_destroy(struct sm_ag *ag)
{
    rb_tree_destroy(&ag->free_by_offset, sm_extent_release, NULL);
    pthread_mutex_destroy(&ag->lock);
} /* sm_ag_destroy */

/*
 * Attempt to allocate `size` bytes from `ag`.  Returns 0 on success and
 * writes the device offset (absolute) into `*r_offset`.  Returns -1 if the
 * AG cannot satisfy the request.  Caller must hold ag->lock.
 *
 * First-fit walk of the by-offset tree.  An optional best-fit secondary
 * index can be added later; the current shape supports it without changing
 * the public API.
 */
static int
sm_ag_alloc_locked(
    struct sm_ag *ag,
    uint64_t      size,
    uint64_t     *r_offset)
{
    struct sm_extent *ext;

    if (ag->free_bytes < size) {
        return -1;
    }

    rb_tree_first(&ag->free_by_offset, ext);

    while (ext) {
        if (ext->length >= size) {
            *r_offset = ext->offset;

            if (ext->length == size) {
                rb_tree_remove(&ag->free_by_offset, &ext->node);
                free(ext);
            } else {
                /* Shrink: offset is the rb-tree key, so remove + reinsert. */
                rb_tree_remove(&ag->free_by_offset, &ext->node);
                ext->offset += size;
                ext->length -= size;
                rb_tree_insert(&ag->free_by_offset, offset, ext);
            }

            ag->free_bytes -= size;
            return 0;
        }

        ext = rb_tree_next(&ag->free_by_offset, ext);
    }

    return -1;
} /* sm_ag_alloc_locked */

/*
 * Return [offset, offset+length) to `ag`'s free tree, coalescing with any
 * adjacent free neighbours.  Caller must hold ag->lock.
 */
static void
sm_ag_free_locked(
    struct sm_ag *ag,
    uint64_t      offset,
    uint64_t      length)
{
    /* Variable names avoid clashing with the rb_tree_query_floor/ceil
     * macros, which declare locals named `floor`/`ceil`. */
    struct sm_extent *prev_ext         = NULL;
    struct sm_extent *next_ext         = NULL;
    int               merged_with_prev = 0;

    sm_abort_if(length == 0, "zero-length free");
    sm_abort_if(offset < ag->base_offset ||
                offset + length > ag->base_offset + ag->size,
                "free out of AG bounds (offset=%lu length=%lu ag=%lu+%lu)",
                offset, length, ag->base_offset, ag->size);

    rb_tree_query_floor(&ag->free_by_offset, offset, offset, prev_ext);
    rb_tree_query_ceil(&ag->free_by_offset, offset + length, offset, next_ext);

    sm_abort_if(prev_ext && prev_ext->offset + prev_ext->length > offset,
                "double-free or overlap at offset=%lu (prev=%lu+%lu)",
                offset, prev_ext->offset, prev_ext->length);
    sm_abort_if(next_ext && next_ext->offset < offset + length,
                "double-free or overlap at offset=%lu (next=%lu+%lu)",
                offset, next_ext->offset, next_ext->length);

    if (prev_ext && prev_ext->offset + prev_ext->length == offset) {
        prev_ext->length += length;
        merged_with_prev  = 1;
    }

    if (merged_with_prev) {
        if (next_ext && next_ext->offset == prev_ext->offset + prev_ext->length) {
            prev_ext->length += next_ext->length;
            rb_tree_remove(&ag->free_by_offset, &next_ext->node);
            free(next_ext);
        }
    } else if (next_ext && next_ext->offset == offset + length) {
        rb_tree_remove(&ag->free_by_offset, &next_ext->node);
        next_ext->offset  = offset;
        next_ext->length += length;
        rb_tree_insert(&ag->free_by_offset, offset, next_ext);
    } else {
        struct sm_extent *fresh = sm_extent_new(offset, length);
        rb_tree_insert(&ag->free_by_offset, offset, fresh);
    }

    ag->free_bytes += length;
} /* sm_ag_free_locked */

struct space_map *
space_map_create(
    const uint64_t *device_sizes,
    uint32_t        num_devices)
{
    struct space_map *sm;
    struct sm_device *dev;
    struct sm_ag     *ag;
    uint32_t          d, a;

    sm_abort_if(num_devices == 0, "space_map_create with zero devices");

    sm              = calloc(1, sizeof(*sm));
    sm->num_devices = num_devices;
    sm->devices     = calloc(num_devices, sizeof(*sm->devices));
    pthread_mutex_init(&sm->lock, NULL);

    for (d = 0; d < num_devices; d++) {
        dev            = &sm->devices[d];
        dev->device_id = d;
        dev->size      = device_sizes[d];

        dev->num_ags  = (dev->size + SM_AG_SIZE - 1) >> SM_AG_SIZE_LOG2;
        dev->ag_rotor = 0;

        sm_abort_if(dev->num_ags == 0, "device %u has zero AGs (size=%lu)",
                    d, dev->size);

        dev->ags = calloc(dev->num_ags, sizeof(*dev->ags));

        sm->total_capacity += dev->size;

        for (a = 0; a < dev->num_ags; a++) {
            uint64_t base              = (uint64_t) a << SM_AG_SIZE_LOG2;
            uint64_t span              = SM_AG_SIZE;
            uint64_t pre_log_reserved  = 0;
            uint64_t post_log_reserved = 0;

            if (base + span > dev->size) {
                span = dev->size - base;
            }

            if (d == SM_INTENT_LOG_DEVICE && a == 0) {
                /* AG 0 of device 0 also hosts the superblock and the
                 * statically-reserved intent log.  Both sit *before* the
                 * AG's own space-map log.  Account for those bytes plus
                 * the AG log itself so they never get handed out. */
                pre_log_reserved = SM_SUPERBLOCK_SIZE + SM_INTENT_LOG_SIZE;
                sm->used_bytes  += pre_log_reserved;

                /* Reserve block_idx == 1 of AG 0 of disk 0 so the very
                 * first inode allocation lands at block_idx == 2 (root
                 * inum = 2).  block_idx == 1 is held for future bootstrap
                 * use (e.g. an AG header). */
                post_log_reserved = SM_BLOCK_SIZE;
                sm->used_bytes   += post_log_reserved;

                sm_abort_if(pre_log_reserved + SM_AG_LOG_SIZE +
                            post_log_reserved > span,
                            "device 0 AG 0 too small (%lu) to hold "
                            "superblock+intent_log+ag_log+bootstrap (%lu)",
                            span,
                            pre_log_reserved + SM_AG_LOG_SIZE +
                            post_log_reserved);
            }

            sm_abort_if(SM_AG_LOG_SIZE >= span,
                        "AG size %lu too small to hold per-AG log %lu",
                        span, SM_AG_LOG_SIZE);

            sm->used_bytes += SM_AG_LOG_SIZE;

            ag = &dev->ags[a];
            sm_ag_init(ag, d, a, base, span,
                       pre_log_reserved, post_log_reserved);
        }
    }

    sm_info("Reserved intent log: device %u offset %lu size %lu",
            (unsigned) SM_INTENT_LOG_DEVICE,
            (uint64_t) SM_INTENT_LOG_OFFSET,
            (uint64_t) SM_INTENT_LOG_SIZE);
    sm_info("Per-AG log size: %lu (%u slots x %lu)",
            (uint64_t) SM_AG_LOG_SIZE,
            (unsigned) SM_AG_LOG_SLOT_COUNT,
            (uint64_t) SM_AG_LOG_SLOT_SIZE);

    return sm;
} /* space_map_create */

void
space_map_destroy(struct space_map *sm)
{
    struct sm_device *dev;
    uint32_t          d, a;

    for (d = 0; d < sm->num_devices; d++) {
        dev = &sm->devices[d];
        for (a = 0; a < dev->num_ags; a++) {
            sm_ag_destroy(&dev->ags[a]);
        }
        free(dev->ags);
    }

    pthread_mutex_destroy(&sm->lock);
    free(sm->devices);
    free(sm);
} /* space_map_destroy */

/*
 * Attempt to reserve `want` bytes by allocating from some AG.  Walks every
 * AG of every device starting from the rotor's current position; returns 0
 * on success.  May return less than `want` only via the failure path; this
 * function never returns a partial reservation.
 */
static int
sm_pick_and_alloc(
    struct space_map *sm,
    uint64_t          want,
    uint32_t         *r_device_id,
    uint64_t         *r_device_offset)
{
    uint32_t start_dev, dev_id;
    uint32_t d, a;

    pthread_mutex_lock(&sm->lock);
    start_dev = sm->device_rotor;
    sm->device_rotor++;
    if (sm->device_rotor >= sm->num_devices) {
        sm->device_rotor = 0;
    }
    pthread_mutex_unlock(&sm->lock);

    for (d = 0; d < sm->num_devices; d++) {
        dev_id = (start_dev + d) % sm->num_devices;
        struct sm_device *dev = &sm->devices[dev_id];
        uint32_t          start_ag;

        pthread_mutex_lock(&sm->lock);
        start_ag = dev->ag_rotor;
        dev->ag_rotor++;
        if (dev->ag_rotor >= dev->num_ags) {
            dev->ag_rotor = 0;
        }
        pthread_mutex_unlock(&sm->lock);

        for (a = 0; a < dev->num_ags; a++) {
            uint32_t      ag_idx = (start_ag + a) % dev->num_ags;
            struct sm_ag *ag     = &dev->ags[ag_idx];
            uint64_t      offset;
            int           rc;

            if (ag->free_bytes < want) {
                continue;
            }

            pthread_mutex_lock(&ag->lock);
            rc = sm_ag_alloc_locked(ag, want, &offset);
            pthread_mutex_unlock(&ag->lock);

            if (rc == 0) {
                *r_device_id     = dev_id;
                *r_device_offset = offset;
                __atomic_fetch_add(&sm->used_bytes, want, __ATOMIC_RELAXED);
                sm_journal_record_alloc(dev_id, offset, want);
                return 0;
            }
        }
    }

    return -1;
} /* sm_pick_and_alloc */

int
space_map_alloc(
    struct space_map       *sm,
    struct sm_thread_cache *cache,
    uint64_t                size,
    uint32_t               *r_device_id,
    uint64_t               *r_device_offset)
{
    uint64_t need = SM_ALIGN_UP(size);
    uint64_t want;
    int      rc;

    sm_abort_if(need == 0, "alloc of zero bytes");

    if (cache->valid && cache->length >= need) {
        *r_device_id     = cache->device_id;
        *r_device_offset = cache->offset;
        cache->offset   += need;
        cache->length   -= need;
        if (cache->length == 0) {
            cache->valid = 0;
        }
        return 0;
    }

    /* Cache cannot satisfy this request.  Return the unused remainder and
     * grab a fresh reservation. */
    if (cache->valid) {
        space_map_thread_cache_return(sm, cache);
    }

    want = need > SM_RESERVATION_MIN ? need : SM_RESERVATION_MIN;

    /* If a reservation crosses an AG boundary it can't be contiguous, so cap
     * the request to the AG size.  Callers requesting more than SM_AG_SIZE
     * cannot be satisfied. */
    if (want > SM_AG_SIZE) {
        return -1;
    }

    rc = sm_pick_and_alloc(sm, want, &cache->device_id, &cache->offset);
    if (rc != 0) {
        /* Try the smaller exact size before giving up, in case fragmentation
         * blocks the reservation but the caller's actual ask is small. */
        if (want != need) {
            rc = sm_pick_and_alloc(sm, need, &cache->device_id, &cache->offset);
            if (rc == 0) {
                cache->length = need;
                cache->valid  = 1;
            }
        }
        if (rc != 0) {
            return rc;
        }
    } else {
        cache->length = want;
        cache->valid  = 1;
    }

    *r_device_id     = cache->device_id;
    *r_device_offset = cache->offset;
    cache->offset   += need;
    cache->length   -= need;
    if (cache->length == 0) {
        cache->valid = 0;
    }
    return 0;
} /* space_map_alloc */

void
space_map_free(
    struct space_map *sm,
    uint32_t          device_id,
    uint64_t          device_offset,
    uint64_t          length)
{
    struct sm_device *dev;
    struct sm_ag     *ag;
    uint32_t          ag_idx;
    uint64_t          aligned = SM_ALIGN_UP(length);

    if (aligned == 0) {
        return;
    }

    sm_abort_if(device_id >= sm->num_devices,
                "space_map_free: bad device_id %u", device_id);

    dev = &sm->devices[device_id];

    ag_idx = device_offset >> SM_AG_SIZE_LOG2;
    sm_abort_if(ag_idx >= dev->num_ags,
                "space_map_free: offset %lu past device %u",
                device_offset, device_id);

    ag = &dev->ags[ag_idx];

    /* For now we forbid frees that straddle AG boundaries.  Callers that
     * allocated through this allocator never see straddling because
     * reservations come from a single AG.  Defensive abort if it ever
     * happens. */
    sm_abort_if(device_offset + aligned > ag->base_offset + ag->size,
                "space_map_free: range crosses AG boundary "
                "(offset=%lu length=%lu ag_base=%lu ag_size=%lu)",
                device_offset, aligned, ag->base_offset, ag->size);

    pthread_mutex_lock(&ag->lock);
    sm_ag_free_locked(ag, device_offset, aligned);
    pthread_mutex_unlock(&ag->lock);

    __atomic_fetch_sub(&sm->used_bytes, aligned, __ATOMIC_RELAXED);
    sm_journal_record_free(device_id, device_offset, aligned);
} /* space_map_free */

void
space_map_thread_cache_return(
    struct space_map       *sm,
    struct sm_thread_cache *cache)
{
    if (!cache->valid || cache->length == 0) {
        cache->valid = 0;
        return;
    }

    space_map_free(sm, cache->device_id, cache->offset, cache->length);
    cache->valid  = 0;
    cache->length = 0;
} /* space_map_thread_cache_return */

int
space_map_write_superblock_path(
    struct space_map *sm,
    const char       *device_path,
    uint64_t          fsid,
    uint64_t          flags,
    uint64_t          root_inum,
    uint32_t          root_gen,
    uint64_t          log_seq)
{
    int                   fd;
    ssize_t               n;
    uint8_t               buf[SM_SUPERBLOCK_SIZE];
    struct sm_superblock *sb = (struct sm_superblock *) buf;

    memset(buf, 0, sizeof(buf));

    sb->magic             = SM_SUPERBLOCK_MAGIC;
    sb->version           = SM_FORMAT_VERSION;
    sb->block_size        = SM_BLOCK_SIZE;
    sb->ag_size           = SM_AG_SIZE;
    sb->ag_log_size       = SM_AG_LOG_SIZE;
    sb->fsid              = fsid;
    sb->num_devices       = sm->num_devices;
    sb->intent_log_device = SM_INTENT_LOG_DEVICE;
    sb->intent_log_offset = SM_INTENT_LOG_OFFSET;
    sb->intent_log_size   = SM_INTENT_LOG_SIZE;
    sb->flags             = flags;
    sb->root_inum         = root_inum;
    sb->root_gen          = root_gen;
    sb->log_seq           = log_seq;
    sb->crc32             = 0;
    sb->crc32             = sm_crc32(buf, sizeof(buf));

    fd = open(device_path, O_WRONLY);
    if (fd < 0) {
        return -1;
    }

    n = pwrite(fd, buf, sizeof(buf), SM_SUPERBLOCK_OFFSET);
    if (n != (ssize_t) sizeof(buf)) {
        close(fd);
        return -1;
    }

    if (fsync(fd) != 0) {
        close(fd);
        return -1;
    }

    close(fd);
    return 0;
} /* space_map_write_superblock_path */

/*
 * Read and validate the superblock from device 0.  Returns 0 and fills *out
 * if a well-formed, current-version superblock with a matching CRC is present;
 * returns -1 (no/!valid superblock -> caller should mkfs) on any mismatch.
 */
int
space_map_read_superblock_path(
    const char           *device_path,
    struct sm_superblock *out)
{
    int                   fd;
    ssize_t               n;
    uint8_t               buf[SM_SUPERBLOCK_SIZE];
    struct sm_superblock *sb = (struct sm_superblock *) buf;
    uint32_t              stored, computed;

    fd = open(device_path, O_RDONLY);
    if (fd < 0) {
        return -1;
    }
    n = pread(fd, buf, sizeof(buf), SM_SUPERBLOCK_OFFSET);
    close(fd);
    if (n != (ssize_t) sizeof(buf)) {
        return -1;
    }

    if (sb->magic != SM_SUPERBLOCK_MAGIC || sb->version != SM_FORMAT_VERSION) {
        return -1;
    }

    stored    = sb->crc32;
    sb->crc32 = 0;
    computed  = sm_crc32(buf, sizeof(buf));
    sb->crc32 = stored;
    if (stored != computed) {
        return -1;
    }

    *out = *sb;
    return 0;
} /* space_map_read_superblock_path */

/*
 * Remove the specific range [offset, offset+length) from the AG's free tree
 * (i.e. mark it allocated), splitting the containing free extent.  Used to
 * replay alloc deltas during reconstruction.  Caller holds ag->lock.
 */
static void
sm_ag_mark_used_locked(
    struct sm_ag *ag,
    uint64_t      offset,
    uint64_t      length)
{
    struct sm_extent *e = NULL;
    uint64_t          e_off, e_len;

    rb_tree_query_floor(&ag->free_by_offset, offset, offset, e);
    sm_abort_if(!e || e->offset > offset ||
                e->offset + e->length < offset + length,
                "mark_used [%lu,%lu) not within a free extent",
                offset, offset + length);

    e_off = e->offset;
    e_len = e->length;
    rb_tree_remove(&ag->free_by_offset, &e->node);
    free(e);

    if (offset > e_off) {
        struct sm_extent *l = sm_extent_new(e_off, offset - e_off);
        rb_tree_insert(&ag->free_by_offset, offset, l);
    }
    if (offset + length < e_off + e_len) {
        struct sm_extent *r = sm_extent_new(offset + length,
                                            (e_off + e_len) - (offset + length));
        rb_tree_insert(&ag->free_by_offset, offset, r);
    }
    ag->free_bytes -= length;
} /* sm_ag_mark_used_locked */

/* Serialize the AG's current free set into `slot` as a condensed base (no
 * deltas) at `generation`.  Caller holds ag->lock.  Returns bytes written. */
static uint64_t
sm_ag_condense_into(
    struct sm_ag *ag,
    uint8_t      *slot,
    uint64_t      generation)
{
    struct sm_ag_log_header *h    = (struct sm_ag_log_header *) slot;
    struct sm_ag_log_ext    *base = (struct sm_ag_log_ext *) (slot + sizeof(*h));
    struct sm_extent        *e;
    uint32_t                 n       = 0;
    uint64_t                 maxbase = (SM_AG_LOG_SLOT_SIZE - sizeof(*h)) /
        sizeof(struct sm_ag_log_ext);

    rb_tree_first(&ag->free_by_offset, e);
    while (e) {
        sm_abort_if(n >= maxbase, "AG condense overflow (%u extents)", n);
        base[n].offset = e->offset;
        base[n].length = e->length;
        n++;
        e = rb_tree_next(&ag->free_by_offset, e);
    }

    h->magic       = SM_AG_LOG_MAGIC;
    h->generation  = generation;
    h->base_count  = n;
    h->delta_count = 0;
    h->reserved    = 0;
    return sizeof(*h) + (uint64_t) n * sizeof(struct sm_ag_log_ext);
} /* sm_ag_condense_into */

/* Rebuild the AG's free tree from a slot image: install the base extents,
 * then replay the appended deltas in order.  Caller holds ag->lock. */
static void
sm_ag_reconstruct(
    struct sm_ag  *ag,
    const uint8_t *slot)
{
    const struct sm_ag_log_header *h      = (const struct sm_ag_log_header *) slot;
    const struct sm_ag_log_ext    *base   = (const struct sm_ag_log_ext *) (slot + sizeof(*h));
    const struct sm_ag_log_delta  *deltas =
        (const struct sm_ag_log_delta *) (base + h->base_count);
    uint32_t                       i;

    rb_tree_destroy(&ag->free_by_offset, sm_extent_release, NULL);
    rb_tree_init(&ag->free_by_offset);
    ag->free_bytes = 0;

    for (i = 0; i < h->base_count; i++) {
        struct sm_extent *e = sm_extent_new(base[i].offset, base[i].length);
        rb_tree_insert(&ag->free_by_offset, offset, e);
        ag->free_bytes += base[i].length;
    }
    for (i = 0; i < h->delta_count; i++) {
        if (deltas[i].op == SM_AG_LOG_OP_ALLOC) {
            sm_ag_mark_used_locked(ag, deltas[i].offset, deltas[i].length);
        } else {
            sm_ag_free_locked(ag, deltas[i].offset, deltas[i].length);
        }
    }

    ag->log_generation  = h->generation;
    ag->log_base_count  = h->base_count;
    ag->log_delta_count = h->delta_count;
} /* sm_ag_reconstruct */

/*
 * Persist the allocator: condense each AG's free set into the *other* slot at
 * generation+1 and switch to it.  (Per-transaction delta journaling, which
 * makes interim allocations crash-durable, is layered on top of this; this
 * full condense is what runs at clean unmount and resets the delta log.)
 */
int
space_map_persist_paths(
    struct space_map *sm,
    char            **device_paths)
{
    uint32_t d, a;

    for (d = 0; d < sm->num_devices; d++) {
        struct sm_device *dev = &sm->devices[d];
        int               fd  = open(device_paths[d], O_WRONLY);

        if (fd < 0) {
            return -1;
        }

        for (a = 0; a < dev->num_ags; a++) {
            struct sm_ag *ag  = &dev->ags[a];
            uint8_t      *buf = calloc(1, SM_AG_LOG_SLOT_SIZE);
            uint32_t      slot;
            uint64_t      gen, payload, aligned, slot_off;
            ssize_t       n;

            pthread_mutex_lock(&ag->lock);
            slot     = 1 - ag->log_slot;
            gen      = ag->log_generation + 1;
            payload  = sm_ag_condense_into(ag, buf, gen);
            slot_off = ag->log_offset + (uint64_t) slot * SM_AG_LOG_SLOT_SIZE;
            aligned  = (payload + SM_BLOCK_SIZE - 1) & ~((uint64_t) SM_BLOCK_SIZE - 1);

            n = pwrite(fd, buf, aligned, slot_off);
            if (n == (ssize_t) aligned) {
                ag->log_slot        = slot;
                ag->log_generation  = gen;
                ag->log_base_count  = ((struct sm_ag_log_header *) buf)->base_count;
                ag->log_delta_count = 0;
            }
            pthread_mutex_unlock(&ag->lock);
            free(buf);
            if (n != (ssize_t) aligned) {
                close(fd);
                return -1;
            }
        }

        if (fsync(fd) != 0) {
            close(fd);
            return -1;
        }
        close(fd);
    }
    return 0;
} /* space_map_persist_paths */

int
space_map_load_paths(
    struct space_map *sm,
    char            **device_paths)
{
    uint32_t d, a;
    uint64_t free_total = 0;

    for (d = 0; d < sm->num_devices; d++) {
        struct sm_device *dev = &sm->devices[d];
        int               fd  = open(device_paths[d], O_RDONLY);

        if (fd < 0) {
            return -1;
        }

        for (a = 0; a < dev->num_ags; a++) {
            struct sm_ag           *ag = &dev->ags[a];
            struct sm_ag_log_header hdr[SM_AG_LOG_SLOT_COUNT];
            uint8_t                *buf;
            int                     best = -1;
            uint32_t                s;
            uint64_t                payload, slot_off;
            ssize_t                 n;

            /* Pick the live slot: highest generation with a valid magic. */
            for (s = 0; s < SM_AG_LOG_SLOT_COUNT; s++) {
                slot_off = ag->log_offset + (uint64_t) s * SM_AG_LOG_SLOT_SIZE;
                n        = pread(fd, &hdr[s], sizeof(hdr[s]), slot_off);
                if (n == (ssize_t) sizeof(hdr[s]) &&
                    hdr[s].magic == SM_AG_LOG_MAGIC &&
                    (best < 0 || hdr[s].generation > hdr[best].generation)) {
                    best = (int) s;
                }
            }
            if (best < 0) {
                close(fd);
                return -1;
            }

            payload = sizeof(struct sm_ag_log_header) +
                (uint64_t) hdr[best].base_count * sizeof(struct sm_ag_log_ext) +
                (uint64_t) hdr[best].delta_count * sizeof(struct sm_ag_log_delta);
            buf      = malloc(payload);
            slot_off = ag->log_offset + (uint64_t) best * SM_AG_LOG_SLOT_SIZE;
            n        = pread(fd, buf, payload, slot_off);
            if (n != (ssize_t) payload) {
                free(buf);
                close(fd);
                return -1;
            }

            pthread_mutex_lock(&ag->lock);
            sm_ag_reconstruct(ag, buf);
            ag->log_slot = (uint32_t) best;
            free_total  += ag->free_bytes;
            pthread_mutex_unlock(&ag->lock);
            free(buf);
        }
        close(fd);
    }

    sm->used_bytes = sm->total_capacity > free_total ?
        sm->total_capacity - free_total : 0;
    return 0;
} /* space_map_load_paths */
