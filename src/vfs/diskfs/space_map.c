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

#define SM_PERSIST_BATCH_OPS   128U
#define SM_PERSIST_BATCH_BYTES (16ULL << 20)

/* Test knob: trigger runtime AG-log condensation after this many deltas
 * instead of at the slot's byte high-water (0 = disabled).  Read lazily so a
 * test can set the variable before mount. */
static uint32_t
sm_condense_test_deltas_get(void)
{
    static int      init;
    static uint32_t val;

    if (!init) {
        const char *e = getenv("DISKFS_AG_CONDENSE_DELTAS");

        val  = e ? (uint32_t) strtoul(e, NULL, 10) : 0;
        init = 1;
    }
    return val;
} /* sm_condense_test_deltas_get */

/*
 * Append one allocation/free delta to the AG's active log slot, journaled
 * through the current transaction (via jnl) so it rides the main redo log and
 * is replayed on crash.  Caller holds ag->lock.  A NULL jnl (format-time or
 * bootstrap allocations) skips logging -- those are captured in the next
 * condensed base instead.  Deltas are appended after the condensed base in
 * 4 KiB-aligned, 128-per-block units (no block-spanning); the delta count
 * lives in the slot header (block 0).
 *
 * When the active slot's delta region hits its high-water mark, journaling
 * parks and a background condensation rewrites the inactive slot with a
 * fresh base and flips (space_map_condense_prepare/commit); clean unmounts
 * also re-condense every AG.
 */
/*
 * The two AG-log blocks an upcoming delta touches, claimed (and pinned into the
 * journaling transaction) up front so the actual write never faults.  Filled by
 * sm_ag_journal_claim; consumed by sm_ag_journal_write.  blk_buf == NULL means
 * "no journaling" (jnl was NULL, e.g. format/bootstrap).
 */
struct sm_ag_jnl_slot {
    void    *hdr_buf;
    void    *blk_buf;
    uint32_t in_block;
    uint32_t idx;
};

/*
 * Claim the AG's log header + current delta block for an upcoming delta, BEFORE
 * any allocator state is mutated.  Caller holds ag->lock.  All disk access is
 * async: claim_block returns NULL when the block is not resident (it parks the
 * journaling request and issues the read), in which case this returns SM_AGAIN
 * and the caller must unwind and retry the whole operation once resumed -- no
 * state has changed, so the retry is clean.  Claims the header (is_new == 0, so
 * it may need a read) first, so an SM_AGAIN there leaves nothing half-claimed;
 * a new delta block (in_block == 0) is is_new and never reads.
 */
static int
sm_ag_journal_claim(
    struct sm_ag            *ag,
    const struct sm_journal *jnl,
    struct sm_ag_jnl_slot   *slot)
{
    uint64_t slot_base, region_off, delta_byte, blk_off;

    if (!jnl) {
        slot->blk_buf = NULL;
        return 0;
    }

    /* Already condensing: park behind it. */
    if (ag->condensing) {
        jnl->ag_park(jnl->user, ag->device_id, ag->ag_index);
        return SM_AGAIN;
    }

    slot_base  = ag->log_offset + (uint64_t) ag->log_slot * SM_AG_LOG_SLOT_SIZE;
    region_off = (sizeof(struct sm_ag_log_header) +
                  (uint64_t) ag->log_base_count * sizeof(struct sm_ag_log_ext) +
                  SM_BLOCK_SIZE - 1) & ~((uint64_t) SM_BLOCK_SIZE - 1);
    slot->idx = ag->log_delta_count;

    delta_byte = region_off + (uint64_t) slot->idx * sizeof(struct sm_ag_log_delta);

    /* High-water: hand the slot to a background condensation (writes the
     * current free set as a fresh base into the inactive slot and flips) and
     * park this operation behind it.  The margin below the hard limit
     * absorbs nothing -- claims park immediately -- it is simply headroom so
     * the abort is unreachable short of a condensation bug.  The test knob
     * (DISKFS_AG_CONDENSE_DELTAS) triggers after a small fixed delta count
     * so tests can exercise the condensation path without journaling ~100K
     * deltas per AG. */
    if (delta_byte + SM_AG_LOG_CONDENSE_MARGIN > SM_AG_LOG_SLOT_SIZE ||
        (sm_condense_test_deltas_get() && slot->idx >= sm_condense_test_deltas_get())) {
        ag->condensing = 1;
        jnl->ag_condense(jnl->user, ag->device_id, ag->ag_index);
        jnl->ag_park(jnl->user, ag->device_id, ag->ag_index);
        return SM_AGAIN;
    }

    sm_abort_if(delta_byte + sizeof(struct sm_ag_log_delta) > SM_AG_LOG_SLOT_SIZE,
                "AG %u/%u delta region full (%u deltas) despite condensation",
                ag->device_id, ag->ag_index, slot->idx);

    blk_off        = slot_base + (delta_byte & ~((uint64_t) SM_BLOCK_SIZE - 1));
    slot->in_block = (uint32_t) (delta_byte & (SM_BLOCK_SIZE - 1));

    slot->hdr_buf = jnl->claim_block(jnl->user, ag->log_device_id, slot_base, 0);
    if (!slot->hdr_buf) {
        return SM_AGAIN;
    }
    slot->blk_buf = jnl->claim_block(jnl->user, ag->log_device_id, blk_off,
                                     slot->in_block == 0);
    if (!slot->blk_buf) {
        return SM_AGAIN;
    }
    return 0;
} /* sm_ag_journal_claim */

/* Write the delta into the pre-claimed log blocks.  Caller holds ag->lock and
 * has already mutated the free tree; this cannot fault. */
static void
sm_ag_journal_write(
    struct sm_ag                *ag,
    const struct sm_ag_jnl_slot *slot,
    uint32_t                     op,
    uint64_t                     offset,
    uint64_t                     length)
{
    struct sm_ag_log_header *h;
    struct sm_ag_log_delta  *d;

    if (!slot->blk_buf) {
        return;     /* no journaling */
    }

    d         = (struct sm_ag_log_delta *) ((char *) slot->blk_buf + slot->in_block);
    d->offset = offset;
    d->length = length;
    d->op     = op;
    d->pad    = 0;
    d->pad2   = 0;

    h                   = (struct sm_ag_log_header *) slot->hdr_buf;
    h->delta_count      = slot->idx + 1;
    ag->log_delta_count = slot->idx + 1;
} /* sm_ag_journal_write */

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
 * Initialize an AG.  Offsets are absolute device offsets.  The log lives on
 * `log_device_id` at `log_offset` (for a LOCAL AG this is the AG's own device,
 * carved off the front of the AG; for a relocated REMOTE AG it is a slot on the
 * local metadata device).  The allocatable data range is [data_off, data_off +
 * data_len) on the AG's own device -- the caller computes it so that the log
 * (LOCAL) and any format-time / signature reservations are excluded.
 */
static void
sm_ag_init(
    struct sm_ag *ag,
    uint32_t      device_id,
    uint32_t      ag_index,
    uint64_t      base_offset,
    uint64_t      size,
    uint32_t      log_device_id,
    uint64_t      log_offset,
    uint64_t      data_off,
    uint64_t      data_len)
{
    struct sm_extent *initial;

    ag->device_id       = device_id;
    ag->ag_index        = ag_index;
    ag->base_offset     = base_offset;
    ag->size            = size;
    ag->log_device_id   = log_device_id;
    ag->log_offset      = log_offset;
    ag->log_size        = SM_AG_LOG_SIZE;
    ag->log_slot        = 0;
    ag->log_generation  = 0;
    ag->log_base_count  = 0;
    ag->log_delta_count = 0;
    ag->condensing      = 0;

    pthread_mutex_init(&ag->lock, NULL);
    rb_tree_init(&ag->free_by_offset);

    if ((int64_t) data_len <= 0) {
        /* AG too small to hold even its own log + reservations: no data range. */
        ag->free_bytes = 0;
        return;
    }

    initial = sm_extent_new(data_off, data_len);

    rb_tree_insert(&ag->free_by_offset, offset, initial);
    ag->free_bytes = data_len;
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
    const struct sm_device_cfg *cfg,
    uint32_t                    num_devices)
{
    struct space_map *sm;
    struct sm_device *dev;
    struct sm_ag     *ag;
    uint32_t          d, a;
    uint32_t          remote_ag_total = 0;
    uint64_t          region_base, reloc_cursor;

    sm_abort_if(num_devices == 0, "space_map_create with zero devices");
    sm_abort_if(cfg[0].role != SM_DEV_LOCAL,
                "device 0 must be a local metadata device");

    sm              = calloc(1, sizeof(*sm));
    sm->num_devices = num_devices;
    sm->devices     = calloc(num_devices, sizeof(*sm->devices));
    pthread_mutex_init(&sm->lock, NULL);

    /* Pass 1: size each device and count remote AGs so the relocated-log
     * region (which is reserved out of device 0's AG 0) can be sized first. */
    for (d = 0; d < num_devices; d++) {
        dev            = &sm->devices[d];
        dev->device_id = d;
        dev->size      = cfg[d].size;
        dev->role      = cfg[d].role;
        dev->ag_rotor  = 0;
        dev->num_ags   = (dev->size + SM_AG_SIZE - 1) >> SM_AG_SIZE_LOG2;

        sm_abort_if(dev->num_ags == 0, "device %u has zero AGs (size=%lu)",
                    d, dev->size);

        if (dev->role == SM_DEV_REMOTE) {
            memcpy(dev->deviceid, cfg[d].deviceid, SM_DEVICEID_SIZE);
            dev->sig_offset = cfg[d].sig_offset;
            dev->sig_len    = cfg[d].sig_len > SM_SIG_MAX ? SM_SIG_MAX : cfg[d].sig_len;
            memcpy(dev->sig, cfg[d].sig, dev->sig_len);
            sm_abort_if(dev->sig_offset + dev->sig_len > SM_AG_SIZE,
                        "device %u signature must lie within the first AG", d);
            remote_ag_total += dev->num_ags;
            sm->num_remote_devices++;
        }

        dev->ags            = calloc(dev->num_ags, sizeof(*dev->ags));
        sm->total_capacity += dev->size;
    }

    /* The relocated remote-AG-log region sits on device 0, between the intent
     * log and AG 0's own space-map log.  Deterministic (device,ag)->slot map. */
    region_base           = SM_SUPERBLOCK_SIZE + SM_INTENT_LOG_SIZE;
    sm->remote_log_offset = region_base;
    sm->remote_log_size   = (uint64_t) remote_ag_total * SM_AG_LOG_SIZE;
    reloc_cursor          = region_base;

    /* Pass 2: lay out each AG. */
    for (d = 0; d < num_devices; d++) {
        dev = &sm->devices[d];

        for (a = 0; a < dev->num_ags; a++) {
            uint64_t base = (uint64_t) a << SM_AG_SIZE_LOG2;
            uint64_t span = SM_AG_SIZE;
            uint32_t log_device_id;
            uint64_t log_offset, data_off, data_end;

            if (base + span > dev->size) {
                span = dev->size - base;
            }

            ag = &dev->ags[a];

            if (dev->role == SM_DEV_REMOTE) {
                /* Remote (data-only) device: log relocated to device 0, the
                 * whole AG is allocatable data (AG 0 also excludes the
                 * signature region at the front). */
                log_device_id = SM_INTENT_LOG_DEVICE;
                log_offset    = reloc_cursor;
                reloc_cursor += SM_AG_LOG_SIZE;

                data_off = base;
                if (a == 0 && dev->sig_len) {
                    data_off = base + SM_ALIGN_UP(dev->sig_offset + dev->sig_len);
                }
                data_end = base + span;
                sm_ag_init(ag, d, a, base, span, log_device_id, log_offset,
                           data_off, data_end > data_off ? data_end - data_off : 0);
                continue;
            }

            /* Local device: the AG's log is carved off the front of the AG. */
            log_device_id = d;
            log_offset    = base;

            if (d == SM_INTENT_LOG_DEVICE && a == 0) {
                /* AG 0 of device 0 also hosts the superblock, the intent log
                 * and the relocated remote-AG-log region, all *before* this
                 * AG's own log.  Then the bootstrap inode blocks after the log
                 * (block_idx 1=reserved, 2=root inode, 3..=orphan-list
                 * shards). */
                uint64_t pre_log = SM_SUPERBLOCK_SIZE + SM_INTENT_LOG_SIZE +
                    sm->remote_log_size;
                uint64_t post_log = (2 + SM_BOOTSTRAP_ORPHAN_SLOTS) * SM_BLOCK_SIZE;

                log_offset = base + pre_log;

                data_off = log_offset + SM_AG_LOG_SIZE + post_log;
                sm_abort_if(pre_log + SM_AG_LOG_SIZE + post_log > span,
                            "device 0 AG 0 too small (%lu) to hold superblock+"
                            "intent_log+remote_log_region(%lu)+ag_log+bootstrap",
                            span, sm->remote_log_size);
            } else {
                data_off = log_offset + SM_AG_LOG_SIZE;
                sm_abort_if(SM_AG_LOG_SIZE >= span,
                            "AG size %lu too small to hold per-AG log %lu",
                            span, SM_AG_LOG_SIZE);
            }

            data_end = base + span;
            sm_ag_init(ag, d, a, base, span, log_device_id, log_offset,
                       data_off, data_end > data_off ? data_end - data_off : 0);
        }
    }

    /* Usable (allocatable) capacity is the live free total at format time, when
     * every AG holds its full data range and nothing is allocated yet.  Metadata
     * regions (logs, superblock, signatures) are already excluded from each AG's
     * data range, so they never count toward usable space. */
    for (d = 0; d < num_devices; d++) {
        for (a = 0; a < sm->devices[d].num_ags; a++) {
            sm->usable_capacity += sm->devices[d].ags[a].free_bytes;
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
    if (sm->num_remote_devices) {
        sm_info("Block mode: %u remote data device(s); relocated AG-log region "
                "on device %u offset %lu size %lu",
                sm->num_remote_devices, (unsigned) SM_INTENT_LOG_DEVICE,
                sm->remote_log_offset, sm->remote_log_size);
    }

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
    struct space_map        *sm,
    const struct sm_journal *jnl,
    uint32_t                 role,
    uint64_t                 want,
    uint32_t                *r_device_id,
    uint64_t                *r_device_offset)
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

        /* Block mode keeps metadata on LOCAL devices and data on REMOTE
         * devices; a role-matched allocation skips the other class. */
        if (dev->role != role) {
            continue;
        }

        pthread_mutex_lock(&sm->lock);
        start_ag = dev->ag_rotor;
        dev->ag_rotor++;
        if (dev->ag_rotor >= dev->num_ags) {
            dev->ag_rotor = 0;
        }
        pthread_mutex_unlock(&sm->lock);

        for (a = 0; a < dev->num_ags; a++) {
            uint32_t              ag_idx = (start_ag + a) % dev->num_ags;
            struct sm_ag         *ag     = &dev->ags[ag_idx];
            struct sm_ag_jnl_slot slot;
            uint64_t              offset;
            int                   rc, jrc;

            if (ag->free_bytes < want) {
                continue;
            }

            pthread_mutex_lock(&ag->lock);
            /* Claim the log blocks before mutating: on a journal-block miss this
             * returns SM_AGAIN with nothing changed, so the parked caller can
             * cleanly retry the whole allocation once the read completes. */
            jrc = sm_ag_journal_claim(ag, jnl, &slot);
            if (jrc == SM_AGAIN) {
                pthread_mutex_unlock(&ag->lock);
                return SM_AGAIN;
            }
            rc = sm_ag_alloc_locked(ag, want, &offset);
            if (rc == 0) {
                sm_ag_journal_write(ag, &slot, SM_AG_LOG_OP_ALLOC, offset, want);
            }
            pthread_mutex_unlock(&ag->lock);

            if (rc == 0) {
                *r_device_id     = dev_id;
                *r_device_offset = offset;
                return 0;
            }
        }
    }

    return -1;
} /* sm_pick_and_alloc */

int
space_map_reserve(
    struct space_map        *sm,
    struct sm_thread_cache  *cache,
    const struct sm_journal *jnl,
    uint32_t                 role,
    uint64_t                 min_bytes,
    uint64_t                 floor)
{
    uint64_t need = SM_ALIGN_UP(min_bytes);
    uint64_t want;
    int      rc;

    if (need == 0 || (cache->valid && cache->length >= need)) {
        return 0;
    }

    /* Cache cannot cover the request.  Return the unused remainder and grab a
     * fresh reservation.  Both the return (a FREE delta) and the refill (an
     * ALLOC delta) journal, so either may park on a cold log block and return
     * SM_AGAIN; the request is then parked and re-driven from the top.  This
     * stays retry-safe: the return clears cache->valid on success, so a retry
     * skips it, and the refill mutates nothing until its log blocks are claimed
     * (see sm_pick_and_alloc). */
    if (cache->valid) {
        rc = space_map_thread_cache_return(sm, jnl, cache);
        if (rc == SM_AGAIN) {
            return SM_AGAIN;
        }
    }

    want = need > floor ? need : floor;

    /* Speculative over-reservation (want > need) batches future small writes by
     * carving out a tail the caller keeps.  That tail is correct accounting --
     * statfs counts it as used -- but on a near-full device it lets a single
     * file privately corner the last free blocks: subsequent writes draw the
     * cached tail on the fast path *without* re-checking the allocator, so they
     * succeed even when the device truly has no free space (no ENOSPC).  Only
     * speculate when the filesystem can comfortably afford it -- enough free
     * that this reservation leaves at least another `want` behind.  Otherwise
     * reserve exactly `need`, so every block a write consumes is drawn (and
     * checked) against the live free pool and ENOSPC surfaces correctly.  This
     * runs on the (already heavyweight, journaling) refill path, not the cache
     * fast path, so the free-bytes scan is off the hot path. */
    if (want > need && space_map_free_bytes(sm) < 2 * want) {
        want = need;
    }

    /* A reservation can't cross an AG boundary, so cap at the AG size; a
     * caller needing more than SM_AG_SIZE contiguous cannot be satisfied. */
    if (want > SM_AG_SIZE) {
        return -1;
    }

    rc = sm_pick_and_alloc(sm, jnl, role, want, &cache->device_id, &cache->offset);
    if (rc == SM_AGAIN) {
        return SM_AGAIN;
    }
    if (rc != 0) {
        /* Try the smaller exact size before giving up, in case fragmentation
         * blocks the reservation but the caller's actual ask is small. */
        if (want != need) {
            rc = sm_pick_and_alloc(sm, jnl, role, need, &cache->device_id, &cache->offset);
            if (rc == SM_AGAIN) {
                return SM_AGAIN;
            }
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
    return 0;
} /* space_map_reserve */

int
space_map_alloc(
    struct space_map        *sm,
    struct sm_thread_cache  *cache,
    const struct sm_journal *jnl,
    uint32_t                 role,
    uint64_t                 size,
    uint64_t                 floor,
    uint32_t                *r_device_id,
    uint64_t                *r_device_offset)
{
    uint64_t need = SM_ALIGN_UP(size);
    int      rc;

    sm_abort_if(need == 0, "alloc of zero bytes");

    /* Ensure the cache covers `need` (refilling -- journals, may SM_AGAIN),
     * then dole from it.  Callers that have front-loaded the reservation via
     * space_map_reserve hit the fast path here with no journaling. */
    rc = space_map_reserve(sm, cache, jnl, role, need, floor);
    if (rc != 0) {
        return rc;     /* SM_AGAIN or -1 (ENOSPC) */
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

/* Resolve and validate the AG owning [device_offset, device_offset+aligned). */
static struct sm_ag *
sm_free_resolve_ag(
    struct space_map *sm,
    uint32_t          device_id,
    uint64_t          device_offset,
    uint64_t          aligned,
    const char       *who)
{
    struct sm_device *dev;
    struct sm_ag     *ag;
    uint32_t          ag_idx;

    sm_abort_if(device_id >= sm->num_devices, "%s: bad device_id %u", who, device_id);
    dev    = &sm->devices[device_id];
    ag_idx = device_offset >> SM_AG_SIZE_LOG2;
    sm_abort_if(ag_idx >= dev->num_ags, "%s: offset %lu past device %u",
                who, device_offset, device_id);
    ag = &dev->ags[ag_idx];
    /* Frees never straddle AGs (reservations come from a single AG). */
    sm_abort_if(device_offset + aligned > ag->base_offset + ag->size,
                "%s: range crosses AG boundary (offset=%lu length=%lu ag_base=%lu ag_size=%lu)",
                who, device_offset, aligned, ag->base_offset, ag->size);
    return ag;
} /* sm_free_resolve_ag */

/*
 * Reduce a free request to the whole blocks strictly contained in
 * [device_offset, device_offset+length).  For an aligned offset (the common
 * case: callers pass block-multiple lengths) this is the full range.  A
 * sub-block, unaligned extent -- produced when DEALLOCATE / zero-range trims an
 * extent at a non-block boundary, a mapping the read path supports via padding
 * -- shares its partial edge blocks with the neighbouring extents carved from
 * the same allocation, so freeing an edge could release a block another extent
 * still maps.  Releasing only the interior is conservative (a partial edge
 * block leaks, consistent with the filesystem's deferred-reclaim model) but
 * never double-frees or frees a shared block.  Returns 0 if no whole block is
 * contained (nothing to free).
 */
static int
sm_free_block_range(
    uint64_t  device_offset,
    uint64_t  length,
    uint64_t *r_offset,
    uint64_t *r_length)
{
    uint64_t start = SM_ALIGN_UP(device_offset);
    uint64_t end   = (device_offset + length) & ~(uint64_t) SM_BLOCK_MASK;

    if (end <= start) {
        return 0;
    }
    *r_offset = start;
    *r_length = end - start;
    return 1;
} /* sm_free_block_range */

/*
 * Journal a FREE delta into the AG's redo log without making the range
 * reusable.  The delta rides the freeing transaction's redo (so a crash
 * replays it via AG-log reconstruction), but the in-memory free is withheld
 * until the transaction is durable -- see space_map_free_apply.  This is the
 * intent half of a "pending free": it prevents the range from being handed out
 * (and a still-cached/pinned metadata block from being reclaimed) before the
 * transaction that freed it has committed.
 */
int
space_map_free_journal(
    struct space_map        *sm,
    const struct sm_journal *jnl,
    uint32_t                 device_id,
    uint64_t                 device_offset,
    uint64_t                 length)
{
    uint64_t              offset, aligned;
    struct sm_ag         *ag;
    struct sm_ag_jnl_slot slot;
    int                   jrc;

    if (!sm_free_block_range(device_offset, length, &offset, &aligned)) {
        return 0;
    }
    ag = sm_free_resolve_ag(sm, device_id, offset, aligned, "space_map_free_journal");

    pthread_mutex_lock(&ag->lock);
    /* Claim before write; on a cold log block this parks the caller and
     * returns SM_AGAIN with nothing changed, for a clean retry. */
    jrc = sm_ag_journal_claim(ag, jnl, &slot);
    if (jrc == SM_AGAIN) {
        pthread_mutex_unlock(&ag->lock);
        return SM_AGAIN;
    }
    sm_ag_journal_write(ag, &slot, SM_AG_LOG_OP_FREE, offset, aligned);
    pthread_mutex_unlock(&ag->lock);
    return 0;
} /* space_map_free_journal */

/*
 * Apply the in-memory free, returning the range to its AG's free pool.  Call
 * once the transaction that journaled the matching FREE delta is durable.
 */
void
space_map_free_apply(
    struct space_map *sm,
    uint32_t          device_id,
    uint64_t          device_offset,
    uint64_t          length)
{
    uint64_t      offset, aligned;
    struct sm_ag *ag;

    if (!sm_free_block_range(device_offset, length, &offset, &aligned)) {
        return;
    }
    ag = sm_free_resolve_ag(sm, device_id, offset, aligned, "space_map_free_apply");

    pthread_mutex_lock(&ag->lock);
    sm_ag_free_locked(ag, offset, aligned);
    pthread_mutex_unlock(&ag->lock);
} /* space_map_free_apply */

/*
 * Immediate free (journal + apply in one step).  Safe only for ranges that are
 * not referenced by any cached/pinned block and need no transactional ordering
 * -- e.g. returning a thread's leftover reservation.  Transactional frees use
 * the pending (journal-then-apply-on-commit) path instead.
 */
int
space_map_free(
    struct space_map        *sm,
    const struct sm_journal *jnl,
    uint32_t                 device_id,
    uint64_t                 device_offset,
    uint64_t                 length)
{
    /* Journal first (may park -> SM_AGAIN, nothing applied yet); only on a
     * durable claim do we return the range to the in-memory free pool. */
    if (space_map_free_journal(sm, jnl, device_id, device_offset, length) == SM_AGAIN) {
        return SM_AGAIN;
    }
    space_map_free_apply(sm, device_id, device_offset, length);
    return 0;
} /* space_map_free */

int
space_map_thread_cache_return(
    struct space_map        *sm,
    const struct sm_journal *jnl,
    struct sm_thread_cache  *cache)
{
    if (!cache->valid || cache->length == 0) {
        cache->valid = 0;
        return 0;
    }

    /* On SM_AGAIN the cache is left valid so a retry re-attempts the return. */
    if (space_map_free(sm, jnl, cache->device_id, cache->offset, cache->length) == SM_AGAIN) {
        return SM_AGAIN;
    }
    cache->valid  = 0;
    cache->length = 0;
    return 0;
} /* space_map_thread_cache_return */

void
space_map_fill_superblock(
    struct space_map *sm,
    void             *buf,
    uint64_t          fsid,
    uint64_t          flags,
    uint64_t          root_inum,
    uint32_t          root_gen,
    uint64_t          log_seq,
    uint64_t          gen_floor)
{
    struct sm_superblock *sb = (struct sm_superblock *) buf;

    memset(buf, 0, SM_SUPERBLOCK_SIZE);

    sb->magic              = SM_SUPERBLOCK_MAGIC;
    sb->version            = SM_FORMAT_VERSION;
    sb->block_size         = SM_BLOCK_SIZE;
    sb->ag_size            = SM_AG_SIZE;
    sb->ag_log_size        = SM_AG_LOG_SIZE;
    sb->fsid               = fsid;
    sb->num_devices        = sm->num_devices;
    sb->intent_log_device  = SM_INTENT_LOG_DEVICE;
    sb->intent_log_offset  = SM_INTENT_LOG_OFFSET;
    sb->intent_log_size    = SM_INTENT_LOG_SIZE;
    sb->flags              = flags;
    sb->root_inum          = root_inum;
    sb->root_gen           = root_gen;
    sb->log_seq            = log_seq;
    sb->num_remote_devices = sm->num_remote_devices;
    sb->remote_log_device  = SM_INTENT_LOG_DEVICE;
    sb->remote_log_offset  = sm->remote_log_offset;
    sb->remote_log_size    = sm->remote_log_size;
    sb->gen_floor          = gen_floor;
    sb->crc32              = 0;
    sb->crc32              = sm_crc32(buf, SM_SUPERBLOCK_SIZE);
} /* space_map_fill_superblock */

int
space_map_write_superblock(
    struct space_map   *sm,
    const struct sm_io *io,
    uint64_t            fsid,
    uint64_t            flags,
    uint64_t            root_inum,
    uint32_t            root_gen,
    uint64_t            log_seq,
    uint64_t            gen_floor)
{
    uint8_t buf[SM_SUPERBLOCK_SIZE];

    space_map_fill_superblock(sm, buf, fsid, flags, root_inum, root_gen,
                              log_seq, gen_floor);

    if (io->write(io->user, 0, buf, sizeof(buf), SM_SUPERBLOCK_OFFSET) != 0) {
        return -1;
    }

    return io->flush(io->user, 0);
} /* space_map_write_superblock */

/*
 * Read and validate the superblock from device 0.  Returns 0 and fills *out
 * if a well-formed, current-version superblock with a matching CRC is present;
 * returns -1 (no/!valid superblock -> caller should mkfs) on any mismatch.
 */
int
space_map_read_superblock(
    const struct sm_io   *io,
    struct sm_superblock *out)
{
    uint8_t               buf[SM_SUPERBLOCK_SIZE];
    struct sm_superblock *sb = (struct sm_superblock *) buf;
    uint32_t              stored, computed;

    if (io->read(io->user, 0, buf, sizeof(buf), SM_SUPERBLOCK_OFFSET) != 0) {
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
} /* space_map_read_superblock */

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
    free(e);
} /* sm_ag_mark_used_locked */

static uint64_t sm_ag_condense_into(
    struct sm_ag *ag,
    uint8_t      *slot,
    uint64_t      generation);

/*
 * Runtime condensation, phase 1: snapshot the AG's free set as a condensed
 * base image (header at buf[0]) for the INACTIVE slot.  Called by the
 * background condenser once jnl->ag_condense fired; all journaling into this
 * AG is parked, so the set is mutated only by free-applies of durable
 * transactions, and any snapshot is crash-safe: allocations are reflected
 * eagerly (a lost one merely leaks) and frees only once durable (so the base
 * never frees a block a committed file still references).  One bounded,
 * self-healing crash window exists: a FREE delta journaled into the old slot
 * whose txn becomes durable after the flip lives only in the dead slot, so a
 * crash before the next condensation/clean unmount leaks that range (the
 * next snapshot re-captures it from memory).  buf must hold
 * SM_AG_LOG_SLOT_SIZE bytes.
 *
 * The caller must write the image to *r_slot_offset with the HEADER BLOCK
 * (first 4 KiB) LAST and FUA: the inactive slot stays invalid (stale, lower
 * generation) until the header lands, so a crash at any point leaves the old
 * slot authoritative.  Then call space_map_condense_commit.
 */
int
space_map_condense_prepare(
    struct space_map *sm,
    uint32_t          device_id,
    uint32_t          ag_index,
    void             *buf,
    uint64_t         *r_slot_offset,
    uint64_t         *r_payload)
{
    struct sm_ag *ag = &sm->devices[device_id].ags[ag_index];
    uint64_t      payload;

    pthread_mutex_lock(&ag->lock);
    sm_abort_if(!ag->condensing, "condense_prepare on a non-condensing AG");

    payload = sm_ag_condense_into(ag, (uint8_t *) buf, ag->log_generation + 1);

    *r_slot_offset = ag->log_offset +
        (uint64_t) (1 - ag->log_slot) * SM_AG_LOG_SLOT_SIZE;
    *r_payload = payload;
    pthread_mutex_unlock(&ag->lock);
    return 0;
} /* space_map_condense_prepare */

/*
 * Runtime condensation, phase 2: the new base image is durable in the
 * inactive slot (header written last) -- flip to it and clear the gate.  The
 * caller then re-drives every operation it parked for this AG.
 */
void
space_map_condense_commit(
    struct space_map *sm,
    uint32_t          device_id,
    uint32_t          ag_index,
    uint32_t          base_count)
{
    struct sm_ag *ag = &sm->devices[device_id].ags[ag_index];

    pthread_mutex_lock(&ag->lock);
    sm_abort_if(!ag->condensing, "condense_commit on a non-condensing AG");
    ag->log_slot        = 1 - ag->log_slot;
    ag->log_generation += 1;
    ag->log_base_count  = base_count;
    ag->log_delta_count = 0;
    ag->condensing      = 0;
    pthread_mutex_unlock(&ag->lock);
} /* space_map_condense_commit */

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

struct sm_persist_update {
    struct sm_ag *ag;
    uint32_t      slot;
    uint64_t      generation;
    uint32_t      base_count;
};

static int
sm_persist_batch_submit(
    const struct sm_io       *io,
    struct sm_io_write       *writes,
    struct sm_persist_update *updates,
    uint32_t                 *count,
    uint64_t                 *bytes)
{
    uint32_t i;
    int      rc = 0;

    if (*count == 0) {
        return 0;
    }

    if (io->write_many) {
        rc = io->write_many(io->user, writes, *count);
    } else {
        for (i = 0; i < *count; i++) {
            if (io->write(io->user, writes[i].device_id, writes[i].buf,
                          writes[i].length, writes[i].offset) != 0) {
                rc = -1;
                break;
            }
        }
    }

    if (rc == 0) {
        for (i = 0; i < *count; i++) {
            pthread_mutex_lock(&updates[i].ag->lock);
            updates[i].ag->log_slot        = updates[i].slot;
            updates[i].ag->log_generation  = updates[i].generation;
            updates[i].ag->log_base_count  = updates[i].base_count;
            updates[i].ag->log_delta_count = 0;
            pthread_mutex_unlock(&updates[i].ag->lock);
        }
    }

    *count = 0;
    *bytes = 0;
    return rc;
} /* sm_persist_batch_submit */

/*
 * Persist the allocator: condense each AG's free set into the *other* slot at
 * generation+1 and switch to it.  (Per-transaction delta journaling, which
 * makes interim allocations crash-durable, is layered on top of this; this
 * full condense is what runs at clean unmount and resets the delta log.)
 */
int
space_map_persist(
    struct space_map   *sm,
    const struct sm_io *io)
{
    struct sm_io_write       writes[SM_PERSIST_BATCH_OPS];
    struct sm_persist_update updates[SM_PERSIST_BATCH_OPS];
    uint8_t                 *arena;
    uint32_t                 count = 0;
    uint64_t                 bytes = 0;
    uint32_t                 d, a;

    /* Slot images are condensed straight into one arena and submitted in
     * batches.  An accepted batch holds at most SM_PERSIST_BATCH_BYTES, and
     * one more image (at most SM_AG_LOG_SLOT_SIZE) is condensed past that
     * mark before an over-full batch is flushed. */
    arena = malloc(SM_PERSIST_BATCH_BYTES + SM_AG_LOG_SLOT_SIZE);
    if (!arena) {
        return -1;
    }

    for (d = 0; d < sm->num_devices; d++) {
        struct sm_device *dev = &sm->devices[d];

        for (a = 0; a < dev->num_ags; a++) {
            struct sm_ag *ag = &dev->ags[a];
            uint8_t      *buf;
            uint32_t      slot;
            uint64_t      gen, payload, aligned, slot_off;

            pthread_mutex_lock(&ag->lock);
            buf      = arena + bytes;
            slot     = 1 - ag->log_slot;
            gen      = ag->log_generation + 1;
            payload  = sm_ag_condense_into(ag, buf, gen);
            slot_off = ag->log_offset + (uint64_t) slot * SM_AG_LOG_SLOT_SIZE;
            aligned  = (payload + SM_BLOCK_SIZE - 1) & ~((uint64_t) SM_BLOCK_SIZE - 1);
            memset(buf + payload, 0, aligned - payload);

            if (count == SM_PERSIST_BATCH_OPS ||
                (bytes > 0 && bytes + aligned > SM_PERSIST_BATCH_BYTES)) {
                pthread_mutex_unlock(&ag->lock);
                if (sm_persist_batch_submit(io, writes, updates, &count, &bytes) != 0) {
                    free(arena);
                    return -1;
                }
                /* The AG may have mutated while unlocked during the submit;
                 * recondense it into the now-empty arena. */
                pthread_mutex_lock(&ag->lock);
                buf      = arena;
                slot     = 1 - ag->log_slot;
                gen      = ag->log_generation + 1;
                payload  = sm_ag_condense_into(ag, buf, gen);
                slot_off = ag->log_offset + (uint64_t) slot * SM_AG_LOG_SLOT_SIZE;
                aligned  = (payload + SM_BLOCK_SIZE - 1) & ~((uint64_t) SM_BLOCK_SIZE - 1);
                memset(buf + payload, 0, aligned - payload);
            }

            /* The log lives on log_device_id (== d for local AGs, the local
             * metadata device for a relocated remote AG). */
            writes[count].device_id   = ag->log_device_id;
            writes[count].buf         = buf;
            writes[count].length      = aligned;
            writes[count].offset      = slot_off;
            updates[count].ag         = ag;
            updates[count].slot       = slot;
            updates[count].generation = gen;
            updates[count].base_count =
                ((struct sm_ag_log_header *) buf)->base_count;
            count++;
            bytes += aligned;
            pthread_mutex_unlock(&ag->lock);
        }
    }

    /* Submit the final partial batch. */
    if (sm_persist_batch_submit(io, writes, updates, &count, &bytes) != 0) {
        free(arena);
        return -1;
    }

    /* Flush only devices that hold real storage; remote data devices have no
     * local handle and never received writes (their logs rode device 0). */
    for (d = 0; d < sm->num_devices; d++) {
        if (sm->devices[d].role != SM_DEV_LOCAL) {
            continue;
        }
        if (io->flush(io->user, d) != 0) {
            free(arena);
            return -1;
        }
    }
    free(arena);
    return 0;
} /* space_map_persist */

uint64_t
space_map_free_bytes(struct space_map *sm)
{
    uint64_t free = 0;
    uint32_t d, a;

    /* Cold path (statfs): sum each AG's live free count.  Space held in a
     * thread's reservation cache (carved out of an AG but not yet handed to a
     * file) reads as not-free here, so the report is conservative -- never an
     * overestimate of what can still be allocated. */
    for (d = 0; d < sm->num_devices; d++) {
        struct sm_device *dev = &sm->devices[d];

        for (a = 0; a < dev->num_ags; a++) {
            struct sm_ag *ag = &dev->ags[a];

            pthread_mutex_lock(&ag->lock);
            free += ag->free_bytes;
            pthread_mutex_unlock(&ag->lock);
        }
    }
    return free;
} /* space_map_free_bytes */

int
space_map_load(
    struct space_map   *sm,
    const struct sm_io *io)
{
    uint32_t d, a;

    for (d = 0; d < sm->num_devices; d++) {
        struct sm_device *dev = &sm->devices[d];

        for (a = 0; a < dev->num_ags; a++) {
            struct sm_ag           *ag = &dev->ags[a];
            struct sm_ag_log_header hdr[SM_AG_LOG_SLOT_COUNT];
            uint8_t                *buf;
            int                     best = -1;
            uint32_t                s;
            uint64_t                payload, slot_off;

            /* Pick the live slot: highest generation with a valid magic.  The
             * log lives on log_device_id (the local metadata device for a
             * relocated remote AG). */
            for (s = 0; s < SM_AG_LOG_SLOT_COUNT; s++) {
                slot_off = ag->log_offset + (uint64_t) s * SM_AG_LOG_SLOT_SIZE;
                if (io->read(io->user, ag->log_device_id, &hdr[s], sizeof(hdr[s]), slot_off) == 0 &&
                    hdr[s].magic == SM_AG_LOG_MAGIC &&
                    (best < 0 || hdr[s].generation > hdr[best].generation)) {
                    best = (int) s;
                }
            }
            if (best < 0) {
                return -1;
            }

            payload = sizeof(struct sm_ag_log_header) +
                (uint64_t) hdr[best].base_count * sizeof(struct sm_ag_log_ext) +
                (uint64_t) hdr[best].delta_count * sizeof(struct sm_ag_log_delta);
            buf      = malloc(payload);
            slot_off = ag->log_offset + (uint64_t) best * SM_AG_LOG_SLOT_SIZE;
            if (io->read(io->user, ag->log_device_id, buf, payload, slot_off) != 0) {
                free(buf);
                return -1;
            }

            pthread_mutex_lock(&ag->lock);
            sm_ag_reconstruct(ag, buf);
            ag->log_slot = (uint32_t) best;
            pthread_mutex_unlock(&ag->lock);
            free(buf);
        }
    }

    return 0;
} /* space_map_load */
