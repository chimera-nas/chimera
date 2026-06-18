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


static struct sm_ag *
sm_free_resolve_ag(
    struct space_map *sm,
    uint32_t          device_id,
    uint64_t          device_offset,
    uint64_t          aligned,
    const char       *who);

static void
sm_init_device_free_totals(
    struct space_map *sm);


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
 * Segregated free-list (by-size) index, maintained alongside free_by_offset.
 * An extent's class is floor(log2(blocks)); allocation finds an adequate
 * extent via the per-AG non-empty class bitmask instead of scanning the
 * offset-ordered tree.  All callers hold ag->lock.
 */
static inline uint32_t
sm_ag_size_class(uint64_t length)
{
    uint64_t blocks = length >> SM_BLOCK_SHIFT;
    uint32_t c;

    if (blocks <= 1) {
        return 0;
    }
    c = 63u - (uint32_t) __builtin_clzll(blocks);   /* floor(log2(blocks)) */
    return c < SM_SIZE_CLASSES ? c : SM_SIZE_CLASSES - 1;
} /* sm_ag_size_class */

/*
 * Mirror this AG's current largest non-empty size class into the device's
 * lock-free free-space index.  Called at the end of every size-list mutation
 * (under ag->lock).  Only acts on a max-class transition (infrequent); the
 * bitmap writes are atomic OR/AND with no shared lock, so concurrent
 * allocators on different AGs never serialize here -- the only interference is
 * cache-line sharing among the 64 AGs that share a word.
 */
static void
sm_ag_mc_update(struct sm_ag *ag)
{
    struct sm_device *dev = ag->dev;
    int               newmax;

    if (!dev) {
        return;     /* pre-wiring (should not happen once initialized) */
    }
    newmax = ag->size_nonempty
             ? (int) (31u - (uint32_t) __builtin_clz(ag->size_nonempty))
             : -1;
    if (newmax == ag->maxclass) {
        return;
    }
    /* Publish into the new class BEFORE retiring the old one, so a concurrent
     * lock-free lookup always finds this AG in at least one class (never in a
     * gap).  A transiently-stale entry in the old class is harmless: the
     * lookup re-verifies under ag->lock (sm_ag_can_alloc_locked). */
    if (newmax >= 0) {
        __atomic_fetch_or(&dev->maxclass_bits[newmax][ag->ag_index >> 6],
                          (1ULL << (ag->ag_index & 63u)), __ATOMIC_RELAXED);
        __atomic_fetch_add(&dev->mc_class_count[newmax], 1, __ATOMIC_RELAXED);
    }
    if (ag->maxclass >= 0) {
        __atomic_fetch_and(&dev->maxclass_bits[ag->maxclass][ag->ag_index >> 6],
                           ~(1ULL << (ag->ag_index & 63u)), __ATOMIC_RELAXED);
        __atomic_fetch_sub(&dev->mc_class_count[ag->maxclass], 1, __ATOMIC_RELAXED);
    }
    ag->maxclass = (int16_t) newmax;
} /* sm_ag_mc_update */

/* Push ext onto the head of its size-class list. */
static void
sm_ag_size_link(
    struct sm_ag     *ag,
    struct sm_extent *ext)
{
    uint32_t c = sm_ag_size_class(ext->length);

    ext->size_prev = NULL;
    ext->size_next = ag->free_by_size[c];
    if (ag->free_by_size[c]) {
        ag->free_by_size[c]->size_prev = ext;
    }
    ag->free_by_size[c] = ext;
    ag->size_nonempty  |= (1u << c);
    sm_ag_mc_update(ag);
} /* sm_ag_size_link */

/*
 * Unlink ext from its size-class list.  Must be called while ext->length still
 * holds the value it was linked under (i.e. unlink before mutating length).
 */
static void
sm_ag_size_unlink(
    struct sm_ag     *ag,
    struct sm_extent *ext)
{
    uint32_t c = sm_ag_size_class(ext->length);

    if (ext->size_prev) {
        ext->size_prev->size_next = ext->size_next;
    } else {
        ag->free_by_size[c] = ext->size_next;
    }
    if (ext->size_next) {
        ext->size_next->size_prev = ext->size_prev;
    }
    ext->size_prev = ext->size_next = NULL;
    if (!ag->free_by_size[c]) {
        ag->size_nonempty &= ~(1u << c);
    }
    sm_ag_mc_update(ag);
} /* sm_ag_size_unlink */

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
    struct sm_ag     *ag,
    struct sm_device *dev,
    uint32_t          device_id,
    uint32_t          ag_index,
    uint64_t          base_offset,
    uint64_t          size,
    uint32_t          log_device_id,
    uint64_t          log_offset,
    uint64_t          data_off,
    uint64_t          data_len)
{
    struct sm_extent *initial;

    ag->device_id       = device_id;
    ag->ag_index        = ag_index;
    ag->dev             = dev;       /* must precede size_link -> sm_ag_mc_update */
    ag->maxclass        = -1;
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
    memset(ag->free_by_size, 0, sizeof(ag->free_by_size));
    ag->size_nonempty = 0;

    if ((int64_t) data_len <= 0) {
        /* AG too small to hold even its own log + reservations: no data range. */
        ag->free_bytes = 0;
        return;
    }

    initial = sm_extent_new(data_off, data_len);

    rb_tree_insert(&ag->free_by_offset, offset, initial);
    sm_ag_size_link(ag, initial);
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
    struct sm_extent *ext   = NULL;
    uint32_t          klass = sm_ag_size_class(size);
    uint32_t          higher;

    if (ag->free_bytes < size) {
        return -1;
    }

    /*
     * Find an adequate free extent via the size-class index instead of
     * scanning free_by_offset.  Every class strictly above `klass` holds only
     * extents whose block count is >= 2^(klass+1) > size, so any of their
     * heads fits -- pick the lowest non-empty such class in O(1).  Extents
     * smaller than the request live in lower classes the mask skips, so the
     * old O(N) walk past small fragments is gone.
     */
    higher = (klass + 1 < SM_SIZE_CLASSES)
             ? (ag->size_nonempty & ~((1u << (klass + 1)) - 1u))
             : 0u;
    if (higher) {
        ext = ag->free_by_size[__builtin_ctz(higher)];
    } else if (ag->size_nonempty & (1u << klass)) {
        /*
         * No larger extent anywhere in the AG (near-full): the only candidates
         * share `size`'s class and may be smaller than the request, so scan
         * just that one class for the first fit.  Bounded to a single class
         * and only reached when the AG holds no bigger extent.
         */
        struct sm_extent *e;

        for (e = ag->free_by_size[klass]; e; e = e->size_next) {
            if (e->length >= size) {
                ext = e;
                break;
            }
        }
    }

    if (!ext) {
        return -1;
    }

    *r_offset = ext->offset;
    rb_tree_remove(&ag->free_by_offset, &ext->node);
    sm_ag_size_unlink(ag, ext);

    if (ext->length == size) {
        free(ext);
    } else {
        /* Shrink: offset is the rb-tree key, so remove + reinsert both indexes. */
        ext->offset += size;
        ext->length -= size;
        rb_tree_insert(&ag->free_by_offset, offset, ext);
        sm_ag_size_link(ag, ext);
    }

    ag->free_bytes -= size;
    __atomic_sub_fetch(&ag->dev->free_bytes, size, __ATOMIC_RELAXED);
    return 0;
} /* sm_ag_alloc_locked */

/*
 * Read-only peek: does this AG hold a free extent that can satisfy `size`?
 * Same selection logic as sm_ag_alloc_locked but without mutating.  Used by
 * sm_pick_and_alloc to avoid claiming/journaling an AG's log blocks before we
 * know it can actually satisfy the request (Bug 2: a fragmented AG with
 * free_bytes >= size but no contiguous extent would otherwise leak a journal
 * block into the transaction on every visit).  Caller holds ag->lock.
 */
static int
sm_ag_can_alloc_locked(
    struct sm_ag *ag,
    uint64_t      size)
{
    uint32_t klass = sm_ag_size_class(size);
    uint32_t higher;

    if (ag->free_bytes < size) {
        return 0;
    }
    higher = (klass + 1 < SM_SIZE_CLASSES)
             ? (ag->size_nonempty & ~((1u << (klass + 1)) - 1u))
             : 0u;
    if (higher) {
        return 1;       /* a strictly-larger class holds an extent >= size */
    }
    if (ag->size_nonempty & (1u << klass)) {
        struct sm_extent *e;
        for (e = ag->free_by_size[klass]; e; e = e->size_next) {
            if (e->length >= size) {
                return 1;
            }
        }
    }
    return 0;
} /* sm_ag_can_alloc_locked */

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

    /* prev (floor of offset) and next (ceil of offset+length) are necessarily
     * distinct nodes: the overlap aborts above pin prev to end <= offset and
     * next to start >= offset+length, so one extent cannot be both.  Asserting
     * it makes the coalesce below provably safe -- it frees next_ext and then
     * relinks prev_ext, which is only a use-after-free if the two alias. */
    sm_abort_if(prev_ext && prev_ext == next_ext,
                "floor/ceil resolved the same extent at offset=%lu", offset);

    if (prev_ext && prev_ext->offset + prev_ext->length == offset) {
        sm_ag_size_unlink(ag, prev_ext);   /* length about to grow; relink below */
        prev_ext->length += length;
        merged_with_prev  = 1;
    }

    if (merged_with_prev) {
        if (next_ext && next_ext->offset == prev_ext->offset + prev_ext->length) {
            sm_ag_size_unlink(ag, next_ext);
            prev_ext->length += next_ext->length;
            rb_tree_remove(&ag->free_by_offset, &next_ext->node);
            free(next_ext);
        }
        sm_ag_size_link(ag, prev_ext);     /* relink prev with its final length */
    } else if (next_ext && next_ext->offset == offset + length) {
        sm_ag_size_unlink(ag, next_ext);   /* offset/length about to change */
        rb_tree_remove(&ag->free_by_offset, &next_ext->node);
        next_ext->offset  = offset;
        next_ext->length += length;
        rb_tree_insert(&ag->free_by_offset, offset, next_ext);
        sm_ag_size_link(ag, next_ext);
    } else {
        struct sm_extent *fresh = sm_extent_new(offset, length);
        rb_tree_insert(&ag->free_by_offset, offset, fresh);
        sm_ag_size_link(ag, fresh);
    }

    ag->free_bytes += length;
    __atomic_add_fetch(&ag->dev->free_bytes, length, __ATOMIC_RELAXED);
} /* sm_ag_free_locked */

struct space_map *
space_map_create(
    const struct sm_device_cfg *cfg,
    uint32_t                    num_devices,
    uint64_t                    intent_log_size)
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
    sm_abort_if(intent_log_size == 0 || (intent_log_size & SM_BLOCK_MASK),
                "intent_log_size %lu must be a non-zero multiple of the block size",
                intent_log_size);

    sm                  = calloc(1, sizeof(*sm));
    sm->intent_log_size = intent_log_size;
    sm->num_devices     = num_devices;
    sm->devices         = calloc(num_devices, sizeof(*sm->devices));
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

        /* Lock-free free-space index: one bit per AG per size class.  Allocated
         * before sm_ag_init (which links the AG's initial extent and sets its
         * max-class bit). */
        dev->mc_words = (dev->num_ags + 63u) >> 6;
        dev->mc_rotor = 0;
        {
            uint32_t c;
            for (c = 0; c < SM_SIZE_CLASSES; c++) {
                dev->maxclass_bits[c] = calloc(dev->mc_words, sizeof(uint64_t));
                sm_abort_if(!dev->maxclass_bits[c], "maxclass_bits alloc failed");
            }
        }
    }

    /* The relocated remote-AG-log region sits on device 0, between the intent
     * log and AG 0's own space-map log.  Deterministic (device,ag)->slot map. */
    region_base           = SM_SUPERBLOCK_SIZE + sm->intent_log_size;
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
                sm_ag_init(ag, dev, d, a, base, span, log_device_id, log_offset,
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
                 * (block_idx 1=reserved, 2=root inode, 3..=orphan-list shards,
                 * then the extent-refcount tree(s)). */
                uint64_t pre_log = SM_SUPERBLOCK_SIZE + sm->intent_log_size +
                    sm->remote_log_size;
                uint64_t post_log = (2 + SM_BOOTSTRAP_ORPHAN_SLOTS +
                                     SM_BOOTSTRAP_REFCOUNT_SLOTS) * SM_BLOCK_SIZE;

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
            sm_ag_init(ag, dev, d, a, base, span, log_device_id, log_offset,
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
            sm->intent_log_size);
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

    sm_init_device_free_totals(sm);
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
        {
            uint32_t c;
            for (c = 0; c < SM_SIZE_CLASSES; c++) {
                free(dev->maxclass_bits[c]);
            }
        }
    }

    pthread_mutex_destroy(&sm->lock);
    free(sm->devices);
    free(sm);
} /* space_map_destroy */

/* Try to allocate `want` from one AG: 0 = allocated, SM_AGAIN = parked (retry
 * the whole allocation), 1 = this AG cannot satisfy (try another).  Claims and
 * journals only after confirming a fit under ag->lock, so a non-fitting AG
 * never leaks a journal block into the txn (Bug 2). */
static int
sm_try_alloc_from_ag(
    struct sm_ag            *ag,
    const struct sm_journal *jnl,
    uint64_t                 want,
    uint32_t                *r_device_id,
    uint64_t                *r_device_offset)
{
    uint64_t offset;
    int      rc;

    if (__atomic_load_n(&ag->free_bytes, __ATOMIC_RELAXED) < want) {
        return 1;
    }
    pthread_mutex_lock(&ag->lock);
    if (!sm_ag_can_alloc_locked(ag, want)) {
        pthread_mutex_unlock(&ag->lock);
        return 1;
    }
    rc = sm_ag_alloc_locked(ag, want, &offset);
    sm_abort_if(rc != 0, "AG %u/%u: can_alloc/alloc mismatch (want=%lu)",
                ag->device_id, ag->ag_index, (unsigned long) want);
    ag->ckpt_dirty = 1;     /* un-checkpointed delta now exists for this AG */
    pthread_mutex_unlock(&ag->lock);

    /* Record the alloc delta into the txn so it rides the redo record (durable +
     * replayed on crash).  Purely in-memory -- no AG-log block claim, so the
     * allocator hot path never parks. */
    if (jnl && jnl->record_delta) {
        jnl->record_delta(jnl->user, ag->device_id, ag->ag_index,
                          offset, want, SM_AG_LOG_OP_ALLOC);
    }
    *r_device_id     = ag->device_id;
    *r_device_offset = offset;
    return 0;
} /* sm_try_alloc_from_ag */

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
    uint64_t                *r_device_offset,
    struct sm_thread_cache  *cache,
    uint32_t                 pick_seed)
{
    uint32_t start_dev, dev_id, d, a;
    uint32_t want_class = sm_ag_size_class(want);
    int      r;

    /* Phase 0 arena fast path: try this thread's sticky AG first.  Each worker
    * sticks to a different AG, so its ag->lock is essentially uncontended --
    * eliminating the thundering herd where many workers gang on one hot AG. */
    if (cache && cache->arena_valid &&
        cache->arena_dev < sm->num_devices &&
        sm->devices[cache->arena_dev].role == role &&
        cache->arena_ag < sm->devices[cache->arena_dev].num_ags) {
        r = sm_try_alloc_from_ag(&sm->devices[cache->arena_dev].ags[cache->arena_ag],
                                 jnl, want, r_device_id, r_device_offset);
        if (r == 0) {
            return 0;
        }
        /* r != 0: arena AG can't satisfy `want` -- re-pick + re-home below.  (The
         * allocator no longer parks, so sm_try_alloc_from_ag never returns
         * SM_AGAIN; r is success(0) or can't-satisfy.  Note SM_AGAIN == 1 aliases
         * the can't-satisfy return, so it must NOT be treated as a park here.) */
    }

    pthread_mutex_lock(&sm->lock);
    start_dev = sm->device_rotor;
    if (++sm->device_rotor >= sm->num_devices) {
        sm->device_rotor = 0;
    }
    pthread_mutex_unlock(&sm->lock);

    for (d = 0; d < sm->num_devices; d++) {
        uint32_t          C, w, start_w, start_ag;
        dev_id = (start_dev + d) % sm->num_devices;
        struct sm_device *dev = &sm->devices[dev_id];

        /* Block mode keeps metadata on LOCAL devices and data on REMOTE
         * devices; a role-matched allocation skips the other class. */
        if (dev->role != role) {
            continue;
        }

        /* Bug 1: fast path -- jump straight to an AG with an adequate free
         * extent via the device's lock-free free-space index, instead of
         * walking all ~30K AGs (and, before Bug 2, dirtying each one's log).
         * mc_class_count[C] names the populated max-free size classes; for
         * each class C >= want_class (lowest first, ~best fit) we walk
         * maxclass_bits[C] to a candidate AG.  C > want_class is a guaranteed
         * fit; C == want_class may not fit `want` exactly, so the per-AG
         * helper re-verifies under the lock.  The count and bitmap are
         * maintained with relaxed atomics (no shared mutex), so updating the
         * index never serializes allocators. */
        /* pick_seed diverges each worker's scan start so they spread across AGs
         * instead of converging on the index's globally "best" AG and convoying
         * on its ag->lock.  The arena (metadata) path derives it from the thread
         * cache pointer; the data path passes its own per-thread seed (cache is
         * NULL there to skip arena stickiness, but it still needs divergence). */
        start_w = dev->mc_words
                  ? ((pick_seed + dev->mc_rotor++) % dev->mc_words)
                  : 0;
        for (C = want_class; C < SM_SIZE_CLASSES; C++) {
            uint64_t *bits;

            if (__atomic_load_n(&dev->mc_class_count[C], __ATOMIC_RELAXED) == 0) {
                continue;       /* no AG's largest free extent is in class C */
            }
            bits = dev->maxclass_bits[C];
            for (w = 0; w < dev->mc_words; w++) {
                uint32_t wi   = (start_w + w) % dev->mc_words;
                uint64_t word = __atomic_load_n(&bits[wi], __ATOMIC_RELAXED);

                while (word) {
                    uint32_t b  = (uint32_t) __builtin_ctzll(word);
                    uint32_t ai = wi * 64u + b;

                    word &= ~(1ULL << b);
                    if (ai >= dev->num_ags) {
                        continue;
                    }
                    r = sm_try_alloc_from_ag(&dev->ags[ai], jnl, want,
                                             r_device_id, r_device_offset);
                    if (r == 0) {
                        if (cache) {
                            cache->arena_dev   = dev_id;
                            cache->arena_ag    = ai;
                            cache->arena_valid = 1;
                        }
                        return 0;
                    }
                    /* r != 0: stale index bit, or a class-want_class extent too
                     * small for `want` -- try the next candidate.  (Never a park:
                     * the allocator does not park; SM_AGAIN==1 aliases this
                     * can't-satisfy return and must not short-circuit here.) */
                }
            }
        }

        /* Backstop: a transiently stale/missing index bit must never produce a
         * false ENOSPC, so before giving up on this device fall back to the
         * full linear scan.  With a consistent index this finds nothing the
         * fast path missed; it exists purely as a correctness guarantee. */
        pthread_mutex_lock(&sm->lock);
        start_ag = dev->ag_rotor;
        if (++dev->ag_rotor >= dev->num_ags) {
            dev->ag_rotor = 0;
        }
        pthread_mutex_unlock(&sm->lock);

        for (a = 0; a < dev->num_ags; a++) {
            uint32_t ai = (start_ag + a) % dev->num_ags;

            r = sm_try_alloc_from_ag(&dev->ags[ai], jnl, want,
                                     r_device_id, r_device_offset);
            if (r == 0) {
                if (cache) {
                    cache->arena_dev   = dev_id;
                    cache->arena_ag    = ai;
                    cache->arena_valid = 1;
                }
                return 0;
            }
            /* r != 0: this AG can't satisfy `want` -- try the next.  (Not a park;
             * SM_AGAIN==1 aliases the can't-satisfy return.) */
        }
    }

    return -1;
} /* sm_pick_and_alloc */

/*
 * Volatile reservation: remove a range from the live in-memory free tree so
 * concurrent allocators cannot reuse it, but do not journal it.  Callers must
 * journal exact sub-allocations before exposing them durably.  Any unused tail
 * can be returned with space_map_thread_cache_discard_volatile(); after a
 * crash it was never removed from the persistent free map.
 */
static int
sm_pick_and_reserve_volatile(
    struct space_map *sm,
    uint32_t          role,
    uint64_t          want,
    uint32_t         *r_device_id,
    uint64_t         *r_device_offset,
    uint32_t          pick_seed)
{
    /*
     * Use the lock-free size-class index (sm_pick_and_alloc) to jump straight to
     * an AG with an adequate free extent, instead of the old linear walk that
     * locked AG after AG -- a major CPU + ag->lock-contention cost on the data
     * write path at scale.  jnl == NULL: sm_try_alloc_from_ag removes the range
     * from the in-memory free tree without journaling (the durable ALLOC delta
     * for the exact sub-range is recorded later by space_map_journal_alloc_exact
     * when the write commits; the unused tail is returned).  cache == NULL: this
     * is the data path, which has no per-thread arena.  pick_seed still gives it
     * per-thread scan divergence so concurrent data writers don't convoy on the
     * same AG's lock (the dominant cost as free extents fragment under INIT).
     */
    return sm_pick_and_alloc(sm, NULL, role, want, r_device_id, r_device_offset,
                             NULL, pick_seed);
} /* sm_pick_and_reserve_volatile */

static int
space_map_journal_alloc_exact(
    struct space_map        *sm,
    const struct sm_journal *jnl,
    uint32_t                 device_id,
    uint64_t                 device_offset,
    uint64_t                 length)
{
    struct sm_ag *ag;

    ag = sm_free_resolve_ag(sm, device_id, device_offset, length,
                            "space_map_journal_alloc_exact");

    pthread_mutex_lock(&ag->lock);
    ag->ckpt_dirty = 1;
    pthread_mutex_unlock(&ag->lock);

    /* The exact range was already removed from the free tree by the volatile
     * reservation; here we only record the alloc delta for the redo record. */
    if (jnl && jnl->record_delta) {
        jnl->record_delta(jnl->user, device_id, ag->ag_index,
                          device_offset, length, SM_AG_LOG_OP_ALLOC);
    }
    return 0;
} /* space_map_journal_alloc_exact */

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

    rc = sm_pick_and_alloc(sm, jnl, role, want, &cache->device_id, &cache->offset, cache,
                           (uint32_t) ((uintptr_t) cache >> 6));
    if (rc == SM_AGAIN) {
        return SM_AGAIN;
    }
    if (rc != 0) {
        /* Try the smaller exact size before giving up, in case fragmentation
         * blocks the reservation but the caller's actual ask is small. */
        if (want != need) {
            rc = sm_pick_and_alloc(sm, jnl, role, need, &cache->device_id, &cache->offset, cache,
                                   (uint32_t) ((uintptr_t) cache >> 6));
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

int
space_map_alloc_volatile_reservation(
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
    uint64_t want;
    int      rc;

    sm_abort_if(need == 0, "alloc of zero bytes");

    if (!cache->valid || cache->length < need) {
        space_map_thread_cache_discard_volatile(sm, cache);

        want = need > floor ? need : floor;
        if (want > SM_AG_SIZE) {
            return -1;
        }

        rc = sm_pick_and_reserve_volatile(sm, role, want,
                                          &cache->device_id, &cache->offset,
                                          (uint32_t) ((uintptr_t) cache >> 6));
        if (rc != 0 && want != need) {
            rc = sm_pick_and_reserve_volatile(sm, role, need,
                                              &cache->device_id, &cache->offset,
                                              (uint32_t) ((uintptr_t) cache >> 6));
            if (rc == 0) {
                cache->length = need;
                cache->valid  = 1;
            }
        } else if (rc == 0) {
            cache->length = want;
            cache->valid  = 1;
        }
        if (rc != 0) {
            return rc;
        }
    }

    rc = space_map_journal_alloc_exact(sm, jnl, cache->device_id,
                                       cache->offset, need);
    if (rc == SM_AGAIN) {
        return SM_AGAIN;
    }

    *r_device_id     = cache->device_id;
    *r_device_offset = cache->offset;
    cache->offset   += need;
    cache->length   -= need;
    if (cache->length == 0) {
        cache->valid = 0;
    }
    return 0;
} /* space_map_alloc_volatile_reservation */

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
    uint64_t      offset, aligned;
    struct sm_ag *ag;

    if (!sm_free_block_range(device_offset, length, &offset, &aligned)) {
        return 0;
    }
    ag = sm_free_resolve_ag(sm, device_id, offset, aligned, "space_map_free_journal");

    pthread_mutex_lock(&ag->lock);
    ag->ckpt_dirty = 1;
    pthread_mutex_unlock(&ag->lock);

    /* Record the FREE delta into the txn so it rides the redo record.  Purely
     * in-memory -- no AG-log block claim, never parks.  (The transactional free
     * path records its frees from txn->pending_frees instead; this immediate
     * path is used only for non-transactional returns, where jnl is NULL.) */
    if (jnl && jnl->record_delta) {
        jnl->record_delta(jnl->user, device_id, ag->ag_index,
                          offset, aligned, SM_AG_LOG_OP_FREE);
    }
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

static void
sm_ag_mark_used_locked(
    struct sm_ag *ag,
    uint64_t      offset,
    uint64_t      length);

static void
sm_ag_remove_claim_locked(
    struct sm_ag    *ag,
    struct sm_claim *claim);

/*
 * Apply a COMMITTED allocation to the in-memory free tree at durability (retire),
 * mirroring space_map_free_apply.  The reservation allocator hands out blocks
 * thread-locally from a claim WITHOUT removing them from the tree, so the tree
 * stays == committed state (condense never captures an uncommitted reservation
 * tail -> no crash leak).  This removes the committed sub-range when its txn's
 * redo record retires, single-owner and in seq order, so it never contends with
 * itself and only briefly with a concurrent grab on the same AG.
 */
void
space_map_alloc_apply(
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
    ag = sm_free_resolve_ag(sm, device_id, offset, aligned, "space_map_alloc_apply");

    pthread_mutex_lock(&ag->lock);
    /* The claim kept this range free in the tree until now; remove it.  (mark_used
     * asserts the range is within a free extent, catching any double-apply.) */
    sm_ag_mark_used_locked(ag, offset, aligned);
    ag->ckpt_dirty = 1;

    /* Unpin the claim backing this range; GC it if its owner has moved on
     * (retiring) and this was the last in-flight allocation from it. */
    {
        struct sm_claim *c;

        for (c = ag->claims; c; c = c->next) {
            if (offset >= c->base && offset < c->base + c->len) {
                if (__atomic_sub_fetch(&c->refcount, 1, __ATOMIC_RELAXED) == 0 &&
                    c->retiring) {
                    sm_ag_remove_claim_locked(ag, c);
                }
                break;
            }
        }
    }
    pthread_mutex_unlock(&ag->lock);
} /* space_map_alloc_apply */

/*
 * Discard an uncommitted allocation (txn abort): the range was never removed
 * from the free tree (only retire does that), so the tree needs no change -- but
 * the claim was pinned at bump, so unpin it (and GC if it was retiring and this
 * was its last in-flight allocation).  Mirrors the unpin half of alloc_apply.
 */
void
space_map_alloc_discard(
    struct space_map *sm,
    uint32_t          device_id,
    uint64_t          device_offset,
    uint64_t          length)
{
    uint64_t         offset, aligned;
    struct sm_ag    *ag;
    struct sm_claim *c;

    if (!sm_free_block_range(device_offset, length, &offset, &aligned)) {
        return;
    }
    ag = sm_free_resolve_ag(sm, device_id, offset, aligned, "space_map_alloc_discard");

    pthread_mutex_lock(&ag->lock);
    for (c = ag->claims; c; c = c->next) {
        if (offset >= c->base && offset < c->base + c->len) {
            if (__atomic_sub_fetch(&c->refcount, 1, __ATOMIC_RELAXED) == 0 &&
                c->retiring) {
                sm_ag_remove_claim_locked(ag, c);
            }
            break;
        }
    }
    pthread_mutex_unlock(&ag->lock);
} /* space_map_alloc_discard */

/* --- Reservation claim list (per-AG, protected by ag->lock) --- */

/* Add a claim for [base, base+len) to the AG.  Caller holds ag->lock. */
static struct sm_claim *
sm_ag_add_claim_locked(
    struct sm_ag *ag,
    uint64_t      base,
    uint64_t      len)
{
    struct sm_claim *c = calloc(1, sizeof(*c));

    c->base     = base;
    c->len      = len;
    c->refcount = 0;
    c->retiring = 0;
    c->next     = ag->claims;
    ag->claims  = c;
    return c;
} /* sm_ag_add_claim_locked */

/* Unlink and free a claim from the AG.  Caller holds ag->lock. */
static void
sm_ag_remove_claim_locked(
    struct sm_ag    *ag,
    struct sm_claim *claim)
{
    struct sm_claim **link;

    for (link = &ag->claims; *link; link = &(*link)->next) {
        if (*link == claim) {
            *link = claim->next;
            free(claim);
            return;
        }
    }
} /* sm_ag_remove_claim_locked */

/* Lowest claim base strictly above `from` in this AG (or UINT64_MAX if none),
 * used to cap a new claim so it can't swallow a higher existing claim.  Caller
 * holds ag->lock. */
static uint64_t
sm_ag_next_claim_base_locked(
    struct sm_ag *ag,
    uint64_t      from)
{
    struct sm_claim *c;
    uint64_t         best = UINT64_MAX;

    for (c = ag->claims; c; c = c->next) {
        if (c->base >= from && c->base < best) {
            best = c->base;
        }
    }
    return best;
} /* sm_ag_next_claim_base_locked */

/*
 * Try to carve a claim-free region of at least `want` (ideally up to `chunk`)
 * out of this AG's committed-free tree, without removing it from the tree.  On
 * success records the claim and returns it via *out_claim with [*out_base,
 * *out_base+*out_len).  Caller holds ag->lock.  Returns 0 on success, 1 if this
 * AG has no claim-free extent big enough.
 */
static int
sm_ag_try_claim_locked(
    struct sm_ag     *ag,
    uint64_t          want,
    uint64_t          chunk,
    uint64_t         *out_base,
    uint64_t         *out_len,
    struct sm_claim **out_claim)
{
    uint32_t want_class = sm_ag_size_class(want);
    uint32_t klass;

    /*
     * Find a claim-free window via the by-size index instead of walking
     * free_by_offset.  Every extent that can hold `want` lives in a class
     * >= want_class (sm_ag_size_class is monotonic in length), so we only ever
     * visit candidate extents -- the old first-fit walk that scanned past
     * thousands of sub-`want` fragments under heavy COW churn is gone.  Largest
     * class first: a bigger extent is most likely to yield the full `chunk`
     * grant (which keeps reserve_chunk genuinely "once per chunk consumed"
     * rather than once per fragment) and least likely to be wholly blocked by
     * in-flight claims, so the common case succeeds on the first extent tried.
     */
    for (klass = SM_SIZE_CLASSES; klass-- > want_class; ) {
        struct sm_extent *e;

        if (!(ag->size_nonempty & (1u << klass))) {
            continue;
        }
        for (e = ag->free_by_size[klass]; e; e = e->size_next) {
            uint64_t e_end = e->offset + e->length;
            uint64_t s     = e->offset;

            if (e->length < want) {
                continue;       /* only reachable in the want_class bucket */
            }
            /* Advance `s` past any claim overlapping [s, s+want) until a claim-free
             * window of `want` fits, or we run off the end of this extent. */
            for (;; ) {
                struct sm_claim *blk = NULL;
                struct sm_claim *c;

                if (s + want > e_end) {
                    break;           /* no room left in this extent */
                }
                for (c = ag->claims; c; c = c->next) {
                    if (s < c->base + c->len && c->base < s + want) {
                        if (!blk || c->base + c->len > blk->base + blk->len) {
                            blk = c; /* the overlapping claim that ends highest */
                        }
                    }
                }
                if (!blk) {
                    /* [s, s+want) is claim-free.  Extend the claim up to `chunk`,
                     * capped at the extent end and the next claim above s. */
                    uint64_t cap = e_end;
                    uint64_t nb  = sm_ag_next_claim_base_locked(ag, s);
                    uint64_t len;

                    if (nb < cap) {
                        cap = nb;
                    }
                    len = cap - s;
                    if (len > chunk) {
                        len = chunk;
                    }
                    *out_base  = s;
                    *out_len   = len;
                    *out_claim = sm_ag_add_claim_locked(ag, s, len);
                    return 0;
                }
                s = blk->base + blk->len;   /* jump past the blocking claim */
            }
        }
    }
    return 1;
} /* sm_ag_try_claim_locked */

/*
 * Grab a fresh bump reservation for a worker thread: find a claim-free,
 * committed-free region of at least `want` (up to `chunk`) in a role-matched AG,
 * record a claim for it (NOT removing it from the free tree), and fill `r`.
 * Returns 0 on success, -1 on ENOSPC.  Rare (~once per chunk consumed), so the
 * AG scan cost is amortized away.  `seed` diverges concurrent grabs across AGs.
 */
int
space_map_reserve_chunk(
    struct space_map      *sm,
    struct sm_reservation *r,
    uint32_t               role,
    uint64_t               want,
    uint64_t               chunk,
    uint32_t               seed)
{
    uint32_t start_dev, d;

    want = SM_ALIGN_UP(want);
    if (chunk < want) {
        chunk = want;
    }

    pthread_mutex_lock(&sm->lock);
    start_dev = sm->device_rotor;
    if (++sm->device_rotor >= sm->num_devices) {
        sm->device_rotor = 0;
    }
    pthread_mutex_unlock(&sm->lock);

    for (d = 0; d < sm->num_devices; d++) {
        uint32_t          dev_id     = (start_dev + d) % sm->num_devices;
        struct sm_device *dev        = &sm->devices[dev_id];
        uint32_t          want_class = sm_ag_size_class(want);
        uint32_t          C, a, start_ag;

        if (dev->role != role) {
            continue;
        }
        /* Jump to AGs whose largest free extent can hold `want` via the lock-free
         * size-class index, seeded per-thread so concurrent grabs diverge. */
        for (C = want_class; C < SM_SIZE_CLASSES; C++) {
            uint64_t *bits;
            uint32_t  w, start_w;

            if (!dev->mc_words ||
                __atomic_load_n(&dev->mc_class_count[C], __ATOMIC_RELAXED) == 0) {
                continue;
            }
            bits    = dev->maxclass_bits[C];
            start_w = (seed + dev->mc_rotor++) % dev->mc_words;
            for (w = 0; w < dev->mc_words; w++) {
                uint32_t wi   = (start_w + w) % dev->mc_words;
                uint64_t word = __atomic_load_n(&bits[wi], __ATOMIC_RELAXED);

                while (word) {
                    uint32_t         b  = (uint32_t) __builtin_ctzll(word);
                    uint32_t         ai = wi * 64u + b;
                    struct sm_ag    *ag;
                    uint64_t         base, len;
                    struct sm_claim *claim;

                    word &= ~(1ULL << b);
                    if (ai >= dev->num_ags) {
                        continue;
                    }
                    ag = &dev->ags[ai];
                    pthread_mutex_lock(&ag->lock);
                    if (sm_ag_try_claim_locked(ag, want, chunk, &base, &len,
                                               &claim) == 0) {
                        pthread_mutex_unlock(&ag->lock);
                        r->device_id = dev_id;
                        r->ag_index  = ai;
                        r->base      = base;
                        r->len       = len;
                        r->cursor    = base;
                        r->claim     = claim;
                        r->valid     = 1;
                        return 0;
                    }
                    pthread_mutex_unlock(&ag->lock);
                }
            }
        }

        /* Backstop: the (count, bitmap) size-class index is read with two
         * independent relaxed loads above, so it is not a consistent snapshot --
         * a concurrent sm_ag_mc_update on a shared word can leave a reader seeing
         * mc_class_count[C] > 0 while maxclass_bits[C] reads 0 (the bit was
         * cleared by one max-class transition before the re-set of the next).
         * The fast path would then visit no AG for class C and fall through to a
         * false ENOSPC even though space is available.  Before giving up on this
         * device, fall back to a full linear scan that re-verifies each AG under
         * ag->lock -- authoritative, and (like sm_pick_and_alloc's backstop) a
         * pure correctness guarantee that finds nothing the fast path missed once
         * the index reconciles. */
        pthread_mutex_lock(&sm->lock);
        start_ag = dev->ag_rotor;
        if (++dev->ag_rotor >= dev->num_ags) {
            dev->ag_rotor = 0;
        }
        pthread_mutex_unlock(&sm->lock);

        for (a = 0; a < dev->num_ags; a++) {
            uint32_t         ai = (start_ag + a) % dev->num_ags;
            struct sm_ag    *ag = &dev->ags[ai];
            uint64_t         base, len;
            struct sm_claim *claim;

            pthread_mutex_lock(&ag->lock);
            if (sm_ag_try_claim_locked(ag, want, chunk, &base, &len,
                                       &claim) == 0) {
                pthread_mutex_unlock(&ag->lock);
                r->device_id = dev_id;
                r->ag_index  = ai;
                r->base      = base;
                r->len       = len;
                r->cursor    = base;
                r->claim     = claim;
                r->valid     = 1;
                return 0;
            }
            pthread_mutex_unlock(&ag->lock);
        }
    }
    return -1;      /* ENOSPC */
} /* space_map_reserve_chunk */

/*
 * Hand out `need` bytes from a thread's bump reservation, thread-local: no lock,
 * no per-block allocator call.  Records the ALLOC delta (rides the txn redo and
 * is applied to the free tree at retire via space_map_alloc_apply).  Returns 0
 * on success; 1 if the reservation can't satisfy `need` (caller must refill via
 * space_map_reserve_chunk and retry).
 */
int
space_map_bump_alloc(
    struct sm_reservation   *r,
    const struct sm_journal *jnl,
    uint64_t                 need,
    uint32_t                *r_device_id,
    uint64_t                *r_device_offset)
{
    need = SM_ALIGN_UP(need);

    if (!r->valid || r->cursor + need > r->base + r->len) {
        return 1;       /* exhausted -- caller refills */
    }
    *r_device_id     = r->device_id;
    *r_device_offset = r->cursor;

    /* Pin the claim: it must outlive this allocation's retire (when
    * space_map_alloc_apply decrements), so a re-grant of the region can't race
    * an in-flight ALLOC delta.  Owner-only increment, sequenced-before any
    * release of this claim, so it never races the retiring flag. */
    __atomic_fetch_add(&r->claim->refcount, 1, __ATOMIC_RELAXED);

    /* ALLOC delta for crash recovery; applied to the in-memory tree only when
     * this txn's redo retires (space_map_alloc_apply), never eagerly -- so the
     * tree stays == committed state and condense can't leak the tail. */
    if (jnl && jnl->record_delta) {
        jnl->record_delta(jnl->user, r->device_id, r->ag_index,
                          r->cursor, need, SM_AG_LOG_OP_ALLOC);
    }
    r->cursor += need;
    return 0;
} /* space_map_bump_alloc */

/*
 * Release a thread's reservation: mark its claim `retiring` so it is GC'd once
 * its in-flight allocations have all retired (refcount==0); if no in-flight
 * allocations remain, drop it now.  The uncommitted tail [cursor, base+len) was
 * never removed from the free tree, so nothing is returned -- it is already
 * free.  Caller passes the reservation; safe to call on an invalid one.
 */
void
space_map_release_reservation(
    struct space_map      *sm,
    struct sm_reservation *r)
{
    struct sm_ag *ag;

    if (!r->valid || !r->claim) {
        r->valid = 0;
        r->claim = NULL;
        return;
    }
    ag = &sm->devices[r->device_id].ags[r->ag_index];

    pthread_mutex_lock(&ag->lock);
    if (r->claim->refcount == 0) {
        sm_ag_remove_claim_locked(ag, r->claim);
    } else {
        r->claim->retiring = 1;
    }
    pthread_mutex_unlock(&ag->lock);

    r->valid = 0;
    r->claim = NULL;
} /* space_map_release_reservation */

/*
 * Ensure `r` can satisfy a `want`-byte bump without refilling: if not, release
 * the (exhausted-or-too-small) current claim and grab a fresh chunk of at least
 * `want`, up to `chunk`.  Lets a caller (e.g. the b+tree RESERVE phase) guarantee
 * a subsequent run of bump_allocs draws purely thread-locally.  Returns 0, or -1
 * on ENOSPC.  Never journals, never parks.
 */
int
space_map_reservation_ensure(
    struct space_map      *sm,
    struct sm_reservation *r,
    uint32_t               role,
    uint64_t               want,
    uint64_t               chunk,
    uint32_t               seed)
{
    want = SM_ALIGN_UP(want);

    if (r->valid && r->cursor + want <= r->base + r->len) {
        return 0;
    }
    space_map_release_reservation(sm, r);       /* retire the old claim (if any) */
    return space_map_reserve_chunk(sm, r, role, want, chunk, seed);
} /* space_map_reservation_ensure */

/*
 * Allocate `need` bytes from `r`, refilling once if exhausted.  The one-stop
 * call for sites that don't pre-reserve (e.g. file data).  Returns 0, or -1 on
 * ENOSPC.
 */
int
space_map_reservation_alloc(
    struct space_map        *sm,
    struct sm_reservation   *r,
    const struct sm_journal *jnl,
    uint32_t                 role,
    uint64_t                 need,
    uint64_t                 chunk,
    uint32_t                 seed,
    uint32_t                *r_device_id,
    uint64_t                *r_device_offset)
{
    if (space_map_bump_alloc(r, jnl, need, r_device_id, r_device_offset) == 0) {
        return 0;
    }
    /* Exhausted: retire the old claim, grab a fresh chunk, retry once. */
    if (space_map_reservation_ensure(sm, r, role, need, chunk, seed) != 0) {
        return -1;
    }
    if (space_map_bump_alloc(r, jnl, need, r_device_id, r_device_offset) != 0) {
        return -1;      /* a freshly-grabbed chunk >= need must satisfy */
    }
    return 0;
} /* space_map_reservation_alloc */

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
space_map_thread_cache_discard_volatile(
    struct space_map       *sm,
    struct sm_thread_cache *cache)
{
    if (!cache->valid || cache->length == 0) {
        cache->valid = 0;
        return;
    }

    space_map_free_apply(sm, cache->device_id, cache->offset, cache->length);
    cache->valid  = 0;
    cache->length = 0;
} /* space_map_thread_cache_discard_volatile */

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
    sb->intent_log_size    = sm->intent_log_size;
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
    sm_ag_size_unlink(ag, e);

    if (offset > e_off) {
        struct sm_extent *l = sm_extent_new(e_off, offset - e_off);
        rb_tree_insert(&ag->free_by_offset, offset, l);
        sm_ag_size_link(ag, l);
    }
    if (offset + length < e_off + e_len) {
        struct sm_extent *r = sm_extent_new(offset + length,
                                            (e_off + e_len) - (offset + length));
        rb_tree_insert(&ag->free_by_offset, offset, r);
        sm_ag_size_link(ag, r);
    }
    ag->free_bytes -= length;
    __atomic_sub_fetch(&ag->dev->free_bytes, length, __ATOMIC_RELAXED);
    free(e);
} /* sm_ag_mark_used_locked */

/* Is [offset, offset+length) entirely within a single free extent?  Caller
 * holds ag->lock.  Used to make redo-delta recovery idempotent. */
static int
sm_ag_range_is_free_locked(
    struct sm_ag *ag,
    uint64_t      offset,
    uint64_t      length)
{
    struct sm_extent *e = NULL;

    rb_tree_query_floor(&ag->free_by_offset, offset, offset, e);
    return e && e->offset <= offset &&
           e->offset + e->length >= offset + length;
} /* sm_ag_range_is_free_locked */

/*
 * Apply one redo-replayed space delta during crash recovery, idempotently.  The
 * AG's checkpoint base may already reflect this delta: an alloc is applied to
 * the free tree eagerly, so a base snapshot can capture an alloc whose seq is
 * above the stamped ckpt_seq (the conservatively-low durable frontier).  So an
 * ALLOC whose range is already used is a no-op (a committed txn re-applying its
 * own eager alloc), and a FREE whose range is already free is a no-op.  Caller
 * is the single-threaded mount-time recovery path.
 */
void
space_map_recover_delta(
    struct space_map *sm,
    uint32_t          device_id,
    uint32_t          ag_index,
    uint64_t          offset,
    uint64_t          length,
    uint32_t          op)
{
    struct sm_ag *ag;

    if (device_id >= sm->num_devices ||
        ag_index >= sm->devices[device_id].num_ags || length == 0) {
        return;     /* corrupt delta -- ignore */
    }
    ag = &sm->devices[device_id].ags[ag_index];

    pthread_mutex_lock(&ag->lock);
    if (op == SM_AG_LOG_OP_ALLOC) {
        if (sm_ag_range_is_free_locked(ag, offset, length)) {
            sm_ag_mark_used_locked(ag, offset, length);
        }
    } else {
        if (!sm_ag_range_is_free_locked(ag, offset, length)) {
            sm_ag_free_locked(ag, offset, length);
        }
    }
    sm_ag_mc_update(ag);
    pthread_mutex_unlock(&ag->lock);
} /* space_map_recover_delta */

static uint64_t sm_ag_condense_into(
    struct sm_ag *ag,
    uint8_t      *slot,
    uint64_t      generation,
    uint64_t      ckpt_seq);

/*
 * Checkpoint, phase 1: snapshot the AG's free set as a condensed base image
 * (header at buf[0]) for the INACTIVE slot, stamped with ckpt_seq.  Kicked by
 * the push thread (diskfs_checkpoint_kick) when the redo trim frontier is
 * blocked on this AG; the allocator never parks on it.  Allocs continue
 * concurrently under ag->lock -- the snapshot is a consistent point: allocs are
 * reflected eagerly (an alloc above ckpt_seq merely replays idempotently on
 * recovery, or leaks if its txn never commits) and frees only once durable (so
 * the base never frees a block a committed file still references; ckpt_seq is
 * sampled from the durable frontier, so every delta <= ckpt_seq is reflected).
 * buf must hold SM_AG_LOG_SLOT_SIZE bytes.
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
    uint32_t         *r_log_device_id,
    uint64_t         *r_payload,
    uint64_t          ckpt_seq)
{
    struct sm_ag *ag = &sm->devices[device_id].ags[ag_index];
    uint64_t      payload;

    pthread_mutex_lock(&ag->lock);
    sm_abort_if(!ag->condensing, "condense_prepare on a non-condensing AG");

    payload = sm_ag_condense_into(ag, (uint8_t *) buf, ag->log_generation + 1,
                                  ckpt_seq);

    *r_slot_offset = ag->log_offset +
        (uint64_t) (1 - ag->log_slot) * SM_AG_LOG_SLOT_SIZE;
    /* The slot lives on the AG's log device, which for a relocated remote AG is
     * device 0, NOT the AG's (queueless) remote data device. */
    *r_log_device_id = ag->log_device_id;
    *r_payload       = payload;
    pthread_mutex_unlock(&ag->lock);
    return 0;
} /* space_map_condense_prepare */

/*
 * Runtime checkpoint, phase 2: the new base image is durable in the inactive
 * slot (header written last) -- flip to it, publish ckpt_seq (the redo trim
 * frontier reads it), and clear the in-progress gate.
 */
void
space_map_condense_commit(
    struct space_map *sm,
    uint32_t          device_id,
    uint32_t          ag_index,
    uint32_t          base_count,
    uint64_t          ckpt_seq)
{
    struct sm_ag *ag = &sm->devices[device_id].ags[ag_index];

    pthread_mutex_lock(&ag->lock);
    sm_abort_if(!ag->condensing, "condense_commit on a non-condensing AG");
    ag->log_slot        = 1 - ag->log_slot;
    ag->log_generation += 1;
    ag->log_base_count  = base_count;
    ag->log_delta_count = 0;
    ag->condensing      = 0;
    /* Publish the new checkpoint frontier for this AG.  A concurrent alloc/free
     * may have re-dirtied it (delta with seq > ckpt_seq) -- leave ckpt_dirty as
     * it stands in that case; only clear it if no newer delta arrived. */
    __atomic_store_n(&ag->ckpt_seq, ckpt_seq, __ATOMIC_RELEASE);
    pthread_mutex_unlock(&ag->lock);
} /* space_map_condense_commit */

/* Read an AG's published checkpoint frontier (the highest redo seq folded into
 * its on-disk snapshot).  Used by the push-thread trim gate. */
uint64_t
space_map_ag_ckpt_seq(
    struct space_map *sm,
    uint32_t          device_id,
    uint32_t          ag_index)
{
    struct sm_ag *ag = &sm->devices[device_id].ags[ag_index];

    return __atomic_load_n(&ag->ckpt_seq, __ATOMIC_ACQUIRE);
} /* space_map_ag_ckpt_seq */

/*
 * Claim the right to start a background checkpoint of this AG.  Returns 1 if the
 * caller (the push thread) should submit the checkpoint job, 0 if a checkpoint
 * is already in progress (its commit will advance ckpt_seq).  Sets the
 * in-progress gate that space_map_condense_prepare/commit assert on.
 */
int
space_map_ag_begin_checkpoint(
    struct space_map *sm,
    uint32_t          device_id,
    uint32_t          ag_index)
{
    struct sm_ag *ag    = &sm->devices[device_id].ags[ag_index];
    int           start = 0;

    pthread_mutex_lock(&ag->lock);
    if (!ag->condensing) {
        ag->condensing = 1;
        start          = 1;
    }
    pthread_mutex_unlock(&ag->lock);
    return start;
} /* space_map_ag_begin_checkpoint */

/* Serialize the AG's current free set into `slot` as a condensed base (no
 * deltas) at `generation`, stamped with `ckpt_seq` (the highest redo seq folded
 * into this snapshot).  Caller holds ag->lock.  Returns bytes written. */
static uint64_t
sm_ag_condense_into(
    struct sm_ag *ag,
    uint8_t      *slot,
    uint64_t      generation,
    uint64_t      ckpt_seq)
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
    h->ckpt_seq    = ckpt_seq;
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
    memset(ag->free_by_size, 0, sizeof(ag->free_by_size));
    ag->size_nonempty = 0;
    ag->free_bytes    = 0;

    for (i = 0; i < h->base_count; i++) {
        struct sm_extent *e = sm_extent_new(base[i].offset, base[i].length);
        rb_tree_insert(&ag->free_by_offset, offset, e);
        sm_ag_size_link(ag, e);
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
    ag->ckpt_seq        = h->ckpt_seq;
    ag->ckpt_dirty      = 0;

    /* Reconcile the lock-free index in case this AG reconstructed to empty
     * (no size_link/unlink ran above, so a pre-reconstruct max-class bit could
     * otherwise be left stale). */
    sm_ag_mc_update(ag);
} /* sm_ag_reconstruct */

struct sm_persist_update {
    struct sm_ag *ag;
    uint32_t      slot;
    uint64_t      generation;
    uint32_t      base_count;
    uint64_t      ckpt_seq;
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
            __atomic_store_n(&updates[i].ag->ckpt_seq, updates[i].ckpt_seq,
                             __ATOMIC_RELEASE);
            updates[i].ag->ckpt_dirty = 0;
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
    const struct sm_io *io,
    uint64_t            ckpt_seq)
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
            payload  = sm_ag_condense_into(ag, buf, gen, ckpt_seq);
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
                payload  = sm_ag_condense_into(ag, buf, gen, ckpt_seq);
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
            updates[count].ckpt_seq = ckpt_seq;
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
    uint32_t d;

    /* O(num_devices): sum the per-device running totals (maintained under the
     * AG lock on every alloc/free).  Space held in a thread's reservation cache
     * (carved out of an AG but not yet handed to a file) reads as not-free here,
     * so the report is conservative -- never an overestimate of what can still
     * be allocated.  This replaces an O(all-AGs) scan that locked every AG and
     * dominated CPU on the reservation hot path. */
    for (d = 0; d < sm->num_devices; d++) {
        free += __atomic_load_n(&sm->devices[d].free_bytes, __ATOMIC_RELAXED);
    }
    return free;
} /* space_map_free_bytes */

/* Seed each device's running free_bytes total from its AGs.  Called once after
 * format and after load; the alloc/free path maintains it incrementally after
 * that. */
static void
sm_init_device_free_totals(struct space_map *sm)
{
    uint32_t d, a;

    for (d = 0; d < sm->num_devices; d++) {
        struct sm_device *dev   = &sm->devices[d];
        uint64_t          total = 0;

        for (a = 0; a < dev->num_ags; a++) {
            total += dev->ags[a].free_bytes;
        }
        __atomic_store_n(&dev->free_bytes, total, __ATOMIC_RELAXED);
    }
} /* sm_init_device_free_totals */

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

    sm_init_device_free_totals(sm);
    return 0;
} /* space_map_load */
