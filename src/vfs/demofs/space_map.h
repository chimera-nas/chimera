// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <pthread.h>

#include "common/rbtree.h"

/*
 * Space map allocator for demofs.
 *
 * Each block device is divided into fixed-size allocation groups (AGs).
 * Each AG owns an in-memory red-black tree of free extents keyed by
 * device offset, protected by its own mutex.  Allocations come from a
 * per-thread reservation cache that is refilled in chunks from an AG,
 * so the per-allocation common case is lock-free.
 *
 * All state is in memory.  Mutating operations call
 * sm_journal_record_alloc / sm_journal_record_free stubs that the future
 * intent log will replace; this is where on-disk durability hooks in.
 */

#define SM_BLOCK_SHIFT       12
#define SM_BLOCK_SIZE        (1ULL << SM_BLOCK_SHIFT)
#define SM_BLOCK_MASK        (SM_BLOCK_SIZE - 1)

#define SM_AG_SIZE_LOG2      30                             /* 1 GiB */
#define SM_AG_SIZE           (1ULL << SM_AG_SIZE_LOG2)
#define SM_AG_OFFSET_MASK    (SM_AG_SIZE - 1)

#define SM_SUPERBLOCK_OFFSET 0
#define SM_SUPERBLOCK_SIZE   4096
#define SM_SUPERBLOCK_MAGIC  0x4D5346534B534944ULL          /* "DISKSFSM" */
#define SM_FORMAT_VERSION    1

#define SM_RESERVATION_MIN   (1ULL << 20)                   /* 1 MiB */

#define SM_ALIGN_UP(x) (((x) + SM_BLOCK_MASK) & ~SM_BLOCK_MASK)

/*
 * Inode-number encoding.  A 64-bit inum is a (disk, ag, block_idx) tuple
 * that locates a 4 KiB inode block on storage with no further indirection.
 *
 *     bits 63..56  disk_id    (256 disks max)
 *     bits 55..32  ag_index   (16 M AGs / disk)
 *     bits 31.. 0  block_idx  (1-based offset into the AG's data region)
 *
 * block_idx == 0 (i.e. the entire inum == 0) is reserved as "invalid".
 * The first usable inode in any AG is at block_idx == 1; for AG 0 of disk
 * 0 we reserve block_idx == 1 at format time so the very first allocation
 * lands at block_idx == 2 (= inum 2 = the root inode).
 */
#define SM_INUM_INDEX_BITS   32
#define SM_INUM_AG_BITS      24
#define SM_INUM_DISK_BITS    8

#define SM_INUM_INDEX_MASK   ((1ULL << SM_INUM_INDEX_BITS) - 1)
#define SM_INUM_AG_MASK      ((1ULL << SM_INUM_AG_BITS)    - 1)
#define SM_INUM_DISK_MASK    ((1ULL << SM_INUM_DISK_BITS)  - 1)

#define SM_INUM_AG_SHIFT     SM_INUM_INDEX_BITS
#define SM_INUM_DISK_SHIFT   (SM_INUM_INDEX_BITS + SM_INUM_AG_BITS)

/*
 * Each AG carves a fixed prefix off the front of its data range for its
 * own space-map log.  The log is double-buffered (slot A + slot B) so
 * condensation can ping-pong atomically.  Sized for the worst-case
 * fragmentation of a 1 GiB AG with 4 KiB blocks (~2 MiB per snapshot) with
 * comfortable headroom.
 */
#define SM_AG_LOG_SLOT_SIZE  (4ULL << 20)  /* 4 MiB */
#define SM_AG_LOG_SLOT_COUNT 2
#define SM_AG_LOG_SIZE       (SM_AG_LOG_SLOT_SIZE * SM_AG_LOG_SLOT_COUNT)

/*
 * On-disk per-AG allocation log.  Each slot begins with this header, followed
 * by `base_count` condensed free-extent records (the free set at the last
 * condensation) and then `delta_count` allocation/free delta records appended
 * since.  The free set is reconstructed at mount as base + replayed deltas.
 * `generation` selects the live slot (highest valid wins); condensation
 * rewrites the *other* slot at generation+1 and switches.  Integrity of the
 * slot's 4 KiB blocks is provided by the main intent log (every block that a
 * delta or condensation touches rides a redo record), so the header carries
 * no separate checksum -- a torn condensation simply leaves the older slot
 * live.
 */
#define SM_AG_LOG_MAGIC      0x474F4C47414B5344ULL     /* "DSKAGLOG" */

#define SM_AG_LOG_OP_ALLOC   0u /* [offset,len) was handed out: remove from free */
#define SM_AG_LOG_OP_FREE    1u /* [offset,len) was returned:   add to free */

struct sm_ag_log_header {
    uint64_t magic;
    uint64_t generation;
    uint32_t base_count;
    uint32_t delta_count;
    uint64_t reserved;
};

struct sm_ag_log_ext {          /* condensed base free extent */
    uint64_t offset;
    uint64_t length;
};

/* 32 bytes so 128 deltas tile a 4 KiB block exactly (no block-spanning). */
struct sm_ag_log_delta {
    uint64_t offset;
    uint64_t length;
    uint32_t op;
    uint32_t pad;
    uint64_t pad2;
};

#define SM_AG_LOG_DELTAS_PER_BLOCK (SM_BLOCK_SIZE / sizeof(struct sm_ag_log_delta))

/*
 * Journal bridge: space_map calls back into demofs to fetch the 4 KiB AG-log
 * block at (device_id, device_offset), pinned into the current transaction so
 * the delta it writes rides the main redo log.  is_new starts from a zeroed
 * block (first use of a delta block) rather than reading disk.  A NULL journal
 * (e.g. format-time or bootstrap allocations) skips delta logging.
 */
struct sm_journal {
    void *(*claim_block)(
        void    *user,
        uint32_t device_id,
        uint64_t device_offset,
        int      is_new);
    void *user;
};

/*
 * Statically-reserved intent log region.  Lives on device 0 right after
 * the superblock; its exact location is also recorded in the superblock
 * so future format versions can move it.  Carved out of AG 0 of device 0.
 */
#define SM_INTENT_LOG_DEVICE 0
#define SM_INTENT_LOG_OFFSET SM_SUPERBLOCK_SIZE
#define SM_INTENT_LOG_SIZE   (64ULL << 20) /* 64 MiB */

/* superblock flags */
#define SM_SB_CLEAN          0x1ULL        /* set at clean unmount, cleared at mount */

struct sm_superblock {
    uint64_t magic;
    uint32_t version;
    uint32_t block_size;
    uint64_t ag_size;
    uint64_t ag_log_size;
    uint64_t fsid;
    uint32_t num_devices;
    uint32_t intent_log_device;
    uint64_t intent_log_offset;
    uint64_t intent_log_size;
    uint32_t crc32;
    /* Mount/recovery state (covered by crc32, which is computed over the
     * whole 4 KiB block with the crc field zeroed). */
    uint64_t flags;
    uint64_t root_inum;
    uint32_t root_gen;
    uint64_t log_seq;           /* next redo seq at clean unmount (for recovery) */
    /* Remainder of the 4 KiB block is implicit zero padding. */
};

/*
 * IEEE CRC-32 (bitwise; used for the superblock and, later, intent-log redo
 * records).  Adequate for these low-frequency / one-record-at-a-time paths.
 */
static inline uint32_t
sm_crc32(
    const void *data,
    size_t      len)
{
    const uint8_t *p   = data;
    uint32_t       crc = 0xFFFFFFFFu;
    size_t         i;
    int            k;

    for (i = 0; i < len; i++) {
        crc ^= p[i];
        for (k = 0; k < 8; k++) {
            crc = (crc >> 1) ^ (0xEDB88320u & (uint32_t) (-(int32_t) (crc & 1)));
        }
    }
    return ~crc;
} /* sm_crc32 */

struct sm_extent {
    struct rb_node node;
    uint64_t       offset;
    uint64_t       length;
};

struct sm_ag {
    uint32_t        device_id;
    uint32_t        ag_index;
    uint64_t        base_offset;
    uint64_t        size;
    uint64_t        log_offset;      /* absolute device offset of this AG's log */
    uint64_t        log_size;        /* total log bytes (both slots) */
    uint64_t        free_bytes;
    struct rb_tree  free_by_offset;
    pthread_mutex_t lock;

    /* On-disk allocation-log state (protected by lock). */
    uint32_t        log_slot;        /* active slot index (0 or 1) */
    uint64_t        log_generation;  /* generation of the active slot */
    uint32_t        log_base_count;  /* condensed base extents in the active slot */
    uint32_t        log_delta_count; /* deltas appended since the last condense */
};

struct sm_device {
    uint32_t      device_id;
    uint64_t      size;
    uint32_t      num_ags;
    uint32_t      ag_rotor;
    struct sm_ag *ags;
};

struct space_map {
    struct sm_device *devices;
    uint32_t          num_devices;
    uint32_t          device_rotor;
    uint64_t          total_capacity;
    uint64_t          used_bytes;     /* atomic accounting for statfs */
    pthread_mutex_t   lock;           /* protects rotors and journaled writes */
};

struct sm_thread_cache {
    uint32_t device_id;
    uint64_t offset;
    uint64_t length;
    int      valid;
};

struct space_map *
space_map_create(
    const uint64_t *device_sizes,
    uint32_t        num_devices);

void
space_map_destroy(
    struct space_map *sm);

int
space_map_alloc(
    struct space_map        *sm,
    struct sm_thread_cache  *cache,
    const struct sm_journal *jnl,
    uint64_t                 size,
    uint32_t                *r_device_id,
    uint64_t                *r_device_offset);

void
space_map_free(
    struct space_map        *sm,
    const struct sm_journal *jnl,
    uint32_t                 device_id,
    uint64_t                 device_offset,
    uint64_t                 length);

/* Pending free: journal the FREE delta now (rides the txn redo), apply the
 * in-memory free only once that txn is durable. */
void
space_map_free_journal(
    struct space_map        *sm,
    const struct sm_journal *jnl,
    uint32_t                 device_id,
    uint64_t                 device_offset,
    uint64_t                 length);

void
space_map_free_apply(
    struct space_map *sm,
    uint32_t          device_id,
    uint64_t          device_offset,
    uint64_t          length);

void
space_map_thread_cache_return(
    struct space_map        *sm,
    const struct sm_journal *jnl,
    struct sm_thread_cache  *cache);

/*
 * Synchronously write a stub superblock to offset 0 of `device_path` by
 * opening the file directly with pwrite + fsync.  Done at format time
 * (before evpl claims the device) so we avoid any reentrancy with the
 * VFS dispatch loop.
 */
int
space_map_write_superblock_path(
    struct space_map *sm,
    const char       *device_path,
    uint64_t          fsid,
    uint64_t          flags,
    uint64_t          root_inum,
    uint32_t          root_gen,
    uint64_t          log_seq);

/* Read + validate the superblock from device 0; 0 on success (fills *out),
 * -1 if absent/corrupt/wrong-version (caller should mkfs). */
int
space_map_read_superblock_path(
    const char           *device_path,
    struct sm_superblock *out);

/*
 * Persist the free-space map to each AG's on-disk log slot (clean unmount),
 * and reload it (mount).  device_paths[d] is the path of device d.  persist
 * returns 0 on success; load returns 0 if every AG's snapshot validated and
 * the in-memory free trees were rebuilt from them, -1 otherwise (caller
 * should treat the filesystem as needing mkfs / recovery).
 */
int
space_map_persist_paths(
    struct space_map *sm,
    char            **device_paths);

int
space_map_load_paths(
    struct space_map *sm,
    char            **device_paths);

static inline uint64_t
space_map_total_capacity(const struct space_map *sm)
{
    return sm->total_capacity;
} // space_map_total_capacity

static inline uint64_t
space_map_used_bytes(const struct space_map *sm)
{
    return __atomic_load_n(&sm->used_bytes, __ATOMIC_RELAXED);
} // space_map_used_bytes

static inline uint64_t
sm_inum_make(
    uint32_t disk,
    uint32_t ag,
    uint32_t idx)
{
    return ((uint64_t) (disk & SM_INUM_DISK_MASK) << SM_INUM_DISK_SHIFT) |
           ((uint64_t) (ag   & SM_INUM_AG_MASK) << SM_INUM_AG_SHIFT)   |
           ((uint64_t) (idx  & SM_INUM_INDEX_MASK));
} // sm_inum_make

static inline void
sm_inum_decode(
    uint64_t  inum,
    uint32_t *r_disk,
    uint32_t *r_ag,
    uint32_t *r_idx)
{
    *r_idx  = (uint32_t) (inum & SM_INUM_INDEX_MASK);
    *r_ag   = (uint32_t) ((inum >> SM_INUM_AG_SHIFT)   & SM_INUM_AG_MASK);
    *r_disk = (uint32_t) ((inum >> SM_INUM_DISK_SHIFT) & SM_INUM_DISK_MASK);
} // sm_inum_decode

/*
 * Resolve an inum to its on-disk location.  Returns the device offset of
 * the inode's 4 KiB block and stores the disk id in *r_disk.  The caller
 * must ensure inum != 0.
 */
static inline uint64_t
sm_inum_to_device_offset(
    const struct space_map *sm,
    uint64_t                inum,
    uint32_t               *r_disk)
{
    uint32_t            disk, ag_idx, block_idx;
    const struct sm_ag *ag;

    sm_inum_decode(inum, &disk, &ag_idx, &block_idx);
    ag      = &sm->devices[disk].ags[ag_idx];
    *r_disk = disk;
    return ag->log_offset + ag->log_size +
           (uint64_t) (block_idx - 1) * SM_BLOCK_SIZE;
} // sm_inum_to_device_offset

/*
 * Is `inum` addressable in this space map (disk / AG / block index all in
 * range, and the resulting block within the device)?  Used before faulting an
 * inode block in from disk so a bogus/stale handle can't index out of bounds.
 */
static inline int
sm_inum_valid(
    const struct space_map *sm,
    uint64_t                inum)
{
    uint32_t            disk, ag_idx, block_idx;
    const struct sm_ag *ag;
    uint64_t            off;

    if (inum == 0) {
        return 0;
    }
    sm_inum_decode(inum, &disk, &ag_idx, &block_idx);
    if (disk >= sm->num_devices || block_idx == 0) {
        return 0;
    }
    if (ag_idx >= sm->devices[disk].num_ags) {
        return 0;
    }
    ag  = &sm->devices[disk].ags[ag_idx];
    off = ag->log_offset + ag->log_size + (uint64_t) (block_idx - 1) * SM_BLOCK_SIZE;
    return off + SM_BLOCK_SIZE <= ag->base_offset + ag->size;
} // sm_inum_valid

/*
 * Inverse of sm_inum_to_device_offset: given a freshly-allocated block at
 * (disk, offset), compute the inum that addresses it.
 */
static inline uint64_t
sm_inum_from_device_offset(
    const struct space_map *sm,
    uint32_t                disk,
    uint64_t                offset)
{
    uint32_t            ag_idx    = (uint32_t) (offset >> SM_AG_SIZE_LOG2);
    const struct sm_ag *ag        = &sm->devices[disk].ags[ag_idx];
    uint64_t            data_base = ag->log_offset + ag->log_size;
    uint32_t            block_idx = (uint32_t) ((offset - data_base) / SM_BLOCK_SIZE) + 1;

    return sm_inum_make(disk, ag_idx, block_idx);
} // sm_inum_from_device_offset
