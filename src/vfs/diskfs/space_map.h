// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <pthread.h>

#include "common/rbtree.h"

/*
 * Space map allocator for diskfs.
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

#define SM_BLOCK_SHIFT            12
#define SM_BLOCK_SIZE             (1ULL << SM_BLOCK_SHIFT)
#define SM_BLOCK_MASK             (SM_BLOCK_SIZE - 1)

#define SM_AG_SIZE_LOG2           31                        /* 2 GiB */
#define SM_AG_SIZE                (1ULL << SM_AG_SIZE_LOG2)
#define SM_AG_OFFSET_MASK         (SM_AG_SIZE - 1)

#define SM_SUPERBLOCK_OFFSET      0
#define SM_SUPERBLOCK_SIZE        4096
#define SM_SUPERBLOCK_MAGIC       0x4D5346534B534944ULL     /* "DISKSFSM" */
#define SM_FORMAT_VERSION         3

/*
 * Bootstrap inode blocks carved after AG 0's log on device 0 at format time:
 * block_idx 1 is reserved (inum 1 invalid by convention), 2 is the root
 * inode, and the next SM_BOOTSTRAP_ORPHAN_SLOTS blocks (inums 3..) are the
 * sharded orphan-list inodes (diskfs spreads deleted-inode records across
 * them so unlinks don't serialize on one inode's write lock).
 */
#define SM_BOOTSTRAP_ORPHAN_SLOTS 16

#define SM_RESERVATION_MIN        (1ULL << 20)              /* 1 MiB */

#define SM_ALIGN_UP(x) (((x) + SM_BLOCK_MASK) & ~SM_BLOCK_MASK)

/*
 * Segregated free-list size classes for the per-AG allocator.  An extent of
 * `length` belongs to class floor(log2(length >> SM_BLOCK_SHIFT)) (block-count
 * classes); class C holds extents whose block count is in [2^C, 2^(C+1)).  A
 * 2 GiB AG with 4 KiB blocks tops out at 2^19 blocks, so 22 classes covers it
 * with headroom, and the per-AG non-empty bitmask fits in a uint32_t.  This
 * size index lets allocation find an adequate free extent without the old
 * O(N) first-fit scan over the offset-ordered tree.
 */
#define SM_SIZE_CLASSES           22

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
#define SM_INUM_INDEX_BITS        32
#define SM_INUM_AG_BITS           24
#define SM_INUM_DISK_BITS         8

#define SM_INUM_INDEX_MASK        ((1ULL << SM_INUM_INDEX_BITS) - 1)
#define SM_INUM_AG_MASK           ((1ULL << SM_INUM_AG_BITS)    - 1)
#define SM_INUM_DISK_MASK         ((1ULL << SM_INUM_DISK_BITS)  - 1)

#define SM_INUM_AG_SHIFT          SM_INUM_INDEX_BITS
#define SM_INUM_DISK_SHIFT        (SM_INUM_INDEX_BITS + SM_INUM_AG_BITS)

/*
 * Each AG carves a fixed prefix off the front of its data range for its
 * own space-map log.  The log is double-buffered (slot A + slot B) so
 * condensation can ping-pong atomically.  Sized for the worst-case
 * fragmentation of a 1 GiB AG with 4 KiB blocks (~2 MiB per snapshot) with
 * comfortable headroom.
 */
#define SM_AG_LOG_SLOT_SIZE       (4ULL << 20) /* 4 MiB */
#define SM_AG_LOG_SLOT_COUNT      2
#define SM_AG_LOG_SIZE            (SM_AG_LOG_SLOT_SIZE * SM_AG_LOG_SLOT_COUNT)

/* Free headroom (bytes) left in a slot's delta region when runtime
 * condensation is triggered; new journaling parks behind the condense, so
 * this is margin against the hard-full abort, not a budget that fills. */
#define SM_AG_LOG_CONDENSE_MARGIN (64ULL << 10)

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
#define SM_AG_LOG_MAGIC           0x474F4C47414B5344ULL /* "DSKAGLOG" */

#define SM_AG_LOG_OP_ALLOC        0u /* [offset,len) was handed out: remove from free */
#define SM_AG_LOG_OP_FREE         1u /* [offset,len) was returned:   add to free */

struct sm_ag_log_header {
    uint64_t magic;
    uint64_t generation;
    uint32_t base_count;
    uint32_t delta_count;
    /* Checkpoint sequence: the highest redo-record seq whose space deltas for
     * this AG are folded into the base extents below.  On recovery, redo deltas
     * with seq > ckpt_seq are replayed against this snapshot (idempotent).  See
     * the checkpoint/trim coupling in diskfs_log.c. */
    uint64_t ckpt_seq;
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
 * Journal bridge: space_map calls back into diskfs to fetch the 4 KiB AG-log
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
    /* Record an alloc delta into the current transaction so it is serialized
     * into the txn's redo record (struct diskfs_redo_delta) and replayed on
     * crash.  Purely in-memory -- no block read, never parks.  op is
     * SM_AG_LOG_OP_ALLOC / SM_AG_LOG_OP_FREE.  Called under the AG lock by the
     * allocator hot path. */
    void  (*record_delta)(
        void    *user,
        uint32_t device_id,
        uint32_t ag_index,
        uint64_t device_offset,
        uint64_t length,
        uint32_t op);
    void *user;
};

/*
 * Mount-time block I/O bridge.  All disk access goes through the async
 * evpl_block path; at mount (before worker threads exist) diskfs drives those
 * async ops to completion by pumping a transient evpl loop and exposes them
 * here.  offset must be block-aligned; read rounds the length up to a block
 * internally and copies the requested bytes out (so callers may request a
 * sub-block struct), write requires a block-aligned length.  Each returns 0 on
 * success, -1 on error.
 */
struct sm_io_write {
    uint32_t    device_id;
    const void *buf;
    uint64_t    length;
    uint64_t    offset;
};

struct sm_io {
    int   (*read)(
        void    *user,
        uint32_t device_id,
        void    *buf,
        uint64_t length,
        uint64_t offset);
    int   (*write)(
        void       *user,
        uint32_t    device_id,
        const void *buf,
        uint64_t    length,
        uint64_t    offset);
    int   (*write_many)(
        void                     *user,
        const struct sm_io_write *writes,
        uint32_t                  count);
    int   (*flush)(
        void    *user,
        uint32_t device_id);
    void *user;
};

/*
 * Statically-reserved intent log region.  Lives on device 0 right after
 * the superblock; its exact location is also recorded in the superblock
 * so future format versions can move it.  Carved out of AG 0 of device 0.
 */
#define SM_INTENT_LOG_DEVICE       0
#define SM_INTENT_LOG_OFFSET       SM_SUPERBLOCK_SIZE

/* The intent-log size is a runtime tunable (diskfs config "intent_log_size",
 * persisted in the superblock and carried on struct space_map / the IL).  A
 * larger log lets more redo records pipeline before the ring laps, which helps
 * write throughput on big devices; small test devices need a small log so the
 * AG 0 metadata reservation fits.  These are the default and the floor. */
#define SM_INTENT_LOG_SIZE_DEFAULT (1ULL << 30)  /* 1 GiB */
#define SM_INTENT_LOG_SIZE_MIN     (4ULL << 20)  /* 4 MiB */

/* superblock flags */
#define SM_SB_CLEAN                0x1ULL  /* set at clean unmount, cleared at mount */

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
    /* Block-mode (pNFS) relocated-AG-log region: where the AG logs for remote
     * data devices live on the local metadata device (device 0), recorded so a
     * persistent remount can validate the deterministic (device,ag)->slot map.
     * remote_log_size == 0 means no remote devices (today's layout). */
    uint32_t num_remote_devices;
    uint32_t remote_log_device;     /* device holding relocated logs (== 0) */
    uint64_t remote_log_offset;     /* region base on remote_log_device */
    uint64_t remote_log_size;       /* total region bytes */
    /* Inode-generation floor: every generation ever issued by this filesystem
     * is below this value, persisted with reserve-ahead so a crash can never
     * re-issue a generation (which would let a stale file handle resolve to a
     * reused inode block).  diskfs owns the semantics; see the gen allocator
     * there. */
    uint64_t gen_floor;
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

/*
 * Device roles.  LOCAL devices hold all metadata (superblock, intent log,
 * inodes/b+trees) and may hold data.  REMOTE devices model storage that lives
 * outside this system for pNFS block layouts: diskfs tracks their free space
 * but never opens them (no evpl_block handle); their AG logs are relocated onto
 * a LOCAL device, and they hold only file data, accessed directly by the pNFS
 * block client.  Device 0 must be LOCAL.
 */
#define SM_DEV_LOCAL     0
#define SM_DEV_REMOTE    1

#define SM_DEVICEID_SIZE 16
#define SM_SIG_MAX       64

/*
 * Per-device configuration handed to space_map_create.  For LOCAL devices only
 * `size` and `role` matter.  REMOTE devices additionally carry the stable
 * 16-byte pNFS deviceid and an RFC 5663 SIMPLE-volume content signature
 * {sig_offset, sig[0..sig_len)} that the block client matches against its local
 * disks (provisioned out of band; diskfs never writes it).
 */
struct sm_device_cfg {
    uint64_t size;
    uint32_t role;
    uint8_t  deviceid[SM_DEVICEID_SIZE];
    uint64_t sig_offset;
    uint32_t sig_len;
    uint8_t  sig[SM_SIG_MAX];
};

struct sm_extent {
    struct rb_node    node;                    /* free_by_offset (coalescing) */
    uint64_t          offset;
    uint64_t          length;
    struct sm_extent *size_prev, *size_next;   /* free_by_size[class] list links */
};

/* A reservation claim: a contiguous region of an AG handed to one worker thread
 * as a bump arena.  It is NOT removed from the AG free tree (the tree stays
 * == committed state, so condense never leaks the uncommitted tail); the claim
 * only stops a concurrent grab from re-handing the same region.  refcount =
 * in-flight (submitted-but-not-yet-durable) txns that allocated from it; the
 * claim is GC'd only once it is `retiring` (the owner thread moved off it) AND
 * refcount hits 0, so no in-flight ALLOC delta can apply to a re-granted
 * region.  Protected by the owning AG's lock. */
struct sm_claim {
    uint64_t         base;          /* absolute device offset of the claimed region */
    uint64_t         len;
    uint32_t         refcount;      /* in-flight txns that allocated from this claim */
    int              retiring;      /* owner released it; GC when refcount==0 */
    struct sm_claim *next;
};

/* Per-thread bump reservation (one for metadata, one for data).  Hands out
 * [cursor, base+len) thread-locally with no lock and no per-block allocator
 * call; refills via space_map_reserve_chunk when exhausted. */
struct sm_reservation {
    uint32_t         device_id;
    uint32_t         ag_index;
    uint64_t         base;
    uint64_t         len;
    uint64_t         cursor;        /* next free offset within [base, base+len) */
    struct sm_claim *claim;         /* the AG claim backing this reservation */
    int              valid;
};

struct sm_device;

struct sm_ag {
    uint32_t          device_id;
    uint32_t          ag_index;
    struct sm_device *dev;            /* owning device (for the lock-free free-space index) */
    int16_t           maxclass;     /* largest non-empty size class, -1 if full; mirrored
                                     * into dev->maxclass_bits[maxclass] (lock-free) */
    uint64_t          base_offset;
    uint64_t          size;
    uint32_t          log_device_id; /* device whose storage holds this AG's log
                                      * (always LOCAL; == device_id unless this
                                      * AG tracks a relocated REMOTE device) */
    uint64_t          log_offset;    /* absolute offset of this AG's log on log_device_id */
    uint64_t          log_size;       /* total log bytes (both slots) */
    uint64_t          free_bytes;
    struct rb_tree    free_by_offset; /* extents keyed by offset */
    struct sm_extent *free_by_size[SM_SIZE_CLASSES];      /* same extents, by size class */
    uint32_t          size_nonempty;  /* bitmask of non-empty classes */
    struct sm_claim  *claims;         /* outstanding reservation claims (protected by lock) */
    pthread_mutex_t   lock;

    /* On-disk allocation-log state (protected by lock). */
    uint32_t          log_slot;       /* active slot index (0 or 1) */
    uint64_t          log_generation; /* generation of the active slot */
    uint32_t          log_base_count; /* condensed base extents in the active slot */
    uint32_t          log_delta_count; /* deltas appended since the last condense */

    /* Checkpoint-in-progress gate (protected by lock): set by
     * space_map_ag_begin_checkpoint, cleared by space_map_condense_commit, so
     * only one background checkpoint of this AG runs at a time.  Allocs/frees
     * proceed concurrently (they no longer park on it). */
    int               condensing;

    /* Checkpoint coupling (protected by lock).  ckpt_seq is the highest redo
     * seq folded into this AG's on-disk base snapshot; the redo trim frontier
     * (diskfs_push_trim) may not advance past a record carrying a delta for an
     * AG whose ckpt_seq is still below that record's seq.  ckpt_dirty is set on
     * the first delta after a checkpoint and cleared when the next checkpoint
     * commits, so only AGs with un-checkpointed deltas are rewritten. */
    uint64_t          ckpt_seq;
    int               ckpt_dirty;
};

struct sm_device {
    uint32_t      device_id;
    uint64_t      size;
    uint32_t      num_ags;
    uint32_t      ag_rotor;
    uint32_t      role;                       /* SM_DEV_LOCAL | SM_DEV_REMOTE */
    uint8_t       deviceid[SM_DEVICEID_SIZE];  /* REMOTE: pNFS deviceid */
    uint64_t      sig_offset;                 /* REMOTE: SIMPLE-volume signature */
    uint32_t      sig_len;
    uint8_t       sig[SM_SIG_MAX];
    struct sm_ag *ags;

    /* Lock-free free-space index: maxclass_bits[c] is a bit array over AG
     * indices; bit a is set iff ags[a].maxclass == c.  Maintained with atomic
     * OR/AND on AG max-class transitions (no shared lock -> allocators never
     * serialize on it).  sm_pick_and_alloc scans these to jump straight to an
     * AG with an adequate extent instead of walking every AG. */
    uint64_t     *maxclass_bits[SM_SIZE_CLASSES];
    uint32_t      mc_class_count[SM_SIZE_CLASSES]; /* #AGs in each class (atomic) -- lets the
                                                    * lookup skip empty classes without scanning */
    uint32_t      mc_words;        /* (num_ags + 63) / 64 */
    uint32_t      mc_rotor;        /* spreads the lookup start to balance AGs */

    /* Running sum of this device's AGs' free_bytes (atomic; adjusted under the
     * AG lock on every alloc/free).  Lets space_map_free_bytes report total free
     * by summing num_devices counters instead of locking and scanning every AG
     * (~30K/device) -- the old scan was an O(all-AGs) lock storm on the
     * reservation hot path.  Seeded by sm_init_device_free_totals after
     * format/load. */
    uint64_t      free_bytes;
};

struct space_map {
    struct sm_device *devices;
    uint32_t          num_devices;
    uint32_t          device_rotor;
    uint64_t          total_capacity;  /* raw sum of device sizes */
    uint64_t          usable_capacity; /* allocatable total (sum of AG data
                                        * ranges, metadata excluded); constant
                                        * after create.  Total free is the sum of
                                        * the per-device free_bytes counters (see
                                        * space_map_free_bytes), maintained
                                        * incrementally on the alloc/free path. */
    pthread_mutex_t   lock;           /* protects rotors and journaled writes */

    /* Relocated remote-AG-log region on device 0 (block mode); zero if no
     * remote devices.  Deterministic from the device cfg, recomputed on mount
     * and cross-checked against the superblock. */
    uint32_t          num_remote_devices;
    uint64_t          remote_log_offset;
    uint64_t          remote_log_size;

    /* Active intent-log size: the configured value at mkfs, or the value the
     * superblock recorded on a remount.  Drives the AG 0 metadata layout. */
    uint64_t          intent_log_size;
};

struct sm_thread_cache {
    uint32_t device_id;
    uint64_t offset;
    uint64_t length;
    int      valid;
    /* Per-thread AG arena (Phase 0 thundering-herd avoidance): the AG this
     * thread most recently allocated from.  Tried first on the next allocation
     * so workers stick to their own AG (uncontended ag->lock) instead of all
     * ganging on the same hot AG.  Re-homed on every successful pick; valid==0
     * until the first allocation. */
    uint32_t arena_dev;
    uint32_t arena_ag;
    int      arena_valid;
};

struct space_map *
space_map_create(
    const struct sm_device_cfg *cfg,
    uint32_t                    num_devices,
    uint64_t                    intent_log_size);

void
space_map_destroy(
    struct space_map *sm);

/*
 * Return code for the journaling operations below: the journal write goes
 * through the async block path (sm_journal.claim_block), so when a log block
 * is not resident the claim parks the calling request and issues the read, and
 * the operation reports SM_AGAIN without changing any allocator state.  The
 * caller must unwind and re-drive the whole operation once resumed; the retry
 * is clean because nothing was mutated.  0 = done, -1 = ENOSPC (alloc only).
 */
#define SM_AGAIN 1

/*
 * Ensure the cache holds at least min_bytes of contiguous space, refilling
 * (journals + may SM_AGAIN) if short.  Hands out nothing; callers use it to
 * front-load the journaling/suspension of a metadata op before a non-suspendable
 * section (e.g. a b+tree modify) so the subsequent allocs are pure cache draws.
 * `floor` is the minimum reservation to grab when refilling (the over-reserve
 * amount that batches future small allocs); pass SM_RESERVATION_MIN for the
 * batched behaviour or 0 for an exact reservation (no retained tail).
 * 0 = covered, -1 = ENOSPC, SM_AGAIN = parked.
 */
int
space_map_reserve(
    struct space_map        *sm,
    struct sm_thread_cache  *cache,
    const struct sm_journal *jnl,
    uint32_t                 role,
    uint64_t                 min_bytes,
    uint64_t                 floor);

/* `role` (SM_DEV_LOCAL/SM_DEV_REMOTE) restricts the allocation to devices of
 * that class: block mode places metadata on LOCAL and data on REMOTE devices.
 * With no remote devices everything is LOCAL (today's single-pool behavior).
 * `floor` is the over-reserve minimum (see space_map_reserve). */
int
space_map_alloc(
    struct space_map        *sm,
    struct sm_thread_cache  *cache,
    const struct sm_journal *jnl,
    uint32_t                 role,
    uint64_t                 size,
    uint64_t                 floor,
    uint32_t                *r_device_id,
    uint64_t                *r_device_offset);

/* File-data allocation path: the reservation tail is live only in memory.
 * Exact consumed ranges are journaled before being returned to the caller; the
 * unused tail can be discarded without a durable FREE. */
int
space_map_alloc_volatile_reservation(
    struct space_map        *sm,
    struct sm_thread_cache  *cache,
    const struct sm_journal *jnl,
    uint32_t                 role,
    uint64_t                 size,
    uint64_t                 floor,
    uint32_t                *r_device_id,
    uint64_t                *r_device_offset);

static inline int
space_map_has_remote(const struct space_map *sm)
{
    return sm->num_remote_devices > 0;
} // space_map_has_remote

/* --- Per-thread reservation allocator (leak-free, intent-log-accounted) --- */

/* Grab a fresh bump reservation (claim-free committed-free region, >= want, up
 * to chunk) into `r`.  Returns 0 / -1 (ENOSPC). */
int
space_map_reserve_chunk(
    struct space_map      *sm,
    struct sm_reservation *r,
    uint32_t               role,
    uint64_t               want,
    uint64_t               chunk,
    uint32_t               seed);

/* Hand out `need` from `r` thread-locally (records the ALLOC delta).  Returns 0,
 * or 1 if exhausted (caller refills via space_map_reserve_chunk and retries). */
int
space_map_bump_alloc(
    struct sm_reservation   *r,
    const struct sm_journal *jnl,
    uint64_t                 need,
    uint32_t                *r_device_id,
    uint64_t                *r_device_offset);

/* Release `r`'s claim (GC'd once its in-flight allocations retire). */
void
space_map_release_reservation(
    struct space_map      *sm,
    struct sm_reservation *r);

/* Ensure `r` can satisfy `want` without refilling (pre-reserve).  0 / -1. */
int
space_map_reservation_ensure(
    struct space_map      *sm,
    struct sm_reservation *r,
    uint32_t               role,
    uint64_t               want,
    uint64_t               chunk,
    uint32_t               seed);

/* Allocate `need` from `r`, refilling once if exhausted.  0 / -1. */
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
    uint64_t                *r_device_offset);

/* Default per-thread reservation chunk (configurable later via diskfs).  Small
 * enough for small test filesystems, large enough that the shared allocator is
 * touched only ~once per chunk consumed. */
#define SM_RESERVATION_CHUNK (4ULL << 20)       /* 4 MiB */

/* Apply a COMMITTED allocation to the in-memory free tree at retire/durability
 * (mirrors space_map_free_apply). */
void
space_map_alloc_apply(
    struct space_map *sm,
    uint32_t          device_id,
    uint64_t          device_offset,
    uint64_t          length);

/* Discard an uncommitted allocation on txn abort (unpin its claim, no tree
 * change). */
void
space_map_alloc_discard(
    struct space_map *sm,
    uint32_t          device_id,
    uint64_t          device_offset,
    uint64_t          length);

int
space_map_free(
    struct space_map        *sm,
    const struct sm_journal *jnl,
    uint32_t                 device_id,
    uint64_t                 device_offset,
    uint64_t                 length);

/* Pending free: journal the FREE delta now (rides the txn redo), apply the
 * in-memory free only once that txn is durable. */
int
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

int
space_map_thread_cache_return(
    struct space_map        *sm,
    const struct sm_journal *jnl,
    struct sm_thread_cache  *cache);

void
space_map_thread_cache_discard_volatile(
    struct space_map       *sm,
    struct sm_thread_cache *cache);

/*
 * Format a complete superblock image (including CRC) into buf, which must be
 * SM_SUPERBLOCK_SIZE bytes.  Used by space_map_write_superblock and by the
 * runtime generation-floor extension, which writes the block through a live
 * worker's block queue instead of the mount-time bridge.
 */
void
space_map_fill_superblock(
    struct space_map *sm,
    void             *buf,
    uint64_t          fsid,
    uint64_t          flags,
    uint64_t          root_inum,
    uint32_t          root_gen,
    uint64_t          log_seq,
    uint64_t          gen_floor);

/*
 * Write a stub superblock to offset 0 of device 0 through the mount-time
 * I/O bridge (async evpl_block, pumped to completion + flushed).  Done at
 * format/unmount time before worker threads exist.
 */
int
space_map_write_superblock(
    struct space_map   *sm,
    const struct sm_io *io,
    uint64_t            fsid,
    uint64_t            flags,
    uint64_t            root_inum,
    uint32_t            root_gen,
    uint64_t            log_seq,
    uint64_t            gen_floor);

/* Runtime AG-log condensation (see the implementation for the full
 * protocol): prepare snapshots the free set as a new base image for the
 * inactive slot; after the caller writes the image (header block last, FUA),
 * commit flips the slot and re-opens journaling.  base_count comes from the
 * image header. */
int
space_map_condense_prepare(
    struct space_map *sm,
    uint32_t          device_id,
    uint32_t          ag_index,
    void             *buf,
    uint64_t         *r_slot_offset,
    uint64_t         *r_payload,
    uint64_t          ckpt_seq);

void
space_map_condense_commit(
    struct space_map *sm,
    uint32_t          device_id,
    uint32_t          ag_index,
    uint32_t          base_count,
    uint64_t          ckpt_seq);

/* Read an AG's published checkpoint frontier (highest redo seq folded into its
 * on-disk snapshot); used by the redo trim gate. */
uint64_t
space_map_ag_ckpt_seq(
    struct space_map *sm,
    uint32_t          device_id,
    uint32_t          ag_index);

/* Claim the right to start a background checkpoint of this AG: 1 -> caller
 * submits the job, 0 -> one is already in progress.  Sets the in-progress gate
 * that space_map_condense_prepare/commit assert on. */
int
space_map_ag_begin_checkpoint(
    struct space_map *sm,
    uint32_t          device_id,
    uint32_t          ag_index);

/* Read + validate the superblock from device 0; 0 on success (fills *out),
 * -1 if absent/corrupt/wrong-version (caller should mkfs). */
int
space_map_read_superblock(
    const struct sm_io   *io,
    struct sm_superblock *out);

/*
 * Persist the free-space map to each AG's on-disk log slot (clean unmount),
 * and reload it (mount), through the mount-time I/O bridge.  persist returns
 * 0 on success; load returns 0 if every AG's snapshot validated and the
 * in-memory free trees were rebuilt from them, -1 otherwise (caller should
 * treat the filesystem as needing mkfs / recovery).
 */
int
space_map_persist(
    struct space_map   *sm,
    const struct sm_io *io,
    uint64_t            ckpt_seq);

int
space_map_load(
    struct space_map   *sm,
    const struct sm_io *io);

/* Apply one redo-replayed space delta to the in-memory free trees during crash
 * recovery (idempotent against the checkpoint base).  op is
 * SM_AG_LOG_OP_ALLOC / SM_AG_LOG_OP_FREE.  Call after space_map_load, in redo
 * seq order, for deltas whose record seq exceeds the owning AG's ckpt_seq. */
void
space_map_recover_delta(
    struct space_map *sm,
    uint32_t          device_id,
    uint32_t          ag_index,
    uint64_t          offset,
    uint64_t          length,
    uint32_t          op);

static inline uint64_t
space_map_total_capacity(const struct space_map *sm)
{
    return sm->total_capacity;
} // space_map_total_capacity

static inline uint64_t
space_map_usable_capacity(const struct space_map *sm)
{
    return sm->usable_capacity;
} // space_map_usable_capacity

/* Live free byte count, summed from the allocation groups (cold path -- statfs
 * only).  Computed on demand so the alloc/free fast path maintains no counter.
 * Space held in a thread's reservation cache (carved from an AG but not yet
 * handed to a file) reads as not-free, so the report is conservative. */
uint64_t
space_map_free_bytes(
    struct space_map *sm);

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
