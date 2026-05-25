// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#define _GNU_SOURCE
#include <stdint.h>
#include <stdlib.h>
#include <fcntl.h>
#include <pthread.h>
#include <errno.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <limits.h>
#include <jansson.h>
#include <utlist.h>
#include <urcu.h>
#include <urcu/urcu-memb.h>
#include <xxhash.h>     /* XXH_INLINE_ALL set in CMakeLists; header-only */

#include "common/varint.h"
#include "common/rbtree.h"

#include "slab_allocator.h"

#include "evpl/evpl.h"

#include "vfs/vfs.h"
#include "vfs/vfs_internal.h"
#include "diskfs.h"
#include "space_map.h"
#include "common/logging.h"
#include "common/misc.h"
#include "common/evpl_iovec_cursor.h"

#ifndef container_of
#define container_of(ptr, type, member) \
        ((type *) ((char *) (ptr) - offsetof(type, member)))
#endif /* ifndef container_of */

/* Inode cache: sharded rb-tree keyed by inum. */
#define DISKFS_INODE_CACHE_SHARDS 256
#define DISKFS_INODE_CACHE_MASK   (DISKFS_INODE_CACHE_SHARDS - 1)

/* Statically-reserved inums (block_idx in AG 0 / disk 0; see space_map.c).
 * inum 2 = root; inum 3 = orphan list (deleted inodes pending incremental
 * reclaim).  The orphan inode is a directory whose b+tree keys are orphan
 * inums; the drainer empties it. */
#define DISKFS_ROOT_INUM          2
#define DISKFS_ORPHAN_INUM        3
#define DISKFS_ORPHAN_GEN         1   /* permanent: created at format, never deleted */

/* Max inodes a single transaction can hold locked at once.  rename needs
 * 4 (two parents, child, replaced target); others (e.g. readdir) touch
 * many but only 2 at a time and release as they go. */
#define DISKFS_TXN_MAX_INODES     4

/* Diskfs RMW writes assemble:
 *   prefix (valid + zero) + request write iovecs +
 *   suffix (valid + zero) + alignment padding.
 * The largest in-tree caller-side write iovec array is CHIMERA_CLIENT_IOV_MAX
 * (260); SMB is 256 and RPC transport receives are lower by default.
 */
#define DISKFS_WRITE_MAX_IOV      260
#define DISKFS_WRITE_RMW_MAX_IOV  (DISKFS_WRITE_MAX_IOV + 5)

#define chimera_diskfs_debug(...) chimera_debug("diskfs", \
                                                __FILE__, \
                                                __LINE__, \
                                                __VA_ARGS__)
#define chimera_diskfs_info(...)  chimera_info("diskfs", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)
#define chimera_diskfs_error(...) chimera_error("diskfs", \
                                                __FILE__, \
                                                __LINE__, \
                                                __VA_ARGS__)
#define chimera_diskfs_fatal(...) chimera_fatal("diskfs", \
                                                __FILE__, \
                                                __LINE__, \
                                                __VA_ARGS__)
#define chimera_diskfs_abort(...) chimera_abort("diskfs", \
                                                __FILE__, \
                                                __LINE__, \
                                                __VA_ARGS__)

#define chimera_diskfs_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "diskfs", __FILE__, __LINE__, __VA_ARGS__)

#define chimera_diskfs_abort_if(cond, ...) \
        chimera_abort_if(cond, "diskfs", __FILE__, __LINE__, __VA_ARGS__)

struct diskfs_extent {
    uint32_t              device_id;
    uint32_t              length;
    uint64_t              device_offset;
    uint64_t              file_offset;
    void                 *buffer;
    struct rb_node        node;
    struct diskfs_extent *next;
};

struct diskfs_request_private {
    int                         opcode;
    int                         status;
    int                         pending;
    int                         niov;
    uint32_t                    read_prefix;
    uint32_t                    read_suffix;
    struct diskfs_thread       *thread;  // Thread for tracking pending I/O
    struct diskfs_txn          *txn;     // Transaction wrapping this op

    /* Data-I/O admission control (see diskfs_thread.io_wait_head).  When a
     * request parks because the block queue is full, io_resume re-enters the
     * paused path; io_reading marks a read whose extent walk has not finished,
     * so a completion that drains its in-flight reads to zero must not finalize
     * it while it is parked mid-walk. */
    void                        (*io_resume)(
        struct chimera_vfs_request *);
    struct chimera_vfs_request *io_wait_next;
    int                         io_reading;
    /* Multi-inode op scratch (lookup_at parent/child, rename's 4-inode
     * chain, etc.).  Per-op semantics documented at use sites. */
    struct diskfs_inode        *inode_stash[4];
    /* Small integer scratch for ops that need to carry state across
     * async callbacks (mount path walker uses it as a path byte offset). */
    uint32_t                    op_scratch;

    /* readdir iteration cursor + the current dirent copied out of the
     * b+tree (carried across the per-child inode fetch). */
    uint64_t                    rd_from_hash;
    uint64_t                    rd_hash;
    uint64_t                    rd_inum;
    uint32_t                    rd_gen;
    int                         rd_namelen;
    char                        rd_name[256];

    /* Scratch buffer for handlers that parse a looked-up b+tree record
     * (e.g. a dirent's inum/gen) in their async continuation.  Sized to hold
     * the largest record (DISKFS_DIRENT_REC_MAX == sizeof(dirent_rec) + 256). */
    char                        rec_scratch[320];

    /* Extent-walk iteration state, hoisted here so an async ext_next can
     * suspend the loop and resume it.  loop_* are generic loop scalars. */
    struct diskfs_extent        ext_iter;
    struct evpl_iovec_cursor    rd_cursor;
    uint64_t                    loop_off;
    uint64_t                    loop_left;
    uint64_t                    loop_pos;
    int                         loop_have;

    struct evpl_iovec           iov[66];

    // For RMW (read-modify-write) on partial block writes
    int                         rmw_phase; // 0 = no RMW, 1 = reading, 2 = writing
    uint64_t                    rmw_aligned_start; // Block-aligned start offset
    uint64_t                    rmw_aligned_length;// Block-aligned length
    uint64_t                    rmw_device_id; // Device for the new extent
    uint64_t                    rmw_device_offset; // Device offset for the new extent
    uint32_t                    rmw_prefix_len; // Bytes to preserve at start of first block
    uint32_t                    rmw_suffix_len; // Bytes to preserve at end of last block
    struct evpl_iovec           rmw_prefix_iov; // IOV for prefix data (if read from existing extent)
    struct evpl_iovec           rmw_suffix_iov; // IOV for suffix data (if read from existing extent)
    int                         rmw_prefix_pending;// Pending read for prefix
    int                         rmw_suffix_pending;// Pending read for suffix
    uint32_t                    rmw_prefix_valid; // Valid bytes in prefix (extent may be truncated)
    uint32_t                    rmw_suffix_adjust; // Adjustment for suffix when block starts before extent
    uint32_t                    rmw_suffix_valid; // Valid bytes in suffix (extent may be truncated)
    /* Carried across the async prefix/suffix lookups + trim walk. */
    int                         need_prefix_read;
    int                         need_suffix_read;
    uint64_t                    prefix_device_id, prefix_device_offset;
    uint64_t                    suffix_device_id, suffix_device_offset;
};

struct diskfs_device {
    struct evpl_block_device *bdev;
    uint64_t                  id;
    uint64_t                  size;
    uint64_t                  max_request_size;
    char                      name[256];
    pthread_mutex_t           lock;
};

struct diskfs_dirent {
    uint64_t              inum;
    uint32_t              gen;
    uint32_t              name_len;
    uint64_t              hash;
    struct rb_node        node;
    struct diskfs_dirent *next;
    char                 *name;
};

struct diskfs_kv_entry {
    uint64_t                hash;
    uint32_t                key_len;
    uint32_t                value_len;
    struct rb_node          node;
    struct diskfs_kv_entry *next;
    void                   *key;
    void                   *value;
};

struct diskfs_kv_shard {
    struct rb_tree  entries;
    pthread_mutex_t lock;
};

struct diskfs_symlink_target {
    int                           length;
    char                         *data;
    struct diskfs_symlink_target *next;
};

struct diskfs_txn;
struct diskfs_inode;
struct diskfs_inode_waiter;
struct diskfs_block;

/* Logical lock mode for an inode held by a transaction. */
enum diskfs_inode_lock_mode {
    DISKFS_INODE_LOCK_READ,
    DISKFS_INODE_LOCK_WRITE,
};

typedef void (*diskfs_inode_cb_t)(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

/* Defined in the block-cache / b+tree sections below. */
static void diskfs_txn_pin_inode_block(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode,
    int                   is_new);
static inline void diskfs_bt_node_init(
    void    *buf,
    uint32_t base,
    uint32_t capacity,
    uint16_t level);

/*
 * A transaction that cannot immediately acquire an inode in the desired
 * mode parks a waiter on the inode's FIFO wait list (protected by the
 * inode's cache shard lock).  When the conflicting holder releases, the
 * releasing thread grants the lock and hands the waiter to the owning
 * worker's grant queue (+doorbell) so the continuation runs back on the
 * transaction's own thread.
 */
struct diskfs_inode_waiter {
    struct diskfs_txn          *txn;
    enum diskfs_inode_lock_mode mode;
    uint32_t                    gen;     /* generation the txn referenced */
    int                         status;  /* CHIMERA_VFS_OK or ENOENT when granted */
    diskfs_inode_cb_t           cb;
    void                       *private_data;
    struct diskfs_inode        *inode;
    struct diskfs_inode_waiter *next;
};

struct diskfs_inode {
    uint64_t                    inum;
    uint32_t                    gen;
    uint32_t                    refcnt;
    uint64_t                    size;
    uint64_t                    space_used;
    uint32_t                    mode;
    uint32_t                    nlink;
    uint32_t                    uid;
    uint32_t                    gid;
    uint64_t                    rdev;
    uint64_t                    atime_sec;
    uint64_t                    ctime_sec;
    uint64_t                    mtime_sec;
    uint32_t                    atime_nsec;
    uint32_t                    ctime_nsec;
    uint32_t                    mtime_nsec;

    /* Inode-cache linkage, keyed by inum.  Lock state and the wait list
     * below are protected by the owning shard's mutex, never held across
     * a callback or I/O. */
    struct rb_node              node;
    int                         readers;     /* shared-lock holders */
    int                         writer;      /* 0/1 exclusive holder */
    struct diskfs_inode_waiter *wait_head;
    struct diskfs_inode_waiter *wait_tail;

    /* Eviction: an idle inode (refcnt==1, unlocked) sits on its shard's LRU
     * as a recycle candidate.  All under the shard lock. */
    struct diskfs_inode        *lru_prev, *lru_next;
    int                         on_lru;

    /* This inode's 4 KiB metadata home block in the block cache; pinned
     * while the inode is dirty in a transaction.  NULL until first claimed.
     * Directory entries, extents and the symlink target all live as keyed
     * records in this inode's b+tree (rooted in the block at offset 256). */
    struct diskfs_block        *block;

    /* Directory only: parent for ".." resolution (also persisted in dinode). */
    uint64_t                    parent_inum;
    uint32_t                    parent_gen;
};

struct diskfs_inode_shard {
    pthread_mutex_t      lock;
    struct rb_tree       inodes;       /* keyed by inum */
    struct diskfs_inode *lru_head, *lru_tail; /* idle (recycle) candidates, LRU-first */
    uint32_t             ninodes;      /* resident inodes in this shard */
};

struct diskfs_inode_cache {
    uint32_t                  shard_cap; /* soft cap before recycling kicks in */
    struct diskfs_inode_shard shards[DISKFS_INODE_CACHE_SHARDS];
};

/* Total inode cache target; per-shard cap = total / shards.  Eviction is a
 * soft cap (grows past it when every resident inode is busy -- bounded by the
 * live working set; the A5b waiter turns this into a hard cap). */
#define DISKFS_INODE_CACHE_DEFAULT_INODES    (256 * 1024)

/* ------------------------------------------------------------------ */
/* Block cache                                                         */
/* ------------------------------------------------------------------ */

#define DISKFS_BLOCK_SHIFT                   SM_BLOCK_SHIFT  /* 12 */
#define DISKFS_BLOCK_SIZE                    SM_BLOCK_SIZE   /* 4096 */
#define DISKFS_BLOCK_CACHE_SHARDS            256
#define DISKFS_BLOCK_CACHE_SHARD_MASK        (DISKFS_BLOCK_CACHE_SHARDS - 1)
#define DISKFS_BLOCK_CACHE_BUCKETS_PER_SHARD 1024
#define DISKFS_BLOCK_CACHE_BUCKET_MASK       (DISKFS_BLOCK_CACHE_BUCKETS_PER_SHARD - 1)

enum diskfs_block_state {
    DISKFS_BLOCK_LOADING,  /* read I/O in flight; buffer not yet valid, ops wait */
    DISKFS_BLOCK_CLEAN,    /* matches final on-disk location; evictable when unpinned */
    DISKFS_BLOCK_DIRTY,    /* modified, pinned by >=1 txn, not yet logged */
    DISKFS_BLOCK_LOGGED,   /* intent record durable; awaiting tail-push to final loc */
};

struct diskfs_bt_op;
struct diskfs_block_waiter;

/*
 * A cached 4 KiB on-disk block, keyed by (device_id, device_offset).  The
 * buffer is plain heap memory; block I/O copies it through a thread-local
 * evpl_iovec so buffers are never shared across evpl instances.  All fields
 * (hash linkage, pin_count, state, LRU membership) are protected by the
 * owning shard lock.  A buffer is a recycle candidate exactly when it is CLEAN
 * and pin_count == 0, in which case it sits on the shard LRU (on_lru == 1);
 * recycling reuses the least-recently-used such buffer.  Buffers are drawn
 * from a pre-allocated fixed pool (shard->pool) -- a free, never-yet-keyed
 * buffer starts CLEAN on the LRU, not in any bucket.
 */
struct diskfs_block {
    uint32_t                    device_id;
    uint64_t                    device_offset; /* block-aligned; key with device_id */
    struct evpl_iovec           iov;           /* SHARED DISKFS_BLOCK_SIZE buffer; .data may
                                                * be NULL on a never-yet-used pool slot */
    int                         pin_count;     /* >0 => pinned, not reclaimable */
    enum diskfs_block_state state;
    uint64_t                    seq;           /* update order for tail-push */
    struct diskfs_block        *hash_next;     /* bucket chain */
    struct diskfs_block        *lru_prev, *lru_next; /* shard LRU (CLEAN + unpinned) */
    int                         on_lru;        /* 1 iff linked on the shard LRU */

    /* Continuations blocked on a LOADING block, woken when the read I/O
     * completes.  Each waiter names its owning worker, so a completion that
     * runs on a different worker (one that issued the read) can dispatch it
     * back home.  Protected by the owning shard lock. */
    struct diskfs_block_waiter *wait_head;
    struct diskfs_block_waiter *wait_tail;
};

struct diskfs_block_shard {
    pthread_mutex_t       lock;
    struct diskfs_block **buckets;     /* [DISKFS_BLOCK_CACHE_BUCKETS_PER_SHARD] */

    /* Pre-allocated fixed pool of block structs (all protected by lock); the
     * structs are never individually freed.  Each struct's buffer is a SHARED
     * evpl iovec allocated lazily on first use and reused across recyclings
     * (released only at teardown).  The LRU holds only CLEAN, unpinned buffers
     * (recycle candidates), ordered least-recently-used first. */
    struct diskfs_block  *pool;                 /* [nblocks] */
    struct diskfs_block  *lru_head, *lru_tail;  /* lru_head = next to recycle */
    uint32_t              nblocks;              /* block structs owned by this shard */
};

struct diskfs_block_cache {
    uint32_t                  shard_cap;   /* max resident buffers per shard */
    struct diskfs_block_shard shards[DISKFS_BLOCK_CACHE_SHARDS];
};

/*
 * The block-cache pool is fixed and never blocks (see diskfs_block_recycle):
 * it must exceed the maximum pinnable set so an unpinned victim always exists.
 * The only long-lived pins are a transaction's blocks, held from claim through
 * LOGGED until the tail-pusher marks them CLEAN, and logging is backpressured
 * to the intent-log size -- so the pinned set is bounded by the journal.  The
 * default is 2x the journal (comfortable per-shard headroom over the variance),
 * and a configured size is floored at 1.5x.
 */
#define DISKFS_INTENT_LOG_BLOCKS          (SM_INTENT_LOG_SIZE / SM_BLOCK_SIZE)
#define DISKFS_BLOCK_CACHE_DEFAULT_BLOCKS (2 * DISKFS_INTENT_LOG_BLOCKS)
#define DISKFS_BLOCK_CACHE_MIN_BLOCKS     (DISKFS_INTENT_LOG_BLOCKS + DISKFS_INTENT_LOG_BLOCKS / 2)

/*
 * On-disk inode block layout (4 KiB):
 *   [0, DISKFS_INODE_AREA)   struct diskfs_dinode (scalar attributes)
 *   [DISKFS_INODE_AREA, end) the inode's b+tree root node (embedded)
 *
 * Directory entries, file extents and the symlink target all live as keyed
 * records in the inode's single b+tree; the root node is embedded in the
 * inode block, and deeper nodes occupy their own 4 KiB blocks.
 */
#define DISKFS_INODE_AREA                 256
#define DISKFS_BT_ROOT_BASE               DISKFS_INODE_AREA
#define DISKFS_BT_ROOT_CAP                (DISKFS_BLOCK_SIZE - DISKFS_INODE_AREA) /* 3840 */
#define DISKFS_BT_NODE_CAP                DISKFS_BLOCK_SIZE            /* 4096 */

struct diskfs_dinode {
    uint64_t inum;
    uint32_t gen;
    uint32_t mode;
    uint32_t nlink;
    uint32_t uid;
    uint32_t gid;
    uint64_t rdev;
    uint64_t size;
    uint64_t space_used;
    uint64_t atime_sec;
    uint64_t mtime_sec;
    uint64_t ctime_sec;
    uint32_t atime_nsec;
    uint32_t mtime_nsec;
    uint32_t ctime_nsec;
    uint64_t parent_inum;     /* directories only */
    uint32_t parent_gen;
};

/* ------------------------------------------------------------------ */
/* Per-inode b+tree (on-disk, slotted nodes)                           */
/* ------------------------------------------------------------------ */

/* Record types share one tree per inode; each occupies a key range. */
enum diskfs_bt_rectype {
    DISKFS_REC_DIRENT  = 1,
    DISKFS_REC_EXTENT  = 2,
    DISKFS_REC_SYMLINK = 3,
    DISKFS_REC_ORPHAN  = 4,   /* orphan-list inode only: subkey = orphaned inum */
};

/* B+tree key: ordered by (type, subkey).  subkey is the name hash for
 * dirents, the file offset for extents, 0 for the single symlink record. */
struct diskfs_bt_key {
    uint8_t  type;
    uint8_t  pad[7];
    uint64_t subkey;
};

/*
 * Slotted node header at the base of every node (embedded root or full
 * block).  level 0 == leaf.  Interior nodes hold a fixed array of
 * {key, child_bptr}; leaves hold a slot array of {key, off, len} plus a
 * record heap growing down from free_end.
 */
struct diskfs_bt_node_hdr {
    uint16_t level;
    uint16_t nitems;
    uint32_t capacity;     /* usable node bytes (DISKFS_BT_ROOT_CAP or _NODE_CAP) */
    uint32_t free_end;     /* leaf heap top (records occupy [free_end, capacity)) */
    uint32_t reserved;
    uint64_t next_leaf;    /* leaf only: bptr of next leaf in key order (0 = none) */
    uint64_t prev_leaf;    /* leaf only: bptr of prev leaf in key order (0 = none) */
};

struct diskfs_bt_islot {          /* interior slot, 24 B */
    struct diskfs_bt_key key;
    uint64_t             child;   /* bptr (sm disk:ag:index encoding) */
};

struct diskfs_bt_lslot {          /* leaf slot, 24 B */
    struct diskfs_bt_key key;
    uint32_t             off;     /* record offset from node base */
    uint32_t             len;     /* record length */
};

/* Leaf record payloads (stored in the leaf heap). */
struct diskfs_dirent_rec {
    uint64_t inum;
    uint32_t gen;
    uint16_t name_len;
    char     name[];
} __attribute__((packed));

struct diskfs_extent_rec {
    uint64_t length;
    uint32_t device_id;
    uint32_t pad;
    uint64_t device_offset;
} __attribute__((packed));

#define DISKFS_DIRENT_REC_MAX (sizeof(struct diskfs_dirent_rec) + 256)
_Static_assert(DISKFS_DIRENT_REC_MAX <= 320,
               "diskfs_request_private.rec_scratch must hold a full dirent record");

/*
 * A symlink stores its target as the single record of the new inode's b+tree,
 * which lives in the embedded root (it can never be split out -- there's only
 * one record), so the target must fit one node: root capacity minus the node
 * header and one leaf slot.  Longer targets are rejected with ENAMETOOLONG
 * rather than aborting the daemon.
 */
#define DISKFS_SYMLINK_TARGET_MAX \
        (DISKFS_BT_ROOT_CAP - sizeof(struct diskfs_bt_node_hdr) - sizeof(struct diskfs_bt_lslot))

/*
 * Intent-log redo record, written into the reserved intent-log region.
 * A record is a header followed by num_blocks (block-header, 4 KiB content)
 * pairs, padded to a 4 KiB multiple.  Full-block redo: the record carries
 * the entire post-image of every dirty block in the transaction.
 */
#define DISKFS_REDO_MAGIC     0x4F44455246534944ULL /* "DISFREDO" */

/*
 * Redo record on-log layout: this header, then num_blocks
 * {diskfs_redo_block_header, 4 KiB image} pairs, padded to a 4 KiB multiple.
 *
 * `magic` is the scan signature and `csum_{lo,hi}` is an XXH3-128 over the
 * entire record (reclen bytes) computed with the csum fields zeroed.  Together
 * they let crash recovery locate intact records anywhere in the (possibly
 * wrapped) circular log: probe 4 KiB boundaries for the magic, then accept the
 * record only if the 128-bit hash over `reclen` bytes verifies -- a partially
 * overwritten or torn record fails and is skipped.  `seq` orders records
 * (latest image of a block wins) and `tail` is the log tail at write time, so
 * recovery bounds replay to [tail, head] from the highest-seq record.
 */
struct diskfs_redo_header {
    uint64_t magic;
    uint64_t csum_lo;      /* XXH3-128 of the record, csum fields zeroed */
    uint64_t csum_hi;
    uint64_t seq;          /* monotonically increasing record sequence */
    uint64_t tail;         /* log_tail (oldest un-pushed offset) at write time */
    uint32_t num_blocks;
    uint32_t reclen;       /* total record length, including padding */
};

struct diskfs_redo_block_header {
    uint32_t device_id;
    uint32_t pad;
    uint64_t device_offset;
};

enum diskfs_txn_type {
    DISKFS_TXN_READ,
    DISKFS_TXN_WRITE,
};

/* A dirty block held (pinned) by a transaction until commit/log. */
struct diskfs_txn_block {
    struct diskfs_block     *block;
    /* Zero-copy snapshot of block->iov, cloned at commit on the worker under
     * the inode write lock (content final, no COW possible there).  Moved into
     * the redo record by the intent-log thread, so the IL thread never touches
     * the live block->iov and the record captures this txn's committed image
     * even if a later writer COWs the cache block. */
    struct evpl_iovec        snap;
    struct diskfs_txn_block *next;
};

struct diskfs_txn_slot {
    struct diskfs_inode *inode;
    enum diskfs_inode_lock_mode mode;
};

/* Commit completion callback.  Will be invoked exactly once per successful
 * commit, with status 0 on success or a CHIMERA_VFS_* code on failure.
 * Today commits never fail, but the signature is async-shaped so a future
 * intent-log write can fail. */
typedef void (*diskfs_txn_commit_cb_t)(
    struct diskfs_txn *txn,
    int                status,
    void              *private_data);

/* A space range freed by a txn.  The FREE delta is journaled immediately (it
 * rides the redo), but the in-memory free is withheld until the txn is durable
 * (applied in diskfs_redo_write_cb) or discarded on abort -- so a freed range
 * (and any still-cached metadata block backing it) can't be reused until the
 * free is committed. */
struct diskfs_txn_free {
    uint32_t                device_id;
    uint64_t                device_offset;
    uint64_t                length;
    int                     journaled; /* FREE delta written (pre-commit flush) */
    struct diskfs_txn_free *next;
};

struct diskfs_txn {
    enum diskfs_txn_type     type;
    struct diskfs_thread    *thread;
    struct diskfs_txn       *next;         /* per-thread free list link */
    struct diskfs_txn_slot   inodes[DISKFS_TXN_MAX_INODES];
    int                      num_inodes;
    struct diskfs_txn_block *blocks;       /* dirty blocks pinned by this txn */
    struct diskfs_txn_free  *pending_frees; /* ranges freed, applied on commit */
};

/*
 * Per-thread NVMe-style submission and completion queues used to hand
 * write transactions off to the intent log thread and to receive commit
 * completions back.  Each ring is single-producer / single-consumer:
 *
 *   sq:  worker (producer)        ->  intent log thread (consumer)
 *   cq:  intent log thread (prod) ->  worker (consumer)
 */
#define DISKFS_IQ_RING_SIZE 1024
#define DISKFS_IQ_RING_MASK (DISKFS_IQ_RING_SIZE - 1)

struct diskfs_iq_entry {
    struct diskfs_txn     *txn;
    diskfs_txn_commit_cb_t cb;
    void                  *private_data;
    int                    status;
};

struct diskfs_iq_ring {
    /* Accessed via __atomic_* builtins. */
    uint32_t               head;     /* consumer position */
    uint32_t               tail;     /* producer position */
    struct diskfs_iq_entry entries[DISKFS_IQ_RING_SIZE];
};

struct diskfs_iq_channel {
    struct diskfs_iq_ring     sq;
    struct diskfs_iq_ring     cq;
    struct evpl_doorbell      cq_doorbell;
    struct diskfs_thread     *worker;

    /* CQEs reserved for redo writes issued but not yet completed.  Owned by
     * the intent-log thread; bounds in-flight writes to available CQ space. */
    uint32_t                  cq_inflight;

    /* Lifecycle: workers append on register, set flag on unregister.
     * Intent log thread owns the slot array. */
    struct diskfs_iq_channel *next_pending;
    int                       unregister_requested;
    int                       unregister_done;
};

#define DISKFS_IL_MAX_CHANNELS 256

/*
 * A durably-logged redo record awaiting tail-push.  Holds the record's own
 * immutable on-log image (iov), so the tail-pusher writes block post-images
 * to their final locations straight from the log copy — never racing a worker
 * that re-dirties the live cache block.
 */
struct diskfs_il_record {
    uint64_t                 seq;
    uint64_t                 offset;     /* byte offset in the intent-log region */
    uint64_t                 reclen;
    uint32_t                 num_blocks;
    uint32_t                 niov;       /* 1 + num_blocks */
    /* Scatter-gather image of the on-log record: iovs[0] is the 4 KiB-aligned
    * header region (redo_header + per-block headers); iovs[1..num_blocks] are
    * zero-copy refs (clones) of the cache blocks' buffers.  The same refs are
    * handed to the tail-pusher, so a block image is copied only when a writer
    * must fork a still-referenced block (COW), never on the log/push path. */
    struct evpl_iovec       *iovs;
    struct diskfs_il_record *next;
};

struct diskfs_intent_log {
    struct evpl_doorbell      wake_doorbell;
    struct evpl              *evpl;
    struct evpl_thread       *thread;
    int                       ready;             /* atomic */
    int                       shutdown;          /* atomic */
    uint32_t                  num_channels;      /* slots[] count, intent log thread only */
    struct diskfs_iq_channel *channels[DISKFS_IL_MAX_CHANNELS];
    pthread_mutex_t           registration_lock;
    struct diskfs_iq_channel *pending_head;

    /* Block queues on the intent-log thread's own evpl (one per device),
     * used both to write redo records into the intent-log region and to
     * tail-push logged blocks to their final on-disk locations. */
    struct evpl_block_queue **queue;
    uint64_t                  log_head;          /* next free byte in the intent-log region */
    uint64_t                  log_tail;          /* oldest un-pushed record (trim point) */
    uint64_t                  log_seq;           /* next redo record sequence number */

    /* Tail-pusher (runs on this thread): FIFO of durably-logged records whose
    * blocks have not yet been written to their final locations.  Processed
    * strictly oldest-first so a re-logged block's newest image lands last. */
    struct diskfs_il_record  *push_head;
    struct diskfs_il_record  *push_tail;
    struct diskfs_il_record  *push_cur;          /* record currently being pushed */
    uint32_t                  push_next;         /* next block index of push_cur to issue */
    int                       push_outstanding;  /* home writes in flight for push_cur */
    int                       redo_inflight;     /* redo writes issued, not yet in FIFO */
    int                       iocbs_inflight;    /* block writes (redo + push) on the queue, not yet completed */
};

struct diskfs_shared {
    struct diskfs_device      *devices;
    char                     **device_paths;     /* for unmount-time persistence */
    int                        num_devices;
    struct diskfs_inode_cache *inode_cache;
    struct diskfs_block_cache *block_cache;
    struct diskfs_kv_shard    *kv_shards;
    int                        num_kv_shards;
    int                        num_active_threads;
    uint8_t                    root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                   root_fhlen;
    int                        orphans_scanned;    /* mount-time orphan recovery done */
    uint64_t                   root_inum;          /* for the clean-unmount superblock */
    uint32_t                   root_gen;
    int                        persistent;         /* config opt-in: detect+reload a clean FS instead of mkfs */
    int                        mounted;            /* 1 = remounted existing FS (enables inode read-back) */
    uint32_t                   block_cache_blocks; /* total resident block-buffer cap (0 = default) */
    uint32_t                   inode_cache_inodes; /* total resident inode cap (0 = default) */
    uint64_t                   fsid;
    struct space_map          *space_map;
    struct diskfs_intent_log   intent_log;
    pthread_mutex_t            lock;
};

struct diskfs_thread {
    struct evpl                *evpl;
    struct diskfs_shared       *shared;
    struct evpl_block_queue   **queue;
    struct evpl_iovec           zero;
    struct evpl_iovec           pad;
    int                         thread_id;
    struct slab_allocator      *allocator;
    struct sm_thread_cache      space_cache;
    struct diskfs_txn          *txn_free_list;
    struct diskfs_inode_waiter *waiter_free_list;
    struct diskfs_iq_channel   *iq_channel;
    int                         pending_io;

    /* Cross-thread lock-grant delivery: any thread that releases an inode
     * and grants it to a waiter belonging to this worker enqueues the
     * granted waiter here and rings grant_doorbell, so the continuation
     * runs back on this worker. */
    pthread_mutex_t             grant_lock;
    struct diskfs_inode_waiter *grant_head;
    struct diskfs_inode_waiter *grant_tail;
    struct evpl_doorbell        grant_doorbell;

    /* Cross-thread continuation resumption: when a block this worker has
     * waiters on finishes loading (possibly on another worker that issued the
     * read), the ready waiters are queued here.  Same-worker resumptions drain
     * via the deferral (no eventfd); cross-worker ones ring the doorbell. */
    pthread_mutex_t             resume_lock;
    struct diskfs_block_waiter *resume_head;
    struct diskfs_block_waiter *resume_tail;
    struct evpl_doorbell        resume_doorbell;
    struct evpl_deferral        resume_deferral;
    struct diskfs_bt_op        *bt_op_free_list;
    struct diskfs_block_waiter *block_waiter_free_list;

    /* Background reclaim of large deleted inodes: a queue of orphan drain
     * contexts processed one at a time on this worker (each drains its inode's
     * b+tree in bounded batches across transactions). */
    struct diskfs_drain        *drain_head, *drain_tail;
    int                         draining;

    /* Data-I/O admission control: the per-thread block queues have a bounded
     * submission ring, so a burst of concurrent (or heavily fragmented) reads
     * and writes can overrun it.  Requests that would exceed the in-flight cap
     * park here and are resumed from a block-I/O completion as capacity frees. */
    struct chimera_vfs_request *io_wait_head, *io_wait_tail;
};

/* ------------------------------------------------------------------ */
/* Async b+tree operation context                                      */
/* ------------------------------------------------------------------ */

#define DISKFS_BT_MAX_DEPTH 16

/*
 * Result callback for an async b+tree operation.  `result` carries the record
 * length (>= 0) or -1 (not found) for lookups, and 0/1 (not found / removed,
 * or inserted) for remove/insert.
 */
typedef void (*diskfs_bt_cb_t)(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);

enum diskfs_bt_opcode {
    DISKFS_BT_OP_LOOKUP_EXACT,
    DISKFS_BT_OP_LOOKUP_GE,
    DISKFS_BT_OP_LOOKUP_LE,
    DISKFS_BT_OP_INSERT,
    DISKFS_BT_OP_REMOVE,
};

enum diskfs_bt_phase {
    DISKFS_BT_PHASE_RESERVE,   /* INSERT: pre-reserve split space (suspendable) */
    DISKFS_BT_PHASE_DESCEND,
    DISKFS_BT_PHASE_WALK_NEXT,
    DISKFS_BT_PHASE_WALK_PREV,
    DISKFS_BT_PHASE_REBALANCE,
    DISKFS_BT_PHASE_COLLAPSE,
};

struct diskfs_bt_path_ent {
    uint64_t bptr;     /* node bptr (0 == embedded root, reached via the inode) */
    uint32_t base;
    int      ci;       /* child index descended into */
};

/*
 * One in-flight b+tree operation.  Holds all state needed to suspend the
 * traversal when a node is not resident (a block read is issued and the op is
 * parked on the block's waiter list) and resume it from the same point once
 * the block loads.  Allocated from a per-thread free list.
 */
struct diskfs_bt_op {
    struct diskfs_thread     *thread;
    struct diskfs_txn        *txn;
    struct diskfs_inode      *inode;
    enum diskfs_bt_opcode     opcode;
    enum diskfs_bt_phase      phase;
    struct diskfs_bt_key      key;

    /* insert payload (copied into op-owned storage so it survives suspension).
     * Most records (dirents, extents) fit recbuf; an oversized one (a long
     * symlink target) is staged in a heap buffer instead.  `rec` points at
     * whichever holds the payload and is freed in diskfs_bt_complete. */
    uint8_t                   recbuf[DISKFS_DIRENT_REC_MAX];
    uint8_t                  *rec;
    uint32_t                  reclen;

    /* lookup output (caller-owned, must outlive the op) */
    void                     *out;
    uint32_t                  out_cap;
    struct diskfs_bt_key     *r_key;
    struct diskfs_bt_key      found_key;   /* op-owned storage for r_key */

    /* traversal cursor */
    uint64_t                  cur_bptr;
    int                       use_root;

    /* descent path for insert split / remove rebalance */
    struct diskfs_bt_path_ent path[DISKFS_BT_MAX_DEPTH];
    int                       depth;
    int                       removed_idx;
    int                       reb_level;

    /* completion.  cb fires only when the op actually suspended (an I/O
     * deferred it); a fully-resident traversal completes inline and reports
     * via `done`/`result` so callers can iterate without recursing. */
    diskfs_bt_cb_t            cb;
    void                     *private_data;
    int                       suspended;
    int                       done;
    int                       result;

    /* Blocks pinned by this op's descent so eviction can't recycle a node
     * while we read it; released at completion.  Sized for the descent path
     * plus the siblings a remove-rebalance can fault at each level. */
    struct diskfs_block      *pins[DISKFS_BT_MAX_DEPTH * 4];
    int                       npins;

    /* block-waiter list / per-worker resume-queue linkage */
    struct diskfs_bt_op      *next;
};

static inline uint32_t
diskfs_inum_to_fh(
    struct diskfs_shared *shared,
    uint8_t              *fh,
    uint64_t              inum,
    uint32_t              gen)
{
    return chimera_vfs_encode_fh_inum_parent(shared->root_fh, inum, gen, fh);
} /* diskfs_inum_to_fh */

static inline void
diskfs_fh_to_inum(
    uint64_t      *inum,
    uint32_t      *gen,
    const uint8_t *fh,
    int            fhlen)
{
    chimera_vfs_decode_fh_inum(fh, fhlen, inum, gen);
} /* diskfs_fh_to_inum */

/* ------------------------------------------------------------------ */
/* Inode cache + logical (read/write) transaction locks                */
/* ------------------------------------------------------------------ */

#define DISKFS_INODE_MODE_FOR_TXN(txn) \
        ((txn)->type == DISKFS_TXN_WRITE ? DISKFS_INODE_LOCK_WRITE \
                                         : DISKFS_INODE_LOCK_READ)

static inline struct diskfs_inode_shard *
diskfs_inode_shard(
    struct diskfs_shared *shared,
    uint64_t              inum)
{
    return &shared->inode_cache->shards[inum & DISKFS_INODE_CACHE_MASK];
} /* diskfs_inode_shard */

static inline struct diskfs_inode_waiter *
diskfs_waiter_alloc(struct diskfs_thread *thread)
{
    struct diskfs_inode_waiter *w = thread->waiter_free_list;

    if (w) {
        thread->waiter_free_list = w->next;
    } else {
        w = malloc(sizeof(*w));
    }
    return w;
} /* diskfs_waiter_alloc */

static inline void
diskfs_waiter_free(
    struct diskfs_thread       *thread,
    struct diskfs_inode_waiter *w)
{
    w->next                  = thread->waiter_free_list;
    thread->waiter_free_list = w;
} /* diskfs_waiter_free */

/*
 * A continuation parked on a block's LOADING waiter list.  Generalizes the
 * wait list so any caller -- a b+tree op, an inode-block pin, the space-map
 * allocator -- can suspend on a block read and be resumed: when the read
 * completes, each waiter is dispatched to its owning worker's resume queue,
 * which invokes resume(thread, arg).  Pooled per-thread; a waiter is always
 * allocated and freed on its owning worker, so the pool stays thread-local
 * even though the dispatch crosses workers.
 */
struct diskfs_block_waiter {
    struct diskfs_thread       *thread; /* worker to resume on (owns the pool) */
    void                        (*resume)(
        struct diskfs_thread *thread,
        void                 *arg);
    void                       *arg;
    struct diskfs_block_waiter *next;
};

static inline struct diskfs_block_waiter *
diskfs_block_waiter_alloc(struct diskfs_thread *thread)
{
    struct diskfs_block_waiter *w = thread->block_waiter_free_list;

    if (w) {
        thread->block_waiter_free_list = w->next;
    } else {
        w = malloc(sizeof(*w));
    }
    w->next = NULL;
    return w;
} /* diskfs_block_waiter_alloc */

static inline void
diskfs_block_waiter_free(
    struct diskfs_thread       *thread,
    struct diskfs_block_waiter *w)
{
    w->next                        = thread->block_waiter_free_list;
    thread->block_waiter_free_list = w;
} /* diskfs_block_waiter_free */

/* Record a held inode in the transaction's fixed slot array. */
static inline void
diskfs_txn_add_slot(
    struct diskfs_txn          *txn,
    struct diskfs_inode        *inode,
    enum diskfs_inode_lock_mode mode)
{
    chimera_diskfs_abort_if(txn->num_inodes >= DISKFS_TXN_MAX_INODES,
                            "diskfs txn inode slots exhausted");
    txn->inodes[txn->num_inodes].inode = inode;
    txn->inodes[txn->num_inodes].mode  = mode;
    txn->num_inodes++;
} /* diskfs_txn_add_slot */

/* Inode LRU (recycle candidates).  All require the owning shard lock. */
static inline void
diskfs_inode_lru_push_tail(
    struct diskfs_inode_shard *shard,
    struct diskfs_inode       *inode)
{
    inode->lru_prev = shard->lru_tail;
    inode->lru_next = NULL;
    if (shard->lru_tail) {
        shard->lru_tail->lru_next = inode;
    } else {
        shard->lru_head = inode;
    }
    shard->lru_tail = inode;
    inode->on_lru   = 1;
} /* diskfs_inode_lru_push_tail */

static inline void
diskfs_inode_lru_unlink(
    struct diskfs_inode_shard *shard,
    struct diskfs_inode       *inode)
{
    if (!inode->on_lru) {
        return;
    }
    if (inode->lru_prev) {
        inode->lru_prev->lru_next = inode->lru_next;
    } else {
        shard->lru_head = inode->lru_next;
    }
    if (inode->lru_next) {
        inode->lru_next->lru_prev = inode->lru_prev;
    } else {
        shard->lru_tail = inode->lru_prev;
    }
    inode->lru_prev = inode->lru_next = NULL;
    inode->on_lru   = 0;
} /* diskfs_inode_lru_unlink */

/*
 * An idle inode is a recycle candidate: no open handle (refcnt == 1, the
 * cache's own reference), not locked, and live (nlink > 0).  Whether its
 * dinode is durable enough to drop is checked separately at recycle time.
 * Caller holds the shard lock.
 */
static inline int
diskfs_inode_idle(const struct diskfs_inode *inode)
{
    return inode->refcnt == 1 && inode->readers == 0 &&
           inode->writer == 0 && inode->nlink > 0;
} /* diskfs_inode_idle */

/* Caller must hold the inode's shard lock. */
static inline int
diskfs_inode_lock_compatible(
    struct diskfs_inode        *inode,
    enum diskfs_inode_lock_mode mode)
{
    if (mode == DISKFS_INODE_LOCK_WRITE) {
        return inode->writer == 0 && inode->readers == 0;
    }
    return inode->writer == 0;
} /* diskfs_inode_lock_compatible */

/* Caller must hold the inode's shard lock. */
static inline void
diskfs_inode_lock_grant(
    struct diskfs_inode        *inode,
    enum diskfs_inode_lock_mode mode)
{
    if (mode == DISKFS_INODE_LOCK_WRITE) {
        inode->writer = 1;
    } else {
        inode->readers++;
    }
} /* diskfs_inode_lock_grant */

/*
 * Hand a granted (or stale-failed) waiter to its owning worker so its
 * continuation runs back on the transaction's own thread.
 */
static void
diskfs_dispatch_grant(struct diskfs_inode_waiter *w)
{
    struct diskfs_thread *worker = w->txn->thread;

    pthread_mutex_lock(&worker->grant_lock);
    w->next = NULL;
    if (worker->grant_tail) {
        worker->grant_tail->next = w;
    } else {
        worker->grant_head = w;
    }
    worker->grant_tail = w;
    pthread_mutex_unlock(&worker->grant_lock);

    evpl_ring_doorbell(&worker->grant_doorbell);
} /* diskfs_dispatch_grant */

/*
 * Drop one held inode lock and grant the lock to compatible FIFO waiters.
 * Safe to call from any thread (worker for read/abort, intent-log thread
 * for write commit); granted waiters are dispatched to their own workers.
 */
static void
diskfs_inode_release_one(
    struct diskfs_thread       *thread,
    struct diskfs_inode        *inode,
    enum diskfs_inode_lock_mode mode)
{
    struct diskfs_inode_shard  *shard   = diskfs_inode_shard(thread->shared, inode->inum);
    struct diskfs_inode_waiter *granted = NULL;
    struct diskfs_inode_waiter *w;

    pthread_mutex_lock(&shard->lock);

    if (mode == DISKFS_INODE_LOCK_WRITE) {
        inode->writer = 0;
    } else {
        inode->readers--;
    }

    while (inode->wait_head) {
        w = inode->wait_head;

        if (w->gen != inode->gen) {
            /* The inode this waiter referenced was freed/replaced.  Fail
             * it with ENOENT rather than handing back a stale inode. */
            inode->wait_head = w->next;
            if (!inode->wait_head) {
                inode->wait_tail = NULL;
            }
            w->status = CHIMERA_VFS_ENOENT;
            w->next   = granted;
            granted   = w;
            continue;
        }

        if (!diskfs_inode_lock_compatible(inode, w->mode)) {
            break;
        }

        inode->wait_head = w->next;
        if (!inode->wait_head) {
            inode->wait_tail = NULL;
        }
        diskfs_inode_lock_grant(inode, w->mode);
        w->status = CHIMERA_VFS_OK;
        w->next   = granted;
        granted   = w;

        if (w->mode == DISKFS_INODE_LOCK_WRITE) {
            break;     /* exclusive: stop granting */
        }
    }

    /* If nobody re-took the lock and the inode is now idle, it becomes a
     * recycle candidate.  (Recycle re-checks evictability, so it's fine that
     * its dinode may not be durable yet.) */
    if (!inode->on_lru && diskfs_inode_idle(inode)) {
        diskfs_inode_lru_push_tail(shard, inode);
    }

    pthread_mutex_unlock(&shard->lock);

    while (granted) {
        w       = granted;
        granted = w->next;
        diskfs_dispatch_grant(w);
    }
} /* diskfs_inode_release_one */

/*
 * Release a single held inode early (readdir-style iteration that walks
 * many children but only needs to hold the current one).  No-op if the
 * inode isn't held by this txn.
 */
static inline void
diskfs_txn_unlock_inode(
    struct diskfs_txn   *txn,
    struct diskfs_inode *inode)
{
    int i;

    for (i = 0; i < txn->num_inodes; i++) {
        if (txn->inodes[i].inode == inode) {
            enum diskfs_inode_lock_mode mode = txn->inodes[i].mode;

            txn->inodes[i] = txn->inodes[txn->num_inodes - 1];
            txn->num_inodes--;
            diskfs_inode_release_one(txn->thread, inode, mode);
            return;
        }
    }
} /* diskfs_txn_unlock_inode */

/*
 * Release every inode held by this txn.  Called by the worker thread for
 * read-txn commits and aborts, and by the intent-log thread for write-txn
 * commits (before it pushes the CQE).
 */
static inline void
diskfs_txn_unlock_all(struct diskfs_txn *txn)
{
    int i;

    for (i = 0; i < txn->num_inodes; i++) {
        diskfs_inode_release_one(txn->thread, txn->inodes[i].inode,
                                 txn->inodes[i].mode);
    }
    txn->num_inodes = 0;
} /* diskfs_txn_unlock_all */

/*
 * Acquire an inode in the given mode for a transaction.
 *   1. already held by this txn -> reuse
 *   2. in the cache -> grant if compatible, else park a waiter
 *   3. not cached -> ENOENT (no disk fetch yet; everything created so far
 *      is resident since we never drop or flush)
 * The callback fires immediately on the calling thread for cases 1/2-grant
 * and 3, or later (via the grant doorbell, on this txn's worker) once a
 * conflicting holder releases.
 */
static void diskfs_inode_load(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    uint64_t                    inum,
    uint32_t                    gen,
    enum diskfs_inode_lock_mode mode,
    diskfs_inode_cb_t           cb,
    void                       *private_data);

/* Ensure a write-locked inode's home block is resident+pinned+attached (via
 * the async block path) then fire cb; used at the two write-lock grant points
 * so every later pin_inode_block on this inode is a resident no-op. */
static void diskfs_inode_finish_write_pin(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode,
    diskfs_inode_cb_t     cb,
    void                 *private_data);

static void diskfs_pin_cont_resume(
    struct diskfs_thread *thread,
    void                 *arg);

static void
diskfs_inode_acquire(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    uint64_t                    inum,
    uint32_t                    gen,
    enum diskfs_inode_lock_mode mode,
    diskfs_inode_cb_t           cb,
    void                       *private_data)
{
    struct diskfs_inode_shard  *shard;
    struct diskfs_inode        *inode;
    struct diskfs_inode_waiter *w;
    int                         i;

    for (i = 0; i < txn->num_inodes; i++) {
        inode = txn->inodes[i].inode;
        if (inode->inum == inum) {
            if (unlikely(inode->gen != gen)) {
                cb(NULL, CHIMERA_VFS_ENOENT, private_data);
            } else {
                cb(inode, CHIMERA_VFS_OK, private_data);
            }
            return;
        }
    }

    shard = diskfs_inode_shard(thread->shared, inum);
    pthread_mutex_lock(&shard->lock);

    rb_tree_query_exact(&shard->inodes, inum, inum, inode);

    if (unlikely(inode && inode->gen != gen)) {
        /* Cached under a different generation: the handle is stale. */
        pthread_mutex_unlock(&shard->lock);
        cb(NULL, CHIMERA_VFS_ENOENT, private_data);
        return;
    }

    if (unlikely(!inode)) {
        /* Not resident: either evicted (its dinode is durably home -- eviction
         * only drops CLEAN inodes) or genuinely absent.  Fault it in from disk
         * whenever the inum is within allocated space; the on-disk dinode read
         * validates inum/gen/nlink and yields ENOENT if it isn't really there.
         * (This must not gate on `mounted` -- a freshly-formatted FS evicts
         * too, so a miss is not necessarily ENOENT.) */
        pthread_mutex_unlock(&shard->lock);
        if (sm_inum_valid(thread->shared->space_map, inum)) {
            diskfs_inode_load(thread, txn, inum, gen, mode, cb, private_data);
        } else {
            cb(NULL, CHIMERA_VFS_ENOENT, private_data);
        }
        return;
    }

    if (diskfs_inode_lock_compatible(inode, mode)) {
        diskfs_inode_lock_grant(inode, mode);
        diskfs_inode_lru_unlink(shard, inode);     /* busy now, not a candidate */
        pthread_mutex_unlock(&shard->lock);
        diskfs_txn_add_slot(txn, inode, mode);
        if (mode == DISKFS_INODE_LOCK_WRITE) {
            /* Pin the home block before reporting the grant; may async-load it
             * (and defer cb) if it was evicted while the inode stayed cached. */
            diskfs_inode_finish_write_pin(thread, txn, inode, cb, private_data);
        } else {
            cb(inode, CHIMERA_VFS_OK, private_data);
        }
        return;
    }

    w               = diskfs_waiter_alloc(thread);
    w->txn          = txn;
    w->mode         = mode;
    w->gen          = gen;
    w->cb           = cb;
    w->private_data = private_data;
    w->inode        = inode;
    w->status       = CHIMERA_VFS_OK;
    w->next         = NULL;

    if (inode->wait_tail) {
        inode->wait_tail->next = w;
    } else {
        inode->wait_head = w;
    }
    inode->wait_tail = w;

    pthread_mutex_unlock(&shard->lock);
} /* diskfs_inode_acquire */

static inline void
diskfs_inode_get_inum_async(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    uint64_t              inum,
    uint32_t              gen,
    diskfs_inode_cb_t     cb,
    void                 *private_data)
{
    diskfs_inode_acquire(thread, txn, inum, gen,
                         DISKFS_INODE_MODE_FOR_TXN(txn), cb, private_data);
} /* diskfs_inode_get_inum_async */

static inline void
diskfs_inode_get_fh_async(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    const uint8_t        *fh,
    int                   fhlen,
    diskfs_inode_cb_t     cb,
    void                 *private_data)
{
    uint64_t inum;
    uint32_t gen;

    diskfs_fh_to_inum(&inum, &gen, fh, fhlen);
    diskfs_inode_get_inum_async(thread, txn, inum, gen, cb, private_data);
} /* diskfs_inode_get_fh_async */

/*
 * Fault an inode in (mount-time orphan recovery + path walk on a remounted
 * FS).  Returns it from cache if resident; otherwise reads the dinode block
 * through the mount-time evpl pump `io` (VFIO-safe) and seeds the cache.  Runs
 * single-threaded at mount, so the blocking pump is fine.  Returns a populated,
 * cache-inserted inode or NULL.
 */
static struct diskfs_block * diskfs_block_claim(
    struct diskfs_thread *thread,
    uint32_t              device_id,
    uint64_t              device_offset,
    int                   is_new);

/* Decrement a block's pin and set its new state; if it becomes CLEAN and
 * unpinned it joins the shard LRU as an eviction candidate. */
static void diskfs_block_unpin(
    struct diskfs_thread   *thread,
    struct diskfs_block    *blk,
    enum diskfs_block_state new_state);

/* Mount-time evpl-pump block I/O (defined with the recovery helpers); the
 * mount-time orphan recovery + path-walk fault paths read through it so they
 * reach a VFIO/io_uring/libaio device rather than pread'ing a device path. */
struct diskfs_mount_io;
static struct diskfs_mount_io * diskfs_mount_io_open(
    struct diskfs_shared *shared);
static void diskfs_mount_io_close(
    struct diskfs_mount_io *io);
static int diskfs_mount_io_read(
    void    *user,
    uint32_t device_id,
    void    *buf,
    uint64_t length,
    uint64_t offset);

/* Defined with the block-cache helpers it consults; used by the fault paths. */
static void diskfs_inode_cache_recycle_locked(
    struct diskfs_shared      *shared,
    struct diskfs_inode_shard *shard);

static struct diskfs_inode *
diskfs_inode_load_sync(
    struct diskfs_thread   *thread,
    struct diskfs_mount_io *io,
    uint64_t                inum,
    uint32_t                gen,
    int                     allow_orphan)
{
    struct diskfs_shared      *shared = thread->shared;
    struct diskfs_inode_shard *shard  = diskfs_inode_shard(shared, inum);
    struct diskfs_inode       *inode;
    struct diskfs_dinode      *di;
    uint8_t                    buf[DISKFS_BLOCK_SIZE];
    uint32_t                   dev;
    uint64_t                   off;

    if (!sm_inum_valid(shared->space_map, inum)) {
        return NULL;
    }

    /* Already resident (e.g. a freshly-bootstrapped root/orphan inode, or a
     * prior fault): return it without touching disk. */
    pthread_mutex_lock(&shard->lock);
    rb_tree_query_exact(&shard->inodes, inum, inum, inode);
    if (inode) {
        int ok = (inode->gen == gen && (inode->nlink != 0 || allow_orphan));

        pthread_mutex_unlock(&shard->lock);
        return ok ? inode : NULL;
    }
    pthread_mutex_unlock(&shard->lock);

    /* Not cached: read the dinode block from disk through the mount-time pump
     * (VFIO-safe).  Safe to read the on-disk image directly -- an inode whose
     * struct is not cached has no in-flight dirty block. */
    off = sm_inum_to_device_offset(shared->space_map, inum, &dev);
    if (diskfs_mount_io_read(io, dev, buf, sizeof(buf), off) != 0) {
        return NULL;
    }

    di = (struct diskfs_dinode *) buf;
    if (di->inum != inum || di->gen != gen ||
        (di->nlink == 0 && !allow_orphan)) {
        return NULL;
    }

    pthread_mutex_lock(&shard->lock);
    rb_tree_query_exact(&shard->inodes, inum, inum, inode);
    if (!inode) {
        diskfs_inode_cache_recycle_locked(shared, shard);
        inode              = calloc(1, sizeof(*inode));
        inode->inum        = inum;
        inode->refcnt      = 1;
        inode->gen         = di->gen;
        inode->mode        = di->mode;
        inode->nlink       = di->nlink;
        inode->uid         = di->uid;
        inode->gid         = di->gid;
        inode->rdev        = di->rdev;
        inode->size        = di->size;
        inode->space_used  = di->space_used;
        inode->atime_sec   = di->atime_sec;
        inode->atime_nsec  = di->atime_nsec;
        inode->mtime_sec   = di->mtime_sec;
        inode->mtime_nsec  = di->mtime_nsec;
        inode->ctime_sec   = di->ctime_sec;
        inode->ctime_nsec  = di->ctime_nsec;
        inode->parent_inum = di->parent_inum;
        inode->parent_gen  = di->parent_gen;
        rb_tree_insert(&shard->inodes, inum, inode);
        shard->ninodes++;
    }
    pthread_mutex_unlock(&shard->lock);

    /* Seed the inode's home block into the block cache from the disk image we
     * just read.  Claim is_new (no read-back): we overwrite the whole block. */
    {
        struct diskfs_block *blk = diskfs_block_claim(thread, dev, off, 1);

        memcpy(blk->iov.data, buf, DISKFS_BLOCK_SIZE);
        diskfs_block_unpin(thread, blk, DISKFS_BLOCK_CLEAN);
    }
    return inode;
} /* diskfs_inode_load_sync */

/* ------------------------------------------------------------------ */
/* Block cache                                                         */
/* ------------------------------------------------------------------ */

static inline uint64_t
diskfs_block_hash(
    uint32_t device_id,
    uint64_t device_offset)
{
    uint64_t h = device_offset >> DISKFS_BLOCK_SHIFT;

    h ^= (uint64_t) device_id * 0x9e3779b97f4a7c15ULL;
    h ^= h >> 29;
    h *= 0xbf58476d1ce4e5b9ULL;
    h ^= h >> 32;
    return h;
} /* diskfs_block_hash */

static inline struct diskfs_block_shard *
diskfs_block_shard(
    struct diskfs_block_cache *cache,
    uint32_t                   device_id,
    uint64_t                   device_offset)
{
    uint64_t hash = diskfs_block_hash(device_id, device_offset);

    return &cache->shards[hash & DISKFS_BLOCK_CACHE_SHARD_MASK];
} /* diskfs_block_shard */

static inline uint32_t
diskfs_block_bucket(
    uint32_t device_id,
    uint64_t device_offset)
{
    uint64_t hash = diskfs_block_hash(device_id, device_offset);

    return (hash >> 8) & DISKFS_BLOCK_CACHE_BUCKET_MASK;
} /* diskfs_block_bucket */

/* --- shard LRU (caller holds the shard lock) --------------------------- */

static inline void
diskfs_block_lru_push_tail(
    struct diskfs_block_shard *shard,
    struct diskfs_block       *blk)
{
    blk->lru_prev = shard->lru_tail;
    blk->lru_next = NULL;
    if (shard->lru_tail) {
        shard->lru_tail->lru_next = blk;
    } else {
        shard->lru_head = blk;
    }
    shard->lru_tail = blk;
    blk->on_lru     = 1;
} /* diskfs_block_lru_push_tail */

static inline void
diskfs_block_lru_unlink(
    struct diskfs_block_shard *shard,
    struct diskfs_block       *blk)
{
    if (!blk->on_lru) {
        return;
    }
    if (blk->lru_prev) {
        blk->lru_prev->lru_next = blk->lru_next;
    } else {
        shard->lru_head = blk->lru_next;
    }
    if (blk->lru_next) {
        blk->lru_next->lru_prev = blk->lru_prev;
    } else {
        shard->lru_tail = blk->lru_prev;
    }
    blk->lru_prev = blk->lru_next = NULL;
    blk->on_lru   = 0;
} /* diskfs_block_lru_unlink */

/*
 * Recycle the least-recently-used CLEAN, unpinned buffer for reuse at a new
 * key.  Caller holds the shard lock.  Returns a buffer with pin_count 0,
 * unlinked from the LRU and removed from its old bucket (a no-op for a free,
 * never-keyed buffer); the caller sets the new key/state and links it into the
 * new bucket.
 *
 * The pool is fixed and never grows or blocks: the cache is provisioned larger
 * than the maximum pinnable set (bounded by the intent log -- see the cache
 * sizing constants), so by the pigeonhole principle the LRU is never empty.
 * An empty LRU means every buffer in this shard is pinned -- a provisioning
 * violation or a leaked pin (and, since the descent that called us holds pins
 * in this shard, the precise self-deadlock condition).  Abort loudly rather
 * than block and hang.
 */
static struct diskfs_block *
diskfs_block_recycle(struct diskfs_block_shard *shard)
{
    struct diskfs_block *blk = shard->lru_head;
    struct diskfs_block *cur, *prev;
    uint32_t             ob;

    chimera_diskfs_abort_if(!blk,
                            "block cache shard exhausted: every buffer pinned "
                            "(raise block_cache_blocks above the intent-log size, "
                            "or a pin was leaked)");

    diskfs_block_lru_unlink(shard, blk);

    /* Unhook from its current bucket (no-op for a never-keyed free buffer:
     * it is in no chain, so the pointer search simply finds nothing). */
    ob   = diskfs_block_bucket(blk->device_id, blk->device_offset);
    prev = NULL;
    for (cur = shard->buckets[ob]; cur; prev = cur, cur = cur->hash_next) {
        if (cur == blk) {
            if (prev) {
                prev->hash_next = cur->hash_next;
            } else {
                shard->buckets[ob] = cur->hash_next;
            }
            break;
        }
    }
    blk->hash_next = NULL;
    return blk;
} /* diskfs_block_recycle */

static void
diskfs_block_unpin(
    struct diskfs_thread   *thread,
    struct diskfs_block    *blk,
    enum diskfs_block_state new_state)
{
    struct diskfs_block_shard *shard = diskfs_block_shard(thread->shared->block_cache,
                                                          blk->device_id, blk->device_offset);

    pthread_mutex_lock(&shard->lock);
    blk->state = new_state;
    if (--blk->pin_count == 0 && blk->state == DISKFS_BLOCK_CLEAN && !blk->on_lru) {
        diskfs_block_lru_push_tail(shard, blk);
    }
    pthread_mutex_unlock(&shard->lock);
} /* diskfs_block_unpin */

/* Release a descent pin without changing the block's state. */
static void
diskfs_block_release(
    struct diskfs_thread *thread,
    struct diskfs_block  *blk)
{
    struct diskfs_block_shard *shard = diskfs_block_shard(thread->shared->block_cache,
                                                          blk->device_id, blk->device_offset);

    pthread_mutex_lock(&shard->lock);
    if (--blk->pin_count == 0 && blk->state == DISKFS_BLOCK_CLEAN && !blk->on_lru) {
        diskfs_block_lru_push_tail(shard, blk);
    }
    pthread_mutex_unlock(&shard->lock);
} /* diskfs_block_release */

static void
diskfs_block_cache_create(struct diskfs_shared *shared)
{
    struct diskfs_block_cache *cache = calloc(1, sizeof(*cache));
    uint32_t                   total = shared->block_cache_blocks ?
        shared->block_cache_blocks : DISKFS_BLOCK_CACHE_DEFAULT_BLOCKS;
    int                        i;
    uint32_t                   j;

    /* The pool never grows or blocks, so it must clear the maximum pinnable
     * set; floor an under-sized configuration rather than risk the recycle
     * abort under load. */
    if (total < DISKFS_BLOCK_CACHE_MIN_BLOCKS) {
        total = DISKFS_BLOCK_CACHE_MIN_BLOCKS;
    }

    cache->shard_cap = total / DISKFS_BLOCK_CACHE_SHARDS;
    if (cache->shard_cap == 0) {
        cache->shard_cap = 1;
    }

    for (i = 0; i < DISKFS_BLOCK_CACHE_SHARDS; i++) {
        struct diskfs_block_shard *shard = &cache->shards[i];

        pthread_mutex_init(&shard->lock, NULL);
        shard->buckets = calloc(DISKFS_BLOCK_CACHE_BUCKETS_PER_SHARD,
                                sizeof(struct diskfs_block *));
        shard->pool = calloc(cache->shard_cap, sizeof(struct diskfs_block));

        /* Pre-populate the struct pool: every block starts free (unkeyed, in
         * no bucket) and CLEAN on the LRU, with no buffer yet (iov.data NULL);
         * the iovec is allocated on first use and reused thereafter. */
        for (j = 0; j < cache->shard_cap; j++) {
            struct diskfs_block *blk = &shard->pool[j];

            blk->state = DISKFS_BLOCK_CLEAN;
            diskfs_block_lru_push_tail(shard, blk);
            shard->nblocks++;
        }
    }
    shared->block_cache = cache;
} /* diskfs_block_cache_create */

static void
diskfs_block_cache_destroy(struct diskfs_shared *shared)
{
    struct diskfs_block_cache *cache = shared->block_cache;
    int                        i;

    if (!cache) {
        return;
    }

    for (i = 0; i < DISKFS_BLOCK_CACHE_SHARDS; i++) {
        struct diskfs_block_shard *shard = &cache->shards[i];
        uint32_t                   j;

        /* Release each block's buffer (NULL evpl -> straight to the global
         * allocator, which is correct at teardown); the structs are one pool
         * array.  At a clean unmount every block is CLEAN (refcount 1). */
        for (j = 0; j < shard->nblocks; j++) {
            if (shard->pool[j].iov.data) {
                evpl_iovec_release(NULL, &shard->pool[j].iov);
            }
        }
        free(shard->pool);
        free(shard->buckets);
        pthread_mutex_destroy(&shard->lock);
    }
    free(cache);
    shared->block_cache = NULL;
} /* diskfs_block_cache_destroy */

/*
 * Find a block already resident in the cache.  Lock-free RCU read; the
 * caller must be inside an rcu read-side critical section (or hold no
 * concurrent eviction risk, as in Stage 1/2 where blocks are never freed).
 */
static inline struct diskfs_block *
diskfs_block_lookup_locked(
    struct diskfs_block_shard *shard,
    uint32_t                   bucket,
    uint32_t                   device_id,
    uint64_t                   device_offset)
{
    struct diskfs_block *blk;

    for (blk = shard->buckets[bucket]; blk; blk = blk->hash_next) {
        if (blk->device_id == device_id && blk->device_offset == device_offset) {
            return blk;
        }
    }
    return NULL;
} /* diskfs_block_lookup_locked */

/*
 * Ensure a block has a backing buffer.  Buffers are SHARED evpl iovecs so the
 * intent logger and tail-pusher can reference them zero-copy for I/O (and RDMA)
 * and hand the reference across threads.  A recycled block keeps (reuses) its
 * iovec -- a CLEAN block's buffer is referenced only by the cache (refcount 1)
 * -- so an allocation happens only on a never-yet-used pool slot (and on COW).
 */
static inline void
diskfs_block_ensure_iov(
    struct diskfs_thread *thread,
    struct diskfs_block  *blk)
{
    if (!blk->iov.data) {
        evpl_iovec_alloc(thread->evpl, DISKFS_BLOCK_SIZE, DISKFS_BLOCK_SIZE, 1,
                         EVPL_IOVEC_FLAG_SHARED, &blk->iov);
    }
} /* diskfs_block_ensure_iov */

/*
 * Find or create the cache entry for (device_id, device_offset) and pin it.
 * On a miss a buffer is obtained from the shard pool (recycling the LRU
 * eviction candidate): is_new (a freshly space-map-allocated block) starts
 * zeroed; otherwise -- always, now that eviction can discard a resident CLEAN
 * block whose content is already home -- the buffer is repopulated from disk so
 * a re-claimed evicted block keeps its contents.  A hit unlinks the block from
 * the LRU (it is now pinned, not a candidate).
 */
static struct diskfs_block *
diskfs_block_claim(
    struct diskfs_thread *thread,
    uint32_t              device_id,
    uint64_t              device_offset,
    int                   is_new)
{
    struct diskfs_block_cache *cache  = thread->shared->block_cache;
    uint64_t                   hash   = diskfs_block_hash(device_id, device_offset);
    uint32_t                   sidx   = hash & DISKFS_BLOCK_CACHE_SHARD_MASK;
    uint32_t                   bucket = (hash >> 8) & DISKFS_BLOCK_CACHE_BUCKET_MASK;
    struct diskfs_block_shard *shard  = &cache->shards[sidx];
    struct diskfs_block       *blk;

    pthread_mutex_lock(&shard->lock);

    blk = diskfs_block_lookup_locked(shard, bucket, device_id, device_offset);
    if (!blk) {
        blk                = diskfs_block_recycle(shard);
        blk->device_id     = device_id;
        blk->device_offset = device_offset;
        blk->state         = DISKFS_BLOCK_CLEAN;
        blk->seq           = 0;
        blk->wait_head     = NULL;
        blk->wait_tail     = NULL;
        diskfs_block_ensure_iov(thread, blk);

        /* is_new starts from a zeroed buffer.  A non-resident is_new==0 claim
         * would need a synchronous disk read, which no longer happens: every
         * such caller either finds the block pinned/resident (inode-block pin
         * after the write-lock grant pre-faults it; b+tree modify nodes faulted
         * + pinned by bt_run) or goes through the async path (diskfs_block_
         * claim_async) / the mount-time pump.  A miss here is therefore a bug. */
        chimera_diskfs_abort_if(!is_new,
                                "synchronous block_claim miss off=%lu -- "
                                "caller must pre-fault or use the async path",
                                device_offset);
        memset(blk->iov.data, 0, DISKFS_BLOCK_SIZE);

        blk->hash_next         = shard->buckets[bucket];
        shard->buckets[bucket] = blk;
    } else if (blk->on_lru) {
        diskfs_block_lru_unlink(shard, blk);
    } else if (blk->state == DISKFS_BLOCK_LOGGED) {
        /* COW: this buffer is still referenced by an un-pushed redo record (and
         * the tail-pusher will write it home), so it must stay immutable.  Fork
         * a private writable copy; the old buffer rides the record to its home
         * and is freed when the pusher releases it.  Done under the shard lock
         * so it serializes against the pusher's LOGGED->CLEAN transition. */
        struct evpl_iovec nv;

        evpl_iovec_alloc(thread->evpl, DISKFS_BLOCK_SIZE, DISKFS_BLOCK_SIZE, 1,
                         EVPL_IOVEC_FLAG_SHARED, &nv);
        memcpy(nv.data, blk->iov.data, DISKFS_BLOCK_SIZE);
        evpl_iovec_release(thread->evpl, &blk->iov);
        evpl_iovec_move(&blk->iov, &nv);
        blk->state = DISKFS_BLOCK_CLEAN;
    }

    blk->pin_count++;
    pthread_mutex_unlock(&shard->lock);

    return blk;
} /* diskfs_block_claim */

/*
 * txn_block link nodes are allocated on the worker (when a block is pinned)
 * but freed on the intent-log thread (when the txn's blocks are unpinned),
 * so they use plain malloc/free rather than a per-thread free list.
 */
static inline void
diskfs_txn_add_block(
    struct diskfs_txn   *txn,
    struct diskfs_block *block)
{
    struct diskfs_txn_block *tb = malloc(sizeof(*tb));

    tb->block   = block;
    tb->next    = txn->blocks;
    txn->blocks = tb;
} /* diskfs_txn_add_block */

/*
 * Journal bridge for the space-map allocation log: claim the AG-log block and
 * pin it into the allocating transaction so the delta written into it rides
 * the main redo log (durable + replayed on crash).  Passed to space_map_alloc/
 * free as a struct sm_journal.
 */
struct diskfs_sm_jnl {
    struct diskfs_thread *thread;
    struct diskfs_txn    *txn;
    /* Continuation re-driving the journaling operation if a log-block claim
     * must wait for a read.  Runs on `thread` once the block loads. */
    void                  (*resume)(
        struct diskfs_thread *thread,
        void                 *arg);
    void                 *resume_arg;
};

static struct diskfs_block * diskfs_block_claim_async(
    struct diskfs_thread *thread,
    uint32_t device_id,
    uint64_t device_offset,
    int is_new,
    void ( *resume )(struct diskfs_thread *, void *),
    void *arg);

static void *
diskfs_sm_claim_block(
    void    *user,
    uint32_t device_id,
    uint64_t device_offset,
    int      is_new)
{
    struct diskfs_sm_jnl *c = user;
    struct diskfs_block  *blk;

    /* Route the journal-block claim through the async path: a not-resident log
     * block parks this journaling op's continuation (c->resume) and issues the
     * read, returning NULL so the allocator unwinds with SM_AGAIN and the
     * caller re-drives once it loads. */
    blk = diskfs_block_claim_async(c->thread, device_id, device_offset, is_new,
                                   c->resume, c->resume_arg);
    if (!blk) {
        return NULL;
    }

    diskfs_txn_add_block(c->txn, blk);
    return blk->iov.data;
} /* diskfs_sm_claim_block */

#define DISKFS_SM_JNL(name, thr, t, res, a)                                  \
        struct diskfs_sm_jnl name ## _ctx = { (thr), (t), (res), (a) };      \
        struct sm_journal    name         = { diskfs_sm_claim_block, &name ## _ctx }

/* A journaling context for a site that has guaranteed its log blocks are
 * resident (e.g. a pre-reserved b+tree modify): a claim must never park, so a
 * suspend here is a bug. */
static void
diskfs_sm_no_suspend(
    struct diskfs_thread *thread,
    void                 *arg)
{
    (void) thread;
    (void) arg;
    chimera_diskfs_abort("space-map journal parked where it must not suspend");
} /* diskfs_sm_no_suspend */

/*
 * Free a device range as part of a transaction.  The FREE delta is journaled
 * now (it rides this txn's redo), but the in-memory free is deferred onto the
 * txn's pending list and applied only once the record is durable
 * (diskfs_txn_apply_frees) or discarded on abort (diskfs_txn_discard_frees).
 * This is required for metadata blocks (b+tree nodes), which unlike file data
 * are resident + pinned in the block cache: applying the free immediately
 * could hand the range to a concurrent allocation that then claims the stale,
 * still-pinned block.  Deferring to commit (block is LOGGED, unpinned by then)
 * makes a re-claim COW cleanly.
 */
static void
diskfs_txn_free_space(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    uint32_t              device_id,
    uint64_t              device_offset,
    uint64_t              length)
{
    struct diskfs_txn_free *f;

    (void) thread;

    /* Record the pending free only -- the FREE delta is journaled later, in the
     * suspendable pre-commit flush (diskfs_txn_flush_free_journals).  This is
     * because diskfs_txn_free_space runs inside the b+tree synchronous modify
     * (merge frees the emptied sibling, teardown frees nodes), which cannot
     * suspend on a cold journal block.  The in-memory free stays deferred to
     * commit (diskfs_txn_apply_frees). */
    f                  = malloc(sizeof(*f));
    f->device_id       = device_id;
    f->device_offset   = device_offset;
    f->length          = length;
    f->journaled       = 0;
    f->next            = txn->pending_frees;
    txn->pending_frees = f;
} /* diskfs_txn_free_space */

/* Carried across a suspended commit: the pre-commit free-journal flush can
 * park on a cold log block, so commit defers and resumes via this. */
struct diskfs_commit_ctx {
    struct diskfs_txn     *txn;
    diskfs_txn_commit_cb_t cb;
    void                  *private_data;
};

static void diskfs_commit_resume(
    struct diskfs_thread *thread,
    void                 *arg);

/*
 * Journal the FREE delta for every pending free recorded during the op's
 * modify.  A suspendable pre-commit phase (run on the worker before the txn is
 * handed to the intent-log thread): the delta write goes through the async
 * journal claim, so on a not-resident log block this parks the request (with
 * diskfs_commit_resume re-driving the commit) and returns SM_AGAIN.  Already-
 * journaled frees are flagged so a resumed flush continues where it left off.
 * Returns 0 when all frees are journaled.
 */
static int
diskfs_txn_flush_free_journals(
    struct diskfs_thread     *thread,
    struct diskfs_txn        *txn,
    struct diskfs_commit_ctx *cctx)
{
    struct diskfs_txn_free *f;

    DISKFS_SM_JNL(jnl, thread, txn, diskfs_commit_resume, cctx);

    for (f = txn->pending_frees; f; f = f->next) {
        if (f->journaled) {
            continue;
        }
        if (space_map_free_journal(thread->shared->space_map, &jnl,
                                   f->device_id, f->device_offset,
                                   f->length) == SM_AGAIN) {
            return SM_AGAIN;
        }
        f->journaled = 1;
    }
    return 0;
} /* diskfs_txn_flush_free_journals */

/* Commit a txn's pending frees -- the ranges become reusable.  Runs on the
 * intent-log thread once the redo record is durable, after the txn's blocks
 * have been unpinned (so a freed metadata block is LOGGED, not DIRTY-pinned). */
static void
diskfs_txn_apply_frees(struct diskfs_txn *txn)
{
    struct space_map       *sm = txn->thread->shared->space_map;
    struct diskfs_txn_free *f, *n;

    for (f = txn->pending_frees; f; f = n) {
        n = f->next;
        space_map_free_apply(sm, f->device_id, f->device_offset, f->length);
        free(f);
    }
    txn->pending_frees = NULL;
} /* diskfs_txn_apply_frees */

/* Discard a txn's pending frees without applying (abort): the journaled FREE
 * deltas never become durable, so the ranges stay allocated. */
static void
diskfs_txn_discard_frees(struct diskfs_txn *txn)
{
    struct diskfs_txn_free *f, *n;

    for (f = txn->pending_frees; f; f = n) {
        n = f->next;
        free(f);
    }
    txn->pending_frees = NULL;
} /* diskfs_txn_discard_frees */

/* ------------------------------------------------------------------ */
/* Async block fetch with per-op suspend/resume                        */
/* ------------------------------------------------------------------ */

static void diskfs_bt_run(
    struct diskfs_bt_op *op);

static inline struct diskfs_bt_op *
diskfs_bt_op_alloc(struct diskfs_thread *thread)
{
    struct diskfs_bt_op *op = thread->bt_op_free_list;

    if (op) {
        thread->bt_op_free_list = op->next;
    } else {
        op = calloc(1, sizeof(*op));
    }
    op->next = NULL;
    return op;
} /* diskfs_bt_op_alloc */

static inline void
diskfs_bt_op_free(
    struct diskfs_thread *thread,
    struct diskfs_bt_op  *op)
{
    op->next                = thread->bt_op_free_list;
    thread->bt_op_free_list = op;
} /* diskfs_bt_op_free */

/*
 * Enqueue a ready waiter on its owning worker's resume queue.  If the waking
 * thread is the waiter's own worker, schedule a deferral (no eventfd);
 * otherwise ring the cross-thread doorbell.
 */
static void
diskfs_block_waiter_dispatch(
    struct diskfs_thread       *waker,
    struct diskfs_block_waiter *w)
{
    struct diskfs_thread *worker = w->thread;

    pthread_mutex_lock(&worker->resume_lock);
    w->next = NULL;
    if (worker->resume_tail) {
        worker->resume_tail->next = w;
    } else {
        worker->resume_head = w;
    }
    worker->resume_tail = w;
    pthread_mutex_unlock(&worker->resume_lock);

    if (worker == waker) {
        evpl_defer(worker->evpl, &worker->resume_deferral);
    } else {
        evpl_ring_doorbell(&worker->resume_doorbell);
    }
} /* diskfs_block_waiter_dispatch */

/* Resume trampoline for a parked b+tree op: re-enter its driver. */
static void
diskfs_bt_op_resume_cb(
    struct diskfs_thread *thread,
    void                 *arg)
{
    (void) thread;
    diskfs_bt_run((struct diskfs_bt_op *) arg);
} /* diskfs_bt_op_resume_cb */

/* Drain this worker's resume queue, invoking each ready waiter's continuation
 * (which re-enters its driver / request step), then recycling the waiter. */
static void
diskfs_bt_resume_drain(struct diskfs_thread *thread)
{
    struct diskfs_block_waiter *list, *w;

    pthread_mutex_lock(&thread->resume_lock);
    list                = thread->resume_head;
    thread->resume_head = NULL;
    thread->resume_tail = NULL;
    pthread_mutex_unlock(&thread->resume_lock);

    while (list) {
        void  (*resume)(
            struct diskfs_thread *,
            void *);
        void *arg;

        w      = list;
        list   = w->next;
        resume = w->resume;
        arg    = w->arg;
        diskfs_block_waiter_free(thread, w);
        resume(thread, arg);
    }
} /* diskfs_bt_resume_drain */

static void
diskfs_bt_resume_doorbell_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct diskfs_thread *thread = container_of(doorbell, struct diskfs_thread,
                                                resume_doorbell);

    (void) evpl;
    diskfs_bt_resume_drain(thread);
} /* diskfs_bt_resume_doorbell_cb */

static void
diskfs_bt_resume_deferral_cb(
    struct evpl *evpl,
    void        *private_data)
{
    (void) evpl;
    diskfs_bt_resume_drain(private_data);
} /* diskfs_bt_resume_deferral_cb */

/* Completion context for an in-flight block read. */
struct diskfs_block_load {
    struct diskfs_block  *blk;
    struct diskfs_thread *thread;     /* worker that issued the read */
};

/* Resume data-I/O requests parked on the admission gate (defined below); a
 * metadata-node read shares the worker queue, so its completion frees capacity
 * too and must wake parked requests, else they hang. */
static void diskfs_io_resume_waiters(
    struct diskfs_thread *thread);

/* Block read completion: data landed directly in blk->iov; mark CLEAN, wake. */
static void
diskfs_block_load_complete(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct diskfs_block_load   *ld    = private_data;
    struct diskfs_block        *blk   = ld->blk;
    struct diskfs_thread       *self  = ld->thread;
    struct diskfs_block_shard  *shard = diskfs_block_shard(self->shared->block_cache,
                                                           blk->device_id, blk->device_offset);
    struct diskfs_block_waiter *waiters, *w;

    (void) evpl;
    chimera_diskfs_abort_if(status != 0, "block read failed off=%lu status=%d",
                            blk->device_offset, status);

    pthread_mutex_lock(&shard->lock);
    blk->state     = DISKFS_BLOCK_CLEAN;
    waiters        = blk->wait_head;
    blk->wait_head = NULL;
    blk->wait_tail = NULL;
    pthread_mutex_unlock(&shard->lock);

    self->pending_io--;
    free(ld);

    while (waiters) {
        w       = waiters;
        waiters = w->next;
        diskfs_block_waiter_dispatch(self, w);
    }

    /* Freed a worker-queue slot: let any parked data-I/O requests resume. */
    diskfs_io_resume_waiters(self);
} /* diskfs_block_load_complete */

/*
 * Fetch the block backing a b+tree node for op.  On a resident, valid block
 * the block is returned immediately.  Otherwise the op is parked on the
 * block's waiter list (a read is issued if it is not already in flight) and
 * NULL is returned; the op's driver will be re-entered once the block loads.
 */
/* Pin a block for op's descent (so it can't be evicted while in use) and
 * record it for release at completion.  Caller holds the shard lock. */
static inline void
diskfs_bt_op_pin(
    struct diskfs_bt_op       *op,
    struct diskfs_block_shard *shard,
    struct diskfs_block       *blk)
{
    if (blk->on_lru) {
        diskfs_block_lru_unlink(shard, blk);
    }
    blk->pin_count++;
    chimera_diskfs_abort_if(op->npins >= (int) (sizeof(op->pins) / sizeof(op->pins[0])),
                            "b+tree op pin list overflow");
    op->pins[op->npins++] = blk;
} /* diskfs_bt_op_pin */

static struct diskfs_block *
diskfs_bt_block_get(
    struct diskfs_bt_op *op,
    uint32_t             device_id,
    uint64_t             device_offset)
{
    struct diskfs_thread      *thread = op->thread;
    struct diskfs_block_cache *cache  = thread->shared->block_cache;
    struct diskfs_block_shard *shard  = diskfs_block_shard(cache, device_id, device_offset);
    uint32_t                   bucket = diskfs_block_bucket(device_id, device_offset);
    struct diskfs_block       *blk;
    struct diskfs_block_load  *ld;
    int                        issue = 0;

    pthread_mutex_lock(&shard->lock);
    blk = diskfs_block_lookup_locked(shard, bucket, device_id, device_offset);
    if (blk && blk->state != DISKFS_BLOCK_LOADING) {
        diskfs_bt_op_pin(op, shard, blk);
        pthread_mutex_unlock(&shard->lock);
        return blk;
    }

    if (!blk) {
        blk                    = diskfs_block_recycle(shard);
        blk->device_id         = device_id;
        blk->device_offset     = device_offset;
        blk->state             = DISKFS_BLOCK_LOADING;
        blk->seq               = 0;
        blk->wait_head         = NULL;
        blk->wait_tail         = NULL;
        blk->hash_next         = shard->buckets[bucket];
        shard->buckets[bucket] = blk;
        issue                  = 1;
    }

    /* Park this op on the block's waiter list (via a continuation that
     * re-enters its driver); it can no longer complete inline, so its result
     * will be delivered via the callback. */
    {
        struct diskfs_block_waiter *w = diskfs_block_waiter_alloc(thread);

        op->suspended = 1;
        w->thread     = op->thread;
        w->resume     = diskfs_bt_op_resume_cb;
        w->arg        = op;
        if (blk->wait_tail) {
            blk->wait_tail->next = w;
        } else {
            blk->wait_head = w;
        }
        blk->wait_tail = w;
    }
    pthread_mutex_unlock(&shard->lock);

    if (issue) {
        diskfs_block_ensure_iov(thread, blk);
        ld         = malloc(sizeof(*ld));
        ld->blk    = blk;
        ld->thread = thread;
        thread->pending_io++;
        evpl_block_read(thread->evpl, thread->queue[device_id], &blk->iov, 1,
                        device_offset, diskfs_block_load_complete, ld);
    }

    return NULL;
} /* diskfs_bt_block_get */

/*
 * Async, COW-aware block claim for non-bt_op callers.  Behaves like
 * diskfs_block_claim (returns the block pinned; is_new starts from a zeroed
 * buffer; a resident LOGGED buffer is COW-forked) but never reads
 * synchronously: on a miss (or a read already in flight) it parks
 * resume(thread, arg) on the block and returns NULL, and the read is driven on
 * the async evpl_block path.  The caller's continuation re-invokes this once
 * the block has loaded, when it returns the now-resident block inline.
 */
static struct diskfs_block *
diskfs_block_claim_async(
    struct diskfs_thread *thread,
    uint32_t device_id,
    uint64_t device_offset,
    int is_new,
    void ( *resume )(struct diskfs_thread *, void *),
    void *arg)
{
    struct diskfs_block_cache  *cache  = thread->shared->block_cache;
    uint64_t                    hash   = diskfs_block_hash(device_id, device_offset);
    uint32_t                    sidx   = hash & DISKFS_BLOCK_CACHE_SHARD_MASK;
    uint32_t                    bucket = (hash >> 8) & DISKFS_BLOCK_CACHE_BUCKET_MASK;
    struct diskfs_block_shard  *shard  = &cache->shards[sidx];
    struct diskfs_block        *blk;
    struct diskfs_block_waiter *w;
    struct diskfs_block_load   *ld;
    int                         issue = 0;

    pthread_mutex_lock(&shard->lock);

    blk = diskfs_block_lookup_locked(shard, bucket, device_id, device_offset);

    if (blk && blk->state == DISKFS_BLOCK_LOADING) {
        /* A read is already in flight: park and resume when it lands. */
        w         = diskfs_block_waiter_alloc(thread);
        w->thread = thread;
        w->resume = resume;
        w->arg    = arg;
        if (blk->wait_tail) {
            blk->wait_tail->next = w;
        } else {
            blk->wait_head = w;
        }
        blk->wait_tail = w;
        pthread_mutex_unlock(&shard->lock);
        return NULL;
    }

    if (!blk) {
        blk                = diskfs_block_recycle(shard);
        blk->device_id     = device_id;
        blk->device_offset = device_offset;
        blk->seq           = 0;
        blk->wait_head     = NULL;
        blk->wait_tail     = NULL;

        if (is_new) {
            blk->state = DISKFS_BLOCK_CLEAN;
            diskfs_block_ensure_iov(thread, blk);
            memset(blk->iov.data, 0, DISKFS_BLOCK_SIZE);
            blk->hash_next         = shard->buckets[bucket];
            shard->buckets[bucket] = blk;
            blk->pin_count++;
            pthread_mutex_unlock(&shard->lock);
            return blk;
        }

        /* Miss: publish a LOADING block, park, and issue the async read. */
        blk->state             = DISKFS_BLOCK_LOADING;
        blk->hash_next         = shard->buckets[bucket];
        shard->buckets[bucket] = blk;
        w                      = diskfs_block_waiter_alloc(thread);
        w->thread              = thread;
        w->resume              = resume;
        w->arg                 = arg;
        blk->wait_head         = w;
        blk->wait_tail         = w;
        issue                  = 1;
    } else if (blk->on_lru) {
        diskfs_block_lru_unlink(shard, blk);
    } else if (blk->state == DISKFS_BLOCK_LOGGED) {
        /* COW (see diskfs_block_claim): fork a private writable copy. */
        struct evpl_iovec nv;

        evpl_iovec_alloc(thread->evpl, DISKFS_BLOCK_SIZE, DISKFS_BLOCK_SIZE, 1,
                         EVPL_IOVEC_FLAG_SHARED, &nv);
        memcpy(nv.data, blk->iov.data, DISKFS_BLOCK_SIZE);
        evpl_iovec_release(thread->evpl, &blk->iov);
        evpl_iovec_move(&blk->iov, &nv);
        blk->state = DISKFS_BLOCK_CLEAN;
    }

    if (issue) {
        pthread_mutex_unlock(&shard->lock);
        diskfs_block_ensure_iov(thread, blk);
        ld         = malloc(sizeof(*ld));
        ld->blk    = blk;
        ld->thread = thread;
        thread->pending_io++;
        evpl_block_read(thread->evpl, thread->queue[device_id], &blk->iov, 1,
                        device_offset, diskfs_block_load_complete, ld);
        return NULL;
    }

    blk->pin_count++;
    pthread_mutex_unlock(&shard->lock);
    return blk;
} /* diskfs_block_claim_async */

/*
 * Ensure a write-locked inode's home block is resident + pinned + attached to
 * the txn, then fire cb(inode, OK, private_data).  The block read (on a cache
 * miss) goes through the async path: if it must wait, this parks a continuation
 * and returns; cb fires later, back on this worker, once the block loads.
 * Idempotent: if the block is already attached (inode->block set), cb fires
 * inline.  Used at the two write-lock grant points.
 */
struct diskfs_pin_cont {
    struct diskfs_thread *thread;
    struct diskfs_txn    *txn;
    struct diskfs_inode  *inode;
    diskfs_inode_cb_t     cb;
    void                 *private_data;
};

static void
diskfs_inode_finish_write_pin(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode,
    diskfs_inode_cb_t     cb,
    void                 *private_data)
{
    struct diskfs_block    *blk;
    struct diskfs_pin_cont *c;
    uint32_t                device_id;
    uint64_t                device_offset;

    if (inode->block) {
        cb(inode, CHIMERA_VFS_OK, private_data);
        return;
    }

    device_offset = sm_inum_to_device_offset(thread->shared->space_map,
                                             inode->inum, &device_id);

    c               = malloc(sizeof(*c));
    c->thread       = thread;
    c->txn          = txn;
    c->inode        = inode;
    c->cb           = cb;
    c->private_data = private_data;

    blk = diskfs_block_claim_async(thread, device_id, device_offset, 0,
                                   diskfs_pin_cont_resume, c);
    if (!blk) {
        return;     /* suspended; diskfs_pin_cont_resume re-runs on load */
    }

    free(c);
    inode->block = blk;
    diskfs_txn_add_block(txn, blk);
    cb(inode, CHIMERA_VFS_OK, private_data);
} /* diskfs_inode_finish_write_pin */

static void
diskfs_pin_cont_resume(
    struct diskfs_thread *thread,
    void                 *arg)
{
    struct diskfs_pin_cont c = *(struct diskfs_pin_cont *) arg;

    (void) thread;
    free(arg);
    diskfs_inode_finish_write_pin(c.thread, c.txn, c.inode, c.cb, c.private_data);
} /* diskfs_pin_cont_resume */

/*
 * Ensure this (write-locked) inode's home block is resident and pinned, and
 * attached to the transaction.  Idempotent per inode: the inode caches its
 * block pointer and we only claim/attach once.
 */
static void
diskfs_txn_pin_inode_block(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode,
    int                   is_new)
{
    uint32_t device_id;
    uint64_t device_offset;

    if (inode->block) {
        return;     /* already pinned by this txn */
    }

    device_offset = sm_inum_to_device_offset(thread->shared->space_map,
                                             inode->inum, &device_id);

    inode->block = diskfs_block_claim(thread, device_id, device_offset, is_new);
    diskfs_txn_add_block(txn, inode->block);

    if (is_new) {
        /* Initialize the embedded b+tree root (empty leaf) in the new
         * inode block. */
        diskfs_bt_node_init(inode->block->iov.data, DISKFS_BT_ROOT_BASE,
                            DISKFS_BT_ROOT_CAP, 0);
    }
} /* diskfs_txn_pin_inode_block */

/* Serialize an inode's durable attributes into the front of its block. */
static void
diskfs_inode_flush(struct diskfs_inode *inode)
{
    struct diskfs_dinode *di;

    if (!inode->block) {
        return;
    }

    di = inode->block->iov.data;

    di->inum       = inode->inum;
    di->gen        = inode->gen;
    di->mode       = inode->mode;
    di->nlink      = inode->nlink;
    di->uid        = inode->uid;
    di->gid        = inode->gid;
    di->rdev       = inode->rdev;
    di->size       = inode->size;
    di->space_used = inode->space_used;
    di->atime_sec  = inode->atime_sec;
    di->mtime_sec  = inode->mtime_sec;
    di->ctime_sec  = inode->ctime_sec;
    di->atime_nsec = inode->atime_nsec;
    di->mtime_nsec = inode->mtime_nsec;
    di->ctime_nsec = inode->ctime_nsec;
    if (S_ISDIR(inode->mode)) {
        di->parent_inum = inode->parent_inum;
        di->parent_gen  = inode->parent_gen;
    }

    inode->block->state = DISKFS_BLOCK_DIRTY;
} /* diskfs_inode_flush */

/*
 * At commit, serialize every write-locked inode into its block buffer.
 * Runs on the worker thread (it owns the live inodes under write lock).
 */
static void
diskfs_txn_flush_inodes(struct diskfs_txn *txn)
{
    int i;

    for (i = 0; i < txn->num_inodes; i++) {
        if (txn->inodes[i].mode == DISKFS_INODE_LOCK_WRITE) {
            diskfs_inode_flush(txn->inodes[i].inode);
        }
    }
} /* diskfs_txn_flush_inodes */

/*
 * Unpin all blocks held by this txn, transitioning them to new_state.  Also
 * clears each write-locked inode's cached block pointer (it is only valid
 * while the txn holds the block pinned; a later txn re-claims it).  Used at
 * commit (intent-log thread) and abort (worker).  Must run while the txn's
 * inode slots are still populated (before diskfs_txn_unlock_all).
 */
static void
diskfs_txn_unpin_blocks(
    struct diskfs_txn      *txn,
    enum diskfs_block_state new_state)
{
    struct diskfs_thread    *thread = txn->thread;
    struct diskfs_txn_block *tb     = txn->blocks;
    struct diskfs_txn_block *n;
    int                      i;

    for (i = 0; i < txn->num_inodes; i++) {
        if (txn->inodes[i].mode == DISKFS_INODE_LOCK_WRITE) {
            txn->inodes[i].inode->block = NULL;
        }
    }

    txn->blocks = NULL;
    while (tb) {
        struct diskfs_block *blk = tb->block;

        n = tb->next;
        diskfs_block_unpin(thread, blk, new_state);
        free(tb);
        tb = n;
    }
} /* diskfs_txn_unpin_blocks */

/* ------------------------------------------------------------------ */
/* Per-inode b+tree                                                    */
/* ------------------------------------------------------------------ */

static inline int
diskfs_bt_key_cmp(
    const struct diskfs_bt_key *a,
    const struct diskfs_bt_key *b)
{
    if (a->type != b->type) {
        return a->type < b->type ? -1 : 1;
    }
    if (a->subkey != b->subkey) {
        return a->subkey < b->subkey ? -1 : 1;
    }
    return 0;
} /* diskfs_bt_key_cmp */

static inline struct diskfs_bt_node_hdr *
diskfs_bt_hdr(
    void    *buf,
    uint32_t base)
{
    return (struct diskfs_bt_node_hdr *) ((char *) buf + base);
} /* diskfs_bt_hdr */

static inline struct diskfs_bt_islot *
diskfs_bt_islots(
    void    *buf,
    uint32_t base)
{
    return (struct diskfs_bt_islot *) ((char *) buf + base + sizeof(struct diskfs_bt_node_hdr));
} /* diskfs_bt_islots */

static inline struct diskfs_bt_lslot *
diskfs_bt_lslots(
    void    *buf,
    uint32_t base)
{
    return (struct diskfs_bt_lslot *) ((char *) buf + base + sizeof(struct diskfs_bt_node_hdr));
} /* diskfs_bt_lslots */

static inline void
diskfs_bt_node_init(
    void    *buf,
    uint32_t base,
    uint32_t capacity,
    uint16_t level)
{
    struct diskfs_bt_node_hdr *h = diskfs_bt_hdr(buf, base);

    h->level     = level;
    h->nitems    = 0;
    h->capacity  = capacity;
    h->free_end  = capacity;
    h->reserved  = 0;
    h->next_leaf = 0;
    h->prev_leaf = 0;
} /* diskfs_bt_node_init */

/* Free bytes available in a leaf for one more (slot + record). */
static inline uint32_t
diskfs_bt_leaf_free(
    void    *buf,
    uint32_t base)
{
    struct diskfs_bt_node_hdr *h          = diskfs_bt_hdr(buf, base);
    uint32_t                   free_start = sizeof(*h) + h->nitems * sizeof(struct diskfs_bt_lslot);

    return h->free_end - free_start;
} /* diskfs_bt_leaf_free */

static inline uint32_t
diskfs_bt_interior_free(
    void    *buf,
    uint32_t base)
{
    struct diskfs_bt_node_hdr *h          = diskfs_bt_hdr(buf, base);
    uint32_t                   free_start = sizeof(*h) + h->nitems * sizeof(struct diskfs_bt_islot);

    return h->capacity - free_start;
} /* diskfs_bt_interior_free */

/*
 * Binary search a leaf for key.  Returns the index of the first slot whose
 * key is >= the search key; sets *exact if that slot's key matches.
 */
static int
diskfs_bt_leaf_search(
    void                       *buf,
    uint32_t                    base,
    const struct diskfs_bt_key *key,
    int                        *exact)
{
    struct diskfs_bt_lslot *sl = diskfs_bt_lslots(buf, base);
    int                     n  = diskfs_bt_hdr(buf, base)->nitems;
    int                     lo = 0, hi = n;

    *exact = 0;
    while (lo < hi) {
        int mid = (lo + hi) >> 1;
        int c   = diskfs_bt_key_cmp(&sl[mid].key, key);

        if (c < 0) {
            lo = mid + 1;
        } else if (c > 0) {
            hi = mid;
        } else {
            *exact = 1;
            return mid;
        }
    }
    return lo;
} /* diskfs_bt_leaf_search */

/* Index of the child subtree that may contain key (largest key <= search). */
static int
diskfs_bt_interior_search(
    void                       *buf,
    uint32_t                    base,
    const struct diskfs_bt_key *key)
{
    struct diskfs_bt_islot *sl = diskfs_bt_islots(buf, base);
    int                     n  = diskfs_bt_hdr(buf, base)->nitems;
    int                     lo = 0, hi = n, ans = 0;

    while (lo < hi) {
        int mid = (lo + hi) >> 1;

        if (diskfs_bt_key_cmp(&sl[mid].key, key) <= 0) {
            ans = mid;
            lo  = mid + 1;
        } else {
            hi = mid;
        }
    }
    return ans;
} /* diskfs_bt_interior_search */

/* Append a leaf record at the end (caller guarantees sorted order + room). */
static void
diskfs_bt_leaf_append(
    void                       *buf,
    uint32_t                    base,
    const struct diskfs_bt_key *key,
    const void                 *rec,
    uint32_t                    reclen)
{
    struct diskfs_bt_node_hdr *h  = diskfs_bt_hdr(buf, base);
    struct diskfs_bt_lslot    *sl = diskfs_bt_lslots(buf, base);

    h->free_end -= reclen;
    memcpy((char *) buf + base + h->free_end, rec, reclen);
    sl[h->nitems].key = *key;
    sl[h->nitems].off = h->free_end;
    sl[h->nitems].len = reclen;
    h->nitems++;
} /* diskfs_bt_leaf_append */

/* Allocate a fresh b+tree node block; returns the (pinned, txn-attached)
 * block and its bptr.  Buffer is zeroed and initialized as an empty node. */
static struct diskfs_block *
diskfs_bt_alloc_node(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    uint16_t              level,
    uint64_t             *r_bptr)
{
    struct diskfs_shared *shared = thread->shared;
    struct diskfs_block  *blk;
    uint32_t              device_id;
    uint64_t              device_offset;
    int                   rc;

    /* Runs inside the synchronous b+tree modify, which pre-reserves enough
     * space (bt_run's RESERVE phase) so this draws from the thread cache and
     * never journals -- hence no_suspend and the rc != 0 abort. */
    DISKFS_SM_JNL(jnl, thread, txn, diskfs_sm_no_suspend, NULL);
    rc = space_map_alloc(shared->space_map, &thread->space_cache, &jnl,
                         DISKFS_BLOCK_SIZE, &device_id, &device_offset);
    chimera_diskfs_abort_if(rc != 0, "b+tree node allocation failed (ENOSPC)");

    blk = diskfs_block_claim(thread, device_id, device_offset, 1);
    diskfs_txn_add_block(txn, blk);

    diskfs_bt_node_init(blk->iov.data, 0, DISKFS_BT_NODE_CAP, level);

    *r_bptr = sm_inum_from_device_offset(shared->space_map, device_id, device_offset);
    return blk;
} /* diskfs_bt_alloc_node */

/* Resolve a child bptr to its block buffer for writing (claim+pin+attach). */
static void *
diskfs_bt_node_for_write(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    uint64_t              bptr)
{
    uint32_t             device_id;
    uint64_t             device_offset;
    struct diskfs_block *blk;

    device_offset = sm_inum_to_device_offset(thread->shared->space_map, bptr, &device_id);
    blk           = diskfs_block_claim(thread, device_id, device_offset, 0);
    diskfs_txn_add_block(txn, blk);
    return blk->iov.data;
} /* diskfs_bt_node_for_write */

/*
 * Split a full leaf (current node at buf/base/cap) while inserting
 * (nkey,nrec).  The lower half stays in place; the upper half plus the new
 * right sibling's bptr are returned via *sep_key / *sep_bptr.
 */
static void
diskfs_bt_leaf_split(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    void                       *buf,
    uint32_t                    base,
    uint32_t                    cap,
    uint64_t                    self_bptr,
    int                         insert_idx,
    const struct diskfs_bt_key *nkey,
    const void                 *nrec,
    uint32_t                    nreclen,
    struct diskfs_bt_key       *sep_key,
    uint64_t                   *sep_bptr)
{
    struct diskfs_bt_node_hdr *h     = diskfs_bt_hdr(buf, base);
    struct diskfs_bt_lslot    *sl    = diskfs_bt_lslots(buf, base);
    int                        n     = h->nitems;
    int                        total = n + 1;

    struct {
        struct diskfs_bt_key key;
        uint32_t             len;
        uint32_t             scratch_off;
    } *items;
    char                      *scratch;
    uint32_t                   sp = 0, total_bytes = 0, half, acc = 0;
    int                        i, oi, split_i;
    struct diskfs_block       *right;
    void                      *rbuf;
    uint64_t                   old_next, old_prev;

    items   = malloc(total * sizeof(*items));
    scratch = malloc(cap + nreclen);

    for (i = 0, oi = 0; i < total; i++) {
        if (i == insert_idx) {
            memcpy(scratch + sp, nrec, nreclen);
            items[i].key = *nkey;
            items[i].len = nreclen;
        } else {
            memcpy(scratch + sp, (char *) buf + base + sl[oi].off, sl[oi].len);
            items[i].key = sl[oi].key;
            items[i].len = sl[oi].len;
            oi++;
        }
        items[i].scratch_off = sp;
        sp                  += items[i].len;
        total_bytes         += items[i].len;
    }

    half    = total_bytes / 2;
    split_i = 1;
    for (i = 0; i < total; i++) {
        if (acc >= half && i > 0) {
            split_i = i;
            break;
        }
        acc    += items[i].len;
        split_i = i + 1;
    }
    if (split_i < 1) {
        split_i = 1;
    }
    if (split_i > total - 1) {
        split_i = total - 1;
    }

    old_next = h->next_leaf;
    old_prev = h->prev_leaf;

    right = diskfs_bt_alloc_node(thread, txn, 0, sep_bptr);
    rbuf  = right->iov.data;

    /* Rebuild the left node in place from scratch (no aliasing).  node_init
     * clears the leaf links, so they are restored explicitly below. */
    diskfs_bt_node_init(buf, base, cap, 0);
    for (i = 0; i < split_i; i++) {
        diskfs_bt_leaf_append(buf, base, &items[i].key,
                              scratch + items[i].scratch_off, items[i].len);
    }
    for (i = split_i; i < total; i++) {
        diskfs_bt_leaf_append(rbuf, 0, &items[i].key,
                              scratch + items[i].scratch_off, items[i].len);
    }

    /* Splice the new right sibling into the doubly-linked leaf chain:
     *   self <-> right <-> old_next
     * (self keeps its own bptr; for the embedded-root-grow case the caller
     * fixes right->prev_leaf to the new left block afterward.) */
    diskfs_bt_hdr(rbuf, 0)->next_leaf = old_next;
    diskfs_bt_hdr(rbuf, 0)->prev_leaf = self_bptr;
    h->next_leaf                      = *sep_bptr;
    h->prev_leaf                      = old_prev;

    if (old_next) {
        void *nbuf = diskfs_bt_node_for_write(thread, txn, old_next);
        diskfs_bt_hdr(nbuf, 0)->prev_leaf = *sep_bptr;
    }

    *sep_key = items[split_i].key;

    chimera_diskfs_abort_if(diskfs_bt_leaf_free(buf, base) > cap ||
                            diskfs_bt_leaf_free(rbuf, 0) > DISKFS_BT_NODE_CAP,
                            "b+tree leaf split overflow");

    free(items);
    free(scratch);
} /* diskfs_bt_leaf_split */

/* Insert (key,rec) into a leaf, splitting if needed.  Returns 1 on split. */
static int
diskfs_bt_leaf_insert(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    void                       *buf,
    uint32_t                    base,
    uint32_t                    cap,
    uint64_t                    self_bptr,
    const struct diskfs_bt_key *key,
    const void                 *rec,
    uint32_t                    reclen,
    struct diskfs_bt_key       *sep_key,
    uint64_t                   *sep_bptr)
{
    struct diskfs_bt_node_hdr *h = diskfs_bt_hdr(buf, base);
    struct diskfs_bt_lslot    *sl;
    int                        idx, exact, j;

    idx = diskfs_bt_leaf_search(buf, base, key, &exact);
    chimera_diskfs_abort_if(exact, "b+tree duplicate key insert");

    if (diskfs_bt_leaf_free(buf, base) >= sizeof(struct diskfs_bt_lslot) + reclen) {
        sl = diskfs_bt_lslots(buf, base);
        for (j = h->nitems; j > idx; j--) {
            sl[j] = sl[j - 1];
        }
        h->free_end -= reclen;
        memcpy((char *) buf + base + h->free_end, rec, reclen);
        sl[idx].key = *key;
        sl[idx].off = h->free_end;
        sl[idx].len = reclen;
        h->nitems++;
        return 0;
    }

    diskfs_bt_leaf_split(thread, txn, buf, base, cap, self_bptr, idx, key, rec,
                         reclen, sep_key, sep_bptr);
    return 1;
} /* diskfs_bt_leaf_insert */

/* Insert (key,child) into an interior node, splitting if needed. */
static int
diskfs_bt_interior_insert(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    void                       *buf,
    uint32_t                    base,
    uint32_t                    cap,
    const struct diskfs_bt_key *key,
    uint64_t                    child,
    struct diskfs_bt_key       *sep_key,
    uint64_t                   *sep_bptr)
{
    struct diskfs_bt_node_hdr *h  = diskfs_bt_hdr(buf, base);
    struct diskfs_bt_islot    *sl = diskfs_bt_islots(buf, base);
    int                        n  = h->nitems;
    int                        idx, j, split_i;
    struct diskfs_block       *right;
    struct diskfs_bt_islot    *rsl;
    struct diskfs_bt_node_hdr *rh;

    /* Find sorted insert position (keys are unique separators). */
    idx = 0;
    while (idx < n && diskfs_bt_key_cmp(&sl[idx].key, key) < 0) {
        idx++;
    }

    if (diskfs_bt_interior_free(buf, base) >= sizeof(struct diskfs_bt_islot)) {
        for (j = n; j > idx; j--) {
            sl[j] = sl[j - 1];
        }
        sl[idx].key   = *key;
        sl[idx].child = child;
        h->nitems++;
        return 0;
    }

    /* Split: build the full set of n+1 slots in order, distribute halves. */
    {
        struct diskfs_bt_islot all[ (DISKFS_BT_NODE_CAP / sizeof(struct diskfs_bt_islot)) + 2 ];
        int                    total = n + 1;
        int                    p = 0, ins = 0;

        for (j = 0; j < n; j++) {
            if (!ins && diskfs_bt_key_cmp(key, &sl[j].key) < 0) {
                all[p].key = *key; all[p].child = child; p++; ins = 1;
            }
            all[p++] = sl[j];
        }
        if (!ins) {
            all[p].key = *key; all[p].child = child; p++;
        }

        split_i = total / 2;

        right = diskfs_bt_alloc_node(thread, txn, h->level, sep_bptr);
        rsl   = diskfs_bt_islots(right->iov.data, 0);
        rh    = diskfs_bt_hdr(right->iov.data, 0);

        h->nitems = split_i;
        for (j = 0; j < split_i; j++) {
            sl[j] = all[j];
        }
        for (j = split_i; j < total; j++) {
            rsl[j - split_i] = all[j];
        }
        rh->nitems = total - split_i;

        *sep_key = rsl[0].key;
        return 1;
    }
} /* diskfs_bt_interior_insert */

static int
diskfs_bt_insert_rec(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    void                       *buf,
    uint32_t                    base,
    uint32_t                    cap,
    uint64_t                    self_bptr,
    const struct diskfs_bt_key *key,
    const void                 *rec,
    uint32_t                    reclen,
    struct diskfs_bt_key       *sep_key,
    uint64_t                   *sep_bptr)
{
    struct diskfs_bt_node_hdr *h = diskfs_bt_hdr(buf, base);
    struct diskfs_bt_islot    *sl;
    int                        ci, csplit;
    uint64_t                   child_bptr;
    void                      *child_buf;
    struct diskfs_bt_key       csep;
    uint64_t                   cbptr;

    if (h->level == 0) {
        return diskfs_bt_leaf_insert(thread, txn, buf, base, cap, self_bptr, key,
                                     rec, reclen, sep_key, sep_bptr);
    }

    ci         = diskfs_bt_interior_search(buf, base, key);
    sl         = diskfs_bt_islots(buf, base);
    child_bptr = sl[ci].child;
    child_buf  = diskfs_bt_node_for_write(thread, txn, child_bptr);

    csplit = diskfs_bt_insert_rec(thread, txn, child_buf, 0, DISKFS_BT_NODE_CAP,
                                  child_bptr, key, rec, reclen, &csep, &cbptr);
    if (!csplit) {
        return 0;
    }

    return diskfs_bt_interior_insert(thread, txn, buf, base, cap, &csep, cbptr,
                                     sep_key, sep_bptr);
} /* diskfs_bt_insert_rec */

/*
 * Insert a record into an inode's b+tree.  The inode must be write-locked and
 * its root block pinned, and the descent path must already be resident (the
 * async driver faults it in first).  Synchronous structural modify.
 */
static void
diskfs_bt_insert_locked(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    struct diskfs_inode        *inode,
    const struct diskfs_bt_key *key,
    const void                 *rec,
    uint32_t                    reclen)
{
    void                *root = inode->block->iov.data;
    struct diskfs_bt_key sep;
    uint64_t             sep_bptr;
    int                  split;

    split = diskfs_bt_insert_rec(thread, txn, root, DISKFS_BT_ROOT_BASE,
                                 DISKFS_BT_ROOT_CAP, inode->inum, key, rec, reclen,
                                 &sep, &sep_bptr);
    if (!split) {
        return;
    }

    /* Root overflowed: grow the tree.  Move the (post-split lower-half)
     * root contents into a new left child, then re-form the root as an
     * interior node pointing at the new left child and the split's right
     * sibling. */
    {
        struct diskfs_bt_node_hdr *rh        = diskfs_bt_hdr(root, DISKFS_BT_ROOT_BASE);
        uint16_t                   old_level = rh->level;
        uint64_t                   left_bptr;
        struct diskfs_block       *left;
        struct diskfs_bt_key       left_min;
        struct diskfs_bt_islot    *isl;

        left = diskfs_bt_alloc_node(thread, txn, old_level, &left_bptr);

        /* Copy the entire embedded root node into the new left block. */
        memcpy((char *) left->iov.data, (char *) root + DISKFS_BT_ROOT_BASE, DISKFS_BT_ROOT_CAP);
        diskfs_bt_hdr(left->iov.data, 0)->capacity = DISKFS_BT_NODE_CAP;

        if (old_level == 0) {
            left_min = diskfs_bt_lslots(left->iov.data, 0)[0].key;
        } else {
            left_min = diskfs_bt_islots(left->iov.data, 0)[0].key;
        }

        diskfs_bt_node_init(root, DISKFS_BT_ROOT_BASE, DISKFS_BT_ROOT_CAP,
                            old_level + 1);
        rh           = diskfs_bt_hdr(root, DISKFS_BT_ROOT_BASE);
        rh->nitems   = 2;
        isl          = diskfs_bt_islots(root, DISKFS_BT_ROOT_BASE);
        isl[0].key   = left_min;
        isl[0].child = left_bptr;
        isl[1].key   = sep;
        isl[1].child = sep_bptr;

        if (old_level == 0) {     /* leaf-root grow: fix right sibling back-link */
            /* The lower half migrated from the (now interior) embedded root
             * into the new left block, so the right sibling's back-link must
             * point at the left block rather than the inode's own bptr. */
            void *rbuf = diskfs_bt_node_for_write(thread, txn, sep_bptr);
            diskfs_bt_hdr(rbuf, 0)->prev_leaf = left_bptr;
        }
    }
} /* diskfs_bt_insert_locked */

/* A node (other than the root) is too empty and must borrow/merge once it
 * holds less than half its capacity: leaves measured in bytes (slots +
 * records), interior nodes in slot count. */
static inline int
diskfs_bt_leaf_underflow(
    void    *buf,
    uint32_t base)
{
    struct diskfs_bt_node_hdr *h    = diskfs_bt_hdr(buf, base);
    uint32_t                   used = h->nitems * sizeof(struct diskfs_bt_lslot) +
        (h->capacity - h->free_end);

    return used * 2 < h->capacity;
} /* diskfs_bt_leaf_underflow */

static inline int
diskfs_bt_interior_underflow(
    void    *buf,
    uint32_t base)
{
    struct diskfs_bt_node_hdr *h    = diskfs_bt_hdr(buf, base);
    uint32_t                   maxi = (h->capacity - sizeof(struct diskfs_bt_node_hdr)) /
        sizeof(struct diskfs_bt_islot);

    return (uint32_t) h->nitems * 2 < maxi;
} /* diskfs_bt_interior_underflow */

/* Repack a leaf's live records into a fresh heap, reclaiming the dead space
 * left by prior slot removals.  Leaf-chain links are preserved. */
static void
diskfs_bt_leaf_compact(
    void    *buf,
    uint32_t base,
    uint32_t cap)
{
    struct diskfs_bt_node_hdr *h  = diskfs_bt_hdr(buf, base);
    struct diskfs_bt_lslot    *sl = diskfs_bt_lslots(buf, base);
    int                        n    = h->nitems, i;
    uint64_t                   next = h->next_leaf, prev = h->prev_leaf;
    struct diskfs_bt_key      *keys    = malloc(n * sizeof(*keys) + 1);
    uint32_t                  *lens    = malloc(n * sizeof(uint32_t) + 1);
    char                      *scratch = malloc(cap);
    uint32_t                   o       = 0;

    for (i = 0; i < n; i++) {
        keys[i] = sl[i].key;
        lens[i] = sl[i].len;
        memcpy(scratch + o, (char *) buf + base + sl[i].off, sl[i].len);
        o += sl[i].len;
    }

    diskfs_bt_node_init(buf, base, cap, 0);
    o = 0;
    for (i = 0; i < n; i++) {
        diskfs_bt_leaf_append(buf, base, &keys[i], scratch + o, lens[i]);
        o += lens[i];
    }
    h            = diskfs_bt_hdr(buf, base);
    h->next_leaf = next;
    h->prev_leaf = prev;

    free(keys);
    free(lens);
    free(scratch);
} /* diskfs_bt_leaf_compact */

/*
 * Rebalance an underflowing leaf (child index ci of the interior parent at
 * pbuf/pbase) against an adjacent sibling: merge the two leaves if their
 * combined contents fit in one node, otherwise redistribute evenly.  Returns
 * 1 if the merge dropped a slot from the parent (which may now underflow), 0
 * otherwise.  Freed leaf blocks are orphaned (reclaim deferred).
 */
static int
diskfs_bt_rebalance_leaf(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    void                 *pbuf,
    uint32_t              pbase,
    int                   ci)
{
    struct diskfs_bt_islot    *psl = diskfs_bt_islots(pbuf, pbase);
    int                        pn  = diskfs_bt_hdr(pbuf, pbase)->nitems;
    int                        lidx, ridx, ln, rn, total, i, merged;
    uint64_t                   l_bptr, r_bptr, l_prev, r_next;
    void                      *lbuf, *rbuf;
    struct diskfs_bt_node_hdr *lh, *rh;
    struct diskfs_bt_key      *keys;
    uint32_t                  *lens;
    char                      *scratch;
    uint32_t                   o, need;

    if (pn < 2) {
        return 0;     /* sole child of a degenerate root: collapse handles it */
    }

    if (ci + 1 < pn) {
        lidx = ci;
        ridx = ci + 1;
    } else {
        lidx = ci - 1;
        ridx = ci;
    }

    l_bptr = psl[lidx].child;
    r_bptr = psl[ridx].child;
    lbuf   = diskfs_bt_node_for_write(thread, txn, l_bptr);
    rbuf   = diskfs_bt_node_for_write(thread, txn, r_bptr);
    lh     = diskfs_bt_hdr(lbuf, 0);
    rh     = diskfs_bt_hdr(rbuf, 0);
    ln     = lh->nitems;
    rn     = rh->nitems;
    total  = ln + rn;
    l_prev = lh->prev_leaf;
    r_next = rh->next_leaf;

    keys    = malloc((total + 1) * sizeof(*keys));
    lens    = malloc((total + 1) * sizeof(uint32_t));
    scratch = malloc(2 * DISKFS_BT_NODE_CAP);

    o = 0;
    for (i = 0; i < ln; i++) {
        struct diskfs_bt_lslot *s = diskfs_bt_lslots(lbuf, 0);
        keys[i] = s[i].key;
        lens[i] = s[i].len;
        memcpy(scratch + o, (char *) lbuf + s[i].off, s[i].len);
        o += s[i].len;
    }
    for (i = 0; i < rn; i++) {
        struct diskfs_bt_lslot *s = diskfs_bt_lslots(rbuf, 0);
        keys[ln + i] = s[i].key;
        lens[ln + i] = s[i].len;
        memcpy(scratch + o, (char *) rbuf + s[i].off, s[i].len);
        o += s[i].len;
    }

    need = sizeof(struct diskfs_bt_node_hdr) +
        total * sizeof(struct diskfs_bt_lslot) + o;

    if (need <= DISKFS_BT_NODE_CAP) {
        /* Merge everything into L; orphan R and unlink it from the chain. */
        uint32_t off = 0;

        diskfs_bt_node_init(lbuf, 0, DISKFS_BT_NODE_CAP, 0);
        for (i = 0; i < total; i++) {
            diskfs_bt_leaf_append(lbuf, 0, &keys[i], scratch + off, lens[i]);
            off += lens[i];
        }
        lh            = diskfs_bt_hdr(lbuf, 0);
        lh->prev_leaf = l_prev;
        lh->next_leaf = r_next;
        if (r_next) {
            void *nn = diskfs_bt_node_for_write(thread, txn, r_next);
            diskfs_bt_hdr(nn, 0)->prev_leaf = l_bptr;
        }

        for (i = ridx; i < pn - 1; i++) {
            psl[i] = psl[i + 1];
        }
        diskfs_bt_hdr(pbuf, pbase)->nitems = pn - 1;
        merged                             = 1;

        /* R is merged away: return its node block to the allocator (pending
         * free, applied when this txn commits). */
        {
            uint32_t fdev;
            uint64_t foff = sm_inum_to_device_offset(thread->shared->space_map,
                                                     r_bptr, &fdev);

            diskfs_txn_free_space(thread, txn, fdev, foff, DISKFS_BLOCK_SIZE);
        }
    } else {
        /* Redistribute evenly across L and R. */
        uint32_t half = o / 2, acc = 0, off = 0;
        int      split = 1;

        for (i = 0; i < total; i++) {
            if (acc >= half && i > 0) {
                split = i;
                break;
            }
            acc  += lens[i];
            split = i + 1;
        }
        if (split < 1) {
            split = 1;
        }
        if (split > total - 1) {
            split = total - 1;
        }

        diskfs_bt_node_init(lbuf, 0, DISKFS_BT_NODE_CAP, 0);
        for (i = 0; i < split; i++) {
            diskfs_bt_leaf_append(lbuf, 0, &keys[i], scratch + off, lens[i]);
            off += lens[i];
        }
        diskfs_bt_node_init(rbuf, 0, DISKFS_BT_NODE_CAP, 0);
        for (i = split; i < total; i++) {
            diskfs_bt_leaf_append(rbuf, 0, &keys[i], scratch + off, lens[i]);
            off += lens[i];
        }

        lh            = diskfs_bt_hdr(lbuf, 0);
        rh            = diskfs_bt_hdr(rbuf, 0);
        lh->prev_leaf = l_prev;
        lh->next_leaf = r_bptr;
        rh->prev_leaf = l_bptr;
        rh->next_leaf = r_next;

        psl[ridx].key = diskfs_bt_lslots(rbuf, 0)[0].key;
        merged        = 0;
    }

    free(keys);
    free(lens);
    free(scratch);
    return merged;
} /* diskfs_bt_rebalance_leaf */

/*
 * Rebalance an underflowing interior node (child index ci of parent
 * pbuf/pbase) against a sibling.  B+tree separators are routing copies, so a
 * merge is a plain concatenation of the two children's slots.  Returns 1 if a
 * parent slot was dropped, 0 otherwise.
 */
static int
diskfs_bt_rebalance_interior(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    void                 *pbuf,
    uint32_t              pbase,
    int                   ci)
{
    struct diskfs_bt_islot *psl = diskfs_bt_islots(pbuf, pbase);
    int                     pn  = diskfs_bt_hdr(pbuf, pbase)->nitems;
    int                     lidx, ridx, ln, rn, total, i, merged;
    void                   *lbuf, *rbuf;
    struct diskfs_bt_islot  all[(2 * DISKFS_BT_NODE_CAP / sizeof(struct diskfs_bt_islot)) + 2];
    uint32_t                maxi;

    if (pn < 2) {
        return 0;
    }

    if (ci + 1 < pn) {
        lidx = ci;
        ridx = ci + 1;
    } else {
        lidx = ci - 1;
        ridx = ci;
    }

    lbuf  = diskfs_bt_node_for_write(thread, txn, psl[lidx].child);
    rbuf  = diskfs_bt_node_for_write(thread, txn, psl[ridx].child);
    ln    = diskfs_bt_hdr(lbuf, 0)->nitems;
    rn    = diskfs_bt_hdr(rbuf, 0)->nitems;
    total = ln + rn;

    for (i = 0; i < ln; i++) {
        all[i] = diskfs_bt_islots(lbuf, 0)[i];
    }
    for (i = 0; i < rn; i++) {
        all[ln + i] = diskfs_bt_islots(rbuf, 0)[i];
    }

    maxi = (DISKFS_BT_NODE_CAP - sizeof(struct diskfs_bt_node_hdr)) /
        sizeof(struct diskfs_bt_islot);

    if ((uint32_t) total <= maxi) {
        uint64_t r_child = psl[ridx].child;   /* captured before the slot shift */
        uint32_t fdev;
        uint64_t foff = sm_inum_to_device_offset(thread->shared->space_map, r_child, &fdev);

        for (i = 0; i < total; i++) {
            diskfs_bt_islots(lbuf, 0)[i] = all[i];
        }
        diskfs_bt_hdr(lbuf, 0)->nitems = total;

        for (i = ridx; i < pn - 1; i++) {
            psl[i] = psl[i + 1];
        }
        diskfs_bt_hdr(pbuf, pbase)->nitems = pn - 1;
        merged                             = 1;

        /* R is merged away: pending-free its node block. */
        diskfs_txn_free_space(thread, txn, fdev, foff, DISKFS_BLOCK_SIZE);
    } else {
        int split = total / 2;

        for (i = 0; i < split; i++) {
            diskfs_bt_islots(lbuf, 0)[i] = all[i];
        }
        diskfs_bt_hdr(lbuf, 0)->nitems = split;
        for (i = split; i < total; i++) {
            diskfs_bt_islots(rbuf, 0)[i - split] = all[i];
        }
        diskfs_bt_hdr(rbuf, 0)->nitems = total - split;

        psl[ridx].key = diskfs_bt_islots(rbuf, 0)[0].key;
        merged        = 0;
    }

    return merged;
} /* diskfs_bt_rebalance_interior */

/*
 * Collapse the tree when the embedded root interior shrinks to a single
 * child: pull that child up into the embedded root, provided its contents fit
 * in the (smaller) embedded area.  Otherwise keep the degenerate one-child
 * root until later removals make it fit.
 */
static void
diskfs_bt_collapse_root(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode)
{
    void    *root = inode->block->iov.data;
    uint32_t base = DISKFS_BT_ROOT_BASE;

    for (;; ) {
        struct diskfs_bt_node_hdr *rh = diskfs_bt_hdr(root, base);
        uint64_t                   cbptr;
        void                      *cbuf;
        struct diskfs_bt_node_hdr *ch;
        uint32_t                   need;
        int                        i, n;

        if (rh->level == 0 || rh->nitems != 1) {
            break;
        }

        cbptr = diskfs_bt_islots(root, base)[0].child;
        cbuf  = diskfs_bt_node_for_write(thread, txn, cbptr);
        ch    = diskfs_bt_hdr(cbuf, 0);
        n     = ch->nitems;

        if (ch->level == 0) {
            need = sizeof(struct diskfs_bt_node_hdr) +
                n * sizeof(struct diskfs_bt_lslot) +
                (DISKFS_BT_NODE_CAP - ch->free_end);
        } else {
            need = sizeof(struct diskfs_bt_node_hdr) +
                n * sizeof(struct diskfs_bt_islot);
        }

        if (need > DISKFS_BT_ROOT_CAP) {
            break;     /* keep the one-child root */
        }

        if (ch->level == 0) {
            struct diskfs_bt_lslot *cs = diskfs_bt_lslots(cbuf, 0);
            uint64_t                cnext = ch->next_leaf, cprev = ch->prev_leaf;
            struct diskfs_bt_key   *keys    = malloc((n + 1) * sizeof(*keys));
            uint32_t               *lens    = malloc((n + 1) * sizeof(uint32_t));
            char                   *scratch = malloc(DISKFS_BT_NODE_CAP);
            uint32_t                o       = 0;

            for (i = 0; i < n; i++) {
                keys[i] = cs[i].key;
                lens[i] = cs[i].len;
                memcpy(scratch + o, (char *) cbuf + cs[i].off, cs[i].len);
                o += cs[i].len;
            }
            diskfs_bt_node_init(root, base, DISKFS_BT_ROOT_CAP, 0);
            o = 0;
            for (i = 0; i < n; i++) {
                diskfs_bt_leaf_append(root, base, &keys[i], scratch + o, lens[i]);
                o += lens[i];
            }
            rh            = diskfs_bt_hdr(root, base);
            rh->next_leaf = cnext;
            rh->prev_leaf = cprev;
            free(keys);
            free(lens);
            free(scratch);
        } else {
            struct diskfs_bt_islot tmp[(DISKFS_BT_NODE_CAP / sizeof(struct diskfs_bt_islot))];
            uint16_t               clevel = ch->level;

            for (i = 0; i < n; i++) {
                tmp[i] = diskfs_bt_islots(cbuf, 0)[i];
            }
            diskfs_bt_node_init(root, base, DISKFS_BT_ROOT_CAP, clevel);
            for (i = 0; i < n; i++) {
                diskfs_bt_islots(root, base)[i] = tmp[i];
            }
            diskfs_bt_hdr(root, base)->nitems = n;
        }
        /* The child was pulled into the embedded root: pending-free its block. */
        {
            uint32_t fdev;
            uint64_t foff = sm_inum_to_device_offset(thread->shared->space_map,
                                                     cbptr, &fdev);

            diskfs_txn_free_space(thread, txn, fdev, foff, DISKFS_BLOCK_SIZE);
        }
    }
} /* diskfs_bt_collapse_root */

/*
 * Reclaim space owned by a deleted inode (nlink just hit 0, not open).
 *
 * The free count for one inode is bounded only by its tree size, but every
 * free must ride a single transaction's redo -- an arbitrarily large inode
 * would overflow the journal / block-I/O queue.  So we reclaim inline only the
 * BOUNDED case: an inode whose entire tree fits in the embedded root (no child
 * node blocks).  That covers small files (data extents in the root, <=~100) and
 * empty/small directories.  We always free the home block.
 *
 * TODO(incremental-drain): a large inode (interior embedded root) still leaks
 * its child node blocks and their data extents here.  Draining those needs a
 * scheme that walks the tree across multiple bounded transactions (an orphan
 * list of deleted-but-not-fully-reclaimed inodes, drained in the background).
 * Also, a file unlinked while still open frees nothing here (deferred to close,
 * which has no write txn) -- another known leak.
 */
static void
diskfs_bt_free_tree(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode)
{
    uint32_t                   dev;
    uint64_t                   off = sm_inum_to_device_offset(thread->shared->space_map,
                                                              inode->inum, &dev);
    void                      *buf;
    struct diskfs_bt_node_hdr *h;

    diskfs_txn_pin_inode_block(thread, txn, inode, 0);
    buf = inode->block->iov.data;
    h   = diskfs_bt_hdr(buf, DISKFS_BT_ROOT_BASE);

    if (h->level == 0 && S_ISREG(inode->mode)) {
        /* Small file: every data extent is recorded in the embedded root. */
        struct diskfs_bt_lslot *s = diskfs_bt_lslots(buf, DISKFS_BT_ROOT_BASE);
        int                     i;

        for (i = 0; i < h->nitems; i++) {
            struct diskfs_extent_rec *e =
                (struct diskfs_extent_rec *) ((char *) buf + DISKFS_BT_ROOT_BASE + s[i].off);

            diskfs_txn_free_space(thread, txn, e->device_id, e->device_offset,
                                  SM_ALIGN_UP(e->length));
        }
    }

    /* The home block stays pinned + logged (with the nlink=0 dinode); its
     * range is reclaimed on commit. */
    diskfs_txn_free_space(thread, txn, dev, off, DISKFS_BLOCK_SIZE);
} /* diskfs_bt_free_tree */

/* Forward declarations for helpers defined later in the file. */
static struct diskfs_txn * diskfs_txn_begin(
    struct diskfs_thread *thread,
    enum diskfs_txn_type  type);
static inline void diskfs_txn_abort(
    struct diskfs_txn *txn);
static inline void diskfs_txn_commit(
    struct diskfs_txn     *txn,
    diskfs_txn_commit_cb_t cb,
    void                  *private_data);
static void diskfs_inode_free(
    struct diskfs_thread *thread,
    struct diskfs_inode  *inode);
static void diskfs_thread_free_space(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    uint32_t              device_id,
    uint64_t              device_offset,
    uint64_t              length);
static int diskfs_bt_remove_async(
    struct diskfs_bt_op        *op,
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    struct diskfs_inode        *inode,
    const struct diskfs_bt_key *key,
    diskfs_bt_cb_t              cb,
    void                       *private_data);
static int diskfs_bt_insert_async(
    struct diskfs_bt_op        *op,
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    struct diskfs_inode        *inode,
    const struct diskfs_bt_key *key,
    const void                 *rec,
    uint32_t                    reclen,
    diskfs_bt_cb_t              cb,
    void                       *private_data);
static int diskfs_bt_lookup_async(
    struct diskfs_bt_op        *op,
    struct diskfs_thread       *thread,
    struct diskfs_inode        *inode,
    enum diskfs_bt_opcode       opcode,
    const struct diskfs_bt_key *key,
    struct diskfs_bt_key       *r_key,
    void                       *out,
    uint32_t                    out_cap,
    diskfs_bt_cb_t              cb,
    void                       *private_data);

/*
 * Add (remove=0) or remove (remove=1) an entry for `inum` in the durable
 * orphan-list inode's b+tree, within `txn`.  The orphan inode is acquired LAST
 * (it is below every file inum and is always taken last, so it is a leaf in
 * the lock order -> can't be in a deadlock cycle).  `done(priv)` is called
 * once the b+tree op completes.  For an insert, the orphaned inode's gen is
 * stored as the value (the mount scan reads it to reload the inode).
 */
struct diskfs_orphan_op {
    struct diskfs_thread *thread;
    struct diskfs_txn    *txn;
    uint64_t              inum;
    uint32_t              gen;
    int                   remove;
    void                  (*done)(
        void *priv);
    void                 *priv;
};

static void
diskfs_orphan_op_done_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *priv)
{
    struct diskfs_orphan_op *o = priv;
    void                     (*done)(
        void *) = o->done;
    void                    *dpriv = o->priv;

    (void) result;
    diskfs_bt_op_free(o->thread, op);
    free(o);
    done(dpriv);
} /* diskfs_orphan_op_done_cb */

static void
diskfs_orphan_op_acquired_cb(
    struct diskfs_inode *orphan_dir,
    int                  status,
    void                *priv)
{
    struct diskfs_orphan_op *o = priv;
    struct diskfs_bt_op     *op;
    struct diskfs_bt_key     key = { .type = DISKFS_REC_ORPHAN, .subkey = o->inum };

    chimera_diskfs_abort_if(status != CHIMERA_VFS_OK,
                            "orphan-list inode acquire failed: %d", status);

    op = diskfs_bt_op_alloc(o->thread);
    if (o->remove) {
        if (diskfs_bt_remove_async(op, o->thread, o->txn, orphan_dir, &key,
                                   diskfs_orphan_op_done_cb, o)) {
            diskfs_orphan_op_done_cb(op, op->result, o);
        }
    } else {
        if (diskfs_bt_insert_async(op, o->thread, o->txn, orphan_dir, &key,
                                   &o->gen, sizeof(o->gen),
                                   diskfs_orphan_op_done_cb, o)) {
            diskfs_orphan_op_done_cb(op, op->result, o);
        }
    }
} /* diskfs_orphan_op_acquired_cb */

static void
diskfs_orphan_op_start(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    uint64_t              inum,
    uint32_t              gen,
    int                   remove,
    void (               *done )(void *priv),
    void                 *priv)
{
    struct diskfs_orphan_op *o = malloc(sizeof(*o));

    o->thread = thread;
    o->txn    = txn;
    o->inum   = inum;
    o->gen    = gen;
    o->remove = remove;
    o->done   = done;
    o->priv   = priv;

    diskfs_inode_acquire(thread, txn, DISKFS_ORPHAN_INUM, DISKFS_ORPHAN_GEN,
                         DISKFS_INODE_LOCK_WRITE, diskfs_orphan_op_acquired_cb, o);
} /* diskfs_orphan_op_start */

/* ------------------------------------------------------------------ */
/* Background drainer: reclaim a large deleted inode incrementally.     */
/*                                                                      */
/* A large inode's tree can't be freed in one transaction (it would     */
/* flood the block-I/O queue), so it is drained in bounded batches: per */
/* transaction, remove up to DISKFS_DRAIN_BATCH of the lowest b+tree    */
/* entries (freeing a file extent's backing data; the remove itself     */
/* reclaims emptied node blocks via merge), commit, repeat -- then a    */
/* final transaction frees the home block + the inode struct.  Generic  */
/* over entry type (extents, dirents, symlink).  Each transaction holds */
/* only the one inode (no multi-inode lock ordering).  The orphan inode */
/* stays cached throughout (A5 never evicts nlink==0).                  */
/*                                                                      */
/* In-memory queue, processed one at a time per worker.  Crash-safe     */
/* resume via the durable orphan-list inode is a follow-up (Part B); a  */
/* crash mid-drain currently leaks the not-yet-freed remainder.         */
/* ------------------------------------------------------------------ */

#define DISKFS_DRAIN_BATCH 64

struct diskfs_drain {
    struct diskfs_thread *thread;
    uint64_t              inum;
    uint32_t              gen;
    struct diskfs_txn    *txn;
    struct diskfs_inode  *inode;
    int                   batch;
    struct diskfs_bt_key  found_key;
    uint8_t               recbuf[sizeof(struct diskfs_extent_rec)];
    struct diskfs_drain  *next;
};

static void diskfs_drain_kick(
    struct diskfs_thread *thread);
static void diskfs_drain_begin(
    struct diskfs_drain *d);

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

static void diskfs_drain_step(
    struct diskfs_drain *d);

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
    (void) txn;
    (void) status;
    diskfs_drain_complete(priv);
} /* diskfs_drain_final_cb */

/* The durable orphan entry is removed; finish reclaiming the inode itself
 * (home block + struct) in the same transaction and commit. */
static void
diskfs_drain_after_unrecord(void *priv)
{
    struct diskfs_drain *d = priv;
    uint32_t             dev;
    uint64_t             off = sm_inum_to_device_offset(d->thread->shared->space_map,
                                                        d->inum, &dev);

    diskfs_txn_pin_inode_block(d->thread, d->txn, d->inode, 0);
    diskfs_txn_free_space(d->thread, d->txn, dev, off, DISKFS_BLOCK_SIZE);
    diskfs_inode_free(d->thread, d->inode);
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
        /* Tree empty: remove the durable orphan entry, then (in the same txn)
         * free the home block + inode struct.  Removing the orphan entry last
         * means a crash before this commit just re-drains on the next mount
         * (idempotent); the orphan inode (3) is acquired last (leaf -> no
         * deadlock). */
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

/* ------------------------------------------------------------------ */
/* Mount-time orphan recovery: re-enqueue inodes left on the durable    */
/* orphan list by a crash mid-drain (draining is idempotent).           */
/* ------------------------------------------------------------------ */

struct diskfs_orphan_ent {
    uint64_t inum;
    uint32_t gen;
};

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

static void
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

    /* Load the orphan-list inode (nlink 1) and read its tree from its home
     * block, collecting every recorded orphan inum + gen. */
    odir = diskfs_inode_load_sync(thread, io, DISKFS_ORPHAN_INUM, DISKFS_ORPHAN_GEN, 0);
    if (!odir) {
        diskfs_mount_io_close(io);
        return;     /* not yet created (no orphans possible) */
    }

    off  = sm_inum_to_device_offset(shared->space_map, DISKFS_ORPHAN_INUM, &dev);
    obuf = malloc(DISKFS_BLOCK_SIZE);
    arr  = malloc(cap * sizeof(*arr));
    if (diskfs_mount_io_read(io, dev, obuf, DISKFS_BLOCK_SIZE, off) == 0) {
        diskfs_orphan_scan_node(thread, io, obuf, DISKFS_BT_ROOT_BASE, &arr, &n, &cap);
    }
    free(obuf);

    for (i = 0; i < n; i++) {
        /* Reload the orphaned (nlink==0) inode into cache, then enqueue it;
         * the drainer resumes its (possibly partially-drained) tree. */
        diskfs_inode_load_sync(thread, io, arr[i].inum, arr[i].gen, 1 /* allow_orphan */);
        diskfs_drain_enqueue(thread, arr[i].inum, arr[i].gen);
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
static int
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

/* ------------------------------------------------------------------ */
/* Async b+tree operation driver                                       */
/* ------------------------------------------------------------------ */

/* Copy a leaf slot's record + key into the op's output; returns true length. */
static inline int
diskfs_bt_op_emit(
    struct diskfs_bt_op *op,
    void                *buf,
    uint32_t             base,
    int                  idx)
{
    struct diskfs_bt_lslot *sl  = diskfs_bt_lslots(buf, base);
    uint32_t                len = sl[idx].len;

    if (op->r_key) {
        *op->r_key = sl[idx].key;
    }
    if (len > op->out_cap) {
        len = op->out_cap;
    }
    if (op->out) {
        memcpy(op->out, (char *) buf + base + sl[idx].off, len);
    }
    return (int) sl[idx].len;
} /* diskfs_bt_op_emit */

static inline void
diskfs_bt_complete(
    struct diskfs_bt_op *op,
    int                  result)
{
    int i;

    /* Release the descent pins (a write op's structural blocks stay pinned by
     * the transaction; this only drops the read pin taken during descent). */
    for (i = 0; i < op->npins; i++) {
        diskfs_block_release(op->thread, op->pins[i]);
    }
    op->npins = 0;

    /* Free an oversized record's heap staging buffer (small records sit in
     * recbuf and need no free). */
    if (op->rec && op->rec != op->recbuf) {
        free(op->rec);
    }
    op->rec = NULL;

    op->result = result;
    if (op->suspended) {
        op->cb(op, result, op->private_data);
    } else {
        op->done = 1;
    }
} /* diskfs_bt_complete */

/*
 * Drive (or resume) an async b+tree operation.  The traversal suspends and
 * returns whenever a needed node is not resident (diskfs_bt_block_get parks
 * the op on the block's waiter list); it is re-entered here from the resume
 * queue once the block loads.  All per-step state lives in *op so the loop is
 * safe to re-enter from the top of the current phase.
 */
static void
diskfs_bt_run(struct diskfs_bt_op *op)
{
    struct diskfs_thread *thread = op->thread;
    struct diskfs_inode  *inode  = op->inode;
    struct diskfs_block  *blk;
    void                 *buf;
    uint32_t              base;
    uint32_t              dev;
    uint64_t              off;

    for (;; ) {
        struct diskfs_bt_node_hdr *h;

        /*
         * Pre-reserve worst-case split space (one new node per tree level)
         * before the descent.  The refill journals and may park on a cold
         * AG-log block -- suspendable here (we re-enter this phase on resume) --
         * so the later synchronous modify's bt_alloc_node calls are guaranteed
         * pure cache draws that never journal.
         */
        if (op->phase == DISKFS_BT_PHASE_RESERVE) {
            /* Worst case a split allocates one node per tree level plus a new
             * root (height+1 <= DISKFS_BT_MAX_DEPTH+1); +2 for margin so the
             * modify can never exhaust the reservation and journal. */
            DISKFS_SM_JNL(jnl, thread, op->txn, diskfs_bt_op_resume_cb, op);
            int rrc = space_map_reserve(thread->shared->space_map,
                                        &thread->space_cache, &jnl,
                                        (uint64_t) (DISKFS_BT_MAX_DEPTH + 2) * DISKFS_BLOCK_SIZE);

            if (rrc == SM_AGAIN) {
                return;     /* parked; resumes back into this phase */
            }
            /* ENOSPC here is left to the modify's allocation to surface; just
             * proceed to the descent. */
            op->phase = DISKFS_BT_PHASE_DESCEND;
            continue;
        }

        /*
         * Remove rebalance can touch the immediate siblings of every node on
         * the descent path; fault them all in here, then run the synchronous
         * modify which is guaranteed not to miss.
         */
        if (op->phase == DISKFS_BT_PHASE_REBALANCE) {
            while (op->reb_level < op->depth) {
                struct diskfs_bt_path_ent *pe = &op->path[op->reb_level];
                struct diskfs_bt_node_hdr *ph;
                int                        ci, pn;
                void                      *pbuf;

                off = (pe->bptr == 0)
                ? sm_inum_to_device_offset(thread->shared->space_map, inode->inum, &dev)
                : sm_inum_to_device_offset(thread->shared->space_map, pe->bptr, &dev);
                blk = diskfs_bt_block_get(op, dev, off);
                if (!blk) {
                    return;
                }
                pbuf = blk->iov.data;
                ph   = diskfs_bt_hdr(pbuf, pe->base);
                ci   = pe->ci;
                pn   = ph->nitems;

                if (op->removed_idx == 0) {
                    if (ci - 1 >= 0) {
                        uint64_t sb = diskfs_bt_islots(pbuf, pe->base)[ci - 1].child;
                        off = sm_inum_to_device_offset(thread->shared->space_map, sb, &dev);
                        if (!diskfs_bt_block_get(op, dev, off)) {
                            return;
                        }
                    }
                    op->removed_idx = 1;
                }
                if (op->removed_idx == 1) {
                    if (ci + 1 < pn) {
                        uint64_t sb = diskfs_bt_islots(pbuf, pe->base)[ci + 1].child;
                        off = sm_inum_to_device_offset(thread->shared->space_map, sb, &dev);
                        if (!diskfs_bt_block_get(op, dev, off)) {
                            return;
                        }
                    }
                    op->removed_idx = 2;
                }
                op->reb_level++;
                op->removed_idx = 0;
            }

            diskfs_bt_complete(op, diskfs_bt_remove_locked(thread, op->txn, inode, &op->key));
            return;
        }

        if (op->phase == DISKFS_BT_PHASE_DESCEND && op->use_root) {
            off  = sm_inum_to_device_offset(thread->shared->space_map, inode->inum, &dev);
            base = DISKFS_BT_ROOT_BASE;
        } else {
            off  = sm_inum_to_device_offset(thread->shared->space_map, op->cur_bptr, &dev);
            base = 0;
        }

        blk = diskfs_bt_block_get(op, dev, off);
        if (!blk) {
            return;     /* suspended; resumed when the block loads */
        }
        buf = blk->iov.data;
        h   = diskfs_bt_hdr(buf, base);

        if (op->phase == DISKFS_BT_PHASE_DESCEND) {
            if (h->level > 0) {
                int ci = diskfs_bt_interior_search(buf, base, &op->key);

                if (op->opcode == DISKFS_BT_OP_INSERT ||
                    op->opcode == DISKFS_BT_OP_REMOVE) {
                    chimera_diskfs_abort_if(op->depth >= DISKFS_BT_MAX_DEPTH,
                                            "b+tree op: path too deep");
                    op->path[op->depth].bptr = op->use_root ? 0 : op->cur_bptr;
                    op->path[op->depth].base = base;
                    op->path[op->depth].ci   = ci;
                    op->depth++;
                }
                op->cur_bptr = diskfs_bt_islots(buf, base)[ci].child;
                op->use_root = 0;
                continue;
            }

            /* At the leaf. */
            if (op->opcode == DISKFS_BT_OP_INSERT) {
                diskfs_bt_insert_locked(thread, op->txn, inode, &op->key,
                                        op->rec, op->reclen);
                diskfs_bt_complete(op, 0);
                return;
            } else if (op->opcode == DISKFS_BT_OP_REMOVE) {
                /* Path faulted in; now fault in rebalance siblings. */
                op->phase       = DISKFS_BT_PHASE_REBALANCE;
                op->reb_level   = 0;
                op->removed_idx = 0;
                continue;
            } else if (op->opcode == DISKFS_BT_OP_LOOKUP_EXACT) {
                int exact, idx = diskfs_bt_leaf_search(buf, base, &op->key, &exact);

                diskfs_bt_complete(op, exact ? diskfs_bt_op_emit(op, buf, base, idx) : -1);
                return;
            } else if (op->opcode == DISKFS_BT_OP_LOOKUP_GE) {
                int exact, idx = h->nitems ? diskfs_bt_leaf_search(buf, base, &op->key, &exact) : 0;

                if (idx < h->nitems) {
                    diskfs_bt_complete(op, diskfs_bt_op_emit(op, buf, base, idx));
                    return;
                }
                op->cur_bptr = h->next_leaf;
                op->use_root = 0;
                op->phase    = DISKFS_BT_PHASE_WALK_NEXT;
                if (op->cur_bptr == 0) {
                    diskfs_bt_complete(op, -1);
                    return;
                }
                continue;
            } else {     /* LOOKUP_LE */
                int exact = 0;
                int idx   = h->nitems ? diskfs_bt_leaf_search(buf, base, &op->key, &exact) : 0;
                int fidx  = h->nitems ? (exact ? idx : idx - 1) : -1;

                if (fidx >= 0) {
                    diskfs_bt_complete(op, diskfs_bt_op_emit(op, buf, base, fidx));
                    return;
                }
                op->cur_bptr = h->prev_leaf;
                op->use_root = 0;
                op->phase    = DISKFS_BT_PHASE_WALK_PREV;
                if (op->cur_bptr == 0) {
                    diskfs_bt_complete(op, -1);
                    return;
                }
                continue;
            }
        } else if (op->phase == DISKFS_BT_PHASE_WALK_NEXT) {
            if (h->nitems > 0) {
                diskfs_bt_complete(op, diskfs_bt_op_emit(op, buf, 0, 0));
                return;
            }
            op->cur_bptr = h->next_leaf;
            if (op->cur_bptr == 0) {
                diskfs_bt_complete(op, -1);
                return;
            }
            continue;
        } else {     /* DISKFS_BT_PHASE_WALK_PREV */
            if (h->nitems > 0) {
                diskfs_bt_complete(op, diskfs_bt_op_emit(op, buf, 0, h->nitems - 1));
                return;
            }
            op->cur_bptr = h->prev_leaf;
            if (op->cur_bptr == 0) {
                diskfs_bt_complete(op, -1);
                return;
            }
            continue;
        }
    }
} /* diskfs_bt_run */

/*
 * Synchronous-completion sink for the transitional sync wrappers below: every
 * traversal currently hits in cache, so the op completes inline before the
 * entry point returns and we capture the result here.
 */
/*
 * Start an async lookup on a caller-owned op.  Returns 1 if it completed
 * synchronously (result in op->result, outputs already written into
 * out/r_key), 0 if it suspended (op->cb will be invoked with the result once
 * the deferring I/O completes).
 */
static int
diskfs_bt_lookup_async(
    struct diskfs_bt_op        *op,
    struct diskfs_thread       *thread,
    struct diskfs_inode        *inode,
    enum diskfs_bt_opcode       opcode,
    const struct diskfs_bt_key *key,
    struct diskfs_bt_key       *r_key,
    void                       *out,
    uint32_t                    out_cap,
    diskfs_bt_cb_t              cb,
    void                       *private_data)
{
    memset(op, 0, sizeof(*op));
    op->thread       = thread;
    op->inode        = inode;
    op->opcode       = opcode;
    op->phase        = DISKFS_BT_PHASE_DESCEND;
    op->key          = *key;
    op->r_key        = r_key;
    op->out          = out;
    op->out_cap      = out_cap;
    op->use_root     = 1;
    op->cb           = cb;
    op->private_data = private_data;

    diskfs_bt_run(op);
    return op->done;
} /* diskfs_bt_lookup_async */

static int
diskfs_bt_insert_async(
    struct diskfs_bt_op        *op,
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    struct diskfs_inode        *inode,
    const struct diskfs_bt_key *key,
    const void                 *rec,
    uint32_t                    reclen,
    diskfs_bt_cb_t              cb,
    void                       *private_data)
{
    /* No record can exceed a single node; callers (e.g. the symlink path)
     * reject oversized payloads before reaching here, so this is a true
     * invariant. */
    chimera_diskfs_abort_if(reclen > DISKFS_BT_NODE_CAP, "b+tree record too large");

    memset(op, 0, sizeof(*op));
    op->thread = thread;
    op->txn    = txn;
    op->inode  = inode;
    op->opcode = DISKFS_BT_OP_INSERT;
    /* Reserve worst-case split space before descending, so the synchronous
    * modify's node allocs are pure cache draws (never journal/suspend). */
    op->phase        = DISKFS_BT_PHASE_RESERVE;
    op->key          = *key;
    op->reclen       = reclen;
    op->use_root     = 1;
    op->cb           = cb;
    op->private_data = private_data;
    /* Stage the payload in op-owned storage so it survives suspension; recbuf
     * for the common (small) case, a heap buffer for an oversized one. */
    op->rec = (reclen > sizeof(op->recbuf)) ? malloc(reclen) : op->recbuf;
    memcpy(op->rec, rec, reclen);

    diskfs_bt_run(op);
    return op->done;
} /* diskfs_bt_insert_async */

static int
diskfs_bt_remove_async(
    struct diskfs_bt_op        *op,
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    struct diskfs_inode        *inode,
    const struct diskfs_bt_key *key,
    diskfs_bt_cb_t              cb,
    void                       *private_data)
{
    memset(op, 0, sizeof(*op));
    op->thread       = thread;
    op->txn          = txn;
    op->inode        = inode;
    op->opcode       = DISKFS_BT_OP_REMOVE;
    op->phase        = DISKFS_BT_PHASE_DESCEND;
    op->key          = *key;
    op->use_root     = 1;
    op->cb           = cb;
    op->private_data = private_data;

    diskfs_bt_run(op);
    return op->done;
} /* diskfs_bt_remove_async */

/*
 * Synchronous wrappers used by init/mount-time paths (which run before
 * concurrent load, so everything is resident).  They assert that the op did
 * not suspend, since a cache miss cannot occur until block eviction exists.
 */
static int
diskfs_bt_lookup_sync(
    struct diskfs_thread       *thread,
    struct diskfs_inode        *inode,
    enum diskfs_bt_opcode       opcode,
    const struct diskfs_bt_key *key,
    struct diskfs_bt_key       *r_key,
    void                       *out,
    uint32_t                    out_cap)
{
    struct diskfs_bt_op op;

    chimera_diskfs_abort_if(!diskfs_bt_lookup_async(&op, thread, inode, opcode, key,
                                                    r_key, out, out_cap, NULL, NULL),
                            "b+tree lookup suspended on a cache miss (no eviction yet)");
    return op.result;
} /* diskfs_bt_lookup_sync */

static void
diskfs_bt_insert(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    struct diskfs_inode        *inode,
    const struct diskfs_bt_key *key,
    const void                 *rec,
    uint32_t                    reclen)
{
    struct diskfs_bt_op op;

    chimera_diskfs_abort_if(!diskfs_bt_insert_async(&op, thread, txn, inode, key,
                                                    rec, reclen, NULL, NULL),
                            "b+tree insert suspended on a cache miss (no eviction yet)");
} /* diskfs_bt_insert */

static int
diskfs_bt_remove(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    struct diskfs_inode        *inode,
    const struct diskfs_bt_key *key)
{
    struct diskfs_bt_op op;

    chimera_diskfs_abort_if(!diskfs_bt_remove_async(&op, thread, txn, inode, key,
                                                    NULL, NULL),
                            "b+tree remove suspended on a cache miss (no eviction yet)");
    return op.result;
} /* diskfs_bt_remove */

static int
diskfs_bt_lookup_exact(
    struct diskfs_thread       *thread,
    struct diskfs_inode        *inode,
    const struct diskfs_bt_key *key,
    void                       *out,
    uint32_t                    out_cap)
{
    return diskfs_bt_lookup_sync(thread, inode, DISKFS_BT_OP_LOOKUP_EXACT,
                                 key, NULL, out, out_cap);
} /* diskfs_bt_lookup_exact */

/* Descend (floor-style) to the leaf that would hold key.  Returns the leaf
 * block (RCU lookup; aborts if not resident) without entering a critical
 * section; the caller must re-enter RCU around its own access.  Used as a
 * helper only where the whole traversal is inside one logic block. */

/*
 * Smallest key >= search key (ceil).  Copies the found key into *r_key and
 * the record into out; returns record length, or -1 if no such key.
 */
static int
diskfs_bt_lookup_ge(
    struct diskfs_thread       *thread,
    struct diskfs_inode        *inode,
    const struct diskfs_bt_key *key,
    struct diskfs_bt_key       *r_key,
    void                       *out,
    uint32_t                    out_cap)
{
    return diskfs_bt_lookup_sync(thread, inode, DISKFS_BT_OP_LOOKUP_GE,
                                 key, r_key, out, out_cap);
} /* diskfs_bt_lookup_ge */

/*
 * Largest key <= search key (floor).  Copies the found key into *r_key and
 * the record into out; returns record length, or -1 if no such key.
 */
static int
diskfs_bt_lookup_le(
    struct diskfs_thread       *thread,
    struct diskfs_inode        *inode,
    const struct diskfs_bt_key *key,
    struct diskfs_bt_key       *r_key,
    void                       *out,
    uint32_t                    out_cap)
{
    return diskfs_bt_lookup_sync(thread, inode, DISKFS_BT_OP_LOOKUP_LE,
                                 key, r_key, out, out_cap);
} /* diskfs_bt_lookup_le */

/* ------------------------------------------------------------------ */
/* Directory / symlink records over the inode b+tree                   */
/* ------------------------------------------------------------------ */

static inline struct diskfs_bt_key
diskfs_dirent_key(uint64_t hash)
{
    struct diskfs_bt_key k = { .type = DISKFS_REC_DIRENT, .subkey = hash };

    return k;
} /* diskfs_dirent_key */

static void
diskfs_dir_insert(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *dir,
    uint64_t              hash,
    const char           *name,
    int                   namelen,
    uint64_t              child_inum,
    uint32_t              child_gen)
{
    char                      buf[DISKFS_DIRENT_REC_MAX];
    struct diskfs_dirent_rec *r   = (struct diskfs_dirent_rec *) buf;
    struct diskfs_bt_key      key = diskfs_dirent_key(hash);

    r->inum     = child_inum;
    r->gen      = child_gen;
    r->name_len = (uint16_t) namelen;
    memcpy(r->name, name, namelen);

    diskfs_bt_insert(thread, txn, dir, &key,
                     buf, sizeof(*r) + namelen);
} /* diskfs_dir_insert */

static int
diskfs_dir_remove(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *dir,
    uint64_t              hash)
{
    struct diskfs_bt_key key = diskfs_dirent_key(hash);

    return diskfs_bt_remove(thread, txn, dir, &key);
} /* diskfs_dir_remove */

/*
 * Find the next directory entry whose hash is >= from_hash.  Returns 0 and
 * fills the out params (the entry's hash, child inum/gen, name) or -1 when
 * there are no more dirents.
 */
static int
diskfs_dir_next(
    struct diskfs_thread *thread,
    struct diskfs_inode  *dir,
    uint64_t              from_hash,
    uint64_t             *r_hash,
    uint64_t             *r_inum,
    uint32_t             *r_gen,
    char                 *name,
    int                  *r_namelen)
{
    char                      buf[DISKFS_DIRENT_REC_MAX];
    struct diskfs_dirent_rec *r   = (struct diskfs_dirent_rec *) buf;
    struct diskfs_bt_key      key = diskfs_dirent_key(from_hash);
    struct diskfs_bt_key      found;
    int                       len;

    len = diskfs_bt_lookup_ge(thread, dir, &key, &found, buf, sizeof(buf));
    if (len < 0 || found.type != DISKFS_REC_DIRENT) {
        return -1;
    }
    *r_hash    = found.subkey;
    *r_inum    = r->inum;
    *r_gen     = r->gen;
    *r_namelen = r->name_len;
    memcpy(name, r->name, r->name_len);
    return 0;
} /* diskfs_dir_next */

/*
 * Async directory-record helpers (thin wrappers over the b+tree op driver).
 * Each returns 1 if it completed synchronously (result in op->result; the
 * looked-up record, if any, written into rec_out), or 0 if it suspended (cb
 * fires with the result later).  Callers parse the dirent record themselves.
 */
static int
diskfs_dir_lookup_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_inode  *dir,
    uint64_t              hash,
    void                 *rec_out,
    uint32_t              rec_cap,
    diskfs_bt_cb_t        cb,
    void                 *private_data)
{
    struct diskfs_bt_key key = diskfs_dirent_key(hash);

    return diskfs_bt_lookup_async(op, thread, dir, DISKFS_BT_OP_LOOKUP_EXACT,
                                  &key, NULL, rec_out, rec_cap, cb, private_data);
} /* diskfs_dir_lookup_async */

static int
diskfs_dir_next_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_inode  *dir,
    uint64_t              from_hash,
    struct diskfs_bt_key *r_key,
    void                 *rec_out,
    uint32_t              rec_cap,
    diskfs_bt_cb_t        cb,
    void                 *private_data)
{
    struct diskfs_bt_key key = diskfs_dirent_key(from_hash);

    return diskfs_bt_lookup_async(op, thread, dir, DISKFS_BT_OP_LOOKUP_GE,
                                  &key, r_key, rec_out, rec_cap, cb, private_data);
} /* diskfs_dir_next_async */

static int
diskfs_dir_insert_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *dir,
    uint64_t              hash,
    const char           *name,
    int                   namelen,
    uint64_t              child_inum,
    uint32_t              child_gen,
    diskfs_bt_cb_t        cb,
    void                 *private_data)
{
    char                      buf[DISKFS_DIRENT_REC_MAX];
    struct diskfs_dirent_rec *r   = (struct diskfs_dirent_rec *) buf;
    struct diskfs_bt_key      key = diskfs_dirent_key(hash);

    r->inum     = child_inum;
    r->gen      = child_gen;
    r->name_len = (uint16_t) namelen;
    memcpy(r->name, name, namelen);

    return diskfs_bt_insert_async(op, thread, txn, dir, &key, buf,
                                  sizeof(*r) + namelen, cb, private_data);
} /* diskfs_dir_insert_async */

static int
diskfs_dir_remove_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *dir,
    uint64_t              hash,
    diskfs_bt_cb_t        cb,
    void                 *private_data)
{
    struct diskfs_bt_key key = diskfs_dirent_key(hash);

    return diskfs_bt_remove_async(op, thread, txn, dir, &key, cb, private_data);
} /* diskfs_dir_remove_async */

static void
diskfs_symlink_set(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode,
    const void           *target,
    int                   len)
{
    struct diskfs_bt_key key = { .type = DISKFS_REC_SYMLINK, .subkey = 0 };

    diskfs_bt_insert(thread, txn, inode, &key, target, len);
} /* diskfs_symlink_set */

static int
diskfs_symlink_set_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode,
    const void           *target,
    int                   len,
    diskfs_bt_cb_t        cb,
    void                 *private_data)
{
    struct diskfs_bt_key key = { .type = DISKFS_REC_SYMLINK, .subkey = 0 };

    return diskfs_bt_insert_async(op, thread, txn, inode, &key, target, len,
                                  cb, private_data);
} /* diskfs_symlink_set_async */

static int
diskfs_symlink_get(
    struct diskfs_thread *thread,
    struct diskfs_inode  *inode,
    void                 *out,
    uint32_t              cap)
{
    struct diskfs_bt_key key = { .type = DISKFS_REC_SYMLINK, .subkey = 0 };

    return diskfs_bt_lookup_exact(thread, inode, &key, out, cap);
} /* diskfs_symlink_get */

/* ------------------------------------------------------------------ */
/* File extents over the inode b+tree                                  */
/* ------------------------------------------------------------------ */

static inline struct diskfs_bt_key
diskfs_extent_key(uint64_t file_offset)
{
    struct diskfs_bt_key k = { .type = DISKFS_REC_EXTENT, .subkey = file_offset };

    return k;
} /* diskfs_extent_key */

static void
diskfs_ext_insert(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode,
    uint64_t              file_offset,
    uint64_t              length,
    uint32_t              device_id,
    uint64_t              device_offset)
{
    struct diskfs_extent_rec rec = {
        .length        = length,
        .device_id     = device_id,
        .pad           = 0,
        .device_offset = device_offset,
    };
    struct diskfs_bt_key     key = diskfs_extent_key(file_offset);

    diskfs_bt_insert(thread, txn, inode, &key, &rec, sizeof(rec));
} /* diskfs_ext_insert */

static int
diskfs_ext_remove(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode,
    uint64_t              file_offset)
{
    struct diskfs_bt_key key = diskfs_extent_key(file_offset);

    return diskfs_bt_remove(thread, txn, inode, &key);
} /* diskfs_ext_remove */

/* Fill *out with the extent whose file_offset is the largest <= the given
 * offset; returns 1 if found, 0 otherwise.  The node/next/buffer fields of
 * *out are left untouched (callers only read the on-disk fields). */
static int
diskfs_ext_floor(
    struct diskfs_thread *thread,
    struct diskfs_inode  *inode,
    uint64_t              file_offset,
    struct diskfs_extent *out)
{
    struct diskfs_bt_key     key = diskfs_extent_key(file_offset);
    struct diskfs_bt_key     found;
    struct diskfs_extent_rec rec;
    int                      len;

    len = diskfs_bt_lookup_le(thread, inode, &key, &found, &rec, sizeof(rec));
    if (len < 0 || found.type != DISKFS_REC_EXTENT) {
        return 0;
    }
    out->file_offset   = found.subkey;
    out->length        = (uint32_t) rec.length;
    out->device_id     = rec.device_id;
    out->device_offset = rec.device_offset;
    return 1;
} /* diskfs_ext_floor */

/* Fill *out with the extent whose file_offset is the smallest >= the given
 * offset; returns 1 if found, 0 otherwise. */
static int
diskfs_ext_ceil(
    struct diskfs_thread *thread,
    struct diskfs_inode  *inode,
    uint64_t              file_offset,
    struct diskfs_extent *out)
{
    struct diskfs_bt_key     key = diskfs_extent_key(file_offset);
    struct diskfs_bt_key     found;
    struct diskfs_extent_rec rec;
    int                      len;

    len = diskfs_bt_lookup_ge(thread, inode, &key, &found, &rec, sizeof(rec));
    if (len < 0 || found.type != DISKFS_REC_EXTENT) {
        return 0;
    }
    out->file_offset   = found.subkey;
    out->length        = (uint32_t) rec.length;
    out->device_id     = rec.device_id;
    out->device_offset = rec.device_offset;
    return 1;
} /* diskfs_ext_ceil */

/* Next extent strictly after after_file_offset. */
static inline int
diskfs_ext_next(
    struct diskfs_thread *thread,
    struct diskfs_inode  *inode,
    uint64_t              after_file_offset,
    struct diskfs_extent *out)
{
    return diskfs_ext_ceil(thread, inode, after_file_offset + 1, out);
} /* diskfs_ext_next */

/*
 * Async extent lookups (floor / ceil / next).  Each returns 1 if it completed
 * synchronously, 0 if it suspended (cb fires later); on completion the result
 * is in op->result and the record + found key are in rec_out / op->found_key.
 * Use diskfs_ext_from_op() in the callback to materialize the extent.
 */
static int
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

static int
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

static int
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

static int
diskfs_ext_insert_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode,
    uint64_t              file_offset,
    uint64_t              length,
    uint32_t              device_id,
    uint64_t              device_offset,
    diskfs_bt_cb_t        cb,
    void                 *private_data)
{
    struct diskfs_extent_rec rec = {
        .length        = length,
        .device_id     = device_id,
        .pad           = 0,
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

/* Materialize the extent from a completed async lookup op (result + the
 * record left in op->out + the key in op->found_key).  Returns 1 if a valid
 * extent record was found, 0 otherwise. */
static inline int
diskfs_ext_from_op(
    struct diskfs_bt_op  *op,
    int                   result,
    struct diskfs_extent *out)
{
    struct diskfs_extent_rec *rec = op->out;

    if (result < 0 || op->found_key.type != DISKFS_REC_EXTENT) {
        return 0;
    }
    out->file_offset   = op->found_key.subkey;
    out->length        = (uint32_t) rec->length;
    out->device_id     = rec->device_id;
    out->device_offset = rec->device_offset;
    return 1;
} /* diskfs_ext_from_op */

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
 * Allocate a bare inode struct for a freshly-minted inum.  The 4 KiB
 * metadata block on storage has already been carved out of the space map;
 * for now we leak that block and only use its address to derive the inum.
 */
static inline struct diskfs_inode *
diskfs_inode_struct_new(uint64_t inum)
{
    struct diskfs_inode *inode = calloc(1, sizeof(*inode));

    inode->inum   = inum;
    inode->gen    = 1;
    inode->refcnt = 1;
    /* readers/writer/wait_head/wait_tail zeroed by calloc */
    return inode;
} /* diskfs_inode_struct_new */

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
static void
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

        diskfs_inode_lru_unlink(shard, inode);
        rb_tree_remove(&shard->inodes, &inode->node);
        shard->ninodes--;
        free(inode);
        return;
    }
} /* diskfs_inode_cache_recycle_locked */

static inline void
diskfs_inode_cache_insert(
    struct diskfs_shared *shared,
    struct diskfs_inode  *inode)
{
    struct diskfs_inode_shard *shard = diskfs_inode_shard(shared, inode->inum);

    pthread_mutex_lock(&shard->lock);
    diskfs_inode_cache_recycle_locked(shared, shard);
    rb_tree_insert(&shard->inodes, inum, inode);
    shard->ninodes++;
    pthread_mutex_unlock(&shard->lock);
} /* diskfs_inode_cache_insert */

/*
 * Fault an inode in from disk on a cache miss (remounted filesystem only).
 * Reads the inode's home block, validates the on-disk dinode against the
 * requested inum/gen, constructs + caches the inode, then re-drives
 * diskfs_inode_acquire (which now hits the cache and grants normally).  The
 * inode's b+tree blocks load lazily via diskfs_bt_block_get as they are
 * traversed.
 */
struct diskfs_inode_load_ctx {
    struct diskfs_thread *thread;
    struct diskfs_txn    *txn;
    uint64_t              inum;
    uint32_t              gen;
    enum diskfs_inode_lock_mode mode;
    diskfs_inode_cb_t     cb;
    void                 *private_data;
    struct evpl_iovec     iov;
};

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
        inode              = diskfs_inode_struct_new(lc->inum);
        inode->gen         = di->gen;
        inode->mode        = di->mode;
        inode->nlink       = di->nlink;
        inode->uid         = di->uid;
        inode->gid         = di->gid;
        inode->rdev        = di->rdev;
        inode->size        = di->size;
        inode->space_used  = di->space_used;
        inode->atime_sec   = di->atime_sec;
        inode->atime_nsec  = di->atime_nsec;
        inode->mtime_sec   = di->mtime_sec;
        inode->mtime_nsec  = di->mtime_nsec;
        inode->ctime_sec   = di->ctime_sec;
        inode->ctime_nsec  = di->ctime_nsec;
        inode->parent_inum = di->parent_inum;
        inode->parent_gen  = di->parent_gen;
        rb_tree_insert(&shard->inodes, inum, inode);
        shard->ninodes++;
    }
    pthread_mutex_unlock(&shard->lock);

    /* Seed the inode's home block (dinode + embedded b+tree root) into the
     * block cache from the disk image, so the b+tree traversal and inode-block
     * pin find the real contents instead of a zero-created block.  No writer
     * can be modifying it yet -- the lock isn't granted until the re-acquire
     * below.  Claim is_new (no disk read): we already hold the freshly-read
     * image in lc->iov and overwrite the whole block below, so reading it back
     * would be redundant -- and a synchronous read here cannot reach a VFIO
     * device anyway. */
    {
        uint32_t             dev;
        uint64_t             off = sm_inum_to_device_offset(shared->space_map,
                                                            lc->inum, &dev);
        struct diskfs_block *blk = diskfs_block_claim(thread, dev, off, 1);

        memcpy(blk->iov.data, lc->iov.data, DISKFS_BLOCK_SIZE);
        diskfs_block_unpin(thread, blk, DISKFS_BLOCK_CLEAN);
    }

    evpl_iovec_release(thread->evpl, &lc->iov);

    /* Now resident: re-drive the acquire to grant the lock as usual. */
    diskfs_inode_acquire(thread, lc->txn, lc->inum, lc->gen, lc->mode,
                         lc->cb, lc->private_data);
    free(lc);
} /* diskfs_inode_load_complete */

static void
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
    evpl_block_read(thread->evpl, thread->queue[dev], &lc->iov, 1, off,
                    diskfs_inode_load_complete, lc);
} /* diskfs_inode_load */

/* Retry context for an inode allocation whose reservation refill parked on a
 * cold AG-log block; the continuation re-drives the whole allocation. */
struct diskfs_inode_alloc_ctx {
    struct diskfs_thread *thread;
    struct diskfs_txn    *txn;
    diskfs_inode_cb_t     cb;
    void                 *private_data;
};

static void diskfs_inode_alloc_resume(
    struct diskfs_thread *thread,
    void                 *arg);

/*
 * Allocate a new inode: grab a 4 KiB metadata block from the space map to
 * mint the inum, create the in-memory inode, publish it write-locked into
 * the cache, and record it in the transaction.
 */
static inline void
diskfs_inode_alloc_async(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    diskfs_inode_cb_t     cb,
    void                 *private_data)
{
    struct diskfs_shared          *shared = thread->shared;
    struct diskfs_inode           *inode;
    struct diskfs_inode_alloc_ctx *actx;
    uint32_t                       device_id;
    uint64_t                       device_offset, inum;
    int                            rc;

    /* The reservation refill journals and may park on a cold log block; carry
     * the retry context so the resumed allocation re-drives from the top. */
    actx               = malloc(sizeof(*actx));
    actx->thread       = thread;
    actx->txn          = txn;
    actx->cb           = cb;
    actx->private_data = private_data;

    DISKFS_SM_JNL(jnl, thread, txn, diskfs_inode_alloc_resume, actx);
    rc = space_map_alloc(shared->space_map, &thread->space_cache, &jnl,
                         SM_BLOCK_SIZE, &device_id, &device_offset);
    if (rc == SM_AGAIN) {
        return;     /* parked; diskfs_inode_alloc_resume re-runs (owns actx) */
    }
    free(actx);
    if (unlikely(rc != 0)) {
        cb(NULL, CHIMERA_VFS_ENOSPC, private_data);
        return;
    }

    inum  = sm_inum_from_device_offset(shared->space_map, device_id, device_offset);
    inode = diskfs_inode_struct_new(inum);

    /* New dirty inode: write-locked by this (write) txn from birth. */
    inode->writer = 1;

    diskfs_inode_cache_insert(shared, inode);
    diskfs_txn_add_slot(txn, inode, DISKFS_INODE_LOCK_WRITE);

    /* Claim and pin the inode's freshly-allocated home block. */
    diskfs_txn_pin_inode_block(thread, txn, inode, 1);

    cb(inode, CHIMERA_VFS_OK, private_data);
} /* diskfs_inode_alloc_async */

static void
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

/*
 * Tear down an inode's contents when its last link/reference is dropped.
 * We bump the generation so stale file handles return ESTALE, but we do
 * NOT remove it from the cache or free the struct yet (no eviction), and
 * we leak its 4 KiB metadata block.  The caller still holds the inode's
 * write lock via the transaction; the lock is released at commit.
 */
static inline void
diskfs_inode_free(
    struct diskfs_thread *thread,
    struct diskfs_inode  *inode)
{
    (void) thread;

    /* All inode contents (dirents, extents, symlink target) live in the
     * inode's b+tree blocks, which are leaked for now (no space reclaim
     * yet); the device ranges backing file extents are returned to the
     * space map by truncate/deallocate before this point. */

    inode->gen++;
    inode->refcnt = 0;
} /* diskfs_inode_free */

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


static inline struct diskfs_kv_entry *
diskfs_kv_entry_alloc(
    struct diskfs_thread *thread,
    uint64_t              hash,
    const void           *key,
    uint32_t              key_len,
    const void           *value,
    uint32_t              value_len)
{
    struct diskfs_kv_entry *entry;

    entry = slab_allocator_alloc(thread->allocator, sizeof(*entry));

    entry->hash      = hash;
    entry->key_len   = key_len;
    entry->value_len = value_len;
    entry->key       = slab_allocator_alloc(thread->allocator, key_len);
    entry->value     = slab_allocator_alloc(thread->allocator, value_len);
    memcpy(entry->key, key, key_len);
    memcpy(entry->value, value, value_len);

    return entry;
} /* diskfs_kv_entry_alloc */

static void
diskfs_kv_entry_free(
    struct diskfs_thread   *thread,
    struct diskfs_kv_entry *entry)
{
    slab_allocator_free(thread->allocator, entry->key, entry->key_len);
    slab_allocator_free(thread->allocator, entry->value, entry->value_len);
    slab_allocator_free(thread->allocator, entry, sizeof(*entry));
} /* diskfs_kv_entry_free */

static void
diskfs_kv_entry_release(
    struct rb_node *node,
    void           *private_data)
{
    struct diskfs_kv_entry *entry = container_of(node, struct diskfs_kv_entry, node);

    free(entry->key);
    free(entry->value);
    free(entry);
} /* diskfs_kv_entry_release */

static inline int
diskfs_thread_alloc_space(
    struct diskfs_thread *thread,
    struct diskfs_txn *txn,
    int64_t desired_size,
    uint64_t *r_device_id,
    uint64_t *r_device_offset,
    void ( *resume )(struct diskfs_thread *, void *),
    void *resume_arg)
{
    uint32_t dev_id;
    int      rc;

    DISKFS_SM_JNL(jnl, thread, txn, resume, resume_arg);
    rc = space_map_alloc(thread->shared->space_map, &thread->space_cache, &jnl,
                         (uint64_t) desired_size, &dev_id, r_device_offset);

    if (rc == SM_AGAIN) {
        return SM_AGAIN;        /* parked; caller's resume re-drives */
    }
    if (rc != 0) {
        return CHIMERA_VFS_ENOSPC;
    }

    *r_device_id = dev_id;
    return 0;
} /* diskfs_thread_alloc_space */

static inline void
diskfs_thread_free_space(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    uint32_t              device_id,
    uint64_t              device_offset,
    uint64_t              length)
{
    /* Pending free: journal now, apply on commit (hardens the data path's
     * abort behavior too -- an aborted txn no longer leaves the range freed
     * in memory while its redo is discarded). */
    diskfs_txn_free_space(thread, txn, device_id, device_offset, length);
} /* diskfs_thread_free_space */

/* ------------------------------------------------------------------ */
/* Transaction plumbing                                                */
/* ------------------------------------------------------------------ */

static inline struct diskfs_txn *
diskfs_txn_begin(
    struct diskfs_thread *thread,
    enum diskfs_txn_type  type)
{
    struct diskfs_txn *txn = thread->txn_free_list;

    if (txn) {
        thread->txn_free_list = txn->next;
    } else {
        txn = malloc(sizeof(*txn));
    }

    txn->type          = type;
    txn->thread        = thread;
    txn->next          = NULL;
    txn->num_inodes    = 0;
    txn->blocks        = NULL;
    txn->pending_frees = NULL;
    return txn;
} /* diskfs_txn_begin */

static inline void
diskfs_txn_release(struct diskfs_txn *txn)
{
    struct diskfs_thread *thread = txn->thread;

    txn->next             = thread->txn_free_list;
    thread->txn_free_list = txn;
} /* diskfs_txn_release */

static inline void
diskfs_txn_abort(struct diskfs_txn *txn)
{
    /* Discard pending frees (their journaled FREE deltas never commit, so the
     * ranges stay allocated).  Drop any blocks the aborted txn pinned (their
     * contents are discarded) and release the inode locks.  NOTE: the in-memory
     * allocator alloc deltas applied during the txn are still not rolled back
     * here -- a pre-existing transaction-atomicity gap, separate from frees. */
    diskfs_txn_discard_frees(txn);
    diskfs_txn_unpin_blocks(txn, DISKFS_BLOCK_CLEAN);
    diskfs_txn_unlock_all(txn);
    diskfs_txn_release(txn);
} /* diskfs_txn_abort */

/* Synchronous commit today; placeholder for the intent log routing
 * added in phase 3. */
/* Forward decls — definition below. */
static inline void diskfs_txn_commit(
    struct diskfs_txn     *txn,
    diskfs_txn_commit_cb_t cb,
    void                  *private_data);

static void
diskfs_txn_request_complete_cb(
    struct diskfs_txn *txn,
    int                status,
    void              *private_data)
{
    struct chimera_vfs_request *request = private_data;

    (void) txn;
    if (status != 0 && request->status == CHIMERA_VFS_OK) {
        request->status = status;
    }
    request->complete(request);
} /* diskfs_txn_request_complete_cb */

/* Op-level helpers: every op ends with one of these. */
static inline void
diskfs_op_fail(
    struct chimera_vfs_request *request,
    struct diskfs_txn          *txn,
    int                         status)
{
    request->status = status;
    if (txn) {
        diskfs_txn_abort(txn);
    }
    request->complete(request);
} /* diskfs_op_fail */

static inline void
diskfs_op_ok(
    struct chimera_vfs_request *request,
    struct diskfs_txn          *txn)
{
    request->status = CHIMERA_VFS_OK;
    if (txn) {
        diskfs_txn_commit(txn, diskfs_txn_request_complete_cb, request);
    } else {
        request->complete(request);
    }
} /* diskfs_op_ok */

/* ------------------------------------------------------------------ */
/* Intent log: SPSC ring helpers + doorbell callbacks + thread         */
/* ------------------------------------------------------------------ */

/* Completion context for an in-flight redo-record write. */
struct diskfs_redo_ctx {
    struct diskfs_intent_log *il;
    struct diskfs_iq_channel *ch;
    struct diskfs_iq_entry    entry;
    struct diskfs_il_record  *rec;     /* owns the record image (iovs) */
    int                       segments; /* outstanding journal writes (see below) */
};

/*
 * A record's scatter-gather image can exceed the block backend's per-request
 * iovec limit (io_uring caps at 64), so the journal write for a large metadata
 * txn is issued in consecutive chunks sharing one redo_ctx; the record is
 * durable only when the last chunk completes.
 */
#define DISKFS_IL_MAX_IOV    64

/*
 * The intent-log thread submits all its block writes (redo records + tail-push
 * home writes) onto a single libaio/io_uring queue, whose submission ring is
 * bounded (libaio_max_pending defaults to 256).  Each wake processes every
 * channel's whole submission queue, so without a cap the redo writes for a
 * burst of queued txns can flood the ring before any completion drains it
 * ("too many pending iocbs").  Gate redo submission once this many writes are
 * in flight, and resume from a completion once it drains back to the low
 * watermark -- leaving headroom for the in-flight tail-push record's writes.
 */
#define DISKFS_IL_IOCB_CAP   128
#define DISKFS_IL_IOCB_LOWAT 64

/*
 * On-log record layout (all 4 KiB-aligned for zero-copy scatter-gather):
 *   [ header region: redo_header + num_blocks * redo_block_header, 4K-padded ]
 *   [ block 0 data (4 KiB) ][ block 1 data ] ... [ block N-1 data ]
 * The header region is materialized into one iovec; each data block is a
 * zero-copy clone of the cache block's buffer.
 */
static inline uint64_t
diskfs_il_hdr_len(uint32_t nblocks)
{
    uint64_t h = sizeof(struct diskfs_redo_header) +
        (uint64_t) nblocks * sizeof(struct diskfs_redo_block_header);

    return (h + DISKFS_BLOCK_SIZE - 1) & ~((uint64_t) DISKFS_BLOCK_SIZE - 1);
} /* diskfs_il_hdr_len */

/* ------------------------------------------------------------------ */
/* Tail-pusher: write logged blocks to final locations + trim the log  */
/* ------------------------------------------------------------------ */

/*
 * Largest contiguous run available for one record (records never wrap the end
 * of the log region; a short run at the end is simply left unused until the
 * tail laps it).  The log is empty exactly when no record is pending.
 */
static uint64_t
diskfs_il_contig_free(struct diskfs_intent_log *il)
{
    uint64_t start = SM_INTENT_LOG_OFFSET;
    uint64_t end   = SM_INTENT_LOG_OFFSET + SM_INTENT_LOG_SIZE;

    if (!il->push_head) {
        return SM_INTENT_LOG_SIZE;
    }
    if (il->log_head >= il->log_tail) {
        uint64_t run_end   = end - il->log_head;
        uint64_t run_start = il->log_tail - start;

        return run_end > run_start ? run_end : run_start;
    }
    return il->log_tail - il->log_head;
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
    uint64_t end = SM_INTENT_LOG_OFFSET + SM_INTENT_LOG_SIZE;
    uint64_t offset;

    if (il->log_head + reclen > end) {
        il->log_head = SM_INTENT_LOG_OFFSET;     /* wrap; tail of region unused */
    }
    offset       = il->log_head;
    il->log_head = offset + reclen;
    return offset;
} /* diskfs_il_place */

static void diskfs_il_push_kick(
    struct diskfs_intent_log *il);

/*
 * A record's block images are now durably home.  Transition the corresponding
 * cache blocks LOGGED -> CLEAN so they become evictable -- but only if the
 * block hasn't been re-logged since (blk->seq still equals this record's seq)
 * and isn't currently pinned by an active transaction.  Checked under the
 * shard lock so it serializes against diskfs_block_claim re-dirtying the block.
 * (A3 will additionally enqueue the cleaned block on the shard LRU and wake a
 * buffer waiter.)
 */
static void
diskfs_il_clean_pushed_record(
    struct diskfs_intent_log *il,
    struct diskfs_il_record  *rec)
{
    struct diskfs_shared      *shared = container_of(il, struct diskfs_shared, intent_log);
    struct diskfs_block_cache *cache  = shared->block_cache;
    char                      *p      = (char *) rec->iovs[0].data + sizeof(struct diskfs_redo_header);
    uint32_t                   i;

    for (i = 0; i < rec->num_blocks; i++) {
        struct diskfs_redo_block_header *bh = (struct diskfs_redo_block_header *) p;
        struct diskfs_block_shard       *shard;
        struct diskfs_block             *blk;
        uint32_t                         bucket;

        p += sizeof(*bh);

        shard  = diskfs_block_shard(cache, bh->device_id, bh->device_offset);
        bucket = diskfs_block_bucket(bh->device_id, bh->device_offset);

        pthread_mutex_lock(&shard->lock);
        blk = diskfs_block_lookup_locked(shard, bucket, bh->device_id, bh->device_offset);
        if (blk && blk->state == DISKFS_BLOCK_LOGGED &&
            blk->seq == rec->seq && blk->pin_count == 0) {
            blk->state = DISKFS_BLOCK_CLEAN;
            if (!blk->on_lru) {
                diskfs_block_lru_push_tail(shard, blk);
            }
        }
        pthread_mutex_unlock(&shard->lock);
    }
} /* diskfs_il_clean_pushed_record */

static void diskfs_il_push_issue(
    struct diskfs_intent_log *il);

/* Finish the current record: mark its blocks evictable, release the record's
* iovecs (header + all block clones), advance the tail, and kick the next. */
static void
diskfs_il_push_finish(struct diskfs_intent_log *il)
{
    struct diskfs_il_record *rec = il->push_cur;

    diskfs_il_clean_pushed_record(il, rec);

    il->push_cur = NULL;
    evpl_iovecs_release(il->evpl, rec->iovs, rec->niov);
    free(rec->iovs);
    free(rec);

    il->log_tail = il->push_head ? il->push_head->offset : il->log_head;

    diskfs_il_push_kick(il);

    /* Space freed: re-poke channel processing in case a writer was blocked
     * on a full log. */
    evpl_ring_doorbell(&il->wake_doorbell);
} /* diskfs_il_push_finish */

/* One home-write of a logged block completed.  private_data is the il (all
 * outstanding writes belong to the single current record). */
static void
diskfs_il_push_block_cb(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct diskfs_intent_log *il = private_data;

    (void) evpl;
    chimera_diskfs_abort_if(status, "tail-push home write failed: %d", status);

    /* One home write drained from the queue; resume redo at the low watermark. */
    if (--il->iocbs_inflight == DISKFS_IL_IOCB_LOWAT) {
        evpl_ring_doorbell(&il->wake_doorbell);
    }

    --il->push_outstanding;

    /* Capacity freed: issue any of this record's remaining home writes that the
     * cap held back. */
    if (il->push_cur && il->push_next < il->push_cur->num_blocks) {
        diskfs_il_push_issue(il);
    }

    /* All of the current record's blocks are issued and durably home. */
    if (il->push_outstanding == 0 &&
        il->push_cur && il->push_next == il->push_cur->num_blocks) {
        diskfs_il_push_finish(il);
    }
} /* diskfs_il_push_block_cb */

/*
 * Start pushing the oldest pending record (if idle).  Writes each block's
 * post-image straight from the record's zero-copy clone of the cache buffer
 * to its final (device, offset) -- no copy.  Strict oldest-first ordering
 * means a block re-logged in a later record gets its newest image last.
 */
static void
diskfs_il_push_kick(struct diskfs_intent_log *il)
{
    struct diskfs_il_record *rec;

    if (il->push_cur || !il->push_head) {
        return;
    }

    rec           = il->push_head;
    il->push_head = rec->next;
    if (!il->push_head) {
        il->push_tail = NULL;
    }
    rec->next            = NULL;
    il->push_cur         = rec;
    il->push_next        = 0;
    il->push_outstanding = 0;

    if (rec->num_blocks == 0) {
        diskfs_il_push_finish(il);
        return;
    }

    diskfs_il_push_issue(il);
} /* diskfs_il_push_kick */

/*
 * Issue as many of the current record's remaining home writes as the block
 * queue's in-flight cap allows.  A large metadata txn can have more blocks than
 * the submission ring holds, so the rest are issued from diskfs_il_push_block_cb
 * as completions free capacity.  Shares the iocb budget with redo writes (both
 * gate on iocbs_inflight) so the IL thread never overruns the queue.
 */
static void
diskfs_il_push_issue(struct diskfs_intent_log *il)
{
    struct diskfs_il_record         *rec  = il->push_cur;
    char                            *base = (char *) rec->iovs[0].data +
        sizeof(struct diskfs_redo_header);
    struct diskfs_redo_block_header *bh;

    while (il->push_next < rec->num_blocks &&
           il->iocbs_inflight < DISKFS_IL_IOCB_CAP) {
        uint32_t idx = il->push_next;

        bh = (struct diskfs_redo_block_header *) (base + (size_t) idx * sizeof(*bh));

        il->push_next++;
        il->push_outstanding++;
        il->iocbs_inflight++;

        evpl_block_write(il->evpl, il->queue[bh->device_id], &rec->iovs[1 + idx], 1,
                         bh->device_offset, 1 /* sync */, diskfs_il_push_block_cb, il);
    }
} /* diskfs_il_push_issue */

/*
 * Runs on the intent-log thread when a redo record has been written
 * durably.  The transaction's changes are now recoverable, so drop the
 * block pins (-> LOGGED, awaiting tail-push) and the inode locks, then push
 * the completion onto the worker's CQ.
 */
static void
diskfs_redo_write_cb(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct diskfs_redo_ctx   *ctx = private_data;
    struct diskfs_iq_channel *ch  = ctx->ch;
    struct diskfs_intent_log *il  = ctx->il;
    struct diskfs_il_record  *rec = ctx->rec;
    uint32_t                  cq_tail;

    (void) evpl;
    chimera_diskfs_abort_if(status, "redo record write failed: %d", status);

    /* One block write (one chunk) drained from the queue.  Resume redo
     * submission once the queue has bled back down to the low watermark. */
    if (--il->iocbs_inflight == DISKFS_IL_IOCB_LOWAT) {
        evpl_ring_doorbell(&il->wake_doorbell);
    }

    /* One chunk of a possibly multi-chunk journal write landed; only the last
     * makes the whole record durable. */
    if (--ctx->segments > 0) {
        return;
    }

    il->redo_inflight--;

    diskfs_txn_unpin_blocks(ctx->entry.txn, DISKFS_BLOCK_LOGGED);
    /* Record now durable: the freed metadata blocks are LOGGED + unpinned, so
     * returning their ranges to the allocator can no longer collide with a
     * still-pinned block (a re-claim COWs cleanly). */
    diskfs_txn_apply_frees(ctx->entry.txn);
    diskfs_txn_unlock_all(ctx->entry.txn);

    ctx->entry.status = status;

    cq_tail                                       = __atomic_load_n(&ch->cq.tail, __ATOMIC_RELAXED);
    ch->cq.entries[cq_tail & DISKFS_IQ_RING_MASK] = ctx->entry;
    __atomic_store_n(&ch->cq.tail, cq_tail + 1, __ATOMIC_RELEASE);

    ch->cq_inflight--;

    /* The record (which owns its on-log image) is now durable; queue it for
     * the tail-pusher.  Its iovec stays in place -- never struct-copied. */
    rec->next = NULL;
    if (il->push_tail) {
        il->push_tail->next = rec;
    } else {
        il->push_head = rec;
    }
    il->push_tail = rec;

    free(ctx);

    diskfs_il_push_kick(il);
    evpl_ring_doorbell(&ch->cq_doorbell);
} /* diskfs_redo_write_cb */

/*
 * Build a full-block redo record for one transaction and issue a durable
 * write into the reserved intent-log region.  Runs on the intent-log thread.
 */
static void
diskfs_il_write_redo(
    struct diskfs_intent_log *il,
    struct diskfs_iq_channel *ch,
    struct diskfs_iq_entry   *entry)
{
    struct diskfs_txn               *txn = entry->txn;
    struct diskfs_txn_block         *tb;
    struct diskfs_redo_ctx          *ctx;
    struct diskfs_il_record         *rec;
    struct diskfs_redo_header       *hdr;
    struct diskfs_redo_block_header *bh;
    uint32_t                         nblocks = 0;
    uint64_t                         hdr_len, reclen, offset;
    uint32_t                         i;
    char                            *p;
    int                              niov;
    XXH3_state_t                     xs;

    for (tb = txn->blocks; tb; tb = tb->next) {
        nblocks++;
    }

    hdr_len = diskfs_il_hdr_len(nblocks);
    reclen  = hdr_len + (uint64_t) nblocks * DISKFS_BLOCK_SIZE;

    /* Caller guarantees space (diskfs_iq_process_channel checks diskfs_il_fits
     * before consuming the SQ entry), so placement always succeeds. */
    offset = diskfs_il_place(il, reclen);

    rec             = malloc(sizeof(*rec));
    rec->seq        = il->log_seq;
    rec->offset     = offset;
    rec->reclen     = reclen;
    rec->num_blocks = nblocks;
    rec->niov       = 1 + nblocks;
    rec->iovs       = malloc(rec->niov * sizeof(struct evpl_iovec));
    rec->next       = NULL;

    /* iovs[0]: materialized header region (redo_header + per-block headers). */
    niov = evpl_iovec_alloc(il->evpl, hdr_len, DISKFS_BLOCK_SIZE, 1,
                            EVPL_IOVEC_FLAG_SHARED, &rec->iovs[0]);
    chimera_diskfs_abort_if(niov != 1, "redo header did not fit in one iovec (%d)", niov);

    ctx        = malloc(sizeof(*ctx));
    ctx->il    = il;
    ctx->ch    = ch;
    ctx->entry = *entry;
    ctx->rec   = rec;

    p               = (char *) rec->iovs[0].data;
    hdr             = (struct diskfs_redo_header *) p;
    hdr->magic      = DISKFS_REDO_MAGIC;
    hdr->csum_lo    = 0;
    hdr->csum_hi    = 0;
    hdr->seq        = il->log_seq++;
    hdr->tail       = il->log_tail;
    hdr->num_blocks = nblocks;
    hdr->reclen     = (uint32_t) reclen;
    p              += sizeof(*hdr);

    i = 0;
    for (tb = txn->blocks; tb; tb = tb->next, i++) {
        struct diskfs_block *blk = tb->block;

        /* Stamp the block with this record's seq so the tail-pusher can tell,
        * on push completion, whether the block has been re-logged since (a
        * higher seq) -- if not, it can be marked CLEAN and made evictable. */
        blk->seq = rec->seq;

        bh                = (struct diskfs_redo_block_header *) p;
        bh->device_id     = blk->device_id;
        bh->pad           = 0;
        bh->device_offset = blk->device_offset;
        p                += sizeof(*bh);

        /* Move the commit-time snapshot ref into the record (no copy, no touch
         * of the live block->iov from this thread).  The image stays immutable
         * until pushed home and released; a later writer COWs the cache block. */
        evpl_iovec_move(&rec->iovs[1 + i], &tb->snap);
    }

    /* Zero the header-region tail padding so the checksum covers deterministic
     * bytes, then stamp the XXH3-128 over the header region + every block. */
    {
        char *end = (char *) rec->iovs[0].data + hdr_len;

        if (p < end) {
            memset(p, 0, (size_t) (end - p));
        }
    }
    XXH3_128bits_reset(&xs);
    XXH3_128bits_update(&xs, rec->iovs[0].data, hdr_len);
    for (i = 0; i < nblocks; i++) {
        XXH3_128bits_update(&xs, rec->iovs[1 + i].data, DISKFS_BLOCK_SIZE);
    }
    {
        XXH128_hash_t h = XXH3_128bits_digest(&xs);

        hdr->csum_lo = h.low64;
        hdr->csum_hi = h.high64;
    }

    ctx->segments = (rec->niov + DISKFS_IL_MAX_IOV - 1) / DISKFS_IL_MAX_IOV;

    ch->cq_inflight++;
    il->redo_inflight++;
    il->iocbs_inflight += ctx->segments;     /* one block write per chunk below */

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

            evpl_block_write(il->evpl, il->queue[SM_INTENT_LOG_DEVICE],
                             &rec->iovs[done], cnt, woff, 1 /* sync */,
                             diskfs_redo_write_cb, ctx);
            woff += bytes;
            done += cnt;
        }
    }
} /* diskfs_il_write_redo */

/* Padded on-log length of the redo record for one transaction. */
static uint64_t
diskfs_il_txn_reclen(struct diskfs_txn *txn)
{
    struct diskfs_txn_block *tb;
    uint32_t                 nblocks = 0;

    for (tb = txn->blocks; tb; tb = tb->next) {
        nblocks++;
    }
    return diskfs_il_hdr_len(nblocks) + (uint64_t) nblocks * DISKFS_BLOCK_SIZE;
} /* diskfs_il_txn_reclen */

static void
diskfs_iq_process_channel(struct diskfs_iq_channel *ch)
{
    struct diskfs_intent_log *il      = &ch->worker->shared->intent_log;
    uint32_t                  sq_head = __atomic_load_n(&ch->sq.head, __ATOMIC_RELAXED);
    uint32_t                  sq_tail = __atomic_load_n(&ch->sq.tail, __ATOMIC_ACQUIRE);
    int                       issued  = 0;

    while (sq_head != sq_tail) {
        struct diskfs_iq_entry *slot = &ch->sq.entries[sq_head & DISKFS_IQ_RING_MASK];
        struct diskfs_iq_entry  entry;
        uint32_t                cq_tail, cq_head;

        /* Don't overrun the block queue's submission ring.  A redo record is a
         * handful of writes; stop consuming the SQ once we're at the cap and
         * let a write completion ring the wake doorbell to resume us.  When the
         * queue is idle, always issue at least one record so we make progress
         * even if a single record exceeds the cap. */
        if (il->iocbs_inflight >= DISKFS_IL_IOCB_CAP) {
            break;
        }

        cq_tail = __atomic_load_n(&ch->cq.tail, __ATOMIC_RELAXED);
        cq_head = __atomic_load_n(&ch->cq.head, __ATOMIC_ACQUIRE);

        /* Reserve a CQ slot per in-flight write so completions can't
         * overflow the CQ.  Defer if no room; the worker's CQ drain pings
         * us to resume. */
        if ((cq_tail - cq_head) + ch->cq_inflight >= DISKFS_IQ_RING_SIZE) {
            break;
        }

        /* Back off if the log lacks room for this record; the tail-pusher
         * rings the wake doorbell once it frees space. */
        if (!diskfs_il_fits(il, diskfs_il_txn_reclen(slot->txn))) {
            break;
        }

        entry = *slot;
        sq_head++;
        entry.status = 0;

        /* Issue a durable redo write; the completion drops pins/locks and
         * pushes the CQE (see diskfs_redo_write_cb). */
        diskfs_il_write_redo(il, ch, &entry);
        issued++;
    }

    if (issued > 0) {
        __atomic_store_n(&ch->sq.head, sq_head, __ATOMIC_RELEASE);
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
    }
} /* diskfs_intent_log_drain_pending */

static void
diskfs_intent_log_wake_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct diskfs_intent_log *il = container_of(doorbell,
                                                struct diskfs_intent_log,
                                                wake_doorbell);
    uint32_t                  i;

    (void) evpl;

    diskfs_intent_log_drain_pending(il);

    /* Unregister pass: compact slots out (swap-with-tail). */
    i = 0;
    while (i < il->num_channels) {
        struct diskfs_iq_channel *ch = il->channels[i];

        if (__atomic_load_n(&ch->unregister_requested, __ATOMIC_ACQUIRE)) {
            uint32_t last = il->num_channels - 1;
            if (i != last) {
                il->channels[i] = il->channels[last];
            }
            il->channels[last] = NULL;
            il->num_channels   = last;
            __atomic_store_n(&ch->unregister_done, 1, __ATOMIC_RELEASE);
            continue;     /* re-process index i (now a different channel) */
        }
        i++;
    }

    for (i = 0; i < il->num_channels; i++) {
        diskfs_iq_process_channel(il->channels[i]);
    }
} /* diskfs_intent_log_wake_cb */

static void
diskfs_iq_cq_doorbell_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct diskfs_iq_channel *ch = container_of(doorbell,
                                                struct diskfs_iq_channel,
                                                cq_doorbell);
    uint32_t                  head    = __atomic_load_n(&ch->cq.head, __ATOMIC_RELAXED);
    uint32_t                  tail    = __atomic_load_n(&ch->cq.tail, __ATOMIC_ACQUIRE);
    int                       drained = 0;

    (void) evpl;

    while (head != tail) {
        struct diskfs_iq_entry entry = ch->cq.entries[head & DISKFS_IQ_RING_MASK];
        head++;
        drained++;

        /* The txn's logical inode locks were already dropped by the intent
         * log thread (diskfs_iq_process_channel); just deliver completion. */
        entry.cb(entry.txn, entry.status, entry.private_data);
        diskfs_txn_release(entry.txn);
    }

    if (drained > 0) {
        __atomic_store_n(&ch->cq.head, head, __ATOMIC_RELEASE);
        /* Wake the intent log thread in case it had deferred work on
         * this channel because the CQ was full. */
        evpl_ring_doorbell(&ch->worker->shared->intent_log.wake_doorbell);
    }
} /* diskfs_iq_cq_doorbell_cb */

static void *
diskfs_intent_log_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_intent_log *il     = private_data;
    struct diskfs_shared     *shared = container_of(il, struct diskfs_shared, intent_log);
    int                       i;

    il->evpl             = evpl;
    il->log_head         = SM_INTENT_LOG_OFFSET;
    il->log_tail         = SM_INTENT_LOG_OFFSET;
    il->log_seq          = 0;
    il->push_head        = NULL;
    il->push_tail        = NULL;
    il->push_cur         = NULL;
    il->push_next        = 0;
    il->push_outstanding = 0;
    il->redo_inflight    = 0;
    il->iocbs_inflight   = 0;

    /* Open block queues on this thread's evpl for redo writes + tail-push. */
    il->queue = calloc(shared->num_devices, sizeof(*il->queue));
    for (i = 0; i < shared->num_devices; i++) {
        il->queue[i] = evpl_block_open_queue(evpl, shared->devices[i].bdev);
    }

    evpl_add_doorbell(evpl, &il->wake_doorbell, diskfs_intent_log_wake_cb);
    __atomic_store_n(&il->ready, 1, __ATOMIC_RELEASE);
    return il;
} /* diskfs_intent_log_thread_init */

static void
diskfs_intent_log_thread_shutdown(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_intent_log *il     = private_data;
    struct diskfs_shared     *shared = container_of(il, struct diskfs_shared, intent_log);
    int                       i;

    /* By the time this runs the VFS layer has destroyed the worker threads,
     * which freed their channels, so no new SQ work can arrive.
     *
     * Clean unmount: drain everything to its final on-disk location so the
     * filesystem is fully consistent at home offsets (no replay needed).
     * Pump our evpl until all in-flight redo writes have landed in the push
     * FIFO and the tail-pusher has flushed every record home.  Drain *before*
     * dropping the wake doorbell: an in-flight redo/push write completing here
     * rings the doorbell to resume submission, so it must still be open. */
    while (il->redo_inflight || il->push_cur || il->push_head) {
        diskfs_il_push_kick(il);
        evpl_continue(evpl);
    }

    /* All writes durable and no submitter remains; now drop the doorbell. */
    evpl_remove_doorbell(evpl, &il->wake_doorbell);

    for (i = 0; i < shared->num_devices; i++) {
        evpl_block_close_queue(evpl, il->queue[i]);
    }
    free(il->queue);
} /* diskfs_intent_log_thread_shutdown */

/*
 * The post-free-flush half of commit: serialize the dirty inodes, snapshot the
 * pinned blocks, and hand the txn to the intent-log thread.  Runs once the
 * deferred FREE deltas are journaled (inline, or resumed via the load).
 */
static void
diskfs_txn_commit_finish(
    struct diskfs_txn     *txn,
    diskfs_txn_commit_cb_t cb,
    void                  *private_data)
{
    struct diskfs_thread     *thread = txn->thread;
    struct diskfs_shared     *shared;
    struct diskfs_iq_channel *ch;
    struct diskfs_iq_entry   *slot;
    uint32_t                  tail, head;

    /* Serialize every dirty inode into its block buffer now, on the worker
     * that owns the live inodes under write lock, before handing the txn
     * (and its pinned blocks) to the intent log thread. */
    diskfs_txn_flush_inodes(txn);

    /* Snapshot each block's buffer (zero-copy ref) while the content is final
     * and the inode locks are still held -- so the redo record captures this
     * txn's committed image, immune to a later COW, and the intent-log thread
     * never has to touch the live block->iov.  The refs are moved into the
     * record by diskfs_il_write_redo. */
    {
        struct diskfs_txn_block *tb;

        for (tb = txn->blocks; tb; tb = tb->next) {
            evpl_iovec_clone(&tb->snap, &tb->block->iov);
        }
    }

    /* Write txn -> intent log thread via this worker's SQ.  The intent
     * log thread drops the txn's logical inode locks when it processes
     * the entry (see diskfs_iq_process_channel); these are logical locks
     * tracked in the cache, not pthread mutexes, so holding them across
     * the SQ-full evpl_continue spin below cannot deadlock (conflicting
     * ops simply park as waiters).  The completion callback fires from the
     * CQ doorbell back on this worker thread. */
    shared = thread->shared;
    ch     = thread->iq_channel;

    tail = __atomic_load_n(&ch->sq.tail, __ATOMIC_RELAXED);
    head = __atomic_load_n(&ch->sq.head, __ATOMIC_ACQUIRE);

    /* SQ-full backpressure: drain our own evpl so the CQ doorbell fires
     * and the intent log thread gets a wake from cq_doorbell_cb. */
    while (tail - head >= DISKFS_IQ_RING_SIZE) {
        evpl_continue(thread->evpl);
        head = __atomic_load_n(&ch->sq.head, __ATOMIC_ACQUIRE);
    }

    slot               = &ch->sq.entries[tail & DISKFS_IQ_RING_MASK];
    slot->txn          = txn;
    slot->cb           = cb;
    slot->private_data = private_data;
    slot->status       = 0;

    __atomic_store_n(&ch->sq.tail, tail + 1, __ATOMIC_RELEASE);

    evpl_ring_doorbell(&shared->intent_log.wake_doorbell);
} /* diskfs_txn_commit_finish */

/* Resume a commit whose pre-commit free-journal flush parked on a log read. */
static void
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

static inline void
diskfs_txn_commit(
    struct diskfs_txn     *txn,
    diskfs_txn_commit_cb_t cb,
    void                  *private_data)
{
    struct diskfs_thread *thread = txn->thread;

    if (txn->type == DISKFS_TXN_READ) {
        /* Read txns don't need durability -- complete inline. */
        diskfs_txn_unlock_all(txn);
        cb(txn, 0, private_data);
        diskfs_txn_release(txn);
        return;
    }

    /* Journal the deferred FREE deltas before block serialization + snapshot,
     * so the FREE-delta log blocks ride this txn's redo.  The journal claim is
     * async: a cold log block parks the request and the flush returns SM_AGAIN,
     * so commit defers and diskfs_commit_resume finishes once it loads. */
    if (txn->pending_frees) {
        struct diskfs_commit_ctx *c = malloc(sizeof(*c));

        c->txn          = txn;
        c->cb           = cb;
        c->private_data = private_data;

        if (diskfs_txn_flush_free_journals(thread, txn, c) == SM_AGAIN) {
            return;     /* parked; diskfs_commit_resume continues */
        }
        free(c);
    }

    diskfs_txn_commit_finish(txn, cb, private_data);
} /* diskfs_txn_commit */

struct diskfs_recover_rec {
    uint64_t seq;
    uint64_t offset;     /* byte offset within the read-in log image */
};

static int
diskfs_recover_rec_cmp(
    const void *a,
    const void *b)
{
    uint64_t sa = ((const struct diskfs_recover_rec *) a)->seq;
    uint64_t sb = ((const struct diskfs_recover_rec *) b)->seq;

    return (sa > sb) - (sa < sb);
} /* diskfs_recover_rec_cmp */

/* ------------------------------------------------------------------ */
/* Mount-time synchronous block I/O via a transient evpl pump.         */
/*                                                                     */
/* All diskfs disk access goes through evpl_block, but mount, crash    */
/* recovery, bootstrap and unmount run with no worker event loop to    */
/* drive completions (and a raw device path is not a file, so pread/   */
/* pwrite cannot reach a VFIO/io_uring/libaio block device).  Stand up */
/* a private evpl + per-device block queues and pump evpl_continue()   */
/* until each async op finishes -- the same drain idiom the intent-log */
/* thread uses at shutdown.                                            */
/* ------------------------------------------------------------------ */
struct diskfs_mount_io {
    struct evpl              *evpl;
    struct evpl_block_queue **queue;
    struct diskfs_shared     *shared;
};

struct diskfs_mount_io_wait {
    int done;
    int status;
};

static void
diskfs_mount_io_complete(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct diskfs_mount_io_wait *w = private_data;

    (void) evpl;
    w->status = status;
    w->done   = 1;
} /* diskfs_mount_io_complete */

static struct diskfs_mount_io *
diskfs_mount_io_open(struct diskfs_shared *shared)
{
    struct diskfs_mount_io *io = calloc(1, sizeof(*io));
    int                     i;

    io->shared = shared;
    io->evpl   = evpl_create(NULL);
    io->queue  = calloc(shared->num_devices, sizeof(*io->queue));
    for (i = 0; i < shared->num_devices; i++) {
        io->queue[i] = evpl_block_open_queue(io->evpl, shared->devices[i].bdev);
    }
    return io;
} /* diskfs_mount_io_open */

static void
diskfs_mount_io_close(struct diskfs_mount_io *io)
{
    int i;

    for (i = 0; i < io->shared->num_devices; i++) {
        evpl_block_close_queue(io->evpl, io->queue[i]);
    }
    free(io->queue);
    evpl_destroy(io->evpl);
    free(io);
} /* diskfs_mount_io_close */

/*
 * sm_io read bridge.  offset must be block-aligned; the device transfer is
 * rounded up to a whole block and only the requested bytes are copied out, so
 * callers may ask for a sub-block struct.  Chunked at the device max request
 * size.
 */
static int
diskfs_mount_io_read(
    void    *user,
    uint32_t device_id,
    void    *buf,
    uint64_t length,
    uint64_t offset)
{
    struct diskfs_mount_io *io     = user;
    uint64_t                maxreq = io->shared->devices[device_id].max_request_size;
    uint64_t                done   = 0;

    while (done < length) {
        struct evpl_iovec           iov;
        struct diskfs_mount_io_wait w    = { 0, 0 };
        uint64_t                    want = length - done;
        uint64_t                    xfer;

        if (want > maxreq) {
            want = maxreq;
        }
        xfer = (want + DISKFS_BLOCK_SIZE - 1) & ~((uint64_t) DISKFS_BLOCK_SIZE - 1);

        evpl_iovec_alloc(io->evpl, xfer, DISKFS_BLOCK_SIZE, 1, 0, &iov);
        evpl_block_read(io->evpl, io->queue[device_id], &iov, 1, offset + done,
                        diskfs_mount_io_complete, &w);
        while (!w.done) {
            evpl_continue(io->evpl);
        }
        if (w.status) {
            evpl_iovec_release(io->evpl, &iov);
            return -1;
        }
        memcpy((char *) buf + done, iov.data, want);
        evpl_iovec_release(io->evpl, &iov);
        done += want;
    }
    return 0;
} /* diskfs_mount_io_read */

/* sm_io write bridge.  offset and length must be block-aligned (callers write
 * whole blocks / the block-padded superblock + condensed log slots). */
static int
diskfs_mount_io_write(
    void       *user,
    uint32_t    device_id,
    const void *buf,
    uint64_t    length,
    uint64_t    offset)
{
    struct diskfs_mount_io *io     = user;
    uint64_t                maxreq = io->shared->devices[device_id].max_request_size;
    uint64_t                done   = 0;

    while (done < length) {
        struct evpl_iovec           iov;
        struct diskfs_mount_io_wait w    = { 0, 0 };
        uint64_t                    want = length - done;

        if (want > maxreq) {
            want = maxreq;
        }
        evpl_iovec_alloc(io->evpl, want, DISKFS_BLOCK_SIZE, 1, 0, &iov);
        memcpy(iov.data, (const char *) buf + done, want);
        evpl_block_write(io->evpl, io->queue[device_id], &iov, 1, offset + done,
                         1 /* sync */, diskfs_mount_io_complete, &w);
        while (!w.done) {
            evpl_continue(io->evpl);
        }
        evpl_iovec_release(io->evpl, &iov);
        if (w.status) {
            return -1;
        }
        done += want;
    }
    return 0;
} /* diskfs_mount_io_write */

static int
diskfs_mount_io_flush(
    void    *user,
    uint32_t device_id)
{
    struct diskfs_mount_io     *io = user;
    struct diskfs_mount_io_wait w  = { 0, 0 };

    evpl_block_flush(io->evpl, io->queue[device_id], diskfs_mount_io_complete, &w);
    while (!w.done) {
        evpl_continue(io->evpl);
    }
    return w.status ? -1 : 0;
} /* diskfs_mount_io_flush */

static struct sm_io
diskfs_mount_sm_io(struct diskfs_mount_io *io)
{
    struct sm_io smio = {
        .read  = diskfs_mount_io_read,
        .write = diskfs_mount_io_write,
        .flush = diskfs_mount_io_flush,
        .user  = io,
    };

    return smio;
} /* diskfs_mount_sm_io */

/*
 * Crash recovery (synchronous replay): the previous instance did not unmount
 * cleanly, so logged-but-not-yet-pushed redo records may still sit in the
 * intent log while their home locations hold stale data.  Sweep the log for
 * intact records -- a 4 KiB-aligned magic whose XXH3-128 over reclen bytes
 * verifies (rejecting torn/partially-overwritten records) -- and write each
 * block image to its home location in seq order (latest image of a block
 * wins), then flush.  After this the on-disk b+tree / inodes / data are
 * consistent with the last acknowledged write, exactly as the tail-pusher
 * would have left them.
 *
 * Replaying every intact record (rather than just [tail, head]) is safe: in a
 * FIFO circular log a superseding record outlives every record it supersedes,
 * so seq-ordered replay always lands the latest image, and re-writing an
 * already-current block is idempotent.  Runs at mount before worker threads
 * exist, so it drives the device through the mount-time evpl pump.
 */
static int
diskfs_recover_log(
    struct diskfs_shared   *shared,
    struct diskfs_mount_io *io)
{
    char                      *log;
    uint64_t                   o;
    struct diskfs_recover_rec *recs;
    uint32_t                   nrec = 0, cap = 4096, i;

    log = malloc(SM_INTENT_LOG_SIZE);
    if (diskfs_mount_io_read(io, SM_INTENT_LOG_DEVICE, log, SM_INTENT_LOG_SIZE,
                             SM_INTENT_LOG_OFFSET) != 0) {
        free(log);
        return -1;
    }

    recs = malloc(cap * sizeof(*recs));

    for (o = 0; o + sizeof(struct diskfs_redo_header) <= SM_INTENT_LOG_SIZE;
         o += DISKFS_BLOCK_SIZE) {
        struct diskfs_redo_header *hdr = (struct diskfs_redo_header *) (log + o);
        uint64_t                   lo, hi;
        XXH128_hash_t              h;

        if (hdr->magic != DISKFS_REDO_MAGIC) {
            continue;
        }
        if (hdr->reclen < sizeof(*hdr) ||
            (hdr->reclen & (DISKFS_BLOCK_SIZE - 1)) ||
            o + hdr->reclen > SM_INTENT_LOG_SIZE) {
            continue;
        }
        lo           = hdr->csum_lo;
        hi           = hdr->csum_hi;
        hdr->csum_lo = 0;
        hdr->csum_hi = 0;
        h            = XXH3_128bits(log + o, hdr->reclen);
        hdr->csum_lo = lo;
        hdr->csum_hi = hi;
        if (h.low64 != lo || h.high64 != hi) {
            continue;
        }

        if (nrec == cap) {
            cap *= 2;
            recs = realloc(recs, cap * sizeof(*recs));
        }
        recs[nrec].seq    = hdr->seq;
        recs[nrec].offset = o;
        nrec++;
    }

    qsort(recs, nrec, sizeof(*recs), diskfs_recover_rec_cmp);

    for (i = 0; i < nrec; i++) {
        struct diskfs_redo_header *hdr  = (struct diskfs_redo_header *) (log + recs[i].offset);
        char                      *bhp  = log + recs[i].offset + sizeof(*hdr);
        char                      *data = log + recs[i].offset + diskfs_il_hdr_len(hdr->num_blocks);
        uint32_t                   b;

        /* New layout: all per-block headers are grouped after the redo header,
         * and the block images follow the 4 KiB-aligned header region. */
        for (b = 0; b < hdr->num_blocks; b++) {
            struct diskfs_redo_block_header *bh =
                (struct diskfs_redo_block_header *) (bhp + (size_t) b * sizeof(*bh));
            char                            *img = data + (size_t) b * DISKFS_BLOCK_SIZE;

            if (bh->device_id < (uint32_t) shared->num_devices) {
                int wr = diskfs_mount_io_write(io, bh->device_id, img,
                                               DISKFS_BLOCK_SIZE,
                                               bh->device_offset);

                chimera_diskfs_abort_if(wr != 0,
                                        "recovery replay write failed");
            }
        }
    }

    for (i = 0; i < (uint32_t) shared->num_devices; i++) {
        diskfs_mount_io_flush(io, i);
    }

    free(recs);
    free(log);
    chimera_diskfs_info("crash recovery: replayed %u intact intent-log records", nrec);
    return 0;
} /* diskfs_recover_log */

static void *
diskfs_init(const char *cfgdata)
{
    struct diskfs_shared       *shared = calloc(1, sizeof(*shared));
    struct diskfs_device       *device;
    enum evpl_block_protocol_id protocol_id;
    const char                 *protocol_name, *device_path;
    char                       *device0_path = NULL;
    int                         i, fd, rc;
    struct stat                 st;
    int64_t                     size;
    uint64_t                   *device_sizes;
    json_t                     *cfg, *devices_cfg, *device_cfg;
    json_error_t                json_error;


    cfg = json_loads(cfgdata, 0, &json_error);

    chimera_diskfs_abort_if(cfg == NULL, "Error parsing config: %s", json_error.text);

    devices_cfg = json_object_get(cfg, "devices");

    shared->num_devices  = json_array_size(devices_cfg);
    shared->devices      = calloc(shared->num_devices, sizeof(*shared->devices));
    shared->device_paths = calloc(shared->num_devices, sizeof(char *));

    json_array_foreach(devices_cfg, i, device_cfg)
    {
        device     = &shared->devices[i];
        device->id = i;

        protocol_name = json_string_value(json_object_get(device_cfg, "type"));
        device_path   = json_string_value(json_object_get(device_cfg, "path"));
        size          = json_integer_value(json_object_get(device_cfg, "size"));

        shared->device_paths[i] = strdup(device_path);

        if (strcmp(protocol_name, "io_uring") == 0) {
            protocol_id = EVPL_BLOCK_PROTOCOL_IO_URING;
        } else if (strcmp(protocol_name, "libaio") == 0) {
            protocol_id = EVPL_BLOCK_PROTOCOL_LIBAIO;
        } else if (strcmp(protocol_name, "vfio") == 0) {
            protocol_id = EVPL_BLOCK_PROTOCOL_VFIO;
        } else {
            chimera_diskfs_abort("Unsupported protocol: %s", protocol_name);
        }

        /* For the file-backed backends a missing path is auto-created + sized.
         * A vfio device's "path" is a PCI BDF (e.g. "01:00.0"), not a file:
         * stat'ing it ENOENTs, so skip the create -- otherwise we'd drop a
         * stray zero-length file named after the BDF in the CWD. */
        if (protocol_id != EVPL_BLOCK_PROTOCOL_VFIO) {
            rc = stat(device_path, &st);

            if (rc < 0 && errno == ENOENT) {

                fd = open(device_path, O_CREAT | O_RDWR, 0644);

                chimera_diskfs_abort_if(fd < 0, "Failed to open device %s: %s", device_path, strerror(errno));

                rc = ftruncate(fd, size);

                chimera_diskfs_abort_if(rc < 0, "Failed to truncate device %s: %s", device_path, strerror(errno));

                close(fd);
            }
        }

        device->bdev = evpl_block_open_device(protocol_id, device_path);

        device->size             = evpl_block_size(device->bdev);
        device->max_request_size = evpl_block_max_request_size(device->bdev);

        chimera_diskfs_info("Device %s size %lu max_request_size %lu",
                            device_path, device->size, device->max_request_size);

        if (i == 0) {
            device0_path = strdup(device_path);
        }
    }

    /* Opt-in persistence: when set, a clean superblock is detected at mount
     * and the prior on-disk state is reloaded instead of reformatting.  Off by
     * default so the common case (and the test suite) reformats every mount;
     * the reload + on-disk read-back paths are exercised only under this flag. */
    shared->persistent         = json_is_true(json_object_get(cfg, "persistent"));
    shared->block_cache_blocks = (uint32_t) json_integer_value(
        json_object_get(cfg, "block_cache_blocks"));
    shared->inode_cache_inodes = (uint32_t) json_integer_value(
        json_object_get(cfg, "inode_cache_inodes"));

    json_decref(cfg);


    pthread_mutex_init(&shared->lock, NULL);

    /* Decide mkfs vs clean-mount vs crash-recovery from the superblock
     * (persistent mode only):
     *   - valid + CLEAN  -> the previous instance unmounted cleanly: reload
     *                       its persisted free-space map and mount.
     *   - valid + !CLEAN -> a crash: synchronously replay the intent log to
     *                       home, then mount the now-consistent image.
     *   - no/garbage     -> mkfs.
     */
    {
        struct sm_superblock    sb;
        int                     have_sb;
        int                     mode; /* 0 = mkfs, 1 = clean mount, 2 = recover */
        struct diskfs_mount_io *mio  = diskfs_mount_io_open(shared);
        struct sm_io            smio = diskfs_mount_sm_io(mio);

        have_sb = shared->persistent &&
            space_map_read_superblock(&smio, &sb) == 0;

        if (have_sb && (sb.flags & SM_SB_CLEAN)) {
            mode         = 1;
            shared->fsid = sb.fsid;
        } else if (have_sb) {
            mode         = 2;
            shared->fsid = sb.fsid;
        } else {
            mode         = 0;
            shared->fsid = chimera_rand64();
        }

        device_sizes = calloc(shared->num_devices, sizeof(*device_sizes));
        for (i = 0; i < shared->num_devices; i++) {
            device_sizes[i] = shared->devices[i].size;
        }
        shared->space_map = space_map_create(device_sizes, shared->num_devices);
        free(device_sizes);

        if (mode == 2) {
            chimera_diskfs_info("superblock not clean: running crash recovery");
            diskfs_recover_log(shared, mio);
        }

        /* Reload the persisted free-space map.  NOTE: after a crash this
         * snapshot is the one from the last *clean* unmount, so it does not
         * reflect allocations made since then -- reads are correct (the log
         * replay made the home image consistent) but the in-memory allocator
         * must be rebuilt (namespace-walk fsck, TODO) before writes are safe.
         * For a clean mount the snapshot must load or we fall back to mkfs. */
        if (mode != 0 &&
            space_map_load(shared->space_map, &smio) != 0) {
            if (mode == 1) {
                chimera_diskfs_error("space-map reload failed; treating as fresh");
                mode = 0;
            } else {
                chimera_diskfs_error(
                    "post-recovery space-map reload failed; allocator is fresh "
                    "(writes unsafe until namespace-walk reconstruction)");
            }
        }

        shared->mounted = (mode != 0);

        if (mode != 0) {
            uint8_t              fsid_buf[CHIMERA_VFS_FSID_SIZE] = { 0 };
            uint64_t             rinum                           = sb.root_inum ? sb.root_inum : 2;
            uint32_t             rgen                            = sb.root_gen;
            uint32_t             rdev;
            uint64_t             roff;
            struct diskfs_dinode rdi;

            /* The superblock's root generation is only refreshed at clean
             * unmount, so after a crash it can be stale (0).  Read the
             * authoritative generation from the root inode's on-disk dinode
             * (consistent post-replay) so the mount handle matches. */
            roff = sm_inum_to_device_offset(shared->space_map, rinum, &rdev);
            if (diskfs_mount_io_read(mio, rdev, &rdi, sizeof(rdi), roff) == 0 &&
                rdi.inum == rinum) {
                rgen = rdi.gen;
            }

            shared->root_inum = rinum;
            shared->root_gen  = rgen;
            memcpy(fsid_buf, &shared->fsid, sizeof(shared->fsid));
            shared->root_fhlen = chimera_vfs_encode_fh_inum_mount(fsid_buf,
                                                                  rinum,
                                                                  rgen,
                                                                  shared->root_fh);
        }

        /* Clear the CLEAN flag for this session: an unclean teardown then
         * leaves it clear, so the next mount won't mistake a crash for a
         * clean shutdown. */
        rc = space_map_write_superblock(shared->space_map, &smio,
                                        shared->fsid, 0,
                                        mode != 0 ? shared->root_inum : 0,
                                        mode != 0 ? shared->root_gen : 0,
                                        mode != 0 ? sb.log_seq : 0);
        chimera_diskfs_abort_if(rc != 0, "Failed to write superblock");

        /* Persistent mkfs: write an initial condensed AG-log base so each
         * slot has a valid header before any runtime delta is journaled --
         * otherwise a crash right after format would leave the allocator log
         * unreadable. */
        if (mode == 0 && shared->persistent) {
            space_map_persist(shared->space_map, &smio);
        }

        diskfs_mount_io_close(mio);
    }
    free(device0_path);

    /* Inode cache: sharded rb-trees keyed by inum, with per-shard LRU eviction
     * of idle inodes (recycle candidates). */
    shared->inode_cache            = calloc(1, sizeof(*shared->inode_cache));
    shared->inode_cache->shard_cap = (shared->inode_cache_inodes ?
                                      shared->inode_cache_inodes :
                                      DISKFS_INODE_CACHE_DEFAULT_INODES) /
        DISKFS_INODE_CACHE_SHARDS;
    if (shared->inode_cache->shard_cap == 0) {
        shared->inode_cache->shard_cap = 1;
    }
    for (i = 0; i < DISKFS_INODE_CACHE_SHARDS; i++) {
        rb_tree_init(&shared->inode_cache->shards[i].inodes);
        pthread_mutex_init(&shared->inode_cache->shards[i].lock, NULL);
    }

    /* Block cache: sharded RCU hash of 4 KiB device blocks. */
    diskfs_block_cache_create(shared);

    /* Initialize KV shards */
    shared->num_kv_shards = 256;
    shared->kv_shards     = calloc(shared->num_kv_shards, sizeof(*shared->kv_shards));

    for (i = 0; i < shared->num_kv_shards; i++) {
        rb_tree_init(&shared->kv_shards[i].entries);
        pthread_mutex_init(&shared->kv_shards[i].lock, NULL);
    }

    /* Bring up the intent log thread.  Spin until its init has registered
     * the wake doorbell, since workers will start ringing it as soon as
     * they begin processing requests. */
    shared->intent_log.ready        = 0;
    shared->intent_log.shutdown     = 0;
    shared->intent_log.num_channels = 0;
    shared->intent_log.pending_head = NULL;
    pthread_mutex_init(&shared->intent_log.registration_lock, NULL);

    shared->intent_log.thread = evpl_thread_create(NULL,
                                                   diskfs_intent_log_thread_init,
                                                   diskfs_intent_log_thread_shutdown,
                                                   &shared->intent_log);
    while (!__atomic_load_n(&shared->intent_log.ready, __ATOMIC_ACQUIRE)) {
        /* spin briefly */
    }

    return shared;
} /* diskfs_init */

static void
diskfs_bootstrap(struct diskfs_thread *thread)
{
    struct diskfs_shared   *shared = thread->shared;
    struct timespec         now;
    struct diskfs_inode    *inode;
    uint32_t                device_id;
    uint64_t                device_offset, inum;
    int                     rc;
    struct diskfs_mount_io *mio;

    /* Guard against concurrent first-touch from multiple workers. */
    pthread_mutex_lock(&shared->lock);
    if (shared->root_fhlen != 0) {
        pthread_mutex_unlock(&shared->lock);
        return;
    }

    /* Bootstrap writes the root + orphan inode blocks to their home locations
     * synchronously (they must be re-readable from disk before becoming
     * evictable CLEAN blocks).  All device access goes through evpl_block, so
     * drive it with a transient mount-time pump rather than this worker's own
     * event loop (avoids re-entering the dispatch loop mid-request). */
    mio = diskfs_mount_io_open(shared);

    clock_gettime(CLOCK_REALTIME, &now);

    /* The root inode lives at the statically-reserved block_idx 2 of AG 0 /
     * disk 0 (inum 2).  It is reserved (not allocated through the allocator)
     * so it is excluded from every condensed free set -- no alloc delta is
     * needed and it can never be re-handed-out after a crash. */
    inum          = 2;
    device_id     = 0;
    device_offset = sm_inum_to_device_offset(shared->space_map, inum, &device_id);
    (void) rc;

    inode = diskfs_inode_struct_new(inum);

    inode->size       = 4096;
    inode->space_used = 4096;
    inode->uid        = 0;
    inode->gid        = 0;
    inode->nlink      = 2;
    inode->mode       = S_IFDIR | 0755;
    inode->atime_sec  = now.tv_sec;
    inode->atime_nsec = now.tv_nsec;
    inode->mtime_sec  = now.tv_sec;
    inode->mtime_nsec = now.tv_nsec;
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;

    /* Root directory's parent is itself for ".." lookup */
    inode->parent_inum = inode->inum;
    inode->parent_gen  = inode->gen;

    diskfs_inode_cache_insert(shared, inode);

    /* Create the root inode's block: an embedded empty b+tree root plus the
     * dinode.  Bootstrap is not a transaction, so write the block to its home
     * location synchronously -- otherwise it would be CLEAN-but-not-home and
     * eviction could discard it before the first write op logs it (CLEAN
     * blocks must be re-readable from disk).  Then detach it, evictable. */
    inode->block = diskfs_block_claim(thread, device_id, device_offset, 1);
    diskfs_bt_node_init(inode->block->iov.data, DISKFS_BT_ROOT_BASE,
                        DISKFS_BT_ROOT_CAP, 0);
    diskfs_inode_flush(inode);
    rc = diskfs_mount_io_write(mio, device_id, inode->block->iov.data,
                               DISKFS_BLOCK_SIZE, device_offset);
    chimera_diskfs_abort_if(rc != 0, "bootstrap root write failed");
    diskfs_mount_io_flush(mio, device_id);
    inode->block->state = DISKFS_BLOCK_CLEAN;
    diskfs_block_unpin(thread, inode->block, DISKFS_BLOCK_CLEAN);
    inode->block = NULL;

    /* Statically-reserved orphan-list inode (inum 3): an empty directory whose
     * b+tree keys are the inums of deleted-but-not-fully-reclaimed inodes.
     * Created at format alongside root (persists; loaded from disk on remount);
     * the incremental drainer scans it on mount and empties it. */
    {
        uint32_t             odev;
        uint64_t             ooff = sm_inum_to_device_offset(shared->space_map,
                                                             DISKFS_ORPHAN_INUM, &odev);
        struct diskfs_inode *oin = diskfs_inode_struct_new(DISKFS_ORPHAN_INUM);

        oin->size        = 4096;
        oin->space_used  = 4096;
        oin->nlink       = 1;
        oin->mode        = S_IFDIR | 0700;
        oin->atime_sec   = now.tv_sec;
        oin->atime_nsec  = now.tv_nsec;
        oin->mtime_sec   = now.tv_sec;
        oin->mtime_nsec  = now.tv_nsec;
        oin->ctime_sec   = now.tv_sec;
        oin->ctime_nsec  = now.tv_nsec;
        oin->parent_inum = DISKFS_ORPHAN_INUM;
        oin->parent_gen  = oin->gen;

        diskfs_inode_cache_insert(shared, oin);

        oin->block = diskfs_block_claim(thread, odev, ooff, 1);
        diskfs_bt_node_init(oin->block->iov.data, DISKFS_BT_ROOT_BASE,
                            DISKFS_BT_ROOT_CAP, 0);
        diskfs_inode_flush(oin);
        rc = diskfs_mount_io_write(mio, odev, oin->block->iov.data,
                                   DISKFS_BLOCK_SIZE, ooff);
        chimera_diskfs_abort_if(rc != 0, "bootstrap orphan write failed");
        diskfs_mount_io_flush(mio, odev);
        oin->block->state = DISKFS_BLOCK_CLEAN;
        diskfs_block_unpin(thread, oin->block, DISKFS_BLOCK_CLEAN);
        oin->block = NULL;
    }

    /* Create 16-byte fsid buffer for root FH encoding (8-byte fsid + 8 bytes padding) */
    {
        uint8_t fsid_buf[CHIMERA_VFS_FSID_SIZE] = { 0 };
        memcpy(fsid_buf, &shared->fsid, sizeof(shared->fsid));
        shared->root_fhlen = chimera_vfs_encode_fh_inum_mount(fsid_buf,
                                                              inode->inum,
                                                              inode->gen,
                                                              shared->root_fh);
    }
    shared->root_inum = inode->inum;
    shared->root_gen  = inode->gen;

    diskfs_mount_io_close(mio);

    pthread_mutex_unlock(&shared->lock);
} /* diskfs_bootstrap */

static void
diskfs_inode_cache_release(
    struct rb_node *node,
    void           *private_data)
{
    struct diskfs_inode *inode = container_of(node, struct diskfs_inode, node);

    (void) private_data;

    /* All inode contents live in b+tree blocks freed via the block cache;
     * we only own and must free the inode struct itself (heap-allocated). */
    free(inode);
} /* diskfs_inode_cache_release */

static void
diskfs_destroy(void *private_data)
{
    struct diskfs_shared *shared = private_data;
    int                   i;

    for (i = 0; i < DISKFS_INODE_CACHE_SHARDS; i++) {
        rb_tree_destroy(&shared->inode_cache->shards[i].inodes,
                        diskfs_inode_cache_release, NULL);
        pthread_mutex_destroy(&shared->inode_cache->shards[i].lock);
    }

    /* Shut down the intent log thread before tearing down anything it
     * might still touch.  Worker threads have already unregistered their
     * channels via the unregister handshake at this point. */
    __atomic_store_n(&shared->intent_log.shutdown, 1, __ATOMIC_RELEASE);
    evpl_thread_destroy(shared->intent_log.thread);
    pthread_mutex_destroy(&shared->intent_log.registration_lock);

    /* Clean unmount: the intent-log thread already drained every logged block
     * to its home location, so persist the free-space map and stamp the
     * superblock CLEAN (with the root + next log seq) so the next mount
     * reloads instead of re-handing-out in-use space.  Driven through the
     * mount-time evpl pump while the devices are still open -- the IL thread
     * (the only other device user) is already gone.  Only mark clean if a root
     * actually exists (an untouched mkfs has nothing to preserve). */
    if (shared->persistent) {
        struct diskfs_mount_io *mio  = diskfs_mount_io_open(shared);
        struct sm_io            smio = diskfs_mount_sm_io(mio);

        if (space_map_persist(shared->space_map, &smio) != 0) {
            chimera_diskfs_error("space-map persist at unmount failed");
        } else if (shared->root_fhlen != 0) {
            int rc = space_map_write_superblock(shared->space_map, &smio,
                                                shared->fsid, SM_SB_CLEAN,
                                                shared->root_inum, shared->root_gen,
                                                shared->intent_log.log_seq);
            if (rc != 0) {
                chimera_diskfs_error("clean-superblock write at unmount failed");
            }
        }
        diskfs_mount_io_close(mio);
    }

    for (int i = 0; i < shared->num_devices; i++) {
        evpl_block_close_device(shared->devices[i].bdev);
    }

    diskfs_block_cache_destroy(shared);

    space_map_destroy(shared->space_map);

    for (int i = 0; i < shared->num_devices; i++) {
        free(shared->device_paths[i]);
    }
    free(shared->device_paths);

    pthread_mutex_destroy(&shared->lock);
    free(shared->devices);
    free(shared->inode_cache);

    /* Clean up KV shards */
    for (i = 0; i < shared->num_kv_shards; i++) {
        rb_tree_destroy(&shared->kv_shards[i].entries, diskfs_kv_entry_release, NULL);
        pthread_mutex_destroy(&shared->kv_shards[i].lock);
    }
    free(shared->kv_shards);

    free(shared);
} /* diskfs_destroy */ /* diskfs_destroy */

/*
 * Runs on a worker thread when another thread has granted it one or more
 * inode locks it was waiting on.  The lock state was already updated by
 * the releasing thread; here we record the slot (on this, the txn's own
 * thread) and resume the parked continuation.
 */
static void
diskfs_grant_doorbell_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct diskfs_thread       *thread = container_of(doorbell,
                                                      struct diskfs_thread,
                                                      grant_doorbell);
    struct diskfs_inode_waiter *list, *w;

    (void) evpl;

    pthread_mutex_lock(&thread->grant_lock);
    list               = thread->grant_head;
    thread->grant_head = NULL;
    thread->grant_tail = NULL;
    pthread_mutex_unlock(&thread->grant_lock);

    while (list) {
        diskfs_inode_cb_t    cb;
        void                *private_data;
        struct diskfs_inode *inode;
        int                  status;

        w    = list;
        list = w->next;

        cb           = w->cb;
        private_data = w->private_data;
        inode        = w->inode;
        status       = w->status;

        if (status == CHIMERA_VFS_OK) {
            struct diskfs_txn          *wtxn  = w->txn;
            enum diskfs_inode_lock_mode wmode = w->mode;

            diskfs_txn_add_slot(wtxn, inode, wmode);
            diskfs_waiter_free(thread, w);

            if (wmode == DISKFS_INODE_LOCK_WRITE) {
                /* Pin the home block (async-load if evicted) before reporting
                 * the grant; cb may fire later, back on this worker. */
                diskfs_inode_finish_write_pin(thread, wtxn, inode, cb, private_data);
            } else {
                cb(inode, CHIMERA_VFS_OK, private_data);
            }
        } else {
            diskfs_waiter_free(thread, w);
            cb(NULL, status, private_data);
        }
    }
} /* diskfs_grant_doorbell_cb */

static void *
diskfs_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_shared *shared = private_data;
    struct diskfs_thread *thread = calloc(1, sizeof(*thread));


    thread->allocator = slab_allocator_create(4096, 1024 * 1024 * 1024);

    evpl_iovec_alloc(evpl, 4096, 4096, 1, 0, &thread->zero);
    memset(thread->zero.data, 0, 4096);  // Zero buffer must contain zeros!
    evpl_iovec_alloc(evpl, 4096, 4096, 1, 0, &thread->pad);

    thread->queue = calloc(shared->num_devices, sizeof(*thread->queue));

    for (int i = 0; i < shared->num_devices; i++) {
        thread->queue[i] = evpl_block_open_queue(evpl, shared->devices[i].bdev);
    }

    thread->shared = shared;
    thread->evpl   = evpl;

    /* Allocate this worker's intent-log channel and register the CQ
     * doorbell on the worker's own evpl. */
    thread->iq_channel         = calloc(1, sizeof(*thread->iq_channel));
    thread->iq_channel->worker = thread;
    evpl_add_doorbell(evpl, &thread->iq_channel->cq_doorbell,
                      diskfs_iq_cq_doorbell_cb);

    /* Inode lock-grant delivery queue + doorbell. */
    pthread_mutex_init(&thread->grant_lock, NULL);
    thread->grant_head = NULL;
    thread->grant_tail = NULL;
    evpl_add_doorbell(evpl, &thread->grant_doorbell, diskfs_grant_doorbell_cb);

    /* B+tree op resume queue: doorbell (cross-thread) + deferral (same-thread). */
    pthread_mutex_init(&thread->resume_lock, NULL);
    thread->resume_head            = NULL;
    thread->resume_tail            = NULL;
    thread->bt_op_free_list        = NULL;
    thread->block_waiter_free_list = NULL;
    evpl_add_doorbell(evpl, &thread->resume_doorbell, diskfs_bt_resume_doorbell_cb);
    evpl_deferral_init(&thread->resume_deferral, diskfs_bt_resume_deferral_cb, thread);

    pthread_mutex_lock(&shared->lock);
    thread->thread_id = shared->num_active_threads++;
    pthread_mutex_unlock(&shared->lock);

    /* Hand the channel to the intent log thread via the pending list. */
    pthread_mutex_lock(&shared->intent_log.registration_lock);
    thread->iq_channel->next_pending = shared->intent_log.pending_head;
    shared->intent_log.pending_head  = thread->iq_channel;
    pthread_mutex_unlock(&shared->intent_log.registration_lock);

    evpl_ring_doorbell(&shared->intent_log.wake_doorbell);

    return thread;
} /* diskfs_thread_init */

static void
diskfs_thread_destroy(void *private_data)
{
    struct diskfs_thread *thread = private_data;
    struct diskfs_shared *shared = thread->shared;

    /* Quiesce background inode drains first: their transactions reference this
     * thread (and, unlike VFS ops, nothing else waits for them), so they must
     * finish before we tear the thread down.  Pump our event loop until the
     * queue empties; the intent-log thread is still alive to complete the
     * drain transactions we issue. */
    while (thread->draining || thread->drain_head) {
        evpl_continue(thread->evpl);
    }

    /* Drain pending block I/O before closing queues */
    if (thread->pending_io > 0) {
        chimera_diskfs_debug("diskfs_thread_destroy: draining %d pending I/O operations",
                             thread->pending_io);
        while (thread->pending_io > 0) {
            evpl_continue(thread->evpl);
        }
        chimera_diskfs_debug("diskfs_thread_destroy: drain complete");
    }

    evpl_iovec_release(thread->evpl, &thread->zero);
    evpl_iovec_release(thread->evpl, &thread->pad);

    slab_allocator_destroy(thread->allocator);

    for (int i = 0; i < shared->num_devices; i++) {
        evpl_block_close_queue(thread->evpl, thread->queue[i]);
    }

    /* No txn at thread teardown; the unused reservation tail returns to the
     * in-memory free set and is captured by the condense at clean unmount. */
    space_map_thread_cache_return(shared->space_map, NULL, &thread->space_cache);

    /* Unregister the intent-log channel.  Caller must have quiesced all
     * in-flight VFS ops on this thread first. */
    if (thread->iq_channel) {
        struct diskfs_iq_channel *ch = thread->iq_channel;

        __atomic_store_n(&ch->unregister_requested, 1, __ATOMIC_RELEASE);
        evpl_ring_doorbell(&shared->intent_log.wake_doorbell);

        while (!__atomic_load_n(&ch->unregister_done, __ATOMIC_ACQUIRE)) {
            /* spin */
        }

        evpl_remove_doorbell(thread->evpl, &ch->cq_doorbell);
        free(ch);
        thread->iq_channel = NULL;
    }

    evpl_remove_doorbell(thread->evpl, &thread->grant_doorbell);
    pthread_mutex_destroy(&thread->grant_lock);

    evpl_remove_doorbell(thread->evpl, &thread->resume_doorbell);
    pthread_mutex_destroy(&thread->resume_lock);

    while (thread->bt_op_free_list) {
        struct diskfs_bt_op *op = thread->bt_op_free_list;
        thread->bt_op_free_list = op->next;
        free(op);
    }

    while (thread->block_waiter_free_list) {
        struct diskfs_block_waiter *w = thread->block_waiter_free_list;
        thread->block_waiter_free_list = w->next;
        free(w);
    }

    while (thread->txn_free_list) {
        struct diskfs_txn *txn = thread->txn_free_list;
        thread->txn_free_list = txn->next;
        free(txn);
    }

    while (thread->waiter_free_list) {
        struct diskfs_inode_waiter *w = thread->waiter_free_list;
        thread->waiter_free_list = w->next;
        free(w);
    }

    free(thread->queue);
    free(thread);
} /* diskfs_thread_destroy */

static inline void
diskfs_map_attrs(
    struct diskfs_thread     *thread,
    struct chimera_vfs_attrs *attr,
    struct diskfs_inode      *inode)
{
    struct diskfs_shared *shared = thread->shared;

    /* We always get attributes atomically with operations */
    attr->va_set_mask = CHIMERA_VFS_ATTR_ATOMIC;

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_FH) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_FH;
        attr->va_fh_len    = diskfs_inum_to_fh(shared, attr->va_fh, inode->inum, inode->gen);
    }

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_MASK_STAT) {
        attr->va_set_mask     |= CHIMERA_VFS_ATTR_MASK_STAT;
        attr->va_mode          = inode->mode;
        attr->va_nlink         = inode->nlink;
        attr->va_uid           = inode->uid;
        attr->va_gid           = inode->gid;
        attr->va_size          = inode->size;
        attr->va_space_used    = inode->space_used;
        attr->va_atime.tv_sec  = inode->atime_sec;
        attr->va_atime.tv_nsec = inode->atime_nsec;
        attr->va_mtime.tv_sec  = inode->mtime_sec;
        attr->va_mtime.tv_nsec = inode->mtime_nsec;
        attr->va_ctime.tv_sec  = inode->ctime_sec;
        attr->va_ctime.tv_nsec = inode->ctime_nsec;
        attr->va_ino           = inode->inum;
        attr->va_dev           = (42UL << 32) | 42;
        attr->va_rdev          = inode->rdev;
    }

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_FSID) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_FSID;
        attr->va_fsid      = shared->fsid;
    }

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_MASK_STATFS) {
        attr->va_set_mask      |= CHIMERA_VFS_ATTR_MASK_STATFS;
        attr->va_fs_space_total = space_map_total_capacity(shared->space_map);
        attr->va_fs_space_used  = space_map_used_bytes(shared->space_map);
        attr->va_fs_space_free  = attr->va_fs_space_total - attr->va_fs_space_used;
        attr->va_fs_space_avail = attr->va_fs_space_free;
        attr->va_fs_files_total = CHIMERA_VFS_SYNTHETIC_FS_INODES;
        attr->va_fs_files_avail = CHIMERA_VFS_SYNTHETIC_FS_INODES;
        attr->va_fs_files_free  = CHIMERA_VFS_SYNTHETIC_FS_INODES;
        attr->va_fsid           = shared->fsid;
    }

} /* diskfs_map_attrs */

static inline void
diskfs_apply_attrs(
    struct diskfs_inode      *inode,
    struct chimera_vfs_attrs *attr)
{
    struct timespec now;
    uint64_t        set_mask = attr->va_set_mask;

    clock_gettime(CLOCK_REALTIME, &now);
    attr->va_set_mask = CHIMERA_VFS_ATTR_ATOMIC;

    if (set_mask & CHIMERA_VFS_ATTR_MODE) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
        inode->mode        = (inode->mode & S_IFMT) | (attr->va_mode & ~S_IFMT);
    }

    if (set_mask & CHIMERA_VFS_ATTR_UID) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_UID;
        inode->uid         = attr->va_uid;
    }

    if (set_mask & CHIMERA_VFS_ATTR_GID) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_GID;
        inode->gid         = attr->va_gid;
    }

    if (set_mask & CHIMERA_VFS_ATTR_SIZE) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_SIZE;
        inode->size        = attr->va_size;
    }

    if (set_mask & CHIMERA_VFS_ATTR_ATIME) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_ATIME;
        if (attr->va_atime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
            inode->atime_sec  = now.tv_sec;
            inode->atime_nsec = now.tv_nsec;
        } else {
            inode->atime_sec  = attr->va_atime.tv_sec;
            inode->atime_nsec = attr->va_atime.tv_nsec;
        }
    }

    if (set_mask & CHIMERA_VFS_ATTR_MTIME) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_MTIME;
        if (attr->va_mtime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
            inode->mtime_sec  = now.tv_sec;
            inode->mtime_nsec = now.tv_nsec;
        } else {
            inode->mtime_sec  = attr->va_mtime.tv_sec;
            inode->mtime_nsec = attr->va_mtime.tv_nsec;
        }
    }

    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;

} /* diskfs_apply_attrs */

static void
diskfs_getattr_inode_cb(
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

    diskfs_map_attrs(p->thread, &request->getattr.r_attr, inode);

    diskfs_op_ok(request, p->txn);
} /* diskfs_getattr_inode_cb */

static void
diskfs_getattr(
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
                              diskfs_getattr_inode_cb, request);
} /* diskfs_getattr */

/*
 * Truncation extent walk (async): remove/trim every extent past new EOF.
 * inode_stash[0] = inode, loop_off = new_size, ext_iter = current extent.
 */
static void diskfs_setattr_trunc_process(
    struct chimera_vfs_request *request);

static void
diskfs_setattr_trunc_done(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_inode           *inode  = p->inode_stash[0];

    diskfs_apply_attrs(inode, request->setattr.set_attr);
    diskfs_map_attrs(thread, &request->setattr.r_post_attr, inode);
    diskfs_op_ok(request, p->txn);
} /* diskfs_setattr_trunc_done */

static void
diskfs_setattr_trunc_walk_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    p->loop_have = diskfs_ext_from_op(op, result, &p->ext_iter);
    diskfs_bt_op_free(p->thread, op);
    diskfs_setattr_trunc_process(request);
} /* diskfs_setattr_trunc_walk_cb */

static void
diskfs_setattr_trunc_advance(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_op           *op     = diskfs_bt_op_alloc(thread);

    if (diskfs_ext_next_async(op, thread, p->inode_stash[0], p->ext_iter.file_offset,
                              p->rec_scratch, sizeof(p->rec_scratch),
                              diskfs_setattr_trunc_walk_cb, request)) {
        diskfs_setattr_trunc_walk_cb(op, op->result, request);
    }
} /* diskfs_setattr_trunc_advance */

static void
diskfs_setattr_trunc_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request *request = private_data;

    (void) result;
    diskfs_bt_op_free(((struct diskfs_request_private *) request->plugin_data)->thread, op);
    diskfs_setattr_trunc_advance(request);
} /* diskfs_setattr_trunc_inserted_cb */

/* A trimmed or removed extent's slot is gone; re-insert the trimmed head
 * (trim case) then advance, or just advance (full-remove case). */
static void
diskfs_setattr_trunc_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request  = private_data;
    struct diskfs_request_private *p        = request->plugin_data;
    struct diskfs_thread          *thread   = p->thread;
    uint64_t                       new_size = p->loop_off;

    (void) result;
    diskfs_bt_op_free(thread, op);

    if (p->ext_iter.file_offset + p->ext_iter.length > new_size &&
        p->ext_iter.file_offset < new_size) {
        /* Trim case: reinsert the surviving head [start, new_size). */
        uint64_t new_logical = new_size - p->ext_iter.file_offset;

        op = diskfs_bt_op_alloc(thread);
        {
            struct diskfs_extent_rec rec = {
                .length        = new_logical,
                .device_id     = p->ext_iter.device_id,
                .pad           = 0,
                .device_offset = p->ext_iter.device_offset,
            };
            struct diskfs_bt_key     key = diskfs_extent_key(p->ext_iter.file_offset);

            if (diskfs_bt_insert_async(op, thread, p->txn, p->inode_stash[0], &key,
                                       &rec, sizeof(rec),
                                       diskfs_setattr_trunc_inserted_cb, request)) {
                diskfs_setattr_trunc_inserted_cb(op, op->result, request);
            }
        }
        return;
    }

    diskfs_setattr_trunc_advance(request);
} /* diskfs_setattr_trunc_removed_cb */

static void
diskfs_setattr_trunc_process(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p        = request->plugin_data;
    struct diskfs_thread          *thread   = p->thread;
    uint64_t                       new_size = p->loop_off;
    uint64_t                       extent_start, extent_end;
    struct diskfs_bt_op           *op;

    if (!p->loop_have) {
        diskfs_setattr_trunc_done(request);
        return;
    }

    extent_start = p->ext_iter.file_offset;
    extent_end   = extent_start + p->ext_iter.length;

    if (extent_start >= new_size) {
        diskfs_thread_free_space(thread, p->txn, p->ext_iter.device_id,
                                 p->ext_iter.device_offset,
                                 SM_ALIGN_UP(p->ext_iter.length));
    } else if (extent_end > new_size) {
        uint64_t old_aligned = SM_ALIGN_UP(p->ext_iter.length);
        uint64_t new_logical = new_size - extent_start;
        uint64_t new_aligned = SM_ALIGN_UP(new_logical);

        if (old_aligned > new_aligned) {
            diskfs_thread_free_space(thread, p->txn, p->ext_iter.device_id,
                                     p->ext_iter.device_offset + new_aligned,
                                     old_aligned - new_aligned);
        }
    } else {
        /* Extent entirely within new size: nothing to do, just advance. */
        diskfs_setattr_trunc_advance(request);
        return;
    }

    /* Both the full-remove and trim cases start by removing the slot. */
    op = diskfs_bt_op_alloc(thread);
    {
        struct diskfs_bt_key key = diskfs_extent_key(extent_start);

        if (diskfs_bt_remove_async(op, thread, p->txn, p->inode_stash[0], &key,
                                   diskfs_setattr_trunc_removed_cb, request)) {
            diskfs_setattr_trunc_removed_cb(op, op->result, request);
        }
    }
} /* diskfs_setattr_trunc_process */

/* First-extent selection for truncation: floor(new_size), else first extent. */
static void
diskfs_setattr_trunc_first_cb(
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
                                  sizeof(p->rec_scratch), diskfs_setattr_trunc_walk_cb,
                                  request)) {
            diskfs_setattr_trunc_walk_cb(op, op->result, request);
        }
        return;
    }

    p->loop_have = 1;
    diskfs_setattr_trunc_process(request);
} /* diskfs_setattr_trunc_first_cb */

static void
diskfs_setattr_inode_cb(
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

    diskfs_map_attrs(thread, &request->setattr.r_pre_attr, inode);

    /* Handle truncation: remove/trim extents past new EOF. */
    if ((request->setattr.set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) &&
        S_ISREG(inode->mode) &&
        request->setattr.set_attr->va_size < inode->size) {

        p->inode_stash[0] = inode;
        p->loop_off       = request->setattr.set_attr->va_size;

        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_floor_async(op, thread, inode, p->loop_off, p->rec_scratch,
                                   sizeof(p->rec_scratch), diskfs_setattr_trunc_first_cb,
                                   request)) {
            diskfs_setattr_trunc_first_cb(op, op->result, request);
        }
        return;
    }

    diskfs_apply_attrs(inode, request->setattr.set_attr);
    diskfs_map_attrs(thread, &request->setattr.r_post_attr, inode);
    diskfs_op_ok(request, p->txn);
} /* diskfs_setattr_inode_cb */

static void
diskfs_setattr(
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
                              diskfs_setattr_inode_cb, request);
} /* diskfs_setattr */

/*
 * Async mount-path resolution.  Walks request->mount.path one component at a
 * time using async inode acquire + async dirent lookup, so it resolves a path
 * through non-resident directories on a remounted FS (and over VFIO) without a
 * synchronous read.  Walk state lives in the request: inode_stash[0] is the
 * directory currently being descended; op_scratch is the parse cursor (byte
 * offset into the path).  Read-locked inodes accumulate in the txn and are
 * released centrally at commit (parents are released eagerly as we descend so
 * a deep walk reuses the slots).
 */
static void diskfs_mount_walk_acquired_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

static void
diskfs_mount_walk_dirent_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_dirent_rec      *rec     = (struct diskfs_dirent_rec *) p->rec_scratch;
    uint64_t                       child_inum;
    uint32_t                       child_gen;

    diskfs_bt_op_free(p->thread, op);

    if (result < 0) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }
    child_inum = rec->inum;
    child_gen  = rec->gen;

    /* Done descending the parent; release its read lock so a deep walk reuses
     * the slot, then fetch the child. */
    diskfs_txn_unlock_inode(p->txn, p->inode_stash[0]);

    diskfs_inode_get_inum_async(p->thread, p->txn, child_inum, child_gen,
                                diskfs_mount_walk_acquired_cb, request);
} /* diskfs_mount_walk_dirent_cb */

static void
diskfs_mount_walk_acquired_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    const char                    *path    = request->mount.path;
    int                            pathlen = request->mount.pathlen;
    const char                    *pathc, *name, *slash;
    int                            namelen;
    uint64_t                       hash;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    pathc = path + p->op_scratch;
    while (pathc < path + pathlen && *pathc == '/') {
        pathc++;
    }

    if (pathc >= path + pathlen) {
        /* Fully resolved. */
        diskfs_map_attrs(thread, &request->mount.r_attr, inode);
        diskfs_op_ok(request, p->txn);
        return;
    }

    if (unlikely(!S_ISDIR(inode->mode))) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    slash         = memchr(pathc, '/', (size_t) (path + pathlen - pathc));
    name          = pathc;
    namelen       = slash ? (int) (slash - pathc) : (int) (path + pathlen - pathc);
    p->op_scratch = (uint32_t) ((pathc + namelen) - path);

    hash              = chimera_vfs_hash(name, namelen);
    p->inode_stash[0] = inode;     /* parent for the dirent lookup */

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_lookup_async(op, thread, inode, hash, p->rec_scratch,
                                sizeof(p->rec_scratch),
                                diskfs_mount_walk_dirent_cb, request)) {
        diskfs_mount_walk_dirent_cb(op, op->result, request);
    }
} /* diskfs_mount_walk_acquired_cb */

static void
diskfs_mount(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;
    uint64_t                       inum;
    uint32_t                       gen;

    (void) private_data;

    p->thread     = thread;
    p->txn        = diskfs_txn_begin(thread, DISKFS_TXN_READ);
    p->op_scratch = 0;

    /* Resolve the mount path asynchronously starting from the root inode. */
    diskfs_fh_to_inum(&inum, &gen, shared->root_fh, shared->root_fhlen);
    diskfs_inode_get_inum_async(thread, p->txn, inum, gen,
                                diskfs_mount_walk_acquired_cb, request);
} /* diskfs_mount */

static void
diskfs_umount(
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
    diskfs_op_ok(request, p->txn);
} /* diskfs_umount */

/* inode_stash[0] = parent dir (locked across child fetch) */

static void
diskfs_lookup_at_child_cb(
    struct diskfs_inode *child,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    diskfs_map_attrs(p->thread, &request->lookup_at.r_attr, child);

    diskfs_op_ok(request, p->txn);
} /* diskfs_lookup_at_child_cb */

/* b+tree lookup completion: parse the dirent and fetch the child inode. */
static void
diskfs_lookup_at_dirent_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_dirent_rec      *rec     = (struct diskfs_dirent_rec *) p->rec_scratch;

    diskfs_bt_op_free(p->thread, op);

    if (result < 0) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    diskfs_inode_get_inum_async(p->thread, p->txn, rec->inum, rec->gen,
                                diskfs_lookup_at_child_cb, request);
} /* diskfs_lookup_at_dirent_cb */

static void
diskfs_lookup_at_parent_cb(
    struct diskfs_inode *parent,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    const char                    *name    = request->lookup_at.component;
    uint32_t                       namelen = request->lookup_at.component_len;
    uint64_t                       hash    = request->lookup_at.component_hash;
    struct diskfs_bt_key           key;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (unlikely(!S_ISDIR(parent->mode))) {
        enum chimera_vfs_error err = S_ISLNK(parent->mode) ?
            CHIMERA_VFS_ESYMLINK : CHIMERA_VFS_ENOTDIR;
        diskfs_op_fail(request, p->txn, err);
        return;
    }

    diskfs_map_attrs(thread, &request->lookup_at.r_dir_attr, parent);

    if (namelen == 1 && name[0] == '.') {
        diskfs_map_attrs(thread, &request->lookup_at.r_attr, parent);
        diskfs_op_ok(request, p->txn);
        return;
    }

    p->inode_stash[0] = parent;

    if (namelen == 2 && name[0] == '.' && name[1] == '.') {
        diskfs_inode_get_inum_async(thread, p->txn,
                                    parent->parent_inum,
                                    parent->parent_gen,
                                    diskfs_lookup_at_child_cb, request);
        return;
    }

    key = diskfs_dirent_key(hash);
    op  = diskfs_bt_op_alloc(thread);
    if (diskfs_bt_lookup_async(op, thread, parent, DISKFS_BT_OP_LOOKUP_EXACT,
                               &key, NULL, p->rec_scratch, sizeof(p->rec_scratch),
                               diskfs_lookup_at_dirent_cb, request)) {
        diskfs_lookup_at_dirent_cb(op, op->result, request);
    }
} /* diskfs_lookup_at_parent_cb */

static void
diskfs_lookup_at(
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
                              diskfs_lookup_at_parent_cb, request);
} /* diskfs_lookup_at */

/* inode_stash[0] = parent (locked across alloc / existing-inode fetch) */

static void
diskfs_mkdir_at_existing_cb(
    struct diskfs_inode *existing_inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *parent  = p->inode_stash[0];

    if (likely(status == CHIMERA_VFS_OK)) {
        diskfs_map_attrs(p->thread, &request->mkdir_at.r_attr, existing_inode);
    }
    diskfs_map_attrs(p->thread, &request->mkdir_at.r_dir_post_attr, parent);

    diskfs_op_fail(request, p->txn, CHIMERA_VFS_EEXIST);
} /* diskfs_mkdir_at_existing_cb */

static void
diskfs_mkdir_at_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_op_ok(request, p->txn);
} /* diskfs_mkdir_at_inserted_cb */

static void
diskfs_mkdir_at_alloc_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *parent  = p->inode_stash[0];
    struct diskfs_bt_op           *op;
    struct timespec                now;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    clock_gettime(CLOCK_REALTIME, &now);

    inode->size       = 4096;
    inode->space_used = 4096;
    inode->uid        = request->cred->uid;
    inode->gid        = request->cred->gid;
    inode->nlink      = 2;
    inode->mode       = S_IFDIR | 0755;
    inode->atime_sec  = now.tv_sec;
    inode->atime_nsec = now.tv_nsec;
    inode->mtime_sec  = now.tv_sec;
    inode->mtime_nsec = now.tv_nsec;
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;

    inode->parent_inum = parent->inum;
    inode->parent_gen  = parent->gen;

    diskfs_apply_attrs(inode, request->mkdir_at.set_attr);
    diskfs_map_attrs(thread, &request->mkdir_at.r_attr, inode);

    parent->nlink++;
    parent->mtime_sec  = now.tv_sec;
    parent->mtime_nsec = now.tv_nsec;
    parent->ctime_sec  = now.tv_sec;
    parent->ctime_nsec = now.tv_nsec;

    diskfs_map_attrs(thread, &request->mkdir_at.r_dir_post_attr, parent);

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_insert_async(op, thread, p->txn, parent,
                                request->mkdir_at.name_hash, request->mkdir_at.name,
                                request->mkdir_at.name_len, inode->inum, inode->gen,
                                diskfs_mkdir_at_inserted_cb, request)) {
        diskfs_mkdir_at_inserted_cb(op, op->result, request);
    }
} /* diskfs_mkdir_at_alloc_cb */

static void
diskfs_mkdir_at_check_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;

    diskfs_bt_op_free(thread, op);

    if (result >= 0) {
        struct diskfs_dirent_rec *rec = (struct diskfs_dirent_rec *) p->rec_scratch;

        diskfs_inode_get_inum_async(thread, p->txn, rec->inum, rec->gen,
                                    diskfs_mkdir_at_existing_cb, request);
        return;
    }

    diskfs_inode_alloc_async(thread, p->txn, diskfs_mkdir_at_alloc_cb, request);
} /* diskfs_mkdir_at_check_cb */

static void
diskfs_mkdir_at_parent_cb(
    struct diskfs_inode *parent,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    uint64_t                       hash    = request->mkdir_at.name_hash;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (!S_ISDIR(parent->mode)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    diskfs_map_attrs(thread, &request->mkdir_at.r_dir_pre_attr, parent);

    p->inode_stash[0] = parent;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_lookup_async(op, thread, parent, hash, p->rec_scratch,
                                sizeof(p->rec_scratch), diskfs_mkdir_at_check_cb,
                                request)) {
        diskfs_mkdir_at_check_cb(op, op->result, request);
    }
} /* diskfs_mkdir_at_parent_cb */

static void
diskfs_mkdir_at(
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
                              diskfs_mkdir_at_parent_cb, request);
} /* diskfs_mkdir_at */

/* inode_stash[0] = parent (locked across alloc / existing fetch) */

static void
diskfs_mknod_at_existing_cb(
    struct diskfs_inode *existing_inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *parent  = p->inode_stash[0];

    if (likely(status == CHIMERA_VFS_OK)) {
        diskfs_map_attrs(p->thread, &request->mknod_at.r_attr, existing_inode);
    }
    diskfs_map_attrs(p->thread, &request->mknod_at.r_dir_post_attr, parent);

    diskfs_op_fail(request, p->txn, CHIMERA_VFS_EEXIST);
} /* diskfs_mknod_at_existing_cb */

static void
diskfs_mknod_at_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_op_ok(request, p->txn);
} /* diskfs_mknod_at_inserted_cb */

static void
diskfs_mknod_at_alloc_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *parent  = p->inode_stash[0];
    struct diskfs_bt_op           *op;
    struct timespec                now;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    clock_gettime(CLOCK_REALTIME, &now);

    inode->size       = 0;
    inode->space_used = 0;
    inode->uid        = request->cred->uid;
    inode->gid        = request->cred->gid;
    inode->nlink      = 1;
    inode->rdev       = 0;
    inode->atime_sec  = now.tv_sec;
    inode->atime_nsec = now.tv_nsec;
    inode->mtime_sec  = now.tv_sec;
    inode->mtime_nsec = now.tv_nsec;
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;

    if (request->mknod_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        inode->mode = request->mknod_at.set_attr->va_mode;
    } else {
        inode->mode = S_IFREG | 0644;
    }
    if (request->mknod_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_RDEV) {
        inode->rdev = request->mknod_at.set_attr->va_rdev;
    }

    diskfs_apply_attrs(inode, request->mknod_at.set_attr);
    diskfs_map_attrs(thread, &request->mknod_at.r_attr, inode);

    parent->mtime_sec  = now.tv_sec;
    parent->mtime_nsec = now.tv_nsec;
    parent->ctime_sec  = now.tv_sec;
    parent->ctime_nsec = now.tv_nsec;

    diskfs_map_attrs(thread, &request->mknod_at.r_dir_post_attr, parent);

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_insert_async(op, thread, p->txn, parent,
                                request->mknod_at.name_hash, request->mknod_at.name,
                                request->mknod_at.name_len, inode->inum, inode->gen,
                                diskfs_mknod_at_inserted_cb, request)) {
        diskfs_mknod_at_inserted_cb(op, op->result, request);
    }
} /* diskfs_mknod_at_alloc_cb */

static void
diskfs_mknod_at_check_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;

    diskfs_bt_op_free(thread, op);

    if (result >= 0) {
        struct diskfs_dirent_rec *rec = (struct diskfs_dirent_rec *) p->rec_scratch;

        diskfs_inode_get_inum_async(thread, p->txn, rec->inum, rec->gen,
                                    diskfs_mknod_at_existing_cb, request);
        return;
    }

    diskfs_inode_alloc_async(thread, p->txn, diskfs_mknod_at_alloc_cb, request);
} /* diskfs_mknod_at_check_cb */

static void
diskfs_mknod_at_parent_cb(
    struct diskfs_inode *parent,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    uint64_t                       hash    = request->mknod_at.name_hash;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (!S_ISDIR(parent->mode)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    diskfs_map_attrs(thread, &request->mknod_at.r_dir_pre_attr, parent);

    p->inode_stash[0] = parent;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_lookup_async(op, thread, parent, hash, p->rec_scratch,
                                sizeof(p->rec_scratch), diskfs_mknod_at_check_cb,
                                request)) {
        diskfs_mknod_at_check_cb(op, op->result, request);
    }
} /* diskfs_mknod_at_parent_cb */

static void
diskfs_mknod_at(
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
                              diskfs_mknod_at_parent_cb, request);
} /* diskfs_mknod_at */

/* inode_stash[0] = parent (locked across child fetch) */

/* Finish a remove: map the parent's post-attrs and commit. */
static void
diskfs_remove_at_finish(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_inode           *parent = p->inode_stash[0];

    diskfs_map_attrs(p->thread, &request->remove_at.r_dir_post_attr, parent);
    diskfs_op_ok(request, p->txn);
} /* diskfs_remove_at_finish */

/* Continuation after a large deleted inode is recorded on the durable orphan
 * list: enqueue it for the in-session drainer and finish the request. */
static void
diskfs_remove_orphan_done(void *priv)
{
    struct chimera_vfs_request    *request = priv;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *inode   = p->inode_stash[1];

    diskfs_drain_enqueue(p->thread, inode->inum, inode->gen);
    diskfs_remove_at_finish(request);
} /* diskfs_remove_orphan_done */

static void
diskfs_remove_at_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *parent  = p->inode_stash[0];
    struct diskfs_inode           *inode   = p->inode_stash[1];
    struct timespec                now;

    diskfs_bt_op_free(thread, op);

    /* The dirent was located before the child fetch and the parent has been
     * write-locked throughout, so it must still be present. */
    if (unlikely(result != 1)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    clock_gettime(CLOCK_REALTIME, &now);

    if (S_ISDIR(inode->mode)) {
        parent->nlink--;
    }
    parent->mtime_sec  = now.tv_sec;
    parent->mtime_nsec = now.tv_nsec;
    parent->ctime_sec  = now.tv_sec;
    parent->ctime_nsec = now.tv_nsec;

    if (S_ISDIR(inode->mode)) {
        inode->nlink = 0;
    } else {
        inode->nlink--;
    }

    if (inode->nlink == 0) {
        request->remove_at.r_removed_attr.va_req_mask = CHIMERA_VFS_ATTR_FH;
    }

    diskfs_map_attrs(thread, &request->remove_at.r_removed_attr, inode);

    if (inode->nlink == 0) {
        --inode->refcnt;
        if (inode->refcnt == 0) {
            struct diskfs_bt_node_hdr *rh;

            diskfs_txn_pin_inode_block(thread, p->txn, inode, 0);
            rh = diskfs_bt_hdr(inode->block->iov.data, DISKFS_BT_ROOT_BASE);

            if (rh->level == 0) {
                /* Small inode (whole tree in the embedded root): reclaim inline. */
                diskfs_bt_free_tree(thread, p->txn, inode);
                diskfs_inode_free(thread, inode);
            } else {
                /* Large inode: record it on the durable orphan list -- atomic
                 * with this unlink txn (the orphan inode is acquired last, a
                 * leaf in the lock order, so no deadlock).  The continuation
                 * enqueues it for the in-session drainer + finishes; a crash
                 * before this txn commits leaves neither the unlink nor the
                 * orphan record, and after it the mount scan can resume. */
                diskfs_orphan_op_start(thread, p->txn, inode->inum, inode->gen,
                                       0 /* insert */, diskfs_remove_orphan_done,
                                       request);
                return;
            }
        }
    }

    diskfs_remove_at_finish(request);
} /* diskfs_remove_at_removed_cb */

static void
diskfs_remove_at_child_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    uint64_t                       hash    = request->remove_at.name_hash;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (S_ISDIR(inode->mode) && inode->nlink > 2) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTEMPTY);
        return;
    }

    p->inode_stash[1] = inode;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_remove_async(op, thread, p->txn, p->inode_stash[0], hash,
                                diskfs_remove_at_removed_cb, request)) {
        diskfs_remove_at_removed_cb(op, op->result, request);
    }
} /* diskfs_remove_at_child_cb */

static void
diskfs_remove_at_lookup_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_dirent_rec      *rec     = (struct diskfs_dirent_rec *) p->rec_scratch;

    diskfs_bt_op_free(thread, op);

    if (result < 0) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    diskfs_inode_get_inum_async(thread, p->txn, rec->inum, rec->gen,
                                diskfs_remove_at_child_cb, request);
} /* diskfs_remove_at_lookup_cb */

static void
diskfs_remove_at_parent_cb(
    struct diskfs_inode *parent,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    uint64_t                       hash    = request->remove_at.name_hash;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    diskfs_map_attrs(thread, &request->remove_at.r_dir_pre_attr, parent);

    if (!S_ISDIR(parent->mode)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    p->inode_stash[0] = parent;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_lookup_async(op, thread, parent, hash, p->rec_scratch,
                                sizeof(p->rec_scratch), diskfs_remove_at_lookup_cb,
                                request)) {
        diskfs_remove_at_lookup_cb(op, op->result, request);
    }
} /* diskfs_remove_at_parent_cb */

static void
diskfs_remove_at(
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
                              diskfs_remove_at_parent_cb, request);
} /* diskfs_remove_at */

/*
 * Cookie values for readdir:
 *   0 = start of directory, will return "."
 *   1 = "." was returned, will return ".."
 *   2 = ".." was returned, will return first real entry
 *   3+ = real entry cookie (hash + 3)
 */
#define DISKFS_COOKIE_DOT    1
#define DISKFS_COOKIE_DOTDOT 2
#define DISKFS_COOKIE_FIRST  3

/*
 * Readdir state machine.
 *   inode_stash[0] = directory inode (read-locked for the whole iteration)
 *   p->rd_from_hash = next dirent hash to fetch (>= cursor)
 *   p->rd_* = the dirent currently being emitted (copied out of the b+tree)
 * r_cookie/r_eof on the request are updated as we go.
 */

static void diskfs_readdir_iter_step(
    struct chimera_vfs_request *request);

static void
diskfs_readdir_complete(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p     = request->plugin_data;
    struct diskfs_inode           *inode = p->inode_stash[0];

    diskfs_map_attrs(p->thread, &request->readdir.r_dir_attr, inode);
    diskfs_op_ok(request, p->txn);
} /* diskfs_readdir_complete */

static void
diskfs_readdir_iter_inode_cb(
    struct diskfs_inode *dirent_inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct chimera_vfs_attrs       attr;
    int                            rc;

    if (status != CHIMERA_VFS_OK) {
        /* Stale dirent — skip to the next. */
        p->rd_from_hash = p->rd_hash + 1;
        diskfs_readdir_iter_step(request);
        return;
    }

    attr.va_req_mask = request->readdir.attr_mask;
    diskfs_map_attrs(thread, &attr, dirent_inode);

    /* Done with this child; release its slot so the next iteration reuses
     * it (only the directory itself stays held across the walk). */
    diskfs_txn_unlock_inode(p->txn, dirent_inode);

    rc = request->readdir.callback(
        p->rd_inum,
        p->rd_hash + DISKFS_COOKIE_FIRST,
        p->rd_name, p->rd_namelen,
        &attr, request->proto_private_data);

    request->readdir.r_cookie = p->rd_hash + DISKFS_COOKIE_FIRST;

    if (rc) {
        request->readdir.r_eof = 0;
        diskfs_readdir_complete(request);
        return;
    }

    p->rd_from_hash = p->rd_hash + 1;
    diskfs_readdir_iter_step(request);
} /* diskfs_readdir_iter_inode_cb */

static void
diskfs_readdir_next_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_dirent_rec      *rec     = (struct diskfs_dirent_rec *) p->rec_scratch;

    if (result < 0 || op->found_key.type != DISKFS_REC_DIRENT) {
        diskfs_bt_op_free(thread, op);
        request->readdir.r_eof = 1;
        diskfs_readdir_complete(request);
        return;
    }

    p->rd_hash    = op->found_key.subkey;
    p->rd_inum    = rec->inum;
    p->rd_gen     = rec->gen;
    p->rd_namelen = rec->name_len;
    memcpy(p->rd_name, rec->name, rec->name_len);

    diskfs_bt_op_free(thread, op);

    diskfs_inode_get_inum_async(thread, p->txn, p->rd_inum, p->rd_gen,
                                diskfs_readdir_iter_inode_cb, request);
} /* diskfs_readdir_next_cb */

static void
diskfs_readdir_iter_step(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_inode           *inode  = p->inode_stash[0];
    struct diskfs_bt_op           *op     = diskfs_bt_op_alloc(thread);

    if (diskfs_dir_next_async(op, thread, inode, p->rd_from_hash, &op->found_key,
                              p->rec_scratch, sizeof(p->rec_scratch),
                              diskfs_readdir_next_cb, request)) {
        diskfs_readdir_next_cb(op, op->result, request);
    }
} /* diskfs_readdir_iter_step */

static void
diskfs_readdir_start_iter(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    uint64_t                       cookie = request->readdir.r_cookie;

    if (cookie < DISKFS_COOKIE_FIRST) {
        p->rd_from_hash = 0;
    } else {
        p->rd_from_hash = (cookie - DISKFS_COOKIE_FIRST) + 1;
    }

    diskfs_readdir_iter_step(request);
} /* diskfs_readdir_start_iter */

static void
diskfs_readdir_emit_dotdot(
    struct chimera_vfs_request *request,
    struct chimera_vfs_attrs   *attr)
{
    struct diskfs_request_private *p     = request->plugin_data;
    struct diskfs_inode           *inode = p->inode_stash[0];
    int                            rc;

    rc = request->readdir.callback(
        inode->parent_inum,
        DISKFS_COOKIE_DOTDOT,
        "..", 2,
        attr, request->proto_private_data);

    if (rc) {
        request->readdir.r_cookie = DISKFS_COOKIE_DOTDOT;
        request->readdir.r_eof    = 0;
        diskfs_readdir_complete(request);
        return;
    }
    request->readdir.r_cookie = DISKFS_COOKIE_DOTDOT;
    diskfs_readdir_start_iter(request);
} /* diskfs_readdir_emit_dotdot */

static void
diskfs_readdir_dotdot_cb(
    struct diskfs_inode *parent_inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *inode   = p->inode_stash[0];
    struct chimera_vfs_attrs       attr;

    attr.va_req_mask = request->readdir.attr_mask;

    if (status == CHIMERA_VFS_OK) {
        diskfs_map_attrs(p->thread, &attr, parent_inode);
        /* Release the parent (".." target); it's distinct from the dir
        * being read (the self-parent root case never reaches here). */
        diskfs_txn_unlock_inode(p->txn, parent_inode);
    } else {
        diskfs_map_attrs(p->thread, &attr, inode);
    }

    diskfs_readdir_emit_dotdot(request, &attr);
} /* diskfs_readdir_dotdot_cb */

static void
diskfs_readdir_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    uint64_t                       cookie  = request->readdir.cookie;
    struct chimera_vfs_attrs       attr;
    int                            rc;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (!S_ISDIR(inode->mode)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    p->inode_stash[0]         = inode;
    request->readdir.r_cookie = cookie;
    request->readdir.r_eof    = 1;

    attr.va_req_mask = request->readdir.attr_mask;

    if ((request->readdir.flags & CHIMERA_VFS_READDIR_EMIT_DOT) &&
        cookie < DISKFS_COOKIE_DOT) {
        diskfs_map_attrs(thread, &attr, inode);
        rc = request->readdir.callback(
            inode->inum, DISKFS_COOKIE_DOT, ".", 1,
            &attr, request->proto_private_data);
        if (rc) {
            request->readdir.r_cookie = DISKFS_COOKIE_DOT;
            request->readdir.r_eof    = 0;
            diskfs_readdir_complete(request);
            return;
        }
        cookie                    = DISKFS_COOKIE_DOT;
        request->readdir.r_cookie = cookie;
    }

    if ((request->readdir.flags & CHIMERA_VFS_READDIR_EMIT_DOT) &&
        cookie < DISKFS_COOKIE_DOTDOT) {
        if (inode->parent_inum == inode->inum &&
            inode->parent_gen == inode->gen) {
            diskfs_map_attrs(thread, &attr, inode);
            diskfs_readdir_emit_dotdot(request, &attr);
            return;
        }
        diskfs_inode_get_inum_async(thread, p->txn,
                                    inode->parent_inum,
                                    inode->parent_gen,
                                    diskfs_readdir_dotdot_cb, request);
        return;
    }

    if (!(request->readdir.flags & CHIMERA_VFS_READDIR_EMIT_DOT) &&
        cookie < DISKFS_COOKIE_DOTDOT) {
        request->readdir.r_cookie = DISKFS_COOKIE_DOTDOT;
    }

    diskfs_readdir_start_iter(request);
} /* diskfs_readdir_inode_cb */

static void
diskfs_readdir(
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
                              diskfs_readdir_inode_cb, request);
} /* diskfs_readdir */

static void
diskfs_open_fh_inode_cb(
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

    inode->refcnt++;

    request->open_fh.r_vfs_private = (uint64_t) inode;
    diskfs_op_ok(request, p->txn);
} /* diskfs_open_fh_inode_cb */

static void
diskfs_open_fh(
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
                              diskfs_open_fh_inode_cb, request);
} /* diskfs_open_fh */

/* inode_stash[0] = parent (locked across alloc / existing-inode fetch) */

static void
diskfs_open_at_finish(
    struct chimera_vfs_request *request,
    struct diskfs_inode        *parent,
    struct diskfs_inode        *inode)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    unsigned int                   flags  = request->open_at.flags;

    if (flags & CHIMERA_VFS_OPEN_INFERRED) {
        request->open_at.r_vfs_private = 0xdeadbeefUL;
    } else {
        inode->refcnt++;
        request->open_at.r_vfs_private = (uint64_t) inode;
    }

    diskfs_map_attrs(thread, &request->open_at.r_dir_post_attr, parent);

    diskfs_map_attrs(thread, &request->open_at.r_attr, inode);

    diskfs_op_ok(request, p->txn);
} /* diskfs_open_at_finish */

static void
diskfs_open_at_existing_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *parent  = p->inode_stash[0];

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    diskfs_open_at_finish(request, parent, inode);
} /* diskfs_open_at_existing_cb */

static void
diskfs_open_at_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_open_at_finish(request, p->inode_stash[0], p->inode_stash[1]);
} /* diskfs_open_at_inserted_cb */

static void
diskfs_open_at_alloc_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *parent  = p->inode_stash[0];
    struct diskfs_bt_op           *op;
    struct timespec                now;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    clock_gettime(CLOCK_REALTIME, &now);

    inode->size       = 0;
    inode->space_used = 0;
    inode->uid        = request->cred->uid;
    inode->gid        = request->cred->gid;
    inode->nlink      = 1;
    inode->mode       = S_IFREG | 0644;
    inode->atime_sec  = now.tv_sec;
    inode->atime_nsec = now.tv_nsec;
    inode->mtime_sec  = now.tv_sec;
    inode->mtime_nsec = now.tv_nsec;
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;

    diskfs_apply_attrs(inode, request->open_at.set_attr);

    parent->mtime_sec  = now.tv_sec;
    parent->mtime_nsec = now.tv_nsec;
    parent->ctime_sec  = now.tv_sec;
    parent->ctime_nsec = now.tv_nsec;

    p->inode_stash[1] = inode;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_insert_async(op, thread, p->txn, parent,
                                request->open_at.name_hash, request->open_at.name,
                                request->open_at.namelen, inode->inum, inode->gen,
                                diskfs_open_at_inserted_cb, request)) {
        diskfs_open_at_inserted_cb(op, op->result, request);
    }
} /* diskfs_open_at_alloc_cb */

static void
diskfs_open_at_check_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    unsigned int                   flags   = request->open_at.flags;

    diskfs_bt_op_free(thread, op);

    if (result < 0) {
        if (!(flags & CHIMERA_VFS_OPEN_CREATE)) {
            diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
            return;
        }
        diskfs_inode_alloc_async(thread, p->txn, diskfs_open_at_alloc_cb, request);
        return;
    }

    if (flags & CHIMERA_VFS_OPEN_EXCLUSIVE) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EEXIST);
        return;
    }

    {
        struct diskfs_dirent_rec *rec = (struct diskfs_dirent_rec *) p->rec_scratch;

        diskfs_inode_get_inum_async(thread, p->txn, rec->inum, rec->gen,
                                    diskfs_open_at_existing_cb, request);
    }
} /* diskfs_open_at_check_cb */

static void
diskfs_open_at_parent_cb(
    struct diskfs_inode *parent,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    uint64_t                       hash    = request->open_at.name_hash;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (!S_ISDIR(parent->mode)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    diskfs_map_attrs(thread, &request->open_at.r_dir_pre_attr, parent);

    p->inode_stash[0] = parent;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_lookup_async(op, thread, parent, hash, p->rec_scratch,
                                sizeof(p->rec_scratch), diskfs_open_at_check_cb,
                                request)) {
        diskfs_open_at_check_cb(op, op->result, request);
    }
} /* diskfs_open_at_parent_cb */

static void
diskfs_open_at(
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
                              diskfs_open_at_parent_cb, request);
} /* diskfs_open_at */


static void
diskfs_create_unlinked_alloc_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct timespec                now;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    clock_gettime(CLOCK_REALTIME, &now);

    inode->size       = 0;
    inode->space_used = 0;
    inode->uid        = request->cred->uid;
    inode->gid        = request->cred->gid;
    inode->nlink      = 0;
    inode->mode       = S_IFREG | 0644;
    inode->atime_sec  = now.tv_sec;
    inode->atime_nsec = now.tv_nsec;
    inode->mtime_sec  = now.tv_sec;
    inode->mtime_nsec = now.tv_nsec;
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;

    diskfs_apply_attrs(inode, request->create_unlinked.set_attr);

    inode->refcnt++;
    request->create_unlinked.r_vfs_private = (uint64_t) inode;

    diskfs_map_attrs(thread, &request->create_unlinked.r_attr, inode);

    diskfs_op_ok(request, p->txn);
} /* diskfs_create_unlinked_alloc_cb */

static void
diskfs_create_unlinked(
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

    diskfs_inode_alloc_async(thread, p->txn,
                             diskfs_create_unlinked_alloc_cb, request);
} /* diskfs_create_unlinked */

static void
diskfs_close_inode_cb(
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

    --inode->refcnt;
    if (inode->refcnt == 0) {
        diskfs_inode_free(p->thread, inode);
    }

    diskfs_op_ok(request, p->txn);
} /* diskfs_close_inode_cb */

static void
diskfs_close(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p     = request->plugin_data;
    struct diskfs_inode           *inode = (struct diskfs_inode *) request->close.vfs_private;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);

    /* The inode pointer came in via vfs_private (set at open); re-acquire
     * it by (inum,gen) so its write lock is tracked in the cache like any
     * other op. */
    diskfs_inode_get_inum_async(thread, p->txn, inode->inum, inode->gen,
                                diskfs_close_inode_cb, request);
} /* diskfs_close */

/*
 * Adjust read iovecs: skip prefix padding and trim to actual read length.
 * Called after block reads complete (or when no reads needed).
 */
static inline void
diskfs_read_adjust_iovecs(
    struct chimera_vfs_request    *request,
    struct diskfs_request_private *diskfs_private)
{
    if (request->read.r_niov == 0) {
        return;
    }

    // Adjust first iovec to skip prefix (alignment padding)
    request->read.iov[0].data   += diskfs_private->read_prefix;
    request->read.iov[0].length -= diskfs_private->read_prefix;

    // Calculate total length across all iovecs
    uint64_t total = 0;

    for (int i = 0; i < request->read.r_niov; i++) {
        total += request->read.iov[i].length;
    }

    // Trim excess from the last iovec(s) if needed
    // Use r_length (actual capped length), not length (original request)
    // NOTE: Do NOT decrement r_niov here - we must keep the count of allocated
    // iovecs so they all get properly released by the caller.
    if (total > request->read.r_length) {
        uint64_t excess = total - request->read.r_length;
        int      last   = request->read.r_niov - 1;

        while (excess > 0 && last >= 0) {
            if (request->read.iov[last].length <= excess) {
                excess                        -= request->read.iov[last].length;
                request->read.iov[last].length = 0;
                last--;
            } else {
                request->read.iov[last].length -= excess;
                excess                          = 0;
            }
        }
    }
} /* diskfs_read_adjust_iovecs */ /* diskfs_read_adjust_iovecs */

/*
 * Data-I/O admission control.  A worker submits read/write block I/O onto its
 * per-device queues, whose submission rings are bounded (libaio 256, io_uring
 * 8192); fragmented reads fan out per extent and many connections (nconnect)
 * drive concurrent requests, so unchecked submission overruns the ring.  Cap
 * the in-flight data I/O well under the smaller ring; a request that would
 * exceed it parks and is resumed from a completion at the low watermark.  The
 * cap leaves headroom for the un-gated, self-limited metadata reads (one
 * outstanding per suspended b+tree op) sharing the same queues.
 */
#define DISKFS_IO_INFLIGHT_CAP   128
#define DISKFS_IO_INFLIGHT_LOWAT 64

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
static void
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
    thread->pending_io--;

    /* Don't finalize a read whose extent walk is still in progress (parked on
     * the admission gate): its remaining reads have yet to be issued.  The
     * io_reading guard is scoped to reads -- request plugin_data is pooled and
     * not zeroed, and only diskfs_read sets the flag (fresh, per op). */
    if (diskfs_private->pending == 0 &&
        !(diskfs_private->opcode == CHIMERA_VFS_OP_READ && diskfs_private->io_reading)) {
        if (diskfs_private->opcode == CHIMERA_VFS_OP_READ) {
            diskfs_read_adjust_iovecs(request, diskfs_private);
        }

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

/*
 * Read extent walk (async).  The data reads themselves are already async
 * (diskfs_io_callback completes the request once pending hits 0); here the
 * extent iteration is also async.  Hoisted state: inode_stash[0] = inode,
 * loop_off = read_offset, loop_left = read_left, loop_pos = aligned end,
 * rd_cursor = result-buffer assembly cursor, ext_iter = current extent.
 */
static void diskfs_read_process(
    struct chimera_vfs_request *request);

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
        diskfs_read_adjust_iovecs(request, p);
        diskfs_op_ok(request, p->txn);
    } else {
        /* I/O is in flight; drop the inode lock so other ops proceed.  The
         * txn commits from diskfs_io_callback once all reads complete. */
        diskfs_txn_unlock_inode(p->txn, inode);
    }
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
        thread->pending_io++;

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
    struct evpl                   *evpl           = thread->evpl;
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

    if (offset + length > inode->size) {
        length = inode->size > offset ? inode->size - offset : 0;
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

    aligned_offset = offset & ~4095ULL;
    aligned_length = ((offset + length + 4095ULL) & ~4095ULL) - aligned_offset;

    diskfs_private->read_prefix = offset - aligned_offset;
    diskfs_private->read_suffix = aligned_length - length;

    request->read.r_length = length;
    request->read.r_eof    = eof;

    request->read.r_niov = evpl_iovec_alloc(evpl, aligned_length, 4096, 1,
                                            0, request->read.iov);

    evpl_iovec_cursor_init(&diskfs_private->rd_cursor, request->read.iov,
                           request->read.r_niov);

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

static void
diskfs_read(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->opcode     = request->opcode;
    p->status     = 0;
    p->pending    = 0;
    p->niov       = 0;
    p->thread     = thread;
    p->io_reading = 1;     /* cleared in diskfs_read_finish when the walk ends */
    p->txn        = diskfs_txn_begin(thread, DISKFS_TXN_READ);

    diskfs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              diskfs_read_inode_cb, request);
} /* diskfs_read */

// Forward declaration
static void diskfs_write_phase2(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request);

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
    thread->pending_io--;

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
        thread->pending_io++;

        evpl_block_write(evpl,
                         thread->queue[diskfs_private->rmw_device_id],
                         chunk_iov,
                         chunk_niov,
                         diskfs_private->rmw_device_offset + offset,
                         1,
                         diskfs_io_callback,
                         request);

        offset += chunk;
        left   -= chunk;
    }

    evpl_iovecs_release(evpl, write_iov, write_niov);

} /* diskfs_write_phase2 */

/*
 * Write (read-modify-write) as an async chain:
 *   prefix lookup -> suffix lookup -> trim overlapping extents ->
 *   insert the new aligned extent -> [RMW reads] -> phase2 data write.
 * State is carried in diskfs_private (rmw_*, need_*_read, *_device_*),
 * inode_stash[0] = inode, ext_iter = current extent during the trim walk.
 */
static void diskfs_write_trim_process(
    struct chimera_vfs_request *request);
static void diskfs_write_trim_done(
    struct chimera_vfs_request *request);

/* Tail: inode metadata, unlock, RMW reads, phase2. */
static void
diskfs_write_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request        = private_data;
    struct diskfs_request_private *diskfs_private = request->plugin_data;
    struct diskfs_thread          *thread         = diskfs_private->thread;
    struct diskfs_shared          *shared         = thread->shared;
    struct evpl                   *evpl           = thread->evpl;
    struct diskfs_inode           *inode          = diskfs_private->inode_stash[0];
    uint64_t                       write_end      = request->write.offset + request->write.length;
    struct timespec                now;

    (void) result;
    diskfs_bt_op_free(thread, op);

    if (inode->size < write_end) {
        inode->size       = write_end;
        inode->space_used = (inode->size + 4095) & ~4095;
    }

    clock_gettime(CLOCK_REALTIME, &now);
    inode->mtime_sec  = now.tv_sec;
    inode->mtime_nsec = now.tv_nsec;
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;

    diskfs_map_attrs(thread, &request->write.r_post_attr, inode);

    request->write.r_length = request->write.length;
    request->write.r_sync   = 1;

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
                thread->pending_io++;
                diskfs_private->rmw_prefix_pending = 1;
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
                thread->pending_io++;
                diskfs_private->rmw_suffix_pending = 1;
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
} /* diskfs_write_inserted_cb */

static void
diskfs_write_trim_done(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_op           *op     = diskfs_bt_op_alloc(thread);

    if (diskfs_ext_insert_async(op, thread, p->txn, p->inode_stash[0],
                                p->rmw_aligned_start, p->rmw_aligned_length,
                                p->rmw_device_id, p->rmw_device_offset,
                                diskfs_write_inserted_cb, request)) {
        diskfs_write_inserted_cb(op, op->result, request);
    }
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

    if (es >= astart && ee <= aend) {
        /* Completely inside the aligned region: remove, then advance. */
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_remove_async(op, thread, p->txn, p->inode_stash[0], es,
                                    diskfs_write_trim_advance_cb, request)) {
            diskfs_write_trim_advance_cb(op, op->result, request);
        }
    } else if (es < astart && ee > aend) {
        /* Spans the region: insert tail at aligned_end first. */
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_insert_async(op, thread, p->txn, p->inode_stash[0], aend,
                                    ee - aend, p->ext_iter.device_id,
                                    p->ext_iter.device_offset + (aend - es),
                                    diskfs_write_trim_spans_after_cb, request)) {
            diskfs_write_trim_spans_after_cb(op, op->result, request);
        }
    } else if (es < astart && ee > astart) {
        /* Overlaps the left edge: remove, reinsert the head. */
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_remove_async(op, thread, p->txn, p->inode_stash[0], es,
                                    diskfs_write_trim_oleft_removed_cb, request)) {
            diskfs_write_trim_oleft_removed_cb(op, op->result, request);
        }
    } else if (es < aend && ee > aend) {
        /* Starts within, extends past: remove, reinsert tail at aligned_end. */
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

static void
diskfs_write_suffix_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request   = private_data;
    struct diskfs_request_private *p         = request->plugin_data;
    int                            have      = diskfs_ext_from_op(op, result, &p->ext_iter);
    uint64_t                       write_end = request->write.offset + request->write.length;
    uint64_t                       aend      = p->rmw_aligned_start + p->rmw_aligned_length;

    diskfs_bt_op_free(p->thread, op);

    if (have && p->ext_iter.file_offset <= write_end &&
        write_end < p->ext_iter.file_offset + p->ext_iter.length) {
        uint64_t suffix_block = write_end & ~4095ULL;
        uint64_t ee           = p->ext_iter.file_offset + p->ext_iter.length;

        if (ee >= aend) {
            p->rmw_suffix_valid = p->rmw_suffix_len;
        } else if (ee > write_end) {
            p->rmw_suffix_valid = ee - write_end;
        } else {
            p->rmw_suffix_valid = 0;
        }

        if (suffix_block >= p->ext_iter.file_offset) {
            p->need_suffix_read     = 1;
            p->suffix_device_id     = p->ext_iter.device_id;
            p->suffix_device_offset = p->ext_iter.device_offset +
                (suffix_block - p->ext_iter.file_offset);
        } else {
            p->need_suffix_read     = 1;
            p->suffix_device_id     = p->ext_iter.device_id;
            p->suffix_device_offset = p->ext_iter.device_offset;
            p->rmw_suffix_adjust    = p->ext_iter.file_offset - suffix_block;
        }
    }

    diskfs_write_trim_start(request);
} /* diskfs_write_suffix_cb */

static void
diskfs_write_suffix_lookup(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p         = request->plugin_data;
    struct diskfs_thread          *thread    = p->thread;
    uint64_t                       write_end = request->write.offset + request->write.length;
    struct diskfs_bt_op           *op;

    if (p->rmw_suffix_len == 0) {
        diskfs_write_trim_start(request);
        return;
    }

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_ext_floor_async(op, thread, p->inode_stash[0], write_end, p->rec_scratch,
                               sizeof(p->rec_scratch), diskfs_write_suffix_cb, request)) {
        diskfs_write_suffix_cb(op, op->result, request);
    }
} /* diskfs_write_suffix_lookup */

static void
diskfs_write_prefix_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    int                            have    = diskfs_ext_from_op(op, result, &p->ext_iter);
    uint64_t                       astart  = p->rmw_aligned_start;

    diskfs_bt_op_free(p->thread, op);

    if (have && p->ext_iter.file_offset <= astart &&
        astart < p->ext_iter.file_offset + p->ext_iter.length) {
        uint64_t ee = p->ext_iter.file_offset + p->ext_iter.length;

        if (ee >= astart + p->rmw_prefix_len) {
            p->rmw_prefix_valid = p->rmw_prefix_len;
        } else if (ee > astart) {
            p->rmw_prefix_valid = ee - astart;
        } else {
            p->rmw_prefix_valid = 0;
        }

        if (p->rmw_prefix_valid > 0) {
            p->need_prefix_read     = 1;
            p->prefix_device_id     = p->ext_iter.device_id;
            p->prefix_device_offset = p->ext_iter.device_offset +
                (astart - p->ext_iter.file_offset);
        }
    }

    diskfs_write_suffix_lookup(request);
} /* diskfs_write_prefix_cb */

static void
diskfs_write_prefix_lookup(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_op           *op;

    if (p->rmw_prefix_len == 0) {
        diskfs_write_suffix_lookup(request);
        return;
    }

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_ext_floor_async(op, thread, p->inode_stash[0], p->rmw_aligned_start,
                               p->rec_scratch, sizeof(p->rec_scratch),
                               diskfs_write_prefix_cb, request)) {
        diskfs_write_prefix_cb(op, op->result, request);
    }
} /* diskfs_write_prefix_lookup */

/* Retry context for the write's data-space allocation when a reservation
 * refill parks on a cold AG-log block; the continuation re-enters
 * diskfs_write_inode_cb with the (already write-locked) inode. */
struct diskfs_write_alloc_ctx {
    struct diskfs_inode        *inode;
    struct chimera_vfs_request *request;
};

static void diskfs_write_alloc_resume(
    struct diskfs_thread *thread,
    void                 *arg);

static void
diskfs_write_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request        = private_data;
    struct diskfs_request_private *diskfs_private = request->plugin_data;
    struct diskfs_thread          *thread         = diskfs_private->thread;
    uint64_t                       write_start    = request->write.offset;
    uint64_t                       write_end      = write_start + request->write.length;
    uint64_t                       aligned_start, aligned_end, aligned_length;
    uint64_t                       device_id, device_offset;
    struct diskfs_write_alloc_ctx *wctx;
    int                            rc;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, diskfs_private->txn, status);
        return;
    }

    if (unlikely(!S_ISREG(inode->mode))) {
        diskfs_op_fail(request, diskfs_private->txn, CHIMERA_VFS_EINVAL);
        return;
    }

    diskfs_map_attrs(thread, &request->write.r_pre_attr, inode);

    aligned_start  = write_start & ~4095ULL;
    aligned_end    = (write_end + 4095ULL) & ~4095ULL;
    aligned_length = aligned_end - aligned_start;

    wctx          = malloc(sizeof(*wctx));
    wctx->inode   = inode;
    wctx->request = request;
    rc            = diskfs_thread_alloc_space(thread, diskfs_private->txn, aligned_length,
                                              &device_id, &device_offset,
                                              diskfs_write_alloc_resume, wctx);
    if (rc == SM_AGAIN) {
        return;     /* parked; diskfs_write_alloc_resume re-runs (owns wctx) */
    }
    free(wctx);
    if (rc) {
        diskfs_op_fail(request, diskfs_private->txn, CHIMERA_VFS_ENOSPC);
        return;
    }

    diskfs_private->rmw_prefix_len     = write_start - aligned_start;
    diskfs_private->rmw_suffix_len     = aligned_end - write_end;
    diskfs_private->rmw_aligned_start  = aligned_start;
    diskfs_private->rmw_aligned_length = aligned_length;
    diskfs_private->rmw_device_id      = device_id;
    diskfs_private->rmw_device_offset  = device_offset;
    diskfs_private->rmw_prefix_valid   = 0;
    diskfs_private->rmw_suffix_valid   = 0;
    diskfs_private->rmw_suffix_adjust  = 0;
    diskfs_private->need_prefix_read   = 0;
    diskfs_private->need_suffix_read   = 0;
    diskfs_private->inode_stash[0]     = inode;

    diskfs_write_prefix_lookup(request);
} /* diskfs_write_inode_cb */

static void
diskfs_write_alloc_resume(
    struct diskfs_thread *thread,
    void                 *arg)
{
    struct diskfs_write_alloc_ctx c = *(struct diskfs_write_alloc_ctx *) arg;

    (void) thread;
    free(arg);
    diskfs_write_inode_cb(c.inode, CHIMERA_VFS_OK, c.request);
} /* diskfs_write_alloc_resume */


static void
diskfs_write(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

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
    p->txn                 = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);

    diskfs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              diskfs_write_inode_cb, request);
} /* diskfs_write */


/*
 * DEALLOCATE hole-punch extent walk (async).  inode_stash[0] = inode,
 * loop_off = hole_start, loop_left = hole_end, ext_iter = current extent.
 */
static void diskfs_dealloc_process(
    struct chimera_vfs_request *request);

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
        /* Spans the hole: insert tail at hole_end first. */
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_insert_async(op, thread, p->txn, p->inode_stash[0], hole_end,
                                    ee - hole_end, p->ext_iter.device_id,
                                    p->ext_iter.device_offset + (hole_end - es),
                                    diskfs_dealloc_spans_after_cb, request)) {
            diskfs_dealloc_spans_after_cb(op, op->result, request);
        }
    } else if (es < hole_start) {
        /* Overlaps the hole start: remove, then reinsert the head. */
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_remove_async(op, thread, p->txn, p->inode_stash[0], es,
                                    diskfs_dealloc_ostart_removed_cb, request)) {
            diskfs_dealloc_ostart_removed_cb(op, op->result, request);
        }
    } else {
        /* Overlaps the hole end: remove, then reinsert the tail at hole_end. */
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
    } else {
        /* ALLOCATE: extend file size if needed. */
        uint64_t new_end = request->allocate.offset + request->allocate.length;

        if (new_end > inode->size) {
            inode->size       = new_end;
            inode->space_used = (inode->size + 4095) & ~4095;
        }
    }

    diskfs_allocate_finalize(request);
} /* diskfs_allocate_inode_cb */

static void
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

/*
 * SEEK_DATA / SEEK_HOLE walk the extent map forward.  The walk is an async
 * state machine: inode_stash[0] = inode, ext_iter = current extent,
 * loop_have = whether ext_iter is valid, loop_pos = current scan position
 * (SEEK_HOLE).  Each step advances via ext_next_async.
 */
static void diskfs_seek_process(
    struct chimera_vfs_request *request);

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
            request->seek.r_eof    = 1;
            request->seek.r_offset = 0;
            diskfs_op_ok(request, p->txn);
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
            request->seek.r_eof = 0;
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
        request->seek.r_eof    = 1;
        request->seek.r_offset = 0;
        diskfs_op_ok(request, p->txn);
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

static void
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

/* inode_stash[0] = parent (locked across alloc), inode_stash[1] = new symlink */

static void
diskfs_symlink_at_dirent_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_op_ok(request, p->txn);
} /* diskfs_symlink_at_dirent_cb */

static void
diskfs_symlink_at_target_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *inode   = p->inode_stash[1];

    (void) result;
    diskfs_bt_op_free(thread, op);

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_insert_async(op, thread, p->txn, p->inode_stash[0],
                                request->symlink_at.name_hash, request->symlink_at.name,
                                request->symlink_at.namelen, inode->inum, inode->gen,
                                diskfs_symlink_at_dirent_cb, request)) {
        diskfs_symlink_at_dirent_cb(op, op->result, request);
    }
} /* diskfs_symlink_at_target_cb */

static void
diskfs_symlink_at_alloc_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *parent  = p->inode_stash[0];
    struct diskfs_bt_op           *op;
    struct timespec                now;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    clock_gettime(CLOCK_REALTIME, &now);

    inode->size       = request->symlink_at.targetlen;
    inode->space_used = request->symlink_at.targetlen;
    inode->uid        = request->cred->uid;
    inode->gid        = request->cred->gid;
    inode->nlink      = 1;
    inode->mode       = S_IFLNK | 0755;
    inode->atime_sec  = now.tv_sec;
    inode->atime_nsec = now.tv_nsec;
    inode->mtime_sec  = now.tv_sec;
    inode->mtime_nsec = now.tv_nsec;
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;

    diskfs_map_attrs(thread, &request->symlink_at.r_attr, inode);

    parent->mtime_sec  = now.tv_sec;
    parent->mtime_nsec = now.tv_nsec;
    parent->ctime_sec  = now.tv_sec;
    parent->ctime_nsec = now.tv_nsec;

    diskfs_map_attrs(thread, &request->symlink_at.r_dir_post_attr, parent);

    /* Chain: insert the symlink target into the new inode's tree, then the
     * dirent into the parent. */
    p->inode_stash[1] = inode;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_symlink_set_async(op, thread, p->txn, inode,
                                 request->symlink_at.target,
                                 request->symlink_at.targetlen,
                                 diskfs_symlink_at_target_cb, request)) {
        diskfs_symlink_at_target_cb(op, op->result, request);
    }
} /* diskfs_symlink_at_alloc_cb */

static void
diskfs_symlink_at_check_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;

    diskfs_bt_op_free(thread, op);

    if (result >= 0) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EEXIST);
        return;
    }

    diskfs_map_attrs(thread, &request->symlink_at.r_dir_pre_attr, p->inode_stash[0]);
    diskfs_inode_alloc_async(thread, p->txn, diskfs_symlink_at_alloc_cb, request);
} /* diskfs_symlink_at_check_cb */

static void
diskfs_symlink_at_parent_cb(
    struct diskfs_inode *parent,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    uint64_t                       hash    = request->symlink_at.name_hash;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (!S_ISDIR(parent->mode)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    p->inode_stash[0] = parent;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_lookup_async(op, thread, parent, hash, p->rec_scratch,
                                sizeof(p->rec_scratch), diskfs_symlink_at_check_cb,
                                request)) {
        diskfs_symlink_at_check_cb(op, op->result, request);
    }
} /* diskfs_symlink_at_parent_cb */

static void
diskfs_symlink_at(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;

    /* The target is the new inode's single b+tree record and must fit one
     * node; reject anything longer rather than aborting deeper in the insert. */
    if (request->symlink_at.targetlen > DISKFS_SYMLINK_TARGET_MAX) {
        request->status = CHIMERA_VFS_ENAMETOOLONG;
        request->complete(request);
        return;
    }

    p->txn = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);

    diskfs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              diskfs_symlink_at_parent_cb, request);
} /* diskfs_symlink_at */

static void
diskfs_readlink_done_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *inode   = p->inode_stash[0];

    diskfs_bt_op_free(p->thread, op);

    chimera_diskfs_abort_if(result < 0, "symlink record missing (inum %lu)", inode->inum);
    request->readlink.r_target_length = result;

    diskfs_map_attrs(p->thread, &request->readlink.r_attr, inode);
    diskfs_op_ok(request, p->txn);
} /* diskfs_readlink_done_cb */

static void
diskfs_readlink_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_bt_key           key     = { .type = DISKFS_REC_SYMLINK, .subkey = 0 };
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (unlikely(!S_ISLNK(inode->mode))) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EINVAL);
        return;
    }

    p->inode_stash[0] = inode;

    op = diskfs_bt_op_alloc(p->thread);
    if (diskfs_bt_lookup_async(op, p->thread, inode, DISKFS_BT_OP_LOOKUP_EXACT,
                               &key, NULL, request->readlink.r_target,
                               request->readlink.target_maxlength,
                               diskfs_readlink_done_cb, request)) {
        diskfs_readlink_done_cb(op, op->result, request);
    }
} /* diskfs_readlink_inode_cb */

static void
diskfs_readlink(
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
                              diskfs_readlink_inode_cb, request);
} /* diskfs_readlink */

static inline int
diskfs_fh_compare(
    const void *fha,
    int         fha_len,
    const void *fhb,
    int         fhb_len)
{
    int minlen = fha_len < fhb_len ? fha_len : fhb_len;

    return memcmp(fha, fhb, minlen);
} /* diskfs_fh_compare */

/*
 * Rename state machine.  Locks are taken in fh-canonical order:
 *
 *   inode_stash[0] = old_parent (locked)
 *   inode_stash[1] = new_parent (locked; aliased to [0] when same dir)
 *   inode_stash[2] = child inode (locked)
 *   inode_stash[3] = existing dest inode (locked, only when replacing)
 */

static void diskfs_rename_at_perform(
    struct chimera_vfs_request *request);

static void
diskfs_rename_at_unlock_parents(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p  = request->plugin_data;
    struct diskfs_inode           *op = p->inode_stash[0];
    struct diskfs_inode           *np = p->inode_stash[1];

    if (op) {
    }
    if (np && np != op) {
    }
} /* diskfs_rename_at_unlock_parents */

static void
diskfs_rename_at_existing_cb(
    struct diskfs_inode *existing_inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_inode           *child   = p->inode_stash[2];

    if (status != CHIMERA_VFS_OK) {
        /* Existing dirent referenced a stale inum — proceed without delete. */
        p->inode_stash[3] = NULL;
        diskfs_rename_at_perform(request);
        return;
    }

    if (S_ISDIR(child->mode) != S_ISDIR(existing_inode->mode)) {
        int err = S_ISDIR(existing_inode->mode) ? CHIMERA_VFS_EISDIR : CHIMERA_VFS_ENOTDIR;
        diskfs_rename_at_unlock_parents(request);
        diskfs_op_fail(request, p->txn, err);
        return;
    }
    if (S_ISDIR(existing_inode->mode) && existing_inode->nlink > 2) {
        diskfs_rename_at_unlock_parents(request);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTEMPTY);
        return;
    }

    p->inode_stash[3] = existing_inode;
    diskfs_rename_at_perform(request);
} /* diskfs_rename_at_existing_cb */

/*
 * Perform stage, async-chained: [remove dest dirent] -> insert new dirent ->
 * remove source dirent -> commit.  old_inum/old_gen live in rd_inum/rd_gen
 * (captured by the source lookup); the source parent stays write-locked so no
 * re-verify lookup is needed.
 */
static void diskfs_rename_at_perform_insert(
    struct chimera_vfs_request *request);

static void
diskfs_rename_at_perform_final_cb(
    struct diskfs_bt_op *bop,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *op      = p->inode_stash[0];
    struct diskfs_inode           *np      = p->inode_stash[1];
    struct diskfs_inode           *child   = p->inode_stash[2];
    struct timespec                now;

    (void) result;
    diskfs_bt_op_free(thread, bop);

    clock_gettime(CLOCK_REALTIME, &now);

    if (S_ISDIR(child->mode)) {
        op->nlink--;
        np->nlink++;
    }

    op->mtime_sec  = now.tv_sec;
    op->mtime_nsec = now.tv_nsec;
    op->ctime_sec  = now.tv_sec;
    op->ctime_nsec = now.tv_nsec;
    if (np != op) {
        np->mtime_sec  = now.tv_sec;
        np->mtime_nsec = now.tv_nsec;
        np->ctime_sec  = now.tv_sec;
        np->ctime_nsec = now.tv_nsec;
    }

    diskfs_map_attrs(thread, &request->rename_at.r_fromdir_post_attr, op);
    diskfs_map_attrs(thread, &request->rename_at.r_todir_post_attr, np);

    diskfs_rename_at_unlock_parents(request);
    diskfs_op_ok(request, p->txn);
} /* diskfs_rename_at_perform_final_cb */

static void
diskfs_rename_at_perform_inserted_cb(
    struct diskfs_bt_op *bop,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;

    (void) result;
    diskfs_bt_op_free(thread, bop);

    bop = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_remove_async(bop, thread, p->txn, p->inode_stash[0],
                                request->rename_at.name_hash,
                                diskfs_rename_at_perform_final_cb, request)) {
        diskfs_rename_at_perform_final_cb(bop, bop->result, request);
    }
} /* diskfs_rename_at_perform_inserted_cb */

static void
diskfs_rename_at_perform_insert(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_op           *bop    = diskfs_bt_op_alloc(thread);

    if (diskfs_dir_insert_async(bop, thread, p->txn, p->inode_stash[1],
                                request->rename_at.new_name_hash,
                                request->rename_at.new_name,
                                request->rename_at.new_namelen,
                                p->rd_inum, p->rd_gen,
                                diskfs_rename_at_perform_inserted_cb, request)) {
        diskfs_rename_at_perform_inserted_cb(bop, bop->result, request);
    }
} /* diskfs_rename_at_perform_insert */

static void
diskfs_rename_at_perform_removed_cb(
    struct diskfs_bt_op *bop,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request        = private_data;
    struct diskfs_request_private *p              = request->plugin_data;
    struct diskfs_inode           *np             = p->inode_stash[1];
    struct diskfs_inode           *existing_inode = p->inode_stash[3];

    diskfs_bt_op_free(p->thread, bop);

    if (result == 1) {
        existing_inode->nlink--;
        if (S_ISDIR(existing_inode->mode)) {
            np->nlink--;
        }
    }

    diskfs_rename_at_perform_insert(request);
} /* diskfs_rename_at_perform_removed_cb */

static void
diskfs_rename_at_perform(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p              = request->plugin_data;
    struct diskfs_thread          *thread         = p->thread;
    struct diskfs_inode           *existing_inode = p->inode_stash[3];
    struct diskfs_bt_op           *bop;

    if (existing_inode) {
        bop = diskfs_bt_op_alloc(thread);
        if (diskfs_dir_remove_async(bop, thread, p->txn, p->inode_stash[1],
                                    request->rename_at.new_name_hash,
                                    diskfs_rename_at_perform_removed_cb, request)) {
            diskfs_rename_at_perform_removed_cb(bop, bop->result, request);
        }
        return;
    }

    diskfs_rename_at_perform_insert(request);
} /* diskfs_rename_at_perform */

/* The dest-name lookup completed; decide replace vs hardlink-shortcut. */
static void
diskfs_rename_at_dest_cb(
    struct diskfs_bt_op *bop,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *op      = p->inode_stash[0];
    struct diskfs_inode           *np      = p->inode_stash[1];
    struct diskfs_dirent_rec      *rec     = (struct diskfs_dirent_rec *) p->rec_scratch;
    uint64_t                       existing_inum;
    uint32_t                       existing_gen;

    diskfs_bt_op_free(thread, bop);

    if (result < 0) {
        p->inode_stash[3] = NULL;
        diskfs_rename_at_perform(request);
        return;
    }

    existing_inum = rec->inum;
    existing_gen  = rec->gen;

    /* Hardlink shortcut: source and dest already refer to the same inode. */
    if (existing_inum == p->rd_inum && existing_gen == p->rd_gen) {
        diskfs_map_attrs(thread, &request->rename_at.r_fromdir_post_attr, op);
        diskfs_map_attrs(thread, &request->rename_at.r_todir_post_attr, np);
        diskfs_rename_at_unlock_parents(request);
        diskfs_op_ok(request, p->txn);
        return;
    }

    diskfs_inode_get_inum_async(thread, p->txn, existing_inum, existing_gen,
                                diskfs_rename_at_existing_cb, request);
} /* diskfs_rename_at_dest_cb */

static void
diskfs_rename_at_child_cb(
    struct diskfs_inode *child,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *np      = p->inode_stash[1];
    struct diskfs_bt_op           *bop;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_rename_at_unlock_parents(request);
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    p->inode_stash[2] = child;

    bop = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_lookup_async(bop, thread, np, request->rename_at.new_name_hash,
                                p->rec_scratch, sizeof(p->rec_scratch),
                                diskfs_rename_at_dest_cb, request)) {
        diskfs_rename_at_dest_cb(bop, bop->result, request);
    }
} /* diskfs_rename_at_child_cb */

/* The source-name lookup completed; capture old inum/gen and fetch the
 * child inode. */
static void
diskfs_rename_at_source_cb(
    struct diskfs_bt_op *bop,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_dirent_rec      *rec     = (struct diskfs_dirent_rec *) p->rec_scratch;

    diskfs_bt_op_free(thread, bop);

    if (result < 0) {
        diskfs_rename_at_unlock_parents(request);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    p->rd_inum = rec->inum;
    p->rd_gen  = rec->gen;

    diskfs_inode_get_inum_async(thread, p->txn, rec->inum, rec->gen,
                                diskfs_rename_at_child_cb, request);
} /* diskfs_rename_at_source_cb */

static void
diskfs_rename_at_have_parents(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_inode           *op     = p->inode_stash[0];
    struct diskfs_inode           *np     = p->inode_stash[1];
    struct diskfs_bt_op           *bop;

    diskfs_map_attrs(thread, &request->rename_at.r_fromdir_pre_attr, op);
    diskfs_map_attrs(thread, &request->rename_at.r_todir_pre_attr, np);

    bop = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_lookup_async(bop, thread, op, request->rename_at.name_hash,
                                p->rec_scratch, sizeof(p->rec_scratch),
                                diskfs_rename_at_source_cb, request)) {
        diskfs_rename_at_source_cb(bop, bop->result, request);
    }
} /* diskfs_rename_at_have_parents */

static void
diskfs_rename_at_second_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    int                            cmp;

    cmp = diskfs_fh_compare(request->fh, request->fh_len,
                            request->rename_at.new_fh,
                            request->rename_at.new_fhlen);

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_rename_at_unlock_parents(request);
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (!S_ISDIR(inode->mode)) {
        diskfs_rename_at_unlock_parents(request);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    if (cmp < 0) {
        p->inode_stash[1] = inode;
    } else {
        p->inode_stash[0] = inode;
    }
    diskfs_rename_at_have_parents(request);
} /* diskfs_rename_at_second_cb */

static void
diskfs_rename_at_first_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    int                            cmp;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (!S_ISDIR(inode->mode)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    cmp = diskfs_fh_compare(request->fh, request->fh_len,
                            request->rename_at.new_fh,
                            request->rename_at.new_fhlen);

    if (cmp == 0) {
        p->inode_stash[0] = inode;
        p->inode_stash[1] = inode;
        diskfs_rename_at_have_parents(request);
        return;
    }

    if (cmp < 0) {
        p->inode_stash[0] = inode;
        diskfs_inode_get_fh_async(thread, p->txn,
                                  request->rename_at.new_fh,
                                  request->rename_at.new_fhlen,
                                  diskfs_rename_at_second_cb, request);
    } else {
        p->inode_stash[1] = inode;
        diskfs_inode_get_fh_async(thread, p->txn,
                                  request->fh, request->fh_len,
                                  diskfs_rename_at_second_cb, request);
    }
} /* diskfs_rename_at_first_cb */

static void
diskfs_rename_at(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;
    int                            cmp;

    (void) shared;
    (void) private_data;

    p->thread         = thread;
    p->txn            = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);
    p->inode_stash[0] = NULL;
    p->inode_stash[1] = NULL;
    p->inode_stash[2] = NULL;
    p->inode_stash[3] = NULL;

    cmp = diskfs_fh_compare(request->fh, request->fh_len,
                            request->rename_at.new_fh,
                            request->rename_at.new_fhlen);

    if (cmp <= 0) {
        diskfs_inode_get_fh_async(thread, p->txn,
                                  request->fh, request->fh_len,
                                  diskfs_rename_at_first_cb, request);
    } else {
        diskfs_inode_get_fh_async(thread, p->txn,
                                  request->rename_at.new_fh,
                                  request->rename_at.new_fhlen,
                                  diskfs_rename_at_first_cb, request);
    }
} /* diskfs_rename_at */

/* inode_stash[0] = parent dir; inode_stash[1] = link target inode (both locked) */

static void
diskfs_link_at_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_op_ok(request, p->txn);
} /* diskfs_link_at_inserted_cb */

static void
diskfs_link_at_finish(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_inode           *parent = p->inode_stash[0];
    struct diskfs_inode           *inode  = p->inode_stash[1];
    uint64_t                       hash   = request->link_at.name_hash;
    struct diskfs_bt_op           *op;
    struct timespec                now;

    clock_gettime(CLOCK_REALTIME, &now);

    inode->nlink++;
    inode->ctime_sec   = now.tv_sec;
    inode->ctime_nsec  = now.tv_nsec;
    parent->mtime_sec  = now.tv_sec;
    parent->mtime_nsec = now.tv_nsec;
    parent->ctime_sec  = now.tv_sec;
    parent->ctime_nsec = now.tv_nsec;

    diskfs_map_attrs(thread, &request->link_at.r_attr, inode);
    diskfs_map_attrs(thread, &request->link_at.r_dir_post_attr, parent);

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_insert_async(op, thread, p->txn, parent, hash,
                                request->link_at.name, request->link_at.namelen,
                                inode->inum, inode->gen, diskfs_link_at_inserted_cb,
                                request)) {
        diskfs_link_at_inserted_cb(op, op->result, request);
    }
} /* diskfs_link_at_finish */

static void
diskfs_link_at_existing_cb(
    struct diskfs_inode *existing_inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    /* The old dirent was already removed; if its inode is still resident,
     * drop its link count for the replace. */
    if (status == CHIMERA_VFS_OK) {
        existing_inode->nlink--;
        diskfs_map_attrs(p->thread, &request->link_at.r_replaced_attr,
                         existing_inode);
    }

    diskfs_link_at_finish(request);
} /* diskfs_link_at_existing_cb */

/* dir_remove of the replaced dirent completed; fetch the old inode to fix up
 * its link count. */
static void
diskfs_link_at_removed_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    (void) result;
    diskfs_bt_op_free(p->thread, op);
    diskfs_inode_get_inum_async(p->thread, p->txn, p->rd_inum, p->rd_gen,
                                diskfs_link_at_existing_cb, request);
} /* diskfs_link_at_removed_cb */

static void
diskfs_link_at_check_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_dirent_rec      *rec     = (struct diskfs_dirent_rec *) p->rec_scratch;
    uint64_t                       hash    = request->link_at.name_hash;

    diskfs_bt_op_free(thread, op);

    if (result >= 0) {
        if (request->link_at.replace) {
            p->rd_inum = rec->inum;
            p->rd_gen  = rec->gen;

            op = diskfs_bt_op_alloc(thread);
            if (diskfs_dir_remove_async(op, thread, p->txn, p->inode_stash[0], hash,
                                        diskfs_link_at_removed_cb, request)) {
                diskfs_link_at_removed_cb(op, op->result, request);
            }
            return;
        }
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EEXIST);
        return;
    }

    diskfs_link_at_finish(request);
} /* diskfs_link_at_check_cb */

static void
diskfs_link_at_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *parent  = p->inode_stash[0];
    uint64_t                       hash    = request->link_at.name_hash;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if (unlikely(S_ISDIR(inode->mode))) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EISDIR);
        return;
    }

    p->inode_stash[1] = inode;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_dir_lookup_async(op, thread, parent, hash, p->rec_scratch,
                                sizeof(p->rec_scratch), diskfs_link_at_check_cb,
                                request)) {
        diskfs_link_at_check_cb(op, op->result, request);
    }
} /* diskfs_link_at_inode_cb */

static void
diskfs_link_at_parent_cb(
    struct diskfs_inode *parent,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    diskfs_map_attrs(thread, &request->link_at.r_dir_pre_attr, parent);

    if (!S_ISDIR(parent->mode)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    p->inode_stash[0] = parent;
    diskfs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              diskfs_link_at_inode_cb, request);
} /* diskfs_link_at_parent_cb */

static void
diskfs_link_at(
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
                              request->link_at.dir_fh,
                              request->link_at.dir_fhlen,
                              diskfs_link_at_parent_cb, request);
} /* diskfs_link_at */


static void
diskfs_put_key(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;
    uint64_t                       hash;
    int                            shard_idx;
    struct diskfs_kv_shard        *shard;
    struct diskfs_kv_entry        *entry, *existing;

    (void) private_data;

    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);

    hash      = chimera_vfs_hash(request->put_key.key, request->put_key.key_len);
    shard_idx = hash % shared->num_kv_shards;
    shard     = &shared->kv_shards[shard_idx];

    pthread_mutex_lock(&shard->lock);

    rb_tree_query_exact(&shard->entries, hash, hash, existing);

    if (existing) {
        slab_allocator_free(thread->allocator, existing->value, existing->value_len);
        existing->value_len = request->put_key.value_len;
        existing->value     = slab_allocator_alloc(thread->allocator, request->put_key.value_len);
        memcpy(existing->value, request->put_key.value, request->put_key.value_len);
    } else {
        entry = diskfs_kv_entry_alloc(thread, hash,
                                      request->put_key.key,
                                      request->put_key.key_len,
                                      request->put_key.value,
                                      request->put_key.value_len);
        rb_tree_insert(&shard->entries, hash, entry);
    }

    pthread_mutex_unlock(&shard->lock);

    diskfs_op_ok(request, p->txn);
} /* diskfs_put_key */

static void
diskfs_get_key(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;
    uint64_t                       hash;
    int                            shard_idx;
    struct diskfs_kv_shard        *shard;
    struct diskfs_kv_entry        *entry;

    (void) private_data;

    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, DISKFS_TXN_READ);

    hash      = chimera_vfs_hash(request->get_key.key, request->get_key.key_len);
    shard_idx = hash % shared->num_kv_shards;
    shard     = &shared->kv_shards[shard_idx];

    pthread_mutex_lock(&shard->lock);

    rb_tree_query_exact(&shard->entries, hash, hash, entry);

    if (!entry) {
        pthread_mutex_unlock(&shard->lock);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    request->get_key.r_value     = entry->value;
    request->get_key.r_value_len = entry->value_len;

    pthread_mutex_unlock(&shard->lock);

    diskfs_op_ok(request, p->txn);
} /* diskfs_get_key */

static void
diskfs_delete_key(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;
    uint64_t                       hash;
    int                            shard_idx;
    struct diskfs_kv_shard        *shard;
    struct diskfs_kv_entry        *entry;

    (void) private_data;

    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);

    hash      = chimera_vfs_hash(request->delete_key.key, request->delete_key.key_len);
    shard_idx = hash % shared->num_kv_shards;
    shard     = &shared->kv_shards[shard_idx];

    pthread_mutex_lock(&shard->lock);

    rb_tree_query_exact(&shard->entries, hash, hash, entry);

    if (!entry) {
        pthread_mutex_unlock(&shard->lock);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    rb_tree_remove(&shard->entries, &entry->node);
    pthread_mutex_unlock(&shard->lock);

    diskfs_kv_entry_free(thread, entry);

    diskfs_op_ok(request, p->txn);
} /* diskfs_delete_key */

static int
diskfs_kv_key_in_range(
    const void *key,
    uint32_t    key_len,
    const void *start_key,
    uint32_t    start_key_len,
    const void *end_key,
    uint32_t    end_key_len)
{
    int cmp;

    /* Compare key to start_key */
    if (start_key_len > 0) {
        cmp = memcmp(key, start_key,
                     key_len < start_key_len ? key_len : start_key_len);
        if (cmp < 0 || (cmp == 0 && key_len < start_key_len)) {
            return 0; /* key < start_key */
        }
    }

    /* Compare key to end_key */
    if (end_key_len > 0) {
        cmp = memcmp(key, end_key,
                     key_len < end_key_len ? key_len : end_key_len);
        if (cmp > 0 || (cmp == 0 && key_len > end_key_len)) {
            return 0; /* key > end_key */
        }
    }

    return 1; /* key is in range [start_key, end_key] */
} /* diskfs_kv_key_in_range */

static void
diskfs_search_keys(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private     *p = request->plugin_data;
    int                                i, rc;
    struct diskfs_kv_shard            *shard;
    struct diskfs_kv_entry            *entry;
    chimera_vfs_search_keys_callback_t callback = request->search_keys.callback;

    (void) private_data;

    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, DISKFS_TXN_READ);

    for (i = 0; i < shared->num_kv_shards; i++) {
        shard = &shared->kv_shards[i];

        pthread_mutex_lock(&shard->lock);

        rb_tree_first(&shard->entries, entry);

        while (entry) {
            if (diskfs_kv_key_in_range(entry->key,
                                       entry->key_len,
                                       request->search_keys.start_key,
                                       request->search_keys.start_key_len,
                                       request->search_keys.end_key,
                                       request->search_keys.end_key_len)) {
                rc = callback(entry->key, entry->key_len,
                              entry->value, entry->value_len,
                              request->proto_private_data);

                if (rc) {
                    pthread_mutex_unlock(&shard->lock);
                    diskfs_op_ok(request, p->txn);
                    return;
                }
            }

            entry = rb_tree_next(&shard->entries, entry);
        }

        pthread_mutex_unlock(&shard->lock);
    }

    diskfs_op_ok(request, p->txn);
} /* diskfs_search_keys */

static void
diskfs_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_thread *thread = private_data;
    struct diskfs_shared *shared = thread->shared;

    if (unlikely(shared->root_fhlen == 0)) {
        diskfs_bootstrap(thread);
    }

    /* Resume any inode drains left pending by a crash (once per mount). */
    if (unlikely(!shared->orphans_scanned)) {
        diskfs_orphan_scan(thread);
    }

    switch (request->opcode) {
        case CHIMERA_VFS_OP_MOUNT:
            diskfs_mount(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_UMOUNT:
            diskfs_umount(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LOOKUP_AT:
            diskfs_lookup_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            diskfs_getattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            diskfs_setattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKDIR_AT:
            diskfs_mkdir_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKNOD_AT:
            diskfs_mknod_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE_AT:
            diskfs_remove_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READDIR:
            diskfs_readdir(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            diskfs_open_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_FH:
            diskfs_open_fh(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_CREATE_UNLINKED:
            diskfs_create_unlinked(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_CLOSE:
            diskfs_close(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READ:
            diskfs_read(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_WRITE:
            diskfs_write(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_COMMIT:
        {
            /* No-op today: writes already wait for durability before
             * acking.  Once a real intent log lands, COMMIT will fence
             * on outstanding intent log records. */
            struct diskfs_request_private *cp = request->plugin_data;
            cp->thread = thread;
            cp->txn    = diskfs_txn_begin(thread, DISKFS_TXN_READ);
            diskfs_op_ok(request, cp->txn);
        }
        break;
        case CHIMERA_VFS_OP_ALLOCATE:
            diskfs_allocate(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SEEK:
            diskfs_seek(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SYMLINK_AT:
            diskfs_symlink_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READLINK:
            diskfs_readlink(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_RENAME_AT:
            diskfs_rename_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LINK_AT:
            diskfs_link_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_PUT_KEY:
            diskfs_put_key(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_GET_KEY:
            diskfs_get_key(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_DELETE_KEY:
            diskfs_delete_key(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SEARCH_KEYS:
            diskfs_search_keys(thread, shared, request, private_data);
            break;
        default:
            chimera_diskfs_error("diskfs_dispatch: unknown operation %d",
                                 request->opcode);
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* diskfs_dispatch */

SYMBOL_EXPORT struct chimera_vfs_module vfs_diskfs = {
    .name         = "diskfs",
    .fh_magic     = CHIMERA_VFS_FH_MAGIC_DISKFS,
    .capabilities = CHIMERA_VFS_CAP_CREATE_UNLINKED | CHIMERA_VFS_CAP_FS | CHIMERA_VFS_CAP_KV |
        CHIMERA_VFS_CAP_FS_RELATIVE_OP,
    .init           = diskfs_init,
    .destroy        = diskfs_destroy,
    .thread_init    = diskfs_thread_init,
    .thread_destroy = diskfs_thread_destroy,
    .dispatch       = diskfs_dispatch,
};
