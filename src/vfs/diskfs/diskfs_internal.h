// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Shared internal header for the diskfs VFS module: on-disk formats, core
 * types (shared state, threads, inodes, blocks, transactions, intent log,
 * b+tree ops), constants, metrics plumbing, cross-file declarations and the
 * hot-path inline helpers.  Each diskfs_*.c includes only this header.
 */

#pragma once

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

#include <urcu/urcu-qsbr.h>

#include <xxhash.h>     /* XXH_INLINE_ALL set in CMakeLists; header-only */


#include "common/varint.h"

#include "common/rbtree.h"


#include "slab_allocator.h"


#include "evpl/evpl.h"

#include "prometheus-c.h"


#include "vfs/vfs.h"

#include "vfs/vfs_internal.h"

#include "vfs/vfs_acl.h"

#include "vfs/vfs_access.h"

#include "vfs/vfs_acl_serialize.h"

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

/* Sharded orphan-list inodes: deleted-but-not-yet-reclaimed inodes are
 * recorded as DISKFS_REC_ORPHAN keys spread across these (by deleted inum),
 * so concurrent unlinks don't serialize on a single inode's write lock
 * (inode locks are held until the txn is durable).  Created at format,
 * permanent.  Count is baked into the on-disk bootstrap layout. */
#define DISKFS_ORPHAN_INUM_BASE   3

#define DISKFS_ORPHAN_SHARDS      SM_BOOTSTRAP_ORPHAN_SLOTS

#define DISKFS_ORPHAN_GEN         1   /* permanent: created at format, never deleted */


#define DISKFS_ORPHAN_SHARD_INUM(inum) \
        (DISKFS_ORPHAN_INUM_BASE + ((inum) % DISKFS_ORPHAN_SHARDS))


/* Max inodes a single transaction can hold locked at once.  rename needs
 * 5 (two parents, child, replaced target, plus the orphan-list shard when
 * the replace deletes the target); others (e.g. readdir) touch many but
 * only 2 at a time and release as they go. */
#define DISKFS_TXN_MAX_INODES     5


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
    uint32_t              flags;       /* DISKFS_EXT_* */
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
    /* rename(2) descendant-loop check: cursor inum/gen of the destination
     * parent's ancestor currently being examined, and the walk depth. */
    uint64_t                    anc_inum;
    uint32_t                    anc_gen;
    int                         anc_depth;
    int                         rd_namelen;
    char                        rd_name[256];
    /* readdir trampoline state: when the b+tree and child inodes are all
     * cache-resident, each step completes synchronously and the
     * iter_inode_cb -> iter_step tail call would otherwise recurse once per
     * entry and overflow the stack on a large directory.  rd_looping marks
     * that an iteration loop is active; rd_advance asks it for another step;
     * rd_done flags that the walk finished (completed by the loop, not inline). */
    int                         rd_looping;
    int                         rd_advance;
    int                         rd_done;

    /* Scratch buffer for handlers that parse a looked-up b+tree record
     * (e.g. a dirent's inum/gen) in their async continuation.  Sized to hold
     * the largest record (DISKFS_DIRENT_REC_MAX == sizeof(dirent_rec) + 256). */
    char                        rec_scratch[320];

    /* Heap landing buffer (DISKFS_BT_NODE_CAP) for the xattr handlers' async
     * record lookups; it must survive a suspension, so it cannot live on the
     * stack of the step that issued the lookup.  Allocated by the step that
     * starts a lookup, freed by the continuation that consumes it. */
    void                       *xattr_rec;

    /* Extent-walk iteration state, hoisted here so an async ext_next can
     * suspend the loop and resume it.  loop_* are generic loop scalars. */
    struct diskfs_extent        ext_iter;
    struct evpl_iovec_cursor    rd_cursor;
    uint64_t                    loop_off;
    uint64_t                    loop_left;
    uint64_t                    loop_pos;
    int                         loop_have;
    uint64_t                    alloc_cap;   /* ALLOCATE: adaptive per-chunk cap */

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
    /* Set when the write hit a single already-WRITTEN extent fully covering the
     * range (in-place overwrite, extent map untouched): the only inode change
     * is the mtime/ctime bump, so a non-FILE_SYNC write can defer it. */
    int                         inplace_written;
    uint64_t                    prefix_device_id, prefix_device_offset;
    uint64_t                    suffix_device_id, suffix_device_offset;

    /* Coalescing-insert descriptor (diskfs_ext_put): the extent to record,
     * merged with a contiguous predecessor when possible; ci_cont runs after. */
    uint64_t                    ci_off, ci_len, ci_devoff;
    uint32_t                    ci_devid, ci_flags;
    void                        (*ci_cont)(
        struct chimera_vfs_request *);
};


struct diskfs_device {
    struct evpl_block_device *bdev;            /* NULL for a REMOTE (pNFS data) device */
    uint64_t                  id;
    uint64_t                  size;
    uint64_t                  max_request_size;
    char                      name[256];
    pthread_mutex_t           lock;

    /* Block-mode (pNFS) device identity.  role == SM_DEV_REMOTE means this
     * device's storage lives outside this system: diskfs allocates space on it
     * and hands the layout to the block client but never opens or touches it. */
    uint32_t                  role;             /* SM_DEV_LOCAL | SM_DEV_REMOTE */
    uint8_t                   deviceid[SM_DEVICEID_SIZE];
    uint64_t                  sig_offset;
    uint32_t                  sig_len;
    uint8_t                   sig[SM_SIG_MAX];

    /* SCSI-layout (RFC 8154) hardware identity: the LU's VPD-0x83 designator a
     * pNFS-SCSI client matches against (nothing written to the data disk).
     * Used when the share is in scsi_layout mode instead of sig_*. */
    uint32_t                  scsi_code_set;     /* 1=binary, 2=ascii */
    uint32_t                  scsi_desig_type;   /* 1=T10, 2=EUI64, 3=NAA */
    uint32_t                  scsi_desig_len;
    uint8_t                   scsi_desig[32];
    uint64_t                  scsi_pr_key;
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


enum diskfs_metric_inode_cache_op {
    DISKFS_METRIC_INODE_CACHE_HIT,
    DISKFS_METRIC_INODE_CACHE_MISS,
    DISKFS_METRIC_INODE_CACHE_STALE,
    DISKFS_METRIC_INODE_CACHE_LOAD,
    DISKFS_METRIC_INODE_CACHE_INSERT,
    DISKFS_METRIC_INODE_CACHE_WAIT,
    DISKFS_METRIC_INODE_CACHE_NUM,
};


enum diskfs_metric_block_cache_op {
    DISKFS_METRIC_BLOCK_CACHE_HIT,
    DISKFS_METRIC_BLOCK_CACHE_MISS,
    DISKFS_METRIC_BLOCK_CACHE_NEW,
    DISKFS_METRIC_BLOCK_CACHE_WAIT,
    DISKFS_METRIC_BLOCK_CACHE_COW,
    DISKFS_METRIC_BLOCK_CACHE_RECYCLE,
    DISKFS_METRIC_BLOCK_CACHE_NUM,
};


/* Deferred-mtime accounting: did a write defer its inode timestamp log, and if
 * not, which gate stopped it.  flushed = coalesced flush records issued. */
enum diskfs_metric_mtime_op {
    DISKFS_METRIC_MTIME_DEFERRED,         /* write deferred its inode log */
    DISKFS_METRIC_MTIME_FLUSHED,          /* coalesced flush record issued */
    DISKFS_METRIC_MTIME_SKIP_NOT_INPLACE, /* not a single-written-extent overwrite */
    DISKFS_METRIC_MTIME_SKIP_SIZE_GREW,   /* write grew the file */
    DISKFS_METRIC_MTIME_SKIP_FILESYNC,    /* client requested FILE_SYNC */
    DISKFS_METRIC_MTIME_NUM,
};


enum diskfs_metric_io_dir {
    DISKFS_METRIC_IO_READ,
    DISKFS_METRIC_IO_WRITE,
    DISKFS_METRIC_IO_NUM_DIRS,
};


enum diskfs_metric_io_class {
    DISKFS_METRIC_IO_DATA,
    DISKFS_METRIC_IO_RMW,
    DISKFS_METRIC_IO_INODE,
    DISKFS_METRIC_IO_BTREE,
    DISKFS_METRIC_IO_METADATA,
    DISKFS_METRIC_IO_INTENT_LOG,
    DISKFS_METRIC_IO_TAIL_PUSH,
    DISKFS_METRIC_IO_NUM_CLASSES,
};


enum diskfs_metric_txn_phase {
    DISKFS_METRIC_TXN_QUEUE_TO_SUBMIT,
    DISKFS_METRIC_TXN_SUBMIT_TO_DURABLE,
    DISKFS_METRIC_TXN_QUEUE_TO_DURABLE,
    DISKFS_METRIC_TXN_DURABLE_TO_CALLBACK,
    DISKFS_METRIC_TXN_QUEUE_TO_CALLBACK,
    DISKFS_METRIC_TXN_NUM_PHASES,
};


struct diskfs_metrics {
    struct prometheus_metrics          *metrics;
    int                                 num_devices;
    struct prometheus_counter          *inode_cache;
    struct prometheus_counter_series   *inode_cache_series[DISKFS_METRIC_INODE_CACHE_NUM];
    struct prometheus_counter          *block_cache;
    struct prometheus_counter_series   *block_cache_series[DISKFS_METRIC_BLOCK_CACHE_NUM];
    struct prometheus_counter          *mtime;
    struct prometheus_counter_series   *mtime_series[DISKFS_METRIC_MTIME_NUM];
    struct prometheus_counter          *block_io_ops;
    struct prometheus_counter_series *block_io_ops_series[DISKFS_METRIC_IO_NUM_DIRS][DISKFS_METRIC_IO_NUM_CLASSES];
    struct prometheus_counter          *block_io_bytes;
    struct prometheus_counter_series *block_io_bytes_series[DISKFS_METRIC_IO_NUM_DIRS][DISKFS_METRIC_IO_NUM_CLASSES];
    struct prometheus_counter          *block_io_device_ops;
    struct prometheus_counter_series  **block_io_device_ops_series;
    struct prometheus_counter          *block_io_device_bytes;
    struct prometheus_counter_series  **block_io_device_bytes_series;
    struct prometheus_counter          *txn;
    struct prometheus_counter_series   *txn_series[3];
    struct prometheus_histogram        *txn_blocks;
    struct prometheus_histogram_series *txn_blocks_series;
    struct prometheus_histogram        *txn_bytes;
    struct prometheus_histogram_series *txn_bytes_series;
    struct prometheus_histogram        *txn_latency;
    struct prometheus_histogram_series *txn_latency_series[DISKFS_METRIC_TXN_NUM_PHASES];
    struct prometheus_gauge            *pending_io;
    struct prometheus_gauge_series     *pending_io_series;
    struct prometheus_gauge            *intent_log;
    struct prometheus_gauge_series     *intent_log_series[9];
};


struct diskfs_thread_metrics {
    struct prometheus_counter_instance   *inode_cache[DISKFS_METRIC_INODE_CACHE_NUM];
    struct prometheus_counter_instance   *block_cache[DISKFS_METRIC_BLOCK_CACHE_NUM];
    struct prometheus_counter_instance   *mtime[DISKFS_METRIC_MTIME_NUM];
    struct prometheus_counter_instance *block_io_ops[DISKFS_METRIC_IO_NUM_DIRS][DISKFS_METRIC_IO_NUM_CLASSES];
    struct prometheus_counter_instance *block_io_bytes[DISKFS_METRIC_IO_NUM_DIRS][DISKFS_METRIC_IO_NUM_CLASSES];
    struct prometheus_counter_instance  **block_io_device_ops;
    struct prometheus_counter_instance  **block_io_device_bytes;
    struct prometheus_counter_instance   *txn[3];
    struct prometheus_histogram_instance *txn_blocks;
    struct prometheus_histogram_instance *txn_bytes;
    struct prometheus_histogram_instance *txn_latency[DISKFS_METRIC_TXN_NUM_PHASES];
    struct prometheus_gauge_instance     *pending_io;
};


struct diskfs_intent_log_metrics {
    struct prometheus_counter_instance *block_io_ops[DISKFS_METRIC_IO_NUM_DIRS][DISKFS_METRIC_IO_NUM_CLASSES];
    struct prometheus_counter_instance *block_io_bytes[DISKFS_METRIC_IO_NUM_DIRS][DISKFS_METRIC_IO_NUM_CLASSES];
    struct prometheus_counter_instance  **block_io_device_ops;
    struct prometheus_counter_instance  **block_io_device_bytes;
    struct prometheus_histogram_instance *txn_latency[DISKFS_METRIC_TXN_NUM_PHASES];
    struct prometheus_gauge_instance     *redo_inflight;
    struct prometheus_gauge_instance     *iocbs_inflight;
    struct prometheus_gauge_instance     *push_outstanding;
    struct prometheus_gauge_instance     *log_used_bytes;
    struct prometheus_gauge_instance     *registered_channels;
    struct prometheus_gauge_instance     *redo_inflight_high_water;
    struct prometheus_gauge_instance     *iocbs_inflight_high_water;
    struct prometheus_gauge_instance     *push_outstanding_high_water;
    struct prometheus_gauge_instance     *log_used_bytes_high_water;
};


/* Logical lock mode for an inode held by a transaction. */
enum diskfs_inode_lock_mode {
    DISKFS_INODE_LOCK_READ,
    DISKFS_INODE_LOCK_WRITE,
};


typedef void (*diskfs_inode_cb_t)(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);


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
    uint64_t                    btime_sec;
    uint32_t                    atime_nsec;
    uint32_t                    ctime_nsec;
    uint32_t                    mtime_nsec;
    uint32_t                    btime_nsec;
    uint32_t                    dos_attributes;

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

    /* Deferred mtime/ctime: a non-FILE_SYNC in-place overwrite bumps the
     * timestamps in memory and links the inode on its shard's mtime-dirty list
     * (holding an extra refcnt to keep it resident) instead of logging the
     * inode block per write; a periodic flusher coalesces them durable.  All
     * under the shard lock; an inode is never on both the LRU and this list
     * (mtime_dirty implies refcnt > 1, so it is not idle). */
    struct diskfs_inode        *mdirty_prev, *mdirty_next;
    int                         mtime_dirty;
    uint64_t                    mtime_dirty_since;  /* monotonic tick of first dirty */

    /* Per-open-file data-space reservation (RAM only, not persisted): a write
     * over-reserves SM_RESERVATION_MIN and retains the tail here for this file's
     * next write so its blocks lay out sequentially; the tail is returned to the
     * space map when the last open handle closes (diskfs_inode_return_reservation).
     * Mutated only under the inode write lock the data path already holds. */
    struct sm_thread_cache      space_resv;

    /* This inode's 4 KiB metadata home block in the block cache; pinned
     * while the inode is dirty in a transaction.  NULL until first claimed.
     * Directory entries, extents and the symlink target all live as keyed
     * records in this inode's b+tree (rooted in the block at offset 256). */
    struct diskfs_block        *block;

    /* In-memory copies of this inode's singleton ACL and pNFS-layout b+tree
     * records (NULL = no record).  The b+tree record stays the durable form;
     * these are faulted in with the inode (so attr mapping never reads the
     * tree, which could suspend on an evicted node) and kept coherent by the
     * write paths under the inode write lock.  Freed with the struct. */
    uint8_t                    *acl_serial;
    uint32_t                    acl_serial_len;
    uint8_t                    *pnfs_blob;
    uint32_t                    pnfs_blob_len;

    /* Directory only: parent for ".." resolution (also persisted in dinode). */
    uint64_t                    parent_inum;
    uint32_t                    parent_gen;
};


struct diskfs_inode_shard {
    pthread_mutex_t      lock;
    struct rb_tree       inodes;       /* keyed by inum */
    struct diskfs_inode *lru_head, *lru_tail; /* idle (recycle) candidates, LRU-first */
    uint32_t             ninodes;      /* resident inodes in this shard */
    struct diskfs_inode *mdirty_head, *mdirty_tail; /* deferred-mtime queue (FIFO) */
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
    uint64_t btime_sec;
    uint32_t atime_nsec;
    uint32_t mtime_nsec;
    uint32_t ctime_nsec;
    uint32_t btime_nsec;
    uint32_t dos_attributes;
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
    DISKFS_REC_XATTR   = 5,
    DISKFS_REC_PNFS    = 6,   /* regular file: opaque pNFS layout blob (flex-files) */
    DISKFS_REC_ACL     = 7,   /* single record: serialized NFSv4/Windows ACL (subkey 0) */
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


/* extent_rec.flags bits */
#define DISKFS_EXT_UNWRITTEN 0x1u   /* space reserved (e.g. fallocate) but never
                                     *
                                     * written: reads return zeros, the first
                                     * write clears the bit */

struct diskfs_extent_rec {
    uint64_t length;
    uint32_t device_id;
    uint32_t flags;
    uint64_t device_offset;
} __attribute__((packed));


struct diskfs_xattr_rec {
    uint32_t name_len;
    uint32_t value_len;
    char     data[];
} __attribute__((packed));


#define DISKFS_DIRENT_REC_MAX (sizeof(struct diskfs_dirent_rec) + 256)


/*
 * A symlink stores its target as the single record of the new inode's b+tree,
 * which lives in the embedded root (it can never be split out -- there's only
 * one record), so the target must fit one node: root capacity minus the node
 * header and one leaf slot.  Longer targets are rejected with ENAMETOOLONG
 * rather than aborting the daemon.
 */
#define DISKFS_SYMLINK_TARGET_MAX \
        (DISKFS_BT_ROOT_CAP - sizeof(struct diskfs_bt_node_hdr) - sizeof(struct diskfs_bt_lslot))

#define DISKFS_XATTR_REC_MAX \
        (DISKFS_BT_ROOT_CAP - sizeof(struct diskfs_bt_node_hdr) - sizeof(struct diskfs_bt_lslot))


/* The biggest ACL whose serialized form still fits one b+tree record. */
#define DISKFS_ACL_REC_MAX \
        (DISKFS_BT_ROOT_CAP - sizeof(struct diskfs_bt_node_hdr) - sizeof(struct diskfs_bt_lslot))

#define DISKFS_ACL_REC_MAX_ACES \
        (((DISKFS_ACL_REC_MAX) -CHIMERA_ACL_SERIAL_HDR) / CHIMERA_ACL_SERIAL_ACE)


/*
 * Intent-log redo record, written into the reserved intent-log region.
 * A record is a header followed by num_blocks (block-header, 4 KiB content)
 * pairs, padded to a 4 KiB multiple.  Full-block redo: the record carries
 * the entire post-image of every dirty block in the transaction.
 */
#define DISKFS_REDO_MAGIC 0x4F44455246534944ULL     /* "DISFREDO" */


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

    /* When the IL submission queue is full, the commit parks on its worker's
     * commit-wait FIFO (carrying its completion cb) instead of spinning the
     * event loop; the CQ doorbell resumes it once SQ space frees. */
    diskfs_txn_commit_cb_t   commit_cb;
    void                    *commit_private;
    struct diskfs_txn       *commit_wait_next;
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
    struct diskfs_txn          *txn;
    diskfs_txn_commit_cb_t      cb;
    void                       *private_data;
    struct prometheus_stopwatch enqueue_time;
    struct prometheus_stopwatch submit_time;
    struct prometheus_stopwatch durable_time;
    int                         status;
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
    int                       registered;
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
    /* Push-thread lifetime: a record is freed only after it is logically
     * retired (trimmed from the log) AND no in-flight home write still reads
     * its iovs (inflight_refs == 0). */
    int                      inflight_refs;
    int                      retired;
};


/*
 * Push-side per-(device,offset) pending home write: the newest logged image of
 * a block that is not yet durably home.  Lives on the push thread only.
 * `iov`/`owner` track the newest image; the `issued_*` fields snapshot the
 * in-flight write's source, so an image that supersedes mid-flight is re-issued
 * on completion (newest lands last) and the in-flight write's source record
 * stays pinned (inflight_refs) until that write lands.
 */
struct diskfs_pending {
    struct diskfs_intent_log *il;          /* owner (recovered in the block-write cb) */
    uint32_t                  device_id;
    uint64_t                  device_offset;
    uint64_t                  seq;         /* newest pending image seq */
    struct evpl_iovec        *iov;         /* newest image (points into owner->iovs) */
    struct diskfs_il_record  *owner;       /* record owning the newest image */
    int                       inflight;    /* a home write is in flight for this block */
    uint64_t                  issued_seq;  /* seq of the in-flight write */
    struct diskfs_il_record  *issued_owner; /* record the in-flight write reads from */
    int                       on_ready;    /* queued on ready list */
    struct diskfs_pending    *hnext;       /* hash chain */
    struct diskfs_pending    *rnext;       /* ready-queue / free-list link */
};


/*
 * Commit-side retirement-ring slot: a submitted redo record awaiting in-order
 * retirement.  `done` is set when the record's journal write(s) complete; the
 * ring retires strictly in submission (== log) order, so ACKs to workers and
 * the hand-off to the push thread happen in order -- replay stops at the first
 * torn record, so a later record is not recoverable until every earlier record
 * is durable.
 */
struct diskfs_retire_slot {
    struct diskfs_redo_ctx *ctx;
    int                     done;
};


#define DISKFS_RETIRE_RING_SIZE  1024   /* >= DISKFS_COMMIT_WATERMARK */

#define DISKFS_RETIRE_RING_MASK  (DISKFS_RETIRE_RING_SIZE - 1)

/* > max records the log can ever hold (one record is >= one 4 KiB block, so a
 * 64 MiB log holds < 16384 records); sized so the ring can never fill. */
#define DISKFS_HANDOFF_RING_SIZE 32768

#define DISKFS_HANDOFF_RING_MASK (DISKFS_HANDOFF_RING_SIZE - 1)


struct diskfs_intent_log {
    /* ---------- commit thread ---------- */
    struct evpl_doorbell             wake_doorbell;   /* workers + push-trim ring this */
    struct evpl                     *evpl;            /* commit thread evpl */
    struct evpl_thread              *thread;          /* commit thread */
    int                              ready;           /* atomic: commit thread up */
    int                              shutdown;        /* atomic */
    int                              commit_alive;    /* atomic: 1 while the commit thread's wake_doorbell is live; the push thread must not ring it once 0 (cleared before the commit thread is destroyed, which closes that fd) */
    struct evpl_poll                *sq_poll;         /* polls all channel SQs every loop iteration */
    int                              awake;           /* atomic (seq_cst): 1 while the commit thread is in poll mode (not blocked).  A submitter skips ringing wake_doorbell when this is set; see diskfs_iq_try_submit / diskfs_il_poll_exit. */
    int                              reg_dirty;       /* atomic (seq_cst): a channel (un)registration is pending.  Set by workers after touching pending_head / unregister_requested; the commit thread's per-iteration poll services it without waiting for the wake doorbell (which is starved while we stay in continuous poll mode under load). */
    uint32_t                         num_channels;
    struct diskfs_iq_channel        *channels[DISKFS_IL_MAX_CHANNELS];
    pthread_mutex_t                  registration_lock;
    struct diskfs_iq_channel        *pending_head;
    struct evpl_block_queue         *log_queue;       /* redo writes -> intent-log device */
    uint64_t                         log_seq;         /* next redo seq (commit only) */
    /* Records placed in the log and not yet trimmed past (atomic; incremented
     * by the commit thread at placement, decremented by the push thread when
     * the trim point passes the record).  Zero means the log is logically
     * empty even when log_head != log_tail -- see the stale-trim-point reset
     * in diskfs_iq_process_channel. */
    uint64_t                         live_records;
    struct diskfs_retire_slot       *retire;          /* [DISKFS_RETIRE_RING_SIZE] */
    uint64_t                         retire_head;     /* next slot to retire (in order) */
    uint64_t                         retire_tail;     /* next submission index */
    int                              redo_inflight;   /* redo block writes in flight (commit watermark) */

    /* ---------- push thread ---------- */
    struct evpl_doorbell             push_doorbell;   /* commit rings after hand-off */
    struct evpl                     *push_evpl;
    struct evpl_thread              *push_thread;
    int                              push_ready;      /* atomic */
    struct evpl_block_queue        **home_queue;      /* [num_devices] home writes */
    struct diskfs_il_record         *push_head;       /* record FIFO (log order, for trim) */
    struct diskfs_il_record         *push_tail;
    struct diskfs_pending          **phash;           /* [phash_mask+1] (dev,off) buckets */
    uint32_t                         phash_mask;
    struct diskfs_pending           *ready_head;      /* FIFO of pending entries to issue */
    struct diskfs_pending           *ready_tail;
    struct diskfs_pending           *pfree;           /* pending entry free list */
    int                              push_outstanding; /* home writes in flight (push watermark) */

    /* ---------- shared (cross-thread) ---------- */
    struct diskfs_il_record        **handoff;         /* SPSC ring of record* (commit -> push) */
    uint32_t                         handoff_head;    /* atomic: push consumer */
    uint32_t                         handoff_tail;    /* atomic: commit producer */
    uint64_t                         log_head;        /* atomic: commit-written (next free byte) */
    uint64_t                         log_tail;        /* atomic: push-written (trim point) */
    int                              sync;            /* FUA/sync flag (0 in unsafe_async) */

    /* ---------- metrics ---------- */
    int                              redo_inflight_high_water;
    int                              push_outstanding_high_water;
    uint64_t                         log_used_bytes_high_water;
    struct diskfs_intent_log_metrics metrics;
};


struct diskfs_shared {
    struct diskfs_device       *devices;
    char                      **device_paths;    /* for unmount-time persistence */
    int                         num_devices;
    struct diskfs_inode_cache  *inode_cache;
    struct diskfs_block_cache  *block_cache;
    struct diskfs_kv_shard     *kv_shards;
    int                         num_kv_shards;
    int                         num_active_threads;
    uint8_t                     root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                    root_fhlen;
    int                         orphans_scanned;   /* mount-time orphan recovery done */
    uint64_t                    root_inum;         /* for the clean-unmount superblock */
    uint32_t                    root_gen;
    int                         unsafe_async;      /* config opt-in: submit block writes without FUA/sync (no crash safety) */
    int                         noatime;           /* config opt-in: never update atime on read (default: relatime) */
    uint64_t                    mtime_defer_us;    /* coalesce non-FILE_SYNC in-place mtime updates: flush each dirty inode at most once per this many us (0 = disabled, log every write); default 1s */
    int                         mounted;           /* 1 = remounted existing FS (enables inode read-back) */
    uint32_t                    block_cache_blocks; /* total resident block-buffer cap (0 = default) */
    uint32_t                    inode_cache_inodes; /* total resident inode cap (0 = default) */
    int                         block_layout;      /* config opt-in: advertise pNFS block layouts */
    int                         scsi_layout;       /* config opt-in: advertise pNFS SCSI layouts  */
    uint64_t                    fsid;
    struct space_map           *space_map;
    struct diskfs_intent_log    intent_log;
    /* Dedicated reclaim workers: deleted-inode burn-down (and other background
     * maintenance) runs here, off the request workers' hot path. */
    struct diskfs_reclaim      *reclaim;
    uint32_t                    reclaim_threads;   /* config knob (0 = default) */
    /* Inode-generation epoch: every generation is drawn from this global
     * monotonic counter; gen_floor is the durably-persisted bound
     * (reserve-ahead) that no issued generation may reach.  A reused inode
     * block therefore always gets a generation greater than any file handle
     * this filesystem ever issued, so a stale handle can never resolve to
     * the new file.  gen_wait parks allocations that catch up to the floor
     * while an extension write is in flight (effectively never). */
    uint64_t                    gen_next;            /* atomic */
    uint64_t                    gen_floor;           /* atomic; durable bound */
    int                         gen_extend_inflight; /* atomic */
    pthread_mutex_t             gen_lock;            /* guards gen_wait */
    struct diskfs_block_waiter *gen_wait;
    /* Per-(device, AG) park lists for journaling operations stalled behind a
     * runtime AG-log condensation; drained when the condense commits. */
    struct diskfs_ag_wait     **agw;                 /* [device][ag] */
    struct diskfs_metrics       metrics;
    pthread_mutex_t             lock;
};


struct diskfs_thread {
    struct evpl                 *evpl;
    struct diskfs_shared        *shared;
    struct evpl_block_queue    **queue;
    struct evpl_iovec            zero;
    struct evpl_iovec            pad;
    int                          thread_id;
    struct slab_allocator       *allocator;
    struct sm_thread_cache       space_cache;      /* metadata (LOCAL devices); file
                                                    * data uses per-inode space_resv */
    struct diskfs_txn           *txn_free_list;
    struct diskfs_inode_waiter  *waiter_free_list;
    struct diskfs_iq_channel    *iq_channel;
    struct evpl_poll            *cq_poll;          /* drains this worker's IL completion queue every loop iteration */
    uint32_t                     commits_inflight; /* commits handed to the IL not yet completed; pins poll mode while > 0 */
    int                          pending_io;

    /* Commits that found the IL submission queue full park here (FIFO) and are
     * resumed by this worker's CQ doorbell when SQ space frees -- never by
     * re-entering evpl_continue. */
    struct diskfs_txn           *commit_wait_head;
    struct diskfs_txn           *commit_wait_tail;

    /* Cross-thread lock-grant delivery: any thread that releases an inode
     * and grants it to a waiter belonging to this worker enqueues the
     * granted waiter here and rings grant_doorbell, so the continuation
     * runs back on this worker. */
    pthread_mutex_t              grant_lock;
    struct diskfs_inode_waiter  *grant_head;
    struct diskfs_inode_waiter  *grant_tail;
    struct evpl_doorbell         grant_doorbell;

    /* Cross-thread continuation resumption: when a block this worker has
     * waiters on finishes loading (possibly on another worker that issued the
     * read), the ready waiters are queued here.  Same-worker resumptions drain
     * via the deferral (no eventfd); cross-worker ones ring the doorbell. */
    pthread_mutex_t              resume_lock;
    struct diskfs_block_waiter  *resume_head;
    struct diskfs_block_waiter  *resume_tail;
    struct evpl_doorbell         resume_doorbell;
    struct evpl_deferral         resume_deferral;
    struct diskfs_bt_op         *bt_op_free_list;
    struct diskfs_block_waiter  *block_waiter_free_list;

    /* Background reclaim of large deleted inodes: a queue of orphan drain
     * contexts processed one at a time on this worker (each drains its inode's
     * b+tree in bounded batches across transactions). */
    struct diskfs_drain         *drain_head, *drain_tail;
    int                          draining;

    /* Deferred-mtime flusher: this worker owns inode-cache shards where
     * (shard % num_active_threads) == thread_id.  The periodic timer kicks the
     * driver, which flushes eligible dirty inodes one txn at a time (drain
     * style); a flush re-logs one inode's block, coalescing all the writes
     * since it went dirty. */
    struct evpl_timer            mtime_timer;
    int                          mtime_flushing;     /* a flush txn is in flight */
    int                          mtime_flush_all;    /* unmount: ignore the age gate, flush everything */
    uint32_t                     mtime_scan_shard;   /* round-robin cursor over owned shards */

    /* Data-I/O admission control: the per-thread block queues have a bounded
     * submission ring, so a burst of concurrent (or heavily fragmented) reads
     * and writes can overrun it.  Requests that would exceed the in-flight cap
     * park here and are resumed from a block-I/O completion as capacity frees. */
    struct chimera_vfs_request  *io_wait_head, *io_wait_tail;
    struct diskfs_thread_metrics metrics;
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
    int                       last_parent_valid;
    int                       last_parent_ci;
    int                       last_parent_nitems;
    struct diskfs_bt_key      last_parent_key;
    struct diskfs_bt_key      last_parent_next_key;
    uint64_t                  last_parent_child;
    uint64_t                  last_parent_next_child;

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


/* ------------------------------------------------------------------ */
/* Inode cache + logical (read/write) transaction locks                */
/* ------------------------------------------------------------------ */

#define DISKFS_INODE_MODE_FOR_TXN(txn) \
        ((txn)->type == DISKFS_TXN_WRITE ? DISKFS_INODE_LOCK_WRITE \
                                         : DISKFS_INODE_LOCK_READ)


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


/* Mount-time evpl-pump block I/O (defined with the recovery helpers); the
 * mount-time orphan recovery + path-walk fault paths read through it so they
 * reach a VFIO/io_uring/libaio device rather than pread'ing a device path. */
struct diskfs_mount_io;


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


#define DISKFS_SM_JNL(name, thr, t, res, a)                                   \
        struct diskfs_sm_jnl name ## _ctx = { (thr), (t), (res), (a) };       \
        struct sm_journal    name         = {                                 \
            .claim_block                  = diskfs_sm_claim_block,                             \
            .ag_condense                  = diskfs_sm_ag_condense,                             \
            .ag_park                      = diskfs_sm_ag_park,                                 \
            .user                         = &name ## _ctx                                      \
        }


/* Carried across a suspended commit: the pre-commit free-journal flush can
 * park on a cold log block, so commit defers and resumes via this. */
struct diskfs_commit_ctx {
    struct diskfs_txn     *txn;
    diskfs_txn_commit_cb_t cb;
    void                  *private_data;
};


/* Completion context for an in-flight block read. */
struct diskfs_block_load {
    struct diskfs_block  *blk;
    struct diskfs_thread *thread;     /* worker that issued the read */
};


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


/* ------------------------------------------------------------------ */
/* Background drainer: reclaim a large deleted inode incrementally.     */
/*                                                                      */
/* A large inode's tree can't be freed in one transaction (it would     */
/* flood the block-I/O queue), so it is drained in bounded batches: per */
/* transaction, remove up to DISKFS_DRAIN_BATCH of the lowest b+tree    */
/* entries (freeing a file extent's backing data; the remove itself     */
/* reclaims emptied node blocks via merge), commit, repeat -- then a    */
/* final transaction removes the orphan record and retires the inode.   */
/* Generic over entry type (extents, dirents, symlink). Each txn holds  */
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


/* ------------------------------------------------------------------ */
/* Reclaim worker pool: dedicated threads for background space reclaim */
/*                                                                      */
/* Deleted-inode burn-down (and other background maintenance) runs on   */
/* its own small pool of evpl threads, each owning a full diskfs thread */
/* context (block queues, b+tree ops, an intent-log channel), so the    */
/* incremental drains never compete for the request workers' loops.     */
/* Jobs are submitted cross-thread onto a per-worker FIFO + doorbell    */
/* and handed to the worker's existing drain machinery.                 */
/* ------------------------------------------------------------------ */

#define DISKFS_RECLAIM_THREADS_DEFAULT 2


enum diskfs_reclaim_job_type {
    DISKFS_RECLAIM_JOB_DRAIN = 0,   /* burn down a deleted inode */
    DISKFS_RECLAIM_JOB_CONDENSE,    /* condense an AG's allocation log */
};


struct diskfs_reclaim_job {
    enum diskfs_reclaim_job_type type;
    uint64_t                   inum;         /* DRAIN */
    uint32_t                   gen;          /* DRAIN */
    uint32_t                   device_id;    /* CONDENSE */
    uint32_t                   ag_index;     /* CONDENSE */
    struct diskfs_reclaim_job *next;
};


/* Journaling continuations parked behind one AG's condensation. */
struct diskfs_ag_wait {
    pthread_mutex_t             lock;
    struct diskfs_block_waiter *head;
};


struct diskfs_reclaim_worker {
    struct diskfs_shared      *shared;
    struct diskfs_thread      *ctx;        /* this worker's diskfs thread context */
    struct evpl_thread        *thread;
    struct evpl_doorbell       doorbell;
    pthread_mutex_t            lock;
    struct diskfs_reclaim_job *head;
    struct diskfs_reclaim_job *tail;
    int                        condenses;  /* condense jobs in flight here */
    int                        ready;      /* atomic: context constructed */
};


struct diskfs_reclaim {
    struct diskfs_reclaim_worker *workers;
    uint32_t                      nworkers;
    uint32_t                      rr;       /* atomic round-robin submit cursor */
    int                           shutdown; /* atomic: pool tearing down */
};


/* ------------------------------------------------------------------ */
/* AG-log runtime condensation                                         */
/*                                                                      */
/* When an AG's active log slot nears full, the space map parks every   */
/* journaling operation for that AG (diskfs_sm_ag_park) and schedules a */
/* condense job here (diskfs_sm_ag_condense).  The job snapshots the    */
/* AG's free set into the inactive slot -- body blocks first, header    */
/* block last, all FUA, so a crash at any point leaves the old slot     */
/* authoritative -- commits the flip, and re-drives the parked ops into */
/* the fresh slot.                                                      */
/* ------------------------------------------------------------------ */

struct diskfs_condense {
    struct diskfs_reclaim_worker *worker;
    uint32_t                      device_id;
    uint32_t                      ag_index;
    struct evpl_iovec             hdr;       /* slot block 0 */
    struct evpl_iovec             body;      /* remaining payload blocks */
    uint64_t                      slot_off;
    uint64_t                      body_len;
    uint8_t                      *scratch;   /* SM_AG_LOG_SLOT_SIZE image */
};


/* ------------------------------------------------------------------ */
/* Inode-generation epoch                                              */
/* ------------------------------------------------------------------ */

/* Reserve-ahead window persisted into the superblock's gen_floor: a crash
 * restarts the counter at the last durable floor, which is always at or
 * above every generation actually issued. */
#define DISKFS_GEN_RESERVE (1u << 20)

#define DISKFS_GEN_FIRST   2   /* 0 invalid; 1 = format-created inodes */


struct diskfs_gen_extend {
    struct diskfs_thread *thread;
    struct evpl_iovec     iov;
    uint64_t              new_floor;
};


/* ------------------------------------------------------------------ */
/* nlink -> 0 transition: durable orphan record + deferred reclaim     */
/* ------------------------------------------------------------------ */

struct diskfs_inode_orphaned_ctx {
    struct diskfs_thread *thread;
    struct diskfs_inode  *inode;
    void                  (*done)(
        void *priv);
    void                 *priv;
};


/* ------------------------------------------------------------------ */
/* Mount-time orphan recovery: re-enqueue inodes left on the durable    */
/* orphan list by a crash mid-drain (draining is idempotent).           */
/* ------------------------------------------------------------------ */

struct diskfs_orphan_ent {
    uint64_t inum;
    uint32_t gen;
};


/*
 * Fault an inode in from disk on a cache miss (evicted or remounted).  Reads
 * the inode's home block, validates the on-disk dinode against the requested
 * inum/gen, constructs the inode and publishes it WRITE-LOCKED (held by the
 * fault itself), loads the singleton ACL/pNFS records into their in-memory
 * mirrors through the async b+tree path, then releases the hold and re-drives
 * diskfs_inode_acquire (which now hits the cache and grants normally).  The
 * loader-held write lock doubles as single-flight: concurrent acquirers park
 * as ordinary lock waiters instead of racing the record loads, and no writer
 * can modify the tree mid-descent.  The rest of the inode's b+tree blocks
 * load lazily via diskfs_bt_block_get as they are traversed.
 */
struct diskfs_inode_load_ctx {
    struct diskfs_thread       *thread;
    struct diskfs_txn          *txn;
    uint64_t                    inum;
    uint32_t                    gen;
    enum diskfs_inode_lock_mode mode;
    diskfs_inode_cb_t           cb;
    void                       *private_data;
    struct evpl_iovec           iov;
    /* Record-load chain state (valid once the inode is published). */
    struct diskfs_inode        *inode;
    int                         acl_len;
    int                         pnfs_len;
    uint8_t                     acl_rec[DISKFS_ACL_REC_MAX];
    uint8_t                     pnfs_rec[CHIMERA_VFS_PNFS_LAYOUT_MAX];
};


/* Retry context for an inode allocation whose reservation refill parked on a
 * cold AG-log block; the continuation re-drives the whole allocation. */
struct diskfs_inode_alloc_ctx {
    struct diskfs_thread *thread;
    struct diskfs_txn    *txn;
    diskfs_inode_cb_t     cb;
    void                 *private_data;
};


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
    uint64_t                  retire_idx; /* slot in the commit retirement ring */
};


/*
 * A record's scatter-gather image can exceed the block backend's per-request
 * iovec limit (io_uring caps at 64), so the journal write for a large metadata
 * txn is issued in consecutive chunks sharing one redo_ctx; the record is
 * durable only when the last chunk completes.
 */
#define DISKFS_IL_MAX_IOV       64


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
/*
 * Each thread keeps its own block writes pipelined.  The commit thread targets
 * this many outstanding redo writes on the log device; the push thread targets
 * this many outstanding home writes across the data devices.  Both gate
 * submission at the high watermark and resume from a completion at the low
 * watermark.  (The libaio/io_uring submission ring defaults to 256 pending.)
 */
#define DISKFS_COMMIT_WATERMARK 256

#define DISKFS_COMMIT_LOWAT     128

#define DISKFS_PUSH_WATERMARK   256

#define DISKFS_PUSH_LOWAT       128


struct diskfs_recover_rec {
    uint64_t seq;
    uint64_t offset;     /* byte offset within the read-in log image */
};


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


struct diskfs_mtime_flush {
    struct diskfs_thread *thread;
    struct diskfs_inode  *inode;
    struct diskfs_txn    *txn;
};


/*
 * Cookie values for readdir:
 *   0 = start of directory, will return "."
 *   1 = "." was returned, will return ".."
 *   2 = ".." was returned, will return first real entry
 *   3+ = real entry cookie (hash + 3)
 */
#define DISKFS_COOKIE_DOT          1

#define DISKFS_COOKIE_DOTDOT       2

#define DISKFS_COOKIE_FIRST        3


/*
 * Read iovecs are now owned and finalized by the VFS core: it allocates the
 * 4 KiB-aligned buffers on the connection thread (diskfs lacks
 * CAP_READ_PROVIDES_BUFFERS), and chimera_vfs_read_complete() skips the prefix
 * pad and trims to r_length after the request bounces back.  diskfs only fills
 * the provided buffers and reports r_length/r_eof.
 */

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
#define DISKFS_IO_INFLIGHT_CAP     128

#define DISKFS_IO_INFLIGHT_LOWAT   64


/*
 * ALLOCATE (fallocate, non-deallocate): reserve real backing space for every
 * gap in the requested range by allocating blocks and recording them as
 * UNWRITTEN extents.  Such extents read back as zeros (no device I/O) until
 * the first write, which clears the bit and overwrites the reserved blocks in
 * place.  This makes ALLOCATE genuinely reserve space without zero-filling it.
 *
 * Async walk over the inode's extent map (all state in p):
 *   loop_off  = current 4 KiB-aligned offset being examined
 *   loop_pos  = 4 KiB-aligned end of the requested range
 *   loop_left = end of the gap currently being filled
 * A single extent is capped so one space-map reservation can satisfy it; a
 * larger gap is filled by several contiguous extents (coalesced on insert).
 */
#define DISKFS_ALLOCATE_MAX_EXTENT (256ULL << 20)


/* ------------------------------------------------------------------ */
/* pNFS block layout (CHIMERA_VFS_OP_GET_LAYOUT, RFC 5663)             */
/* ------------------------------------------------------------------ */
/*
 * Produce a block layout for [offset, offset+length): walk the inode's extent
 * b+tree, emit one block segment per backed run, and (for an RW layout) allocate
 * + record extents over unbacked gaps so the client can write directly to the
 * remote data volume.  The allocation rides p->txn; diskfs_op_ok commits the
 * txn (making the extent records durable) BEFORE the layout is returned, so a
 * crash can never re-hand-out a block the client is about to write.  Block-mode
 * shares keep file data on REMOTE devices, so blk_vol_offset is the extent's
 * absolute device offset on that volume (the signature region is reserved out
 * of allocation, so no extent overlaps it).  Freshly-allocated / beyond-EOF
 * ranges are INVALID_DATA; ranges within the committed size are READ_WRITE/READ
 * (the size advances only on LAYOUTCOMMIT).
 */
#define DISKFS_LAYOUT_GAP_MAX      (256ULL << 20)

#define DISKFS_LAYOUTIOMODE_RW     2 /* LAYOUTIOMODE4_RW */


static const struct diskfs_bt_key diskfs_acl_key = {
    .type = DISKFS_REC_ACL, .subkey = 0
};

static const struct diskfs_bt_key diskfs_pnfs_key = {
    .type = DISKFS_REC_PNFS, .subkey = 0
};

/* ------------------------------------------------------------------ */
/* Cross-file function declarations                                    */
/* ------------------------------------------------------------------ */

void
diskfs_intent_log_metrics_init(
    struct diskfs_intent_log *il);

void
diskfs_inode_release_one(
    struct diskfs_thread       *thread,
    struct diskfs_inode        *inode,
    enum diskfs_inode_lock_mode mode);

void
diskfs_inode_grant_locked(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    struct diskfs_inode_shard  *shard,
    struct diskfs_inode        *inode,
    uint32_t                    gen,
    enum diskfs_inode_lock_mode mode,
    diskfs_inode_cb_t           cb,
    void                       *private_data);

void
diskfs_inode_acquire(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    uint64_t                    inum,
    uint32_t                    gen,
    enum diskfs_inode_lock_mode mode,
    diskfs_inode_cb_t           cb,
    void                       *private_data);

void
diskfs_inode_acquire_pinned(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    struct diskfs_inode        *inode,
    enum diskfs_inode_lock_mode mode,
    diskfs_inode_cb_t           cb,
    void                       *private_data);

struct diskfs_inode *
diskfs_inode_load_sync(
    struct diskfs_thread   *thread,
    struct diskfs_mount_io *io,
    uint64_t                inum,
    uint32_t                gen,
    int                     allow_orphan);

void
diskfs_block_unpin(
    struct diskfs_thread   *thread,
    struct diskfs_block    *blk,
    enum diskfs_block_state new_state);

void
diskfs_block_release(
    struct diskfs_thread *thread,
    struct diskfs_block  *blk);

void
diskfs_block_cache_create(
    struct diskfs_shared *shared);

void
diskfs_block_cache_destroy(
    struct diskfs_shared *shared);

struct diskfs_block *
diskfs_block_claim(
    struct diskfs_thread *thread,
    uint32_t              device_id,
    uint64_t              device_offset,
    int                   is_new);

void *
diskfs_sm_claim_block(
    void    *user,
    uint32_t device_id,
    uint64_t device_offset,
    int      is_new);

void
diskfs_sm_no_suspend(
    struct diskfs_thread *thread,
    void                 *arg);

void
diskfs_txn_free_space(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    uint32_t              device_id,
    uint64_t              device_offset,
    uint64_t              length);

int
diskfs_txn_flush_free_journals(
    struct diskfs_thread     *thread,
    struct diskfs_txn        *txn,
    struct diskfs_commit_ctx *cctx);

void
diskfs_txn_apply_frees(
    struct diskfs_txn *txn);

void
diskfs_txn_discard_frees(
    struct diskfs_txn *txn);

void
diskfs_block_waiter_dispatch(
    struct diskfs_thread       *waker,
    struct diskfs_block_waiter *w);

void
diskfs_bt_op_resume_cb(
    struct diskfs_thread *thread,
    void                 *arg);

void
diskfs_bt_resume_doorbell_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell);

void
diskfs_bt_resume_deferral_cb(
    struct evpl *evpl,
    void        *private_data);

struct diskfs_block *
diskfs_bt_block_get(
    struct diskfs_bt_op *op,
    uint32_t             device_id,
    uint64_t             device_offset);

void
diskfs_inode_finish_write_pin(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode,
    diskfs_inode_cb_t     cb,
    void                 *private_data);

void
diskfs_txn_pin_inode_block(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode,
    int                   is_new);

void
diskfs_txn_drop_inode_block(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode);

void
diskfs_inode_flush(
    struct diskfs_inode *inode);

void
diskfs_txn_flush_inodes(
    struct diskfs_txn *txn);

void
diskfs_txn_unpin_blocks(
    struct diskfs_txn      *txn,
    enum diskfs_block_state new_state);

int
diskfs_bt_leaf_search(
    void                       *buf,
    uint32_t                    base,
    const struct diskfs_bt_key *key,
    int                        *exact);

int
diskfs_bt_interior_search(
    void                       *buf,
    uint32_t                    base,
    const struct diskfs_bt_key *key);

int
diskfs_bt_lookup_pump(
    struct diskfs_shared       *shared,
    struct diskfs_mount_io     *io,
    void                       *home,
    const struct diskfs_bt_key *key,
    void                       *out,
    uint32_t                    cap);

void *
diskfs_bt_node_for_write(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    uint64_t              bptr);

void
diskfs_bt_leaf_compact(
    void    *buf,
    uint32_t base,
    uint32_t cap);

int
diskfs_bt_rebalance_leaf(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    void                 *pbuf,
    uint32_t              pbase,
    int                   ci);

int
diskfs_bt_rebalance_interior(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    void                 *pbuf,
    uint32_t              pbase,
    int                   ci);

void
diskfs_bt_collapse_root(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode);

void
diskfs_orphan_op_start(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    uint64_t              inum,
    uint32_t              gen,
    int                   remove,
    void (               *done )(void *priv),
    void                 *priv);

void
diskfs_reclaim_create(
    struct diskfs_shared *shared);

void
diskfs_reclaim_destroy(
    struct diskfs_shared *shared);

void
diskfs_sm_ag_condense(
    void    *user,
    uint32_t device_id,
    uint32_t ag_index);

void
diskfs_sm_ag_park(
    void    *user,
    uint32_t device_id,
    uint32_t ag_index);

void
diskfs_gen_extend(
    struct diskfs_thread *thread);

int
diskfs_gen_alloc(
    struct diskfs_thread *thread,
    uint32_t *r_gen,
    void ( *resume )(struct diskfs_thread *, void *),
    void *arg);

void
diskfs_inode_orphaned(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode,
    void (               *done )(void *priv),
    void                 *priv);

void
diskfs_inode_ref_drop(
    struct diskfs_thread *thread,
    struct diskfs_inode  *inode);

void
diskfs_orphan_scan(
    struct diskfs_thread *thread);

int
diskfs_bt_remove_locked(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    struct diskfs_inode        *inode,
    const struct diskfs_bt_key *key);

void
diskfs_bt_run(
    struct diskfs_bt_op *op);

int
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
    void                       *private_data);

int
diskfs_bt_insert_async(
    struct diskfs_bt_op        *op,
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    struct diskfs_inode        *inode,
    const struct diskfs_bt_key *key,
    const void                 *rec,
    uint32_t                    reclen,
    diskfs_bt_cb_t              cb,
    void                       *private_data);

int
diskfs_bt_remove_async(
    struct diskfs_bt_op        *op,
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    struct diskfs_inode        *inode,
    const struct diskfs_bt_key *key,
    diskfs_bt_cb_t              cb,
    void                       *private_data);

int
diskfs_dir_lookup_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_inode  *dir,
    uint64_t              hash,
    void                 *rec_out,
    uint32_t              rec_cap,
    diskfs_bt_cb_t        cb,
    void                 *private_data);

int
diskfs_ext_floor_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_inode  *inode,
    uint64_t              file_offset,
    void                 *rec_out,
    uint32_t              rec_cap,
    diskfs_bt_cb_t        cb,
    void                 *private_data);

int
diskfs_ext_ceil_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_inode  *inode,
    uint64_t              file_offset,
    void                 *rec_out,
    uint32_t              rec_cap,
    diskfs_bt_cb_t        cb,
    void                 *private_data);

int
diskfs_ext_next_async(
    struct diskfs_bt_op  *op,
    struct diskfs_thread *thread,
    struct diskfs_inode  *inode,
    uint64_t              after_file_offset,
    void                 *rec_out,
    uint32_t              rec_cap,
    diskfs_bt_cb_t        cb,
    void                 *private_data);

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
    void                 *private_data);

void
diskfs_inode_cache_recycle_locked(
    struct diskfs_shared      *shared,
    struct diskfs_inode_shard *shard);

void
diskfs_inode_load(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    uint64_t                    inum,
    uint32_t                    gen,
    enum diskfs_inode_lock_mode mode,
    diskfs_inode_cb_t           cb,
    void                       *private_data);

void
diskfs_inode_alloc_resume(
    struct diskfs_thread *thread,
    void                 *arg);

void
diskfs_kv_entry_free(
    struct diskfs_thread   *thread,
    struct diskfs_kv_entry *entry);

void
diskfs_kv_entry_release(
    struct rb_node *node,
    void           *private_data);

void
diskfs_txn_request_complete_cb(
    struct diskfs_txn *txn,
    int                status,
    void              *private_data);

void
diskfs_redo_write_cb(
    struct evpl *evpl,
    int          status,
    void        *private_data);

void
diskfs_iq_process_channel(
    struct diskfs_iq_channel *ch);

void
diskfs_il_poll_exit(
    struct evpl *evpl,
    void        *private_data);

int
diskfs_iq_try_submit(
    struct diskfs_thread  *thread,
    struct diskfs_txn     *txn,
    diskfs_txn_commit_cb_t cb,
    void                  *private_data);

int
diskfs_iq_drain_cq(
    struct diskfs_iq_channel *ch);

void
diskfs_iq_cq_doorbell_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell);

void
diskfs_iq_cq_poll(
    struct evpl *evpl,
    void        *private_data);

void *
diskfs_intent_log_thread_init(
    struct evpl *evpl,
    void        *private_data);

void
diskfs_intent_log_thread_shutdown(
    struct evpl *evpl,
    void        *private_data);

void *
diskfs_il_push_thread_init(
    struct evpl *evpl,
    void        *private_data);

void
diskfs_il_push_thread_shutdown(
    struct evpl *evpl,
    void        *private_data);

void
diskfs_txn_commit_finish(
    struct diskfs_txn     *txn,
    diskfs_txn_commit_cb_t cb,
    void                  *private_data);

void
diskfs_commit_resume(
    struct diskfs_thread *thread,
    void                 *arg);

int
diskfs_recover_rec_cmp(
    const void *a,
    const void *b);

struct diskfs_mount_io *
diskfs_mount_io_open(
    struct diskfs_shared *shared);

void
diskfs_mount_io_close(
    struct diskfs_mount_io *io);

int
diskfs_mount_io_read(
    void    *user,
    uint32_t device_id,
    void    *buf,
    uint64_t length,
    uint64_t offset);

void *
diskfs_init(
    const char                *cfgdata,
    struct prometheus_metrics *metrics);

void
diskfs_bootstrap(
    struct diskfs_thread *thread);

void
diskfs_destroy(
    void *private_data);

void
diskfs_grant_doorbell_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell);

int
diskfs_mtime_any_dirty(
    struct diskfs_thread *thread);

void
diskfs_mtime_flush_kick(
    struct diskfs_thread *thread);

void
diskfs_mtime_flush_timer_cb(
    struct evpl       *evpl,
    struct evpl_timer *timer);

void *
diskfs_thread_init(
    struct evpl *evpl,
    void        *private_data);

void
diskfs_thread_destroy(
    void *private_data);

void
diskfs_acl_decode_into(
    struct chimera_vfs_attrs *attr,
    const uint8_t            *serial,
    int                       len,
    uint32_t                  mode);

int
diskfs_inode_access(
    struct diskfs_thread          *thread,
    struct diskfs_inode           *inode,
    const struct chimera_vfs_cred *cred,
    uint32_t                       requested);

void
diskfs_acl_serial_install(
    struct diskfs_inode *inode,
    const uint8_t       *serial,
    int                  len);

int
diskfs_inherit_acl_async(
    struct diskfs_bt_op      *op,
    struct diskfs_thread     *thread,
    struct diskfs_txn        *txn,
    struct diskfs_inode      *child,
    struct diskfs_inode      *parent,
    const struct chimera_acl *new_acl,
    int                       windows_default,
    diskfs_bt_cb_t            cb,
    void                     *private_data);

void
diskfs_getattr(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_setattr(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_mount(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_umount(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_lookup_at_child_cb(
    struct diskfs_inode *child,
    int                  status,
    void                *private_data);

void
diskfs_lookup_at(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_mkdir_at(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_mknod_at(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_remove_at(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_readdir(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_open_fh_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data);

void
diskfs_open_fh(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_open_at(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_create_unlinked(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_close(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_io_resume_waiters(
    struct diskfs_thread *thread);

void
diskfs_read(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_ext_put(
    struct chimera_vfs_request *request);

void
diskfs_write(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_allocate(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_seek(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_symlink_at(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_readlink(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_rename_at(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_link_at(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_put_key(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_get_key(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_delete_key(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_search_keys(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_get_xattr(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_set_xattr(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_list_xattrs(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_remove_xattr(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_get_layout(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

void
diskfs_commit(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data);

extern struct chimera_vfs_module vfs_diskfs;


/* ------------------------------------------------------------------ */
/* Shared inline helpers                                               */
/* ------------------------------------------------------------------ */

static inline void
diskfs_metric_counter_inc(
    struct prometheus_counter_instance *inst);

static inline void
diskfs_metric_counter_add(
    struct prometheus_counter_instance *inst,
    uint64_t                            value);

static inline void
diskfs_metric_il_block_io(
    struct diskfs_intent_log   *il,
    enum diskfs_metric_io_dir   dir,
    enum diskfs_metric_io_class class,
    uint64_t                    bytes);

static inline size_t
diskfs_metric_io_device_idx(
    uint32_t                    device_id,
    enum diskfs_metric_io_dir   dir,
    enum diskfs_metric_io_class class);

static inline void
diskfs_metric_il_block_io_device(
    struct diskfs_intent_log   *il,
    uint32_t                    device_id,
    enum diskfs_metric_io_dir   dir,
    enum diskfs_metric_io_class class,
    uint64_t                    bytes);

static inline void
diskfs_metric_gauge_set(
    struct prometheus_gauge_instance *inst,
    int64_t                           value);

static inline uint64_t
diskfs_il_used_bytes(
    struct diskfs_intent_log *il);

static inline void
diskfs_il_commit_metrics(
    struct diskfs_intent_log *il);

static inline void
diskfs_il_push_metrics(
    struct diskfs_intent_log *il);

static inline void
diskfs_metric_histogram_sample(
    struct prometheus_histogram_instance *inst,
    uint64_t                              value);

static inline void
diskfs_metric_time_sample(
    struct prometheus_histogram_instance *inst,
    struct prometheus_stopwatch          *sw);

static inline void
diskfs_metric_inode_cache(
    struct diskfs_thread             *thread,
    enum diskfs_metric_inode_cache_op op);

static inline void
diskfs_metric_block_cache(
    struct diskfs_thread             *thread,
    enum diskfs_metric_block_cache_op op);

static inline void
diskfs_metric_mtime(
    struct diskfs_thread       *thread,
    enum diskfs_metric_mtime_op op);

static inline void
diskfs_metric_block_io(
    struct diskfs_thread       *thread,
    enum diskfs_metric_io_dir   dir,
    enum diskfs_metric_io_class class,
    uint64_t                    bytes);

static inline void
diskfs_metric_block_io_device(
    struct diskfs_thread       *thread,
    uint32_t                    device_id,
    enum diskfs_metric_io_dir   dir,
    enum diskfs_metric_io_class class,
    uint64_t                    bytes);

static inline void
diskfs_pending_io_add(
    struct diskfs_thread *thread,
    int                   delta);

static inline uint32_t
diskfs_inum_to_fh(
    struct diskfs_shared *shared,
    uint8_t              *fh,
    uint64_t              inum,
    uint32_t              gen);

static inline void
diskfs_fh_to_inum(
    uint64_t      *inum,
    uint32_t      *gen,
    const uint8_t *fh,
    int            fhlen);

static inline uint64_t
diskfs_inum_hash(
    uint64_t inum);

static inline struct diskfs_inode_shard *
diskfs_inode_shard(
    struct diskfs_shared *shared,
    uint64_t              inum);

static inline struct diskfs_block_waiter *
diskfs_block_waiter_alloc(
    struct diskfs_thread *thread);

static inline void
diskfs_block_waiter_free(
    struct diskfs_thread       *thread,
    struct diskfs_block_waiter *w);

static inline void
diskfs_txn_add_slot(
    struct diskfs_txn          *txn,
    struct diskfs_inode        *inode,
    enum diskfs_inode_lock_mode mode);

static inline void
diskfs_inode_lru_push_tail(
    struct diskfs_inode_shard *shard,
    struct diskfs_inode       *inode);

static inline void
diskfs_inode_lru_unlink(
    struct diskfs_inode_shard *shard,
    struct diskfs_inode       *inode);

static inline int
diskfs_inode_idle(
    const struct diskfs_inode *inode);

static inline void
diskfs_inode_mtime_dirty_locked(
    struct diskfs_inode_shard *shard,
    struct diskfs_inode       *inode,
    uint64_t                   now_ns);

static inline void
diskfs_inode_mtime_unlink_locked(
    struct diskfs_inode_shard *shard,
    struct diskfs_inode       *inode);

static inline void
diskfs_txn_unlock_inode(
    struct diskfs_txn   *txn,
    struct diskfs_inode *inode);

static inline void
diskfs_txn_unlock_all(
    struct diskfs_txn *txn);

static inline void
diskfs_inode_get_inum_async(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    uint64_t              inum,
    uint32_t              gen,
    diskfs_inode_cb_t     cb,
    void                 *private_data);

static inline void
diskfs_inode_get_fh_async(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    const uint8_t        *fh,
    int                   fhlen,
    diskfs_inode_cb_t     cb,
    void                 *private_data);

static inline uint64_t
diskfs_block_hash(
    uint32_t device_id,
    uint64_t device_offset);

static inline struct diskfs_block_shard *
diskfs_block_shard(
    struct diskfs_block_cache *cache,
    uint32_t                   device_id,
    uint64_t                   device_offset);

static inline uint32_t
diskfs_block_bucket(
    uint32_t device_id,
    uint64_t device_offset);

static inline void
diskfs_block_lru_push_tail(
    struct diskfs_block_shard *shard,
    struct diskfs_block       *blk);

static inline struct diskfs_block *
diskfs_block_lookup_locked(
    struct diskfs_block_shard *shard,
    uint32_t                   bucket,
    uint32_t                   device_id,
    uint64_t                   device_offset);

static inline void
diskfs_txn_add_block(
    struct diskfs_txn   *txn,
    struct diskfs_block *block);

static inline struct diskfs_bt_op *
diskfs_bt_op_alloc(
    struct diskfs_thread *thread);

static inline void
diskfs_bt_op_free(
    struct diskfs_thread *thread,
    struct diskfs_bt_op  *op);

static inline struct diskfs_bt_node_hdr *
diskfs_bt_hdr(
    void    *buf,
    uint32_t base);

static inline struct diskfs_bt_islot *
diskfs_bt_islots(
    void    *buf,
    uint32_t base);

static inline struct diskfs_bt_lslot *
diskfs_bt_lslots(
    void    *buf,
    uint32_t base);

static inline void
diskfs_bt_node_init(
    void    *buf,
    uint32_t base,
    uint32_t capacity,
    uint16_t level);

static inline int
diskfs_bt_leaf_underflow(
    void    *buf,
    uint32_t base);

static inline int
diskfs_bt_interior_underflow(
    void    *buf,
    uint32_t base);

static inline void
diskfs_bt_complete(
    struct diskfs_bt_op *op,
    int                  result);

static inline struct diskfs_bt_key
diskfs_extent_key(
    uint64_t file_offset);

static inline int
diskfs_ext_from_op(
    struct diskfs_bt_op  *op,
    int                   result,
    struct diskfs_extent *out);

static inline struct diskfs_inode *
diskfs_inode_struct_new(
    uint64_t inum);

static inline void
diskfs_inode_struct_free(
    struct diskfs_inode *inode);

static inline void
diskfs_inode_cache_insert(
    struct diskfs_shared *shared,
    struct diskfs_inode  *inode);

static inline void
diskfs_inode_alloc_async(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    diskfs_inode_cb_t     cb,
    void                 *private_data);

static inline void
diskfs_inode_free(
    struct diskfs_thread *thread,
    struct diskfs_inode  *inode);

static inline struct diskfs_kv_entry *
diskfs_kv_entry_alloc(
    struct diskfs_thread *thread,
    uint64_t              hash,
    const void           *key,
    uint32_t              key_len,
    const void           *value,
    uint32_t              value_len);

static inline int
diskfs_inode_alloc_space(
    struct diskfs_thread *thread,
    struct diskfs_txn *txn,
    struct diskfs_inode *inode,
    int64_t desired_size,
    uint64_t floor,
    uint64_t *r_device_id,
    uint64_t *r_device_offset,
    void ( *resume )(struct diskfs_thread *, void *),
    void *resume_arg);

static inline void
diskfs_thread_free_space(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    uint32_t              device_id,
    uint64_t              device_offset,
    uint64_t              length);

static inline void
diskfs_inode_return_reservation(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode);

static inline struct diskfs_txn *
diskfs_txn_begin(
    struct diskfs_thread *thread,
    enum diskfs_txn_type  type);

static inline void
diskfs_txn_release(
    struct diskfs_txn *txn);

static inline void
diskfs_txn_abort(
    struct diskfs_txn *txn);

static inline void
diskfs_op_fail(
    struct chimera_vfs_request *request,
    struct diskfs_txn          *txn,
    int                         status);

static inline void
diskfs_op_ok(
    struct chimera_vfs_request *request,
    struct diskfs_txn          *txn);

static inline uint64_t
diskfs_il_hdr_len(
    uint32_t nblocks);

static inline void
diskfs_txn_commit(
    struct diskfs_txn     *txn,
    diskfs_txn_commit_cb_t cb,
    void                  *private_data);

static inline void
diskfs_map_attrs(
    struct diskfs_thread     *thread,
    struct chimera_vfs_attrs *attr,
    struct diskfs_inode      *inode);

static inline void
diskfs_apply_attrs(
    struct diskfs_inode      *inode,
    struct chimera_vfs_attrs *attr);


static inline void
diskfs_metric_counter_inc(struct prometheus_counter_instance *inst)
{
    if (inst) {
        prometheus_counter_increment(inst);
    }
} /* diskfs_metric_counter_inc */


static inline void
diskfs_metric_counter_add(
    struct prometheus_counter_instance *inst,
    uint64_t                            value)
{
    if (inst) {
        prometheus_counter_add(inst, value);
    }
} /* diskfs_metric_counter_add */


static inline void
diskfs_metric_il_block_io(
    struct diskfs_intent_log   *il,
    enum diskfs_metric_io_dir   dir,
    enum diskfs_metric_io_class class,
    uint64_t                    bytes)
{
    diskfs_metric_counter_inc(il->metrics.block_io_ops[dir][class]);
    diskfs_metric_counter_add(il->metrics.block_io_bytes[dir][class], bytes);
} /* diskfs_metric_il_block_io */


static inline size_t
diskfs_metric_io_device_idx(
    uint32_t                    device_id,
    enum diskfs_metric_io_dir   dir,
    enum diskfs_metric_io_class class)
{
    return ((size_t) device_id * DISKFS_METRIC_IO_NUM_DIRS + dir) *
           DISKFS_METRIC_IO_NUM_CLASSES + class;
} /* diskfs_metric_io_device_idx */


static inline void
diskfs_metric_il_block_io_device(
    struct diskfs_intent_log   *il,
    uint32_t                    device_id,
    enum diskfs_metric_io_dir   dir,
    enum diskfs_metric_io_class class,
    uint64_t                    bytes)
{
    struct diskfs_shared *shared = container_of(il, struct diskfs_shared, intent_log);
    size_t                idx;

    if (!il->metrics.block_io_device_ops || device_id >= (uint32_t) shared->num_devices) {
        return;
    }
    idx = diskfs_metric_io_device_idx(device_id, dir, class);
    diskfs_metric_counter_inc(il->metrics.block_io_device_ops[idx]);
    diskfs_metric_counter_add(il->metrics.block_io_device_bytes[idx], bytes);
} /* diskfs_metric_il_block_io_device */


static inline void
diskfs_metric_gauge_set(
    struct prometheus_gauge_instance *inst,
    int64_t                           value)
{
    if (inst) {
        prometheus_gauge_set(inst, value);
    }
} /* diskfs_metric_gauge_set */


static inline uint64_t
diskfs_il_used_bytes(struct diskfs_intent_log *il)
{
    uint64_t head = __atomic_load_n(&il->log_head, __ATOMIC_RELAXED);
    uint64_t tail = __atomic_load_n(&il->log_tail, __ATOMIC_RELAXED);

    if (head >= tail) {
        return head - tail;
    }
    return SM_INTENT_LOG_SIZE - (tail - head);
} /* diskfs_il_used_bytes */


/* Commit-thread metrics: redo write depth + registered channels. */
static inline void
diskfs_il_commit_metrics(struct diskfs_intent_log *il)
{
    if (il->redo_inflight > il->redo_inflight_high_water) {
        il->redo_inflight_high_water = il->redo_inflight;
    }
    diskfs_metric_gauge_set(il->metrics.redo_inflight, il->redo_inflight);
    diskfs_metric_gauge_set(il->metrics.registered_channels, il->num_channels);
    diskfs_metric_gauge_set(il->metrics.redo_inflight_high_water,
                            il->redo_inflight_high_water);
} /* diskfs_il_commit_metrics */


/* Push-thread metrics: home write depth + log occupancy. */
static inline void
diskfs_il_push_metrics(struct diskfs_intent_log *il)
{
    uint64_t used = diskfs_il_used_bytes(il);

    if (il->push_outstanding > il->push_outstanding_high_water) {
        il->push_outstanding_high_water = il->push_outstanding;
    }
    if (used > il->log_used_bytes_high_water) {
        il->log_used_bytes_high_water = used;
    }
    diskfs_metric_gauge_set(il->metrics.push_outstanding, il->push_outstanding);
    diskfs_metric_gauge_set(il->metrics.log_used_bytes, used);
    diskfs_metric_gauge_set(il->metrics.push_outstanding_high_water,
                            il->push_outstanding_high_water);
    diskfs_metric_gauge_set(il->metrics.log_used_bytes_high_water,
                            il->log_used_bytes_high_water);
} /* diskfs_il_push_metrics */


static inline void
diskfs_metric_histogram_sample(
    struct prometheus_histogram_instance *inst,
    uint64_t                              value)
{
    if (inst) {
        prometheus_histogram_sample(inst, value ? (int64_t) value : 1);
    }
} /* diskfs_metric_histogram_sample */


static inline void
diskfs_metric_time_sample(
    struct prometheus_histogram_instance *inst,
    struct prometheus_stopwatch          *sw)
{
    if (inst) {
        prometheus_time_histogram_sample(inst, sw);
    }
} /* diskfs_metric_time_sample */


static inline void
diskfs_metric_inode_cache(
    struct diskfs_thread             *thread,
    enum diskfs_metric_inode_cache_op op)
{
    if (thread) {
        diskfs_metric_counter_inc(thread->metrics.inode_cache[op]);
    }
} /* diskfs_metric_inode_cache */


static inline void
diskfs_metric_block_cache(
    struct diskfs_thread             *thread,
    enum diskfs_metric_block_cache_op op)
{
    if (thread) {
        diskfs_metric_counter_inc(thread->metrics.block_cache[op]);
    }
} /* diskfs_metric_block_cache */


static inline void
diskfs_metric_mtime(
    struct diskfs_thread       *thread,
    enum diskfs_metric_mtime_op op)
{
    if (thread) {
        diskfs_metric_counter_inc(thread->metrics.mtime[op]);
    }
} /* diskfs_metric_mtime */


static inline void
diskfs_metric_block_io(
    struct diskfs_thread       *thread,
    enum diskfs_metric_io_dir   dir,
    enum diskfs_metric_io_class class,
    uint64_t                    bytes)
{
    if (thread) {
        diskfs_metric_counter_inc(thread->metrics.block_io_ops[dir][class]);
        diskfs_metric_counter_add(thread->metrics.block_io_bytes[dir][class], bytes);
    }
} /* diskfs_metric_block_io */


static inline void
diskfs_metric_block_io_device(
    struct diskfs_thread       *thread,
    uint32_t                    device_id,
    enum diskfs_metric_io_dir   dir,
    enum diskfs_metric_io_class class,
    uint64_t                    bytes)
{
    size_t idx;

    if (!thread || !thread->metrics.block_io_device_ops ||
        device_id >= (uint32_t) thread->shared->num_devices) {
        return;
    }
    idx = diskfs_metric_io_device_idx(device_id, dir, class);
    diskfs_metric_counter_inc(thread->metrics.block_io_device_ops[idx]);
    diskfs_metric_counter_add(thread->metrics.block_io_device_bytes[idx], bytes);
} /* diskfs_metric_block_io_device */


static inline void
diskfs_pending_io_add(
    struct diskfs_thread *thread,
    int                   delta)
{
    thread->pending_io += delta;
    diskfs_metric_gauge_set(thread->metrics.pending_io, thread->pending_io);
} /* diskfs_pending_io_add */


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


/*
 * Mix the inum before selecting the shard.  inums are
 * (disk<<56 | ag<<32 | block_idx) where block_idx is a small, often dense or
 * stride-correlated integer, so the raw low bits have little entropy and
 * cluster inodes onto a handful of the 256 shards -- concentrating contention
 * on those few shard mutexes.  Use the SplitMix64 finalizer to spread them
 * evenly.  (This is a pure-integer mix rather than hashing &inum through XXH3:
 * the byte-wise XXH3 read of a stack scalar trips -Werror=maybe-uninitialized
 * when this is deeply inlined on some toolchains, and an integer finalizer is
 * both immune to that and faster.)
 */
static inline uint64_t
diskfs_inum_hash(uint64_t inum)
{
    uint64_t x = inum;

    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    x =  x ^ (x >> 31);
    return x;
} /* diskfs_inum_hash */


static inline struct diskfs_inode_shard *
diskfs_inode_shard(
    struct diskfs_shared *shared,
    uint64_t              inum)
{
    return &shared->inode_cache->shards[diskfs_inum_hash(inum) & DISKFS_INODE_CACHE_MASK];
} /* diskfs_inode_shard */


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


/*
 * Caller holds the inode's shard lock.  Queue a freshly-dirtied inode on the
 * shard's deferred-mtime list, taking a refcnt pin so it stays resident until
 * the flusher logs it.  Idempotent: a re-dirty of an already-queued inode is a
 * no-op (the flusher serializes whatever in-memory mtime is current at flush
 * time, coalescing every write since it went dirty).  now_ns is the realtime
 * clock the write already read for mtime.
 */
static inline void
diskfs_inode_mtime_dirty_locked(
    struct diskfs_inode_shard *shard,
    struct diskfs_inode       *inode,
    uint64_t                   now_ns)
{
    if (inode->mtime_dirty) {
        return;
    }
    inode->mtime_dirty       = 1;
    inode->mtime_dirty_since = now_ns;
    inode->refcnt++;                          /* keep resident until flushed */
    diskfs_inode_lru_unlink(shard, inode);    /* refcnt > 1: no longer idle */

    inode->mdirty_prev = shard->mdirty_tail;
    inode->mdirty_next = NULL;
    if (shard->mdirty_tail) {
        shard->mdirty_tail->mdirty_next = inode;
    } else {
        shard->mdirty_head = inode;
    }
    shard->mdirty_tail = inode;
} /* diskfs_inode_mtime_dirty_locked */


/*
 * Caller holds the shard lock.  Unlink from the deferred-mtime list and clear
 * the flag.  Does NOT drop the refcnt pin -- the flusher does that after the
 * flush commits (a concurrent write that re-dirties between this unlink and the
 * flush commit simply re-queues with a fresh pin).
 */
static inline void
diskfs_inode_mtime_unlink_locked(
    struct diskfs_inode_shard *shard,
    struct diskfs_inode       *inode)
{
    if (!inode->mtime_dirty) {
        return;
    }
    inode->mtime_dirty = 0;
    if (inode->mdirty_prev) {
        inode->mdirty_prev->mdirty_next = inode->mdirty_next;
    } else {
        shard->mdirty_head = inode->mdirty_next;
    }
    if (inode->mdirty_next) {
        inode->mdirty_next->mdirty_prev = inode->mdirty_prev;
    } else {
        shard->mdirty_tail = inode->mdirty_prev;
    }
    inode->mdirty_prev = inode->mdirty_next = NULL;
} /* diskfs_inode_mtime_unlink_locked */


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


/* ------------------------------------------------------------------ */
/* File extents over the inode b+tree                                  */
/* ------------------------------------------------------------------ */

static inline struct diskfs_bt_key
diskfs_extent_key(uint64_t file_offset)
{
    struct diskfs_bt_key k = { .type = DISKFS_REC_EXTENT, .subkey = file_offset };

    return k;
} /* diskfs_extent_key */


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
    out->flags         = rec->flags;
    return 1;
} /* diskfs_ext_from_op */



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


/* Free an inode struct and the record mirrors riding on it. */
static inline void
diskfs_inode_struct_free(struct diskfs_inode *inode)
{
    free(inode->acl_serial);
    free(inode->pnfs_blob);
    free(inode);
} /* diskfs_inode_struct_free */


static inline void
diskfs_inode_cache_insert(
    struct diskfs_shared *shared,
    struct diskfs_inode  *inode)
{
    struct diskfs_inode_shard *shard = diskfs_inode_shard(shared, inode->inum);
    struct diskfs_inode       *stale;

    pthread_mutex_lock(&shard->lock);

    /* A reallocated inum can collide with the previous life's retired struct
     * (kept cached through its background drain).  By the time the space map
     * re-issued the home block, the drain fully retired it -- dead, unlocked,
     * unreferenced -- so it can be replaced; any straggling lookup by the old
     * generation gets ENOENT from the new struct's gen check. */
    rb_tree_query_exact(&shard->inodes, inode->inum, inum, stale);
    if (stale) {
        chimera_diskfs_abort_if(stale->nlink != 0 || stale->refcnt != 0 ||
                                stale->writer || stale->readers ||
                                stale->wait_head,
                                "inum %lu reallocated while previous life "
                                "is still live", inode->inum);
        diskfs_inode_lru_unlink(shard, stale);
        diskfs_inode_mtime_unlink_locked(shard, stale);
        rb_tree_remove(&shard->inodes, &stale->node);
        shard->ninodes--;
        diskfs_inode_struct_free(stale);
    }

    diskfs_inode_cache_recycle_locked(shared, shard);
    rb_tree_insert(&shard->inodes, inum, inode);
    shard->ninodes++;
    pthread_mutex_unlock(&shard->lock);
} /* diskfs_inode_cache_insert */


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
    uint32_t                       gen;
    int                            rc;

    /* The reservation refill journals and may park on a cold log block; carry
     * the retry context so the resumed allocation re-drives from the top. */
    actx               = malloc(sizeof(*actx));
    actx->thread       = thread;
    actx->txn          = txn;
    actx->cb           = cb;
    actx->private_data = private_data;

    /* Draw the generation before the space draw so a (rare) park on the
    * generation floor can re-drive without having allocated a block. */
    if (diskfs_gen_alloc(thread, &gen, diskfs_inode_alloc_resume,
                         actx) == SM_AGAIN) {
        return;     /* parked; diskfs_inode_alloc_resume re-runs (owns actx) */
    }

    DISKFS_SM_JNL(jnl, thread, txn, diskfs_inode_alloc_resume, actx);
    rc = space_map_alloc(shared->space_map, &thread->space_cache, &jnl,
                         SM_DEV_LOCAL, SM_BLOCK_SIZE, SM_RESERVATION_MIN,
                         &device_id, &device_offset);
    if (rc == SM_AGAIN) {
        return;     /* parked; diskfs_inode_alloc_resume re-runs (owns actx) */
    }
    free(actx);
    if (unlikely(rc != 0)) {
        cb(NULL, CHIMERA_VFS_ENOSPC, private_data);
        return;
    }

    inum       = sm_inum_from_device_offset(shared->space_map, device_id, device_offset);
    inode      = diskfs_inode_struct_new(inum);
    inode->gen = gen;

    /* New dirty inode: write-locked by this (write) txn from birth. */
    inode->writer = 1;

    diskfs_inode_cache_insert(shared, inode);
    diskfs_metric_inode_cache(thread, DISKFS_METRIC_INODE_CACHE_INSERT);
    diskfs_txn_add_slot(txn, inode, DISKFS_INODE_LOCK_WRITE);

    /* Claim and pin the inode's freshly-allocated home block. */
    diskfs_txn_pin_inode_block(thread, txn, inode, 1);

    cb(inode, CHIMERA_VFS_OK, private_data);
} /* diskfs_inode_alloc_async */


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

    /* The per-open-file data reservation must already have been returned at the
     * last close (or this retire's caller) -- catch a leak. */
    chimera_diskfs_abort_if(inode->space_resv.valid,
                            "inode %lu freed with a live space reservation",
                            inode->inum);

    inode->gen++;
    inode->refcnt = 0;
} /* diskfs_inode_free */



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


/*
 * Allocate file-data backing for `inode` from its own per-open-file reservation
 * (inode->space_resv) rather than a shared per-thread cache, so a file's blocks
 * lay out sequentially and the unused tail is returned when the file is closed
 * (not stranded per-thread).  `floor` is the over-reserve minimum: writes pass
 * SM_RESERVATION_MIN (reserve 1 MiB or the write, whichever is larger, and keep
 * the rest for the next write); fallocate passes 0 (exact, no retained tail).
 * Must be called with the inode write-locked (the data path holds it).  Returns
 * SM_AGAIN on a journal-block miss (caller's resume re-drives), ENOSPC, or 0.
 */
static inline int
diskfs_inode_alloc_space(
    struct diskfs_thread *thread,
    struct diskfs_txn *txn,
    struct diskfs_inode *inode,
    int64_t desired_size,
    uint64_t floor,
    uint64_t *r_device_id,
    uint64_t *r_device_offset,
    void ( *resume )(struct diskfs_thread *, void *),
    void *resume_arg)
{
    uint32_t          dev_id;
    int               rc;
    struct space_map *sm = thread->shared->space_map;

    /* File data goes to REMOTE (pNFS data) devices in block mode, LOCAL
     * otherwise; metadata keeps its own per-thread reservation, so the two
     * classes never collide. */
    uint32_t          role = space_map_has_remote(sm) ? SM_DEV_REMOTE : SM_DEV_LOCAL;

    DISKFS_SM_JNL(jnl, thread, txn, resume, resume_arg);
    rc = space_map_alloc(sm, &inode->space_resv, &jnl, role,
                         (uint64_t) desired_size, floor, &dev_id, r_device_offset);

    if (rc == SM_AGAIN) {
        return SM_AGAIN;        /* parked; caller's resume re-drives */
    }
    if (rc != 0) {
        return CHIMERA_VFS_ENOSPC;
    }

    *r_device_id = dev_id;
    return 0;
} /* diskfs_inode_alloc_space */


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


/*
 * Return this file's unused data-space reservation tail to the space map when
 * its last open handle closes (or it is otherwise torn down with a live txn).
 * Uses the pending-free path (journaled at pre-commit, applied on commit), so it
 * never has to suspend inside the caller, and clears the reservation so it is
 * returned exactly once.  Must run under the inode write lock.
 */
static inline void
diskfs_inode_return_reservation(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode)
{
    struct sm_thread_cache *resv = &inode->space_resv;

    if (!resv->valid || resv->length == 0) {
        resv->valid  = 0;
        resv->length = 0;
        return;
    }

    diskfs_thread_free_space(thread, txn, resv->device_id, resv->offset,
                             resv->length);
    resv->valid  = 0;
    resv->length = 0;
} /* diskfs_inode_return_reservation */


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

    /* A write txn with no dirty blocks and no deferred frees has nothing to
     * make durable -- e.g. an in-place overwrite whose only inode change was a
     * deferred mtime/ctime bump (logged later by the coalescing flusher), or
     * whose data went straight to the device.  Unlock inline like a read txn;
     * routing it through the intent log would write a header-only record per
     * write and defeat the deferral. */
    if (!txn->blocks && !txn->pending_frees) {
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

        /* diskfs persists DOS attributes natively (in-memory + on-disk
         * dinode), so report them alongside stat (matching memfs/cairn). */
        attr->va_set_mask      |= CHIMERA_VFS_ATTR_DOS_ATTRIBUTES;
        attr->va_dos_attributes = inode->dos_attributes;
    }

    /* Birth time (SMB create time) is tracked natively but lives outside
     * MASK_STAT, so map it only when explicitly requested. */
    if (attr->va_req_mask & CHIMERA_VFS_ATTR_BTIME) {
        attr->va_set_mask     |= CHIMERA_VFS_ATTR_BTIME;
        attr->va_btime.tv_sec  = inode->btime_sec;
        attr->va_btime.tv_nsec = inode->btime_nsec;
    }

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_FSID) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_FSID;
        attr->va_fsid      = shared->fsid;
    }

    /*
     * Opaque pNFS layout state (flex-files), persisted as the single
     * DISKFS_REC_PNFS record in this inode's b+tree and mirrored into
     * inode->pnfs_blob at fault/setattr time, so mapping it never reads the
     * tree (which could suspend on an evicted node -- this helper has no
     * continuation).
     */
    if ((attr->va_req_mask & CHIMERA_VFS_ATTR_PNFS_LAYOUT) && inode->pnfs_blob) {
        memcpy(attr->va_pnfs, inode->pnfs_blob, inode->pnfs_blob_len);
        attr->va_set_mask |= CHIMERA_VFS_ATTR_PNFS_LAYOUT;
        attr->va_pnfs_len  = inode->pnfs_blob_len;
    }

    /*
     * Native ACL: the in-memory mirror of the single DISKFS_REC_ACL record
     * (or, when absent, an ACL synthesised from the mode) so SMB/NFS callers
     * always see one.
     */
    if (attr->va_req_mask & CHIMERA_VFS_ATTR_ACL) {
        diskfs_acl_decode_into(attr, inode->acl_serial,
                               inode->acl_serial ? (int) inode->acl_serial_len : -1,
                               inode->mode);
    }

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_MASK_STATFS) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_MASK_STATFS;
        /* Free/used are derived from the space map's live per-AG free counts
         * (cold path) rather than a running counter the alloc/free fast path
         * would have to maintain. */
        attr->va_fs_space_total = space_map_usable_capacity(shared->space_map);
        attr->va_fs_space_free  = space_map_free_bytes(shared->space_map);
        if (attr->va_fs_space_free > attr->va_fs_space_total) {
            attr->va_fs_space_free = attr->va_fs_space_total;
        }
        attr->va_fs_space_used  = attr->va_fs_space_total - attr->va_fs_space_free;
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
        struct timespec t;
        attr->va_set_mask |= CHIMERA_VFS_ATTR_ATIME;
        if (chimera_vfs_resolve_set_time(&attr->va_atime, &now, &t)) {
            inode->atime_sec  = t.tv_sec;
            inode->atime_nsec = t.tv_nsec;
        }
    }

    if (set_mask & CHIMERA_VFS_ATTR_MTIME) {
        struct timespec t;
        attr->va_set_mask |= CHIMERA_VFS_ATTR_MTIME;
        if (chimera_vfs_resolve_set_time(&attr->va_mtime, &now, &t)) {
            inode->mtime_sec  = t.tv_sec;
            inode->mtime_nsec = t.tv_nsec;
        }
    }

    if (set_mask & CHIMERA_VFS_ATTR_BTIME) {
        struct timespec t;
        attr->va_set_mask |= CHIMERA_VFS_ATTR_BTIME;
        if (chimera_vfs_resolve_set_time(&attr->va_btime, &now, &t)) {
            inode->btime_sec  = t.tv_sec;
            inode->btime_nsec = t.tv_nsec;
        }
    }

    if (set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) {
        attr->va_set_mask    |= CHIMERA_VFS_ATTR_DOS_ATTRIBUTES;
        inode->dos_attributes = attr->va_dos_attributes;
    }

    /* ctime: round-trip a caller-supplied change_time (SMB FileBasicInformation
     * SetInfo) or preserve it on TIME_OMIT; otherwise stamp it with now for the
     * implicit metadata change.  See memfs_apply_attrs() for the rationale. */
    if (set_mask & CHIMERA_VFS_ATTR_CTIME) {
        struct timespec t;
        attr->va_set_mask |= CHIMERA_VFS_ATTR_CTIME;
        if (chimera_vfs_resolve_set_time(&attr->va_ctime, &now, &t)) {
            inode->ctime_sec  = t.tv_sec;
            inode->ctime_nsec = t.tv_nsec;
        }
    } else {
        inode->ctime_sec  = now.tv_sec;
        inode->ctime_nsec = now.tv_nsec;
    }

} /* diskfs_apply_attrs */
