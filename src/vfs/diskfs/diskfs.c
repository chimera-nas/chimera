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

static const char *diskfs_metric_inode_cache_op_names[] = {
    "hit",
    "miss",
    "stale",
    "load",
    "insert",
    "wait",
};

static const char *diskfs_metric_block_cache_op_names[] = {
    "hit",
    "miss",
    "new",
    "wait",
    "cow",
    "recycle",
};

static const char *diskfs_metric_mtime_op_names[] = {
    "deferred",
    "flushed",
    "skip_not_inplace",
    "skip_size_grew",
    "skip_filesync",
};

static const char *diskfs_metric_io_dir_names[] = {
    "read",
    "write",
};

static const char *diskfs_metric_io_class_names[] = {
    "data",
    "rmw",
    "inode",
    "btree",
    "metadata",
    "intent_log",
    "tail_push",
};

static const char *diskfs_metric_txn_phase_names[] = {
    "queue_to_submit",
    "submit_to_durable",
    "queue_to_durable",
    "durable_to_callback",
    "queue_to_callback",
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
#define DISKFS_XATTR_REC_MAX \
        (DISKFS_BT_ROOT_CAP - sizeof(struct diskfs_bt_node_hdr) - sizeof(struct diskfs_bt_lslot))

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
    int                        unsafe_async;       /* config opt-in: submit block writes without FUA/sync (no crash safety) */
    int                        noatime;            /* config opt-in: never update atime on read (default: relatime) */
    uint64_t                   mtime_defer_us;     /* coalesce non-FILE_SYNC in-place mtime updates: flush each dirty inode at most once per this many us (0 = disabled, log every write); default 1s */
    int                        mounted;            /* 1 = remounted existing FS (enables inode read-back) */
    uint32_t                   block_cache_blocks; /* total resident block-buffer cap (0 = default) */
    uint32_t                   inode_cache_inodes; /* total resident inode cap (0 = default) */
    int                        block_layout;       /* config opt-in: advertise pNFS block layouts */
    int                        scsi_layout;        /* config opt-in: advertise pNFS SCSI layouts  */
    uint64_t                   fsid;
    struct space_map          *space_map;
    struct diskfs_intent_log   intent_log;
    struct diskfs_metrics      metrics;
    pthread_mutex_t            lock;
};

struct diskfs_thread {
    struct evpl                 *evpl;
    struct diskfs_shared        *shared;
    struct evpl_block_queue    **queue;
    struct evpl_iovec            zero;
    struct evpl_iovec            pad;
    int                          thread_id;
    struct slab_allocator       *allocator;
    struct sm_thread_cache       space_cache;      /* metadata (LOCAL devices) */
    struct sm_thread_cache       data_space_cache; /* block-mode file data (REMOTE devices) */
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

static void
diskfs_metrics_init(
    struct diskfs_shared      *shared,
    struct prometheus_metrics *metrics)
{
    struct diskfs_metrics *m               = &shared->metrics;
    static const char     *op_label[]      = { "op" };
    static const char     *phase_label[]   = { "phase" };
    static const char     *io_labels[]     = { "direction", "class" };
    static const char     *io_dev_labels[] = { "direction", "class", "device" };
    static const char     *intent_label[]  = { "name" };
    static const char     *txn_label[]     = { "name" };
    static const char     *txn_names[]     = { "write", "blocks", "bytes" };
    static const char     *intent_names[]  = {
        "redo_inflight",
        "iocbs_inflight",
        "push_outstanding",
        "log_used_bytes",
        "registered_channels",
        "redo_inflight_high_water",
        "iocbs_inflight_high_water",
        "push_outstanding_high_water",
        "log_used_bytes_high_water",
    };

    if (!metrics) {
        return;
    }

    m->metrics     = metrics;
    m->num_devices = shared->num_devices;
    m->inode_cache = prometheus_metrics_create_counter(
        metrics, "chimera_diskfs_inode_cache",
        "Diskfs inode cache events");
    m->block_cache = prometheus_metrics_create_counter(
        metrics, "chimera_diskfs_block_cache",
        "Diskfs block cache events");
    m->mtime = prometheus_metrics_create_counter(
        metrics, "chimera_diskfs_mtime",
        "Diskfs deferred-mtime accounting (deferred/flushed/skip reasons)");
    m->block_io_ops = prometheus_metrics_create_counter(
        metrics, "chimera_diskfs_block_io_ops",
        "Diskfs classified block I/O submissions");
    m->block_io_bytes = prometheus_metrics_create_counter(
        metrics, "chimera_diskfs_block_io_bytes",
        "Diskfs classified block I/O submitted bytes");
    m->block_io_device_ops = prometheus_metrics_create_counter(
        metrics, "chimera_diskfs_block_io_device_ops",
        "Diskfs classified block I/O submissions by device");
    m->block_io_device_bytes = prometheus_metrics_create_counter(
        metrics, "chimera_diskfs_block_io_device_bytes",
        "Diskfs classified block I/O submitted bytes by device");
    m->txn = prometheus_metrics_create_counter(
        metrics, "chimera_diskfs_txn",
        "Diskfs transaction counters");
    m->txn_blocks = prometheus_metrics_create_histogram_exponential(
        metrics, "chimera_diskfs_txn_blocks",
        "Diskfs dirty blocks per transaction", 24);
    m->txn_bytes = prometheus_metrics_create_histogram_exponential(
        metrics, "chimera_diskfs_txn_bytes",
        "Diskfs dirty bytes per transaction", 32);
    m->txn_latency = prometheus_metrics_create_histogram_time(
        metrics, "chimera_diskfs_txn_latency_nanoseconds",
        "Diskfs transaction latency in nanoseconds", 34);
    m->pending_io = prometheus_metrics_create_gauge(
        metrics, "chimera_diskfs_pending_io",
        "Diskfs outstanding worker block I/O");
    m->intent_log = prometheus_metrics_create_gauge(
        metrics, "chimera_diskfs_intent_log",
        "Diskfs intent-log pressure gauges");
    for (int i = 0; i < DISKFS_METRIC_INODE_CACHE_NUM; i++) {
        m->inode_cache_series[i] = prometheus_counter_create_series(
            m->inode_cache, op_label, &diskfs_metric_inode_cache_op_names[i], 1);
    }
    for (int i = 0; i < DISKFS_METRIC_BLOCK_CACHE_NUM; i++) {
        m->block_cache_series[i] = prometheus_counter_create_series(
            m->block_cache, op_label, &diskfs_metric_block_cache_op_names[i], 1);
    }
    for (int i = 0; i < DISKFS_METRIC_MTIME_NUM; i++) {
        m->mtime_series[i] = prometheus_counter_create_series(
            m->mtime, op_label, &diskfs_metric_mtime_op_names[i], 1);
    }
    for (int d = 0; d < DISKFS_METRIC_IO_NUM_DIRS; d++) {
        for (int c = 0; c < DISKFS_METRIC_IO_NUM_CLASSES; c++) {
            const char *values[] = {
                diskfs_metric_io_dir_names[d],
                diskfs_metric_io_class_names[c],
            };

            m->block_io_ops_series[d][c] = prometheus_counter_create_series(
                m->block_io_ops, io_labels, values, 2);
            m->block_io_bytes_series[d][c] = prometheus_counter_create_series(
                m->block_io_bytes, io_labels, values, 2);
        }
    }
    m->block_io_device_ops_series = calloc(
        (size_t) shared->num_devices * DISKFS_METRIC_IO_NUM_DIRS *
        DISKFS_METRIC_IO_NUM_CLASSES, sizeof(*m->block_io_device_ops_series));
    m->block_io_device_bytes_series = calloc(
        (size_t) shared->num_devices * DISKFS_METRIC_IO_NUM_DIRS *
        DISKFS_METRIC_IO_NUM_CLASSES, sizeof(*m->block_io_device_bytes_series));
    for (int dev = 0; dev < shared->num_devices; dev++) {
        for (int d = 0; d < DISKFS_METRIC_IO_NUM_DIRS; d++) {
            for (int c = 0; c < DISKFS_METRIC_IO_NUM_CLASSES; c++) {
                size_t      idx = ((size_t) dev * DISKFS_METRIC_IO_NUM_DIRS + d) *
                    DISKFS_METRIC_IO_NUM_CLASSES + c;
                const char *values[] = {
                    diskfs_metric_io_dir_names[d],
                    diskfs_metric_io_class_names[c],
                    shared->devices[dev].name,
                };

                m->block_io_device_ops_series[idx] = prometheus_counter_create_series(
                    m->block_io_device_ops, io_dev_labels, values, 3);
                m->block_io_device_bytes_series[idx] = prometheus_counter_create_series(
                    m->block_io_device_bytes, io_dev_labels, values, 3);
            }
        }
    }
    for (int i = 0; i < 3; i++) {
        m->txn_series[i] = prometheus_counter_create_series(
            m->txn, txn_label, &txn_names[i], 1);
    }
    m->txn_blocks_series = prometheus_histogram_create_series(m->txn_blocks, NULL, NULL, 0);
    m->txn_bytes_series  = prometheus_histogram_create_series(m->txn_bytes, NULL, NULL, 0);
    for (int i = 0; i < DISKFS_METRIC_TXN_NUM_PHASES; i++) {
        m->txn_latency_series[i] = prometheus_histogram_create_series(
            m->txn_latency, phase_label, &diskfs_metric_txn_phase_names[i], 1);
    }
    m->pending_io_series = prometheus_gauge_create_series(m->pending_io, NULL, NULL, 0);
    for (int i = 0; i < 9; i++) {
        m->intent_log_series[i] = prometheus_gauge_create_series(
            m->intent_log, intent_label, &intent_names[i], 1);
    }
} /* diskfs_metrics_init */

static void
diskfs_thread_metrics_init(struct diskfs_thread *thread)
{
    struct diskfs_metrics        *m  = &thread->shared->metrics;
    struct diskfs_thread_metrics *tm = &thread->metrics;

    if (!m->metrics) {
        return;
    }

    for (int i = 0; i < DISKFS_METRIC_INODE_CACHE_NUM; i++) {
        tm->inode_cache[i] = prometheus_counter_series_create_instance(m->inode_cache_series[i]);
    }
    for (int i = 0; i < DISKFS_METRIC_BLOCK_CACHE_NUM; i++) {
        tm->block_cache[i] = prometheus_counter_series_create_instance(m->block_cache_series[i]);
    }
    for (int i = 0; i < DISKFS_METRIC_MTIME_NUM; i++) {
        tm->mtime[i] = prometheus_counter_series_create_instance(m->mtime_series[i]);
    }
    for (int d = 0; d < DISKFS_METRIC_IO_NUM_DIRS; d++) {
        for (int c = 0; c < DISKFS_METRIC_IO_NUM_CLASSES; c++) {
            tm->block_io_ops[d][c] =
                prometheus_counter_series_create_instance(m->block_io_ops_series[d][c]);
            tm->block_io_bytes[d][c] =
                prometheus_counter_series_create_instance(m->block_io_bytes_series[d][c]);
        }
    }
    tm->block_io_device_ops = calloc(
        (size_t) m->num_devices * DISKFS_METRIC_IO_NUM_DIRS *
        DISKFS_METRIC_IO_NUM_CLASSES, sizeof(*tm->block_io_device_ops));
    tm->block_io_device_bytes = calloc(
        (size_t) m->num_devices * DISKFS_METRIC_IO_NUM_DIRS *
        DISKFS_METRIC_IO_NUM_CLASSES, sizeof(*tm->block_io_device_bytes));
    for (int dev = 0; dev < m->num_devices; dev++) {
        for (int d = 0; d < DISKFS_METRIC_IO_NUM_DIRS; d++) {
            for (int c = 0; c < DISKFS_METRIC_IO_NUM_CLASSES; c++) {
                size_t idx = ((size_t) dev * DISKFS_METRIC_IO_NUM_DIRS + d) *
                    DISKFS_METRIC_IO_NUM_CLASSES + c;

                tm->block_io_device_ops[idx] =
                    prometheus_counter_series_create_instance(
                        m->block_io_device_ops_series[idx]);
                tm->block_io_device_bytes[idx] =
                    prometheus_counter_series_create_instance(
                        m->block_io_device_bytes_series[idx]);
            }
        }
    }
    for (int i = 0; i < 3; i++) {
        tm->txn[i] = prometheus_counter_series_create_instance(m->txn_series[i]);
    }
    tm->txn_blocks = prometheus_histogram_series_create_instance(m->txn_blocks_series);
    tm->txn_bytes  = prometheus_histogram_series_create_instance(m->txn_bytes_series);
    for (int i = 0; i < DISKFS_METRIC_TXN_NUM_PHASES; i++) {
        tm->txn_latency[i] = prometheus_histogram_series_create_instance(m->txn_latency_series[i]);
    }
    tm->pending_io = prometheus_gauge_series_create_instance(m->pending_io_series);
} /* diskfs_thread_metrics_init */

static void
diskfs_intent_log_metrics_init(struct diskfs_intent_log *il)
{
    struct diskfs_shared  *shared = container_of(il, struct diskfs_shared, intent_log);
    struct diskfs_metrics *m      = &shared->metrics;

    if (!m->metrics) {
        return;
    }

    for (int d = 0; d < DISKFS_METRIC_IO_NUM_DIRS; d++) {
        for (int c = 0; c < DISKFS_METRIC_IO_NUM_CLASSES; c++) {
            il->metrics.block_io_ops[d][c] =
                prometheus_counter_series_create_instance(m->block_io_ops_series[d][c]);
            il->metrics.block_io_bytes[d][c] =
                prometheus_counter_series_create_instance(m->block_io_bytes_series[d][c]);
        }
    }
    il->metrics.block_io_device_ops = calloc(
        (size_t) m->num_devices * DISKFS_METRIC_IO_NUM_DIRS *
        DISKFS_METRIC_IO_NUM_CLASSES, sizeof(*il->metrics.block_io_device_ops));
    il->metrics.block_io_device_bytes = calloc(
        (size_t) m->num_devices * DISKFS_METRIC_IO_NUM_DIRS *
        DISKFS_METRIC_IO_NUM_CLASSES, sizeof(*il->metrics.block_io_device_bytes));
    for (int dev = 0; dev < m->num_devices; dev++) {
        for (int d = 0; d < DISKFS_METRIC_IO_NUM_DIRS; d++) {
            for (int c = 0; c < DISKFS_METRIC_IO_NUM_CLASSES; c++) {
                size_t idx = ((size_t) dev * DISKFS_METRIC_IO_NUM_DIRS + d) *
                    DISKFS_METRIC_IO_NUM_CLASSES + c;

                il->metrics.block_io_device_ops[idx] =
                    prometheus_counter_series_create_instance(
                        m->block_io_device_ops_series[idx]);
                il->metrics.block_io_device_bytes[idx] =
                    prometheus_counter_series_create_instance(
                        m->block_io_device_bytes_series[idx]);
            }
        }
    }
    for (int i = 0; i < DISKFS_METRIC_TXN_NUM_PHASES; i++) {
        il->metrics.txn_latency[i] =
            prometheus_histogram_series_create_instance(m->txn_latency_series[i]);
    }
    il->metrics.redo_inflight =
        prometheus_gauge_series_create_instance(m->intent_log_series[0]);
    il->metrics.iocbs_inflight =
        prometheus_gauge_series_create_instance(m->intent_log_series[1]);
    il->metrics.push_outstanding =
        prometheus_gauge_series_create_instance(m->intent_log_series[2]);
    il->metrics.log_used_bytes =
        prometheus_gauge_series_create_instance(m->intent_log_series[3]);
    il->metrics.registered_channels =
        prometheus_gauge_series_create_instance(m->intent_log_series[4]);
    il->metrics.redo_inflight_high_water =
        prometheus_gauge_series_create_instance(m->intent_log_series[5]);
    il->metrics.iocbs_inflight_high_water =
        prometheus_gauge_series_create_instance(m->intent_log_series[6]);
    il->metrics.push_outstanding_high_water =
        prometheus_gauge_series_create_instance(m->intent_log_series[7]);
    il->metrics.log_used_bytes_high_water =
        prometheus_gauge_series_create_instance(m->intent_log_series[8]);
} /* diskfs_intent_log_metrics_init */

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

/*
 * Grant (or enqueue a waiter for) `inode` with the shard lock already held, and
 * release the lock before returning.  Shared by diskfs_inode_acquire (after the
 * rb-tree lookup) and diskfs_inode_acquire_pinned (lookup skipped because an
 * open handle pins the inode).  On a compatible WRITE grant this pins the home
 * block, which may async-load it and defer the callback.  `gen` is recorded on
 * a parked waiter so a later grant can detect a stale generation.
 */
static void
diskfs_inode_grant_locked(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    struct diskfs_inode_shard  *shard,
    struct diskfs_inode        *inode,
    uint32_t                    gen,
    enum diskfs_inode_lock_mode mode,
    diskfs_inode_cb_t           cb,
    void                       *private_data)
{
    struct diskfs_inode_waiter *w;

    if (diskfs_inode_lock_compatible(inode, mode)) {
        diskfs_metric_inode_cache(thread, DISKFS_METRIC_INODE_CACHE_HIT);
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

    w = diskfs_waiter_alloc(thread);
    diskfs_metric_inode_cache(thread, DISKFS_METRIC_INODE_CACHE_WAIT);
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
} /* diskfs_inode_grant_locked */

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
    struct diskfs_inode_shard *shard;
    struct diskfs_inode       *inode;
    int                        i;

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
        diskfs_metric_inode_cache(thread, DISKFS_METRIC_INODE_CACHE_STALE);
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
        diskfs_metric_inode_cache(thread, DISKFS_METRIC_INODE_CACHE_MISS);
        pthread_mutex_unlock(&shard->lock);
        if (sm_inum_valid(thread->shared->space_map, inum)) {
            diskfs_inode_load(thread, txn, inum, gen, mode, cb, private_data);
        } else {
            cb(NULL, CHIMERA_VFS_ENOENT, private_data);
        }
        return;
    }

    diskfs_inode_grant_locked(thread, txn, shard, inode, gen, mode, cb, private_data);
} /* diskfs_inode_acquire */

/*
 * Acquire the inode lock on an inode already pinned by an open handle (its
 * refcnt was bumped in diskfs_open_fh_inode_cb, so it is resident and will not
 * be freed; gen bumps only on free).  This skips the fh->inum decode and the
 * inode-cache rb-tree lookup -- the hot per-I/O cost on a warm handle -- but
 * still takes the shard lock to serialize the lock-state grant against the
 * concurrent release/completion path.
 */
static void
diskfs_inode_acquire_pinned(
    struct diskfs_thread       *thread,
    struct diskfs_txn          *txn,
    struct diskfs_inode        *inode,
    enum diskfs_inode_lock_mode mode,
    diskfs_inode_cb_t           cb,
    void                       *private_data)
{
    struct diskfs_inode_shard *shard;
    int                        i;

    /* Already locked in this txn: reuse the held grant (matches the txn-slot
     * fast path in diskfs_inode_acquire). */
    for (i = 0; i < txn->num_inodes; i++) {
        if (txn->inodes[i].inode == inode) {
            cb(inode, CHIMERA_VFS_OK, private_data);
            return;
        }
    }

    shard = diskfs_inode_shard(thread->shared, inode->inum);
    pthread_mutex_lock(&shard->lock);
    diskfs_inode_grant_locked(thread, txn, shard, inode, inode->gen, mode, cb,
                              private_data);
} /* diskfs_inode_acquire_pinned */

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
        inode                 = calloc(1, sizeof(*inode));
        inode->inum           = inum;
        inode->refcnt         = 1;
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
        inode->parent_inum    = di->parent_inum;
        inode->parent_gen     = di->parent_gen;
        rb_tree_insert(&shard->inodes, inum, inode);
        shard->ninodes++;
        diskfs_metric_inode_cache(thread, DISKFS_METRIC_INODE_CACHE_LOAD);
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
diskfs_block_recycle(
    struct diskfs_thread      *thread,
    struct diskfs_block_shard *shard)
{
    struct diskfs_block *blk = shard->lru_head;
    struct diskfs_block *cur, *prev;
    uint32_t             ob;

    chimera_diskfs_abort_if(!blk,
                            "block cache shard exhausted: every buffer pinned "
                            "(raise block_cache_blocks above the intent-log size, "
                            "or a pin was leaked)");
    diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_RECYCLE);

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
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_MISS);
        blk                = diskfs_block_recycle(thread, shard);
        blk->device_id     = device_id;
        blk->device_offset = device_offset;
        blk->state         = DISKFS_BLOCK_CLEAN;
        blk->seq           = 0;
        blk->wait_head     = NULL;
        blk->wait_tail     = NULL;
        diskfs_block_ensure_iov(thread, blk);
        if (is_new) {
            diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_NEW);
        }

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
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_HIT);
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
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_COW);
    } else {
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_HIT);
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

    diskfs_pending_io_add(self, -1);
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
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_HIT);
        diskfs_bt_op_pin(op, shard, blk);
        pthread_mutex_unlock(&shard->lock);
        return blk;
    }

    if (!blk) {
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_MISS);
        blk                    = diskfs_block_recycle(thread, shard);
        blk->device_id         = device_id;
        blk->device_offset     = device_offset;
        blk->state             = DISKFS_BLOCK_LOADING;
        blk->seq               = 0;
        blk->wait_head         = NULL;
        blk->wait_tail         = NULL;
        blk->hash_next         = shard->buckets[bucket];
        shard->buckets[bucket] = blk;
        issue                  = 1;
    } else {
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_WAIT);
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
        diskfs_pending_io_add(thread, 1);
        diskfs_metric_block_io(thread, DISKFS_METRIC_IO_READ,
                               DISKFS_METRIC_IO_BTREE, DISKFS_BLOCK_SIZE);
        diskfs_metric_block_io_device(thread, device_id, DISKFS_METRIC_IO_READ,
                                      DISKFS_METRIC_IO_BTREE, DISKFS_BLOCK_SIZE);
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
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_WAIT);
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
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_MISS);
        blk                = diskfs_block_recycle(thread, shard);
        blk->device_id     = device_id;
        blk->device_offset = device_offset;
        blk->seq           = 0;
        blk->wait_head     = NULL;
        blk->wait_tail     = NULL;

        if (is_new) {
            diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_NEW);
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
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_HIT);
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
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_COW);
    } else {
        diskfs_metric_block_cache(thread, DISKFS_METRIC_BLOCK_CACHE_HIT);
    }

    if (issue) {
        pthread_mutex_unlock(&shard->lock);
        diskfs_block_ensure_iov(thread, blk);
        ld         = malloc(sizeof(*ld));
        ld->blk    = blk;
        ld->thread = thread;
        diskfs_pending_io_add(thread, 1);
        diskfs_metric_block_io(thread, DISKFS_METRIC_IO_READ,
                               DISKFS_METRIC_IO_METADATA, DISKFS_BLOCK_SIZE);
        diskfs_metric_block_io_device(thread, device_id, DISKFS_METRIC_IO_READ,
                                      DISKFS_METRIC_IO_METADATA, DISKFS_BLOCK_SIZE);
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

/*
 * Inverse of the write-lock home-block pin, for the deferred-mtime path: detach
 * the inode's home block from the txn and unpin it so the txn commits with no
 * durable block (the mtime bump is held in memory and logged later by the
 * coalescing flusher).  The block stays in the cache (CLEAN/evictable) and is
 * re-claimed when something next needs it.  Only valid when the block is clean
 * (the deferred path makes no on-block change), which an in-place overwrite of
 * an already-written extent guarantees.
 */
static void
diskfs_txn_drop_inode_block(
    struct diskfs_thread *thread,
    struct diskfs_txn    *txn,
    struct diskfs_inode  *inode)
{
    struct diskfs_txn_block **pp, *tb;

    if (!inode->block) {
        return;
    }
    for (pp = &txn->blocks; *pp; pp = &(*pp)->next) {
        if ((*pp)->block == inode->block) {
            tb  = *pp;
            *pp = tb->next;
            free(tb);
            break;
        }
    }
    diskfs_block_release(thread, inode->block);
    inode->block = NULL;
} /* diskfs_txn_drop_inode_block */

/* Serialize an inode's durable attributes into the front of its block. */
static void
diskfs_inode_flush(struct diskfs_inode *inode)
{
    struct diskfs_dinode *di;

    if (!inode->block) {
        return;
    }

    di = inode->block->iov.data;

    di->inum           = inode->inum;
    di->gen            = inode->gen;
    di->mode           = inode->mode;
    di->nlink          = inode->nlink;
    di->uid            = inode->uid;
    di->gid            = inode->gid;
    di->rdev           = inode->rdev;
    di->size           = inode->size;
    di->space_used     = inode->space_used;
    di->atime_sec      = inode->atime_sec;
    di->mtime_sec      = inode->mtime_sec;
    di->ctime_sec      = inode->ctime_sec;
    di->btime_sec      = inode->btime_sec;
    di->atime_nsec     = inode->atime_nsec;
    di->mtime_nsec     = inode->mtime_nsec;
    di->ctime_nsec     = inode->ctime_nsec;
    di->btime_nsec     = inode->btime_nsec;
    di->dos_attributes = inode->dos_attributes;
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

static inline struct diskfs_bt_key
diskfs_bt_node_min_key(
    void    *buf,
    uint32_t base)
{
    struct diskfs_bt_node_hdr *h = diskfs_bt_hdr(buf, base);

    chimera_diskfs_abort_if(h->nitems == 0, "b+tree empty node has no minimum key");
    return h->level == 0 ? diskfs_bt_lslots(buf, base)[0].key :
           diskfs_bt_islots(buf, base)[0].key;
} /* diskfs_bt_node_min_key */

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
                         SM_DEV_LOCAL, DISKFS_BLOCK_SIZE, &device_id, &device_offset);
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
    sl[ci].key = diskfs_bt_node_min_key(child_buf, 0);
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
        if (total > 0) {
            psl[lidx].key = diskfs_bt_lslots(lbuf, 0)[0].key;
        }
        merged = 1;

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

        psl[lidx].key = diskfs_bt_lslots(lbuf, 0)[0].key;
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
        if (total > 0) {
            psl[lidx].key = diskfs_bt_islots(lbuf, 0)[0].key;
        }
        merged = 1;

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

        psl[lidx].key = diskfs_bt_islots(lbuf, 0)[0].key;
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
 * empty/small directories.  The inode home block is not returned to the
 * allocator yet: inode structs stay cached after unlink and generation reuse is
 * not persisted for newly allocated inodes, so reusing an inum can collide with
 * the cache and stale file handles.
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
    void                      *buf;
    struct diskfs_bt_node_hdr *h;

    diskfs_txn_pin_inode_block(thread, txn, inode, 0);
    buf = inode->block->iov.data;
    h   = diskfs_bt_hdr(buf, DISKFS_BT_ROOT_BASE);

    if (h->level == 0 && S_ISREG(inode->mode)) {
        /* Small file: data extents are recorded in the embedded root, mixed
         * with non-space records such as xattrs. */
        struct diskfs_bt_lslot *s = diskfs_bt_lslots(buf, DISKFS_BT_ROOT_BASE);
        int                     i;

        for (i = 0; i < h->nitems; i++) {
            struct diskfs_extent_rec *e =
                (struct diskfs_extent_rec *) ((char *) buf + DISKFS_BT_ROOT_BASE + s[i].off);

            if (s[i].key.type != DISKFS_REC_EXTENT) {
                continue;
            }
            diskfs_txn_free_space(thread, txn, e->device_id, e->device_offset,
                                  SM_ALIGN_UP(e->length));
        }
    }

    /* Leave the inode home block pinned + logged with the nlink=0 dinode. */
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

/* The durable orphan entry is removed; finish retiring the inode in the same
 * transaction and commit.  The inode home block is intentionally leaked for now;
 * see diskfs_bt_free_tree. */
static void
diskfs_drain_after_unrecord(void *priv)
{
    struct diskfs_drain *d = priv;

    diskfs_txn_pin_inode_block(d->thread, d->txn, d->inode, 0);
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
        /* Tree empty: remove the durable orphan entry, then retire the inode in
         * the same txn.  Removing the orphan entry last means a crash before
         * this commit just re-drains on the next mount (idempotent); the orphan
         * inode (3) is acquired last (leaf -> no deadlock). */
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

    /* Remount fault-in: bootstrap (mkfs) seeds the reserved root inode into
     * cache, but a remount skips bootstrap, so the root lives only on disk.
     * Seed it synchronously here -- the MOUNT op's own walk would otherwise
     * fault it in with an async read that the mount-time context never pumps
     * to completion, hanging the mount.  A freshly bootstrapped FS already has
     * it resident, so this is a cheap cache hit. */
    diskfs_inode_load_sync(thread, io, shared->root_inum, shared->root_gen, 0);

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
                                        &thread->space_cache, &jnl, SM_DEV_LOCAL,
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
                if (op->removed_idx == 2 && op->reb_level == op->depth - 1) {
                    /* Leaf-parent level: a leaf merge rewrites the right
                     * participant's next-neighbour back-link.  That neighbour
                     * is reachable only through the leaf chain (it is not a
                     * child of any node on the descent path), so the merge's
                     * synchronous node_for_write would miss it on a cold cache
                     * (remount).  Fault it in here.  The right participant is
                     * ridx = (ci+1 < pn) ? ci+1 : ci (matching
                     * diskfs_bt_rebalance_leaf) and is already resident (faulted
                     * by the descent or the ci+1 step above). */
                    int                  ridx = (ci + 1 < pn) ? ci + 1 : ci;
                    uint64_t             rb   = diskfs_bt_islots(pbuf, pe->base)[ridx].child;
                    struct diskfs_block *rblk;
                    uint64_t             rnext;

                    off  = sm_inum_to_device_offset(thread->shared->space_map, rb, &dev);
                    rblk = diskfs_bt_block_get(op, dev, off);
                    if (!rblk) {
                        return;
                    }
                    rnext = diskfs_bt_hdr(rblk->iov.data, 0)->next_leaf;
                    if (rnext) {
                        off = sm_inum_to_device_offset(thread->shared->space_map, rnext, &dev);
                        if (!diskfs_bt_block_get(op, dev, off)) {
                            return;
                        }
                    }
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
                struct diskfs_bt_islot *isl = diskfs_bt_islots(buf, base);
                int                     ci  = diskfs_bt_interior_search(buf, base, &op->key);

                op->last_parent_valid      = 1;
                op->last_parent_ci         = ci;
                op->last_parent_nitems     = h->nitems;
                op->last_parent_key        = isl[ci].key;
                op->last_parent_child      = isl[ci].child;
                op->last_parent_next_child = 0;
                if (ci + 1 < h->nitems) {
                    op->last_parent_next_key   = isl[ci + 1].key;
                    op->last_parent_next_child = isl[ci + 1].child;
                }

                if (op->opcode == DISKFS_BT_OP_INSERT ||
                    op->opcode == DISKFS_BT_OP_REMOVE) {
                    chimera_diskfs_abort_if(op->depth >= DISKFS_BT_MAX_DEPTH,
                                            "b+tree op: path too deep");
                    op->path[op->depth].bptr = op->use_root ? 0 : op->cur_bptr;
                    op->path[op->depth].base = base;
                    op->path[op->depth].ci   = ci;
                    op->depth++;
                }
                op->cur_bptr = isl[ci].child;
                op->use_root = 0;
                continue;
            }

            /* At the leaf. */
            if (op->opcode == DISKFS_BT_OP_INSERT) {
                /* A leaf split rewrites the right sibling's prev_leaf link.
                 * That sibling is off the descent path, so on a cold cache
                 * (remount) the synchronous split's node_for_write would miss
                 * it.  Fault it in first via the async evpl_block path (parks +
                 * resumes into this phase if not resident); a warm cache hits. */
                if (h->next_leaf) {
                    off = sm_inum_to_device_offset(thread->shared->space_map,
                                                   h->next_leaf, &dev);
                    if (!diskfs_bt_block_get(op, dev, off)) {
                        return;
                    }
                }
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

                if (unlikely(!exact)) {
                    diskfs_bt_complete(op, -1);
                    return;
                }
                diskfs_bt_complete(op, diskfs_bt_op_emit(op, buf, base, idx));
                return;
            } else if (op->opcode == DISKFS_BT_OP_LOOKUP_GE) {
                int exact, idx = h->nitems ? diskfs_bt_leaf_search(buf, base, &op->key, &exact) : 0;

                if (idx < h->nitems) {
                    if (unlikely(diskfs_bt_key_cmp(&diskfs_bt_lslots(buf, base)[idx].key,
                                                   &op->key) < 0)) {
                        chimera_diskfs_error("b+tree lookup_ge routed backwards");
                        diskfs_bt_complete(op, -1);
                        return;
                    }
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
                if (unlikely(diskfs_bt_key_cmp(&diskfs_bt_lslots(buf, base)[0].key,
                                               &op->key) < 0)) {
                    chimera_diskfs_error("b+tree leaf chain moved backwards during lookup_ge");
                    diskfs_bt_complete(op, -1);
                    return;
                }
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

/* True if `dir` contains no entries.  "." and ".." are synthesised by readdir
 * (not stored), so a directory with no DIRENT records is genuinely empty -- a
 * stronger test than nlink (which only counts subdirectories). */
static int
diskfs_dir_is_empty(
    struct diskfs_thread *thread,
    struct diskfs_inode  *dir)
{
    char     name[DISKFS_DIRENT_REC_MAX];
    uint64_t r_hash, r_inum;
    uint32_t r_gen;
    int      r_namelen;

    return diskfs_dir_next(thread, dir, 0, &r_hash, &r_inum, &r_gen,
                           name, &r_namelen) != 0;
} /* diskfs_dir_is_empty */

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
    uint64_t              device_offset,
    uint32_t              flags)
{
    struct diskfs_extent_rec rec = {
        .length        = length,
        .device_id     = device_id,
        .flags         = flags,
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
    out->flags         = rec.flags;
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
    out->flags         = rec.flags;
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
        inode->parent_inum    = di->parent_inum;
        inode->parent_gen     = di->parent_gen;
        rb_tree_insert(&shard->inodes, inum, inode);
        shard->ninodes++;
        diskfs_metric_inode_cache(thread, DISKFS_METRIC_INODE_CACHE_LOAD);
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
    diskfs_metric_block_io(thread, DISKFS_METRIC_IO_READ,
                           DISKFS_METRIC_IO_INODE, DISKFS_BLOCK_SIZE);
    diskfs_metric_block_io_device(thread, dev, DISKFS_METRIC_IO_READ,
                                  DISKFS_METRIC_IO_INODE, DISKFS_BLOCK_SIZE);
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
                         SM_DEV_LOCAL, SM_BLOCK_SIZE, &device_id, &device_offset);
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
    diskfs_metric_inode_cache(thread, DISKFS_METRIC_INODE_CACHE_INSERT);
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
    uint32_t                dev_id;
    int                     rc;
    struct space_map       *sm = thread->shared->space_map;

    /* File data goes to REMOTE (pNFS data) devices when this is a block-mode
     * filesystem, and to LOCAL devices otherwise; each class draws from its own
     * per-thread reservation cache so a local metadata reservation and a remote
     * data reservation never collide. */
    int                     remote = space_map_has_remote(sm);
    uint32_t                role   = remote ? SM_DEV_REMOTE : SM_DEV_LOCAL;
    struct sm_thread_cache *cache  = remote ? &thread->data_space_cache :
        &thread->space_cache;

    DISKFS_SM_JNL(jnl, thread, txn, resume, resume_arg);
    rc = space_map_alloc(sm, cache, &jnl, role,
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
/* Commit thread owns log_head; the push thread advances log_tail (trim) and
 * the commit thread reads it here to check space.  head == tail means empty
 * (place() always leaves the wrap gap, so the ring never reads full as empty). */
static uint64_t
diskfs_il_contig_free(struct diskfs_intent_log *il)
{
    uint64_t start = SM_INTENT_LOG_OFFSET;
    uint64_t end   = SM_INTENT_LOG_OFFSET + SM_INTENT_LOG_SIZE;
    uint64_t head  = il->log_head;     /* commit-owned */
    uint64_t tail  = __atomic_load_n(&il->log_tail, __ATOMIC_ACQUIRE);

    if (head == tail) {
        return SM_INTENT_LOG_SIZE;     /* empty */
    }
    if (head >= tail) {
        uint64_t run_end   = end - head;
        uint64_t run_start = tail - start;

        return run_end > run_start ? run_end : run_start;
    }
    return tail - head;
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
    uint64_t end  = SM_INTENT_LOG_OFFSET + SM_INTENT_LOG_SIZE;
    uint64_t head = il->log_head;
    uint64_t offset;

    if (head + reclen > end) {
        head = SM_INTENT_LOG_OFFSET;     /* wrap; tail of region unused */
    }
    offset = head;
    __atomic_store_n(&il->log_head, head + reclen, __ATOMIC_RELEASE);
    return offset;
} /* diskfs_il_place */

/* ================================================================== */
/* Tail-push thread: per-block coalescing home writes + in-order trim  */
/* ================================================================== */

static inline uint32_t
diskfs_il_pow2(uint32_t v)
{
    v--;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    return v + 1;
} /* diskfs_il_pow2 */

/* Hash a home location to a pending-map bucket. */
static inline uint32_t
diskfs_il_home_hash(
    uint32_t device_id,
    uint64_t device_offset,
    uint32_t mask)
{
    uint64_t k = (device_offset / DISKFS_BLOCK_SIZE) ^ ((uint64_t) device_id << 48);

    k *= 0x9E3779B97F4A7C15ULL;
    return (uint32_t) (k >> 32) & mask;
} /* diskfs_il_home_hash */

static struct diskfs_pending *
diskfs_pending_lookup(
    struct diskfs_intent_log *il,
    uint32_t                  dev,
    uint64_t                  off)
{
    struct diskfs_pending *p = il->phash[diskfs_il_home_hash(dev, off, il->phash_mask)];

    for (; p; p = p->hnext) {
        if (p->device_id == dev && p->device_offset == off) {
            return p;
        }
    }
    return NULL;
} /* diskfs_pending_lookup */

static struct diskfs_pending *
diskfs_pending_alloc(struct diskfs_intent_log *il)
{
    struct diskfs_pending *p = il->pfree;

    if (p) {
        il->pfree = p->rnext;
    } else {
        p = malloc(sizeof(*p));
    }
    p->il       = il;
    p->inflight = 0;
    p->on_ready = 0;
    return p;
} /* diskfs_pending_alloc */

static void
diskfs_pending_insert(
    struct diskfs_intent_log *il,
    struct diskfs_pending    *p)
{
    uint32_t b = diskfs_il_home_hash(p->device_id, p->device_offset, il->phash_mask);

    p->hnext     = il->phash[b];
    il->phash[b] = p;
} /* diskfs_pending_insert */

static void
diskfs_pending_remove(
    struct diskfs_intent_log *il,
    struct diskfs_pending    *p)
{
    uint32_t                b  = diskfs_il_home_hash(p->device_id, p->device_offset, il->phash_mask);
    struct diskfs_pending **pp = &il->phash[b];

    while (*pp && *pp != p) {
        pp = &(*pp)->hnext;
    }
    if (*pp) {
        *pp = p->hnext;
    }
    p->rnext  = il->pfree;     /* recycle onto the free list */
    il->pfree = p;
} /* diskfs_pending_remove */

static void
diskfs_ready_push(
    struct diskfs_intent_log *il,
    struct diskfs_pending    *p)
{
    if (p->on_ready) {
        return;
    }
    p->on_ready = 1;
    p->rnext    = NULL;
    if (il->ready_tail) {
        il->ready_tail->rnext = p;
    } else {
        il->ready_head = p;
    }
    il->ready_tail = p;
} /* diskfs_ready_push */

static struct diskfs_pending *
diskfs_ready_pop(struct diskfs_intent_log *il)
{
    struct diskfs_pending *p = il->ready_head;

    if (!p) {
        return NULL;
    }
    il->ready_head = p->rnext;
    if (!il->ready_head) {
        il->ready_tail = NULL;
    }
    p->on_ready = 0;
    p->rnext    = NULL;
    return p;
} /* diskfs_ready_pop */

static void
diskfs_il_free_record(
    struct diskfs_intent_log *il,
    struct diskfs_il_record  *rec)
{
    evpl_iovecs_release(il->push_evpl, rec->iovs, rec->niov);
    free(rec->iovs);
    free(rec);
} /* diskfs_il_free_record */

/*
 * Fold one durable record's blocks into the pending map.  Records are folded in
 * log/seq order, so a later record's image of a block supersedes an earlier
 * one (the coalescing win: a block logged by many queued commits is written
 * home once).  A newly-pending block is queued for issue; a block already in
 * flight is left for its completion to re-issue the newer image.
 */
static void
diskfs_push_fold_record(
    struct diskfs_intent_log *il,
    struct diskfs_il_record  *rec)
{
    char    *p = (char *) rec->iovs[0].data + sizeof(struct diskfs_redo_header);
    uint32_t i;

    for (i = 0; i < rec->num_blocks; i++) {
        struct diskfs_redo_block_header *bh = (struct diskfs_redo_block_header *) p;
        struct diskfs_pending           *e;

        p += sizeof(*bh);

        e = diskfs_pending_lookup(il, bh->device_id, bh->device_offset);
        if (!e) {
            e                = diskfs_pending_alloc(il);
            e->device_id     = bh->device_id;
            e->device_offset = bh->device_offset;
            e->seq           = rec->seq;
            e->iov           = &rec->iovs[1 + i];
            e->owner         = rec;
            diskfs_pending_insert(il, e);
            diskfs_ready_push(il, e);
        } else {
            e->seq   = rec->seq;     /* newest image supersedes */
            e->iov   = &rec->iovs[1 + i];
            e->owner = rec;
            if (!e->inflight) {
                diskfs_ready_push(il, e);
            }
        }
    }
} /* diskfs_push_fold_record */

/* A block is durably home at `seq`: LOGGED -> CLEAN if it has not been
 * re-logged since (blk->seq still == seq) and isn't pinned.  Under the shard
 * lock so it serializes against diskfs_block_claim re-dirtying the block. */
static void
diskfs_push_clean_block(
    struct diskfs_intent_log *il,
    uint32_t                  dev,
    uint64_t                  off,
    uint64_t                  seq)
{
    struct diskfs_shared      *shared = container_of(il, struct diskfs_shared, intent_log);
    struct diskfs_block_cache *cache  = shared->block_cache;
    struct diskfs_block_shard *shard  = diskfs_block_shard(cache, dev, off);
    uint32_t                   bucket = diskfs_block_bucket(dev, off);
    struct diskfs_block       *blk;

    pthread_mutex_lock(&shard->lock);
    blk = diskfs_block_lookup_locked(shard, bucket, dev, off);
    if (blk && blk->state == DISKFS_BLOCK_LOGGED &&
        __atomic_load_n(&blk->seq, __ATOMIC_ACQUIRE) == seq && blk->pin_count == 0) {
        blk->state = DISKFS_BLOCK_CLEAN;
        if (!blk->on_lru) {
            diskfs_block_lru_push_tail(shard, blk);
        }
    }
    pthread_mutex_unlock(&shard->lock);
} /* diskfs_push_clean_block */

/*
 * Record R is coverable iff every block it logged is either home at its newest
 * seq (absent from pending) or superseded by a later still-logged record
 * (pending seq > R.seq).  pending[X].seq == R.seq means R's own image is still
 * the newest pending and not yet home, so R must wait.
 */
static int
diskfs_push_record_covered(
    struct diskfs_intent_log *il,
    struct diskfs_il_record  *rec)
{
    char    *p = (char *) rec->iovs[0].data + sizeof(struct diskfs_redo_header);
    uint32_t i;

    for (i = 0; i < rec->num_blocks; i++) {
        struct diskfs_redo_block_header *bh = (struct diskfs_redo_block_header *) p;
        struct diskfs_pending           *e;

        p += sizeof(*bh);
        e  = diskfs_pending_lookup(il, bh->device_id, bh->device_offset);
        if (e && e->seq == rec->seq) {
            return 0;
        }
    }
    return 1;
} /* diskfs_push_record_covered */

/* Advance the trim point over the contiguous prefix of fully-covered records,
 * freeing each once no in-flight home write still reads its image. */
static void
diskfs_push_trim(struct diskfs_intent_log *il)
{
    int advanced = 0;

    while (il->push_head && diskfs_push_record_covered(il, il->push_head)) {
        struct diskfs_il_record *rec = il->push_head;

        il->push_head = rec->next;
        if (!il->push_head) {
            il->push_tail = NULL;
        }

        /* Trim point = start of the oldest record we have not retired: the next
         * record in the FIFO, or (FIFO drained) the oldest record the commit
         * thread has handed off but we have not yet consumed.  If neither is
         * known, leave log_tail unchanged (conservative -- the next hand-off
         * advances it). */
        if (il->push_head) {
            __atomic_store_n(&il->log_tail, il->push_head->offset, __ATOMIC_RELEASE);
        } else {
            uint32_t hh = il->handoff_head;
            uint32_t ht = __atomic_load_n(&il->handoff_tail, __ATOMIC_ACQUIRE);
            if (hh != ht) {
                __atomic_store_n(&il->log_tail,
                                 il->handoff[hh & DISKFS_HANDOFF_RING_MASK]->offset,
                                 __ATOMIC_RELEASE);
            }
        }

        rec->retired = 1;
        if (rec->inflight_refs == 0) {
            diskfs_il_free_record(il, rec);
        }
        advanced = 1;
    }

    if (advanced) {
        diskfs_il_push_metrics(il);
        /* Freed log space -> resume the commit thread, but only while its
         * doorbell is still live.  The commit thread is destroyed before the
         * push thread (the push thread drains the records it handed off), which
         * closes wake_doorbell's fd; ringing it after that aborts.  commit_alive
         * is cleared before that teardown, and during shutdown the commit thread
         * makes progress by self-pumping, so a skipped wake is harmless. */
        if (__atomic_load_n(&il->commit_alive, __ATOMIC_ACQUIRE)) {
            evpl_ring_doorbell(&il->wake_doorbell);
        }
    }
} /* diskfs_push_trim */

static void diskfs_push_block_cb(
    struct evpl *evpl,
    int          status,
    void        *private_data);

/* Issue ready home writes up to the push watermark, one per unique block. */
static void
diskfs_push_issue(struct diskfs_intent_log *il)
{
    while (il->push_outstanding < DISKFS_PUSH_WATERMARK) {
        struct diskfs_pending *e = diskfs_ready_pop(il);

        if (!e) {
            break;
        }

        e->inflight     = 1;
        e->issued_seq   = e->seq;
        e->issued_owner = e->owner;
        e->owner->inflight_refs++;
        il->push_outstanding++;
        diskfs_il_push_metrics(il);

        diskfs_metric_il_block_io(il, DISKFS_METRIC_IO_WRITE,
                                  DISKFS_METRIC_IO_TAIL_PUSH, DISKFS_BLOCK_SIZE);
        diskfs_metric_il_block_io_device(il, e->device_id, DISKFS_METRIC_IO_WRITE,
                                         DISKFS_METRIC_IO_TAIL_PUSH, DISKFS_BLOCK_SIZE);
        evpl_block_write(il->push_evpl, il->home_queue[e->device_id], e->iov, 1,
                         e->device_offset, il->sync, diskfs_push_block_cb, e);
    }
} /* diskfs_push_issue */

/* One home write completed (push thread). */
static void
diskfs_push_block_cb(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct diskfs_pending    *e  = private_data;
    struct diskfs_intent_log *il = e->il;
    struct diskfs_il_record  *io = e->issued_owner;

    (void) evpl;
    chimera_diskfs_abort_if(status, "tail-push home write failed: %d", status);

    il->push_outstanding--;

    /* Release the record the in-flight write read from; free it if it has been
     * retired and no other in-flight write still reads it. */
    if (--io->inflight_refs == 0 && io->retired) {
        diskfs_il_free_record(il, io);
    }

    e->inflight = 0;

    if (e->seq > e->issued_seq) {
        /* A newer image arrived mid-flight: re-issue it (newest lands last). */
        diskfs_ready_push(il, e);
    } else {
        /* Durably home at its newest seq: mark CLEAN and drop the entry. */
        diskfs_push_clean_block(il, e->device_id, e->device_offset, e->issued_seq);
        diskfs_pending_remove(il, e);
    }

    diskfs_il_push_metrics(il);
    diskfs_push_issue(il);
    diskfs_push_trim(il);
} /* diskfs_push_block_cb */

/*
 * Push-thread doorbell: drain the hand-off ring into the record FIFO and the
 * pending map, then issue and trim.  Rung by the commit thread after it hands
 * off durable records.
 */
static void
diskfs_il_push_doorbell_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct diskfs_intent_log *il = container_of(doorbell,
                                                struct diskfs_intent_log,
                                                push_doorbell);
    uint32_t                  head = il->handoff_head;
    uint32_t                  tail = __atomic_load_n(&il->handoff_tail, __ATOMIC_ACQUIRE);

    (void) evpl;

    while (head != tail) {
        struct diskfs_il_record *rec = il->handoff[head & DISKFS_HANDOFF_RING_MASK];

        head++;
        rec->next          = NULL;
        rec->inflight_refs = 0;
        rec->retired       = 0;
        if (il->push_tail) {
            il->push_tail->next = rec;
        } else {
            il->push_head = rec;
        }
        il->push_tail = rec;
        diskfs_push_fold_record(il, rec);
    }
    __atomic_store_n(&il->handoff_head, head, __ATOMIC_RELEASE);

    diskfs_push_issue(il);
    diskfs_push_trim(il);
} /* diskfs_il_push_doorbell_cb */

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
    struct diskfs_redo_ctx   *ctx        = private_data;
    struct diskfs_intent_log *il         = ctx->il;
    int                       handed_off = 0;

    (void) evpl;
    chimera_diskfs_abort_if(status, "redo record write failed: %d", status);

    /* One redo block write (one chunk) drained from the log queue.  Resume SQ
     * draining once redo writes bleed back down to the low watermark. */
    if (--il->redo_inflight == DISKFS_COMMIT_LOWAT) {
        evpl_ring_doorbell(&il->wake_doorbell);
    }

    /* One chunk of a possibly multi-chunk journal write landed; the record is
     * durable only when its last chunk completes. */
    if (--ctx->segments > 0) {
        diskfs_il_commit_metrics(il);
        return;
    }

    /* Mark this record done; retire the contiguous completed prefix strictly in
     * submission (== log) order.  Replay stops at the first torn record, so a
     * later record is not recoverable -- nor ACKable -- until every earlier
     * record is durable. */
    il->retire[ctx->retire_idx & DISKFS_RETIRE_RING_MASK].done = 1;

    while (il->retire_head != il->retire_tail &&
           il->retire[il->retire_head & DISKFS_RETIRE_RING_MASK].done) {
        struct diskfs_retire_slot *slot = &il->retire[il->retire_head & DISKFS_RETIRE_RING_MASK];
        struct diskfs_redo_ctx    *rc   = slot->ctx;
        struct diskfs_iq_channel  *ch   = rc->ch;
        struct diskfs_il_record   *rec  = rc->rec;
        uint32_t                   cq_tail, ht;

        prometheus_stopwatch_start(&rc->entry.durable_time);
        diskfs_metric_time_sample(
            il->metrics.txn_latency[DISKFS_METRIC_TXN_SUBMIT_TO_DURABLE],
            &rc->entry.submit_time);
        diskfs_metric_time_sample(
            il->metrics.txn_latency[DISKFS_METRIC_TXN_QUEUE_TO_DURABLE],
            &rc->entry.enqueue_time);

        /* Record durable & recoverable: drop block pins (-> LOGGED), return
         * freed ranges to the allocator, release the inode locks. */
        diskfs_txn_unpin_blocks(rc->entry.txn, DISKFS_BLOCK_LOGGED);
        diskfs_txn_apply_frees(rc->entry.txn);
        diskfs_txn_unlock_all(rc->entry.txn);

        /* ACK the worker by posting the CQE.  No doorbell: the worker is pinned
         * in poll mode while it has a commit outstanding (diskfs_txn_commit_finish),
         * so it reaps this via diskfs_iq_cq_poll every loop iteration.  The
         * cq_doorbell remains registered only as a backstop. */
        rc->entry.status                              = 0;
        cq_tail                                       = __atomic_load_n(&ch->cq.tail, __ATOMIC_RELAXED);
        ch->cq.entries[cq_tail & DISKFS_IQ_RING_MASK] = rc->entry;
        __atomic_store_n(&ch->cq.tail, cq_tail + 1, __ATOMIC_RELEASE);
        ch->cq_inflight--;

        /* Hand the durable record to the push thread, in log order.  The
         * hand-off ring is sized larger than the log can ever hold, so it
         * cannot fill before the log does. */
        ht = il->handoff_tail;
        chimera_diskfs_abort_if(ht - __atomic_load_n(&il->handoff_head, __ATOMIC_ACQUIRE) >=
                                DISKFS_HANDOFF_RING_SIZE, "intent-log hand-off ring overflow");
        il->handoff[ht & DISKFS_HANDOFF_RING_MASK] = rec;
        __atomic_store_n(&il->handoff_tail, ht + 1, __ATOMIC_RELEASE);
        handed_off = 1;

        slot->ctx  = NULL;
        slot->done = 0;
        free(rc);
        il->retire_head++;
    }

    diskfs_il_commit_metrics(il);

    if (handed_off) {
        evpl_ring_doorbell(&il->push_doorbell);
    }
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
    hdr->tail       = __atomic_load_n(&il->log_tail, __ATOMIC_ACQUIRE);
    hdr->num_blocks = nblocks;
    hdr->reclen     = (uint32_t) reclen;
    p              += sizeof(*hdr);

    i = 0;
    for (tb = txn->blocks; tb; tb = tb->next, i++) {
        struct diskfs_block *blk = tb->block;

        /* Stamp the block with this record's seq so the tail-pusher can tell,
         * on push completion, whether the block has been re-logged since (a
         * higher seq) -- if not, it can be marked CLEAN and made evictable.
         * Atomic: the push thread reads blk->seq under the shard lock. */
        __atomic_store_n(&blk->seq, rec->seq, __ATOMIC_RELEASE);

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

    /* Reserve this record's retirement-ring slot (in submission/log order) so
     * the completion can retire the contiguous done-prefix in order.  Caller
     * (diskfs_iq_process_channel) guaranteed ring space before issuing. */
    ctx->retire_idx                                            = il->retire_tail;
    il->retire[il->retire_tail & DISKFS_RETIRE_RING_MASK].ctx  = ctx;
    il->retire[il->retire_tail & DISKFS_RETIRE_RING_MASK].done = 0;
    il->retire_tail++;

    ch->cq_inflight++;
    il->redo_inflight += ctx->segments;     /* one redo block write per chunk below */
    diskfs_il_commit_metrics(il);

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

            evpl_block_write(il->evpl, il->log_queue,
                             &rec->iovs[done], cnt, woff, il->sync,
                             diskfs_redo_write_cb, ctx);
            diskfs_metric_il_block_io(il, DISKFS_METRIC_IO_WRITE,
                                      DISKFS_METRIC_IO_INTENT_LOG, bytes);
            diskfs_metric_il_block_io_device(il, SM_INTENT_LOG_DEVICE,
                                             DISKFS_METRIC_IO_WRITE,
                                             DISKFS_METRIC_IO_INTENT_LOG, bytes);
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

        /* Keep redo writes pipelined up to the commit watermark; a completion
         * rings the wake doorbell at the low watermark to resume us. */
        if (il->redo_inflight >= DISKFS_COMMIT_WATERMARK) {
            break;
        }

        /* Need a free retirement-ring slot for this record. */
        if (il->retire_tail - il->retire_head >= DISKFS_RETIRE_RING_SIZE) {
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

        prometheus_stopwatch_start(&entry.submit_time);
        diskfs_metric_time_sample(
            il->metrics.txn_latency[DISKFS_METRIC_TXN_QUEUE_TO_SUBMIT],
            &entry.enqueue_time);

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

        __atomic_store_n(&ch->registered, 1, __ATOMIC_RELEASE);
    }
    diskfs_il_commit_metrics(il);
} /* diskfs_intent_log_drain_pending */

/* Drain newly-registered channels into the slot array and compact out any that
 * requested unregistration.  Rare (worker-thread lifecycle), so it stays on the
 * wake-doorbell path rather than the per-iteration poll. */
static void
diskfs_il_service_registrations(struct diskfs_intent_log *il)
{
    uint32_t i;

    /* Clear the dirty flag before we read pending_head / scan for unregisters,
     * so a (un)registration published after this point re-sets it and is picked
     * up on a later poll rather than being lost. */
    __atomic_store_n(&il->reg_dirty, 0, __ATOMIC_SEQ_CST);

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
            diskfs_il_commit_metrics(il);
            continue;     /* re-process index i (now a different channel) */
        }
        i++;
    }
} /* diskfs_il_service_registrations */

/* Process every registered channel's SQ.  Returns 1 if any channel had work. */
static int
diskfs_il_process_all(struct diskfs_intent_log *il)
{
    uint32_t i;
    int      worked = 0;

    for (i = 0; i < il->num_channels; i++) {
        struct diskfs_iq_channel *ch = il->channels[i];

        if (__atomic_load_n(&ch->sq.tail, __ATOMIC_ACQUIRE) !=
            __atomic_load_n(&ch->sq.head, __ATOMIC_RELAXED)) {
            worked = 1;
        }
        diskfs_iq_process_channel(ch);
    }
    return worked;
} /* diskfs_il_process_all */

/* Seq-cst re-scan for the poll-exit wakeup handshake (diskfs_iq_try_submit). */
static int
diskfs_il_has_sq_work(struct diskfs_intent_log *il)
{
    uint32_t i;

    for (i = 0; i < il->num_channels; i++) {
        struct diskfs_iq_channel *ch = il->channels[i];

        if (__atomic_load_n(&ch->sq.tail, __ATOMIC_SEQ_CST) !=
            __atomic_load_n(&ch->sq.head, __ATOMIC_SEQ_CST)) {
            return 1;
        }
    }
    return 0;
} /* diskfs_il_has_sq_work */

/* The wake doorbell only needs to rouse a sleeping commit thread; once awake,
* diskfs_il_sq_poll discovers SQ work every iteration without it.  Workers ring
* it on channel (un)registration and only when the commit thread is asleep. */
static void
diskfs_intent_log_wake_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct diskfs_intent_log *il = container_of(doorbell,
                                                struct diskfs_intent_log,
                                                wake_doorbell);

    (void) evpl;

    diskfs_il_service_registrations(il);
    diskfs_il_process_all(il);
} /* diskfs_intent_log_wake_cb */

/* Per-iteration SQ poll: discover and process committed transactions without
* waiting for a doorbell.  Scanning an SQ is just two atomic loads, so this is
* cheap when idle; processing work marks activity to keep us in poll mode. */
static void
diskfs_il_sq_poll(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_intent_log *il = private_data;

    /* Pick up channel (un)registrations without waiting for the wake doorbell:
     * while we stay in continuous poll mode under load the doorbell is starved,
     * so a freshly-registered channel would otherwise never enter channels[] and
     * its commits would never be seen.  Gated on a cheap atomic so the common
     * (no-change) case avoids the registration_lock. */
    if (__atomic_load_n(&il->reg_dirty, __ATOMIC_ACQUIRE)) {
        diskfs_il_service_registrations(il);
    }

    if (diskfs_il_process_all(il)) {
        evpl_activity(evpl);
    }
} /* diskfs_il_sq_poll */

/* Entering poll mode -> the commit thread is awake and polling the SQs. */
static void
diskfs_il_poll_enter(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_intent_log *il = private_data;

    (void) evpl;
    __atomic_store_n(&il->awake, 1, __ATOMIC_SEQ_CST);
} /* diskfs_il_poll_enter */

/* Leaving poll mode (about to block).  Publish "asleep", then re-scan once: a
 * submitter that enqueued before observing awake=0 is picked up here; one that
 * enqueues afterward observes awake=0 and rings the wake doorbell.  Dekker-style
 * handshake -- the awake flag and the SQ tails are all seq_cst, so a wakeup can
 * never be lost. */
static void
diskfs_il_poll_exit(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_intent_log *il = private_data;

    __atomic_store_n(&il->awake, 0, __ATOMIC_SEQ_CST);

    if (diskfs_il_has_sq_work(il)) {
        __atomic_store_n(&il->awake, 1, __ATOMIC_SEQ_CST);
        diskfs_il_process_all(il);
        evpl_activity(evpl);   /* stay awake; the loop will not block this pass */
    }
} /* diskfs_il_poll_exit */

/* Push one txn onto this worker's IL submission queue.  Returns 1 on success,
 * 0 if the SQ is full.  The completion fires later from the CQ doorbell. */
static int
diskfs_iq_try_submit(
    struct diskfs_thread  *thread,
    struct diskfs_txn     *txn,
    diskfs_txn_commit_cb_t cb,
    void                  *private_data)
{
    struct diskfs_shared     *shared = thread->shared;
    struct diskfs_iq_channel *ch     = thread->iq_channel;
    struct diskfs_iq_entry   *slot;
    uint32_t                  tail, head;

    tail = __atomic_load_n(&ch->sq.tail, __ATOMIC_RELAXED);
    head = __atomic_load_n(&ch->sq.head, __ATOMIC_ACQUIRE);

    if (tail - head >= DISKFS_IQ_RING_SIZE) {
        return 0;
    }

    slot               = &ch->sq.entries[tail & DISKFS_IQ_RING_MASK];
    slot->txn          = txn;
    slot->cb           = cb;
    slot->private_data = private_data;
    slot->status       = 0;
    prometheus_stopwatch_start(&slot->enqueue_time);

    /* Seq-cst so this store is ordered before the awake load below; pairs with
     * the commit thread's diskfs_il_poll_exit handshake. */
    __atomic_store_n(&ch->sq.tail, tail + 1, __ATOMIC_SEQ_CST);

    /* The commit thread polls every channel's SQ each loop iteration while it is
     * awake, so the wake doorbell is only needed to rouse it once it has gone to
     * sleep.  Skip the eventfd write in the common (awake) case. */
    if (!__atomic_load_n(&shared->intent_log.awake, __ATOMIC_SEQ_CST)) {
        evpl_ring_doorbell(&shared->intent_log.wake_doorbell);
    }
    return 1;
} /* diskfs_iq_try_submit */

/* Resume commits parked on the SQ-full FIFO, in order, as space allows.  Called
 * from the CQ doorbell, which fires once the intent-log thread has consumed SQ
 * entries (freeing space) and posted completions on this worker. */
static void
diskfs_iq_resume_commit_waiters(struct diskfs_thread *thread)
{
    struct diskfs_txn *txn;

    while ((txn = thread->commit_wait_head)) {
        if (!diskfs_iq_try_submit(thread, txn, txn->commit_cb, txn->commit_private)) {
            break;     /* SQ full again; the next CQ doorbell resumes us */
        }

        thread->commit_wait_head = txn->commit_wait_next;
        if (!thread->commit_wait_head) {
            thread->commit_wait_tail = NULL;
        }
    }
} /* diskfs_iq_resume_commit_waiters */

/* Drain this worker's completion queue: deliver callbacks, release txns, drop
 * the poll-mode pin once no commits remain outstanding, and resume any commits
 * that parked on a full SQ.  Returns the number of completions drained.  Runs
 * every loop iteration via diskfs_iq_cq_poll (the fast path, while the worker is
 * poll-pinned) and also from the CQ doorbell as a backstop. */
static int
diskfs_iq_drain_cq(struct diskfs_iq_channel *ch)
{
    struct diskfs_thread     *worker  = ch->worker;
    struct diskfs_intent_log *il      = &worker->shared->intent_log;
    uint32_t                  head    = __atomic_load_n(&ch->cq.head, __ATOMIC_RELAXED);
    uint32_t                  tail    = __atomic_load_n(&ch->cq.tail, __ATOMIC_ACQUIRE);
    int                       drained = 0;

    while (head != tail) {
        struct diskfs_iq_entry entry = ch->cq.entries[head & DISKFS_IQ_RING_MASK];
        head++;
        drained++;

        diskfs_metric_time_sample(
            worker->metrics.txn_latency[DISKFS_METRIC_TXN_QUEUE_TO_CALLBACK],
            &entry.enqueue_time);
        diskfs_metric_time_sample(
            worker->metrics.txn_latency[DISKFS_METRIC_TXN_DURABLE_TO_CALLBACK],
            &entry.durable_time);

        /* The txn's logical inode locks were already dropped by the intent
         * log thread (diskfs_iq_process_channel); just deliver completion. */
        entry.cb(entry.txn, entry.status, entry.private_data);
        diskfs_txn_release(entry.txn);

        /* Last outstanding commit done -> release the poll-mode pin taken in
         * diskfs_txn_commit_finish so the worker may sleep again when idle. */
        if (--worker->commits_inflight == 0) {
            evpl_poll_unpin(worker->evpl);
        }
    }

    if (drained > 0) {
        __atomic_store_n(&ch->cq.head, head, __ATOMIC_RELEASE);
        /* Freeing CQ space may let the IL resume a channel it deferred; it sees
         * that on its next poll if awake, so only rouse it if it is asleep. */
        if (!__atomic_load_n(&il->awake, __ATOMIC_SEQ_CST)) {
            evpl_ring_doorbell(&il->wake_doorbell);
        }
    }

    /* The IL has now consumed SQ entries (freeing space), so retry any commits
     * that parked on a full SQ.  No-op when none are waiting. */
    diskfs_iq_resume_commit_waiters(worker);
    return drained;
} /* diskfs_iq_drain_cq */

/* Backstop: drain on the doorbell (rarely rung now -- the worker polls the CQ
 * every iteration while pinned). */
static void
diskfs_iq_cq_doorbell_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct diskfs_iq_channel *ch = container_of(doorbell,
                                                struct diskfs_iq_channel,
                                                cq_doorbell);

    (void) evpl;
    diskfs_iq_drain_cq(ch);
} /* diskfs_iq_cq_doorbell_cb */

/* Per-iteration completion poll.  While a commit is outstanding the worker is
 * pinned in poll mode (diskfs_txn_commit_finish), so completions are reaped
 * within a loop iteration instead of waiting for the doorbell at the spin
 * boundary. */
static void
diskfs_iq_cq_poll(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_iq_channel *ch = private_data;

    if (diskfs_iq_drain_cq(ch)) {
        evpl_activity(evpl);
    }
} /* diskfs_iq_cq_poll */

static void *
diskfs_intent_log_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_intent_log *il     = private_data;
    struct diskfs_shared     *shared = container_of(il, struct diskfs_shared, intent_log);
    int                       i;

    il->evpl                        = evpl;
    il->log_head                    = SM_INTENT_LOG_OFFSET;
    il->log_tail                    = SM_INTENT_LOG_OFFSET;
    il->log_seq                     = 0;
    il->redo_inflight               = 0;
    il->redo_inflight_high_water    = 0;
    il->push_outstanding            = 0;
    il->push_outstanding_high_water = 0;
    il->log_used_bytes_high_water   = 0;
    il->sync                        = !shared->unsafe_async;

    /* In-order retirement ring + cross-thread hand-off ring to the push thread. */
    il->retire      = calloc(DISKFS_RETIRE_RING_SIZE, sizeof(*il->retire));
    il->retire_head = 0;
    il->retire_tail = 0;
    il->handoff     = calloc(DISKFS_HANDOFF_RING_SIZE, sizeof(*il->handoff));
    __atomic_store_n(&il->handoff_head, 0, __ATOMIC_RELAXED);
    __atomic_store_n(&il->handoff_tail, 0, __ATOMIC_RELAXED);

    diskfs_intent_log_metrics_init(il);
    diskfs_il_commit_metrics(il);

    /* Redo records are written only to the intent-log device. */
    il->log_queue = shared->devices[SM_INTENT_LOG_DEVICE].bdev ?
        evpl_block_open_queue(evpl, shared->devices[SM_INTENT_LOG_DEVICE].bdev) : NULL;

    (void) i;
    evpl_add_doorbell(evpl, &il->wake_doorbell, diskfs_intent_log_wake_cb);

    /* Poll all channel SQs every loop iteration (cheap atomic loads) so commit
     * pickup never waits for the wake doorbell; the doorbell only rouses us when
     * we have actually gone to sleep.  Start "awake" -- we are about to spin. */
    /* awake must track the loop's poll_mode, which starts at 0 (the thread is
     * event-driven until activity pulls it into poll mode).  Initialising this
     * to 1 would lie -- a submitter would skip the wake doorbell believing we
     * are polling while we are actually asleep, stranding the commit until some
     * unrelated doorbell happens to wake us.  poll_enter/poll_exit own it from
     * here; it is 0 (asleep) until the first poll_enter. */
    __atomic_store_n(&il->awake, 0, __ATOMIC_SEQ_CST);
    __atomic_store_n(&il->reg_dirty, 1, __ATOMIC_SEQ_CST);   /* service any channels registered before we started polling */
    il->sq_poll = evpl_add_poll(evpl, diskfs_il_poll_enter, diskfs_il_poll_exit,
                                diskfs_il_sq_poll, il);

    __atomic_store_n(&il->ready, 1, __ATOMIC_RELEASE);
    return il;
} /* diskfs_intent_log_thread_init */

static void
diskfs_intent_log_thread_shutdown(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_intent_log *il = private_data;

    /* Workers are gone, so no new SQ work arrives.  Drain every in-flight redo
     * write and retire (hand off, in order) every record to the push thread --
     * which is still running and will flush them home before it is itself shut
     * down (the push thread is destroyed after this one). */
    while (il->redo_inflight || il->retire_head != il->retire_tail) {
        evpl_continue(evpl);
    }

    evpl_remove_poll(evpl, il->sq_poll);
    evpl_remove_doorbell(evpl, &il->wake_doorbell);
    if (il->log_queue) {
        evpl_block_close_queue(evpl, il->log_queue);
    }
    free(il->retire);
} /* diskfs_intent_log_thread_shutdown */

static void *
diskfs_il_push_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_intent_log *il     = private_data;
    struct diskfs_shared     *shared = container_of(il, struct diskfs_shared, intent_log);
    uint32_t                  cap;
    int                       i;

    il->push_evpl  = evpl;
    il->push_head  = NULL;
    il->push_tail  = NULL;
    il->ready_head = NULL;
    il->ready_tail = NULL;
    il->pfree      = NULL;

    /* Pending map: one bucket budget per distinct block the log can hold. */
    cap            = diskfs_il_pow2((SM_INTENT_LOG_SIZE / DISKFS_BLOCK_SIZE) * 2);
    il->phash_mask = cap - 1;
    il->phash      = calloc(cap, sizeof(*il->phash));

    /* Home writes can target any device. */
    il->home_queue = calloc(shared->num_devices, sizeof(*il->home_queue));
    for (i = 0; i < shared->num_devices; i++) {
        il->home_queue[i] = shared->devices[i].bdev ?
            evpl_block_open_queue(evpl, shared->devices[i].bdev) : NULL;
    }

    evpl_add_doorbell(evpl, &il->push_doorbell, diskfs_il_push_doorbell_cb);
    __atomic_store_n(&il->push_ready, 1, __ATOMIC_RELEASE);
    return il;
} /* diskfs_il_push_thread_init */

static void
diskfs_il_push_thread_shutdown(
    struct evpl *evpl,
    void        *private_data)
{
    struct diskfs_intent_log *il     = private_data;
    struct diskfs_shared     *shared = container_of(il, struct diskfs_shared, intent_log);
    struct diskfs_pending    *p;
    int                       i;

    /* The commit thread is already gone, so no new hand-offs arrive.  Drain
     * every handed-off record home and trim the log fully (clean unmount => no
     * replay needed). */
    while (il->handoff_head != __atomic_load_n(&il->handoff_tail, __ATOMIC_ACQUIRE) ||
           il->push_head || il->push_outstanding) {
        diskfs_il_push_doorbell_cb(evpl, &il->push_doorbell);
        evpl_continue(evpl);
    }

    evpl_remove_doorbell(evpl, &il->push_doorbell);

    for (i = 0; i < shared->num_devices; i++) {
        if (il->home_queue[i]) {
            evpl_block_close_queue(evpl, il->home_queue[i]);
        }
    }
    free(il->home_queue);

    while ((p = il->pfree)) {
        il->pfree = p->rnext;
        free(p);
    }
    free(il->phash);
} /* diskfs_il_push_thread_shutdown */

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
    struct diskfs_thread *thread = txn->thread;

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
        uint64_t                 blocks = 0;

        for (tb = txn->blocks; tb; tb = tb->next) {
            evpl_iovec_clone(&tb->snap, &tb->block->iov);
            blocks++;
        }
        diskfs_metric_counter_inc(thread->metrics.txn[0]);
        diskfs_metric_counter_add(thread->metrics.txn[1], blocks);
        diskfs_metric_counter_add(thread->metrics.txn[2], blocks * DISKFS_BLOCK_SIZE);
        diskfs_metric_histogram_sample(thread->metrics.txn_blocks, blocks);
        diskfs_metric_histogram_sample(thread->metrics.txn_bytes,
                                       blocks * DISKFS_BLOCK_SIZE);
    }

    /* Hand the txn -> intent log thread via this worker's SQ.  The intent log
     * thread drops the txn's logical inode locks when it processes the entry
     * (see diskfs_iq_process_channel); these are logical locks tracked in the
     * cache, not pthread mutexes, so holding them while the commit is parked
     * on a full SQ cannot deadlock (conflicting ops simply park as waiters).
     * The completion callback fires from the CQ doorbell back on this worker.
     *
     * SQ-full backpressure parks the commit on this worker's FIFO and returns;
     * the CQ doorbell (diskfs_iq_resume_commit_waiters) retries it once the IL
     * thread frees SQ space.  We never spin evpl_continue here -- doing so
     * would re-enter this worker's event loop from within a callback that is
     * itself running under evpl_continue (e.g. the close-thread sweep), which
     * recurses without bound.  If commits are already parked, queue behind them
     * to preserve submission order. */
    /* This commit is entering the intent-log pipeline (submitted now or parked
    * for later).  Pin the worker in poll mode so it keeps draining its CQ
    * every iteration (diskfs_iq_cq_poll) and never sleeps with a commit
    * outstanding; released in diskfs_iq_drain_cq when the commit completes. */
    if (thread->commits_inflight++ == 0) {
        evpl_poll_pin(thread->evpl);
    }

    if (!thread->commit_wait_head &&
        diskfs_iq_try_submit(thread, txn, cb, private_data)) {
        return;
    }

    txn->commit_cb        = cb;
    txn->commit_private   = private_data;
    txn->commit_wait_next = NULL;

    if (thread->commit_wait_tail) {
        thread->commit_wait_tail->commit_wait_next = txn;
    } else {
        thread->commit_wait_head = txn;
    }
    thread->commit_wait_tail = txn;
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
        io->queue[i] = shared->devices[i].bdev ?
            evpl_block_open_queue(io->evpl, shared->devices[i].bdev) : NULL;
    }
    return io;
} /* diskfs_mount_io_open */

static void
diskfs_mount_io_close(struct diskfs_mount_io *io)
{
    int i;

    for (i = 0; i < io->shared->num_devices; i++) {
        if (io->queue[i]) {
            evpl_block_close_queue(io->evpl, io->queue[i]);
        }
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
    struct diskfs_mount_io *io = user;
    uint64_t                maxreq;
    uint64_t                done = 0;

    if (device_id >= (uint32_t) io->shared->num_devices ||
        !io->queue[device_id]) {
        return -1;
    }
    maxreq = io->shared->devices[device_id].max_request_size;
    if (maxreq == 0) {
        return -1;
    }

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
    struct diskfs_mount_io *io = user;
    uint64_t                maxreq;
    uint64_t                done = 0;

    if (device_id >= (uint32_t) io->shared->num_devices ||
        !io->queue[device_id]) {
        return -1;
    }
    maxreq = io->shared->devices[device_id].max_request_size;
    if (maxreq == 0) {
        return -1;
    }

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
                         !io->shared->unsafe_async, diskfs_mount_io_complete, &w);
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
diskfs_mount_io_write_many(
    void                     *user,
    const struct sm_io_write *writes,
    uint32_t                  count)
{
    struct diskfs_mount_io      *io = user;
    struct diskfs_mount_io_wait *waits;
    struct evpl_iovec           *iovs;
    uint32_t                     i, done = 0;
    int                          rc = 0;

    if (count == 0) {
        return 0;
    }

    waits = calloc(count, sizeof(*waits));
    iovs  = calloc(count, sizeof(*iovs));
    if (!waits || !iovs) {
        free(waits);
        free(iovs);
        return -1;
    }

    for (i = 0; i < count; i++) {
        struct diskfs_device *dev;

        if (writes[i].device_id >= (uint32_t) io->shared->num_devices ||
            !io->queue[writes[i].device_id]) {
            rc    = -1;
            count = i;
            goto out;
        }
        dev = &io->shared->devices[writes[i].device_id];

        if (dev->max_request_size == 0 || writes[i].length > dev->max_request_size) {
            rc    = -1;
            count = i;
            goto out;
        }

        evpl_iovec_alloc(io->evpl, writes[i].length, DISKFS_BLOCK_SIZE, 1, 0,
                         &iovs[i]);
        memcpy(iovs[i].data, writes[i].buf, writes[i].length);
        evpl_block_write(io->evpl, io->queue[writes[i].device_id], &iovs[i],
                         1, writes[i].offset, !io->shared->unsafe_async,
                         diskfs_mount_io_complete, &waits[i]);
    }

    while (done < count) {
        evpl_continue(io->evpl);
        done = 0;
        for (i = 0; i < count; i++) {
            if (waits[i].done) {
                done++;
            }
        }
    }

    for (i = 0; i < count; i++) {
        if (waits[i].status) {
            rc = -1;
            break;
        }
    }

 out:
    for (i = 0; i < count; i++) {
        evpl_iovec_release(io->evpl, &iovs[i]);
    }
    free(iovs);
    free(waits);
    return rc;
} /* diskfs_mount_io_write_many */

static int
diskfs_mount_io_flush(
    void    *user,
    uint32_t device_id)
{
    struct diskfs_mount_io     *io = user;
    struct diskfs_mount_io_wait w  = { 0, 0 };

    if (device_id >= (uint32_t) io->shared->num_devices ||
        !io->queue[device_id]) {
        return -1;
    }

    evpl_block_flush(io->evpl, io->queue[device_id], diskfs_mount_io_complete, &w);
    while (!w.done) {
        evpl_continue(io->evpl);
    }
    return w.status ? -1 : 0;
} /* diskfs_mount_io_flush */

/* Discard (deallocate) a device byte range, pumping the mount-time evpl to
 * completion.  Used at mkfs to clear whole devices.  Backends without native
 * discard treat it as a no-op success. */
static int
diskfs_mount_io_discard(
    void    *user,
    uint32_t device_id,
    uint64_t offset,
    uint64_t length)
{
    struct diskfs_mount_io     *io = user;
    struct diskfs_mount_io_wait w  = { 0, 0 };

    if (device_id >= (uint32_t) io->shared->num_devices ||
        !io->queue[device_id]) {
        return -1;
    }

    evpl_block_discard(io->evpl, io->queue[device_id], offset, length,
                       diskfs_mount_io_complete, &w);
    while (!w.done) {
        evpl_continue(io->evpl);
    }
    return w.status ? -1 : 0;
} /* diskfs_mount_io_discard */

static struct sm_io
diskfs_mount_sm_io(struct diskfs_mount_io *io)
{
    struct sm_io smio = {
        .read       = diskfs_mount_io_read,
        .write      = diskfs_mount_io_write,
        .write_many = diskfs_mount_io_write_many,
        .flush      = diskfs_mount_io_flush,
        .user       = io,
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

SYMBOL_EXPORT struct chimera_vfs_module vfs_diskfs;

/*
 * Decode a hex string (e.g. "deadbeef" or "de:ad:be:ef") into up to `max`
 * bytes; returns the number of bytes decoded.  Used for the block-mode device
 * id and SIMPLE-volume signature, which are configured as hex.
 */
static uint32_t
diskfs_parse_hex(
    const char *hex,
    uint8_t    *out,
    uint32_t    max)
{
    uint32_t n  = 0;
    int      hi = -1;

    if (!hex) {
        return 0;
    }
    for (; *hex && n < max; hex++) {
        int v;

        if (*hex >= '0' && *hex <= '9') {
            v = *hex - '0';
        } else if (*hex >= 'a' && *hex <= 'f') {
            v = *hex - 'a' + 10;
        } else if (*hex >= 'A' && *hex <= 'F') {
            v = *hex - 'A' + 10;
        } else {
            continue;   /* skip separators (':', '-', spaces) */
        }
        if (hi < 0) {
            hi = v;
        } else {
            out[n++] = (uint8_t) ((hi << 4) | v);
            hi       = -1;
        }
    }
    return n;
} /* diskfs_parse_hex */

static void *
diskfs_init(
    const char                *cfgdata,
    struct prometheus_metrics *metrics)
{
    struct diskfs_shared       *shared = calloc(1, sizeof(*shared));
    struct diskfs_device       *device;
    enum evpl_block_protocol_id protocol_id;
    const char                 *protocol_name, *device_path;
    char                       *device0_path = NULL;
    int                         i, fd, rc;
    struct stat                 st;
    int64_t                     size;
    struct sm_device_cfg       *dev_cfg;
    json_t                     *cfg, *devices_cfg, *device_cfg;
    json_error_t                json_error;
    int                         initialize;


    cfg = json_loads(cfgdata, 0, &json_error);

    chimera_diskfs_abort_if(cfg == NULL, "Error parsing config: %s", json_error.text);

    devices_cfg = json_object_get(cfg, "devices");

    shared->num_devices  = json_array_size(devices_cfg);
    shared->devices      = calloc(shared->num_devices, sizeof(*shared->devices));
    shared->device_paths = calloc(shared->num_devices, sizeof(char *));

    json_array_foreach(devices_cfg, i, device_cfg)
    {
        const char *role_name;

        device     = &shared->devices[i];
        device->id = i;

        protocol_name = json_string_value(json_object_get(device_cfg, "type"));
        device_path   = json_string_value(json_object_get(device_cfg, "path"));
        size          = json_integer_value(json_object_get(device_cfg, "size"));
        role_name     = json_string_value(json_object_get(device_cfg, "role"));

        /* A "remote" device models pNFS-block data storage outside this system:
         * diskfs tracks its free space but never opens it.  Its size, stable
         * deviceid and SIMPLE-volume signature come from config; its AG logs are
         * relocated onto the local metadata device by the allocator. */
        if (role_name && strcmp(role_name, "remote") == 0) {
            json_t *sig_cfg  = json_object_get(device_cfg, "signature");
            json_t *scsi_cfg = json_object_get(device_cfg, "scsi");

            chimera_diskfs_abort_if(i == 0, "device 0 must be a local metadata device");

            device->role             = SM_DEV_REMOTE;
            device->bdev             = NULL;
            device->size             = size;
            device->max_request_size = 0;
            shared->device_paths[i]  = strdup(device_path ? device_path : "");
            snprintf(device->name, sizeof(device->name), "%s",
                     device_path ? device_path : "remote");

            diskfs_parse_hex(json_string_value(json_object_get(device_cfg, "deviceid")),
                             device->deviceid, SM_DEVICEID_SIZE);
            /* A remote device carries either a SIMPLE-volume content signature
             * (block layout, RFC 5663) or a SCSI VPD-0x83 designator (SCSI
             * layout, RFC 8154); the share-level mode flag selects which. */
            if (json_is_object(sig_cfg)) {
                device->sig_offset = json_integer_value(json_object_get(sig_cfg, "offset"));
                device->sig_len    = diskfs_parse_hex(
                    json_string_value(json_object_get(sig_cfg, "bytes")),
                    device->sig, SM_SIG_MAX);
            }
            if (json_is_object(scsi_cfg)) {
                const char *dtype = json_string_value(
                    json_object_get(scsi_cfg, "designator_type"));
                const char *cset = json_string_value(
                    json_object_get(scsi_cfg, "code_set"));

                /* Default binary NAA -- the common SAS/FC/iSCSI WWID form. */
                device->scsi_desig_type = 3; /* NAA */
                if (dtype && strcmp(dtype, "eui64") == 0) {
                    device->scsi_desig_type = 2;
                } else if (dtype && strcmp(dtype, "t10") == 0) {
                    device->scsi_desig_type = 1;
                }
                device->scsi_code_set  = (cset && strcmp(cset, "ascii") == 0) ? 2 : 1;
                device->scsi_desig_len = diskfs_parse_hex(
                    json_string_value(json_object_get(scsi_cfg, "id")),
                    device->scsi_desig, sizeof(device->scsi_desig));
                device->scsi_pr_key = json_integer_value(
                    json_object_get(scsi_cfg, "pr_key"));
            }

            chimera_diskfs_info(
                "Remote data device %s size %lu sig_len %u scsi_desig_len %u",
                device->name, device->size, device->sig_len,
                device->scsi_desig_len);
            continue;
        }

        device->role            = SM_DEV_LOCAL;
        shared->device_paths[i] = strdup(device_path);
        snprintf(device->name, sizeof(device->name), "%s", device_path);

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

    /* Opt-in unsafe async I/O: when set, block writes are submitted without
     * FUA/sync, so diskfs runs lighter at the cost of crash safety.  Off by
     * default; tests that do not exercise crash recovery enable it to run more
     * efficiently. */
    initialize           = json_object_get(cfg, "initialize") != NULL;
    shared->unsafe_async = json_is_true(json_object_get(cfg, "unsafe_async"));
    shared->noatime      = json_is_true(json_object_get(cfg, "noatime"));
    {
        /* Deferred-mtime coalescing window (ms in config); 0 disables it. */
        json_t *mdv = json_object_get(cfg, "mtime_defer_ms");
        shared->mtime_defer_us = mdv ? (uint64_t) json_integer_value(mdv) * 1000 : 1000000;
    }
    /* Opt-in pNFS block / SCSI layout mode: diskfs sources RFC 5663 block or
     * RFC 8154 SCSI layouts and keeps file data on remote (data-only) devices.
     * Both share the same remote-device data path and allocator; they differ
     * only in how the client identifies the disk (content signature vs. SCSI
     * hardware designator) and the encoded layout type. */
    shared->block_layout       = json_is_true(json_object_get(cfg, "block_layout"));
    shared->scsi_layout        = json_is_true(json_object_get(cfg, "scsi_layout"));
    shared->block_cache_blocks = (uint32_t) json_integer_value(
        json_object_get(cfg, "block_cache_blocks"));

    chimera_diskfs_abort_if(shared->block_layout && shared->scsi_layout,
                            "block_layout and scsi_layout are mutually exclusive");

    /* Layout-sourcing mode and orchestrated flex-files are mutually exclusive
     * per module (the NFS server routes on CAP_LAYOUT_SOURCE before CAP_LAYOUT).
     * A diskfs module is loaded once per daemon with one config, so switch the
     * module's advertised layout capability here: a sourcing mode SOURCEs block
     * or SCSI layouts, otherwise we persist the orchestrated flex blob. */
    if (shared->block_layout || shared->scsi_layout) {
        vfs_diskfs.capabilities &= ~(uint64_t) CHIMERA_VFS_CAP_LAYOUT;
        vfs_diskfs.capabilities |= CHIMERA_VFS_CAP_LAYOUT_SOURCE |
            (shared->scsi_layout ? CHIMERA_VFS_CAP_LAYOUT_CLASS_SCSI
                                 : CHIMERA_VFS_CAP_LAYOUT_CLASS_BLOCK);
    }
    shared->inode_cache_inodes = (uint32_t) json_integer_value(
        json_object_get(cfg, "inode_cache_inodes"));

    json_decref(cfg);


    pthread_mutex_init(&shared->lock, NULL);
    diskfs_metrics_init(shared, metrics);

    /* Decide mkfs vs clean-mount vs crash-recovery from the superblock, just as
     * a real filesystem would:
     *   - initialize present -> mkfs, regardless of any existing contents.
     *   - valid + CLEAN      -> previous instance unmounted cleanly: reload
     *                           the persisted free-space map and mount.
     *   - valid + !CLEAN     -> crash: replay the intent log to home, then
     *                           mount the now-consistent image.
     *   - no/garbage         -> fail; callers must opt in to mkfs.
     */
    {
        struct sm_superblock    sb;
        int                     have_sb;
        int                     mode; /* 0 = mkfs, 1 = clean mount, 2 = recover */
        struct diskfs_mount_io *mio  = diskfs_mount_io_open(shared);
        struct sm_io            smio = diskfs_mount_sm_io(mio);

        have_sb = space_map_read_superblock(&smio, &sb) == 0;

        if (initialize) {
            if (have_sb) {
                chimera_diskfs_info(
                    "initialize=true: ignoring existing superblock and formatting fresh");
            }
            mode         = 0;
            shared->fsid = chimera_rand64();
        } else if (have_sb && (sb.flags & SM_SB_CLEAN)) {
            mode         = 1;
            shared->fsid = sb.fsid;
        } else if (have_sb) {
            mode         = 2;
            shared->fsid = sb.fsid;
        } else {
            chimera_diskfs_abort(
                "diskfs superblock missing or invalid; specify initialize to format");
        }

        dev_cfg = calloc(shared->num_devices, sizeof(*dev_cfg));
        for (i = 0; i < shared->num_devices; i++) {
            struct diskfs_device *dv = &shared->devices[i];

            dev_cfg[i].size = dv->size;
            dev_cfg[i].role = dv->role;
            if (dv->role == SM_DEV_REMOTE) {
                memcpy(dev_cfg[i].deviceid, dv->deviceid, SM_DEVICEID_SIZE);
                dev_cfg[i].sig_offset = dv->sig_offset;
                dev_cfg[i].sig_len    = dv->sig_len;
                memcpy(dev_cfg[i].sig, dv->sig, dv->sig_len);
            }
        }
        shared->space_map = space_map_create(dev_cfg, shared->num_devices);
        free(dev_cfg);

        /* On a persistent remount the relocated-log map is recomputed from the
         * device cfg; it must match what the superblock recorded or the remote
         * AG logs would be read from the wrong offsets. */
        if (mode != 0) {
            chimera_diskfs_abort_if(
                sb.num_remote_devices != shared->space_map->num_remote_devices ||
                sb.remote_log_offset != shared->space_map->remote_log_offset ||
                sb.remote_log_size != shared->space_map->remote_log_size,
                "device configuration changed since last mount (remote-log region "
                "mismatch): refusing to mount to avoid corrupting relocated AG logs");
        }

        if (mode == 2) {
            chimera_diskfs_info("superblock not clean: running crash recovery");
            diskfs_recover_log(shared, mio);
        }

        /* Reload the persisted free-space map.  The allocator is authoritative
         * for future writes; after recovery, mounting without it would let new
         * allocations overlap live metadata/data until namespace-walk rebuild
         * exists. */
        if (mode != 0 &&
            space_map_load(shared->space_map, &smio) != 0) {
            if (mode == 1) {
                chimera_diskfs_abort("space-map reload failed");
            } else {
                chimera_diskfs_abort(
                    "post-recovery space-map reload failed; refusing unsafe mount "
                    "until namespace-walk reconstruction is implemented");
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

        /* Fresh format: deallocate every device before writing any metadata,
         * so the filesystem starts on a clean slate.  This drops the drives'
         * stale FTL mappings (resetting GC/wear state) and makes cold-cache
         * behavior reproducible run to run.  Deallocate is only a hint and the
         * post-discard read value is unspecified, but diskfs never relies on
         * device-provided zeros (unwritten extents read as zeros from the
         * b+tree), so a device that ignores it is harmless.  Backends without
         * native discard treat it as a no-op success. */
        if (mode == 0) {
            for (i = 0; i < shared->num_devices; i++) {
                if (!shared->devices[i].bdev) {
                    continue;
                }
                if (diskfs_mount_io_discard(mio, i, 0,
                                            shared->devices[i].size) != 0) {
                    chimera_diskfs_info(
                        "device %d: full-device discard failed or unsupported; "
                        "continuing format", i);
                }
            }
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

        /* mkfs: write an initial condensed AG-log base so each slot has a valid
         * header before any runtime delta is journaled -- otherwise a crash
         * right after format would leave the allocator log unreadable. */
        if (mode == 0) {
            rc = space_map_persist(shared->space_map, &smio);
            chimera_diskfs_abort_if(rc != 0, "Failed to persist initial space map");
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
    shared->intent_log.push_ready   = 0;
    shared->intent_log.shutdown     = 0;
    shared->intent_log.commit_alive = 1;
    shared->intent_log.num_channels = 0;
    shared->intent_log.pending_head = NULL;
    pthread_mutex_init(&shared->intent_log.registration_lock, NULL);

    /* Commit thread first: it allocates the cross-thread hand-off ring the
     * push thread consumes, and opens the intent-log device queue. */
    shared->intent_log.thread = evpl_thread_create(NULL,
                                                   diskfs_intent_log_thread_init,
                                                   diskfs_intent_log_thread_shutdown,
                                                   &shared->intent_log);
    while (!__atomic_load_n(&shared->intent_log.ready, __ATOMIC_ACQUIRE)) {
        /* spin briefly */
    }

    shared->intent_log.push_thread = evpl_thread_create(NULL,
                                                        diskfs_il_push_thread_init,
                                                        diskfs_il_push_thread_shutdown,
                                                        &shared->intent_log);
    while (!__atomic_load_n(&shared->intent_log.push_ready, __ATOMIC_ACQUIRE)) {
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
    /* World-writable fresh root: with VFS-layer ADD_FILE/ADD_SUBDIRECTORY
     * enforcement a root-owned 0755 root would refuse all creation by non-root
     * clients on this engine-authoritative backend (matches memfs/cairn).
     * Subdirs are still created owned by their creator with 0755. */
    inode->mode           = S_IFDIR | 0777;
    inode->atime_sec      = now.tv_sec;
    inode->atime_nsec     = now.tv_nsec;
    inode->mtime_sec      = now.tv_sec;
    inode->mtime_nsec     = now.tv_nsec;
    inode->ctime_sec      = now.tv_sec;
    inode->ctime_nsec     = now.tv_nsec;
    inode->btime_sec      = now.tv_sec;
    inode->btime_nsec     = now.tv_nsec;
    inode->dos_attributes = 0;

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

        oin->size           = 4096;
        oin->space_used     = 4096;
        oin->nlink          = 1;
        oin->mode           = S_IFDIR | 0700;
        oin->atime_sec      = now.tv_sec;
        oin->atime_nsec     = now.tv_nsec;
        oin->mtime_sec      = now.tv_sec;
        oin->mtime_nsec     = now.tv_nsec;
        oin->ctime_sec      = now.tv_sec;
        oin->ctime_nsec     = now.tv_nsec;
        oin->btime_sec      = now.tv_sec;
        oin->btime_nsec     = now.tv_nsec;
        oin->dos_attributes = 0;
        oin->parent_inum    = DISKFS_ORPHAN_INUM;
        oin->parent_gen     = oin->gen;

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
    shared->root_inum       = inode->inum;
    shared->root_gen        = inode->gen;
    shared->orphans_scanned = 1;

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

    /* Shut down the intent-log threads before tearing down anything they
     * might still touch.  Worker threads have already unregistered their
     * channels via the unregister handshake at this point.  Order matters: the
     * commit thread first (it drains all redo writes and hands every record to
     * the push thread), then the push thread (it flushes every record home and
     * trims the log).  Only then are the shared rings and device-metric arrays
     * safe to free. */
    __atomic_store_n(&shared->intent_log.shutdown, 1, __ATOMIC_RELEASE);
    /* Stop the push thread from ringing the commit thread's wake_doorbell:
     * destroying the commit thread closes that fd, and the push thread (torn
     * down afterwards, to drain what the commit thread handed off) would
     * otherwise abort writing to it. */
    __atomic_store_n(&shared->intent_log.commit_alive, 0, __ATOMIC_RELEASE);
    evpl_thread_destroy(shared->intent_log.thread);
    evpl_thread_destroy(shared->intent_log.push_thread);
    pthread_mutex_destroy(&shared->intent_log.registration_lock);
    free(shared->intent_log.handoff);
    free(shared->intent_log.metrics.block_io_device_ops);
    free(shared->intent_log.metrics.block_io_device_bytes);

    /* Clean unmount: the intent-log thread already drained every logged block
     * to its home location, so persist the free-space map and stamp the
     * superblock CLEAN (with the root + next log seq) so the next mount
     * reloads instead of re-handing-out in-use space.  Driven through the
     * mount-time evpl pump while the devices are still open -- the IL thread
     * (the only other device user) is already gone.  Only mark clean if a root
     * actually exists (an untouched mkfs has nothing to preserve). */
    {
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
        if (shared->devices[i].bdev) {
            evpl_block_close_device(shared->devices[i].bdev);
        }
    }

    diskfs_block_cache_destroy(shared);

    space_map_destroy(shared->space_map);

    for (int i = 0; i < shared->num_devices; i++) {
        free(shared->device_paths[i]);
    }
    free(shared->device_paths);
    free(shared->metrics.block_io_device_ops_series);
    free(shared->metrics.block_io_device_bytes_series);

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

/* ================================================================== */
/* Deferred-mtime flusher: coalesce per-write inode timestamp updates  */
/* ================================================================== */

/* This worker owns inode-cache shards where (shard % num_active_threads) ==
 * thread_id; flushing each shard on a single worker keeps two flushers off the
 * same inode without a cross-thread lock. */
static inline int
diskfs_mtime_owns_shard(
    const struct diskfs_thread *thread,
    uint32_t                    shard)
{
    int n = thread->shared->num_active_threads;

    return (int) (shard % (uint32_t) (n > 0 ? n : 1)) == thread->thread_id;
} /* diskfs_mtime_owns_shard */

/*
 * Claim the oldest deferred-mtime inode from this worker's owned shards that
 * has been dirty at least the coalescing window (or any, when flushing for
 * unmount).  Claiming unlinks it and clears the flag but KEEPS the dirty-pin
 * (now owned by the in-flight flush); a concurrent re-dirty re-queues with a
 * fresh pin.  Round-robins the shard cursor so no shard starves.
 */
static struct diskfs_inode *
diskfs_mtime_flush_pick(struct diskfs_thread *thread)
{
    struct diskfs_shared *shared    = thread->shared;
    uint64_t              period_ns = shared->mtime_defer_us * 1000;
    struct timespec       ts;
    uint64_t              now_ns;
    uint32_t              n;

    if ((uint32_t) thread->thread_id >= DISKFS_INODE_CACHE_SHARDS) {
        return NULL;     /* more workers than shards: this one owns none */
    }

    clock_gettime(CLOCK_REALTIME, &ts);
    now_ns = (uint64_t) ts.tv_sec * 1000000000ULL + ts.tv_nsec;

    for (n = 0; n < DISKFS_INODE_CACHE_SHARDS; n++) {
        uint32_t                   s = thread->mtime_scan_shard;
        struct diskfs_inode_shard *shard;
        struct diskfs_inode       *inode = NULL;

        /* Advance the cursor to this worker's next owned shard. */
        do {
            thread->mtime_scan_shard = (thread->mtime_scan_shard + 1) &
                DISKFS_INODE_CACHE_MASK;
        } while (!diskfs_mtime_owns_shard(thread, thread->mtime_scan_shard));

        if (!diskfs_mtime_owns_shard(thread, s)) {
            continue;
        }

        shard = &shared->inode_cache->shards[s];
        pthread_mutex_lock(&shard->lock);
        if (shard->mdirty_head &&
            (thread->mtime_flush_all ||
             now_ns - shard->mdirty_head->mtime_dirty_since >= period_ns)) {
            inode = shard->mdirty_head;
            diskfs_inode_mtime_unlink_locked(shard, inode);   /* keeps the pin */
        }
        pthread_mutex_unlock(&shard->lock);

        if (inode) {
            return inode;
        }
    }
    return NULL;
} /* diskfs_mtime_flush_pick */

/* Any deferred-mtime work left in this worker's owned shards? (unmount drain) */
static int
diskfs_mtime_any_dirty(struct diskfs_thread *thread)
{
    struct diskfs_shared *shared = thread->shared;
    uint32_t              s;

    for (s = 0; s < DISKFS_INODE_CACHE_SHARDS; s++) {
        if (!diskfs_mtime_owns_shard(thread, s)) {
            continue;
        }
        if (shared->inode_cache->shards[s].mdirty_head) {
            return 1;
        }
    }
    return 0;
} /* diskfs_mtime_any_dirty */

/* Drop the dirty-pin on a flushed inode, re-LRUing it if it became idle. */
static void
diskfs_mtime_flush_drop_pin(
    struct diskfs_thread *thread,
    struct diskfs_inode  *inode)
{
    struct diskfs_inode_shard *shard = diskfs_inode_shard(thread->shared, inode->inum);

    pthread_mutex_lock(&shard->lock);
    --inode->refcnt;
    if (diskfs_inode_idle(inode) && !inode->on_lru) {
        diskfs_inode_lru_push_tail(shard, inode);
    }
    pthread_mutex_unlock(&shard->lock);
} /* diskfs_mtime_flush_drop_pin */

struct diskfs_mtime_flush {
    struct diskfs_thread *thread;
    struct diskfs_inode  *inode;
    struct diskfs_txn    *txn;
};

static void diskfs_mtime_flush_kick(
    struct diskfs_thread *thread);

static void
diskfs_mtime_flush_committed_cb(
    struct diskfs_txn *txn,
    int                status,
    void              *priv)
{
    struct diskfs_mtime_flush *f      = priv;
    struct diskfs_thread      *thread = f->thread;

    (void) txn;
    (void) status;

    diskfs_metric_mtime(thread, DISKFS_METRIC_MTIME_FLUSHED);
    diskfs_mtime_flush_drop_pin(thread, f->inode);
    free(f);
    thread->mtime_flushing = 0;
    diskfs_mtime_flush_kick(thread);     /* next eligible inode, if any */
} /* diskfs_mtime_flush_committed_cb */

static void
diskfs_mtime_flush_acquired_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *priv)
{
    struct diskfs_mtime_flush *f      = priv;
    struct diskfs_thread      *thread = f->thread;

    if (status != CHIMERA_VFS_OK) {
        /* Should not happen for a pinned inode; release the pin and move on. */
        diskfs_txn_abort(f->txn);
        diskfs_mtime_flush_drop_pin(thread, inode);
        free(f);
        thread->mtime_flushing = 0;
        diskfs_mtime_flush_kick(thread);
        return;
    }

    /* Pin the home block; commit serializes the current in-memory mtime/ctime
     * (coalescing every write since it went dirty) into it and logs it. */
    diskfs_txn_pin_inode_block(thread, f->txn, inode, 0);
    diskfs_txn_commit(f->txn, diskfs_mtime_flush_committed_cb, f);
} /* diskfs_mtime_flush_acquired_cb */

static void
diskfs_mtime_flush_kick(struct diskfs_thread *thread)
{
    struct diskfs_mtime_flush *f;
    struct diskfs_inode       *inode;

    if (thread->mtime_flushing) {
        return;     /* one flush txn at a time per worker */
    }

    inode = diskfs_mtime_flush_pick(thread);
    if (!inode) {
        return;     /* nothing ready; the timer re-kicks next tick */
    }

    thread->mtime_flushing = 1;
    f                      = malloc(sizeof(*f));
    f->thread              = thread;
    f->inode               = inode;
    f->txn                 = diskfs_txn_begin(thread, DISKFS_TXN_WRITE);
    diskfs_inode_acquire_pinned(thread, f->txn, inode, DISKFS_INODE_LOCK_WRITE,
                                diskfs_mtime_flush_acquired_cb, f);
} /* diskfs_mtime_flush_kick */

static void
diskfs_mtime_flush_timer_cb(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct diskfs_thread *thread = container_of(timer, struct diskfs_thread, mtime_timer);

    (void) evpl;
    diskfs_mtime_flush_kick(thread);
} /* diskfs_mtime_flush_timer_cb */

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
        /* Remote (pNFS data) devices have no local handle: leave queue NULL. */
        thread->queue[i] = shared->devices[i].bdev ?
            evpl_block_open_queue(evpl, shared->devices[i].bdev) : NULL;
    }

    thread->shared = shared;
    thread->evpl   = evpl;

    /* Allocate this worker's intent-log channel and register the CQ
     * doorbell on the worker's own evpl. */
    thread->iq_channel         = calloc(1, sizeof(*thread->iq_channel));
    thread->iq_channel->worker = thread;
    thread->commits_inflight   = 0;
    evpl_add_doorbell(evpl, &thread->iq_channel->cq_doorbell,
                      diskfs_iq_cq_doorbell_cb);
    /* Reap intent-log completions every loop iteration (the worker is pinned in
     * poll mode while a commit is outstanding); the doorbell is only a backstop. */
    thread->cq_poll = evpl_add_poll(evpl, NULL, NULL, diskfs_iq_cq_poll,
                                    thread->iq_channel);

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
    diskfs_thread_metrics_init(thread);

    /* Deferred-mtime coalescing flusher: scan from this worker's first owned
     * shard, and fire the flush driver every coalescing window. */
    thread->mtime_scan_shard = (uint32_t) thread->thread_id & DISKFS_INODE_CACHE_MASK;
    if (shared->mtime_defer_us > 0) {
        evpl_add_timer(evpl, &thread->mtime_timer, diskfs_mtime_flush_timer_cb,
                       shared->mtime_defer_us);
    }

    /* Hand the channel to the intent log thread via the pending list. */
    pthread_mutex_lock(&shared->intent_log.registration_lock);
    thread->iq_channel->next_pending = shared->intent_log.pending_head;
    shared->intent_log.pending_head  = thread->iq_channel;
    pthread_mutex_unlock(&shared->intent_log.registration_lock);

    /* Publish "registration pending" before the doorbell: the commit thread
     * services this from its per-iteration poll (reg_dirty) when awake, or from
     * the wake doorbell when asleep. */
    __atomic_store_n(&shared->intent_log.reg_dirty, 1, __ATOMIC_SEQ_CST);
    evpl_ring_doorbell(&shared->intent_log.wake_doorbell);

    return thread;
} /* diskfs_thread_init */

static void
diskfs_thread_destroy(void *private_data)
{
    struct diskfs_thread *thread = private_data;
    struct diskfs_shared *shared = thread->shared;

    /* Flush every deferred-mtime inode this worker owns so the latest timestamps
     * are durable before teardown (clean unmount => no replay).  The intent-log
     * thread is still alive to complete the flush txns.  Stop the periodic timer
     * and drive the flusher directly, ignoring the age gate. */
    if (shared->mtime_defer_us > 0) {
        evpl_remove_timer(thread->evpl, &thread->mtime_timer);
    }
    thread->mtime_flush_all = 1;
    diskfs_mtime_flush_kick(thread);
    while (thread->mtime_flushing || diskfs_mtime_any_dirty(thread)) {
        diskfs_mtime_flush_kick(thread);
        evpl_continue(thread->evpl);
    }

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
        if (!thread->queue[i]) {
            continue;
        }
        evpl_block_close_queue(thread->evpl, thread->queue[i]);
    }

    /* No txn at thread teardown; the unused reservation tail returns to the
     * in-memory free set and is captured by the condense at clean unmount. */
    space_map_thread_cache_return(shared->space_map, NULL, &thread->space_cache);
    space_map_thread_cache_return(shared->space_map, NULL, &thread->data_space_cache);

    /* Unregister the intent-log channel.  Caller must have quiesced all
     * in-flight VFS ops on this thread first. */
    if (thread->iq_channel) {
        struct diskfs_iq_channel *ch = thread->iq_channel;

        __atomic_store_n(&ch->unregister_requested, 1, __ATOMIC_RELEASE);
        __atomic_store_n(&shared->intent_log.reg_dirty, 1, __ATOMIC_SEQ_CST);
        evpl_ring_doorbell(&shared->intent_log.wake_doorbell);

        while (!__atomic_load_n(&ch->unregister_done, __ATOMIC_ACQUIRE)) {
            /* spin */
        }

        evpl_remove_poll(thread->evpl, thread->cq_poll);
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

    free(thread->metrics.block_io_device_ops);
    free(thread->metrics.block_io_device_bytes);
    free(thread->queue);
    free(thread);
} /* diskfs_thread_destroy */

/* ------------------------------------------------------------------ */
/* ACLs: one serialized NFSv4/Windows ACL per inode, stored as the single
 * DISKFS_REC_ACL record in the inode's b+tree (mirrors the memfs/cairn
 * native-ACL stores).  Read/written with the sync b+tree wrappers, the same
 * convention the pNFS-layout record (read in diskfs_map_attrs) and the xattr
 * records (written from their inode callbacks) already use on a loaded
 * inode. */

/* The biggest ACL whose serialized form still fits one b+tree record. */
#define DISKFS_ACL_REC_MAX \
        (DISKFS_BT_ROOT_CAP - sizeof(struct diskfs_bt_node_hdr) - sizeof(struct diskfs_bt_lslot))
#define DISKFS_ACL_REC_MAX_ACES \
        (((DISKFS_ACL_REC_MAX) -CHIMERA_ACL_SERIAL_HDR) / CHIMERA_ACL_SERIAL_ACE)

static const struct diskfs_bt_key diskfs_acl_key = {
    .type = DISKFS_REC_ACL, .subkey = 0
};

/*
 * Decode a serialized ACL record (serial[0..len), len<0 == no record) into a
 * per-thread scratch chimera_acl and point attr->va_acl at it.  When no ACL is
 * stored the ACL is synthesised from the mode, so a caller always sees an ACL
 * for an inode (identical to memfs/cairn).  The scratch is valid through the
 * synchronous completion that consumes it.
 */
static void
diskfs_acl_decode_into(
    struct chimera_vfs_attrs *attr,
    const uint8_t            *serial,
    int                       len,
    uint32_t                  mode)
{
    static __thread uint8_t scratch[sizeof(struct chimera_acl) +
                                    CHIMERA_ACL_MAX_ACES * sizeof(struct chimera_ace)];
    struct chimera_acl     *dst = (struct chimera_acl *) scratch;

    if (len < 0 ||
        chimera_acl_deserialize((const char *) serial, len, dst,
                                CHIMERA_ACL_MAX_ACES) < 0) {
        chimera_acl_from_mode(mode, dst, CHIMERA_ACL_MAX_ACES);
    }

    attr->va_acl       = dst;
    attr->va_set_mask |= CHIMERA_VFS_ATTR_ACL;
} /* diskfs_acl_decode_into */

/* Mode/ACL-aware access check against a loaded diskfs inode (mirror of
 * memfs/cairn): build the canonical attrs (the stored ACL, or one synthesised
 * from the mode) and consult the shared access engine.  Uses the synchronous
 * b+tree ACL read, valid while the inode's leaf is cached. */
static int
diskfs_inode_access(
    struct diskfs_thread          *thread,
    struct diskfs_inode           *inode,
    const struct chimera_vfs_cred *cred,
    uint32_t                       requested)
{
    struct chimera_vfs_attrs attr;
    uint8_t                  serial[DISKFS_ACL_REC_MAX];
    int                      len;

    attr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
        CHIMERA_VFS_ATTR_GID;
    attr.va_mode = inode->mode;
    attr.va_uid  = inode->uid;
    attr.va_gid  = inode->gid;

    len = diskfs_bt_lookup_exact(thread, inode, &diskfs_acl_key,
                                 serial, sizeof(serial));
    diskfs_acl_decode_into(&attr, serial, len, inode->mode);

    return chimera_vfs_access_allowed(&attr, cred, requested);
} /* diskfs_inode_access */

/*
 * Persist `acl` as inode's ACL record (replacing any existing one), or remove
 * the record when `acl` is NULL/empty (revert to mode-derived).  The b+tree
 * insert aborts on a duplicate key, so an existing record is removed first --
 * the same lookup/remove/insert sequence the xattr writes use.  All sync: the
 * inode (and, for a fresh create, its empty tree) is resident in this write
 * txn, matching the xattr-record convention.
 */
static void
diskfs_acl_store(
    struct diskfs_thread     *thread,
    struct diskfs_txn        *txn,
    struct diskfs_inode      *inode,
    const struct chimera_acl *acl)
{
    uint8_t probe[1];

    /* Remove any existing record (insert won't overwrite a duplicate key).
     * The probe length is irrelevant -- the lookup reports the full record
     * length even when the copy is truncated, so >= 0 means present. */
    if (diskfs_bt_lookup_exact(thread, inode, &diskfs_acl_key, probe,
                               sizeof(probe)) >= 0) {
        diskfs_bt_remove(thread, txn, inode, &diskfs_acl_key);
    }

    if (acl && acl->num_aces && acl->num_aces <= DISKFS_ACL_REC_MAX_ACES) {
        uint8_t buf[DISKFS_ACL_REC_MAX];
        int     len = chimera_acl_serialize(acl, buf, sizeof(buf));

        if (len >= 0) {
            diskfs_bt_insert(thread, txn, inode, &diskfs_acl_key, buf, len);
        }
    }
} /* diskfs_acl_store */

/*
 * Seed a freshly-created child's ACL (mirrors memfs/cairn).  Precedence:
 *   1. parent has ACEs inheritable for the child's type -> store the inherited
 *      ACL and re-derive the child mode from it (Windows inheritance);
 *   2. otherwise, for an SMB create (windows_default) -> store a Windows-style
 *      owner-full-control default DACL, leaving the POSIX mode intact;
 *   3. otherwise (NFS/POSIX create) -> no record, child stays mode-derived.
 * The child is brand new (empty resident b+tree), so the writes are sync; the
 * parent's ACL is read with the sync wrapper like diskfs_map_attrs does.
 */
static void
diskfs_inherit_acl(
    struct diskfs_thread     *thread,
    struct diskfs_txn        *txn,
    struct diskfs_inode      *child,
    struct diskfs_inode      *parent,
    const struct chimera_acl *new_acl,
    int                       windows_default)
{
    int                 is_dir = S_ISDIR(child->mode);
    uint16_t            want   = CHIMERA_ACE_FLAG_FILE_INHERIT |
        (is_dir ? CHIMERA_ACE_FLAG_DIR_INHERIT : 0);
    uint8_t             pserial[DISKFS_ACL_REC_MAX];
    int                 plen;
    uint8_t             pbuf[sizeof(struct chimera_acl) +
                             DISKFS_ACL_REC_MAX_ACES * sizeof(struct chimera_ace)];
    struct chimera_acl *parent_acl = (struct chimera_acl *) pbuf;
    int                 has_inh    = 0;

    /* An explicit ACL supplied at create (e.g. an SMB SD via SecD) takes
     * precedence over inheritance and the windows_default below.  The caller
     * snapshots new_acl from set_attr BEFORE diskfs_apply_attrs() runs, since
     * apply_attrs resets va_set_mask down to the bits it applied (ACL isn't
     * one of them).  Passing the pointer explicitly avoids relying on a
     * possibly-uninitialized set_attr->va_acl in callers that don't always
     * set ATTR_ACL (e.g., NFS3 creates). */
    if (new_acl && new_acl->num_aces) {
        diskfs_acl_store(thread, txn, child, new_acl);
        child->mode = (child->mode & S_IFMT) | chimera_acl_to_mode(new_acl);
        return;
    }

    plen = diskfs_bt_lookup_exact(thread, parent, &diskfs_acl_key,
                                  pserial, sizeof(pserial));

    if (plen < 0 ||
        chimera_acl_deserialize((const char *) pserial, plen, parent_acl,
                                DISKFS_ACL_REC_MAX_ACES) < 0) {
        parent_acl = NULL;
    }

    if (parent_acl) {
        for (unsigned i = 0; i < parent_acl->num_aces; i++) {
            if (parent_acl->aces[i].flags & want) {
                has_inh = 1;
                break;
            }
        }
    }

    if (has_inh) {
        uint8_t             tbuf[sizeof(struct chimera_acl) +
                                 DISKFS_ACL_REC_MAX_ACES * sizeof(struct chimera_ace)];
        struct chimera_acl *tmp = (struct chimera_acl *) tbuf;
        int                 n   = chimera_acl_inherit(parent_acl, is_dir,
                                                      child->mode & 07777, tmp,
                                                      DISKFS_ACL_REC_MAX_ACES);

        if (n > 0) {
            diskfs_acl_store(thread, txn, child, tmp);
            child->mode = (child->mode & S_IFMT) | chimera_acl_to_mode(tmp);
            return;
        }
        /* Nothing actually inherited: fall through to the default below. */
    }

    if (windows_default) {
        uint8_t             dbuf[sizeof(struct chimera_acl) +
                                 4 * sizeof(struct chimera_ace)];
        struct chimera_acl *def = (struct chimera_acl *) dbuf;

        if (chimera_acl_default_acl(child->mode & 07777, def, 4) > 0) {
            diskfs_acl_store(thread, txn, child, def);
        }
    }
} /* diskfs_inherit_acl */

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
     * Opaque pNFS layout state (flex-files), persisted verbatim for the NFS
     * server as the single DISKFS_REC_PNFS record in this inode's b+tree.  A
     * file that carries a layout blob has its data on the data server, never
     * local extents, so the record always lives in the resident embedded root
     * -- the sync lookup never suspends (same invariant as the symlink record).
     */
    if (attr->va_req_mask & CHIMERA_VFS_ATTR_PNFS_LAYOUT) {
        struct diskfs_bt_key pnfs_key = { .type = DISKFS_REC_PNFS, .subkey = 0 };
        int                  len      = diskfs_bt_lookup_exact(thread, inode, &pnfs_key,
                                                               attr->va_pnfs,
                                                               CHIMERA_VFS_PNFS_LAYOUT_MAX);

        if (len >= 0) {
            attr->va_set_mask |= CHIMERA_VFS_ATTR_PNFS_LAYOUT;
            attr->va_pnfs_len  = len;
        }
    }

    /*
     * Native ACL: the single DISKFS_REC_ACL record (or, when absent, an ACL
     * synthesised from the mode) so SMB/NFS callers always see one.  Read with
     * the sync wrapper, like the pNFS record above and the xattr records.
     * Unlike pNFS/symlink the ACL coexists with dirents/extents, so on a large
     * inode whose tree has split it may not sit in the resident embedded root;
     * the sync read then relies on that leaf being cached (it aborts on a true
     * cache miss).  An async ACL read is the hardening follow-up, shared with
     * the broader "sync b+tree op under block-cache eviction" concern.
     */
    if (attr->va_req_mask & CHIMERA_VFS_ATTR_ACL) {
        uint8_t serial[DISKFS_ACL_REC_MAX];
        int     len = diskfs_bt_lookup_exact(thread, inode, &diskfs_acl_key,
                                             serial, sizeof(serial));

        diskfs_acl_decode_into(attr, serial, len, inode->mode);
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
        } else if (attr->va_atime.tv_nsec != CHIMERA_VFS_TIME_OMIT) {
            inode->atime_sec  = attr->va_atime.tv_sec;
            inode->atime_nsec = attr->va_atime.tv_nsec;
        }
    }

    if (set_mask & CHIMERA_VFS_ATTR_MTIME) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_MTIME;
        if (attr->va_mtime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
            inode->mtime_sec  = now.tv_sec;
            inode->mtime_nsec = now.tv_nsec;
        } else if (attr->va_mtime.tv_nsec != CHIMERA_VFS_TIME_OMIT) {
            inode->mtime_sec  = attr->va_mtime.tv_sec;
            inode->mtime_nsec = attr->va_mtime.tv_nsec;
        }
    }

    if (set_mask & CHIMERA_VFS_ATTR_BTIME) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_BTIME;
        if (attr->va_btime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
            inode->btime_sec  = now.tv_sec;
            inode->btime_nsec = now.tv_nsec;
        } else if (attr->va_btime.tv_nsec != CHIMERA_VFS_TIME_OMIT) {
            inode->btime_sec  = attr->va_btime.tv_sec;
            inode->btime_nsec = attr->va_btime.tv_nsec;
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
        attr->va_set_mask |= CHIMERA_VFS_ATTR_CTIME;
        if (attr->va_ctime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
            inode->ctime_sec  = now.tv_sec;
            inode->ctime_nsec = now.tv_nsec;
        } else if (attr->va_ctime.tv_nsec != CHIMERA_VFS_TIME_OMIT) {
            inode->ctime_sec  = attr->va_ctime.tv_sec;
            inode->ctime_nsec = attr->va_ctime.tv_nsec;
        }
    } else {
        inode->ctime_sec  = now.tv_sec;
        inode->ctime_nsec = now.tv_nsec;
    }

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

    /* POSIX: a successful (f)truncate marks the last data modification time.
     * Bump mtime unless the caller supplied an explicit mtime, or is an
     * AUTH_ATTR (SMB/Windows) caller managing the write time itself. */
    int                            bump_mtime = !(request->setattr.set_attr->va_set_mask &
                                                  CHIMERA_VFS_ATTR_MTIME) &&
        request->cred->flavor != CHIMERA_VFS_AUTH_ATTR;

    diskfs_apply_attrs(inode, request->setattr.set_attr);

    if (bump_mtime) {
        struct timespec now;
        clock_gettime(CLOCK_REALTIME, &now);
        inode->mtime_sec  = now.tv_sec;
        inode->mtime_nsec = now.tv_nsec;
    }

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
                .flags         = p->ext_iter.flags,
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

/*
 * Completion for a setattr that also persisted a pNFS layout blob: the
 * DISKFS_REC_PNFS record is now in the b+tree (resident root, so the insert
 * never split), so map the post attrs and commit.
 */
static void
diskfs_setattr_pnfs_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;

    diskfs_bt_op_free(thread, op);

    if (unlikely(result < 0)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EIO);
        return;
    }

    diskfs_map_attrs(thread, &request->setattr.r_post_attr, p->inode_stash[0]);
    diskfs_op_ok(request, p->txn);
} /* diskfs_setattr_pnfs_cb */

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
    /* diskfs_apply_attrs() rewrites set_attr->va_set_mask (resets to ATOMIC,
     * re-adds only the scalar bits), dropping the ACL bit -- capture the
     * caller's original mask first. */
    uint64_t                       orig_mask = request->setattr.set_attr->va_set_mask;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    if ((request->setattr.set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) &&
        !S_ISREG(inode->mode)) {
        diskfs_op_fail(request, p->txn,
                       S_ISDIR(inode->mode) ?
                       CHIMERA_VFS_EISDIR : CHIMERA_VFS_EINVAL);
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

    /* Persist the opaque pNFS layout blob as this inode's single PNFS record. */
    if (request->setattr.set_attr->va_set_mask & CHIMERA_VFS_ATTR_PNFS_LAYOUT) {
        struct diskfs_bt_key key      = { .type = DISKFS_REC_PNFS, .subkey = 0 };
        uint32_t             blob_len = request->setattr.set_attr->va_pnfs_len;

        if (blob_len > CHIMERA_VFS_PNFS_LAYOUT_MAX) {
            blob_len = CHIMERA_VFS_PNFS_LAYOUT_MAX;
        }

        p->inode_stash[0] = inode;
        op                = diskfs_bt_op_alloc(thread);
        if (diskfs_bt_insert_async(op, thread, p->txn, inode, &key,
                                   request->setattr.set_attr->va_pnfs, blob_len,
                                   diskfs_setattr_pnfs_cb, request)) {
            diskfs_setattr_pnfs_cb(op, op->result, request);
        }
        return;
    }

    /*
     * ACL coherence (mirrors memfs/cairn): an explicit ACL set is persisted and
     * the mode re-derived; a bare chmod regenerates the special-who ACEs of any
     * stored ACL while preserving named entries.  diskfs_acl_store does the
     * lookup/remove/insert synchronously, the same convention the xattr-record
     * writes use after loading the inode.
     */
    if (orig_mask & CHIMERA_VFS_ATTR_ACL) {
        const struct chimera_acl *acl = request->setattr.set_attr->va_acl;

        if (acl && acl->num_aces) {
            inode->mode = (inode->mode & S_IFMT) | chimera_acl_to_mode(acl);
        }
        diskfs_acl_store(thread, p->txn, inode, acl);
    } else if (orig_mask & CHIMERA_VFS_ATTR_MODE) {
        uint8_t serial[DISKFS_ACL_REC_MAX];
        int     slen = diskfs_bt_lookup_exact(thread, inode, &diskfs_acl_key,
                                              serial, sizeof(serial));

        if (slen >= 0) {
            uint8_t             obuf[sizeof(struct chimera_acl) +
                                     DISKFS_ACL_REC_MAX_ACES * sizeof(struct chimera_ace)];
            uint8_t             nbuf[sizeof(struct chimera_acl) +
                                     DISKFS_ACL_REC_MAX_ACES * sizeof(struct chimera_ace)];
            struct chimera_acl *old_acl = (struct chimera_acl *) obuf;
            struct chimera_acl *new_acl = (struct chimera_acl *) nbuf;

            if (chimera_acl_deserialize((const char *) serial, slen, old_acl,
                                        DISKFS_ACL_REC_MAX_ACES) >= 0 &&
                chimera_acl_chmod(old_acl, inode->mode, new_acl,
                                  DISKFS_ACL_REC_MAX_ACES) >= 0) {
                diskfs_acl_store(thread, p->txn, inode, new_acl);
            }
        }
    }

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

    /* Resume any inode drains left pending by a crash during mount, before
     * the export becomes usable.  Fresh bootstrap creates an empty orphan list
     * and marks it scanned. */
    if (unlikely(!shared->orphans_scanned)) {
        diskfs_orphan_scan(thread);
    }
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

    inode->size           = 4096;
    inode->space_used     = 4096;
    inode->uid            = request->cred->uid;
    inode->gid            = request->cred->gid;
    inode->nlink          = 2;
    inode->mode           = S_IFDIR | 0755;
    inode->atime_sec      = now.tv_sec;
    inode->atime_nsec     = now.tv_nsec;
    inode->mtime_sec      = now.tv_sec;
    inode->mtime_nsec     = now.tv_nsec;
    inode->ctime_sec      = now.tv_sec;
    inode->ctime_nsec     = now.tv_nsec;
    inode->btime_sec      = now.tv_sec;
    inode->btime_nsec     = now.tv_nsec;
    inode->dos_attributes = 0;

    inode->parent_inum = parent->inum;
    inode->parent_gen  = parent->gen;

    /* Snapshot any explicit ACL pointer BEFORE diskfs_apply_attrs() rewrites
     * va_set_mask and drops the ATTR_ACL bit. */
    const struct chimera_acl *new_acl_mkdir =
        (request->mkdir_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_ACL)
        ? request->mkdir_at.set_attr->va_acl : NULL;

    diskfs_apply_attrs(inode, request->mkdir_at.set_attr);

    /* Seed the new directory's ACL (inherited, or a Windows default DACL for
     * SMB creates) before mapping attrs so r_attr reflects any inherited mode
     * and carries the freshly-stored ACL.  An explicit ACL in set_attr (e.g.
     * an SMB SD via SecD) takes precedence. */
    diskfs_inherit_acl(thread, p->txn, inode, parent,
                       new_acl_mkdir,
                       request->cred->flavor == CHIMERA_VFS_AUTH_ATTR);

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

    inode->size           = 0;
    inode->space_used     = 0;
    inode->uid            = request->cred->uid;
    inode->gid            = request->cred->gid;
    inode->nlink          = 1;
    inode->rdev           = 0;
    inode->atime_sec      = now.tv_sec;
    inode->atime_nsec     = now.tv_nsec;
    inode->mtime_sec      = now.tv_sec;
    inode->mtime_nsec     = now.tv_nsec;
    inode->ctime_sec      = now.tv_sec;
    inode->ctime_nsec     = now.tv_nsec;
    inode->btime_sec      = now.tv_sec;
    inode->btime_nsec     = now.tv_nsec;
    inode->dos_attributes = 0;

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
        chimera_diskfs_error("remove_at lost dirent after lookup name=%.*s hash=%lu parent=%lu",
                             request->remove_at.namelen,
                             request->remove_at.name,
                             request->remove_at.name_hash,
                             parent->inum);
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
        /* Removing one of several hard links changes the surviving inode's
         * link count, which is a status change: bump its ctime. */
        if (inode->nlink > 0) {
            inode->ctime_sec  = now.tv_sec;
            inode->ctime_nsec = now.tv_nsec;
        }
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

    if (S_ISDIR(inode->mode) && !diskfs_dir_is_empty(thread, inode)) {
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
diskfs_readdir_finish(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p     = request->plugin_data;
    struct diskfs_inode           *inode = p->inode_stash[0];

    diskfs_map_attrs(p->thread, &request->readdir.r_dir_attr, inode);
    diskfs_op_ok(request, p->txn);
} /* diskfs_readdir_finish */

static void
diskfs_readdir_complete(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p = request->plugin_data;

    /* If a synchronous iteration loop is driving the walk (see
     * diskfs_readdir_iter_step), only flag completion: the loop calls
     * diskfs_readdir_finish() once it unwinds.  Finishing inline here would
     * commit the txn and free the request out from under the active loop. */
    if (p->rd_looping) {
        p->rd_done = 1;
        return;
    }

    diskfs_readdir_finish(request);
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
    struct diskfs_bt_op           *op;

    /* A re-entrant call from a step that completed synchronously: don't
    * recurse, just ask the active loop to advance to the next entry. */
    if (p->rd_looping) {
        p->rd_advance = 1;
        return;
    }

    p->rd_looping = 1;
    do {
        p->rd_advance = 0;
        op            = diskfs_bt_op_alloc(thread);
        if (diskfs_dir_next_async(op, thread, inode, p->rd_from_hash, &op->found_key,
                                  p->rec_scratch, sizeof(p->rec_scratch),
                                  diskfs_readdir_next_cb, request)) {
            diskfs_readdir_next_cb(op, op->result, request);
        }
        /* rd_advance: the step finished inline; loop for the next entry.
         * rd_done:    the walk completed (terminal dirent or full buffer).
         * neither:    the step suspended on block I/O; its completion will
         *             re-enter this function with rd_looping clear. */
    } while (p->rd_advance && !p->rd_done);

    p->rd_looping = 0;

    if (p->rd_done) {
        diskfs_readdir_finish(request);
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
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
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

    p->thread     = thread;
    p->txn        = diskfs_txn_begin(thread, DISKFS_TXN_READ);
    p->rd_looping = 0;
    p->rd_advance = 0;
    p->rd_done    = 0;

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

    if ((request->open_fh.flags & CHIMERA_VFS_OPEN_DIRECTORY) &&
        !S_ISDIR(inode->mode)) {
        /* A directory open of a symlink is NFS4ERR_SYMLINK, not NOTDIR (e.g.
         * LOOKUP/LOOKUPP through a symlink); other non-dirs are NOTDIR. */
        diskfs_op_fail(request, p->txn,
                       S_ISLNK(inode->mode) ? CHIMERA_VFS_ESYMLINK : CHIMERA_VFS_ENOTDIR);
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

    /* diskfs is CAP_OPEN_FILE_REQUIRED: every open (inferred or not) yields a
     * cached handle matched by a diskfs_close, so always pin the inode and stash
     * the real pointer in vfs_private.  read/write reuse it (and close releases
     * the pin); there is no throwaway/synthetic open_at for this backend. */
    inode->refcnt++;
    request->open_at.r_vfs_private = (uint64_t) inode;

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

    /* A symlink as the final component under O_NOFOLLOW: a *data* open
     * (POSIX open(O_NOFOLLOW)) must fail with ELOOP, but an O_PATH-style open
     * (SMB FILE_OPEN_REPARSE_POINT, i.e. O_PATH|O_NOFOLLOW) wants a handle to
     * the link itself so the caller can read its attributes / security
     * descriptor / reparse data -- so fall through and open the symlink inode
     * in that case (mirrors memfs and the linux backend). */
    if (S_ISLNK(inode->mode) &&
        (request->open_at.flags & CHIMERA_VFS_OPEN_NOFOLLOW) &&
        !(request->open_at.flags & CHIMERA_VFS_OPEN_PATH)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ELOOP);
        return;
    }

    if ((request->open_at.flags & CHIMERA_VFS_OPEN_DIRECTORY) &&
        !S_ISDIR(inode->mode)) {
        diskfs_op_fail(request, p->txn,
                       S_ISLNK(inode->mode) ? CHIMERA_VFS_ESYMLINK : CHIMERA_VFS_ENOTDIR);
        return;
    }

    /* Access is enforced at the VFS layer (the credential-keyed gate in
     * chimera_vfs_read/write and the protocol's own create-time check), which
     * is ACL-aware and honors each protocol's access semantics; diskfs does not
     * re-check here -- a coarse mode-based read/write test would mis-handle SMB
     * opens that carry only control rights (e.g. WRITE_DAC) and not data access,
     * and would ignore a stored ACL that grants more (or less) than the mode.
     * (Mirrors the memfs and cairn backends.) */

    /* Overwrite/supersede disposition: replace the existing file's contents
     * (truncate to zero) and apply the new attributes (including DOS
     * attributes), mirroring memfs/cairn.  The SMB layer conveys the truncate
     * via the OPEN_TRUNCATE flag rather than a SIZE=0 set_attr, so honor both.
     * As with the pre-existing SIZE=0 path, this resets EOF without reclaiming
     * the data extents here (the open flow finishes synchronously); a non-empty
     * file's extents are reclaimed lazily / on the async setattr-truncate path. */
    if (S_ISREG(inode->mode) &&
        ((request->open_at.flags & CHIMERA_VFS_OPEN_TRUNCATE) ||
         ((request->open_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) &&
          request->open_at.set_attr->va_size == 0))) {
        inode->size       = 0;
        inode->space_used = 0;
        diskfs_apply_attrs(inode, request->open_at.set_attr);
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
    /* Signal to the protocol layer that this open created the file (vs.
     * opened an existing one) so the SMB CREATE reply reports the correct
     * Create Action (FILE_CREATED vs FILE_OPENED) for OPEN_IF / SUPERSEDE /
     * OVERWRITE_IF dispositions.  Matches memfs/cairn. */
    request->open_at.r_created = 1;
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

    inode->size           = 0;
    inode->space_used     = 0;
    inode->uid            = request->cred->uid;
    inode->gid            = request->cred->gid;
    inode->nlink          = 1;
    inode->mode           = S_IFREG | 0644;
    inode->atime_sec      = now.tv_sec;
    inode->atime_nsec     = now.tv_nsec;
    inode->mtime_sec      = now.tv_sec;
    inode->mtime_nsec     = now.tv_nsec;
    inode->ctime_sec      = now.tv_sec;
    inode->ctime_nsec     = now.tv_nsec;
    inode->btime_sec      = now.tv_sec;
    inode->btime_nsec     = now.tv_nsec;
    inode->dos_attributes = 0;

    /* Snapshot any explicit ACL pointer BEFORE diskfs_apply_attrs() rewrites
     * va_set_mask and drops the ATTR_ACL bit. */
    const struct chimera_acl *new_acl_open =
        (request->open_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_ACL)
        ? request->open_at.set_attr->va_acl : NULL;

    diskfs_apply_attrs(inode, request->open_at.set_attr);

    /* Seed the new file's ACL (inherited from the parent, or a Windows default
     * DACL for SMB creates) into the child's fresh, resident b+tree.  An
     * explicit ACL in set_attr (e.g. an SMB SD via SecD) takes precedence. */
    diskfs_inherit_acl(thread, p->txn, inode, parent,
                       new_acl_open,
                       request->cred->flavor == CHIMERA_VFS_AUTH_ATTR);

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

        /* Creating a new name requires write+search permission on the parent
         * directory.  Enforce POSIX semantics for AUTH_UNIX callers (root is
         * exempt); SMB/ACL (AUTH_ATTR) callers are authorized by the engine. */
        if (request->cred->flavor == CHIMERA_VFS_AUTH_UNIX &&
            request->cred->uid != 0 &&
            !diskfs_inode_access(thread, p->inode_stash[0], request->cred,
                                 CHIMERA_ACE_APPEND_DATA | CHIMERA_ACE_EXECUTE)) {
            diskfs_op_fail(request, p->txn, CHIMERA_VFS_EACCES);
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
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
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

    inode->size           = 0;
    inode->space_used     = 0;
    inode->uid            = request->cred->uid;
    inode->gid            = request->cred->gid;
    inode->nlink          = 0;
    inode->mode           = S_IFREG | 0644;
    inode->atime_sec      = now.tv_sec;
    inode->atime_nsec     = now.tv_nsec;
    inode->mtime_sec      = now.tv_sec;
    inode->mtime_nsec     = now.tv_nsec;
    inode->ctime_sec      = now.tv_sec;
    inode->ctime_nsec     = now.tv_nsec;
    inode->btime_sec      = now.tv_sec;
    inode->btime_nsec     = now.tv_nsec;
    inode->dos_attributes = 0;

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

static void
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
                         !shared->unsafe_async,
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
                         !shared->unsafe_async,
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

/*
 * Insert a data extent, coalescing it with the immediately-preceding extent
 * when the two are logically + physically contiguous and share the same
 * written-ness -- so a sequential run of writes/allocations collapses into a
 * single extent record instead of one record per write.  The descriptor
 * (ci_off/ci_len/ci_devid/ci_devoff/ci_flags) and the continuation (ci_cont)
 * live in the request private.  bt_insert aborts on a duplicate key, so a
 * merge removes the predecessor and re-inserts the widened extent at its key.
 */
static void
diskfs_ext_put_insert(
    struct chimera_vfs_request *request);

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

static void
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
        request->write.r_sync = CHIMERA_VFS_WRITE_DATASYNC;
    } else {
        /* Record which gate stopped the deferral (in expression order). */
        if (!diskfs_private->inplace_written) {
            diskfs_metric_mtime(thread, DISKFS_METRIC_MTIME_SKIP_NOT_INPLACE);
        } else if (size_grew) {
            diskfs_metric_mtime(thread, DISKFS_METRIC_MTIME_SKIP_SIZE_GREW);
        } else if (request->write.sync == CHIMERA_VFS_WRITE_FILESYNC) {
            diskfs_metric_mtime(thread, DISKFS_METRIC_MTIME_SKIP_FILESYNC);
        }
        request->write.r_sync = CHIMERA_VFS_WRITE_FILESYNC;
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
        uint64_t ee = p->ext_iter.file_offset + p->ext_iter.length;

        if (ee >= aend) {
            p->rmw_suffix_valid = p->rmw_suffix_len;
        } else if (ee > write_end) {
            p->rmw_suffix_valid = ee - write_end;
        } else {
            p->rmw_suffix_valid = 0;
        }

        /* Read the block-aligned device offset that physically holds write_end.
         * The extent maps file write_end to device_offset + (write_end -
         * file_offset); rounding that down to a block gives a 4 KiB-aligned
         * read (required by O_DIRECT/libaio) whose write_end byte sits at
         * index (write_end & 4095) -- so no separate adjust is needed even
         * when the extent starts mid-block (e.g. a punch-created extent whose
         * file_offset/device_offset are unaligned but congruent mod 4096). */
        p->need_suffix_read     = 1;
        p->suffix_device_id     = p->ext_iter.device_id;
        p->suffix_device_offset = (p->ext_iter.device_offset +
                                   (write_end - p->ext_iter.file_offset)) & ~4095ULL;
        p->rmw_suffix_adjust = 0;
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

/* Forward declarations for the write classify -> placement chain. */
static void diskfs_write_classify_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data);
static void diskfs_write_redirect_alloc(
    struct chimera_vfs_request *request);
static void diskfs_write_alloc_resume(
    struct diskfs_thread *thread,
    void                 *arg);
static void diskfs_write_split_start(
    struct chimera_vfs_request *request);

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
        request->write.r_sync   = CHIMERA_VFS_WRITE_FILESYNC;
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

    rc = diskfs_thread_alloc_space(thread, p->txn,
                                   (int64_t) p->rmw_aligned_length,
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

    diskfs_write_prefix_lookup(request);
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


static void
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
        /* Spans the hole: insert tail at hole_end first. */
        op = diskfs_bt_op_alloc(thread);
        if (diskfs_ext_insert_async(op, thread, p->txn, p->inode_stash[0], hole_end,
                                    ee - hole_end, p->ext_iter.device_id,
                                    p->ext_iter.device_offset + (hole_end - es),
                                    p->ext_iter.flags,
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

static void diskfs_allocate_reserve_step(
    struct chimera_vfs_request *request);
static void diskfs_allocate_do_alloc(
    struct chimera_vfs_request *request);

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
    if (chunk > DISKFS_ALLOCATE_MAX_EXTENT) {
        chunk = DISKFS_ALLOCATE_MAX_EXTENT;
    }

    rc = diskfs_thread_alloc_space(thread, p->txn, (int64_t) chunk,
                                   &dev_id, &dev_off,
                                   diskfs_allocate_alloc_resume, request);
    if (rc == SM_AGAIN) {
        return;     /* parked; resume re-drives diskfs_allocate_do_alloc */
    }
    if (rc) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENOSPC);
        return;
    }

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
        diskfs_allocate_reserve_step(request);
        return;
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

    inode->size           = request->symlink_at.targetlen;
    inode->space_used     = request->symlink_at.targetlen;
    inode->uid            = request->cred->uid;
    inode->gid            = request->cred->gid;
    inode->nlink          = 1;
    inode->mode           = S_IFLNK | 0755;
    inode->atime_sec      = now.tv_sec;
    inode->atime_nsec     = now.tv_nsec;
    inode->mtime_sec      = now.tv_sec;
    inode->mtime_nsec     = now.tv_nsec;
    inode->ctime_sec      = now.tv_sec;
    inode->ctime_nsec     = now.tv_nsec;
    inode->btime_sec      = now.tv_sec;
    inode->btime_nsec     = now.tv_nsec;
    inode->dos_attributes = 0;

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
    if (S_ISDIR(existing_inode->mode) &&
        !diskfs_dir_is_empty(p->thread, existing_inode)) {
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

    /* POSIX: a successful rename marks the renamed file's status-change time. */
    child->ctime_sec  = now.tv_sec;
    child->ctime_nsec = now.tv_nsec;

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

static inline int
diskfs_xattr_rec_matches(
    const struct diskfs_xattr_rec *rec,
    uint32_t                       rec_len,
    const char                    *name,
    uint32_t                       name_len)
{
    return rec_len >= sizeof(*rec) &&
           rec->name_len == name_len &&
           rec_len >= sizeof(*rec) + rec->name_len + rec->value_len &&
           memcmp(rec->data, name, name_len) == 0;
} /* diskfs_xattr_rec_matches */

static void
diskfs_get_xattr_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_bt_key           key;
    struct diskfs_xattr_rec       *rec;
    int                            len;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    key.type   = DISKFS_REC_XATTR;
    key.subkey = chimera_vfs_hash(request->get_xattr.name,
                                  request->get_xattr.namelen);

    rec = malloc(DISKFS_BT_NODE_CAP);
    len = diskfs_bt_lookup_exact(p->thread, inode, &key,
                                 rec, DISKFS_BT_NODE_CAP);
    if (len < 0 ||
        !diskfs_xattr_rec_matches(rec, len, request->get_xattr.name,
                                  request->get_xattr.namelen)) {
        free(rec);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENODATA);
        return;
    }

    if (rec->value_len > request->get_xattr.value_maxlen) {
        free(rec);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ERANGE);
        return;
    }

    memcpy(request->get_xattr.value, rec->data + rec->name_len,
           rec->value_len);
    request->get_xattr.r_value_len = rec->value_len;
    free(rec);
    diskfs_op_ok(request, p->txn);
} /* diskfs_get_xattr_inode_cb */

static void
diskfs_get_xattr(
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
                              diskfs_get_xattr_inode_cb, request);
} /* diskfs_get_xattr */

static void
diskfs_set_xattr_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_bt_key           key;
    struct diskfs_xattr_rec       *old_rec;
    struct diskfs_xattr_rec       *new_rec;
    uint32_t                       rec_len;
    int                            old_len;
    struct timespec                now;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    rec_len = sizeof(*new_rec) + request->set_xattr.namelen +
        request->set_xattr.value_len;
    if (rec_len > DISKFS_XATTR_REC_MAX) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EFBIG);
        return;
    }

    diskfs_map_attrs(p->thread, &request->set_xattr.r_pre_attr, inode);

    key.type   = DISKFS_REC_XATTR;
    key.subkey = chimera_vfs_hash(request->set_xattr.name,
                                  request->set_xattr.namelen);

    old_rec = malloc(DISKFS_BT_NODE_CAP);
    old_len = diskfs_bt_lookup_exact(p->thread, inode, &key,
                                     old_rec, DISKFS_BT_NODE_CAP);
    if (old_len >= 0 &&
        !diskfs_xattr_rec_matches(old_rec, old_len, request->set_xattr.name,
                                  request->set_xattr.namelen)) {
        free(old_rec);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EEXIST);
        return;
    }

    if (old_len >= 0) {
        if (request->set_xattr.option == CHIMERA_VFS_XATTR_CREATE) {
            free(old_rec);
            diskfs_op_fail(request, p->txn, CHIMERA_VFS_EEXIST);
            return;
        }
        diskfs_bt_remove(p->thread, p->txn, inode, &key);
    } else if (request->set_xattr.option == CHIMERA_VFS_XATTR_REPLACE) {
        free(old_rec);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENODATA);
        return;
    }
    free(old_rec);

    new_rec            = malloc(rec_len);
    new_rec->name_len  = request->set_xattr.namelen;
    new_rec->value_len = request->set_xattr.value_len;
    memcpy(new_rec->data, request->set_xattr.name, request->set_xattr.namelen);
    memcpy(new_rec->data + request->set_xattr.namelen,
           request->set_xattr.value, request->set_xattr.value_len);

    diskfs_bt_insert(p->thread, p->txn, inode, &key, new_rec, rec_len);
    free(new_rec);

    clock_gettime(CLOCK_REALTIME, &now);
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;

    diskfs_map_attrs(p->thread, &request->set_xattr.r_post_attr, inode);
    diskfs_op_ok(request, p->txn);
} /* diskfs_set_xattr_inode_cb */

static void
diskfs_set_xattr(
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
                              diskfs_set_xattr_inode_cb, request);
} /* diskfs_set_xattr */

static void
diskfs_list_xattrs_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_bt_key           key     = { .type = DISKFS_REC_XATTR, .subkey = 0 };
    struct diskfs_bt_key           found;
    struct diskfs_xattr_rec       *rec;
    uint8_t                       *buf = request->list_xattrs.buffer;
    uint32_t                       offset = 0, count = 0;
    int                            len;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    rec = malloc(DISKFS_BT_NODE_CAP);
    for (;;) {
        len = diskfs_bt_lookup_ge(p->thread, inode, &key, &found,
                                  rec, DISKFS_BT_NODE_CAP);
        if (len < 0 || found.type != DISKFS_REC_XATTR) {
            break;
        }
        if (len < (int) sizeof(*rec) ||
            len < (int) (sizeof(*rec) + rec->name_len + rec->value_len)) {
            free(rec);
            diskfs_op_fail(request, p->txn, CHIMERA_VFS_EIO);
            return;
        }
        if (offset + rec->name_len + 1 > request->list_xattrs.max_bytes) {
            free(rec);
            diskfs_op_fail(request, p->txn, CHIMERA_VFS_ERANGE);
            return;
        }
        memcpy(buf + offset, rec->data, rec->name_len);
        offset       += rec->name_len;
        buf[offset++] = '\0';
        count++;
        if (found.subkey == UINT64_MAX) {
            break;
        }
        key.subkey = found.subkey + 1;
    }

    request->list_xattrs.r_len    = offset;
    request->list_xattrs.r_count  = count;
    request->list_xattrs.r_eof    = 1;
    request->list_xattrs.r_cookie = 0;
    free(rec);
    diskfs_op_ok(request, p->txn);
} /* diskfs_list_xattrs_inode_cb */

static void
diskfs_list_xattrs(
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
                              diskfs_list_xattrs_inode_cb, request);
} /* diskfs_list_xattrs */

static void
diskfs_remove_xattr_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_bt_key           key;
    struct diskfs_xattr_rec       *rec;
    int                            len;
    struct timespec                now;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }

    diskfs_map_attrs(p->thread, &request->remove_xattr.r_pre_attr, inode);

    key.type   = DISKFS_REC_XATTR;
    key.subkey = chimera_vfs_hash(request->remove_xattr.name,
                                  request->remove_xattr.namelen);

    rec = malloc(DISKFS_BT_NODE_CAP);
    len = diskfs_bt_lookup_exact(p->thread, inode, &key,
                                 rec, DISKFS_BT_NODE_CAP);
    if (len < 0 ||
        !diskfs_xattr_rec_matches(rec, len, request->remove_xattr.name,
                                  request->remove_xattr.namelen)) {
        free(rec);
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_ENODATA);
        return;
    }
    free(rec);

    diskfs_bt_remove(p->thread, p->txn, inode, &key);

    clock_gettime(CLOCK_REALTIME, &now);
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;

    diskfs_map_attrs(p->thread, &request->remove_xattr.r_post_attr, inode);
    diskfs_op_ok(request, p->txn);
} /* diskfs_remove_xattr_inode_cb */

static void
diskfs_remove_xattr(
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
                              diskfs_remove_xattr_inode_cb, request);
} /* diskfs_remove_xattr */

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
#define DISKFS_LAYOUT_GAP_MAX  (256ULL << 20)
#define DISKFS_LAYOUTIOMODE_RW 2     /* LAYOUTIOMODE4_RW */

static void diskfs_get_layout_process(
    struct chimera_vfs_request *request);

static void
diskfs_layout_add_device(
    struct chimera_vfs_request *request,
    struct diskfs_shared       *shared,
    uint32_t                    device_id)
{
    struct diskfs_device             *dev = &shared->devices[device_id];
    struct chimera_vfs_layout_device *ld;
    uint32_t                          i;

    for (i = 0; i < request->get_layout.r_num_devices; i++) {
        if (memcmp(request->get_layout.r_devices[i].deviceid, dev->deviceid,
                   SM_DEVICEID_SIZE) == 0) {
            return;
        }
    }
    if (request->get_layout.r_num_devices >= CHIMERA_VFS_LAYOUT_MAX_DEVICES) {
        return;
    }
    ld = &request->get_layout.r_devices[request->get_layout.r_num_devices++];
    memset(ld, 0, sizeof(*ld));
    memcpy(ld->deviceid, dev->deviceid, SM_DEVICEID_SIZE);

    if (shared->scsi_layout) {
        /* SCSI layout (RFC 8154): the client matches the LU by its VPD-0x83
         * hardware designator; nothing is written to the disk. */
        ld->layout_class    = CHIMERA_VFS_LAYOUT_CLASS_SCSI;
        ld->blk_vol_size    = dev->size;
        ld->scsi_code_set   = dev->scsi_code_set;
        ld->scsi_desig_type = dev->scsi_desig_type;
        ld->scsi_desig_len  = dev->scsi_desig_len;
        memcpy(ld->scsi_desig, dev->scsi_desig, dev->scsi_desig_len);
        ld->scsi_pr_key = dev->scsi_pr_key;
    } else {
        /* Block layout (RFC 5663): the client matches the disk by a content
         * signature read at a fixed offset. */
        ld->layout_class   = CHIMERA_VFS_LAYOUT_CLASS_BLOCK;
        ld->blk_vol_size   = dev->size;
        ld->blk_sig_offset = dev->sig_offset;
        ld->blk_sig_len    = dev->sig_len;
        memcpy(ld->blk_sig, dev->sig, dev->sig_len);
    }
} /* diskfs_layout_add_device */

/* Append one block segment [file_off, file_off+len) -> (device_id, vol_off). */
static void
diskfs_layout_emit(
    struct chimera_vfs_request *request,
    struct diskfs_shared       *shared,
    uint64_t                    file_off,
    uint64_t                    len,
    uint32_t                    device_id,
    uint64_t                    vol_off,
    uint32_t                    state)
{
    struct chimera_vfs_layout_segment *seg =
        &request->get_layout.r_segments[request->get_layout.r_num_segments++];

    memset(seg, 0, sizeof(*seg));
    seg->offset = file_off;
    seg->length = len;
    seg->iomode = request->get_layout.iomode;
    memcpy(seg->deviceid, shared->devices[device_id].deviceid, SM_DEVICEID_SIZE);
    seg->blk_vol_offset = vol_off;
    seg->blk_state      = state;
    diskfs_layout_add_device(request, shared, device_id);
} /* diskfs_layout_emit */

static void
diskfs_get_layout_finish(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p = request->plugin_data;

    request->get_layout.r_layout_class = p->thread->shared->scsi_layout
        ? CHIMERA_VFS_LAYOUT_CLASS_SCSI : CHIMERA_VFS_LAYOUT_CLASS_BLOCK;
    /* Commits the txn (durably persisting any new extent records) before the
     * layout is handed back; a pure-READ layout has nothing journaled. */
    diskfs_op_ok(request, p->txn);
} /* diskfs_get_layout_finish */

/* Advance the walk to the next extent after the current one, then re-process. */
static void
diskfs_get_layout_walk_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;

    p->loop_have = diskfs_ext_from_op(op, result, &p->ext_iter);
    diskfs_bt_op_free(p->thread, op);
    diskfs_get_layout_process(request);
} /* diskfs_get_layout_walk_cb */

static void
diskfs_get_layout_advance(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_bt_op           *op     = diskfs_bt_op_alloc(thread);

    if (diskfs_ext_next_async(op, thread, p->inode_stash[0], p->ext_iter.file_offset,
                              p->rec_scratch, sizeof(p->rec_scratch),
                              diskfs_get_layout_walk_cb, request)) {
        diskfs_get_layout_walk_cb(op, op->result, request);
    }
} /* diskfs_get_layout_advance */

/* The freshly-allocated gap extent is now in the b+tree (durable on commit);
* emit it as INVALID_DATA and continue (the held ext_iter is still valid). */
static void
diskfs_get_layout_inserted_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_inode           *inode   = p->inode_stash[0];

    diskfs_bt_op_free(thread, op);

    if (unlikely(result < 0)) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EIO);
        return;
    }

    inode->space_used += p->rmw_aligned_length;

    diskfs_layout_emit(request, thread->shared, p->rmw_aligned_start,
                       p->rmw_aligned_length, (uint32_t) p->rmw_device_id,
                       p->rmw_device_offset, CHIMERA_VFS_BLOCK_INVALID_DATA);

    p->loop_off += p->rmw_aligned_length;
    diskfs_get_layout_process(request);
} /* diskfs_get_layout_inserted_cb */

/* Allocate one gap extent at p->loop_off (length p->rmw_aligned_length) and
 * insert it.  Re-driven by the allocator on SM_AGAIN. */
static void
diskfs_get_layout_do_alloc(
    struct diskfs_thread *thread,
    void                 *arg)
{
    struct chimera_vfs_request    *request = arg;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_bt_op           *op;
    uint64_t                       dev_id, dev_off;
    int                            rc;

    rc = diskfs_thread_alloc_space(thread, p->txn, (int64_t) p->rmw_aligned_length,
                                   &dev_id, &dev_off,
                                   diskfs_get_layout_do_alloc, request);
    if (rc == SM_AGAIN) {
        return;     /* parked; re-driven into this function */
    }
    if (rc != 0) {
        diskfs_op_fail(request, p->txn, rc);
        return;
    }

    p->rmw_device_id     = dev_id;
    p->rmw_device_offset = dev_off;
    p->rmw_aligned_start = p->loop_off;

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_ext_insert_async(op, thread, p->txn, p->inode_stash[0],
                                p->loop_off, p->rmw_aligned_length,
                                (uint32_t) dev_id, dev_off, 0,
                                diskfs_get_layout_inserted_cb, request)) {
        diskfs_get_layout_inserted_cb(op, op->result, request);
    }
} /* diskfs_get_layout_do_alloc */

static void
diskfs_get_layout_process(struct chimera_vfs_request *request)
{
    struct diskfs_request_private *p      = request->plugin_data;
    struct diskfs_thread          *thread = p->thread;
    struct diskfs_shared          *shared = thread->shared;
    struct diskfs_inode           *inode  = p->inode_stash[0];
    struct diskfs_extent          *ext    = &p->ext_iter;
    int                            rw     = request->get_layout.iomode == DISKFS_LAYOUTIOMODE_RW;
    uint64_t                       size   = inode->size;
    uint64_t                       end    = p->loop_pos;

    /* Done: range covered, or no more segment slots (client re-LAYOUTGETs). */
    if (p->loop_off >= end ||
        request->get_layout.r_num_segments >= request->get_layout.max_segments) {
        diskfs_get_layout_finish(request);
        return;
    }

    if (p->loop_have && ext->file_offset <= p->loop_off &&
        ext->file_offset + ext->length > p->loop_off) {
        /* Backed run at the cursor. */
        uint64_t ext_end = ext->file_offset + ext->length;
        uint64_t seg_end = ext_end < end ? ext_end : end;
        uint64_t vol_off = ext->device_offset + (p->loop_off - ext->file_offset);

        if (rw && p->loop_off < size && seg_end > size) {
            /* Straddles committed size: written part then unwritten part. */
            uint64_t split = size;
            diskfs_layout_emit(request, shared, p->loop_off, split - p->loop_off,
                               ext->device_id, vol_off, CHIMERA_VFS_BLOCK_READ_WRITE_DATA);
            diskfs_layout_emit(request, shared, split, seg_end - split,
                               ext->device_id, vol_off + (split - p->loop_off),
                               CHIMERA_VFS_BLOCK_INVALID_DATA);
        } else {
            uint32_t state = rw ?
                (p->loop_off < size ? CHIMERA_VFS_BLOCK_READ_WRITE_DATA :
                 CHIMERA_VFS_BLOCK_INVALID_DATA) : CHIMERA_VFS_BLOCK_READ_DATA;
            diskfs_layout_emit(request, shared, p->loop_off, seg_end - p->loop_off,
                               ext->device_id, vol_off, state);
        }
        p->loop_off = seg_end;
        diskfs_get_layout_advance(request);
        return;
    }

    /* Gap from the cursor to the next extent (or the end of the range). */
    {
        uint64_t gap_end = (p->loop_have && ext->file_offset < end) ?
            ext->file_offset : end;

        if (!rw) {
            /* Read of a hole: skip it (the client reads zeros). */
            p->loop_off = gap_end;
            diskfs_get_layout_process(request);
            return;
        }

        uint64_t gap_len = gap_end - p->loop_off;
        if (gap_len > DISKFS_LAYOUT_GAP_MAX) {
            gap_len = DISKFS_LAYOUT_GAP_MAX;
        }
        p->rmw_aligned_length = gap_len;
        diskfs_get_layout_do_alloc(thread, request);
    }
} /* diskfs_get_layout_process */

static void
diskfs_get_layout_first_cb(
    struct diskfs_bt_op *op,
    int                  result,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;

    p->loop_have = diskfs_ext_from_op(op, result, &p->ext_iter);
    diskfs_bt_op_free(thread, op);

    /* A floor extent entirely before the cursor doesn't overlap: step to the
     * next one so the cursor sees the first extent at or after loop_off. */
    if (p->loop_have &&
        p->ext_iter.file_offset + p->ext_iter.length <= p->loop_off) {
        diskfs_get_layout_advance(request);
        return;
    }
    diskfs_get_layout_process(request);
} /* diskfs_get_layout_first_cb */

static void
diskfs_get_layout_inode_cb(
    struct diskfs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct diskfs_request_private *p       = request->plugin_data;
    struct diskfs_thread          *thread  = p->thread;
    struct diskfs_shared          *shared  = thread->shared;
    uint64_t                       off, end;
    struct diskfs_bt_op           *op;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        diskfs_op_fail(request, p->txn, status);
        return;
    }
    if (unlikely(!S_ISREG(inode->mode))) {
        diskfs_op_fail(request, p->txn, CHIMERA_VFS_EINVAL);
        return;
    }

    off = request->get_layout.offset & ~(uint64_t) (DISKFS_BLOCK_SIZE - 1);
    end = (request->get_layout.offset + request->get_layout.length +
           DISKFS_BLOCK_SIZE - 1) & ~(uint64_t) (DISKFS_BLOCK_SIZE - 1);

    /* A READ layout is never returned past the committed size (the client reads
     * zeros for a hole / beyond EOF). */
    if (request->get_layout.iomode != DISKFS_LAYOUTIOMODE_RW) {
        uint64_t size_end = (inode->size + DISKFS_BLOCK_SIZE - 1) &
            ~(uint64_t) (DISKFS_BLOCK_SIZE - 1);
        if (end > size_end) {
            end = size_end;
        }
    }

    p->inode_stash[0] = inode;
    p->loop_off       = off;
    p->loop_pos       = end;

    request->get_layout.r_layout_class = shared->scsi_layout
        ? CHIMERA_VFS_LAYOUT_CLASS_SCSI : CHIMERA_VFS_LAYOUT_CLASS_BLOCK;
    request->get_layout.r_num_segments = 0;
    request->get_layout.r_num_devices  = 0;

    if (off >= end) {
        diskfs_get_layout_finish(request);
        return;
    }

    op = diskfs_bt_op_alloc(thread);
    if (diskfs_ext_floor_async(op, thread, inode, off, p->rec_scratch,
                               sizeof(p->rec_scratch), diskfs_get_layout_first_cb,
                               request)) {
        diskfs_get_layout_first_cb(op, op->result, request);
    }
} /* diskfs_get_layout_inode_cb */

static void
diskfs_get_layout(
    struct diskfs_thread       *thread,
    struct diskfs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct diskfs_request_private *p = request->plugin_data;
    int                            rw;

    (void) private_data;

    if (unlikely(!shared->block_layout && !shared->scsi_layout)) {
        request->status = CHIMERA_VFS_ENOTSUP;
        request->complete(request);
        return;
    }

    rw        = request->get_layout.iomode == DISKFS_LAYOUTIOMODE_RW;
    p->thread = thread;
    p->txn    = diskfs_txn_begin(thread, rw ? DISKFS_TXN_WRITE : DISKFS_TXN_READ);

    diskfs_inode_get_fh_async(thread, p->txn, request->fh, request->fh_len,
                              diskfs_get_layout_inode_cb, request);
} /* diskfs_get_layout */

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
        --inode->refcnt;     /* drop the dirty-pin; the txn write lock holds it */
        pthread_mutex_unlock(&shard->lock);
        diskfs_txn_pin_inode_block(thread, cp->txn, inode, 0);
    } else {
        pthread_mutex_unlock(&shard->lock);
    }

    diskfs_op_ok(request, cp->txn);     /* logs the inode if pinned, else inline */
} /* diskfs_commit_acquired_cb */

static void
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
            diskfs_commit(thread, shared, request, private_data);
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
        case CHIMERA_VFS_OP_GET_XATTR:
            diskfs_get_xattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SET_XATTR:
            diskfs_set_xattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LIST_XATTRS:
            diskfs_list_xattrs(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE_XATTR:
            diskfs_remove_xattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_GET_LAYOUT:
            diskfs_get_layout(thread, shared, request, private_data);
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
        CHIMERA_VFS_CAP_FS_RELATIVE_OP | CHIMERA_VFS_CAP_XATTR | CHIMERA_VFS_CAP_LAYOUT |
        /* Require a real open so every file op carries a pinned inode in
         * handle->vfs_private (diskfs_open_fh_inode_cb), which read/write reuse
         * to skip per-I/O inode resolution. */
        CHIMERA_VFS_CAP_OPEN_FILE_REQUIRED,
    .init           = diskfs_init,
    .destroy        = diskfs_destroy,
    .thread_init    = diskfs_thread_init,
    .thread_destroy = diskfs_thread_destroy,
    .dispatch       = diskfs_dispatch,
};
