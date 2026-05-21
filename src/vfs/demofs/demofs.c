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

#include "common/varint.h"
#include "common/rbtree.h"

#include "slab_allocator.h"

#include "evpl/evpl.h"

#include "vfs/vfs.h"
#include "vfs/vfs_internal.h"
#include "demofs.h"
#include "space_map.h"
#include "common/logging.h"
#include "common/misc.h"
#include "common/evpl_iovec_cursor.h"

#ifndef container_of
#define container_of(ptr, type, member) \
        ((type *) ((char *) (ptr) - offsetof(type, member)))
#endif /* ifndef container_of */

/* Inode cache: sharded rb-tree keyed by inum. */
#define DEMOFS_INODE_CACHE_SHARDS 256
#define DEMOFS_INODE_CACHE_MASK   (DEMOFS_INODE_CACHE_SHARDS - 1)

/* Max inodes a single transaction can hold locked at once.  rename needs
 * 4 (two parents, child, replaced target); others (e.g. readdir) touch
 * many but only 2 at a time and release as they go. */
#define DEMOFS_TXN_MAX_INODES     4

#define chimera_demofs_debug(...) chimera_debug("demofs", \
                                                __FILE__, \
                                                __LINE__, \
                                                __VA_ARGS__)
#define chimera_demofs_info(...)  chimera_info("demofs", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)
#define chimera_demofs_error(...) chimera_error("demofs", \
                                                __FILE__, \
                                                __LINE__, \
                                                __VA_ARGS__)
#define chimera_demofs_fatal(...) chimera_fatal("demofs", \
                                                __FILE__, \
                                                __LINE__, \
                                                __VA_ARGS__)
#define chimera_demofs_abort(...) chimera_abort("demofs", \
                                                __FILE__, \
                                                __LINE__, \
                                                __VA_ARGS__)

#define chimera_demofs_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "demofs", __FILE__, __LINE__, __VA_ARGS__)

#define chimera_demofs_abort_if(cond, ...) \
        chimera_abort_if(cond, "demofs", __FILE__, __LINE__, __VA_ARGS__)

struct demofs_request_private {
    int                   opcode;
    int                   status;
    int                   pending;
    int                   niov;
    uint32_t              read_prefix;
    uint32_t              read_suffix;
    struct demofs_thread *thread;        // Thread for tracking pending I/O
    struct demofs_txn    *txn;           // Transaction wrapping this op
    /* Multi-inode op scratch (lookup_at parent/child, rename's 4-inode
     * chain, etc.).  Per-op semantics documented at use sites. */
    struct demofs_inode  *inode_stash[4];
    /* Small integer scratch for ops that need to carry state across
     * async callbacks (mount path walker uses it as a path byte offset). */
    uint32_t              op_scratch;

    /* readdir iteration cursor + the current dirent copied out of the
     * b+tree (carried across the per-child inode fetch). */
    uint64_t              rd_from_hash;
    uint64_t              rd_hash;
    uint64_t              rd_inum;
    uint32_t              rd_gen;
    int                   rd_namelen;
    char                  rd_name[256];

    struct evpl_iovec     iov[66];

    // For RMW (read-modify-write) on partial block writes
    int                   rmw_phase;     // 0 = no RMW, 1 = reading, 2 = writing
    uint64_t              rmw_aligned_start; // Block-aligned start offset
    uint64_t              rmw_aligned_length;// Block-aligned length
    uint64_t              rmw_device_id; // Device for the new extent
    uint64_t              rmw_device_offset; // Device offset for the new extent
    uint32_t              rmw_prefix_len; // Bytes to preserve at start of first block
    uint32_t              rmw_suffix_len; // Bytes to preserve at end of last block
    struct evpl_iovec     rmw_prefix_iov; // IOV for prefix data (if read from existing extent)
    struct evpl_iovec     rmw_suffix_iov; // IOV for suffix data (if read from existing extent)
    int                   rmw_prefix_pending;// Pending read for prefix
    int                   rmw_suffix_pending;// Pending read for suffix
    uint32_t              rmw_prefix_valid;  // Valid bytes in prefix (extent may be truncated)
    uint32_t              rmw_suffix_adjust; // Adjustment for suffix when block starts before extent
    uint32_t              rmw_suffix_valid;  // Valid bytes in suffix (extent may be truncated)
};

struct demofs_extent {
    uint32_t              device_id;
    uint32_t              length;
    uint64_t              device_offset;
    uint64_t              file_offset;
    void                 *buffer;
    struct rb_node        node;
    struct demofs_extent *next;
};

struct demofs_device {
    struct evpl_block_device *bdev;
    uint64_t                  id;
    uint64_t                  size;
    uint64_t                  max_request_size;
    char                      name[256];
    pthread_mutex_t           lock;
};

struct demofs_dirent {
    uint64_t              inum;
    uint32_t              gen;
    uint32_t              name_len;
    uint64_t              hash;
    struct rb_node        node;
    struct demofs_dirent *next;
    char                 *name;
};

struct demofs_kv_entry {
    uint64_t                hash;
    uint32_t                key_len;
    uint32_t                value_len;
    struct rb_node          node;
    struct demofs_kv_entry *next;
    void                   *key;
    void                   *value;
};

struct demofs_kv_shard {
    struct rb_tree  entries;
    pthread_mutex_t lock;
};

struct demofs_symlink_target {
    int                           length;
    char                         *data;
    struct demofs_symlink_target *next;
};

struct demofs_txn;
struct demofs_inode;
struct demofs_inode_waiter;
struct demofs_block;

/* Logical lock mode for an inode held by a transaction. */
enum demofs_inode_lock_mode {
    DEMOFS_INODE_LOCK_READ,
    DEMOFS_INODE_LOCK_WRITE,
};

typedef void (*demofs_inode_cb_t)(
    struct demofs_inode *inode,
    int                  status,
    void                *private_data);

/* Defined in the block-cache / b+tree sections below. */
static void demofs_txn_pin_inode_block(
    struct demofs_thread *thread,
    struct demofs_txn    *txn,
    struct demofs_inode  *inode,
    int                   is_new);
static inline void demofs_bt_node_init(
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
struct demofs_inode_waiter {
    struct demofs_txn          *txn;
    enum demofs_inode_lock_mode mode;
    uint32_t                    gen;     /* generation the txn referenced */
    int                         status;  /* CHIMERA_VFS_OK or ENOENT when granted */
    demofs_inode_cb_t           cb;
    void                       *private_data;
    struct demofs_inode        *inode;
    struct demofs_inode_waiter *next;
};

struct demofs_inode {
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
    struct demofs_inode_waiter *wait_head;
    struct demofs_inode_waiter *wait_tail;

    /* This inode's 4 KiB metadata home block in the block cache; pinned
     * while the inode is dirty in a transaction.  NULL until first claimed.
     * Directory entries, extents and the symlink target all live as keyed
     * records in this inode's b+tree (rooted in the block at offset 256). */
    struct demofs_block        *block;

    /* Directory only: parent for ".." resolution (also persisted in dinode). */
    uint64_t                    parent_inum;
    uint32_t                    parent_gen;

    /* TODO(phase 1c): extents move into the b+tree; until then regular
     * files keep their extents in this in-memory rb-tree. */
    struct {
        struct rb_tree extents;
    } file;
};

struct demofs_inode_shard {
    pthread_mutex_t lock;
    struct rb_tree  inodes;       /* keyed by inum */
};

struct demofs_inode_cache {
    struct demofs_inode_shard shards[DEMOFS_INODE_CACHE_SHARDS];
};

/* ------------------------------------------------------------------ */
/* Block cache                                                         */
/* ------------------------------------------------------------------ */

#define DEMOFS_BLOCK_SHIFT                   SM_BLOCK_SHIFT  /* 12 */
#define DEMOFS_BLOCK_SIZE                    SM_BLOCK_SIZE   /* 4096 */
#define DEMOFS_BLOCK_CACHE_SHARDS            256
#define DEMOFS_BLOCK_CACHE_SHARD_MASK        (DEMOFS_BLOCK_CACHE_SHARDS - 1)
#define DEMOFS_BLOCK_CACHE_BUCKETS_PER_SHARD 1024
#define DEMOFS_BLOCK_CACHE_BUCKET_MASK       (DEMOFS_BLOCK_CACHE_BUCKETS_PER_SHARD - 1)

enum demofs_block_state {
    DEMOFS_BLOCK_CLEAN,    /* matches final on-disk location; evictable when unpinned */
    DEMOFS_BLOCK_DIRTY,    /* modified, pinned by >=1 txn, not yet logged */
    DEMOFS_BLOCK_LOGGED,   /* intent record durable; awaiting tail-push to final loc */
};

/*
 * A cached 4 KiB on-disk block, keyed by (device_id, device_offset).  The
 * buffer is plain heap memory; block I/O copies it through a thread-local
 * evpl_iovec so buffers are never shared across evpl instances.  Hash-chain
 * linkage is RCU-managed for lock-free lookup; mutation happens under the
 * owning shard lock and frees defer through call_rcu.
 */
struct demofs_block {
    uint32_t             device_id;
    uint64_t             device_offset;        /* block-aligned; key with device_id */
    void                *buffer;               /* DEMOFS_BLOCK_SIZE bytes */
    int                  pin_count;            /* atomic; >0 => not reclaimable */
    enum demofs_block_state state;
    uint64_t             seq;                  /* update order for tail-push */
    struct demofs_block *hash_next;            /* RCU bucket chain */
    struct rcu_head      rcu;
    struct demofs_block *lru_prev, *lru_next;      /* clean+unpinned LRU (Stage 3) */
    struct demofs_block *wb_next;              /* writeback queue (Stage 3) */
};

struct demofs_block_shard {
    pthread_mutex_t       lock;
    struct demofs_block **buckets;     /* [DEMOFS_BLOCK_CACHE_BUCKETS_PER_SHARD] */
};

struct demofs_block_cache {
    struct demofs_block_shard shards[DEMOFS_BLOCK_CACHE_SHARDS];
};

/*
 * On-disk inode block layout (4 KiB):
 *   [0, DEMOFS_INODE_AREA)   struct demofs_dinode (scalar attributes)
 *   [DEMOFS_INODE_AREA, end) the inode's b+tree root node (embedded)
 *
 * Directory entries, file extents and the symlink target all live as keyed
 * records in the inode's single b+tree; the root node is embedded in the
 * inode block, and deeper nodes occupy their own 4 KiB blocks.
 */
#define DEMOFS_INODE_AREA   256
#define DEMOFS_BT_ROOT_BASE DEMOFS_INODE_AREA
#define DEMOFS_BT_ROOT_CAP  (DEMOFS_BLOCK_SIZE - DEMOFS_INODE_AREA)   /* 3840 */
#define DEMOFS_BT_NODE_CAP  DEMOFS_BLOCK_SIZE                          /* 4096 */

struct demofs_dinode {
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
enum demofs_bt_rectype {
    DEMOFS_REC_DIRENT  = 1,
    DEMOFS_REC_EXTENT  = 2,
    DEMOFS_REC_SYMLINK = 3,
};

/* B+tree key: ordered by (type, subkey).  subkey is the name hash for
 * dirents, the file offset for extents, 0 for the single symlink record. */
struct demofs_bt_key {
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
struct demofs_bt_node_hdr {
    uint16_t level;
    uint16_t nitems;
    uint32_t capacity;     /* usable node bytes (DEMOFS_BT_ROOT_CAP or _NODE_CAP) */
    uint32_t free_end;     /* leaf heap top (records occupy [free_end, capacity)) */
    uint32_t reserved;
    uint64_t next_leaf;    /* leaf only: bptr of next leaf in key order (0 = none) */
};

struct demofs_bt_islot {          /* interior slot, 24 B */
    struct demofs_bt_key key;
    uint64_t             child;   /* bptr (sm disk:ag:index encoding) */
};

struct demofs_bt_lslot {          /* leaf slot, 24 B */
    struct demofs_bt_key key;
    uint32_t             off;     /* record offset from node base */
    uint32_t             len;     /* record length */
};

/* Leaf record payloads (stored in the leaf heap). */
struct demofs_dirent_rec {
    uint64_t inum;
    uint32_t gen;
    uint16_t name_len;
    char     name[];
} __attribute__((packed));

struct demofs_extent_rec {
    uint64_t length;
    uint32_t device_id;
    uint32_t pad;
    uint64_t device_offset;
} __attribute__((packed));

#define DEMOFS_DIRENT_REC_MAX (sizeof(struct demofs_dirent_rec) + 256)

/*
 * Intent-log redo record, written into the reserved intent-log region.
 * A record is a header followed by num_blocks (block-header, 4 KiB content)
 * pairs, padded to a 4 KiB multiple.  Full-block redo: the record carries
 * the entire post-image of every dirty block in the transaction.
 */
#define DEMOFS_REDO_MAGIC     0x4F44455246534944ULL /* "DISFREDO" */

struct demofs_redo_header {
    uint64_t magic;
    uint64_t seq;          /* monotonically increasing record sequence */
    uint32_t num_blocks;
    uint32_t reclen;       /* total record length, including padding */
};

struct demofs_redo_block_header {
    uint32_t device_id;
    uint32_t pad;
    uint64_t device_offset;
};

enum demofs_txn_type {
    DEMOFS_TXN_READ,
    DEMOFS_TXN_WRITE,
};

/* A dirty block held (pinned) by a transaction until commit/log. */
struct demofs_txn_block {
    struct demofs_block     *block;
    struct demofs_txn_block *next;
};

struct demofs_txn_slot {
    struct demofs_inode *inode;
    enum demofs_inode_lock_mode mode;
};

/* Commit completion callback.  Will be invoked exactly once per successful
 * commit, with status 0 on success or a CHIMERA_VFS_* code on failure.
 * Today commits never fail, but the signature is async-shaped so a future
 * intent-log write can fail. */
typedef void (*demofs_txn_commit_cb_t)(
    struct demofs_txn *txn,
    int                status,
    void              *private_data);

struct demofs_txn {
    enum demofs_txn_type     type;
    struct demofs_thread    *thread;
    struct demofs_txn       *next;         /* per-thread free list link */
    struct demofs_txn_slot   inodes[DEMOFS_TXN_MAX_INODES];
    int                      num_inodes;
    struct demofs_txn_block *blocks;       /* dirty blocks pinned by this txn */
};

/*
 * Per-thread NVMe-style submission and completion queues used to hand
 * write transactions off to the intent log thread and to receive commit
 * completions back.  Each ring is single-producer / single-consumer:
 *
 *   sq:  worker (producer)        ->  intent log thread (consumer)
 *   cq:  intent log thread (prod) ->  worker (consumer)
 */
#define DEMOFS_IQ_RING_SIZE 1024
#define DEMOFS_IQ_RING_MASK (DEMOFS_IQ_RING_SIZE - 1)

struct demofs_iq_entry {
    struct demofs_txn     *txn;
    demofs_txn_commit_cb_t cb;
    void                  *private_data;
    int                    status;
};

struct demofs_iq_ring {
    /* Accessed via __atomic_* builtins. */
    uint32_t               head;     /* consumer position */
    uint32_t               tail;     /* producer position */
    struct demofs_iq_entry entries[DEMOFS_IQ_RING_SIZE];
};

struct demofs_iq_channel {
    struct demofs_iq_ring     sq;
    struct demofs_iq_ring     cq;
    struct evpl_doorbell      cq_doorbell;
    struct demofs_thread     *worker;

    /* CQEs reserved for redo writes issued but not yet completed.  Owned by
     * the intent-log thread; bounds in-flight writes to available CQ space. */
    uint32_t                  cq_inflight;

    /* Lifecycle: workers append on register, set flag on unregister.
     * Intent log thread owns the slot array. */
    struct demofs_iq_channel *next_pending;
    int                       unregister_requested;
    int                       unregister_done;
};

#define DEMOFS_IL_MAX_CHANNELS 256

struct demofs_intent_log {
    struct evpl_doorbell      wake_doorbell;
    struct evpl              *evpl;
    struct evpl_thread       *thread;
    int                       ready;             /* atomic */
    int                       shutdown;          /* atomic */
    uint32_t                  num_channels;      /* slots[] count, intent log thread only */
    struct demofs_iq_channel *channels[DEMOFS_IL_MAX_CHANNELS];
    pthread_mutex_t           registration_lock;
    struct demofs_iq_channel *pending_head;

    /* Block queues on the intent-log thread's own evpl (one per device),
     * used to write redo records into the reserved intent-log region. */
    struct evpl_block_queue **queue;
    uint64_t                  log_head;          /* next free byte in the intent-log region */
    uint64_t                  log_seq;           /* next redo record sequence number */
};

struct demofs_shared {
    struct demofs_device      *devices;
    int                        num_devices;
    struct demofs_inode_cache *inode_cache;
    struct demofs_block_cache *block_cache;
    struct demofs_kv_shard    *kv_shards;
    int                        num_kv_shards;
    int                        num_active_threads;
    uint8_t                    root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                   root_fhlen;
    uint64_t                   fsid;
    struct space_map          *space_map;
    struct demofs_intent_log   intent_log;
    pthread_mutex_t            lock;
};

struct demofs_thread {
    struct evpl                *evpl;
    struct demofs_shared       *shared;
    struct evpl_block_queue   **queue;
    struct evpl_iovec           zero;
    struct evpl_iovec           pad;
    int                         thread_id;
    struct slab_allocator      *allocator;
    struct sm_thread_cache      space_cache;
    struct demofs_txn          *txn_free_list;
    struct demofs_inode_waiter *waiter_free_list;
    struct demofs_iq_channel   *iq_channel;
    int                         pending_io;

    /* Cross-thread lock-grant delivery: any thread that releases an inode
     * and grants it to a waiter belonging to this worker enqueues the
     * granted waiter here and rings grant_doorbell, so the continuation
     * runs back on this worker. */
    pthread_mutex_t             grant_lock;
    struct demofs_inode_waiter *grant_head;
    struct demofs_inode_waiter *grant_tail;
    struct evpl_doorbell        grant_doorbell;
};

static inline uint32_t
demofs_inum_to_fh(
    struct demofs_shared *shared,
    uint8_t              *fh,
    uint64_t              inum,
    uint32_t              gen)
{
    return chimera_vfs_encode_fh_inum_parent(shared->root_fh, inum, gen, fh);
} /* demofs_inum_to_fh */

static inline void
demofs_fh_to_inum(
    uint64_t      *inum,
    uint32_t      *gen,
    const uint8_t *fh,
    int            fhlen)
{
    chimera_vfs_decode_fh_inum(fh, fhlen, inum, gen);
} /* demofs_fh_to_inum */

/* ------------------------------------------------------------------ */
/* Inode cache + logical (read/write) transaction locks                */
/* ------------------------------------------------------------------ */

#define DEMOFS_INODE_MODE_FOR_TXN(txn) \
        ((txn)->type == DEMOFS_TXN_WRITE ? DEMOFS_INODE_LOCK_WRITE \
                                         : DEMOFS_INODE_LOCK_READ)

static inline struct demofs_inode_shard *
demofs_inode_shard(
    struct demofs_shared *shared,
    uint64_t              inum)
{
    return &shared->inode_cache->shards[inum & DEMOFS_INODE_CACHE_MASK];
} /* demofs_inode_shard */

static inline struct demofs_inode_waiter *
demofs_waiter_alloc(struct demofs_thread *thread)
{
    struct demofs_inode_waiter *w = thread->waiter_free_list;

    if (w) {
        thread->waiter_free_list = w->next;
    } else {
        w = malloc(sizeof(*w));
    }
    return w;
} /* demofs_waiter_alloc */

static inline void
demofs_waiter_free(
    struct demofs_thread       *thread,
    struct demofs_inode_waiter *w)
{
    w->next                  = thread->waiter_free_list;
    thread->waiter_free_list = w;
} /* demofs_waiter_free */

/* Record a held inode in the transaction's fixed slot array. */
static inline void
demofs_txn_add_slot(
    struct demofs_txn          *txn,
    struct demofs_inode        *inode,
    enum demofs_inode_lock_mode mode)
{
    chimera_demofs_abort_if(txn->num_inodes >= DEMOFS_TXN_MAX_INODES,
                            "demofs txn inode slots exhausted");
    txn->inodes[txn->num_inodes].inode = inode;
    txn->inodes[txn->num_inodes].mode  = mode;
    txn->num_inodes++;
} /* demofs_txn_add_slot */

/* Caller must hold the inode's shard lock. */
static inline int
demofs_inode_lock_compatible(
    struct demofs_inode        *inode,
    enum demofs_inode_lock_mode mode)
{
    if (mode == DEMOFS_INODE_LOCK_WRITE) {
        return inode->writer == 0 && inode->readers == 0;
    }
    return inode->writer == 0;
} /* demofs_inode_lock_compatible */

/* Caller must hold the inode's shard lock. */
static inline void
demofs_inode_lock_grant(
    struct demofs_inode        *inode,
    enum demofs_inode_lock_mode mode)
{
    if (mode == DEMOFS_INODE_LOCK_WRITE) {
        inode->writer = 1;
    } else {
        inode->readers++;
    }
} /* demofs_inode_lock_grant */

/*
 * Hand a granted (or stale-failed) waiter to its owning worker so its
 * continuation runs back on the transaction's own thread.
 */
static void
demofs_dispatch_grant(struct demofs_inode_waiter *w)
{
    struct demofs_thread *worker = w->txn->thread;

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
} /* demofs_dispatch_grant */

/*
 * Drop one held inode lock and grant the lock to compatible FIFO waiters.
 * Safe to call from any thread (worker for read/abort, intent-log thread
 * for write commit); granted waiters are dispatched to their own workers.
 */
static void
demofs_inode_release_one(
    struct demofs_thread       *thread,
    struct demofs_inode        *inode,
    enum demofs_inode_lock_mode mode)
{
    struct demofs_inode_shard  *shard   = demofs_inode_shard(thread->shared, inode->inum);
    struct demofs_inode_waiter *granted = NULL;
    struct demofs_inode_waiter *w;

    pthread_mutex_lock(&shard->lock);

    if (mode == DEMOFS_INODE_LOCK_WRITE) {
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

        if (!demofs_inode_lock_compatible(inode, w->mode)) {
            break;
        }

        inode->wait_head = w->next;
        if (!inode->wait_head) {
            inode->wait_tail = NULL;
        }
        demofs_inode_lock_grant(inode, w->mode);
        w->status = CHIMERA_VFS_OK;
        w->next   = granted;
        granted   = w;

        if (w->mode == DEMOFS_INODE_LOCK_WRITE) {
            break;     /* exclusive: stop granting */
        }
    }

    pthread_mutex_unlock(&shard->lock);

    while (granted) {
        w       = granted;
        granted = w->next;
        demofs_dispatch_grant(w);
    }
} /* demofs_inode_release_one */

/*
 * Release a single held inode early (readdir-style iteration that walks
 * many children but only needs to hold the current one).  No-op if the
 * inode isn't held by this txn.
 */
static inline void
demofs_txn_unlock_inode(
    struct demofs_txn   *txn,
    struct demofs_inode *inode)
{
    int i;

    for (i = 0; i < txn->num_inodes; i++) {
        if (txn->inodes[i].inode == inode) {
            enum demofs_inode_lock_mode mode = txn->inodes[i].mode;

            txn->inodes[i] = txn->inodes[txn->num_inodes - 1];
            txn->num_inodes--;
            demofs_inode_release_one(txn->thread, inode, mode);
            return;
        }
    }
} /* demofs_txn_unlock_inode */

/*
 * Release every inode held by this txn.  Called by the worker thread for
 * read-txn commits and aborts, and by the intent-log thread for write-txn
 * commits (before it pushes the CQE).
 */
static inline void
demofs_txn_unlock_all(struct demofs_txn *txn)
{
    int i;

    for (i = 0; i < txn->num_inodes; i++) {
        demofs_inode_release_one(txn->thread, txn->inodes[i].inode,
                                 txn->inodes[i].mode);
    }
    txn->num_inodes = 0;
} /* demofs_txn_unlock_all */

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
static void
demofs_inode_acquire(
    struct demofs_thread       *thread,
    struct demofs_txn          *txn,
    uint64_t                    inum,
    uint32_t                    gen,
    enum demofs_inode_lock_mode mode,
    demofs_inode_cb_t           cb,
    void                       *private_data)
{
    struct demofs_inode_shard  *shard;
    struct demofs_inode        *inode;
    struct demofs_inode_waiter *w;
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

    shard = demofs_inode_shard(thread->shared, inum);
    pthread_mutex_lock(&shard->lock);

    rb_tree_query_exact(&shard->inodes, inum, inum, inode);

    if (unlikely(!inode || inode->gen != gen)) {
        pthread_mutex_unlock(&shard->lock);
        cb(NULL, CHIMERA_VFS_ENOENT, private_data);
        return;
    }

    if (demofs_inode_lock_compatible(inode, mode)) {
        demofs_inode_lock_grant(inode, mode);
        pthread_mutex_unlock(&shard->lock);
        demofs_txn_add_slot(txn, inode, mode);
        if (mode == DEMOFS_INODE_LOCK_WRITE) {
            demofs_txn_pin_inode_block(thread, txn, inode, 0);
        }
        cb(inode, CHIMERA_VFS_OK, private_data);
        return;
    }

    w               = demofs_waiter_alloc(thread);
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
} /* demofs_inode_acquire */

static inline void
demofs_inode_get_inum_async(
    struct demofs_thread *thread,
    struct demofs_txn    *txn,
    uint64_t              inum,
    uint32_t              gen,
    demofs_inode_cb_t     cb,
    void                 *private_data)
{
    demofs_inode_acquire(thread, txn, inum, gen,
                         DEMOFS_INODE_MODE_FOR_TXN(txn), cb, private_data);
} /* demofs_inode_get_inum_async */

static inline void
demofs_inode_get_fh_async(
    struct demofs_thread *thread,
    struct demofs_txn    *txn,
    const uint8_t        *fh,
    int                   fhlen,
    demofs_inode_cb_t     cb,
    void                 *private_data)
{
    uint64_t inum;
    uint32_t gen;

    demofs_fh_to_inum(&inum, &gen, fh, fhlen);
    demofs_inode_get_inum_async(thread, txn, inum, gen, cb, private_data);
} /* demofs_inode_get_fh_async */

/*
 * Synchronous read-lock acquire, used by the mount-time path walk which
 * runs before concurrent load.  Records the read lock in the txn so it is
 * released centrally at commit.  Asserts there is no conflicting writer
 * (cannot happen at mount); returns NULL if the inode isn't resident or
 * the generation is stale.
 */
static struct demofs_inode *
demofs_inode_acquire_sync_read(
    struct demofs_thread *thread,
    struct demofs_txn    *txn,
    uint64_t              inum,
    uint32_t              gen)
{
    struct demofs_inode_shard *shard;
    struct demofs_inode       *inode;
    int                        i;

    for (i = 0; i < txn->num_inodes; i++) {
        if (txn->inodes[i].inode->inum == inum) {
            return txn->inodes[i].inode->gen == gen ? txn->inodes[i].inode : NULL;
        }
    }

    shard = demofs_inode_shard(thread->shared, inum);
    pthread_mutex_lock(&shard->lock);
    rb_tree_query_exact(&shard->inodes, inum, inum, inode);
    if (!inode || inode->gen != gen) {
        pthread_mutex_unlock(&shard->lock);
        return NULL;
    }
    chimera_demofs_abort_if(inode->writer,
                            "mount path walk: inode %lu write-locked", inum);
    inode->readers++;
    pthread_mutex_unlock(&shard->lock);

    demofs_txn_add_slot(txn, inode, DEMOFS_INODE_LOCK_READ);
    return inode;
} /* demofs_inode_acquire_sync_read */

/* ------------------------------------------------------------------ */
/* Block cache                                                         */
/* ------------------------------------------------------------------ */

static inline uint64_t
demofs_block_hash(
    uint32_t device_id,
    uint64_t device_offset)
{
    uint64_t h = device_offset >> DEMOFS_BLOCK_SHIFT;

    h ^= (uint64_t) device_id * 0x9e3779b97f4a7c15ULL;
    h ^= h >> 29;
    h *= 0xbf58476d1ce4e5b9ULL;
    h ^= h >> 32;
    return h;
} /* demofs_block_hash */

static void
demofs_block_cache_create(struct demofs_shared *shared)
{
    struct demofs_block_cache *cache = calloc(1, sizeof(*cache));
    int                        i;

    for (i = 0; i < DEMOFS_BLOCK_CACHE_SHARDS; i++) {
        pthread_mutex_init(&cache->shards[i].lock, NULL);
        cache->shards[i].buckets = calloc(DEMOFS_BLOCK_CACHE_BUCKETS_PER_SHARD,
                                          sizeof(struct demofs_block *));
    }
    shared->block_cache = cache;
} /* demofs_block_cache_create */

static void
demofs_block_cache_destroy(struct demofs_shared *shared)
{
    struct demofs_block_cache *cache = shared->block_cache;
    int                        i;
    uint32_t                   b;

    if (!cache) {
        return;
    }

    for (i = 0; i < DEMOFS_BLOCK_CACHE_SHARDS; i++) {
        struct demofs_block_shard *shard = &cache->shards[i];

        for (b = 0; b < DEMOFS_BLOCK_CACHE_BUCKETS_PER_SHARD; b++) {
            struct demofs_block *blk = shard->buckets[b];

            while (blk) {
                struct demofs_block *next = blk->hash_next;
                free(blk->buffer);
                free(blk);
                blk = next;
            }
        }
        free(shard->buckets);
        pthread_mutex_destroy(&shard->lock);
    }
    free(cache);
    shared->block_cache = NULL;
} /* demofs_block_cache_destroy */

/*
 * Find a block already resident in the cache.  Lock-free RCU read; the
 * caller must be inside an rcu read-side critical section (or hold no
 * concurrent eviction risk, as in Stage 1/2 where blocks are never freed).
 */
static inline struct demofs_block *
demofs_block_lookup_locked(
    struct demofs_block_shard *shard,
    uint32_t                   bucket,
    uint32_t                   device_id,
    uint64_t                   device_offset)
{
    struct demofs_block *blk;

    for (blk = shard->buckets[bucket]; blk; blk = blk->hash_next) {
        if (blk->device_id == device_id && blk->device_offset == device_offset) {
            return blk;
        }
    }
    return NULL;
} /* demofs_block_lookup_locked */

/*
 * Find or create the cache entry for (device_id, device_offset) and pin it.
 * If is_new is set the block was just allocated from the space map, so its
 * buffer is zeroed and it starts DIRTY; otherwise an existing block is
 * returned (created zeroed on a miss for now, since the read-back path is
 * not implemented yet).
 */
static struct demofs_block *
demofs_block_claim(
    struct demofs_thread *thread,
    uint32_t              device_id,
    uint64_t              device_offset,
    int                   is_new)
{
    struct demofs_block_cache *cache  = thread->shared->block_cache;
    uint64_t                   hash   = demofs_block_hash(device_id, device_offset);
    uint32_t                   sidx   = hash & DEMOFS_BLOCK_CACHE_SHARD_MASK;
    uint32_t                   bucket = (hash >> 8) & DEMOFS_BLOCK_CACHE_BUCKET_MASK;
    struct demofs_block_shard *shard  = &cache->shards[sidx];
    struct demofs_block       *blk;

    (void) is_new;

    pthread_mutex_lock(&shard->lock);

    blk = demofs_block_lookup_locked(shard, bucket, device_id, device_offset);
    if (!blk) {
        blk                = calloc(1, sizeof(*blk));
        blk->device_id     = device_id;
        blk->device_offset = device_offset;
        blk->buffer        = calloc(1, DEMOFS_BLOCK_SIZE);
        blk->state         = DEMOFS_BLOCK_CLEAN;
        blk->pin_count     = 0;
        /* Publish into the bucket chain (RCU readers will use hash_next). */
        blk->hash_next = shard->buckets[bucket];
        rcu_assign_pointer(shard->buckets[bucket], blk);
    }

    blk->pin_count++;
    pthread_mutex_unlock(&shard->lock);

    return blk;
} /* demofs_block_claim */

/*
 * txn_block link nodes are allocated on the worker (when a block is pinned)
 * but freed on the intent-log thread (when the txn's blocks are unpinned),
 * so they use plain malloc/free rather than a per-thread free list.
 */
static inline void
demofs_txn_add_block(
    struct demofs_txn   *txn,
    struct demofs_block *block)
{
    struct demofs_txn_block *tb = malloc(sizeof(*tb));

    tb->block   = block;
    tb->next    = txn->blocks;
    txn->blocks = tb;
} /* demofs_txn_add_block */

/*
 * Ensure this (write-locked) inode's home block is resident and pinned, and
 * attached to the transaction.  Idempotent per inode: the inode caches its
 * block pointer and we only claim/attach once.
 */
static void
demofs_txn_pin_inode_block(
    struct demofs_thread *thread,
    struct demofs_txn    *txn,
    struct demofs_inode  *inode,
    int                   is_new)
{
    uint32_t device_id;
    uint64_t device_offset;

    if (inode->block) {
        return;     /* already pinned by this txn */
    }

    device_offset = sm_inum_to_device_offset(thread->shared->space_map,
                                             inode->inum, &device_id);

    inode->block = demofs_block_claim(thread, device_id, device_offset, is_new);
    demofs_txn_add_block(txn, inode->block);

    if (is_new) {
        /* Initialize the embedded b+tree root (empty leaf) in the new
         * inode block. */
        demofs_bt_node_init(inode->block->buffer, DEMOFS_BT_ROOT_BASE,
                            DEMOFS_BT_ROOT_CAP, 0);
    }
} /* demofs_txn_pin_inode_block */

/* Serialize an inode's durable attributes into the front of its block. */
static void
demofs_inode_flush(struct demofs_inode *inode)
{
    struct demofs_dinode *di;

    if (!inode->block) {
        return;
    }

    di = inode->block->buffer;

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

    inode->block->state = DEMOFS_BLOCK_DIRTY;
} /* demofs_inode_flush */

/*
 * At commit, serialize every write-locked inode into its block buffer.
 * Runs on the worker thread (it owns the live inodes under write lock).
 */
static void
demofs_txn_flush_inodes(struct demofs_txn *txn)
{
    int i;

    for (i = 0; i < txn->num_inodes; i++) {
        if (txn->inodes[i].mode == DEMOFS_INODE_LOCK_WRITE) {
            demofs_inode_flush(txn->inodes[i].inode);
        }
    }
} /* demofs_txn_flush_inodes */

/*
 * Unpin all blocks held by this txn, transitioning them to new_state.  Also
 * clears each write-locked inode's cached block pointer (it is only valid
 * while the txn holds the block pinned; a later txn re-claims it).  Used at
 * commit (intent-log thread) and abort (worker).  Must run while the txn's
 * inode slots are still populated (before demofs_txn_unlock_all).
 */
static void
demofs_txn_unpin_blocks(
    struct demofs_txn      *txn,
    enum demofs_block_state new_state)
{
    struct demofs_thread    *thread = txn->thread;
    struct demofs_txn_block *tb     = txn->blocks;
    struct demofs_txn_block *n;
    int                      i;

    for (i = 0; i < txn->num_inodes; i++) {
        if (txn->inodes[i].mode == DEMOFS_INODE_LOCK_WRITE) {
            txn->inodes[i].inode->block = NULL;
        }
    }

    (void) thread;

    txn->blocks = NULL;
    while (tb) {
        struct demofs_block *blk = tb->block;

        n          = tb->next;
        blk->state = new_state;
        __atomic_fetch_sub(&blk->pin_count, 1, __ATOMIC_RELAXED);
        free(tb);
        tb = n;
    }
} /* demofs_txn_unpin_blocks */

/* ------------------------------------------------------------------ */
/* Per-inode b+tree                                                    */
/* ------------------------------------------------------------------ */

/* Lock-free RCU lookup of a resident block; NULL if not cached. */
static struct demofs_block *
demofs_block_lookup_rcu(
    struct demofs_thread *thread,
    uint32_t              device_id,
    uint64_t              device_offset)
{
    struct demofs_block_cache *cache  = thread->shared->block_cache;
    uint64_t                   hash   = demofs_block_hash(device_id, device_offset);
    uint32_t                   sidx   = hash & DEMOFS_BLOCK_CACHE_SHARD_MASK;
    uint32_t                   bucket = (hash >> 8) & DEMOFS_BLOCK_CACHE_BUCKET_MASK;
    struct demofs_block_shard *shard  = &cache->shards[sidx];
    struct demofs_block       *blk;

    for (blk = rcu_dereference(shard->buckets[bucket]); blk;
         blk = rcu_dereference(blk->hash_next)) {
        if (blk->device_id == device_id && blk->device_offset == device_offset) {
            return blk;
        }
    }
    return NULL;
} /* demofs_block_lookup_rcu */

static inline int
demofs_bt_key_cmp(
    const struct demofs_bt_key *a,
    const struct demofs_bt_key *b)
{
    if (a->type != b->type) {
        return a->type < b->type ? -1 : 1;
    }
    if (a->subkey != b->subkey) {
        return a->subkey < b->subkey ? -1 : 1;
    }
    return 0;
} /* demofs_bt_key_cmp */

static inline struct demofs_bt_node_hdr *
demofs_bt_hdr(
    void    *buf,
    uint32_t base)
{
    return (struct demofs_bt_node_hdr *) ((char *) buf + base);
} /* demofs_bt_hdr */

static inline struct demofs_bt_islot *
demofs_bt_islots(
    void    *buf,
    uint32_t base)
{
    return (struct demofs_bt_islot *) ((char *) buf + base + sizeof(struct demofs_bt_node_hdr));
} /* demofs_bt_islots */

static inline struct demofs_bt_lslot *
demofs_bt_lslots(
    void    *buf,
    uint32_t base)
{
    return (struct demofs_bt_lslot *) ((char *) buf + base + sizeof(struct demofs_bt_node_hdr));
} /* demofs_bt_lslots */

static inline void
demofs_bt_node_init(
    void    *buf,
    uint32_t base,
    uint32_t capacity,
    uint16_t level)
{
    struct demofs_bt_node_hdr *h = demofs_bt_hdr(buf, base);

    h->level     = level;
    h->nitems    = 0;
    h->capacity  = capacity;
    h->free_end  = capacity;
    h->reserved  = 0;
    h->next_leaf = 0;
} /* demofs_bt_node_init */

/* Free bytes available in a leaf for one more (slot + record). */
static inline uint32_t
demofs_bt_leaf_free(
    void    *buf,
    uint32_t base)
{
    struct demofs_bt_node_hdr *h          = demofs_bt_hdr(buf, base);
    uint32_t                   free_start = sizeof(*h) + h->nitems * sizeof(struct demofs_bt_lslot);

    return h->free_end - free_start;
} /* demofs_bt_leaf_free */

static inline uint32_t
demofs_bt_interior_free(
    void    *buf,
    uint32_t base)
{
    struct demofs_bt_node_hdr *h          = demofs_bt_hdr(buf, base);
    uint32_t                   free_start = sizeof(*h) + h->nitems * sizeof(struct demofs_bt_islot);

    return h->capacity - free_start;
} /* demofs_bt_interior_free */

/*
 * Binary search a leaf for key.  Returns the index of the first slot whose
 * key is >= the search key; sets *exact if that slot's key matches.
 */
static int
demofs_bt_leaf_search(
    void                       *buf,
    uint32_t                    base,
    const struct demofs_bt_key *key,
    int                        *exact)
{
    struct demofs_bt_lslot *sl = demofs_bt_lslots(buf, base);
    int                     n  = demofs_bt_hdr(buf, base)->nitems;
    int                     lo = 0, hi = n;

    *exact = 0;
    while (lo < hi) {
        int mid = (lo + hi) >> 1;
        int c   = demofs_bt_key_cmp(&sl[mid].key, key);

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
} /* demofs_bt_leaf_search */

/* Index of the child subtree that may contain key (largest key <= search). */
static int
demofs_bt_interior_search(
    void                       *buf,
    uint32_t                    base,
    const struct demofs_bt_key *key)
{
    struct demofs_bt_islot *sl = demofs_bt_islots(buf, base);
    int                     n  = demofs_bt_hdr(buf, base)->nitems;
    int                     lo = 0, hi = n, ans = 0;

    while (lo < hi) {
        int mid = (lo + hi) >> 1;

        if (demofs_bt_key_cmp(&sl[mid].key, key) <= 0) {
            ans = mid;
            lo  = mid + 1;
        } else {
            hi = mid;
        }
    }
    return ans;
} /* demofs_bt_interior_search */

/* Append a leaf record at the end (caller guarantees sorted order + room). */
static void
demofs_bt_leaf_append(
    void                       *buf,
    uint32_t                    base,
    const struct demofs_bt_key *key,
    const void                 *rec,
    uint32_t                    reclen)
{
    struct demofs_bt_node_hdr *h  = demofs_bt_hdr(buf, base);
    struct demofs_bt_lslot    *sl = demofs_bt_lslots(buf, base);

    h->free_end -= reclen;
    memcpy((char *) buf + base + h->free_end, rec, reclen);
    sl[h->nitems].key = *key;
    sl[h->nitems].off = h->free_end;
    sl[h->nitems].len = reclen;
    h->nitems++;
} /* demofs_bt_leaf_append */

/* Allocate a fresh b+tree node block; returns the (pinned, txn-attached)
 * block and its bptr.  Buffer is zeroed and initialized as an empty node. */
static struct demofs_block *
demofs_bt_alloc_node(
    struct demofs_thread *thread,
    struct demofs_txn    *txn,
    uint16_t              level,
    uint64_t             *r_bptr)
{
    struct demofs_shared *shared = thread->shared;
    struct demofs_block  *blk;
    uint32_t              device_id;
    uint64_t              device_offset;
    int                   rc;

    rc = space_map_alloc(shared->space_map, &thread->space_cache,
                         DEMOFS_BLOCK_SIZE, &device_id, &device_offset);
    chimera_demofs_abort_if(rc != 0, "b+tree node allocation failed (ENOSPC)");

    blk = demofs_block_claim(thread, device_id, device_offset, 1);
    demofs_txn_add_block(txn, blk);

    demofs_bt_node_init(blk->buffer, 0, DEMOFS_BT_NODE_CAP, level);

    *r_bptr = sm_inum_from_device_offset(shared->space_map, device_id, device_offset);
    return blk;
} /* demofs_bt_alloc_node */

/* Resolve a child bptr to its block buffer for writing (claim+pin+attach). */
static void *
demofs_bt_node_for_write(
    struct demofs_thread *thread,
    struct demofs_txn    *txn,
    uint64_t              bptr)
{
    uint32_t             device_id;
    uint64_t             device_offset;
    struct demofs_block *blk;

    device_offset = sm_inum_to_device_offset(thread->shared->space_map, bptr, &device_id);
    blk           = demofs_block_claim(thread, device_id, device_offset, 0);
    demofs_txn_add_block(txn, blk);
    return blk->buffer;
} /* demofs_bt_node_for_write */

/*
 * Split a full leaf (current node at buf/base/cap) while inserting
 * (nkey,nrec).  The lower half stays in place; the upper half plus the new
 * right sibling's bptr are returned via *sep_key / *sep_bptr.
 */
static void
demofs_bt_leaf_split(
    struct demofs_thread       *thread,
    struct demofs_txn          *txn,
    void                       *buf,
    uint32_t                    base,
    uint32_t                    cap,
    int                         insert_idx,
    const struct demofs_bt_key *nkey,
    const void                 *nrec,
    uint32_t                    nreclen,
    struct demofs_bt_key       *sep_key,
    uint64_t                   *sep_bptr)
{
    struct demofs_bt_node_hdr *h     = demofs_bt_hdr(buf, base);
    struct demofs_bt_lslot    *sl    = demofs_bt_lslots(buf, base);
    int                        n     = h->nitems;
    int                        total = n + 1;

    struct {
        struct demofs_bt_key key;
        uint32_t             len;
        uint32_t             scratch_off;
    } *items;
    char                      *scratch;
    uint32_t                   sp = 0, total_bytes = 0, half, acc = 0;
    int                        i, oi, split_i;
    struct demofs_block       *right;
    void                      *rbuf;
    uint64_t                   old_next;

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

    right = demofs_bt_alloc_node(thread, txn, 0, sep_bptr);
    rbuf  = right->buffer;

    /* Rebuild the left node in place from scratch (no aliasing). */
    demofs_bt_node_init(buf, base, cap, 0);
    for (i = 0; i < split_i; i++) {
        demofs_bt_leaf_append(buf, base, &items[i].key,
                              scratch + items[i].scratch_off, items[i].len);
    }
    for (i = split_i; i < total; i++) {
        demofs_bt_leaf_append(rbuf, 0, &items[i].key,
                              scratch + items[i].scratch_off, items[i].len);
    }

    demofs_bt_hdr(rbuf, 0)->next_leaf = old_next;
    h->next_leaf                      = *sep_bptr;

    *sep_key = items[split_i].key;

    chimera_demofs_abort_if(demofs_bt_leaf_free(buf, base) > cap ||
                            demofs_bt_leaf_free(rbuf, 0) > DEMOFS_BT_NODE_CAP,
                            "b+tree leaf split overflow");

    free(items);
    free(scratch);
} /* demofs_bt_leaf_split */

/* Insert (key,rec) into a leaf, splitting if needed.  Returns 1 on split. */
static int
demofs_bt_leaf_insert(
    struct demofs_thread       *thread,
    struct demofs_txn          *txn,
    void                       *buf,
    uint32_t                    base,
    uint32_t                    cap,
    const struct demofs_bt_key *key,
    const void                 *rec,
    uint32_t                    reclen,
    struct demofs_bt_key       *sep_key,
    uint64_t                   *sep_bptr)
{
    struct demofs_bt_node_hdr *h = demofs_bt_hdr(buf, base);
    struct demofs_bt_lslot    *sl;
    int                        idx, exact, j;

    idx = demofs_bt_leaf_search(buf, base, key, &exact);
    chimera_demofs_abort_if(exact, "b+tree duplicate key insert");

    if (demofs_bt_leaf_free(buf, base) >= sizeof(struct demofs_bt_lslot) + reclen) {
        sl = demofs_bt_lslots(buf, base);
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

    demofs_bt_leaf_split(thread, txn, buf, base, cap, idx, key, rec, reclen,
                         sep_key, sep_bptr);
    return 1;
} /* demofs_bt_leaf_insert */

/* Insert (key,child) into an interior node, splitting if needed. */
static int
demofs_bt_interior_insert(
    struct demofs_thread       *thread,
    struct demofs_txn          *txn,
    void                       *buf,
    uint32_t                    base,
    uint32_t                    cap,
    const struct demofs_bt_key *key,
    uint64_t                    child,
    struct demofs_bt_key       *sep_key,
    uint64_t                   *sep_bptr)
{
    struct demofs_bt_node_hdr *h  = demofs_bt_hdr(buf, base);
    struct demofs_bt_islot    *sl = demofs_bt_islots(buf, base);
    int                        n  = h->nitems;
    int                        idx, j, split_i;
    struct demofs_block       *right;
    struct demofs_bt_islot    *rsl;
    struct demofs_bt_node_hdr *rh;

    /* Find sorted insert position (keys are unique separators). */
    idx = 0;
    while (idx < n && demofs_bt_key_cmp(&sl[idx].key, key) < 0) {
        idx++;
    }

    if (demofs_bt_interior_free(buf, base) >= sizeof(struct demofs_bt_islot)) {
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
        struct demofs_bt_islot all[ (DEMOFS_BT_NODE_CAP / sizeof(struct demofs_bt_islot)) + 2 ];
        int                    total = n + 1;
        int                    p = 0, ins = 0;

        for (j = 0; j < n; j++) {
            if (!ins && demofs_bt_key_cmp(key, &sl[j].key) < 0) {
                all[p].key = *key; all[p].child = child; p++; ins = 1;
            }
            all[p++] = sl[j];
        }
        if (!ins) {
            all[p].key = *key; all[p].child = child; p++;
        }

        split_i = total / 2;

        right = demofs_bt_alloc_node(thread, txn, h->level, sep_bptr);
        rsl   = demofs_bt_islots(right->buffer, 0);
        rh    = demofs_bt_hdr(right->buffer, 0);

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
} /* demofs_bt_interior_insert */

static int
demofs_bt_insert_rec(
    struct demofs_thread       *thread,
    struct demofs_txn          *txn,
    void                       *buf,
    uint32_t                    base,
    uint32_t                    cap,
    const struct demofs_bt_key *key,
    const void                 *rec,
    uint32_t                    reclen,
    struct demofs_bt_key       *sep_key,
    uint64_t                   *sep_bptr)
{
    struct demofs_bt_node_hdr *h = demofs_bt_hdr(buf, base);
    struct demofs_bt_islot    *sl;
    int                        ci, csplit;
    uint64_t                   child_bptr;
    void                      *child_buf;
    struct demofs_bt_key       csep;
    uint64_t                   cbptr;

    if (h->level == 0) {
        return demofs_bt_leaf_insert(thread, txn, buf, base, cap, key, rec, reclen,
                                     sep_key, sep_bptr);
    }

    ci         = demofs_bt_interior_search(buf, base, key);
    sl         = demofs_bt_islots(buf, base);
    child_bptr = sl[ci].child;
    child_buf  = demofs_bt_node_for_write(thread, txn, child_bptr);

    csplit = demofs_bt_insert_rec(thread, txn, child_buf, 0, DEMOFS_BT_NODE_CAP,
                                  key, rec, reclen, &csep, &cbptr);
    if (!csplit) {
        return 0;
    }

    return demofs_bt_interior_insert(thread, txn, buf, base, cap, &csep, cbptr,
                                     sep_key, sep_bptr);
} /* demofs_bt_insert_rec */

/*
 * Insert a record into an inode's b+tree.  The inode must be write-locked
 * and its root block pinned (true for any write op that acquired it).
 */
static void
demofs_bt_insert(
    struct demofs_thread       *thread,
    struct demofs_txn          *txn,
    struct demofs_inode        *inode,
    const struct demofs_bt_key *key,
    const void                 *rec,
    uint32_t                    reclen)
{
    void                *root = inode->block->buffer;
    struct demofs_bt_key sep;
    uint64_t             sep_bptr;
    int                  split;

    split = demofs_bt_insert_rec(thread, txn, root, DEMOFS_BT_ROOT_BASE,
                                 DEMOFS_BT_ROOT_CAP, key, rec, reclen, &sep, &sep_bptr);
    if (!split) {
        return;
    }

    /* Root overflowed: grow the tree.  Move the (post-split lower-half)
     * root contents into a new left child, then re-form the root as an
     * interior node pointing at the new left child and the split's right
     * sibling. */
    {
        struct demofs_bt_node_hdr *rh        = demofs_bt_hdr(root, DEMOFS_BT_ROOT_BASE);
        uint16_t                   old_level = rh->level;
        uint64_t                   left_bptr;
        struct demofs_block       *left;
        struct demofs_bt_key       left_min;
        struct demofs_bt_islot    *isl;

        left = demofs_bt_alloc_node(thread, txn, old_level, &left_bptr);

        /* Copy the entire embedded root node into the new left block. */
        memcpy((char *) left->buffer, (char *) root + DEMOFS_BT_ROOT_BASE, DEMOFS_BT_ROOT_CAP);
        demofs_bt_hdr(left->buffer, 0)->capacity = DEMOFS_BT_NODE_CAP;

        if (old_level == 0) {
            left_min = demofs_bt_lslots(left->buffer, 0)[0].key;
        } else {
            left_min = demofs_bt_islots(left->buffer, 0)[0].key;
        }

        demofs_bt_node_init(root, DEMOFS_BT_ROOT_BASE, DEMOFS_BT_ROOT_CAP,
                            old_level + 1);
        rh           = demofs_bt_hdr(root, DEMOFS_BT_ROOT_BASE);
        rh->nitems   = 2;
        isl          = demofs_bt_islots(root, DEMOFS_BT_ROOT_BASE);
        isl[0].key   = left_min;
        isl[0].child = left_bptr;
        isl[1].key   = sep;
        isl[1].child = sep_bptr;
    }
} /* demofs_bt_insert */

/*
 * Remove a key from an inode's b+tree (lazy: no merge/rebalance).  Returns
 * 1 if removed, 0 if not found.  The descent dirties the path blocks.
 */
static int
demofs_bt_remove(
    struct demofs_thread       *thread,
    struct demofs_txn          *txn,
    struct demofs_inode        *inode,
    const struct demofs_bt_key *key)
{
    void    *buf  = inode->block->buffer;
    uint32_t base = DEMOFS_BT_ROOT_BASE;

    for (;; ) {
        struct demofs_bt_node_hdr *h = demofs_bt_hdr(buf, base);

        if (h->level == 0) {
            struct demofs_bt_lslot *sl = demofs_bt_lslots(buf, base);
            int                     idx, exact, j;

            idx = demofs_bt_leaf_search(buf, base, key, &exact);
            if (!exact) {
                return 0;
            }
            for (j = idx; j < h->nitems - 1; j++) {
                sl[j] = sl[j + 1];
            }
            h->nitems--;
            return 1;
        } else {
            int ci = demofs_bt_interior_search(buf, base, key);

            buf = demofs_bt_node_for_write(thread, txn,
                                           demofs_bt_islots(buf, base)[ci].child);
            base = 0;
        }
    }
} /* demofs_bt_remove */

/*
 * Copy out the record for an exact key match.  Read path: traverses via the
 * RCU block cache, one critical section per node.  Returns the record
 * length (copied into out, up to out_cap) or -1 if not found.  Phase 1
 * aborts if a needed node is not resident.
 */
static int
demofs_bt_lookup_exact(
    struct demofs_thread       *thread,
    struct demofs_inode        *inode,
    const struct demofs_bt_key *key,
    void                       *out,
    uint32_t                    out_cap)
{
    uint32_t device_id;
    uint64_t device_offset;
    uint64_t bptr = 0;
    void    *buf;
    uint32_t base;
    int      result   = -1;
    int      use_root = 1;

    device_offset = sm_inum_to_device_offset(thread->shared->space_map, inode->inum, &device_id);

    for (;; ) {
        struct demofs_block *blk;

        urcu_memb_read_lock();
        if (use_root) {
            blk = demofs_block_lookup_rcu(thread, device_id, device_offset);
        } else {
            uint32_t cdev;
            uint64_t coff = sm_inum_to_device_offset(thread->shared->space_map, bptr, &cdev);
            blk = demofs_block_lookup_rcu(thread, cdev, coff);
        }
        chimera_demofs_abort_if(!blk, "b+tree read: node not resident (inum %lu)", inode->inum);

        buf  = blk->buffer;
        base = use_root ? DEMOFS_BT_ROOT_BASE : 0;

        struct demofs_bt_node_hdr *h = demofs_bt_hdr(buf, base);
        if (h->level == 0) {
            int exact;
            int idx = demofs_bt_leaf_search(buf, base, key, &exact);
            if (exact) {
                struct demofs_bt_lslot *sl  = demofs_bt_lslots(buf, base);
                uint32_t                len = sl[idx].len;
                if (len > out_cap) {
                    len = out_cap;
                }
                memcpy(out, (char *) buf + base + sl[idx].off, len);
                result = (int) sl[idx].len;
            }
            urcu_memb_read_unlock();
            return result;
        } else {
            int ci = demofs_bt_interior_search(buf, base, key);
            bptr = demofs_bt_islots(buf, base)[ci].child;
            urcu_memb_read_unlock();
            use_root = 0;
        }
    }
} /* demofs_bt_lookup_exact */

/* Descend (floor-style) to the leaf that would hold key.  Returns the leaf
 * block (RCU lookup; aborts if not resident) without entering a critical
 * section; the caller must re-enter RCU around its own access.  Used as a
 * helper only where the whole traversal is inside one logic block. */

/*
 * Smallest key >= search key (ceil).  Copies the found key into *r_key and
 * the record into out; returns record length, or -1 if no such key.
 */
static int
demofs_bt_lookup_ge(
    struct demofs_thread       *thread,
    struct demofs_inode        *inode,
    const struct demofs_bt_key *key,
    struct demofs_bt_key       *r_key,
    void                       *out,
    uint32_t                    out_cap)
{
    uint32_t device_id;
    uint64_t device_offset;
    uint64_t bptr      = 0;
    int      use_root  = 1;
    uint64_t next_leaf = 0;

    device_offset = sm_inum_to_device_offset(thread->shared->space_map, inode->inum, &device_id);

    for (;; ) {
        struct demofs_block       *blk;
        struct demofs_bt_node_hdr *h;
        void                      *buf;
        uint32_t                   base;

        urcu_memb_read_lock();
        if (use_root) {
            blk = demofs_block_lookup_rcu(thread, device_id, device_offset);
        } else {
            uint32_t cdev;
            uint64_t coff = sm_inum_to_device_offset(thread->shared->space_map, bptr, &cdev);
            blk = demofs_block_lookup_rcu(thread, cdev, coff);
        }
        chimera_demofs_abort_if(!blk, "b+tree read: node not resident (inum %lu)", inode->inum);

        buf  = blk->buffer;
        base = use_root ? DEMOFS_BT_ROOT_BASE : 0;
        h    = demofs_bt_hdr(buf, base);

        if (h->level == 0) {
            int exact;
            int idx = demofs_bt_leaf_search(buf, base, key, &exact);

            if (idx < h->nitems) {
                struct demofs_bt_lslot *sl  = demofs_bt_lslots(buf, base);
                uint32_t                len = sl[idx].len;

                *r_key = sl[idx].key;
                if (len > out_cap) {
                    len = out_cap;
                }
                memcpy(out, (char *) buf + base + sl[idx].off, len);
                len = sl[idx].len;
                urcu_memb_read_unlock();
                return (int) len;
            }
            next_leaf = h->next_leaf;
            urcu_memb_read_unlock();

            if (next_leaf == 0) {
                return -1;     /* no key >= search */
            }
            /* Successor is the first slot of the next leaf. */
            bptr     = next_leaf;
            use_root = 0;
            {
                uint32_t cdev;
                uint64_t coff = sm_inum_to_device_offset(thread->shared->space_map, bptr, &cdev);

                urcu_memb_read_lock();
                blk = demofs_block_lookup_rcu(thread, cdev, coff);
                chimera_demofs_abort_if(!blk, "b+tree read: next leaf not resident");
                h = demofs_bt_hdr(blk->buffer, 0);
                if (h->nitems == 0) {
                    urcu_memb_read_unlock();
                    return -1;
                }
                struct demofs_bt_lslot *sl  = demofs_bt_lslots(blk->buffer, 0);
                uint32_t                len = sl[0].len;
                *r_key = sl[0].key;
                if (len > out_cap) {
                    len = out_cap;
                }
                memcpy(out, (char *) blk->buffer + sl[0].off, len);
                len = sl[0].len;
                urcu_memb_read_unlock();
                return (int) len;
            }
        } else {
            int ci = demofs_bt_interior_search(buf, base, key);
            bptr = demofs_bt_islots(buf, base)[ci].child;
            urcu_memb_read_unlock();
            use_root = 0;
        }
    }
} /* demofs_bt_lookup_ge */

/*
 * Largest key <= search key (floor).  Copies the found key into *r_key and
 * the record into out; returns record length, or -1 if no such key.
 */
static int
demofs_bt_lookup_le(
    struct demofs_thread       *thread,
    struct demofs_inode        *inode,
    const struct demofs_bt_key *key,
    struct demofs_bt_key       *r_key,
    void                       *out,
    uint32_t                    out_cap)
{
    uint32_t device_id;
    uint64_t device_offset;
    uint64_t bptr     = 0;
    int      use_root = 1;

    device_offset = sm_inum_to_device_offset(thread->shared->space_map, inode->inum, &device_id);

    for (;; ) {
        struct demofs_block       *blk;
        struct demofs_bt_node_hdr *h;
        void                      *buf;
        uint32_t                   base;

        urcu_memb_read_lock();
        if (use_root) {
            blk = demofs_block_lookup_rcu(thread, device_id, device_offset);
        } else {
            uint32_t cdev;
            uint64_t coff = sm_inum_to_device_offset(thread->shared->space_map, bptr, &cdev);
            blk = demofs_block_lookup_rcu(thread, cdev, coff);
        }
        chimera_demofs_abort_if(!blk, "b+tree read: node not resident (inum %lu)", inode->inum);

        buf  = blk->buffer;
        base = use_root ? DEMOFS_BT_ROOT_BASE : 0;
        h    = demofs_bt_hdr(buf, base);

        if (h->level == 0) {
            int exact;
            int idx = demofs_bt_leaf_search(buf, base, key, &exact);
            int fidx;

            if (h->nitems == 0) {
                urcu_memb_read_unlock();
                return -1;
            }
            fidx = exact ? idx : idx - 1;
            if (fidx < 0) {
                urcu_memb_read_unlock();
                return -1;     /* no key <= search in this (leftmost) leaf */
            }
            {
                struct demofs_bt_lslot *sl  = demofs_bt_lslots(buf, base);
                uint32_t                len = sl[fidx].len;

                *r_key = sl[fidx].key;
                if (len > out_cap) {
                    len = out_cap;
                }
                memcpy(out, (char *) buf + base + sl[fidx].off, len);
                len = sl[fidx].len;
                urcu_memb_read_unlock();
                return (int) len;
            }
        } else {
            int ci = demofs_bt_interior_search(buf, base, key);
            bptr = demofs_bt_islots(buf, base)[ci].child;
            urcu_memb_read_unlock();
            use_root = 0;
        }
    }
} /* demofs_bt_lookup_le */

/* ------------------------------------------------------------------ */
/* Directory / symlink records over the inode b+tree                   */
/* ------------------------------------------------------------------ */

static inline struct demofs_bt_key
demofs_dirent_key(uint64_t hash)
{
    struct demofs_bt_key k = { .type = DEMOFS_REC_DIRENT, .subkey = hash };

    return k;
} /* demofs_dirent_key */

static void
demofs_dir_insert(
    struct demofs_thread *thread,
    struct demofs_txn    *txn,
    struct demofs_inode  *dir,
    uint64_t              hash,
    const char           *name,
    int                   namelen,
    uint64_t              child_inum,
    uint32_t              child_gen)
{
    char                      buf[DEMOFS_DIRENT_REC_MAX];
    struct demofs_dirent_rec *r   = (struct demofs_dirent_rec *) buf;
    struct demofs_bt_key      key = demofs_dirent_key(hash);

    r->inum     = child_inum;
    r->gen      = child_gen;
    r->name_len = (uint16_t) namelen;
    memcpy(r->name, name, namelen);

    demofs_bt_insert(thread, txn, dir, &key,
                     buf, sizeof(*r) + namelen);
} /* demofs_dir_insert */

/* Returns 0 and fills the inum/gen out params if found, -1 otherwise. */
static int
demofs_dir_lookup(
    struct demofs_thread *thread,
    struct demofs_inode  *dir,
    uint64_t              hash,
    uint64_t             *r_inum,
    uint32_t             *r_gen)
{
    char                      buf[DEMOFS_DIRENT_REC_MAX];
    struct demofs_dirent_rec *r   = (struct demofs_dirent_rec *) buf;
    struct demofs_bt_key      key = demofs_dirent_key(hash);
    int                       len;

    len = demofs_bt_lookup_exact(thread, dir, &key, buf, sizeof(buf));
    if (len < 0) {
        return -1;
    }
    *r_inum = r->inum;
    *r_gen  = r->gen;
    return 0;
} /* demofs_dir_lookup */

static int
demofs_dir_remove(
    struct demofs_thread *thread,
    struct demofs_txn    *txn,
    struct demofs_inode  *dir,
    uint64_t              hash)
{
    struct demofs_bt_key key = demofs_dirent_key(hash);

    return demofs_bt_remove(thread, txn, dir, &key);
} /* demofs_dir_remove */

/*
 * Find the next directory entry whose hash is >= from_hash.  Returns 0 and
 * fills the out params (the entry's hash, child inum/gen, name) or -1 when
 * there are no more dirents.
 */
static int
demofs_dir_next(
    struct demofs_thread *thread,
    struct demofs_inode  *dir,
    uint64_t              from_hash,
    uint64_t             *r_hash,
    uint64_t             *r_inum,
    uint32_t             *r_gen,
    char                 *name,
    int                  *r_namelen)
{
    char                      buf[DEMOFS_DIRENT_REC_MAX];
    struct demofs_dirent_rec *r   = (struct demofs_dirent_rec *) buf;
    struct demofs_bt_key      key = demofs_dirent_key(from_hash);
    struct demofs_bt_key      found;
    int                       len;

    len = demofs_bt_lookup_ge(thread, dir, &key, &found, buf, sizeof(buf));
    if (len < 0 || found.type != DEMOFS_REC_DIRENT) {
        return -1;
    }
    *r_hash    = found.subkey;
    *r_inum    = r->inum;
    *r_gen     = r->gen;
    *r_namelen = r->name_len;
    memcpy(name, r->name, r->name_len);
    return 0;
} /* demofs_dir_next */

static void
demofs_symlink_set(
    struct demofs_thread *thread,
    struct demofs_txn    *txn,
    struct demofs_inode  *inode,
    const void           *target,
    int                   len)
{
    struct demofs_bt_key key = { .type = DEMOFS_REC_SYMLINK, .subkey = 0 };

    demofs_bt_insert(thread, txn, inode, &key, target, len);
} /* demofs_symlink_set */

static int
demofs_symlink_get(
    struct demofs_thread *thread,
    struct demofs_inode  *inode,
    void                 *out,
    uint32_t              cap)
{
    struct demofs_bt_key key = { .type = DEMOFS_REC_SYMLINK, .subkey = 0 };

    return demofs_bt_lookup_exact(thread, inode, &key, out, cap);
} /* demofs_symlink_get */

static inline struct demofs_extent *
demofs_extent_alloc(struct demofs_thread *thread)
{
    struct demofs_extent *extent;

    extent = slab_allocator_alloc(thread->allocator, sizeof(struct demofs_extent));

    return extent;
} /* demofs_extent_alloc */ /* demofs_extent_alloc */ /* demofs_extent_alloc */

static inline void
demofs_extent_free(
    struct demofs_thread *thread,
    struct demofs_extent *extent)
{
    slab_allocator_free(thread->allocator, extent, sizeof(*extent));
} /* demofs_extent_free */ /* demofs_extent_free */ /* demofs_extent_free */

static inline void
demofs_extent_release(
    struct rb_node *node,
    void           *private_data)
{
    struct demofs_thread *thread = private_data;
    struct demofs_extent *extent = container_of(node, struct demofs_extent, node);

    if (thread) {
        slab_allocator_free(thread->allocator, extent, sizeof(*extent));
    }
} /* demofs_extent_release */

static inline struct demofs_symlink_target *
demofs_symlink_target_alloc(
    struct demofs_thread *thread,
    const char           *data,
    int                   length)
{
    struct demofs_symlink_target *target;

    target = slab_allocator_alloc(thread->allocator, sizeof(struct demofs_symlink_target));

    target->data = slab_allocator_alloc(thread->allocator, length);

    target->length = length;

    memcpy(target->data, data, length);

    return target;
} /* demofs_symlink_target_alloc */

static inline void
demofs_symlink_target_free(
    struct demofs_thread         *thread,
    struct demofs_symlink_target *target)
{
    slab_allocator_free(thread->allocator, target->data, target->length);
    slab_allocator_free(thread->allocator, target, sizeof(*target));
} /* demofs_symlink_target_free */


/*
 * Allocate a bare inode struct for a freshly-minted inum.  The 4 KiB
 * metadata block on storage has already been carved out of the space map;
 * for now we leak that block and only use its address to derive the inum.
 */
static inline struct demofs_inode *
demofs_inode_struct_new(uint64_t inum)
{
    struct demofs_inode *inode = calloc(1, sizeof(*inode));

    inode->inum   = inum;
    inode->gen    = 1;
    inode->refcnt = 1;
    /* readers/writer/wait_head/wait_tail zeroed by calloc */
    return inode;
} /* demofs_inode_struct_new */

static inline void
demofs_inode_cache_insert(
    struct demofs_shared *shared,
    struct demofs_inode  *inode)
{
    struct demofs_inode_shard *shard = demofs_inode_shard(shared, inode->inum);

    pthread_mutex_lock(&shard->lock);
    rb_tree_insert(&shard->inodes, inum, inode);
    pthread_mutex_unlock(&shard->lock);
} /* demofs_inode_cache_insert */

/*
 * Allocate a new inode: grab a 4 KiB metadata block from the space map to
 * mint the inum, create the in-memory inode, publish it write-locked into
 * the cache, and record it in the transaction.
 */
static inline void
demofs_inode_alloc_async(
    struct demofs_thread *thread,
    struct demofs_txn    *txn,
    demofs_inode_cb_t     cb,
    void                 *private_data)
{
    struct demofs_shared *shared = thread->shared;
    struct demofs_inode  *inode;
    uint32_t              device_id;
    uint64_t              device_offset, inum;
    int                   rc;

    rc = space_map_alloc(shared->space_map, &thread->space_cache,
                         SM_BLOCK_SIZE, &device_id, &device_offset);
    if (unlikely(rc != 0)) {
        cb(NULL, CHIMERA_VFS_ENOSPC, private_data);
        return;
    }

    inum  = sm_inum_from_device_offset(shared->space_map, device_id, device_offset);
    inode = demofs_inode_struct_new(inum);

    /* New dirty inode: write-locked by this (write) txn from birth. */
    inode->writer = 1;

    demofs_inode_cache_insert(shared, inode);
    demofs_txn_add_slot(txn, inode, DEMOFS_INODE_LOCK_WRITE);

    /* Claim and pin the inode's freshly-allocated home block. */
    demofs_txn_pin_inode_block(thread, txn, inode, 1);

    cb(inode, CHIMERA_VFS_OK, private_data);
} /* demofs_inode_alloc_async */

static void
demofs_dirent_release(
    struct rb_node *node,
    void           *private_data)
{
    struct demofs_thread *thread = private_data;
    struct demofs_dirent *dirent = container_of(node, struct demofs_dirent, node);

    if (thread) {
        slab_allocator_free(thread->allocator, dirent, sizeof(*dirent));
    }
} /* demofs_dirent_release */

/*
 * Tear down an inode's contents when its last link/reference is dropped.
 * We bump the generation so stale file handles return ESTALE, but we do
 * NOT remove it from the cache or free the struct yet (no eviction), and
 * we leak its 4 KiB metadata block.  The caller still holds the inode's
 * write lock via the transaction; the lock is released at commit.
 */
static inline void
demofs_inode_free(
    struct demofs_thread *thread,
    struct demofs_inode  *inode)
{
    if (S_ISREG(inode->mode)) {
        rb_tree_destroy(&inode->file.extents, demofs_extent_release, thread);
        rb_tree_init(&inode->file.extents);
    }
    /* Directory / symlink contents live in the inode's b+tree blocks, which
     * are leaked for now (no space reclaim yet). */

    inode->gen++;
    inode->refcnt = 0;
} /* demofs_inode_free */

static inline struct demofs_dirent *
demofs_dirent_alloc(
    struct demofs_thread *thread,
    uint64_t              inum,
    uint32_t              gen,
    uint64_t              hash,
    const char           *name,
    int                   name_len)
{
    struct demofs_dirent *dirent = slab_allocator_alloc(thread->allocator, sizeof(struct demofs_dirent));

    dirent->inum     = inum;
    dirent->gen      = gen;
    dirent->hash     = hash;
    dirent->name_len = name_len;

    dirent->name = slab_allocator_alloc(thread->allocator, name_len);
    memcpy(dirent->name, name, name_len);

    return dirent;

} /* demofs_dirent_alloc */

static void
demofs_dirent_free(
    struct demofs_thread *thread,
    struct demofs_dirent *dirent)
{
    slab_allocator_free(thread->allocator, dirent->name, dirent->name_len);
    slab_allocator_free(thread->allocator, dirent, sizeof(*dirent));
} /* demofs_dirent_free */


static inline struct demofs_kv_entry *
demofs_kv_entry_alloc(
    struct demofs_thread *thread,
    uint64_t              hash,
    const void           *key,
    uint32_t              key_len,
    const void           *value,
    uint32_t              value_len)
{
    struct demofs_kv_entry *entry;

    entry = slab_allocator_alloc(thread->allocator, sizeof(*entry));

    entry->hash      = hash;
    entry->key_len   = key_len;
    entry->value_len = value_len;
    entry->key       = slab_allocator_alloc(thread->allocator, key_len);
    entry->value     = slab_allocator_alloc(thread->allocator, value_len);
    memcpy(entry->key, key, key_len);
    memcpy(entry->value, value, value_len);

    return entry;
} /* demofs_kv_entry_alloc */

static void
demofs_kv_entry_free(
    struct demofs_thread   *thread,
    struct demofs_kv_entry *entry)
{
    slab_allocator_free(thread->allocator, entry->key, entry->key_len);
    slab_allocator_free(thread->allocator, entry->value, entry->value_len);
    slab_allocator_free(thread->allocator, entry, sizeof(*entry));
} /* demofs_kv_entry_free */

static void
demofs_kv_entry_release(
    struct rb_node *node,
    void           *private_data)
{
    struct demofs_kv_entry *entry = container_of(node, struct demofs_kv_entry, node);

    free(entry->key);
    free(entry->value);
    free(entry);
} /* demofs_kv_entry_release */

static inline int
demofs_thread_alloc_space(
    struct demofs_thread *thread,
    int64_t               desired_size,
    uint64_t             *r_device_id,
    uint64_t             *r_device_offset)
{
    uint32_t dev_id;
    int      rc;

    rc = space_map_alloc(thread->shared->space_map, &thread->space_cache,
                         (uint64_t) desired_size, &dev_id, r_device_offset);

    if (rc != 0) {
        return CHIMERA_VFS_ENOSPC;
    }

    *r_device_id = dev_id;
    return 0;
} /* demofs_thread_alloc_space */

static inline void
demofs_thread_free_space(
    struct demofs_thread *thread,
    uint32_t              device_id,
    uint64_t              device_offset,
    uint64_t              length)
{
    space_map_free(thread->shared->space_map, device_id, device_offset, length);
} /* demofs_thread_free_space */

/* ------------------------------------------------------------------ */
/* Transaction plumbing                                                */
/* ------------------------------------------------------------------ */

static inline struct demofs_txn *
demofs_txn_begin(
    struct demofs_thread *thread,
    enum demofs_txn_type  type)
{
    struct demofs_txn *txn = thread->txn_free_list;

    if (txn) {
        thread->txn_free_list = txn->next;
    } else {
        txn = malloc(sizeof(*txn));
    }

    txn->type       = type;
    txn->thread     = thread;
    txn->next       = NULL;
    txn->num_inodes = 0;
    txn->blocks     = NULL;
    return txn;
} /* demofs_txn_begin */

static inline void
demofs_txn_release(struct demofs_txn *txn)
{
    struct demofs_thread *thread = txn->thread;

    txn->next             = thread->txn_free_list;
    thread->txn_free_list = txn;
} /* demofs_txn_release */

static inline void
demofs_txn_abort(struct demofs_txn *txn)
{
    /* Nothing to undo today; future intent records and deferred frees
     * get discarded here.  Drop any blocks the aborted txn pinned (their
     * contents are discarded) and release the inode locks. */
    demofs_txn_unpin_blocks(txn, DEMOFS_BLOCK_CLEAN);
    demofs_txn_unlock_all(txn);
    demofs_txn_release(txn);
} /* demofs_txn_abort */

/* Synchronous commit today; placeholder for the intent log routing
 * added in phase 3. */
/* Forward decls — definition below. */
static inline void demofs_txn_commit(
    struct demofs_txn     *txn,
    demofs_txn_commit_cb_t cb,
    void                  *private_data);

static void
demofs_txn_request_complete_cb(
    struct demofs_txn *txn,
    int                status,
    void              *private_data)
{
    struct chimera_vfs_request *request = private_data;

    (void) txn;
    if (status != 0 && request->status == CHIMERA_VFS_OK) {
        request->status = status;
    }
    request->complete(request);
} /* demofs_txn_request_complete_cb */

/* Op-level helpers: every op ends with one of these. */
static inline void
demofs_op_fail(
    struct chimera_vfs_request *request,
    struct demofs_txn          *txn,
    int                         status)
{
    request->status = status;
    if (txn) {
        demofs_txn_abort(txn);
    }
    request->complete(request);
} /* demofs_op_fail */

static inline void
demofs_op_ok(
    struct chimera_vfs_request *request,
    struct demofs_txn          *txn)
{
    request->status = CHIMERA_VFS_OK;
    if (txn) {
        demofs_txn_commit(txn, demofs_txn_request_complete_cb, request);
    } else {
        request->complete(request);
    }
} /* demofs_op_ok */

/* ------------------------------------------------------------------ */
/* Intent log: SPSC ring helpers + doorbell callbacks + thread         */
/* ------------------------------------------------------------------ */

/* Completion context for an in-flight redo-record write. */
struct demofs_redo_ctx {
    struct demofs_intent_log *il;
    struct demofs_iq_channel *ch;
    struct demofs_iq_entry    entry;
    struct evpl_iovec         iov;
};

/*
 * Runs on the intent-log thread when a redo record has been written
 * durably.  The transaction's changes are now recoverable, so drop the
 * block pins (-> LOGGED, awaiting tail-push) and the inode locks, then push
 * the completion onto the worker's CQ.
 */
static void
demofs_redo_write_cb(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct demofs_redo_ctx   *ctx = private_data;
    struct demofs_iq_channel *ch  = ctx->ch;
    struct demofs_intent_log *il  = ctx->il;
    uint32_t                  cq_tail;

    (void) evpl;

    demofs_txn_unpin_blocks(ctx->entry.txn, DEMOFS_BLOCK_LOGGED);
    demofs_txn_unlock_all(ctx->entry.txn);

    ctx->entry.status = status;

    cq_tail                                       = __atomic_load_n(&ch->cq.tail, __ATOMIC_RELAXED);
    ch->cq.entries[cq_tail & DEMOFS_IQ_RING_MASK] = ctx->entry;
    __atomic_store_n(&ch->cq.tail, cq_tail + 1, __ATOMIC_RELEASE);

    ch->cq_inflight--;

    evpl_iovec_release(il->evpl, &ctx->iov);
    free(ctx);

    evpl_ring_doorbell(&ch->cq_doorbell);
} /* demofs_redo_write_cb */

/*
 * Build a full-block redo record for one transaction and issue a durable
 * write into the reserved intent-log region.  Runs on the intent-log thread.
 */
static void
demofs_il_write_redo(
    struct demofs_intent_log *il,
    struct demofs_iq_channel *ch,
    struct demofs_iq_entry   *entry)
{
    struct demofs_txn               *txn = entry->txn;
    struct demofs_txn_block         *tb;
    struct demofs_redo_ctx          *ctx;
    struct demofs_redo_header       *hdr;
    struct demofs_redo_block_header *bh;
    uint32_t                         nblocks = 0;
    uint64_t                         reclen, offset;
    char                            *p;
    int                              niov;

    for (tb = txn->blocks; tb; tb = tb->next) {
        nblocks++;
    }

    reclen = sizeof(struct demofs_redo_header) +
        (uint64_t) nblocks * (sizeof(struct demofs_redo_block_header) + DEMOFS_BLOCK_SIZE);
    reclen = (reclen + DEMOFS_BLOCK_SIZE - 1) & ~((uint64_t) DEMOFS_BLOCK_SIZE - 1);

    /* Circular region; no tail/checkpoint yet, so just wrap and overwrite. */
    if (il->log_head + reclen > SM_INTENT_LOG_OFFSET + SM_INTENT_LOG_SIZE) {
        il->log_head = SM_INTENT_LOG_OFFSET;
    }
    offset        = il->log_head;
    il->log_head += reclen;

    ctx        = malloc(sizeof(*ctx));
    ctx->il    = il;
    ctx->ch    = ch;
    ctx->entry = *entry;

    niov = evpl_iovec_alloc(il->evpl, reclen, DEMOFS_BLOCK_SIZE, 1, 0, &ctx->iov);
    chimera_demofs_abort_if(niov != 1, "redo record did not fit in one iovec (%d)", niov);

    p               = ctx->iov.data;
    hdr             = (struct demofs_redo_header *) p;
    hdr->magic      = DEMOFS_REDO_MAGIC;
    hdr->seq        = il->log_seq++;
    hdr->num_blocks = nblocks;
    hdr->reclen     = (uint32_t) reclen;
    p              += sizeof(*hdr);

    for (tb = txn->blocks; tb; tb = tb->next) {
        struct demofs_block *blk = tb->block;

        bh                = (struct demofs_redo_block_header *) p;
        bh->device_id     = blk->device_id;
        bh->pad           = 0;
        bh->device_offset = blk->device_offset;
        p                += sizeof(*bh);

        memcpy(p, blk->buffer, DEMOFS_BLOCK_SIZE);
        p += DEMOFS_BLOCK_SIZE;
    }

    ch->cq_inflight++;

    evpl_block_write(il->evpl, il->queue[SM_INTENT_LOG_DEVICE],
                     &ctx->iov, 1, offset, 1 /* sync */,
                     demofs_redo_write_cb, ctx);
} /* demofs_il_write_redo */

static void
demofs_iq_process_channel(struct demofs_iq_channel *ch)
{
    struct demofs_intent_log *il      = &ch->worker->shared->intent_log;
    uint32_t                  sq_head = __atomic_load_n(&ch->sq.head, __ATOMIC_RELAXED);
    uint32_t                  sq_tail = __atomic_load_n(&ch->sq.tail, __ATOMIC_ACQUIRE);
    int                       issued  = 0;

    while (sq_head != sq_tail) {
        struct demofs_iq_entry entry;
        uint32_t               cq_tail, cq_head;

        cq_tail = __atomic_load_n(&ch->cq.tail, __ATOMIC_RELAXED);
        cq_head = __atomic_load_n(&ch->cq.head, __ATOMIC_ACQUIRE);

        /* Reserve a CQ slot per in-flight write so completions can't
         * overflow the CQ.  Defer if no room; the worker's CQ drain pings
         * us to resume. */
        if ((cq_tail - cq_head) + ch->cq_inflight >= DEMOFS_IQ_RING_SIZE) {
            break;
        }

        entry = ch->sq.entries[sq_head & DEMOFS_IQ_RING_MASK];
        sq_head++;
        entry.status = 0;

        /* Issue a durable redo write; the completion drops pins/locks and
         * pushes the CQE (see demofs_redo_write_cb). */
        demofs_il_write_redo(il, ch, &entry);
        issued++;
    }

    if (issued > 0) {
        __atomic_store_n(&ch->sq.head, sq_head, __ATOMIC_RELEASE);
    }
} /* demofs_iq_process_channel */

static void
demofs_intent_log_drain_pending(struct demofs_intent_log *il)
{
    struct demofs_iq_channel *head, *ch;

    pthread_mutex_lock(&il->registration_lock);
    head             = il->pending_head;
    il->pending_head = NULL;
    pthread_mutex_unlock(&il->registration_lock);

    while (head) {
        ch               = head;
        head             = ch->next_pending;
        ch->next_pending = NULL;

        chimera_demofs_abort_if(il->num_channels >= DEMOFS_IL_MAX_CHANNELS,
                                "intent log: too many channels (%u >= %u)",
                                il->num_channels, DEMOFS_IL_MAX_CHANNELS);
        il->channels[il->num_channels++] = ch;
    }
} /* demofs_intent_log_drain_pending */

static void
demofs_intent_log_wake_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct demofs_intent_log *il = container_of(doorbell,
                                                struct demofs_intent_log,
                                                wake_doorbell);
    uint32_t                  i;

    (void) evpl;

    demofs_intent_log_drain_pending(il);

    /* Unregister pass: compact slots out (swap-with-tail). */
    i = 0;
    while (i < il->num_channels) {
        struct demofs_iq_channel *ch = il->channels[i];

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
        demofs_iq_process_channel(il->channels[i]);
    }
} /* demofs_intent_log_wake_cb */

static void
demofs_iq_cq_doorbell_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct demofs_iq_channel *ch = container_of(doorbell,
                                                struct demofs_iq_channel,
                                                cq_doorbell);
    uint32_t                  head    = __atomic_load_n(&ch->cq.head, __ATOMIC_RELAXED);
    uint32_t                  tail    = __atomic_load_n(&ch->cq.tail, __ATOMIC_ACQUIRE);
    int                       drained = 0;

    (void) evpl;

    while (head != tail) {
        struct demofs_iq_entry entry = ch->cq.entries[head & DEMOFS_IQ_RING_MASK];
        head++;
        drained++;

        /* The txn's logical inode locks were already dropped by the intent
         * log thread (demofs_iq_process_channel); just deliver completion. */
        entry.cb(entry.txn, entry.status, entry.private_data);
        demofs_txn_release(entry.txn);
    }

    if (drained > 0) {
        __atomic_store_n(&ch->cq.head, head, __ATOMIC_RELEASE);
        /* Wake the intent log thread in case it had deferred work on
         * this channel because the CQ was full. */
        evpl_ring_doorbell(&ch->worker->shared->intent_log.wake_doorbell);
    }
} /* demofs_iq_cq_doorbell_cb */

static void *
demofs_intent_log_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct demofs_intent_log *il     = private_data;
    struct demofs_shared     *shared = container_of(il, struct demofs_shared, intent_log);
    int                       i;

    il->evpl     = evpl;
    il->log_head = SM_INTENT_LOG_OFFSET;
    il->log_seq  = 0;

    /* Open block queues on this thread's evpl for redo-record writes. */
    il->queue = calloc(shared->num_devices, sizeof(*il->queue));
    for (i = 0; i < shared->num_devices; i++) {
        il->queue[i] = evpl_block_open_queue(evpl, shared->devices[i].bdev);
    }

    evpl_add_doorbell(evpl, &il->wake_doorbell, demofs_intent_log_wake_cb);
    __atomic_store_n(&il->ready, 1, __ATOMIC_RELEASE);
    return il;
} /* demofs_intent_log_thread_init */

static void
demofs_intent_log_thread_shutdown(
    struct evpl *evpl,
    void        *private_data)
{
    struct demofs_intent_log *il     = private_data;
    struct demofs_shared     *shared = container_of(il, struct demofs_shared, intent_log);
    int                       i;

    /* By the time this runs the VFS layer has destroyed the worker
     * threads, which freed their channels.  Just drop the doorbell and
     * close our block queues. */
    evpl_remove_doorbell(evpl, &il->wake_doorbell);

    for (i = 0; i < shared->num_devices; i++) {
        evpl_block_close_queue(evpl, il->queue[i]);
    }
    free(il->queue);
} /* demofs_intent_log_thread_shutdown */

static inline void
demofs_txn_commit(
    struct demofs_txn     *txn,
    demofs_txn_commit_cb_t cb,
    void                  *private_data)
{
    struct demofs_thread     *thread = txn->thread;
    struct demofs_shared     *shared;
    struct demofs_iq_channel *ch;
    struct demofs_iq_entry   *slot;
    uint32_t                  tail, head;

    if (txn->type == DEMOFS_TXN_READ) {
        /* Read txns don't need durability -- complete inline. */
        demofs_txn_unlock_all(txn);
        cb(txn, 0, private_data);
        demofs_txn_release(txn);
        return;
    }

    /* Serialize every dirty inode into its block buffer now, on the worker
     * that owns the live inodes under write lock, before handing the txn
     * (and its pinned blocks) to the intent log thread. */
    demofs_txn_flush_inodes(txn);

    /* Write txn -> intent log thread via this worker's SQ.  The intent
     * log thread drops the txn's logical inode locks when it processes
     * the entry (see demofs_iq_process_channel); these are logical locks
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
    while (tail - head >= DEMOFS_IQ_RING_SIZE) {
        evpl_continue(thread->evpl);
        head = __atomic_load_n(&ch->sq.head, __ATOMIC_ACQUIRE);
    }

    slot               = &ch->sq.entries[tail & DEMOFS_IQ_RING_MASK];
    slot->txn          = txn;
    slot->cb           = cb;
    slot->private_data = private_data;
    slot->status       = 0;

    __atomic_store_n(&ch->sq.tail, tail + 1, __ATOMIC_RELEASE);

    evpl_ring_doorbell(&shared->intent_log.wake_doorbell);
} /* demofs_txn_commit */

static void *
demofs_init(const char *cfgdata)
{
    struct demofs_shared       *shared = calloc(1, sizeof(*shared));
    struct demofs_device       *device;
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

    chimera_demofs_abort_if(cfg == NULL, "Error parsing config: %s", json_error.text);

    devices_cfg = json_object_get(cfg, "devices");

    shared->num_devices = json_array_size(devices_cfg);
    shared->devices     = calloc(shared->num_devices, sizeof(*shared->devices));

    json_array_foreach(devices_cfg, i, device_cfg)
    {
        device     = &shared->devices[i];
        device->id = i;

        protocol_name = json_string_value(json_object_get(device_cfg, "type"));
        device_path   = json_string_value(json_object_get(device_cfg, "path"));
        size          = json_integer_value(json_object_get(device_cfg, "size"));

        if (strcmp(protocol_name, "io_uring") == 0) {
            protocol_id = EVPL_BLOCK_PROTOCOL_IO_URING;
        } else if (strcmp(protocol_name, "libaio") == 0) {
            protocol_id = EVPL_BLOCK_PROTOCOL_LIBAIO;
        } else if (strcmp(protocol_name, "vfio") == 0) {
            protocol_id = EVPL_BLOCK_PROTOCOL_VFIO;
        } else {
            chimera_demofs_abort("Unsupported protocol: %s", protocol_name);
        }

        rc = stat(device_path, &st);

        if (rc < 0 && errno == ENOENT) {

            fd = open(device_path, O_CREAT | O_RDWR, 0644);

            chimera_demofs_abort_if(fd < 0, "Failed to open device %s: %s", device_path, strerror(errno));

            rc = ftruncate(fd, size);

            chimera_demofs_abort_if(rc < 0, "Failed to truncate device %s: %s", device_path, strerror(errno));

            close(fd);
        }

        device->bdev = evpl_block_open_device(protocol_id, device_path);

        device->size             = evpl_block_size(device->bdev);
        device->max_request_size = evpl_block_max_request_size(device->bdev);

        chimera_demofs_info("Device %s size %lu max_request_size %lu",
                            device_path, device->size, device->max_request_size);

        if (i == 0) {
            device0_path = strdup(device_path);
        }
    }

    json_decref(cfg);


    pthread_mutex_init(&shared->lock, NULL);

    /* Generate a random 64-bit filesystem ID */
    shared->fsid = chimera_rand64();

    device_sizes = calloc(shared->num_devices, sizeof(*device_sizes));
    for (i = 0; i < shared->num_devices; i++) {
        device_sizes[i] = shared->devices[i].size;
    }
    shared->space_map = space_map_create(device_sizes, shared->num_devices);
    free(device_sizes);

    rc = space_map_write_superblock_path(shared->space_map, device0_path,
                                         shared->fsid);
    chimera_demofs_abort_if(rc != 0,
                            "Failed to write superblock to %s: %s",
                            device0_path, strerror(errno));
    free(device0_path);

    /* Inode cache: sharded rb-trees keyed by inum.  Never evicts yet. */
    shared->inode_cache = calloc(1, sizeof(*shared->inode_cache));
    for (i = 0; i < DEMOFS_INODE_CACHE_SHARDS; i++) {
        rb_tree_init(&shared->inode_cache->shards[i].inodes);
        pthread_mutex_init(&shared->inode_cache->shards[i].lock, NULL);
    }

    /* Block cache: sharded RCU hash of 4 KiB device blocks. */
    demofs_block_cache_create(shared);

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
                                                   demofs_intent_log_thread_init,
                                                   demofs_intent_log_thread_shutdown,
                                                   &shared->intent_log);
    while (!__atomic_load_n(&shared->intent_log.ready, __ATOMIC_ACQUIRE)) {
        /* spin briefly */
    }

    return shared;
} /* demofs_init */

static void
demofs_bootstrap(struct demofs_thread *thread)
{
    struct demofs_shared *shared = thread->shared;
    struct timespec       now;
    struct demofs_inode  *inode;
    uint32_t              device_id;
    uint64_t              device_offset, inum;
    int                   rc;

    /* Guard against concurrent first-touch from multiple workers. */
    pthread_mutex_lock(&shared->lock);
    if (shared->root_fhlen != 0) {
        pthread_mutex_unlock(&shared->lock);
        return;
    }

    clock_gettime(CLOCK_REALTIME, &now);

    /* The very first space-map allocation lands at block_idx 2 of AG 0 /
     * disk 0 (block_idx 1 is reserved at format time), so the root inode
     * gets inum 2. */
    rc = space_map_alloc(shared->space_map, &thread->space_cache,
                         SM_BLOCK_SIZE, &device_id, &device_offset);
    chimera_demofs_abort_if(rc != 0, "bootstrap: failed to allocate root inode block");

    inum = sm_inum_from_device_offset(shared->space_map, device_id, device_offset);
    chimera_demofs_abort_if(inum != 2, "bootstrap: root inode is %lu, expected 2", inum);

    inode = demofs_inode_struct_new(inum);

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

    demofs_inode_cache_insert(shared, inode);

    /* Create the root inode's block in the cache: an embedded empty b+tree
     * root plus the dinode.  Bootstrap is not a transaction, so leave the
     * block resident but unpinned/detached; the first write op that touches
     * root will re-claim and log it. */
    inode->block = demofs_block_claim(thread, device_id, device_offset, 1);
    demofs_bt_node_init(inode->block->buffer, DEMOFS_BT_ROOT_BASE,
                        DEMOFS_BT_ROOT_CAP, 0);
    demofs_inode_flush(inode);
    inode->block->state = DEMOFS_BLOCK_CLEAN;
    __atomic_fetch_sub(&inode->block->pin_count, 1, __ATOMIC_RELAXED);
    inode->block = NULL;

    /* Create 16-byte fsid buffer for root FH encoding (8-byte fsid + 8 bytes padding) */
    {
        uint8_t fsid_buf[CHIMERA_VFS_FSID_SIZE] = { 0 };
        memcpy(fsid_buf, &shared->fsid, sizeof(shared->fsid));
        shared->root_fhlen = chimera_vfs_encode_fh_inum_mount(fsid_buf,
                                                              inode->inum,
                                                              inode->gen,
                                                              shared->root_fh);
    }

    pthread_mutex_unlock(&shared->lock);
} /* demofs_bootstrap */

static void
demofs_inode_cache_release(
    struct rb_node *node,
    void           *private_data)
{
    struct demofs_inode *inode = container_of(node, struct demofs_inode, node);

    (void) private_data;

    /* Process is exiting; thread slab allocators are already gone, so the
     * extent release callback no-ops with a NULL thread.  Directory/symlink
     * b+tree blocks are freed via the block cache.  We still own and must
     * free the inode struct itself (heap-allocated). */
    if (S_ISREG(inode->mode)) {
        rb_tree_destroy(&inode->file.extents, demofs_extent_release, NULL);
    }

    free(inode);
} /* demofs_inode_cache_release */

static void
demofs_destroy(void *private_data)
{
    struct demofs_shared *shared = private_data;
    int                   i;

    for (i = 0; i < DEMOFS_INODE_CACHE_SHARDS; i++) {
        rb_tree_destroy(&shared->inode_cache->shards[i].inodes,
                        demofs_inode_cache_release, NULL);
        pthread_mutex_destroy(&shared->inode_cache->shards[i].lock);
    }

    /* Shut down the intent log thread before tearing down anything it
     * might still touch.  Worker threads have already unregistered their
     * channels via the unregister handshake at this point. */
    __atomic_store_n(&shared->intent_log.shutdown, 1, __ATOMIC_RELEASE);
    evpl_thread_destroy(shared->intent_log.thread);
    pthread_mutex_destroy(&shared->intent_log.registration_lock);

    for (int i = 0; i < shared->num_devices; i++) {
        evpl_block_close_device(shared->devices[i].bdev);
    }

    demofs_block_cache_destroy(shared);

    space_map_destroy(shared->space_map);

    pthread_mutex_destroy(&shared->lock);
    free(shared->devices);
    free(shared->inode_cache);

    /* Clean up KV shards */
    for (i = 0; i < shared->num_kv_shards; i++) {
        rb_tree_destroy(&shared->kv_shards[i].entries, demofs_kv_entry_release, NULL);
        pthread_mutex_destroy(&shared->kv_shards[i].lock);
    }
    free(shared->kv_shards);

    free(shared);
} /* demofs_destroy */ /* demofs_destroy */

/*
 * Runs on a worker thread when another thread has granted it one or more
 * inode locks it was waiting on.  The lock state was already updated by
 * the releasing thread; here we record the slot (on this, the txn's own
 * thread) and resume the parked continuation.
 */
static void
demofs_grant_doorbell_cb(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct demofs_thread       *thread = container_of(doorbell,
                                                      struct demofs_thread,
                                                      grant_doorbell);
    struct demofs_inode_waiter *list, *w;

    (void) evpl;

    pthread_mutex_lock(&thread->grant_lock);
    list               = thread->grant_head;
    thread->grant_head = NULL;
    thread->grant_tail = NULL;
    pthread_mutex_unlock(&thread->grant_lock);

    while (list) {
        demofs_inode_cb_t    cb;
        void                *private_data;
        struct demofs_inode *inode;
        int                  status;

        w    = list;
        list = w->next;

        cb           = w->cb;
        private_data = w->private_data;
        inode        = w->inode;
        status       = w->status;

        if (status == CHIMERA_VFS_OK) {
            struct demofs_txn          *wtxn  = w->txn;
            enum demofs_inode_lock_mode wmode = w->mode;

            demofs_txn_add_slot(wtxn, inode, wmode);
            if (wmode == DEMOFS_INODE_LOCK_WRITE) {
                demofs_txn_pin_inode_block(thread, wtxn, inode, 0);
            }
        } else {
            inode = NULL;
        }

        demofs_waiter_free(thread, w);

        cb(inode, status, private_data);
    }
} /* demofs_grant_doorbell_cb */

static void *
demofs_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct demofs_shared *shared = private_data;
    struct demofs_thread *thread = calloc(1, sizeof(*thread));


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
                      demofs_iq_cq_doorbell_cb);

    /* Inode lock-grant delivery queue + doorbell. */
    pthread_mutex_init(&thread->grant_lock, NULL);
    thread->grant_head = NULL;
    thread->grant_tail = NULL;
    evpl_add_doorbell(evpl, &thread->grant_doorbell, demofs_grant_doorbell_cb);

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
} /* demofs_thread_init */

static void
demofs_thread_destroy(void *private_data)
{
    struct demofs_thread *thread = private_data;
    struct demofs_shared *shared = thread->shared;

    /* Drain pending block I/O before closing queues */
    if (thread->pending_io > 0) {
        chimera_demofs_debug("demofs_thread_destroy: draining %d pending I/O operations",
                             thread->pending_io);
        while (thread->pending_io > 0) {
            evpl_continue(thread->evpl);
        }
        chimera_demofs_debug("demofs_thread_destroy: drain complete");
    }

    evpl_iovec_release(thread->evpl, &thread->zero);
    evpl_iovec_release(thread->evpl, &thread->pad);

    slab_allocator_destroy(thread->allocator);

    for (int i = 0; i < shared->num_devices; i++) {
        evpl_block_close_queue(thread->evpl, thread->queue[i]);
    }

    space_map_thread_cache_return(shared->space_map, &thread->space_cache);

    /* Unregister the intent-log channel.  Caller must have quiesced all
     * in-flight VFS ops on this thread first. */
    if (thread->iq_channel) {
        struct demofs_iq_channel *ch = thread->iq_channel;

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

    while (thread->txn_free_list) {
        struct demofs_txn *txn = thread->txn_free_list;
        thread->txn_free_list = txn->next;
        free(txn);
    }

    while (thread->waiter_free_list) {
        struct demofs_inode_waiter *w = thread->waiter_free_list;
        thread->waiter_free_list = w->next;
        free(w);
    }

    free(thread->queue);
    free(thread);
} /* demofs_thread_destroy */

static inline void
demofs_map_attrs(
    struct demofs_thread     *thread,
    struct chimera_vfs_attrs *attr,
    struct demofs_inode      *inode)
{
    struct demofs_shared *shared = thread->shared;

    /* We always get attributes atomically with operations */
    attr->va_set_mask = CHIMERA_VFS_ATTR_ATOMIC;

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_FH) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_FH;
        attr->va_fh_len    = demofs_inum_to_fh(shared, attr->va_fh, inode->inum, inode->gen);
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

} /* demofs_map_attrs */

static inline void
demofs_apply_attrs(
    struct demofs_inode      *inode,
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

} /* demofs_apply_attrs */

static void
demofs_getattr_inode_cb(
    struct demofs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
        return;
    }

    demofs_map_attrs(p->thread, &request->getattr.r_attr, inode);

    demofs_op_ok(request, p->txn);
} /* demofs_getattr_inode_cb */

static void
demofs_getattr(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = demofs_txn_begin(thread, DEMOFS_TXN_READ);

    demofs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              demofs_getattr_inode_cb, request);
} /* demofs_getattr */

static void
demofs_setattr_inode_cb(
    struct demofs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_thread          *thread  = p->thread;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
        return;
    }

    demofs_map_attrs(thread, &request->setattr.r_pre_attr, inode);

    /* Handle truncation: remove/trim extents past new EOF */
    if ((request->setattr.set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) &&
        S_ISREG(inode->mode) &&
        request->setattr.set_attr->va_size < inode->size) {

        uint64_t              new_size = request->setattr.set_attr->va_size;
        struct demofs_extent *extent, *next_extent;

        rb_tree_query_floor(&inode->file.extents, new_size, file_offset, extent);

        if (!extent) {
            rb_tree_first(&inode->file.extents, extent);
        }

        while (extent) {
            uint64_t extent_start = extent->file_offset;
            uint64_t extent_end   = extent_start + extent->length;

            next_extent = rb_tree_next(&inode->file.extents, extent);

            if (extent_start >= new_size) {
                demofs_thread_free_space(thread, extent->device_id,
                                         extent->device_offset,
                                         SM_ALIGN_UP(extent->length));
                rb_tree_remove(&inode->file.extents, &extent->node);
                demofs_extent_free(thread, extent);
            } else if (extent_end > new_size) {
                uint64_t old_aligned = SM_ALIGN_UP(extent->length);
                uint64_t new_logical = new_size - extent_start;
                uint64_t new_aligned = SM_ALIGN_UP(new_logical);

                if (old_aligned > new_aligned) {
                    demofs_thread_free_space(thread, extent->device_id,
                                             extent->device_offset + new_aligned,
                                             old_aligned - new_aligned);
                }
                extent->length = new_logical;
            }

            extent = next_extent;
        }
    }

    demofs_apply_attrs(inode, request->setattr.set_attr);
    demofs_map_attrs(thread, &request->setattr.r_post_attr, inode);


    demofs_op_ok(request, p->txn);
} /* demofs_setattr_inode_cb */

static void
demofs_setattr(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = demofs_txn_begin(thread, DEMOFS_TXN_WRITE);

    demofs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              demofs_setattr_inode_cb, request);
} /* demofs_setattr */

static inline struct demofs_inode *
demofs_lookup_path(
    struct demofs_thread *thread,
    struct demofs_txn    *txn,
    const char           *path,
    int                   pathlen)
{
    struct demofs_shared *shared = thread->shared;
    struct demofs_inode  *parent, *inode;
    const char           *name;
    const char           *pathc = path;
    const char           *slash;
    int                   namelen;
    uint64_t              hash, inum, child_inum;
    uint32_t              gen, child_gen;

    demofs_fh_to_inum(&inum, &gen, shared->root_fh, shared->root_fhlen);
    inode = demofs_inode_acquire_sync_read(thread, txn, inum, gen);

    if (unlikely(!inode)) {
        return NULL;
    }

    while (*pathc == '/') {
        pathc++;
    }

    while (pathc < (path + pathlen)) {

        slash = strchr(pathc, '/');

        if (slash) {
            name    = pathc;
            namelen = slash - pathc;
        } else {
            name    = pathc;
            namelen = pathlen - (pathc - path);
        }

        pathc += namelen;

        while (*pathc == '/') {
            pathc++;
        }

        hash = chimera_vfs_hash(name, namelen);

        if (demofs_dir_lookup(thread, inode, hash, &child_inum, &child_gen) != 0) {
            return NULL;
        }

        parent = inode;

        inode = demofs_inode_acquire_sync_read(thread, txn, child_inum, child_gen);

        /* Done with the parent now; release its slot so deep walks reuse it. */
        demofs_txn_unlock_inode(txn, parent);

        if (!inode) {
            return NULL;
        }

        if (!S_ISDIR(inode->mode)) {
            return NULL;
        }

    }

    return inode;

} /* demofs_lookup_path */

static void
demofs_mount(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;
    struct demofs_inode           *inode;

    (void) private_data;

    (void) shared;

    p->thread = thread;
    p->txn    = demofs_txn_begin(thread, DEMOFS_TXN_READ);

    /* Synchronous read-locked path walk; the resolved inode (and any
     * parents still held) are recorded in the txn and released at commit. */
    inode = demofs_lookup_path(thread, p->txn, request->mount.path,
                               request->mount.pathlen);

    if (unlikely(!inode)) {
        demofs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    demofs_map_attrs(thread, &request->mount.r_attr, inode);

    demofs_op_ok(request, p->txn);
} /* demofs_mount */

static void
demofs_umount(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = demofs_txn_begin(thread, DEMOFS_TXN_READ);
    demofs_op_ok(request, p->txn);
} /* demofs_umount */

/* inode_stash[0] = parent dir (locked across child fetch) */

static void
demofs_lookup_at_child_cb(
    struct demofs_inode *child,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
        return;
    }

    demofs_map_attrs(p->thread, &request->lookup_at.r_attr, child);

    demofs_op_ok(request, p->txn);
} /* demofs_lookup_at_child_cb */

static void
demofs_lookup_at_parent_cb(
    struct demofs_inode *parent,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_thread          *thread  = p->thread;
    const char                    *name    = request->lookup_at.component;
    uint32_t                       namelen = request->lookup_at.component_len;
    uint64_t                       hash    = request->lookup_at.component_hash;
    uint64_t                       child_inum;
    uint32_t                       child_gen;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
        return;
    }

    if (unlikely(!S_ISDIR(parent->mode))) {
        enum chimera_vfs_error err = S_ISLNK(parent->mode) ?
            CHIMERA_VFS_ESYMLINK : CHIMERA_VFS_ENOTDIR;
        demofs_op_fail(request, p->txn, err);
        return;
    }

    demofs_map_attrs(thread, &request->lookup_at.r_dir_attr, parent);

    if (namelen == 1 && name[0] == '.') {
        demofs_map_attrs(thread, &request->lookup_at.r_attr, parent);
        demofs_op_ok(request, p->txn);
        return;
    }

    p->inode_stash[0] = parent;

    if (namelen == 2 && name[0] == '.' && name[1] == '.') {
        demofs_inode_get_inum_async(thread, p->txn,
                                    parent->parent_inum,
                                    parent->parent_gen,
                                    demofs_lookup_at_child_cb, request);
        return;
    }

    if (demofs_dir_lookup(thread, parent, hash, &child_inum, &child_gen) != 0) {
        demofs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    demofs_inode_get_inum_async(thread, p->txn, child_inum, child_gen,
                                demofs_lookup_at_child_cb, request);
} /* demofs_lookup_at_parent_cb */

static void
demofs_lookup_at(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = demofs_txn_begin(thread, DEMOFS_TXN_READ);

    demofs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              demofs_lookup_at_parent_cb, request);
} /* demofs_lookup_at */

/* inode_stash[0] = parent (locked across alloc / existing-inode fetch) */

static void
demofs_mkdir_at_existing_cb(
    struct demofs_inode *existing_inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_inode           *parent  = p->inode_stash[0];

    if (likely(status == CHIMERA_VFS_OK)) {
        demofs_map_attrs(p->thread, &request->mkdir_at.r_attr, existing_inode);
    }
    demofs_map_attrs(p->thread, &request->mkdir_at.r_dir_post_attr, parent);

    demofs_op_fail(request, p->txn, CHIMERA_VFS_EEXIST);
} /* demofs_mkdir_at_existing_cb */

static void
demofs_mkdir_at_alloc_cb(
    struct demofs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_thread          *thread  = p->thread;
    struct demofs_inode           *parent  = p->inode_stash[0];
    struct timespec                now;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
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

    demofs_apply_attrs(inode, request->mkdir_at.set_attr);
    demofs_map_attrs(thread, &request->mkdir_at.r_attr, inode);

    demofs_dir_insert(thread, p->txn, parent, request->mkdir_at.name_hash,
                      request->mkdir_at.name, request->mkdir_at.name_len,
                      inode->inum, inode->gen);

    parent->nlink++;
    parent->mtime_sec  = now.tv_sec;
    parent->mtime_nsec = now.tv_nsec;
    parent->ctime_sec  = now.tv_sec;
    parent->ctime_nsec = now.tv_nsec;

    demofs_map_attrs(thread, &request->mkdir_at.r_dir_post_attr, parent);

    demofs_op_ok(request, p->txn);
} /* demofs_mkdir_at_alloc_cb */

static void
demofs_mkdir_at_parent_cb(
    struct demofs_inode *parent,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_thread          *thread  = p->thread;
    uint64_t                       hash    = request->mkdir_at.name_hash;
    uint64_t                       existing_inum;
    uint32_t                       existing_gen;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
        return;
    }

    if (!S_ISDIR(parent->mode)) {
        demofs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    demofs_map_attrs(thread, &request->mkdir_at.r_dir_pre_attr, parent);

    p->inode_stash[0] = parent;

    if (demofs_dir_lookup(thread, parent, hash, &existing_inum, &existing_gen) == 0) {
        demofs_inode_get_inum_async(thread, p->txn,
                                    existing_inum, existing_gen,
                                    demofs_mkdir_at_existing_cb, request);
        return;
    }

    demofs_inode_alloc_async(thread, p->txn,
                             demofs_mkdir_at_alloc_cb, request);
} /* demofs_mkdir_at_parent_cb */

static void
demofs_mkdir_at(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = demofs_txn_begin(thread, DEMOFS_TXN_WRITE);

    demofs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              demofs_mkdir_at_parent_cb, request);
} /* demofs_mkdir_at */

/* inode_stash[0] = parent (locked across alloc / existing fetch) */

static void
demofs_mknod_at_existing_cb(
    struct demofs_inode *existing_inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_inode           *parent  = p->inode_stash[0];

    if (likely(status == CHIMERA_VFS_OK)) {
        demofs_map_attrs(p->thread, &request->mknod_at.r_attr, existing_inode);
    }
    demofs_map_attrs(p->thread, &request->mknod_at.r_dir_post_attr, parent);

    demofs_op_fail(request, p->txn, CHIMERA_VFS_EEXIST);
} /* demofs_mknod_at_existing_cb */

static void
demofs_mknod_at_alloc_cb(
    struct demofs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_thread          *thread  = p->thread;
    struct demofs_inode           *parent  = p->inode_stash[0];
    struct timespec                now;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
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

    demofs_apply_attrs(inode, request->mknod_at.set_attr);
    demofs_map_attrs(thread, &request->mknod_at.r_attr, inode);

    demofs_dir_insert(thread, p->txn, parent, request->mknod_at.name_hash,
                      request->mknod_at.name, request->mknod_at.name_len,
                      inode->inum, inode->gen);

    parent->mtime_sec  = now.tv_sec;
    parent->mtime_nsec = now.tv_nsec;
    parent->ctime_sec  = now.tv_sec;
    parent->ctime_nsec = now.tv_nsec;

    demofs_map_attrs(thread, &request->mknod_at.r_dir_post_attr, parent);

    demofs_op_ok(request, p->txn);
} /* demofs_mknod_at_alloc_cb */

static void
demofs_mknod_at_parent_cb(
    struct demofs_inode *parent,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_thread          *thread  = p->thread;
    uint64_t                       hash    = request->mknod_at.name_hash;
    uint64_t                       existing_inum;
    uint32_t                       existing_gen;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
        return;
    }

    if (!S_ISDIR(parent->mode)) {
        demofs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    demofs_map_attrs(thread, &request->mknod_at.r_dir_pre_attr, parent);

    p->inode_stash[0] = parent;

    if (demofs_dir_lookup(thread, parent, hash, &existing_inum, &existing_gen) == 0) {
        demofs_inode_get_inum_async(thread, p->txn,
                                    existing_inum, existing_gen,
                                    demofs_mknod_at_existing_cb, request);
        return;
    }

    demofs_inode_alloc_async(thread, p->txn,
                             demofs_mknod_at_alloc_cb, request);
} /* demofs_mknod_at_parent_cb */

static void
demofs_mknod_at(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = demofs_txn_begin(thread, DEMOFS_TXN_WRITE);

    demofs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              demofs_mknod_at_parent_cb, request);
} /* demofs_mknod_at */

/* inode_stash[0] = parent (locked across child fetch) */

static void
demofs_remove_at_child_cb(
    struct demofs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_thread          *thread  = p->thread;
    struct demofs_inode           *parent  = p->inode_stash[0];
    uint64_t                       hash    = request->remove_at.name_hash;
    struct timespec                now;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
        return;
    }

    if (S_ISDIR(inode->mode) && inode->nlink > 2) {
        demofs_op_fail(request, p->txn, CHIMERA_VFS_ENOTEMPTY);
        return;
    }

    /* The dirent was located before the child fetch and the parent has been
     * write-locked throughout, so it must still be present. */
    if (unlikely(demofs_dir_remove(thread, p->txn, parent, hash) != 1)) {
        demofs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
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

    demofs_map_attrs(thread, &request->remove_at.r_removed_attr, inode);

    if (inode->nlink == 0) {
        --inode->refcnt;
        if (inode->refcnt == 0) {
            demofs_inode_free(thread, inode);
        }
    }

    demofs_map_attrs(thread, &request->remove_at.r_dir_post_attr, parent);

    demofs_op_ok(request, p->txn);
} /* demofs_remove_at_child_cb */

static void
demofs_remove_at_parent_cb(
    struct demofs_inode *parent,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_thread          *thread  = p->thread;
    uint64_t                       hash    = request->remove_at.name_hash;
    uint64_t                       child_inum;
    uint32_t                       child_gen;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
        return;
    }

    demofs_map_attrs(thread, &request->remove_at.r_dir_pre_attr, parent);

    if (!S_ISDIR(parent->mode)) {
        demofs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    if (demofs_dir_lookup(thread, parent, hash, &child_inum, &child_gen) != 0) {
        demofs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    p->inode_stash[0] = parent;
    demofs_inode_get_inum_async(thread, p->txn, child_inum, child_gen,
                                demofs_remove_at_child_cb, request);
} /* demofs_remove_at_parent_cb */

static void
demofs_remove_at(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = demofs_txn_begin(thread, DEMOFS_TXN_WRITE);

    demofs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              demofs_remove_at_parent_cb, request);
} /* demofs_remove_at */

/*
 * Cookie values for readdir:
 *   0 = start of directory, will return "."
 *   1 = "." was returned, will return ".."
 *   2 = ".." was returned, will return first real entry
 *   3+ = real entry cookie (hash + 3)
 */
#define DEMOFS_COOKIE_DOT    1
#define DEMOFS_COOKIE_DOTDOT 2
#define DEMOFS_COOKIE_FIRST  3

/*
 * Readdir state machine.
 *   inode_stash[0] = directory inode (read-locked for the whole iteration)
 *   p->rd_from_hash = next dirent hash to fetch (>= cursor)
 *   p->rd_* = the dirent currently being emitted (copied out of the b+tree)
 * r_cookie/r_eof on the request are updated as we go.
 */

static void demofs_readdir_iter_step(
    struct chimera_vfs_request *request);

static void
demofs_readdir_complete(struct chimera_vfs_request *request)
{
    struct demofs_request_private *p     = request->plugin_data;
    struct demofs_inode           *inode = p->inode_stash[0];

    demofs_map_attrs(p->thread, &request->readdir.r_dir_attr, inode);
    demofs_op_ok(request, p->txn);
} /* demofs_readdir_complete */

static void
demofs_readdir_iter_inode_cb(
    struct demofs_inode *dirent_inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_thread          *thread  = p->thread;
    struct chimera_vfs_attrs       attr;
    int                            rc;

    if (status != CHIMERA_VFS_OK) {
        /* Stale dirent — skip to the next. */
        p->rd_from_hash = p->rd_hash + 1;
        demofs_readdir_iter_step(request);
        return;
    }

    attr.va_req_mask = request->readdir.attr_mask;
    demofs_map_attrs(thread, &attr, dirent_inode);

    /* Done with this child; release its slot so the next iteration reuses
     * it (only the directory itself stays held across the walk). */
    demofs_txn_unlock_inode(p->txn, dirent_inode);

    rc = request->readdir.callback(
        p->rd_inum,
        p->rd_hash + DEMOFS_COOKIE_FIRST,
        p->rd_name, p->rd_namelen,
        &attr, request->proto_private_data);

    request->readdir.r_cookie = p->rd_hash + DEMOFS_COOKIE_FIRST;

    if (rc) {
        request->readdir.r_eof = 0;
        demofs_readdir_complete(request);
        return;
    }

    p->rd_from_hash = p->rd_hash + 1;
    demofs_readdir_iter_step(request);
} /* demofs_readdir_iter_inode_cb */

static void
demofs_readdir_iter_step(struct chimera_vfs_request *request)
{
    struct demofs_request_private *p      = request->plugin_data;
    struct demofs_thread          *thread = p->thread;
    struct demofs_inode           *inode  = p->inode_stash[0];

    if (demofs_dir_next(thread, inode, p->rd_from_hash, &p->rd_hash,
                        &p->rd_inum, &p->rd_gen, p->rd_name, &p->rd_namelen) != 0) {
        request->readdir.r_eof = 1;
        demofs_readdir_complete(request);
        return;
    }

    demofs_inode_get_inum_async(thread, p->txn, p->rd_inum, p->rd_gen,
                                demofs_readdir_iter_inode_cb, request);
} /* demofs_readdir_iter_step */

static void
demofs_readdir_start_iter(struct chimera_vfs_request *request)
{
    struct demofs_request_private *p      = request->plugin_data;
    uint64_t                       cookie = request->readdir.r_cookie;

    if (cookie < DEMOFS_COOKIE_FIRST) {
        p->rd_from_hash = 0;
    } else {
        p->rd_from_hash = (cookie - DEMOFS_COOKIE_FIRST) + 1;
    }

    demofs_readdir_iter_step(request);
} /* demofs_readdir_start_iter */

static void
demofs_readdir_emit_dotdot(
    struct chimera_vfs_request *request,
    struct chimera_vfs_attrs   *attr)
{
    struct demofs_request_private *p     = request->plugin_data;
    struct demofs_inode           *inode = p->inode_stash[0];
    int                            rc;

    rc = request->readdir.callback(
        inode->parent_inum,
        DEMOFS_COOKIE_DOTDOT,
        "..", 2,
        attr, request->proto_private_data);

    if (rc) {
        request->readdir.r_cookie = DEMOFS_COOKIE_DOTDOT;
        request->readdir.r_eof    = 0;
        demofs_readdir_complete(request);
        return;
    }
    request->readdir.r_cookie = DEMOFS_COOKIE_DOTDOT;
    demofs_readdir_start_iter(request);
} /* demofs_readdir_emit_dotdot */

static void
demofs_readdir_dotdot_cb(
    struct demofs_inode *parent_inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_inode           *inode   = p->inode_stash[0];
    struct chimera_vfs_attrs       attr;

    attr.va_req_mask = request->readdir.attr_mask;

    if (status == CHIMERA_VFS_OK) {
        demofs_map_attrs(p->thread, &attr, parent_inode);
        /* Release the parent (".." target); it's distinct from the dir
        * being read (the self-parent root case never reaches here). */
        demofs_txn_unlock_inode(p->txn, parent_inode);
    } else {
        demofs_map_attrs(p->thread, &attr, inode);
    }

    demofs_readdir_emit_dotdot(request, &attr);
} /* demofs_readdir_dotdot_cb */

static void
demofs_readdir_inode_cb(
    struct demofs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_thread          *thread  = p->thread;
    uint64_t                       cookie  = request->readdir.cookie;
    struct chimera_vfs_attrs       attr;
    int                            rc;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
        return;
    }

    if (!S_ISDIR(inode->mode)) {
        demofs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    p->inode_stash[0]         = inode;
    request->readdir.r_cookie = cookie;
    request->readdir.r_eof    = 1;

    attr.va_req_mask = request->readdir.attr_mask;

    if ((request->readdir.flags & CHIMERA_VFS_READDIR_EMIT_DOT) &&
        cookie < DEMOFS_COOKIE_DOT) {
        demofs_map_attrs(thread, &attr, inode);
        rc = request->readdir.callback(
            inode->inum, DEMOFS_COOKIE_DOT, ".", 1,
            &attr, request->proto_private_data);
        if (rc) {
            request->readdir.r_cookie = DEMOFS_COOKIE_DOT;
            request->readdir.r_eof    = 0;
            demofs_readdir_complete(request);
            return;
        }
        cookie                    = DEMOFS_COOKIE_DOT;
        request->readdir.r_cookie = cookie;
    }

    if ((request->readdir.flags & CHIMERA_VFS_READDIR_EMIT_DOT) &&
        cookie < DEMOFS_COOKIE_DOTDOT) {
        if (inode->parent_inum == inode->inum &&
            inode->parent_gen == inode->gen) {
            demofs_map_attrs(thread, &attr, inode);
            demofs_readdir_emit_dotdot(request, &attr);
            return;
        }
        demofs_inode_get_inum_async(thread, p->txn,
                                    inode->parent_inum,
                                    inode->parent_gen,
                                    demofs_readdir_dotdot_cb, request);
        return;
    }

    if (!(request->readdir.flags & CHIMERA_VFS_READDIR_EMIT_DOT) &&
        cookie < DEMOFS_COOKIE_DOTDOT) {
        request->readdir.r_cookie = DEMOFS_COOKIE_DOTDOT;
    }

    demofs_readdir_start_iter(request);
} /* demofs_readdir_inode_cb */

static void
demofs_readdir(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = demofs_txn_begin(thread, DEMOFS_TXN_READ);

    demofs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              demofs_readdir_inode_cb, request);
} /* demofs_readdir */

static void
demofs_open_fh_inode_cb(
    struct demofs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
        return;
    }

    inode->refcnt++;

    request->open_fh.r_vfs_private = (uint64_t) inode;
    demofs_op_ok(request, p->txn);
} /* demofs_open_fh_inode_cb */

static void
demofs_open_fh(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = demofs_txn_begin(thread, DEMOFS_TXN_WRITE);

    demofs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              demofs_open_fh_inode_cb, request);
} /* demofs_open_fh */

/* inode_stash[0] = parent (locked across alloc / existing-inode fetch) */

static void
demofs_open_at_finish(
    struct chimera_vfs_request *request,
    struct demofs_inode        *parent,
    struct demofs_inode        *inode)
{
    struct demofs_request_private *p      = request->plugin_data;
    struct demofs_thread          *thread = p->thread;
    unsigned int                   flags  = request->open_at.flags;

    if (flags & CHIMERA_VFS_OPEN_INFERRED) {
        request->open_at.r_vfs_private = 0xdeadbeefUL;
    } else {
        inode->refcnt++;
        request->open_at.r_vfs_private = (uint64_t) inode;
    }

    demofs_map_attrs(thread, &request->open_at.r_dir_post_attr, parent);

    demofs_map_attrs(thread, &request->open_at.r_attr, inode);

    demofs_op_ok(request, p->txn);
} /* demofs_open_at_finish */

static void
demofs_open_at_existing_cb(
    struct demofs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_inode           *parent  = p->inode_stash[0];

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
        return;
    }

    demofs_open_at_finish(request, parent, inode);
} /* demofs_open_at_existing_cb */

static void
demofs_open_at_alloc_cb(
    struct demofs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_thread          *thread  = p->thread;
    struct demofs_inode           *parent  = p->inode_stash[0];
    struct timespec                now;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
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

    rb_tree_init(&inode->file.extents);
    demofs_apply_attrs(inode, request->open_at.set_attr);

    demofs_dir_insert(thread, p->txn, parent, request->open_at.name_hash,
                      request->open_at.name, request->open_at.namelen,
                      inode->inum, inode->gen);

    parent->mtime_sec  = now.tv_sec;
    parent->mtime_nsec = now.tv_nsec;
    parent->ctime_sec  = now.tv_sec;
    parent->ctime_nsec = now.tv_nsec;

    demofs_open_at_finish(request, parent, inode);
} /* demofs_open_at_alloc_cb */

static void
demofs_open_at_parent_cb(
    struct demofs_inode *parent,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_thread          *thread  = p->thread;
    unsigned int                   flags   = request->open_at.flags;
    uint64_t                       hash    = request->open_at.name_hash;
    uint64_t                       child_inum;
    uint32_t                       child_gen;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
        return;
    }

    if (!S_ISDIR(parent->mode)) {
        demofs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    demofs_map_attrs(thread, &request->open_at.r_dir_pre_attr, parent);

    p->inode_stash[0] = parent;

    if (demofs_dir_lookup(thread, parent, hash, &child_inum, &child_gen) != 0) {
        if (!(flags & CHIMERA_VFS_OPEN_CREATE)) {
            demofs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
            return;
        }
        demofs_inode_alloc_async(thread, p->txn,
                                 demofs_open_at_alloc_cb, request);
        return;
    }

    if (flags & CHIMERA_VFS_OPEN_EXCLUSIVE) {
        demofs_op_fail(request, p->txn, CHIMERA_VFS_EEXIST);
        return;
    }

    demofs_inode_get_inum_async(thread, p->txn, child_inum, child_gen,
                                demofs_open_at_existing_cb, request);
} /* demofs_open_at_parent_cb */

static void
demofs_open_at(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = demofs_txn_begin(thread, DEMOFS_TXN_WRITE);

    demofs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              demofs_open_at_parent_cb, request);
} /* demofs_open_at */


static void
demofs_create_unlinked_alloc_cb(
    struct demofs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_thread          *thread  = p->thread;
    struct timespec                now;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
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

    rb_tree_init(&inode->file.extents);

    demofs_apply_attrs(inode, request->create_unlinked.set_attr);

    inode->refcnt++;
    request->create_unlinked.r_vfs_private = (uint64_t) inode;

    demofs_map_attrs(thread, &request->create_unlinked.r_attr, inode);

    demofs_op_ok(request, p->txn);
} /* demofs_create_unlinked_alloc_cb */

static void
demofs_create_unlinked(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = demofs_txn_begin(thread, DEMOFS_TXN_WRITE);

    demofs_inode_alloc_async(thread, p->txn,
                             demofs_create_unlinked_alloc_cb, request);
} /* demofs_create_unlinked */

static void
demofs_close_inode_cb(
    struct demofs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
        return;
    }

    --inode->refcnt;
    if (inode->refcnt == 0) {
        demofs_inode_free(p->thread, inode);
    }

    demofs_op_ok(request, p->txn);
} /* demofs_close_inode_cb */

static void
demofs_close(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p     = request->plugin_data;
    struct demofs_inode           *inode = (struct demofs_inode *) request->close.vfs_private;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = demofs_txn_begin(thread, DEMOFS_TXN_WRITE);

    /* The inode pointer came in via vfs_private (set at open); re-acquire
     * it by (inum,gen) so its write lock is tracked in the cache like any
     * other op. */
    demofs_inode_get_inum_async(thread, p->txn, inode->inum, inode->gen,
                                demofs_close_inode_cb, request);
} /* demofs_close */

/*
 * Adjust read iovecs: skip prefix padding and trim to actual read length.
 * Called after block reads complete (or when no reads needed).
 */
static inline void
demofs_read_adjust_iovecs(
    struct chimera_vfs_request    *request,
    struct demofs_request_private *demofs_private)
{
    if (request->read.r_niov == 0) {
        return;
    }

    // Adjust first iovec to skip prefix (alignment padding)
    request->read.iov[0].data   += demofs_private->read_prefix;
    request->read.iov[0].length -= demofs_private->read_prefix;

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
} /* demofs_read_adjust_iovecs */ /* demofs_read_adjust_iovecs */

static inline void
demofs_io_callback(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct chimera_vfs_request    *request        = (struct chimera_vfs_request *) private_data;
    struct demofs_request_private *demofs_private = request->plugin_data;
    struct demofs_thread          *thread         = demofs_private->thread;

    if (demofs_private->status == 0 && status) {
        demofs_private->status = status;
    }

    demofs_private->pending--;
    thread->pending_io--;

    if (demofs_private->pending == 0) {
        if (demofs_private->opcode == CHIMERA_VFS_OP_READ) {
            demofs_read_adjust_iovecs(request, demofs_private);
        }

        evpl_iovecs_release(thread->evpl, demofs_private->iov, demofs_private->niov);

        if (demofs_private->status != 0) {
            demofs_op_fail(request, demofs_private->txn,
                           demofs_private->status);
        } else {
            demofs_op_ok(request, demofs_private->txn);
        }
    }
} /* demofs_io_callback */

static void
demofs_read_inode_cb(
    struct demofs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request        = private_data;
    struct demofs_request_private *demofs_private = request->plugin_data;
    struct demofs_thread          *thread         = demofs_private->thread;
    struct demofs_shared          *shared         = thread->shared;
    struct evpl                   *evpl           = thread->evpl;
    struct demofs_extent          *extent;
    uint64_t                       offset, length, read_offset, read_left;
    uint64_t                       extent_end, overlap_start, overlap_length;
    uint64_t                       aligned_offset, aligned_length, chunk;
    uint32_t                       eof = 0, chunk_niov;
    struct evpl_iovec             *chunk_iov;
    struct evpl_iovec_cursor       cursor;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, demofs_private->txn, status);
        return;
    }

    if (unlikely(!S_ISREG(inode->mode))) {
        demofs_op_fail(request, demofs_private->txn, CHIMERA_VFS_EINVAL);
        return;
    }

    offset = request->read.offset;
    length = request->read.length;

    if (offset + length > inode->size) {
        length = inode->size > offset ? inode->size - offset : 0;
        eof    = 1;
    }

    if (unlikely(length == 0)) {
        demofs_map_attrs(thread, &request->read.r_attr, inode);
        request->read.r_niov   = 0;
        request->read.r_length = 0;
        request->read.r_eof    = eof;
        demofs_op_ok(request, demofs_private->txn);
        return;
    }

    aligned_offset = offset & ~4095ULL;
    aligned_length = ((offset + length + 4095ULL) & ~4095ULL) - aligned_offset;

    demofs_private->read_prefix = offset - aligned_offset;
    demofs_private->read_suffix = aligned_length - length;

    request->read.r_length = length;
    request->read.r_eof    = eof;

    // Allocate iovec for full aligned size
    request->read.r_niov = evpl_iovec_alloc(evpl, aligned_length, 4096, 1,
                                            0, request->read.iov);

    read_offset = aligned_offset;
    read_left   = aligned_length;

    evpl_iovec_cursor_init(&cursor, request->read.iov, request->read.r_niov);

    // Find first extent that could contain our offset
    rb_tree_query_floor(&inode->file.extents, read_offset, file_offset, extent);

    if (!extent) {
        /* No extent at or before read_offset - get the first extent
         * in case there's one that starts within our range */
        rb_tree_first(&inode->file.extents, extent);
    } else if (extent->file_offset + extent->length <= read_offset) {
        extent = rb_tree_next(&inode->file.extents, extent);
    }

    while (read_left && extent && extent->file_offset < aligned_offset + aligned_length) {

        if (read_offset < extent->file_offset) {
            chunk = extent->file_offset - read_offset;
            evpl_iovec_cursor_zero(&cursor, chunk);
            read_offset += chunk;
            read_left   -= chunk;
        }


        extent_end = extent->file_offset + extent->length;

        // Calculate overlap with current extent
        overlap_start  = read_offset - extent->file_offset;
        overlap_length = extent_end - read_offset;

        if (overlap_length > read_left) {
            overlap_length = read_left;
        }

        while (overlap_length) {

            if (overlap_length > shared->devices[extent->device_id].max_request_size) {
                chunk = shared->devices[extent->device_id].max_request_size;
            } else {
                chunk = overlap_length;
            }

            chunk_iov = &demofs_private->iov[demofs_private->niov];

            uint64_t dev_offset = extent->device_offset + overlap_start;
            uint32_t dev_pad    = (uint32_t) (dev_offset & 4095ULL);
            int      pad_niov   = 0;

            if (dev_pad) {
                /* Device offset is not 4K-aligned (can happen after
                 * DEALLOCATE trims an extent at a non-block boundary).
                 * Prepend a discard buffer so the block-device read
                 * starts at an aligned offset. */
                evpl_iovec_clone_segment(&chunk_iov[0], &thread->pad,
                                         0, dev_pad);
                pad_niov    = 1;
                dev_offset -= dev_pad;
            }

            chunk_niov = evpl_iovec_cursor_move(&cursor,
                                                &chunk_iov[pad_niov],
                                                32, chunk, 1);
            chunk_niov += pad_niov;

            uint32_t total = dev_pad + chunk;

            if (total & 4095) {
                evpl_iovec_clone_segment(&chunk_iov[chunk_niov],
                                         &thread->pad, 0,
                                         4096 - (total & 4095));
                chunk_niov++;
            }

            demofs_private->niov += chunk_niov;

            demofs_private->pending++;
            thread->pending_io++;

            evpl_block_read(evpl,
                            thread->queue[extent->device_id],
                            chunk_iov,
                            chunk_niov,
                            dev_offset,
                            demofs_io_callback,
                            request);

            overlap_length -= chunk;
            overlap_start  += chunk;

            read_offset += chunk;
            read_left   -= chunk;
        }

        extent = rb_tree_next(&inode->file.extents, extent);
    }

    if (read_left) {
        evpl_iovec_cursor_zero(&cursor, read_left);
    }

    demofs_map_attrs(thread, &request->read.r_attr, inode);


    if (demofs_private->pending == 0) {
        demofs_read_adjust_iovecs(request, demofs_private);
        demofs_op_ok(request, demofs_private->txn);
    } else {
        /* I/O is in flight.  Drop the inode lock now so other ops on this
         * worker thread can proceed; the txn will be committed (with no
         * remaining refs to unlock) from demofs_io_callback. */
        demofs_txn_unlock_inode(demofs_private->txn, inode);
    }
} /* demofs_read_inode_cb */

static void
demofs_read(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->opcode  = request->opcode;
    p->status  = 0;
    p->pending = 0;
    p->niov    = 0;
    p->thread  = thread;
    p->txn     = demofs_txn_begin(thread, DEMOFS_TXN_READ);

    demofs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              demofs_read_inode_cb, request);
} /* demofs_read */

// Forward declaration
static void demofs_write_phase2(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request);

// Callback for RMW prefix/suffix reads
static void
demofs_write_rmw_read_callback(
    struct evpl *evpl,
    int          status,
    void        *private_data)
{
    struct chimera_vfs_request    *request        = private_data;
    struct demofs_request_private *demofs_private = request->plugin_data;
    struct demofs_thread          *thread         = demofs_private->thread;
    struct demofs_shared          *shared         = thread->shared;

    if (status && demofs_private->status == 0) {
        demofs_private->status = status;
    }

    demofs_private->pending--;
    thread->pending_io--;

    if (demofs_private->pending == 0) {
        if (demofs_private->status) {
            // RMW read failed
            if (demofs_private->rmw_prefix_iov.data) {
                evpl_iovec_release(thread->evpl, &demofs_private->rmw_prefix_iov);
            }
            if (demofs_private->rmw_suffix_iov.data) {
                evpl_iovec_release(thread->evpl, &demofs_private->rmw_suffix_iov);
            }
            request->status = demofs_private->status;
            request->complete(request);
            return;
        }

        // All RMW reads complete, proceed to write phase
        demofs_private->rmw_phase = 2;
        demofs_write_phase2(thread, shared, request);
    }
} /* demofs_write_rmw_read_callback */

// Phase 2: Issue actual writes (called after RMW reads complete or if no RMW needed)
static void
demofs_write_phase2(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request)
{
    struct evpl                   *evpl           = thread->evpl;
    struct demofs_request_private *demofs_private = request->plugin_data;
    struct evpl_iovec              write_iov[66]; // prefix + data + suffix + padding
    int                            write_niov = 0;
    uint64_t                       offset, chunk;
    uint32_t                       left;
    struct evpl_iovec             *chunk_iov;
    int                            chunk_niov;
    struct evpl_iovec_cursor       cursor;
    uint64_t                       write_length = request->write.length;
    uint32_t                       prefix_len   = demofs_private->rmw_prefix_len;
    uint32_t                       suffix_len   = demofs_private->rmw_suffix_len;

    // Build the combined write iovec:
    // [prefix (if any)] + [write data] + [suffix (if any)] + [padding to 4KB]

    // Add prefix if present
    if (prefix_len > 0) {
        if (demofs_private->rmw_prefix_iov.data && demofs_private->rmw_prefix_valid > 0) {
            // Prefix from existing extent
            uint32_t valid_len = demofs_private->rmw_prefix_valid;

            if (valid_len > prefix_len) {
                valid_len = prefix_len;
            }

            // Add the valid portion from existing extent
            evpl_iovec_move_segment(&write_iov[write_niov], &demofs_private->rmw_prefix_iov, 0, valid_len);
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
        if (demofs_private->rmw_suffix_iov.data && demofs_private->rmw_suffix_valid > 0) {
            // Suffix from existing extent - extract the portion after write_end
            uint64_t write_end = request->write.offset + write_length;
            // suffix_start is the offset within the read buffer to find write_end's data
            // Normally it's (write_end & 4095), but if we had to adjust because the
            // block started before the extent, we subtract the adjustment
            uint32_t suffix_start = (write_end & 4095) - demofs_private->rmw_suffix_adjust;
            uint32_t valid_len    = demofs_private->rmw_suffix_valid;

            if (valid_len > suffix_len) {
                valid_len = suffix_len;
            }

            // Add the valid portion from existing extent
            evpl_iovec_move_segment(&write_iov[write_niov], &demofs_private->rmw_suffix_iov, suffix_start, valid_len);
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
        // Use thread->zero without adding ref - it's persistent
        write_iov[write_niov]        = thread->zero;
        write_iov[write_niov].length = padding;
        write_niov++;
    }

    // Reset pending and niov for write phase
    demofs_private->pending = 0;
    demofs_private->niov    = 0;

    evpl_iovec_cursor_init(&cursor, write_iov, write_niov);

    offset = 0;
    left   = demofs_private->rmw_aligned_length;

    while (left) {
        chunk = shared->devices[demofs_private->rmw_device_id].max_request_size;

        if (left < chunk) {
            chunk = left;
        }

        chunk_iov = &demofs_private->iov[demofs_private->niov];

        chunk_niov = evpl_iovec_cursor_move(&cursor, chunk_iov, 32, chunk, 1);

        demofs_private->niov += chunk_niov;

        demofs_private->pending++;
        thread->pending_io++;

        evpl_block_write(evpl,
                         thread->queue[demofs_private->rmw_device_id],
                         chunk_iov,
                         chunk_niov,
                         demofs_private->rmw_device_offset + offset,
                         1,
                         demofs_io_callback,
                         request);

        offset += chunk;
        left   -= chunk;
    }

    evpl_iovecs_release(evpl, write_iov, write_niov);

} /* demofs_write_phase2 */

// Find extent covering a specific file offset
static struct demofs_extent *
demofs_find_extent_at(
    struct demofs_inode *inode,
    uint64_t             file_offset)
{
    struct demofs_extent *extent;

    rb_tree_query_floor(&inode->file.extents, file_offset, file_offset, extent);

    if (extent) {
        uint64_t extent_end = extent->file_offset + extent->length;
        if (file_offset >= extent->file_offset && file_offset < extent_end) {
            return extent;
        }
    }

    return NULL;
} /* demofs_find_extent_at */

static void
demofs_write_inode_cb(
    struct demofs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request        = private_data;
    struct demofs_request_private *demofs_private = request->plugin_data;
    struct demofs_thread          *thread         = demofs_private->thread;
    struct demofs_shared          *shared         = thread->shared;
    struct evpl                   *evpl           = thread->evpl;
    struct demofs_extent          *extent, *next_extent, *new_extent;
    uint64_t                       write_start = request->write.offset;
    uint64_t                       write_end   = write_start + request->write.length;
    uint64_t                       aligned_start, aligned_end, aligned_length;
    uint64_t                       device_id, device_offset;
    uint64_t                       extent_start, extent_end;
    int                            rc;
    struct timespec                now;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, demofs_private->txn, status);
        return;
    }

    if (unlikely(!S_ISREG(inode->mode))) {
        demofs_op_fail(request, demofs_private->txn, CHIMERA_VFS_EINVAL);
        return;
    }

    demofs_map_attrs(thread, &request->write.r_pre_attr, inode);

    // Calculate block-aligned boundaries
    aligned_start  = write_start & ~4095ULL;
    aligned_end    = (write_end + 4095ULL) & ~4095ULL;
    aligned_length = aligned_end - aligned_start;

    // Calculate prefix/suffix lengths for partial blocks
    uint32_t prefix_len = write_start - aligned_start;
    uint32_t suffix_len = aligned_end - write_end;

    demofs_private->rmw_prefix_len     = prefix_len;
    demofs_private->rmw_suffix_len     = suffix_len;
    demofs_private->rmw_aligned_start  = aligned_start;
    demofs_private->rmw_aligned_length = aligned_length;

    // Allocate space for the aligned write
    rc = demofs_thread_alloc_space(thread, aligned_length, &device_id, &device_offset);

    if (rc) {
        demofs_op_fail(request, demofs_private->txn, CHIMERA_VFS_ENOSPC);
        return;
    }

    demofs_private->rmw_device_id     = device_id;
    demofs_private->rmw_device_offset = device_offset;

    // Check if we need to read existing data for partial blocks
    int                   need_prefix_read = 0;
    int                   need_suffix_read = 0;
    struct demofs_extent *prefix_extent    = NULL;
    struct demofs_extent *suffix_extent    = NULL;
    uint64_t              prefix_device_id = 0, prefix_device_offset = 0;
    uint64_t              suffix_device_id = 0, suffix_device_offset = 0;

    if (prefix_len > 0) {
        // Check if there's an existing extent covering the prefix region
        prefix_extent = demofs_find_extent_at(inode, aligned_start);
        if (prefix_extent) {
            uint64_t extent_end = prefix_extent->file_offset + prefix_extent->length;

            // Calculate how much of the prefix is actually valid data
            // (extent may have been truncated to non-4K boundary)
            if (extent_end >= aligned_start + prefix_len) {
                demofs_private->rmw_prefix_valid = prefix_len;
            } else if (extent_end > aligned_start) {
                demofs_private->rmw_prefix_valid = extent_end - aligned_start;
            } else {
                demofs_private->rmw_prefix_valid = 0;
            }

            if (demofs_private->rmw_prefix_valid > 0) {
                need_prefix_read     = 1;
                prefix_device_id     = prefix_extent->device_id;
                prefix_device_offset = prefix_extent->device_offset +
                    (aligned_start - prefix_extent->file_offset);
            }
        }
    }

    if (suffix_len > 0) {
        // Check if there's an existing extent covering the suffix region
        // The suffix starts at write_end
        suffix_extent = demofs_find_extent_at(inode, write_end);
        if (suffix_extent) {
            // The block containing write_end
            uint64_t suffix_block = write_end & ~4095ULL;
            uint64_t extent_end   = suffix_extent->file_offset + suffix_extent->length;

            // Calculate how much of the suffix is actually valid data
            // (extent may have been truncated to non-4K boundary)
            if (extent_end >= aligned_end) {
                demofs_private->rmw_suffix_valid = suffix_len;
            } else if (extent_end > write_end) {
                demofs_private->rmw_suffix_valid = extent_end - write_end;
            } else {
                demofs_private->rmw_suffix_valid = 0;
            }

            // Only read if the block start is within the extent
            // If suffix_block < suffix_extent->file_offset, then the early part
            // of the block has no data. The suffix (from write_end to aligned_end)
            // IS covered by the extent, so read from extent start and adjust.
            if (suffix_block >= suffix_extent->file_offset) {
                need_suffix_read     = 1;
                suffix_device_id     = suffix_extent->device_id;
                suffix_device_offset = suffix_extent->device_offset +
                    (suffix_block - suffix_extent->file_offset);
            } else {
                // Read from extent start, adjust in phase2 by storing offset
                need_suffix_read     = 1;
                suffix_device_id     = suffix_extent->device_id;
                suffix_device_offset = suffix_extent->device_offset;
                // Store the adjustment: how much the read buffer offset differs
                // from the expected (write_end & 4095) position
                demofs_private->rmw_suffix_adjust =
                    suffix_extent->file_offset - suffix_block;
            }
        }
    }

    // Remove/trim extents that overlap with the aligned write region
    rb_tree_query_floor(&inode->file.extents, aligned_start, file_offset, extent);

    if (!extent) {
        /* No extent at or before aligned_start - get the first extent
         * in case there's one that starts within our write range */
        rb_tree_first(&inode->file.extents, extent);
    }

    while (extent) {
        extent_start = extent->file_offset;
        extent_end   = extent_start + extent->length;

        next_extent = rb_tree_next(&inode->file.extents, extent);

        if (extent_start >= aligned_end) {
            break;
        }

        // Check if extent is completely inside aligned region - remove it
        if (extent_start >= aligned_start && extent_end <= aligned_end) {
            rb_tree_remove(&inode->file.extents, &extent->node);
            demofs_extent_free(thread, extent);
            extent = next_extent;
            continue;
        }

        // Check if extent completely spans the write region - need to split
        if (extent_start < aligned_start && extent_end > aligned_end) {
            // Create new extent for the portion after aligned_end
            struct demofs_extent *after_extent = demofs_extent_alloc(thread);
            uint64_t              after_shift  = aligned_end - extent_start;

            after_extent->device_id     = extent->device_id;
            after_extent->device_offset = extent->device_offset + after_shift;
            after_extent->file_offset   = aligned_end;
            after_extent->length        = extent_end - aligned_end;
            after_extent->buffer        = NULL;

            rb_tree_insert(&inode->file.extents, file_offset, after_extent);

            // Trim original extent to end at aligned_start
            extent->length = aligned_start - extent_start;
        } else if (extent_start < aligned_start && extent_end > aligned_start) {
            // Trim extent that extends before aligned_start AND overlaps
            extent->length = aligned_start - extent_start;
        } else if (extent_start < aligned_end && extent_end > aligned_end) {
            // Trim extent that starts within write region but extends past.
            // Must remove/re-insert because file_offset is the rb-tree key.
            uint64_t shift = aligned_end - extent_start;

            rb_tree_remove(&inode->file.extents, &extent->node);

            extent->file_offset    = aligned_end;
            extent->device_offset += shift;
            extent->length        -= shift;
            if (extent->buffer) {
                extent->buffer = (char *) extent->buffer + shift;
            }

            rb_tree_insert(&inode->file.extents, file_offset, extent);
        }

        extent = next_extent;
    }

    // Create new extent for the aligned write
    new_extent = demofs_extent_alloc(thread);

    new_extent->device_id     = device_id;
    new_extent->device_offset = device_offset;
    new_extent->file_offset   = aligned_start;
    new_extent->length        = aligned_length;
    new_extent->buffer        = NULL;

    rb_tree_insert(&inode->file.extents, file_offset, new_extent);

    // Update inode metadata
    if (inode->size < write_end) {
        inode->size       = write_end;
        inode->space_used = (inode->size + 4095) & ~4095;
    }

    clock_gettime(CLOCK_REALTIME, &now);

    inode->mtime_sec  = now.tv_sec;
    inode->mtime_nsec = now.tv_nsec;
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;

    demofs_map_attrs(thread, &request->write.r_post_attr, inode);

    request->write.r_length = request->write.length;
    request->write.r_sync   = 1;

    /* All in-memory inode mutations are done.  Release the inode lock
     * before submitting block I/O so other ops on this worker (or other
     * workers) can proceed concurrently, and so we don't self-deadlock
     * when SQ-full backpressure spins this worker's evpl loop. */
    demofs_txn_unlock_inode(demofs_private->txn, inode);

    // Issue RMW reads if needed
    if (need_prefix_read || need_suffix_read) {
        demofs_private->rmw_phase = 1;

        if (need_prefix_read) {
            // Allocate buffer and read prefix block
            int niov = evpl_iovec_alloc(evpl, 4096, 4096, 1,
                                        0, &demofs_private->rmw_prefix_iov);
            if (niov > 0) {
                demofs_private->pending++;
                thread->pending_io++;
                demofs_private->rmw_prefix_pending = 1;

                evpl_block_read(evpl,
                                thread->queue[prefix_device_id],
                                &demofs_private->rmw_prefix_iov,
                                1,
                                prefix_device_offset,
                                demofs_write_rmw_read_callback,
                                request);
            }
        }

        if (need_suffix_read) {
            // Allocate buffer and read suffix block
            int niov = evpl_iovec_alloc(evpl, 4096, 4096, 1,
                                        0, &demofs_private->rmw_suffix_iov);
            if (niov > 0) {
                demofs_private->pending++;
                thread->pending_io++;
                demofs_private->rmw_suffix_pending = 1;

                evpl_block_read(evpl,
                                thread->queue[suffix_device_id],
                                &demofs_private->rmw_suffix_iov,
                                1,
                                suffix_device_offset,
                                demofs_write_rmw_read_callback,
                                request);
            }
        }

        // Wait for RMW reads to complete before proceeding
        if (demofs_private->pending == 0) {
            // No reads were actually issued (allocation failed?)
            demofs_private->rmw_phase = 2;
            demofs_write_phase2(thread, shared, request);
        }
    } else {
        // No RMW needed, proceed directly to write
        demofs_private->rmw_phase = 2;
        demofs_write_phase2(thread, shared, request);
    }
} /* demofs_write_inode_cb */

static void
demofs_write(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;

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
    p->txn                 = demofs_txn_begin(thread, DEMOFS_TXN_WRITE);

    demofs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              demofs_write_inode_cb, request);
} /* demofs_write */


static void
demofs_allocate_inode_cb(
    struct demofs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_thread          *thread  = p->thread;
    struct timespec                now;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
        return;
    }

    clock_gettime(CLOCK_REALTIME, &now);

    demofs_map_attrs(thread, &request->allocate.r_pre_attr, inode);

    if (request->allocate.flags & CHIMERA_VFS_ALLOCATE_DEALLOCATE) {
        /* DEALLOCATE: punch hole in [offset, offset+length) */
        uint64_t              hole_start = request->allocate.offset;
        uint64_t              hole_end   = hole_start + request->allocate.length;
        struct demofs_extent *extent, *next_extent;

        if (hole_end > inode->size) {
            hole_end = inode->size;
        }

        if (hole_start < hole_end) {
            rb_tree_query_floor(&inode->file.extents, hole_start, file_offset,
                                extent);

            if (!extent) {
                rb_tree_first(&inode->file.extents, extent);
            }

            while (extent) {
                uint64_t extent_start = extent->file_offset;
                uint64_t extent_end   = extent_start + extent->length;

                next_extent = rb_tree_next(&inode->file.extents, extent);

                /* Skip extents entirely before hole */
                if (extent_end <= hole_start) {
                    extent = next_extent;
                    continue;
                }

                /* Stop if extent starts at or after hole end */
                if (extent_start >= hole_end) {
                    break;
                }

                if (extent_start >= hole_start && extent_end <= hole_end) {
                    /* Extent is completely inside hole - remove it.
                     * Partial-overlap cases below currently leak the punched
                     * device range (sub-block accounting), deferred. */
                    demofs_thread_free_space(thread, extent->device_id,
                                             extent->device_offset,
                                             SM_ALIGN_UP(extent->length));
                    rb_tree_remove(&inode->file.extents, &extent->node);
                    demofs_extent_free(thread, extent);
                } else if (extent_start < hole_start &&
                           extent_end > hole_end) {
                    /* Extent spans the entire hole - split it */
                    struct demofs_extent *after_extent =
                        demofs_extent_alloc(thread);
                    uint64_t              after_shift = hole_end - extent_start;

                    after_extent->device_id     = extent->device_id;
                    after_extent->device_offset = extent->device_offset +
                        after_shift;
                    after_extent->file_offset = hole_end;
                    after_extent->length      = extent_end - hole_end;
                    after_extent->buffer      = NULL;

                    rb_tree_insert(&inode->file.extents, file_offset,
                                   after_extent);

                    /* Trim original extent to end at hole_start */
                    extent->length = hole_start - extent_start;
                } else if (extent_start < hole_start) {
                    /* Extent overlaps start of hole - trim end */
                    extent->length = hole_start - extent_start;
                } else {
                    /* Extent overlaps end of hole - trim start.
                     * Must remove/re-insert because file_offset is
                     * the rb-tree key. */
                    uint64_t shift = hole_end - extent_start;

                    rb_tree_remove(&inode->file.extents, &extent->node);

                    extent->file_offset    = hole_end;
                    extent->device_offset += shift;
                    extent->length        -= shift;

                    if (extent->buffer) {
                        extent->buffer = (char *) extent->buffer + shift;
                    }

                    rb_tree_insert(&inode->file.extents, file_offset,
                                   extent);
                }

                extent = next_extent;
            }

            inode->space_used = (inode->size + 4095) & ~4095;
        }
    } else {
        /* ALLOCATE: extend file size if needed */
        uint64_t new_end = request->allocate.offset + request->allocate.length;

        if (new_end > inode->size) {
            inode->size       = new_end;
            inode->space_used = (inode->size + 4095) & ~4095;
        }
    }

    inode->mtime_sec  = now.tv_sec;
    inode->mtime_nsec = now.tv_nsec;
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;

    demofs_map_attrs(thread, &request->allocate.r_post_attr, inode);


    demofs_op_ok(request, p->txn);
} /* demofs_allocate_inode_cb */

static void
demofs_allocate(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = demofs_txn_begin(thread, DEMOFS_TXN_WRITE);

    demofs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              demofs_allocate_inode_cb, request);
} /* demofs_allocate */

static void
demofs_seek_inode_cb(
    struct demofs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    uint64_t                       offset  = request->seek.offset;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
        return;
    }

    if (offset >= inode->size) {
        request->seek.r_eof    = 1;
        request->seek.r_offset = 0;
        demofs_op_ok(request, p->txn);
        return;
    }

    if (request->seek.what == 0) {
        /* SEEK_DATA: find first extent covering or after offset */
        struct demofs_extent *extent;

        rb_tree_query_floor(&inode->file.extents, offset, file_offset, extent);

        if (extent) {
            uint64_t extent_end = extent->file_offset + extent->length;
            if (extent_end <= offset) {
                extent = rb_tree_next(&inode->file.extents, extent);
            }
        } else {
            rb_tree_first(&inode->file.extents, extent);
        }

        while (extent) {
            uint64_t extent_end = extent->file_offset + extent->length;

            if (extent_end > offset) {
                request->seek.r_offset = (extent->file_offset > offset) ?
                    extent->file_offset : offset;
                request->seek.r_eof = 0;
                demofs_op_ok(request, p->txn);
                return;
            }

            extent = rb_tree_next(&inode->file.extents, extent);
        }

        request->seek.r_eof    = 1;
        request->seek.r_offset = 0;
        demofs_op_ok(request, p->txn);
        return;
    } else {
        /* SEEK_HOLE: find first gap from offset forward */
        struct demofs_extent *extent;
        uint64_t              current_pos = offset;

        rb_tree_query_floor(&inode->file.extents, offset, file_offset, extent);

        if (extent) {
            uint64_t extent_end = extent->file_offset + extent->length;
            if (extent_end <= offset) {
                extent = rb_tree_next(&inode->file.extents, extent);
            }
        } else {
            rb_tree_first(&inode->file.extents, extent);
        }

        while (extent) {
            uint64_t extent_end = extent->file_offset + extent->length;

            if (extent_end <= current_pos) {
                extent = rb_tree_next(&inode->file.extents, extent);
                continue;
            }

            if (extent->file_offset > current_pos) {
                request->seek.r_offset = current_pos;
                request->seek.r_eof    = 0;
                demofs_op_ok(request, p->txn);
                return;
            }

            current_pos = extent_end;
            extent      = rb_tree_next(&inode->file.extents, extent);
        }

        if (current_pos < inode->size) {
            request->seek.r_offset = current_pos;
        } else {
            request->seek.r_offset = inode->size;
        }
        request->seek.r_eof = 0;
        demofs_op_ok(request, p->txn);
        return;
    }
} /* demofs_seek_inode_cb */

static void
demofs_seek(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = demofs_txn_begin(thread, DEMOFS_TXN_READ);

    demofs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              demofs_seek_inode_cb, request);
} /* demofs_seek */

/* inode_stash[0] = parent (locked across alloc) */

static void
demofs_symlink_at_alloc_cb(
    struct demofs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_thread          *thread  = p->thread;
    struct demofs_inode           *parent  = p->inode_stash[0];
    struct timespec                now;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
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

    demofs_symlink_set(thread, p->txn, inode,
                       request->symlink_at.target, request->symlink_at.targetlen);
    demofs_map_attrs(thread, &request->symlink_at.r_attr, inode);

    demofs_dir_insert(thread, p->txn, parent, request->symlink_at.name_hash,
                      request->symlink_at.name, request->symlink_at.namelen,
                      inode->inum, inode->gen);

    parent->mtime_sec  = now.tv_sec;
    parent->mtime_nsec = now.tv_nsec;
    parent->ctime_sec  = now.tv_sec;
    parent->ctime_nsec = now.tv_nsec;

    demofs_map_attrs(thread, &request->symlink_at.r_dir_post_attr, parent);

    demofs_op_ok(request, p->txn);
} /* demofs_symlink_at_alloc_cb */

static void
demofs_symlink_at_parent_cb(
    struct demofs_inode *parent,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_thread          *thread  = p->thread;
    uint64_t                       hash    = request->symlink_at.name_hash;
    uint64_t                       existing_inum;
    uint32_t                       existing_gen;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
        return;
    }

    if (!S_ISDIR(parent->mode)) {
        demofs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    if (demofs_dir_lookup(thread, parent, hash, &existing_inum, &existing_gen) == 0) {
        demofs_op_fail(request, p->txn, CHIMERA_VFS_EEXIST);
        return;
    }

    demofs_map_attrs(thread, &request->symlink_at.r_dir_pre_attr, parent);

    p->inode_stash[0] = parent;
    demofs_inode_alloc_async(thread, p->txn,
                             demofs_symlink_at_alloc_cb, request);
} /* demofs_symlink_at_parent_cb */

static void
demofs_symlink_at(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = demofs_txn_begin(thread, DEMOFS_TXN_WRITE);

    demofs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              demofs_symlink_at_parent_cb, request);
} /* demofs_symlink_at */

static void
demofs_readlink_inode_cb(
    struct demofs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
        return;
    }

    if (unlikely(!S_ISLNK(inode->mode))) {
        demofs_op_fail(request, p->txn, CHIMERA_VFS_EINVAL);
        return;
    }

    {
        int len = demofs_symlink_get(p->thread, inode,
                                     request->readlink.r_target,
                                     request->readlink.target_maxlength);
        chimera_demofs_abort_if(len < 0, "symlink record missing (inum %lu)", inode->inum);
        request->readlink.r_target_length = len;
    }

    demofs_map_attrs(p->thread, &request->readlink.r_attr, inode);

    demofs_op_ok(request, p->txn);
} /* demofs_readlink_inode_cb */

static void
demofs_readlink(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = demofs_txn_begin(thread, DEMOFS_TXN_READ);

    demofs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              demofs_readlink_inode_cb, request);
} /* demofs_readlink */

static inline int
demofs_fh_compare(
    const void *fha,
    int         fha_len,
    const void *fhb,
    int         fhb_len)
{
    int minlen = fha_len < fhb_len ? fha_len : fhb_len;

    return memcmp(fha, fhb, minlen);
} /* demofs_fh_compare */

/*
 * Rename state machine.  Locks are taken in fh-canonical order:
 *
 *   inode_stash[0] = old_parent (locked)
 *   inode_stash[1] = new_parent (locked; aliased to [0] when same dir)
 *   inode_stash[2] = child inode (locked)
 *   inode_stash[3] = existing dest inode (locked, only when replacing)
 */

static void demofs_rename_at_perform(
    struct chimera_vfs_request *request);

static void
demofs_rename_at_unlock_parents(struct chimera_vfs_request *request)
{
    struct demofs_request_private *p  = request->plugin_data;
    struct demofs_inode           *op = p->inode_stash[0];
    struct demofs_inode           *np = p->inode_stash[1];

    if (op) {
    }
    if (np && np != op) {
    }
} /* demofs_rename_at_unlock_parents */

static void
demofs_rename_at_existing_cb(
    struct demofs_inode *existing_inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_inode           *child   = p->inode_stash[2];

    if (status != CHIMERA_VFS_OK) {
        /* Existing dirent referenced a stale inum — proceed without delete. */
        p->inode_stash[3] = NULL;
        demofs_rename_at_perform(request);
        return;
    }

    if (S_ISDIR(child->mode) != S_ISDIR(existing_inode->mode)) {
        int err = S_ISDIR(existing_inode->mode) ? CHIMERA_VFS_EISDIR : CHIMERA_VFS_ENOTDIR;
        demofs_rename_at_unlock_parents(request);
        demofs_op_fail(request, p->txn, err);
        return;
    }
    if (S_ISDIR(existing_inode->mode) && existing_inode->nlink > 2) {
        demofs_rename_at_unlock_parents(request);
        demofs_op_fail(request, p->txn, CHIMERA_VFS_ENOTEMPTY);
        return;
    }

    p->inode_stash[3] = existing_inode;
    demofs_rename_at_perform(request);
} /* demofs_rename_at_existing_cb */

static void
demofs_rename_at_perform(struct chimera_vfs_request *request)
{
    struct demofs_request_private *p              = request->plugin_data;
    struct demofs_thread          *thread         = p->thread;
    struct demofs_inode           *op             = p->inode_stash[0];
    struct demofs_inode           *np             = p->inode_stash[1];
    struct demofs_inode           *child          = p->inode_stash[2];
    struct demofs_inode           *existing_inode = p->inode_stash[3];
    uint64_t                       hash           = request->rename_at.name_hash;
    uint64_t                       new_hash       = request->rename_at.new_name_hash;
    uint64_t                       old_inum;
    uint32_t                       old_gen;
    struct timespec                now;

    clock_gettime(CLOCK_REALTIME, &now);

    /* The source dirent was verified before we fetched the child and the
     * source parent has been write-locked the whole time, so it must still
     * be present. */
    if (unlikely(demofs_dir_lookup(thread, op, hash, &old_inum, &old_gen) != 0)) {
        demofs_rename_at_unlock_parents(request);
        demofs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    if (existing_inode) {
        if (demofs_dir_remove(thread, p->txn, np, new_hash) == 1) {
            existing_inode->nlink--;
            if (S_ISDIR(existing_inode->mode)) {
                np->nlink--;
            }
        }
    }

    demofs_dir_insert(thread, p->txn, np, new_hash,
                      request->rename_at.new_name, request->rename_at.new_namelen,
                      old_inum, old_gen);

    demofs_dir_remove(thread, p->txn, op, hash);

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

    demofs_map_attrs(thread, &request->rename_at.r_fromdir_post_attr, op);
    demofs_map_attrs(thread, &request->rename_at.r_todir_post_attr, np);

    demofs_rename_at_unlock_parents(request);
    demofs_op_ok(request, p->txn);
} /* demofs_rename_at_perform */

static void
demofs_rename_at_child_cb(
    struct demofs_inode *child,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request  = private_data;
    struct demofs_request_private *p        = request->plugin_data;
    struct demofs_thread          *thread   = p->thread;
    struct demofs_inode           *op       = p->inode_stash[0];
    struct demofs_inode           *np       = p->inode_stash[1];
    uint64_t                       hash     = request->rename_at.name_hash;
    uint64_t                       new_hash = request->rename_at.new_name_hash;
    uint64_t                       old_inum, existing_inum;
    uint32_t                       old_gen, existing_gen;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_rename_at_unlock_parents(request);
        demofs_op_fail(request, p->txn, status);
        return;
    }

    p->inode_stash[2] = child;

    if (unlikely(demofs_dir_lookup(thread, op, hash, &old_inum, &old_gen) != 0)) {
        demofs_rename_at_unlock_parents(request);
        demofs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    if (demofs_dir_lookup(thread, np, new_hash, &existing_inum, &existing_gen) != 0) {
        p->inode_stash[3] = NULL;
        demofs_rename_at_perform(request);
        return;
    }

    /* Hardlink shortcut: source and dest already refer to the same inode. */
    if (existing_inum == old_inum && existing_gen == old_gen) {
        demofs_map_attrs(thread, &request->rename_at.r_fromdir_post_attr, op);
        demofs_map_attrs(thread, &request->rename_at.r_todir_post_attr, np);
        demofs_rename_at_unlock_parents(request);
        demofs_op_ok(request, p->txn);
        return;
    }

    demofs_inode_get_inum_async(thread, p->txn,
                                existing_inum, existing_gen,
                                demofs_rename_at_existing_cb, request);
} /* demofs_rename_at_child_cb */

static void
demofs_rename_at_have_parents(struct chimera_vfs_request *request)
{
    struct demofs_request_private *p      = request->plugin_data;
    struct demofs_thread          *thread = p->thread;
    struct demofs_inode           *op     = p->inode_stash[0];
    struct demofs_inode           *np     = p->inode_stash[1];
    uint64_t                       hash   = request->rename_at.name_hash;
    uint64_t                       old_inum;
    uint32_t                       old_gen;

    demofs_map_attrs(thread, &request->rename_at.r_fromdir_pre_attr, op);
    demofs_map_attrs(thread, &request->rename_at.r_todir_pre_attr, np);

    if (demofs_dir_lookup(thread, op, hash, &old_inum, &old_gen) != 0) {
        demofs_rename_at_unlock_parents(request);
        demofs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    demofs_inode_get_inum_async(thread, p->txn,
                                old_inum, old_gen,
                                demofs_rename_at_child_cb, request);
} /* demofs_rename_at_have_parents */

static void
demofs_rename_at_second_cb(
    struct demofs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    int                            cmp;

    cmp = demofs_fh_compare(request->fh, request->fh_len,
                            request->rename_at.new_fh,
                            request->rename_at.new_fhlen);

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_rename_at_unlock_parents(request);
        demofs_op_fail(request, p->txn, status);
        return;
    }

    if (!S_ISDIR(inode->mode)) {
        demofs_rename_at_unlock_parents(request);
        demofs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    if (cmp < 0) {
        p->inode_stash[1] = inode;
    } else {
        p->inode_stash[0] = inode;
    }
    demofs_rename_at_have_parents(request);
} /* demofs_rename_at_second_cb */

static void
demofs_rename_at_first_cb(
    struct demofs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_thread          *thread  = p->thread;
    int                            cmp;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
        return;
    }

    if (!S_ISDIR(inode->mode)) {
        demofs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    cmp = demofs_fh_compare(request->fh, request->fh_len,
                            request->rename_at.new_fh,
                            request->rename_at.new_fhlen);

    if (cmp == 0) {
        p->inode_stash[0] = inode;
        p->inode_stash[1] = inode;
        demofs_rename_at_have_parents(request);
        return;
    }

    if (cmp < 0) {
        p->inode_stash[0] = inode;
        demofs_inode_get_fh_async(thread, p->txn,
                                  request->rename_at.new_fh,
                                  request->rename_at.new_fhlen,
                                  demofs_rename_at_second_cb, request);
    } else {
        p->inode_stash[1] = inode;
        demofs_inode_get_fh_async(thread, p->txn,
                                  request->fh, request->fh_len,
                                  demofs_rename_at_second_cb, request);
    }
} /* demofs_rename_at_first_cb */

static void
demofs_rename_at(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;
    int                            cmp;

    (void) shared;
    (void) private_data;

    p->thread         = thread;
    p->txn            = demofs_txn_begin(thread, DEMOFS_TXN_WRITE);
    p->inode_stash[0] = NULL;
    p->inode_stash[1] = NULL;
    p->inode_stash[2] = NULL;
    p->inode_stash[3] = NULL;

    cmp = demofs_fh_compare(request->fh, request->fh_len,
                            request->rename_at.new_fh,
                            request->rename_at.new_fhlen);

    if (cmp <= 0) {
        demofs_inode_get_fh_async(thread, p->txn,
                                  request->fh, request->fh_len,
                                  demofs_rename_at_first_cb, request);
    } else {
        demofs_inode_get_fh_async(thread, p->txn,
                                  request->rename_at.new_fh,
                                  request->rename_at.new_fhlen,
                                  demofs_rename_at_first_cb, request);
    }
} /* demofs_rename_at */

/* inode_stash[0] = parent dir; inode_stash[1] = link target inode (both locked) */

static void
demofs_link_at_finish(struct chimera_vfs_request *request)
{
    struct demofs_request_private *p      = request->plugin_data;
    struct demofs_thread          *thread = p->thread;
    struct demofs_inode           *parent = p->inode_stash[0];
    struct demofs_inode           *inode  = p->inode_stash[1];
    uint64_t                       hash   = request->link_at.name_hash;
    struct timespec                now;

    clock_gettime(CLOCK_REALTIME, &now);

    demofs_dir_insert(thread, p->txn, parent, hash,
                      request->link_at.name, request->link_at.namelen,
                      inode->inum, inode->gen);

    inode->nlink++;
    inode->ctime_sec   = now.tv_sec;
    inode->ctime_nsec  = now.tv_nsec;
    parent->mtime_sec  = now.tv_sec;
    parent->mtime_nsec = now.tv_nsec;
    parent->ctime_sec  = now.tv_sec;
    parent->ctime_nsec = now.tv_nsec;

    demofs_map_attrs(thread, &request->link_at.r_attr, inode);
    demofs_map_attrs(thread, &request->link_at.r_dir_post_attr, parent);

    demofs_op_ok(request, p->txn);
} /* demofs_link_at_finish */

static void
demofs_link_at_existing_cb(
    struct demofs_inode *existing_inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;

    /* The old dirent was already removed; if its inode is still resident,
     * drop its link count for the replace. */
    if (status == CHIMERA_VFS_OK) {
        existing_inode->nlink--;
        demofs_map_attrs(p->thread, &request->link_at.r_replaced_attr,
                         existing_inode);
    }

    demofs_link_at_finish(request);
} /* demofs_link_at_existing_cb */

static void
demofs_link_at_inode_cb(
    struct demofs_inode *inode,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_thread          *thread  = p->thread;
    struct demofs_inode           *parent  = p->inode_stash[0];
    uint64_t                       hash    = request->link_at.name_hash;
    uint64_t                       einum;
    uint32_t                       egen;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
        return;
    }

    if (unlikely(S_ISDIR(inode->mode))) {
        demofs_op_fail(request, p->txn, CHIMERA_VFS_EISDIR);
        return;
    }

    p->inode_stash[1] = inode;

    if (demofs_dir_lookup(thread, parent, hash, &einum, &egen) == 0) {
        if (request->link_at.replace && !S_ISDIR(inode->mode)) {
            demofs_dir_remove(thread, p->txn, parent, hash);

            demofs_inode_get_inum_async(thread, p->txn, einum, egen,
                                        demofs_link_at_existing_cb, request);
            return;
        }
        demofs_op_fail(request, p->txn, CHIMERA_VFS_EEXIST);
        return;
    }

    demofs_link_at_finish(request);
} /* demofs_link_at_inode_cb */

static void
demofs_link_at_parent_cb(
    struct demofs_inode *parent,
    int                  status,
    void                *private_data)
{
    struct chimera_vfs_request    *request = private_data;
    struct demofs_request_private *p       = request->plugin_data;
    struct demofs_thread          *thread  = p->thread;

    if (unlikely(status != CHIMERA_VFS_OK)) {
        demofs_op_fail(request, p->txn, status);
        return;
    }

    demofs_map_attrs(thread, &request->link_at.r_dir_pre_attr, parent);

    if (!S_ISDIR(parent->mode)) {
        demofs_op_fail(request, p->txn, CHIMERA_VFS_ENOTDIR);
        return;
    }

    p->inode_stash[0] = parent;
    demofs_inode_get_fh_async(thread, p->txn,
                              request->fh, request->fh_len,
                              demofs_link_at_inode_cb, request);
} /* demofs_link_at_parent_cb */

static void
demofs_link_at(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;

    (void) shared;
    (void) private_data;

    p->thread = thread;
    p->txn    = demofs_txn_begin(thread, DEMOFS_TXN_WRITE);

    demofs_inode_get_fh_async(thread, p->txn,
                              request->link_at.dir_fh,
                              request->link_at.dir_fhlen,
                              demofs_link_at_parent_cb, request);
} /* demofs_link_at */


static void
demofs_put_key(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;
    uint64_t                       hash;
    int                            shard_idx;
    struct demofs_kv_shard        *shard;
    struct demofs_kv_entry        *entry, *existing;

    (void) private_data;

    p->thread = thread;
    p->txn    = demofs_txn_begin(thread, DEMOFS_TXN_WRITE);

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
        entry = demofs_kv_entry_alloc(thread, hash,
                                      request->put_key.key,
                                      request->put_key.key_len,
                                      request->put_key.value,
                                      request->put_key.value_len);
        rb_tree_insert(&shard->entries, hash, entry);
    }

    pthread_mutex_unlock(&shard->lock);

    demofs_op_ok(request, p->txn);
} /* demofs_put_key */

static void
demofs_get_key(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;
    uint64_t                       hash;
    int                            shard_idx;
    struct demofs_kv_shard        *shard;
    struct demofs_kv_entry        *entry;

    (void) private_data;

    p->thread = thread;
    p->txn    = demofs_txn_begin(thread, DEMOFS_TXN_READ);

    hash      = chimera_vfs_hash(request->get_key.key, request->get_key.key_len);
    shard_idx = hash % shared->num_kv_shards;
    shard     = &shared->kv_shards[shard_idx];

    pthread_mutex_lock(&shard->lock);

    rb_tree_query_exact(&shard->entries, hash, hash, entry);

    if (!entry) {
        pthread_mutex_unlock(&shard->lock);
        demofs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    request->get_key.r_value     = entry->value;
    request->get_key.r_value_len = entry->value_len;

    pthread_mutex_unlock(&shard->lock);

    demofs_op_ok(request, p->txn);
} /* demofs_get_key */

static void
demofs_delete_key(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private *p = request->plugin_data;
    uint64_t                       hash;
    int                            shard_idx;
    struct demofs_kv_shard        *shard;
    struct demofs_kv_entry        *entry;

    (void) private_data;

    p->thread = thread;
    p->txn    = demofs_txn_begin(thread, DEMOFS_TXN_WRITE);

    hash      = chimera_vfs_hash(request->delete_key.key, request->delete_key.key_len);
    shard_idx = hash % shared->num_kv_shards;
    shard     = &shared->kv_shards[shard_idx];

    pthread_mutex_lock(&shard->lock);

    rb_tree_query_exact(&shard->entries, hash, hash, entry);

    if (!entry) {
        pthread_mutex_unlock(&shard->lock);
        demofs_op_fail(request, p->txn, CHIMERA_VFS_ENOENT);
        return;
    }

    rb_tree_remove(&shard->entries, &entry->node);
    pthread_mutex_unlock(&shard->lock);

    demofs_kv_entry_free(thread, entry);

    demofs_op_ok(request, p->txn);
} /* demofs_delete_key */

static int
demofs_kv_key_in_range(
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
} /* demofs_kv_key_in_range */

static void
demofs_search_keys(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_request_private     *p = request->plugin_data;
    int                                i, rc;
    struct demofs_kv_shard            *shard;
    struct demofs_kv_entry            *entry;
    chimera_vfs_search_keys_callback_t callback = request->search_keys.callback;

    (void) private_data;

    p->thread = thread;
    p->txn    = demofs_txn_begin(thread, DEMOFS_TXN_READ);

    for (i = 0; i < shared->num_kv_shards; i++) {
        shard = &shared->kv_shards[i];

        pthread_mutex_lock(&shard->lock);

        rb_tree_first(&shard->entries, entry);

        while (entry) {
            if (demofs_kv_key_in_range(entry->key,
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
                    demofs_op_ok(request, p->txn);
                    return;
                }
            }

            entry = rb_tree_next(&shard->entries, entry);
        }

        pthread_mutex_unlock(&shard->lock);
    }

    demofs_op_ok(request, p->txn);
} /* demofs_search_keys */

static void
demofs_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_thread *thread = private_data;
    struct demofs_shared *shared = thread->shared;

    if (unlikely(shared->root_fhlen == 0)) {
        demofs_bootstrap(thread);
    }

    switch (request->opcode) {
        case CHIMERA_VFS_OP_MOUNT:
            demofs_mount(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_UMOUNT:
            demofs_umount(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LOOKUP_AT:
            demofs_lookup_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            demofs_getattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            demofs_setattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKDIR_AT:
            demofs_mkdir_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKNOD_AT:
            demofs_mknod_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE_AT:
            demofs_remove_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READDIR:
            demofs_readdir(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            demofs_open_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_FH:
            demofs_open_fh(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_CREATE_UNLINKED:
            demofs_create_unlinked(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_CLOSE:
            demofs_close(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READ:
            demofs_read(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_WRITE:
            demofs_write(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_COMMIT:
        {
            /* No-op today: writes already wait for durability before
             * acking.  Once a real intent log lands, COMMIT will fence
             * on outstanding intent log records. */
            struct demofs_request_private *cp = request->plugin_data;
            cp->thread = thread;
            cp->txn    = demofs_txn_begin(thread, DEMOFS_TXN_READ);
            demofs_op_ok(request, cp->txn);
        }
        break;
        case CHIMERA_VFS_OP_ALLOCATE:
            demofs_allocate(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SEEK:
            demofs_seek(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SYMLINK_AT:
            demofs_symlink_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READLINK:
            demofs_readlink(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_RENAME_AT:
            demofs_rename_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LINK_AT:
            demofs_link_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_PUT_KEY:
            demofs_put_key(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_GET_KEY:
            demofs_get_key(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_DELETE_KEY:
            demofs_delete_key(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SEARCH_KEYS:
            demofs_search_keys(thread, shared, request, private_data);
            break;
        default:
            chimera_demofs_error("demofs_dispatch: unknown operation %d",
                                 request->opcode);
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* demofs_dispatch */

SYMBOL_EXPORT struct chimera_vfs_module vfs_demofs = {
    .name         = "demofs",
    .fh_magic     = CHIMERA_VFS_FH_MAGIC_DEMOFS,
    .capabilities = CHIMERA_VFS_CAP_CREATE_UNLINKED | CHIMERA_VFS_CAP_FS | CHIMERA_VFS_CAP_KV |
        CHIMERA_VFS_CAP_FS_RELATIVE_OP,
    .init           = demofs_init,
    .destroy        = demofs_destroy,
    .thread_init    = demofs_thread_init,
    .thread_destroy = demofs_thread_destroy,
    .dispatch       = demofs_dispatch,
};
