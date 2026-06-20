// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <endian.h>
#include <rocksdb/c.h>
#include "rocksdb_compat.h"

#ifdef CHIMERA_DECLARE_FLUSH_WAL
/* librocksdb exports rocksdb_flush_wal but this distro's rocksdb/c.h omits
 * the declaration (see CMakeLists.txt).  Declare it ourselves. */
void rocksdb_flush_wal(
    rocksdb_t    *db,
    unsigned char sync,
    char        **errptr);
#endif /* ifdef CHIMERA_DECLARE_FLUSH_WAL */

#include <jansson.h>
#include <limits.h>
#include <utlist.h>


#include "common/varint.h"

#include "vfs/vfs.h"
#include "vfs/vfs_internal.h"
#include "vfs/vfs_acl.h"
#include "vfs/vfs_acl_serialize.h"
#include "vfs/vfs_access.h"
#include "cairn.h"
#include "common/logging.h"
#include "common/misc.h"
#include "common/macros.h"
#include "evpl_iovec_cursor.h"

#define CAIRN_KEY_INODE          0
#define CAIRN_KEY_DIRENT         1
#define CAIRN_KEY_SYMLINK        2
#define CAIRN_KEY_EXTENT         3
#define CAIRN_KEY_SUPER          4
#define CAIRN_KEY_KV             5
#define CAIRN_KEY_XATTR          6
#define CAIRN_KEY_ACL            7

/*
 * Storage layout:
 *   metadb at <path>/meta : inode/dirent/symlink/super/kv keys. WriteBatchWithIndex,
 *                          committed with sync=true so metadata ops are durable on reply.
 *   datadb at <path>/data : extent keys. WriteBatchWithIndex, sync flag selected per batch
 *                          (sync iff any pending op requested durable data).
 *
 * Multi-DB ordering invariant: when a thread commits a cycle's batches, the data batch
 * is written first and the metadata batch second.  This ensures that a recovered metadb
 * never claims a file size that points at extent data still missing from datadb.
 */
#define CAIRN_INODE_LOCK_STRIPES 1024

/*
 * Upper bound on requests batched into a single commit.  Natural batching
 * already happens because a delegation thread is blocked in rocksdb_write
 * while its inbox fills up; this cap is purely to bound tail latency when
 * load is high and commits are slow — the inbox could otherwise accumulate
 * far more requests than any single op would tolerate waiting on.
 */
#define CAIRN_BATCH_MAX_OPS      16

#define chimera_cairn_debug(...) chimera_debug("cairn", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)
#define chimera_cairn_info(...)  chimera_info("cairn", \
                                              __FILE__, \
                                              __LINE__, \
                                              __VA_ARGS__)
#define chimera_cairn_error(...) chimera_error("cairn", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)
#define chimera_cairn_fatal(...) chimera_fatal("cairn", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)
#define chimera_cairn_abort(...) chimera_abort("cairn", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)

#define chimera_cairn_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "cairn", __FILE__, __LINE__, __VA_ARGS__)

#define chimera_cairn_abort_if(cond, ...) \
        chimera_abort_if(cond, "cairn", __FILE__, __LINE__, __VA_ARGS__)

struct cairn_inode_key {
    uint8_t  keytype;
    uint64_t inum;
} __attribute__((packed));

struct cairn_dirent_key {
    uint8_t  keytype;
    uint64_t inum;
    uint64_t hash;
} __attribute__((packed));

struct cairn_symlink_key {
    uint8_t  keytype;
    uint64_t inum;
} __attribute__((packed));

struct cairn_extent_key {
    uint8_t  keytype;
    uint64_t inum;
    uint64_t offset;
} __attribute__((packed));

struct cairn_acl_key {
    uint8_t  keytype;
    uint64_t inum;
} __attribute__((packed));

struct cairn_xattr_key {
    uint8_t  keytype;
    uint64_t inum;
    uint64_t hash;
} __attribute__((packed));

struct cairn_super_key {
    uint8_t keytype;
} __attribute__((packed));

struct cairn_super {
    uint64_t fsid;
};

/* KV key structure: keytype (1 byte) + key data (variable length) */
#define CAIRN_KV_KEY_MAX 4096

struct cairn_dirent_value {
    uint64_t inum;
    uint32_t name_len;
    char     name[256];
};

struct cairn_dirent_handle {
    struct cairn_dirent_value *dirent;
    rocksdb_pinnableslice_t   *slice;
};

struct cairn_symlink_target {
    int  length;
    char data[PATH_MAX];
};

struct cairn_xattr_value {
    uint32_t name_len;
    uint32_t value_len;
    char     data[];
} __attribute__((packed));

struct cairn_inode {
    uint64_t        inum;
    uint64_t        parent_inum; /* Parent directory for ".." lookup */
    uint32_t        gen;
    uint32_t        refcnt;
    uint64_t        size;
    uint64_t        space_used;
    uint32_t        mode;
    uint32_t        nlink;
    uint32_t        uid;
    uint32_t        gid;
    uint64_t        rdev;
    struct timespec atime;
    struct timespec mtime;
    struct timespec ctime;
    struct timespec btime;
    uint32_t        dos_attributes;
    uint64_t        change;       /* native monotonic change counter */
};

struct cairn_inode_handle {
    struct cairn_inode      *inode;
    rocksdb_pinnableslice_t *slice;
};

struct cairn_shared {
    /*
     * metadb is wrapped in OptimisticTransactionDB so cross-thread races on
     * shared inodes (e.g. cross-dir rename + concurrent setattr on the same
     * directory) are detected at commit time and force a retry instead of
     * silently losing one of the updates.  meta_base_db is the underlying
     * rocksdb_t* exposed by the wrapper for non-transactional reads (super
     * block at init) and for iterator base creation.
     *
     * datadb stays a plain rocksdb_t* — extents are per-file and the fh-hash
     * routing already serializes their writes by file, so there's nothing
     * cross-thread to detect a conflict against.
     */
    rocksdb_optimistictransactiondb_t       *meta_otxn_db;
    rocksdb_t                               *meta_base_db;
    rocksdb_t                               *datadb;
    rocksdb_cache_t                         *meta_cache;
    rocksdb_cache_t                         *data_cache;
    rocksdb_options_t                       *meta_options;
    rocksdb_options_t                       *data_options;
    rocksdb_writeoptions_t                  *meta_write_opts;       /* sync=1 */
    rocksdb_writeoptions_t                  *data_write_opts_async; /* sync=0 */
    rocksdb_writeoptions_t                  *data_write_opts_sync;  /* sync=1 */
    rocksdb_readoptions_t                   *read_options;
    rocksdb_optimistictransaction_options_t *meta_otxn_opts;
    rocksdb_block_based_table_options_t     *meta_table_options;
    rocksdb_block_based_table_options_t     *data_table_options;
    int                                      num_active_threads;
    uint8_t                                  root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                                 root_fhlen;
    uint64_t                                 fsid;
    pthread_mutex_t                          lock;
    /*
     * Striped per-inode mutexes (used by helpers below for fine-grained
     * locking on the metadata of a single inode).  Combined with
     * multi_inode_lock, they prevent cross-thread metadata races between
     * fh-routed single-inode ops and multi-inode ops (rename_at / link_at).
     *
     * NOTE (follow-up): only rename_at and link_at currently take these.
     * Single-inode ops (setattr, write inode-side, etc.) still rely on
     * fh-hash routing alone, which is unsafe against a concurrent
     * cross-thread rename/link that touches their inode.  Wrapping every
     * metadata-mutating op with the appropriate stripe locks is tracked
     * as Phase A.2.
     */
    pthread_mutex_t                          multi_inode_lock;
    pthread_mutex_t                          inode_mutexes[CAIRN_INODE_LOCK_STRIPES];
    int                                      noatime;
};

struct cairn_thread {
    struct evpl                 *evpl;
    struct cairn_shared         *shared;
    /* Per-cycle metadata transaction, lazily begun on first metadata op.
     * Reads and writes go through this; on commit any key it read that has
     * since been modified by another committed transaction triggers a Busy
     * status, which causes cairn_thread_commit to roll back and replay every
     * queued request against a fresh transaction.
     *
     * data_batch stays a plain WriteBatch since there is no cross-thread
     * write contention to detect on extents. */
    rocksdb_transaction_t       *meta_txn;
    rocksdb_writebatch_t        *data_batch;
    int                          data_needs_sync;
    /* Set by the NFS COMMIT op handler so cairn_thread_commit issues an
     * explicit rocksdb_flush_wal(datadb, sync=1) after the cycle.  This
     * covers UNSTABLE writes from prior cycles whose WAL append never got
     * fsynced (data_needs_sync was 0 then); for writes in the same cycle
     * as the COMMIT, the sync data commit's fsync already covers them. */
    int                          needs_data_wal_flush;
    /* Set when evpl_defer(&thread->commit) has been called this cycle.
     * Cleared inside cairn_thread_commit before its handler returns.
     * Read-only op handlers DL_APPEND to txn_requests and rely on the
     * deferred commit to drain that list and call request->complete(); if we
     * forgot to schedule the deferral those requests would hang forever. */
    int                          commit_scheduled;
    /* Set while cairn_thread_commit is running (including during replay).
     * Suppresses the CAIRN_BATCH_MAX_OPS force-commit in cairn_dispatch so
     * a replay can't recurse into cairn_thread_commit. */
    int                          in_commit;
    /* Count of requests queued into txn_requests since the last commit.
     * Used to bound batch size via CAIRN_BATCH_MAX_OPS. */
    int                          request_count;
    /*
     * Read view for the currently-executing read-only op.  When non-NULL,
     * metadata/extent reads hit the committed base DB at a consistent
     * snapshot instead of going through meta_txn — so a read sees a stable
     * multi-key view, doesn't bloat the optimistic read-set, and isn't part
     * of the retriable write transaction (no replay, no leaked iovecs / no
     * double-emitted readdir entries on conflict).  Set by cairn_read_begin,
     * cleared by cairn_read_end.  NULL during writer ops, which read through
     * meta_txn for read-your-writes + conflict detection. */
    const rocksdb_readoptions_t *read_meta_opts;
    const rocksdb_snapshot_t    *read_meta_snap;
    const rocksdb_readoptions_t *read_data_opts;
    const rocksdb_snapshot_t    *read_data_snap;
    struct chimera_vfs_request  *txn_requests;
    struct evpl_deferral         commit;
    int                          thread_id;
    uint64_t                     next_inum;
};

/* Forward declaration for truncation handling */
static inline void
cairn_punch_hole(
    struct cairn_thread *thread,
    struct cairn_shared *shared,
    struct cairn_inode  *inode,
    uint64_t             offset,
    uint64_t             length);

/* Forward declarations (defined after cairn_thread_commit). */
static rocksdb_transaction_t * cairn_get_meta_txn(
    struct cairn_thread *thread);
static rocksdb_writebatch_t * cairn_get_data_batch(
    struct cairn_thread *thread);
/* For the optimistic-retry replay path. */
static void cairn_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data);

static inline uint32_t
cairn_inum_to_fh(
    struct cairn_shared *shared,
    uint8_t             *fh,
    uint64_t             inum,
    uint32_t             gen)
{
    return chimera_vfs_encode_fh_inum_parent(shared->root_fh, inum, gen, fh);
} /* cairn_inum_to_fh */

static inline void
cairn_fh_to_inum(
    uint64_t      *inum,
    uint32_t      *gen,
    const uint8_t *fh,
    int            fhlen)
{
    chimera_vfs_decode_fh_inum(fh, fhlen, inum, gen);
} /* cairn_fh_to_inum */

static inline void
cairn_inode_handle_release(struct cairn_inode_handle *ih)
{
    rocksdb_pinnableslice_destroy(ih->slice);
} /* cairn_inode_handle_release */

static inline pthread_mutex_t *
cairn_inode_stripe(
    struct cairn_shared *shared,
    uint64_t             inum)
{
    return &shared->inode_mutexes[inum % CAIRN_INODE_LOCK_STRIPES];
} /* cairn_inode_stripe */

static inline void
cairn_lock_inode(
    struct cairn_shared *shared,
    uint64_t             inum)
{
    pthread_mutex_lock(cairn_inode_stripe(shared, inum));
} /* cairn_lock_inode */

static inline void
cairn_unlock_inode(
    struct cairn_shared *shared,
    uint64_t             inum)
{
    pthread_mutex_unlock(cairn_inode_stripe(shared, inum));
} /* cairn_unlock_inode */

/*
 * Acquire multiple striped inode locks in inum-sorted order to avoid deadlock.
 * Duplicate inums (and inums mapping to the same stripe) are de-duplicated so we
 * never double-lock a non-recursive mutex.
 */
static inline void
cairn_lock_inodes(
    struct cairn_shared *shared,
    uint64_t            *inums,
    int                  n)
{
    pthread_mutex_t *stripes[8];
    int              ns = 0, i, j;

    for (i = 0; i < n; i++) {
        pthread_mutex_t *s   = cairn_inode_stripe(shared, inums[i]);
        int              dup = 0;
        for (j = 0; j < ns; j++) {
            if (stripes[j] == s) {
                dup = 1; break;
            }
        }
        if (!dup) {
            /* Insertion sort by pointer to enforce a global lock order. */
            int k = ns;
            while (k > 0 && stripes[k - 1] > s) {
                stripes[k] = stripes[k - 1];
                k--;
            }
            stripes[k] = s;
            ns++;
        }
    }

    for (i = 0; i < ns; i++) {
        pthread_mutex_lock(stripes[i]);
    }
} /* cairn_lock_inodes */

static inline void
cairn_unlock_inodes(
    struct cairn_shared *shared,
    uint64_t            *inums,
    int                  n)
{
    pthread_mutex_t *stripes[8];
    int              ns = 0, i, j;

    for (i = 0; i < n; i++) {
        pthread_mutex_t *s   = cairn_inode_stripe(shared, inums[i]);
        int              dup = 0;
        for (j = 0; j < ns; j++) {
            if (stripes[j] == s) {
                dup = 1; break;
            }
        }
        if (!dup) {
            stripes[ns++] = s;
        }
    }

    for (i = 0; i < ns; i++) {
        pthread_mutex_unlock(stripes[i]);
    }
} /* cairn_unlock_inodes */

static inline void
cairn_dirent_handle_release(struct cairn_dirent_handle *dh)
{
    rocksdb_pinnableslice_destroy(dh->slice);
} /* cairn_dirent_handle_release */

/*
 * Enter a read-only op's snapshot view.  Pins a consistent point-in-time
 * view of metadb (and datadb when with_data) so all of the op's reads are
 * mutually consistent without touching the write transaction.  Must be
 * paired with cairn_read_end before the op completes.
 */
static inline void
cairn_read_begin(
    struct cairn_thread *thread,
    int                  with_data)
{
    struct cairn_shared   *shared = thread->shared;
    rocksdb_readoptions_t *mopts  = rocksdb_readoptions_create();

    thread->read_meta_snap = rocksdb_create_snapshot(shared->meta_base_db);
    rocksdb_readoptions_set_snapshot(mopts, thread->read_meta_snap);
    thread->read_meta_opts = mopts;

    if (with_data) {
        rocksdb_readoptions_t *dopts = rocksdb_readoptions_create();
        thread->read_data_snap = rocksdb_create_snapshot(shared->datadb);
        rocksdb_readoptions_set_snapshot(dopts, thread->read_data_snap);
        thread->read_data_opts = dopts;
    }
} /* cairn_read_begin */

static inline void
cairn_read_end(struct cairn_thread *thread)
{
    struct cairn_shared *shared = thread->shared;

    if (thread->read_meta_opts) {
        rocksdb_readoptions_destroy((rocksdb_readoptions_t *) thread->read_meta_opts);
        rocksdb_release_snapshot(shared->meta_base_db, thread->read_meta_snap);
        thread->read_meta_opts = NULL;
        thread->read_meta_snap = NULL;
    }
    if (thread->read_data_opts) {
        rocksdb_readoptions_destroy((rocksdb_readoptions_t *) thread->read_data_opts);
        rocksdb_release_snapshot(shared->datadb, thread->read_data_snap);
        thread->read_data_opts = NULL;
        thread->read_data_snap = NULL;
    }
} /* cairn_read_end */

/*
 * Metadata point read.  Reader ops (read_meta_opts set) read the committed
 * base DB at their pinned snapshot; writer ops read through meta_txn for
 * read-your-writes and optimistic conflict tracking.
 */
static inline rocksdb_pinnableslice_t *
cairn_meta_get_pinned(
    struct cairn_thread *thread,
    const void          *key,
    size_t               klen,
    char               **err)
{
    struct cairn_shared *shared = thread->shared;

    if (thread->read_meta_opts) {
        return rocksdb_get_pinned(shared->meta_base_db, thread->read_meta_opts,
                                  (const char *) key, klen, err);
    }
    return rocksdb_transaction_get_pinned(cairn_get_meta_txn(thread),
                                          shared->read_options,
                                          (const char *) key, klen, err);
} /* cairn_meta_get_pinned */

static inline int
cairn_dirent_get(
    struct cairn_thread        *thread,
    struct cairn_dirent_key    *key,
    struct cairn_dirent_handle *dh)
{
    char  *err = NULL;
    size_t len;

    dh->slice = cairn_meta_get_pinned(thread, key, sizeof(*key), &err);

    chimera_cairn_abort_if(err, "Error getting dirent: %s\n", err);

    if (!dh->slice) {
        dh->dirent = NULL;
        return -1;
    }

    dh->dirent = (struct cairn_dirent_value *) rocksdb_pinnableslice_value(dh->slice, &len);

    return 0;
} /* cairn_dirent_get */

/*
 * Iterator over metadb that sees this transaction's pending mutations
 * merged with the on-disk state.  rocksdb_transaction_create_iterator does
 * this natively.
 */
static inline rocksdb_iterator_t *
cairn_meta_iterator(struct cairn_thread *thread)
{
    struct cairn_shared *shared = thread->shared;

    if (thread->read_meta_opts) {
        return rocksdb_create_iterator(shared->meta_base_db, thread->read_meta_opts);
    }
    return rocksdb_transaction_create_iterator(cairn_get_meta_txn(thread),
                                               shared->read_options);
} /* cairn_meta_iterator */

/*
 * datadb still uses a plain WriteBatch (no per-thread conflict checking),
 * so iterators on it merge the batch with the DB explicitly via
 * writebatch_wi-style base+overlay.  We keep this path on the WBWI side
 * because extents are only iterated by the file's own home thread (no
 * cross-thread visibility issue), so a plain base-iter would also be
 * correct for typical workloads — but using WBWI keeps "write extent, then
 * unlink-file iterates extents" race-free within a single thread cycle.
 *
 * Implementation note: rocksdb_writebatch_t has no with-base iterator
 * helper in the C API, so for extent iteration we use a base iterator
 * over datadb directly.  In-batch pending extent puts are not visible to
 * the iterator, which matches the original txn behaviour for the typical
 * write-then-not-immediately-iterate pattern.
 */
static inline rocksdb_iterator_t *
cairn_data_iterator(struct cairn_thread *thread)
{
    struct cairn_shared         *shared = thread->shared;
    const rocksdb_readoptions_t *ropts  = thread->read_data_opts
        ? thread->read_data_opts
        : shared->read_options;

    return rocksdb_create_iterator(shared->datadb, ropts);
} /* cairn_data_iterator */

static inline int
cairn_dirent_scan(
    struct cairn_thread *thread,
    uint64_t inum,
    uint64_t start_hash,
    int ( *callback )(struct cairn_dirent_key *key, struct cairn_dirent_value *dirent, void *private_data),
    void *private_data)
{
    rocksdb_iterator_t        *iter;
    struct cairn_dirent_key    start_key, *dirent_key;
    struct cairn_dirent_value *dirent_value;
    size_t                     len;

    start_key.keytype = CAIRN_KEY_DIRENT;
    start_key.inum    = inum;
    start_key.hash    = start_hash;

    iter = cairn_meta_iterator(thread);

    rocksdb_iter_seek(iter, (const char *) &start_key, sizeof(start_key));

    while (rocksdb_iter_valid(iter)) {
        dirent_key = (struct cairn_dirent_key *) rocksdb_iter_key(iter, &len);

        if (dirent_key->keytype != CAIRN_KEY_DIRENT || dirent_key->inum != inum) {
            break;
        }

        dirent_value = (struct cairn_dirent_value *) rocksdb_iter_value(iter, &len);

        if (callback(dirent_key, dirent_value, private_data)) {
            break;
        }
    }

    rocksdb_iter_destroy(iter);

    return 0;
} /* cairn_dirent_scan */

static inline int
cairn_inode_get_inum(
    struct cairn_thread       *thread,
    uint64_t                   inum,
    struct cairn_inode_handle *ih)
{
    char                  *err = NULL;
    size_t                 len;
    struct cairn_inode_key key;

    key.keytype = CAIRN_KEY_INODE;
    key.inum    = inum;

    ih->slice = cairn_meta_get_pinned(thread, &key, sizeof(key), &err);

    chimera_cairn_abort_if(err, "Error getting inode: %s\n", err);

    if (!ih->slice) {
        ih->inode = NULL;
        return -1;
    }

    ih->inode = (struct cairn_inode *) rocksdb_pinnableslice_value(ih->slice, &len);

    return 0;
} /* cairn_inode_get_inum */

static inline int
cairn_inode_get_fh(
    struct cairn_thread       *thread,
    const uint8_t             *fh,
    int                        fhlen,
    struct cairn_inode_handle *ih)
{
    uint64_t inum;
    uint32_t gen;
    int      rc;

    cairn_fh_to_inum(&inum, &gen, fh, fhlen);

    rc = cairn_inode_get_inum(thread, inum, ih);

    if (rc == 0 && ih->inode->gen != gen) {
        cairn_inode_handle_release(ih);
        rc = -1;
    }

    return rc;
} /* cairn_inode_get_fh */

static inline void
cairn_put_dirent(
    struct cairn_thread       *thread,
    struct cairn_dirent_key   *key,
    struct cairn_dirent_value *value)
{
    rocksdb_transaction_t *txn = cairn_get_meta_txn(thread);
    char                  *err = NULL;
    int                    len;

    len = sizeof(value->inum) + sizeof(value->name_len) + value->name_len;

    rocksdb_transaction_put(txn,
                            (const char *) key, sizeof(*key),
                            (const char *) value, len, &err);
    chimera_cairn_abort_if(err, "Error putting dirent: %s\n", err);
} /* cairn_put_dirent */

static inline void
cairn_put_inode(
    struct cairn_thread *thread,
    struct cairn_inode  *inode)
{
    rocksdb_transaction_t *txn = cairn_get_meta_txn(thread);
    char                  *err = NULL;
    struct cairn_inode_key key;

    key.keytype = CAIRN_KEY_INODE;
    key.inum    = inode->inum;

    rocksdb_transaction_put(txn,
                            (const char *) &key, sizeof(key),
                            (const char *) inode, sizeof(*inode), &err);
    chimera_cairn_abort_if(err, "Error putting inode: %s\n", err);
} /* cairn_put_inode */

static inline void
cairn_remove_dirent(
    struct cairn_thread     *thread,
    struct cairn_dirent_key *key)
{
    rocksdb_transaction_t *txn = cairn_get_meta_txn(thread);
    char                  *err = NULL;

    rocksdb_transaction_delete(txn, (const char *) key, sizeof(*key), &err);
    chimera_cairn_abort_if(err, "Error deleting dirent: %s\n", err);
} /* cairn_remove_dirent */

static inline void
cairn_remove_inode(
    struct cairn_thread *thread,
    struct cairn_inode  *inode)
{
    rocksdb_transaction_t *txn = cairn_get_meta_txn(thread);
    char                  *err = NULL;
    struct cairn_inode_key key;

    key.keytype = CAIRN_KEY_INODE;
    key.inum    = inode->inum;

    rocksdb_transaction_delete(txn, (const char *) &key, sizeof(key), &err);
    chimera_cairn_abort_if(err, "Error deleting inode: %s\n", err);
} /* cairn_remove_inode */

static inline void
cairn_remove_symlink_target(
    struct cairn_thread *thread,
    uint64_t             inum)
{
    rocksdb_transaction_t   *txn = cairn_get_meta_txn(thread);
    char                    *err = NULL;
    struct cairn_symlink_key key;

    key.keytype = CAIRN_KEY_SYMLINK;
    key.inum    = inum;

    rocksdb_transaction_delete(txn, (const char *) &key, sizeof(key), &err);
    chimera_cairn_abort_if(err, "Error deleting symlink target: %s\n", err);
} /* cairn_remove_symlink_target */

/* Scratch big enough to (de)serialize the largest permitted ACL. */
#define CAIRN_ACL_SCRATCH        (CHIMERA_ACL_SERIAL_HDR + \
                                  CHIMERA_ACL_MAX_ACES * CHIMERA_ACL_SERIAL_ACE)
#define CAIRN_ACL_STRUCT_SCRATCH (sizeof(struct chimera_acl) + \
                                  CHIMERA_ACL_MAX_ACES * sizeof(struct chimera_ace))

static inline void
cairn_put_acl(
    struct cairn_thread      *thread,
    uint64_t                  inum,
    const struct chimera_acl *acl)
{
    rocksdb_transaction_t  *txn = cairn_get_meta_txn(thread);
    char                   *err = NULL;
    struct cairn_acl_key    key;
    static __thread uint8_t buf[CAIRN_ACL_SCRATCH];
    int                     len;

    len = chimera_acl_serialize(acl, buf, sizeof(buf));
    if (len < 0) {
        return;
    }

    key.keytype = CAIRN_KEY_ACL;
    key.inum    = inum;

    rocksdb_transaction_put(txn, (const char *) &key, sizeof(key),
                            (const char *) buf, len, &err);
    chimera_cairn_abort_if(err, "Error putting acl: %s\n", err);
} /* cairn_put_acl */

static inline void
cairn_remove_acl(
    struct cairn_thread *thread,
    uint64_t             inum)
{
    rocksdb_transaction_t *txn = cairn_get_meta_txn(thread);
    char                  *err = NULL;
    struct cairn_acl_key   key;

    key.keytype = CAIRN_KEY_ACL;
    key.inum    = inum;

    rocksdb_transaction_delete(txn, (const char *) &key, sizeof(key), &err);
    chimera_cairn_abort_if(err, "Error deleting acl: %s\n", err);
} /* cairn_remove_acl */

/*
 * Load the stored ACL for `inum` into `out` (capacity for CHIMERA_ACL_MAX_ACES
 * ACEs); returns 1 if a stored ACL was found, 0 otherwise.
 */
static inline int
cairn_load_acl(
    struct cairn_thread *thread,
    uint64_t             inum,
    struct chimera_acl  *out)
{
    rocksdb_pinnableslice_t *slice;
    struct cairn_acl_key     key;
    char                    *err = NULL;
    const char              *blob;
    size_t                   len;
    int                      found = 0;

    key.keytype = CAIRN_KEY_ACL;
    key.inum    = inum;

    slice = cairn_meta_get_pinned(thread, &key, sizeof(key), &err);
    chimera_cairn_abort_if(err, "Error getting acl: %s\n", err);

    if (slice) {
        blob = rocksdb_pinnableslice_value(slice, &len);
        if (chimera_acl_deserialize(blob, len, out, CHIMERA_ACL_MAX_ACES) >= 0) {
            found = 1;
        }
        rocksdb_pinnableslice_destroy(slice);
    }

    return found;
} /* cairn_load_acl */

/*
 * Populate attr->va_acl when CHIMERA_VFS_ATTR_ACL is requested: the stored ACL
 * if present, else one synthesised from the inode mode.  Uses a per-thread
 * scratch buffer valid for the duration of the (synchronous) completion.
 */
static inline void
cairn_map_acl(
    struct cairn_thread      *thread,
    struct chimera_vfs_attrs *attr,
    const struct cairn_inode *inode)
{
    static __thread uint8_t scratch[CAIRN_ACL_STRUCT_SCRATCH];
    struct chimera_acl     *dst = (struct chimera_acl *) scratch;

    if (!(attr->va_req_mask & CHIMERA_VFS_ATTR_ACL)) {
        return;
    }

    if (!cairn_load_acl(thread, inode->inum, dst)) {
        chimera_acl_from_mode(inode->mode, dst, CHIMERA_ACL_MAX_ACES);
    }

    attr->va_acl       = dst;
    attr->va_set_mask |= CHIMERA_VFS_ATTR_ACL;
} /* cairn_map_acl */

/*
 * Seed a freshly-created child's ACL, mirroring memfs_inherit_acl():
 *   1. An explicit ACL supplied at create is stored as-is.
 *   2. Otherwise inherit the parent's inheritable ACEs, if any.
 *   3. Otherwise, for an SMB-originated create (windows_default), store a
 *      Windows-style default DACL granting the owner full control while
 *      leaving the POSIX mode intact (plain mode would deny e.g. FILE_EXECUTE
 *      and WRITE_OWNER on a 0644 file).
 *   4. Otherwise leave the child mode-derived (no stored ACL).
 * The ACL is written into the current meta transaction, so the create's own
 * attribute readback (cairn_map_acl) reflects it immediately.
 */
static void
cairn_inherit_acl(
    struct cairn_thread      *thread,
    struct cairn_inode       *child,
    uint64_t                  parent_inum,
    const struct chimera_acl *new_acl,
    int                       windows_default)
{
    static __thread uint8_t pbuf[CAIRN_ACL_STRUCT_SCRATCH];
    struct chimera_acl     *pacl   = (struct chimera_acl *) pbuf;
    int                     is_dir = S_ISDIR(child->mode);
    uint16_t                want   = CHIMERA_ACE_FLAG_FILE_INHERIT |
        (is_dir ? CHIMERA_ACE_FLAG_DIR_INHERIT : 0);

    /* An explicit ACL supplied at create (e.g. an SMB SD via SecD) takes
     * precedence over inheritance / the windows_default below.  The caller
     * extracts new_acl from set_attr BEFORE cairn_apply_attrs() runs, since
     * apply_attrs resets va_set_mask down to the bits it applied (ACL isn't
     * one of them).  Passing the pointer explicitly avoids relying on a
     * possibly-uninitialized set_attr->va_acl pointer in callers that don't
     * always set ATTR_ACL (e.g., NFS3 creates). */
    if (new_acl && new_acl->num_aces) {
        cairn_put_acl(thread, child->inum, new_acl);
        child->mode = (child->mode & S_IFMT) | chimera_acl_to_mode(new_acl);
        return;
    }

    if (cairn_load_acl(thread, parent_inum, pacl)) {
        int has_inh = 0;

        for (unsigned i = 0; i < pacl->num_aces; i++) {
            if (pacl->aces[i].flags & want) {
                has_inh = 1;
                break;
            }
        }

        if (has_inh) {
            unsigned            cap = pacl->num_aces * 2;
            struct chimera_acl *tmp = malloc(chimera_acl_size(cap));
            int                 n   = chimera_acl_inherit(pacl, is_dir,
                                                          child->mode & 07777, tmp, cap);

            if (n > 0) {
                cairn_put_acl(thread, child->inum, tmp);
                child->mode = (child->mode & S_IFMT) | chimera_acl_to_mode(tmp);
                free(tmp);
                return;
            }
            free(tmp);
        }
    }

    if (windows_default) {
        uint8_t             buf[sizeof(struct chimera_acl) + 4 * sizeof(struct chimera_ace)];
        struct chimera_acl *def = (struct chimera_acl *) buf;

        if (chimera_acl_default_acl(child->mode & 07777, def, 4) > 0) {
            cairn_put_acl(thread, child->inum, def);
        }
    }
} /* cairn_inherit_acl */

static inline void
cairn_remove_directory_contents(
    struct cairn_thread *thread,
    uint64_t             dir_inum)
{
    rocksdb_iterator_t     *iter;
    struct cairn_dirent_key start_key, *dirent_key;
    size_t                  klen;

    start_key.keytype = CAIRN_KEY_DIRENT;
    start_key.inum    = dir_inum;
    start_key.hash    = 0;

    iter = cairn_meta_iterator(thread);

    rocksdb_iter_seek(iter, (const char *) &start_key, sizeof(start_key));

    while (rocksdb_iter_valid(iter)) {
        dirent_key = (struct cairn_dirent_key *) rocksdb_iter_key(iter, &klen);

        if (dirent_key->keytype != CAIRN_KEY_DIRENT ||
            dirent_key->inum != dir_inum) {
            break;
        }

        cairn_remove_dirent(thread, dirent_key);
        rocksdb_iter_next(iter);
    }

    rocksdb_iter_destroy(iter);
} /* cairn_remove_directory_contents */

static inline int
cairn_directory_is_empty(
    struct cairn_thread *thread,
    uint64_t             dir_inum)
{
    rocksdb_iterator_t     *iter;
    struct cairn_dirent_key start_key, *dirent_key;
    size_t                  klen;
    int                     is_empty = 1;

    start_key.keytype = CAIRN_KEY_DIRENT;
    start_key.inum    = dir_inum;
    start_key.hash    = 0;

    iter = cairn_meta_iterator(thread);

    rocksdb_iter_seek(iter, (const char *) &start_key, sizeof(start_key));

    if (rocksdb_iter_valid(iter)) {
        dirent_key = (struct cairn_dirent_key *) rocksdb_iter_key(iter, &klen);

        if (dirent_key->keytype == CAIRN_KEY_DIRENT &&
            dirent_key->inum == dir_inum) {
            is_empty = 0;  /* Found at least one dirent */
        }
    }

    rocksdb_iter_destroy(iter);

    return is_empty;
} /* cairn_directory_is_empty */

static inline void
cairn_remove_file_extents(
    struct cairn_thread *thread,
    uint64_t             file_inum)
{
    rocksdb_writebatch_t   *batch;
    rocksdb_iterator_t     *iter;
    struct cairn_extent_key start_key, *extent_key;
    size_t                  klen;

    batch = cairn_get_data_batch(thread);

    start_key.keytype = CAIRN_KEY_EXTENT;
    start_key.inum    = file_inum;
    start_key.offset  = htobe64(0);

    iter = cairn_data_iterator(thread);

    rocksdb_iter_seek(iter, (const char *) &start_key, sizeof(start_key));

    while (rocksdb_iter_valid(iter)) {
        extent_key = (struct cairn_extent_key *) rocksdb_iter_key(iter, &klen);

        if (extent_key->keytype != CAIRN_KEY_EXTENT ||
            extent_key->inum != file_inum) {
            break;
        }

        rocksdb_writebatch_delete(batch, (const char *) extent_key, sizeof(*extent_key));

        rocksdb_iter_next(iter);
    }

    rocksdb_iter_destroy(iter);
} /* cairn_remove_file_extents */

static void *
cairn_init(
    const char                *cfgdata,
    struct prometheus_metrics *metrics)
{
    (void) metrics;
    struct cairn_shared *shared = calloc(1, sizeof(*shared));
    json_t              *cfg;
    json_error_t         json_error;
    const char          *base_path;
    char                 meta_path[PATH_MAX];
    char                 data_path[PATH_MAX];
    struct cairn_inode   inode;
    int                  initialize;
    struct timespec      now;
    char                *err          = NULL;
    size_t               cache_mb     = 64;
    int                  compression  = 1; // Default to enabled
    int                  bloom_filter = 1; // Default to enabled
    int                  statistics   = 0; // Opt-in for diagnostics
    int                  i;

    cfg = json_loads(cfgdata, 0, &json_error);

    chimera_cairn_abort_if(!cfg, "Failed to parse config: %s\n", json_error.text);

    base_path = json_string_value(json_object_get(cfg, "path"));

    chimera_cairn_abort_if(!base_path, "cairn: 'path' missing in config\n");

    snprintf(meta_path, sizeof(meta_path), "%s/meta", base_path);
    snprintf(data_path, sizeof(data_path), "%s/data", base_path);

    // Get cache sizes, compression and bloom filter settings from config
    json_t *cache_obj = json_object_get(cfg, "cache");
    if (cache_obj && json_is_integer(cache_obj)) {
        cache_mb = json_integer_value(cache_obj);
    }

    json_t *compression_obj = json_object_get(cfg, "compression");
    if (compression_obj && json_is_boolean(compression_obj)) {
        compression = json_boolean_value(compression_obj);
    }

    json_t *bloom_filter_obj = json_object_get(cfg, "bloom_filter");
    if (bloom_filter_obj && json_is_boolean(bloom_filter_obj)) {
        bloom_filter = json_boolean_value(bloom_filter_obj);
    }

    json_t *statistics_obj = json_object_get(cfg, "statistics");
    if (statistics_obj && json_is_boolean(statistics_obj)) {
        statistics = json_boolean_value(statistics_obj);
    }

    // Get noatime setting from config
    json_t *noatime_obj = json_object_get(cfg, "noatime");
    if (noatime_obj && json_is_boolean(noatime_obj)) {
        shared->noatime = json_boolean_value(noatime_obj);
    } else {
        shared->noatime = 0; // Default to false
    }

    pthread_mutex_init(&shared->lock, NULL);
    pthread_mutex_init(&shared->multi_inode_lock, NULL);
    for (i = 0; i < CAIRN_INODE_LOCK_STRIPES; i++) {
        pthread_mutex_init(&shared->inode_mutexes[i], NULL);
    }

    /*
     * Two independent RocksDB instances:
     *   metadb: small block cache reserves ~1/4 of the configured cache budget
     *           (metadata is the hot working set; uncompressed for speed).
     *   datadb: gets the remainder; compressed when enabled.
     */
    {
        size_t meta_cache_mb = cache_mb / 4;
        if (meta_cache_mb < 16) {
            meta_cache_mb = (cache_mb < 16 ? cache_mb : 16);
        }
        shared->meta_cache = rocksdb_cache_create_lru(meta_cache_mb * 1024 * 1024);
        shared->data_cache = rocksdb_cache_create_lru((cache_mb - meta_cache_mb) * 1024 * 1024);
    }

    /* metadb options: small writes, no compression, tuned for low latency.
    * pipelined_write lets the WAL append and memtable insert overlap, which
    * raises commit throughput when several threads commit concurrently. */
    shared->meta_options = rocksdb_options_create();
    rocksdb_options_set_compression(shared->meta_options, rocksdb_no_compression);
    rocksdb_options_set_write_buffer_size(shared->meta_options, 64 * 1024 * 1024);
    rocksdb_options_set_max_write_buffer_number(shared->meta_options, 8);
    rocksdb_options_set_max_background_jobs(shared->meta_options, 8);
    rocksdb_options_increase_parallelism(shared->meta_options, 8);
    rocksdb_options_set_allow_concurrent_memtable_write(shared->meta_options, 1);
    rocksdb_options_set_enable_write_thread_adaptive_yield(shared->meta_options, 1);
    rocksdb_options_set_enable_pipelined_write(shared->meta_options, 1);

    if (statistics) {
        rocksdb_options_enable_statistics(shared->meta_options);
    }

    shared->meta_table_options = rocksdb_block_based_options_create();
    rocksdb_block_based_options_set_block_cache(shared->meta_table_options, shared->meta_cache);
    rocksdb_block_based_options_set_block_size(shared->meta_table_options, 4 * 1024);

    if (bloom_filter) {
        rocksdb_filterpolicy_t *bloom = rocksdb_filterpolicy_create_bloom(10);
        rocksdb_block_based_options_set_filter_policy(shared->meta_table_options, bloom);
    }
    rocksdb_options_set_block_based_table_factory(shared->meta_options, shared->meta_table_options);

    /* datadb options: large blocks, configurable compression, big memtable for bulk writes. */
    shared->data_options = rocksdb_options_create();
    rocksdb_options_set_compression(shared->data_options,
                                    compression ? rocksdb_lz4_compression : rocksdb_no_compression);
    rocksdb_options_set_write_buffer_size(shared->data_options, 1024 * 1024 * 1024);
    rocksdb_options_set_max_write_buffer_number(shared->data_options, 64);
    rocksdb_options_set_max_background_jobs(shared->data_options, 64);
    rocksdb_options_increase_parallelism(shared->data_options, 64);
    rocksdb_options_set_memtable_huge_page_size(shared->data_options, 1024 * 1024 * 1024);
    rocksdb_options_set_allow_concurrent_memtable_write(shared->data_options, 1);
    rocksdb_options_set_enable_write_thread_adaptive_yield(shared->data_options, 1);
    rocksdb_options_set_enable_pipelined_write(shared->data_options, 1);
    rocksdb_options_set_max_background_compactions(shared->data_options, 64);
    rocksdb_options_set_max_background_flushes(shared->data_options, 64);

    if (statistics) {
        rocksdb_options_enable_statistics(shared->data_options);
    }

    shared->data_table_options = rocksdb_block_based_options_create();
    rocksdb_block_based_options_set_block_cache(shared->data_table_options, shared->data_cache);
    rocksdb_block_based_options_set_block_size(shared->data_table_options, 64 * 1024);
    rocksdb_options_set_block_based_table_factory(shared->data_options, shared->data_table_options);

    initialize = json_boolean_value(json_object_get(cfg, "initialize"));

    if (initialize) {
        rocksdb_destroy_db(shared->meta_options, meta_path, &err);
        chimera_cairn_abort_if(err, "Failed to destroy metadb: %s\n", err);
        rocksdb_destroy_db(shared->data_options, data_path, &err);
        chimera_cairn_abort_if(err, "Failed to destroy datadb: %s\n", err);

        rocksdb_options_set_create_if_missing(shared->meta_options, 1);
        rocksdb_options_set_create_if_missing(shared->data_options, 1);
    }

    /* Write options:
     *   meta_write_opts: always sync (metadata durability == POSIX expectation).
     *   data_write_opts_async: no sync (used for NFS UNSTABLE writes).
     *   data_write_opts_sync: sync (used for FILE_SYNC writes or NFS COMMIT).
     */
    shared->meta_write_opts = rocksdb_writeoptions_create();
    rocksdb_writeoptions_set_sync(shared->meta_write_opts, 1);

    shared->data_write_opts_async = rocksdb_writeoptions_create();
#ifdef CHIMERA_HAVE_ROCKSDB_FLUSH_WAL
    rocksdb_writeoptions_set_sync(shared->data_write_opts_async, 0);
#else  /* ifdef CHIMERA_HAVE_ROCKSDB_FLUSH_WAL */
    /* No on-demand WAL flush available on this RocksDB; make every extent
     * write durable so NFS COMMIT's guarantee holds without flush_wal.
     * Costs UNSTABLE-write throughput, but only on ancient RocksDB. */
    rocksdb_writeoptions_set_sync(shared->data_write_opts_async, 1);
#endif /* ifdef CHIMERA_HAVE_ROCKSDB_FLUSH_WAL */

    shared->data_write_opts_sync = rocksdb_writeoptions_create();
    rocksdb_writeoptions_set_sync(shared->data_write_opts_sync, 1);

    shared->read_options = rocksdb_readoptions_create();

    shared->meta_otxn_opts = rocksdb_optimistictransaction_options_create();

    shared->meta_otxn_db = rocksdb_optimistictransactiondb_open(
        shared->meta_options, meta_path, &err);
    chimera_cairn_abort_if(err, "Failed to open metadb at %s: %s\n", meta_path, err);

    shared->meta_base_db = rocksdb_optimistictransactiondb_get_base_db(shared->meta_otxn_db);

    shared->datadb = rocksdb_open(shared->data_options, data_path, &err);
    chimera_cairn_abort_if(err, "Failed to open datadb at %s: %s\n", data_path, err);

    json_decref(cfg);

    clock_gettime(CLOCK_REALTIME, &now);

    if (initialize) {
        struct cairn_super_key super_key;
        struct cairn_super     super;
        struct cairn_inode_key inode_key;
        rocksdb_writebatch_t  *init_batch;

        inode.inum        = 2;
        inode.parent_inum = 2; /* Root directory's parent is itself */
        inode.gen         = 1;
        inode.size        = 4096;
        inode.space_used  = 4096;
        inode.refcnt      = 1;
        inode.uid         = 0;
        inode.gid         = 0;
        inode.nlink       = 2;
        inode.rdev        = 0;
        /* World-writable fresh root: with VFS-layer ADD_FILE/ADD_SUBDIRECTORY
         * enforcement a root-owned 0755 root would refuse all creation by
         * non-root clients on this engine-authoritative backend.  Subdirs are
         * still created owned by their creator with 0755. */
        inode.mode           = S_IFDIR | 0777;
        inode.atime          = now;
        inode.mtime          = now;
        inode.ctime          = now;
        inode.btime          = now;
        inode.dos_attributes = 0;
        inode.change         = 0;

        super.fsid = chimera_rand64();

        inode_key.keytype = CAIRN_KEY_INODE;
        inode_key.inum    = inode.inum;
        super_key.keytype = CAIRN_KEY_SUPER;

        init_batch = rocksdb_writebatch_create();
        rocksdb_writebatch_put(init_batch,
                               (const char *) &inode_key, sizeof(inode_key),
                               (const char *) &inode, sizeof(inode));
        rocksdb_writebatch_put(init_batch,
                               (const char *) &super_key, sizeof(super_key),
                               (const char *) &super, sizeof(super));
        rocksdb_write(shared->meta_base_db, shared->meta_write_opts, init_batch, &err);
        chimera_cairn_abort_if(err, "Error initializing metadb: %s\n", err);
        rocksdb_writebatch_destroy(init_batch);
    }

    /* Load the super block to get the persisted FSID */
    {
        struct cairn_super_key super_key;
        struct cairn_super    *super;
        size_t                 super_len;

        super_key.keytype = CAIRN_KEY_SUPER;

        super = (struct cairn_super *) rocksdb_get(
            shared->meta_base_db,
            shared->read_options,
            (const char *) &super_key, sizeof(super_key),
            &super_len, &err);

        chimera_cairn_abort_if(err, "Error reading super block: %s\n", err);
        chimera_cairn_abort_if(!super, "Super block not found in metadb\n");

        shared->fsid = super->fsid;
        free(super);
    }

    /* Create 16-byte fsid buffer for root FH encoding (8-byte fsid + 8 bytes padding) */
    {
        uint8_t fsid_buf[CHIMERA_VFS_FSID_SIZE] = { 0 };
        memcpy(fsid_buf, &shared->fsid, sizeof(shared->fsid));
        shared->root_fhlen = chimera_vfs_encode_fh_inum_mount(fsid_buf, 2, 1,
                                                              shared->root_fh);
    }

    return shared;
} /* cairn_init */

static void
cairn_destroy(void *private_data)
{
    struct cairn_shared *shared = private_data;
    int                  i;

    rocksdb_optimistictransactiondb_close_base_db(shared->meta_base_db);
    rocksdb_optimistictransactiondb_close(shared->meta_otxn_db);
    rocksdb_close(shared->datadb);
    rocksdb_writeoptions_destroy(shared->meta_write_opts);
    rocksdb_writeoptions_destroy(shared->data_write_opts_async);
    rocksdb_writeoptions_destroy(shared->data_write_opts_sync);
    rocksdb_readoptions_destroy(shared->read_options);
    rocksdb_optimistictransaction_options_destroy(shared->meta_otxn_opts);
    rocksdb_options_destroy(shared->meta_options);
    rocksdb_options_destroy(shared->data_options);
    rocksdb_cache_destroy(shared->meta_cache);
    rocksdb_cache_destroy(shared->data_cache);
    rocksdb_block_based_options_destroy(shared->meta_table_options);
    rocksdb_block_based_options_destroy(shared->data_table_options);
    for (i = 0; i < CAIRN_INODE_LOCK_STRIPES; i++) {
        pthread_mutex_destroy(&shared->inode_mutexes[i]);
    }
    pthread_mutex_destroy(&shared->multi_inode_lock);
    pthread_mutex_destroy(&shared->lock);
    free(shared);
} /* cairn_destroy */

/*
 * Schedule cairn_thread_commit to fire at the end of this event loop cycle.
 *
 * Every op handler — including pure-read ones that DL_APPEND to txn_requests —
 * must call this before queueing its request, otherwise the deferred drain
 * never runs and the request hangs.  We guard with commit_scheduled so the
 * underlying evpl_defer is invoked at most once per cycle (libevpl deferrals
 * are not idempotent).
 */
static inline void
cairn_ensure_commit_scheduled(struct cairn_thread *thread)
{
    if (!thread->commit_scheduled) {
        evpl_defer(thread->evpl, &thread->commit);
        thread->commit_scheduled = 1;
    }
} /* cairn_ensure_commit_scheduled */

/*
 * Append a request that will be completed when the current cycle's batch
 * commits.  Bumps the per-thread request_count; cairn_dispatch checks this
 * after the op handler returns and force-commits if we're at the cap.
 */
static inline void
cairn_queue_request(
    struct cairn_thread        *thread,
    struct chimera_vfs_request *request)
{
    /* Queued requests are completed by the deferred commit, so ensure it's
     * scheduled.  (Read-only ops complete inline and never come here.) */
    cairn_ensure_commit_scheduled(thread);
    DL_APPEND(thread->txn_requests, request);
    thread->request_count++;
} /* cairn_queue_request */

/*
 * Ordered two-stage commit:
 *   1. datadb batch (sync iff any pending op requested durable data)
 *   2. metadb batch (always sync)
 *   3. complete all batched requests
 *
 * Step ordering preserves the invariant that durable metadata never refers to
 * extent data that isn't on disk: if we crash between (1) and (2), nothing has
 * been acknowledged and the metadata batch is rolled back along with the request
 * replies.  Once (2) returns successfully, both halves are durable and the
 * replies are safe to send.
 *
 * Empty batches are skipped so read-only cycles don't pay an unnecessary fsync.
 */
#define CAIRN_MAX_COMMIT_RETRIES 8

/*
 * Begin a fresh metadata transaction.  Used at the start of a cycle and on
 * every retry attempt.
 */
static inline rocksdb_transaction_t *
cairn_meta_txn_begin(
    struct cairn_shared   *shared,
    rocksdb_transaction_t *old)
{
    return rocksdb_optimistictransaction_begin(
        shared->meta_otxn_db,
        shared->meta_write_opts,
        shared->meta_otxn_opts,
        old);
} /* cairn_meta_txn_begin */

/*
 * Ordered two-stage commit with optimistic-retry on the metadata side.
 *
 *   1. datadb batch (plain WriteBatch, no conflict possible — fh-routing
 *      already serializes extent writes per file).
 *   2. metadb transaction (OptimisticTransaction).  If the commit returns
 *      Busy, another thread modified a key we read since our transaction
 *      began.  We rollback, begin a fresh transaction, re-dispatch every
 *      queued request to rebuild the metadata mutations against the new DB
 *      state, and retry.
 *   3. complete all batched requests.
 *
 * in_commit is set across the whole function so the CAIRN_BATCH_MAX_OPS
 * force-commit in cairn_dispatch doesn't recurse into us during replay.
 *
 * Error handling:
 *   - Conflicts (Busy / TryAgain) drive replay, up to CAIRN_MAX_COMMIT_RETRIES.
 *   - Any other commit error (real I/O failure, retry budget exhausted, WAL
 *     flush failure) is logged once and surfaced to every queued request as
 *     CHIMERA_VFS_EIO instead of aborting the server.  The cycle's pending
 *     state is discarded (batch destroyed, transaction rolled back), the
 *     thread reverts to a clean state, and subsequent ops can proceed (or
 *     also fail, depending on whether the underlying DB is still usable).
 *
 * Cross-DB note: data is committed BEFORE metadata.  On metadata retry,
 * data writes have already been committed; replay rebuilds an idempotent
 * data_batch (same keys/values) which gets re-committed each iteration.
 * On final EIO, prior data writes may remain in datadb without referencing
 * metadata — these are orphan rows; they're harmless until / unless a
 * future compactor GC pass picks them up.
 */
static void
cairn_thread_commit(
    struct evpl *evpl,
    void        *private_data)
{
    struct cairn_thread        *thread = private_data;
    struct cairn_shared        *shared = thread->shared;
    struct chimera_vfs_request *request;
    struct chimera_vfs_request *replay_head;
    char                       *err     = NULL;
    int                         retries = 0;

    (void) evpl;

    thread->in_commit = 1;

    /*
     * Retry loop.  Each pass:
     *   1. commits the data WriteBatch (if any) — extent puts/deletes are
     *      idempotent across retries (same keys, same values), so re-running
     *      replay rebuilds + re-commits them safely;
     *   2. commits the metadata transaction;
     *   3. on Busy / TryAgain, rolls back the metadata, then re-dispatches
     *      every queued request to rebuild both batches against fresh DB
     *      state and goes around again.
     *
     * Keeping data inside the retry loop is what makes the replay path
     * correct: if we committed data once outside the loop and a replay
     * recreated data_batch, the replayed extents would otherwise sit
     * un-committed until the next cycle's commit.
     */
    int commit_status = CHIMERA_VFS_OK;

    while (1) {
        if (thread->data_batch) {
            if (rocksdb_writebatch_count(thread->data_batch) > 0) {
                rocksdb_writeoptions_t *wo = thread->data_needs_sync
                    ? shared->data_write_opts_sync
                    : shared->data_write_opts_async;

                rocksdb_write(shared->datadb, wo, thread->data_batch, &err);
                if (err) {
                    chimera_cairn_error("Error committing data batch: %s", err);
                    free(err);
                    err           = NULL;
                    commit_status = CHIMERA_VFS_EIO;
                    rocksdb_writebatch_destroy(thread->data_batch);
                    thread->data_batch      = NULL;
                    thread->data_needs_sync = 0;
                    if (thread->meta_txn) {
                        rocksdb_transaction_rollback(thread->meta_txn, &err);
                        if (err) {
                            free(err); err = NULL;
                        }
                        rocksdb_transaction_destroy(thread->meta_txn);
                        thread->meta_txn = NULL;
                    }
                    break;
                }
            }
            rocksdb_writebatch_destroy(thread->data_batch);
            thread->data_batch      = NULL;
            thread->data_needs_sync = 0;
        }

        if (!thread->meta_txn) {
            break;
        }

        rocksdb_transaction_commit(thread->meta_txn, &err);
        if (!err) {
            rocksdb_transaction_destroy(thread->meta_txn);
            thread->meta_txn = NULL;
            break;
        }

        /*
         * RocksDB stringifies optimistic-transaction conflicts as
         * "Resource busy: ..." (Status::Busy) or "Operation timed out: ..."
         * (Status::TryAgain).  Treat either as the retry signal; any other
         * status string is surfaced to clients as CHIMERA_VFS_EIO rather
         * than aborting the server — the DB-level error is logged so it
         * isn't silent.
         */
        if (!(strstr(err, "busy") || strstr(err, "Busy") ||
              strstr(err, "timed out") || strstr(err, "TryAgain"))) {
            chimera_cairn_error("Error committing meta transaction: %s", err);
            free(err);
            err           = NULL;
            commit_status = CHIMERA_VFS_EIO;
            rocksdb_transaction_rollback(thread->meta_txn, &err);
            if (err) {
                free(err); err = NULL;
            }
            rocksdb_transaction_destroy(thread->meta_txn);
            thread->meta_txn = NULL;
            break;
        }
        free(err);
        err = NULL;

        if (++retries > CAIRN_MAX_COMMIT_RETRIES) {
            chimera_cairn_error("metadb commit conflicted %d times in a row; giving up",
                                CAIRN_MAX_COMMIT_RETRIES);
            commit_status = CHIMERA_VFS_EIO;
            rocksdb_transaction_rollback(thread->meta_txn, &err);
            if (err) {
                free(err); err = NULL;
            }
            rocksdb_transaction_destroy(thread->meta_txn);
            thread->meta_txn = NULL;
            break;
        }

        rocksdb_transaction_rollback(thread->meta_txn, &err);
        if (err) {
            free(err); err = NULL;
        }
        rocksdb_transaction_destroy(thread->meta_txn);
        thread->meta_txn = NULL;

        /*
         * Take the queued requests off the thread and re-dispatch them.
         * Each re-dispatch lazy-creates a fresh meta_txn and (if needed) a
         * fresh data_batch and re-emits its puts/deletes against them.
         * Op handlers are replay-safe — they read state, derive new state,
         * and write it back; per-thread side effects like next_inum++ are
         * either harmless to repeat (we just burn one inum) or already
         * amortized.
         */
        replay_head           = thread->txn_requests;
        thread->txn_requests  = NULL;
        thread->request_count = 0;

        while (replay_head) {
            request = replay_head;
            DL_DELETE(replay_head, request);
            cairn_dispatch(request, thread);
        }
    }

    /*
     * NFS COMMIT semantics: explicitly fsync datadb's WAL so prior cycles'
     * UNSTABLE writes (sync=0 then) become durable.  In-cycle data writes
     * already got fsynced above via data_needs_sync; this covers the gap
     * for everything written before the current cycle started.  Skip on
     * commit error — we're already returning EIO and the WAL state is
     * unclear.
     */
    if (thread->needs_data_wal_flush && commit_status == CHIMERA_VFS_OK) {
#ifdef CHIMERA_HAVE_ROCKSDB_FLUSH_WAL
        rocksdb_flush_wal(shared->datadb, 1, &err);
        if (err) {
            chimera_cairn_error("Error flushing data WAL: %s", err);
            free(err);
            err           = NULL;
            commit_status = CHIMERA_VFS_EIO;
        }
#endif /* ifdef CHIMERA_HAVE_ROCKSDB_FLUSH_WAL */
        /*
         * Without rocksdb_flush_wal, cairn_init forces data_write_opts_async
         * to sync=1, so all extent writes are already durable here and there
         * is nothing extra to flush.
         */
    }
    thread->needs_data_wal_flush = 0;

    while (thread->txn_requests) {
        request = thread->txn_requests;
        DL_DELETE(thread->txn_requests, request);
        if (commit_status != CHIMERA_VFS_OK) {
            request->status = commit_status;
        }
        request->complete(request);
    }

    thread->request_count    = 0;
    thread->commit_scheduled = 0;
    thread->in_commit        = 0;
} /* cairn_thread_commit */

static rocksdb_transaction_t *
cairn_get_meta_txn(struct cairn_thread *thread)
{
    if (!thread->meta_txn) {
        thread->meta_txn = cairn_meta_txn_begin(thread->shared, NULL);
    }
    cairn_ensure_commit_scheduled(thread);
    return thread->meta_txn;
} /* cairn_get_meta_txn */

/* Stage an optional atomic handle-state record (carried on an open) into the
 * current meta transaction, so it commits in the same rocksdb_transaction_commit
 * as the inode/dirent writes for the open — no gap, no second round trip.
 * Stored under the shared CAIRN_KEY_KV namespace so the generic search/delete
 * KV ops can later enumerate and remove it. */
static void
cairn_stage_handle_state(
    struct cairn_thread             *thread,
    struct chimera_vfs_handle_state *hs)
{
    rocksdb_transaction_t *txn;
    char                  *err = NULL;
    uint8_t                kv_key[1 + CAIRN_KV_KEY_MAX];
    size_t                 kv_key_len;

    if (!hs || hs->key_len > CAIRN_KV_KEY_MAX) {
        return;
    }

    kv_key[0]  = CAIRN_KEY_KV;
    kv_key_len = 1 + hs->key_len;
    memcpy(kv_key + 1, hs->key, hs->key_len);

    txn = cairn_get_meta_txn(thread);
    rocksdb_transaction_put(txn,
                            (const char *) kv_key, kv_key_len,
                            (const char *) hs->value, hs->value_len, &err);
    chimera_cairn_abort_if(err, "Error staging handle-state: %s\n", err);
} /* cairn_stage_handle_state */

static rocksdb_writebatch_t *
cairn_get_data_batch(struct cairn_thread *thread)
{
    if (!thread->data_batch) {
        thread->data_batch = rocksdb_writebatch_create();
    }
    cairn_ensure_commit_scheduled(thread);
    return thread->data_batch;
} /* cairn_get_data_batch */

static void *
cairn_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct cairn_shared *shared = private_data;
    struct cairn_thread *thread = calloc(1, sizeof(*thread));

    evpl_deferral_init(&thread->commit, cairn_thread_commit, thread);

    thread->shared = shared;
    thread->evpl   = evpl;
    pthread_mutex_lock(&shared->lock);
    thread->thread_id = shared->num_active_threads++;
    pthread_mutex_unlock(&shared->lock);

    thread->next_inum = 3;

    return thread;
} /* cairn_thread_init */

static void
cairn_thread_destroy(void *private_data)
{
    struct cairn_thread *thread = private_data;

    cairn_thread_commit(thread->evpl, thread);

    free(thread);
} /* cairn_thread_destroy */

static inline void
cairn_alloc_inum(
    struct cairn_thread *thread,
    struct cairn_inode  *inode)
{
    uint64_t id = thread->next_inum++;

    inode->inum   = (id << 8) + thread->thread_id;
    inode->gen    = 1;
    inode->change = 0;
} /* cairn_alloc_inum */

static inline void
cairn_map_attrs(
    struct cairn_shared      *shared,
    struct chimera_vfs_attrs *attr,
    struct cairn_inode       *inode)
{
    /* We always get attributes atomically with operations */
    attr->va_set_mask = CHIMERA_VFS_ATTR_ATOMIC;

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_FH) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_FH;
        attr->va_fh_len    = cairn_inum_to_fh(shared, attr->va_fh, inode->inum, inode->gen);
    }

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_MASK_STAT) {
        attr->va_set_mask  |= CHIMERA_VFS_ATTR_MASK_STAT;
        attr->va_mode       = inode->mode;
        attr->va_nlink      = inode->nlink;
        attr->va_uid        = inode->uid;
        attr->va_gid        = inode->gid;
        attr->va_size       = inode->size;
        attr->va_space_used = inode->space_used;
        attr->va_atime      = inode->atime;
        attr->va_mtime      = inode->mtime;
        attr->va_ctime      = inode->ctime;
        attr->va_ino        = inode->inum;
        attr->va_dev        = (42UL << 32) | 42;
        attr->va_rdev       = inode->rdev;

        /* cairn persists DOS attributes natively, so report them alongside
         * stat (matching memfs). */
        attr->va_set_mask      |= CHIMERA_VFS_ATTR_DOS_ATTRIBUTES;
        attr->va_dos_attributes = inode->dos_attributes;
    }

    /* Birth time (SMB create time) is tracked natively but lives outside
     * MASK_STAT, so map it only when explicitly requested. */
    if (attr->va_req_mask & CHIMERA_VFS_ATTR_BTIME) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_BTIME;
        attr->va_btime     = inode->btime;
    }

    /* Native monotonic change counter (CHIMERA_VFS_CAP_CHANGE). */
    if (attr->va_req_mask & CHIMERA_VFS_ATTR_CHANGE) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_CHANGE;
        attr->va_change    = inode->change;
    }

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_FSID) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_FSID;
        attr->va_fsid      = shared->fsid;
    }

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_MASK_STATFS_VALUES) {
        attr->va_set_mask      |= CHIMERA_VFS_ATTR_MASK_STATFS;
        attr->va_fs_space_avail = CHIMERA_VFS_SYNTHETIC_FS_BYTES;
        attr->va_fs_space_free  = CHIMERA_VFS_SYNTHETIC_FS_BYTES;
        attr->va_fs_space_total = CHIMERA_VFS_SYNTHETIC_FS_BYTES;
        attr->va_fs_space_used  = 0;
        attr->va_fs_files_total = CHIMERA_VFS_SYNTHETIC_FS_INODES;
        attr->va_fs_files_free  = CHIMERA_VFS_SYNTHETIC_FS_INODES;
        attr->va_fs_files_avail = CHIMERA_VFS_SYNTHETIC_FS_INODES;
        attr->va_fsid           = shared->fsid;
    }
} /* cairn_map_attrs */

static inline void
cairn_apply_attrs(
    struct cairn_inode       *inode,
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
        chimera_vfs_resolve_set_time(&attr->va_atime, &now, &inode->atime);
    }

    if (set_mask & CHIMERA_VFS_ATTR_MTIME) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_MTIME;
        chimera_vfs_resolve_set_time(&attr->va_mtime, &now, &inode->mtime);
    }

    if (set_mask & CHIMERA_VFS_ATTR_BTIME) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_BTIME;
        chimera_vfs_resolve_set_time(&attr->va_btime, &now, &inode->btime);
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
        chimera_vfs_resolve_set_time(&attr->va_ctime, &now, &inode->ctime);
    } else {
        inode->ctime = now;
    }

    /* Any setattr is a metadata change; advance the native change counter. */
    inode->change++;

} /* cairn_apply_attrs */

static void
cairn_getattr(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle ih;
    struct cairn_inode       *inode;
    int                       rc;

    rc = cairn_inode_get_fh(thread, request->fh, request->fh_len, &ih);

    if (unlikely(rc)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;

    cairn_map_attrs(shared, &request->getattr.r_attr, inode);
    cairn_map_acl(thread, &request->getattr.r_attr, inode);

    cairn_inode_handle_release(&ih);

    request->status = CHIMERA_VFS_OK;

    request->complete(request);
} /* cairn_getattr */

static void
cairn_setattr(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle ih;
    struct cairn_inode       *inode;
    int                       rc;

    rc = cairn_inode_get_fh(thread, request->fh, request->fh_len, &ih);

    if (unlikely(rc)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;

    if ((request->setattr.set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) &&
        !S_ISREG(inode->mode)) {
        /* `inode` points into the pinned RocksDB slice owned by `ih`, so read
         * inode->mode before releasing the handle -- releasing frees the slice. */
        request->status = S_ISDIR(inode->mode) ?
            CHIMERA_VFS_EISDIR : CHIMERA_VFS_EINVAL;
        cairn_inode_handle_release(&ih);
        request->complete(request);
        return;
    }

    cairn_map_attrs(shared, &request->setattr.r_pre_attr, inode);

    /* Handle truncation: remove extents past new EOF when size decreases */
    if ((request->setattr.set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) &&
        S_ISREG(inode->mode) &&
        request->setattr.set_attr->va_size < inode->size) {

        uint64_t new_size = request->setattr.set_attr->va_size;
        uint64_t old_size = inode->size;

        cairn_punch_hole(thread, shared, inode, new_size, old_size - new_size);
    }

    /* cairn_apply_attrs() rewrites set_attr->va_set_mask down to the scalar
     * bits it consumes (it drops the ACL bit), so capture the caller's original
     * mask first -- the ACL-coherence block below keys off it. */
    uint64_t orig_set_mask = request->setattr.set_attr->va_set_mask;

    cairn_apply_attrs(inode, request->setattr.set_attr);

    /* POSIX: a successful (f)truncate marks the last data modification time.
     * ctime is stamped by cairn_apply_attrs; bump mtime here unless the caller
     * supplied an explicit mtime, or is an AUTH_ATTR (SMB/Windows) caller that
     * manages the write time itself. */
    if ((orig_set_mask & CHIMERA_VFS_ATTR_SIZE) && S_ISREG(inode->mode) &&
        !(orig_set_mask & CHIMERA_VFS_ATTR_MTIME) &&
        request->cred->flavor != CHIMERA_VFS_AUTH_ATTR) {
        clock_gettime(CLOCK_REALTIME, &inode->mtime);
    }

    /* Restore the caller's mask: on an optimistic-commit conflict cairn rolls
     * back and re-dispatches this queued request, and the replay re-reads
     * orig_set_mask from this field.  If it stayed scalar-only the ACL re-put
     * would be skipped and the replayed transaction would drop the descriptor
     * (an intermittent, load-dependent read-your-writes failure for SET-ACL). */
    request->setattr.set_attr->va_set_mask = orig_set_mask;

    /* ACL coherence (mirrors memfs): an explicit ACL set is persisted and the
     * mode re-derived; a bare chmod regenerates the special-who ACEs of any
     * stored ACL while preserving named entries. */
    {
        struct chimera_vfs_attrs *sa = request->setattr.set_attr;

        if (orig_set_mask & CHIMERA_VFS_ATTR_ACL) {
            if (sa->va_acl && sa->va_acl->num_aces) {
                cairn_put_acl(thread, inode->inum, sa->va_acl);
                inode->mode = (inode->mode & S_IFMT) |
                    chimera_acl_to_mode(sa->va_acl);
            } else {
                cairn_remove_acl(thread, inode->inum);
            }
        } else if (orig_set_mask & CHIMERA_VFS_ATTR_MODE) {
            static __thread uint8_t old_buf[CAIRN_ACL_STRUCT_SCRATCH];
            static __thread uint8_t new_buf[CAIRN_ACL_STRUCT_SCRATCH];
            struct chimera_acl     *old_acl = (struct chimera_acl *) old_buf;
            struct chimera_acl     *new_acl = (struct chimera_acl *) new_buf;

            if (cairn_load_acl(thread, inode->inum, old_acl) &&
                chimera_acl_chmod(old_acl, inode->mode, new_acl,
                                  CHIMERA_ACL_MAX_ACES) >= 0) {
                cairn_put_acl(thread, inode->inum, new_acl);
            }
        }
    }

    cairn_map_attrs(shared, &request->setattr.r_post_attr, inode);

    cairn_put_inode(thread, inode);
    cairn_inode_handle_release(&ih);

    request->status = CHIMERA_VFS_OK;

    cairn_queue_request(thread, request);
} /* cairn_setattr */

static inline int
cairn_lookup_path(
    struct cairn_thread       *thread,
    struct cairn_shared       *shared,
    const char                *path,
    int                        pathlen,
    struct cairn_inode_handle *ih)
{
    struct cairn_inode_handle  parent_ih;
    struct cairn_inode        *inode;
    struct cairn_dirent_key    dirent_key;
    struct cairn_dirent_value *dirent_value;
    struct cairn_dirent_handle dh;
    const char                *name;
    const char                *pathc = path;
    const char                *slash;
    int                        namelen;
    uint64_t                   hash;
    int                        rc;

    rc = cairn_inode_get_fh(thread, shared->root_fh, shared->root_fhlen, &parent_ih);

    if (unlikely(rc)) {
        return -1;
    }

    inode = parent_ih.inode;

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

        if (!S_ISDIR(inode->mode)) {
            cairn_inode_handle_release(&parent_ih);
            return -1;
        }

        hash = chimera_vfs_hash(name, namelen);

        dirent_key.keytype = CAIRN_KEY_DIRENT;
        dirent_key.inum    = inode->inum;
        dirent_key.hash    = hash;

        rc = cairn_dirent_get(thread, &dirent_key, &dh);

        if (rc) {
            cairn_inode_handle_release(&parent_ih);
            return -1;
        }

        dirent_value = dh.dirent;

        cairn_inode_handle_release(&parent_ih);

        rc = cairn_inode_get_inum(thread, dirent_value->inum, &parent_ih);

        cairn_dirent_handle_release(&dh);

        if (rc) {
            return -1;
        }

        inode = parent_ih.inode;

    }

    *ih = parent_ih;

    return 0;

} /* cairn_lookup_path */

static void
cairn_mount(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle ih;
    struct cairn_inode       *inode;
    int                       rc;

    rc = cairn_lookup_path(thread, shared, request->mount.path, request->mount.pathlen, &ih);

    if (unlikely(rc)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;

    cairn_map_attrs(shared, &request->mount.r_attr, inode);

    cairn_inode_handle_release(&ih);

    request->status = CHIMERA_VFS_OK;

    request->complete(request);
} /* cairn_mount */

static void
cairn_umount(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    /* No action required */
    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* cairn_umount */

static void
cairn_lookup_at(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle  ih, child_ih;
    struct cairn_inode        *inode, *child;
    struct cairn_dirent_key    dirent_key;
    struct cairn_dirent_value *dirent_value;
    struct cairn_dirent_handle dh;
    const char                *name    = request->lookup_at.component;
    uint32_t                   namelen = request->lookup_at.component_len;
    int                        rc;

    rc = cairn_inode_get_fh(thread, request->fh, request->fh_len, &ih);

    if (unlikely(rc)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;

    if (unlikely(!S_ISDIR(inode->mode))) {
        enum chimera_vfs_error err = S_ISLNK(inode->mode) ? CHIMERA_VFS_ESYMLINK : CHIMERA_VFS_ENOTDIR;
        cairn_inode_handle_release(&ih);
        request->status = err;
        request->complete(request);
        return;
    }

    /* Handle "." - return the directory itself */
    if (namelen == 1 && name[0] == '.') {
        cairn_map_attrs(shared, &request->lookup_at.r_dir_attr, inode);
        cairn_map_attrs(shared, &request->lookup_at.r_attr, inode);
        cairn_inode_handle_release(&ih);
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    /* Handle ".." - return the parent directory */
    if (namelen == 2 && name[0] == '.' && name[1] == '.') {
        cairn_map_attrs(shared, &request->lookup_at.r_dir_attr, inode);

        rc = cairn_inode_get_inum(thread, inode->parent_inum, &child_ih);

        if (unlikely(rc)) {
            cairn_inode_handle_release(&ih);
            request->status = CHIMERA_VFS_ENOENT;
            request->complete(request);
            return;
        }

        child = child_ih.inode;
        cairn_map_attrs(shared, &request->lookup_at.r_attr, child);
        cairn_inode_handle_release(&ih);
        cairn_inode_handle_release(&child_ih);
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    dirent_key.keytype = CAIRN_KEY_DIRENT;
    dirent_key.inum    = inode->inum;
    dirent_key.hash    = request->lookup_at.component_hash;

    rc = cairn_dirent_get(thread, &dirent_key, &dh);

    if (rc) {
        cairn_inode_handle_release(&ih);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    dirent_value = dh.dirent;

    cairn_map_attrs(shared, &request->lookup_at.r_dir_attr, inode);

    rc = cairn_inode_get_inum(thread, dirent_value->inum, &child_ih);

    if (rc) {
        cairn_inode_handle_release(&ih);
        cairn_dirent_handle_release(&dh);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    child = child_ih.inode;

    cairn_map_attrs(shared, &request->lookup_at.r_attr, child);

    cairn_inode_handle_release(&ih);
    cairn_dirent_handle_release(&dh);
    cairn_inode_handle_release(&child_ih);

    request->status = CHIMERA_VFS_OK;

    request->complete(request);
} /* cairn_lookup_at */

static void
cairn_mkdir_at(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle  parent_ih, existing_ih;
    struct cairn_inode        *parent_inode, *existing_inode, inode;
    struct cairn_dirent_key    dirent_key;
    struct cairn_dirent_value  dirent_value;
    struct cairn_dirent_handle dh;
    int                        rc;
    struct timespec            now;

    clock_gettime(CLOCK_REALTIME, &now);

    rc = cairn_inode_get_fh(thread, request->fh, request->fh_len, &parent_ih);

    if (unlikely(rc)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    parent_inode = parent_ih.inode;

    if (!S_ISDIR(parent_inode->mode)) {
        cairn_inode_handle_release(&parent_ih);
        request->status = CHIMERA_VFS_ENOTDIR;
        request->complete(request);
        return;
    }

    dirent_key.keytype = CAIRN_KEY_DIRENT;
    dirent_key.inum    = parent_inode->inum;
    dirent_key.hash    = request->mkdir_at.name_hash;

    rc = cairn_dirent_get(thread, &dirent_key, &dh);

    if (rc == 0) {
        cairn_map_attrs(shared, &request->mkdir_at.r_dir_pre_attr, parent_inode);
        cairn_map_attrs(shared, &request->mkdir_at.r_dir_post_attr, parent_inode);

        rc = cairn_inode_get_inum(thread, dh.dirent->inum, &existing_ih);

        if (rc == 0) {
            existing_inode = existing_ih.inode;
            cairn_map_attrs(shared, &request->mkdir_at.r_attr, existing_inode);
            cairn_inode_handle_release(&existing_ih);
        }
        cairn_inode_handle_release(&parent_ih);
        cairn_dirent_handle_release(&dh);
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        return;
    }

    cairn_alloc_inum(thread, &inode);
    inode.parent_inum = parent_inode->inum;    /* Set parent for ".." lookup */
    inode.size        = 4096;
    inode.space_used  = 4096;
    inode.uid         = request->cred->uid;
    inode.gid         = request->cred->gid;
    inode.nlink       = 2;
    inode.rdev        = 0;
    inode.mode        = S_IFDIR | 0755;
    inode.atime       = now;
    inode.mtime       = now;
    inode.ctime       = now;
    inode.change++;
    inode.btime          = now;
    inode.dos_attributes = 0;

    /* Snapshot any explicit ACL pointer BEFORE cairn_apply_attrs() rewrites
     * va_set_mask and drops the ATTR_ACL bit. */
    const struct chimera_acl *new_acl_mkdir =
        (request->mkdir_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_ACL)
        ? request->mkdir_at.set_attr->va_acl : NULL;

    cairn_apply_attrs(&inode, request->mkdir_at.set_attr);

    cairn_inherit_acl(thread, &inode, parent_inode->inum,
                      new_acl_mkdir,
                      request->cred->flavor == CHIMERA_VFS_AUTH_ATTR);

    cairn_map_attrs(shared, &request->mkdir_at.r_attr, &inode);
    cairn_map_acl(thread, &request->mkdir_at.r_attr, &inode);

    dirent_value.inum     = inode.inum;
    dirent_value.name_len = request->mkdir_at.name_len;
    memcpy(dirent_value.name, request->mkdir_at.name, request->mkdir_at.name_len);

    cairn_map_attrs(shared, &request->mkdir_at.r_dir_pre_attr, parent_inode);

    parent_inode->nlink++;

    parent_inode->mtime = now;
    parent_inode->ctime = now;
    parent_inode->change++;

    cairn_map_attrs(shared, &request->mkdir_at.r_dir_post_attr, parent_inode);

    cairn_put_dirent(thread, &dirent_key, &dirent_value);
    cairn_put_inode(thread, parent_inode);
    cairn_put_inode(thread, &inode);

    request->status = CHIMERA_VFS_OK;

    cairn_inode_handle_release(&parent_ih);

    cairn_queue_request(thread, request);
} /* cairn_mkdir_at */

static void
cairn_mknod_at(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle  parent_ih, existing_ih;
    struct cairn_inode        *parent_inode, *existing_inode, inode;
    struct cairn_dirent_key    dirent_key;
    struct cairn_dirent_value  dirent_value;
    struct cairn_dirent_handle dh;
    int                        rc;
    struct timespec            now;

    clock_gettime(CLOCK_REALTIME, &now);

    rc = cairn_inode_get_fh(thread, request->fh, request->fh_len, &parent_ih);

    if (unlikely(rc)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    parent_inode = parent_ih.inode;

    if (!S_ISDIR(parent_inode->mode)) {
        cairn_inode_handle_release(&parent_ih);
        request->status = CHIMERA_VFS_ENOTDIR;
        request->complete(request);
        return;
    }

    dirent_key.keytype = CAIRN_KEY_DIRENT;
    dirent_key.inum    = parent_inode->inum;
    dirent_key.hash    = request->mknod_at.name_hash;

    rc = cairn_dirent_get(thread, &dirent_key, &dh);

    if (rc == 0) {
        cairn_map_attrs(shared, &request->mknod_at.r_dir_pre_attr, parent_inode);
        cairn_map_attrs(shared, &request->mknod_at.r_dir_post_attr, parent_inode);

        rc = cairn_inode_get_inum(thread, dh.dirent->inum, &existing_ih);

        if (rc == 0) {
            existing_inode = existing_ih.inode;
            cairn_map_attrs(shared, &request->mknod_at.r_attr, existing_inode);
            cairn_inode_handle_release(&existing_ih);
        }
        cairn_inode_handle_release(&parent_ih);
        cairn_dirent_handle_release(&dh);
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        return;
    }

    cairn_alloc_inum(thread, &inode);
    inode.parent_inum = parent_inode->inum;
    inode.size        = 0;
    inode.space_used  = 0;
    inode.uid         = request->cred->uid;
    inode.gid         = request->cred->gid;
    inode.nlink       = 1;
    inode.rdev        = 0;
    inode.atime       = now;
    inode.mtime       = now;
    inode.ctime       = now;
    inode.change++;
    inode.btime          = now;
    inode.dos_attributes = 0;

    /* Set mode (including file type bits) and rdev from set_attr */
    if (request->mknod_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        inode.mode = request->mknod_at.set_attr->va_mode;
    } else {
        inode.mode = S_IFREG | 0644;
    }

    if (request->mknod_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_RDEV) {
        inode.rdev = request->mknod_at.set_attr->va_rdev;
    }

    cairn_apply_attrs(&inode, request->mknod_at.set_attr);

    cairn_map_attrs(shared, &request->mknod_at.r_attr, &inode);

    dirent_value.inum     = inode.inum;
    dirent_value.name_len = request->mknod_at.name_len;
    memcpy(dirent_value.name, request->mknod_at.name, request->mknod_at.name_len);

    cairn_map_attrs(shared, &request->mknod_at.r_dir_pre_attr, parent_inode);

    parent_inode->mtime = now;
    parent_inode->ctime = now;
    parent_inode->change++;

    cairn_map_attrs(shared, &request->mknod_at.r_dir_post_attr, parent_inode);

    cairn_put_dirent(thread, &dirent_key, &dirent_value);
    cairn_put_inode(thread, parent_inode);
    cairn_put_inode(thread, &inode);

    request->status = CHIMERA_VFS_OK;

    cairn_inode_handle_release(&parent_ih);

    cairn_queue_request(thread, request);
} /* cairn_mknod_at */

static void
cairn_remove_at(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle  parent_ih, child_ih;
    struct cairn_inode        *parent_inode, *inode;
    struct cairn_dirent_key    dirent_key;
    struct cairn_dirent_handle dh;
    struct cairn_dirent_value *dirent_value;
    int                        rc;
    struct timespec            now;

    clock_gettime(CLOCK_REALTIME, &now);
    rc = cairn_inode_get_fh(thread, request->fh, request->fh_len, &parent_ih);

    if (unlikely(rc)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    parent_inode = parent_ih.inode;

    if (!S_ISDIR(parent_inode->mode)) {
        cairn_inode_handle_release(&parent_ih);
        request->status = CHIMERA_VFS_ENOTDIR;
        request->complete(request);
        return;
    }

    dirent_key.keytype = CAIRN_KEY_DIRENT;
    dirent_key.inum    = parent_inode->inum;
    dirent_key.hash    = request->remove_at.name_hash;

    rc = cairn_dirent_get(thread, &dirent_key, &dh);

    if (rc) {
        cairn_inode_handle_release(&parent_ih);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    dirent_value = dh.dirent;

    rc = cairn_inode_get_inum(thread, dirent_value->inum, &child_ih);

    if (rc) {
        cairn_inode_handle_release(&parent_ih);
        cairn_dirent_handle_release(&dh);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = child_ih.inode;

    if (S_ISDIR(inode->mode)) {
        /* Check if directory is empty (proper rmdir semantics) */
        if (!cairn_directory_is_empty(thread, inode->inum)) {
            cairn_inode_handle_release(&parent_ih);
            cairn_inode_handle_release(&child_ih);
            cairn_dirent_handle_release(&dh);
            request->status = CHIMERA_VFS_ENOTEMPTY;
            request->complete(request);
            return;
        }
    }

    cairn_map_attrs(shared, &request->remove_at.r_dir_pre_attr, parent_inode);

    parent_inode->mtime = now;
    parent_inode->ctime = now;
    parent_inode->change++;

    if (S_ISDIR(inode->mode)) {
        inode->nlink = 0;
        parent_inode->nlink--;

    } else {
        inode->nlink--;
        /* Removing one of several hard links changes the surviving inode's
         * link count, which is a status change: bump its ctime. */
        if (inode->nlink > 0) {
            inode->ctime = now;
            inode->change++;
        }
    }

    if (inode->nlink == 0) {
        request->remove_at.r_removed_attr.va_req_mask = CHIMERA_VFS_ATTR_FH;
    }

    cairn_map_attrs(shared, &request->remove_at.r_removed_attr, inode);

    if (inode->nlink == 0) {
        --inode->refcnt;

        if (inode->refcnt == 0) {
            // Remove type-specific data before removing inode
            if (S_ISREG(inode->mode)) {
                cairn_remove_file_extents(thread, inode->inum);
            } else if (S_ISLNK(inode->mode)) {
                cairn_remove_symlink_target(thread, inode->inum);
            }

            cairn_remove_inode(thread, inode);
            cairn_remove_acl(thread, inode->inum);
        } else {
            cairn_put_inode(thread, inode);
        }
    } else {
        // nlink > 0: file still has other hard links, persist the decremented nlink
        cairn_put_inode(thread, inode);
    }

    cairn_map_attrs(shared, &request->remove_at.r_dir_post_attr, parent_inode);

    cairn_remove_dirent(thread, &dirent_key);

    cairn_put_inode(thread, parent_inode);

    cairn_inode_handle_release(&parent_ih);
    cairn_inode_handle_release(&child_ih);
    cairn_dirent_handle_release(&dh);

    request->status = CHIMERA_VFS_OK;
    cairn_queue_request(thread, request);
} /* cairn_remove_at */

/*
 * Cookie values for readdir:
 *   0 = start of directory, will return "."
 *   1 = "." was returned, will return ".."
 *   2 = ".." was returned, will return first real entry
 *   3+ = real entry cookie (hash + 3)
 */
#define CAIRN_COOKIE_DOT    1
#define CAIRN_COOKIE_DOTDOT 2
#define CAIRN_COOKIE_FIRST  3

static void
cairn_readdir(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle  ih, dirent_ih, parent_ih;
    struct cairn_inode        *inode, *dirent_inode, *parent_inode;
    uint64_t                   cookie      = request->readdir.cookie;
    uint64_t                   next_cookie = 0;
    int                        rc, eof = 1;
    struct chimera_vfs_attrs   attr;
    rocksdb_iterator_t        *iter = NULL;
    struct cairn_dirent_key    start_key, *dirent_key;
    struct cairn_dirent_value *dirent_value;
    size_t                     len;

    rc = cairn_inode_get_fh(thread, request->fh, request->fh_len, &ih);

    if (rc) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;

    if (!S_ISDIR(inode->mode)) {
        cairn_inode_handle_release(&ih);
        request->status = CHIMERA_VFS_ENOTDIR;
        request->complete(request);
        return;
    }

    attr.va_req_mask = request->readdir.attr_mask;

    /* Handle "." and ".." entries only if requested */
    if (request->readdir.flags & CHIMERA_VFS_READDIR_EMIT_DOT) {
        /* Handle "." entry (cookie 0 -> 1) */
        if (cookie < CAIRN_COOKIE_DOT) {
            cairn_map_attrs(shared, &attr, inode);

            rc = request->readdir.callback(
                inode->inum,
                CAIRN_COOKIE_DOT,
                ".",
                1,
                &attr,
                request->proto_private_data);

            if (rc) {
                next_cookie = CAIRN_COOKIE_DOT;
                eof         = 0;
                goto out;
            }

            cookie = CAIRN_COOKIE_DOT;
        }

        /* Handle ".." entry (cookie 1 -> 2) */
        if (cookie < CAIRN_COOKIE_DOTDOT) {
            rc = cairn_inode_get_inum(thread, inode->parent_inum, &parent_ih);

            if (rc == 0) {
                parent_inode = parent_ih.inode;
                cairn_map_attrs(shared, &attr, parent_inode);
                cairn_inode_handle_release(&parent_ih);
            } else {
                cairn_map_attrs(shared, &attr, inode);
            }

            rc = request->readdir.callback(
                inode->parent_inum,
                CAIRN_COOKIE_DOTDOT,
                "..",
                2,
                &attr,
                request->proto_private_data);

            if (rc) {
                next_cookie = CAIRN_COOKIE_DOTDOT;
                eof         = 0;
                goto out;
            }

            cookie = CAIRN_COOKIE_DOTDOT;
        }
    } else {
        /* Skip . and .. entries - advance cookie past them */
        if (cookie < CAIRN_COOKIE_DOTDOT) {
            cookie = CAIRN_COOKIE_DOTDOT;
        }
    }

    /* Handle real directory entries (cookie >= 2) */
    start_key.keytype = CAIRN_KEY_DIRENT;
    start_key.inum    = inode->inum;

    if (cookie < CAIRN_COOKIE_FIRST) {
        /* Start from the beginning of real entries */
        start_key.hash = 0;
    } else {
        /* Resume from where we left off - cookie is (hash + 3) */
        start_key.hash = cookie - CAIRN_COOKIE_FIRST;
    }

    iter = cairn_meta_iterator(thread);

    rocksdb_iter_seek(iter, (const char *) &start_key, sizeof(start_key));

    /* If resuming (cookie >= 3), skip past the last returned entry */
    if (rocksdb_iter_valid(iter) && cookie >= CAIRN_COOKIE_FIRST) {
        rocksdb_iter_next(iter);
    }

    while (rocksdb_iter_valid(iter)) {

        dirent_key = (struct cairn_dirent_key *) rocksdb_iter_key(iter, &len);

        if (dirent_key->keytype != CAIRN_KEY_DIRENT || dirent_key->inum != inode->inum) {
            break;
        }

        dirent_value = (struct cairn_dirent_value *) rocksdb_iter_value(iter, &len);

        rc = cairn_inode_get_inum(thread, dirent_value->inum, &dirent_ih);

        if (rc) {
            rocksdb_iter_next(iter);
            continue;
        }

        dirent_inode = dirent_ih.inode;

        cairn_map_attrs(shared, &attr, dirent_inode);
        cairn_map_acl(thread, &attr, dirent_inode);

        cairn_inode_handle_release(&dirent_ih);

        rc = request->readdir.callback(
            dirent_value->inum,
            dirent_key->hash + CAIRN_COOKIE_FIRST,
            dirent_value->name,
            dirent_value->name_len,
            &attr,
            request->proto_private_data);

        next_cookie = dirent_key->hash + CAIRN_COOKIE_FIRST;

        if (rc) {
            eof = 0;
            break;
        }

        rocksdb_iter_next(iter);

    } /* cairn_readdir */

    rocksdb_iter_destroy(iter);
    iter = NULL;

 out:
    if (iter) {
        rocksdb_iter_destroy(iter);
    }

    cairn_map_attrs(shared, &request->readdir.r_dir_attr, inode);

    cairn_inode_handle_release(&ih);

    request->status           = CHIMERA_VFS_OK;
    request->readdir.r_cookie = next_cookie;
    request->readdir.r_eof    = eof;

    request->complete(request);
} /* cairn_readdir */

static void
cairn_open_fh(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle ih;
    struct cairn_inode       *inode;
    int                       rc;

    rc = cairn_inode_get_fh(thread, request->fh, request->fh_len, &ih);

    if (rc) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;

    if ((request->open_fh.flags & CHIMERA_VFS_OPEN_DIRECTORY) &&
        !S_ISDIR(inode->mode)) {
        cairn_inode_handle_release(&ih);
        request->status = CHIMERA_VFS_ENOTDIR;
        request->complete(request);
        return;
    }

    inode->refcnt++;

    request->open_fh.r_vfs_private = (uint64_t) inode->inum;

    cairn_put_inode(thread, inode);
    cairn_inode_handle_release(&ih);

    cairn_stage_handle_state(thread, request->open_fh.handle_state);

    request->status = CHIMERA_VFS_OK;

    cairn_queue_request(thread, request);
} /* cairn_open_fh */

/* Mode/ACL-aware access check against a cairn inode (mirror of
 * memfs_inode_access): build the canonical attrs and consult the shared engine,
 * loading the stored ACL when present. */
static int
cairn_inode_access(
    struct cairn_thread           *thread,
    const struct cairn_inode      *inode,
    const struct chimera_vfs_cred *cred,
    uint32_t                       requested)
{
    static __thread uint8_t  aclbuf[CAIRN_ACL_STRUCT_SCRATCH];
    struct chimera_acl      *acl = (struct chimera_acl *) aclbuf;
    struct chimera_vfs_attrs attr;

    attr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
        CHIMERA_VFS_ATTR_GID;
    attr.va_mode = inode->mode;
    attr.va_uid  = inode->uid;
    attr.va_gid  = inode->gid;

    if (cairn_load_acl(thread, inode->inum, acl)) {
        attr.va_set_mask |= CHIMERA_VFS_ATTR_ACL;
        attr.va_acl       = acl;
    }

    return chimera_vfs_access_allowed(&attr, cred, requested);
} /* cairn_inode_access */

static void
cairn_open_at(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle  parent_ih, child_ih;
    struct cairn_inode        *parent_inode, *inode = NULL, new_inode;
    struct cairn_dirent_key    dirent_key;
    struct cairn_dirent_handle dh;
    struct cairn_dirent_value *dirent_value, new_dirent_value;
    unsigned int               flags = request->open_at.flags;
    int                        rc, is_new_inode = 0;
    struct timespec            now;

    clock_gettime(CLOCK_REALTIME, &now);

    rc = cairn_inode_get_fh(thread, request->fh, request->fh_len, &parent_ih);

    if (unlikely(rc)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    parent_inode = parent_ih.inode;

    if (!S_ISDIR(parent_inode->mode)) {
        cairn_inode_handle_release(&parent_ih);
        request->status = CHIMERA_VFS_ENOTDIR;
        request->complete(request);
        return;
    }

    cairn_map_attrs(shared, &request->open_at.r_dir_pre_attr, parent_inode);

    dirent_key.keytype = CAIRN_KEY_DIRENT;
    dirent_key.inum    = parent_inode->inum;
    dirent_key.hash    = request->open_at.name_hash;

    rc = cairn_dirent_get(thread, &dirent_key, &dh);

    if (rc) {
        if (!(flags & CHIMERA_VFS_OPEN_CREATE)) {
            cairn_inode_handle_release(&parent_ih);
            request->status = CHIMERA_VFS_ENOENT;
            request->complete(request);
            return;
        }

        /* Creating a new file requires add-file (WRITE_DATA) + search (EXECUTE)
         * permission on the parent directory.  On the NFSv4/Windows ACL model
         * WRITE_DATA == ADD_FILE and APPEND_DATA == ADD_SUBDIRECTORY, so a plain
         * file create is gated by WRITE_DATA (mkdir is gated by APPEND_DATA in
         * the VFS-core mkdir_at path).  Enforce POSIX semantics for AUTH_UNIX
         * callers (root is exempt); SMB/ACL (AUTH_ATTR) callers are authorized
         * by the engine. */
        if (request->cred->flavor == CHIMERA_VFS_AUTH_UNIX &&
            request->cred->uid != 0 &&
            !cairn_inode_access(thread, parent_inode, request->cred,
                                CHIMERA_ACE_WRITE_DATA | CHIMERA_ACE_EXECUTE)) {
            cairn_inode_handle_release(&parent_ih);
            request->status = CHIMERA_VFS_EACCES;
            request->complete(request);
            return;
        }

        is_new_inode = 1;

        cairn_alloc_inum(thread, &new_inode);
        new_inode.size       = 0;
        new_inode.space_used = 0;
        new_inode.uid        = request->cred->uid;
        new_inode.gid        = request->cred->gid;
        new_inode.nlink      = 1;
        new_inode.rdev       = 0;
        new_inode.mode       = S_IFREG |  0644;
        new_inode.atime      = now;
        new_inode.mtime      = now;
        new_inode.ctime      = now;
        new_inode.change++;
        new_inode.btime          = now;
        new_inode.dos_attributes = 0;
        new_inode.refcnt         = 1;

        /* Snapshot any explicit ACL pointer BEFORE cairn_apply_attrs() rewrites
         * va_set_mask and drops the ATTR_ACL bit. */
        const struct chimera_acl *new_acl_open =
            (request->open_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_ACL)
            ? request->open_at.set_attr->va_acl : NULL;

        cairn_apply_attrs(&new_inode, request->open_at.set_attr);

        cairn_inherit_acl(thread, &new_inode, parent_inode->inum,
                          new_acl_open,
                          request->cred->flavor == CHIMERA_VFS_AUTH_ATTR);

        new_dirent_value.inum     = new_inode.inum;
        new_dirent_value.name_len = request->open_at.namelen;
        memcpy(new_dirent_value.name, request->open_at.name, request->open_at.namelen);

        cairn_put_dirent(thread, &dirent_key, &new_dirent_value);

        parent_inode->mtime = now;
        parent_inode->ctime = now;
        parent_inode->change++;

        /* Signal to the protocol layer that this open created the file (vs.
         * opened an existing one) so the SMB CREATE reply reports the correct
         * Create Action (FILE_CREATED vs FILE_OPENED) for OPEN_IF / SUPERSEDE /
         * OVERWRITE_IF dispositions.  Matches memfs/diskfs. */
        request->open_at.r_created = 1;

        inode = &new_inode;
    } else if (flags & CHIMERA_VFS_OPEN_EXCLUSIVE) {
        cairn_inode_handle_release(&parent_ih);
        cairn_dirent_handle_release(&dh);
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        return;
    } else {

        dirent_value = dh.dirent;

        rc = cairn_inode_get_inum(thread, dirent_value->inum, &child_ih);

        if (rc) {
            cairn_inode_handle_release(&parent_ih);
            cairn_dirent_handle_release(&dh);
            request->status = CHIMERA_VFS_ENOENT;
            request->complete(request);
            return;
        }

        inode = child_ih.inode;

        cairn_dirent_handle_release(&dh);

        /* A symlink as the final component under O_NOFOLLOW: a *data* open
         * (POSIX open(O_NOFOLLOW)) must fail with ELOOP, but an O_PATH-style
         * open (SMB FILE_OPEN_REPARSE_POINT, i.e. O_PATH|O_NOFOLLOW) wants a
         * handle to the link itself so the caller can read its attributes /
         * security descriptor / reparse data -- so fall through and open the
         * symlink inode in that case (mirrors memfs and the linux backend). */
        if (S_ISLNK(inode->mode) && (flags & CHIMERA_VFS_OPEN_NOFOLLOW) &&
            !(flags & CHIMERA_VFS_OPEN_PATH)) {
            cairn_inode_handle_release(&parent_ih);
            cairn_inode_handle_release(&child_ih);
            request->status = CHIMERA_VFS_ELOOP;
            request->complete(request);
            return;
        }
    }

    if ((flags & CHIMERA_VFS_OPEN_DIRECTORY) && !S_ISDIR(inode->mode)) {
        cairn_inode_handle_release(&parent_ih);
        if (!is_new_inode) {
            cairn_inode_handle_release(&child_ih);
        }
        request->status = CHIMERA_VFS_ENOTDIR;
        request->complete(request);
        return;
    }

    /* No coarse mode-based open access check here: it is ACL-blind (it would
     * deny e.g. a Windows owner-full-control DACL on a 0644 file) and
     * mishandles SMB control-only opens.  Access is enforced by the ACL-aware
     * VFS gate and the protocol create-time check, as on memfs/diskfs. */

    if (!is_new_inode && S_ISREG(inode->mode) &&
        (flags & CHIMERA_VFS_OPEN_TRUNCATE)) {
        /* Overwrite/supersede disposition: replace the existing file's
         * contents (truncate to zero) and apply the new attributes (including
         * DOS attributes), mirroring memfs.  The SMB layer conveys the truncate
         * via OPEN_TRUNCATE rather than a SIZE=0 set_attr, so key off the flag. */
        if (inode->size) {
            cairn_punch_hole(thread, shared, inode, 0, inode->size);
        }
        inode->size       = 0;
        inode->space_used = 0;
        cairn_apply_attrs(inode, request->open_at.set_attr);
    } else if (!is_new_inode &&
               (request->open_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) &&
               request->open_at.set_attr->va_size == 0 &&
               S_ISREG(inode->mode)) {
        cairn_apply_attrs(inode, request->open_at.set_attr);
        inode->space_used = 0;
    }

    if (flags & CHIMERA_VFS_OPEN_INFERRED) {
        /* If this is an inferred open (ie an NFS3 create)
         * then we aren't returning a handle so we don't need
         * to increment the refcnt */

        request->open_at.r_vfs_private = 0xdeadbeefUL;

    } else {
        inode->refcnt++;
        request->open_at.r_vfs_private = (uint64_t) inode->inum;
    }

    cairn_map_attrs(shared, &request->open_at.r_dir_post_attr, parent_inode);
    cairn_map_attrs(shared, &request->open_at.r_attr, inode);
    cairn_map_acl(thread, &request->open_at.r_attr, inode);

    cairn_put_inode(thread, parent_inode);
    cairn_put_inode(thread, inode);

    cairn_inode_handle_release(&parent_ih);

    if (!is_new_inode) {
        cairn_inode_handle_release(&child_ih);
    }

    cairn_stage_handle_state(thread, request->open_at.handle_state);

    request->status = CHIMERA_VFS_OK;
    cairn_queue_request(thread, request);
} /* cairn_open_at */

static void
cairn_close(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle ih;
    struct cairn_inode       *inode;
    int                       rc;

    rc = cairn_inode_get_fh(thread, request->fh, request->fh_len, &ih);

    if (rc) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;
    inode->refcnt--;

    if (inode->refcnt == 0 && inode->nlink == 0) {
        // Remove type-specific data before removing inode
        if (S_ISREG(inode->mode)) {
            cairn_remove_file_extents(thread, inode->inum);
        } else if (S_ISLNK(inode->mode)) {
            cairn_remove_symlink_target(thread, inode->inum);
        }

        cairn_remove_inode(thread, inode);
        cairn_remove_acl(thread, inode->inum);
    } else {
        cairn_put_inode(thread, inode);
    }

    cairn_inode_handle_release(&ih);

    request->status = CHIMERA_VFS_OK;

    cairn_queue_request(thread, request);
} /* cairn_close */

static void
cairn_read(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    rocksdb_iterator_t       *iter;
    struct cairn_inode_handle ih;
    struct cairn_inode       *inode;
    struct cairn_extent_key   start_key, *extent_key;
    struct evpl_iovec        *iov;
    uint64_t                  offset, length, current_offset;
    uint64_t                  bytes_remaining;
    uint32_t                  eof = 0;
    size_t                    klen, vlen;
    int                       rc;
    int                       need_atime = !shared->noatime;
    struct timespec           now;

    clock_gettime(CLOCK_REALTIME, &now);

    offset = request->read.offset;

    length = request->read.length;

    if (unlikely(length == 0)) {
        request->status        = CHIMERA_VFS_OK;
        request->read.r_niov   = 0;
        request->read.r_length = 0;
        request->read.r_eof    = 0;
        request->complete(request);
        return;
    }

    /*
     * cairn_read self-manages its snapshot view (it is not wrapped by
     * cairn_dispatch) because the best-effort atime stamp below has to run
     * after the snapshot is released, against the write transaction.
     */
    cairn_read_begin(thread, 1);

    rc = cairn_inode_get_fh(thread, request->fh, request->fh_len, &ih);

    if (rc) {
        cairn_read_end(thread);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;

    /* relatime: only stamp atime when the file changed since last access or the
     * recorded atime is a day stale, so steady-state reads neither rewrite the
     * inode to RocksDB nor churn the VFS attr cache. */
    need_atime = need_atime &&
        chimera_vfs_relatime_needs_update(&inode->atime, &inode->mtime, &inode->ctime, &now);

    if (offset >= inode->size) {
        cairn_map_attrs(shared, &request->read.r_attr, inode);
        cairn_inode_handle_release(&ih);
        cairn_read_end(thread);
        request->status        = CHIMERA_VFS_OK;
        request->read.r_niov   = 0;
        request->read.r_length = 0;
        request->read.r_eof    = 1;
        request->complete(request);
        return;
    }

    if (length >= inode->size - offset) {
        length = inode->size - offset;
        eof    = 1;
    }

    request->read.r_niov = evpl_iovec_alloc(thread->evpl, length, 4096, 1, EVPL_IOVEC_FLAG_SHARED, request->read.iov);
    iov                  = request->read.iov;

    start_key.keytype = CAIRN_KEY_EXTENT;
    start_key.inum    = inode->inum;
    start_key.offset  = htobe64(offset);

    iter = cairn_data_iterator(thread);

    rocksdb_iter_seek_for_prev(iter, (const char *) &start_key, sizeof(start_key));

    /*
     * After seek_for_prev, we might be at:
     * 1. No valid position (before the first key)
     * 2. An extent for a different inode
     * In these cases, seek forward to find extents within our range.
     */
    if (!rocksdb_iter_valid(iter)) {
        start_key.offset = htobe64(0);
        rocksdb_iter_seek(iter, (const char *) &start_key, sizeof(start_key));
    } else {
        extent_key = (struct cairn_extent_key *) rocksdb_iter_key(iter, &klen);

        if (extent_key->keytype != CAIRN_KEY_EXTENT || extent_key->inum != inode->inum) {
            /* Different inode, seek forward to our inode's first extent */
            start_key.offset = htobe64(0);
            rocksdb_iter_seek(iter, (const char *) &start_key, sizeof(start_key));
        }
    }

    current_offset  = offset;
    bytes_remaining = length;

    while (bytes_remaining > 0 && rocksdb_iter_valid(iter)) {
        extent_key = (struct cairn_extent_key *) rocksdb_iter_key(iter, &klen);

        if (extent_key->keytype != CAIRN_KEY_EXTENT || extent_key->inum != inode->inum) {
            break;
        }

        uint64_t extent_start = be64toh(extent_key->offset);
        uint64_t extent_length;
        rocksdb_iter_value(iter, &extent_length);
        uint64_t extent_end = extent_start + extent_length;

        if (current_offset < extent_start) {
            /* Fill hole with zeros */
            uint64_t hole_size = extent_start - current_offset;

            if (hole_size > bytes_remaining) {
                hole_size = bytes_remaining;
            }
            memset(iov[0].data + (current_offset - offset), 0, hole_size);
            current_offset  += hole_size;
            bytes_remaining -= hole_size;
        }

        // Skip if extent is entirely after our range
        if (extent_start >= offset + length) {
            break;
        }

        // Handle overlapping extent
        if (extent_end > current_offset && extent_start < offset + length) {
            const char *data          = rocksdb_iter_value(iter, &vlen);
            uint64_t    extent_offset = current_offset > extent_start ?
                current_offset - extent_start : 0;
            uint64_t    dest_offset = extent_start > offset ?
                extent_start - offset : 0;

            uint64_t    copy_size = extent_end - (extent_start + extent_offset);

            if (copy_size > bytes_remaining) {
                copy_size = bytes_remaining;
            }

            memcpy(iov[0].data + dest_offset,
                   data + extent_offset,
                   copy_size);

            current_offset  += copy_size;
            bytes_remaining -= copy_size;
        }

        rocksdb_iter_next(iter);
    }

    if (bytes_remaining) {
        /* Fill trailing hole with zeros */
        memset(iov[0].data + (current_offset - offset), 0, bytes_remaining);
    }

    rocksdb_iter_destroy(iter);

    cairn_map_attrs(shared, &request->read.r_attr, inode);

    cairn_inode_handle_release(&ih);

    /* Done with the snapshot view. */
    cairn_read_end(thread);

    /*
     * Best-effort atime stamp through the write transaction.  We re-read the
     * inode through the txn (read-your-writes) so we don't clobber a
     * concurrent same-cycle inode update, set atime, and write it back.  This
     * is decoupled from the read's completion: the read already returned its
     * data, and if the cycle's commit conflicts the read is not replayed, so
     * the atime update is simply lost (acceptable — atime is advisory).
     */
    if (need_atime) {
        struct cairn_inode_handle aih;

        if (cairn_inode_get_fh(thread, request->fh, request->fh_len, &aih) == 0) {
            aih.inode->atime = now;
            cairn_put_inode(thread, aih.inode);
            cairn_inode_handle_release(&aih);
        }
    }

    request->status        = CHIMERA_VFS_OK;
    request->read.r_length = length;
    request->read.r_eof    = eof;
    request->read.iov      = iov;
    iov[0].length          = length;

    request->complete(request);
} /* cairn_read */

static inline void
cairn_punch_hole(
    struct cairn_thread *thread,
    struct cairn_shared *shared,
    struct cairn_inode  *inode,
    uint64_t             offset,
    uint64_t             length)
{
    rocksdb_writebatch_t   *batch;
    rocksdb_iterator_t     *iter;
    struct cairn_extent_key start_key, *extent_key;
    uint64_t                hole_end    = offset + length;
    uint64_t                space_freed = 0;
    size_t                  klen;

    (void) shared;

    batch = cairn_get_data_batch(thread);

    start_key.keytype = CAIRN_KEY_EXTENT;
    start_key.inum    = inode->inum;
    start_key.offset  = htobe64(offset);

    iter = cairn_data_iterator(thread);

    // Find first extent less than or equal to our start offset
    rocksdb_iter_seek_for_prev(iter, (const char *) &start_key, sizeof(start_key));

    /*
     * After seek_for_prev, if we don't find a valid extent for our inode,
     * seek forward to find extents that might overlap our punch range.
     */
    if (!rocksdb_iter_valid(iter)) {
        start_key.offset = htobe64(0);
        rocksdb_iter_seek(iter, (const char *) &start_key, sizeof(start_key));
    } else {
        extent_key = (struct cairn_extent_key *) rocksdb_iter_key(iter, &klen);

        if (extent_key->keytype != CAIRN_KEY_EXTENT || extent_key->inum != inode->inum) {
            start_key.offset = htobe64(0);
            rocksdb_iter_seek(iter, (const char *) &start_key, sizeof(start_key));
        }
    }

    while (rocksdb_iter_valid(iter)) {
        extent_key = (struct cairn_extent_key *) rocksdb_iter_key(iter, &klen);
        uint64_t    extent_length;
        const char *extent_data = rocksdb_iter_value(iter, &extent_length);

        // Stop if we've moved past this inode
        if (extent_key->keytype != CAIRN_KEY_EXTENT || extent_key->inum != inode->inum) {
            break;
        }

        uint64_t    extent_start = be64toh(extent_key->offset);
        uint64_t    extent_end   = extent_start + extent_length;

        // Stop if extent starts after hole
        if (extent_start >= hole_end) {
            break;
        }

        // Check for overlap
        if (extent_end > offset && extent_start < hole_end) {
            // Track space being freed from original extent
            space_freed += extent_length;

            // Delete the original extent
            rocksdb_writebatch_delete(batch, (const char *) extent_key, sizeof(*extent_key));

            // If there's data before the hole, create a new extent
            if (extent_start < offset) {
                struct cairn_extent_key new_key = {
                    .keytype = CAIRN_KEY_EXTENT,
                    .inum    = inode->inum,
                    .offset  = htobe64(extent_start),
                };

                rocksdb_writebatch_put(batch, (const char *) &new_key, sizeof(new_key),
                                       extent_data, offset - extent_start);

                // Add back space for the preserved portion
                space_freed -= offset - extent_start;
            }

            // If there's data after the hole, create a new extent
            if (extent_end > hole_end) {
                struct cairn_extent_key new_key = {
                    .keytype = CAIRN_KEY_EXTENT,
                    .inum    = inode->inum,
                    .offset  = htobe64(hole_end),
                };

                rocksdb_writebatch_put(batch, (const char *) &new_key, sizeof(new_key),
                                       extent_data + (hole_end - extent_start),
                                       extent_end - hole_end);

                // Add back space for the preserved portion
                space_freed -= extent_end - hole_end;
            }
        }

        rocksdb_iter_next(iter);
    }

    rocksdb_iter_destroy(iter);

    // Update the inode's space_used
    if (space_freed > 0) {
        inode->space_used -= space_freed;
    }
} /* cairn_punch_hole */

static void
cairn_write(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle ih;
    struct cairn_inode       *inode;
    uint64_t                  current_offset;
    uint64_t                  total_space = 0;
    int                       rc, i;
    struct timespec           now;

    clock_gettime(CLOCK_REALTIME, &now);

    rc = cairn_inode_get_fh(thread, request->fh, request->fh_len, &ih);

    if (rc) {
        /* Note: Write iovecs are NOT released here. They were allocated on the
         * server thread and must be released there. The server's write completion
         * callback handles the release after this request completes via doorbell.
         */
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;

    cairn_map_attrs(shared, &request->write.r_pre_attr, inode);

    if (request->write.length == 0) {
        cairn_map_attrs(shared, &request->write.r_post_attr, inode);
        cairn_inode_handle_release(&ih);

        request->status         = CHIMERA_VFS_OK;
        request->write.r_length = 0;
        request->write.r_sync   = CHIMERA_VFS_WRITE_FILESYNC;
        request->complete(request);
        return;
    }

    if (inode->size > request->write.offset) {
        cairn_punch_hole(thread, shared, inode, request->write.offset, request->write.length);
    }

    // Write each iovec as a new extent
    current_offset = request->write.offset;

    {
        rocksdb_writebatch_t *data_batch = cairn_get_data_batch(thread);

        for (i = 0; i < request->write.niov; i++) {
            const struct evpl_iovec *iov = &request->write.iov[i];

            struct cairn_extent_key  key = {
                .keytype = CAIRN_KEY_EXTENT,
                .inum    = inode->inum,
                .offset  = htobe64(current_offset),
            };

            rocksdb_writebatch_put(data_batch,
                                   (const char *) &key, sizeof(key),
                                   iov->data, iov->length);

            total_space    += iov->length;
            current_offset += iov->length;
        }
    }

    // Update inode size if needed
    if (inode->size < request->write.offset + request->write.length) {
        inode->size = request->write.offset + request->write.length;
    }

    // Update space used to track actual extent sizes
    inode->space_used += total_space;
    inode->mtime       = now;
    inode->ctime       = now;
    inode->change++;

    /* POSIX kill-priv: a non-privileged write to a regular file clears the
     * set-user-ID bit and the set-group-ID bit (when group-executable). */
    inode->mode = chimera_vfs_killpriv_mode(request->cred, inode->mode);

    cairn_map_attrs(shared, &request->write.r_post_attr, inode);

    cairn_put_inode(thread, inode);
    cairn_inode_handle_release(&ih);

    request->status         = CHIMERA_VFS_OK;
    request->write.r_length = request->write.length;
    request->write.r_sync   = CHIMERA_VFS_WRITE_FILESYNC;

    /* Note: Write iovecs are NOT released here. They were allocated on the
     * server thread and must be released there. The server's write completion
     * callback handles the release after this request completes via doorbell.
     */

    cairn_queue_request(thread, request);
} /* cairn_write */


static void
cairn_allocate(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle ih;
    struct cairn_inode       *inode;
    int                       rc;
    struct timespec           now;

    clock_gettime(CLOCK_REALTIME, &now);

    rc = cairn_inode_get_fh(thread, request->fh, request->fh_len, &ih);

    if (rc) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;

    cairn_map_attrs(shared, &request->allocate.r_pre_attr, inode);

    if (request->allocate.flags & CHIMERA_VFS_ALLOCATE_DEALLOCATE) {
        /* DEALLOCATE: punch hole in [offset, offset+length) */
        cairn_punch_hole(thread, shared, inode, request->allocate.offset,
                         request->allocate.length);
    } else {
        /* ALLOCATE: extend file size if needed */
        uint64_t new_end = request->allocate.offset + request->allocate.length;

        if (new_end > inode->size) {
            inode->size = new_end;
        }
    }

    inode->mtime = now;
    inode->ctime = now;
    inode->change++;

    cairn_map_attrs(shared, &request->allocate.r_post_attr, inode);

    cairn_put_inode(thread, inode);
    cairn_inode_handle_release(&ih);

    request->status = CHIMERA_VFS_OK;

    cairn_queue_request(thread, request);
} /* cairn_allocate */

static void
cairn_seek(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    rocksdb_iterator_t       *iter;
    struct cairn_inode_handle ih;
    struct cairn_inode       *inode;
    struct cairn_extent_key   start_key, *extent_key;
    uint64_t                  offset = request->seek.offset;
    int                       rc;
    size_t                    klen;

    rc = cairn_inode_get_fh(thread, request->fh, request->fh_len, &ih);

    if (rc) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;

    if (offset >= inode->size) {
        /* No data or hole at or beyond EOF: SEEK must fail with NXIO
         * (POSIX lseek ENXIO / RFC 7862 NFS4ERR_NXIO). */
        cairn_inode_handle_release(&ih);
        request->status = CHIMERA_VFS_ENXIO;
        request->complete(request);
        return;
    }

    start_key.keytype = CAIRN_KEY_EXTENT;
    start_key.inum    = inode->inum;
    start_key.offset  = htobe64(offset);

    iter = cairn_data_iterator(thread);

    if (request->seek.what == 0) {
        /* SEEK_DATA: find first extent covering or after offset */
        rocksdb_iter_seek_for_prev(iter, (const char *) &start_key,
                                   sizeof(start_key));

        if (!rocksdb_iter_valid(iter)) {
            start_key.offset = htobe64(0);
            rocksdb_iter_seek(iter, (const char *) &start_key,
                              sizeof(start_key));
        } else {
            extent_key = (struct cairn_extent_key *)
                rocksdb_iter_key(iter, &klen);

            if (extent_key->keytype != CAIRN_KEY_EXTENT ||
                extent_key->inum != inode->inum) {
                start_key.offset = htobe64(0);
                rocksdb_iter_seek(iter, (const char *) &start_key,
                                  sizeof(start_key));
            }
        }

        while (rocksdb_iter_valid(iter)) {
            size_t   vlen;
            uint64_t extent_length;

            extent_key = (struct cairn_extent_key *)
                rocksdb_iter_key(iter, &klen);

            if (extent_key->keytype != CAIRN_KEY_EXTENT ||
                extent_key->inum != inode->inum) {
                break;
            }

            uint64_t extent_start = be64toh(extent_key->offset);

            rocksdb_iter_value(iter, &vlen);
            extent_length = vlen;

            uint64_t extent_end = extent_start + extent_length;

            if (extent_end > offset) {
                /* This extent covers or is after our offset */
                request->seek.r_offset = (extent_start > offset) ?
                    extent_start : offset;
                request->seek.r_eof = 0;
                rocksdb_iter_destroy(iter);
                cairn_inode_handle_release(&ih);
                request->status = CHIMERA_VFS_OK;
                request->complete(request);
                return;
            }

            rocksdb_iter_next(iter);
        }

        /* No data at or beyond the offset: SEEK_DATA fails with NXIO
         * (the trailing region is an implicit hole to EOF). */
        rocksdb_iter_destroy(iter);
        cairn_inode_handle_release(&ih);
        request->status = CHIMERA_VFS_ENXIO;
        request->complete(request);
        return;
    } else {
        /* SEEK_HOLE: find first gap from offset forward */
        uint64_t current_pos = offset;

        rocksdb_iter_seek_for_prev(iter, (const char *) &start_key,
                                   sizeof(start_key));

        if (!rocksdb_iter_valid(iter)) {
            start_key.offset = htobe64(0);
            rocksdb_iter_seek(iter, (const char *) &start_key,
                              sizeof(start_key));
        } else {
            extent_key = (struct cairn_extent_key *)
                rocksdb_iter_key(iter, &klen);

            if (extent_key->keytype != CAIRN_KEY_EXTENT ||
                extent_key->inum != inode->inum) {
                start_key.offset = htobe64(0);
                rocksdb_iter_seek(iter, (const char *) &start_key,
                                  sizeof(start_key));
            }
        }

        while (rocksdb_iter_valid(iter)) {
            size_t   vlen;
            uint64_t extent_length;

            extent_key = (struct cairn_extent_key *)
                rocksdb_iter_key(iter, &klen);

            if (extent_key->keytype != CAIRN_KEY_EXTENT ||
                extent_key->inum != inode->inum) {
                break;
            }

            uint64_t extent_start = be64toh(extent_key->offset);

            rocksdb_iter_value(iter, &vlen);
            extent_length = vlen;

            uint64_t extent_end = extent_start + extent_length;

            /* Skip extents entirely before current_pos */
            if (extent_end <= current_pos) {
                rocksdb_iter_next(iter);
                continue;
            }

            /* If there's a gap before this extent, that's a hole */
            if (extent_start > current_pos) {
                request->seek.r_offset = current_pos;
                request->seek.r_eof    = 0;
                rocksdb_iter_destroy(iter);
                cairn_inode_handle_release(&ih);
                request->status = CHIMERA_VFS_OK;
                request->complete(request);
                return;
            }

            /* This extent covers current_pos, advance past it */
            current_pos = extent_end;
            rocksdb_iter_next(iter);
        }

        /* Virtual hole at or after all extents */
        if (current_pos < inode->size) {
            request->seek.r_offset = current_pos;
        } else {
            request->seek.r_offset = inode->size;
        }

        /* The match is the implicit hole at EOF only once the returned offset
         * reaches the logical size; RFC 7862 §11.4.4 requires sr_eof TRUE there
         * (surfaced by the Linux client to lseek).  A gap that begins before
         * the size is a real hole short of EOF, so flag eof on size reached. */
        request->seek.r_eof = (request->seek.r_offset >= inode->size);
        rocksdb_iter_destroy(iter);
        cairn_inode_handle_release(&ih);
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }
} /* cairn_seek */

static void
cairn_symlink_at(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle  parent_ih;
    struct cairn_dirent_handle dh;
    struct cairn_inode        *parent_inode, new_inode;
    struct cairn_dirent_key    dirent_key;
    struct cairn_dirent_value  dirent_value;
    struct cairn_symlink_key   target_key;
    int                        rc;
    struct timespec            now;

    clock_gettime(CLOCK_REALTIME, &now);
    rc = cairn_inode_get_fh(thread, request->fh, request->fh_len, &parent_ih);

    if (unlikely(rc)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    parent_inode = parent_ih.inode;

    if (!S_ISDIR(parent_inode->mode)) {
        cairn_inode_handle_release(&parent_ih);
        request->status = CHIMERA_VFS_ENOTDIR;
        request->complete(request);
        return;
    }

    cairn_map_attrs(shared, &request->symlink_at.r_dir_pre_attr, parent_inode);

    dirent_key.keytype = CAIRN_KEY_DIRENT;
    dirent_key.inum    = parent_inode->inum;
    dirent_key.hash    = request->symlink_at.name_hash;

    rc = cairn_dirent_get(thread, &dirent_key, &dh);

    if (rc == 0) {
        cairn_inode_handle_release(&parent_ih);
        cairn_dirent_handle_release(&dh);
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        return;
    }

    cairn_alloc_inum(thread, &new_inode);
    new_inode.size       = request->symlink_at.targetlen;
    new_inode.space_used = request->symlink_at.targetlen;
    new_inode.uid        = request->cred->uid;
    new_inode.gid        = request->cred->gid;
    new_inode.nlink      = 1;
    new_inode.rdev       = 0;
    new_inode.mode       = S_IFLNK | 0755;
    new_inode.atime      = now;
    new_inode.mtime      = now;
    new_inode.ctime      = now;
    new_inode.change++;
    new_inode.btime          = now;
    new_inode.dos_attributes = 0;

    dirent_value.inum     = new_inode.inum;
    dirent_value.name_len = request->symlink_at.namelen;
    memcpy(dirent_value.name, request->symlink_at.name, request->symlink_at.namelen);

    parent_inode->mtime = now;
    parent_inode->ctime = now;
    parent_inode->change++;

    cairn_map_attrs(shared, &request->symlink_at.r_attr, &new_inode);
    cairn_map_attrs(shared, &request->symlink_at.r_dir_post_attr, parent_inode);

    target_key.keytype = CAIRN_KEY_SYMLINK;
    target_key.inum    = new_inode.inum;
    {
        char *symlink_err = NULL;
        rocksdb_transaction_put(cairn_get_meta_txn(thread),
                                (const char *) &target_key, sizeof(target_key),
                                request->symlink_at.target,
                                request->symlink_at.targetlen, &symlink_err);
        chimera_cairn_abort_if(symlink_err,
                               "Error putting symlink target: %s\n", symlink_err);
    }

    cairn_put_dirent(thread, &dirent_key, &dirent_value);
    cairn_put_inode(thread, parent_inode);
    cairn_put_inode(thread, &new_inode);

    cairn_inode_handle_release(&parent_ih);

    request->status = CHIMERA_VFS_OK;

    cairn_queue_request(thread, request);
} /* cairn_symlink_at */

static void
cairn_readlink(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle ih;
    struct cairn_inode       *inode;
    struct cairn_symlink_key  target_key;
    char                     *err = NULL;
    size_t                    target_len;
    int                       rc;

    rc = cairn_inode_get_fh(thread, request->fh, request->fh_len, &ih);

    if (rc) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;

    if (!S_ISLNK(inode->mode)) {
        cairn_inode_handle_release(&ih);
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    target_key.keytype = CAIRN_KEY_SYMLINK;
    target_key.inum    = inode->inum;

    {
        rocksdb_pinnableslice_t *slice;
        const char              *target_data;

        slice = cairn_meta_get_pinned(thread, &target_key, sizeof(target_key), &err);

        chimera_cairn_abort_if(err, "Error getting symlink target: %s\n", err);

        if (!slice) {
            cairn_inode_handle_release(&ih);
            request->status = CHIMERA_VFS_EINVAL;
            request->complete(request);
            return;
        }

        target_data = rocksdb_pinnableslice_value(slice, &target_len);

        request->readlink.r_target_length = target_len;
        memcpy(request->readlink.r_target, target_data, target_len);

        rocksdb_pinnableslice_destroy(slice);
    }

    cairn_map_attrs(shared, &request->readlink.r_attr, inode);

    cairn_inode_handle_release(&ih);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* cairn_readlink */

static inline int
cairn_fh_compare(
    const void *fha,
    int         fha_len,
    const void *fhb,
    int         fhb_len)
{
    int minlen = fha_len < fhb_len ? fha_len : fhb_len;

    return memcmp(fha, fhb, minlen);
} /* cairn_fh_compare */

static void
cairn_rename_at(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle  old_parent_ih, new_parent_ih = { 0 };
    struct cairn_inode        *old_parent_inode, *new_parent_inode;
    struct cairn_dirent_key    old_dirent_key, new_dirent_key;
    struct cairn_dirent_handle old_dh, new_dh;
    struct cairn_dirent_value *old_dirent_value;
    struct cairn_dirent_value  new_dirent_value;
    struct cairn_inode_handle  target_ih;
    struct cairn_inode        *target_inode;
    int                        cmp, rc, have_new_parent_ih = 0;
    struct timespec            now;

    /*
     * TODO(phase-A.2): cross-thread races.  When old_parent and new_parent
     * (or the moved / overwritten inode) live on different delegation
     * threads from each other or from a concurrent single-inode op, the
     * per-thread WriteBatchWithIndex isolates each thread's pending
     * mutations until commit, so one thread can overwrite another's
     * in-flight changes.  TransactionDB's row locks previously covered
     * this; replacing them needs either routing-based delegation of the
     * secondary inode writes back to the secondary's home thread, or a
     * hold-through-commit lock protocol that also covers the deferred
     * commit window.  Neither is in this change; for now we rely on the
     * fh-hash routing for correctness and document the gap.
     */
    (void) shared;

    clock_gettime(CLOCK_REALTIME, &now);
    cmp = cairn_fh_compare(request->fh,
                           request->fh_len,
                           request->rename_at.new_fh,
                           request->rename_at.new_fhlen);

    if (cmp == 0) {
        rc = cairn_inode_get_fh(thread, request->fh, request->fh_len, &old_parent_ih);

        if (unlikely(rc)) {
            request->status = CHIMERA_VFS_ENOENT;
            request->complete(request);
            return;
        }

        old_parent_inode = old_parent_ih.inode;
        new_parent_inode = old_parent_inode;

        if (!S_ISDIR(old_parent_inode->mode)) {
            cairn_inode_handle_release(&old_parent_ih);
            request->status = CHIMERA_VFS_ENOTDIR;
            request->complete(request);
            return;
        }
    } else {

        have_new_parent_ih = 1;

        if (cmp < 0) {
            rc = cairn_inode_get_fh(thread, request->fh, request->fh_len, &old_parent_ih);
            if (rc) {
                request->status = CHIMERA_VFS_ENOENT;
                request->complete(request);
                return;
            }

            rc = cairn_inode_get_fh(thread, request->rename_at.new_fh, request->rename_at.new_fhlen, &
                                    new_parent_ih);
            if (rc) {
                cairn_inode_handle_release(&old_parent_ih);
                request->status = CHIMERA_VFS_ENOENT;
                request->complete(request);
                return;
            }
        } else {
            rc = cairn_inode_get_fh(thread, request->rename_at.new_fh, request->rename_at.new_fhlen, &
                                    new_parent_ih);
            if (rc) {
                request->status = CHIMERA_VFS_ENOENT;
                request->complete(request);
                return;
            }

            rc = cairn_inode_get_fh(thread, request->fh, request->fh_len, &old_parent_ih);
            if (rc) {
                cairn_inode_handle_release(&new_parent_ih);
                request->status = CHIMERA_VFS_ENOENT;
                request->complete(request);
                return;
            }
        }

        old_parent_inode = old_parent_ih.inode;
        new_parent_inode = new_parent_ih.inode;

        if (!S_ISDIR(old_parent_inode->mode)) {
            cairn_inode_handle_release(&old_parent_ih);
            cairn_inode_handle_release(&new_parent_ih);
            request->status = CHIMERA_VFS_ENOTDIR;
            request->complete(request);
            return;
        }

        if (!S_ISDIR(new_parent_inode->mode)) {
            cairn_inode_handle_release(&old_parent_ih);
            cairn_inode_handle_release(&new_parent_ih);
            request->status = CHIMERA_VFS_ENOTDIR;
            request->complete(request);
            return;
        }
    }

    old_dirent_key.keytype = CAIRN_KEY_DIRENT;
    old_dirent_key.inum    = old_parent_inode->inum;
    old_dirent_key.hash    = request->rename_at.name_hash;

    rc = cairn_dirent_get(thread, &old_dirent_key, &old_dh);
    if (rc) {
        cairn_inode_handle_release(&old_parent_ih);

        if (have_new_parent_ih) {
            cairn_inode_handle_release(&new_parent_ih);
        }
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    old_dirent_value = old_dh.dirent;

    rc = cairn_inode_get_inum(thread, old_dirent_value->inum, &target_ih);
    if (rc) {
        cairn_dirent_handle_release(&old_dh);
        cairn_inode_handle_release(&old_parent_ih);
        if (have_new_parent_ih) {
            cairn_inode_handle_release(&new_parent_ih);
        }
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    target_inode = target_ih.inode;

    /* POSIX: a directory may not be renamed into itself or one of its own
     * descendants (EINVAL).  Walk the destination parent's ancestry; if the
     * moved directory appears, reject.  (parent_inum is read out of each handle
     * before it is released -- the inode lives in a pinned slice.) */
    if (S_ISDIR(target_inode->mode)) {
        uint64_t walk        = new_parent_inode->inum;
        uint64_t walk_parent = new_parent_inode->parent_inum;
        int      bad         = 0;

        for (int depth = 0; depth < CHIMERA_VFS_PATH_MAX; depth++) {
            if (walk == target_inode->inum) {
                bad = 1;
                break;
            }
            if (walk_parent == walk) {
                break;  /* reached the root (parent of root is itself) */
            }
            struct cairn_inode_handle anc_ih;
            if (cairn_inode_get_inum(thread, walk_parent, &anc_ih)) {
                break;
            }
            walk        = walk_parent;
            walk_parent = anc_ih.inode->parent_inum;
            cairn_inode_handle_release(&anc_ih);
        }

        if (bad) {
            cairn_dirent_handle_release(&old_dh);
            cairn_inode_handle_release(&target_ih);
            cairn_inode_handle_release(&old_parent_ih);
            if (have_new_parent_ih) {
                cairn_inode_handle_release(&new_parent_ih);
            }
            request->status = CHIMERA_VFS_EINVAL;
            request->complete(request);
            return;
        }
    }

    new_dirent_key.keytype = CAIRN_KEY_DIRENT;
    new_dirent_key.inum    = new_parent_inode->inum;
    new_dirent_key.hash    = request->rename_at.new_name_hash;

    rc = cairn_dirent_get(thread, &new_dirent_key, &new_dh);
    if (rc == 0) {
        // Target exists
        // Per POSIX: if old and new refer to same file, return success with no action
        if (new_dh.dirent->inum == old_dirent_value->inum) {
            cairn_dirent_handle_release(&old_dh);
            cairn_dirent_handle_release(&new_dh);
            cairn_inode_handle_release(&target_ih);
            cairn_inode_handle_release(&old_parent_ih);
            if (have_new_parent_ih) {
                cairn_inode_handle_release(&new_parent_ih);
            }
            request->status = CHIMERA_VFS_OK;
            request->complete(request);
            return;
        }

        // Target is different inode - need to remove it
        struct cairn_inode_handle existing_ih;
        struct cairn_inode       *existing_inode;

        rc = cairn_inode_get_inum(thread, new_dh.dirent->inum, &existing_ih);
        if (rc == 0) {
            existing_inode = existing_ih.inode;

            if (S_ISDIR(target_inode->mode) != S_ISDIR(existing_inode->mode)) {
                int status = S_ISDIR(existing_inode->mode)
                             ? CHIMERA_VFS_EISDIR
                             : CHIMERA_VFS_ENOTDIR;

                cairn_inode_handle_release(&existing_ih);
                cairn_dirent_handle_release(&new_dh);
                cairn_dirent_handle_release(&old_dh);
                cairn_inode_handle_release(&target_ih);
                cairn_inode_handle_release(&old_parent_ih);
                if (have_new_parent_ih) {
                    cairn_inode_handle_release(&new_parent_ih);
                }
                request->status = status;
                request->complete(request);
                return;
            }

            if (S_ISDIR(existing_inode->mode)) {
                if (!cairn_directory_is_empty(thread, existing_inode->inum)) {
                    cairn_inode_handle_release(&existing_ih);
                    cairn_dirent_handle_release(&new_dh);
                    cairn_dirent_handle_release(&old_dh);
                    cairn_inode_handle_release(&target_ih);
                    cairn_inode_handle_release(&old_parent_ih);
                    if (have_new_parent_ih) {
                        cairn_inode_handle_release(&new_parent_ih);
                    }
                    request->status = CHIMERA_VFS_ENOTEMPTY;
                    request->complete(request);
                    return;
                }

                existing_inode->nlink = 0;
                new_parent_inode->nlink--;
            } else {
                existing_inode->nlink--;
            }

            if (existing_inode->nlink == 0) {
                existing_inode->refcnt--;

                if (existing_inode->refcnt == 0) {
                    // Remove type-specific data before removing inode
                    if (S_ISREG(existing_inode->mode)) {
                        cairn_remove_file_extents(thread, existing_inode->inum);
                    } else if (S_ISLNK(existing_inode->mode)) {
                        cairn_remove_symlink_target(thread, existing_inode->inum);
                    }

                    cairn_remove_inode(thread, existing_inode);
                    cairn_remove_acl(thread, existing_inode->inum);
                } else {
                    cairn_put_inode(thread, existing_inode);
                }
            } else {
                cairn_put_inode(thread, existing_inode);
            }

            cairn_inode_handle_release(&existing_ih);
        }
        cairn_dirent_handle_release(&new_dh);
    }

    target_inode->ctime = now;
    target_inode->change++;
    if (cmp != 0 && S_ISDIR(target_inode->mode)) {
        target_inode->parent_inum = new_parent_inode->inum;
    }
    cairn_put_inode(thread, target_inode);

    // Create new dirent
    new_dirent_value.inum     = old_dirent_value->inum;
    new_dirent_value.name_len = request->rename_at.new_namelen;
    memcpy(new_dirent_value.name, request->rename_at.new_name, request->rename_at.new_namelen);

    // Update directory entries and parent inodes
    cairn_remove_dirent(thread, &old_dirent_key);
    cairn_put_dirent(thread, &new_dirent_key, &new_dirent_value);

    old_parent_inode->mtime = now;
    old_parent_inode->ctime = now;
    old_parent_inode->change++;
    new_parent_inode->mtime = now;
    new_parent_inode->ctime = now;
    new_parent_inode->change++;

    if (cmp != 0 && S_ISDIR(target_inode->mode)) {
        old_parent_inode->nlink--;
        new_parent_inode->nlink++;
    }

    cairn_put_inode(thread, old_parent_inode);
    if (cmp != 0) {
        cairn_put_inode(thread, new_parent_inode);
    }

    // Cleanup
    cairn_dirent_handle_release(&old_dh);
    cairn_inode_handle_release(&target_ih);

    cairn_inode_handle_release(&old_parent_ih);

    if (have_new_parent_ih) {
        cairn_inode_handle_release(&new_parent_ih);
    }

    request->status = CHIMERA_VFS_OK;
    cairn_queue_request(thread, request);
} /* cairn_rename_at */

static void
cairn_link_at(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle  parent_ih, target_ih;
    struct cairn_inode        *parent_inode, *target_inode;
    struct cairn_dirent_key    dirent_key;
    struct cairn_dirent_value  dirent_value;
    struct cairn_dirent_handle dh;
    int                        rc;
    struct timespec            now;

    clock_gettime(CLOCK_REALTIME, &now);

    rc = cairn_inode_get_fh(thread, request->link_at.dir_fh, request->link_at.dir_fhlen, &parent_ih);

    if (unlikely(rc)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    parent_inode = parent_ih.inode;

    if (!S_ISDIR(parent_inode->mode)) {
        cairn_inode_handle_release(&parent_ih);
        request->status = CHIMERA_VFS_ENOTDIR;
        request->complete(request);
        return;
    }

    rc = cairn_inode_get_fh(thread, request->fh, request->fh_len, &target_ih);

    if (rc) {
        cairn_inode_handle_release(&parent_ih);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    target_inode = target_ih.inode;

    if (S_ISDIR(target_inode->mode)) {
        cairn_inode_handle_release(&parent_ih);
        cairn_inode_handle_release(&target_ih);
        request->status = CHIMERA_VFS_EISDIR;
        request->complete(request);
        return;
    }

    dirent_key.keytype = CAIRN_KEY_DIRENT;
    dirent_key.inum    = parent_inode->inum;
    dirent_key.hash    = request->link_at.name_hash;

    rc = cairn_dirent_get(thread, &dirent_key, &dh);

    if (rc == 0) {
        cairn_inode_handle_release(&parent_ih);
        cairn_inode_handle_release(&target_ih);
        cairn_dirent_handle_release(&dh);
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        return;
    }

    dirent_value.inum     = target_inode->inum;
    dirent_value.name_len = request->link_at.namelen;
    memcpy(dirent_value.name, request->link_at.name, request->link_at.namelen);

    target_inode->nlink++;
    target_inode->ctime = now;
    target_inode->change++;
    parent_inode->mtime = now;
    parent_inode->ctime = now;
    parent_inode->change++;

    cairn_put_dirent(thread, &dirent_key, &dirent_value);
    cairn_put_inode(thread, parent_inode);
    cairn_put_inode(thread, target_inode);

    cairn_inode_handle_release(&parent_ih);
    cairn_inode_handle_release(&target_ih);

    request->status = CHIMERA_VFS_OK;

    cairn_queue_request(thread, request);
} /* cairn_link_at */


/*
 * NFS COMMIT semantics: "make all my UNSTABLE writes up to this point
 * durable."  In the two-DB model, UNSTABLE writes appended to datadb's
 * WAL with sync=0 and committed metadata to metadb with sync=1 (metadata
 * is always sync).  The metadata fsync only flushes metadb's WAL — it
 * does not touch datadb's WAL fd, so unsynced extent bytes in datadb's
 * OS page cache survive only by luck on power loss.
 *
 * Mark needs_data_wal_flush so cairn_thread_commit issues an explicit
 * rocksdb_flush_wal(datadb, sync=1) after the current cycle.  Also set
 * data_needs_sync so any pending data writes in this same cycle commit
 * with sync=true (which would already fsync the WAL on its own; the
 * separate flush_wal handles the case where this cycle has no data
 * writes of its own).
 */
static void
cairn_commit_op(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    (void) shared;
    (void) private_data;
    thread->data_needs_sync      = 1;
    thread->needs_data_wal_flush = 1;
    request->status              = CHIMERA_VFS_OK;
    cairn_queue_request(thread, request);
} /* cairn_commit_op */

static void
cairn_put_key(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    rocksdb_transaction_t *txn;
    char                  *err = NULL;
    uint8_t                kv_key[1 + CAIRN_KV_KEY_MAX];
    size_t                 kv_key_len;

    (void) shared;
    (void) private_data;

    if (request->put_key.key_len > CAIRN_KV_KEY_MAX) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    /* Build RocksDB key: keytype + user key */
    kv_key[0]  = CAIRN_KEY_KV;
    kv_key_len = 1 + request->put_key.key_len;
    memcpy(kv_key + 1, request->put_key.key, request->put_key.key_len);

    txn = cairn_get_meta_txn(thread);
    rocksdb_transaction_put(txn,
                            (const char *) kv_key, kv_key_len,
                            (const char *) request->put_key.value,
                            request->put_key.value_len, &err);
    chimera_cairn_abort_if(err, "Error putting KV: %s\n", err);

    request->status = CHIMERA_VFS_OK;

    cairn_queue_request(thread, request);
} /* cairn_put_key */

static void
cairn_get_key(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    char   *err = NULL;
    uint8_t kv_key[1 + CAIRN_KV_KEY_MAX];
    size_t  kv_key_len;
    size_t  value_len;

    (void) private_data;

    if (request->get_key.key_len > CAIRN_KV_KEY_MAX) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    /* Build RocksDB key: keytype + user key */
    kv_key[0]  = CAIRN_KEY_KV;
    kv_key_len = 1 + request->get_key.key_len;
    memcpy(kv_key + 1, request->get_key.key, request->get_key.key_len);

    {
        rocksdb_pinnableslice_t *slice;
        const char              *vp;

        slice = cairn_meta_get_pinned(thread, kv_key, kv_key_len, &err);
        chimera_cairn_abort_if(err, "Error getting KV: %s\n", err);

        if (!slice) {
            request->status = CHIMERA_VFS_ENOENT;
            request->complete(request);
            return;
        }

        vp = rocksdb_pinnableslice_value(slice, &value_len);
        memcpy(request->plugin_data, vp, value_len);
        rocksdb_pinnableslice_destroy(slice);
    }

    request->get_key.r_value     = request->plugin_data;
    request->get_key.r_value_len = value_len;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* cairn_get_key */

static void
cairn_delete_key(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    rocksdb_transaction_t *txn;
    char                  *err = NULL;
    uint8_t                kv_key[1 + CAIRN_KV_KEY_MAX];
    size_t                 kv_key_len;

    (void) shared;
    (void) private_data;

    if (request->delete_key.key_len > CAIRN_KV_KEY_MAX) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    /* Build RocksDB key: keytype + user key */
    kv_key[0]  = CAIRN_KEY_KV;
    kv_key_len = 1 + request->delete_key.key_len;
    memcpy(kv_key + 1, request->delete_key.key, request->delete_key.key_len);

    txn = cairn_get_meta_txn(thread);
    rocksdb_transaction_delete(txn, (const char *) kv_key, kv_key_len, &err);
    chimera_cairn_abort_if(err, "Error deleting KV: %s\n", err);

    request->status = CHIMERA_VFS_OK;

    cairn_queue_request(thread, request);
} /* cairn_delete_key */

static void
cairn_search_keys(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    rocksdb_iterator_t                *iter;
    uint8_t                            start_kv_key[1 + CAIRN_KV_KEY_MAX];
    uint8_t                            end_kv_key[1 + CAIRN_KV_KEY_MAX];
    size_t                             start_kv_key_len, end_kv_key_len;
    const char                        *key, *value;
    size_t                             key_len, value_len;
    int                                rc;
    chimera_vfs_search_keys_callback_t callback = request->search_keys.callback;
    uint32_t                           flags    = request->search_keys.flags;

    if (request->search_keys.start_key_len > CAIRN_KV_KEY_MAX ||
        request->search_keys.end_key_len > CAIRN_KV_KEY_MAX) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    /* Build start key: keytype + user start key */
    start_kv_key[0]  = CAIRN_KEY_KV;
    start_kv_key_len = 1 + request->search_keys.start_key_len;
    if (request->search_keys.start_key_len > 0) {
        memcpy(start_kv_key + 1, request->search_keys.start_key,
               request->search_keys.start_key_len);
    }

    /* Build end key: keytype + user end key */
    end_kv_key[0]  = CAIRN_KEY_KV;
    end_kv_key_len = 1 + request->search_keys.end_key_len;
    if (request->search_keys.end_key_len > 0) {
        memcpy(end_kv_key + 1, request->search_keys.end_key,
               request->search_keys.end_key_len);
    }

    iter = cairn_meta_iterator(thread);

    rocksdb_iter_seek(iter, (const char *) start_kv_key, start_kv_key_len);

    while (rocksdb_iter_valid(iter)) {
        key   = rocksdb_iter_key(iter, &key_len);
        value = rocksdb_iter_value(iter, &value_len);

        /* Check if we're still in KV keyspace */
        if (key_len < 1 || (uint8_t) key[0] != CAIRN_KEY_KV) {
            break;
        }

        /* Check if key is past end key (if end key specified).  Compare
         * lexicographically: the shared prefix is compared first and length
         * only breaks an exact tie, so a key that shares end_kv_key's bytes but
         * is LONGER sorts after it, while a longer key that differs within the
         * shared prefix can still sort before it (the bug the old length-first
         * check had).  With END_EXCLUSIVE a key byte-equal to end_kv_key is
         * itself past the range. */
        if (request->search_keys.end_key_len > 0) {
            size_t cmplen = (key_len < end_kv_key_len) ? key_len : end_kv_key_len;
            int    c      = memcmp(key, end_kv_key, cmplen);

            if (c == 0) {
                c = (key_len < end_kv_key_len) ? -1 :
                    (key_len > end_kv_key_len) ? 1 : 0;
            }

            if ((flags & CHIMERA_VFS_SEARCH_KEYS_END_EXCLUSIVE) ? (c >= 0) : (c > 0)) {
                break;
            }
        }

        /* Callback with user key (skip keytype byte) */
        rc = callback(key + 1, key_len - 1,
                      value, value_len,
                      request->proto_private_data);

        if (rc) {
            /* Caller wants to abort search */
            break;
        }

        rocksdb_iter_next(iter);
    }

    rocksdb_iter_destroy(iter);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* cairn_search_keys */

static inline int
cairn_xattr_value_matches(
    const struct cairn_xattr_value *xv,
    size_t                          xv_len,
    const char                     *name,
    uint32_t                        name_len)
{
    return xv_len >= sizeof(*xv) &&
           xv->name_len == name_len &&
           xv_len >= sizeof(*xv) + xv->name_len + xv->value_len &&
           memcmp(xv->data, name, name_len) == 0;
} /* cairn_xattr_value_matches */

static void
cairn_get_xattr(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle ih;
    struct cairn_xattr_key    key;
    rocksdb_pinnableslice_t  *slice;
    const char               *vp;
    size_t                    vlen;
    char                     *err = NULL;

    (void) shared;
    (void) private_data;

    if (cairn_inode_get_fh(thread, request->fh, request->fh_len, &ih) != 0) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    key.keytype = CAIRN_KEY_XATTR;
    key.inum    = ih.inode->inum;
    key.hash    = chimera_vfs_hash(request->get_xattr.name,
                                   request->get_xattr.namelen);
    cairn_inode_handle_release(&ih);

    slice = cairn_meta_get_pinned(thread, &key, sizeof(key), &err);
    chimera_cairn_abort_if(err, "Error getting xattr: %s\n", err);

    if (!slice) {
        request->status = CHIMERA_VFS_ENODATA;
        request->complete(request);
        return;
    }

    vp = rocksdb_pinnableslice_value(slice, &vlen);
    if (!cairn_xattr_value_matches((const struct cairn_xattr_value *) vp, vlen,
                                   request->get_xattr.name,
                                   request->get_xattr.namelen)) {
        rocksdb_pinnableslice_destroy(slice);
        request->status = CHIMERA_VFS_ENODATA;
        request->complete(request);
        return;
    }

    {
        const struct cairn_xattr_value *xv = (const struct cairn_xattr_value *) vp;
        if (xv->value_len > request->get_xattr.value_maxlen) {
            rocksdb_pinnableslice_destroy(slice);
            request->status = CHIMERA_VFS_ERANGE;
            request->complete(request);
            return;
        }
        memcpy(request->get_xattr.value, xv->data + xv->name_len,
               xv->value_len);
        request->get_xattr.r_value_len = xv->value_len;
    }

    rocksdb_pinnableslice_destroy(slice);
    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* cairn_get_xattr */

static void
cairn_set_xattr(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle ih;
    struct cairn_inode       *inode;
    struct cairn_xattr_key    key;
    rocksdb_pinnableslice_t  *slice;
    rocksdb_transaction_t    *txn;
    struct cairn_xattr_value *xv;
    uint32_t                  xv_len;
    char                     *err = NULL;
    const char               *vp;
    size_t                    vlen;

    (void) private_data;

    if (cairn_inode_get_fh(thread, request->fh, request->fh_len, &ih) != 0) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;
    cairn_map_attrs(shared, &request->set_xattr.r_pre_attr, inode);

    key.keytype = CAIRN_KEY_XATTR;
    key.inum    = inode->inum;
    key.hash    = chimera_vfs_hash(request->set_xattr.name,
                                   request->set_xattr.namelen);

    slice = cairn_meta_get_pinned(thread, &key, sizeof(key), &err);
    chimera_cairn_abort_if(err, "Error getting xattr: %s\n", err);

    if (slice) {
        vp = rocksdb_pinnableslice_value(slice, &vlen);
        if (!cairn_xattr_value_matches((const struct cairn_xattr_value *) vp, vlen,
                                       request->set_xattr.name,
                                       request->set_xattr.namelen)) {
            rocksdb_pinnableslice_destroy(slice);
            cairn_inode_handle_release(&ih);
            request->status = CHIMERA_VFS_EEXIST;
            cairn_queue_request(thread, request);
            return;
        }
        rocksdb_pinnableslice_destroy(slice);
        if (request->set_xattr.option == CHIMERA_VFS_XATTR_CREATE) {
            cairn_inode_handle_release(&ih);
            request->status = CHIMERA_VFS_EEXIST;
            cairn_queue_request(thread, request);
            return;
        }
    } else if (request->set_xattr.option == CHIMERA_VFS_XATTR_REPLACE) {
        cairn_inode_handle_release(&ih);
        request->status = CHIMERA_VFS_ENODATA;
        cairn_queue_request(thread, request);
        return;
    }

    xv_len = sizeof(*xv) + request->set_xattr.namelen +
        request->set_xattr.value_len;
    xv            = malloc(xv_len);
    xv->name_len  = request->set_xattr.namelen;
    xv->value_len = request->set_xattr.value_len;
    memcpy(xv->data, request->set_xattr.name, request->set_xattr.namelen);
    memcpy(xv->data + request->set_xattr.namelen,
           request->set_xattr.value, request->set_xattr.value_len);

    txn = cairn_get_meta_txn(thread);
    rocksdb_transaction_put(txn, (const char *) &key, sizeof(key),
                            (const char *) xv, xv_len, &err);
    free(xv);
    chimera_cairn_abort_if(err, "Error putting xattr: %s\n", err);

    clock_gettime(CLOCK_REALTIME, &inode->ctime);
    cairn_put_inode(thread, inode);
    cairn_map_attrs(shared, &request->set_xattr.r_post_attr, inode);

    cairn_inode_handle_release(&ih);
    request->status = CHIMERA_VFS_OK;
    cairn_queue_request(thread, request);
} /* cairn_set_xattr */

static void
cairn_list_xattrs(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle ih;
    struct cairn_xattr_key    start_key, *key;
    rocksdb_iterator_t       *iter;
    const char               *value;
    size_t                    klen, vlen;
    uint8_t                  *buf = request->list_xattrs.buffer;
    uint32_t                  offset = 0, count = 0;

    (void) shared;
    (void) private_data;

    if (cairn_inode_get_fh(thread, request->fh, request->fh_len, &ih) != 0) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    start_key.keytype = CAIRN_KEY_XATTR;
    start_key.inum    = ih.inode->inum;
    start_key.hash    = 0;
    cairn_inode_handle_release(&ih);

    iter = cairn_meta_iterator(thread);
    rocksdb_iter_seek(iter, (const char *) &start_key, sizeof(start_key));

    while (rocksdb_iter_valid(iter)) {
        key = (struct cairn_xattr_key *) rocksdb_iter_key(iter, &klen);
        if (klen != sizeof(*key) ||
            key->keytype != CAIRN_KEY_XATTR ||
            key->inum != start_key.inum) {
            break;
        }

        value = rocksdb_iter_value(iter, &vlen);
        if (vlen < sizeof(struct cairn_xattr_value)) {
            rocksdb_iter_destroy(iter);
            request->status = CHIMERA_VFS_EIO;
            request->complete(request);
            return;
        }
        {
            const struct cairn_xattr_value *xv =
                (const struct cairn_xattr_value *) value;
            if (vlen < sizeof(*xv) + xv->name_len + xv->value_len) {
                rocksdb_iter_destroy(iter);
                request->status = CHIMERA_VFS_EIO;
                request->complete(request);
                return;
            }
            if (offset + xv->name_len + 1 > request->list_xattrs.max_bytes) {
                rocksdb_iter_destroy(iter);
                request->status = CHIMERA_VFS_ERANGE;
                request->complete(request);
                return;
            }
            memcpy(buf + offset, xv->data, xv->name_len);
            offset       += xv->name_len;
            buf[offset++] = '\0';
            count++;
        }

        rocksdb_iter_next(iter);
    }

    rocksdb_iter_destroy(iter);
    request->list_xattrs.r_len    = offset;
    request->list_xattrs.r_count  = count;
    request->list_xattrs.r_eof    = 1;
    request->list_xattrs.r_cookie = 0;
    request->status               = CHIMERA_VFS_OK;
    request->complete(request);
} /* cairn_list_xattrs */

static void
cairn_remove_xattr(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle ih;
    struct cairn_inode       *inode;
    struct cairn_xattr_key    key;
    rocksdb_pinnableslice_t  *slice;
    rocksdb_transaction_t    *txn;
    char                     *err = NULL;
    const char               *vp;
    size_t                    vlen;

    (void) private_data;

    if (cairn_inode_get_fh(thread, request->fh, request->fh_len, &ih) != 0) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;
    cairn_map_attrs(shared, &request->remove_xattr.r_pre_attr, inode);

    key.keytype = CAIRN_KEY_XATTR;
    key.inum    = inode->inum;
    key.hash    = chimera_vfs_hash(request->remove_xattr.name,
                                   request->remove_xattr.namelen);

    slice = cairn_meta_get_pinned(thread, &key, sizeof(key), &err);
    chimera_cairn_abort_if(err, "Error getting xattr: %s\n", err);

    if (!slice) {
        cairn_inode_handle_release(&ih);
        request->status = CHIMERA_VFS_ENODATA;
        cairn_queue_request(thread, request);
        return;
    }

    vp = rocksdb_pinnableslice_value(slice, &vlen);
    if (!cairn_xattr_value_matches((const struct cairn_xattr_value *) vp, vlen,
                                   request->remove_xattr.name,
                                   request->remove_xattr.namelen)) {
        rocksdb_pinnableslice_destroy(slice);
        cairn_inode_handle_release(&ih);
        request->status = CHIMERA_VFS_ENODATA;
        cairn_queue_request(thread, request);
        return;
    }
    rocksdb_pinnableslice_destroy(slice);

    txn = cairn_get_meta_txn(thread);
    rocksdb_transaction_delete(txn, (const char *) &key, sizeof(key), &err);
    chimera_cairn_abort_if(err, "Error deleting xattr: %s\n", err);

    clock_gettime(CLOCK_REALTIME, &inode->ctime);
    cairn_put_inode(thread, inode);
    cairn_map_attrs(shared, &request->remove_xattr.r_post_attr, inode);

    cairn_inode_handle_release(&ih);
    request->status = CHIMERA_VFS_OK;
    cairn_queue_request(thread, request);
} /* cairn_remove_xattr */

static void
cairn_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_thread *thread = private_data;
    struct cairn_shared *shared = thread->shared;

    /*
     * Read-only ops run under a snapshot view (cairn_read_begin/end) and
     * complete inline; they never touch the write transaction or get queued,
     * so they're never replayed on optimistic conflict (no leaked iovecs, no
     * double-emitted readdir entries) and don't bloat the conflict read-set.
     * Writer ops read through meta_txn and queue their request; the deferred
     * commit (scheduled by cairn_queue_request / the write helpers) drains
     * and completes them.
     */
    switch (request->opcode) {
        case CHIMERA_VFS_OP_MOUNT:
            cairn_read_begin(thread, 0);
            cairn_mount(thread, shared, request, private_data);
            cairn_read_end(thread);
            break;
        case CHIMERA_VFS_OP_UMOUNT:
            cairn_umount(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LOOKUP_AT:
            cairn_read_begin(thread, 0);
            cairn_lookup_at(thread, shared, request, private_data);
            cairn_read_end(thread);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            cairn_read_begin(thread, 0);
            cairn_getattr(thread, shared, request, private_data);
            cairn_read_end(thread);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            cairn_setattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKDIR_AT:
            cairn_mkdir_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKNOD_AT:
            cairn_mknod_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE_AT:
            cairn_remove_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READDIR:
            cairn_read_begin(thread, 0);
            cairn_readdir(thread, shared, request, private_data);
            cairn_read_end(thread);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            cairn_open_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_FH:
            cairn_open_fh(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_CLOSE:
            cairn_close(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READ:
            cairn_read(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_WRITE:
            cairn_write(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_COMMIT:
            cairn_commit_op(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_ALLOCATE:
            cairn_allocate(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SEEK:
            cairn_read_begin(thread, 1);
            cairn_seek(thread, shared, request, private_data);
            cairn_read_end(thread);
            break;
        case CHIMERA_VFS_OP_SYMLINK_AT:
            cairn_symlink_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READLINK:
            cairn_read_begin(thread, 0);
            cairn_readlink(thread, shared, request, private_data);
            cairn_read_end(thread);
            break;
        case CHIMERA_VFS_OP_RENAME_AT:
            cairn_rename_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LINK_AT:
            cairn_link_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_PUT_KEY:
            cairn_put_key(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_GET_KEY:
            cairn_read_begin(thread, 0);
            cairn_get_key(thread, shared, request, private_data);
            cairn_read_end(thread);
            break;
        case CHIMERA_VFS_OP_DELETE_KEY:
            cairn_delete_key(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SEARCH_KEYS:
            cairn_read_begin(thread, 0);
            cairn_search_keys(thread, shared, request, private_data);
            cairn_read_end(thread);
            break;
        case CHIMERA_VFS_OP_GET_XATTR:
            cairn_read_begin(thread, 0);
            cairn_get_xattr(thread, shared, request, private_data);
            cairn_read_end(thread);
            break;
        case CHIMERA_VFS_OP_SET_XATTR:
            cairn_set_xattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LIST_XATTRS:
            cairn_read_begin(thread, 0);
            cairn_list_xattrs(thread, shared, request, private_data);
            cairn_read_end(thread);
            break;
        case CHIMERA_VFS_OP_REMOVE_XATTR:
            cairn_remove_xattr(thread, shared, request, private_data);
            break;
        default:
            chimera_cairn_error("cairn_dispatch: unknown operation %d",
                                request->opcode);
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */

    /*
     * Bound the batch size.  Natural batching happens for free: while we
     * block in rocksdb_write, requests pile up in our delegation inbox and
     * the next event-loop wake processes them as one batch.  But under high
     * load with slow commits that batch can grow arbitrarily large, and a
     * request near the tail would wait for every commit ahead of it.
     * Force an early commit once the queue reaches CAIRN_BATCH_MAX_OPS so
     * tail latency stays bounded.  Suppressed while in_commit is set so a
     * replay (which re-dispatches queued requests) can't recurse into
     * cairn_thread_commit.
     */
    if (!thread->in_commit && thread->request_count >= CAIRN_BATCH_MAX_OPS) {
        cairn_thread_commit(thread->evpl, thread);
    }
} /* cairn_dispatch */

SYMBOL_EXPORT struct chimera_vfs_module vfs_cairn = {
    .name     = "cairn",
    .fh_magic = CHIMERA_VFS_FH_MAGIC_CAIRN,
    /* CAP_READ_PROVIDES_BUFFERS: cairn_read fills a single contiguous buffer it
     * allocates itself (SHARED, so the CAP_BLOCKING worker->connection-thread
     * release is safe).  TODO: drop this cap and convert cairn_read to scatter
     * its RocksDB extent fill across VFS-core-provided buffers via an
     * append-blob cursor, like diskfs/linux/io_uring, so it can use cheaper
     * non-SHARED connection-thread buffers. */
    .capabilities   = CHIMERA_VFS_CAP_BLOCKING | CHIMERA_VFS_CAP_FS | CHIMERA_VFS_CAP_KV |
        CHIMERA_VFS_CAP_FS_RELATIVE_OP | CHIMERA_VFS_CAP_ACL_NATIVE |
        CHIMERA_VFS_CAP_ATOMIC_HANDLE_STATE |
        CHIMERA_VFS_CAP_XATTR | CHIMERA_VFS_CAP_READ_PROVIDES_BUFFERS |
        CHIMERA_VFS_CAP_FS_LOCK | CHIMERA_VFS_CAP_CHANGE,
    .init           = cairn_init,
    .destroy        = cairn_destroy,
    .thread_init    = cairn_thread_init,
    .thread_destroy = cairn_thread_destroy,
    .dispatch       = cairn_dispatch,
};
