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
#include <jansson.h>
#include <limits.h>
#include <utlist.h>


#include "common/varint.h"

#include "vfs/vfs.h"
#include "vfs/vfs_internal.h"
#include "cairn.h"
#include "common/logging.h"
#include "common/misc.h"
#include "common/macros.h"
#include "evpl_iovec_cursor.h"

#define CAIRN_KEY_INODE   0
#define CAIRN_KEY_DIRENT  1
#define CAIRN_KEY_SYMLINK 2
#define CAIRN_KEY_EXTENT  3
#define CAIRN_KEY_SUPER   4

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

struct cairn_super_key {
    uint8_t keytype;
} __attribute__((packed));

struct cairn_super {
    uint64_t fsid;
};

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
};

struct cairn_inode_handle {
    struct cairn_inode      *inode;
    rocksdb_pinnableslice_t *slice;
};

struct cairn_shared {
    rocksdb_t                           *db;
    rocksdb_cache_t                     *cache;
    rocksdb_transactiondb_t             *db_txn;
    rocksdb_options_t                   *options;
    rocksdb_transactiondb_options_t     *txndb_options;
    rocksdb_writeoptions_t              *write_options;
    rocksdb_readoptions_t               *read_options;
    rocksdb_transaction_options_t       *txn_options;
    rocksdb_block_based_table_options_t *table_options;
    int                                  num_active_threads;
    uint8_t                              root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                             root_fhlen;
    uint64_t                             fsid;
    pthread_mutex_t                      lock;
    int                                  noatime;
};

struct cairn_thread {
    struct evpl                *evpl;
    struct cairn_shared        *shared;
    rocksdb_transaction_t      *txn;
    struct chimera_vfs_request *txn_requests;
    struct evpl_deferral        commit;
    int                         thread_id;
    uint64_t                    next_inum;
};

/* Forward declaration for truncation handling */
static inline void
cairn_punch_hole(
    struct cairn_thread *thread,
    struct cairn_shared *shared,
    struct cairn_inode  *inode,
    uint64_t             offset,
    uint64_t             length);

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

static inline void
cairn_dirent_handle_release(struct cairn_dirent_handle *dh)
{
    rocksdb_pinnableslice_destroy(dh->slice);
} /* cairn_dirent_handle_release */

static inline int
cairn_dirent_get(
    struct cairn_thread        *thread,
    rocksdb_transaction_t      *txn,
    struct cairn_dirent_key    *key,
    struct cairn_dirent_handle *dh)
{
    struct cairn_shared *shared = thread->shared;
    char                *err    = NULL;
    size_t               len;

    dh->slice = rocksdb_transaction_get_pinned(txn, shared->read_options,
                                               (const char *) key, sizeof(*key),
                                               &err);

    chimera_cairn_abort_if(err, "Error getting dirent: %s\n", err);

    if (!dh->slice) {
        dh->dirent = NULL;
        return -1;
    }

    dh->dirent = (struct cairn_dirent_value *) rocksdb_pinnableslice_value(dh->slice, &len);

    return 0;
} /* cairn_dirent_get */

static inline int
cairn_dirent_scan(
    struct cairn_thread *thread,
    rocksdb_transaction_t *txn,
    uint64_t inum,
    uint64_t start_hash,
    int ( *callback )(struct cairn_dirent_key *key, struct cairn_dirent_value *dirent, void *private_data),
    void *private_data)
{
    struct cairn_shared       *shared = thread->shared;
    rocksdb_iterator_t        *iter;
    struct cairn_dirent_key    start_key, *dirent_key;
    struct cairn_dirent_value *dirent_value;
    size_t                     len;

    start_key.keytype = CAIRN_KEY_DIRENT;
    start_key.inum    = inum;
    start_key.hash    = start_hash;

    iter = rocksdb_transaction_create_iterator(txn, shared->read_options);

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
    rocksdb_transaction_t     *txn,
    uint64_t                   inum,
    int                        write,
    struct cairn_inode_handle *ih)
{
    struct cairn_shared   *shared = thread->shared;
    char                  *err    = NULL;
    size_t                 len;
    struct cairn_inode_key key;

    key.keytype = CAIRN_KEY_INODE;
    key.inum    = inum;

    if (write) {
        ih->slice = rocksdb_transaction_get_pinned_for_update(txn, shared->read_options,
                                                              (const char *) &key, sizeof(key),
                                                              1, &err);
    } else {
        ih->slice = rocksdb_transaction_get_pinned(txn, shared->read_options,
                                                   (const char *) &key, sizeof(key),
                                                   &err);
    }

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
    rocksdb_transaction_t     *txn,
    const uint8_t             *fh,
    int                        fhlen,
    int                        write,
    struct cairn_inode_handle *ih)
{
    uint64_t inum;
    uint32_t gen;
    int      rc;

    cairn_fh_to_inum(&inum, &gen, fh, fhlen);

    rc = cairn_inode_get_inum(thread, txn, inum, write, ih);

    if (rc == 0 && ih->inode->gen != gen) {
        cairn_inode_handle_release(ih);
        rc = -1;
    }

    return rc;
} /* cairn_inode_get_fh */

static inline void
cairn_put_dirent(
    rocksdb_transaction_t     *txn,
    struct cairn_dirent_key   *key,
    struct cairn_dirent_value *value)
{
    char *err = NULL;
    int   len;

    len = sizeof(value->inum) + sizeof(value->name_len) + value->name_len;

    rocksdb_transaction_put(txn,
                            (const char *) key, sizeof(*key),
                            (const char *) value, len, &err);

    chimera_cairn_abort_if(err, "Error putting dirent: %s\n", err);
} /* cairn_put_dirent */

static inline void
cairn_put_inode(
    rocksdb_transaction_t *txn,
    struct cairn_inode    *inode)
{
    char                  *err = NULL;
    struct cairn_inode_key key;

    key.keytype = CAIRN_KEY_INODE;
    key.inum    = inode->inum;

    rocksdb_transaction_put(txn,
                            (const char *) &key, sizeof(key),
                            (const char *) inode, sizeof(*inode), &err);

    chimera_cairn_abort_if(err, "Error putting root inode: %s\n", err);
} /* cairn_put_inode */

static inline void
cairn_remove_dirent(
    rocksdb_transaction_t   *txn,
    struct cairn_dirent_key *key)
{
    char *err = NULL;

    rocksdb_transaction_delete(txn,
                               (const char *) key, sizeof(*key),
                               &err);

    chimera_cairn_abort_if(err, "Error deleting dirent: %s\n", err);
} /* cairn_remove_dirent */

static inline void
cairn_remove_inode(
    rocksdb_transaction_t *txn,
    struct cairn_inode    *inode)
{
    char                  *err = NULL;
    struct cairn_inode_key key;

    key.keytype = CAIRN_KEY_INODE;
    key.inum    = inode->inum;

    rocksdb_transaction_delete(txn,
                               (const char *) &key, sizeof(key),
                               &err);

    chimera_cairn_abort_if(err, "Error deleting inode: %s\n", err);
} /* cairn_remove_inode */

static inline void
cairn_remove_symlink_target(
    rocksdb_transaction_t *txn,
    uint64_t               inum)
{
    char                    *err = NULL;
    struct cairn_symlink_key key;

    key.keytype = CAIRN_KEY_SYMLINK;
    key.inum    = inum;

    rocksdb_transaction_delete(txn,
                               (const char *) &key, sizeof(key),
                               &err);

    chimera_cairn_abort_if(err, "Error deleting symlink target: %s\n", err);
} /* cairn_remove_symlink_target */

static inline void
cairn_remove_directory_contents(
    struct cairn_thread   *thread,
    rocksdb_transaction_t *txn,
    uint64_t               dir_inum)
{
    struct cairn_shared    *shared = thread->shared;
    rocksdb_iterator_t     *iter;
    struct cairn_dirent_key start_key, *dirent_key;
    size_t                  klen;

    start_key.keytype = CAIRN_KEY_DIRENT;
    start_key.inum    = dir_inum;
    start_key.hash    = 0;

    iter = rocksdb_transaction_create_iterator(txn, shared->read_options);

    rocksdb_iter_seek(iter, (const char *) &start_key, sizeof(start_key));

    while (rocksdb_iter_valid(iter)) {
        dirent_key = (struct cairn_dirent_key *) rocksdb_iter_key(iter, &klen);

        if (dirent_key->keytype != CAIRN_KEY_DIRENT ||
            dirent_key->inum != dir_inum) {
            break;
        }

        cairn_remove_dirent(txn, dirent_key);
        rocksdb_iter_next(iter);
    }

    rocksdb_iter_destroy(iter);
} /* cairn_remove_directory_contents */

static inline int
cairn_directory_is_empty(
    struct cairn_thread   *thread,
    rocksdb_transaction_t *txn,
    uint64_t               dir_inum)
{
    struct cairn_shared    *shared = thread->shared;
    rocksdb_iterator_t     *iter;
    struct cairn_dirent_key start_key, *dirent_key;
    size_t                  klen;
    int                     is_empty = 1;

    start_key.keytype = CAIRN_KEY_DIRENT;
    start_key.inum    = dir_inum;
    start_key.hash    = 0;

    iter = rocksdb_transaction_create_iterator(txn, shared->read_options);

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
    struct cairn_thread   *thread,
    rocksdb_transaction_t *txn,
    uint64_t               file_inum)
{
    struct cairn_shared    *shared = thread->shared;
    rocksdb_iterator_t     *iter;
    struct cairn_extent_key start_key, *extent_key;
    char                   *err = NULL;
    size_t                  klen;

    start_key.keytype = CAIRN_KEY_EXTENT;
    start_key.inum    = file_inum;
    start_key.offset  = htobe64(0);

    iter = rocksdb_transaction_create_iterator(txn, shared->read_options);

    rocksdb_iter_seek(iter, (const char *) &start_key, sizeof(start_key));

    while (rocksdb_iter_valid(iter)) {
        extent_key = (struct cairn_extent_key *) rocksdb_iter_key(iter, &klen);

        if (extent_key->keytype != CAIRN_KEY_EXTENT ||
            extent_key->inum != file_inum) {
            break;
        }

        rocksdb_transaction_delete(txn,
                                   (const char *) extent_key, sizeof(*extent_key),
                                   &err);

        chimera_cairn_abort_if(err, "Error deleting extent: %s\n", err);

        rocksdb_iter_next(iter);
    }

    rocksdb_iter_destroy(iter);
} /* cairn_remove_file_extents */

static void *
cairn_init(const char *cfgfile)
{
    struct cairn_shared   *shared = calloc(1, sizeof(*shared));
    json_t                *cfg;
    json_error_t           json_error;
    const char            *db_path;
    struct cairn_inode     inode;
    struct cairn_inode_key inode_key;
    rocksdb_transaction_t *txn;
    int                    initialize;
    struct timespec        now;
    char                  *err          = NULL;
    size_t                 cache_mb     = 64;
    int                    compression  = 1; // Default to enabled
    int                    bloom_filter = 1; // Default to enabled

    cfg = json_loads(cfgfile, 0, &json_error);

    chimera_cairn_abort_if(!cfg, "Failed to parse config: %s\n", json_error.text);

    db_path = json_string_value(json_object_get(cfg, "path"));

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

    // Get noatime setting from config
    json_t *noatime_obj = json_object_get(cfg, "noatime");
    if (noatime_obj && json_is_boolean(noatime_obj)) {
        shared->noatime = json_boolean_value(noatime_obj);
    } else {
        shared->noatime = 0; // Default to false
    }

    shared->cache = rocksdb_cache_create_lru(cache_mb * 1024 * 1024);

    shared->options = rocksdb_options_create();

    rocksdb_options_set_compression(shared->options, compression ? rocksdb_lz4_compression : rocksdb_no_compression);

    rocksdb_options_set_write_buffer_size(shared->options, 1024 * 1024 * 1024);
    rocksdb_options_set_max_write_buffer_number(shared->options, 64);
    rocksdb_options_set_max_background_jobs(shared->options, 64);
    rocksdb_options_increase_parallelism(shared->options, 64);
    //rocksdb_options_set_unordered_write(shared->options, 1);
    rocksdb_options_set_memtable_huge_page_size(shared->options, 1024 * 1024 * 1024);
    rocksdb_options_set_allow_concurrent_memtable_write(shared->options, 1);
    rocksdb_options_set_enable_write_thread_adaptive_yield(shared->options, 1);
    ////rocksdb_options_set_disable_auto_compactions(shared->options, 1);
    rocksdb_options_set_max_background_compactions(shared->options, 64);
    rocksdb_options_set_max_background_flushes(shared->options, 64);
    //rocksdb_options_set_level0_file_num_compaction_trigger(shared->options, 1000000);
    //rocksdb_options_set_level0_slowdown_writes_trigger(shared->options, 1000000);
    //rocksdb_options_set_level0_stop_writes_trigger(shared->options, 1000000);

    // Create and configure block based table options
    shared->table_options = rocksdb_block_based_options_create();
    rocksdb_block_based_options_set_block_cache(shared->table_options, shared->cache);

    if (bloom_filter) {
        // Create a bloom filter with 10 bits per key
        rocksdb_filterpolicy_t *bloom = rocksdb_filterpolicy_create_bloom(10);
        rocksdb_block_based_options_set_filter_policy(shared->table_options, bloom);
    }

    // Attach table options to the main options
    rocksdb_options_set_block_based_table_factory(shared->options, shared->table_options);

    initialize = json_boolean_value(json_object_get(cfg, "initialize"));

    if (initialize) {
        rocksdb_destroy_db(shared->options, db_path, &err);
        chimera_cairn_abort_if(err, "Failed to destroy database: %s\n", err);

        rocksdb_options_set_create_if_missing(shared->options, 1);
        rocksdb_options_set_create_missing_column_families(shared->options, 1);
    }

    shared->txndb_options = rocksdb_transactiondb_options_create();
    shared->write_options = rocksdb_writeoptions_create();
    shared->read_options  = rocksdb_readoptions_create();
    shared->txn_options   = rocksdb_transaction_options_create();

    shared->db_txn = rocksdb_transactiondb_open(shared->options, shared->txndb_options, db_path, &err);

    chimera_cairn_abort_if(err, "Failed to open database: %s\n", err);

    json_decref(cfg);

    clock_gettime(CLOCK_REALTIME, &now);

    if (initialize) {
        struct cairn_super_key super_key;
        struct cairn_super     super;

        inode.inum        = 2;
        inode.parent_inum = 2; /* Root directory's parent is itself */
        inode.gen         = 1;
        inode.size        = 4096;
        inode.space_used  = 4096;
        inode.gen         = 1;
        inode.refcnt      = 1;
        inode.uid         = 0;
        inode.gid         = 0;
        inode.nlink       = 2;
        inode.rdev        = 0;
        inode.mode        = S_IFDIR | 0755;
        inode.atime       = now;
        inode.mtime       = now;
        inode.ctime       = now;

        /* Generate a random 64-bit filesystem ID */
        super.fsid = chimera_rand64();

        txn = rocksdb_transaction_begin(shared->db_txn, shared->write_options, shared->txn_options, NULL);

        chimera_cairn_abort_if(err, "Error starting transaction: %s\n", err);

        inode_key.keytype = CAIRN_KEY_INODE;
        inode_key.inum    = inode.inum;

        rocksdb_transaction_put(txn,
                                (const char *) &inode_key, sizeof(inode_key),
                                (const char *) &inode, sizeof(inode), &err);

        chimera_cairn_abort_if(err, "Error putting root inode: %s\n", err);

        /* Store the super block with FSID */
        super_key.keytype = CAIRN_KEY_SUPER;

        rocksdb_transaction_put(txn,
                                (const char *) &super_key, sizeof(super_key),
                                (const char *) &super, sizeof(super), &err);

        chimera_cairn_abort_if(err, "Error putting super block: %s\n", err);

        rocksdb_transaction_commit(txn, &err);

        chimera_cairn_abort_if(err, "Error committing initialization transaction: %s\n", err);

        rocksdb_transaction_destroy(txn);

    }

    /* Load the super block to get the persisted FSID */
    {
        struct cairn_super_key super_key;
        struct cairn_super    *super;
        size_t                 super_len;

        super_key.keytype = CAIRN_KEY_SUPER;

        super = (struct cairn_super *) rocksdb_transactiondb_get(
            shared->db_txn,
            shared->read_options,
            (const char *) &super_key, sizeof(super_key),
            &super_len, &err);

        chimera_cairn_abort_if(err, "Error reading super block: %s\n", err);
        chimera_cairn_abort_if(!super, "Super block not found in database\n");

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

    rocksdb_transactiondb_close(shared->db_txn);
    rocksdb_writeoptions_destroy(shared->write_options);
    rocksdb_readoptions_destroy(shared->read_options);
    rocksdb_options_destroy(shared->options);
    rocksdb_transactiondb_options_destroy(shared->txndb_options);
    rocksdb_transaction_options_destroy(shared->txn_options);
    rocksdb_cache_destroy(shared->cache);
    rocksdb_block_based_options_destroy(shared->table_options);
    free(shared);
} /* cairn_destroy */

static void
cairn_thread_commit(
    struct evpl *evpl,
    void        *private_data)
{
    struct cairn_thread        *thread = private_data;
    struct chimera_vfs_request *request;
    char                       *err = NULL;

    if (thread->txn) {
        rocksdb_transaction_commit(thread->txn, &err);

        chimera_cairn_abort_if(err, "Error committing transaction: %s\n", err);

        rocksdb_transaction_destroy(thread->txn);

        thread->txn = NULL;
    }

    while (thread->txn_requests) {
        request = thread->txn_requests;
        DL_DELETE(thread->txn_requests, request);
        request->complete(request);
    }
} /* cairn_thread_commit */

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

    inode->inum = (id << 8) + thread->thread_id;
    inode->gen  = 1;
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
    }

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_MASK_STATFS) {
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
        if (attr->va_atime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
            inode->atime = now;
        } else {
            inode->atime = attr->va_atime;
        }
    }

    if (set_mask & CHIMERA_VFS_ATTR_MTIME) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_MTIME;
        if (attr->va_mtime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
            inode->mtime = now;
        } else {
            inode->mtime = attr->va_mtime;
        }
    }

    inode->ctime = now;

} /* cairn_apply_attrs */

static rocksdb_transaction_t *
cairn_get_transaction(struct cairn_thread *thread)
{
    if (!thread->txn) {
        thread->txn = rocksdb_transaction_begin(thread->shared->db_txn, thread->shared->write_options, thread->shared->
                                                txn_options, NULL);

        evpl_defer(thread->evpl, &thread->commit);
    }

    return thread->txn;
} /* cairn_get_transaction */

static void
cairn_getattr(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle ih;
    struct cairn_inode       *inode;
    rocksdb_transaction_t    *txn;
    int                       rc;

    txn = cairn_get_transaction(thread);

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, 0, &ih);

    if (unlikely(rc)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;

    cairn_map_attrs(shared, &request->getattr.r_attr, inode);

    cairn_inode_handle_release(&ih);

    request->status = CHIMERA_VFS_OK;

    DL_APPEND(thread->txn_requests, request);
} /* cairn_getattr */

static void
cairn_setattr(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    rocksdb_transaction_t    *txn;
    struct cairn_inode_handle ih;
    struct cairn_inode       *inode;
    int                       rc;

    txn = cairn_get_transaction(thread);

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, 1, &ih);

    if (unlikely(rc)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;

    cairn_map_attrs(shared, &request->setattr.r_pre_attr, inode);

    /* Handle truncation: remove extents past new EOF when size decreases */
    if ((request->setattr.set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) &&
        S_ISREG(inode->mode) &&
        request->setattr.set_attr->va_size < inode->size) {

        uint64_t new_size = request->setattr.set_attr->va_size;
        uint64_t old_size = inode->size;

        cairn_punch_hole(thread, shared, inode, new_size, old_size - new_size);
    }

    cairn_apply_attrs(inode, request->setattr.set_attr);

    cairn_map_attrs(shared, &request->setattr.r_post_attr, inode);

    cairn_put_inode(txn, inode);
    cairn_inode_handle_release(&ih);

    request->status = CHIMERA_VFS_OK;

    DL_APPEND(thread->txn_requests, request);
} /* cairn_setattr */

static inline int
cairn_lookup_path(
    struct cairn_thread       *thread,
    struct cairn_shared       *shared,
    rocksdb_transaction_t     *txn,
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

    rc = cairn_inode_get_fh(thread, txn, shared->root_fh, shared->root_fhlen, 0, &parent_ih);

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

        rc = cairn_dirent_get(thread, txn, &dirent_key, &dh);

        if (rc) {
            cairn_inode_handle_release(&parent_ih);
            return -1;
        }

        dirent_value = dh.dirent;

        cairn_inode_handle_release(&parent_ih);

        rc = cairn_inode_get_inum(thread, txn, dirent_value->inum, 0, &parent_ih);

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
    rocksdb_transaction_t    *txn;
    struct cairn_inode_handle ih;
    struct cairn_inode       *inode;
    int                       rc;

    txn = cairn_get_transaction(thread);

    rc = cairn_lookup_path(thread, shared, txn, request->mount.path, request->mount.pathlen, &ih);

    if (unlikely(rc)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;

    cairn_map_attrs(shared, &request->mount.r_attr, inode);

    cairn_inode_handle_release(&ih);

    request->status = CHIMERA_VFS_OK;

    DL_APPEND(thread->txn_requests, request);
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
cairn_lookup(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    rocksdb_transaction_t     *txn;
    struct cairn_inode_handle  ih, child_ih;
    struct cairn_inode        *inode, *child;
    struct cairn_dirent_key    dirent_key;
    struct cairn_dirent_value *dirent_value;
    struct cairn_dirent_handle dh;
    const char                *name    = request->lookup.component;
    uint32_t                   namelen = request->lookup.component_len;
    int                        rc;

    txn = cairn_get_transaction(thread);

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, 0, &ih);

    if (unlikely(rc)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;

    if (unlikely(!S_ISDIR(inode->mode))) {
        cairn_inode_handle_release(&ih);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    /* Handle "." - return the directory itself */
    if (namelen == 1 && name[0] == '.') {
        cairn_map_attrs(shared, &request->lookup.r_dir_attr, inode);
        cairn_map_attrs(shared, &request->lookup.r_attr, inode);
        cairn_inode_handle_release(&ih);
        request->status = CHIMERA_VFS_OK;
        DL_APPEND(thread->txn_requests, request);
        return;
    }

    /* Handle ".." - return the parent directory */
    if (namelen == 2 && name[0] == '.' && name[1] == '.') {
        cairn_map_attrs(shared, &request->lookup.r_dir_attr, inode);

        rc = cairn_inode_get_inum(thread, txn, inode->parent_inum, 0, &child_ih);

        if (unlikely(rc)) {
            cairn_inode_handle_release(&ih);
            request->status = CHIMERA_VFS_ENOENT;
            request->complete(request);
            return;
        }

        child = child_ih.inode;
        cairn_map_attrs(shared, &request->lookup.r_attr, child);
        cairn_inode_handle_release(&ih);
        cairn_inode_handle_release(&child_ih);
        request->status = CHIMERA_VFS_OK;
        DL_APPEND(thread->txn_requests, request);
        return;
    }

    dirent_key.keytype = CAIRN_KEY_DIRENT;
    dirent_key.inum    = inode->inum;
    dirent_key.hash    = request->lookup.component_hash;

    rc = cairn_dirent_get(thread, txn, &dirent_key, &dh);

    if (rc) {
        cairn_inode_handle_release(&ih);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    dirent_value = dh.dirent;

    cairn_map_attrs(shared, &request->lookup.r_dir_attr, inode);

    rc = cairn_inode_get_inum(thread, txn, dirent_value->inum, 0, &child_ih);

    if (rc) {
        cairn_inode_handle_release(&ih);
        cairn_dirent_handle_release(&dh);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    child = child_ih.inode;

    cairn_map_attrs(shared, &request->lookup.r_attr, child);

    cairn_inode_handle_release(&ih);
    cairn_dirent_handle_release(&dh);
    cairn_inode_handle_release(&child_ih);

    request->status = CHIMERA_VFS_OK;

    DL_APPEND(thread->txn_requests, request);
} /* cairn_lookup */

static void
cairn_mkdir(
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
    rocksdb_transaction_t     *txn;
    int                        rc;
    struct timespec            now;

    clock_gettime(CLOCK_REALTIME, &now);

    txn = cairn_get_transaction(thread);

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, 1, &parent_ih);

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
    dirent_key.hash    = request->mkdir.name_hash;

    rc = cairn_dirent_get(thread, txn, &dirent_key, &dh);

    if (rc == 0) {
        cairn_map_attrs(shared, &request->mkdir.r_dir_pre_attr, parent_inode);
        cairn_map_attrs(shared, &request->mkdir.r_dir_post_attr, parent_inode);

        rc = cairn_inode_get_inum(thread, txn, dh.dirent->inum, 0, &existing_ih);

        if (rc == 0) {
            existing_inode = existing_ih.inode;
            cairn_map_attrs(shared, &request->mkdir.r_attr, existing_inode);
            cairn_inode_handle_release(&existing_ih);
        }
        cairn_inode_handle_release(&parent_ih);
        cairn_dirent_handle_release(&dh);
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        return;
    }

    cairn_alloc_inum(thread, &inode);
    inode.parent_inum = parent_inode->inum; /* Set parent for ".." lookup */
    inode.size        = 4096;
    inode.space_used  = 4096;
    inode.uid         = 0;
    inode.gid         = 0;
    inode.nlink       = 2;
    inode.rdev        = 0;
    inode.mode        = S_IFDIR | 0755;
    inode.atime       = now;
    inode.mtime       = now;
    inode.ctime       = now;

    cairn_apply_attrs(&inode, request->mkdir.set_attr);

    cairn_map_attrs(shared, &request->mkdir.r_attr, &inode);

    dirent_value.inum     = inode.inum;
    dirent_value.name_len = request->mkdir.name_len;
    memcpy(dirent_value.name, request->mkdir.name, request->mkdir.name_len);

    cairn_map_attrs(shared, &request->mkdir.r_dir_pre_attr, parent_inode);

    parent_inode->nlink++;

    parent_inode->mtime = now;

    cairn_map_attrs(shared, &request->mkdir.r_dir_post_attr, parent_inode);

    cairn_put_dirent(txn, &dirent_key, &dirent_value);
    cairn_put_inode(txn, parent_inode);
    cairn_put_inode(txn, &inode);

    request->status = CHIMERA_VFS_OK;

    cairn_inode_handle_release(&parent_ih);

    DL_APPEND(thread->txn_requests, request);
} /* cairn_mkdir */

static void
cairn_mknod(
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
    rocksdb_transaction_t     *txn;
    int                        rc;
    struct timespec            now;

    clock_gettime(CLOCK_REALTIME, &now);

    txn = cairn_get_transaction(thread);

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, 1, &parent_ih);

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
    dirent_key.hash    = request->mknod.name_hash;

    rc = cairn_dirent_get(thread, txn, &dirent_key, &dh);

    if (rc == 0) {
        cairn_map_attrs(shared, &request->mknod.r_dir_pre_attr, parent_inode);
        cairn_map_attrs(shared, &request->mknod.r_dir_post_attr, parent_inode);

        rc = cairn_inode_get_inum(thread, txn, dh.dirent->inum, 0, &existing_ih);

        if (rc == 0) {
            existing_inode = existing_ih.inode;
            cairn_map_attrs(shared, &request->mknod.r_attr, existing_inode);
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
    inode.uid         = 0;
    inode.gid         = 0;
    inode.nlink       = 1;
    inode.rdev        = 0;
    inode.atime       = now;
    inode.mtime       = now;
    inode.ctime       = now;

    /* Set mode (including file type bits) and rdev from set_attr */
    if (request->mknod.set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        inode.mode = request->mknod.set_attr->va_mode;
    } else {
        inode.mode = S_IFREG | 0644;
    }

    if (request->mknod.set_attr->va_set_mask & CHIMERA_VFS_ATTR_RDEV) {
        inode.rdev = request->mknod.set_attr->va_rdev;
    }

    cairn_apply_attrs(&inode, request->mknod.set_attr);

    cairn_map_attrs(shared, &request->mknod.r_attr, &inode);

    dirent_value.inum     = inode.inum;
    dirent_value.name_len = request->mknod.name_len;
    memcpy(dirent_value.name, request->mknod.name, request->mknod.name_len);

    cairn_map_attrs(shared, &request->mknod.r_dir_pre_attr, parent_inode);

    parent_inode->mtime = now;

    cairn_map_attrs(shared, &request->mknod.r_dir_post_attr, parent_inode);

    cairn_put_dirent(txn, &dirent_key, &dirent_value);
    cairn_put_inode(txn, parent_inode);
    cairn_put_inode(txn, &inode);

    request->status = CHIMERA_VFS_OK;

    cairn_inode_handle_release(&parent_ih);

    DL_APPEND(thread->txn_requests, request);
} /* cairn_mknod */

static void
cairn_remove(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    rocksdb_transaction_t     *txn;
    struct cairn_inode_handle  parent_ih, child_ih;
    struct cairn_inode        *parent_inode, *inode;
    struct cairn_dirent_key    dirent_key;
    struct cairn_dirent_handle dh;
    struct cairn_dirent_value *dirent_value;
    int                        rc;
    struct timespec            now;

    clock_gettime(CLOCK_REALTIME, &now);
    txn = cairn_get_transaction(thread);

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, 1, &parent_ih);

    if (unlikely(rc)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    parent_inode = parent_ih.inode;

    if (!S_ISDIR(parent_inode->mode)) {
        cairn_inode_handle_release(&parent_ih);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    dirent_key.keytype = CAIRN_KEY_DIRENT;
    dirent_key.inum    = parent_inode->inum;
    dirent_key.hash    = request->remove.name_hash;

    rc = cairn_dirent_get(thread, txn, &dirent_key, &dh);

    if (rc) {
        cairn_inode_handle_release(&parent_ih);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    dirent_value = dh.dirent;

    rc = cairn_inode_get_inum(thread, txn, dirent_value->inum, 1, &child_ih);

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
        if (!cairn_directory_is_empty(thread, txn, inode->inum)) {
            cairn_inode_handle_release(&parent_ih);
            cairn_inode_handle_release(&child_ih);
            cairn_dirent_handle_release(&dh);
            request->status = CHIMERA_VFS_ENOTEMPTY;
            request->complete(request);
            return;
        }
    }

    cairn_map_attrs(shared, &request->remove.r_dir_pre_attr, parent_inode);

    parent_inode->mtime = now;

    if (S_ISDIR(inode->mode)) {
        inode->nlink = 0;
        parent_inode->nlink--;

    } else {
        inode->nlink--;
    }

    if (inode->nlink == 0) {
        request->remove.r_removed_attr.va_req_mask = CHIMERA_VFS_ATTR_FH;
    }

    cairn_map_attrs(shared, &request->remove.r_removed_attr, inode);

    if (inode->nlink == 0) {
        --inode->refcnt;

        if (inode->refcnt == 0) {
            // Remove type-specific data before removing inode
            if (S_ISREG(inode->mode)) {
                cairn_remove_file_extents(thread, txn, inode->inum);
            } else if (S_ISLNK(inode->mode)) {
                cairn_remove_symlink_target(txn, inode->inum);
            }

            cairn_remove_inode(txn, inode);
        } else {
            cairn_put_inode(txn, inode);
        }
    } else {
        // nlink > 0: file still has other hard links, persist the decremented nlink
        cairn_put_inode(txn, inode);
    }

    cairn_map_attrs(shared, &request->remove.r_dir_post_attr, parent_inode);

    cairn_remove_dirent(txn, &dirent_key);

    cairn_put_inode(txn, parent_inode);

    cairn_inode_handle_release(&parent_ih);
    cairn_inode_handle_release(&child_ih);
    cairn_dirent_handle_release(&dh);

    request->status = CHIMERA_VFS_OK;
    DL_APPEND(thread->txn_requests, request);
} /* cairn_remove */

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
    rocksdb_transaction_t     *txn;
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

    txn = cairn_get_transaction(thread);

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, 0, &ih);

    if (rc) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;

    if (!S_ISDIR(inode->mode)) {
        cairn_inode_handle_release(&ih);
        request->status = CHIMERA_VFS_ENOENT;
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
            rc = cairn_inode_get_inum(thread, txn, inode->parent_inum, 0, &parent_ih);

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

    iter = rocksdb_transaction_create_iterator(txn, shared->read_options);

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

        rc = cairn_inode_get_inum(thread, txn, dirent_value->inum, 0, &dirent_ih);

        if (rc) {
            rocksdb_iter_next(iter);
            continue;
        }

        dirent_inode = dirent_ih.inode;

        cairn_map_attrs(shared, &attr, dirent_inode);

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

    DL_APPEND(thread->txn_requests, request);
} /* cairn_readdir */

static void
cairn_open(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    rocksdb_transaction_t    *txn;
    struct cairn_inode_handle ih;
    struct cairn_inode       *inode;
    int                       rc;

    txn = cairn_get_transaction(thread);

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, 1, &ih);

    if (rc) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;
    inode->refcnt++;

    request->open.r_vfs_private = (uint64_t) inode->inum;

    cairn_put_inode(txn, inode);
    cairn_inode_handle_release(&ih);

    request->status = CHIMERA_VFS_OK;

    DL_APPEND(thread->txn_requests, request);
} /* cairn_open */

static void
cairn_open_at(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    rocksdb_transaction_t     *txn;
    struct cairn_inode_handle  parent_ih, child_ih;
    struct cairn_inode        *parent_inode, *inode = NULL, new_inode;
    struct cairn_dirent_key    dirent_key;
    struct cairn_dirent_handle dh;
    struct cairn_dirent_value *dirent_value, new_dirent_value;
    unsigned int               flags = request->open_at.flags;
    int                        rc, is_new_inode = 0;
    struct timespec            now;

    clock_gettime(CLOCK_REALTIME, &now);

    txn = cairn_get_transaction(thread);

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, 1, &parent_ih);

    if (unlikely(rc)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    parent_inode = parent_ih.inode;

    if (!S_ISDIR(parent_inode->mode)) {
        cairn_inode_handle_release(&parent_ih);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    cairn_map_attrs(shared, &request->open_at.r_dir_pre_attr, parent_inode);

    dirent_key.keytype = CAIRN_KEY_DIRENT;
    dirent_key.inum    = parent_inode->inum;
    dirent_key.hash    = request->open_at.name_hash;

    rc = cairn_dirent_get(thread, txn, &dirent_key, &dh);

    if (rc) {
        if (!(flags & CHIMERA_VFS_OPEN_CREATE)) {
            cairn_inode_handle_release(&parent_ih);
            request->status = CHIMERA_VFS_EEXIST;
            request->complete(request);
            return;
        }

        is_new_inode = 1;

        cairn_alloc_inum(thread, &new_inode);
        new_inode.size       = 0;
        new_inode.space_used = 0;
        new_inode.uid        = 0;
        new_inode.gid        = 0;
        new_inode.nlink      = 1;
        new_inode.rdev       = 0;
        new_inode.mode       = S_IFREG |  0644;
        new_inode.atime      = now;
        new_inode.mtime      = now;
        new_inode.ctime      = now;
        new_inode.refcnt     = 1;

        cairn_apply_attrs(&new_inode, request->open_at.set_attr);

        new_dirent_value.inum     = new_inode.inum;
        new_dirent_value.name_len = request->open_at.namelen;
        memcpy(new_dirent_value.name, request->open_at.name, request->open_at.namelen);

        cairn_put_dirent(txn, &dirent_key, &new_dirent_value);

        parent_inode->mtime = now;

        inode = &new_inode;
    } else if (flags & CHIMERA_VFS_OPEN_EXCLUSIVE) {
        cairn_inode_handle_release(&parent_ih);
        cairn_dirent_handle_release(&dh);
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        return;
    } else {

        dirent_value = dh.dirent;

        rc = cairn_inode_get_inum(thread, txn, dirent_value->inum, 1, &child_ih);

        if (rc) {
            cairn_inode_handle_release(&parent_ih);
            cairn_dirent_handle_release(&dh);
            request->status = CHIMERA_VFS_ENOENT;
            request->complete(request);
            return;
        }

        inode = child_ih.inode;

        cairn_dirent_handle_release(&dh);
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

    cairn_put_inode(txn, parent_inode);
    cairn_put_inode(txn, inode);

    cairn_inode_handle_release(&parent_ih);

    if (!is_new_inode) {
        cairn_inode_handle_release(&child_ih);
    }

    request->status = CHIMERA_VFS_OK;
    DL_APPEND(thread->txn_requests, request);
} /* cairn_open_at */

static void
cairn_close(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    rocksdb_transaction_t    *txn;
    struct cairn_inode_handle ih;
    struct cairn_inode       *inode;
    int                       rc;

    txn = cairn_get_transaction(thread);

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, 1, &ih);

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
            cairn_remove_file_extents(thread, txn, inode->inum);
        } else if (S_ISLNK(inode->mode)) {
            cairn_remove_symlink_target(txn, inode->inum);
        }

        cairn_remove_inode(txn, inode);
    } else {
        cairn_put_inode(txn, inode);
    }

    cairn_inode_handle_release(&ih);

    request->status = CHIMERA_VFS_OK;

    DL_APPEND(thread->txn_requests, request);
} /* cairn_close */

static void
cairn_read(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    rocksdb_transaction_t    *txn;
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
        request->read.r_eof    = 1;
        request->complete(request);
        return;
    }

    txn = cairn_get_transaction(thread);

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, need_atime, &ih);

    if (rc) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;

    if (offset >= inode->size) {
        cairn_inode_handle_release(&ih);
        request->status        = CHIMERA_VFS_OK;
        request->read.r_niov   = 0;
        request->read.r_length = 0;
        request->read.r_eof    = 1;
        request->complete(request);
        return;
    }

    if (offset + length > inode->size) {
        length = inode->size - offset;
        eof    = 1;
    }

    request->read.r_niov = evpl_iovec_alloc(thread->evpl, length, 4096, 1, EVPL_IOVEC_FLAG_SHARED, request->read.iov);
    iov                  = request->read.iov;

    start_key.keytype = CAIRN_KEY_EXTENT;
    start_key.inum    = inode->inum;
    start_key.offset  = htobe64(offset);

    iter = rocksdb_transaction_create_iterator(txn, shared->read_options);

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

    if (need_atime) {
        inode->atime = now;
        cairn_put_inode(txn, inode);
    }

    cairn_map_attrs(shared, &request->read.r_attr, inode);

    cairn_inode_handle_release(&ih);

    request->status        = CHIMERA_VFS_OK;
    request->read.r_length = length;
    request->read.r_eof    = eof;
    request->read.iov      = iov;
    iov[0].length          = length;

    DL_APPEND(thread->txn_requests, request);
} /* cairn_read */

static inline void
cairn_punch_hole(
    struct cairn_thread *thread,
    struct cairn_shared *shared,
    struct cairn_inode  *inode,
    uint64_t             offset,
    uint64_t             length)
{
    rocksdb_transaction_t  *txn;
    rocksdb_iterator_t     *iter;
    struct cairn_extent_key start_key, *extent_key;
    uint64_t                hole_end    = offset + length;
    uint64_t                space_freed = 0;
    char                   *err         = NULL;
    size_t                  klen;

    txn = cairn_get_transaction(thread);

    start_key.keytype = CAIRN_KEY_EXTENT;
    start_key.inum    = inode->inum;
    start_key.offset  = htobe64(offset);

    iter = rocksdb_transaction_create_iterator(txn, shared->read_options);

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
        uint64_t extent_length;
        void    *extent_data = (void *) rocksdb_iter_value(iter, &extent_length);

        // Stop if we've moved past this inode
        if (extent_key->keytype != CAIRN_KEY_EXTENT || extent_key->inum != inode->inum) {
            break;
        }

        uint64_t extent_start = be64toh(extent_key->offset);
        uint64_t extent_end   = extent_start + extent_length;

        // Stop if extent starts after hole
        if (extent_start >= hole_end) {
            break;
        }

        // Check for overlap
        if (extent_end > offset && extent_start < hole_end) {
            // Track space being freed from original extent
            space_freed += extent_length;

            // Delete the original extent
            rocksdb_transaction_delete(txn, (const char *) extent_key, sizeof(*extent_key),
                                       &err);
            chimera_cairn_abort_if(err, "Error deleting extent: %s\n", err);

            // If there's data before the hole, create a new extent
            if (extent_start < offset) {
                struct cairn_extent_key new_key = {
                    .keytype = CAIRN_KEY_EXTENT,
                    .inum    = inode->inum,
                    .offset  = htobe64(extent_start),
                };

                rocksdb_transaction_put(txn, (const char *) &new_key, sizeof(new_key),
                                        extent_data, offset - extent_start,
                                        &err);
                chimera_cairn_abort_if(err, "Error putting extent: %s\n", err);

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

                rocksdb_transaction_put(txn, (const char *) &new_key, sizeof(new_key),
                                        extent_data + (hole_end - extent_start),
                                        extent_end - hole_end,
                                        &err);
                chimera_cairn_abort_if(err, "Error putting extent: %s\n", err);

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
    rocksdb_transaction_t    *txn;
    struct cairn_inode_handle ih;
    struct cairn_inode       *inode;
    uint64_t                  current_offset;
    uint64_t                  total_space = 0;
    char                     *err         = NULL;
    int                       rc, i;
    struct timespec           now;

    clock_gettime(CLOCK_REALTIME, &now);

    txn = cairn_get_transaction(thread);

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, 1, &ih);

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

    if (inode->size > request->write.offset) {
        cairn_punch_hole(thread, shared, inode, request->write.offset, request->write.length);
    }

    // Write each iovec as a new extent
    current_offset = request->write.offset;

    for (i = 0; i < request->write.niov; i++) {
        const struct evpl_iovec *iov = &request->write.iov[i];

        struct cairn_extent_key  key = {
            .keytype = CAIRN_KEY_EXTENT,
            .inum    = inode->inum,
            .offset  = htobe64(current_offset),
        };

        // Store the extent with just the data
        rocksdb_transaction_put(txn, (const char *) &key, sizeof(key),
                                iov->data, iov->length,
                                &err);

        chimera_cairn_abort_if(err, "Error putting extent: %s\n", err);

        total_space    += iov->length;
        current_offset += iov->length;
    }

    // Update inode size if needed
    if (inode->size < request->write.offset + request->write.length) {
        inode->size = request->write.offset + request->write.length;
    }

    // Update space used to track actual extent sizes
    inode->space_used += total_space;
    inode->mtime       = now;

    cairn_map_attrs(shared, &request->write.r_post_attr, inode);

    cairn_put_inode(txn, inode);
    cairn_inode_handle_release(&ih);

    request->status         = CHIMERA_VFS_OK;
    request->write.r_length = request->write.length;
    request->write.r_sync   = 1;

    /* Note: Write iovecs are NOT released here. They were allocated on the
     * server thread and must be released there. The server's write completion
     * callback handles the release after this request completes via doorbell.
     */

    DL_APPEND(thread->txn_requests, request);
} /* cairn_write */

static void
cairn_symlink(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    rocksdb_transaction_t    *txn;
    struct cairn_inode_handle parent_ih;
    struct cairn_inode       *parent_inode, new_inode;
    struct cairn_dirent_key   dirent_key;
    struct cairn_dirent_value dirent_value;
    struct cairn_symlink_key  target_key;
    char                     *err = NULL;
    int                       rc;
    struct timespec           now;

    clock_gettime(CLOCK_REALTIME, &now);
    txn = cairn_get_transaction(thread);

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, 1, &parent_ih);

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

    cairn_map_attrs(shared, &request->symlink.r_dir_pre_attr, parent_inode);

    dirent_key.keytype = CAIRN_KEY_DIRENT;
    dirent_key.inum    = parent_inode->inum;
    dirent_key.hash    = request->symlink.name_hash;

    cairn_alloc_inum(thread, &new_inode);
    new_inode.size       = request->symlink.targetlen;
    new_inode.space_used = request->symlink.targetlen;
    new_inode.uid        = 0;
    new_inode.gid        = 0;
    new_inode.nlink      = 1;
    new_inode.rdev       = 0;
    new_inode.mode       = S_IFLNK | 0755;
    new_inode.atime      = now;
    new_inode.mtime      = now;
    new_inode.ctime      = now;

    dirent_value.inum     = new_inode.inum;
    dirent_value.name_len = request->symlink.namelen;
    memcpy(dirent_value.name, request->symlink.name, request->symlink.namelen);

    parent_inode->mtime = now;

    cairn_map_attrs(shared, &request->symlink.r_attr, &new_inode);
    cairn_map_attrs(shared, &request->symlink.r_dir_post_attr, parent_inode);

    target_key.keytype = CAIRN_KEY_SYMLINK;
    target_key.inum    = new_inode.inum;
    rocksdb_transaction_put(txn, (const char *) &target_key, sizeof(target_key),
                            request->symlink.target, request->symlink.targetlen,
                            &err);

    chimera_cairn_abort_if(err, "Error putting symlink target: %s\n", err);

    cairn_put_dirent(txn, &dirent_key, &dirent_value);
    cairn_put_inode(txn, parent_inode);
    cairn_put_inode(txn, &new_inode);

    cairn_inode_handle_release(&parent_ih);

    request->status = CHIMERA_VFS_OK;

    DL_APPEND(thread->txn_requests, request);
} /* cairn_symlink */

static void
cairn_readlink(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    rocksdb_transaction_t    *txn;
    struct cairn_inode_handle ih;
    struct cairn_inode       *inode;
    struct cairn_symlink_key  target_key;
    char                     *err = NULL;
    size_t                    target_len;
    int                       rc;

    txn = cairn_get_transaction(thread);

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, 0, &ih);

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

    rocksdb_pinnableslice_t *slice = rocksdb_transaction_get_pinned(
        txn, shared->read_options,
        (const char *) &target_key, sizeof(target_key),
        &err);

    chimera_cairn_abort_if(err, "Error getting symlink target: %s\n", err);

    if (!slice) {
        cairn_inode_handle_release(&ih);
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    const char *target = rocksdb_pinnableslice_value(slice, &target_len);

    request->readlink.r_target_length = target_len;
    memcpy(request->readlink.r_target, target, target_len);

    rocksdb_pinnableslice_destroy(slice);
    cairn_inode_handle_release(&ih);

    request->status = CHIMERA_VFS_OK;
    DL_APPEND(thread->txn_requests, request);
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
cairn_rename(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    rocksdb_transaction_t     *txn;
    struct cairn_inode_handle  old_parent_ih, new_parent_ih;
    struct cairn_inode        *old_parent_inode, *new_parent_inode;
    struct cairn_dirent_key    old_dirent_key, new_dirent_key;
    struct cairn_dirent_handle old_dh, new_dh;
    struct cairn_dirent_value *old_dirent_value;
    struct cairn_dirent_value  new_dirent_value;
    struct cairn_inode_handle  target_ih;
    struct cairn_inode        *target_inode;
    int                        cmp, rc, have_new_parent_ih = 0;
    struct timespec            now;

    clock_gettime(CLOCK_REALTIME, &now);
    txn = cairn_get_transaction(thread);

    cmp = cairn_fh_compare(request->fh,
                           request->fh_len,
                           request->rename.new_fh,
                           request->rename.new_fhlen);

    if (cmp == 0) {
        rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, 1, &old_parent_ih);

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
            rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, 1, &old_parent_ih);
            if (rc) {
                request->status = CHIMERA_VFS_ENOENT;
                request->complete(request);
                return;
            }

            rc = cairn_inode_get_fh(thread, txn, request->rename.new_fh, request->rename.new_fhlen, 1, &new_parent_ih);
            if (rc) {
                cairn_inode_handle_release(&old_parent_ih);
                request->status = CHIMERA_VFS_ENOENT;
                request->complete(request);
                return;
            }
        } else {
            rc = cairn_inode_get_fh(thread, txn, request->rename.new_fh, request->rename.new_fhlen, 1, &new_parent_ih);
            if (rc) {
                request->status = CHIMERA_VFS_ENOENT;
                request->complete(request);
                return;
            }

            rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, 1, &old_parent_ih);
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
    old_dirent_key.hash    = request->rename.name_hash;

    rc = cairn_dirent_get(thread, txn, &old_dirent_key, &old_dh);
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

    new_dirent_key.keytype = CAIRN_KEY_DIRENT;
    new_dirent_key.inum    = new_parent_inode->inum;
    new_dirent_key.hash    = request->rename.new_name_hash;

    rc = cairn_dirent_get(thread, txn, &new_dirent_key, &new_dh);
    if (rc == 0) {
        // Target exists
        // Per POSIX: if old and new refer to same file, return success with no action
        if (new_dh.dirent->inum == old_dirent_value->inum) {
            cairn_dirent_handle_release(&old_dh);
            cairn_dirent_handle_release(&new_dh);
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

        rc = cairn_inode_get_inum(thread, txn, new_dh.dirent->inum, 1, &existing_ih);
        if (rc == 0) {
            existing_inode = existing_ih.inode;
            existing_inode->nlink--;

            if (existing_inode->nlink == 0) {
                existing_inode->refcnt--;

                if (existing_inode->refcnt == 0) {
                    // Remove type-specific data before removing inode
                    if (S_ISREG(existing_inode->mode)) {
                        cairn_remove_file_extents(thread, txn, existing_inode->inum);
                    } else if (S_ISLNK(existing_inode->mode)) {
                        cairn_remove_symlink_target(txn, existing_inode->inum);
                    }

                    cairn_remove_inode(txn, existing_inode);
                } else {
                    cairn_put_inode(txn, existing_inode);
                }
            } else {
                cairn_put_inode(txn, existing_inode);
            }

            cairn_inode_handle_release(&existing_ih);
        }
        cairn_dirent_handle_release(&new_dh);
    }

    // Get the target inode to update its ctime
    rc = cairn_inode_get_inum(thread, txn, old_dirent_value->inum, 1, &target_ih);
    if (rc == 0) {
        target_inode        = target_ih.inode;
        target_inode->ctime = now;
        cairn_put_inode(txn, target_inode);
        cairn_inode_handle_release(&target_ih);
    }

    // Create new dirent
    new_dirent_value.inum     = old_dirent_value->inum;
    new_dirent_value.name_len = request->rename.new_namelen;
    memcpy(new_dirent_value.name, request->rename.new_name, request->rename.new_namelen);

    // Update directory entries and parent inodes
    cairn_remove_dirent(txn, &old_dirent_key);
    cairn_put_dirent(txn, &new_dirent_key, &new_dirent_value);

    old_parent_inode->mtime = now;
    new_parent_inode->mtime = now;

    if (cmp != 0) {
        /* XXX only if dir */
        old_parent_inode->nlink--;
        new_parent_inode->nlink++;
    }

    cairn_put_inode(txn, old_parent_inode);
    if (cmp != 0) {
        cairn_put_inode(txn, new_parent_inode);
    }

    // Cleanup
    cairn_dirent_handle_release(&old_dh);

    cairn_inode_handle_release(&old_parent_ih);

    if (have_new_parent_ih) {
        cairn_inode_handle_release(&new_parent_ih);
    }

    request->status = CHIMERA_VFS_OK;
    DL_APPEND(thread->txn_requests, request);
} /* cairn_rename */

static void
cairn_link(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    rocksdb_transaction_t     *txn;
    struct cairn_inode_handle  parent_ih, target_ih;
    struct cairn_inode        *parent_inode, *target_inode;
    struct cairn_dirent_key    dirent_key;
    struct cairn_dirent_value  dirent_value;
    struct cairn_dirent_handle dh;
    int                        rc;
    struct timespec            now;

    clock_gettime(CLOCK_REALTIME, &now);

    txn = cairn_get_transaction(thread);

    rc = cairn_inode_get_fh(thread, txn, request->link.dir_fh, request->link.dir_fhlen, 1, &parent_ih);

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

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, 1, &target_ih);

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
        request->status = CHIMERA_VFS_EPERM;
        request->complete(request);
        return;
    }

    dirent_key.keytype = CAIRN_KEY_DIRENT;
    dirent_key.inum    = parent_inode->inum;
    dirent_key.hash    = request->link.name_hash;

    rc = cairn_dirent_get(thread, txn, &dirent_key, &dh);

    if (rc == 0) {
        cairn_inode_handle_release(&parent_ih);
        cairn_inode_handle_release(&target_ih);
        cairn_dirent_handle_release(&dh);
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        return;
    }

    dirent_value.inum     = target_inode->inum;
    dirent_value.name_len = request->link.namelen;
    memcpy(dirent_value.name, request->link.name, request->link.namelen);

    target_inode->nlink++;
    target_inode->ctime = now;
    parent_inode->mtime = now;

    cairn_put_dirent(txn, &dirent_key, &dirent_value);
    cairn_put_inode(txn, parent_inode);
    cairn_put_inode(txn, target_inode);

    cairn_inode_handle_release(&parent_ih);
    cairn_inode_handle_release(&target_ih);

    request->status = CHIMERA_VFS_OK;

    DL_APPEND(thread->txn_requests, request);
} /* cairn_link */

static void
cairn_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_thread *thread = private_data;
    struct cairn_shared *shared = thread->shared;

    switch (request->opcode) {
        case CHIMERA_VFS_OP_MOUNT:
            cairn_mount(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_UMOUNT:
            cairn_umount(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LOOKUP:
            cairn_lookup(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            cairn_getattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            cairn_setattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKDIR:
            cairn_mkdir(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKNOD:
            cairn_mknod(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE:
            cairn_remove(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READDIR:
            cairn_readdir(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            cairn_open_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN:
            cairn_open(thread, shared, request, private_data);
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
            request->status = CHIMERA_VFS_OK;
            request->complete(request);
            break;
        case CHIMERA_VFS_OP_SYMLINK:
            cairn_symlink(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READLINK:
            cairn_readlink(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_RENAME:
            cairn_rename(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LINK:
            cairn_link(thread, shared, request, private_data);
            break;
        default:
            chimera_cairn_error("cairn_dispatch: unknown operation %d",
                                request->opcode);
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* cairn_dispatch */

SYMBOL_EXPORT struct chimera_vfs_module vfs_cairn = {
    .name           = "cairn",
    .fh_magic       = CHIMERA_VFS_FH_MAGIC_CAIRN,
    .capabilities   = CHIMERA_VFS_CAP_BLOCKING,
    .init           = cairn_init,
    .destroy        = cairn_destroy,
    .thread_init    = cairn_thread_init,
    .thread_destroy = cairn_thread_destroy,
    .dispatch       = cairn_dispatch,
};
