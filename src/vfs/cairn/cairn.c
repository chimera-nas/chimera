#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <rocksdb/c.h>
#include <jansson.h>
#include <limits.h>
#include "common/varint.h"
#include "xxhash.h"

#include "uthash/utlist.h"
#include "vfs/vfs.h"
#include "cairn.h"
#include "common/logging.h"
#include "common/misc.h"
#include "core/deferral.h"
#include "evpl_iovec_cursor.h"

#define CAIRN_CF_DEFAULT 0
#define CAIRN_CF_INODE   1
#define CAIRN_CF_DIRENT  2
#define CAIRN_CF_SYMLINK 3
#define CAIRN_CF_EXTENT  4
#define CAIRN_NUM_CF     5

const char *cairn_cf_names[] = { "default", "inode", "dirent", "symlink", "extent" };

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

struct cairn_dirent_key {
    uint64_t inum;
    uint64_t hash;
} __attribute__((packed));

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

struct cairn_extent_key {
    uint64_t inum;
    uint64_t offset;
} __attribute__((packed));

struct cairn_inode {
    uint64_t        inum;
    uint32_t        gen;
    uint32_t        refcnt;
    uint64_t        size;
    uint64_t        space_used;
    uint32_t        mode;
    uint32_t        nlink;
    uint32_t        uid;
    uint32_t        gid;
    struct timespec atime;
    struct timespec mtime;
    struct timespec ctime;
};

struct cairn_inode_handle {
    struct cairn_inode      *inode;
    rocksdb_pinnableslice_t *slice;
};

struct cairn_shared {
    rocksdb_t                       *db;
    rocksdb_transactiondb_t         *db_txn;
    rocksdb_options_t               *options;
    rocksdb_transactiondb_options_t *txndb_options;
    rocksdb_writeoptions_t          *write_options;
    rocksdb_readoptions_t           *read_options;
    rocksdb_transaction_options_t   *txn_options;
    rocksdb_options_t               *cf_options[CAIRN_NUM_CF];
    rocksdb_column_family_handle_t  *cf_handles[CAIRN_NUM_CF];
    int                              num_active_threads;
    uint8_t                          root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                         root_fhlen;
    pthread_mutex_t                  lock;
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

static inline uint32_t
cairn_inum_to_fh(
    uint8_t *fh,
    uint64_t inum,
    uint32_t gen)
{
    uint8_t *ptr = fh;

    *ptr++ = CHIMERA_VFS_FH_MAGIC_CAIRN;

    ptr += chimera_encode_uint64(inum, ptr);
    ptr += chimera_encode_uint32(gen, ptr);

    return ptr - fh;
} /* cairn_inum_to_fh */

static inline void
cairn_fh_to_inum(
    uint64_t      *inum,
    uint32_t      *gen,
    const uint8_t *fh,
    int            fhlen)
{
    const uint8_t *ptr = fh;

    ptr++;

    ptr += chimera_decode_uint64(ptr, inum);
    ptr += chimera_decode_uint32(ptr, gen);
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

    dh->slice = rocksdb_transaction_get_pinned_cf(txn, shared->read_options,
                                                  shared->cf_handles[CAIRN_CF_DIRENT],
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

    start_key.inum = inum;
    start_key.hash = start_hash;

    iter = rocksdb_transaction_create_iterator_cf(txn, shared->read_options, shared->cf_handles[CAIRN_CF_DIRENT]);

    rocksdb_iter_seek(iter, (const char *) &start_key, sizeof(start_key));

    while (rocksdb_iter_valid(iter)) {
        dirent_key   = (struct cairn_dirent_key *) rocksdb_iter_key(iter, &len);
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
    struct cairn_shared *shared = thread->shared;
    char                *err    = NULL;
    size_t               len;

    if (write) {
        ih->slice = rocksdb_transaction_get_pinned_for_update_cf(txn, shared->read_options,
                                                                 shared->cf_handles[CAIRN_CF_INODE],
                                                                 (const char *) &inum, sizeof(inum),
                                                                 1, &err);
    } else {
        ih->slice = rocksdb_transaction_get_pinned_cf(txn, shared->read_options,
                                                      shared->cf_handles[CAIRN_CF_INODE],
                                                      (const char *) &inum, sizeof(inum),
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
    struct cairn_thread       *thread,
    rocksdb_transaction_t     *txn,
    struct cairn_dirent_key   *key,
    struct cairn_dirent_value *value)
{
    struct cairn_shared *shared = thread->shared;
    char                *err    = NULL;
    int                  len;

    len = sizeof(value->inum) + sizeof(value->name_len) + value->name_len;

    rocksdb_transaction_put_cf(txn, shared->cf_handles[CAIRN_CF_DIRENT],
                               (const char *) key, sizeof(*key),
                               (const char *) value, len, &err);

    chimera_cairn_abort_if(err, "Error putting dirent: %s\n", err);
} /* cairn_put_dirent */

static inline void
cairn_put_inode(
    struct cairn_thread   *thread,
    rocksdb_transaction_t *txn,
    struct cairn_inode    *inode)
{
    struct cairn_shared *shared = thread->shared;
    char                *err    = NULL;

    rocksdb_transaction_put_cf(txn, shared->cf_handles[CAIRN_CF_INODE],
                               (const char *) &inode->inum, sizeof(inode->inum),
                               (const char *) inode, sizeof(*inode), &err);

    chimera_cairn_abort_if(err, "Error putting root inode: %s\n", err);
} /* cairn_put_inode */

static inline void
cairn_remove_dirent(
    struct cairn_thread     *thread,
    rocksdb_transaction_t   *txn,
    struct cairn_dirent_key *key)
{
    struct cairn_shared *shared = thread->shared;
    char                *err    = NULL;

    rocksdb_transaction_delete_cf(txn, shared->cf_handles[CAIRN_CF_DIRENT],
                                  (const char *) key, sizeof(*key), &err);

    chimera_cairn_abort_if(err, "Error deleting dirent: %s\n", err);
} /* cairn_remove_dirent */

static inline void
cairn_remove_inode(
    struct cairn_thread   *thread,
    rocksdb_transaction_t *txn,
    struct cairn_inode    *inode)
{
    struct cairn_shared *shared = thread->shared;
    char                *err    = NULL;

    rocksdb_transaction_delete_cf(txn, shared->cf_handles[CAIRN_CF_INODE],
                                  (const char *) &inode->inum, sizeof(inode->inum),
                                  &err);

    chimera_cairn_abort_if(err, "Error deleting inode: %s\n", err);
} /* cairn_remove_inode */

static inline void
cairn_remove_symlink_target(
    struct cairn_thread   *thread,
    rocksdb_transaction_t *txn,
    uint64_t               inum)
{
    struct cairn_shared *shared = thread->shared;
    char                *err    = NULL;

    rocksdb_transaction_delete_cf(txn, shared->cf_handles[CAIRN_CF_SYMLINK],
                                  (const char *) &inum, sizeof(inum),
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

    start_key.inum = dir_inum;
    start_key.hash = 0;

    iter = rocksdb_transaction_create_iterator_cf(txn, shared->read_options,
                                                  shared->cf_handles[CAIRN_CF_DIRENT]);

    rocksdb_iter_seek(iter, (const char *) &start_key, sizeof(start_key));

    while (rocksdb_iter_valid(iter)) {
        dirent_key = (struct cairn_dirent_key *) rocksdb_iter_key(iter, &klen);

        if (dirent_key->inum != dir_inum) {
            break;
        }

        cairn_remove_dirent(thread, txn, dirent_key);
        rocksdb_iter_next(iter);
    }

    rocksdb_iter_destroy(iter);
} /* cairn_remove_directory_contents */

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

    start_key.inum   = file_inum;
    start_key.offset = 0;

    iter = rocksdb_transaction_create_iterator_cf(txn, shared->read_options,
                                                  shared->cf_handles[CAIRN_CF_EXTENT]);

    rocksdb_iter_seek(iter, (const char *) &start_key, sizeof(start_key));

    while (rocksdb_iter_valid(iter)) {
        extent_key = (struct cairn_extent_key *) rocksdb_iter_key(iter, &klen);

        if (extent_key->inum != file_inum) {
            break;
        }

        rocksdb_transaction_delete_cf(txn, shared->cf_handles[CAIRN_CF_EXTENT],
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
    rocksdb_transaction_t *txn;
    int                    initialize, i;
    struct timespec        now;
    char                  *err           = NULL;
    size_t                 meta_cache_mb = 64;
    size_t                 data_cache_mb = 64;
    rocksdb_cache_t       *meta_cache, *data_cache;
    int                    compression = 1; // Default to enabled

    cfg = json_load_file(cfgfile, 0, &json_error);

    chimera_cairn_abort_if(!cfg, "Failed to load config file: %s\n", json_error.text);

    db_path = json_string_value(json_object_get(cfg, "path"));

    // Get cache sizes and compression setting from config
    json_t *meta_cache_obj = json_object_get(cfg, "meta_cache");
    if (meta_cache_obj && json_is_integer(meta_cache_obj)) {
        meta_cache_mb = json_integer_value(meta_cache_obj);
    }

    json_t *data_cache_obj = json_object_get(cfg, "data_cache");
    if (data_cache_obj && json_is_integer(data_cache_obj)) {
        data_cache_mb = json_integer_value(data_cache_obj);
    }

    json_t *compression_obj = json_object_get(cfg, "compression");
    if (compression_obj && json_is_boolean(compression_obj)) {
        compression = json_boolean_value(compression_obj);
    }

    // Create LRU caches
    chimera_cairn_info("Creating LRU caches: meta_cache_mb=%zu data_cache_mb=%zu compression=%d",
                       meta_cache_mb, data_cache_mb, compression);
    meta_cache = rocksdb_cache_create_lru(meta_cache_mb * 1024 * 1024);
    data_cache = rocksdb_cache_create_lru(data_cache_mb * 1024 * 1024);

    shared->options = rocksdb_options_create();

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

    // Create table options for each column family
    rocksdb_block_based_table_options_t *meta_table_options = rocksdb_block_based_options_create();
    rocksdb_block_based_table_options_t *data_table_options = rocksdb_block_based_options_create();

    // Configure meta cache for all column families except extent
    rocksdb_block_based_options_set_block_cache(meta_table_options, meta_cache);

    // Configure data cache for extent column family
    rocksdb_block_based_options_set_block_cache(data_table_options, data_cache);

    for (i = 0; i < CAIRN_NUM_CF; i++) {
        shared->cf_options[i] = rocksdb_options_create();

        if (i == CAIRN_CF_EXTENT) {
            rocksdb_options_set_block_based_table_factory(shared->cf_options[i], data_table_options);

            if (compression) {
                rocksdb_options_set_compression(shared->cf_options[i], rocksdb_lz4_compression);
            } else {
                rocksdb_options_set_compression(shared->cf_options[i], rocksdb_no_compression);
            }

        } else {
            rocksdb_options_set_block_based_table_factory(shared->cf_options[i], meta_table_options);
            rocksdb_options_set_compression(shared->cf_options[i], rocksdb_no_compression);

        }
    }

    shared->db_txn = rocksdb_transactiondb_open_column_families(shared->options, shared->txndb_options, db_path,
                                                                CAIRN_NUM_CF, cairn_cf_names,
                                                                (const rocksdb_options_t * const *) shared->cf_options,
                                                                shared->cf_handles, &err);
    chimera_cairn_abort_if(err, "Failed to open database: %s\n", err);

    // Clean up table options
    rocksdb_block_based_options_destroy(meta_table_options);
    rocksdb_block_based_options_destroy(data_table_options);

    // Cache objects will be cleaned up when the DB is closed
    rocksdb_cache_destroy(meta_cache);
    rocksdb_cache_destroy(data_cache);

    json_decref(cfg);

    clock_gettime(CLOCK_REALTIME, &now);

    if (initialize) {

        inode.inum       = 2;
        inode.gen        = 1;
        inode.size       = 4096;
        inode.space_used = 4096;
        inode.gen        = 1;
        inode.refcnt     = 1;
        inode.uid        = 0;
        inode.gid        = 0;
        inode.nlink      = 2;
        inode.mode       = S_IFDIR | 0755;
        inode.atime      = now;
        inode.mtime      = now;
        inode.ctime      = now;


        txn = rocksdb_transaction_begin(shared->db_txn, shared->write_options, shared->txn_options, NULL);

        chimera_cairn_abort_if(err, "Error starting transaction: %s\n", err);

        rocksdb_transaction_put_cf(txn, shared->cf_handles[CAIRN_CF_INODE],
                                   (const char *) &inode.inum, sizeof(inode.inum),
                                   (const char *) &inode, sizeof(inode), &err);

        chimera_cairn_abort_if(err, "Error putting root inode: %s\n", err);

        rocksdb_transaction_commit(txn, &err);

        chimera_cairn_abort_if(err, "Error committing initialization transaction: %s\n", err);

        rocksdb_transaction_destroy(txn);

    }

    shared->root_fhlen = cairn_inum_to_fh(shared->root_fh, 2, 1);

    return shared;
} /* cairn_init */

static void
cairn_destroy(void *private_data)
{
    struct cairn_shared *shared = private_data;
    int                  i;

    for (i = 0; i < CAIRN_NUM_CF; i++) {
        rocksdb_column_family_handle_destroy(shared->cf_handles[i]);
        rocksdb_options_destroy(shared->cf_options[i]);
    }

    rocksdb_transactiondb_close(shared->db_txn);
    rocksdb_writeoptions_destroy(shared->write_options);
    rocksdb_readoptions_destroy(shared->read_options);
    rocksdb_options_destroy(shared->options);
    rocksdb_transactiondb_options_destroy(shared->txndb_options);
    rocksdb_transaction_options_destroy(shared->txn_options);
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
    struct chimera_vfs_attrs *attr,
    uint64_t                  mask,
    struct cairn_inode       *inode)
{
    attr->va_mask = 0;

    if (mask & CHIMERA_VFS_ATTR_FH) {
        attr->va_mask  |= CHIMERA_VFS_ATTR_FH;
        attr->va_fh_len = cairn_inum_to_fh(attr->va_fh, inode->inum, inode->gen);
    }

    if (mask & CHIMERA_VFS_ATTR_MASK_STAT) {
        attr->va_mask      |= CHIMERA_VFS_ATTR_MASK_STAT;
        attr->va_mode       = inode->mode;
        attr->va_nlink      = inode->nlink;
        attr->va_uid        = inode->uid;
        attr->va_gid        = inode->gid;
        attr->va_size       = inode->size;
        attr->va_space_used = (inode->space_used + 4095) & ~4095;
        attr->va_atime      = inode->atime;
        attr->va_mtime      = inode->mtime;
        attr->va_ctime      = inode->ctime;
        attr->va_ino        = inode->inum;
        attr->va_dev        = (42UL << 32) | 42;
        attr->va_rdev       = (42UL << 32) | 42;
    }
} /* cairn_map_attrs */

static rocksdb_transaction_t *
cairn_get_transaction(struct cairn_thread *thread)
{
    if (!thread->txn) {
        thread->txn = rocksdb_transaction_begin(thread->shared->db_txn, thread->shared->write_options, thread->shared->
                                                txn_options, NULL);
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
    uint32_t                  attr_mask = request->getattr.attr_mask;
    struct chimera_vfs_attrs *attr      = &request->getattr.r_attr;
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

    attr->va_mask = attr_mask;

    if (attr_mask & CHIMERA_VFS_ATTR_FH) {
        attr->va_fh_len = cairn_inum_to_fh(attr->va_fh, inode->inum,
                                           inode->gen);
    }

    cairn_map_attrs(attr, request->getattr.attr_mask, inode);

    cairn_inode_handle_release(&ih);

    request->status = CHIMERA_VFS_OK;

    evpl_defer(thread->evpl, &thread->commit);

    request->complete(request);
} /* cairn_getattr */

static void
cairn_setattr(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    rocksdb_transaction_t          *txn;
    struct cairn_inode_handle       ih;
    struct cairn_inode             *inode;
    const struct chimera_vfs_attrs *attr        = request->setattr.attr;
    struct chimera_vfs_attrs       *r_pre_attr  = &request->setattr.r_pre_attr;
    struct chimera_vfs_attrs       *r_post_attr = &request->setattr.r_post_attr;
    int                             rc;

    txn = cairn_get_transaction(thread);

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, 1, &ih);

    if (unlikely(rc)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;

    cairn_map_attrs(r_pre_attr, request->setattr.attr_mask, inode);

    if (attr->va_mask & CHIMERA_VFS_ATTR_MODE) {
        inode->mode = (inode->mode & S_IFMT) | (attr->va_mode & ~S_IFMT);
    }

    if (attr->va_mask & CHIMERA_VFS_ATTR_UID) {
        inode->uid = attr->va_uid;
    }

    if (attr->va_mask & CHIMERA_VFS_ATTR_GID) {
        inode->gid = attr->va_gid;
    }

    if (attr->va_mask & CHIMERA_VFS_ATTR_SIZE) {
        inode->size = attr->va_size;
    }

    if (attr->va_mask & CHIMERA_VFS_ATTR_ATIME) {
        if (attr->va_atime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
            inode->atime = request->start_time;
        } else {
            inode->atime = attr->va_atime;
        }
    }

    if (attr->va_mask & CHIMERA_VFS_ATTR_MTIME) {
        if (attr->va_mtime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
            inode->mtime = request->start_time;
        } else {
            inode->mtime = attr->va_mtime;
        }
    }

    inode->ctime = request->start_time;

    cairn_map_attrs(r_post_attr, request->setattr.attr_mask, inode);

    cairn_put_inode(thread, txn, inode);
    cairn_inode_handle_release(&ih);

    evpl_defer(thread->evpl, &thread->commit);

    request->status = CHIMERA_VFS_OK;

    DL_APPEND(thread->txn_requests, request);
} /* cairn_setattr */

static void
cairn_lookup_path(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    request->status = CHIMERA_VFS_OK;
    memcpy(request->lookup_path.r_attr.va_fh, shared->root_fh, shared->
           root_fhlen);
    request->lookup_path.r_attr.va_fh_len = shared->root_fhlen;
    request->lookup_path.r_attr.va_mask  |= CHIMERA_VFS_ATTR_FH;
    request->complete(request);
} /* cairn_lookup_path */

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

    dirent_key.inum = inode->inum;
    dirent_key.hash = XXH3_64bits(request->lookup.component, request->lookup.component_len);

    rc = cairn_dirent_get(thread, txn, &dirent_key, &dh);

    if (rc) {
        cairn_inode_handle_release(&ih);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    dirent_value = dh.dirent;

    if (request->lookup.attrmask) {
        cairn_map_attrs(&request->lookup.r_dir_attr,
                        request->lookup.attrmask,
                        inode);

        rc = cairn_inode_get_inum(thread, txn, dirent_value->inum, 0, &child_ih);

        if (rc == 0) {

            child = child_ih.inode;

            cairn_map_attrs(&request->lookup.r_attr,
                            request->lookup.attrmask,
                            child);
        } else {
            chimera_cairn_error("cairn_lookup: cairn_inode_get_inum failed\n");
        }

        cairn_inode_handle_release(&child_ih);
    }

    cairn_inode_handle_release(&ih);
    cairn_dirent_handle_release(&dh);

    evpl_defer(thread->evpl, &thread->commit);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* cairn_lookup */

static void
cairn_mkdir(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode_handle  parent_ih;
    struct cairn_inode        *parent_inode, inode;
    struct cairn_dirent_key    dirent_key;
    struct cairn_dirent_value  dirent_value;
    struct cairn_dirent_handle dh;
    struct chimera_vfs_attrs  *r_attr          = &request->mkdir.r_attr;
    struct chimera_vfs_attrs  *r_dir_pre_attr  = &request->mkdir.r_dir_pre_attr;
    struct chimera_vfs_attrs  *r_dir_post_attr = &request->mkdir.r_dir_post_attr;
    rocksdb_transaction_t     *txn;
    int                        rc;

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

    dirent_key.inum = parent_inode->inum;
    dirent_key.hash = XXH3_64bits(request->mkdir.name, request->mkdir.name_len);

    rc = cairn_dirent_get(thread, txn, &dirent_key, &dh);

    if (rc == 0) {
        cairn_inode_handle_release(&parent_ih);
        cairn_dirent_handle_release(&dh);
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        return;
    }

    cairn_alloc_inum(thread, &inode);
    inode.size       = 4096;
    inode.space_used = 4096;
    inode.uid        = 0;
    inode.gid        = 0;
    inode.nlink      = 2;
    inode.mode       = S_IFDIR | 0755;
    inode.atime      = request->start_time;
    inode.mtime      = request->start_time;
    inode.ctime      = request->start_time;

    cairn_map_attrs(r_attr, request->mkdir.attrmask, &inode);

    dirent_value.inum     = inode.inum;
    dirent_value.name_len = request->mkdir.name_len;
    memcpy(dirent_value.name, request->mkdir.name, request->mkdir.name_len);

    cairn_map_attrs(r_dir_pre_attr, request->mkdir.attrmask, parent_inode);

    parent_inode->nlink++;

    parent_inode->mtime = request->start_time;

    cairn_map_attrs(r_dir_post_attr, request->mkdir.attrmask, parent_inode);

    cairn_put_dirent(thread, txn, &dirent_key, &dirent_value);
    cairn_put_inode(thread, txn, parent_inode);
    cairn_put_inode(thread, txn, &inode);

    request->status = CHIMERA_VFS_OK;

    cairn_inode_handle_release(&parent_ih);

    evpl_defer(thread->evpl, &thread->commit);

    DL_APPEND(thread->txn_requests, request);
} /* cairn_mkdir */

static void
cairn_access(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    #if 0
    struct cairn_inode *inode;

    if (request->access.attrmask) {
        inode = cairn_inode_get_fh(shared, request->fh, request->fh_len);

        if (inode) {
            cairn_map_attrs(&request->access.r_attr, request->access.attrmask,
                            inode);

            pthread_mutex_unlock(&inode->lock);
        }
    }

    request->status          = CHIMERA_VFS_OK;
    request->access.r_access = request->access.access;
    request->complete(request);
    #endif /* if 0 */
} /* cairn_access */

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
    struct chimera_vfs_attrs  *r_pre_attr  = &request->remove.r_pre_attr;
    struct chimera_vfs_attrs  *r_post_attr = &request->remove.r_post_attr;
    int                        rc;

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

    dirent_key.inum = parent_inode->inum;
    dirent_key.hash = XXH3_64bits(request->remove.name, request->remove.namelen);

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

    if (S_ISDIR(inode->mode) && inode->nlink > 2) {
        cairn_inode_handle_release(&parent_ih);
        cairn_inode_handle_release(&child_ih);
        cairn_dirent_handle_release(&dh);
        request->status = CHIMERA_VFS_ENOTEMPTY;
        request->complete(request);
        return;
    }

    cairn_map_attrs(r_pre_attr, request->remove.attr_mask, parent_inode);

    parent_inode->mtime = request->start_time;

    if (S_ISDIR(inode->mode)) {
        inode->nlink = 0;
        parent_inode->nlink--;

        // Remove all directory entries
        cairn_remove_directory_contents(thread, txn, inode->inum);

    } else {
        inode->nlink--;
    }

    if (inode->nlink == 0) {
        --inode->refcnt;

        if (inode->refcnt == 0) {
            // Remove type-specific data before removing inode
            if (S_ISREG(inode->mode)) {
                cairn_remove_file_extents(thread, txn, inode->inum);
            } else if (S_ISLNK(inode->mode)) {
                cairn_remove_symlink_target(thread, txn, inode->inum);
            }

            cairn_remove_inode(thread, txn, inode);
        } else {
            cairn_put_inode(thread, txn, inode);
        }
    }

    cairn_map_attrs(r_post_attr, request->remove.attr_mask, parent_inode);

    cairn_remove_dirent(thread, txn, &dirent_key);

    cairn_put_inode(thread, txn, parent_inode);

    cairn_inode_handle_release(&parent_ih);
    cairn_inode_handle_release(&child_ih);
    cairn_dirent_handle_release(&dh);

    evpl_defer(thread->evpl, &thread->commit);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* cairn_remove */

static void
cairn_readdir(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    rocksdb_transaction_t     *txn;
    struct cairn_inode_handle  ih, dirent_ih;
    struct cairn_inode        *inode, *dirent_inode;
    uint64_t                   next_cookie = request->readdir.cookie;
    int                        rc, eof = 1;
    struct chimera_vfs_attrs   attr;
    rocksdb_iterator_t        *iter;
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

    start_key.inum = inode->inum;
    start_key.hash = request->readdir.cookie;

    iter = rocksdb_transaction_create_iterator_cf(txn, shared->read_options, shared->cf_handles[CAIRN_CF_DIRENT]);

    rocksdb_iter_seek(iter, (const char *) &start_key, sizeof(start_key));

    while (rocksdb_iter_valid(iter)) {

        dirent_key = (struct cairn_dirent_key *) rocksdb_iter_key(iter, &len);

        if (dirent_key->inum != inode->inum) {
            break;
        }

        dirent_value = (struct cairn_dirent_value *) rocksdb_iter_value(iter, &len);

        rc = cairn_inode_get_inum(thread, txn, dirent_value->inum, 0, &dirent_ih);

        if (rc) {
            continue;
        }

        dirent_inode = dirent_ih.inode;

        attr.va_mask   = CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT;
        attr.va_fh_len = cairn_inum_to_fh(attr.va_fh, dirent_inode->inum,
                                          dirent_inode->gen);

        cairn_map_attrs(&attr, request->readdir.attrmask, dirent_inode);

        cairn_inode_handle_release(&dirent_ih);

        next_cookie = dirent_key->hash;

        rc = request->readdir.callback(
            dirent_value->inum,
            dirent_key->hash,
            dirent_value->name,
            dirent_value->name_len,
            &attr,
            request->proto_private_data);

        if (rc) {
            eof = 0;
            break;
        }

        rocksdb_iter_next(iter);

    } /* cairn_readdir */

    rocksdb_iter_destroy(iter);


    if (request->readdir.attrmask & CHIMERA_VFS_ATTR_MASK_STAT) {
        cairn_map_attrs(&request->readdir.r_dir_attr, request->readdir.attrmask,
                        inode);
    }

    cairn_inode_handle_release(&ih);

    evpl_defer(thread->evpl, &thread->commit);

    request->status           = CHIMERA_VFS_OK;
    request->readdir.r_cookie = next_cookie;
    request->readdir.r_eof    = eof;

    request->complete(request);
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

    cairn_put_inode(thread, txn, inode);
    cairn_inode_handle_release(&ih);

    evpl_defer(thread->evpl, &thread->commit);

    request->open.r_vfs_private = (uint64_t) inode->inum;
    request->status             = CHIMERA_VFS_OK;

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
    unsigned int               flags           = request->open_at.flags;
    struct chimera_vfs_attrs  *r_attr          = &request->open_at.r_attr;
    struct chimera_vfs_attrs  *r_dir_pre_attr  = &request->open_at.r_dir_pre_attr;
    struct chimera_vfs_attrs  *r_dir_post_attr = &request->open_at.r_dir_post_attr;
    int                        rc, is_new_inode = 0;

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

    cairn_map_attrs(r_dir_pre_attr, request->open_at.attrmask, parent_inode);

    dirent_key.inum = parent_inode->inum;
    dirent_key.hash = XXH3_64bits(request->open_at.name, request->open_at.namelen);

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
        new_inode.mode       = S_IFREG |  0644;
        new_inode.atime      = request->start_time;
        new_inode.mtime      = request->start_time;
        new_inode.ctime      = request->start_time;

        new_dirent_value.inum     = new_inode.inum;
        new_dirent_value.name_len = request->open_at.namelen;
        memcpy(new_dirent_value.name, request->open_at.name, request->open_at.namelen);

        cairn_put_dirent(thread, txn, &dirent_key, &new_dirent_value);

        parent_inode->mtime = request->start_time;

        inode = &new_inode;
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

    cairn_map_attrs(r_dir_post_attr, request->open_at.attrmask, parent_inode);
    cairn_map_attrs(r_attr, request->open_at.attrmask, inode);

    cairn_put_inode(thread, txn, parent_inode);
    cairn_put_inode(thread, txn, inode);

    cairn_inode_handle_release(&parent_ih);

    if (!is_new_inode) {
        cairn_inode_handle_release(&child_ih);
    }

    evpl_defer(thread->evpl, &thread->commit);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
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


    if (inode->refcnt == 0) {
        cairn_remove_inode(thread, txn, inode);
    } else {
        cairn_put_inode(thread, txn, inode);
    }

    cairn_inode_handle_release(&ih);

    evpl_defer(thread->evpl, &thread->commit);

    request->open.r_vfs_private = (uint64_t) inode->inum;
    request->status             = CHIMERA_VFS_OK;

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

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, 0, &ih);

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

    request->read.r_niov = evpl_iovec_alloc(thread->evpl, length, 4096, 1, request->read.iov);
    iov                  = request->read.iov;

    start_key.inum   = inode->inum;
    start_key.offset = offset;

    iter = rocksdb_transaction_create_iterator_cf(txn, shared->read_options,
                                                  shared->cf_handles[CAIRN_CF_EXTENT]);

    rocksdb_iter_seek_for_prev(iter, (const char *) &start_key, sizeof(start_key));

    current_offset  = offset;
    bytes_remaining = length;

    while (bytes_remaining > 0 && rocksdb_iter_valid(iter)) {
        extent_key = (struct cairn_extent_key *) rocksdb_iter_key(iter, &klen);

        if (extent_key->inum != inode->inum) {
            break;
        }

        uint64_t extent_start = extent_key->offset;
        uint64_t extent_length;
        rocksdb_iter_value(iter, &extent_length);
        uint64_t extent_end = extent_start + extent_length;

        if (current_offset < extent_start) {
            memset(iov[0].data + current_offset - extent_start, 0, extent_start - current_offset);
            current_offset   = extent_start;
            bytes_remaining -= extent_start - current_offset;
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
        memset(iov[0].data + current_offset, 0, bytes_remaining);
    }

    rocksdb_iter_destroy(iter);

    inode->atime = request->start_time;

    if (request->read.attrmask) {
        cairn_map_attrs(&request->read.r_attr, request->read.attrmask, inode);
    }

    cairn_put_inode(thread, txn, inode);
    cairn_inode_handle_release(&ih);

    evpl_defer(thread->evpl, &thread->commit);

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
    rocksdb_transaction_t  *txn;
    rocksdb_iterator_t     *iter;
    struct cairn_extent_key start_key, *extent_key;
    uint64_t                hole_end    = offset + length;
    uint64_t                space_freed = 0;
    char                   *err         = NULL;
    size_t                  klen;

    txn = cairn_get_transaction(thread);

    start_key.inum   = inode->inum;
    start_key.offset = offset;

    iter = rocksdb_transaction_create_iterator_cf(txn, shared->read_options,
                                                  shared->cf_handles[CAIRN_CF_EXTENT]);

    // Find first extent less than or equal to our start offset
    rocksdb_iter_seek_for_prev(iter, (const char *) &start_key, sizeof(start_key));

    while (rocksdb_iter_valid(iter)) {
        extent_key = (struct cairn_extent_key *) rocksdb_iter_key(iter, &klen);
        uint64_t extent_length;
        void    *extent_data = (void *) rocksdb_iter_value(iter, &extent_length);

        // Stop if we've moved past this inode
        if (extent_key->inum != inode->inum) {
            break;
        }

        uint64_t extent_start = extent_key->offset;
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
            rocksdb_transaction_delete_cf(txn, shared->cf_handles[CAIRN_CF_EXTENT],
                                          (const char *) extent_key, sizeof(*extent_key),
                                          &err);
            chimera_cairn_abort_if(err, "Error deleting extent: %s\n", err);

            // If there's data before the hole, create a new extent
            if (extent_start < offset) {
                struct cairn_extent_key new_key = {
                    .inum   = inode->inum,
                    .offset = extent_start,
                };

                rocksdb_transaction_put_cf(txn, shared->cf_handles[CAIRN_CF_EXTENT],
                                           (const char *) &new_key, sizeof(new_key),
                                           extent_data, offset - extent_start,
                                           &err);
                chimera_cairn_abort_if(err, "Error putting extent: %s\n", err);

                // Add back space for the preserved portion
                space_freed -= offset - extent_start;
            }

            // If there's data after the hole, create a new extent
            if (extent_end > hole_end) {
                struct cairn_extent_key new_key = {
                    .inum   = inode->inum,
                    .offset = hole_end,
                };

                rocksdb_transaction_put_cf(txn, shared->cf_handles[CAIRN_CF_EXTENT],
                                           (const char *) &new_key, sizeof(new_key),
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
    struct chimera_vfs_attrs *r_pre_attr  = &request->write.r_pre_attr;
    struct chimera_vfs_attrs *r_post_attr = &request->write.r_post_attr;
    struct cairn_inode_handle ih;
    struct cairn_inode       *inode;
    uint64_t                  current_offset;
    uint64_t                  total_space = 0;
    char                     *err         = NULL;
    int                       rc, i;

    txn = cairn_get_transaction(thread);

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, 1, &ih);

    if (rc) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = ih.inode;

    cairn_map_attrs(r_pre_attr, request->write.attrmask, inode);

    // Clear any existing extents in the write range
    cairn_punch_hole(thread, shared, inode, request->write.offset, request->write.length);

    // Write each iovec as a new extent
    current_offset = request->write.offset;

    for (i = 0; i < request->write.niov; i++) {
        const struct evpl_iovec *iov = &request->write.iov[i];

        struct cairn_extent_key  key = {
            .inum   = inode->inum,
            .offset = current_offset,
        };

        // Store the extent with just the data
        rocksdb_transaction_put_cf(txn, shared->cf_handles[CAIRN_CF_EXTENT],
                                   (const char *) &key, sizeof(key),
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
    inode->mtime       = request->start_time;

    cairn_map_attrs(r_post_attr, request->write.attrmask, inode);

    cairn_put_inode(thread, txn, inode);
    cairn_inode_handle_release(&ih);

    evpl_defer(thread->evpl, &thread->commit);

    request->status         = CHIMERA_VFS_OK;
    request->write.r_length = request->write.length;
    request->write.r_sync   = 1;

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
    struct chimera_vfs_attrs *r_attr     = &request->symlink.r_attr;
    struct chimera_vfs_attrs *r_dir_attr = &request->symlink.r_dir_attr;
    char                     *err        = NULL;
    int                       rc;

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

    dirent_key.inum = parent_inode->inum;
    dirent_key.hash = XXH3_64bits(request->symlink.name, request->symlink.namelen);

    cairn_alloc_inum(thread, &new_inode);
    new_inode.size       = request->symlink.targetlen;
    new_inode.space_used = request->symlink.targetlen;
    new_inode.uid        = 0;
    new_inode.gid        = 0;
    new_inode.nlink      = 1;
    new_inode.mode       = S_IFLNK | 0755;
    new_inode.atime      = request->start_time;
    new_inode.mtime      = request->start_time;
    new_inode.ctime      = request->start_time;

    dirent_value.inum     = new_inode.inum;
    dirent_value.name_len = request->symlink.namelen;
    memcpy(dirent_value.name, request->symlink.name, request->symlink.namelen);

    parent_inode->mtime = request->start_time;

    cairn_map_attrs(r_attr, request->symlink.attrmask, &new_inode);
    cairn_map_attrs(r_dir_attr, request->symlink.attrmask, parent_inode);

    rocksdb_transaction_put_cf(txn, shared->cf_handles[CAIRN_CF_SYMLINK],
                               (const char *) &new_inode.inum, sizeof(new_inode.inum),
                               request->symlink.target, request->symlink.targetlen,
                               &err);

    chimera_cairn_abort_if(err, "Error putting symlink target: %s\n", err);

    cairn_put_dirent(thread, txn, &dirent_key, &dirent_value);
    cairn_put_inode(thread, txn, parent_inode);
    cairn_put_inode(thread, txn, &new_inode);

    cairn_inode_handle_release(&parent_ih);

    evpl_defer(thread->evpl, &thread->commit);

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

    rocksdb_pinnableslice_t *slice = rocksdb_transaction_get_pinned_cf(
        txn, shared->read_options,
        shared->cf_handles[CAIRN_CF_SYMLINK],
        (const char *) &inode->inum, sizeof(inode->inum),
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

    evpl_defer(thread->evpl, &thread->commit);

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
    int                        cmp, rc;

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

    old_dirent_key.inum = old_parent_inode->inum;
    old_dirent_key.hash = XXH3_64bits(request->rename.name, request->rename.namelen);

    rc = cairn_dirent_get(thread, txn, &old_dirent_key, &old_dh);
    if (rc) {
        if (cmp != 0) {
            cairn_inode_handle_release(&old_parent_ih);
            cairn_inode_handle_release(&new_parent_ih);
        } else {
            cairn_inode_handle_release(&old_parent_ih);
        }
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    old_dirent_value = old_dh.dirent;

    new_dirent_key.inum = new_parent_inode->inum;
    new_dirent_key.hash = XXH3_64bits(request->rename.new_name, request->rename.new_namelen);

    rc = cairn_dirent_get(thread, txn, &new_dirent_key, &new_dh);
    if (rc == 0) {
        // Target exists - need to remove it
        struct cairn_inode_handle existing_ih;
        struct cairn_inode       *existing_inode;

        rc = cairn_inode_get_inum(thread, txn, new_dh.dirent->inum, 1, &existing_ih);
        if (rc == 0) {
            existing_inode = existing_ih.inode;
            existing_inode->nlink--;

            if (existing_inode->nlink == 0) {
                cairn_remove_inode(thread, txn, existing_inode);
            } else {
                cairn_put_inode(thread, txn, existing_inode);
            }

            cairn_inode_handle_release(&existing_ih);
        }
        cairn_dirent_handle_release(&new_dh);
    }

    // Get the target inode to update its ctime
    rc = cairn_inode_get_inum(thread, txn, old_dirent_value->inum, 1, &target_ih);
    if (rc == 0) {
        target_inode        = target_ih.inode;
        target_inode->ctime = request->start_time;
        cairn_put_inode(thread, txn, target_inode);
        cairn_inode_handle_release(&target_ih);
    }

    // Create new dirent
    new_dirent_value.inum     = old_dirent_value->inum;
    new_dirent_value.name_len = request->rename.new_namelen;
    memcpy(new_dirent_value.name, request->rename.new_name, request->rename.new_namelen);

    // Update directory entries and parent inodes
    cairn_remove_dirent(thread, txn, &old_dirent_key);
    cairn_put_dirent(thread, txn, &new_dirent_key, &new_dirent_value);

    old_parent_inode->mtime = request->start_time;
    new_parent_inode->mtime = request->start_time;

    if (cmp != 0) {
        /* XXX only if dir */
        old_parent_inode->nlink--;
        new_parent_inode->nlink++;
    }

    cairn_put_inode(thread, txn, old_parent_inode);
    if (cmp != 0) {
        cairn_put_inode(thread, txn, new_parent_inode);
    }

    // Cleanup
    cairn_dirent_handle_release(&old_dh);
    if (cmp != 0) {
        cairn_inode_handle_release(&old_parent_ih);
        cairn_inode_handle_release(&new_parent_ih);
    } else {
        cairn_inode_handle_release(&old_parent_ih);
    }

    evpl_defer(thread->evpl, &thread->commit);

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

    dirent_key.inum = parent_inode->inum;
    dirent_key.hash = XXH3_64bits(request->link.name, request->link.namelen);

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
    target_inode->ctime = request->start_time;
    parent_inode->mtime = request->start_time;

    cairn_put_dirent(thread, txn, &dirent_key, &dirent_value);
    cairn_put_inode(thread, txn, parent_inode);
    cairn_put_inode(thread, txn, target_inode);

    cairn_inode_handle_release(&parent_ih);
    cairn_inode_handle_release(&target_ih);

    evpl_defer(thread->evpl, &thread->commit);

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
        case CHIMERA_VFS_OP_LOOKUP_PATH:
            cairn_lookup_path(thread, shared, request, private_data);
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
        case CHIMERA_VFS_OP_ACCESS:
            cairn_access(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKDIR:
            cairn_mkdir(thread, shared, request, private_data);
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

struct chimera_vfs_module vfs_cairn = {
    .name               = "cairn",
    .fh_magic           = CHIMERA_VFS_FH_MAGIC_CAIRN,
    .blocking           = 1,
    .path_open_required = 0,
    .file_open_required = 0,
    .init               = cairn_init,
    .destroy            = cairn_destroy,
    .thread_init        = cairn_thread_init,
    .thread_destroy     = cairn_thread_destroy,
    .dispatch           = cairn_dispatch,
};
