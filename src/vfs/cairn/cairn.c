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
    uint64_t      inum;
    XXH128_hash_t hash;
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
    struct evpl_iovec           zero;
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
cairn_inode_get_inum(
    struct cairn_thread       *thread,
    rocksdb_transaction_t     *txn,
    uint64_t                   inum,
    struct cairn_inode_handle *ih)
{
    struct cairn_shared *shared = thread->shared;
    char                *err    = NULL;
    size_t               len;

    ih->slice = rocksdb_transaction_get_pinned_cf(txn, shared->read_options,
                                                  shared->cf_handles[CAIRN_CF_INODE],
                                                  (const char *) &inum, sizeof(inum),
                                                  &err);

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
    struct cairn_inode_handle *ih)
{
    uint64_t inum;
    uint32_t gen;
    int      rc;

    cairn_fh_to_inum(&inum, &gen, fh, fhlen);

    rc = cairn_inode_get_inum(thread, txn, inum, ih);

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
    char                  *err = NULL;


    cfg = json_load_file(cfgfile, 0, &json_error);

    chimera_cairn_abort_if(!cfg, "Failed to load config file: %s\n", json_error.text);

    db_path = json_string_value(json_object_get(cfg, "path"));

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

    for (i = 0; i < CAIRN_NUM_CF; i++) {
        shared->cf_options[i] = rocksdb_options_create();
    }

    shared->db_txn = rocksdb_transactiondb_open_column_families(shared->options, shared->txndb_options, db_path,
                                                                CAIRN_NUM_CF, cairn_cf_names,
                                                                (const rocksdb_options_t * const *) shared->cf_options,
                                                                shared->cf_handles, &err);
    chimera_cairn_abort_if(err, "Failed to open database: %s\n", err);

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

    evpl_iovec_alloc(evpl, 4096, 4096, 1, &thread->zero);

    evpl_deferral_init(&thread->commit, cairn_thread_commit, thread);

    thread->shared = shared;
    thread->evpl   = evpl;
    pthread_mutex_lock(&shared->lock);
    thread->thread_id = shared->num_active_threads++;
    pthread_mutex_unlock(&shared->lock);

    thread->next_inum = 0;

    return thread;
} /* cairn_thread_init */

static void
cairn_thread_destroy(void *private_data)
{
    struct cairn_thread *thread = private_data;

    evpl_iovec_release(&thread->zero);

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
        attr->va_fh_len = cairn_inum_to_fh(attr->va_fh, inode->inum, inode->gen)
        ;
    }

    if (mask & CHIMERA_VFS_ATTR_MASK_STAT) {
        attr->va_mask      |= CHIMERA_VFS_ATTR_MASK_STAT;
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

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, &ih);

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
    #if 0
    struct cairn_inode             *inode;
    const struct chimera_vfs_attrs *attr        = request->setattr.attr;
    struct chimera_vfs_attrs       *r_pre_attr  = &request->setattr.r_pre_attr;
    struct chimera_vfs_attrs       *r_post_attr = &request->setattr.r_post_attr;

    inode = cairn_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }
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

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
    #endif /* if 0 */
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

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, &ih);

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
    dirent_key.hash = XXH3_128bits(request->lookup.component, request->lookup.component_len);

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

        rc = cairn_inode_get_inum(thread, txn, dirent_value->inum, &child_ih);

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

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, &parent_ih);

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
    dirent_key.hash = XXH3_128bits(request->mkdir.name, request->mkdir.name_len);

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
    request->complete(request);

    cairn_inode_handle_release(&parent_ih);

    evpl_defer(thread->evpl, &thread->commit);
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

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, &parent_ih);

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
    dirent_key.hash = XXH3_128bits(request->remove.name, request->remove.namelen);

    rc = cairn_dirent_get(thread, txn, &dirent_key, &dh);

    if (rc) {
        cairn_inode_handle_release(&parent_ih);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    dirent_value = dh.dirent;

    rc = cairn_inode_get_inum(thread, txn, dirent_value->inum, &child_ih);

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

    parent_inode->nlink--;
    parent_inode->mtime = request->start_time;

    if (S_ISDIR(inode->mode)) {
        inode->nlink = 0;
    } else {
        inode->nlink--;
    }

    if (inode->nlink == 0) {
        --inode->refcnt;

        if (inode->refcnt == 0) {
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
    #if 0
    struct cairn_inode      *inode, *dirent_inode;
    struct cairn_dirent     *dirent, *tmp;
    uint64_t                 cookie       = request->readdir.cookie;
    uint64_t                 next_cookie  = 0;
    int                      found_cookie = 0;
    int                      rc, eof = 1;
    struct chimera_vfs_attrs attr;

    if (cookie == 0) {
        found_cookie = 1;
    }

    inode = cairn_inode_get_fh(shared, request->fh, request->fh_len);

    if (!inode) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    if (!S_ISDIR(inode->mode)) {
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    HASH_ITER(hh, inode->dir.dirents, dirent, tmp)
    {
        if (dirent->inum == cookie) {
            found_cookie = 1;
        }

        if (!found_cookie) {
            continue;
        }

        attr.va_mask   = CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT;
        attr.va_fh_len = cairn_inum_to_fh(attr.va_fh, dirent->inum,
                                          dirent->gen);

        dirent_inode = cairn_inode_get_inum(shared, dirent->inum, dirent->gen);

        if (!dirent_inode) {
            continue;
        }

        attr.va_mode       = dirent_inode->mode;
        attr.va_nlink      = dirent_inode->nlink;
        attr.va_uid        = dirent_inode->uid;
        attr.va_gid        = dirent_inode->gid;
        attr.va_size       = dirent_inode->size;
        attr.va_space_used = dirent_inode->space_used;
        attr.va_atime      = dirent_inode->atime;
        attr.va_mtime      = dirent_inode->mtime;
        attr.va_ctime      = dirent_inode->ctime;
        attr.va_ino        = dirent_inode->inum;
        attr.va_dev        = (42UL << 32) | 42;
        attr.va_rdev       = (42UL << 32) | 42;

        pthread_mutex_unlock(&dirent_inode->lock);

        rc = request->readdir.callback(
            dirent->inum,
            dirent->inum,
            dirent->name,
            dirent->name_len,
            &attr,
            request->proto_private_data);

        if (rc) {
            eof = 0;
            break;
        }

        next_cookie = dirent->inum;
    }

    if (request->readdir.attrmask & CHIMERA_VFS_ATTR_MASK_STAT) {
        cairn_map_attrs(&request->readdir.r_dir_attr, request->readdir.attrmask,
                        inode);
    }

    pthread_mutex_unlock(&inode->lock);

    request->status           = CHIMERA_VFS_OK;
    request->readdir.r_cookie = next_cookie;
    request->readdir.r_eof    = eof;
    request->complete(request);
    #endif /* if 0 */
} /* cairn_readdir */

static void
cairn_open(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    #if 0
    struct cairn_inode *inode;

    inode = cairn_inode_get_fh(shared, request->fh, request->fh_len);

    if (!inode) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode->refcnt++;
    pthread_mutex_unlock(&inode->lock);

    request->open.r_vfs_private = (uint64_t) inode;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
    #endif /* if 0 */
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

    rc = cairn_inode_get_fh(thread, txn, request->fh, request->fh_len, &parent_ih);

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
    dirent_key.hash = XXH3_128bits(request->open_at.name, request->open_at.namelen);

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

        parent_inode->nlink++;
        parent_inode->mtime = request->start_time;

        inode = &new_inode;
    } else {

        dirent_value = dh.dirent;

        rc = cairn_inode_get_inum(thread, txn, dirent_value->inum, &child_ih);

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
    #if 0
    struct cairn_inode *inode;

    inode = (struct cairn_inode *) request->close.vfs_private;

    pthread_mutex_lock(&inode->lock);

    --inode->refcnt;

    if (inode->refcnt == 0) {
        cairn_inode_free(thread, inode);
    }

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
    #endif /* if 0 */
} /* cairn_close */

static void
cairn_read(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    #if 0
    struct evpl             *evpl = thread->evpl;
    struct cairn_inode      *inode;
    struct cairn_block      *block;
    struct evpl_iovec_cursor cursor;
    uint64_t                 offset, length;
    uint32_t                 eof = 0;
    uint64_t                 first_block, last_block, num_block, max_iov, bi;
    uint32_t                 block_offset, left, block_len;
    struct evpl_iovec       *iov;
    int                      niov = 0;

    offset = request->read.offset;
    length = request->read.length;

    if (unlikely(length == 0)) {
        request->status        = CHIMERA_VFS_OK;
        request->read.r_niov   = 0;
        request->read.r_length = length;
        request->read.r_eof    = eof;
        request->complete(request);
        return;
    }

    inode = cairn_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    if (offset + length > inode->size) {
        length = inode->size > offset ? inode->size - offset : 0;
        eof    = 1;
    }

    first_block  = offset >> CHIMERA_cairn_BLOCK_SHIFT;
    block_offset = offset & CHIMERA_cairn_BLOCK_MASK;
    last_block   = (offset + length - 1) >> CHIMERA_cairn_BLOCK_SHIFT;
    left         = length;

    num_block = last_block - first_block + 1;
    max_iov   = num_block * CHIMERA_cairn_BLOCK_MAX_IOV;

    iov = request->read.iov;

    for (bi = first_block; bi <= last_block; bi++) {

        if (left < CHIMERA_cairn_BLOCK_SIZE - block_offset) {
            block_len = left;
        } else {
            block_len = CHIMERA_cairn_BLOCK_SIZE - block_offset;
        }

        if (bi < inode->file.num_blocks) {
            block = inode->file.blocks[bi];
        } else {
            block = NULL;
        }

        if (!block) {
            iov[niov]        = thread->zero;
            iov[niov].length = block_len;
            evpl_iovec_addref(&iov[niov]);
            niov++;
        } else {

            evpl_iovec_cursor_init(&cursor, block->iov, block->niov);

            evpl_iovec_cursor_skip(&cursor, block_offset);

            niov += evpl_iovec_cursor_move(evpl,
                                           &cursor,
                                           &iov[niov],
                                           max_iov - niov,
                                           block_len);
        }

        block_offset = 0;
        left        -= block_len;
    }

    inode->atime = request->start_time;

    if (request->read.attrmask & CHIMERA_VFS_ATTR_MASK_STAT) {
        cairn_map_attrs(&request->read.r_attr, request->read.attrmask, inode);
    }

    pthread_mutex_unlock(&inode->lock);

    request->status        = CHIMERA_VFS_OK;
    request->read.r_niov   = niov;
    request->read.r_length = length;
    request->read.r_eof    = eof;

    request->complete(request);
    #endif /* if 0 */
} /* cairn_read */

static void
cairn_write(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    #if 0
    struct evpl              *evpl        = thread->evpl;
    struct chimera_vfs_attrs *r_pre_attr  = &request->write.r_pre_attr;
    struct chimera_vfs_attrs *r_post_attr = &request->write.r_post_attr;
    struct cairn_inode       *inode;
    struct cairn_block      **blocks, *block, *old_block;
    struct evpl_iovec_cursor  cursor, old_block_cursor;
    uint64_t                  first_block, last_block, bi;
    uint32_t                  block_offset, left, block_len;

    evpl_iovec_cursor_init(&cursor, request->write.iov, request->write.niov);

    first_block  = request->write.offset >> CHIMERA_cairn_BLOCK_SHIFT;
    block_offset = request->write.offset & CHIMERA_cairn_BLOCK_MASK;
    last_block   = (request->write.offset + request->write.length - 1) >>
        CHIMERA_cairn_BLOCK_SHIFT;
    left = request->write.length;

    inode = cairn_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    cairn_map_attrs(r_pre_attr, request->write.attrmask, inode);

    if (inode->file.max_blocks <= last_block) {

        blocks = inode->file.blocks;

        inode->file.max_blocks = 1024;

        while (inode->file.max_blocks <= last_block) {
            inode->file.max_blocks <<= 1;
        }

        inode->file.blocks = malloc(inode->file.max_blocks *
                                    sizeof(struct cairn_block *));

        memcpy(inode->file.blocks, blocks,
               inode->file.num_blocks * sizeof(struct cairn_block *));

        memset(inode->file.blocks + inode->file.num_blocks,
               0,
               (inode->file.max_blocks - inode->file.num_blocks) *
               sizeof(struct cairn_block *));
    }

    inode->file.num_blocks = last_block + 1;


    for (bi = first_block; bi <= last_block; bi++) {

        block_len = CHIMERA_cairn_BLOCK_SIZE - block_offset;

        if (left < block_len) {
            block_len = left;
        }

        old_block = inode->file.blocks[bi];

        block = cairn_block_alloc(thread);

        if (block_offset || block_len < CHIMERA_cairn_BLOCK_SIZE) {

            block->niov = evpl_iovec_alloc(evpl, 4096, 4096,
                                           CHIMERA_cairn_BLOCK_MAX_IOV,
                                           block->iov);

            chimera_cairn_abort_if(block->niov < 0,
                                   "evpl_iovec_alloc failed");
            if (old_block) {

                evpl_iovec_cursor_init(&old_block_cursor,
                                       old_block->iov,
                                       old_block->niov);
                evpl_iovec_cursor_copy(&old_block_cursor,
                                       block->iov[0].data,
                                       block_offset);

                evpl_iovec_cursor_skip(&old_block_cursor, block_len);

                evpl_iovec_cursor_copy(&old_block_cursor,
                                       block->iov[0].data + block_offset +
                                       block_len,
                                       CHIMERA_cairn_BLOCK_SIZE - block_len -
                                       block_offset);

                cairn_block_free(thread, old_block);
            } else {
                memset(block->iov[0].data, 0, block_offset);

                memset(block->iov[0].data + block_offset + block_len, 0,
                       CHIMERA_cairn_BLOCK_SIZE - block_offset - block_len);
            }

            evpl_iovec_cursor_copy(&cursor,
                                   block->iov[0].data + block_offset,
                                   block_len);
        } else {

            block->niov = evpl_iovec_cursor_move(evpl,
                                                 &cursor,
                                                 block->iov,
                                                 4,
                                                 block_len);
        }

        inode->file.blocks[bi] = block;
        block_offset           = 0;
        left                  -= block_len;
    }

    if (inode->size < request->write.offset + request->write.length) {
        inode->size       = request->write.offset + request->write.length;
        inode->space_used = (inode->size + 4095) & ~4095;
    }

    inode->mtime = request->start_time;

    cairn_map_attrs(r_post_attr, request->write.attrmask, inode);

    pthread_mutex_unlock(&inode->lock);

    request->status         = CHIMERA_VFS_OK;
    request->write.r_length = request->write.length;
    request->write.r_sync   = 1;

    request->complete(request);
    #endif /* if 0 */
} /* cairn_write */

static void
cairn_symlink(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    #if 0
    struct cairn_inode       *parent_inode, *inode;
    struct cairn_dirent      *dirent, *existing_dirent;
    struct chimera_vfs_attrs *r_attr     = &request->symlink.r_attr;
    struct chimera_vfs_attrs *r_dir_attr = &request->symlink.r_dir_attr;

    /* Optimistically allocate an inode */
    inode = cairn_inode_alloc_thread(thread);

    inode->size       = request->symlink.targetlen;
    inode->space_used = request->symlink.targetlen;
    inode->uid        = 0;
    inode->gid        = 0;
    inode->nlink      = 1;
    inode->mode       = S_IFLNK | 0755;
    inode->atime      = request->start_time;
    inode->mtime      = request->start_time;
    inode->ctime      = request->start_time;

    inode->symlink.target = cairn_symlink_target_alloc(thread);

    inode->symlink.target->length = request->symlink.targetlen;
    memcpy(inode->symlink.target->data,
           request->symlink.target,
           request->symlink.targetlen);

    cairn_map_attrs(r_attr, request->symlink.attrmask, inode);

    /* Optimistically allocate a dirent */
    dirent = cairn_dirent_alloc(thread,
                                inode->inum,
                                inode->gen,
                                request->symlink.name,
                                request->symlink.namelen);

    parent_inode = cairn_inode_get_fh(shared, request->fh, request->fh_len);

    if (!parent_inode) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        cairn_inode_free(thread, inode);
        cairn_dirent_free(thread, dirent);
        return;
    }

    if (!S_ISDIR(parent_inode->mode)) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_ENOTDIR;
        request->complete(request);
        cairn_inode_free(thread, inode);
        cairn_dirent_free(thread, dirent);
        return;
    }

    HASH_FIND(hh, parent_inode->dir.dirents,
              request->symlink.name, request->symlink.namelen,
              existing_dirent);

    if (existing_dirent) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        cairn_inode_free(thread, inode);
        cairn_dirent_free(thread, dirent);
        return;
    }

    HASH_ADD(hh, parent_inode->dir.dirents, name, dirent->name_len, dirent);

    parent_inode->nlink++;

    parent_inode->mtime = request->start_time;

    cairn_map_attrs(r_dir_attr, request->symlink.attrmask, parent_inode);

    pthread_mutex_unlock(&parent_inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
    #endif /* if 0 */
} /* cairn_symlink */

static void
cairn_readlink(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    #if 0
    struct cairn_inode *inode;

    inode = cairn_inode_get_fh(shared, request->fh, request->fh_len);

    if (!inode) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    request->readlink.r_target_length = inode->symlink.target->length;

    memcpy(request->readlink.r_target,
           inode->symlink.target->data,
           inode->symlink.target->length);

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;

    request->complete(request);
    #endif /* if 0 */
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
    #if 0
    struct cairn_inode  *old_parent_inode, *new_parent_inode;
    struct cairn_dirent *dirent, *old_dirent;
    int                  cmp;

    cmp = cairn_fh_compare(request->fh,
                           request->fh_len,
                           request->rename.new_fh,
                           request->rename.new_fhlen);

    if (cmp == 0) {
        old_parent_inode = cairn_inode_get_fh(shared,
                                              request->fh,
                                              request->fh_len);

        if (!old_parent_inode) {
            request->status = CHIMERA_VFS_ENOENT;
            request->complete(request);
            return;
        }

        if (!S_ISDIR(old_parent_inode->mode)) {
            pthread_mutex_unlock(&old_parent_inode->lock);
            request->status = CHIMERA_VFS_ENOTDIR;
            request->complete(request);
            return;
        }

        new_parent_inode = old_parent_inode;
    } else {
        if (cmp < 0) {
            old_parent_inode = cairn_inode_get_fh(shared,
                                                  request->fh,
                                                  request->fh_len);

            new_parent_inode = cairn_inode_get_fh(shared,
                                                  request->rename.new_fh,
                                                  request->rename.new_fhlen);
        } else {
            new_parent_inode = cairn_inode_get_fh(shared,
                                                  request->rename.new_fh,
                                                  request->rename.new_fhlen);
            old_parent_inode = cairn_inode_get_fh(shared,
                                                  request->fh,
                                                  request->fh_len);
        }

        if (!old_parent_inode) {
            request->status = CHIMERA_VFS_ENOENT;
            request->complete(request);
            return;
        }

        if (!S_ISDIR(old_parent_inode->mode)) {
            pthread_mutex_unlock(&old_parent_inode->lock);
            request->status = CHIMERA_VFS_ENOTDIR;
            request->complete(request);
            return;
        }

        if (!new_parent_inode) {
            request->status = CHIMERA_VFS_ENOENT;
            request->complete(request);
            return;
        }

        if (!S_ISDIR(new_parent_inode->mode)) {
            pthread_mutex_unlock(&new_parent_inode->lock);
            request->status = CHIMERA_VFS_ENOTDIR;
            request->complete(request);
            return;
        }
    }

    HASH_FIND(hh, old_parent_inode->dir.dirents,
              request->rename.name, request->rename.namelen,
              old_dirent);

    if (!old_dirent) {
        pthread_mutex_unlock(&old_parent_inode->lock);
        pthread_mutex_unlock(&new_parent_inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    HASH_FIND(hh, new_parent_inode->dir.dirents,
              request->rename.new_name, request->rename.new_namelen,
              dirent);

    if (dirent) {
        pthread_mutex_unlock(&old_parent_inode->lock);
        pthread_mutex_unlock(&new_parent_inode->lock);
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        return;
    }

    dirent = cairn_dirent_alloc(thread,
                                old_dirent->inum,
                                old_dirent->gen,
                                request->rename.new_name,
                                request->rename.new_namelen);

    HASH_ADD(hh, new_parent_inode->dir.dirents, name, dirent->name_len, dirent);

    old_parent_inode->nlink--;
    new_parent_inode->nlink++;

    old_parent_inode->ctime = request->start_time;
    new_parent_inode->mtime = request->start_time;

    if (cmp != 0) {
        pthread_mutex_unlock(&old_parent_inode->lock);
        pthread_mutex_unlock(&new_parent_inode->lock);
    } else {
        pthread_mutex_unlock(&old_parent_inode->lock);
    }

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
#endif /* if 0 */
} /* cairn_rename */

static void
cairn_link(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    #if 0
    struct cairn_inode  *parent_inode, *inode;
    struct cairn_dirent *dirent, *existing_dirent;


    parent_inode = cairn_inode_get_fh(shared,
                                      request->link.dir_fh,
                                      request->link.dir_fhlen);

    if (!parent_inode) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    if (!S_ISDIR(parent_inode->mode)) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_ENOTDIR;
        request->complete(request);
        return;
    }

    inode = cairn_inode_get_fh(shared, request->fh, request->fh_len);

    if (!inode) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    HASH_FIND(hh, parent_inode->dir.dirents,
              request->link.name, request->link.namelen,
              existing_dirent);

    if (existing_dirent) {
        pthread_mutex_unlock(&parent_inode->lock);
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        return;
    }

    dirent = cairn_dirent_alloc(thread,
                                inode->inum,
                                inode->gen,
                                request->link.name,
                                request->link.namelen);

    HASH_ADD(hh, parent_inode->dir.dirents, name, dirent->name_len, dirent);

    inode->nlink++;
    parent_inode->nlink++;

    inode->ctime        = request->start_time;
    parent_inode->mtime = request->start_time;

    pthread_mutex_unlock(&parent_inode->lock);
    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
#endif /* if 0 */
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
