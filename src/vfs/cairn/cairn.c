#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <rocksdb/c.h>
#include <jansson.h>

#include "common/varint.h"
#include "xxhash.h"

#define HASH_FUNCTION(keyptr, keylen, hashv) do { \
            hashv = XXH3_64bits(keyptr, keylen); \
} while (0)
#define HASH_KEY(keyptr, keylen)             keyptr, keylen

#include "uthash/utlist.h"
#include "uthash/uthash.h"
#include "vfs/vfs.h"
#include "cairn.h"
#include "common/logging.h"
#include "common/misc.h"
#include "evpl_iovec_cursor.h"

#define CAIRN_CF_DEFAULT                0
#define CAIRN_CF_INODE                  1
#define CAIRN_CF_DIRENT                 2
#define CAIRN_CF_SYMLINK                3
#define CAIRN_CF_EXTENT                 4
#define CAIRN_NUM_CF                    5

const char *cairn_cf_names[] = { "default", "inode", "dirent", "symlink", "extent" };



#define CHIMERA_cairn_BLOCK_MAX_IOV     4

#define CHIMERA_cairn_BLOCK_SHIFT       12
#define CHIMERA_cairn_BLOCK_SIZE        (1 << CHIMERA_cairn_BLOCK_SHIFT)
#define CHIMERA_cairn_BLOCK_MASK        (CHIMERA_cairn_BLOCK_SIZE - 1)

#define CHIMERA_cairn_INODE_LIST_SHIFT  8
#define CHIMERA_cairn_INODE_NUM_LISTS   (1 << CHIMERA_cairn_INODE_LIST_SHIFT)
#define CHIMERA_cairn_INODE_LIST_MASK   (CHIMERA_cairn_INODE_NUM_LISTS - 1)


#define CHIMERA_cairn_INODE_BLOCK_SHIFT 10
#define CHIMERA_cairn_INODE_BLOCK       (1 << CHIMERA_cairn_INODE_BLOCK_SHIFT)
#define CHIMERA_cairn_INODE_BLOCK_MASK  (CHIMERA_cairn_INODE_BLOCK - 1)

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

struct cairn_block {
    struct cairn_thread *owner;
    uint64_t             key;
    int                  niov;
    struct cairn_block  *next;
    UT_hash_handle       hh;
    struct evpl_iovec    iov[CHIMERA_cairn_BLOCK_MAX_IOV];

};

struct cairn_dirent {
    uint64_t             inum;
    uint32_t             gen;
    uint32_t             name_len;
    UT_hash_handle       hh;
    struct cairn_dirent *next;
    char                 name[256];
};

struct cairn_symlink_target {
    int                          length;
    char                         data[PATH_MAX];
    struct cairn_symlink_target *next;
};

struct cairn_inode {
    uint64_t            inum;
    uint32_t            gen;
    uint32_t            refcnt;
    uint64_t            size;
    uint64_t            space_used;
    uint32_t            mode;
    uint32_t            nlink;
    uint32_t            uid;
    uint32_t            gid;
    struct timespec     atime;
    struct timespec     mtime;
    struct timespec     ctime;
    struct cairn_inode *next;

    pthread_mutex_t     lock;

    union {
        struct {
            struct cairn_dirent *dirents;
        } dir;
        struct {
            struct cairn_block **blocks;
            unsigned int         num_blocks;
            unsigned int         max_blocks;
        } file;
        struct {
            struct cairn_symlink_target *target;
        } symlink;
    };
};

struct cairn_inode_list {
    uint32_t             id;
    uint32_t             num_blocks;
    uint32_t             max_blocks;
    struct cairn_inode **inode;
    struct cairn_inode  *free_inode;
    pthread_mutex_t      lock;
};

struct cairn_shared {
    rocksdb_t                       *db;
    rocksdb_transactiondb_t         *db_txn;
    rocksdb_options_t               *options;
    rocksdb_transactiondb_options_t *txn_options;
    rocksdb_writeoptions_t          *write_options;
    rocksdb_readoptions_t           *read_options;
    rocksdb_options_t               *cf_options[CAIRN_NUM_CF];
    rocksdb_column_family_handle_t  *cf_handles[CAIRN_NUM_CF];
    struct cairn_inode_list         *inode_list;
    int                              num_inode_list;
    int                              num_active_threads;
    uint8_t                          root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                         root_fhlen;
    pthread_mutex_t                  lock;
};

struct cairn_thread {
    struct evpl                 *evpl;
    struct cairn_shared         *shared;
    struct evpl_iovec            zero;
    int                          thread_id;
    struct cairn_dirent         *free_dirent;
    struct cairn_symlink_target *free_symlink_target;
    struct cairn_block          *free_block;
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

static inline struct cairn_inode *
cairn_inode_get_inum(
    struct cairn_shared *shared,
    uint64_t             inum,
    uint32_t             gen)
{
    uint64_t                 inum_block;
    uint32_t                 list_id, block_id, block_index;
    struct cairn_inode_list *inode_list;
    struct cairn_inode      *inode;

    list_id     = inum & CHIMERA_cairn_INODE_LIST_MASK;
    inum_block  = inum >> CHIMERA_cairn_INODE_LIST_SHIFT;
    block_index = inum_block & CHIMERA_cairn_INODE_BLOCK_MASK;
    block_id    = inum_block >> CHIMERA_cairn_INODE_BLOCK_SHIFT;

    if (unlikely(list_id >= shared->num_inode_list)) {
        return NULL;
    }

    inode_list = &shared->inode_list[list_id];

    if (unlikely(block_id >= inode_list->num_blocks)) {
        return NULL;
    }

    inode = &inode_list->inode[block_id][block_index];

    pthread_mutex_lock(&inode->lock);

    if (unlikely(inode->gen != gen)) {
        pthread_mutex_unlock(&inode->lock);
        return NULL;
    }

    return inode;
} /* cairn_inode_get_inum */

static inline struct cairn_inode *
cairn_inode_get_fh(
    struct cairn_shared *shared,
    const uint8_t       *fh,
    int                  fhlen)
{
    uint64_t inum;
    uint32_t gen;

    cairn_fh_to_inum(&inum, &gen, fh, fhlen);

    return cairn_inode_get_inum(shared, inum, gen);
} /* cairn_inode_get_fh */

static inline struct cairn_block *
cairn_block_alloc(struct cairn_thread *thread)
{
    struct cairn_block *block;

    block = thread->free_block;

    if (block) {
        LL_DELETE(thread->free_block, block);
    } else {
        block        = malloc(sizeof(*block));
        block->owner = thread;
    }

    return block;
} /* cairn_block_alloc */

static inline void
cairn_block_free(
    struct cairn_thread *thread,
    struct cairn_block  *block)
{
    int i;

    for (i = 0; i < block->niov; i++) {
        evpl_iovec_release(&block->iov[i]);
    }

    LL_PREPEND(thread->free_block, block);
} /* cairn_block_free */

static inline struct cairn_symlink_target *
cairn_symlink_target_alloc(struct cairn_thread *thread)
{
    struct cairn_symlink_target *target;

    target = thread->free_symlink_target;

    if (target) {
        LL_DELETE(thread->free_symlink_target, target);
    } else {
        target = malloc(sizeof(*target));
    }

    return target;
} /* cairn_symlink_target_alloc */

static inline void
cairn_symlink_target_free(
    struct cairn_thread         *thread,
    struct cairn_symlink_target *target)
{
    LL_PREPEND(thread->free_symlink_target, target);
} /* cairn_symlink_target_free */


static inline struct cairn_inode *
cairn_inode_alloc(
    struct cairn_shared *shared,
    uint32_t             list_id)
{
    struct cairn_inode_list *inode_list;
    struct cairn_inode      *inodes, *inode, *last;
    uint32_t                 bi, i, base_id, old_max_blocks;

    inode_list = &shared->inode_list[list_id];

    pthread_mutex_lock(&inode_list->lock);

    inode = inode_list->free_inode;

    if (!inode) {

        bi = inode_list->num_blocks++;

        if (bi >= inode_list->max_blocks) {

            if (inode_list->max_blocks == 0) {
                inode_list->max_blocks = 1024;

                inode_list->inode = calloc(inode_list->max_blocks,
                                           sizeof(*inode_list->inode));
            } else {
                old_max_blocks = inode_list->max_blocks;
                while (inode_list->max_blocks <= bi) {
                    inode_list->max_blocks *= 2;
                }

                inode_list->inode = realloc(inode_list->inode,
                                            inode_list->max_blocks *
                                            sizeof(*inode_list->inode));

                memset(inode_list->inode + old_max_blocks, 0,
                       (inode_list->max_blocks - old_max_blocks) *
                       sizeof(*inode_list->inode));
            }
        }

        inodes = malloc(CHIMERA_cairn_INODE_BLOCK * sizeof(*inodes));

        base_id = bi << CHIMERA_cairn_INODE_BLOCK_SHIFT;

        inode_list->inode[bi] = inodes;

        last = NULL;

        for (i = 0; i < CHIMERA_cairn_INODE_BLOCK; i++) {
            inode       = &inodes[i];
            inode->inum = (base_id + i) << 8 | list_id;
            pthread_mutex_init(&inode->lock, NULL);

            if (inode->inum) {
                /* Toss inode 0, we want non-zero inums */
                inode->next = last;
                last        = inode;
            }
        }
        inode_list->free_inode = last;

        inode = inode_list->free_inode;
    }

    LL_DELETE(inode_list->free_inode, inode);

    pthread_mutex_unlock(&inode_list->lock);

    inode->gen++;
    inode->refcnt = 1;
    inode->mode   = 0;

    return inode;

} /* cairn_inode_alloc */

static inline struct cairn_inode *
cairn_inode_alloc_thread(struct cairn_thread *thread)
{
    struct cairn_shared *shared  = thread->shared;
    uint32_t             list_id = thread->thread_id &
        CHIMERA_cairn_INODE_LIST_MASK;

    return cairn_inode_alloc(shared, list_id);
} /* cairn_inode_alloc */

static inline void
cairn_inode_free(
    struct cairn_thread *thread,
    struct cairn_inode  *inode)
{
    struct cairn_shared     *shared = thread->shared;
    struct cairn_inode_list *inode_list;
    int                      i;
    struct cairn_block      *block;
    uint32_t                 list_id = thread->thread_id &
        CHIMERA_cairn_INODE_LIST_MASK;

    inode_list = &shared->inode_list[list_id];

    if (S_ISREG(inode->mode)) {
        if (inode->file.blocks) {
            for (i = 0; i < inode->file.num_blocks; i++) {
                block = inode->file.blocks[i];
                if (block) {
                    cairn_block_free(thread, block);
                    inode->file.blocks[i] = NULL;
                }
            }
            free(inode->file.blocks);
            inode->file.blocks = NULL;
        }
    } else if (S_ISLNK(inode->mode)) {
        cairn_symlink_target_free(thread, inode->symlink.target);
        inode->symlink.target = NULL;
    }

    pthread_mutex_lock(&inode_list->lock);
    LL_PREPEND(inode_list->free_inode, inode);
    pthread_mutex_unlock(&inode_list->lock);
} /* cairn_inode_free */

static inline struct cairn_dirent *
cairn_dirent_alloc(
    struct cairn_thread *thread,
    uint64_t             inum,
    uint32_t             gen,
    const char          *name,
    int                  name_len)
{
    struct cairn_dirent *dirent;

    dirent = thread->free_dirent;

    if (dirent) {
        LL_DELETE(thread->free_dirent, dirent);
    } else {
        dirent = malloc(sizeof(*dirent));
    }

    dirent->inum     = inum;
    dirent->gen      = gen;
    dirent->name_len = name_len;
    memcpy(dirent->name, name, name_len);

    return dirent;

} /* cairn_dirent_alloc */

static inline void
cairn_dirent_free(
    struct cairn_thread *thread,
    struct cairn_dirent *dirent)
{
    LL_PREPEND(thread->free_dirent, dirent);
} /* cairn_dirent_free */

static void *
cairn_init(const char *cfgfile)
{
    struct cairn_shared     *shared = calloc(1, sizeof(*shared));
    json_t                  *cfg;
    json_error_t             json_error;
    const char              *db_path;
    struct cairn_inode_list *inode_list;
    struct cairn_inode      *inode;
    int                      i;
    struct timespec          now;
    char                    *err = NULL;


    cfg = json_load_file(cfgfile, 0, &json_error);

    chimera_cairn_abort_if(!cfg, "Failed to load config file: %s\n", json_error.text);

    db_path = json_string_value(json_object_get(cfg, "path"));

    shared->options = rocksdb_options_create();

    if (json_object_get(cfg, "initialize")) {
        rocksdb_options_set_create_if_missing(shared->options, 1);
        rocksdb_options_set_create_missing_column_families(shared->options, 1);
    }

    shared->txn_options   = rocksdb_transactiondb_options_create();
    shared->write_options = rocksdb_writeoptions_create();
    shared->read_options  = rocksdb_readoptions_create();
    for (i = 0; i < CAIRN_NUM_CF; i++) {
        shared->cf_options[i] = rocksdb_options_create();
    }

    shared->db_txn = rocksdb_transactiondb_open_column_families(shared->options, shared->txn_options, db_path,
                                                                CAIRN_NUM_CF, cairn_cf_names,
                                                                (const rocksdb_options_t * const *) shared->cf_options,
                                                                shared->cf_handles, &err);
    chimera_cairn_abort_if(err, "Failed to open database: %s\n", err);

    json_decref(cfg);

    clock_gettime(CLOCK_REALTIME, &now);

    pthread_mutex_init(&shared->lock, NULL);

    shared->num_inode_list = 255;
    shared->inode_list     = calloc(shared->num_inode_list,
                                    sizeof(*shared->inode_list));

    for (i = 0; i < shared->num_inode_list; i++) {

        inode_list = &shared->inode_list[i];

        inode_list->id         = i;
        inode_list->num_blocks = 0;
        inode_list->max_blocks = 0;

        pthread_mutex_init(&inode_list->lock, NULL);
    }

    inode_list = &shared->inode_list[0];

    inode = cairn_inode_alloc(shared, 0);

    inode->size       = 4096;
    inode->space_used = 4096;
    inode->gen        = 1;
    inode->refcnt     = 1;
    inode->uid        = 0;
    inode->gid        = 0;
    inode->nlink      = 2;
    inode->mode       = S_IFDIR | 0755;
    inode->atime      = now;
    inode->mtime      = now;
    inode->ctime      = now;

    shared->root_fhlen = cairn_inum_to_fh(shared->root_fh, inode->inum,
                                          inode->gen);

    return shared;
} /* cairn_init */

static void
cairn_destroy(void *private_data)
{
    struct cairn_shared *shared = private_data;
    struct cairn_inode  *inode;
    struct cairn_dirent *dirent;
    int                  i, j, k, bi, iovi;

    for (i = 0; i < shared->num_inode_list; i++) {
        for (j = 0; j < shared->inode_list[i].num_blocks; j++) {
            for (k = 0; k < CHIMERA_cairn_INODE_BLOCK; k++) {
                inode = &shared->inode_list[i].inode[j][k];

                if (inode->gen == 0 || inode->refcnt == 0) {
                    continue;
                }

                if (S_ISDIR(inode->mode)) {
                    while (inode->dir.dirents) {
                        dirent = inode->dir.dirents;
                        HASH_DELETE(hh, inode->dir.dirents, dirent);
                        free(dirent);
                    }
                } else if (S_ISLNK(inode->mode)) {
                    free(inode->symlink.target);
                } else if (S_ISREG(inode->mode)) {
                    for (bi = 0; bi < inode->file.num_blocks; bi++) {
                        if (inode->file.blocks[bi]) {
                            for (iovi = 0; iovi < inode->file.blocks[bi]->niov;
                                 iovi++) {
                                evpl_iovec_release(&inode->file.blocks[bi]->iov[
                                                       iovi]);
                            }
                            free(inode->file.blocks[bi]);
                        }
                    }

                    if (inode->file.blocks) {
                        free(inode->file.blocks);
                    }
                }
            }
            free(shared->inode_list[i].inode[j]);
        }
        free(shared->inode_list[i].inode);
    }

    pthread_mutex_destroy(&shared->lock);
    free(shared->inode_list);

    rocksdb_writeoptions_destroy(shared->write_options);
    rocksdb_readoptions_destroy(shared->read_options);
    for (int i = 0; i < CAIRN_NUM_CF; i++) {
        rocksdb_column_family_handle_destroy(shared->cf_handles[i]);
        rocksdb_options_destroy(shared->cf_options[i]);
    }
    rocksdb_transactiondb_close(shared->db_txn);
    rocksdb_options_destroy(shared->options);
    rocksdb_transactiondb_options_destroy(shared->txn_options);
    free(shared);
} /* cairn_destroy */

static void *
cairn_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct cairn_shared *shared = private_data;
    struct cairn_thread *thread = calloc(1, sizeof(*thread));

    evpl_iovec_alloc(evpl, 4096, 4096, 1, &thread->zero);

    thread->shared = shared;
    thread->evpl   = evpl;
    pthread_mutex_lock(&shared->lock);
    thread->thread_id = shared->num_active_threads++;
    pthread_mutex_unlock(&shared->lock);

    return thread;
} /* cairn_thread_init */

static void
cairn_thread_destroy(void *private_data)
{
    struct cairn_thread         *thread = private_data;
    struct cairn_dirent         *dirent;
    struct cairn_symlink_target *target;
    struct cairn_block          *block;

    evpl_iovec_release(&thread->zero);

    while (thread->free_dirent) {
        dirent = thread->free_dirent;

        LL_DELETE(thread->free_dirent, dirent);
        free(dirent);
    }

    while (thread->free_symlink_target) {
        target = thread->free_symlink_target;
        LL_DELETE(thread->free_symlink_target, target);
        free(target);
    }

    while (thread->free_block) {
        block = thread->free_block;
        LL_DELETE(thread->free_block, block);
        free(block);
    }

    free(thread);
} /* cairn_thread_destroy */

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

static void
cairn_getattr(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    uint32_t                  attr_mask = request->getattr.attr_mask;
    struct chimera_vfs_attrs *attr      = &request->getattr.r_attr;
    struct cairn_inode       *inode;

    inode = cairn_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    attr->va_mask = attr_mask;

    if (attr_mask & CHIMERA_VFS_ATTR_FH) {
        attr->va_fh_len = cairn_inum_to_fh(attr->va_fh, inode->inum,
                                           inode->gen);
    }

    cairn_map_attrs(attr, request->getattr.attr_mask, inode);

    pthread_mutex_unlock(&inode->lock);

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
    struct cairn_inode  *inode, *child;
    struct cairn_dirent *dirent;

    inode = cairn_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    if (unlikely(!S_ISDIR(inode->mode))) {
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    HASH_FIND(hh, inode->dir.dirents, request->lookup.component,
              request->lookup.component_len, dirent);

    if (!dirent) {
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    if (request->lookup.attrmask) {
        cairn_map_attrs(&request->lookup.r_dir_attr,
                        request->lookup.attrmask,
                        inode);

        child = cairn_inode_get_inum(shared, dirent->inum, dirent->gen);

        cairn_map_attrs(&request->lookup.r_attr,
                        request->lookup.attrmask,
                        child);

        pthread_mutex_unlock(&child->lock);
    }

    pthread_mutex_unlock(&inode->lock);

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
    struct cairn_inode       *parent_inode, *inode;
    struct cairn_dirent      *dirent, *existing_dirent;
    struct chimera_vfs_attrs *r_attr          = &request->mkdir.r_attr;
    struct chimera_vfs_attrs *r_dir_pre_attr  = &request->mkdir.r_dir_pre_attr;
    struct chimera_vfs_attrs *r_dir_post_attr = &request->mkdir.r_dir_post_attr;

    /* Optimistically allocate an inode */
    inode = cairn_inode_alloc_thread(thread);

    inode->size        = 4096;
    inode->space_used  = 4096;
    inode->uid         = 0;
    inode->gid         = 0;
    inode->nlink       = 2;
    inode->mode        = S_IFDIR | 0755;
    inode->atime       = request->start_time;
    inode->mtime       = request->start_time;
    inode->ctime       = request->start_time;
    inode->dir.dirents = NULL;

    cairn_map_attrs(r_attr, request->mkdir.attrmask, inode);

    /* Optimistically allocate a dirent */
    dirent = cairn_dirent_alloc(thread,
                                inode->inum,
                                inode->gen,
                                request->mkdir.name,
                                request->mkdir.name_len);

    parent_inode = cairn_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!parent_inode)) {
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

    cairn_map_attrs(r_dir_pre_attr, request->mkdir.attrmask, parent_inode);

    HASH_FIND(hh, parent_inode->dir.dirents,
              request->mkdir.name, request->mkdir.name_len,
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

    cairn_map_attrs(r_dir_post_attr, request->mkdir.attrmask, parent_inode);

    pthread_mutex_unlock(&parent_inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* cairn_mkdir */

static void
cairn_access(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
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
} /* cairn_access */

static void
cairn_remove(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode       *parent_inode, *inode;
    struct cairn_dirent      *dirent;
    struct chimera_vfs_attrs *r_pre_attr  = &request->remove.r_pre_attr;
    struct chimera_vfs_attrs *r_post_attr = &request->remove.r_post_attr;

    parent_inode = cairn_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!parent_inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    cairn_map_attrs(r_pre_attr, request->remove.attr_mask, parent_inode);

    if (!S_ISDIR(parent_inode->mode)) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    HASH_FIND(hh, parent_inode->dir.dirents,
              request->remove.name, request->remove.namelen,
              dirent);

    if (!dirent) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = cairn_inode_get_inum(shared, dirent->inum, dirent->gen);

    if (!inode) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    if (S_ISDIR(inode->mode) && inode->nlink > 2) {
        pthread_mutex_unlock(&parent_inode->lock);
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_ENOTEMPTY;
        request->complete(request);
        return;
    }

    parent_inode->nlink--;
    parent_inode->mtime = request->start_time;
    HASH_DEL(parent_inode->dir.dirents, dirent);

    if (S_ISDIR(inode->mode)) {
        inode->nlink = 0;
    } else {
        inode->nlink--;
    }

    if (inode->nlink == 0) {
        --inode->refcnt;

        if (inode->refcnt == 0) {
            cairn_inode_free(thread, inode);
        }
    }
    cairn_map_attrs(r_post_attr, request->remove.attr_mask, parent_inode);

    pthread_mutex_unlock(&parent_inode->lock);
    pthread_mutex_unlock(&inode->lock);

    cairn_dirent_free(thread, dirent);

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
} /* cairn_readdir */

static void
cairn_open(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
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
} /* cairn_open */

static void
cairn_open_at(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct cairn_inode       *parent_inode, *inode = NULL;
    struct cairn_dirent      *dirent;
    unsigned int              flags           = request->open_at.flags;
    struct chimera_vfs_attrs *r_attr          = &request->open_at.r_attr;
    struct chimera_vfs_attrs *r_dir_pre_attr  = &request->open_at.r_dir_pre_attr;
    struct chimera_vfs_attrs *r_dir_post_attr = &request->open_at.r_dir_post_attr;

    parent_inode = cairn_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!parent_inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    if (!S_ISDIR(parent_inode->mode)) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    cairn_map_attrs(r_dir_pre_attr, request->open_at.attrmask, parent_inode);

    HASH_FIND(hh, parent_inode->dir.dirents, request->open_at.name,
              request->open_at.namelen, dirent);

    if (!dirent) {
        if (!(flags & CHIMERA_VFS_OPEN_CREATE)) {
            pthread_mutex_unlock(&parent_inode->lock);
            request->status = CHIMERA_VFS_EEXIST;
            request->complete(request);
            return;
        }

        inode = cairn_inode_alloc_thread(thread);

        pthread_mutex_lock(&inode->lock);

        inode->size            = 0;
        inode->space_used      = 0;
        inode->uid             = 0;
        inode->gid             = 0;
        inode->nlink           = 1;
        inode->mode            = S_IFREG |  0644;
        inode->atime           = request->start_time;
        inode->mtime           = request->start_time;
        inode->ctime           = request->start_time;
        inode->file.blocks     = NULL;
        inode->file.max_blocks = 0;
        inode->file.num_blocks = 0;

        dirent = cairn_dirent_alloc(thread,
                                    inode->inum,
                                    inode->gen,
                                    request->open_at.name,
                                    request->open_at.namelen);

        HASH_ADD(hh, parent_inode->dir.dirents, name, dirent->name_len, dirent);

        parent_inode->nlink++;
        parent_inode->mtime = request->start_time;
    } else {
        inode = cairn_inode_get_inum(shared, dirent->inum, dirent->gen);

        if (!inode) {
            pthread_mutex_unlock(&parent_inode->lock);
            request->status = CHIMERA_VFS_ENOENT;
            request->complete(request);
            return;
        }
    }

    if (flags & CHIMERA_VFS_OPEN_INFERRED) {
        /* If this is an inferred open (ie an NFS3 create)
         * then we aren't returning a handle so we don't need
         * to increment the refcnt */

        request->open_at.r_vfs_private = 0xdeadbeefUL;

    } else {
        inode->refcnt++;
        request->open_at.r_vfs_private = (uint64_t) inode;
    }

    cairn_map_attrs(r_dir_post_attr, request->open_at.attrmask, parent_inode);

    pthread_mutex_unlock(&parent_inode->lock);

    cairn_map_attrs(r_attr, request->open_at.attrmask, inode);

    pthread_mutex_unlock(&inode->lock);

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
} /* cairn_close */

static void
cairn_read(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
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
} /* cairn_read */

static void
cairn_write(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
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
} /* cairn_write */

static void
cairn_symlink(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
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
} /* cairn_symlink */

static void
cairn_readlink(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
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

} /* cairn_rename */

static void
cairn_link(
    struct cairn_thread        *thread,
    struct cairn_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
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
