// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <utlist.h>

#include "common/varint.h"
#include "common/rbtree.h"

#include "vfs/vfs.h"
#include "memfs.h"
#include "common/logging.h"
#include "common/misc.h"
#include "common/macros.h"
#include "common/evpl_iovec_cursor.h"

#ifndef container_of
#define container_of(ptr, type, member) \
        ((type *) ((char *) (ptr) - offsetof(type, member)))
#endif /* ifndef container_of */

#define CHIMERA_MEMFS_BLOCK_MAX_IOV     4

#define CHIMERA_MEMFS_BLOCK_SHIFT       12
#define CHIMERA_MEMFS_BLOCK_SIZE        (1 << CHIMERA_MEMFS_BLOCK_SHIFT)
#define CHIMERA_MEMFS_BLOCK_MASK        (CHIMERA_MEMFS_BLOCK_SIZE - 1)

#define CHIMERA_MEMFS_INODE_LIST_SHIFT  8
#define CHIMERA_MEMFS_INODE_NUM_LISTS   (1 << CHIMERA_MEMFS_INODE_LIST_SHIFT)
#define CHIMERA_MEMFS_INODE_LIST_MASK   (CHIMERA_MEMFS_INODE_NUM_LISTS - 1)


#define CHIMERA_MEMFS_INODE_BLOCK_SHIFT 10
#define CHIMERA_MEMFS_INODE_BLOCK       (1 << CHIMERA_MEMFS_INODE_BLOCK_SHIFT)
#define CHIMERA_MEMFS_INODE_BLOCK_MASK  (CHIMERA_MEMFS_INODE_BLOCK - 1)

#define chimera_memfs_debug(...) chimera_debug("memfs", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)
#define chimera_memfs_info(...)  chimera_info("memfs", \
                                              __FILE__, \
                                              __LINE__, \
                                              __VA_ARGS__)
#define chimera_memfs_error(...) chimera_error("memfs", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)
#define chimera_memfs_fatal(...) chimera_fatal("memfs", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)
#define chimera_memfs_abort(...) chimera_abort("memfs", \
                                               __FILE__, \
                                               __LINE__, \
                                               __VA_ARGS__)

#define chimera_memfs_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "memfs", __FILE__, __LINE__, __VA_ARGS__)

#define chimera_memfs_abort_if(cond, ...) \
        chimera_abort_if(cond, "memfs", __FILE__, __LINE__, __VA_ARGS__)

struct memfs_block {
    struct memfs_thread *owner;
    uint64_t             key;
    int                  niov;
    struct memfs_block  *next;
    struct evpl_iovec    iov[CHIMERA_MEMFS_BLOCK_MAX_IOV];
};

struct memfs_dirent {
    uint64_t             inum;
    uint32_t             gen;
    uint32_t             name_len;
    uint64_t             hash;
    struct rb_node       node;
    struct memfs_dirent *next;
    char                 name[256];
};

struct memfs_symlink_target {
    int                          length;
    char                         data[4096];
    struct memfs_symlink_target *next;
};

struct memfs_inode {
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
    struct memfs_inode *next;

    pthread_mutex_t     lock;

    union {
        struct {
            struct rb_tree dirents;
        } dir;
        struct {
            struct memfs_block **blocks;
            unsigned int         num_blocks;
            unsigned int         max_blocks;
        } file;
        struct {
            struct memfs_symlink_target *target;
        } symlink;
    };
};

struct memfs_inode_list {
    uint32_t             id;
    uint32_t             num_blocks;
    uint32_t             max_blocks;
    struct memfs_inode **inode;
    struct memfs_inode  *free_inode;
    pthread_mutex_t      lock;
};

struct memfs_shared {
    struct memfs_inode_list *inode_list;
    int                      num_inode_list;
    int                      num_active_threads;
    uint8_t                  root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                 root_fhlen;
    pthread_mutex_t          lock;
};

struct memfs_thread {
    struct evpl                 *evpl;
    struct memfs_shared         *shared;
    struct evpl_iovec            zero;
    int                          thread_id;
    struct memfs_dirent         *free_dirent;
    struct memfs_symlink_target *free_symlink_target;
    struct memfs_block          *free_block;
};

static inline uint32_t
memfs_inum_to_fh(
    uint8_t *fh,
    uint64_t inum,
    uint32_t gen)
{
    uint8_t *ptr = fh;

    *ptr++ = CHIMERA_VFS_FH_MAGIC_MEMFS;

    ptr += chimera_encode_uint64(inum, ptr);
    ptr += chimera_encode_uint32(gen, ptr);

    return ptr - fh;
} /* memfs_inum_to_fh */

static inline void
memfs_fh_to_inum(
    uint64_t      *inum,
    uint32_t      *gen,
    const uint8_t *fh,
    int            fhlen)
{
    const uint8_t *ptr = fh;

    ptr++;

    ptr += chimera_decode_uint64(ptr, inum);
    chimera_decode_uint32(ptr, gen);
} /* memfs_fh_to_inum */

static inline struct memfs_inode *
memfs_inode_get_inum(
    struct memfs_shared *shared,
    uint64_t             inum,
    uint32_t             gen)
{
    uint64_t                 inum_block;
    uint32_t                 list_id, block_id, block_index;
    struct memfs_inode_list *inode_list;
    struct memfs_inode      *inode;

    list_id     = inum & CHIMERA_MEMFS_INODE_LIST_MASK;
    inum_block  = inum >> CHIMERA_MEMFS_INODE_LIST_SHIFT;
    block_index = inum_block & CHIMERA_MEMFS_INODE_BLOCK_MASK;
    block_id    = inum_block >> CHIMERA_MEMFS_INODE_BLOCK_SHIFT;

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
} /* memfs_inode_get_inum */

static inline struct memfs_inode *
memfs_inode_get_fh(
    struct memfs_shared *shared,
    const uint8_t       *fh,
    int                  fhlen)
{
    uint64_t inum;
    uint32_t gen;

    memfs_fh_to_inum(&inum, &gen, fh, fhlen);

    return memfs_inode_get_inum(shared, inum, gen);
} /* memfs_inode_get_fh */

static inline struct memfs_block *
memfs_block_alloc(struct memfs_thread *thread)
{
    struct memfs_block *block;

    block = thread->free_block;

    if (block) {
        LL_DELETE(thread->free_block, block);
    } else {
        block        = malloc(sizeof(*block));
        block->owner = thread;
    }

    return block;
} /* memfs_block_alloc */

static inline void
memfs_block_free(
    struct memfs_thread *thread,
    struct memfs_block  *block)
{
    int i;

    for (i = 0; i < block->niov; i++) {
        evpl_iovec_release(&block->iov[i]);
    }

    LL_PREPEND(thread->free_block, block);
} /* memfs_block_free */

static inline struct memfs_symlink_target *
memfs_symlink_target_alloc(struct memfs_thread *thread)
{
    struct memfs_symlink_target *target;

    target = thread->free_symlink_target;

    if (target) {
        LL_DELETE(thread->free_symlink_target, target);
    } else {
        target = malloc(sizeof(*target));
    }

    return target;
} /* memfs_symlink_target_alloc */

static inline void
memfs_symlink_target_free(
    struct memfs_thread         *thread,
    struct memfs_symlink_target *target)
{
    LL_PREPEND(thread->free_symlink_target, target);
} /* memfs_symlink_target_free */


static inline struct memfs_inode *
memfs_inode_alloc(
    struct memfs_shared *shared,
    uint32_t             list_id)
{
    struct memfs_inode_list *inode_list;
    struct memfs_inode      *inodes, *inode, *last;
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

        inodes = malloc(CHIMERA_MEMFS_INODE_BLOCK * sizeof(*inodes));

        base_id = bi << CHIMERA_MEMFS_INODE_BLOCK_SHIFT;

        inode_list->inode[bi] = inodes;

        last = NULL;

        for (i = 0; i < CHIMERA_MEMFS_INODE_BLOCK; i++) {
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

} /* memfs_inode_alloc */

static inline struct memfs_inode *
memfs_inode_alloc_thread(struct memfs_thread *thread)
{
    struct memfs_shared *shared  = thread->shared;
    uint32_t             list_id = thread->thread_id &
        CHIMERA_MEMFS_INODE_LIST_MASK;

    return memfs_inode_alloc(shared, list_id);
} /* memfs_inode_alloc */

static inline void
memfs_inode_free(
    struct memfs_thread *thread,
    struct memfs_inode  *inode)
{
    struct memfs_shared     *shared = thread->shared;
    struct memfs_inode_list *inode_list;
    int                      i;
    struct memfs_block      *block;
    uint32_t                 list_id = thread->thread_id &
        CHIMERA_MEMFS_INODE_LIST_MASK;

    inode_list = &shared->inode_list[list_id];

    if (S_ISREG(inode->mode)) {
        if (inode->file.blocks) {
            for (i = 0; i < inode->file.num_blocks; i++) {
                block = inode->file.blocks[i];
                if (block) {
                    memfs_block_free(thread, block);
                    inode->file.blocks[i] = NULL;
                }
            }
            free(inode->file.blocks);
            inode->file.blocks = NULL;
        }
    } else if (S_ISLNK(inode->mode)) {
        memfs_symlink_target_free(thread, inode->symlink.target);
        inode->symlink.target = NULL;
    }

    pthread_mutex_lock(&inode_list->lock);
    LL_PREPEND(inode_list->free_inode, inode);
    pthread_mutex_unlock(&inode_list->lock);
} /* memfs_inode_free */

static inline struct memfs_dirent *
memfs_dirent_alloc(
    struct memfs_thread *thread,
    uint64_t             inum,
    uint32_t             gen,
    uint64_t             hash,
    const char          *name,
    int                  name_len)
{
    struct memfs_dirent *dirent;

    dirent = thread->free_dirent;

    if (dirent) {
        LL_DELETE(thread->free_dirent, dirent);
    } else {
        dirent = malloc(sizeof(*dirent));
    }

    dirent->inum     = inum;
    dirent->gen      = gen;
    dirent->hash     = hash;
    dirent->name_len = name_len;
    memcpy(dirent->name, name, name_len);

    return dirent;

} /* memfs_dirent_alloc */

static inline void
memfs_dirent_free(
    struct memfs_thread *thread,
    struct memfs_dirent *dirent)
{
    LL_PREPEND(thread->free_dirent, dirent);
} /* memfs_dirent_free */

static void
memfs_dirent_release(
    struct rb_node *node,
    void           *private_data)
{
    struct memfs_thread *thread = private_data;
    struct memfs_dirent *dirent = container_of(node, struct memfs_dirent, node);

    if (thread) {
        memfs_dirent_free(thread, dirent);
    } else {
        free(dirent);
    }
} /* memfs_dirent_release */

static void *
memfs_init(const char *cfgfile)
{
    struct memfs_shared     *shared = calloc(1, sizeof(*shared));
    struct memfs_inode_list *inode_list;
    struct memfs_inode      *inode;
    int                      i;
    struct timespec          now;

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

    inode = memfs_inode_alloc(shared, 0);

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

    rb_tree_init(&inode->dir.dirents);

    shared->root_fhlen = memfs_inum_to_fh(shared->root_fh, inode->inum,
                                          inode->gen);

    return shared;
} /* memfs_init */

static void
memfs_destroy(void *private_data)
{
    struct memfs_shared *shared = private_data;
    struct memfs_inode  *inode;
    int                  i, j, k, bi, iovi;

    for (i = 0; i < shared->num_inode_list; i++) {
        for (j = 0; j < shared->inode_list[i].num_blocks; j++) {
            for (k = 0; k < CHIMERA_MEMFS_INODE_BLOCK; k++) {
                inode = &shared->inode_list[i].inode[j][k];

                if (inode->gen == 0 || inode->refcnt == 0) {
                    continue;
                }

                if (S_ISDIR(inode->mode)) {
                    rb_tree_destroy(&inode->dir.dirents, memfs_dirent_release, NULL);
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
    free(shared);
} /* memfs_destroy */

static void *
memfs_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct memfs_shared *shared = private_data;
    struct memfs_thread *thread = calloc(1, sizeof(*thread));

    evpl_iovec_alloc(evpl, 4096, 4096, 1, &thread->zero);

    thread->shared = shared;
    thread->evpl   = evpl;
    pthread_mutex_lock(&shared->lock);
    thread->thread_id = shared->num_active_threads++;
    pthread_mutex_unlock(&shared->lock);

    return thread;
} /* memfs_thread_init */

static void
memfs_thread_destroy(void *private_data)
{
    struct memfs_thread         *thread = private_data;
    struct memfs_dirent         *dirent;
    struct memfs_symlink_target *target;
    struct memfs_block          *block;

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
} /* memfs_thread_destroy */

static inline void
memfs_map_attrs(
    struct chimera_vfs_attrs *attr,
    struct memfs_inode       *inode)
{
    /* We always get attributes atomically with operations */
    attr->va_set_mask = CHIMERA_VFS_ATTR_ATOMIC;

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_FH) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_FH;
        attr->va_fh_len    = memfs_inum_to_fh(attr->va_fh, inode->inum, inode->gen)
        ;
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
        attr->va_rdev       = (42UL << 32) | 42;
    }

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_MASK_STATFS) {
        attr->va_set_mask      |= CHIMERA_VFS_ATTR_MASK_STATFS;
        attr->va_fs_space_avail = 0;
        attr->va_fs_space_free  = 0;
        attr->va_fs_space_total = 0;
        attr->va_fs_space_used  = 0;
        attr->va_fs_files_total = 0;
        attr->va_fs_files_free  = 0;
        attr->va_fs_files_avail = 0;
    }

} /* memfs_map_attrs */

static inline void
memfs_apply_attrs(
    struct memfs_inode       *inode,
    struct chimera_vfs_attrs *attr)
{
    struct timespec now;

    clock_gettime(CLOCK_REALTIME, &now);

    attr->va_set_mask = CHIMERA_VFS_ATTR_ATOMIC;

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_MODE) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
        inode->mode        = (inode->mode & S_IFMT) | (attr->va_mode & ~S_IFMT);
    }

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_UID) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_UID;
        inode->uid         = attr->va_uid;
    }

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_GID) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_GID;
        inode->gid         = attr->va_gid;
    }

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_SIZE) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_SIZE;
        inode->size        = attr->va_size;
    }

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_ATIME) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_ATIME;
        if (attr->va_atime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
            inode->atime = now;
        } else {
            inode->atime = attr->va_atime;
        }
    }

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_MTIME) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_MTIME;
        if (attr->va_mtime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
            inode->mtime = now;
        } else {
            inode->mtime = attr->va_mtime;
        }
    }

    inode->ctime = now;

} /* memfs_apply_attrs */

static void
memfs_getattr(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode *inode;

    inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    memfs_map_attrs(&request->getattr.r_attr, inode);

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_getattr */

static void
memfs_setattr(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode *inode;

    inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    memfs_map_attrs(&request->setattr.r_pre_attr, inode);

    memfs_apply_attrs(inode, request->setattr.set_attr);

    memfs_map_attrs(&request->setattr.r_post_attr, inode);

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_setattr */

static void
memfs_getrootfh(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode *inode;

    inode = memfs_inode_get_fh(shared, shared->root_fh, shared->root_fhlen);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    memfs_map_attrs(&request->getrootfh.r_attr, inode);

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_getrootfh */

static void
memfs_lookup(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode  *inode, *child;
    struct memfs_dirent *dirent;
    uint64_t             hash;

    hash = request->lookup.component_hash;

    inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    if (unlikely(!S_ISDIR(inode->mode))) {
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_ENOTDIR;
        request->complete(request);
        return;
    }

    rb_tree_query_exact(&inode->dir.dirents, hash, hash, dirent);

    if (!dirent) {
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    memfs_map_attrs(&request->lookup.r_dir_attr, inode);


    child = memfs_inode_get_inum(shared, dirent->inum, dirent->gen);

    if (unlikely(!child)) {
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    memfs_map_attrs(&request->lookup.r_attr, child);

    pthread_mutex_unlock(&child->lock);

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_lookup */

static void
memfs_mkdir(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode       *parent_inode, *inode, *existing_inode;
    struct memfs_dirent      *dirent, *existing_dirent;
    struct chimera_vfs_attrs *r_attr          = &request->mkdir.r_attr;
    struct chimera_vfs_attrs *r_dir_pre_attr  = &request->mkdir.r_dir_pre_attr;
    struct chimera_vfs_attrs *r_dir_post_attr = &request->mkdir.r_dir_post_attr;
    struct timespec           now;
    uint64_t                  hash;

    clock_gettime(CLOCK_REALTIME, &now);

    hash = request->mkdir.name_hash;

    /* Optimistically allocate an inode */
    inode = memfs_inode_alloc_thread(thread);

    inode->size       = 4096;
    inode->space_used = 4096;
    inode->uid        = 0;
    inode->gid        = 0;
    inode->nlink      = 2;
    inode->mode       = S_IFDIR | 0755;
    inode->atime      = now;
    inode->mtime      = now;
    inode->ctime      = now;

    rb_tree_init(&inode->dir.dirents);

    memfs_apply_attrs(inode, request->mkdir.set_attr);

    memfs_map_attrs(r_attr, inode);

    /* Optimistically allocate a dirent */
    dirent = memfs_dirent_alloc(thread,
                                inode->inum,
                                inode->gen,
                                hash,
                                request->mkdir.name,
                                request->mkdir.name_len);

    parent_inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!parent_inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        memfs_inode_free(thread, inode);
        memfs_dirent_free(thread, dirent);
        return;
    }

    if (!S_ISDIR(parent_inode->mode)) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_ENOTDIR;
        request->complete(request);
        memfs_inode_free(thread, inode);
        memfs_dirent_free(thread, dirent);
        return;
    }

    memfs_map_attrs(r_dir_pre_attr, parent_inode);

    rb_tree_query_exact(&parent_inode->dir.dirents, hash, hash, existing_dirent);

    if (existing_dirent) {

        existing_inode = memfs_inode_get_inum(shared, existing_dirent->inum, existing_dirent->gen);

        memfs_map_attrs(r_attr, existing_inode);
        memfs_map_attrs(r_dir_post_attr, parent_inode);

        pthread_mutex_unlock(&parent_inode->lock);
        pthread_mutex_unlock(&existing_inode->lock);

        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        memfs_inode_free(thread, inode);
        memfs_dirent_free(thread, dirent);
        return;
    }

    rb_tree_insert(&parent_inode->dir.dirents, hash, dirent);

    parent_inode->nlink++;

    parent_inode->mtime = now;

    memfs_map_attrs(r_dir_post_attr, parent_inode);

    pthread_mutex_unlock(&parent_inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_mkdir */

static void
memfs_remove(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode  *parent_inode, *inode;
    struct memfs_dirent *dirent;
    struct timespec      now;
    uint64_t             hash;

    clock_gettime(CLOCK_REALTIME, &now);

    hash = request->remove.name_hash;

    parent_inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!parent_inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    memfs_map_attrs(&request->remove.r_dir_pre_attr, parent_inode);

    if (!S_ISDIR(parent_inode->mode)) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    rb_tree_query_exact(&parent_inode->dir.dirents, hash, hash, dirent);

    if (!dirent) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = memfs_inode_get_inum(shared, dirent->inum, dirent->gen);

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

    if (S_ISDIR(inode->mode)) {
        parent_inode->nlink--;
    }
    parent_inode->mtime = now;

    rb_tree_remove(&parent_inode->dir.dirents, &dirent->node);

    if (S_ISDIR(inode->mode)) {
        inode->nlink = 0;
    } else {
        inode->nlink--;
    }

    if (inode->nlink == 0) {
        request->remove.r_removed_attr.va_req_mask = CHIMERA_VFS_ATTR_FH;
    }

    memfs_map_attrs(&request->remove.r_removed_attr, inode);

    if (inode->nlink == 0) {
        --inode->refcnt;

        if (inode->refcnt == 0) {
            memfs_inode_free(thread, inode);
        }
    }
    memfs_map_attrs(&request->remove.r_dir_post_attr, parent_inode);

    pthread_mutex_unlock(&parent_inode->lock);
    pthread_mutex_unlock(&inode->lock);

    memfs_dirent_free(thread, dirent);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_remove */

static void
memfs_readdir(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode      *inode, *dirent_inode;
    struct memfs_dirent     *dirent;
    uint64_t                 cookie      = request->readdir.cookie;
    uint64_t                 next_cookie = 0;
    int                      rc, eof = 1;
    struct chimera_vfs_attrs attr;

    inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

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

    attr.va_req_mask = request->readdir.attr_mask;

    if (cookie) {
        rb_tree_query_ceil(&inode->dir.dirents, cookie + 1, hash, dirent);
    } else {
        rb_tree_first(&inode->dir.dirents, dirent);
    }

    while (dirent) {

        dirent_inode = memfs_inode_get_inum(shared, dirent->inum, dirent->gen);

        if (!dirent_inode) {
            continue;
        }

        memfs_map_attrs(&attr, dirent_inode);

        pthread_mutex_unlock(&dirent_inode->lock);

        rc = request->readdir.callback(
            dirent->inum,
            dirent->hash,
            dirent->name,
            dirent->name_len,
            &attr,
            request->proto_private_data);

        if (rc) {
            eof = 0;
            break;
        }

        next_cookie = dirent->hash;

        dirent = rb_tree_next(&inode->dir.dirents, dirent);
    }

    memfs_map_attrs(&request->readdir.r_dir_attr, inode);

    pthread_mutex_unlock(&inode->lock);

    request->status           = CHIMERA_VFS_OK;
    request->readdir.r_cookie = next_cookie;
    request->readdir.r_eof    = eof;
    request->complete(request);
} /* memfs_readdir */

static void
memfs_open(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode *inode;

    inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

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
} /* memfs_open */

static void
memfs_open_at(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode  *parent_inode, *inode = NULL;
    struct memfs_dirent *dirent;
    unsigned int         flags = request->open_at.flags;
    struct timespec      now;
    uint64_t             hash;

    clock_gettime(CLOCK_REALTIME, &now);

    hash = request->open_at.name_hash;

    parent_inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

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

    memfs_map_attrs(&request->open_at.r_dir_pre_attr, parent_inode);

    rb_tree_query_exact(&parent_inode->dir.dirents, hash, hash, dirent);

    if (!dirent) {
        if (!(flags & CHIMERA_VFS_OPEN_CREATE)) {
            pthread_mutex_unlock(&parent_inode->lock);
            request->status = CHIMERA_VFS_ENOENT;
            request->complete(request);
            return;
        }

        inode = memfs_inode_alloc_thread(thread);

        pthread_mutex_lock(&inode->lock);

        inode->size            = 0;
        inode->space_used      = 0;
        inode->uid             = 0;
        inode->gid             = 0;
        inode->nlink           = 1;
        inode->mode            = S_IFREG |  0644;
        inode->atime           = now;
        inode->mtime           = now;
        inode->ctime           = now;
        inode->file.blocks     = NULL;
        inode->file.max_blocks = 0;
        inode->file.num_blocks = 0;

        memfs_apply_attrs(inode, request->open_at.set_attr);

        dirent = memfs_dirent_alloc(thread,
                                    inode->inum,
                                    inode->gen,
                                    hash,
                                    request->open_at.name,
                                    request->open_at.namelen);

        rb_tree_insert(&parent_inode->dir.dirents, hash, dirent);

        parent_inode->mtime = now;
    } else {
        inode = memfs_inode_get_inum(shared, dirent->inum, dirent->gen);

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

    memfs_map_attrs(&request->open_at.r_dir_post_attr, parent_inode);

    pthread_mutex_unlock(&parent_inode->lock);

    memfs_map_attrs(&request->open_at.r_attr, inode);

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* memfs_open_at */


static void
memfs_create_unlinked(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode *inode = NULL;
    struct timespec     now;

    clock_gettime(CLOCK_REALTIME, &now);

    inode = memfs_inode_alloc_thread(thread);

    inode->size            = 0;
    inode->space_used      = 0;
    inode->uid             = 0;
    inode->gid             = 0;
    inode->nlink           = 0;
    inode->mode            = S_IFREG |  0644;
    inode->atime           = now;
    inode->mtime           = now;
    inode->ctime           = now;
    inode->file.blocks     = NULL;
    inode->file.max_blocks = 0;
    inode->file.num_blocks = 0;

    inode->refcnt++;


    memfs_apply_attrs(inode, request->create_unlinked.set_attr);

    request->create_unlinked.r_vfs_private = (uint64_t) inode;

    memfs_map_attrs(&request->create_unlinked.r_attr, inode);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* memfs_open_at */

static void
memfs_close(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode *inode;

    inode = (struct memfs_inode *) request->close.vfs_private;

    pthread_mutex_lock(&inode->lock);

    --inode->refcnt;

    if (inode->refcnt == 0) {
        memfs_inode_free(thread, inode);
    }

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_close */

static void
memfs_read(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode      *inode;
    struct memfs_block      *block;
    struct evpl_iovec_cursor cursor;
    uint64_t                 offset, length;
    uint32_t                 eof = 0;
    uint64_t                 first_block, last_block, num_block, max_iov, bi;
    uint32_t                 block_offset, left, block_len;
    struct evpl_iovec       *iov;
    int                      niov = 0;
    struct timespec          now;

    clock_gettime(CLOCK_REALTIME, &now);

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

    if (request->read.handle->vfs_private) {
        inode = (struct memfs_inode *) request->read.handle->vfs_private;
        pthread_mutex_lock(&inode->lock);
    } else {
        inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

        if (unlikely(!inode)) {
            request->status = CHIMERA_VFS_ENOENT;
            request->complete(request);
            return;
        }

    }

    if (unlikely(inode->size <= offset)) {
        pthread_mutex_unlock(&inode->lock);
        request->status        = CHIMERA_VFS_OK;
        request->read.r_niov   = 0;
        request->read.r_length = 0;
        request->read.r_eof    = 1;
        request->complete(request);
        return;
    }

    if (offset + length > inode->size) {
        length = inode->size > offset ? inode->size - offset : 0;
        eof    = 1;
    }

    first_block  = offset >> CHIMERA_MEMFS_BLOCK_SHIFT;
    block_offset = offset & CHIMERA_MEMFS_BLOCK_MASK;
    last_block   = (offset + length - 1) >> CHIMERA_MEMFS_BLOCK_SHIFT;
    left         = length;

    num_block = last_block - first_block + 1;
    max_iov   = num_block * CHIMERA_MEMFS_BLOCK_MAX_IOV;

    iov = request->read.iov;

    for (bi = first_block; bi <= last_block; bi++) {

        if (left < CHIMERA_MEMFS_BLOCK_SIZE - block_offset) {
            block_len = left;
        } else {
            block_len = CHIMERA_MEMFS_BLOCK_SIZE - block_offset;
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

            niov += evpl_iovec_cursor_move(&cursor,
                                           &iov[niov],
                                           max_iov - niov,
                                           block_len, 1);
        }

        block_offset = 0;
        left        -= block_len;
    }

    inode->atime = now;

    memfs_map_attrs(&request->read.r_attr, inode);

    pthread_mutex_unlock(&inode->lock);

    request->status        = CHIMERA_VFS_OK;
    request->read.r_niov   = niov;
    request->read.r_length = length;
    request->read.r_eof    = eof;

    request->complete(request);
} /* memfs_read */

static void
memfs_write(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct evpl             *evpl = thread->evpl;
    struct memfs_inode      *inode;
    struct memfs_block     **blocks, *block, *old_block;
    struct evpl_iovec_cursor cursor, old_block_cursor;
    uint64_t                 first_block, last_block, bi;
    uint32_t                 block_offset, left, block_len;
    struct timespec          now;

    clock_gettime(CLOCK_REALTIME, &now);

    evpl_iovec_cursor_init(&cursor, request->write.iov, request->write.niov);

    first_block  = request->write.offset >> CHIMERA_MEMFS_BLOCK_SHIFT;
    block_offset = request->write.offset & CHIMERA_MEMFS_BLOCK_MASK;
    last_block   = (request->write.offset + request->write.length - 1) >>
        CHIMERA_MEMFS_BLOCK_SHIFT;
    left = request->write.length;

    if (request->write.handle->vfs_private) {
        inode = (struct memfs_inode *) request->write.handle->vfs_private;
        pthread_mutex_lock(&inode->lock);
    } else {
        inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

        if (unlikely(!inode)) {
            request->status = CHIMERA_VFS_ENOENT;
            request->complete(request);
            return;
        }
    }

    memfs_map_attrs(&request->write.r_pre_attr, inode);

    if (inode->file.max_blocks <= last_block) {

        blocks = inode->file.blocks;

        inode->file.max_blocks = 1024;

        while (inode->file.max_blocks <= last_block) {
            inode->file.max_blocks <<= 1;
        }

        inode->file.blocks = malloc(inode->file.max_blocks *
                                    sizeof(struct memfs_block *));

        memcpy(inode->file.blocks, blocks,
               inode->file.num_blocks * sizeof(struct memfs_block *));

        memset(inode->file.blocks + inode->file.num_blocks,
               0,
               (inode->file.max_blocks - inode->file.num_blocks) *
               sizeof(struct memfs_block *));

        free(blocks);
    }

    inode->file.num_blocks = last_block + 1;


    for (bi = first_block; bi <= last_block; bi++) {

        block_len = CHIMERA_MEMFS_BLOCK_SIZE - block_offset;

        if (left < block_len) {
            block_len = left;
        }

        old_block = inode->file.blocks[bi];

        block = memfs_block_alloc(thread);

        block->niov = evpl_iovec_alloc(evpl, 4096, 4096,
                                       CHIMERA_MEMFS_BLOCK_MAX_IOV,
                                       block->iov);

        if (block_offset || block_len < CHIMERA_MEMFS_BLOCK_SIZE) {

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
                                       CHIMERA_MEMFS_BLOCK_SIZE - block_len -
                                       block_offset);

                memfs_block_free(thread, old_block);
            } else {
                memset(block->iov[0].data, 0, block_offset);

                memset(block->iov[0].data + block_offset + block_len, 0,
                       CHIMERA_MEMFS_BLOCK_SIZE - block_offset - block_len);
            }
        }

        evpl_iovec_cursor_copy(&cursor,
                               block->iov[0].data + block_offset,
                               block_len);

        inode->file.blocks[bi] = block;
        block_offset           = 0;
        left                  -= block_len;
    }

    if (inode->size < request->write.offset + request->write.length) {
        inode->size       = request->write.offset + request->write.length;
        inode->space_used = (inode->size + 4095) & ~4095;
    }

    inode->mtime = now;

    memfs_map_attrs(&request->write.r_post_attr, inode);

    pthread_mutex_unlock(&inode->lock);

    request->status         = CHIMERA_VFS_OK;
    request->write.r_length = request->write.length;
    request->write.r_sync   = 1;

    request->complete(request);
} /* memfs_write */

static void
memfs_symlink(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode  *parent_inode, *inode;
    struct memfs_dirent *dirent, *existing_dirent;
    struct timespec      now;
    uint64_t             hash;

    clock_gettime(CLOCK_REALTIME, &now);

    hash = request->symlink.name_hash;

    /* Optimistically allocate an inode */
    inode = memfs_inode_alloc_thread(thread);

    inode->size       = request->symlink.targetlen;
    inode->space_used = request->symlink.targetlen;
    inode->uid        = 0;
    inode->gid        = 0;
    inode->nlink      = 1;
    inode->mode       = S_IFLNK | 0755;
    inode->atime      = now;
    inode->mtime      = now;
    inode->ctime      = now;

    inode->symlink.target = memfs_symlink_target_alloc(thread);

    inode->symlink.target->length = request->symlink.targetlen;
    memcpy(inode->symlink.target->data,
           request->symlink.target,
           request->symlink.targetlen);

    memfs_map_attrs(&request->symlink.r_attr, inode);

    /* Optimistically allocate a dirent */
    dirent = memfs_dirent_alloc(thread,
                                inode->inum,
                                inode->gen,
                                hash,
                                request->symlink.name,
                                request->symlink.namelen);

    parent_inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

    if (!parent_inode) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        memfs_inode_free(thread, inode);
        memfs_dirent_free(thread, dirent);
        return;
    }

    if (!S_ISDIR(parent_inode->mode)) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_ENOTDIR;
        request->complete(request);
        memfs_inode_free(thread, inode);
        memfs_dirent_free(thread, dirent);
        return;
    }

    rb_tree_query_exact(&parent_inode->dir.dirents, hash, hash, existing_dirent);

    if (existing_dirent) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        memfs_inode_free(thread, inode);
        memfs_dirent_free(thread, dirent);
        return;
    }

    memfs_map_attrs(&request->symlink.r_dir_pre_attr, parent_inode);

    rb_tree_insert(&parent_inode->dir.dirents, hash, dirent);

    parent_inode->mtime = now;

    memfs_map_attrs(&request->symlink.r_dir_post_attr, parent_inode);

    pthread_mutex_unlock(&parent_inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_symlink */

static void
memfs_readlink(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode *inode;

    inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

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
} /* memfs_readlink */

static inline int
memfs_fh_compare(
    const void *fha,
    int         fha_len,
    const void *fhb,
    int         fhb_len)
{
    int minlen = fha_len < fhb_len ? fha_len : fhb_len;

    return memcmp(fha, fhb, minlen);
} /* memfs_fh_compare */

static void
memfs_rename(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode  *old_parent_inode, *new_parent_inode, *child_inode;
    struct memfs_dirent *dirent, *old_dirent;
    int                  cmp;
    struct timespec      now;
    uint64_t             hash, new_hash;

    clock_gettime(CLOCK_REALTIME, &now);

    hash     = request->rename.name_hash;
    new_hash = request->rename.new_name_hash;

    cmp = memfs_fh_compare(request->fh,
                           request->fh_len,
                           request->rename.new_fh,
                           request->rename.new_fhlen);

    if (cmp == 0) {
        old_parent_inode = memfs_inode_get_fh(shared,
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
            old_parent_inode = memfs_inode_get_fh(shared,
                                                  request->fh,
                                                  request->fh_len);

            new_parent_inode = memfs_inode_get_fh(shared,
                                                  request->rename.new_fh,
                                                  request->rename.new_fhlen);
        } else {
            new_parent_inode = memfs_inode_get_fh(shared,
                                                  request->rename.new_fh,
                                                  request->rename.new_fhlen);
            old_parent_inode = memfs_inode_get_fh(shared,
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

    rb_tree_query_exact(&old_parent_inode->dir.dirents, hash, hash, old_dirent);

    if (!old_dirent) {
        pthread_mutex_unlock(&old_parent_inode->lock);
        pthread_mutex_unlock(&new_parent_inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    rb_tree_query_exact(&new_parent_inode->dir.dirents, new_hash, hash, dirent);

    if (dirent) {
        pthread_mutex_unlock(&old_parent_inode->lock);
        pthread_mutex_unlock(&new_parent_inode->lock);
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        return;
    }

    child_inode = memfs_inode_get_inum(shared, old_dirent->inum, old_dirent->gen);

    if (!child_inode) {
        pthread_mutex_unlock(&old_parent_inode->lock);
        pthread_mutex_unlock(&new_parent_inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    dirent = memfs_dirent_alloc(thread,
                                old_dirent->inum,
                                old_dirent->gen,
                                new_hash,
                                request->rename.new_name,
                                request->rename.new_namelen);

    rb_tree_insert(&new_parent_inode->dir.dirents, hash, dirent);

    rb_tree_remove(&old_parent_inode->dir.dirents, &old_dirent->node);

    if (S_ISDIR(child_inode->mode)) {
        old_parent_inode->nlink--;
        new_parent_inode->nlink++;
    }

    old_parent_inode->ctime = now;
    new_parent_inode->mtime = now;

    pthread_mutex_unlock(&child_inode->lock);

    if (cmp != 0) {
        pthread_mutex_unlock(&old_parent_inode->lock);
        pthread_mutex_unlock(&new_parent_inode->lock);
    } else {
        pthread_mutex_unlock(&old_parent_inode->lock);
    }

    memfs_dirent_free(thread, old_dirent);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* memfs_rename */

static void
memfs_link(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode  *parent_inode, *inode;
    struct memfs_dirent *dirent, *existing_dirent;
    struct timespec      now;
    uint64_t             hash;

    clock_gettime(CLOCK_REALTIME, &now);

    hash = request->link.name_hash;

    parent_inode = memfs_inode_get_fh(shared,
                                      request->link.dir_fh,
                                      request->link.dir_fhlen);

    if (!parent_inode) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    memfs_map_attrs(&request->link.r_dir_pre_attr, parent_inode);

    if (!S_ISDIR(parent_inode->mode)) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_ENOTDIR;
        request->complete(request);
        return;
    }

    inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

    if (!inode) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    if (unlikely(S_ISDIR(inode->mode))) {
        pthread_mutex_unlock(&parent_inode->lock);
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_EPERM;
        request->complete(request);
        return;
    }

    rb_tree_query_exact(&parent_inode->dir.dirents, hash, hash, existing_dirent);

    if (existing_dirent) {
        pthread_mutex_unlock(&parent_inode->lock);
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        return;
    }

    dirent = memfs_dirent_alloc(thread,
                                inode->inum,
                                inode->gen,
                                hash,
                                request->link.name,
                                request->link.namelen);

    rb_tree_insert(&parent_inode->dir.dirents, hash, dirent);

    inode->nlink++;

    inode->ctime        = now;
    parent_inode->mtime = now;

    memfs_map_attrs(&request->link.r_dir_post_attr, parent_inode);
    memfs_map_attrs(&request->link.r_attr, inode);

    pthread_mutex_unlock(&parent_inode->lock);
    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* memfs_link */

static void
memfs_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_thread *thread = private_data;
    struct memfs_shared *shared = thread->shared;

    switch (request->opcode) {
        case CHIMERA_VFS_OP_GETROOTFH:
            memfs_getrootfh(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LOOKUP:
            memfs_lookup(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            memfs_getattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            memfs_setattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKDIR:
            memfs_mkdir(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE:
            memfs_remove(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READDIR:
            memfs_readdir(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            memfs_open_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN:
            memfs_open(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_CREATE_UNLINKED:
            memfs_create_unlinked(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_CLOSE:
            memfs_close(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READ:
            memfs_read(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_WRITE:
            memfs_write(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_COMMIT:
            request->status = CHIMERA_VFS_OK;
            request->complete(request);
            break;
        case CHIMERA_VFS_OP_SYMLINK:
            memfs_symlink(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READLINK:
            memfs_readlink(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_RENAME:
            memfs_rename(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LINK:
            memfs_link(thread, shared, request, private_data);
            break;
        default:
            chimera_memfs_error("memfs_dispatch: unknown operation %d",
                                request->opcode);
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* memfs_dispatch */

SYMBOL_EXPORT struct chimera_vfs_module vfs_memfs = {
    .name           = "memfs",
    .fh_magic       = CHIMERA_VFS_FH_MAGIC_MEMFS,
    .capabilities   = CHIMERA_VFS_CAP_HANDLE_ALL | CHIMERA_VFS_CAP_CREATE_UNLINKED,
    .init           = memfs_init,
    .destroy        = memfs_destroy,
    .thread_init    = memfs_thread_init,
    .thread_destroy = memfs_thread_destroy,
    .dispatch       = memfs_dispatch,
};
