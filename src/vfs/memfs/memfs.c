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
#include <utlist.h>

#include "common/varint.h"
#include "common/rbtree.h"

#include "vfs/vfs.h"
#include "vfs/vfs_internal.h"
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
    uint64_t            rdev;
    struct timespec     atime;
    struct timespec     mtime;
    struct timespec     ctime;
    struct memfs_inode *next;

    pthread_mutex_t     lock;

    union {
        struct {
            struct rb_tree dirents;
            uint64_t       parent_inum;
            uint32_t       parent_gen;
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
    uint64_t                 fsid;
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

static inline void
memfs_fh_to_inum(
    uint64_t      *inum,
    uint32_t      *gen,
    const uint8_t *fh,
    int            fhlen)
{
    chimera_vfs_decode_fh_inum(fh, fhlen, inum, gen);
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
        block = malloc(sizeof(*block));

        if (!block) {
            return NULL;
        }

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
        evpl_iovec_release(thread->evpl, &block->iov[i]);
    }

    /* Clear niov to prevent stale access from iterating over freed iovecs */
    block->niov = 0;

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
        /* Reset block tracking fields to prevent stale state if inode is
         * reused or accessed via a stale handle */
        inode->file.num_blocks = 0;
        inode->file.max_blocks = 0;
    } else if (S_ISLNK(inode->mode)) {
        memfs_symlink_target_free(thread, inode->symlink.target);
        inode->symlink.target = NULL;
    }

    /* Increment generation so stale file handles return ESTALE */
    inode->gen++;

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

    /* Generate a random 64-bit filesystem ID */
    shared->fsid = chimera_rand64();

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

    /* Root directory's parent is itself */
    inode->dir.parent_inum = inode->inum;
    inode->dir.parent_gen  = inode->gen;

    /* Create 16-byte fsid buffer for root FH encoding (8-byte fsid + 8 bytes padding) */
    {
        uint8_t fsid_buf[CHIMERA_VFS_FSID_SIZE] = { 0 };
        memcpy(fsid_buf, &shared->fsid, sizeof(shared->fsid));
        shared->root_fhlen = chimera_vfs_encode_fh_inum_mount(fsid_buf,
                                                              inode->inum,
                                                              inode->gen,
                                                              shared->root_fh);
    }

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
                                evpl_iovec_release(NULL, &inode->file.blocks[bi]->iov[
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

    evpl_iovec_alloc(evpl, 4096, 4096, 1, 0, &thread->zero);

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

    evpl_iovec_release(thread->evpl, &thread->zero);

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
    struct memfs_shared      *shared,
    struct chimera_vfs_attrs *attr,
    struct memfs_inode       *inode,
    const void               *parent_fh)
{
    /* We always get attributes atomically with operations */
    attr->va_set_mask = CHIMERA_VFS_ATTR_ATOMIC;

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_FH) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_FH;
        attr->va_fh_len    = chimera_vfs_encode_fh_inum_parent(parent_fh, inode->inum, inode->gen, attr->va_fh);
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

} /* memfs_map_attrs */

static inline void
memfs_apply_attrs(
    struct memfs_inode       *inode,
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

    memfs_map_attrs(shared, &request->getattr.r_attr, inode, request->fh);

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
    struct memfs_inode       *inode;
    struct chimera_vfs_attrs *attr = request->setattr.set_attr;

    inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    memfs_map_attrs(shared, &request->setattr.r_pre_attr, inode, request->fh);

    /* Handle truncation: free blocks past new EOF and zero partial block */
    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) &&
        S_ISREG(inode->mode) &&
        attr->va_size < inode->size) {

        struct evpl *evpl           = thread->evpl;
        uint64_t     new_size       = attr->va_size;
        uint64_t     new_num_blocks = (new_size + CHIMERA_MEMFS_BLOCK_SIZE - 1) >>
            CHIMERA_MEMFS_BLOCK_SHIFT;
        uint64_t     bi;

        /* Free blocks that are entirely past the new EOF */
        if (inode->file.blocks) {
            for (bi = new_num_blocks; bi < inode->file.num_blocks; bi++) {
                if (inode->file.blocks[bi]) {
                    memfs_block_free(thread, inode->file.blocks[bi]);
                    inode->file.blocks[bi] = NULL;
                }
            }
        }

        /* Zero the partial region in the last block if EOF is not aligned.
         * We must allocate a new block and copy the retained portion because
         * readers may still be referencing the old block's iovecs. */
        if (new_size > 0 && (new_size & CHIMERA_MEMFS_BLOCK_MASK)) {
            uint64_t last_block_idx = (new_size - 1) >> CHIMERA_MEMFS_BLOCK_SHIFT;

            if (inode->file.blocks &&
                last_block_idx < inode->file.num_blocks &&
                inode->file.blocks[last_block_idx]) {

                struct memfs_block      *old_block = inode->file.blocks[last_block_idx];
                struct memfs_block      *new_block;
                struct evpl_iovec_cursor old_cursor;
                uint32_t                 offset_in_block = new_size &
                    CHIMERA_MEMFS_BLOCK_MASK;

                new_block = memfs_block_alloc(thread);

                if (!new_block) {
                    pthread_mutex_unlock(&inode->lock);
                    request->status = CHIMERA_VFS_ENOSPC;
                    request->complete(request);
                    return;
                }

                new_block->niov = evpl_iovec_alloc(evpl, 4096, 4096,
                                                   CHIMERA_MEMFS_BLOCK_MAX_IOV,
                                                   EVPL_IOVEC_FLAG_SHARED, new_block->iov);

                /* Copy the retained portion from the old block */
                evpl_iovec_cursor_init(&old_cursor, old_block->iov,
                                       old_block->niov);
                evpl_iovec_cursor_copy(&old_cursor, new_block->iov[0].data,
                                       offset_in_block);

                /* Zero the rest of the block */
                memset(new_block->iov[0].data + offset_in_block, 0,
                       CHIMERA_MEMFS_BLOCK_SIZE - offset_in_block);

                memfs_block_free(thread, old_block);

                /* Replace old block with new block */
                inode->file.blocks[last_block_idx] = new_block;
            }
        }

        /* Only update num_blocks if blocks array exists.
         * If blocks is NULL (sparse file extended via setattr), keep num_blocks=0. */
        if (inode->file.blocks) {
            inode->file.num_blocks = new_num_blocks;
            inode->space_used      = new_num_blocks * CHIMERA_MEMFS_BLOCK_SIZE;
        } else {
            inode->file.num_blocks = 0;
            inode->space_used      = 0;
        }
    }

    memfs_apply_attrs(inode, attr);

    memfs_map_attrs(shared, &request->setattr.r_post_attr, inode, request->fh);

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_setattr */

static inline struct memfs_inode *
memfs_lookup_path(
    struct memfs_thread *thread,
    struct memfs_shared *shared,
    const char          *path,
    int                  pathlen)
{
    struct memfs_inode  *parent, *inode;
    struct memfs_dirent *dirent;
    const char          *name;
    const char          *pathc = path;
    const char          *slash;
    int                  namelen;
    uint64_t             hash;

    inode = memfs_inode_get_fh(shared, shared->root_fh, shared->root_fhlen);

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

        rb_tree_query_exact(&inode->dir.dirents, hash, hash, dirent);

        if (!dirent) {
            pthread_mutex_unlock(&inode->lock);
            return NULL;
        }

        parent = inode;

        inode = memfs_inode_get_inum(shared, dirent->inum, dirent->gen);

        pthread_mutex_unlock(&parent->lock);

        if (!S_ISDIR(inode->mode)) {
            pthread_mutex_unlock(&inode->lock);
            return NULL;
        }

    }

    return inode;

} /* memfs_lookup_path */

static void
memfs_mount(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode       *inode;
    struct chimera_vfs_attrs *attr                            = &request->mount.r_attr;
    uint8_t                   fsid_buf[CHIMERA_VFS_FSID_SIZE] = { 0 };

    inode = memfs_lookup_path(thread, shared, request->mount.path, request->mount.pathlen);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    /* For MOUNT, encode FH using FSID (not parent FH) */
    memcpy(fsid_buf, &shared->fsid, sizeof(shared->fsid));

    attr->va_set_mask = CHIMERA_VFS_ATTR_ATOMIC | CHIMERA_VFS_ATTR_FH;
    attr->va_fh_len   = chimera_vfs_encode_fh_inum_mount(fsid_buf, inode->inum, inode->gen, attr->va_fh);

    /* Fill in other attrs if requested */
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
        attr->va_fs_space_avail = 0;
        attr->va_fs_space_free  = 0;
        attr->va_fs_space_total = 0;
        attr->va_fs_space_used  = 0;
        attr->va_fs_files_total = 0;
        attr->va_fs_files_free  = 0;
        attr->va_fs_files_avail = 0;
        attr->va_fsid           = shared->fsid;
    }

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_mount */


static void
memfs_umount(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{

    /* No action required */
    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_umount */

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
    const char          *name    = request->lookup.component;
    uint32_t             namelen = request->lookup.component_len;

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

    memfs_map_attrs(shared, &request->lookup.r_dir_attr, inode, request->fh);

    /* Handle "." - return the directory itself */
    if (namelen == 1 && name[0] == '.') {
        memfs_map_attrs(shared, &request->lookup.r_attr, inode, request->fh);
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    /* Handle ".." - return the parent directory */
    if (namelen == 2 && name[0] == '.' && name[1] == '.') {
        child = memfs_inode_get_inum(shared, inode->dir.parent_inum, inode->dir.parent_gen);
        if (unlikely(!child)) {
            pthread_mutex_unlock(&inode->lock);
            request->status = CHIMERA_VFS_ENOENT;
            request->complete(request);
            return;
        }
        memfs_map_attrs(shared, &request->lookup.r_attr, child, request->fh);
        pthread_mutex_unlock(&child->lock);
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_OK;
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

    child = memfs_inode_get_inum(shared, dirent->inum, dirent->gen);

    if (unlikely(!child)) {
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    memfs_map_attrs(shared, &request->lookup.r_attr, child, request->fh);

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

    memfs_map_attrs(shared, r_attr, inode, request->fh);

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

    /* Set parent pointer for .. lookup support */
    inode->dir.parent_inum = parent_inode->inum;
    inode->dir.parent_gen  = parent_inode->gen;

    memfs_map_attrs(shared, r_dir_pre_attr, parent_inode, request->fh);

    rb_tree_query_exact(
        &parent_inode->dir.dirents,
        hash,
        hash,
        existing_dirent);

    if (existing_dirent) {

        existing_inode = memfs_inode_get_inum(shared, existing_dirent->inum, existing_dirent->gen);

        memfs_map_attrs(shared, r_attr, existing_inode, request->fh);
        memfs_map_attrs(shared, r_dir_post_attr, parent_inode, request->fh);

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

    memfs_map_attrs(shared, r_dir_post_attr, parent_inode, request->fh);

    pthread_mutex_unlock(&parent_inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_mkdir */

static void
memfs_mknod(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode       *parent_inode, *inode, *existing_inode;
    struct memfs_dirent      *dirent, *existing_dirent;
    struct chimera_vfs_attrs *r_attr          = &request->mknod.r_attr;
    struct chimera_vfs_attrs *r_dir_pre_attr  = &request->mknod.r_dir_pre_attr;
    struct chimera_vfs_attrs *r_dir_post_attr = &request->mknod.r_dir_post_attr;
    struct timespec           now;
    uint64_t                  hash;

    clock_gettime(CLOCK_REALTIME, &now);

    hash = request->mknod.name_hash;

    /* Optimistically allocate an inode */
    inode = memfs_inode_alloc_thread(thread);

    inode->size       = 0;
    inode->space_used = 0;
    inode->uid        = 0;
    inode->gid        = 0;
    inode->nlink      = 1;
    inode->rdev       = 0;
    inode->atime      = now;
    inode->mtime      = now;
    inode->ctime      = now;

    /* The mode (including file type bits S_IFCHR/S_IFBLK/S_IFSOCK/S_IFIFO)
     * and rdev are set via set_attr by the caller */
    if (request->mknod.set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        inode->mode = request->mknod.set_attr->va_mode;
    } else {
        inode->mode = S_IFREG | 0644;
    }

    if (request->mknod.set_attr->va_set_mask & CHIMERA_VFS_ATTR_RDEV) {
        inode->rdev = request->mknod.set_attr->va_rdev;
    }

    memfs_apply_attrs(inode, request->mknod.set_attr);

    memfs_map_attrs(shared, r_attr, inode, request->fh);

    /* Optimistically allocate a dirent */
    dirent = memfs_dirent_alloc(thread,
                                inode->inum,
                                inode->gen,
                                hash,
                                request->mknod.name,
                                request->mknod.name_len);

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

    memfs_map_attrs(shared, r_dir_pre_attr, parent_inode, request->fh);

    rb_tree_query_exact(
        &parent_inode->dir.dirents,
        hash,
        hash,
        existing_dirent);

    if (existing_dirent) {

        existing_inode = memfs_inode_get_inum(shared, existing_dirent->inum, existing_dirent->gen);

        memfs_map_attrs(shared, r_attr, existing_inode, request->fh);
        memfs_map_attrs(shared, r_dir_post_attr, parent_inode, request->fh);

        pthread_mutex_unlock(&parent_inode->lock);
        pthread_mutex_unlock(&existing_inode->lock);

        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        memfs_inode_free(thread, inode);
        memfs_dirent_free(thread, dirent);
        return;
    }

    rb_tree_insert(&parent_inode->dir.dirents, hash, dirent);

    parent_inode->mtime = now;

    memfs_map_attrs(shared, r_dir_post_attr, parent_inode, request->fh);

    pthread_mutex_unlock(&parent_inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_mknod */

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

    memfs_map_attrs(shared, &request->remove.r_dir_pre_attr, parent_inode, request->fh);

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

    memfs_map_attrs(shared, &request->remove.r_removed_attr, inode, request->fh);

    if (inode->nlink == 0) {
        --inode->refcnt;

        if (inode->refcnt == 0) {
            memfs_inode_free(thread, inode);
        }
    }
    memfs_map_attrs(shared, &request->remove.r_dir_post_attr, parent_inode, request->fh);

    pthread_mutex_unlock(&parent_inode->lock);
    pthread_mutex_unlock(&inode->lock);

    memfs_dirent_free(thread, dirent);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_remove */

/*
 * Cookie values for readdir:
 *   0 = start of directory, will return "."
 *   1 = "." was returned, will return ".."
 *   2 = ".." was returned, will return first real entry
 *   3+ = real entry cookie (hash + 3)
 */
#define MEMFS_COOKIE_DOT    1
#define MEMFS_COOKIE_DOTDOT 2
#define MEMFS_COOKIE_FIRST  3

static void
memfs_readdir(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode      *inode, *dirent_inode, *parent_inode;
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

    /* Handle "." and ".." entries only if requested */
    if (request->readdir.flags & CHIMERA_VFS_READDIR_EMIT_DOT) {
        /* Handle "." entry (cookie 0 -> 1) */
        if (cookie < MEMFS_COOKIE_DOT) {
            memfs_map_attrs(shared, &attr, inode, request->fh);

            rc = request->readdir.callback(
                inode->inum,
                MEMFS_COOKIE_DOT,
                ".",
                1,
                &attr,
                request->proto_private_data);

            if (rc) {
                /* Caller wants to stop after this entry */
                next_cookie = MEMFS_COOKIE_DOT;
                eof         = 0;
                goto out;
            }

            cookie = MEMFS_COOKIE_DOT;
        }

        /* Handle ".." entry (cookie 1 -> 2) */
        if (cookie < MEMFS_COOKIE_DOTDOT) {
            /* Get parent inode for ".." attributes */
            /* Check if parent is the same inode (root directory case) to avoid deadlock */
            if (inode->dir.parent_inum == inode->inum &&
                inode->dir.parent_gen == inode->gen) {
                /* Root directory - parent is self, reuse current inode */
                memfs_map_attrs(shared, &attr, inode, request->fh);
            } else {
                parent_inode = memfs_inode_get_inum(shared,
                                                    inode->dir.parent_inum,
                                                    inode->dir.parent_gen);

                if (parent_inode) {
                    memfs_map_attrs(shared, &attr, parent_inode, request->fh);
                    pthread_mutex_unlock(&parent_inode->lock);
                } else {
                    /* Fallback to current directory attrs if parent not found */
                    memfs_map_attrs(shared, &attr, inode, request->fh);
                }
            }

            rc = request->readdir.callback(
                inode->dir.parent_inum,
                MEMFS_COOKIE_DOTDOT,
                "..",
                2,
                &attr,
                request->proto_private_data);

            if (rc) {
                next_cookie = MEMFS_COOKIE_DOTDOT;
                eof         = 0;
                goto out;
            }

            cookie = MEMFS_COOKIE_DOTDOT;
        }
    } else {
        /* Skip . and .. entries - advance cookie past them */
        if (cookie < MEMFS_COOKIE_DOTDOT) {
            cookie = MEMFS_COOKIE_DOTDOT;
        }
    }

    /* Handle real directory entries (cookie >= 2) */
    if (cookie < MEMFS_COOKIE_FIRST) {
        /* Start from the first real entry */
        rb_tree_first(&inode->dir.dirents, dirent);
    } else {
        /* Resume from where we left off - cookie is (hash + 3) */
        uint64_t hash_cookie = cookie - MEMFS_COOKIE_FIRST;

        rb_tree_query_ceil(&inode->dir.dirents, hash_cookie + 1, hash, dirent);
    }

    while (dirent) {

        dirent_inode = memfs_inode_get_inum(shared, dirent->inum, dirent->gen);

        if (!dirent_inode) {
            dirent = rb_tree_next(&inode->dir.dirents, dirent);
            continue;
        }

        memfs_map_attrs(shared, &attr, dirent_inode, request->fh);

        pthread_mutex_unlock(&dirent_inode->lock);

        rc = request->readdir.callback(
            dirent->inum,
            dirent->hash + MEMFS_COOKIE_FIRST,
            dirent->name,
            dirent->name_len,
            &attr,
            request->proto_private_data);

        next_cookie = dirent->hash + MEMFS_COOKIE_FIRST;

        if (rc) {
            eof = 0;
            break;
        }

        dirent = rb_tree_next(&inode->dir.dirents, dirent);
    }

 out:
    memfs_map_attrs(shared, &request->readdir.r_dir_attr, inode, request->fh);

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

    memfs_map_attrs(shared, &request->open_at.r_dir_pre_attr, parent_inode, request->fh);

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
    } else if (flags & CHIMERA_VFS_OPEN_EXCLUSIVE) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        return;
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

    memfs_map_attrs(shared, &request->open_at.r_dir_post_attr, parent_inode, request->fh);

    pthread_mutex_unlock(&parent_inode->lock);

    memfs_map_attrs(shared, &request->open_at.r_attr, inode, request->fh);

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

    memfs_map_attrs(shared, &request->create_unlinked.r_attr, inode, request->fh);

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

        if (inode->file.blocks && bi < inode->file.num_blocks) {
            block = inode->file.blocks[bi];
        } else {
            block = NULL;
        }

        if (!block) {
            evpl_iovec_clone_segment(&iov[niov], &thread->zero, 0, block_len);
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

    memfs_map_attrs(shared, &request->read.r_attr, inode, request->fh);

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

    memfs_map_attrs(shared, &request->write.r_pre_attr, inode, request->fh);

    if (inode->file.max_blocks <= last_block || !inode->file.blocks) {
        struct memfs_block **new_blocks;
        unsigned int         new_max_blocks;

        blocks = inode->file.blocks;

        new_max_blocks = 1024;

        while (new_max_blocks <= last_block) {
            new_max_blocks <<= 1;
        }

        new_blocks = malloc(new_max_blocks * sizeof(struct memfs_block *));

        if (!new_blocks) {
            pthread_mutex_unlock(&inode->lock);
            request->status = CHIMERA_VFS_ENOSPC;
            request->complete(request);
            return;
        }

        if (blocks) {
            memcpy(new_blocks, blocks,
                   inode->file.num_blocks * sizeof(struct memfs_block *));
            free(blocks);
        }

        memset(new_blocks + inode->file.num_blocks,
               0,
               (new_max_blocks - inode->file.num_blocks) *
               sizeof(struct memfs_block *));

        inode->file.blocks     = new_blocks;
        inode->file.max_blocks = new_max_blocks;
    }

    /* Only increase num_blocks, never decrease it during write */
    if (last_block + 1 > inode->file.num_blocks) {
        inode->file.num_blocks = last_block + 1;
    }

    for (bi = first_block; bi <= last_block; bi++) {

        block_len = CHIMERA_MEMFS_BLOCK_SIZE - block_offset;

        if (left < block_len) {
            block_len = left;
        }

        old_block = inode->file.blocks ? inode->file.blocks[bi] : NULL;

        block = memfs_block_alloc(thread);

        if (!block) {
            pthread_mutex_unlock(&inode->lock);
            request->status = CHIMERA_VFS_ENOSPC;
            request->complete(request);
            return;
        }

        block->niov = evpl_iovec_alloc(evpl, 4096, 4096,
                                       CHIMERA_MEMFS_BLOCK_MAX_IOV,
                                       EVPL_IOVEC_FLAG_SHARED, block->iov);

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

                inode->file.blocks[bi] = NULL;
                memfs_block_free(thread, old_block);
            } else {
                memset(block->iov[0].data, 0, block_offset);

                memset(block->iov[0].data + block_offset + block_len, 0,
                       CHIMERA_MEMFS_BLOCK_SIZE - block_offset - block_len);
            }
        } else if (old_block) {
            /* Full block overwrite: free the old block */
            inode->file.blocks[bi] = NULL;
            memfs_block_free(thread, old_block);
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

    memfs_map_attrs(shared, &request->write.r_post_attr, inode, request->fh);

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

    memfs_map_attrs(shared, &request->symlink.r_attr, inode, request->fh);

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

    memfs_map_attrs(shared, &request->symlink.r_dir_pre_attr, parent_inode, request->fh);

    rb_tree_insert(&parent_inode->dir.dirents, hash, dirent);

    parent_inode->mtime = now;

    memfs_map_attrs(shared, &request->symlink.r_dir_post_attr, parent_inode, request->fh);

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
    struct memfs_inode  *existing_inode = NULL;
    struct memfs_dirent *new_dirent, *old_dirent, *existing_dirent = NULL;
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

    memfs_map_attrs(shared, &request->rename.r_fromdir_pre_attr, old_parent_inode, request->fh);
    memfs_map_attrs(shared, &request->rename.r_todir_pre_attr, new_parent_inode, request->rename.new_fh);

    rb_tree_query_exact(&old_parent_inode->dir.dirents, hash, hash, old_dirent);

    if (!old_dirent) {
        pthread_mutex_unlock(&old_parent_inode->lock);
        if (cmp != 0) {
            pthread_mutex_unlock(&new_parent_inode->lock);
        }
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    child_inode = memfs_inode_get_inum(shared, old_dirent->inum, old_dirent->gen);

    if (!child_inode) {
        pthread_mutex_unlock(&old_parent_inode->lock);
        if (cmp != 0) {
            pthread_mutex_unlock(&new_parent_inode->lock);
        }
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    /* Check if destination already exists */
    rb_tree_query_exact(&new_parent_inode->dir.dirents, new_hash, hash, existing_dirent);

    if (existing_dirent) {
        /* Check if source and destination refer to the same inode (hardlinks).
         * Per POSIX/Linux: if oldpath and newpath are hardlinks to the same file,
         * rename() should do nothing and return success. */
        if (existing_dirent->inum == old_dirent->inum &&
            existing_dirent->gen == old_dirent->gen) {
            /* Same inode - do nothing, just return success */
            memfs_map_attrs(shared, &request->rename.r_fromdir_post_attr, old_parent_inode, request->fh);
            memfs_map_attrs(shared, &request->rename.r_todir_post_attr, new_parent_inode, request->rename.new_fh);
            pthread_mutex_unlock(&child_inode->lock);
            if (cmp != 0) {
                pthread_mutex_unlock(&old_parent_inode->lock);
                pthread_mutex_unlock(&new_parent_inode->lock);
            } else {
                pthread_mutex_unlock(&old_parent_inode->lock);
            }

            request->status = CHIMERA_VFS_OK;
            request->complete(request);
            return;
        }

        /* Destination exists - check if we can replace it */
        existing_inode = memfs_inode_get_inum(shared, existing_dirent->inum, existing_dirent->gen);

        if (existing_inode) {
            /* Cannot rename a directory over a non-directory or vice versa */
            if (S_ISDIR(child_inode->mode) != S_ISDIR(existing_inode->mode)) {
                pthread_mutex_unlock(&existing_inode->lock);
                pthread_mutex_unlock(&child_inode->lock);
                pthread_mutex_unlock(&old_parent_inode->lock);
                if (cmp != 0) {
                    pthread_mutex_unlock(&new_parent_inode->lock);
                }
                request->status = S_ISDIR(existing_inode->mode) ? CHIMERA_VFS_EISDIR : CHIMERA_VFS_ENOTDIR;
                request->complete(request);
                return;
            }

            /* Cannot replace non-empty directory */
            if (S_ISDIR(existing_inode->mode) && existing_inode->nlink > 2) {
                pthread_mutex_unlock(&existing_inode->lock);
                pthread_mutex_unlock(&child_inode->lock);
                pthread_mutex_unlock(&old_parent_inode->lock);
                if (cmp != 0) {
                    pthread_mutex_unlock(&new_parent_inode->lock);
                }
                request->status = CHIMERA_VFS_ENOTEMPTY;
                request->complete(request);
                return;
            }

            /* Remove the existing destination entry and decrement link count */
            rb_tree_remove(&new_parent_inode->dir.dirents, &existing_dirent->node);
            existing_inode->nlink--;
            if (S_ISDIR(existing_inode->mode)) {
                new_parent_inode->nlink--;
            }
            pthread_mutex_unlock(&existing_inode->lock);
            memfs_dirent_free(thread, existing_dirent);
        }
    }

    new_dirent = memfs_dirent_alloc(thread,
                                    old_dirent->inum,
                                    old_dirent->gen,
                                    new_hash,
                                    request->rename.new_name,
                                    request->rename.new_namelen);

    rb_tree_insert(&new_parent_inode->dir.dirents, hash, new_dirent);

    rb_tree_remove(&old_parent_inode->dir.dirents, &old_dirent->node);

    if (S_ISDIR(child_inode->mode)) {
        old_parent_inode->nlink--;
        new_parent_inode->nlink++;
    }

    old_parent_inode->ctime = now;
    new_parent_inode->mtime = now;

    memfs_map_attrs(shared, &request->rename.r_fromdir_post_attr, old_parent_inode, request->fh);
    memfs_map_attrs(shared, &request->rename.r_todir_post_attr, new_parent_inode, request->rename.new_fh);

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

    memfs_map_attrs(shared, &request->link.r_dir_pre_attr, parent_inode, request->link.dir_fh);

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

    memfs_map_attrs(shared, &request->link.r_dir_post_attr, parent_inode, request->link.dir_fh);
    memfs_map_attrs(shared, &request->link.r_attr, inode, request->fh);

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
        case CHIMERA_VFS_OP_MOUNT:
            memfs_mount(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_UMOUNT:
            memfs_umount(thread, shared, request, private_data);
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
        case CHIMERA_VFS_OP_MKNOD:
            memfs_mknod(thread, shared, request, private_data);
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
    .capabilities   = CHIMERA_VFS_CAP_CREATE_UNLINKED,
    .init           = memfs_init,
    .destroy        = memfs_destroy,
    .thread_init    = memfs_thread_init,
    .thread_destroy = memfs_thread_destroy,
    .dispatch       = memfs_dispatch,
};
