#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>

#include "uthash/utlist.h"
#include "uthash/uthash.h"
#include "vfs/vfs.h"
#include "memfs.h"
#include "common/logging.h"
#include "common/misc.h"

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

struct memfs_dirent {
    uint64_t             inum;
    uint32_t             gen;
    uint32_t             name_len;
    UT_hash_handle       hh;
    struct memfs_dirent *next;
    char                 name[256];
};

struct memfs_inode {
    uint64_t             inum;
    uint32_t             gen;
    uint32_t             refcnt;
    uint64_t             size;
    uint64_t             space_used;
    uint32_t             mode;
    uint32_t             nlink;
    uint32_t             uid;
    uint32_t             gid;
    struct timespec      atime;
    struct timespec      mtime;
    struct timespec      ctime;
    struct memfs_dirent *dirents;
    struct memfs_inode  *next;
    pthread_mutex_t      lock;
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
    int                      next_thread_id;
    uint8_t                  root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                 root_fhlen;
    pthread_mutex_t          lock;
};

struct memfs_thread {
    struct memfs_shared *shared;
    int                  thread_id;
    struct memfs_dirent *free_dirent;
};

static inline uint32_t
memfs_inum_to_fh(
    uint8_t *fh,
    uint64_t inum,
    uint32_t gen)
{
    fh[0] = CHIMERA_VFS_FH_MAGIC_MEMFS;
    memcpy(fh + 1, &inum, sizeof(inum));
    memcpy(fh + 9, &gen, sizeof(gen));
    return 13;
} /* memfs_inum_to_fh */

static inline void
memfs_fh_to_inum(
    uint64_t      *inum,
    uint32_t      *gen,
    const uint8_t *fh,
    int            fhlen)
{
    if (unlikely(fhlen != 13)) {
        *inum = UINT64_MAX;
        *gen  = 0;
        return;
    }

    *inum = *(uint64_t *) (fh + 1);
    *gen  = *(uint32_t *) (fh + 9);
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

    inode_list = &shared->inode_list[list_id];

    inode = &inode_list->inode[block_id][block_index];

    pthread_mutex_lock(&inode->lock);

    if (inode->gen != gen) {
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

static inline struct memfs_inode *
memfs_inode_alloc(struct memfs_thread *thread)
{
    struct memfs_shared     *shared = thread->shared;
    struct memfs_inode_list *inode_list;
    struct memfs_inode      *inode;
    uint32_t                 list_id = thread->thread_id &
        CHIMERA_MEMFS_INODE_LIST_MASK;

    inode_list = &shared->inode_list[list_id];

    pthread_mutex_lock(&inode_list->lock);

    inode = inode_list->free_inode;

    chimera_memfs_fatal_if(!inode, "memfs_inode_alloc: no free inodes");

    LL_DELETE(inode_list->free_inode, inode);

    pthread_mutex_unlock(&inode_list->lock);

    inode->gen++;
    inode->refcnt = 1;

    return inode;

} /* memfs_inode_alloc */

static inline void
memfs_inode_free(
    struct memfs_thread *thread,
    struct memfs_inode  *inode)
{
    struct memfs_shared     *shared = thread->shared;
    struct memfs_inode_list *inode_list;
    uint32_t                 list_id = thread->thread_id &
        CHIMERA_MEMFS_INODE_LIST_MASK;

    inode->dirents = NULL;


    inode_list = &shared->inode_list[list_id];

    pthread_mutex_lock(&inode_list->lock);
    LL_PREPEND(inode_list->free_inode, inode);
    pthread_mutex_unlock(&inode_list->lock);
} /* memfs_inode_free */

static inline struct memfs_dirent *
memfs_dirent_alloc(
    struct memfs_thread *thread,
    uint64_t             inum,
    uint32_t             gen,
    const char          *name,
    int                  name_len)
{
    struct memfs_dirent *dirent;

    dirent = thread->free_dirent;

    if (dirent) {
        LL_DELETE(thread->free_dirent, dirent);
    } else {
        dirent = calloc(1, sizeof(*dirent));
    }

    dirent->inum     = inum;
    dirent->gen      = gen;
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

static void *
memfs_init(void)
{
    struct memfs_shared     *shared = calloc(1, sizeof(*shared));
    struct memfs_inode_list *inode_list;
    struct memfs_inode      *inode, *last;
    int                      i, j;

    pthread_mutex_init(&shared->lock, NULL);

    shared->num_inode_list = 255;
    shared->inode_list     = calloc(shared->num_inode_list,
                                    sizeof(*shared->inode_list));

    for (i = 0; i < shared->num_inode_list; i++) {

        inode_list = &shared->inode_list[i];

        inode_list->id         = i;
        inode_list->num_blocks = 1;
        inode_list->max_blocks = 4096;

        pthread_mutex_init(&inode_list->lock, NULL);

        inode_list->inode = calloc(inode_list->max_blocks,
                                   sizeof(*inode_list->inode));

        inode_list->inode[0] = calloc(CHIMERA_MEMFS_INODE_BLOCK,
                                      sizeof(**inode_list->inode));

        last = NULL;
        for (j = 0; j < CHIMERA_MEMFS_INODE_BLOCK; j++) {
            inode       = &inode_list->inode[0][j];
            inode->inum = (j << 8) | i;
            pthread_mutex_init(&inode->lock, NULL);
            inode->next = last;
            last        = inode;
        }
        inode_list->free_inode = last;
    }

    inode_list = &shared->inode_list[0];

    /* Throw away inode 0 so all our inums are non-zero */
    inode = inode_list->free_inode;
    LL_DELETE(inode_list->free_inode, inode);

    /* Setup root inode */
    inode = inode_list->free_inode;
    LL_DELETE(inode_list->free_inode, inode);

    inode->size       = 4096;
    inode->space_used = 4096;
    inode->gen        = 1;
    inode->refcnt     = 1;
    inode->uid        = 0;
    inode->gid        = 0;
    inode->nlink      = 2;
    inode->mode       = S_IFDIR | 0755;
    clock_gettime(CLOCK_REALTIME, &inode->atime);
    clock_gettime(CLOCK_REALTIME, &inode->mtime);
    clock_gettime(CLOCK_REALTIME, &inode->ctime);

    shared->root_fhlen = memfs_inum_to_fh(shared->root_fh, inode->inum,
                                          inode->gen);

    return shared;
} /* memfs_init */

static void
memfs_destroy(void *private_data)
{
    struct memfs_shared *shared = private_data;
    struct memfs_inode  *inode;
    struct memfs_dirent *dirent;
    int                  i, j, k;

    for (i = 0; i < shared->num_inode_list; i++) {
        for (j = 0; j < shared->inode_list[i].num_blocks; j++) {
            for (k = 0; k < CHIMERA_MEMFS_INODE_BLOCK; k++) {
                inode = &shared->inode_list[i].inode[j][k];

                if (inode->gen == 0) {
                    continue;
                }

                if (inode->mode & S_IFDIR) {
                    while (inode->dirents) {
                        dirent = inode->dirents;
                        HASH_DELETE(hh, inode->dirents, dirent);
                        free(dirent);
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

    thread->shared = shared;

    pthread_mutex_lock(&shared->lock);
    thread->thread_id = shared->next_thread_id++;
    pthread_mutex_unlock(&shared->lock);

    return thread;
} /* memfs_thread_init */

static void
memfs_thread_destroy(void *private_data)
{
    struct memfs_thread *thread = private_data;
    struct memfs_dirent *dirent;

    while (thread->free_dirent) {
        dirent = thread->free_dirent;
        LL_DELETE(thread->free_dirent, dirent);
        free(dirent);
    }

    free(thread);
} /* memfs_thread_destroy */

static void
memfs_getattr(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    uint32_t                  attr_mask = request->getattr.attr_mask;
    struct chimera_vfs_attrs *attr      = &request->getattr.r_attr;
    struct memfs_inode       *inode;

    inode = memfs_inode_get_fh(shared, request->getattr.fh,
                               request->getattr.fh_len);

    if (!inode) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    request->status = CHIMERA_VFS_OK;

    attr->va_mask = attr_mask;

    if (attr_mask & CHIMERA_VFS_ATTR_FH) {
        attr->va_fh_len = memfs_inum_to_fh(attr->va_fh, inode->inum,
                                           inode->gen);
    }

    if (attr_mask & CHIMERA_VFS_ATTR_MASK_STAT) {
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

    pthread_mutex_unlock(&inode->lock);

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
    struct chimera_vfs_attrs *attr = &request->setattr.attr;

    inode = memfs_inode_get_fh(shared, request->setattr.fh, request->setattr.
                               fh_len);

    if (!inode) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    if (attr->va_mask & CHIMERA_VFS_ATTR_MODE) {
        inode->mode = attr->va_mode;
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
            clock_gettime(CLOCK_REALTIME, &inode->atime);
        } else {
            inode->atime = attr->va_atime;
        }
    }

    if (attr->va_mask & CHIMERA_VFS_ATTR_MTIME) {
        if (attr->va_mtime.tv_nsec == CHIMERA_VFS_TIME_NOW) {
            clock_gettime(CLOCK_REALTIME, &inode->mtime);
        } else {
            inode->mtime = attr->va_mtime;
        }
    }

    clock_gettime(CLOCK_REALTIME, &inode->ctime);

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_setattr */

static void
memfs_lookup_path(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    request->status = CHIMERA_VFS_OK;
    memcpy(request->lookup_path.r_fh, shared->root_fh, shared->root_fhlen);
    request->lookup_path.r_fh_len = shared->root_fhlen;
    request->complete(request);
} /* memfs_lookup_path */

static void
memfs_lookup(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode  *inode;
    struct memfs_dirent *dirent;

    inode = memfs_inode_get_fh(shared, request->lookup.fh,
                               request->lookup.fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    if (unlikely(!(inode->mode & S_IFDIR))) {
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    HASH_FIND(hh, inode->dirents, request->lookup.component,
              request->lookup.component_len, dirent);

    if (!dirent) {
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }


    request->lookup.r_fh_len = memfs_inum_to_fh(request->lookup.r_fh,
                                                dirent->inum, dirent->gen);

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
    struct memfs_inode  *parent_inode, *inode;
    struct memfs_dirent *dirent, *existing_dirent;
    struct timespec      now;

    clock_gettime(CLOCK_REALTIME, &now);

    /* Optimistically allocate an inode */
    inode = memfs_inode_alloc(thread);

    inode->size       = 4096;
    inode->space_used = 4096;
    inode->uid        = 0;
    inode->gid        = 0;
    inode->nlink      = 2;
    inode->mode       = S_IFDIR | 0755;
    inode->atime      = now;
    inode->mtime      = now;
    inode->ctime      = now;

    /* Optimistically allocate a dirent */
    dirent = memfs_dirent_alloc(thread,
                                inode->inum,
                                inode->gen,
                                request->mkdir.name,
                                request->mkdir.name_len);

    parent_inode = memfs_inode_get_fh(shared,
                                      request->mkdir.fh,
                                      request->mkdir.fh_len);

    if (!parent_inode) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        memfs_inode_free(thread, inode);
        memfs_dirent_free(thread, dirent);
        return;
    }

    if (!(parent_inode->mode & S_IFDIR)) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_ENOTDIR;
        request->complete(request);
        memfs_inode_free(thread, inode);
        memfs_dirent_free(thread, dirent);
        return;
    }

    HASH_FIND(hh, parent_inode->dirents,
              request->mkdir.name, request->mkdir.name_len,
              existing_dirent);

    if (existing_dirent) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        memfs_inode_free(thread, inode);
        memfs_dirent_free(thread, dirent);
        return;
    }

    HASH_ADD(hh, parent_inode->dirents, name, dirent->name_len, dirent);

    parent_inode->nlink++;

    parent_inode->mtime = now;

    pthread_mutex_unlock(&parent_inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_mkdir */

static void
memfs_access(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_access */

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

    clock_gettime(CLOCK_REALTIME, &now);

    parent_inode = memfs_inode_get_fh(shared,
                                      request->remove.fh,
                                      request->remove.fh_len);

    if (!parent_inode) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    if (!(parent_inode->mode & S_IFDIR)) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    HASH_FIND(hh, parent_inode->dirents,
              request->remove.name, request->remove.namelen,
              dirent);

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

    if ((inode->mode & S_IFDIR) && inode->nlink > 2) {
        pthread_mutex_unlock(&parent_inode->lock);
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_ENOTEMPTY;
        request->complete(request);
        return;
    }

    parent_inode->nlink--;
    parent_inode->mtime = now;
    HASH_DEL(parent_inode->dirents, dirent);

    if (inode->mode & S_IFDIR) {
        inode->nlink = 0;
    } else {
        inode->nlink--;
    }

    if (inode->nlink == 0) {
        --inode->refcnt;

        if (inode->refcnt == 0) {
            memfs_inode_free(thread, inode);
        }
    }

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
    struct memfs_dirent     *dirent, *tmp;
    uint64_t                 cookie       = request->readdir.cookie;
    uint64_t                 next_cookie  = 0;
    int                      found_cookie = 0;
    int                      rc, eof = 1;
    struct chimera_vfs_attrs attr;

    if (cookie == 0) {
        found_cookie = 1;
    }

    inode = memfs_inode_get_fh(shared, request->lookup.fh,
                               request->lookup.fh_len);

    if (!inode) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    if (!(inode->mode & S_IFDIR)) {
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    HASH_ITER(hh, inode->dirents, dirent, tmp)
    {
        if (dirent->inum == cookie) {
            found_cookie = 1;
        }

        if (!found_cookie) {
            continue;
        }

        attr.va_mask   = CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT;
        attr.va_fh_len = memfs_inum_to_fh(attr.va_fh, dirent->inum,
                                          dirent->gen);

        dirent_inode = memfs_inode_get_inum(shared, dirent->inum, dirent->gen);

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

    inode = memfs_inode_get_fh(shared,
                               request->open.fh,
                               request->open.fh_len);

    if (!inode) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode->refcnt++;
    pthread_mutex_unlock(&inode->lock);

    request->open.handle.vfs_private = (uint64_t) inode;

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

    clock_gettime(CLOCK_REALTIME, &now);

    parent_inode = memfs_inode_get_fh(shared, request->open_at.parent_fh,
                                      request->open_at.parent_fh_len);

    if (!parent_inode) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    if (!(parent_inode->mode & S_IFDIR)) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    HASH_FIND(hh, parent_inode->dirents, request->open_at.name,
              request->open_at.namelen, dirent);

    if (!dirent) {
        if (!(flags & CHIMERA_VFS_OPEN_CREATE)) {
            pthread_mutex_unlock(&parent_inode->lock);
            request->status = CHIMERA_VFS_EEXIST;
            request->complete(request);
            return;
        }

        inode = memfs_inode_alloc(thread);

        pthread_mutex_lock(&inode->lock);

        inode->size       = 0;
        inode->space_used = 0;
        inode->uid        = 0;
        inode->gid        = 0;
        inode->nlink      = 1;
        inode->mode       = S_IFREG | 0644;
        inode->atime      = now;
        inode->mtime      = now;
        inode->ctime      = now;

        dirent = memfs_dirent_alloc(thread,
                                    inode->inum,
                                    inode->gen,
                                    request->open_at.name,
                                    request->open_at.namelen);

        HASH_ADD(hh, parent_inode->dirents, name, dirent->name_len, dirent);

        parent_inode->nlink++;
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

    inode->refcnt++;

    pthread_mutex_unlock(&inode->lock);
    pthread_mutex_unlock(&parent_inode->lock);

    request->open_at.fh_len = memfs_inum_to_fh(request->open_at.fh,
                                               inode->inum,
                                               inode->gen);

    request->open_at.handle.vfs_private = (uint64_t) inode;

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

    inode = (struct memfs_inode *) request->close.handle->vfs_private;

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
memfs_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_thread *thread = private_data;
    struct memfs_shared *shared = thread->shared;

    switch (request->opcode) {
        case CHIMERA_VFS_OP_LOOKUP_PATH:
            memfs_lookup_path(thread, shared, request, private_data);
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
        case CHIMERA_VFS_OP_ACCESS:
            memfs_access(thread, shared, request, private_data);
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
        case CHIMERA_VFS_OP_CLOSE:
            memfs_close(thread, shared, request, private_data);
            break;
        default:
            chimera_memfs_error("memfs_dispatch: unknown operation %d",
                                request->opcode);
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* memfs_dispatch */

struct chimera_vfs_module vfs_memvfs = {
    .name           = "memfs",
    .fh_magic       = CHIMERA_VFS_FH_MAGIC_MEMFS,
    .blocking       = 0,
    .init           = memfs_init,
    .destroy        = memfs_destroy,
    .thread_init    = memfs_thread_init,
    .thread_destroy = memfs_thread_destroy,
    .dispatch       = memfs_dispatch,
};
