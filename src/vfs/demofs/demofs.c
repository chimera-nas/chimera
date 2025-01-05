#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <limits.h>
#include <jansson.h>
#include <xxhash.h>
#include "common/varint.h"
#include "common/rbtree.h"

#include "core/evpl.h"

#include "uthash/utlist.h"
#include "vfs/vfs.h"
#include "demofs.h"
#include "common/logging.h"
#include "common/misc.h"
#include "evpl_iovec_cursor.h"

#ifndef container_of
#define container_of(ptr, type, member) \
        ((type *) ((char *) (ptr) - offsetof(type, member)))
#endif /* ifndef container_of */

#define CHIMERA_DEMOFS_INODE_LIST_SHIFT  8
#define CHIMERA_DEMOFS_INODE_NUM_LISTS   (1 << CHIMERA_DEMOFS_INODE_LIST_SHIFT)
#define CHIMERA_DEMOFS_INODE_LIST_MASK   (CHIMERA_DEMOFS_INODE_NUM_LISTS - 1)


#define CHIMERA_DEMOFS_INODE_BLOCK_SHIFT 10
#define CHIMERA_DEMOFS_INODE_BLOCK       (1 << CHIMERA_DEMOFS_INODE_BLOCK_SHIFT)
#define CHIMERA_DEMOFS_INODE_BLOCK_MASK  (CHIMERA_DEMOFS_INODE_BLOCK - 1)

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
    int      opcode;
    int      status;
    int      pending;
    uint32_t read_prefix;
    uint32_t read_suffix;
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

struct demofs_freespace {
    uint32_t                 device_id;
    uint32_t                 length;
    uint64_t                 offset;
    struct demofs_freespace *next;
};

struct demofs_device {
    struct evpl_block_device *bdev;
    uint64_t                  id;
    uint64_t                  size;
    char                      name[256];
    pthread_mutex_t           lock;
    struct demofs_freespace  *free_space;
};

struct demofs_dirent {
    uint64_t              inum;
    uint32_t              gen;
    uint32_t              name_len;
    uint64_t              hash;
    struct rb_node        node;
    struct demofs_dirent *next;
    char                  name[256];
};

struct demofs_symlink_target {
    int                           length;
    char                          data[PATH_MAX];
    struct demofs_symlink_target *next;
};

struct demofs_inode {
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
    struct demofs_inode *next;

    pthread_mutex_t      lock;

    union {
        struct {
            struct rb_tree dirents;
        } dir;
        struct {
            struct rb_tree extents;
        } file;
        struct {
            struct demofs_symlink_target *target;
        } symlink;
    };
};

struct demofs_inode_list {
    uint32_t              id;
    uint32_t              num_blocks;
    uint32_t              max_blocks;
    struct demofs_inode **inode;
    struct demofs_inode  *free_inode;
    pthread_mutex_t       lock;
};

struct demofs_shared {
    struct demofs_device     *devices;
    int                       num_devices;
    int                       device_rotor;
    struct demofs_inode_list *inode_list;
    int                       num_inode_list;
    int                       num_active_threads;
    uint8_t                   root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                  root_fhlen;
    pthread_mutex_t           lock;
};

struct demofs_thread {
    struct evpl                  *evpl;
    struct demofs_shared         *shared;
    struct evpl_block_queue     **queue;
    struct evpl_iovec             zero;
    int                           thread_id;
    struct demofs_dirent         *free_dirent;
    struct demofs_symlink_target *free_symlink_target;
    struct demofs_extent         *free_extent;
    struct demofs_freespace      *freespace;
};

static inline uint32_t
demofs_inum_to_fh(
    uint8_t *fh,
    uint64_t inum,
    uint32_t gen)
{
    uint8_t *ptr = fh;

    *ptr++ = CHIMERA_VFS_FH_MAGIC_DEMOFS;

    ptr += chimera_encode_uint64(inum, ptr);
    ptr += chimera_encode_uint32(gen, ptr);

    return ptr - fh;
} /* demofs_inum_to_fh */

static inline void
demofs_fh_to_inum(
    uint64_t      *inum,
    uint32_t      *gen,
    const uint8_t *fh,
    int            fhlen)
{
    const uint8_t *ptr = fh;

    ptr++;

    ptr += chimera_decode_uint64(ptr, inum);
    ptr += chimera_decode_uint32(ptr, gen);
} /* demofs_fh_to_inum */

static inline struct demofs_inode *
demofs_inode_get_inum(
    struct demofs_shared *shared,
    uint64_t              inum,
    uint32_t              gen)
{
    uint64_t                  inum_block;
    uint32_t                  list_id, block_id, block_index;
    struct demofs_inode_list *inode_list;
    struct demofs_inode      *inode;

    list_id     = inum & CHIMERA_DEMOFS_INODE_LIST_MASK;
    inum_block  = inum >> CHIMERA_DEMOFS_INODE_LIST_SHIFT;
    block_index = inum_block & CHIMERA_DEMOFS_INODE_BLOCK_MASK;
    block_id    = inum_block >> CHIMERA_DEMOFS_INODE_BLOCK_SHIFT;

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
} /* demofs_inode_get_inum */

static inline struct demofs_inode *
demofs_inode_get_fh(
    struct demofs_shared *shared,
    const uint8_t        *fh,
    int                   fhlen)
{
    uint64_t inum;
    uint32_t gen;

    demofs_fh_to_inum(&inum, &gen, fh, fhlen);

    return demofs_inode_get_inum(shared, inum, gen);
} /* demofs_inode_get_fh */

static inline struct demofs_extent *
demofs_extent_alloc(struct demofs_thread *thread)
{
    struct demofs_extent *extent;

    extent = thread->free_extent;

    if (extent) {
        LL_DELETE(thread->free_extent, extent);
    } else {
        extent = malloc(sizeof(*extent));
    }

    return extent;
} /* demofs_extent_alloc */

static inline void
demofs_extent_free(
    struct demofs_thread *thread,
    struct demofs_extent *extent)
{
    LL_PREPEND(thread->free_extent, extent);
} /* demofs_extent_free */

static inline void
demofs_extent_release(
    struct rb_node *node,
    void           *private_data)
{
    struct demofs_extent *extent = container_of(node, struct demofs_extent, node);

    free(extent);
} /* demofs_extent_free */

static inline struct demofs_symlink_target *
demofs_symlink_target_alloc(struct demofs_thread *thread)
{
    struct demofs_symlink_target *target;

    target = thread->free_symlink_target;

    if (target) {
        LL_DELETE(thread->free_symlink_target, target);
    } else {
        target = malloc(sizeof(*target));
    }

    return target;
} /* demofs_symlink_target_alloc */

static inline void
demofs_symlink_target_free(
    struct demofs_thread         *thread,
    struct demofs_symlink_target *target)
{
    LL_PREPEND(thread->free_symlink_target, target);
} /* demofs_symlink_target_free */


static inline struct demofs_inode *
demofs_inode_alloc(
    struct demofs_shared *shared,
    uint32_t              list_id)
{
    struct demofs_inode_list *inode_list;
    struct demofs_inode      *inodes, *inode, *last;
    uint32_t                  bi, i, base_id, old_max_blocks;

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

        inodes = malloc(CHIMERA_DEMOFS_INODE_BLOCK * sizeof(*inodes));

        base_id = bi << CHIMERA_DEMOFS_INODE_BLOCK_SHIFT;

        inode_list->inode[bi] = inodes;

        last = NULL;

        for (i = 0; i < CHIMERA_DEMOFS_INODE_BLOCK; i++) {
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

} /* demofs_inode_alloc */

static inline struct demofs_inode *
demofs_inode_alloc_thread(struct demofs_thread *thread)
{
    struct demofs_shared *shared  = thread->shared;
    uint32_t              list_id = thread->thread_id &
        CHIMERA_DEMOFS_INODE_LIST_MASK;

    return demofs_inode_alloc(shared, list_id);
} /* demofs_inode_alloc */

static inline void
demofs_inode_free(
    struct demofs_thread *thread,
    struct demofs_inode  *inode)
{
    struct demofs_shared     *shared = thread->shared;
    struct demofs_inode_list *inode_list;
    uint32_t                  list_id = thread->thread_id &
        CHIMERA_DEMOFS_INODE_LIST_MASK;

    inode_list = &shared->inode_list[list_id];

    if (S_ISREG(inode->mode)) {
        rb_tree_destroy(&inode->file.extents, demofs_extent_release, thread);
    } else if (S_ISLNK(inode->mode)) {
        demofs_symlink_target_free(thread, inode->symlink.target);
        inode->symlink.target = NULL;
    }

    pthread_mutex_lock(&inode_list->lock);
    LL_PREPEND(inode_list->free_inode, inode);
    pthread_mutex_unlock(&inode_list->lock);
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
    struct demofs_dirent *dirent;

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

} /* demofs_dirent_alloc */

static void
demofs_dirent_free(
    struct demofs_thread *thread,
    struct demofs_dirent *dirent)
{
    LL_PREPEND(thread->free_dirent, dirent);
} /* demofs_dirent_free */

static void
demofs_dirent_release(
    struct rb_node *node,
    void           *private_data)
{
    struct demofs_dirent *dirent = container_of(node, struct demofs_dirent, node);

    free(dirent);
} /* demofs_dirent_release */

static inline int
demofs_thread_alloc_space(
    struct demofs_thread *thread,
    int64_t               desired_size,
    uint64_t             *r_device_id,
    uint64_t             *r_device_offset)
{
    struct demofs_shared    *shared = thread->shared;
    struct demofs_device    *device;
    struct demofs_freespace *freespace;
    uint64_t                 size;
    uint64_t                 rsrv_size;

    size = (desired_size + 4095) & ~4095;

 again:

    if (thread->freespace) {
        freespace = thread->freespace;

        if (freespace->length >= size) {
            *r_device_id       = freespace->device_id;
            *r_device_offset   = freespace->offset;
            freespace->length -= size;
            freespace->offset += size;
            return 0;
        }
    }

    if (!thread->freespace) {
        thread->freespace = calloc(1, sizeof(*freespace));
    }

    pthread_mutex_lock(&shared->lock);

    device = &shared->devices[shared->device_rotor];

    shared->device_rotor++;
    if (shared->device_rotor >= shared->num_devices) {
        shared->device_rotor = 0;
    }

    if (!device->free_space) {
        pthread_mutex_unlock(&shared->lock);
        return CHIMERA_VFS_ENOSPC;
    }

    thread->freespace->offset = device->free_space->offset;

    rsrv_size = 1024 * 1024 * 1024;

    if (device->free_space->length < rsrv_size) {
        rsrv_size = device->free_space->length;
    }

    thread->freespace->length    = rsrv_size;
    thread->freespace->device_id = device->id;

    device->free_space->length -= rsrv_size;
    device->free_space->offset += rsrv_size;

    pthread_mutex_unlock(&shared->lock);

    goto again;
} /* demofs_freespace_alloc */

static void *
demofs_init(const char *cfgfile)
{
    struct demofs_shared       *shared = calloc(1, sizeof(*shared));
    struct demofs_inode_list   *inode_list;
    struct demofs_inode        *inode;
    struct demofs_device       *device;
    struct demofs_freespace    *free_space;
    enum evpl_block_protocol_id protocol_id;
    const char                 *protocol_name, *device_path;
    int                         i;
    struct timespec             now;
    json_t                     *cfg, *devices_cfg, *device_cfg;
    json_error_t                json_error;


    cfg = json_load_file(cfgfile, 0, &json_error);

    chimera_demofs_abort_if(cfg == NULL, "Error parsing JSON: %s\n", json_error.text);

    devices_cfg = json_object_get(cfg, "devices");

    shared->num_devices = json_array_size(devices_cfg);
    shared->devices     = calloc(shared->num_devices, sizeof(*shared->devices));

    json_array_foreach(devices_cfg, i, device_cfg)
    {
        device     = &shared->devices[i];
        device->id = i;

        protocol_name = json_string_value(json_object_get(device_cfg, "type"));
        device_path   = json_string_value(json_object_get(device_cfg, "path"));
        if (strcmp(protocol_name, "io_uring") == 0) {
            protocol_id = EVPL_BLOCK_PROTOCOL_IO_URING;
        } else {
            chimera_demofs_abort("Unsupported protocol: %s\n", protocol_name);
        }

        device->bdev = evpl_block_open_device(protocol_id, device_path);

        device->size  = json_integer_value(json_object_get(device_cfg, "size"));
        device->size *= 1000000000UL;

        free_space            = calloc(1, sizeof(*free_space));
        free_space->device_id = device->id;
        free_space->offset    = 0;
        free_space->length    = device->size;
        free_space->next      = NULL;
        device->free_space    = free_space;
    }

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

    inode = demofs_inode_alloc(shared, 0);

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

    shared->root_fhlen = demofs_inum_to_fh(shared->root_fh, inode->inum,
                                           inode->gen);

    return shared;
} /* demofs_init */

static void
demofs_destroy(void *private_data)
{
    struct demofs_shared    *shared = private_data;
    struct demofs_inode     *inode;
    struct demofs_device    *device;
    struct demofs_freespace *freespace;
    int                      i, j, k;

    for (i = 0; i < shared->num_inode_list; i++) {
        for (j = 0; j < shared->inode_list[i].num_blocks; j++) {
            for (k = 0; k < CHIMERA_DEMOFS_INODE_BLOCK; k++) {
                inode = &shared->inode_list[i].inode[j][k];

                if (inode->gen == 0 || inode->refcnt == 0) {
                    continue;
                }

                if (S_ISDIR(inode->mode)) {
                    rb_tree_destroy(&inode->dir.dirents, demofs_dirent_release, NULL);
                } else if (S_ISLNK(inode->mode)) {
                    free(inode->symlink.target);
                } else if (S_ISREG(inode->mode)) {
                    rb_tree_destroy(&inode->file.extents, demofs_extent_release, NULL);
                }
            }
            free(shared->inode_list[i].inode[j]);
        }
        free(shared->inode_list[i].inode);
    }

    for (int i = 0; i < shared->num_devices; i++) {
        device = &shared->devices[i];
        evpl_block_close_device(shared->devices[i].bdev);

        while (device->free_space) {
            freespace = device->free_space;
            LL_DELETE(device->free_space, freespace);
            free(freespace);
        }
    }

    pthread_mutex_destroy(&shared->lock);
    free(shared->devices);
    free(shared->inode_list);
    free(shared);
} /* demofs_destroy */

static void *
demofs_thread_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct demofs_shared *shared = private_data;
    struct demofs_thread *thread = calloc(1, sizeof(*thread));

    evpl_iovec_alloc(evpl, 4096, 4096, 1, &thread->zero);

    thread->queue = calloc(shared->num_devices, sizeof(*thread->queue));

    for (int i = 0; i < shared->num_devices; i++) {
        thread->queue[i] = evpl_block_open_queue(evpl, shared->devices[i].bdev);
    }

    thread->shared = shared;
    thread->evpl   = evpl;
    pthread_mutex_lock(&shared->lock);
    thread->thread_id = shared->num_active_threads++;
    pthread_mutex_unlock(&shared->lock);

    return thread;
} /* demofs_thread_init */

static void
demofs_thread_destroy(void *private_data)
{
    struct demofs_thread         *thread = private_data;
    struct demofs_shared         *shared = thread->shared;
    struct demofs_dirent         *dirent;
    struct demofs_symlink_target *target;
    struct demofs_extent         *extent;

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

    while (thread->free_extent) {
        extent = thread->free_extent;
        LL_DELETE(thread->free_extent, extent);
        free(extent);
    }

    for (int i = 0; i < shared->num_devices; i++) {
        evpl_block_close_queue(thread->evpl, thread->queue[i]);
    }

    if (thread->freespace) {
        free(thread->freespace);
    }

    free(thread->queue);
    free(thread);
} /* demofs_thread_destroy */

static inline void
demofs_map_attrs(
    struct chimera_vfs_attrs *attr,
    uint64_t                  mask,
    struct demofs_inode      *inode)
{

    attr->va_mask = 0;

    if (mask & CHIMERA_VFS_ATTR_FH) {
        attr->va_mask  |= CHIMERA_VFS_ATTR_FH;
        attr->va_fh_len = demofs_inum_to_fh(attr->va_fh, inode->inum, inode->gen)
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

} /* demofs_map_attrs */

static void
demofs_getattr(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    uint32_t                  attr_mask = request->getattr.attr_mask;
    struct chimera_vfs_attrs *attr      = &request->getattr.r_attr;
    struct demofs_inode      *inode;

    inode = demofs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    attr->va_mask = attr_mask;

    if (attr_mask & CHIMERA_VFS_ATTR_FH) {
        attr->va_fh_len = demofs_inum_to_fh(attr->va_fh, inode->inum,
                                            inode->gen);
    }

    demofs_map_attrs(attr, request->getattr.attr_mask, inode);

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* demofs_getattr */

static void
demofs_setattr(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_inode            *inode;
    const struct chimera_vfs_attrs *attr        = request->setattr.attr;
    struct chimera_vfs_attrs       *r_pre_attr  = &request->setattr.r_pre_attr;
    struct chimera_vfs_attrs       *r_post_attr = &request->setattr.r_post_attr;

    inode = demofs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }
    demofs_map_attrs(r_pre_attr, request->setattr.attr_mask, inode);

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

    demofs_map_attrs(r_post_attr, request->setattr.attr_mask, inode);

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* demofs_setattr */

static void
demofs_lookup_path(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    request->status = CHIMERA_VFS_OK;
    memcpy(request->lookup_path.r_attr.va_fh, shared->root_fh, shared->
           root_fhlen);
    request->lookup_path.r_attr.va_fh_len = shared->root_fhlen;
    request->lookup_path.r_attr.va_mask  |= CHIMERA_VFS_ATTR_FH;
    request->complete(request);
} /* demofs_lookup_path */

static void
demofs_lookup(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_inode  *inode, *child;
    struct demofs_dirent *dirent;
    uint64_t              hash;

    hash = XXH3_64bits(request->lookup.component, request->lookup.component_len);

    inode = demofs_inode_get_fh(shared, request->fh, request->fh_len);

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

    rb_tree_query_exact(&inode->dir.dirents, hash, hash, dirent);

    if (!dirent) {
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    if (request->lookup.attrmask) {
        demofs_map_attrs(&request->lookup.r_dir_attr,
                         request->lookup.attrmask,
                         inode);

        child = demofs_inode_get_inum(shared, dirent->inum, dirent->gen);

        demofs_map_attrs(&request->lookup.r_attr,
                         request->lookup.attrmask,
                         child);

        pthread_mutex_unlock(&child->lock);
    }

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* demofs_lookup */

static void
demofs_mkdir(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_inode      *parent_inode, *inode;
    struct demofs_dirent     *dirent, *existing_dirent;
    struct chimera_vfs_attrs *r_attr          = &request->mkdir.r_attr;
    struct chimera_vfs_attrs *r_dir_pre_attr  = &request->mkdir.r_dir_pre_attr;
    struct chimera_vfs_attrs *r_dir_post_attr = &request->mkdir.r_dir_post_attr;
    uint64_t                  hash;

    hash = XXH3_64bits(request->mkdir.name, request->mkdir.name_len);

    /* Optimistically allocate an inode */
    inode = demofs_inode_alloc_thread(thread);

    inode->size       = 4096;
    inode->space_used = 4096;
    inode->uid        = 0;
    inode->gid        = 0;
    inode->nlink      = 2;
    inode->mode       = S_IFDIR | 0755;
    inode->atime      = request->start_time;
    inode->mtime      = request->start_time;
    inode->ctime      = request->start_time;

    rb_tree_init(&inode->dir.dirents);

    demofs_map_attrs(r_attr, request->mkdir.attrmask, inode);

    /* Optimistically allocate a dirent */
    dirent = demofs_dirent_alloc(thread,
                                 inode->inum,
                                 inode->gen,
                                 hash,
                                 request->mkdir.name,
                                 request->mkdir.name_len);

    parent_inode = demofs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!parent_inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        demofs_inode_free(thread, inode);
        demofs_dirent_free(thread, dirent);
        return;
    }

    if (!S_ISDIR(parent_inode->mode)) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_ENOTDIR;
        request->complete(request);
        demofs_inode_free(thread, inode);
        demofs_dirent_free(thread, dirent);
        return;
    }

    demofs_map_attrs(r_dir_pre_attr, request->mkdir.attrmask, parent_inode);

    rb_tree_query_exact(&parent_inode->dir.dirents, hash, hash, existing_dirent);

    if (existing_dirent) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        demofs_inode_free(thread, inode);
        demofs_dirent_free(thread, dirent);
        return;
    }


    rb_tree_insert(&parent_inode->dir.dirents, hash, dirent);

    parent_inode->nlink++;

    parent_inode->mtime = request->start_time;

    demofs_map_attrs(r_dir_post_attr, request->mkdir.attrmask, parent_inode);

    pthread_mutex_unlock(&parent_inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* demofs_mkdir */

static void
demofs_access(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_inode *inode;

    if (request->access.attrmask) {
        inode = demofs_inode_get_fh(shared, request->fh, request->fh_len);

        if (inode) {
            demofs_map_attrs(&request->access.r_attr, request->access.attrmask,
                             inode);

            pthread_mutex_unlock(&inode->lock);
        }
    }

    request->status          = CHIMERA_VFS_OK;
    request->access.r_access = request->access.access;
    request->complete(request);
} /* demofs_access */

static void
demofs_remove(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_inode      *parent_inode, *inode;
    struct demofs_dirent     *dirent;
    uint64_t                  hash;
    struct chimera_vfs_attrs *r_pre_attr  = &request->remove.r_pre_attr;
    struct chimera_vfs_attrs *r_post_attr = &request->remove.r_post_attr;

    hash = XXH3_64bits(request->remove.name, request->remove.namelen);

    parent_inode = demofs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!parent_inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    demofs_map_attrs(r_pre_attr, request->remove.attr_mask, parent_inode);

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

    inode = demofs_inode_get_inum(shared, dirent->inum, dirent->gen);

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

    rb_tree_remove(&parent_inode->dir.dirents, &dirent->node);

    if (S_ISDIR(inode->mode)) {
        inode->nlink = 0;
    } else {
        inode->nlink--;
    }

    if (inode->nlink == 0) {
        --inode->refcnt;

        if (inode->refcnt == 0) {
            demofs_inode_free(thread, inode);
        }
    }
    demofs_map_attrs(r_post_attr, request->remove.attr_mask, parent_inode);

    pthread_mutex_unlock(&parent_inode->lock);
    pthread_mutex_unlock(&inode->lock);

    demofs_dirent_free(thread, dirent);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* demofs_remove */

static void
demofs_readdir(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_inode     *inode, *dirent_inode;
    struct demofs_dirent    *dirent;
    uint64_t                 cookie       = request->readdir.cookie;
    uint64_t                 next_cookie  = 0;
    int                      found_cookie = 0;
    int                      rc, eof = 1;
    struct chimera_vfs_attrs attr;

    if (cookie == 0) {
        found_cookie = 1;
    }

    inode = demofs_inode_get_fh(shared, request->fh, request->fh_len);

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

    rb_tree_query_ceil(&inode->dir.dirents, cookie, hash, dirent);

    while (dirent) {

        if (dirent->inum == cookie) {
            found_cookie = 1;
        }

        if (!found_cookie) {
            continue;
        }

        attr.va_mask   = CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT;
        attr.va_fh_len = demofs_inum_to_fh(attr.va_fh, dirent->inum,
                                           dirent->gen);

        dirent_inode = demofs_inode_get_inum(shared, dirent->inum, dirent->gen);

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

    if (request->readdir.attrmask & CHIMERA_VFS_ATTR_MASK_STAT) {
        demofs_map_attrs(&request->readdir.r_dir_attr, request->readdir.attrmask,
                         inode);
    }

    pthread_mutex_unlock(&inode->lock);

    request->status           = CHIMERA_VFS_OK;
    request->readdir.r_cookie = next_cookie;
    request->readdir.r_eof    = eof;
    request->complete(request);
} /* demofs_readdir */

static void
demofs_open(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_inode *inode;

    inode = demofs_inode_get_fh(shared, request->fh, request->fh_len);

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
} /* demofs_open */

static void
demofs_open_at(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_inode      *parent_inode, *inode = NULL;
    struct demofs_dirent     *dirent;
    uint64_t                  hash;
    unsigned int              flags           = request->open_at.flags;
    struct chimera_vfs_attrs *r_attr          = &request->open_at.r_attr;
    struct chimera_vfs_attrs *r_dir_pre_attr  = &request->open_at.r_dir_pre_attr;
    struct chimera_vfs_attrs *r_dir_post_attr = &request->open_at.r_dir_post_attr;

    hash = XXH3_64bits(request->open_at.name, request->open_at.namelen);

    parent_inode = demofs_inode_get_fh(shared, request->fh, request->fh_len);

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

    demofs_map_attrs(r_dir_pre_attr, request->open_at.attrmask, parent_inode);

    rb_tree_query_exact(&parent_inode->dir.dirents, hash, hash, dirent);

    if (!dirent) {
        if (!(flags & CHIMERA_VFS_OPEN_CREATE)) {
            pthread_mutex_unlock(&parent_inode->lock);
            request->status = CHIMERA_VFS_EEXIST;
            request->complete(request);
            return;
        }

        inode = demofs_inode_alloc_thread(thread);

        pthread_mutex_lock(&inode->lock);

        inode->size       = 0;
        inode->space_used = 0;
        inode->uid        = 0;
        inode->gid        = 0;
        inode->nlink      = 1;
        inode->mode       = S_IFREG |  0644;
        inode->atime      = request->start_time;
        inode->mtime      = request->start_time;
        inode->ctime      = request->start_time;

        rb_tree_init(&inode->file.extents);

        dirent = demofs_dirent_alloc(thread,
                                     inode->inum,
                                     inode->gen,
                                     hash,
                                     request->open_at.name,
                                     request->open_at.namelen);

        rb_tree_insert(&parent_inode->dir.dirents, hash, dirent);

        parent_inode->nlink++;
        parent_inode->mtime = request->start_time;

    } else {

        inode = demofs_inode_get_inum(shared, dirent->inum, dirent->gen);

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

    demofs_map_attrs(r_dir_post_attr, request->open_at.attrmask, parent_inode);

    pthread_mutex_unlock(&parent_inode->lock);

    demofs_map_attrs(r_attr, request->open_at.attrmask, inode);

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* demofs_open_at */

static void
demofs_close(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_inode *inode;

    inode = (struct demofs_inode *) request->close.vfs_private;

    pthread_mutex_lock(&inode->lock);

    --inode->refcnt;

    if (inode->refcnt == 0) {
        demofs_inode_free(thread, inode);
    }

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* demofs_close */

static inline void
demofs_io_callback(
    int   status,
    void *private_data)
{
    struct chimera_vfs_request    *request        = (struct chimera_vfs_request *) private_data;
    struct demofs_request_private *demofs_private = request->plugin_data;

    if (demofs_private->status == 0 && status) {
        demofs_private->status = status;
    }

    if (demofs_private->opcode == CHIMERA_VFS_OP_READ) {
        int last = request->read.r_niov - 1;
        request->read.iov[0].data   += demofs_private->read_prefix;
        request->read.iov[0].length -= demofs_private->read_prefix;

        request->read.iov[last].length -= demofs_private->read_suffix;
    }

    demofs_private->pending--;

    if (demofs_private->pending == 0) {
        request->status = demofs_private->status;
        request->complete(request);
    }
} /* demofs_read_callback */

static void
demofs_read(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct evpl                   *evpl = thread->evpl;
    struct demofs_inode           *inode;
    struct demofs_extent          *extent;
    struct demofs_request_private *demofs_private;
    uint64_t                       offset, length, read_offset;
    uint64_t                       extent_end, overlap_start, overlap_length;
    uint64_t                       aligned_offset, aligned_length;
    uint32_t                       eof = 0;
    struct evpl_block_queue       *queue;

    demofs_private          = request->plugin_data;
    demofs_private->opcode  = request->opcode;
    demofs_private->status  = 0;
    demofs_private->pending = 0;

    offset = request->read.offset;
    length = request->read.length;

    // Calculate 4KB aligned values
    aligned_offset = offset & ~4095ULL;
    aligned_length = ((offset + length + 4095ULL) & ~4095ULL) - aligned_offset;

    demofs_private->read_prefix = offset - aligned_offset;
    demofs_private->read_suffix = aligned_length - length;

    if (unlikely(length == 0)) {
        request->status        = CHIMERA_VFS_OK;
        request->read.r_niov   = 0;
        request->read.r_length = length;
        request->read.r_eof    = eof;
        request->complete(request);
        return;
    }

    inode = demofs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    if (offset + length > inode->size) {
        length = inode->size > offset ? inode->size - offset : 0;
        eof    = 1;
    }

    request->read.r_length = length;
    request->read.r_eof    = eof;

    // Allocate iovec for full aligned size
    request->read.r_niov = evpl_iovec_alloc(evpl, aligned_length, 4096, 1,
                                            request->read.iov);

    read_offset = aligned_offset;

    // Find first extent that could contain our offset
    rb_tree_query_floor(&inode->file.extents, read_offset, file_offset, extent);

    while (read_offset < aligned_offset + aligned_length) {
        if (!extent) {
            // No more extents - zero fill the rest
            memset(request->read.iov[0].data + (read_offset - aligned_offset),
                   0,
                   (aligned_offset + aligned_length) - read_offset);
            break;
        }

        extent_end = extent->file_offset + extent->length;

        if (read_offset < extent->file_offset) {
            // Gap before extent - zero fill
            overlap_length =  extent->file_offset - read_offset;

            if (overlap_length > (aligned_offset + aligned_length) - read_offset) {
                overlap_length = (aligned_offset + aligned_length) - read_offset;
            }

            memset(request->read.iov[0].data + (read_offset - aligned_offset),
                   0,
                   overlap_length);

            read_offset += overlap_length;

            continue;
        }

        // Calculate overlap with current extent
        overlap_start  = read_offset - extent->file_offset;
        overlap_length = extent_end - read_offset;

        if (overlap_length > (aligned_offset + aligned_length) - read_offset) {
            overlap_length = (aligned_offset + aligned_length) - read_offset;
        }

        if (overlap_length > 0) {
            if (extent->buffer) {
                memcpy(request->read.iov[0].data + (read_offset - aligned_offset),
                       extent->buffer + overlap_start,
                       overlap_length);
            } else {
                queue = thread->queue[extent->device_id];
                demofs_private->pending++;

                evpl_block_read(thread->evpl,
                                queue,
                                request->read.iov,
                                request->read.r_niov,
                                extent->device_offset + overlap_start,
                                demofs_io_callback,
                                request);
            }
            read_offset += overlap_length;
        }

        extent = rb_tree_next(&inode->file.extents, extent);
    }

    if (request->read.attrmask & CHIMERA_VFS_ATTR_MASK_STAT) {
        demofs_map_attrs(&request->read.r_attr, request->read.attrmask, inode);
    }

    pthread_mutex_unlock(&inode->lock);

    if (demofs_private->pending == 0) {
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
    }
} /* demofs_read */

static void
demofs_write(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct evpl                   *evpl        = thread->evpl;
    struct chimera_vfs_attrs      *r_pre_attr  = &request->write.r_pre_attr;
    struct chimera_vfs_attrs      *r_post_attr = &request->write.r_post_attr;
    struct demofs_request_private *demofs_private;
    struct demofs_inode           *inode;
    struct demofs_extent          *extent, *next_extent, *new_extent;
    uint64_t                       write_start = request->write.offset;
    uint64_t                       write_end   = write_start + request->write.length;
    uint64_t                       extent_start, extent_end, device_id, device_offset;
    const struct evpl_iovec       *iov;
    int                            niov;
    struct evpl_block_queue       *queue;
    int                            rc;

    demofs_private          = request->plugin_data;
    demofs_private->opcode  = request->opcode;
    demofs_private->status  = 0;
    demofs_private->pending = 1;

    inode = demofs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    demofs_map_attrs(r_pre_attr, request->write.attrmask, inode);

    rc = demofs_thread_alloc_space(thread, request->write.length, &device_id, &device_offset);

    if (rc) {
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_ENOSPC;
        request->complete(request);
        return;
    }


    // Find first potentially overlapping extent
    rb_tree_query_floor(&inode->file.extents, write_start, file_offset, extent);

    // Handle overlapping extents
    while (extent) {
        extent_start = extent->file_offset;
        extent_end   = extent_start + extent->length;

        // Get next node before we potentially modify the tree
        next_extent = rb_tree_next(&inode->file.extents, extent);

        if (extent_start >= write_end) {
            break;
        }

        // Remove fully overlapped extent
        if (extent_start >= write_start && extent_end <= write_end) {
            rb_tree_remove(&inode->file.extents, &extent->node);
            demofs_extent_free(thread, extent);
            extent = next_extent;
            continue;
        }

        // Trim start of extent if needed
        if (extent_start < write_start && extent_end > write_start) {
            extent->length = write_start - extent_start;
        }

        // Trim end of extent if needed
        if (extent_start < write_end && extent_end > write_end) {
            uint64_t overlap = write_end - extent_start;
            extent->file_offset   += overlap;
            extent->device_offset += overlap;
            extent->length        -= overlap;
            if (extent->buffer) {
                extent->buffer += overlap;
            }
        }

        extent = next_extent;
    }

    // Allocate new extent for write
    new_extent = demofs_extent_alloc(thread);

    // Initialize new extent
    new_extent->device_id     = device_id;
    new_extent->device_offset = device_offset;
    new_extent->file_offset   = write_start;
    new_extent->length        = request->write.length;
    new_extent->buffer        = NULL;

    // Insert new extent
    rb_tree_insert(&inode->file.extents, file_offset, new_extent);

    // Update inode metadata
    if (inode->size < write_end) {
        inode->size       = write_end;
        inode->space_used = (inode->size + 4095) & ~4095;
    }
    inode->mtime = request->start_time;

    demofs_map_attrs(r_post_attr, request->write.attrmask, inode);

    pthread_mutex_unlock(&inode->lock);

    // Submit write

    if (request->write.length & 4095) {
        struct evpl_iovec *copy_iov;

        niov = request->write.niov + 1;

        copy_iov = alloca(niov * sizeof(*copy_iov));

        memcpy(copy_iov, request->write.iov, request->write.niov * sizeof(*copy_iov));

        copy_iov[niov - 1]        = thread->zero;
        copy_iov[niov - 1].length = 4096 - (request->write.length & 4095);

        iov = copy_iov;

    } else {
        iov  = request->write.iov;
        niov = request->write.niov;
    }

    queue = thread->queue[device_id];
    evpl_block_write(evpl,
                     queue,
                     iov,
                     niov,
                     new_extent->device_offset,
                     1,
                     demofs_io_callback,
                     request);

    request->write.r_length = request->write.length;
    request->write.r_sync   = 1;
} /* demofs_write */

static void
demofs_symlink(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_inode      *parent_inode, *inode;
    struct demofs_dirent     *dirent, *existing_dirent;
    struct chimera_vfs_attrs *r_attr     = &request->symlink.r_attr;
    struct chimera_vfs_attrs *r_dir_attr = &request->symlink.r_dir_attr;
    uint64_t                  hash;

    hash = XXH3_64bits(request->symlink.name, request->symlink.namelen);

    /* Optimistically allocate an inode */
    inode = demofs_inode_alloc_thread(thread);

    inode->size       = request->symlink.targetlen;
    inode->space_used = request->symlink.targetlen;
    inode->uid        = 0;
    inode->gid        = 0;
    inode->nlink      = 1;
    inode->mode       = S_IFLNK | 0755;
    inode->atime      = request->start_time;
    inode->mtime      = request->start_time;
    inode->ctime      = request->start_time;

    inode->symlink.target = demofs_symlink_target_alloc(thread);

    inode->symlink.target->length = request->symlink.targetlen;
    memcpy(inode->symlink.target->data,
           request->symlink.target,
           request->symlink.targetlen);

    demofs_map_attrs(r_attr, request->symlink.attrmask, inode);

    /* Optimistically allocate a dirent */
    dirent = demofs_dirent_alloc(thread,
                                 inode->inum,
                                 inode->gen,
                                 hash,
                                 request->symlink.name,
                                 request->symlink.namelen);

    parent_inode = demofs_inode_get_fh(shared, request->fh, request->fh_len);

    if (!parent_inode) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        demofs_inode_free(thread, inode);
        demofs_dirent_free(thread, dirent);
        return;
    }

    if (!S_ISDIR(parent_inode->mode)) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_ENOTDIR;
        request->complete(request);
        demofs_inode_free(thread, inode);
        demofs_dirent_free(thread, dirent);
        return;
    }

    rb_tree_query_exact(&parent_inode->dir.dirents, hash, hash, existing_dirent);

    if (existing_dirent) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        demofs_inode_free(thread, inode);
        demofs_dirent_free(thread, dirent);
        return;
    }

    rb_tree_insert(&parent_inode->dir.dirents, hash, dirent);

    parent_inode->nlink++;

    parent_inode->mtime = request->start_time;

    demofs_map_attrs(r_dir_attr, request->symlink.attrmask, parent_inode);

    pthread_mutex_unlock(&parent_inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* demofs_symlink */

static void
demofs_readlink(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_inode *inode;

    inode = demofs_inode_get_fh(shared, request->fh, request->fh_len);

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

static void
demofs_rename(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_inode  *old_parent_inode, *new_parent_inode;
    struct demofs_dirent *dirent, *old_dirent;
    uint64_t              hash, new_hash;
    int                   cmp;

    hash     = XXH3_64bits(request->rename.name, request->rename.namelen);
    new_hash = XXH3_64bits(request->rename.new_name, request->rename.new_namelen);

    cmp = demofs_fh_compare(request->fh,
                            request->fh_len,
                            request->rename.new_fh,
                            request->rename.new_fhlen);

    if (cmp == 0) {
        old_parent_inode = demofs_inode_get_fh(shared,
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
            old_parent_inode = demofs_inode_get_fh(shared,
                                                   request->fh,
                                                   request->fh_len);

            new_parent_inode = demofs_inode_get_fh(shared,
                                                   request->rename.new_fh,
                                                   request->rename.new_fhlen);
        } else {
            new_parent_inode = demofs_inode_get_fh(shared,
                                                   request->rename.new_fh,
                                                   request->rename.new_fhlen);
            old_parent_inode = demofs_inode_get_fh(shared,
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

    dirent = demofs_dirent_alloc(thread,
                                 old_dirent->inum,
                                 old_dirent->gen,
                                 new_hash,
                                 request->rename.new_name,
                                 request->rename.new_namelen);

    rb_tree_insert(&new_parent_inode->dir.dirents, hash, dirent);

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

} /* demofs_rename */

static void
demofs_link(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_inode  *parent_inode, *inode;
    uint64_t              hash;
    struct demofs_dirent *dirent;

    hash = XXH3_64bits(request->link.name, request->link.namelen);

    parent_inode = demofs_inode_get_fh(shared,
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

    inode = demofs_inode_get_fh(shared, request->fh, request->fh_len);

    if (!inode) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    rb_tree_query_exact(&parent_inode->dir.dirents, hash, hash, dirent);

    if (dirent) {
        pthread_mutex_unlock(&parent_inode->lock);
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        return;
    }

    dirent = demofs_dirent_alloc(thread,
                                 inode->inum,
                                 inode->gen,
                                 hash,
                                 request->link.name,
                                 request->link.namelen);

    rb_tree_insert(&parent_inode->dir.dirents, hash, dirent);

    inode->nlink++;
    parent_inode->nlink++;

    inode->ctime        = request->start_time;
    parent_inode->mtime = request->start_time;

    pthread_mutex_unlock(&parent_inode->lock);
    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* demofs_link */

static void
demofs_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_thread *thread = private_data;
    struct demofs_shared *shared = thread->shared;

    switch (request->opcode) {
        case CHIMERA_VFS_OP_LOOKUP_PATH:
            demofs_lookup_path(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LOOKUP:
            demofs_lookup(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            demofs_getattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            demofs_setattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_ACCESS:
            demofs_access(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKDIR:
            demofs_mkdir(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE:
            demofs_remove(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READDIR:
            demofs_readdir(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            demofs_open_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN:
            demofs_open(thread, shared, request, private_data);
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
            request->status = CHIMERA_VFS_OK;
            request->complete(request);
            break;
        case CHIMERA_VFS_OP_SYMLINK:
            demofs_symlink(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READLINK:
            demofs_readlink(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_RENAME:
            demofs_rename(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LINK:
            demofs_link(thread, shared, request, private_data);
            break;
        default:
            chimera_demofs_error("demofs_dispatch: unknown operation %d",
                                 request->opcode);
            request->status = CHIMERA_VFS_ENOTSUP;
            request->complete(request);
            break;
    } /* switch */
} /* demofs_dispatch */

struct chimera_vfs_module vfs_demofs = {
    .name               = "demofs",
    .fh_magic           = CHIMERA_VFS_FH_MAGIC_DEMOFS,
    .blocking           = 0,
    .path_open_required = 0,
    .file_open_required = 0,
    .init               = demofs_init,
    .destroy            = demofs_destroy,
    .thread_init        = demofs_thread_init,
    .thread_destroy     = demofs_thread_destroy,
    .dispatch           = demofs_dispatch,
};
