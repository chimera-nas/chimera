#define _GNU_SOURCE
#include <stdint.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <limits.h>
#include <jansson.h>
#include <xxhash.h>
#include "common/varint.h"
#include "common/rbtree.h"

#include "slab_allocator.h"

#include "evpl/evpl.h"

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


#define CHIMERA_DEMOFS_INODE_BLOCK_SHIFT 16
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
    int               opcode;
    int               status;
    int               pending;
    int               niov;
    uint32_t          read_prefix;
    uint32_t          read_suffix;
    struct evpl_iovec iov[64];
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
    uint64_t                 length;
    uint64_t                 offset;
    struct demofs_freespace *next;
};

struct demofs_device {
    struct evpl_block_device *bdev;
    uint64_t                  id;
    uint64_t                  size;
    uint64_t                  max_request_size;
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
    char                 *name;
};

struct demofs_symlink_target {
    int                           length;
    char                         *data;
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
    uint64_t             atime_sec;
    uint64_t             ctime_sec;
    uint64_t             mtime_sec;
    uint32_t             atime_nsec;
    uint32_t             ctime_nsec;
    uint32_t             mtime_nsec;
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
    uint64_t              num_inodes;
    uint64_t              total_inodes;
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
    uint64_t                  total_bytes;
    pthread_mutex_t           lock;
};

struct demofs_thread {
    struct evpl              *evpl;
    struct demofs_shared     *shared;
    struct evpl_block_queue **queue;
    struct evpl_iovec         zero;
    struct evpl_iovec         pad;
    int                       thread_id;
    struct slab_allocator    *allocator;
    struct demofs_freespace  *freespace;
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
    chimera_decode_uint32(ptr, gen);
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
    return slab_allocator_alloc(thread->allocator, sizeof(struct demofs_extent));
} /* demofs_extent_alloc */

static inline void
demofs_extent_free(
    struct demofs_thread *thread,
    struct demofs_extent *extent)
{
    slab_allocator_free(thread->allocator, extent, sizeof(*extent));
} /* demofs_extent_free */

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
} /* demofs_extent_free */

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


static inline struct demofs_inode *
demofs_inode_alloc(
    struct demofs_thread *thread,
    uint32_t              list_id)
{
    struct demofs_shared     *shared = thread->shared;
    struct demofs_inode_list *inode_list;
    struct demofs_inode      *inodes, *inode, *last;
    uint32_t                  bi, i, base_id;

    inode_list = &shared->inode_list[list_id];

    pthread_mutex_lock(&inode_list->lock);

    inode = inode_list->free_inode;

    if (!inode) {

        bi = inode_list->num_blocks++;

        chimera_demofs_abort_if(bi >= inode_list->max_blocks, "max inode blocks exceeded");

        inodes = slab_allocator_alloc_perm(thread->allocator, CHIMERA_DEMOFS_INODE_BLOCK * sizeof(*inodes));

        base_id = bi << CHIMERA_DEMOFS_INODE_BLOCK_SHIFT;

        if (unlikely(!inode_list->inode)) {
            inode_list->inode = slab_allocator_alloc_perm(thread->allocator,
                                                          inode_list->max_blocks *
                                                          sizeof(*inode_list->inode));
        }

        inode_list->inode[bi] = inodes;

        last = NULL;

        inode_list->total_inodes += CHIMERA_DEMOFS_INODE_BLOCK;

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

    inode_list->num_inodes++;

    pthread_mutex_unlock(&inode_list->lock);

    inode->gen++;
    inode->refcnt = 1;
    inode->mode   = 0;

    return inode;

} /* demofs_inode_alloc */

static inline struct demofs_inode *
demofs_inode_alloc_thread(struct demofs_thread *thread)
{
    uint32_t list_id = thread->thread_id &
        CHIMERA_DEMOFS_INODE_LIST_MASK;

    return demofs_inode_alloc(thread, list_id);
} /* demofs_inode_alloc */

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
    } else if (S_ISDIR(inode->mode)) {
        rb_tree_destroy(&inode->dir.dirents, demofs_dirent_release, thread);
    } else if (S_ISLNK(inode->mode)) {
        demofs_symlink_target_free(thread, inode->symlink.target);
        inode->symlink.target = NULL;
    }

    pthread_mutex_lock(&inode_list->lock);
    LL_PREPEND(inode_list->free_inode, inode);
    inode_list->num_inodes--;
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

    shared->total_bytes += rsrv_size;

    pthread_mutex_unlock(&shared->lock);

    goto again;
} /* demofs_freespace_alloc */

static void *
demofs_init(const char *cfgfile)
{
    struct demofs_shared       *shared = calloc(1, sizeof(*shared));
    struct demofs_inode_list   *inode_list;
    struct demofs_device       *device;
    struct demofs_freespace    *free_space;
    enum evpl_block_protocol_id protocol_id;
    const char                 *protocol_name, *device_path;
    int                         i;
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
        } else if (strcmp(protocol_name, "vfio") == 0) {
            protocol_id = EVPL_BLOCK_PROTOCOL_VFIO;
        } else {
            chimera_demofs_abort("Unsupported protocol: %s\n", protocol_name);
        }

        device->bdev = evpl_block_open_device(protocol_id, device_path);

        device->size             = evpl_block_size(device->bdev);
        device->max_request_size = evpl_block_max_request_size(device->bdev);

        free_space            = calloc(1, sizeof(*free_space));
        free_space->device_id = device->id;
        free_space->offset    = 0;
        free_space->length    = device->size;
        free_space->next      = NULL;
        device->free_space    = free_space;
    }

    json_decref(cfg);


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

        inode_list->max_blocks = 1024 * 1024;
    }

    return shared;
} /* demofs_init */

static void
demofs_bootstrap(struct demofs_thread *thread)
{
    struct demofs_shared *shared = thread->shared;
    struct timespec       now;
    struct demofs_inode  *inode;

    clock_gettime(CLOCK_REALTIME, &now);

    inode = demofs_inode_alloc(thread, 0);

    inode->size       = 4096;
    inode->space_used = 4096;
    inode->gen        = 1;
    inode->refcnt     = 1;
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

    rb_tree_init(&inode->dir.dirents);

    shared->root_fhlen = demofs_inum_to_fh(shared->root_fh, inode->inum,
                                           inode->gen);
} /* demofs_bootstrap */

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
                    /* do nothing */
                } else if (S_ISREG(inode->mode)) {
                    rb_tree_destroy(&inode->file.extents, demofs_extent_release, NULL);
                }
            }
        }
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


    thread->allocator = slab_allocator_create(4096, 1024 * 1024 * 1024);

    evpl_iovec_alloc(evpl, 4096, 4096, 1, &thread->zero);
    evpl_iovec_alloc(evpl, 4096, 4096, 1, &thread->pad);

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
    struct demofs_thread *thread = private_data;
    struct demofs_shared *shared = thread->shared;

    evpl_iovec_release(&thread->zero);
    evpl_iovec_release(&thread->pad);

    slab_allocator_destroy(thread->allocator);

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
    struct demofs_thread     *thread,
    struct chimera_vfs_attrs *attr,
    struct demofs_inode      *inode)
{
    struct demofs_shared *shared = thread->shared;
    struct demofs_device *device;

    /* We always get attributes atomically with operations */
    attr->va_set_mask = CHIMERA_VFS_ATTR_ATOMIC;

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_FH) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_FH;
        attr->va_fh_len    = demofs_inum_to_fh(attr->va_fh, inode->inum, inode->gen)
        ;
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
        attr->va_rdev          = (42UL << 32) | 42;
    }

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_MASK_STATFS) {
        attr->va_set_mask   |= CHIMERA_VFS_ATTR_MASK_STATFS;
        attr->va_space_total = 0;
        attr->va_space_used  = 0;
        attr->va_space_avail = 0;
        attr->va_space_free  = 0;
        attr->va_files_total = 0;
        attr->va_files_avail = 0;
        attr->va_files_free  = 0;

        pthread_mutex_lock(&shared->lock);

        for (int i = 0; i < shared->num_devices; i++) {
            device                = &shared->devices[i];
            attr->va_space_total += device->size;
        }

        attr->va_space_used  = shared->total_bytes;
        attr->va_space_free  = attr->va_space_total - attr->va_space_used;
        attr->va_space_avail = attr->va_space_free;

        pthread_mutex_unlock(&shared->lock);

        for (int i = 0; i < shared->num_inode_list; i++) {
            pthread_mutex_lock(&shared->inode_list[i].lock);
            attr->va_files_total += shared->inode_list[i].total_inodes;
            pthread_mutex_unlock(&shared->inode_list[i].lock);
        }

        attr->va_files_free  = 0;
        attr->va_files_avail = 0;

    }

} /* demofs_map_attrs */

static inline void
demofs_apply_attrs(
    struct demofs_inode      *inode,
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
            inode->atime_sec  = now.tv_sec;
            inode->atime_nsec = now.tv_nsec;
        } else {
            inode->atime_sec  = attr->va_atime.tv_sec;
            inode->atime_nsec = attr->va_atime.tv_nsec;
        }
    }

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_MTIME) {
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
demofs_getattr(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_vfs_attrs *attr = &request->getattr.r_attr;
    struct demofs_inode      *inode;

    inode = demofs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    demofs_map_attrs(thread, attr, inode);

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
    struct demofs_inode *inode;

    inode = demofs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }
    demofs_map_attrs(thread, &request->setattr.r_pre_attr, inode);

    demofs_apply_attrs(inode, request->setattr.set_attr);

    demofs_map_attrs(thread, &request->setattr.r_post_attr, inode);

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
    struct demofs_inode *inode;

    if (strncmp(request->lookup_path.path, "/", request->lookup_path.pathlen)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    inode = demofs_inode_get_fh(shared, shared->root_fh, shared->root_fhlen);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    demofs_map_attrs(thread, &request->lookup_path.r_attr, inode);

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
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

    demofs_map_attrs(thread, &request->lookup.r_dir_attr, inode);

    child = demofs_inode_get_inum(shared, dirent->inum, dirent->gen);

    demofs_map_attrs(thread, &request->lookup.r_attr, child);

    pthread_mutex_unlock(&child->lock);

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
    struct demofs_inode  *parent_inode, *inode;
    struct demofs_dirent *dirent, *existing_dirent;
    uint64_t              hash;
    struct timespec       now;

    clock_gettime(CLOCK_REALTIME, &now);

    hash = XXH3_64bits(request->mkdir.name, request->mkdir.name_len);

    /* Optimistically allocate an inode */
    inode = demofs_inode_alloc_thread(thread);

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

    rb_tree_init(&inode->dir.dirents);

    demofs_apply_attrs(inode, request->mkdir.set_attr);

    demofs_map_attrs(thread, &request->mkdir.r_attr, inode);

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

    demofs_map_attrs(thread, &request->mkdir.r_dir_pre_attr, parent_inode);

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

    parent_inode->mtime_sec  = now.tv_sec;
    parent_inode->mtime_nsec = now.tv_nsec;

    demofs_map_attrs(thread, &request->mkdir.r_dir_post_attr, parent_inode);

    pthread_mutex_unlock(&parent_inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* demofs_mkdir */

static void
demofs_remove(
    struct demofs_thread       *thread,
    struct demofs_shared       *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct demofs_inode  *parent_inode, *inode;
    struct demofs_dirent *dirent;
    uint64_t              hash;
    struct timespec       now;

    clock_gettime(CLOCK_REALTIME, &now);

    hash = XXH3_64bits(request->remove.name, request->remove.namelen);

    parent_inode = demofs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!parent_inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    demofs_map_attrs(thread, &request->remove.r_dir_pre_attr, parent_inode);

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

    if (S_ISDIR(inode->mode)) {
        parent_inode->nlink--;
    }

    parent_inode->mtime_sec  = now.tv_sec;
    parent_inode->mtime_nsec = now.tv_nsec;

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

    demofs_map_attrs(thread, &request->remove.r_dir_post_attr, parent_inode);

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
    uint64_t                 cookie      = request->readdir.cookie;
    uint64_t                 next_cookie = 0;
    int                      rc, eof = 1;
    struct chimera_vfs_attrs attr;

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

    if (cookie) {
        rb_tree_query_ceil(&inode->dir.dirents, cookie + 1, hash, dirent);
    } else {
        rb_tree_first(&inode->dir.dirents, dirent);
    }

    attr.va_req_mask = request->readdir.attr_mask;

    while (dirent) {

        dirent_inode = demofs_inode_get_inum(shared, dirent->inum, dirent->gen);

        if (!dirent_inode) {
            continue;
        }

        demofs_map_attrs(thread, &attr, dirent_inode);

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

    demofs_map_attrs(thread, &request->readdir.r_dir_attr, inode);

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
    struct demofs_inode  *parent_inode, *inode = NULL;
    struct demofs_dirent *dirent;
    uint64_t              hash;
    unsigned int          flags = request->open_at.flags;
    struct timespec       now;

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

    demofs_map_attrs(thread, &request->open_at.r_dir_pre_attr, parent_inode);

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

        clock_gettime(CLOCK_REALTIME, &now);

        inode->size       = 0;
        inode->space_used = 0;
        inode->uid        = 0;
        inode->gid        = 0;
        inode->nlink      = 1;
        inode->mode       = S_IFREG |  0644;
        inode->atime_sec  = now.tv_sec;
        inode->atime_nsec = now.tv_nsec;
        inode->mtime_sec  = now.tv_sec;
        inode->mtime_nsec = now.tv_nsec;
        inode->ctime_sec  = now.tv_sec;
        inode->ctime_nsec = now.tv_nsec;

        rb_tree_init(&inode->file.extents);

        demofs_apply_attrs(inode, request->open_at.set_attr);

        dirent = demofs_dirent_alloc(thread,
                                     inode->inum,
                                     inode->gen,
                                     hash,
                                     request->open_at.name,
                                     request->open_at.namelen);

        rb_tree_insert(&parent_inode->dir.dirents, hash, dirent);

        parent_inode->mtime_sec  = now.tv_sec;
        parent_inode->mtime_nsec = now.tv_nsec;

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

    demofs_map_attrs(thread, &request->open_at.r_dir_post_attr, parent_inode);

    pthread_mutex_unlock(&parent_inode->lock);

    demofs_map_attrs(thread, &request->open_at.r_attr, inode);

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
    uint64_t                       offset, length, read_offset, read_left;
    uint64_t                       extent_end, overlap_start, overlap_length;
    uint64_t                       aligned_offset, aligned_length, chunk;
    uint32_t                       eof = 0, chunk_niov;
    struct evpl_iovec             *chunk_iov;
    struct evpl_iovec_cursor       cursor;

    demofs_private          = request->plugin_data;
    demofs_private->opcode  = request->opcode;
    demofs_private->status  = 0;
    demofs_private->pending = 0;
    demofs_private->niov    = 0;

    if (unlikely(request->read.length == 0)) {
        request->status        = CHIMERA_VFS_OK;
        request->read.r_niov   = 0;
        request->read.r_length = 0;
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

    if (unlikely(!S_ISREG(inode->mode))) {
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    offset = request->read.offset;
    length = request->read.length;

    if (offset + length > inode->size) {
        length = inode->size > offset ? inode->size - offset : 0;
        eof    = 1;
    }

    aligned_offset = offset & ~4095ULL;
    aligned_length = ((offset + length + 4095ULL) & ~4095ULL) - aligned_offset;

    demofs_private->read_prefix = offset - aligned_offset;
    demofs_private->read_suffix = aligned_length - length;

    request->read.r_length = length;
    request->read.r_eof    = eof;

    // Allocate iovec for full aligned size
    request->read.r_niov = evpl_iovec_alloc(evpl, aligned_length, 4096, 1,
                                            request->read.iov);

    read_offset = aligned_offset;
    read_left   = aligned_length;

    evpl_iovec_cursor_init(&cursor, request->read.iov, request->read.r_niov);

    // Find first extent that could contain our offset
    rb_tree_query_floor(&inode->file.extents, read_offset, file_offset, extent);

    if (extent && extent->file_offset + extent->length <= read_offset) {
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

            chunk_niov = evpl_iovec_cursor_move(&cursor, chunk_iov, 32, chunk);

            if (chunk & 4095) {
                chunk_iov[chunk_niov]        = thread->pad;
                chunk_iov[chunk_niov].length = 4096 - (chunk & 4095);
                chunk_niov++;
            }

            demofs_private->niov += chunk_niov;

            demofs_private->pending++;

            evpl_block_read(evpl,
                            thread->queue[extent->device_id],
                            chunk_iov,
                            chunk_niov,
                            extent->device_offset + overlap_start,
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
    struct evpl                   *evpl = thread->evpl;
    struct demofs_request_private *demofs_private;
    struct demofs_inode           *inode;
    struct demofs_extent          *extent, *next_extent, *new_extent;
    uint64_t                       write_start = request->write.offset;
    uint64_t                       write_end   = write_start + request->write.length;
    uint64_t                       extent_start, extent_end, device_id, device_offset;
    uint64_t                       offset, chunk;
    uint32_t                       left;
    const struct evpl_iovec       *iov;
    struct evpl_iovec             *chunk_iov;
    int                            niov, chunk_niov;
    int                            rc;
    struct evpl_iovec_cursor       cursor;
    struct timespec                now;

    demofs_private          = request->plugin_data;
    demofs_private->opcode  = request->opcode;
    demofs_private->status  = 0;
    demofs_private->pending = 0;
    demofs_private->niov    = 0;

    inode = demofs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    if (unlikely(!S_ISREG(inode->mode))) {
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    demofs_map_attrs(thread, &request->write.r_pre_attr, inode);

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

    clock_gettime(CLOCK_REALTIME, &now);

    inode->mtime_sec  = now.tv_sec;
    inode->mtime_nsec = now.tv_nsec;

    demofs_map_attrs(thread, &request->write.r_post_attr, inode);

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

    evpl_iovec_cursor_init(&cursor, iov, niov);

    offset = 0;
    left   = (request->write.length + 4095) & ~4095;

    while (left) {
        chunk = shared->devices[device_id].max_request_size;

        if (left < chunk) {
            chunk = left;
        }

        chunk_iov = &demofs_private->iov[demofs_private->niov];

        chunk_niov = evpl_iovec_cursor_move(&cursor, chunk_iov, 32, chunk);

        demofs_private->niov += chunk_niov;

        demofs_private->pending++;

        evpl_block_write(evpl,
                         thread->queue[new_extent->device_id],
                         chunk_iov,
                         chunk_niov,
                         new_extent->device_offset + offset,
                         1,
                         demofs_io_callback,
                         request);

        offset += chunk;
        left   -= chunk;
    }

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
    struct demofs_inode  *parent_inode, *inode;
    struct demofs_dirent *dirent, *existing_dirent;
    uint64_t              hash;
    struct timespec       now;

    clock_gettime(CLOCK_REALTIME, &now);

    hash = XXH3_64bits(request->symlink.name, request->symlink.namelen);

    /* Optimistically allocate an inode */
    inode = demofs_inode_alloc_thread(thread);

    inode->size       = request->symlink.targetlen;
    inode->space_used = request->symlink.targetlen;
    inode->uid        = 0;
    inode->gid        = 0;
    inode->nlink      = 1;
    inode->mode       = S_IFLNK | 0755;
    inode->atime_sec  = now.tv_sec;
    inode->atime_nsec = now.tv_nsec;
    inode->mtime_sec  = now.tv_sec;
    inode->mtime_nsec = now.tv_nsec;
    inode->ctime_sec  = now.tv_sec;
    inode->ctime_nsec = now.tv_nsec;

    inode->symlink.target = demofs_symlink_target_alloc(thread,
                                                        request->symlink.target,
                                                        request->symlink.targetlen);

    demofs_map_attrs(thread, &request->symlink.r_attr, inode);

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

    demofs_map_attrs(thread, &request->symlink.r_dir_pre_attr, parent_inode);

    rb_tree_insert(&parent_inode->dir.dirents, hash, dirent);

    parent_inode->mtime_sec  = now.tv_sec;
    parent_inode->mtime_nsec = now.tv_nsec;

    demofs_map_attrs(thread, &request->symlink.r_dir_post_attr, parent_inode);

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

    if (unlikely(!S_ISLNK(inode->mode))) {
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_EINVAL;
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
    struct demofs_inode  *old_parent_inode, *new_parent_inode, *child_inode;
    struct demofs_dirent *dirent, *old_dirent;
    uint64_t              hash, new_hash;
    int                   cmp;
    struct timespec       now;

    clock_gettime(CLOCK_REALTIME, &now);

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

    child_inode = demofs_inode_get_inum(shared, old_dirent->inum, old_dirent->gen);

    if (!child_inode) {
        pthread_mutex_unlock(&old_parent_inode->lock);
        pthread_mutex_unlock(&new_parent_inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
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

    if (S_ISDIR(child_inode->mode)) {
        old_parent_inode->nlink--;
        new_parent_inode->nlink++;
    }

    old_parent_inode->ctime_sec  = now.tv_sec;
    old_parent_inode->ctime_nsec = now.tv_nsec;
    new_parent_inode->mtime_sec  = now.tv_sec;
    new_parent_inode->mtime_nsec = now.tv_nsec;

    if (cmp != 0) {
        pthread_mutex_unlock(&old_parent_inode->lock);
        pthread_mutex_unlock(&new_parent_inode->lock);
    } else {
        pthread_mutex_unlock(&old_parent_inode->lock);
    }

    pthread_mutex_unlock(&child_inode->lock);

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
    struct timespec       now;

    clock_gettime(CLOCK_REALTIME, &now);

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

    if (unlikely(S_ISDIR(inode->mode))) {
        pthread_mutex_unlock(&parent_inode->lock);
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_EPERM;
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

    inode->ctime_sec         = now.tv_sec;
    inode->ctime_nsec        = now.tv_nsec;
    parent_inode->mtime_sec  = now.tv_sec;
    parent_inode->mtime_nsec = now.tv_nsec;

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

    if (unlikely(shared->root_fhlen == 0)) {
        demofs_bootstrap(thread);
    }

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
