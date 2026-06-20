// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include <stdbool.h>
#include <stdlib.h>
#include <pthread.h>
#include <string.h>
#include <strings.h>
#include <time.h>
#include <unistd.h>
#include <sys/stat.h>
#include <jansson.h>
#include <utlist.h>

#include "common/varint.h"
#include "common/rbtree.h"

#include "vfs/vfs.h"
#include "vfs/vfs_internal.h"
#include "vfs/vfs_acl.h"
#include "vfs/vfs_access.h"
#include "memfs.h"
#include "common/logging.h"
#include "common/misc.h"
#include "common/macros.h"
#include "common/evpl_iovec_cursor.h"

#ifndef container_of
#define container_of(ptr, type, member) \
        ((type *) ((char *) (ptr) - offsetof(type, member)))
#endif /* ifndef container_of */

#define CHIMERA_MEMFS_BLOCK_MAX_IOV      4

#define CHIMERA_MEMFS_BLOCK_SIZE_MIN     (4 * 1024)
#define CHIMERA_MEMFS_BLOCK_SIZE_MAX     (1024 * 1024)
#define CHIMERA_MEMFS_BLOCK_SIZE_DEFAULT (64 * 1024)

/* clone_range honours alignment to this granularity regardless of the (larger)
 * internal storage block size: a clone range that fully covers an internal
 * block is shared copy-on-write (zero-copy), while partial edges are realised
 * by read-modify-write.  4 KiB matches the allocation unit the SMB server
 * advertises (smb_attr.h), so SMB clients cloning at cluster granularity (e.g.
 * FSCTL_DUPLICATE_EXTENTS_TO_FILE, ODX OFFLOAD_WRITE) land on a clean boundary.
 * Always a power-of-two divisor of every supported block size (>= 4 KiB). */
#define CHIMERA_MEMFS_CLONE_ALIGN        (4 * 1024)

#define CHIMERA_MEMFS_INODE_LIST_SHIFT   8
#define CHIMERA_MEMFS_INODE_NUM_LISTS    (1 << CHIMERA_MEMFS_INODE_LIST_SHIFT)
#define CHIMERA_MEMFS_INODE_LIST_MASK    (CHIMERA_MEMFS_INODE_NUM_LISTS - 1)


#define CHIMERA_MEMFS_INODE_BLOCK_SHIFT  10
#define CHIMERA_MEMFS_INODE_BLOCK        (1 << CHIMERA_MEMFS_INODE_BLOCK_SHIFT)
#define CHIMERA_MEMFS_INODE_BLOCK_MASK   (CHIMERA_MEMFS_INODE_BLOCK - 1)

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

/* A regular file's data fork: a sparse array of fixed-size blocks.  The inode's
 * default (unnamed) data fork lives in the inode union as `file`; each named
 * stream carries its own fork.  Note the default fork's logical size lives in
 * inode->size / inode->space_used (shared with the stat path), whereas a named
 * stream keeps its own size/space_used (see struct memfs_named_stream). */
struct memfs_fork {
    struct memfs_block **blocks;
    unsigned int         num_blocks;
    unsigned int         max_blocks;
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

struct memfs_xattr {
    struct memfs_xattr *next;
    char               *name;
    void               *value;
    uint32_t            name_len;
    uint32_t            value_len;
};

/* A named stream (SMB Alternate Data Stream) hung off a regular-file inode.
 * It is an independent data fork with its own size, but shares the base file's
 * metadata.  `id` is a stable, never-reused per-inode identifier used to encode
 * the stream into a file handle.  `linked` is 1 while the stream is present in
 * inode->streams (analogous to nlink); `refcnt` counts open handles.  An
 * unlinked stream with open handles survives until its last close. */
struct memfs_named_stream {
    struct memfs_named_stream *next;
    char                      *name;
    uint16_t                   name_len;
    uint8_t                    linked;
    uint32_t                   id;
    uint32_t                   refcnt;
    uint64_t                   size;
    uint64_t                   space_used;
    struct memfs_fork          fork;
};

/* Per-open descriptor for a named-stream handle.  A stream open stores
 * `(uintptr_t)desc | 1` in the open handle's vfs_private; the low tag bit
 * distinguishes it from a plain inode pointer (heap pointers are >= 8-aligned).
 * `stream == NULL` denotes the default/unnamed fork.  `open_next` threads every
 * live descriptor on its base inode's `stream_opens` list so an orphaned open
 * (one the protocol layer abandons without a close) is reclaimed when the inode
 * is torn down rather than leaked. */
struct memfs_stream_open {
    struct memfs_inode        *inode;
    struct memfs_named_stream *stream;
    struct memfs_stream_open  *open_next;
};

/* Opaque per-file pNFS layout state (CHIMERA_VFS_ATTR_PNFS_LAYOUT).  memfs
 * persists and returns this blob verbatim via getattr/setattr; only the NFS
 * server interprets it.  Present (non-NULL) once the NFS server has recorded a
 * layout for the file. */
struct memfs_remote {
    uint32_t len;
    uint8_t  data[CHIMERA_VFS_PNFS_LAYOUT_MAX];
};

struct memfs_inode {
    uint64_t                   inum;
    uint32_t                   gen;
    uint32_t                   refcnt;
    uint64_t                   size;
    uint64_t                   space_used;
    uint32_t                   mode;
    uint32_t                   nlink;
    uint32_t                   uid;
    uint32_t                   gid;
    uint64_t                   rdev;
    uint32_t                   dos_attributes;
    struct chimera_acl        *acl; /* NULL => mode-derived; CAP_ACL_NATIVE storage */
    struct timespec            atime;
    struct timespec            mtime;
    struct timespec            ctime;
    struct timespec            btime;
    uint64_t                   change; /* native monotonic change counter */
    struct memfs_inode        *next;
    struct memfs_xattr        *xattrs;
    struct memfs_remote       *remote; /* non-NULL => pNFS stub (data lives on a DS) */

    /* Named streams (SMB ADS) attached to a regular file.  NULL when none.
     * next_stream_id is a monotonic per-inode id allocator (ids are never
     * reused so a stale stream file handle resolves to nothing). */
    struct memfs_named_stream *streams;
    uint32_t                   next_stream_id;

    /* Streams that have been unlinked from `streams` (via remove_stream or a
     * stream delete-on-close) while a handle still holds them open: an unlinked
     * stream survives until its last close frees it.  They are tracked here so
     * that if the base inode is torn down before that final close ever arrives
     * (e.g. a protocol-layer open is abandoned without a matching close), their
     * fork blocks / iovec references and the node itself are still reclaimed
     * rather than leaked -- they are no longer reachable from `streams`. */
    struct memfs_named_stream *dead_streams;

    /* Live per-open stream descriptors (struct memfs_stream_open) threaded by
     * open_next.  An entry is removed on its close; any still present when the
     * inode is torn down belongs to an abandoned open and is freed there. */
    struct memfs_stream_open  *stream_opens;

    pthread_mutex_t            lock;

    union {
        struct {
            struct rb_tree dirents;
            uint64_t       parent_inum;
            uint32_t       parent_gen;
        } dir;
        struct memfs_fork file;
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
    uint32_t                 block_size;
    uint32_t                 block_shift;
    uint32_t                 block_mask;
    int                      noatime;     /* config: disable atime updates on read */
    /* Optional capacity (config "size", bytes).  0 = unlimited (the default --
     * memfs reports a synthetic, never-shrinking size).  When non-zero, memfs
     * accounts live data blocks against this limit and returns ENOSPC when full;
     * fs_space_used is maintained atomically at the block alloc/free choke
     * points (so every path that allocates or frees data is covered). */
    uint64_t                 fs_size;
    uint64_t                 fs_space_used;
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

/* A named-stream file handle is the base file's inum+gen fragment with a
 * non-zero stream id varint appended.  The base inum/gen still decode with the
 * shared chimera_vfs_decode_fh_inum (which ignores the trailing bytes), so the
 * stream handle resolves to the base inode; the stream id selects the fork. */
static inline uint32_t
memfs_encode_stream_fh(
    const void *parent_fh,
    uint64_t    inum,
    uint32_t    gen,
    uint32_t    stream_id,
    void       *out_fh)
{
    uint8_t *fh  = out_fh;
    uint8_t *ptr = fh + CHIMERA_VFS_MOUNT_ID_SIZE;

    memcpy(fh, parent_fh, CHIMERA_VFS_MOUNT_ID_SIZE);
    ptr += chimera_encode_uint64(inum, ptr);
    ptr += chimera_encode_uint32(gen, ptr);
    ptr += chimera_encode_uint32(stream_id, ptr);

    return ptr - fh;
} /* memfs_encode_stream_fh */

static inline void
memfs_decode_stream_fh(
    const void *fh,
    int         fhlen,
    uint64_t   *inum,
    uint32_t   *gen,
    uint32_t   *stream_id)
{
    const uint8_t *ptr = (const uint8_t *) fh + CHIMERA_VFS_MOUNT_ID_SIZE;
    const uint8_t *end = (const uint8_t *) fh + fhlen;

    ptr += chimera_decode_uint64(ptr, inum);
    ptr += chimera_decode_uint32(ptr, gen);

    if (ptr < end) {
        chimera_decode_uint32(ptr, stream_id);
    } else {
        *stream_id = 0;
    }
} /* memfs_decode_stream_fh */

static inline struct memfs_named_stream *
memfs_stream_find_by_name(
    struct memfs_inode *inode,
    const char         *name,
    uint32_t            name_len)
{
    struct memfs_named_stream *stream;

    /* Named-stream lookup is case-insensitive, matching Windows semantics even
     * on a volume that reports FILE_CASE_SENSITIVE_SEARCH (smb2.streams.names3
     * opens "StreamName" via "streamname"/"STREAMNAME"). */
    for (stream = inode->streams; stream; stream = stream->next) {
        if (stream->name_len == name_len &&
            strncasecmp(stream->name, name, name_len) == 0) {
            return stream;
        }
    }

    return NULL;
} /* memfs_stream_find_by_name */

/* Case-insensitive directory scan, used as a fallback when an exact (case-
 * sensitive hash) lookup misses for an SMB/Windows (AUTH_ATTR) caller.  Windows
 * opens are case-insensitive even on a volume that reports
 * FILE_CASE_SENSITIVE_SEARCH (smb2.streams.names3 opens the file via its
 * upper/lower-cased path).  O(n) in the directory size, so it is reached only
 * on a miss; NFS/POSIX (AUTH_UNIX) callers keep strict case-sensitive semantics
 * and never run it.  Caller holds the directory inode lock. */
static inline struct memfs_dirent *
memfs_dirent_find_ci(
    struct memfs_inode *dir,
    const char         *name,
    uint32_t            name_len)
{
    struct memfs_dirent *dirent;

    rb_tree_first(&dir->dir.dirents, dirent);

    while (dirent) {
        if (dirent->name_len == name_len &&
            strncasecmp(dirent->name, name, name_len) == 0) {
            return dirent;
        }
        dirent = rb_tree_next(&dir->dir.dirents, dirent);
    }

    return NULL;
} /* memfs_dirent_find_ci */

static inline struct memfs_named_stream *
memfs_stream_find_by_id(
    struct memfs_inode *inode,
    uint32_t            id)
{
    struct memfs_named_stream *stream;

    for (stream = inode->streams; stream; stream = stream->next) {
        if (stream->id == id) {
            return stream;
        }
    }

    return NULL;
} /* memfs_stream_find_by_id */

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

/* Resolve (and lock) the base inode plus target named stream for a handle-based
 * data op.  Prefers the open handle's vfs_private: a tagged value (low bit set)
 * points at a struct memfs_stream_open (carrying base inode + named stream);
 * an untagged non-zero value is a plain base-inode pointer; zero falls back to
 * decoding the file handle (which may itself carry a stream id).  Returns the
 * locked inode or NULL (caller maps to ENOENT).  *out_stream is the target
 * named fork, or NULL for the default/unnamed fork. */
static inline struct memfs_inode *
memfs_resolve_io(
    struct memfs_shared            *shared,
    struct chimera_vfs_open_handle *handle,
    const uint8_t                  *fh,
    int                             fhlen,
    struct memfs_named_stream     **out_stream)
{
    uint64_t            vp = handle ? handle->vfs_private : 0;
    struct memfs_inode *inode;

    *out_stream = NULL;

    if (vp & 1) {
        struct memfs_stream_open *so =
            (struct memfs_stream_open *) (uintptr_t) (vp & ~1ULL);

        inode = so->inode;
        pthread_mutex_lock(&inode->lock);
        *out_stream = so->stream;
        return inode;
    }

    if (vp) {
        inode = (struct memfs_inode *) (uintptr_t) vp;
        pthread_mutex_lock(&inode->lock);
        return inode;
    }

    {
        uint64_t inum;
        uint32_t gen, sid = 0;

        memfs_decode_stream_fh(fh, fhlen, &inum, &gen, &sid);

        inode = memfs_inode_get_inum(shared, inum, gen);

        if (inode && sid) {
            *out_stream = memfs_stream_find_by_id(inode, sid);
        }

        return inode;
    }
} /* memfs_resolve_io */

/* charge=1 accounts this block against the capacity limit (config "size") and
 * returns NULL (ENOSPC) if it would exceed it.  charge=0 skips accounting: used
 * when an allocation is paired with an immediate free of an existing block (an
 * in-place overwrite / COW), which is net-zero and must not transiently
 * overshoot the limit at exactly-full -- a write into already-allocated space
 * has to succeed even when the device is full. */
static inline struct memfs_block *
memfs_block_alloc_charged(
    struct memfs_thread *thread,
    int                  charge)
{
    struct memfs_shared *shared = thread->shared;
    struct memfs_block  *block;

    if (charge && shared->fs_size) {
        uint64_t used = __atomic_add_fetch(&shared->fs_space_used,
                                           shared->block_size, __ATOMIC_RELAXED);
        if (used > shared->fs_size) {
            __atomic_sub_fetch(&shared->fs_space_used, shared->block_size,
                               __ATOMIC_RELAXED);
            return NULL;
        }
    }

    block = thread->free_block;

    if (block) {
        LL_DELETE(thread->free_block, block);
    } else {
        block = malloc(sizeof(*block));

        if (!block) {
            if (charge && shared->fs_size) {
                __atomic_sub_fetch(&shared->fs_space_used, shared->block_size,
                                   __ATOMIC_RELAXED);
            }
            return NULL;
        }

        block->owner = thread;
    }

    return block;
} /* memfs_block_alloc_charged */

static inline struct memfs_block *
memfs_block_alloc(struct memfs_thread *thread)
{
    return memfs_block_alloc_charged(thread, 1);
} /* memfs_block_alloc */

static inline void
memfs_block_free_charged(
    struct memfs_thread *thread,
    struct memfs_block  *block,
    int                  uncharge)
{
    int i;

    for (i = 0; i < block->niov; i++) {
        evpl_iovec_release(thread->evpl, &block->iov[i]);
    }

    /* Clear niov to prevent stale access from iterating over freed iovecs */
    block->niov = 0;

    if (uncharge && thread->shared->fs_size) {
        __atomic_sub_fetch(&thread->shared->fs_space_used,
                           thread->shared->block_size, __ATOMIC_RELAXED);
    }

    LL_PREPEND(thread->free_block, block);
} /* memfs_block_free_charged */

static inline void
memfs_block_free(
    struct memfs_thread *thread,
    struct memfs_block  *block)
{
    memfs_block_free_charged(thread, block, 1);
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

        inodes = calloc(CHIMERA_MEMFS_INODE_BLOCK, sizeof(*inodes));

        base_id = bi << CHIMERA_MEMFS_INODE_BLOCK_SHIFT;

        inode_list->inode[bi] = inodes;

        last = NULL;

        for (i = 0; i < CHIMERA_MEMFS_INODE_BLOCK; i++) {
            inode       = &inodes[i];
            inode->inum = (base_id + i) << 8 | list_id;
            pthread_mutex_init(&inode->lock, NULL);

            /* Until an inode is handed out by memfs_inode_alloc it must look
             * free: memfs_destroy() walks every slot and keys off gen/refcnt,
             * and dereferences xattrs. Don't rely on the block being zeroed. */
            inode->gen    = 0;
            inode->refcnt = 0;
            inode->xattrs = NULL;

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
    inode->refcnt         = 1;
    inode->mode           = 0;
    inode->change         = 0;
    inode->dos_attributes = 0;
    inode->acl            = NULL;
    inode->xattrs         = NULL;
    inode->remote         = NULL;
    inode->streams        = NULL;
    inode->dead_streams   = NULL;
    inode->stream_opens   = NULL;
    inode->next_stream_id = 0;

    return inode;

} /* memfs_inode_alloc */

/*
 * Replace inode->acl with a deep copy of `src` (NULL clears it).  Caller holds
 * the inode lock.
 */
static inline void
memfs_inode_set_acl(
    struct memfs_inode       *inode,
    const struct chimera_acl *src)
{
    if (inode->acl) {
        free(inode->acl);
        inode->acl = NULL;
    }

    if (src && src->num_aces) {
        size_t sz = chimera_acl_size(src->num_aces);

        inode->acl = malloc(sz);
        memcpy(inode->acl, src, sz);
    }
} /* memfs_inode_set_acl */

static inline struct memfs_inode *
memfs_inode_alloc_thread(struct memfs_thread *thread)
{
    struct memfs_shared *shared  = thread->shared;
    uint32_t             list_id = thread->thread_id &
        CHIMERA_MEMFS_INODE_LIST_MASK;

    return memfs_inode_alloc(shared, list_id);
} /* memfs_inode_alloc */

static void
memfs_dirent_release(
    struct rb_node *node,
    void           *private_data);

static inline void
memfs_xattr_free_all(struct memfs_inode *inode)
{
    struct memfs_xattr *xattr;

    while (inode->xattrs) {
        xattr         = inode->xattrs;
        inode->xattrs = xattr->next;
        free(xattr->name);
        free(xattr->value);
        free(xattr);
    }
} /* memfs_xattr_free_all */

/* Free all data blocks of a regular file and reset its block tracking.
 * Leaves inode->size untouched (callers set it). */
static void
memfs_inode_truncate_blocks(
    struct memfs_thread *thread,
    struct memfs_inode  *inode)
{
    struct memfs_block *block;
    int                 i;

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
    inode->file.num_blocks = 0;
    inode->file.max_blocks = 0;
} /* memfs_inode_truncate_blocks */

/* Free all data blocks of an arbitrary fork (a named stream's fork) and reset
 * its block tracking.  The fork's logical size is the caller's concern. */
static void
memfs_fork_free_blocks(
    struct memfs_thread *thread,
    struct memfs_fork   *fork)
{
    unsigned int i;

    if (fork->blocks) {
        for (i = 0; i < fork->num_blocks; i++) {
            if (fork->blocks[i]) {
                memfs_block_free(thread, fork->blocks[i]);
                fork->blocks[i] = NULL;
            }
        }
        free(fork->blocks);
        fork->blocks = NULL;
    }
    fork->num_blocks = 0;
    fork->max_blocks = 0;
} /* memfs_fork_free_blocks */

/* Free a single named-stream node (its fork blocks, name and the node itself).
 * The node must already be unlinked from inode->streams. */
static void
memfs_stream_node_free(
    struct memfs_thread       *thread,
    struct memfs_named_stream *stream)
{
    memfs_fork_free_blocks(thread, &stream->fork);
    free(stream->name);
    free(stream);
} /* memfs_stream_node_free */

/* Detach an unlinked stream from inode->dead_streams (where remove_stream parks
 * a still-open, unlinked stream).  No-op if it is not parked there. */
static inline void
memfs_dead_stream_detach(
    struct memfs_inode        *inode,
    struct memfs_named_stream *stream)
{
    struct memfs_named_stream **pp;

    for (pp = &inode->dead_streams; *pp; pp = &(*pp)->next) {
        if (*pp == stream) {
            *pp = stream->next;
            return;
        }
    }
} /* memfs_dead_stream_detach */

/* Unlink every named stream from a still-live inode (SUPERSEDE/OVERWRITE drops
* NTFS streams).  A stream with no open handle is freed; one still held open is
* unlinked and parked on dead_streams so its last close frees it -- and so it is
* still reclaimed if the inode is later torn down before that close arrives. */
static void
memfs_streams_free_all(
    struct memfs_thread *thread,
    struct memfs_inode  *inode)
{
    struct memfs_named_stream *stream;

    while (inode->streams) {
        stream         = inode->streams;
        inode->streams = stream->next;
        stream->linked = 0;
        if (stream->refcnt == 0) {
            memfs_stream_node_free(thread, stream);
        } else {
            stream->next        = inode->dead_streams;
            inode->dead_streams = stream;
        }
    }
} /* memfs_streams_free_all */

/* Unconditionally free every named stream of an inode that is being torn down
 * (its last reference is gone, or the whole filesystem is being destroyed):
 * both the live (linked) streams and any unlinked-but-still-referenced ones
 * parked on dead_streams.  Once the base inode is gone an orphaned stream open
 * (one abandoned by the protocol layer without a matching close) can never be
 * cleanly closed, so its node, fork blocks and evpl_iovec references must be
 * reclaimed here rather than leaked. */
static void
memfs_streams_destroy_all(
    struct memfs_thread *thread,
    struct memfs_inode  *inode)
{
    struct memfs_named_stream *stream;
    struct memfs_stream_open  *so;

    while (inode->streams) {
        stream         = inode->streams;
        inode->streams = stream->next;
        stream->linked = 0;
        memfs_stream_node_free(thread, stream);
    }

    while (inode->dead_streams) {
        stream              = inode->dead_streams;
        inode->dead_streams = stream->next;
        memfs_stream_node_free(thread, stream);
    }

    /* Per-open descriptors of any abandoned (never-closed) stream opens: the
     * stream nodes they referenced are freed above, so the close that would
     * normally free these will never come -- reclaim them here. */
    while (inode->stream_opens) {
        so                  = inode->stream_opens;
        inode->stream_opens = so->open_next;
        free(so);
    }
} /* memfs_streams_destroy_all */

static void
memfs_inode_free(
    struct memfs_thread *thread,
    struct memfs_inode  *inode)
{
    struct memfs_shared     *shared = thread->shared;
    struct memfs_inode_list *inode_list;
    uint32_t                 list_id = thread->thread_id &
        CHIMERA_MEMFS_INODE_LIST_MASK;

    inode_list = &shared->inode_list[list_id];

    if (inode->acl) {
        free(inode->acl);
        inode->acl = NULL;
    }

    if (S_ISREG(inode->mode)) {
        memfs_inode_truncate_blocks(thread, inode);
    } else if (S_ISLNK(inode->mode)) {
        memfs_symlink_target_free(thread, inode->symlink.target);
        inode->symlink.target = NULL;
    } else if (S_ISDIR(inode->mode)) {
        /* Release any remaining directory entries.  For a normally-removed
         * (empty) directory this is a no-op; it also prevents leaking the
         * entries of a directory torn down while still populated. */
        rb_tree_destroy(&inode->dir.dirents, memfs_dirent_release, thread);
    }

    /* Extended attributes hang off every inode type. */
    memfs_xattr_free_all(inode);

    /* Named streams cascade with the base file -- including any unlinked-but-
     * still-open ones parked on dead_streams, and the per-open descriptors of
     * any stream opens that were abandoned without a close; none can ever be
     * cleanly closed once their base inode is gone. */
    if (inode->streams || inode->dead_streams || inode->stream_opens) {
        memfs_streams_destroy_all(thread, inode);
    }

    if (inode->remote) {
        free(inode->remote);
        inode->remote = NULL;
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
memfs_init(
    const char                *cfgdata,
    struct prometheus_metrics *metrics)
{
    (void) metrics;
    struct memfs_shared     *shared = calloc(1, sizeof(*shared));
    struct memfs_inode_list *inode_list;
    struct memfs_inode      *inode;
    int                      i;
    struct timespec          now;
    uint32_t                 block_size = CHIMERA_MEMFS_BLOCK_SIZE_DEFAULT;

    chimera_vfs_realtime(&now);

    pthread_mutex_init(&shared->lock, NULL);

    /* Generate a random 64-bit filesystem ID */
    shared->fsid = chimera_rand64();

    if (cfgdata && cfgdata[0] != '\0') {
        json_error_t json_error;
        json_t      *cfg = json_loads(cfgdata, 0, &json_error);

        chimera_memfs_abort_if(!cfg, "Failed to parse memfs config: %s",
                               json_error.text);

        json_t      *bs = json_object_get(cfg, "block_size");

        if (bs) {
            chimera_memfs_abort_if(!json_is_integer(bs),
                                   "memfs block_size must be an integer");

            json_int_t v = json_integer_value(bs);

            chimera_memfs_abort_if(
                v < CHIMERA_MEMFS_BLOCK_SIZE_MIN ||
                v > CHIMERA_MEMFS_BLOCK_SIZE_MAX ||
                (v & (v - 1)) != 0,
                "memfs block_size must be a power of two between %d and %d (got %lld)",
                CHIMERA_MEMFS_BLOCK_SIZE_MIN,
                CHIMERA_MEMFS_BLOCK_SIZE_MAX,
                (long long) v);

            block_size = (uint32_t) v;
        }

        /* A stable fsid keeps the mount_id constant across restarts (useful
         * for a data server so its handles stay valid).  Hex or decimal int. */
        json_t *fsid_cfg = json_object_get(cfg, "fsid");
        if (fsid_cfg) {
            if (json_is_integer(fsid_cfg)) {
                shared->fsid = (uint64_t) json_integer_value(fsid_cfg);
            } else if (json_is_string(fsid_cfg)) {
                shared->fsid = strtoull(json_string_value(fsid_cfg), NULL, 0);
            }
        }

        /* noatime disables read atime updates entirely; default (off) keeps
         * relatime semantics. */
        shared->noatime = json_is_true(json_object_get(cfg, "noatime"));

        /* Optional capacity in bytes; 0/absent means unlimited. */
        json_t *size_cfg = json_object_get(cfg, "size");
        if (size_cfg) {
            chimera_memfs_abort_if(!json_is_integer(size_cfg),
                                   "memfs size must be an integer (bytes)");
            shared->fs_size = (uint64_t) json_integer_value(size_cfg);
        }

        json_decref(cfg);
    }

    shared->block_size  = block_size;
    shared->block_mask  = block_size - 1;
    shared->block_shift = __builtin_ctz(block_size);

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
    /* The freshly-created in-memory root has no configured ownership, so make
     * it world-writable: a fresh memfs share is a blank scratch namespace any
     * connecting user may populate (mirroring a writable share root).  Now that
     * the VFS layer enforces ADD_FILE/ADD_SUBDIRECTORY on the parent, a
     * root-owned 0755 root would (correctly) refuse all creation by non-root
     * clients.  Subdirectories created beneath it are owned by their creator
     * with the usual 0755. */
    inode->mode  = S_IFDIR | 0777;
    inode->atime = now;
    inode->mtime = now;
    inode->ctime = now;
    inode->change++;
    inode->btime = now;

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

                if (inode->acl) {
                    free(inode->acl);
                    inode->acl = NULL;
                }
                memfs_xattr_free_all(inode);

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

                    /* Named-stream forks are freed the same way as the main
                     * fork (direct release, not via the per-thread freelist,
                     * which is gone by destroy time).  Both the live streams and
                     * any unlinked-but-still-open ones parked on dead_streams are
                     * reclaimed -- the latter belong to protocol-layer opens that
                     * were never closed and would otherwise leak their node, fork
                     * blocks and iovec references. */
                    for (int slist = 0; slist < 2; slist++) {
                        struct memfs_named_stream **head =
                            slist == 0 ? &inode->streams : &inode->dead_streams;

                        while (*head) {
                            struct memfs_named_stream *stream = *head;
                            unsigned int               sbi;

                            *head = stream->next;

                            for (sbi = 0; sbi < stream->fork.num_blocks; sbi++) {
                                if (stream->fork.blocks[sbi]) {
                                    for (iovi = 0;
                                         iovi < stream->fork.blocks[sbi]->niov;
                                         iovi++) {
                                        evpl_iovec_release(NULL,
                                                           &stream->fork.blocks[sbi]->iov[iovi]);
                                    }
                                    free(stream->fork.blocks[sbi]);
                                }
                            }
                            if (stream->fork.blocks) {
                                free(stream->fork.blocks);
                            }
                            free(stream->name);
                            free(stream);
                        }
                    }

                    /* Per-open descriptors of abandoned (never-closed) stream
                     * opens whose stream nodes were just freed above. */
                    while (inode->stream_opens) {
                        struct memfs_stream_open *so = inode->stream_opens;
                        inode->stream_opens = so->open_next;
                        free(so);
                    }

                    /* Stubs are always regular files; free the remote
                     * descriptor here so we never touch the uninitialized
                     * fields of never-allocated free-list inodes. */
                    if (inode->remote) {
                        free(inode->remote);
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

    evpl_iovec_alloc(evpl, shared->block_size, 4096, 1, 0, &thread->zero);
    memset(thread->zero.data, 0, shared->block_size);

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

/*
 * Lightweight POSIX permission check on an inode for an AUTH_UNIX caller: build
 * a minimal attr (mode/uid/gid, plus ACL if present) and run it through the
 * canonical access engine.  Returns non-zero if all `requested` ACE rights are
 * granted.  Used to authorize creation in a parent directory at open/create
 * time (the only place that knows whether a name is actually being created).
 */
static int
memfs_inode_access(
    struct memfs_inode            *inode,
    const struct chimera_vfs_cred *cred,
    uint32_t                       requested)
{
    struct chimera_vfs_attrs attr;

    attr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
        CHIMERA_VFS_ATTR_GID;
    attr.va_mode = inode->mode;
    attr.va_uid  = inode->uid;
    attr.va_gid  = inode->gid;
    if (inode->acl) {
        attr.va_set_mask |= CHIMERA_VFS_ATTR_ACL;
        attr.va_acl       = inode->acl;
    }
    return chimera_vfs_access_allowed(&attr, cred, requested);
} /* memfs_inode_access */

static void
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

        /* memfs persists DOS attributes, so report them alongside stat. */
        attr->va_set_mask      |= CHIMERA_VFS_ATTR_DOS_ATTRIBUTES;
        attr->va_dos_attributes = inode->dos_attributes;
    }

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_ACL) {
        /* Copy into a per-thread scratch buffer: memfs releases the inode lock
         * before the (synchronous) completion runs, so we must not hand out the
         * live inode->acl pointer.  The scratch is valid through completion
         * because each memfs thread serves one request at a time. */
        static __thread uint8_t acl_scratch[
            sizeof(struct chimera_acl) +
            CHIMERA_ACL_MAX_ACES * sizeof(struct chimera_ace)];
        struct chimera_acl     *dst = (struct chimera_acl *) acl_scratch;

        if (inode->acl) {
            memcpy(dst, inode->acl, chimera_acl_size(inode->acl->num_aces));
        } else {
            chimera_acl_from_mode(inode->mode, dst, CHIMERA_ACL_MAX_ACES);
        }

        attr->va_acl       = dst;
        attr->va_set_mask |= CHIMERA_VFS_ATTR_ACL;
    }

    /* Birth time is optional and lives outside MASK_STAT, so report it under
     * its own request bit. */
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

    /* Opaque pNFS layout state, persisted verbatim for the NFS server. */
    if ((attr->va_req_mask & CHIMERA_VFS_ATTR_PNFS_LAYOUT) && inode->remote) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_PNFS_LAYOUT;
        attr->va_pnfs_len  = inode->remote->len;
        memcpy(attr->va_pnfs, inode->remote->data, inode->remote->len);
    }

    if (attr->va_req_mask & CHIMERA_VFS_ATTR_MASK_STATFS_VALUES) {
        attr->va_set_mask      |= CHIMERA_VFS_ATTR_MASK_STATFS;
        attr->va_fs_space_total = shared->fs_size ? shared->fs_size :
            CHIMERA_VFS_SYNTHETIC_FS_BYTES;
        attr->va_fs_space_used = shared->fs_size ?
            __atomic_load_n(&shared->fs_space_used, __ATOMIC_RELAXED) : 0;
        attr->va_fs_space_avail = attr->va_fs_space_used < attr->va_fs_space_total ?
            attr->va_fs_space_total - attr->va_fs_space_used : 0;
        attr->va_fs_space_free  = attr->va_fs_space_avail;
        attr->va_fs_files_total = CHIMERA_VFS_SYNTHETIC_FS_INODES;
        attr->va_fs_files_free  = CHIMERA_VFS_SYNTHETIC_FS_INODES;
        attr->va_fs_files_avail = CHIMERA_VFS_SYNTHETIC_FS_INODES;
        attr->va_fsid           = shared->fsid;
    }

} /* memfs_map_attrs */

/* Map attributes for a (possibly named-stream) data fork.  Metadata is always
* the base inode's; for a named stream the reported size/allocation come from
* the stream's own fork and the returned file handle encodes the stream id. */
static inline void
memfs_map_attrs_fork(
    struct memfs_shared       *shared,
    struct chimera_vfs_attrs  *attr,
    struct memfs_inode        *inode,
    struct memfs_named_stream *stream,
    const void                *base_fh)
{
    memfs_map_attrs(shared, attr, inode, base_fh);

    if (!stream) {
        return;
    }

    if (attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) {
        attr->va_size       = stream->size;
        attr->va_space_used = stream->space_used;
    }

    if (attr->va_set_mask & CHIMERA_VFS_ATTR_FH) {
        attr->va_fh_len = memfs_encode_stream_fh(base_fh, inode->inum,
                                                 inode->gen, stream->id,
                                                 attr->va_fh);
    }
} /* memfs_map_attrs_fork */

static inline void
memfs_apply_attrs(
    struct memfs_inode       *inode,
    struct chimera_vfs_attrs *attr)
{
    struct timespec now;
    uint64_t        set_mask = attr->va_set_mask;

    chimera_vfs_realtime(&now);

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

    if (set_mask & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) {
        attr->va_set_mask    |= CHIMERA_VFS_ATTR_DOS_ATTRIBUTES;
        inode->dos_attributes = attr->va_dos_attributes;
    }

    if (set_mask & CHIMERA_VFS_ATTR_ATIME) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_ATIME;
        chimera_vfs_resolve_set_time(&attr->va_atime, &now, &inode->atime);
    }

    if (set_mask & CHIMERA_VFS_ATTR_MTIME) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_MTIME;
        chimera_vfs_resolve_set_time(&attr->va_mtime, &now, &inode->mtime);
    }

    /* ACL coherence.  An explicit ACL set replaces storage and re-derives mode;
     * a bare chmod (MODE without ACL) regenerates the special-who ACEs of any
     * existing rich ACL while preserving named entries. */
    if (set_mask & CHIMERA_VFS_ATTR_ACL) {
        memfs_inode_set_acl(inode, attr->va_acl);
        if (inode->acl) {
            inode->mode = (inode->mode & S_IFMT) | chimera_acl_to_mode(inode->acl);
        }
        attr->va_set_mask |= CHIMERA_VFS_ATTR_ACL;
    } else if ((set_mask & CHIMERA_VFS_ATTR_MODE) && inode->acl) {
        unsigned            cap = inode->acl->num_aces + 8;
        struct chimera_acl *tmp = malloc(chimera_acl_size(cap));
        int                 n   = chimera_acl_chmod(inode->acl, inode->mode,
                                                    tmp, cap);

        if (n >= 0) {
            free(inode->acl);
            inode->acl = tmp;
        } else {
            free(tmp);
        }
    }

    if (set_mask & CHIMERA_VFS_ATTR_BTIME) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_BTIME;
        chimera_vfs_resolve_set_time(&attr->va_btime, &now, &inode->btime);
    }

    /* Opaque pNFS layout state: persist the NFS server's blob verbatim. */
    if (set_mask & CHIMERA_VFS_ATTR_PNFS_LAYOUT) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_PNFS_LAYOUT;
        if (!inode->remote) {
            inode->remote = calloc(1, sizeof(*inode->remote));
        }
        inode->remote->len = attr->va_pnfs_len;
        memcpy(inode->remote->data, attr->va_pnfs,
               attr->va_pnfs_len <= CHIMERA_VFS_PNFS_LAYOUT_MAX ?
               attr->va_pnfs_len : CHIMERA_VFS_PNFS_LAYOUT_MAX);
    }

    /* ctime: an SMB SetInfo(FileBasicInformation) carries an explicit
     * change_time and MS-FSCC requires the server to round-trip it (the
     * change is to the caller-supplied value, not "now").  TIME_OMIT means
     * the caller asked to preserve the stored value — this is how the SMB
     * layer suppresses the implicit ctime bump on a FileBasicInformation
     * SetInfo whose ChangeTime field was zero.  POSIX semantics still apply
     * to any other metadata change — if the caller did not supply CTIME at
     * all (NFS chmod/chown, etc.), stamp it with `now`. */
    if (set_mask & CHIMERA_VFS_ATTR_CTIME) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_CTIME;
        chimera_vfs_resolve_set_time(&attr->va_ctime, &now, &inode->ctime);
    } else {
        inode->ctime = now;
    }

    /* Any setattr is a metadata change; advance the native change counter. */
    inode->change++;

} /* memfs_apply_attrs */

/*
 * Seed a freshly-created child's ACL.  Precedence:
 *   1. An explicit ACL supplied at create (e.g. an SMB SD) is kept as-is.
 *   2. Otherwise, if the parent holds ACEs inheritable for the child's type,
 *      compute the inherited ACL via the shared engine and store it, re-deriving
 *      the mode from it (Windows inheritance defines the child's access).
 *   3. Otherwise, for an SMB-originated create (`windows_default`), store a
 *      Windows-style default DACL granting the owner full control while leaving
 *      the POSIX mode intact, so a Windows client sees owner-full-control (plain
 *      mode would deny e.g. FILE_EXECUTE on a 0644 file).
 *   4. Otherwise (NFS/POSIX create, no inheritance) the child stays mode-derived
 *      (acl == NULL) -- matching legacy and mode-only-backend behaviour.
 * Both inodes are held locked by the caller.
 */
static void
memfs_inherit_acl(
    struct memfs_inode *child,
    struct memfs_inode *parent,
    int                 windows_default)
{
    int      is_dir = S_ISDIR(child->mode);
    uint16_t want   = CHIMERA_ACE_FLAG_FILE_INHERIT |
        (is_dir ? CHIMERA_ACE_FLAG_DIR_INHERIT : 0);
    int      has_inh = 0;

    if (child->acl) {
        return;
    }

    if (parent->acl) {
        for (unsigned i = 0; i < parent->acl->num_aces; i++) {
            if (parent->acl->aces[i].flags & want) {
                has_inh = 1;
                break;
            }
        }
    }

    if (has_inh) {
        /* A directory child can yield up to two ACEs per inheritable parent ACE
         * (an effective entry plus an inherit-only continuation). */
        unsigned            cap = parent->acl->num_aces * 2;
        struct chimera_acl *tmp = malloc(chimera_acl_size(cap));
        int                 n   = chimera_acl_inherit(parent->acl, is_dir,
                                                      child->mode & 07777, tmp, cap);

        if (n > 0) {
            memfs_inode_set_acl(child, tmp);
            child->mode = (child->mode & S_IFMT) | chimera_acl_to_mode(child->acl);
        }
        free(tmp);

        if (child->acl) {
            return;
        }
        /* Nothing actually inherited (e.g. an OBJECT_INHERIT-only ACE on a new
         * directory): fall through to the default below. */
    }

    if (windows_default) {
        uint8_t             buf[sizeof(struct chimera_acl) +
                                4 * sizeof(struct chimera_ace)];
        struct chimera_acl *def = (struct chimera_acl *) buf;

        if (chimera_acl_default_acl(child->mode & 07777, def, 4) > 0) {
            memfs_inode_set_acl(child, def);
        }
    }
} /* memfs_inherit_acl */


static void
memfs_getattr(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode        *inode;
    struct memfs_named_stream *stream;

    inode = memfs_resolve_io(shared, request->getattr.handle,
                             request->fh, request->fh_len, &stream);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    memfs_map_attrs_fork(shared, &request->getattr.r_attr, inode, stream, request->fh);

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_getattr */

/* memfs holds all data in memory, so COMMIT is a no-op for durability.
 * It still returns the requested pre/post attributes so callers (e.g. the
 * NFSv4 server) can validate the target's file type without an extra
 * round-trip. */
static void
memfs_commit(
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

    memfs_map_attrs(shared, &request->commit.r_pre_attr, inode, request->fh);
    memfs_map_attrs(shared, &request->commit.r_post_attr, inode, request->fh);

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_commit */

static void
memfs_setattr(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode        *inode;
    struct memfs_named_stream *stream;
    struct memfs_fork         *fork;
    uint64_t                  *p_size, *p_space_used;
    struct chimera_vfs_attrs  *attr = request->setattr.set_attr;

    inode = memfs_resolve_io(shared, request->setattr.handle,
                             request->fh, request->fh_len, &stream);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    /* SETATTR(SIZE) is only meaningful for regular files. RFC 7530 §5.7:
     * directories must report ISDIR; symlinks should report SYMLINK or
     * INVAL; other non-regular file types report INVAL.  (A named-stream
     * handle always resolves to a regular base inode, so streams pass.) */
    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) &&
        !S_ISREG(inode->mode)) {
        enum chimera_vfs_error err;
        if (S_ISDIR(inode->mode)) {
            err = CHIMERA_VFS_EISDIR;
        } else {
            err = CHIMERA_VFS_EINVAL;
        }
        pthread_mutex_unlock(&inode->lock);
        request->status = err;
        request->complete(request);
        return;
    }

    fork         = stream ? &stream->fork : &inode->file;
    p_size       = stream ? &stream->size : &inode->size;
    p_space_used = stream ? &stream->space_used : &inode->space_used;

    memfs_map_attrs_fork(shared, &request->setattr.r_pre_attr, inode, stream, request->fh);

    /* Handle truncation: free blocks past new EOF and zero partial block */
    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) &&
        S_ISREG(inode->mode) &&
        attr->va_size < *p_size) {

        struct evpl   *evpl           = thread->evpl;
        const uint32_t block_size     = shared->block_size;
        const uint32_t block_shift    = shared->block_shift;
        const uint32_t block_mask     = shared->block_mask;
        uint64_t       new_size       = attr->va_size;
        uint64_t       new_num_blocks = (new_size + block_size - 1) >>
            block_shift;
        uint64_t       bi;

        /* Free blocks that are entirely past the new EOF */
        if (fork->blocks) {
            for (bi = new_num_blocks; bi < fork->num_blocks; bi++) {
                if (fork->blocks[bi]) {
                    memfs_block_free(thread, fork->blocks[bi]);
                    fork->blocks[bi] = NULL;
                }
            }
        }

        /* Zero the partial region in the last block if EOF is not aligned.
         * We must allocate a new block and copy the retained portion because
         * readers may still be referencing the old block's iovecs. */
        if (new_size > 0 && (new_size & block_mask)) {
            uint64_t last_block_idx = (new_size - 1) >> block_shift;

            if (fork->blocks &&
                last_block_idx < fork->num_blocks &&
                fork->blocks[last_block_idx]) {

                struct memfs_block      *old_block = fork->blocks[last_block_idx];
                struct memfs_block      *new_block;
                struct evpl_iovec_cursor old_cursor;
                uint32_t                 offset_in_block = new_size &
                    block_mask;

                /* Net-zero replace of the partial last block: no charge. */
                new_block = memfs_block_alloc_charged(thread, 0);

                if (!new_block) {
                    pthread_mutex_unlock(&inode->lock);
                    request->status = CHIMERA_VFS_ENOSPC;
                    request->complete(request);
                    return;
                }

                new_block->niov = evpl_iovec_alloc(evpl, block_size, 4096,
                                                   CHIMERA_MEMFS_BLOCK_MAX_IOV,
                                                   EVPL_IOVEC_FLAG_SHARED, new_block->iov);

                /* Copy the retained portion from the old block */
                evpl_iovec_cursor_init(&old_cursor, old_block->iov,
                                       old_block->niov);
                evpl_iovec_cursor_copy(&old_cursor, new_block->iov[0].data,
                                       offset_in_block);

                /* Zero the rest of the block */
                memset(new_block->iov[0].data + offset_in_block, 0,
                       block_size - offset_in_block);

                memfs_block_free_charged(thread, old_block, 0);

                /* Replace old block with new block */
                fork->blocks[last_block_idx] = new_block;
            }
        }

        /* Only update num_blocks if blocks array exists.
         * If blocks is NULL (sparse file extended via setattr), keep num_blocks=0. */
        if (fork->blocks) {
            fork->num_blocks = new_num_blocks;
            *p_space_used    = new_num_blocks * block_size;
        } else {
            fork->num_blocks = 0;
            *p_space_used    = 0;
        }
    }

    /* Apply the new logical size to the resolved fork ourselves (the base
     * inode's size lives on the inode, a stream's on its node), then mask the
     * SIZE bit off so memfs_apply_attrs only touches base-inode metadata. */
    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) && S_ISREG(inode->mode)) {
        *p_size = attr->va_size;
        /* POSIX: a successful (f)truncate marks both the last data modification
         * (mtime) and last status change (ctime) times for update.  ctime is
         * stamped unconditionally by memfs_apply_attrs; bump mtime here unless
         * the caller supplied an explicit mtime (in which case apply_attrs
         * applies the caller's value).  AUTH_ATTR (SMB/Windows) callers manage
         * the write time themselves (sticky write-time, SetInfo EndOfFile), so
         * the implicit bump applies only to POSIX/NFS callers. */
        if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_MTIME) &&
            request->cred->flavor != CHIMERA_VFS_AUTH_ATTR) {
            chimera_vfs_realtime(&inode->mtime);
        }
        attr->va_set_mask &= ~CHIMERA_VFS_ATTR_SIZE;
        memfs_apply_attrs(inode, attr);
        attr->va_set_mask |= CHIMERA_VFS_ATTR_SIZE;
    } else {
        memfs_apply_attrs(inode, attr);
    }

    memfs_map_attrs_fork(shared, &request->setattr.r_post_attr, inode, stream, request->fh);

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

        /* memfs persists DOS attributes, so report them alongside stat. */
        attr->va_set_mask      |= CHIMERA_VFS_ATTR_DOS_ATTRIBUTES;
        attr->va_dos_attributes = inode->dos_attributes;
    }

    /* Birth time is optional and lives outside MASK_STAT, so report it under
     * its own request bit. */
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
        attr->va_fs_space_total = shared->fs_size ? shared->fs_size :
            CHIMERA_VFS_SYNTHETIC_FS_BYTES;
        attr->va_fs_space_used = shared->fs_size ?
            __atomic_load_n(&shared->fs_space_used, __ATOMIC_RELAXED) : 0;
        attr->va_fs_space_avail = attr->va_fs_space_used < attr->va_fs_space_total ?
            attr->va_fs_space_total - attr->va_fs_space_used : 0;
        attr->va_fs_space_free  = attr->va_fs_space_avail;
        attr->va_fs_files_total = CHIMERA_VFS_SYNTHETIC_FS_INODES;
        attr->va_fs_files_free  = CHIMERA_VFS_SYNTHETIC_FS_INODES;
        attr->va_fs_files_avail = CHIMERA_VFS_SYNTHETIC_FS_INODES;
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

/*
 * Reverse-path-lookup: given a directory FH, return its parent's FH and the
 * directory's own name within that parent.  The change-notify subtree
 * resolver walks the ancestor directory chain with this (memfs advertises
 * CHIMERA_VFS_CAP_RPL), so it only ever needs to resolve directories — which
 * track their parent via dir.parent_inum/parent_gen.
 */
static void
memfs_getparent(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode  *inode, *parent_inode;
    struct memfs_dirent *dirent;
    uint64_t             parent_inum;
    uint32_t             parent_gen;
    uint64_t             child_inum;
    uint32_t             child_gen;
    int                  found = 0;

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

    child_inum  = inode->inum;
    child_gen   = inode->gen;
    parent_inum = inode->dir.parent_inum;
    parent_gen  = inode->dir.parent_gen;

    pthread_mutex_unlock(&inode->lock);

    /* Encode the parent FH using the child FH as the magic/mount template. */
    request->getparent.r_parent_fh_len =
        chimera_vfs_encode_fh_inum_parent(request->fh, parent_inum, parent_gen,
                                          request->getparent.r_parent_fh);
    request->getparent.r_name_len = 0;

    /* The root directory is its own parent: there is no enclosing name.  The
     * resolver detects the mount root by FH and stops, so an empty name is
     * fine here. */
    if (parent_inum == child_inum && parent_gen == child_gen) {
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    parent_inode = memfs_inode_get_inum(shared, parent_inum, parent_gen);

    if (unlikely(!parent_inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    /* Scan the parent's entries for the one pointing back at this child. */
    rb_tree_first(&parent_inode->dir.dirents, dirent);

    while (dirent) {
        if (dirent->inum == child_inum && dirent->gen == child_gen) {
            uint16_t nlen = dirent->name_len;
            if (nlen > sizeof(request->getparent.r_name)) {
                nlen = sizeof(request->getparent.r_name);
            }
            memcpy(request->getparent.r_name, dirent->name, nlen);
            request->getparent.r_name_len = nlen;
            found                         = 1;
            break;
        }
        dirent = rb_tree_next(&parent_inode->dir.dirents, dirent);
    }

    pthread_mutex_unlock(&parent_inode->lock);

    /* A missing name (child unlinked from the parent mid-walk) is not fatal
     * for the resolver — it still has the parent FH to continue upward. */
    (void) found;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_getparent */

static void
memfs_lookup_at(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode  *inode, *child;
    struct memfs_dirent *dirent;
    uint64_t             hash;
    const char          *name    = request->lookup_at.component;
    uint32_t             namelen = request->lookup_at.component_len;

    hash = request->lookup_at.component_hash;

    inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    if (unlikely(!S_ISDIR(inode->mode))) {
        enum chimera_vfs_error err = S_ISLNK(inode->mode) ? CHIMERA_VFS_ESYMLINK : CHIMERA_VFS_ENOTDIR;
        pthread_mutex_unlock(&inode->lock);
        request->status = err;
        request->complete(request);
        return;
    }

    memfs_map_attrs(shared, &request->lookup_at.r_dir_attr, inode, request->fh);

    /* Handle "." - return the directory itself */
    if (namelen == 1 && name[0] == '.') {
        memfs_map_attrs(shared, &request->lookup_at.r_attr, inode, request->fh);
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    /* Handle ".." - return the parent directory.  The root directory is its
     * own parent, so locking the parent here would re-lock the inode we
     * already hold and deadlock on the non-recursive mutex; resolve ".." to
     * the directory itself in that case (mirrors the readdir ".." handling). */
    if (namelen == 2 && name[0] == '.' && name[1] == '.') {
        if (inode->dir.parent_inum == inode->inum &&
            inode->dir.parent_gen == inode->gen) {
            memfs_map_attrs(shared, &request->lookup_at.r_attr, inode, request->fh);
            pthread_mutex_unlock(&inode->lock);
            request->status = CHIMERA_VFS_OK;
            request->complete(request);
            return;
        }
        child = memfs_inode_get_inum(shared, inode->dir.parent_inum, inode->dir.parent_gen);
        if (unlikely(!child)) {
            pthread_mutex_unlock(&inode->lock);
            request->status = CHIMERA_VFS_ENOENT;
            request->complete(request);
            return;
        }
        memfs_map_attrs(shared, &request->lookup_at.r_attr, child, request->fh);
        pthread_mutex_unlock(&child->lock);
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    rb_tree_query_exact(&inode->dir.dirents, hash, hash, dirent);

    /* Windows opens are case-insensitive: fall back to a case-insensitive scan
     * for an SMB (AUTH_ATTR) caller when the exact match misses (names3). */
    if (!dirent && request->cred->flavor == CHIMERA_VFS_AUTH_ATTR) {
        dirent = memfs_dirent_find_ci(inode, name, namelen);
    }

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

    memfs_map_attrs(shared, &request->lookup_at.r_attr, child, request->fh);

    pthread_mutex_unlock(&child->lock);

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_lookup_at */

static void
memfs_mkdir_at(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode       *parent_inode, *inode, *existing_inode;
    struct memfs_dirent      *dirent, *existing_dirent;
    struct chimera_vfs_attrs *r_attr          = &request->mkdir_at.r_attr;
    struct chimera_vfs_attrs *r_dir_pre_attr  = &request->mkdir_at.r_dir_pre_attr;
    struct chimera_vfs_attrs *r_dir_post_attr = &request->mkdir_at.r_dir_post_attr;
    struct timespec           now;
    uint64_t                  hash;

    chimera_vfs_realtime(&now);

    hash = request->mkdir_at.name_hash;

    /* Optimistically allocate an inode */
    inode = memfs_inode_alloc_thread(thread);

    inode->size       = 4096;
    inode->space_used = 4096;
    inode->uid        = request->cred->uid;
    inode->gid        = request->cred->gid;
    inode->nlink      = 2;
    inode->mode       = S_IFDIR | 0755;
    inode->atime      = now;
    inode->mtime      = now;
    inode->ctime      = now;
    inode->change++;
    inode->btime = now;

    rb_tree_init(&inode->dir.dirents);

    memfs_apply_attrs(inode, request->mkdir_at.set_attr);

    memfs_map_attrs(shared, r_attr, inode, request->fh);

    /* Optimistically allocate a dirent */
    dirent = memfs_dirent_alloc(thread,
                                inode->inum,
                                inode->gen,
                                hash,
                                request->mkdir_at.name,
                                request->mkdir_at.name_len);

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

    /* Inherit the parent's inheritable ACEs (or seed a Windows default DACL for
     * SMB creates); refresh the child's attrs since the mode may have changed. */
    memfs_inherit_acl(inode, parent_inode,
                      request->cred->flavor == CHIMERA_VFS_AUTH_ATTR);
    memfs_map_attrs(shared, r_attr, inode, request->fh);

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
    parent_inode->ctime = now;
    parent_inode->change++;

    memfs_map_attrs(shared, r_dir_post_attr, parent_inode, request->fh);

    pthread_mutex_unlock(&parent_inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_mkdir_at */

static void
memfs_mknod_at(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode       *parent_inode, *inode, *existing_inode;
    struct memfs_dirent      *dirent, *existing_dirent;
    struct chimera_vfs_attrs *r_attr          = &request->mknod_at.r_attr;
    struct chimera_vfs_attrs *r_dir_pre_attr  = &request->mknod_at.r_dir_pre_attr;
    struct chimera_vfs_attrs *r_dir_post_attr = &request->mknod_at.r_dir_post_attr;
    struct timespec           now;
    uint64_t                  hash;

    chimera_vfs_realtime(&now);

    hash = request->mknod_at.name_hash;

    /* Optimistically allocate an inode */
    inode = memfs_inode_alloc_thread(thread);

    inode->size       = 0;
    inode->space_used = 0;
    inode->uid        = request->cred->uid;
    inode->gid        = request->cred->gid;
    inode->nlink      = 1;
    inode->rdev       = 0;
    inode->atime      = now;
    inode->mtime      = now;
    inode->ctime      = now;
    inode->change++;
    inode->btime = now;

    /* The mode (including file type bits S_IFCHR/S_IFBLK/S_IFSOCK/S_IFIFO)
     * and rdev are set via set_attr by the caller */
    if (request->mknod_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        inode->mode = request->mknod_at.set_attr->va_mode;
    } else {
        inode->mode = S_IFREG | 0644;
    }

    if (request->mknod_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_RDEV) {
        inode->rdev = request->mknod_at.set_attr->va_rdev;
    }

    memfs_apply_attrs(inode, request->mknod_at.set_attr);

    memfs_map_attrs(shared, r_attr, inode, request->fh);

    /* Optimistically allocate a dirent */
    dirent = memfs_dirent_alloc(thread,
                                inode->inum,
                                inode->gen,
                                hash,
                                request->mknod_at.name,
                                request->mknod_at.name_len);

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
    parent_inode->ctime = now;
    parent_inode->change++;

    memfs_map_attrs(shared, r_dir_post_attr, parent_inode, request->fh);

    pthread_mutex_unlock(&parent_inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_mknod_at */

static void
memfs_remove_at(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode  *parent_inode, *inode;
    struct memfs_dirent *dirent;
    struct timespec      now;
    uint64_t             hash;

    chimera_vfs_realtime(&now);

    hash = request->remove_at.name_hash;

    parent_inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!parent_inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    memfs_map_attrs(shared, &request->remove_at.r_dir_pre_attr, parent_inode, request->fh);

    if (!S_ISDIR(parent_inode->mode)) {
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_ENOTDIR;
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

    if (S_ISDIR(inode->mode) && !rb_tree_empty(&inode->dir.dirents)) {
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
    parent_inode->ctime = now;
    parent_inode->change++;

    rb_tree_remove(&parent_inode->dir.dirents, &dirent->node);

    if (S_ISDIR(inode->mode)) {
        inode->nlink = 0;
    } else {
        inode->nlink--;
        /* Removing one of several hard links changes the surviving inode's
         * link count, which is a status change: bump its ctime. */
        if (inode->nlink > 0) {
            inode->ctime = now;
            inode->change++;
        }
    }

    /* Don't drop the caller's requested attrs even when the inode is
     * about to be freed.  The inode is still intact at this point, so
     * mapping the full requested mask is safe — and downstream
     * consumers (notify dispatch, attr cache) rely on at least
     * va_mode being available to distinguish file vs directory
     * removals. */
    memfs_map_attrs(shared, &request->remove_at.r_removed_attr, inode, request->fh);

    if (inode->nlink == 0) {
        --inode->refcnt;

        if (inode->refcnt == 0) {
            memfs_inode_free(thread, inode);
        }
    }
    memfs_map_attrs(shared, &request->remove_at.r_dir_post_attr, parent_inode, request->fh);

    pthread_mutex_unlock(&parent_inode->lock);
    pthread_mutex_unlock(&inode->lock);

    memfs_dirent_free(thread, dirent);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_remove_at */

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
        enum chimera_vfs_error err = S_ISLNK(inode->mode) ?
            CHIMERA_VFS_ESYMLINK : CHIMERA_VFS_ENOTDIR;
        pthread_mutex_unlock(&inode->lock);
        request->status = err;
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
memfs_open_fh(
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

    request->open_fh.r_vfs_private = (uint64_t) inode;

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_open_fh */

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

    chimera_vfs_realtime(&now);

    hash = request->open_at.name_hash;

    parent_inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!parent_inode)) {
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

    memfs_map_attrs(shared, &request->open_at.r_dir_pre_attr, parent_inode, request->fh);

    rb_tree_query_exact(&parent_inode->dir.dirents, hash, hash, dirent);

    /* Windows opens are case-insensitive: an SMB (AUTH_ATTR) caller that misses
     * the exact match falls back to a case-insensitive scan, so an existing
     * file is opened (or collides on FILE_CREATE) regardless of the requested
     * case (names3).  A genuine miss still creates the requested-case name. */
    if (!dirent && request->cred->flavor == CHIMERA_VFS_AUTH_ATTR) {
        dirent = memfs_dirent_find_ci(parent_inode, request->open_at.name,
                                      request->open_at.namelen);
    }

    if (!dirent) {
        if (!(flags & CHIMERA_VFS_OPEN_CREATE)) {
            pthread_mutex_unlock(&parent_inode->lock);
            request->status = CHIMERA_VFS_ENOENT;
            request->complete(request);
            return;
        }

        /* Creating a new file requires add-file (WRITE_DATA) + search (EXECUTE)
         * permission on the parent directory.  On the NFSv4/Windows ACL model
         * WRITE_DATA == ADD_FILE and APPEND_DATA == ADD_SUBDIRECTORY, so a plain
         * file create is gated by WRITE_DATA (mkdir, which adds a subdirectory,
         * is gated by APPEND_DATA in the VFS-core mkdir_at path).  Enforce POSIX
         * semantics for AUTH_UNIX callers (root is exempt); SMB/ACL (AUTH_ATTR)
         * callers are authorized by the engine. */
        if (request->cred->flavor == CHIMERA_VFS_AUTH_UNIX &&
            request->cred->uid != 0 &&
            !memfs_inode_access(parent_inode, request->cred,
                                CHIMERA_ACE_WRITE_DATA | CHIMERA_ACE_EXECUTE)) {
            pthread_mutex_unlock(&parent_inode->lock);
            request->status = CHIMERA_VFS_EACCES;
            request->complete(request);
            return;
        }

        inode = memfs_inode_alloc_thread(thread);

        pthread_mutex_lock(&inode->lock);

        inode->size       = 0;
        inode->space_used = 0;
        inode->uid        = request->cred->uid;
        inode->gid        = request->cred->gid;
        inode->nlink      = 1;
        inode->mode       = S_IFREG |  0644;
        inode->atime      = now;
        inode->mtime      = now;
        inode->ctime      = now;
        inode->change++;
        inode->file.blocks     = NULL;
        inode->file.max_blocks = 0;
        inode->file.num_blocks = 0;

        memfs_apply_attrs(inode, request->open_at.set_attr);

        memfs_inherit_acl(inode, parent_inode,
                          request->cred->flavor == CHIMERA_VFS_AUTH_ATTR);

        dirent = memfs_dirent_alloc(thread,
                                    inode->inum,
                                    inode->gen,
                                    hash,
                                    request->open_at.name,
                                    request->open_at.namelen);

        rb_tree_insert(&parent_inode->dir.dirents, hash, dirent);

        parent_inode->mtime = now;
        parent_inode->ctime = now;
        parent_inode->change++;
        request->open_at.r_created = 1;
    } else {
        inode = memfs_inode_get_inum(shared, dirent->inum, dirent->gen);

        if (!inode) {
            pthread_mutex_unlock(&parent_inode->lock);
            request->status = CHIMERA_VFS_ENOENT;
            request->complete(request);
            return;
        }

        /* SMB stop-on-symlink: an existing symbolic link as the final component
         * when the caller did not ask to open the reparse point.  Return ELOOP
         * *before* the O_EXCL collision / truncate / open below, so the SMB
         * create path answers STATUS_STOPPED_ON_SYMLINK regardless of the create
         * disposition (MS-SMB2 3.3.5.9; FILE_CREATE on a symlink leaf stops at
         * the link rather than colliding). */
        if (S_ISLNK(inode->mode) && (flags & CHIMERA_VFS_OPEN_STOP_SYMLINK)) {
            pthread_mutex_unlock(&inode->lock);
            pthread_mutex_unlock(&parent_inode->lock);
            request->status = CHIMERA_VFS_ELOOP;
            request->complete(request);
            return;
        }

        if (flags & CHIMERA_VFS_OPEN_EXCLUSIVE) {
            pthread_mutex_unlock(&inode->lock);
            pthread_mutex_unlock(&parent_inode->lock);
            request->status = CHIMERA_VFS_EEXIST;
            request->complete(request);
            return;
        }

        /* A symlink as the final component under O_NOFOLLOW: a *data* open
         * (POSIX open(O_NOFOLLOW)) must fail with ELOOP, but an O_PATH-style
         * open (SMB FILE_OPEN_REPARSE_POINT, i.e. O_PATH|O_NOFOLLOW) wants a
         * handle to the link itself so the caller can read its attributes /
         * security descriptor / reparse data -- so fall through and open the
         * symlink inode in that case (mirrors the linux backend's O_PATH retry).
         * memfs_inode_get_inum() returned the inode locked, so release both. */
        if (S_ISLNK(inode->mode) && (flags & CHIMERA_VFS_OPEN_NOFOLLOW) &&
            !(flags & CHIMERA_VFS_OPEN_PATH)) {
            pthread_mutex_unlock(&inode->lock);
            pthread_mutex_unlock(&parent_inode->lock);
            request->status = CHIMERA_VFS_ELOOP;
            request->complete(request);
            return;
        }

        /* Overwrite/supersede disposition: replace the existing file's
         * contents (truncate to zero) and apply the new attributes.  NTFS
         * drops a file's named streams on SUPERSEDE/OVERWRITE, so discard them
         * on the explicit truncate-on-open path (but not on a plain create). */
        if ((flags & CHIMERA_VFS_OPEN_TRUNCATE) && S_ISREG(inode->mode)) {
            memfs_inode_truncate_blocks(thread, inode);
            if (inode->streams) {
                memfs_streams_free_all(thread, inode);
            }
            inode->size       = 0;
            inode->space_used = 0;
            memfs_apply_attrs(inode, request->open_at.set_attr);
        } else if ((flags & CHIMERA_VFS_OPEN_CREATE) &&
                   S_ISREG(inode->mode) &&
                   (request->open_at.set_attr->va_set_mask & CHIMERA_VFS_ATTR_SIZE) &&
                   request->open_at.set_attr->va_size == 0) {
            memfs_inode_truncate_blocks(thread, inode);
            inode->size       = 0;
            inode->space_used = 0;
        }
    }

    if ((flags & CHIMERA_VFS_OPEN_DIRECTORY) && !S_ISDIR(inode->mode)) {
        pthread_mutex_unlock(&inode->lock);
        pthread_mutex_unlock(&parent_inode->lock);
        request->status = CHIMERA_VFS_ENOTDIR;
        request->complete(request);
        return;
    }

    /* Access is enforced at the VFS layer (the credential-keyed gate in
    * chimera_vfs_read/write and the protocol's own create-time check), which
    * is ACL-aware and honors each protocol's access semantics; memfs does not
    * re-check here -- a coarse read/write test would mis-handle SMB opens that
    * carry only control rights (e.g. WRITE_DAC) and not data access. */

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

    chimera_vfs_realtime(&now);

    inode = memfs_inode_alloc_thread(thread);

    inode->size       = 0;
    inode->space_used = 0;
    inode->uid        = request->cred->uid;
    inode->gid        = request->cred->gid;
    inode->nlink      = 0;
    inode->mode       = S_IFREG |  0644;
    inode->atime      = now;
    inode->mtime      = now;
    inode->ctime      = now;
    inode->change++;
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
    struct memfs_inode        *inode;
    struct memfs_named_stream *stream    = NULL;
    struct memfs_stream_open  *stream_op = NULL;
    uint64_t                   vp        = request->close.vfs_private;

    if (vp & 1) {
        stream_op = (struct memfs_stream_open *) (uintptr_t) (vp & ~1ULL);
        inode     = stream_op->inode;
        stream    = stream_op->stream;
    } else {
        inode = (struct memfs_inode *) (uintptr_t) vp;
    }

    pthread_mutex_lock(&inode->lock);

    /* Unhook this descriptor from the inode's live-open list before any cascade
     * below (inode_free) walks it, so its memory is freed exactly once -- here,
     * after the lock is dropped. */
    if (stream_op) {
        struct memfs_stream_open **opp;

        for (opp = &inode->stream_opens; *opp; opp = &(*opp)->open_next) {
            if (*opp == stream_op) {
                *opp = stream_op->open_next;
                break;
            }
        }
    }

    /* Release the stream node first.  An unlinked stream (removed by
     * remove_stream while still open) is no longer in inode->streams, so the
     * inode_free cascade below would miss it -- free it here on its last
     * close, independent of whether the inode itself is being freed. */
    if (stream && stream->refcnt > 0) {
        stream->refcnt--;
    }
    if (stream && stream->refcnt == 0 && !stream->linked) {
        memfs_dead_stream_detach(inode, stream);
        memfs_stream_node_free(thread, stream);
    }

    --inode->refcnt;

    if (inode->refcnt == 0) {
        /* Frees the inode and cascades its remaining (still-linked) streams. */
        memfs_inode_free(thread, inode);
    }

    pthread_mutex_unlock(&inode->lock);

    if (stream_op) {
        free(stream_op);
    }

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
    struct memfs_inode        *inode;
    struct memfs_named_stream *stream;
    struct memfs_fork         *fork;
    uint64_t                   fork_size;
    struct memfs_block        *block;
    struct evpl_iovec_cursor   cursor;
    uint64_t                   offset, length;
    uint32_t                   eof = 0;
    uint64_t                   first_block, last_block, max_iov, bi;
    uint32_t                   block_offset, left, block_len;
    struct evpl_iovec         *iov;
    int                        niov = 0;
    struct timespec            now;

    chimera_vfs_realtime(&now);

    offset = request->read.offset;
    length = request->read.length;

    /* memfs advertises CAP_READ_PROVIDES_BUFFERS: it returns zero-copy refs to
     * its own SHARED block iovecs, so the VFS core never pre-allocates buffers
     * for it (buffers_provided is always 0).  If a future VFS data cache ever
     * hands memfs buffers to populate, this is where memfs would memcpy its
     * block data into request->read.iov instead of cloning refs below. */
    chimera_memfs_abort_if(request->read.buffers_provided,
                           "memfs read received VFS-provided buffers but only "
                           "implements the zero-copy ref path");

    if (unlikely(length == 0)) {
        request->status        = CHIMERA_VFS_OK;
        request->read.r_niov   = 0;
        request->read.r_length = length;
        request->read.r_eof    = eof;
        request->complete(request);
        return;
    }

    inode = memfs_resolve_io(shared, request->read.handle,
                             request->fh, request->fh_len, &stream);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    fork      = stream ? &stream->fork : &inode->file;
    fork_size = stream ? stream->size : inode->size;

    /* Read authorization is enforced by the VFS-layer ACL gate (and the
     * credential-keyed open cache), not by an ACL-blind mode check here -- so
     * main's memfs_cred_can_read() check is intentionally dropped on this
     * branch (it is undefined here and would double-evaluate / ignore ACLs). */

    if (unlikely(fork_size <= offset)) {
        memfs_map_attrs_fork(shared, &request->read.r_attr, inode, stream, request->fh);
        pthread_mutex_unlock(&inode->lock);
        request->status        = CHIMERA_VFS_OK;
        request->read.r_niov   = 0;
        request->read.r_length = 0;
        request->read.r_eof    = 1;
        request->complete(request);
        return;
    }

    if (length >= fork_size - offset) {
        length = fork_size - offset;
        eof    = 1;
    }

    if (length == 0) {
        /* Nothing to read. Returning early also avoids the
         * (offset + length - 1) underflow below that would spin the
         * block loop on a zero-length request. */
        memfs_map_attrs_fork(shared, &request->read.r_attr, inode, stream, request->fh);
        pthread_mutex_unlock(&inode->lock);
        request->status        = CHIMERA_VFS_OK;
        request->read.r_niov   = 0;
        request->read.r_length = 0;
        request->read.r_eof    = eof;
        request->complete(request);
        return;
    }

    const uint32_t block_size  = shared->block_size;
    const uint32_t block_shift = shared->block_shift;
    const uint32_t block_mask  = shared->block_mask;

    first_block  = offset >> block_shift;
    block_offset = offset & block_mask;
    last_block   = (offset + length - 1) >> block_shift;
    left         = length;

    max_iov = request->read.niov;

    iov = request->read.iov;

    for (bi = first_block; bi <= last_block; bi++) {

        if (left < block_size - block_offset) {
            block_len = left;
        } else {
            block_len = block_size - block_offset;
        }

        if (fork->blocks && bi < fork->num_blocks) {
            block = fork->blocks[bi];
        } else {
            block = NULL;
        }

        if (!block) {
            if (niov >= max_iov) {
                break;
            }
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

    /* relatime: only advance atime when the file changed since last access or
     * the recorded atime is a day stale, so steady-state reads return identical
     * attrs (and stop churning the VFS attr cache). */
    if (!shared->noatime &&
        chimera_vfs_relatime_needs_update(&inode->atime, &inode->mtime, &inode->ctime, &now)) {
        inode->atime = now;
    }

    memfs_map_attrs_fork(shared, &request->read.r_attr, inode, stream, request->fh);

    pthread_mutex_unlock(&inode->lock);

    request->status        = CHIMERA_VFS_OK;
    request->read.r_niov   = niov;
    request->read.r_length = length - left;
    request->read.r_eof    = left ? 0 : eof;

    request->complete(request);
} /* memfs_read */

static void
memfs_write(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct evpl               *evpl = thread->evpl;
    struct memfs_inode        *inode;
    struct memfs_named_stream *stream;
    struct memfs_fork         *fork;
    uint64_t                  *p_size, *p_space_used;
    struct memfs_block       **blocks, *block, *old_block;
    struct evpl_iovec_cursor   cursor, old_block_cursor;
    uint64_t                   first_block, last_block, bi;
    uint32_t                   block_offset, left, block_len;
    struct timespec            now;

    chimera_vfs_realtime(&now);

    const uint32_t             block_size  = shared->block_size;
    const uint32_t             block_shift = shared->block_shift;
    const uint32_t             block_mask  = shared->block_mask;

    evpl_iovec_cursor_init(&cursor, request->write.iov, request->write.niov);

    first_block  = request->write.offset >> block_shift;
    block_offset = request->write.offset & block_mask;
    last_block   = (request->write.offset + request->write.length - 1) >>
        block_shift;
    left = request->write.length;

    inode = memfs_resolve_io(shared, request->write.handle,
                             request->fh, request->fh_len, &stream);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    if (!S_ISREG(inode->mode)) {
        request->status = S_ISDIR(inode->mode) ?
            CHIMERA_VFS_EISDIR : CHIMERA_VFS_EINVAL;
        pthread_mutex_unlock(&inode->lock);
        request->complete(request);
        return;
    }

    fork         = stream ? &stream->fork : &inode->file;
    p_size       = stream ? &stream->size : &inode->size;
    p_space_used = stream ? &stream->space_used : &inode->space_used;

    /* Write access is enforced at the VFS layer (credential-keyed gate); see
     * the note in memfs_open_at. */

    memfs_map_attrs_fork(shared, &request->write.r_pre_attr, inode, stream, request->fh);

    if (request->write.length == 0) {
        /* A zero-length write changes nothing. Returning early also avoids
         * the (offset + length - 1) underflow above, which drives last_block
         * to a huge value and spins the 32-bit block-growth loop forever. */
        memfs_map_attrs_fork(shared, &request->write.r_post_attr, inode, stream, request->fh);
        pthread_mutex_unlock(&inode->lock);
        request->status         = CHIMERA_VFS_OK;
        request->write.r_length = 0;
        request->write.r_sync   = CHIMERA_VFS_WRITE_FILESYNC;
        request->complete(request);
        return;
    }

    if (fork->max_blocks <= last_block || !fork->blocks) {
        struct memfs_block **new_blocks;
        unsigned int         new_max_blocks;

        blocks = fork->blocks;

        new_max_blocks = 1024;

        while (new_max_blocks <= last_block) {
            new_max_blocks <<= 1;
        }

        /* calloc (not malloc+memset) so a sparse write at a high block index
         * does not force the whole pointer array resident: large allocations
         * are served by mmap and the unwritten tail stays backed by the zero
         * page until a block is actually stored there. */
        new_blocks = calloc(new_max_blocks, sizeof(struct memfs_block *));

        if (!new_blocks) {
            pthread_mutex_unlock(&inode->lock);
            request->status = CHIMERA_VFS_ENOSPC;
            request->complete(request);
            return;
        }

        if (blocks) {
            memcpy(new_blocks, blocks,
                   fork->num_blocks * sizeof(struct memfs_block *));
            free(blocks);
        }

        fork->blocks     = new_blocks;
        fork->max_blocks = new_max_blocks;
    }

    /* Only increase num_blocks, never decrease it during write */
    if (last_block + 1 > fork->num_blocks) {
        fork->num_blocks = last_block + 1;
    }

    for (bi = first_block; bi <= last_block; bi++) {

        block_len = block_size - block_offset;

        if (left < block_len) {
            block_len = left;
        }

        old_block = fork->blocks ? fork->blocks[bi] : NULL;

        /* Overwriting an existing block is net-zero (new block paired with the
        * old block's free below), so don't charge it -- a write into already
        * allocated space must succeed even when the device is exactly full. */
        block = memfs_block_alloc_charged(thread, old_block ? 0 : 1);

        if (!block) {
            pthread_mutex_unlock(&inode->lock);
            request->status = CHIMERA_VFS_ENOSPC;
            request->complete(request);
            return;
        }

        block->niov = evpl_iovec_alloc(evpl, block_size, 4096,
                                       CHIMERA_MEMFS_BLOCK_MAX_IOV,
                                       EVPL_IOVEC_FLAG_SHARED, block->iov);

        if (block_offset || block_len < block_size) {

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
                                       block_size - block_len -
                                       block_offset);

                fork->blocks[bi] = NULL;
                memfs_block_free_charged(thread, old_block, 0);
            } else {
                memset(block->iov[0].data, 0, block_offset);

                memset(block->iov[0].data + block_offset + block_len, 0,
                       block_size - block_offset - block_len);
            }
        } else if (old_block) {
            /* Full block overwrite: free the old block (net-zero, no uncharge) */
            fork->blocks[bi] = NULL;
            memfs_block_free_charged(thread, old_block, 0);
        }

        evpl_iovec_cursor_copy(&cursor,
                               block->iov[0].data + block_offset,
                               block_len);

        fork->blocks[bi] = block;
        block_offset     = 0;
        left            -= block_len;
    }

    if (*p_size < request->write.offset + request->write.length) {
        *p_size       = request->write.offset + request->write.length;
        *p_space_used = (*p_size + 4095) & ~4095;
    }

    inode->mtime = now;
    inode->ctime = now;
    inode->change++;

    /* POSIX kill-priv: a non-privileged write to a regular file clears the
     * set-user-ID bit and the set-group-ID bit (when group-executable).  Named
     * streams share the parent inode's mode, so this applies on either path. */
    inode->mode = chimera_vfs_killpriv_mode(request->cred, inode->mode);

    memfs_map_attrs_fork(shared, &request->write.r_post_attr, inode, stream, request->fh);

    pthread_mutex_unlock(&inode->lock);

    request->status         = CHIMERA_VFS_OK;
    request->write.r_length = request->write.length;
    request->write.r_sync   = CHIMERA_VFS_WRITE_FILESYNC;

    request->complete(request);
} /* memfs_write */


static int memfs_grow_blocks(
    struct memfs_inode *inode,
    uint64_t            last_block);

static void
memfs_allocate(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct evpl               *evpl = thread->evpl;
    struct memfs_inode        *inode;
    struct memfs_named_stream *stream;
    struct memfs_fork         *fork;
    uint64_t                  *p_size, *p_space_used;
    struct timespec            now;

    chimera_vfs_realtime(&now);

    inode = memfs_resolve_io(shared, request->allocate.handle,
                             request->fh, request->fh_len, &stream);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    fork         = stream ? &stream->fork : &inode->file;
    p_size       = stream ? &stream->size : &inode->size;
    p_space_used = stream ? &stream->space_used : &inode->space_used;

    memfs_map_attrs_fork(shared, &request->allocate.r_pre_attr, inode, stream, request->fh);

    if (request->allocate.flags & CHIMERA_VFS_ALLOCATE_DEALLOCATE) {
        /* DEALLOCATE: punch hole in [offset, offset+length) */
        const uint32_t block_size  = shared->block_size;
        const uint32_t block_shift = shared->block_shift;
        uint64_t       hole_start  = request->allocate.offset;
        uint64_t       hole_end    = hole_start + request->allocate.length;
        uint64_t       first_block, last_block, bi;

        if (hole_end > *p_size) {
            hole_end = *p_size;
        }

        if (hole_start < hole_end && fork->blocks) {
            first_block = hole_start >> block_shift;
            last_block  = (hole_end - 1) >> block_shift;

            for (bi = first_block; bi <= last_block && bi < fork->num_blocks; bi++) {
                if (!fork->blocks[bi]) {
                    continue;
                }

                uint64_t block_start = bi << block_shift;
                uint64_t block_end   = block_start + block_size;

                if (hole_start <= block_start && hole_end >= block_end) {
                    /* Entire block is within hole - free it */
                    memfs_block_free(thread, fork->blocks[bi]);
                    fork->blocks[bi] = NULL;
                } else {
                    /* Partial block - COW and zero the hole portion */
                    struct memfs_block      *old_block = fork->blocks[bi];
                    struct memfs_block      *new_block;
                    struct evpl_iovec_cursor old_cursor;
                    uint32_t                 zero_start, zero_end;

                    zero_start = (hole_start > block_start) ?
                        (hole_start - block_start) : 0;
                    zero_end = (hole_end < block_end) ?
                        (hole_end - block_start) : block_size;

                    /* Net-zero replace of a partial boundary block: no charge. */
                    new_block = memfs_block_alloc_charged(thread, 0);

                    if (!new_block) {
                        pthread_mutex_unlock(&inode->lock);
                        request->status = CHIMERA_VFS_ENOSPC;
                        request->complete(request);
                        return;
                    }

                    new_block->niov = evpl_iovec_alloc(evpl, block_size, 4096,
                                                       CHIMERA_MEMFS_BLOCK_MAX_IOV,
                                                       EVPL_IOVEC_FLAG_SHARED,
                                                       new_block->iov);

                    /* Copy entire old block, then zero the hole portion */
                    evpl_iovec_cursor_init(&old_cursor, old_block->iov,
                                           old_block->niov);
                    evpl_iovec_cursor_copy(&old_cursor,
                                           new_block->iov[0].data,
                                           block_size);

                    memset(new_block->iov[0].data + zero_start, 0,
                           zero_end - zero_start);

                    memfs_block_free_charged(thread, old_block, 0);
                    fork->blocks[bi] = new_block;
                }
            }

            *p_space_used = 0;

            for (bi = 0; bi < fork->num_blocks; bi++) {
                if (fork->blocks[bi]) {
                    *p_space_used += block_size;
                }
            }
        }
    } else {
        /* ALLOCATE: reserve space for [offset, offset+length) and extend size. */
        uint64_t new_end = request->allocate.offset + request->allocate.length;

        /* In capacity mode, materialize zero blocks across the range so the
         * space is actually consumed -- and charged at the block alloc/free
         * choke points.  This keeps the accounting honest under the operations
         * the reservation model got wrong: a later write reuses these blocks
         * (alloc-new + free-old nets to zero, so no double charge), and a
         * DEALLOCATE frees them (releasing the space).  block_alloc returns
         * NULL (ENOSPC) when the filesystem is full.  Streams keep the cheap
         * size-only path (memfs_grow_blocks operates on the base fork). */
        if (shared->fs_size && !stream && request->allocate.length) {
            const uint32_t block_size  = shared->block_size;
            const uint32_t block_shift = shared->block_shift;
            uint64_t       first_block = request->allocate.offset >> block_shift;
            uint64_t       last_block  = (new_end - 1) >> block_shift;
            uint64_t       bi;

            if (memfs_grow_blocks(inode, last_block) != 0) {
                pthread_mutex_unlock(&inode->lock);
                request->status = CHIMERA_VFS_ENOSPC;
                request->complete(request);
                return;
            }

            if (last_block + 1 > fork->num_blocks) {
                fork->num_blocks = last_block + 1;
            }

            for (bi = first_block; bi <= last_block; bi++) {
                struct memfs_block *block;

                if (fork->blocks[bi]) {
                    continue;   /* already materialized */
                }

                block = memfs_block_alloc(thread);

                if (!block) {
                    pthread_mutex_unlock(&inode->lock);
                    request->status = CHIMERA_VFS_ENOSPC;
                    request->complete(request);
                    return;
                }

                block->niov = evpl_iovec_alloc(evpl, block_size, 4096,
                                               CHIMERA_MEMFS_BLOCK_MAX_IOV,
                                               EVPL_IOVEC_FLAG_SHARED, block->iov);
                memset(block->iov[0].data, 0, block_size);
                fork->blocks[bi] = block;
            }
        }

        if (new_end > *p_size) {
            *p_size = new_end;
        }
    }

    inode->mtime = now;
    inode->ctime = now;
    inode->change++;

    memfs_map_attrs_fork(shared, &request->allocate.r_post_attr, inode, stream, request->fh);

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_allocate */

/* Copy `len` bytes starting at `src_pos` from src inode into the flat buffer
 * `dst`. Reads from src blocks; holes and past-EOF read as zeros.
 * Returns the number of bytes within file bounds; the caller's view of
 * how much was logically copied is min(len, src->size - src_pos).
 */
static void
memfs_copy_from_inode(
    struct memfs_shared *shared,
    struct memfs_inode  *src,
    uint64_t             src_pos,
    void                *dst,
    uint32_t             len)
{
    const uint32_t block_size  = shared->block_size;
    const uint32_t block_shift = shared->block_shift;
    const uint32_t block_mask  = shared->block_mask;
    uint8_t       *out         = dst;

    while (len > 0) {
        uint64_t            src_bi  = src_pos >> block_shift;
        uint32_t            src_off = src_pos & block_mask;
        uint32_t            chunk   = block_size - src_off;
        struct memfs_block *sb;

        if (chunk > len) {
            chunk = len;
        }

        if (src->file.blocks && src_bi < src->file.num_blocks) {
            sb = src->file.blocks[src_bi];
        } else {
            sb = NULL;
        }

        if (sb) {
            memcpy(out, (uint8_t *) sb->iov[0].data + src_off, chunk);
        } else {
            memset(out, 0, chunk);
        }

        out     += chunk;
        src_pos += chunk;
        len     -= chunk;
    }
} /* memfs_copy_from_inode */

static void
memfs_recompute_space_used(
    struct memfs_shared *shared,
    struct memfs_inode  *inode)
{
    const uint32_t block_size = shared->block_size;
    uint64_t       bi;

    inode->space_used = 0;

    if (!inode->file.blocks) {
        return;
    }

    for (bi = 0; bi < inode->file.num_blocks; bi++) {
        if (inode->file.blocks[bi]) {
            inode->space_used += block_size;
        }
    }
} /* memfs_recompute_space_used */

static int
memfs_grow_blocks(
    struct memfs_inode *inode,
    uint64_t            last_block)
{
    struct memfs_block **new_blocks;
    unsigned int         new_max_blocks;

    if (inode->file.blocks && inode->file.max_blocks > last_block) {
        return 0;
    }

    new_max_blocks = inode->file.max_blocks ? inode->file.max_blocks : 1024;

    while (new_max_blocks <= last_block) {
        new_max_blocks <<= 1;
    }

    new_blocks = malloc(new_max_blocks * sizeof(struct memfs_block *));

    if (!new_blocks) {
        return -1;
    }

    if (inode->file.blocks) {
        memcpy(new_blocks, inode->file.blocks,
               inode->file.num_blocks * sizeof(struct memfs_block *));
        free(inode->file.blocks);
    }

    memset(new_blocks + inode->file.num_blocks, 0,
           (new_max_blocks - inode->file.num_blocks) *
           sizeof(struct memfs_block *));

    inode->file.blocks     = new_blocks;
    inode->file.max_blocks = new_max_blocks;
    return 0;
} /* memfs_grow_blocks */

static void
memfs_copy_range(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct evpl        *evpl        = thread->evpl;
    const uint32_t      block_size  = shared->block_size;
    const uint32_t      block_shift = shared->block_shift;
    const uint32_t      block_mask  = shared->block_mask;
    struct memfs_inode *src_inode, *dst_inode;
    struct memfs_block *old_block, *new_block;
    uint64_t            src_offset, dst_offset, length, src_eof_len;
    uint64_t            first_block, last_block, bi;
    uint32_t            block_offset, left, block_len;
    uint64_t            copied = 0;
    struct timespec     now;

    if (request->copy_range.src_handle->vfs_module !=
        request->copy_range.dst_handle->vfs_module) {
        request->status = CHIMERA_VFS_ENOTSUP;
        request->complete(request);
        return;
    }

    /* Range copy between named streams is not supported; a tagged vfs_private
     * is a stream descriptor, not an inode pointer. */
    if ((request->copy_range.src_handle->vfs_private & 1) ||
        (request->copy_range.dst_handle->vfs_private & 1)) {
        request->status = CHIMERA_VFS_ENOTSUP;
        request->complete(request);
        return;
    }

    src_inode = (struct memfs_inode *) request->copy_range.src_handle->vfs_private;
    dst_inode = (struct memfs_inode *) request->copy_range.dst_handle->vfs_private;

    if (!src_inode || !dst_inode) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    src_offset = request->copy_range.src_offset;
    dst_offset = request->copy_range.dst_offset;
    length     = request->copy_range.length;

    if (length == 0) {
        request->status              = CHIMERA_VFS_OK;
        request->copy_range.r_length = 0;
        request->complete(request);
        return;
    }

    /* Same file: reject overlap (POSIX copy_file_range semantics) */
    if (src_inode == dst_inode) {
        uint64_t s_end = src_offset + length;
        uint64_t d_end = dst_offset + length;
        if (src_offset < d_end && dst_offset < s_end) {
            request->status = CHIMERA_VFS_EINVAL;
            request->complete(request);
            return;
        }
    }

    chimera_vfs_realtime(&now);

    /* Lock in deterministic order to avoid AB/BA deadlock */
    if (src_inode == dst_inode) {
        pthread_mutex_lock(&src_inode->lock);
    } else if (src_inode < dst_inode) {
        pthread_mutex_lock(&src_inode->lock);
        pthread_mutex_lock(&dst_inode->lock);
    } else {
        pthread_mutex_lock(&dst_inode->lock);
        pthread_mutex_lock(&src_inode->lock);
    }

    memfs_map_attrs(shared, &request->copy_range.r_pre_attr, dst_inode,
                    request->copy_range.dst_handle->fh);

    /* Clamp length to what's available in source */
    if (src_offset >= src_inode->size) {
        src_eof_len = 0;
    } else {
        src_eof_len = src_inode->size - src_offset;
        if (src_eof_len > length) {
            src_eof_len = length;
        }
    }

    if (src_eof_len == 0) {
        memfs_map_attrs(shared, &request->copy_range.r_post_attr, dst_inode,
                        request->copy_range.dst_handle->fh);
        if (src_inode != dst_inode) {
            pthread_mutex_unlock(&src_inode->lock);
        }
        pthread_mutex_unlock(&dst_inode->lock);
        request->status              = CHIMERA_VFS_OK;
        request->copy_range.r_length = 0;
        request->complete(request);
        return;
    }

    length       = src_eof_len;
    first_block  = dst_offset >> block_shift;
    block_offset = dst_offset & block_mask;
    last_block   = (dst_offset + length - 1) >> block_shift;
    left         = length;

    if (memfs_grow_blocks(dst_inode, last_block) != 0) {
        if (src_inode != dst_inode) {
            pthread_mutex_unlock(&src_inode->lock);
        }
        pthread_mutex_unlock(&dst_inode->lock);
        request->status = CHIMERA_VFS_ENOSPC;
        request->complete(request);
        return;
    }

    if (last_block + 1 > dst_inode->file.num_blocks) {
        dst_inode->file.num_blocks = last_block + 1;
    }

    for (bi = first_block; bi <= last_block; bi++) {
        block_len = block_size - block_offset;
        if (left < block_len) {
            block_len = left;
        }

        old_block = dst_inode->file.blocks[bi];
        new_block = memfs_block_alloc(thread);

        if (!new_block) {
            if (src_inode != dst_inode) {
                pthread_mutex_unlock(&src_inode->lock);
            }
            pthread_mutex_unlock(&dst_inode->lock);
            request->status = CHIMERA_VFS_ENOSPC;
            request->complete(request);
            return;
        }

        new_block->niov = evpl_iovec_alloc(evpl, block_size, 4096,
                                           CHIMERA_MEMFS_BLOCK_MAX_IOV,
                                           EVPL_IOVEC_FLAG_SHARED,
                                           new_block->iov);

        /* Preserve edges of the destination block outside [block_offset, +block_len) */
        if (block_offset || block_len < block_size) {
            if (old_block) {
                if (block_offset) {
                    memcpy(new_block->iov[0].data,
                           old_block->iov[0].data, block_offset);
                }
                uint32_t tail_off = block_offset + block_len;
                if (tail_off < block_size) {
                    memcpy((uint8_t *) new_block->iov[0].data + tail_off,
                           (uint8_t *) old_block->iov[0].data + tail_off,
                           block_size - tail_off);
                }
            } else {
                memset(new_block->iov[0].data, 0, block_offset);
                uint32_t tail_off = block_offset + block_len;
                if (tail_off < block_size) {
                    memset((uint8_t *) new_block->iov[0].data + tail_off, 0,
                           block_size - tail_off);
                }
            }
        }

        memfs_copy_from_inode(shared, src_inode, src_offset + copied,
                              (uint8_t *) new_block->iov[0].data + block_offset,
                              block_len);

        if (old_block) {
            memfs_block_free(thread, old_block);
        }
        dst_inode->file.blocks[bi] = new_block;

        copied      += block_len;
        left        -= block_len;
        block_offset = 0;
    }

    if (dst_inode->size < dst_offset + length) {
        dst_inode->size = dst_offset + length;
    }

    memfs_recompute_space_used(shared, dst_inode);

    dst_inode->mtime = now;
    dst_inode->ctime = now;
    dst_inode->change++;
    src_inode->atime = now;

    memfs_map_attrs(shared, &request->copy_range.r_post_attr, dst_inode,
                    request->copy_range.dst_handle->fh);

    if (src_inode != dst_inode) {
        pthread_mutex_unlock(&src_inode->lock);
    }
    pthread_mutex_unlock(&dst_inode->lock);

    request->status              = CHIMERA_VFS_OK;
    request->copy_range.r_length = copied;
    request->complete(request);
} /* memfs_copy_range */

static void
memfs_move_range(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    const uint32_t      block_shift = shared->block_shift;
    const uint32_t      block_mask  = shared->block_mask;
    struct memfs_inode *src_inode, *dst_inode;
    uint64_t            src_offset, dst_offset, length;
    uint64_t            first_block, last_block, bi;
    uint64_t            src_first_block, n_blocks;
    struct timespec     now;

    if (request->move_range.src_handle->vfs_module !=
        request->move_range.dst_handle->vfs_module) {
        request->status = CHIMERA_VFS_ENOTSUP;
        request->complete(request);
        return;
    }

    src_offset = request->move_range.src_offset;
    dst_offset = request->move_range.dst_offset;
    length     = request->move_range.length;

    /* Move is zero-copy at block granularity */
    if ((src_offset & block_mask) ||
        (dst_offset & block_mask) ||
        (length     & block_mask)) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    if (length == 0) {
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    /* Range move between named streams is not supported. */
    if ((request->move_range.src_handle->vfs_private & 1) ||
        (request->move_range.dst_handle->vfs_private & 1)) {
        request->status = CHIMERA_VFS_ENOTSUP;
        request->complete(request);
        return;
    }

    src_inode = (struct memfs_inode *) request->move_range.src_handle->vfs_private;
    dst_inode = (struct memfs_inode *) request->move_range.dst_handle->vfs_private;

    if (!src_inode || !dst_inode) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    if (src_inode == dst_inode) {
        uint64_t s_end = src_offset + length;
        uint64_t d_end = dst_offset + length;
        if (src_offset < d_end && dst_offset < s_end) {
            request->status = CHIMERA_VFS_EINVAL;
            request->complete(request);
            return;
        }
    }

    chimera_vfs_realtime(&now);

    if (src_inode == dst_inode) {
        pthread_mutex_lock(&src_inode->lock);
    } else if (src_inode < dst_inode) {
        pthread_mutex_lock(&src_inode->lock);
        pthread_mutex_lock(&dst_inode->lock);
    } else {
        pthread_mutex_lock(&dst_inode->lock);
        pthread_mutex_lock(&src_inode->lock);
    }

    memfs_map_attrs(shared, &request->move_range.r_dst_pre_attr, dst_inode,
                    request->move_range.dst_handle->fh);

    first_block     = dst_offset >> block_shift;
    last_block      = (dst_offset + length - 1) >> block_shift;
    src_first_block = src_offset >> block_shift;
    n_blocks        = length >> block_shift;

    if (memfs_grow_blocks(dst_inode, last_block) != 0) {
        if (src_inode != dst_inode) {
            pthread_mutex_unlock(&src_inode->lock);
        }
        pthread_mutex_unlock(&dst_inode->lock);
        request->status = CHIMERA_VFS_ENOSPC;
        request->complete(request);
        return;
    }

    if (last_block + 1 > dst_inode->file.num_blocks) {
        dst_inode->file.num_blocks = last_block + 1;
    }

    for (bi = 0; bi < n_blocks; bi++) {
        uint64_t            si = src_first_block + bi;
        uint64_t            di = first_block + bi;
        struct memfs_block *src_block;

        if (src_inode->file.blocks && si < src_inode->file.num_blocks) {
            src_block                  = src_inode->file.blocks[si];
            src_inode->file.blocks[si] = NULL;
        } else {
            src_block = NULL;
        }

        /* Free anything currently at the destination slot before overwriting */
        if (dst_inode->file.blocks[di]) {
            memfs_block_free(thread, dst_inode->file.blocks[di]);
        }

        dst_inode->file.blocks[di] = src_block;
    }

    if (dst_inode->size < dst_offset + length) {
        dst_inode->size = dst_offset + length;
    }

    memfs_recompute_space_used(shared, dst_inode);
    memfs_recompute_space_used(shared, src_inode);

    dst_inode->mtime = now;
    dst_inode->ctime = now;
    dst_inode->change++;
    src_inode->mtime = now;
    src_inode->ctime = now;
    src_inode->change++;

    memfs_map_attrs(shared, &request->move_range.r_dst_post_attr, dst_inode,
                    request->move_range.dst_handle->fh);
    memfs_map_attrs(shared, &request->move_range.r_src_post_attr, src_inode,
                    request->move_range.src_handle->fh);

    if (src_inode != dst_inode) {
        pthread_mutex_unlock(&src_inode->lock);
    }
    pthread_mutex_unlock(&dst_inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_move_range */

static void
memfs_clone_range(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct evpl        *evpl        = thread->evpl;
    const uint32_t      block_size  = shared->block_size;
    const uint32_t      block_shift = shared->block_shift;
    const uint32_t      block_mask  = shared->block_mask;
    struct memfs_inode *src_inode, *dst_inode;
    struct memfs_block *old_block, *new_block;
    uint64_t            src_offset, dst_offset, length;
    uint64_t            first_block, last_block, bi;
    uint32_t            block_offset, left, block_len;
    uint64_t            copied = 0;
    struct timespec     now;

    if (request->clone_range.src_handle->vfs_module !=
        request->clone_range.dst_handle->vfs_module) {
        request->status = CHIMERA_VFS_ENOTSUP;
        request->complete(request);
        return;
    }

    src_offset = request->clone_range.src_offset;
    dst_offset = request->clone_range.dst_offset;
    length     = request->clone_range.length;

    /* Honour clone-granularity (4 KiB) alignment rather than the larger internal
     * block size: whole internal blocks are shared copy-on-write below, partial
     * edges fall back to read-modify-write.  Sub-cluster offsets/lengths are
     * still rejected, matching POSIX FICLONERANGE block-alignment semantics. */
    if ((src_offset & (CHIMERA_MEMFS_CLONE_ALIGN - 1)) ||
        (dst_offset & (CHIMERA_MEMFS_CLONE_ALIGN - 1)) ||
        (length & (CHIMERA_MEMFS_CLONE_ALIGN - 1))) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    if (length == 0) {
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }

    /* Range clone between named streams is not supported. */
    if ((request->clone_range.src_handle->vfs_private & 1) ||
        (request->clone_range.dst_handle->vfs_private & 1)) {
        request->status = CHIMERA_VFS_ENOTSUP;
        request->complete(request);
        return;
    }

    src_inode = (struct memfs_inode *) request->clone_range.src_handle->vfs_private;
    dst_inode = (struct memfs_inode *) request->clone_range.dst_handle->vfs_private;

    if (!src_inode || !dst_inode) {
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    if (src_inode == dst_inode) {
        uint64_t s_end = src_offset + length;
        uint64_t d_end = dst_offset + length;
        if (src_offset < d_end && dst_offset < s_end) {
            request->status = CHIMERA_VFS_EINVAL;
            request->complete(request);
            return;
        }
    }

    chimera_vfs_realtime(&now);

    if (src_inode == dst_inode) {
        pthread_mutex_lock(&src_inode->lock);
    } else if (src_inode < dst_inode) {
        pthread_mutex_lock(&src_inode->lock);
        pthread_mutex_lock(&dst_inode->lock);
    } else {
        pthread_mutex_lock(&dst_inode->lock);
        pthread_mutex_lock(&src_inode->lock);
    }

    memfs_map_attrs(shared, &request->clone_range.r_pre_attr, dst_inode,
                    request->clone_range.dst_handle->fh);

    first_block  = dst_offset >> block_shift;
    block_offset = dst_offset & block_mask;
    last_block   = (dst_offset + length - 1) >> block_shift;
    left         = length;

    if (memfs_grow_blocks(dst_inode, last_block) != 0) {
        if (src_inode != dst_inode) {
            pthread_mutex_unlock(&src_inode->lock);
        }
        pthread_mutex_unlock(&dst_inode->lock);
        request->status = CHIMERA_VFS_ENOSPC;
        request->complete(request);
        return;
    }

    if (last_block + 1 > dst_inode->file.num_blocks) {
        dst_inode->file.num_blocks = last_block + 1;
    }

    for (bi = first_block; bi <= last_block; bi++) {
        uint64_t            cur_src_off = src_offset + copied;
        uint64_t            si          = cur_src_off >> block_shift;
        struct memfs_block *src_block;
        int                 whole_block;

        block_len = block_size - block_offset;
        if (left < block_len) {
            block_len = left;
        }

        if (src_inode->file.blocks && si < src_inode->file.num_blocks) {
            src_block = src_inode->file.blocks[si];
        } else {
            src_block = NULL;
        }

        old_block = dst_inode->file.blocks[bi];

        /* A full-block hole in the source stays sparse at the destination
         * (reads as zeros) -- preserves sparseness across the clone. */
        if (!src_block && block_offset == 0 && block_len == block_size) {
            if (old_block) {
                memfs_block_free(thread, old_block);
                dst_inode->file.blocks[bi] = NULL;
            }
            copied      += block_len;
            left        -= block_len;
            block_offset = 0;
            continue;
        }

        /* Zero-copy fast path: the clone range fully covers this internal
         * block, the source position is block-aligned, and the source block is
         * fully backed (not a trailing partial block or a hole).  Share the
         * source iovecs copy-on-write -- writes on either side allocate a fresh
         * block, so the sharing is invisible. */
        whole_block = (block_offset == 0 && block_len == block_size &&
                       (cur_src_off & block_mask) == 0 &&
                       cur_src_off + block_size <= src_inode->size &&
                       src_block != NULL);

        new_block = memfs_block_alloc(thread);

        if (!new_block) {
            if (src_inode != dst_inode) {
                pthread_mutex_unlock(&src_inode->lock);
            }
            pthread_mutex_unlock(&dst_inode->lock);
            request->status = CHIMERA_VFS_ENOSPC;
            request->complete(request);
            return;
        }

        if (whole_block) {
            new_block->niov = src_block->niov;
            for (int j = 0; j < src_block->niov; j++) {
                evpl_iovec_clone_segment(&new_block->iov[j],
                                         &src_block->iov[j],
                                         0,
                                         src_block->iov[j].length);
            }
        } else {
            /* Partial / misaligned edge: read-modify-write.  Allocate backing,
             * preserve the destination bytes outside [block_offset, +block_len),
             * then copy the covered range from the source (memfs_copy_from_inode
             * zero-fills past the source's EOF, matching hole semantics). */
            new_block->niov = evpl_iovec_alloc(evpl, block_size, 4096,
                                               CHIMERA_MEMFS_BLOCK_MAX_IOV,
                                               EVPL_IOVEC_FLAG_SHARED,
                                               new_block->iov);

            if (block_offset || block_len < block_size) {
                if (old_block) {
                    if (block_offset) {
                        memcpy(new_block->iov[0].data,
                               old_block->iov[0].data, block_offset);
                    }
                    uint32_t tail_off = block_offset + block_len;
                    if (tail_off < block_size) {
                        memcpy((uint8_t *) new_block->iov[0].data + tail_off,
                               (uint8_t *) old_block->iov[0].data + tail_off,
                               block_size - tail_off);
                    }
                } else {
                    memset(new_block->iov[0].data, 0, block_offset);
                    uint32_t tail_off = block_offset + block_len;
                    if (tail_off < block_size) {
                        memset((uint8_t *) new_block->iov[0].data + tail_off, 0,
                               block_size - tail_off);
                    }
                }
            }

            memfs_copy_from_inode(shared, src_inode, cur_src_off,
                                  (uint8_t *) new_block->iov[0].data + block_offset,
                                  block_len);
        }

        if (old_block) {
            memfs_block_free(thread, old_block);
        }
        dst_inode->file.blocks[bi] = new_block;

        copied      += block_len;
        left        -= block_len;
        block_offset = 0;
    }

    if (dst_inode->size < dst_offset + length) {
        dst_inode->size = dst_offset + length;
    }

    memfs_recompute_space_used(shared, dst_inode);

    dst_inode->mtime = now;
    dst_inode->ctime = now;
    dst_inode->change++;
    src_inode->atime = now;

    memfs_map_attrs(shared, &request->clone_range.r_post_attr, dst_inode,
                    request->clone_range.dst_handle->fh);

    if (src_inode != dst_inode) {
        pthread_mutex_unlock(&src_inode->lock);
    }
    pthread_mutex_unlock(&dst_inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_clone_range */

static void
memfs_seek(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode        *inode;
    struct memfs_named_stream *stream;
    struct memfs_fork         *fork;
    uint64_t                   fork_size;
    uint64_t                   offset = request->seek.offset;
    uint64_t                   bi, block_start;

    inode = memfs_resolve_io(shared, request->seek.handle,
                             request->fh, request->fh_len, &stream);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    fork      = stream ? &stream->fork : &inode->file;
    fork_size = stream ? stream->size : inode->size;

    if (offset >= fork_size) {
        /* Neither data nor a hole exists at or beyond EOF, so SEEK must fail
         * with NXIO (POSIX lseek ENXIO / RFC 7862 NFS4ERR_NXIO) rather than
         * silently succeed. */
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_ENXIO;
        request->complete(request);
        return;
    }

    const uint32_t block_shift = shared->block_shift;

    if (request->seek.what == 0) {
        /* SEEK_DATA: find first non-NULL block from offset forward */
        bi = offset >> block_shift;

        while (bi < fork->num_blocks) {
            if (fork->blocks && fork->blocks[bi]) {
                block_start = bi << block_shift;

                request->seek.r_offset = (block_start > offset) ?
                    block_start : offset;
                request->seek.r_eof = 0;
                pthread_mutex_unlock(&inode->lock);
                request->status = CHIMERA_VFS_OK;
                request->complete(request);
                return;
            }
            bi++;
        }

        /* No data at or beyond the offset: SEEK_DATA fails with NXIO
         * (the trailing region is an implicit hole to EOF). */
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_ENXIO;
        request->complete(request);
        return;
    } else {
        /* SEEK_HOLE: find first NULL block or past num_blocks from offset */
        bi = offset >> block_shift;

        while (bi < fork->num_blocks) {
            if (!fork->blocks || !fork->blocks[bi]) {
                block_start = bi << block_shift;

                request->seek.r_offset = (block_start > offset) ?
                    block_start : offset;
                request->seek.r_eof = 0;
                pthread_mutex_unlock(&inode->lock);
                request->status = CHIMERA_VFS_OK;
                request->complete(request);
                return;
            }
            bi++;
        }

        /* Virtual hole at EOF - all blocks are data */
        request->seek.r_offset = fork->num_blocks <<
            block_shift;

        if (request->seek.r_offset < offset) {
            request->seek.r_offset = offset;
        }

        if (request->seek.r_offset >= fork_size) {
            request->seek.r_offset = fork_size;
        }

        /* No explicit hole block found before EOF: the match is the implicit
         * hole at the end of the file, so the search has reached EOF.  RFC 7862
         * §11.4.4 requires sr_eof TRUE here (the Linux client surfaces it to
         * lseek).  A trailing unallocated region that begins before the logical
         * size is a real hole short of EOF, so only flag eof once the returned
         * offset reaches fork_size. */
        request->seek.r_eof = (request->seek.r_offset >= fork_size);
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_OK;
        request->complete(request);
        return;
    }
} /* memfs_seek */

static void
memfs_symlink_at(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode  *parent_inode, *inode;
    struct memfs_dirent *dirent, *existing_dirent;
    struct timespec      now;
    uint64_t             hash;

    chimera_vfs_realtime(&now);

    hash = request->symlink_at.name_hash;

    /* Optimistically allocate an inode */
    inode = memfs_inode_alloc_thread(thread);

    inode->size       = request->symlink_at.targetlen;
    inode->space_used = request->symlink_at.targetlen;
    inode->uid        = request->cred->uid;
    inode->gid        = request->cred->gid;
    inode->nlink      = 1;
    inode->mode       = S_IFLNK | 0755;
    inode->atime      = now;
    inode->mtime      = now;
    inode->ctime      = now;
    inode->change++;
    inode->btime = now;

    inode->symlink.target = memfs_symlink_target_alloc(thread);

    inode->symlink.target->length = request->symlink_at.targetlen;
    memcpy(inode->symlink.target->data,
           request->symlink_at.target,
           request->symlink_at.targetlen);

    memfs_map_attrs(shared, &request->symlink_at.r_attr, inode, request->fh);

    /* Optimistically allocate a dirent */
    dirent = memfs_dirent_alloc(thread,
                                inode->inum,
                                inode->gen,
                                hash,
                                request->symlink_at.name,
                                request->symlink_at.namelen);

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

    memfs_map_attrs(shared, &request->symlink_at.r_dir_pre_attr, parent_inode, request->fh);

    rb_tree_insert(&parent_inode->dir.dirents, hash, dirent);

    parent_inode->mtime = now;
    parent_inode->ctime = now;
    parent_inode->change++;

    memfs_map_attrs(shared, &request->symlink_at.r_dir_post_attr, parent_inode, request->fh);

    pthread_mutex_unlock(&parent_inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_symlink_at */

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

    if (!S_ISLNK(inode->mode)) {
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    request->readlink.r_target_length = inode->symlink.target->length;

    memcpy(request->readlink.r_target,
           inode->symlink.target->data,
           inode->symlink.target->length);

    memfs_map_attrs(shared, &request->readlink.r_attr, inode, request->fh);

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
memfs_rename_at(
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

    chimera_vfs_realtime(&now);

    hash     = request->rename_at.name_hash;
    new_hash = request->rename_at.new_name_hash;

    cmp = memfs_fh_compare(request->fh,
                           request->fh_len,
                           request->rename_at.new_fh,
                           request->rename_at.new_fhlen);

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
                                                  request->rename_at.new_fh,
                                                  request->rename_at.new_fhlen);
        } else {
            new_parent_inode = memfs_inode_get_fh(shared,
                                                  request->rename_at.new_fh,
                                                  request->rename_at.new_fhlen);
            old_parent_inode = memfs_inode_get_fh(shared,
                                                  request->fh,
                                                  request->fh_len);
        }

        /* Cross-directory rename: both parent inodes are locked at this
         * point (or NULL on lookup miss). Every early-return below must
         * release whichever locks are still held, or the next request
         * touching the unreleased inode will deadlock. */
        if (!old_parent_inode) {
            if (new_parent_inode) {
                pthread_mutex_unlock(&new_parent_inode->lock);
            }
            request->status = CHIMERA_VFS_ENOENT;
            request->complete(request);
            return;
        }

        if (!S_ISDIR(old_parent_inode->mode)) {
            pthread_mutex_unlock(&old_parent_inode->lock);
            if (new_parent_inode) {
                pthread_mutex_unlock(&new_parent_inode->lock);
            }
            request->status = CHIMERA_VFS_ENOTDIR;
            request->complete(request);
            return;
        }

        if (!new_parent_inode) {
            pthread_mutex_unlock(&old_parent_inode->lock);
            request->status = CHIMERA_VFS_ENOENT;
            request->complete(request);
            return;
        }

        if (!S_ISDIR(new_parent_inode->mode)) {
            pthread_mutex_unlock(&new_parent_inode->lock);
            pthread_mutex_unlock(&old_parent_inode->lock);
            request->status = CHIMERA_VFS_ENOTDIR;
            request->complete(request);
            return;
        }
    }

    memfs_map_attrs(shared, &request->rename_at.r_fromdir_pre_attr, old_parent_inode, request->fh);
    memfs_map_attrs(shared, &request->rename_at.r_todir_pre_attr, new_parent_inode, request->rename_at.new_fh);

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

    /* POSIX: a directory may not be renamed into itself or one of its own
     * descendants (EINVAL).  Detect this before locking the child -- otherwise,
     * when the source is the destination parent (or an ancestor of it), locking
     * the child would re-lock an already-held inode and self-deadlock.  Walk the
     * destination parent's ancestry; the two already-held parent inodes are read
     * directly, any others are briefly locked. */
    {
        uint64_t cur_inum = new_parent_inode->inum;
        uint64_t par_inum = new_parent_inode->dir.parent_inum;
        uint32_t par_gen  = new_parent_inode->dir.parent_gen;
        int      bad      = 0;

        for (int depth = 0; depth < CHIMERA_VFS_PATH_MAX; depth++) {
            if (cur_inum == old_dirent->inum) {
                bad = 1;
                break;
            }
            if (par_inum == cur_inum) {
                break;  /* reached the root (parent of root is itself) */
            }
            cur_inum = par_inum;
            if (par_inum == new_parent_inode->inum) {
                par_inum = new_parent_inode->dir.parent_inum;
                par_gen  = new_parent_inode->dir.parent_gen;
            } else if (par_inum == old_parent_inode->inum) {
                par_inum = old_parent_inode->dir.parent_inum;
                par_gen  = old_parent_inode->dir.parent_gen;
            } else {
                struct memfs_inode *anc = memfs_inode_get_inum(shared, par_inum, par_gen);
                if (!anc) {
                    break;
                }
                par_inum = anc->dir.parent_inum;
                par_gen  = anc->dir.parent_gen;
                pthread_mutex_unlock(&anc->lock);
            }
        }

        if (bad) {
            pthread_mutex_unlock(&old_parent_inode->lock);
            if (cmp != 0) {
                pthread_mutex_unlock(&new_parent_inode->lock);
            }
            request->status = CHIMERA_VFS_EINVAL;
            request->complete(request);
            return;
        }
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
            memfs_map_attrs(shared, &request->rename_at.r_fromdir_post_attr, old_parent_inode, request->fh);
            memfs_map_attrs(shared, &request->rename_at.r_todir_post_attr, new_parent_inode, request->rename_at.new_fh);
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
            if (S_ISDIR(existing_inode->mode) &&
                !rb_tree_empty(&existing_inode->dir.dirents)) {
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
                                    request->rename_at.new_name,
                                    request->rename_at.new_namelen);

    rb_tree_insert(&new_parent_inode->dir.dirents, hash, new_dirent);

    rb_tree_remove(&old_parent_inode->dir.dirents, &old_dirent->node);

    if (S_ISDIR(child_inode->mode) && cmp != 0) {
        /* Cross-directory move of a directory: the source parent loses its
         * subdirectory backlink and the destination parent gains one.  Also
         * re-home the moved directory's ".." so it resolves to its new parent
         * (memfs derives ".." from these stored fields, not a real dirent). */
        old_parent_inode->nlink--;
        new_parent_inode->nlink++;
        child_inode->dir.parent_inum = new_parent_inode->inum;
        child_inode->dir.parent_gen  = new_parent_inode->gen;
    }

    old_parent_inode->mtime = now;
    old_parent_inode->ctime = now;
    old_parent_inode->change++;
    new_parent_inode->mtime = now;
    new_parent_inode->ctime = now;
    new_parent_inode->change++;

    /* POSIX: a successful rename marks the renamed file's status-change time. */
    child_inode->ctime = now;
    child_inode->change++;

    memfs_map_attrs(shared, &request->rename_at.r_fromdir_post_attr, old_parent_inode, request->fh);
    memfs_map_attrs(shared, &request->rename_at.r_todir_post_attr, new_parent_inode, request->rename_at.new_fh);

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

} /* memfs_rename_at */

static void
memfs_link_at(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode  *parent_inode, *inode, *existing_inode;
    struct memfs_dirent *dirent, *existing_dirent;
    struct timespec      now;
    uint64_t             hash;

    chimera_vfs_realtime(&now);

    hash = request->link_at.name_hash;

    parent_inode = memfs_inode_get_fh(shared,
                                      request->link_at.dir_fh,
                                      request->link_at.dir_fhlen);

    if (!parent_inode) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    memfs_map_attrs(shared, &request->link_at.r_dir_pre_attr, parent_inode, request->link_at.dir_fh);

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
        /* Hard-linking a directory is not permitted.  The VFS reports the
         * physical condition as EISDIR (NFS4 -> NFS4ERR_ISDIR, SMB ->
         * STATUS_FILE_IS_A_DIRECTORY); the POSIX link() wrapper maps EISDIR to
         * EPERM per link(2). */
        pthread_mutex_unlock(&parent_inode->lock);
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_EISDIR;
        request->complete(request);
        return;
    }

    rb_tree_query_exact(&parent_inode->dir.dirents, hash, hash, existing_dirent);

    if (existing_dirent) {
        /* The name is taken. Without an explicit replace request this is an
         * error; with one we clobber the existing entry (CIFS rename with
         * replace-if-exists, S3 PutObject/CopyObject overwrite). */
        if (!request->link_at.replace) {
            pthread_mutex_unlock(&parent_inode->lock);
            pthread_mutex_unlock(&inode->lock);
            request->status = CHIMERA_VFS_EEXIST;
            request->complete(request);
            return;
        }

        /* If the name already points at the link target itself, the link is
         * already in place — succeed without disturbing it. Guard this before
         * locking the existing inode, which would otherwise self-deadlock on
         * the target's mutex. */
        if (existing_dirent->inum == inode->inum &&
            existing_dirent->gen == inode->gen) {
            inode->ctime = now;
            inode->change++;
            parent_inode->mtime = now;
            parent_inode->ctime = now;
            parent_inode->change++;
            memfs_map_attrs(shared, &request->link_at.r_dir_post_attr,
                            parent_inode, request->link_at.dir_fh);
            memfs_map_attrs(shared, &request->link_at.r_attr, inode,
                            request->fh);
            pthread_mutex_unlock(&parent_inode->lock);
            pthread_mutex_unlock(&inode->lock);
            request->status = CHIMERA_VFS_OK;
            request->complete(request);
            return;
        }

        existing_inode = memfs_inode_get_inum(shared, existing_dirent->inum,
                                              existing_dirent->gen);

        /* Refuse to clobber a directory with a file link. */
        if (existing_inode && S_ISDIR(existing_inode->mode)) {
            pthread_mutex_unlock(&parent_inode->lock);
            pthread_mutex_unlock(&inode->lock);
            pthread_mutex_unlock(&existing_inode->lock);
            request->status = CHIMERA_VFS_EISDIR;
            request->complete(request);
            return;
        }

        /* Detach the existing entry and release its inode link, freeing the
         * inode if it now has neither links nor open handles (mirrors
         * memfs_remove_at). */
        rb_tree_remove(&parent_inode->dir.dirents, &existing_dirent->node);

        if (existing_inode) {
            existing_inode->nlink--;
            if (existing_inode->nlink == 0 && --existing_inode->refcnt == 0) {
                memfs_inode_free(thread, existing_inode);
            }
            pthread_mutex_unlock(&existing_inode->lock);
        }

        memfs_dirent_free(thread, existing_dirent);
    }

    dirent = memfs_dirent_alloc(thread,
                                inode->inum,
                                inode->gen,
                                hash,
                                request->link_at.name,
                                request->link_at.namelen);

    rb_tree_insert(&parent_inode->dir.dirents, hash, dirent);

    inode->nlink++;

    inode->ctime = now;
    inode->change++;
    parent_inode->mtime = now;
    parent_inode->ctime = now;
    parent_inode->change++;

    memfs_map_attrs(shared, &request->link_at.r_dir_post_attr, parent_inode, request->link_at.dir_fh);
    memfs_map_attrs(shared, &request->link_at.r_attr, inode, request->fh);

    pthread_mutex_unlock(&parent_inode->lock);
    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);

} /* memfs_link_at */


static inline struct memfs_xattr *
memfs_xattr_find(
    struct memfs_inode *inode,
    const char         *name,
    uint32_t            name_len)
{
    struct memfs_xattr *xattr;

    for (xattr = inode->xattrs; xattr; xattr = xattr->next) {
        if (xattr->name_len == name_len &&
            memcmp(xattr->name, name, name_len) == 0) {
            return xattr;
        }
    }

    return NULL;
} /* memfs_xattr_find */

static void
memfs_get_xattr(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode *inode;
    struct memfs_xattr *xattr;

    inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    xattr = memfs_xattr_find(inode, request->get_xattr.name,
                             request->get_xattr.namelen);

    if (!xattr) {
        request->status = CHIMERA_VFS_ENODATA;
    } else if (xattr->value_len > request->get_xattr.value_maxlen) {
        request->status = CHIMERA_VFS_ERANGE;
    } else {
        memcpy(request->get_xattr.value, xattr->value, xattr->value_len);
        request->get_xattr.r_value_len = xattr->value_len;
        request->status                = CHIMERA_VFS_OK;
    }

    pthread_mutex_unlock(&inode->lock);

    request->complete(request);
} /* memfs_get_xattr */

static void
memfs_set_xattr(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode *inode;
    struct memfs_xattr *xattr;
    struct timespec     now;
    void               *value;

    inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    memfs_map_attrs(shared, &request->set_xattr.r_pre_attr, inode, request->fh);

    xattr = memfs_xattr_find(inode, request->set_xattr.name,
                             request->set_xattr.namelen);

    if (xattr) {
        if (request->set_xattr.option == CHIMERA_VFS_XATTR_CREATE) {
            pthread_mutex_unlock(&inode->lock);
            request->status = CHIMERA_VFS_EEXIST;
            request->complete(request);
            return;
        }

        value = malloc(request->set_xattr.value_len);
        memcpy(value, request->set_xattr.value, request->set_xattr.value_len);
        free(xattr->value);
        xattr->value     = value;
        xattr->value_len = request->set_xattr.value_len;
    } else {
        if (request->set_xattr.option == CHIMERA_VFS_XATTR_REPLACE) {
            pthread_mutex_unlock(&inode->lock);
            request->status = CHIMERA_VFS_ENODATA;
            request->complete(request);
            return;
        }

        xattr           = malloc(sizeof(*xattr));
        xattr->name_len = request->set_xattr.namelen;
        xattr->name     = malloc(request->set_xattr.namelen);
        memcpy(xattr->name, request->set_xattr.name, request->set_xattr.namelen);
        xattr->value_len = request->set_xattr.value_len;
        xattr->value     = malloc(request->set_xattr.value_len);
        memcpy(xattr->value, request->set_xattr.value, request->set_xattr.value_len);
        xattr->next   = inode->xattrs;
        inode->xattrs = xattr;
    }

    chimera_vfs_realtime(&now);
    inode->ctime = now;
    inode->change++;

    memfs_map_attrs(shared, &request->set_xattr.r_post_attr, inode, request->fh);

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_set_xattr */

static void
memfs_list_xattrs(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode *inode;
    struct memfs_xattr *xattr;
    uint8_t            *buf    = request->list_xattrs.buffer;
    uint32_t            offset = 0;
    uint32_t            count  = 0;

    inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    /* memfs returns the entire list in a single, non-paginated reply. */
    for (xattr = inode->xattrs; xattr; xattr = xattr->next) {
        if (offset + xattr->name_len + 1 > request->list_xattrs.max_bytes) {
            pthread_mutex_unlock(&inode->lock);
            request->status = CHIMERA_VFS_ERANGE;
            request->complete(request);
            return;
        }
        memcpy(buf + offset, xattr->name, xattr->name_len);
        offset       += xattr->name_len;
        buf[offset++] = '\0';
        count++;
    }

    pthread_mutex_unlock(&inode->lock);

    request->list_xattrs.r_len    = offset;
    request->list_xattrs.r_count  = count;
    request->list_xattrs.r_eof    = 1;
    request->list_xattrs.r_cookie = 0;
    request->status               = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_list_xattrs */

static void
memfs_remove_xattr(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode *inode;
    struct memfs_xattr *xattr, **pprev;
    struct timespec     now;

    inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    memfs_map_attrs(shared, &request->remove_xattr.r_pre_attr, inode, request->fh);

    pprev = &inode->xattrs;
    for (xattr = inode->xattrs; xattr; xattr = xattr->next) {
        if (xattr->name_len == request->remove_xattr.namelen &&
            memcmp(xattr->name, request->remove_xattr.name,
                   request->remove_xattr.namelen) == 0) {
            break;
        }
        pprev = &xattr->next;
    }

    if (!xattr) {
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_ENODATA;
        request->complete(request);
        return;
    }

    *pprev = xattr->next;
    free(xattr->name);
    free(xattr->value);
    free(xattr);

    chimera_vfs_realtime(&now);
    inode->ctime = now;
    inode->change++;

    memfs_map_attrs(shared, &request->remove_xattr.r_post_attr, inode, request->fh);

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_remove_xattr */

static void
memfs_open_stream(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode        *inode;
    struct memfs_named_stream *stream;
    struct memfs_stream_open  *so;
    unsigned int               flags = request->open_stream.flags;
    struct timespec            now;

    chimera_vfs_realtime(&now);

    inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    /* Named streams attach only to regular files. */
    if (!S_ISREG(inode->mode)) {
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_EINVAL;
        request->complete(request);
        return;
    }

    stream = memfs_stream_find_by_name(inode, request->open_stream.name,
                                       request->open_stream.namelen);

    if (!stream) {
        if (!(flags & CHIMERA_VFS_OPEN_CREATE)) {
            pthread_mutex_unlock(&inode->lock);
            request->status = CHIMERA_VFS_ENOENT;
            request->complete(request);
            return;
        }

        stream       = calloc(1, sizeof(*stream));
        stream->name = malloc(request->open_stream.namelen);
        memcpy(stream->name, request->open_stream.name,
               request->open_stream.namelen);
        stream->name_len = request->open_stream.namelen;
        stream->id       = ++inode->next_stream_id;
        stream->linked   = 1;
        stream->next     = inode->streams;
        inode->streams   = stream;
        inode->mtime     = now;
        inode->ctime     = now;
        inode->change++;
        request->open_stream.r_created = 1;
    } else if (flags & CHIMERA_VFS_OPEN_EXCLUSIVE) {
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_EEXIST;
        request->complete(request);
        return;
    } else if (flags & CHIMERA_VFS_OPEN_TRUNCATE) {
        memfs_fork_free_blocks(thread, &stream->fork);
        stream->size       = 0;
        stream->space_used = 0;
        inode->mtime       = now;
        inode->ctime       = now;
        inode->change++;
    }

    /* Named streams share the base file's metadata (mode/owner/timestamps and
     * DOS attributes).  When a stream open creates or overwrites the stream,
     * apply the caller's requested attributes (e.g. the create's
     * FileAttributes -> ARCHIVE/HIDDEN) to the base inode, mirroring how a
     * regular create stamps them (smb2.streams.attributes2). */
    if (request->open_stream.set_attr &&
        (request->open_stream.r_created || (flags & CHIMERA_VFS_OPEN_TRUNCATE))) {
        memfs_apply_attrs(inode, request->open_stream.set_attr);
    }

    /* A stream open pins both the stream node and the base inode. */
    stream->refcnt++;
    inode->refcnt++;

    so                  = malloc(sizeof(*so));
    so->inode           = inode;
    so->stream          = stream;
    so->open_next       = inode->stream_opens;
    inode->stream_opens = so;

    request->open_stream.r_vfs_private = (uint64_t) (uintptr_t) so | 1ULL;

    memfs_map_attrs_fork(shared, &request->open_stream.r_attr, inode, stream,
                         request->fh);

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_open_stream */

static void
memfs_list_streams(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode             *inode;
    struct memfs_named_stream      *stream;
    uint8_t                        *buf    = request->list_streams.buffer;
    uint32_t                        max    = request->list_streams.max_bytes;
    uint32_t                        offset = 0;
    uint32_t                        count  = 0;
    struct chimera_vfs_stream_entry entry;
    uint32_t                        rec;

    inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    /* Default unnamed data fork ("::$DATA"), reported first with an empty name
     * so the SMB layer can format it as the default stream.  Only regular files
     * have a data fork -- a directory reports no streams (smb2.streams.dir
     * expects an empty stream list on a directory). */
    if (S_ISREG(inode->mode)) {
        rec = sizeof(entry);
        if (offset + rec > max) {
            pthread_mutex_unlock(&inode->lock);
            request->status = CHIMERA_VFS_ERANGE;
            request->complete(request);
            return;
        }
        entry.size     = inode->size;
        entry.alloc    = inode->space_used;
        entry.name_len = 0;
        memcpy(buf + offset, &entry, sizeof(entry));
        offset += rec;
        offset  = (offset + 7) & ~7u;
        count++;
    }

    for (stream = inode->streams; stream; stream = stream->next) {
        rec = sizeof(entry) + stream->name_len;
        if (offset + rec > max) {
            pthread_mutex_unlock(&inode->lock);
            request->status = CHIMERA_VFS_ERANGE;
            request->complete(request);
            return;
        }
        entry.size     = stream->size;
        entry.alloc    = stream->space_used;
        entry.name_len = stream->name_len;
        memcpy(buf + offset, &entry, sizeof(entry));
        memcpy(buf + offset + sizeof(entry), stream->name, stream->name_len);
        offset += rec;
        offset  = (offset + 7) & ~7u;
        count++;
    }

    pthread_mutex_unlock(&inode->lock);

    request->list_streams.r_len    = offset;
    request->list_streams.r_count  = count;
    request->list_streams.r_eof    = 1;
    request->list_streams.r_cookie = 0;
    request->status                = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_list_streams */

static void
memfs_remove_stream(
    struct memfs_thread        *thread,
    struct memfs_shared        *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct memfs_inode        *inode;
    struct memfs_named_stream *stream, **pprev;
    struct timespec            now;

    chimera_vfs_realtime(&now);

    inode = memfs_inode_get_fh(shared, request->fh, request->fh_len);

    if (unlikely(!inode)) {
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    memfs_map_attrs(shared, &request->remove_stream.r_pre_attr, inode, request->fh);

    pprev = &inode->streams;
    for (stream = inode->streams; stream; stream = stream->next) {
        if (stream->name_len == request->remove_stream.namelen &&
            memcmp(stream->name, request->remove_stream.name,
                   request->remove_stream.namelen) == 0) {
            break;
        }
        pprev = &stream->next;
    }

    if (!stream) {
        pthread_mutex_unlock(&inode->lock);
        request->status = CHIMERA_VFS_ENOENT;
        request->complete(request);
        return;
    }

    /* Unlink from the inode's stream list.  If no handle holds it open, free it
     * now; otherwise it survives (unlinked) until its last close -- park it on
     * dead_streams so it is still reclaimed if the inode is torn down before
     * that close arrives. */
    *pprev         = stream->next;
    stream->linked = 0;

    if (stream->refcnt == 0) {
        memfs_stream_node_free(thread, stream);
    } else {
        stream->next        = inode->dead_streams;
        inode->dead_streams = stream;
    }

    inode->ctime = now;
    inode->change++;

    memfs_map_attrs(shared, &request->remove_stream.r_post_attr, inode, request->fh);

    pthread_mutex_unlock(&inode->lock);

    request->status = CHIMERA_VFS_OK;
    request->complete(request);
} /* memfs_remove_stream */

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
        case CHIMERA_VFS_OP_LOOKUP_AT:
            memfs_lookup_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_GETPARENT:
            memfs_getparent(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_GETATTR:
            memfs_getattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SETATTR:
            memfs_setattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKDIR_AT:
            memfs_mkdir_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MKNOD_AT:
            memfs_mknod_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE_AT:
            memfs_remove_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READDIR:
            memfs_readdir(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            memfs_open_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_FH:
            memfs_open_fh(thread, shared, request, private_data);
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
            memfs_commit(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_ALLOCATE:
            memfs_allocate(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_COPY_RANGE:
            memfs_copy_range(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_CLONE_RANGE:
            memfs_clone_range(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_MOVE_RANGE:
            memfs_move_range(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SEEK:
            memfs_seek(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SYMLINK_AT:
            memfs_symlink_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_READLINK:
            memfs_readlink(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_RENAME_AT:
            memfs_rename_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LINK_AT:
            memfs_link_at(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_GET_XATTR:
            memfs_get_xattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_SET_XATTR:
            memfs_set_xattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LIST_XATTRS:
            memfs_list_xattrs(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE_XATTR:
            memfs_remove_xattr(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_OPEN_STREAM:
            memfs_open_stream(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_LIST_STREAMS:
            memfs_list_streams(thread, shared, request, private_data);
            break;
        case CHIMERA_VFS_OP_REMOVE_STREAM:
            memfs_remove_stream(thread, shared, request, private_data);
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
    .name         = "memfs",
    .fh_magic     = CHIMERA_VFS_FH_MAGIC_MEMFS,
    .capabilities = CHIMERA_VFS_CAP_CREATE_UNLINKED | CHIMERA_VFS_CAP_FS |
        CHIMERA_VFS_CAP_FS_RELATIVE_OP |
        CHIMERA_VFS_CAP_COPY_RANGE | CHIMERA_VFS_CAP_CLONE_RANGE | CHIMERA_VFS_CAP_MOVE_RANGE |
        CHIMERA_VFS_CAP_ACL_NATIVE | CHIMERA_VFS_CAP_XATTR | CHIMERA_VFS_CAP_LAYOUT |
        CHIMERA_VFS_CAP_READ_PROVIDES_BUFFERS |
        CHIMERA_VFS_CAP_NAMED_STREAMS | CHIMERA_VFS_CAP_RPL | CHIMERA_VFS_CAP_FS_LOCK |
        CHIMERA_VFS_CAP_CHANGE,
    .init           = memfs_init,
    .destroy        = memfs_destroy,
    .thread_init    = memfs_thread_init,
    .thread_destroy = memfs_thread_destroy,
    .dispatch       = memfs_dispatch,
};
