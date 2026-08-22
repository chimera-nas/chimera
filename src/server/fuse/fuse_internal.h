// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <pthread.h>
#include <errno.h>
#include <utlist.h>
#include <linux/fuse.h>

#include "evpl/evpl.h"
#include "common/logging.h"
#include "vfs/vfs.h"
#include "vfs/vfs_error.h"
#include "vfs/vfs_cred.h"
#include "fuse_node_table.h"

#define chimera_fuse_debug(...) chimera_debug("fuse", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_fuse_info(...)  chimera_info("fuse", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_fuse_error(...) chimera_error("fuse", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_fuse_fatal(...) chimera_fatal("fuse", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_fuse_abort(...) chimera_abort("fuse", __FILE__, __LINE__, __VA_ARGS__)

#define chimera_fuse_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "fuse", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_fuse_abort_if(cond, ...) \
        chimera_abort_if(cond, "fuse", __FILE__, __LINE__, __VA_ARGS__)

#define CHIMERA_FUSE_MAX_MOUNTS      16
#define CHIMERA_FUSE_MAX_THREADS     64

/* What we ask the kernel to cap a single WRITE payload at.  The negotiated
 * value lands in mount->max_write and can only be smaller. */
#define CHIMERA_FUSE_MAX_WRITE       (1024 * 1024)

/* A /dev/fuse read must always offer room for max_write plus the request
 * headers, or the kernel fails the read with EINVAL. */
#define CHIMERA_FUSE_BUFSZ           (CHIMERA_FUSE_MAX_WRITE + 4096)

/* Scratch iovec array for VFS reads: enough for max_write in 4KB pieces. */
#define CHIMERA_FUSE_IOV_MAX         260

/* Oldest FUSE minor we speak; 7.23 dates to Linux 3.15, so the compat
 * variable-size INIT/SETATTR shapes older kernels need are out of scope. */
#define CHIMERA_FUSE_MIN_MINOR       23

/* Requests drained from a channel per read-callback invocation before
 * yielding the loop to other work; the event stays readable so the next pass
 * resumes immediately. */
#define CHIMERA_FUSE_READ_BATCH      16

/* Pooled requests kept per thread; each pins a CHIMERA_FUSE_BUFSZ buffer. */
#define CHIMERA_FUSE_MAX_POOLED_REQS 64

/* Clone one /dev/fuse channel per core thread (FUSE_DEV_IOC_CLONE), so each
 * thread reads and replies on its own kernel queue.  Set to 0 to fall back
 * to a single channel on thread slot 0 for debugging. */
#define CHIMERA_FUSE_MULTIQUEUE      1

struct chimera_fuse_thread;
struct chimera_fuse_shared;

struct chimera_fuse_mount {
    char                            mountpoint[256];
    char                            share_path[256];
    int                             allow_other;
    int                             default_permissions;
    uint32_t                        attr_timeout_ms;
    uint32_t                        entry_timeout_ms;
    uint32_t                        max_write;   /* negotiated */
    uint32_t                        proto_minor; /* negotiated */
    int                             mounted;
    int                             dead;
    int                             num_channels;
    int                             channel_fds[CHIMERA_FUSE_MAX_THREADS];
    struct chimera_fuse_node_table *node_table;
    /* Open files the kernel has not RELEASEd yet, so shutdown can release
    * their VFS handles.  Shared across threads (multi-queue delivery). */
    pthread_mutex_t                 open_lock;
    struct chimera_fuse_open_file  *open_files;
    uint8_t                         root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        root_fh_len;
};

struct chimera_fuse_shared {
    struct chimera_vfs         *vfs;
    struct prometheus_metrics  *metrics;
    pthread_mutex_t             lock;
    int                         num_mounts;
    struct chimera_fuse_mount   mounts[CHIMERA_FUSE_MAX_MOUNTS];
    int                         num_threads;
    int                         threads_alive;
    struct chimera_fuse_thread *threads[CHIMERA_FUSE_MAX_THREADS];
    int                         started;
};

struct chimera_fuse_channel {
    struct chimera_fuse_thread *thread;
    struct chimera_fuse_mount  *mount;
    int                         fd;
    int                         armed;
    int                         dead;
    struct evpl_fd_event        event;
};

/* One per kernel OPEN/OPENDIR/CREATE; fuse_open_out.fh carries its pointer.
* The VFS handle is captured into the request at dispatch time, so nothing
* but OPEN/RELEASE and the shutdown sweep touches this struct afterwards. */
struct chimera_fuse_open_file {
    struct chimera_vfs_open_handle *handle;
    struct chimera_fuse_mount      *mount;
    uint64_t                        readdir_verifier;
    struct chimera_fuse_open_file  *prev;
    struct chimera_fuse_open_file  *next;
};

struct chimera_fuse_request;

struct chimera_fuse_thread {
    struct evpl                 *evpl;
    struct chimera_vfs_thread   *vfs_thread;
    struct chimera_fuse_shared  *shared;
    int                          thread_slot;
    struct evpl_doorbell         attach_doorbell;
    struct chimera_fuse_channel  channels[CHIMERA_FUSE_MAX_MOUNTS];
    int                          num_channels;
    struct chimera_fuse_request *free_requests;
    int                          num_free_requests;
    int                          active_requests;
};

struct chimera_fuse_request {
    struct chimera_fuse_thread     *thread;
    struct chimera_fuse_channel    *channel;
    struct chimera_fuse_request    *next;
    struct chimera_vfs_cred         cred;
    uint64_t                        unique;
    uint32_t                        opcode;
    uint64_t                        nodeid;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;
    /* Second handle for the two-node ops (LINK's target, RENAME's newdir). */
    uint8_t                         fh2[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh2_len;
    /* Transient VFS handle for the op in flight; released by the terminal
     * completion before the reply. */
    struct chimera_vfs_open_handle *handle;
    /* OPEN/CREATE result carrier. */
    struct chimera_fuse_open_file  *file;
    /* Receive buffer; request field pointers (names, write payload) point
     * into it, so it is not reused until the request is freed. */
    struct evpl_iovec               buf;
    int                             buf_allocated;
    uint32_t                        buf_len;

    union {
        struct {
            uint32_t size;      /* kernel's reply size limit */
            uint32_t used;      /* bytes packed so far */
            int      plus;      /* READDIRPLUS */
        } readdir;
        struct {
            struct chimera_vfs_attrs set_attr;
        } setattr;
        struct {
            struct chimera_vfs_attrs set_attr;
        } create;               /* also mkdir/mknod/symlink */
        struct {
            struct evpl_iovec iov;
            uint32_t          size;
        } write;
        struct {
            struct evpl_iovec iov[CHIMERA_FUSE_IOV_MAX];
        } read;
        struct {
            uint32_t size;      /* getxattr/listxattr size probe or limit */
        } xattr;
    } u;
};

/* fuse_dispatch.c */
void
chimera_fuse_channel_readable(
    struct evpl          *evpl,
    struct evpl_fd_event *event);

void
chimera_fuse_channel_error(
    struct evpl          *evpl,
    struct evpl_fd_event *event);

void
chimera_fuse_channel_dead(
    struct chimera_fuse_channel *channel);

/* Reply helpers: deliver (or drop) the reply, release any transient handle,
 * and recycle the request.  The int-returning ones report whether the kernel
 * actually took the reply (0) or never will (-1), for callers whose reply
 * hands the kernel a reference they must otherwise undo. */
int
chimera_fuse_reply(
    struct chimera_fuse_request *req,
    int                          error,
    const void                  *payload,
    size_t                       payload_len);

/* READ replies: sends header + data iovecs, then releases the iovecs on the
 * request's thread before the request is recycled. */
void
chimera_fuse_reply_read(
    struct chimera_fuse_request *req,
    int                          error,
    struct evpl_iovec           *iov,
    int                          niov,
    size_t                       data_len);

int
chimera_fuse_reply_entry(
    struct chimera_fuse_request    *req,
    const struct chimera_vfs_attrs *attr,
    const void                     *extra,
    size_t                          extra_len);

/* Split primitives for replies that must inspect the delivery result while
 * the request (and its buffer) is still alive: send without recycling, then
 * finish (release any transient handle, recycle the request). */
int
chimera_fuse_send_only(
    struct chimera_fuse_request *req,
    int                          error,
    const void                  *payload,
    size_t                       payload_len);

void
chimera_fuse_request_finish(
    struct chimera_fuse_request *req);

void
chimera_fuse_request_free(
    struct chimera_fuse_thread  *thread,
    struct chimera_fuse_request *req);

/* fuse_mount.c */
int
chimera_fuse_mount_setup(
    struct chimera_fuse_shared *shared,
    struct chimera_fuse_mount  *mount);

void
chimera_fuse_mount_teardown(
    struct chimera_fuse_mount *mount);

/* Per-opcode handlers (fuse_proc_*.c); each owns the request until it calls
 * a reply helper (or frees it directly for the no-reply ops). */
typedef void (*chimera_fuse_handler_t)(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);

#define CHIMERA_FUSE_OPCODE_MAX 64

extern const chimera_fuse_handler_t chimera_fuse_handlers[CHIMERA_FUSE_OPCODE_MAX];

/* fuse_proc_lookup.c */
void chimera_fuse_op_lookup(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_forget(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_batch_forget(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);

/* fuse_proc_getattr.c */
void chimera_fuse_op_getattr(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_setattr(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_readlink(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_statfs(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_access(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);

/* fuse_proc_dir.c */
void chimera_fuse_op_opendir(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_readdir(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_releasedir(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_fsyncdir(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);

/* fuse_proc_io.c */
void chimera_fuse_op_open(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_create(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_read(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_write(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_flush(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_fsync(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_release(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_fallocate(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_lseek(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);

/* fuse_proc_namespace.c */
void chimera_fuse_op_mkdir(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_mknod(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_symlink(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_link(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_unlink(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_rmdir(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_rename(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);

/* fuse_proc_xattr.c */
void chimera_fuse_op_getxattr(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_setxattr(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_listxattr(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);
void chimera_fuse_op_removexattr(
    struct chimera_fuse_request *req,
    const struct fuse_in_header *hdr,
    const void                  *arg,
    uint32_t                     arglen);

/* Byte offset into the request buffer where reply payloads are staged.  The
 * incoming request occupies at most a few hundred bytes at the front (WRITE
 * payloads are consumed before any reply is built), so the same buffer
 * doubles as readdir/xattr/readlink reply space. */
#define CHIMERA_FUSE_REPLY_OFF 4096

static inline uint8_t *
chimera_fuse_reply_space(struct chimera_fuse_request *req)
{
    return (uint8_t *) evpl_iovec_data(&req->buf) + CHIMERA_FUSE_REPLY_OFF;
} /* chimera_fuse_reply_space */

static inline struct chimera_fuse_open_file *
chimera_fuse_file(uint64_t fh)
{
    return (struct chimera_fuse_open_file *) (uintptr_t) fh;
} /* chimera_fuse_file */

static inline void
chimera_fuse_file_link(
    struct chimera_fuse_mount     *mount,
    struct chimera_fuse_open_file *file)
{
    pthread_mutex_lock(&mount->open_lock);
    DL_APPEND(mount->open_files, file);
    pthread_mutex_unlock(&mount->open_lock);
} /* chimera_fuse_file_link */

static inline void
chimera_fuse_file_unlink(
    struct chimera_fuse_mount     *mount,
    struct chimera_fuse_open_file *file)
{
    pthread_mutex_lock(&mount->open_lock);
    DL_DELETE(mount->open_files, file);
    pthread_mutex_unlock(&mount->open_lock);
} /* chimera_fuse_file_unlink */

/* Resolve the request's nodeid to a file handle; -1 means the kernel named a
 * node we no longer know (reply ESTALE). */
static inline int
chimera_fuse_resolve_nodeid(struct chimera_fuse_request *req)
{
    struct chimera_fuse_mount *mount = req->channel->mount;

    if (req->nodeid == FUSE_ROOT_ID) {
        memcpy(req->fh, mount->root_fh, mount->root_fh_len);
        req->fh_len = mount->root_fh_len;
        return 0;
    }

    return chimera_fuse_node_get_fh(mount->node_table, req->nodeid,
                                    req->fh, &req->fh_len);
} /* chimera_fuse_resolve_nodeid */

/* Same for a second node named inside the request body (LINK, RENAME). */
static inline int
chimera_fuse_resolve_nodeid2(
    struct chimera_fuse_request *req,
    uint64_t                     nodeid)
{
    struct chimera_fuse_mount *mount = req->channel->mount;

    if (nodeid == FUSE_ROOT_ID) {
        memcpy(req->fh2, mount->root_fh, mount->root_fh_len);
        req->fh2_len = mount->root_fh_len;
        return 0;
    }

    return chimera_fuse_node_get_fh(mount->node_table, nodeid,
                                    req->fh2, &req->fh2_len);
} /* chimera_fuse_resolve_nodeid2 */

/*
 * chimera_vfs_error values are Linux errno numbers apart from two synthetic
 * sentinels, so the mapping to the (positive) errno a FUSE reply carries is
 * nearly the identity.
 */
static inline int
chimera_fuse_errno(enum chimera_vfs_error error_code)
{
    switch (error_code) {
        case CHIMERA_VFS_OK:
            return 0;
        case CHIMERA_VFS_EBADCOOKIE:
            return EINVAL;
        case CHIMERA_VFS_UNSET:
            return EIO;
        default:
            return (int) error_code;
    } /* switch */
} /* chimera_fuse_errno */

/* The FUSE header carries only uid/gid (no supplementary groups); mounting
 * with default_permissions makes the kernel do mode-bit checks with the
 * caller's full group list against the attrs we return. */
static inline void
chimera_fuse_map_cred(
    struct chimera_vfs_cred     *cred,
    const struct fuse_in_header *hdr)
{
    chimera_vfs_cred_init_unix(cred, hdr->uid, hdr->gid, 0, NULL);
} /* chimera_fuse_map_cred */
