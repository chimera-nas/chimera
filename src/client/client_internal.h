// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <pthread.h>
#include <sys/uio.h>
#include <utlist.h>

#include "client.h"
#include "common/macros.h"
#include "vfs/vfs.h"
#include "vfs/vfs_cred.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "common/logging.h"

#define chimera_client_debug(...) chimera_debug("client", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_client_info(...)  chimera_info("client", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_client_error(...) chimera_error("client", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_client_fatal(...) chimera_fatal("client", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_client_abort(...) chimera_abort("client", __FILE__, __LINE__, __VA_ARGS__)

#define chimera_client_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "client", __FILE__, __LINE__, __VA_ARGS__)

#define CHIMERA_CLIENT_MAX_MODULES 64

struct chimera_client_fh {
    struct chimera_vfs_open_handle *handle;
};

enum chimera_client_request_opcode {
    CHIMERA_CLIENT_OP_MOUNT,
    CHIMERA_CLIENT_OP_UMOUNT,
    CHIMERA_CLIENT_OP_OPEN,
    CHIMERA_CLIENT_OP_MKDIR,
    CHIMERA_CLIENT_OP_READ,
    CHIMERA_CLIENT_OP_WRITE,
    CHIMERA_CLIENT_OP_SYMLINK,
    CHIMERA_CLIENT_OP_LINK,
    CHIMERA_CLIENT_OP_REMOVE,
    CHIMERA_CLIENT_OP_RENAME,
    CHIMERA_CLIENT_OP_READLINK,
    CHIMERA_CLIENT_OP_STAT,
    CHIMERA_CLIENT_OP_FSTAT,
    CHIMERA_CLIENT_OP_READDIR,
    CHIMERA_CLIENT_OP_SETATTR,
    CHIMERA_CLIENT_OP_FSETATTR,
    CHIMERA_CLIENT_OP_COMMIT,
    CHIMERA_CLIENT_OP_STATFS,
    CHIMERA_CLIENT_OP_FSTATFS,
    CHIMERA_CLIENT_OP_MKNOD,
    CHIMERA_CLIENT_OP_LOCK,
    CHIMERA_CLIENT_OP_COPY_RANGE,
    CHIMERA_CLIENT_OP_CLONE_RANGE,
    CHIMERA_CLIENT_OP_ALLOCATE,
    CHIMERA_CLIENT_OP_SEEK,
};

struct chimera_client_request;

typedef void (*chimera_client_request_callback)(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request);

struct chimera_client_request {
    enum chimera_client_request_opcode opcode;
    struct chimera_client_thread      *thread;
    struct chimera_client_request     *prev;
    struct chimera_client_request     *next;

    int                                heap_allocated;

    /* Per-request credential override.  When has_cred is set, req_cred is used
     * for VFS authorization instead of the client-global credential.  This lets
     * a single client issue operations on behalf of different users (e.g. the
     * POSIX layer switching uid/gid per call).  Default (has_cred == 0) falls
     * back to thread->client->cred, so existing callers are unaffected. */
    int                                has_cred;
    struct chimera_vfs_cred            req_cred;

    ssize_t                            sync_result;
    struct chimera_vfs_open_handle    *sync_open_handle;
    struct chimera_stat                sync_stat;
    struct chimera_statvfs             sync_statvfs;
    int                                sync_target_len;

    chimera_client_request_callback    sync_callback;

    uint32_t                           fh_len;

    uint8_t                            fh[CHIMERA_VFS_FH_SIZE];

    /* Explicit-transaction bookkeeping (CHIMERA_VFS_CAP_TRANSACTIONAL).  Every
     * client operation runs as one VFS transaction: begin (READ/WRITE) -> the
     * op's VFS calls (enlisted via request->txn) -> commit before the user
     * callback fires.  txn is the backend handle (NULL for a non-transactional
     * backend -> autocommit, unchanged).  txn_ts is the wait-die priority,
     * assigned once and reused across retries so a conflicting op cannot starve;
     * txn_attempt bounds the retries.  txn_op_status carries the op result
     * across an async EndTransaction.  txn_fh/txn_fhlen is the routing hint the
     * transaction begins on (saved for replay), txn_mode the begin mode, and
     * txn_start/txn_reply the op's two driver callbacks (see client_txn.h):
     * txn_start runs the op's VFS chain with request->txn set; txn_reply
     * releases resources, invokes the user callback and frees the request. */
    struct chimera_vfs_transaction    *txn;
    uint64_t                           txn_ts;
    int                                txn_attempt;
    enum chimera_vfs_error             txn_op_status;
    int                                txn_fhlen;
    uint8_t                            txn_mode;
    uint8_t                            txn_fh[CHIMERA_VFS_FH_SIZE];
    chimera_client_request_callback    txn_start;
    chimera_client_request_callback    txn_reply;

    union {
        struct {
            chimera_mount_callback_t callback;
            void                    *private_data;
            char                     mount_path[CHIMERA_VFS_PATH_MAX];
            char                     module_path[CHIMERA_VFS_PATH_MAX];
            char                     module_name[64];
            char                     options[CHIMERA_VFS_PATH_MAX];
        } mount;

        struct {
            chimera_umount_callback_t callback;
            void                     *private_data;
            char                      mount_path[CHIMERA_VFS_PATH_MAX];
        } umount;

        struct {
            struct chimera_vfs_open_handle *parent_handle;
            chimera_open_callback_t         callback;
            void                           *private_data;
            unsigned int                    flags;
            int                             path_len;
            int                             parent_len;
            int                             name_offset;
            struct chimera_vfs_attrs        set_attr;
            char                            path[CHIMERA_VFS_PATH_MAX];
        } open;

        struct {
            struct chimera_vfs_open_handle *parent_handle;
            chimera_mkdir_callback_t        callback;
            void                           *private_data;
            int                             path_len;
            int                             parent_len;
            int                             name_offset;
            struct chimera_vfs_attrs        set_attr;
            char                            path[CHIMERA_VFS_PATH_MAX];
        } mkdir;

        struct {
            struct chimera_vfs_open_handle *parent_handle;
            chimera_mknod_callback_t        callback;
            void                           *private_data;
            int                             path_len;
            int                             parent_len;
            int                             name_offset;
            struct chimera_vfs_attrs        set_attr;
            char                            path[CHIMERA_VFS_PATH_MAX];
        } mknod;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            uint32_t                        length;
            uint32_t                        result_count;
            uint32_t                        result_eof;
            int                             niov;
            chimera_read_callback_t         callback;
            void                           *private_data;
            void                           *buf;
            /* Result iov/niov stashed by the op completion for the txn reply to
             * hand back to the caller (the buffers survive the read commit). */
            struct evpl_iovec              *r_iov;
            int                             r_niov;
            struct evpl_iovec               iov[CHIMERA_CLIENT_IOV_MAX];
        } read;

        /* For chimera_read_into - caller provides destination evpl_iovec(s).
         * dest_iov is a borrowed shallow copy of the caller's buffers (the
         * caller keeps its own refs alive until the callback); iov is scratch
         * the VFS core/backend works through. */
        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            uint32_t                        length;
            uint32_t                        result_count;
            uint32_t                        result_eof;
            int                             dest_niov;
            chimera_read_into_callback_t    callback;
            void                           *private_data;
            struct evpl_iovec              *r_iov;
            int                             r_niov;
            struct evpl_iovec               dest_iov[CHIMERA_CLIENT_IOV_MAX];
            struct evpl_iovec               iov[CHIMERA_CLIENT_IOV_MAX];
        } read_into;

        /* For chimera_write - caller provides a simple buffer */
        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            uint32_t                        length;
            int                             niov;
            chimera_write_callback_t        callback;
            void                           *private_data;
            const void                     *buf;
            struct evpl_iovec               iov[CHIMERA_CLIENT_IOV_MAX];
        } write;

        /* For chimera_writev - caller provides struct iovec array */
        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            uint32_t                        length;
            int                             niov;
            chimera_write_callback_t        callback;
            void                           *private_data;
            const struct iovec             *src_iov;
            int                             src_iovcnt;
            struct evpl_iovec               iov[CHIMERA_CLIENT_IOV_MAX];
        } writev;

        /* For chimera_writerv - caller provides evpl_iovec */
        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            uint32_t                        length;
            int                             niov;
            chimera_write_callback_t        callback;
            void                           *private_data;
            struct evpl_iovec               iov[CHIMERA_CLIENT_IOV_MAX];
        } writerv;

        struct {
            struct chimera_vfs_open_handle *parent_handle;
            chimera_symlink_callback_t      callback;
            void                           *private_data;
            int                             path_len;
            int                             parent_len;
            int                             name_offset;
            int                             target_len;
            struct chimera_vfs_attrs        set_attr;
            char                            path[CHIMERA_VFS_PATH_MAX];
            char                            target[CHIMERA_VFS_PATH_MAX];
        } symlink;

        struct {
            struct chimera_vfs_open_handle *dest_parent_handle;
            chimera_link_callback_t         callback;
            void                           *private_data;
            int                             source_path_len;
            int                             source_parent_len;
            int                             source_name_offset;
            int                             dest_path_len;
            int                             dest_parent_len;
            int                             dest_name_offset;
            uint32_t                        source_fh_len;
            uint32_t                        dest_fh_len;
            char                            source_path[CHIMERA_VFS_PATH_MAX];
            char                            dest_path[CHIMERA_VFS_PATH_MAX];
            uint8_t                         source_fh[CHIMERA_VFS_FH_SIZE];
            uint8_t                         dest_fh[CHIMERA_VFS_FH_SIZE];
        } link;

        struct {
            struct chimera_vfs_open_handle *parent_handle;
            chimera_remove_callback_t       callback;
            void                           *private_data;
            int                             path_len;
            int                             parent_len;
            int                             name_offset;
            int                             child_fh_len;
            uint8_t                         child_fh[CHIMERA_VFS_FH_SIZE];
            char                            path[CHIMERA_VFS_PATH_MAX];
        } remove;

        struct {
            struct chimera_vfs_open_handle *source_parent_handle;
            struct chimera_vfs_open_handle *dest_parent_handle;
            chimera_rename_callback_t       callback;
            void                           *private_data;
            int                             source_path_len;
            int                             source_parent_len;
            int                             source_name_offset;
            int                             dest_path_len;
            int                             dest_parent_len;
            int                             dest_name_offset;
            uint32_t                        source_fh_len;
            uint32_t                        dest_fh_len;
            int                             target_fh_len;
            char                            source_path[CHIMERA_VFS_PATH_MAX];
            char                            dest_path[CHIMERA_VFS_PATH_MAX];
            uint8_t                         source_fh[CHIMERA_VFS_FH_SIZE];
            uint8_t                         dest_fh[CHIMERA_VFS_FH_SIZE];
            uint8_t                         target_fh[CHIMERA_VFS_FH_SIZE];
        } rename;

        struct {
            struct chimera_vfs_open_handle *handle;
            chimera_readlink_callback_t     callback;
            void                           *private_data;
            uint32_t                        target_maxlength;
            char                           *target;
            int                             path_len;
            char                            path[CHIMERA_VFS_PATH_MAX];
        } readlink;

        struct {
            struct chimera_vfs_open_handle *handle;
            chimera_stat_callback_t         callback;
            void                           *private_data;
            uint32_t                        flags;  /* CHIMERA_VFS_LOOKUP_FOLLOW for stat, 0 for lstat */
            int                             path_len;
            char                            path[CHIMERA_VFS_PATH_MAX];
        } stat;

        struct {
            struct chimera_vfs_open_handle *handle;
            chimera_fstat_callback_t        callback;
            void                           *private_data;
        } fstat;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        cookie;
            chimera_readdir_callback_t      callback;
            chimera_readdir_complete_t      complete;
            void                           *private_data;
            /* Result cookie/eof stashed by the op completion for the txn reply
             * (kept separate from the input cookie so a replay re-reads from the
             * same starting cookie). */
            uint64_t                        r_cookie;
            uint32_t                        r_eof;
        } readdir;

        struct {
            struct chimera_vfs_open_handle *parent_handle;
            chimera_setattr_callback_t      callback;
            void                           *private_data;
            int                             path_len;
            int                             parent_len;
            int                             name_offset;
            struct chimera_vfs_attrs        set_attr;
            char                            path[CHIMERA_VFS_PATH_MAX];
        } setattr;

        struct {
            struct chimera_vfs_open_handle *handle;
            chimera_fsetattr_callback_t     callback;
            void                           *private_data;
            struct chimera_vfs_attrs        set_attr;
        } fsetattr;

        struct {
            struct chimera_vfs_open_handle *handle;
            chimera_commit_callback_t       callback;
            void                           *private_data;
        } commit;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            uint64_t                        length;
            uint32_t                        flags;
            chimera_commit_callback_t       callback;
            void                           *private_data;
        } allocate;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            uint32_t                        what;   /* 0 = SEEK_DATA, 1 = SEEK_HOLE */
            chimera_seek_callback_t         callback;
            void                           *private_data;
            int                             r_eof;
            uint64_t                        r_offset;
        } seek;

        struct {
            struct chimera_vfs_open_handle *handle;
            chimera_statfs_callback_t       callback;
            void                           *private_data;
            int                             path_len;
            char                            path[CHIMERA_VFS_PATH_MAX];
        } statfs;

        struct {
            struct chimera_vfs_open_handle *handle;
            chimera_fstatfs_callback_t      callback;
            void                           *private_data;
        } fstatfs;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            uint64_t                        length;
            uint32_t                        lock_type;    /* CHIMERA_VFS_LOCK_{READ,WRITE,UNLOCK} */
            uint32_t                        flags;        /* CHIMERA_VFS_LOCK_{WAIT,TEST} */
            int32_t                         whence;       /* SEEK_SET or SEEK_END */
            chimera_lock_callback_t         callback;
            void                           *private_data;
            /* Result fields populated by VFS callback */
            uint32_t                        r_conflict_type;
            uint64_t                        r_conflict_offset;
            uint64_t                        r_conflict_length;
            pid_t                           r_conflict_pid;
        } lock;

        struct {
            struct chimera_vfs_open_handle *src_handle;
            struct chimera_vfs_open_handle *dst_handle;
            uint64_t                        src_offset;
            uint64_t                        dst_offset;
            uint64_t                        length;
            uint64_t                        r_length;
            chimera_copy_range_callback_t   callback;
            void                           *private_data;
        } copy_range;

        struct {
            struct chimera_vfs_open_handle *src_handle;
            struct chimera_vfs_open_handle *dst_handle;
            uint64_t                        src_offset;
            uint64_t                        dst_offset;
            uint64_t                        length;
            chimera_clone_range_callback_t  callback;
            void                           *private_data;
        } clone_range;

        /* GETACL: resolve `path`, open an O_PATH handle, getattr CHIMERA_VFS_
         * ATTR_ACL, and copy the returned ACL into the caller-owned `acl_buf`
         * (capacity `acl_bufsize` bytes).  r_acl_aces is set to the object's ACE
         * count even when the buffer is too small (so the caller can size a
         * retry); CHIMERA_VFS_ERANGE is returned in that case. */
        struct {
            struct chimera_vfs_open_handle *handle;
            chimera_setattr_callback_t      callback;
            void                           *private_data;
            int                             path_len;
            struct chimera_acl             *acl_buf;
            size_t                          acl_bufsize;
            uint16_t                        r_acl_aces;
            char                            path[CHIMERA_VFS_PATH_MAX];
        } getacl;
    };
} __attribute__((aligned(64)));

struct chimera_client_config {
    int                           core_threads;
    int                           sync_delegation;
    int                           sync_delegation_threads;
    int                           async_delegation;
    int                           async_delegation_threads;
    int                           cache_ttl;
    int                           attr_cache_enabled;
    int                           name_cache_enabled;
    int                           rcu_reclaim_threads;
    int                           max_fds;
    enum chimera_tcp_flavor       tcp_flavor;
    char                          kv_module[64];
    struct chimera_vfs_module_cfg modules[CHIMERA_CLIENT_MAX_MODULES];
    int                           num_modules;
};
/* No __attribute__((aligned(64))) here: this is a cold, singly-allocated config
 * blob (cache-line alignment buys nothing), and over-aligning it is unsafe given
 * it is calloc'd -- calloc only guarantees 16-byte alignment, but with alignof
 * 64 the optimizer is free to initialize the struct with 32-byte *aligned* AVX
 * stores (vmovdqa), which GP-fault when the allocation lands 16- but not
 * 32-byte aligned (Release-only; -O0 Debug uses scalar stores and survives). */

struct chimera_client {
    const struct chimera_client_config *config;
    struct chimera_vfs                 *vfs;
    struct chimera_vfs_cred             cred;
    uint32_t                            root_fh_len;
    uint8_t                             root_fh[CHIMERA_VFS_FH_SIZE];
} __attribute__((aligned(64)));

struct chimera_client_thread {
    struct chimera_client         *client;
    struct chimera_vfs_thread     *vfs_thread;
    struct chimera_client_request *free_requests;
} __attribute__((aligned(64)));

/*
 * Return the effective credential for a request: the per-request override when
 * present, otherwise the client-global credential.  All VFS dispatch paths use
 * this so that a per-call credential transparently overrides the default.
 */
static inline const struct chimera_vfs_cred *
chimera_client_req_cred(const struct chimera_client_request *request)
{
    return request->has_cred ? &request->req_cred : &request->thread->client->cred;
} /* chimera_client_req_cred */

static inline struct chimera_client_request *
chimera_client_request_alloc(struct chimera_client_thread *thread)
{
    struct chimera_client_request *request;

    request = thread->free_requests;

    if (request) {
        DL_DELETE(thread->free_requests, request);
    } else {
        request         = calloc(1, sizeof(struct chimera_client_request));
        request->thread = thread;
    }

    request->heap_allocated = 1;

    return request;
} /* chimera_client_request_alloc */

static inline void
chimera_client_request_free(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    if (!request->heap_allocated) {
        return;
    }

    DL_PREPEND(thread->free_requests, request);
} /* chimera_client_request_free */

void chimera_dispatch_mount(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request);

void chimera_dispatch_umount(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request);