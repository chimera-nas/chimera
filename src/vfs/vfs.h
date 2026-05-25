// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once
#include <stdint.h>
#include <sys/time.h>
#include <uthash.h>
#include "vfs_attrs.h"
#include "vfs_dump.h"
#include "vfs_error.h"
#include "vfs_cred.h"
#include "vfs_pnfs.h"
#include "vfs_lease_types.h"
#include "evpl/evpl.h"

#define CHIMERA_VFS_PATH_MAX 4096
#define CHIMERA_VFS_NAME_MAX 256
struct evpl;

struct chimera_vfs;
struct chimera_vfs_user;
struct chimera_vfs_user_cache;
struct prometheus_metrics;

/* FSSTAT values used with builtin backends until statvfs tracking is implemented */
#define CHIMERA_VFS_SYNTHETIC_FS_BYTES   ((uint64_t) 100 * 1024 * 1024 * 1024)
#define CHIMERA_VFS_SYNTHETIC_FS_INODES  (1024 * 1024)

/* Flags for chimera_vfs_lookup */
#define CHIMERA_VFS_LOOKUP_FOLLOW        (1U << 0) /* Follow symlinks in final component */

/* Maximum number of symlinks to follow before returning ELOOP */
#define CHIMERA_VFS_SYMLOOP_MAX          40

#define CHIMERA_VFS_MOUNT_OPT_MAX        16
#define CHIMERA_VFS_MOUNT_OPT_BUFFER_MAX 1024

struct chimera_vfs_mount_option {
    const char *key;
    const char *value;   /* NULL if no value */
};

struct chimera_vfs_mount_options {
    int                             num_options;
    struct chimera_vfs_mount_option options[CHIMERA_VFS_MOUNT_OPT_MAX];
};

#define CHIMERA_VFS_OP_MOUNT            1
#define CHIMERA_VFS_OP_UMOUNT           2
#define CHIMERA_VFS_OP_LOOKUP_AT        3
#define CHIMERA_VFS_OP_GETATTR          4
#define CHIMERA_VFS_OP_READDIR          5
#define CHIMERA_VFS_OP_READLINK         6
#define CHIMERA_VFS_OP_OPEN_FH          7
#define CHIMERA_VFS_OP_OPEN_AT          8
#define CHIMERA_VFS_OP_CLOSE            9
#define CHIMERA_VFS_OP_READ             10
#define CHIMERA_VFS_OP_WRITE            11
#define CHIMERA_VFS_OP_REMOVE_AT        12
#define CHIMERA_VFS_OP_MKDIR_AT         13
#define CHIMERA_VFS_OP_COMMIT           14
#define CHIMERA_VFS_OP_SYMLINK_AT       15
#define CHIMERA_VFS_OP_RENAME_AT        16
#define CHIMERA_VFS_OP_SETATTR          17
#define CHIMERA_VFS_OP_LINK_AT          18
#define CHIMERA_VFS_OP_CREATE_UNLINKED  19
#define CHIMERA_VFS_OP_MKNOD_AT         20
#define CHIMERA_VFS_OP_PUT_KEY          21
#define CHIMERA_VFS_OP_GET_KEY          22
#define CHIMERA_VFS_OP_DELETE_KEY       23
#define CHIMERA_VFS_OP_SEARCH_KEYS      24
#define CHIMERA_VFS_OP_ALLOCATE         25
#define CHIMERA_VFS_OP_SEEK             26
#define CHIMERA_VFS_OP_LOCK             27
#define CHIMERA_VFS_OP_GETPARENT        28
#define CHIMERA_VFS_OP_COPY_RANGE       29
#define CHIMERA_VFS_OP_CLONE_RANGE      30
#define CHIMERA_VFS_OP_MOVE_RANGE       31
#define CHIMERA_VFS_OP_GET_XATTR        32
#define CHIMERA_VFS_OP_SET_XATTR        33
#define CHIMERA_VFS_OP_LIST_XATTRS      34
#define CHIMERA_VFS_OP_REMOVE_XATTR     35
#define CHIMERA_VFS_OP_GET_LAYOUT       36
#define CHIMERA_VFS_OP_NUM              37

#define CHIMERA_VFS_OPEN_CREATE         (1U << 0)
#define CHIMERA_VFS_OPEN_PATH           (1U << 1)
#define CHIMERA_VFS_OPEN_INFERRED       (1U << 2)
#define CHIMERA_VFS_OPEN_DIRECTORY      (1U << 3)
#define CHIMERA_VFS_OPEN_READ_ONLY      (1U << 4)
#define CHIMERA_VFS_OPEN_EXCLUSIVE      (1U << 5)
#define CHIMERA_VFS_OPEN_NOFOLLOW       (1U << 6)
/* Replace an existing file's contents on open: truncate to zero and apply
 * set_attr (used for the SMB OVERWRITE / OVERWRITE_IF / SUPERSEDE
 * dispositions).  Backends that do not honor it simply open the file. */
#define CHIMERA_VFS_OPEN_TRUNCATE       (1U << 7)

/* Allocate flags */
#define CHIMERA_VFS_ALLOCATE_DEALLOCATE 0x01

/* Lock types */
#define CHIMERA_VFS_LOCK_READ           0 /* shared / read lock */
#define CHIMERA_VFS_LOCK_WRITE          1 /* exclusive / write lock */
#define CHIMERA_VFS_LOCK_UNLOCK         2 /* release lock */

/* Lock flags */
#define CHIMERA_VFS_LOCK_WAIT           (1U << 0) /* block until lock is acquired (F_SETLKW) */
#define CHIMERA_VFS_LOCK_TEST           (1U << 1) /* probe only, do not acquire (F_GETLK) */

/* Readdir flags */
#define CHIMERA_VFS_READDIR_EMIT_DOT    (1U << 0) /* Emit "." and ".." entries */

#define CHIMERA_VFS_OPEN_ID_SYNTHETIC   0
#define CHIMERA_VFS_OPEN_ID_PATH        1
#define CHIMERA_VFS_OPEN_ID_FILE        2

struct chimera_vfs_metrics {
    struct prometheus_metrics           *metrics;
    struct prometheus_histogram         *op_latency;
    struct prometheus_histogram_series **op_latency_series;
};

struct chimera_vfs_thread_metrics {
    struct prometheus_histogram_instance **op_latency_series;
};

#define CHIMERA_VFS_OPEN_HANDLE_EXCLUSIVE 0x1
#define CHIMERA_VFS_OPEN_HANDLE_PENDING   0x2
#define CHIMERA_VFS_OPEN_HANDLE_FILE_ID   0x4
#define CHIMERA_VFS_OPEN_HANDLE_DETACHED  0x8

#define CHIMERA_VFS_ACCESS_MODE_RW        0
#define CHIMERA_VFS_ACCESS_MODE_RO        1

struct chimera_vfs_open_handle {
    struct chimera_vfs_module      *vfs_module;
    uint64_t                        fh_hash;
    uint16_t                        fh_len;
    uint8_t                         cache_id;
    uint8_t                         flags;
    uint8_t                         access_mode;
    /* Whether the most recent open via this handle created the file (vs opened
     * an existing one).  Refreshed on every open completion and read by the SMB
     * create path immediately afterwards to report OPENED vs CREATED. */
    uint8_t                         r_created;
    uint32_t                        opencnt;
    struct chimera_vfs_request     *blocked_requests;
    uint64_t                        vfs_private;
    void                            ( *callback )(
        struct chimera_vfs_request     *request,
        struct chimera_vfs_open_handle *handle);
    struct chimera_vfs_request     *request;
    struct timespec                 timestamp;
    struct chimera_vfs_open_handle *bucket_next;
    struct chimera_vfs_open_handle *bucket_prev;
    struct chimera_vfs_open_handle *prev;
    struct chimera_vfs_open_handle *next;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE + 16];

    /* Delete-on-close state: when doc_delete_on_close is set and opencnt
     * drops to zero, the file at (doc_parent_fh, doc_name) is removed
     * before the handle is recycled. */
    uint8_t                         doc_delete_on_close;
    uint16_t                        doc_parent_fh_len;
    uint16_t                        doc_name_len;
    struct chimera_vfs_cred         doc_cred;
    uint8_t                         doc_parent_fh[CHIMERA_VFS_FH_SIZE];
    char                            doc_name[CHIMERA_VFS_NAME_MAX];
};


typedef void (*chimera_vfs_lookup_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

typedef void (*chimera_vfs_create_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

/* Path-based operation callbacks */

typedef void (*chimera_vfs_open_callback_t)(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data);

typedef void (*chimera_vfs_mkdir_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

typedef void (*chimera_vfs_remove_callback_t)(
    enum chimera_vfs_error error_code,
    void                  *private_data);

typedef void (*chimera_vfs_rename_callback_t)(
    enum chimera_vfs_error error_code,
    void                  *private_data);

typedef void (*chimera_vfs_symlink_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

typedef void (*chimera_vfs_link_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

typedef void (*chimera_vfs_mknod_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

typedef void (*chimera_vfs_complete_callback_t)(
    struct chimera_vfs_request *request);

typedef int (*chimera_vfs_readdir_callback_t)(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg);

typedef void (*chimera_vfs_readdir_complete_t)(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        eof,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data);


typedef int (*chimera_vfs_filter_callback_t)(
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *attr,
    void                           *private_data);

typedef int (*chimera_vfs_find_callback_t)(
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *attr,
    void                           *private_data);

typedef void (*chimera_vfs_find_complete_t)(
    enum chimera_vfs_error error_code,
    void                  *private_data);

/* KV operation callbacks */

typedef void (*chimera_vfs_put_key_callback_t)(
    enum chimera_vfs_error error_code,
    void                  *private_data);

typedef void (*chimera_vfs_get_key_callback_t)(
    enum chimera_vfs_error error_code,
    const void            *value,
    uint32_t               value_len,
    void                  *private_data);

typedef void (*chimera_vfs_delete_key_callback_t)(
    enum chimera_vfs_error error_code,
    void                  *private_data);

/* Returns non-zero to abort the search */
typedef int (*chimera_vfs_search_keys_callback_t)(
    const void *key,
    uint32_t    key_len,
    const void *value,
    uint32_t    value_len,
    void       *private_data);

typedef void (*chimera_vfs_search_keys_complete_t)(
    enum chimera_vfs_error error_code,
    void                  *private_data);


struct chimera_vfs_find_result {
    int                             path_len;
    int                             emitted;
    struct chimera_vfs_request     *child_request;
    struct chimera_vfs_find_result *prev;
    struct chimera_vfs_find_result *next;
    struct chimera_vfs_attrs        attrs;
    char                            path[CHIMERA_VFS_PATH_MAX];
};


#define CHIMERA_VFS_ACCESS_READ         0x01
#define CHIMERA_VFS_ACCESS_WRITE        0x02
#define CHIMERA_VFS_ACCESS_EXECUTE      0x04

struct chimera_vfs_request_handle {
    uint8_t slot;
};

#define CHIMERA_VFS_REQUEST_MAX_HANDLES 3

struct chimera_vfs_request {
    struct chimera_vfs_thread         *thread;
    const struct chimera_vfs_cred     *cred;
    uint32_t                           opcode;
    enum chimera_vfs_error             status;
    chimera_vfs_complete_callback_t    complete;
    chimera_vfs_complete_callback_t    complete_delegate;
    struct timespec                    start_time;
    uint64_t                           elapsed_ns;

    /* Points to one page of memory that the plugin may use as desired */
    void                              *plugin_data;

    /* For use by the plugin if desired, see io_uring for example */
    struct chimera_vfs_request_handle  handle[CHIMERA_VFS_REQUEST_MAX_HANDLES];
    uint8_t                            token_count;

    struct chimera_vfs_module         *module;
    void                              *proto_callback;
    void                              *proto_private_data;

    /* VFS plugins may use these while processing the request */
    struct chimera_vfs_request        *prev;
    struct chimera_vfs_request        *next;

    /* For use by vfs core only */
    struct chimera_vfs_request        *active_prev;
    struct chimera_vfs_request        *active_next;

    uint8_t                            fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                           fh_len;
    uint64_t                           fh_hash;

    /* Implicit I/O lease mediation (chimera_vfs_io_lease_acquire).  For a
     * lease-holding client (NFSv4 delegation, SMB oplock) the protocol fills
     * io_owner + io_owner_valid so the I/O coalesces with the client's own
     * lease instead of recalling it.  io_next is the continuation invoked
     * once the lease is held (normally chimera_vfs_dispatch); io_lease_file
     * is the per-file state whose implicit lease this request has pinned
     * (NULL on the fast path where nothing was pinned). */
    struct chimera_vfs_lease_owner     io_owner;
    uint8_t                            io_owner_valid;
    /* Set by chimera_vfs_io_recall(): this request is a namespace/metadata
     * mutation that must recall every caching lease on a target file (regardless
     * of owner) rather than hold an implicit lease. */
    uint8_t                            io_recall_all;
    void                               ( *io_next )(
        struct chimera_vfs_request *request);
    struct chimera_vfs_file_state     *io_lease_file;
    struct chimera_vfs_pending_acquire io_lease_ticket;

    struct chimera_vfs_open_handle    *pending_handle;

    void                               ( *unblock_callback )(
        struct chimera_vfs_request     *request,
        struct chimera_vfs_open_handle *handle);

    union {
        struct {
            char                           *path;
            char                           *pathc;
            int                             pathlen;
            struct chimera_vfs_open_handle *handle;
            uint64_t                        attr_mask;
            uint32_t                        flags;
            uint32_t                        symlink_count;
            chimera_vfs_lookup_callback_t   callback;
            void                           *private_data;
            uint8_t                         next_fh[CHIMERA_VFS_FH_SIZE];
            uint8_t                         parent_fh[CHIMERA_VFS_FH_SIZE];
            int                             parent_fh_len;
        } lookup;

        struct {
            char                           *path;
            char                           *pathc;
            int                             pathlen;
            struct chimera_vfs_open_handle *handle;
            struct chimera_vfs_attrs       *set_attr;
            uint64_t                        attr_mask;
            chimera_vfs_lookup_callback_t   callback;
            void                           *private_data;
            uint8_t                         next_fh[CHIMERA_VFS_FH_SIZE];
        } create;

        struct {
            char                           *path;
            int                             pathlen;
            int                             parent_len;
            int                             name_offset;
            uint32_t                        flags;
            struct chimera_vfs_attrs       *set_attr;
            uint64_t                        attr_mask;
            struct chimera_vfs_open_handle *parent_handle;
            chimera_vfs_open_callback_t     callback;
            void                           *private_data;
            uint8_t                         parent_fh[CHIMERA_VFS_FH_SIZE];
            int                             parent_fh_len;
        } open;

        struct {
            char                           *path;
            int                             pathlen;
            int                             parent_len;
            int                             name_offset;
            struct chimera_vfs_attrs       *set_attr;
            uint64_t                        attr_mask;
            struct chimera_vfs_open_handle *parent_handle;
            chimera_vfs_mkdir_callback_t    callback;
            void                           *private_data;
            uint8_t                         parent_fh[CHIMERA_VFS_FH_SIZE];
            int                             parent_fh_len;
        } mkdir;

        struct {
            char                           *path;
            int                             pathlen;
            int                             parent_len;
            int                             name_offset;
            struct chimera_vfs_open_handle *parent_handle;
            chimera_vfs_remove_callback_t   callback;
            void                           *private_data;
            uint8_t                         parent_fh[CHIMERA_VFS_FH_SIZE];
            int                             parent_fh_len;
            uint8_t                         child_fh[CHIMERA_VFS_FH_SIZE];
            int                             child_fh_len;
        } remove;

        struct {
            char                         *path;
            int                           pathlen;
            int                           parent_len;
            int                           name_offset;
            char                         *new_path;
            int                           new_pathlen;
            int                           new_parent_len;
            int                           new_name_offset;
            chimera_vfs_rename_callback_t callback;
            void                         *private_data;
            uint8_t                       old_parent_fh[CHIMERA_VFS_FH_SIZE];
            int                           old_parent_fh_len;
            uint8_t                       new_parent_fh[CHIMERA_VFS_FH_SIZE];
            int                           new_parent_fh_len;
            uint8_t                       target_fh[CHIMERA_VFS_FH_SIZE];
            int                           target_fh_len;
        } rename;

        struct {
            char                           *path;
            int                             pathlen;
            int                             parent_len;
            int                             name_offset;
            const char                     *target;
            int                             targetlen;
            struct chimera_vfs_attrs       *set_attr;
            uint64_t                        attr_mask;
            struct chimera_vfs_open_handle *parent_handle;
            chimera_vfs_symlink_callback_t  callback;
            void                           *private_data;
            uint8_t                         parent_fh[CHIMERA_VFS_FH_SIZE];
            int                             parent_fh_len;
        } symlink;

        struct {
            char                       *path;
            int                         pathlen;
            char                       *new_path;
            int                         new_pathlen;
            int                         new_parent_len;
            int                         new_name_offset;
            unsigned int                replace;
            uint64_t                    attr_mask;
            chimera_vfs_link_callback_t callback;
            void                       *private_data;
            uint8_t                     source_fh[CHIMERA_VFS_FH_SIZE];
            int                         source_fh_len;
            uint8_t                     dest_parent_fh[CHIMERA_VFS_FH_SIZE];
            int                         dest_parent_fh_len;
        } link;

        struct {
            char                           *path;
            int                             pathlen;
            int                             parent_len;
            int                             name_offset;
            struct chimera_vfs_attrs       *set_attr;
            uint64_t                        attr_mask;
            struct chimera_vfs_open_handle *parent_handle;
            chimera_vfs_mknod_callback_t    callback;
            void                           *private_data;
            uint8_t                         parent_fh[CHIMERA_VFS_FH_SIZE];
            int                             parent_fh_len;
        } mknod;

        struct {
            char                           *path;
            int                             path_len;
            int16_t                         is_complete;
            int16_t                         complete_called;
            uint64_t                        attr_mask;
            struct chimera_vfs_request     *root;
            struct chimera_vfs_find_result *parent;
            struct chimera_vfs_find_result *results;
            chimera_vfs_filter_callback_t   filter;
            chimera_vfs_find_callback_t     callback;
            chimera_vfs_find_complete_t     complete;
            void                           *private_data;
        } find;

        struct {
            uint8_t                          fh_magic;
            const char                      *path;
            uint32_t                         pathlen;
            struct chimera_vfs_module       *module;
            const char                      *mount_path;
            uint32_t                         mount_pathlen;
            struct chimera_vfs_mount_options options;
            char                             options_buffer[CHIMERA_VFS_MOUNT_OPT_BUFFER_MAX];
            void                            *r_mount_private;
            struct chimera_vfs_attrs         r_attr;
        } mount;

        struct {
            struct chimera_vfs_mount *mount;
            void                     *mount_private;
        } umount;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        component_hash;
            const char                     *component;
            uint32_t                        component_len;
            struct chimera_vfs_attrs        r_attr;
            struct chimera_vfs_attrs        r_dir_attr;
        } lookup_at;

        struct {
            struct chimera_vfs_request *getattr;
            struct chimera_vfs_request *dir_getattr;
        } lookup_hit;

        struct {
            struct chimera_vfs_open_handle *handle;
            struct chimera_vfs_attrs        r_attr;
        } getattr;

        struct {
            struct chimera_vfs_open_handle *handle;
            struct chimera_vfs_attrs       *set_attr;
            struct chimera_vfs_attrs        r_pre_attr;
            struct chimera_vfs_attrs        r_post_attr;
        } setattr;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        cookie;
            uint64_t                        verifier;
            uint64_t                        attr_mask;
            uint32_t                        flags;
            uint64_t                        r_cookie;
            uint64_t                        r_verifier;
            uint32_t                        r_eof;
            struct chimera_vfs_attrs        r_dir_attr;
            chimera_vfs_readdir_callback_t  callback;

            struct evpl_iovec               bounce_iov;
            int                             bounce_offset;
            chimera_vfs_readdir_callback_t  orig_callback;
            void                           *orig_private_data;
        } readdir;

        struct {
            struct chimera_vfs_open_handle *handle;
            const char                     *name;
            uint32_t                        name_len;
            uint64_t                        name_hash;
            struct chimera_vfs_attrs       *set_attr;
            struct chimera_vfs_attrs        r_attr;
            struct chimera_vfs_attrs        r_dir_pre_attr;
            struct chimera_vfs_attrs        r_dir_post_attr;
        } mkdir_at;

        struct {
            struct chimera_vfs_open_handle *handle;
            const char                     *name;
            uint32_t                        name_len;
            uint64_t                        name_hash;
            struct chimera_vfs_attrs       *set_attr;
            struct chimera_vfs_attrs        r_attr;
            struct chimera_vfs_attrs        r_dir_pre_attr;
            struct chimera_vfs_attrs        r_dir_post_attr;
        } mknod_at;

        struct {
            uint32_t                         flags;
            /* Optional: opaque record to persist atomically with the open,
             * honored only by modules advertising CAP_ATOMIC_HANDLE_STATE. */
            struct chimera_vfs_handle_state *handle_state;
            uint64_t                         r_vfs_private;
        } open_fh;

        struct {
            struct chimera_vfs_open_handle  *handle;
            const char                      *name;
            uint64_t                         name_hash;
            int                              namelen;
            uint32_t                         flags;
            struct chimera_vfs_attrs        *set_attr;
            /* Optional: opaque record to persist atomically with the open,
             * honored only by modules advertising CAP_ATOMIC_HANDLE_STATE. */
            struct chimera_vfs_handle_state *handle_state;
            struct chimera_vfs_attrs         r_attr;
            struct chimera_vfs_attrs         r_dir_pre_attr;
            struct chimera_vfs_attrs         r_dir_post_attr;
            uint64_t                         r_vfs_private;
            /* Set by the module when the open created a new file (vs opened an
             * existing one); lets the SMB server report OPENED vs CREATED.
             * Modules that don't set it leave it 0 (treated as "opened"). */
            uint8_t                          r_created;
        } open_at;

        struct {
            uint32_t                  flags;
            struct chimera_vfs_attrs *set_attr;
            struct chimera_vfs_attrs  r_attr;
            uint64_t                  r_vfs_private;
        } create_unlinked;

        struct {
            uint64_t vfs_private;
        } close;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            uint32_t                        length;
            uint64_t                        attrmask;
            struct evpl_iovec              *iov;
            int                             niov;
            int                             r_niov;
            uint32_t                        r_length;
            uint32_t                        r_eof;
            struct chimera_vfs_attrs        r_attr;
        } read;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            uint32_t                        length;
            uint32_t                        sync;
            struct evpl_iovec              *iov;
            int                             niov;
            uint32_t                        r_sync;
            uint32_t                        r_length;
            struct chimera_vfs_attrs        r_pre_attr;
            struct chimera_vfs_attrs        r_post_attr;
        } write;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            uint64_t                        length;
            struct chimera_vfs_attrs        r_pre_attr;
            struct chimera_vfs_attrs        r_post_attr;
        } commit;

        struct {
            struct chimera_vfs_open_handle *handle;
            const char                     *name;
            int                             namelen;
            uint64_t                        name_hash;
            const uint8_t                  *child_fh;     /* Optional: child FH if known */
            int                             child_fh_len; /* 0 if child_fh not provided */
            struct chimera_vfs_attrs        r_dir_pre_attr;
            struct chimera_vfs_attrs        r_dir_post_attr;
            struct chimera_vfs_attrs        r_removed_attr;
        } remove_at;

        struct {
            struct chimera_vfs_open_handle *handle;
            const char                     *name;
            int                             namelen;
            uint64_t                        name_hash;
            const char                     *target;
            int                             targetlen;
            struct chimera_vfs_attrs       *set_attr;
            struct chimera_vfs_attrs        r_attr;
            struct chimera_vfs_attrs        r_dir_pre_attr;
            struct chimera_vfs_attrs        r_dir_post_attr;
        } symlink_at;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint32_t                        target_maxlength;
            uint32_t                        r_target_length;
            void                           *r_target;
            struct chimera_vfs_attrs        r_attr;
        } readlink;

        struct {
            const char              *name;
            int                      namelen;
            uint64_t                 name_hash;
            uint64_t                 new_fh_hash;
            const void              *new_fh;
            int                      new_fhlen;
            uint64_t                 new_name_hash;
            const char              *new_name;
            int                      new_namelen;
            const uint8_t           *target_fh; /* Optional: target FH if known (for silly rename) */
            int                      target_fh_len; /* 0 if target_fh not provided */
            struct chimera_vfs_attrs r_fromdir_pre_attr;
            struct chimera_vfs_attrs r_fromdir_post_attr;
            struct chimera_vfs_attrs r_todir_pre_attr;
            struct chimera_vfs_attrs r_todir_post_attr;
        } rename_at;

        struct {
            const void              *dir_fh;
            int                      dir_fhlen;
            uint64_t                 dir_fh_hash;
            const char              *name;
            int                      namelen;
            unsigned int             replace;
            uint64_t                 name_hash;
            struct chimera_vfs_attrs r_attr;
            struct chimera_vfs_attrs r_replaced_attr;
            struct chimera_vfs_attrs r_dir_pre_attr;
            struct chimera_vfs_attrs r_dir_post_attr;
        } link_at;

        struct {
            const void *key;
            uint32_t    key_len;
            const void *value;
            uint32_t    value_len;
        } put_key;

        struct {
            const void *key;
            uint32_t    key_len;
            const void *r_value;
            uint32_t    r_value_len;
        } get_key;

        struct {
            const void *key;
            uint32_t    key_len;
        } delete_key;

        struct {
            const void                        *start_key;
            uint32_t                           start_key_len;
            const void                        *end_key;
            uint32_t                           end_key_len;
            chimera_vfs_search_keys_callback_t callback;
        } search_keys;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            uint64_t                        length;
            uint32_t                        flags;
            struct chimera_vfs_attrs        r_pre_attr;
            struct chimera_vfs_attrs        r_post_attr;
        } allocate;

        struct {
            struct chimera_vfs_open_handle *src_handle;
            struct chimera_vfs_open_handle *dst_handle;
            uint64_t                        src_offset;
            uint64_t                        dst_offset;
            uint64_t                        length;
            uint64_t                        r_length;
            struct chimera_vfs_attrs        r_pre_attr;
            struct chimera_vfs_attrs        r_post_attr;
        } copy_range;

        struct {
            struct chimera_vfs_open_handle *src_handle;
            struct chimera_vfs_open_handle *dst_handle;
            uint64_t                        src_offset;
            uint64_t                        dst_offset;
            uint64_t                        length;
            struct chimera_vfs_attrs        r_pre_attr;
            struct chimera_vfs_attrs        r_post_attr;
        } clone_range;

        struct {
            struct chimera_vfs_open_handle *src_handle;
            struct chimera_vfs_open_handle *dst_handle;
            uint64_t                        src_offset;
            uint64_t                        dst_offset;
            uint64_t                        length;
            struct chimera_vfs_attrs        r_src_post_attr;
            struct chimera_vfs_attrs        r_dst_pre_attr;
            struct chimera_vfs_attrs        r_dst_post_attr;
        } move_range;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            uint32_t                        what;
            int                             r_eof;
            uint64_t                        r_offset;
        } seek;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;      /* first byte of range (raw l_start for SEEK_END) */
            uint64_t                        length;      /* 0 = to EOF (raw l_len for SEEK_END) */
            uint32_t                        lock_type;   /* CHIMERA_VFS_LOCK_{READ,WRITE,UNLOCK} */
            uint32_t                        flags;       /* CHIMERA_VFS_LOCK_{WAIT,TEST} */
            int32_t                         whence;      /* SEEK_SET or SEEK_END */
            /* Result fields: populated when CHIMERA_VFS_LOCK_TEST is set */
            uint32_t                        r_conflict_type;
            uint64_t                        r_conflict_offset;
            uint64_t                        r_conflict_length;
            pid_t                           r_conflict_pid;
        } lock;

        struct {
            uint8_t  r_parent_fh[CHIMERA_VFS_FH_SIZE];
            uint16_t r_parent_fh_len;
            char     r_name[CHIMERA_VFS_NAME_MAX];
            uint16_t r_name_len;
        } getparent;

        struct {
            struct chimera_vfs_open_handle *handle;
            const char                     *name;
            uint32_t                        namelen;
            void                           *value;        /* caller-provided buffer */
            uint32_t                        value_maxlen;
            uint32_t                        r_value_len;   /* bytes written to value */
        } get_xattr;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint32_t                        option;       /* setxattr_option4 */
            const char                     *name;
            uint32_t                        namelen;
            const void                     *value;
            uint32_t                        value_len;
            struct chimera_vfs_attrs        r_pre_attr;
            struct chimera_vfs_attrs        r_post_attr;
        } set_xattr;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        cookie;
            void                           *buffer;        /* caller-provided buffer */
            uint32_t                        max_bytes;
            /* buffer is filled with NUL-terminated names, back to back */
            uint32_t                        r_len;         /* bytes written to buffer */
            uint32_t                        r_count;       /* number of names written */
            uint32_t                        r_eof;
            uint64_t                        r_cookie;
        } list_xattrs;

        struct {
            struct chimera_vfs_open_handle *handle;
            const char                     *name;
            uint32_t                        namelen;
            struct chimera_vfs_attrs        r_pre_attr;
            struct chimera_vfs_attrs        r_post_attr;
        } remove_xattr;

        /* pNFS: a layout-sourcing backend (CHIMERA_VFS_CAP_LAYOUT_SOURCE)
         * describes where the file's data physically lives.  The NFS server
         * encodes the returned segments/devices into a flex-files or block
         * layout; the descriptors are protocol-neutral. */
        struct {
            struct chimera_vfs_open_handle   *handle;
            uint64_t                          offset;
            uint64_t                          length;
            uint32_t                          iomode;        /* LAYOUTIOMODE4_*           */
            uint32_t                          layout_class;  /* requested CHIMERA_VFS_LAYOUT_CLASS_* */
            uint32_t                          max_segments;
            uint32_t                          r_layout_class;/* class actually produced   */
            uint32_t                          r_num_segments;
            uint32_t                          r_num_devices;
            struct chimera_vfs_layout_segment r_segments[CHIMERA_VFS_LAYOUT_MAX_SEGMENTS];
            struct chimera_vfs_layout_device  r_devices[CHIMERA_VFS_LAYOUT_MAX_DEVICES];
        } get_layout;
    };
};




/* Each module must have a unique FH_MAGIC value
 * that can never be changed.  They can be reserved
 * here.
 *
 * The 1-byte magic must be the first byte of all
 * file handles returned by the plugin to ensure
 * uniqueness across plugins.
 *
 */

enum CHIMERA_FS_FH_MAGIC {
    /* Reserved for internal use by chimera */
    CHIMERA_VFS_FH_MAGIC_ROOT     = 0,
    CHIMERA_VFS_FH_MAGIC_MEMFS    = 1,
    CHIMERA_VFS_FH_MAGIC_LINUX    = 2,
    CHIMERA_VFS_FH_MAGIC_IO_URING = 3,
    CHIMERA_VFS_FH_MAGIC_CAIRN    = 4,
    CHIMERA_VFS_FH_MAGIC_DISKFS   = 5,
    CHIMERA_VFS_FH_MAGIC_NFS      = 6,
    CHIMERA_VFS_FH_MAGIC_MAX      = 7

};

/* If set, module requires open handles for path operations
 * such as mkdir, remove, open_at, etc.  Equivalent to POSIX open
 * with O_PATH flag.
 *
 * If not set, VFS may create synthetic open handles that
 * only contain the file handle w/o an explicit open callout
 * to the module for stateless operation (ie NFS3).
 */

#define CHIMERA_VFS_CAP_OPEN_PATH_REQUIRED  (1U << 0)


/* If set, module requires open handles for file operations
 * and for setattr on directories.
 *
 * If not set, VFS may create synthetic open handles that
 * only contain the file handle w/o an explicit open callout
 * to the module for stateless operation (ie NFS3).
 */
#define CHIMERA_VFS_CAP_OPEN_FILE_REQUIRED  (1U << 1)

/* If set, dispatch function is synchronous/blocking
 * and chimera will delegate VFS requests to a separate
 * threadpool.  This is useful for modules that perform
 * blocking operations such as I/O.
 *
 * If not set, VFS requests will be dispatched from the
 * main threadpool and the dispatch function is expected
 * to return quickly.
 */
#define CHIMERA_VFS_CAP_BLOCKING            (1U << 2)

/* If set, module supports chimera_vfs_create_unlinked()
 * Used primarily for S3 PUT.
 */

#define CHIMERA_VFS_CAP_CREATE_UNLINKED     (1U << 3)

/* If set, module supports filesystem operations (directories, files, etc.)
 * All current backends should declare this capability.
 */
#define CHIMERA_VFS_CAP_FS                  (1U << 4)

/* If set, module supports key-value operations
 * (put_key, get_key, delete_key, search_keys)
 */
#define CHIMERA_VFS_CAP_KV                  (1U << 5)

/* If set, module supports FH-relative operations (lookup_at, mkdir_at, etc.)
 * All current backends support this.
 */
#define CHIMERA_VFS_CAP_FS_RELATIVE_OP      (1U << 6)

/* If set, module supports path-based operations (open, mkdir, etc.)
 * Path-based operations take a full path relative to the mount point.
 * If a module does not support path ops, the VFS core will resolve
 * path components one at a time using FH-relative operations.
 */
#define CHIMERA_VFS_CAP_FS_PATH_OP          (1U << 7)

/* If set, module supports byte-range file locking via chimera_vfs_lock(). */
#define CHIMERA_VFS_CAP_FS_LOCK             (1U << 8)

/* If set, module supports reverse path lookup: given a directory FH,
 * resolve (parent_fh, name_in_parent).  Enables precise subtree
 * change notifications via CHIMERA_VFS_OP_GETPARENT. */
#define CHIMERA_VFS_CAP_RPL                 (1U << 9)

/* If set, module supports server-side byte-range copy between two
 * open handles served by the same module (chimera_vfs_copy_range). */
#define CHIMERA_VFS_CAP_COPY_RANGE          (1U << 10)

/* If set, module supports reflink/COW share of a byte range between
 * two open handles served by the same module (chimera_vfs_clone_range).
 * Modules backed by filesystems without reflink support should leave
 * this unset; the VFS layer will surface ENOTSUP. */
#define CHIMERA_VFS_CAP_CLONE_RANGE         (1U << 11)

/* If set, module supports zero-copy move of a byte range between two
 * open handles served by the same module (chimera_vfs_move_range): block
 * references are transferred from source to destination and the source
 * range becomes a hole. Not exposed over NFS; intended for server-internal
 * use (e.g. S3 multipart-upload completion). */
#define CHIMERA_VFS_CAP_MOVE_RANGE          (1U << 12)

/* If set, the module can persist an opaque "handle-state" record atomically
 * with an open/create (see struct chimera_vfs_handle_state below): the record
 * is committed in the same transaction as the open, so a crash cannot leave
 * the file created without its record (or vice versa).  Used by the SMB server
 * to persist SMB3 persistent-handle records.  Backends without this cap ignore
 * any handle_state passed on an open. */
#define CHIMERA_VFS_CAP_ATOMIC_HANDLE_STATE (1U << 13)

/* Opaque key/value record the caller asks the backend to persist atomically
 * as part of an open/create.  The VFS layer never interprets the bytes; for
 * the SMB server the key is "smbdh\0"+CreateGuid and the value is a serialized
 * persistent-handle record.  Stored in the backend's KV namespace, so it can
 * later be enumerated with chimera_vfs_search_keys and removed with
 * chimera_vfs_delete_key (clear-on-close / reap need not be atomic). */
struct chimera_vfs_handle_state {
    const void *key;
    uint32_t    key_len;
    const void *value;
    uint32_t    value_len;
};

/* If set, module supports extended attributes via
 * chimera_vfs_get_xattr / set_xattr / list_xattrs / remove_xattr.
 * Surfaced over NFSv4.2 (RFC 8276). Modules that leave this unset
 * cause the VFS layer to return ENOTSUP. */
#define CHIMERA_VFS_CAP_XATTR              (1U << 18)

/* setxattr_option4 values (RFC 8276 §8) passed to chimera_vfs_set_xattr().
 * Kept numerically identical to the on-the-wire NFSv4.2 enum. */
#define CHIMERA_VFS_XATTR_EITHER           0  /* create or replace */
#define CHIMERA_VFS_XATTR_CREATE           1  /* must not already exist */
#define CHIMERA_VFS_XATTR_REPLACE          2  /* must already exist */

/* Module persists the opaque CHIMERA_VFS_ATTR_PNFS_LAYOUT attribute, so the NFS
 * server can store per-file pNFS layout state on it and hand out pNFS layouts.
 * This is the "orchestrated" model: the module is a passive vessel and the NFS
 * server produces the layout (creating data-server backing files itself). */
#define CHIMERA_VFS_CAP_LAYOUT             (1U << 14)

/* Module SOURCES the layout itself: it already knows where a file's data
 * physically lives and synthesizes a protocol-neutral layout via
 * CHIMERA_VFS_OP_GET_LAYOUT.  The NFS server is the consumer (it only encodes
 * what the module returns) and does NO orchestration.  Mutually exclusive in
 * effect with CHIMERA_VFS_CAP_LAYOUT for a given file.  Exactly one of the
 * class bits below should accompany it. */
#define CHIMERA_VFS_CAP_LAYOUT_SOURCE      (1U << 15)
#define CHIMERA_VFS_CAP_LAYOUT_CLASS_FLEX  (1U << 16) /* produces flex-files (RFC 8435)  */
#define CHIMERA_VFS_CAP_LAYOUT_CLASS_BLOCK (1U << 17) /* produces block volume (RFC 5663)*/

struct chimera_vfs_module {
    /* Required
     * Short name for the module to be used in creating shares
     */

    const char *name;

    /* Required
     * Set to CHIMERA_FS_FH_MAGIC value reserved above
     */

    uint8_t     fh_magic;

    /* Required
     * Bitwise OR of CHIMERA_VFS_CAP_* above
     */

    uint64_t    capabilities;


    /* Optional
     * Called once at initialization to setup global state
     * Return a pointer to global state structure
     * Receives module-specific configuration JSON data as an argument.
     */

    void      * (*init)(
        const char *cfgdata);

    /* Optional
     * Called once at destruction to clean up global state
     * returned from the init function
     */

    void        (*destroy)(
        void *);

    /* Optional
     * Called once per thread at initialization to setup per-thread state
     * Receives global state pointer as an argument
     * Return a pointer to per-thread state structure
     */

    void      * (*thread_init)(
        struct evpl *evpl,
        void        *private_data);

    /* Optional
     * Called once per thread at destruction to clean up per-thread state
     * Receives per-thread state pointer as an argument
     */

    void        (*thread_destroy)(
        void *);

    /* Required
     * Called to dispatch a request to the module
     * Receives request and per-thread state pointer as an argument
     *
     * Module shuold call request->complete(request) when the
     * request processing is completed.
     *
     * If dispatch logic is blocking, set the blocking flag to 1 above.
     *
     * If blocking flag is unset, requests will be dispatched from
     * chimera's main threadpool, ie the same threadpool that is
     * pumping network traffic.  In this case the dispatch function is
     * expected to quickly complete and then asynchronously make the
     * complete callback later after any underlying slow operations
     * such as I/O have been asynchronously completed.
     *
     * If blocking flag is set, requests will be dispatched from a
     * separate dedicated pool of threads which will expect to process
     * only one request at a time.  The thread handoff adds overhead,
     * but nonetheless this scheme avoids stalling the main network
     * threads due to blocking inside VFS modules.
     *
     * Implementing VFS modules in a non-blocking manner is recommended
     * where feasible.
     */

    void (*dispatch)(
        struct chimera_vfs_request *request,
        void                       *private_data);

};

struct chimera_vfs_mount_attrs {
    uint64_t flags;
};

struct chimera_vfs_mount {
    struct chimera_vfs_module     *module;
    char                          *path;
    uint32_t                       pathlen;
    int                            root_fh_len;
    void                          *mount_private;
    struct chimera_vfs_mount_attrs attrs;
    struct chimera_vfs_mount      *prev;
    struct chimera_vfs_mount      *next;

    /* The first CHIMERA_VFS_MOUNT_ID_SIZE (16) bytes of root_fh is the mount_id,
     * which is itself a 128-bit hash. The remaining bytes are the fh_fragment.
     * Extra space is provided for NFS file handles which may exceed CHIMERA_VFS_FH_SIZE.
     */
    uint8_t                        root_fh[CHIMERA_VFS_FH_SIZE + 16];
};

enum chimera_vfs_delegation_mode {
    CHIMERA_VFS_DELEGATION_SYNC,
    CHIMERA_VFS_DELEGATION_ASYNC,
};

struct chimera_vfs_delegation_thread {
    struct evpl                *evpl;
    struct chimera_vfs         *vfs;
    struct evpl_thread         *evpl_thread;
    struct chimera_vfs_thread  *vfs_thread;
    struct chimera_vfs_request *requests;
    pthread_mutex_t             lock;
    struct evpl_doorbell        doorbell;
    enum chimera_vfs_delegation_mode mode;
    struct evpl_poll           *poll;
};

struct chimera_vfs_close_thread {
    struct evpl               *evpl;
    struct chimera_vfs        *vfs;
    struct evpl_thread        *evpl_thread;
    struct chimera_vfs_thread *vfs_thread;
    int                        shutdown;
    int                        num_pending;
    int                        signaled;
    struct evpl_doorbell       doorbell;
    struct evpl_timer          timer;
    pthread_mutex_t            lock;
    pthread_cond_t             cond;
};

struct chimera_vfs_mount_table;

struct chimera_vfs_notify;
struct chimera_vfs_state;
struct chimera_vfs_pnfs;

struct chimera_vfs {
    struct chimera_vfs_module            *modules[CHIMERA_VFS_FH_MAGIC_MAX];
    void                                 *module_private[CHIMERA_VFS_FH_MAGIC_MAX];
    struct chimera_vfs_module            *kv_module;
    struct vfs_open_cache                *vfs_open_path_cache;
    struct vfs_open_cache                *vfs_open_file_cache;
    struct chimera_vfs_name_cache        *vfs_name_cache;
    struct chimera_vfs_attr_cache        *vfs_attr_cache;
    struct chimera_vfs_user_cache        *vfs_user_cache;
    struct chimera_vfs_notify            *vfs_notify;
    struct chimera_vfs_state             *vfs_state;
    struct chimera_vfs_mount_table       *mount_table;
    struct chimera_vfs_pnfs              *pnfs;
    int                                   num_sync_delegation_threads;
    struct chimera_vfs_delegation_thread *sync_delegation_threads;
    int                                   num_async_delegation_threads;
    struct chimera_vfs_delegation_thread *async_delegation_threads;
    struct chimera_vfs_close_thread       close_thread;
    struct chimera_vfs_metrics            metrics;
    int                                   machine_name_len;
    char                                  machine_name[256];
};

struct chimera_vfs_thread {
    struct evpl                      *evpl;
    struct chimera_vfs               *vfs;
    void                             *module_private[CHIMERA_VFS_FH_MAGIC_MAX];
    struct chimera_vfs_find_result   *free_find_results;
    struct chimera_vfs_request       *free_requests;
    struct chimera_vfs_request       *active_requests;
    uint64_t                          num_active_requests;
    struct chimera_vfs_open_handle   *free_synth_handles;

    struct chimera_vfs_request       *pending_complete_requests;
    struct chimera_vfs_request       *unblocked_requests;
    /* Parked I/O requests being resumed on their owning thread (the lease
     * pump runs on whatever thread released/broke a lease, but a request's
     * dispatch+reply must run on the thread that owns its connection iovecs). */
    struct chimera_vfs_request       *pending_io_resume;
    struct evpl_doorbell              doorbell;
    pthread_mutex_t                   lock;
    uint64_t                          anon_fh_key;

    struct chimera_vfs_thread_metrics metrics;
};

struct chimera_vfs_module_cfg {
    char module_name[64];
    char module_path[256];
    char config_data[4096];
};

struct chimera_vfs *
chimera_vfs_init(
    int                                  num_sync_delegation_threads,
    int                                  num_async_delegation_threads,
    const struct chimera_vfs_module_cfg *module_cfgs,
    int                                  num_modules,
    const char                          *kv_module_name,
    int                                  cache_ttl,
    struct prometheus_metrics           *metrics);

void
chimera_vfs_destroy(
    struct chimera_vfs *vfs);

/* Get the root pseudo-filesystem's file handle */
void
chimera_vfs_get_root_fh(
    uint8_t  *fh,
    uint32_t *fh_len);

struct chimera_vfs_thread *
chimera_vfs_thread_init(
    struct evpl        *evpl,
    struct chimera_vfs *vfs);

void
chimera_vfs_thread_destroy(
    struct chimera_vfs_thread *thread);

void
chimera_vfs_register(
    struct chimera_vfs        *vfs,
    struct chimera_vfs_module *module,
    const char                *cfgdata);

void
chimera_vfs_thread_drain(
    struct chimera_vfs_thread *thread);


int
chimera_vfs_add_user(
    struct chimera_vfs *vfs,
    const char         *username,
    const char         *password,
    const char         *smbpasswd,
    const char         *sid,
    uint32_t            uid,
    uint32_t            gid,
    uint32_t            ngids,
    const uint32_t     *gids,
    int                 pinned);

int
chimera_vfs_remove_user(
    struct chimera_vfs *vfs,
    const char         *username);

const struct chimera_vfs_user *
chimera_vfs_lookup_user_by_name(
    struct chimera_vfs *vfs,
    const char         *username);

int
chimera_vfs_user_is_member(
    struct chimera_vfs *vfs,
    uint32_t            uid,
    uint32_t            gid);


typedef int (*chimera_vfs_user_iterate_cb)(
    const struct chimera_vfs_user *user,
    void                          *data);

void
chimera_vfs_iterate_builtin_users(
    struct chimera_vfs         *vfs,
    chimera_vfs_user_iterate_cb callback,
    void                       *data);
void
chimera_vfs_watchdog(
    struct chimera_vfs_thread *thread);
