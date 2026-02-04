// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once
#include <stdint.h>
#include <sys/time.h>
#include <uthash.h>
#include "vfs_dump.h"
#include "vfs_error.h"
#include "vfs_cred.h"
#include "evpl/evpl.h"


#define CHIMERA_VFS_FH_SIZE              48

#define CHIMERA_VFS_PATH_MAX             4096
#define CHIMERA_VFS_NAME_MAX             256
struct evpl;

struct chimera_vfs;
struct prometheus_metrics;

#define CHIMERA_VFS_ATTR_DEV             (1UL << 0)
#define CHIMERA_VFS_ATTR_INUM            (1UL << 1)
#define CHIMERA_VFS_ATTR_MODE            (1UL << 2)
#define CHIMERA_VFS_ATTR_NLINK           (1UL << 3)
#define CHIMERA_VFS_ATTR_UID             (1UL << 4)
#define CHIMERA_VFS_ATTR_GID             (1UL << 5)
#define CHIMERA_VFS_ATTR_RDEV            (1UL << 6)
#define CHIMERA_VFS_ATTR_SIZE            (1UL << 7)
#define CHIMERA_VFS_ATTR_ATIME           (1UL << 8)
#define CHIMERA_VFS_ATTR_MTIME           (1UL << 9)
#define CHIMERA_VFS_ATTR_CTIME           (1UL << 10)
#define CHIMERA_VFS_ATTR_SPACE_USED      (1UL << 11)

#define CHIMERA_VFS_ATTR_SPACE_AVAIL     (1UL << 12)
#define CHIMERA_VFS_ATTR_SPACE_FREE      (1UL << 13)
#define CHIMERA_VFS_ATTR_SPACE_TOTAL     (1UL << 14)
#define CHIMERA_VFS_ATTR_FILES_TOTAL     (1UL << 15)
#define CHIMERA_VFS_ATTR_FILES_FREE      (1UL << 16)
#define CHIMERA_VFS_ATTR_FILES_AVAIL     (1UL << 17)

#define CHIMERA_VFS_ATTR_FH              (1UL << 18)
#define CHIMERA_VFS_ATTR_ATOMIC          (1UL << 19)
#define CHIMERA_VFS_ATTR_FSID            (1UL << 20)

#define CHIMERA_VFS_ATTR_MASK_STAT       ( \
            CHIMERA_VFS_ATTR_DEV | \
            CHIMERA_VFS_ATTR_INUM | \
            CHIMERA_VFS_ATTR_MODE | \
            CHIMERA_VFS_ATTR_NLINK | \
            CHIMERA_VFS_ATTR_UID | \
            CHIMERA_VFS_ATTR_GID | \
            CHIMERA_VFS_ATTR_RDEV | \
            CHIMERA_VFS_ATTR_SIZE | \
            CHIMERA_VFS_ATTR_SPACE_USED | \
            CHIMERA_VFS_ATTR_ATIME | \
            CHIMERA_VFS_ATTR_MTIME | \
            CHIMERA_VFS_ATTR_CTIME)

#define CHIMERA_VFS_ATTR_MASK_STATFS     ( \
            CHIMERA_VFS_ATTR_SPACE_AVAIL | \
            CHIMERA_VFS_ATTR_SPACE_FREE | \
            CHIMERA_VFS_ATTR_SPACE_TOTAL | \
            CHIMERA_VFS_ATTR_FILES_TOTAL | \
            CHIMERA_VFS_ATTR_FILES_FREE | \
            CHIMERA_VFS_ATTR_FILES_AVAIL | \
            CHIMERA_VFS_ATTR_FSID)

#define CHIMERA_VFS_ATTR_MASK_CACHEABLE  ( \
            CHIMERA_VFS_ATTR_MASK_STAT)

#define CHIMERA_VFS_TIME_NOW             ((1l << 30) - 3l)

/* FSSTAT values used with builtin backends until statvfs tracking is implemented */
#define CHIMERA_VFS_SYNTHETIC_FS_BYTES   ((uint64_t) 100 * 1024 * 1024 * 1024)
#define CHIMERA_VFS_SYNTHETIC_FS_INODES  (1024 * 1024)

/* Flags for chimera_vfs_lookup_path */
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

struct chimera_vfs_attrs {
    uint64_t        va_req_mask;
    uint64_t        va_set_mask;

    uint64_t        va_dev;
    uint64_t        va_ino;
    uint64_t        va_mode;
    uint64_t        va_nlink;
    uint64_t        va_uid;
    uint64_t        va_gid;
    uint64_t        va_rdev;
    uint64_t        va_size;
    uint64_t        va_space_used;
    struct timespec va_atime;
    struct timespec va_mtime;
    struct timespec va_ctime;

    uint64_t        va_fs_space_avail;
    uint64_t        va_fs_space_free;
    uint64_t        va_fs_space_total;
    uint64_t        va_fs_space_used;
    uint64_t        va_fs_files_total;
    uint64_t        va_fs_files_free;
    uint64_t        va_fs_files_avail;
    uint64_t        va_fsid;

    uint32_t        va_fh_len;
    uint64_t        va_fh_hash;

    /* XXH3 uses SIMD memory loads that may read beyond the end
     * of the actual data, so we need to provide enough padding
     * to prevent this from causing compiler complaints?
     */
    uint8_t         va_fh[CHIMERA_VFS_FH_SIZE + 16];
};

#define CHIMERA_VFS_OP_MOUNT           1
#define CHIMERA_VFS_OP_UMOUNT          2
#define CHIMERA_VFS_OP_LOOKUP          3
#define CHIMERA_VFS_OP_GETATTR         4
#define CHIMERA_VFS_OP_READDIR         5
#define CHIMERA_VFS_OP_READLINK        6
#define CHIMERA_VFS_OP_OPEN            7
#define CHIMERA_VFS_OP_OPEN_AT         8
#define CHIMERA_VFS_OP_CLOSE           9
#define CHIMERA_VFS_OP_READ            10
#define CHIMERA_VFS_OP_WRITE           11
#define CHIMERA_VFS_OP_REMOVE          12
#define CHIMERA_VFS_OP_MKDIR           13
#define CHIMERA_VFS_OP_COMMIT          14
#define CHIMERA_VFS_OP_SYMLINK         15
#define CHIMERA_VFS_OP_RENAME          16
#define CHIMERA_VFS_OP_SETATTR         17
#define CHIMERA_VFS_OP_LINK            18
#define CHIMERA_VFS_OP_CREATE_UNLINKED 19
#define CHIMERA_VFS_OP_MKNOD           20
#define CHIMERA_VFS_OP_NUM             21

#define CHIMERA_VFS_OPEN_CREATE        (1U << 0)
#define CHIMERA_VFS_OPEN_PATH          (1U << 1)
#define CHIMERA_VFS_OPEN_INFERRED      (1U << 2)
#define CHIMERA_VFS_OPEN_DIRECTORY     (1U << 3)
#define CHIMERA_VFS_OPEN_READ_ONLY     (1U << 4)
#define CHIMERA_VFS_OPEN_EXCLUSIVE     (1U << 5)

/* Readdir flags */
#define CHIMERA_VFS_READDIR_EMIT_DOT   (1U << 0) /* Emit "." and ".." entries */

#define CHIMERA_VFS_OPEN_ID_SYNTHETIC  0
#define CHIMERA_VFS_OPEN_ID_PATH       1
#define CHIMERA_VFS_OPEN_ID_FILE       2

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

};


typedef void (*chimera_vfs_lookup_path_callback_t)(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data);

typedef void (*chimera_vfs_create_path_callback_t)(
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
    struct chimera_vfs_thread        *thread;
    const struct chimera_vfs_cred    *cred;
    uint32_t                          opcode;
    enum chimera_vfs_error            status;
    chimera_vfs_complete_callback_t   complete;
    chimera_vfs_complete_callback_t   complete_delegate;
    struct timespec                   start_time;
    uint64_t                          elapsed_ns;

    /* Points to one page of memory that the plugin may use as desired */
    void                             *plugin_data;

    /* For use by the plugin if desired, see io_uring for example */
    struct chimera_vfs_request_handle handle[CHIMERA_VFS_REQUEST_MAX_HANDLES];
    uint8_t                           token_count;

    struct chimera_vfs_module        *module;
    void                             *proto_callback;
    void                             *proto_private_data;

    /* VFS plugins may use these while processing the request */
    struct chimera_vfs_request       *prev;
    struct chimera_vfs_request       *next;

    /* For use by vfs core only */
    struct chimera_vfs_request       *active_prev;
    struct chimera_vfs_request       *active_next;

    uint8_t                           fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                          fh_len;
    uint64_t                          fh_hash;

    struct chimera_vfs_open_handle   *pending_handle;

    void                              ( *unblock_callback )(
        struct chimera_vfs_request     *request,
        struct chimera_vfs_open_handle *handle);

    union {
        struct {
            char                              *path;
            char                              *pathc;
            int                                pathlen;
            struct chimera_vfs_open_handle    *handle;
            uint64_t                           attr_mask;
            uint32_t                           flags;
            uint32_t                           symlink_count;
            chimera_vfs_lookup_path_callback_t callback;
            void                              *private_data;
            uint8_t                            next_fh[CHIMERA_VFS_FH_SIZE];
            uint8_t                            parent_fh[CHIMERA_VFS_FH_SIZE];
            int                                parent_fh_len;
        } lookup_path;

        struct {
            char                              *path;
            char                              *pathc;
            int                                pathlen;
            struct chimera_vfs_open_handle    *handle;
            struct chimera_vfs_attrs          *set_attr;
            uint64_t                           attr_mask;
            chimera_vfs_lookup_path_callback_t callback;
            void                              *private_data;
            uint8_t                            next_fh[CHIMERA_VFS_FH_SIZE];
        } create_path;

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
        } lookup;

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
        } mkdir;

        struct {
            struct chimera_vfs_open_handle *handle;
            const char                     *name;
            uint32_t                        name_len;
            uint64_t                        name_hash;
            struct chimera_vfs_attrs       *set_attr;
            struct chimera_vfs_attrs        r_attr;
            struct chimera_vfs_attrs        r_dir_pre_attr;
            struct chimera_vfs_attrs        r_dir_post_attr;
        } mknod;

        struct {
            uint32_t flags;
            uint64_t r_vfs_private;
        } open;

        struct {
            struct chimera_vfs_open_handle *handle;
            const char                     *name;
            uint64_t                        name_hash;
            int                             namelen;
            uint32_t                        flags;
            struct chimera_vfs_attrs       *set_attr;
            struct chimera_vfs_attrs        r_attr;
            struct chimera_vfs_attrs        r_dir_pre_attr;
            struct chimera_vfs_attrs        r_dir_post_attr;
            uint64_t                        r_vfs_private;
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
        } remove;

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
        } symlink;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint32_t                        target_maxlength;
            uint32_t                        r_target_length;
            void                           *r_target;
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
        } rename;

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
        } link;
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
    CHIMERA_VFS_FH_MAGIC_DEMOFS   = 5,
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

#define CHIMERA_VFS_CAP_OPEN_PATH_REQUIRED (1U << 0)


/* If set, module requires open handles for file operations
 * and for setattr on directories.
 *
 * If not set, VFS may create synthetic open handles that
 * only contain the file handle w/o an explicit open callout
 * to the module for stateless operation (ie NFS3).
 */
#define CHIMERA_VFS_CAP_OPEN_FILE_REQUIRED (1U << 1)

/* If set, dispatch function is synchronous/blocking
 * and chimera will delegate VFS requests to a separate
 * threadpool.  This is useful for modules that perform
 * blocking operations such as I/O.
 *
 * If not set, VFS requests will be dispatched from the
 * main threadpool and the dispatch function is expected
 * to return quickly.
 */
#define CHIMERA_VFS_CAP_BLOCKING           (1U << 2)

/* If set, module supports chimera_vfs_create_unlinked()
 * Used primarily for S3 PUT.
 */

#define CHIMERA_VFS_CAP_CREATE_UNLINKED    (1U << 3)

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
     */

    void      * (*init)(
        const char *cfgfile);

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

struct chimera_vfs_delegation_thread {
    struct evpl                *evpl;
    struct chimera_vfs         *vfs;
    struct evpl_thread         *evpl_thread;
    struct chimera_vfs_thread  *vfs_thread;
    struct chimera_vfs_request *requests;
    pthread_mutex_t             lock;
    struct evpl_doorbell        doorbell;
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

struct chimera_vfs {
    struct chimera_vfs_module            *modules[CHIMERA_VFS_FH_MAGIC_MAX];
    void                                 *module_private[CHIMERA_VFS_FH_MAGIC_MAX];
    struct vfs_open_cache                *vfs_open_path_cache;
    struct vfs_open_cache                *vfs_open_file_cache;
    struct chimera_vfs_name_cache        *vfs_name_cache;
    struct chimera_vfs_attr_cache        *vfs_attr_cache;
    struct chimera_vfs_mount_table       *mount_table;
    int                                   num_delegation_threads;
    struct chimera_vfs_delegation_thread *delegation_threads;
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
    int                                  num_delegation_threads,
    const struct chimera_vfs_module_cfg *module_cfgs,
    int                                  num_modules,
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
    const char                *cfgfile);

void
chimera_vfs_thread_drain(
    struct chimera_vfs_thread *thread);

void
chimera_vfs_watchdog(
    struct chimera_vfs_thread *thread);
