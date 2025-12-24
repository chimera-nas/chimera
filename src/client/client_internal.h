// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <pthread.h>
#include <utlist.h>

#include "client.h"
#include "common/macros.h"
#include "vfs/vfs.h"
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

extern const uint8_t root_fh[1];


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

    ssize_t                            sync_result;
    struct chimera_vfs_open_handle    *sync_open_handle;
    struct chimera_stat                sync_stat;
    int                                sync_target_len;

    chimera_client_request_callback    sync_callback;
    struct chimera_client_request     *sync_next;

    uint32_t                           fh_len;

    uint8_t                            fh[CHIMERA_VFS_FH_SIZE];

    union {
        struct {
            chimera_mount_callback_t callback;
            void                    *private_data;
            char                     mount_path[CHIMERA_VFS_PATH_MAX];
            char                     module_path[CHIMERA_VFS_PATH_MAX];
            char                     module_name[64];
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
            char                            path[CHIMERA_VFS_PATH_MAX];
        } mkdir;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            uint32_t                        length;
            int                             niov;
            chimera_read_callback_t         callback;
            void                           *private_data;
            void                           *buf;
            struct evpl_iovec               iov[CHIMERA_CLIENT_IOV_MAX];
        } read;

        struct {
            struct chimera_vfs_open_handle *handle;
            uint64_t                        offset;
            uint32_t                        length;
            int                             niov;
            chimera_write_callback_t        callback;
            void                           *private_data;
            struct evpl_iovec               iov[CHIMERA_CLIENT_IOV_MAX];
        } write;

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
            char                            source_path[CHIMERA_VFS_PATH_MAX];
            char                            dest_path[CHIMERA_VFS_PATH_MAX];
            uint8_t                         source_fh[CHIMERA_VFS_FH_SIZE];
            uint8_t                         dest_fh[CHIMERA_VFS_FH_SIZE];
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
            int                             path_len;
            char                            path[CHIMERA_VFS_PATH_MAX];
        } stat;
    };
};

struct chimera_client_config {
    int                           core_threads;
    int                           delegation_threads;
    int                           cache_ttl;
    int                           max_fds;
    struct chimera_vfs_module_cfg modules[CHIMERA_CLIENT_MAX_MODULES];
    int                           num_modules;
};

struct chimera_client {
    const struct chimera_client_config *config;
    struct chimera_vfs                 *vfs;
};

struct chimera_client_thread {
    struct chimera_client         *client;
    struct chimera_vfs_thread     *vfs_thread;
    struct chimera_client_request *free_requests;
};

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