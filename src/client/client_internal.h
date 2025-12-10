// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

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
    CHIMERA_CLIENT_OP_OPEN,
    CHIMERA_CLIENT_OP_MKDIR,
    CHIMERA_CLIENT_OP_READ,
    CHIMERA_CLIENT_OP_WRITE,
};

struct chimera_client_request {
    enum chimera_client_request_opcode opcode;
    struct chimera_client_thread      *thread;
    struct chimera_client_request     *prev;
    struct chimera_client_request     *next;

    union {
        struct {
            struct chimera_vfs_open_handle *parent_handle;
            chimera_open_callback_t         callback;
            void                           *private_data;
            unsigned int                    flags;
            int                             path_len;
            int                             parent_len;
            int                             name_offset;
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
            int                     niov;
            chimera_read_callback_t callback;
            void                   *private_data;
            struct evpl_iovec       iov[CHIMERA_CLIENT_IOV_MAX];
        } read;

        struct {
            chimera_write_callback_t callback;
            void                    *private_data;
        } write;
    };
};

struct chimera_client_config {
    int                           core_threads;
    int                           delegation_threads;
    int                           cache_ttl;
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

    return request;
} /* chimera_client_request_alloc */

static inline void
chimera_client_request_free(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    DL_PREPEND(thread->free_requests, request);
} /* chimera_client_request_free */