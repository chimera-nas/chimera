#pragma once

#include <stdlib.h>

#include "common/logging.h"
#include "uthash/utlist.h"

#define chimera_vfs_debug(...) chimera_debug("vfs", __VA_ARGS__)
#define chimera_vfs_info(...)  chimera_info("vfs", __VA_ARGS__)
#define chimera_vfs_error(...) chimera_error("vfs", __VA_ARGS__)
#define chimera_vfs_fatal(...) chimera_fatal("vfs", __VA_ARGS__)
#define chimera_vfs_abort(...) chimera_abort("core", __VA_ARGS__)

#define chimera_vfs_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "vfs", __VA_ARGS__)

static inline struct chimera_vfs_request *
chimera_vfs_request_alloc(struct chimera_vfs_thread *thread)
{
    struct chimera_vfs_request *request;

    if (thread->free_requests) {
        request = thread->free_requests;
        DL_DELETE(thread->free_requests, request);
    } else {
        request         = calloc(1, sizeof(struct chimera_vfs_request));
        request->thread = thread;
        request->status = CHIMERA_VFS_UNSET;
    }

    return request;
} /* chimera_vfs_request_alloc */

static inline void
chimera_vfs_request_free(
    struct chimera_vfs_thread  *thread,
    struct chimera_vfs_request *request)
{
    DL_PREPEND(thread->free_requests, request);
} /* chimera_vfs_request_free */
