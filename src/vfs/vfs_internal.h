#pragma once

#include <stdlib.h>
#include <time.h>

#include "common/logging.h"
#include "common/misc.h"
#include "uthash/utlist.h"

#include "vfs/vfs_dump.h"

#define chimera_vfs_debug(...) chimera_debug("vfs", \
                                             __FILE__, \
                                             __LINE__, \
                                             __VA_ARGS__)
#define chimera_vfs_info(...)  chimera_info("vfs", \
                                            __FILE__, \
                                            __LINE__, \
                                            __VA_ARGS__)
#define chimera_vfs_error(...) chimera_error("vfs", \
                                             __FILE__, \
                                             __LINE__, \
                                             __VA_ARGS__)
#define chimera_vfs_fatal(...) chimera_fatal("vfs", \
                                             __FILE__, \
                                             __LINE__, \
                                             __VA_ARGS__)
#define chimera_vfs_abort(...) chimera_abort("vfs", \
                                             __FILE__, \
                                             __LINE__, \
                                             __VA_ARGS__)

#define chimera_vfs_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "vfs", \
                         __FILE__, \
                         __LINE__, \
                         __VA_ARGS__)

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
    }

    request->status = CHIMERA_VFS_UNSET;
    clock_gettime(CLOCK_MONOTONIC, &request->start_time);

    return request;
} /* chimera_vfs_request_alloc */

static inline void
chimera_vfs_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread *thread = request->thread;

    struct timespec            now;

    clock_gettime(CLOCK_MONOTONIC, &now);

    request->elapsed_ns = chimera_get_elapsed_ns(&now, &request->start_time);

    thread->metrics[request->opcode].num_requests++;

    thread->metrics[request->opcode].total_latency += request->elapsed_ns;

    if (request->elapsed_ns > thread->metrics[request->opcode].max_latency) {
        thread->metrics[request->opcode].max_latency = request->elapsed_ns;
    }

    if (request->elapsed_ns < thread->metrics[request->opcode].min_latency ||
        thread->metrics[request->opcode].min_latency == 0) {
        thread->metrics[request->opcode].min_latency = request->elapsed_ns;
    }

    chimera_vfs_dump_reply(request);
} /* chimera_vfs_complete */

static inline void
chimera_vfs_request_free(
    struct chimera_vfs_thread  *thread,
    struct chimera_vfs_request *request)
{
    DL_PREPEND(thread->free_requests, request);
} /* chimera_vfs_request_free */

static inline struct chimera_vfs_module *
chimera_vfs_get_module(
    struct chimera_vfs_thread *thread,
    const void                *fh,
    int                        fhlen)
{
    struct chimera_vfs *vfs = thread->vfs;

    uint8_t             fh_magic;

    if (fhlen < 1) {
        return NULL;
    }

    fh_magic = *(uint8_t *) fh;

    return vfs->modules[fh_magic];
} /* chimera_vfs_get_module */

static inline void
chimera_vfs_dispatch(
    struct chimera_vfs_thread  *thread,
    struct chimera_vfs_module  *module,
    struct chimera_vfs_request *request)
{
    chimera_vfs_dump_request(request);
    module->dispatch(request, thread->module_private[module->fh_magic]);
} /* chimera_vfs_dispatch */
