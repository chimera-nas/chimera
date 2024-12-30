#pragma once

#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <xxhash.h>

#include "vfs/vfs.h"
#include "common/logging.h"
#include "common/misc.h"
#include "uthash/utlist.h"

#include "vfs/vfs_dump.h"
#ifndef container_of
#define container_of(ptr, type, member) ({            \
        typeof(((type *) 0)->member) * __mptr = (ptr); \
        (type *) ((char *) __mptr - offsetof(type, member)); })
#endif // ifndef container_of

#define chimera_vfs_debug(...)          chimera_debug("vfs", \
                                                      __FILE__, \
                                                      __LINE__, \
                                                      __VA_ARGS__)
#define chimera_vfs_info(...)           chimera_info("vfs", \
                                                     __FILE__, \
                                                     __LINE__, \
                                                     __VA_ARGS__)
#define chimera_vfs_error(...)          chimera_error("vfs", \
                                                      __FILE__, \
                                                      __LINE__, \
                                                      __VA_ARGS__)
#define chimera_vfs_fatal(...)          chimera_fatal("vfs", \
                                                      __FILE__, \
                                                      __LINE__, \
                                                      __VA_ARGS__)
#define chimera_vfs_abort(...)          chimera_abort("vfs", \
                                                      __FILE__, \
                                                      __LINE__, \
                                                      __VA_ARGS__)

#define chimera_vfs_fatal_if(cond, ...) \
        chimera_fatal_if(cond, "vfs", \
                         __FILE__, \
                         __LINE__, \
                         __VA_ARGS__)

#define chimera_vfs_abort_if(cond, ...) \
        chimera_abort_if(cond, "vfs", \
                         __FILE__, \
                         __LINE__, \
                         __VA_ARGS__)

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

static inline struct chimera_vfs_request *
chimera_vfs_request_alloc_by_hash(
    struct chimera_vfs_thread *thread,
    const void                *fh,
    int                        fhlen,
    uint64_t                   fh_hash)
{
    struct chimera_vfs_request *request;

    if (thread->free_requests) {
        request = thread->free_requests;
        LL_DELETE(thread->free_requests, request);
    } else {
        request              = calloc(1, sizeof(struct chimera_vfs_request));
        request->thread      = thread;
        request->plugin_data = malloc(4096);
    }

    request->status = CHIMERA_VFS_UNSET;

    request->module = chimera_vfs_get_module(thread, fh, fhlen);

    request->fh      = fh;
    request->fh_len  = fhlen;
    request->fh_hash = fh_hash;
    clock_gettime(CLOCK_MONOTONIC, &request->start_time);

    thread->active_requests++;

    return request;
} /* chimera_vfs_request_alloc_by_hash */

static inline struct chimera_vfs_request *
chimera_vfs_request_alloc(
    struct chimera_vfs_thread *thread,
    const void                *fh,
    int                        fhlen)
{
    uint64_t fh_hash = XXH3_64bits(fh, fhlen);

    return chimera_vfs_request_alloc_by_hash(thread, fh, fhlen, fh_hash);
} /* chimera_vfs_request_alloc */

static inline struct chimera_vfs_request *
chimera_vfs_request_alloc_by_handle(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle)
{
    return chimera_vfs_request_alloc_by_hash(thread, handle->fh, handle->fh_len, handle->fh_hash);
} /* chimera_vfs_request_alloc_by_handle */

static inline struct chimera_vfs_open_handle *
chimera_vfs_synth_handle_alloc(struct chimera_vfs_thread *thread)
{
    struct chimera_vfs_open_handle *handle;

    if (thread->free_synth_handles) {
        handle = thread->free_synth_handles;
        LL_DELETE(thread->free_synth_handles, handle);
    } else {
        handle           = calloc(1, sizeof(struct chimera_vfs_open_handle));
        handle->cache_id = CHIMERA_VFS_OPEN_ID_SYNTHETIC;
    }

    return handle;
} /* chimera_vfs_synth_handle_alloc */

static inline void
chimera_vfs_synth_handle_free(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle)
{
    chimera_vfs_abort_if(handle->cache_id != CHIMERA_VFS_OPEN_ID_SYNTHETIC, "real handle freed by synthetic procedure");
    LL_PREPEND(thread->free_synth_handles, handle);
} /* chimera_vfs_synth_handle_free */

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
    thread->active_requests--;

    LL_PREPEND(thread->free_requests, request);
} /* chimera_vfs_request_free */

static inline void
chimera_vfs_complete_delegate(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread *thread = request->thread;
    uint64_t                   one    = 1;

    pthread_mutex_lock(&thread->lock);
    DL_APPEND(thread->pending_complete_requests, request);
    pthread_mutex_unlock(&thread->lock);

    write(thread->eventfd, &one, sizeof(one));

} /* chimera_vfs_complete_delegate */

static inline void
chimera_vfs_dispatch(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread            *thread = request->thread;
    struct chimera_vfs                   *vfs    = thread->vfs;
    struct chimera_vfs_module            *module = request->module;
    struct chimera_vfs_delegation_thread *delegation_thread;
    int                                   thread_id;

    chimera_vfs_dump_request(request);

    if (module->blocking) {
        thread_id = request->fh_hash % vfs->num_delegation_threads;

        request->complete_delegate = request->complete;
        request->complete          = chimera_vfs_complete_delegate;

        delegation_thread = &vfs->delegation_threads[thread_id];

        pthread_mutex_lock(&delegation_thread->lock);
        DL_APPEND(delegation_thread->requests, request);
        pthread_mutex_unlock(&delegation_thread->lock);

        evpl_thread_wake(delegation_thread->evpl_thread);
    } else {
        module->dispatch(request, thread->module_private[module->fh_magic]);
    }
} /* chimera_vfs_dispatch */

static inline void
chimera_vfs_copy_attr(
    struct chimera_vfs_attrs       *dest,
    const struct chimera_vfs_attrs *src)
{
    dest->va_mask = src->va_mask;

    if (src->va_mask & CHIMERA_VFS_ATTR_FH) {
        memcpy(dest->va_fh, src->va_fh, src->va_fh_len);
        dest->va_fh_len = src->va_fh_len;
    }

    if (src->va_mask & CHIMERA_VFS_ATTR_MASK_STAT) {
        dest->va_dev   = src->va_dev;
        dest->va_ino   = src->va_ino;
        dest->va_mode  = src->va_mode;
        dest->va_nlink = src->va_nlink;
        dest->va_uid   = src->va_uid;
        dest->va_gid   = src->va_gid;
        dest->va_rdev  = src->va_rdev;
        dest->va_size  = src->va_size;
        dest->va_atime = src->va_atime;
        dest->va_mtime = src->va_mtime;
        dest->va_ctime = src->va_ctime;
    }

    if (src->va_mask & CHIMERA_VFS_ATTR_MASK_STATFS) {
        dest->va_space_avail = src->va_space_avail;
        dest->va_space_free  = src->va_space_free;
        dest->va_space_total = src->va_space_total;
        dest->va_space_used  = src->va_space_used;
    }
} /* chimera_vfs_copy_attr */
