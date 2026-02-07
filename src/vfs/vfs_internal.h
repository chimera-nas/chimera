// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdlib.h>
#include <time.h>
#include <unistd.h>
#include <pthread.h>
#include <xxhash.h>
#include <uthash.h>
#include <utlist.h>

#include "vfs/vfs.h"
#include "vfs/vfs_fh.h"
#include "vfs/vfs_mount_table.h"
#include "common/logging.h"
#include "common/misc.h"
#include "metrics/metrics.h"
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

/* ERR_PTR style error handling for request allocation */
#define CHIMERA_VFS_MAX_ERRNO 4095
#define CHIMERA_VFS_ERR_PTR(err)        ((void *) (long) (-(err)))
#define CHIMERA_VFS_PTR_ERR(ptr)        ((int) (-(long) (ptr)))
#define CHIMERA_VFS_IS_ERR(ptr)         ((unsigned long) (ptr) > (unsigned long) -CHIMERA_VFS_MAX_ERRNO)

/* Structure for readdir entries stored in bounce buffer */
struct chimera_vfs_readdir_entry {
    uint64_t                 inum;
    uint64_t                 cookie;
    uint32_t                 namelen;
    struct chimera_vfs_attrs attrs;
    /* Name follows immediately after this struct */
};

static inline uint64_t
chimera_vfs_hash(
    const void *data,
    int         len)
{
    /* Mask the MSB to ensure the result is non-negative when interpreted as
     * a signed 64-bit value.  NFS readdir cookies are derived from this hash
     * and the Linux kernel rejects negative loff_t values in nfs_llseek_dir(),
     * which breaks seekdir()/telldir() for cookies with bit 63 set. */
    return XXH3_64bits(data, len) & INT64_MAX;
} /* chimera_vfs_hash */

static inline struct chimera_vfs_find_result *
chimera_vfs_find_result_alloc(struct chimera_vfs_thread *thread)
{
    struct chimera_vfs_find_result *result;

    if (thread->free_find_results) {
        result = thread->free_find_results;
        LL_DELETE(thread->free_find_results, result);
    } else {
        result = calloc(1, sizeof(struct chimera_vfs_find_result));
    }

    return result;
} /* chimera_vfs_find_result_alloc */

static inline void
chimera_vfs_find_result_free(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_find_result *result)
{
    LL_PREPEND(thread->free_find_results, result);
} /* chimera_vfs_find_result_free */

static inline struct chimera_vfs_module *
chimera_vfs_get_module(
    struct chimera_vfs_thread *thread,
    const void                *fh,
    int                        fhlen)
{
    struct chimera_vfs        *vfs = thread->vfs;
    struct chimera_vfs_mount  *mount;
    struct chimera_vfs_module *module;

    if (fhlen < CHIMERA_VFS_MOUNT_ID_SIZE) {
        return NULL;
    }

    urcu_memb_read_lock();

    mount = chimera_vfs_mount_table_lookup(vfs->mount_table, fh);

    module = mount ? mount->module : NULL;

    urcu_memb_read_unlock();

    return module;
} /* chimera_vfs_get_module */

/*
 * Common request allocation helper with capability enforcement.
 * Returns ERR_PTR on failure:
 *   - CHIMERA_VFS_ESTALE if module is NULL
 *   - CHIMERA_VFS_ENOTSUP if module lacks required capability
 */
static inline struct chimera_vfs_request *
chimera_vfs_request_alloc_common(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    struct chimera_vfs_module     *module,
    const void                    *fh,
    int                            fhlen,
    uint64_t                       fh_hash,
    uint32_t                       required_cap)
{
    struct chimera_vfs_request *request;

    if (!module) {
        return CHIMERA_VFS_ERR_PTR(CHIMERA_VFS_ESTALE);
    }

    if (!(module->capabilities & required_cap)) {
        return CHIMERA_VFS_ERR_PTR(CHIMERA_VFS_ENOTSUP);
    }

    if (thread->free_requests) {
        request = thread->free_requests;
        LL_DELETE(thread->free_requests, request);
    } else {
        request              = calloc(1, sizeof(struct chimera_vfs_request));
        request->thread      = thread;
        request->plugin_data = malloc(4096);
    }
    request->status = CHIMERA_VFS_UNSET;
    request->cred   = cred;
    request->module = module;

    if (fh && fhlen > 0) {
        memcpy(request->fh, fh, fhlen);
    }
    request->fh_len      = fhlen;
    request->fh_hash     = fh_hash;
    request->active_prev = NULL;
    request->active_next = NULL;

    clock_gettime(CLOCK_MONOTONIC, &request->start_time);

    thread->num_active_requests++;
    DL_APPEND2(thread->active_requests, request, active_prev, active_next);

    return request;
} /* chimera_vfs_request_alloc_common */

static inline struct chimera_vfs_request *
chimera_vfs_request_alloc_by_hash(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    uint64_t                       fh_hash)
{
    struct chimera_vfs_module *module = chimera_vfs_get_module(thread, fh, fhlen);

    return chimera_vfs_request_alloc_common(thread, cred, module, fh, fhlen,
                                            fh_hash, CHIMERA_VFS_CAP_FS);
} /* chimera_vfs_request_alloc_by_hash */


static inline struct chimera_vfs_request *
chimera_vfs_request_alloc_anon(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    uint64_t                       fh_key)
{
    uint64_t fh_hash = chimera_vfs_hash(&fh_key, sizeof(fh_key));

    return chimera_vfs_request_alloc_by_hash(thread, cred, fh, fhlen, fh_hash);
} /* chimera_vfs_request_alloc_by_hash */

static inline struct chimera_vfs_request *
chimera_vfs_request_alloc(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen)
{
    uint64_t fh_hash = chimera_vfs_hash(fh, fhlen);

    return chimera_vfs_request_alloc_by_hash(thread, cred, fh, fhlen, fh_hash);
} /* chimera_vfs_request_alloc */

static inline struct chimera_vfs_request *
chimera_vfs_request_alloc_by_handle(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle)
{
    return chimera_vfs_request_alloc_by_hash(thread, cred, handle->fh, handle->fh_len, handle->fh_hash);
} /* chimera_vfs_request_alloc_by_handle */

/*
 * Allocate a request with a pre-determined module (no mount table lookup).
 * Use this when the module is already known, e.g., from an open handle.
 */
static inline struct chimera_vfs_request *
chimera_vfs_request_alloc_with_module(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    uint64_t                       fh_hash,
    struct chimera_vfs_module     *module)
{
    return chimera_vfs_request_alloc_common(thread, cred, module, fh, fhlen,
                                            fh_hash, CHIMERA_VFS_CAP_FS);
} /* chimera_vfs_request_alloc_with_module */

/*
 * Allocate a request for KV operations.
 * Uses the pre-configured kv_module instead of looking up by FH.
 * The key is hashed to determine the delegation thread for blocking modules.
 */
static inline struct chimera_vfs_request *
chimera_vfs_request_alloc_kv(
    struct chimera_vfs_thread *thread,
    const void                *key,
    uint32_t                   key_len)
{
    struct chimera_vfs *vfs      = thread->vfs;
    uint64_t            key_hash = chimera_vfs_hash(key, key_len);

    return chimera_vfs_request_alloc_common(thread, NULL, vfs->kv_module,
                                            NULL, 0, key_hash, CHIMERA_VFS_CAP_KV);
} /* chimera_vfs_request_alloc_kv */

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

    if (thread->metrics.op_latency_series) {
        prometheus_histogram_sample(thread->metrics.op_latency_series[request->opcode], request->elapsed_ns);
    }

    chimera_vfs_dump_reply(request);
} /* chimera_vfs_complete */

static inline void
chimera_vfs_request_free(
    struct chimera_vfs_thread  *thread,
    struct chimera_vfs_request *request)
{

#ifdef __clang_analyzer__
    chimera_vfs_abort_if(request->active_prev != request && request->active_next == NULL,
                         "clang static analysis thinks this can happen");
#endif /* ifdef __clang_analyzer__ */

    DL_DELETE2(thread->active_requests, request, active_prev, active_next);

    thread->num_active_requests--;

    LL_PREPEND(thread->free_requests, request);
} /* chimera_vfs_request_free */

static inline void
chimera_vfs_complete_delegate(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread *thread = request->thread;

    pthread_mutex_lock(&thread->lock);
    DL_APPEND(thread->pending_complete_requests, request);
    pthread_mutex_unlock(&thread->lock);

    evpl_ring_doorbell(&thread->doorbell);
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

    if (!module || !thread->module_private[module->fh_magic]) {
        request->status = CHIMERA_VFS_ESTALE;
        request->complete(request);
        return;
    }

    if (module->capabilities & CHIMERA_VFS_CAP_BLOCKING) {
        thread_id = request->fh_hash % vfs->num_delegation_threads;

        request->complete_delegate = request->complete;
        request->complete          = chimera_vfs_complete_delegate;

        delegation_thread = &vfs->delegation_threads[thread_id];

        pthread_mutex_lock(&delegation_thread->lock);
        DL_APPEND(delegation_thread->requests, request);
        pthread_mutex_unlock(&delegation_thread->lock);

        evpl_ring_doorbell(&delegation_thread->doorbell);

    } else {
        module->dispatch(request, thread->module_private[module->fh_magic]);
    }
} /* chimera_vfs_dispatch */

static inline void
chimera_vfs_copy_attr(
    struct chimera_vfs_attrs       *dest,
    const struct chimera_vfs_attrs *src)
{
    dest->va_req_mask = src->va_req_mask;
    dest->va_set_mask = src->va_set_mask;

    if (src->va_req_mask & CHIMERA_VFS_ATTR_FH) {
        memcpy(dest->va_fh, src->va_fh, src->va_fh_len);
        dest->va_fh_len = src->va_fh_len;
    }

    if (src->va_req_mask & CHIMERA_VFS_ATTR_MASK_STAT) {
        dest->va_dev        = src->va_dev;
        dest->va_ino        = src->va_ino;
        dest->va_mode       = src->va_mode;
        dest->va_nlink      = src->va_nlink;
        dest->va_uid        = src->va_uid;
        dest->va_gid        = src->va_gid;
        dest->va_rdev       = src->va_rdev;
        dest->va_size       = src->va_size;
        dest->va_space_used = src->va_space_used;
        dest->va_atime      = src->va_atime;
        dest->va_mtime      = src->va_mtime;
        dest->va_ctime      = src->va_ctime;
    }

    if (src->va_req_mask & CHIMERA_VFS_ATTR_MASK_STATFS) {
        dest->va_fs_space_avail = src->va_fs_space_avail;
        dest->va_fs_space_free  = src->va_fs_space_free;
        dest->va_fs_space_total = src->va_fs_space_total;
        dest->va_fs_space_used  = src->va_fs_space_used;
        dest->va_fs_files_total = src->va_fs_files_total;
        dest->va_fs_files_free  = src->va_fs_files_free;
        dest->va_fs_files_avail = src->va_fs_files_avail;
    }
} /* chimera_vfs_copy_attr */
