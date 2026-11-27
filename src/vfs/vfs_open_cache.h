// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "common/atomic.h"
#include "common/thread.h"

#include "common/format.h"
#include "common/misc.h"
#include "vfs.h"
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_unlinked.h"
#include "prometheus-c.h"

struct vfs_open_cache_shard {
    evpl_mutex_t                        lock;
    struct chimera_vfs_open_handle     *handles;
    struct chimera_vfs_open_handle     *pending_close;
    struct chimera_vfs_open_handle     *free_handles;
    /* Handles taken out of `handles` while still in use (see
     * chimera_vfs_open_cache_detach).  They are on no other list, so without
     * this one the handles of an unlinked file could not all be found. */
    struct chimera_vfs_open_handle     *detached;
    /* Records of the files unlinked while referenced; NULL for nearly every
     * shard nearly all of the time. */
    struct chimera_vfs_unlinked        *unlinked;
    uint8_t                             cache_id;
    uint32_t                            max_open_files;
    uint32_t                            open_handles;
    struct prometheus_counter_instance *acquire;
    struct prometheus_counter_instance *insert;
    struct prometheus_gauge_instance   *num_handles;
};

struct vfs_open_cache {
    unsigned int                      num_shards;
    unsigned int                      shard_mask;
    struct vfs_open_cache_shard      *shards;
    struct prometheus_metrics        *metrics;
    struct prometheus_counter        *open_cache;
    struct prometheus_counter_series *open_cache_acquire;
    struct prometheus_counter_series *open_cache_insert;
    struct chimera_vfs_unlinked_ctl   unlinked;
};

#define CHIMERA_VFS_OPEN_CACHE_NO_BACKEND_OPEN (UINT64_MAX - 1)

static inline int
chimera_vfs_open_handle_needs_backend_close(const struct chimera_vfs_open_handle *handle)
{
    return !(handle->flags & CHIMERA_VFS_OPEN_HANDLE_NO_BACKEND_OPEN);
} // chimera_vfs_open_handle_needs_backend_close

/*
 * A close is always issued after the shard lock is dropped, and by then the
 * handle it applies to has usually been freed -- so everything the close needs
 * is copied out first.  The file handle in particular has to be copied rather
 * than pointed at, which is why this is a struct and not a pointer.
 */
struct chimera_vfs_close_ref {
    struct chimera_vfs_module *module;
    uint64_t                   vfs_private;
    uint64_t                   fh_hash;
    int                        fh_len;
    int                        needs_close;
    uint8_t                    fh[CHIMERA_VFS_FH_SIZE + 16];
};

static inline void
chimera_vfs_close_ref_capture(
    struct chimera_vfs_close_ref         *ref,
    const struct chimera_vfs_open_handle *handle)
{
    ref->module      = handle->vfs_module;
    ref->vfs_private = handle->vfs_private;
    ref->fh_hash     = handle->fh_hash;
    ref->fh_len      = handle->fh_len;
    ref->needs_close = chimera_vfs_open_handle_needs_backend_close(handle);
    memcpy(ref->fh, handle->fh, handle->fh_len);
} /* chimera_vfs_close_ref_capture */

/* No-op when the handle never had a backend open (synthesized handles), so
 * callers do not repeat that test. */
static inline void
chimera_vfs_close_ref_dispatch(
    struct chimera_vfs_thread          *thread,
    const struct chimera_vfs_close_ref *ref,
    chimera_vfs_close_callback_t        callback,
    void                               *private_data)
{
    if (!ref->needs_close) {
        return;
    }

    chimera_vfs_close(thread, ref->module, ref->fh, ref->fh_len,
                      ref->vfs_private, ref->fh_hash, callback, private_data);
} /* chimera_vfs_close_ref_dispatch */

/* --- Unlinked files (see vfs_unlinked.h) --- */

/*
 * One unit of work for the close thread.  A handle of an unlinked file that
 * leaves the cache is not closed where it leaves; everything the close needs
 * is copied here (the handle struct is recycled at once) and the close thread
 * takes it from here.  The struct is malloc'd: handles of unlinked files are
 * rare and the close thread, not the hot path, frees it.
 */
enum chimera_vfs_unlinked_work_kind {
    /* A handle of the file left the cache: take it out of the count, close it,
     * and purge the file if it was the last. */
    CHIMERA_VFS_UNLINKED_WORK_HANDLE,
    /* The file has no strong reference left: collect the holders that have not
     * been called and call them. */
    CHIMERA_VFS_UNLINKED_WORK_RECALL,
    /* Holders already collected (umount): just call them. */
    CHIMERA_VFS_UNLINKED_WORK_CALLS,
};

struct chimera_vfs_unlinked_work {
    struct chimera_vfs_unlinked_work *next;
    struct chimera_vfs_unlinked      *rec;
    uint8_t                           kind;
    uint8_t                           reason;
    struct chimera_vfs_recall_call   *calls;
    uint32_t                          ncalls;
    struct chimera_vfs_close_ref      close_ref;
    /* Counted in the close fence umount waits on (the handle left the cache
     * through the idle sweep or a umount). */
    uint8_t                           fenced;
    /* The last handle was a pending open that failed, so there is no backend
     * open to close and the handle count was already settled. */
    uint8_t                           carrierless;
    /* Set by the close thread: this handle was the file's last, so its close
     * is the purge.  And the thread that owns the item. */
    uint8_t                           last;
    struct chimera_vfs_close_thread  *close_thread;
    /* Counted in the close thread's in-flight total until finished. */
    uint8_t                           pending;
    /* How the purge ended, for the record's final count. */
    uint8_t                           failed;
    uint8_t                           relinked;
    uint8_t                           leaked;
    /* The purge's own state: root for the link recheck, a stand-in for the
     * carrier handle (whose struct is gone) to ask it through, and the data
     * server's root directory while the backing file is being deleted. */
    struct chimera_vfs_cred           root;
    struct chimera_vfs_open_handle    probe;
    struct chimera_vfs_open_handle   *ds_root;
};

static inline struct chimera_vfs_unlinked *
chimera_vfs_unlinked_find(
    const struct vfs_open_cache_shard *shard,
    const uint8_t                     *fh,
    uint32_t                           fh_len)
{
    struct chimera_vfs_unlinked *rec;

    for (rec = shard->unlinked; rec; rec = rec->next) {
        if (rec->fh_len == fh_len && memcmp(rec->fh, fh, fh_len) == 0) {
            return rec;
        }
    }

    return NULL;
} /* chimera_vfs_unlinked_find */

/* Caller holds the shard lock. */
static inline void
chimera_vfs_unlinked_queue(
    struct vfs_open_cache            *cache,
    struct chimera_vfs_unlinked_work *work)
{
    struct chimera_vfs_unlinked_ctl *ctl = &cache->unlinked;

    work->next = NULL;

    evpl_mutex_lock(&ctl->lock);
    if (ctl->work_tail) {
        ctl->work_tail->next = work;
    } else {
        ctl->work_head = work;
    }
    ctl->work_tail = work;
    /* Ring under the lock, as the other cross-thread wakeups do. */
    if (ctl->doorbell) {
        evpl_ring_doorbell(ctl->doorbell);
    }
    evpl_mutex_unlock(&ctl->lock);
} /* chimera_vfs_unlinked_queue */

/*
 * A handle of an unlinked file is leaving the cache.  Hand its close to the
 * close thread.  The caller still removes the handle from the lists it is on
 * and frees the struct.  Caller holds the shard lock.
 */
static inline void
chimera_vfs_unlinked_route(
    struct vfs_open_cache          *cache,
    struct vfs_open_cache_shard    *shard,
    struct chimera_vfs_open_handle *handle,
    int                             fenced)
{
    struct chimera_vfs_unlinked_work *work;
    struct chimera_vfs_unlinked      *rec;

    rec = chimera_vfs_unlinked_find(shard, handle->fh, handle->fh_len);

    chimera_vfs_abort_if(!rec, "handle flagged unlinked has no record");

    work = calloc(1, sizeof(*work));
    chimera_vfs_close_ref_capture(&work->close_ref, handle);
    work->rec    = rec;
    work->fenced = fenced;

    if (fenced && cache->unlinked.fence_issued) {
        chimera_atomic_add_fetch(cache->unlinked.fence_issued, 1, CHIMERA_MEMORY_RELEASE);
    }

    chimera_vfs_unlinked_queue(cache, work);
} /* chimera_vfs_unlinked_route */

/* A new handle for a file that has a record joins its counts.  A file whose
 * purge is already running is left alone: the handle is then an ordinary one
 * and the open cache never hands out an open for it (see acquire). */
static inline void
chimera_vfs_unlinked_adopt(
    struct vfs_open_cache_shard    *shard,
    struct chimera_vfs_open_handle *handle)
{
    struct chimera_vfs_unlinked *rec;

    rec = chimera_vfs_unlinked_find(shard, handle->fh, handle->fh_len);

    if (rec && rec->state == CHIMERA_VFS_UNLINKED_TRACKED) {
        handle->flags |= CHIMERA_VFS_OPEN_HANDLE_UNLINKED;
        rec->handles++;
        rec->strong++;
    }
} /* chimera_vfs_unlinked_adopt */

static inline void
chimera_vfs_unlinked_ref_inc(
    struct vfs_open_cache_shard    *shard,
    struct chimera_vfs_open_handle *handle)
{
    struct chimera_vfs_unlinked *rec;

    rec = chimera_vfs_unlinked_find(shard, handle->fh, handle->fh_len);

    chimera_vfs_abort_if(!rec, "handle flagged unlinked has no record");

    rec->strong++;
} /* chimera_vfs_unlinked_ref_inc */

/*
 * The file has no real open left and some holder has not yet been asked to give
 * its reference back: have the close thread ask.  The release path only queues;
 * the callbacks run on the close thread, outside every lock.  Caller holds the
 * shard lock.
 */
static inline void
chimera_vfs_unlinked_queue_recall(
    struct vfs_open_cache       *cache,
    struct chimera_vfs_unlinked *rec)
{
    struct chimera_vfs_unlinked_work *work;

    if (rec->state != CHIMERA_VFS_UNLINKED_TRACKED || rec->strong != 0 ||
        rec->unfired == 0 || rec->fire_queued) {
        return;
    }

    rec->fire_queued = 1;

    work         = calloc(1, sizeof(*work));
    work->kind   = CHIMERA_VFS_UNLINKED_WORK_RECALL;
    work->reason = CHIMERA_VFS_RECALL_UNLINKED;
    work->rec    = rec;
    chimera_vfs_unlinked_queue(cache, work);
} /* chimera_vfs_unlinked_queue_recall */

static inline void
chimera_vfs_unlinked_ref_dec(
    struct vfs_open_cache          *cache,
    struct vfs_open_cache_shard    *shard,
    struct chimera_vfs_open_handle *handle)
{
    struct chimera_vfs_unlinked *rec;

    rec = chimera_vfs_unlinked_find(shard, handle->fh, handle->fh_len);

    chimera_vfs_abort_if(!rec, "handle flagged unlinked has no record");
    chimera_vfs_abort_if(rec->strong == 0, "unlinked file strong count underflow");

    rec->strong--;

    chimera_vfs_unlinked_queue_recall(cache, rec);
} /* chimera_vfs_unlinked_ref_dec */

/*
 * A pending open of an unlinked file failed.  The handle is freed without a
 * backend open, and every reference it carried (the opener's and any request
 * parked behind it) goes with it.  If it was the file's last handle there is
 * no carrier left for the purge, so queue the purge without one.
 */
static inline void
chimera_vfs_unlinked_failed_open(
    struct vfs_open_cache          *cache,
    struct vfs_open_cache_shard    *shard,
    struct chimera_vfs_open_handle *handle)
{
    struct chimera_vfs_unlinked      *rec;
    struct chimera_vfs_unlinked_work *work;

    rec = chimera_vfs_unlinked_find(shard, handle->fh, handle->fh_len);

    chimera_vfs_abort_if(!rec, "handle flagged unlinked has no record");
    chimera_vfs_abort_if(rec->strong < handle->opencnt, "unlinked file strong count underflow");
    chimera_vfs_abort_if(rec->handles == 0, "unlinked file handle count underflow");

    rec->strong -= handle->opencnt;
    rec->handles--;

    if (rec->handles == 0) {
        rec->state = CHIMERA_VFS_UNLINKED_PURGING;

        work              = calloc(1, sizeof(*work));
        work->rec         = rec;
        work->carrierless = 1;
        chimera_vfs_unlinked_queue(cache, work);
    } else {
        chimera_vfs_unlinked_queue_recall(cache, rec);
    }
} /* chimera_vfs_unlinked_failed_open */

/* --- Shard linked-list helpers --- */

static inline void
chimera_vfs_open_cache_shard_insert(
    struct vfs_open_cache_shard    *shard,
    struct chimera_vfs_open_handle *handle)
{
    handle->bucket_prev = NULL;
    handle->bucket_next = shard->handles;
    if (shard->handles) {
        shard->handles->bucket_prev = handle;
    }
    shard->handles = handle;
} // chimera_vfs_open_cache_shard_insert

static inline void
chimera_vfs_open_cache_shard_remove(
    struct vfs_open_cache_shard    *shard,
    struct chimera_vfs_open_handle *handle)
{
    if (handle->bucket_prev) {
        handle->bucket_prev->bucket_next = handle->bucket_next;
    } else {
        shard->handles = handle->bucket_next;
    }
    if (handle->bucket_next) {
        handle->bucket_next->bucket_prev = handle->bucket_prev;
    }
    handle->bucket_next = NULL;
    handle->bucket_prev = NULL;
} // chimera_vfs_open_cache_shard_remove

/* Take a handle that is still in use out of the cache's lookup list.  Its
 * holders keep working; it closes when the last one releases it.  Caller holds
 * the shard lock. */
static inline void
chimera_vfs_open_cache_detach(
    struct vfs_open_cache_shard    *shard,
    struct chimera_vfs_open_handle *handle)
{
    chimera_vfs_open_cache_shard_remove(shard, handle);
    handle->flags |= CHIMERA_VFS_OPEN_HANDLE_DETACHED;
    DL_APPEND(shard->detached, handle);
} /* chimera_vfs_open_cache_detach */

/*
 * Find a handle in the shard matching fh, access_mode, and credential identity.
 *
 * The cache is keyed by cred_hash so each caller gets its own handle (and its
 * own authorization result).  Within one identity, a RW handle can satisfy
 * any access mode; RO and WO handles can only satisfy their own mode.
 */
static inline struct chimera_vfs_open_handle *
chimera_vfs_open_cache_shard_find(
    struct vfs_open_cache_shard *shard,
    const void                  *fh,
    uint32_t                     fhlen,
    uint8_t                      access_mode,
    uint64_t                     cred_hash)
{
    struct chimera_vfs_open_handle *h;

    for (h = shard->handles; h; h = h->bucket_next) {
        if (h->fh_len == fhlen && h->cred_hash == cred_hash &&
            memcmp(h->fh, fh, fhlen) == 0) {
            if (h->access_mode == CHIMERA_VFS_ACCESS_MODE_RW ||
                h->access_mode == access_mode) {
                return h;
            }
        }
    }
    return NULL;
} /* chimera_vfs_open_cache_shard_find */

/* Derive access mode from open flags */
static inline uint8_t
chimera_vfs_open_access_mode(unsigned int open_flags)
{
    /* Keep write-only handles separate: proxy backends open real protocol
     * state with these rights, so a write-only handle cannot serve a reader. */
    if ((open_flags & CHIMERA_VFS_OPEN_READ_ONLY) &&
        !(open_flags & CHIMERA_VFS_OPEN_WRITE_ONLY)) {
        return CHIMERA_VFS_ACCESS_MODE_RO;
    }
    if ((open_flags & CHIMERA_VFS_OPEN_WRITE_ONLY) &&
        !(open_flags & CHIMERA_VFS_OPEN_READ_ONLY)) {
        return CHIMERA_VFS_ACCESS_MODE_WO;
    }
    return CHIMERA_VFS_ACCESS_MODE_RW;
} // chimera_vfs_open_access_mode

/* --- Init / Destroy --- */

static inline struct vfs_open_cache *
chimera_vfs_open_cache_init(
    uint8_t                    cache_id,
    int                        num_shard_bits,
    int                        max_open_files,
    struct prometheus_metrics *metrics,
    const char                *cache_name)
{
    struct vfs_open_cache *cache;
    int                    num_shard_bits_actual;
    int                    max_per_shard;

    cache = calloc(1, sizeof(*cache));

    num_shard_bits_actual = num_shard_bits + 4;
    cache->num_shards     = 1 << num_shard_bits_actual;
    cache->shard_mask     = cache->num_shards - 1;

    max_per_shard = max_open_files / cache->num_shards;
    if (max_per_shard < 4) {
        max_per_shard = 4;
    }

    cache->shards = calloc(cache->num_shards, sizeof(*cache->shards));

    /* Both caches carry the inbox for symmetry, but only the file cache ever
     * holds records, so only it registers the metrics. */
    chimera_vfs_unlinked_ctl_init(&cache->unlinked,
                                  cache_id == CHIMERA_VFS_OPEN_ID_FILE ? metrics : NULL);

    if (metrics) {
        cache->metrics    = metrics;
        cache->open_cache = prometheus_metrics_create_counter(metrics, "chimera_vfs_open_cache",
                                                              "Chimera VFS open cache operations");
        cache->open_cache_insert = prometheus_counter_create_series(cache->open_cache,
                                                                    (const char *[]) { "name", "op" },
                                                                    (const char *[]) { cache_name, "insert" }, 2);
        cache->open_cache_acquire = prometheus_counter_create_series(cache->open_cache,
                                                                     (const char *[]) { "name", "op" },
                                                                     (const char *[]) { cache_name, "acquire" }, 2);
    }

    for (unsigned int i = 0; i < cache->num_shards; i++) {
        evpl_mutex_init(&cache->shards[i].lock, NULL);
        cache->shards[i].handles        = NULL;
        cache->shards[i].free_handles   = NULL;
        cache->shards[i].pending_close  = NULL;
        cache->shards[i].max_open_files = max_per_shard;
        cache->shards[i].cache_id       = cache_id;

        if (metrics) {
            cache->shards[i].insert  = prometheus_counter_series_create_instance(cache->open_cache_insert);
            cache->shards[i].acquire = prometheus_counter_series_create_instance(cache->open_cache_acquire);
        }
    }

    return cache;
} /* vfs_open_cache_init */

static inline void
chimera_vfs_open_cache_destroy(struct vfs_open_cache *cache)
{
    struct vfs_open_cache_shard    *shard;
    struct chimera_vfs_open_handle *handle, *tmp;

    chimera_vfs_unlinked_cache_destroy(cache);

    for (unsigned int i = 0; i < cache->num_shards; i++) {
        shard = &cache->shards[i];

        /* Free any handles remaining on the free list */
        handle = shard->free_handles;
        while (handle) {
            tmp = handle->next;
            free(handle);
            handle = tmp;
        }

        evpl_mutex_destroy(&shard->lock);
    }

    chimera_vfs_unlinked_ctl_destroy(&cache->unlinked);

    if (cache->metrics) {
        prometheus_counter_destroy_series(cache->open_cache, cache->open_cache_acquire);
        prometheus_counter_destroy_series(cache->open_cache, cache->open_cache_insert);
        prometheus_counter_destroy(cache->metrics, cache->open_cache);
    }

    free(cache->shards);
    free(cache);
} /* vfs_open_cache_destroy */

/* --- Handle alloc / free --- */

static inline struct chimera_vfs_open_handle *
chimera_vfs_open_cache_alloc(struct vfs_open_cache_shard *shard)
{
    struct chimera_vfs_open_handle *handle;

    handle = shard->free_handles;
    if (handle) {
        LL_DELETE(shard->free_handles, handle);
    } else {
        handle           = calloc(1, sizeof(*handle));
        handle->cache_id = shard->cache_id;
    }

    return handle;
} /* chimera_vfs_open_cache_alloc */

static inline void
chimera_vfs_open_cache_free(
    struct vfs_open_cache_shard    *shard,
    struct chimera_vfs_open_handle *handle)
{
    /* Drop the per-file lease-state reference the handle anchored, if any (see
     * chimera_vfs_open_handle::file_state).  Safe under the shard lock:
     * chimera_vfs_state_put takes a vfs_state bucket lock and no path takes a
     * shard lock while holding a bucket lock, so there is no ordering cycle.
     * NULL the field afterwards because the free list is reused without memset,
     * so a recycled handle struct must not carry a stale pointer. */
    if (handle->file_state) {
        chimera_vfs_file_state_release(handle->file_state);
        handle->file_state = NULL;
    }
    LL_PREPEND(shard->free_handles, handle);
} /* chimera_vfs_open_cache_free */

/* --- Blocked request handling --- */

static inline void
chimera_vfs_open_cache_release_blocked(
    struct chimera_vfs_thread  *thread,
    struct chimera_vfs_request *requests,
    enum chimera_vfs_error      error_code)
{
    struct chimera_vfs_thread  *request_thread;
    struct chimera_vfs_request *request;


    while (requests) {

        request = requests;

        LL_DELETE(requests, request);

        request->status = error_code;

        if (error_code) {
            request->pending_handle = NULL;
        }

        request_thread = request->thread;

        if (request_thread == thread) {
            /* This is a request from the same thread, so we can dispatch it immediately */
            request->unblock_callback(request, request->pending_handle);
        } else {
            /* This is a request from a different thread, so we need to send it home */
            /* Wake it under the lock; see chimera_vfs_complete_delegate for
             * why the ring must not follow the unlock. */
            evpl_mutex_lock(&request_thread->lock);
            LL_PREPEND(request_thread->unblocked_requests, request);
            evpl_ring_doorbell(&request_thread->doorbell);
            evpl_mutex_unlock(&request_thread->lock);
        }
    }

} /* chimera_vfs_open_cache_release_blocked */

/* --- Release --- */

static inline void
chimera_vfs_open_cache_release(
    struct chimera_vfs_thread      *thread,
    struct vfs_open_cache          *cache,
    struct chimera_vfs_open_handle *handle,
    enum chimera_vfs_error          error_code)
{
    struct vfs_open_cache_shard *shard;
    struct chimera_vfs_request  *requests;

    shard = &cache->shards[handle->fh_hash & cache->shard_mask];

    chimera_vfs_abort_if(handle->cache_id != shard->cache_id, "handle released by wrong cache");

    evpl_mutex_lock(&shard->lock);

    handle->flags &= ~CHIMERA_VFS_OPEN_HANDLE_EXCLUSIVE;

    requests                 = handle->blocked_requests;
    handle->blocked_requests = NULL;

    if (error_code) {
        /* We failed to open the file, so we need to remove it from the cache
         * and we'll give everyone else NULL callbacks so they'll never
         * release themselves.  Therefore we can free the handle now
         */
        if (unlikely(handle->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED)) {
            chimera_vfs_unlinked_failed_open(cache, shard, handle);
        }
        handle->opencnt = 0;
        if (!(handle->flags & CHIMERA_VFS_OPEN_HANDLE_DETACHED)) {
            chimera_vfs_open_cache_shard_remove(shard, handle);
            shard->open_handles--;
        } else {
            DL_DELETE(shard->detached, handle);
        }
        chimera_vfs_open_cache_free(shard, handle);

    } else {

        handle->opencnt--;

        chimera_vfs_abort_if(handle->opencnt < handle->recallcnt,
                             "plain release of a handle whose remaining references are all recallable");

        if (unlikely(handle->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED)) {
            chimera_vfs_unlinked_ref_dec(cache, shard, handle);
        }

        if (handle->opencnt == 0) {
            if (handle->flags & CHIMERA_VFS_OPEN_HANDLE_DETACHED) {
                /* Detached handle: close immediately, do not add to pending_close.
                 * Do not decrement open_handles - the handle was already removed
                 * from the cache when it was detached by insert(). */
                struct chimera_vfs_close_ref close_ref;
                int                          routed;

                DL_DELETE(shard->detached, handle);

                /* The close thread closes it, and counts it out of its file's
                * record on the way: the purge of an unlinked file is decided
                * by whoever takes the last handle, and that is never here. */
                routed = !!(handle->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED);

                if (routed) {
                    chimera_vfs_unlinked_route(cache, shard, handle, 0);
                } else {
                    chimera_vfs_close_ref_capture(&close_ref, handle);
                }
                chimera_vfs_open_cache_free(shard, handle);
                evpl_mutex_unlock(&shard->lock);
                chimera_vfs_open_cache_release_blocked(thread, requests, error_code);
                if (!routed) {
                    chimera_vfs_close_ref_dispatch(thread, &close_ref, NULL, NULL);
                }
                return;
            }
            handle->timestamp = chimera_vfs_now_ticks();
            DL_APPEND(shard->pending_close, handle);
        }
    }

    evpl_mutex_unlock(&shard->lock);

    chimera_vfs_open_cache_release_blocked(thread, requests, error_code);

} /* vfs_open_cache_release */

/* Increment opencnt on an already-acquired handle (for dup operations) */
static inline void
chimera_vfs_open_cache_dup(
    struct vfs_open_cache          *cache,
    struct chimera_vfs_open_handle *handle)
{
    struct vfs_open_cache_shard *shard;

    shard = &cache->shards[handle->fh_hash & cache->shard_mask];

    chimera_vfs_abort_if(handle->cache_id != shard->cache_id, "handle duped by wrong cache");

    evpl_mutex_lock(&shard->lock);

    chimera_vfs_abort_if(handle->opencnt == 0, "dup on handle with zero opencnt");

    handle->opencnt++;

    if (unlikely(handle->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED)) {
        chimera_vfs_unlinked_ref_inc(shard, handle);
    }

    evpl_mutex_unlock(&shard->lock);

} /* chimera_vfs_open_cache_dup */

/* --- Evict by file handle --- */

/*
 * Evict the cached open handle for (fh, fhlen), closing it.  Intended for a
 * backend that learns out-of-band that a cached handle must be relinquished --
 * e.g. an SMB lease break, where the server is reclaiming a handle the client
 * still caches and it must be closed so a conflicting operation can proceed.
 *
 * An idle handle (no current holders) is removed and closed immediately,
 * mirroring the pending-close eviction in insert()/defer_close().  A handle
 * still in use is detached: current holders keep working, but it is removed from
 * the cache (never handed out again) and closes when the last holder releases.
 *
 * Shard-agnostic (scans every shard for the fh) so it does not depend on how the
 * handle's fh_hash was derived; a file handle maps to a single cached handle, so
 * the scan stops at the first match.  Called off the hot path (on a break), so
 * the full scan is acceptable.
 */
static inline void
chimera_vfs_open_cache_evict(
    struct chimera_vfs_thread *thread,
    struct vfs_open_cache     *cache,
    const void                *fh,
    uint32_t                   fhlen)
{
    uint32_t s;

    for (s = 0; s <= cache->shard_mask; s++) {
        struct vfs_open_cache_shard    *shard = &cache->shards[s];
        struct chimera_vfs_open_handle *handle;

        evpl_mutex_lock(&shard->lock);

        for (handle = shard->handles; handle; handle = handle->bucket_next) {
            if (handle->fh_len == fhlen && memcmp(handle->fh, fh, fhlen) == 0) {
                break;
            }
        }

        if (!handle) {
            evpl_mutex_unlock(&shard->lock);
            continue;
        }

        if (handle->opencnt == 0) {
            /* Idle: remove from both lists and close (outside the lock). */
            struct chimera_vfs_close_ref close_ref;
            int                          routed;

            routed = !!(handle->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED);

            if (routed) {
                chimera_vfs_unlinked_route(cache, shard, handle, 0);
            } else {
                chimera_vfs_close_ref_capture(&close_ref, handle);
            }

            DL_DELETE(shard->pending_close, handle);
            chimera_vfs_open_cache_shard_remove(shard, handle);
            shard->open_handles--;
            chimera_vfs_open_cache_free(shard, handle);

            evpl_mutex_unlock(&shard->lock);
            if (!routed) {
                chimera_vfs_close_ref_dispatch(thread, &close_ref, NULL, NULL);
            }
        } else {
            /* In use: detach now, close on the final release. */
            shard->open_handles--;
            chimera_vfs_open_cache_detach(shard, handle);
            evpl_mutex_unlock(&shard->lock);
        }

        return;   /* an fh maps to a single cached handle */
    }
} /* chimera_vfs_open_cache_evict */

/* --- Close callback for eviction --- */

static void
chimera_vfs_open_cache_close_callback(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_vfs_open_handle *handle  = private_data;
    struct chimera_vfs_request     *request = handle->request;

    chimera_vfs_abort_if(error_code, "open cache failed to close existing handle");

    request->status = error_code;

    handle->callback(request, handle);
} /* chimera_vfs_open_cache_close_callback */

/* --- Populate (after pending open completes) --- */

static inline void
chimera_vfs_open_cache_populate(
    struct chimera_vfs_thread      *thread,
    struct vfs_open_cache          *cache,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        vfs_private_data)
{
    struct vfs_open_cache_shard *shard;
    struct chimera_vfs_request  *requests = NULL;

    shard = &cache->shards[handle->fh_hash & cache->shard_mask];

    evpl_mutex_lock(&shard->lock);

    handle->vfs_private = vfs_private_data;
    handle->flags      &= ~CHIMERA_VFS_OPEN_HANDLE_PENDING;

    if (!(handle->flags & CHIMERA_VFS_OPEN_HANDLE_EXCLUSIVE)) {
        requests                 = handle->blocked_requests;
        handle->blocked_requests = NULL;
    }

    evpl_mutex_unlock(&shard->lock);

    if (requests) {
        chimera_vfs_open_cache_release_blocked(thread, requests, 0);
    }

} /* chimera_vfs_open_cache_populate */

/* --- Acquire (cache-first, used by open-by-fh) --- */

static inline void
chimera_vfs_open_cache_acquire(
    struct chimera_vfs_thread  *thread,
    struct vfs_open_cache      *cache,
    struct chimera_vfs_module  *module,
    struct chimera_vfs_request *request,
    const void                 *fh,
    uint32_t                    fhlen,
    uint64_t                    fh_hash,
    uint64_t                    vfs_private_data,
    unsigned int                open_flags,
    int                         exclusive,
    void (                     *callback )(
        struct chimera_vfs_request     *request,
        struct chimera_vfs_open_handle *handle))
{
    struct vfs_open_cache_shard    *shard;
    struct chimera_vfs_open_handle *handle, *existing;
    uint8_t                         access_mode;
    uint64_t                        cred_hash;
    int                             done = 0;

    access_mode = chimera_vfs_open_access_mode(open_flags);
    cred_hash   = chimera_vfs_cred_hash(request->cred);

    shard = &cache->shards[fh_hash & cache->shard_mask];

    evpl_mutex_lock(&shard->lock);

    if (unlikely(shard->unlinked != NULL)) {
        struct chimera_vfs_unlinked *rec = chimera_vfs_unlinked_find(shard, fh, fhlen);

        if (rec && rec->state == CHIMERA_VFS_UNLINKED_PURGING) {
            /* The last handle is gone and the data is about to be: nothing may
             * bring the file back by its handle any more. */
            evpl_mutex_unlock(&shard->lock);
            request->status = CHIMERA_VFS_ESTALE;
            callback(request, NULL);
            return;
        }
    }

    handle = chimera_vfs_open_cache_shard_find(shard, fh, fhlen, access_mode, cred_hash);

    if (handle) {

        chimera_vfs_abort_if((handle->flags & CHIMERA_VFS_OPEN_HANDLE_PENDING) && vfs_private_data != UINT64_MAX,
                             "open cache pending handle with vfs private data");

        if (handle->opencnt == 0) {
            DL_DELETE(shard->pending_close, handle);
            handle->doc_delete_on_close = 0;
        }

        handle->opencnt++;

        if (unlikely(handle->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED)) {
            chimera_vfs_unlinked_ref_inc(shard, handle);
        }

        if (handle->flags & (CHIMERA_VFS_OPEN_HANDLE_EXCLUSIVE | CHIMERA_VFS_OPEN_HANDLE_PENDING)) {
            request->unblock_callback = callback;
            request->pending_handle   = handle;
            LL_PREPEND(handle->blocked_requests, request);
            done = 0;
        } else {
            done = 1;
        }
    } else {

        prometheus_counter_increment(shard->insert);

        handle                      = chimera_vfs_open_cache_alloc(shard);
        handle->vfs_module          = module;
        handle->fh_hash             = fh_hash;
        handle->fh_len              = fhlen;
        handle->cred_hash           = cred_hash;
        handle->granted_valid       = 0;
        handle->granted_bound       = 0;
        handle->opencnt             = 1;
        handle->recallcnt           = 0;
        handle->recallables         = NULL;
        handle->access_mode         = access_mode;
        handle->flags               = exclusive ? CHIMERA_VFS_OPEN_HANDLE_EXCLUSIVE : 0;
        handle->callback            = callback;
        handle->request             = request;
        handle->vfs_private         = vfs_private_data;
        handle->blocked_requests    = NULL;
        handle->doc_delete_on_close = 0;

        if (handle->vfs_private == CHIMERA_VFS_OPEN_CACHE_NO_BACKEND_OPEN) {
            handle->flags      |= CHIMERA_VFS_OPEN_HANDLE_NO_BACKEND_OPEN;
            handle->vfs_private = 0;
        }

        if (handle->vfs_private == UINT64_MAX) {
            handle->flags |= CHIMERA_VFS_OPEN_HANDLE_PENDING;
        }

        memcpy(handle->fh, fh, fhlen);

        if (unlikely(shard->unlinked != NULL)) {
            chimera_vfs_unlinked_adopt(shard, handle);
        }

        chimera_vfs_open_cache_shard_insert(shard, handle);

        if (shard->open_handles < shard->max_open_files) {
            shard->open_handles++;
            done = 1;
        } else if (shard->pending_close) {
            existing = shard->pending_close;

            DL_DELETE(shard->pending_close, existing);
            chimera_vfs_open_cache_shard_remove(shard, existing);

            struct chimera_vfs_close_ref close_ref;

            if (unlikely(existing->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED)) {
                /* The close thread closes it.  The open this victim made room
                 * for need not wait for that, so go on at once. */
                chimera_vfs_unlinked_route(cache, shard, existing, 0);
                prometheus_counter_increment(shard->acquire);
                chimera_vfs_open_cache_free(shard, existing);
                evpl_mutex_unlock(&shard->lock);
                chimera_vfs_open_cache_close_callback(CHIMERA_VFS_OK, handle);
                return;
            }

            chimera_vfs_close_ref_capture(&close_ref, existing);

            prometheus_counter_increment(shard->acquire);

            chimera_vfs_open_cache_free(shard, existing);
            evpl_mutex_unlock(&shard->lock);

            if (close_ref.needs_close) {
                chimera_vfs_close_ref_dispatch(thread, &close_ref,
                                               chimera_vfs_open_cache_close_callback, handle);
            } else {
                chimera_vfs_open_cache_close_callback(CHIMERA_VFS_OK, handle);
            }

            return;
        } else {
            /* All handles are active; just exceed the limit */
            shard->open_handles++;
            done = 1;
        }

    }

    prometheus_counter_increment(shard->acquire);

    evpl_mutex_unlock(&shard->lock);

    if (done) {
        callback(request, handle);
    }
} /* chimera_vfs_open_cache_acquire */

/* --- Insert (always-insert, used by open_at / create_unlinked) --- */

static inline void
chimera_vfs_open_cache_insert(
    struct chimera_vfs_thread  *thread,
    struct vfs_open_cache      *cache,
    struct chimera_vfs_module  *module,
    struct chimera_vfs_request *request,
    const void                 *fh,
    uint32_t                    fhlen,
    uint64_t                    fh_hash,
    uint64_t                    vfs_private_data,
    unsigned int                open_flags,
    void (                     *callback )(
        struct chimera_vfs_request     *request,
        struct chimera_vfs_open_handle *handle))
{
    struct vfs_open_cache_shard    *shard;
    struct chimera_vfs_open_handle *handle, *existing;
    uint8_t                         access_mode;
    uint64_t                        cred_hash;

    access_mode = chimera_vfs_open_access_mode(open_flags);
    cred_hash   = chimera_vfs_cred_hash(request->cred);

    shard = &cache->shards[fh_hash & cache->shard_mask];

    evpl_mutex_lock(&shard->lock);

    prometheus_counter_increment(shard->insert);

    /* Allocate and populate the new handle */
    handle                      = chimera_vfs_open_cache_alloc(shard);
    handle->vfs_module          = module;
    handle->fh_hash             = fh_hash;
    handle->fh_len              = fhlen;
    handle->cred_hash           = cred_hash;
    handle->granted_valid       = 0;
    handle->granted_bound       = 0;
    handle->opencnt             = 1;
    handle->recallcnt           = 0;
    handle->recallables         = NULL;
    handle->access_mode         = access_mode;
    handle->flags               = 0;
    handle->callback            = callback;
    handle->request             = request;
    handle->vfs_private         = vfs_private_data;
    handle->blocked_requests    = NULL;
    handle->doc_delete_on_close = 0;

    if (handle->vfs_private == CHIMERA_VFS_OPEN_CACHE_NO_BACKEND_OPEN) {
        handle->flags      |= CHIMERA_VFS_OPEN_HANDLE_NO_BACKEND_OPEN;
        handle->vfs_private = 0;
    }

    memcpy(handle->fh, fh, fhlen);

    if (unlikely(shard->unlinked != NULL)) {
        chimera_vfs_unlinked_adopt(shard, handle);
    }

    /* Check for existing entry with same (fh, access_mode, cred) */
    existing = chimera_vfs_open_cache_shard_find(shard, fh, fhlen, access_mode, cred_hash);

    if (existing) {
        if (existing->opencnt == 0) {
            /* On pending_close: remove from both lists, close old, free */
            DL_DELETE(shard->pending_close, existing);
            chimera_vfs_open_cache_shard_remove(shard, existing);
            /* Insert new handle before dropping lock to prevent
             * concurrent acquires from creating duplicate entries. */
            chimera_vfs_open_cache_shard_insert(shard, handle);

            struct chimera_vfs_close_ref close_ref;
            int                          routed;

            routed = !!(existing->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED);

            if (routed) {
                chimera_vfs_unlinked_route(cache, shard, existing, 0);
            } else {
                chimera_vfs_close_ref_capture(&close_ref, existing);
            }

            chimera_vfs_open_cache_free(shard, existing);
            evpl_mutex_unlock(&shard->lock);
            if (!routed) {
                chimera_vfs_close_ref_dispatch(thread, &close_ref, NULL, NULL);
            }
            callback(request, handle);
            return;
        } else {
            /* In use: detach. Current holders keep working; on release it closes immediately */
            chimera_vfs_open_cache_detach(shard, existing);
        }
    } else {
        /* No existing entry — we're adding a net new handle to the shard */
        if (shard->open_handles < shard->max_open_files) {
            shard->open_handles++;
        } else {
            /* Need to evict a pending_close entry to make room */
            if (shard->pending_close) {
                struct chimera_vfs_open_handle *victim = shard->pending_close;
                DL_DELETE(shard->pending_close, victim);
                chimera_vfs_open_cache_shard_remove(shard, victim);
                /* Insert new handle before dropping lock to prevent
                 * concurrent acquires from creating duplicate entries. */
                chimera_vfs_open_cache_shard_insert(shard, handle);

                struct chimera_vfs_close_ref    close_ref;
                int                             routed;

                routed = !!(victim->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED);

                if (routed) {
                    chimera_vfs_unlinked_route(cache, shard, victim, 0);
                } else {
                    chimera_vfs_close_ref_capture(&close_ref, victim);
                }

                chimera_vfs_open_cache_free(shard, victim);
                evpl_mutex_unlock(&shard->lock);
                if (!routed) {
                    chimera_vfs_close_ref_dispatch(thread, &close_ref, NULL, NULL);
                }
                callback(request, handle);
                return;
            } else {
                /* All handles are active; just exceed the limit */
                shard->open_handles++;
            }
        }
    }

    /* Insert new handle into shard */
    chimera_vfs_open_cache_shard_insert(shard, handle);

    evpl_mutex_unlock(&shard->lock);

    callback(request, handle);
} /* chimera_vfs_open_cache_insert */

/* --- Deferred close (close thread) --- */

static inline struct chimera_vfs_open_handle *
chimera_vfs_open_cache_defer_close(
    struct vfs_open_cache *cache,
    uint64_t               now_ticks,
    uint64_t               min_age,
    uint64_t              *r_count,
    uint64_t              *r_issued)
{
    struct vfs_open_cache_shard    *shard;
    struct chimera_vfs_open_handle *handle, *closed = NULL;
    uint64_t                        elapsed, count = 0;

    for (unsigned int i = 0; i < cache->num_shards; i++) {
        shard = &cache->shards[i];

        evpl_mutex_lock(&shard->lock);

        while (shard->pending_close) {

            handle = shard->pending_close;

            elapsed = now_ticks > handle->timestamp ?
                chimera_vfs_ticks_to_ns(now_ticks - handle->timestamp) : 0;

            if (elapsed < min_age) {
                break;
            }

            DL_DELETE(shard->pending_close, handle);
            chimera_vfs_open_cache_shard_remove(shard, handle);
            shard->open_handles--;

            if (unlikely(handle->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED)) {
                /* Not closed by the sweep: the close thread's unlinked-file
                 * inbox takes it, and counts it in the close fence itself. */
                chimera_vfs_unlinked_route(cache, shard, handle, 1);
                chimera_vfs_open_cache_free(shard, handle);
                continue;
            }

            LL_PREPEND(closed, handle);

            /* Count the handle as taken here, under the same shard lock that
             * removes it, so that no observer can find it absent from both the
             * cache and the close fence.  Bumping this after the loop instead
             * would leave exactly that window, which is the race the fence
             * exists to close. */
            chimera_atomic_add_fetch(r_issued, 1, CHIMERA_MEMORY_RELEASE);
        }

        /* Count AFTER processing - this is the number of handles still in the cache */
        count += shard->open_handles;

        evpl_mutex_unlock(&shard->lock);
    }
    *r_count = count;
    return closed;
} /* vfs_open_cache_defer_close */

/*
 * Take this mount's unreferenced handles out of the cache ahead of a umount,
 * and report how many are still referenced.
 *
 * Unreferenced handles are removed and returned via r_purged for the caller
 * to dispose of: one that holds a backend open needs a close first, one that
 * does not can simply be freed.  Either way it is gone by the time the caller
 * is done with the list.
 *
 * Referenced handles are counted, not touched -- their holder is still using
 * them.  umount waits for that count to reach zero, so every handle on the
 * mount is genuinely gone (and every close that had to run has run) before
 * the mount goes away.
 *
 * The count is transiently non-zero after ordinary activity: a path walk's
 * reference on the mount root outlives the operation that took it by about a
 * millisecond.  That is precisely why umount waits on it rather than failing
 * the moment it sees one.
 */
static inline uint64_t
chimera_vfs_open_cache_purge_by_mount(
    struct vfs_open_cache           *cache,
    const uint8_t                   *mount_id,
    struct chimera_vfs_open_handle **r_purged)
{
    struct vfs_open_cache_shard    *shard;
    struct chimera_vfs_open_handle *handle, *next;
    uint64_t                        referenced = 0;

    for (unsigned int i = 0; i < cache->num_shards; i++) {
        shard = &cache->shards[i];

        evpl_mutex_lock(&shard->lock);

        handle = shard->handles;

        while (handle) {
            next = handle->bucket_next;

            if (memcmp(handle->fh, mount_id, CHIMERA_VFS_MOUNT_ID_SIZE) != 0) {
                handle = next;
                continue;
            }

            if (handle->opencnt) {
                referenced++;
            } else {
                /* Unreferenced handles sit on pending_close awaiting the idle
                 * sweep; umount cannot wait out that timer.  Both detach sites
                 * unlink from shard->handles before setting DETACHED, so
                 * anything still walked here is attached and every attached
                 * handle that reaches opencnt 0 was appended to pending_close.
                 * utlist's own assert on that is compiled out under NDEBUG,
                 * which leaves DL_DELETE dereferencing a null head -- say so
                 * here instead, in every build. */
                chimera_vfs_abort_if(!shard->pending_close,
                                     "purge_by_mount: unreferenced handle absent from pending_close");
                DL_DELETE(shard->pending_close, handle);
                chimera_vfs_open_cache_shard_remove(shard, handle);
                shard->open_handles--;

                if (unlikely(handle->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED)) {
                    /* The close thread closes it; the fence makes umount wait. */
                    chimera_vfs_unlinked_route(cache, shard, handle, 1);
                    chimera_vfs_open_cache_free(shard, handle);
                    handle = next;
                    continue;
                }

                LL_PREPEND(*r_purged, handle);
            }

            handle = next;
        }

        evpl_mutex_unlock(&shard->lock);
    }

    return referenced;
} /* chimera_vfs_open_cache_purge_by_mount */

/*
 * Mark all handles belonging to the given mount for immediate close.
 * This sets their timestamp to 0 so the close thread will close them
 * on the next sweep.
 * Returns count of handles marked.
 */
static inline uint64_t
chimera_vfs_open_cache_mark_for_close_by_mount(
    struct vfs_open_cache *cache,
    const uint8_t         *mount_id)
{
    struct vfs_open_cache_shard    *shard;
    struct chimera_vfs_open_handle *handle;
    uint64_t                        count = 0;

    for (unsigned int i = 0; i < cache->num_shards; i++) {
        shard = &cache->shards[i];

        evpl_mutex_lock(&shard->lock);

        for (handle = shard->handles; handle; handle = handle->bucket_next) {
            if (memcmp(handle->fh, mount_id, CHIMERA_VFS_MOUNT_ID_SIZE) == 0) {
                /* Set timestamp to 0 so it will be closed immediately */
                handle->timestamp = 0;
                count++;
            }
        }

        evpl_mutex_unlock(&shard->lock);
    }

    return count;
} /* chimera_vfs_open_cache_mark_for_close_by_mount */

/*
 * Have the close sweep take the idle handles of one file on its next pass,
 * ahead of everything that is merely old.  The unlink path uses this for the
 * pin it has just dropped, so a file nothing else references is purged at
 * once instead of after the idle age.  The sweep stops at the first handle that
 * is not old enough, so a handle stamped as ancient has to move to the front of
 * the list to be reached.
 */
static inline void
chimera_vfs_open_cache_expedite_close(
    struct vfs_open_cache *cache,
    const uint8_t         *fh,
    uint32_t               fh_len,
    uint64_t               fh_hash)
{
    struct vfs_open_cache_shard    *shard = &cache->shards[fh_hash & cache->shard_mask];
    struct chimera_vfs_open_handle *handle, *tmp;
    int                             found = 0;

    evpl_mutex_lock(&shard->lock);

    DL_FOREACH_SAFE(shard->pending_close, handle, tmp)
    {
        if (handle->fh_len == fh_len && memcmp(handle->fh, fh, fh_len) == 0) {
            DL_DELETE(shard->pending_close, handle);
            handle->timestamp = 0;
            DL_PREPEND(shard->pending_close, handle);
            found = 1;
        }
    }

    if (found && cache->unlinked.doorbell) {
        evpl_ring_doorbell(cache->unlinked.doorbell);
    }

    evpl_mutex_unlock(&shard->lock);
} /* chimera_vfs_open_cache_expedite_close */

/*
 * Lookup a file handle in the open cache and increment its refcount if found.
 *
 * This is used for checking if a file is open (e.g., for silly rename on remove).
 * If the file is open (opencnt > 0), increments opencnt and returns the handle.
 * If not found or not open, returns NULL.
 *
 * The caller must call chimera_vfs_open_cache_release() when done with the handle.
 */
static inline struct chimera_vfs_open_handle *
chimera_vfs_open_cache_lookup_ref(
    struct vfs_open_cache *cache,
    const uint8_t         *fh,
    uint32_t               fh_len,
    uint64_t               fh_hash)
{
    struct vfs_open_cache_shard    *shard;
    struct chimera_vfs_open_handle *handle, *found = NULL;

    shard = &cache->shards[fh_hash & cache->shard_mask];

    evpl_mutex_lock(&shard->lock);

    /* Scan for any handle matching fh with opencnt > 0, regardless of access_mode */
    for (handle = shard->handles; handle; handle = handle->bucket_next) {
        if (handle->fh_len == fh_len &&
            memcmp(handle->fh, fh, fh_len) == 0 &&
            handle->opencnt > 0 &&
            !(handle->flags & CHIMERA_VFS_OPEN_HANDLE_PENDING)) {
            handle->opencnt++;
            if (unlikely(handle->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED)) {
                chimera_vfs_unlinked_ref_inc(shard, handle);
            }
            found = handle;
            break;
        }
    }

    evpl_mutex_unlock(&shard->lock);

    return found;
} /* chimera_vfs_open_cache_lookup_ref */

/*
 * Check if a file handle exists in the open cache.
 *
 * Returns true if the handle exists (regardless of opencnt or pending state).
 */
static inline int
chimera_vfs_open_cache_exists(
    struct vfs_open_cache *cache,
    const uint8_t         *fh,
    uint32_t               fh_len,
    uint64_t               fh_hash)
{
    struct vfs_open_cache_shard    *shard;
    struct chimera_vfs_open_handle *handle;
    int                             found = 0;

    shard = &cache->shards[fh_hash & cache->shard_mask];

    evpl_mutex_lock(&shard->lock);

    /* Scan for any handle matching fh, regardless of access_mode */
    for (handle = shard->handles; handle; handle = handle->bucket_next) {
        if (handle->fh_len == fh_len &&
            memcmp(handle->fh, fh, fh_len) == 0) {
            found = 1;
            break;
        }
    }

    evpl_mutex_unlock(&shard->lock);

    return found;
} /* chimera_vfs_open_cache_exists */

/* --- Delete-on-close helpers --- */

/*
 * Mark a cached handle for delete-on-close.
 */
static inline void
chimera_vfs_open_cache_set_doc(
    struct vfs_open_cache          *cache,
    struct chimera_vfs_open_handle *handle,
    const uint8_t                  *parent_fh,
    uint16_t                        parent_fh_len,
    const char                     *name,
    uint16_t                        name_len,
    const struct chimera_vfs_cred  *cred)
{
    struct vfs_open_cache_shard *shard;

    chimera_vfs_abort_if(parent_fh_len > CHIMERA_VFS_FH_SIZE,
                         "set_doc: parent_fh_len exceeds FH_SIZE");
    chimera_vfs_abort_if(name_len > CHIMERA_VFS_NAME_MAX,
                         "set_doc: name_len exceeds NAME_MAX");

    shard = &cache->shards[handle->fh_hash & cache->shard_mask];

    evpl_mutex_lock(&shard->lock);

    handle->doc_delete_on_close = 1;
    handle->doc_parent_fh_len   = parent_fh_len;
    handle->doc_name_len        = name_len;
    handle->doc_cred            = *cred;

    if (parent_fh_len > 0) {
        memcpy(handle->doc_parent_fh, parent_fh, parent_fh_len);
    }

    if (name_len > 0) {
        memcpy(handle->doc_name, name, name_len);
    }

    evpl_mutex_unlock(&shard->lock);
} /* chimera_vfs_open_cache_set_doc */

/*
 * Clear the delete-on-close flag.
 */
static inline void
chimera_vfs_open_cache_clear_doc(
    struct vfs_open_cache          *cache,
    struct chimera_vfs_open_handle *handle)
{
    struct vfs_open_cache_shard *shard;

    shard = &cache->shards[handle->fh_hash & cache->shard_mask];

    evpl_mutex_lock(&shard->lock);

    handle->doc_delete_on_close = 0;
    handle->doc_parent_fh_len   = 0;
    handle->doc_name_len        = 0;

    evpl_mutex_unlock(&shard->lock);
} /* chimera_vfs_open_cache_clear_doc */

/*
 * Atomically release a handle reference and check for delete-on-close.
 *
 * If this is the last reference (opencnt drops to 0) and DOC is set,
 * the handle is removed from the cache and the DOC parameters are
 * copied into the caller-provided output buffers.  Returns 1.
 *
 * Otherwise performs a normal release and returns 0.
 */
struct chimera_vfs_doc_info {
    uint8_t                      parent_fh[CHIMERA_VFS_FH_SIZE];
    char                         name[CHIMERA_VFS_NAME_MAX];
    struct chimera_vfs_cred      cred;
    uint16_t                     parent_fh_len;
    uint16_t                     name_len;
    /* Backend close state -- the caller closes after the unlink */
    struct chimera_vfs_close_ref close_ref;
};

static inline int
chimera_vfs_open_cache_release_doc(
    struct chimera_vfs_thread      *thread,
    struct vfs_open_cache          *cache,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_doc_info    *doc_out)
{
    struct vfs_open_cache_shard *shard;
    struct chimera_vfs_request  *requests;
    int                          do_doc = 0;

    shard = &cache->shards[handle->fh_hash & cache->shard_mask];

    chimera_vfs_abort_if(handle->cache_id != shard->cache_id,
                         "handle released by wrong cache");

    evpl_mutex_lock(&shard->lock);

    handle->flags &= ~CHIMERA_VFS_OPEN_HANDLE_EXCLUSIVE;

    requests                 = handle->blocked_requests;
    handle->blocked_requests = NULL;

    handle->opencnt--;

    if (unlikely(handle->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED)) {
        chimera_vfs_unlinked_ref_dec(cache, shard, handle);
    }

    /* A file that already lost its last link has no name left for the
     * delete-on-close to remove, and its handle must go through the close
     * thread like any other of its file, so it takes the plain release. */
    if (handle->opencnt == 0 && handle->doc_delete_on_close &&
        !(handle->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED)) {
        /* Last reference with DOC — extract deletion info and remove
         * from cache.  The caller is responsible for the actual unlink
         * and for closing the underlying VFS module handle. */
        doc_out->parent_fh_len = handle->doc_parent_fh_len;
        doc_out->name_len      = handle->doc_name_len;
        doc_out->cred          = handle->doc_cred;
        chimera_vfs_close_ref_capture(&doc_out->close_ref, handle);
        memcpy(doc_out->parent_fh, handle->doc_parent_fh,
               handle->doc_parent_fh_len);
        memcpy(doc_out->name, handle->doc_name, handle->doc_name_len);

        if (handle->flags & CHIMERA_VFS_OPEN_HANDLE_DETACHED) {
            DL_DELETE(shard->detached, handle);
            chimera_vfs_open_cache_free(shard, handle);
        } else {
            chimera_vfs_open_cache_shard_remove(shard, handle);
            shard->open_handles--;
            chimera_vfs_open_cache_free(shard, handle);
        }

        do_doc = 1;

        evpl_mutex_unlock(&shard->lock);
        chimera_vfs_open_cache_release_blocked(thread, requests, 0);
        return do_doc;
    }

    if (handle->opencnt == 0) {
        if (handle->flags & CHIMERA_VFS_OPEN_HANDLE_DETACHED) {
            struct chimera_vfs_close_ref close_ref;
            int                          routed;

            DL_DELETE(shard->detached, handle);

            routed = !!(handle->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED);

            if (routed) {
                chimera_vfs_unlinked_route(cache, shard, handle, 0);
            } else {
                chimera_vfs_close_ref_capture(&close_ref, handle);
            }
            chimera_vfs_open_cache_free(shard, handle);
            evpl_mutex_unlock(&shard->lock);
            chimera_vfs_open_cache_release_blocked(thread, requests, 0);
            if (!routed) {
                chimera_vfs_close_ref_dispatch(thread, &close_ref, NULL, NULL);
            }
            return 0;
        }
        handle->timestamp = chimera_vfs_now_ticks();
        DL_APPEND(shard->pending_close, handle);
    }

    evpl_mutex_unlock(&shard->lock);
    chimera_vfs_open_cache_release_blocked(thread, requests, 0);

    return 0;
} /* chimera_vfs_open_cache_release_doc */
