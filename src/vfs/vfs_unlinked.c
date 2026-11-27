// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>

#include "vfs_internal.h"
#include "vfs_open_cache.h"
#include "vfs_unlinked.h"
#include "vfs_ref.h"
#include "vfs_release.h"
#include "common/atomic.h"
#include "common/macros.h"
#include "common/misc.h"
#include "prometheus-c.h"

static const char                        *chimera_vfs_unlinked_event_names[CHIMERA_VFS_UNLINKED_EV_NUM] = {
    "tracked", "recalled", "purged", "purge_failed", "relinked", "leaked_at_shutdown",
};

/* Over the life of the process and every instance, for tests that have to look
 * after the instance that counted an event is gone. */
static uint64_t                           chimera_vfs_unlinked_total[CHIMERA_VFS_UNLINKED_EV_NUM];

/* Called by the test that wants to see where a purge runs. */
static chimera_vfs_unlinked_test_purge_fn chimera_vfs_unlinked_purge_hook;
static void                              *chimera_vfs_unlinked_purge_hook_arg;

/* Called by the test that wants to see the order of the purge's steps. */
static chimera_vfs_unlinked_test_step_fn  chimera_vfs_unlinked_step_hook;
static void                              *chimera_vfs_unlinked_step_hook_arg;

void
chimera_vfs_unlinked_ctl_init(
    struct chimera_vfs_unlinked_ctl *ctl,
    struct prometheus_metrics       *metrics)
{
    evpl_mutex_init(&ctl->lock, NULL);

    if (!metrics) {
        return;
    }

    ctl->metrics = metrics;
    ctl->counter = prometheus_metrics_create_counter(metrics, "chimera_vfs_unlinked",
                                                     "Files unlinked while referenced, by what became of them");

    for (int i = 0; i < CHIMERA_VFS_UNLINKED_EV_NUM; i++) {
        ctl->series[i] = prometheus_counter_create_series(ctl->counter,
                                                          (const char *[]) { "event" },
                                                          (const char *[]) { chimera_vfs_unlinked_event_names[i] }, 1);
        ctl->instance[i] = prometheus_counter_series_create_instance(ctl->series[i]);
    }

    ctl->gauge = prometheus_metrics_create_gauge(metrics, "chimera_vfs_unlinked_records",
                                                 "Unlinked files the VFS still holds a record for");
    ctl->gauge_series   = prometheus_gauge_create_series(ctl->gauge, NULL, NULL, 0);
    ctl->gauge_instance = prometheus_gauge_series_create_instance(ctl->gauge_series);
} /* chimera_vfs_unlinked_ctl_init */

void
chimera_vfs_unlinked_ctl_destroy(struct chimera_vfs_unlinked_ctl *ctl)
{
    if (ctl->metrics) {
        prometheus_gauge_destroy_series(ctl->gauge, ctl->gauge_series);
        prometheus_gauge_destroy(ctl->metrics, ctl->gauge);

        for (int i = 0; i < CHIMERA_VFS_UNLINKED_EV_NUM; i++) {
            prometheus_counter_destroy_series(ctl->counter, ctl->series[i]);
        }
        prometheus_counter_destroy(ctl->metrics, ctl->counter);
    }

    evpl_mutex_destroy(&ctl->lock);
} /* chimera_vfs_unlinked_ctl_destroy */

void
chimera_vfs_unlinked_attach_close_thread(
    struct vfs_open_cache           *cache,
    struct chimera_vfs_close_thread *close_thread)
{
    cache->unlinked.doorbell        = &close_thread->doorbell;
    cache->unlinked.fence_issued    = &close_thread->closes_issued;
    cache->unlinked.fence_completed = &close_thread->closes_completed;
} /* chimera_vfs_unlinked_attach_close_thread */

void
chimera_vfs_unlinked_event(
    struct chimera_vfs_unlinked_ctl *ctl,
    enum chimera_vfs_unlinked_event  event)
{
    chimera_atomic_add_fetch(&chimera_vfs_unlinked_total[event], 1, CHIMERA_MEMORY_RELEASE);

    evpl_mutex_lock(&ctl->lock);
    ctl->events[event]++;
    if (ctl->metrics) {
        prometheus_counter_increment(ctl->instance[event]);
    }
    evpl_mutex_unlock(&ctl->lock);
} /* chimera_vfs_unlinked_event */

static void
chimera_vfs_unlinked_alive_add(
    struct chimera_vfs_unlinked_ctl *ctl,
    int                              delta)
{
    evpl_mutex_lock(&ctl->lock);
    ctl->alive += delta;
    if (ctl->metrics) {
        prometheus_gauge_set(ctl->gauge_instance, (int64_t) ctl->alive);
    }
    evpl_mutex_unlock(&ctl->lock);
} /* chimera_vfs_unlinked_alive_add */

/* The record goes once its purge is over.  Only the close thread calls this,
 * and only for a record in PURGING, so no handle can refer to it. */
static void
chimera_vfs_unlinked_record_delete(
    struct vfs_open_cache            *cache,
    struct chimera_vfs_unlinked_work *work)
{
    struct chimera_vfs_unlinked *rec   = work->rec;
    struct vfs_open_cache_shard *shard = rec->shard;

    evpl_mutex_lock(&shard->lock);
    DL_DELETE(shard->unlinked, rec);
    evpl_mutex_unlock(&shard->lock);

    free(rec);

    /* A relinked file, a failed delete and a leak have each been counted where
     * they happened; what is left is the ordinary end of a purge. */
    if (!work->relinked && !work->failed && !work->leaked) {
        chimera_vfs_unlinked_event(&cache->unlinked, CHIMERA_VFS_UNLINKED_EV_PURGED);
    }
    chimera_vfs_unlinked_alive_add(&cache->unlinked, -1);
} /* chimera_vfs_unlinked_record_delete */

static void
chimera_vfs_unlinked_work_finish(struct chimera_vfs_unlinked_work *work)
{
    struct chimera_vfs_close_thread *ct    = work->close_thread;
    struct vfs_open_cache           *cache = ct->vfs->vfs_open_file_cache;

    if (work->last) {
        chimera_vfs_unlinked_record_delete(cache, work);
    }

    if (work->fenced) {
        chimera_atomic_add_fetch(&ct->closes_completed, 1, CHIMERA_MEMORY_RELEASE);
    }

    if (work->pending) {
        ct->num_pending--;
    }

    free(work);
} /* chimera_vfs_unlinked_work_finish */

static void
chimera_vfs_unlinked_close_done(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    chimera_vfs_unlinked_work_finish(private_data);
} /* chimera_vfs_unlinked_close_done */

static void
chimera_vfs_unlinked_step(
    struct chimera_vfs_unlinked   *rec,
    enum chimera_vfs_unlinked_step step)
{
    if (chimera_vfs_unlinked_step_hook) {
        chimera_vfs_unlinked_step_hook(step, rec->fh, rec->fh_len, chimera_vfs_unlinked_step_hook_arg);
    }
} /* chimera_vfs_unlinked_step */

/* The last step: the backend close.  For a block or SCSI file this is what
 * lets the file system reclaim the extents, and it comes after the backing file
 * is gone so that a crash in between leaves an inode that the mount-time orphan
 * scan reclaims rather than a backing file nobody can name. */
static void
chimera_vfs_unlinked_close_step(struct chimera_vfs_unlinked_work *work)
{
    struct chimera_vfs_close_thread *ct = work->close_thread;

    if (work->last) {
        chimera_vfs_unlinked_step(work->rec, CHIMERA_VFS_UNLINKED_STEP_BACKEND_CLOSE);
    }

    if (work->close_ref.needs_close) {
        chimera_vfs_close_ref_dispatch(ct->vfs_thread, &work->close_ref,
                                       chimera_vfs_unlinked_close_done, work);
    } else {
        chimera_vfs_unlinked_work_finish(work);
    }
} /* chimera_vfs_unlinked_close_step */

static void
chimera_vfs_unlinked_backing_removed(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_vfs_unlinked_work *work = private_data;
    struct chimera_vfs_close_thread  *ct   = work->close_thread;
    struct chimera_vfs_unlinked      *rec  = work->rec;

    chimera_vfs_release(ct->vfs_thread, work->ds_root);
    work->ds_root = NULL;

    if (error_code == CHIMERA_VFS_ENOENT) {
        /* Already gone: there is nothing left to leak. */
        chimera_vfs_info("pNFS: data-server backing file %s already gone", rec->facts.backing_name);
    } else if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_error("pNFS: failed to delete data-server backing file %s (err=%d); space leaked",
                          rec->facts.backing_name, error_code);
        work->failed = 1;
        chimera_vfs_unlinked_event(&ct->vfs->vfs_open_file_cache->unlinked,
                                   CHIMERA_VFS_UNLINKED_EV_PURGE_FAILED);
    }

    chimera_vfs_unlinked_close_step(work);
} /* chimera_vfs_unlinked_backing_removed */

static void
chimera_vfs_unlinked_ds_root_opened(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct chimera_vfs_unlinked_work *work = private_data;
    struct chimera_vfs_close_thread  *ct   = work->close_thread;
    struct chimera_vfs_unlinked      *rec  = work->rec;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_error("pNFS: failed to delete data-server backing file %s (err=%d); space leaked",
                          rec->facts.backing_name, error_code);
        work->failed = 1;
        chimera_vfs_unlinked_event(&ct->vfs->vfs_open_file_cache->unlinked,
                                   CHIMERA_VFS_UNLINKED_EV_PURGE_FAILED);
        chimera_vfs_unlinked_close_step(work);
        return;
    }

    work->ds_root = handle;

    chimera_vfs_info("pNFS: deleting data-server backing file %s of an unlinked file",
                     rec->facts.backing_name);

    chimera_vfs_remove_at(ct->vfs_thread, &rec->facts.cred, handle,
                          rec->facts.backing_name, strlen(rec->facts.backing_name),
                          NULL, 0, 0, 0, 0, NULL,
                          chimera_vfs_unlinked_backing_removed, work);
} /* chimera_vfs_unlinked_ds_root_opened */

static void
chimera_vfs_unlinked_delete_backing(struct chimera_vfs_unlinked_work *work)
{
    struct chimera_vfs_close_thread *ct  = work->close_thread;
    struct chimera_vfs_unlinked     *rec = work->rec;
    const struct chimera_vfs_ds     *ds;

    if (!rec->facts.has_backing) {
        /* Nothing on a data server by name: closing the file is all there is. */
        chimera_vfs_unlinked_close_step(work);
        return;
    }

    if (ct->shutdown) {
        /* The data server's mount may already be gone. */
        chimera_vfs_error("pNFS: shutting down with the backing file %s of an unlinked file still there; "
                          "space leaked", rec->facts.backing_name);
        work->leaked = 1;
        chimera_vfs_unlinked_event(&ct->vfs->vfs_open_file_cache->unlinked,
                                   CHIMERA_VFS_UNLINKED_EV_LEAKED_AT_SHUTDOWN);
        chimera_vfs_unlinked_close_step(work);
        return;
    }

    chimera_vfs_unlinked_step(rec, CHIMERA_VFS_UNLINKED_STEP_BACKING_DELETE);

    ds = chimera_vfs_pnfs_find_device(ct->vfs, rec->facts.deviceid);

    if (!ds || ds->root_fh_len == 0) {
        chimera_vfs_error("pNFS: no data server for backing file %s; space leaked",
                          rec->facts.backing_name);
        work->failed = 1;
        chimera_vfs_unlinked_event(&ct->vfs->vfs_open_file_cache->unlinked,
                                   CHIMERA_VFS_UNLINKED_EV_PURGE_FAILED);
        chimera_vfs_unlinked_close_step(work);
        return;
    }

    chimera_vfs_open_fh(ct->vfs_thread, &rec->facts.cred, ds->root_fh, ds->root_fh_len,
                        CHIMERA_VFS_OPEN_DIRECTORY | CHIMERA_VFS_OPEN_INFERRED,
                        chimera_vfs_unlinked_ds_root_opened, work);
} /* chimera_vfs_unlinked_delete_backing */

/* The link count read through the carrier just before the delete.  A LINK made
 * after the unlink brought the file back, and its data with it: delete nothing.
 * A count that cannot be read is not a reason to keep the file. */
static void
chimera_vfs_unlinked_recheck_done(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_unlinked_work *work = private_data;
    struct chimera_vfs_close_thread  *ct   = work->close_thread;

    if (error_code == CHIMERA_VFS_OK &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_NLINK) &&
        attr->va_nlink > 0) {
        chimera_vfs_info("pNFS: unlinked file has a link again; keeping its data");
        work->relinked = 1;
        chimera_vfs_unlinked_event(&ct->vfs->vfs_open_file_cache->unlinked,
                                   CHIMERA_VFS_UNLINKED_EV_RELINKED);
        chimera_vfs_unlinked_close_step(work);
        return;
    }

    chimera_vfs_unlinked_delete_backing(work);
} /* chimera_vfs_unlinked_recheck_done */

/* The last handle of the file has left the cache: decide whether its data goes. */
static void
chimera_vfs_unlinked_purge(struct chimera_vfs_unlinked_work *work)
{
    struct chimera_vfs_close_thread *ct  = work->close_thread;
    struct chimera_vfs_unlinked     *rec = work->rec;
    struct chimera_vfs_open_handle  *probe;

    if (!rec->have_facts) {
        chimera_vfs_unlinked_close_step(work);
        return;
    }

    if (work->carrierless || !work->close_ref.needs_close) {
        /* No backend open is left to ask.  diskfs cannot open the inode by
         * handle any more, which itself means it has no links. */
        chimera_vfs_info("pNFS: no handle to ask for the link count of an unlinked file; "
                         "deleting its data without a recheck");
        chimera_vfs_unlinked_delete_backing(work);
        return;
    }

    chimera_vfs_unlinked_step(rec, CHIMERA_VFS_UNLINKED_STEP_RECHECK);

    /* The handle struct was recycled when it left the cache; a stand-in with
    * the same module, file handle and backend slot reaches the same open. */
    probe              = &work->probe;
    probe->vfs_module  = work->close_ref.module;
    probe->vfs_private = work->close_ref.vfs_private;
    probe->fh_hash     = work->close_ref.fh_hash;
    probe->fh_len      = (uint16_t) work->close_ref.fh_len;
    probe->cache_id    = CHIMERA_VFS_OPEN_ID_FILE;
    probe->opencnt     = 1;
    memcpy(probe->fh, work->close_ref.fh, work->close_ref.fh_len);

    chimera_vfs_cred_init_unix(&work->root, 0, 0, 0, NULL);

    /* The layout bit is not wanted for its value: a request for anything the
     * attribute cache does not hold is what keeps it from answering with the
     * link count from before the unlink. */
    chimera_vfs_getattr(ct->vfs_thread, &work->root, probe,
                        CHIMERA_VFS_ATTR_NLINK | CHIMERA_VFS_ATTR_PNFS_LAYOUT,
                        chimera_vfs_unlinked_recheck_done, work);
} /* chimera_vfs_unlinked_purge */


/* ---- Recalling the holders of recallable references ---- */

static void
chimera_vfs_recall_calls_add(
    struct chimera_vfs_recall_call **calls,
    uint32_t                        *n,
    uint32_t                        *cap,
    chimera_vfs_recall_fn            fn,
    void                            *arg,
    const uint8_t                   *fh,
    uint16_t                         fh_len)
{
    struct chimera_vfs_recall_call *c;

    /* A holder with several references on one file is called once.  The key is
     * the callback, its argument and the file: the argument is what tells two
     * holders of the same kind apart. */
    for (uint32_t i = 0; i < *n; i++) {
        c = &(*calls)[i];

        if (c->fn == fn && c->arg == arg && c->fh_len == fh_len && memcmp(c->fh, fh, fh_len) == 0) {
            return;
        }
    }

    if (*n == *cap) {
        *cap   = *cap ? *cap * 2 : 4;
        *calls = realloc(*calls, *cap * sizeof(**calls));
        chimera_vfs_abort_if(!*calls, "out of memory collecting recall calls");
    }

    c         = &(*calls)[(*n)++];
    c->fn     = fn;
    c->arg    = arg;
    c->fh_len = fh_len;
    memcpy(c->fh, fh, fh_len);
} /* chimera_vfs_recall_calls_add */

/* Mark every recallable reference of one handle that has not been called as
 * called, and collect who to call.  Caller holds the shard lock. */
static void
chimera_vfs_unlinked_collect_handle(
    struct chimera_vfs_unlinked     *rec,
    struct chimera_vfs_open_handle  *handle,
    struct chimera_vfs_recall_call **calls,
    uint32_t                        *n,
    uint32_t                        *cap)
{
    struct chimera_vfs_recallable *r;

    for (r = handle->recallables; r; r = r->next) {
        if (r->fired) {
            continue;
        }

        r->fired = 1;

        if (rec) {
            chimera_vfs_abort_if(rec->unfired == 0, "unlinked file unfired count underflow");
            rec->unfired--;
        }

        chimera_vfs_recall_calls_add(calls, n, cap, r->fn, r->arg, handle->fh, handle->fh_len);
    }
} /* chimera_vfs_unlinked_collect_handle */

static void
chimera_vfs_unlinked_invoke(
    struct chimera_vfs_unlinked_ctl *ctl,
    struct chimera_vfs_recall_call  *calls,
    uint32_t                         n,
    enum chimera_vfs_recall_reason   reason)
{
    for (uint32_t i = 0; i < n; i++) {
        chimera_vfs_unlinked_event(ctl, CHIMERA_VFS_UNLINKED_EV_RECALLED);
        calls[i].fn(calls[i].fh, calls[i].fh_len, reason, calls[i].arg);
    }
} /* chimera_vfs_unlinked_invoke */

static void
chimera_vfs_unlinked_process_recall(
    struct chimera_vfs_close_thread  *ct,
    struct chimera_vfs_unlinked_work *work)
{
    struct vfs_open_cache          *cache = ct->vfs->vfs_open_file_cache;
    struct chimera_vfs_recall_call *calls = NULL;
    uint32_t                        n     = 0, cap = 0;

    if (work->kind == CHIMERA_VFS_UNLINKED_WORK_RECALL) {
        struct chimera_vfs_unlinked    *rec   = work->rec;
        struct vfs_open_cache_shard    *shard = rec->shard;
        struct chimera_vfs_open_handle *handle;

        /* Collected and marked under the shard lock, called after it is dropped:
         * the holder may release on another thread while its callback runs, and
         * the callback may call back into the VFS. */
        evpl_mutex_lock(&shard->lock);

        rec->fire_queued = 0;

        for (handle = shard->handles; handle; handle = handle->bucket_next) {
            if (handle->fh_len == rec->fh_len && memcmp(handle->fh, rec->fh, rec->fh_len) == 0) {
                chimera_vfs_unlinked_collect_handle(rec, handle, &calls, &n, &cap);
            }
        }
        for (handle = shard->detached; handle; handle = handle->next) {
            if (handle->fh_len == rec->fh_len && memcmp(handle->fh, rec->fh, rec->fh_len) == 0) {
                chimera_vfs_unlinked_collect_handle(rec, handle, &calls, &n, &cap);
            }
        }

        evpl_mutex_unlock(&shard->lock);
    } else {
        calls       = work->calls;
        n           = work->ncalls;
        work->calls = NULL;
    }

    if (n) {
        chimera_vfs_info("unlinked file: recalling %u holder(s) of references on it", n);
        chimera_vfs_unlinked_invoke(&cache->unlinked, calls, n, (enum chimera_vfs_recall_reason) work->reason);
    }

    free(calls);
    free(work);
} /* chimera_vfs_unlinked_process_recall */

void
chimera_vfs_unlinked_recall_mount(
    struct chimera_vfs *vfs,
    const uint8_t      *mount_id)
{
    struct vfs_open_cache            *cache = vfs->vfs_open_file_cache;
    struct chimera_vfs_recall_call   *calls = NULL;
    struct chimera_vfs_unlinked_work *work;
    uint32_t                          n = 0, cap = 0;

    if (chimera_atomic_load_n(&cache->unlinked.live_recallables, CHIMERA_MEMORY_ACQUIRE) == 0) {
        return;
    }

    for (unsigned int i = 0; i < cache->num_shards; i++) {
        struct vfs_open_cache_shard    *shard = &cache->shards[i];
        struct chimera_vfs_open_handle *handle;

        evpl_mutex_lock(&shard->lock);

        for (int pass = 0; pass < 2; pass++) {
            for (handle = pass ? shard->detached : shard->handles; handle;
                 handle = pass ? handle->next : handle->bucket_next) {
                struct chimera_vfs_unlinked *rec = NULL;

                if (!handle->recallables ||
                    memcmp(handle->fh, mount_id, CHIMERA_VFS_MOUNT_ID_SIZE) != 0) {
                    continue;
                }

                if (handle->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED) {
                    rec = chimera_vfs_unlinked_find(shard, handle->fh, handle->fh_len);
                }

                chimera_vfs_unlinked_collect_handle(rec, handle, &calls, &n, &cap);
            }
        }

        evpl_mutex_unlock(&shard->lock);
    }

    if (!n) {
        free(calls);
        return;
    }

    work         = calloc(1, sizeof(*work));
    work->kind   = CHIMERA_VFS_UNLINKED_WORK_CALLS;
    work->reason = CHIMERA_VFS_RECALL_UMOUNT;
    work->calls  = calls;
    work->ncalls = n;

    chimera_vfs_unlinked_queue(cache, work);
} /* chimera_vfs_unlinked_recall_mount */

SYMBOL_EXPORT enum chimera_vfs_error
chimera_vfs_ref_make_recallable(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_recallable  *r)
{
    struct vfs_open_cache       *cache = thread->vfs->vfs_open_file_cache;
    struct vfs_open_cache_shard *shard;
    struct chimera_vfs_unlinked *rec = NULL;

    chimera_vfs_abort_if(handle->cache_id != CHIMERA_VFS_OPEN_ID_FILE,
                         "recallable reference on a handle that is not in the file cache");

    shard = &cache->shards[handle->fh_hash & cache->shard_mask];

    evpl_mutex_lock(&shard->lock);

    chimera_vfs_abort_if(handle->opencnt - handle->recallcnt == 0,
                         "recallable reference made without a plain reference to convert");

    if (handle->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED) {
        rec = chimera_vfs_unlinked_find(shard, handle->fh, handle->fh_len);

        chimera_vfs_abort_if(!rec, "handle flagged unlinked has no record");

        if (rec->state == CHIMERA_VFS_UNLINKED_PURGING) {
            evpl_mutex_unlock(&shard->lock);
            return CHIMERA_VFS_ESTALE;
        }

        if (rec->strong <= 1) {
            /* The caller's own reference is the last real one, so the file
             * would be recalled the moment this returned. */
            evpl_mutex_unlock(&shard->lock);
            return CHIMERA_VFS_EAGAIN;
        }

        rec->strong--;
        rec->recallable++;
        rec->unfired++;
    }

    r->handle = handle;
    r->fired  = 0;
    handle->recallcnt++;
    DL_APPEND(handle->recallables, r);

    chimera_atomic_add_fetch(&cache->unlinked.live_recallables, 1, CHIMERA_MEMORY_RELEASE);

    evpl_mutex_unlock(&shard->lock);

    return CHIMERA_VFS_OK;
} /* chimera_vfs_ref_make_recallable */

SYMBOL_EXPORT void
chimera_vfs_ref_release_recallable(
    struct chimera_vfs_thread     *thread,
    struct chimera_vfs_recallable *r)
{
    struct vfs_open_cache          *cache  = thread->vfs->vfs_open_file_cache;
    struct chimera_vfs_open_handle *handle = r->handle;
    struct vfs_open_cache_shard    *shard;

    if (!handle) {
        /* Forgotten at shutdown, or never registered. */
        return;
    }

    shard = &cache->shards[handle->fh_hash & cache->shard_mask];

    evpl_mutex_lock(&shard->lock);

    DL_DELETE(handle->recallables, r);
    handle->recallcnt--;
    r->handle = NULL;

    /* Turn it back into a plain reference and let the ordinary release drop it,
     * so the file's counts and the close of an idle handle follow one path. */
    if (handle->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED) {
        struct chimera_vfs_unlinked *rec = chimera_vfs_unlinked_find(shard, handle->fh, handle->fh_len);

        chimera_vfs_abort_if(!rec, "handle flagged unlinked has no record");
        chimera_vfs_abort_if(rec->recallable == 0, "unlinked file recallable count underflow");

        rec->recallable--;
        rec->strong++;

        if (!r->fired) {
            chimera_vfs_abort_if(rec->unfired == 0, "unlinked file unfired count underflow");
            rec->unfired--;
        }
    }

    chimera_atomic_sub_fetch(&cache->unlinked.live_recallables, 1, CHIMERA_MEMORY_RELEASE);

    evpl_mutex_unlock(&shard->lock);

    chimera_vfs_release(thread, handle);
} /* chimera_vfs_ref_release_recallable */

void
chimera_vfs_unlinked_abandon_recallable(struct chimera_vfs_close_thread *ct)
{
    struct vfs_open_cache          *cache = ct->vfs->vfs_open_file_cache;
    struct chimera_vfs_open_handle *drop[256];
    uint32_t                        drop_n[256], ndrop;

    if (chimera_atomic_load_n(&cache->unlinked.live_recallables, CHIMERA_MEMORY_ACQUIRE) == 0) {
        return;
    }

    for (unsigned int i = 0; i < cache->num_shards; i++) {
        struct vfs_open_cache_shard    *shard = &cache->shards[i];
        struct chimera_vfs_open_handle *handle;
        int                             more;

        do {
            more  = 0;
            ndrop = 0;

            evpl_mutex_lock(&shard->lock);

            for (int pass = 0; pass < 2 && !more; pass++) {
                for (handle = pass ? shard->detached : shard->handles; handle;
                     handle = pass ? handle->next : handle->bucket_next) {
                    struct chimera_vfs_recallable *r;
                    struct chimera_vfs_unlinked   *rec = NULL;
                    uint32_t                       n   = 0;

                    if (!handle->recallcnt) {
                        continue;
                    }

                    if (ndrop == 256) {
                        more = 1;
                        break;
                    }

                    if (handle->flags & CHIMERA_VFS_OPEN_HANDLE_UNLINKED) {
                        rec = chimera_vfs_unlinked_find(shard, handle->fh, handle->fh_len);
                    }

                    for (r = handle->recallables; r; r = r->next) {
                        if (rec) {
                            rec->recallable--;
                            rec->strong++;
                            if (!r->fired) {
                                rec->unfired--;
                            }
                        }
                        r->handle = NULL;
                        n++;
                    }

                    handle->recallables = NULL;
                    handle->recallcnt   = 0;
                    drop[ndrop]         = handle;
                    drop_n[ndrop]       = n;
                    ndrop++;
                    chimera_atomic_sub_fetch(&cache->unlinked.live_recallables, n, CHIMERA_MEMORY_RELEASE);
                }
            }

            evpl_mutex_unlock(&shard->lock);

            /* Each is now an ordinary reference; the handle stays in the cache
             * while they are outstanding, so it can be released here. */
            for (uint32_t k = 0; k < ndrop; k++) {
                for (uint32_t j = 0; j < drop_n[k]; j++) {
                    chimera_vfs_release(ct->vfs_thread, drop[k]);
                }
            }
        } while (more);
    }
} /* chimera_vfs_unlinked_abandon_recallable */

SYMBOL_EXPORT uint64_t
chimera_vfs_unlinked_test_total_event(enum chimera_vfs_unlinked_event event)
{
    return chimera_atomic_load_n(&chimera_vfs_unlinked_total[event], CHIMERA_MEMORY_ACQUIRE);
} /* chimera_vfs_unlinked_test_total_event */

/*
 * Take one routed handle out of its file's count and close it.  The handle
 * that brings the count to zero carries the purge: it rechecks the link count,
 * deletes the data server's backing file, and only then does the backend close.
 */
static void
chimera_vfs_unlinked_process(
    struct chimera_vfs_close_thread  *ct,
    struct chimera_vfs_unlinked_work *work)
{
    struct chimera_vfs_unlinked *rec;

    if (work->kind != CHIMERA_VFS_UNLINKED_WORK_HANDLE) {
        chimera_vfs_unlinked_process_recall(ct, work);
        return;
    }

    rec = work->rec;

    work->close_thread = ct;
    work->pending      = 1;
    ct->num_pending++;

    if (work->carrierless) {
        /* The count was settled when the open failed. */
        work->last = 1;
    } else {
        struct vfs_open_cache_shard *shard = rec->shard;

        evpl_mutex_lock(&shard->lock);
        chimera_vfs_abort_if(rec->handles == 0, "unlinked file handle count underflow");
        rec->handles--;
        if (rec->handles == 0) {
            rec->state = CHIMERA_VFS_UNLINKED_PURGING;
            work->last = 1;
        }
        evpl_mutex_unlock(&shard->lock);
    }

    if (work->last && chimera_vfs_unlinked_purge_hook) {
        chimera_vfs_unlinked_purge_hook(rec->fh, rec->fh_len, work->carrierless,
                                        chimera_vfs_unlinked_purge_hook_arg);
    }

    if (work->last) {
        chimera_vfs_unlinked_purge(work);
    } else {
        chimera_vfs_unlinked_close_step(work);
    }
} /* chimera_vfs_unlinked_process */

uint64_t
chimera_vfs_unlinked_drain(struct chimera_vfs_close_thread *ct)
{
    struct chimera_vfs_unlinked_ctl  *ctl = &ct->vfs->vfs_open_file_cache->unlinked;
    struct chimera_vfs_unlinked_work *list, *work;
    uint64_t                          count = 0;

    evpl_mutex_lock(&ctl->lock);
    list           = ctl->work_head;
    ctl->work_head = NULL;
    ctl->work_tail = NULL;
    evpl_mutex_unlock(&ctl->lock);

    while (list) {
        work = list;
        list = work->next;
        chimera_vfs_unlinked_process(ct, work);
        count++;
    }

    return count;
} /* chimera_vfs_unlinked_drain */

void
chimera_vfs_unlinked_cache_destroy(struct vfs_open_cache *cache)
{
    struct chimera_vfs_unlinked_work *work;
    struct chimera_vfs_unlinked      *rec, *next;

    while ((work = cache->unlinked.work_head)) {
        cache->unlinked.work_head = work->next;
        free(work->calls);
        free(work);
    }
    cache->unlinked.work_tail = NULL;

    for (unsigned int i = 0; i < cache->num_shards; i++) {
        for (rec = cache->shards[i].unlinked; rec; rec = next) {
            next = rec->next;
            chimera_vfs_unlinked_event(&cache->unlinked, CHIMERA_VFS_UNLINKED_EV_LEAKED_AT_SHUTDOWN);
            free(rec);
        }
        cache->shards[i].unlinked = NULL;
    }
} /* chimera_vfs_unlinked_cache_destroy */

static void
chimera_vfs_unlinked_scan_list(
    struct chimera_vfs_unlinked    *rec,
    struct chimera_vfs_open_handle *handle,
    int                             detached)
{
    struct chimera_vfs_recallable *r;

    for (; handle; handle = detached ? handle->next : handle->bucket_next) {
        if (handle->fh_len != rec->fh_len || memcmp(handle->fh, rec->fh, rec->fh_len) != 0) {
            continue;
        }

        handle->flags |= CHIMERA_VFS_OPEN_HANDLE_UNLINKED;
        rec->handles++;
        rec->strong     += handle->opencnt - handle->recallcnt;
        rec->recallable += handle->recallcnt;

        for (r = handle->recallables; r; r = r->next) {
            if (!r->fired) {
                rec->unfired++;
            }
        }
    }
} /* chimera_vfs_unlinked_scan_list */

/* Whether any handle of the file, attached or detached, has a recallable
 * reference.  Caller holds the shard lock.  Looks only; the scan that makes a
 * record flags every handle it finds, so the question has to be settled first. */
static int
chimera_vfs_unlinked_has_recallable(
    struct vfs_open_cache_shard *shard,
    const uint8_t               *fh,
    uint16_t                     fh_len)
{
    struct chimera_vfs_open_handle *handle;

    for (int pass = 0; pass < 2; pass++) {
        for (handle = pass ? shard->detached : shard->handles; handle;
             handle = pass ? handle->next : handle->bucket_next) {
            if (handle->recallcnt && handle->fh_len == fh_len && memcmp(handle->fh, fh, fh_len) == 0) {
                return 1;
            }
        }
    }

    return 0;
} /* chimera_vfs_unlinked_has_recallable */

static enum chimera_vfs_error
chimera_vfs_unlinked_track_common(
    struct chimera_vfs                      *vfs,
    const uint8_t                           *fh,
    uint16_t                                 fh_len,
    const struct chimera_vfs_unlinked_facts *facts,
    int                                      require_recallable)
{
    struct vfs_open_cache       *cache = vfs->vfs_open_file_cache;
    struct vfs_open_cache_shard *shard;
    struct chimera_vfs_unlinked *rec;
    uint64_t fh_hash;

    if (!vfs->ref_tracking) {
        return CHIMERA_VFS_EINVAL;
    }

    fh_hash = chimera_vfs_hash(fh, fh_len);
    shard   = &cache->shards[fh_hash & cache->shard_mask];

    evpl_mutex_lock(&shard->lock);

    if (chimera_vfs_unlinked_find(shard, fh, fh_len)) {
        evpl_mutex_unlock(&shard->lock);
        return CHIMERA_VFS_OK;
    }

    if (require_recallable && !chimera_vfs_unlinked_has_recallable(shard, fh, fh_len)) {
        /* Nothing to ask back: leave the handles as they are. */
        evpl_mutex_unlock(&shard->lock);
        return CHIMERA_VFS_ENOENT;
    }

    rec          = calloc(1, sizeof(*rec));
    rec->shard   = shard;
    rec->fh_hash = fh_hash;
    rec->fh_len  = fh_len;
    rec->state   = CHIMERA_VFS_UNLINKED_TRACKED;
    memcpy(rec->fh, fh, fh_len);

    if (facts) {
        rec->have_facts = 1;
        rec->facts      = *facts;
    }

    /* Every handle of the file: attached, still pending, idle, or detached.
     * A pending open is counted too, since once its backend open lands it is an
     * ordinary reference. */
    chimera_vfs_unlinked_scan_list(rec, shard->handles, 0);
    chimera_vfs_unlinked_scan_list(rec, shard->detached, 1);

    if (rec->handles == 0) {
        evpl_mutex_unlock(&shard->lock);
        free(rec);
        return CHIMERA_VFS_ENOENT;
    }

    DL_APPEND(shard->unlinked, rec);

    /* A file whose only references are recallable is recalled as soon as it is
     * known to be unlinked: nothing else will bring its strong count to zero. */
    chimera_vfs_unlinked_queue_recall(cache, rec);

    evpl_mutex_unlock(&shard->lock);

    chimera_vfs_unlinked_event(&cache->unlinked, CHIMERA_VFS_UNLINKED_EV_TRACKED);
    chimera_vfs_unlinked_alive_add(&cache->unlinked, 1);

    return CHIMERA_VFS_OK;
} /* chimera_vfs_unlinked_track_common */

SYMBOL_EXPORT enum chimera_vfs_error
chimera_vfs_unlinked_track(
    struct chimera_vfs *vfs,
    const uint8_t      *fh,
    uint16_t            fh_len)
{
    return chimera_vfs_unlinked_track_common(vfs, fh, fh_len, NULL, 0);
} /* chimera_vfs_unlinked_track */

enum chimera_vfs_error
chimera_vfs_unlinked_track_facts(
    struct chimera_vfs                      *vfs,
    const uint8_t                           *fh,
    uint16_t                                 fh_len,
    const struct chimera_vfs_unlinked_facts *facts)
{
    return chimera_vfs_unlinked_track_common(vfs, fh, fh_len, facts, 0);
} /* chimera_vfs_unlinked_track_facts */

enum chimera_vfs_error
chimera_vfs_unlinked_track_if_recallable(
    struct chimera_vfs                      *vfs,
    const uint8_t                           *fh,
    uint16_t                                 fh_len,
    const struct chimera_vfs_unlinked_facts *facts)
{
    return chimera_vfs_unlinked_track_common(vfs, fh, fh_len, facts, 1);
} /* chimera_vfs_unlinked_track_if_recallable */

SYMBOL_EXPORT void
chimera_vfs_ref_tracking_enable(struct chimera_vfs *vfs)
{
    vfs->ref_tracking = 1;
} /* chimera_vfs_ref_tracking_enable */

SYMBOL_EXPORT bool
chimera_vfs_ref_unlinked_referenced(
    struct chimera_vfs *vfs,
    const uint8_t      *fh,
    uint16_t            fh_len)
{
    struct vfs_open_cache       *cache = vfs->vfs_open_file_cache;
    struct vfs_open_cache_shard *shard;
    struct chimera_vfs_unlinked *rec;
    bool                         referenced = false;

    if (!vfs->ref_tracking) {
        return false;
    }

    shard = &cache->shards[chimera_vfs_hash(fh, fh_len) & cache->shard_mask];

    evpl_mutex_lock(&shard->lock);

    rec = shard->unlinked ? chimera_vfs_unlinked_find(shard, fh, fh_len) : NULL;

    if (rec) {
        referenced = rec->strong > 0 || rec->recallable > 0;
    }

    evpl_mutex_unlock(&shard->lock);

    return referenced;
} /* chimera_vfs_ref_unlinked_referenced */

SYMBOL_EXPORT void
chimera_vfs_unlinked_test_info(
    struct chimera_vfs               *vfs,
    const uint8_t                    *fh,
    uint16_t                          fh_len,
    struct chimera_vfs_unlinked_info *info)
{
    struct vfs_open_cache       *cache = vfs->vfs_open_file_cache;
    struct vfs_open_cache_shard *shard;
    struct chimera_vfs_unlinked *rec;

    memset(info, 0, sizeof(*info));

    shard = &cache->shards[chimera_vfs_hash(fh, fh_len) & cache->shard_mask];

    evpl_mutex_lock(&shard->lock);

    rec = chimera_vfs_unlinked_find(shard, fh, fh_len);

    if (rec) {
        info->exists     = 1;
        info->state      = rec->state;
        info->handles    = rec->handles;
        info->strong     = rec->strong;
        info->recallable = rec->recallable;
    }

    evpl_mutex_unlock(&shard->lock);
} /* chimera_vfs_unlinked_test_info */

SYMBOL_EXPORT uint64_t
chimera_vfs_unlinked_test_event(
    struct chimera_vfs             *vfs,
    enum chimera_vfs_unlinked_event event)
{
    struct chimera_vfs_unlinked_ctl *ctl = &vfs->vfs_open_file_cache->unlinked;
    uint64_t                         value;

    evpl_mutex_lock(&ctl->lock);
    value = ctl->events[event];
    evpl_mutex_unlock(&ctl->lock);

    return value;
} /* chimera_vfs_unlinked_test_event */

SYMBOL_EXPORT void
chimera_vfs_unlinked_test_set_purge_hook(
    chimera_vfs_unlinked_test_purge_fn fn,
    void                              *arg)
{
    chimera_vfs_unlinked_purge_hook_arg = arg;
    chimera_vfs_unlinked_purge_hook     = fn;
} /* chimera_vfs_unlinked_test_set_purge_hook */

SYMBOL_EXPORT void
chimera_vfs_unlinked_test_set_step_hook(
    chimera_vfs_unlinked_test_step_fn fn,
    void                             *arg)
{
    chimera_vfs_unlinked_step_hook_arg = arg;
    chimera_vfs_unlinked_step_hook     = fn;
} /* chimera_vfs_unlinked_test_set_step_hook */

