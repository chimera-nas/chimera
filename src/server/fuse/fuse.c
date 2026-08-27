// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <utlist.h>

#include "fuse.h"
#include "fuse_internal.h"
#include "common/macros.h"
#include "server/server.h"
#include "vfs/vfs_release.h"

static void *
fuse_server_init(
    const struct chimera_server_config *config,
    struct chimera_vfs                 *vfs,
    struct prometheus_metrics          *metrics)
{
    struct chimera_fuse_shared *shared;

    if (!chimera_server_config_get_fuse_enabled(config)) {
        chimera_fuse_info("FUSE server disabled (not enabled in config)");
        return NULL;
    }

    shared = calloc(1, sizeof(*shared));

    shared->vfs     = vfs;
    shared->metrics = metrics;

    pthread_mutex_init(&shared->lock, NULL);
    pthread_mutex_init(&shared->notifier_lock, NULL);
    pthread_cond_init(&shared->notifier_cond, NULL);

    return shared;
} /* fuse_server_init */

static void
fuse_server_destroy(void *data)
{
    struct chimera_fuse_shared *shared = data;
    struct chimera_fuse_mount  *mount;
    int                         m, slot;

    for (m = 0; m < shared->num_mounts; m++) {
        mount = &shared->mounts[m];

        for (slot = 0; slot < CHIMERA_FUSE_MAX_THREADS; slot++) {
            if (mount->mounted && mount->channel_fds[slot] >= 0) {
                close(mount->channel_fds[slot]);
            }
        }

        chimera_fuse_coherence_shutdown(shared, mount);

        if (mount->node_table) {
            chimera_fuse_node_table_destroy(mount->node_table);
        }

        pthread_mutex_destroy(&mount->open_lock);
        pthread_mutex_destroy(&mount->lock_lock);
        pthread_mutex_destroy(&mount->grant_lock);
    }

    pthread_mutex_destroy(&shared->lock);
    pthread_mutex_destroy(&shared->notifier_lock);
    pthread_cond_destroy(&shared->notifier_cond);

    free(shared);
} /* fuse_server_destroy */

static void
fuse_server_start(void *data)
{
    struct chimera_fuse_shared *shared = data;
    int                         m, t, rc;

    shared->started = 1;

    for (m = 0; m < shared->num_mounts; m++) {
        rc = chimera_fuse_mount_setup(shared, &shared->mounts[m]);

        chimera_fuse_abort_if(rc != 0, "failed to establish FUSE mount %s",
                              shared->mounts[m].mountpoint);
    }

    for (t = 0; t < shared->num_threads; t++) {
        evpl_ring_doorbell(&shared->threads[t]->attach_doorbell);
    }

    chimera_fuse_notifier_start(shared);
} /* fuse_server_start */

static void
fuse_server_stop(void *data)
{
    struct chimera_fuse_shared *shared = data;
    int                         m;

    for (m = 0; m < shared->num_mounts; m++) {
        chimera_fuse_mount_teardown(&shared->mounts[m]);
    }

    /* The kernel can no longer send anything; cancel parked blocking locks
     * (their EINTR replies drain on the still-live pool threads) and drop
     * every granted lock lease. */
    for (m = 0; m < shared->num_mounts; m++) {
        chimera_fuse_locks_shutdown(shared, &shared->mounts[m]);
    }

    /* Drains queued invalidations (their acks matter; the writes are
     * harmless no-ops on the detached mounts), then exits. */
    chimera_fuse_notifier_stop(shared);
} /* fuse_server_stop */

/*
 * Rung by start() once every mount's channels exist: each thread claims the
 * channel fd for its slot on every mount and arms it in its own event loop.
 */
static void
chimera_fuse_attach_channels(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_fuse_thread  *thread = container_of(doorbell, struct chimera_fuse_thread, attach_doorbell);
    struct chimera_fuse_shared  *shared = thread->shared;
    struct chimera_fuse_mount   *mount;
    struct chimera_fuse_channel *channel;
    int                          m, c, fd, attached;

    for (m = 0; m < shared->num_mounts; m++) {
        mount = &shared->mounts[m];

        if (!mount->mounted || mount->dead) {
            continue;
        }

        fd = mount->channel_fds[thread->thread_slot];

        if (fd < 0) {
            continue;
        }

        attached = 0;

        for (c = 0; c < thread->num_channels; c++) {
            if (thread->channels[c].mount == mount) {
                attached = 1;
                break;
            }
        }

        if (attached) {
            continue;
        }

        channel = &thread->channels[thread->num_channels++];

        channel->thread = thread;
        channel->mount  = mount;
        channel->fd     = fd;
        channel->dead   = 0;

        evpl_add_fd_event(evpl, &channel->event, fd,
                          chimera_fuse_channel_readable,
                          NULL,
                          chimera_fuse_channel_error);

        evpl_fd_event_read_interest(evpl, &channel->event);

        channel->armed = 1;
    }
} /* chimera_fuse_attach_channels */

static void *
fuse_server_thread_init(
    struct evpl               *evpl,
    struct chimera_vfs_thread *vfs_thread,
    void                      *data)
{
    struct chimera_fuse_shared *shared = data;
    struct chimera_fuse_thread *thread;

    thread = calloc(1, sizeof(*thread));

    thread->evpl       = evpl;
    thread->vfs_thread = vfs_thread;
    thread->shared     = shared;

    pthread_mutex_lock(&shared->lock);

    chimera_fuse_abort_if(shared->num_threads >= CHIMERA_FUSE_MAX_THREADS,
                          "too many core threads for FUSE server (max %d)",
                          CHIMERA_FUSE_MAX_THREADS);

    thread->thread_slot                  = shared->num_threads;
    shared->threads[shared->num_threads] = thread;
    shared->num_threads++;
    shared->threads_alive++;

    pthread_mutex_unlock(&shared->lock);

    pthread_mutex_init(&thread->resume_lock, NULL);

    evpl_add_doorbell(evpl, &thread->attach_doorbell, chimera_fuse_attach_channels);
    evpl_add_doorbell(evpl, &thread->resume_doorbell, chimera_fuse_resume_doorbell);

    return thread;
} /* fuse_server_thread_init */

/*
 * Release the VFS handles of files the kernel never sent RELEASE for.  Runs
 * on the last thread to shut down: every other thread has already drained,
 * and the mounts are detached, so nothing else references these handles.
 */
static void
chimera_fuse_sweep_open_files(
    struct chimera_fuse_shared *shared,
    struct chimera_vfs_thread  *vfs_thread)
{
    struct chimera_fuse_mount     *mount;
    struct chimera_fuse_open_file *file, *files;
    int                            m;

    for (m = 0; m < shared->num_mounts; m++) {
        mount = &shared->mounts[m];

        pthread_mutex_lock(&mount->open_lock);
        files             = mount->open_files;
        mount->open_files = NULL;
        pthread_mutex_unlock(&mount->open_lock);

        while (files) {
            file = files;
            DL_DELETE(files, file);

            chimera_fuse_info("fuse mount %s: releasing handle the kernel never closed",
                              mount->mountpoint);

            chimera_vfs_release(vfs_thread, file->handle);
            free(file);
        }
    }
} /* chimera_fuse_sweep_open_files */

static void
fuse_server_thread_destroy(void *data)
{
    struct chimera_fuse_thread  *thread = data;
    struct chimera_fuse_shared  *shared = thread->shared;
    struct chimera_fuse_request *req;
    int                          c, last;

    for (c = 0; c < thread->num_channels; c++) {
        if (thread->channels[c].armed) {
            evpl_remove_fd_event(thread->evpl, &thread->channels[c].event);
            thread->channels[c].armed = 0;
        }
    }

    chimera_fuse_abort_if(thread->active_requests,
                          "fuse thread destroyed with %d active requests",
                          thread->active_requests);

    while (thread->free_requests) {
        req                   = thread->free_requests;
        thread->free_requests = req->next;

        if (req->buf_allocated) {
            evpl_iovec_release(thread->evpl, &req->buf);
        }
        free(req);
    }

    evpl_remove_doorbell(thread->evpl, &thread->attach_doorbell);
    evpl_remove_doorbell(thread->evpl, &thread->resume_doorbell);

    pthread_mutex_destroy(&thread->resume_lock);

    pthread_mutex_lock(&shared->lock);
    last = (--shared->threads_alive == 0);
    pthread_mutex_unlock(&shared->lock);

    if (last) {
        chimera_fuse_sweep_open_files(shared, thread->vfs_thread);
    }

    free(thread);
} /* fuse_server_thread_destroy */

SYMBOL_EXPORT int
chimera_fuse_add_mount(
    void       *fuse_shared,
    const char *mountpoint,
    const char *path,
    const char *options)
{
    struct chimera_fuse_shared *shared = fuse_shared;
    struct chimera_fuse_mount  *mount;
    char                       *opts, *opt, *saveptr = NULL;
    int                         rc = 0;

    if (!shared) {
        return -1;
    }

    if (shared->started) {
        chimera_fuse_error("FUSE mount %s added after server start", mountpoint);
        return -1;
    }

    if (shared->num_mounts >= CHIMERA_FUSE_MAX_MOUNTS) {
        chimera_fuse_error("too many FUSE mounts (max %d)", CHIMERA_FUSE_MAX_MOUNTS);
        return -1;
    }

    mount = &shared->mounts[shared->num_mounts];

    if (strlen(mountpoint) >= sizeof(mount->mountpoint) ||
        strlen(path) >= sizeof(mount->share_path)) {
        chimera_fuse_error("FUSE mount %s: mountpoint or path too long", mountpoint);
        return -1;
    }

    memset(mount, 0, sizeof(*mount));

    strncpy(mount->mountpoint, mountpoint, sizeof(mount->mountpoint) - 1);
    strncpy(mount->share_path, path, sizeof(mount->share_path) - 1);

    mount->shared              = shared;
    mount->synthetic_fd        = -1;
    mount->default_permissions = 1;
    mount->attr_timeout_ms     = 1000;
    mount->entry_timeout_ms    = 1000;
    /* Fully coherent by default: kernel caches are only trusted while covered
     * by a live grant/watch, and a conflicting mutation elsewhere completes
     * only after this kernel's caches are invalidated.  coherence=ttl opts
     * into the async model where the timeouts alone bound staleness. */
    mount->coherence_sync      = 1;
    mount->negative_timeout_ms = UINT32_MAX; /* default resolved below */

    if (options && *options) {
        opts = strdup(options);

        for (opt = strtok_r(opts, ",", &saveptr);
             opt;
             opt = strtok_r(NULL, ",", &saveptr)) {

            if (strcmp(opt, "allow_other") == 0) {
                mount->allow_other = 1;
            } else if (strcmp(opt, "no_default_permissions") == 0) {
                mount->default_permissions = 0;
            } else if (strncmp(opt, "attr_timeout_ms=", 16) == 0) {
                mount->attr_timeout_ms = strtoul(opt + 16, NULL, 10);
            } else if (strncmp(opt, "entry_timeout_ms=", 17) == 0) {
                mount->entry_timeout_ms = strtoul(opt + 17, NULL, 10);
            } else if (strncmp(opt, "negative_timeout_ms=", 20) == 0) {
                mount->negative_timeout_ms = strtoul(opt + 20, NULL, 10);
            } else if (strcmp(opt, "coherence=sync") == 0) {
                mount->coherence_sync = 1;
            } else if (strcmp(opt, "coherence=ttl") == 0) {
                mount->coherence_sync = 0;
            } else if (strcmp(opt, "direct_io") == 0) {
                mount->direct_io = 1;
            } else if (strcmp(opt, "parallel_direct_writes") == 0) {
                mount->parallel_direct_writes = 1;
            } else {
                chimera_fuse_error("FUSE mount %s: unknown option '%s'",
                                   mountpoint, opt);
                rc = -1;
                break;
            }
        }

        free(opts);

        if (rc != 0) {
            return rc;
        }
    }

    /* Negative dentries are safe to cache when namespace mutations complete
     * synchronously against our watches (coherence=sync); without that
     * interlock a cached ENOENT could outlive a foreign create, so ttl mode
     * defaults them off unless explicitly enabled. */
    if (mount->negative_timeout_ms == UINT32_MAX) {
        mount->negative_timeout_ms =
            mount->coherence_sync ? mount->entry_timeout_ms : 0;
    }

    pthread_mutex_init(&mount->open_lock, NULL);
    pthread_mutex_init(&mount->dir_notifier_lock, NULL);
    pthread_cond_init(&mount->dir_notifier_cond, NULL);
    pthread_mutex_init(&mount->lock_lock, NULL);
    pthread_mutex_init(&mount->grant_lock, NULL);

    shared->num_mounts++;

    return 0;
} /* chimera_fuse_add_mount */

SYMBOL_EXPORT int
chimera_fuse_add_synthetic_mount(
    void       *fuse_shared,
    const char *path,
    int         fd)
{
    struct chimera_fuse_shared *shared = fuse_shared;
    struct chimera_fuse_mount  *mount;
    int                         rc;

    rc = chimera_fuse_add_mount(fuse_shared, "(simulated)", path, NULL);

    if (rc != 0) {
        return rc;
    }

    mount               = &shared->mounts[shared->num_mounts - 1];
    mount->synthetic_fd = fd;

    return 0;
} /* chimera_fuse_add_synthetic_mount */

SYMBOL_EXPORT struct chimera_server_protocol fuse_protocol = {
    .init           = fuse_server_init,
    .destroy        = fuse_server_destroy,
    .start          = fuse_server_start,
    .stop           = fuse_server_stop,
    .thread_init    = fuse_server_thread_init,
    .thread_destroy = fuse_server_thread_destroy,
};
