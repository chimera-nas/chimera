
// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "evpl/evpl_timer.h"
#include "vfs_mount_table.h"
#include "vfs_open_cache.h"
#include "vfs_state.h"
#include "common/macros.h"


static void
chimera_vfs_umount_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread    *thread   = request->thread;
    chimera_vfs_umount_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(thread, CHIMERA_VFS_OK, request->proto_private_data);

    chimera_vfs_request_free(thread, request);

    free(request->umount.mount->path);
    free(request->umount.mount->module_path);
    free(request->umount.mount->options);
    free(request->umount.mount);

} /* chimera_vfs_umount */


/*
 * How often umount re-checks for handles it does not own to be dropped.  The
 * wait exists for the millisecond-scale tail of ordinary activity -- a path
 * walk's reference on the mount root outlives its operation -- not for a
 * client that is genuinely holding a file open, which is why it gives up
 * after common.umount_timeout_ms rather than waiting forever.
 */
#define CHIMERA_VFS_UMOUNT_POLL_US 1000

struct chimera_vfs_umount_wait {
    struct evpl_timer           timer;
    struct chimera_vfs_request *request;
    uint64_t                    waited_us;
};

static void chimera_vfs_umount_progress(
    struct chimera_vfs_request *request);

/*
 * Every handle on this mount is gone and every close they needed has run;
 * unlink it and hand the backend its UMOUNT.
 */
static void
chimera_vfs_umount_dispatch(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread *thread = request->thread;
    struct chimera_vfs        *vfs    = thread->vfs;
    struct chimera_vfs_mount  *mount  = request->umount.mount;

    chimera_vfs_mount_table_remove_by_path(vfs->mount_table, mount->path,
                                           mount->pathlen);

    chimera_vfs_dispatch(request);
} /* chimera_vfs_umount_dispatch */

/* Give up: hand the mount back and report it busy. */
static void
chimera_vfs_umount_abandon(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread    *thread   = request->thread;
    chimera_vfs_umount_callback_t callback = request->proto_callback;
    void                         *arg      = request->proto_private_data;

    request->umount.mount->unmounting = 0;

    chimera_vfs_request_free(thread, request);

    callback(thread, CHIMERA_VFS_EBUSY, arg);
} /* chimera_vfs_umount_abandon */

static void
chimera_vfs_umount_wait_timer(
    struct evpl       *evpl,
    struct evpl_timer *timer)
{
    struct chimera_vfs_umount_wait *wait =
        container_of(timer, struct chimera_vfs_umount_wait, timer);

    wait->waited_us += CHIMERA_VFS_UMOUNT_POLL_US;

    chimera_vfs_umount_progress(wait->request);
} /* chimera_vfs_umount_wait_timer */

static void
chimera_vfs_umount_wait_stop(struct chimera_vfs_request *request)
{
    struct chimera_vfs_umount_wait *wait = request->umount.wait;

    if (wait) {
        evpl_remove_timer(request->thread->evpl, &wait->timer);
        free(wait);
        request->umount.wait = NULL;
    }
} /* chimera_vfs_umount_wait_stop */

static void
chimera_vfs_umount_close_callback(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct chimera_vfs_request *request = private_data;

    if (--request->umount.pending_closes == 0) {
        chimera_vfs_umount_progress(request);
    }
} /* chimera_vfs_umount_close_callback */

/*
 * Sweep one cache: dispose of the handles nobody is using and report how many
 * someone still is.
 *
 * The mount is deliberately still in the table while this runs, so the closes
 * issued here resolve it exactly as any other op would -- which is what lets a
 * backend treat "the mount my close names is still live" as an invariant
 * rather than a hope.
 */
static uint64_t
chimera_vfs_umount_sweep_cache(
    struct chimera_vfs_request *request,
    struct vfs_open_cache      *cache)
{
    struct chimera_vfs_thread      *thread = request->thread;
    struct chimera_vfs_mount       *mount  = request->umount.mount;
    struct chimera_vfs_open_handle *purged  = NULL, *handle;
    uint64_t                        referenced;

    referenced = chimera_vfs_open_cache_purge_by_mount(cache, mount->root_fh,
                                                       &purged);

    while (purged) {
        handle = purged;
        LL_DELETE(purged, handle);

        /* A handle with no backend open has nothing to close; dropping it
         * here is the whole of its teardown. */
        if (chimera_vfs_open_handle_needs_backend_close(handle)) {
            request->umount.pending_closes++;

            chimera_vfs_close(thread,
                              handle->vfs_module,
                              handle->fh,
                              handle->fh_len,
                              handle->vfs_private,
                              handle->fh_hash,
                              chimera_vfs_umount_close_callback,
                              request);
        }

        /* purge_by_mount took the handle out of the cache but left the struct
         * to us, as defer_close does for the close thread. */
        chimera_vfs_file_state_release(handle->file_state);

        free(handle);
    }

    return referenced;
} /* chimera_vfs_umount_sweep_cache */

/*
 * Drive the umount forward: sweep both caches, and finish once nothing on the
 * mount remains.  Re-entered from a close completion or the poll timer, so it
 * is written to be safe to call repeatedly.
 */
static void
chimera_vfs_umount_progress(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread      *thread = request->thread;
    struct chimera_vfs             *vfs    = thread->vfs;
    struct chimera_vfs_umount_wait *wait;
    uint64_t                        referenced;

    /* Held across the sweeps so a close completing inline cannot finish the
     * umount while handles are still being issued. */
    request->umount.pending_closes++;

    referenced  = chimera_vfs_umount_sweep_cache(request, vfs->vfs_open_file_cache);
    referenced += chimera_vfs_umount_sweep_cache(request, vfs->vfs_open_path_cache);

    request->umount.pending_closes--;

    if (request->umount.pending_closes) {
        /* Closes still in flight; their completion re-enters here, so no
         * timer is armed against them.  The timeout below bounds the wait on
         * references we do not own, which is the case that arises in normal
         * operation.  It deliberately does not bound this one: a close that
         * never completes is a backend that still owns this request, and
         * giving up on it would free memory the backend is about to touch.
         * chimera_vfs_close always runs its callback, including inline when
         * it cannot allocate, so the only way to stay here is that bug. */
        return;
    }

    if (referenced == 0) {
        chimera_vfs_umount_wait_stop(request);
        chimera_vfs_umount_dispatch(request);
        return;
    }

    /* Someone else still holds a handle.  Wait for them to drop it. */
    wait = request->umount.wait;

    if (!wait) {
        wait                 = calloc(1, sizeof(*wait));
        wait->request        = request;
        request->umount.wait = wait;
    } else if (wait->waited_us >= vfs->umount_timeout_us) {
        chimera_vfs_umount_wait_stop(request);
        chimera_vfs_umount_abandon(request);
        return;
    }

    /* One-shot rather than periodic: evpl takes a one-shot out of the timer
     * set before running its callback, so the callback below is free to end
     * the wait and release this struct.  A periodic timer is re-armed after
     * the callback returns, which would be a use-after-free. */
    evpl_add_oneshot_timer(thread->evpl, &wait->timer,
                           chimera_vfs_umount_wait_timer,
                           CHIMERA_VFS_UMOUNT_POLL_US);
} /* chimera_vfs_umount_progress */

SYMBOL_EXPORT void
chimera_vfs_umount(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const char                    *mount_path,
    chimera_vfs_umount_callback_t  callback,
    void                          *private_data)
{
    struct chimera_vfs         *vfs = thread->vfs;
    struct chimera_vfs_mount   *mount;
    struct chimera_vfs_request *request;
    const char                 *path = mount_path;

    while (*path == '/') {
        path++;
    }

    mount = chimera_vfs_mount_table_find_exact(vfs->mount_table, path, strlen(path));

    if (!mount) {
        callback(thread, CHIMERA_VFS_ENOENT, private_data);
        return;
    }

    if (mount->unmounting) {
        callback(thread, CHIMERA_VFS_EBUSY, private_data);
        return;
    }

    /* Claim the mount before counting, so the count can only fall afterwards.
     * Counting first would let an open land in between and leave a handle
     * referencing a mount that is already on its way out. */
    mount->unmounting = 1;

    /* The mount stays in the table until the drain below finishes.
     * chimera_vfs_get_module now refuses it, so nothing new routes here, but
     * the umount and its closes carry their module explicitly and proceed. */
    request = chimera_vfs_request_alloc_with_module(thread, cred,
                                                    mount->root_fh, mount->root_fh_len,
                                                    chimera_vfs_hash(mount->root_fh, mount->root_fh_len),
                                                    mount->module);

    if (CHIMERA_VFS_IS_ERR(request)) {
        mount->unmounting = 0;
        callback(thread, CHIMERA_VFS_PTR_ERR(request), private_data);
        return;
    }

    request->opcode               = CHIMERA_VFS_OP_UMOUNT;
    request->complete             = chimera_vfs_umount_complete;
    request->umount.mount         = mount;
    request->umount.mount_private = mount->mount_private;
    request->proto_callback       = callback;
    request->proto_private_data   = private_data;

    request->umount.pending_closes = 0;
    request->umount.wait           = NULL;

    chimera_vfs_umount_progress(request);

} /* chimera_vfs_umount */