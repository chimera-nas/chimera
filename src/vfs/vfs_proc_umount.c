
// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <unistd.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"
#include "vfs_open_cache.h"
#include "vfs_release.h"
#include "vfs_mount_table.h"
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
    free(request->umount.mount);

} /* chimera_vfs_umount */


SYMBOL_EXPORT void
chimera_vfs_umount(
    struct chimera_vfs_thread    *thread,
    const char                   *mount_path,
    chimera_vfs_umount_callback_t callback,
    void                         *private_data)
{
    struct chimera_vfs         *vfs = thread->vfs;
    struct chimera_vfs_mount   *mount;
    struct chimera_vfs_request *request;
    const char                 *path = mount_path;
    uint64_t                    count;
    int                         max_wait_ms = 5000;
    int                         waited_ms   = 0;

    while (*path == '/') {
        path++;
    }

    /* Step 1: Set pending_umount flag on the mount.
     * This prevents new operations from using this mount. */
    mount = chimera_vfs_mount_table_set_pending_umount_by_path(vfs->mount_table, path, strlen(path));

    if (!mount) {
        callback(thread, CHIMERA_VFS_ENOENT, private_data);
        return;
    }

    /* Step 2: Mark all handles for this mount for immediate close.
     * This sets their timestamp to 0 so the close thread will close them ASAP. */
    chimera_vfs_open_cache_mark_for_close_by_mount(vfs->vfs_open_path_cache, mount->root_fh);
    chimera_vfs_open_cache_mark_for_close_by_mount(vfs->vfs_open_file_cache, mount->root_fh);

    /* Step 3: Wake the close thread to expedite handle closing */
    evpl_ring_doorbell(&vfs->close_thread.doorbell);

    /* Step 4: Wait for all handles to be closed.
     * Poll until no handles are actively referenced for this mount. */
    while (waited_ms < max_wait_ms) {
        count  = chimera_vfs_open_cache_count_by_mount(vfs->vfs_open_path_cache, mount->root_fh);
        count += chimera_vfs_open_cache_count_by_mount(vfs->vfs_open_file_cache, mount->root_fh);

        if (count == 0) {
            break;
        }

        /* Wake close thread again and wait a bit */
        evpl_ring_doorbell(&vfs->close_thread.doorbell);
        usleep(1000); /* 1ms */
        waited_ms++;
    }

    if (waited_ms >= max_wait_ms) {
        chimera_vfs_error("umount: timed out waiting for %lu handles to close for mount %s",
                          count, path);
    }

    /* Step 5: Remove mount from table now that handles are closed */
    mount = chimera_vfs_mount_table_remove_by_path(vfs->mount_table, path, strlen(path));

    if (!mount) {
        /* This shouldn't happen since we just found it, but handle gracefully */
        callback(thread, CHIMERA_VFS_ENOENT, private_data);
        return;
    }

    /* Step 6: Dispatch umount to the module */
    request = chimera_vfs_request_alloc(thread, mount->root_fh, mount->root_fh_len);

    /* For umount operations, the mount was already removed from the table,
     * so chimera_vfs_get_module returns NULL. Set the module directly. */
    request->module = mount->module;

    request->opcode               = CHIMERA_VFS_OP_UMOUNT;
    request->complete             = chimera_vfs_umount_complete;
    request->umount.mount         = mount;
    request->umount.mount_private = mount->mount_private;
    request->proto_callback       = callback;
    request->proto_private_data   = private_data;

    chimera_vfs_dispatch(request);

} /* chimera_vfs_umount */