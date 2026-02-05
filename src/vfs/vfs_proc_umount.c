
// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
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

    mount = chimera_vfs_mount_table_remove_by_path(vfs->mount_table, path, strlen(path));

    if (!mount) {
        callback(thread, CHIMERA_VFS_ENOENT, private_data);
        return;
    }

    /* For umount operations, the mount was already removed from the table,
     * so chimera_vfs_get_module returns NULL. Use alloc_with_module. */
    request = chimera_vfs_request_alloc_with_module(thread, cred,
                                                    mount->root_fh, mount->root_fh_len,
                                                    chimera_vfs_hash(mount->root_fh, mount->root_fh_len),
                                                    mount->module);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(thread, CHIMERA_VFS_PTR_ERR(request), private_data);
        return;
    }

    request->opcode               = CHIMERA_VFS_OP_UMOUNT;
    request->complete             = chimera_vfs_umount_complete;
    request->umount.mount         = mount;
    request->umount.mount_private = mount->mount_private;
    request->proto_callback       = callback;
    request->proto_private_data   = private_data;

    chimera_vfs_dispatch(request);

} /* chimera_vfs_umount */