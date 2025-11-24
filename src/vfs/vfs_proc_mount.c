// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"
#include "vfs_open_cache.h"
#include "vfs_release.h"
#include "common/macros.h"


static void
chimera_vfs_mount_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread   *thread = request->thread;
    struct chimera_vfs          *vfs    = thread->vfs;
    struct chimera_vfs_mount    *mount;
    chimera_vfs_mount_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    if (request->status != CHIMERA_VFS_OK) {
        callback(thread, request->status, request->proto_private_data);
        return;
    }

    mount = calloc(1, sizeof(*mount));

    mount->module        = request->mount.module;
    mount->path          = strdup(request->mount.mount_path);
    mount->pathlen       = strlen(request->mount.mount_path);
    mount->mount_private = request->mount.r_mount_private;

    memcpy(mount->fh, request->mount.r_attr.va_fh, request->mount.r_attr.va_fh_len);
    mount->fhlen = request->mount.r_attr.va_fh_len;

    pthread_rwlock_wrlock(&vfs->mounts_lock);
    DL_APPEND(vfs->mounts, mount);
    pthread_rwlock_unlock(&vfs->mounts_lock);

    callback(thread, CHIMERA_VFS_OK, request->proto_private_data);

    chimera_vfs_request_free(thread, request);

} /* chimera_vfs_mount_complete */

SYMBOL_EXPORT void
chimera_vfs_mount(
    struct chimera_vfs_thread   *thread,
    const char                  *mount_path,
    const char                  *module_name,
    const char                  *module_path,
    chimera_vfs_mount_callback_t callback,
    void                        *private_data)
{
    struct chimera_vfs         *vfs    = thread->vfs;
    struct chimera_vfs_module  *module = NULL;
    int                         i;
    struct chimera_vfs_request *request;

    while (mount_path[0] == '/') {
        mount_path++;
    }

    for (i = 0; i < CHIMERA_VFS_FH_MAGIC_MAX; i++) {
        module = vfs->modules[i];

        if (!module) {
            continue;
        }

        if (strcmp(module->name, module_name) == 0) {
            break;
        }
    }

    if (i == CHIMERA_VFS_FH_MAGIC_MAX) {
        chimera_vfs_error("chimera_vfs_mount: module %s not found",
                          module_name);
        callback(thread, CHIMERA_VFS_ENOENT, private_data);
        return;
    }

    request = chimera_vfs_request_alloc(thread, &module->fh_magic, 1);

    request->opcode                   = CHIMERA_VFS_OP_MOUNT;
    request->complete                 = chimera_vfs_mount_complete;
    request->mount.path               = module_path;
    request->mount.pathlen            = strlen(module_path);
    request->mount.module             = module;
    request->mount.mount_path         = mount_path;
    request->mount.mount_pathlen      = strlen(mount_path);
    request->mount.r_attr.va_req_mask = CHIMERA_VFS_ATTR_MASK_CACHEABLE | CHIMERA_VFS_ATTR_FH;
    request->mount.r_attr.va_set_mask = 0;
    request->proto_callback           = callback;
    request->proto_private_data       = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_mount */