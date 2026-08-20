// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"

static void
chimera_vfs_rmfs_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread  *thread   = request->thread;
    chimera_vfs_rmfs_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(thread, request->status, request->proto_private_data);

    chimera_vfs_request_free(thread, request);
} /* chimera_vfs_rmfs_complete */

SYMBOL_EXPORT void
chimera_vfs_rmfs(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const char                    *module_name,
    const char                    *fsname,
    chimera_vfs_rmfs_callback_t    callback,
    void                          *private_data)
{
    struct chimera_vfs         *vfs    = thread->vfs;
    struct chimera_vfs_module  *module = NULL;
    int                         i;
    struct chimera_vfs_request *request;

    for (i = 0; i < CHIMERA_VFS_MAX_MODULES; i++) {
        module = vfs->modules[i];

        if (!module) {
            continue;
        }

        if (strcmp(module->name, module_name) == 0) {
            break;
        }
    }

    if (i == CHIMERA_VFS_MAX_MODULES) {
        chimera_vfs_error("chimera_vfs_rmfs: module %s not found",
                          module_name);
        callback(thread, CHIMERA_VFS_ENOENT, private_data);
        return;
    }

    if (!(module->capabilities & CHIMERA_VFS_CAP_MKFS)) {
        callback(thread, CHIMERA_VFS_ENOTSUP, private_data);
        return;
    }

    if (!fsname || !fsname[0] || strchr(fsname, '/')) {
        chimera_vfs_error("chimera_vfs_rmfs: invalid filesystem name: %s",
                          fsname ? fsname : "(null)");
        callback(thread, CHIMERA_VFS_EINVAL, private_data);
        return;
    }

    request = chimera_vfs_request_alloc_with_module(thread, cred,
                                                    &module->fh_magic, 1,
                                                    chimera_vfs_hash(&module->fh_magic, 1),
                                                    module);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(thread, CHIMERA_VFS_PTR_ERR(request), private_data);
        return;
    }

    request->opcode             = CHIMERA_VFS_OP_RMFS;
    request->complete           = chimera_vfs_rmfs_complete;
    request->rmfs.name          = fsname;
    request->rmfs.namelen       = strlen(fsname);
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_rmfs */
