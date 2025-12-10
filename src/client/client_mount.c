// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_internal.h"

static void
chimera_client_mount_callback(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct chimera_client_request *request = private_data;

    request->mount.callback(request->thread, status, request->mount.private_data);

    chimera_client_request_free(request->thread, request);
} /* chimera_client_mount_callback */

SYMBOL_EXPORT void
chimera_mount(
    struct chimera_client_thread *client_thread,
    const char                   *mount_path,
    const char                   *module_name,
    const char                   *module_path,
    chimera_mount_callback_t      callback,
    void                         *private_data)
{
    struct chimera_client_request *request;

    request = chimera_client_request_alloc(client_thread);

    request->opcode             = CHIMERA_CLIENT_OP_MOUNT;
    request->mount.callback     = callback;
    request->mount.private_data = private_data;

    memcpy(request->mount.module_path, module_path, strlen(module_path) + 1);
    memcpy(request->mount.mount_path, mount_path, strlen(mount_path) + 1);
    memcpy(request->mount.module_name, module_name, strlen(module_name) + 1);

    chimera_vfs_mount(client_thread->vfs_thread,
                      request->mount.mount_path,
                      request->mount.module_name,
                      request->mount.
                      module_path,
                      chimera_client_mount_callback,
                      request);
} /* chimera_client_mount */
