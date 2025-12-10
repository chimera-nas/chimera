// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_internal.h"

static void
chimera_client_umount_callback(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct chimera_client_request *request = private_data;

    request->umount.callback(request->thread, status, request->umount.private_data);

    chimera_client_request_free(request->thread, request);
} /* chimera_client_mount_callback */

SYMBOL_EXPORT void
chimera_umount(
    struct chimera_client_thread *client_thread,
    const char                   *mount_path,
    chimera_umount_callback_t     callback,
    void                         *private_data)
{
    struct chimera_client_request *request;

    request = chimera_client_request_alloc(client_thread);

    request->opcode              = CHIMERA_CLIENT_OP_UMOUNT;
    request->umount.callback     = callback;
    request->umount.private_data = private_data;

    chimera_vfs_umount(client_thread->vfs_thread, mount_path, chimera_client_umount_callback, request);
} /* chimera_client_mount */