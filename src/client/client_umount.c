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
    struct chimera_client_request *request       = private_data;
    struct chimera_client_thread  *client_thread = request->thread;
    chimera_umount_callback_t      callback      = request->umount.callback;
    void                          *callback_arg  = request->umount.private_data;

    chimera_client_request_free(client_thread, request);

    callback(client_thread, status, callback_arg);
} /* chimera_client_umount_callback */

SYMBOL_EXPORT void
chimera_dispatch_umount(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_umount(thread->vfs_thread,
                       request->umount.mount_path,
                       chimera_client_umount_callback,
                       request);
} /* chimera_dispatch_umount */

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

    memcpy(request->umount.mount_path, mount_path, strlen(mount_path) + 1);

    chimera_dispatch_umount(client_thread, request);
} /* chimera_client_umount */