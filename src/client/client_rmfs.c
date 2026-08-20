// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_internal.h"

static void
chimera_client_rmfs_callback(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct chimera_client_request *request       = private_data;
    struct chimera_client_thread  *client_thread = request->thread;
    chimera_rmfs_callback_t        callback      = request->rmfs.callback;
    void                          *callback_arg  = request->rmfs.private_data;

    chimera_client_request_free(client_thread, request);

    callback(client_thread, status, callback_arg);
} /* chimera_client_rmfs_callback */

SYMBOL_EXPORT void
chimera_dispatch_rmfs(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_rmfs(thread->vfs_thread,
                     &thread->client->cred,
                     request->rmfs.module_name,
                     request->rmfs.fsname,
                     chimera_client_rmfs_callback,
                     request);
} /* chimera_dispatch_rmfs */

SYMBOL_EXPORT void
chimera_rmfs(
    struct chimera_client_thread *client_thread,
    const char                   *module_name,
    const char                   *fsname,
    chimera_rmfs_callback_t       callback,
    void                         *private_data)
{
    struct chimera_client_request *request;

    request = chimera_client_request_alloc(client_thread);

    request->opcode            = CHIMERA_CLIENT_OP_RMFS;
    request->rmfs.callback     = callback;
    request->rmfs.private_data = private_data;

    memcpy(request->rmfs.module_name, module_name, strlen(module_name) + 1);
    memcpy(request->rmfs.fsname, fsname, strlen(fsname) + 1);

    chimera_dispatch_rmfs(client_thread, request);
} /* chimera_rmfs */
