// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_internal.h"

static void
chimera_client_mkfs_callback(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct chimera_client_request *request       = private_data;
    struct chimera_client_thread  *client_thread = request->thread;
    chimera_mkfs_callback_t        callback      = request->mkfs.callback;
    void                          *callback_arg  = request->mkfs.private_data;

    chimera_client_request_free(client_thread, request);

    callback(client_thread, status, callback_arg);
} /* chimera_client_mkfs_callback */

SYMBOL_EXPORT void
chimera_dispatch_mkfs(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_mkfs(thread->vfs_thread,
                     &thread->client->cred,
                     request->mkfs.module_name,
                     request->mkfs.fsname,
                     request->mkfs.options[0] ? request->mkfs.options : NULL,
                     chimera_client_mkfs_callback,
                     request);
} /* chimera_dispatch_mkfs */

SYMBOL_EXPORT void
chimera_mkfs(
    struct chimera_client_thread *client_thread,
    const char                   *module_name,
    const char                   *fsname,
    const char                   *options,
    chimera_mkfs_callback_t       callback,
    void                         *private_data)
{
    struct chimera_client_request *request;

    request = chimera_client_request_alloc(client_thread);

    request->opcode            = CHIMERA_CLIENT_OP_MKFS;
    request->mkfs.callback     = callback;
    request->mkfs.private_data = private_data;

    memcpy(request->mkfs.module_name, module_name, strlen(module_name) + 1);
    memcpy(request->mkfs.fsname, fsname, strlen(fsname) + 1);

    if (options) {
        memcpy(request->mkfs.options, options, strlen(options) + 1);
    } else {
        request->mkfs.options[0] = '\0';
    }

    chimera_dispatch_mkfs(client_thread, request);
} /* chimera_mkfs */
