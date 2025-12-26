// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_readdir.h"

SYMBOL_EXPORT void
chimera_readdir(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    chimera_readdir_callback_t      callback,
    chimera_readdir_complete_t      complete,
    void                           *private_data)
{
    struct chimera_client_request *request;

    request = chimera_client_request_alloc(thread);

    request->opcode               = CHIMERA_CLIENT_OP_READDIR;
    request->readdir.callback     = callback;
    request->readdir.complete     = complete;
    request->readdir.private_data = private_data;
    request->readdir.handle       = handle;
    request->readdir.cookie       = cookie;

    chimera_dispatch_readdir(thread, request);
} /* chimera_readdir */
