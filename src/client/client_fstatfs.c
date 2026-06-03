// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_fstatfs.h"

SYMBOL_EXPORT void
chimera_fstatfs(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *handle,
    chimera_fstatfs_callback_t      callback,
    void                           *private_data)
{
    struct chimera_client_request *request;

    request = chimera_client_request_alloc(thread);

    request->opcode               = CHIMERA_CLIENT_OP_FSTATFS;
    request->fstatfs.handle       = handle;
    request->fstatfs.callback     = callback;
    request->fstatfs.private_data = private_data;

    chimera_dispatch_fstatfs(thread, request);
} /* chimera_fstatfs */
