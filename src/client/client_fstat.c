// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_fstat.h"

SYMBOL_EXPORT void
chimera_fstat(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *handle,
    chimera_fstat_callback_t        callback,
    void                           *private_data)
{
    struct chimera_client_request *request;

    request = chimera_client_request_alloc(thread);

    request->opcode             = CHIMERA_CLIENT_OP_FSTAT;
    request->fstat.handle       = handle;
    request->fstat.callback     = callback;
    request->fstat.private_data = private_data;

    chimera_dispatch_fstat(thread, request);
} /* chimera_fstat */
