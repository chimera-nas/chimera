// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_read.h"

SYMBOL_EXPORT void
chimera_read(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        length,
    chimera_read_callback_t         callback,
    void                           *private_data)
{
    struct chimera_client_request *request;

    request = chimera_client_request_alloc(thread);

    request->opcode            = CHIMERA_CLIENT_OP_READ;
    request->read.callback     = callback;
    request->read.private_data = private_data;
    request->read.handle       = handle;
    request->read.offset       = offset;
    request->read.length       = length;

    chimera_dispatch_read(thread, request);
} /* chimera_read */
