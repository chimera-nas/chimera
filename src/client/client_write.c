// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_write.h"

SYMBOL_EXPORT void
chimera_write(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        length,
    struct evpl_iovec              *iov,
    int                             niov,
    chimera_write_callback_t        callback,
    void                           *private_data)
{
    struct chimera_client_request *request;

    request = chimera_client_request_alloc(thread);

    request->opcode             = CHIMERA_CLIENT_OP_WRITE;
    request->write.callback     = callback;
    request->write.private_data = private_data;
    request->write.handle       = handle;
    request->write.offset       = offset;
    request->write.length       = length;
    request->write.niov         = niov;

    for (int i = 0; i < niov; i++) {
        evpl_iovec_move(&request->write.iov[i], &iov[i]);
    }

    chimera_dispatch_write(thread, request);
} /* chimera_write */
