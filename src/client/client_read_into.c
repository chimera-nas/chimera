// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_read_into.h"

SYMBOL_EXPORT void
chimera_read_into(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        length,
    struct evpl_iovec              *iov,
    int                             niov,
    chimera_read_into_callback_t    callback,
    void                           *private_data)
{
    struct chimera_client_request *request;

    request = chimera_client_request_alloc(thread);

    request->opcode                 = CHIMERA_CLIENT_OP_READ;
    request->read_into.callback     = callback;
    request->read_into.private_data = private_data;
    request->read_into.handle       = handle;
    request->read_into.offset       = offset;
    request->read_into.length       = length;
    request->read_into.dest_niov    = niov;

    /* Borrow the caller's destination buffers: shallow-copy the iovec structs
     * (same underlying buffer) without taking a ref.  The caller guarantees the
     * buffers stay alive until the callback. */
    for (int i = 0; i < niov; i++) {
        request->read_into.dest_iov[i] = iov[i];
    }

    chimera_dispatch_read_into(thread, request);
} /* chimera_read_into */
