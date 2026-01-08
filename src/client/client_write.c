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
    const void                     *buf,
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
    request->write.buf          = buf;

    chimera_dispatch_write(thread, request);
} /* chimera_write */

SYMBOL_EXPORT void
chimera_writev(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        length,
    const struct iovec             *iov,
    int                             iovcnt,
    chimera_write_callback_t        callback,
    void                           *private_data)
{
    struct chimera_client_request *request;

    request = chimera_client_request_alloc(thread);

    request->opcode              = CHIMERA_CLIENT_OP_WRITE;
    request->writev.callback     = callback;
    request->writev.private_data = private_data;
    request->writev.handle       = handle;
    request->writev.offset       = offset;
    request->writev.length       = length;
    request->writev.src_iov      = iov;
    request->writev.src_iovcnt   = iovcnt;

    chimera_dispatch_writev(thread, request);
} /* chimera_writev */

SYMBOL_EXPORT void
chimera_writerv(
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

    request->opcode              = CHIMERA_CLIENT_OP_WRITE;
    request->writerv.callback     = callback;
    request->writerv.private_data = private_data;
    request->writerv.handle       = handle;
    request->writerv.offset       = offset;
    request->writerv.length       = length;
    request->writerv.niov         = niov;

    for (int i = 0; i < niov; i++) {
        evpl_iovec_move(&request->writerv.iov[i], &iov[i]);
    }

    chimera_dispatch_writerv(thread, request);
} /* chimera_writerv */
