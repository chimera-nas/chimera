// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_internal.h"

static void
chimera_read_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    request->read.callback(request->thread, error_code, iov, niov, request->read.private_data);

    chimera_client_request_free(request->thread, request);
} /* chimera_read_complete */

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

    chimera_vfs_read(thread->vfs_thread, handle, offset, length,
                     request->read.iov, CHIMERA_CLIENT_IOV_MAX, 0,
                     chimera_read_complete, request);
} /* chimera_read */
