// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_internal.h"

static void
chimera_write_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    request->write.callback(request->thread, error_code, request->write.private_data);

    chimera_client_request_free(request->thread, request);

} /* chimera_write_complete */

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

    chimera_vfs_write(thread->vfs_thread, handle, offset, length, 1, 0, 0, iov, niov, chimera_write_complete,
                      request);

} /* chimera_write */