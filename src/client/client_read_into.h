// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void
chimera_read_into_complete(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request       = private_data;
    struct chimera_client_thread  *client_thread = request->thread;
    chimera_read_into_callback_t   callback      = request->read_into.callback;
    void                          *callback_arg  = request->read_into.private_data;

    /* The data has already landed in the caller's destination buffers; iov/niov
     * just reference them.  The caller owns and releases those buffers, so we
     * release nothing here. */
    request->read_into.result_count = count;
    request->read_into.result_eof   = eof;

    chimera_client_request_free(client_thread, request);

    callback(client_thread, error_code, count, eof, callback_arg);
} /* chimera_read_into_complete */

static inline void
chimera_dispatch_read_into(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_read_into(thread->vfs_thread,
                          &thread->client->cred,
                          request->read_into.handle,
                          request->read_into.offset,
                          request->read_into.length,
                          request->read_into.iov,
                          CHIMERA_CLIENT_IOV_MAX,
                          request->read_into.dest_iov,
                          request->read_into.dest_niov,
                          0,
                          chimera_read_into_complete,
                          request);
} /* chimera_dispatch_read_into */
