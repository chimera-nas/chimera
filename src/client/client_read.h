// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

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
    struct chimera_client_request *request        = private_data;
    struct chimera_client_thread  *client_thread  = request->thread;
    chimera_read_callback_t        callback       = request->read.callback;
    void                          *callback_arg   = request->read.private_data;
    int                            heap_allocated = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(client_thread, request);
    }

    callback(client_thread, error_code, iov, niov, callback_arg);
} /* chimera_read_complete */

static inline void
chimera_dispatch_read(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_read(thread->vfs_thread,
                     request->read.handle,
                     request->read.offset,
                     request->read.length,
                     request->read.iov,
                     CHIMERA_CLIENT_IOV_MAX,
                     0,
                     chimera_read_complete,
                     request);
} /* chimera_dispatch_read */
