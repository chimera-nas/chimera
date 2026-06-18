// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void
chimera_seek_complete(
    enum chimera_vfs_error error_code,
    int                    eof,
    uint64_t               offset,
    void                  *private_data)
{
    struct chimera_client_request *request        = private_data;
    struct chimera_client_thread  *client_thread  = request->thread;
    chimera_seek_callback_t        callback       = request->seek.callback;
    void                          *callback_arg   = request->seek.private_data;
    int                            heap_allocated = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(client_thread, request);
    }

    callback(client_thread, error_code, eof, offset, callback_arg);
} /* chimera_seek_complete */

static inline void
chimera_dispatch_seek(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_seek(
        thread->vfs_thread,
        chimera_client_req_cred(request),
        request->seek.handle,
        request->seek.offset,
        request->seek.what,
        chimera_seek_complete,
        request);
} /* chimera_dispatch_seek */
