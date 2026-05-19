// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void
chimera_copy_range_complete(
    enum chimera_vfs_error    error_code,
    uint64_t                  length,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request        = private_data;
    struct chimera_client_thread  *client_thread  = request->thread;
    chimera_copy_range_callback_t  callback       = request->copy_range.callback;
    void                          *callback_arg   = request->copy_range.private_data;
    int                            heap_allocated = request->heap_allocated;

    request->copy_range.r_length = length;

    if (heap_allocated) {
        chimera_client_request_free(client_thread, request);
    }

    callback(client_thread, error_code, length, callback_arg);
} /* chimera_copy_range_complete */

static inline void
chimera_dispatch_copy_range(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_copy_range(
        thread->vfs_thread,
        &thread->client->cred,
        request->copy_range.src_handle,
        request->copy_range.src_offset,
        request->copy_range.dst_handle,
        request->copy_range.dst_offset,
        request->copy_range.length,
        0,
        0,
        chimera_copy_range_complete,
        request);
} /* chimera_dispatch_copy_range */
