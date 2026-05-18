// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void
chimera_clone_range_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request        = private_data;
    struct chimera_client_thread  *client_thread  = request->thread;
    chimera_clone_range_callback_t callback       = request->clone_range.callback;
    void                          *callback_arg   = request->clone_range.private_data;
    int                            heap_allocated = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(client_thread, request);
    }

    callback(client_thread, error_code, callback_arg);
} /* chimera_clone_range_complete */

static inline void
chimera_dispatch_clone_range(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_clone_range(
        thread->vfs_thread,
        &thread->client->cred,
        request->clone_range.src_handle,
        request->clone_range.src_offset,
        request->clone_range.dst_handle,
        request->clone_range.dst_offset,
        request->clone_range.length,
        0,
        0,
        chimera_clone_range_complete,
        request);
} /* chimera_dispatch_clone_range */
