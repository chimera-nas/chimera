// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void
chimera_allocate_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request        = private_data;
    struct chimera_client_thread  *client_thread  = request->thread;
    chimera_commit_callback_t      callback       = request->allocate.callback;
    void                          *callback_arg   = request->allocate.private_data;
    int                            heap_allocated = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(client_thread, request);
    }

    callback(client_thread, error_code, callback_arg);
} /* chimera_allocate_complete */

static inline void
chimera_dispatch_allocate(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_allocate(
        thread->vfs_thread,
        chimera_client_req_cred(request),
        request->allocate.handle,
        request->allocate.offset,
        request->allocate.length,
        request->allocate.flags,
        0,  /* pre_attr_mask */
        0,  /* post_attr_mask */
        chimera_allocate_complete,
        request);
} /* chimera_dispatch_allocate */
