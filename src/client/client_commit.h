// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void
chimera_commit_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request        = private_data;
    struct chimera_client_thread  *client_thread  = request->thread;
    chimera_commit_callback_t      callback       = request->commit.callback;
    void                          *callback_arg   = request->commit.private_data;
    int                            heap_allocated = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(client_thread, request);
    }

    callback(client_thread, error_code, callback_arg);
} /* chimera_commit_complete */

static inline void
chimera_dispatch_commit(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_commit(
        thread->vfs_thread,
        request->commit.handle,
        0,  /* offset - sync entire file */
        0,  /* count - sync entire file */
        0,  /* pre_attr_mask */
        0,  /* post_attr_mask */
        chimera_commit_complete,
        request);
} /* chimera_dispatch_commit */
