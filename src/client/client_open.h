// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void
chimera_open_vfs_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct chimera_client_request *request      = private_data;
    struct chimera_client_thread  *thread       = request->thread;
    chimera_open_callback_t        callback     = request->open.callback;
    void                          *callback_arg = request->open.private_data;

    chimera_client_request_free(thread, request);

    callback(thread, error_code, oh, callback_arg);
} /* chimera_open_vfs_complete */

static inline void
chimera_dispatch_open(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    request->open.set_attr.va_req_mask = 0;
    request->open.set_attr.va_set_mask = 0;

    chimera_vfs_open(
        thread->vfs_thread,
        &thread->client->cred,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->open.path,
        request->open.path_len,
        request->open.flags,
        &request->open.set_attr,
        CHIMERA_VFS_ATTR_FH,
        chimera_open_vfs_complete,
        request);
} /* chimera_dispatch_open */

static void
chimera_open_at_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct chimera_client_request *request        = private_data;
    struct chimera_client_thread  *thread         = request->thread;
    chimera_open_callback_t        callback       = request->open.callback;
    void                          *callback_arg   = request->open.private_data;
    int                            heap_allocated = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(thread, request);
    }

    callback(thread, error_code, oh, callback_arg);

} /* chimera_open_at_complete */

static inline void
chimera_dispatch_open_at(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *parent_handle,
    struct chimera_client_request  *request)
{
    request->open.set_attr.va_req_mask = 0;
    request->open.set_attr.va_set_mask = 0;

    chimera_vfs_open_at(
        thread->vfs_thread,
        &thread->client->cred,
        parent_handle,
        request->open.path,
        request->open.path_len,
        request->open.flags,
        &request->open.set_attr,
        CHIMERA_VFS_ATTR_FH,
        0,
        0,
        chimera_open_at_complete,
        request);
} /* chimera_dispatch_open_at */
