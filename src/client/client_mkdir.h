// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_dispatch.h"

static void
chimera_mkdir_vfs_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request      = private_data;
    struct chimera_client_thread  *thread       = request->thread;
    chimera_mkdir_callback_t       callback     = request->mkdir.callback;
    void                          *callback_arg = request->mkdir.private_data;

    chimera_client_request_free(thread, request);

    callback(thread, error_code, callback_arg);
} /* chimera_mkdir_vfs_complete */

static inline void
chimera_dispatch_mkdir(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{

    if (unlikely(request->mkdir.name_offset == -1)) {
        /* Caller is trying to mkdir the root directory, which always exists already */
        chimera_dispatch_error_mkdir(thread, request, CHIMERA_VFS_EEXIST);
        return;
    }

    request->mkdir.set_attr.va_req_mask = 0;
    request->mkdir.set_attr.va_set_mask = 0;

    chimera_vfs_mkdir(
        thread->vfs_thread,
        &thread->client->cred,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->mkdir.path,
        request->mkdir.path_len,
        &request->mkdir.set_attr,
        0,
        chimera_mkdir_vfs_complete,
        request);
} /* chimera_dispatch_mkdir */

static void
chimera_mkdir_dispatch_at_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request        = private_data;
    struct chimera_client_thread  *client_thread  = request->thread;
    chimera_mkdir_callback_t       callback       = request->mkdir.callback;
    void                          *callback_arg   = request->mkdir.private_data;
    int                            heap_allocated = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(client_thread, request);
    }

    /* Note: parent handle is NOT released - caller owns it */
    callback(client_thread, error_code, callback_arg);
} /* chimera_mkdir_dispatch_at_complete */

static inline void
chimera_dispatch_mkdir_at(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *parent_handle,
    struct chimera_client_request  *request)
{
    chimera_vfs_mkdir_at(
        thread->vfs_thread,
        &thread->client->cred,
        parent_handle,
        request->mkdir.path,
        request->mkdir.path_len,
        &request->mkdir.set_attr,
        CHIMERA_VFS_ATTR_FH,
        0,
        0,
        chimera_mkdir_dispatch_at_complete,
        request);
} /* chimera_dispatch_mkdir_at */
