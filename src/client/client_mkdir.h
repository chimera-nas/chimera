// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_dispatch.h"

static void chimera_mkdir_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

static void
chimera_mkdir_at_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct chimera_client_request  *request       = private_data;
    struct chimera_client_thread   *client_thread = request->thread;
    struct chimera_vfs_open_handle *parent_handle = request->mkdir.parent_handle;
    chimera_mkdir_callback_t        callback      = request->mkdir.callback;
    void                           *callback_arg  = request->mkdir.private_data;

    chimera_client_request_free(client_thread, request);

    chimera_vfs_release(client_thread->vfs_thread, parent_handle);

    callback(client_thread, error_code, callback_arg);
} /* chimera_mkdir_at_complete */

static void
chimera_mkdir_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *client_thread = request->thread;
        chimera_mkdir_callback_t      callback      = request->mkdir.callback;
        void                         *callback_arg  = request->mkdir.private_data;

        chimera_client_request_free(client_thread, request);
        callback(client_thread, error_code, callback_arg);
        return;
    }

    request->mkdir.parent_handle = oh;

    request->mkdir.set_attr.va_req_mask = 0;
    request->mkdir.set_attr.va_set_mask = 0;

    chimera_vfs_mkdir(
        request->thread->vfs_thread,
        oh,
        request->mkdir.path + request->mkdir.name_offset,
        request->mkdir.path_len - request->mkdir.name_offset,
        &request->mkdir.set_attr,
        0,
        0,
        0,
        chimera_mkdir_at_complete,
        request);

} /* chimera_mkdir_parent_complete */

static void
chimera_mkdir_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *client_thread = request->thread;
        chimera_mkdir_callback_t      callback      = request->mkdir.callback;
        void                         *callback_arg  = request->mkdir.private_data;

        chimera_client_request_free(client_thread, request);
        callback(client_thread, error_code, callback_arg);
        return;
    }

    chimera_vfs_open(
        request->thread->vfs_thread,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_mkdir_parent_complete,
        request);

} /* chimera_mkdir_parent_lookup_complete */

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

    chimera_vfs_lookup_path(
        thread->vfs_thread,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->mkdir.path,
        request->mkdir.parent_len,
        CHIMERA_VFS_ATTR_FH,
        CHIMERA_VFS_LOOKUP_FOLLOW,
        chimera_mkdir_parent_lookup_complete,
        request);
} /* chimera_mkdir */

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
    chimera_vfs_mkdir(
        thread->vfs_thread,
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
