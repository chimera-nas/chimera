// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void chimera_remove_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

static void
chimera_remove_at_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request  *request        = private_data;
    struct chimera_client_thread   *thread         = request->thread;
    struct chimera_vfs_open_handle *parent_handle  = request->remove.parent_handle;
    chimera_remove_callback_t       callback       = request->remove.callback;
    void                           *callback_arg   = request->remove.private_data;
    int                             heap_allocated = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(thread, request);
    }

    chimera_vfs_release(thread->vfs_thread, parent_handle);

    callback(thread, error_code, callback_arg);

} /* chimera_remove_at_complete */

static void
chimera_remove_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_remove_callback_t     callback     = request->remove.callback;
        void                         *callback_arg = request->remove.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, callback_arg);
        return;
    }

    request->remove.parent_handle = oh;

    chimera_vfs_remove(
        request->thread->vfs_thread,
        oh,
        request->remove.path + request->remove.name_offset,
        request->remove.path_len - request->remove.name_offset,
        0,
        0,
        chimera_remove_at_complete,
        request);

} /* chimera_remove_parent_complete */

static void
chimera_remove_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_remove_callback_t     callback     = request->remove.callback;
        void                         *callback_arg = request->remove.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, callback_arg);
        return;
    }

    chimera_vfs_open(
        request->thread->vfs_thread,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_remove_parent_complete,
        request);


} /* chimera_remove_parent_lookup_complete */

static inline void
chimera_dispatch_remove(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{

    if (unlikely(request->remove.name_offset == -1)) {
        chimera_remove_callback_t callback     = request->remove.callback;
        void                     *callback_arg = request->remove.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, CHIMERA_VFS_EINVAL, callback_arg);
        return;
    }

    chimera_vfs_lookup_path(
        thread->vfs_thread,
        root_fh,
        sizeof(root_fh),
        request->remove.path,
        request->remove.parent_len,
        CHIMERA_VFS_ATTR_FH,
        chimera_remove_parent_lookup_complete,
        request);
} /* chimera_dispatch_remove */

static void
chimera_remove_dispatch_at_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_client_request *request        = private_data;
    struct chimera_client_thread  *thread         = request->thread;
    chimera_remove_callback_t      callback       = request->remove.callback;
    void                          *callback_arg   = request->remove.private_data;
    int                            heap_allocated = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(thread, request);
    }

    /* Note: parent handle is NOT released - caller owns it */
    callback(thread, error_code, callback_arg);
} /* chimera_remove_dispatch_at_complete */

static inline void
chimera_dispatch_remove_at(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *parent_handle,
    struct chimera_client_request  *request)
{
    chimera_vfs_remove(
        thread->vfs_thread,
        parent_handle,
        request->remove.path,
        request->remove.path_len,
        0,
        0,
        chimera_remove_dispatch_at_complete,
        request);
} /* chimera_dispatch_remove_at */
