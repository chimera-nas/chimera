// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_dispatch.h"

static void chimera_symlink_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

static void
chimera_symlink_at_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct chimera_client_request  *request        = private_data;
    struct chimera_client_thread   *thread         = request->thread;
    struct chimera_vfs_open_handle *parent_handle  = request->symlink.parent_handle;
    chimera_symlink_callback_t      callback       = request->symlink.callback;
    void                           *callback_arg   = request->symlink.private_data;
    int                             heap_allocated = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(thread, request);
    }

    chimera_vfs_release(thread->vfs_thread, parent_handle);

    callback(thread, error_code, callback_arg);

} /* chimera_symlink_at_complete */

static void
chimera_symlink_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_symlink_callback_t    callback     = request->symlink.callback;
        void                         *callback_arg = request->symlink.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, callback_arg);
        return;
    }

    request->symlink.parent_handle = oh;

    request->symlink.set_attr.va_req_mask = 0;
    request->symlink.set_attr.va_set_mask = 0;

    chimera_vfs_symlink(
        request->thread->vfs_thread,
        oh,
        request->symlink.path + request->symlink.name_offset,
        request->symlink.path_len - request->symlink.name_offset,
        request->symlink.target,
        request->symlink.target_len,
        &request->symlink.set_attr,
        CHIMERA_VFS_ATTR_FH,
        0,
        0,
        chimera_symlink_at_complete,
        request);

} /* chimera_symlink_parent_complete */

static void
chimera_symlink_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_symlink_callback_t    callback     = request->symlink.callback;
        void                         *callback_arg = request->symlink.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, callback_arg);
        return;
    }

    memcpy(request->fh, attr->va_fh, attr->va_fh_len);
    request->fh_len = attr->va_fh_len;

    chimera_vfs_open(
        request->thread->vfs_thread,
        request->fh,
        request->fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_symlink_parent_complete,
        request);

} /* chimera_symlink_parent_lookup_complete */

static inline void
chimera_dispatch_symlink(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{

    if (unlikely(request->symlink.name_offset == -1)) {
        chimera_dispatch_error_symlink(thread, request, CHIMERA_VFS_EINVAL);
        return;
    }

    chimera_vfs_lookup_path(
        thread->vfs_thread,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->symlink.path,
        request->symlink.parent_len,
        CHIMERA_VFS_ATTR_FH,
        chimera_symlink_parent_lookup_complete,
        request);
} /* chimera_dispatch_symlink */
