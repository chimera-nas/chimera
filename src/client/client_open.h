// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void
chimera_open_path_at_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct chimera_client_request  *request        = private_data;
    struct chimera_client_thread   *thread         = request->thread;
    struct chimera_vfs_open_handle *parent_handle  = request->open.parent_handle;
    chimera_open_callback_t         callback       = request->open.callback;
    void                           *callback_arg   = request->open.private_data;
    int                             heap_allocated = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(thread, request);
    }

    chimera_vfs_release(thread->vfs_thread, parent_handle);

    callback(thread, error_code, oh, callback_arg);

} /* chimera_open_path_complete */

static void
chimera_open_path_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
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
} /* chimera_open_path_complete */

static void chimera_open_path_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

static void
chimera_open_path_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_open_callback_t       callback     = request->open.callback;
        void                         *callback_arg = request->open.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, NULL, callback_arg);
        return;
    }
    memcpy(request->fh, attr->va_fh, attr->va_fh_len);
    request->fh_len = attr->va_fh_len;

    chimera_vfs_open(
        request->thread->vfs_thread,
        request->fh,
        request->fh_len,
        request->open.flags,
        chimera_open_path_complete,
        request);


} /* chimera_open_path_complete */

static void
chimera_open_path_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_open_callback_t       callback     = request->open.callback;
        void                         *callback_arg = request->open.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, NULL, callback_arg);
        return;
    }

    request->open.parent_handle = oh;

    request->open.set_attr.va_req_mask = 0;
    request->open.set_attr.va_set_mask = 0;

    chimera_vfs_open_at(
        request->thread->vfs_thread,
        oh,
        request->open.path + request->open.name_offset,
        request->open.path_len - request->open.name_offset,
        request->open.flags,
        &request->open.set_attr,
        CHIMERA_VFS_ATTR_FH,
        0,
        0,
        chimera_open_path_at_complete,
        request);

} /* chimera_client_open_path_parent_complete */

static void
chimera_open_path_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_open_callback_t       callback     = request->open.callback;
        void                         *callback_arg = request->open.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, NULL, callback_arg);
        return;
    }

    memcpy(request->fh, attr->va_fh, attr->va_fh_len);
    request->fh_len = attr->va_fh_len;

    chimera_vfs_open(
        request->thread->vfs_thread,
        request->fh,
        request->fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_open_path_parent_complete,
        request);


} /* chimera_client_open_path_complete */

static inline void
chimera_dispatch_open(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    if (request->open.flags & CHIMERA_VFS_OPEN_CREATE) {

        chimera_vfs_lookup_path(
            thread->vfs_thread,
            thread->client->root_fh,
            thread->client->root_fh_len,
            request->open.path,
            request->open.parent_len,
            CHIMERA_VFS_ATTR_FH,
            CHIMERA_VFS_LOOKUP_FOLLOW,
            chimera_open_path_parent_lookup_complete,
            request);
    } else {
        chimera_vfs_lookup_path(
            thread->vfs_thread,
            thread->client->root_fh,
            thread->client->root_fh_len,
            request->open.path,
            request->open.path_len,
            CHIMERA_VFS_ATTR_FH,
            CHIMERA_VFS_LOOKUP_FOLLOW,
            chimera_open_path_lookup_complete,
            request);

    }

} /* chimera_client_open_path */

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
