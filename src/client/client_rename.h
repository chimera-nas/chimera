// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_dispatch.h"

static void chimera_rename_dest_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

static void chimera_rename_source_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

static void
chimera_rename_at_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_client_request  *request              = private_data;
    struct chimera_client_thread   *thread               = request->thread;
    struct chimera_vfs_open_handle *source_parent_handle = request->rename.source_parent_handle;
    struct chimera_vfs_open_handle *dest_parent_handle   = request->rename.dest_parent_handle;
    chimera_rename_callback_t       callback             = request->rename.callback;
    void                           *callback_arg         = request->rename.private_data;
    int                             heap_allocated       = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(thread, request);
    }

    chimera_vfs_release(thread->vfs_thread, source_parent_handle);
    chimera_vfs_release(thread->vfs_thread, dest_parent_handle);

    callback(thread, error_code, callback_arg);

} /* chimera_rename_at_complete */

static void
chimera_rename_dest_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread   *thread               = request->thread;
        struct chimera_vfs_open_handle *source_parent_handle = request->rename.source_parent_handle;
        chimera_rename_callback_t       callback             = request->rename.callback;
        void                           *callback_arg         = request->rename.private_data;

        chimera_client_request_free(thread, request);
        chimera_vfs_release(thread->vfs_thread, source_parent_handle);
        callback(thread, error_code, callback_arg);
        return;
    }

    request->rename.dest_parent_handle = oh;
    memcpy(request->rename.dest_fh, oh->fh, oh->fh_len);
    request->rename.dest_fh_len = oh->fh_len;

    chimera_vfs_rename(
        request->thread->vfs_thread,
        request->rename.source_fh,
        request->rename.source_fh_len,
        request->rename.source_path + request->rename.source_name_offset,
        request->rename.source_path_len - request->rename.source_name_offset,
        request->rename.dest_fh,
        request->rename.dest_fh_len,
        request->rename.dest_path + request->rename.dest_name_offset,
        request->rename.dest_path_len - request->rename.dest_name_offset,
        chimera_rename_at_complete,
        request);

} /* chimera_rename_dest_parent_complete */

static void
chimera_rename_dest_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread   *thread               = request->thread;
        struct chimera_vfs_open_handle *source_parent_handle = request->rename.source_parent_handle;
        chimera_rename_callback_t       callback             = request->rename.callback;
        void                           *callback_arg         = request->rename.private_data;

        chimera_client_request_free(thread, request);
        chimera_vfs_release(thread->vfs_thread, source_parent_handle);
        callback(thread, error_code, callback_arg);
        return;
    }

    chimera_vfs_open(
        request->thread->vfs_thread,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_rename_dest_parent_complete,
        request);

} /* chimera_rename_dest_parent_lookup_complete */

static void
chimera_rename_source_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_rename_callback_t     callback     = request->rename.callback;
        void                         *callback_arg = request->rename.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, callback_arg);
        return;
    }

    request->rename.source_parent_handle = oh;
    memcpy(request->rename.source_fh, oh->fh, oh->fh_len);
    request->rename.source_fh_len = oh->fh_len;

    chimera_vfs_lookup_path(
        request->thread->vfs_thread,
        request->thread->client->root_fh,
        request->thread->client->root_fh_len,
        request->rename.dest_path,
        request->rename.dest_parent_len,
        CHIMERA_VFS_ATTR_FH,
        chimera_rename_dest_parent_lookup_complete,
        request);

} /* chimera_rename_source_parent_complete */

static void
chimera_rename_source_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_rename_callback_t     callback     = request->rename.callback;
        void                         *callback_arg = request->rename.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, callback_arg);
        return;
    }

    chimera_vfs_open(
        request->thread->vfs_thread,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_rename_source_parent_complete,
        request);

} /* chimera_rename_source_parent_lookup_complete */

static inline void
chimera_dispatch_rename(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{

    if (unlikely(request->rename.source_name_offset == -1 || request->rename.dest_name_offset == -1)) {
        chimera_dispatch_error_rename(thread, request, CHIMERA_VFS_EINVAL);
        return;
    }

    chimera_vfs_lookup_path(
        thread->vfs_thread,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->rename.source_path,
        request->rename.source_parent_len,
        CHIMERA_VFS_ATTR_FH,
        chimera_rename_source_parent_lookup_complete,
        request);
} /* chimera_dispatch_rename */
