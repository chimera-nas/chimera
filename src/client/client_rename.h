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

static void chimera_rename_target_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data);

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
chimera_rename_target_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code == CHIMERA_VFS_OK) {
        /* Target exists - save its FH for silly rename handling */
        request->rename.target_fh_len = attr->va_fh_len;
        memcpy(request->rename.target_fh, attr->va_fh, attr->va_fh_len);
    } else if (error_code == CHIMERA_VFS_ENOENT) {
        /* Target doesn't exist - that's fine, no silly rename needed */
        request->rename.target_fh_len = 0;
    } else {
        /* Other error - fail the rename */
        struct chimera_client_thread   *thread               = request->thread;
        struct chimera_vfs_open_handle *source_parent_handle = request->rename.source_parent_handle;
        struct chimera_vfs_open_handle *dest_parent_handle   = request->rename.dest_parent_handle;
        chimera_rename_callback_t       callback             = request->rename.callback;
        void                           *callback_arg         = request->rename.private_data;

        chimera_client_request_free(thread, request);
        chimera_vfs_release(thread->vfs_thread, source_parent_handle);
        chimera_vfs_release(thread->vfs_thread, dest_parent_handle);
        callback(thread, error_code, callback_arg);
        return;
    }

    /* Now do the actual rename with optional target FH */
    chimera_vfs_rename(
        request->thread->vfs_thread,
        &request->thread->client->cred,
        request->rename.source_fh,
        request->rename.source_fh_len,
        request->rename.source_path + request->rename.source_name_offset,
        request->rename.source_path_len - request->rename.source_name_offset,
        request->rename.dest_fh,
        request->rename.dest_fh_len,
        request->rename.dest_path + request->rename.dest_name_offset,
        request->rename.dest_path_len - request->rename.dest_name_offset,
        request->rename.target_fh_len ? request->rename.target_fh : NULL,
        request->rename.target_fh_len,
        chimera_rename_at_complete,
        request);

} /* chimera_rename_target_lookup_complete */

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

    /* Look up the target file (if it exists) for silly rename handling */
    chimera_vfs_lookup(
        request->thread->vfs_thread,
        &request->thread->client->cred,
        oh,
        request->rename.dest_path + request->rename.dest_name_offset,
        request->rename.dest_path_len - request->rename.dest_name_offset,
        CHIMERA_VFS_ATTR_FH,
        0,  /* Don't follow symlinks */
        chimera_rename_target_lookup_complete,
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
        &request->thread->client->cred,
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
        &request->thread->client->cred,
        request->thread->client->root_fh,
        request->thread->client->root_fh_len,
        request->rename.dest_path,
        request->rename.dest_parent_len,
        CHIMERA_VFS_ATTR_FH,
        CHIMERA_VFS_LOOKUP_FOLLOW,
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
        &request->thread->client->cred,
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
        &thread->client->cred,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->rename.source_path,
        request->rename.source_parent_len,
        CHIMERA_VFS_ATTR_FH,
        CHIMERA_VFS_LOOKUP_FOLLOW,
        chimera_rename_source_parent_lookup_complete,
        request);
} /* chimera_dispatch_rename */
