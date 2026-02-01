// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_dispatch.h"

static void chimera_mknod_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

static void
chimera_mknod_at_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct chimera_client_request  *request       = private_data;
    struct chimera_client_thread   *client_thread = request->thread;
    struct chimera_vfs_open_handle *parent_handle = request->mknod.parent_handle;
    chimera_mknod_callback_t        callback      = request->mknod.callback;
    void                           *callback_arg  = request->mknod.private_data;

    chimera_client_request_free(client_thread, request);

    chimera_vfs_release(client_thread->vfs_thread, parent_handle);

    callback(client_thread, error_code, callback_arg);
} /* chimera_mknod_at_complete */

static void
chimera_mknod_parent_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *client_thread = request->thread;
        chimera_mknod_callback_t      callback      = request->mknod.callback;
        void                         *callback_arg  = request->mknod.private_data;

        chimera_client_request_free(client_thread, request);
        callback(client_thread, error_code, callback_arg);
        return;
    }

    request->mknod.parent_handle = oh;

    chimera_vfs_mknod(
        request->thread->vfs_thread,
        &request->thread->client->cred,
        oh,
        request->mknod.path + request->mknod.name_offset,
        request->mknod.path_len - request->mknod.name_offset,
        &request->mknod.set_attr,
        0,
        0,
        0,
        chimera_mknod_at_complete,
        request);

} /* chimera_mknod_parent_complete */

static void
chimera_mknod_parent_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *client_thread = request->thread;
        chimera_mknod_callback_t      callback      = request->mknod.callback;
        void                         *callback_arg  = request->mknod.private_data;

        chimera_client_request_free(client_thread, request);
        callback(client_thread, error_code, callback_arg);
        return;
    }

    chimera_vfs_open(
        request->thread->vfs_thread,
        &request->thread->client->cred,
        attr->va_fh,
        attr->va_fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_DIRECTORY,
        chimera_mknod_parent_complete,
        request);

} /* chimera_mknod_parent_lookup_complete */

static inline void
chimera_dispatch_mknod(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{

    if (unlikely(request->mknod.name_offset == -1)) {
        chimera_dispatch_error_mknod(thread, request, CHIMERA_VFS_EINVAL);
        return;
    }

    chimera_vfs_lookup_path(
        thread->vfs_thread,
        &thread->client->cred,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->mknod.path,
        request->mknod.parent_len,
        CHIMERA_VFS_ATTR_FH,
        CHIMERA_VFS_LOOKUP_FOLLOW,
        chimera_mknod_parent_lookup_complete,
        request);
} /* chimera_dispatch_mknod */
