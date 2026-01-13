// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"

static void chimera_readlink_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data);

static void
chimera_readlink_complete(
    enum chimera_vfs_error error_code,
    int                    targetlen,
    void                  *private_data)
{
    struct chimera_client_request  *request        = private_data;
    struct chimera_client_thread   *thread         = request->thread;
    struct chimera_vfs_open_handle *handle         = request->readlink.handle;
    chimera_readlink_callback_t     callback       = request->readlink.callback;
    void                           *callback_arg   = request->readlink.private_data;
    char                           *target         = request->readlink.target;
    int                             heap_allocated = request->heap_allocated;

    if (heap_allocated) {
        chimera_client_request_free(thread, request);
    }

    chimera_vfs_release(thread->vfs_thread, handle);

    callback(thread, error_code, target, targetlen, callback_arg);

} /* chimera_readlink_complete */

static void
chimera_readlink_open_complete(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_readlink_callback_t   callback     = request->readlink.callback;
        void                         *callback_arg = request->readlink.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, NULL, 0, callback_arg);
        return;
    }

    request->readlink.handle = oh;

    chimera_vfs_readlink(
        request->thread->vfs_thread,
        oh,
        request->readlink.target,
        request->readlink.target_maxlength,
        chimera_readlink_complete,
        request);

} /* chimera_readlink_open_complete */

static void
chimera_readlink_lookup_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        struct chimera_client_thread *thread       = request->thread;
        chimera_readlink_callback_t   callback     = request->readlink.callback;
        void                         *callback_arg = request->readlink.private_data;

        chimera_client_request_free(thread, request);
        callback(thread, error_code, NULL, 0, callback_arg);
        return;
    }

    memcpy(request->fh, attr->va_fh, attr->va_fh_len);
    request->fh_len = attr->va_fh_len;

    chimera_vfs_open(
        request->thread->vfs_thread,
        request->fh,
        request->fh_len,
        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED,
        chimera_readlink_open_complete,
        request);

} /* chimera_readlink_lookup_complete */

static inline void
chimera_dispatch_readlink(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    /* Do not follow the final symlink - we want to read its target */
    chimera_vfs_lookup_path(
        thread->vfs_thread,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->readlink.path,
        request->readlink.path_len,
        CHIMERA_VFS_ATTR_FH,
        0,
        chimera_readlink_lookup_complete,
        request);
} /* chimera_dispatch_readlink */
