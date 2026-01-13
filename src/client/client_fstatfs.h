// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_statfs.h"

static void
chimera_fstatfs_getattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request       = private_data;
    struct chimera_client_thread  *client_thread = request->thread;
    chimera_fstatfs_callback_t     callback      = request->fstatfs.callback;
    void                          *callback_arg  = request->fstatfs.private_data;
    struct chimera_statvfs         st;

    chimera_client_request_free(client_thread, request);

    if (error_code != CHIMERA_VFS_OK) {
        callback(client_thread, error_code, NULL, callback_arg);
        return;
    }

    chimera_attrs_to_statvfs(attr, &st);

    callback(client_thread, CHIMERA_VFS_OK, &st, callback_arg);
} /* chimera_fstatfs_getattr_complete */

static inline void
chimera_dispatch_fstatfs(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_getattr(
        thread->vfs_thread,
        request->fstatfs.handle,
        CHIMERA_VFS_ATTR_MASK_STATFS,
        chimera_fstatfs_getattr_complete,
        request);
} /* chimera_dispatch_fstatfs */
