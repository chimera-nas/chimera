// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_stat.h"

static void
chimera_fstat_getattr_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request       = private_data;
    struct chimera_client_thread  *client_thread = request->thread;
    chimera_fstat_callback_t       callback      = request->fstat.callback;
    void                          *callback_arg  = request->fstat.private_data;
    struct chimera_stat            st;

    chimera_client_request_free(client_thread, request);

    if (error_code != CHIMERA_VFS_OK) {
        callback(client_thread, error_code, NULL, callback_arg);
        return;
    }

    chimera_attrs_to_stat(attr, &st);

    callback(client_thread, CHIMERA_VFS_OK, &st, callback_arg);
} /* chimera_fstat_getattr_complete */

static inline void
chimera_dispatch_fstat(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    chimera_vfs_getattr(
        thread->vfs_thread,
        request->fstat.handle,
        CHIMERA_VFS_ATTR_MASK_STAT,
        chimera_fstat_getattr_complete,
        request);
} /* chimera_dispatch_fstat */
