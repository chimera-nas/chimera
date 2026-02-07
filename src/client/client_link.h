// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "client_internal.h"
#include "client_dispatch.h"

static void
chimera_link_vfs_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_client_request *request      = private_data;
    struct chimera_client_thread  *thread       = request->thread;
    chimera_link_callback_t        callback     = request->link.callback;
    void                          *callback_arg = request->link.private_data;

    chimera_client_request_free(thread, request);

    callback(thread, error_code, callback_arg);
} /* chimera_link_vfs_complete */

static inline void
chimera_dispatch_link(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{

    if (unlikely(request->link.dest_name_offset == -1)) {
        chimera_dispatch_error_link(thread, request, CHIMERA_VFS_EINVAL);
        return;
    }

    chimera_vfs_link(
        thread->vfs_thread,
        &thread->client->cred,
        thread->client->root_fh,
        thread->client->root_fh_len,
        request->link.source_path,
        request->link.source_path_len,
        request->link.dest_path,
        request->link.dest_path_len,
        0,
        CHIMERA_VFS_ATTR_FH,
        chimera_link_vfs_complete,
        request);
} /* chimera_dispatch_link */
