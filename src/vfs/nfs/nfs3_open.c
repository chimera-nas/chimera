// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_internal.h"

void
chimera_nfs3_open(
    struct chimera_nfs_thread          *thread,
    struct chimera_nfs_shared          *shared,
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_nfs_client_open_handle *open_handle;

    open_handle                 = chimera_nfs_thread_open_handle_alloc(thread);
    open_handle->dirty          = 0;
    request->open.r_vfs_private = (uint64_t) open_handle;

    request->status = CHIMERA_VFS_ENOTSUP;
    request->complete(request);
} /* chimera_nfs3_open */

