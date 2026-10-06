// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_readdir.h"

SYMBOL_EXPORT void
chimera_readdir(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    chimera_readdir_callback_t      callback,
    chimera_readdir_complete_t      complete,
    void                           *private_data)
{
    struct chimera_client_request *request;

    request = chimera_client_request_alloc(thread);

    request->opcode               = CHIMERA_CLIENT_OP_READDIR;
    request->readdir.callback     = callback;
    request->readdir.complete     = complete;
    request->readdir.private_data = private_data;
    request->readdir.handle       = handle;
    /* A bare handle does not record CHIMERA_VFS_OPEN_DIRECTORY, and the
     * sequence will not serve a READDIR from a lent handle without it.  The
     * contract of this call is that `handle` is an open directory, so the
     * bit is a real flag of the caller's open and is restored here. */
    request->readdir.open_flags = chimera_client_handle_open_flags(handle) |
        CHIMERA_VFS_OPEN_DIRECTORY;
    request->readdir.cookie = cookie;

    chimera_dispatch_readdir(thread, request);
} /* chimera_readdir */
