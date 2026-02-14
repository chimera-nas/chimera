// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_symlink.h"

SYMBOL_EXPORT void
chimera_symlink(
    struct chimera_client_thread *thread,
    const char                   *path,
    int                           path_len,
    const char                   *target,
    int                           target_len,
    chimera_symlink_callback_t    callback,
    void                         *private_data)
{
    struct chimera_client_request *request;
    const char                    *slash;

    if (unlikely(target_len >= CHIMERA_VFS_PATH_MAX)) {
        callback(thread, CHIMERA_VFS_ENAMETOOLONG, private_data);
        return;
    }

    slash = rindex(path, '/');

    request = chimera_client_request_alloc(thread);

    request->opcode               = CHIMERA_CLIENT_OP_SYMLINK;
    request->symlink.callback     = callback;
    request->symlink.private_data = private_data;
    request->symlink.path_len     = path_len;
    request->symlink.target_len   = target_len;

    while (slash && *slash == '/') {
        slash++;
    }

    request->symlink.name_offset = slash ? slash - path : -1;

    memcpy(request->symlink.path, path, path_len);
    memcpy(request->symlink.target, target, target_len);

    chimera_dispatch_symlink(thread, request);
} /* chimera_symlink */
