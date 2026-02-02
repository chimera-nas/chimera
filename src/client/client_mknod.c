// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_mknod.h"

SYMBOL_EXPORT void
chimera_mknod(
    struct chimera_client_thread *thread,
    const char                   *path,
    int                           path_len,
    mode_t                        mode,
    dev_t                         dev,
    chimera_mknod_callback_t      callback,
    void                         *private_data)
{
    struct chimera_client_request *request;
    const char                    *slash;

    slash = rindex(path, '/');

    request = chimera_client_request_alloc(thread);

    request->opcode             = CHIMERA_CLIENT_OP_MKNOD;
    request->mknod.callback     = callback;
    request->mknod.private_data = private_data;
    request->mknod.path_len     = path_len;
    request->mknod.parent_len   = slash ? slash - path : path_len;

    while (slash && *slash == '/') {
        slash++;
    }

    request->mknod.name_offset = slash ? slash - path : -1;

    memcpy(request->mknod.path, path, path_len);

    request->mknod.set_attr.va_req_mask = 0;
    request->mknod.set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV;
    request->mknod.set_attr.va_mode     = mode;
    request->mknod.set_attr.va_rdev     = dev;

    chimera_dispatch_mknod(thread, request);
} /* chimera_mknod */
