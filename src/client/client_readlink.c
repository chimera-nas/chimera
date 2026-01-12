// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_readlink.h"

SYMBOL_EXPORT void
chimera_readlink(
    struct chimera_client_thread *thread,
    const char                   *path,
    int                           path_len,
    char                         *target,
    uint32_t                      target_maxlength,
    chimera_readlink_callback_t   callback,
    void                         *private_data)
{
    struct chimera_client_request *request;

    if (unlikely(target_maxlength > CHIMERA_VFS_PATH_MAX)) {
        callback(thread, CHIMERA_VFS_EINVAL, NULL, 0, private_data);
        return;
    }

    request = chimera_client_request_alloc(thread);

    request->opcode                    = CHIMERA_CLIENT_OP_READLINK;
    request->readlink.callback         = callback;
    request->readlink.private_data     = private_data;
    request->readlink.target_maxlength = target_maxlength;
    request->readlink.target           = target;
    request->readlink.path_len         = path_len;

    memcpy(request->readlink.path, path, path_len);

    chimera_dispatch_readlink(thread, request);
} /* chimera_readlink */
