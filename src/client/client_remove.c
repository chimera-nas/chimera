// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_remove.h"

SYMBOL_EXPORT void
chimera_remove(
    struct chimera_client_thread *thread,
    const char                   *path,
    int                           path_len,
    chimera_remove_callback_t     callback,
    void                         *private_data)
{
    struct chimera_client_request *request;
    const char                    *slash;

    slash = rindex(path, '/');

    request = chimera_client_request_alloc(thread);

    request->opcode              = CHIMERA_CLIENT_OP_REMOVE;
    request->remove.callback     = callback;
    request->remove.private_data = private_data;
    request->remove.path_len     = path_len;
    request->remove.parent_len   = slash ? slash - path : path_len;

    while (slash && *slash == '/') {
        slash++;
    }

    request->remove.name_offset = slash ? slash - path : -1;

    memcpy(request->remove.path, path, path_len);

    chimera_dispatch_remove(thread, request);
} /* chimera_remove */
