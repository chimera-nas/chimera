// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_mkdir.h"

SYMBOL_EXPORT void
chimera_mkdir(
    struct chimera_client_thread *thread,
    const char                   *path,
    int                           path_len,
    chimera_mkdir_callback_t      callback,
    void                         *private_data)
{
    struct chimera_client_request *request;
    const char                    *slash;

    slash = rindex(path, '/');

    request = chimera_client_request_alloc(thread);

    request->opcode             = CHIMERA_CLIENT_OP_MKDIR;
    request->mkdir.callback     = callback;
    request->mkdir.private_data = private_data;
    request->mkdir.path_len     = path_len;

    while (slash && *slash == '/') {
        slash++;
    }

    request->mkdir.name_offset = slash ? slash - path : -1;

    memcpy(request->mkdir.path, path, path_len);

    chimera_dispatch_mkdir(thread, request);
} /* chimera_mkdir */
