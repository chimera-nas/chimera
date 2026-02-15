// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_open.h"

SYMBOL_EXPORT void
chimera_open(
    struct chimera_client_thread *thread,
    const char                   *path,
    int                           path_len,
    unsigned int                  flags,
    chimera_open_callback_t       callback,
    void                         *private_data)
{
    struct chimera_client_request *request;

    request = chimera_client_request_alloc(thread);

    request->opcode            = CHIMERA_CLIENT_OP_OPEN;
    request->open.callback     = callback;
    request->open.private_data = private_data;
    request->open.flags        = flags;
    request->open.path_len     = path_len;

    memcpy(request->open.path, path, path_len);

    chimera_dispatch_open(thread, request);
} /* chimera_open */
