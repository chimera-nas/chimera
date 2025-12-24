// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_stat.h"

SYMBOL_EXPORT void
chimera_stat(
    struct chimera_client_thread *thread,
    const char                   *path,
    int                           path_len,
    chimera_stat_callback_t       callback,
    void                         *private_data)
{
    struct chimera_client_request *request;

    request = chimera_client_request_alloc(thread);

    request->opcode            = CHIMERA_CLIENT_OP_STAT;
    request->stat.callback     = callback;
    request->stat.private_data = private_data;
    request->stat.path_len     = path_len;

    memcpy(request->stat.path, path, path_len);

    chimera_dispatch_stat(thread, request);
} /* chimera_stat */
