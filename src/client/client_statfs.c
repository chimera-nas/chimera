// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_statfs.h"

SYMBOL_EXPORT void
chimera_statfs(
    struct chimera_client_thread *thread,
    const char                   *path,
    int                           path_len,
    chimera_statfs_callback_t     callback,
    void                         *private_data)
{
    struct chimera_client_request *request;

    request = chimera_client_request_alloc(thread);

    request->opcode              = CHIMERA_CLIENT_OP_STATFS;
    request->statfs.callback     = callback;
    request->statfs.private_data = private_data;
    request->statfs.path_len     = path_len;

    memcpy(request->statfs.path, path, path_len);

    chimera_dispatch_statfs(thread, request);
} /* chimera_statfs */
