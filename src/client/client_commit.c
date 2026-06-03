// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_commit.h"

SYMBOL_EXPORT void
chimera_commit(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *handle,
    chimera_commit_callback_t       callback,
    void                           *private_data)
{
    struct chimera_client_request *request;

    request = chimera_client_request_alloc(thread);

    request->opcode              = CHIMERA_CLIENT_OP_COMMIT;
    request->commit.handle       = handle;
    request->commit.callback     = callback;
    request->commit.private_data = private_data;

    chimera_dispatch_commit(thread, request);
} /* chimera_commit */
