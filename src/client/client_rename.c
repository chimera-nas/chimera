// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_rename.h"

SYMBOL_EXPORT void
chimera_rename(
    struct chimera_client_thread *thread,
    const char                   *source_path,
    int                           source_path_len,
    const char                   *dest_path,
    int                           dest_path_len,
    chimera_rename_callback_t     callback,
    void                         *private_data)
{
    struct chimera_client_request *request;
    const char                    *source_slash;
    const char                    *dest_slash;

    source_slash = rindex(source_path, '/');
    dest_slash   = rindex(dest_path, '/');

    request = chimera_client_request_alloc(thread);

    request->opcode                 = CHIMERA_CLIENT_OP_RENAME;
    request->rename.callback        = callback;
    request->rename.private_data    = private_data;
    request->rename.source_path_len = source_path_len;
    request->rename.dest_path_len   = dest_path_len;

    while (source_slash && *source_slash == '/') {
        source_slash++;
    }

    while (dest_slash && *dest_slash == '/') {
        dest_slash++;
    }

    request->rename.source_name_offset = source_slash ? source_slash - source_path : -1;
    request->rename.dest_name_offset   = dest_slash ? dest_slash - dest_path : -1;

    memcpy(request->rename.source_path, source_path, source_path_len);
    memcpy(request->rename.dest_path, dest_path, dest_path_len);

    chimera_dispatch_rename(thread, request);
} /* chimera_rename */
