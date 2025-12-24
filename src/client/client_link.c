// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_link.h"

SYMBOL_EXPORT void
chimera_link(
    struct chimera_client_thread *thread,
    const char                   *source_path,
    int                           source_path_len,
    const char                   *dest_path,
    int                           dest_path_len,
    chimera_link_callback_t       callback,
    void                         *private_data)
{
    struct chimera_client_request *request;
    const char                    *source_slash;
    const char                    *dest_slash;

    source_slash = rindex(source_path, '/');
    dest_slash   = rindex(dest_path, '/');

    request = chimera_client_request_alloc(thread);

    request->opcode                 = CHIMERA_CLIENT_OP_LINK;
    request->link.callback          = callback;
    request->link.private_data      = private_data;
    request->link.source_path_len   = source_path_len;
    request->link.source_parent_len = source_slash ? source_slash - source_path : source_path_len;
    request->link.dest_path_len     = dest_path_len;
    request->link.dest_parent_len   = dest_slash ? dest_slash - dest_path : dest_path_len;

    while (dest_slash && *dest_slash == '/') {
        dest_slash++;
    }

    request->link.dest_name_offset = dest_slash ? dest_slash - dest_path : -1;

    memcpy(request->link.source_path, source_path, source_path_len);
    memcpy(request->link.dest_path, dest_path, dest_path_len);

    chimera_dispatch_link(thread, request);
} /* chimera_link */
