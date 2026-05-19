// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_clone_range.h"

SYMBOL_EXPORT void
chimera_clone_range(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *src_handle,
    uint64_t                        src_offset,
    struct chimera_vfs_open_handle *dst_handle,
    uint64_t                        dst_offset,
    uint64_t                        length,
    chimera_clone_range_callback_t  callback,
    void                           *private_data)
{
    struct chimera_client_request *request;

    request = chimera_client_request_alloc(thread);

    request->opcode                   = CHIMERA_CLIENT_OP_CLONE_RANGE;
    request->clone_range.callback     = callback;
    request->clone_range.private_data = private_data;
    request->clone_range.src_handle   = src_handle;
    request->clone_range.dst_handle   = dst_handle;
    request->clone_range.src_offset   = src_offset;
    request->clone_range.dst_offset   = dst_offset;
    request->clone_range.length       = length;

    chimera_dispatch_clone_range(thread, request);
} /* chimera_clone_range */
