// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_copy_range.h"

SYMBOL_EXPORT void
chimera_copy_range(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *src_handle,
    uint64_t                        src_offset,
    struct chimera_vfs_open_handle *dst_handle,
    uint64_t                        dst_offset,
    uint64_t                        length,
    chimera_copy_range_callback_t   callback,
    void                           *private_data)
{
    struct chimera_client_request *request;

    request = chimera_client_request_alloc(thread);

    request->opcode                  = CHIMERA_CLIENT_OP_COPY_RANGE;
    request->copy_range.callback     = callback;
    request->copy_range.private_data = private_data;
    request->copy_range.src_handle   = src_handle;
    request->copy_range.dst_handle   = dst_handle;
    request->copy_range.src_offset   = src_offset;
    request->copy_range.dst_offset   = dst_offset;
    request->copy_range.length       = length;
    request->copy_range.r_length     = 0;

    chimera_dispatch_copy_range(thread, request);
} /* chimera_copy_range */
