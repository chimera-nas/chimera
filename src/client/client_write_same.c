// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_write_same.h"

SYMBOL_EXPORT void
chimera_write_same(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        block_size,
    uint64_t                        block_count,
    const void                     *pattern,
    uint32_t                        pattern_len,
    uint32_t                        reloff_pattern,
    chimera_write_same_callback_t   callback,
    void                           *private_data)
{
    struct chimera_client_request *request;

    request = chimera_client_request_alloc(thread);

    request->opcode                    = CHIMERA_CLIENT_OP_WRITE_SAME;
    request->write_same.handle         = handle;
    request->write_same.offset         = offset;
    request->write_same.block_size     = block_size;
    request->write_same.block_count    = block_count;
    request->write_same.pattern        = pattern;
    request->write_same.pattern_len    = pattern_len;
    request->write_same.reloff_pattern = reloff_pattern;
    request->write_same.callback       = callback;
    request->write_same.private_data   = private_data;

    chimera_dispatch_write_same(thread, request);
} /* chimera_write_same */
