// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"

static void
chimera_vfs_list_streams_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_list_streams_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             request->list_streams.buffer,
             request->list_streams.r_len,
             request->list_streams.r_count,
             request->list_streams.r_eof,
             request->list_streams.r_cookie,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_list_streams_complete */

SYMBOL_EXPORT void
chimera_vfs_list_streams(
    struct chimera_vfs_thread          *thread,
    const struct chimera_vfs_cred      *cred,
    struct chimera_vfs_open_handle     *handle,
    uint64_t                            cookie,
    void                               *buffer,
    uint32_t                            max_bytes,
    int                                 want_fh,
    chimera_vfs_list_streams_callback_t callback,
    void                               *private_data)
{
    struct chimera_vfs_request *request;

    if (!(handle->vfs_module->capabilities & CHIMERA_VFS_CAP_NAMED_STREAMS)) {
        callback(CHIMERA_VFS_ENOTSUP, NULL, 0, 0, 1, 0, private_data);
        return;
    }

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, 0, 0, 1, 0, private_data);
        return;
    }

    request->opcode                 = CHIMERA_VFS_OP_LIST_STREAMS;
    request->complete               = chimera_vfs_list_streams_complete;
    request->list_streams.handle    = handle;
    request->list_streams.cookie    = cookie;
    request->list_streams.buffer    = buffer;
    request->list_streams.max_bytes = max_bytes;
    request->list_streams.want_fh   = want_fh ? 1 : 0;
    request->list_streams.r_len     = 0;
    request->list_streams.r_count   = 0;
    request->list_streams.r_eof     = 0;
    request->list_streams.r_cookie  = 0;
    request->proto_callback         = callback;
    request->proto_private_data     = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_list_streams */
