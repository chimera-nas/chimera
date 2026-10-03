// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"

static void
chimera_vfs_rename_stream_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_rename_stream_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             &request->rename_stream.r_pre_attr,
             &request->rename_stream.r_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_rename_stream_complete */

SYMBOL_EXPORT void
chimera_vfs_rename_stream(
    struct chimera_vfs_thread           *thread,
    const struct chimera_vfs_cred       *cred,
    struct chimera_vfs_open_handle      *handle,
    const char                          *name,
    uint32_t                             namelen,
    const char                          *new_name,
    uint32_t                             new_namelen,
    uint32_t                             flags,
    chimera_vfs_rename_stream_callback_t callback,
    void                                *private_data)
{
    struct chimera_vfs_request *request;

    if (!(handle->vfs_module->capabilities & CHIMERA_VFS_CAP_NAMED_STREAMS)) {
        callback(CHIMERA_VFS_ENOTSUP, NULL, NULL, private_data);
        return;
    }

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, private_data);
        return;
    }

    request->opcode                                = CHIMERA_VFS_OP_RENAME_STREAM;
    request->complete                              = chimera_vfs_rename_stream_complete;
    request->rename_stream.handle                  = handle;
    request->rename_stream.name                    = name;
    request->rename_stream.namelen                 = namelen;
    request->rename_stream.new_name                = new_name;
    request->rename_stream.new_namelen             = new_namelen;
    request->rename_stream.flags                   = flags;
    request->rename_stream.r_pre_attr.va_req_mask  = 0;
    request->rename_stream.r_pre_attr.va_set_mask  = 0;
    request->rename_stream.r_post_attr.va_req_mask = 0;
    request->rename_stream.r_post_attr.va_set_mask = 0;
    request->proto_callback                        = callback;
    request->proto_private_data                    = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_rename_stream */
