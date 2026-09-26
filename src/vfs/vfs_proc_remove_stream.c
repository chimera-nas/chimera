// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_internal_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"

static void
chimera_vfs_remove_stream_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_remove_stream_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             &request->remove_stream.r_pre_attr,
             &request->remove_stream.r_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_remove_stream_complete */

SYMBOL_EXPORT void
chimera_vfs_remove_stream_checked(
    struct chimera_vfs_thread           *thread,
    const struct chimera_vfs_cred       *cred,
    struct chimera_vfs_open_handle      *handle,
    const char                          *name,
    uint32_t                             namelen,
    uint32_t                             flags,
    const uint8_t                       *expected_fh,
    uint32_t                             expected_fh_len,
    chimera_vfs_remove_stream_callback_t callback,
    void                                *private_data)
{
    struct chimera_vfs_request *request;

    if ((flags & ~CHIMERA_VFS_REMOVE_STREAM_MATCH_FH) ||
        namelen > CHIMERA_VFS_NAME_MAX || !name || !namelen ||
        ((flags & CHIMERA_VFS_REMOVE_STREAM_MATCH_FH) &&
         (!expected_fh || !expected_fh_len || expected_fh_len > CHIMERA_VFS_FH_SIZE))) {
        callback(CHIMERA_VFS_EINVAL, NULL, NULL, private_data);
        return;
    }
    if ((flags & CHIMERA_VFS_REMOVE_STREAM_MATCH_FH) &&
        !(handle->vfs_module->capabilities & CHIMERA_VFS_CAP_REMOVE_STREAM_MATCH_FH)) {
        callback(CHIMERA_VFS_ENOTSUP, NULL, NULL, private_data);
        return;
    }
    if (!(handle->vfs_module->capabilities & CHIMERA_VFS_CAP_NAMED_STREAMS)) {
        callback(CHIMERA_VFS_ENOTSUP, NULL, NULL, private_data);
        return;
    }

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, private_data);
        return;
    }

    request->opcode                        = CHIMERA_VFS_OP_REMOVE_STREAM;
    request->complete                      = chimera_vfs_remove_stream_complete;
    request->remove_stream.handle          = handle;
    request->remove_stream.name            = name;
    request->remove_stream.namelen         = namelen;
    request->remove_stream.flags           = flags;
    request->remove_stream.expected_fh_len = 0;
    if (flags & CHIMERA_VFS_REMOVE_STREAM_MATCH_FH) {
        memcpy(request->remove_stream.expected_fh, expected_fh, expected_fh_len);
        request->remove_stream.expected_fh_len = expected_fh_len;
    }
    request->remove_stream.r_pre_attr.va_req_mask  = 0;
    request->remove_stream.r_pre_attr.va_set_mask  = 0;
    request->remove_stream.r_post_attr.va_req_mask = 0;
    request->remove_stream.r_post_attr.va_set_mask = 0;
    request->proto_callback                        = callback;
    request->proto_private_data                    = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_remove_stream_checked */

SYMBOL_EXPORT void
chimera_vfs_remove_stream(
    struct chimera_vfs_thread           *thread,
    const struct chimera_vfs_cred       *cred,
    struct chimera_vfs_open_handle      *handle,
    const char                          *name,
    uint32_t                             namelen,
    chimera_vfs_remove_stream_callback_t callback,
    void                                *private_data)
{
    chimera_vfs_remove_stream_checked(thread, cred, handle, name, namelen,
                                      0, NULL, 0, callback, private_data);
} /* chimera_vfs_remove_stream */
