// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_open_cache.h"
#include "vfs_attr_cache.h"
#include "common/macros.h"

static void
chimera_vfs_copy_range_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_copy_range_callback_t callback = request->proto_callback;

    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_attr_cache_insert(request->thread, request->thread->vfs->vfs_attr_cache,
                                      request->copy_range.dst_handle->fh_hash,
                                      request->copy_range.dst_handle->fh,
                                      request->copy_range.dst_handle->fh_len,
                                      &request->copy_range.r_post_attr);
    }

    chimera_vfs_complete(request);

    callback(request->status,
             request->copy_range.r_length,
             &request->copy_range.r_pre_attr,
             &request->copy_range.r_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_copy_range_complete */

SYMBOL_EXPORT void
chimera_vfs_copy_range(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *src_handle,
    uint64_t                          src_offset,
    struct chimera_vfs_open_handle   *dst_handle,
    uint64_t                          dst_offset,
    uint64_t                          length,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    chimera_vfs_copy_range_callback_t callback,
    void                             *private_data)
{
    struct chimera_vfs_request *request;

    if (!(dst_handle->vfs_module->capabilities & CHIMERA_VFS_CAP_COPY_RANGE)) {
        callback(CHIMERA_VFS_ENOTSUP, 0, NULL, NULL, private_data);
        return;
    }

    request = chimera_vfs_request_alloc_by_handle(thread, cred, dst_handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), 0, NULL, NULL, private_data);
        return;
    }

    request->opcode                             = CHIMERA_VFS_OP_COPY_RANGE;
    request->complete                           = chimera_vfs_copy_range_complete;
    request->copy_range.src_handle              = src_handle;
    request->copy_range.dst_handle              = dst_handle;
    request->copy_range.src_offset              = src_offset;
    request->copy_range.dst_offset              = dst_offset;
    request->copy_range.length                  = length;
    request->copy_range.r_length                = 0;
    request->copy_range.r_pre_attr.va_req_mask  = pre_attr_mask;
    request->copy_range.r_pre_attr.va_set_mask  = 0;
    request->copy_range.r_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->copy_range.r_post_attr.va_set_mask = 0;
    request->proto_callback                     = callback;
    request->proto_private_data                 = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_copy_range */
