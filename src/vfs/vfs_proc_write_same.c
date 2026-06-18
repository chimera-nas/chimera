// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_attr_cache.h"
#include "common/macros.h"

static void
chimera_vfs_write_same_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_write_same_callback_t callback = request->proto_callback;

    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_attr_cache_insert(request->thread,
                                      request->thread->vfs->vfs_attr_cache,
                                      request->write_same.handle->fh_hash,
                                      request->write_same.handle->fh,
                                      request->write_same.handle->fh_len,
                                      &request->write_same.r_post_attr);
    }

    chimera_vfs_complete(request);

    callback(request->status,
             request->write_same.r_count,
             request->write_same.r_sync,
             &request->write_same.r_pre_attr,
             &request->write_same.r_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_write_same_complete */

SYMBOL_EXPORT void
chimera_vfs_write_same(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint32_t                          block_size,
    uint64_t                          block_count,
    const void                       *pattern,
    uint32_t                          pattern_len,
    uint32_t                          reloff_pattern,
    uint32_t                          sync,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    chimera_vfs_write_same_callback_t callback,
    void                             *private_data)
{
    struct chimera_vfs_request *request;

    if (!(handle->vfs_module->capabilities & CHIMERA_VFS_CAP_WRITE_SAME)) {
        callback(CHIMERA_VFS_ENOTSUP, 0, 0, NULL, NULL, private_data);
        return;
    }

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), 0, 0, NULL, NULL, private_data);
        return;
    }

    request->opcode                             = CHIMERA_VFS_OP_WRITE_SAME;
    request->complete                           = chimera_vfs_write_same_complete;
    request->write_same.handle                  = handle;
    request->write_same.offset                  = offset;
    request->write_same.block_size              = block_size;
    request->write_same.block_count             = block_count;
    request->write_same.pattern                 = pattern;
    request->write_same.pattern_len             = pattern_len;
    request->write_same.reloff_pattern          = reloff_pattern;
    request->write_same.sync                    = sync;
    request->write_same.r_count                 = 0;
    request->write_same.r_sync                  = sync;
    request->write_same.r_pre_attr.va_req_mask  = pre_attr_mask;
    request->write_same.r_pre_attr.va_set_mask  = 0;
    request->write_same.r_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->write_same.r_post_attr.va_set_mask = 0;
    request->proto_callback                     = callback;
    request->proto_private_data                 = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_write_same */
