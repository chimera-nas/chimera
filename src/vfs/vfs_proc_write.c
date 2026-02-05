// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_open_cache.h"
#include "vfs_attr_cache.h"
#include "common/macros.h"

static void
chimera_vfs_write_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_write_callback_t callback = request->proto_callback;


    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_attr_cache_insert(request->thread->vfs->vfs_attr_cache,
                                      request->write.handle->fh_hash,
                                      request->write.handle->fh,
                                      request->write.handle->fh_len,
                                      &request->write.r_post_attr);
    }

    chimera_vfs_complete(request);

    callback(request->status,
             request->write.r_length,
             request->write.r_sync,
             &request->write.r_pre_attr,
             &request->write.r_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_write_complete */

SYMBOL_EXPORT void
chimera_vfs_write(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        count,
    uint32_t                        sync,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    struct evpl_iovec              *iov,
    int                             niov,
    chimera_vfs_write_callback_t    callback,
    void                           *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), 0, 0, NULL, NULL, private_data);
        return;
    }

    request->opcode                        = CHIMERA_VFS_OP_WRITE;
    request->complete                      = chimera_vfs_write_complete;
    request->write.handle                  = handle;
    request->write.offset                  = offset;
    request->write.length                  = count;
    request->write.sync                    = sync;
    request->write.r_pre_attr.va_req_mask  = pre_attr_mask;
    request->write.r_pre_attr.va_set_mask  = 0;
    request->write.r_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->write.r_post_attr.va_set_mask = 0;
    request->write.iov                     = iov;
    request->write.niov                    = niov;
    request->proto_callback                = callback;
    request->proto_private_data            = private_data;

    chimera_vfs_dispatch(request);


} /* chimera_vfs_write */
