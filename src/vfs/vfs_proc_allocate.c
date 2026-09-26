// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs/vfs_claim.h"
#include "vfs_internal.h"
#include "vfs_open_cache.h"
#include "vfs_attr_cache.h"
#include "common/macros.h"
static void
chimera_vfs_allocate_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_allocate_callback_t callback = request->proto_callback;

    chimera_vfs_io_claim_release(request);

    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_attr_cache_insert(request->thread, request->thread->vfs->vfs_attr_cache,
                                      request->allocate.handle->fh_hash,
                                      request->allocate.handle->fh,
                                      request->allocate.handle->fh_len,
                                      &request->allocate.r_post_attr);
    }

    chimera_vfs_complete(request);

    callback(request->status,
             &request->allocate.r_pre_attr,
             &request->allocate.r_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_allocate_complete */

SYMBOL_EXPORT void
chimera_vfs_allocate_owned(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint64_t                          length,
    uint32_t                          flags,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    const struct chimera_claim_actor *owner,
    chimera_vfs_allocate_callback_t   callback,
    void                             *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, private_data);
        return;
    }

    request->opcode                           = CHIMERA_VFS_OP_ALLOCATE;
    request->complete                         = chimera_vfs_allocate_complete;
    request->allocate.handle                  = handle;
    request->allocate.offset                  = offset;
    request->allocate.length                  = length;
    request->allocate.flags                   = flags;
    request->allocate.r_pre_attr.va_req_mask  = pre_attr_mask;
    request->allocate.r_pre_attr.va_set_mask  = 0;
    request->allocate.r_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->allocate.r_post_attr.va_set_mask = 0;
    request->proto_callback                   = callback;
    request->proto_private_data               = private_data;

    /* Claim-holding clients have already checked mandatory ranges against
     * their compound overlay. Invalidate peer data caches before changing the
     * allocated bytes, and wait for synchronous victims just as WRITE does. */
    if (owner) {
        request->io_handle      = handle;
        request->io_owner       = *owner;
        request->io_owner_valid = 1;
        request->io_view.owner  = &request->io_owner;
        chimera_vfs_io_claim_acquire(request, &request->io_owner, chimera_vfs_dispatch);
    } else {
        chimera_vfs_dispatch(request);
    }
} /* chimera_vfs_allocate_owned */

SYMBOL_EXPORT void
chimera_vfs_allocate(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint64_t                        length,
    uint32_t                        flags,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_allocate_callback_t callback,
    void                           *private_data)
{
    chimera_vfs_allocate_owned(thread, cred, handle, offset, length, flags,
                               pre_attr_mask, post_attr_mask, NULL, callback, private_data);
} /* chimera_vfs_allocate */
