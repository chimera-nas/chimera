// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_internal_procs.h"
#include "vfs/vfs_pnfs.h"
#include "vfs/vfs_claim.h"
#include "vfs_internal.h"
#include "vfs_open_cache.h"
#include "vfs_attr_cache.h"
#include "common/macros.h"

static void
chimera_vfs_clone_range_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_clone_range_callback_t callback = request->proto_callback;

    chimera_vfs_io_claim_release(request);

    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_attr_cache_insert(request->thread, request->thread->vfs->vfs_attr_cache,
                                      request->clone_range.dst_handle->fh_hash,
                                      request->clone_range.dst_handle->fh,
                                      request->clone_range.dst_handle->fh_len,
                                      &request->clone_range.r_post_attr);
    }

    chimera_vfs_complete(request);

    callback(request->status,
             &request->clone_range.r_pre_attr,
             &request->clone_range.r_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_clone_range_complete */

SYMBOL_EXPORT void
chimera_vfs_clone_range_owned(
    struct chimera_vfs_thread         *thread,
    const struct chimera_vfs_cred     *cred,
    struct chimera_vfs_open_handle    *src_handle,
    uint64_t                           src_offset,
    struct chimera_vfs_open_handle    *dst_handle,
    uint64_t                           dst_offset,
    uint64_t                           length,
    uint64_t                           pre_attr_mask,
    uint64_t                           post_attr_mask,
    const struct chimera_claim_actor  *src_owner,
    const struct chimera_claim_actor  *dst_owner,
    chimera_vfs_clone_range_callback_t callback,
    void                              *private_data)
{
    struct chimera_vfs_request *request;

    /* Server-side range copy is declined outright while pNFS is configured.
     * Either handle may be DS-resident, so a correct implementation would have
     * to resolve both and drive the copy between two backing files; until it
     * does, ENOTSUP sends the caller down the read+write fallback, which is
     * redirected and therefore correct.  This costs an optimization, never
     * correctness -- every protocol that offers a server-side copy is required
     * to cope with the server refusing it. */
    if (chimera_vfs_pnfs_io_possible(thread, dst_handle) ||
        chimera_vfs_pnfs_io_possible(thread, src_handle)) {
        callback(CHIMERA_VFS_ENOTSUP, NULL, NULL, private_data);
        return;
    }

    if (!(dst_handle->vfs_module->capabilities & CHIMERA_VFS_CAP_CLONE_RANGE)) {
        callback(CHIMERA_VFS_ENOTSUP, NULL, NULL, private_data);
        return;
    }

    request = chimera_vfs_request_alloc_by_handle(thread, cred, dst_handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, private_data);
        return;
    }

    request->opcode                              = CHIMERA_VFS_OP_CLONE_RANGE;
    request->complete                            = chimera_vfs_clone_range_complete;
    request->clone_range.src_handle              = src_handle;
    request->clone_range.dst_handle              = dst_handle;
    request->clone_range.src_offset              = src_offset;
    request->clone_range.dst_offset              = dst_offset;
    request->clone_range.length                  = length;
    request->clone_range.r_pre_attr.va_req_mask  = pre_attr_mask;
    request->clone_range.r_pre_attr.va_set_mask  = 0;
    request->clone_range.r_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->clone_range.r_post_attr.va_set_mask = 0;
    request->proto_callback                      = callback;
    request->proto_private_data                  = private_data;

    /* Like an ordinary owned READ, the source actor needs no implicit cache
     * claim. The destination must fire WRITE invalidation and await synchronous
     * victims before the backend can change bytes. Legacy anonymous clone
     * admission is unchanged; callers using this variant already hold grants. */
    (void) src_owner;
    if (dst_owner) {
        request->io_handle      = dst_handle;
        request->io_owner       = *dst_owner;
        request->io_owner_valid = 1;
        request->io_view.owner  = &request->io_owner;
        chimera_vfs_io_claim_acquire(request, &request->io_owner, chimera_vfs_dispatch);
    } else {
        chimera_vfs_dispatch(request);
    }
} /* chimera_vfs_clone_range */

SYMBOL_EXPORT void
chimera_vfs_clone_range(
    struct chimera_vfs_thread         *thread,
    const struct chimera_vfs_cred     *cred,
    struct chimera_vfs_open_handle    *src_handle,
    uint64_t                           src_offset,
    struct chimera_vfs_open_handle    *dst_handle,
    uint64_t                           dst_offset,
    uint64_t                           length,
    uint64_t                           pre_attr_mask,
    uint64_t                           post_attr_mask,
    chimera_vfs_clone_range_callback_t callback,
    void                              *private_data)
{
    chimera_vfs_clone_range_owned(thread, cred, src_handle, src_offset,
                                  dst_handle, dst_offset, length, pre_attr_mask, post_attr_mask,
                                  NULL, NULL, callback, private_data);
} /* chimera_vfs_clone_range */
