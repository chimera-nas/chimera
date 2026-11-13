// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
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

static void
chimera_vfs_clone_range_dispatch_now(struct chimera_vfs_request *request)
{
    /* Restore the real opcode -- phase 2 below borrows CHIMERA_VFS_OP_WRITE
     * so the claim layer invalidates/recalls exactly as a WRITE would. */
    request->opcode = CHIMERA_VFS_OP_CLONE_RANGE;
    chimera_vfs_dispatch(request);
} /* chimera_vfs_clone_range_dispatch_now */

static void
chimera_vfs_clone_range_mediate_dst(struct chimera_vfs_request *request)
{
    /* Phase 2: mediate the destination the way WRITE does -- invalidate
     * other holders' read caches and recall conflicting claims -- before the
     * reflink overwrites dst's data.  Borrowing CHIMERA_VFS_OP_WRITE here is
     * what makes chimera_vfs_io_claim_acquire treat this as a write for
     * invalidation/implicit-claim purposes; it is restored to CLONE_RANGE
     * before the actual dispatch. */
    request->io_handle      = request->clone_range.dst_handle;
    request->opcode         = CHIMERA_VFS_OP_WRITE;
    request->io_owner_valid = request->clone_range.dst_io_owner_valid;
    if (request->clone_range.dst_io_owner_valid) {
        request->io_owner = request->clone_range.dst_io_owner;
    }

    chimera_vfs_io_claim_acquire(request,
                                 request->clone_range.dst_io_owner_valid
                                 ? &request->io_owner : NULL,
                                 chimera_vfs_clone_range_dispatch_now);
} /* chimera_vfs_clone_range_mediate_dst */

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
    const struct chimera_claim_actor  *src_io_owner,
    const struct chimera_claim_actor  *dst_io_owner,
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

    /* Stash the destination actor for phase 2 (chimera_vfs_clone_range_mediate_dst);
     * the generic io_owner slot below carries the source actor for phase 1. */
    request->clone_range.dst_io_owner_valid = dst_io_owner != NULL;
    if (dst_io_owner) {
        request->clone_range.dst_io_owner = *dst_io_owner;
    }

    /* Phase 1: mediate the source as READ does -- recall another client's
     * conflicting write claim before the reflink reads its data -- then
     * phase 2 mediates the destination, and only then is the reflink
     * actually issued.  This closes the gap where CLONE never recalled
     * another client's delegation the way READ/WRITE do. */
    request->io_handle      = src_handle;
    request->opcode         = CHIMERA_VFS_OP_READ;
    request->io_owner_valid = src_io_owner != NULL;
    if (src_io_owner) {
        request->io_owner = *src_io_owner;
    }

    chimera_vfs_io_claim_acquire(request,
                                 src_io_owner ? &request->io_owner : NULL,
                                 chimera_vfs_clone_range_mediate_dst);
} /* chimera_vfs_clone_range */
