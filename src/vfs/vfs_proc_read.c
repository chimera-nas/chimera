// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs/vfs_state.h"
#include "vfs_internal.h"
#include "vfs_open_cache.h"
#include "vfs_attr_cache.h"
#include "common/macros.h"

static void
chimera_vfs_read_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_read_callback_t callback = request->proto_callback;

    /* Release the implicit-lease pin taken before dispatch (no-op when none
     * was taken). */
    chimera_vfs_io_lease_release(request);

    /* Only refresh the attr cache when the caller actually requested stat
     * attributes (e.g. NFSv3 READ, which carries post_op_attr).  READ is
     * never a mutating operation, so it has no role in keeping the cache
     * coherent for WCC -- mutating ops insert their own post-op attrs.  For
     * callers that don't want attrs (NFSv4/SMB/S3 READ pass attr_mask 0) the
     * backend skips the stat entirely, so there is nothing to cache and
     * inserting here would only evict a valid entry. */
    if (request->status == CHIMERA_VFS_OK &&
        (request->read.r_attr.va_set_mask & CHIMERA_VFS_ATTR_MASK_STAT) == CHIMERA_VFS_ATTR_MASK_STAT) {
        chimera_vfs_attr_cache_insert(request->thread->vfs->vfs_attr_cache,
                                      request->read.handle->fh_hash,
                                      request->read.handle->fh,
                                      request->read.handle->fh_len,
                                      &request->read.r_attr);
    }

    chimera_vfs_complete(request);

    callback(request->status,
             request->read.r_length,
             request->read.r_eof,
             request->read.iov,
             request->read.r_niov,
             &request->read.r_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_read_complete */

SYMBOL_EXPORT void
chimera_vfs_read_owned(
    struct chimera_vfs_thread            *thread,
    const struct chimera_vfs_cred        *cred,
    struct chimera_vfs_open_handle       *handle,
    uint64_t                              offset,
    uint32_t                              count,
    struct evpl_iovec                    *iov,
    int                                   niov,
    uint64_t                              attr_mask,
    const struct chimera_vfs_lease_owner *io_owner,
    chimera_vfs_read_callback_t           callback,
    void                                 *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), 0, 0, NULL, 0, NULL, private_data);
        return;
    }

    request->opcode                  = CHIMERA_VFS_OP_READ;
    request->complete                = chimera_vfs_read_complete;
    request->read.handle             = handle;
    request->read.offset             = offset;
    request->read.length             = count;
    request->read.iov                = iov;
    request->read.niov               = niov;
    request->read.r_length           = 0;
    request->read.r_niov             = 0;
    request->read.r_eof              = 0;
    request->read.r_attr.va_req_mask = attr_mask;
    request->read.r_attr.va_set_mask = 0;
    request->proto_callback          = callback;
    request->proto_private_data      = private_data;

    /* Mediate the read through the lease layer (acquire/hold the implicit
     * lease for a leaseless actor, recalling another holder's conflicting
     * write cache), then dispatch. */
    chimera_vfs_io_lease_acquire(request, io_owner, chimera_vfs_dispatch);
} /* chimera_vfs_read_owned */

SYMBOL_EXPORT void
chimera_vfs_read(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        count,
    struct evpl_iovec              *iov,
    int                             niov,
    uint64_t                        attr_mask,
    chimera_vfs_read_callback_t     callback,
    void                           *private_data)
{
    chimera_vfs_read_owned(thread, cred, handle, offset, count, iov, niov,
                           attr_mask, NULL, callback, private_data);
} /* chimera_vfs_read */