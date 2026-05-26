// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs/vfs_state.h"
#include "vfs_internal.h"
#include "vfs_open_cache.h"
#include "vfs_attr_cache.h"
#include "common/macros.h"

/* Finalize VFS-core-provided read buffers on the success path.  The backend
 * read the 4 KiB-aligned range into request->read.iov starting at buffer offset
 * 0 (== file offset `offset - aligned_prefix`) and reported r_length (the bytes
 * the client actually asked for and that exist).  Skip the leading prefix pad
 * and trim trailing length so the iovecs sum to exactly r_length.  r_niov is
 * kept at the full provided count so the reply path releases every buffer we
 * allocated (trailing buffers simply shrink to length 0).  Mirrors what each
 * backend used to do for its own buffers (e.g. diskfs_read_adjust_iovecs). */
static void
chimera_vfs_read_finalize_buffers(struct chimera_vfs_request *request)
{
    struct evpl_iovec *iov      = request->read.iov;
    int                provided = request->read.buffers_provided;
    uint32_t           prefix   = request->read.aligned_prefix;
    uint64_t           total;
    int                i;

    if (request->read.r_length == 0) {
        evpl_iovecs_release(request->thread->evpl, iov, provided);
        request->read.r_niov = 0;
        return;
    }

    iov[0].data   += prefix;
    iov[0].length -= prefix;

    total = 0;
    for (i = 0; i < provided; i++) {
        total += iov[i].length;
    }

    if (total > request->read.r_length) {
        uint64_t excess = total - request->read.r_length;
        int      last   = provided - 1;

        while (excess > 0 && last >= 0) {
            if (iov[last].length >= excess) {
                iov[last].length -= excess;
                excess            = 0;
            } else {
                excess          -= iov[last].length;
                iov[last].length = 0;
                last--;
            }
        }
    }

    request->read.r_niov = provided;
} /* chimera_vfs_read_finalize_buffers */

static void
chimera_vfs_read_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_read_callback_t callback = request->proto_callback;

    /* Release the implicit-lease pin taken before dispatch (no-op when none
     * was taken). */
    chimera_vfs_io_lease_release(request);

    /* Account for buffers the VFS core allocated on behalf of a backend that
     * does not provide its own read memory.  On success, trim them to the
     * requested range; on any error the reply sends nothing, so release them
     * here (the reply path only releases on the success leg). */
    if (request->read.buffers_provided) {
        if (request->status == CHIMERA_VFS_OK) {
            chimera_vfs_read_finalize_buffers(request);
        } else {
            evpl_iovecs_release(request->thread->evpl, request->read.iov,
                                request->read.buffers_provided);
            request->read.r_niov   = 0;
            request->read.r_length = 0;
        }
    }

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
    request->read.buffers_provided   = 0;
    request->read.aligned_prefix     = 0;
    request->proto_callback          = callback;
    request->proto_private_data      = private_data;

    /* Buffer ownership.  Backends that advertise CAP_READ_PROVIDES_BUFFERS
     * supply their own read memory (memfs returns refs to its SHARED in-memory
     * blocks; the nfs proxy returns its upstream reply buffers).  For everyone
     * else the VFS core allocates the buffers HERE, on the connection thread,
     * padded to a 4 KiB boundary on both sides.  The (possibly worker-thread)
     * backend then only fills them; because the connection thread that
     * allocated them is also the one that releases them after the reply, no
     * cross-thread / SHARED iovec is required. */
    if (count > 0 &&
        !(request->module->capabilities & CHIMERA_VFS_CAP_READ_PROVIDES_BUFFERS)) {
        uint64_t aligned_offset = offset & ~4095ULL;
        uint64_t aligned_end    = (offset + count + 4095ULL) & ~4095ULL;
        uint32_t aligned_length = (uint32_t) (aligned_end - aligned_offset);
        int      n;

        n = evpl_iovec_alloc(thread->evpl, aligned_length, 4096, niov, 0,
                             request->read.iov);
        chimera_vfs_abort_if(n <= 0,
                             "vfs read: failed to allocate %u read-buffer bytes",
                             aligned_length);

        request->read.buffers_provided = n;
        request->read.aligned_prefix   = (uint32_t) (offset - aligned_offset);
    }

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