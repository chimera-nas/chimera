// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include "vfs/vfs_procs.h"
#include "vfs/vfs_state.h"
#include "vfs_internal.h"
#include "vfs_open_cache.h"
#include "vfs_attr_cache.h"
#include "vfs_access.h"
#include "vfs_acl.h"
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

/* Scatter-copy up to `length` bytes from a source iovec vector into a
 * destination iovec vector (used by read_into when a backend could not land the
 * data directly in the caller's buffers). */
static void
chimera_vfs_read_iovec_copy(
    struct evpl_iovec *dst,
    int                dst_niov,
    struct evpl_iovec *src,
    int                src_niov,
    uint32_t           length)
{
    int      di = 0, si = 0;
    uint32_t doff = 0, soff = 0, copied = 0;

    while (copied < length && di < dst_niov && si < src_niov) {
        uint32_t davail = dst[di].length - doff;
        uint32_t savail = src[si].length - soff;
        uint32_t chunk  = davail < savail ? davail : savail;

        if (chunk > length - copied) {
            chunk = length - copied;
        }

        if (chunk) {
            memcpy((char *) dst[di].data + doff, (char *) src[si].data + soff, chunk);
            doff   += chunk;
            soff   += chunk;
            copied += chunk;
        }

        if (doff == dst[di].length) {
            di++;
            doff = 0;
        }
        if (soff == src[si].length) {
            si++;
            soff = 0;
        }
    }
} /* chimera_vfs_read_iovec_copy */

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

    /* read_into: the caller wants the data in its own destination buffers.
     * Unless a backend already landed it there (landed_in_dest), copy the
     * result out of the scratch/backend buffers into dest and release those
     * source buffers on the caller's behalf (read_into has no reply path that
     * would otherwise consume and release them).  The caller owns dest and
     * releases it itself, so the callback hands dest back as iov/niov. */
    if (request->read.dest_provided) {
        if (request->status == CHIMERA_VFS_OK && !request->read.landed_in_dest) {
            chimera_vfs_read_iovec_copy(request->read.dest_iov,
                                        request->read.dest_niov,
                                        request->read.iov,
                                        request->read.r_niov,
                                        request->read.r_length);
            evpl_iovecs_release(request->thread->evpl, request->read.iov,
                                request->read.r_niov);
        }
        request->read.iov    = request->read.dest_iov;
        request->read.r_niov = request->read.dest_niov;
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
        chimera_vfs_attr_cache_refresh(request->thread, request->thread->vfs->vfs_attr_cache,
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

static void
chimera_vfs_read_dispatch(
    struct chimera_vfs_thread            *thread,
    const struct chimera_vfs_cred        *cred,
    struct chimera_vfs_transaction       *txn,
    struct chimera_vfs_open_handle       *handle,
    uint64_t                              offset,
    uint32_t                              count,
    struct evpl_iovec                    *iov,
    int                                   niov,
    struct evpl_iovec                    *dest_iov,
    int                                   dest_niov,
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

    request->transaction = txn;

    request->opcode      = CHIMERA_VFS_OP_READ;
    request->complete    = chimera_vfs_read_complete;
    request->read.handle = handle;
    /* Anchor the implicit lease on the cached handle (chimera_vfs_io_lease_acquire). */
    request->io_handle               = handle;
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
    request->read.dest_iov           = dest_iov;
    request->read.dest_niov          = dest_niov;
    request->read.dest_provided      = (dest_iov != NULL);
    request->read.landed_in_dest     = 0;
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
} /* chimera_vfs_read_dispatch */

/* Continuation for the first gated read on a handle: a getattr+ACL computes the
 * caller's effective access mask, which is cached on the handle for reuse. */
struct chimera_vfs_read_gate {
    struct chimera_vfs_thread      *thread;
    const struct chimera_vfs_cred  *cred;
    struct chimera_vfs_transaction *txn;
    struct chimera_vfs_open_handle *handle;
    uint64_t                        offset;
    uint32_t                        count;
    struct evpl_iovec              *iov;
    int                             niov;
    struct evpl_iovec              *dest_iov;
    int                             dest_niov;
    uint64_t                        attr_mask;
    /* io_owner is copied by value (not by pointer): the gate's async getattr
     * callback fires after the caller's stack frame is gone. */
    bool                            has_io_owner;
    struct chimera_vfs_lease_owner  io_owner;
    chimera_vfs_read_callback_t     callback;
    void                           *private_data;
};

static void
chimera_vfs_read_gate_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_read_gate *gate = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        gate->callback(error_code, 0, 0, NULL, 0, NULL, gate->private_data);
        free(gate);
        return;
    }

    gate->handle->granted_access = chimera_vfs_access_check(attr, gate->cred,
                                                            CHIMERA_ACE_MASK_ALL);
    gate->handle->granted_valid = 1;

    if (!(gate->handle->granted_access & CHIMERA_ACE_READ_DATA)) {
        gate->callback(CHIMERA_VFS_EACCES, 0, 0, NULL, 0, NULL, gate->private_data);
        free(gate);
        return;
    }

    chimera_vfs_read_dispatch(gate->thread, gate->cred, gate->txn, gate->handle,
                              gate->offset, gate->count, gate->iov, gate->niov,
                              gate->dest_iov, gate->dest_niov,
                              gate->attr_mask,
                              gate->has_io_owner ? &gate->io_owner : NULL,
                              gate->callback,
                              gate->private_data);
    free(gate);
} /* chimera_vfs_read_gate_complete */

/* Common path for chimera_vfs_read{,_owned,_into}: mediate the read through the
 * optional ACL gate, then dispatch.  dest_iov is NULL for the plain reads and
 * the caller's destination for read_into. */
static void
chimera_vfs_read_submit(
    struct chimera_vfs_thread            *thread,
    const struct chimera_vfs_cred        *cred,
    struct chimera_vfs_transaction       *txn,
    struct chimera_vfs_open_handle       *handle,
    uint64_t                              offset,
    uint32_t                              count,
    struct evpl_iovec                    *iov,
    int                                   niov,
    struct evpl_iovec                    *dest_iov,
    int                                   dest_niov,
    uint64_t                              attr_mask,
    const struct chimera_vfs_lease_owner *io_owner,
    chimera_vfs_read_callback_t           callback,
    void                                 *private_data)
{
    struct chimera_vfs_read_gate *gate;

    if (chimera_vfs_gate_needed(handle->vfs_module->capabilities, cred)) {
        if (handle->granted_valid) {
            /* Fast path: the caller's grant is cached on the handle. */
            if (!(handle->granted_access & CHIMERA_ACE_READ_DATA)) {
                callback(CHIMERA_VFS_EACCES, 0, 0, NULL, 0, NULL, private_data);
                return;
            }
        } else {
            /* First gated I/O on this handle: compute and cache the grant. */
            gate = malloc(sizeof(*gate));

            gate->thread    = thread;
            gate->cred      = cred;
            gate->txn       = txn;
            gate->handle    = handle;
            gate->offset    = offset;
            gate->count     = count;
            gate->iov       = iov;
            gate->niov      = niov;
            gate->dest_iov  = dest_iov;
            gate->dest_niov = dest_niov;
            gate->attr_mask = attr_mask;
            if (io_owner) {
                /* Deep copy: see read_gate struct comment. */
                gate->has_io_owner = true;
                gate->io_owner     = *io_owner;
            } else {
                gate->has_io_owner = false;
            }
            gate->callback     = callback;
            gate->private_data = private_data;

            chimera_vfs_getattr(thread, cred, NULL, handle,
                                CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL,
                                chimera_vfs_read_gate_complete, gate);
            return;
        }
    }

    chimera_vfs_read_dispatch(thread, cred, txn, handle, offset, count, iov, niov,
                              dest_iov, dest_niov, attr_mask, io_owner, callback,
                              private_data);
} /* chimera_vfs_read_submit */

SYMBOL_EXPORT void
chimera_vfs_read_owned(
    struct chimera_vfs_thread            *thread,
    const struct chimera_vfs_cred        *cred,
    struct chimera_vfs_transaction       *txn,
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
    chimera_vfs_read_submit(thread, cred, txn, handle, offset, count, iov, niov,
                            NULL, 0, attr_mask, io_owner, callback, private_data);
} /* chimera_vfs_read_owned */

SYMBOL_EXPORT void
chimera_vfs_read(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_transaction *txn,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        count,
    struct evpl_iovec              *iov,
    int                             niov,
    uint64_t                        attr_mask,
    chimera_vfs_read_callback_t     callback,
    void                           *private_data)
{
    chimera_vfs_read_submit(thread, cred, txn, handle, offset, count, iov, niov,
                            NULL, 0, attr_mask, NULL, callback, private_data);
} /* chimera_vfs_read */

SYMBOL_EXPORT void
chimera_vfs_read_into(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_transaction *txn,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        count,
    struct evpl_iovec              *work_iov,
    int                             work_niov,
    struct evpl_iovec              *dest_iov,
    int                             dest_niov,
    uint64_t                        attr_mask,
    chimera_vfs_read_callback_t     callback,
    void                           *private_data)
{
    chimera_vfs_read_submit(thread, cred, txn, handle, offset, count,
                            work_iov, work_niov, dest_iov, dest_niov,
                            attr_mask, NULL, callback, private_data);
} /* chimera_vfs_read_into */