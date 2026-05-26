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

static void
chimera_vfs_read_dispatch(
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
} /* chimera_vfs_read_dispatch */

/* Continuation for the first gated read on a handle: a getattr+ACL computes the
 * caller's effective access mask, which is cached on the handle for reuse. */
struct chimera_vfs_read_gate {
    struct chimera_vfs_thread            *thread;
    const struct chimera_vfs_cred        *cred;
    struct chimera_vfs_open_handle       *handle;
    uint64_t                              offset;
    uint32_t                              count;
    struct evpl_iovec                    *iov;
    int                                   niov;
    uint64_t                              attr_mask;
    const struct chimera_vfs_lease_owner *io_owner;
    chimera_vfs_read_callback_t           callback;
    void                                 *private_data;
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

    chimera_vfs_read_dispatch(gate->thread, gate->cred, gate->handle,
                              gate->offset, gate->count, gate->iov, gate->niov,
                              gate->attr_mask, gate->io_owner, gate->callback,
                              gate->private_data);
    free(gate);
} /* chimera_vfs_read_gate_complete */

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

            gate->thread       = thread;
            gate->cred         = cred;
            gate->handle       = handle;
            gate->offset       = offset;
            gate->count        = count;
            gate->iov          = iov;
            gate->niov         = niov;
            gate->attr_mask    = attr_mask;
            gate->io_owner     = io_owner;
            gate->callback     = callback;
            gate->private_data = private_data;

            chimera_vfs_getattr(thread, cred, handle,
                                CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL,
                                chimera_vfs_read_gate_complete, gate);
            return;
        }
    }

    chimera_vfs_read_dispatch(thread, cred, handle, offset, count, iov, niov,
                              attr_mask, io_owner, callback, private_data);
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