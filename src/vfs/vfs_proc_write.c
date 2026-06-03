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
chimera_vfs_write_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_write_callback_t callback = request->proto_callback;

    /* Release the implicit-lease pin taken before dispatch (no-op when none
     * was taken). */
    chimera_vfs_io_lease_release(request);

    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_attr_cache_insert(request->thread, request->thread->vfs->vfs_attr_cache,
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

static void
chimera_vfs_write_dispatch(
    struct chimera_vfs_thread            *thread,
    const struct chimera_vfs_cred        *cred,
    struct chimera_vfs_open_handle       *handle,
    uint64_t                              offset,
    uint32_t                              count,
    uint32_t                              sync,
    uint64_t                              pre_attr_mask,
    uint64_t                              post_attr_mask,
    struct evpl_iovec                    *iov,
    int                                   niov,
    const struct chimera_vfs_lease_owner *io_owner,
    chimera_vfs_write_callback_t          callback,
    void                                 *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), 0, 0, NULL, NULL, private_data);
        return;
    }

    request->opcode       = CHIMERA_VFS_OP_WRITE;
    request->complete     = chimera_vfs_write_complete;
    request->write.handle = handle;
    /* Anchor the implicit lease on the cached handle (chimera_vfs_io_lease_acquire). */
    request->io_handle                     = handle;
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

    /* Mediate the write through the lease layer (acquire/hold the implicit
     * lease for a leaseless actor, or break other holders' read caches for a
     * lease-holding client), then dispatch. */
    chimera_vfs_io_lease_acquire(request, io_owner, chimera_vfs_dispatch);
} /* chimera_vfs_write_dispatch */

/* Continuation for the first gated write on a handle (see read counterpart).
 * io_owner is copied by value (not by pointer) because the callback fires
 * after an async getattr has returned to the event loop -- the SMB caller's
 * stack frame that owned the original io_owner is gone by then.  Both the
 * has_io_owner flag and the copy let the dispatch tail re-emit a const-
 * pointer to a stable address. */
struct chimera_vfs_write_gate {
    struct chimera_vfs_thread      *thread;
    const struct chimera_vfs_cred  *cred;
    struct chimera_vfs_open_handle *handle;
    uint64_t                        offset;
    uint32_t                        count;
    uint32_t                        sync;
    uint64_t                        pre_attr_mask;
    uint64_t                        post_attr_mask;
    struct evpl_iovec              *iov;
    int                             niov;
    bool                            has_io_owner;
    struct chimera_vfs_lease_owner  io_owner;
    chimera_vfs_write_callback_t    callback;
    void                           *private_data;
};

static void
chimera_vfs_write_gate_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_write_gate *gate = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        gate->callback(error_code, 0, 0, NULL, NULL, gate->private_data);
        free(gate);
        return;
    }

    gate->handle->granted_access = chimera_vfs_access_check(attr, gate->cred,
                                                            CHIMERA_ACE_MASK_ALL);
    gate->handle->granted_valid = 1;

    if (!(gate->handle->granted_access & CHIMERA_ACE_WRITE_DATA)) {
        gate->callback(CHIMERA_VFS_EACCES, 0, 0, NULL, NULL, gate->private_data);
        free(gate);
        return;
    }

    chimera_vfs_write_dispatch(gate->thread, gate->cred, gate->handle,
                               gate->offset, gate->count, gate->sync,
                               gate->pre_attr_mask, gate->post_attr_mask,
                               gate->iov, gate->niov,
                               gate->has_io_owner ? &gate->io_owner : NULL,
                               gate->callback, gate->private_data);
    free(gate);
} /* chimera_vfs_write_gate_complete */

SYMBOL_EXPORT void
chimera_vfs_write_owned(
    struct chimera_vfs_thread            *thread,
    const struct chimera_vfs_cred        *cred,
    struct chimera_vfs_open_handle       *handle,
    uint64_t                              offset,
    uint32_t                              count,
    uint32_t                              sync,
    uint64_t                              pre_attr_mask,
    uint64_t                              post_attr_mask,
    struct evpl_iovec                    *iov,
    int                                   niov,
    const struct chimera_vfs_lease_owner *io_owner,
    chimera_vfs_write_callback_t          callback,
    void                                 *private_data)
{
    struct chimera_vfs_write_gate *gate;

    if (chimera_vfs_gate_needed(handle->vfs_module->capabilities, cred)) {
        if (handle->granted_valid) {
            if (!(handle->granted_access & CHIMERA_ACE_WRITE_DATA)) {
                callback(CHIMERA_VFS_EACCES, 0, 0, NULL, NULL, private_data);
                return;
            }
        } else {
            gate = malloc(sizeof(*gate));

            gate->thread         = thread;
            gate->cred           = cred;
            gate->handle         = handle;
            gate->offset         = offset;
            gate->count          = count;
            gate->sync           = sync;
            gate->pre_attr_mask  = pre_attr_mask;
            gate->post_attr_mask = post_attr_mask;
            gate->iov            = iov;
            gate->niov           = niov;
            if (io_owner) {
                /* Deep copy: the caller's io_owner is a stack variable that
                 * will be gone by the time the async getattr callback fires. */
                gate->has_io_owner = true;
                gate->io_owner     = *io_owner;
            } else {
                gate->has_io_owner = false;
            }
            gate->callback     = callback;
            gate->private_data = private_data;

            chimera_vfs_getattr(thread, cred, handle,
                                CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL,
                                chimera_vfs_write_gate_complete, gate);
            return;
        }
    }

    chimera_vfs_write_dispatch(thread, cred, handle, offset, count, sync,
                               pre_attr_mask, post_attr_mask, iov, niov,
                               io_owner, callback, private_data);
} /* chimera_vfs_write_owned */

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
    chimera_vfs_write_owned(thread, cred, handle, offset, count, sync,
                            pre_attr_mask, post_attr_mask, iov, niov,
                            NULL, callback, private_data);
} /* chimera_vfs_write */
