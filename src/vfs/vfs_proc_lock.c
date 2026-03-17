// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"

static void
chimera_vfs_lock_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_lock_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             request->lock.r_conflict_type,
             request->lock.r_conflict_offset,
             request->lock.r_conflict_length,
             request->lock.r_conflict_pid,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_lock_complete */

SYMBOL_EXPORT void
chimera_vfs_lock(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint64_t                        length,
    uint32_t                        lock_type,
    uint32_t                        flags,
    chimera_vfs_lock_callback_t     callback,
    void                           *private_data)
{
    struct chimera_vfs_request *request;

    if (!(handle->vfs_module->capabilities & CHIMERA_VFS_CAP_FS_LOCK)) {
        callback(CHIMERA_VFS_ENOTSUP, 0, 0, 0, 0, private_data);
        return;
    }

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), 0, 0, 0, 0, private_data);
        return;
    }

    request->opcode             = CHIMERA_VFS_OP_LOCK;
    request->complete           = chimera_vfs_lock_complete;
    request->lock.handle        = handle;
    request->lock.offset        = offset;
    request->lock.length        = length;
    request->lock.lock_type     = lock_type;
    request->lock.flags         = flags;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(request);

} /* chimera_vfs_lock */
