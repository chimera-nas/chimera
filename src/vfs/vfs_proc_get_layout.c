// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"

static void
chimera_vfs_get_layout_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_get_layout_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             request->get_layout.r_layout_class,
             request->get_layout.r_num_segments,
             request->get_layout.r_segments,
             request->get_layout.r_num_devices,
             request->get_layout.r_devices,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_get_layout_complete */

SYMBOL_EXPORT void
chimera_vfs_get_layout(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint64_t                          length,
    uint32_t                          iomode,
    uint32_t                          layout_class,
    uint32_t                          max_segments,
    chimera_vfs_get_layout_callback_t callback,
    void                             *private_data)
{
    struct chimera_vfs_request *request;

    /* Only a layout-sourcing backend can answer; orchestrated backends
     * (CHIMERA_VFS_CAP_LAYOUT) never see this op. */
    if (!(handle->vfs_module->capabilities & CHIMERA_VFS_CAP_LAYOUT_SOURCE)) {
        callback(CHIMERA_VFS_ENOTSUP, 0, 0, NULL, 0, NULL, private_data);
        return;
    }

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), 0, 0, NULL, 0, NULL, private_data);
        return;
    }

    if (max_segments > CHIMERA_VFS_LAYOUT_MAX_SEGMENTS) {
        max_segments = CHIMERA_VFS_LAYOUT_MAX_SEGMENTS;
    }

    request->opcode                    = CHIMERA_VFS_OP_GET_LAYOUT;
    request->complete                  = chimera_vfs_get_layout_complete;
    request->get_layout.handle         = handle;
    request->get_layout.offset         = offset;
    request->get_layout.length         = length;
    request->get_layout.iomode         = iomode;
    request->get_layout.layout_class   = layout_class;
    request->get_layout.max_segments   = max_segments;
    request->get_layout.r_layout_class = 0;
    request->get_layout.r_num_segments = 0;
    request->get_layout.r_num_devices  = 0;
    request->proto_callback            = callback;
    request->proto_private_data        = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_get_layout */
