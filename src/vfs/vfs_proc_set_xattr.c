// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"

static void
chimera_vfs_set_xattr_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_set_xattr_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             &request->set_xattr.r_pre_attr,
             &request->set_xattr.r_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_set_xattr_complete */

SYMBOL_EXPORT void
chimera_vfs_set_xattr(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    uint32_t                         option,
    const char                      *name,
    uint32_t                         namelen,
    const void                      *value,
    uint32_t                         value_len,
    chimera_vfs_set_xattr_callback_t callback,
    void                            *private_data)
{
    struct chimera_vfs_request *request;

    if (!(handle->vfs_module->capabilities & CHIMERA_VFS_CAP_XATTR)) {
        callback(CHIMERA_VFS_ENOTSUP, NULL, NULL, private_data);
        return;
    }

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, private_data);
        return;
    }

    request->opcode                            = CHIMERA_VFS_OP_SET_XATTR;
    request->complete                          = chimera_vfs_set_xattr_complete;
    request->set_xattr.handle                  = handle;
    request->set_xattr.option                  = option;
    request->set_xattr.name                    = name;
    request->set_xattr.namelen                 = namelen;
    request->set_xattr.value                   = value;
    request->set_xattr.value_len               = value_len;
    request->set_xattr.r_pre_attr.va_req_mask  = CHIMERA_VFS_ATTR_MASK_STAT;
    request->set_xattr.r_pre_attr.va_set_mask  = 0;
    request->set_xattr.r_post_attr.va_req_mask = CHIMERA_VFS_ATTR_MASK_STAT;
    request->set_xattr.r_post_attr.va_set_mask = 0;
    request->proto_callback                    = callback;
    request->proto_private_data                = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_set_xattr */
