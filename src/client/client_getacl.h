// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once
#include "client_internal.h"
#include "vfs/sdk/vfs_acl.h"

static void
chimera_getacl_sequence_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct chimera_client_request  *request      = private_data;
    struct chimera_client_thread   *thread       = request->thread;
    chimera_setattr_callback_t      callback     = request->getacl.callback;
    void                           *callback_arg = request->getacl.private_data;
    enum chimera_vfs_error          status       = chimera_vfs_compound_status(compound);
    const struct chimera_vfs_attrs *attr         = &chimera_vfs_compound_op(compound,
                                                                            chimera_vfs_compound_num_ops(compound) - 1)
        ->attr;

    /* Copy into caller storage only once the complete read has been accepted. */
    if (status == CHIMERA_VFS_OK) {
        if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_ACL) || !attr->va_acl) {
            /* Backend returned no ACL (mode-only object / backend without ACL
             * storage): report an empty ACL rather than failing. */
            request->getacl.r_acl_aces = 0;
            if (request->getacl.acl_bufsize >= chimera_acl_size(0)) {
                request->getacl.acl_buf->num_aces   = 0;
                request->getacl.acl_buf->ctrl_flags = 0;
            } else {
                status = CHIMERA_VFS_ERANGE;
            }
        } else {
            const struct chimera_acl *acl  = attr->va_acl;
            size_t                    need = chimera_acl_size(acl->num_aces);

            request->getacl.r_acl_aces = acl->num_aces;

            if (request->getacl.acl_bufsize >= need) {
                memcpy(request->getacl.acl_buf, acl, need);
            } else {
                status = CHIMERA_VFS_ERANGE;
            }
        }
    }


    chimera_vfs_compound_free(compound);
    chimera_client_request_free(thread, request);
    callback(thread, status, callback_arg);
} // chimera_getacl_sequence_complete

static inline void
chimera_dispatch_getacl(
    struct chimera_client_thread  *thread,
    struct chimera_client_request *request)
{
    struct chimera_vfs_compound *compound = chimera_client_compound_at_root(thread, request);
    int                          opened   = chimera_vfs_compound_add_open_path(compound,
                                                                               request->getacl.path, request->getacl.
                                                                               path_len,
                                                                               CHIMERA_VFS_OPEN_PATH |
                                                                               CHIMERA_VFS_OPEN_INFERRED, NULL, 0);
    int                          attr = chimera_vfs_compound_add_getattr(compound, CHIMERA_VFS_ATTR_ACL);

    if (opened >= 0 && attr >= 0) {
        chimera_vfs_compound_op_use_handle(compound, attr, opened);
    }
    chimera_frontend_compound_submit(compound, chimera_getacl_sequence_complete, request);
} // chimera_dispatch_getacl
