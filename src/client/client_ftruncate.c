// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_fsetattr.h"

/*
 * Set the size of an open file (ftruncate). Implemented as an fsetattr that sets
 * only the size attribute, mirroring how the POSIX layer's chimera_posix_ftruncate
 * drives the same dispatch helper.
 */
SYMBOL_EXPORT void
chimera_ftruncate(
    struct chimera_client_thread   *thread,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        size,
    chimera_fsetattr_callback_t     callback,
    void                           *private_data)
{
    struct chimera_client_request *request;

    request = chimera_client_request_alloc(thread);

    request->opcode                = CHIMERA_CLIENT_OP_FSETATTR;
    request->fsetattr.handle       = handle;
    request->fsetattr.callback     = callback;
    request->fsetattr.private_data = private_data;

    request->fsetattr.set_attr.va_req_mask = 0;
    request->fsetattr.set_attr.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
    request->fsetattr.set_attr.va_size     = size;

    chimera_dispatch_fsetattr(thread, request);
} /* chimera_ftruncate */
