// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"

static void
chimera_vfs_get_key_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_get_key_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             request->get_key.r_value,
             request->get_key.r_value_len,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_get_key_complete */

SYMBOL_EXPORT void
chimera_vfs_get_key(
    struct chimera_vfs_thread     *thread,
    const void                    *key,
    uint32_t                       key_len,
    chimera_vfs_get_key_callback_t callback,
    void                          *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_kv(thread, key, key_len);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, 0, private_data);
        return;
    }

    request->opcode              = CHIMERA_VFS_OP_GET_KEY;
    request->complete            = chimera_vfs_get_key_complete;
    request->get_key.key         = key;
    request->get_key.key_len     = key_len;
    request->get_key.r_value     = NULL;
    request->get_key.r_value_len = 0;
    request->proto_callback      = callback;
    request->proto_private_data  = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_get_key */
