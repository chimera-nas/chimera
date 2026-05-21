// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"
#include "common/macros.h"

static void
chimera_vfs_delete_key_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_delete_key_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status, request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_delete_key_complete */

SYMBOL_EXPORT void
chimera_vfs_delete_key(
    struct chimera_vfs_thread        *thread,
    const void                       *key,
    uint32_t                          key_len,
    chimera_vfs_delete_key_callback_t callback,
    void                             *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_kv(thread, key, key_len);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), private_data);
        return;
    }

    request->opcode             = CHIMERA_VFS_OP_DELETE_KEY;
    request->complete           = chimera_vfs_delete_key_complete;
    request->delete_key.key     = key;
    request->delete_key.key_len = key_len;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_delete_key */

/* Delete a key from the backend that serves `fh` (rather than the global
 * kv_module), so handle-state records persisted alongside a file are removed
 * from the same backend.  The key is copied into the request scratch so the
 * caller need not keep it alive across the async dispatch. */
SYMBOL_EXPORT void
chimera_vfs_delete_key_at(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    const void                       *fh,
    int                               fhlen,
    const void                       *key,
    uint32_t                          key_len,
    chimera_vfs_delete_key_callback_t callback,
    void                             *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_by_hash(thread, cred, fh, fhlen,
                                                chimera_vfs_hash(fh, fhlen));

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), private_data);
        return;
    }

    memcpy(request->plugin_data, key, key_len);

    request->opcode             = CHIMERA_VFS_OP_DELETE_KEY;
    request->complete           = chimera_vfs_delete_key_complete;
    request->delete_key.key     = request->plugin_data;
    request->delete_key.key_len = key_len;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_delete_key_at */
