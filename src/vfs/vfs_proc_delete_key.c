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

/* Delete a key associated with the backend that serves `fh`, so handle-state
 * records persisted alongside a file are removed from the same place they were
 * written.  If that backend has native KV the op is dispatched to it; otherwise
 * it routes to the default KV module with the key namespaced by the source
 * backend's fh_magic (see chimera_vfs_kv_route_fh).  The key is copied into the
 * request scratch so the caller need not keep it alive across the async
 * dispatch. */
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
    struct chimera_vfs_kv_route route;
    uint8_t                    *scratch;

    chimera_vfs_kv_route_fh(thread, fh, fhlen, &route);

    if (route.fallback) {
        request = chimera_vfs_request_alloc_common(thread, NULL, route.module,
                                                   NULL, 0,
                                                   chimera_vfs_hash(key, key_len),
                                                   CHIMERA_VFS_CAP_KV);
    } else {
        request = chimera_vfs_request_alloc_by_hash(thread, cred, fh, fhlen,
                                                    chimera_vfs_hash(fh, fhlen));
    }

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), private_data);
        return;
    }

    scratch = request->plugin_data;

    if (route.fallback) {
        scratch[0] = route.ns;
        memcpy(scratch + 1, key, key_len);
        request->delete_key.key     = scratch;
        request->delete_key.key_len = key_len + 1;
    } else {
        memcpy(scratch, key, key_len);
        request->delete_key.key     = scratch;
        request->delete_key.key_len = key_len;
    }

    request->opcode             = CHIMERA_VFS_OP_DELETE_KEY;
    request->complete           = chimera_vfs_delete_key_complete;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_delete_key_at */
