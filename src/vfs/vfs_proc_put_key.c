// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"

static void
chimera_vfs_put_key_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_put_key_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status, request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_put_key_complete */

SYMBOL_EXPORT void
chimera_vfs_put_key(
    struct chimera_vfs_thread     *thread,
    const void                    *key,
    uint32_t                       key_len,
    const void                    *value,
    uint32_t                       value_len,
    chimera_vfs_put_key_callback_t callback,
    void                          *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_kv(thread, key, key_len);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), private_data);
        return;
    }

    request->opcode             = CHIMERA_VFS_OP_PUT_KEY;
    request->complete           = chimera_vfs_put_key_complete;
    request->put_key.key        = key;
    request->put_key.key_len    = key_len;
    request->put_key.value      = value;
    request->put_key.value_len  = value_len;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_put_key */

/* Store a key/value associated with the backend that serves `fh`.  If that
 * backend has native KV the record is stored co-located with the file;
 * otherwise it routes to the default KV with the key namespaced by the source
 * backend's fh_magic (see chimera_vfs_kv_route_fh) so it can later be found by
 * chimera_vfs_search_keys_at / removed by chimera_vfs_delete_key_at on the same
 * fh.  Both key and value are copied into the request scratch so the caller
 * need not keep them alive across the async dispatch. */
SYMBOL_EXPORT void
chimera_vfs_put_key_at(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const void                    *key,
    uint32_t                       key_len,
    const void                    *value,
    uint32_t                       value_len,
    chimera_vfs_put_key_callback_t callback,
    void                          *private_data)
{
    struct chimera_vfs_request *request;
    struct chimera_vfs_kv_route route;
    uint8_t                    *scratch;
    uint32_t                    key_off;

    chimera_vfs_kv_route_fh(thread, fh, fhlen, &route);

    if (route.fallback) {
        request = chimera_vfs_request_alloc_common(thread, NULL, route.module,
                                                   NULL, 0,
                                                   chimera_vfs_hash(fh, fhlen),
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
    key_off = 0;

    if (route.fallback) {
        scratch[0] = route.ns;
        key_off    = 1;
    }

    memcpy(scratch + key_off, key, key_len);
    memcpy(scratch + key_off + key_len, value, value_len);

    request->opcode             = CHIMERA_VFS_OP_PUT_KEY;
    request->complete           = chimera_vfs_put_key_complete;
    request->put_key.key        = scratch;
    request->put_key.key_len    = key_off + key_len;
    request->put_key.value      = scratch + key_off + key_len;
    request->put_key.value_len  = value_len;
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_put_key_at */
