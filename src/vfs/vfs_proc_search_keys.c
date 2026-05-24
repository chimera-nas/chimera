// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"
#include "common/macros.h"

static void
chimera_vfs_search_keys_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_search_keys_complete_t complete = request->proto_callback;

    chimera_vfs_complete(request);

    complete(request->status, request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_search_keys_complete */

SYMBOL_EXPORT void
chimera_vfs_search_keys(
    struct chimera_vfs_thread         *thread,
    const void                        *start_key,
    uint32_t                           start_key_len,
    const void                        *end_key,
    uint32_t                           end_key_len,
    chimera_vfs_search_keys_callback_t callback,
    chimera_vfs_search_keys_complete_t complete,
    void                              *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_kv(thread, start_key, start_key_len);

    if (CHIMERA_VFS_IS_ERR(request)) {
        complete(CHIMERA_VFS_PTR_ERR(request), private_data);
        return;
    }

    request->opcode                    = CHIMERA_VFS_OP_SEARCH_KEYS;
    request->complete                  = chimera_vfs_search_keys_complete;
    request->search_keys.start_key     = start_key;
    request->search_keys.start_key_len = start_key_len;
    request->search_keys.end_key       = end_key;
    request->search_keys.end_key_len   = end_key_len;
    request->search_keys.callback      = callback;
    request->proto_callback            = complete;
    request->proto_private_data        = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_search_keys */

/* Search a key range in the backend that serves `fh` (rather than the global
 * kv_module), used to enumerate handle-state records on a specific share's
 * backend at startup.  start/end keys are copied into the request scratch so
 * the caller need not keep them alive across the async dispatch. */
SYMBOL_EXPORT void
chimera_vfs_search_keys_at(
    struct chimera_vfs_thread         *thread,
    const struct chimera_vfs_cred     *cred,
    const void                        *fh,
    int                                fhlen,
    const void                        *start_key,
    uint32_t                           start_key_len,
    const void                        *end_key,
    uint32_t                           end_key_len,
    chimera_vfs_search_keys_callback_t callback,
    chimera_vfs_search_keys_complete_t complete,
    void                              *private_data)
{
    struct chimera_vfs_request *request;
    uint8_t                    *scratch;

    request = chimera_vfs_request_alloc_by_hash(thread, cred, fh, fhlen,
                                                chimera_vfs_hash(fh, fhlen));

    if (CHIMERA_VFS_IS_ERR(request)) {
        complete(CHIMERA_VFS_PTR_ERR(request), private_data);
        return;
    }

    scratch = request->plugin_data;
    if (start_key_len) {
        memcpy(scratch, start_key, start_key_len);
    }
    if (end_key_len) {
        memcpy(scratch + start_key_len, end_key, end_key_len);
    }

    request->opcode                    = CHIMERA_VFS_OP_SEARCH_KEYS;
    request->complete                  = chimera_vfs_search_keys_complete;
    request->search_keys.start_key     = start_key_len ? scratch : NULL;
    request->search_keys.start_key_len = start_key_len;
    request->search_keys.end_key       = end_key_len ? scratch + start_key_len : NULL;
    request->search_keys.end_key_len   = end_key_len;
    request->search_keys.callback      = callback;
    request->proto_callback            = complete;
    request->proto_private_data        = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_search_keys_at */
