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

/* When an fh-routed search is served by the default KV (the backend lacks
 * native KV), keys are stored with a 1-byte namespace prefix.  These
 * trampolines live in request->plugin_data and strip that prefix back off so
 * the caller observes exactly the keys it stored, identical to the native
 * path. */
struct chimera_vfs_kv_ns_search_ctx {
    chimera_vfs_search_keys_callback_t user_callback;
    chimera_vfs_search_keys_complete_t user_complete;
    void                              *user_private;
    uint8_t                            ns_len;
};

static int
chimera_vfs_kv_ns_search_cb(
    const void *key,
    uint32_t    key_len,
    const void *value,
    uint32_t    value_len,
    void       *private_data)
{
    struct chimera_vfs_kv_ns_search_ctx *ctx = private_data;

    return ctx->user_callback((const uint8_t *) key + ctx->ns_len,
                              key_len - ctx->ns_len,
                              value, value_len, ctx->user_private);
} /* chimera_vfs_kv_ns_search_cb */

static void
chimera_vfs_kv_ns_search_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_vfs_kv_ns_search_ctx *ctx = private_data;

    ctx->user_complete(error_code, ctx->user_private);
} /* chimera_vfs_kv_ns_search_complete */

/* Search a key range associated with the backend that serves `fh`, used to
 * enumerate handle-state records for a specific share at startup.  If that
 * backend has native KV the search runs there; otherwise it routes to the
 * default KV with the range namespaced by the source backend's fh_magic (see
 * chimera_vfs_kv_route_fh), and the prefix is stripped from results before they
 * reach the caller.  start/end keys are copied into the request scratch so the
 * caller need not keep them alive across the async dispatch. */
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
    struct chimera_vfs_kv_route route;
    uint8_t                    *scratch;

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
        complete(CHIMERA_VFS_PTR_ERR(request), private_data);
        return;
    }

    request->opcode   = CHIMERA_VFS_OP_SEARCH_KEYS;
    request->complete = chimera_vfs_search_keys_complete;

    if (route.fallback) {
        struct chimera_vfs_kv_ns_search_ctx *ctx = request->plugin_data;
        uint8_t                             *p;
        uint32_t                             off;

        ctx->user_callback = callback;
        ctx->user_complete = complete;
        ctx->user_private  = private_data;
        ctx->ns_len        = 1;

        off = (sizeof(*ctx) + 7) & ~7u;
        p   = (uint8_t *) request->plugin_data + off;

        /* start = ns || start_key */
        p[0] = route.ns;
        if (start_key_len) {
            memcpy(p + 1, start_key, start_key_len);
        }
        request->search_keys.start_key     = p;
        request->search_keys.start_key_len = start_key_len + 1;
        p                                 += start_key_len + 1;

        /* end = ns || end_key when bounded; otherwise the next namespace byte,
         * an upper bound that keeps the scan inside this backend's namespace. */
        if (end_key_len) {
            p[0] = route.ns;
            memcpy(p + 1, end_key, end_key_len);
            request->search_keys.end_key     = p;
            request->search_keys.end_key_len = end_key_len + 1;
        } else {
            p[0]                             = (uint8_t) (route.ns + 1);
            request->search_keys.end_key     = p;
            request->search_keys.end_key_len = 1;
        }

        request->search_keys.callback = chimera_vfs_kv_ns_search_cb;
        request->proto_callback       = chimera_vfs_kv_ns_search_complete;
        request->proto_private_data   = ctx;
    } else {
        scratch = request->plugin_data;
        if (start_key_len) {
            memcpy(scratch, start_key, start_key_len);
        }
        if (end_key_len) {
            memcpy(scratch + start_key_len, end_key, end_key_len);
        }

        request->search_keys.start_key     = start_key_len ? scratch : NULL;
        request->search_keys.start_key_len = start_key_len;
        request->search_keys.end_key       = end_key_len ? scratch + start_key_len : NULL;
        request->search_keys.end_key_len   = end_key_len;
        request->search_keys.callback      = callback;
        request->proto_callback            = complete;
        request->proto_private_data        = private_data;
    }

    chimera_vfs_dispatch(request);
} /* chimera_vfs_search_keys_at */
