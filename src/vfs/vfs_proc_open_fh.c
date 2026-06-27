// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"
#include "vfs_open_cache.h"
#include "vfs_access.h"
#include "vfs_release.h"
#include "common/macros.h"
static void
chimera_vfs_open_fh_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread      *thread   = request->thread;
    struct chimera_vfs_open_handle *handle   = request->pending_handle;
    chimera_vfs_open_fh_callback_t  callback = request->proto_callback;

    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_populate_handle(thread, handle, request->open_fh.r_vfs_private);
    } else {
        chimera_vfs_release_failed(thread, handle, request->status);
        handle = NULL;
    }

    chimera_vfs_complete(request);

    callback(request->status,
             handle,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);

} /* chimera_vfs_open_fh_complete */

static void
chimera_vfs_open_fh_hdl_callback(
    struct chimera_vfs_request     *request,
    struct chimera_vfs_open_handle *handle)
{
    chimera_vfs_open_fh_callback_t callback = request->proto_callback;



    if (!handle) {
        /* Someone was already in process opening the file when we tried
         * and they failed, so we fail too.
         */
        callback(request->status, NULL, request->proto_private_data);
        chimera_vfs_request_free(request->thread, request);
    } else if (handle->flags & CHIMERA_VFS_OPEN_HANDLE_PENDING) {
        /* Miss on the open cache, so a pending open record was inserted
         * for us and its now our job to actually dispatch the open
         */
        request->pending_handle = handle;
        chimera_vfs_dispatch(request);
    } else {
        /* File was already open in the cache so we're done */
        callback(CHIMERA_VFS_OK, handle, request->proto_private_data);
        chimera_vfs_request_free(request->thread, request);
    }
} /* chimera_vfs_open_fh_hdl_callback */

SYMBOL_EXPORT void
chimera_vfs_open_fh_hs(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_transaction  *txn,
    const void                      *fh,
    int                              fhlen,
    unsigned int                     flags,
    struct chimera_vfs_handle_state *handle_state,
    chimera_vfs_open_fh_callback_t   callback,
    void                            *private_data)
{
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;
    struct vfs_open_cache      *cache;
    uint64_t                    fh_hash;

    if (flags & CHIMERA_VFS_OPEN_PATH) {
        cache = thread->vfs->vfs_open_path_cache;
    } else {
        cache = thread->vfs->vfs_open_file_cache;
    }

    fh_hash = chimera_vfs_hash(fh, fhlen);

    module = chimera_vfs_get_module(thread, fh, fhlen);

    if (!module) {
        callback(CHIMERA_VFS_ESTALE, NULL, private_data);
        return;
    }

    if ((module->capabilities & CHIMERA_VFS_CAP_OPEN_FILE_REQUIRED) || !(flags & CHIMERA_VFS_OPEN_INFERRED) ||
        chimera_vfs_gate_needed(module->capabilities, cred)) {

        /* We really need to open the file -- or we are on an engine-authoritative
         * backend for a non-exempt caller, in which case we route the inferred
         * open through the (credential-keyed) cache so the per-caller ACL grant
         * computed on first I/O is cached and reused, rather than re-derived on a
         * throwaway synthetic handle for every operation. */

        request = chimera_vfs_request_alloc_by_hash(thread, cred, fh, fhlen, fh_hash);

        if (CHIMERA_VFS_IS_ERR(request)) {
            callback(CHIMERA_VFS_PTR_ERR(request), NULL, private_data);
            return;
        }

        request->transaction = txn;

        request->opcode               = CHIMERA_VFS_OP_OPEN_FH;
        request->complete             = chimera_vfs_open_fh_complete;
        request->open_fh.flags        = flags;
        request->open_fh.handle_state = handle_state;
        request->proto_callback       = callback;
        request->proto_private_data   = private_data;

        chimera_vfs_open_cache_acquire(
            thread,
            cache,
            module,
            request,
            fh,
            fhlen,
            fh_hash,
            UINT64_MAX,
            request->open_fh.flags,
            0,
            chimera_vfs_open_fh_hdl_callback);

    } else {

        /* Inferred open on a backend that does not require a real open handle.
         * Keep a cached handle anyway so per-handle state (notably VFS lease
         * mediation) is attached once per cache lifetime instead of once per
         * NFSv3-style stateless operation.  The handle has no backend open, so
         * cache eviction/release must skip chimera_vfs_close().
         */
        request = chimera_vfs_request_alloc_by_hash(thread, cred, fh, fhlen, fh_hash);

        if (CHIMERA_VFS_IS_ERR(request)) {
            callback(CHIMERA_VFS_PTR_ERR(request), NULL, private_data);
            return;
        }

        request->opcode             = CHIMERA_VFS_OP_OPEN_FH;
        request->open_fh.flags      = flags;
        request->proto_callback     = callback;
        request->proto_private_data = private_data;

        chimera_vfs_open_cache_acquire(
            thread,
            cache,
            module,
            request,
            fh,
            fhlen,
            fh_hash,
            CHIMERA_VFS_OPEN_CACHE_NO_BACKEND_OPEN,
            request->open_fh.flags,
            0,
            chimera_vfs_open_fh_hdl_callback);

        return;
    }
} /* chimera_vfs_open_fh_hs */

SYMBOL_EXPORT void
chimera_vfs_open_fh(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_transaction *txn,
    const void                     *fh,
    int                             fhlen,
    unsigned int                    flags,
    chimera_vfs_open_fh_callback_t  callback,
    void                           *private_data)
{
    chimera_vfs_open_fh_hs(thread, cred, txn, fh, fhlen, flags, NULL, callback, private_data);
} /* chimera_vfs_open_fh */
