// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"
#include "vfs_open_cache.h"
#include "vfs_release.h"
#include "common/macros.h"
static void
chimera_vfs_open_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread      *thread   = request->thread;
    struct chimera_vfs_open_handle *handle   = request->pending_handle;
    chimera_vfs_open_callback_t     callback = request->proto_callback;

    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_populate_handle(thread, handle, request->open.r_vfs_private);
    } else {
        chimera_vfs_release_failed(thread, handle, request->status);
        handle = NULL;
    }

    chimera_vfs_complete(request);

    callback(request->status,
             handle,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);

} /* chimera_vfs_open_complete */

static void
chimera_vfs_open_hdl_callback(
    struct chimera_vfs_request     *request,
    struct chimera_vfs_open_handle *handle)
{
    chimera_vfs_open_callback_t callback = request->proto_callback;



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
} /* chimera_vfs_open_hdl_callback */

SYMBOL_EXPORT void
chimera_vfs_open(
    struct chimera_vfs_thread  *thread,
    const void                 *fh,
    int                         fhlen,
    unsigned int                flags,
    chimera_vfs_open_callback_t callback,
    void                       *private_data)
{
    struct chimera_vfs_module      *module;
    struct chimera_vfs_request     *request;
    struct chimera_vfs_open_handle *handle;
    struct vfs_open_cache          *cache;
    uint64_t                        fh_hash;

    if (flags & CHIMERA_VFS_OPEN_PATH) {
        cache = thread->vfs->vfs_open_path_cache;
    } else {
        cache = thread->vfs->vfs_open_file_cache;
    }

    fh_hash = chimera_vfs_hash(fh, fhlen);

    module = chimera_vfs_get_module(thread, fh, fhlen);

    if ((module->capabilities & CHIMERA_VFS_CAP_OPEN_FILE_REQUIRED) || !(flags & CHIMERA_VFS_OPEN_INFERRED)) {

        /* We really need to open the file */

        request = chimera_vfs_request_alloc_by_hash(thread, fh, fhlen, fh_hash);

        request->opcode             = CHIMERA_VFS_OP_OPEN;
        request->complete           = chimera_vfs_open_complete;
        request->open.flags         = flags;
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
            UINT64_MAX,
            0,
            chimera_vfs_open_hdl_callback);

    } else {

        /* This is an inferred open from the likes of NFS3
         * where caller does not need to hold a reference count
         * and our module does not need open handles, so
         * we can synthesize a handle and return it immediately */

        handle = chimera_vfs_synth_handle_alloc(thread);

        memcpy(handle->fh, fh, fhlen);
        handle->vfs_module  = module;
        handle->fh_len      = fhlen;
        handle->fh_hash     = fh_hash;
        handle->vfs_private = 0;

        callback(CHIMERA_VFS_OK, handle, private_data);
        return;
    }
} /* chimera_vfs_open */
