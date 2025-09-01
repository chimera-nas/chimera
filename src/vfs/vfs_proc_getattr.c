// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_attr_cache.h"
#include "common/misc.h"
#include "common/macros.h"

static void
chimera_vfs_getattr_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread     *thread     = request->thread;
    struct chimera_vfs_attr_cache *attr_cache = thread->vfs->vfs_attr_cache;
    chimera_vfs_getattr_callback_t callback   = request->proto_callback;

    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_attr_cache_insert(attr_cache,
                                      request->getattr.handle->fh_hash,
                                      request->getattr.handle->fh,
                                      request->getattr.handle->fh_len,
                                      &request->getattr.r_attr);
    }

    chimera_vfs_complete(request);

    callback(request->status,
             &request->getattr.r_attr,
             request->proto_private_data);

    chimera_vfs_request_free(thread, request);
} /* chimera_vfs_getattr_complete */

SYMBOL_EXPORT void
chimera_vfs_getattr(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        req_attr_mask,
    chimera_vfs_getattr_callback_t  callback,
    void                           *private_data)
{
    struct chimera_vfs_attr_cache *attr_cache = thread->vfs->vfs_attr_cache;
    struct chimera_vfs_request    *request;
    struct chimera_vfs_attrs       cached_attr;
    int                            rc;

    if (!(req_attr_mask & ~(CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_CACHEABLE))) {
        rc = chimera_vfs_attr_cache_lookup(attr_cache,
                                           handle->fh_hash,
                                           handle->fh,
                                           handle->fh_len,
                                           &cached_attr);

        if (rc == 0) {
            callback(CHIMERA_VFS_OK,
                     &cached_attr,
                     private_data);
            return;
        }
    } /* chimera_vfs_getattr */

    request = chimera_vfs_request_alloc_by_handle(thread, handle);

    request->opcode                     = CHIMERA_VFS_OP_GETATTR;
    request->complete                   = chimera_vfs_getattr_complete;
    request->getattr.handle             = handle;
    request->getattr.r_attr.va_req_mask = req_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->getattr.r_attr.va_set_mask = 0;
    request->proto_callback             = callback;
    request->proto_private_data         = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_getattr */
