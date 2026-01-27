// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs/vfs_internal.h"
#include "vfs/vfs_name_cache.h"
#include "vfs/vfs_attr_cache.h"
#include "common/macros.h"

static void
chimera_vfs_remove_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread     *thread     = request->thread;
    struct chimera_vfs_attr_cache *attr_cache = thread->vfs->vfs_attr_cache;
    struct chimera_vfs_name_cache *name_cache = thread->vfs->vfs_name_cache;
    chimera_vfs_remove_callback_t  callback   = request->proto_callback;

    if (request->status == CHIMERA_VFS_OK) {

        chimera_vfs_name_cache_insert(name_cache,
                                      request->remove.handle->fh_hash,
                                      request->remove.handle->fh,
                                      request->remove.handle->fh_len,
                                      request->remove.name_hash,
                                      request->remove.name,
                                      request->remove.namelen,
                                      NULL,
                                      0);

        chimera_vfs_attr_cache_insert(attr_cache,
                                      request->remove.handle->fh_hash,
                                      request->remove.handle->fh,
                                      request->remove.handle->fh_len,
                                      &request->remove.r_dir_post_attr);

        if (request->remove.r_removed_attr.va_set_mask & CHIMERA_VFS_ATTR_FH) {
            chimera_vfs_attr_cache_insert(attr_cache,
                                          chimera_vfs_hash(request->remove.r_removed_attr.va_fh, request->remove.
                                                           r_removed_attr.va_fh_len),
                                          request->remove.r_removed_attr.va_fh,
                                          request->remove.r_removed_attr.va_fh_len,
                                          &request->remove.r_removed_attr);
        }
    }

    chimera_vfs_complete(request);

    callback(request->status,
             &request->remove.r_dir_pre_attr,
             &request->remove.r_dir_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_remove_complete */

SYMBOL_EXPORT void
chimera_vfs_remove(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    const uint8_t                  *child_fh,
    int                             child_fh_len,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_remove_callback_t   callback,
    void                           *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    request->opcode                             = CHIMERA_VFS_OP_REMOVE;
    request->complete                           = chimera_vfs_remove_complete;
    request->remove.handle                      = handle;
    request->remove.name                        = name;
    request->remove.namelen                     = namelen;
    request->remove.name_hash                   = chimera_vfs_hash(name, namelen);
    request->remove.child_fh                    = child_fh;
    request->remove.child_fh_len                = child_fh_len;
    request->remove.r_dir_pre_attr.va_req_mask  = pre_attr_mask;
    request->remove.r_dir_pre_attr.va_set_mask  = 0;
    request->remove.r_dir_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->remove.r_dir_post_attr.va_set_mask = 0;
    request->remove.r_removed_attr.va_req_mask  = CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->remove.r_removed_attr.va_set_mask  = 0;
    request->proto_callback                     = callback;
    request->proto_private_data                 = private_data;

    chimera_vfs_dispatch(request);

} /* chimera_vfs_remove */
