// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_name_cache.h"
#include "vfs_attr_cache.h"
#include "common/misc.h"
#include "common/macros.h"
static void
chimera_vfs_mkdir_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread     *thread   = request->thread;
    struct chimera_vfs_name_cache *cache    = thread->vfs->vfs_name_cache;
    chimera_vfs_mkdir_callback_t   callback = request->proto_callback;

    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_name_cache_insert(cache,
                                      request->mkdir.handle->fh_hash,
                                      request->mkdir.handle->fh,
                                      request->mkdir.handle->fh_len,
                                      request->mkdir.name_hash,
                                      request->mkdir.name,
                                      request->mkdir.name_len,
                                      request->mkdir.r_attr.va_fh,
                                      request->mkdir.r_attr.va_fh_len);

        chimera_vfs_attr_cache_insert(thread->vfs->vfs_attr_cache,
                                      request->mkdir.handle->fh_hash,
                                      request->mkdir.handle->fh,
                                      request->mkdir.handle->fh_len,
                                      &request->mkdir.r_dir_post_attr);

        chimera_vfs_attr_cache_insert(thread->vfs->vfs_attr_cache,
                                      chimera_vfs_hash(request->mkdir.r_attr.va_fh, request->mkdir.r_attr.va_fh_len)
                                      ,
                                      request->mkdir.r_attr.va_fh,
                                      request->mkdir.r_attr.va_fh_len,
                                      &request->mkdir.r_attr);
    }

    chimera_vfs_complete(request);

    callback(request->status,
             request->mkdir.set_attr,
             &request->mkdir.r_attr,
             &request->mkdir.r_dir_pre_attr,
             &request->mkdir.r_dir_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_mkdir_complete */

SYMBOL_EXPORT void
chimera_vfs_mkdir(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    struct chimera_vfs_attrs       *attr,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_mkdir_callback_t    callback,
    void                           *private_data)
{
    struct chimera_vfs_request    *request;
    uint64_t                       name_hash;
    struct chimera_vfs_name_cache *name_cache = thread->vfs->vfs_name_cache;
    struct chimera_vfs_attr_cache *attr_cache = thread->vfs->vfs_attr_cache;
    struct chimera_vfs_attrs       cached_attr;
    struct chimera_vfs_attrs       cached_dir_attr;
    int                            rc;

    name_hash = chimera_vfs_hash(name, namelen);

    if (!(attr_mask & ~(CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_CACHEABLE)) &&
        !(pre_attr_mask & ~(CHIMERA_VFS_ATTR_MASK_CACHEABLE)) &&
        !(post_attr_mask & ~(CHIMERA_VFS_ATTR_MASK_CACHEABLE))) {

        cached_attr.va_req_mask = 0;
        cached_attr.va_set_mask = 0;

        rc = chimera_vfs_name_cache_lookup(
            name_cache,
            handle->fh_hash,
            handle->fh,
            handle->fh_len,
            name_hash,
            name,
            namelen,
            cached_attr.va_fh,
            &cached_attr.va_fh_len);

        if (rc == 0 && cached_attr.va_fh_len > 0) {

            rc = chimera_vfs_attr_cache_lookup(
                attr_cache,
                handle->fh_hash,
                handle->fh,
                handle->fh_len,
                &cached_dir_attr);

            if (rc == 0) {

                rc = chimera_vfs_attr_cache_lookup(
                    attr_cache,
                    chimera_vfs_hash(cached_attr.va_fh, cached_attr.va_fh_len),
                    cached_attr.va_fh,
                    cached_attr.va_fh_len,
                    &cached_attr);

                if (rc == 0) {
                    callback(CHIMERA_VFS_EEXIST,
                             &cached_attr,
                             &cached_attr,
                             &cached_dir_attr,
                             &cached_dir_attr,
                             private_data);
                    return;
                }
            }
        }
    }

    request = chimera_vfs_request_alloc_by_handle(thread, handle);

    request->opcode                            = CHIMERA_VFS_OP_MKDIR;
    request->complete                          = chimera_vfs_mkdir_complete;
    request->mkdir.handle                      = handle;
    request->mkdir.name                        = name;
    request->mkdir.name_len                    = namelen;
    request->mkdir.name_hash                   = name_hash;
    request->mkdir.set_attr                    = attr;
    request->mkdir.set_attr->va_set_mask       = 0;
    request->mkdir.r_attr.va_req_mask          = attr_mask | CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->mkdir.r_attr.va_set_mask          = 0;
    request->mkdir.r_dir_pre_attr.va_req_mask  = pre_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->mkdir.r_dir_pre_attr.va_set_mask  = 0;
    request->mkdir.r_dir_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->mkdir.r_dir_post_attr.va_set_mask = 0;
    request->proto_callback                    = callback;
    request->proto_private_data                = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_mkdir */
