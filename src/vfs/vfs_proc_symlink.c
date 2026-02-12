// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/macros.h"
#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_name_cache.h"
#include "vfs_attr_cache.h"
#include "common/misc.h"

static void
chimera_vfs_symlink_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread     *thread     = request->thread;
    struct chimera_vfs_name_cache *name_cache = thread->vfs->vfs_name_cache;
    struct chimera_vfs_attr_cache *attr_cache = thread->vfs->vfs_attr_cache;
    chimera_vfs_symlink_callback_t callback   = request->proto_callback;


    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_name_cache_insert(name_cache,
                                      request->fh_hash,
                                      request->fh,
                                      request->fh_len,
                                      request->symlink.name_hash,
                                      request->symlink.name,
                                      request->symlink.namelen,
                                      request->symlink.r_attr.va_fh,
                                      request->symlink.r_attr.va_fh_len);

        chimera_vfs_attr_cache_insert(attr_cache,
                                      request->fh_hash,
                                      request->fh,
                                      request->fh_len,
                                      &request->symlink.r_dir_post_attr);

        chimera_vfs_attr_cache_insert(attr_cache,
                                      chimera_vfs_hash(request->symlink.r_attr.va_fh, request->symlink.r_attr.
                                                       va_fh_len),
                                      request->symlink.r_attr.va_fh,
                                      request->symlink.r_attr.va_fh_len,
                                      &request->symlink.r_attr);
    }


    chimera_vfs_complete(request);

    callback(request->status,
             &request->symlink.r_attr,
             &request->symlink.r_dir_pre_attr,
             &request->symlink.r_dir_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_symlink_complete */

SYMBOL_EXPORT void
chimera_vfs_symlink(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    const char                     *target,
    int                             targetlen,
    struct chimera_vfs_attrs       *set_attr,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_symlink_callback_t  callback,
    void                           *private_data)
{
    struct chimera_vfs_request *request;

    /* Validate symlink name and target lengths.
     * NAME_MAX (255) for names, PATH_MAX (4096) for symlink targets. */
    if (namelen > 255 || targetlen > 4096) {
        callback(CHIMERA_VFS_ENAMETOOLONG, NULL, NULL, NULL, private_data);
        return;
    }

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, NULL, private_data);
        return;
    }

    request->opcode                              = CHIMERA_VFS_OP_SYMLINK;
    request->complete                            = chimera_vfs_symlink_complete;
    request->symlink.handle                      = handle;
    request->symlink.name                        = name;
    request->symlink.namelen                     = namelen;
    request->symlink.name_hash                   = chimera_vfs_hash(name, namelen);
    request->symlink.target                      = target;
    request->symlink.targetlen                   = targetlen;
    request->symlink.set_attr                    = set_attr;
    request->symlink.r_attr.va_req_mask          = attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->symlink.r_attr.va_set_mask          = 0;
    request->symlink.r_dir_pre_attr.va_req_mask  = pre_attr_mask;
    request->symlink.r_dir_pre_attr.va_set_mask  = 0;
    request->symlink.r_dir_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->symlink.r_dir_post_attr.va_set_mask = 0;
    request->proto_callback                      = callback;
    request->proto_private_data                  = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_symlink */
