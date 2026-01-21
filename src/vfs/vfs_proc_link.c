// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
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
chimera_vfs_link_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread     *thread     = request->thread;
    struct chimera_vfs_name_cache *name_cache = thread->vfs->vfs_name_cache;
    struct chimera_vfs_attr_cache *attr_cache = thread->vfs->vfs_attr_cache;
    chimera_vfs_link_callback_t    callback   = request->proto_callback;


    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_name_cache_insert(name_cache,
                                      request->link.dir_fh_hash,
                                      request->link.dir_fh,
                                      request->link.dir_fhlen,
                                      request->link.name_hash,
                                      request->link.name,
                                      request->link.namelen,
                                      request->fh,
                                      request->fh_len);

        chimera_vfs_attr_cache_insert(attr_cache,
                                      request->link.dir_fh_hash,
                                      request->link.dir_fh,
                                      request->link.dir_fhlen,
                                      &request->link.r_dir_post_attr);

        chimera_vfs_attr_cache_insert(attr_cache,
                                      request->fh_hash,
                                      request->fh,
                                      request->fh_len,
                                      &request->link.r_attr);

        if (request->link.r_replaced_attr.va_set_mask & CHIMERA_VFS_ATTR_FH) {
            chimera_vfs_attr_cache_insert(attr_cache,
                                          request->link.r_replaced_attr.va_fh_hash,
                                          request->link.r_replaced_attr.va_fh,
                                          request->link.r_replaced_attr.va_fh_len,
                                          &request->link.r_replaced_attr);
        }
    }

    chimera_vfs_complete(request);

    callback(request->status, request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_link_complete */

SYMBOL_EXPORT void
chimera_vfs_link(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fhlen,
    const void                    *dir_fh,
    int                            dir_fhlen,
    const char                    *name,
    int                            namelen,
    unsigned int                   replace,
    uint64_t                       attr_mask,
    uint64_t                       pre_attr_mask,
    uint64_t                       post_attr_mask,
    chimera_vfs_link_callback_t    callback,
    void                          *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc(thread, cred, fh, fhlen);

    request->opcode                           = CHIMERA_VFS_OP_LINK;
    request->complete                         = chimera_vfs_link_complete;
    request->link.dir_fh_hash                 = chimera_vfs_hash(dir_fh, dir_fhlen);
    request->link.dir_fh                      = dir_fh;
    request->link.dir_fhlen                   = dir_fhlen;
    request->link.name                        = name;
    request->link.namelen                     = namelen;
    request->link.name_hash                   = chimera_vfs_hash(name, namelen);
    request->link.replace                     = replace;
    request->link.r_attr.va_req_mask          = attr_mask | CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->link.r_attr.va_set_mask          = 0;
    request->link.r_replaced_attr.va_req_mask = CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->link.r_replaced_attr.va_set_mask = 0;
    request->link.r_dir_pre_attr.va_req_mask  = pre_attr_mask;
    request->link.r_dir_pre_attr.va_set_mask  = 0;
    request->link.r_dir_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->link.r_dir_post_attr.va_set_mask = 0;
    request->proto_callback                   = callback;
    request->proto_private_data               = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_link */
