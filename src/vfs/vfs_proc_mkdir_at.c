// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <stdlib.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_name_cache.h"
#include "vfs_attr_cache.h"
#include "vfs_notify.h"
#include "vfs_access.h"
#include "vfs_acl.h"
#include "common/misc.h"
#include "common/macros.h"
static void
chimera_vfs_mkdir_at_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread      *thread   = request->thread;
    struct chimera_vfs_name_cache  *cache    = thread->vfs->vfs_name_cache;
    chimera_vfs_mkdir_at_callback_t callback = request->proto_callback;

    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_notify_emit(thread->vfs->vfs_notify,
                                request->mkdir_at.handle->fh,
                                request->mkdir_at.handle->fh_len,
                                CHIMERA_VFS_NOTIFY_DIR_ADDED,
                                request->mkdir_at.name,
                                request->mkdir_at.name_len,
                                NULL, 0);

        chimera_vfs_attr_cache_insert(thread, thread->vfs->vfs_attr_cache,
                                      request->mkdir_at.handle->fh_hash,
                                      request->mkdir_at.handle->fh,
                                      request->mkdir_at.handle->fh_len,
                                      &request->mkdir_at.r_dir_post_attr);

        /* A path-only backend returns no child fh; skip the child name/attr
         * cache (va_fh_len==0 is the name cache's negative marker). */
        if (request->mkdir_at.r_attr.va_fh_len > 0) {
            chimera_vfs_name_cache_insert(thread, cache,
                                          request->mkdir_at.handle->fh_hash,
                                          request->mkdir_at.handle->fh,
                                          request->mkdir_at.handle->fh_len,
                                          request->mkdir_at.name_hash,
                                          request->mkdir_at.name,
                                          request->mkdir_at.name_len,
                                          request->mkdir_at.r_attr.va_fh,
                                          request->mkdir_at.r_attr.va_fh_len);

            chimera_vfs_attr_cache_insert(thread, thread->vfs->vfs_attr_cache,
                                          chimera_vfs_hash(request->mkdir_at.r_attr.va_fh,
                                                           request->mkdir_at.r_attr.va_fh_len),
                                          request->mkdir_at.r_attr.va_fh,
                                          request->mkdir_at.r_attr.va_fh_len,
                                          &request->mkdir_at.r_attr);
        }
    }

    chimera_vfs_complete(request);

    callback(request->status,
             request->mkdir_at.set_attr,
             &request->mkdir_at.r_attr,
             &request->mkdir_at.r_dir_pre_attr,
             &request->mkdir_at.r_dir_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_mkdir_at_complete */

static void
chimera_vfs_mkdir_at_dispatch(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    struct chimera_vfs_attrs       *attr,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_mkdir_at_callback_t callback,
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

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, NULL, NULL, private_data);
        return;
    }

    request->opcode                               = CHIMERA_VFS_OP_MKDIR_AT;
    request->complete                             = chimera_vfs_mkdir_at_complete;
    request->mkdir_at.handle                      = handle;
    request->mkdir_at.name                        = name;
    request->mkdir_at.name_len                    = namelen;
    request->mkdir_at.name_hash                   = name_hash;
    request->mkdir_at.set_attr                    = attr;
    request->mkdir_at.r_attr.va_req_mask          = attr_mask | CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->mkdir_at.r_attr.va_set_mask          = 0;
    request->mkdir_at.r_dir_pre_attr.va_req_mask  = pre_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->mkdir_at.r_dir_pre_attr.va_set_mask  = 0;
    request->mkdir_at.r_dir_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->mkdir_at.r_dir_post_attr.va_set_mask = 0;
    request->proto_callback                       = callback;
    request->proto_private_data                   = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_mkdir_at_dispatch */

/*
 * Enforcement pre-step context: creating a directory requires ADD_SUBDIRECTORY
 * on the parent.  On engine-authoritative backends the gate fetches the
 * parent's attrs+ACL and authorizes before the real mkdir is dispatched.
 */
struct chimera_vfs_mkdir_at_gate {
    struct chimera_vfs_thread      *thread;
    const struct chimera_vfs_cred  *cred;
    struct chimera_vfs_open_handle *handle;
    const char                     *name;
    int                             namelen;
    struct chimera_vfs_attrs       *attr;
    uint64_t                        attr_mask;
    uint64_t                        pre_attr_mask;
    uint64_t                        post_attr_mask;
    chimera_vfs_mkdir_at_callback_t callback;
    void                           *private_data;
};

static void
chimera_vfs_mkdir_at_gate_complete(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct chimera_vfs_mkdir_at_gate *gate = private_data;

    if (status != CHIMERA_VFS_OK) {
        gate->callback(status, NULL, NULL, NULL, NULL, gate->private_data);
        free(gate);
        return;
    }

    chimera_vfs_mkdir_at_dispatch(gate->thread, gate->cred, gate->handle,
                                  gate->name, gate->namelen, gate->attr,
                                  gate->attr_mask, gate->pre_attr_mask,
                                  gate->post_attr_mask, gate->callback,
                                  gate->private_data);
    free(gate);
} /* chimera_vfs_mkdir_at_gate_complete */

SYMBOL_EXPORT void
chimera_vfs_mkdir_at(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    const char                     *name,
    int                             namelen,
    struct chimera_vfs_attrs       *attr,
    uint64_t                        attr_mask,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_mkdir_at_callback_t callback,
    void                           *private_data)
{
    struct chimera_vfs_mkdir_at_gate *gate;

    if (namelen >= CHIMERA_VFS_NAME_MAX) {
        callback(CHIMERA_VFS_ENAMETOOLONG, NULL, NULL, NULL, NULL, private_data);
        return;
    }

    if (chimera_vfs_gate_needed(handle->vfs_module->capabilities, cred)) {
        gate                 = malloc(sizeof(*gate));
        gate->thread         = thread;
        gate->cred           = cred;
        gate->handle         = handle;
        gate->name           = name;
        gate->namelen        = namelen;
        gate->attr           = attr;
        gate->attr_mask      = attr_mask;
        gate->pre_attr_mask  = pre_attr_mask;
        gate->post_attr_mask = post_attr_mask;
        gate->callback       = callback;
        gate->private_data   = private_data;

        /* Creating an entry in a directory requires both the right to add a
         * subdirectory (APPEND_DATA) and search permission (EXECUTE) on it. */
        chimera_vfs_gate_fh(thread, cred, handle->fh, handle->fh_len,
                            CHIMERA_ACE_APPEND_DATA | CHIMERA_ACE_EXECUTE,
                            chimera_vfs_mkdir_at_gate_complete, gate);
        return;
    }

    chimera_vfs_mkdir_at_dispatch(thread, cred, handle, name, namelen, attr,
                                  attr_mask, pre_attr_mask, post_attr_mask,
                                  callback, private_data);
} /* chimera_vfs_mkdir_at */
