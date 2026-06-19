// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <stdlib.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_name_cache.h"
#include "vfs_attr_cache.h"
#include "vfs_access.h"
#include "vfs_acl.h"
#include "common/format.h"
#include "common/misc.h"
#include "common/macros.h"

static void
chimera_vfs_lookup_at_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread       *thread     = request->thread;
    struct chimera_vfs_name_cache   *name_cache = thread->vfs->vfs_name_cache;
    struct chimera_vfs_attr_cache   *attr_cache = thread->vfs->vfs_attr_cache;
    chimera_vfs_lookup_at_callback_t callback   = request->proto_callback;

    if (request->status == CHIMERA_VFS_OK) {
        /* A path-only backend resolves the whole path in one shot and returns no
         * stable child fh (va_fh_len == 0).  Skip the child name/attr cache: a
         * zero-length fh is the name cache's NEGATIVE (ENOENT) marker, so caching
         * a success-without-fh here would poison subsequent lookups. */
        if (request->lookup_at.r_attr.va_fh_len > 0) {
            chimera_vfs_name_cache_insert(thread, name_cache,
                                          request->lookup_at.handle->fh_hash,
                                          request->lookup_at.handle->fh,
                                          request->lookup_at.handle->fh_len,
                                          request->lookup_at.component_hash,
                                          request->lookup_at.component,
                                          request->lookup_at.component_len,
                                          request->lookup_at.r_attr.va_fh,
                                          request->lookup_at.r_attr.va_fh_len);

            chimera_vfs_attr_cache_refresh(thread, attr_cache,
                                           chimera_vfs_hash(request->lookup_at.r_attr.va_fh, request->lookup_at.r_attr.
                                                            va_fh_len),
                                           request->lookup_at.r_attr.va_fh,
                                           request->lookup_at.r_attr.va_fh_len,
                                           &request->lookup_at.r_attr);
        }

        chimera_vfs_attr_cache_refresh(thread, attr_cache,
                                       request->lookup_at.handle->fh_hash,
                                       request->lookup_at.handle->fh,
                                       request->lookup_at.handle->fh_len,
                                       &request->lookup_at.r_dir_attr);
    }

    chimera_vfs_complete(request);

    callback(request->status,
             &request->lookup_at.r_attr,
             &request->lookup_at.r_dir_attr,
             request->proto_private_data);

    chimera_vfs_request_free(thread, request);

} /* chimera_vfs_lookup_at_complete */

static void
chimera_vfs_lookup_at_dispatch(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    uint32_t                         namelen,
    uint64_t                         attr_mask,
    uint64_t                         dir_attr_mask,
    chimera_vfs_lookup_at_callback_t callback,
    void                            *private_data)
{
    struct chimera_vfs_name_cache *name_cache = thread->vfs->vfs_name_cache;
    struct chimera_vfs_attr_cache *attr_cache = thread->vfs->vfs_attr_cache;
    struct chimera_vfs_request    *request;
    uint64_t                       name_hash;
    int                            rc;
    struct chimera_vfs_attrs       cached_attr, cached_dir_attr;

    name_hash = chimera_vfs_hash(name, namelen);

    if (!(attr_mask & ~(CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_CACHEABLE)) &&
        !(dir_attr_mask & ~(CHIMERA_VFS_ATTR_MASK_CACHEABLE))) {

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

        if (rc == 0) {

            if (cached_attr.va_fh_len == 0) {

                callback(CHIMERA_VFS_ENOENT,
                         &cached_attr,
                         &cached_dir_attr,
                         private_data);
                return;
            }

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
                    callback(CHIMERA_VFS_OK,
                             &cached_attr,
                             &cached_dir_attr,
                             private_data);
                    return;
                }
            }
        }
    }

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, private_data);
        return;
    }

    request->opcode                           = CHIMERA_VFS_OP_LOOKUP_AT;
    request->complete                         = chimera_vfs_lookup_at_complete;
    request->lookup_at.handle                 = handle;
    request->lookup_at.component              = name;
    request->lookup_at.component_len          = namelen;
    request->lookup_at.component_hash         = name_hash;
    request->lookup_at.r_attr.va_req_mask     = attr_mask | CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->lookup_at.r_attr.va_set_mask     = 0;
    request->lookup_at.r_dir_attr.va_req_mask = dir_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->lookup_at.r_dir_attr.va_set_mask = 0;
    request->proto_callback                   = callback;
    request->proto_private_data               = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_lookup_at_dispatch */

/*
 * Enforcement pre-step context: resolving a name requires EXECUTE (directory
 * search) on the parent.  The gate runs before the lookup so the child
 * name-cache fast path in the dispatch is only reached once search is granted.
 */
struct chimera_vfs_lookup_at_gate {
    struct chimera_vfs_thread       *thread;
    const struct chimera_vfs_cred   *cred;
    struct chimera_vfs_open_handle  *handle;
    const char                      *name;
    uint32_t                         namelen;
    uint64_t                         attr_mask;
    uint64_t                         dir_attr_mask;
    chimera_vfs_lookup_at_callback_t callback;
    void                            *private_data;
};

static void
chimera_vfs_lookup_at_gate_complete(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct chimera_vfs_lookup_at_gate *gate = private_data;

    if (status != CHIMERA_VFS_OK) {
        gate->callback(status, NULL, NULL, gate->private_data);
        free(gate);
        return;
    }

    chimera_vfs_lookup_at_dispatch(gate->thread, gate->cred, gate->handle,
                                   gate->name, gate->namelen, gate->attr_mask,
                                   gate->dir_attr_mask, gate->callback,
                                   gate->private_data);
    free(gate);
} /* chimera_vfs_lookup_at_gate_complete */

SYMBOL_EXPORT void
chimera_vfs_lookup_at(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    struct chimera_vfs_open_handle  *handle,
    const char                      *name,
    uint32_t                         namelen,
    uint64_t                         attr_mask,
    uint64_t                         dir_attr_mask,
    chimera_vfs_lookup_at_callback_t callback,
    void                            *private_data)
{
    struct chimera_vfs_lookup_at_gate *gate;

    /* Path-prefix search (EXECUTE) is enforced by the engine even for
     * DELEGATES_DAC (passthrough) backends when the caller is a POSIX (AUTH_UNIX)
     * client: they resolve each component by file handle (open_by_handle_at),
     * which bypasses the kernel's directory-search DAC, so the prefix would
     * otherwise never be checked.  SMB keeps its own model (see gate_needed_dac). */
    if (chimera_vfs_gate_needed_dac(handle->vfs_module->capabilities, cred)) {
        gate                = malloc(sizeof(*gate));
        gate->thread        = thread;
        gate->cred          = cred;
        gate->handle        = handle;
        gate->name          = name;
        gate->namelen       = namelen;
        gate->attr_mask     = attr_mask;
        gate->dir_attr_mask = dir_attr_mask;
        gate->callback      = callback;
        gate->private_data  = private_data;

        chimera_vfs_gate_fh_dac(thread, cred, handle->fh, handle->fh_len,
                                CHIMERA_ACE_EXECUTE,
                                chimera_vfs_lookup_at_gate_complete, gate);
        return;
    }

    chimera_vfs_lookup_at_dispatch(thread, cred, handle, name, namelen,
                                   attr_mask, dir_attr_mask, callback,
                                   private_data);
} /* chimera_vfs_lookup_at */
