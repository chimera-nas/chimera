// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <stdlib.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_attr_cache.h"
#include "vfs_access.h"
#include "vfs_acl.h"
#include "common/misc.h"
#include "common/macros.h"

/*
 * Map the attributes a SETATTR is changing to the canonical ACE rights the
 * caller must hold.  A chmod (or an explicit ACL set) rewrites the special-who
 * ACEs, so it is treated as an ACL write; chown/chgrp require WRITE_OWNER; a
 * size change is a data write; a metadata-time set requires WRITE_ATTRIBUTES,
 * except utimes-to-now ("touch"), which a data writer is allowed to do.
 *
 * `cur` is the object's current attributes (NULL if not yet fetched).  When
 * present, an owner/group field set to the value it already holds is not a
 * change and does not require WRITE_OWNER -- which matters because a SET_INFO
 * security descriptor commonly restates the existing owner alongside a DACL,
 * and the owner holds WRITE_ACL but not WRITE_OWNER implicitly.
 */
static uint32_t
chimera_vfs_setattr_required(
    const struct chimera_vfs_attrs *set_attr,
    const struct chimera_vfs_attrs *cur)
{
    uint64_t m        = set_attr->va_set_mask;
    uint32_t required = 0;
    int      chowns;

    if (m & (CHIMERA_VFS_ATTR_ACL | CHIMERA_VFS_ATTR_MODE)) {
        required |= CHIMERA_ACE_WRITE_ACL;
    }

    chowns = ((m & CHIMERA_VFS_ATTR_UID) &&
              !(cur && (cur->va_set_mask & CHIMERA_VFS_ATTR_UID) &&
                cur->va_uid == set_attr->va_uid)) ||
        ((m & CHIMERA_VFS_ATTR_GID) &&
         !(cur && (cur->va_set_mask & CHIMERA_VFS_ATTR_GID) &&
           cur->va_gid == set_attr->va_gid));

    if (chowns) {
        required |= CHIMERA_ACE_WRITE_OWNER;
    }

    if (m & CHIMERA_VFS_ATTR_SIZE) {
        required |= CHIMERA_ACE_WRITE_DATA;
    }

    if (m & CHIMERA_VFS_ATTR_DOS_ATTRIBUTES) {
        required |= CHIMERA_ACE_WRITE_ATTRIBUTES;
    }

    if (m & CHIMERA_VFS_ATTR_ATIME) {
        required |= (set_attr->va_atime.tv_nsec == CHIMERA_VFS_TIME_NOW) ?
            CHIMERA_ACE_WRITE_DATA : CHIMERA_ACE_WRITE_ATTRIBUTES;
    }

    if (m & CHIMERA_VFS_ATTR_MTIME) {
        required |= (set_attr->va_mtime.tv_nsec == CHIMERA_VFS_TIME_NOW) ?
            CHIMERA_ACE_WRITE_DATA : CHIMERA_ACE_WRITE_ATTRIBUTES;
    }

    return required;
} /* chimera_vfs_setattr_required */

static void
chimera_vfs_setattr_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread     *thread   = request->thread;
    chimera_vfs_setattr_callback_t callback = request->proto_callback;

    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_attr_cache_insert(thread->vfs->vfs_attr_cache,
                                      request->fh_hash,
                                      request->fh,
                                      request->fh_len,
                                      &request->setattr.r_post_attr);
    }

    chimera_vfs_complete(request);

    callback(request->status,
             &request->setattr.r_pre_attr,
             request->setattr.set_attr,
             &request->setattr.r_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(thread, request);
} /* chimera_vfs_setattr_complete */

static void
chimera_vfs_setattr_dispatch(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *set_attr,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_setattr_callback_t  callback,
    void                           *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, NULL, private_data);
        return;
    }

    request->opcode                          = CHIMERA_VFS_OP_SETATTR;
    request->complete                        = chimera_vfs_setattr_complete;
    request->setattr.handle                  = handle;
    request->setattr.set_attr                = set_attr;
    request->setattr.r_pre_attr.va_req_mask  = pre_attr_mask;
    request->setattr.r_pre_attr.va_set_mask  = 0;
    request->setattr.r_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->setattr.r_post_attr.va_set_mask = 0;
    request->proto_callback                  = callback;
    request->proto_private_data              = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_setattr_dispatch */

/*
 * Continuation context for the enforcement pre-step: a getattr+ACL is issued
 * before the mutation so the gate can authorize it, then the real SETATTR is
 * dispatched (or the caller's callback is completed with EACCES).
 */
struct chimera_vfs_setattr_gate {
    struct chimera_vfs_thread      *thread;
    const struct chimera_vfs_cred  *cred;
    struct chimera_vfs_open_handle *handle;
    struct chimera_vfs_attrs       *set_attr;
    uint64_t                        pre_attr_mask;
    uint64_t                        post_attr_mask;
    chimera_vfs_setattr_callback_t  callback;
    void                           *private_data;
};

static void
chimera_vfs_setattr_gate_complete(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_setattr_gate *gate = private_data;
    uint32_t                         required;

    if (error_code != CHIMERA_VFS_OK) {
        gate->callback(error_code, NULL, NULL, NULL, gate->private_data);
        free(gate);
        return;
    }

    /* Recompute against the now-known current attributes so a no-op
     * owner/group restate does not demand WRITE_OWNER. */
    required = chimera_vfs_setattr_required(gate->set_attr, attr);

    if (chimera_vfs_gate(attr, gate->cred, required) != CHIMERA_VFS_OK) {
        gate->callback(CHIMERA_VFS_EACCES, NULL, NULL, NULL, gate->private_data);
        free(gate);
        return;
    }

    chimera_vfs_setattr_dispatch(gate->thread, gate->cred, gate->handle,
                                 gate->set_attr, gate->pre_attr_mask,
                                 gate->post_attr_mask, gate->callback,
                                 gate->private_data);
    free(gate);
} /* chimera_vfs_setattr_gate_complete */

SYMBOL_EXPORT void
chimera_vfs_setattr(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *set_attr,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask,
    chimera_vfs_setattr_callback_t  callback,
    void                           *private_data)
{
    struct chimera_vfs_setattr_gate *gate;
    uint32_t                         required;

    /* Engine-authoritative backend and a non-exempt caller: authorize the
     * mutation against a fresh attr+ACL fetch before applying it.  (For
     * delegated/AUTH_NONE/root cases the kernel or engine grants anyway, so we
     * dispatch directly and avoid the extra round-trip.) */
    if (chimera_vfs_gate_needed(handle->vfs_module->capabilities, cred)) {
        /* Preliminary (current attrs unknown): worst-case required set, just to
         * decide whether the pre-step is needed.  The real check in the gate
         * callback recomputes once the current owner/group is known. */
        required = chimera_vfs_setattr_required(set_attr, NULL);

        if (required) {
            gate = malloc(sizeof(*gate));

            gate->thread         = thread;
            gate->cred           = cred;
            gate->handle         = handle;
            gate->set_attr       = set_attr;
            gate->pre_attr_mask  = pre_attr_mask;
            gate->post_attr_mask = post_attr_mask;
            gate->callback       = callback;
            gate->private_data   = private_data;

            chimera_vfs_getattr(thread, cred, handle,
                                CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL,
                                chimera_vfs_setattr_gate_complete, gate);
            return;
        }
    }

    chimera_vfs_setattr_dispatch(thread, cred, handle, set_attr,
                                 pre_attr_mask, post_attr_mask,
                                 callback, private_data);
} /* chimera_vfs_setattr */