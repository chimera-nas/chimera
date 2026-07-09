// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "common/macros.h"
#include <string.h>
#include <stdlib.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_name_cache.h"
#include "vfs_attr_cache.h"
#include "vfs_access.h"
#include "vfs_acl.h"
#include "common/misc.h"

static void
chimera_vfs_symlink_at_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread        *thread     = request->thread;
    struct chimera_vfs_name_cache    *name_cache = thread->vfs->vfs_name_cache;
    struct chimera_vfs_attr_cache    *attr_cache = thread->vfs->vfs_attr_cache;
    chimera_vfs_symlink_at_callback_t callback   = request->proto_callback;


    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_name_cache_insert(thread, name_cache,
                                      request->fh_hash,
                                      request->fh,
                                      request->fh_len,
                                      request->symlink_at.name_hash,
                                      request->symlink_at.name,
                                      request->symlink_at.namelen,
                                      request->symlink_at.r_attr.va_fh,
                                      request->symlink_at.r_attr.va_fh_len);

        chimera_vfs_attr_cache_insert(thread, attr_cache,
                                      request->fh_hash,
                                      request->fh,
                                      request->fh_len,
                                      &request->symlink_at.r_dir_post_attr);

        chimera_vfs_attr_cache_insert(thread, attr_cache,
                                      chimera_vfs_hash(request->symlink_at.r_attr.va_fh, request->symlink_at.r_attr.
                                                       va_fh_len),
                                      request->symlink_at.r_attr.va_fh,
                                      request->symlink_at.r_attr.va_fh_len,
                                      &request->symlink_at.r_attr);
    }


    chimera_vfs_complete(request);

    callback(request->status,
             &request->symlink_at.r_attr,
             &request->symlink_at.r_dir_pre_attr,
             &request->symlink_at.r_dir_post_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_symlink_at_complete */

static void
chimera_vfs_symlink_at_dispatch(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_transaction   *txn,
    struct chimera_vfs_open_handle   *handle,
    const char                       *name,
    int                               namelen,
    const char                       *target,
    int                               targetlen,
    struct chimera_vfs_attrs         *set_attr,
    uint64_t                          attr_mask,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    chimera_vfs_symlink_at_callback_t callback,
    void                             *private_data)
{
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_by_handle(thread, cred, handle);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, NULL, NULL, private_data);
        return;
    }

    request->transaction = txn;

    request->opcode                                 = CHIMERA_VFS_OP_SYMLINK_AT;
    request->complete                               = chimera_vfs_symlink_at_complete;
    request->symlink_at.handle                      = handle;
    request->symlink_at.name                        = name;
    request->symlink_at.namelen                     = namelen;
    request->symlink_at.name_hash                   = chimera_vfs_hash(name, namelen);
    request->symlink_at.target                      = target;
    request->symlink_at.targetlen                   = targetlen;
    request->symlink_at.set_attr                    = set_attr;
    request->symlink_at.r_attr.va_req_mask          = attr_mask | CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->symlink_at.r_attr.va_set_mask          = 0;
    request->symlink_at.r_dir_pre_attr.va_req_mask  = pre_attr_mask;
    request->symlink_at.r_dir_pre_attr.va_set_mask  = 0;
    request->symlink_at.r_dir_post_attr.va_req_mask = post_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->symlink_at.r_dir_post_attr.va_set_mask = 0;
    request->proto_callback                         = callback;
    request->proto_private_data                     = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_symlink_at_dispatch */

/*
 * Enforcement pre-step context: creating a symlink requires ADD_FILE on the
 * parent directory.
 */
struct chimera_vfs_symlink_at_gate {
    struct chimera_vfs_thread        *thread;
    const struct chimera_vfs_cred    *cred;
    struct chimera_vfs_transaction   *txn;
    struct chimera_vfs_open_handle   *handle;
    const char                       *name;
    int                               namelen;
    const char                       *target;
    int                               targetlen;
    struct chimera_vfs_attrs         *set_attr;
    uint64_t                          attr_mask;
    uint64_t                          pre_attr_mask;
    uint64_t                          post_attr_mask;
    chimera_vfs_symlink_at_callback_t callback;
    void                             *private_data;
};

static void
chimera_vfs_symlink_at_gate_complete(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct chimera_vfs_symlink_at_gate *gate = private_data;

    if (status != CHIMERA_VFS_OK) {
        gate->callback(status, NULL, NULL, NULL, gate->private_data);
        free(gate);
        return;
    }

    chimera_vfs_symlink_at_dispatch(gate->thread, gate->cred, gate->txn, gate->handle,
                                    gate->name, gate->namelen, gate->target,
                                    gate->targetlen, gate->set_attr,
                                    gate->attr_mask, gate->pre_attr_mask,
                                    gate->post_attr_mask, gate->callback,
                                    gate->private_data);
    free(gate);
} /* chimera_vfs_symlink_at_gate_complete */

SYMBOL_EXPORT void
chimera_vfs_symlink_at(
    struct chimera_vfs_thread        *thread,
    const struct chimera_vfs_cred    *cred,
    struct chimera_vfs_transaction   *txn,
    struct chimera_vfs_open_handle   *handle,
    const char                       *name,
    int                               namelen,
    const char                       *target,
    int                               targetlen,
    struct chimera_vfs_attrs         *set_attr,
    uint64_t                          attr_mask,
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    chimera_vfs_symlink_at_callback_t callback,
    void                             *private_data)
{
    struct chimera_vfs_symlink_at_gate *gate;

    if (namelen >= CHIMERA_VFS_NAME_MAX) {
        callback(CHIMERA_VFS_ENAMETOOLONG, NULL, NULL, NULL, private_data);
        return;
    }

    if (chimera_vfs_gate_needed(handle->vfs_module->capabilities, cred)) {
        gate                 = malloc(sizeof(*gate));
        gate->thread         = thread;
        gate->cred           = cred;
        gate->txn            = txn;
        gate->handle         = handle;
        gate->name           = name;
        gate->namelen        = namelen;
        gate->target         = target;
        gate->targetlen      = targetlen;
        gate->set_attr       = set_attr;
        gate->attr_mask      = attr_mask;
        gate->pre_attr_mask  = pre_attr_mask;
        gate->post_attr_mask = post_attr_mask;
        gate->callback       = callback;
        gate->private_data   = private_data;

        chimera_vfs_gate_fh(thread, cred, handle->fh, handle->fh_len,
                            CHIMERA_ACE_WRITE_DATA | CHIMERA_ACE_EXECUTE,
                            chimera_vfs_symlink_at_gate_complete, gate);
        return;
    }

    chimera_vfs_symlink_at_dispatch(thread, cred, txn, handle, name, namelen, target,
                                    targetlen, set_attr, attr_mask,
                                    pre_attr_mask, post_attr_mask, callback,
                                    private_data);
} /* chimera_vfs_symlink_at */
