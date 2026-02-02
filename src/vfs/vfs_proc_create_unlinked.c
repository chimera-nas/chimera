// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/misc.h"
#include "vfs_open_cache.h"
#include "vfs_name_cache.h"
#include "vfs_attr_cache.h"
#include "common/macros.h"

static void
chimera_vfs_create_unlinked_hdl_callback(
    struct chimera_vfs_request     *request,
    struct chimera_vfs_open_handle *handle)
{
    struct chimera_vfs_thread             *thread   = request->thread;
    chimera_vfs_create_unlinked_callback_t callback = request->proto_callback;

    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_attr_cache_insert(thread->vfs->vfs_attr_cache,
                                      chimera_vfs_hash(request->create_unlinked.r_attr.va_fh, request->create_unlinked.
                                                       r_attr.
                                                       va_fh_len),
                                      request->create_unlinked.r_attr.va_fh,
                                      request->create_unlinked.r_attr.va_fh_len,
                                      &request->create_unlinked.r_attr);
    }

    chimera_vfs_complete(request);

    callback(request->status,
             handle,
             request->create_unlinked.set_attr,
             &request->create_unlinked.r_attr,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_open_at_hdl_callback */

static void
chimera_vfs_create_unlinked_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread *thread = request->thread;
    struct vfs_open_cache     *cache  = thread->vfs->vfs_open_file_cache;
    uint64_t                   fh_hash;

    fh_hash = chimera_vfs_hash(request->create_unlinked.r_attr.va_fh,
                               request->create_unlinked.r_attr.va_fh_len);

    chimera_vfs_open_cache_insert(
        thread,
        cache,
        request->module,
        request,
        request->create_unlinked.r_attr.va_fh,
        request->create_unlinked.r_attr.va_fh_len,
        fh_hash,
        request->create_unlinked.r_vfs_private,
        request->create_unlinked.flags,
        chimera_vfs_create_unlinked_hdl_callback);
} /* chimera_vfs_create_unlinked_complete */

SYMBOL_EXPORT void
chimera_vfs_create_unlinked(
    struct chimera_vfs_thread             *thread,
    const struct chimera_vfs_cred         *cred,
    const uint8_t                         *fh,
    int                                    fh_len,
    struct chimera_vfs_attrs              *set_attr,
    uint64_t                               attr_mask,
    chimera_vfs_create_unlinked_callback_t callback,
    void                                  *private_data)
{
    struct chimera_vfs_request *request;

    chimera_vfs_abort_if(!set_attr, "no setattr provided");

    request = chimera_vfs_request_alloc_anon(thread, cred, fh, fh_len, thread->anon_fh_key++);

    chimera_vfs_abort_if(!(request->module->capabilities & CHIMERA_VFS_CAP_CREATE_UNLINKED),
                         "module does not support create_unlinked");

    request->opcode                             = CHIMERA_VFS_OP_CREATE_UNLINKED;
    request->complete                           = chimera_vfs_create_unlinked_complete;
    request->create_unlinked.set_attr           = set_attr;
    request->create_unlinked.r_attr.va_req_mask = attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->create_unlinked.r_attr.va_set_mask = 0;
    request->proto_callback                     = callback;
    request->proto_private_data                 = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_open */
