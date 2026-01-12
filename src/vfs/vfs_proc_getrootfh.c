// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_attr_cache.h"
#include "common/misc.h"
#include "common/macros.h"

static void
chimera_vfs_getrootfh_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread     *thread     = request->thread;
    struct chimera_vfs_attr_cache *attr_cache = thread->vfs->vfs_attr_cache;
    chimera_vfs_getattr_callback_t callback   = request->proto_callback;


    if (request->status == CHIMERA_VFS_OK) {
        chimera_vfs_attr_cache_insert(attr_cache,
                                      request->fh_hash,
                                      request->fh,
                                      request->fh_len,
                                      &request->getrootfh.r_attr);
    }

    chimera_vfs_complete(
        request);

    callback(request->status,
             &request->getrootfh.r_attr,
             request->proto_private_data);

    chimera_vfs_request_free(thread, request);
} /* chimera_vfs_getattr_complete */

SYMBOL_EXPORT void
chimera_vfs_getrootfh(
    struct chimera_vfs_thread       *thread,
    struct chimera_vfs_module       *module,
    const char                      *path,
    uint32_t                         pathlen,
    uint64_t                         req_attr_mask,
    chimera_vfs_getrootfh_callback_t callback,
    void                            *private_data)
{
    struct chimera_vfs_request *request;
    uint64_t                    fh_hash;
    uint8_t                    *fh = &module->fh_magic;

    fh_hash = chimera_vfs_hash(fh, 1);

    request = chimera_vfs_request_alloc_by_hash(thread, fh, 1, fh_hash);

    /* For getrootfh operations, the module is passed directly - set it
     * since chimera_vfs_get_module returns NULL (no mount exists for this FH) */
    request->module = module;

    request->opcode                       = CHIMERA_VFS_OP_GETROOTFH;
    request->complete                     = chimera_vfs_getrootfh_complete;
    request->getrootfh.path               = path;
    request->getrootfh.pathlen            = pathlen;
    request->getrootfh.r_attr.va_req_mask = req_attr_mask | CHIMERA_VFS_ATTR_MASK_CACHEABLE;
    request->getrootfh.r_attr.va_set_mask = 0;
    request->proto_callback               = callback;
    request->proto_private_data           = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_getrootfh */
