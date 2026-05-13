// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"

static void
chimera_vfs_getparent_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_getparent_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             request->getparent.r_parent_fh,
             request->getparent.r_parent_fh_len,
             request->getparent.r_name,
             request->getparent.r_name_len,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_getparent_complete */

SYMBOL_EXPORT void
chimera_vfs_getparent(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    const void                      *fh,
    int                              fhlen,
    chimera_vfs_getparent_callback_t callback,
    void                            *private_data)
{
    struct chimera_vfs_module  *module;
    struct chimera_vfs_request *request;
    uint64_t                    fh_hash;

    module = chimera_vfs_get_module(thread, fh, fhlen);

    fh_hash = chimera_vfs_hash(fh, fhlen);

    request = chimera_vfs_request_alloc_common(thread, cred, module,
                                               fh, fhlen, fh_hash,
                                               CHIMERA_VFS_CAP_RPL);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, 0, NULL, 0, private_data);
        return;
    }

    request->opcode                    = CHIMERA_VFS_OP_GETPARENT;
    request->complete                  = chimera_vfs_getparent_complete;
    request->getparent.r_parent_fh_len = 0;
    request->getparent.r_name_len      = 0;
    request->proto_callback            = callback;
    request->proto_private_data        = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_getparent */
