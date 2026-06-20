// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"

static void
chimera_vfs_lease_release_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_lease_backend_release_cb_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status, request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_lease_release_complete */

SYMBOL_EXPORT void
chimera_vfs_lease_release_backend(
    struct chimera_vfs_thread             *thread,
    const struct chimera_vfs_cred         *cred,
    const void                            *fh,
    int                                    fhlen,
    uint64_t                               token,
    struct chimera_vfs_lease_mode          mode,
    uint64_t                               offset,
    uint64_t                               length,
    chimera_vfs_lease_backend_release_cb_t callback,
    void                                  *private_data)
{
    struct chimera_vfs_request *request;
    struct chimera_vfs_module  *module = chimera_vfs_get_module(thread, fh, fhlen);

    request = chimera_vfs_request_alloc_common(thread, cred, module, fh, fhlen,
                                               chimera_vfs_hash(fh, fhlen),
                                               CHIMERA_VFS_CAP_LEASE);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), private_data);
        return;
    }

    request->opcode               = CHIMERA_VFS_OP_LEASE_RELEASE;
    request->complete             = chimera_vfs_lease_release_complete;
    request->lease_release.token  = token;
    request->lease_release.mode   = mode;
    request->lease_release.offset = offset;
    request->lease_release.length = length;
    request->proto_callback       = callback;
    request->proto_private_data   = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_lease_release_backend */
