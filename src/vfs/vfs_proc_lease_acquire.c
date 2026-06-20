// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"

static void
chimera_vfs_lease_acquire_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_lease_backend_acquire_cb_t callback = request->proto_callback;
    enum chimera_vfs_lease_result          result;

    chimera_vfs_complete(request);

    /* The backend reports a grant by returning OK with a non-empty granted
     * mode; a hard cross-node conflict comes back as OK with granted == 0 (or
     * an explicit error).  Anything else is treated as DENIED. */
    if (request->status == CHIMERA_VFS_OK &&
        request->lease_acquire.r_granted.granted != 0) {
        result = CHIMERA_VFS_LEASE_GRANTED;
    } else {
        result = CHIMERA_VFS_LEASE_DENIED;
    }

    callback(request->status,
             result,
             request->lease_acquire.r_granted,
             request->lease_acquire.r_token,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_lease_acquire_complete */

SYMBOL_EXPORT void
chimera_vfs_lease_acquire_backend(
    struct chimera_vfs_thread             *thread,
    const struct chimera_vfs_cred         *cred,
    const void                            *fh,
    int                                    fhlen,
    uint8_t                                kind,
    struct chimera_vfs_lease_mode          mode,
    uint64_t                               offset,
    uint64_t                               length,
    uint32_t                               protocol,
    uint64_t                               owner_lo,
    uint64_t                               owner_hi,
    chimera_vfs_lease_backend_acquire_cb_t callback,
    void                                  *private_data)
{
    struct chimera_vfs_request *request;
    struct chimera_vfs_module  *module = chimera_vfs_get_module(thread, fh, fhlen);

    request = chimera_vfs_request_alloc_common(thread, cred, module, fh, fhlen,
                                               chimera_vfs_hash(fh, fhlen),
                                               CHIMERA_VFS_CAP_LEASE);

    if (CHIMERA_VFS_IS_ERR(request)) {
        struct chimera_vfs_lease_mode none = { 0, 0 };
        callback(CHIMERA_VFS_PTR_ERR(request), CHIMERA_VFS_LEASE_DENIED, none, 0,
                 private_data);
        return;
    }

    request->opcode                          = CHIMERA_VFS_OP_LEASE_ACQUIRE;
    request->complete                        = chimera_vfs_lease_acquire_complete;
    request->lease_acquire.kind              = kind;
    request->lease_acquire.mode              = mode;
    request->lease_acquire.offset            = offset;
    request->lease_acquire.length            = length;
    request->lease_acquire.protocol          = protocol;
    request->lease_acquire.owner_lo          = owner_lo;
    request->lease_acquire.owner_hi          = owner_hi;
    request->lease_acquire.r_token           = 0;
    request->lease_acquire.r_granted.granted = 0;
    request->lease_acquire.r_granted.denied  = 0;
    request->proto_callback                  = callback;
    request->proto_private_data              = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_lease_acquire_backend */
