// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"

/* Backend lease acquire (the claim-core projection boundary).  The module is
 * resolved from the bare fh and must declare CHIMERA_VFS_CAP_LEASE (the
 * alloc's capability gate answers ENOTSUP otherwise). */

static void
chimera_vfs_lease_acquire_backend_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_lease_acquire_backend_cb_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    callback(request->status,
             request->lease_acquire.r_granted,
             request->lease_acquire.r_token,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_lease_acquire_backend_complete */

SYMBOL_EXPORT void
chimera_vfs_lease_acquire_backend(
    struct chimera_vfs_thread             *thread,
    const uint8_t                         *fh,
    uint8_t                                fh_len,
    uint64_t                               fh_hash,
    uint8_t                                klass,
    uint8_t                                rev_used,
    uint8_t                                bind_deny,
    uint8_t                                exclusive,
    uint64_t                               offset,
    uint64_t                               length,
    const struct chimera_claim_owner      *owner,
    uint64_t                               prev_token,
    void                                ( *recall_cb )(
        void          *recall_arg,
        const uint8_t *fh,
        uint8_t        fh_len,
        uint64_t       fh_hash,
        uint64_t       token,
        uint8_t        retain),
    void                                  *recall_arg,
    chimera_vfs_lease_acquire_backend_cb_t callback,
    void                                  *private_data)
{
    void                       *mount_private;
    struct chimera_vfs_module  *module =
        chimera_vfs_resolve_mount(thread, fh, fh_len, 1, &mount_private);
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_common(thread, NULL, module,
                                               mount_private,
                                               fh, fh_len, fh_hash,
                                               CHIMERA_VFS_CAP_LEASE);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), 0, 0, private_data);
        return;
    }

    request->opcode                   = CHIMERA_VFS_OP_LEASE_ACQUIRE;
    request->complete                 = chimera_vfs_lease_acquire_backend_complete;
    request->lease_acquire.klass      = klass;
    request->lease_acquire.rev_used   = rev_used;
    request->lease_acquire.bind_deny  = bind_deny;
    request->lease_acquire.exclusive  = exclusive;
    request->lease_acquire.offset     = offset;
    request->lease_acquire.length     = length;
    request->lease_acquire.owner      = *owner;
    request->lease_acquire.prev_token = prev_token;
    request->lease_acquire.recall_cb  = recall_cb;
    request->lease_acquire.recall_arg = recall_arg;
    request->lease_acquire.r_token    = 0;
    request->lease_acquire.r_granted  = 0;
    request->proto_callback           = callback;
    request->proto_private_data       = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_lease_acquire_backend */
