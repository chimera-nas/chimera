// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"

/* Backend claim acquire (the claim-core projection boundary).  The module is
 * resolved from the bare fh and must declare the capability matching the
 * klass being projected -- AGGREGATE for the per-node cache token, RANGE for
 * a byte-range record (the alloc's capability gate answers ENOTSUP
 * otherwise), since a backend may arbitrate one without the other. */

static void
chimera_vfs_claim_acquire_backend_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_claim_acquire_backend_cb_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    struct chimera_claim_range_conflict    conflict = {
        .type   = request->claim_acquire.r_conflict_type,
        .offset = request->claim_acquire.r_conflict_offset,
        .length = request->claim_acquire.r_conflict_length,
        .pid    = request->claim_acquire.r_conflict_pid,
    };

    callback(request->status,
             request->claim_acquire.r_granted,
             request->claim_acquire.r_token,
             &conflict,
             request->proto_private_data);

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_claim_acquire_backend_complete */

SYMBOL_EXPORT void
chimera_vfs_claim_acquire_backend(
    struct chimera_vfs_thread             *thread,
    const uint8_t                         *fh,
    uint8_t                                fh_len,
    uint64_t                               fh_hash,
    uint8_t                                klass,
    uint8_t                                rev_used,
    uint8_t                                bind_deny,
    uint8_t                                exclusive,
    uint8_t                                flags,
    int32_t                                whence,
    uint64_t                               offset,
    uint64_t                               length,
    const struct chimera_claim_owner      *owner,
    uint64_t                               prev_token,
    void (                                *recall_cb )(
        void          *recall_arg,
        const uint8_t *fh,
        uint8_t        fh_len,
        uint64_t       fh_hash,
        uint64_t       token,
        uint8_t        retain),
    void                                  *recall_arg,
    chimera_vfs_claim_acquire_backend_cb_t callback,
    void                                  *private_data)
{
    void                       *mount_private;
    struct chimera_vfs_module  *module =
        chimera_vfs_resolve_mount(thread, fh, fh_len, 1, &mount_private);
    struct chimera_vfs_request *request;

    request = chimera_vfs_request_alloc_common(thread, NULL, module,
                                               mount_private,
                                               fh, fh_len, fh_hash,
                                               klass == CHIMERA_VFS_CLAIM_KLASS_RANGE
                                               ? CHIMERA_VFS_CAP_CLAIM_RANGE
                                               : CHIMERA_VFS_CAP_CLAIM_AGGREGATE);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), 0, 0, NULL, private_data);
        return;
    }

    request->opcode                          = CHIMERA_VFS_OP_CLAIM_ACQUIRE;
    request->complete                        = chimera_vfs_claim_acquire_backend_complete;
    request->claim_acquire.klass             = klass;
    request->claim_acquire.rev_used          = rev_used;
    request->claim_acquire.bind_deny         = bind_deny;
    request->claim_acquire.exclusive         = exclusive;
    request->claim_acquire.flags             = flags;
    request->claim_acquire.whence            = whence;
    request->claim_acquire.offset            = offset;
    request->claim_acquire.length            = length;
    request->claim_acquire.owner             = *owner;
    request->claim_acquire.prev_token        = prev_token;
    request->claim_acquire.recall_cb         = recall_cb;
    request->claim_acquire.recall_arg        = recall_arg;
    request->claim_acquire.r_token           = 0;
    request->claim_acquire.r_granted         = 0;
    request->claim_acquire.r_conflict_type   = CHIMERA_VFS_LOCK_UNLOCK;
    request->claim_acquire.r_conflict_offset = 0;
    request->claim_acquire.r_conflict_length = 0;
    request->claim_acquire.r_conflict_pid    = 0;
    request->proto_callback                  = callback;
    request->proto_private_data              = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_claim_acquire_backend */
