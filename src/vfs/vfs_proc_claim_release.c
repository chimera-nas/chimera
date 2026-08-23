// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs/vfs_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"

/* Backend lease release/downgrade.  For an AGGREGATE token under recall the
 * release with the retained mask IS the recall acknowledgment. */

static void
chimera_vfs_claim_release_backend_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_claim_release_backend_cb_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    if (callback) {
        callback(request->status, request->proto_private_data);
    }

    chimera_vfs_request_free(request->thread, request);
} /* chimera_vfs_claim_release_backend_complete */

SYMBOL_EXPORT void
chimera_vfs_claim_release_backend(
    struct chimera_vfs_thread             *thread,
    const uint8_t                         *fh,
    uint8_t                                fh_len,
    uint64_t                               fh_hash,
    uint8_t                                klass,
    uint64_t                               token,
    uint8_t                                retained,
    int32_t                                whence,
    uint64_t                               offset,
    uint64_t                               length,
    const struct chimera_claim_owner      *owner,
    chimera_vfs_claim_release_backend_cb_t callback,
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
        if (callback) {
            callback(CHIMERA_VFS_PTR_ERR(request), private_data);
        }
        return;
    }

    request->opcode                 = CHIMERA_VFS_OP_CLAIM_RELEASE;
    request->complete               = chimera_vfs_claim_release_backend_complete;
    request->claim_release.token    = token;
    request->claim_release.retained = retained;
    request->claim_release.klass    = klass;
    request->claim_release.whence   = whence;
    request->claim_release.offset   = offset;
    request->claim_release.length   = length;
    if (owner) {
        request->claim_release.owner = *owner;
    } else {
        memset(&request->claim_release.owner, 0,
               sizeof(request->claim_release.owner));
    }
    request->proto_callback     = callback;
    request->proto_private_data = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_claim_release_backend */
