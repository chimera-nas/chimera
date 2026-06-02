// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"

SYMBOL_EXPORT uint64_t
chimera_vfs_txn_alloc_ts(struct chimera_vfs_thread *thread)
{
    /* Strictly increasing and globally unique across threads.  Starts at 1 so a
     * priority of 0 is never handed out.  Lower = older = wins under wait-die. */
    return __atomic_add_fetch(&thread->vfs->txn_ts_counter, 1, __ATOMIC_RELAXED);
} /* chimera_vfs_txn_alloc_ts */

static void
chimera_vfs_begin_transaction_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread       *thread   = request->thread;
    chimera_vfs_begin_txn_callback_t callback = request->proto_callback;
    struct chimera_vfs_transaction  *txn      = NULL;

    if (request->status == CHIMERA_VFS_OK) {
        txn = request->transaction_op.r_txn;

        if (txn) {
            /* The backend allocated its transaction object; stamp the core-owned
             * header fields the VFS core relies on (dispatch routing + wait-die). */
            txn->ts         = request->transaction_op.ts;
            txn->route_hash = request->transaction_op.route_hash;
            txn->mode       = request->transaction_op.mode;
            txn->module     = request->module;
        }
    }

    chimera_vfs_complete(request);

    callback(request->status, txn, request->proto_private_data);

    chimera_vfs_request_free(thread, request);
} /* chimera_vfs_begin_transaction_complete */

SYMBOL_EXPORT void
chimera_vfs_begin_transaction(
    struct chimera_vfs_thread       *thread,
    const struct chimera_vfs_cred   *cred,
    const void                      *fh,
    int                              fhlen,
    enum chimera_vfs_txn_mode        mode,
    uint64_t                         ts,
    chimera_vfs_begin_txn_callback_t callback,
    void                            *private_data)
{
    struct chimera_vfs_module  *module = chimera_vfs_get_module(thread, fh, fhlen);
    struct chimera_vfs_request *request;

    /* No-op fast path: the module is unknown or non-transactional.  Allocate and
     * dispatch nothing; the caller proceeds in legacy autocommit mode (it leaves
     * request->transaction NULL on every subsequent op). */
    if (!module || !(module->capabilities & CHIMERA_VFS_CAP_TRANSACTIONAL)) {
        callback(CHIMERA_VFS_OK, NULL, private_data);
        return;
    }

    request = chimera_vfs_request_alloc_common(thread, cred, module, fh, fhlen,
                                               chimera_vfs_hash(fh, fhlen),
                                               CHIMERA_VFS_CAP_TRANSACTIONAL);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), NULL, private_data);
        return;
    }

    request->opcode              = CHIMERA_VFS_OP_BEGIN_TRANSACTION;
    request->complete            = chimera_vfs_begin_transaction_complete;
    request->transaction         = NULL;       /* begin is not itself enlisted */
    request->transaction_op.mode = mode;
    request->transaction_op.ts   = ts;
    /* Pin every enlisted op + the end op to the thread this begin routes to. */
    request->transaction_op.route_hash = request->fh_hash;
    request->transaction_op.r_txn      = NULL;
    request->proto_callback            = callback;
    request->proto_private_data        = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_begin_transaction */
