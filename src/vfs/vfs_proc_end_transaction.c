// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"

static void
chimera_vfs_end_transaction_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread     *thread   = request->thread;
    chimera_vfs_end_txn_callback_t callback = request->proto_callback;

    chimera_vfs_complete(request);

    /* ETXN_CONFLICT here (e.g. cairn optimistic-commit validation) means the
     * transaction was already rolled back; the caller must retry from the top. */
    callback(request->status, request->proto_private_data);

    chimera_vfs_request_free(thread, request);
} /* chimera_vfs_end_transaction_complete */

SYMBOL_EXPORT void
chimera_vfs_end_transaction(
    struct chimera_vfs_thread      *thread,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_transaction *txn,
    enum chimera_vfs_txn_end        end_flag,
    chimera_vfs_end_txn_callback_t  callback,
    void                           *private_data)
{
    struct chimera_vfs_request *request;

    /* No-op: the backend was non-transactional (begin returned NULL). */
    if (!txn) {
        callback(CHIMERA_VFS_OK, private_data);
        return;
    }

    request = chimera_vfs_request_alloc_common(thread, cred, txn->module,
                                               NULL, 0, txn->route_hash,
                                               CHIMERA_VFS_CAP_TRANSACTIONAL);

    if (CHIMERA_VFS_IS_ERR(request)) {
        callback(CHIMERA_VFS_PTR_ERR(request), private_data);
        return;
    }

    request->opcode   = CHIMERA_VFS_OP_END_TRANSACTION;
    request->complete = chimera_vfs_end_transaction_complete;
    /* Set so dispatch routes to the transaction's owning thread and the backend
     * recovers its transaction object from request->transaction. */
    request->transaction             = txn;
    request->transaction_op.end_flag = end_flag;
    request->proto_callback          = callback;
    request->proto_private_data      = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_end_transaction */
