// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"

SYMBOL_EXPORT uint64_t
chimera_vfs_txn_alloc_ts(struct chimera_vfs_thread *thread)
{
    /* Globally-unique transaction priority without a shared atomic.  The high
     * bits are a TSC-anchored, per-thread strictly-increasing counter (age order
     * -> a longer-lived txn outranks a newcomer, so WFG victim selection -- abort
     * the highest ts -- is starvation-free); the low CHIMERA_VFS_TXN_THREAD_BITS
     * carry this thread's dense id so two threads never collide.  Shifting the
     * TSC down by the thread bits first means (hi << bits) can never overflow,
     * regardless of the raw counter magnitude; the per-thread bump guarantees
     * uniqueness for two txns that land in the same TSC tick.  txn_ts_hi is
     * seeded to 1 at thread init so the returned ts is never 0 (which is
     * reserved for autocommit txns). */
    uint64_t hi = chimera_vfs_now_ticks() >> CHIMERA_VFS_TXN_THREAD_BITS;

    if (hi <= thread->txn_ts_hi) {
        hi = thread->txn_ts_hi + 1;
    }
    thread->txn_ts_hi = hi;

    return (hi << CHIMERA_VFS_TXN_THREAD_BITS) | thread->txn_thread_id;
} /* chimera_vfs_txn_alloc_ts */

/* Fire-and-forget completion for the backend begin op: there is no caller
 * waiting on it (the handle was returned synchronously), so just retire the
 * request.  Any backend setup ran in the begin handler on the owning thread,
 * ahead of the first enlisted op by dispatch FIFO order. */
static void
chimera_vfs_begin_transaction_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread *thread = request->thread;

    chimera_vfs_complete(request);
    chimera_vfs_request_free(thread, request);
} /* chimera_vfs_begin_transaction_complete */

SYMBOL_EXPORT struct chimera_vfs_transaction *
chimera_vfs_begin_transaction(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred,
    const void                    *hint_fh,
    int                            hint_fhlen,
    enum chimera_vfs_txn_mode      mode,
    uint64_t                       ts)
{
    struct chimera_vfs_module      *module;
    struct chimera_vfs_transaction *txn;
    struct chimera_vfs_request     *request;
    uint64_t                        route_hash;

    /* Autocommit fast path: no hint (module unknowable) or a non-transactional
     * module.  Return NULL; the caller leaves request->transaction NULL on every
     * subsequent op and each one autocommits independently. */
    module = hint_fh ? chimera_vfs_get_module(thread, hint_fh, hint_fhlen) : NULL;

    if (!module || !(module->capabilities & CHIMERA_VFS_CAP_TRANSACTIONAL)) {
        return NULL;
    }

    /* Fast, local allocation of the handle: the core owns this memory (sized by
    * the backend's txn_size) and frees it at end; the backend initializes its
    * portion in place in the begin handler.  Hashing the hint steers the
    * transaction's owning thread -- the same file always lands on the same
    * worker (read/write locality), and distinct files spread across workers. */
    route_hash = chimera_vfs_hash(hint_fh, hint_fhlen);

    txn = calloc(1, module->txn_size);

    txn->ts         = ts;
    txn->route_hash = route_hash;
    txn->mode       = mode;
    txn->module     = module;

    /* Eager fire-and-forget begin op: lets the backend set up per-transaction
     * state on the owning thread before the first enlisted op arrives.  It
     * routes by txn->route_hash (request->transaction is set), so it queues
     * ahead of every enlisted op on that one thread. */
    request = chimera_vfs_request_alloc_common(thread, cred, module,
                                               hint_fh, hint_fhlen, route_hash,
                                               CHIMERA_VFS_CAP_TRANSACTIONAL);

    if (CHIMERA_VFS_IS_ERR(request)) {
        free(txn);
        return NULL;
    }

    request->opcode             = CHIMERA_VFS_OP_BEGIN_TRANSACTION;
    request->complete           = chimera_vfs_begin_transaction_complete;
    request->transaction        = txn;
    request->proto_private_data = NULL;

    chimera_vfs_dispatch(request);

    return txn;
} /* chimera_vfs_begin_transaction */
