// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "vfs_procs.h"
#include "vfs_internal.h"
#include "common/macros.h"

static void
chimera_vfs_compound_end_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_thread          *thread   = request->thread;
    chimera_vfs_compound_end_callback_t callback = request->proto_callback;
    struct chimera_vfs_compound        *compound = request->compound;

    /* Conflict suppression, mirrored from the member-op chokepoint in
     * chimera_vfs_complete (which exempts the end opcode): a delivered
     * ECOMPOUND_CONFLICT obligates a replay, but a compound that lost a
     * MUTATING op to ejection must never be replayed (the ejected mutation
     * already committed standalone; a replay would double-execute it), and
     * one whose consumer did not opt in (RETRYABLE) will never replay.
     * Read-only ejections (the root-module path hop) are harmless to
     * re-execute and do not forfeit the replay right.  Rewrite to the
     * retriable-but-never-replayed ECOMPOUND_EXHAUSTED in the core rather
     * than trusting drivers to honor the rule. */
    if (request->status == CHIMERA_VFS_ECOMPOUND_CONFLICT &&
        (compound->ejected_mutating_ops > 0 ||
         !(compound->flags & CHIMERA_VFS_COMPOUND_RETRYABLE))) {
        request->status = CHIMERA_VFS_ECOMPOUND_EXHAUSTED;
    }

    chimera_vfs_complete(request);

    /* ECOMPOUND_CONFLICT here (e.g. cairn optimistic-commit validation) means the
     * compound was already rolled back; the caller must replay from the top
     * (which acquires a fresh handle), so the old handle is dead either way. */
    callback(request->status, request->proto_private_data);

    /* The core owns the handle's memory: registry out, back to the thread
     * pool (the backend released only its own per-compound resources at
     * commit/abort). */
    DL_DELETE(thread->active_compounds, compound);
    LL_PREPEND(thread->free_compounds, compound);

    chimera_vfs_request_free(thread, request);
} /* chimera_vfs_compound_end_complete */

SYMBOL_EXPORT void
chimera_vfs_compound_end(
    struct chimera_vfs_thread          *thread,
    const struct chimera_vfs_cred      *cred,
    struct chimera_vfs_compound        *compound,
    enum chimera_vfs_compound_end       end_flag,
    chimera_vfs_compound_end_callback_t callback,
    void                               *private_data)
{
    struct chimera_vfs_request *request;

    /* The per-thread LOOSE singleton is never bound and never registered:
     * end is a synchronous OK that recycles nothing.  (A NULL compound is
     * tolerated the same way for legacy callers, though begin can no longer
     * produce one.) */
    if (!compound || compound == thread->loose_compound) {
        callback(CHIMERA_VFS_OK, private_data);
        return;
    }

    if (!compound->module) {
        /* Never bound: no backend ever saw this compound, so there is
         * nothing to commit or abort -- a synchronous OK at zero backend
         * cost.  Retire the handle locally. */
        DL_DELETE(thread->active_compounds, compound);
        LL_PREPEND(thread->free_compounds, compound);
        callback(CHIMERA_VFS_OK, private_data);
        return;
    }

#ifdef CHIMERA_SANITIZE
    chimera_vfs_abort_if(compound->inflight_ops != 0,
                         "compound end issued with %u enlisted op(s) still in flight",
                         compound->inflight_ops);
#endif /* ifdef CHIMERA_SANITIZE */

    request = chimera_vfs_request_alloc_common(thread, cred, compound->module,
                                               compound->mount_private,
                                               NULL, 0, compound->route_hash,
                                               CHIMERA_VFS_CAP_COMPOUND);

    if (CHIMERA_VFS_IS_ERR(request)) {
        /* Unreachable for a bound compound (its module advertises
         * CAP_COMPOUND), but if it ever fires, retire the handle so the
         * thread teardown check does not report a core-made leak. */
        DL_DELETE(thread->active_compounds, compound);
        LL_PREPEND(thread->free_compounds, compound);
        callback(CHIMERA_VFS_PTR_ERR(request), private_data);
        return;
    }

    request->opcode   = CHIMERA_VFS_OP_COMPOUND_END;
    request->complete = chimera_vfs_compound_end_complete;
    /* Set so dispatch routes to the compound's owning thread and the backend
     * recovers its compound object from request->compound. */
    request->compound             = compound;
    request->compound_op.end_flag = end_flag;
    request->proto_callback       = callback;
    request->proto_private_data   = private_data;

    chimera_vfs_dispatch(request);
} /* chimera_vfs_compound_end */
