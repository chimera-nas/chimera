// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>

#include "vfs_procs.h"
#include "vfs_internal.h"
#include "vfs_notify.h"
#include "common/macros.h"

/*
 * Deferred change-notify recording for enlisted mutations.
 *
 * An op enlisted in an explicit compound suppresses its change-notify
 * emission at completion (chimera_vfs_request_publishes): the effect is
 * provisional and a watcher woken now could observe state that never
 * commits.  But suppressing it FOREVER is a correctness hole the other way:
 * change-notify watchers and SMB directory-lease holders would never learn
 * of a committed remove/rename/create, so pending CHANGE_NOTIFYs would park
 * forever and cached directory enumerations would go stale.  The gated
 * sites therefore RECORD the emission here, on the compound, and
 * chimera_vfs_compound_end_complete replays the records in order once the
 * commit outcome is known.
 *
 * Recording and replay both run on the compound's beginning thread (member
 * op completions and the end completion are drained onto request->thread),
 * so the compound's list and the thread's node pool need no locking --
 * and replay calls chimera_vfs_notify_emit* from exactly the same thread
 * context a standalone op's completion would.
 */
void
chimera_vfs_notify_defer(
    struct chimera_vfs_request *request,
    int                         kind,
    const uint8_t              *fh,
    uint16_t                    fh_len,
    uint32_t                    action,
    const char                 *name,
    uint16_t                    name_len,
    const char                 *old_name,
    uint16_t                    old_name_len,
    uint64_t                    skip_lo,
    uint64_t                    skip_hi,
    uint8_t                     has_skip)
{
    struct chimera_vfs_thread          *thread   = request->thread;
    struct chimera_vfs_compound        *compound = request->compound;
    struct chimera_vfs_deferred_notify *node;

    chimera_vfs_abort_if(!compound,
                         "deferred notify recorded on a request with no compound");

    if (fh_len == 0 || fh_len > CHIMERA_VFS_FH_SIZE) {
        /* No routable fh reaches here with a bad length; refuse rather
         * than overflow the record's buffer. */
        return;
    }
    if (name_len > CHIMERA_VFS_NAME_MAX) {
        name_len = CHIMERA_VFS_NAME_MAX;
    }
    if (old_name_len > CHIMERA_VFS_NAME_MAX) {
        old_name_len = CHIMERA_VFS_NAME_MAX;
    }

    if (compound->notify_event_count >= CHIMERA_VFS_COMPOUND_NOTIFY_MAX) {
        /* Cap reached: degrade this fh to one coarse OVERFLOW record
         * (dedup'd), which replays as mark-every-watch-overflowed plus a
         * directory-lease break -- the honest "too many changes, rescan"
         * signal (upstream STATUS_NOTIFY_ENUM_DIR).  A DELETE past the cap
         * degrades the same way: the watch armed on the removed object
         * wakes, rescans, and finds it gone. */
        for (node = compound->notify_events; node; node = node->next) {
            if (node->kind == CHIMERA_VFS_DEFERRED_NOTIFY_OVERFLOW &&
                node->fh_len == fh_len &&
                memcmp(node->fh, fh, fh_len) == 0) {
                return;
            }
        }

        if (compound->notify_overflow_count >=
            CHIMERA_VFS_COMPOUND_NOTIFY_OVERFLOW_MAX) {
            compound->notify_dropped++;
            return;
        }

        node = chimera_vfs_deferred_notify_alloc(thread);

        if (!node) {
            compound->notify_dropped++;
            return;
        }

        node->kind         = CHIMERA_VFS_DEFERRED_NOTIFY_OVERFLOW;
        node->has_skip     = 0;
        node->fh_len       = fh_len;
        node->name_len     = 0;
        node->old_name_len = 0;
        node->action       = 0;
        node->skip_lo      = 0;
        node->skip_hi      = 0;
        memcpy(node->fh, fh, fh_len);

        compound->notify_overflow_count++;
    } else {
        node = chimera_vfs_deferred_notify_alloc(thread);

        if (!node) {
            compound->notify_dropped++;
            return;
        }

        node->kind         = (uint8_t) kind;
        node->has_skip     = has_skip;
        node->fh_len       = fh_len;
        node->name_len     = name_len;
        node->old_name_len = old_name_len;
        node->action       = action;
        node->skip_lo      = skip_lo;
        node->skip_hi      = skip_hi;
        memcpy(node->fh, fh, fh_len);
        if (name_len) {
            memcpy(node->name, name, name_len);
        }
        if (old_name_len) {
            memcpy(node->old_name, old_name, old_name_len);
        }

        compound->notify_event_count++;
    }

    node->next = NULL;
    if (compound->notify_events_tail) {
        compound->notify_events_tail->next = node;
    } else {
        compound->notify_events = node;
    }
    compound->notify_events_tail = node;
} /* chimera_vfs_notify_defer */

void
chimera_vfs_compound_notify_flush(
    struct chimera_vfs_thread   *thread,
    struct chimera_vfs_compound *compound,
    int                          emit)
{
    struct chimera_vfs_notify          *notify = thread->vfs->vfs_notify;
    struct chimera_vfs_deferred_notify *node;

    while ((node = compound->notify_events) != NULL) {
        compound->notify_events = node->next;

        if (emit) {
            switch (node->kind) {
                case CHIMERA_VFS_DEFERRED_NOTIFY_EMIT:
                    /* has_skip == 0 makes this identical to the plain
                     * chimera_vfs_notify_emit the site would have called. */
                    chimera_vfs_notify_emit_lease(notify,
                                                  node->fh, node->fh_len,
                                                  node->action,
                                                  node->name_len ? node->name : NULL,
                                                  node->name_len,
                                                  node->old_name_len ? node->old_name : NULL,
                                                  node->old_name_len,
                                                  node->skip_lo, node->skip_hi,
                                                  node->has_skip != 0);
                    break;
                case CHIMERA_VFS_DEFERRED_NOTIFY_DELETE:
                    chimera_vfs_notify_emit_delete(notify,
                                                   node->fh, node->fh_len);
                    break;
                case CHIMERA_VFS_DEFERRED_NOTIFY_OVERFLOW:
                    chimera_vfs_notify_emit_overflow(notify,
                                                     node->fh, node->fh_len);
                    break;
                default:
                    break;
            } /* switch */
        }

        chimera_vfs_deferred_notify_free(thread, node);
    }

    compound->notify_events_tail    = NULL;
    compound->notify_event_count    = 0;
    compound->notify_overflow_count = 0;

    if (compound->notify_dropped) {
        /* Beyond even the per-fh coarse records: some watcher wakeups were
         * lost.  Log the first occurrence process-wide; the condition needs
         * a single compound touching > CHIMERA_VFS_COMPOUND_NOTIFY_OVERFLOW_MAX
         * distinct directories past the record cap (or node allocation
         * failure), so it marks a workload to widen the caps for. */
        static int chimera_vfs_notify_drop_logged;

        if (!__atomic_exchange_n(&chimera_vfs_notify_drop_logged, 1,
                                 __ATOMIC_RELAXED)) {
            chimera_vfs_error(
                "compound deferred-notify overflow: %u change-notify wakeup(s) "
                "dropped beyond %u records + %u coarse rescan(s); "
                "further occurrences are not logged",
                compound->notify_dropped,
                CHIMERA_VFS_COMPOUND_NOTIFY_MAX,
                CHIMERA_VFS_COMPOUND_NOTIFY_OVERFLOW_MAX);
        }
        compound->notify_dropped = 0;
    }
} /* chimera_vfs_compound_notify_flush */

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

    /* Replay the change-notify emissions this compound's enlisted mutations
     * recorded (chimera_vfs_notify_defer), in recorded order and before the
     * consumer's end callback -- matching standalone ops, which emit before
     * their own completion callback runs.  ABORT and a rolled-back commit
     * (a delivered conflict, whether or not rewritten to EXHAUSTED above)
     * free the records unemitted: those effects never became observable.
     * Any other end status emits -- for a failure of unknown depth (e.g. a
     * durability barrier error after apply) a spurious wakeup merely makes
     * a watcher rescan, while a missed FILE_REMOVED/DELETE_PENDING wakeup
     * would park a CHANGE_NOTIFY forever. */
    chimera_vfs_compound_notify_flush(
        thread, compound,
        request->compound_op.end_flag != CHIMERA_VFS_COMPOUND_ABORT &&
        request->status != CHIMERA_VFS_ECOMPOUND_CONFLICT &&
        request->status != CHIMERA_VFS_ECOMPOUND_EXHAUSTED);

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
         * cost.  Retire the handle locally.
         *
         * The deferred-notify list must be empty here: only ENLISTED
         * mutations record, and an unbound compound never enlisted anything
         * (binding is monotonic, so unbound at end means never bound; its
         * ops all ejected and published standalone).  Assert that, and
         * drain defensively so a release build cannot leak nodes. */
#ifdef CHIMERA_SANITIZE
        chimera_vfs_abort_if(compound->notify_events != NULL,
                             "unbound compound carries deferred notify records");
#endif /* ifdef CHIMERA_SANITIZE */
        chimera_vfs_compound_notify_flush(thread, compound, 0);
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
         * thread teardown check does not report a core-made leak.  Nothing
         * committed, so any deferred notify records are freed unemitted. */
        chimera_vfs_compound_notify_flush(thread, compound, 0);
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
