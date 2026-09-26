// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * The compound executor (contract in vfs_compound.h).
 *
 * A sequence is a small array of ops plus a cursor and a CURRENT object.  The
 * executor issues one op at a time through the ordinary per-op API -- so the
 * backend, the caches, the DAC gate and the claim layer all see exactly what
 * they see for an unsequenced op -- and each completion advances the cursor.
 *
 * The CURRENT object is kept as an open handle rather than a file handle,
 * which is the one real difference from how a protocol server chains by hand.
 * A server holding a raw fh has to re-open it for every op that needs a
 * handle; NFS4's LOOKUP, GETATTR and ACCESS each do their own open_fh of the
 * same current fh today.  Holding the handle across the sequence collapses
 * that to one open per object, and the handle is released as soon as the
 * current object changes.
 *
 * THE SAVED SLOT holds a file handle and nothing else.  It would be tempting to
 * park the open handle there too and hand it back on RESTOREFH, but an open
 * handle is a refcount in one of the VFS open caches with exactly one owner
 * here: putting the same pointer in two slots means every release has to know
 * whether the other slot still refers to it, and the escape hatch
 * (chimera_vfs_dup_handle, to take a second reference) aborts outright on a
 * synthetic handle -- which is what an inferred open of a native backend
 * produces, i.e. the common case.  So SAVEFH copies the handle *name* and
 * RESTOREFH re-opens: one extra open on a path a protocol takes rarely, in
 * exchange for a slot that cannot double-release or leak.
 */

#include <stdlib.h>
#include <fcntl.h>
#include <string.h>
#include <stddef.h>

#include <utlist.h>

#include "vfs_compound.h"
#include "vfs_procs.h"
#include "vfs_claim.h"
#include "vfs_claim_journal_attempt.h"
#include "vfs_internal.h"
#include "vfs_release.h"
#include "sdk/vfs_access.h"
#include "sdk/vfs_acl.h"
#include "common/macros.h"

struct chimera_vfs_compound_coordination {
    struct chimera_vfs_compound_coordination *next;
    uint32_t                                  index, fh_len;
    uint64_t                                  token;
    chimera_vfs_compound_coordinate_t          start;
    void                                     *private_data;
    uint8_t                                   fh[CHIMERA_VFS_FH_SIZE];
    enum chimera_vfs_error status;
};

struct chimera_vfs_compound_group {
    struct chimera_vfs_compound_group_config config;
    enum chimera_vfs_error                   status;
    uint32_t                                 last_op;
};

struct chimera_vfs_compound {
    struct chimera_vfs_thread                *thread;
    const struct chimera_vfs_cred            *cred;
    const struct chimera_vfs_cred            *default_cred;

    /* Link for the owning thread's free list.  Everything from here down to
     * (but not including) ops[] is cleared wholesale by chimera_vfs_compound_
     * reset(), so a field added to this struct is reset without being named --
     * which is why ops[] is last. */
    struct chimera_vfs_compound              *next;

    struct chimera_vfs_claim_journal         *claim_journal;
    uint32_t                                  claim_journal_budget;
    uint8_t                                   claim_journal_published;
    struct chimera_vfs_claim_access_journal  *access_journal;
    uint8_t                                   access_journal_published;
    struct chimera_vfs_compound_group        *groups;
    uint32_t                                  num_groups;
    uint32_t                                  group_index;
    uint8_t                                   group_active;
    uint8_t                                   canceled;
    uint32_t                                  cancel_defer_end; /* active end index + 1 */
    const void                               *admission_cookie;
    uint32_t                                  num_ops;
    /* An adder could not build its op -- the sequence is full, or an argument
     * was malformed.  Submitting runs nothing and fails. */
    uint8_t                                   build_failed;
    enum chimera_vfs_error                    build_error;
    uint32_t                                  index; /* op being executed        */
    uint32_t                                  completed; /* ops that ran             */
    enum chimera_vfs_error                    status;
    enum chimera_vfs_error                    execution_status;
    enum chimera_vfs_error                    finish_status;
    chimera_vfs_compound_finish_handler_t     finish_handler;
    void                                     *finish_private;
    uint8_t                                   finishing;

    /* The current object: its file handle, and an open handle for it once
     * something has needed one.  handle_flags records what that handle was
     * opened with, so an op needing more than it carries (a LOOKUP wanting a
     * directory open) can re-open rather than settle for less. */
    uint8_t                                   fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                                  fh_len;
    struct chimera_vfs_open_handle           *handle;
    unsigned int                              handle_flags;
    /* GETHANDLE gave this handle to the caller: the sequence still addresses it
     * but no longer releases it. */
    uint8_t                                   handle_taken;
    uint32_t                                  handle_origin;
    /* The caller chose this handle -- OPEN said what to open it with, or
     * PUTHANDLE lent one already open.  The executor never re-opens such a
     * handle: the caller knows what it is for, and re-opening would discard
     * the very reference the caller supplied. */
    uint8_t                                   handle_explicit;
    /* The saved OPEN slot.  SAVEHANDLE moves into it and RESTOREHANDLE moves
     * back, so exactly one slot refers to a handle at any moment and the
     * ownership bits travel with it. */
    struct chimera_vfs_open_handle           *saved_handle;
    unsigned int                              saved_handle_flags;
    uint8_t                                   saved_handle_borrowed;
    uint8_t                                   saved_handle_taken;
    uint32_t                                  saved_handle_origin;
    /* The current handle is the CALLER'S, seeded by PUTHANDLE, and must not be
     * released with the sequence or when the current object moves.  Cleared
     * the moment the executor opens one of its own. */
    uint8_t                                   handle_borrowed;

    /* The saved slot (SAVEFH/RESTOREFH): a file handle, no open handle. */
    uint8_t                                   saved_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                                  saved_fh_len;

    /* An OPEN that must resolve its name before opening runs in two steps, and
     * this says which one is next.  Cleared whenever the sequence advances, so
     * it can never be read as belonging to a different op. */
    uint8_t                                   open_resolved;

    /* An exclusive create collided and is being re-opened -- see
     * CHIMERA_VFS_COMPOUND_OPEN_EXCLUSIVE_RETRY.  Set so the retry cannot
     * itself retry.  Cleared whenever the sequence advances. */
    uint8_t                                   open_retried;

    /* A READ or WRITE that addresses the current object establishes that the
     * object is a regular file before it opens it for data -- so a protocol
     * answers for the type rather than for whatever errno a backend's data
     * open of a directory happens to produce, and so that open is never
     * attempted at all.  This says that step has been done.  Cleared whenever
     * the sequence advances. */
    uint8_t                                   io_typechecked;

    chimera_vfs_compound_callback_t           callback;
    void                                     *private_data;

    chimera_vfs_compound_gate_t               gate;
    void                                     *gate_private;

    /* Last: see the note on ->next.  This array is the whole reason a compound
    * is recycled rather than malloc'd per request -- it is by far the largest
    * thing in the struct, and only the ops a sequence actually used are ever
    * touched, so resetting is proportional to the sequence, not to the cap. */
    struct chimera_vfs_compound_op           *original_ops;
    uint32_t                                  original_num_ops;
    uint8_t                                   original_build_failed;
    enum chimera_vfs_error                    original_build_error;
    chimera_vfs_compound_attempt_reset_t      attempt_reset;
    void                                     *attempt_private;
    uint8_t                                   running;
    uint8_t                                   stepping;
    uint8_t                                   step_pending;
    uint8_t                                   finish_pending;
    uint8_t                                   preparing;
    uint8_t                                   completing;
    uint8_t                                   ownership_taken;
    /* Request lifetime, deliberately outside original_ops and attempt reset. */
    struct chimera_vfs_compound_coordination *coordinations;
    struct chimera_vfs_compound_coordination *coordination_pending;
    uint32_t                                  num_coordinations;
    /* Stable blocks: callbacks can append operations without invalidating
     * result pointers held by the executor or frontend. */
    struct chimera_vfs_compound_op           *ops[CHIMERA_VFS_COMPOUND_MAX_OPS];
};

/*
 * How many spent compounds a thread keeps.  Each one is large, so this is a
 * cap on retained memory rather than a hit-rate target: a thread's steady
 * state is a handful in flight at once, and a burst past the cap simply frees
 * the excess instead of holding it for a peak that has passed.
 */
#define CHIMERA_VFS_COMPOUND_FREE_MAX 64

static void chimera_vfs_compound_step(
    struct chimera_vfs_compound *compound);

SYMBOL_EXPORT struct chimera_vfs_compound *
chimera_vfs_compound_alloc(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred)
{
    struct chimera_vfs_compound *compound;

    compound = thread->free_compounds;

    if (compound) {
        LL_DELETE(thread->free_compounds, compound);
        thread->num_free_compounds--;
    } else {
        compound = calloc(1, sizeof(*compound));
    }

    compound->thread = thread;
    compound->cred   = cred;
    compound->default_cred = cred;
    compound->status = CHIMERA_VFS_OK;

    return compound;
} /* chimera_vfs_compound_alloc */

SYMBOL_EXPORT int
chimera_vfs_compound_add_group(
    struct chimera_vfs_compound                    *compound,
    const struct chimera_vfs_compound_group_config *config)
{
    uint32_t expected = compound->num_groups ?
        compound->groups[compound->num_groups - 1].config.first_op +
        compound->groups[compound->num_groups - 1].config.num_ops : 0;

    if (compound->original_ops || compound->running || !config ||
        compound->num_groups == CHIMERA_VFS_COMPOUND_MAX_OPS ||
        config->first_op != expected || !config->num_ops ||
        config->first_op > compound->num_ops ||
        config->num_ops > compound->num_ops - config->first_op ||
        config->dependency < -1 ||
        config->dependency >= (int32_t) compound->num_groups ||
        (config->dependency >= 0 &&
         (config->dependency_error == CHIMERA_VFS_OK ||
          config->dependency_error == CHIMERA_VFS_UNSET))) {
        compound->build_failed = 1;
        compound->build_error = CHIMERA_VFS_EINVAL;
        return -1;
    }
    if (!compound->groups) {
        compound->groups = calloc(CHIMERA_VFS_COMPOUND_MAX_OPS, sizeof(*compound->groups));
        if (!compound->groups) {
            compound->build_failed = 1;
            compound->build_error = CHIMERA_VFS_ENOSPC;
            return -1;
        }
    }
    struct chimera_vfs_compound_group *group = &compound->groups[compound->num_groups];
    group->config = *config;
    group->status = CHIMERA_VFS_UNSET;
    group->last_op = config->first_op + config->num_ops - 1;
    for (uint32_t i = config->first_op; i <= group->last_op; i++) {
        compound->ops[i]->group_next = i == group->last_op ? -1 : (int32_t) (i + 1);
    }
    return compound->num_groups++;
} /* chimera_vfs_compound_add_group */

SYMBOL_EXPORT enum chimera_vfs_error
chimera_vfs_compound_group_status(
    const struct chimera_vfs_compound *compound,
    uint32_t                           group)
{
    return group < compound->num_groups ? compound->groups[group].status : CHIMERA_VFS_UNSET;
} /* chimera_vfs_compound_group_status */

SYMBOL_EXPORT void *
chimera_vfs_compound_group_context(
    const struct chimera_vfs_compound *compound,
    uint32_t                           group)
{
    return group < compound->num_groups ? compound->groups[group].config.context : NULL;
} /* chimera_vfs_compound_group_context */

SYMBOL_EXPORT bool
chimera_vfs_compound_set_admission_cookie(struct chimera_vfs_compound *compound,
                                         const void *cookie)
{
    if (compound->running || compound->original_ops || !cookie) return false;
    compound->admission_cookie = cookie;
    return true;
}

SYMBOL_EXPORT bool
chimera_vfs_compound_set_cancel_scope(struct chimera_vfs_compound *compound,
                                     uint32_t start, uint32_t end)
{
    if (compound->running || compound->original_ops || start >= end || end >= compound->num_ops) return false;
    for (uint32_t i = 0; i < compound->num_ops; i++) {
        uint32_t prior_end = compound->ops[i]->cancel_scope_end;
        if (prior_end && start < prior_end && i <= end) return false;
    }
    compound->ops[start]->cancel_scope_end = end + 1;
    return true;
}

static bool
compound_cancel_stops(const struct chimera_vfs_compound *compound)
{
    return compound->canceled && !compound->cancel_defer_end;
}

SYMBOL_EXPORT bool
chimera_vfs_compound_cancel(struct chimera_vfs_compound *compound)
{
    if (!compound->running || compound->finishing || compound->finish_pending) {
        return false;
    }
    compound->canceled = 1;
    if (!compound->cancel_defer_end && compound->index < compound->num_ops) {
        /* Completion may synchronously destroy compound. */
        chimera_vfs_compound_range_cancel(compound, compound->index);
    }
    return true;
} /* chimera_vfs_compound_cancel */

SYMBOL_EXPORT bool
chimera_vfs_compound_is_canceled(const struct chimera_vfs_compound *compound)
{
    return compound->canceled;
}

SYMBOL_EXPORT void
chimera_vfs_compound_set_gate(
    struct chimera_vfs_compound *compound,
    chimera_vfs_compound_gate_t  gate,
    void                        *private_data)
{
    compound->gate         = gate;
    compound->gate_private = private_data;
} /* chimera_vfs_compound_set_gate */

SYMBOL_EXPORT void
chimera_vfs_compound_set_op_callbacks(
    struct chimera_vfs_compound       *compound,
    uint32_t                           index,
    chimera_vfs_compound_op_callback_t prepare,
    chimera_vfs_compound_op_callback_t complete,
    void                              *private_data)
{
    if (index >= compound->num_ops) {
        compound->build_failed = 1;
        return;
    }
    compound->ops[index]->prepare_private  = private_data;
    compound->ops[index]->prepare          = prepare;
    compound->ops[index]->complete         = complete;
    compound->ops[index]->callback_private = private_data;
} /* chimera_vfs_compound_set_op_callbacks */

SYMBOL_EXPORT void
chimera_vfs_compound_set_op_prepare(
    struct chimera_vfs_compound       *compound,
    uint32_t                           index,
    chimera_vfs_compound_op_callback_t prepare,
    void                              *private_data)
{
    if (index >= compound->num_ops) {
        compound->build_failed = 1;
        return;
    }
    compound->ops[index]->prepare         = prepare;
    compound->ops[index]->prepare_private = private_data;
} /* chimera_vfs_compound_set_op_prepare */

SYMBOL_EXPORT const uint8_t *
chimera_vfs_compound_current_fh(
    const struct chimera_vfs_compound *compound,
    uint32_t                          *length)
{
    *length = compound->fh_len;
    return compound->fh_len ? compound->fh : NULL;
} /* chimera_vfs_compound_current_fh */

SYMBOL_EXPORT const uint8_t *
chimera_vfs_compound_saved_fh(
    const struct chimera_vfs_compound *compound,
    uint32_t                          *length)
{
    *length = compound->saved_fh_len;
    return compound->saved_fh_len ? compound->saved_fh : NULL;
} /* chimera_vfs_compound_saved_fh */

SYMBOL_EXPORT void
chimera_vfs_compound_op_skip(
    struct chimera_vfs_compound *compound,
    uint32_t                     index)
{
    if (!compound->preparing || index != compound->index) {
        compound->build_failed = 1;
        return;
    }
    compound->ops[index]->skipped = 1;
} /* chimera_vfs_compound_op_skip */

SYMBOL_EXPORT struct chimera_vfs_compound_op *
chimera_vfs_compound_op_args(
    struct chimera_vfs_compound *compound,
    uint32_t                     index)
{
    if (index >= compound->num_ops ||
        (compound->running &&
         (!compound->preparing || index != compound->index))) {
        return NULL;
    }
    return compound->ops[index];
} /* chimera_vfs_compound_op_args */

SYMBOL_EXPORT void
chimera_vfs_compound_set_attempt_reset(
    struct chimera_vfs_compound         *compound,
    chimera_vfs_compound_attempt_reset_t reset,
    void                                *private_data)
{
    compound->attempt_reset   = reset;
    compound->attempt_private = private_data;
} /* chimera_vfs_compound_set_attempt_reset */

SYMBOL_EXPORT struct chimera_vfs_file_state *
chimera_vfs_compound_take_reservation(
    struct chimera_vfs_compound *compound,
    uint32_t                     index)
{
    struct chimera_vfs_compound_op *op;
    struct chimera_vfs_file_state  *file;

    if (compound->running || compound->finish_status != CHIMERA_VFS_OK ||
        index >= compound->num_ops) {
        return NULL;
    }
    op = compound->ops[index];
    if (!op->claim_held || op->access_owner) {
        return NULL;
    }
    file                      = op->claim_file;
    op->claim_file            = NULL;
    op->claim_held            = 0;
    compound->ownership_taken = 1;
    return file;
} /* chimera_vfs_compound_take_reservation */

SYMBOL_EXPORT struct chimera_vfs_claim_access_owner *
chimera_vfs_compound_take_access_owner(struct chimera_vfs_compound *compound,
                                      uint32_t index, struct chimera_vfs_file_state **file)
{
    if (!file || compound->running || compound->finish_status != CHIMERA_VFS_OK ||
        index >= compound->num_ops) return NULL;
    struct chimera_vfs_compound_op *op = compound->ops[index];
    if (!op->claim_held || !op->access_owner) return NULL;
    struct chimera_vfs_claim_access_owner *owner = op->access_owner;
    op->access_owner = NULL;
    *file = op->claim_file;
    op->claim_file = NULL;
    op->claim_held = 0;
    compound->ownership_taken = 1;
    return owner;
}

SYMBOL_EXPORT struct chimera_vfs_claim_owner *
chimera_vfs_compound_take_range_owner(struct chimera_vfs_compound *compound, uint32_t index)
{
    if (compound->running || compound->finish_status != CHIMERA_VFS_OK ||
        index >= compound->num_ops) return NULL;
    struct chimera_vfs_compound_op *op = compound->ops[index];
    if (!op->completed || op->status != CHIMERA_VFS_OK || !op->out_range_owner) return NULL;
    struct chimera_vfs_claim_owner *owner = op->out_range_owner;
    op->out_range_owner = NULL;
    compound->ownership_taken = 1;
    return owner;
}

static void
compound_release_range_owners(struct chimera_vfs_compound *compound)
{
    for (uint32_t i = 0; i < compound->num_ops; i++) {
        struct chimera_vfs_compound_op *op = compound->ops[i];
        if (op->out_range_owner) {
            chimera_vfs_claim_owner_retire(op->out_range_owner, NULL, NULL);
            chimera_vfs_claim_owner_put(op->out_range_owner);
            op->out_range_owner = NULL;
        }
    }
}

static void
chimera_vfs_compound_release_reservation(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_compound_op *op)
{
    if (op->claim_file) {
        if (op->access_owner) {
            chimera_vfs_claim_access_owner_retire(op->access_owner);
        } else if (op->claim_held) {
            chimera_vfs_claim_release(compound->thread->vfs->vfs_state,
                                      op->claim_file, op->claim);
        }
        chimera_vfs_state_put(compound->thread->vfs->vfs_state, op->claim_file);
        op->claim_file = NULL;
        op->claim_held = 0;
    }
    if (op->access_owner) {
        chimera_vfs_claim_access_owner_retire(op->access_owner);
        chimera_vfs_claim_access_owner_put(op->access_owner);
        op->access_owner = NULL;
    }
} /* chimera_vfs_compound_release_reservation */

/*
 * Release everything the sequence holds and return the compound to its
 * initial state, ready to be handed out again.  Only the ops the sequence
 * actually used are touched: a one-op FUSE request costs one op's worth of
 * clearing, not the whole CHIMERA_VFS_COMPOUND_MAX_OPS array.
 */
/*
 * Drop the current object's open handle, unless it is the caller's.
 *
 * Every site that lets go of compound->handle goes through here.  Having three
 * of them spell the rule out separately is how one of them came to be missing
 * the borrowed check, which released a handle PUTHANDLE had only borrowed.
 */
static void
chimera_vfs_compound_release_cursor(struct chimera_vfs_compound *compound)
{
    if (compound->handle &&
        !compound->handle_borrowed && !compound->handle_taken) {
        chimera_vfs_release(compound->thread, compound->handle);
    }

    compound->handle          = NULL;
    compound->handle_borrowed = 0;
    compound->handle_taken    = 0;
    compound->handle_origin   = 0;
    compound->handle_explicit = 0;
} /* chimera_vfs_compound_release_cursor */

/* The saved OPEN slot, on the same ownership rules as the current one. */
static void
chimera_vfs_compound_release_saved(struct chimera_vfs_compound *compound)
{
    if (compound->saved_handle &&
        !compound->saved_handle_borrowed && !compound->saved_handle_taken) {
        chimera_vfs_release(compound->thread, compound->saved_handle);
    }

    compound->saved_handle          = NULL;
    compound->saved_handle_borrowed = 0;
    compound->saved_handle_taken    = 0;
    compound->saved_handle_origin   = 0;
} /* chimera_vfs_compound_release_saved */

static void
compound_search_keys_release(struct chimera_vfs_compound_op *op)
{
    for (uint32_t i = 0; i < op->kv_num_entries; i++) free(op->kv_entries[i].key);
    free(op->kv_entries);
    free(op->kv_next_key);
    op->kv_entries = NULL;
    op->kv_next_key = NULL;
    op->kv_num_entries = op->kv_next_key_len = op->kv_result_bytes = 0;
    op->kv_more = false;
    op->kv_error = CHIMERA_VFS_OK;
}

static void
chimera_vfs_compound_reset(struct chimera_vfs_compound *compound)
{
    struct chimera_vfs_thread *thread = compound->thread;
    uint32_t                   i;

    chimera_vfs_abort_if(compound->coordination_pending,
                         "freeing compound with outstanding coordination");

    if (compound->claim_journal) {
        if (compound->claim_journal_published) {
            chimera_vfs_claim_journal_complete(compound->claim_journal);
        }
        chimera_vfs_claim_journal_free(compound->claim_journal);
    }
    if (compound->access_journal) {
        if (compound->access_journal_published) {
            chimera_vfs_claim_access_journal_complete(compound->access_journal);
        }
        chimera_vfs_claim_access_journal_free(compound->access_journal);
    }
    compound_release_range_owners(compound);
    /* Reservations may borrow a producer's handle as an actor anchor. Drain
     * all consumers before releasing any producer/cursor reference. */
    for (i = 0; i < compound->num_ops; i++) {
        chimera_vfs_compound_release_reservation(compound, compound->ops[i]);
    }
    chimera_vfs_compound_release_cursor(compound);
    chimera_vfs_compound_release_saved(compound);

    for (i = 0; i < compound->num_ops; i++) {
        chimera_vfs_lock_attempt_free(compound->ops[i]->lock_attempt);
        chimera_vfs_claim_range_attempt_free(compound->ops[i]->range_attempt);
        if (compound->finish_status == CHIMERA_VFS_OK && compound->ops[i]->close_handle) {
            chimera_vfs_release(thread, compound->ops[i]->close_handle);
        }
        if (compound->ops[i]->closed_output_handle) {
            chimera_vfs_release(thread, compound->ops[i]->closed_output_handle);
        }
        free(compound->ops[i]->attr.va_acl);
        free(compound->ops[i]->applied_acl);
        /* An open handle the caller did not take: see OPEN HANDLE OWNERSHIP.
         * Releasing here is what makes "take it if you want it" safe, rather
         * than making every one of the caller's error paths responsible. */
        if (compound->ops[i]->out_handle) {
            chimera_vfs_release(thread, compound->ops[i]->out_handle);
        }
        /* Data the caller did not take, for the same reason and on the same
         * terms as the handle above. */
        if (compound->ops[i]->niov) {
            evpl_iovecs_release(thread->evpl,
                                compound->ops[i]->iov,
                                compound->ops[i]->niov);
        }
        free(compound->ops[i]->target);
        free(compound->ops[i]->link_target);
        free(compound->ops[i]->path);
        free(compound->ops[i]->new_path);
        compound_search_keys_release(compound->ops[i]);
        free(compound->ops[i]->kv_key);
        free(compound->ops[i]->kv_value);
        free(compound->ops[i]->entries);
        free(compound->ops[i]->buffer);
        free(compound->ops[i]->journal_excluded);
        free(compound->ops[i]->journal_src_excluded);

        memset(compound->ops[i], 0, sizeof(*compound->ops[i]));
    }

    free(compound->groups);
    free(compound->original_ops);
    while (compound->coordinations) {
        struct chimera_vfs_compound_coordination *memo = compound->coordinations;
        compound->coordinations = memo->next;
        free(memo);
    }
    /* Keep only the small first block in the thread pool. Large S3 requests
     * must not permanently raise retained memory on every worker thread. */
    for (i = 16; i < CHIMERA_VFS_COMPOUND_MAX_OPS; i += 16) {
        free(compound->ops[i]);
        memset(&compound->ops[i], 0, 16 * sizeof(compound->ops[i]));
    }

    /* Everything ahead of ops[] in one go, so a field added to the struct
     * later is reset whether or not anyone remembers to name it here. */
    memset(compound, 0, offsetof(struct chimera_vfs_compound, ops));

    compound->thread = thread;
} /* chimera_vfs_compound_reset */

SYMBOL_EXPORT void
chimera_vfs_compound_free(struct chimera_vfs_compound *compound)
{
    struct chimera_vfs_thread *thread = compound->thread;

    chimera_vfs_compound_reset(compound);

    if (thread->num_free_compounds >= CHIMERA_VFS_COMPOUND_FREE_MAX) {
        for (uint32_t i = 0; i < CHIMERA_VFS_COMPOUND_MAX_OPS; i += 16) {
            free(compound->ops[i]);
        }
        free(compound);
        return;
    }

    LL_PREPEND(thread->free_compounds, compound);
    thread->num_free_compounds++;
} /* chimera_vfs_compound_free */

void
chimera_vfs_compound_thread_destroy(struct chimera_vfs_thread *thread)
{
    struct chimera_vfs_compound *compound;

    while (thread->free_compounds) {
        compound = thread->free_compounds;
        LL_DELETE(thread->free_compounds, compound);
        for (uint32_t i = 0; i < CHIMERA_VFS_COMPOUND_MAX_OPS; i += 16) {
            free(compound->ops[i]);
        }
        free(compound);
    }

    thread->num_free_compounds = 0;
} /* chimera_vfs_compound_thread_destroy */

SYMBOL_EXPORT void
chimera_vfs_compound_op_set_handle(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    struct chimera_vfs_open_handle *handle)
{
    if (index >= compound->num_ops) {
        return;
    }

    compound->ops[index]->in_handle = handle;
} /* chimera_vfs_compound_op_set_handle */

SYMBOL_EXPORT struct chimera_vfs_open_handle *
chimera_vfs_compound_take_handle(
    struct chimera_vfs_compound *compound,
    uint32_t                     index)
{
    struct chimera_vfs_open_handle *handle;

    if (index >= compound->num_ops) {
        return NULL;
    }

    if (compound->running || compound->finish_status != CHIMERA_VFS_OK) {
        return NULL;
    }
    handle                           = compound->ops[index]->out_handle;
    compound->ownership_taken       |= handle != NULL;
    compound->ops[index]->out_handle = NULL;

    return handle;
} /* chimera_vfs_compound_take_handle */

SYMBOL_EXPORT void
chimera_vfs_compound_take_iov(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    struct evpl_iovec          **iov,
    int                         *niov)
{
    *iov  = NULL;
    *niov = 0;

    if (index >= compound->num_ops) {
        return;
    }

    if (compound->running || compound->finish_status != CHIMERA_VFS_OK) {
        return;
    }
    compound->ownership_taken |= compound->ops[index]->niov != 0;
    *iov                       = compound->ops[index]->iov;
    *niov                      = compound->ops[index]->niov;

    /* Only the references move.  The array holding the descriptors stays the
     * compound's and is freed with it, so a caller that needs them to outlive
     * the sequence copies them somewhere that does. */
    compound->ops[index]->niov = 0;
} /* chimera_vfs_compound_take_iov */

/* Claim the next op slot, or -1 when the sequence is full. */
static struct chimera_vfs_compound_op *
chimera_vfs_compound_next_op(
    struct chimera_vfs_compound      *compound,
    enum chimera_vfs_compound_op_type type,
    int                              *index)
{
    struct chimera_vfs_compound_op *op;

    if (compound->running && compound->cancel_defer_end) {
        *index = -1;
        compound->build_failed = 1;
        compound->build_error = CHIMERA_VFS_ENOTSUP;
        return NULL;
    }
    if (compound->num_ops >= CHIMERA_VFS_COMPOUND_MAX_OPS ||
        (compound->num_groups && compound->original_ops &&
         (!compound->running || compound->finishing || !compound->group_active))) {
        *index = -1;
        compound->build_failed = 1;
        return NULL;
    }

    if (!compound->ops[compound->num_ops]) {
        struct chimera_vfs_compound_op *block = calloc(16, sizeof(*block));
        if (!block) {
            *index = -1;
            compound->build_failed = 1;
            return NULL;
        }
        for (uint32_t i = 0; i < 16; i++) {
            compound->ops[compound->num_ops + i] = &block[i];
        }
    }

    *index = (int) compound->num_ops++;
    op              = compound->ops[*index];
    op->type        = (uint8_t) type;
    op->handle_from = -1;
    op->status      = CHIMERA_VFS_UNSET;
    op->group_next  = -1;
    if (compound->running && compound->num_groups) {
        struct chimera_vfs_compound_group *group = &compound->groups[compound->group_index];
        compound->ops[group->last_op]->group_next = *index;
        group->last_op = *index;
    }

    return op;
} /* chimera_vfs_compound_next_op */

SYMBOL_EXPORT int
chimera_vfs_compound_add_range_owner(struct chimera_vfs_compound *compound,
    const struct chimera_claim_actor *actor, bool zero_point)
{
    if (compound->running || compound->original_ops) {
        compound->build_failed = 1;
        compound->build_error = CHIMERA_VFS_ENOTSUP;
        return -1;
    }
    int index;
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_next_op(compound,
        CHIMERA_VFS_COMPOUND_OP_RANGE_OWNER, &index);
    if (!op) return -1;
    if (actor) { op->io_owner = *actor; op->have_io_owner = true; }
    op->range_zero_point = zero_point;
    return index;
}

SYMBOL_EXPORT int
chimera_vfs_compound_add_range_batch(
    struct chimera_vfs_compound *compound,
    struct chimera_vfs_claim_owner *owner,
    const struct chimera_vfs_claim_exact_range *ranges,
    uint32_t count, bool unlock)
{
    if (compound->running || compound->original_ops || !count || !ranges ||
        count > CHIMERA_VFS_COMPOUND_MAX_OPS - compound->claim_journal_budget) {
        compound->build_failed = 1;
        compound->build_error = CHIMERA_VFS_ENOTSUP;
        return -1;
    }
    uint32_t budget = compound->claim_journal_budget + count;
    struct chimera_vfs_claim_journal *journal = chimera_vfs_claim_journal_alloc(
        CHIMERA_VFS_COMPOUND_MAX_OPS, budget);
    if (!journal) {
        compound->build_failed = 1;
        compound->build_error = CHIMERA_VFS_ENOSPC;
        return -1;
    }
    chimera_vfs_claim_journal_free(compound->claim_journal);
    compound->claim_journal = journal;
    compound->claim_journal_budget = budget;
    int index;
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_next_op(compound,
        CHIMERA_VFS_COMPOUND_OP_RANGE_BATCH, &index);
    if (!op) return -1;
    op->range_attempt = chimera_vfs_claim_range_attempt_alloc(compound->thread);
    if (!op->range_attempt) {
        compound->build_failed = 1;
        compound->build_error = CHIMERA_VFS_ENOSPC;
        return -1;
    }
    op->range_owner = owner;
    op->exact_ranges = ranges;
    op->num_exact_ranges = count;
    op->range_unlock = unlock;
    return index;
}

SYMBOL_EXPORT bool
chimera_vfs_compound_range_cancel(struct chimera_vfs_compound *compound, uint32_t index)
{
    return compound->running && !compound->finishing && !compound->finish_pending &&
           index == compound->index && index < compound->num_ops &&
           compound->ops[index]->type == CHIMERA_VFS_COMPOUND_OP_RANGE_BATCH &&
           chimera_vfs_claim_range_attempt_cancel(compound->ops[index]->range_attempt);
}

SYMBOL_EXPORT bool
chimera_vfs_compound_io_denied(
    struct chimera_vfs_compound *compound,
    const struct chimera_vfs_open_handle *handle,
    uint64_t offset, uint64_t length, bool write,
    const struct chimera_claim_actor *actor)
{
    struct chimera_vfs_state *state = compound->thread->vfs->vfs_state;
    if (!compound->claim_journal) {
        return chimera_vfs_claim_io_denied(state, handle->fh, handle->fh_len,
            handle->fh_hash, offset, length, write, actor);
    }
    struct chimera_vfs_file_state *file = chimera_vfs_state_get(state, handle->fh,
        handle->fh_len, handle->fh_hash, false);
    bool denied = chimera_vfs_claim_journal_io_denied(compound->claim_journal,
        file, offset, length, write, actor);
    if (file) chimera_vfs_state_put(state, file);
    return denied;
}

static int
chimera_vfs_compound_add_lock(
    struct chimera_vfs_compound           *compound,
    struct chimera_vfs_lock_domain        *domain,
    struct chimera_vfs_open_handle        *handle,
    const struct chimera_vfs_lock_request *request,
    enum chimera_vfs_compound_op_type      type)
{
    int                             index;
    struct chimera_vfs_compound_op *op;

    /* The dedicated-lock contract is validated at submit. A dynamic lock
     * would bypass it and mix projected or late-veto publication with ordinary
     * operations; reject before allocating any operation/attempt resources. */
    if (compound->running || compound->original_ops) {
        compound->build_failed = 1;
        compound->build_error = CHIMERA_VFS_ENOTSUP;
        return -1;
    }
    op = chimera_vfs_compound_next_op(compound, type, &index);
    if (!op) {
        return -1;
    }
    if (!request || !handle || !domain) {
        compound->build_failed = 1;
        return -1;
    }
    op->in_handle    = handle;
    op->lock_request = *request;
    op->lock_attempt = chimera_vfs_lock_attempt_alloc(compound->thread, domain, handle, request,
                                                      type == CHIMERA_VFS_COMPOUND_OP_LOCK_TEST, type ==
                                                      CHIMERA_VFS_COMPOUND_OP_LOCK_RELEASE_OWNER);
    if (!op->lock_attempt) {
        compound->build_failed = 1;
        compound->build_error  = CHIMERA_VFS_ENOSPC;
        return -1;
    }
    return index;
} /* chimera_vfs_compound_add_lock */

SYMBOL_EXPORT int
chimera_vfs_compound_add_lock_test(
    struct chimera_vfs_compound           *compound,
    struct chimera_vfs_lock_domain        *domain,
    struct chimera_vfs_open_handle        *handle,
    const struct chimera_vfs_lock_request *request)
{
    return chimera_vfs_compound_add_lock(compound, domain, handle, request, CHIMERA_VFS_COMPOUND_OP_LOCK_TEST);
} /* chimera_vfs_compound_add_lock_test */

SYMBOL_EXPORT int
chimera_vfs_compound_add_lock_change(
    struct chimera_vfs_compound           *compound,
    struct chimera_vfs_lock_domain        *domain,
    struct chimera_vfs_open_handle        *handle,
    const struct chimera_vfs_lock_request *request)
{
    return chimera_vfs_compound_add_lock(compound, domain, handle, request, CHIMERA_VFS_COMPOUND_OP_LOCK_CHANGE);
} /* chimera_vfs_compound_add_lock_change */

SYMBOL_EXPORT int
chimera_vfs_compound_add_lock_release_owner(
    struct chimera_vfs_compound      *compound,
    struct chimera_vfs_lock_domain   *domain,
    struct chimera_vfs_open_handle   *handle,
    const struct chimera_claim_owner *owner)
{
    struct chimera_vfs_lock_request request = { .owner = *owner,
                                                .type  = CHIMERA_VFS_LOCK_UNLOCK, .whence
                                                       =
                                                        SEEK_SET,
                                                .length = UINT64_MAX,             .project_backend
                                                        = true };

    return chimera_vfs_compound_add_lock(compound, domain, handle, &request,
                                         CHIMERA_VFS_COMPOUND_OP_LOCK_RELEASE_OWNER);
} /* chimera_vfs_compound_add_lock_release_owner */

SYMBOL_EXPORT bool
chimera_vfs_compound_lock_cancel(
    struct chimera_vfs_compound *compound,
    uint32_t                     index)
{
    return index < compound->num_ops && compound->ops[index]->lock_attempt &&
           chimera_vfs_lock_attempt_cancel(compound->ops[index]->lock_attempt);
} /* chimera_vfs_compound_lock_cancel */


SYMBOL_EXPORT int
chimera_vfs_compound_add_putfh(
    struct chimera_vfs_compound *compound,
    const void                  *fh,
    int                          fhlen)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (fhlen <= 0 || fhlen > CHIMERA_VFS_FH_SIZE) {
        compound->build_failed = 1;
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_PUTFH, &index);

    if (!op) {
        return -1;
    }

    memcpy(op->arg_fh, fh, fhlen);
    op->arg_fh_len = (uint32_t) fhlen;

    return index;
} /* chimera_vfs_compound_add_putfh */

SYMBOL_EXPORT int
chimera_vfs_compound_add_lookup(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen,
    uint64_t                     attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (namelen <= 0 || namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
        if (namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
            compound->build_error = CHIMERA_VFS_ENAMETOOLONG;
        }
        compound->build_failed = 1;
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_LOOKUP, &index);

    if (!op) {
        return -1;
    }

    memcpy(op->name, name, namelen);
    op->name[namelen] = '\0';
    op->name_len      = (uint32_t) namelen;
    op->attr_mask     = attr_mask;

    return index;
} /* chimera_vfs_compound_add_lookup */

SYMBOL_EXPORT int
chimera_vfs_compound_add_getattr(
    struct chimera_vfs_compound *compound,
    uint64_t                     attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_GETATTR, &index);

    if (!op) {
        return -1;
    }

    op->attr_mask = attr_mask;

    return index;
} /* chimera_vfs_compound_add_getattr */

SYMBOL_EXPORT int
chimera_vfs_compound_add_access(
    struct chimera_vfs_compound *compound,
    uint32_t                     requested)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_ACCESS, &index);

    if (!op) {
        return -1;
    }

    op->requested = requested;
    /* The ACL as well as the mode: chimera_vfs_access_check evaluates a native
     * ACL when the object has one and only falls back to the mode bits when it
     * does not, so fetching stat alone would silently answer every ACL-bearing
     * object from its mode. */
    op->attr_mask = CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL;

    return index;
} /* chimera_vfs_compound_add_access */

SYMBOL_EXPORT int
chimera_vfs_compound_add_getfh(struct chimera_vfs_compound *compound)
{
    int index;

    chimera_vfs_compound_next_op(compound,
                                 CHIMERA_VFS_COMPOUND_OP_GETFH, &index);

    return index;
} /* chimera_vfs_compound_add_getfh */

SYMBOL_EXPORT int
chimera_vfs_compound_add_readlink(struct chimera_vfs_compound *compound)
{
    int index;

    chimera_vfs_compound_next_op(compound,
                                 CHIMERA_VFS_COMPOUND_OP_READLINK, &index);

    return index;
} /* chimera_vfs_compound_add_readlink */

SYMBOL_EXPORT int
chimera_vfs_compound_add_savefh(struct chimera_vfs_compound *compound)
{
    int index;

    chimera_vfs_compound_next_op(compound,
                                 CHIMERA_VFS_COMPOUND_OP_SAVEFH, &index);

    return index;
} /* chimera_vfs_compound_add_savefh */

SYMBOL_EXPORT int
chimera_vfs_compound_add_restorefh(struct chimera_vfs_compound *compound)
{
    int index;

    chimera_vfs_compound_next_op(compound,
                                 CHIMERA_VFS_COMPOUND_OP_RESTOREFH, &index);

    return index;
} /* chimera_vfs_compound_add_restorefh */

SYMBOL_EXPORT int
chimera_vfs_compound_add_lookupp(
    struct chimera_vfs_compound *compound,
    uint64_t                     attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_LOOKUPP, &index);

    if (!op) {
        return -1;
    }

    op->attr_mask = attr_mask;

    return index;
} /* chimera_vfs_compound_add_lookupp */

SYMBOL_EXPORT int
chimera_vfs_compound_add_commit(
    struct chimera_vfs_compound *compound,
    uint64_t                     offset,
    uint64_t                     count,
    uint64_t                     pre_attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_COMMIT, &index);

    if (!op) {
        return -1;
    }

    op->offset    = offset;
    op->count     = count;
    op->attr_mask = pre_attr_mask;

    return index;
} /* chimera_vfs_compound_add_commit */

SYMBOL_EXPORT int
chimera_vfs_compound_add_allocate(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint64_t                        length,
    uint32_t                        flags,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_ALLOCATE, &index);

    if (!op) {
        return -1;
    }

    op->in_handle      = handle;
    op->offset         = offset;
    op->length         = length;
    op->allocate_flags = flags;
    op->attr_mask      = pre_attr_mask;
    op->post_attr_mask = post_attr_mask;

    return index;
} /* chimera_vfs_compound_add_allocate */

SYMBOL_EXPORT int
chimera_vfs_compound_add_seek(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        what)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_SEEK, &index);

    if (!op) {
        return -1;
    }

    op->in_handle = handle;
    op->offset    = offset;
    op->seek_what = what;

    return index;
} /* chimera_vfs_compound_add_seek */

static int
chimera_vfs_compound_add_range(
    struct chimera_vfs_compound    *compound,
    uint8_t                         type,
    struct chimera_vfs_open_handle *src_handle,
    uint64_t                        src_offset,
    struct chimera_vfs_open_handle *dst_handle,
    uint64_t                        dst_offset,
    uint64_t                        length)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    /* Endpoints may be bound from earlier results by a prepare callback.
     * Execution validates that both handles exist before issuing the range. */

    op = chimera_vfs_compound_next_op(compound, type, &index);

    if (!op) {
        return -1;
    }

    op->src_handle = src_handle;
    op->src_offset = src_offset;
    op->in_handle  = dst_handle;
    op->offset     = dst_offset;
    op->length     = length;

    return index;
} /* chimera_vfs_compound_add_range */

SYMBOL_EXPORT int
chimera_vfs_compound_add_copy_range(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *src_handle,
    uint64_t                        src_offset,
    struct chimera_vfs_open_handle *dst_handle,
    uint64_t                        dst_offset,
    uint64_t                        length,
    uint32_t                        flags,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask)
{
    int index = chimera_vfs_compound_add_range(
        compound, CHIMERA_VFS_COMPOUND_OP_COPY_RANGE,
        src_handle, src_offset, dst_handle, dst_offset, length);

    if (index >= 0) {
        compound->ops[index]->copy_flags     = flags;
        compound->ops[index]->attr_mask      = pre_attr_mask;
        compound->ops[index]->post_attr_mask = post_attr_mask;
    }

    return index;
} /* chimera_vfs_compound_add_copy_range */

SYMBOL_EXPORT int
chimera_vfs_compound_add_clone_range(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *src_handle,
    uint64_t                        src_offset,
    struct chimera_vfs_open_handle *dst_handle,
    uint64_t                        dst_offset,
    uint64_t                        length,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask)
{
    int index = chimera_vfs_compound_add_range(
        compound, CHIMERA_VFS_COMPOUND_OP_CLONE_RANGE,
        src_handle, src_offset, dst_handle, dst_offset, length);

    if (index >= 0) {
        compound->ops[index]->attr_mask      = pre_attr_mask;
        compound->ops[index]->post_attr_mask = post_attr_mask;
    }

    return index;
} /* chimera_vfs_compound_add_clone_range */

SYMBOL_EXPORT int
chimera_vfs_compound_add_move_range(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *src_handle,
    uint64_t                        src_offset,
    struct chimera_vfs_open_handle *dst_handle,
    uint64_t                        dst_offset,
    uint64_t                        length,
    uint64_t                        src_post_attr_mask,
    uint64_t                        dst_pre_attr_mask,
    uint64_t                        dst_post_attr_mask)
{
    int index = chimera_vfs_compound_add_range(
        compound, CHIMERA_VFS_COMPOUND_OP_MOVE_RANGE,
        src_handle, src_offset, dst_handle, dst_offset, length);

    if (index >= 0) {
        /* The source's own post-change attributes ride in `requested`, which
         * only ACCESS uses and which no range op has any other need for. */
        compound->ops[index]->requested      = (uint32_t) src_post_attr_mask;
        compound->ops[index]->attr_mask      = dst_pre_attr_mask;
        compound->ops[index]->post_attr_mask = dst_post_attr_mask;
    }

    return index;
} /* chimera_vfs_compound_add_move_range */

SYMBOL_EXPORT int
chimera_vfs_compound_add_write_same(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        block_size,
    uint64_t                        block_count,
    const void                     *pattern,
    uint32_t                        pattern_len,
    uint32_t                        reloff_pattern,
    uint32_t                        sync,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_WRITE_SAME,
                                      &index);

    if (!op) {
        return -1;
    }

    op->in_handle      = handle;
    op->offset         = offset;
    op->block_size     = block_size;
    op->block_count    = block_count;
    op->pattern        = pattern;
    op->pattern_len    = pattern_len;
    op->reloff_pattern = reloff_pattern;
    op->sync           = sync;
    op->attr_mask      = pre_attr_mask;
    op->post_attr_mask = post_attr_mask;

    return index;
} /* chimera_vfs_compound_add_write_same */

SYMBOL_EXPORT int
chimera_vfs_compound_add_read_plus(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint64_t                        length)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_READ_PLUS,
                                      &index);

    if (!op) {
        return -1;
    }

    op->in_handle = handle;
    op->offset    = offset;
    op->length    = length;

    return index;
} /* chimera_vfs_compound_add_read_plus */

/* Stash a copy of `path` on the op.  Paths are heap-allocated rather than
 * inline because CHIMERA_VFS_PATH_MAX is far too large to sit in every slot of
 * every sequence; the compound owns the copy and frees it on reset. */
static int
chimera_vfs_compound_set_path(
    char      **dst,
    uint32_t   *dst_len,
    const char *path,
    int         pathlen)
{
    /* No length rule here: what is too long is the VFS's answer to give
     * (ENAMETOOLONG, for the whole path or any one component), and refusing
     * to build the op would turn that into a sequence that quietly does
     * less. */
    if (pathlen < 0) {
        return -1;
    }

    *dst = malloc(pathlen + 1);

    if (!*dst) {
        return -1;
    }

    memcpy(*dst, path, pathlen);
    (*dst)[pathlen] = '\0';
    *dst_len        = (uint32_t) pathlen;

    return 0;
} /* chimera_vfs_compound_set_path */

/* The cursor ops that carry no arguments all build the same way. */
static int
chimera_vfs_compound_add_simple(
    struct chimera_vfs_compound      *compound,
    enum chimera_vfs_compound_op_type type)
{
    int index;

    return chimera_vfs_compound_next_op(compound, type, &index) ? index : -1;
} /* chimera_vfs_compound_add_simple */

SYMBOL_EXPORT int
chimera_vfs_compound_add_putroot(struct chimera_vfs_compound *compound)
{
    return chimera_vfs_compound_add_simple(
        compound, CHIMERA_VFS_COMPOUND_OP_PUTROOT);
} /* chimera_vfs_compound_add_putroot */

SYMBOL_EXPORT int
chimera_vfs_compound_add_gethandle(struct chimera_vfs_compound *compound)
{
    return chimera_vfs_compound_add_simple(
        compound, CHIMERA_VFS_COMPOUND_OP_GETHANDLE);
} /* chimera_vfs_compound_add_gethandle */

SYMBOL_EXPORT int
chimera_vfs_compound_add_checkpoint(struct chimera_vfs_compound *compound)
{
    return chimera_vfs_compound_add_simple(compound, CHIMERA_VFS_COMPOUND_OP_CHECKPOINT);
} /* chimera_vfs_compound_add_checkpoint */

SYMBOL_EXPORT int
chimera_vfs_compound_add_coordinate(
    struct chimera_vfs_compound      *compound,
    chimera_vfs_compound_coordinate_t start,
    void                             *private_data)
{
    int                             index;
    struct chimera_vfs_compound_op *op;

    if (!start || (compound->running &&
                   (!compound->num_groups || !compound->completing || compound->preparing)) ||
        (!compound->running && compound->original_ops)) {
        compound->build_failed = 1;
        return -1;
    }
    op = chimera_vfs_compound_next_op(compound, CHIMERA_VFS_COMPOUND_OP_COORDINATE, &index);
    if (!op) {
        return -1;
    }
    op->coordinate         = start;
    op->coordinate_private = private_data;
    return index;
} /* chimera_vfs_compound_add_coordinate */

SYMBOL_EXPORT int
chimera_vfs_compound_add_close(struct chimera_vfs_compound *compound)
{
    return chimera_vfs_compound_add_simple(
        compound, CHIMERA_VFS_COMPOUND_OP_CLOSE);
} /* chimera_vfs_compound_add_close */

SYMBOL_EXPORT int
chimera_vfs_compound_add_savehandle(struct chimera_vfs_compound *compound)
{
    return chimera_vfs_compound_add_simple(
        compound, CHIMERA_VFS_COMPOUND_OP_SAVEHANDLE);
} /* chimera_vfs_compound_add_savehandle */

SYMBOL_EXPORT int
chimera_vfs_compound_add_restorehandle(struct chimera_vfs_compound *compound)
{
    return chimera_vfs_compound_add_simple(
        compound, CHIMERA_VFS_COMPOUND_OP_RESTOREHANDLE);
} /* chimera_vfs_compound_add_restorehandle */

SYMBOL_EXPORT int
chimera_vfs_compound_add_open_current(
    struct chimera_vfs_compound *compound,
    unsigned int                 flags,
    uint64_t                     attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_OPEN_CURRENT,
                                      &index);

    if (!op) {
        return -1;
    }

    op->open_flags = flags;
    op->attr_mask  = attr_mask;

    return index;
} /* chimera_vfs_compound_add_open_current */

SYMBOL_EXPORT int
chimera_vfs_compound_add_puthandle(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *handle,
    unsigned int                    open_flags)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (!handle) {
        compound->build_failed = 1;
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_PUTHANDLE,
                                      &index);

    if (!op) {
        return -1;
    }

    op->in_handle  = handle;
    op->open_flags = open_flags;

    return index;
} /* chimera_vfs_compound_add_puthandle */

SYMBOL_EXPORT int
chimera_vfs_compound_add_puthandle_from(
    struct chimera_vfs_compound *compound,
    int32_t                      from,
    unsigned int                 open_flags)
{
    int index;
    struct chimera_vfs_compound_op *op;

    if (from < -1 || (from >= 0 && (uint32_t) from >= compound->num_ops)) {
        compound->build_failed = 1;
        compound->build_error = CHIMERA_VFS_EINVAL;
        return -1;
    }
    op = chimera_vfs_compound_next_op(compound, CHIMERA_VFS_COMPOUND_OP_PUTHANDLE, &index);
    if (!op) {
        return -1;
    }
    op->handle_from = from;
    op->open_flags = open_flags;
    return index;
} /* chimera_vfs_compound_add_puthandle_from */

SYMBOL_EXPORT int
chimera_vfs_compound_add_lookup_path(
    struct chimera_vfs_compound *compound,
    const char                  *path,
    int                          pathlen,
    uint64_t                     attr_mask,
    uint32_t                     flags)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_LOOKUP_PATH,
                                      &index);

    if (!op) {
        return -1;
    }

    if (chimera_vfs_compound_set_path(&op->path, &op->path_len,
                                      path, pathlen) != 0) {
        compound->build_failed = 1;
        return -1;
    }

    op->attr_mask = attr_mask;
    op->open_opts = flags;

    return index;
} /* chimera_vfs_compound_add_lookup_path */

SYMBOL_EXPORT int
chimera_vfs_compound_add_open_path(
    struct chimera_vfs_compound    *compound,
    const char                     *path,
    int                             pathlen,
    unsigned int                    flags,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_OPEN_PATH,
                                      &index);

    if (!op) {
        return -1;
    }

    if (chimera_vfs_compound_set_path(&op->path, &op->path_len,
                                      path, pathlen) != 0) {
        compound->build_failed = 1;
        return -1;
    }

    op->open_flags = flags;
    op->attr_mask  = attr_mask;

    if (set_attr) {
        op->set_attr = *set_attr;
    }

    return index;
} /* chimera_vfs_compound_add_open_path */

SYMBOL_EXPORT int
chimera_vfs_compound_add_open_at(
    struct chimera_vfs_compound    *compound,
    const char                     *path,
    int                             pathlen,
    unsigned int                    flags,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask)
{
    int index = chimera_vfs_compound_add_open_path(compound, path, pathlen,
                                                   flags, set_attr, attr_mask);

    if (index >= 0) {
        /* OPEN_PATH walks from an FH. OPEN-at uses the current open directory,
         * preserving path-only backend and retained-handle semantics. */
        struct chimera_vfs_compound_op *op = compound->ops[index];
        op->type     = CHIMERA_VFS_COMPOUND_OP_OPEN;
        op->name_len = op->path_len;
        if (op->path_len <= CHIMERA_VFS_COMPOUND_NAME_MAX) {
            memcpy(op->name, op->path, op->path_len + 1);
        }
    }
    return index;
} /* chimera_vfs_compound_add_open_at */

SYMBOL_EXPORT int
chimera_vfs_compound_add_create_path(
    struct chimera_vfs_compound    *compound,
    uint8_t                         create_type,
    const char                     *path,
    int                             pathlen,
    const char                     *target,
    int                             targetlen,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    /* Before the slot is claimed: a rejection that leaves a half-built op
     * behind is worse than no op at all -- a CREATE_PATH whose create_type was
     * never assigned is a DIRECTORY create. */
    if (create_type == CHIMERA_VFS_COMPOUND_CREATE_SYMLINK &&
        (!target || targetlen <= 0)) {
        compound->build_failed = 1;
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_CREATE_PATH,
                                      &index);

    if (!op) {
        return -1;
    }

    if (chimera_vfs_compound_set_path(&op->path, &op->path_len,
                                      path, pathlen) != 0) {
        compound->build_failed = 1;
        return -1;
    }

    if (create_type == CHIMERA_VFS_COMPOUND_CREATE_SYMLINK) {

        op->link_target = malloc(targetlen + 1);

        if (!op->link_target) {
            compound->build_failed = 1;
            return -1;
        }

        memcpy(op->link_target, target, targetlen);
        op->link_target[targetlen] = '\0';
        op->link_target_len        = (uint32_t) targetlen;
    }

    op->create_type = create_type;
    op->attr_mask   = attr_mask;

    if (set_attr) {
        op->set_attr = *set_attr;
    }

    return index;
} /* chimera_vfs_compound_add_create_path */

SYMBOL_EXPORT int
chimera_vfs_compound_add_create_tree(
    struct chimera_vfs_compound    *compound,
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask)
{
    return chimera_vfs_compound_add_create_path(
        compound, CHIMERA_VFS_COMPOUND_CREATE_DIR_TREE,
        path, pathlen, NULL, 0, set_attr, attr_mask | CHIMERA_VFS_ATTR_FH);
} /* chimera_vfs_compound_add_create_tree */

SYMBOL_EXPORT int
chimera_vfs_compound_add_create_unlinked(
    struct chimera_vfs_compound    *compound,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_CREATE_UNLINKED,
                                      &index);
    if (!op) {
        return -1;
    }
    op->attr_mask  = attr_mask | CHIMERA_VFS_ATTR_FH;
    op->open_flags = CHIMERA_VFS_OPEN_CREATE;
    if (set_attr) {
        op->set_attr = *set_attr;
    }
    return index;
} /* chimera_vfs_compound_add_create_unlinked */

SYMBOL_EXPORT int
chimera_vfs_compound_add_remove_path(
    struct chimera_vfs_compound *compound,
    const char                  *path,
    int                          pathlen,
    unsigned int                 flags)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_REMOVE_PATH,
                                      &index);

    if (!op) {
        return -1;
    }

    if (chimera_vfs_compound_set_path(&op->path, &op->path_len,
                                      path, pathlen) != 0) {
        compound->build_failed = 1;
        return -1;
    }

    op->remove_flags = flags;

    return index;
} /* chimera_vfs_compound_add_remove_path */

SYMBOL_EXPORT int
chimera_vfs_compound_add_remove_at_path(
    struct chimera_vfs_compound *compound,
    const char                  *directory,
    int                          directory_len,
    const char                  *name,
    int                          namelen,
    unsigned int                 flags)
{
    int                             index;
    struct chimera_vfs_compound_op *op;

    if (namelen < 0 || namelen > CHIMERA_VFS_COMPOUND_NAME_MAX ||
        (namelen && !name) || directory_len < 0 ||
        (directory_len && !directory)) {
        if (namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
            compound->build_error = CHIMERA_VFS_ENAMETOOLONG;
        }
        compound->build_failed = 1;
        return -1;
    }
    index = chimera_vfs_compound_add_remove_path(compound,
                                                 directory ? directory : "", directory_len, flags);
    if (index < 0) {
        return -1;
    }
    op            = compound->ops[index];
    op->open_opts = 1;
    if (namelen) {
        memcpy(op->name, name, namelen);
    }
    op->name[namelen] = '\0';
    op->name_len      = namelen;
    return index;
} /* chimera_vfs_compound_add_remove_at_path */

SYMBOL_EXPORT int
chimera_vfs_compound_add_rename_path(
    struct chimera_vfs_compound *compound,
    const char                  *old_path,
    int                          old_pathlen,
    const char                  *new_path,
    int                          new_pathlen)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_RENAME_PATH,
                                      &index);

    if (!op) {
        return -1;
    }

    if (chimera_vfs_compound_set_path(&op->path, &op->path_len,
                                      old_path, old_pathlen) != 0 ||
        chimera_vfs_compound_set_path(&op->new_path, &op->new_path_len,
                                      new_path, new_pathlen) != 0) {
        compound->build_failed = 1;
        return -1;
    }

    return index;
} /* chimera_vfs_compound_add_rename_path */

SYMBOL_EXPORT int
chimera_vfs_compound_add_link_path(
    struct chimera_vfs_compound *compound,
    const char                  *old_path,
    int                          old_pathlen,
    unsigned int                 source_lookup_flags,
    const char                  *new_path,
    int                          new_pathlen,
    uint64_t                     attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_LINK_PATH,
                                      &index);

    if (!op) {
        return -1;
    }

    if (chimera_vfs_compound_set_path(&op->path, &op->path_len,
                                      old_path, old_pathlen) != 0 ||
        chimera_vfs_compound_set_path(&op->new_path, &op->new_path_len,
                                      new_path, new_pathlen) != 0) {
        compound->build_failed = 1;
        return -1;
    }

    op->open_flags = source_lookup_flags;
    op->attr_mask  = attr_mask;

    return index;
} /* chimera_vfs_compound_add_link_path */

SYMBOL_EXPORT void
chimera_vfs_compound_op_use_handle(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    uint32_t                     from)
{
    /* Construction references point backwards. A dynamic grouped producer
     * can have a higher physical index: permit prepare to bind it only after
     * its successful execution, independently of numeric ordering. */
    if (index >= compound->num_ops || from >= compound->num_ops ||
        (from >= index &&
         (!compound->preparing || index != compound->index ||
          !compound->ops[from]->completed || compound->ops[from]->status != CHIMERA_VFS_OK ||
          !compound->ops[from]->out_handle))) {
        return;
    }

    compound->ops[index]->handle_from = (int) from;
} /* chimera_vfs_compound_op_use_handle */

SYMBOL_EXPORT int
chimera_vfs_compound_add_readdir(
    struct chimera_vfs_compound *compound,
    uint64_t                     cookie,
    uint64_t                     verifier,
    uint32_t                     dircount,
    uint32_t                     maxcount,
    uint32_t                     max_entries,
    uint64_t                     attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (max_entries > CHIMERA_VFS_COMPOUND_READDIR_MAX_ENTRIES) {
        compound->build_failed = 1;
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_READDIR, &index);

    if (!op) {
        return -1;
    }

    op->cookie      = cookie;
    op->verifier    = verifier;
    op->dircount    = dircount;
    op->maxcount    = maxcount;
    op->max_entries = max_entries;
    op->attr_mask   = attr_mask;

    return index;
} /* chimera_vfs_compound_add_readdir */

SYMBOL_EXPORT int
chimera_vfs_compound_add_readdir_stream(
    struct chimera_vfs_compound          *compound,
    uint64_t                              cookie,
    uint64_t                              verifier,
    uint64_t                              attr_mask,
    chimera_vfs_compound_readdir_reset_t  reset,
    chimera_vfs_compound_readdir_append_t append,
    void                                 *private_data)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (!reset || !append) {
        compound->build_failed = 1;
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_READDIR, &index);

    if (!op) {
        return -1;
    }

    op->cookie          = cookie;
    op->verifier        = verifier;
    op->attr_mask       = attr_mask;
    op->readdir_reset   = reset;
    op->readdir_append  = append;
    op->readdir_private = private_data;

    return index;
} /* chimera_vfs_compound_add_readdir_stream */

SYMBOL_EXPORT int
chimera_vfs_compound_add_getxattr(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen,
    uint32_t                     value_max)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (namelen <= 0 || namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
        if (namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
            compound->build_error = CHIMERA_VFS_ENAMETOOLONG;
        }
        compound->build_failed = 1;
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_GETXATTR, &index);

    if (!op) {
        return -1;
    }

    memcpy(op->name, name, namelen);
    op->name[namelen] = '\0';
    op->name_len      = (uint32_t) namelen;
    op->buffer_max    = value_max;

    return index;
} /* chimera_vfs_compound_add_getxattr */

static int
chimera_vfs_compound_add_key(struct chimera_vfs_compound *compound,
    enum chimera_vfs_compound_op_type type, const void *key, uint32_t key_len,
    const void *value, uint32_t value_len)
{
    if ((key_len && !key) || (value_len && !value)) {
        compound->build_failed = 1;
        compound->build_error = CHIMERA_VFS_EINVAL;
        return -1;
    }
    if ((uint64_t) key_len + value_len > CHIMERA_VFS_PLUGIN_DATA_SIZE) {
        compound->build_failed = 1;
        compound->build_error = CHIMERA_VFS_ERANGE;
        return -1;
    }
    int index;
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_next_op(compound, type, &index);
    if (!op) return -1;
    op->kv_key = malloc(key_len ? key_len : 1);
    op->kv_value = malloc(value_len ? value_len : 1);
    if (!op->kv_key || !op->kv_value) {
        compound->build_failed = 1;
        compound->build_error = CHIMERA_VFS_ENOSPC;
        return -1;
    }
    if (key_len) memcpy(op->kv_key, key, key_len);
    if (value_len) memcpy(op->kv_value, value, value_len);
    op->kv_key_len = key_len;
    op->kv_value_len = value_len;
    return index;
}

SYMBOL_EXPORT int
chimera_vfs_compound_add_put_key_at(struct chimera_vfs_compound *compound,
    const void *key, uint32_t key_len, const void *value, uint32_t value_len)
{
    return chimera_vfs_compound_add_key(compound, CHIMERA_VFS_COMPOUND_OP_PUT_KEY_AT,
        key, key_len, value, value_len);
}

SYMBOL_EXPORT int
chimera_vfs_compound_add_delete_key_at(struct chimera_vfs_compound *compound,
    const void *key, uint32_t key_len)
{
    return chimera_vfs_compound_add_key(compound, CHIMERA_VFS_COMPOUND_OP_DELETE_KEY_AT,
        key, key_len, NULL, 0);
}

SYMBOL_EXPORT int
chimera_vfs_compound_add_search_keys_at(struct chimera_vfs_compound *compound,
    const void *start_key, uint32_t start_len, const void *end_key, uint32_t end_len,
    uint32_t flags, uint32_t max_entries, uint32_t max_bytes)
{
    if (!max_entries || max_entries > 65536 || !max_bytes || max_bytes > 16 * 1024 * 1024 ||
        start_len > 4096 || end_len > 4096 ||
        (uint64_t) start_len + end_len > CHIMERA_VFS_PLUGIN_DATA_SIZE - 128 ||
        (flags & ~CHIMERA_VFS_SEARCH_KEYS_END_EXCLUSIVE)) {
        compound->build_failed = 1;
        compound->build_error = CHIMERA_VFS_ERANGE;
        return -1;
    }
    int index = chimera_vfs_compound_add_key(compound, CHIMERA_VFS_COMPOUND_OP_SEARCH_KEYS_AT,
        start_key, start_len, end_key, end_len);
    if (index < 0) return -1;
    struct chimera_vfs_compound_op *op = compound->ops[index];
    op->kv_flags = flags;
    op->kv_max_entries = max_entries;
    op->kv_max_bytes = max_bytes;
    return index;
}

SYMBOL_EXPORT int
chimera_vfs_compound_add_setxattr(
    struct chimera_vfs_compound *compound,
    uint32_t                     option,
    const char                  *name,
    int                          namelen,
    const void                  *value,
    uint32_t                     value_len)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (namelen <= 0 || namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
        if (namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
            compound->build_error = CHIMERA_VFS_ENAMETOOLONG;
        }
        compound->build_failed = 1;
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_SETXATTR, &index);

    if (!op) {
        return -1;
    }

    memcpy(op->name, name, namelen);
    op->name[namelen]   = '\0';
    op->name_len        = (uint32_t) namelen;
    op->xattr_option    = option;
    op->xattr_value     = value;
    op->xattr_value_len = value_len;

    return index;
} /* chimera_vfs_compound_add_setxattr */

SYMBOL_EXPORT int
chimera_vfs_compound_add_remove_paths(
    struct chimera_vfs_compound *compound,
    const char *const           *paths,
    uint32_t                     num_paths,
    unsigned int                 flags,
    int                          ignore_errors)
{
    int                             index;
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_next_op(
        compound, CHIMERA_VFS_COMPOUND_OP_REMOVE_PATHS, &index);

    if (!op) {
        return -1;
    }
    op->remove_paths         = paths;
    op->remove_num_paths     = num_paths;
    op->remove_flags         = flags;
    op->remove_ignore_errors = !!ignore_errors;
    return index;
} /* chimera_vfs_compound_add_remove_paths */

SYMBOL_EXPORT int
chimera_vfs_compound_add_find(
    struct chimera_vfs_compound         *compound,
    uint64_t                             attr_mask,
    chimera_vfs_compound_readdir_reset_t reset,
    chimera_vfs_compound_find_entry_t    filter,
    chimera_vfs_compound_find_entry_t    append,
    void                                *private_data)
{
    int                             index;
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_next_op(
        compound, CHIMERA_VFS_COMPOUND_OP_FIND, &index);

    if (!op) {
        return -1;
    }
    op->attr_mask    = attr_mask;
    op->find_reset   = reset;
    op->find_filter  = filter;
    op->find_append  = append;
    op->find_private = private_data;
    return index;
} /* chimera_vfs_compound_add_find */

static int
compound_add_stream_name(struct chimera_vfs_compound *compound,
                         enum chimera_vfs_compound_op_type type,
                         const char *name, int namelen)
{
    int index;
    if (!name || namelen <= 0 || namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
        compound->build_failed = 1;
        compound->build_error = namelen > CHIMERA_VFS_COMPOUND_NAME_MAX ?
                                CHIMERA_VFS_ENAMETOOLONG : CHIMERA_VFS_EINVAL;
        return -1;
    }
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_next_op(compound, type, &index);
    if (!op) return -1;
    memcpy(op->name, name, namelen);
    op->name[namelen] = 0;
    op->name_len = namelen;
    return index;
}

SYMBOL_EXPORT int
chimera_vfs_compound_add_open_stream(struct chimera_vfs_compound *compound,
    const char *name, int namelen, unsigned int flags,
    const struct chimera_vfs_attrs *set_attr, uint64_t attr_mask)
{
    int index = compound_add_stream_name(compound, CHIMERA_VFS_COMPOUND_OP_OPEN_STREAM, name, namelen);
    if (index < 0) return index;
    struct chimera_vfs_compound_op *op = compound->ops[index];
    op->open_flags = flags;
    op->attr_mask = attr_mask;
    if (set_attr) op->set_attr = *set_attr;
    return index;
}

SYMBOL_EXPORT int
chimera_vfs_compound_add_list_streams(struct chimera_vfs_compound *compound,
    uint64_t cookie, uint32_t max_bytes, bool want_fh)
{
    int index;
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_next_op(compound,
        CHIMERA_VFS_COMPOUND_OP_LIST_STREAMS, &index);
    if (!op) return -1;
    op->cookie = cookie;
    op->buffer_max = max_bytes;
    op->stream_want_fh = want_fh;
    return index;
}

SYMBOL_EXPORT int
chimera_vfs_compound_add_remove_stream(struct chimera_vfs_compound *compound,
    const char *name, int namelen)
{
    return compound_add_stream_name(compound, CHIMERA_VFS_COMPOUND_OP_REMOVE_STREAM, name, namelen);
}

SYMBOL_EXPORT int
chimera_vfs_compound_add_remove_stream_checked(struct chimera_vfs_compound *compound,
    const char *name, int namelen, const uint8_t *expected_fh, uint32_t expected_fh_len)
{
    if (!expected_fh || !expected_fh_len || expected_fh_len > CHIMERA_VFS_FH_SIZE) {
        compound->build_failed = 1;
        return -1;
    }
    int index = chimera_vfs_compound_add_remove_stream(compound, name, namelen);
    if (index < 0) return index;
    struct chimera_vfs_compound_op *op = compound->ops[index];
    op->remove_flags = CHIMERA_VFS_REMOVE_STREAM_MATCH_FH;
    op->arg_fh_len = expected_fh_len;
    memcpy(op->arg_fh, expected_fh, expected_fh_len);
    return index;
}

SYMBOL_EXPORT int
chimera_vfs_compound_add_listxattrs(
    struct chimera_vfs_compound *compound,
    uint64_t                     cookie,
    uint32_t                     max_bytes)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_LISTXATTRS,
                                      &index);

    if (!op) {
        return -1;
    }

    op->cookie     = cookie;
    op->buffer_max = max_bytes;

    return index;
} /* chimera_vfs_compound_add_listxattrs */

SYMBOL_EXPORT int
chimera_vfs_compound_add_removexattr(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (namelen <= 0 || namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
        if (namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
            compound->build_error = CHIMERA_VFS_ENAMETOOLONG;
        }
        compound->build_failed = 1;
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_REMOVEXATTR,
                                      &index);

    if (!op) {
        return -1;
    }

    memcpy(op->name, name, namelen);
    op->name[namelen] = '\0';
    op->name_len      = (uint32_t) namelen;

    return index;
} /* chimera_vfs_compound_add_removexattr */

SYMBOL_EXPORT int
chimera_vfs_compound_add_create(
    struct chimera_vfs_compound    *compound,
    uint8_t                         create_type,
    const char                     *name,
    int                             namelen,
    const char                     *target,
    int                             targetlen,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (namelen <= 0 || namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
        if (namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
            compound->build_error = CHIMERA_VFS_ENAMETOOLONG;
        }
        compound->build_failed = 1;
        return -1;
    }

    if (create_type == CHIMERA_VFS_COMPOUND_CREATE_SYMLINK &&
        (!target || targetlen <= 0)) {
        compound->build_failed = 1;
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_CREATE, &index);

    if (!op) {
        return -1;
    }

    memcpy(op->name, name, namelen);
    op->name[namelen] = '\0';
    op->name_len      = (uint32_t) namelen;
    op->create_type   = create_type;
    op->attr_mask     = attr_mask;

    if (set_attr) {
        op->set_attr = *set_attr;
    }

    if (create_type == CHIMERA_VFS_COMPOUND_CREATE_SYMLINK) {
        op->link_target = malloc((size_t) targetlen + 1);

        if (!op->link_target) {
            return -1;
        }

        memcpy(op->link_target, target, targetlen);
        op->link_target[targetlen] = '\0';
        op->link_target_len        = (uint32_t) targetlen;
    }

    return index;
} /* chimera_vfs_compound_add_create */

SYMBOL_EXPORT int
chimera_vfs_compound_add_remove(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen,
    unsigned int                 flags)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (namelen <= 0 || namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
        if (namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
            compound->build_error = CHIMERA_VFS_ENAMETOOLONG;
        }
        compound->build_failed = 1;
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_REMOVE, &index);

    if (!op) {
        return -1;
    }

    memcpy(op->name, name, namelen);
    op->name[namelen] = '\0';
    op->name_len      = (uint32_t) namelen;
    op->remove_flags  = flags;

    return index;
} /* chimera_vfs_compound_add_remove */

SYMBOL_EXPORT int
chimera_vfs_compound_add_rename(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen,
    const char                  *new_name,
    int                          new_namelen,
    unsigned int                 flags)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (!name || namelen <= 0 || namelen > CHIMERA_VFS_COMPOUND_NAME_MAX ||
        new_namelen < 0 || new_namelen > CHIMERA_VFS_COMPOUND_NAME_MAX ||
        (new_namelen && !new_name)) {
        if (namelen > CHIMERA_VFS_COMPOUND_NAME_MAX || new_namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
            compound->build_error = CHIMERA_VFS_ENAMETOOLONG;
        }
        compound->build_failed = 1;
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_RENAME, &index);

    if (!op) {
        return -1;
    }

    memcpy(op->name, name, namelen);
    op->name[namelen] = '\0';
    op->name_len      = (uint32_t) namelen;

    op->remove_flags = flags;

    if (new_namelen) {
        memcpy(op->new_name, new_name, new_namelen);
    }
    op->new_name[new_namelen] = '\0';
    op->new_name_len          = (uint32_t) new_namelen;

    return index;
} /* chimera_vfs_compound_add_rename */

SYMBOL_EXPORT int
chimera_vfs_compound_add_link(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen,
    uint64_t                     attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (namelen <= 0 || namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
        if (namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
            compound->build_error = CHIMERA_VFS_ENAMETOOLONG;
        }
        compound->build_failed = 1;
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_LINK, &index);

    if (!op) {
        return -1;
    }

    memcpy(op->name, name, namelen);
    op->name[namelen] = '\0';
    op->name_len      = (uint32_t) namelen;
    op->attr_mask     = attr_mask;

    return index;
} /* chimera_vfs_compound_add_link */

SYMBOL_EXPORT int
chimera_vfs_compound_add_read(
    struct chimera_vfs_compound      *compound,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint32_t                          count,
    struct evpl_iovec                *iov,
    int                               max_iov,
    const struct chimera_claim_actor *io_owner)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (max_iov <= 0 || !iov) {
        compound->build_failed = 1;
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_READ, &index);

    if (!op) {
        return -1;
    }

    op->iov       = iov;
    op->in_handle = handle;

    if (io_owner) {
        op->io_owner      = *io_owner;
        op->have_io_owner = 1;
    }
    op->offset  = offset;
    op->count   = count;
    op->max_iov = max_iov;

    return index;
} /* chimera_vfs_compound_add_read */

SYMBOL_EXPORT int
chimera_vfs_compound_add_write(
    struct chimera_vfs_compound      *compound,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint32_t                          count,
    uint32_t                          sync,
    struct evpl_iovec                *iov,
    int                               niov,
    const struct chimera_claim_actor *io_owner)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_WRITE, &index);

    if (!op) {
        return -1;
    }

    op->in_handle = handle;
    op->offset    = offset;
    op->count     = count;
    op->sync      = sync;
    op->w_iov     = iov;
    op->w_niov    = niov;

    if (io_owner) {
        op->io_owner      = *io_owner;
        op->have_io_owner = 1;
    }

    return index;
} /* chimera_vfs_compound_add_write */

SYMBOL_EXPORT int
chimera_vfs_compound_add_setattr(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *handle,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_SETATTR, &index);

    if (!op) {
        return -1;
    }

    op->in_handle = handle;
    op->attr_mask = attr_mask;

    if (set_attr) {
        op->set_attr = *set_attr;
    }

    return index;
} /* chimera_vfs_compound_add_setattr */

SYMBOL_EXPORT int
chimera_vfs_compound_add_overwrite(
    struct chimera_vfs_compound      *compound,
    struct chimera_vfs_open_handle   *handle,
    const struct chimera_vfs_attrs   *set_attr,
    uint64_t                          attr_mask,
    const struct chimera_claim_actor *io_owner)
{
    int index = chimera_vfs_compound_add_setattr(compound, handle, set_attr, attr_mask);

    if (index >= 0) {
        struct chimera_vfs_compound_op *op = compound->ops[index];
        op->type = CHIMERA_VFS_COMPOUND_OP_OVERWRITE;
        if (io_owner) {
            op->io_owner = *io_owner;
            op->have_io_owner = 1;
        }
    }
    return index;
} /* chimera_vfs_compound_add_overwrite */

SYMBOL_EXPORT int
chimera_vfs_compound_add_open(
    struct chimera_vfs_compound    *compound,
    const char                     *name,
    int                             namelen,
    unsigned int                    flags,
    uint32_t                        opts,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
        compound->build_error  = CHIMERA_VFS_ENAMETOOLONG;
        compound->build_failed = 1;
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_OPEN,
                                      &index);

    if (!op) {
        return -1;
    }

    if (name && namelen > 0) {
        memcpy(op->name, name, namelen);
        op->name[namelen] = '\0';
        op->name_len      = (uint32_t) namelen;
    }

    op->open_flags = flags;
    op->open_opts  = opts;
    op->attr_mask  = attr_mask;

    if (set_attr) {
        op->set_attr = *set_attr;
    }

    return index;
} /* chimera_vfs_compound_add_open */

SYMBOL_EXPORT int
chimera_vfs_compound_add_reserve(
    struct chimera_vfs_compound *compound,
    uint32_t                     handle_from,
    struct chimera_vfs_claim    *claim)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (handle_from >= compound->num_ops || !claim) {
        compound->build_failed = 1;
        return -1;
    }
    op = chimera_vfs_compound_next_op(compound, CHIMERA_VFS_COMPOUND_OP_RESERVE,
                                      &index);
    if (!op) {
        return -1;
    }
    op->handle_from = (int) handle_from;
    op->claim       = claim;
    return index;
} /* chimera_vfs_compound_add_reserve */

SYMBOL_EXPORT int
chimera_vfs_compound_add_reserve_access(struct chimera_vfs_compound *compound,
                                       uint32_t handle_from, struct chimera_vfs_claim *template_claim)
{
    int index = chimera_vfs_compound_add_reserve(compound, handle_from, template_claim);
    if (index >= 0) compound->ops[index]->type = CHIMERA_VFS_COMPOUND_OP_RESERVE_ACCESS;
    return index;
}

/* Follow execution links: a dynamic suffix can have larger physical indices
 * than a following prebuilt group. Endpoints must be ordered in one group. */
static bool
compound_access_ready_in_group(const struct chimera_vfs_compound *compound,
                               uint32_t group_index, uint32_t reserve_index, uint32_t ready_index)
{
    bool seen = false;
    for (int32_t i = compound->groups[group_index].config.first_op; i >= 0;
         i = compound->ops[i]->group_next) {
        if ((uint32_t) i == ready_index) return seen;
        if ((uint32_t) i == reserve_index) seen = true;
    }
    return false;
}

SYMBOL_EXPORT bool
chimera_vfs_compound_reserve_access_until(struct chimera_vfs_compound *compound,
                                         uint32_t reserve_index, uint32_t ready_index)
{
    if (reserve_index >= compound->num_ops || ready_index >= compound->num_ops ||
        compound->ops[reserve_index]->type != CHIMERA_VFS_COMPOUND_OP_RESERVE_ACCESS ||
        compound->ops[ready_index]->type != CHIMERA_VFS_COMPOUND_OP_CHECKPOINT ||
        compound->ops[reserve_index]->prepared || compound->ops[ready_index]->prepared ||
        (compound->running && (!compound->completing || !compound->num_groups ||
          !compound_access_ready_in_group(compound, compound->group_index, reserve_index, ready_index)))) {
        compound->build_failed = 1;
        compound->build_error = CHIMERA_VFS_EINVAL;
        return false;
    }
    compound->ops[reserve_index]->access_ready = ready_index + 1;
    return true;
}

SYMBOL_EXPORT int
chimera_vfs_compound_add_narrow_access(struct chimera_vfs_compound *compound,
                                      uint32_t reserve_index, uint8_t used, uint8_t denied)
{
    if (reserve_index >= compound->num_ops ||
        compound->ops[reserve_index]->type != CHIMERA_VFS_COMPOUND_OP_RESERVE_ACCESS ||
        (compound->running && (!compound->completing || !compound->num_groups))) {
        compound->build_failed = 1;
        compound->build_error = CHIMERA_VFS_EINVAL;
        return -1;
    }
    if (!compound->access_journal) {
        compound->access_journal = chimera_vfs_claim_access_journal_alloc(CHIMERA_VFS_COMPOUND_MAX_OPS);
        if (!compound->access_journal) {
            compound->build_failed = 1;
            compound->build_error = CHIMERA_VFS_ENOSPC;
            return -1;
        }
    }
    int index;
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_next_op(compound,
        CHIMERA_VFS_COMPOUND_OP_NARROW_ACCESS, &index);
    if (!op) return -1;
    op->access_narrow_from = reserve_index;
    op->access_narrow_used = used;
    op->access_narrow_denied = denied;
    return index;
}

SYMBOL_EXPORT int
chimera_vfs_compound_add_retire_access(struct chimera_vfs_compound *compound,
                                      struct chimera_vfs_claim_access_owner *owner)
{
    if (compound->running || compound->original_ops) {
        compound->build_failed = 1;
        compound->build_error = CHIMERA_VFS_ENOTSUP;
        return -1;
    }
    if (!compound->access_journal) {
        compound->access_journal = chimera_vfs_claim_access_journal_alloc(CHIMERA_VFS_COMPOUND_MAX_OPS);
        if (!compound->access_journal) {
            compound->build_failed = 1;
            compound->build_error = CHIMERA_VFS_ENOSPC;
            return -1;
        }
    }
    int index;
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_next_op(compound,
        CHIMERA_VFS_COMPOUND_OP_RETIRE_ACCESS, &index);
    if (!op) return -1;
    op->access_retire_owner = owner;
    return index;
}

SYMBOL_EXPORT int
chimera_vfs_compound_add_retire_open_claims(struct chimera_vfs_compound *compound,
    struct chimera_vfs_claim_owner *range_owner,
    struct chimera_vfs_claim_access_owner *access_owner,
    struct chimera_vfs_claim_access_owner *base_access_owner)
{
    int index = chimera_vfs_compound_add_retire_access(compound, access_owner);
    if (index < 0) return -1;
    if (!compound->claim_journal) {
        compound->claim_journal = chimera_vfs_claim_journal_alloc(CHIMERA_VFS_COMPOUND_MAX_OPS, 1);
        if (!compound->claim_journal) {
            compound->build_failed = 1;
            compound->build_error = CHIMERA_VFS_ENOSPC;
            return -1;
        }
    }
    struct chimera_vfs_compound_op *op = compound->ops[index];
    op->type = CHIMERA_VFS_COMPOUND_OP_RETIRE_OPEN_CLAIMS;
    op->range_retire_owner = range_owner;
    op->base_access_retire_owner = base_access_owner;
    return index;
}

SYMBOL_EXPORT int
chimera_vfs_compound_add_retire_range_owner(struct chimera_vfs_compound *compound,
    struct chimera_vfs_claim_owner *range_owner)
{
    return chimera_vfs_compound_add_retire_open_claims(compound, range_owner, NULL, NULL);
}

SYMBOL_EXPORT int
chimera_vfs_compound_add_reserve_handle(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_claim       *claim)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (!claim) {
        compound->build_failed = 1;
        return -1;
    }
    op = chimera_vfs_compound_next_op(compound, CHIMERA_VFS_COMPOUND_OP_RESERVE, &index);
    if (!op) {
        return -1;
    }
    op->in_handle = handle;
    op->claim     = claim;
    return index;
} /* chimera_vfs_compound_add_reserve_handle */

SYMBOL_EXPORT int
chimera_vfs_compound_add_link_replace(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen,
    uint64_t                     attr_mask)
{
    int index;

    if (namelen == 0) {
        struct chimera_vfs_compound_op *op = chimera_vfs_compound_next_op(
            compound, CHIMERA_VFS_COMPOUND_OP_LINK, &index);
        if (!op) {
            return -1;
        }
        op->attr_mask = attr_mask;
    } else {
        if (!name) {
            compound->build_failed = 1;
            return -1;
        }
        index = chimera_vfs_compound_add_link(compound, name, namelen, attr_mask);
    }
    if (index >= 0) {
        compound->ops[index]->open_opts = 1;
    }
    return index;
} /* chimera_vfs_compound_add_link_replace */

/* ---------------------------------------------------------------------- */
/* Execution                                                              */
/* ---------------------------------------------------------------------- */

SYMBOL_EXPORT void
chimera_vfs_compound_set_result_masks(
    struct chimera_vfs_compound *compound,
    int                          index,
    uint64_t                     object_mask,
    uint64_t                     pre_mask,
    uint64_t                     post_mask)
{
    if (index < 0 || (uint32_t) index >= compound->num_ops) {
        compound->build_failed = 1;
        return;
    }
    struct chimera_vfs_compound_op *op = compound->ops[index];
    op->result_masks_set      = 1;
    op->result_attr_mask      = object_mask;
    op->result_pre_attr_mask  = pre_mask;
    op->result_post_attr_mask = post_mask;
} /* chimera_vfs_compound_set_result_masks */

static uint64_t
compound_object_mask(
    const struct chimera_vfs_compound_op *op,
    uint64_t                              fallback)
{
    return op->result_masks_set ? op->result_attr_mask : fallback;
} /* compound_object_mask */

static uint64_t
compound_pre_mask(
    const struct chimera_vfs_compound_op *op,
    uint64_t                              fallback)
{
    return op->result_masks_set ? op->result_pre_attr_mask : fallback;
} /* compound_pre_mask */

static uint64_t
compound_post_mask(
    const struct chimera_vfs_compound_op *op,
    uint64_t                              fallback)
{
    return op->result_masks_set ? op->result_post_attr_mask : fallback;
} /* compound_post_mask */

/* Before/after directory snapshots never borrow a callback-scoped ACL. */
static void
compound_store_aux(
    struct chimera_vfs_attrs       *dst,
    const struct chimera_vfs_attrs *src)
{
    if (src) {
        *dst              = *src;
        dst->va_acl       = NULL;
        dst->va_set_mask &= ~CHIMERA_VFS_ATTR_ACL;
        dst->va_req_mask &= ~CHIMERA_VFS_ATTR_ACL;
    }
} /* compound_store_aux */

SYMBOL_EXPORT void
chimera_vfs_compound_set_finish_handler(
    struct chimera_vfs_compound          *compound,
    chimera_vfs_compound_finish_handler_t handler,
    void                                 *private_data)
{
    compound->finish_handler = handler;
    compound->finish_private = private_data;
} /* chimera_vfs_compound_set_finish_handler */

SYMBOL_EXPORT enum chimera_vfs_error
chimera_vfs_compound_execution_status(const struct chimera_vfs_compound *compound)
{
    return compound->execution_status;
} /* chimera_vfs_compound_execution_status */

SYMBOL_EXPORT enum chimera_vfs_error
chimera_vfs_compound_finish_status(const struct chimera_vfs_compound *compound)
{
    return compound->finish_status;
} /* chimera_vfs_compound_finish_status */

SYMBOL_EXPORT void
chimera_vfs_compound_finish_result(
    struct chimera_vfs_compound *compound,
    enum chimera_vfs_error       status)
{
    if (!compound->finishing) {
        return;
    }
    /* Local locks publish their preallocated range journal only now. Close
     * generation invalidation can veto publication even while finish waited.
     * Mandatory legacy release cleanup has already finalized independently. */
    if (status == CHIMERA_VFS_OK) {
        for (uint32_t i = 0; i < compound->num_ops; i++) {
            struct chimera_vfs_compound_op *op = compound->ops[i];
            if (op->completed && op->lock_attempt && op->status == CHIMERA_VFS_OK) {
                enum chimera_vfs_error lock_status = chimera_vfs_lock_attempt_accept(op->lock_attempt);
                if (lock_status != CHIMERA_VFS_OK) {
                    op->status                 = lock_status;
                    if (compound->execution_status == CHIMERA_VFS_OK) {
                        compound->execution_status = lock_status;
                    }
                    for (uint32_t g = 0; g < compound->num_groups; g++) {
                        for (int32_t j = compound->groups[g].config.first_op; j >= 0;
                             j = compound->ops[j]->group_next) {
                            if ((uint32_t) j == i) {
                                compound->groups[g].status = lock_status;
                                break;
                            }
                        }
                    }
                }
            }
        }
    }
    if (status == CHIMERA_VFS_OK && compound->access_journal) {
        chimera_vfs_claim_access_journal_publish(compound->access_journal);
        compound->access_journal_published = 1;
    }
    if (status == CHIMERA_VFS_OK && compound->claim_journal) {
        chimera_vfs_claim_journal_publish(compound->claim_journal);
        compound->claim_journal_published = 1;
    }
    compound->finishing     = 0;
    compound->finish_status = status;
    compound->status        = status == CHIMERA_VFS_OK ? compound->execution_status : status;
    compound->running       = 0;
    compound->callback(compound, compound->private_data);
} /* chimera_vfs_compound_finish_result */

static void
chimera_vfs_compound_finish_dispatch(struct chimera_vfs_compound *compound)
{
    compound->finishing = 1;
    if (compound->claim_journal) {
        chimera_vfs_claim_journal_seal(compound->claim_journal);
    }
    if (compound->access_journal) {
        chimera_vfs_claim_access_journal_seal(compound->access_journal);
    }
    if (compound->finish_handler) {
        compound->finish_handler(compound, compound->finish_private);
    } else {
        chimera_vfs_compound_finish_result(compound, CHIMERA_VFS_OK);
    }
} /* chimera_vfs_compound_finish_dispatch */

static void
chimera_vfs_compound_finish(
    struct chimera_vfs_compound *compound,
    enum chimera_vfs_error       status)
{
    if (compound->execution_status == CHIMERA_VFS_OK) {
        compound->execution_status = status;
    }
    if (compound->stepping) {
        compound->finish_pending = 1;
        return;
    }
    chimera_vfs_compound_finish_dispatch(compound);
} /* chimera_vfs_compound_finish */

/* Withdraw only explicitly private, failed producer admissions. Keep token and
 * file storage alive: frontend aliases and an earlier journal retirement can
 * still reference it. A journal-held token is already excluded in this attempt
 * and its cutoff drains after the journal, preserving prior accepted deltas. */
static void
compound_finish_private_reservations(struct chimera_vfs_compound *compound,
                                     struct chimera_vfs_compound_group *group,
                                     enum chimera_vfs_error status)
{
    for (int32_t i = group->config.first_op; i >= 0; i = compound->ops[i]->group_next) {
        struct chimera_vfs_compound_op *op = compound->ops[i];
        if (!op->access_ready || !op->access_owner || !op->claim_held) continue;
        const struct chimera_vfs_compound_op *ready = compound->ops[op->access_ready - 1];
        if (status == CHIMERA_VFS_OK && ready->completed && !ready->skipped &&
            ready->status == CHIMERA_VFS_OK) continue;
        op->claim_held = 0;
        chimera_vfs_claim_access_owner_retire(op->access_owner);
    }
}

/* Complete a group without masking its error or executing skipped callouts. */
static void
chimera_vfs_compound_group_done(
    struct chimera_vfs_compound *compound,
    enum chimera_vfs_error       status)
{
    struct chimera_vfs_compound_group *group = &compound->groups[compound->group_index];
    compound->cancel_defer_end = 0;
    group->status = status;
    compound_finish_private_reservations(compound, group, status);
    if (compound->execution_status == CHIMERA_VFS_OK) {
        compound->execution_status = status;
    }
    if (compound->build_failed || compound->canceled ||
        (status != CHIMERA_VFS_OK && !group->config.continue_on_error)) {
        chimera_vfs_compound_finish(compound, status);
        return;
    }
    compound->group_index++;
    compound->group_active = 0;
    compound->index = compound->group_index == compound->num_groups ? compound->num_ops :
        compound->groups[compound->group_index].config.first_op;
    chimera_vfs_compound_step(compound);
} /* chimera_vfs_compound_group_done */

/* One op finished.  Record it and either advance or stop. */
static void
chimera_vfs_compound_op_done(
    struct chimera_vfs_compound *compound,
    enum chimera_vfs_error       status)
{
    struct chimera_vfs_compound_op *done = compound->ops[compound->index];

    compound->completed++;
    done->completed = 1;

    /* Record what the op ended up addressing, so a caller describing the
     * object in its reply does not have to re-derive it. */
    if (compound->fh_len) {
        memcpy(done->fh, compound->fh, compound->fh_len);
        done->fh_len = compound->fh_len;
    }

    if (status == CHIMERA_VFS_OK && done->result_error != CHIMERA_VFS_OK) {
        status = done->result_error;
    }

    /* Arm before the caller can cancel from complete(). A failed/skipped
     * commit point never creates a mandatory suffix. */
    if (status == CHIMERA_VFS_OK && !done->skipped && done->cancel_scope_end) {
        compound->cancel_defer_end = done->cancel_scope_end;
    }

    /* The caller's veto, before anything behind this op runs -- and after the
     * op's own results are recorded, because that is what it inspects. */
    if (done->complete) {
        compound->completing = 1;
        done->complete(compound, compound->index, &status,
                       done->callback_private);
        compound->completing = 0;
    }
    if (compound->build_failed && status == CHIMERA_VFS_OK) {
        status = compound->build_error ? compound->build_error : CHIMERA_VFS_EINVAL;
    }
    if (compound->gate) {
        compound->gate(compound, compound->index, &status,
                       compound->gate_private);
    }

    done->status = status;
    if (compound->cancel_defer_end == compound->index + 1) compound->cancel_defer_end = 0;

    compound->open_resolved  = 0;
    compound->open_retried   = 0;
    compound->io_typechecked = 0;
    if (compound->num_groups) {
        if (status != CHIMERA_VFS_OK || done->group_next < 0 || compound_cancel_stops(compound)) {
            chimera_vfs_compound_group_done(compound,
                status == CHIMERA_VFS_OK && compound_cancel_stops(compound) ? CHIMERA_VFS_EINTR : status);
            return;
        }
        compound->index = done->group_next;
    } else {
        if (status != CHIMERA_VFS_OK || compound->canceled) {
            chimera_vfs_compound_finish(compound,
                status == CHIMERA_VFS_OK ? CHIMERA_VFS_EINTR : status);
            return;
        }
        compound->index++;
    }
    chimera_vfs_compound_step(compound);
} /* chimera_vfs_compound_op_done */

SYMBOL_EXPORT bool
chimera_vfs_compound_coordinate_done(
    struct chimera_vfs_compound *compound,
    uint64_t                     token,
    enum chimera_vfs_error       status)
{
    struct chimera_vfs_compound_coordination *memo = compound->coordination_pending;

    if (!memo || memo->token != token || !compound->running) {
        return false;
    }
    memo->status                   = status;
    compound->coordination_pending = NULL;
    chimera_vfs_compound_op_done(compound, status);
    return true;
} /* chimera_vfs_compound_coordinate_done */

static void
chimera_vfs_compound_coordinate(struct chimera_vfs_compound *compound)
{
    struct chimera_vfs_compound_op           *op = compound->ops[compound->index];
    struct chimera_vfs_compound_coordination *memo;

    if (!compound->fh_len) {
        chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
        return;
    }
    for (memo = compound->coordinations; memo; memo = memo->next) {
        if (!op->coordinate_each_attempt &&
            memo->index == compound->index && memo->start == op->coordinate &&
            memo->private_data == op->coordinate_private && memo->fh_len == compound->fh_len &&
            !memcmp(memo->fh, compound->fh, memo->fh_len)) {
            chimera_vfs_compound_op_done(compound, memo->status);
            return;
        }
    }
    if (compound->num_coordinations == CHIMERA_VFS_COMPOUND_MAX_OPS ||
        !(memo = calloc(1, sizeof(*memo)))) {
        chimera_vfs_compound_op_done(compound, CHIMERA_VFS_ENOSPC);
        return;
    }
    memo->index  = compound->index;
    memo->start = op->coordinate;
    memo->private_data = op->coordinate_private;
    memo->fh_len = compound->fh_len;
    memcpy(memo->fh, compound->fh, memo->fh_len);
    memo->token                    = ++compound->num_coordinations;
    memo->next                     = compound->coordinations;
    compound->coordinations        = memo;
    compound->coordination_pending = memo;
    /* The compound is parked before start so inline completion is safe. No
     * access after this callback: completion may accept and free the request. */
    op->coordinate(compound, compound->index, memo->token, memo->fh, memo->fh_len,
                   op->coordinate_private);
} /* chimera_vfs_compound_coordinate */

/* The current object changed: drop the handle we were holding for the old
 * one.  The next op that needs a handle opens the new fh. */
static void
chimera_vfs_compound_set_current(
    struct chimera_vfs_compound *compound,
    const void                  *fh,
    uint32_t                     fh_len)
{
    chimera_vfs_compound_release_cursor(compound);

    compound->handle_flags = 0;

    memcpy(compound->fh, fh, fh_len);
    compound->fh_len = fh_len;
} /* chimera_vfs_compound_set_current */

static void
chimera_vfs_compound_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct chimera_vfs_compound *compound = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_op_done(compound, error_code);
        return;
    }

    compound->handle = handle;

    /* Re-enter the op that needed the handle. */
    chimera_vfs_compound_step(compound);
} /* chimera_vfs_compound_open_callback */

/* The backend's ACL is callback-scoped. Keep an owned copy so execution-time
 * authorization uses the same attributes the backend actually returned. */
static void
chimera_vfs_compound_store_attr(
    struct chimera_vfs_compound_op *op,
    const struct chimera_vfs_attrs *attr)
{
    free(op->attr.va_acl);
    op->attr        = *attr;
    op->attr.va_acl = NULL;
    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_ACL) && attr->va_acl) {
        size_t size = chimera_acl_size(attr->va_acl->num_aces);
        op->attr.va_acl = malloc(size);
        if (!op->attr.va_acl) {
            op->result_error = CHIMERA_VFS_ENOSPC;
            return;
        }
        memcpy(op->attr.va_acl, attr->va_acl, size);
    }
} /* chimera_vfs_compound_store_attr */

static void
chimera_vfs_compound_lookup_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    compound_store_aux(&op->dir_post_attr, dir_attr);

    if (error_code == CHIMERA_VFS_OK) {
        if (attr) {
            chimera_vfs_compound_store_attr(op, attr);
        }

        if (attr && (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
            chimera_vfs_compound_set_current(compound, attr->va_fh,
                                             attr->va_fh_len);
        } else {
            /* A backend that resolves names without producing a handle
             * cannot be chained: the sequence has no way to address what it
             * just found. */
            error_code = CHIMERA_VFS_ENOTSUP;
        }
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_lookup_callback */

static void
chimera_vfs_compound_getattr_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (error_code == CHIMERA_VFS_OK && attr) {
        chimera_vfs_compound_store_attr(op, attr);

        if (op->type == CHIMERA_VFS_COMPOUND_OP_ACCESS) {
            op->granted = chimera_vfs_access_check(attr, compound->cred,
                                                   op->requested);
        }
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_getattr_callback */

/* readlink writes into a caller-supplied buffer and reports only the length,
 * so the target buffer is allocated before the call and sized here. */
#define CHIMERA_VFS_COMPOUND_TARGET_MAX 4096

static void
chimera_vfs_compound_readlink_callback(
    enum chimera_vfs_error    error_code,
    int                       targetlen,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    (void) attr;

    if (error_code == CHIMERA_VFS_OK && targetlen > 0) {
        op->target[targetlen] = '\0';
        op->target_len        = (uint32_t) targetlen;
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_readlink_callback */

static void
chimera_vfs_compound_commit_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    compound_store_aux(&op->dir_pre_attr, pre_attr);
    compound_store_aux(&op->dir_post_attr, post_attr);

    if (error_code == CHIMERA_VFS_OK && pre_attr) {
        chimera_vfs_compound_store_attr(op, pre_attr);
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_commit_callback */

/* One directory entry.  Returns non-zero to stop the enumeration, which is how
 * every readdir caller applies a budget; the backend then reports eof=0 and the
 * cookie of the entry it was refused. */
static int
chimera_vfs_compound_readdir_entry(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct chimera_vfs_compound        *compound = arg;
    struct chimera_vfs_compound_op     *op       = compound->ops[compound->index];
    struct chimera_vfs_compound_dirent *dirent;

    /* Streaming: the caller marshals this entry now and says whether it fits.
     * Nothing is staged, so there is nothing here to bound. */
    if (op->readdir_append) {
        return op->readdir_append(compound, compound->index,
                                  inum, cookie, name, namelen, attrs,
                                  op->readdir_private);
    }

    if (op->num_entries >= op->max_entries) {
        return -1;
    }

    /* A name the entry struct cannot hold would be silently truncated into a
     * different name, so stop rather than report one. */
    if (namelen <= 0 || namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
        return -1;
    }

    dirent = &op->entries[op->num_entries];

    dirent->inum     = inum;
    dirent->cookie   = cookie;
    dirent->name_len = (uint32_t) namelen;
    memcpy(dirent->name, name, namelen);
    dirent->name[namelen] = '\0';

    dirent->attr = *attrs;
    /* Same rule as every other attribute result here: the backend owns the ACL
     * only while this callback runs. */
    dirent->attr.va_acl       = NULL;
    dirent->attr.va_req_mask &= ~CHIMERA_VFS_ATTR_ACL;
    dirent->attr.va_set_mask &= ~CHIMERA_VFS_ATTR_ACL;

    op->num_entries++;

    return 0;
} /* chimera_vfs_compound_readdir_entry */

static void
chimera_vfs_compound_readdir_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        eof,
    struct chimera_vfs_attrs       *dir_attr,
    void                           *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    (void) handle;
    compound_store_aux(&op->dir_post_attr, dir_attr);

    if (error_code == CHIMERA_VFS_OK) {
        op->r_cookie   = cookie;
        op->r_verifier = verifier;
        op->eof        = eof;
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_readdir_callback */

static void
chimera_vfs_compound_get_xattr_callback(
    enum chimera_vfs_error error_code,
    uint32_t               value_len,
    void                  *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (error_code == CHIMERA_VFS_OK) {
        op->buffer_len = value_len;
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_get_xattr_callback */

static void
chimera_vfs_compound_list_xattrs_callback(
    enum chimera_vfs_error error_code,
    const char            *names,
    uint32_t               names_len,
    uint32_t               count,
    uint32_t               eof,
    uint64_t               cookie,
    void                  *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    /* The backend wrote the names into the buffer we gave it, which is
     * op->buffer; `names` is that same pointer handed back. */
    (void) names;

    if (error_code == CHIMERA_VFS_OK) {
        op->buffer_len   = names_len;
        op->buffer_count = count;
        op->eof          = eof;
        op->r_cookie     = cookie;
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_list_xattrs_callback */

/* SETXATTR and REMOVEXATTR report the object's ctime either side of the
 * change; both callbacks land here. */
static void
chimera_vfs_compound_xattr_change_callback(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (error_code == CHIMERA_VFS_OK) {
        if (pre_attr) {
            op->pre_ctime = pre_attr->va_ctime;
        }
        if (post_attr) {
            op->post_ctime = post_attr->va_ctime;
        }
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_xattr_change_callback */

/*
 * An OPEN with REGULAR_ONLY refused the object it found.  The caller decides
 * what to say about it -- op->existing_mode is there precisely because no
 * errno carries "not a regular file, and here is what it was" -- but a status
 * still has to be something, so report the closest POSIX answer for the type.
 */
static enum chimera_vfs_error
chimera_vfs_compound_nonreg_error(uint32_t mode)
{
    if (S_ISDIR(mode)) {
        return CHIMERA_VFS_EISDIR;
    }

    if (S_ISLNK(mode)) {
        return CHIMERA_VFS_ELOOP;
    }

    return CHIMERA_VFS_EINVAL;
} /* chimera_vfs_compound_nonreg_error */

/*
 * Step one of a two-step OPEN: the name has been resolved (or found absent).
 * Apply what the resolve was for, then re-enter step() to do the open itself.
 */
static void
chimera_vfs_compound_open_resolve_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    (void) dir_attr;

    compound->open_resolved = 1;

    if (error_code == CHIMERA_VFS_ENOENT) {
        /* Nothing there.  A create proceeds and makes it; a plain open fails
         * exactly as the open itself would have. */
        if (!(op->open_flags & CHIMERA_VFS_OPEN_CREATE)) {
            chimera_vfs_compound_op_done(compound, CHIMERA_VFS_ENOENT);
            return;
        }
        chimera_vfs_compound_step(compound);
        return;
    }

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_op_done(compound, error_code);
        return;
    }

    op->existed = 1;

    if (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        op->existing_mode = attr->va_mode;

        if ((op->open_opts & CHIMERA_VFS_COMPOUND_OPEN_REGULAR_ONLY) &&
            !S_ISREG(attr->va_mode)) {
            chimera_vfs_compound_op_done(
                compound,
                chimera_vfs_compound_nonreg_error(attr->va_mode));
            return;
        }
    }

    if (op->open_opts & CHIMERA_VFS_COMPOUND_OPEN_ATTRS_ON_CREATE_ONLY) {
        /* The object exists, so the create attributes do not describe it. */
        op->applied_attr.va_set_mask = 0;
        op->applied_attr.va_req_mask = 0;
    }

    chimera_vfs_compound_step(compound);
} /* chimera_vfs_compound_open_resolve_callback */

static void
chimera_vfs_compound_open_at_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    (void) set_attr;

    compound_store_aux(&op->dir_pre_attr, dir_pre_attr);
    compound_store_aux(&op->dir_post_attr, dir_post_attr);
    if (attr) {
        chimera_vfs_compound_store_attr(op, attr);
    }

    if (error_code == CHIMERA_VFS_EEXIST &&
        (op->open_opts & CHIMERA_VFS_COMPOUND_OPEN_EXCLUSIVE_RETRY) &&
        !compound->open_retried) {
        /* The name was taken.  Open what is there so the caller can decide what
         * the collision means; it applies no attributes, and asks for no
         * access, because this open exists to be looked at. */
        compound->open_retried = 1;
        op->existed            = 1;

        op->applied_attr.va_set_mask = 0;
        op->applied_attr.va_req_mask = 0;

        chimera_vfs_open_at(compound->thread, compound->cred,
                            compound->handle,
                            op->path ? op->path : op->name, op->name_len,
                            CHIMERA_VFS_OPEN_INFERRED,
                            &op->applied_attr,
                            compound_object_mask(op, op->attr_mask) | CHIMERA_VFS_ATTR_FH,
                            compound_pre_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                            compound_post_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                            chimera_vfs_compound_open_at_callback,
                            compound);
        return;
    }

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_op_done(compound, error_code);
        return;
    }

    op->created = handle->r_created;

    /* OPEN selects both the filehandle and open cursor. The operation owns
     * the reference; the cursor borrows it, so GETHANDLE and following I/O can
     * use precisely the object and access grant OPEN produced. */
    chimera_vfs_compound_set_current(compound, handle->fh, handle->fh_len);

    op->out_handle            = handle;
    compound->handle          = handle;
    compound->handle_flags    = op->open_flags;
    compound->handle_explicit = 1;
    compound->handle_taken    = 1;
    compound->handle_origin   = compound->index + 1;


    chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
} /* chimera_vfs_compound_open_at_callback */

static void
chimera_vfs_compound_open_stream_callback(enum chimera_vfs_error error,
    struct chimera_vfs_open_handle *handle, struct chimera_vfs_attrs *attr, void *private_data)
{
    chimera_vfs_compound_open_at_callback(error, handle, NULL, attr, NULL, NULL, private_data);
}

static void
chimera_vfs_compound_list_streams_callback(enum chimera_vfs_error error,
    const void *records, uint32_t length, uint32_t count, uint32_t eof,
    uint64_t cookie, void *private_data)
{
    chimera_vfs_compound_list_xattrs_callback(error, records, length, count, eof, cookie, private_data);
}

static void
chimera_vfs_compound_remove_stream_callback(enum chimera_vfs_error error,
    const struct chimera_vfs_attrs *pre, const struct chimera_vfs_attrs *post, void *private_data)
{
    struct chimera_vfs_compound *compound = private_data;
    struct chimera_vfs_compound_op *op = compound->ops[compound->index];
    compound_store_aux(&op->dir_pre_attr, pre);
    compound_store_aux(&op->dir_post_attr, post);
    chimera_vfs_compound_op_done(compound, error);
}

static void
chimera_vfs_compound_open_fh_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_op_done(compound, error_code);
        return;
    }

    if (op->inherited_grant_handle &&
        op->inherited_grant_handle->granted_valid &&
        op->inherited_grant_handle->granted_bound) {
        /* The source is an already authorized open on this same object.
         * Preserve its retained rights when upgrading the physical handle;
         * current ACLs govern only newly requested rights, not old grants. */
        chimera_vfs_handle_stamp_access(handle,
                                        op->inherited_grant_handle->granted_access);
    }
    if (op->inherited_grant_handle2 &&
        op->inherited_grant_handle2->granted_valid &&
        op->inherited_grant_handle2->granted_bound) {
        chimera_vfs_handle_stamp_access(handle,
                                        op->inherited_grant_handle2->granted_access);
    }

    /* Re-opening the current object does not move it, and open_fh reports no
     * attributes: a caller wanting them asks for a GETATTR after this. */
    chimera_vfs_compound_release_cursor(compound);
    op->out_handle            = handle;
    compound->handle          = handle;
    compound->handle_flags    = op->open_flags;
    compound->handle_explicit = 1;
    compound->handle_taken    = 1;
    compound->handle_origin   = compound->index + 1;


    chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
} /* chimera_vfs_compound_open_fh_callback */

/*
 * A CREATE finished.  The three underlying calls answer with the same three
 * things -- the new object's attributes and the parent's, either side -- so one
 * completion serves all of them.
 */
static void
chimera_vfs_compound_create_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    (void) set_attr;

    compound_store_aux(&op->dir_pre_attr, dir_pre_attr);
    compound_store_aux(&op->dir_post_attr, dir_post_attr);
    if (attr) {
        chimera_vfs_compound_store_attr(op, attr);
    }

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_op_done(compound, error_code);
        return;
    }

    op->created = 1;

    /* The new object becomes current, which is what lets a caller ask for its
     * file handle or its attributes without naming it again. */
    if (attr && (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        chimera_vfs_compound_set_current(compound, attr->va_fh,
                                         attr->va_fh_len);
    }

    chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
} /* chimera_vfs_compound_create_callback */

/* symlink_at reports no set_attr, so it needs its own entry point into the
 * completion above rather than a different completion. */
static void
chimera_vfs_compound_symlink_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    chimera_vfs_compound_create_callback(error_code, NULL, attr,
                                         dir_pre_attr, dir_post_attr,
                                         private_data);
} /* chimera_vfs_compound_symlink_callback */

/* Step one of a two-step READ or WRITE: the object's type, from a path open,
 * before it is opened for data. */
static void
chimera_vfs_compound_io_type_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_op_done(compound, error_code);
        return;
    }

    compound->io_typechecked = 1;

    if (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
        op->existing_mode = attr->va_mode;

        if (!S_ISREG(attr->va_mode)) {
            chimera_vfs_compound_op_done(
                compound,
                chimera_vfs_compound_nonreg_error(attr->va_mode));
            return;
        }
    }

    /* Re-enter: the op now wants a data handle, which the step will open
     * because a path handle cannot serve it. */
    chimera_vfs_compound_step(compound);
} /* chimera_vfs_compound_io_type_callback */

static void
chimera_vfs_compound_read_callback(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (attr) {
        chimera_vfs_compound_store_attr(op, attr);
    }

    if (error_code != CHIMERA_VFS_OK) {
        /* Nothing was handed over, so nothing is ours to keep. */
        evpl_iovecs_release(compound->thread->evpl, iov, niov);
        chimera_vfs_compound_op_done(compound, error_code);
        return;
    }

    /* Backend descriptor storage may expire as soon as this callback returns
     * (an upstream RPC reply, for example), while later compound operations
     * can suspend. Move ownership into the caller's durable array. A memcpy
     * would leave evpl's ownership canary attached to the old descriptor. */
    if (niov > op->max_iov) {
        evpl_iovecs_release(compound->thread->evpl, iov, niov);
        chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EOVERFLOW);
        return;
    }
    if (iov != op->iov) {
        for (int i = 0; i < niov; i++) {
            evpl_iovec_move(&op->iov[i], &iov[i]);
        }
    }
    op->niov     = niov;
    op->read_len = count;
    op->eof_read = eof;

    chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
} /* chimera_vfs_compound_read_callback */

static void
chimera_vfs_compound_write_callback(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    compound_store_aux(&op->dir_pre_attr, pre_attr);
    compound_store_aux(&op->dir_post_attr, post_attr);
    if (post_attr) {
        chimera_vfs_compound_store_attr(op, post_attr);
    }

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_op_done(compound, error_code);
        return;
    }

    op->written   = length;
    op->committed = sync;

    chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
} /* chimera_vfs_compound_write_callback */

static void
chimera_vfs_compound_setattr_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    compound_store_aux(&op->dir_pre_attr, pre_attr);
    compound_store_aux(&op->dir_post_attr, post_attr);
    if (post_attr) {
        chimera_vfs_compound_store_attr(op, post_attr);
    }
    if (set_attr) {
        op->applied_attr = *set_attr;
    }

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_op_done(compound, error_code);
        return;
    }


    chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
} /* chimera_vfs_compound_setattr_callback */

static void
chimera_vfs_compound_remove_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    compound_store_aux(&op->dir_pre_attr, pre_attr);
    compound_store_aux(&op->dir_post_attr, post_attr);

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_op_done(compound, error_code);
        return;
    }

    /* A REMOVE does not move the current object: it unlinks a name FROM it. */

    chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
} /* chimera_vfs_compound_remove_callback */


static void
chimera_vfs_compound_remove_at_path_callback(
    enum chimera_vfs_error    status,
    struct chimera_vfs_attrs *pre,
    struct chimera_vfs_attrs *post,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    chimera_vfs_release(compound->thread, op->out_handle);
    op->out_handle = NULL;
    compound_store_aux(&op->dir_pre_attr, pre);
    compound_store_aux(&op->dir_post_attr, post);
    chimera_vfs_compound_op_done(compound, status);
} /* chimera_vfs_compound_remove_at_path_callback */

static void
chimera_vfs_compound_remove_at_path_open(
    enum chimera_vfs_error          status,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (status != CHIMERA_VFS_OK) {
        chimera_vfs_compound_op_done(compound, status);
        return;
    }
    op->out_handle = handle;
    chimera_vfs_remove_at(compound->thread, compound->cred, handle,
                          op->name, op->name_len, NULL, 0, op->remove_flags,
                          compound_pre_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                          compound_post_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                          NULL, chimera_vfs_compound_remove_at_path_callback, compound);
} /* chimera_vfs_compound_remove_at_path_open */

static void
chimera_vfs_compound_remove_at_path_lookup(
    enum chimera_vfs_error    status,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_compound *compound = private_data;

    if (status != CHIMERA_VFS_OK) {
        chimera_vfs_compound_op_done(compound, status);
        return;
    }
    chimera_vfs_open_fh(compound->thread, compound->cred, attr->va_fh, attr->va_fh_len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                        CHIMERA_VFS_OPEN_DIRECTORY,
                        chimera_vfs_compound_remove_at_path_open, compound);
} /* chimera_vfs_compound_remove_at_path_lookup */

/*
 * Neither RENAME nor LINK moves the current object: both change a name IN it,
 * the way REMOVE does.  A caller that wants the result GETATTRs it, exactly as
 * it would have after the op-at-a-time call.
 *
 * RENAME reports both directories, because it changed both: from_dir_* is the
 * saved one it took the name from and dir_* the current one it put the name
 * in.  They are the same two change_infos NFSv4's RENAME reply carries.
 */
static void
chimera_vfs_compound_rename_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *fromdir_pre_attr,
    struct chimera_vfs_attrs *fromdir_post_attr,
    struct chimera_vfs_attrs *todir_pre_attr,
    struct chimera_vfs_attrs *todir_post_attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    compound_store_aux(&op->from_dir_pre_attr, fromdir_pre_attr);
    compound_store_aux(&op->from_dir_post_attr, fromdir_post_attr);
    compound_store_aux(&op->dir_pre_attr, todir_pre_attr);
    compound_store_aux(&op->dir_post_attr, todir_post_attr);

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_rename_callback */

/* LINK's callback also hands back the linked object's own attributes; the
 * directory pair is the current object's, as for RENAME's target half. */
static void
chimera_vfs_compound_link_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *r_attr,
    struct chimera_vfs_attrs *r_dir_pre_attr,
    struct chimera_vfs_attrs *r_dir_post_attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    compound_store_aux(&op->dir_pre_attr, r_dir_pre_attr);
    compound_store_aux(&op->dir_post_attr, r_dir_post_attr);
    if (r_attr) {
        chimera_vfs_compound_store_attr(op, r_attr);
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_link_callback */

static void
chimera_vfs_compound_allocate_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (error_code == CHIMERA_VFS_OK) {
        if (pre_attr) {
            op->dir_pre_attr = *pre_attr;
        }
        if (post_attr) {
            chimera_vfs_compound_store_attr(op, post_attr);
        }
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_allocate_callback */

static void
chimera_vfs_compound_seek_callback(
    enum chimera_vfs_error error_code,
    int                    sr_eof,
    uint64_t               sr_offset,
    void                  *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (error_code == CHIMERA_VFS_OK) {
        op->seek_offset = sr_offset;
        op->seek_eof    = (uint32_t) sr_eof;
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_seek_callback */

static void
chimera_vfs_compound_copy_range_callback(
    enum chimera_vfs_error    error_code,
    uint64_t                  length,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (error_code == CHIMERA_VFS_OK) {
        op->written = length;
        if (pre_attr) {
            op->dir_pre_attr = *pre_attr;
        }
        if (post_attr) {
            chimera_vfs_compound_store_attr(op, post_attr);
        }
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_copy_range_callback */

static void
chimera_vfs_compound_clone_range_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (error_code == CHIMERA_VFS_OK) {
        if (pre_attr) {
            op->dir_pre_attr = *pre_attr;
        }
        if (post_attr) {
            chimera_vfs_compound_store_attr(op, post_attr);
        }
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_clone_range_callback */

static void
chimera_vfs_compound_move_range_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *src_post_attr,
    struct chimera_vfs_attrs *dst_pre_attr,
    struct chimera_vfs_attrs *dst_post_attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (error_code == CHIMERA_VFS_OK) {
        /* The source's post-change attributes go where a name op keeps the
         * directory's, which a range op has no use for otherwise. */
        if (src_post_attr) {
            op->dir_post_attr = *src_post_attr;
        }
        if (dst_pre_attr) {
            op->dir_pre_attr = *dst_pre_attr;
        }
        if (dst_post_attr) {
            chimera_vfs_compound_store_attr(op, dst_post_attr);
        }
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_move_range_callback */

static void
chimera_vfs_compound_write_same_callback(
    enum chimera_vfs_error    error_code,
    uint64_t                  count,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (error_code == CHIMERA_VFS_OK) {
        op->written   = count;
        op->committed = sync;
        if (pre_attr) {
            op->dir_pre_attr = *pre_attr;
        }
        if (post_attr) {
            chimera_vfs_compound_store_attr(op, post_attr);
        }
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_write_same_callback */

static void
chimera_vfs_compound_read_plus_callback(
    enum chimera_vfs_error error_code,
    uint32_t               is_data,
    uint64_t               length,
    uint32_t               eof,
    void                  *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (error_code == CHIMERA_VFS_OK) {
        op->is_data  = is_data;
        op->read_len = (uint32_t) length;
        op->eof_read = eof;
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_read_plus_callback */

/*
 * A path op that resolved an object.  Its attributes are the op's result, and
 * when the caller asked for the file handle among them the object becomes
 * current -- so an fh-addressed op can follow on a backend that has file
 * handles.  When it did not, the current object stays put, which is the only
 * thing a path-only mount can support.
 */
static void
chimera_vfs_compound_path_attr_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_op_done(compound, error_code);
        return;
    }

    chimera_vfs_compound_store_attr(op, attr);

    if (attr && (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        chimera_vfs_compound_set_current(compound, attr->va_fh,
                                         attr->va_fh_len);
    }

    chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
} /* chimera_vfs_compound_path_attr_callback */

static void
chimera_vfs_compound_path_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_op_done(compound, error_code);
        return;
    }

    /* Owned by the compound until taken -- see OPEN HANDLE OWNERSHIP.  On a
     * path-only mount this handle is the ONLY usable reference to what the
     * path resolved; chimera_vfs_compound_op_use_handle is how the op after
     * this one reaches it. */
    op->out_handle = oh;
    op->created    = oh ? oh->r_created : 0;

    if (attr) {
        chimera_vfs_compound_store_attr(op, attr);
    }

    if (oh) {
        /* Moves the FH cursor, which clears the open cursor -- so the open
         * cursor is set after, not before. */
        chimera_vfs_compound_set_current(compound, oh->fh, oh->fh_len);

        /* A path OPEN is an OPEN: what it produced becomes the current open
         * handle, so the op after it just reads the cursor.  On a path-only
         * mount this is the ONLY usable reference to what the path resolved,
         * the object itself having no re-openable file handle. */
        compound->handle          = oh;
        compound->handle_flags    = op->open_flags;
        compound->handle_explicit = 1;

        /* The OP owns this handle, via out_handle, the way a named OPEN's
         * does -- so the cursor addresses it without owning it.  Marking it
         * taken is what keeps exactly one owner: the cursor and out_handle are
         * both released at teardown, and a handle in both is released twice. */
        compound->handle_taken  = 1;
        compound->handle_origin = compound->index + 1;
    }

    chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
} /* chimera_vfs_compound_path_open_callback */

static void
chimera_vfs_compound_create_unlinked_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    (void) set_attr;
    chimera_vfs_compound_path_open_callback(error_code, handle, attr,
                                            private_data);
} /* chimera_vfs_compound_create_unlinked_callback */

static void
chimera_vfs_compound_path_status_callback(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_vfs_compound *compound = private_data;

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_path_status_callback */

static void
chimera_vfs_compound_remove_paths_callback(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (status != CHIMERA_VFS_OK && !op->remove_ignore_errors) {
        chimera_vfs_compound_op_done(compound, status);
        return;
    }
    op->remove_path_index++;
    chimera_vfs_compound_step(compound);
} /* chimera_vfs_compound_remove_paths_callback */

static int
chimera_vfs_compound_find_filter(
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *attr,
    void                           *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    return op->find_filter ? op->find_filter(path, pathlen, attr, op->find_private) : 0;
} /* chimera_vfs_compound_find_filter */

static int
chimera_vfs_compound_find_append(
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *attr,
    void                           *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    return op->find_append ? op->find_append(path, pathlen, attr, op->find_private) : 0;
} /* chimera_vfs_compound_find_append */

static void
chimera_vfs_compound_open_current_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_op_done(compound, error_code);
        return;
    }

    /* Whatever the slot held goes first, on the ordinary rules. */
    chimera_vfs_compound_release_cursor(compound);

    compound->handle          = handle;
    compound->handle_flags    = op->open_flags;
    compound->handle_explicit = 1;

    /* NOT recorded as out_handle.  The CURSOR owns this handle, and teardown
     * releases the cursor and every op's out_handle -- so putting it in both
     * places releases it twice, which frees a live handle and corrupts the
     * open cache for whoever takes that slot next.  GETHANDLE is what
     * publishes it, and it hands ownership over at the same time. */

    chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
} /* chimera_vfs_compound_open_current_callback */

/*
 * The flags this op needs the current object opened with, or 0 if it addresses
 * the current object without a handle at all.
 *
 * LOOKUP, LOOKUPP and READDIR ask for a directory open, which is what every
 * protocol's own versions of them do today: reaching through a non-directory
 * then fails on the open, uniformly, instead of on however a particular
 * backend's lookup_at chooses to report a non-directory parent.
 *
 * COMMIT and the xattr ops ask for a *data* open (no CHIMERA_VFS_OPEN_PATH),
 * again matching what those operations do outside a sequence.
 *
 * OPEN is the one op whose answer depends on its arguments rather than its
 * type: a named open resolves through the current object as a directory, while
 * an unnamed one re-opens the current object itself and so needs no handle on
 * it at all.
 */
static unsigned int
chimera_vfs_compound_op_open_flags(const struct chimera_vfs_compound_op *op)
{
    /* An op that brought its own handle needs nothing opened for it -- nor
     * does one taking the handle an earlier op produced, nor a path op, which
     * addresses a raw file handle and a path rather than the current object. */
    if (op->in_handle || op->handle_from >= 0 || (op->path && op->type != CHIMERA_VFS_COMPOUND_OP_OPEN)) {
        return 0;
    }

    switch (op->type) {
        case CHIMERA_VFS_COMPOUND_OP_OPEN:
            return (op->name_len || op->path) ? (CHIMERA_VFS_OPEN_INFERRED |
                                                 CHIMERA_VFS_OPEN_PATH |
                                                 CHIMERA_VFS_OPEN_DIRECTORY) : 0;
        case CHIMERA_VFS_COMPOUND_OP_LOOKUP:
        case CHIMERA_VFS_COMPOUND_OP_LOOKUPP:
        case CHIMERA_VFS_COMPOUND_OP_READDIR:
        case CHIMERA_VFS_COMPOUND_OP_CREATE:
        case CHIMERA_VFS_COMPOUND_OP_REMOVE:
            return CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                   CHIMERA_VFS_OPEN_DIRECTORY;
        case CHIMERA_VFS_COMPOUND_OP_GETATTR:
        case CHIMERA_VFS_COMPOUND_OP_ACCESS:
        case CHIMERA_VFS_COMPOUND_OP_READLINK:
            return CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH;
        case CHIMERA_VFS_COMPOUND_OP_OVERWRITE:
            return CHIMERA_VFS_OPEN_INFERRED;
        case CHIMERA_VFS_COMPOUND_OP_SETATTR:
            /* Size changes require a data descriptor for ftruncate. Replacing
            * an already-open data cursor with O_PATH loses its write rights
            * and makes backend fallback truncate recheck the current mode. */
            return CHIMERA_VFS_OPEN_INFERRED |
                   ((op->applied_attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE) ? 0 : CHIMERA_VFS_OPEN_PATH);
        case CHIMERA_VFS_COMPOUND_OP_READ:
        case CHIMERA_VFS_COMPOUND_OP_WRITE:
            /* The type check comes first, through a PATH open; the data then
             * comes through a DATA one.  They are different handles from
             * different caches, so the sequence opens twice -- exactly as the
             * per-op path does, and for the same reason. */
            if (!op->io_typechecked_flag) {
                return CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH;
            }
            return op->type == CHIMERA_VFS_COMPOUND_OP_READ ?
                   (CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_READ_ONLY) :
                   CHIMERA_VFS_OPEN_INFERRED;
        /* RENAME and LINK address two objects by file handle -- the saved slot
         * and the current one -- and rename_at/link_at take handles, so
         * neither wants the current object opened at all. */
        case CHIMERA_VFS_COMPOUND_OP_RENAME:
        case CHIMERA_VFS_COMPOUND_OP_LINK:
            return 0;
        /* COMMIT flushes file data; ALLOCATE changes it; SEEK reads the map
         * that describes it.  All three want the data open. */
        case CHIMERA_VFS_COMPOUND_OP_COMMIT:
        case CHIMERA_VFS_COMPOUND_OP_ALLOCATE:
        case CHIMERA_VFS_COMPOUND_OP_SEEK:
        case CHIMERA_VFS_COMPOUND_OP_WRITE_SAME:
            return CHIMERA_VFS_OPEN_INFERRED;
        case CHIMERA_VFS_COMPOUND_OP_READ_PLUS:
            return CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_READ_ONLY;
        /* The range ops bring both of their objects; there is no current one
         * for them to want opened.  in_handle already makes this 0, so this
         * arm only says so out loud. */
        case CHIMERA_VFS_COMPOUND_OP_COPY_RANGE:
        case CHIMERA_VFS_COMPOUND_OP_CLONE_RANGE:
        case CHIMERA_VFS_COMPOUND_OP_MOVE_RANGE:
            return 0;
        /* The xattr ops are metadata: they never touch the object's data, and
         * a PATH open is the one that works on every type.  A data open of a
         * FIFO blocks until a peer arrives, and of a directory is refused
         * outright on some backends -- both are objects that can carry
         * xattrs, so asking for data here would make setting an attribute on
         * one hang or fail for no reason connected to the attribute. */
        case CHIMERA_VFS_COMPOUND_OP_OPEN_STREAM:
        case CHIMERA_VFS_COMPOUND_OP_LIST_STREAMS:
        case CHIMERA_VFS_COMPOUND_OP_REMOVE_STREAM:
            return CHIMERA_VFS_OPEN_INFERRED;
        case CHIMERA_VFS_COMPOUND_OP_GETXATTR:
        case CHIMERA_VFS_COMPOUND_OP_SETXATTR:
        case CHIMERA_VFS_COMPOUND_OP_LISTXATTRS:
        case CHIMERA_VFS_COMPOUND_OP_REMOVEXATTR:
            return CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH;
        default:
            return 0;
    } /* switch */
} /* chimera_vfs_compound_op_open_flags */

/*
 * Can the handle we are holding serve an op that wants `want`?
 *
 * Only if it carries every flag `want` asks for AND sits on the same side of
 * CHIMERA_VFS_OPEN_PATH.  The second half is not pedantry: a path open and a
 * data open come out of two different open caches and are two different things
 * to a backend (O_PATH versus a real descriptor), so a data op offered a path
 * handle is not being given "more than it asked for", it is being given the
 * wrong handle -- and by the subset rule alone it would take it, because a data
 * open's flags are a subset of a path open's.
 */
static int
chimera_vfs_compound_handle_serves(
    unsigned int have,
    unsigned int want)
{
    if (want & ~have) {
        return 0;
    }

    return ((have ^ want) & CHIMERA_VFS_OPEN_PATH) == 0;
} /* chimera_vfs_compound_handle_serves */

static void
compound_range_complete(const struct chimera_vfs_claim_batch_result *result, void *private_data)
{
    struct chimera_vfs_compound *compound = private_data;
    struct chimera_vfs_compound_op *op = compound->ops[compound->index];
    op->range_result = *result;
    chimera_vfs_compound_op_done(compound, result->status);
}

static int
compound_search_keys_entry(const void *key, uint32_t key_len,
                           const void *value, uint32_t value_len, void *private_data)
{
    struct chimera_vfs_compound *compound = private_data;
    struct chimera_vfs_compound_op *op = compound->ops[compound->index];
    uint64_t bytes = (uint64_t) key_len + value_len;
    if (op->kv_error != CHIMERA_VFS_OK || op->kv_more) return 1;
    if (key_len > 4096) { op->kv_error = CHIMERA_VFS_ERANGE; return 1; }
    if (op->kv_num_entries == op->kv_max_entries || bytes > op->kv_max_bytes - op->kv_result_bytes) {
        if (!op->kv_num_entries) { op->kv_error = CHIMERA_VFS_ERANGE; return 1; }
        op->kv_next_key = malloc(key_len ? key_len : 1);
        if (!op->kv_next_key) { op->kv_error = CHIMERA_VFS_ENOSPC; return 1; }
        if (key_len) memcpy(op->kv_next_key, key, key_len);
        op->kv_next_key_len = key_len;
        op->kv_more = true;
        return 1;
    }
    uint8_t *copy = malloc(bytes ? (size_t) bytes : 1);
    if (!copy) { op->kv_error = CHIMERA_VFS_ENOSPC; return 1; }
    if (key_len) memcpy(copy, key, key_len);
    if (value_len) memcpy(copy + key_len, value, value_len);
    op->kv_entries[op->kv_num_entries++] = (struct chimera_vfs_compound_kv_entry) {
        .key = copy, .key_len = key_len, .value = copy + key_len, .value_len = value_len };
    op->kv_result_bytes += (uint32_t) bytes;
    return 0;
}

static void
compound_search_keys_complete(enum chimera_vfs_error status, void *private_data)
{
    struct chimera_vfs_compound *compound = private_data;
    struct chimera_vfs_compound_op *op = compound->ops[compound->index];
    if (op->kv_error != CHIMERA_VFS_OK) status = op->kv_error;
    chimera_vfs_compound_op_done(compound, status);
}

static bool
compound_range_is_canceled(void *private_data)
{
    struct chimera_vfs_compound *compound = private_data;
    struct chimera_vfs_compound_op *op = compound->ops[compound->index];
    return op->range_is_canceled &&
           op->range_is_canceled(compound, compound->index, op->range_wait_private);
}

static void
compound_range_wait(void *private_data)
{
    struct chimera_vfs_compound *compound = private_data;
    struct chimera_vfs_compound_op *op = compound->ops[compound->index];
    if (op->range_on_wait) {
        op->range_on_wait(compound, compound->index, op->range_wait_private);
    }
}

/* Each endpoint keeps its own array alive through asynchronous backend I/O. */
static bool
compound_journal_view(struct chimera_vfs_compound *compound,
                      const struct chimera_vfs_open_handle *target,
                      struct chimera_vfs_io_view *view,
                      const struct chimera_vfs_claim ***storage)
{
    if (!compound->claim_journal || !target) return true;
    struct chimera_vfs_state *state = compound->thread->vfs->vfs_state;
    struct chimera_vfs_file_state *file = chimera_vfs_state_get(state, target->fh,
        target->fh_len, target->fh_hash, false);
    uint32_t count = file ? chimera_vfs_claim_journal_excluded(
        compound->claim_journal, file, NULL, 0) : 0;
    bool ok = true;
    if (count) {
        *storage = calloc((size_t) count + view->num_excluded, sizeof(**storage));
        if (!*storage) {
            ok = false;
        } else {
            for (uint32_t i = 0; i < view->num_excluded; i++) (*storage)[i] = view->excluded[i];
            chimera_vfs_claim_journal_excluded(compound->claim_journal, file,
                *storage + view->num_excluded, count);
            view->excluded = *storage;
            view->num_excluded += count;
        }
    }
    if (file) chimera_vfs_state_put(state, file);
    return ok;
}

static void chimera_vfs_compound_step_once(
    struct chimera_vfs_compound *compound);

/* Synchronous backends may complete an entire request inline. Trampoline the
 * continuation so large dynamically built compounds do not consume one C
 * stack frame per operation. Deliver final completion outside this loop: the
 * frontend may free the compound or retry it from that callback. */
static void
chimera_vfs_compound_lock_complete(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    chimera_vfs_lock_attempt_result(op->lock_attempt, &op->claim_conflict, &op->lock_pid);
    chimera_vfs_compound_op_done(compound, status);
} /* chimera_vfs_compound_lock_complete */

static void
chimera_vfs_compound_step(struct chimera_vfs_compound *compound)
{
    if (compound->stepping) {
        compound->step_pending = 1;
        return;
    }
    compound->stepping = 1;
    do {
        compound->step_pending = 0;
        chimera_vfs_compound_step_once(compound);
        if (compound->finish_pending) {
            compound->finish_pending = 0;
            compound->stepping       = 0;
            chimera_vfs_compound_finish_dispatch(compound);
            return;
        }
    } while (compound->step_pending);
    compound->stepping = 0;
} /* chimera_vfs_compound_step */

static void
chimera_vfs_compound_step_once(struct chimera_vfs_compound *compound)
{
    struct chimera_vfs_compound_op *op;
    struct chimera_vfs_open_handle *target, *range_src, *range_dst;
    unsigned int                    open_flags;

    if (compound_cancel_stops(compound)) {
        if (compound->num_groups && compound->group_index < compound->num_groups) {
            chimera_vfs_compound_group_done(compound, CHIMERA_VFS_EINTR);
            return;
        }
        chimera_vfs_compound_finish(compound, CHIMERA_VFS_EINTR);
        return;
    }
    if (compound->num_groups && compound->group_index < compound->num_groups &&
        !compound->group_active) {
        struct chimera_vfs_compound_group *group = &compound->groups[compound->group_index];
        compound->group_active = 1;
        chimera_vfs_compound_release_cursor(compound);
        chimera_vfs_compound_release_saved(compound);
        compound->fh_len = compound->saved_fh_len = 0;
        compound->handle_flags = compound->saved_handle_flags = 0;
        compound->cred = group->config.cred ? group->config.cred : compound->default_cred;
        if (group->config.dependency >= 0 &&
            compound->groups[group->config.dependency].status != CHIMERA_VFS_OK) {
            chimera_vfs_compound_group_done(compound, group->config.dependency_error);
            return;
        }
    }
    if (compound->index >= compound->num_ops) {
        chimera_vfs_compound_finish(compound, CHIMERA_VFS_OK);
        return;
    }

    op = compound->ops[compound->index];

    if (!op->prepared) {
        enum chimera_vfs_error status = CHIMERA_VFS_OK;
        op->prepared        = 1;
        compound->preparing = 1;
        if (op->prepare) {
            op->prepare(compound, compound->index, &status,
                        op->prepare_private);
        }
        compound->preparing = 0;
        if (compound_cancel_stops(compound) && status == CHIMERA_VFS_OK) {
            status = CHIMERA_VFS_EINTR;
        }
        op->applied_attr    = op->set_attr;
        if (compound->build_failed && status == CHIMERA_VFS_OK) {
            status = compound->build_error ? compound->build_error : CHIMERA_VFS_EINVAL;
        }
        if (status != CHIMERA_VFS_OK || op->skipped) {
            chimera_vfs_compound_op_done(compound, status);
            return;
        }
        if ((op->set_attr.va_set_mask & CHIMERA_VFS_ATTR_ACL) &&
            op->set_attr.va_acl) {
            size_t size = chimera_acl_size(op->set_attr.va_acl->num_aces);
            op->applied_acl = malloc(size);
            if (!op->applied_acl) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_ENOSPC);
                return;
            }
            memcpy(op->applied_acl, op->set_attr.va_acl, size);
            op->applied_attr.va_acl = op->applied_acl;
        }
    }

    /* Group order need not match physical indices. A dependent handle must
     * come from a successfully completed operation, never a skipped group. */
    if (!op->in_handle && op->handle_from >= 0 &&
        ((uint32_t) op->handle_from >= compound->num_ops ||
         !compound->ops[op->handle_from]->completed ||
         compound->ops[op->handle_from]->status != CHIMERA_VFS_OK ||
         !compound->ops[op->handle_from]->out_handle)) {
        chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
        return;
    }

    const struct chimera_vfs_cred *namespace_cred =
        (op->type == CHIMERA_VFS_COMPOUND_OP_OPEN_CURRENT || op->type == CHIMERA_VFS_COMPOUND_OP_REMOVE) &&
        op->namespace_cred ? op->namespace_cred : compound->cred;

    /* The object this op acts on: the one the caller handed in, or else the
     * sequence's current object.  Ops that resolve a NAME use compound->handle
     * directly instead, because for them it is the directory to resolve in and
     * not the object being acted on. */
    range_src = op->src_handle ? op->src_handle : compound->saved_handle;
    range_dst = op->in_handle ? op->in_handle : compound->handle;

    target = op->in_handle ? op->in_handle :
        (op->handle_from >= 0 ? compound->ops[op->handle_from]->out_handle :
         compound->handle);

    /* op_open_flags cannot see the sequence, so tell it where the two-step I/O
     * has got to. */
    op->io_typechecked_flag = compound->io_typechecked;

    open_flags = chimera_vfs_compound_op_open_flags(op);

    if (open_flags) {
        if (compound->fh_len == 0) {
            /* No current object: the sequence addressed one before naming
             * one.  A caller builds these itself, so this is its bug. */
            chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
            return;
        }

        /* handle_serves is not advisory: a path open and a data open come out
         * of different caches and are different things to a backend, so a
         * handle that does not serve must be re-opened even when the caller
         * chose it.  What the caller chose it for was the op it chose it for,
         * not this one. */
        if (!compound->handle ||
            !chimera_vfs_compound_handle_serves(compound->handle_flags,
                                                open_flags)) {
            /* Open the current object once; every op that follows on the same
             * object reuses this handle -- unless it needs flags the handle
             * was not opened with, in which case it is re-opened. */
            /* A borrowed handle that cannot serve this op is left alone -- it
             * is the caller's -- and the sequence opens its own. */
            chimera_vfs_compound_release_cursor(compound);

            compound->handle_flags = open_flags;

            chimera_vfs_open_fh(compound->thread, namespace_cred,
                                compound->fh, (int) compound->fh_len,
                                open_flags,
                                chimera_vfs_compound_open_callback, compound);
            return;
        }
    }

    switch (op->type) {
        case CHIMERA_VFS_COMPOUND_OP_PUTFH:
            chimera_vfs_compound_set_current(compound, op->arg_fh,
                                             op->arg_fh_len);
            chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
            break;

        case CHIMERA_VFS_COMPOUND_OP_PUT_KEY_AT:
        case CHIMERA_VFS_COMPOUND_OP_DELETE_KEY_AT:
            if (!compound->fh_len) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
            } else if (op->type == CHIMERA_VFS_COMPOUND_OP_PUT_KEY_AT) {
                chimera_vfs_put_key_at(compound->thread, compound->cred,
                    compound->fh, compound->fh_len, op->kv_key, op->kv_key_len,
                    op->kv_value, op->kv_value_len,
                    chimera_vfs_compound_path_status_callback, compound);
            } else {
                chimera_vfs_delete_key_at(compound->thread, compound->cred,
                    compound->fh, compound->fh_len, op->kv_key, op->kv_key_len,
                    chimera_vfs_compound_path_status_callback, compound);
            }
            break;

        case CHIMERA_VFS_COMPOUND_OP_SEARCH_KEYS_AT:
            if (!compound->fh_len) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }
            op->kv_entries = calloc(op->kv_max_entries, sizeof(*op->kv_entries));
            if (!op->kv_entries) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_ENOSPC);
                break;
            }
            chimera_vfs_search_keys_at(compound->thread, compound->cred,
                compound->fh, compound->fh_len, op->kv_key, op->kv_key_len,
                op->kv_value, op->kv_value_len, op->kv_flags,
                compound_search_keys_entry, compound_search_keys_complete, compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_CHECKPOINT:
            chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
            break;

        case CHIMERA_VFS_COMPOUND_OP_COORDINATE:
            chimera_vfs_compound_coordinate(compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_GETFH:
            if (compound->fh_len == 0) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }
            /* The fh is recorded for every op below; GETFH exists so the
             * caller has an index to read it from. */
            chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
            break;

        case CHIMERA_VFS_COMPOUND_OP_SAVEFH:
            if (compound->fh_len == 0) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }
            /* A handle copy, not a handle: see the SAVED SLOT note above. */
            memcpy(compound->saved_fh, compound->fh, compound->fh_len);
            compound->saved_fh_len = compound->fh_len;
            chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
            break;

        case CHIMERA_VFS_COMPOUND_OP_RESTOREFH:
            if (compound->saved_fh_len == 0) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }
            /* set_current releases whatever handle the old current object had;
             * the saved slot never held one, so nothing is released twice and
             * nothing is left behind. */
            chimera_vfs_compound_set_current(compound, compound->saved_fh,
                                             compound->saved_fh_len);
            chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
            break;

        case CHIMERA_VFS_COMPOUND_OP_OPEN:
            if (op->inherited_grant_handle &&
                (op->name_len || op->path ||
                 op->inherited_grant_handle->fh_len != compound->fh_len ||
                 memcmp(op->inherited_grant_handle->fh, compound->fh,
                        compound->fh_len) != 0)) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }
            if (op->inherited_grant_handle2 &&
                (op->name_len || op->path ||
                 op->inherited_grant_handle2->fh_len != compound->fh_len ||
                 memcmp(op->inherited_grant_handle2->fh, compound->fh,
                        compound->fh_len) != 0)) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }
            if (op->name_len == 0 && !op->path) {
                /* Re-open the current object by handle. */
                if (compound->fh_len == 0) {
                    chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                    break;
                }
                chimera_vfs_open_fh(compound->thread, compound->cred,
                                    compound->fh, (int) compound->fh_len,
                                    op->open_flags,
                                    chimera_vfs_compound_open_fh_callback,
                                    compound);
                break;
            }

            if (!compound->open_resolved &&
                (op->open_opts & (CHIMERA_VFS_COMPOUND_OPEN_REGULAR_ONLY |
                                  CHIMERA_VFS_COMPOUND_OPEN_ATTRS_ON_CREATE_ONLY))) {
                /* Resolve the name before opening it -- step one of two. */
                chimera_vfs_lookup_at(compound->thread, compound->cred,
                                      target,
                                      op->path ? op->path : op->name, op->name_len,
                                      CHIMERA_VFS_ATTR_MODE,
                                      0,
                                      chimera_vfs_compound_open_resolve_callback,
                                      compound);
                break;
            }

            chimera_vfs_open_at(compound->thread, compound->cred,
                                target,
                                op->path ? op->path : op->name, op->name_len,
                                op->open_flags,
                                &op->applied_attr,
                                compound_object_mask(op, op->attr_mask) | CHIMERA_VFS_ATTR_FH,
                                compound_pre_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                                compound_post_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                                chimera_vfs_compound_open_at_callback,
                                compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_CREATE:
            switch (op->create_type) {
                case CHIMERA_VFS_COMPOUND_CREATE_DIR:
                    chimera_vfs_mkdir_at_flags(compound->thread, compound->cred,
                                         target,
                                         op->name, op->name_len,
                                         &op->applied_attr, op->namespace_flags,
                                         compound_object_mask(op, op->attr_mask) | CHIMERA_VFS_ATTR_FH,
                                         compound_pre_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                                         compound_post_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                                         chimera_vfs_compound_create_callback,
                                         compound);
                    break;
                case CHIMERA_VFS_COMPOUND_CREATE_SYMLINK:
                    chimera_vfs_symlink_at_flags(compound->thread, compound->cred,
                                           target,
                                           op->name, op->name_len,
                                           op->link_target,
                                           (int) op->link_target_len,
                                           &op->applied_attr, op->namespace_flags,
                                           compound_object_mask(op, op->attr_mask) | CHIMERA_VFS_ATTR_FH,
                                           compound_pre_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                                           compound_post_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                                           chimera_vfs_compound_symlink_callback,
                                           compound);
                    break;
                default:
                    chimera_vfs_mknod_at_flags(compound->thread, compound->cred,
                                         target,
                                         op->name, op->name_len,
                                         &op->applied_attr, op->namespace_flags,
                                         compound_object_mask(op, op->attr_mask) | CHIMERA_VFS_ATTR_FH,
                                         compound_pre_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                                         compound_post_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                                         chimera_vfs_compound_create_callback,
                                         compound);
                    break;
            } /* switch */
            break;

        case CHIMERA_VFS_COMPOUND_OP_READ:
        case CHIMERA_VFS_COMPOUND_OP_WRITE:
        {
            /* An op addressing the current object establishes the object's
             * type before it is opened for data -- so a non-regular one is
             * refused here, and the data open is never attempted.  An op that
             * brought a data handle needs none of this. An explicit PATH
             * handle (including PUTFH validation) has not checked the type. */
            if (!op->in_handle &&
                (!compound->handle_explicit ||
                 (compound->handle_flags & CHIMERA_VFS_OPEN_PATH)) &&
                !compound->io_typechecked) {
                chimera_vfs_getattr(compound->thread, compound->cred,
                                    target,
                                    CHIMERA_VFS_ATTR_MODE,
                                    chimera_vfs_compound_io_type_callback,
                                    compound);
                break;
            }

            struct chimera_vfs_io_view view = op->io_view;
            view.owner = op->have_io_owner ? &op->io_owner : NULL;
            if (!compound_journal_view(compound, target, &view, &op->journal_excluded)) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_ENOSPC);
                break;
            }
            if (op->type == CHIMERA_VFS_COMPOUND_OP_READ) {
                chimera_vfs_read_view(compound->thread, compound->cred, target,
                                      op->offset, op->count, op->iov, op->max_iov,
                                      compound_object_mask(op, 0), &view, chimera_vfs_compound_read_callback, compound);
            } else {
                chimera_vfs_write_view(compound->thread, compound->cred, target,
                                       op->offset, op->count, op->sync,
                                       compound_pre_mask(op, 0), compound_post_mask(op, 0),
                                       op->w_iov, op->w_niov, &view,
                                       chimera_vfs_compound_write_callback, compound);
            }
            break;
        }

        case CHIMERA_VFS_COMPOUND_OP_OVERWRITE:
            chimera_vfs_overwrite(compound->thread, compound->cred, target,
                                  &op->applied_attr,
                                  compound_post_mask(op, op->attr_mask),
                                  op->have_io_owner ? &op->io_owner : NULL,
                                  chimera_vfs_compound_setattr_callback, compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_SETATTR:
            /* With the caller's own handle the change is authorized by that
             * open's grant rather than re-checked against the object's mode --
             * the difference between ftruncate(2) and truncate(2), and the
             * whole reason a caller hands a handle in. */
            if (op->have_io_owner) {
                if (op->in_handle) {
                    chimera_vfs_fsetattr_owned(compound->thread, compound->cred,
                        op->in_handle, &op->applied_attr,
                        compound_pre_mask(op, 0), compound_post_mask(op, op->attr_mask),
                        &op->io_owner, chimera_vfs_compound_setattr_callback, compound);
                } else {
                    chimera_vfs_setattr_owned(compound->thread, compound->cred,
                        target, &op->applied_attr,
                        compound_pre_mask(op, 0), compound_post_mask(op, op->attr_mask),
                        &op->io_owner, chimera_vfs_compound_setattr_callback, compound);
                }
            } else if (op->in_handle) {
                chimera_vfs_fsetattr(compound->thread, compound->cred,
                                     op->in_handle,
                                     &op->applied_attr,
                                     compound_pre_mask(op, 0), compound_post_mask(op, op->attr_mask),
                                     chimera_vfs_compound_setattr_callback,
                                     compound);
            } else {
                chimera_vfs_setattr(compound->thread, compound->cred,
                                    target,
                                    &op->applied_attr,
                                    compound_pre_mask(op, 0), compound_post_mask(op, op->attr_mask),
                                    chimera_vfs_compound_setattr_callback,
                                    compound);
            }
            break;

        case CHIMERA_VFS_COMPOUND_OP_REMOVE:
            if (op->remove_match_child_fh) {
                chimera_vfs_remove_at_match_fh_flags(compound->thread, namespace_cred,
                    target, op->name, op->name_len, op->arg_fh, op->arg_fh_len, op->remove_flags,
                    compound_pre_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                    compound_post_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                    op->namespace_parent_lease_key_valid ? op->namespace_parent_lease_key : NULL,
                    op->have_io_owner ? &op->io_owner : NULL, &op->remove_unmatched,
                    chimera_vfs_compound_remove_callback, compound);
            } else {
                chimera_vfs_remove_at(compound->thread, namespace_cred,
                    target, op->name, op->name_len,
                    op->arg_fh_len ? op->arg_fh : NULL, op->arg_fh_len, op->remove_flags,
                    compound_pre_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                    compound_post_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                    op->namespace_parent_lease_key_valid ? op->namespace_parent_lease_key : NULL,
                    chimera_vfs_compound_remove_callback, compound);
            }
            break;

        /* Source from the SAVED slot, target from the current object.  Both
         * take file handles, so neither needs the saved object opened -- which
         * is the whole reason the saved slot can hold a bare handle.
         *
         * An unset saved slot is EINVAL, the same answer RESTOREFH gives it:
         * the adder cannot tell, because whether a SAVEFH ran is a property of
         * the sequence as it executes and not of the op being added. */
        case CHIMERA_VFS_COMPOUND_OP_RENAME:
            if (compound->saved_fh_len == 0) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }
            chimera_vfs_rename_at_checked_result_actor(compound->thread, compound->cred,
                                  compound->saved_fh, compound->saved_fh_len,
                                  op->name, op->name_len,
                                  compound->fh, compound->fh_len,
                                  op->new_name, op->new_name_len,
                                  op->rename_target_fh_len ? op->rename_target_fh : NULL,
                                  op->rename_target_fh_len, op->remove_flags,
                                  compound_pre_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                                  compound_post_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                                  op->namespace_parent_lease_key_valid ? op->namespace_parent_lease_key : NULL,
                                  op->have_io_owner ? op->io_owner.op_handle : NULL,
                                  op->have_io_owner ? &op->io_owner : NULL,
                                  (op->remove_flags & CHIMERA_VFS_RENAME_MATCH_SOURCE_FH) ? op->arg_fh : NULL,
                                  (op->remove_flags & CHIMERA_VFS_RENAME_MATCH_SOURCE_FH) ? op->arg_fh_len : 0,
                                  &op->rename_outcome,
                                  chimera_vfs_compound_rename_callback,
                                  compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_LINK:
            if (compound->saved_fh_len == 0) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }
            chimera_vfs_link_at_flags_actor(compound->thread, compound->cred,
                                compound->saved_fh, compound->saved_fh_len,
                                compound->fh, compound->fh_len,
                                op->name, op->name_len,
                                op->open_opts, op->namespace_flags, compound_object_mask(op, op->attr_mask),
                                compound_pre_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                                compound_post_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                                op->namespace_parent_lease_key_valid ? op->namespace_parent_lease_key : NULL,
                                op->have_io_owner ? op->io_owner.op_handle : NULL,
                                op->have_io_owner ? &op->io_owner : NULL,
                                chimera_vfs_compound_link_callback,
                                compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_LOOKUP:
            chimera_vfs_lookup_at(compound->thread, compound->cred,
                                  compound->handle,
                                  op->name, op->name_len,
                                  compound_object_mask(op, op->attr_mask) | CHIMERA_VFS_ATTR_FH,
                                  compound_post_mask(op, 0),
                                  chimera_vfs_compound_lookup_callback,
                                  compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_LOOKUPP:
            /* ".." through the open directory: the parent, by the one route
             * every backend supports.  The result chains exactly as a LOOKUP's
             * does, so they share a completion. */
            chimera_vfs_lookup_at(compound->thread, compound->cred,
                                  compound->handle,
                                  "..", 2,
                                  compound_object_mask(op, op->attr_mask) | CHIMERA_VFS_ATTR_FH,
                                  compound_post_mask(op, 0),
                                  chimera_vfs_compound_lookup_callback,
                                  compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_COMMIT:
            chimera_vfs_commit(compound->thread, compound->cred,
                               target,
                               op->offset, op->count,
                               compound_pre_mask(op, op->attr_mask), compound_post_mask(op, 0),
                               chimera_vfs_compound_commit_callback,
                               compound);
            break;

        /* Two open objects: the source from the SAVED open cursor, the
         * destination from the current one -- the same shape RENAME and LINK
         * have one level down, where the two operands are file handles.  The
         * adder arguments are still honoured while the front ends move over. */
        case CHIMERA_VFS_COMPOUND_OP_COPY_RANGE:
        {
            struct chimera_vfs_io_view src_view = op->src_io_view;
            struct chimera_vfs_io_view dst_view = op->io_view;
            src_view.owner = op->have_src_io_owner ? &op->src_io_owner : NULL;
            dst_view.owner = op->have_io_owner ? &op->io_owner : NULL;
            if (!range_src || !range_dst) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }
            if (!compound_journal_view(compound, range_src, &src_view, &op->journal_src_excluded) ||
                !compound_journal_view(compound, range_dst, &dst_view, &op->journal_excluded)) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_ENOSPC);
                break;
            }
            chimera_vfs_copy_range_view(compound->thread, compound->cred,
                                        range_src, op->src_offset,
                                        range_dst, op->offset,
                                        op->length, op->copy_flags,
                                        op->attr_mask, op->post_attr_mask,
                                        &src_view, &dst_view,
                                        chimera_vfs_compound_copy_range_callback,
                                        compound);
            break;
        }

        case CHIMERA_VFS_COMPOUND_OP_CLONE_RANGE:
            if (!range_src || !range_dst) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }
            if (op->have_io_owner && op->have_src_io_owner) {
                chimera_vfs_clone_range_owned(compound->thread, compound->cred,
                    range_src, op->src_offset, range_dst, op->offset,
                    op->length, op->attr_mask, op->post_attr_mask,
                    &op->src_io_owner, &op->io_owner,
                    chimera_vfs_compound_clone_range_callback, compound);
                break;
            }
            chimera_vfs_clone_range(compound->thread, compound->cred,
                                    range_src, op->src_offset,
                                    range_dst, op->offset,
                                    op->length,
                                    op->attr_mask, op->post_attr_mask,
                                    chimera_vfs_compound_clone_range_callback,
                                    compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_MOVE_RANGE:
            if (!range_src || !range_dst) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }
            chimera_vfs_move_range(compound->thread, compound->cred,
                                   range_src, op->src_offset,
                                   range_dst, op->offset,
                                   op->length,
                                   op->requested,
                                   op->attr_mask, op->post_attr_mask,
                                   chimera_vfs_compound_move_range_callback,
                                   compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_WRITE_SAME:
            chimera_vfs_write_same(compound->thread, compound->cred,
                                   target,
                                   op->offset,
                                   op->block_size, op->block_count,
                                   op->pattern, op->pattern_len,
                                   op->reloff_pattern, op->sync,
                                   op->attr_mask, op->post_attr_mask,
                                   chimera_vfs_compound_write_same_callback,
                                   compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_READ_PLUS:
            chimera_vfs_read_plus(compound->thread, compound->cred,
                                  target,
                                  op->offset, op->length,
                                  chimera_vfs_compound_read_plus_callback,
                                  compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_PUTROOT:
        {
            uint8_t  root_fh[CHIMERA_VFS_FH_SIZE];
            uint32_t root_fh_len = 0;

            chimera_vfs_get_root_fh(root_fh, &root_fh_len);

            chimera_vfs_compound_set_current(compound, root_fh, root_fh_len);
            chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
            break;
        }

        case CHIMERA_VFS_COMPOUND_OP_OPEN_CURRENT:
            if (compound->fh_len == 0) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }

            chimera_vfs_open_fh(compound->thread, namespace_cred,
                                compound->fh, (int) compound->fh_len,
                                op->open_flags,
                                chimera_vfs_compound_open_current_callback,
                                compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_GETHANDLE:
            if (!compound->handle) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }

            /* Every result owns a distinct reference. A borrowed cursor must
             * never become an owning result, and a later CLOSE must not release
             * a reference also owned by this output. Synthetic handles have no
             * backend open to duplicate; copy their descriptor instead. */
            if (compound->handle->cache_id == CHIMERA_VFS_OPEN_ID_SYNTHETIC) {
                op->out_handle       = chimera_vfs_synth_handle_alloc(compound->thread);
                *op->out_handle      = *compound->handle;
                op->out_handle->next = NULL;
            } else {
                chimera_vfs_dup_handle(compound->thread, compound->handle);
                op->out_handle = compound->handle;
            }

            chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
            break;

        case CHIMERA_VFS_COMPOUND_OP_CLOSE:
            if (!compound->handle) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }

            /* External references survive rejected attempts. Attempt-owned
             * outputs become unavailable immediately, but their references
             * anchor provisional claims until journal and reservation drain. */
            if (compound->handle_borrowed && !compound->handle_origin) {
                op->close_handle = compound->handle;
            } else {
                if (compound->handle_origin) {
                    compound->ops[compound->handle_origin - 1]->out_handle = NULL;
                    /* Two cursors may alias one result reference through
                     * PUTHANDLE_FROM. Closing it invalidates both aliases;
                     * GETHANDLE results own distinct references and survive. */
                    if (compound->saved_handle_origin == compound->handle_origin) {
                        compound->saved_handle = NULL;
                        compound->saved_handle_origin = 0;
                        compound->saved_handle_flags = 0;
                        compound->saved_handle_taken = 0;
                        compound->saved_handle_borrowed = 0;
                    }
                }
                op->closed_output_handle = compound->handle;
            }

            compound->handle_origin   = 0;
            compound->handle          = NULL;
            compound->handle_borrowed = 0;
            compound->handle_taken    = 0;
            compound->handle_flags    = 0;

            chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
            break;

        case CHIMERA_VFS_COMPOUND_OP_SAVEHANDLE:
            if (!compound->handle) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }

            /* A MOVE: the ownership bits travel with the handle and the
             * current slot is left empty, so exactly one slot refers to it. */
            chimera_vfs_compound_release_saved(compound);

            compound->saved_handle          = compound->handle;
            compound->saved_handle_flags    = compound->handle_flags;
            compound->saved_handle_borrowed = compound->handle_borrowed;
            compound->saved_handle_taken    = compound->handle_taken;
            compound->saved_handle_origin   = compound->handle_origin;

            compound->handle          = NULL;
            compound->handle_flags    = 0;
            compound->handle_borrowed = 0;
            compound->handle_taken    = 0;
            compound->handle_origin   = 0;

            chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
            break;

        case CHIMERA_VFS_COMPOUND_OP_RESTOREHANDLE:
            if (!compound->saved_handle) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }

            chimera_vfs_compound_release_cursor(compound);

            compound->handle          = compound->saved_handle;
            compound->handle_flags    = compound->saved_handle_flags;
            compound->handle_borrowed = compound->saved_handle_borrowed;
            compound->handle_taken    = compound->saved_handle_taken;
            compound->handle_origin   = compound->saved_handle_origin;

            compound->saved_handle          = NULL;
            compound->saved_handle_flags    = 0;
            compound->saved_handle_borrowed = 0;
            compound->saved_handle_taken    = 0;

            chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
            break;

        case CHIMERA_VFS_COMPOUND_OP_PUTHANDLE:
            if ((!op->in_handle && op->handle_from < 0) || !target) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }
            /* Preserve reference provenance explicitly. Pointer equality
             * cannot infer it: cached handles can represent several separately
             * owned references at the same address. */
            chimera_vfs_compound_set_current(compound, target->fh, target->fh_len);

            compound->handle          = target;
            compound->handle_flags    = op->open_flags;
            compound->handle_borrowed = op->in_handle != NULL;
            compound->handle_taken    = op->in_handle == NULL;
            compound->handle_origin   = op->in_handle ? 0 : op->handle_from + 1;
            compound->handle_explicit = 1;

            chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
            break;

        case CHIMERA_VFS_COMPOUND_OP_REMOVE_PATHS:
            if (op->remove_path_index >= op->remove_num_paths) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
            } else {
                const char *path = op->remove_paths[op->remove_path_index];
                chimera_vfs_remove(compound->thread, compound->cred,
                                   compound->fh, compound->fh_len,
                                   path, strlen(path), op->remove_flags,
                                   chimera_vfs_compound_remove_paths_callback, compound);
            }
            break;

        case CHIMERA_VFS_COMPOUND_OP_FIND:
            if (op->find_reset) {
                op->find_reset(compound, compound->index, op->find_private);
            }
            chimera_vfs_find(compound->thread, compound->cred,
                             compound->fh, compound->fh_len, op->attr_mask,
                             chimera_vfs_compound_find_filter,
                             chimera_vfs_compound_find_append,
                             chimera_vfs_compound_path_status_callback, compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_LOOKUP_PATH:
            chimera_vfs_lookup(compound->thread, compound->cred,
                               compound->fh, (int) compound->fh_len,
                               op->path, (int) op->path_len,
                               op->attr_mask, op->open_opts,
                               chimera_vfs_compound_path_attr_callback,
                               compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_OPEN_PATH:
            chimera_vfs_open(compound->thread, compound->cred,
                             compound->fh, (int) compound->fh_len,
                             op->path, (int) op->path_len,
                             op->open_flags,
                             op->applied_attr.va_set_mask ? &op->applied_attr : NULL,
                             op->attr_mask,
                             chimera_vfs_compound_path_open_callback,
                             compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_RANGE_OWNER: {
            if (!target || !op->have_io_owner || op->io_owner.owner.proto != CHIMERA_CLAIM_PROTO_SMB2) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }
            /* SMB ranges are local under the existing projection contract;
             * POSIX projected ownership is deliberately not accepted here. */
            struct chimera_vfs_file_state *file = chimera_vfs_state_get(compound->thread->vfs->vfs_state,
                target->fh, target->fh_len, target->fh_hash, true);
            if (file) {
                op->out_range_owner = chimera_vfs_claim_owner_create_compat(file, &op->io_owner, op->range_zero_point);
                chimera_vfs_state_put(compound->thread->vfs->vfs_state, file);
            }
            chimera_vfs_compound_op_done(compound, op->out_range_owner ? CHIMERA_VFS_OK : CHIMERA_VFS_ENOSPC);
            break;
        }

        case CHIMERA_VFS_COMPOUND_OP_RANGE_BATCH:
            chimera_vfs_claim_range_attempt_execute(op->range_attempt,
                compound->claim_journal, op->range_owner, op->exact_ranges,
                op->num_exact_ranges, op->range_unlock, op->range_wait,
                op->range_timeout_ms, compound_range_wait, compound_range_is_canceled,
                compound_range_complete, compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_LOCK_TEST:
        case CHIMERA_VFS_COMPOUND_OP_LOCK_CHANGE:
        case CHIMERA_VFS_COMPOUND_OP_LOCK_RELEASE_OWNER:
            op->nonretryable = !chimera_vfs_lock_attempt_retryable(op->lock_attempt);
            chimera_vfs_lock_attempt_execute(op->lock_attempt, compound->finish_handler != NULL,
                                             chimera_vfs_compound_lock_complete, compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_NARROW_ACCESS: {
            uint32_t from = op->access_narrow_from;
            struct chimera_vfs_compound_op *reserve = from < compound->num_ops ? compound->ops[from] : NULL;
            enum chimera_vfs_error error = CHIMERA_VFS_EINVAL;
            if (reserve && reserve->type == CHIMERA_VFS_COMPOUND_OP_RESERVE_ACCESS &&
                reserve->completed && reserve->status == CHIMERA_VFS_OK &&
                reserve->claim_held && reserve->access_owner) {
                error = chimera_vfs_claim_access_journal_narrow(compound->access_journal,
                    reserve->access_owner, op->access_narrow_used, op->access_narrow_denied);
            }
            chimera_vfs_compound_op_done(compound, error);
            break;
        }

        case CHIMERA_VFS_COMPOUND_OP_RETIRE_ACCESS:
            chimera_vfs_compound_op_done(compound,
                chimera_vfs_claim_access_journal_retire(compound->access_journal, op->access_retire_owner));
            break;

        case CHIMERA_VFS_COMPOUND_OP_RETIRE_OPEN_CLAIMS: {
            uint32_t range_checkpoint = chimera_vfs_claim_journal_retire_checkpoint(compound->claim_journal);
            uint32_t access_checkpoint = chimera_vfs_claim_access_journal_checkpoint(compound->access_journal);
            enum chimera_vfs_error error = CHIMERA_VFS_OK;
            if (op->range_retire_owner) {
                error = chimera_vfs_claim_journal_retire_owner(compound->claim_journal, op->range_retire_owner);
            }
            if (error == CHIMERA_VFS_OK && op->access_retire_owner) {
                error = chimera_vfs_claim_access_journal_retire(compound->access_journal, op->access_retire_owner);
            }
            if (error == CHIMERA_VFS_OK && op->base_access_retire_owner) {
                error = chimera_vfs_claim_access_journal_retire(compound->access_journal, op->base_access_retire_owner);
            }
            if (error != CHIMERA_VFS_OK) {
                chimera_vfs_claim_access_journal_rewind(compound->access_journal, access_checkpoint);
                chimera_vfs_claim_journal_retire_rewind(compound->claim_journal, range_checkpoint);
            }
            chimera_vfs_compound_op_done(compound, error);
            break;
        }

        case CHIMERA_VFS_COMPOUND_OP_RESERVE_ACCESS:
        case CHIMERA_VFS_COMPOUND_OP_RESERVE:
            if (!target) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }
            if (op->claim->klass == CHIMERA_CLAIM_CLASS_RANGE &&
                !chimera_vfs_claim_range_is_local(compound->thread, target, op->claim)) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_ENOTSUP);
                break;
            }
            op->claim_file = chimera_vfs_state_get(compound->thread->vfs->vfs_state,
                                                   target->fh, target->fh_len,
                                                   target->fh_hash, true);
            if (!op->claim_file) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_ENOSPC);
                break;
            }
            if (op->type == CHIMERA_VFS_COMPOUND_OP_RESERVE_ACCESS) {
                op->access_owner = chimera_vfs_claim_access_owner_alloc(op->claim_file, op->claim);
                if (!op->access_owner) {
                    chimera_vfs_compound_op_done(compound, CHIMERA_VFS_ENOSPC);
                    break;
                }
                op->claim = chimera_vfs_claim_access_owner_claim(op->access_owner);
            }
            op->claim_result = chimera_vfs_claim_test_range_view(NULL, op->claim,
                                                                 op->claim_ranges, op->num_claim_ranges, &op->
                                                                 claim_conflict);
            if (op->claim_result == CHIMERA_CLAIM_GRANTED && compound->access_journal) {
                op->claim_result = chimera_vfs_claim_access_journal_test(compound->access_journal,
                    op->claim_file, op->claim, &op->claim_conflict);
            }
            if (op->claim_result == CHIMERA_CLAIM_GRANTED) {
                const struct chimera_vfs_claim *const *original = op->claim->admit_excluded;
                uint32_t original_count = op->claim->admit_num_excluded;
                uint32_t count = compound->access_journal ? chimera_vfs_claim_access_journal_excluded(
                    compound->access_journal, op->claim_file, NULL, 0) : 0;
                const struct chimera_vfs_claim **excluded = NULL;
                if (count) {
                    excluded = calloc((size_t) count + original_count, sizeof(*excluded));
                    if (!excluded) {
                        chimera_vfs_compound_op_done(compound, CHIMERA_VFS_ENOSPC);
                        break;
                    }
                    for (uint32_t i = 0; i < original_count; i++) excluded[i] = original[i];
                    chimera_vfs_claim_access_journal_excluded(compound->access_journal,
                        op->claim_file, excluded + original_count, count);
                    op->claim->admit_excluded = excluded;
                    op->claim->admit_num_excluded = original_count + count;
                }
                const void *original_cookie = op->claim->admission_cookie;
                op->claim->admission_cookie = compound->admission_cookie ? compound->admission_cookie : compound;
                op->claim_result = chimera_vfs_claim_try_acquire(
                    compound->thread->vfs->vfs_state, op->claim_file, op->claim, &op->claim_conflict);
                op->claim->admission_cookie = original_cookie;
                op->claim->admit_excluded = original;
                op->claim->admit_num_excluded = original_count;
                free(excluded);
            }
            op->claim_held = op->claim_result == CHIMERA_CLAIM_GRANTED;
            chimera_vfs_compound_op_done(compound,
                                         op->claim_held ? CHIMERA_VFS_OK :
                                         op->claim_conflict.admission_fenced ? CHIMERA_VFS_EBUSY : CHIMERA_VFS_EACCES);
            break;

        case CHIMERA_VFS_COMPOUND_OP_CREATE_UNLINKED:
            if (!compound->fh_len) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }
            chimera_vfs_create_unlinked(compound->thread, compound->cred,
                                        compound->fh, compound->fh_len,
                                        &op->applied_attr, op->attr_mask,
                                        chimera_vfs_compound_create_unlinked_callback,
                                        compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_CREATE_PATH:
            switch (op->create_type) {
                case CHIMERA_VFS_COMPOUND_CREATE_DIR_TREE:
                    chimera_vfs_create(compound->thread, compound->cred,
                                       compound->fh, compound->fh_len,
                                       op->path, op->path_len,
                                       &op->applied_attr, op->attr_mask,
                                       chimera_vfs_compound_path_attr_callback,
                                       compound);
                    break;
                case CHIMERA_VFS_COMPOUND_CREATE_DIR:
                    chimera_vfs_mkdir(compound->thread, compound->cred,
                                      compound->fh, (int) compound->fh_len,
                                      op->path, (int) op->path_len,
                                      &op->applied_attr, op->attr_mask,
                                      chimera_vfs_compound_path_attr_callback,
                                      compound);
                    break;
                case CHIMERA_VFS_COMPOUND_CREATE_SYMLINK:
                    chimera_vfs_symlink(compound->thread, compound->cred,
                                        compound->fh, (int) compound->fh_len,
                                        op->path, (int) op->path_len,
                                        op->link_target,
                                        (int) op->link_target_len,
                                        &op->applied_attr, op->attr_mask,
                                        chimera_vfs_compound_path_attr_callback,
                                        compound);
                    break;
                default:
                    chimera_vfs_mknod(compound->thread, compound->cred,
                                      compound->fh, (int) compound->fh_len,
                                      op->path, (int) op->path_len,
                                      &op->applied_attr, op->attr_mask,
                                      chimera_vfs_compound_path_attr_callback,
                                      compound);
                    break;
            } /* switch */
            break;

        case CHIMERA_VFS_COMPOUND_OP_REMOVE_PATH:
            if (op->open_opts) {
                if (op->path_len) {
                    chimera_vfs_lookup(compound->thread, compound->cred,
                                       compound->fh, compound->fh_len, op->path, op->path_len,
                                       CHIMERA_VFS_ATTR_FH, 0,
                                       chimera_vfs_compound_remove_at_path_lookup, compound);
                } else {
                    chimera_vfs_open_fh(compound->thread, compound->cred,
                                        compound->fh, compound->fh_len,
                                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                                        CHIMERA_VFS_OPEN_DIRECTORY,
                                        chimera_vfs_compound_remove_at_path_open, compound);
                }
                break;
            }
            chimera_vfs_remove(compound->thread, compound->cred,
                               compound->fh, (int) compound->fh_len,
                               op->path, (int) op->path_len,
                               op->remove_flags,
                               chimera_vfs_compound_path_status_callback,
                               compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_RENAME_PATH:
            chimera_vfs_rename(compound->thread, compound->cred,
                               compound->fh, (int) compound->fh_len,
                               op->path, (int) op->path_len,
                               op->new_path, (int) op->new_path_len,
                               chimera_vfs_compound_path_status_callback,
                               compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_LINK_PATH:
            chimera_vfs_link(compound->thread, compound->cred,
                             compound->fh, (int) compound->fh_len,
                             op->path, (int) op->path_len,
                             op->open_flags,
                             op->new_path, (int) op->new_path_len,
                             0, /* replace: link(2) never clobbers */
                             op->attr_mask,
                             chimera_vfs_compound_path_attr_callback,
                             compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_ALLOCATE:
            chimera_vfs_allocate_owned(compound->thread, compound->cred,
                                 target,
                                 op->offset, op->length,
                                 op->allocate_flags,
                                 op->attr_mask, op->post_attr_mask,
                                 op->have_io_owner ? &op->io_owner : NULL,
                                 chimera_vfs_compound_allocate_callback,
                                 compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_SEEK:
            chimera_vfs_seek(compound->thread, compound->cred,
                             target,
                             op->offset, op->seek_what,
                             chimera_vfs_compound_seek_callback,
                             compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_READDIR:
            if (op->readdir_reset) {
                /* The directory being listed, recorded before the enumeration
                 * rather than after it: the append callback has to know which
                 * directory an entry came from, and a READDIR cannot move the
                 * current object, so there is nothing here to invalidate. */
                memcpy(op->fh, compound->fh, compound->fh_len);
                op->fh_len = compound->fh_len;

                /* Before EVERY execution, so a retry is a first run from the
                 * caller's side and it never has to ask which it is in. */
                op->readdir_reset(compound, compound->index,
                                  op->readdir_private);
            } else {
                if (!op->entries && op->max_entries) {
                    op->entries = calloc(op->max_entries,
                                         sizeof(*op->entries));
                }
            }
            op->num_entries = 0;
            chimera_vfs_readdir(compound->thread, compound->cred,
                                target,
                                compound_object_mask(op, op->attr_mask),
                                compound_post_mask(op, 0),
                                op->cookie,
                                op->verifier,
                                op->readdir_flags,
                                NULL, 0,
                                chimera_vfs_compound_readdir_entry,
                                chimera_vfs_compound_readdir_callback,
                                compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_OPEN_STREAM:
            chimera_vfs_open_stream(compound->thread, compound->cred, target,
                op->name, op->name_len, op->open_flags, &op->applied_attr,
                compound_object_mask(op, op->attr_mask),
                chimera_vfs_compound_open_stream_callback, compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_LIST_STREAMS:
            if (!op->buffer && op->buffer_max) {
                op->buffer = calloc(1, op->buffer_max);
                if (!op->buffer) {
                    chimera_vfs_compound_op_done(compound, CHIMERA_VFS_ENOSPC);
                    break;
                }
            }
            chimera_vfs_list_streams(compound->thread, compound->cred, target,
                op->cookie, op->buffer, op->buffer_max, op->stream_want_fh,
                chimera_vfs_compound_list_streams_callback, compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_REMOVE_STREAM:
            chimera_vfs_remove_stream_checked(compound->thread, compound->cred, target,
                op->name, op->name_len, op->remove_flags, op->arg_fh, op->arg_fh_len,
                chimera_vfs_compound_remove_stream_callback, compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_GETXATTR:
            if (!op->buffer && op->buffer_max) {
                op->buffer = calloc(1, op->buffer_max);
            }
            chimera_vfs_get_xattr(compound->thread, compound->cred,
                                  target,
                                  op->name, op->name_len,
                                  op->buffer, op->buffer_max,
                                  chimera_vfs_compound_get_xattr_callback,
                                  compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_SETXATTR:
            chimera_vfs_set_xattr(compound->thread, compound->cred,
                                  target,
                                  op->xattr_option,
                                  op->name, op->name_len,
                                  op->xattr_value, op->xattr_value_len,
                                  chimera_vfs_compound_xattr_change_callback,
                                  compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_LISTXATTRS:
            if (!op->buffer && op->buffer_max) {
                op->buffer = calloc(1, op->buffer_max);
            }
            chimera_vfs_list_xattrs(compound->thread, compound->cred,
                                    target,
                                    op->cookie,
                                    op->buffer, op->buffer_max,
                                    chimera_vfs_compound_list_xattrs_callback,
                                    compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_REMOVEXATTR:
            chimera_vfs_remove_xattr(compound->thread, compound->cred,
                                     target,
                                     op->name, op->name_len,
                                     chimera_vfs_compound_xattr_change_callback,
                                     compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_GETATTR:
        case CHIMERA_VFS_COMPOUND_OP_ACCESS:
            chimera_vfs_getattr(compound->thread, compound->cred,
                                target, op->attr_mask,
                                chimera_vfs_compound_getattr_callback,
                                compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_READLINK:
            if (!op->target) {
                op->target = calloc(1, CHIMERA_VFS_COMPOUND_TARGET_MAX + 1);
            }
            chimera_vfs_readlink(compound->thread, compound->cred,
                                 target,
                                 op->target, CHIMERA_VFS_COMPOUND_TARGET_MAX,
                                 0,
                                 chimera_vfs_compound_readlink_callback,
                                 compound);
            break;

        default:
            chimera_vfs_compound_op_done(compound, CHIMERA_VFS_ENOTSUP);
            break;
    } /* switch */
} /* chimera_vfs_compound_step */

SYMBOL_EXPORT bool
chimera_vfs_compound_retry(struct chimera_vfs_compound *compound)
{
    uint32_t i;

    /* Ownership transfer marks publication. Retrying after that would release
     * resources the caller now owns and repeat an accepted operation. */
    if (compound->running || compound->canceled || compound->ownership_taken ||
        compound->claim_journal_published || compound->access_journal_published || !compound->original_ops) {
        return false;
    }

    for (i = 0; i < compound->num_ops; i++) {
        if (compound->ops[i]->nonretryable) {
            return false;
        }
    }
    if (compound->claim_journal) {
        chimera_vfs_claim_journal_reset(compound->claim_journal);
    }
    if (compound->access_journal) {
        chimera_vfs_claim_access_journal_reset(compound->access_journal);
    }
    compound_release_range_owners(compound);
    /* Reservations may borrow a producer's handle as an actor anchor. Drain
     * all consumers before releasing any producer/cursor reference. */
    for (i = 0; i < compound->num_ops; i++) {
        chimera_vfs_compound_release_reservation(compound, compound->ops[i]);
    }
    chimera_vfs_compound_release_cursor(compound);
    chimera_vfs_compound_release_saved(compound);
    for (i = 0; i < compound->num_ops; i++) {
        struct chimera_vfs_compound_op *op = compound->ops[i];
        if (op->closed_output_handle) {
            chimera_vfs_release(compound->thread, op->closed_output_handle);
        }
        if (op->lock_attempt) {
            chimera_vfs_lock_attempt_reset(op->lock_attempt);
        }
        if (op->out_handle) {
            chimera_vfs_release(compound->thread, op->out_handle);
        }
        if (op->niov) {
            evpl_iovecs_release(compound->thread->evpl, op->iov, op->niov);
        }
        free(op->attr.va_acl);
        free(op->applied_acl);
        free(op->target);
        free(op->entries);
        free(op->buffer);
        free(op->journal_excluded);
        free(op->journal_src_excluded);
        compound_search_keys_release(op);
        if (i < compound->original_num_ops) {
            *op = compound->original_ops[i];
        } else {
            free(op->link_target);
            free(op->path);
            free(op->new_path);
            free(op->kv_key);
            free(op->kv_value);
            memset(op, 0, sizeof(*op));
        }
    }
    for (i = 0; i < compound->num_groups; i++) {
        struct chimera_vfs_compound_group *group = &compound->groups[i];
        group->status = CHIMERA_VFS_UNSET;
        group->last_op = group->config.first_op + group->config.num_ops - 1;
    }
    compound->group_index = 0;
    compound->group_active = 0;
    compound->cancel_defer_end = 0;
    compound->cred = compound->default_cred;
    compound->num_ops            = compound->original_num_ops;
    compound->index              = 0;
    compound->completed          = 0;
    compound->status             = CHIMERA_VFS_OK;
    compound->execution_status   = CHIMERA_VFS_OK;
    compound->finish_status      = CHIMERA_VFS_OK;
    compound->build_failed       = compound->original_build_failed;
    compound->build_error        = compound->original_build_error;
    compound->fh_len             = 0;
    compound->saved_fh_len       = 0;
    compound->handle_flags       = 0;
    compound->saved_handle_flags = 0;
    compound->open_resolved      = 0;
    compound->open_retried       = 0;
    compound->io_typechecked     = 0;
    compound->running            = 1;
    if (compound->attempt_reset) {
        compound->attempt_reset(compound, compound->attempt_private);
    }
    if (compound->build_failed) {
        chimera_vfs_compound_finish(compound, compound->build_error ? compound->build_error : CHIMERA_VFS_EINVAL);
        return true;
    }
    chimera_vfs_compound_step(compound);
    return true;
} /* chimera_vfs_compound_retry */

SYMBOL_EXPORT void
chimera_vfs_compound_submit(
    struct chimera_vfs_compound    *compound,
    chimera_vfs_compound_callback_t callback,
    void                           *private_data)
{
    compound->callback     = callback;
    compound->private_data = private_data;
    compound->index        = 0;
    compound->completed    = 0;
    compound->running      = 1;

    if (compound->num_groups) {
        struct chimera_vfs_compound_group *last = &compound->groups[compound->num_groups - 1];
        if (last->config.first_op + last->config.num_ops != compound->num_ops) {
            compound->build_failed = 1;
            compound->build_error = CHIMERA_VFS_EINVAL;
        }
    }

    for (uint32_t i = 0; i < compound->num_ops; i++) {
        uint32_t ready = compound->ops[i]->access_ready;
        if (!ready) continue;
        bool valid = false;
        for (uint32_t g = 0; g < compound->num_groups; g++) {
            if (compound_access_ready_in_group(compound, g, i, ready - 1)) {
                valid = true;
                break;
            }
        }
        if (!valid) {
            compound->build_failed = 1;
            compound->build_error = CHIMERA_VFS_EINVAL;
        }
    }

    /* Scope endpoints must belong to one registered static group. Runtime
     * dynamic insertion is unsupported inside a declared cleanup suffix. */
    for (uint32_t i = 0; i < compound->num_ops; i++) {
        uint32_t end = compound->ops[i]->cancel_scope_end;
        if (!end) continue;
        bool valid = false;
        for (uint32_t g = 0; g < compound->num_groups; g++) {
            const struct chimera_vfs_compound_group_config *config = &compound->groups[g].config;
            if (i >= config->first_op && end <= config->first_op + config->num_ops) {
                valid = true;
                break;
            }
        }
        if (!valid) {
            compound->build_failed = 1;
            compound->build_error = CHIMERA_VFS_EINVAL;
        }
    }

    /* Initial lock routing is deliberately a dedicated compound: no backend
     * transaction is kept open across an unbounded lock wait, and legacy
     * projection cannot mix with retryable filesystem mutations. */
    unsigned lock_ops = 0;
    for (uint32_t i = 0; i < compound->num_ops; i++) {
        lock_ops += compound->ops[i]->lock_attempt != NULL;
    }
    if (lock_ops) {
        for (uint32_t i = 0; i < compound->num_ops; i++) {
            struct chimera_vfs_compound_op *op = compound->ops[i];
            if (lock_ops != 1 || (!op->lock_attempt &&
                                  op->type != CHIMERA_VFS_COMPOUND_OP_PUTFH &&
                                  op->type != CHIMERA_VFS_COMPOUND_OP_PUTHANDLE)) {
                compound->build_failed = 1;
                compound->build_error  = CHIMERA_VFS_ENOTSUP;
            }
        }
    }
    compound->original_build_failed = compound->build_failed;
    compound->original_build_error  = compound->build_error;
    /* Even an empty submitted attempt needs a snapshot marker so a rejected
    * finish can be retried. Allocation failure leaves retry unavailable. */
    compound->original_ops = calloc(compound->num_ops ? compound->num_ops : 1,
                                    sizeof(*compound->original_ops));
    if (!compound->original_ops) {
        chimera_vfs_compound_finish(compound, CHIMERA_VFS_ENOSPC);
        return;
    }
    compound->original_num_ops = compound->num_ops;
    for (uint32_t i = 0; i < compound->num_ops; i++) {
        compound->original_ops[i] = *compound->ops[i];
    }
    if (compound->attempt_reset) {
        compound->attempt_reset(compound, compound->attempt_private);
    }

    /* An op that could not be built is not an op the sequence may skip: a
     * shorter sequence is a different request, and one that has quietly
     * dropped the operation the caller cared about usually succeeds. */
    if (compound->build_failed) {
        chimera_vfs_compound_finish(compound, compound->build_error ? compound->build_error : CHIMERA_VFS_EINVAL);
        return;
    }

    chimera_vfs_compound_step(compound);
} /* chimera_vfs_compound_submit */

/* ---------------------------------------------------------------------- */
/* Results                                                                */
/* ---------------------------------------------------------------------- */

SYMBOL_EXPORT uint32_t
chimera_vfs_compound_num_groups(const struct chimera_vfs_compound *compound)
{
    return compound->num_groups;
}

SYMBOL_EXPORT uint32_t
chimera_vfs_compound_num_ops(const struct chimera_vfs_compound *compound)
{
    return compound->num_ops;
} /* chimera_vfs_compound_num_ops */

SYMBOL_EXPORT uint32_t
chimera_vfs_compound_num_completed(const struct chimera_vfs_compound *compound)
{
    return compound->completed;
} /* chimera_vfs_compound_num_completed */

SYMBOL_EXPORT enum chimera_vfs_error
chimera_vfs_compound_status(const struct chimera_vfs_compound *compound)
{
    return compound->status;
} /* chimera_vfs_compound_status */

SYMBOL_EXPORT const struct chimera_vfs_compound_op *
chimera_vfs_compound_op(
    const struct chimera_vfs_compound *compound,
    uint32_t                           index)
{
    if (index >= compound->num_ops) {
        return NULL;
    }

    return compound->ops[index];
} /* chimera_vfs_compound_op */
