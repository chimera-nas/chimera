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
#include <stdatomic.h>
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <unistd.h>
#endif /* ifdef _WIN32 */

#include <utlist.h>

#include "vfs_compound.h"
#include "vfs_internal_procs.h"
#include "vfs_claim.h"
#include "vfs_claim_journal_attempt.h"
#include "vfs_internal.h"
#include "vfs_release.h"
#include "vfs_claim.h"
#include "sdk/vfs_access.h"
#include "sdk/vfs_acl.h"
#include "common/macros.h"

struct chimera_vfs_compound_coordination {
    struct chimera_vfs_compound_coordination *next;
    uint32_t                                  index, fh_len;
    uint64_t                                  token;
    chimera_vfs_compound_coordinate_t         start;
    void                                     *private_data;
    uint8_t                                   fh[CHIMERA_VFS_FH_SIZE];
    enum chimera_vfs_error status;
};

struct chimera_vfs_compound_group {
    struct chimera_vfs_compound_group_config config;
    enum chimera_vfs_error status;
    uint32_t                                 last_op;
};

struct chimera_vfs_compound {
    struct chimera_vfs_thread               *thread;
    const struct chimera_vfs_cred           *cred;
    const struct chimera_vfs_cred           *default_cred;

    /* Link for the owning thread's free list.  Everything from here down to
     * (but not including) ops[] is cleared wholesale by chimera_vfs_compound_
     * reset(), so a field added to this struct is reset without being named --
     * which is why ops[] is last. */
    struct chimera_vfs_compound             *next;

    struct chimera_vfs_claim_journal        *claim_journal;
    uint32_t                                 claim_journal_budget;
    uint8_t                                  claim_journal_published;
    struct chimera_vfs_claim_access_journal *access_journal;
    uint8_t                                  access_journal_published;
    struct chimera_vfs_compound_group       *groups;
    uint32_t                                 num_groups;
    uint32_t                                 group_index;
    uint8_t                                  group_active;
    uint8_t                                  canceled;
    uint32_t                                 cancel_defer_end;  /* active end index + 1 */
    const void                              *admission_cookie;
    uint32_t                                 num_ops;
    /* An adder could not build its op -- the sequence is full, or an argument
     * was malformed.  Submitting runs nothing and fails. */
    uint8_t                                  build_failed;
    enum chimera_vfs_error                   build_error;
    uint32_t                                 index;  /* op being executed        */
    uint32_t                                 completed;  /* ops that ran             */
    enum chimera_vfs_error                   status;
    enum chimera_vfs_error                   execution_status;
    enum chimera_vfs_error                   finish_status;
    chimera_vfs_compound_finish_handler_t    finish_handler;
    void                                    *finish_private;
    uint8_t                                  finishing;

    /* The current object: its file handle, and an open handle for it once
     * something has needed one.  handle_flags records what that handle was
     * opened with, so an op needing more than it carries (a LOOKUP wanting a
     * directory open) can re-open rather than settle for less. */
    uint8_t                                  fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                                 fh_len;
    struct chimera_vfs_open_handle          *handle;
    unsigned int                             handle_flags;
    /* GETHANDLE gave this handle to the caller: the sequence still addresses it
     * but no longer releases it. */
    uint8_t                                  handle_taken;
    uint32_t                                 handle_origin;
    /* The caller chose this handle -- OPEN said what to open it with, or
     * PUTHANDLE lent one already open.  The executor never re-opens such a
     * handle: the caller knows what it is for, and re-opening would discard
     * the very reference the caller supplied. */
    uint8_t                                  handle_explicit;
    /* The handle refers to an object OTHER than the one the FILE cursor
     * names -- a CREATE_UNLINKED's, which lives in the current directory
     * and has no name of its own.  The re-open rule (a handle that does not
     * serve an op is released and the current fh opened afresh) would then
     * open the DIRECTORY in the object's place, so such a handle is held to
     * the lent-handle rule instead: an op it does not serve fails EINVAL.
     * The name-resolving ops are the exception, because for them the
     * directory IS what they want opened. */
    uint8_t                                  handle_nameless;
    uint8_t                                  saved_handle_nameless;
    /* The saved OPEN slot.  SAVEHANDLE moves into it and RESTOREHANDLE moves
     * back, so exactly one slot refers to a handle at any moment and the
     * ownership bits travel with it. */
    struct chimera_vfs_open_handle          *saved_handle;
    unsigned int                             saved_handle_flags;
    uint8_t                                  saved_handle_borrowed;
    uint8_t                                  saved_handle_taken;
    uint32_t                                 saved_handle_origin;
    /* The current handle is the CALLER'S, seeded by PUTHANDLE, and must not be
     * released with the sequence or when the current object moves.  Cleared
     * the moment the executor opens one of its own. */
    uint8_t                                  handle_borrowed;

    /* The saved slot (SAVEFH/RESTOREFH): a file handle, no open handle. */
    uint8_t                                  saved_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                                 saved_fh_len;

    /* An OPEN that must resolve its name before opening runs in two steps, and
     * this says which one is next.  Cleared whenever the sequence advances, so
     * it can never be read as belonging to a different op. */
    uint8_t                                  open_resolved;

    /* An exclusive create collided and is being re-opened -- see
     * CHIMERA_VFS_COMPOUND_OPEN_EXCLUSIVE_RETRY.  Set so the retry cannot
     * itself retry.  Cleared whenever the sequence advances. */
    uint8_t                                  open_retried;

    /* A READ or WRITE that addresses the current object establishes that the
     * object is a regular file before it opens it for data -- so a protocol
     * answers for the type rather than for whatever errno a backend's data
     * open of a directory happens to produce, and so that open is never
     * attempted at all.  This says that step has been done.  Cleared whenever
     * the sequence advances. */
    uint8_t                                  io_typechecked;

    /* A LENT handle that does not carry CHIMERA_VFS_OPEN_DIRECTORY has been
     * asked whether it addresses one, and does -- see the lent-handle rules in
     * the header.  Asked once per op, like the I/O type check, and cleared
     * whenever the sequence advances. */
    uint8_t                                  lent_dirchecked;

    /* A parking RECALL's inert request answered -- inside the recall call,
     * or later off the owning thread's resume drain.  Read once the call
     * returns to tell an inline answer from a park; see the RECALL arm of
     * step.  Cleared whenever the sequence advances. */
    uint8_t                                  recall_answered;

    /* A parked RECALL's inert request, kept here for exactly as long as the
     * park lasts: it is what chimera_vfs_claim_recall_cancel takes to unlink
     * the io-wait ticket.  NULL whenever the run is not parked on a RECALL. */
    struct chimera_vfs_request              *recall_request;

    /* WHAT THE RUN IS PARKED ON, and whether the caller has been told.
     *
     * park_state is the cancel handle: it says which of the two cancellable
     * parks the run is sitting in, and therefore which core call takes it
     * back.  It is set as the park is entered -- before the caller's park_cb
     * can see it -- and cleared the moment the park ends, whichever way it
     * ends (the answer arrives, or a cancel takes it).  Nothing else is a
     * park as far as the caller is concerned: an ordinary op parked below us
     * in the claim layer, and a CLAIM_TEST projected to a backend arbiter,
     * both leave this NONE, because neither is ours to take back.
     *
     * park_fired is the once-per-submission latch on the notification, and
     * park_notifying guards the one thing a park callback must not do. */
    uint8_t                                  park_state;
    uint8_t                                  park_fired;
    uint8_t                                  park_notifying;

    chimera_vfs_compound_park_cb_t           park_cb;
    void                                    *park_private;

    /* A CLAIM's handshake with its claim callback, which may answer inside
     * the acquire call or later from whichever thread released the blocker
     * (or, for a CLAIM_TEST projected to a backend arbiter, from whichever
     * thread the backend answered on).  claim_phase is what tells the two
     * apart (see the CLAIM arm of step and chimera_vfs_compound_claim_
     * callback); claim_resume is the request that carries a late answer
     * home through the owning thread's doorbell. */
    _Atomic uint8_t                          claim_phase;
    struct chimera_vfs_request              *claim_resume;

    /* THE CROSS-THREAD CANCEL -- chimera_vfs_compound_cancel_post.
     *
     * The vehicle that carries a post home to the submitting thread, and the
     * latch that makes any number of posts exactly one ride.  Both are read
     * and written from threads that are not the submitting one, which is what
     * shapes them: the poster cannot take a request off this thread's free
     * list (it is per-thread and unlocked), so the vehicle is allocated WITH
     * the compound and lives exactly as long as it does -- surviving the
     * recycle, so a warm thread allocates none -- and the pointer is never
     * NULL for a compound a caller is holding.
     *
     * cancel_post_state says who owns the vehicle: IDLE, the compound's;
     * POSTED, the doorbell's; ORPHAN, the doorbell's and the compound has
     * been freed underneath it, so the drain does the recycling the free
     * deferred.  See chimera_vfs_compound_cancel_post. */
    struct chimera_vfs_request               *cancel_post;
    _Atomic uint8_t                           cancel_post_state;

    chimera_vfs_compound_callback_t           callback;
    void                                     *private_data;

    chimera_vfs_compound_gate_t               gate;
    void                                     *gate_private;
    /* The gate is on the stack, and this is the op it was consulted on: what
     * chimera_vfs_compound_op_edit refuses to edit at or below.  Outside the
     * call gating is 0 and nothing may be edited at all. */
    uint8_t                                   gating;
    uint32_t                                  gate_index;

    /* A CLOSE(CLOSE_DOC) whose release fired the delete-on-close: what the
     * release handed back (the parent, the name, the arming credential, and
     * the backend close it detached from the cache), the parent handle opened
     * to unlink through, and the doomed object's own fh, which the unlink
     * matches on.  Live only between the release and the unlink's completion
     * -- one op, one at a time -- and cleared with everything else on reset. */
    struct chimera_vfs_doc_info               close_doc;
    struct chimera_vfs_open_handle           *close_doc_parent;
    uint8_t                                   close_doc_child_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                                  close_doc_child_fh_len;

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
#define CHIMERA_VFS_COMPOUND_FREE_MAX      64

static void chimera_vfs_compound_step(
    struct chimera_vfs_compound *compound);

/* Who owns the cross-thread cancel's vehicle -- see cancel_post_state on the
 * compound, and chimera_vfs_compound_cancel_post. */
#define CHIMERA_VFS_COMPOUND_CANCEL_IDLE   0
#define CHIMERA_VFS_COMPOUND_CANCEL_POSTED 1
#define CHIMERA_VFS_COMPOUND_CANCEL_ORPHAN 2

static void chimera_vfs_compound_cancel_resume(
    struct chimera_vfs_request *request);

/*
 * Give the compound its cancel vehicle.  On the owning thread, once, when the
 * compound is first allocated: a recycled compound still has the one it was
 * born with, and a poster on another thread must never find this NULL.
 *
 * `complete` and `proto_private_data` are set here and never change -- the
 * vehicle serves one compound for life.  notify_gate_resume is the flag the
 * drain routes on and the drain CLEARS it, so the post sets it each time.
 */
static void
chimera_vfs_compound_cancel_alloc(struct chimera_vfs_compound *compound)
{
    struct chimera_vfs_request *request;
    void                       *scratch;

    scratch = chimera_vfs_gate_scratch_alloc(compound->thread);
    request = container_of(scratch, struct chimera_vfs_request, gate.data);

    request->complete           = chimera_vfs_compound_cancel_resume;
    request->proto_private_data = compound;
    request->notify_gate_resume = 0;

    compound->cancel_post = request;

    atomic_store(&compound->cancel_post_state,
                 CHIMERA_VFS_COMPOUND_CANCEL_IDLE);
} /* chimera_vfs_compound_cancel_alloc */

/* Return the vehicle to the thread's request pool.  Only for a compound that
 * is going back to the allocator rather than onto the free list, and only
 * with the vehicle IDLE -- a posted one is the doorbell's. */
static void
chimera_vfs_compound_cancel_free(struct chimera_vfs_compound *compound)
{
    struct chimera_vfs_request *request = compound->cancel_post;

    if (!request) {
        return;
    }

    request->proto_private_data = NULL;
    request->notify_gate_resume = 0;

    chimera_vfs_gate_scratch_free(compound->thread, request->gate.data);

    compound->cancel_post = NULL;
} /* chimera_vfs_compound_cancel_free */

/*
 * Release the by-value ACL and SIDs an attribute RESULT slot carries.
 *
 * The rule that makes this safe to call blind: a non-NULL va_acl,
 * va_owner_sid or va_group_sid in any of an op's result slots (attr,
 * pre_attr, the four dir_* slots) is heap memory the compound allocated in
 * chimera_vfs_compound_store_attr_to, and nothing else ever writes those
 * pointers there.  The op's set_attr is NOT a result slot -- its pointers are
 * the caller's, BORROWED, and are never passed through here.
 */
static void
chimera_vfs_compound_attr_release(struct chimera_vfs_attrs *attr)
{
    free(attr->va_acl);
    free(attr->va_owner_sid);
    free(attr->va_group_sid);

    attr->va_acl       = NULL;
    attr->va_owner_sid = NULL;
    attr->va_group_sid = NULL;
} /* chimera_vfs_compound_attr_release */

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

    compound->thread       = thread;
    compound->cred         = cred;
    compound->default_cred = cred;
    compound->status       = CHIMERA_VFS_OK;

    /* A recycled compound kept the one it was born with. */
    if (!compound->cancel_post) {
        chimera_vfs_compound_cancel_alloc(compound);
    }

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
        compound->build_error  = CHIMERA_VFS_EINVAL;
        return -1;
    }
    if (!compound->groups) {
        compound->groups = calloc(CHIMERA_VFS_COMPOUND_MAX_OPS, sizeof(*compound->groups));
        if (!compound->groups) {
            compound->build_failed = 1;
            compound->build_error  = CHIMERA_VFS_ENOSPC;
            return -1;
        }
    }
    struct chimera_vfs_compound_group *group = &compound->groups[compound->num_groups];
    group->config  = *config;
    group->status  = CHIMERA_VFS_UNSET;
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
chimera_vfs_compound_set_admission_cookie(
    struct chimera_vfs_compound *compound,
    const void                  *cookie)
{
    if (compound->running || compound->original_ops || !cookie) {
        return false;
    }
    compound->admission_cookie = cookie;
    return true;
} /* chimera_vfs_compound_set_admission_cookie */

SYMBOL_EXPORT bool
chimera_vfs_compound_set_cancel_scope(
    struct chimera_vfs_compound *compound,
    uint32_t                     start,
    uint32_t                     end)
{
    if (compound->running || compound->original_ops || start >= end || end >= compound->num_ops) {
        return false;
    }
    for (uint32_t i = 0; i < compound->num_ops; i++) {
        uint32_t prior_end = compound->ops[i]->cancel_scope_end;
        if (prior_end && start < prior_end && i <= end) {
            return false;
        }
    }
    compound->ops[start]->cancel_scope_end = end + 1;
    return true;
} /* chimera_vfs_compound_set_cancel_scope */

static bool
compound_cancel_stops(const struct chimera_vfs_compound *compound)
{
    return compound->canceled && !compound->cancel_defer_end;
} /* compound_cancel_stops */


static int chimera_vfs_compound_cancel_park(
    struct chimera_vfs_compound *compound);

SYMBOL_EXPORT bool
chimera_vfs_compound_cancel(struct chimera_vfs_compound *compound)
{
    if (!compound->running || compound->finishing || compound->finish_pending) {
        return false;
    }
    if (compound->park_state && !compound->cancel_defer_end) {
        return chimera_vfs_compound_cancel_park(compound);
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
} /* chimera_vfs_compound_is_canceled */

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
chimera_vfs_compound_set_park_cb(
    struct chimera_vfs_compound   *compound,
    chimera_vfs_compound_park_cb_t park_cb,
    void                          *private_data)
{
    compound->park_cb      = park_cb;
    compound->park_private = private_data;
} /* chimera_vfs_compound_set_park_cb */

/* The two cancellable parks -- see park_state on the compound, and the park
 * callback's contract in the header for why there are only two. */
#define CHIMERA_VFS_COMPOUND_PARK_NONE   0
#define CHIMERA_VFS_COMPOUND_PARK_CLAIM  1
#define CHIMERA_VFS_COMPOUND_PARK_RECALL 2

/*
 * The run has parked on `what`.  Record it -- that is the cancel handle --
 * and tell the caller, the first time only.
 *
 * Always on the submitting thread: a CLAIM's blocked_cb fires inside the
 * acquire the dispatch is making, and a RECALL's park is observed by the
 * dispatch itself when the recall call returns without answering.
 */
static void
chimera_vfs_compound_parked(
    struct chimera_vfs_compound *compound,
    uint8_t                      what)
{
    compound->park_state = what;

    if (!compound->park_cb || compound->park_fired) {
        return;
    }

    compound->park_fired = 1;

    /* The callback may emit and arm a timer; it may not re-enter us.  See
     * chimera_vfs_compound_cancel, which is what the guard is for. */
    compound->park_notifying = 1;
    compound->park_cb(compound, compound->index, compound->park_private);
    compound->park_notifying = 0;
} /* chimera_vfs_compound_parked */

/* A CLAIM's ticket queued: the claim core fires this exactly once, inside
 * the acquire, iff the acquire could not answer. */
static void
chimera_vfs_compound_claim_blocked(void *private_data)
{
    chimera_vfs_compound_parked(private_data,
                                CHIMERA_VFS_COMPOUND_PARK_CLAIM);
} /* chimera_vfs_compound_claim_blocked */

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
chimera_vfs_compound_take_access_owner(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    struct chimera_vfs_file_state **file)
{
    if (!file || compound->running || compound->finish_status != CHIMERA_VFS_OK ||
        index >= compound->num_ops) {
        return NULL;
    }
    struct chimera_vfs_compound_op *op = compound->ops[index];
    if (op->type == CHIMERA_VFS_COMPOUND_OP_CLAIM) {
        if (!op->access_owner || !op->lock_file_state ||
            op->claim_result != CHIMERA_CLAIM_GRANTED) {
            return NULL;
        }
        struct chimera_vfs_claim_access_owner *owner = op->access_owner;
        op->access_owner = NULL;
        *file = op->lock_file_state;
        op->lock_file_state       = NULL;
        compound->ownership_taken = 1;
        return owner;
    }
    if (!op->claim_held || !op->access_owner) {
        return NULL;
    }
    struct chimera_vfs_claim_access_owner *owner = op->access_owner;
    op->access_owner = NULL;
    *file = op->claim_file;
    op->claim_file            = NULL;
    op->claim_held            = 0;
    compound->ownership_taken = 1;
    return owner;
} /* chimera_vfs_compound_take_access_owner */

SYMBOL_EXPORT struct chimera_vfs_claim_owner *
chimera_vfs_compound_take_range_owner(
    struct chimera_vfs_compound *compound,
    uint32_t                     index)
{
    if (compound->running || compound->finish_status != CHIMERA_VFS_OK ||
        index >= compound->num_ops) {
        return NULL;
    }
    struct chimera_vfs_compound_op *op = compound->ops[index];
    if (!op->completed || op->status != CHIMERA_VFS_OK || !op->out_range_owner) {
        return NULL;
    }
    struct chimera_vfs_claim_owner *owner = op->out_range_owner;
    op->out_range_owner       = NULL;
    compound->ownership_taken = 1;
    return owner;
} /* chimera_vfs_compound_take_range_owner */

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
} /* compound_release_range_owners */

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
    compound->handle_nameless = 0;
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
    compound->saved_handle_nameless = 0;
    compound->saved_handle_origin   = 0;
} /* chimera_vfs_compound_release_saved */

static void
compound_search_keys_release(struct chimera_vfs_compound_op *op)
{
    for (uint32_t i = 0; i < op->kv_num_entries; i++) {
        free(op->kv_entries[i].key);
    }
    free(op->kv_entries);
    free(op->kv_next_key);
    op->kv_entries     = NULL;
    op->kv_next_key    = NULL;
    op->kv_num_entries = op->kv_next_key_len = op->kv_result_bytes = 0;
    op->kv_more        = false;
    op->kv_error       = CHIMERA_VFS_OK;
} /* compound_search_keys_release */

static void
chimera_vfs_compound_reset(struct chimera_vfs_compound *compound)
{
    struct chimera_vfs_thread  *thread = compound->thread;
    /* The cancel vehicle and its latch outlive the reset: the vehicle is the
     * compound's for life, and the latch may say a post is still riding the
     * doorbell towards it, which is precisely what the free below reads. */
    struct chimera_vfs_request *cancel_post  = compound->cancel_post;
    uint8_t                     cancel_state =
        atomic_load(&compound->cancel_post_state);
    uint32_t                    i;

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
        chimera_vfs_compound_attr_release(&compound->ops[i]->attr);
        chimera_vfs_compound_attr_release(&compound->ops[i]->pre_attr);
        chimera_vfs_compound_attr_release(&compound->ops[i]->dir_pre_attr);
        chimera_vfs_compound_attr_release(&compound->ops[i]->dir_post_attr);
        chimera_vfs_compound_attr_release(&compound->ops[i]->from_dir_pre_attr);
        chimera_vfs_compound_attr_release(&compound->ops[i]->from_dir_post_attr);
        free(compound->ops[i]->layout_segments);
        free(compound->ops[i]->layout_devices);
        if (compound->ops[i]->lock_file_state) {
            chimera_vfs_state_put(thread->vfs->vfs_state, compound->ops[i]->lock_file_state);
        }
        free(compound->ops[i]->applied_acl);
        /* An open handle the caller did not take: see OPEN HANDLE OWNERSHIP.
         * Releasing here is what makes "take it if you want it" safe, rather
         * than making every one of the caller's error paths responsible. */
        if (compound->ops[i]->out_handle) {
            chimera_vfs_release(thread, compound->ops[i]->out_handle);
        }
        /* Data the caller did not take, for the same reason and on the same
         * terms as the handle above. */
        if (compound->ops[i]->niov && !compound->ops[i]->dest_published) {
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

    compound->thread      = thread;
    compound->cancel_post = cancel_post;
    atomic_store(&compound->cancel_post_state, cancel_state);
} /* chimera_vfs_compound_reset */

/* The tail of the free: the compound holds nothing any more and goes back to
 * the thread's pool, or to the allocator when the pool is full -- in which
 * case its cancel vehicle goes back to the request pool with it. */
static void
chimera_vfs_compound_recycle(struct chimera_vfs_compound *compound)
{
    struct chimera_vfs_thread *thread = compound->thread;

    if (thread->num_free_compounds >= CHIMERA_VFS_COMPOUND_FREE_MAX) {
        chimera_vfs_compound_cancel_free(compound);
        for (uint32_t i = 0; i < CHIMERA_VFS_COMPOUND_MAX_OPS; i += 16) {
            free(compound->ops[i]);
        }
        free(compound);
        return;
    }

    LL_PREPEND(thread->free_compounds, compound);
    thread->num_free_compounds++;
} /* chimera_vfs_compound_recycle */

SYMBOL_EXPORT void
chimera_vfs_compound_free(struct chimera_vfs_compound *compound)
{
    chimera_vfs_compound_reset(compound);

    /*
     * A cross-thread cancel is still on the doorbell holding this pointer.
     * That is not the caller's mistake -- it posted while the run was alive
     * and the run then finished -- so the recycle waits rather than hand the
     * drain a compound that has been handed out again.  Everything the
     * compound held has been released above; only the memory is held back,
     * and only until the drain, which runs on this thread and is already
     * carrying the vehicle.
     *
     * The one thing that does not collect it is a thread torn down without
     * draining its doorbell first, which leaks this compound -- as it already
     * leaks every request still posted to that thread.  A server drains on
     * its event loop and a test drains before it destroys anything, so the
     * case is the same pathology, not a new one.
     */
    if (atomic_load(&compound->cancel_post_state) ==
        CHIMERA_VFS_COMPOUND_CANCEL_POSTED) {
        atomic_store(&compound->cancel_post_state,
                     CHIMERA_VFS_COMPOUND_CANCEL_ORPHAN);
        return;
    }

    chimera_vfs_compound_recycle(compound);
} /* chimera_vfs_compound_free */

void
chimera_vfs_compound_thread_destroy(struct chimera_vfs_thread *thread)
{
    struct chimera_vfs_compound *compound;

    while (thread->free_compounds) {
        compound = thread->free_compounds;
        LL_DELETE(thread->free_compounds, compound);
        /* Straight to the allocator, not back to the request pool: this runs
         * from chimera_vfs_thread_destroy, which has already emptied
         * thread->free_requests, and a request prepended to it now would
         * never be freed at all. */
        if (compound->cancel_post) {
            free(compound->cancel_post->plugin_data);
            free(compound->cancel_post);
            compound->cancel_post = NULL;
        }
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

    if (compound->running || compound->finish_status != CHIMERA_VFS_OK ||
        compound->ops[index]->dest_iov) {
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
        compound->build_error  = CHIMERA_VFS_ENOTSUP;
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
        group->last_op                            = *index;
    }

    return op;
} /* chimera_vfs_compound_next_op */

SYMBOL_EXPORT int
chimera_vfs_compound_add_range_owner(
    struct chimera_vfs_compound      *compound,
    const struct chimera_claim_actor *actor,
    bool                              zero_point)
{
    if (compound->running || compound->original_ops) {
        compound->build_failed = 1;
        compound->build_error  = CHIMERA_VFS_ENOTSUP;
        return -1;
    }
    int                             index;
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_next_op(compound,
                                                                      CHIMERA_VFS_COMPOUND_OP_RANGE_OWNER, &index);
    if (!op) {
        return -1;
    }
    if (actor) {
        op->io_owner = *actor; op->have_io_owner = true;
    }
    op->range_zero_point = zero_point;
    return index;
} /* chimera_vfs_compound_add_range_owner */

SYMBOL_EXPORT int
chimera_vfs_compound_add_range_batch(
    struct chimera_vfs_compound                *compound,
    struct chimera_vfs_claim_owner             *owner,
    const struct chimera_vfs_claim_exact_range *ranges,
    uint32_t                                    count,
    bool                                        unlock)
{
    if (compound->running || compound->original_ops || !count || !ranges ||
        count > CHIMERA_VFS_COMPOUND_MAX_OPS - compound->claim_journal_budget) {
        compound->build_failed = 1;
        compound->build_error  = CHIMERA_VFS_ENOTSUP;
        return -1;
    }
    uint32_t                          budget  = compound->claim_journal_budget + count;
    struct chimera_vfs_claim_journal *journal = chimera_vfs_claim_journal_alloc(
        CHIMERA_VFS_COMPOUND_MAX_OPS, budget);
    if (!journal) {
        compound->build_failed = 1;
        compound->build_error  = CHIMERA_VFS_ENOSPC;
        return -1;
    }
    chimera_vfs_claim_journal_free(compound->claim_journal);
    compound->claim_journal        = journal;
    compound->claim_journal_budget = budget;
    int                             index;
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_next_op(compound,
                                                                      CHIMERA_VFS_COMPOUND_OP_RANGE_BATCH, &index);
    if (!op) {
        return -1;
    }
    op->range_attempt = chimera_vfs_claim_range_attempt_alloc(compound->thread);
    if (!op->range_attempt) {
        compound->build_failed = 1;
        compound->build_error  = CHIMERA_VFS_ENOSPC;
        return -1;
    }
    op->range_owner      = owner;
    op->exact_ranges     = ranges;
    op->num_exact_ranges = count;
    op->range_unlock     = unlock;
    return index;
} /* chimera_vfs_compound_add_range_batch */

SYMBOL_EXPORT bool
chimera_vfs_compound_range_cancel(
    struct chimera_vfs_compound *compound,
    uint32_t                     index)
{
    return compound->running && !compound->finishing && !compound->finish_pending &&
           index == compound->index && index < compound->num_ops &&
           compound->ops[index]->type == CHIMERA_VFS_COMPOUND_OP_RANGE_BATCH &&
           chimera_vfs_claim_range_attempt_cancel(compound->ops[index]->range_attempt);
} /* chimera_vfs_compound_range_cancel */

SYMBOL_EXPORT bool
chimera_vfs_compound_io_denied(
    struct chimera_vfs_compound          *compound,
    const struct chimera_vfs_open_handle *handle,
    uint64_t                              offset,
    uint64_t                              length,
    bool                                  write,
    const struct chimera_claim_actor     *actor)
{
    struct chimera_vfs_state      *state = compound->thread->vfs->vfs_state;

    if (!compound->claim_journal) {
        return chimera_vfs_claim_io_denied(state, handle->fh, handle->fh_len,
                                           handle->fh_hash, offset, length, write, actor);
    }
    struct chimera_vfs_file_state *file = chimera_vfs_state_get(state, handle->fh,
                                                                handle->fh_len, handle->fh_hash, false);
    bool                           denied = chimera_vfs_claim_journal_io_denied(compound->claim_journal,
                                                                                file, offset, length, write, actor);
    if (file) {
        chimera_vfs_state_put(state, file);
    }
    return denied;
} /* chimera_vfs_compound_io_denied */

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
        compound->build_error  = CHIMERA_VFS_ENOTSUP;
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
    uint64_t                     attr_mask,
    uint64_t                     dir_attr_mask)
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
    op->dir_attr_mask = dir_attr_mask;

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
    uint64_t                     pre_attr_mask,
    uint64_t                     post_attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_COMMIT, &index);

    if (!op) {
        return -1;
    }

    op->offset        = offset;
    op->count         = count;
    op->pre_attr_mask = pre_attr_mask;
    op->attr_mask     = post_attr_mask;

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
chimera_vfs_compound_add_close_doc(
    struct chimera_vfs_compound *compound,
    unsigned int                 flags,
    const uint8_t               *parent_lease_skip)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_CLOSE, &index);

    if (!op) {
        return -1;
    }

    op->close_flags = flags;

    /* Copied, like every other lease key here: a caller assembling one in a
     * stack buffer should not have to keep it alive across the submission. */
    if (parent_lease_skip) {
        memcpy(op->namespace_parent_lease_key, parent_lease_skip,
               sizeof(op->namespace_parent_lease_key));
        op->namespace_parent_lease_key_valid = 1;
        memcpy(op->parent_lease_skip, parent_lease_skip, sizeof(op->parent_lease_skip));
        op->parent_lease_skip_valid = 1;
    }

    return index;
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
    int                             index;
    struct chimera_vfs_compound_op *op;

    if (from < -1 || (from >= 0 && (uint32_t) from >= compound->num_ops)) {
        compound->build_failed = 1;
        compound->build_error  = CHIMERA_VFS_EINVAL;
        return -1;
    }
    op = chimera_vfs_compound_next_op(compound, CHIMERA_VFS_COMPOUND_OP_PUTHANDLE, &index);
    if (!op) {
        return -1;
    }
    op->handle_from = from;
    op->open_flags  = open_flags;
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
    uint64_t                        attr_mask,
    uint8_t                         intermediates)
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

    /* Only a directory create walks a chain; recorded only there so the
     * dispatch of the other two never has to ask. */
    op->path_intermediates =
        (create_type == CHIMERA_VFS_COMPOUND_CREATE_DIR) && intermediates;

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
    return chimera_vfs_compound_add_create_path(compound, CHIMERA_VFS_COMPOUND_CREATE_DIR_TREE, path, pathlen, NULL, 0,
                                                set_attr, attr_mask | CHIMERA_VFS_ATTR_FH, 0);
} /* chimera_vfs_compound_add_create_tree */

SYMBOL_EXPORT int
chimera_vfs_compound_add_create_unlinked(
    struct chimera_vfs_compound    *compound,
    unsigned int                    flags,
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
    op->open_flags = flags;
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

/*
 * Which op types leave a handle in out_handle for a later op to address.
 *
 * The list is short and it is exactly the set of completions that assign
 * out_handle.  OPEN_CURRENT is the one that reads as if it belongs here and
 * does not: the handle it opens is owned by the CURSOR, and publishing it in
 * out_handle as well would release it twice -- GETHANDLE is what hands it over,
 * and that is the op to name.  A PUTHANDLE is the other near miss: the handle
 * is the caller's own, and the sequence addresses it as the current object,
 * which is what a PUTHANDLE is FOR.
 */
static int
chimera_vfs_compound_op_produces_handle(uint8_t type)
{
    switch (type) {
        case CHIMERA_VFS_COMPOUND_OP_OPEN:
        case CHIMERA_VFS_COMPOUND_OP_OPEN_PATH:
        case CHIMERA_VFS_COMPOUND_OP_OPEN_STREAM:
        case CHIMERA_VFS_COMPOUND_OP_CREATE_UNLINKED:
        case CHIMERA_VFS_COMPOUND_OP_GETHANDLE:
            return 1;
        default:
            return 0;
    } /* switch */
} /* chimera_vfs_compound_op_produces_handle */

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

    if (!chimera_vfs_compound_op_produces_handle(compound->ops[from]->type) ||
        compound->ops[from]->skip_build) {
        compound->build_failed = 1;
        return;
    }
    compound->ops[index]->handle_from = (int) from;
} /* chimera_vfs_compound_op_use_handle */

/*
 * The caller's own skip -- see the declaration for the precedence rule.
 *
 * It is a separate field from the gate's and not the same one set early,
 * because submit clears the gate's: sharing the field would mean a sequence
 * submitted twice ran a different shape the second time, silently.
 */
SYMBOL_EXPORT void
chimera_vfs_compound_op_set_skip(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    int                          skip)
{
    uint32_t i;

    if (index >= compound->num_ops) {
        return;
    }

    /* An op that produces the handle a later op addresses cannot be skipped:
     * that op would be handed NULL.  Refused at build, the way use_handle
     * refuses a source that never had a handle to give. */
    if (skip) {
        for (i = index + 1; i < compound->num_ops; i++) {
            if (compound->ops[i]->handle_from == (int) index) {
                compound->build_failed = 1;
                return;
            }
        }
    }

    compound->ops[index]->skip_build = skip ? 1 : 0;
} /* chimera_vfs_compound_op_set_skip */

/*
 * The name-op setters.  Each names an op that already exists and must be of
 * the type the knob belongs to: a knob on the wrong op is a caller bug, and a
 * sequence that quietly ran without it would be the worst way to find out.
 * An index past the end is the adder's failure, already reported through its
 * return value, and is ignored here as op_use_handle ignores one.
 */
static struct chimera_vfs_compound_op *
chimera_vfs_compound_op_of_type(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    uint8_t                      type_a,
    uint8_t                      type_b)
{
    struct chimera_vfs_compound_op *op;

    if (index >= compound->num_ops) {
        return NULL;
    }

    op = compound->ops[index];

    chimera_vfs_abort_if(op->type != type_a && op->type != type_b,
                         "compound: setter applied to op %u of type %u",
                         index, op->type);

    return op;
} /* chimera_vfs_compound_op_of_type */

/* The 16-byte directory lease key, copied; NULL clears it. */
static void
chimera_vfs_compound_op_set_lease_skip(
    struct chimera_vfs_compound_op *op,
    const uint8_t                  *parent_lease_skip)
{
    if (parent_lease_skip) {
        memcpy(op->namespace_parent_lease_key, parent_lease_skip,
               sizeof(op->namespace_parent_lease_key));
        op->namespace_parent_lease_key_valid = 1;
        memcpy(op->parent_lease_skip, parent_lease_skip, sizeof(op->parent_lease_skip));
        op->parent_lease_skip_valid = 1;
    } else {
        op->namespace_parent_lease_key_valid = 0;
        op->parent_lease_skip_valid          = 0;
    }
} /* chimera_vfs_compound_op_set_lease_skip */

SYMBOL_EXPORT void
chimera_vfs_compound_op_set_remove_match(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    const uint8_t               *child_fh,
    uint32_t                     child_fh_len,
    int                          match,
    const uint8_t               *parent_lease_skip)
{
    struct chimera_vfs_compound_op *op;

    op = chimera_vfs_compound_op_of_type(compound, index,
                                         CHIMERA_VFS_COMPOUND_OP_REMOVE,
                                         CHIMERA_VFS_COMPOUND_OP_REMOVE);

    if (!op) {
        return;
    }

    /* A match with nothing to match against, or an fh that cannot be one,
     * is refused the way an adder refuses a malformed argument: the op stays
     * in the sequence and submit reports the build failure, rather than an
     * unconditional remove running where a guarded one was written. */
    if (child_fh_len > CHIMERA_VFS_FH_SIZE ||
        (child_fh_len > 0 && !child_fh) ||
        (match && child_fh_len == 0)) {
        compound->build_failed = 1;
        return;
    }

    if (child_fh_len) {
        memcpy(op->arg_fh, child_fh, child_fh_len);
    }
    op->arg_fh_len            = child_fh_len;
    op->remove_match_child_fh = match ? 1 : 0;
    op->child_fh_match        = match ? 1 : 0;
    op->child_fh_len          = child_fh_len;
    if (child_fh_len) {
        memcpy(op->child_fh, child_fh, child_fh_len);
    }

    chimera_vfs_compound_op_set_lease_skip(op, parent_lease_skip);
} /* chimera_vfs_compound_op_set_remove_match */

SYMBOL_EXPORT void
chimera_vfs_compound_op_set_rename_opts(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    const uint8_t                  *target_fh,
    uint32_t                        target_fh_len,
    struct chimera_vfs_open_handle *op_exempt_handle,
    const uint8_t                  *parent_lease_skip,
    unsigned int                    flags)
{
    struct chimera_vfs_compound_op *op;

    op = chimera_vfs_compound_op_of_type(compound, index,
                                         CHIMERA_VFS_COMPOUND_OP_RENAME,
                                         CHIMERA_VFS_COMPOUND_OP_RENAME);

    if (!op) {
        return;
    }

    if (target_fh_len > CHIMERA_VFS_FH_SIZE ||
        (target_fh_len > 0 && !target_fh)) {
        compound->build_failed = 1;
        return;
    }

    if (target_fh_len) {
        memcpy(op->rename_target_fh, target_fh, target_fh_len);
    }
    op->rename_target_fh_len = target_fh_len;
    op->target_fh_len        = target_fh_len;
    if (target_fh_len) {
        memcpy(op->target_fh, target_fh, target_fh_len);
    }
    op->op_exempt_handle = op_exempt_handle;
    op->remove_flags    |= flags;
    op->rename_flags    |= flags;

    chimera_vfs_compound_op_set_lease_skip(op, parent_lease_skip);
} /* chimera_vfs_compound_op_set_rename_opts */

SYMBOL_EXPORT void
chimera_vfs_compound_op_set_link_opts(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    int                             replace,
    const uint8_t                  *parent_lease_skip,
    struct chimera_vfs_open_handle *op_exempt_handle)
{
    struct chimera_vfs_compound_op *op;

    op = chimera_vfs_compound_op_of_type(compound, index,
                                         CHIMERA_VFS_COMPOUND_OP_LINK,
                                         CHIMERA_VFS_COMPOUND_OP_LINK);

    if (!op) {
        return;
    }

    op->open_opts        = replace ? 1 : 0;
    op->link_replace     = replace ? 1 : 0;
    op->op_exempt_handle = op_exempt_handle;

    chimera_vfs_compound_op_set_lease_skip(op, parent_lease_skip);
} /* chimera_vfs_compound_op_set_link_opts */

SYMBOL_EXPORT void
chimera_vfs_compound_op_set_handle_state(
    struct chimera_vfs_compound     *compound,
    uint32_t                         index,
    struct chimera_vfs_handle_state *handle_state)
{
    struct chimera_vfs_compound_op *op;

    op = chimera_vfs_compound_op_of_type(compound, index,
                                         CHIMERA_VFS_COMPOUND_OP_OPEN,
                                         CHIMERA_VFS_COMPOUND_OP_OPEN_PATH);

    if (!op) {
        return;
    }

    op->handle_state = handle_state;
} /* chimera_vfs_compound_op_set_handle_state */

SYMBOL_EXPORT int
chimera_vfs_compound_add_readdir(
    struct chimera_vfs_compound *compound,
    uint64_t                     cookie,
    uint64_t                     verifier,
    uint32_t                     dircount,
    uint32_t                     maxcount,
    uint32_t                     max_entries,
    uint64_t                     attr_mask,
    uint64_t                     dir_attr_mask)
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

    op->cookie        = cookie;
    op->verifier      = verifier;
    op->dircount      = dircount;
    op->maxcount      = maxcount;
    op->max_entries   = max_entries;
    op->attr_mask     = attr_mask;
    op->dir_attr_mask = dir_attr_mask;

    return index;
} /* chimera_vfs_compound_add_readdir */

SYMBOL_EXPORT int
chimera_vfs_compound_add_readdir_stream(
    struct chimera_vfs_compound          *compound,
    uint64_t                              cookie,
    uint64_t                              verifier,
    uint64_t                              attr_mask,
    uint64_t                              dir_attr_mask,
    uint32_t                              flags,
    const char                           *pattern,
    uint32_t                              pattern_len,
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

    op->cookie              = cookie;
    op->verifier            = verifier;
    op->attr_mask           = attr_mask;
    op->dir_attr_mask       = dir_attr_mask;
    op->readdir_flags       = flags;
    op->readdir_pattern     = pattern;
    op->readdir_pattern_len = pattern_len;
    op->readdir_reset       = reset;
    op->readdir_append      = append;
    op->readdir_private     = private_data;

    return index;
} /* chimera_vfs_compound_add_readdir_stream */

SYMBOL_EXPORT int
chimera_vfs_compound_add_claim_test(
    struct chimera_vfs_compound *compound,
    struct chimera_vfs_claim    *claim,
    unsigned int                 flags)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    /* A probe never parks and never inserts, so the only flag that means
     * anything to it is the backend projection. */
    if (!claim || (flags & ~CHIMERA_VFS_COMPOUND_CLAIM_TEST_BACKEND)) {
        compound->build_failed = 1;
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_CLAIM_TEST,
                                      &index);

    if (!op) {
        return -1;
    }

    op->claim       = claim;
    op->claim_flags = flags;

    return index;
} /* chimera_vfs_compound_add_claim_test */

SYMBOL_EXPORT int
chimera_vfs_compound_add_claim(
    struct chimera_vfs_compound        *compound,
    struct chimera_vfs_claim           *claim,
    struct chimera_vfs_pending_acquire *ticket,
    unsigned int                        flags,
    uint8_t                             pre,
    uint8_t                             pre_retain,
    uint8_t                             deny,
    uint8_t                             deny_retain)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    /* The ticket is what a parked acquire lives in, so an acquire that may
     * wait has to have one.  Refusing here rather than at execution keeps the
     * sequence from being built at all, which build_failed then reports at
     * submit.  The same for the two ways the flag word can contradict
     * itself: TRY with a wait, and the probe-only backend projection. */
    if (!claim || !ticket ||
        ((flags & CHIMERA_VFS_COMPOUND_CLAIM_TRY) &&
         (flags & (CHIMERA_VFS_COMPOUND_CLAIM_WAIT |
                   CHIMERA_VFS_COMPOUND_CLAIM_WAIT_HARD))) ||
        (flags & CHIMERA_VFS_COMPOUND_CLAIM_TEST_BACKEND)) {
        compound->build_failed = 1;
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_CLAIM, &index);

    if (!op) {
        return -1;
    }

    op->claim              = claim;
    op->ticket             = ticket;
    op->claim_flags        = flags;
    op->claim_pre_trigger  = pre;
    op->claim_pre_retain   = pre_retain;
    op->claim_deny_trigger = deny;
    op->claim_deny_retain  = deny_retain;

    return index;
} /* chimera_vfs_compound_add_claim */

SYMBOL_EXPORT void
chimera_vfs_compound_op_set_claim_post(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    uint8_t                      post,
    uint8_t                      post_retain)
{
    if (index >= compound->num_ops ||
        compound->ops[index]->type != CHIMERA_VFS_COMPOUND_OP_CLAIM) {
        compound->build_failed = 1;
        return;
    }

    compound->ops[index]->claim_post_trigger = post;
    compound->ops[index]->claim_post_retain  = post_retain;
} /* chimera_vfs_compound_op_set_claim_post */

SYMBOL_EXPORT void
chimera_vfs_compound_op_set_claim_grant_opts(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    int                          is_v2,
    int                          cap_strict,
    void                        *member_seed)
{
    if (index >= compound->num_ops ||
        compound->ops[index]->type != CHIMERA_VFS_COMPOUND_OP_CLAIM) {
        compound->build_failed = 1;
        return;
    }

    compound->ops[index]->claim_is_v2       = is_v2 ? 1 : 0;
    compound->ops[index]->claim_cap_strict  = cap_strict ? 1 : 0;
    compound->ops[index]->claim_member_seed = member_seed;
} /* chimera_vfs_compound_op_set_claim_grant_opts */

SYMBOL_EXPORT struct chimera_vfs_file_state *
chimera_vfs_compound_take_file_state(
    struct chimera_vfs_compound *compound,
    uint32_t                     index)
{
    struct chimera_vfs_file_state *file_state;

    if (index >= compound->num_ops) {
        return NULL;
    }

    file_state                            = compound->ops[index]->lock_file_state;
    compound->ops[index]->lock_file_state = NULL;

    return file_state;
} /* chimera_vfs_compound_take_file_state */

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
chimera_vfs_compound_add_key(
    struct chimera_vfs_compound      *compound,
    enum chimera_vfs_compound_op_type type,
    const void                       *key,
    uint32_t                          key_len,
    const void                       *value,
    uint32_t                          value_len)
{
    if ((key_len && !key) || (value_len && !value)) {
        compound->build_failed = 1;
        compound->build_error  = CHIMERA_VFS_EINVAL;
        return -1;
    }
    if ((uint64_t) key_len + value_len > CHIMERA_VFS_PLUGIN_DATA_SIZE) {
        compound->build_failed = 1;
        compound->build_error  = CHIMERA_VFS_ERANGE;
        return -1;
    }
    int                             index;
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_next_op(compound, type, &index);
    if (!op) {
        return -1;
    }
    op->kv_key   = malloc(key_len ? key_len : 1);
    op->kv_value = malloc(value_len ? value_len : 1);
    if (!op->kv_key || !op->kv_value) {
        compound->build_failed = 1;
        compound->build_error  = CHIMERA_VFS_ENOSPC;
        return -1;
    }
    if (key_len) {
        memcpy(op->kv_key, key, key_len);
    }
    if (value_len) {
        memcpy(op->kv_value, value, value_len);
    }
    op->kv_key_len   = key_len;
    op->kv_value_len = value_len;
    return index;
} /* chimera_vfs_compound_add_key */

SYMBOL_EXPORT int
chimera_vfs_compound_add_put_key_at(
    struct chimera_vfs_compound *compound,
    const void                  *key,
    uint32_t                     key_len,
    const void                  *value,
    uint32_t                     value_len)
{
    return chimera_vfs_compound_add_key(compound, CHIMERA_VFS_COMPOUND_OP_PUT_KEY_AT,
                                        key, key_len, value, value_len);
} /* chimera_vfs_compound_add_put_key_at */

SYMBOL_EXPORT int
chimera_vfs_compound_add_delete_key_at(
    struct chimera_vfs_compound *compound,
    const void                  *key,
    uint32_t                     key_len)
{
    return chimera_vfs_compound_add_key(compound, CHIMERA_VFS_COMPOUND_OP_DELETE_KEY_AT,
                                        key, key_len, NULL, 0);
} /* chimera_vfs_compound_add_delete_key_at */

SYMBOL_EXPORT int
chimera_vfs_compound_add_search_keys_at(
    struct chimera_vfs_compound *compound,
    const void                  *start_key,
    uint32_t                     start_len,
    const void                  *end_key,
    uint32_t                     end_len,
    uint32_t                     flags,
    uint32_t                     max_entries,
    uint32_t                     max_bytes)
{
    if (!max_entries || max_entries > 65536 || !max_bytes || max_bytes > 16 * 1024 * 1024 ||
        start_len > 4096 || end_len > 4096 ||
        (uint64_t) start_len + end_len > CHIMERA_VFS_PLUGIN_DATA_SIZE - 128 ||
        (flags & ~CHIMERA_VFS_SEARCH_KEYS_END_EXCLUSIVE)) {
        compound->build_failed = 1;
        compound->build_error  = CHIMERA_VFS_ERANGE;
        return -1;
    }
    int                             index = chimera_vfs_compound_add_key(compound,
                                                                         CHIMERA_VFS_COMPOUND_OP_SEARCH_KEYS_AT,
                                                                         start_key, start_len, end_key, end_len);
    if (index < 0) {
        return -1;
    }
    struct chimera_vfs_compound_op *op = compound->ops[index];
    op->kv_flags       = flags;
    op->kv_max_entries = max_entries;
    op->kv_max_bytes   = max_bytes;
    return index;
} /* chimera_vfs_compound_add_search_keys_at */

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
compound_add_stream_name(
    struct chimera_vfs_compound      *compound,
    enum chimera_vfs_compound_op_type type,
    const char                       *name,
    int                               namelen)
{
    int                             index;

    if (!name || namelen <= 0 || namelen > CHIMERA_VFS_COMPOUND_NAME_MAX) {
        compound->build_failed = 1;
        compound->build_error  = namelen > CHIMERA_VFS_COMPOUND_NAME_MAX ?
            CHIMERA_VFS_ENAMETOOLONG : CHIMERA_VFS_EINVAL;
        return -1;
    }
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_next_op(compound, type, &index);
    if (!op) {
        return -1;
    }
    memcpy(op->name, name, namelen);
    op->name[namelen] = 0;
    op->name_len      = namelen;
    return index;
} /* compound_add_stream_name */

SYMBOL_EXPORT int
chimera_vfs_compound_add_open_stream(
    struct chimera_vfs_compound    *compound,
    const char                     *name,
    int                             namelen,
    unsigned int                    flags,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask)
{
    int                             index = compound_add_stream_name(compound, CHIMERA_VFS_COMPOUND_OP_OPEN_STREAM, name
                                                                     , namelen);

    if (index < 0) {
        return index;
    }
    struct chimera_vfs_compound_op *op = compound->ops[index];
    op->open_flags = flags;
    op->attr_mask  = attr_mask;
    if (set_attr) {
        op->set_attr = *set_attr;
    }
    return index;
} /* chimera_vfs_compound_add_open_stream */

SYMBOL_EXPORT int
chimera_vfs_compound_add_list_streams(
    struct chimera_vfs_compound *compound,
    uint64_t                     cookie,
    uint32_t                     max_bytes,
    bool                         want_fh)
{
    int                             index;
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_next_op(compound,
                                                                      CHIMERA_VFS_COMPOUND_OP_LIST_STREAMS, &index);

    if (!op) {
        return -1;
    }
    op->cookie         = cookie;
    op->buffer_max     = max_bytes;
    op->stream_want_fh = want_fh;
    return index;
} /* chimera_vfs_compound_add_list_streams */

SYMBOL_EXPORT int
chimera_vfs_compound_add_remove_stream(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen)
{
    return compound_add_stream_name(compound, CHIMERA_VFS_COMPOUND_OP_REMOVE_STREAM, name, namelen);
} /* chimera_vfs_compound_add_remove_stream */

SYMBOL_EXPORT int
chimera_vfs_compound_add_remove_stream_checked(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen,
    const uint8_t               *expected_fh,
    uint32_t                     expected_fh_len)
{
    if (!expected_fh || !expected_fh_len || expected_fh_len > CHIMERA_VFS_FH_SIZE) {
        compound->build_failed = 1;
        return -1;
    }
    int                             index = chimera_vfs_compound_add_remove_stream(compound, name, namelen);
    if (index < 0) {
        return index;
    }
    struct chimera_vfs_compound_op *op = compound->ops[index];
    op->remove_flags = CHIMERA_VFS_REMOVE_STREAM_MATCH_FH;
    op->arg_fh_len   = expected_fh_len;
    memcpy(op->arg_fh, expected_fh, expected_fh_len);
    return index;
} /* chimera_vfs_compound_add_remove_stream_checked */

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
chimera_vfs_compound_add_get_layout(
    struct chimera_vfs_compound *compound,
    uint64_t                     offset,
    uint64_t                     length,
    uint32_t                     iomode,
    uint32_t                     layout_class,
    uint32_t                     max_segments)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_GET_LAYOUT,
                                      &index);

    if (!op) {
        return -1;
    }

    op->layout_offset       = offset;
    op->layout_length       = length;
    op->layout_iomode       = iomode;
    op->layout_class        = layout_class;
    op->layout_max_segments = max_segments;

    return index;
} /* chimera_vfs_compound_add_get_layout */


SYMBOL_EXPORT int
chimera_vfs_compound_add_recall(
    struct chimera_vfs_compound *compound,
    const uint8_t               *fh,
    uint32_t                     fh_len,
    uint8_t                      retain,
    unsigned int                 flags)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    /* An fh that cannot be one, and a floor on the shape that has none: the
     * full recall breaks every holder all the way down, so a NOWAIT with a
     * retain would quietly do more than was written. */
    if (fh_len > CHIMERA_VFS_FH_SIZE ||
        (fh_len > 0 && !fh) ||
        ((flags & CHIMERA_VFS_COMPOUND_RECALL_NOWAIT) && retain != 0)) {
        compound->build_failed = 1;
        return -1;
    }

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_RECALL, &index);

    if (!op) {
        return -1;
    }

    if (fh_len) {
        memcpy(op->recall_fh, fh, fh_len);
    }
    op->recall_fh_len = fh_len;
    op->recall_retain = retain;
    op->recall_flags  = flags;

    return index;
} /* chimera_vfs_compound_add_recall */

SYMBOL_EXPORT int
chimera_vfs_compound_add_create(
    struct chimera_vfs_compound    *compound,
    uint8_t                         create_type,
    const char                     *name,
    int                             namelen,
    const char                     *target,
    int                             targetlen,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask,
    uint64_t                        dir_pre_attr_mask,
    uint64_t                        dir_post_attr_mask)
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
    op->name[namelen]     = '\0';
    op->name_len          = (uint32_t) namelen;
    op->create_type       = create_type;
    op->attr_mask         = attr_mask;
    op->dir_pre_attr_mask = dir_pre_attr_mask;
    op->dir_attr_mask     = dir_post_attr_mask;

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
    unsigned int                 flags,
    uint64_t                     dir_pre_attr_mask,
    uint64_t                     dir_post_attr_mask)
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
    op->name[namelen]     = '\0';
    op->name_len          = (uint32_t) namelen;
    op->remove_flags      = flags;
    op->rename_flags      = flags;
    op->dir_pre_attr_mask = dir_pre_attr_mask;
    op->dir_attr_mask     = dir_post_attr_mask;

    return index;
} /* chimera_vfs_compound_add_remove */

SYMBOL_EXPORT int
chimera_vfs_compound_add_rename(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen,
    const char                  *new_name,
    int                          new_namelen,
    unsigned int                 flags,
    uint64_t                     dir_pre_attr_mask,
    uint64_t                     dir_post_attr_mask)
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

    op->remove_flags      = flags;
    op->rename_flags      = flags;
    op->dir_pre_attr_mask = dir_pre_attr_mask;
    op->dir_attr_mask     = dir_post_attr_mask;

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
    uint64_t                     attr_mask,
    uint64_t                     dir_pre_attr_mask,
    uint64_t                     dir_post_attr_mask)
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
    op->name[namelen]     = '\0';
    op->name_len          = (uint32_t) namelen;
    op->attr_mask         = attr_mask;
    op->dir_pre_attr_mask = dir_pre_attr_mask;
    op->dir_attr_mask     = dir_post_attr_mask;

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
    uint64_t                          attr_mask,
    const struct chimera_claim_actor *io_owner,
    struct evpl_iovec                *dest_iov,
    int                               dest_niov)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    if (max_iov <= 0 || !iov) {
        compound->build_failed = 1;
        return -1;
    }

    /* A destination is a pointer AND a count, or neither; and read_into has
     * no owned variant, so a read that names a lease owner cannot also land
     * in the caller's buffers -- refused here rather than quietly dropping
     * the owner, which would have the claim layer recall the caller's own
     * delegation for its own read. */
    if ((dest_iov != NULL) != (dest_niov > 0) ||
        (dest_iov && io_owner)) {
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
    op->attr_mask = attr_mask;

    if (io_owner) {
        op->io_owner      = *io_owner;
        op->have_io_owner = 1;
    }
    op->offset    = offset;
    op->count     = count;
    op->max_iov   = max_iov;
    op->dest_iov  = dest_iov;
    op->dest_niov = dest_niov;
    if (dest_iov) {
        uint64_t capacity = 0;
        for (int i = 0; i < dest_niov; i++) {
            capacity += evpl_iovec_length(&dest_iov[i]);
        }
        if (capacity < count) {
            compound->build_failed = 1; return -1;
        }
    }

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
    uint64_t                          pre_attr_mask,
    uint64_t                          post_attr_mask,
    const struct chimera_claim_actor *io_owner)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_WRITE, &index);

    if (!op) {
        return -1;
    }

    op->in_handle     = handle;
    op->offset        = offset;
    op->count         = count;
    op->sync          = sync;
    op->w_iov         = iov;
    op->w_niov        = niov;
    op->pre_attr_mask = pre_attr_mask;
    op->attr_mask     = post_attr_mask;

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
    uint64_t                        pre_attr_mask,
    uint64_t                        attr_mask)
{
    struct chimera_vfs_compound_op *op;
    int                             index;

    op = chimera_vfs_compound_next_op(compound,
                                      CHIMERA_VFS_COMPOUND_OP_SETATTR, &index);

    if (!op) {
        return -1;
    }

    op->in_handle     = handle;
    op->pre_attr_mask = pre_attr_mask;
    op->attr_mask     = attr_mask;

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
    int index = chimera_vfs_compound_add_setattr(compound, handle, set_attr, 0, attr_mask);

    if (index >= 0) {
        struct chimera_vfs_compound_op *op = compound->ops[index];
        op->type = CHIMERA_VFS_COMPOUND_OP_OVERWRITE;
        if (io_owner) {
            op->io_owner      = *io_owner;
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
    uint64_t                        attr_mask,
    uint64_t                        dir_pre_attr_mask,
    uint64_t                        dir_post_attr_mask)
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

    op->open_flags        = flags;
    op->open_opts         = opts;
    op->dir_pre_attr_mask = dir_pre_attr_mask;
    op->dir_attr_mask     = dir_post_attr_mask;
    op->attr_mask         = attr_mask;

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
chimera_vfs_compound_add_reserve_access(
    struct chimera_vfs_compound *compound,
    uint32_t                     handle_from,
    struct chimera_vfs_claim    *template_claim)
{
    int index = chimera_vfs_compound_add_reserve(compound, handle_from, template_claim);

    if (index >= 0) {
        compound->ops[index]->type = CHIMERA_VFS_COMPOUND_OP_RESERVE_ACCESS;
    }
    return index;
} /* chimera_vfs_compound_add_reserve_access */

/* Follow execution links: a dynamic suffix can have larger physical indices
 * than a following prebuilt group. Endpoints must be ordered in one group. */
static bool
compound_access_ready_in_group(
    const struct chimera_vfs_compound *compound,
    uint32_t                           group_index,
    uint32_t                           reserve_index,
    uint32_t                           ready_index)
{
    bool seen = false;

    for (int32_t i = compound->groups[group_index].config.first_op; i >= 0;
         i = compound->ops[i]->group_next) {
        if ((uint32_t) i == ready_index) {
            return seen;
        }
        if ((uint32_t) i == reserve_index) {
            seen = true;
        }
    }
    return false;
} /* compound_access_ready_in_group */

SYMBOL_EXPORT bool
chimera_vfs_compound_reserve_access_until(
    struct chimera_vfs_compound *compound,
    uint32_t                     reserve_index,
    uint32_t                     ready_index)
{
    if (reserve_index >= compound->num_ops || ready_index >= compound->num_ops ||
        compound->ops[reserve_index]->type != CHIMERA_VFS_COMPOUND_OP_RESERVE_ACCESS ||
        compound->ops[ready_index]->type != CHIMERA_VFS_COMPOUND_OP_CHECKPOINT ||
        compound->ops[reserve_index]->prepared || compound->ops[ready_index]->prepared ||
        (compound->running && (!compound->completing || !compound->num_groups ||
                               !compound_access_ready_in_group(compound, compound->group_index, reserve_index,
                                                               ready_index)))) {
        compound->build_failed = 1;
        compound->build_error  = CHIMERA_VFS_EINVAL;
        return false;
    }
    compound->ops[reserve_index]->access_ready = ready_index + 1;
    return true;
} /* chimera_vfs_compound_reserve_access_until */

SYMBOL_EXPORT int
chimera_vfs_compound_add_narrow_access(
    struct chimera_vfs_compound *compound,
    uint32_t                     reserve_index,
    uint8_t                      used,
    uint8_t                      denied)
{
    if (reserve_index >= compound->num_ops ||
        compound->ops[reserve_index]->type != CHIMERA_VFS_COMPOUND_OP_RESERVE_ACCESS ||
        (compound->running && (!compound->completing || !compound->num_groups))) {
        compound->build_failed = 1;
        compound->build_error  = CHIMERA_VFS_EINVAL;
        return -1;
    }
    if (!compound->access_journal) {
        compound->access_journal = chimera_vfs_claim_access_journal_alloc(CHIMERA_VFS_COMPOUND_MAX_OPS);
        if (!compound->access_journal) {
            compound->build_failed = 1;
            compound->build_error  = CHIMERA_VFS_ENOSPC;
            return -1;
        }
    }
    int                             index;
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_next_op(compound,
                                                                      CHIMERA_VFS_COMPOUND_OP_NARROW_ACCESS, &index);
    if (!op) {
        return -1;
    }
    op->access_narrow_from   = reserve_index;
    op->access_narrow_used   = used;
    op->access_narrow_denied = denied;
    return index;
} /* chimera_vfs_compound_add_narrow_access */

SYMBOL_EXPORT int
chimera_vfs_compound_add_retire_access(
    struct chimera_vfs_compound           *compound,
    struct chimera_vfs_claim_access_owner *owner)
{
    if (compound->running || compound->original_ops) {
        compound->build_failed = 1;
        compound->build_error  = CHIMERA_VFS_ENOTSUP;
        return -1;
    }
    if (!compound->access_journal) {
        compound->access_journal = chimera_vfs_claim_access_journal_alloc(CHIMERA_VFS_COMPOUND_MAX_OPS);
        if (!compound->access_journal) {
            compound->build_failed = 1;
            compound->build_error  = CHIMERA_VFS_ENOSPC;
            return -1;
        }
    }
    int                             index;
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_next_op(compound,
                                                                      CHIMERA_VFS_COMPOUND_OP_RETIRE_ACCESS, &index);
    if (!op) {
        return -1;
    }
    op->access_retire_owner = owner;
    return index;
} /* chimera_vfs_compound_add_retire_access */

SYMBOL_EXPORT int
chimera_vfs_compound_add_retire_open_claims(
    struct chimera_vfs_compound           *compound,
    struct chimera_vfs_claim_owner        *range_owner,
    struct chimera_vfs_claim_access_owner *access_owner,
    struct chimera_vfs_claim_access_owner *base_access_owner)
{
    int index = chimera_vfs_compound_add_retire_access(compound, access_owner);

    if (index < 0) {
        return -1;
    }
    if (!compound->claim_journal) {
        compound->claim_journal = chimera_vfs_claim_journal_alloc(CHIMERA_VFS_COMPOUND_MAX_OPS, 1);
        if (!compound->claim_journal) {
            compound->build_failed = 1;
            compound->build_error  = CHIMERA_VFS_ENOSPC;
            return -1;
        }
    }
    struct chimera_vfs_compound_op *op = compound->ops[index];
    op->type                     = CHIMERA_VFS_COMPOUND_OP_RETIRE_OPEN_CLAIMS;
    op->range_retire_owner       = range_owner;
    op->base_access_retire_owner = base_access_owner;
    return index;
} /* chimera_vfs_compound_add_retire_open_claims */

SYMBOL_EXPORT int
chimera_vfs_compound_add_retire_range_owner(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_claim_owner *range_owner)
{
    return chimera_vfs_compound_add_retire_open_claims(compound, range_owner, NULL, NULL);
} /* chimera_vfs_compound_add_retire_range_owner */

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
        index = chimera_vfs_compound_add_link(compound, name, namelen, attr_mask, 0, 0);
    }
    if (index >= 0) {
        compound->ops[index]->open_opts = 1;
    }
    return index;
} /* chimera_vfs_compound_add_link_replace */

/* ---------------------------------------------------------------------- */
/* Execution                                                              */
/* ---------------------------------------------------------------------- */

/*
 * A sequence stopped short of finishing: release every claim a CLAIM in it
 * inserted, so the caller sees a failed sequence with nothing inserted.
 *
 * Safe in the way a release generally is not, because nobody outside this
 * sequence has been told about the grant: the caller has not seen it (the
 * completion has not fired) and the clients it blocked meanwhile were handed
 * nothing they could act on.  The op keeps its own status and claim_result --
 * it did run, and the arbiter did say GRANTED -- and what says the claim is
 * gone is that chimera_vfs_compound_take_file_state() answers NULL for it.
 *
 * Only ops that RAN are looked at (completed, not num_ops), and only a CLAIM
 * still holding its file state has a claim to release: a refused CLAIM put
 * its state back in the callback, and there is no earlier point at which the
 * state can have been taken.
 *
 * Each kind is released the way its consumer releases it: a coalition grant
 * by dropping the reference the acquire took (chimera_vfs_claim_grant_
 * release -- a coalesce hit bumped an existing grant's refcount, a fresh
 * grant was born at one, and either way this drops exactly that), a range
 * with chimera_vfs_claim_release_ranged, a share or a single-holder cache
 * with chimera_vfs_claim_release.
 */
static void
chimera_vfs_compound_abort_claims(struct chimera_vfs_compound *compound)
{
    struct chimera_vfs_thread      *thread    = compound->thread;
    struct chimera_vfs_state       *vfs_state = thread->vfs->vfs_state;
    struct chimera_vfs_compound_op *op;
    uint32_t                        i;

    for (i = 0; i < compound->completed; i++) {
        op = compound->ops[i];

        if (op->type != CHIMERA_VFS_COMPOUND_OP_CLAIM ||
            !op->lock_file_state ||
            op->claim_result != CHIMERA_CLAIM_GRANTED) {
            continue;
        }

        if (op->access_owner) {
            chimera_vfs_claim_access_owner_retire(op->access_owner);
        } else if (op->claim_grant) {
            chimera_vfs_claim_grant_release(vfs_state, op->claim_grant,
                                            true /* pump */);
            op->claim_grant         = NULL;
            op->claim_member_seeded = 0;
        } else if (op->claim->klass == CHIMERA_CLAIM_CLASS_RANGE) {
            chimera_vfs_claim_release_ranged(thread, vfs_state,
                                             op->lock_file_state, op->claim);
        } else {
            chimera_vfs_claim_release(vfs_state, op->lock_file_state,
                                      op->claim);
        }

        chimera_vfs_state_put(vfs_state, op->lock_file_state);
        op->lock_file_state = NULL;
    }
} /* chimera_vfs_compound_abort_claims */

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
    return op->result_masks_set ? op->result_attr_mask : (fallback | op->attr_mask);
} /* compound_object_mask */

static uint64_t
compound_pre_mask(
    const struct chimera_vfs_compound_op *op,
    uint64_t                              fallback)
{
    return op->result_masks_set ? op->result_pre_attr_mask :
           (fallback | op->pre_attr_mask | op->dir_pre_attr_mask);
} /* compound_pre_mask */

static uint64_t
compound_post_mask(
    const struct chimera_vfs_compound_op *op,
    uint64_t                              fallback)
{
    uint64_t mask = op->dir_attr_mask;

    if (op->type == CHIMERA_VFS_COMPOUND_OP_WRITE ||
        op->type == CHIMERA_VFS_COMPOUND_OP_COMMIT ||
        op->type == CHIMERA_VFS_COMPOUND_OP_SETATTR) {
        mask |= op->attr_mask;
    }
    return op->result_masks_set ? op->result_post_attr_mask : (fallback | mask);
} /* compound_post_mask */

/* Before/after directory snapshots never borrow a callback-scoped ACL. */
static void
compound_store_aux(
    struct chimera_vfs_compound_op *op,
    struct chimera_vfs_attrs       *dst,
    const struct chimera_vfs_attrs *src)
{
    if (!src) {
        return;
    }
    chimera_vfs_compound_attr_release(dst);
    *dst              = *src;
    dst->va_acl       = NULL;
    dst->va_owner_sid = NULL;
    dst->va_group_sid = NULL;
    if ((src->va_set_mask & CHIMERA_VFS_ATTR_ACL) && src->va_acl &&
        src->va_acl->num_aces <= CHIMERA_ACL_MAX_ACES) {
        size_t size = chimera_acl_size(src->va_acl->num_aces);
        dst->va_acl = malloc(size);
        if (!dst->va_acl) {
            op->result_error = CHIMERA_VFS_ENOSPC; return;
        }
        memcpy(dst->va_acl, src->va_acl, size);
    } else {
        dst->va_set_mask &= ~CHIMERA_VFS_ATTR_ACL;
    }
    if ((src->va_set_mask & CHIMERA_VFS_ATTR_OWNER_SID) && chimera_sid_present(src->va_owner_sid)) {
        dst->va_owner_sid = malloc(sizeof(*dst->va_owner_sid));
        if (!dst->va_owner_sid) {
            op->result_error = CHIMERA_VFS_ENOSPC; return;
        }
        *dst->va_owner_sid = *src->va_owner_sid;
    } else {
        dst->va_set_mask &= ~CHIMERA_VFS_ATTR_OWNER_SID;
    }
    if ((src->va_set_mask & CHIMERA_VFS_ATTR_GROUP_SID) && chimera_sid_present(src->va_group_sid)) {
        dst->va_group_sid = malloc(sizeof(*dst->va_group_sid));
        if (!dst->va_group_sid) {
            op->result_error = CHIMERA_VFS_ENOSPC; return;
        }
        *dst->va_group_sid = *src->va_group_sid;
    } else {
        dst->va_set_mask &= ~CHIMERA_VFS_ATTR_GROUP_SID;
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
                    op->status = lock_status;
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
    if (status == CHIMERA_VFS_OK) {
        for (uint32_t i = 0; i < compound->num_ops; i++) {
            struct chimera_vfs_compound_op *op = compound->ops[i];
            if (!op->completed || op->status != CHIMERA_VFS_OK || !op->dest_iov) {
                continue;
            }
            size_t                          remaining = op->read_len, src_off = 0, dst_off = 0;
            int                             src = 0, dst = 0;
            while (remaining) {
                size_t src_left = evpl_iovec_length(&op->iov[src]) - src_off;
                size_t dst_left = evpl_iovec_length(&op->dest_iov[dst]) - dst_off;
                size_t length   = src_left < dst_left ? src_left : dst_left;
                if (length > remaining) {
                    length = remaining;
                }
                memcpy((char *) evpl_iovec_data(&op->dest_iov[dst]) + dst_off,
                       (char *) evpl_iovec_data(&op->iov[src]) + src_off, length);
                remaining -= length; src_off += length; dst_off += length;
                if (src_off == evpl_iovec_length(&op->iov[src])) {
                    src++; src_off = 0;
                }
                if (dst_off == evpl_iovec_length(&op->dest_iov[dst])) {
                    dst++; dst_off = 0;
                }
            }
            evpl_iovecs_release(compound->thread->evpl, op->iov, op->niov);
            op->iov                   = op->dest_iov;
            op->niov                  = op->dest_niov;
            op->dest_published        = 1;
            compound->ownership_taken = 1;
        }
    }
    compound->finishing     = 0;
    compound->finish_status = status;
    compound->status        = status == CHIMERA_VFS_OK ? compound->execution_status : status;
    compound->running       = 0;
    if (compound->status != CHIMERA_VFS_OK) {
        chimera_vfs_compound_abort_claims(compound);
    } else {
        for (uint32_t i = 0; i < compound->num_ops; i++) {
            if (compound->ops[i]->type == CHIMERA_VFS_COMPOUND_OP_CLAIM &&
                compound->ops[i]->lock_file_state &&
                compound->ops[i]->claim_result == CHIMERA_CLAIM_GRANTED) {
                compound->ownership_taken = 1;
            }
        }
    }
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

/*
 * The gate's index rule, enforced where it can be cheaply enforced.
 *
 * A gate may edit the ARGUMENTS of any op AHEAD of the one it is consulted on,
 * and may not touch one at or below it -- those have run, and their arguments
 * are what they ran with.  Nothing can stop a gate writing wherever it likes,
 * so what this does is NOTICE: it fingerprints the argument region of every op
 * at or below the gate's index before the call and again after, and aborts on a
 * difference.  A gate that quietly rewrites an op that has already run produces
 * a sequence that did something other than what the results say it did, which is
 * the one way a gate edit can corrupt a run.
 *
 * Hash the entire completed operation, including result ownership. The
 * status passed to the gate is separate; a gate must not rewrite either the
 * inputs or the results of operations that have already executed.
 *
 * DEBUG ONLY.  The cost is one pass over up to the whole sequence per op
 * completion, paid only by a run that HAS a gate; that is cheap enough to leave
 * on while the tests run and not cheap enough to leave on in production, where
 * the caller being checked is the VFS's own front ends.
 */
#ifndef NDEBUG
#define CHIMERA_VFS_COMPOUND_ARG_OFFSET 0
#define CHIMERA_VFS_COMPOUND_ARG_LEN    sizeof(struct chimera_vfs_compound_op)

static uint64_t
chimera_vfs_compound_arg_fingerprint(
    const struct chimera_vfs_compound *compound,
    uint32_t                           through)
{
    const uint8_t *bytes;
    uint64_t       hash = 14695981039346656037UL;
    uint32_t       i;
    size_t         j;

    for (i = 0; i <= through; i++) {
        bytes = (const uint8_t *) compound->ops[i] +
            CHIMERA_VFS_COMPOUND_ARG_OFFSET;

        for (j = 0; j < CHIMERA_VFS_COMPOUND_ARG_LEN; j++) {
            hash ^= bytes[j];
            hash *= 1099511628211UL;
        }
    }

    return hash;
} /* chimera_vfs_compound_arg_fingerprint */
#endif /* ifndef NDEBUG */

/* Withdraw only explicitly private, failed producer admissions. Keep token and
* file storage alive: frontend aliases and an earlier journal retirement can
* still reference it. A journal-held token is already excluded in this attempt
* and its cutoff drains after the journal, preserving prior accepted deltas. */
static void
compound_finish_private_reservations(
    struct chimera_vfs_compound       *compound,
    struct chimera_vfs_compound_group *group,
    enum chimera_vfs_error             status)
{
    for (int32_t i = group->config.first_op; i >= 0; i = compound->ops[i]->group_next) {
        struct chimera_vfs_compound_op       *op = compound->ops[i];
        if (!op->access_ready || !op->access_owner || !op->claim_held) {
            continue;
        }
        const struct chimera_vfs_compound_op *ready = compound->ops[op->access_ready - 1];
        if (status == CHIMERA_VFS_OK && ready->completed && !ready->skipped &&
            ready->status == CHIMERA_VFS_OK) {
            continue;
        }
        op->claim_held = 0;
        chimera_vfs_claim_access_owner_retire(op->access_owner);
    }
} /* compound_finish_private_reservations */

/* Complete a group without masking its error or executing skipped callouts. */
static void
chimera_vfs_compound_group_done(
    struct chimera_vfs_compound *compound,
    enum chimera_vfs_error       status)
{
    struct chimera_vfs_compound_group *group = &compound->groups[compound->group_index];

    compound->cancel_defer_end = 0;
    group->status              = status;
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
    compound->index        = compound->group_index == compound->num_groups ? compound->num_ops :
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

    /* Whatever the op was parked on, it is not parked on it now -- every arm
     * of every park ends here.  Clearing it is what makes a cancel arriving
     * after the answer a no-op rather than a second completion. */
    compound->park_state     = CHIMERA_VFS_COMPOUND_PARK_NONE;
    compound->recall_request = NULL;

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
#ifndef NDEBUG
        uint64_t before = chimera_vfs_compound_arg_fingerprint(compound,
                                                               compound->index);
#endif /* ifndef NDEBUG */

        compound->gating     = 1;
        compound->gate_index = compound->index;

        compound->gate(compound, compound->index, &status,
                       compound->gate_private);

        compound->gating = 0;

#ifndef NDEBUG
        chimera_vfs_abort_if(
            before != chimera_vfs_compound_arg_fingerprint(compound,
                                                           compound->index),
            "compound gate edited op %u or an op before it; a gate may only "
            "edit ops that have not run", compound->index);
#endif /* ifndef NDEBUG */
    }

    done->status = status;
    if (compound->cancel_defer_end == compound->index + 1) {
        compound->cancel_defer_end = 0;
    }

    compound->open_resolved  = 0;
    compound->open_retried   = 0;
    compound->io_typechecked = 0;
    if (compound->num_groups) {
        if (status != CHIMERA_VFS_OK || done->group_next < 0 || compound_cancel_stops(compound)) {
            chimera_vfs_compound_group_done(compound,
                                            status == CHIMERA_VFS_OK && compound_cancel_stops(compound) ?
                                            CHIMERA_VFS_EINTR : status);
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
    memo->index        = compound->index;
    memo->start        = op->coordinate;
    memo->private_data = op->coordinate_private;
    memo->fh_len       = compound->fh_len;
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
    compound_store_aux(op, &op->attr, attr);
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

    compound_store_aux(op, &op->dir_post_attr, dir_attr);

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

    compound_store_aux(op, &op->pre_attr, pre_attr);
    compound_store_aux(op, &op->dir_pre_attr, pre_attr);
    compound_store_aux(op, &op->dir_post_attr, post_attr);

    if (post_attr) {
        chimera_vfs_compound_store_attr(op, post_attr);
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
    /* The one attribute result that is NOT by value: a staged entry is a
     * fixed-size record by design (see the READDIR page note in the header),
     * and an ACL is not -- up to CHIMERA_ACL_MAX_ACES of 88 bytes, times a
     * page of up to 512 entries.  The backend owns the ACL and the SIDs only
     * while this callback runs, so they are dropped here, bits and pointers
     * together; a caller that wants per-entry ACLs streams (readdir_append is
     * handed the live attrs) as every consumer that marshals them does. */
    dirent->attr.va_acl       = NULL;
    dirent->attr.va_owner_sid = NULL;
    dirent->attr.va_group_sid = NULL;
    dirent->attr.va_req_mask &= ~(CHIMERA_VFS_ATTR_ACL |
                                  CHIMERA_VFS_ATTR_OWNER_SID |
                                  CHIMERA_VFS_ATTR_GROUP_SID);
    dirent->attr.va_set_mask &= ~(CHIMERA_VFS_ATTR_ACL |
                                  CHIMERA_VFS_ATTR_OWNER_SID |
                                  CHIMERA_VFS_ATTR_GROUP_SID);

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
    compound_store_aux(op, &op->dir_post_attr, dir_attr);

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

    /* Kept whatever the status, as the other change_info results are.  Only
     * the ctime survives here and a timespec has no set-mask of its own, so
     * the backend's mask is consulted before copying: a reading it did not
     * take leaves the zero the op started with, which is "not available" for
     * these two fields. */
    if (pre_attr && (pre_attr->va_set_mask & CHIMERA_VFS_ATTR_CTIME)) {
        op->pre_ctime = pre_attr->va_ctime;
    }
    if (post_attr && (post_attr->va_set_mask & CHIMERA_VFS_ATTR_CTIME)) {
        op->post_ctime = post_attr->va_ctime;
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

    compound_store_aux(op, &op->dir_pre_attr, dir_pre_attr);
    compound_store_aux(op, &op->dir_post_attr, dir_post_attr);
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
chimera_vfs_compound_open_stream_callback(
    enum chimera_vfs_error          error,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    chimera_vfs_compound_open_at_callback(error, handle, NULL, attr, NULL, NULL, private_data);
} /* chimera_vfs_compound_open_stream_callback */

static void
chimera_vfs_compound_list_streams_callback(
    enum chimera_vfs_error error,
    const void            *records,
    uint32_t               length,
    uint32_t               count,
    uint32_t               eof,
    uint64_t               cookie,
    void                  *private_data)
{
    chimera_vfs_compound_list_xattrs_callback(error, records, length, count, eof, cookie, private_data);
} /* chimera_vfs_compound_list_streams_callback */

static void
chimera_vfs_compound_remove_stream_callback(
    enum chimera_vfs_error          error,
    const struct chimera_vfs_attrs *pre,
    const struct chimera_vfs_attrs *post,
    void                           *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    compound_store_aux(op, &op->dir_pre_attr, pre);
    compound_store_aux(op, &op->dir_post_attr, post);
    chimera_vfs_compound_op_done(compound, error);
} /* chimera_vfs_compound_remove_stream_callback */

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

static void
chimera_vfs_compound_open_self_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    (void) attr;
    chimera_vfs_compound_open_fh_callback(error_code, handle, private_data);
} /* chimera_vfs_compound_open_self_callback */

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

    /* The directory pair is kept whatever the status: every NFSv3 create-class
     * reply -- CREATE3res, MKDIR3res, SYMLINK3res, MKNOD3res -- carries
     * dir_wcc on both arms, and a create that lost a race is exactly when a
     * client wants to know the directory moved. */
    if (dir_pre_attr) {
        compound_store_aux(op, &op->dir_pre_attr, dir_pre_attr);
    }

    if (dir_post_attr) {
        compound_store_aux(op, &op->dir_post_attr, dir_post_attr);
    }

    compound_store_aux(op, &op->dir_pre_attr, dir_pre_attr);
    compound_store_aux(op, &op->dir_post_attr, dir_post_attr);
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
        /* Nothing was handed over, so nothing is ours to keep -- unless what
         * came back is the caller's own destination, which read_into hands
         * back whatever the status and which is not ours to release. */
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

/*
 * A CREATE_UNLINKED finished.  The new object's handle becomes the current
 * open; the current FILE handle stays on the directory, because an unlinked
 * object has no name to make current -- see the op.
 */
static void chimera_vfs_compound_path_open_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data);

static void
chimera_vfs_compound_create_unlinked_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    (void) set_attr;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_op_done(compound, error_code);
        return;
    }

    if (attr) {
        chimera_vfs_compound_store_attr(op, attr);
    }
    op->created = 1;

    /* The directory open the cursor held goes, on the ordinary rules, and the
     * new object's handle takes the slot -- as a path OPEN's does, and on the
     * same single-owner terms: the OP owns it through out_handle, so the
     * cursor addresses it as taken.  The FILE cursor is left where it was. */
    chimera_vfs_compound_release_cursor(compound);

    compound->handle          = handle;
    compound->handle_flags    = op->open_flags;
    compound->handle_explicit = 1;
    compound->handle_taken    = 1;
    compound->handle_nameless = 1;

    op->out_handle          = handle;
    compound->handle_origin = compound->index + 1;

    chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
} /* chimera_vfs_compound_create_unlinked_callback */

/*
 * An OPEN_STREAM finished.  The stream becomes the current object in both
 * cursors -- its fh and its handle -- exactly as a path OPEN's result does;
 * the base's handle, if the cursor held it, is released by the fh move.
 */


/*
 * A GET_LAYOUT finished.  The backend's segments and devices are valid only
 * while this callback runs, so they are copied into arrays the op owns --
 * the whole reason the op exists rather than the caller reading the backend's
 * directly.  Allocated per execution: a retry frees the previous copy first,
 * since a backend may answer a different count the second time.
 */
static void
chimera_vfs_compound_get_layout_callback(
    enum chimera_vfs_error                   error_code,
    uint32_t                                 layout_class,
    uint32_t                                 num_segments,
    const struct chimera_vfs_layout_segment *segments,
    uint32_t                                 num_devices,
    const struct chimera_vfs_layout_device  *devices,
    void                                    *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    free(op->layout_segments);
    free(op->layout_devices);
    op->layout_segments     = NULL;
    op->layout_devices      = NULL;
    op->layout_num_segments = 0;
    op->layout_num_devices  = 0;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_op_done(compound, error_code);
        return;
    }

    op->layout_returned_class = layout_class;

    if (num_segments) {
        op->layout_segments = malloc(num_segments * sizeof(*segments));
        memcpy(op->layout_segments, segments,
               num_segments * sizeof(*segments));
        op->layout_num_segments = num_segments;
    }

    if (num_devices) {
        op->layout_devices = malloc(num_devices * sizeof(*devices));
        memcpy(op->layout_devices, devices, num_devices * sizeof(*devices));
        op->layout_num_devices = num_devices;
    }

    chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
} /* chimera_vfs_compound_get_layout_callback */

/*
 * The FIND bridges.  chimera_vfs_find's callbacks take a private pointer and
 * no op, so each of these looks the op up through the compound and re-issues
 * the call in the caller's shape.  The attributes go through as the walker
 * hands them over -- nothing is staged, so nothing needs copying, and an ACL
 * in them is the backend's, live for the duration of the callback exactly as
 * a streaming READDIR's append sees it.  The walk cannot be abandoned once it
 * is dispatched (see the typedefs), so a refused append sets find_stopped and
 * the two bridges below then drop entries and prune every directory until
 * the walker has drained.
 */
static int
chimera_vfs_compound_find_filter(
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *attr,
    void                           *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (op->find_stopped) {
        return 1;
    }
    if (op->find_stream_filter) {
        return op->find_stream_filter(compound, compound->index, path,
                                      pathlen, attr, op->find_private);
    }
    return op->find_filter ? op->find_filter(path, pathlen, attr, op->find_private) : 0;
} /* chimera_vfs_compound_find_filter */

static int
chimera_vfs_compound_find_entry(
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *attr,
    void                           *arg)
{
    struct chimera_vfs_compound    *compound = arg;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (op->find_stopped) {
        return 0;
    }

    int                             stopped = op->find_stream_append ?
        op->find_stream_append(compound, compound->index, path, pathlen,
                               attr, op->find_private) :
        op->find_append(path, pathlen, attr, op->find_private);
    if (stopped != 0) {
        op->find_stopped = 1;
    }

    return 0;
} /* chimera_vfs_compound_find_entry */

static void
chimera_vfs_compound_find_complete(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (error_code == CHIMERA_VFS_OK) {
        /* eof says whether the walk ran out of entries or the caller stopped
         * it -- the READDIR page rule. */
        op->eof = !op->find_stopped;
    }

    chimera_vfs_compound_op_done(compound, error_code);
} /* chimera_vfs_compound_find_complete */

/* Start the walk.  The walker opens each directory for itself, the root
 * included, so it takes the root's fh rather than the handle the prelude
 * opened; the fh and the mode are what it descends on. */
static void
chimera_vfs_compound_find_start(struct chimera_vfs_compound *compound)
{
    struct chimera_vfs_compound_op *op = compound->ops[compound->index];
    struct chimera_vfs_open_handle *target;

    target = op->in_handle ? op->in_handle :
        (op->handle_from >= 0 ? compound->ops[op->handle_from]->out_handle :
         compound->handle);

    chimera_vfs_find(compound->thread, compound->cred,
                     target->fh, (int) target->fh_len,
                     op->attr_mask | CHIMERA_VFS_ATTR_FH |
                     CHIMERA_VFS_ATTR_MODE,
                     chimera_vfs_compound_find_filter,
                     chimera_vfs_compound_find_entry,
                     chimera_vfs_compound_find_complete,
                     compound);
} /* chimera_vfs_compound_find_start */

/* Step one of a two-step FIND: the root's type, before the walk starts.
 * Opening the root as a directory is not the gate it is on a backend whose
 * PATH open checks the type -- memfs's does not, and its readdir's ENOTDIR
 * then vanishes inside the walker, which completes an unreadable directory
 * as an empty one.  So the type is asked for outright, the way READ and WRITE
 * ask before opening for data, and a FIND through anything but a directory
 * is ENOTDIR on every backend. */
static void
chimera_vfs_compound_find_type_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_compound *compound = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_op_done(compound, error_code);
        return;
    }

    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) && !S_ISDIR(attr->va_mode)) {
        chimera_vfs_compound_op_done(compound, CHIMERA_VFS_ENOTDIR);
        return;
    }

    chimera_vfs_compound_find_start(compound);
} /* chimera_vfs_compound_find_type_callback */

/*
 * A parking RECALL's recall has drained.  The `next` of the inert request the
 * RECALL arm of step built -- reached inside chimera_vfs_io_recall_single when
 * there was nothing to break, or later through the owning thread's resume
 * drain once the last holder acked, and on the submitting thread either way.
 *
 * What is reported is what recall_caching_fh reports: whether a live share
 * holder remains now that the recall is over.  The request is finished on
 * the terms every inert request is (chimera_vfs_complete, then freed), and
 * its fh is read before that because the report is keyed on it.
 */
static void
chimera_vfs_compound_recall_complete(struct chimera_vfs_request *request)
{
    struct chimera_vfs_compound    *compound = request->proto_private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    op->recall_still_open = chimera_vfs_fh_has_share_holder(request->thread,
                                                            request->fh,
                                                            request->fh_len)
        ? 1 : 0;

    chimera_vfs_complete(request);
    chimera_vfs_request_free(request->thread, request);

    compound->recall_answered = 1;

    chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
} /* chimera_vfs_compound_recall_complete */

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

    compound_store_aux(op, &op->pre_attr, pre_attr);
    compound_store_aux(op, &op->dir_pre_attr, pre_attr);
    compound_store_aux(op, &op->dir_post_attr, post_attr);
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

    compound_store_aux(op, &op->dir_pre_attr, pre_attr);
    compound_store_aux(op, &op->dir_post_attr, post_attr);
    if (post_attr) {
        chimera_vfs_compound_store_attr(op, post_attr);
    }
    if (set_attr) {
        op->applied_attr = *set_attr;
    }

    if (pre_attr) {
        compound_store_aux(op, &op->pre_attr, pre_attr);
    }

    if (post_attr) {
        chimera_vfs_compound_store_attr(op, post_attr);
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

    compound_store_aux(op, &op->dir_pre_attr, pre_attr);
    compound_store_aux(op, &op->dir_post_attr, post_attr);

    /* A REMOVE does not move the current object: it unlinks a name FROM it.
     * Kept whatever the status -- REMOVE3res and RMDIR3res carry dir_wcc on
     * both arms, and a failed unlink is when a client most wants to know
     * whether the directory moved under it. */
    if (pre_attr) {
        compound_store_aux(op, &op->dir_pre_attr, pre_attr);
    }

    /* A REMOVE does not move the current object: it unlinks a name FROM it. */

    chimera_vfs_compound_op_done(compound, error_code);
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
    compound_store_aux(op, &op->dir_pre_attr, pre);
    compound_store_aux(op, &op->dir_post_attr, post);
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

    compound_store_aux(op, &op->from_dir_pre_attr, fromdir_pre_attr);
    compound_store_aux(op, &op->from_dir_post_attr, fromdir_post_attr);
    compound_store_aux(op, &op->dir_pre_attr, todir_pre_attr);
    compound_store_aux(op, &op->dir_post_attr, todir_post_attr);

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

    compound_store_aux(op, &op->dir_pre_attr, r_dir_pre_attr);
    compound_store_aux(op, &op->dir_post_attr, r_dir_post_attr);
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

    /* Both readings kept whatever the status -- the WRITE rule.  The
     * pre-change reading rides in dir_pre_attr, where the adder's comment
     * says it does. */
    if (pre_attr) {
        compound_store_aux(op, &op->dir_pre_attr, pre_attr);
    }
    if (post_attr) {
        chimera_vfs_compound_store_attr(op, post_attr);
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

    /* Attributes kept whatever the status; the count is a success result. */
    if (pre_attr) {
        compound_store_aux(op, &op->dir_pre_attr, pre_attr);
    }
    if (post_attr) {
        chimera_vfs_compound_store_attr(op, post_attr);
    }

    if (error_code == CHIMERA_VFS_OK) {
        op->written = length;
        if (pre_attr) {
            compound_store_aux(op, &op->dir_pre_attr, pre_attr);
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

    /* Kept whatever the status -- the WRITE rule. */
    if (pre_attr) {
        compound_store_aux(op, &op->dir_pre_attr, pre_attr);
    }
    if (post_attr) {
        chimera_vfs_compound_store_attr(op, post_attr);
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

    /* All three kept whatever the status -- the WRITE rule.  The source's
     * post-change attributes go where a name op keeps the directory's, which
     * a range op has no use for otherwise. */
    if (src_post_attr) {
        compound_store_aux(op, &op->dir_post_attr, src_post_attr);
    }
    if (dst_pre_attr) {
        compound_store_aux(op, &op->dir_pre_attr, dst_pre_attr);
    }
    if (dst_post_attr) {
        chimera_vfs_compound_store_attr(op, dst_post_attr);
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

    /* Attributes kept whatever the status -- the WRITE rule; the count and
     * the stability achieved are success results. */
    if (pre_attr) {
        compound_store_aux(op, &op->dir_pre_attr, pre_attr);
    }
    if (post_attr) {
        chimera_vfs_compound_store_attr(op, post_attr);
    }

    if (error_code == CHIMERA_VFS_OK) {
        op->written   = count;
        op->committed = sync;
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
 * A CLOSE(CLOSE_DOC) is over: close the backend handle the release detached
 * from the cache, and report what the unlink did.
 *
 * The order is the whole reason this lives inside the op.  The unlink
 * addresses the object and must precede the backend close -- a backend that
 * has closed its handle has let go of what the unlink names -- and there is no
 * way to express "unlink, then close the handle the release took away" as two
 * ops, because the second addresses nothing the cursors hold.
 *
 * The op itself still succeeds.  A CLOSE ends the handle, which it did; what
 * the unlink did is `doc_status`, for the caller to map.
 */
static void
chimera_vfs_compound_close_doc_finish(
    struct chimera_vfs_compound *compound,
    enum chimera_vfs_error       doc_status)
{
    struct chimera_vfs_compound_op *op = compound->ops[compound->index];

    op->doc_status = doc_status;

    chimera_vfs_close_ref_dispatch(compound->thread,
                                   &compound->close_doc.close_ref, NULL, NULL);

    chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
} /* chimera_vfs_compound_close_doc_finish */

static void
chimera_vfs_compound_close_doc_remove_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct chimera_vfs_compound *compound = private_data;

    (void) pre_attr;
    (void) post_attr;

    chimera_vfs_release(compound->thread, compound->close_doc_parent);
    compound->close_doc_parent = NULL;

    chimera_vfs_compound_close_doc_finish(compound, error_code);
} /* chimera_vfs_compound_close_doc_remove_callback */

/*
 * The doomed object's parent is open; unlink the name the arming recorded.
 *
 * MATCHED against the doomed object's own file handle: by the time a
 * delete-on-close fires, the name may belong to something else that was
 * created after the last open of the original closed, and the caller asked for
 * ITS object to go.  A name that no longer resolves to it is left alone and
 * the unlink reports OK, which is the outcome the caller wanted either way.
 */
static void
chimera_vfs_compound_close_doc_parent_callback(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (error_code != CHIMERA_VFS_OK) {
        /* The parent is not reachable, so the name cannot be unlinked through
         * it.  The backend handle is still closed -- it was detached from the
         * cache and nothing else will -- and the caller is told why. */
        chimera_vfs_compound_close_doc_finish(compound, error_code);
        return;
    }

    compound->close_doc_parent = handle;

    chimera_vfs_remove_at_match_fh(
        compound->thread, &compound->close_doc.cred,
        handle,
        compound->close_doc.name, compound->close_doc.name_len,
        compound->close_doc_child_fh, (int) compound->close_doc_child_fh_len,
        0, 0,
        op->parent_lease_skip_valid ? op->parent_lease_skip : NULL,
        chimera_vfs_compound_close_doc_remove_callback,
        compound);
} /* chimera_vfs_compound_close_doc_parent_callback */

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
        /* FIND walks from the current object, which is a directory or the
         * op is wrong: opened as one exactly as READDIR opens it, so the
         * refusal is ENOTDIR here rather than however the walker's own
         * open of the root would report it. */
        case CHIMERA_VFS_COMPOUND_OP_FIND:
        case CHIMERA_VFS_COMPOUND_OP_CREATE:
        case CHIMERA_VFS_COMPOUND_OP_REMOVE:
        /* CREATE_UNLINKED creates IN the current object, which is a
         * directory or the op is wrong: opened as one, as a named OPEN opens
         * it, so a non-directory fails here with ENOTDIR. */
        case CHIMERA_VFS_COMPOUND_OP_CREATE_UNLINKED:
            return CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                   CHIMERA_VFS_OPEN_DIRECTORY;
        /* The stream ops act on the base through whatever handle refers to
         * it; a PATH one is what every per-op consumer opens for them, and
         * the cheapest on every type a stream can hang off. */
        case CHIMERA_VFS_COMPOUND_OP_OPEN_STREAM:
        case CHIMERA_VFS_COMPOUND_OP_LIST_STREAMS:
        case CHIMERA_VFS_COMPOUND_OP_REMOVE_STREAM:
            return CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH;
        case CHIMERA_VFS_COMPOUND_OP_GET_LAYOUT:
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
             * per-op path does, and for the same reason.
             *
             * READ names the capability it needs (READ_ONLY, which a lent
             * read-write handle carries alongside WRITE_ONLY -- the bits are
             * capabilities, see PUTHANDLE); WRITE names none, so the write
             * access a lent handle carries is the backend's to refuse rather
             * than this rule's.  Asking for WRITE_ONLY here would also be
             * asking the executor to OPEN for write when it opens for itself,
             * which is a different question from what a lent handle serves. */
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
        /* RECALL takes a file handle and spares whatever handle the op
         * happens to address; it opens nothing of its own. */
        case CHIMERA_VFS_COMPOUND_OP_RECALL:
            return 0;
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
        case CHIMERA_VFS_COMPOUND_OP_GETXATTR:
        case CHIMERA_VFS_COMPOUND_OP_SETXATTR:
        case CHIMERA_VFS_COMPOUND_OP_LISTXATTRS:
        case CHIMERA_VFS_COMPOUND_OP_REMOVEXATTR:
        /* A claim is arbitrated per file and uses only the fh, so it wants
         * the same: a data open of a FIFO would block a LOCKT on it (which
         * opens PATH|NOFOLLOW on the per-op path today).  A data handle
         * already in the cursor serves the want. */
        case CHIMERA_VFS_COMPOUND_OP_CLAIM_TEST:
        case CHIMERA_VFS_COMPOUND_OP_CLAIM:
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
 *
 * The converse is not true, and treating it as though it were is what made the
 * rule too strong.  A real descriptor does everything an O_PATH one does and
 * more: fstat, fgetxattr, fdopendir all work on it.  PATH in `want` is a
 * statement about what is CHEAPEST to open, not about what is REQUIRED -- so an
 * op that would have opened a path handle is served by a data handle already in
 * hand.  SMB2 is where this bites: it opens a directory for enumeration with
 * FILE_LIST_DIRECTORY, which is a data open, and then lends that handle to a
 * QUERY_DIRECTORY whose READDIR would have preferred O_PATH.  Refusing it left
 * the caller nothing it could legally do -- it may not substitute a different
 * handle for a lent one -- so every enumeration failed.
 *
 * CHIMERA_VFS_OPEN_INFERRED is not a capability either, and is normalized out
 * of both sides before the subset test.  It is PROVENANCE: it says the VFS
 * opened this handle on an op's behalf rather than at a caller's request, and
 * an explicitly opened handle does everything an inferred one does.  Every
 * `want` this executor computes carries the bit, because the executor's own
 * opens are inferred by definition; a caller lending its real flags -- a data
 * handle is READ_ONLY or WRITE_ONLY, an opendir handle is PATH|DIRECTORY --
 * never has it, and treating it as required refused every such handle for
 * COMMIT, ALLOCATE, SEEK and GETATTR.
 */
static int
chimera_vfs_compound_handle_serves(
    unsigned int have,
    unsigned int want)
{
    have &= ~CHIMERA_VFS_OPEN_INFERRED;
    want &= ~CHIMERA_VFS_OPEN_INFERRED;

    if (!(have & CHIMERA_VFS_OPEN_PATH)) {
        want &= ~CHIMERA_VFS_OPEN_PATH;
    }

    if (want & ~have) {
        return 0;
    }

    return ((have ^ want) & CHIMERA_VFS_OPEN_PATH) == 0;
} /* chimera_vfs_compound_handle_serves */

/*
 * The same question of a LENT handle, where the answer is final: a lent handle
 * that does not serve fails the op rather than being set aside for one the
 * sequence opens itself (see the PUTHANDLE arm of step).
 *
 * One op is excepted from the PATH-parity half of the rule.  COMMIT's want is
 * data-side because that is what the executor opens for it when it opens for
 * itself -- but a COMMIT is served by whatever handle the caller lends, path or
 * data.  fsyncdir(2) is exactly a commit through an O_PATH directory handle,
 * every backend accepts one, and FUSE's FSYNCDIR lends the OPENDIR handle it
 * already holds.  The general rule -- a PATH handle never serves a data want --
 * stands for everything else: an ALLOCATE or a WRITE through an O_PATH
 * descriptor is a real EBADF, not a parity technicality.
 */
static int
chimera_vfs_compound_lent_serves(
    const struct chimera_vfs_compound_op *op,
    unsigned int                          have,
    unsigned int                          want)
{
    if (op->type == CHIMERA_VFS_COMPOUND_OP_COMMIT) {
        return 1;
    }

    return chimera_vfs_compound_handle_serves(have, want);
} /* chimera_vfs_compound_lent_serves */

/*
 * ...and whether the ONLY thing standing between them is the DIRECTORY bit.
 *
 * That bit is provenance, not a capability: a caller whose dirfd came from
 * open(dir, O_RDONLY) holds an open directory it cannot describe as one, and
 * what a READDIR or a name op actually needs is that the handle ADDRESS a
 * directory.  So a lent handle failing on that bit alone is not refused -- the
 * object is asked, and it is the object's type that answers (see the lent-
 * handle rules in the header).  Everything else about `serves` still stands:
 * a PATH handle lent to a WRITE is still the EBADF it was.
 */
static int
chimera_vfs_compound_lent_wants_dir(
    const struct chimera_vfs_compound_op *op,
    unsigned int                          have,
    unsigned int                          want)
{
    if (!(want & CHIMERA_VFS_OPEN_DIRECTORY) ||
        (have & CHIMERA_VFS_OPEN_DIRECTORY)) {
        return 0;
    }

    return chimera_vfs_compound_lent_serves(op, have,
                                            want & ~CHIMERA_VFS_OPEN_DIRECTORY);
} /* chimera_vfs_compound_lent_wants_dir */

/* The answer: the lent handle addresses a directory, or it does not and the
 * op's real complaint is ENOTDIR rather than a flag mismatch. */
static void
chimera_vfs_compound_lent_dir_callback(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct chimera_vfs_compound *compound = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        chimera_vfs_compound_op_done(compound, error_code);
        return;
    }

    if ((attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
        !S_ISDIR(attr->va_mode)) {
        chimera_vfs_compound_op_done(compound, CHIMERA_VFS_ENOTDIR);
        return;
    }

    compound->lent_dirchecked = 1;

    chimera_vfs_compound_step(compound);
} /* chimera_vfs_compound_lent_dir_callback */

/*
 * A CLAIM's answer, and which thread it arrives on.
 *
 * chimera_vfs_claim_acquire answers inside the call when it can (GRANTED or
 * DENIED on the spot) and later when it cannot: a blocking lock parks on the
 * file's pending queue, and the pump that eventually grants it runs on
 * WHATEVER THREAD RELEASED THE BLOCKER -- another protocol's thread, the
 * close thread, a delegation thread.  The header promises the completion on
 * the submitting thread, and everything the rest of the sequence touches (the
 * thread's request pool, its open caches, the caller's reply buffers) is
 * thread-local, so a late answer cannot simply carry on from where it lands.
 *
 * The rule is the one FUSE's SETLKW uses: an answer that arrives WHILE THE
 * DISPATCH IS STILL ON THE STACK is finished inline, when the acquire returns;
 * any other answer is marshalled home, whatever thread it came on -- even the
 * submitting one, because a deferred grant runs inside some other consumer's
 * release call, under that consumer's locks, which is no place to run the rest
 * of a sequence and the caller's completion.  The two sides agree through
 * claim_phase: the dispatch sets DISPATCHING before asking, the callback tries
 * to move it to ANSWERED, and the dispatch tries to move it to PARKED once the
 * acquire has returned.  Whichever of the two CASes loses knows the other
 * side has the completion.
 *
 * The marshalling is the core's own: the same per-thread doorbell and
 * pending_io_resume list that carry a parked I/O request back to its owning
 * thread (chimera_vfs_io_resume_post, drained by
 * chimera_vfs_process_completion).  What rides it is a gate-scratch request
 * -- never dispatched, off the owning thread's free list -- whose `complete`
 * the drain calls for a request flagged notify_gate_resume.  It is allocated
 * on the owning thread before the acquire and freed there afterwards,
 * whichever way the answer came, which is the term gate-scratch requests
 * already impose.
 *
 * A CLAIM_TEST projected to a backend arbiter (TEST_BACKEND) is the same
 * handshake with a different asker: the backend's completion may run inline
 * or on a delegation thread, and either way it records and CASes exactly as
 * the claim core's callback does.  A coalition grant (grant_settle) always
 * answers inside the call and needs none of this.
 */
#define CHIMERA_VFS_COMPOUND_CLAIM_DISPATCHING 0
#define CHIMERA_VFS_COMPOUND_CLAIM_ANSWERED    1
#define CHIMERA_VFS_COMPOUND_CLAIM_PARKED      2

static void
chimera_vfs_compound_claim_resume(
    struct chimera_vfs_request *request);

static void
chimera_vfs_compound_claim_resume_alloc(struct chimera_vfs_compound *compound)
{
    struct chimera_vfs_request *request;
    void                       *scratch;

    scratch = chimera_vfs_gate_scratch_alloc(compound->thread);
    request = container_of(scratch, struct chimera_vfs_request, gate.data);

    request->complete           = chimera_vfs_compound_claim_resume;
    request->proto_private_data = compound;
    request->notify_gate_resume = 1;

    compound->claim_resume = request;
} /* chimera_vfs_compound_claim_resume_alloc */

static void
chimera_vfs_compound_claim_resume_free(struct chimera_vfs_compound *compound)
{
    struct chimera_vfs_request *request = compound->claim_resume;

    /* A synchronous path (a coalition grant, a probe that never projected)
     * allocated none. */
    if (!request) {
        return;
    }

    /* Cleared here rather than trusted to the drain, which only clears it on
     * the path that went through it: an inline answer never did, and a pooled
     * request still flagged would route the next parked I/O it is reused for
     * into the wrong arm of the drain. */
    request->notify_gate_resume = 0;
    request->proto_private_data = NULL;

    chimera_vfs_gate_scratch_free(compound->thread, request->gate.data);

    compound->claim_resume = NULL;
} /* chimera_vfs_compound_claim_resume_free */

/* The SMB coalition constructs: the ones whose standing claim is a core-
 * allocated grant and whose acquire is the coalesce / cap / settle loop.
 * Everything else -- ranges, shares, deny probes, delegations, FUSE grants --
 * inserts the caller's own claim struct. */
static inline int
chimera_vfs_compound_claim_is_coalition(const struct chimera_vfs_claim *claim)
{
    switch (claim->construct) {
        case CHIMERA_CONSTRUCT_RQLS:
        case CHIMERA_CONSTRUCT_OPLOCK_II:
        case CHIMERA_CONSTRUCT_OPLOCK_EX:
        case CHIMERA_CONSTRUCT_OPLOCK_BATCH:
        case CHIMERA_CONSTRUCT_DIR_LEASE:
            return 1;
        default:
            return 0;
    } /* switch */
} /* chimera_vfs_compound_claim_is_coalition */

/* Fire one of a CLAIM's trigger words against the file the op resolved, as
 * the claim's owner through the claim's handle -- see TRIGGERS on the CLAIM
 * op for why that actor expresses the KEY-circle self-exemption without a
 * separate argument.  0 is "no break". */
static void
chimera_vfs_compound_claim_fire(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_compound_op *op,
    uint8_t                         trigger,
    uint8_t                         retain)
{
    struct chimera_vfs_file_state *file = op->lock_file_state;
    struct chimera_claim_actor     actor;

    if (!trigger) {
        return;
    }

    actor.owner     = op->claim->owner;
    actor.op_handle = op->claim->op_handle;

    chimera_vfs_claim_invalidate(compound->thread->vfs->vfs_state,
                                 file->fh, file->fh_len, file->fh_hash,
                                 (enum chimera_claim_trigger) trigger,
                                 &actor, retain);
} /* chimera_vfs_compound_claim_fire */

/* The answer is recorded on the op; act on it.  Always on the owning
 * thread, whichever way the answer arrived.  `sync` says it arrived inside
 * the dispatch's own call, which is the only case the deny trigger fires
 * in: a ticket that queued and later answered DENIED never fires it. */
static void
chimera_vfs_compound_claim_finish(
    struct chimera_vfs_compound *compound,
    int                          sync)
{
    struct chimera_vfs_compound_op *op        = compound->ops[compound->index];
    struct chimera_vfs_state       *vfs_state = compound->thread->vfs->vfs_state;

    chimera_vfs_compound_claim_resume_free(compound);

    if (op->type == CHIMERA_VFS_COMPOUND_OP_CLAIM_TEST) {
        /* A probe inserts nothing, so it owns nothing afterwards -- and a
         * probe that says "denied" has ANSWERED: that is the whole of LOCKT
         * and F_GETLK, and the sequence goes on. */
        chimera_vfs_state_put(vfs_state, op->lock_file_state);
        op->lock_file_state = NULL;
        chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
        return;
    }

    if (op->claim_result == CHIMERA_CLAIM_GRANTED) {
        /* The claim is inserted and the file state goes to the caller with it.
         * op->lock_file_state already holds it; leaving it there is what makes
         * take_file_state work -- and what lets an abort find it.  The post
         * trigger is the phase-2 break a granted share fires before the cache
         * grant behind it is asked for. */
        chimera_vfs_compound_claim_fire(compound, op, op->claim_post_trigger,
                                        op->claim_post_retain);
        chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
        return;
    }

    if (sync) {
        chimera_vfs_compound_claim_fire(compound, op, op->claim_deny_trigger,
                                        op->claim_deny_retain);
    }

    /* Nothing was taken, so the file state is ours to put. */
    chimera_vfs_state_put(vfs_state, op->lock_file_state);
    op->lock_file_state     = NULL;
    op->claim_grant         = NULL;
    op->claim_member_seeded = 0;

    /* A refusal is the op's answer, not a malfunction -- but it still stops
     * the sequence, because everything behind a claim in a sequence was
     * written on the assumption the claim was held.  The caller reads
     * claim_result and conflict to say WHY in its own protocol's terms:
     * NFS4ERR_DENIED, an SMB2 LOCK_NOT_GRANTED, an EAGAIN from fcntl.  Unless
     * the caller said the claim was OPTIONAL -- an opportunistic cache grant,
     * for which NONE is an outcome, not a failure -- in which case the answer
     * is recorded and the sequence goes on. */
    if (op->claim_flags & CHIMERA_VFS_COMPOUND_CLAIM_OPTIONAL) {
        chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
        return;
    }

    chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EAGAIN);
} /* chimera_vfs_compound_claim_finish */

/* The drain's entry point for a marshalled answer: back on the owning
 * thread, with the request having done its one job. */
static void
chimera_vfs_compound_claim_resume(struct chimera_vfs_request *request)
{
    struct chimera_vfs_compound *compound = request->proto_private_data;

    chimera_vfs_compound_claim_finish(compound, 0);
} /* chimera_vfs_compound_claim_resume */

/* Publish a recorded answer: inside the asking call the dispatch finishes
 * the op when the call returns to it; after it, the sequence is parked and
 * the answer rides the doorbell home to chimera_vfs_compound_claim_resume.
 * Everything the answer wrote has to be written before this: the CAS is what
 * publishes it to whichever side finishes. */
static void
chimera_vfs_compound_claim_answered(struct chimera_vfs_compound *compound)
{
    uint8_t expected = CHIMERA_VFS_COMPOUND_CLAIM_DISPATCHING;

    if (atomic_compare_exchange_strong(&compound->claim_phase, &expected,
                                       CHIMERA_VFS_COMPOUND_CLAIM_ANSWERED)) {
        return;
    }

    chimera_vfs_io_resume_post(compound->claim_resume);
} /* chimera_vfs_compound_claim_answered */

/* The claim core's answer, on whatever thread it chose.  Only records and
 * decides who finishes; it touches nothing thread-local itself.
 *
 * `granted` is the caller's own claim struct handed back, so there is nothing
 * to copy: on GRANTED it is now inserted. */
static void
chimera_vfs_compound_claim_callback(
    enum chimera_vfs_claim_result            result,
    struct chimera_vfs_claim                *granted,
    const struct chimera_vfs_claim_conflict *conflict,
    void                                    *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    (void) granted;

    op->claim_result = result;

    if (conflict) {
        op->conflict = *conflict;
    }

    chimera_vfs_compound_claim_answered(compound);
} /* chimera_vfs_compound_claim_callback */

/* A backend RANGE arbiter's answer to a projected CLAIM_TEST.  The local
 * probe was already clear (that is the only time it is asked), so the only
 * thing it can add is a holder the local arbiter cannot see -- another
 * process's lock on a passthrough backend -- reported in the range-conflict
 * shape the POSIX F_GETLK caller renders today.  A backend that could not
 * answer at all (ENOTSUP from a module that does not arbitrate ranges after
 * all, an I/O error) leaves the local answer standing: the projection is a
 * second opinion, not a gate. */
static void
chimera_vfs_compound_claim_probe_callback(
    enum chimera_vfs_error                     status,
    uint8_t                                    granted,
    uint64_t                                   token,
    const struct chimera_claim_range_conflict *conflict,
    void                                      *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    (void) granted;
    (void) token;

    if (status == CHIMERA_VFS_OK && conflict &&
        conflict->type != CHIMERA_VFS_LOCK_UNLOCK) {
        op->claim_result = CHIMERA_CLAIM_DENIED;
        memset(&op->conflict, 0, sizeof(op->conflict));
        op->conflict.construct = CHIMERA_CONSTRUCT_LOCK_ADVISORY;
        op->conflict.used      = (conflict->type == CHIMERA_VFS_LOCK_WRITE)
            ? (CHIMERA_CLAIM_LR | CHIMERA_CLAIM_LW) : CHIMERA_CLAIM_LR;
        op->conflict.offset         = conflict->offset;
        op->conflict.length         = conflict->length;
        op->conflict.owner.owner_lo = conflict->pid;
    }

    chimera_vfs_compound_claim_answered(compound);
} /* chimera_vfs_compound_claim_probe_callback */

static void
compound_range_complete(
    const struct chimera_vfs_claim_batch_result *result,
    void                                        *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    op->range_result = *result;
    chimera_vfs_compound_op_done(compound, result->status);
} /* compound_range_complete */

static int
compound_search_keys_entry(
    const void *key,
    uint32_t    key_len,
    const void *value,
    uint32_t    value_len,
    void       *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];
    uint64_t                        bytes    = (uint64_t) key_len + value_len;

    if (op->kv_error != CHIMERA_VFS_OK || op->kv_more) {
        return 1;
    }
    if (key_len > 4096) {
        op->kv_error = CHIMERA_VFS_ERANGE; return 1;
    }
    if (op->kv_num_entries == op->kv_max_entries || bytes > op->kv_max_bytes - op->kv_result_bytes) {
        if (!op->kv_num_entries) {
            op->kv_error = CHIMERA_VFS_ERANGE; return 1;
        }
        op->kv_next_key = malloc(key_len ? key_len : 1);
        if (!op->kv_next_key) {
            op->kv_error = CHIMERA_VFS_ENOSPC; return 1;
        }
        if (key_len) {
            memcpy(op->kv_next_key, key, key_len);
        }
        op->kv_next_key_len = key_len;
        op->kv_more         = true;
        return 1;
    }
    uint8_t *copy = malloc(bytes ? (size_t) bytes : 1);
    if (!copy) {
        op->kv_error = CHIMERA_VFS_ENOSPC; return 1;
    }
    if (key_len) {
        memcpy(copy, key, key_len);
    }
    if (value_len) {
        memcpy(copy + key_len, value, value_len);
    }
    op->kv_entries[op->kv_num_entries++] = (struct chimera_vfs_compound_kv_entry) {
        .key = copy, .key_len = key_len, .value = copy + key_len, .value_len = value_len
    };
    op->kv_result_bytes += (uint32_t) bytes;
    return 0;
} /* compound_search_keys_entry */

static void
compound_search_keys_complete(
    enum chimera_vfs_error status,
    void                  *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (op->kv_error != CHIMERA_VFS_OK) {
        status = op->kv_error;
    }
    chimera_vfs_compound_op_done(compound, status);
} /* compound_search_keys_complete */

static bool
compound_range_is_canceled(void *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    return op->range_is_canceled &&
           op->range_is_canceled(compound, compound->index, op->range_wait_private);
} /* compound_range_is_canceled */

static void
compound_range_wait(void *private_data)
{
    struct chimera_vfs_compound    *compound = private_data;
    struct chimera_vfs_compound_op *op       = compound->ops[compound->index];

    if (op->range_on_wait) {
        op->range_on_wait(compound, compound->index, op->range_wait_private);
    }
} /* compound_range_wait */

/* Each endpoint keeps its own array alive through asynchronous backend I/O. */
static bool
compound_journal_view(
    struct chimera_vfs_compound          *compound,
    const struct chimera_vfs_open_handle *target,
    struct chimera_vfs_io_view           *view,
    const struct chimera_vfs_claim     ***storage)
{
    if (!compound->claim_journal || !target) {
        return true;
    }
    struct chimera_vfs_state      *state = compound->thread->vfs->vfs_state;
    struct chimera_vfs_file_state *file  = chimera_vfs_state_get(state, target->fh,
                                                                 target->fh_len, target->fh_hash, false);
    uint32_t                       count = file ? chimera_vfs_claim_journal_excluded(
        compound->claim_journal, file, NULL, 0) : 0;
    bool                           ok = true;
    if (count) {
        *storage = calloc((size_t) count + view->num_excluded, sizeof(**storage));
        if (!*storage) {
            ok = false;
        } else {
            for (uint32_t i = 0; i < view->num_excluded; i++) {
                (*storage)[i] = view->excluded[i];
            }
            chimera_vfs_claim_journal_excluded(compound->claim_journal, file,
                                               *storage + view->num_excluded, count);
            view->excluded      = *storage;
            view->num_excluded += count;
        }
    }
    if (file) {
        chimera_vfs_state_put(state, file);
    }
    return ok;
} /* compound_journal_view */

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

    /* A skipped op is run PAST, not run: nothing is dispatched, its status
     * stays CHIMERA_VFS_UNSET and its results are untouched, and it does not
     * count among the ops that ran.  Nothing is cleared on the way through
     * either -- the per-op scratch below was cleared when the sequence advanced
     * off the last op that actually ran.
     *
     * The two skips are ORed and neither clears the other: the caller's was
     * decided when the sequence was built and survives submission, the gate's
     * is decided on every execution. */
    while (compound->index < compound->num_ops &&
           (compound->ops[compound->index]->skip ||
            compound->ops[compound->index]->skip_build)) {
        compound->index++;
    }

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
        compound->fh_len       = compound->saved_fh_len = 0;
        compound->handle_flags = compound->saved_handle_flags = 0;
        compound->cred         = group->config.cred ? group->config.cred : compound->default_cred;
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
        op->applied_attr = op->set_attr;
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

    /* A skipped op produced no handle, and an op addressing one by
     * use_handle would be acting on NULL -- silently, on whatever the
     * underlying call does with it.  The gate that skipped it wrote a sequence
     * that does not hold together, so say so here rather than three frames
     * down in a backend. */
    chimera_vfs_abort_if(op->handle_from >= 0 &&
                         compound->ops[op->handle_from]->skip,
                         "compound op %u addresses the handle of op %d, which "
                         "a gate skipped", compound->index, op->handle_from);

    /* The other way that handle can be missing, and the one that is not a
     * caller bug: the op ran, and produced nothing.  A path OPEN on a backend
     * that resolved the name without handing back a reference is the case --
     * the adder has already refused every op type that CANNOT produce one, so
     * what is left here is an op that could have and did not.  That is an
     * answer, so it is reported rather than aborted. */
    if (op->handle_from >= 0 && !compound->ops[op->handle_from]->out_handle) {
        chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
        return;
    }

    /* The object this op acts on: the one the caller handed in, or else the
     * sequence's current object.  Ops that resolve a NAME use compound->handle
     * directly instead, because for them it is the directory to resolve in and
     * not the object being acted on. */
    range_src = op->src_handle ? op->src_handle : compound->saved_handle;
    range_dst = op->in_handle ? op->in_handle : compound->handle;

    target = op->in_handle ? op->in_handle :
        (op->handle_from >= 0 ? compound->ops[op->handle_from]->out_handle :
         compound->handle);

    /* op_open_flags cannot see the sequence, so tell it whether the two-step
     * I/O type check still has to happen.  It does not when the caller opened
     * or lent the handle: the caller established the type by opening it, and
     * asking for a PATH open first would reject the very handle it supplied.
     * The dispatch skips the check on the same condition. */
    op->io_typechecked_flag = compound->io_typechecked ||
        compound->handle_explicit;

    open_flags = chimera_vfs_compound_op_open_flags(op);

    if (open_flags) {
        if (compound->fh_len == 0) {
            /* No current object: the sequence addressed one before naming
             * one.  A caller builds these itself, so this is its bug. */
            chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
            return;
        }

        /* A LENT handle is the one to act on, full stop.  Opening a different
         * one because this does not serve would discard what the caller's own
         * open bound to it -- an SMB2 FileId's granted_access, an NFSv4
         * stateid's rights -- and hand the op a handle the caller never
         * authorized.  So a mismatch here is the caller's bug and is reported,
         * not papered over.
         *
         * A NAMELESS handle (CREATE_UNLINKED's) is held to the same rule for
         * every op but the name-resolvers: re-opening the current fh in its
         * place would open the directory, a different object, where the
         * caller wrote the unlinked one.  An op that wants a directory open
         * wants exactly that directory, and re-opens it as usual. */
        if (compound->handle &&
            (compound->handle_borrowed ||
             (compound->handle_nameless &&
              !(open_flags & CHIMERA_VFS_OPEN_DIRECTORY)))) {
            if (!chimera_vfs_compound_lent_serves(op, compound->handle_flags,
                                                  open_flags)) {
                /* Unless all it lacks is the DIRECTORY bit, which is
                 * provenance: ask the object what it is, once, and let its
                 * type answer.  A caller that DID lend the bit is taken at its
                 * word and pays nothing. */
                if (!chimera_vfs_compound_lent_wants_dir(op,
                                                         compound->handle_flags,
                                                         open_flags)) {
                    chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                    return;
                }

                if (!compound->lent_dirchecked) {
                    chimera_vfs_getattr(compound->thread, compound->cred,
                                        compound->handle,
                                        CHIMERA_VFS_ATTR_MODE,
                                        chimera_vfs_compound_lent_dir_callback,
                                        compound);
                    return;
                }
            }
        } else if (!compound->handle ||
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
                /* An explicit data OPEN must authorize and bind its rights
                 * just like a pathname open. An upgrade carrying retained
                 * grants has already authorized its newly requested rights;
                 * rechecking the union would revoke older grants on chmod.
                 * SMB supplies its own authorization through AUTH_ATTR. */
                if (!op->handle_state && !op->inherited_grant_handle &&
                    !op->inherited_grant_handle2 &&
                    !(op->open_flags & (CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH)) &&
                    compound->cred->flavor != CHIMERA_VFS_AUTH_ATTR) {
                    chimera_vfs_open(compound->thread, compound->cred,
                                     compound->fh, (int) compound->fh_len,
                                     NULL, 0, op->open_flags, NULL, 0,
                                     chimera_vfs_compound_open_self_callback,
                                     compound);
                    break;
                }
                chimera_vfs_open_fh_hs(compound->thread, compound->cred,
                                       compound->fh, (int) compound->fh_len,
                                       op->open_flags,
                                       op->handle_state,
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

            if (op->handle_state) {
                chimera_vfs_open_at_hs(compound->thread, compound->cred,
                                       target,
                                       op->path ? op->path : op->name, op->name_len,
                                       op->open_flags,
                                       &op->applied_attr,
                                       compound_object_mask(op, op->attr_mask) | CHIMERA_VFS_ATTR_FH,
                                       compound_pre_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                                       compound_post_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME),
                                       op->handle_state,
                                       chimera_vfs_compound_open_at_callback,
                                       compound);
            } else {
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
            }
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
                                                 compound_pre_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME)
                                                 ,
                                                 compound_post_mask(op, CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME
                                                                    ),
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
            if (op->setattr_after_write) {
                chimera_vfs_setattr_after_write(compound->thread, compound->cred,
                                                target, &op->applied_attr,
                                                compound_pre_mask(op, 0), compound_post_mask(op, op->attr_mask),
                                                chimera_vfs_compound_setattr_callback,
                                                compound);
                break;
            }
            /* With the caller's own handle the change is authorized by that
             * open's grant rather than re-checked against the object's mode --
             * the difference between ftruncate(2) and truncate(2), and the
             * whole reason a caller hands a handle in. */
            if (op->have_io_owner) {
                if (op->in_handle || (op->handle_from >= 0 &&
                                      compound->ops[op->handle_from]->type != CHIMERA_VFS_COMPOUND_OP_OPEN_PATH &&
                                      !(compound->ops[op->handle_from]->open_flags & CHIMERA_VFS_OPEN_PATH))) {
                    chimera_vfs_fsetattr_owned(compound->thread, compound->cred,
                                               target, &op->applied_attr,
                                               compound_pre_mask(op, 0), compound_post_mask(op, op->attr_mask),
                                               &op->io_owner, chimera_vfs_compound_setattr_callback, compound);
                } else {
                    chimera_vfs_setattr_owned(compound->thread, compound->cred,
                                              target, &op->applied_attr,
                                              compound_pre_mask(op, 0), compound_post_mask(op, op->attr_mask),
                                              &op->io_owner, chimera_vfs_compound_setattr_callback, compound);
                }
            } else if (op->in_handle || (op->handle_from >= 0 &&
                                         compound->ops[op->handle_from]->type != CHIMERA_VFS_COMPOUND_OP_OPEN_PATH &&
                                         !(compound->ops[op->handle_from]->open_flags & CHIMERA_VFS_OPEN_PATH))) {
                chimera_vfs_fsetattr(compound->thread, compound->cred,
                                     target,
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
                                                     target, op->name, op->name_len, op->arg_fh, op->arg_fh_len, op->
                                                     remove_flags,
                                                     compound_pre_mask(op, CHIMERA_VFS_ATTR_CHANGE |
                                                                       CHIMERA_VFS_ATTR_CTIME),
                                                     compound_post_mask(op, CHIMERA_VFS_ATTR_CHANGE |
                                                                        CHIMERA_VFS_ATTR_CTIME),
                                                     op->namespace_parent_lease_key_valid ? op->
                                                     namespace_parent_lease_key : NULL,
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
                                                       compound_pre_mask(op, CHIMERA_VFS_ATTR_CHANGE |
                                                                         CHIMERA_VFS_ATTR_CTIME),
                                                       compound_post_mask(op, CHIMERA_VFS_ATTR_CHANGE |
                                                                          CHIMERA_VFS_ATTR_CTIME),
                                                       op->namespace_parent_lease_key_valid ? op->
                                                       namespace_parent_lease_key : NULL,
                                                       op->have_io_owner ? op->io_owner.op_handle : op->op_exempt_handle
                                                       ,
                                                       op->have_io_owner ? &op->io_owner : NULL,
                                                       (op->remove_flags & CHIMERA_VFS_RENAME_MATCH_SOURCE_FH) ? op->
                                                       arg_fh : NULL,
                                                       (op->remove_flags & CHIMERA_VFS_RENAME_MATCH_SOURCE_FH) ? op->
                                                       arg_fh_len : 0,
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
                                            op->namespace_parent_lease_key_valid ? op->namespace_parent_lease_key : NULL
                                            ,
                                            op->have_io_owner ? op->io_owner.op_handle : op->op_exempt_handle,
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
        {
            struct chimera_vfs_open_handle *closing = compound->handle;
            struct chimera_vfs_state       *vfs_state;
            struct chimera_vfs_file_state  *file;
            uint64_t                        fh_hash;
            int                             fired;

            if (!closing) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }

            if (!(op->close_flags & CHIMERA_VFS_COMPOUND_CLOSE_DOC)) {
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
                            compound->saved_handle          = NULL;
                            compound->saved_handle_origin   = 0;
                            compound->saved_handle_flags    = 0;
                            compound->saved_handle_taken    = 0;
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

            }
            /* The legacy DOC release consumes the reference during execution;
             * this lifecycle cannot be replayed until DOC has a journal. */
            op->nonretryable = 1;
            if (compound->handle_origin) {
                compound->ops[compound->handle_origin - 1]->out_handle = NULL;
                if (compound->saved_handle_origin == compound->handle_origin) {
                    compound->saved_handle          = NULL;
                    compound->saved_handle_origin   = 0;
                    compound->saved_handle_flags    = 0;
                    compound->saved_handle_taken    = 0;
                    compound->saved_handle_borrowed = 0;
                }
            }
            compound->handle_origin = 0;

            /* The doomed object, read off the handle before the release, which
             * may free it: the unlink matches on this fh, and the delete-
             * pending check below asks about this file. */
            vfs_state = compound->thread->vfs->vfs_state;
            fh_hash   = closing->fh_hash;

            compound->close_doc_child_fh_len = closing->fh_len;
            memcpy(compound->close_doc_child_fh, closing->fh, closing->fh_len);
            memset(&compound->close_doc, 0, sizeof(compound->close_doc));

            fired = chimera_vfs_release_doc(compound->thread, closing,
                                            &compound->close_doc);

            compound->handle          = NULL;
            compound->handle_borrowed = 0;
            compound->handle_taken    = 0;
            compound->handle_flags    = 0;
            compound->handle_explicit = 0;
            compound->handle_nameless = 0;

            if (!fired) {
                /* Not the last reference, or the flag was never armed: an
                 * ordinary release, and nothing to report. */
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
                break;
            }

            /* A named stream still holds this base file open.  Removing it now
             * would pull the object out from under the stream, so the removal
             * waits: the file is marked delete-pending -- which is what makes a
             * name open of it, or of one of its streams, answer "this is going
             * away" -- and the stream's own last close performs it.  The
             * backend handle the release detached is still closed here. */
            file = chimera_vfs_state_get(vfs_state,
                                         compound->close_doc_child_fh,
                                         (uint8_t) compound->close_doc_child_fh_len,
                                         fh_hash, false);

            if (file) {
                if (chimera_vfs_state_stream_holders(file) > 0) {
                    chimera_vfs_state_set_delete_pending(file);
                    op->doc_base_deferred = 1;
                }
                chimera_vfs_state_put(vfs_state, file);
            }

            /* ...and a handle armed with no name to unlink (the parent was not
             * recorded) has nothing to do either. */
            if (op->doc_base_deferred || compound->close_doc.parent_fh_len == 0) {
                chimera_vfs_compound_close_doc_finish(compound, CHIMERA_VFS_OK);
                break;
            }

            op->doc_fired = 1;

            /* Under the credential the flag was ARMED with, not the one
             * closing: the client that asked for the deletion is the one whose
             * rights it is done on, and the last close may well be another's. */
            chimera_vfs_open_fh(compound->thread, &compound->close_doc.cred,
                                compound->close_doc.parent_fh,
                                compound->close_doc.parent_fh_len,
                                CHIMERA_VFS_OPEN_INFERRED |
                                CHIMERA_VFS_OPEN_PATH,
                                chimera_vfs_compound_close_doc_parent_callback,
                                compound);
            break;
        }

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
            compound->saved_handle_nameless = compound->handle_nameless;
            compound->saved_handle_origin   = compound->handle_origin;

            compound->handle          = NULL;
            compound->handle_flags    = 0;
            compound->handle_borrowed = 0;
            compound->handle_taken    = 0;
            compound->handle_nameless = 0;
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
            compound->handle_nameless = compound->saved_handle_nameless;
            compound->handle_origin   = compound->saved_handle_origin;

            compound->saved_handle          = NULL;
            compound->saved_handle_flags    = 0;
            compound->saved_handle_borrowed = 0;
            compound->saved_handle_taken    = 0;
            compound->saved_handle_nameless = 0;

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
            op->find_stopped = 0;
            op->eof          = 0;
            memcpy(op->fh, compound->fh, compound->fh_len);
            op->fh_len = compound->fh_len;
            chimera_vfs_getattr(compound->thread, compound->cred, target,
                                CHIMERA_VFS_ATTR_MODE,
                                chimera_vfs_compound_find_type_callback,
                                compound);
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
            /* No path-addressed open takes a handle-state record; opening
             * without the record a caller asked to persist would be a durable
             * handle with nothing behind it, so the op refuses instead.  See
             * the setter. */
            if (op->handle_state) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_ENOTSUP);
                break;
            }
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
                                                                        target->fh, target->fh_len, target->fh_hash,
                                                                        true);
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
                                                    op->range_timeout_ms, compound_range_wait,
                                                    compound_range_is_canceled,
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
            uint32_t                        from    = op->access_narrow_from;
            struct chimera_vfs_compound_op *reserve = from < compound->num_ops ? compound->ops[from] : NULL;
            enum chimera_vfs_error          error   = CHIMERA_VFS_EINVAL;
            if (reserve && reserve->type == CHIMERA_VFS_COMPOUND_OP_RESERVE_ACCESS &&
                reserve->completed && reserve->status == CHIMERA_VFS_OK &&
                reserve->claim_held && reserve->access_owner) {
                error = chimera_vfs_claim_access_journal_narrow(compound->access_journal,
                                                                reserve->access_owner, op->access_narrow_used, op->
                                                                access_narrow_denied);
            }
            chimera_vfs_compound_op_done(compound, error);
            break;
        }

        case CHIMERA_VFS_COMPOUND_OP_RETIRE_ACCESS:
            chimera_vfs_compound_op_done(compound,
                                         chimera_vfs_claim_access_journal_retire(compound->access_journal, op->
                                                                                 access_retire_owner));
            break;

        case CHIMERA_VFS_COMPOUND_OP_RETIRE_OPEN_CLAIMS: {
            uint32_t               range_checkpoint = chimera_vfs_claim_journal_retire_checkpoint(compound->
                                                                                                  claim_journal);
            uint32_t               access_checkpoint = chimera_vfs_claim_access_journal_checkpoint(compound->
                                                                                                   access_journal);
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
                                                                         op->claim_file, op->claim, &op->claim_conflict)
                ;
            }
            if (op->claim_result == CHIMERA_CLAIM_GRANTED) {
                const struct chimera_vfs_claim *const *original       = op->claim->admit_excluded;
                uint32_t                               original_count = op->claim->admit_num_excluded;
                uint32_t                               count          = compound->access_journal ?
                    chimera_vfs_claim_access_journal_excluded(
                    compound->access_journal, op->claim_file, NULL, 0) : 0;
                const struct chimera_vfs_claim       **excluded = NULL;
                if (count) {
                    excluded = calloc((size_t) count + original_count, sizeof(*excluded));
                    if (!excluded) {
                        chimera_vfs_compound_op_done(compound, CHIMERA_VFS_ENOSPC);
                        break;
                    }
                    for (uint32_t i = 0; i < original_count; i++) {
                        excluded[i] = original[i];
                    }
                    chimera_vfs_claim_access_journal_excluded(compound->access_journal,
                                                              op->claim_file, excluded + original_count, count);
                    op->claim->admit_excluded     = excluded;
                    op->claim->admit_num_excluded = original_count + count;
                }
                const void *original_cookie = op->claim->admission_cookie;
                op->claim->admission_cookie = compound->admission_cookie ? compound->admission_cookie : compound;
                op->claim_result            = chimera_vfs_claim_try_acquire(
                    compound->thread->vfs->vfs_state, op->claim_file, op->claim, &op->claim_conflict);
                op->claim->admission_cookie   = original_cookie;
                op->claim->admit_excluded     = original;
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
            if (!(chimera_vfs_module_capabilities(compound->thread, compound->fh,
                                                  compound->fh_len) & CHIMERA_VFS_CAP_CREATE_UNLINKED)) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_ENOTSUP);
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
                    /* mkdir -p is chimera_vfs_create's component walk, which
                     * accepts an existing component -- the leaf included --
                     * and answers with the same (status, attr) pair a
                     * single-level mkdir does, so the two share a completion
                     * and the object it made or found becomes current on the
                     * same terms. */
                    if (op->path_intermediates) {
                        chimera_vfs_create(compound->thread, compound->cred,
                                           compound->fh, (int) compound->fh_len,
                                           op->path, (int) op->path_len,
                                           &op->set_attr, op->attr_mask,
                                           chimera_vfs_compound_path_attr_callback,
                                           compound);
                        break;
                    }
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

        case CHIMERA_VFS_COMPOUND_OP_GET_LAYOUT:
            /* The per-op call answers ENOTSUP itself for a backend that
             * does not source layouts, so there is nothing to gate here. */
            chimera_vfs_get_layout(compound->thread, compound->cred,
                                   target,
                                   op->layout_offset, op->layout_length,
                                   op->layout_iomode, op->layout_class,
                                   op->layout_max_segments,
                                   chimera_vfs_compound_get_layout_callback,
                                   compound);
            break;

        case CHIMERA_VFS_COMPOUND_OP_RECALL:
        {
            struct chimera_vfs_request     *request;
            struct chimera_vfs_open_handle *spare;
            const uint8_t                  *fh;
            uint32_t                        fh_len;
            uint64_t                        fh_hash;

            /* The object: the op's own fh, else the handle the op addresses
             * (whose lease is spared), else the current file handle with
             * nothing spared -- see the adder. */
            if (op->recall_fh_len) {
                fh     = op->recall_fh;
                fh_len = op->recall_fh_len;
                spare  = NULL;
            } else if (target) {
                fh     = target->fh;
                fh_len = target->fh_len;
                spare  = target;
            } else if (compound->fh_len) {
                fh     = compound->fh;
                fh_len = compound->fh_len;
                spare  = NULL;
            } else {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                break;
            }

            /* Generic recalls publish breaks before finish acceptance. */
            op->nonretryable = 1;
            fh_hash          = chimera_vfs_hash(fh, (int) fh_len);

            if (op->recall_flags & CHIMERA_VFS_COMPOUND_RECALL_NOWAIT) {
                /* The full recall as a synchronous question: kicked, and
                 * answered with whether a holder still blocks. */
                op->recall_still_open = chimera_vfs_claim_break_caching(
                    compound->thread->vfs->vfs_state,
                    fh, (uint8_t) fh_len, fh_hash) ? 1 : 0;
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_OK);
                break;
            }

            /* The inert request recall_handle_lease and recall_caching_fh
             * build: never dispatched (the opcode is a placeholder for the
             * metrics), it exists to park in the claim core until the
             * single-step recall it starts has drained, and its `complete`
             * is where the sequence goes on.  io_handle is the spared
             * handle, or NULL to break every holder. */
            request = chimera_vfs_request_alloc_by_hash(compound->thread,
                                                        compound->cred,
                                                        fh, (int) fh_len,
                                                        fh_hash);

            if (CHIMERA_VFS_IS_ERR(request)) {
                chimera_vfs_compound_op_done(compound,
                                             CHIMERA_VFS_PTR_ERR(request));
                break;
            }

            request->opcode             = CHIMERA_VFS_OP_GETATTR;
            request->complete           = chimera_vfs_compound_recall_complete;
            request->io_handle          = spare;
            request->proto_private_data = compound;

            compound->recall_answered = 0;

            chimera_vfs_io_recall_single(request, fh, (uint8_t) fh_len,
                                         fh_hash, op->recall_retain,
                                         chimera_vfs_compound_recall_complete);

            /* Unlike a CLAIM's grant, a parked recall's continuation can
             * only ever reach this thread through its own resume drain
             * (chimera_vfs_claim_pump_io posts to the owning thread; the
             * drain runs it), so there is no race to arbitrate: the call
             * returned without answering iff the sequence is parked. */
            if (!compound->recall_answered) {
                /* PARKED.  Nothing behind this op runs until the last
                 * holder acks or is revoked.  The request is kept for the
                 * duration: it is what chimera_vfs_claim_recall_cancel
                 * takes to unlink the io-wait ticket and keep
                 * chimera_vfs_compound_recall_complete from ever running. */
                compound->recall_request = request;
                chimera_vfs_compound_parked(
                    compound, CHIMERA_VFS_COMPOUND_PARK_RECALL);
            }
            break;
        }

        case CHIMERA_VFS_COMPOUND_OP_CLAIM_TEST:
        case CHIMERA_VFS_COMPOUND_OP_CLAIM:
        {
            struct chimera_vfs_state *vfs_state =
                compound->thread->vfs->vfs_state;
            uint8_t                   expected;

            /* Claims are arbitrated per FILE, not per open, so the question
             * is asked of the object the current open refers to. */
            op->lock_file_state = chimera_vfs_state_get(vfs_state,
                                                        target->fh,
                                                        (uint8_t) target->fh_len,
                                                        target->fh_hash,
                                                        true);

            if (!op->lock_file_state) {
                chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EIO);
                break;
            }

            if (op->claim_flags & CHIMERA_VFS_COMPOUND_CLAIM_ACCESS_OWNER) {
                if (op->type != CHIMERA_VFS_COMPOUND_OP_CLAIM ||
                    op->claim->klass != CHIMERA_CLAIM_CLASS_ACCESS) {
                    chimera_vfs_state_put(vfs_state, op->lock_file_state);
                    op->lock_file_state = NULL;
                    chimera_vfs_compound_op_done(compound, CHIMERA_VFS_EINVAL);
                    break;
                }
                op->access_owner = chimera_vfs_claim_access_owner_alloc(op->lock_file_state, op->claim);
                if (!op->access_owner) {
                    chimera_vfs_state_put(vfs_state, op->lock_file_state);
                    op->lock_file_state = NULL;
                    chimera_vfs_compound_op_done(compound, CHIMERA_VFS_ENOSPC);
                    break;
                }
                op->claim                   = chimera_vfs_claim_access_owner_claim(op->access_owner);
                op->claim->op_handle        = target;
                op->claim->admission_cookie = compound->admission_cookie;
            }

            /* A cache claim built before its handle existed (a single-run
             * CREATE's grant template) anchors the HOLDER circle to the
             * handle the op runs against, so a metadata op through that
             * handle does not recall the grant against itself.  A caller
             * that stamped its own is left alone.  Only the CACHE class:
             * open handles are cached per (fh, access mode, cred) and
             * SHARED, so stamping a range claim or a share claim would fold
             * every lock-owner or open-owner using that handle into one
             * holder -- see OP_HANDLE on the CLAIM op. */
            if (op->type == CHIMERA_VFS_COMPOUND_OP_CLAIM &&
                op->claim->klass == CHIMERA_CLAIM_CLASS_CACHE &&
                !op->claim->op_handle) {
                op->claim->op_handle = target;
            }

            if (op->type == CHIMERA_VFS_COMPOUND_OP_CLAIM_TEST) {
                op->claim_result = chimera_vfs_claim_test(op->lock_file_state,
                                                          op->claim,
                                                          &op->conflict);

                /* Clear here, and asked to look further: a backend RANGE
                 * arbiter sees holders in other processes that the local
                 * arbiter cannot.  Without one registered the local answer
                 * is the answer, and nothing is dispatched.  The answer may
                 * come inside this call or later on another thread, on the
                 * same terms as an acquire's. */
                if (op->claim_result == CHIMERA_CLAIM_GRANTED &&
                    (op->claim_flags & CHIMERA_VFS_COMPOUND_CLAIM_TEST_BACKEND) &&
                    op->claim->klass == CHIMERA_CLAIM_CLASS_RANGE &&
                    chimera_vfs_claim_backend_range_capable(vfs_state)) {
                    chimera_vfs_compound_claim_resume_alloc(compound);
                    atomic_store(&compound->claim_phase,
                                 CHIMERA_VFS_COMPOUND_CLAIM_DISPATCHING);

                    chimera_vfs_claim_acquire_backend(
                        compound->thread,
                        target->fh, (uint8_t) target->fh_len, target->fh_hash,
                        CHIMERA_VFS_CLAIM_KLASS_RANGE, 0, 0,
                        (op->claim->used & CHIMERA_CLAIM_LW) ? 1 : 0,
                        CHIMERA_VFS_CLAIM_TEST, SEEK_SET,
                        op->claim->offset, op->claim->length,
                        &op->claim->owner, 0, NULL, NULL,
                        chimera_vfs_compound_claim_probe_callback, compound);

                    expected = CHIMERA_VFS_COMPOUND_CLAIM_DISPATCHING;

                    if (atomic_compare_exchange_strong(
                            &compound->claim_phase, &expected,
                            CHIMERA_VFS_COMPOUND_CLAIM_PARKED)) {
                        break;
                    }
                }

                chimera_vfs_compound_claim_finish(compound, 1);
                break;
            }

            /* Generic claims publish into the live arbiter. Native ACCESS
             * and RANGE operations use attempt journals for retry safety. */
            op->nonretryable = 1;
            /* The phase-1 break, before admission is asked: SMB's OPEN_H /
             * OPEN_H_FORCE handle break ahead of its share check. */
            chimera_vfs_compound_claim_fire(compound, op, op->claim_pre_trigger,
                                            op->claim_pre_retain);

            if (chimera_vfs_compound_claim_is_coalition(op->claim)) {
                /* A coalition grant: coalesce / cap / acquire / settle in the
                 * core, always answered inside the call.  The caller's claim
                 * is the template; the standing claim is the grant's. */
                bool seeded = false;

                op->claim_result = chimera_vfs_claim_grant_settle(
                    vfs_state, op->lock_file_state, op->claim,
                    op->claim_is_v2, op->claim_cap_strict != 0,
                    op->claim_member_seed, &seeded,
                    &op->claim_grant, &op->conflict);
                op->claim_member_seeded = seeded ? 1 : 0;

                chimera_vfs_compound_claim_finish(compound, 1);
                break;
            }

            /* The answer may come inside this call or later on another
             * thread -- see chimera_vfs_compound_claim_callback.  Everything
             * a late answer needs is set up before asking, so the callback
             * finds it whichever way it arrives. */
            chimera_vfs_compound_claim_resume_alloc(compound);
            atomic_store(&compound->claim_phase,
                         CHIMERA_VFS_COMPOUND_CLAIM_DISPATCHING);

            chimera_vfs_claim_acquire(
                compound->thread, vfs_state, op->lock_file_state,
                op->claim, op->ticket,
                !!(op->claim_flags & CHIMERA_VFS_COMPOUND_CLAIM_WAIT),
                !!(op->claim_flags & CHIMERA_VFS_COMPOUND_CLAIM_WAIT_HARD),
                chimera_vfs_compound_claim_callback,
                chimera_vfs_compound_claim_blocked,
                compound);

            expected = CHIMERA_VFS_COMPOUND_CLAIM_DISPATCHING;

            if (atomic_compare_exchange_strong(&compound->claim_phase, &expected,
                                               CHIMERA_VFS_COMPOUND_CLAIM_PARKED)) {
                /* Not answered yet.  The sequence waits here; the callback
                 * will bring the answer home through the doorbell. */
                break;
            }

            /* Answered inside the call: finish inline, as before. */
            chimera_vfs_compound_claim_finish(compound, 1);
            break;
        }

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
        if (op->niov && !op->dest_published) {
            evpl_iovecs_release(compound->thread->evpl, op->iov, op->niov);
        }
        chimera_vfs_compound_attr_release(&op->attr);
        chimera_vfs_compound_attr_release(&op->pre_attr);
        chimera_vfs_compound_attr_release(&op->dir_pre_attr);
        chimera_vfs_compound_attr_release(&op->dir_post_attr);
        chimera_vfs_compound_attr_release(&op->from_dir_pre_attr);
        chimera_vfs_compound_attr_release(&op->from_dir_post_attr);
        free(op->layout_segments);
        free(op->layout_devices);
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
        group->status  = CHIMERA_VFS_UNSET;
        group->last_op = group->config.first_op + group->config.num_ops - 1;
    }
    compound->group_index        = 0;
    compound->group_active       = 0;
    compound->cancel_defer_end   = 0;
    compound->cred               = compound->default_cred;
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
    compound->park_state         = CHIMERA_VFS_COMPOUND_PARK_NONE;
    compound->park_fired         = 0;
    compound->recall_answered    = 0;
    compound->lent_dirchecked    = 0;
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
            compound->build_error  = CHIMERA_VFS_EINVAL;
        }
    }

    for (uint32_t i = 0; i < compound->num_ops; i++) {
        uint32_t ready = compound->ops[i]->access_ready;
        if (!ready) {
            continue;
        }
        bool     valid = false;
        for (uint32_t g = 0; g < compound->num_groups; g++) {
            if (compound_access_ready_in_group(compound, g, i, ready - 1)) {
                valid = true;
                break;
            }
        }
        if (!valid) {
            compound->build_failed = 1;
            compound->build_error  = CHIMERA_VFS_EINVAL;
        }
    }

    /* Scope endpoints must belong to one registered static group. Runtime
     * dynamic insertion is unsupported inside a declared cleanup suffix. */
    for (uint32_t i = 0; i < compound->num_ops; i++) {
        uint32_t end = compound->ops[i]->cancel_scope_end;
        if (!end) {
            continue;
        }
        bool     valid = false;
        for (uint32_t g = 0; g < compound->num_groups; g++) {
            const struct chimera_vfs_compound_group_config *config = &compound->groups[g].config;
            if (i >= config->first_op && end <= config->first_op + config->num_ops) {
                valid = true;
                break;
            }
        }
        if (!valid) {
            compound->build_failed = 1;
            compound->build_error  = CHIMERA_VFS_EINVAL;
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

    /* The park notification is once per SUBMISSION, and nothing is parked
     * before the first op runs. */
    compound->park_fired     = 0;
    compound->park_state     = CHIMERA_VFS_COMPOUND_PARK_NONE;
    compound->recall_request = NULL;

    /* A gate's edits are re-applied on every execution, and a skip is one of
     * them: the last run's is cleared so the gate decides again, rather than
     * an op staying skipped because it was skipped once.  This is the READDIR
     * reset rule applied to the sequence itself.
     *
     * The CALLER's skip is not one of the gate's edits and is not cleared: it
     * is an argument of the sequence as built, like a READ's offset, and a
     * sequence submitted twice has to be the same sequence both times. */
    for (uint32_t i = 0; i < compound->num_ops; i++) {
        compound->ops[i]->skip = 0;
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

/*
 * Abandon a parked run -- see the contract on the declaration.
 *
 * The whole of the arbitration is the core call's return value, and the two
 * cancellable parks answer it on identical terms: true means we took the
 * park back and nothing will ever be handed to the op, false means the
 * answer is already in flight and owns the completion.  Nothing here waits,
 * looks at the other thread, or re-checks afterwards -- a cancel that tried
 * to arbitrate by hand is the bug this shape exists to make impossible.
 *
 * The order inside the taken case matters in one place.  The parked CLAIM's
 * file state is put and cleared BEFORE the finish, because
 * chimera_vfs_compound_abort_claims recognizes a claim to release by
 * (lock_file_state != NULL && claim_result == GRANTED) and CHIMERA_CLAIM_
 * GRANTED is 0 -- the value claim_result still carries on an op that never
 * got an answer.  Clearing the state is what says "this op inserted nothing",
 * and it is true: a cancelled acquire never ran its callback.  The abort
 * release then does its ordinary work on the CLAIMs BEFORE this one.
 */

/*
 * The cross-thread cancel, arriving home -- see the contract on the
 * declaration.
 *
 * Two things happen here and their order is the whole of the safety.  The
 * latch goes back to IDLE FIRST, because the cancel below may complete the
 * run inside this call and the caller may free the compound inside its
 * completion: a free that still saw POSTED would defer a recycle that nobody
 * is left to perform.  Then the cancel runs, and from that call on the
 * compound is not ours to touch -- it may already be back on the thread's
 * free list.
 *
 * The ORPHAN arm is the other side of that: the run finished and the caller
 * freed the compound while this was riding the doorbell, so there is nothing
 * to cancel and the recycle the free deferred is ours to finish.
 */
static void
chimera_vfs_compound_cancel_resume(struct chimera_vfs_request *request)
{
    struct chimera_vfs_compound *compound = request->proto_private_data;
    uint8_t                      state    =
        atomic_load(&compound->cancel_post_state);

    atomic_store(&compound->cancel_post_state,
                 CHIMERA_VFS_COMPOUND_CANCEL_IDLE);

    if (state == CHIMERA_VFS_COMPOUND_CANCEL_ORPHAN) {
        chimera_vfs_compound_recycle(compound);
        return;
    }

    chimera_vfs_compound_cancel(compound);
} /* chimera_vfs_compound_cancel_resume */

/*
 * Ask for a run to be cancelled from any thread -- see the contract on the
 * declaration.
 *
 * Nothing is decided here.  The CAS is the whole of the idempotence: exactly
 * one post per ride, and a second one while the first is still in flight is
 * dropped because the ride it would ask for is already booked.  Everything
 * that could answer -- whether the run is parked, whether the claim core
 * gives the park back, whether the grant got there first -- is answered on
 * the submitting thread by the ordinary chimera_vfs_compound_cancel the drain
 * calls, which is what keeps one arbitration rather than two.
 */
SYMBOL_EXPORT void
chimera_vfs_compound_cancel_post(struct chimera_vfs_compound *compound)
{
    uint8_t expected = CHIMERA_VFS_COMPOUND_CANCEL_IDLE;

    if (!atomic_compare_exchange_strong(&compound->cancel_post_state,
                                        &expected,
                                        CHIMERA_VFS_COMPOUND_CANCEL_POSTED)) {
        /* Already riding, or riding towards a compound the caller has freed.
         * Either way one cancel is on its way and a second changes nothing. */
        return;
    }

    /* Ours exclusively from the CAS until the drain takes it: the submitting
     * thread reads the vehicle only to recycle it, which it cannot do while
     * the latch says POSTED. */
    compound->cancel_post->notify_gate_resume = 1;

    chimera_vfs_io_resume_post(compound->cancel_post);
} /* chimera_vfs_compound_cancel_post */

/* ---------------------------------------------------------------------- */
/* Results                                                                */
/* ---------------------------------------------------------------------- */

SYMBOL_EXPORT uint32_t
chimera_vfs_compound_num_groups(const struct chimera_vfs_compound *compound)
{
    return compound->num_groups;
} /* chimera_vfs_compound_num_groups */

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

/*
 * The writable view, for a gate.  The index rule is exact here -- the executor
 * knows which op the gate is being consulted on -- so this refuses rather than
 * notices: NULL for an op that has already run, for one that does not exist,
 * and for a caller that is not a gate at all.  A gate is the only thing that
 * may edit a built sequence, because it is the only thing the executor stops
 * for between the ops.
 */
SYMBOL_EXPORT struct chimera_vfs_compound_op *
chimera_vfs_compound_op_edit(
    struct chimera_vfs_compound *compound,
    uint32_t                     index)
{
    if (!compound->gating || index <= compound->gate_index ||
        index >= compound->num_ops) {
        return NULL;
    }

    return compound->ops[index];
} /* chimera_vfs_compound_op_edit */

static int
chimera_vfs_compound_cancel_park(struct chimera_vfs_compound *compound)
{
    struct chimera_vfs_state       *vfs_state =
        compound->thread->vfs->vfs_state;
    struct chimera_vfs_compound_op *op;

    chimera_vfs_abort_if(compound->park_notifying,
                         "compound cancelled from inside its own park callback");

    switch (compound->park_state) {
        case CHIMERA_VFS_COMPOUND_PARK_CLAIM:
            op = compound->ops[compound->index];

            if (!chimera_vfs_claim_cancel(vfs_state, op->ticket)) {
                /* The grant owns the completion: it is running, or about to,
                 * on whatever thread released the conflict, and will come
                 * home through the doorbell.  Not parked any more either
                 * way, so a second cancel has nothing to take. */
                compound->park_state = CHIMERA_VFS_COMPOUND_PARK_NONE;
                return 0;
            }

            /* The acquire callback will never fire, so the request that was
             * to carry its answer home has no answer to carry. */
            chimera_vfs_compound_claim_resume_free(compound);

            chimera_vfs_state_put(vfs_state, op->lock_file_state);
            op->lock_file_state = NULL;
            break;

        case CHIMERA_VFS_COMPOUND_PARK_RECALL:
            if (!chimera_vfs_claim_recall_cancel(vfs_state,
                                                 compound->recall_request)) {
                compound->park_state     = CHIMERA_VFS_COMPOUND_PARK_NONE;
                compound->recall_request = NULL;
                return 0;
            }

            /* The request is the core's from here -- finished inside the
             * call, or by the drain that had already been handed it. */
            compound->recall_request = NULL;
            break;

        default:
            /* Never parked, already answered, or suspended on something that
             * is not ours to take back.  Legal, and nothing to do. */
            return 0;
    } /* switch */

    compound->park_state = CHIMERA_VFS_COMPOUND_PARK_NONE;
    compound->canceled   = 1;

    /* The op ran -- it got as far as parking -- and ECANCELED is its answer.
     * The gate is not consulted: a cancel is not an outcome a caller vetoes,
     * and the gate's contract is to answer from what it already has about an
     * op that produced a result. */
    compound->completed                    = compound->index + 1;
    compound->ops[compound->index]->status = CHIMERA_VFS_ECANCELED;

    chimera_vfs_compound_finish(compound, CHIMERA_VFS_ECANCELED);

    return 1;
} /* chimera_vfs_compound_cancel */

SYMBOL_EXPORT int
chimera_vfs_compound_add_find_stream(
    struct chimera_vfs_compound         *compound,
    uint64_t                             attr_mask,
    chimera_vfs_compound_find_filter_t   filter,
    chimera_vfs_compound_find_append_t   append,
    chimera_vfs_compound_readdir_reset_t reset,
    void                                *private_data)
{
    if (!filter || !append || !reset) {
        compound->build_failed = 1;
        return -1;
    }
    int                             index;
    struct chimera_vfs_compound_op *op = chimera_vfs_compound_next_op(
        compound, CHIMERA_VFS_COMPOUND_OP_FIND, &index);
    if (!op) {
        return -1;
    }
    op->attr_mask          = attr_mask;
    op->find_stream_filter = filter;
    op->find_stream_append = append;
    op->find_reset         = reset;
    op->find_private       = private_data;
    return index;
} /* chimera_vfs_compound_add_find_stream */
