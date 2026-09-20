// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Running the tail of an NFSv4 COMPOUND as one VFS compound.
 *
 * The compound dispatcher normally walks the argarray one op at a time, each op
 * driving its own VFS calls and re-entering the dispatcher when it finishes.
 * When everything still to do is expressible in the VFS compound vocabulary
 * (vfs/vfs_compound.h), this instead builds the whole sequence, hands it to the
 * VFS, and fills every result from the single callback.  Nothing else changes:
 * a remainder that is not fully expressible is refused here and dispatched
 * exactly as before.
 *
 * WHERE THE SEQUENCE ENDS.  Usually at the end of the COMPOUND.  A few ops end
 * it early (nfs4_vfs_op_ends_run): a SECINFO, a LOCK, and an OPEN that could
 * still earn a delegation -- the one part of an OPEN that can suspend the
 * request is the CB_NULL probe its grant attempt may park on, so an OPEN that
 * cannot reach that probe is carried like any other op and the run goes on
 * past it.  Whatever follows a run-ending op is dispatched op by op
 * afterwards.  Handing the dispatcher back a req->index short of the end is
 * not a special case -- that is what every per-op handler does when it
 * completes.
 *
 * WHY IT LIVES INSIDE THE DISPATCH LOOP.  The attempt is made just before each
 * op is dispatched, not once at compound entry.  A 4.1+ COMPOUND always opens
 * with SEQUENCE, which is not expressible; letting it dispatch normally and
 * re-trying at the next index is what makes 4.1/4.2 traffic reachable at all.
 *
 * WHAT IT REFUSES.  Every refusal below exists because the operation carries
 * NFSv4-specific handling the VFS knows nothing about -- the pseudo-root, the
 * synthetic named-attribute directory, junction shadowing at a "/" export's
 * root, the CB_GETATTR delegation combine, the wire handle's export id and
 * squash policy.  Refusing is always safe: the request is untouched and the
 * per-op path runs.
 *
 * INJECTED OPS.  Several NFSv4 operations do more VFS work than their
 * VFS-compound counterpart: PUTFH stats the handle it validates (the zero-link
 * staleness rule), READLINK stats the object before reading it (the symlink
 * type gate), COMMIT stats it before flushing (the regular-file gate), and
 * LOCKT stats it before asking the claim layer whether its range is free.
 * Each therefore encodes as two or more VFS ops, and the map below records
 * which VFS ops belong to which NFSv4 op so a failure lands on the right one.
 *
 * SLOTS.  Four NFSv4 operations encode to NO VFS op at all -- CLOSE, LOCKU,
 * OPEN_DOWNGRADE and DELEGRETURN, each of which resolves a stateid the server
 * already holds, advances a seqid and changes state without touching a
 * backend.  A run carries a place for one and applies it, in op order, as the
 * results are filled (nfs4_vfs_op_is_slot, nfs4_vfs_slot_apply).  The point of
 * them is what they no longer do: a COMPOUND ending "... ; CLOSE" used to stop
 * the run in front of the CLOSE, and now does not.
 *
 * WHAT IS NOT IDENTICAL.  Execution stops at the first VFS failure, but the
 * checks above are NFSv4-side and are applied when the results are filled -- by
 * which time the ops after them have already run.  With one exception the
 * encodable set is read-only, and the reply is truncated at the failing op just
 * as it would be, so the wire result is unchanged; what differs is that some
 * reads were performed that the per-op path would have skipped.
 *
 * A slot is the same difference twice over, and is fenced twice.  Its own
 * answer -- a 4.0 replay, a bad seqid, a stale stateid, an inexpressible
 * downgrade -- lands after every VFS op in the run has executed, so nothing
 * that MUTATES may follow one (the may_fail_late rule, which LOCKT used to be
 * the only user of).  And its EFFECT on NFSv4 state lands after the ops behind
 * it were authorized against that state, so nothing that carries a stateid or
 * reads live claim state may follow one either: a READ behind a CLOSE of the
 * stateid it names would otherwise succeed where the per-op path answers
 * NFS4ERR_BAD_STATEID.  That second fence is nfs4_vfs_op_reads_state.
 *
 * The 4.1 current-stateid lifecycle moves with them: it is applied op by op as
 * the results are filled (nfs4_vfs_current_stateid_step) rather than replayed
 * for the whole run before it is submitted, because a slot RESOLVES the
 * current stateid when it is applied and the ops that SET it do so then too.
 *
 * The exception is SETXATTR and REMOVEXATTR, which mutate.  A sequence that
 * fails partway leaves the earlier ones applied -- exactly as the per-op path
 * does, since it is the same calls in the same order -- but a mutation can now
 * also run *after* a check that will fail the reply.  A COMMIT whose type gate
 * says NFS4ERR_ISDIR, with a SETXATTR behind it, is the shape: the per-op path
 * would never reach the SETXATTR, this path already has.  So an NFSv4 op that
 * mutates is encoded only when nothing that could fail on an NFSv4-side check
 * precedes it in the same sequence.
 *
 * WHAT A STATEID STILL COSTS.  An OPEN inside a run produces a stateid, and
 * an op behind it that presents the "current stateid" would have to be
 * authorized against state that does not exist when the sequence is built.
 * The OBJECT is reachable -- chimera_vfs_compound_op_use_handle names the
 * handle an earlier op produced, which is how an OPEN's own truncate reaches
 * it -- but the OWNER is not: an operation under an open stateid is
 * attributed to that open's handle, and a sequence has no way to say "the
 * handle op N will produce".  Those ops are therefore still refused at
 * themselves and dispatched op by op, after the OPEN's fill has set the
 * current stateid.  See the refusals in the READ/WRITE and SETATTR arms of
 * the scan.
 *
 * The dispatcher's reply-buffer headroom test (NFS4ERR_RESOURCE) is one more
 * check that this path can only apply after the fact, and it gets the same
 * treatment from the other side: a run that mutates is built only when the
 * buffer is roomy enough that the test cannot fire on it at all -- see the
 * headroom rule in chimera_nfs4_compound_try_vfs.
 */

#include <stdlib.h>
#include <string.h>

#include "nfs4_procs.h"
#include "nfs4_status.h"
#include "nfs4_attr.h"
#include "nfs4_access.h"
#include "nfs4_named_attr.h"
#include "nfs4_session.h"
#include "nfs4_state.h"
#include "nfs4_op_matrix.h"
#include "server/server.h"
#include "vfs/sdk/vfs_xattr_name.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include <xxhash.h>
#include "vfs/vfs_claim.h"
#include "vfs/vfs_compound.h"

/*
 * The encoder opens the current object itself.
 *
 * A sequence's ops act on an OPEN handle, and something has to open it.  The
 * executor used to decide, from a table keyed on op type; now the encoder says
 * so, because the encoder is what knows why it is opening -- and the table is
 * what handed an O_PATH descriptor to fgetxattr and made a data open of a FIFO
 * block.
 *
 * `cur_flags` tracks what the sequence's current open handle was opened with as
 * the sequence is BUILT, so consecutive ops on one object share a single OPEN:
 * a LOOKUP and the GETATTR behind it, a READLINK and its type gate.  It is
 * cleared wherever the current filehandle moves, because an open of the old
 * object says nothing about the new one.
 *
 * The serve test mirrors chimera_vfs_compound_handle_serves exactly: a handle
 * serves an op that asks for no flag it lacks and that wants the same side of
 * CHIMERA_VFS_OPEN_PATH -- a path open and a data open are different handles
 * from different caches, never interchangeable.
 *
 * Returns the OPEN's index, 0 when the open already in hand serves and no op
 * was added, or -1 if the sequence is full.  Callers test only for -1; index 0
 * is the seed PUTFH's and can never be an OPEN's.
 */
static int
nfs4_vfs_open_for(
    struct chimera_vfs_compound *compound,
    unsigned int                *cur_flags,
    unsigned int                 want)
{
    if (*cur_flags &&
        !(want & ~*cur_flags) &&
        (((*cur_flags ^ want) & CHIMERA_VFS_OPEN_PATH) == 0)) {
        return 0;
    }

    *cur_flags = want;

    return chimera_vfs_compound_add_open_current(compound, want, 0);
} /* nfs4_vfs_open_for */

/*
 * What a handle the server already holds was really opened with, which is what
 * PUTHANDLE requires so the sequence can tell whether it serves the ops behind
 * it.
 *
 * The open cache keeps the access MODE rather than the flag word (it is what
 * the cache is keyed on), and that is the whole of it for the handles this
 * lends: an NFSv4 open state's handle is a data handle, opened for read and/or
 * write by chimera_nfs4_open, and a lock state's is a dup of one.
 */
static unsigned int
nfs4_vfs_handle_open_flags(const struct chimera_vfs_open_handle *handle)
{
    return (handle->access_mode == CHIMERA_VFS_ACCESS_MODE_RO) ?
           CHIMERA_VFS_OPEN_READ_ONLY : 0;
} /* nfs4_vfs_handle_open_flags */

/* A CLAIM_NULL OPEN names a file in the current directory; every other claim
 * re-opens an object the client already has, and needs no directory. */
static int
nfs4_vfs_open_is_named(const struct nfs_argop4 *argop)
{
    return argop->opopen.claim.claim == CLAIM_NULL;
} /* nfs4_vfs_open_is_named */

/* The three intents an NFSv4 op has for the object it addresses. */
#define NFS4_VFS_OPEN_DIR                                       \
        (CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |        \
         CHIMERA_VFS_OPEN_DIRECTORY)
#define NFS4_VFS_OPEN_META                                      \
        (CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH)
/* REGULAR_ONLY is what carries the type rule when the caller opens
 * explicitly: the open refuses a directory with EISDIR (NFS4ERR_ISDIR)
 * atomically, rather than the data operation failing afterwards with whatever
 * errno the backend gives a directory. */
#define NFS4_VFS_OPEN_DATA                                      \
        (CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_REGULAR_ONLY)

/* One NFSv4 op encodes to several VFS ops -- the operation, the open the
 * encoder puts in front of it, and for the ops with a type gate a stat and a
 * second open.  The NFSv4 op count is still bounded by the VFS compound's own
 * limit, because every NFSv4 op costs at least one VFS op; the running total
 * is what vfs_ops bounds, op by op. */
#define NFS4_VFS_COMPOUND_MAX_OPS  CHIMERA_VFS_COMPOUND_MAX_OPS

/*
 * The least reply-buffer space one READDIR entry can consume: the attribute
 * value buffer alone is 256 bytes (see chimera_nfs4_readdir_entry_fill), before
 * the entry struct, its name and its attrmask array.  Used to bound how many
 * entries a given maxcount could possibly admit.
 */
#define NFS4_VFS_READDIR_MIN_ENTRY 256

/* How many iovecs one READ's answer may arrive in; the per-op path reserves the
 * same number. */
#define NFS4_VFS_READ_MAX_IOV      256

/*
 * How many entries a READDIR's reply could possibly hold.
 *
 * Two things bound the page in the per-op path: maxcount, charged 16 bytes of
 * fixed READDIR4resok overhead before any entry, and the reply buffer's own
 * 8192-byte floor.  Whichever is smaller, divided by the least an entry can
 * cost, is the most entries that reply can carry -- so a VFS sequence asked for
 * that many will never be the thing that ends the page first.
 */
static uint32_t
nfs4_vfs_readdir_max_entries(
    uint32_t maxcount,
    uint64_t avail)
{
    uint64_t budget = maxcount - 16;
    uint64_t floor  = avail > 8192 ? avail - 8192 : 0;

    if (floor < budget) {
        budget = floor;
    }

    return (uint32_t) (budget / NFS4_VFS_READDIR_MIN_ENTRY);
} /* nfs4_vfs_readdir_max_entries */

struct nfs4_vfs_op {
    /* The status the gate decided, for the answers no errno encodes:
     * VERIFY/NVERIFY's NFS4ERR_SAME and NOT_SAME, and LOCKT's NFS4ERR_DENIED.
     * The gate fails the op with whatever errno is nearest and leaves the real
     * answer here; the completion reads it back in preference. */
    nfsstat4                        gate_status;

    /* READDIR, which marshals its page entry by entry as the sequence produces
     * it: the entry list being built, and the reply-buffer mark it started
     * from so a retried sequence can put the buffer back. */
    struct nfs_nfs4_readdir_cursor  readdir_cursor;
    uint32_t                        readdir_mark;
    int                             readdir_have_mark;

    /* A SLOT: an NFSv4 op the run carries a place for but which encodes to no
     * VFS op at all (CLOSE, LOCKU, OPEN_DOWNGRADE, DELEGRETURN).  Its vfs
     * range is empty -- vfs_lo = vfs_hi + 1 -- so the completion's "every VFS
     * op inside a filled NFSv4 op ran" walk simply finds nothing to check, and
     * its vfs_res is -1 because it has no result to read. */
    int                             is_slot;

    uint32_t                        res_index; /* index into the COMPOUND's arg/res arrays        */
    int                             vfs_lo; /* first VFS op belonging to this NFSv4 op         */
    int                             vfs_hi; /* last VFS op belonging to this NFSv4 op          */
    int                             vfs_aux; /* injected helper getattr, or -1                  */
    int                             vfs_res; /* the VFS op this NFSv4 op's result comes from    */
    /* OPEN: an UNCHECKED4 create asked for size 0, which truncates an object
     * that already existed.  Recorded here because the create attributes it is
     * read from are blanked by the executor once the name resolves to something
     * (CHIMERA_VFS_COMPOUND_OPEN_ATTRS_ON_CREATE_ONLY), which is the same
     * blanking the per-op path does and for the same reason. */
    int                             open_trunc_if_existed;
    /* OPEN: whether the open named a child (CLAIM_NULL) rather than re-opening
     * the current filehandle (CLAIM_FH).  The two differ in what the open
     * reports back -- an open-by-handle produces no attributes and no directory
     * change info -- and so in what may be passed on from it. */
    int                             open_by_name;
    /* READ, WRITE, SETATTR: the handle this op resolved from its stateid, and
     * holds a reference to for as long as the sequence runs.  Borrowed by the
     * VFS op; released here when the sequence is over, whatever the outcome. */
    struct chimera_vfs_open_handle *io_handle;
    /* LOCKT: the range probe the CLAIM_TEST op asks.  A probe is never
     * inserted, so the claim core keeps no pointer into it once the op has
     * answered -- but the executor reads it while the op runs, so it lives
     * here, on memory that outlives the sequence, rather than on the build's
     * stack (vfs_compound.h, add_claim_test). */
    struct chimera_vfs_claim        probe;
    /* OPEN: the SHARE reservation the run takes for it.  Unlike the probe
     * above this one IS inserted, so the claim core keeps pointers into it
     * and it has to outlive the run AND the claim -- which is why it is
     * heap-allocated here and adopted by the open state, rather than being a
     * field of a state that does not exist when the run is built. */
    struct nfs4_share_lease        *share;
    /* OPEN: the open_owner the reservation is taken for, resolved when the
     * sequence is built so the gate can read the client's existing opens
     * without resolving anything itself.  A reference of ours, dropped when
     * the sequence is over whatever the outcome. */
    struct nfs_open_owner          *open_owner;
    /* OPEN: the gate found an open this owner already holds on the object,
     * so this OPEN coalesces onto it and takes no reservation of its own. */
    int                             open_coalesce;
};

struct nfs4_vfs_compound_ctx {
    struct nfs_request      *req;
    uint32_t                 num_ops;
    struct nfs4_vfs_op       ops[NFS4_VFS_COMPOUND_MAX_OPS];

    /* The OPEN this sequence carries, if any -- recorded when the sequence is
     * built, because every way out of it has to go through the OPEN's own
     * completion, including the ways where the OPEN never ran. */
    /* A SECINFO that succeeded has taken the current filehandle away. */
    int                      fh_consumed;
    /* A 4.0 SECINFO leaves the current filehandle alone, but the VFS work it
     * runs is a LOOKUP, and a LOOKUP moves the sequence's current object to
     * what it resolved.  This is that LOOKUP's VFS op index, so the completion
     * can put back what the op before it left current.  0 when the sequence
     * carries no 4.0 SECINFO: index 0 is the seed PUTFH's and can never be a
     * LOOKUP's. */
    int                      secinfo_lookup;

    int                      open_present;
    uint32_t                 open_res_index;
    /* The OPEN's own VFS ops, beyond the open itself: the CLAIM that takes
     * its share reservation, and the SETATTR that applies an UNCHECKED4
     * size-0 create's truncate to a file that already existed.  -1 when the
     * run carries neither; 0 is a real VFS op index (the seed PUTFH's) and
     * can never be either of these. */
    int                      open_claim_op;
    int                      open_setattr_op;
    /* Whether the OPEN is the last op the run carries.  It is, and only is,
     * when the OPEN could still earn a delegation -- see
     * nfs4_vfs_open_may_delegate. */
    int                      open_ends_run;

    /* The LOCK this sequence carries, if any.  Like the OPEN, every way out
     * of it goes through its own completion -- the RFC 7530 §9.1.7 seqid
     * wrapper -- including the ways where the CLAIM never ran.
     *
     * lock_fh_op is the VFS op immediately in front of the LOCK's own, whose
     * recorded current object is what the COMPOUND is left holding: the LOCK
     * addresses the handle its stateid names, which chimera lends the run with
     * a PUTHANDLE, and that is not where the COMPOUND's current filehandle
     * goes.  0 when the sequence carries no LOCK: index 0 is the seed PUTFH's
     * and can never be a LOCK's. */
    int                      lock_present;
    int                      lock_filled;
    uint32_t                 lock_res_index;
    int                      lock_fh_op;
    int                      lock_claim_op;

    /* An OPEN that filled successfully still owes the part of itself that can
     * suspend -- the delegation grant, the deferred truncate -- and that part
     * runs after the sequence has been freed, so what it needs is copied out
     * here rather than left pointing into the compound. */
    int                      open_filled;
    int                      open_has_attr;
    struct chimera_vfs_attrs open_attr;
};

static int
nfs4_vfs_op_encodable(uint32_t argop)
{
    switch (argop) {
        case OP_PUTFH:
        case OP_LOOKUP:
        case OP_LOOKUPP:
        case OP_GETATTR:
        case OP_ACCESS:
        case OP_GETFH:
        case OP_READLINK:
        case OP_SAVEFH:
        case OP_RESTOREFH:
        case OP_COMMIT:
        case OP_READDIR:
        case OP_GETXATTR:
        case OP_SETXATTR:
        case OP_LISTXATTRS:
        case OP_REMOVEXATTR:
        case OP_OPEN:
        case OP_CREATE:
        case OP_REMOVE:
        case OP_RENAME:
        case OP_LINK:
        case OP_SETATTR:
        case OP_READ:
        case OP_WRITE:
        case OP_LOCKT:
        case OP_LOCK:
        case OP_SECINFO:
        case OP_ALLOCATE:
        case OP_DEALLOCATE:
        case OP_SEEK:
        case OP_WRITE_SAME:
        case OP_VERIFY:
        case OP_NVERIFY:
        /* The zero-VFS-op slots -- see nfs4_vfs_op_is_slot. */
        case OP_CLOSE:
        case OP_LOCKU:
        case OP_OPEN_DOWNGRADE:
        case OP_DELEGRETURN:
            return 1;
        default:
            return 0;
    } /* switch */
} /* nfs4_vfs_op_encodable */

/*
 * Does this op drive no VFS call at all?
 *
 * Four do.  Each resolves a stateid the server already holds, advances a
 * seqid and changes state, and touches no backend -- so a run can carry the
 * operation without adding anything to the VFS sequence and apply it, in op
 * order, when the sequence's results are filled.  What that buys is that a
 * COMPOUND ending in "... ; CLOSE" no longer has to stop the run in front of
 * the CLOSE: the ops before it stay one sequence.
 *
 * The cost is the same one every NFSv4-side check in this file pays: a slot's
 * decisions are made after every VFS op in the run has already executed.  So
 * a slot is treated exactly as LOCKT used to be -- nothing that mutates may
 * follow one (may_fail_late), and nothing that reads or is authorized against
 * NFSv4 state may follow one either, because the slot's own effect on that
 * state lands later than the op that read it.  See nfs4_vfs_op_reads_state.
 */
static int
nfs4_vfs_op_is_slot(uint32_t argop)
{
    switch (argop) {
        case OP_CLOSE:
        case OP_LOCKU:
        case OP_OPEN_DOWNGRADE:
        case OP_DELEGRETURN:
            return 1;
        default:
            return 0;
    } /* switch */
} /* nfs4_vfs_op_is_slot */

/*
 * Does this op's VFS work depend on NFSv4 state a slot behind it would change?
 *
 * Every one of these either carries a stateid the encoder resolves when the
 * sequence is BUILT (the I/O ops, SETATTR and the v4.2 sparse ops, through
 * nfs4_vfs_io_authorize / nfs4_vfs_setattr_authorize) or reads live claim
 * state while the sequence RUNS (LOCKT's probe).  Both happen before a slot
 * in the same run applies, so a CLOSE, LOCKU, OPEN_DOWNGRADE or DELEGRETURN
 * in front of one of these would be honoured after it rather than before it:
 * a READ behind a CLOSE of the very stateid it names would succeed where the
 * op-at-a-time path answers NFS4ERR_BAD_STATEID.  So the run ends at a slot
 * whenever one of these follows, and the per-op path takes it from there.
 */
static int
nfs4_vfs_op_reads_state(uint32_t argop)
{
    switch (argop) {
        case OP_READ:
        case OP_WRITE:
        case OP_SETATTR:
        case OP_ALLOCATE:
        case OP_DEALLOCATE:
        case OP_SEEK:
        case OP_WRITE_SAME:
        case OP_LOCKT:
        case OP_LOCK:
        /* An OPEN reads live claim state while the sequence runs, for the
         * same reason LOCKT does: its share reservation is a CLAIM op, and
         * the arbiter answers it in op order inside the run.  A CLOSE in
         * front of it applies its own effect -- destroying the state and
         * releasing the reservation it held -- only when the results are
         * filled, so an OPEN behind a CLOSE of the very open that denies it
         * would be told SHARE_DENIED where the op-at-a-time path, which
         * closes first, succeeds. */
        case OP_OPEN:
            return 1;
        default:
            return 0;
    } /* switch */
} /* nfs4_vfs_op_reads_state */

/*
 * Does this op touch the reply buffer at a moment the two paths do not share?
 *
 * Results are marshalled in op order at the end, so the buffer fills the same
 * way it would have op by op -- including READDIR's, whose page is bounded by
 * the buffer's floor at exactly the point the per-op path would have met it.
 * The four xattr ops are the exception: GETXATTR, SETXATTR and REMOVEXATTR
 * stage their "user."-qualified name when the sequence is *built*, and GETXATTR
 * and LISTXATTRS decide how large an answer to ask for from the headroom at
 * that same moment -- before any result has been marshalled.  Each therefore
 * sees a different amount of free buffer than the op-at-a-time path would have
 * left it, and shifts the buffer under everything that follows.  The headroom
 * test in chimera_nfs4_compound_try_vfs is what stops either from changing an
 * answer.
 */
static int
nfs4_vfs_op_stages_early(uint32_t argop)
{
    switch (argop) {
        case OP_GETXATTR:
        case OP_SETXATTR:
        case OP_LISTXATTRS:
        case OP_REMOVEXATTR:
            return 1;
        default:
            return 0;
    } /* switch */
} /* nfs4_vfs_op_stages_early */

/*
 * Does this op change the filesystem?
 *
 * What the answer decides is when the reply-buffer headroom test may run.  The
 * per-op dispatcher refuses an op with NFS4ERR_RESOURCE BEFORE running it when
 * the reply buffer is within 8192 bytes of full; this path can apply the same
 * test only as each result is filled, which is after every op in the run has
 * executed.  For an op that only reads the difference is invisible -- the reply
 * is truncated at the same op either way -- but for one of these it would
 * report RESOURCE for a change that has already been applied, and an OPEN that
 * created would leave the file behind.  See chimera_nfs4_compound_try_vfs for
 * the rule this feeds.
 *
 * An OPEN counts whether or not it creates, and for the reason a LOCK does:
 * its share reservation is taken by a CLAIM op of the run and is the caller's
 * from the completion callback on, so a headroom refusal at the OPEN's own
 * fill -- before install_state has handed the reservation to an open state --
 * would leave it inserted with nobody owning it.  (Before the reservation was
 * an op of the run, an OPEN that only opened undid itself completely: the
 * handle went back with the sequence and no state was installed.)
 */
static int
nfs4_vfs_op_mutates(const struct nfs_argop4 *argop)
{
    switch (argop->argop) {
        case OP_OPEN:
        case OP_CREATE:
        case OP_REMOVE:
        case OP_RENAME:
        case OP_LINK:
        case OP_SETATTR:
        case OP_WRITE:
        case OP_SETXATTR:
        case OP_REMOVEXATTR:
        case OP_ALLOCATE:
        case OP_DEALLOCATE:
        case OP_WRITE_SAME:
            return 1;
        /* A LOCK changes nothing in the filesystem, but its CLAIM inserts a
         * claim that is the caller's from the completion callback on, and a
         * headroom refusal at the fill would leave it inserted with nobody
         * owning it -- the executor does not release a claim behind a
         * sequence that finished OK.  So it takes the mutating op's rule:
         * carried only when the whole run's worst-case reply fits, which is
         * what makes the late test unable to fire. */
        case OP_LOCK:
            return 1;
        default:
            return 0;
    } /* switch */
} /* nfs4_vfs_op_mutates */

/*
 * Could this OPEN still earn a delegation once the run is over?
 *
 * The question is not whether one WILL be granted -- that is settled by the
 * CB_NULL probe, and the probe stays where it is (see
 * chimera_nfs4_open_grant_delegation).  It is whether the grant attempt can
 * get as far as the probe at all, which is decidable here because every arm
 * of the grant that declines BEFORE the probe reads only the configuration,
 * the client, and the operation's own arguments.
 *
 * An OPEN that answers no here cannot park and cannot grant, so the whole of
 * what it still owes after its result is filled is memory, and the run can
 * carry ops behind it.  One that answers yes is made the last op of its run:
 * the probe can DEFER, which parks the OPEN, and a fill loop with results
 * still to fill behind it cannot be suspended.
 */
static int
nfs4_vfs_open_may_delegate(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    const struct nfs_argop4          *argop)
{
    const struct OPEN4args *args = &argop->opopen;
    uint32_t                want;

    if (!chimera_server_config_get_nfs4_delegations(thread->shared->config)) {
        return 0;
    }

    if (!req->session || !req->session->client_unified) {
        return 0;
    }

    if (args->claim.claim != CLAIM_NULL && args->claim.claim != CLAIM_FH) {
        return 0;
    }

    /* RFC 8881 §18.16: an explicit "no delegation wanted" (or a bare cancel)
     * declines ahead of the probe. */
    want = args->share_access & OPEN4_SHARE_ACCESS_WANT_DELEG_MASK;

    if (want == OPEN4_SHARE_ACCESS_WANT_NO_DELEG ||
        want == OPEN4_SHARE_ACCESS_WANT_CANCEL) {
        return 0;
    }

    if ((args->share_access & (OPEN4_SHARE_ACCESS_READ |
                               OPEN4_SHARE_ACCESS_WRITE)) == 0) {
        return 0;
    }

    return 1;
} /* nfs4_vfs_open_may_delegate */

/*
 * Does this op end the encodable run, whatever follows it?
 *
 * An OPEN used to, always.  Everything it still owed once the object was open
 * -- installing the open state, taking the share reservation, offering a
 * delegation, an UNCHECKED4 truncate -- happened when its result was filled,
 * and two of those could suspend: the delegation grant parks on an in-flight
 * CB_NULL probe, and the truncate was another VFS call.
 *
 * The truncate is an op of the run now (a SETATTR through the handle the OPEN
 * produced, which is also what puts it in front of a delegation grant rather
 * than behind one), and the share reservation is a CLAIM op.  What is left is
 * the probe, and only an OPEN that could reach it -- nfs4_vfs_open_may_
 * delegate -- still ends the run.  With delegations off, which is the common
 * configuration, no OPEN does, and "PUTFH; OPEN; GETFH; GETATTR" is one
 * sequence.
 */
static int
nfs4_vfs_op_ends_run(uint32_t argop)
{
    /* SECINFO joins OPEN, for a related reason: on 4.1 it CONSUMES the current
     * filehandle (RFC 8881 §18.29.3), so every op behind it must fail
     * NFS4ERR_NOFILEHANDLE -- and a sequence whose current object has been
     * taken away has nothing left to address.  On 4.0 the handle survives
     * (RFC 7530 §16.31.3), so ending the run there is merely conservative:
     * what follows is dispatched op by op against a filehandle that is still
     * good. */
    /* LOCK joins them, for the abort release.  A run that finishes with any
     * non-OK status releases every claim a CLAIM in it inserted, before the
     * completion callback -- which is right for a claim nobody has been told
     * about, and wrong for one the reply has already committed.  A LOCK's
     * result IS that commitment: "LOCK: OK, here is your stateid" followed by
     * a GETATTR that failed EIO would leave the client believing it holds a
     * byte range the abort released.  The OPEN's share claim takes the other
     * way out and re-acquires, because an OPEN is routinely followed by a
     * GETFH and a re-arbitration that fails maps onto the OPEN's own result,
     * which is the one still being written; a byte-range re-acquire could be
     * DENIED by a holder that arrived in the window, and the LOCK's result is
     * already written by then.  Under one rule: a claim taken for an op whose
     * result is already committed cannot be re-arbitrated, and a claim taken
     * for the op currently being filled can.
     *
     * It costs nothing.  Linux and pynfs send [SEQUENCE;] PUTFH; LOCK with
     * nothing behind it, and the model's LOCK steps are single-op. */
    return argop == OP_SECINFO || argop == OP_LOCK;
} /* nfs4_vfs_op_ends_run */

/*
 * Is this an xattr name the sequence can carry?
 *
 * The per-op path stages the "user."-qualified name and reports NFS4ERR_INVAL
 * for an empty key and NFS4ERR_NAMETOOLONG for one that will not fit -- both
 * decided before any VFS call, so they belong to the per-op path.  Testing the
 * length here rather than staging the name and refusing afterwards matters: a
 * staged name cannot be given back to the reply buffer, and a refusal that had
 * consumed some of it would leave the per-op path with less to work with.
 */
static int
nfs4_vfs_xattr_name_ok(uint32_t wire_len)
{
    return wire_len > 0 &&
           wire_len <= CHIMERA_VFS_XATTR_NAME_MAX -
           CHIMERA_VFS_XATTR_USER_PREFIX_LEN;
} /* nfs4_vfs_xattr_name_ok */

/*
 * An upper bound on the reply-buffer space one op can consume, staged or
 * filled.  Deliberately generous: it exists only to decide whether the buffer
 * is roomy enough that no size decision anywhere in the sequence can be
 * affected by how the two paths interleave their allocations -- and, for a run
 * that mutates, that no fill can reach the dispatcher's RESOURCE floor (see
 * nfs4_vfs_op_mutates).  Every allocation the build, the run or a fill makes
 * from the reply buffer has to be covered here, or the second use is unsound.
 */
static uint64_t
nfs4_vfs_op_reply_bound(const struct nfs_argop4 *argop)
{
    /* Slack for the small fixed allocations around each result -- attrmask
     * arrays, opaque headers, and xdr_dbuf_alloc_space's own rounding. */
    const uint64_t slack = 512;

    switch (argop->argop) {
        case OP_GETATTR:
            /* An ACL request is refused, so the attribute buffer is the fixed
             * 4096 chimera_nfs4_getattr_fill reserves. */
            return 4096 + slack;
        case OP_READLINK:
            return 4096 + slack;
        case OP_GETFH:
            return CHIMERA_NFS_FH_MAX + slack;
        case OP_READ:
            /* The iovec array the data lands in, allocated from the reply
             * buffer when the sequence is BUILT -- the per-op path reserves
             * the same array, at op time. */
            return sizeof(struct evpl_iovec) * NFS4_VFS_READ_MAX_IOV + slack;
        case OP_LOCKT:
        case OP_LOCK:
            /* A denied answer copies the holder's owner string into the reply
             * (nfs4_fill_denied_owner), up to the protocol's opaque limit. */
            return NFS4_OPAQUE_LIMIT + slack;
        case OP_READDIR:
            /* Entries are charged against maxcount, plus one entry's worth for
             * the candidate that is allocated and rolled back when it does not
             * fit -- that allocation must succeed, or the entry would be
             * refused for want of buffer rather than for want of maxcount. */
            return (uint64_t) argop->opreaddir.maxcount + 4096 + slack;
        case OP_GETXATTR:
            return (uint64_t) CHIMERA_NFS4_GETXATTR_MAX +
                   CHIMERA_VFS_XATTR_NAME_MAX + slack;
        case OP_LISTXATTRS:
            /* The staged name buffer, plus the result array that points into
            * it: at most one entry per two bytes of names, 16 bytes each. */
            return 9 * (uint64_t) argop->oplistxattrs.lxa_maxcount + slack;
        case OP_SETXATTR:
        case OP_REMOVEXATTR:
            return CHIMERA_VFS_XATTR_NAME_MAX + slack;
        default:
            return slack;
    } /* switch */
} /* nfs4_vfs_op_reply_bound */

/*
 * Build the range probe a LOCKT asks, from its arguments alone.
 *
 * Everything it needs is on the wire or in the client table, so it is built
 * when the sequence is -- which is what lets the probe itself be an op the
 * sequence executes rather than a question asked once every result is in.
 */
static void
nfs4_vfs_lockt_init_probe(
    struct nfs_request       *req,
    const struct LOCKT4args  *args,
    struct chimera_vfs_claim *probe)
{
    struct chimera_claim_owner owner;

    memset(&owner, 0, sizeof(owner));
    owner.proto = CHIMERA_CLAIM_PROTO_NFSV4;

    /* RFC 8881 §2.4: in 4.1+ the client is the session's, not the one in
     * lock_owner4, which clients routinely leave zero or stale.  LOCK
     * registers under the server-assigned id, so keying the probe on the wire
     * field would make the caller's own locks look foreign. */
    if (req->minorversion > 0 && req->session &&
        req->session->client_unified) {
        owner.client_key = req->session->client_unified->client_id;
    } else {
        owner.client_key = args->owner.clientid;
    }
    owner.owner_lo = XXH3_64bits(args->owner.owner.data,
                                 args->owner.owner.len);
    owner.owner_hi = 0;

    /* NFSv4 and the claim core both use UINT64_MAX as the "to EOF" sentinel,
     * so the wire length passes through unchanged. */
    chimera_vfs_claim_init_range(probe,
                                 !(args->locktype == READ_LT ||
                                   args->locktype == READW_LT),
                                 /*smb=*/ false,
                                 args->offset, args->length,
                                 &owner);
} /* nfs4_vfs_lockt_init_probe */

/* The holder a LOCKT's probe reported, as the operation's denied body. */
static void
nfs4_vfs_lockt_fill_denied(
    struct nfs_request                      *req,
    struct LOCKT4res                        *res,
    const struct chimera_vfs_claim_conflict *conflict)
{
    res->denied.offset = conflict->offset;
    /* conflict.length already uses UINT64_MAX for a to-EOF holder, so it maps
     * directly to the NFSv4 denied length. */
    res->denied.length = conflict->length;
    /* WRITE_LT iff the holder writes: a write delegation (CW) must report
     * WRITE_LT though it holds no LW. */
    res->denied.locktype = (conflict->used & (CHIMERA_CLAIM_W |
                                              CHIMERA_CLAIM_CW |
                                              CHIMERA_CLAIM_LW))
        ? WRITE_LT : READ_LT;
    nfs4_fill_denied_owner(&req->thread->shared->nfs4_shared_clients,
                           conflict, &res->denied.owner,
                           req->encoding->dbuf);
} /* nfs4_vfs_lockt_fill_denied */

/* The NFSv4 op a given VFS op carries the result of. */
static struct nfs4_vfs_op *
nfs4_vfs_op_by_vfs_res(
    struct nfs4_vfs_compound_ctx *ctx,
    uint32_t                      index)
{
    uint32_t k;

    for (k = 0; k < ctx->num_ops; k++) {
        if (ctx->ops[k].vfs_res >= 0 &&
            (uint32_t) ctx->ops[k].vfs_res == index) {
            return &ctx->ops[k];
        }
    }

    return NULL;
} /* nfs4_vfs_op_by_vfs_res */

/*
 * Discard whatever a previous attempt at this READDIR marshalled.
 *
 * Rewinding the reply buffer to the mark, rather than merely stopping at it,
 * is safe because during a sequence the entry fill is the ONLY thing that
 * allocates from that buffer: every other result is marshalled after the
 * sequence has finished, and what the build staged (the xattr names) it staged
 * before the sequence started.  So everything past the mark belongs to this op.
 */
static void
nfs4_vfs_readdir_reset(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    void                        *private_data)
{
    struct nfs4_vfs_compound_ctx *ctx = private_data;
    struct nfs4_vfs_op           *map = nfs4_vfs_op_by_vfs_res(ctx, index);

    if (!map) {
        return;
    }

    if (map->readdir_have_mark) {
        ctx->req->encoding->dbuf->used = map->readdir_mark;
    } else {
        map->readdir_mark      = ctx->req->encoding->dbuf->used;
        map->readdir_have_mark = 1;
    }

    /* The fixed READDIR4resok overhead is charged before any entry:
     * cookieverf (8) plus the dirlist4 booleans (4 each). */
    map->readdir_cursor.count   = 16;
    map->readdir_cursor.entries = NULL;
    map->readdir_cursor.last    = NULL;
} /* nfs4_vfs_readdir_reset */

/*
 * Marshal one entry, by the same code and at the same moment the per-op path
 * runs it from: inside the enumeration, while the backend still owns the
 * attributes it is handing over -- which is what lets a per-entry ACL be
 * encoded here at all.  maxcount is applied entry by entry through the cursor,
 * so the page ends on the entry that does not fit rather than at a count
 * guessed before anything was marshalled.
 */
static int
nfs4_vfs_readdir_append(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *private_data)
{
    struct nfs4_vfs_compound_ctx         *ctx = private_data;
    struct nfs4_vfs_op                   *map = nfs4_vfs_op_by_vfs_res(ctx, index);
    const struct chimera_vfs_compound_op *vop;

    (void) inum;

    if (!map) {
        return -1;
    }

    vop = chimera_vfs_compound_op(compound, index);

    return chimera_nfs4_readdir_entry_fill(
        ctx->req,
        &ctx->req->args_compound->argarray[map->res_index].opreaddir,
        &map->readdir_cursor,
        vop->fh, (int) vop->fh_len,
        cookie, name, namelen, attrs);
} /* nfs4_vfs_readdir_append */

/*
 * Finish a READDIR whose page is already marshalled.  All that is left is what
 * the per-op path does in its own completion: the too-small judgement, the
 * cookie verifier, and pointing the result at the list.
 */
static void
nfs4_vfs_readdir_fill(
    struct READDIR4res                   *res,
    const struct chimera_vfs_compound_op *vop,
    const struct nfs4_vfs_op             *map)
{
    uint64_t cv;

    /* The too-small judgement (RFC 7530 16.24.4) is the gate's: a READDIR that
     * reaches here passed it. */
    cv = vop->r_verifier ? vop->r_verifier : vop->r_cookie;
    memcpy(res->resok4.cookieverf, &cv, sizeof(res->resok4.cookieverf));

    res->resok4.reply.eof     = vop->eof;
    res->resok4.reply.entries = map->readdir_cursor.entries;
} /* nfs4_vfs_readdir_fill */

/*
 * Does the object an exclusive create collided with carry this OPEN's own
 * verifier?  If so the create is a retry of one that already succeeded and the
 * OPEN succeeds against the existing object; if not, somebody else's file is in
 * the way (RFC 7530 §16.16.4).
 */
static int
nfs4_vfs_open_verifier_matches(
    const struct OPEN4args         *args,
    const struct chimera_vfs_attrs *attr)
{
    const uint8_t *verf;
    uint32_t       verf_atime, verf_mtime;

    verf = (args->openhow.how.mode == EXCLUSIVE4) ?
        args->openhow.how.createverf :
        args->openhow.how.ch_createboth.cva_verf;

    memcpy(&verf_atime, verf, sizeof(verf_atime));
    memcpy(&verf_mtime, verf + sizeof(verf_atime), sizeof(verf_mtime));

    return (attr->va_set_mask & CHIMERA_VFS_ATTR_ATIME) &&
           (attr->va_set_mask & CHIMERA_VFS_ATTR_MTIME) &&
           attr->va_atime.tv_sec == verf_atime &&
           attr->va_mtime.tv_sec == verf_mtime;
} /* nfs4_vfs_open_verifier_matches */

static int
nfs4_vfs_open_is_exclusive(const struct OPEN4args *args)
{
    return args->openhow.opentype == OPEN4_CREATE &&
           (args->openhow.how.mode == EXCLUSIVE4 ||
            args->openhow.how.mode == EXCLUSIVE4_1);
} /* nfs4_vfs_open_is_exclusive */

/*
 * Map a failed VFS op onto the status its NFSv4 operation reports.  Two
 * operations do not use the generic mapping: PUTFH turns a missing object into
 * NFS4ERR_STALE, and LISTXATTRS turns a too-small buffer into NFS4ERR_TOOSMALL
 * rather than the xattr-size error.
 */
static nfsstat4
nfs4_vfs_op_errno(
    const struct nfs_argop4              *ap,
    const struct chimera_vfs_compound_op *vop,
    struct nfs_request                   *req)
{
    enum chimera_vfs_error err   = vop->status;
    uint32_t               argop = ap->argop;

    if (argop == OP_PUTFH) {
        return chimera_nfs4_putfh_errno(err);
    }

    /* LISTXATTRS and READDIR both turn a too-small buffer into
     * NFS4ERR_TOOSMALL rather than the generic size error -- READDIR's is set
     * by the gate, which is where its page is judged. */
    if ((argop == OP_LISTXATTRS || argop == OP_READDIR) &&
        err == CHIMERA_VFS_ERANGE) {
        return NFS4ERR_TOOSMALL;
    }

    /* An exclusive create whose re-open of the colliding object failed.  No
     * object that is not a regular file can be carrying the verifier an
     * exclusive create stamped, so whatever is in the way and however the
     * backend reported it, the answer the protocol wants is simply "something
     * else is already there" (RFC 7530 §16.16.4). */
    if (argop == OP_OPEN && vop->existed && nfs4_vfs_open_is_exclusive(&ap->opopen)) {
        return NFS4ERR_EXIST;
    }

    /* An OPEN refused by the type gate: the VFS reports the nearest POSIX
     * answer, but NFSv4 distinguishes a directory from a symlink from any other
     * special file, and differently in each minor version, so the mode the
     * refusal carried is what decides it.  A mode is recorded only by the
     * resolve step, which runs only when the type gate was asked for, so a
     * non-regular one here means the gate is what refused. */
    if (argop == OP_OPEN && vop->existed && vop->existing_mode &&
        !S_ISREG(vop->existing_mode)) {
        return chimera_nfs4_open_nonreg_status(req->minorversion,
                                               vop->existing_mode);
    }

    /* I/O the executor refused on the object's type, before it opened it for
     * data.  It reports the nearest POSIX answer; NFSv4 has its own. */
    if ((argop == OP_READ || argop == OP_WRITE) && vop->existing_mode &&
        !S_ISREG(vop->existing_mode)) {
        return chimera_nfs4_data_nonreg_status(vop->existing_mode);
    }

    return chimera_nfs4_errno_to_nfsstat4(err);
} /* nfs4_vfs_op_errno */

/*
 * Everything an OPEN settles before the object it names has been opened.
 *
 * Two things, and both for the same reason LOCK's prepare exists: they are
 * pure functions of the operation's arguments and the client table, and the
 * ops that depend on them are ops of the run.  The open_owner is resolved so
 * the gate can read this client's existing opens on the object without
 * resolving anything from inside a gate; the share reservation is BUILT so
 * the CLAIM op that takes it has a claim to take -- the claim core keeps
 * pointers into an inserted claim, so it cannot be a copy made later.
 *
 * Returns 0 when the OPEN cannot be carried (no client, or no reservation to
 * take, both of which the op-at-a-time path answers for), leaving nothing
 * pinned.
 */
static int
nfs4_vfs_open_prepare(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    const struct nfs_argop4          *argop,
    struct nfs4_vfs_op               *map)
{
    const struct OPEN4args *args   = &argop->opopen;
    struct nfs_client      *client = req->session ?
        req->session->client_unified : NULL;
    bool                    created = false;

    (void) thread;

    if (!client) {
        return 0;
    }

    /* The adopt candidate is the 4.0 entry's pin, exactly as install_state
     * passes it: a lease sweep that unpublished the owner mid-flight
     * republishes the SAME struct, which is what keeps the seqid bookkeeping
     * and this open state on one object. */
    map->open_owner = nfs_open_owner_find_or_adopt(client,
                                                   req->open_4_0_owner,
                                                   args->owner.owner.data,
                                                   args->owner.owner.len,
                                                   &created);

    if (!map->open_owner) {
        return 0;
    }

    map->share = chimera_nfs4_open_share_lease_build(args, client,
                                                     args->share_access,
                                                     args->share_deny);

    if (!map->share) {
        /* The scan admits no OPEN whose share_access is empty, so this is an
         * allocation failure; the op-at-a-time path reports it. */
        nfs_open_owner_put(map->open_owner);
        map->open_owner = NULL;
        return 0;
    }

    return 1;
} /* nfs4_vfs_open_prepare */

/*
 * Give back everything a prepared OPEN holds, for every way it can end
 * without its state being installed: the build gave up, the sequence stopped
 * in front of the OPEN, or the OPEN's own group failed.
 *
 * No seqid advances here -- that is the OPEN's own wrapper's business, and it
 * runs (or deliberately does not) on its own terms.
 */
static void
nfs4_vfs_open_abandon(
    struct chimera_server_nfs_thread *thread,
    struct nfs4_vfs_op               *map)
{
    if (map->share) {
        nfs4_share_lease_free(thread->vfs->vfs_state, map->share);
        map->share = NULL;
    }

    if (map->open_owner) {
        nfs_open_owner_put(map->open_owner);
        map->open_owner = NULL;
    }
} /* nfs4_vfs_open_abandon */

/* Fill one NFSv4 result from the VFS ops that produced it. */
static nfsstat4
nfs4_vfs_op_fill(
    struct nfs_request           *req,
    struct chimera_vfs_compound  *compound,
    struct nfs4_vfs_compound_ctx *ctx,
    struct nfs4_vfs_op           *map,
    struct nfs_argop4            *argop,
    struct nfs_resop4            *resop)
{
    const struct chimera_vfs_compound_op *vop =
        chimera_vfs_compound_op(compound, (uint32_t) map->vfs_res);
    nfsstat4                              status;
    uint32_t                              requested;
    void                                 *names;

    switch (argop->argop) {
        case OP_OPEN:
        {
            struct OPEN4args               *oargs = &argop->opopen;
            struct OPEN4res                *ores  = &resop->opopen;
            struct chimera_vfs_open_handle *handle;
            uint32_t                        install_rflags = 0;
            struct nfs4_share_lease        *share          = NULL;
            bool                            deferred;
            int                             rc;

            /* The verifier and type rules ran in the gate, in front of the
             * CLAIM and the SETATTR behind this op -- see there. */

            handle = chimera_vfs_compound_take_handle(
                compound, (uint32_t) map->vfs_res);

            if (!handle) {
                return NFS4ERR_SERVERFAULT;
            }

            /* The share reservation.  A granted CLAIM's file state is the
             * caller's, and taking it is what says the reservation is held;
             * on a coalesce the gate spared the CLAIM and there is nothing to
             * take.
             *
             * take_file_state answering NULL for a CLAIM that WAS granted
             * means the run failed behind the OPEN and the executor's abort
             * release took the reservation back before this callback -- right
             * for a claim nobody has been told about, and this reply has not
             * been written yet, so the OPEN simply arbitrates again for the
             * result it is in the middle of writing.  (The other way out, the
             * one LOCK takes, is to end the run; a claim taken for an op whose
             * result is already committed cannot be re-arbitrated, and one
             * taken for the op currently being filled can.) */
            if (map->open_coalesce) {
                /* Nothing was asked for and nothing is held. */
                nfs4_vfs_open_abandon(req->thread, map);
            } else if (map->share && ctx->open_claim_op >= 0) {
                const struct chimera_vfs_compound_op *cvop =
                    chimera_vfs_compound_op(compound,
                                            (uint32_t) ctx->open_claim_op);
                struct chimera_vfs_file_state        *fs =
                    chimera_vfs_compound_take_file_state(
                        compound, (uint32_t) ctx->open_claim_op);

                if (fs) {
                    status = chimera_nfs4_open_share_status(
                        map->share, fs, cvop->claim_result);
                } else {
                    status = chimera_nfs4_open_share_lease_acquire(
                        req, map->share, handle);
                }

                if (status != NFS4_OK) {
                    chimera_vfs_release(req->thread->vfs_thread, handle);
                    nfs4_vfs_open_abandon(req->thread, map);
                    return status;
                }

                share = map->share;
                /* install_state owns it from here, on every one of its own
                 * exits. */
                map->share = NULL;
            }

            /* Capture the file handle before install_state, which may release
             * the handle when it coalesces onto an existing open state. */
            memcpy(req->fh, handle->fh, handle->fh_len);
            req->fhlen = handle->fh_len;

            /* From here the two paths are the same code: the object is open
             * and its attributes are in hand, which is all install_state ever
             * needed.  It owns the handle now, including releasing it on every
             * failure.
             *
             * An open-by-handle reports no attributes, and install_state reads
             * that as "access was established when this filehandle was
             * resolved" and skips the check -- which is what it must not be
             * told by an empty attribute set that looks like a real one. */
            status = chimera_nfs4_open_install_state(req, handle,
                                                     map->open_by_name ?
                                                     &vop->attr : NULL,
                                                     vop->created,
                                                     NULL, 0, share,
                                                     &ores->resok4.stateid,
                                                     &install_rflags);

            if (status != NFS4_OK) {
                /* install_state gave the reservation back on its way out. */
                nfs4_vfs_open_abandon(req->thread, map);
                return status;
            }

            ores->status        = NFS4_OK;
            ores->resok4.rflags = install_rflags |
                OPEN4_RESULT_LOCKTYPE_POSIX;
            ores->resok4.num_attrset = 0;

            /* Which of the requested attributes the create actually applied.
             * set_attr is the executor's copy, which it blanks when the name
             * resolved to something that already existed -- so an open that
             * created nothing reports nothing set, which is what the per-op
             * path reports for the same reason.
             *
             * The requested set lives in a different arm of openhow.how for
             * each create mode, and EXCLUSIVE4 has none at all: its verifier
             * occupies that slot, so reading it as an attribute request would
             * be reading the verifier's bytes as an attribute mask. */
            if (oargs->openhow.opentype == OPEN4_CREATE &&
                oargs->openhow.how.mode != EXCLUSIVE4) {
                struct chimera_vfs_attrs applied = vop->set_attr;
                uint32_t                 n_mask;
                uint32_t                *mask;

                if (oargs->openhow.how.mode == EXCLUSIVE4_1) {
                    n_mask = oargs->openhow.how.ch_createboth.cva_attrs.num_attrmask;
                    mask   = oargs->openhow.how.ch_createboth.cva_attrs.attrmask;
                } else {
                    n_mask = oargs->openhow.how.createattrs.num_attrmask;
                    mask   = oargs->openhow.how.createattrs.attrmask;
                }

                rc = xdr_dbuf_alloc_array(&ores->resok4, attrset, 4,
                                          req->encoding->dbuf);
                chimera_nfs_abort_if(rc, "Failed to allocate array");

                ores->resok4.num_attrset = chimera_nfs4_mask2attr(
                    &applied, n_mask, mask, ores->resok4.attrset);
            }

            if (map->open_by_name) {
                struct chimera_vfs_attrs pre  = vop->dir_pre_attr;
                struct chimera_vfs_attrs post = vop->dir_post_attr;

                chimera_nfs4_set_changeinfo(&ores->resok4.cinfo, &pre, &post);
            } else {
                /* An open-by-handle changed no directory. */
                ores->resok4.cinfo.atomic = 0;
                ores->resok4.cinfo.before = 0;
                ores->resok4.cinfo.after  = 0;
            }

            /* An UNCHECKED4 size-0 create of a name that was already there is
             * a SETATTR op of the run now, behind the CLAIM that reserves the
             * share -- so it still does not empty a file for an OPEN that
             * fails, and it is now also in front of a delegation grant rather
             * than behind one, which is where a truncate belongs. */
            req->open_trunc_pending = false;

            /* The owner reference the build took; install_state resolved its
             * own. */
            if (map->open_owner) {
                nfs_open_owner_put(map->open_owner);
                map->open_owner = NULL;
            }

            ctx->open_filled   = 1;
            ctx->open_has_attr = map->open_by_name;
            ctx->open_attr     = vop->attr;

            if (ctx->open_ends_run) {
                /* The OPEN still owes a delegation decision that can park on
                 * an in-flight CB_NULL probe, so it runs once the sequence has
                 * been freed -- which is why the attributes it takes were
                 * copied out above.  See nfs4_vfs_compound_complete. */
                return NFS4_OK;
            }

            /* No delegation can be earned (nfs4_vfs_open_may_delegate said
             * so when the run was built), so the whole of what is left is
             * memory and it happens here, while req->index still names the
             * OPEN: the decline the reply owes, and the RFC 7530 §9.1.7
             * wrapper.  The ops behind the OPEN are filled after it, and the
             * generic completion hands the request on once. */
            deferred = chimera_nfs4_open_grant_delegation(
                req, ores, map->open_by_name ? &vop->attr : NULL);

            chimera_nfs_abort_if(deferred,
                                 "NFSv4 OPEN: a run carried an OPEN that then parked");

            chimera_nfs4_open_settle(req, NFS4_OK);

            return NFS4_OK;
        }


        case OP_CREATE:
        {
            struct CREATE4args      *cargs   = &argop->opcreate;
            struct CREATE4res       *cres    = &resop->opcreate;
            struct chimera_vfs_attrs applied = vop->set_attr;
            struct chimera_vfs_attrs pre     = vop->dir_pre_attr;
            struct chimera_vfs_attrs post    = vop->dir_post_attr;

            cres->status         = NFS4_OK;
            cres->resok4.attrset = xdr_dbuf_alloc_space(4 * sizeof(uint32_t),
                                                        req->encoding->dbuf);
            chimera_nfs_abort_if(cres->resok4.attrset == NULL,
                                 "Failed to allocate space");
            cres->resok4.num_attrset = chimera_nfs4_mask2attr(
                &applied,
                cargs->createattrs.num_attrmask,
                cargs->createattrs.attrmask,
                cres->resok4.attrset);

            chimera_nfs4_set_changeinfo(&cres->resok4.cinfo, &pre, &post);
            return NFS4_OK;
        }

        case OP_READ:
        {
            struct READ4res   *rdres = &resop->opread;
            struct evpl_iovec *riov;
            int                rniov;

            chimera_vfs_compound_take_iov(compound, (uint32_t) map->vfs_res,
                                          &riov, &rniov);

            rdres->status             = NFS4_OK;
            rdres->resok4.eof         = vop->eof_read;
            rdres->resok4.data.length = vop->read_len;
            rdres->resok4.data.niov   = rniov;
            rdres->resok4.data.iov    = riov;
            return NFS4_OK;
        }

        case OP_WRITE:
        {
            struct WRITE4res *wrres = &resop->opwrite;

            wrres->status       = NFS4_OK;
            wrres->resok4.count = vop->written;
            /* Achieved durability, which the backend may report as more than
             * was asked for. */
            wrres->resok4.committed = vop->committed;
            memcpy(wrres->resok4.writeverf, &req->thread->shared->nfs_verifier,
                   sizeof(wrres->resok4.writeverf));
            return NFS4_OK;
        }

        case OP_SECINFO:
        {
            struct SECINFO4res              *sires = &resop->opsecinfo;
            const struct chimera_nfs_export *export;

            /* The name resolved, so it lives in the current filehandle's
             * export and that export's policy is the answer. */
            export = chimera_nfs_get_export_by_id(req->thread->shared,
                                                  req->export_id);

            sires->resok4 = xdr_dbuf_alloc_space(4 * sizeof(struct secinfo4),
                                                 req->encoding->dbuf);
            chimera_nfs_abort_if(sires->resok4 == NULL,
                                 "Failed to allocate space");

            sires->num_resok4 = chimera_nfs_fill_secinfo(
                sires->resok4,
                export ? export->sec_allowed : 0,
                req->thread->shared->gss_enabled);

            /* RFC 8881 §18.29.3: SECINFO consumes the current filehandle on
             * success -- and that is a 4.1 rule.  RFC 7530 §16.31.3 describes
             * the same operation without it and leaves the handle in place,
             * which is what a 4.0 client is entitled to rely on, and what both
             * NFS-Ganesha and the Linux server do.  The per-op path gates this
             * on the minor version (nfs4_proc_secinfo.c); this path did not,
             * which put the same consumption back for 4.0 compounds.
             *
             * Recorded rather than applied, because the sequence sets req->fh
             * from its last op after every result has been filled -- see
             * nfs4_vfs_compound_complete. */
            if (req->minorversion >= 1) {
                ctx->fh_consumed = 1;
            }

            sires->status = NFS4_OK;
            return NFS4_OK;
        }

        case OP_LOCKT:
            /* The probe ran as a CLAIM_TEST op inside the sequence, and it
             * answered GRANTED -- a denial is the gate's business (see
             * nfs4_vfs_compound_gate), because a denied LOCKT truncates the
             * COMPOUND and nothing behind it may run. */
            resop->oplockt.status = NFS4_OK;
            return NFS4_OK;

        case OP_LOCK:
        {
            struct nfs4_range_lease *rl = req->nfs_inflight_range;

            /* The CLAIM was granted, and the claim it inserted is the
             * caller's from here -- which means the file state is too, and
             * the coalesce surgery in the apply needs it. */
            rl->file_state = chimera_vfs_compound_take_file_state(
                compound, (uint32_t) map->vfs_res);

            chimera_nfs_abort_if(rl->file_state == NULL,
                                 "NFSv4 LOCK: granted claim without a file state");

            ctx->lock_filled = 1;

            return chimera_nfs4_lock_apply(req, CHIMERA_CLAIM_GRANTED, NULL);
        }

        case OP_SETATTR:
        {
            struct SETATTR4args     *sargs   = &argop->opsetattr;
            struct SETATTR4res      *sres    = &resop->opsetattr;
            struct chimera_vfs_attrs applied = vop->set_attr;

            sres->status   = NFS4_OK;
            sres->attrsset = xdr_dbuf_alloc_space(4 * sizeof(uint32_t),
                                                  req->encoding->dbuf);
            chimera_nfs_abort_if(sres->attrsset == NULL,
                                 "Failed to allocate space");
            sres->num_attrsset = chimera_nfs4_mask2attr(
                &applied,
                sargs->obj_attributes.num_attrmask,
                sargs->obj_attributes.attrmask,
                sres->attrsset);
            return NFS4_OK;
        }

        case OP_REMOVE:
        {
            struct REMOVE4res       *rres = &resop->opremove;
            struct chimera_vfs_attrs pre  = vop->dir_pre_attr;
            struct chimera_vfs_attrs post = vop->dir_post_attr;

            rres->status = NFS4_OK;
            /* change_info4 for the parent directory (RFC 7530 §16.25.5). */
            chimera_nfs4_set_changeinfo(&rres->resok4.cinfo, &pre, &post);
            return NFS4_OK;
        }

        case OP_RENAME:
        {
            struct RENAME4res       *rres      = &resop->oprename;
            struct chimera_vfs_attrs from_pre  = vop->from_dir_pre_attr;
            struct chimera_vfs_attrs from_post = vop->from_dir_post_attr;
            struct chimera_vfs_attrs to_pre    = vop->dir_pre_attr;
            struct chimera_vfs_attrs to_post   = vop->dir_post_attr;

            rres->status = NFS4_OK;
            /* Two change_info4s, because a rename changes two directories
             * (RFC 7530 SS16.27.5).  The VFS op reports both for the same
             * reason, so each half comes straight across. */
            chimera_nfs4_set_changeinfo(&rres->resok4.source_cinfo,
                                        &from_pre, &from_post);
            chimera_nfs4_set_changeinfo(&rres->resok4.target_cinfo,
                                        &to_pre, &to_post);
            return NFS4_OK;
        }

        case OP_LINK:
        {
            struct LINK4res         *lres = &resop->oplink;
            struct chimera_vfs_attrs pre  = vop->dir_pre_attr;
            struct chimera_vfs_attrs post = vop->dir_post_attr;

            lres->status = NFS4_OK;
            /* One directory changed: the one the new name went into
             * (RFC 7530 SS16.9.5). */
            chimera_nfs4_set_changeinfo(&lres->resok4.cinfo, &pre, &post);
            return NFS4_OK;
        }

        case OP_PUTFH:
            /* The staleness rule already ran as the precheck; nothing else in
             * a PUTFH4res but its status. */
            resop->opputfh.status = NFS4_OK;
            return NFS4_OK;

        case OP_LOOKUP:
            /* LOOKUP4res carries only a status; the object it resolved is the
             * sequence's current filehandle. */
            resop->oplookup.status = NFS4_OK;
            return NFS4_OK;

        case OP_LOOKUPP:
            /* As LOOKUP: the parent it resolved is the current filehandle. */
            resop->oplookupp.status = NFS4_OK;
            return NFS4_OK;

        case OP_SAVEFH:
            /* Save the object the sequence had current when the SAVEFH ran, so
             * a RESTOREFH -- here or, in principle, later -- finds it. */
            chimera_nfs4_savefh_apply(req, vop->fh, (int) vop->fh_len);
            resop->opsavefh.status = NFS4_OK;
            return NFS4_OK;

        case OP_RESTOREFH:
            /* The executor has already made the saved object current; this
             * restores the request-side bookkeeping that rides with it.  A
             * RESTOREFH is only encoded when a SAVEFH earlier in this same
             * sequence filled the slot, so the export (and with it the squash
             * the sequence ran under) is the one already in force. */
            chimera_nfs4_restorefh_apply(req);
            resop->oprestorefh.status = NFS4_OK;
            return NFS4_OK;

        case OP_COMMIT:
            /* The regular-file rule already ran as the precheck; this re-tests
             * it against the attributes the flush itself reported, as the
             * per-op completion does, and stamps the write verifier. */
            status = chimera_nfs4_commit_fill(req, &resop->opcommit,
                                              &vop->attr);
            resop->opcommit.status = status;
            return status;

        case OP_VERIFY:
        case OP_NVERIFY:
            /* A mismatch failed the op in the gate; reaching here is a match. */
            resop->opverify.status = NFS4_OK;
            return NFS4_OK;

        case OP_WRITE_SAME:
            resop->opwrite_same.resok4.num_wr_callback_id = 0;
            resop->opwrite_same.resok4.wr_callback_id     = NULL;
            resop->opwrite_same.resok4.wr_count           = vop->written;
            resop->opwrite_same.resok4.wr_committed       = vop->committed;
            memcpy(resop->opwrite_same.resok4.wr_writeverf,
                   &req->thread->shared->nfs_verifier,
                   sizeof(resop->opwrite_same.resok4.wr_writeverf));
            resop->opwrite_same.wsr_status = NFS4_OK;
            return NFS4_OK;

        case OP_ALLOCATE:
            resop->opallocate.ar_status = NFS4_OK;
            return NFS4_OK;

        case OP_DEALLOCATE:
            resop->opdeallocate.dr_status = NFS4_OK;
            return NFS4_OK;

        case OP_SEEK:
            resop->opseek.resok4.sr_eof    = vop->seek_eof;
            resop->opseek.resok4.sr_offset = vop->seek_offset;
            resop->opseek.sa_status        = NFS4_OK;
            return NFS4_OK;

        case OP_READDIR:
            nfs4_vfs_readdir_fill(&resop->opreaddir, vop, map);
            status                  = NFS4_OK;
            resop->opreaddir.status = status;
            return status;

        case OP_GETXATTR:
            status = chimera_nfs4_getxattr_fill(req, &resop->opgetxattr,
                                                vop->buffer, vop->buffer_len);
            resop->opgetxattr.gxr_status = status;
            return status;

        case OP_SETXATTR:
            chimera_nfs4_setxattr_fill(&resop->opsetxattr, &vop->pre_ctime,
                                       &vop->post_ctime);
            resop->opsetxattr.sxr_status = NFS4_OK;
            return NFS4_OK;

        case OP_LISTXATTRS:
            /* The result entries point into the name buffer instead of copying
             * each name out of it, so it has to outlive the reply -- and the
             * sequence's buffer is freed the moment this callback returns.
             * Restage the names where the per-op path keeps them, in the reply
             * buffer, before pointing anything at them. */
            names = xdr_dbuf_alloc_space(vop->buffer_len ? vop->buffer_len : 1,
                                         req->encoding->dbuf);

            if (!names) {
                resop->oplistxattrs.lxr_status = NFS4ERR_RESOURCE;
                return NFS4ERR_RESOURCE;
            }

            memcpy(names, vop->buffer, vop->buffer_len);

            status = chimera_nfs4_listxattrs_fill(req, &resop->oplistxattrs,
                                                  names,
                                                  vop->buffer_count,
                                                  vop->eof, vop->r_cookie);
            resop->oplistxattrs.lxr_status = status;
            return status;

        case OP_REMOVEXATTR:
            chimera_nfs4_removexattr_fill(&resop->opremovexattr,
                                          &vop->pre_ctime, &vop->post_ctime);
            resop->opremovexattr.rxr_status = NFS4_OK;
            return NFS4_OK;

        case OP_GETATTR:
            status = chimera_nfs4_getattr_fill(req, &argop->opgetattr,
                                               &resop->opgetattr, &vop->attr,
                                               vop->fh, (int) vop->fh_len);
            resop->opgetattr.status = status;
            return status;

        case OP_ACCESS:
            requested = chimera_nfs4_access_requested(req, &argop->opaccess,
                                                      &vop->attr, vop->fh,
                                                      (int) vop->fh_len);
            /* The executor evaluated the client's whole request against the
             * object's ACL (or mode) while it held it; the mapping back is
             * limited to the bits this server says it evaluated. */
            chimera_nfs4_access_fill(req, &resop->opaccess, &vop->attr,
                                     requested, vop->granted);
            return NFS4_OK;

        case OP_GETFH:
            status = chimera_nfs4_getfh_fill(req, &resop->opgetfh,
                                             vop->fh, (int) vop->fh_len);
            resop->opgetfh.status = status;
            return status;

        case OP_READLINK:
            status = chimera_nfs4_readlink_fill(req, &resop->opreadlink,
                                                vop->target, vop->target_len);
            resop->opreadlink.status = status;
            return status;

        default:
            return NFS4ERR_SERVERFAULT;
    } /* switch */
} /* nfs4_vfs_op_fill */

/*
 * Apply a slot: the whole of an NFSv4 operation that drives no VFS call.
 *
 * These run in fill order, so their effects land in the order the COMPOUND
 * wrote them relative to each other and to the fills of the VFS-backed ops --
 * but after every VFS op in the run has executed.  That is what the scan's
 * refusals in front of a slot exist to make safe (nfs4_vfs_op_is_slot).
 *
 * Each writes its own result and hands the status back; the caller treats it
 * exactly as it treats a fill's.
 */
static nfsstat4
nfs4_vfs_slot_apply(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    struct nfs_argop4                *argop,
    struct nfs_resop4                *resop)
{
    switch (argop->argop) {
        case OP_CLOSE:
            return chimera_nfs4_close_apply(thread, req, argop, resop);
        case OP_LOCKU:
            return chimera_nfs4_locku_apply(thread, req, argop, resop);
        case OP_OPEN_DOWNGRADE:
            return chimera_nfs4_open_downgrade_apply(thread, req, argop, resop);
        case OP_DELEGRETURN:
            return chimera_nfs4_delegreturn_apply(thread, req, argop, resop);
        default:
            return NFS4ERR_SERVERFAULT;
    } /* switch */
} /* nfs4_vfs_slot_apply */

/*
 * The NFS4.1 current-stateid lifecycle (RFC 8881 §16.2.3.1.2) for one op,
 * applied before that op's result is filled -- which is where the per-op
 * dispatcher applies it too, immediately before it dispatches.
 *
 * Only the ops that CLEAR or CARRY it are here.  The ops that SET it (OPEN,
 * and the slots) do so from their own bodies as they are applied, so they are
 * already in the right place in this order.  That ordering is the whole
 * reason this is done op by op rather than replayed in bulk before the
 * sequence is submitted: with a CLOSE inside the run, "OPEN; PUTFH x;
 * CLOSE(current)" would otherwise clear at build time and set at fill time.
 */
static void
nfs4_vfs_current_stateid_step(
    struct nfs_request *req,
    uint32_t            argop)
{
    switch (argop) {
        case OP_PUTFH:
        case OP_LOOKUP:
        case OP_LOOKUPP:
        case OP_CREATE:
            chimera_nfs4_clear_current_stateid(req);
            break;
        case OP_SAVEFH:
            req->saved_current_stateid_valid = req->current_stateid_valid;
            req->saved_current_stateid       = req->current_stateid;
            break;
        case OP_RESTOREFH:
            req->current_stateid_valid = req->saved_current_stateid_valid;
            req->current_stateid       = req->saved_current_stateid;
            break;
        default:
            break;
    } /* switch */
} /* nfs4_vfs_current_stateid_step */

/*
 * Give back everything the sequence borrowed on the caller's behalf: the
 * handles its I/O ops resolved from stateids, and the payload of any WRITE it
 * carried.
 *
 * A WRITE's payload is released here rather than in its fill, because it has to
 * be released whether or not the fill ran -- the op may have failed, or the
 * sequence may have stopped in front of it.  Zeroing niov is what stops the
 * dispatcher's own sweep of undispatched WRITEs from releasing it a second
 * time.
 */
static void
nfs4_vfs_compound_give_back(
    struct chimera_server_nfs_thread *thread,
    struct nfs4_vfs_compound_ctx     *ctx)
{
    struct nfs_request *req = ctx->req;
    uint32_t            k;

    for (k = 0; k < ctx->num_ops; k++) {
        struct nfs_argop4 *argop =
            &req->args_compound->argarray[ctx->ops[k].res_index];

        if (ctx->ops[k].io_handle) {
            chimera_vfs_release(thread->vfs_thread, ctx->ops[k].io_handle);
            ctx->ops[k].io_handle = NULL;
        }

        /* A prepared OPEN whose state was never installed: the reservation it
         * built and the owner reference it took go back.  The fill clears
         * both as it hands them on, so this is the ways it did not run --
         * the sequence stopped in front of the OPEN, or its own group
         * failed.  No seqid advances: that is the OPEN's own wrapper's. */
        if (argop->argop == OP_OPEN) {
            nfs4_vfs_open_abandon(thread, &ctx->ops[k]);
        }

        if (argop->argop == OP_WRITE && argop->opwrite.data.niov) {
            evpl_iovecs_release(thread->evpl, argop->opwrite.data.iov,
                                argop->opwrite.data.niov);
            argop->opwrite.data.niov = 0;
        }
    }
} /* nfs4_vfs_compound_give_back */

/*
 * The NFSv4-side rules that decide whether an operation may proceed, applied as
 * each op finishes rather than when its result is filled.
 *
 * PUTFH's staleness rule is the one that has to be here.  A file handle is
 * stale when the object has no names left and no client holds it open -- and
 * only the server knows the second half, so the VFS cannot answer it.  Applied
 * at fill time it came too late: everything behind the PUTFH had already run,
 * which was harmless while the sequence only read and stopped being harmless as
 * soon as it could rename.
 *
 * It answers from the client table, which is memory the server already holds:
 * no I/O, no waiting, and nothing remembered, so asking it twice asks the same
 * question rather than taking a step twice.
 */
static void
nfs4_vfs_compound_gate(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data)
{
    struct nfs4_vfs_compound_ctx *ctx = private_data;
    struct nfs_request           *req = ctx->req;
    uint32_t                      k;

    if (*status != CHIMERA_VFS_OK) {
        return;
    }

    /* READDIR judges its own page, and now can: the page is marshalled inside
     * the enumeration, so by the time the op finishes the answer exists.  It
     * used to be settled after the sequence, which is what forced the rule
     * that nothing mutating may follow a READDIR. */
    for (k = 0; k < ctx->num_ops; k++) {
        struct nfs4_vfs_op                   *map = &ctx->ops[k];
        const struct chimera_vfs_compound_op *vop;

        if (map->vfs_res < 0 || (uint32_t) map->vfs_res != index) {
            continue;
        }

        vop = chimera_vfs_compound_op(compound, index);

        switch (req->args_compound->argarray[map->res_index].argop) {
            case OP_READDIR:
                /* RFC 7530 16.24.4, applied here so it stops the sequence
                 * rather than being discovered once the ops behind it have
                 * already run. */
                if (!vop->eof && map->readdir_cursor.entries == NULL) {
                    *status = CHIMERA_VFS_ERANGE;
                }
                break;

            case OP_OPEN:
            {
                const struct OPEN4args         *oargs =
                    &req->args_compound->argarray[map->res_index].opopen;
                struct chimera_vfs_compound_op *edit;
                nfsstat4                        ps;
                int                             coalesce = 0;

                /* Everything an OPEN can refuse for, asked here rather than
                 * when the result is filled -- because what is behind it in
                 * the run is the CLAIM that reserves the share and the
                 * SETATTR that empties the file, and neither may run for an
                 * OPEN that is about to fail.  (That ordering is why an OPEN
                 * that fails still leaves an existing file's contents alone,
                 * which is the rule the deferred truncate existed for.) */
                if (vop->existed && nfs4_vfs_open_is_exclusive(oargs)) {
                    /* An exclusive create that collided.  Carrying this
                     * OPEN's verifier is what makes the object ours; nothing
                     * else about it matters, its TYPE included -- no object
                     * that is not a regular file can be carrying a verifier
                     * an exclusive create stamped, so a directory here is
                     * somebody else's name, not a type error (RFC 7530
                     * §16.16.4). */
                    if (!nfs4_vfs_open_verifier_matches(oargs, &vop->attr)) {
                        map->gate_status = NFS4ERR_EXIST;
                        *status          = CHIMERA_VFS_EEXIST;
                        break;
                    }
                } else if ((vop->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
                           !S_ISREG(vop->attr.va_mode)) {
                    /* RFC 7530 §16.16.6 / RFC 8881 §18.16.4: OPEN targets a
                     * regular file.  The type gate in front of the open
                     * catches this for the modes that ask for it; a GUARDED4
                     * create does not, so the object it opened is classified
                     * here. */
                    map->gate_status = chimera_nfs4_open_nonreg_status(
                        req->minorversion, vop->attr.va_mode);
                    *status = CHIMERA_VFS_EINVAL;
                    break;
                }

                /* The NFSv4 rules that outrank a share conflict, and whether
                 * this OPEN coalesces onto one this owner already holds. */
                ps = chimera_nfs4_open_precheck(
                    req, oargs,
                    req->session ? req->session->client_unified : NULL,
                    map->open_owner,
                    map->open_by_name ? &vop->attr : NULL,
                    vop->created,
                    vop->fh, (uint16_t) vop->fh_len,
                    &coalesce);

                if (ps != NFS4_OK) {
                    map->gate_status = ps;
                    *status          = CHIMERA_VFS_EACCES;
                    break;
                }

                map->open_coalesce = coalesce;

                /* A coalesce takes no reservation -- the first OPEN of this
                 * (owner, fh) keeps the one it took -- so the CLAIM op has
                 * nothing to ask and is spared.  Computed from what the OPEN
                 * just reported rather than accumulated, so a re-run of the
                 * sequence makes the same decision. */
                if (ctx->open_claim_op >= 0) {
                    edit = chimera_vfs_compound_op_edit(
                        compound, (uint32_t) ctx->open_claim_op);

                    if (edit) {
                        edit->skip = coalesce ? 1 : 0;
                    }
                }

                /* An UNCHECKED4 size-0 create only truncates when the name
                 * was already there; a create that made the object applied
                 * the size with the open. */
                if (ctx->open_setattr_op >= 0) {
                    edit = chimera_vfs_compound_op_edit(
                        compound, (uint32_t) ctx->open_setattr_op);

                    if (edit) {
                        edit->skip = vop->existed ? 0 : 1;
                    }
                }
                break;
            }

            case OP_LOCKT:
                /* The CLAIM_TEST answered.  A conflict is NFS4ERR_DENIED,
                 * which is a successful query result on the wire but an ERROR
                 * status in a COMPOUND: it truncates the reply there, and
                 * nothing behind it runs.  Failing the op here is what makes
                 * that true -- which is what lets a mutating op sit behind a
                 * LOCKT in the same sequence at all.  The conflict itself is
                 * read back off the op in the completion, where the reply
                 * buffer the owner string is copied into is being written. */
                if (vop->claim_result != CHIMERA_CLAIM_GRANTED) {
                    map->gate_status = NFS4ERR_DENIED;
                    *status          = CHIMERA_VFS_EAGAIN;
                }
                break;

            default:
                break;
        } /* switch */

        /* Not a return: an op whose result IS its gate stat -- VERIFY and
         * NVERIFY, whose map points both vfs_res and vfs_aux at the one
         * getattr -- still has to reach the loop below. */
        break;
    }

    if (*status != CHIMERA_VFS_OK) {
        return;
    }

    for (k = 0; k < ctx->num_ops; k++) {
        struct nfs4_vfs_op                   *map = &ctx->ops[k];
        const struct chimera_vfs_compound_op *aux;

        if (map->vfs_aux < 0 || (uint32_t) map->vfs_aux != index) {
            continue;
        }

        aux = chimera_vfs_compound_op(compound, index);

        switch (req->args_compound->argarray[map->res_index].argop) {
            case OP_PUTFH:
                if (chimera_nfs4_putfh_check_stale(req, &aux->attr, aux->fh,
                                                   (int) aux->fh_len) !=
                    NFS4_OK) {
                    *status = CHIMERA_VFS_ESTALE;
                }
                break;

            case OP_READLINK:
                /* RFC 7530 §16.25: READLINK applies to a symbolic link. */
                if (!(aux->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE)) {
                    *status = CHIMERA_VFS_EFAULT;
                } else if (!S_ISLNK(aux->attr.va_mode)) {
                    *status = CHIMERA_VFS_EINVAL;
                }
                break;

            case OP_VERIFY:
            case OP_NVERIFY:
            {
                nfsstat4 vs = chimera_nfs4_verify_status(req, map->res_index,
                                                         &aux->attr,
                                                         aux->fh,
                                                         (int) aux->fh_len);

                if (vs != NFS4_OK) {
                    /* Carried out of band: the answer is an NFSv4 status with
                     * no errno that means it, and the fill reads it back. */
                    map->gate_status = vs;
                    *status          = CHIMERA_VFS_EINVAL;
                }
                break;
            }

            case OP_LOCKT:
                /* RFC 7530 §16.11.4: byte-range locking is defined only for
                 * regular files.  A directory is NFS4ERR_ISDIR, a symlink
                 * NFS4ERR_SYMLINK, anything else NFS4ERR_INVAL -- distinctions
                 * no errno carries, so the answer goes out of band and the op
                 * fails with the nearest one.  Ahead of the probe, so a
                 * CLAIM_TEST never runs against an object the operation is not
                 * defined for. */
                if ((aux->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
                    !S_ISREG(aux->attr.va_mode)) {
                    map->gate_status =
                        chimera_nfs4_data_nonreg_status(aux->attr.va_mode);
                    *status = CHIMERA_VFS_EINVAL;
                }
                break;

            case OP_COMMIT:
            /* RFC 7530 §16.4 for COMMIT, RFC 7862 §15.1/15.4/15.11 for the
             * three v4.2 ops: all four apply to a regular file only. */
            case OP_ALLOCATE:
            case OP_DEALLOCATE:
            case OP_SEEK:
                if ((aux->attr.va_set_mask & CHIMERA_VFS_ATTR_MODE) &&
                    !S_ISREG(aux->attr.va_mode)) {
                    *status = chimera_vfs_nonreg_error(aux->attr.va_mode);
                }
                break;

            default:
                break;
        } /* switch */

        return;
    }
} /* nfs4_vfs_compound_gate */

/*
 * The sequence is over.  Fill the results of every NFSv4 op that ran, stopping
 * at the first that failed, and hand the request back to the reply path exactly
 * as a failing per-op handler would.
 */
static void
nfs4_vfs_compound_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct nfs4_vfs_compound_ctx         *ctx    = private_data;
    struct nfs_request                   *req    = ctx->req;
    struct chimera_server_nfs_thread     *thread = req->thread;
    const struct chimera_vfs_compound_op *vop;
    uint32_t                              completed;
    uint32_t                              k;
    int                                   j;
    nfsstat4                              status   = NFS4_OK;
    uint32_t                              fail_res = 0;
    int                                   failed   = 0;

    completed = chimera_vfs_compound_num_completed(compound);

    for (k = 0; k < ctx->num_ops && !failed; k++) {
        struct nfs4_vfs_op *map   = &ctx->ops[k];
        struct nfs_argop4  *argop = &req->args_compound->argarray[map->res_index];
        struct nfs_resop4  *resop = &req->res_compound.resarray[map->res_index];

        /* Result slots come from a bump allocator that does not zero, and some
         * result types marshal fields that sit OUTSIDE their status union --
         * SETATTR4res carries num_attrsset and an attrsset pointer.  A fill
         * that never runs, because the sequence failed in front of it, would
         * otherwise leave the marshaller dereferencing whatever was there.
         * Each per-op handler initializes its own result on entry; this is the
         * same guarantee for every op the sequence carries. */
        memset(resop, 0, sizeof(*resop));
        resop->resop = argop->argop;

        /* Everything a fill or a slot runs reads the COMPOUND's arguments and
         * writes its result through req->index -- install_state does, and so
         * does every per-op body a slot reuses unchanged.  The post-loop
         * assignment below is what the dispatcher finally sees; this is what
         * each op sees while it is being filled. */
        req->index = (int) map->res_index;

        /* The 4.1 current-stateid lifecycle, in op order -- see
         * nfs4_vfs_current_stateid_step. */
        nfs4_vfs_current_stateid_step(req, argop->argop);

        /* The same reply-buffer headroom gate the per-op dispatcher applies
         * before it runs an operation -- applied here after the whole run has
         * executed, which is only acceptable because a run that mutates is
         * admitted solely when this cannot fire (the headroom rule in
         * chimera_nfs4_compound_try_vfs).  On a run that only reads it
         * truncates the reply at the same op the dispatcher would have. */
        if (req->encoding->dbuf->size - req->encoding->dbuf->used < 8192) {
            nfs4_fail_undispatched_op(thread, argop, resop, NFS4ERR_RESOURCE);
            status   = NFS4ERR_RESOURCE;
            fail_res = map->res_index;
            failed   = 1;
            break;
        }

        for (j = map->vfs_lo; j <= map->vfs_hi; j++) {
            vop = chimera_vfs_compound_op(compound, (uint32_t) j);

            /* An op the gate spared did not run and is not meant to have: an
            * OPEN's group carries a CLAIM that a coalesce makes pointless
            * and a truncate that only a name already there earns, and the
            * gate decides both from what the OPEN reported.  Its status is
            * left UNSET, which is exactly what the check below reads as a
            * hole in the run, so it is stepped over rather than excepted. */
            if (vop && vop->skip) {
                continue;
            }

            chimera_nfs_abort_if(vop == NULL || vop->status == CHIMERA_VFS_UNSET,
                                 "NFSv4 compound: VFS op %d never ran", j);

            if (vop->status != CHIMERA_VFS_OK) {
                /* VERIFY and NVERIFY answer with NFS4ERR_NOT_SAME or
                 * NFS4ERR_SAME, which no errno encodes; the gate recorded the
                 * real answer when it failed the op. */
                if (argop->argop == OP_OPEN && j == ctx->open_claim_op) {
                    /* A refused share CLAIM.  The arbiter answered, and what
                     * it answered is the OPEN's result: BREAKING is a holder
                     * the break has been kicked for, which RFC 7530 §10.2
                     * answers NFS4ERR_DELAY, and anything else is
                     * NFS4ERR_SHARE_DENIED.  The lease owns nothing either
                     * way -- the executor put the file state -- and
                     * give_back frees it. */
                    status = chimera_nfs4_open_share_status(
                        map->share, NULL, vop->claim_result);
                    resop->opopen.status = status;
                    fail_res             = map->res_index;
                    failed               = 1;
                    break;
                }

                if (argop->argop == OP_LOCK && j == ctx->lock_claim_op) {
                    /* A refused CLAIM: the arbiter answered, and what it
                     * answered is the operation's result, not a failure of
                     * the sequence.  The apply writes the denied body from
                     * the holder the op reported and hands back the status
                     * the seqid wrapper is owed. */
                    status = chimera_nfs4_lock_apply(req, vop->claim_result,
                                                     &vop->conflict);
                    resop->oplock.status = status;
                    ctx->lock_filled     = 1;
                    fail_res             = map->res_index;
                    failed               = 1;
                    break;
                }

                status = map->gate_status ? map->gate_status :
                    nfs4_vfs_op_errno(argop, vop, req);
                resop->opillegal.status = status;

                /* A denied LOCKT still has a body: the holder that refused it.
                 * Filled here rather than in the gate because copying the
                 * owner string allocates from the reply buffer, which nothing
                 * may touch while the sequence is still running. */
                if (argop->argop == OP_LOCKT && status == NFS4ERR_DENIED) {
                    nfs4_vfs_lockt_fill_denied(req, &resop->oplockt,
                                               &vop->conflict);
                }

                fail_res = map->res_index;
                failed   = 1;
                break;
            }
        }

        if (failed) {
            break;
        }

        status = map->is_slot ?
            nfs4_vfs_slot_apply(thread, req, argop, resop) :
            nfs4_vfs_op_fill(req, compound, ctx, map, argop, resop);

        if (status != NFS4_OK) {
            fail_res = map->res_index;
            failed   = 1;
        }
    }

    /* The current filehandle the COMPOUND is left with is whatever the last op
     * that ran was addressing (a LOOKUP that failed did not move it; a
     * RESTOREFH moved it back to the saved object).  Applied after the fills,
     * not before them, because a RESTOREFH's fill re-points req->fh at the
     * saved handle as its own bookkeeping -- for a RESTOREFH in the middle of a
     * sequence that is not where the COMPOUND ends up. */
    if (completed > 0) {
        vop = chimera_vfs_compound_op(compound, completed - 1);
        if (vop && vop->fh_len) {
            memcpy(req->fh, vop->fh, vop->fh_len);
            req->fhlen = (int) vop->fh_len;
        }
    }

    /* A SECINFO took the current filehandle away.  Applied here, after the
     * sequence has said where it ended up, because what it resolved on the way
     * -- the name it looked up -- is not what the COMPOUND is left holding. */
    if (ctx->fh_consumed) {
        req->fhlen = 0;
    } else if (ctx->lock_fh_op >= 0) {
        /* A LOCK lent the run the handle its stateid names, and a lent handle
         * becomes the current object like any other -- but that object is not
         * where the COMPOUND ends up: LOCK does not change the current
         * filehandle (RFC 7530 §16.10).  Put back what the op in front of the
         * lent handle recorded, which is what was current when the LOCK ran. */
        vop = chimera_vfs_compound_op(compound, (uint32_t) ctx->lock_fh_op);

        if (vop && vop->fh_len) {
            memcpy(req->fh, vop->fh, vop->fh_len);
            req->fhlen = (int) vop->fh_len;
        }
    } else if (ctx->secinfo_lookup > 0) {
        /* A 4.0 SECINFO whose name resolved: the name is not what the
         * COMPOUND holds afterwards -- the directory it resolved the name IN
         * is (RFC 7530 §16.31.3).  That directory is whatever was current
         * immediately before the LOOKUP, which is exactly what the VFS op in
         * front of it recorded as its own fh: every op stamps the current
         * object as it finishes (op->fh, vfs_compound.h), and there is always
         * an op in front, because the seed PUTFH is op 0 and the LOOKUP is
         * never it.  Read from the run rather than from a copy taken when the
         * sequence was built, because the current object can have moved
         * between the seed and the SECINFO and a build-time copy only ever
         * knew the seed.  A LOOKUP that failed needs nothing: it did not move
         * the current object, and the rule above already left the directory.
         */
        vop = chimera_vfs_compound_op(compound,
                                      (uint32_t) ctx->secinfo_lookup);

        if (vop && vop->status == CHIMERA_VFS_OK) {
            vop = chimera_vfs_compound_op(
                compound, (uint32_t) ctx->secinfo_lookup - 1);

            if (vop && vop->fh_len) {
                memcpy(req->fh, vop->fh, vop->fh_len);
                req->fhlen = (int) vop->fh_len;
            }
        }
    }

    /* Point req->index at the operation whose status the compound carries, so
     * chimera_nfs4_compound_complete truncates (or completes) exactly as it
     * does for a per-op handler.  When nothing failed that is the last op the
     * sequence carried, which for a sequence ending in an OPEN is short of the
     * COMPOUND's end -- the dispatcher picks the remainder up from there. */
    req->index = failed ? (int) fail_res :
        (int) ctx->ops[ctx->num_ops - 1].res_index;

    /*
     * Every way out of an OPEN goes through its own completion, not the generic
     * one.  chimera_nfs4_open_finish is what advances a 4.0 open_owner's seqid
     * and drops the reference the encoder pinned on it -- and it has to run for
     * the outcomes that FAIL too, because most OPEN errors are in the advance
     * set (RFC 7530 §9.1.7).  Skipping it on the failure path leaves the owner
     * one seqid behind, and the client's next OPEN is answered NFS4ERR_BAD_SEQID
     * for a request that was perfectly good.
     */
    nfs4_vfs_compound_give_back(thread, ctx);

    /*
     * A LOCK gets the same treatment, and for the same reason: its own
     * completion is the RFC 7530 §9.1.7 seqid wrapper, which has to run for
     * the outcomes that fail too.  When the CLAIM never ran -- the sequence
     * stopped in front of it -- nothing was consumed, so the request's
     * references are given back without any advance and the generic
     * completion truncates the reply at whatever did fail.
     */
    if (ctx->lock_present) {
        struct nfs_argop4 *largop =
            &req->args_compound->argarray[ctx->lock_res_index];

        if (ctx->lock_filled) {
            chimera_vfs_compound_free(compound);
            free(ctx);

            chimera_nfs4_lock_finish(req, status);
            return;
        }

        chimera_nfs4_lock_abandon(thread, req, largop);
    }

    if (failed && ctx->open_present && fail_res == ctx->open_res_index) {
        req->index = (int) fail_res;

        chimera_vfs_compound_free(compound);
        free(ctx);

        chimera_nfs4_open_complete(req, status);
        return;
    }

    if (ctx->open_present && !ctx->open_filled && req->open_4_0_owner) {
        /* The sequence stopped before the OPEN ran at all, so there is no
         * seqid to advance -- but the encoder's pin is still outstanding. */
        nfs_open_owner_put(req->open_4_0_owner);
        req->open_4_0_owner = NULL;
    }

    if (!failed && ctx->open_filled && ctx->open_ends_run) {
        /* The OPEN's own tail, for the one shape that still has one: an OPEN
         * that could earn a delegation, whose grant can park on a CB_NULL
         * probe.  It owns the completion from here, so nothing of the
         * sequence may still be needed -- which is why the attributes it
         * takes were copied out of the compound before this.  An OPEN that
         * could not earn one settled itself in its own fill, and leaves
         * through the generic completion below like any other op. */
        struct chimera_vfs_attrs fattr        = ctx->open_attr;
        int                      ctx_has_attr = ctx->open_has_attr;
        struct OPEN4res         *ores         =
            &req->res_compound.resarray[ctx->open_res_index].opopen;

        chimera_vfs_compound_free(compound);
        free(ctx);

        if (chimera_nfs4_open_grant_delegation(req, ores,
                                               ctx_has_attr ? &fattr : NULL)) {
            return; /* parked; resumes through nfs4_cb_null_complete */
        }

        chimera_nfs4_open_complete(req, NFS4_OK);
        return;
    }

    chimera_vfs_compound_free(compound);
    free(ctx);

    chimera_nfs4_compound_complete(req, failed ? status : NFS4_OK);
} /* nfs4_vfs_compound_complete */

/* READDIR carries its verifier as an opaque; the VFS takes it as a value. */
static uint64_t
nfs4_vfs_readdir_verifier(const struct READDIR4args *args)
{
    uint64_t verifier;

    memcpy(&verifier, args->cookieverf, sizeof(verifier));

    return verifier;
} /* nfs4_vfs_readdir_verifier */

/*
 * Append one xattr op, staging its name the way the per-op handler does.
 * Returns the VFS op index, or -1 (which refuses the whole sequence) if the
 * name will not fit in the reply buffer -- the length itself was already
 * accepted by nfs4_vfs_xattr_name_ok during the scan.
 */
static int
nfs4_vfs_add_xattr_op(
    struct nfs_request          *req,
    struct chimera_vfs_compound *compound,
    const struct nfs_argop4     *argop)
{
    const xdr_opaque *wire;
    char             *name;
    int               namelen;

    switch (argop->argop) {
        case OP_GETXATTR:
            wire = &argop->opgetxattr.gxa_name;
            break;
        case OP_SETXATTR:
            wire = &argop->opsetxattr.sxa_key;
            break;
        default:
            wire = &argop->opremovexattr.rxa_name;
            break;
    } /* switch */

    if (chimera_nfs4_xattr_stage_name(req, wire->data, wire->len,
                                      &name, &namelen) != NFS4_OK) {
        return -1;
    }

    switch (argop->argop) {
        case OP_GETXATTR:
            return chimera_vfs_compound_add_getxattr(
                compound, name, namelen,
                chimera_nfs4_xattr_stage_max(req, CHIMERA_NFS4_GETXATTR_MAX));
        case OP_SETXATTR:
            return chimera_vfs_compound_add_setxattr(
                compound, argop->opsetxattr.sxa_option, name, namelen,
                argop->opsetxattr.sxa_value.data,
                argop->opsetxattr.sxa_value.len);
        default:
            return chimera_vfs_compound_add_removexattr(compound, name,
                                                        namelen);
    } /* switch */
} /* nfs4_vfs_add_xattr_op */

/*
 * The create attributes an exclusive OPEN carries.
 *
 * EXCLUSIVE4 carries none at all -- the verifier occupies the attribute slot --
 * so the object's mode is undefined until the client's follow-up SETATTR (RFC
 * 7530 §16.16.5) and it is created owner-only, the same safe default the per-op
 * path, Linux nfsd and NFS-Ganesha all use.  EXCLUSIVE4_1 does carry
 * attributes, and takes the same default when they leave the mode out.
 *
 * Either way the verifier is stamped into atime and mtime, overwriting anything
 * the client asked for there -- which is why an EXCLUSIVE4_1 that sets
 * time_access_set or time_modify_set is refused rather than silently clobbered.
 */
static void
nfs4_vfs_open_exclusive_attrs(
    const struct OPEN4args   *args,
    struct chimera_vfs_attrs *attr)
{
    const uint8_t *verf;
    uint32_t       part;

    if (args->openhow.how.mode == EXCLUSIVE4_1) {
        chimera_nfs4_unmarshall_attrs(
            attr,
            args->openhow.how.ch_createboth.cva_attrs.num_attrmask,
            args->openhow.how.ch_createboth.cva_attrs.attrmask,
            args->openhow.how.ch_createboth.cva_attrs.attr_vals.data,
            args->openhow.how.ch_createboth.cva_attrs.attr_vals.len,
            NULL, 0);
        verf = args->openhow.how.ch_createboth.cva_verf;
    } else {
        verf = args->openhow.how.createverf;
    }

    attr->va_set_mask |= CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME;

    memcpy(&part, verf, 4);
    attr->va_atime.tv_sec  = part;
    attr->va_atime.tv_nsec = 0;
    memcpy(&part, verf + 4, 4);
    attr->va_mtime.tv_sec  = part;
    attr->va_mtime.tv_nsec = 0;

    if (!(attr->va_set_mask & CHIMERA_VFS_ATTR_MODE)) {
        attr->va_set_mask |= CHIMERA_VFS_ATTR_MODE;
        attr->va_mode      = 0600;
    }
} /* nfs4_vfs_open_exclusive_attrs */

/*
 * Append an OPEN, marshalling its arguments the way the per-op path does before
 * it makes any VFS call.
 *
 * All of this is a pure function of the OPEN's own arguments -- the create mode
 * selects flags and unmarshals the create attributes, share_access selects the
 * data-access intent -- which is why it can happen here, when the sequence is
 * built, rather than from inside it.  The two things that are NOT arguments,
 * because they depend on what the name resolves to, are the options: refusing a
 * non-regular object by type, and applying the create attributes only to an
 * object this open actually creates.
 */
static int
nfs4_vfs_add_open_op(
    struct nfs_request          *req,
    struct chimera_vfs_compound *compound,
    const struct nfs_argop4     *argop,
    struct nfs4_vfs_op          *map)
{
    const struct OPEN4args  *args = &argop->opopen;
    struct chimera_vfs_attrs attr;
    unsigned int             flags   = 0;
    uint32_t                 opts    = 0;
    const char              *name    = NULL;
    int                      namelen = 0;
    uint64_t                 attr_mask;

    memset(&attr, 0, sizeof(attr));

    if (args->claim.claim == CLAIM_NULL) {
        name              = (const char *) args->claim.file.data;
        namelen           = (int) args->claim.file.len;
        map->open_by_name = 1;
    }

    if (args->openhow.opentype == OPEN4_CREATE) {
        flags |= CHIMERA_VFS_OPEN_CREATE;

        if (args->openhow.how.mode != UNCHECKED4) {
            flags |= CHIMERA_VFS_OPEN_EXCLUSIVE;
        }

        if (args->openhow.how.mode == UNCHECKED4 ||
            args->openhow.how.mode == GUARDED4) {
            /* Resolve an existing name's type rather than opening it, so a
             * socket or a directory is answered for by type.  An exclusive
             * create does NOT ask for this: it wants the plain collision, so
             * that whatever is in the way it can go and look at it.  (The
             * per-op path draws the line in the same place.) */
            flags |= CHIMERA_VFS_OPEN_CREATE_REGULAR;

            chimera_nfs4_unmarshall_attrs(&attr,
                                          args->openhow.how.createattrs.num_attrmask,
                                          args->openhow.how.createattrs.attrmask,
                                          args->openhow.how.createattrs.attr_vals.data,
                                          args->openhow.how.createattrs.attr_vals.len,
                                          NULL, 0);
        } else {
            /* EXCLUSIVE4 and EXCLUSIVE4_1 stamp the client's verifier into the
             * object's atime and mtime, which is how a repeat of the same
             * create recognises its own earlier one.  (Linux nfsd does the
             * same; a server-private xattr would be better and is a TODO on the
             * per-op path.)  A collision therefore has to be looked at rather
             * than refused, which is what EXCLUSIVE_RETRY is for. */
            opts |= CHIMERA_VFS_COMPOUND_OPEN_EXCLUSIVE_RETRY;

            nfs4_vfs_open_exclusive_attrs(args, &attr);
        }

        if (args->openhow.how.mode == UNCHECKED4) {
            /* An UNCHECKED4 create of a name that is already there opens it
             * without restyling it, except that size 0 truncates -- and the
             * truncate is deliberately not part of the open, so an OPEN that
             * fails afterwards leaves the file's contents alone. */
            opts |= CHIMERA_VFS_COMPOUND_OPEN_REGULAR_ONLY |
                CHIMERA_VFS_COMPOUND_OPEN_ATTRS_ON_CREATE_ONLY;

            map->open_trunc_if_existed =
                (attr.va_set_mask & CHIMERA_VFS_ATTR_SIZE) && attr.va_size == 0;
        }
    } else if (namelen) {
        /* A plain open must classify a non-regular object before a backend
         * tries to open it. */
        opts |= CHIMERA_VFS_COMPOUND_OPEN_REGULAR_ONLY;
    }

    /* The share access the client asked for is the data-access intent the
     * engine's open gate authorizes and stamps on the handle for every later
     * stateful READ/WRITE through this open. */
    if (args->share_access & OPEN4_SHARE_ACCESS_READ) {
        flags |= CHIMERA_VFS_OPEN_READ_ONLY;
    }
    if (args->share_access & OPEN4_SHARE_ACCESS_WRITE) {
        flags |= CHIMERA_VFS_OPEN_WRITE_ONLY;
    }

    (void) req;

    /* The same attributes the per-op path's open asks for -- no more, so an
     * object is not stat'd more thoroughly on one path than the other.  An
     * exclusive create adds the two the verifier lives in. */
    attr_mask = CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE |
        CHIMERA_VFS_ATTR_CHANGE | CHIMERA_VFS_ATTR_CTIME;

    if (opts & CHIMERA_VFS_COMPOUND_OPEN_EXCLUSIVE_RETRY) {
        attr_mask |= CHIMERA_VFS_ATTR_ATIME | CHIMERA_VFS_ATTR_MTIME;
    }

    return chimera_vfs_compound_add_open(compound, name, namelen, flags, opts,
                                         &attr, attr_mask, 0, 0);
} /* nfs4_vfs_add_open_op */

/*
 * Resolve the handle a READ or WRITE runs against, and authorize it.
 *
 * A special stateid is anonymous: there is no open to consult, so the I/O runs
 * against the current object and the deny-share reservations held by any owner
 * of any client are what authorize it.  A real one names an open (or a lock,
 * which has one), and then that open's own handle and its granted access mode
 * are what authorize it -- the OPENMODE rule.
 *
 * Any non-OK return sends the op back to the per-op path, which reaches the
 * same answer.  That includes the cases this path declines rather than fails:
 * a delegation stateid, which authorizes the I/O but carries no handle and
 * changes which lease the I/O is attributed to.
 */
static nfsstat4
nfs4_vfs_io_authorize(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    const struct stateid4            *sid,
    uint32_t                          share_access,
    const uint8_t                    *fh,
    int                               fhlen,
    struct chimera_vfs_open_handle  **out_handle,
    struct chimera_claim_actor       *out_owner,
    int                              *have_owner)
{
    struct nfs_state_table         *table = &thread->shared->nfs4_state_table;
    struct nfs_open_state          *open_state;
    struct nfs_lock_state          *lock_state;
    struct chimera_vfs_open_handle *state_handle;
    uint32_t                        current_seqid;
    void                           *state_void;
    uint8_t                         state_type;
    nfsstat4                        status;

    *out_handle = NULL;
    *have_owner = 0;

    if (nfs4_stateid_is_special(sid)) {
        return nfs4_clients_check_io_denied(&thread->shared->nfs4_shared_clients,
                                            fh, fhlen, share_access);
    }

    status = nfs_state_table_acquire(table, sid, 0, &state_void, &state_type);

    if (status != NFS4_OK) {
        return status;
    }

    status = nfs_state_check_client(state_void, state_type,
                                    req->session ?
                                    req->session->client_unified : NULL);

    if (status != NFS4_OK) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        return status;
    }

    if (state_type == NFS4_SLOT_TYPE_DELEG) {
        /* Authorizes the I/O but carries no handle, and the per-op path
         * attributes the on-the-fly I/O to the delegation holder so it does not
         * recall the client's own delegation.  Leave it there. */
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        return NFS4ERR_NOTSUPP;
    }

    if (state_type == NFS4_SLOT_TYPE_OPEN) {
        open_state    = state_void;
        state_handle  = open_state->handle;
        current_seqid = open_state->seqid;
    } else {
        lock_state    = state_void;
        open_state    = lock_state->open_state;
        state_handle  = lock_state->handle;
        current_seqid = lock_state->seqid;
    }

    if (req->minorversion == 0) {
        status = nfs4_stateid_check_seqid(current_seqid, sid->seqid);

        if (status != NFS4_OK) {
            nfs_state_table_release(table, state_void, state_type,
                                    thread->vfs_thread);
            return status;
        }
    }

    /* RFC 7530 §9.1.4 / RFC 8881 §9.1.2: I/O through an open (or lock) stateid
     * is limited to the associated open's granted access mode. */
    if ((open_state->share_access & share_access) == 0) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        return NFS4ERR_OPENMODE;
    }

    status = nfs_open_state_check_io_denied(open_state, share_access);

    if (status != NFS4_OK) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        return status;
    }

    if (!nfs_open_state_check_principal(open_state,
                                        req->principal_flavor,
                                        req->principal_machinename,
                                        req->principal_machinename_len)) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        return NFS4ERR_ACCESS;
    }

    if (!state_handle) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        return NFS4ERR_NOTSUPP;
    }

    /* Whose I/O this is.  Without it the claim layer arbitrates the client's
     * own I/O against the client's own share reservation -- denying it, and
     * recalling the very delegation it is being done under. */
    memset(out_owner, 0, sizeof(*out_owner));
    out_owner->owner.proto      = CHIMERA_CLAIM_PROTO_NFSV4;
    out_owner->owner.client_key = open_state->owner->client->client_id;
    out_owner->owner.owner_lo   = state_handle->fh_hash;
    out_owner->owner.owner_hi   = 0;
    *have_owner                 = 1;

    /* A reference of our own, so the handle outlives the state slot. */
    chimera_vfs_dup_handle(thread->vfs_thread, state_handle);
    *out_handle = state_handle;

    nfs_state_table_release(table, state_void, state_type, thread->vfs_thread);

    return NFS4_OK;
} /* nfs4_vfs_io_authorize */

/*
 * Authorize a size-changing SETATTR, and resolve the handle it applies
 * through.
 *
 * This is the per-op path's own rule, at the one moment this path can apply it:
 * a size change is a write (RFC 7530 §9.1.4.3), so through a special stateid it
 * must honour deny-WRITE reservations held by any owner of any client, and
 * through a real one the stateid must name an open OF THIS OBJECT that was
 * opened for writing.  The open's own handle then carries that grant, the way a
 * descriptor carries ftruncate(2)'s -- which a fresh open by name would not.
 *
 * Returns NFS4_OK with *out_handle either a reference the caller must release,
 * or NULL meaning "apply against the current object".
 */
static nfsstat4
nfs4_vfs_setattr_authorize(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req,
    const struct SETATTR4args        *args,
    const uint8_t                    *fh,
    int                               fhlen,
    struct chimera_vfs_open_handle  **out_handle)
{
    struct nfs_state_table *table = &thread->shared->nfs4_state_table;
    struct nfs_open_state  *open_state;
    void                   *state_void;
    uint8_t                 state_type;
    nfsstat4                status;
    bool                    has_write;

    *out_handle = NULL;

    if (nfs4_stateid_is_special(&args->stateid)) {
        return nfs4_clients_check_io_denied(&thread->shared->nfs4_shared_clients,
                                            fh, fhlen,
                                            OPEN4_SHARE_ACCESS_WRITE);
    }

    status = nfs_state_table_acquire(table, &args->stateid,
                                     NFS4_SLOT_TYPE_OPEN,
                                     &state_void, &state_type);

    if (status != NFS4_OK) {
        return status;
    }

    status = nfs_state_check_client(state_void, state_type,
                                    req->session ?
                                    req->session->client_unified : NULL);

    if (status != NFS4_OK) {
        nfs_state_table_release(table, state_void, state_type,
                                thread->vfs_thread);
        return status;
    }

    open_state = state_void;

    /* RFC 7530 §9.1.4.3: the stateid must name an open of the object that is
     * the current filehandle, not some other open file. */
    if (open_state->fh_len != (uint32_t) fhlen ||
        memcmp(open_state->fh, fh, (size_t) fhlen) != 0) {
        nfs_state_table_release(table, open_state, NFS4_SLOT_TYPE_OPEN,
                                thread->vfs_thread);
        return NFS4ERR_BAD_STATEID;
    }

    has_write = (open_state->share_access & OPEN4_SHARE_ACCESS_WRITE) != 0;

    if (has_write && open_state->handle) {
        /* A reference of our own, so the handle outlives the state slot --
         * which the per-op path takes for the same reason. */
        chimera_vfs_dup_handle(thread->vfs_thread, open_state->handle);
        *out_handle = open_state->handle;
    }

    nfs_state_table_release(table, open_state, NFS4_SLOT_TYPE_OPEN,
                            thread->vfs_thread);

    return has_write ? NFS4_OK : NFS4ERR_OPENMODE;
} /* nfs4_vfs_setattr_authorize */

/*
 * Append a CREATE, translating the object type NFSv4 names into the three
 * shapes the VFS makes.  A device's numbers and a special file's type travel in
 * the attributes, which is where mknod wants them anyway, so the translation is
 * all here and there is none on the other side.
 */
static int
nfs4_vfs_add_create_op(
    struct chimera_vfs_compound *compound,
    const struct nfs_argop4     *argop)
{
    const struct CREATE4args *args = &argop->opcreate;
    struct chimera_vfs_attrs  attr;
    uint8_t                   type;
    const char               *target    = NULL;
    int                       targetlen = 0;

    memset(&attr, 0, sizeof(attr));

    chimera_nfs4_unmarshall_attrs(&attr,
                                  args->createattrs.num_attrmask,
                                  args->createattrs.attrmask,
                                  args->createattrs.attr_vals.data,
                                  args->createattrs.attr_vals.len,
                                  NULL, 0);

    switch (args->objtype.type) {
        case NF4DIR:
            type = CHIMERA_VFS_COMPOUND_CREATE_DIR;
            break;
        case NF4LNK:
            type      = CHIMERA_VFS_COMPOUND_CREATE_SYMLINK;
            target    = (const char *) args->objtype.linkdata.data;
            targetlen = (int) args->objtype.linkdata.len;
            break;
        default:
            /* NF4BLK, NF4CHR, NF4SOCK or NF4FIFO -- the scan admits no other
             * type here. */
            type              = CHIMERA_VFS_COMPOUND_CREATE_NODE;
            attr.va_set_mask |= CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_RDEV;
            attr.va_rdev      = 0;

            switch (args->objtype.type) {
                case NF4BLK:
                    attr.va_mode = (attr.va_mode & ~S_IFMT) | S_IFBLK;
                    attr.va_rdev =
                        ((uint64_t) args->objtype.devdata.specdata1 << 32) |
                        (uint64_t) args->objtype.devdata.specdata2;
                    break;
                case NF4CHR:
                    attr.va_mode = (attr.va_mode & ~S_IFMT) | S_IFCHR;
                    attr.va_rdev =
                        ((uint64_t) args->objtype.devdata.specdata1 << 32) |
                        (uint64_t) args->objtype.devdata.specdata2;
                    break;
                case NF4SOCK:
                    attr.va_mode = (attr.va_mode & ~S_IFMT) | S_IFSOCK;
                    break;
                default: /* NF4FIFO */
                    attr.va_mode = (attr.va_mode & ~S_IFMT) | S_IFIFO;
                    break;
            } /* switch */
            break;
    } /* switch */

    return chimera_vfs_compound_add_create(compound, type,
                                           (const char *) args->objname.data,
                                           (int) args->objname.len,
                                           target, targetlen,
                                           &attr,
                                           CHIMERA_VFS_ATTR_FH, 0, 0);
} /* nfs4_vfs_add_create_op */

int
chimera_nfs4_compound_try_vfs(
    struct chimera_server_nfs_thread *thread,
    struct nfs_request               *req)
{
    struct chimera_vfs_compound    *compound;
    struct nfs4_vfs_compound_ctx   *ctx;
    struct nfs_argop4              *argop;
    uint32_t                        first, num, nenc, i, k;
    uint8_t                         cur_fh[NFS4_FHSIZE];
    int                             cur_fhlen = 0;
    int                             lead_putfh, have_lookup = 0, have_getattr = 0;
    int                             have_lookupp = 0, have_saved = 0;
    int                             cur_moved = 0, stages_early = 0;
    /* Whether any op the run carries changes the filesystem -- see
     * nfs4_vfs_op_mutates and the headroom rule below. */
    int                             mutates = 0;
    /* What the sequence's current open handle carries, as it is built.  Zero
     * means nothing is open on the current object -- see nfs4_vfs_open_for. */
    unsigned int                    cur_open_flags = 0;
    int                             may_fail_late  = 0;
    /* Set once the run carries a zero-VFS-op slot: what follows it is bounded
     * by nfs4_vfs_op_reads_state as well as by may_fail_late. */
    int                             state_slot = 0;
    /* Index of the OPEN this sequence carries, or -1.  At most one: an OPEN is
     * always the last op of its run. */
    int                             open_at = -1;
    /* Index of the LOCK this sequence carries, or -1.  At most one: a LOCK is
     * always the last op of its run. */
    int                             lock_at       = -1;
    int                             lock_prepared = 0;
    /* Whether that OPEN is the last op the run carries -- see
     * nfs4_vfs_open_may_delegate. */
    int                             open_ends_run = 0;
    /* Index of a SETATTR whose size change has to be authorized before the
     * sequence runs, or -1. */
    int                             setattr_at = -1;
    /* The export the sequence runs under, established by the op that seeds the
     * current object.  A later PUTFH is admitted only back into this one -- see
     * the PUTFH case.  Read during the scan, which is before the seed handle is
     * decoded, so it cannot come from req->export_id. */
    uint16_t                        seq_export = req->export_id;
    /* Set when the scan meets an op the sequence cannot carry: the run ends in
     * front of it, and the dispatcher picks up from there. */
    int                             stop = 0;
    /* The seed PUTFH the sequence always opens with. */
    uint32_t                        vfs_ops = 1;
    uint64_t                        reply_bound = 0, avail;
    int                             idx, next;
    int                             open_4_0_pinned = 0;
    struct chimera_vfs_open_handle *setattr_handle  = NULL;

    first = (uint32_t) req->index;
    num   = req->res_compound.num_resarray;
    /* One past the last op the sequence will carry.  Normally the whole
     * remainder; an op that ends the run (see nfs4_vfs_op_ends_run) pulls it
     * in, and what is left is dispatched op by op afterwards. */
    nenc = num;

    if (first >= num) {
        return 0;
    }

    /* A remainder longer than the sequence can hold is not refused: the scan's
     * own vfs_ops budget ends the run when it fills up, and the rest is
     * dispatched op by op. */

    /* The dispatcher fails an op with NFS4ERR_RESOURCE rather than running it
     * when the reply buffer is nearly full; leave that to it. */
    avail = req->encoding->dbuf->size - req->encoding->dbuf->used;

    if (avail < 8192) {
        return 0;
    }

    /* When the remainder opens with a PUTFH, that handle names the export;
    * otherwise the sequence inherits whatever the COMPOUND already had. */
    if (req->args_compound->argarray[first].argop == OP_PUTFH) {
        uint8_t seed_fh[CHIMERA_VFS_FH_SIZE];
        int     seed_len;

        if (chimera_nfs_fh_unwrap(
                req->args_compound->argarray[first].opputfh.object.data,
                (int) req->args_compound->argarray[first].opputfh.object.len,
                &seq_export, seed_fh, &seed_len,
                thread->shared->fh_key,
                thread->shared->fh_sign) != CHIMERA_NFS_FH_OK) {
            return 0;
        }
    }

    for (i = first; i < num; i++) {
        argop = &req->args_compound->argarray[i];

        if (!nfs4_vfs_op_encodable(argop->argop)) {
            nenc = i;
            break;
        }

        /* The per-op gates decide statuses this path has no vocabulary for, so
         * anything they would reject goes back to the per-op path to be
         * rejected there. */
        if (nfs4_op_check_minor(argop->argop, req->minorversion, i,
                                req->seen_sequence) != NFS4_OK) {
            nenc = i;
            break;
        }

        if (nfs4_rofs_gate(req, argop) != NFS4_OK) {
            nenc = i;
            break;
        }

        /* A slot changes NFSv4 state only once every VFS op in the run has
         * executed, so an op whose own VFS work was decided against that
         * state -- authorized from a stateid when the sequence was built, or
         * reading live claim state while it ran -- must not sit behind one.
         * The run ends at the slot and the per-op path takes over, which is
         * where the two orderings agree again. */
        if (state_slot && nfs4_vfs_op_reads_state(argop->argop)) {
            nenc = i;
            stop = 1;
            break;
        }

        /* An upper bound on the VFS ops one NFSv4 op encodes to.  Counted up
         * front: a sequence discovered to be too long only once it was half
         * built would have to be abandoned after staging xattr names into the
         * reply buffer, which cannot be taken back.
         *
         * The encoder opens the objects it addresses, so most ops now cost an
         * OPEN as well as themselves, and the ops that take a type gate before
         * touching data cost two opens -- a metadata one for the stat and a
         * data one for the operation, which are different handles from
         * different caches.  Over-counting only declines to sequence a long
         * compound, which then takes the op-at-a-time path and is correct;
         * under-counting would fail the build half way. */
        switch (argop->argop) {
            case OP_COMMIT:
            case OP_ALLOCATE:
            case OP_DEALLOCATE:
            case OP_SEEK:
            case OP_WRITE_SAME:
                /* open(meta) + getattr + open(data) + the op */
                vfs_ops += 4;
                break;
            case OP_READLINK:
            /* open + getattr + readlink */
            case OP_LOCKT:
                /* open + getattr (the type gate) + the claim probe */
                vfs_ops += 3;
                break;
            case OP_LOCK:
                /* the lent handle + the claim; no open, the handle is the
                 * client's and the sequence borrows it */
                vfs_ops += 2;
                break;
            case OP_PUTFH:
                /* The seed PUTFH below is the leading one's own op. */
                vfs_ops += (i == first) ? 2 : 3;
                break;
            case OP_GETFH:
            case OP_SAVEFH:
            case OP_RESTOREFH:
            case OP_RENAME:
            case OP_LINK:
                /* Cursor work, or two objects named by file handle: no open. */
                vfs_ops += 1;
                break;
            case OP_CLOSE:
            case OP_LOCKU:
            case OP_OPEN_DOWNGRADE:
            case OP_DELEGRETURN:
                /* A slot: nothing at all -- see nfs4_vfs_op_is_slot. */
                break;
            default:
                /* open + the op */
                vfs_ops += 2;
                break;
        } /* switch */

        if (vfs_ops > NFS4_VFS_COMPOUND_MAX_OPS) {
            nenc = i;
            break;
        }

        /* Ops that can still fail once the whole sequence has run, which
         * nothing that mutates may follow -- see the MUTATION note at the top.
         *
         * LOCKT alone, now.  Every other NFSv4-side check runs in the gate as
         * its op finishes, so it stops what is behind it instead of reporting
         * after the fact.  READDIR was the other one, because its page was
         * judged while being marshalled and that happened after the sequence;
         * the page is marshalled inside the enumeration now, so its judgement
         * moved into the gate too.  LOCKT's cannot follow: what it reports is
         * live claim state read at fill time, and there is no earlier moment
         * at which that answer exists. */

        switch (argop->argop) {
            case OP_PUTFH:
                /* A later PUTFH is allowed only back into the SAME export.
                 *
                 * What a second one threatens is the credential: a different
                 * export has a different squash policy, and the sequence runs
                 * under one credential fixed when it was submitted.  Into the
                 * same export there is no such change -- same policy, same
                 * credential, same security flavor, all of them already
                 * established by the first.
                 *
                 * That is not a narrow escape hatch.  RENAME and LINK both
                 * require their two objects to be in the same filesystem
                 * (NFS4ERR_XDEV otherwise), so same-export is the only shape
                 * either of them is ever legally given -- and they are what a
                 * second PUTFH is nearly always for. */
                if (i != first) {
                    uint8_t  later_fh[CHIMERA_VFS_FH_SIZE];
                    int      later_len;
                    uint16_t later_export;

                    if (fh_is_nfs4_root(argop->opputfh.object.data,
                                        argop->opputfh.object.len) ||
                        argop->opputfh.object.len > NFS4_FHSIZE ||
                        chimera_nfs_fh_unwrap(argop->opputfh.object.data,
                                              (int) argop->opputfh.object.len,
                                              &later_export,
                                              later_fh, &later_len,
                                              thread->shared->fh_key,
                                              thread->shared->fh_sign) !=
                        CHIMERA_NFS_FH_OK ||
                        later_export != seq_export ||
                        chimera_nfs4_fh_is_attrdir(later_fh, later_len) ||
                        !chimera_vfs_fh_is_plausible(thread->vfs_thread,
                                                     later_fh, later_len)) {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }
                break;

            case OP_LOOKUP:
                if (chimera_nfs4_validate_name(&argop->oplookup.objname) !=
                    NFS4_OK) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }
                have_lookup = 1;
                cur_moved   = 1;
                break;

            case OP_LOOKUPP:
                /* An export root and the pseudo-root have parents the VFS
                 * cannot name -- the namespace root, or the "/" export's real
                 * root -- so a LOOKUPP is encodable only from an object we can
                 * test here, which means the one the sequence starts from.
                 * Once a LOOKUP (or a RESTOREFH) has moved the current object,
                 * what a later LOOKUPP would climb from is not known until the
                 * sequence has already run past it. */
                if (cur_moved) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }
                have_lookupp = 1;
                cur_moved    = 1;
                break;

            case OP_SAVEFH:
                have_saved = 1;
                break;

            case OP_RESTOREFH:
                /* Only a slot this sequence filled.  A handle saved before the
                 * sequence may belong to a different export, and restoring it
                 * re-derives the squash -- changing the credential the rest of
                 * the sequence would have to run under, which was fixed when it
                 * was submitted. */
                if (!have_saved) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }
                cur_moved = 1;
                break;

            case OP_READDIR:
                /* The per-op path answers each of these itself, with statuses
                 * (TOOSMALL, BAD_COOKIE, an invalid attribute request) that
                 * belong to it rather than to any VFS result. */
                if (argop->opreaddir.maxcount < 16 ||
                    argop->opreaddir.cookie == 1 ||
                    argop->opreaddir.cookie == 2) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }

                if (chimera_nfs4_validate_getattr_request(
                        argop->opreaddir.num_attr_request,
                        argop->opreaddir.attr_request) != NFS4_OK) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }

                /* Per-entry ACLs need no exception: the page is marshalled
                 * inside the enumeration, while the backend still owns the
                 * ACL it is handing over -- the same moment the per-op path
                 * encodes it. */
                break;

            case OP_GETXATTR:
                if (!nfs4_vfs_xattr_name_ok(argop->opgetxattr.gxa_name.len)) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }
                break;

            case OP_SETXATTR:
                if (may_fail_late ||
                    !nfs4_vfs_xattr_name_ok(argop->opsetxattr.sxa_key.len)) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }

                /* RFC 8276 §8.3: only the three defined option values are
                 * valid, and the per-op path is what says INVAL. */
                if (argop->opsetxattr.sxa_option != SETXATTR4_EITHER &&
                    argop->opsetxattr.sxa_option != SETXATTR4_CREATE &&
                    argop->opsetxattr.sxa_option != SETXATTR4_REPLACE) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }
                break;

            case OP_REMOVEXATTR:
                if (may_fail_late ||
                    !nfs4_vfs_xattr_name_ok(argop->opremovexattr.rxa_name.len)) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }
                break;

            case OP_GETATTR:
                if (chimera_nfs4_validate_getattr_request(
                        argop->opgetattr.num_attr_request,
                        argop->opgetattr.attr_request) != NFS4_OK) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }

                /* A backend owns the ACL it reports only for the duration of
                 * its own completion, so the copy the sequence keeps has a
                 * dangling va_acl by the time results are filled.  An ACL
                 * request must be answered by the per-op path, which marshals
                 * it while it is still live. */
                if (argop->opgetattr.num_attr_request >= 1 &&
                    (argop->opgetattr.attr_request[0] & (1U << FATTR4_ACL))) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }
                have_getattr = 1;
                break;

            case OP_CREATE:
            {
                const struct CREATE4args *ca = &argop->opcreate;

                /* CREATE mutates, so the rule that applies to SETXATTR applies
                 * to it: nothing whose NFSv4-side check fails after the
                 * sequence has run may precede it. */
                if (may_fail_late) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* Only the types a CREATE actually makes, named explicitly:
                 * everything else is a status the per-op path decides and this
                 * path has no vocabulary for -- NFS4ERR_BADTYPE for a regular
                 * file (that is what OPEN is for), NOTSUPP for the synthetic
                 * named-attribute objects.  An allow-list, because guessing
                 * what an unlisted type meant is how NF4REG became a FIFO. */
                switch (ca->objtype.type) {
                    case NF4DIR:
                    case NF4BLK:
                    case NF4CHR:
                    case NF4SOCK:
                    case NF4FIFO:
                        break;
                    case NF4LNK:
                        /* An empty target is NFS4ERR_INVAL, decided before any
                         * VFS call. */
                        if (ca->objtype.linkdata.len == 0) {
                            nenc = i;
                            stop = 1;
                        }
                        break;
                    default:
                        nenc = i;
                        stop = 1;
                        break;
                } /* switch */

                if (stop) {
                    break;
                }

                if (chimera_nfs4_validate_name(&ca->objname) != NFS4_OK ||
                    chimera_nfs4_validate_createattrs(
                        ca->createattrs.num_attrmask,
                        ca->createattrs.attrmask) != NFS4_OK) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* The sequence carries no ACL: a backend owns the one it
                 * reports only while its own completion runs. */
                if (ca->createattrs.num_attrmask >= 1 &&
                    (ca->createattrs.attrmask[0] & (1U << FATTR4_ACL))) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                cur_moved = 1;
                break;
            }

            case OP_SECINFO:
                if (chimera_nfs4_validate_name(&argop->opsecinfo.name) !=
                    NFS4_OK) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* At a "/" export's root a name matching a sibling export is a
                 * junction, and SECINFO answers with THAT export's flavors --
                 * a name the VFS cannot resolve and a policy it does not hold.
                 */
                if (thread->shared->root_export_id != 0) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* Everything after a SECINFO is dispatched op by op, against
                 * the filehandle it took away. */
                nenc = i + 1;
                break;

            case OP_LOCKT:
            {
                const struct LOCKT4args *la = &argop->oplockt;

                /* Statuses the per-op path decides before it touches the VFS:
                 * the grace window, and the same length rules as LOCK. */
                if (nfs_recovery_io_check(&thread->shared->nfs4_recovery) !=
                    NFS4_OK) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                if (la->length == 0 ||
                    (la->length != UINT64_MAX &&
                     la->offset > UINT64_MAX - la->length)) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* RFC 7530 §9.1.4: the lock-owner names a clientid, and one
                 * the server has no record of is NFS4ERR_STALE_CLIENTID.
                 * 4.1+ identifies the client through the session instead.
                 * Asked here rather than skipped: the per-op path asks it,
                 * and a run that carried the LOCKT without it answered a
                 * probe for a client that does not exist. */
                if (req->minorversion == 0) {
                    struct nfs4_session *s = nfs4_session_find_by_clientid(
                        &thread->shared->nfs4_shared_clients,
                        la->owner.clientid);

                    if (!s) {
                        nenc = i;
                        stop = 1;
                        break;
                    }

                    /* RFC 7530 §9.5: LOCKT is a clientid-bearing operation
                     * and renews all of the client's leases. */
                    nfs_client_touch(s->client_unified);
                    nfs4_session_put(s);
                }

                /* The probe is an op of the run now (a CLAIM_TEST), so its
                 * answer arrives in order with everything else and a denial
                 * stops the sequence where it stands -- which is why nothing
                 * behind a LOCKT has to be refused any more. */
                break;
            }

            case OP_LOCK:

                /* A run cannot BEGIN with a LOCK, for the reason a run cannot
                 * begin with a slot: the seed PUTFH's own failure has to land
                 * on the op at the front, and what a LOCK contributes is a
                 * lent handle and a claim, neither of which is where the seed
                 * was resolved.  It buys nothing either -- a LOCK is the last
                 * op of its run, so a run led by one carries only it. */
                if (i == first) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* The existing-lock-stateid rule (RFC 7530 §16.10.5) asks
                 * whether this lock-owner already has a stateid ON THIS FILE,
                 * and reads the COMPOUND's current filehandle to say which
                 * file that is.  The prepare below runs before the sequence
                 * does, so the only current object it can be told about is the
                 * one the sequence starts from. */
                if (cur_moved) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* The grace window is the one answer LOCK owes before it has
                 * looked anything up, and nfs_recovery_open_check is where the
                 * reclaim half of reboot recovery lives -- statuses that
                 * belong to the operation and not to any claim result. */
                if (nfs_recovery_open_check(
                        &thread->shared->nfs4_recovery,
                        req->session ? req->session->client_unified : NULL,
                        argop->oplock.reclaim != 0) != NFS4_OK) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* Everything else the operation settles is state the server
                 * already holds, and it is settled when the sequence is built
                 * -- see the nfs4_vfs_lock_prepare call below, which is what
                 * decides whether the LOCK is really carried.  The run ends
                 * here whatever it decides (nfs4_vfs_op_ends_run). */
                lock_at = (int) i;
                nenc    = i + 1;
                break;

            case OP_CLOSE:
            case OP_LOCKU:
            case OP_OPEN_DOWNGRADE:
            case OP_DELEGRETURN:

                /* A run cannot BEGIN with a slot.  The sequence always seeds
                 * the current object with a PUTFH of its own, and every map
                 * covers the VFS ops from where the last one ended -- so the
                 * op at `first` is what the seed's own failure lands on.  A
                 * slot has no VFS ops to cover it with, and a slot is the one
                 * op that does not care whether the handle is good (CLOSE
                 * resolves a stateid, not a name), so a run led by one would
                 * answer NFS4ERR_STALE where the per-op path answers OK.  It
                 * also buys nothing: what a slot is for is not ending the run
                 * that came before it. */
                if (i == first) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* Everything it can answer -- a replay, a bad
                 * seqid, a stale stateid, an inexpressible downgrade -- is
                 * decided when it is applied, which is after every VFS op in
                 * the run has executed.  So nothing that mutates may follow
                 * it (its failure would truncate a reply whose change had
                 * already been made) and nothing whose own VFS work was
                 * settled against the state it changes may follow it either.
                 *
                 * Nothing bounds a slot from IN FRONT.  Two slots in a row
                 * are applied in order, so even a 4.0 seqid chain across them
                 * (OPEN_DOWNGRADE n; CLOSE n+1 on one open_owner) classifies
                 * against the seqid the one before it advanced -- which is
                 * exactly what the op-at-a-time path does. */
                may_fail_late = 1;
                state_slot    = 1;
                break;

            case OP_ALLOCATE:
            case OP_DEALLOCATE:
            case OP_SEEK:
            case OP_WRITE_SAME:
            {
                const struct stateid4 *sid =
                    (argop->argop == OP_ALLOCATE) ? &argop->opallocate.aa_stateid :
                    (argop->argop == OP_DEALLOCATE) ? &argop->opdeallocate.da_stateid :
                    (argop->argop == OP_WRITE_SAME) ? &argop->opwrite_same.wsa_stateid :
                    &argop->opseek.sa_stateid;

                /* The per-op path answers each of these itself, with a status
                 * that belongs to the operation rather than to any VFS
                 * result. */
                if (argop->argop == OP_WRITE_SAME) {
                    const struct app_data_block4 *adb =
                        &argop->opwrite_same.wsa_adb;

                    /* Per-block-number stamping is the unsupported arm of the
                     * union, the ADB geometry has to be sane, and the total
                     * must not overflow -- three answers the operation owes
                     * that no VFS result carries. */
                    if (adb->adb_reloff_blocknum != NFS4_UINT64_MAX ||
                        adb->adb_block_size == 0 ||
                        adb->adb_block_size > UINT32_MAX ||
                        adb->adb_reloff_pattern > adb->adb_block_size ||
                        adb->adb_reloff_pattern + adb->adb_pattern.len >
                        adb->adb_block_size ||
                        (adb->adb_block_count != 0 &&
                         adb->adb_block_size >
                         NFS4_UINT64_MAX / adb->adb_block_count)) {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }

                if (argop->argop == OP_SEEK &&
                    argop->opseek.sa_what != NFS4_CONTENT_DATA &&
                    argop->opseek.sa_what != NFS4_CONTENT_HOLE) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* RFC 7862: the range is [offset, offset + length); one whose
                 * end does not fit in a uint64 names no such interval. */
                if (argop->argop == OP_ALLOCATE &&
                    argop->opallocate.aa_length &&
                    argop->opallocate.aa_offset >
                    UINT64_MAX - argop->opallocate.aa_length) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                if (argop->argop == OP_DEALLOCATE &&
                    argop->opdeallocate.da_length &&
                    argop->opdeallocate.da_offset >
                    UINT64_MAX - argop->opdeallocate.da_length) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* The same two stateid rules READ and WRITE have, and for the
                 * same reasons: a current stateid needs an io_owner the
                 * sequence cannot derive (see the SETATTR case), and a special
                 * stateid is authorized against the object the sequence starts
                 * from. */
                if (chimera_nfs4_stateid_is_current(sid)) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                if (nfs4_stateid_is_special(sid) && cur_moved) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                break;
            }

            case OP_READ:
            case OP_WRITE:
            {
                const struct stateid4 *sid = (argop->argop == OP_READ) ?
                    &argop->opread.stateid : &argop->opwrite.stateid;

                if (argop->argop == OP_WRITE && may_fail_late) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* See the SETATTR case: substituting the current stateid
                 * needs an io_owner the sequence cannot derive. */
                if (chimera_nfs4_stateid_is_current(sid)) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* An anonymous I/O is authorized against the object it
                 * addresses, and that authorization is settled before the
                 * sequence is submitted -- so the object has to be the one the
                 * sequence starts from. */
                if (nfs4_stateid_is_special(sid) && cur_moved) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* A data server serves I/O by file handle without a state
                 * table, on the metadata server's authority; that is a
                 * different authorization model, not this one. */
                if (chimera_server_config_get_nfs_data_server(
                        thread->shared->config)) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                break;
            }

            case OP_SETATTR:
            {
                const struct SETATTR4args *sa = &argop->opsetattr;
                int                        wants_size;

                if (may_fail_late) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                if (chimera_nfs4_validate_createattrs(
                        sa->obj_attributes.num_attrmask,
                        sa->obj_attributes.attrmask) != NFS4_OK) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* The sequence carries no ACL. */
                if (sa->obj_attributes.num_attrmask >= 1 &&
                    (sa->obj_attributes.attrmask[0] & (1U << FATTR4_ACL))) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* The "current stateid" (RFC 8881 §16.2.3.1.2) means whatever
                 * the op before this one left.
                 *
                 * An OPEN in the same run produces one -- and now sits inside
                 * the run rather than ending it, so "PUTFH; OPEN; WRITE(current)"
                 * is a shape this path could be asked to carry.  It still
                 * cannot.  The object is reachable: the op addresses the
                 * handle the OPEN produced, which is what
                 * chimera_vfs_compound_op_use_handle names and what the
                 * truncate behind an OPEN already does.  What is NOT reachable
                 * is who the I/O belongs to: an operation under an open
                 * stateid is attributed to (client, that handle's fh_hash)
                 * (nfs4_vfs_io_authorize), and the handle does not exist when
                 * the sequence is built.  The executor takes io_owner as a
                 * value the caller supplies (vfs_compound.c:2414, :2459) and
                 * never derives owner_lo from a handle_from target, so there
                 * is nothing to fill it with -- and a stateid operation that
                 * arbitrates against its own client's reservation is denied,
                 * and recalls the delegation it is being done under.
                 *
                 * So these stay refused, whole rather than half: the op-at-a-
                 * time path runs them after the OPEN's fill has set the
                 * current stateid, and reaches the right answer. */
                if (chimera_nfs4_stateid_is_current(&sa->stateid)) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                wants_size = sa->obj_attributes.num_attrmask >= 1 &&
                    (sa->obj_attributes.attrmask[0] & (1U << FATTR4_SIZE));

                if (wants_size) {
                    /* A size change is a write, and everything that makes it
                     * one is decided against the object the SETATTR addresses:
                     * the grace window, deny-WRITE reservations held by any
                     * client, and -- through a real stateid -- that the stateid
                     * names THIS object and was opened for writing.  All of
                     * that is settled before the sequence is submitted, so the
                     * object has to be the one the sequence starts from. */
                    if (cur_moved || setattr_at >= 0) {
                        nenc = i;
                        stop = 1;
                        break;
                    }

                    if (nfs_recovery_io_check(&thread->shared->nfs4_recovery) !=
                        NFS4_OK) {
                        nenc = i;
                        stop = 1;
                        break;
                    }

                    /* A writable layout held anywhere has to be recalled and
                     * flushed before the truncate; the sequence has no way to
                     * wait for that. */
                    if (chimera_vfs_pnfs_feature_enabled(thread->shared->vfs)) {
                        nenc = i;
                        stop = 1;
                        break;
                    }

                    setattr_at = (int) i;
                }
                break;
            }

            case OP_REMOVE:
                if (may_fail_late ||
                    chimera_nfs4_validate_name(&argop->opremove.target) !=
                    NFS4_OK) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* With either in play the per-op path looks the victim up
                 * before unlinking it -- to recall a delegation on it (RFC 7530
                 * §10.4.4), or to learn the data-server backing it has to
                 * delete afterwards.  Both are decisions about the object being
                 * removed that the sequence has no way to reach.
                 *
                 * The delegation half is now ALMOST expressible: a RECALL op
                 * with CHIMERA_VFS_COMPOUND_RECALL_NOWAIT is exactly the
                 * chimera_vfs_claim_break_caching the per-op path calls, and
                 * reports the same boolean.  What is missing is a way to reach
                 * the victim without moving the cursor off the directory the
                 * REMOVE then needs: resolving it takes a LOOKUP, a LOOKUP
                 * makes the victim current, and a REMOVE resolves its name in
                 * whatever is current.  The clean shape is for the recall to
                 * happen inside remove_at, driven by the lease-recall bit
                 * remove_flags already documents (vfs_compound.h, the REMOVE
                 * op and its adder) and which vfs_proc_remove.c does not
                 * implement -- a VFS-core change, not one this file can make.
                 * Until then both halves keep the whole REMOVE out. */
                if (chimera_server_config_get_nfs4_delegations(
                        thread->shared->config) ||
                    chimera_vfs_pnfs_enabled(thread->shared->vfs)) {
                    nenc = i;
                    stop = 1;
                    break;
                }
                break;

            case OP_RENAME:
                if (may_fail_late ||
                    chimera_nfs4_validate_name(&argop->oprename.oldname) !=
                    NFS4_OK ||
                    chimera_nfs4_validate_name(&argop->oprename.newname) !=
                    NFS4_OK) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* The source directory is the saved filehandle, and only a
                 * slot this sequence filled will do -- the same rule, and for
                 * the same reason, as RESTOREFH's above. */
                if (!have_saved) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* A rename onto an existing name unlinks what was there, so it
                 * has REMOVE's problem too: a delegation on the displaced
                 * object to recall, or a data server backing it to delete.
                 * Both are decisions about an object the sequence never names.
                 *
                 * RENAME has it twice -- the source is recalled as well as the
                 * target -- and once more besides: the target LOOKUP finding
                 * NOTHING is the ordinary case and must not fail the run,
                 * which a gate cannot express.  It may fail an op that
                 * succeeded; it may not pass one that failed
                 * (chimera_vfs_compound_gate_t).  See the REMOVE case for the
                 * recall the VFS core would have to own. */
                if (chimera_server_config_get_nfs4_delegations(
                        thread->shared->config) ||
                    chimera_vfs_pnfs_enabled(thread->shared->vfs)) {
                    nenc = i;
                    stop = 1;
                    break;
                }
                break;

            case OP_LINK:
                if (may_fail_late ||
                    chimera_nfs4_validate_name(&argop->oplink.newname) !=
                    NFS4_OK) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* The object to link is the saved filehandle; see RENAME. */
                if (!have_saved) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* No delegation or pNFS gate: a LINK displaces nothing.  The
                 * name it creates must not already exist -- RFC 7530 SS16.9.5
                 * makes that NFS4ERR_EXIST -- so unlike RENAME there is never
                 * a victim to recall a delegation on or a backing to delete. */
                break;

            case OP_OPEN:
            {
                struct OPEN4args *oa = &argop->opopen;

                /* OPEN creates, so the mutation rule applies to it exactly as
                 * it does to SETXATTR: nothing whose NFSv4-side check fails
                 * after the sequence has run may precede it. */
                if (may_fail_late) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }

                /* One OPEN to a run.  The request carries one open_owner
                 * pin, one share reservation and one delegation decision,
                 * and a second OPEN would need its own of each; the
                 * op-at-a-time path takes it from there. */
                if (open_at >= 0) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* Only the two claims that are an ordinary open of a name or of
                 * the current filehandle.  CLAIM_PREVIOUS is a reclaim,
                 * CLAIM_DELEGATE_CUR validates a delegation the client cites,
                 * and CLAIM_DELEGATE_PREV is refused outright. */
                if (oa->claim.claim != CLAIM_NULL &&
                    oa->claim.claim != CLAIM_FH) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }

                /* RFC 8881 §18.16.3: an EXCLUSIVE4_1 attribute outside
                 * suppattr_exclcreat is NFS4ERR_INVAL, and the per-op path is
                 * what says so.  Without this the verifier would silently
                 * clobber a time_access_set or time_modify_set the client
                 * asked for. */
                if (oa->openhow.opentype == OPEN4_CREATE &&
                    oa->openhow.how.mode == EXCLUSIVE4_1 &&
                    (chimera_nfs4_validate_createattrs(
                         oa->openhow.how.ch_createboth.cva_attrs.num_attrmask,
                         oa->openhow.how.ch_createboth.cva_attrs.attrmask) !=
                     NFS4_OK ||
                     chimera_nfs4_validate_exclcreat_attrs(
                         oa->openhow.how.ch_createboth.cva_attrs.num_attrmask,
                         oa->openhow.how.ch_createboth.cva_attrs.attrmask) !=
                     NFS4_OK ||
                     (oa->openhow.how.ch_createboth.cva_attrs.num_attrmask >= 1 &&
                      (oa->openhow.how.ch_createboth.cva_attrs.attrmask[0] &
                       (1U << FATTR4_ACL))))) {
                    nenc = i;
                    stop = 1;
                    break;
                }

                /* Statuses the per-op path decides before it opens anything. */
                if ((oa->share_access & (OPEN4_SHARE_ACCESS_READ |
                                         OPEN4_SHARE_ACCESS_WRITE)) == 0) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }

                if (oa->claim.claim == CLAIM_NULL &&
                    chimera_nfs4_validate_name(&oa->claim.file) != NFS4_OK) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }

                if (oa->openhow.opentype == OPEN4_CREATE &&
                    (oa->openhow.how.mode == UNCHECKED4 ||
                     oa->openhow.how.mode == GUARDED4)) {
                    if (chimera_nfs4_validate_createattrs(
                            oa->openhow.how.createattrs.num_attrmask,
                            oa->openhow.how.createattrs.attrmask) != NFS4_OK) {
                        {
                            nenc = i;
                            stop = 1;
                            break;
                        }
                    }

                    /* An ACL in the create attributes would have to survive
                    * from the moment the sequence is built to the moment it
                    * runs, and the sequence deliberately carries no ACL. */
                    if (oa->openhow.how.createattrs.num_attrmask >= 1 &&
                        (oa->openhow.how.createattrs.attrmask[0] &
                         (1U << FATTR4_ACL))) {
                        {
                            nenc = i;
                            stop = 1;
                            break;
                        }
                    }
                }

                /* The grace-window and per-client reclaim gates, which the
                 * per-op path applies at OPEN entry.  When either would refuse,
                 * let it be the one to say so. */
                if (nfs_recovery_open_check(&thread->shared->nfs4_recovery,
                                            req->session ?
                                            req->session->client_unified : NULL,
                                            false) != NFS4_OK) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }

                if (req->minorversion > 0 && req->session &&
                    !nfs4_client_reclaim_complete(
                        &thread->shared->nfs4_shared_clients,
                        req->session->nfs4_session_clientid)) {
                    {
                        nenc = i;
                        stop = 1;
                        break;
                    }
                }

                /* A delegation grant can park on an in-flight CB_NULL probe,
                 * which suspends the OPEN, and the probe is also *kicked* by
                 * the grant attempt -- which OPEN kicks it being observable
                 * (the kicking OPEN gets no delegation, the next one does).
                 * The probe therefore stays exactly where it is, and an OPEN
                 * that could still reach it is made the last op of the run so
                 * the fill can hand the whole tail over.  An OPEN that could
                 * not is carried like any other op and the run goes on past
                 * it -- which, with delegations off, is every OPEN. */
                open_ends_run = nfs4_vfs_open_may_delegate(thread, req, argop);

                open_at = (int) i;

                if (open_ends_run) {
                    nenc = i + 1;
                }

                /* The object the OPEN opened is the current one now. */
                cur_moved = 1;
                break;
            }

            default:
                break;
        } /* switch */

        if (stop) {
            break;
        }

        /* Counted only for an op the run actually carries: one the scan just
         * declined is dispatched op by op, where the dispatcher sizes its
         * answer and applies its own headroom test to it. */
        reply_bound  += nfs4_vfs_op_reply_bound(argop);
        stages_early |= nfs4_vfs_op_stages_early(argop->argop);
        mutates      |= nfs4_vfs_op_mutates(argop);

        if (nfs4_vfs_op_ends_run(argop->argop) ||
            (argop->argop == OP_OPEN && open_ends_run)) {
            break;
        }
    }

    /* Nothing at all was expressible, so there is no sequence to build. */
    if (nenc <= first) {
        return 0;
    }

    /*
     * Reply-buffer headroom for a sequence that stages part of its answer while
     * it runs (see nfs4_vfs_op_stages_early).  Those stagings all happen before
     * any result has been marshalled, so the headroom READDIR, GETXATTR and
     * LISTXATTRS size their answers from is not the headroom the per-op path
     * would have left them.
     *
     * Rather than model the per-op path's allocation order, encode only when
     * the buffer is roomy enough that the question cannot arise: if the whole
     * sequence's worst case plus the dispatcher's 8192-byte floor fits in what
     * is free now, then each of those ops saturates its own cap in both paths
     * and neither path can reach the floor.  Same sizes, same statuses, same
     * entries -- whatever order the allocations happen in.
     *
     * The same test guards a run that MUTATES, for a different reason.  The
     * dispatcher refuses an op with NFS4ERR_RESOURCE before running it once the
     * buffer is within 8192 bytes of full; this path can only ask that as each
     * result is filled, after every op in the run has executed.  On a run that
     * only reads a late refusal is the same reply, truncated at the same op.
     * On a run with a CREATE, REMOVE, RENAME, LINK, SETATTR, WRITE, an xattr
     * write, an allocate, or an OPEN that creates, it would report RESOURCE
     * for a change that has already been made -- and the created file would be
     * left behind.  So the rule is: A RUN THAT CARRIES A MUTATING OP IS BUILT
     * ONLY WHEN THE WHOLE RUN'S WORST-CASE REPLY CONSUMPTION, PLUS THE 8192-BYTE
     * FLOOR, FITS IN WHAT IS FREE NOW.  The whole run, not the ops in front of
     * the mutation: what the build stages and what a READDIR marshals while the
     * sequence runs are consumed before any fill, wherever they sit in the
     * order.  Under the bound no fill can reach the floor, so the late test
     * (nfs4_vfs_compound_complete) never fires on such a run; a run it refuses
     * goes to the per-op path, which pre-checks each op itself.
     */
    if ((stages_early || mutates) && reply_bound + 8192 > avail) {
        return 0;
    }

    /* At a "/" export's root a sibling export shadows any real entry of the
     * same name (nfs4_root_junction_check); the VFS resolves names in the
     * backend and cannot see that graft. */
    if (have_lookup && thread->shared->root_export_id != 0) {
        return 0;
    }

    /* With a "/" export configured, LOOKUPP has to recognize the namespace root
     * and hand back export roots' parents from it (nfs4_root_export_fh_get);
     * neither is visible to the VFS. */
    if (have_lookupp && thread->shared->root_export_id != 0) {
        return 0;
    }

    /* RFC 7530/8881 §10.4.3: when another client holds a write delegation the
     * GETATTR must query it via CB_GETATTR and combine the answer.  That query
     * only ever runs for a client reached through a session.  Expressible as
     * an ends-run GETATTR whose fill parks on the CB_GETATTR, the way an OPEN
     * that could earn a delegation ends its run -- but the combine itself
     * lives in nfs4_proc_getattr.c and the split belongs with it. */
    if (have_getattr &&
        chimera_server_config_get_nfs4_delegations(thread->shared->config) &&
        req->session && req->session->client_unified) {
        return 0;
    }

    /* Establish the object the sequence starts from. */
    lead_putfh = (req->args_compound->argarray[first].argop == OP_PUTFH);

    if (lead_putfh) {
        struct PUTFH4args *pa = &req->args_compound->argarray[first].opputfh;

        /* Mirrors chimera_nfs4_putfh.  The pseudo-root is not a wrapped VFS
         * handle at all; the decode authenticates the wire handle, recovers the
         * inner VFS handle, and re-derives the export id and squashed
         * credential the sequence will run under; a named-attribute directory
         * handle addresses a synthetic object the VFS does not have. */
        if (fh_is_nfs4_root(pa->object.data, pa->object.len) ||
            pa->object.len > NFS4_FHSIZE) {
            return 0;
        }

        if (chimera_nfs_fh_decode(req, pa->object.data, pa->object.len,
                                  cur_fh, &cur_fhlen) != CHIMERA_NFS_FH_OK) {
            return 0;
        }

        if (chimera_nfs4_fh_is_attrdir(cur_fh, cur_fhlen) ||
            !chimera_vfs_fh_is_plausible(thread->vfs_thread, cur_fh,
                                         cur_fhlen)) {
            return 0;
        }
    } else {
        if (req->fhlen == 0 ||
            fh_is_nfs4_root(req->fh, req->fhlen) ||
            chimera_nfs4_fh_is_attrdir(req->fh, req->fhlen)) {
            return 0;
        }

        memcpy(cur_fh, req->fh, req->fhlen);
        cur_fhlen = req->fhlen;
    }

    /* An export root's parent is the NFSv4 namespace root, not the backend's
     * physical parent -- a graft the VFS knows nothing about, so its ".." is
     * the wrong answer.  Only reachable for a LOOKUPP from the object the
     * sequence starts from, which is the only one a LOOKUPP is encoded for. */
    if (have_lookupp &&
        chimera_nfs4_fh_is_vfs_mount_root(thread->vfs, cur_fh,
                                          (uint32_t) cur_fhlen)) {
        return 0;
    }

    /* ---- ops [first, nenc) are expressible: build the sequence ---- */

    /* Authorize a size-changing SETATTR and resolve the handle it applies
     * through.  Like the 4.0 OPEN entry below, this takes a reference, so it
     * runs only once the sequence is certain to be built; a refusal from here
     * sends the op back to the per-op path, which reaches the same answer. */
    if (setattr_at >= 0) {
        if (nfs4_vfs_setattr_authorize(
                thread, req,
                &req->args_compound->argarray[setattr_at].opsetattr,
                cur_fh, cur_fhlen, &setattr_handle) != NFS4_OK) {
            return 0;
        }
    }

    /* Everything a LOCK settles before it asks the arbiter anything.  A LOCK
     * is always the last op of its run, so a prepare that answers instead of
     * preparing simply drops the LOCK from the run: nothing it did mutates,
     * and the per-op path reaches the same answer -- and produces the seqid
     * advance that goes with it, which this path deliberately does not.
     *
     * Ahead of the 4.0 OPEN entry below and behind the SETATTR authorize
     * above, in the same window and for the same reason: it takes references,
     * so it must not run before a decision that could still send the whole
     * COMPOUND back to the per-op path. */
    if (lock_at >= 0) {
        nfsstat4 lock_status;

        /* The object the sequence starts from is what the LOCK's
         * existing-stateid rule reads as the current filehandle, and the scan
         * admitted the LOCK only while nothing had moved it.  Stating it on
         * the request is what the per-op path would have had in hand; a
         * leading PUTFH re-states the same handle if the build gives up. */
        memcpy(req->fh, cur_fh, (size_t) cur_fhlen);
        req->fhlen = cur_fhlen;

        if (chimera_nfs4_lock_prepare(
                thread, req,
                &req->args_compound->argarray[lock_at],
                &req->res_compound.resarray[lock_at],
                &lock_status) == NFS4_LOCK_PREPARE_READY) {
            lock_prepared = 1;
        } else {
            nenc    = (uint32_t) lock_at;
            lock_at = -1;

            if (nenc <= first) {
                return 0;
            }
        }
    }

    /* The 4.0 OPEN's entry-time seqid classification.  Deliberately the last
     * thing before the sequence is built: it pins the open_owner on the request
     * for chimera_nfs4_open_finish to advance, so it must not run ahead of a
     * decision that could still send this COMPOUND back to the per-op path --
     * which would resolve the same owner a second time and pin it twice. */
    if (open_at >= 0 && req->minorversion == 0) {
        nfsstat4 entry_status;

        if (chimera_nfs4_open_4_0_entry(thread, req, (uint32_t) open_at,
                                        &entry_status)) {
            /* Answered without any VFS work -- a replay, a bad seqid, a stale
             * clientid.  Nothing it did mutates and nothing was pinned, so
             * the OPEN simply drops out of the run and the op-at-a-time path
             * reaches the same answer, producing the seqid advance that goes
             * with it.  The same shape a prepare that answers instead of
             * preparing takes for LOCK, and what lets an OPEN sit anywhere in
             * a 4.0 run rather than only at the front of one. */
            nenc          = (uint32_t) open_at;
            open_at       = -1;
            open_ends_run = 0;

            if (nenc <= first) {
                return 0;
            }
        } else {
            open_4_0_pinned = 1;
        }
    }

    compound = chimera_vfs_compound_alloc(thread->vfs_thread, &req->cred);

    ctx = calloc(1, sizeof(*ctx));
    chimera_nfs_abort_if(ctx == NULL, "Failed to allocate NFSv4 compound context");
    ctx->req = req;
    /* 0 is a real VFS op index -- the seed PUTFH's -- so "no LOCK" and "no
     * share CLAIM" have to be said with something else. */
    ctx->lock_claim_op   = -1;
    ctx->lock_fh_op      = -1;
    ctx->open_claim_op   = -1;
    ctx->open_setattr_op = -1;
    ctx->open_ends_run   = open_ends_run;

    /* Seed the current object.  When the remainder opens with a PUTFH this is
     * that PUTFH; otherwise it re-states the COMPOUND's current filehandle and
     * belongs to whichever op comes first. */
    idx = chimera_vfs_compound_add_putfh(compound, cur_fh, cur_fhlen);

    if (idx < 0) {
        goto refuse;
    }

    next = 0;

    for (i = first, k = 0; i < nenc; i++, k++) {
        struct nfs4_vfs_op *map = &ctx->ops[k];

        argop = &req->args_compound->argarray[i];

        map->res_index   = i;
        map->vfs_lo      = next;
        map->vfs_aux     = -1;
        map->gate_status = 0;

        /* A slot adds no VFS op: it takes an EMPTY vfs range, no result op,
         * and leaves `next` and `idx` where they were, so the op after it
         * encodes exactly where it would have with the slot not there at all.
         * The completion's "every VFS op inside a filled NFSv4 op ran" walk
         * then has nothing to walk for it, which is the invariant unchanged
         * rather than an exception to it.  See nfs4_vfs_op_is_slot. */
        if (nfs4_vfs_op_is_slot(argop->argop)) {
            map->is_slot = 1;
            map->vfs_res = -1;
            map->vfs_hi  = next - 1;
            continue;
        }

        switch (argop->argop) {
            case OP_PUTFH:
                if (i == first) {
                    /* The seed PUTFH above is this op. */
                    map->vfs_res = idx;
                } else {
                    /* A later PUTFH, into the same export -- see the scan.  It
                     * moves the sequence's current object the way any other op
                     * does, so it is an op of its own. */
                    uint8_t  later_fh[CHIMERA_VFS_FH_SIZE];
                    int      later_len;
                    uint16_t later_export;

                    if (chimera_nfs_fh_unwrap(argop->opputfh.object.data,
                                              (int) argop->opputfh.object.len,
                                              &later_export,
                                              later_fh, &later_len,
                                              thread->shared->fh_key,
                                              thread->shared->fh_sign) !=
                        CHIMERA_NFS_FH_OK) {
                        goto refuse;
                    }

                    idx = chimera_vfs_compound_add_putfh(compound,
                                                         later_fh,
                                                         later_len);
                    map->vfs_res = idx;

                    if (idx < 0) {
                        goto refuse;
                    }
                }

                /* The PUTFH moved the current object; nothing is open on the
                 * new one. */
                cur_open_flags = 0;

                /* Stat the handle so the zero-link staleness rule has something
                 * to test; the gate reads it as this op finishes. */
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_getattr(
                    compound, CHIMERA_VFS_ATTR_NLINK);
                map->vfs_aux = idx;
                break;

            case OP_LOOKUP:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_DIR) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_lookup(
                    compound,
                    (const char *) argop->oplookup.objname.data,
                    (int) argop->oplookup.objname.len,
                    0, 0);
                map->vfs_res = idx;

                /* The child is the current object now. */
                cur_open_flags = 0;
                break;

            case OP_GETATTR:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_getattr(
                    compound,
                    chimera_nfs4_attr2mask(argop->opgetattr.attr_request,
                                           argop->opgetattr.num_attr_request));
                map->vfs_res = idx;
                break;

            case OP_ACCESS:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_access(
                    compound,
                    chimera_nfs4_access4_to_mask(argop->opaccess.access));
                map->vfs_res = idx;
                break;

            case OP_GETFH:
                idx          = chimera_vfs_compound_add_getfh(compound);
                map->vfs_res = idx;
                break;

            case OP_READLINK:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                /* The type gate READLINK applies before it reads.  It and the
                 * READLINK act on the same object, so they share the open. */
                idx = chimera_vfs_compound_add_getattr(
                    compound, CHIMERA_VFS_ATTR_MODE);
                map->vfs_aux = idx;

                if (idx >= 0) {
                    idx = chimera_vfs_compound_add_readlink(compound);
                }
                map->vfs_res = idx;
                break;

            case OP_LOOKUPP:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_DIR) < 0) {
                    goto refuse;
                }

                idx          = chimera_vfs_compound_add_lookupp(compound, 0);
                map->vfs_res = idx;

                /* The parent is the current object now. */
                cur_open_flags = 0;
                break;

            case OP_CREATE:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_DIR) < 0) {
                    goto refuse;
                }

                idx          = nfs4_vfs_add_create_op(compound, argop);
                map->vfs_res = idx;

                /* The created object is the current one now. */
                cur_open_flags = 0;
                break;

            case OP_SECINFO:
                /* The name lookup is the whole of the VFS work: it produces
                 * NFS4ERR_NOTDIR for a current object that is not a directory
                 * and NFS4ERR_NOENT for a name that is not there, which are the
                 * two errors SECINFO is specified to return.  The flavors
                 * themselves come from the export, which the request already
                 * names. */
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_DIR) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_lookup(
                    compound,
                    (const char *) argop->opsecinfo.name.data,
                    (int) argop->opsecinfo.name.len, 0, 0);
                map->vfs_res = idx;

                /* Only 4.1 consumes the handle; on 4.0 it survives the LOOKUP's
                 * move, and the completion undoes that move from the run
                 * itself -- see nfs4_vfs_compound_complete.  Not from cur_fh:
                 * that is the SEED, and a LOOKUP, LOOKUPP, CREATE, RESTOREFH
                 * or later PUTFH in front of this op has moved the current
                 * object since without touching it. */
                if (req->minorversion < 1) {
                    ctx->secinfo_lookup = idx;
                }

                cur_open_flags = 0;
                break;

            case OP_LOCKT:
                /* The type gate, and then the probe itself.  A CLAIM_TEST is
                 * arbitrated per file and reads only the object's handle, so
                 * it shares the metadata open the gate's stat needs -- which
                 * is also the open the per-op path uses (PATH|NOFOLLOW), and
                 * not a data open, which of a FIFO would block. */
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_getattr(
                    compound, CHIMERA_VFS_ATTR_MODE);
                map->vfs_aux = idx;

                if (idx >= 0) {
                    nfs4_vfs_lockt_init_probe(req, &argop->oplockt,
                                              &map->probe);
                    idx = chimera_vfs_compound_add_claim_test(compound,
                                                              &map->probe, 0);
                }
                map->vfs_res = idx;
                break;

            case OP_LOCK:
            {
                struct nfs_lock_state   *ls = req->nfs_state_ref;
                struct nfs4_range_lease *rl = req->nfs_inflight_range;

                /* The lock is arbitrated on the object the STATEID names, not
                 * on whatever the sequence's cursor holds -- the per-op path
                 * acts on lock_state->handle and the two can differ.  So the
                 * handle is lent to the run; it is the client's, pinned for
                 * the sequence by the acquire-ref prepare took, and the
                 * sequence never releases it.  A CLAIM reads only the fh, so
                 * whatever the handle was opened for serves. */
                ctx->lock_fh_op = next - 1;

                idx = chimera_vfs_compound_add_puthandle(
                    compound, ls->handle,
                    nfs4_vfs_handle_open_flags(ls->handle));

                if (idx >= 0) {
                    /* TRY: NFSv4 answers DENIED on conflict rather than
                     * blocking, and a breakable cross-protocol holder still
                     * has its break kicked inside the acquire. */
                    idx = chimera_vfs_compound_add_claim(
                        compound, &rl->claim, &rl->ticket,
                        CHIMERA_VFS_COMPOUND_CLAIM_TRY, 0, 0, 0, 0);
                }

                map->vfs_res        = idx;
                ctx->lock_present   = 1;
                ctx->lock_res_index = i;
                ctx->lock_claim_op  = idx;

                /* The lent handle is the current object now, and it is not
                 * where the COMPOUND ends up -- see lock_fh_op. */
                cur_open_flags = 0;
                break;
            }

            case OP_READ:
            case OP_WRITE:
            {
                const struct stateid4     *sid = (argop->argop == OP_READ) ?
                    &argop->opread.stateid : &argop->opwrite.stateid;
                uint32_t                   want = (argop->argop == OP_READ) ?
                    OPEN4_SHARE_ACCESS_READ : OPEN4_SHARE_ACCESS_WRITE;
                struct chimera_claim_actor io_owner;
                int                        have_owner = 0;

                if (nfs4_vfs_io_authorize(thread, req, sid, want,
                                          cur_fh, cur_fhlen,
                                          &map->io_handle,
                                          &io_owner, &have_owner) != NFS4_OK) {
                    goto refuse;
                }

                /* An anonymous stateid leaves no handle, so the I/O acts on
                 * the current object and the encoder opens it -- for DATA, and
                 * REGULAR_ONLY, because the object has to be a regular file
                 * and the open is where that is settled.  A path open would
                 * not settle it: opening a directory for a path succeeds, and
                 * the data open behind it fails with whatever errno the
                 * backend has for "you cannot read a directory" rather than
                 * the NFS4ERR_ISDIR the client is owed. */
                if (!map->io_handle &&
                    nfs4_vfs_open_for(compound, &cur_open_flags,
                                      (argop->argop == OP_READ) ?
                                      (NFS4_VFS_OPEN_DATA |
                                       CHIMERA_VFS_OPEN_READ_ONLY) :
                                      NFS4_VFS_OPEN_DATA) < 0) {
                    goto refuse;
                }

                if (argop->argop == OP_READ) {
                    {
                        struct evpl_iovec *riov = xdr_dbuf_alloc_space(
                            sizeof(*riov) * NFS4_VFS_READ_MAX_IOV,
                            req->encoding->dbuf);

                        chimera_nfs_abort_if(riov == NULL,
                                             "Failed to allocate space");

                        /* From the reply's own memory, because the descriptors
                         * the read writes here are where the reply will read
                         * them from and cannot be moved afterwards. */
                        idx = chimera_vfs_compound_add_read(
                            compound, map->io_handle,
                            argop->opread.offset, argop->opread.count,
                            riov, NFS4_VFS_READ_MAX_IOV,
                            0,
                            have_owner ? &io_owner : NULL, NULL, 0);
                    }
                } else {
                    /* Ownership of the payload moves off the RPC2 message, so
                     * that freeing the message does not release iovecs this
                     * sequence is about to hand to the backend.  A no-op unless
                     * the data arrived in an RDMA read chunk.
                     *
                     * Taken here, before the build is certain to submit, and
                     * that is safe: a later `goto refuse` re-dispatches this
                     * WRITE per-op, and chimera_nfs4_write takes the chunk
                     * again -- but evpl_rpc2_encoding_take_read_chunk
                     * (ext/libevpl/include/evpl/evpl_rpc2_program.h) does
                     * nothing but zero read_chunk->niov, so a second take is
                     * idempotent, and the iovecs themselves are still
                     * referenced from opwrite.data, which every path that
                     * fails the WRITE without running it releases
                     * (nfs4_fail_undispatched_op, the sweep in
                     * chimera_nfs4_compound_complete).  The one thing a take
                     * must never be followed by is the RPC layer's own
                     * release, and zeroing niov is exactly what prevents it. */
                    evpl_rpc2_encoding_take_read_chunk(req->encoding, NULL,
                                                       NULL);

                    idx = chimera_vfs_compound_add_write(
                        compound, map->io_handle,
                        argop->opwrite.offset,
                        argop->opwrite.data.length,
                        argop->opwrite.stable,
                        argop->opwrite.data.iov,
                        argop->opwrite.data.niov,
                        0, 0,
                        have_owner ? &io_owner : NULL);
                }

                map->vfs_res = idx;
                break;
            }

            case OP_SETATTR:
            {
                struct chimera_vfs_attrs sattr;

                memset(&sattr, 0, sizeof(sattr));
                chimera_nfs4_unmarshall_attrs(
                    &sattr,
                    argop->opsetattr.obj_attributes.num_attrmask,
                    argop->opsetattr.obj_attributes.attrmask,
                    argop->opsetattr.obj_attributes.attr_vals.data,
                    argop->opsetattr.obj_attributes.attr_vals.len,
                    NULL, 0);

                if ((int) i == setattr_at) {
                    map->io_handle = setattr_handle;
                    setattr_handle = NULL;
                }

                if (!map->io_handle &&
                    nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_setattr(compound,
                                                       map->io_handle,
                                                       &sattr, 0, 0);
                map->vfs_res = idx;
                break;
            }

            case OP_REMOVE:
                /* No type assertion and no recall request: NFS4's REMOVE is
                 * type-agnostic, and the recall decision is made on the
                 * protocol side before the sequence is built. */
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_DIR) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_remove(
                    compound,
                    (const char *) argop->opremove.target.data,
                    (int) argop->opremove.target.len,
                    0, 0, 0);
                map->vfs_res = idx;
                break;

            /* Source from the saved filehandle, target from the current one --
             * which is what the VFS op takes, so neither needs anything said
             * about where the other directory is. */
            case OP_RENAME:
                idx = chimera_vfs_compound_add_rename(
                    compound,
                    (const char *) argop->oprename.oldname.data,
                    (int) argop->oprename.oldname.len,
                    (const char *) argop->oprename.newname.data,
                    (int) argop->oprename.newname.len,
                    0, 0, 0);
                map->vfs_res = idx;
                break;

            case OP_LINK:
                /* NFS4's LINK reply is a change_info for the directory; the
                 * linked object's own attributes have no reader. */
                idx = chimera_vfs_compound_add_link(
                    compound,
                    (const char *) argop->oplink.newname.data,
                    (int) argop->oplink.newname.len,
                    0, 0, 0);
                map->vfs_res = idx;
                break;

            case OP_OPEN:
            {
                int open_idx;

                /* A named OPEN resolves in the current directory; an unnamed
                * one re-opens the current object and needs nothing first. */
                if (nfs4_vfs_open_is_named(argop) &&
                    nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_DIR) < 0) {
                    goto refuse;
                }

                ctx->open_present   = 1;
                ctx->open_res_index = i;

                /* Everything the OPEN settles before anything is open: the
                 * open_owner the gate reads, and the share reservation the
                 * CLAIM below takes. */
                if (!nfs4_vfs_open_prepare(thread, req, argop, map)) {
                    goto refuse;
                }

                idx          = nfs4_vfs_add_open_op(req, compound, argop, map);
                map->vfs_res = idx;
                open_idx     = idx;

                if (idx < 0) {
                    goto refuse;
                }

                /* The cross-protocol SHARE reservation, arbitrated inside the
                 * run rather than out of band once the result is filled --
                 * with TRY, which is the wait=false the out-of-band acquire
                 * asked for, so BREAKING comes back as the op's result and
                 * becomes NFS4ERR_DELAY.  It addresses the handle the OPEN
                 * produced rather than the sequence's cursor: a claim is
                 * arbitrated per file, and that is the file.  The gate spares
                 * it when the OPEN turns out to coalesce onto an open this
                 * owner already holds. */
                idx = chimera_vfs_compound_add_claim(
                    compound, &map->share->claim, &map->share->ticket,
                    CHIMERA_VFS_COMPOUND_CLAIM_TRY, 0, 0, 0, 0);

                if (idx < 0) {
                    goto refuse;
                }

                chimera_vfs_compound_op_use_handle(compound, (uint32_t) idx,
                                                   (uint32_t) open_idx);
                ctx->open_claim_op = idx;

                /* An UNCHECKED4 create that asked for size 0 truncates a name
                 * that was already there.  An op of the run now, behind the
                 * CLAIM -- so the file is still not emptied for an OPEN that
                 * fails -- and through the handle the OPEN just produced, so
                 * it is authorized by the access this OPEN was granted rather
                 * than re-checked against the file's mode, which is the
                 * ftruncate(2) rule and the same grant a WRITE would use.
                 * The gate skips it unless the name resolved to something. */
                if (map->open_trunc_if_existed) {
                    struct chimera_vfs_attrs trunc;

                    memset(&trunc, 0, sizeof(trunc));
                    trunc.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
                    trunc.va_req_mask = CHIMERA_VFS_ATTR_SIZE;
                    trunc.va_size     = 0;

                    idx = chimera_vfs_compound_add_setattr(compound, NULL,
                                                           &trunc, 0, 0);

                    if (idx < 0) {
                        goto refuse;
                    }

                    chimera_vfs_compound_op_use_handle(compound,
                                                       (uint32_t) idx,
                                                       (uint32_t) open_idx);
                    ctx->open_setattr_op = idx;
                }

                /* The opened object is the current one now, and the handle the
                 * OPEN produced is its own -- not the sequence's cursor. */
                cur_open_flags = 0;
                break;
            }

            case OP_SAVEFH:
                idx          = chimera_vfs_compound_add_savefh(compound);
                map->vfs_res = idx;
                break;

            case OP_RESTOREFH:
                idx          = chimera_vfs_compound_add_restorefh(compound);
                map->vfs_res = idx;

                /* A different object is current; nothing is open on it. */
                cur_open_flags = 0;
                break;

            case OP_COMMIT:
                /* The regular-file gate COMMIT applies before it flushes, from
                 * a stat of the object taken through a path open -- the same
                 * two-open shape the per-op path has. */
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_getattr(
                    compound, CHIMERA_VFS_ATTR_MODE);
                map->vfs_aux = idx;

                /* The flush itself needs the data open, which is a different
                 * handle from a different cache than the stat above. */
                if (idx >= 0 &&
                    nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_DATA) < 0) {
                    goto refuse;
                }

                if (idx >= 0) {
                    idx = chimera_vfs_compound_add_commit(
                        compound,
                        argop->opcommit.offset,
                        argop->opcommit.count,
                        0, CHIMERA_VFS_ATTR_MODE);
                }
                map->vfs_res = idx;
                break;

            case OP_READDIR:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_DIR) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_readdir_stream(
                    compound,
                    argop->opreaddir.cookie,
                    nfs4_vfs_readdir_verifier(&argop->opreaddir),
                    chimera_nfs4_attr2mask(argop->opreaddir.attr_request,
                                           argop->opreaddir.num_attr_request),
                    0,
                    0, NULL, 0,
                    nfs4_vfs_readdir_reset,
                    nfs4_vfs_readdir_append,
                    ctx);
                map->vfs_res = idx;
                break;

            case OP_VERIFY:
            case OP_NVERIFY:
                /* The whole of VERIFY is a comparison against attributes the
                 * client sent; the VFS work is the stat it compares.  The
                 * comparison itself runs in the gate, so a mismatch stops the
                 * ops behind it -- which is what VERIFY exists to do. */
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_getattr(
                    compound,
                    chimera_nfs4_attr2mask(
                        argop->opverify.obj_attributes.attrmask,
                        argop->opverify.obj_attributes.num_attrmask));
                map->vfs_aux = idx;
                map->vfs_res = idx;
                break;

            case OP_ALLOCATE:
            case OP_DEALLOCATE:
            case OP_SEEK:
            case OP_WRITE_SAME:
            {
                /* All three name their object with a stateid and act on the
                * current filehandle, exactly as READ and WRITE do -- so they
                * authorize the same way, and a special stateid leaves
                * io_handle NULL and the op addresses the current object. */
                const struct stateid4     *sid =
                    (argop->argop == OP_ALLOCATE) ? &argop->opallocate.aa_stateid :
                    (argop->argop == OP_DEALLOCATE) ? &argop->opdeallocate.da_stateid :
                    &argop->opseek.sa_stateid;
                uint32_t                   want = (argop->argop == OP_SEEK) ?
                    OPEN4_SHARE_ACCESS_READ : OPEN4_SHARE_ACCESS_WRITE;
                struct chimera_claim_actor io_owner;
                int                        have_owner = 0;

                if (nfs4_vfs_io_authorize(thread, req, sid, want,
                                          cur_fh, cur_fhlen,
                                          &map->io_handle,
                                          &io_owner, &have_owner) != NFS4_OK) {
                    goto refuse;
                }

                /* The regular-file gate, from a stat of the current object --
                 * the same shape COMMIT uses. */
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_getattr(
                    compound, CHIMERA_VFS_ATTR_MODE);
                map->vfs_aux = idx;

                /* A special stateid left io_handle NULL, so the op acts on the
                 * current object and wants it open for data. */
                if (idx >= 0 && !map->io_handle &&
                    nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_DATA) < 0) {
                    goto refuse;
                }

                if (idx >= 0) {
                    if (argop->argop == OP_SEEK) {
                        idx = chimera_vfs_compound_add_seek(
                            compound, map->io_handle,
                            argop->opseek.sa_offset,
                            argop->opseek.sa_what == NFS4_CONTENT_HOLE ? 1 : 0);
                    } else if (argop->argop == OP_ALLOCATE) {
                        idx = chimera_vfs_compound_add_allocate(
                            compound, map->io_handle,
                            argop->opallocate.aa_offset,
                            argop->opallocate.aa_length,
                            0, 0, 0);
                    } else if (argop->argop == OP_DEALLOCATE) {
                        idx = chimera_vfs_compound_add_allocate(
                            compound, map->io_handle,
                            argop->opdeallocate.da_offset,
                            argop->opdeallocate.da_length,
                            CHIMERA_VFS_ALLOCATE_DEALLOCATE, 0, 0);
                    } else {
                        idx = chimera_vfs_compound_add_write_same(
                            compound, map->io_handle,
                            argop->opwrite_same.wsa_adb.adb_offset,
                            argop->opwrite_same.wsa_adb.adb_block_size,
                            argop->opwrite_same.wsa_adb.adb_block_count,
                            argop->opwrite_same.wsa_adb.adb_pattern.data,
                            argop->opwrite_same.wsa_adb.adb_pattern.len,
                            argop->opwrite_same.wsa_adb.adb_reloff_pattern,
                            argop->opwrite_same.wsa_stable,
                            0, 0);
                    }
                }

                map->vfs_res = idx;
                break;
            }

            case OP_GETXATTR:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx          = nfs4_vfs_add_xattr_op(req, compound, argop);
                map->vfs_res = idx;
                break;

            case OP_SETXATTR:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx          = nfs4_vfs_add_xattr_op(req, compound, argop);
                map->vfs_res = idx;
                break;

            case OP_LISTXATTRS:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx = chimera_vfs_compound_add_listxattrs(
                    compound,
                    argop->oplistxattrs.lxa_cookie,
                    chimera_nfs4_xattr_stage_max(
                        req, argop->oplistxattrs.lxa_maxcount));
                map->vfs_res = idx;
                break;

            case OP_REMOVEXATTR:
                if (nfs4_vfs_open_for(compound, &cur_open_flags,
                                      NFS4_VFS_OPEN_META) < 0) {
                    goto refuse;
                }

                idx          = nfs4_vfs_add_xattr_op(req, compound, argop);
                map->vfs_res = idx;
                break;

            default:
                idx = -1;
                break;
        } /* switch */

        if (idx < 0) {
            goto refuse;
        }

        map->vfs_hi = idx;
        next        = idx + 1;
    }

    ctx->num_ops = k;

    /* The 4.1 current-stateid lifecycle used to be replayed here, for the
     * whole run at once, on the grounds that the value never leaves the
     * request and so when it moves is not observable.  It is observable now:
     * a slot resolves the current stateid when it is APPLIED, and the ops
     * that set it (OPEN, and the slots themselves) do so then too, so a bulk
     * replay before the sequence ran would clear at build time what a CLOSE
     * behind an OPEN is entitled to read.  It is applied op by op in the fill
     * loop instead -- nfs4_vfs_current_stateid_step.  (Only for the ops the
     * run carries: anything past nenc is dispatched op by op afterwards and
     * the dispatcher applies it then.) */

    chimera_vfs_compound_set_gate(compound, nfs4_vfs_compound_gate, ctx);

    chimera_vfs_compound_submit(compound, nfs4_vfs_compound_complete, ctx);

    return 1;

 refuse:

    /* The build gave up after the 4.0 entry pinned the owner.  The per-op path
     * is about to resolve it again, so drop this reference rather than leave
     * two outstanding against one chimera_nfs4_open_finish. */
    if (open_4_0_pinned && req->open_4_0_owner) {
        nfs_open_owner_put(req->open_4_0_owner);
        req->open_4_0_owner = NULL;
    }

    if (setattr_handle) {
        chimera_vfs_release(thread->vfs_thread, setattr_handle);
    }

    /* The build gave up after the LOCK was prepared.  Nothing was consumed, so
     * the references go back without a seqid advance and the per-op path
     * settles the operation from the beginning. */
    if (lock_prepared && lock_at >= 0) {
        chimera_nfs4_lock_abandon(thread, req,
                                  &req->args_compound->argarray[lock_at]);
    }

    /* The build gave up partway; anything it resolved goes back.  The WRITE
     * payloads stay put -- the per-op path is about to run and releases them
     * itself. */
    for (k = 0; k < NFS4_VFS_COMPOUND_MAX_OPS; k++) {
        if (ctx->ops[k].io_handle) {
            chimera_vfs_release(thread->vfs_thread, ctx->ops[k].io_handle);
            ctx->ops[k].io_handle = NULL;
        }

        /* A prepared OPEN, whose reservation was never taken and whose owner
         * reference the op-at-a-time path is about to resolve again. */
        nfs4_vfs_open_abandon(thread, &ctx->ops[k]);
    }

    chimera_vfs_compound_free(compound);
    free(ctx);

    return 0;
} /* chimera_nfs4_compound_try_vfs */
