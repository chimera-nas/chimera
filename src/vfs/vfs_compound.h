// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "vfs.h"
#include "vfs_claim_types.h"
#include "vfs_lock.h"
#include "vfs_claim_journal.h"
#include "vfs_claim_access.h"

/*
 * VFS compounds own a sequence of filesystem operations and report its final
 * result through one completion callback. Backends currently execute the
 * ordinary per-operation requests; transactional backend integration is a
 * separate step.
 *
 * EXECUTION AND FRONTEND CALLOUTS. Build the known prefix before submission.
 * An operation's prepare callback can bind inputs from earlier results, skip
 * that operation, or reject it before its filesystem action. Its complete
 * callback can inspect the result, change its status, and append dependent
 * operations. These callbacks are synchronous and replayable: they may update
 * attempt-private state, but must not send replies, consume input, change
 * shared frontend state, or perform unrelated I/O. Reserve fallible protocol
 * admission/resources before submission; publish only after accepted finish.
 * Streaming enumeration callbacks obey the same rule.
 *
 * ERRORS AND FINISH. By default an ordinary operation error stops execution
 * and preserves the successful prefix. Explicit operation groups may continue
 * after an error; groups are not transaction savepoints. A finish rejection instead rejects the attempt,
 * including that prefix. Callers must inspect the compound's finish/aggregate
 * status before using individual results. A backend finish adapter may delay
 * final completion; without one, execution finishes immediately. This adapter
 * is a lifecycle seam, not a transaction implementation.
 *
 * RETRY. Original operation inputs are snapshotted at submission. Retry frees
 * attempt outputs, discards dynamically appended operations, restores those
 * inputs, and invokes the frontend reset callback before executing again.
 * Referenced credentials, buffers, names, and callback contexts must remain
 * valid across attempts; resetting the operation structs cannot restore data
 * the caller has overwritten. Retry is valid only after backend rollback (or
 * for a read-only attempt). An ordinary EAGAIN from an operation is not proof
 * of rollback. Current backends do not provide transactional rollback.
 *
 * ADDRESSING. CURRENT FH and SAVED FH name objects; SAVEFH copies the name.
 * CURRENT OPEN and SAVED OPEN hold references; SAVEHANDLE moves its reference.
 * PUTFH/LOOKUP/path operations select the current object, and OPEN selects its
 * current open handle. The executor can acquire an inferred handle for an
 * operation that needs one. PUTHANDLE and per-operation in_handle/src_handle
 * borrow caller-held references. An explicit per-operation handle addresses
 * that operation without changing the current cursor. RENAME/LINK use the
 * saved and current names; range operations can use explicit open handles.
 *
 * OWNERSHIP. Primary attribute ACL snapshots, staged directory entries,
 * buffers, open handles, claims, and READ iovecs belong to the compound until
 * freed, retried, or explicitly taken after accepted finish. A struct copy of
 * a result does not extend the lifetime of its pointers. Auxiliary directory
 * attributes do not retain ACLs; staged READDIR entries omit their ACLs.
 * GETHANDLE acquires an
 * independent reference, including when the current reference was borrowed;
 * take_handle/take_iov/take_reservation transfer their respective resources.
 * PUTHANDLE itself never transfers ownership. CLOSE of a borrowed handle
 * consumes the caller's reference only during accepted teardown; a rejected
 * attempt leaves that reference available for retry. CLOSE of an internal
 * provisional handle can release it during execution.
 *
 * Calls may complete synchronously or asynchronously. The executor advances
 * synchronous operations through a trampoline and parks for asynchronous VFS
 * work. Frontend op callouts must not wait; asynchronous filesystem work must
 * be expressed as operations in the sequence.
 */

struct chimera_vfs_compound;

int chimera_vfs_compound_add_checkpoint(
    struct chimera_vfs_compound *compound);

enum chimera_vfs_compound_op_type {
    /* Ordered frontend decision; no filesystem work. Callbacks obey the
     * same attempt-private contract as callbacks on filesystem operations. */
    CHIMERA_VFS_COMPOUND_OP_CHECKPOINT,
    /* Request coordination with external peers, memoized across attempts. */
    CHIMERA_VFS_COMPOUND_OP_COORDINATE,
    /* Make `fh` the current object.  Any sequence that addresses an object
     * starts with one of these. */
    CHIMERA_VFS_COMPOUND_OP_PUTFH,
    /* Resolve `name` in the current object; the result becomes current.  The
     * current object is opened as a directory, so a LOOKUP through anything
     * else fails on the open (ENOTDIR) rather than in the backend. */
    CHIMERA_VFS_COMPOUND_OP_LOOKUP,
    /* Attributes of the current object. */
    CHIMERA_VFS_COMPOUND_OP_GETATTR,
    /* Which of `requested` the credential holds on the current object. */
    CHIMERA_VFS_COMPOUND_OP_ACCESS,
    /* The current object's file handle, as a result the caller can read. */
    CHIMERA_VFS_COMPOUND_OP_GETFH,
    /* The current object's symlink target. */
    CHIMERA_VFS_COMPOUND_OP_READLINK,
    /* Copy the current object into the SAVED slot, and put the SAVED slot back
     * as the current object.  Neither touches a backend.  The saved slot holds
     * a file handle only, never an open handle -- see the note in
     * vfs_compound.c on why the handle cache is not shared between slots. */
    CHIMERA_VFS_COMPOUND_OP_SAVEFH,
    CHIMERA_VFS_COMPOUND_OP_RESTOREFH,
    /* The parent of the current object; the parent becomes current.  Expressed
     * as a lookup of ".." through the current object opened as a directory,
     * which is what resolves a parent on every backend -- the native ones
     * understand ".." directly, the passthroughs reach it through the open
     * directory.  (chimera_vfs_getparent is a different thing: it needs
     * CHIMERA_VFS_CAP_RPL and answers with a parent *and the name*, which is a
     * reverse-path-lookup service, not this.) */
    CHIMERA_VFS_COMPOUND_OP_LOOKUPP,
    /* Flush a byte range of the current object to stable storage. */
    CHIMERA_VFS_COMPOUND_OP_COMMIT,
    /* One page of the current object's directory entries. */
    CHIMERA_VFS_COMPOUND_OP_READDIR,
    /* Open (and optionally create) an object, which becomes current.  With a
     * `name`, the name is resolved in the current object, which is opened as a
     * directory; without one, the current object itself is opened.  MUTATES
     * when the flags say create -- see the MUTATION note above.
     *
     * Unlike every other op here, an OPEN produces a resource the caller keeps:
     * the open handle.  See OPEN HANDLE OWNERSHIP below. */
    CHIMERA_VFS_COMPOUND_OP_OPEN,
    /* Create `name` in the current object; the new object becomes current.
     * What is created is chosen by `create_type`, because the three shapes take
     * different arguments -- a directory takes only attributes, a node takes
     * its type and device numbers in the mode and rdev of those attributes, and
     * a symlink takes a target.  MUTATES.
     *
     * (An ordinary file is not one of them: a protocol creates one by OPENing
     * it, so that the create and the open it is for are a single act.) */
    CHIMERA_VFS_COMPOUND_OP_CREATE,
    /* Unlink `name` from the current object, which stays current.  MUTATES. */
    CHIMERA_VFS_COMPOUND_OP_REMOVE,
    /* Rename `name` in the SAVED object to `new_name` in the current object.
     * The current object stays current.  MUTATES.
     *
     * The only two ops here that address the saved slot as well as the current
     * one, and they do it because the operations themselves take two
     * directories.  That is not a special case invented for the compound: it
     * is how NFSv4 already spells them -- RENAME takes its source directory
     * from the saved filehandle and its target from the current one, LINK its
     * source object from saved and its target directory from current -- so a
     * caller that has SAVEFH'd where it came from has already said everything
     * these need.
     *
     * Neither opens anything.  rename_at and link_at take file handles, and
     * the saved slot holds a file handle (see SAVEFH), so the pair costs no
     * open that the op-at-a-time path would not also have paid.
     *
     * RENAME reports BOTH directories' change_info -- from_dir_* for the saved
     * one and dir_* for the current one -- because rename_at hands back both
     * and a protocol reply has a slot for each. */
    CHIMERA_VFS_COMPOUND_OP_RENAME,
    /* Link the SAVED object into the current object as `name`.  The current
     * object stays current.  MUTATES.  See RENAME for why these two read the
     * saved slot. */
    CHIMERA_VFS_COMPOUND_OP_LINK,
    /* Read from the current object, or from `in_handle`.  The data comes back
     * as iovecs referencing the backend's own buffers rather than a copy, so
     * they carry references and are owned the way an OPEN's handle is -- see
     * OPEN HANDLE OWNERSHIP.
     *
     * With `dest_iov` set the read lands in the CALLER'S buffers instead
     * (chimera_vfs_read_into): the op's iov then references those, which the
     * caller owned all along and releases itself, and the compound owns
     * nothing.  One op, a pair of fields selecting the underlying call -- the
     * shape ALLOCATE's flags and CREATE's type already use -- rather than a
     * READ_INTO op duplicating READ's whole result surface for one field. */
    CHIMERA_VFS_COMPOUND_OP_READ,
    /* Write to the current object, or to `in_handle`.  The data iovecs are
     * BORROWED, like in_handle: the caller owns the buffers and releases them
     * once the sequence is over.  MUTATES. */
    CHIMERA_VFS_COMPOUND_OP_WRITE,
    /* Apply `set_attr`.  Through a handle the CALLER named -- an `in_handle`,
     * or the handle an earlier op produced when chimera_vfs_compound_op_use_
     * handle names it -- the attributes are applied with descriptor rights: the
     * ftruncate(2) rule, where that open's own grant authorizes the change
     * rather than the object's current mode.  That is why a protocol which
     * resolved a handle for itself hands it in rather than letting this re-open
     * by name, and it holds for the handle an OPEN in the SAME sequence just
     * produced: the truncate behind an SMB2 CREATE with FILE_OVERWRITE, or
     * behind an NFSv4 OPEN of an existing file with a size in its attributes,
     * is authorized by the open the client was just granted -- a file it may
     * write but whose mode forbids writing is exactly the case the two rules
     * disagree about.
     *
     * A PATH open is the exception, and POSIX makes the same one: an O_PATH
     * descriptor cannot ftruncate or futimens.  It was opened to REACH the
     * object and asked for no access, so a handle an OPEN_PATH (or any other
     * PATH open) in this sequence produced carries no rights to ride and the
     * object's mode decides -- which is what a path-addressed truncate(2) or
     * utimensat(2) means, and how a caller that reaches an object by path gets
     * the answer its own API owes.  A handle the caller LENT is taken at its
     * word whatever it is: the caller holds the descriptor and says what it is
     * for.
     *
     * Without either, the current object is opened and the attributes applied
     * against the object's mode.  MUTATES. */
    CHIMERA_VFS_COMPOUND_OP_SETATTR,
    /* Apply an admitted overwrite (SIZE=0), including base named-stream
     * removal. The caller retains its overwrite/share reservation through
     * acceptance; this is a filesystem mutation, not an input callback. */
    CHIMERA_VFS_COMPOUND_OP_OVERWRITE,
    /* Extended attributes of the current object.  SETXATTR and REMOVEXATTR
     * MUTATE -- see the MUTATION note above. */
    CHIMERA_VFS_COMPOUND_OP_GETXATTR,
    CHIMERA_VFS_COMPOUND_OP_SETXATTR,
    CHIMERA_VFS_COMPOUND_OP_LISTXATTRS,
    CHIMERA_VFS_COMPOUND_OP_REMOVEXATTR,
    CHIMERA_VFS_COMPOUND_OP_OPEN_STREAM,
    CHIMERA_VFS_COMPOUND_OP_LIST_STREAMS,
    CHIMERA_VFS_COMPOUND_OP_REMOVE_STREAM,
    CHIMERA_VFS_COMPOUND_OP_ALLOCATE,
    CHIMERA_VFS_COMPOUND_OP_SEEK,
    CHIMERA_VFS_COMPOUND_OP_COPY_RANGE,
    CHIMERA_VFS_COMPOUND_OP_CLONE_RANGE,
    CHIMERA_VFS_COMPOUND_OP_MOVE_RANGE,
    CHIMERA_VFS_COMPOUND_OP_WRITE_SAME,
    CHIMERA_VFS_COMPOUND_OP_READ_PLUS,
    /* Byte-range locks BECAME the general claim op.  A CLAIM carries any
     * claim the caller's chimera_vfs_claim_init_* built -- a byte range, an
     * SMB open share, an NFSv4 open share, a deny probe, or a caching grant
     * (delegation / lease / oplock / directory lease / FUSE grant) -- and
     * takes it against the current open handle (locks and grants are
     * arbitrated per FILE, so the object is the one the current open refers
     * to).  CLAIM_TEST asks whether it WOULD be granted, changing nothing.
     *
     * RELEASING A CLAIM IS STILL OUT OF BAND (chimera_vfs_claim_release
     * [_ranged], grant_release, ack, revoke, shrink): none addresses the
     * cursors or mutates what the run holds, and a release pumps waiters and
     * is not reversible, so it must not sit behind an op that can fail.
     * What a run DOES undo is a claim its OWN CLAIM op inserted, when a later
     * op fails -- the abort release, safe because an acquire that handed
     * nobody anything blocked others for a while and gave them nothing to
     * act on.
     *
     * ROUTING, BY CONSTRUCT CLASS.  The executor neither builds nor inspects
     * the claim beyond its construct.  The SMB coalition constructs -- RQLS,
     * OPLOCK_II/EX/BATCH, DIR_LEASE, whose standing claim is the core-
     * allocated grant N opens share -- go through
     * chimera_vfs_claim_grant_settle: one call that coalesces onto the
     * owner's existing grant, caps the mode to what is grantable without
     * breaking a peer, and acquires, stepping CW -> CR|H -> CR on a residual
     * conflict (the loop SMB's create path ran by hand).  The result is the
     * grant in `claim_grant`, and the claim the caller passed was only ever a
     * TEMPLATE for it.  Every other construct -- a range, a share, a deny
     * probe, and the single-holder caches (a delegation, a FUSE grant),
     * whose claim struct IS the standing claim exactly as their consumers
     * hold it today -- goes through chimera_vfs_claim_acquire, and the
     * caller's claim is what gets inserted.
     *
     * TRIGGERS.  Three trigger words ride on the op: `pre` fires
     * (chimera_vfs_claim_invalidate) before the admission attempt -- SMB's
     * phase-1 OPEN_H / OPEN_H_FORCE handle break; `post` after GRANTED --
     * SMB's phase-2 OPEN_W break between the share grant and the cache
     * grant; `deny` on a SYNCHRONOUS denial only, never for a ticket that
     * queued and later answered DENIED.  The actor is derived from the
     * claim: {claim->owner, claim->op_handle}.  That expresses the KEY-circle
     * self-exemption directly, because the trigger engine keys it on
     * owner.key (chimera_claim_owner_same_lease: key + client), which the
     * caller's claim already carries -- SMB stamps the LeaseKey on the share
     * claim's owner exactly so.  The lo/hi rewrite SMB's own break_for_open
     * does is redundant for exemption (same_key covers it wherever same_owner
     * would have), so there is no separate actor argument.
     *
     * OP_HANDLE.  Before the acquire, a CACHE-class claim (a grant template,
     * a delegation, a FUSE grant) whose op_handle is NULL is stamped with the
     * target open handle, so a claim built before the handle existed (a
     * single-run CREATE) still anchors the HOLDER circle and a metadata op
     * through that handle does not recall it against itself.  The stamp is
     * the handle the op ran against; a caller whose claim outlives the run
     * and needs a durable anchor lends the handle (PUTHANDLE) or stamps
     * op_handle itself, since a handle the executor opened for the op is
     * released with the run and only ever compared by address afterwards.
     * A range claim, a share claim, a deny probe, and a CLAIM_TEST probe are
     * NEVER stamped: open handles are cached per (fh, access mode, cred) and
     * shared, so a stamp there would make every lock-owner or open-owner
     * using that handle one holder to the OWNER circle -- a LOCKT from a
     * second lock-owner through the same open would see no conflict, two
     * open-owners' share reservations would not deny each other.  SMB
     * stamps its own share claim today, knowing its handles; a consumer
     * that wants that anchoring stamps it itself.
     *
     * A consequence for NFSv4 (verified against chimera_vfs_claim_deny_rows):
     * a cache grant is exempt from the requester's OWN share claim by
     * construction for SMB -- cache bits never intersect a share's R|W|D
     * deny mask, and the sole-opener rule skips the requester's own client
     * (legacy oplock) or any keyed open (lease) -- so OPEN -> CLAIM(share)
     * -> CLAIM(grant) in one run does not self-conflict.  A delegation
     * carries the data bits too (R11), so it IS blocked by its own open's
     * deny bits unless the two are one holder (the share deny row's OWNER
     * circle: owner_equal, or same op_handle).  NFSv4's share owner
     * (open-owner hash) and delegation owner (fh hash) differ, and the share
     * is not stamped, so a deny-carrying NFSv4 OPEN's own delegation is
     * refused -- exactly as on the per-op path today.  A consumer that
     * wants it granted shrinks the deny out of band before the grant, or
     * aligns the two identities itself.  A deny-free open (the common case)
     * never blocks its own delegation.
     *
     * THREADS.  A CLAIM that waits is answered from whatever thread releases
     * the blocker -- the claim core's pump runs where the release ran, and
     * that is some other protocol's thread as often as not.  The promise that
     * the completion fires on the submitting thread holds regardless: an
     * answer that arrives inside the acquire call continues the sequence
     * inline, and any later answer is marshalled back to the submitting
     * thread through the core's own resume doorbell before the sequence goes
     * on -- even an answer that happens to land on the submitting thread,
     * because a deferred grant runs inside another consumer's release call
     * and under its locks.  Nothing behind a CLAIM, and no completion, ever
     * runs anywhere but on the thread that submitted.  A CLAIM_TEST that
     * projects to a backend arbiter (TEST_BACKEND) comes home the same way.
     *
     * RELEASE AND TRANSFER, for every kind.  A claim a CLAIM inserts belongs
     * to the SEQUENCE until the sequence is over, and then to exactly one of
     * two owners:
     *
     *   the sequence FINISHES OK  -> the claim is the caller's from the
     *     completion callback on.  The caller takes the op's file state with
     *     chimera_vfs_compound_take_file_state() -- it is what a later
     *     release needs -- and releases the claim when it is done: a range
     *     with chimera_vfs_claim_release_ranged, a share or single-holder
     *     cache with chimera_vfs_claim_release, a coalition grant with
     *     chimera_vfs_claim_grant_release on `claim_grant`.  The executor
     *     never touches that claim again: not on chimera_vfs_compound_free(),
     *     which only puts a file state the caller left behind, and never by
     *     releasing behind a caller that may already have told its client
     *     the claim is held.
     *
     *   the sequence FINISHES WITH ANY OTHER STATUS -> the executor releases
     *     every claim a CLAIM in it inserted, each by its own kind's release,
     *     and puts their file states, before the completion callback, so the
     *     caller sees a failed sequence with nothing inserted.  A later op
     *     failing, a veto from the gate (on the CLAIM itself or on anything
     *     after it), a refused CLAIM behind a granted one: all of these.  The
     *     CLAIM op keeps its own status and claim_result -- it ran, and the
     *     arbiter did say GRANTED -- and what says the claim is gone is that
     *     take_file_state() answers NULL (and claim_grant is NULL).
     *
     *   A CLAIM that did not reach GRANTED inserted nothing and owns
     *     nothing: its file state is put in the op's callback.
     *
     * There is no third outcome.  A sequence cannot be torn down from
     * outside, and the one way it stops early that is not an op failing --
     * chimera_vfs_compound_cancel on a parked run -- is the second case
     * above: the run finishes CHIMERA_VFS_ECANCELED, and the claims the
     * CLAIMs before the parked one inserted are released with it.  The
     * cancelled CLAIM itself never reached GRANTED, so it is the third case
     * and has nothing to release.
     *
     * PARKING AND CANCEL.  A CLAIM that waits parks the run, and that park is
     * one of the two the executor reports through the park callback and can
     * take back through chimera_vfs_compound_cancel.  A ticket that queued is
     * cancellable until the moment the claim core answers it; which of the
     * two wins is the core's own arbitration, never a guess -- see the cancel
     * call's contract. */
    CHIMERA_VFS_COMPOUND_OP_CLAIM_TEST,
    CHIMERA_VFS_COMPOUND_OP_CLAIM,
    /* Create an anonymous, unlinked object in the directory the current FILE
     * handle names; the new object's open handle becomes the current OPEN.
     * The current FILE handle does NOT move -- an unlinked object has no name
     * to make current, and the directory is where the caller will LINK it
     * once it is written (the S3 PUT shape: create, write, publish).  So the
     * op's `fh` result is still the directory's; the object's own is in its
     * attr.va_fh and on the handle.  The mirror of an unnamed OPEN that also
     * creates: `created` is set, `attr` describes the object, and out_handle
     * carries the handle on OPEN's ownership terms.  MUTATES.
     *
     * Because the two cursors then name different objects, the handle is
     * held to the LENT-handle rule: an op it does not serve (a READ behind a
     * WRITE_ONLY create) fails EINVAL, where the executor would otherwise
     * re-open the current fh in its place -- which here is the directory,
     * not the object the caller wrote.  The name-resolving ops are the
     * exception and re-open the directory as usual, so a LOOKUP or a
     * second CREATE_UNLINKED can follow in the same sequence.
     *
     * The directory is opened as a directory first, exactly as a named OPEN
     * opens it, and whether a non-directory is refused there is the
     * backend's to decide, as it is for the named OPEN: a backend whose PATH
     * open checks the type answers ENOTDIR before any create; memfs, whose
     * PATH open does not and whose create takes the fh only to find the
     * filesystem, creates the object regardless.  A backend without
     * CHIMERA_VFS_CAP_CREATE_UNLINKED fails the op ENOTSUP -- the executor
     * checks, because the per-op call ABORTS on such a backend rather than
     * reporting -- and the caller falls back to a temp-name OPEN and a
     * RENAME, as S3 does today. */
    CHIMERA_VFS_COMPOUND_OP_CREATE_UNLINKED,
    /* Named-stream (SMB ADS, NFSv4 named attribute) ops on the base file the
     * current OPEN handle refers to.  Each addresses the base on the ordinary
     * rules for an op that acts on an object: an `in_handle` first, then the
     * handle an earlier op produced when chimera_vfs_compound_op_use_handle
     * names it -- the shape SMB issues, OPEN(base name) then OPEN_STREAM with
     * use_handle(base), because a named OPEN's handle sits in its out_handle
     * and not on the cursor -- and otherwise the current open, or a PATH open
     * of the current file handle when there is none, which is what every
     * per-op consumer opens the base with today.
     *
     * OPEN_STREAM opens (and per CHIMERA_VFS_OPEN_CREATE / EXCLUSIVE /
     * TRUNCATE in `stream_flags`, creates or truncates) the fork named
     * `name`, and the STREAM becomes the current object in both cursors: its
     * fh is the current file handle and its handle the current open, the
     * base's handle having been released on the ordinary cursor rules.  That
     * is what an NFSv4 LOOKUP inside an OPENATTR directory means, and what an
     * SMB stream CREATE keeps.  `attr` is what chimera_vfs_open_stream
     * reports, stored as every other attribute result is: the base's metadata
     * (mode, owner, times, DOS attributes -- what SMB reads for granted and
     * maximal access) with the fork's own size and allocation, and the
     * stream's fh.  `created` says whether this open made the fork, and
     * out_handle carries the handle on OPEN's terms.  MUTATES when the flags
     * say create or truncate.
     *
     * LIST_STREAMS is a LISTXATTRS-shaped page into op->buffer -- the record
     * layout is on the adder.  REMOVE_STREAM removes one named fork; the base
     * stays current.  MUTATES.
     *
     * All three are gated by CHIMERA_VFS_CAP_NAMED_STREAMS exactly as the
     * per-op calls are: a backend without it fails the op ENOTSUP.  What a
     * backend WITH it refuses -- a stream on a symlink, say -- comes back as
     * that backend's own status. */



    /* pNFS: where the current object's data lives.  Asks the backend for up
     * to `layout_max_segments` segments covering [layout_offset, +length)
     * at `layout_iomode`, in `layout_class`, and COPIES what comes back --
     * the segments and the devices they name -- into arrays the compound
     * owns (layout_segments / layout_devices, freed with the compound),
     * because the backend's are valid only while its callback runs.  The
     * object is opened for data, as LAYOUTGET opens it today.
     *
     * Only a backend that SOURCES layouts (CHIMERA_VFS_CAP_LAYOUT_SOURCE --
     * diskfs) can answer; one that is merely orchestrated under a layout
     * (CAP_LAYOUT -- memfs) never sees the op and the per-op call reports
     * ENOTSUP for it, which the op passes through.  The NFS server routes
     * on that distinction before it asks, so a caller that wants the
     * orchestrated flex-files layout asks a GETATTR for
     * CHIMERA_VFS_ATTR_PNFS_LAYOUT instead. */
    CHIMERA_VFS_COMPOUND_OP_GET_LAYOUT,
    /* Recursive streaming walk of the directory tree rooted at the current
     * object, which is opened as a directory the way READDIR opens it and,
     * before that, established to BE one: a FIND through anything else is
     * ENOTDIR on every backend, rather than the empty walk a backend whose
     * PATH open does not check the type would otherwise produce (the
     * walker swallows its readdir's refusal).  Every entry below the root
     * reaches `find_append` as it is found; every DIRECTORY entry is first
     * put to `find_filter`, which answers 0 to descend into it and non-zero
     * to prune it.  Both callbacks are REVERSIBLE on READDIR's terms, and
     * `find_reset` runs before every execution -- see the typedefs.  The
     * state machine is chimera_vfs_find's; a caller never grows its own,
     * and nothing is ever staged. */
    CHIMERA_VFS_COMPOUND_OP_FIND,
    /* Recall the caching leases on an object -- the current one, or the
     * bare fh set on the op -- and by default PARK until the recall drains:
     * an op with no backend mutation, the recall_handle_lease /
     * recall_caching_fh pair unified.  With CHIMERA_VFS_COMPOUND_RECALL_
     * NOWAIT it kicks the recall and reports without parking.  Either way
     * the op reports `recall_still_open`: a holder still stands in the way
     * when the op answers.  See the adder for what each shape is. */
    CHIMERA_VFS_COMPOUND_OP_RECALL,
    /* Path-addressed.  See the note on ->path. */
    CHIMERA_VFS_COMPOUND_OP_LOOKUP_PATH,
    CHIMERA_VFS_COMPOUND_OP_OPEN_PATH,


    CHIMERA_VFS_COMPOUND_OP_REMOVE_PATHS,
    CHIMERA_VFS_COMPOUND_OP_RESERVE,
    CHIMERA_VFS_COMPOUND_OP_RESERVE_ACCESS,
    CHIMERA_VFS_COMPOUND_OP_RETIRE_ACCESS,
    CHIMERA_VFS_COMPOUND_OP_NARROW_ACCESS,
    CHIMERA_VFS_COMPOUND_OP_RETIRE_OPEN_CLAIMS,
    CHIMERA_VFS_COMPOUND_OP_RANGE_BATCH,
    CHIMERA_VFS_COMPOUND_OP_RANGE_OWNER,
    CHIMERA_VFS_COMPOUND_OP_LOCK_TEST,
    CHIMERA_VFS_COMPOUND_OP_LOCK_CHANGE,
    CHIMERA_VFS_COMPOUND_OP_LOCK_RELEASE_OWNER,
    CHIMERA_VFS_COMPOUND_OP_CREATE_PATH,
    CHIMERA_VFS_COMPOUND_OP_REMOVE_PATH,
    CHIMERA_VFS_COMPOUND_OP_RENAME_PATH,
    CHIMERA_VFS_COMPOUND_OP_LINK_PATH,
    CHIMERA_VFS_COMPOUND_OP_PUTHANDLE,
    /* The cursor operations of the four-cursor model.  See ADDRESSING. */
    CHIMERA_VFS_COMPOUND_OP_PUTROOT,
    CHIMERA_VFS_COMPOUND_OP_OPEN_CURRENT,
    CHIMERA_VFS_COMPOUND_OP_GETHANDLE,
    CHIMERA_VFS_COMPOUND_OP_CLOSE,
    CHIMERA_VFS_COMPOUND_OP_SAVEHANDLE,
    CHIMERA_VFS_COMPOUND_OP_RESTOREHANDLE,
    /* Mutating handle-state records routed through the current object's FH. */
    CHIMERA_VFS_COMPOUND_OP_PUT_KEY_AT,
    CHIMERA_VFS_COMPOUND_OP_DELETE_KEY_AT,
    CHIMERA_VFS_COMPOUND_OP_SEARCH_KEYS_AT,
};

#define CHIMERA_VFS_COMPOUND_MAX_OPS  1024
#define CHIMERA_VFS_COMPOUND_NAME_MAX 255

/* Key/value bytes are copied at construction. Both operations preserve the
 * cursor and use its FH to select the same backend as the *_key_at API. */
int chimera_vfs_compound_add_put_key_at(
    struct chimera_vfs_compound *compound,
    const void                  *key,
    uint32_t                     key_len,
    const void                  *value,
    uint32_t                     value_len);
int chimera_vfs_compound_add_delete_key_at(
    struct chimera_vfs_compound *compound,
    const void                  *key,
    uint32_t                     key_len);

/* Bounded FH-routed search. Start is inclusive; end follows SEARCH_KEYS flags.
 * Inputs are copied. Results own their binary bytes until reset/free. kv_more
 * supplies the first unconsumed key for the next page's inclusive start; build
 * that page before freeing the prior result. Result payload is bounded by
 * max_bytes (<=16 MiB), entry array by max_entries (<=65536), with at most one
 * continuation key of <=4096 bytes. A first row exceeding the payload budget
 * returns ERANGE, ensuring pagination cannot loop without progress. Backend
 * scan/snapshot allocation is outside this frontend result budget. */
struct chimera_vfs_compound_kv_entry {
    uint8_t *key, *value;
    uint32_t key_len, value_len;
};
int chimera_vfs_compound_add_search_keys_at(
    struct chimera_vfs_compound *compound,
    const void                  *start_key,
    uint32_t                     start_len,
    const void                  *end_key,
    uint32_t                     end_len,
    uint32_t                     flags,
    uint32_t                     max_entries,
    uint32_t                     max_bytes);

/*
 * A READDIR's result is a page, not the whole directory: the caller says how
 * many entries it can use, the executor keeps that many and stops the
 * enumeration there -- reporting eof=0 and, in r_cookie, the cookie of the
 * entry it refused, which is where the backend says it stopped.  A cookie
 * names the entry it belongs to and a READDIR from it returns what FOLLOWS, so
 * a caller wanting more resumes from the cookie of the last entry it TOOK, not
 * from r_cookie -- resuming from r_cookie would skip the refused entry.  Every
 * protocol already does this, because every wire format carries a cookie per
 * entry; r_cookie is the right place to resume from only when the enumeration
 * stopped of its own accord (eof set).
 *
 * The budget is a count rather than a byte size because the entries are fixed
 * size (an inline name plus a copied attribute set, ~730 bytes each): a caller
 * whose own limit is in bytes divides by the least an entry can cost it, which
 * is the only bound it can compute before it has marshalled anything.
 *
 * CHIMERA_VFS_COMPOUND_READDIR_MAX_ENTRIES is the ceiling on that budget, and
 * so on what one page can cost: ~370KB at the ceiling, but allocated only for a
 * READDIR and only for as many entries as that READDIR asked for -- a caller
 * whose reply holds 32 entries pays for 32.
 */
#define CHIMERA_VFS_COMPOUND_READDIR_MAX_ENTRIES 512

/* What a CREATE makes. */
enum chimera_vfs_compound_create_type {
    CHIMERA_VFS_COMPOUND_CREATE_DIR = 0,
    /* Device, socket or FIFO.  Which one is carried in set_attr's va_mode, and
     * a device's numbers in its va_rdev -- the same way the underlying mknod
     * takes them, so there is nothing here to translate. */
    CHIMERA_VFS_COMPOUND_CREATE_NODE,
    CHIMERA_VFS_COMPOUND_CREATE_SYMLINK,
    /* CREATE_PATH only: materialize the entire directory chain. */
    CHIMERA_VFS_COMPOUND_CREATE_DIR_TREE,
};

/*
 * OPEN options.  These express the two things a protocol open wants that a
 * bare open_at does not do, and that a caller would otherwise have to get by
 * looking the name up itself first -- which is exactly what NFS4's OPEN did
 * before this op existed, at the cost of being back in the middle of its own
 * sequence between the lookup and the open.
 *
 * The executor implements both by resolving the name once before the open,
 * which is the same two steps the caller used to take; the point is that they
 * are now on this side of the submission.  Pushing the type gate down into the
 * backends' open paths would save that resolve and close the window between it
 * and the open -- CHIMERA_VFS_OPEN_CREATE_REGULAR is the same idea for the
 * create path -- but it is a change to every backend, and separable from this.
 */
/* Refuse a non-regular object rather than opening it: a native open of a FIFO,
 * socket or device can block or report a backend-specific errno where a
 * protocol wants to answer for the type.  The op fails, and `existing_mode`
 * carries the mode so the caller can say what it wants about it. */
#define CHIMERA_VFS_COMPOUND_OPEN_REGULAR_ONLY         (1U << 0)
/* Apply `set_attr` only if the open actually creates the object.  An open that
 * finds an existing one leaves it alone.  (NFS4 UNCHECKED4 and NFS3 UNCHECKED
 * both mean this: the create attributes describe a creation, not an open.) */
#define CHIMERA_VFS_COMPOUND_OPEN_ATTRS_ON_CREATE_ONLY (1U << 1)
/* An exclusive create that collides opens what is already there instead of
 * failing, reporting `existed`.  The caller decides what the collision means --
 * NFS4's EXCLUSIVE4 compares a verifier stamped in the object's timestamps to
 * tell its own earlier create from somebody else's file, which it can only do
 * with the object open and its attributes in hand.  The re-open applies no
 * attributes and takes no data-access intent; it exists to look, and the
 * caller closes or keeps it. */
#define CHIMERA_VFS_COMPOUND_OPEN_EXCLUSIVE_RETRY      (1U << 2)

/*
 * Streaming READDIR: the caller marshals each entry as it arrives, instead of
 * the sequence staging a copy of every entry for the caller to walk afterwards.
 *
 * WHY THIS AND NOT IDEMPOTENCE.  Everything else a caller does during a
 * sequence has to be answerable from memory and repeatable, because a retried
 * sequence asks again.  Marshalling cannot be either: it writes, and it writes
 * different bytes the second time if the directory changed underneath.  So the
 * requirement here is not that appending be repeatable but that it be
 * REVERSIBLE -- `reset` must leave the caller exactly as it was before the
 * first entry, at which point re-running is a first run.
 *
 * The executor calls `reset` immediately before every execution of the op, not
 * only before a retry, so a caller never has to know which one it is in: the
 * first call resets nothing and the cost is a cursor assignment.
 *
 * `append` returns 0 to take the entry and -1 to stop the enumeration there,
 * exactly as the per-op entry callback does -- so a caller bounded in bytes
 * stops on the entry that does not fit rather than guessing a count up front.
 * The page then ends at that entry's cookie with eof clear.
 *
 * The attributes append receives are the backend's own, not a copy: an ACL or
 * SID among them (when attr_mask asked) is live for the duration of the call
 * and no longer, which is where every consumer that marshals per-entry ACLs
 * marshals them.  The staged variant drops them instead -- see the dirent.
 *
 * WHAT APPEND MAY NOT DO is anything reset cannot take back -- above all,
 * emit.  A caller that writes to its own reply buffer and sends only when the
 * sequence has finished is fine; one that streams onto a socket is not.
 */
typedef void (*chimera_vfs_compound_readdir_reset_t)(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    void                        *private_data);

typedef int (*chimera_vfs_compound_readdir_append_t)(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *private_data);

/*
 * Streaming FIND: the recursive walk, on the same terms as the streaming
 * READDIR above -- `reset` before every execution, `append` per entry,
 * append must not emit and reset must take back everything it did.
 *
 * WHAT THE WALK HANDS OVER.  Both callbacks receive the entry's path RELATIVE
 * TO THE ROOT, as chimera_vfs_find reports it: each component preceded by a
 * '/', so the root's own children are "/name" and their children
 * "/name/child"; the root itself is never an entry, and neither is "." or
 * "..".  The attributes are what the caller's attr_mask asked for, with the
 * fh and the mode always present (the walk needs both to descend), handed
 * over as the walker reports them: an ACL or SID among them is the backend's
 * own, live for the duration of the callback and no longer -- the streaming
 * READDIR rule, since nothing here is staged.  Entries arrive in the
 * backend's readdir order, a directory's own entry BEFORE anything below it.
 *
 * `filter` is asked only for a DIRECTORY entry, before that entry's own
 * append: 0 descends into it, non-zero prunes the subtree below it.  The
 * directory entry itself is still appended either way -- pruning is about
 * what is walked, not what is reported, and the S3 consumers decide per entry
 * whether a directory is an object or pure traversal.
 *
 * `append` returns 0 to take the entry and -1 to stop.  chimera_vfs_find has
 * no way to abandon a walk in flight, so a stop is expressed on this side:
 * from that entry on, nothing more is appended and every filter prunes, and
 * the walker drains what it had already dispatched.  The op then reports OK
 * with `eof` CLEAR, exactly as a refused READDIR entry leaves the page; a walk
 * that ran out of entries reports eof SET.  What a refusal bounds is what the
 * caller stages, not what the backend reads.
 */
typedef int (*chimera_vfs_compound_find_filter_t)(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *attr,
    void                           *private_data);

typedef int (*chimera_vfs_compound_find_append_t)(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *attr,
    void                           *private_data);

struct chimera_vfs_compound_dirent {
    uint64_t                 inum;
    uint64_t                 cookie;
    uint32_t                 name_len;
    char                     name[CHIMERA_VFS_COMPOUND_NAME_MAX + 1];
    /* The ONE attribute result that does not carry the ACL or the SIDs: here
     * va_acl, va_owner_sid and va_group_sid are NULL and their bits clear,
     * whatever the READDIR asked for.  A staged entry is a fixed-size record
     * -- that is what lets a page be budgeted by count (see the READDIR page
     * note) -- and an ACL is not: up to CHIMERA_ACL_MAX_ACES of 88 bytes,
     * per entry, times a page.  Every consumer that marshals per-entry ACLs
     * (NFSv4 READDIR, SMB's access-based enumeration) does so inside the
     * enumeration, and the streaming READDIR is where that happens: its
     * append is handed the backend's live attributes, ACL included, for the
     * duration of the call. */
    struct chimera_vfs_attrs attr;
};

/* Execution-time decisions belong to the operation that needs them. Prepare
 * runs once before any VFS work for the operation; complete runs with its
 * results before a following operation may execute. Both callbacks may update
 * only attempt-private state and the supplied status. They must not publish
 * protocol state, consume request data, send replies, perform I/O, or wait.
 * The original arguments and callback context must survive retries. A retry
 * can produce different results; decisions are recomputed from those results.
 * A complete callback may normalize an optional error to OK. */
typedef void (*chimera_vfs_compound_op_callback_t)(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data);

typedef void (*chimera_vfs_compound_attempt_reset_t)(
    struct chimera_vfs_compound *compound,
    void                        *private_data);

/* Unlike pure operation callbacks, this explicit operation may initiate
 * external protocol coordination. The executor parks until coordinate_done;
 * completion may be inline, but must run on the compound's owning VFS thread.
 * By default it is called at most once per original operation and resolved
 * current FH. coordinate_each_attempt is reserved for repeatable cache-input
 * resolution (e.g. principals read from current attributes), never one-shot
 * protocol notifications. Such resolution runs again after finish rejection.
 * FH bytes and token belong to the compound and remain valid until free.
 * Coordination is independent of a backend transaction: do not publish the
 * request's result, mutate the filesystem, or publish tentative protocol state.
 * Pure prepare/complete checks still run on every attempt and must reject a
 * new blocker that appeared after coordination. Frontend cached data must be
 * keyed by these FH bytes and live outside its resettable attempt state. */
typedef void (*chimera_vfs_compound_coordinate_t)(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    uint64_t                     token,
    const uint8_t               *fh,
    uint32_t                     fh_len,
    void                        *private_data);

/* FIND traverses beneath the current FH. The filter/append callbacks may
 * inspect inputs and accumulate attempt-private results only. reset releases
 * those results before each traversal, including a retry. */
typedef int (*chimera_vfs_compound_find_entry_t)(
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *attr,
    void                           *private_data);

struct chimera_vfs_compound_op {
    uint8_t                                     type;
    /* CHIMERA_VFS_UNSET until the op has run. */
    enum chimera_vfs_error                      status;

    chimera_vfs_compound_op_callback_t          prepare;
    chimera_vfs_compound_op_callback_t          complete;
    void                                       *prepare_private;
    void                                       *callback_private;
    chimera_vfs_compound_coordinate_t           coordinate;
    void                                       *coordinate_private;
    uint8_t                                     coordinate_each_attempt;
    uint8_t                                     prepared;
    uint8_t                                     skipped;
    /* Completed execution/callouts, independent of physical operation order.
     * False for group/dependency-skipped operations (whose status is UNSET). */
    uint8_t                                     completed;
    /* Executor-owned link: dynamic group suffixes keep stable op indices. */
    int32_t                                     group_next;
    uint32_t                                    cancel_scope_end; /* construction, end index + 1 */
    uint32_t                                    access_ready; /* opt-in private reservation endpoint + 1 */

    chimera_vfs_compound_find_filter_t          find_stream_filter;
    chimera_vfs_compound_find_append_t          find_stream_append;
    chimera_vfs_compound_find_entry_t           find_filter;
    chimera_vfs_compound_find_entry_t           find_append;
    chimera_vfs_compound_readdir_reset_t        find_reset;
    void                                       *find_private;

    /* REMOVE_PATHS borrows a stable list of NUL-terminated paths for this
     * attempt. Each removal remains an ordinary VFS operation. */
    const char *const                          *remove_paths;
    uint32_t                                    remove_num_paths;
    uint32_t                                    remove_path_index;
    uint8_t                                     remove_ignore_errors;

    /* LINK namespace options. With NO_NOTIFY the frontend must emit the
     * accepted notification itself. io_owner.op_handle preserves the source
     * open's lease identity when have_io_owner is set. */
    uint32_t                                    namespace_flags;
    /* Borrowed immutable credential, bound by prepare if necessary. Applies
     * only to OPEN_CURRENT/REMOVE and the latter's implicit parent open. */
    const struct chimera_vfs_cred              *namespace_cred;
    uint8_t                                     remove_match_child_fh;
    uint8_t                                     remove_unmatched;
    uint8_t                                     namespace_parent_lease_key[16];
    uint8_t                                     namespace_parent_lease_key_valid;

    /* ---- arguments ---- */
    /* PUTFH input; REMOVE may also use these as the resolved child identity
     * for VFS lease recall and silly-rename handling. Zero length means none. */
    uint8_t                                     arg_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                                    arg_fh_len;
    char                                        name[CHIMERA_VFS_COMPOUND_NAME_MAX + 1];
    uint32_t                                    name_len;
    uint64_t                                    attr_mask;
    /* Optional protocol result masks; otherwise each builder's defaults apply. */
    uint8_t                                     result_masks_set;
    uint64_t                                    result_attr_mask;
    uint64_t                                    result_pre_attr_mask;
    uint64_t                                    result_post_attr_mask;
    uint32_t                                    requested;
    uint64_t                                    offset; /* COMMIT                             */
    uint64_t                                    count; /* COMMIT                             */
    uint64_t                                    cookie; /* READDIR, LISTXATTRS                */
    uint32_t                                    readdir_flags; /* CHIMERA_VFS_READDIR_* */
    uint64_t                                    verifier; /* READDIR                            */
    uint32_t                                    dircount; /* READDIR (advisory; see the adder)  */
    uint32_t                                    maxcount; /* READDIR (advisory; see the adder)  */
    uint32_t                                    max_entries; /* READDIR                            */
    /* READDIR, streaming variant: with these set the sequence stages nothing
     * and the caller marshals each entry itself.  See the typedefs above. */
    chimera_vfs_compound_readdir_reset_t        readdir_reset;
    chimera_vfs_compound_readdir_append_t       readdir_append;
    void                                       *readdir_private;
    /* READDIR: CHIMERA_VFS_READDIR_* and the caller's search pattern, which the
     * VFS core matches on the caller's behalf.  The pattern is BORROWED and is
     * re-matched on every execution, so it has to outlive a retry. */
    const char                                 *readdir_pattern;
    uint32_t                                    readdir_pattern_len;
    /* Address this handle instead of the current object.  BORROWED from the
     * caller -- see ADDRESSING SOMETHING OTHER THAN CURRENT above.  NULL for
     * every op that addresses the current object, which is most of them. */
    struct chimera_vfs_open_handle             *in_handle;
    uint8_t                                     create_type; /* CREATE                             */
    /* CREATE of a symlink: its target.  Copied by the adder and owned by the
     * compound, so the caller need not keep it alive. */
    char                                       *link_target;
    uint32_t                                    link_target_len;
    /* RENAME: the name in the CURRENT object to rename to.  `name` is the one
     * in the saved object to rename from. */
    char                                        new_name[CHIMERA_VFS_COMPOUND_NAME_MAX + 1];
    uint32_t                                    new_name_len;
    /* OPEN: the CHIMERA_VFS_OPEN_* word to open with -- and, for PUTHANDLE,
     * the word the caller says its LENT handle was opened with.
     *
     * THE ACCESS BITS ARE CAPABILITIES, NOT RESTRICTIONS, whatever their names
     * suggest: READ_ONLY means "this can read" and WRITE_ONLY "this can
     * write", so a read-write handle sets BOTH -- which is how O_RDWR is
     * spelled everywhere in the VFS and how chimera_vfs_open_access_mode reads
     * it back (READ_ONLY without WRITE_ONLY is the only thing that is a
     * read-only handle).  A caller that lends a read-write handle as READ_ONLY
     * alone is not describing a restriction, it is withholding a capability,
     * and the op that needs the withheld one fails.  See the serves rules on
     * PUTHANDLE. */
    unsigned int                                open_flags;
    /* CLOSE: CHIMERA_VFS_COMPOUND_CLOSE_* -- whether this close honours the
     * handle's delete-on-close.  See the adder. */
    unsigned int                                close_flags;
    /* REMOVE, RENAME: CHIMERA_VFS_REMOVE_* -- the type assertion and the
     * lease-recall request, which are the caller's to make. */
    unsigned int                                remove_flags;
    uint8_t                                     rename_target_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                                    rename_target_fh_len;
    enum chimera_vfs_rename_outcome             rename_outcome;
    uint32_t                                    open_opts; /* OPEN: CHIMERA_VFS_COMPOUND_OPEN_*  */
    /* Unnamed OPEN may inherit retained permissions from another authorized
     * open of the same FH. Borrowed and pinned through completion; prepare may
     * bind it to an earlier result. Only OPEN-bound grants are inherited;
     * a lazy stateless access-cache entry never becomes permanent this way.
     * The executor combines these rights with those already on the result. */
    const struct chimera_vfs_open_handle       *inherited_grant_handle;
    /* Second authorized grant source, e.g. the newly requested OPEN when the
     * union reopen instead selects the older write-capable cache entry. */
    const struct chimera_vfs_open_handle       *inherited_grant_handle2;
    /* Immutable OPEN/CREATE/SETATTR input. Scalars are copied by the builder;
     * va_acl is borrowed and must remain valid through completion/retries.
     * ATTRS_ON_CREATE_ONLY clears only the execution copy below. */
    struct chimera_vfs_attrs                    set_attr;
    /* Execution copy: resolving an existing name must not erase the original
     * create attributes needed by a subsequent attempt. */
    struct chimera_vfs_attrs                    applied_attr;
    /* Attempt-local anonymous admission; excluded claims remain pinned by
    * the frontend. Owned actors still use io_owner/src_io_owner below. */
    struct chimera_vfs_io_view                  io_view;
    struct chimera_vfs_io_view                  src_io_view;
    /* Writable ACL supplied to this attempt. set_attr.va_acl remains the
     * caller's immutable input for the lifetime of the compound. */
    struct chimera_acl                         *applied_acl;
    /* Metadata publication for an already-authorized data write. */
    uint8_t                                     setattr_after_write;
    /* A PATH-ADDRESSED op resolves this, relative to the sequence's current
     * file handle, instead of addressing the current object.  Owned by the
     * compound and copied by the adder. OPEN-at also stores its potentially
     * longer name here, but resolves against the current open directory.
     *
     * This is the only way to reach an object on a path-only mount, where a
     * child's handle is an opaque per-open token that open_fh cannot reopen --
     * so the current object there can only ever be the mount root, and every
     * operation has to name its target by path from it.  It is also one call
     * where chaining would be several, on every backend. */
    char                                       *path;
    uint32_t                                    path_len;
    /* RENAME and LINK name two paths; this is the destination. */
    char                                       *new_path;
    uint32_t                                    new_path_len;
    /* Address the handle that op `handle_from` produced, rather than the
     * current object -- for the op after a path OPEN, whose result is the only
     * usable reference to an object a path-only mount will not reopen.  -1
     * when unused, which the adders leave it as. */
    int                                         handle_from;
    /* Range endpoints are borrowed: in_handle is the destination and
    * src_handle the source. Prepare may bind either from prior owned results.
    * If omitted, execution uses the saved/current open cursors; both handles
    * must be available before dispatch. These are distinct from SAVEFH's
    * saved filehandle, which does not itself keep an open handle. */
    struct chimera_vfs_open_handle             *src_handle;
    uint64_t                                    src_offset;
    uint32_t                                    copy_flags; /* COPY_RANGE                        */
    /* WRITE_SAME: the pattern is BORROWED, like a WRITE's payload. */
    uint32_t                                    block_size;
    uint64_t                                    block_count;
    const void                                 *pattern;
    uint32_t                                    pattern_len;
    uint32_t                                    reloff_pattern;
    uint32_t                                    allocate_flags; /* ALLOCATE: CHIMERA_VFS_ALLOCATE_* */
    uint64_t                                    length; /* ALLOCATE                           */
    /* ALLOCATE: the attributes to fetch after the change.  `attr_mask` is the
     * pre-change one, as it is for COMMIT. */
    uint64_t                                    post_attr_mask;
    uint32_t                                    seek_what; /* SEEK: data (0) or hole (1)         */
    uint32_t                                    xattr_option; /* SETXATTR                          */
    const void                                 *xattr_value; /* SETXATTR (borrowed from caller)    */
    uint32_t                                    xattr_value_len;
    /* Immutable binary inputs copied by the KV builders, retained on retry. */
    uint8_t                                    *kv_key;
    uint8_t                                    *kv_value;
    uint32_t                                    kv_key_len;
    uint32_t                                    kv_value_len;
    uint32_t                                    kv_flags, kv_max_entries, kv_max_bytes;
    struct chimera_vfs_compound_kv_entry       *kv_entries;
    uint32_t                                    kv_num_entries, kv_result_bytes;
    uint8_t                                    *kv_next_key;
    uint32_t                                    kv_next_key_len;
    bool                                        kv_more;
    enum chimera_vfs_error                      kv_error;
    uint32_t                                    buffer_max; /* GETXATTR, LISTXATTRS               */
    uint8_t                                     stream_want_fh;
    int                                         max_iov; /* READ                               */
    /* WRITE: the data, BORROWED from the caller -- see ADDRESSING SOMETHING
     * OTHER THAN CURRENT, which these are owned on the same terms as. */
    struct evpl_iovec                          *w_iov;
    int                                         w_niov;
    uint32_t                                    sync; /* WRITE: requested stability         */
    /* READ, WRITE: whose I/O this is.  A caller holding a lease on the object
     * has to say so, or the claim layer arbitrates its own I/O against its own
     * reservation -- denying the write, and recalling the delegation the write
     * is being done under. */
    struct chimera_claim_actor                  io_owner;
    uint8_t                                     have_io_owner;
    struct chimera_claim_actor                  src_io_owner;
    uint8_t                                     have_src_io_owner;
    /* Executor scratch: whether the two-step I/O type check has run.  Lives on
     * the op only so the open-flags decision, which sees an op and not the
     * sequence, can tell the two steps apart. */
    uint8_t                                     io_typechecked_flag;

    /* RESERVE: caller-owned attempt-private claim storage. The VFS owns its
     * acquisition and release until accepted completion takes the reservation. */
    struct chimera_vfs_claim                   *claim;
    struct chimera_vfs_claim_access_owner      *access_owner; /* owned RESERVE_ACCESS result */
    struct chimera_vfs_claim_access_owner      *access_retire_owner; /* borrowed input */
    uint32_t                                    access_narrow_from;
    uint8_t                                     access_narrow_used, access_narrow_denied;
    struct chimera_vfs_claim_access_owner      *base_access_retire_owner;
    struct chimera_vfs_claim_owner             *range_retire_owner;
    /* Same-file private range overlay, borrowed through this operation. */
    const struct chimera_vfs_claim *const      *claim_ranges;
    uint32_t                                    num_claim_ranges;

    /* Exact local claim journal inputs and per-batch result. */
    struct chimera_vfs_claim_owner             *range_owner;
    struct chimera_vfs_claim_owner             *out_range_owner; /* owned typed allocation */
    bool                                        range_zero_point;
    const struct chimera_vfs_claim_exact_range *exact_ranges;
    uint32_t                                    num_exact_ranges;
    uint8_t                                     range_unlock;
    uint8_t                                     range_wait;
    uint32_t                                    range_timeout_ms;
    struct chimera_vfs_claim_range_attempt     *range_attempt;
    void                                        (*range_on_wait)(
        struct chimera_vfs_compound *,
        uint32_t,
        void *);
    bool                                        (*range_is_canceled)(
        struct chimera_vfs_compound *,
        uint32_t,
        void *);
    void                                       *range_wait_private;
    struct chimera_vfs_claim_batch_result       range_result;
    const struct chimera_vfs_claim            **journal_excluded;
    const struct chimera_vfs_claim            **journal_src_excluded;

    struct chimera_vfs_lock_request             lock_request;
    struct chimera_vfs_lock_attempt            *lock_attempt;
    uint8_t                                     nonretryable;
    uint32_t                                    lock_pid;

    /* ---- results ---- */
    /* CLOSE of an external borrowed reference is deferred until acceptance. */
    struct chimera_vfs_open_handle             *close_handle;
    /* Attempt-owned CLOSE output, unavailable to consumers immediately but
     * retained through journal/claim drain on accepted or rejected cleanup. */
    struct chimera_vfs_open_handle             *closed_output_handle;
    enum chimera_vfs_error                      result_error;
    struct chimera_vfs_file_state              *claim_file;
    uint8_t                                     claim_held;
    uint8_t                                     claim_result;
    struct chimera_vfs_claim_conflict           claim_conflict;
    /* LOOKUP, GETATTR, ACCESS and OPEN. ACL storage is copied and owned by
     * the compound, valid through accepted completion until free/retry. */
    struct chimera_vfs_attrs                    attr;
    /* The current object AFTER this op ran: what a LOOKUP resolved, what a
     * PUTFH selected, and for everything else the object the op addressed.
     * A streaming READDIR is the one op that fills this BEFORE it runs rather
     * than after, because its append callback needs to know which directory it
     * is listing and a READDIR cannot move the current object anyway.
     * A caller that must describe the object an op acted on -- which is most
     * of what a protocol reply is -- would otherwise have to re-derive it. */
    uint8_t                                     fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                                    fh_len;
    uint32_t                                    granted; /* ACCESS                            */
    /* READ_PLUS: whether the range it reported is data rather than a hole.
     * Its length and eof land in read_len and eof_read, as a READ's do. */
    uint32_t                                    is_data;
    /* SEEK: where the next data or hole begins, and whether the search ran off
     * the end of the file without finding one. */
    uint64_t                                    seek_offset;
    uint32_t                                    seek_eof;
    char                                       *target; /* READLINK (owned by the compound)  */
    uint32_t                                    target_len;

    /* SETXATTR, REMOVEXATTR, REMOVE_STREAM.  Only the ctime is kept: it is
     * the whole of what a change_info reply needs, and keeping two more
     * attribute sets per op would double the size of a sequence for one
     * field.  (remove_stream asks its backend for no attributes at all today,
     * so for it these stay zero unless the backend volunteers a ctime.) */
    struct timespec                             pre_ctime;
    struct timespec                             post_ctime;

    /* ---- READ results ---- */
    /* The data, as references to the backend's buffers, written into the array
     * the caller supplied.  The references are owned by the compound until
     * chimera_vfs_compound_take_iov(); the array never is.
     *
     * A READ with `dest_iov` is different: on success these are dest_iov and
     * dest_niov handed back -- the caller's own buffers, with the first
     * read_len bytes filled -- and the compound owns none of it.  take_iov
     * then answers NULL / 0, and free releases nothing. */
    struct evpl_iovec                          *iov;
    int                                         niov;
    uint32_t                                    read_len;
    uint32_t                                    eof_read;

    /* ---- WRITE results ---- */
    uint64_t                                    written;
    /* Durability actually achieved, which may exceed what was asked for and
     * may fall short of it only by the backend's own report. */
    uint32_t                                    committed;

    /* ---- OPEN results (and CREATE_UNLINKED's and OPEN_STREAM's) ---- */
    /* The open handle, owned by the CALLER once the sequence has finished --
     * see OPEN HANDLE OWNERSHIP below.  NULL if the op did not run or failed.
     * CREATE_UNLINKED and OPEN_STREAM produce theirs here on the same terms. */
    struct chimera_vfs_open_handle             *out_handle;
    /* Whether the open created the object (the fork, for OPEN_STREAM; always
     * set for CREATE_UNLINKED, which creates by definition). */
    uint8_t                                     created;
    /* Set when the executor resolved the name before opening (which it does
     * for REGULAR_ONLY or ATTRS_ON_CREATE_ONLY) and found an existing object.
     * `existing_mode` is that object's mode -- the whole point of the
     * REGULAR_ONLY failure, whose status says only that the open was refused
     * and not what was in the way. */
    uint8_t                                     existed;
    uint32_t                                    existing_mode;
    /* The parent directory before and after, for a change_info reply.  Set by
     * CREATE and REMOVE, and by an OPEN that named a child. */
    struct chimera_vfs_attrs                    dir_pre_attr;
    struct chimera_vfs_attrs                    dir_post_attr;
    /* RENAME only: the SOURCE directory's change_info.  The pair above is the
     * target's, which is what every other name-changing op reports.  Both are
     * filled because rename_at hands back both and NFSv4's RENAME reply has a
     * slot for each -- source_cinfo and target_cinfo. */
    struct chimera_vfs_attrs                    from_dir_pre_attr;
    struct chimera_vfs_attrs                    from_dir_post_attr;

    /* READDIR.  `entries` is allocated on demand and owned by the compound. */
    struct chimera_vfs_compound_dirent         *entries;
    uint32_t                                    num_entries;
    /* CLAIM_TEST and CLAIM.  `claim` and `ticket` are BORROWED and outlive
     * the sequence AND, for a granted CLAIM, the claim it inserts: the claim
     * core keeps pointers INTO the claim once it is inserted, so its address
     * is its identity and no copy will do.  That is why they are the
     * caller's memory and not the sequence's.  The caller builds `claim`
     * with the matching chimera_vfs_claim_init_* -- range, smb_open,
     * nfs4_open, rqls, oplock, dir_lease, delegation, fuse_grant, deny_probe
     * -- and shapes it (op_handle, policy_tag, owner.key, the callbacks)
     * exactly as it does for the per-op acquire today.  For a coalition
     * construct (rqls / oplock / dir_lease) it is a TEMPLATE: the inserted
     * claim is the grant's own, reported in `claim_grant`.
     *
     * `claim_result` is the arbitration answer and `conflict` describes the
     * holder that refused it (by value, valid whatever the result says; a
     * ZERO conflict on a refused grant means it capped to nothing rather
     * than met a holder -- see chimera_vfs_claim_grant_settle).
     *
     * `lock_file_state` is the per-file claim state the op had to resolve to
     * ask the question.  CLAIM_TEST puts it back itself.  CLAIM hands it to
     * the caller ON GRANTED IN A SEQUENCE THAT FINISHED OK, and only then --
     * the caller needs it to release the claim later -- and puts it back on
     * any other outcome, so exactly one side owns it in every case.  Take it
     * with chimera_vfs_compound_take_file_state(); NULL from that on a
     * GRANTED op means the sequence failed after the grant and the claim was
     * released with it (see RELEASE AND TRANSFER on the CLAIM op). */
    struct chimera_vfs_pending_acquire *ticket;
    struct chimera_vfs_file_state      *lock_file_state;
    struct chimera_vfs_claim_conflict   conflict;
    /* CHIMERA_VFS_COMPOUND_CLAIM_* -- WAIT / WAIT_HARD / TRY / OPTIONAL /
     * TEST_BACKEND. */
    unsigned int                        claim_flags;
    /* The trigger words fired around the acquire (enum chimera_claim_trigger;
     * 0 = none) and their CHIMERA_CLAIM_* retain floors: `pre` before the
     * admission attempt, `post` after GRANTED, `deny` on a synchronous
     * denial.  A caller that fires its own breaks (NFSv4) leaves them 0.
     * See TRIGGERS on the CLAIM op for the actor. */
    uint8_t                             claim_pre_trigger;
    uint8_t                             claim_pre_retain;
    uint8_t                             claim_post_trigger;
    uint8_t                             claim_post_retain;
    uint8_t                             claim_deny_trigger;
    uint8_t                             claim_deny_retain;
    /* Coalition-grant arguments (chimera_vfs_compound_op_set_claim_grant_
     * opts): v2 epoch semantics, the stat-open strict cap, and the member
     * seed stored as a fresh grant's member head under the insert.  Ignored
     * for every other construct. */
    uint8_t                             claim_is_v2;
    uint8_t                             claim_cap_strict;
    void                               *claim_member_seed;
    /* Coalition-grant results: the (possibly coalesced) grant on GRANTED --
     * the standing claim is grant->claim, and chimera_vfs_claim_grant_release
     * is how it is released -- and whether the seed was consumed (false on a
     * coalesce hit or a racing-create collapse, where the caller registers
     * its member on the returned grant itself).  NULL / 0 for every other
     * construct, and NULL after the abort release. */
    struct chimera_vfs_claim_grant     *claim_grant;
    uint8_t                             claim_member_seeded;
    /* READDIR and LISTXATTRS: whether the enumeration reached the end, and the
     * cookie of the entry it stopped at, as the backend reported them.  When
     * append refused an entry, that is the refused entry's cookie, and a
     * resume from it would skip that entry -- see the note on the READDIR page.
     * r_verifier is the directory's verifier (READDIR only). */
    uint32_t                            eof;
    uint64_t                            r_cookie;
    uint64_t                            r_verifier;

    /* GETXATTR (value), LISTXATTRS (back-to-back NUL-terminated names),
     * LIST_STREAMS (packed chimera_vfs_stream_entry records -- see the adder).
     * Owned by the compound, buffer_max bytes, valid until it is freed. */
    void                               *buffer;
    uint32_t                            buffer_len;
    uint32_t                            buffer_count;   /* LISTXATTRS, LIST_STREAMS: entries */

    /* GET_LAYOUT: the backend's answer, COPIED out of its callback into
     * arrays the compound owns and frees -- the backend's own are valid only
     * while that callback runs.  layout_returned_class is the class the
     * backend actually produced; a caller that asked for another treats the
     * mismatch as it would on the per-op path.  Both counts are 0 and both
     * arrays NULL until the op has run OK. */
    uint32_t                            layout_num_segments;
    struct chimera_vfs_layout_segment  *layout_segments;
    uint32_t                            layout_num_devices;
    struct chimera_vfs_layout_device   *layout_devices;
    uint32_t                            layout_returned_class;

    /* RECALL: a holder still stands in the way when the op answers.  For the
     * parking shape that is a live share holder left after the recall
     * drained -- a client that acked the break but kept the file open, the
     * one thing recall_caching_fh reports; for NOWAIT it is a holder whose
     * break is still outstanding, the boolean NFSv4 turns into
     * NFS4ERR_DELAY.  0 is the same answer in both: nothing in the way. */
    uint8_t                             recall_still_open;

    /* ---- CLOSE(CHIMERA_VFS_COMPOUND_CLOSE_DOC) ---- */
    /* The release was the last reference on a handle armed for delete-on-
     * close, so this CLOSE performed the unlink -- see the adder. */
    uint8_t                             doc_fired;
    /* ...or did not, because a named stream still holds the base open: the
     * base is left marked delete-pending and its removal waits for the
     * stream's own last close, which the caller drives. */
    uint8_t                             doc_base_deferred;
    /* The unlink's VFS status, for the caller to map to its own protocol --
     * MS-FSA wants a non-empty directory's ENOTEMPTY reported to the client
     * (the object survived), not swallowed.  It is NOT the op's status: the
     * CLOSE itself succeeded, and the handle is gone either way.  OK when
     * doc_fired is clear. */
    enum chimera_vfs_error              doc_status;
    /* Additional sequence inputs and results retained from the PR. */
    uint8_t                             dest_published;
    uint8_t                             skip_build;
    uint8_t                             skip;
    uint64_t                            pre_attr_mask;
    uint64_t                            dir_attr_mask;
    uint64_t                            dir_pre_attr_mask;
    uint8_t                             child_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                            child_fh_len;
    uint8_t                             child_fh_match;
    uint8_t                             parent_lease_skip[16];
    uint8_t                             parent_lease_skip_valid;
    struct chimera_vfs_open_handle     *op_exempt_handle;
    uint8_t                             target_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                            target_fh_len;
    unsigned int                        rename_flags;
    uint8_t                             link_replace;
    struct chimera_vfs_handle_state    *handle_state;
    uint8_t                             path_intermediates;
    unsigned int                        stream_flags;
    struct evpl_iovec                  *dest_iov;
    int                                 dest_niov;
    uint64_t                            layout_offset;
    uint64_t                            layout_length;
    uint32_t                            layout_iomode;
    uint32_t                            layout_class;
    uint32_t                            layout_max_segments;
    uint8_t                             find_stopped;
    uint8_t                             recall_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                            recall_fh_len;
    uint8_t                             recall_retain;
    unsigned int                        recall_flags;
    struct chimera_vfs_attrs            pre_attr;

};

/*
 * A caller's veto on an op that has just finished, before the sequence goes on.
 *
 * Some of what a protocol requires of an operation is not a property of the
 * object and cannot be asked of the VFS -- NFSv4 calls a file handle stale when
 * the object has no names left AND no client holds it open, and only the server
 * knows the second half.  A caller that has to apply such a rule after the whole
 * sequence has run has already let the ops behind it happen, which is fine while
 * they only read and is not fine once one of them mutates.
 *
 * So the caller answers during the sequence instead.  It is handed the op that
 * just finished and the status it is carrying, and may replace that status:
 * leave it CHIMERA_VFS_OK to go on, set anything else to fail that op and stop
 * the sequence, exactly as a failing VFS op would.
 *
 * (The status travels by pointer rather than as a return value because
 * chimera_vfs_error is also a function-like logging macro, and a declaration
 * naming the enum immediately before a '(' expands it.)
 *
 * It must answer FROM WHAT IT ALREADY HAS.  No I/O, no waiting, and no
 * remembering that it was asked: it may be asked again for the same op if the
 * sequence is ever retried, and the answer has to be the same question asked
 * twice rather than a step taken twice.
 *
 * IT MAY ALSO EDIT THE OPS THAT HAVE NOT RUN.  Beyond replacing the status of
 * the op it is being consulted on, the gate may write into the ARGUMENTS of any
 * op at a HIGHER index than that one -- a SETATTR's set_attr size, a READ or
 * WRITE count or offset, a SEEK offset, a CLONE or COPY length, a claim's flags
 * -- and may set an op's `skip`, which makes the executor run past it without
 * dispatching it at all: its status is left CHIMERA_VFS_UNSET, its results are
 * untouched, and it is not counted among the ops that ran.  A skipped op that a
 * later op addresses by chimera_vfs_compound_op_use_handle is a caller error
 * and aborts, rather than handing that op a NULL handle to act on.
 *
 * This is what turns a question a caller can only answer once an earlier op has
 * answered into ONE sequence instead of two: SMB2's SET_INFO ALLOCATION (the
 * size to set is a function of the size just read), a READ_PLUS-driven read, a
 * CLONE whose count of 0 means "to the end", COPY's range check.  An UNBOUNDED
 * fan-out -- an xattr list and then a GETXATTR per name the list returned -- is
 * still consecutive sequences: the gate edits the ops that are there, it does
 * not add any.
 *
 * It may NOT touch an op at or BELOW its own index: those have run, and their
 * arguments are what they ran with.  chimera_vfs_compound_op_edit, which is how
 * a gate reaches a writable op at all, refuses one; a debug build additionally
 * notices a gate that went around it and aborts.
 *
 * A GATE's skip does NOT persist: every submission clears it, and the gate is
 * asked again.  A CALLER's does -- see chimera_vfs_compound_op_set_skip, which
 * is also where the precedence between the two is stated.
 *
 * The idempotence rule above is unchanged, and the edits make it load-bearing a
 * second time: the gate is consulted, and re-applies its edits, on EVERY
 * execution of the sequence, so it must compute them from what the CALLER
 * ALREADY HAD -- the size in the op as the caller wrote it, plus what the
 * earlier op just reported -- and never accumulate onto what it wrote last
 * time.  A gate that adds a delta to an op's offset gets a different sequence
 * the second time round; one that assigns the offset gets the same one.
 */
typedef void (*chimera_vfs_compound_gate_t)(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data);

/* Backend finish seam. The handler runs after operations stop, and eventually
 * calls finish_result exactly once. A retryable rejection must mean backend
 * effects were aborted before finish_result is called. No backend transaction
 * support is implied by installing this handler; the default accepts immediately. */
typedef void (*chimera_vfs_compound_finish_handler_t)(
    struct chimera_vfs_compound *compound,
    void                        *private_data);

typedef void (*chimera_vfs_compound_callback_t)(
    struct chimera_vfs_compound *compound,
    void                        *private_data);

/*
 * The sequence has PARKED, and the caller may want to say so.
 *
 * Fires ONCE per submission, on the submitting thread, the first time any op
 * in the run parks -- and not at all for a run that never parks.  `index` is
 * the op that parked.  A front end uses it to emit the interim its client is
 * waiting for (an SMB2 STATUS_PENDING, an NLM4_BLOCKED) and to arm a deadline
 * timer whose expiry calls chimera_vfs_compound_cancel.  Nothing else about
 * the run changes: the completion still fires exactly once, later, however
 * the park ends.
 *
 * WHICH PARKS ARE OBSERVABLE.  The two the executor itself drives, which are
 * also exactly the two it can cancel:
 *
 *   a CLAIM whose TICKET QUEUED.  The acquire could not answer, so the claim
 *     core queued the ticket on the file's pending list to be answered when
 *     the conflicting holder finishes breaking (WAIT) or lets go (WAIT_HARD).
 *     The lease-break park and the hard-conflict park are the same park:
 *     both are that one queued ticket, and both are cancellable.
 *
 *   a parking RECALL whose recall did not drain inside the call.  The op is
 *     waiting on the io-wait ticket the claim core parked for it.
 *
 * WHICH ARE NOT.  An ORDINARY op -- a READ, a WRITE, a REMOVE, a RENAME --
 * can also park inside the claim layer: on the implicit claim's own break,
 * where chimera_vfs_io_try parks the request on the file's io-wait queue, or
 * on the namespace recall a name op fires.  The executor cannot see it.  It
 * called an ordinary per-op implementation and is waiting for that op's callback,
 * and the core offers no hook on the way in -- the parking happens several
 * frames below, on a request the executor does not hold.  Such a park does
 * not fire this callback and cannot be cancelled; the run simply waits, as it
 * would outside a sequence.  Nor is one invented for it: a hook that made the
 * executor's own request pointer reach into the io-wait queue for every op
 * would put the whole per-op path under the cancel contract to buy the one
 * consumer nothing -- no front end cancels a READ.
 *
 * A CLAIM_TEST projected to a backend arbiter (TEST_BACKEND) suspends the run
 * too, and is likewise neither reported nor cancellable: the projection is a
 * question already asked of a backend, with no ticket to take back.
 *
 * The callback must not submit, cancel or free anything of this compound's.
 * It runs from inside the very acquire that is parking, before the executor
 * has finished recording the park; cancelling from inside it aborts.
 * chimera_vfs_compound_cancel_post is the exception, and is safe here: it
 * arbitrates nothing in the call, so the cancel it asks for is made from the
 * drain, after the park has been recorded.
 */
typedef void (*chimera_vfs_compound_park_cb_t)(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    void                        *private_data);

/* Allocate a sequence.  `cred` must outlive the submission. */
struct chimera_vfs_compound *
chimera_vfs_compound_alloc(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred);

/* Optional command-sized operation groups. Register after constructing each
 * group's operations, before submit. Groups must partition the original op
 * array in increasing order, without holes or empty groups. With no groups,
 * the existing stop-at-first-error and shared-cursor behavior is unchanged.
 *
 * The first error terminates its group, leaving remaining ops uncompleted with
 * status UNSET. continue_on_error permits the next group to run. The aggregate
 * execution status remains the first failure even if later groups succeed.
 * dependency is an earlier group ID or -1; an unsuccessful dependency skips
 * this entire group with dependency_error (which must be non-OK/non-UNSET).
 * A skipped group's prepare/complete callbacks do not run. These are execution
 * boundaries, NOT savepoints: a failed group's successful prefix remains.
 *
 * Each group starts with empty current/saved cursors. Bind handles explicitly
 * or use earlier op results; credentials never authorize implicit inherited
 * cursors. cred=NULL selects the allocation credential. Credentials/context
 * are borrowed immutable request inputs, valid until the compound is freed.
 *
 * Existing add_* calls from execution prepare/complete callbacks append to the
 * CURRENT GROUP's tail, before later groups, preserving physical op indices.
 * Appending never inserts before the group's already-planned remaining ops.
 * A dynamic producer may have a higher physical index than its consumer:
 * bind through op_use_handle during the consumer's prepare; it accepts a
 * higher index only after successful producer completion. Ordinary I/O may
 * also borrow out_handle via in_handle. Cursor binding MUST use
 * add_puthandle_from/op_use_handle to preserve CLOSE reference provenance.
 * Group registration is construction-only. Retry discards dynamic suffixes and
 * resets group statuses/links, retaining the original immutable descriptors.
 */
struct chimera_vfs_compound_group_config {
    uint32_t                       first_op;
    uint32_t                       num_ops;
    const struct chimera_vfs_cred *cred;
    void                          *context;
    int32_t                        dependency;
    enum chimera_vfs_error dependency_error;
    bool                           continue_on_error;
};

int
chimera_vfs_compound_add_group(
    struct chimera_vfs_compound                    *compound,
    const struct chimera_vfs_compound_group_config *config);

enum chimera_vfs_error
chimera_vfs_compound_group_status(
    const struct chimera_vfs_compound *compound,
    uint32_t                           group);

void *
chimera_vfs_compound_group_context(
    const struct chimera_vfs_compound *compound,
    uint32_t                           group);

/* Cooperative cancellation, on the owning VFS thread only. Stops subsequent
 * operations with EINTR after outstanding work/callouts drain. It does not
 * undo successful operations, cancel underlying I/O, or settle COORDINATE:
 * that owner must still call coordinate_done before freeing its context.
 * Returns false if not executing or already in finish; otherwise the request
 * remains owned until its normal terminal callback. Canceled compounds cannot
 * retry. Cancellation never certifies transaction rollback. */


/* Read-only cancellation request state, including during an active cleanup
 * suffix and after acceptance. Valid until compound_free. */
bool
chimera_vfs_compound_is_canceled(
    const struct chimera_vfs_compound *compound);

/* Declare a static cancellation-deferral suffix within one command group.
 * Once start completes successfully (not skipped), cancellation is recorded
 * but stops execution only after end completes. Activation precedes start's
 * complete callback. Errors still stop the group; callbacks may explicitly
 * normalize optional cleanup errors while preserving them in private results.
 * Scopes cannot overlap, cross groups, or include dynamically appended ops.
 * Does not cancel/settle underlying I/O or COORDINATE, promise cleanup after an
 * unhandled error, or certify rollback. Cancellation before start stays prompt.
 * Construction only; group registration may follow this declaration. */
bool chimera_vfs_compound_set_cancel_scope(
    struct chimera_vfs_compound *compound,
    uint32_t                     start,
    uint32_t                     end);
/* Borrowed immutable identity for typed ACCESS admissions (default: compound).
 * Frontend fences that outlive compound_free must use a separate stable cookie
 * until every fence is released; a pooled compound address can be reused.
 * Construction only, non-NULL; pure callbacks cannot change the identity. */
bool chimera_vfs_compound_set_admission_cookie(
    struct chimera_vfs_compound *compound,
    const void                  *cookie);

/* Execute an exact local range batch through the compound-owned journal.
 * Construction only; original aggregate batch counts may not exceed MAX_OPS.
 * owner/ranges are borrowed immutable request inputs; prepare may bind a
 * deferred NULL owner, but must not mutate shared claims itself. Results are
 * op->range_result. Acquires are atomic per batch; unlock keeps its successful
 * prefix. Seal/publish/reset follow compound acceptance; accepted teardown
 * completes publication only after the frontend terminal callback has staged
 * its protocol state. Old POSIX typed-lock dedicated restriction is unchanged.
 * Cache coordination and backend projection remain separate. Set range_wait
 * for asynchronous local admission (1..100ms polling); timeout_ms=0 waits
 * indefinitely. range_on_wait is an explicit coordination callback, may emit
 * an interim, and needs a request-lifetime memo across retries. It is not a
 * pure prepare/complete callback. range_is_canceled is a pure owning-loop
 * predicate sharing range_wait_private and checked before every poll. Prior
 * deltas remain pinned while waiting. */
int
chimera_vfs_compound_add_range_batch(
    struct chimera_vfs_compound                *compound,
    struct chimera_vfs_claim_owner             *owner,
    const struct chimera_vfs_claim_exact_range *ranges,
    uint32_t                                    count,
    bool                                        unlock);

/* Cancel only this pending range batch on its owning thread. This reports
 * ordinary EINTR to its group; successful prefixes and later independent
 * groups retain normal semantics. Completion may synchronously free compound. */
bool
chimera_vfs_compound_range_cancel(
    struct chimera_vfs_compound *compound,
    uint32_t                     index);

/* Pure mandatory-I/O observation including this attempt's range overlay.
 * Without an exact journal this is the ordinary public claim predicate. */
bool
chimera_vfs_compound_io_denied(
    struct chimera_vfs_compound          *compound,
    const struct chimera_vfs_open_handle *handle,
    uint64_t                              offset,
    uint64_t                              length,
    bool                                  write,
    const struct chimera_claim_actor     *actor);

/* Append one op.  Returns its index, or -1 if the sequence is full (or the
 * name is too long).  A caller that cannot express an operation should build
 * no sequence at all and answer the request from what it already knows, rather
 * than submitting a partial one. */
int
chimera_vfs_compound_add_putfh(
    struct chimera_vfs_compound *compound,
    const void                  *fh,
    int                          fhlen);

/* Resolve `name` in the current object, which becomes the resolved object.
 *
 * `attr_mask` describes the object found; `dir_attr_mask` describes the
 * directory it was found in, landing in the op's dir_post_attr.  NFSv3's
 * LOOKUP3resok carries both -- obj_attributes and dir_attributes -- and the
 * second is only free to ask for here, alongside the resolve the backend is
 * doing anyway. */
int
chimera_vfs_compound_add_lookup(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen,
    uint64_t                     attr_mask,
    uint64_t                     dir_attr_mask);

int
chimera_vfs_compound_add_getattr(
    struct chimera_vfs_compound *compound,
    uint64_t                     attr_mask);

int
chimera_vfs_compound_add_access(
    struct chimera_vfs_compound *compound,
    uint32_t                     requested);

/* Override result masks without changing filesystem semantics. OPEN/CREATE/LINK
 * return object attributes and parent before/after attributes; LOOKUP returns
 * object and parent-after attributes; READ returns object attributes; READDIR
 * returns entry and directory-after attributes. WRITE/SETATTR/COMMIT return
 * before/after attributes in dir_pre_attr/dir_post_attr. Auxiliary attributes
 * omit ACL pointers. Configure before submission (inputs survive retry). */
void
chimera_vfs_compound_set_result_masks(
    struct chimera_vfs_compound *compound,
    int                          index,
    uint64_t                     object_mask,
    uint64_t                     pre_mask,
    uint64_t                     post_mask);

int
chimera_vfs_compound_add_getfh(
    struct chimera_vfs_compound *compound);

int
chimera_vfs_compound_add_readlink(
    struct chimera_vfs_compound *compound);

int
chimera_vfs_compound_add_savefh(
    struct chimera_vfs_compound *compound);

/* Fails with CHIMERA_VFS_EINVAL at execution time when nothing was saved.  A
 * caller whose protocol distinguishes that case (NFS4ERR_RESTOREFH) should
 * check for itself that the sequence saves before it restores. */
int
chimera_vfs_compound_add_restorefh(
    struct chimera_vfs_compound *compound);

int
chimera_vfs_compound_add_lookupp(
    struct chimera_vfs_compound *compound,
    uint64_t                     attr_mask);

/* The two masks sample the object either side of the flush, into the op's
 * pre_attr and attr, so a caller that must classify what it just committed does
 * not need a separate getattr -- and, for NFSv3's COMMIT3resok.file_wcc, could
 * not use one anyway: a getattr after the fact is not atomic with the flush. */
int
chimera_vfs_compound_add_commit(
    struct chimera_vfs_compound *compound,
    uint64_t                     offset,
    uint64_t                     count,
    uint64_t                     pre_attr_mask,
    uint64_t                     post_attr_mask);

/* Allocate (or, with CHIMERA_VFS_ALLOCATE_DEALLOCATE, punch) `length` bytes at
 * `offset` in the current object, or -- when `handle` is non-NULL -- in that
 * handle, which is BORROWED.  This is NFSv4.2's ALLOCATE and DEALLOCATE and
 * POSIX's fallocate; which one is `flags`, not a separate op, because the VFS
 * call they all reach is one call. */
int
chimera_vfs_compound_add_allocate(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint64_t                        length,
    uint32_t                        flags,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask);

/* The three range operations.  Each takes BOTH objects from the caller and
 * never addresses the current one -- see the note on src_handle. Endpoints
 * may be NULL during construction and bound by prepare; both must exist at
 * execution (explicit arguments or saved/current open cursors).
 *
 * COPY_RANGE reads from the source and writes to the destination; its result
 * is how many bytes moved (op->written), which may be short.  CLONE_RANGE
 * shares the range instead of copying it, so there is no length to report --
 * a clone is all-or-nothing.  MOVE_RANGE transfers the blocks and leaves the
 * source range a hole.
 */
int
chimera_vfs_compound_add_copy_range(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *src_handle,
    uint64_t                        src_offset,
    struct chimera_vfs_open_handle *dst_handle,
    uint64_t                        dst_offset,
    uint64_t                        length,
    uint32_t                        flags,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask);

int
chimera_vfs_compound_add_clone_range(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *src_handle,
    uint64_t                        src_offset,
    struct chimera_vfs_open_handle *dst_handle,
    uint64_t                        dst_offset,
    uint64_t                        length,
    uint64_t                        pre_attr_mask,
    uint64_t                        post_attr_mask);

int
chimera_vfs_compound_add_move_range(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *src_handle,
    uint64_t                        src_offset,
    struct chimera_vfs_open_handle *dst_handle,
    uint64_t                        dst_offset,
    uint64_t                        length,
    uint64_t                        src_post_attr_mask,
    uint64_t                        dst_pre_attr_mask,
    uint64_t                        dst_post_attr_mask);

/* Write `block_count` copies of `pattern` from `offset` in the current object,
 * or -- when `handle` is non-NULL -- in that handle, which is BORROWED.  So is
 * `pattern`, on the same terms as a WRITE's payload: the caller holds it for
 * as long as the sequence runs. */
int
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
    uint64_t                        post_attr_mask);

/* Describe the next extent at `offset`: whether it is data or a hole, how long
 * it runs, and whether it reaches the end of the file.  This reads no data --
 * it is the map query NFSv4.2's READ_PLUS needs to decide what to encode, and
 * the caller issues an ordinary READ for the bytes themselves. */
int
chimera_vfs_compound_add_read_plus(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint64_t                        length);

/* Where the next data (`what` 0) or hole (`what` 1) begins at or after
 * `offset` in the current object, or -- when `handle` is non-NULL -- in that
 * handle, which is BORROWED.
 *
 * This asks the backend's allocation map a question; it does not move anything.
 * Nothing in the VFS carries a file position -- NFSv4 has none and the FUSE
 * kernel keeps its own -- so SEEK here is what SEEK_DATA/SEEK_HOLE and
 * NFSv4.2's SEEK are, and nothing else.  The answer lands in the op's
 * seek_offset and seek_eof.
 */
int
chimera_vfs_compound_add_seek(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        offset,
    uint32_t                        what);

/* One page of entries from `cookie`, at most `max_entries` of them (0 to
 * CHIMERA_VFS_COMPOUND_READDIR_MAX_ENTRIES; larger is refused).  `dircount` and
 * `maxcount` are the caller's own byte budgets: the executor records them for
 * the caller's benefit but does not enforce them -- it cannot know what an
 * entry will cost once the caller has marshalled it -- so the caller applies
 * them to the entry list it gets back and, if it stops earlier than the
 * executor did, reports that entry's cookie rather than `r_cookie`. */
int
chimera_vfs_compound_add_readdir(
    struct chimera_vfs_compound *compound,
    uint64_t                     cookie,
    uint64_t                     verifier,
    uint32_t                     dircount,
    uint32_t                     maxcount,
    uint32_t                     max_entries,
    uint64_t                     attr_mask,
    uint64_t                     dir_attr_mask);

/* READDIR that streams: no entry is staged, `append` is called with each one as
 * the backend produces it, and `reset` is called before the op runs (and so
 * again before any retry of it).  `max_entries` is not taken because the
 * caller's own bound is what stops the enumeration -- which is the point.
 *
 * The contract on the two callbacks is on their typedefs; the short version is
 * that reset must undo everything append did, and append must not emit.
 *
 * `flags` is a CHIMERA_VFS_READDIR_* word and `pattern` the caller's search
 * pattern, which the VFS core matches so the backend stays oblivious to it --
 * SMB2's QUERY_DIRECTORY is the caller that needs both.  `pattern` is BORROWED
 * and must outlive the sequence, which a retry makes load-bearing: it is
 * matched again on every execution. */
int
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
    void                                 *private_data);

/* `name` is the fully-qualified xattr name ("user.foo"), as the VFS xattr calls
 * take it.  `value` must outlive the submission; the compound copies neither it
 * nor, for GETXATTR/LISTXATTRS, the answer's destination -- it allocates and
 * owns that (op->buffer). */
int
chimera_vfs_compound_add_getxattr(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen,
    uint32_t                     value_max);

int
chimera_vfs_compound_add_setxattr(
    struct chimera_vfs_compound *compound,
    uint32_t                     option,
    const char                  *name,
    int                          namelen,
    const void                  *value,
    uint32_t                     value_len);

int
chimera_vfs_compound_add_listxattrs(
    struct chimera_vfs_compound *compound,
    uint64_t                     cookie,
    uint32_t                     max_bytes);

int
chimera_vfs_compound_add_removexattr(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen);

/* Create an anonymous unlinked object in the directory the current file
 * handle names; its open handle becomes the current open -- see the op.
 * `flags` is a CHIMERA_VFS_OPEN_* word describing what the handle is for (a
 * READ or WRITE that follows is served by it on those flags); the per-op
 * create takes none, so the backend sees only the attributes.  `set_attr`
 * is required by the underlying call and is copied; the mode's type bits are
 * the backend's to supply (memfs makes a regular file whatever is asked).
 * `attr_mask` describes the new object; the fh is always included. */


/* Open the fork `name` on the base the op addresses; the stream becomes
 * current -- see the op.  `flags` is CHIMERA_VFS_OPEN_*: CREATE, EXCLUSIVE and
 * TRUNCATE select the disposition, the rest describe the handle.  `set_attr`
 * may be NULL; it is copied, and a creating or truncating open stamps it on
 * the BASE (streams share their base's metadata).  `attr_mask` is fetched
 * against the stream; the fh is always included. */
int
chimera_vfs_compound_add_open_stream(
    struct chimera_vfs_compound    *compound,
    const char                     *name,
    int                             namelen,
    unsigned int                    flags,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask);

/* One page of the base's streams from `cookie`, into an op->buffer of
 * `max_bytes` the compound allocates and owns.  The page is what
 * chimera_vfs_list_streams produces, verbatim:
 *
 *   buffer_count records, each a struct chimera_vfs_stream_entry (size,
 *   alloc, name_len, fh_len) followed by name_len bytes of un-terminated
 *   name, then fh_len bytes of the stream's file handle -- fh_len is 0 unless
 *   `want_fh` was set -- with the next record at the following 8-byte-aligned
 *   offset; buffer_len is where the last one ends.  The unnamed data fork is
 *   reported first, as a record with an empty name and the file's own size
 *   (and, with want_fh, the base's own fh), on every backend that has one --
 *   memfs reports it for a regular file and nothing for a directory.
 *
 * eof and r_cookie are as for LISTXATTRS.  A page too small for its first
 * record is the backend's ERANGE. */


/* Remove the fork `name` from the base the op addresses, which stays
 * current.  The ctime pair lands in pre_ctime / post_ctime as REMOVEXATTR's
 * does. */
int
chimera_vfs_compound_add_remove_stream(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen);

/* Where the data of the object the op addresses lives: up to `max_segments`
 * segments (capped at CHIMERA_VFS_LAYOUT_MAX_SEGMENTS, as the per-op call
 * caps it) covering [offset, offset + length) at `iomode`, in
 * `layout_class` (a CHIMERA_VFS_LAYOUT_CLASS_*).  The answer is copied into
 * the op's layout_* arrays -- see the results.  Only a CAP_LAYOUT_SOURCE
 * backend answers; the rest report ENOTSUP, which stops the sequence as any
 * failing op does. */
int
chimera_vfs_compound_add_get_layout(
    struct chimera_vfs_compound *compound,
    uint64_t                     offset,
    uint64_t                     length,
    uint32_t                     iomode,
    uint32_t                     layout_class,
    uint32_t                     max_segments);

/* Walk the tree below the current object -- see the FIND op and the
 * typedefs.  `attr_mask` is fetched for every entry; the fh and the mode are
 * added to it because the walk descends on them.  All three callbacks are
 * required: a walk that stages nothing has nowhere to put an entry but
 * `append`, and a walk that cannot be reset cannot be re-run. */


/* RECALL kicks the recall and answers at once instead of parking: the
 * chimera_vfs_claim_break_caching shape, a FULL recall (every caching
 * holder broken all the way down, which is why `retain` does not apply to
 * it) whose answer is whether a holder's break is still outstanding.  NFSv4
 * turns that boolean into NFS4ERR_DELAY and lets the client retry; the
 * retry finds the recall drained. */
#define CHIMERA_VFS_COMPOUND_RECALL_NOWAIT (1U << 0)

/* Recall the caching leases on `fh`, or -- with fh NULL / fh_len 0 -- on the
 * object the op addresses: an in_handle, a use_handle target, the current
 * open, or else the current file handle, which needs nothing opened.  The
 * fh is copied.
 *
 * WITHOUT NOWAIT the op parks until the recall drains, and what is drained
 * is what recall_handle_lease and recall_caching_fh drain today, the two
 * being one shape with two arguments:
 *
 *   the SPARED handle.  When the op addresses a handle, that handle's own
 *   lease is spared -- the operating open must not break the lease it
 *   holds on the file it is changing (recall_handle_lease, the SMB
 *   delete-on-close SetInfo, which lends the open with PUTHANDLE).  A
 *   recall by bare fh spares nothing: every holder is broken
 *   (recall_caching_fh, the SMB directory-rename recall of each contained
 *   child).  A handle the executor opened for an earlier op holds no lease
 *   and sparing it changes nothing.
 *
 *   the RETAIN floor.  Each holder is broken ONCE, down to `retain` and no
 *   further -- CHIMERA_CLAIM_CR for the delete-on-close recall (RH -> R),
 *   CHIMERA_CLAIM_CR | CHIMERA_CLAIM_CW for the rename recall (RWH -> RW:
 *   a rename invalidates a contained open's cached handle, not its data),
 *   0 to break every caching mode.  A holder already at or below the floor
 *   is not broken at all.
 *
 * The op then parks while any holder it broke is still BREAKING, and
 * answers when the last ack (or a deadline revoke) lands -- on the
 * submitting thread, because the claim core resumes a parked recall through
 * the owning thread's doorbell before it runs anything of the request's.
 * `recall_still_open` is then whether a live share holder remains.
 *
 * WITH NOWAIT `retain` must be 0 (a non-zero floor is refused at build,
 * because the full recall has no floor to honour), the op never parks, and
 * `recall_still_open` is the break-outstanding boolean.
 *
 * A parking RECALL is a PARKING op: nothing behind it runs until the recall
 * drains.  It is one of the two parks the executor reports through the park
 * callback, and one of the two chimera_vfs_compound_cancel can take back --
 * the wait is abandoned, the run completes CHIMERA_VFS_ECANCELED, and the
 * breaks the recall already kicked STAY KICKED.  That is safe for the reason
 * an abandoned acquire is: a recall hands nobody anything, so its victims
 * were given nothing to act on.  NOWAIT never parks, so it never fires the
 * park callback and there is nothing to cancel. */
int
chimera_vfs_compound_add_recall(
    struct chimera_vfs_compound *compound,
    const uint8_t               *fh,
    uint32_t                     fh_len,
    uint8_t                      retain,
    unsigned int                 flags);

/* Open, and with CHIMERA_VFS_OPEN_CREATE create, `name` in the current object;
 * the opened object becomes current.  A NULL (or empty) `name` opens the
 * current object itself, which is how a protocol re-opens by file handle.
 *
 * `flags` is an ordinary CHIMERA_VFS_OPEN_* word and means exactly what it
 * means to chimera_vfs_open_at.  `opts` selects the two resolve-first
 * behaviours documented on CHIMERA_VFS_COMPOUND_OPEN_* above.  `set_attr` may
 * be NULL; the struct is copied, so the caller need not keep it alive -- but
 * an ACL or SID it points at is BORROWED for the life of the run, on the
 * terms given on the op's set_attr.
 *
 * The handle this produces belongs to the compound until taken -- see OPEN
 * HANDLE OWNERSHIP at the top of this file. */
int
chimera_vfs_compound_add_open(
    struct chimera_vfs_compound    *compound,
    const char                     *name,
    int                             namelen,
    unsigned int                    flags,
    uint32_t                        opts,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask,
    uint64_t                        dir_pre_attr_mask,
    uint64_t                        dir_post_attr_mask);

/* CLAIM waits for a conflicting holder to finish breaking rather than failing
 * the op.  Without it a BREAKING conflict is reported as it stands. */
#define CHIMERA_VFS_COMPOUND_CLAIM_WAIT         (1U << 0)
/* ... and additionally waits on a HARD conflict -- another owner's incompatible
 * byte-range lock, which no recall will clear.  This is a blocking lock
 * (F_SETLKW, an SMB2 LOCK without FAIL_IMMEDIATELY) and it can park for as long
 * as the holder keeps it, which is the same open-ended wait a lease break
 * already makes a sequence accept. */
#define CHIMERA_VFS_COMPOUND_CLAIM_WAIT_HARD    (1U << 1)
/* Never park: BREAKING and DENIED come back as the op's result.  WAIT /
 * WAIT_HARD / TRY are three points on one axis, not independent bits -- TRY
 * is "neither wait", spelled as its own flag so a caller reads intent rather
 * than an absence.  The executor treats (!WAIT && !WAIT_HARD) and TRY
 * identically, and refuses to build a CLAIM that combines TRY with either. */
#define CHIMERA_VFS_COMPOUND_CLAIM_TRY          (1U << 2)
/* CLAIM_TEST only: when the local probe is clear, project it to a CAP_LEASE
 * backend RANGE arbiter (F_GETLK across processes) and report ITS holder, if
 * any, in `conflict` (used LR / LR|LW, offset, length, owner.owner_lo = the
 * pid a real OS lock reports).  With no RANGE-capable backend registered the
 * local answer stands, without a dispatch.  Range claims only. */
#define CHIMERA_VFS_COMPOUND_CLAIM_TEST_BACKEND (1U << 3)
/* A non-GRANTED answer is a SUCCESSFUL result: the op completes OK with
 * claim_result / conflict recorded, the file state put, and the run goes on.
 * SMB's cache grant is opportunistic (NONE is a valid outcome) and NFSv4's
 * delegation grant likewise; without this a refused grant would abort-
 * release the share claim granted just before it.  Without OPTIONAL a
 * non-GRANTED answer is EAGAIN and stops the run. */
#define CHIMERA_VFS_COMPOUND_CLAIM_OPTIONAL     (1U << 4)
/* Allocate canonical ACCESS storage before admission, including a possible
* wait. The builder's claim is a template; take_access_owner transfers the
* admitted token and its file-state reference after accepted completion. */
#define CHIMERA_VFS_COMPOUND_CLAIM_ACCESS_OWNER (1U << 5)


/* Ask whether `claim` WOULD be granted against the current open handle,
 * changing nothing.  This is NFSv4 LOCKT, F_GETLK, NLM TEST, and SMB2's
 * FAIL_IMMEDIATELY pre-check.  The answer lands in the op's `claim_result`,
 * with the refusing holder in `conflict`; a "denied" is an ANSWER, so the
 * op succeeds and the sequence goes on.  `flags` is
 * CHIMERA_VFS_COMPOUND_CLAIM_TEST_BACKEND or 0.
 *
 * `claim` is BORROWED but need not outlive the sequence: a probe is never
 * inserted, so nothing keeps a pointer to it afterwards.  Its op_handle is
 * never stamped (see OP_HANDLE on the CLAIM op): a probe through a shared
 * open must see what another owner holds through it.
 *
 * The caller builds the claim with the matching chimera_vfs_claim_init_*
 * and shapes it -- SMB2 stamps op_handle and policy_tag, and carries its
 * grant's lease key so a lock and its own caching lease do not break each
 * other.  None of that belongs in a VFS op table: the caller knows what it is
 * asking for, the same way it knows why it is opening.
 *
 * Wants a PATH open: a probe uses only the fh, and a data open of a FIFO
 * blocks.  A data handle already in the cursor serves it. */
int
chimera_vfs_compound_add_claim_test(
    struct chimera_vfs_compound *compound,
    struct chimera_vfs_claim    *claim,
    unsigned int                 flags);

/* Take `claim` against the current open handle.  `flags` is
 * CHIMERA_VFS_COMPOUND_CLAIM_*.  `pre` / `deny` are the trigger words fired
 * around the acquire (enum chimera_claim_trigger; 0 = none) and `pre_retain`
 * / `deny_retain` their CHIMERA_CLAIM_* floors; the `post` pair rides on
 * chimera_vfs_compound_op_set_claim_post, and a coalition grant's knobs on
 * chimera_vfs_compound_op_set_claim_grant_opts.
 *
 * `claim` and `ticket` are BORROWED and must outlive the sequence AND the
 * claim -- see the note on the op's fields.  On GRANTED the claim is inserted
 * (for a coalition construct, the grant's copy of it: `claim_grant`) and the
 * file state comes to the caller (take_file_state); on any other outcome the
 * run stops with claim_result + conflict and the file state is put -- unless
 * OPTIONAL, which makes that a successful result the run goes on past.
 *
 * A sequence that fails after this op was GRANTED releases the claim before
 * the completion callback, which is safe in the way a release generally is
 * not: an acquire that is rolled back blocked other clients for a while and
 * handed them nothing they could act on, so there is nothing for them to have
 * acted upon -- and the caller has not seen the grant either.  The full rule,
 * the routing by construct, the triggers, and the thread the answer comes
 * back on, are on the CLAIM op above.  Wants a PATH open, as CLAIM_TEST does. */
int
chimera_vfs_compound_add_claim(
    struct chimera_vfs_compound        *compound,
    struct chimera_vfs_claim           *claim,
    struct chimera_vfs_pending_acquire *ticket,
    unsigned int                        flags,
    uint8_t                             pre,
    uint8_t                             pre_retain,
    uint8_t                             deny,
    uint8_t                             deny_retain);

/* CLAIM (op `index`): the trigger fired after GRANTED, and its floor --
 * SMB's phase-2 OPEN_W write-cache break between the share grant and the
 * cache grant.  0 = none. */
void
chimera_vfs_compound_op_set_claim_post(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    uint8_t                      post,
    uint8_t                      post_retain);

/* CLAIM (op `index`) of a coalition construct (rqls / oplock / dir_lease):
 * v2 epoch semantics, the stat-open strict cap (0 at the CR floor), and the
 * member seed (walk-ready: its protocol next-link NULL) a fresh grant is
 * born holding.  Read back through claim_grant / claim_member_seeded.  A
 * CLAIM of any other construct ignores them. */
void
chimera_vfs_compound_op_set_claim_grant_opts(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    int                          is_v2,
    int                          cap_strict,
    void                        *member_seed);

/* ---- path-addressed operations ----
 *
 * Each resolves a whole path against the sequence's CURRENT FILE HANDLE -- not
 * the current open handle, which is the point: these need nothing opened, and
 * on a path-only mount there is nothing openable to reach.  The object a path
 * op resolves becomes current, so a LOOKUP_PATH can be followed by an op that
 * addresses it; on a path-only mount, though, only what the op itself returns
 * is usable, so ask for the attributes you need on the op that resolves them.
 *
 * `path` is copied.  The flags are the CHIMERA_VFS_* words the path-based VFS
 * calls take, and mean exactly what they mean there.
 */
/* ---- the cursor operations ---- */

/* The export root becomes the current FILE HANDLE.  Every path-addressed
 * sequence starts here, and on a path-only mount it is the only file handle
 * that means anything. */
int
chimera_vfs_compound_add_putroot(
    struct chimera_vfs_compound *compound);

/* Open the current FILE HANDLE; the result becomes the current OPEN HANDLE.
 *
 * `flags` is an ordinary CHIMERA_VFS_OPEN_* word and means what it means to
 * chimera_vfs_open_fh.  The caller chooses it because the caller is what knows
 * why it is opening -- to read data, to resolve names in a directory, to read
 * an attribute off an object of unknown type.
 *
 * The handle belongs to the sequence unless GETHANDLE takes it.  Opening again
 * releases whatever the slot held (unless that was borrowed or taken). */
int
chimera_vfs_compound_add_open_current(
    struct chimera_vfs_compound *compound,
    unsigned int                 flags,
    uint64_t                     attr_mask);

/* Take the current OPEN HANDLE for the caller: the sequence stops owning it,
 * and it is readable as this op's out_handle once the sequence has finished.
 * The current open slot keeps addressing it -- taking is about who releases it,
 * not about where it is. */
int
chimera_vfs_compound_add_gethandle(
    struct chimera_vfs_compound *compound);

/* CLOSE honours the handle's delete-on-close: on the last reference, unlink the
 * name the flag was armed with (matching the doomed child's fh, sparing
 * `parent_lease_skip`) and then close the backend handle that release detached.
 * Without it a CLOSE is the bare release it always was. */
#define CHIMERA_VFS_COMPOUND_CLOSE_DOC (1U << 0)

/* End the current OPEN HANDLE and empty the slot.
 *
 * Provenance does not matter: a handle lent with PUTHANDLE is closed too, which
 * is exactly what an SMB2 or NFSv4 CLOSE of a client's open means.  The caller
 * must not release that handle afterwards.
 *
 * CLOSE PERFORMS THE DELETE-ON-CLOSE UNLINK.  With CHIMERA_VFS_COMPOUND_CLOSE_
 * DOC the executor releases through chimera_vfs_release_doc, and when that says
 * this was the LAST reference on a handle armed for delete-on-close it goes on
 * to do what the arming asked for: open the recorded parent, unlink the
 * recorded name -- only while that name still resolves to the doomed object, so
 * an unlink this late cannot destroy something else that has since taken the
 * name -- and then dispatch the backend close that the release detached from
 * the cache.  All of it inside the op, because the unlink addresses an object
 * and must happen BEFORE the backend close the run owns; a caller cannot get
 * between them.  The unlink runs under the credential the flag was armed with,
 * which is the one that asked for the deletion, and spares
 * `parent_lease_skip` (16 bytes, or NULL to spare none) -- the closing handle's
 * own ParentLeaseKey when it is the handle that armed the flag, so a client
 * does not break the directory lease whose cached view is coherent with the
 * removal it caused, and nothing when some other open triggers the removal.
 *
 * The op reports what happened in `doc_fired`, `doc_status` and
 * `doc_base_deferred` -- see the results.  None of it changes the op's OWN
 * status: the handle is gone whatever the unlink did, which is what a CLOSE
 * means, and a caller that must tell its client the object survived reads
 * doc_status (SMB maps ENOTEMPTY to STATUS_DIRECTORY_NOT_EMPTY; swallowing it
 * makes a recursive client teardown believe it worked).
 *
 * SETTING AND CLEARING the delete-on-close flag stays OUT OF BAND
 * (chimera_vfs_set_delete_on_close / _clear_delete_on_close): it addresses
 * nothing through the cursors and mutates nothing the run holds, so by THE RULE
 * it is not a sequence op.  What is in band is the unlink the flag eventually
 * causes, which addresses an object.
 *
 * A CLOSE without the flag releases and reports nothing, even on a handle that
 * IS armed: the arming caller is the one that asked for the unlink, and a CLOSE
 * that did not ask does not perform it. */
int
chimera_vfs_compound_add_close_doc(
    struct chimera_vfs_compound *compound,
    unsigned int                 flags,
    const uint8_t               *parent_lease_skip);

/* Park the current OPEN HANDLE in the saved slot, and take it back.  Both MOVE:
 * the source slot is empty afterwards.  Anything the destination slot held is
 * released first, on the ordinary ownership rules. */
int
chimera_vfs_compound_add_savehandle(
    struct chimera_vfs_compound *compound);

int
chimera_vfs_compound_add_restorehandle(
    struct chimera_vfs_compound *compound);

/* Make the caller's OPEN HANDLE the current object.
 *
 * PUTFH names the current object by file handle and leaves the sequence to
 * open it; this hands over a handle that is already open.  For a caller that
 * holds one -- an *_at API, whose whole shape is "in this directory I have
 * open" -- that is the difference between reusing a reference and taking a
 * second one.
 *
 * The handle is BORROWED: the caller holds it for the life of the sequence and
 * releases it afterwards, and the sequence will not release it when the
 * current object moves.  `open_flags` is what the caller opened it with -- the
 * REAL flags: a data handle is READ_ONLY and/or WRITE_ONLY, an opendir handle
 * is PATH|DIRECTORY -- so the sequence can tell whether it serves the ops that
 * follow.  A lent handle that does not serve an op FAILS that op with EINVAL
 * rather than being set aside for one the sequence opens itself: the caller's
 * open bound rights to this handle (an SMB2 FileId's granted access, an NFSv4
 * stateid's), and acting through a different one would discard them.
 *
 * THE FLAGS ARE CAPABILITIES, NOT RESTRICTIONS.  An op's want is the set of
 * things it needs the handle to be able to do, and a lent handle serves when it
 * can do all of them -- so READ_ONLY means "this can read" and WRITE_ONLY "this
 * can write", and a read-write handle is lent as READ_ONLY|WRITE_ONLY.  That is
 * not a quirk of this rule: it is how O_RDWR is spelled throughout the VFS, and
 * chimera_vfs_open_access_mode reads exactly that pair back as the read-write
 * access mode (READ_ONLY without WRITE_ONLY is the only read-only handle).  The
 * names read the other way round, which is why it is said here: a caller that
 * lends its read-write handle as READ_ONLY has withheld a capability, and the
 * READ it lent it for still serves while the WRITE behind it does not.
 *
 * Three rules decide "serves":
 *
 *   1. CHIMERA_VFS_OPEN_INFERRED is never required.  It is provenance -- the
 *      VFS opened this for an op -- not a capability, and a handle the caller
 *      opened explicitly serves anything an inferred one would.  So a
 *      READ_ONLY handle serves COMMIT, ALLOCATE, SEEK and GETATTR, whose own
 *      wants are spelled with the bit because the executor's opens are
 *      inferred by definition.
 *
 *   2. A PATH handle never serves a data-side op -- READ, WRITE, ALLOCATE,
 *      SEEK -- because an O_PATH descriptor cannot do those; that refusal is
 *      the real EBADF, reported early.  COMMIT is the one exception: it is
 *      served by whatever handle is lent, path or data, because fsyncdir(2)
 *      IS a commit through an O_PATH directory handle and every backend
 *      accepts one.
 *
 *   3. CHIMERA_VFS_OPEN_DIRECTORY is provenance too, and what settles a
 *      directory-wanting op -- READDIR, FIND, and the ops that resolve a name
 *      -- is what the handle ADDRESSES.  A dirfd from open(dir, O_RDONLY) is
 *      an open directory that cannot report the bit; refusing it would leave
 *      the caller nothing legal to do (it may not substitute another handle),
 *      and a caller that ORs the bit in on faith is asserting something it has
 *      not checked.  So a lent handle without the bit is accepted for such an
 *      op when the object IS a directory and refused ENOTDIR -- the errno for
 *      the thing that is actually wrong -- when it is not.
 *
 *      That costs one GETATTR of the mode, and it is paid ONLY by a lent
 *      handle that does not carry the bit and is asked to serve a
 *      directory-wanting op: a handle lent as PATH|DIRECTORY (an opendir, an
 *      SMB2 enumeration handle) is taken at its word and costs nothing, as it
 *      always did.  A handle the sequence opened for itself is not affected at
 *      all -- it is re-opened as a directory, and the open is the check.
 */
int
chimera_vfs_compound_add_puthandle(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *handle,
    unsigned int                    open_flags);

/* Select an operation-owned result as the cursor without taking ownership.
 * Unlike external PUTHANDLE, CLOSE consumes that producer's reference exactly
 * once and clears its out_handle. GETHANDLE still creates a separate reference.
 * Use this for provisional OPEN/GETHANDLE results across group boundaries;
 * never pass those result pointers to external-borrowing add_puthandle.
 * from=-1 permits a prepare callback to bind a dynamically discovered producer
 * with op_use_handle (or handle_from). An unbound/failed/consumed source fails
 * EINVAL at execution. Inputs and provenance are restored on finish retry. */
int
chimera_vfs_compound_add_puthandle_from(
    struct chimera_vfs_compound *compound,
    int32_t                      from,
    unsigned int                 open_flags);

int
chimera_vfs_compound_add_lookup_path(
    struct chimera_vfs_compound *compound,
    const char                  *path,
    int                          pathlen,
    uint64_t                     attr_mask,
    uint32_t                     flags);

/* OPEN relative to the current open directory. Unlike the component-only
 * OPEN builder, accepts a full path for FS_PATH_OP backends, with the same
 * semantics and limits as chimera_vfs_open_at. The path is copied. */
int
chimera_vfs_compound_add_open_at(
    struct chimera_vfs_compound    *compound,
    const char                     *path,
    int                             pathlen,
    unsigned int                    flags,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask);

int
chimera_vfs_compound_add_open_path(
    struct chimera_vfs_compound    *compound,
    const char                     *path,
    int                             pathlen,
    unsigned int                    flags,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask);

/* `create_type` is a CHIMERA_VFS_COMPOUND_CREATE_*, as for the name-based
 * CREATE; `target` is the symlink target and read only for a symlink.
 *
 * `intermediates` is mkdir -p, and only the DIR create type honours it: the
 * path's interior components are created as they are walked and a component
 * already there is not an error -- for the S3 "make the object's parent
 * chain" and REST paths, which walk chimera_vfs_create's loop by hand today.
 * That loop is what runs here, so its rule is the rule: a LEAF that already
 * exists is accepted too (a second run of the same op is not an error), which
 * is what mkdir -p means and what the consumers rely on.  A caller that wants
 * the leaf's EEXIST leaves this clear and gets the single-level mkdir, which
 * fails ENOENT on a missing parent as it always has.  The attributes are
 * applied to every component the walk creates. */
int
chimera_vfs_compound_add_create_path(
    struct chimera_vfs_compound    *compound,
    uint8_t                         create_type,
    const char                     *path,
    int                             pathlen,
    const char                     *target,
    int                             targetlen,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask,
    uint8_t                         intermediates);

/* Materialize the directory chain and leave its final directory current,
 * using the same semantics as chimera_vfs_create(). */
int
chimera_vfs_compound_add_create_tree(
    struct chimera_vfs_compound    *compound,
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask);

/* Create an unnamed file in the current directory. The compound owns the
 * resulting handle, which becomes current and can be taken at completion. */


/* Resolve a directory path and remove one leaf without changing the current
 * cursor. Empty leaf names are passed through to the backend, as remove_at
 * does; this preserves callers whose namespace represents an empty leaf. */
int
chimera_vfs_compound_add_remove_at_path(
    struct chimera_vfs_compound *compound,
    const char                  *directory,
    int                          directory_len,
    const char                  *name,
    int                          namelen,
    unsigned int                 flags);

int
chimera_vfs_compound_add_remove_path(
    struct chimera_vfs_compound *compound,
    const char                  *path,
    int                          pathlen,
    unsigned int                 flags);

int
chimera_vfs_compound_add_rename_path(
    struct chimera_vfs_compound *compound,
    const char                  *old_path,
    int                          old_pathlen,
    const char                  *new_path,
    int                          new_pathlen);

int
chimera_vfs_compound_add_link_path(
    struct chimera_vfs_compound *compound,
    const char                  *old_path,
    int                          old_pathlen,
    unsigned int                 source_lookup_flags,
    const char                  *new_path,
    int                          new_pathlen,
    uint64_t                     attr_mask);

/* Make op `index` address the handle op `from` produced, instead of the
 * current object.  For the operation after a path OPEN: on a path-only mount
 * that handle is the only usable reference to what the open resolved.
 * Construction references must have a lower physical index; the consumer's
 * prepare may also bind a higher index whose producer successfully completed.
 * For PUTHANDLE_FROM this preserves ownership; in_handle must remain NULL. */
void
chimera_vfs_compound_op_use_handle(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    uint32_t                     from);

/* Run PAST op `index` without dispatching it: its status stays
 * CHIMERA_VFS_UNSET, its results are untouched, and it is not counted among the
 * ops that ran -- the gate's skip, decided at BUILD time instead.
 *
 * A caller that already knows the answer should be allowed to say so.  Two run
 * shapes deciding the same question -- does this run need the truncate, does it
 * need the grant -- one by appending the op conditionally and one by a gate
 * that skips it, are two spellings of one thing, and the conditional append
 * moves every later op's index with it.  A fixed shape whose unwanted ops are
 * skipped keeps the indices the caller wrote down.
 *
 * PRECEDENCE, because there are now two skips and they must not be able to
 * disagree.  They are ORed: an op is run past if EITHER says so, and neither
 * can clear the other.  A gate's skip is cleared by every submit, so its
 * decision is re-made per execution; this one is an argument of the sequence as
 * built and is NOT cleared, so a compound submitted twice skips the same op
 * both times without the caller rebuilding it.  A gate setting `skip` on an op
 * already skipped here changes nothing, and a gate cannot un-skip one -- the
 * gate contract gives it no way to clear a skip at all.
 *
 * An op skipped here may not be the source of a later use_handle, for the
 * reason use_handle refuses a source that leaves no handle: it would produce
 * none.  Either order of the two calls is refused, and the sequence fails to
 * build.
 *
 * An index past the end is ignored, as the setters ignore one. */
void
chimera_vfs_compound_op_set_skip(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    int                          skip);

/* Acquire a provisional claim through the handle produced by an
 * earlier operation. Failed attempts release it automatically. Claim storage
 * must outlive the compound and must be reinitialized by prepare on retry.
 * RANGE claims must be locally arbitrated; projected ranges return ENOTSUP.
 * The result includes a copied conflict snapshot. Prepare may bind a same-file
 * private interval view through claim_ranges/num_claim_ranges in addition to
 * claim->admit_excluded; admission checks that view before public claims. */
int
chimera_vfs_compound_add_reserve(
    struct chimera_vfs_compound *compound,
    uint32_t                     handle_from,
    struct chimera_vfs_claim    *claim);

/* Canonical ACCESS reservation: typed execution clones the unlinked template
 * into owner-owned storage before admission. op->access_owner is a borrowed
 * attempt result until take_access_owner transfers it after acceptance. */
int chimera_vfs_compound_add_reserve_access(
    struct chimera_vfs_compound *compound,
    uint32_t                     handle_from,
    struct chimera_vfs_claim    *template_claim);
/* Opt-in failed-producer cleanup. RESERVE_ACCESS remains provisional until a
* later CHECKPOINT in the same logical group completes OK without being skipped
* and that group succeeds. Otherwise group exit retires ONLY this reservation,
* before later groups run. Its owner/file/result storage remains borrowed and
* valid through ordinary compound cleanup, but take_access_owner cannot take it.
* This does not undo filesystem effects, retire earlier groups' reservations,
* or rewind prior journal edits. The owner must never have been externally
* published. Register during construction or grouped completion suffix append;
* both operations must still be unexecuted. Rejected attempts recreate it. */
bool chimera_vfs_compound_reserve_access_until(
    struct chimera_vfs_compound *compound,
    uint32_t                     reserve_index,
    uint32_t                     ready_index);

/* Subset-only private rights view for this compound's RESERVE_ACCESS result.
 * Use after a successful typed mutation and before producer readiness. Other
 * clients retain the original conservative claim until accepted publish; later
 * reservations in this attempt see the narrowed row. No public token input,
 * widening, reinsertion, or allocation/admission during accepted publication.
 * Supports ordinary grouped completion suffix append and replay. */
int chimera_vfs_compound_add_narrow_access(
    struct chimera_vfs_compound *compound,
    uint32_t                     reserve_index,
    uint8_t                      used,
    uint8_t                      denied);

/* Transfer token plus the reservation's file-state reference together. file
 * must be non-NULL. The owner has its own separate file-state reference. */
struct chimera_vfs_claim_access_owner * chimera_vfs_compound_take_access_owner(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    struct chimera_vfs_file_state **file);
/* Construction only. A deferred NULL owner may be bound through prepare's
 * access_retire_owner. Exclusions affect later RESERVE in the same attempt;
 * public unlink waits accepted publication. Teardown must use owner_retire. */
int chimera_vfs_compound_add_retire_access(
    struct chimera_vfs_compound           *compound,
    struct chimera_vfs_claim_access_owner *owner);
/* Atomically reserve retirement of canonical range, ordinary ACCESS, and base
 * ACCESS owners before backend CLOSE. Inputs are borrowed/pinned by frontend;
 * NULL means absent, and prepare may bind them from an attempt-private open.
 * Failure withdraws only this op's reservations; earlier command edits survive.
 * No frontend or filesystem effects occur here. Successful retirement remains
 * staged even if a later independent operation fails: the CLOSE mapper must
 * preserve its accepted-prefix semantics. Local canonical owners only. */
int chimera_vfs_compound_add_retire_open_claims(
    struct chimera_vfs_compound           *compound,
    struct chimera_vfs_claim_owner        *range_owner,
    struct chimera_vfs_claim_access_owner *access_owner,
    struct chimera_vfs_claim_access_owner *base_access_owner);
int chimera_vfs_compound_add_retire_range_owner(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_claim_owner *range_owner);
/* Allocate canonical local SMB RANGE ownership during typed execution using
 * the current handle and actor (copied input, or prepare-bound io_owner).
 * Results stay compound-owned until accepted transfer. Reset/free retires
 * untransferred tokens before releasing handle anchors. */
int chimera_vfs_compound_add_range_owner(
    struct chimera_vfs_compound      *compound,
    const struct chimera_claim_actor *actor,
    bool                              zero_point);
struct chimera_vfs_claim_owner * chimera_vfs_compound_take_range_owner(
    struct chimera_vfs_compound *compound,
    uint32_t                     index);



/* As above, with a borrowed pinned handle; no reopen or DAC recheck. NULL may
 * be bound by prepare through op_args->in_handle, otherwise uses the current
 * handle. The handle must remain live until the reservation has completed. */
int
chimera_vfs_compound_add_reserve_handle(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *handle,
    struct chimera_vfs_claim       *claim);

/* Transfer the held reservation and file-state reference after acceptance. */
struct chimera_vfs_file_state *
chimera_vfs_compound_take_reservation(
    struct chimera_vfs_compound *compound,
    uint32_t                     index);

int
chimera_vfs_compound_add_link_replace(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen,
    uint64_t                     attr_mask);

void
chimera_vfs_compound_set_finish_handler(
    struct chimera_vfs_compound          *compound,
    chimera_vfs_compound_finish_handler_t handler,
    void                                 *private_data);

/* Construct statically, or append to a grouped compound from an op complete
 * callback. Prepare, parked coordination, finish, and cancellation-deferral
 * scopes cannot introduce dynamic coordination. A current FH is required.
 * Outcomes (including errors) are cached across finish rejection by physical
 * index, start function, private context identity, and FH. Dynamic suffix shape
 * changes cannot reuse another context's result. The callback and context must
 * retain stable semantic identity and stay alive until compound_free, even
 * after a dynamic op is discarded on retry; mutable attempt results are fine.
 * At most MAX_OPS distinct keys are retained; exhaustion fails ENOSPC before
 * new coordination starts. Neither free nor retry is allowed while parked.
 * Disconnect/cancellation must settle the outstanding callback first. */
int
chimera_vfs_compound_add_coordinate(
    struct chimera_vfs_compound      *compound,
    chimera_vfs_compound_coordinate_t start,
    void                             *private_data);

/* Complete exactly once. False rejects an inactive/stale token without
 * affecting the current attempt. The token prevents a late duplicate from
 * completing a different FH's coordination at the same operation index. */
bool
chimera_vfs_compound_coordinate_done(
    struct chimera_vfs_compound *compound,
    uint64_t                     token,
    enum chimera_vfs_error       status);

void
chimera_vfs_compound_finish_result(
    struct chimera_vfs_compound *compound,
    enum chimera_vfs_error       status);

/* Execution status is the first operation error; finish status is independent
 * and takes precedence in compound_status when the backend rejects an attempt. */
enum chimera_vfs_error
chimera_vfs_compound_execution_status(
    const struct chimera_vfs_compound *compound);
enum chimera_vfs_error
chimera_vfs_compound_finish_status(
    const struct chimera_vfs_compound *compound);

/* Callbacks are stored per operation; registration is part of construction. */
void
chimera_vfs_compound_set_op_callbacks(
    struct chimera_vfs_compound       *compound,
    uint32_t                           index,
    chimera_vfs_compound_op_callback_t prepare,
    chimera_vfs_compound_op_callback_t complete,
    void                              *private_data);

/* Set a conditional operation to succeed without issuing VFS work. Only its
 * prepare callback may call this, and complete still runs. */
void
chimera_vfs_compound_op_skip(
    struct chimera_vfs_compound *compound,
    uint32_t                     index);

/* Bind scalar arguments/borrowed handles during prepare. Never replace owned
 * pointers (path, new_path, link_target, entries, buffer), callbacks, or type.
 * The immutable submission snapshot is restored on retry. Returned operation
 * addresses remain stable even when a complete callback appends a suffix. */
struct chimera_vfs_compound_op *
chimera_vfs_compound_op_args(
    struct chimera_vfs_compound *compound,
    uint32_t                     index);

/* Install a prepare check without replacing the operation's result callback.
 * Both callbacks retain independent private contexts. */
void
chimera_vfs_compound_set_op_prepare(
    struct chimera_vfs_compound       *compound,
    uint32_t                           index,
    chimera_vfs_compound_op_callback_t prepare,
    void                              *private_data);

/* Borrow the current execution cursor for a synchronous callout. The pointer
 * must not be retained; copy it to keep a snapshot. NULL means no current FH. */
const uint8_t *
chimera_vfs_compound_current_fh(
    const struct chimera_vfs_compound *compound,
    uint32_t                          *length);

/* Same borrowing contract as current_fh; NULL means no saved filehandle. */
const uint8_t *
chimera_vfs_compound_saved_fh(
    const struct chimera_vfs_compound *compound,
    uint32_t                          *length);

/* Reset all frontend attempt-private state before each attempt, including the
 * first. Dynamic suffixes appended by callbacks are discarded on retry. */
void
chimera_vfs_compound_set_attempt_reset(
    struct chimera_vfs_compound         *compound,
    chimera_vfs_compound_attempt_reset_t reset,
    void                                *private_data);

/* Re-execute a finished attempt ONLY after its backend transaction has been
 * aborted. This does not itself undo filesystem effects. No handles/iovecs may
 * have been taken and no result may have been published. Backend transaction
 * integration calls this on finish-time EAGAIN; ordinary op errors preserve
 * successful-prefix semantics and must not trigger an automatic retry.
 * Returns false without starting if still running, ownership was taken, or no
 * submitted input snapshot exists. True means started; synchronous completion
 * may already have called the final callback and freed the compound. */
bool
chimera_vfs_compound_retry(
    struct chimera_vfs_compound *compound);

/* Register a veto consulted as each op finishes -- see
 * chimera_vfs_compound_gate_t.  Optional; without one the sequence is governed
 * by the ops' own statuses alone. */
void
chimera_vfs_compound_set_gate(
    struct chimera_vfs_compound *compound,
    chimera_vfs_compound_gate_t  gate,
    void                        *private_data);

/* Execute an attempt. The callback fires once per attempt, on the submitting
 * thread, after operations stop and the finish adapter resolves acceptance.
 * A finish adapter may complete asynchronously. Retrying starts a new attempt
 * with another completion; the compound remains valid until explicitly freed. */
void
chimera_vfs_compound_submit(
    struct chimera_vfs_compound    *compound,
    chimera_vfs_compound_callback_t callback,
    void                           *private_data);

/*
 * Abandon a PARKED run: stop waiting, and complete it with
 * CHIMERA_VFS_ECANCELED reported at the op that was parked.
 *
 * Called ONLY from the submitting thread -- the thread that will run, or is
 * running, the completion -- and only while the run is parked, which is what
 * the park callback tells the caller.  This is the deadline pattern, and
 * nothing else: the park callback emits the protocol's interim and arms a
 * timer on the submitting thread; the timer, if it fires first, calls this.
 * An SMB2 CANCEL, a FUSE INTERRUPT, an NLM CANCEL and a session teardown
 * want the same cancel but arrive on some other thread, and they call
 * chimera_vfs_compound_cancel_post below, which marshals here.  The caller
 * maps ECANCELED to its own cancelled status.
 *
 * Returns non-zero when the cancel TOOK, and 0 when it did not.  There is no
 * third answer, and both are ordinary:
 *
 *   NON-ZERO.  We took the park back and the run is over.  The completion
 *     callback has ALREADY RUN, inside this call, with the status
 *     CHIMERA_VFS_ECANCELED and the parked op's own status the same; the
 *     compound is the caller's to read and free exactly as after any other
 *     completion.  Whatever the parked op was waiting for will never be
 *     handed to it: a cancelled CLAIM is never granted, and a cancelled
 *     RECALL's continuation never runs.  The breaks a cancelled RECALL
 *     already kicked stay kicked -- a recall hands nobody anything, so
 *     abandoning the wait costs its victims nothing they could have acted
 *     on.  Every claim an EARLIER CLAIM in the run inserted is abort-released
 *     before the completion, on the ordinary rule (see RELEASE AND TRANSFER
 *     on the CLAIM op): a run that ends any way but OK leaves nothing
 *     inserted.  The parked CLAIM itself inserted nothing and releases
 *     nothing -- and nothing answered it, so its `claim_result` and
 *     `conflict` say nothing either: read its `status`, and
 *     chimera_vfs_compound_take_file_state(), which answers NULL for it.
 *
 *   ZERO.  The cancel lost the race and did nothing.  Either the run was not
 *     parked (it never parked, it has already finished, or it is suspended on
 *     something that is not a cancellable park -- see the park callback), or
 *     the answer the park was waiting for is already in flight and OWNS the
 *     completion: the grant is running, or about to run, on whatever thread
 *     released the conflict.  Either way the caller must not free the
 *     compound here.  It waits for the completion, which will arrive on the
 *     submitting thread carrying whatever the run actually did.
 *
 * The arbitration is the claim core's, not a guess made here:
 * chimera_vfs_claim_cancel's (and chimera_vfs_claim_recall_cancel's) return
 * value decides which of the two it is, exactly as the SMB LOCK abort and the
 * NLM CANCEL path already rely on, and the caller never races the callback by
 * hand.  So the completion fires EXACTLY ONCE for a submission whatever the
 * timing: a holder releasing on another thread at the same moment as this
 * call produces one outcome or the other, never both and never neither.
 *
 * Cancelling a run that is not parked is legal and does nothing, so a caller
 * whose timer and whose protocol event both fire need not arbitrate between
 * them.  Cancelling from inside the park callback is not: the run has not
 * finished parking yet, and the executor aborts rather than corrupt it.
 */


/*
 * Ask for a run to be cancelled FROM ANY THREAD.
 *
 * chimera_vfs_compound_cancel above is the submitting thread's call: it
 * arbitrates on the spot and runs the completion inline when it wins.  That
 * is the right shape for the deadline timer, which is armed on the submitting
 * thread by the park callback and fires there.  It is the wrong shape for
 * every other cancel a front end actually has, because every other cancel
 * TRIGGER arrives somewhere else: a FUSE_INTERRUPT is read off the kernel
 * queue by whichever thread got to it, an SMB2 CANCEL arrives on another
 * channel, an NLM CANCEL on another connection, and a session or connection
 * teardown runs wherever the disconnect landed.  Such a caller holds its own
 * state lock over the list it found the run in, and must not have a
 * completion -- which replies, frees and recycles ITS request -- run inside
 * the call, on its thread, under its lock.
 *
 * So this one decides nothing where it is called.  It marshals the request to
 * cancel onto the submitting thread through the core's own resume doorbell --
 * the same one that carries a deferred CLAIM grant home -- and the ordinary
 * chimera_vfs_compound_cancel runs there.  It never blocks, never runs the
 * completion, and never re-enters the caller: safe to call with the caller's
 * own state lock held, which is the whole point of it.
 *
 * WHAT THE CALLER MAY ASSUME ON RETURN.  Nothing about the outcome: there is
 * no return value because nothing has been arbitrated yet.  What is promised
 * is what was already promised -- the completion callback fires EXACTLY ONCE,
 * on the submitting thread, and it now carries one of two answers:
 *
 *   CHIMERA_VFS_ECANCELED, at the op that was parked.  The cancel took, and
 *     everything on chimera_vfs_compound_cancel's non-zero arm holds.
 *
 *   the run's REAL outcome.  The cancel lost, or there was nothing to cancel
 *     by the time it arrived: the grant won the race, or the run was never
 *     parked, or it had already finished.  A post that finds nothing to take
 *     back is legal and silent -- a caller whose timer and whose protocol
 *     event both fire need not arbitrate between them.
 *
 * Because the caller learns nothing synchronously, it must keep whatever the
 * completion needs -- the compound, its request, its reply buffers -- alive
 * until the completion fires.  That is the whole difference from the inline
 * form, where a taken cancel hands the completion back before the call
 * returns.
 *
 * IDEMPOTENCE.  Any number of posts for one run produce one completion.  A
 * latch on the compound admits one post at a time: a second while the first
 * is still riding is dropped, because the ride it asks for is already booked,
 * and one that arrives after the first was drained rides again and finds
 * nothing to take back.  Posting against the run's own completion is the same
 * arbitration the inline form makes, made once, on the submitting thread.
 * The two forms do not interfere: a deadline timer that fires inline while a
 * post is in flight simply cancels first, and the post arrives to find the
 * run over.
 *
 * LIVENESS -- THE CALLER'S OBLIGATION.  Post only while the compound is
 * guaranteed not to have been freed yet.  A pointer to a freed compound is a
 * use-after-free the core cannot detect, here as anywhere.  In practice this
 * is free: the consumers that need this keep their parked runs on a list
 * under a lock, a run leaves that list only in its own completion, which
 * takes the same lock, and the post is made while holding it.  The core
 * covers the half the caller CANNOT: a post made legally, while the run was
 * alive, that is still on the doorbell when the run completes and the caller
 * frees the compound.  Such a free releases everything the compound held but
 * defers the recycle to the drain, so the vehicle never lands on a compound
 * that has been handed out again.
 *
 * Unlike the inline form this is safe from inside the park callback: nothing
 * is cancelled in the call, and the drain runs long after the park has been
 * recorded.
 */
void
chimera_vfs_compound_cancel_post(
    struct chimera_vfs_compound *compound);

/*
 * Return a finished compound.  This releases anything the sequence still
 * holds -- see OPEN HANDLE OWNERSHIP -- along with everything the ops own
 * outright (the by-value ACL and SID copies in every attribute result, a
 * GET_LAYOUT's arrays, a READLINK's target, the xattr and stream buffers),
 * and recycles the compound onto the thread's free list rather than
 * returning it to the allocator.  Nothing BORROWED is touched: a lent handle,
 * a WRITE's payload, a set_attr's ACL.
 */
void
chimera_vfs_compound_free(
    struct chimera_vfs_compound *compound);

/*
 * Discard a thread's recycled compounds.  For chimera_vfs_thread_destroy();
 * no caller outside the VFS core has any business with this.
 */
void
chimera_vfs_compound_thread_destroy(
    struct chimera_vfs_thread *thread);

/* ---- results ---- */

uint32_t
chimera_vfs_compound_num_groups(
    const struct chimera_vfs_compound *compound);

uint32_t
chimera_vfs_compound_num_ops(
    const struct chimera_vfs_compound *compound);

/* How many ops actually ran (index of the failure, or all of them). */
/* Count of completed operations, not an array prefix when groups are used.
 * Inspect op->completed/status when enumerating physical operation slots. */
uint32_t
chimera_vfs_compound_num_completed(
    const struct chimera_vfs_compound *compound);

/* Finish rejection takes precedence; otherwise the first operation error. */
enum chimera_vfs_error
chimera_vfs_compound_status(
    const struct chimera_vfs_compound *compound);

const struct chimera_vfs_compound_op *
chimera_vfs_compound_op(
    const struct chimera_vfs_compound *compound,
    uint32_t                           index);

/*
 * The WRITABLE view of op `index`, for a gate editing an op that has not run --
 * see the gate's contract, which is where the rules are.  This is the only
 * sanctioned way to get one, and it enforces the one rule that can be enforced
 * exactly: it answers NULL, rather than a pointer, for anything a gate may not
 * edit -- an index at or below the one the gate is being consulted on, an index
 * past the end of the sequence, or a call from outside a gate altogether.  (A
 * debug build additionally notices a caller that edited an op that has run by
 * casting away the const above, and aborts.)
 *
 * Everything the gate writes through it is an ARGUMENT of an op that has not
 * run, and everything it writes it must write again on a re-run, computed from
 * what the caller already had.
 */
struct chimera_vfs_compound_op *
chimera_vfs_compound_op_edit(
    struct chimera_vfs_compound *compound,
    uint32_t                     index);

/* Create `name` in the current object; it becomes current.  `set_attr` may be
 * NULL.  `target` is the symlink target and is required for -- and only read
 * for -- CHIMERA_VFS_COMPOUND_CREATE_SYMLINK; it is copied.
 * namespace_flags accepts the matching MKDIR/MKNOD/SYMLINK_NO_NOTIFY flag;
 * the frontend then owns notification publication after accepted finish. */
int
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
    uint64_t                        dir_post_attr_mask);

/* Rename `name` in the saved object to `new_name` in the current object.  A
 * sequence that reaches this without a SAVEFH fails the op with EINVAL, the
 * same answer RESTOREFH gives an empty saved slot -- the adder cannot tell,
 * because whether a SAVEFH ran is a property of the sequence as it executes.
 * Strict MATCH_SOURCE_FH uses op.arg_fh/arg_fh_len as the immutable expected
 * source identity. MATCH_DEST_FH uses rename_target_fh/rename_target_fh_len;
 * rename_outcome reports atomic MOVED/NOOP or UNKNOWN without backend support.
 * NOREPLACE/MATCH require explicit backend capabilities; NO_NOTIFY leaves
 * observer events to accepted frontend publication. */
int
chimera_vfs_compound_add_rename(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen,
    const char                  *new_name,
    int                          new_namelen,
    unsigned int                 flags,
    uint64_t                     dir_pre_attr_mask,
    uint64_t                     dir_post_attr_mask);

/* Link the saved object into the current object as `name`.  `attr_mask` is
 * fetched against the newly linked object, for a caller whose reply describes
 * it; a protocol that reports only the directory change passes 0. */
int
chimera_vfs_compound_add_link(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen,
    uint64_t                     attr_mask,
    uint64_t                     dir_pre_attr_mask,
    uint64_t                     dir_post_attr_mask);

/* Unlink `name` from the current object, which stays current.
 *
 * `flags` is the CHIMERA_VFS_REMOVE_* set the per-op API takes, and means the
 * same here: the type assertion a protocol owes (ISDIR for rmdir, ISNOTDIR for
 * unlink) and whether the VFS should resolve the doomed object to recall
 * leases on it.  A protocol that makes those decisions itself passes 0. */
int
chimera_vfs_compound_add_remove(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen,
    unsigned int                 flags,
    uint64_t                     dir_pre_attr_mask,
    uint64_t                     dir_post_attr_mask);

/* Read `count` bytes from `offset` of the current object, or -- when `handle` is
 * non-NULL -- of that handle, which is BORROWED.
 *
 * `iov` is space for `max_iov` descriptors, supplied by the caller and BORROWED
 * the same way: an evpl_iovec records the address of the struct that owns it,
 * so descriptors cannot be copied anywhere afterwards, which means the memory
 * they are written into has to be memory the caller is willing to keep.  A read
 * needing more than `max_iov` is served short, as any short read is.
 *
 * The DATA those descriptors reference is owned by the compound until taken --
 * see OPEN HANDLE OWNERSHIP.
 *
 * `dest_iov` / `dest_niov`, when given, are the caller's own buffers for the
 * data to land in, and turn the op into chimera_vfs_read_into: `iov` is then
 * the scratch array that call takes, the result's iov/niov are dest_iov and
 * dest_niov handed back, the first read_len bytes of them filled, and the
 * compound owns nothing -- the caller releases its buffers after the run as it
 * releases a WRITE's.  NULL / 0 is today's READ.  There is no owned variant of
 * read_into, so a read that lands in the caller's buffers cannot also name an
 * `io_owner`; supplying both is refused at build. */
int
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
    int                               dest_niov);

/* Write `count` bytes of `iov` at `offset` to the current object, or -- when
 * `handle` is non-NULL -- to that handle.  Both `handle` and `iov` are
 * BORROWED: the caller holds them for as long as the sequence runs and releases
 * them afterwards.
 *
 * `pre_attr_mask` and `post_attr_mask` sample the file either side of the
 * write, into the op's `pre_attr` and `attr`.  A protocol that reports the
 * change -- NFSv3's wcc_data, SMB2 restoring the mtime a write advanced on a
 * write-time-sticky handle -- needs those two readings to be atomic with the
 * write, which a separate GETATTR in the same sequence would not be.
 *
 * `io_owner` here is a value the caller already holds.  A caller keying the
 * owner on an open handle -- NFSv4 attributes an operation under a stateid to
 * (client, that handle's fh_hash) -- holds no such value when the OPEN is IN
 * this run: the handle does not exist when the sequence is written.  There is
 * nothing to add for it, because `io_owner` and `have_io_owner` are ARGUMENTS
 * of the op, and an argument a run computes for itself is a GATE's to write.
 *
 * owner_lo is what it has to compute, and fh_hash is chimera_vfs_hash of the
 * FILE HANDLE and nothing else -- it names the OBJECT, not the open -- so the
 * value exists as soon as the object does.  A caller that already knows the fh
 * (an anonymous or delegation stateid, whose object is the one the run starts
 * from) computes owner_lo at build and passes it here.  One whose object this
 * run resolves has the gate consulted on the resolving op read its fh -- or its
 * out_handle's fh_hash, which is the same number -- and write both fields into
 * the READ or WRITE ahead of it, through chimera_vfs_compound_op_edit.
 *
 * That is an ordinary gate edit under the ordinary rule: ASSIGNED from what the
 * caller already had plus what the finished op reported, never accumulated onto
 * what it wrote last time, so a second execution computes the same owner.
 *
 * Left unwritten, the claim layer reads the client's own I/O as a stranger's:
 * it denies it against the client's own share reservation and recalls the
 * delegation the I/O is being done under. */
int
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
    const struct chimera_claim_actor *io_owner);

/* Apply `set_attr` to the current object, or -- when `handle` is non-NULL -- to
 * that handle with descriptor rights.  `handle` is BORROWED: see ADDRESSING
 * SOMETHING OTHER THAN CURRENT.  On return the op's `applied_attr` reports which
 * attributes were actually applied. */
int
chimera_vfs_compound_add_setattr(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *handle,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        pre_attr_mask,
    uint64_t                        attr_mask);

/* Overwrite the exact current/borrowed object after frontend admission. Uses
 * chimera_vfs_overwrite semantics, including removal of a base file's named
 * streams. set_attr must request SIZE=0; attr/applied_attr report the post-state
 * and applied fields. The actor and attributes are copied; handle is borrowed. */
int
chimera_vfs_compound_add_overwrite(
    struct chimera_vfs_compound      *compound,
    struct chimera_vfs_open_handle   *handle,
    const struct chimera_vfs_attrs   *set_attr,
    uint64_t                          attr_mask,
    const struct chimera_claim_actor *io_owner);

/* Give an op the handle it should act on, after appending it -- see ADDRESSING
 * SOMETHING OTHER THAN CURRENT.  Separate from the adders because most callers
 * address the current object and would carry an argument they never use. */
void
chimera_vfs_compound_op_set_handle(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    struct chimera_vfs_open_handle *handle);

/* ---- the name-op setters ----
 *
 * Each is applied to the op at `index` after its adder returned, on the same
 * reasoning as op_set_handle: the knobs are SMB's and a few NFSv4 paths', and
 * the common caller pays no argument it never uses.  The index must name an
 * op of the right type -- anything else is a caller bug and aborts, because a
 * knob silently applied to the wrong op is a sequence that quietly does
 * something other than what was written.  An index past the end is ignored,
 * as op_use_handle ignores one: the adder already reported the failure.
 *
 * File handles and the lease key are COPIED; the exempt handle and the
 * handle-state record are BORROWED, on the in_handle terms. */

/* REMOVE: with `match` set, unlink `name` only while it still resolves to
 * `child_fh` (remove_at_match_fh) -- so an asynchronous delete-on-close cannot
 * destroy an unrelated object that has since taken the name.  A name that no
 * longer resolves to it is left intact and the op reports OK: the caller's
 * object is already gone, which is the outcome it wanted.  `match` therefore
 * requires a child_fh.  Without `match`, a non-NULL child_fh is the recall
 * target remove_at would otherwise resolve for itself, and the op's
 * remove_flags apply as usual; with it, remove_at_match_fh takes no flags, so
 * the type assertion and the recall request are the caller's to have already
 * made -- which every match caller (SMB delete-on-close, the durable reap)
 * has.  `parent_lease_skip` is the 16-byte directory lease key to spare, or
 * NULL to spare none. */
void
chimera_vfs_compound_op_set_remove_match(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    const uint8_t               *child_fh,
    uint32_t                     child_fh_len,
    int                          match,
    const uint8_t               *parent_lease_skip);

/* RENAME: `target_fh` is the object already at the destination name when the
 * caller resolved it (NULL / 0 leaves rename_at to resolve it), `op_exempt_
 * handle` the operating open whose own file lease the source recall spares,
 * `parent_lease_skip` the directory lease to spare (16 bytes or NULL), and
 * `flags` is CHIMERA_VFS_RENAME_SRC_IS_DIR or 0, which the executor ORs into
 * the word rename_at takes beside the adder's remove_flags. */
void
chimera_vfs_compound_op_set_rename_opts(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    const uint8_t                  *target_fh,
    uint32_t                        target_fh_len,
    struct chimera_vfs_open_handle *op_exempt_handle,
    const uint8_t                  *parent_lease_skip,
    unsigned int                    flags);

/* LINK: `replace` clobbers an existing destination name (the SMB
 * ReplaceIfExists rename-via-link and the S3 publish; link(2) never does),
 * and the other two are as for RENAME. */
void
chimera_vfs_compound_op_set_link_opts(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    int                             replace,
    const uint8_t                  *parent_lease_skip,
    struct chimera_vfs_open_handle *op_exempt_handle);

/* OPEN, OPEN_PATH: persist `handle_state` with the open.  BORROWED, and it
 * must outlive the run.  NULL is a plain open.
 *
 * What "with" means depends on the backend, and the caller decides whether
 * that is good enough BEFORE building the sequence, with
 * chimera_vfs_can_persist_handle_state -- exactly as the SMB create path
 * does today:
 *
 *   CHIMERA_VFS_CAP_ATOMIC_HANDLE_STATE  the backend stores the record in
 *     the same transaction as the open (cairn).
 *
 *   no capability, a named OPEN  the VFS core stores the record in the
 *     default KV AFTER the open has succeeded, keyed by the new object's fh
 *     -- a second, non-atomic write that only the in-memory and passthrough
 *     backends ever take.  It is best-effort: a KV failure is logged and the
 *     open still succeeds, without its record.  With no default KV configured
 *     the record is not stored at all and nothing says so.
 *
 *   no capability, an unnamed OPEN (a re-open of the current object by fh)
 *     the record is handed to the backend and, lacking the capability, the
 *     backend ignores it: open_fh has no default-KV fallback.  A caller that
 *     needs the record on such a backend opens by name.
 *
 *   OPEN_PATH  there is no path-addressed open that takes a record, so an
 *     OPEN_PATH carrying one fails ENOTSUP at execution rather than opening
 *     without it.  No consumer needs the combination: SMB, the one caller of
 *     handle state, opens by name through a directory it holds open. */
void
chimera_vfs_compound_op_set_handle_state(
    struct chimera_vfs_compound     *compound,
    uint32_t                         index,
    struct chimera_vfs_handle_state *handle_state);

/* Take ownership of an OPEN's handle: returns it and clears out_handle, so the
 * compound will not release it and the caller must.  NULL if that op is not an
 * OPEN, did not run, failed, or has already been taken. */
struct chimera_vfs_open_handle *
chimera_vfs_compound_take_handle(
    struct chimera_vfs_compound *compound,
    uint32_t                     index);

/* Take ownership of a CLAIM's file state: returns it and clears the op's copy,
* so the compound will not put it and the caller must.  NULL if that op is not
* a CLAIM, did not run, was not GRANTED, was GRANTED in a sequence that then
* failed (the claim was released with the failure -- see RELEASE AND TRANSFER
* on the CLAIM op), or has already been taken.  A caller whose sequence
* finished OK MUST take it: the claim is inserted and is the caller's, and the
* file state is what releasing it needs -- whatever the kind: a range, a
* share, a delegation, or a coalition grant (whose handle is `claim_grant`). */
struct chimera_vfs_file_state *
chimera_vfs_compound_take_file_state(
    struct chimera_vfs_compound *compound,
    uint32_t                     index);

/* Take ownership of a READ's data: on return the *iov / *niov references are the
 * caller's to release, and the compound will not.  *iov is the array the caller
 * supplied, which it has owned all along.  *niov is 0 if that op is not a READ,
 * did not run, failed, or has already been taken.
 *
 * A READ with dest_iov has nothing of the compound's to hand over -- the data
 * landed in the caller's own buffers, which it owns and releases regardless
 * -- so this answers NULL / 0 for it, safe to call and to release the answer
 * of, and leaves the op's iov / niov readable.  The caller that supplied
 * dest_iov already holds the references this would otherwise be moving. */
void
chimera_vfs_compound_take_iov(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    struct evpl_iovec          **iov,
    int                         *niov);



/* Remove a discovered list without allocating an operation slot per path.
 * paths and its strings must outlive the compound attempt; a retry's FIND
 * callback may construct a new suffix with a new private list. */
int
chimera_vfs_compound_add_remove_paths(
    struct chimera_vfs_compound *compound,
    const char *const           *paths,
    uint32_t                     num_paths,
    unsigned int                 flags,
    int                          ignore_errors);

/* Named data forks. OPEN_STREAM selects the stream as current and owns its
 * out_handle under the ordinary OPEN rules. LIST/REMOVE address the current
 * base object (or explicit op handle); list results are compound-owned packed
 * chimera_vfs_stream_entry records in buffer with buffer_len/buffer_count,
 * eof/r_cookie. Names and attribute inputs follow the ordinary builder rules. */
int chimera_vfs_compound_add_open_stream(
    struct chimera_vfs_compound    *compound,
    const char                     *name,
    int                             namelen,
    unsigned int                    flags,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask);

int chimera_vfs_compound_add_remove_stream(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen);

/* Owns a copy of the expected stream FH. Strict atomic identity mismatch is
 * ESTALE, unsupported backend is ENOTSUP before mutation. */
int chimera_vfs_compound_add_remove_stream_checked(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen,
    const uint8_t               *expected_fh,
    uint32_t                     expected_fh_len);

bool
chimera_vfs_compound_cancel(
    struct chimera_vfs_compound *compound);

int
chimera_vfs_compound_add_create_unlinked(
    struct chimera_vfs_compound    *compound,
    unsigned int                    flags,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask);

int
chimera_vfs_compound_add_find(
    struct chimera_vfs_compound         *compound,
    uint64_t                             attr_mask,
    chimera_vfs_compound_readdir_reset_t reset,
    chimera_vfs_compound_find_entry_t    filter,
    chimera_vfs_compound_find_entry_t    append,
    void                                *private_data);

int chimera_vfs_compound_add_list_streams(
    struct chimera_vfs_compound *compound,
    uint64_t                     cookie,
    uint32_t                     max_bytes,
    bool                         want_fh);

int chimera_vfs_compound_add_close(
    struct chimera_vfs_compound *compound);

void chimera_vfs_compound_set_park_cb(
    struct chimera_vfs_compound   *compound,
    chimera_vfs_compound_park_cb_t park_cb,
    void                          *private_data);

/* FIND with operation-aware streaming callbacks. Both callback forms share
 * staging, reset, type validation and early-stop semantics. */
int chimera_vfs_compound_add_find_stream(
    struct chimera_vfs_compound         *compound,
    uint64_t                             attr_mask,
    chimera_vfs_compound_find_filter_t   filter,
    chimera_vfs_compound_find_append_t   append,
    chimera_vfs_compound_readdir_reset_t reset,
    void                                *private_data);
