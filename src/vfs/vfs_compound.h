// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "vfs.h"
#include "vfs_claim_types.h"

/*
 * VFS compounds: submit a whole sequence of operations, get one callback.
 *
 * The existing per-op API is unchanged and remains the way most callers work.
 * This is an ADDITIONAL entrance for a caller that already knows, at the time
 * it starts, everything it wants to do -- which is true of every protocol
 * front end: one NFS3 RPC, one NFS4 COMPOUND, one SMB2 chain, one FUSE
 * request, one SDK call.  Such a caller builds the whole sequence, submits it,
 * and is called back once when the sequence is over.
 *
 * WHY.  Two things follow from the VFS owning the sequence rather than the
 * caller driving it op by op.  The obvious one is that the chaining every
 * protocol already does by hand -- "the object the previous operation
 * resolved" -- moves into one place: ops address CURRENT, the VFS threads the
 * result forward, and the caller never handles an intermediate file handle.
 * The one that matters more is that the caller is no longer between the
 * operations.  It sees results only when the sequence has finished, so
 * anything the VFS does in the middle -- waiting on a lease recall, and later,
 * retrying the sequence after a conflict -- is invisible, and the caller
 * cannot have acted on an answer that a retry would invalidate.
 *
 * WHAT THIS IS NOT.  The sequence is not handed to a backend as a batch and it
 * is not atomic.  The VFS executes the ops one at a time through the ordinary
 * per-op path, so every backend, every cache and every claim behaves exactly
 * as it does for an unsequenced op.  Nothing here changes what an operation
 * means; it changes who holds the sequence.
 *
 * ERRORS.  Execution stops at the first op that fails -- the NFS4 rule, and
 * the only sane one for a sequence whose later ops address what the earlier
 * ones resolved.  Ops that ran carry their own status and results; ops after
 * the failure did not run and are left CHIMERA_VFS_UNSET.
 *
 * MUTATION.  Most ops here are read-only, but SETXATTR and REMOVEXATTR are not.
 * Stopping at a failure therefore leaves the mutations of the ops that already
 * ran applied: a sequence is not a transaction and is not rolled back.  That is
 * exactly what the op-at-a-time path does with the same sequence of calls -- the
 * ops are the same ops -- and it is the only behaviour available while nothing
 * retries, which nothing does yet.  A caller that needs all-or-nothing wants the
 * VFS transaction API, not this.
 *
 * PARKING.  An op that must wait -- for a lease break, for a peer's ack --
 * parks exactly as it would outside a sequence.  The sequence simply does not
 * advance until it completes.  Nothing is held that would not otherwise be
 * held, because the ops are the same ops.
 *
 * ADDRESSING SOMETHING OTHER THAN CURRENT.  Ops address the current object,
 * which is what makes a sequence a sequence.  One kind of caller cannot: a
 * protocol whose request names an object by something it resolved itself -- an
 * NFSv4 stateid, an SMB2 file id -- already holds an open handle for it, and
 * the operation has to run against that handle rather than against whatever the
 * sequence last resolved.
 *
 * Such an op carries an `in_handle`.  It is BORROWED: the caller opened it, the
 * caller holds the reference for as long as the sequence runs, and the caller
 * releases it afterwards.  The executor uses it and does nothing else with it,
 * which is the whole of the rule -- the mirror of OPEN's out_handle, where the
 * ownership runs the other way.
 *
 * An in_handle does not move the current object.  The op acts on the handle;
 * the sequence's own idea of where it is stays where it was.
 *
 * OPEN HANDLE OWNERSHIP.  Most ops leave nothing behind: the executor opens
 * what it needs, and releases it when the current object moves on or the
 * sequence ends.  Two are different, because what they produce is the whole
 * point of them and has to outlive the sequence -- an OPEN's handle, and a
 * READ's data iovecs.
 *
 * The rule for both is that the compound owns it until the caller takes it.  On
 * the completion callback they are readable as op->out_handle and op->iov;
 * chimera_vfs_compound_take_handle() and chimera_vfs_compound_take_iov()
 * transfer them, after which the caller releases them.  Anything the caller does
 * NOT take is released by chimera_vfs_compound_free(), so the failure paths -- a
 * later op failed, the caller decided not to install the state, the caller
 * simply forgot -- leak nothing.  Defaulting to "the compound still owns it" is
 * deliberate: a caller that must remember to release on every error path is a
 * caller that eventually does not.
 */

struct chimera_vfs_compound;

enum chimera_vfs_compound_op_type {
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
     * OPEN HANDLE OWNERSHIP. */
    CHIMERA_VFS_COMPOUND_OP_READ,
    /* Write to the current object, or to `in_handle`.  The data iovecs are
     * BORROWED, like in_handle: the caller owns the buffers and releases them
     * once the sequence is over.  MUTATES. */
    CHIMERA_VFS_COMPOUND_OP_WRITE,
    /* Apply `set_attr`.  With an `in_handle` the attributes are applied through
     * it with descriptor rights -- the ftruncate(2) rule, where the open's own
     * grant authorizes the change rather than the object's current mode --
     * which is why a protocol that resolved a handle for itself hands it in
     * rather than letting this re-open by name.  Without one, the current
     * object is opened and the attributes applied against the object's mode.
     * MUTATES. */
    CHIMERA_VFS_COMPOUND_OP_SETATTR,
    /* Extended attributes of the current object.  SETXATTR and REMOVEXATTR
     * MUTATE -- see the MUTATION note above. */
    CHIMERA_VFS_COMPOUND_OP_GETXATTR,
    CHIMERA_VFS_COMPOUND_OP_SETXATTR,
    CHIMERA_VFS_COMPOUND_OP_LISTXATTRS,
    CHIMERA_VFS_COMPOUND_OP_REMOVEXATTR,
};

#define CHIMERA_VFS_COMPOUND_MAX_OPS             32
#define CHIMERA_VFS_COMPOUND_NAME_MAX            255

/*
 * A READDIR's result is a page, not the whole directory: the caller says how
 * many entries it can use, the executor keeps that many and stops the
 * enumeration there -- reporting eof=0 and the cookie of the first entry it
 * refused, exactly as it would for any caller that stopped early.  A caller
 * wanting more issues another READDIR from that cookie.
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

struct chimera_vfs_compound_dirent {
    uint64_t                 inum;
    uint64_t                 cookie;
    uint32_t                 name_len;
    char                     name[CHIMERA_VFS_COMPOUND_NAME_MAX + 1];
    /* As for every other attribute result here, va_acl is NULL and the ACL bit
     * is clear: the backend owns the ACL only for the duration of the entry
     * callback. */
    struct chimera_vfs_attrs attr;
};

struct chimera_vfs_compound_op {
    uint8_t                             type;
    /* CHIMERA_VFS_UNSET until the op has run. */
    enum chimera_vfs_error              status;

    /* ---- arguments ---- */
    uint8_t                             arg_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                            arg_fh_len;
    char                                name[CHIMERA_VFS_COMPOUND_NAME_MAX + 1];
    uint32_t                            name_len;
    uint64_t                            attr_mask;
    uint32_t                            requested;
    uint64_t                            offset; /* COMMIT                             */
    uint64_t                            count; /* COMMIT                             */
    uint64_t                            cookie; /* READDIR, LISTXATTRS                */
    uint64_t                            verifier; /* READDIR                            */
    uint32_t                            dircount; /* READDIR (advisory; see the adder)  */
    uint32_t                            maxcount; /* READDIR (advisory; see the adder)  */
    uint32_t                            max_entries; /* READDIR                            */
    /* Address this handle instead of the current object.  BORROWED from the
     * caller -- see ADDRESSING SOMETHING OTHER THAN CURRENT above.  NULL for
     * every op that addresses the current object, which is most of them. */
    struct chimera_vfs_open_handle     *in_handle;
    uint8_t                             create_type; /* CREATE                             */
    /* CREATE of a symlink: its target.  Copied by the adder and owned by the
     * compound, so the caller need not keep it alive. */
    char                               *link_target;
    uint32_t                            link_target_len;
    /* RENAME: the name in the CURRENT object to rename to.  `name` is the one
     * in the saved object to rename from. */
    char                                new_name[CHIMERA_VFS_COMPOUND_NAME_MAX + 1];
    uint32_t                            new_name_len;
    unsigned int                        open_flags; /* OPEN: CHIMERA_VFS_OPEN_*           */
    uint32_t                            open_opts; /* OPEN: CHIMERA_VFS_COMPOUND_OPEN_*  */
    /* OPEN and CREATE: attributes to apply to a created object.  Read by the
     * executor at execution time, so ATTRS_ON_CREATE_ONLY can clear it once the
     * name has been resolved. */
    struct chimera_vfs_attrs            set_attr;
    uint32_t                            xattr_option; /* SETXATTR                          */
    const void                         *xattr_value; /* SETXATTR (borrowed from caller)    */
    uint32_t                            xattr_value_len;
    uint32_t                            buffer_max; /* GETXATTR, LISTXATTRS               */
    int                                 max_iov; /* READ                               */
    /* WRITE: the data, BORROWED from the caller -- see ADDRESSING SOMETHING
     * OTHER THAN CURRENT, which these are owned on the same terms as. */
    struct evpl_iovec                  *w_iov;
    int                                 w_niov;
    uint32_t                            sync; /* WRITE: requested stability         */
    /* READ, WRITE: whose I/O this is.  A caller holding a lease on the object
     * has to say so, or the claim layer arbitrates its own I/O against its own
     * reservation -- denying the write, and recalling the delegation the write
     * is being done under. */
    struct chimera_claim_actor          io_owner;
    uint8_t                             have_io_owner;
    /* Executor scratch: whether the two-step I/O type check has run.  Lives on
     * the op only so the open-flags decision, which sees an op and not the
     * sequence, can tell the two steps apart. */
    uint8_t                             io_typechecked_flag;

    /* ---- results ---- */
    /* LOOKUP, GETATTR, ACCESS.  va_acl is always NULL here and the ACL bit is
     * always clear in the masks: a backend reports an ACL by pointing at its
     * own live inode state, which does not outlive its completion, so a copy
     * that survives the callback cannot carry one.  Rather than leave a
     * pointer that looks valid and is not, the sequence drops it -- a caller
     * that needs an ACL issues that getattr itself.  ACCESS's `granted` is
     * computed while the ACL is still live, so it is unaffected. */
    struct chimera_vfs_attrs            attr;
    /* The current object AFTER this op ran: what a LOOKUP resolved, what a
     * PUTFH selected, and for everything else the object the op addressed.
     * A caller that must describe the object an op acted on -- which is most
     * of what a protocol reply is -- would otherwise have to re-derive it. */
    uint8_t                             fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                            fh_len;
    uint32_t                            granted; /* ACCESS                            */
    char                               *target; /* READLINK (owned by the compound)  */
    uint32_t                            target_len;

    /* SETXATTR, REMOVEXATTR.  Only the ctime is kept: it is the whole of what
     * a change_info reply needs, and keeping two more attribute sets per op
     * would double the size of a sequence for one field. */
    struct timespec                     pre_ctime;
    struct timespec                     post_ctime;

    /* ---- READ results ---- */
    /* The data, as references to the backend's buffers, written into the array
     * the caller supplied.  The references are owned by the compound until
     * chimera_vfs_compound_take_iov(); the array never is. */
    struct evpl_iovec                  *iov;
    int                                 niov;
    uint32_t                            read_len;
    uint32_t                            eof_read;

    /* ---- WRITE results ---- */
    uint32_t                            written;
    /* Durability actually achieved, which may exceed what was asked for and
     * may fall short of it only by the backend's own report. */
    uint32_t                            committed;

    /* ---- OPEN results ---- */
    /* The open handle, owned by the CALLER once the sequence has finished --
     * see OPEN HANDLE OWNERSHIP below.  NULL if the op did not run or failed. */
    struct chimera_vfs_open_handle     *out_handle;
    /* Whether the open created the object. */
    uint8_t                             created;
    /* Set when the executor resolved the name before opening (which it does
     * for REGULAR_ONLY or ATTRS_ON_CREATE_ONLY) and found an existing object.
     * `existing_mode` is that object's mode -- the whole point of the
     * REGULAR_ONLY failure, whose status says only that the open was refused
     * and not what was in the way. */
    uint8_t                             existed;
    uint32_t                            existing_mode;
    /* The parent directory before and after, for a change_info reply.  Set by
     * CREATE and REMOVE, and by an OPEN that named a child. */
    struct chimera_vfs_attrs            dir_pre_attr;
    struct chimera_vfs_attrs            dir_post_attr;
    /* RENAME only: the SOURCE directory's change_info.  The pair above is the
     * target's, which is what every other name-changing op reports.  Both are
     * filled because rename_at hands back both and NFSv4's RENAME reply has a
     * slot for each -- source_cinfo and target_cinfo. */
    struct chimera_vfs_attrs            from_dir_pre_attr;
    struct chimera_vfs_attrs            from_dir_post_attr;

    /* READDIR.  `entries` is allocated on demand and owned by the compound. */
    struct chimera_vfs_compound_dirent *entries;
    uint32_t                            num_entries;
    /* READDIR and LISTXATTRS: whether the enumeration reached the end, and the
     * cookie to resume it from, as the backend reported them when it stopped.
     * r_verifier is the directory's verifier (READDIR only). */
    uint32_t                            eof;
    uint64_t                            r_cookie;
    uint64_t                            r_verifier;

    /* GETXATTR (value), LISTXATTRS (back-to-back NUL-terminated names).  Owned
     * by the compound, buffer_max bytes, valid until it is freed. */
    void                               *buffer;
    uint32_t                            buffer_len;
    uint32_t                            buffer_count;  /* LISTXATTRS: names   */
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
 */
typedef void (*chimera_vfs_compound_gate_t)(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    enum chimera_vfs_error      *status,
    void                        *private_data);

typedef void (*chimera_vfs_compound_callback_t)(
    struct chimera_vfs_compound *compound,
    void                        *private_data);

/* Allocate a sequence.  `cred` must outlive the submission. */
struct chimera_vfs_compound *
chimera_vfs_compound_alloc(
    struct chimera_vfs_thread     *thread,
    const struct chimera_vfs_cred *cred);

/* Append one op.  Returns its index, or -1 if the sequence is full (or the
 * name is too long).  A caller that cannot express an operation should build
 * no sequence at all and use the per-op API, rather than submitting a partial
 * one. */
int
chimera_vfs_compound_add_putfh(
    struct chimera_vfs_compound *compound,
    const void                  *fh,
    int                          fhlen);

int
chimera_vfs_compound_add_lookup(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen,
    uint64_t                     attr_mask);

int
chimera_vfs_compound_add_getattr(
    struct chimera_vfs_compound *compound,
    uint64_t                     attr_mask);

int
chimera_vfs_compound_add_access(
    struct chimera_vfs_compound *compound,
    uint32_t                     requested);

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

/* `pre_attr_mask` is fetched against the object before the flush, so a caller
 * that must classify what it just committed does not need a separate getattr. */
int
chimera_vfs_compound_add_commit(
    struct chimera_vfs_compound *compound,
    uint64_t                     offset,
    uint64_t                     count,
    uint64_t                     pre_attr_mask);

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
    uint64_t                     attr_mask);

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

/* Open, and with CHIMERA_VFS_OPEN_CREATE create, `name` in the current object;
 * the opened object becomes current.  A NULL (or empty) `name` opens the
 * current object itself, which is how a protocol re-opens by file handle.
 *
 * `flags` is an ordinary CHIMERA_VFS_OPEN_* word and means exactly what it
 * means to chimera_vfs_open_at.  `opts` selects the two resolve-first
 * behaviours documented on CHIMERA_VFS_COMPOUND_OPEN_* above.  `set_attr` may
 * be NULL; it is copied, so the caller need not keep it alive.
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
    uint64_t                        attr_mask);

/* Register a veto consulted as each op finishes -- see
 * chimera_vfs_compound_gate_t.  Optional; without one the sequence is governed
 * by the ops' own statuses alone. */
void
chimera_vfs_compound_set_gate(
    struct chimera_vfs_compound *compound,
    chimera_vfs_compound_gate_t  gate,
    void                        *private_data);

/* Execute the sequence.  The callback fires exactly once, on the submitting
 * thread, when execution has stopped -- because every op ran or because one
 * failed.  The compound stays valid until the caller frees it. */
void
chimera_vfs_compound_submit(
    struct chimera_vfs_compound    *compound,
    chimera_vfs_compound_callback_t callback,
    void                           *private_data);

void
chimera_vfs_compound_free(
    struct chimera_vfs_compound *compound);

/* ---- results ---- */

uint32_t
chimera_vfs_compound_num_ops(
    const struct chimera_vfs_compound *compound);

/* How many ops actually ran (index of the failure, or all of them). */
uint32_t
chimera_vfs_compound_num_completed(
    const struct chimera_vfs_compound *compound);

/* The first failing status, or CHIMERA_VFS_OK. */
enum chimera_vfs_error
chimera_vfs_compound_status(
    const struct chimera_vfs_compound *compound);

const struct chimera_vfs_compound_op *
chimera_vfs_compound_op(
    const struct chimera_vfs_compound *compound,
    uint32_t                           index);

/* Create `name` in the current object; it becomes current.  `set_attr` may be
 * NULL.  `target` is the symlink target and is required for -- and only read
 * for -- CHIMERA_VFS_COMPOUND_CREATE_SYMLINK; it is copied. */
int
chimera_vfs_compound_add_create(
    struct chimera_vfs_compound    *compound,
    uint8_t                         create_type,
    const char                     *name,
    int                             namelen,
    const char                     *target,
    int                             targetlen,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask);

/* Rename `name` in the saved object to `new_name` in the current object.  A
 * sequence that reaches this without a SAVEFH fails the op with EINVAL, the
 * same answer RESTOREFH gives an empty saved slot -- the adder cannot tell,
 * because whether a SAVEFH ran is a property of the sequence as it executes. */
int
chimera_vfs_compound_add_rename(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen,
    const char                  *new_name,
    int                          new_namelen);

/* Link the saved object into the current object as `name`. */
int
chimera_vfs_compound_add_link(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen);

/* Unlink `name` from the current object, which stays current. */
int
chimera_vfs_compound_add_remove(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen);

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
 * see OPEN HANDLE OWNERSHIP. */
int
chimera_vfs_compound_add_read(
    struct chimera_vfs_compound      *compound,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint32_t                          count,
    struct evpl_iovec                *iov,
    int                               max_iov,
    const struct chimera_claim_actor *io_owner);

/* Write `count` bytes of `iov` at `offset` to the current object, or -- when
 * `handle` is non-NULL -- to that handle.  Both `handle` and `iov` are
 * BORROWED: the caller holds them for as long as the sequence runs and releases
 * them afterwards. */
int
chimera_vfs_compound_add_write(
    struct chimera_vfs_compound      *compound,
    struct chimera_vfs_open_handle   *handle,
    uint64_t                          offset,
    uint32_t                          count,
    uint32_t                          sync,
    struct evpl_iovec                *iov,
    int                               niov,
    const struct chimera_claim_actor *io_owner);

/* Apply `set_attr` to the current object, or -- when `handle` is non-NULL -- to
 * that handle with descriptor rights.  `handle` is BORROWED: see ADDRESSING
 * SOMETHING OTHER THAN CURRENT.  On return the op's `set_attr` reports which
 * attributes were actually applied. */
int
chimera_vfs_compound_add_setattr(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *handle,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask);

/* Take ownership of an OPEN's handle: returns it and clears out_handle, so the
 * compound will not release it and the caller must.  NULL if that op is not an
 * OPEN, did not run, failed, or has already been taken. */
struct chimera_vfs_open_handle *
chimera_vfs_compound_take_handle(
    struct chimera_vfs_compound *compound,
    uint32_t                     index);

/* Take ownership of a READ's data: on return the *iov / *niov references are the
 * caller's to release, and the compound will not.  *iov is the array the caller
 * supplied, which it has owned all along.  *niov is 0 if that op is not a READ,
 * did not run, failed, or has already been taken. */
void
chimera_vfs_compound_take_iov(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    struct evpl_iovec          **iov,
    int                         *niov);
