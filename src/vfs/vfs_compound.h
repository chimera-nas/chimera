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
 * ADDRESSING: FOUR CURSORS.  A sequence carries four pieces of state, and every
 * op reads whichever ones its underlying VFS call takes.  No op is handed a file
 * handle or an open handle as an argument -- that is what the cursors are for,
 * and four different ways of saying "act on this object" is what this design
 * replaced.
 *
 *   CURRENT FH     a file handle: a NAME.  Set by PUTFH/PUTROOT, and by the ops
 *                  that resolve an object (LOOKUP, and the path ops).
 *   SAVED FH       a parked copy of it.  SAVEFH COPIES -- a name may exist in
 *                  two places, and NFSv4's wire SAVEFH leaves the current
 *                  filehandle in place, so the sequence must too.
 *   CURRENT OPEN   an open handle: a REFERENCE.  Set by OPEN (which opens the
 *                  current FH) and by PUTHANDLE (which lends the caller's).
 *   SAVED OPEN     a parked one.  SAVEHANDLE MOVES -- a reference has exactly
 *                  one owner, so parking it takes it out of the current slot.
 *                  (A copy would mean two owners; see the SAVED SLOT note in
 *                  vfs_compound.c for why that was avoided.)
 *
 * So the two two-operand families fall out symmetrically: RENAME and LINK name
 * two objects and read (SAVED FH, CURRENT FH); COPY_RANGE, CLONE_RANGE and
 * MOVE_RANGE act on two open files and read (SAVED OPEN, CURRENT OPEN).
 *
 * OPENING IS EXPLICIT.  The sequence never opens anything by itself.  A caller
 * that wants to GETATTR an object says PUTFH, OPEN, GETATTR -- which is what it
 * already wrote by hand before sequences existed, and it is the caller, not the
 * VFS, that knows what the open is for.  The alternative (the executor choosing
 * open flags from a per-op-type table) is what produced an O_PATH descriptor
 * handed to fgetxattr, and a data open of a FIFO that blocked.
 *
 * It also buys what an implicit open cannot: three lookups in one directory are
 * OPEN once and LOOKUP three times, where an implicitly-opened sequence must
 * re-PUTFH the parent between them and re-open it each time.
 *
 * HANDLE OWNERSHIP, in three rules:
 *
 *   1. The sequence owns what OPEN opened, and releases it when the slot is
 *      overwritten or the sequence ends.
 *   2. GETHANDLE transfers that ownership to the caller, which collects the
 *      handle from the op's result and releases it itself.  PUTHANDLE's handle
 *      is the caller's already and is never released by the sequence.
 *   3. CLOSE ends the handle whatever its provenance -- that is the point of
 *      it, and it is what an SMB2 CLOSE or an NFSv4 CLOSE means.  After a CLOSE
 *      the caller must NOT release that handle itself, including one it lent
 *      with PUTHANDLE.
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
 * It names the object the op ACTS ON, so it is accepted by every op that
 * addresses one -- the I/O ops, SETATTR, GETATTR, ACCESS, READLINK, COMMIT,
 * READDIR and the xattr ops.  An op that resolves a NAME (LOOKUP, CREATE,
 * REMOVE, RENAME, LINK, a named OPEN) takes its directory from the current
 * object instead: there the handle is where the name is looked up rather than
 * the thing being acted on, and a caller that wants a different directory says
 * so by making it current.
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
    CHIMERA_VFS_COMPOUND_OP_ALLOCATE,
    CHIMERA_VFS_COMPOUND_OP_SEEK,
    CHIMERA_VFS_COMPOUND_OP_COPY_RANGE,
    CHIMERA_VFS_COMPOUND_OP_CLONE_RANGE,
    CHIMERA_VFS_COMPOUND_OP_MOVE_RANGE,
    CHIMERA_VFS_COMPOUND_OP_WRITE_SAME,
    CHIMERA_VFS_COMPOUND_OP_READ_PLUS,
    /* Path-addressed.  See the note on ->path. */
    CHIMERA_VFS_COMPOUND_OP_LOOKUP_PATH,
    CHIMERA_VFS_COMPOUND_OP_OPEN_PATH,
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
    uint8_t                               type;
    /* CHIMERA_VFS_UNSET until the op has run. */
    enum chimera_vfs_error                status;

    /* ---- arguments ---- */
    uint8_t                               arg_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                              arg_fh_len;
    char                                  name[CHIMERA_VFS_COMPOUND_NAME_MAX + 1];
    uint32_t                              name_len;
    uint64_t                              attr_mask;
    /* Attributes to sample BEFORE the op runs, landing in `pre_attr`.  The
     * pair (pre_attr_mask, attr_mask) is what a protocol needs to report a
     * change atomically with the change itself: NFSv3's wcc_data, and SMB2's
     * write-time-sticky handle, which restores the mtime a write advanced. */
    uint64_t                              pre_attr_mask;
    /* Attributes of the DIRECTORY an op names a child in, landing in
     * `dir_post_attr`.  Separate from pre_attr_mask because these are a second
     * object's attributes, not this one's at an earlier moment: NFSv3's
     * LOOKUP3resok returns obj_attributes and dir_attributes side by side. */
    uint64_t                              dir_attr_mask;
    uint32_t                              requested;
    uint64_t                              offset; /* COMMIT                             */
    uint64_t                              count; /* COMMIT                             */
    uint64_t                              cookie; /* READDIR, LISTXATTRS                */
    uint64_t                              verifier; /* READDIR                            */
    uint32_t                              dircount; /* READDIR (advisory; see the adder)  */
    uint32_t                              maxcount; /* READDIR (advisory; see the adder)  */
    uint32_t                              max_entries; /* READDIR                            */
    /* READDIR, streaming variant: with these set the sequence stages nothing
     * and the caller marshals each entry itself.  See the typedefs above. */
    chimera_vfs_compound_readdir_reset_t  readdir_reset;
    chimera_vfs_compound_readdir_append_t readdir_append;
    void                                 *readdir_private;
    /* READDIR: CHIMERA_VFS_READDIR_* and the caller's search pattern, which the
     * VFS core matches on the caller's behalf.  The pattern is BORROWED and is
     * re-matched on every execution, so it has to outlive a retry. */
    uint32_t                              readdir_flags;
    const char                           *readdir_pattern;
    uint32_t                              readdir_pattern_len;
    /* Address this handle instead of the current object.  BORROWED from the
     * caller -- see ADDRESSING SOMETHING OTHER THAN CURRENT above.  NULL for
     * every op that addresses the current object, which is most of them. */
    struct chimera_vfs_open_handle       *in_handle;
    uint8_t                               create_type; /* CREATE                             */
    /* CREATE of a symlink: its target.  Copied by the adder and owned by the
     * compound, so the caller need not keep it alive. */
    char                                 *link_target;
    uint32_t                              link_target_len;
    /* RENAME: the name in the CURRENT object to rename to.  `name` is the one
     * in the saved object to rename from. */
    char                                  new_name[CHIMERA_VFS_COMPOUND_NAME_MAX + 1];
    uint32_t                              new_name_len;
    unsigned int                          open_flags; /* OPEN: CHIMERA_VFS_OPEN_*           */
    /* REMOVE, RENAME: CHIMERA_VFS_REMOVE_* -- the type assertion and the
     * lease-recall request, which are the caller's to make. */
    unsigned int                          remove_flags;
    uint32_t                              open_opts; /* OPEN: CHIMERA_VFS_COMPOUND_OPEN_*  */
    /* OPEN and CREATE: attributes to apply to a created object.  Read by the
     * executor at execution time, so ATTRS_ON_CREATE_ONLY can clear it once the
     * name has been resolved. */
    struct chimera_vfs_attrs              set_attr;
    /* A PATH-ADDRESSED op resolves this, relative to the sequence's current
     * file handle, instead of addressing the current object.  Owned by the
     * compound and copied by the adder.
     *
     * This is the only way to reach an object on a path-only mount, where a
     * child's handle is an opaque per-open token that open_fh cannot reopen --
     * so the current object there can only ever be the mount root, and every
     * operation has to name its target by path from it.  It is also one call
     * where chaining would be several, on every backend. */
    char                                 *path;
    uint32_t                              path_len;
    /* RENAME and LINK name two paths; this is the destination. */
    char                                 *new_path;
    uint32_t                              new_path_len;
    /* Address the handle that op `handle_from` produced, rather than the
     * current object -- for the op after a path OPEN, whose result is the only
     * usable reference to an object a path-only mount will not reopen.  -1
     * when unused, which the adders leave it as. */
    int                                   handle_from;
    /* The range ops (COPY_RANGE, CLONE_RANGE, MOVE_RANGE) address TWO objects,
     * and both are caller-supplied: `in_handle` is the destination, the object
     * being written, and this is the source.  Neither ever addresses the
     * current object -- a sequence cannot hold two of those, and every caller
     * of these already holds both handles (two open files for FUSE, two
     * stateids for NFSv4).  Both are BORROWED on the usual terms. */
    struct chimera_vfs_open_handle       *src_handle;
    uint64_t                              src_offset;
    uint32_t                              copy_flags; /* COPY_RANGE                        */
    /* WRITE_SAME: the pattern is BORROWED, like a WRITE's payload. */
    uint32_t                              block_size;
    uint64_t                              block_count;
    const void                           *pattern;
    uint32_t                              pattern_len;
    uint32_t                              reloff_pattern;
    uint32_t                              allocate_flags; /* ALLOCATE: CHIMERA_VFS_ALLOCATE_* */
    uint64_t                              length; /* ALLOCATE                           */
    /* ALLOCATE: the attributes to fetch after the change.  `attr_mask` is the
     * pre-change one, as it is for COMMIT. */
    uint64_t                              post_attr_mask;
    uint32_t                              seek_what; /* SEEK: data (0) or hole (1)         */
    uint32_t                              xattr_option; /* SETXATTR                          */
    const void                           *xattr_value; /* SETXATTR (borrowed from caller)    */
    uint32_t                              xattr_value_len;
    uint32_t                              buffer_max; /* GETXATTR, LISTXATTRS               */
    int                                   max_iov; /* READ                               */
    /* WRITE: the data, BORROWED from the caller -- see ADDRESSING SOMETHING
     * OTHER THAN CURRENT, which these are owned on the same terms as. */
    struct evpl_iovec                    *w_iov;
    int                                   w_niov;
    uint32_t                              sync; /* WRITE: requested stability         */
    /* READ, WRITE: whose I/O this is.  A caller holding a lease on the object
     * has to say so, or the claim layer arbitrates its own I/O against its own
     * reservation -- denying the write, and recalling the delegation the write
     * is being done under. */
    struct chimera_claim_actor            io_owner;
    uint8_t                               have_io_owner;
    /* Executor scratch: whether the two-step I/O type check has run.  Lives on
     * the op only so the open-flags decision, which sees an op and not the
     * sequence, can tell the two steps apart. */
    uint8_t                               io_typechecked_flag;

    /* ---- results ---- */
    /* LOOKUP, GETATTR, ACCESS.  va_acl is always NULL here and the ACL bit is
     * always clear in the masks: a backend reports an ACL by pointing at its
     * own live inode state, which does not outlive its completion, so a copy
     * that survives the callback cannot carry one.  Rather than leave a
     * pointer that looks valid and is not, the sequence drops it -- a caller
     * that needs an ACL issues that getattr itself.  ACCESS's `granted` is
     * computed while the ACL is still live, so it is unaffected. */
    struct chimera_vfs_attrs              attr;
    /* The same object BEFORE this op ran, sampled under whatever lock makes
     * it atomic with the change -- which is the whole reason it comes back
     * from the op rather than from a GETATTR the caller issues first.  Only
     * the attributes named in `pre_attr_mask` are filled; the ACL is dropped
     * for the reason given above. */
    struct chimera_vfs_attrs              pre_attr;
    /* The current object AFTER this op ran: what a LOOKUP resolved, what a
     * PUTFH selected, and for everything else the object the op addressed.
     * A streaming READDIR is the one op that fills this BEFORE it runs rather
     * than after, because its append callback needs to know which directory it
     * is listing and a READDIR cannot move the current object anyway.
     * A caller that must describe the object an op acted on -- which is most
     * of what a protocol reply is -- would otherwise have to re-derive it. */
    uint8_t                               fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                              fh_len;
    uint32_t                              granted; /* ACCESS                            */
    /* READ_PLUS: whether the range it reported is data rather than a hole.
     * Its length and eof land in read_len and eof_read, as a READ's do. */
    uint32_t                              is_data;
    /* SEEK: where the next data or hole begins, and whether the search ran off
     * the end of the file without finding one. */
    uint64_t                              seek_offset;
    uint32_t                              seek_eof;
    char                                 *target; /* READLINK (owned by the compound)  */
    uint32_t                              target_len;

    /* SETXATTR, REMOVEXATTR.  Only the ctime is kept: it is the whole of what
     * a change_info reply needs, and keeping two more attribute sets per op
     * would double the size of a sequence for one field. */
    struct timespec                       pre_ctime;
    struct timespec                       post_ctime;

    /* ---- READ results ---- */
    /* The data, as references to the backend's buffers, written into the array
     * the caller supplied.  The references are owned by the compound until
     * chimera_vfs_compound_take_iov(); the array never is. */
    struct evpl_iovec                    *iov;
    int                                   niov;
    uint32_t                              read_len;
    uint32_t                              eof_read;

    /* ---- WRITE results ---- */
    uint32_t                              written;
    /* Durability actually achieved, which may exceed what was asked for and
     * may fall short of it only by the backend's own report. */
    uint32_t                              committed;

    /* ---- OPEN results ---- */
    /* The open handle, owned by the CALLER once the sequence has finished --
     * see OPEN HANDLE OWNERSHIP below.  NULL if the op did not run or failed. */
    struct chimera_vfs_open_handle       *out_handle;
    /* Whether the open created the object. */
    uint8_t                               created;
    /* Set when the executor resolved the name before opening (which it does
     * for REGULAR_ONLY or ATTRS_ON_CREATE_ONLY) and found an existing object.
     * `existing_mode` is that object's mode -- the whole point of the
     * REGULAR_ONLY failure, whose status says only that the open was refused
     * and not what was in the way. */
    uint8_t                               existed;
    uint32_t                              existing_mode;
    /* The parent directory before and after, for a change_info reply.  Set by
     * CREATE and REMOVE, and by an OPEN that named a child. */
    struct chimera_vfs_attrs              dir_pre_attr;
    struct chimera_vfs_attrs              dir_post_attr;
    /* RENAME only: the SOURCE directory's change_info.  The pair above is the
     * target's, which is what every other name-changing op reports.  Both are
     * filled because rename_at hands back both and NFSv4's RENAME reply has a
     * slot for each -- source_cinfo and target_cinfo. */
    struct chimera_vfs_attrs              from_dir_pre_attr;
    struct chimera_vfs_attrs              from_dir_post_attr;

    /* READDIR.  `entries` is allocated on demand and owned by the compound. */
    struct chimera_vfs_compound_dirent   *entries;
    uint32_t                              num_entries;
    /* READDIR and LISTXATTRS: whether the enumeration reached the end, and the
     * cookie to resume it from, as the backend reported them when it stopped.
     * r_verifier is the directory's verifier (READDIR only). */
    uint32_t                              eof;
    uint64_t                              r_cookie;
    uint64_t                              r_verifier;

    /* GETXATTR (value), LISTXATTRS (back-to-back NUL-terminated names).  Owned
     * by the compound, buffer_max bytes, valid until it is freed. */
    void                                 *buffer;
    uint32_t                              buffer_len;
    uint32_t                              buffer_count; /* LISTXATTRS: names   */
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
 * never addresses the current one -- see the note on src_handle.
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

/* End the current OPEN HANDLE and empty the slot.
 *
 * Provenance does not matter: a handle lent with PUTHANDLE is closed too, which
 * is exactly what an SMB2 or NFSv4 CLOSE of a client's open means.  The caller
 * must not release that handle afterwards. */
int
chimera_vfs_compound_add_close(
    struct chimera_vfs_compound *compound);

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
 * current object moves.  `open_flags` is what the caller opened it with, so
 * the sequence can tell whether it serves an op that needs more; when it does
 * not, the sequence opens its own and leaves this one alone.
 */
int
chimera_vfs_compound_add_puthandle(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *handle,
    unsigned int                    open_flags);

int
chimera_vfs_compound_add_lookup_path(
    struct chimera_vfs_compound *compound,
    const char                  *path,
    int                          pathlen,
    uint64_t                     attr_mask,
    uint32_t                     flags);

int
chimera_vfs_compound_add_open_path(
    struct chimera_vfs_compound    *compound,
    const char                     *path,
    int                             pathlen,
    unsigned int                    flags,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask);

/* `create_type` is a CHIMERA_VFS_COMPOUND_CREATE_*, as for the name-based
 * CREATE; `target` is the symlink target and read only for a symlink. */
int
chimera_vfs_compound_add_create_path(
    struct chimera_vfs_compound    *compound,
    uint8_t                         create_type,
    const char                     *path,
    int                             pathlen,
    const char                     *target,
    int                             targetlen,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask);

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
* that handle is the only usable reference to what the open resolved. */
void
chimera_vfs_compound_op_use_handle(
    struct chimera_vfs_compound *compound,
    uint32_t                     index,
    uint32_t                     from);

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

/*
 * Return a finished compound.  This releases anything the sequence still
 * holds -- see OPEN HANDLE OWNERSHIP -- and recycles the compound onto the
 * thread's free list rather than returning it to the allocator.
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
    int                          new_namelen,
    unsigned int                 flags);

/* Link the saved object into the current object as `name`.  `attr_mask` is
 * fetched against the newly linked object, for a caller whose reply describes
 * it; a protocol that reports only the directory change passes 0. */
int
chimera_vfs_compound_add_link(
    struct chimera_vfs_compound *compound,
    const char                  *name,
    int                          namelen,
    uint64_t                     attr_mask);

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
    unsigned int                 flags);

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
    uint64_t                          attr_mask,
    const struct chimera_claim_actor *io_owner);

/* Write `count` bytes of `iov` at `offset` to the current object, or -- when
 * `handle` is non-NULL -- to that handle.  Both `handle` and `iov` are
 * BORROWED: the caller holds them for as long as the sequence runs and releases
 * them afterwards.
 *
 * `pre_attr_mask` and `post_attr_mask` sample the file either side of the
 * write, into the op's `pre_attr` and `attr`.  A protocol that reports the
 * change -- NFSv3's wcc_data, SMB2 restoring the mtime a write advanced on a
 * write-time-sticky handle -- needs those two readings to be atomic with the
 * write, which a separate GETATTR in the same sequence would not be. */
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
 * SOMETHING OTHER THAN CURRENT.  On return the op's `set_attr` reports which
 * attributes were actually applied.
 *
 * `pre_attr_mask` and `attr_mask` sample the object either side of the change,
 * into the op's pre_attr and attr.  NFSv3's SETATTR3resok.obj_wcc needs that
 * pair to be atomic with the change, which a getattr in front of the op is
 * not. */
int
chimera_vfs_compound_add_setattr(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *handle,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        pre_attr_mask,
    uint64_t                        attr_mask);

/* Give an op the handle it should act on, after appending it -- see ADDRESSING
 * SOMETHING OTHER THAN CURRENT.  Separate from the adders because most callers
 * address the current object and would carry an argument they never use. */
void
chimera_vfs_compound_op_set_handle(
    struct chimera_vfs_compound    *compound,
    uint32_t                        index,
    struct chimera_vfs_open_handle *handle);

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
