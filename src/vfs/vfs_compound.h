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
    /* Path-addressed.  See the note on ->path. */
    CHIMERA_VFS_COMPOUND_OP_LOOKUP_PATH,
    CHIMERA_VFS_COMPOUND_OP_OPEN_PATH,
    CHIMERA_VFS_COMPOUND_OP_CREATE_UNLINKED,
    CHIMERA_VFS_COMPOUND_OP_FIND,
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

#define CHIMERA_VFS_COMPOUND_MAX_OPS             1024
#define CHIMERA_VFS_COMPOUND_NAME_MAX            255

/* Key/value bytes are copied at construction. Both operations preserve the
 * cursor and use its FH to select the same backend as the *_key_at API. */
int chimera_vfs_compound_add_put_key_at(struct chimera_vfs_compound *compound,
    const void *key, uint32_t key_len, const void *value, uint32_t value_len);
int chimera_vfs_compound_add_delete_key_at(struct chimera_vfs_compound *compound,
    const void *key, uint32_t key_len);

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
int chimera_vfs_compound_add_search_keys_at(struct chimera_vfs_compound *compound,
    const void *start_key, uint32_t start_len, const void *end_key, uint32_t end_len,
    uint32_t flags, uint32_t max_entries, uint32_t max_bytes);

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
    uint8_t                                type;
    /* CHIMERA_VFS_UNSET until the op has run. */
    enum chimera_vfs_error                 status;

    chimera_vfs_compound_op_callback_t     prepare;
    chimera_vfs_compound_op_callback_t     complete;
    void                                  *prepare_private;
    void                                  *callback_private;
    chimera_vfs_compound_coordinate_t      coordinate;
    void                                  *coordinate_private;
    uint8_t                                coordinate_each_attempt;
    uint8_t                                prepared;
    uint8_t                                skipped;
    /* Completed execution/callouts, independent of physical operation order.
     * False for group/dependency-skipped operations (whose status is UNSET). */
    uint8_t                                completed;
    /* Executor-owned link: dynamic group suffixes keep stable op indices. */
    int32_t                                group_next;
    uint32_t                               cancel_scope_end; /* construction, end index + 1 */
    uint32_t                               access_ready; /* opt-in private reservation endpoint + 1 */

    chimera_vfs_compound_find_entry_t      find_filter;
    chimera_vfs_compound_find_entry_t      find_append;
    chimera_vfs_compound_readdir_reset_t   find_reset;
    void                                  *find_private;

    /* REMOVE_PATHS borrows a stable list of NUL-terminated paths for this
     * attempt. Each removal remains an ordinary VFS operation. */
    const char *const                     *remove_paths;
    uint32_t                               remove_num_paths;
    uint32_t                               remove_path_index;
    uint8_t                                remove_ignore_errors;

    /* LINK namespace options. With NO_NOTIFY the frontend must emit the
     * accepted notification itself. io_owner.op_handle preserves the source
     * open's lease identity when have_io_owner is set. */
    uint32_t                               namespace_flags;
    /* Borrowed immutable credential, bound by prepare if necessary. Applies
     * only to OPEN_CURRENT/REMOVE and the latter's implicit parent open. */
    const struct chimera_vfs_cred          *namespace_cred;
    uint8_t                                remove_match_child_fh;
    uint8_t                                remove_unmatched;
    uint8_t                                namespace_parent_lease_key[16];
    uint8_t                                namespace_parent_lease_key_valid;

    /* ---- arguments ---- */
    /* PUTFH input; REMOVE may also use these as the resolved child identity
     * for VFS lease recall and silly-rename handling. Zero length means none. */
    uint8_t                                arg_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                               arg_fh_len;
    char                                   name[CHIMERA_VFS_COMPOUND_NAME_MAX + 1];
    uint32_t                               name_len;
    uint64_t                               attr_mask;
    /* Optional protocol result masks; otherwise each builder's defaults apply. */
    uint8_t                                result_masks_set;
    uint64_t                               result_attr_mask;
    uint64_t                               result_pre_attr_mask;
    uint64_t                               result_post_attr_mask;
    uint32_t                               requested;
    uint64_t                               offset; /* COMMIT                             */
    uint64_t                               count; /* COMMIT                             */
    uint64_t                               cookie; /* READDIR, LISTXATTRS                */
    uint32_t                               readdir_flags; /* CHIMERA_VFS_READDIR_* */
    uint64_t                               verifier; /* READDIR                            */
    uint32_t                               dircount; /* READDIR (advisory; see the adder)  */
    uint32_t                               maxcount; /* READDIR (advisory; see the adder)  */
    uint32_t                               max_entries; /* READDIR                            */
    /* READDIR, streaming variant: with these set the sequence stages nothing
     * and the caller marshals each entry itself.  See the typedefs above. */
    chimera_vfs_compound_readdir_reset_t   readdir_reset;
    chimera_vfs_compound_readdir_append_t  readdir_append;
    void                                  *readdir_private;
    /* Address this handle instead of the current object.  BORROWED from the
     * caller -- see ADDRESSING SOMETHING OTHER THAN CURRENT above.  NULL for
     * every op that addresses the current object, which is most of them. */
    struct chimera_vfs_open_handle        *in_handle;
    uint8_t                                create_type; /* CREATE                             */
    /* CREATE of a symlink: its target.  Copied by the adder and owned by the
     * compound, so the caller need not keep it alive. */
    char                                  *link_target;
    uint32_t                               link_target_len;
    /* RENAME: the name in the CURRENT object to rename to.  `name` is the one
     * in the saved object to rename from. */
    char                                   new_name[CHIMERA_VFS_COMPOUND_NAME_MAX + 1];
    uint32_t                               new_name_len;
    unsigned int                           open_flags; /* OPEN: CHIMERA_VFS_OPEN_*           */
    /* REMOVE, RENAME: CHIMERA_VFS_REMOVE_* -- the type assertion and the
     * lease-recall request, which are the caller's to make. */
    unsigned int                           remove_flags;
    uint8_t                                rename_target_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                               rename_target_fh_len;
    enum chimera_vfs_rename_outcome         rename_outcome;
    uint32_t                               open_opts; /* OPEN: CHIMERA_VFS_COMPOUND_OPEN_*  */
    /* Unnamed OPEN may inherit retained permissions from another authorized
     * open of the same FH. Borrowed and pinned through completion; prepare may
     * bind it to an earlier result. Only OPEN-bound grants are inherited;
     * a lazy stateless access-cache entry never becomes permanent this way.
     * The executor combines these rights with those already on the result. */
    const struct chimera_vfs_open_handle  *inherited_grant_handle;
    /* Second authorized grant source, e.g. the newly requested OPEN when the
     * union reopen instead selects the older write-capable cache entry. */
    const struct chimera_vfs_open_handle  *inherited_grant_handle2;
    /* Immutable OPEN/CREATE/SETATTR input. Scalars are copied by the builder;
     * va_acl is borrowed and must remain valid through completion/retries.
     * ATTRS_ON_CREATE_ONLY clears only the execution copy below. */
    struct chimera_vfs_attrs               set_attr;
    /* Execution copy: resolving an existing name must not erase the original
     * create attributes needed by a subsequent attempt. */
    struct chimera_vfs_attrs               applied_attr;
    /* Attempt-local anonymous admission; excluded claims remain pinned by
    * the frontend. Owned actors still use io_owner/src_io_owner below. */
    struct chimera_vfs_io_view             io_view;
    struct chimera_vfs_io_view             src_io_view;
    /* Writable ACL supplied to this attempt. set_attr.va_acl remains the
     * caller's immutable input for the lifetime of the compound. */
    struct chimera_acl                    *applied_acl;
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
    char                                  *path;
    uint32_t                               path_len;
    /* RENAME and LINK name two paths; this is the destination. */
    char                                  *new_path;
    uint32_t                               new_path_len;
    /* Address the handle that op `handle_from` produced, rather than the
     * current object -- for the op after a path OPEN, whose result is the only
     * usable reference to an object a path-only mount will not reopen.  -1
     * when unused, which the adders leave it as. */
    int                                    handle_from;
    /* Range endpoints are borrowed: in_handle is the destination and
    * src_handle the source. Prepare may bind either from prior owned results.
    * If omitted, execution uses the saved/current open cursors; both handles
    * must be available before dispatch. These are distinct from SAVEFH's
    * saved filehandle, which does not itself keep an open handle. */
    struct chimera_vfs_open_handle        *src_handle;
    uint64_t                               src_offset;
    uint32_t                               copy_flags; /* COPY_RANGE                        */
    /* WRITE_SAME: the pattern is BORROWED, like a WRITE's payload. */
    uint32_t                               block_size;
    uint64_t                               block_count;
    const void                            *pattern;
    uint32_t                               pattern_len;
    uint32_t                               reloff_pattern;
    uint32_t                               allocate_flags; /* ALLOCATE: CHIMERA_VFS_ALLOCATE_* */
    uint64_t                               length; /* ALLOCATE                           */
    /* ALLOCATE: the attributes to fetch after the change.  `attr_mask` is the
     * pre-change one, as it is for COMMIT. */
    uint64_t                               post_attr_mask;
    uint32_t                               seek_what; /* SEEK: data (0) or hole (1)         */
    uint32_t                               xattr_option; /* SETXATTR                          */
    const void                            *xattr_value; /* SETXATTR (borrowed from caller)    */
    uint32_t                               xattr_value_len;
    /* Immutable binary inputs copied by the KV builders, retained on retry. */
    uint8_t                               *kv_key;
    uint8_t                               *kv_value;
    uint32_t                               kv_key_len;
    uint32_t                               kv_value_len;
    uint32_t                               kv_flags, kv_max_entries, kv_max_bytes;
    struct chimera_vfs_compound_kv_entry   *kv_entries;
    uint32_t                               kv_num_entries, kv_result_bytes;
    uint8_t                               *kv_next_key;
    uint32_t                               kv_next_key_len;
    bool                                   kv_more;
    enum chimera_vfs_error                 kv_error;
    uint32_t                               buffer_max; /* GETXATTR, LISTXATTRS               */
    uint8_t                                stream_want_fh;
    int                                    max_iov; /* READ                               */
    /* WRITE: the data, BORROWED from the caller -- see ADDRESSING SOMETHING
     * OTHER THAN CURRENT, which these are owned on the same terms as. */
    struct evpl_iovec                     *w_iov;
    int                                    w_niov;
    uint32_t                               sync; /* WRITE: requested stability         */
    /* READ, WRITE: whose I/O this is.  A caller holding a lease on the object
     * has to say so, or the claim layer arbitrates its own I/O against its own
     * reservation -- denying the write, and recalling the delegation the write
     * is being done under. */
    struct chimera_claim_actor             io_owner;
    uint8_t                                have_io_owner;
    struct chimera_claim_actor             src_io_owner;
    uint8_t                                have_src_io_owner;
    /* Executor scratch: whether the two-step I/O type check has run.  Lives on
     * the op only so the open-flags decision, which sees an op and not the
     * sequence, can tell the two steps apart. */
    uint8_t                                io_typechecked_flag;

    /* RESERVE: caller-owned attempt-private claim storage. The VFS owns its
     * acquisition and release until accepted completion takes the reservation. */
    struct chimera_vfs_claim              *claim;
    struct chimera_vfs_claim_access_owner *access_owner; /* owned RESERVE_ACCESS result */
    struct chimera_vfs_claim_access_owner *access_retire_owner; /* borrowed input */
    uint32_t                               access_narrow_from;
    uint8_t                                access_narrow_used, access_narrow_denied;
    struct chimera_vfs_claim_access_owner *base_access_retire_owner;
    struct chimera_vfs_claim_owner        *range_retire_owner;
    /* Same-file private range overlay, borrowed through this operation. */
    const struct chimera_vfs_claim *const *claim_ranges;
    uint32_t                               num_claim_ranges;

    /* Exact local claim journal inputs and per-batch result. */
    struct chimera_vfs_claim_owner        *range_owner;
    struct chimera_vfs_claim_owner        *out_range_owner; /* owned typed allocation */
    bool                                   range_zero_point;
    const struct chimera_vfs_claim_exact_range *exact_ranges;
    uint32_t                               num_exact_ranges;
    uint8_t                                range_unlock;
    uint8_t                                range_wait;
    uint32_t                               range_timeout_ms;
    struct chimera_vfs_claim_range_attempt *range_attempt;
    void (*range_on_wait)(struct chimera_vfs_compound *, uint32_t, void *);
    bool (*range_is_canceled)(struct chimera_vfs_compound *, uint32_t, void *);
    void                                  *range_wait_private;
    struct chimera_vfs_claim_batch_result  range_result;
    const struct chimera_vfs_claim       **journal_excluded;
    const struct chimera_vfs_claim       **journal_src_excluded;

    struct chimera_vfs_lock_request        lock_request;
    struct chimera_vfs_lock_attempt       *lock_attempt;
    uint8_t                                nonretryable;
    uint32_t                               lock_pid;

    /* ---- results ---- */
    /* CLOSE of an external borrowed reference is deferred until acceptance. */
    struct chimera_vfs_open_handle        *close_handle;
    /* Attempt-owned CLOSE output, unavailable to consumers immediately but
     * retained through journal/claim drain on accepted or rejected cleanup. */
    struct chimera_vfs_open_handle        *closed_output_handle;
    enum chimera_vfs_error                 result_error;
    struct chimera_vfs_file_state         *claim_file;
    uint8_t                                claim_held;
    uint8_t                                claim_result;
    struct chimera_vfs_claim_conflict      claim_conflict;
    /* LOOKUP, GETATTR, ACCESS and OPEN. ACL storage is copied and owned by
     * the compound, valid through accepted completion until free/retry. */
    struct chimera_vfs_attrs               attr;
    /* The current object AFTER this op ran: what a LOOKUP resolved, what a
     * PUTFH selected, and for everything else the object the op addressed.
     * A streaming READDIR is the one op that fills this BEFORE it runs rather
     * than after, because its append callback needs to know which directory it
     * is listing and a READDIR cannot move the current object anyway.
     * A caller that must describe the object an op acted on -- which is most
     * of what a protocol reply is -- would otherwise have to re-derive it. */
    uint8_t                                fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                               fh_len;
    uint32_t                               granted; /* ACCESS                            */
    /* READ_PLUS: whether the range it reported is data rather than a hole.
     * Its length and eof land in read_len and eof_read, as a READ's do. */
    uint32_t                               is_data;
    /* SEEK: where the next data or hole begins, and whether the search ran off
     * the end of the file without finding one. */
    uint64_t                               seek_offset;
    uint32_t                               seek_eof;
    char                                  *target; /* READLINK (owned by the compound)  */
    uint32_t                               target_len;

    /* SETXATTR, REMOVEXATTR.  Only the ctime is kept: it is the whole of what
     * a change_info reply needs, and keeping two more attribute sets per op
     * would double the size of a sequence for one field. */
    struct timespec                        pre_ctime;
    struct timespec                        post_ctime;

    /* ---- READ results ---- */
    /* The data, as references to the backend's buffers, written into the array
     * the caller supplied.  The references are owned by the compound until
     * chimera_vfs_compound_take_iov(); the array never is. */
    struct evpl_iovec                     *iov;
    int                                    niov;
    uint32_t                               read_len;
    uint32_t                               eof_read;

    /* ---- WRITE results ---- */
    uint64_t                               written;
    /* Durability actually achieved, which may exceed what was asked for and
     * may fall short of it only by the backend's own report. */
    uint32_t                               committed;

    /* ---- OPEN results ---- */
    /* The open handle, owned by the CALLER once the sequence has finished --
     * see OPEN HANDLE OWNERSHIP below.  NULL if the op did not run or failed. */
    struct chimera_vfs_open_handle        *out_handle;
    /* Whether the open created the object. */
    uint8_t                                created;
    /* Set when the executor resolved the name before opening (which it does
     * for REGULAR_ONLY or ATTRS_ON_CREATE_ONLY) and found an existing object.
     * `existing_mode` is that object's mode -- the whole point of the
     * REGULAR_ONLY failure, whose status says only that the open was refused
     * and not what was in the way. */
    uint8_t                                existed;
    uint32_t                               existing_mode;
    /* The parent directory before and after, for a change_info reply.  Set by
     * CREATE and REMOVE, and by an OPEN that named a child. */
    struct chimera_vfs_attrs               dir_pre_attr;
    struct chimera_vfs_attrs               dir_post_attr;
    /* RENAME only: the SOURCE directory's change_info.  The pair above is the
     * target's, which is what every other name-changing op reports.  Both are
     * filled because rename_at hands back both and NFSv4's RENAME reply has a
     * slot for each -- source_cinfo and target_cinfo. */
    struct chimera_vfs_attrs               from_dir_pre_attr;
    struct chimera_vfs_attrs               from_dir_post_attr;

    /* READDIR.  `entries` is allocated on demand and owned by the compound. */
    struct chimera_vfs_compound_dirent    *entries;
    uint32_t                               num_entries;
    /* READDIR and LISTXATTRS: whether the enumeration reached the end, and the
     * cookie to resume it from, as the backend reported them when it stopped.
     * r_verifier is the directory's verifier (READDIR only). */
    uint32_t                               eof;
    uint64_t                               r_cookie;
    uint64_t                               r_verifier;

    /* GETXATTR (value), LISTXATTRS (back-to-back NUL-terminated names).  Owned
     * by the compound, buffer_max bytes, valid until it is freed. */
    void                                  *buffer;
    uint32_t                               buffer_len;
    uint32_t                               buffer_count; /* LISTXATTRS: names   */
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
    enum chimera_vfs_error         dependency_error;
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
bool
chimera_vfs_compound_cancel(struct chimera_vfs_compound *compound);

/* Read-only cancellation request state, including during an active cleanup
 * suffix and after acceptance. Valid until compound_free. */
bool
chimera_vfs_compound_is_canceled(const struct chimera_vfs_compound *compound);

/* Declare a static cancellation-deferral suffix within one command group.
 * Once start completes successfully (not skipped), cancellation is recorded
 * but stops execution only after end completes. Activation precedes start's
 * complete callback. Errors still stop the group; callbacks may explicitly
 * normalize optional cleanup errors while preserving them in private results.
 * Scopes cannot overlap, cross groups, or include dynamically appended ops.
 * Does not cancel/settle underlying I/O or COORDINATE, promise cleanup after an
 * unhandled error, or certify rollback. Cancellation before start stays prompt.
 * Construction only; group registration may follow this declaration. */
bool chimera_vfs_compound_set_cancel_scope(struct chimera_vfs_compound *compound,
    uint32_t start, uint32_t end);
/* Borrowed immutable identity for typed ACCESS admissions (default: compound).
 * Frontend fences that outlive compound_free must use a separate stable cookie
 * until every fence is released; a pooled compound address can be reused.
 * Construction only, non-NULL; pure callbacks cannot change the identity. */
bool chimera_vfs_compound_set_admission_cookie(struct chimera_vfs_compound *compound,
    const void *cookie);

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
    struct chimera_vfs_compound *compound,
    struct chimera_vfs_claim_owner *owner,
    const struct chimera_vfs_claim_exact_range *ranges,
    uint32_t count, bool unlock);

/* Cancel only this pending range batch on its owning thread. This reports
 * ordinary EINTR to its group; successful prefixes and later independent
 * groups retain normal semantics. Completion may synchronously free compound. */
bool
chimera_vfs_compound_range_cancel(struct chimera_vfs_compound *compound, uint32_t index);

/* Pure mandatory-I/O observation including this attempt's range overlay.
 * Without an exact journal this is the ordinary public claim predicate. */
bool
chimera_vfs_compound_io_denied(
    struct chimera_vfs_compound *compound,
    const struct chimera_vfs_open_handle *handle,
    uint64_t offset, uint64_t length, bool write,
    const struct chimera_claim_actor *actor);

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

/* `pre_attr_mask` is fetched against the object before the flush, so a caller
 * that must classify what it just committed does not need a separate getattr. */
int
chimera_vfs_compound_add_commit(
    struct chimera_vfs_compound *compound,
    uint64_t                     offset,
    uint64_t                     count,
    uint64_t                     pre_attr_mask);

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
    uint64_t                     attr_mask);

/* READDIR that streams: no entry is staged, `append` is called with each one as
 * the backend produces it, and `reset` is called before the op runs (and so
 * again before any retry of it).  `max_entries` is not taken because the
 * caller's own bound is what stops the enumeration -- which is the point.
 *
 * The contract on the two callbacks is on their typedefs; the short version is
 * that reset must undo everything append did, and append must not emit. */
int
chimera_vfs_compound_add_readdir_stream(
    struct chimera_vfs_compound          *compound,
    uint64_t                              cookie,
    uint64_t                              verifier,
    uint64_t                              attr_mask,
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
int
chimera_vfs_compound_add_create_unlinked(
    struct chimera_vfs_compound    *compound,
    const struct chimera_vfs_attrs *set_attr,
    uint64_t                        attr_mask);

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
    struct chimera_vfs_compound *compound, uint32_t handle_from,
    struct chimera_vfs_claim *template_claim);
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
    struct chimera_vfs_compound *compound, uint32_t reserve_index, uint32_t ready_index);

/* Subset-only private rights view for this compound's RESERVE_ACCESS result.
 * Use after a successful typed mutation and before producer readiness. Other
 * clients retain the original conservative claim until accepted publish; later
 * reservations in this attempt see the narrowed row. No public token input,
 * widening, reinsertion, or allocation/admission during accepted publication.
 * Supports ordinary grouped completion suffix append and replay. */
int chimera_vfs_compound_add_narrow_access(struct chimera_vfs_compound *compound,
    uint32_t reserve_index, uint8_t used, uint8_t denied);

/* Transfer token plus the reservation's file-state reference together. file
 * must be non-NULL. The owner has its own separate file-state reference. */
struct chimera_vfs_claim_access_owner *chimera_vfs_compound_take_access_owner(
    struct chimera_vfs_compound *compound, uint32_t index,
    struct chimera_vfs_file_state **file);
/* Construction only. A deferred NULL owner may be bound through prepare's
 * access_retire_owner. Exclusions affect later RESERVE in the same attempt;
 * public unlink waits accepted publication. Teardown must use owner_retire. */
int chimera_vfs_compound_add_retire_access(struct chimera_vfs_compound *compound,
    struct chimera_vfs_claim_access_owner *owner);
/* Atomically reserve retirement of canonical range, ordinary ACCESS, and base
 * ACCESS owners before backend CLOSE. Inputs are borrowed/pinned by frontend;
 * NULL means absent, and prepare may bind them from an attempt-private open.
 * Failure withdraws only this op's reservations; earlier command edits survive.
 * No frontend or filesystem effects occur here. Successful retirement remains
 * staged even if a later independent operation fails: the CLOSE mapper must
 * preserve its accepted-prefix semantics. Local canonical owners only. */
int chimera_vfs_compound_add_retire_open_claims(struct chimera_vfs_compound *compound,
    struct chimera_vfs_claim_owner *range_owner,
    struct chimera_vfs_claim_access_owner *access_owner,
    struct chimera_vfs_claim_access_owner *base_access_owner);
int chimera_vfs_compound_add_retire_range_owner(struct chimera_vfs_compound *compound,
    struct chimera_vfs_claim_owner *range_owner);
/* Allocate canonical local SMB RANGE ownership during typed execution using
 * the current handle and actor (copied input, or prepare-bound io_owner).
 * Results stay compound-owned until accepted transfer. Reset/free retires
 * untransferred tokens before releasing handle anchors. */
int chimera_vfs_compound_add_range_owner(struct chimera_vfs_compound *compound,
    const struct chimera_claim_actor *actor, bool zero_point);
struct chimera_vfs_claim_owner *chimera_vfs_compound_take_range_owner(
    struct chimera_vfs_compound *compound, uint32_t index);



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
chimera_vfs_compound_num_groups(const struct chimera_vfs_compound *compound);

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
    uint64_t                        attr_mask);

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
 * SOMETHING OTHER THAN CURRENT.  On return the op's `applied_attr` reports which
 * attributes were actually applied. */
int
chimera_vfs_compound_add_setattr(
    struct chimera_vfs_compound    *compound,
    struct chimera_vfs_open_handle *handle,
    const struct chimera_vfs_attrs *set_attr,
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

int
chimera_vfs_compound_add_find(
    struct chimera_vfs_compound         *compound,
    uint64_t                             attr_mask,
    chimera_vfs_compound_readdir_reset_t reset,
    chimera_vfs_compound_find_entry_t    filter,
    chimera_vfs_compound_find_entry_t    append,
    void                                *private_data);

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
    struct chimera_vfs_compound *compound, const char *name, int namelen,
    unsigned int flags, const struct chimera_vfs_attrs *set_attr, uint64_t attr_mask);
int chimera_vfs_compound_add_list_streams(
    struct chimera_vfs_compound *compound, uint64_t cookie, uint32_t max_bytes, bool want_fh);
int chimera_vfs_compound_add_remove_stream(
    struct chimera_vfs_compound *compound, const char *name, int namelen);

/* Owns a copy of the expected stream FH. Strict atomic identity mismatch is
 * ESTALE, unsupported backend is ENOTSUP before mutation. */
int chimera_vfs_compound_add_remove_stream_checked(
    struct chimera_vfs_compound *compound, const char *name, int namelen,
    const uint8_t *expected_fh, uint32_t expected_fh_len);
