// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include "vfs.h"

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
    /* Extended attributes of the current object.  SETXATTR and REMOVEXATTR
     * MUTATE -- see the MUTATION note above. */
    CHIMERA_VFS_COMPOUND_OP_GETXATTR,
    CHIMERA_VFS_COMPOUND_OP_SETXATTR,
    CHIMERA_VFS_COMPOUND_OP_LISTXATTRS,
    CHIMERA_VFS_COMPOUND_OP_REMOVEXATTR,
};

#define CHIMERA_VFS_COMPOUND_MAX_OPS  32
#define CHIMERA_VFS_COMPOUND_NAME_MAX 255

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
    uint8_t                type;
    /* CHIMERA_VFS_UNSET until the op has run. */
    enum chimera_vfs_error status;

    /* ---- arguments ---- */
    uint8_t                arg_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t               arg_fh_len;
    char                   name[CHIMERA_VFS_COMPOUND_NAME_MAX + 1];
    uint32_t               name_len;
    uint64_t               attr_mask;
    uint32_t               requested;
    uint64_t               offset;      /* COMMIT                             */
    uint64_t               count;       /* COMMIT                             */
    uint64_t               cookie;      /* READDIR, LISTXATTRS                */
    uint64_t               verifier;    /* READDIR                            */
    uint32_t               dircount;    /* READDIR (advisory; see the adder)  */
    uint32_t               maxcount;    /* READDIR (advisory; see the adder)  */
    uint32_t               max_entries; /* READDIR                            */
    uint32_t               xattr_option; /* SETXATTR                          */
    const void            *xattr_value; /* SETXATTR (borrowed from caller)    */
    uint32_t               xattr_value_len;
    uint32_t               buffer_max;  /* GETXATTR, LISTXATTRS               */

    /* ---- results ---- */
    /* LOOKUP, GETATTR, ACCESS.  va_acl is always NULL here and the ACL bit is
     * always clear in the masks: a backend reports an ACL by pointing at its
     * own live inode state, which does not outlive its completion, so a copy
     * that survives the callback cannot carry one.  Rather than leave a
     * pointer that looks valid and is not, the sequence drops it -- a caller
     * that needs an ACL issues that getattr itself.  ACCESS's `granted` is
     * computed while the ACL is still live, so it is unaffected. */
    struct chimera_vfs_attrs attr;
    /* The current object AFTER this op ran: what a LOOKUP resolved, what a
     * PUTFH selected, and for everything else the object the op addressed.
     * A caller that must describe the object an op acted on -- which is most
     * of what a protocol reply is -- would otherwise have to re-derive it. */
    uint8_t                  fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                 fh_len;
    uint32_t                 granted;   /* ACCESS                            */
    char                    *target;    /* READLINK (owned by the compound)  */
    uint32_t                 target_len;

    /* SETXATTR, REMOVEXATTR.  Only the ctime is kept: it is the whole of what
     * a change_info reply needs, and keeping two more attribute sets per op
     * would double the size of a sequence for one field. */
    struct timespec          pre_ctime;
    struct timespec          post_ctime;

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
