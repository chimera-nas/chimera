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
};

#define CHIMERA_VFS_COMPOUND_MAX_OPS  32
#define CHIMERA_VFS_COMPOUND_NAME_MAX 255

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
