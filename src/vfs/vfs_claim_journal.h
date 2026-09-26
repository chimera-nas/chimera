// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once

#include "vfs_claim.h"

struct chimera_vfs_claim_owner;
struct chimera_vfs_claim_journal;

/* Exact SMB acquisitions, not POSIX interval replacement. length is the raw
 * finite length, including zero and UINT64_MAX; offset+length may equal 2^64.
 * A zero-byte record is unlockable but owns no bytes. This intentionally
 * differs from the legacy claim core's interior-point zero-range test. SMB
 * adapters can opt into explicit owner zero_point compatibility; they must
 * never split an owner between legacy and journal lists. Default exact owners
 * retain the no-byte policy until a protocol oracle settles the discrepancy. */
struct chimera_vfs_claim_exact_range {
    uint64_t offset;
    uint64_t length;
    bool exclusive;
};

struct chimera_vfs_claim_batch_result {
    enum chimera_vfs_error status;
    enum chimera_vfs_claim_result admission;
    uint32_t applied;
    uint32_t failed; /* count on success; zero-based element otherwise */
    bool self_conflict; /* existing same-open exclusive conflict: never wait */
    struct chimera_vfs_claim_conflict conflict;
};

/* One canonical token per SMB open, including an unpublished provisional open.
 * Caller reserves its protocol identity and supplies the resolved full file
 * identity; this layer never inserts into SMB FileId/durable registries. The
 * token owns a file-state reference and exact accepted acquisitions. All other
 * lock changes/close for this owner must use this token (no legacy list edits).
 * Tokens are refcounted; final put requires completed retirement. */
struct chimera_vfs_claim_owner *chimera_vfs_claim_owner_create(
    struct chimera_vfs_file_state *file,
    const struct chimera_claim_owner *identity);
/* SMB migration compatibility: zero_point retains legacy interior-point
 * zero-range conflicts, including same-owner exclusive acquisition checks.
 * actor.op_handle is an identity-only borrowed anchor valid for owner lifetime;
 * actor.owner carries the stable open identity and grant key. This does not
 * change generic claim/POSIX/NFS geometry or the default exact API policy. */
struct chimera_vfs_claim_owner *chimera_vfs_claim_owner_create_compat(
    struct chimera_vfs_file_state *file,
    const struct chimera_claim_actor *actor, bool zero_point);
void chimera_vfs_claim_owner_ref(struct chimera_vfs_claim_owner *owner);
void chimera_vfs_claim_owner_put(struct chimera_vfs_claim_owner *owner);

/* Accepted-record observation for durable-open preservation policy. */
bool chimera_vfs_claim_owner_has_locks(struct chimera_vfs_claim_owner *owner);
/* Retirement has drained accepted claims and all active journal pins. */
bool chimera_vfs_claim_owner_is_retired(struct chimera_vfs_claim_owner *owner);

/* Thread-safe cutoff. Stops new journal bindings immediately. Active journals
 * pin owner lifetime through complete/reset; retirement waits without holding
 * locks and drains before callback. Callback may be inline, or on the journal
 * completion thread; caller marshals to its worker and retains its context.
 * Callbacks may drop owner references, but must not reenter/free the invoking
 * journal: its caller retains it until complete/reset returns.
 * Returns false for a duplicate retirement request (no callback registered).
 * CLOSE must not reply before the accepted retirement callback. */
bool chimera_vfs_claim_owner_retire(
    struct chimera_vfs_claim_owner *owner,
    void (*complete)(void *), void *private_data);

/* Bounds cover owner bindings and journal deltas, not existing held records.
 * An acquisition or a removal consumes one change slot. All journal methods
 * except owner lifecycle run on one execution thread; they are typed-operation
 * work, NOT pure prepare callbacks. Owner lifetime pins are concurrent; removal
 * reservations protect individual records, never an entire owner edit lane.
 * No mutex is held across I/O/backend finish.
 * Only immediately grantable LOCAL SMB claims are supported. Cache recall
 * requirements return admission BREAKING, without starting a break; caller
 * coordinates outside tentative publication. No backend projection/rollback. */
struct chimera_vfs_claim_journal *chimera_vfs_claim_journal_alloc(
    uint32_t max_owners, uint32_t max_changes);
enum chimera_vfs_error chimera_vfs_claim_journal_bind(
    struct chimera_vfs_claim_journal *journal,
    struct chimera_vfs_claim_owner *owner);
/* Executor wait support: release a binding only if this owner has no journal
 * deltas. A failed attempt need not delay idle-owner retirement.
 * Retirement callback may run here; caller retains journal and owner. */
bool chimera_vfs_claim_journal_unbind_idle(
    struct chimera_vfs_claim_journal *journal,
    struct chimera_vfs_claim_owner *owner);
void chimera_vfs_claim_journal_acquire(
    struct chimera_vfs_claim_journal *journal,
    struct chimera_vfs_claim_owner *owner,
    const struct chimera_vfs_claim_exact_range *ranges, uint32_t count,
    struct chimera_vfs_claim_batch_result *result);
/* Whole-owner CLOSE admission. Snapshot at most 65536 additional records per
 * attempt (about 2 MiB of delta/exclusion storage), allocating before setting
 * any fence. Removes own earlier acquisitions too. Other active journal pins
 * cause ordinary EBUSY, never a wait or a whole-compound retry. The fence makes
 * later operations on this owner fail (peer EBUSY, same attempt EINTR), while
 * other owners see the private exclusions for admission and mandatory I/O.
 * Reset restores the owner unless independently cut off; accepted publication
 * makes cutoff permanent. Complete releases the frontend lifetime barrier.
 * A failed later CLOSE sub-operation does not undo successful retirement;
 * callers must admit multi-owner/ACCESS retirement atomically before CLOSE. */
#define CHIMERA_VFS_CLAIM_RETIRE_MAX_RECORDS 65536U
enum chimera_vfs_error chimera_vfs_claim_journal_retire_owner(
    struct chimera_vfs_claim_journal *journal,
    struct chimera_vfs_claim_owner *owner);

/* Typed multi-claim admission only: rewind newly staged retirements before any
 * later range mutation or filesystem effect. Earlier command deltas survive.
 * External cutoff is still honored; this never asserts backend rollback. */
uint32_t chimera_vfs_claim_journal_retire_checkpoint(
    const struct chimera_vfs_claim_journal *journal);
void chimera_vfs_claim_journal_retire_rewind(
    struct chimera_vfs_claim_journal *journal, uint32_t checkpoint);

/* Consume the oldest available matching accepted (or own provisional) exact
 * acquisition. Failure preserves the successful prefix; exclusive is ignored
 * for matching. If all matches are reserved by other journals, return EBUSY:
 * an ordinary conflicting operation, never wait or retry the whole compound.
 * ENOENT means absent from this attempt, not a peer's tentative removal. */
void chimera_vfs_claim_journal_unlock(
    struct chimera_vfs_claim_journal *journal,
    struct chimera_vfs_claim_owner *owner,
    const struct chimera_vfs_claim_exact_range *ranges, uint32_t count,
    struct chimera_vfs_claim_batch_result *result);

/* Pure mandatory-lock observation for subsequent operations in this attempt.
 * Accepted rows tentatively removed here are excluded; tentative acquisitions
 * remain admission blockers to other requests but are hidden from GETLK. */
bool chimera_vfs_claim_journal_io_denied(
    struct chimera_vfs_claim_journal *journal,
    struct chimera_vfs_file_state *file,
    uint64_t offset, uint64_t length, bool write,
    const struct chimera_claim_actor *actor);

/* Copy same-file tentative-removal pointers for a VFS io_view. Return the
 * required count; copy only when capacity suffices (out=NULL queries count).
 * Storage and claims remain valid until the next journal mutation/reset. */
uint32_t chimera_vfs_claim_journal_excluded(
    const struct chimera_vfs_claim_journal *journal,
    const struct chimera_vfs_file_state *file,
    const struct chimera_vfs_claim **out, uint32_t capacity);

/* No fallible validation after filesystem effects: execution has already
 * pinned owners/admission and allocated all nodes. Seal and publish have no
 * failure return, allocation, callbacks or I/O. Publication never vetoes an
 * accepted backend finish. Ordinary command errors keep prior deltas; only an
 * actually rejected/aborted attempt may reset them. Backend rollback is not
 * provided by this journal. */
void chimera_vfs_claim_journal_seal(struct chimera_vfs_claim_journal *journal);
void chimera_vfs_claim_journal_publish(struct chimera_vfs_claim_journal *journal);
/* Publish SMB protocol state before complete releases lifetime pins, runs
 * retirement callbacks and pumps waiters. No callback runs under a mutex. */
void chimera_vfs_claim_journal_complete(struct chimera_vfs_claim_journal *journal);
void chimera_vfs_claim_journal_reset(struct chimera_vfs_claim_journal *journal);
void chimera_vfs_claim_journal_free(struct chimera_vfs_claim_journal *journal);
