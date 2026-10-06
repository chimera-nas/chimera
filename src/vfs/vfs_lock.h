// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#pragma once
#include "vfs.h"
#include "vfs_claim_types.h"

struct chimera_vfs_compound;
struct chimera_vfs_lock_domain;
struct chimera_vfs_lock_attempt;

/* SET ranges are normalized absolute intervals; UINT64_MAX means to EOF.
 * END preserves raw signed offset/length bits and requires backend projection.
 * A successful END mutation moves this owner/file to backend-only arbitration
 * until close: the backend owns atomic END resolution, so local claim queries
 * from other protocols no longer see that owner's ranges.
 * generation is captured at frontend admission, before worker enqueue, and
 * remains unchanged through finish retries. Handles are borrowed/pinned by
 * the caller until terminal completion. */
struct chimera_vfs_lock_request {
    struct chimera_claim_owner owner;
    uint64_t                   generation;
    uint64_t                   offset;
    uint64_t                   length;
    int32_t                    whence;
    uint8_t                    type; /* CHIMERA_VFS_LOCK_READ/WRITE/UNLOCK */
    bool                       wait;
    bool                       project_backend;
    bool                       fail_on_recall; /* nonblocking NLM also refuses cache breaks */
    /* Explicit coordination, on the submitting worker, when admission queues.
     * May send an interim reply; never a final grant. Can recur after retry. */
    void                       (*on_wait)(
        void *);
    void                      *wait_private;
};

struct chimera_vfs_lock_domain * chimera_vfs_lock_domain_create(
    struct chimera_vfs *vfs);
void chimera_vfs_lock_domain_shutdown_async(
    struct chimera_vfs_thread *thread,
    struct chimera_vfs_lock_domain *domain,
    void ( *callback )(enum chimera_vfs_error, void *),
    void *private_data);
void chimera_vfs_lock_domain_destroy(
    struct chimera_vfs_lock_domain *domain);
/* Admission keeps a lightweight owner/full-FH generation tombstone until
 * domain destruction. Empty tombstones do not pin VFS file-state entries. */
uint64_t chimera_vfs_lock_domain_admit(
    struct chimera_vfs_lock_domain   *domain,
    struct chimera_vfs_open_handle   *handle,
    const struct chimera_claim_owner *owner);
/* Local arbitration admission before an asynchronous bookkeeping OPEN. */
uint64_t chimera_vfs_lock_domain_admit_fh(
    struct chimera_vfs_lock_domain   *domain,
    const uint8_t                    *fh,
    uint32_t                          fh_len,
    const struct chimera_claim_owner *owner);
/* Client recovery cutoff for a local-only domain. Invalidates pending opens
 * and attempts as well as accepted coverage; future admissions use new epochs. */
void chimera_vfs_lock_domain_retire_all(
    struct chimera_vfs_lock_domain *domain);
bool chimera_vfs_lock_domain_has_locks(
    struct chimera_vfs_lock_domain   *domain,
    const uint8_t                    *fh,
    uint32_t                          fh_len,
    const struct chimera_claim_owner *owner);
/* Mandatory close cutoff, callable from any thread; no inline completion.
 * Local standing ranges retire immediately. Backend releases are drained by
 * LOCK_RELEASE_OWNER on an owning worker. New admissions receive a new epoch. */
void chimera_vfs_lock_domain_retire(
    struct chimera_vfs_lock_domain   *domain,
    const uint8_t                    *fh,
    uint32_t                          fh_len,
    const struct chimera_claim_owner *owner);
/* Invalidates every admission and retires local ranges. NULL thread is valid
 * for local-only domains; projected cleanup requires a worker and drains via
 * ordinary release operations before destroying the VFS. */
void chimera_vfs_lock_domain_shutdown(
    struct chimera_vfs_thread      *thread,
    struct chimera_vfs_lock_domain *domain);

/* Exactly one typed lock may be built into a compound, before submission.
 * A local-only TEST/CHANGE can accompany cursor operations, non-mutating
 * OPEN_CURRENT, path resolution, metadata queries and replayable CHECKPOINT
 * callouts. Static operations are checked before execution; dynamic additions
 * and prepared arguments are checked before dispatch. Filesystem mutations,
 * other claim journals and multiple typed locks are unsupported: retirement
 * can veto the lock at finish, and separate journals cannot publish atomically.
 * Like any compound, a failed suffix leaves its successful prefix accepted.
 * Backend-projected locks and mandatory RELEASE_OWNER retain dedicated scope,
 * allowing only PUTFH/PUTHANDLE seeds alongside the lock. */
int chimera_vfs_compound_add_lock_test(
    struct chimera_vfs_compound           *compound,
    struct chimera_vfs_lock_domain        *domain,
    struct chimera_vfs_open_handle        *handle,
    const struct chimera_vfs_lock_request *request);
int chimera_vfs_compound_add_lock_change(
    struct chimera_vfs_compound           *compound,
    struct chimera_vfs_lock_domain        *domain,
    struct chimera_vfs_open_handle        *handle,
    const struct chimera_vfs_lock_request *request);
/* Local-only PUTFH, OPEN_CURRENT, GETHANDLE, LOCK_CHANGE. The attempt is reserved during
 * construction, so cancellation/retirement also covers the pending OPEN.
 * UNLOCK only needs PUTFH, LOCK_CHANGE: retiring local coverage must also work
 * for a stale/unlinked backend object. For acquisitions, GETHANDLE is operation 2 and can be taken after accepted completion.
 * The local-only composition rules above apply to any appended operations. */
int chimera_vfs_compound_add_lock_change_fh(
    struct chimera_vfs_compound           *compound,
    struct chimera_vfs_lock_domain        *domain,
    const uint8_t                         *fh,
    uint32_t                               fh_len,
    const struct chimera_vfs_lock_request *request);
int chimera_vfs_compound_add_lock_release_owner(
    struct chimera_vfs_compound      *compound,
    struct chimera_vfs_lock_domain   *domain,
    struct chimera_vfs_open_handle   *handle,
    const struct chimera_claim_owner *owner);
/* Thread safe, including before submit. Requires caller to protect compound
 * lifetime. Cancellation never invokes terminal completion inline. */
bool chimera_vfs_compound_lock_cancel(
    struct chimera_vfs_compound *compound,
    uint32_t                     index);
