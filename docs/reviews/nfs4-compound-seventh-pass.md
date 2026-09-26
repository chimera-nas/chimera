# NFSv4 seventh compound pass: downgrade and protocol checkpoints

This pass converts ordinary NFSv4.1/v4.2 OPEN_DOWNGRADE, TEST_STATEID,
IO_ADVISE and SECINFO_NO_NAME into the VFS compound execution path. Changes
remain uncommitted. The guarded cases below still use legacy dispatch.

Follow-up: the [eighth pass](nfs4-compound-eighth-pass.md) adds ordinary
v4.1/v4.2 LOCK/LOCKU and private byte-range journals. The limits below describe
the seventh-pass snapshot.

## Implementation

OPEN_DOWNGRADE reserves its owner before submission, including runs without an
OPEN. Execution checks the requested access/deny subset against private state
and correlated OPEN history, reserves the narrowed VFS claim, and commits a
journal checkpoint. Each successful operation retains its response stateid.
Later operations see reduced rights and collapsed history; other requests keep
seeing original public state until accepted finish. A failed suffix preserves
the successful prefix. Rejected finish rebuilds from frozen original state.

The admission view excludes superseded public/intermediate claims while enforcing
unrelated NFS/SMB claims; rebuilding and deduplicating it bounds storage. New VFS
RESERVE_HANDLE admits a claim against a caller-pinned handle independently of
the cursor. Accepted claim replacement allows narrowing without a release/reacquire
gap, and processes waiters after protocol publication locks are released.

TEST_STATEID fills staged results at execution, including private OPEN, downgrade
and CLOSE effects. Retries overwrite its retained result array. Per-state errors
do not terminate the compound. It can start without a filehandle or follow
SECINFO_NO_NAME, and special stateids are invalid. These rules are stated in
[RFC 8881 section 18.48.3](https://www.rfc-editor.org/rfc/rfc8881.html#section-18.48.3).

IO_ADVISE validates execution-time state and reports no honored hints. Public
TEST_STATEID/IO_ADVISE validators use shard-locked snapshots and atomic stateid
sequence scalars, without acquiring/releasing references, renewing leases,
mutating RPC input or triggering final-reference cleanup. Owner replay sequence
fields remain unchanged. Private state resolves through the journal. Advice for
state types other than OPEN/LOCK retains guarded fallback.

SECINFO_NO_NAME reports existing export security policy and consumes the logical
current filehandle on success. PUTFH/RESTOREFH can restore it within the same VFS
compound; premature filehandle users get NOFILEHANDLE. The synthetic pseudo-root
remains a namespace boundary.


## Additional correctness fixes

This pass corrects the sixth pass's saved-CURRENT assumption. I/O substitutes
sequence zero for CURRENT and uses the latest state version, even after
RESTOREFH. CLOSE and OPEN_DOWNGRADE preserve the saved version. Explicit nonzero
old/future stateids still require version checks. See [RFC 8881 section 8.2.3](https://www.rfc-editor.org/rfc/rfc8881.html#section-8.2.3)
and [section 8.2.4](https://www.rfc-editor.org/rfc/rfc8881.html#section-8.2.4).
Converted I/O, advice, size-SETATTR and range endpoints resolve CURRENT into
local copies. Tests distinguish saved-CURRENT READ success from saved-CURRENT
OPEN_DOWNGRADE returning OLD_STATEID after an intervening upgrade.

Review and wire testing exposed missing explicit version checks on public v4.1+
I/O state and incomplete public size-SETATTR authorization. Converted paths now
check versions, principal and applicable peer share denials. Downgrade ignores
delegation WANT bits when checking effective access, per [RFC 8881 section 18.18.3](https://www.rfc-editor.org/rfc/rfc8881.html#section-18.18.3).

Abandoned compound construction now restores its reply-buffer mark before
legacy fallback, preventing repeated attempts from consuming the reply arena.
The regression repeatedly refuses construction after allocating 4,000 status
results, then verifies the full legacy TEST_STATEID prefix and failed SETATTR
suffix without mutation. A larger valid 6,000-stateid request exposed a separate
legacy allocation bug: results consumed the space needed by the generated reply
sender's 260 transport iovecs. The legacy handler now preserves aligned reply
headroom and returns REP_TOO_BIG before allocation. Its regression verifies the
successful prefix, rejected TEST_STATEID, stopped WRITE and subsequent requests.

## Remaining limits

- NFSv4.0 replay/lease behavior, exclusive/delegated/reclaim OPEN, LOCK/LOCKU,
  child-lock CLOSE and delegation/layout coordination retain boundaries.
- Busy states, child locks, named streams, competing owner reservations and
  bounded journal capacity can force fallback before execution.
- Namespace/pseudo-root/junction handling, cross-export credentials, reply and
  operation capacity limits, asynchronous delegation/pNFS and proxy COPY
  ownership still need work.
- Anonymous size-SETATTR and range operations after CLOSE need private admission
  plumbing; anonymous CLONE also remains a boundary.
- Legacy CURRENT substitution and generic TEST_STATEID validation have not
  received all converted-path corrections and merit a consistency pass.
- Fresh state publication still uses process-fatal uthash allocation. It has no
  recoverable publication error, but is not allocation-free.
- Backend begin/end, transactions and rollback remain deferred. Synthetic finish
  rejection only exercises filesystem-read-only compounds; it demonstrates
  frontend publication/retry behavior, not backend rollback.

## Validation

The final Debug+ASan build passed without warnings. Broad regression after the
headroom fix ran 326 CTest entries in 27.04 seconds: 324 passed, two skipped,
no failures. It includes all three pynfs
suites, 224 SDK tests, eight FUSE tests, 57 boto3 S3 tests, Ceph/model tests,
NFS/VFS units and five SMB compatibility entries. The Linux/io_uring NFS3 model
probes skipped because scratch lacks name_to_handle_at support. KVM was not run.
No production edits followed this final regression run.

Focused state/VFS tests passed, including 188 claim assertions, explicit-handle
reservation across retry, narrowed-claim publication/wakeup, zero-candidate
owner reservations, and pure TEST_STATEID/IO_ADVISE validation. These cover
client/version/revocation checks, frozen states and dead-parent LOCK validation
without reference or lease mutation.

Four wire variants passed 91 v4.1 and 128 v4.2 cases, both normally and with
synthetic finish rejection. Measured converted sequences require one VFS submission;
intentional fallback cases declare their expected counts. The retry fixture
requires the targeted existing-state OPEN/DOWNGRADE/READ case to reject once
and then accept. Python/shell syntax and git diff whitespace checks passed.

Logs:
- `/tmp/chimera-compounds-seventh-final-build.log`
- `/tmp/chimera-compounds-seventh-final-boundaries.log`
- `/tmp/chimera-compounds-seventh-final-tests.log`
- `/tmp/chimera-downgrade-state-tests.log`
- `/tmp/chimera-advise-state-tests.log`
- `/tmp/chimera-compounds-seventh-headroom-build.log`
- `/tmp/chimera-compounds-seventh-headroom-wire.log`
- `/tmp/chimera-compounds-seventh-headroom-tests.log`
