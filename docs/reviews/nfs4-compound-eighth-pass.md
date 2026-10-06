# NFSv4 eighth compound pass: LOCK and LOCKU

Follow-up: the [ninth pass](nfs4-compound-ninth-pass.md) removes the child-lock
CLOSE, post-LOCK parent mutation, shared-parent reservation and v4.1 reclaim
LOCK boundaries described below. NFSv4.0 owner replay remains separate work.

The user requested LOCK and LOCKU next. Ordinary NFSv4.1/v4.2 locking now runs
inside the VFS compound, including a new lock owner referring to an OPEN earlier
in the same run. Changes remain uncommitted.

## Execution and publication

Before submission, lock-capable OPEN-owner reservations freeze the parent OPENs
and their existing child LOCK states. Lock-owner groups reserve unpublished
candidate identities and hold client lifetime pins. Active borrowers and
incompatible reservations force construction fallback. Concurrent legacy state
acquisition, LOCK creation and RELEASE_LOCKOWNER cannot mutate frozen state.

Each LOCK checks its execution-time filehandle, session client, principal,
stateid version, parent access mode, recovery status and requested interval.
VFS RESERVE_HANDLE admits a map-owned range claim. Only successful admission
commits the private interval journal and advances its sequence number. LOCKU
changes that journal directly: it splits or removes intervals, and permits a
successful no-op when the requested region holds no lock. All response stateids
are per-operation snapshots. CURRENT substitution uses local copies.

The journal normalizes overlapping or adjacent same-mode intervals and replaces
the overlapping portion on a mode conversion. Its interval arithmetic represents
the exclusive end beyond UINT64_MAX, preserving the final byte for to-EOF locks.
Storage for transformations and final public lease nodes is allocated before
execution. Execution callbacks do not allocate or publish protocol state.

Original and intermediate physical range claims remain held until finish. This
attempt excludes them from admission and supplies its normalized private ranges
as an overlay. Later LOCKs from other owners and LOCKT therefore see partial
unlocks and mode changes correctly while other requests remain protected by the
physical reservations. I/O, TEST_STATEID and IO_ADVISE resolve private LOCK
identities and versions through the same parent journals.

Accepted completion publishes OPEN parents first, then atomically replaces each
lock state's full physical range set under the file lock. Replacement coverage
must be backed by previously admitted claims. There is no release/reacquire gap.
It then publishes the child identity, range list and sequence number; waiter
processing occurs outside protocol publication locks. Teardown releases VFS
attempt resources before journals, then lock-owner groups before parent groups.

Rejected finish discards tentative claims and resets geometry, versions and
CURRENT state from frozen originals. An accepted operation error publishes only
the successful prefix. LOCK denial preserves the conflicting owner's bytes,
mode and interval for the eventual reply.

Final review also tightened legacy LOCK lifetime ordering: a new child is
acquired before releasing its parent borrow, and failed fresh states are
destroyed before releasing their acquire reference. These close gaps in which
compound construction could otherwise freeze state still being finalized by
legacy dispatch.

The interval/version behavior follows [RFC 8881 sections 9.2–9.4](https://www.rfc-editor.org/rfc/rfc8881.html#section-9.2),
with operation rules in [LOCK](https://www.rfc-editor.org/rfc/rfc8881.html#section-18.10)
and [LOCKU](https://www.rfc-editor.org/rfc/rfc8881.html#section-18.12).

## Remaining boundaries

- NFSv4.0 lock-owner replay and reclaim LOCK retain legacy dispatch.
- CLOSE with child lock states, and CLOSE/OPEN_DOWNGRADE after a LOCK/LOCKU
  within the run, still split at the dispatcher. Extending parent retirement
  across the lock journal is a natural next step.
- A lock owner whose other parent OPENs have not been frozen by this request
  forces fallback. Busy states, owner/state limits, more than 256 original
  intervals per journal, and compound operation/reply budgets remain bounded.
- Preexisting overlapping mixed-mode legacy interval lists have ambiguous
  ordering and fall back rather than guessing which mode was last applied.
- Projected backend range tokens are excluded. NFSv4 ranges already use local
  arbitration under the existing backend projection policy; this pass preserves
  that policy. Blocking lock requests retain the existing nonwaiting response
  behavior. Backend lock projection and compound transactions remain separate work.
- The earlier namespace/export, delegation/pNFS, anonymous range-admission,
  legacy validation and fresh-state publication allocation limits still apply.

## Validation

All four wire variants passed: 109 v4.1 and 146 v4.2 measured cases, normally and
with finish rejection. New cases exercise OPEN→LOCK→LOCKU→READ/TEST_STATEID,
existing lock identities, splits, merges, mode conversion, to-EOF and no-op
unlocks, private competing owners, LOCKT overlays, denial ownership, successful
prefixes, old stateids, wrong filehandles and malformed ranges stopping WRITE.
Converted sequences assert one VFS submission. The retry harness requires the
specific OPEN/LOCK/LOCKU fixture to reject once and accept once.

Focused ASan VFS suites passed; claim tests now pass 209 assertions. State and
owner-lifetime suites passed, including reservation abort, unpublished candidate
identity, private interval reset, accepted replacement, unrelated SMB conflicts,
and deferred client teardown across a fresh OPEN parent and LOCK child.

The final Debug+ASan build passed without warnings. After the legacy lifetime
fixes, the broad selection ran 326 CTest entries in 27.11 seconds: 324 passed,
two skipped, no failures. It includes all three pynfs suites, 224 SDK tests,
eight FUSE tests, 57 boto3 S3 tests, Ceph/model tests, NFS/VFS units and five SMB
compatibility entries. The Linux/io_uring NFS3 model probes skipped because the
scratch filesystem lacks name_to_handle_at support. Python/shell syntax and git
diff whitespace checks passed. No production edits followed the final run.
Synthetic retry only uses filesystem-read-only work and does not demonstrate
backend transaction rollback. KVM tests were not run.

Logs:
- `/tmp/chimera-compounds-eighth-final-build.log`
- `/tmp/chimera-compounds-eighth-final-tests.log`
- `/tmp/chimera-compounds-eighth-wire.log`
- `/tmp/chimera-compounds-eighth-wire-detail.log`
- `/tmp/chimera-compounds-eighth-vfs-tests.log`
- `/tmp/chimera-lock-state-ctest.log`
