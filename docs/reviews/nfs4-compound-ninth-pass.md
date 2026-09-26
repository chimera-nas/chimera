# NFSv4 ninth compound pass: parent retirement and shared lock owners

Follow-up: the [tenth pass](nfs4-compound-tenth-pass.md) converts ordinary v4.0
owner replay and confirmation, exclusive/reclaim OPEN and v4.0 LOCKT. Its
boundary inventory supersedes the remaining-boundary summary below.

This pass removes the remaining ordinary NFSv4.1/v4.2 lock-related splits:
CLOSE with child LOCK states, CLOSE or OPEN_DOWNGRADE after LOCK/LOCKU, and
lock owners shared across parent OPENs not explicitly named by the request.
Reclaim LOCK also enters the compound path. Changes remain uncommitted.

## Execution and publication

All converted CLOSE operations now use the OPEN-owner journal. Construction
freezes child states even when the request has no explicit LOCK operation.
The execution checkpoint closes the private parent and advances its private
version. Children remain as tombstones so later stateid operations reject them,
while later LOCKT and range admission see their coverage as released. The
original and tentative physical claims remain held until finish accepts.

Accepted publication retires closed children's ranges before parent teardown
and replacement identity publication. Retirement detaches claims under the file
lock without running callbacks. Owned file references keep waiter queues alive;
waiters are processed after protocol publication. Existing child slots are
invalidated through parent teardown, while frozen references preserve lease
nodes until disposal. Children created and closed within the attempt never
become public. Retry resets the private journals to their frozen originals.

For shared lock owners, construction discovers their parent identities and
reserves the connected set of OPEN-owner groups. It follows children in newly
discovered groups until closure, within the existing capacity limits. Busy or
oversized sets still fall back. Discovery copies identities under locks and
does not expose borrowed parent pointers. Parent reservation also rejects a
child owner already unpublished by legacy RELEASE_LOCKOWNER, closing a race
between owner removal and child destruction.

Reclaim LOCK's recovery check already runs at the execution checkpoint and
only reads recovery state. Removing its scan-time exclusion therefore requires
no new publication action. This supports reclaim LOCK against an established
OPEN; OPEN CLAIM_PREVIOUS remains a separate boundary.

## Remaining boundaries and audit findings

NFSv4.0 still needs an owner replay journal before its state-changing operations
can coalesce safely. New-owner LOCK involves both OPEN-owner and LOCK-owner
sequence numbers. Replay must be classified at the operation checkpoint;
accepted completion must publish consuming errors as well as successful replies.
The current adapter also obtains the client from a v4.1 session and needs a
pinned state-derived client for v4.0.

The audit found three related legacy issues, not fixed in this pass:

- The generic replay cache cannot hold LOCK DENIED's conflicting owner, range
  and type. Legacy LOCK replay copies the success stateid even for cached denial.
- LOCKU's early OLD_STATEID and INVAL returns bypass sequence consumption,
  although the shared sequence-advancement classifier includes these errors.
- Legacy v4.0 LOCK passes a null client to the recovery gate, omitting the
  client-specific recovery-record and reclaim-completed checks.

Typed replay snapshots and attempt-local owner sequence journals are the next
substantial prerequisite. Exclusive/delegated/reclaim OPEN, namespace/export
transitions, delegation/pNFS coordination and earlier anonymous-range and legacy
validation limits remain. Busy reservations, bounded owner/state/interval and
reply budgets, ambiguous preexisting mixed-mode lock lists, and projected
backend range tokens also retain fallback paths. Backend begin/end,
transactions and rollback remain outside this frontend pass.

## Validation

The full Debug+ASan build passed. All four wire variants passed: 122 measured
cases for v4.1 and 160 for v4.2, each normally and with finish-time EAGAIN.
Converted sequences assert one VFS submission. New coverage includes held and
empty child CLOSE, OPEN/LOCK/LOCKU/CLOSE, downgrade, close/reopen identities,
shared lock-owner parents, range visibility after CLOSE, child tombstones,
successful-prefix publication, and errors preventing suffix WRITE. The retry
fixture specifically requires OPEN/LOCK/held-CLOSE to reject once and accept.

State lifecycle tests passed 2/2 and VFS claim tests passed 215 assertions,
including delayed waiter callbacks after multi-owner retirement. Recovery tests
repeat the grace/client gate while checking its state remains unchanged. The
wire reclaim test covers outside-grace rejection; successful grace recovery is
not covered end to end.

The broader selection ran 326 CTest entries in 27.02 seconds: 324 passed, two
skipped, no failures. It includes all three pynfs suites, 224 SDK tests, eight
FUSE tests, 57 boto3 S3 tests, Ceph/model tests, NFS/VFS units and five SMB
compatibility entries. Linux/io_uring NFS3 model probes skipped because the
scratch filesystem lacks name_to_handle_at support. Synthetic retry remains
restricted to filesystem-read-only work and does not establish backend rollback.
KVM tests were not run.

After comment and test-format cleanup, the incremental build passed again.
Formatting, Python/shell syntax and git diff whitespace checks passed.

Logs:

- `/tmp/chimera-compounds-ninth-build.log`
- `/tmp/chimera-compounds-ninth-final-build.log`
- `/tmp/chimera-compounds-ninth-final-tests.log`
- `/tmp/chimera-compounds-ninth-wire.log`
- `/tmp/chimera-compounds-ninth-wire-detail.log`
- `/tmp/chimera-close-children-tests.log`
- `/tmp/chimera-close-children-claims.log`
- `/tmp/chimera-recovery-gate-test.log`
