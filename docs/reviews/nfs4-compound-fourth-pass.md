# NFSv4 fourth compound pass: provisional OPEN and CLOSE

See the [fifth pass](nfs4-compound-fifth-pass.md) for newer boundary reductions.

This report supersedes the OPEN/CLOSE inventory in the earlier pass reports.
The user approved per-operation provisional protocol state, starting with CLOSE
and multiple OPENs. Changes remain uncommitted in `compounds-refinement`.

## Implemented behavior

Each supported fresh-owner v4.1/v4.2 OPEN now has its own reserved slot, stateid,
handle, claim and publication flag. The former single-OPEN suffix restriction
is removed. Later operations authorize against the execution-time current and
saved stateids and can use any earlier provisional OPEN, including COPY/CLONE
subject to their existing endpoint/export/backend guards.

CLOSE uses an ordered VFS CHECKPOINT. Public CLOSE state and owner/client
resources are reserved before submission, like fresh OPEN reservations, and
remain pinned across attempts. Competing requests may receive DELAY while a
reservation is outstanding. This is construction-time admission: the CLOSE
execution callback only checks its actual filehandle/stateid and updates the
attempt-private closed-state view. It neither destroys public state nor acquires
a shared reservation. Earlier I/O in the same run can still use the reserved
state until its CLOSE executes.

A successful CLOSE invalidates current-stateid use and explicit or saved uses
of the closed identity for subsequent operations. Public state destruction and
lease renewal occur after accepted finish. A successful CLOSE remains effective
when a later operation fails; a rejected attempt publishes neither it nor OPEN.
An OPEN closed in the same attempt never needs a public state slot at all.

Later fresh-owner OPEN admission excludes claims closed earlier in this attempt.
The exclusion view belongs to the proposed claim only: other requests continue
to see the original claim, and unrelated conflicting claims still reject the
new OPEN. Public claim publication clears the borrowed exclusion view.

PUTFH's unlinked-file liveness check now combines public open states excluding
pending closes with still-live provisional OPENs. This handles both
OPEN/REMOVE/PUTFH and CLOSE/PUTFH without publishing private state early.

Session compound state lookups have a no-renewal API. The accepted completion
renews the session client's lease. State cleanup retains its client pin while
releasing claims and handles outside protocol locks, including the case where
another request destroys a newly published OPEN before reply assembly ends.

## Contract and remaining boundaries

The synchronous prepare/complete callbacks retain their attempt-private
contract. Protocol admission that must reserve shared resources happens before
submission; accepted completion publishes, and disposal cancels unaccepted or
unreached reservations. Retry clears the private view and retains construction
reservations. The new CHECKPOINT is an ordered decision with no filesystem work.

This remains supported-run conversion, not universal request-wide mapping:

- Multiple fresh owners are supported. Same-owner/coalesced OPEN, multiple
  public CLOSEs under one owner, v4.0 replay and exclusive/delegated OPEN retain
  boundaries. A CLOSE that cannot reserve safely ends the run before that op.
- Existing CLOSE with child lock states stays on the legacy path. OPEN_DOWNGRADE,
  LOCK/LOCKU and other shared protocol-state changes need equivalent reservations.
- Anonymous/special-stateid I/O after CLOSE still starts another run because its
  implicit VFS claim admission lacks a pending-close exclusion view. Owned I/O
  and fresh-owner OPEN can use the private view within the run.
- Inherited saved-FH seeding, namespace junctions/export credentials, SECINFO
  cursor consumption, capacity/reply limits, asynchronous delegation/pNFS and
  the NFS proxy COPY buffer contract remain previous boundaries.
- v4.0 state lookups retain their existing lease-renewal behavior. Legacy OPEN
  tails retain fallible post-finish installation/coalescing. These paths still
  need refinement before claiming the strict retry contract for all NFSv4.

Backend transaction begin/end integration and actual rollback remain deferred.
The finish-EAGAIN interposer is test-only and rejects only filesystem-read-only
compounds; it does not simulate rollback of mutations.

## Validation

Debug+ASan build passed with no compiler warnings. All four wire test variants
passed: 35 v4.1 and 59 v4.2 cases, each requiring exactly one tagged VFS
submission. The finish-retry variants injected and accepted 23/30 read-only
finish conflicts, including multiple OPENs and an execution-error prefix.

The wire cases cover multiple fresh OPENs and saved/current stateids, private
and existing CLOSE, use-after-CLOSE rejection, successful CLOSE prefixes,
READ before a pre-reserved CLOSE, deny-release/reopen ordering, private OPEN
pins after unlink, and last-CLOSE stale filehandles. The test-only preload
wrapper preserves ASan runtime ordering.

The final broad selection ran 321 CTest entries in 26.92 seconds: 319 passed,
two skipped, zero failures. It included all three pynfs suites (v4.0/4.1/4.2),
224 SDK tests, eight FUSE tests, 57 boto3 S3 tests, Ceph S3/model tests, and
NFS/VFS units. The two NFS3 Linux/io_uring model probes skipped because the
scratch filesystem lacks the required name_to_handle_at support.

State tests cover close reservation validation, contention, abort/accept,
client/owner lifetime and deferred destruction, no-renewal lookup, and private
liveness exclusions. Claim tests passed 157 assertions, including 11 new
checks that private exclusions preserve ordinary visibility and unrelated
blockers. Python/shell syntax and git diff whitespace checks passed.

Logs:
- `/tmp/chimera-compounds-fourth-build.log`
- `/tmp/chimera-compounds-close-lifetime.log` (four focused wire variants)
- `/tmp/chimera-compounds-fourth-final-tests.log` (broad final selection)
- `/tmp/chimera-claim-exclusions.log` (focused claim assertions)

No production changes followed the broad run. KVM tests were not run; backend
transaction rollback remains outside this pass.
