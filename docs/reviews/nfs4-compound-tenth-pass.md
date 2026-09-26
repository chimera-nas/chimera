# NFSv4 tenth compound pass: replay, confirmation and remaining OPEN forms

This pass converts ordinary NFSv4.0 OPEN, OPEN_CONFIRM, OPEN_DOWNGRADE, CLOSE,
LOCK, LOCKU and LOCKT, including owner replay, to the compound path. Fresh
unconfirmed owners and coalesced OPENs use the same machinery. EXCLUSIVE4,
EXCLUSIVE4_1 and CLAIM_PREVIOUS OPEN also stay in compounds when delegation
coordination is disabled. All changes remain uncommitted.

## Replay and acceptance

Reserved OPEN and LOCK owners carry immutable initial replay/sequence snapshots
and private attempt journals. Each operation classifies its sequence number at
its execution checkpoint. The journal rejects a same-sequence replay of a
different operation. Successful operations and consuming errors update private
replay state before the next operation executes. A new-owner LOCK can update
both its OPEN-owner and LOCK-owner journals. Finish rejection resets all of
these journals; accepted finish publishes them before parent teardown creates
CLOSE tombstones.

Replay skips only the replayed operation's VFS steps. Skips occur in each
step's own prepare callback. OPEN replay restores its cached output filehandle
through a trailing PUTFH before successors execute. Per-operation snapshots
preserve replies even when a later operation updates the same owner. Cached
CLOSE tombstones are copied during construction and remain stable across retry.

Typed replay caches own the complete LOCK DENIED owner/range/type and
nondelegated OPEN stateid, flags, change information, attribute bitmap and output
filehandle. Their payloads contain no request-buffer pointers. OPEN and denial
payloads share a union. The larger denial cache costs 1096 bytes per cache and
1120 bytes per state-table slot; slot blocks are allocated lazily. OPEN's typed
payload adds no further cache growth. Delegated OPEN replies remain guarded.

OPEN_CONFIRM changes the private confirmation flag and stateid version. Both
publish only after accepted finish. OPEN4_RESULT_CONFIRM is captured for each
OPEN, so a later confirmation cannot retroactively change its reply. The
existing stateid-zero validation behavior is retained; this pass does not claim
to resolve all legacy v4.0/v4.1 validation differences.

## OPEN checks and client lifetime

Exclusive verifier validation now runs before share admission and successor
operations. Matching retries retain the existing create-only attribute behavior;
mismatches stop suffix writes. Reclaim OPEN checks the recovery/client gates at
execution, as reclaim LOCK already does. Recovery checks remain read-only.

The compound holds a client lifetime pin until disposal. v4.0 LOCKT additionally
looks up and pins the client named by that operation, including without a bound
connection session. Unknown clientids fail at the checkpoint. Lease renewal is
deferred to accepted completion and occurs only for LOCKT operations that reached
their client check. All pins are released on refused construction and final
disposal.

Review found that a session reference does not keep its unified client alive
across client replacement. Client lookup and pinning therefore happen atomically
under the client-table lock, before concurrent removal can destroy the client.
Legacy OPEN also checks the owner reservation before classifying its sequence
number, returning DELAY instead of modifying an owner's replay cache behind a
compound's snapshot.

## Legacy corrections

- OPEN replay now restores the output filehandle, change information and
  attribute bitmap instead of fabricating empty fields.
- LOCK replay preserves complete denial replies and rejects wrong-operation
  sequence replays.
- LOCK/LOCKU early OLD_STATEID and LOCKU INVAL outcomes consume/cache sequence
  numbers consistently with the shared advancement classifier.
- Legacy sessionless LOCK recovery checks use the client pinned by the acquired
  OPEN/LOCK state.

## Validation and limits

All six wire variants passed: 40 v4.0, 129 v4.1 and 167 v4.2 cases, normally and
with finish-time rejection. Retry runs rejected and accepted 40, 180 and 197
compounds respectively. Every measured v4.0 request asserts the exact start
index and complete operation count of its single VFS submission. New exclusive
and reclaim cases do the same after SEQUENCE. This catches a legacy tail that
a submission-count-only assertion would miss.

Tests cover fresh/coalesced OPEN and complete replay, OPEN_CONFIRM, consuming
errors and their replay, successful prefixes, downgrade, range operations,
denial owner bytes, destroyed-state CLOSE replay, LOCKT client validation,
exclusive verifier matching/mismatch and reclaim rejection outside grace.
Lifecycle tests exercise private confirmation/replay reset, accepted publication,
deferred client destruction and legacy OPEN reservation exclusion. A new test
drives the actual legacy LOCKU procedure through consuming errors and replay.

The final Debug+ASan build passed without warnings. After the atomic client-pin
fix and final formatting, all 329 selected CTest entries completed in 27.54
seconds: 327 passed, two expected skips, no failures. This includes all six wire
variants, all three pynfs suites, 224 SDK tests, eight FUSE tests, 57 boto3 S3
tests, Ceph/model tests, NFS/VFS units and five SMB compatibility entries. The
Linux/io_uring NFS3 model probes skipped because the scratch filesystem lacks
name_to_handle_at support. All three focused state/lifecycle/legacy LOCKU suites
also passed with the latest client-pin tests. Formatting, Python/shell syntax
and diff whitespace checks passed. No production edits followed this run.

Successful persisted recovery during grace remains untested end to end. Retry
injection is restricted to filesystem-read-only work; backend transactions and
rollback remain outside scope. KVM tests were not run.

The remaining boundaries are detailed in the
[boundary inventory](nfs4-compound-tenth-boundaries.md). They include credential
and export transitions, synthetic namespace/attribute handles, delegation/pNFS
coordination, proxy/backend range-token ownership, some anonymous admission
paths, sessionless or differently bound v4.0 state operations other than LOCKT,
and capacity/contention fallbacks. Some invalid-input/grace checks still split
during scanning. Legacy fallback parity and successful recovery need further
coverage. This is substantial additional coverage, not universal one-to-one
mapping of every possible wire compound.

Logs:

- `/tmp/chimera-compounds-tenth-final-build.log`
- `/tmp/chimera-compounds-tenth-final-wire.log`
- `/tmp/chimera-compounds-tenth-final-wire-detail.log`
- `/tmp/chimera-compounds-tenth-tests.log`
- `/tmp/chimera-compounds-tenth-final-tests.log`
- `/tmp/chimera-v40-confirm-state-tests.log`
