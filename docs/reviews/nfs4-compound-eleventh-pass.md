# NFSv4 eleventh pass: delegation and pNFS coordination

This pass removes delegation-enabled OPEN/GETATTR/I/O and pNFS size-SETATTR
boundaries while keeping retryable protocol callbacks free of publication and
external callback sends. Changes remain uncommitted.

The subsequent [correctness pass](compound-correctness-pass.md) supersedes the
delegation attribute and independent pNFS backing limitations below, and
distinguishes the earlier probe runs from actual generated model replay.

## Execution contract

`COORDINATE` is an explicit VFS operation, distinct from pure operation
prepare/complete callbacks. It parks the same compound, supports inline or
asynchronous completion on its owning thread, and memoizes its result by
original operation index and resolved FH. A unique completion token rejects
stale/duplicate completions. Retry repeats pure checks without resending a
query/recall for the same key. A changed resolved FH coordinates separately;
memo capacity is bounded and dynamically appended coordination is refused.

Coordination may send external queries/recalls and retain admission barriers.
It may not mutate the filesystem, publish tentative request state, or reply.
Frontend memo data lives outside attempt reset. New blockers must be checked
again before dependent work. Backend transactions must eventually support
parking without preventing the peer operation needed to resume (for example
LAYOUTRETURN). This pass adds no production backend begin/end, rollback, or
transaction support.

## Delegation paths

Session OPEN grants delegations only after accepted VFS finish. The accepted
finalizer retains the compound, client and owner reservations while a callback
probe is outstanding, then resumes publication without repeating state install
or result filling. It restores the final protocol FH/index after processing
successful OPENs. OPENs whose state was closed later in the compound may decline
the optional grant. v4.0 delegated OPEN remains a boundary because its typed
owner replay cache currently stores only nondelegated responses.

Delegation-stateid READ/WRITE/READ_PLUS use a pure state-table authorization
snapshot: client, FH, stateid version, revocation and access type are checked;
the claim actor is copied without lease renewal or shared-state mutation.
This fixes READ_PLUS recalling its own client's delegation.

Foreign GETATTR issues CB_GETATTR as coordination, retains its answer and time,
and reserves a shared per-delegation change-combine journal. Each attempt resets
that journal, revalidates the current delegation without taking transient state
references, calculates into a temporary copy and advances only after reply
staging succeeds. Accepted completion publishes combine state; rejected attempts
do not. Holder clients and delegations remain pinned through asynchronous use.
Legacy GETATTR refuses a reserved combine journal with DELAY. Grant-time change
seeding happens before the delegation claim becomes discoverable.

Delegation-enabled REMOVE and RENAME now perform their victim/source/target
lookups and recall coordination inside the same VFS compound. Temporary cursor
moves preserve the wire saved FH and restore the parent/destination even when a
recall ends the operation with DELAY. Recall-only lookup failures preserve the
legacy ordering by leaving the actual namespace operation to decide its error.
A retained file-state reference permits pure new-cache checks on every attempt.
The pre-existing name-replacement and check-to-mutation races remain; there is
no new atomic namespace claim reservation in this pass.

## pNFS paths

Size SETATTR authorizes first, acquires a per-FH admission barrier, recalls all
holders and parks until their layouts return. The barrier survives retries and
remains held through finish. Final LAYOUTGET publication is bracketed by an
atomic admission check, closing the in-flight grant-after-recall race. Counted
barriers also retain empty table entries until all conflicting requests finish.
Recall snapshots now include all holders instead of truncating at 32 (which
could otherwise leave an unnotified holder keeping the request asleep).

Data-server READ/WRITE/READ_PLUS preserve the existing FH/credential authority
model: the DS does not validate MDS-issued stateids against its own state table.
The bypass is limited to these operations; export, credentials, read-only,
recovery, and file-type checks remain. pNFS alone no longer excludes RENAME:
the legacy RENAME handler does not perform DS backing-file cleanup.

Callback lifetime review also removed shared intrusive layout-recall queue
linkage: every recall owns its own queue node, original layout reference and
client pin. Failure revokes that original layout rather than a later replacement
found by FH. Layout references retain client memory after logical teardown;
recall pins can still finish a pending destruction, avoiding a cycle where a
compound waits for layout return while client destruction waits for the same
compound. CB_GETATTR retains a separate wrapper client pin through release of
its callback-owned delegation reference, even if its resume finishes the request.

## Limits and follow-up

- The real DS fixture exposed an existing pNFS data-coherence limitation:
  LAYOUTGET creates an empty DS backing for an already-written MDS file without
  migrating its bytes. Direct DS tests seed that actual backing explicitly;
  passing compound tests does not establish MDS/DS data coherence.
- An inherited delegation GETATTR combine bug remains: with backend CHANGE=3
  and a holder repeatedly reporting CHANGE=4 and a newer SIZE, the first query
  can return the holder's values but the next treats 4 as unchanged and returns
  backend CHANGE=3 and stale SIZE. Both the old handler and the journal update
  their comparison baseline this way. Fixing this requires separate grant/report
  tracking and a retained dirty attribute view; retry-safe publication alone
  does not fix the semantics.
- pNFS REMOVE still needs accepted-only best-effort DS backing-file cleanup;
  executing cleanup during a rejected attempt would be unsafe.
- v4.0 delegated OPEN needs a full owned delegation response in owner replay.
- Delegation-claim OPEN forms and LAYOUTGET/LAYOUTRETURN control operations
  still use protocol handlers; this pass does not claim universal conversion.
- Granting optional delegations after the accepted prefix can decline a grant
  that an earlier per-op grant would have offered, because successors have
  already established their final state/claims.
- Namespace/cross-export credentials, proxy range-token ownership, bounded
  capacity/contention fallbacks and prior legacy parity items remain.
- Backend transactions must define the validity of retained coordination
  inputs and avoid holding transaction resources needed by recall completion.

## Validation

Final Debug/ASan build passed. The broad 335-test selection finished with
333 passed, two expected `name_to_handle_at` capability skips, and zero failures
in 29.84 seconds. It includes the existing NFSv4.0/4.1/4.2 wire and finish-retry
suites, all three pynfs suites, SDK, FUSE, S3, SMB, VFS and protocol models.

The four new feature suites passed with exact wire-to-VFS span assertions:
14 delegation cases each normal/retry; six MDS pNFS cases plus three direct DS
cases each normal/retry. Tests obtain actual grants/layouts, observe callbacks,
check delegation-stateid READ/WRITE/READ_PLUS, recall source and displaced target
for RENAME, and verify suffix suppression. Truncate stays parked until explicit
LAYOUTRETURN, and a new LAYOUTGET is rejected during that wait. The retry runs
accepted all 24 delegation and four pNFS injected attempts; CB_GETATTR was sent
exactly once across retry. The fault injector excludes filesystem mutation;
these tests do not establish backend transaction rollback.

Six focused coordination/claim/state/lifetime suites passed; delegation and
layout lifetime tests also passed with LeakSanitizer enabled. Unit coverage
includes asynchronous/inline completion, stale tokens, changing FH memo keys,
retry/error memoization, barriers against in-flight grants, 40-holder recall,
client memory retained after teardown and recall during deferred destruction.
Python/shell syntax, per-file formatting and whitespace checks passed. Only
formatting and documentation changed after the final broad run; a subsequent
incremental build passed. KVM was not needed or run.

Evidence: `/tmp/chimera-compounds-eleventh-final-tests.log`,
`/tmp/chimera-compounds-eleventh-feature-final.log`,
`/tmp/chimera-compounds-eleventh-unit.log`,
`/tmp/chimera-compounds-eleventh-leaks.log`, and
`/tmp/chimera-compounds-eleventh-formatted-build.log`.

The initial retry failures were traced to a stale test preload ABI after sandbox
CMake regeneration omitted host-only targets. Host configuration and rebuilding
the injector fixed them. Namespace integration also caught and fixed explicit
COORDINATE EAGAIN-to-DELAY mapping; exhausted finish EAGAIN returns DELAY too.
