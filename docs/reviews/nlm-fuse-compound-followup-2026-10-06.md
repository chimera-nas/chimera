<!--
SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
SPDX-License-Identifier: Unlicense
-->

# NLM/FUSE compound follow-up — 2026-10-06

Ordinary NLM and FUSE operations already enter through compounds. This pass
addresses the remaining shared lock composition/API boundary and a confirmed
file-state lifetime bug, rather than adding a second frontend conversion.

## Changes

- A local-only typed lock can share its compound with path resolution, cursor
  management, non-mutating OPEN_CURRENT, metadata queries and replayable
  CHECKPOINT callbacks. The executor no longer demands the exact four-operation
  NLM bookkeeping-open shape. The existing NLM convenience builder still
  reserves admission before OPEN and preserves its result indices.
- Scope checks run before submission dispatch and again after each operation's
  prepare callback, before even implicit opens. Runtime-added mutations and
  prepared CREATE/TRUNCATE flags cannot evade the static check. Supported
  dynamic metadata suffixes are discarded and rebuilt on retry.
- Executor lock-attempt allocation, execution, acceptance and disposal moved
  from the public lock header to a guarded internal header. The northside API
  check now rejects those symbols and that header in production consumers.
  Frontends retain compound builders, cancellation and domain lifecycle APIs.
- Admission retains only an owner/file generation tombstone; file state is
  acquired when an attempt is constructed. Close/recovery releases the domain's
  file-state reference once no attempts, accepted ranges or backend cleanup
  need it. Previously a lock accepted and freed before retirement left that
  reference pinned until domain destruction. Pending attempts retain their
  reference, and generation tombstones prevent old queued admissions from
  becoming valid after cleanup.

## Remaining limits and intentional control APIs

Exactly one typed lock journal is allowed per compound. Multiple mutations need
joint reservation and publication: simply removing the count check would allow
same-owner admission to deadlock, build later edits from stale committed ranges,
or partially publish before another owner's cancellation veto. Filesystem
mutations and other claim journals also remain excluded. A late close/recovery
veto cannot undo already completed backend effects. Ordinary prefix semantics
still apply: failure of a later metadata query does not undo an accepted lock.

Projected POSIX locks and mandatory owner release retain their dedicated scope.
Backend begin/finish/abort, transaction-aware lock projection, and NLM projection
remain the backend follow-up. NLM SHARE/UNSHARE's preexisting lack of enforcement
is a separate feature, not an unconverted filesystem operation.

NLM CANCEL, recovery, disconnect and shutdown remain independent control actions
so they can invalidate parked work. FUSE FLUSH runs COMMIT through its compound
and performs mandatory owner retirement only after terminal completion, including
a terminal failure. Moving retirement into a retryable pre-finish callback would
release locks on a rejected attempt. FUSE interrupt, teardown and optional cache
coherence controls likewise remain outside ordinary filesystem sequences.

Generation tombstones remain allocated until domain teardown. Reclaiming them
requires tracking admissions across frontend enqueue, not just active compound
attempts, to prevent generation reuse. They no longer pin VFS file-state entries.

## Validation

A new quick-tier VFS regression covers admission without file-state retention,
accepted retirement and finish-pending retirement for both NLM/FUSE identities,
old queued admissions after cleanup, local metadata composition with finish
retry, dynamic suffix replay, and static/runtime rejection of unsafe mixtures.

The focused Debug and Release runs each pass 28 tests across the VFS compound/claim executor,
NLM on five backends, FUSE simulation and mounted locks, and POSIX/native NFS3
locking. The FUSE io_uring mount test is skipped because the host FUSE module has
`fuse.enable_uring=0`.
Both Debug and Release quick sweeps pass 286/289; only the three established
remote pNFS suites fail. A formatting-only test initializer correction passes
`make syntax` and `syntax-check`; the rebuilt Debug regression passes again.
Both Clang stages retain exactly the prior 40 warning signatures / 112
occurrences, with none added or increased. The full `make -k check
CTEST_PARALLEL=8` remains red on the established pNFS failures and analyzer
reports. Its initial test-format failure was corrected and the formatting
checks rerun successfully. SDK/northside API guards, REUSE and copyright checks
pass. Both final builds and the new locking regression pass after retaining the
public opaque type declaration used by compound structures; attempt methods
remain private.

Logs are `/tmp/chimera-nlm-fuse-check.log`,
`/tmp/chimera-nlm-fuse-focused-{debug,release}.log`,
`/tmp/chimera-nlm-fuse-final-unit-{debug,release}.log`, and
`/tmp/chimera-nlm-fuse-warning-compare.txt`. MEMORY.md records the implementation
and validation details.
