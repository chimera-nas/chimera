<!--
SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
SPDX-License-Identifier: Unlicense
-->

# Compound branch reconciliation

The refinement worktree started from cached PR commit `b54b6b32` on September
22. The local tracking ref learned of a forced update on September 23; that
reflog time is not evidence of when the remote rewrite occurred. The PR contains
a patch-equivalent counterpart at `8b1c4142` and substantial subsequent work,
ending at `18035808`. The refinement checkpoint is `21104a97`, published on
`compounds-refinement`. Both histories are preserved while reconciling into the
existing draft PR #1692, `compound-boilerplate`.

The integration worktree is `/tmp/chimera-compound-reconcile`, branch
`compounds-reconciled`. The reconciliation checkpoint was pushed as `bc13021f`,
then the PR history was rebased onto main `c971e5a5` (including the Windows port,
SETATTR lock-stateid and Cairn metadata-conflict fixes, diskfs shutdown repair,
and claim-waiter race fix). The original worktree uses `compound-boilerplate`
and tracks the published PR branch. The old PR tip also has a local backup branch,
`backup/compound-pr-before-reconcile-20260926`.

## Integration choices

Retain the refinement's finish acceptance, retry reconstruction, operation
callbacks, command groups, private frontend results, canonical access owners,
range/access journals, admission coordination, namespace fences and asynchronous
retirement. Retain the PR's richer typed builders, public/internal VFS header
separation, generic claim/recall operations, cancellation parking, owned ACL/SID
results, fallback compound paths and generated test configurations.

Generic CLAIM and RECALL publish to the live arbiter and therefore explicitly
refuse retry. Native journal operations remain the retry-safe path. CLOSE_DOC
also remains a nonretryable compatibility operation. The stronger guarded SMB
reparse replacement and asynchronous CLOSE retirement take precedence over the
PR's simpler replacements; their remaining boundaries must not be hidden.

Merge validation repaired dropped NFSv4 share-state cleanup, SMB canonical owner
and parent-lease actor plumbing, AppInstance reference pins, CREATE-DOC state,
stream metadata and reconnect handling, persistent record identity/lifetime,
private directory pagination and bound lease-key create races. SETATTR must
preserve the distinction between descriptor grants and path permission checks.
READ into application buffers stages data until finish acceptance; a regression
rejects the first finish and verifies the destination stays untouched.

Several PR probes pinned old deviations. DeleteObject now returns 204 for both
existing and missing keys. NLM upgrades re-evaluate conflicts and partial unlock
preserves outside ranges. Buffered COPY supports NFS proxy handles. The modern
JSON corpus configuration must declare these behaviors as well as the older
standalone Quint profiles. NFSv4 namespace recall returns DELAY before unlink;
its probe must return the delegation and verify a subsequent REMOVE succeeds.

## Main rebase and integration repairs

The pre-main reconciliation was pushed to `compound-boilerplate` as requested.
The local rebased history retains the PR changes and the refinement checkpoint,
with backup refs for the original PR, reconciliation checkpoint, and first main
rebase. The first rebase targeted `92ca73e7`; after refreshing main, the two newer
commits through `eb322605` were replayed too. A conflict-resolution fixup was
folded into its predecessor so the published history does not retain intermediate
conflict markers. Validation repairs were published as `989821ec`. Two more
main commits then landed, through `c971e5a5`; the final rebase includes both.
The published intermediate tip is preserved at
`backup/compound-before-final-main-20260926`.

Main's libevpl and ndrzcc revisions are retained. The specs merge is `8dfc579`,
combining the earlier reconciliation with main's platform harness and explicit
pending-CREATE model. Its 104 SMB model selftests pass at 20 samples each;
the full multi-protocol corpus generation also passes. This does not claim the
default 10,000 samples were completed.

Integration repairs preserve:

- Main's portable thread, mutex, condition-variable, export, GSS and platform
  guards, alongside the compound journals and frontend lifetime handling.
- Main's MDS-to-DS ordinary-I/O routing and layout-source behavior. LAYOUTGET
  retains the MDS handle through an explicit GETHANDLE result. LAYOUTCOMMIT uses
  the already-authorized SETATTR path rather than rechecking owner permissions.
- NFS size-changing SETATTR through lock stateids and the correct retained
  read/write handle. Filehandle OPEN performs main's access check and binds the
  descriptor grant; a retained-grant upgrade does not reauthorize old rights.
  Metadata SEEK accepts either read or write opens. Nonregular OPEN maps to the
  protocol's type error. Exclusive-create verifier timestamps survive deferred
  initial-size SETATTR.
- Named streams on directories remain readable despite main's directory-read
  type check. Main's CREATE owner/group SIDs coexist with staged SMB publication.
- Capability bits for main's CREATE_GID_ENGINE/SPARSE and the new namespace
  operations do not overlap. Capability snapshots remain 64 bits.
- Shared range arithmetic uses a portable carry bit instead of GNU 128-bit
  integers. One million randomized comparisons against the previous arithmetic
  pass; native boundary tests cover EOF, overflow, and signed SEEK_END geometry.
  Linux-only backend arithmetic remains platform-specific.
- Main's claim-waiter fix rechecks blockers and queues under the same file
  mutex, preventing a missed wakeup when a break settles before enqueue.
  The integration also checks compound admission fences under that mutex:
  existing waiters remain queued, while fresh requests return without parking.
  A new regression covers both cases and completion after fence release;
  main's immediate-acknowledgment regressions remain active.
- Main's diskfs shutdown repair retains the commit wake sender until the push
  thread stops; those production changes are unchanged from main.

The explicit-open retry fixture now checks mode-compatible handles rather than
assuming that the mode-keyed cache returns the same pointer for WRITE and RDWR.
The payload-ownership fixture keeps assertions active in optimized builds.
Required formatting was applied; aggregate initializers that the formatter
would repeatedly shift were expanded to stabilize subsequent checks.

## Validation and remaining work

GCC Debug/ASan and Release builds pass. The ClangDebug analysis build completed
with 42 reports; ClangRelease initially stopped on a fixture compilation warning,
then completed after repairs. These reports have not all been established as
runtime bugs or resolved. No native Windows build has been performed.

Full Debug and Release quick results at `989821ec` each:
**214 passed, 29 skipped, 30 failed**, 273 total. These full runs precede the
last two main commits; the final rebase is checked with focused regressions.
After the final rebase, 14 focused tests pass in both Debug/ASan and Release,
including claim admission and journals, compound retries, SMB compounds and
the base SMB model, NFS lock replay, FUSE/POSIX locking, and diskfs model/smoke
coverage. The Debug results combine 13 passing tests from the main run with a
separate diskfs smoke run. Its additional `leases_memfs_plain` run reported
oplock-grant and missing lease-break mismatches, then timed out at 600 seconds;
the final rebase has not resolved the known SMB lease-model failures.
Seventeen focused arithmetic, locking and compound boundary/retry checks
pass. Earlier focused S3, SDK and NFS regressions pass, as do filehandle OPEN
authorization, POSIX-over-NFSv4 models, all five pNFS models and the three SMB
stream/reconnect probes. Linux/io_uring filehandle-dependent tests skip or abort
because this container's backing filesystem cannot provide the fixture; these
must be distinguished from behavioral mismatches. Both full-suite configurations
have the same failing test names, although lease timing differs. This is not a
clean CI sweep.

`make syntax` and `make check` were run. Optimized-build warnings in the journal,
payload test, and diskfs harness were repaired. Final syntax, SDK include
boundary, REUSE and copyright checks pass. REUSE was rerun with
`--no-multiprocessing` because sandboxed Python could not bind its forkserver.

Outstanding behavior includes NLM blocking-model divergences, SMB cache-grant
and lease-identity/model mismatches, POSIX-over-SMB mismatches, and NFSv3 RMDIR
through the remote NFSv4 proxy accepting a non-directory target. Those require
follow-up work; branch reconciliation does not certify the conversion complete.

The user approved publishing a separate specs PR. Commit `8dfc579` is reachable on
`chimera-nas/specs:compounds-reconciled`, with draft PR
[specs #33](https://github.com/chimera-nas/specs/pull/33). Root publication uses
force-with-lease; any newer remote work must be reconciled before updating the
lease. The original working branch tracks the PR branch, so ordinary future
pushes target the same history.

The previously recorded LockSequence publication race remains a source-level
finding: VFS journal publication and SMB replay-state publication do not form
one universal cross-layer serialization barrier. It has not been reproduced or
fixed by branch reconciliation. Backend compound transactions remain future work.
