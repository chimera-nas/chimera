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
`compounds-reconciled`. Its parent is the PR tip. The original worktree remains
at its clean checkpoint. The old PR tip also has a local backup branch,
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

## Validation and remaining work

Debug/ASan builds and all model generation self-tests pass. Focused VFS and SMB
regressions and all six NFSv4 compound boundary/finish-retry variants have passed
in the integration process. S3 base and multipart models pass. The full quick
suite exposed additional shared integration defects and stale model settings;
validation is ongoing. The latest pre-rebase run still fails NLM blocking
traces, SMB model profiles (including unexpectedly parked CREATEs), and the
POSIX-over-SMB model. These are explicitly unresolved at the reconciliation
checkpoint. The delegation probe now resolves the returned filehandle and checks
that DELEGRETURN actually executed. NLM model self-tests include partial unlock
and successful partial upgrade; all nine full-profile self-tests pass.

The main rebase, including the Windows port and dependency updates, has not yet
started. The reconciliation checkpoint is being published first, as requested; final
validation and the main rebase remain outstanding.

The previously recorded LockSequence publication race remains a source-level
finding: VFS journal publication and SMB replay-state publication do not form
one universal cross-layer serialization barrier. It has not been reproduced or
fixed by branch reconciliation. Backend compound transactions remain future work.
