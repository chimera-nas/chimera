<!--
SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
SPDX-License-Identifier: Unlicense
-->

# Compound frontend review memory

Recorded 2026-09-22 at the user's request. The earlier sections preserve the original review and read-only investigation; the implementation follow-up below supersedes their checkout and authorization notes.

Latest status: [NFSv3, SDK/POSIX, and multipart implementation and completion assessment](docs/reviews/compound-nfs3-sdk-multipart-pass.md), following the [remaining conversion audit](docs/reviews/compound-conversion-remaining.md) and [correctness cleanup](docs/reviews/compound-correctness-pass.md). Earlier findings below are historical; consult the latest pass records before treating them as still open.

## Goal and reviewed revision

The user wants appropriate frontend operations routed through native VFS compounds, with NFSv4 and SMB2 wire compounds mapping as closely as possible to one VFS compound and S3 requests becoming compounds. Production backend transaction support is deferred. Frontend design must already accommodate incremental execution, backend begin/end notifications, per-operation compound association, and EAGAIN at compound finish without publishing rejected-attempt results or state.

The `/worktrees/compounds` checkout is `8b231dc1`, matching origin/main, and does not contain the conversion. The review targeted the existing `origin/compound-boilerplate` ref at `b54b6b32d8f12c3346b99076f938fab767a1888d`, merge base `7944bdd63f2f4af6b6e04837563b4fd04a9cc0f0`. The user was told this target; no explicit alternative was supplied. Do not silently assume the current checkout contains the reviewed code or confuse it with origin/transactions.

Full findings: [docs/reviews/compound-frontends-b54b6b32.md](docs/reviews/compound-frontends-b54b6b32.md). Archived source is `/tmp/chimera-compound-review`; reconstruct with `git archive b54b6b32` if absent. Source links in the saved report are pinned to the reviewed commit.

## Findings to retain

Recommendation: hold before merge. Current regressions identified by tracing new and original paths:

- NFSv4 RO policy checks precede leading PUTFH decode and use the old export ID, permitting mutations on RO exports.
- Converted NFSv4 READ/WRITE omit reboot-grace checks.
- Later PUTFH changes executor state without changing the authorization target; size SETATTR can truncate the wrong file.
- ALLOCATE/DEALLOCATE lose stateid-to-current-filehandle validation.
- SETATTR ignores attribute decode errors and can apply partially decoded mutations.
- VERIFY/NVERIFY omit mask validation and compare stripped/mode-derived ACLs.
- Response-space errors are detected after later mutations execute.
- LOCKT's late-failure guard is missing for ALLOCATE/DEALLOCATE/WRITE_SAME.
- NFSv3 ACCESS loses ACL-based access checks.
- COPY_RANGE/WRITE_SAME completion counts truncate from 64 to 32 bits.
- GETHANDLE can turn a borrowed reference into an owning output; repeated GETHANDLE or subsequent CLOSE can double-release.

Architecture/retry findings:

- NFSv4 completion ignores compound-level status; finish-time EAGAIN with successful per-op statuses would publish success.
- FUSE READDIRPLUS publishes shared node/claim state before finish; its reset does not fully undo grants on existing nodes.
- OPEN clears stored input attributes; replay cannot recover original arguments. Multiple READDIRs retain obsolete absolute response-buffer marks across retries.
- SMB2/S3/REST have no VFS-compound calls in this revision; NFSv3 converts only six procedures. NFSv4 groups eligible runs rather than preserving one request-wide transaction boundary.
- OPEN ends a VFS run. OPEN state/share installation occurs during result filling, after VFS execution. The compound is freed before delegation handling and possible UNCHECKED size-zero truncation. Later wire ops resume through the original dispatcher.
- Executor stop-on-first-error and one credential per compound do not represent existing SMB2 independent/related chain behavior.
- Result representation drops ACLs and required pre/post attributes, obstructing full NFSv3 WCC and SMB security conversion.
- Four-cursor migration is incomplete: implicit opening and several addressing mechanisms coexist.

Validation: isolated C probe including reviewed vfs_compound.c, with a release counter and available debug libraries, confirmed erroneous releases, 4 GiB WRITE_SAME reporting zero, and ACL-denied READ becoming allowed after attribute stripping. Probe source saved in docs/reviews/compound-review-probe.c. These are targeted probes, not full protocol/backend tests; full suites were not run against this branch. LeakSanitizer required disabling in the traced environment. Other findings are source-trace conclusions, with examples in the full report.

## Active follow-up

The user asks to investigate why an NFSv4 compound must stop at OPEN, calling it a major conceptual gap. Investigate and explain before making implementation changes. Distinguish the wire compound (continues) from the VFS sequence (ends). Inspect nfs4_vfs_op_ends_run, OPEN result filling, nfs4_vfs_compound_complete, chimera_nfs4_open_install_state, grant_delegation, open_complete/open_finish, current-stateid dependencies, and the execution gate's synchronous/no-side-effects contract.

Investigation conclusion: stopping at OPEN is an implementation boundary, not a requirement to end the wire compound. The current VFS sequence can execute asynchronous operations, but it exposes only a synchronous pure per-op veto and a final callback; the NFS adapter places unfinished OPEN semantics in that final callback.

- `nfs4_compound_vfs.c:322` explicitly ends the run at OPEN. Its context has only one `open_present/open_res_index/open_filled` record (around line 218).
- OPEN result filling (around 623–750) still performs exclusive-verifier/type checks, takes the VFS handle, installs or coalesces NFS open state, acquires share claims, and records deferred truncate. These can fail, so later mutations cannot run ahead of them.
- `nfs4_compound_vfs.c:1451` frees the compound before delegation handling and `chimera_nfs4_open_complete`.
- `nfs4_proc_open.c:754` can issue fsetattr(size=0) outside the sequence, after share admission, for UNCHECKED opening an existing file. This ordering prevents a refused OPEN from truncating the file and must be preserved within a redesigned compound.
- `nfs4_proc_open.c:638` advances v4.0 owner replay/seqid state, establishes v4.1 current stateid, then resumes wire dispatch. Those are coupled today but need separate tentative execution state and final publication.
- `nfs4_compound_vfs.c:2885` actually excludes OPEN whenever delegations are enabled. Thus asynchronous delegation handling is a stated rationale, but converted OPEN already splits even with delegations disabled and without truncation.
- `nfs4_compound_vfs.c:2558` rejects following READ/WRITE using current stateid; CLOSE is absent from the encoder allow-list (around 239). Removing the OPEN cutoff cannot fix these dependent gaps.
- `nfs4_state.c:992` installs new opens in shared tables; `nfs4_state.c:1270` coalesces existing state by mutating shares and incrementing seqid. Moving these unchanged into the pure veto callback would violate retry safety. The existing truncate-failure unwind is not a general rollback for coalesced state.

Design direction to discuss: retain one compound across OPEN and its dependent operations; resolve dependencies at execution time; give the attempt private protocol state and VFS-owned provisional claims; include conditional truncate in the same compound after admission; allow execution to park without finishing the transaction; publish shared NFS state, replay/seqids, delegation results, and replies only after accepted backend finish. Preparation must settle fallible admission and reserve publication resources before backend commit. This can be expressed through compound operations and pure/private-state adapters, without embedding NFS types in VFS or allowing arbitrary side-effectful frontend callbacks. Preserve successful-prefix semantics when a later protocol operation fails; distinguish that from finish-time EAGAIN, which discards/retries an attempt. No implementation changes have been authorized or made.

## User clarification and delegation

The user explicitly confirmed the intended model: operation-specific frontend callbacks are provided when building the compound, and the VFS invokes them during execution whenever it needs frontend input/checks. The frontend guarantees no side effects because callbacks may run again on retry. A callback may return a different decision when an attempt observes different filesystem state; replay safety does not mean caching its first answer. Keep externally visible coordination and publication out of these pure callbacks; attempt-local outputs must be reset on retry.

The user then requested launching subagents in a dynamic workflow. Three bounded read-only investigations were launched: `open_callouts` (OPEN responsibility separation), `callback_contract` (VFS callout/attempt API and tests), and `frontend_fit` (SMB2/S3 requirements). Primary agent coordinates and integrates; no implementation edits are authorized by this delegation request.

All three completed. Additional conclusions to retain:

- The existing gate is compound-wide and post-operation (`vfs_compound.h:580`, `vfs_compound.c:1644`). Operation-specific checks need checkpoints before an action they can reject, with the actual resolved target and prior attempt results. Immutable operation inputs, attempt outputs, attempt reset, and final teardown need distinct lifetimes.
- OPEN v4.0 replay needs an execution-time decision that can skip that OPEN's filesystem action without suppressing preceding wire operations. Existing entry mutates owner/client/connection state (`nfs4_proc_open.c:1736`); separate request admission/serialization from pure checks. Multiple OPENs need per-op records and a private overlay for tentative state/coalescing/current stateid.
- Share acquisition can initiate delegation recalls (`nfs4_proc_open.c:93`); delegation grant can start CB_NULL probes (`:244`). Calling these tentative reservations does not make emitted messages reversible. They need explicit retry-safe coordination outside pure frontend callbacks. Reserve protocol admission/resources before backend commit so final publication cannot fail afterward.
- SMB existing-handle lookup clears durable replay eligibility, and channel sequence checks mutate shared opens (`smb_internal.h:3150,3197`); CREATE publishes tree state (`smb_proc_create.c:1849`). Following commands must instead resolve attempt-private opens and tentative updates until acceptance.
- SMB STATUS_PENDING sends credits, establishes AsyncId and cancellation registration (`smb_async_interim.c:85,113`). These are logical-request control effects that must happen at most once and survive attempt retries, separate from pure callouts. Per-command credentials/context and protocol-specific continuation remain necessary; S3 DeleteObjects also continues after individual failures (`s3_delete_objects.c:360`).
- S3 PUT consumes/decodes HTTP input and releases write buffers (`s3_put.c:209,275,310`). Whole-request retry requires retained or spooled replayable payload and resettable progress. S3 GET publishes headers/body before all reads finish (`s3_get.c:85,199,230`); retryable whole-request transactions require deferred/spooled output, or a backend-supported snapshot/streaming contract guaranteeing no late retry after publication begins. Callback purity alone cannot solve streaming.
- Proposed validation: finish EAGAIN with all op statuses OK; OPEN sees existing then absent on retry; checks and operations use identical resolved FH; failed admission prevents truncate; multiple variable-size READDIR outputs reset correctly and handles release once; suspension resumes the same compound and ordinary operation failure preserves the accepted prefix.

These additions are source-review/design conclusions, not newly executed tests. Source line references refer to archived b54b6b32, not the main checkout.


## Authorized implementation follow-up (2026-09-22)

The user subsequently explicitly requested three parallel implementation agents:
mechanical NFS3/FUSE/SDK completion cleanup, OPEN/op-callout contract work, and
S3 conversion with bounded transfer compounds. Implementation and tests are now
authorized. The active checkout is `compounds-refinement`, based on reviewed
`b54b6b32`; changes are uncommitted. Do not work in the archived review tree.
Submodules were clean and aligned to this revision. No merge/commit authorized.

Work in progress/completed before final validation:

- Flattened straightforward completion adapters in NFS3, FUSE and SDK; retained
  complex/shared callbacks and public SDK signatures.
- VFS per-op prepare/complete callbacks, skip and binding, immutable submitted
  inputs, resettable dynamic suffixes, explicit retry, stable growable operation
  blocks, synchronous-completion trampoline, independent GETHANDLE references,
  separate OPEN input/output attributes, 64-bit write counts, S3 composite ops.
- NFSv4.1 fresh-owner, nondelegated/nonexclusive OPEN can include supported
  dependent GETATTR/GETFH/current-stateid READ/WRITE/COMMIT operations. State is
  reserved unpublished before submission, share admission and conditional
  truncate are inside the compound, publication is after accepted finish.
  v4.0, existing/coalesced owners, delegations, exclusive OPEN, second OPEN and
  CLOSE retain fallback boundaries. This does not provide request-wide
  transactions for all NFSv4 compounds.
- S3 filesystem handlers now use compounds, including copy/multipart,
  ACL/tagging, listing, bucket operations, and DeleteObjects. GET/PUT small
  object threshold uses configured io_size, defaults to128KiB, caps at1MiB.
  Larger transfers retain handles and immutable current-chunk buffers through
  acceptance and submit bounded subsequent compounds. HTTP input decoding and
  output emission are outside replayable per-op callouts.
- Integration testing exposed/fixed max1000 DeleteObjects synchronous stack
  overflow; named OPEN failing to install current handle on Linux/cairn;
  lost backing-bucket validation after dispatcher lookup removal; missing
  BucketNotEmpty serialization; CompleteMultipart/Abort double drop.
- Cross-review exposed borrowed CLOSE releasing before retry, ACL snapshots
  dropping access-control data, and ambiguity between op errors and backend
  finish rejection. These are being fixed with owned ACL snapshots, accepted
  CLOSE teardown, and separate finish status/adapter. Do not claim validation
  of those changes until final test results are recorded.

Build is `/tmp/chimera-compounds-build` (Debug+ASan), with pinned submodules,
Python `/opt/s3-tests/.venv/bin/python`, `SPECS_BUNDLE=NONE`. Use `ctest -C extended`
to include integration tests. Host netns/FUSE tests require sandbox escalation.
KVM image tests are unavailable because `oras` is absent. Backend transaction
implementation remains deferred; retry tests cannot prove real backend rollback.

Intermediate evidence: all224 SDK tests and8 FUSE tests passed. Full S3 V4
50/51 passed, with new bucket test exposing missing XML error serialization
(now fixed, rerun pending). NFS4.1 combined passed. NFS4.0 combined exposed
VERIFY/SETATTR validation regressions (fixes pending validation). Final counts
and remaining limitations will be recorded in the refinement report.

Known large-S3-GET transport limit: after earlier chunk bytes/headers are sent,
a permanent later read failure cannot revise the response and libevpl exposes
no public reset/abort operation. First-chunk failure is withheld before headers.
Original review items outside this follow-up (e.g. complete SMB conversion,
FUSE READDIRPLUS shared grant publication) must not be assumed resolved.


## Implementation result and final evidence

Three workstreams implemented on `compounds-refinement`; all changes remain
uncommitted. Main report: [compound-refinement.md](docs/reviews/compound-refinement.md).
Detailed S3 boundary/lifetime note:
[s3-compound-conversion.md](docs/reviews/s3-compound-conversion.md).

The previously pending cross-review fixes landed: independent GETHANDLE refs,
borrowed CLOSE consumed only at accepted teardown, owned primary ACL snapshots
(and NFS3 ACCESS lifetime ordering), explicit backend-finish status overriding
operation status, immutable inputs and resettable attempt buffers. Tests inject
finish EAGAIN both after all operations succeed and after a failing-op prefix;
no resource publication occurs before accepted retry. Provisional share denial
is tested to precede truncate. Real backend transactions/rollback remain absent.

S3 has no direct filesystem execution calls remaining (only compound APIs,
handle release, module capability inspection and root FH initialization).
Small GET/PUT bound is128KiB default, configured io_size capped1MiB. Failed named
temporary uploads use independent cleanup compounds; malformed300KB streaming
input tests check no orphan. Trailing-slash compatibility needed explicit
path-and-leaf removal so batch delete removes marker leaves while preserving
children;1000marker batch fits one compound. DeleteBucket returns409
BucketNotEmpty; the obsolete model deviation was retired.

Supported NFSv4.1 fresh-owner OPEN now spans LOOKUP/OPEN/GETFH/WRITE in one VFS
submission, verified with pynfs CSID3 and trace (`wire first=3 count=4 vfs_ops=8
reserved_open=1`). CLOSE still falls back. Owner/client/publication lifetimes
have reservation tests. Final narrow guards address current-FH/stateid
association, cursor-moving authorization fallback, late-LOCKT range mutations,
and READ response space accounting. Broader OPEN/private-state conversion
(v4.0, coalescing, exclusive, delegations, multiple OPENs, CLOSE) remains deferred.

Final broad Debug+ASan ctest batch:316selected,314passed,0failed,2skipped. Coverage:
224SDK,8FUSE,57botoS3,Ceph214internal cases,S3model probe,13NFSunit,3pynfs suites,
4VFSunit,3NFS3model probes. Linux/io_uring NFS3model probes skipped because scratch
filesystem lacks name_to_handle_at; KVM not run (oras absent). Pynfs internal
passing counts511(v4.0),137(v4.1),152(v4.2), plus configured skips. Logs:
`/tmp/chimera-compounds-final-tests.log`, `/tmp/chimera-compounds-open-mapping.log`.
The final targeted20test NFS/core rerun after the last narrow guards passed20/20,
including all three pynfs suites. Log:`/tmp/chimera-compounds-final-nfs.log`.
Final build and git diff --check passed. Implementation/testing work is complete
for this authorized pass; follow-up boundaries remain as listed below.

Do not infer full merge readiness or all-frontends retry safety from these
passes: SMB/REST conversion, FUSE READDIRPLUS early shared grants, broad NFS
compound fallback boundaries, backend rollback and late largeGET transport
abort remain follow-up issues. Preserve original review as historical evidence;
findings addressed here are not necessarily still open in the edited tree.


## NFSv4 adherence follow-up review

The user asked which remaining compound boundaries are rectifiable. Read-only
review recorded in [nfs4-compound-adherence-followup.md](docs/reviews/nfs4-compound-adherence-followup.md).
Main conclusion: still supported-run grouping, not request-wide ownership.
Remaining avoidable boundaries include build-time stateid/FH authorization,
one reserved OPEN with narrow suffix, legacy CLOSE/downgrade/locks and coalescing,
LOCKT decision at result filling, ACL input/encoding exclusions, omitted
READ_PLUS/COPY/CLONE, namespace/export/saved-state limitations, SECINFO cutoff,
128-op encoder budget vs1024 VFS capacity and validation-error fallback.
Delegation/pNFS waits need explicit asynchronous logical-request coordination;
pure callbacks must not send recalls. Legacy OPEN still performs fallible state
installation after VFS finish and may truncate after compound disposal, so it
is not transaction-ready. First priority: per-op NFS status checkpoints and
execution-time private FH/stateid context/authorization, then broader provisional
state handling. Protocol admission/replay envelope need not become backend ops.
No code changes or new tests in this review; add boundary assertions to future
tests because wire correctness alone can pass through fallback.

## NFSv4 second implementation pass (2026-09-22)

Completed the user's follow-up implementation request. Updated the adherence
review with a second-pass section; the original inventory above it is historical.

- READ/WRITE, size SETATTR, and range-operation stateid/FH authorization now runs
  in prepare callbacks against the VFS execution cursor. Repeated size SETATTR
  is supported. Current/saved stateids remain attempt-private and reset on retry.
- A supported reserved fresh-owner OPEN additionally admits SETATTR, SAVEFH,
  RESTOREFH, ALLOCATE, DEALLOCATE, SEEK, WRITE_SAME. Multiple/coalesced/v4.0/
  exclusive/delegated OPENs and CLOSE still require broader reservations.
- Session LOCKT probes at execution, snapshots denial details, and vetoes later
  mutations. Removed may_fail_late cutoffs. v4.0 LOCKT retains legacy clientid
  validation/lease renewal pending an explicit pin/publication design.
- ACL VERIFY/NVERIFY stays in compounds; dynamically sized scratch fixes large
  ACL comparisons. ACL GETATTR reply budgeting and ACL inputs remain guarded.
- VFS exposes synchronous current-FH inspection and independent prepare/complete
  callback contexts. A new finish-EAGAIN test checks both across two attempts.
- Fixed NFS completion asserting when VFS submission fails before any operation
  runs. Reviewed cleanup/error mapping; NFS allocation failure was not injected.

Added repository-owned wire tests with debug-tag submission counting:14 v4.1
and19 v4.2 cases, all confirm one VFS compound while checking replies/content.
Final Debug+ASan build and318-test CTest run:316passed,0failed,2skipped
(same Linux/io_uring NFS3 name_to_handle_at limitation). SDK/FUSE/S3/NFS/VFS all
covered; all3pynfs suites passed. Final logs:
`/tmp/chimera-compounds-followup-build.log`,
`/tmp/chimera-compounds-followup-final-tests.log`.
Formatting/diff and shell/Python syntax checks passed. Changes remain uncommitted.

Do not claim request-wide NFS conversion: general protocol-state publication,
cross-export credentials, asynchronous delegation/pNFS, READ_PLUS/COPY/CLONE,
reply-budget work, and real backend transaction rollback remain outstanding.

## NFSv4 third implementation pass (2026-09-22)

User requested another larger pass. Implemented READ_PLUS, COPY/CLONE, and ACL
inputs/GETATTR. Report: [nfs4-compound-third-pass.md](docs/reviews/nfs4-compound-third-pass.md).
Earlier review inventories are historical; these operations are no longer
blanket conversion gaps in the edited tree.

- READ_PLUS: type gate, extent classifier, conditional DATA read, then suffix in
  one VFS compound. Read-only classifier; independent outputs; publication after
  accepted finish. Existing delegation/DS/reply guards remain.
- COPY/CLONE: execution-time saved-source/current-destination authorization,
  private endpoint refs, source-size checks, restore destination cursor before
  transfer. Anonymous COPY owns both OPEN results and asks explicit READ_ONLY/
  WRITE_ONLY rights. Failed source inspection preserves protocol destinationFH.
  Multiple transfers/cursor switches work. Special CLONE, inherited savedFH,
  cross-export, reservedOPEN suffix, and identified NFS-proxyCOPY remain fallback.
- ACL SETATTR/CREATE/OPEN inputs are per-map immutable allocations; VFS owns a
  writable ACL copy each attempt. ACL GETATTR stages replies with conservative
  actual-snapshot space checks before successor mutation; retry rewinds staging.
- Generic COPY fallback preserves endpoint claim actors, streams cross-module
  copies, uses trampoline for synchronous chunks, rejects zero/short writes,
  retains callback-scoped ACLs. Original public copy API remains wrapper.
  NFS proxy's consuming WRITE-buffer contract remains excluded from fallback.

Review/testing caught and fixed anonymous endpoint use of cursor-only
OPEN_CURRENT, missing anonymous destination WRITE_ONLY, and failure cursor
publication. ACL budget fixture adjusted200->180ACEs because reserved suffix
space correctly rejected200 atsecondGETATTR. COPY stress fixture now measures
actual frame address rather than an ASan-relocated local variable.

Validation: Debug+ASan build passed;18v4.1 +42v4.2 wire cases all passed with
exactlyoneVFSsubmission assertions. Broad319test run passed all except new
stack-measurement assertion; that test-only correction passed final focused
clone_range_test rerun. Combined final outcomes317passed,2skipped (same Linux/
io_uring NFS3 name_to_handle_at scratch limitation). All3pynfs suites,224SDK,
8FUSE,57botoS3,CephS3,modelprobes,NFS/VFSunits included. VFS tests exercise ACL
input mutation across finishEAGAIN, actor async lifetime, cross-module fallback,
short writes,256synchronous chunks and short-lived backend ACL results.
Logs: `/tmp/chimera-compounds-third-build.log`,
`/tmp/chimera-compounds-third-final-tests.log`,
`/tmp/chimera-compounds-copy-final.log`. Syntax/format/diff checks passed.
No production changes after broad run. All changes remain uncommitted.

Still not request-wide NFS conversion: CLOSE/downgrade/locks, multiple/coalesced/
v4.0/exclusive/delegated OPEN, namespace/export credentials and asynchronous
delegation/pNFS need broader private protocol-state machinery. Real backend
transaction integration and rollback are deferred, not validated here.


## NFSv4 fourth implementation pass (2026-09-22)

User accepted per-operation provisional protocol state, starting with CLOSE
and multiple OPENs. Implemented in the uncommitted compounds-refinement tree.
Latest report: [nfs4-compound-fourth-pass.md](docs/reviews/nfs4-compound-fourth-pass.md).
This supersedes earlier blanket multiple-OPEN/CLOSE and reserved-OPEN suffix gaps.

- Each fresh-owner v4.1/v4.2 OPEN has private state/handle/claim/publication fields.
  Removed single-OPEN suffix whitelist; private stateids work in I/O and range
  endpoints, with existing namespace/export/operation guards preserved.
- CLOSE is a generic ordered VFS CHECKPOINT. Existing public CLOSE resources
  are reserved before submission and retained across retry. Callback only
  validates FH/stateid and updates private closed-state view. Earlier I/O can
  use a pre-reserved public state until CLOSE executes. Public destruction and
  lease renewal happen only after accepted finish. OPEN closed within an
  attempt is never published. Unreached/rejected CLOSE reservations abort.
- Closed identity tombstones reject explicit/current/saved uses. Successful
  CLOSE persists on ordinary later-op error; finish rejection resets private
  state without publication. Claim admission excludes pending closes only for
  this attempt's new OPEN probes, preserving other requests' public view.
- PUTFH unlinked-file liveness includes provisional OPENs and excludes pending
  closes. This was a review-found integration bug fixed before final tests.
- Session state lookup uses no-renewal API; accepted completion touches lease.
  OPEN/CLOSE cleanup retains client pin while releasing state/claims/handles
  outside protocol locks. Public CLOSE reservation was deliberately moved out
  of execution callback to preserve the strict pure-callout contract.

Final Debug+ASan build passed, no warnings. All four wire variants passed:
35v4.1+59v4.2 distinct cases, each asserts one VFS submission; retry variants
injected+accepted23/30 read-only finish conflicts including multipleOPEN and
execution-error prefix. Broad321 CTest entries:319pass,2skip,0fail in26.92sec.
All3pynfs,224SDK,8FUSE,57botoS3,Ceph/model,NFS/VFSunits included. Same NFS3
Linux/io_uring name_to_handle_at scratch limitation causes skips. Claim test
157assertions (11new); lifecycle tests cover validation/contention/abort/accept,
preaccept lease purity and cleanup/deferreddestroy. Syntax/format/diff clean.
Logs: /tmp/chimera-compounds-fourth-build.log,
/tmp/chimera-compounds-close-lifetime.log,
/tmp/chimera-compounds-fourth-final-tests.log.
No production changes after broad run; nothing committed or merged.

Remaining: same-owner/coalesced/exclusive/v4.0/delegated OPEN; existing CLOSE
with childlocks or repeatedowner reservation; downgrade/LOCK/LOCKU; anonymous
I/O after CLOSE (implicitclaim view boundary); inherited savedFH/namespace/
export credentials/SECINFO/capacity/reply and asyncdelegation/pNFS/proxyCOPY.
v4.0 lookup lease touches and legacyOPEN fallible postfinish tail still need
strict retry-contract refinement. Backend begin/end/rollback is deferred;
read-only interposer demonstrates frontend publication safety, not rollback.


## NFSv4 fifth implementation pass (2026-09-22)

User said to keep going with remaining boundaries. Completed another pass;
latest report: [nfs4-compound-fifth-pass.md](docs/reviews/nfs4-compound-fifth-pass.md).
This supersedes fourth-pass blanket anonymous READ/WRITE/COPY-after-CLOSE,
repeated-public-owner CLOSE, inherited-saved-FH, and SECINFO-end-run boundaries.

- Anonymous READ/WRITE and NFS READ_PLUS classifier+READ carry independent
  VFS I/O admission views. Pending CLOSE claims are excluded only for that
  operation; no fake owner bypass. Unrelated NFS/SMB claims remain enforced,
  shared implicit claims retain no borrowed exclusions, and later requests
  test their own needed rights rather than inheriting cached write rights.
- COPY carries separate source/dest views; anonymous copies use bounded
  read/write fallback even on native-capable backends because native COPY has
  no admission gate. Fully owned no-exclusion copies stay native. This costs
  anonymous SDK/S3 performance. Updated three SMB COPYCHUNK/offload fallback
  callers to pass their existing endpoint actors, preserving owned behavior.
- NFS anonymous authorization includes live unpublished OPEN deny reservations
  as well as public states minus private closes. Review caught and fixed
  anonymous READ/COPY/sizeSETATTR bypass of unpublished denies; now LOCKED
  stops execution before suffix mutation.
- Multiple different public states under one owner can reserve CLOSE with the
  same non-null group cookie (ctx). Owner group/count survives freeing any
  first state; other groups/legacyOPEN excluded. Pre-submission reservation,
  retained-across-retry ownership, independent accept/abort preserved.
- Same-export inherited savedFH imported through hidden cursor seeds, mapped
  correctly to first operation. RESTORE/COPY/CLONE/LINK/RENAME can use it;
  RESTORE can start with no currentFH after SECINFO. Cross-export stays guarded.
- SECINFO execution consumes private logicalcurrentFH. Subsequent FH users
  fail NOFILEHANDLE before FS work; PUTFH/RESTORE may recover within same run.
  Reset/reply publication handle the logicalcursor correctly.

Final build Debug+ASan passed, no warnings. Four wire variants allpassed:
48v4.1+77v4.2 cases; measured one-run sequences and exact two-run TEST_STATEID
fallback/inheritedsuffix cases. Finishretry32/42 readonlyconflicts accepted.
Setup-only OPEN afteranonREAD_PLUS mayreceive transientDELAY forimplicitclaim
cachebreak; bounded3s retry confirmedresolution and forbiddenonmeasuredtags.
Focused VFS4/4, state2/2, SMBFSCTL+twoIOCTL suites passed. Final broad326:
324passed,2skipped,0failed in26.45sec. All3pynfs/224SDK/8FUSE/57botoS3/Ceph/
model/NFS/VFS plus5SMBcompat entries. Same name_to_handle_at NFS3probe skips.
Final log /tmp/chimera-compounds-fifth-final-tests.log;
wire /tmp/chimera-compounds-fifth-final-boundaries.log;
VFS /tmp/chimera-anonymous-view-final-ctest.log;
state /tmp/chimera-compounds-close-group-tests.log;
SMB /tmp/chimera-smb-copy-identity-ioctl.log. Syntax/format/diff clean.
No production changes after broad run. All edits remain uncommitted/unmerged.

Remaining: same-owner/coalesced/exclusive/v4.0/delegated OPEN; downgrade/LOCK/
LOCKU and CLOSEchildlocks; identical-state duplicateCLOSE prescan split;
anonymous sizeSETATTR/other rangeops afterCLOSE VFSadmission plumbing;
cross-export/namespace/capacity/reply/asynchronousdelegation/pNFS/proxyCOPY.
v4.0 callbacklease touches and legacyOPEN falliblepostfinish still need universal
retry-contract work. Backendtransactions/rollback remain deferred, KVMnotrun.


## NFSv4 sixth implementation pass: coalesced OPEN (2026-09-22)

User explicitly requested coalesced OPEN. Completed with the existing three
parallel agents: state/owner reservation, VFS claim/grant support, wire tests;
root owns adapter integration. Latest report:
[nfs4-compound-sixth-pass.md](docs/reviews/nfs4-compound-sixth-pass.md).
This supersedes the fifth-pass blanket same-owner/coalesced OPEN boundary.

- Ordinary nonexclusive/nondelegated v4.1/v4.2 OPENs reserve owners before
  submission, freeze existing states and preallocate candidate slots. A private
  journal represents existing and newly opened states throughout execution.
  Multiple same-owner/same-file OPENs retain identity, combine access/deny and
  correlated OPEN_DOWNGRADE history, and snapshot one seqid per successful OPEN.
- OPEN checks requested access/self/peer conflicts, acquires a distinct
  map-owned union share claim, conditionally obtains a union-capable handle,
  truncates only after admission, then commits its private journal checkpoint.
  Failed suffix OPENs cannot overwrite the successful prefix's state.
- CLOSE updates the journal, excludes every intermediate/public claim and
  preserves identity tombstones. Same-owner CLOSE/reopen creates a new identity.
  Accepted finish publishes final journal states once; initial closes precede
  replacement candidates. Handle refs are duplicated and final reservations
  transferred before compound cleanup. Retry rebuilds frozen original state.
- VFS claim_move_replace atomically relocates accepted union reservations into
  stable state-embedded claims, clearing borrowed exclusions without callbacks
  or a release/reacquire gap. Claims used by CP ops remain separate from target
  claims, avoiding cleanup releasing a newly published claim at an aliased address.
- Optional unnamed OPEN inherited_grant_handle and inherited_grant_handle2
  retain old and newly authorized OPEN-bound permissions. Both source FHs must
  match; unbound lazy permissions cannot become retained grants. Initial review
  caught misplaced source2 validation, fixed with an explicit regression.
- Public state mutators reject reserved slots; active borrowers, child locks,
  named streams, competing groups and journal limits conservatively fall back
  before execution. Group/client pins prevent expiry while resources are held.

Final Debug+ASan build passed without warnings; four wire variants passed:
64v4.1 +93v4.2 cases normal/finishretry, 16newcases each. Exact one-submit
coalescing assertions and targeted two-OPEN retry seqid publication passed.
Retry harness now uses monotonic fixture IDs because asynchronous debug logging
can reorder around stderr and compound addresses are reused. Readonly filesystem
eligibility remains mandatory; this is not a backend rollback demonstration.
Claim test175 assertions (18new); compound inheritedgrant tests; state2/2 pass.
Independent reviews found no further adapter lifetime/retry/prefix/cursor defect.
Broad326:324pass,2skip,0fail in26.73s, same suites and name_to_handle_at skips
as fifth pass. Syntax/format/diff clean. No production edits after broad run.
Final logs /tmp/chimera-compounds-sixth-final-{build,boundaries,tests}.log;
/tmp/chimera-compounds-owner-journal-tests.log;
/tmp/chimera-coalesced-claim-tests.log; /tmp/chimera-open-grant-tests.log.

Remaining: v4.0 replay/exclusive/delegated OPEN; OPEN_DOWNGRADE/LOCK/LOCKU and
child-lock CLOSE; busy/bounded owner reservation fallback; prior anonymous
sizeSETATTR/range-after-CLOSE admission plumbing; cross-export/namespace/capacity/
reply/asyncdelegation/pNFS/proxyCOPY boundaries. Remote NFS union OPEN can recheck
remote permissions. Backendtransactions/rollback deferred; KVMnotrun.
Everything remains uncommitted and unmerged. Root adapter pre-sixth backup:
/tmp/nfs4-compound-before-coalesce.c (for incremental review only, do not restore).

Final state-agent source audit: accepted fresh candidate publication still uses
uthash HASH_ADD, which may allocate table/buckets or resize. Actual compilation
uses malloc, HASH_NONFATAL_OOM=0, uthash_fatal=exit(-1), same as the preexisting
fresh OPEN publication path. Thus publication has no recoverable/retryable error,
not an allocation-free/OOM-immune guarantee. Report records this existing limit;
preallocated hash storage/growth (including last-CLOSE/reopen) remains refinement.


## NFSv4 seventh implementation pass (2026-09-23)

User authorized the next pass. Ordinary v4.1/v4.2 OPEN_DOWNGRADE, TEST_STATEID,
IO_ADVISE and SECINFO_NO_NAME now execute in VFS compounds. Latest report:
[nfs4-compound-seventh-pass.md](docs/reviews/nfs4-compound-seventh-pass.md).
This supersedes sixth-pass ordinary downgrade and these checkpoint boundaries.

- OPEN_DOWNGRADE shares reserved-owner journals, including zero-candidate groups
  without any OPEN. Checks access/deny subset and correlated event history,
  reserves narrowed claim against pinned handle, commits private history/seqid,
  preserves each response snapshot and accepted prefix. Deduplicated claim views
  exclude superseded public/intermediate claims but enforce unrelated actors.
- VFS RESERVE_HANDLE allows independent caller-pinned target across retry.
  Deferred claim replacement admits narrowing without release/reacquire gaps;
  waiter processing follows publication after protocol locks are released.
- TEST_STATEID stages per-element execution-time results without requiring FH;
  special IDs are BAD_STATEID, never CURRENT-substituted. Private OPEN/downgrade/
  CLOSE effects resolve through journal; no-FH and post-SECINFO checkpoints work.
- IO_ADVISE uses pure public OPEN/LOCK snapshot validation or private journal;
  no hints honored. Public TEST/ADVISE helpers use shard locks and atomic stateid
  seqids, no lease/ref mutation or final-reference cleanup. Owner replay seqids
  unchanged. An initial acquire/release helper was replaced after review found
  concurrent CLOSE could make release trigger cleanup inside the pure callout.
- SECINFO_NO_NAME consumes logical FH; PUTFH/RESTORE recover inside same run.
  Existing export policy retained; synthetic pseudo-root remains guarded.
- IMPORTANT correction to sixth-pass assumption: RFC8881 section8.2.3 CURRENT
  substitutes seqidZERO for I/O/advice/range/sizeSETATTR, including afterRESTOREFH;
  CLOSE and OPEN_DOWNGRADE retain saved seqid. Explicit old/future nonzero SIDs
  require validation (section8.2.4). Local copies avoid RPC input mutation.
  Tests changed savedCURRENT READ from OLD toOK and added savedCURRENT downgrade
  OLD check. Public I/O had skipped version checks for4.1+, now fixed; public
  sizeSETATTR also gains version/principal/peer-deny checks. Legacy paths still
  need consistency review; do not generalize converted guarantees to fallback.
- Refused compound construction now restores original dbuf mark after disposal.
  Test4000IDs/8leadingPUTFH/malformedOWNER exercises repeated discarded builds
  with bounded input/output, full successful TEST prefix, no suffix mutation.

Initial finalbuild Debug+ASan passed without warnings. Broad326:324pass,2skip,
0fail in27.12s (same suites/name_to_handle_at skips as sixth). Expanded4wire
variants90v4.1/127v4.2 allpassed, including4000-ID refusal regression. Purehelper
tests passed, claim188assertions. Targeted existingOPEN/DOWNGRADE/READ fixture
mustrejectonce+accept, independent of other injected cases. Filesystem-read-only
restriction still means no backend rollback claim.

Late valid6000-ID probe found legacy TEST_STATEID allocates status array without
retaining transport iovec headroom in shared128KiB decode/encode arena; send_reply
then fatals. Fixed legacy allocation to preserve aligned headroom, at least8192
and260*sizeof(evpl_iovec). REP_TOO_BIG/WRITE-suffix regression passes. Raw prior log:
/tmp/chimera-compound-wire.qbdSIC/chimera.log. No user data affected (isolatedmemfs).

FINAL afterheadroomfix: fullDebug+ASanbuild passed, nowarnings. All4wirevariants
91v4.1/128v4.2 pass (normal+finishretry). Broad326rerun324pass/2expectedskip/0fail
in27.04sec, includesnewregressions. Python/shellsyntax/diffchecks clean. No
productionedits afterfinalrun. Logs /tmp/chimera-compounds-seventh-headroom-
{build,wire,tests}.log; supportinghelpers /tmp/chimera-downgrade-state-tests.log
and /tmp/chimera-advise-state-tests.log. Allchangesremainuncommitted/unmerged.

Remaining: v4.0 replay/lease behavior, exclusive/delegated/reclaimOPEN, LOCK/LOCKU,
childlockCLOSE, busy/namedstream/boundedowner fallback, namespaces/cross-export,
anonymoussizeSETATTR/rangeafterCLOSE/CLONE, asyncdelegation/pNFS/proxyCOPY,
reply/capacity, legacyCURRENT/validationconsistency, freshpublicationfatalOOM.
Backendbegin/end/transactions/rollback deferred; KVMnotrun. Alluncommitted.


## NFSv4 eighth implementation pass: LOCK and LOCKU (2026-09-23)

User requested LOCK/LOCKU. Ordinary v4.1/v4.2 new/existing lock owners now run
inside VFS compounds, including OPEN→LOCK→LOCKU→READ/TEST_STATEID. Latest report:
[nfs4-compound-eighth-pass.md](docs/reviews/nfs4-compound-eighth-pass.md).
This supersedes seventh-pass blanket LOCK/LOCKU boundary. Root owns adapter,
callback_contract state/journal helpers, open_callouts VFS + completed wiretests.
Frontend_fit partially added tests before repeated failed agent turns; its edits
were retained and finished by open_callouts. Everything remains uncommitted.

- Lock-capable parent OPEN-owner reservations freeze child LOCK slots and pin
  their owners. Lock-owner reservations preallocate candidate slot identities;
  all existing parents for that lock owner must already belong to this cookie.
  Busy borrowers/groups and unsupported shape cause construction fallback.
- LOCK validates FH/client/principal/version/access/recovery/interval in a pure
  checkpoint, admits map-owned RANGE via VFS RESERVE_HANDLE, then commits private
  interval geometry and seqid. LOCKU splits/removes/no-ops in the private journal.
  Storage and eventual public range-lease nodes preallocated before execution.
- Original and intermediate claims stay physical until accepted finish. RANGE
  RESERVE and LOCKT combine pointer exclusions with normalized private overlays,
  preserving residual intervals and other private owners' conflicts. I/O, TEST
  and IO_ADVISE resolve private LOCK versions/parents; CURRENT uses localcopies.
- Accepted publication installs OPENparents first, then atomically replaces each
  RANGE set under file lock with pre-admitted coverage validation. Protocol
  state/seqid/list publication and waiter processing follow; callbacks are pumped
  outside protocol locks. No release/reacquire gap. Finish rejection resets from
  frozen originals; accepted errors publish only successful dirty prefix.
- Dispose CPclaims beforejournalstorage, lockgroups beforeparentgroups. Tested
  deferred clientdestroy across freshparent+child accepted publication. Partial
  and toEOF ranges use128bit exclusiveends to includeUINT64_MAX finalbyte.
- Pure localrange policy query preserves current NFSv4 behavior: backend range
  projection already excludes nonPOSIXowners. Existing projected tokens cannot
  enter journal. Backendtransactions/projection remain separatework.
- Legacy LOCK now refuses busyreservedlockowners; RELEASE_LOCKOWNER similarly
  returnsDELAY. FREE_STATEID/LOCKU acquisitions are blocked by reservationgate.
  Final rootreview found release-before-destroy gaps at4new-state LOCK error
  tails: reverse to destroywhileborrowheld thenrelease. Also retainparentborrow
  untilnewchildacquire succeeds, avoiding constructionfreeze between newchild
  publication anditsfirstacquire. Finalrerun afterthesechanges passedbelow.

Validation beforefinallegacyorderingfix: Debug+ASanfullbuild; VFS2/2 and209claim
assertions; state2/2 including3newreservation/range/deferreddestroytests. Fourwire
variants109v4.1/146v4.2 pass normal+finishretry (18newcases each). Specific
OPEN→LOCK→LOCKU two-read fixture requiresonefinishreject+accept; readonlyonly.
Broad326324pass/2expectedname_to_handle_at skips/0fail in27.22sec. Independent
adapterreview found noadditional retry/prefix/alias/lifetime issue. Finalbuild
afterlegacyorderingfix passedwithoutwarnings. FINALbroad rerun326:
324passed/2sameexpectedskips/0failed in27.11sec, including109/146wirecases
normal+finishretry. All3pynfs/224SDK/8FUSE/57botoS3/Ceph/model/NFS/VFS/5SMB
included. Syntax/format/diff clean. No productioneditsafterfinalrun. Logs:
/tmp/chimera-compounds-eighth-final-{build,tests}.log,
/tmp/chimera-compounds-eighth-wire{,-detail}.log,
/tmp/chimera-compounds-eighth-vfs-tests.log,
/tmp/chimera-lock-state-ctest.log. Everythinguncommitted/unmerged.

Remaining lockboundaries: v4.0/reclaim; childlockCLOSE and CLOSE/DOWNGRADE after
LOCK/LOCKU; lockownerotherparents notfrozen byrequest; busy/bounded states,
>256originalintervals/journal, >NFS4_COMPOUND_OWNER_MAX_STATES(128)modifications,
ambiguouspreexistingmixedmodeoverlaps, backendprojection/asyncblocking behavior.
The old namespace/export/delegation/pNFS/anonymousrange/legacyvalidation/fatalOOM
publication limits remain. No backendrollbackclaim; KVMnotrun. Adapterbackup
/tmp/nfs4-compound-before-locks.c isforreviewonly, donotrestore.

## NFSv4 ninth implementation pass: remaining lock boundaries (2026-09-23)

User requested remaining boundaries. Implemented child-lock CLOSE, CLOSE and
OPEN_DOWNGRADE following LOCK/LOCKU, connected sibling-parent reservations for
shared lock owners, and v4.1/v4.2 reclaim LOCK. Supersedes those eighth-pass
remaining-boundary entries. Report:
[nfs4-compound-ninth-pass.md](docs/reviews/nfs4-compound-ninth-pass.md).

- All converted CLOSEs now use owner journals; removed the adapter's separate
  per-state CLOSE reservation path. Parent mutations freeze child journals even
  without explicit LOCK. Private closed parents preserve child tombstones and
  physical claim exclusions, but contribute no live range overlay. New locks
  skip closed-parent children so CLOSE/reopen gets fresh identities.
- Accepted-only nfs_lock_range_journal_retire and VFS range_retire detach old
  and tentative claims without callbacks. Retain owned file references, retire
  before parent teardown/replacement publication, then process waiters after
  protocol publication. Child refs keep lease nodes alive until disposal.
  Closed unpublished children stay unpublished; rejection resets originals.
- nfs_lock_owner_compound_parents copies sibling parent stateids under locks.
  Construction freezes the bounded transitive parent set, following children
  of newly appended OPEN groups, before reserving lock-owner journals. Busy or
  oversized closures still fall back. Parent freeze now rejects an unpublished
  child owner during legacy RELEASE_LOCKOWNER's removal-to-destruction gap.
- Reclaim LOCK gate was already a pure execution callout. Removed v4.1+ scan
  exclusion; OPEN CLAIM_PREVIOUS still guarded. Recovery gate repeated checks
  cover grace/eligibility/completion without mutations; wire only tests reclaim
  outside grace, not successful restart recovery end to end.

Unfixed v4.0 audit findings: replay cache cannot retain LOCK DENIED owner/range/
type; replay branches copy success stateid even on denial. LOCKU OLD_STATEID/
INVAL early tails skip seqid consumption despite shared advancement classifier.
Legacy minor0 LOCK passes NULL recovery client, omitting client-specific checks.
Need typed replay snapshots and attempt-local OPEN/LOCK-owner seqid journals;
new LOCK touches both owners. Must handle consuming error publication and pin
state-derived client before removing minor0 guard. No v4.0 conversion claimed.

Validation: full Debug+ASan build passed; state lifecycle 2/2; VFS claims215
assertions; recovery gate suite passed. All4wirevariants122v4.1/160v4.2 passed,
with one-submit assertions and targeted OPEN/LOCK/held-CLOSE reject+accept.
Broad326:324passed/2expectedname_to_handle_at skips/0failed in27.02s, including
all3pynfs/224SDK/8FUSE/57botoS3/Ceph/model/NFS/VFS/5SMB. Independent reviews found
no additional retirement/disposal/closure defect. Logs:
/tmp/chimera-compounds-ninth-{build,final-tests,wire,wire-detail}.log,
/tmp/chimera-close-children-{tests,claims}.log,
/tmp/chimera-recovery-gate-test.log. Only comment/test-format/documentation edits
followed broad validation. Backend transactions/rollback deferred; retry fixture
filesystem-read-only; KVM not run. All changes uncommitted and unmerged.
Backup /tmp/nfs4-compound-before-child-close.c is for review only, do not restore.
Final incremental build after comment/test formatting passed; individual format,
Python/shell syntax and diff checks passed. Additional build log:
/tmp/chimera-compounds-ninth-final-build.log.

## NFSv4 tenth implementation pass: v4.0 replay and remaining OPEN forms (2026-09-23)

User asked to address last remaining boundaries. Implemented ordinary v4.0
OPEN (fresh/unconfirmed and coalesced), OPEN_CONFIRM, OPEN_DOWNGRADE, CLOSE,
LOCK/LOCKU and LOCKT compound paths. EXCLUSIVE4/EXCLUSIVE4_1 and CLAIM_PREVIOUS
OPEN also coalesce with delegation coordination disabled. Reports:
[tenth pass](docs/reviews/nfs4-compound-tenth-pass.md),
[remaining inventory](docs/reviews/nfs4-compound-tenth-boundaries.md).
These supersede ninth's blanket v4.0/exclusive/reclaim OPEN boundaries.

- Reservation-owned replay journals snapshot/reset owner seqid, typed reply
  and confirmation. Execution classifies owner sequence and op type; successful
  and consuming-error outcomes update private journals. New-owner LOCK may
  update both owners. Accepted completion publishes replay before CLOSE slot
  tombstones. OPEN_CONFIRM also publishes private stateid version. Retry
  resets OPEN and LOCK owner journals plus private state and confirmed flags.
- Typed caches own LOCK DENIED bytes/range/type and nondelegated OPEN cinfo,
  attrset, flags, stateid and output FH. Union keeps OPEN from adding more space
  atop denial cache: cache1096B, slot1120B, lazy64-slot blocks70KiB. No request
  pointers escape. Per-operation OPEN snapshots survive later same-owner ops.
- Replays skip each VFS step in its OWN prepare (future-index op_skip is
  invalid). OPEN replay executes trailing cached-FH PUTFH before successors;
  ordinary OPEN skips that restore. Cached errors stop suffix normally.
  CLOSE tombstones are copied at construction, not re-read on each attempt.
- Exclusive verifier/type check moved before admission/suffix. Reclaim OPEN
  recovery checks moved from scan to pure execution checkpoints, including
  per-client RECLAIM_COMPLETE. Successful grace recovery still not wiretested.
- Client pins span compound lifetime and prevent deferred teardown. LOCKT
  minor0 independently pins named clients (including no session), checks
  invalid client at checkpoint, and renews lease only after accepted execution.
  All map pins release on refused construction; ctx client releases aftergroups.
- CRITICAL review fix: session refs don't retain client_unified. A lookup then
  pin can race client replacement/free. New nfs4_client_reserve_compound does
  lookup+pin under table->client locks; ctx uses immutable session clientid.
  Adapter has no raw client_unified reads. State-derived different-client
  v4.0 ops fall back instead of reporting BAD_STATEID from cached conn client.
- Legacy OPEN entry refuses compound-reserved owners with DELAY before seqid
  classification; otherwise a later borrower could publish an early consuming
  error behind the immutable owner snapshot. Actual-entry test covers this.
- Legacy corrections: full OPEN replay restores FH/cinfo/attrset; full LOCK
  denial replay and wrong-op guards; LOCK/LOCKU OLD_STATEID and LOCKU INVAL
  consume/cache owner seqids; sessionless LOCK recovery uses acquired-state
  client. New test_legacy_lock_replay drives actual LOCKU error/replay paths.

Validation before final atomic-pin fix: six wire variants40v4.0/129v4.1/167v4.2
passed normal+finishretry, injected/accepted40/180/197. Every measured minor0
case checks exact full submitted wire start/count, not merely one submit (old
count-only check could miss legacy tails). New reclaim/exclusive same assertion.
Broad329:327pass/2expectedname_to_handle_at skips/0fail in57.84s. Same broad
suites as ninth plus two v4.0 wire variants and actual legacy LOCKU unit.
Final atomic-pin/lifecycle and formatting build/rerun recorded below on finish.

Remaining: namespace and cross-export credentials; delegation/pNFS async
coordination; proxy/backend range token ownership; some anonymous/range I/O;
sessionless/differently-bound minor0 stateops otherthanLOCKT; input/grace scan
guards; contention/capacity fallbacks. Legacy OPEN_CONFIRM consumed-error/op
replay and legacy LOCK recovery-error ordering still need fallback paritywork.
Shared stateid seqid0 behavior retained (no broad protocolvalidation claim).
Backend begin/end/transactions/rollback deferred; syntheticretry filesystem-
readonly; successful persisted grace recovery and KVM not tested. All changes
uncommitted/unmerged. Adapter review backup /tmp/nfs4-compound-before-tenth.c;
never restore it over work. Logs /tmp/chimera-compounds-tenth-{final-build,
final-wire,final-wire-detail,tests}.log; final logs appended below.

FINAL tenth: Debug+ASan full build passed without warnings after atomic table
client-pin fix; latest focused state_table/legacy_lock_replay/open_owner_lifetime
3/3 pass. Final broad329:327passed/2expectedname_to_handle_at skips/0failed in
27.54sec, including all six wirevariants40/129/167 cases normal+finishretry.
Syntax, formatting and diff checks passed. No production edits after finalrun.
Logs /tmp/chimera-compounds-tenth-final-{build,tests}.log and
/tmp/chimera-v40-confirm-state-tests.log. Everything remains uncommitted.

## NFSv4 eleventh pass in progress: delegation and pNFS (2026-09-23)

User: "Lets try doing the delegation and pNFS paths". Report under construction:
docs/reviews/nfs4-compound-eleventh-pass.md. All changes remain uncommitted.
Three existing agents reused: callback_contract (VFS COORDINATE, layout barriers,
namespace recalls), frontend_fit (pure delegation/combine helpers, callback
lifetime, DS I/O), open_callouts (actual delegation and two-daemon pNFS tests).

- Explicit VFS COORDINATE parks same compound; memo key original op+resolvedFH,
  unique token handles stale/duplicate callback; inline safe; bounded memos;
  no dynamic appended coordination. Pure frontend callbacks remain publication-
  free; explicit coordination can query/recall but not mutate FS/publish reply.
- Session OPEN grants after accepted finish with retained finalization ctx and
  probe continuation; restores terminalFH/index. v40 delegation OPEN retained
  boundary because typed ownercache only NONE. Closed-in-prefix OPEN can decline
  optional grant. Delegation READ/WRITE/READ_PLUS copied actor authorization
  fixes own READ_PLUS self-recall; helper checks FH/client/access/seq/revoked.
- CB_GETATTR memo inputs and per-deleg exclusive private combine journal survive
  retry; reset initialvalues eachattempt; apply into pendingcopy, assign only
  after successful staging; accepted-only publish. Pure identityrecheck avoids
  releasing differentdelegation lastref insidepurecallback. ACL budget extra
  only whenACLrequested; coordinatorallocation mapsRESOURCE eachattempt.
- pNFS SETATTR authorizes beforeCOORDINATE, holds perFH countedbarrier across
  retries/finish; final LAYOUTGET grantsection prevents asyncgrant-after-recall.
  Recall snapshot allholders instead of32 (oldcutoffcouldhang). Namespace
  REMOVE/RENAME use VFSlookups+COORDINATE+capturedGETFH/runtimePUTFH to preserve
  current/savedFH. Failurecompletionrestoresparent/dest evenbeforeinternalrestore.
  Pure caching query uses retained filestate to detectnewholder onretry. Existing
  name-replacement/check-to-mutation races remain, no namespaceatomicreservation.
- DS READ/WRITE/READ_PLUS use original FH/credential authority, not MDSstateid
  table. AllsessionIOclasses admitted; MDS unsupportedclass =>BAD_STATEID in
  pureexecution instead of legacyLAYOUT-as-LOCK cast. pNFS RENAME no longer
  blanketexcluded; legacy hasnoDScleanup. pNFS REMOVE staysboundary for
  accepted-only best-effort backingcleanup.
- Review caught callback lifetime gaps: querywrapper ref can outlive caller
  clientpin; layoutref notclient lifetime; layoutrecall intrusive queue reused
  concurrently; failure callback looked up replacement byFH instead of original.
  frontend_fit finishing bounded memoryref/clientpin/separatequeuectx fixes;
  verify final status before claiming complete.

Pre-final: new VFS coordination tests and delegation_io/layout_barrier2/2 pass.
Normal broad329 passes except expected2skips; retrywrapperinitiallyfailed due
STALE preload ABI after sandbox CMake dropped hosttargets. Hostconfigure+
rebuild injector fixed; existing v40/v41/v42 retrysuites3/3pass. Initialfeature
8delegation exactspans normal+retry pass13injected/accepted, CB_GETATTR once;
pNFS5cases normal+retry pass. Finalfixtures now14delegation,6MDSpNFS+3actualDS
cases including held-layout admissionRECALLCONFLICT. Finalfullbuild+broad335
still pending. Testmanifest /tmp/chimera-eleventh-test-names.txt. Hostconfigure
required if sandboxregeneration loseshosttests; neverrunoverlappingbuilds.

FINAL eleventh (supersedes pending notes above): completed production/lifetime
fixes and finalvalidation. Layoutrefs nowretainclientmemory; logicalteardown
flag plus callbackpins permit recalls evenwhen destructionpending, avoiding
selfwaitcycle. Separatelayoutrecallqueue nodes holdoriginalholder+client;
callbackfailure destroysoriginalonly. GETATTR wrapperduplicatesclientpin until
itsown delegationborrow releasedafterresume. GenericLAYOUTrelease routesput
so newclientmemoryref notleaked. Focusedunit covers retainedlayoutafterclient
teardown, pendingdestroyrecallpin, duplicatepin and genericrelease; LSan2/2pass.

Featuretests FINAL14delegation normal+retry and6MDSpNFS+3DS normal+retry allpass
exactspans. Actualsource/victimrename+remove recalls; DSusesactuallayoutFH and
bothadvertisedtoken/MDSstateid; MDSrejectslayoutSIDBAD_STATEID incompound;
CB_GETATTR exactlyonceacrossretry; heldtruncate parksuntilreturn andnewLAYOUTGET
RECALLCONFLICT. Accepted24delegation/4pNFSinjections; FSmutations excluded.
Fixednamespace COORDINATE EAGAIN=>DELAY (genericmapperwasSERVERFAULT), frontend
allocENOSPC=>RESOURCE, pure namespaceveto=>DELAY; exhaustedfinishEAGAIN=>DELAY.
FinalDebugASanbuild andbroad335:333pass,2expectedname_to_handle_at skips,0fail
29.84s. Sixfocusedstate/VFS/claim/lifetimepass. Formatting/syntax/diffpass;
onlyformat/docsafterbroad, incrementalformattedbuild recordedbelow.
Logs /tmp/chimera-compounds-eleventh-{final-tests,feature-final,unit,leaks,
formatted-build}.log. No KVM needed/run. No commits/merge.

IMPORTANT inherited findings discovered by actualfeaturetest/review, NOTfixed:
1. pNFS LAYOUTGET createsemptyDSbacking foralreadywrittenMDSfile, no data
   migration. DSfixtureexplicitlyseedsDS; cannotclaimMDS/DScoherence.
2. Delegation GETATTR combine (bothHEADlegacyandnewjournal) overwritesbaseline:
   localCHANGE3, repeatedholderCHANGE4/sizeNew ->first4/sizeNew, nexttreats4
   unchanged andreturnsbackend3/sizeOld. Needsseparategrant/reporttracking and
   retaineddirtyattributeview. Retryjournalingdoesnotfixsemanticregression.
Remainingconversion: v40delegatedOPENownerreplayfullreply; delegationclaimOPEN
forms; pNFSREMOVEaccepted-onlybest-effortDScleanup; layoutcontrolhandlers;
namespace/cross-exportcredentials andpriorproxy/rangetoken/legacyparitygaps.
Namespace name-replacement/check-to-mutation race preserved, noatomicreservation.
Backendtransactions mustpermitparkingwithoutblockingpeerreturns anddefine
memoizedcoordinputvalidity. Report docs/reviews/nfs4-compound-eleventh-pass.md.

## Twelfth pass: MBT and correctness cleanup (2026-09-23)

Latest implementation record; supersedes intermediate validation and open-issue
notes above. Changes remain uncommitted on compounds-refinement, including dirty
ext/specs model changes. Full report: [compound correctness cleanup](docs/reviews/compound-correctness-pass.md).

Implemented:
- NFS LOCK and OPEN_CONFIRM legacy admission/replay/consuming errors; v4.0
  SECINFO keeps CFH; ACCESS execute permission; complete owned delegated OPEN
  replay including principal bytes, attributes, cinfo and output FH.
- Native AUTH_UNIX OPEN_AT parent SEARCH gate before lookup/truncate, including
  INFERRED/compound CREATE. Removed NFS3 create-search-permission tolerance.
- Delegated GETATTR accepted-only dirty CHANGE/size projection across return,
  incomplete callback DELAY, bounded non-evicting server-life journal.
- FUSE READDIRPLUS publishes staged nodes/grants only after accepted finish;
  sync TTL zero because existing grant boolean has no incarnation proof.
- Coherent pNFS requires authoritative configured proxy DS backing or native
  layout source. Independent empty DS copies disabled. Both-direction MDS/DS
  write/truncate tests. Full layout identity, epoch/refcount/client lifetime and
  publish/return locking; actual LAYOUTCOMMIT size/mtime reporting.
- Proxy XDR WRITE input ownership, compound READ descriptor lifetime, PATH cursor
  READ/WRITE type checks, CHANGE/cinfo/pre-post attrs, access masks, fixed state
  identity with seq0 for I/O/CLOSE. Safe-prefix DELAY timer retry/cancel/wakeup.
- SMB defer overwrite truncate until share/cache admission; denied base/stream
  overwrite preserves bytes. Durable retirement/reconnect and warm registry
  cleanup; duplicate lease-key namespace preflight; ClientGuid+key identity.
- SMB sole-opener caps count same-client/metadata opens. Size SETATTR actor
  preserves coherent own lease but breaks own legacy LEVEL_II. Fresh grant uses
  retained advertised rights while breaking; rescue ignores own provisional cache.
- SET_INFO rights, SACL privilege, EA allocation/cleanup, accepted-only timestamp
  policy, disposition readonly/nonempty/flag checks, pooled CREATE DOS reset,
  directory WRITE error and admitted self-rename no-op.
- Ordinary SMB deletion retires logical opens atomically, owns/pins pending action
  path/credential/identity, waits last holder; covers CLOSE/tree/transport/durable
  cleanup. Stream helper chains stream and base cleanup and propagates failures.
  Shared DeletePending query and cancellation after original opener close fixed.
- Explicit POSIX disposition unlinks on deleting CLOSE despite peers; backend
  SMB unlink/rmdir requests EX DELETE|POSIX_SEMANTICS then always closes FID,
  preserves errors. Unsupported peer capability returns error, not deferred success.
  Replacement names survive old-peer close; sequential hardlink deletion works.
- Model fixes synchronize same-key lease members, retained ClientGuid, EOF/batch
  break/ACK behavior, post-break grant rights, pending namespace error priority,
  normal close semantics on LOGOFF/tree teardown. Positive deletion/directory
  WRITE/selfrename generator paths restored. No new hiding tolerances.

Validation completed through build20 (overlapping suites; do not sum):
- Corpus754 traces, SMB94 including original0x21 plus8additional0x23 lease traces.
  All selftests/generation/11 coverage gates pass.
- Strict SMB five profiles (plain, signed311, encrypted311, ntlmv2, signed30):
  94/94 each, 470 total, zero mismatches/skips/abandoned/deviations. CD1/CD2/CD4
  obsolete exclusions removed. Finalbuild21 ordinaryregisteredCTest repeats
 all470executions clean,zero diagnosticdeviations/skips/abandonments.
- All14 SMB probes/hardening/Samba pass, including24 concurrent close pairs,
  cancellation/streams/disconnect, POSIX old data/replacement/hardlink checks.
- POSIX over SMB53/53 traces pass; original128-step unlink/recreate regression
  passes. All8S3MBTs and7event/diskfs/FUSEmodels pass. OtherPOSIX/proxy batches
  pass, unavailable host backend suites capability-skip.
- NFS/FUSE66:44pass22hostskip;1729complete trace executions/191138ops/297 trace
  capability stops. NFS4 unique union140/142 memfs/diskfs,132/142cairn. Remaining
 2expectDELAY rather than completed recall; cairn8more READ_PLUS/CLONEunsupported.
- NativeNFS3 SEARCH tolerance removal:7TCP/RDMA/GSS profiles,196traces/29568steps/
 9590attrs,zeroattrskips/capabilitydeclines; enforcement/ACL/compound tests pass.
- pNFS3batches pass:18complete+6xattrstops each,4320compounds total. Accepted
 reconciliations perbackend:symlink0755x22,typeerrorsx15,CREATEpriorityx6.
- Proxy fullmodel+DELAY lifecycle+4backendPIO pass3repeats each. Finishretry
 120parallel executions pass after atomicfixturelogrecord fix.
- Final build21 broad335:333pass2hostfilehandleskip. All16focusedtests pass:
 244claim assertions,36hardening/lifetime assertions,14SMBprobe/Samba tests,
 andPOSIX-over-SMB53trace corpus. Claimfixture corrected from cascadingnamespace
 recall to actualCREATEOVERWRITE WRITEtrigger; productionunchanged.
- Final disposition validation and recall own handles across concurrentCLOSE;
 synthetics copied, cachedrefs duped underretirementlock. Deterministicregression
 verifies syntheticidentity afteroriginalrecycling and cachedref2->1->0.

Still explicit limitations:
- Backend compound transactions/rollback deferred; synthetic finishEAGAIN only.
- LAYOUTCOMMIT GETATTR->SETATTR races concurrent extension; needs atomicmax-size
 backendoperation/sharedall-writer serialization. Sequential tests aren't proof.
- CHANGE projection persists only serverlife, not restart; delegatedv40OPEN
 still legacyprotocolboundary. General namespace admission/mutation atomicity
 needsbackendcontract. SMBpendingcheck/shareadmission and streamverify/remove
 are separate; baseunlink has expectedFH protection.
- SMBpendingintent perinode, not perhardlink: sequential hardlinks okay, independent
 simultaneouslinkdispositions need keyedintents. NumberOfLinks rawbackendcount.
- Modeldoesnotrepresent concurrentcommandsduringbreak orparkedcompoundsuffixes.
 SMBMBTmemfsonly. NFS/POSIXreconciliation registries stillapply, notstrictconformance.
 HostD4-25unlinkedLINK/D4-27timestampverifierlimitations untested capabilityskip.
 POSIXPD3real-dirfd-fstatat/PD22seekerrno/PD25longleaf/PD24fdlimit/PD17errorpriority
 retained. POSIX-SMB SD-DAC skipsmodelDAC-deniedops(rootmountidentity),SD-SETID
 allowsrootretainedset-IDbits,SD-SPECIALallowsEINVALvsENXIO. Do not describe green batches as deviation-free except strictSMB.

Final state:
- Allfive ordinaryregisteredSMBprofile batches pass build21 without include-declined
 override, afterremovingobsoleteCD1/CD2/CD4exclusions:94each/470total,zeroissues.
 Rootadded encrypted311/ntlmv2/signed30CTestregistration toexistingplain/signed311.
- Broad21:333pass2expectedskip. Focused21:16/16pass. Alltestsfinished; no active
 binarytests/builds. No further build needed. Changes remain uncommitted.
- Report finalized in docs/reviews/compound-correctness-pass.md; this memory
 records current findings. Remainingarchitecturalissues above are NOT fixed.
- Rebuild /tmp/chimera-compounds-build only when testsidle. Debug/ASan,
 SPECS_BUNDLE=OFF,SPECS_TRACES_DIR=/tmp/chimera-specs-current/traces,Extended.
 Hostbuild/ctestauthorized;CCACHE_DIR=/tmp/chimera-compounds-ccache,
 ASAN_OPTIONS=detect_leaks=0. ElevenlatesteditedC/Hformatchecks pass individually;
 gitdiff--checkclean. ext/specs changes must be preserved as separate submodulework.
- Final logs /tmp/chimera-correctness-{broad21,focused21,smb-registered21}.log;
 earlier NFS/S3/proxy evidence /tmp/chimera-correctness-*.log;
 NFSaudit /tmp/chimera-nfs-final18-audit.json. Suitesoverlap,don'tsumunique tests.

## Remaining conversion audit (2026-09-23, after correctness cleanup)

User requested a fresh report of what remains compound conversion outside SMB.
Read-only source/dispatch review; no production edits, rebuilds or new tests.
Full report: [remaining frontend conversion](docs/reviews/compound-conversion-remaining.md).
This supersedes any assumption that passing MBTs prove universal adoption.

- NFS3: only6/21 non-NULL procedures converted:GETATTR,ACCESS,READLINK,FSSTAT,
  FSINFO,PATHCONF. Fifteen LIVE direct implementations remain:SETATTR,LOOKUP,
  READ,WRITE,CREATE,MKDIR,SYMLINK,MKNOD,REMOVE,RMDIR,RENAME,LINK,READDIR,
  READDIRPLUS,COMMIT. Preserve WCC/verifiers/guards; stage directory replies and
  retain WRITE inputs until accepted completion.
- SDK public writev/writerv/read_into/readdir direct; internal POSIX-facing
  open_at/mkdir_at/remove_at/lsetattr/getacl direct. POSIX fchmodat/fchownat
  real-dirfd branches and utimensat direct. Public SETATTR/FSETATTR/STATFS ARE
  converted. Uncalled dispatch_setattr_at is dead helper, not live gap.
  SDK readdir invokes arbitrary application callback during execution: stage
  batches before converting; read_into needs explicit destination-buffer contract.
- NFS4 supported-run adapter42opcodes, not guaranteedwire1:1. Remaining:
  configured '/' root export rejects any run containingLOOKUP/LOOKUPP;
  pseudo-root/cross-export/syntheticattrdir and moved-cursorLOOKUPP;
  allv40OPEN when delegationconfigenabled, delegationclaimOPENvariants;
  nonjournalablelegacyOPENtail; broaderdelegation/layoutstateidclasses;
  allpNFS-enabledREMOVE; LAYOUTGET/LAYOUTCOMMIT; OPENATTR/streams;
  NFSproxyCOPY. ProxyCOPYcommentstillcitesWRITEconsumption nowaddressed by
  payloadclonefix: strongcandidateforreevaluation, notblindguardremoval.
  Budgets128estimatedops/replycapacity/reservation/invalidargumentfallbacksremain.
  OrdinaryOPEN/coalescing/CONFIRM/CLOSE/DOWNGRADE/LOCK/LOCKU alreadyintegrated.
- FUSE ordinaryfilesystemhandlersconverted. Lockclaimoperationsdirect andFLUSH
  releasesprocesslocksbeforeCOMMITcompound. NLM/POSIXlockstate similar. These
  needacceptedpublicationcontracts, notsimplecallbackwrapping.
- S3 allfilesystemoperationcallsthroughcompounds; smallGET/PUTsinglecompound,
  default128KiB/max1MiB boundedtransferunit; largeHTTPchunksintentional.
  Server-sideCOPY/UploadPartCopyonechunkpercompound; complete128rangeops or
  onebufferedread. Coalescingpolicychoice, notallmissingconversion.
- NEW HIGH-PRIORITY STATIC FINDING (not fault-reproduced): multipart completion
  choosesMOVE(s3_multipart.c2229), accepts128-opbatches2182/2299, laterfailure
  removesscratch1963 andclearscompleting1986/2011leavinguploadretryable.
  memfsMOVEclears sourceblocks(memfs.c5024). Prioracceptedpartsareconsumed;
  percompoundrollbackcannotrestorethem. Usecopy/clone untilpublication or
  assemblywide rollback. Addlatefailure+retryCompletebyteintegrityregression.
- NEW bounded S3 completenessfinding: metadataLISTXATTRSonlyfirst16KiBpage
  (s3_metadata.c321/419), noeof/cookiecontinuation; unrelatedxattrscanhide
  metadata/tagsbeyondfirstpage. Appendpages insideexistingcompound as taggingdoes.
- SMB actuallyzeroVFScompoundsubmissions; wirecompounddispatcheronly. Needs
  adapterwithSMBcontinue-after-error semantics andprivateIDs/open/lock/notify/
  RDMApublication; stream/layout/overwriteprimitivesasneeded.
- OptionalRESTdebugfsop directunlink/rename/link/chmod; management/bootstrap/
  referencesarenotordinaryconversiongaps.
- OnlyNFS4frontendcurrentlycallscompound_retry onfinishEAGAIN. Commonretry
  policyforothersstillneeded, separatefromcalloutpurity. Backendrequestassociation,
  lifecyclehooks/rollback anddeferredVFScache/notify remainexplicitfuturework.
- RecommendedoutsideSMBorder:S3destructiveMOVEfix; NFS3+SDK/POSIX; NFS4proxyCOPY/
  v40delegOPEN/pNFS; sharedstreams/layout/namespacecredentialprimitives; locking.
  Addspanassertions+finishrejectionpublicationtests, notonlysemanticMBTpasses.


## NFSv3 / SDK-POSIX / multipart completion (2026-09-23)

The user explicitly requested three parallel source-only implementation agents,
then primary-agent builds/tests, followed by an assessment of whether their
assignments were actually completed. All agents obeyed the no-build/no-test
constraint. Primary integrated shared VFS support and performed all validation.

Full record: [compound-nfs3-sdk-multipart-pass.md](docs/reviews/compound-nfs3-sdk-multipart-pass.md).

- NFSv3: all 21 non-NULL filesystem handlers now submit one compound; all 15
  previously missing procedures converted. EXCLUSIVE/guards are operation
  callbacks; READDIR/PLUS reply arena resets on retry. Accepted-only publication,
  eight finish-EAGAIN retries, JUKEBOX exhaustion, and retained WCC snapshots.
- SDK/POSIX: writev/writerv/read_into/readdir, descriptor-relative helpers,
  lsetattr/getacl, real-dirfd chmod/chown and utimensat converted. read_into uses
  private data plus accepted copy (direct RDMA destination placement deferred).
  Application READDIR callbacks see accepted pages only, capped at512 entries.
- S3: destructive multipart MOVE removed; COPY/RW preserve parts through final
  publication failure. Metadata follows list cookies and grows ERANGE lists to
  1MiB while preserving earlier-page results.
- Root corrections after agent handoff: READ ownership call signature and fixture
  watchdog include; explicit live-handle bindings (18 POSIX-over-SMB errno
  mismatches across11 traces fixed); full-path OPEN-at, child-FH REMOVE and result
  mask support; ENAMETOOLONG preservation and construction-error retry retention.
- Final ASan build7 succeeded. Nine new focused tests +534 matrix cases =543
  distinct selections,527passed/16skipped/0failed. Twelve skips need a scratch
  filesystem with name_to_handle_at; four fchownat variants require root creds.
  Logs `/tmp/chimera-conversion-{build7,focused2,matrix2}.log`; detailed immutable
  test copy `/tmp/chimera-conversion-final-LastTest.log`. Final source audit confirms
  21NFS3 submission sites and no ordinary direct FS calls in SDK/POSIX.
- Completion assessment: assigned conversions finished after root fixes, not at
  initial source handoff. Backend transactions/rollback and deferred VFS cache/
  notify publication remain deferred; SDK/S3 universal automatic finish retry
  remains absent. SMB, NFS4 runtime fallbacks, REST debug endpoints, and lock/
  lifecycle integration remain as prior audit. Nothing committed or merged.


## SDK/S3 retry and NFSv4 adoption follow-up (2026-09-23)

Two user-requested source-only agents completed SDK/S3 retry work and a bounded
NFSv4 next batch. Root built/tested, reviewed completion, and corrected integration.
Full record: [compound-sdk-s3-nfs4-followup.md](docs/reviews/compound-sdk-s3-nfs4-followup.md).

- Supersedes earlier "SDK/S3 universal retry absent": all56 submissions (31SDK,
  3POSIX,22S3) use common bounded finish-EAGAIN adapter, eight retries; ordinary
  op EAGAIN never replays. No ordinary direct filesystem dispatch remains there.
- S3 resets preserve temporary names, continuation cursors, and empty-tail
  publication. Exhausted finish returns InternalError. Integration found/fixed
  UploadPartCopy named OPEN passed request name length instead of attempt length;
  accepted-error UploadPart cleanup now retains scratch name. Cairn/memfs pass.
- VFS retry now returns bool; empty attempts can retry and construction errors
  persist. NFS3/NFS4 callers handle retry unavailability. No backend rollback added.
- NFS4 proxy COPY is compound-native through NFS3/NFS4, obsolete shared VFS
  exclusion removed after checking borrowed WRITE payload ref cloning. Six wire
  tests cover627500-byte multi-hop copies/stateids/EOF/exact spans and root names.
- With / exported, ordinary LOOKUP/SECINFO stay coalesced. Possible junction names
  retain fallback; locked snapshots and execution/retry rechecks detect newly
  added junctions and return DELAY. Async REST-between-attempt test passes.
- Fixture fixes: separate proxy upstream namespaces for portmap/mountd; async
  finish timer avoids blocking REST's event loop.
- Broad models still pinned NFS proxy copyRange=false. Root enabled capability in
  specs POSIX NFS3/NFS4 profiles and Python profiles, removed obsolete ND8 exception,
  regenerated106 traces with original seeds plus2 self-tests. Both53-trace batches
  pass; no mismatch suppression added. Specs submodule has additional uncommitted
  profile edits alongside preexisting changes.
- Final554distinctCTest selections:538passed16skipped0unresolved. Focused20/20;
  broad516pass16skip2stale-profile failures, those2pass on regenerated traces.
  Production build2 passed; build3 changes only replayer. Logs/tmp/chimera-next-
  {build2,build3,focused2,matrix1,posix-gen,model-rerun1}.log; immutable LastTest
  copies and dispatch-audit.json recorded in report. Diff checks clean.
- Remaining NFS4: actual export/credential transitions; noncrossing LOOKUPP and
  matched-name conservative fallbacks; v40 delegation OPEN/CLAIM_DELEGATE replay
  journal; pNFS layouts/removal; synthetic streams; pure validation checkpoints
  and some stateid variants. Preserve independent DELEGRETURN/LAYOUTRETURN progress.
- Backend begin/end/op association, rollback, deferred VFS cache/notify still
  future work. SDK staged read_into/512entry readdir; S3 chunks accepted separately;
  frontend tables not backend participants. Export snapshots do not freeze config
  across async I/O; stronger isolation needs generations/reservations. SMB/REST and
  lock/lifecycle scope unchanged. Nothing committed/merged.

## FUSE/POSIX locking design proposal (2026-09-23)

User requested background FUSE FLUSH agent and a locking plan, not broad locking
implementation yet. Agent `/root/fuse_flush` is doing source-only FLUSH changes
and regressions. Proposed terminal-only lock release after bounded COMMIT retries,
including terminal errors; FSYNC never releases. Agent verified current COMMIT has
no claim admission dependency on these locks. Root has not built/tested this work.
Plan: docs/reviews/fuse-posix-compound-locking-plan.md. Shared typed lock ops and
protocol-neutral interval journal based on existing local RESERVE/range_publish;
preallocated accepted publication, VFS-owned cancellable wait tickets, owner/file
generation for close-vs-grant. Projected backend unlock/downgrade cannot safely be
rolled back by reacquisition; require explicit backend transaction capability.
SEEK_END needs resolved geometry or explicit backend-only mode, not GETATTR+LOCK.
Found existing grant-before-track lifetime TODO and silent carve allocation-failure
behavior in POSIX; FUSE release_owner ignores pending grants. Include in locking
conversion tests/design. User should review proposal before expanding this scope.

## FUSE/POSIX compound locking implementation and validation (2026-09-23)

User approved plan; implementation finished with root-owned builds/tests and
post-agent assessment. Full record:
[report](docs/reviews/fuse-posix-compound-locking-implementation.md).

- FUSE GETLK/SETLK/SETLKW/unlock and POSIX fcntl/lockf use shared typed compounds.
  Shared VFS owns preallocated interval journals, provisional admission (excluded
  from GETLK), accepted publication, cancellation and owner/file close epochs.
  FUSE FLUSH retains locks through COMMIT retries, retires once terminal including
  errors; FSYNC does not release. Mandatory cleanup is nonretryable even with
  rejected finish. Callback lifetime and detached-ticket cancellation races fixed.
- POSIX close retires before draining active fds; slot reservation/generations,
  waiter broadcasts and per-client dup2 serialization prevent reuse/crossdup races.
  Checked offset/length arithmetic and GETLK error/PID publication preserved.
- Backend review required CLAIM_REPLACE: memfs exact preallocated replacement/
  carve; Linux/io_uring one owner-FD anchor and exact fcntl; NFS3 typed owner
  anchors, pre-RPC allocation, exact NLM unlock and retained uncertain ownership.
  NLM server fixed partial unlock and same-owner downgrade (including duplicate
  mode check), with fragment storage reserved before blocking admission.
- Remaining limits: one typed lock op per compound; legacy projected mutation
  rejects finish adapters before effects, no backend transaction rollback;
  SEEK_END moves owner/file to backend-only arbitration until close (cross-protocol
  local-visibility gap), NFS3 GETATTR+NLM EOF race remains; generation tombstones
  persist until domain teardown, empty buckets drop inode refs; persistent cleanup
  failure reported after3 shutdown tries/logged; NLM pending/direct dispatch and
  legacy claim_range_replace limitations remain. No NLM compound conversion claim.
- Root integrated ASan Debug build7; production last changed at build5. Focused42
  pass after test interceptor export, duplicate module symbol, and held-finish
  synchronization fixes. Initial broad543:527pass16skip. Reran12 handle-dependent
  skips using CHIMERA_MBT_SCRATCH=/build/test (ext4), found NFS3/Linux+io_uring data
  model failures. GDB/minimal3-op trace proved SIZE SETATTR downgraded DATA to PATH:
  ftruncate failed, mode000 path fallback denied allocation. Fixed generic handle
  selection, added independent regular-file size+handle-identity regression.
  Both53-trace NFS3 batches then passed; no model suppression/profile change.
- Final full585distinctCTest selection with ext4:581pass4intentionalnonroot
  fchownat skips0fail, no ASan errors (detect_leaks=0 existing configuration).
  Teardown logs exposed synthetic-owner fixture omissions; corrected cleanup only
  after observations, reran all9affected variants:9pass0unresolved-ownerlogs.
  Logs /tmp/chimera-lock-{build5,build6,build7,final,cleanup}.log, immutable
  final/cleanup-LastTest.log; selections /tmp/chimera-lock-{all,focused}.txt.
  git diff --check clean. Nothing committed or merged.

## Overall compound adoption audit after locking (2026-09-23)

Fresh source-only report: docs/reviews/compound-conversion-status-after-locking.md.
No production changes or new tests this audit. Ordinary SDK/POSIX/S3/NFS3/FUSE
filesystem dispatch is compound-routed, including recent FUSE/POSIX locks and
FLUSH. Do not repeat obsolete multipart, vectored SDK, NFS3, or lock-routing gaps.
SMB has no VFS compound references: wire compound advancement still calls ordinary
handlers. Largest remaining project, needing error continuation/related FileId
and accepted durable/share/output publication design. NLM remains direct open,
claim, pending grant/replay machinery despite interval correctness fixes.
NFS4 remaining: namespace/pseudoroot/export credential transitions, conservative
LOOKUPP/junction guards; v40 delegation-enabled OPEN and delegate claims;
nonjournalable state cases; typed pNFS LAYOUTGET/COMMIT and pNFS REMOVE; OPENATTR/
synthetic streams; state-return/control boundaries; pure validation fallbacks.
Ordinary OPEN/LOCK/LOCKU and root-export LOOKUP/proxy COPY already converted.
Smaller direct paths: FUSE mount root lookup and optional REST debug fsop.
Ancillary NFS3/NFS4 DRC, NFS4 recovery and NSM persistence remain direct KV;
need explicit transaction/accepted-ordering scope, not ordinary inode-gap labels.
Typed locks still require a dedicated one-lock compound; projected backend locks
cannot optimistic-retry, SEEK_END local visibility and NFS3 EOF race remain.
S3 chunks/final publication and SDK accepted pages are intentional boundaries.
Backend begin/end/abort and nested operation association remain deferred; VFS
cache/notify currently publish per operation (remove_at example), so backend
rollback alone will not suffice. Latest prior tests581pass4skip, not rerun here.

## REST debug filesystem endpoint converted (2026-09-23)

User requested closing the REST gap. rest_debug.c now builds one compound per
unlink/rename/link/chmod, using the common bounded finish retry adapter and one
terminal HTTP callback. Chmod lookup/open/setattr stays in one compound; copied
paths/attrs and intermediate handles belong to it. Preserves server credentials,
symlink semantics and HTTP400/500/200 behavior; validates before construction.
No direct ordinary filesystem dispatch remains in this endpoint. The overall
status report above is updated; do not keep listing REST as unconverted.
New Linux ELF-interposition HTTP regression checks observed NFS results, all four
mutations after2finish rejections, symlink behavior, exhausted9attempt rejection,
operation EAGAIN without replay, lookup/mutation errors and invalid JSON/no submit.
Rejected attempts veto mutations before execution; no rollback claim. Added
rest_debug_fsops knob to shared NFS test environment; retained existing changes.
ASan build passes. All8REST/control-plane CTests pass; pynfs DELEG16-20 all5pass
through real HTTP recall helper. Logs /tmp/chimera-rest-compound-{suite,deleg}.log,
deleg.xml; builds3/4 and final build5 (comment/CMake portability guard only).
ASAN_OPTIONS=detect_leaks=0. No production backend changes, commits or merges.

## SMB compound fleet plan (2026-09-23)

User requested a division-of-work plan, not implementation yet. Saved complete
plan in docs/reviews/smb-compound-agent-plan.md; no agents launched this turn.
Four concurrency slots => root coordinator/integration/build/test owner plus
three source/test-writing workers. First wave: VFS execution groups/contracts,
SMB runtime/attempt journal, independent conformance/coverage. Agree error
continuation, group credentials, handle/journal ownership, accepted publication,
retry/cancel/coordination contracts; validate a thin vertical slice before broad
handler conversion. Wave2: claims foundation, basic I/O/enumeration, metadata.
Wave3: CREATE/CLOSE/durable, streams/reparse/copy IOCTLs, async/locks/control.
Wave4: independent coverage, retry/lifetime and protocol-equivalence reviews.
One owner each for VFS compound core and SMB runtime/shared headers; explicit
handoffs for CREATE/stream and QUERY_INFO/stream overlaps. Root owns CMake and
all builds/tests; workers report gaps and do not declare source edits validated.
Backend transactions and VFS deferred cache/notify remain separate prerequisites;
reject mutating injected attempts before effects, no pretend rollback. Final
audit must prove near1:1 wire mapping and classify direct/hidden side effects,
not merely count compound wrappers. Detailed plan includes acceptance matrix.

## SMB compound implementation in progress (2026-09-23)

User authorized full fleet plan. Three workers launched; root owns integration,
CMake and all builds/tests. Foundation adds VFS operation groups (continue errors,
immutable per-group creds/context, dependency skip, stable dynamic-op links),
PUTHANDLE_FROM handle provenance and cooperative cancel. Dynamic typed locks
remain forbidden in mixed compounds. First runtime coalesces existing-open
normal READ/FLUSH/QUERY_BASIC, with replayable checks, shared private open state,
accepted publication, independent VFS handle pins and session/tree memory pins
that do NOT defer logical disconnect cleanup. Context pins survive reply.
Coverage finding: existing SMB MBT do_message sends individual PDUs, so passing
that suite does not prove wire-compound mapping. New smb2_compound_probe sends
real NextCommand PDUs and counts submissions, holds finish, rejects readonly
attempts, tests error/inheritance/own-lock cases and tree retirement during hold.
Its first ECHO interposition was invalid (hidden symbol); fixed with owning-loop
timer captured by compound_alloc interposition. Never fake backend rollback.
Full ASan build passed; VFS compound/groups 2pass. Initial integrated selection
19pass1fail: new wire probe passes; info probe found surviving POSIX-unlinked
open READ returning FILE_CLOSED. Root removed inappropriate file-wide
smb_delete_started rejection in handle pinning; individual close still checked.
Fix awaiting next integration run. Logs /tmp/chimera-smb-foundation-{full-build,
build3,tests}.log; baseline old SMB suite14pass before foundations.
Wave2 ongoing: VFS worker exact range-claim attempt journal; IO worker WRITE,
QUERY_DIRECTORY, RDMA; metadata worker query/set/security/EA. Root runtime now
has gather_inputs once, accepted async publication, reply_release lifetime,
flags overlay, preserves builder prepare callbacks. Root VFS SETATTR propagates
explicit claim actor including descriptor rights; COORDINATE each-attempt option
only for repeatable cache inputs (idmap), not notifications. Not yet validated.
Remaining package gates and waves in docs/reviews/smb-compound-agent-plan.md.

### SMB integration continuation (2026-09-23, Wave 3)

Runtime now coalesces ordinary READ/WRITE/FLUSH, QUERY_DIRECTORY, most information
and security/EA operations, sparse/copy/offload/clone/integrity/reparse-query
IOCTLs, typed stream listing and fresh simple CREATE/use/CLOSE. Existing
metadata-only FILE_OPEN also joins the provisional-open path. Full data OPEN,
existing CLOSE, namespace/disposition, lease/durable contexts, reparse replacement
and backend-projected locks still need lifecycle facilities; do not claim full
conversion. Canonical exact range-owner journal and async RANGE_BATCH are built;
SMB LOCK mapper remains disabled until canonical-owner migration is reviewed.
Latest exact residual inventory: docs/reviews/smb-compound-wave1-coverage.md.

Root fixes: surviving POSIX-unlinked opens must remain usable; native owned
COPY/CLONE and now ALLOCATE wait for peer cache invalidation; directory ADS
SETATTR must resize stream bytes even when base inode is a directory. Coverage
fixed stream-list result count, stream DOC handle provenance, dropped-reply EA
buffer ownership and accepted mutation notifications after requester disconnect.
Metadata readonly retry/exhaustion fixture verifies EA/security/stream output.
True wire tests verify fresh CREATE->WRITE->QUERY->READ->CLOSE, failed CREATE
related error inheritance, metadata OPEN->QUERY->denied READ->CLOSE, cursor
snapshots and multiple directory pages; all count a single VFS submission.

Full build4 passed (/tmp/chimera-smb-wave3-build4.log); focused VFS compound and
three SMB compound probes 4/4 passed (/tmp/chimera-smb-wave3-tests4.log); native
COPY/CLONE/ALLOCATE cache-gate regression passed (/tmp/chimera-smb-native-gate2.log).
VFS groups timeout diagnosed under gdb as fixture event loop blocking after last
oneshot callback completed; wait_ms=1 fixture configuration fixes it, groups test
passes (/tmp/chimera-smb-groups-fixed.log). Broad suite currently in progress at
/tmp/chimera-smb-wave3-suite.log. Later source changes require another build/test.
That suite finished 16pass/5fail: all five SMB MBT profiles have the same new
root metadata OPEN regression (327 mismatches, many cascading FILE_CLOSED).
Independent coverage audit identified OPEN_CURRENT does not fill out_handle or
attrs: root FILE_OPEN needs explicit GETATTR/GETHANDLE; parent provenance also
needs GETHANDLE for nested CREATE/symlink paths. Runtime agent is fixing this;
do not suppress model mismatches. Cross-protocol selection 8/8 passed: NFS4
ordinary/delegation/named attrs, S3 ordinary/multipart, FUSE and POSIX memfs MBT
(/tmp/chimera-smb-cross-protocol.log).

### SMB continuation (2026-09-24)

Fixed root metadata OPEN explicitly fetching attrs/owned handle; targeted old
failing trace now passes. Local SMB LOCK mapper enabled with canonical exact
owner migration; real wire LOCK/WRITE/peer denial/UNLOCK, claim-only retries,
duplicate shared records, blocking grant/CANCEL/CLOSE probe passes, as do VFS
groups. All five SMB MBT wire profiles pass with this enabled
(/tmp/chimera-smb-lock-mbt.log, build7). Backend-projected/legacy-owned locks and
unresolved provisional handles retain explicit boundaries.

Important fixture correction: one compound submission alone does NOT prove
whole-wire adoption (legacy CREATE + QUERY compound + legacy CLOSE also counts
one). All true-wire fixtures now require exact group count, using new
chimera_vfs_compound_num_groups accessor. New lifecycle test exposed cold-tree
CREATE eligibility fallback and QFid loss with CREATE->CLOSE legacy lifetime;
runtime owner fixing cold-root lookup inside compound. Independent review also
found provisional CLOSE missing accepted parent lease FILE_MODIFIED notification.
Both in active refinement, not yet validated.

Non-replacing HARDLINK implementation/test source added, preserving source and
parent lease identity and moving notification to accepted publication; awaiting
tests. Replacement remains boundary. New ACCESS owner/journal needed for public
CLOSE: canonical independently owned share-claim storage, staged retirement,
exclusions for later admissions, external teardown cutoff and explicit drainage;
VFS agent implementing core/tests, runtime agent migrating SMB share/base-share
accesses and retirement. Namespace rename needs its own path/DOC journal;
coverage agent investigating/implementing this, no unsafe RENAME enabled.

Latest integration: cold-tree CREATE now resolves root inside same group;
lifecycle contexts/root/nested/257-open test passes. True linked compounds pass
signed30, signed311 and encrypted311 profiles with per-command signatures
verified. HARDLINK and exact group-count checks pass. ACCESS token migration
build4 passes; focused SMB/VFS21/21 pass (/tmp/chimera-smb-wave4-tests4.log).
New ACCESS unit + compound retry/retire tests pass. Existing public CLOSE still
disabled pending forced-close/durable cleanup fencing and DOC namespace peers.

New critical root review finding: exclusive per-owner range-journal edit leases
can deadlock two compounds taking owners A/B in opposite order even for disjoint
ranges. VFS agent confirmed; redesign authorized to concurrent owner lifetime
pins and per-record removal reservations (not artificial client denial). Record
refs must survive accepted publication until every referencing journal drains.
This known issue is ACTIVE pending source-ready redesign/tests; prior passing
LOCK MBT does not cover it. Namespace helper also must distinguish hardlink
aliases (same FH, different parent/name) and share-relative paths; root flagged
unconditional same-FH repath before integration. Directory descendant path
updates remain a separate enablement requirement.

### SMB integration: retirement, namespace membership and shutdown

Range journal owner-wide deadlock fixed with concurrent lifetime pins and
per-record removal reservations. Root target build7/test7 passed 7/7, including
opposite-owner/duplicate/external-cutoff regression, namespace unit and ACCESS
retirement real-wire fixture. Added atomic RETIRE_OPEN_CLAIMS (RANGE + ordinary
and base ACCESS) with operation-local unwind: a failed CLOSE admission must not
retire only a subset or discard successful earlier command deltas. Root combined
retirement regressions passed 2/2 (/tmp/chimera-smb-retire-tests9.log).

Root added typed PUT_KEY_AT/DELETE_KEY_AT with immutable binary input copies,
current-FH routing, reset/free ownership and cursor preservation. Added bounds
checks to routed direct KV APIs before request scratch copies (8192-byte scratch,
including fallback namespace prefix); binary roundtrip and oversized lengths
regression passed. SEARCH_KEYS_AT remains active VFS-agent follow-up.

Independent weak namespace participants now retain DOC discovery after ACCESS
retirement without creating permanent open-reference cycles. CREATE preallocates
and binds attempt-private participants and attaches only accepted public opens;
physical recycle detaches before reuse, with registry-before-file/bucket order.
Namespace edit/RENAME production dispatch still disabled pending full guards,
path snapshots, lifecycle fences and hardlink/share-root identity constraints.
DOC-only fences/disposition gating are being integrated for public CLOSE.

Explicit shutdown drainage and exact wire-compound lifetime accounting exposed
six old malformed/rejected-parser paths leaking compounds. Replay test timed out
22/23 suite (/tmp/chimera-smb-wave4-tests8.log); gdb showed live_compounds=1 and
retiring_opens=NULL. Runtime fixed all six parser exits plus parked CREATE/pipe
and share-ticket cancellation completion ownership. Full build10 and focused5/5
pass (/tmp/chimera-smb-wave4-tests10.log); replay now0.47s. ACCESS probe pins real
journal through AppInstance eviction, verifies anchors survive then drain; new
shutdown case suppresses timer for one hour and requires explicit drain. Further
parked pipe/CREATE shutdown source added after build10, awaiting next build.
Public existing CLOSE is still active implementation, not yet declared converted.

### SMB Wave 5 active work

Full build10 + SMB MBT5/5 and cross-protocol10/10 passed
(/tmp/chimera-smb-wave4-mbt10.log, /tmp/chimera-smb-wave4-cross10.log).
Public no-DOC regular CLOSE now coalesces with existing-open commands using
atomic RANGE/ACCESS retirement and accepted-only unhash. First lifecycle test
found a retained-reference leak: POSTQUERY implicitly reopened a PATH cursor,
then CLOSE closed that cursor rather than the original independently retained
handle. Explicit rebind before CLOSE fixed it. Full wave5-build6 and focused
24/24 pass (/tmp/chimera-smb-wave5-tests6.log), including namespace, streams,
true wire/protected profiles, lifetime/LOCK and expanded shutdown fixtures.

Root durable maintenance conversion: retired-share record deletion is a typed
DELETE_KEY_AT compound; cold recovery uses bounded SEARCH_KEYS_AT pages (128
records /1MiB), prepares a private hash before finish and publishes only accepted
pages without allocation. Retries discard private entries; continuations defer
to event loop to avoid recursive page stack. Thread maintenance_compounds drains
at shutdown. PID advancement now CAS-based so concurrent CREATE cannot be
clobbered by recovery load/store. New 300-record test checks3pages, transient and
exhausted readonly finish rejection, zero early registry/allocator publication,
malformed record skipping and idempotence; passes. SEARCH_KEYS_AT owns copied
binary inputs/results/cursor, with routed scratch bounds.

VFS strict matched REMOVE + NO_NOTIFY source/regressions added. Existing legacy
match-child-FH is implemented only by memfs; new CAP_REMOVE_MATCH_FH advertised
there only and strict typed form rejects unsupported backends before dispatch.
NO_NOTIFY suppresses both generic and remove-completion events while retaining
cache coordination; caller publishes actual removal events after acceptance.
Namespace credential override limited to OPEN_CURRENT/REMOVE. Latest core tests
including exact-new-name preservation/unmatched and held-finish notification
cases need validation against latest source (some landed after build6).

Active: DOC module disposition/query/CLOSE with batch-private intent, provisional
CREATE LOCK owner operation, full public CLOSE retry fixture. Root flagged DOC
last-close race with unrelated non-DOC CREATE admission, parent-lease exemption
for innocent last closer, and legacy ENOENT status preservation; owners refining
before enablement. Ordinary data OPEN and broader namespace/durable/grant paths
remain conversion work; do not claim complete.

### SMB Wave 6 integration (active)

Wave5 build11/focused24/24 passed, validating related CREATE, public regular and
directory/root CLOSE, provisional RANGE_OWNER and CREATE→LOCK→WRITE→UNLOCK→CLOSE,
strict matched REMOVE and ACCESS retirement. Important correction: SMB range
owners are always local according to claim_range_is_local (all non-POSIX
owners); backend-projected SMB locks are not a valid blanket conversion boundary.
Phase0 durable registry fixture fixed to supply actual tree lifetime pins;
manual test43/43 passes. Durable park absent-registry case no longer leaks a pin.

Wave6 build1+incremental2 passes. Added ACCESS insertion fences (including inert
claims), paused existing admission tickets, stable batch-owned admission cookie,
NFS4 DELAY mapping for fence contention, and declared same-group cancellation
scopes. Scope RETIRE_OPEN_CLAIMS→CLOSE ensures cancellation cannot strand a
successful claim retirement before the preallocated DOC deletion/close suffix.
Successful typed start arms before complete callbacks; later groups still stop.

DOC disposition/query/regular-file CLOSE now production enabled, with typed
strict matched REMOVE and accepted-only events. New true-wire fixture passes
held intent finish, retries, cross-handle pending and last-close effects. Root
resiliency IOCTL conversion reserves a full private durable hash at construction;
accepted publication inserts without allocation, repeated grants remain one
entry, rejected finishes publish nothing. Wire fixture verifies exhausted and
transient rejection plus repeated timeout updates.

Wave6 focused suite24/25 passes (/tmp/chimera-smb-wave6-tests2.log). ACTIVE
regression: info_probe concurrent DOC CLOSE tests expect both closes to succeed;
fail-fast DOC fence returns FILE_NOT_AVAILABLE and leaves pending names.
Do not weaken tests. Runtime/coverage agents implementing safe admission before
any compound work, acquire full known DOC fence set or unwind and wait holding
none. Waiting with partial fences/claims can deadlock. Directory CREATE still
needs typed mkdir NO_NOTIFY, active VFS/runtime work. Full conversion remains
unfinished, including broad data OPEN, durable/grants, namespace rename/link,
streams and reparse identity migration.

Wave6 broader checkpoint: SMB MBT5/5 and crossprotocol11/11 pass
(/tmp/chimera-smb-wave6-mbt2.log, /tmp/chimera-smb-wave6-cross2.log), despite
the separate concurrent-CLOSE information-probe regression. Additional active
audit fixes: legacy release_doc ignored logical DOC fences and could overwrite
compound-private pending snapshots; legacy retirement must release recall/grant
rights first, then wait guarded DOC processing without blocking recall progress.
VFS provisional producer CLOSE dropped its handle before RANGE_OWNER journal
consumers finished; VFS agent adding owned close-handle retention until journal
cleanup on accepted/rejected attempts. Runtime fixing accepted modified-before-
removed observer event order. Directory mkdir NO_NOTIFY facility/regression
source ready; directory frontend conversion follows correctness integration.

Wave6 build6 passes; focused24/25: prior concurrent CLOSE regression FIXED;
only new CREATE-DOC peer-clear fixture failed. Investigation found its initial
expectation wrong: MS-FSA plain FileDispositionInformation clears link IsDeleted,
not per-open Mode.FILE_DELETE_ON_CLOSE; subsequent flagged close re-marks it.
Sources: https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/386d9ec5-e0f6-4853-b175-c05be01419e0
and https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/d142c93a-72bc-4b05-9d96-8e00371c3308 .
Coverage added sequential control, removed cache-handle-alias dependency by
snapshotting logical open path/credential, and intentionally preserves CREATE
mode across own ordinary clear too; runtime aligns legacy release_doc. Pending
build7 tests. Notification modified-before-removed change preserves existing
legacy order; MS-FSA Phase5 actually lists removed before pending modified, so
do not describe the original ordering as a proven protocol defect.

All-set DOC preflight now waits before VFS submission with no partial holds;
blocking LOCK and baseline directory-CLOSE batches split from guarded DOC work
to avoid waits on peers which need those guards. Mixed legacy close and common
disconnect/durable/finalizer retirement now wait for DOC+ACCESS guards before
release_doc/unlink, retain namespace membership until free, and hold guards
through cleanup. Additional early-cache liveness fix: revoke last-member cache
rights before refcount/journal waits but retain grant storage (ACCESS own_cache
borrows it) until safe final drainage. Never simply free grant before journals.

Core retained-producer-handle regression passed tests5 after correcting its
fixture to use an actual producing OPEN (OPEN_CURRENT owns cursor only).
Root resiliency tests now pass invalid timeout, initial/update exhaustion,
repeated accepted grants, provisional CREATE→resiliency and coalesced resilient
QUERY→CLOSE. SDK module version bumped3 for incompatible request layout changes.
Next active work: directory CREATE wire fixture, ordinary data OPEN, regular
nonreplacing RENAME with strict source-match/NOREPLACE/NO_NOTIFY and private
path overlay. Stream verify-FH→REMOVE_STREAM helper still has a check/use race;
it needs strict backend source matching or equivalent namespace protection.

### SMB wave6 follow-up: build8 checkpoint

Build8 initially failed because the direct backend regression instantiated hidden
VFS debug logging via inline dispatch/complete helpers; fixture now calls backend
dispatch directly. Full build8b and 25 focused tests pass, including directory
CREATE accepted-prefix notification and strict matched/NOREPLACE rename core.
SMB rename mapper remains gated pending unpublished OPEN path coordination.

Broader build7 run: all 11 cross-protocol checks passed, SMB five profiles each
failed the same two stale model expectations (traces stepNs seed0xd, sequences
0 and4). Root verified MS-FSA FileDispositionInformation/Closing an Open:
plain disposition clear changes Link.IsDeleted but does not clear CREATE mode.
Fixed doSetDisposition to preserve deleteOnClose, added own-clear and peer-clear
model tests; all three Quint self-test configurations and regenerated94 traces
pass. SMB wire rerun in progress; no mismatch suppression added.

Root found identical stream legacy clear bug: preserve doc_from_create on plain
clear and let stream retirement honor it independently of cleared current flag.
Added real-wire CREATE-mode clear case to stream last-close fixture; source ready,
not yet built/tested. Stream matched-remove check/use race remains separate.

Build8b SMB wire rerun passes all five profiles (94 traces/profile) plus NFS4
named-attribute probe. Source-only stream clear fix awaits next full build.
Regular-file memfs rename now avoids an inherited ancestor-lock inversion via
immutable dirent.is_dir initialized at all six allocation sites. Directory
ancestry locking itself still needs broader backend concurrency review; do not
claim the whole directory lock-order problem fixed. New deterministic backend
lock fixture is in the Extended ctest tier and initially failed setup before
rename success; VFS owner is correcting it. Compound-groups test passes build9.

Important new integration constraints: pending private OPEN can precede namespace
publication, so accepted rename must preserve its path through binding/finish/
retry. Coverage/runtime implementing invisible pre-OPEN name token with accepted
external rename deltas, matching actual FH on bind. Never publish foreign open
fields from another thread without synchronization. OPEN_H/W coordination waits
must split from DOC-preflight guarded batches, like blocking LOCK, or a recalled
peer CLOSE could deadlock on the same namespace guards. NULL provisional ACCESS
claims must conservatively count as live peers during legacy DOC retirement.

### SMB wave6 final validated checkpoint: build13

Full build13 passes. Focused26/26 pass, including live BATCH/EXCLUSIVE recall
with two finish rejections, data OPEN deferred suffix ownership/exhaustion,
activated bounded RENAME chains/hardlink aliases/DOC and held readonly producer
A→B→C acceptance versus retry opening a recreated A. Extended memfs rename-lock
regression passes. Cross-protocol12/12 pass (NFS3 compound, NFS4 ordinary/deleg/
namedattrs, S3 four profiles, FUSE MBT+locks, POSIX MBT, REST fsop). All5 SMB
profiles pass94traces each. Build/test logs /tmp/chimera-smb-wave6-*13.log.
Root status snapshot: docs/reviews/smb-compound-current-status.md.

Additional corrections after build10:
- Typed admission_fenced EBUSY maps FILE_NOT_AVAILABLE, not generic CREATE error.
- Harness waits for final async CREATE reply before using its modeled FileId;
  ACK on another connection does not guarantee final CREATE already arrived.
  Fixed trace smb2Leases_stepLease_300_0x23_7 state24 FILE_CLOSED in4profiles.
  This is separate from earlier Quint disposition model correction.
- Live-cache recall and pending namespace coordination run each attempt; avoids
  historical FH A→B→A memo reuse and cached deferred EINTR mismatches.
- Core permits grouped dynamic COORD from completion callbacks only. Memo key
  includes index+function+private context+FH; stable contexts live through cp_free.
  Tests change callback and context at reused dynamic positions across retries.
- Reverse descriptor destruction keeps private CREATE alive for LOCK consumers.
- Legacy DOC counts NULL provisional ACCESS as peers.
- Root stream plain-clear fix tested: CREATE mode survives cleared current flag.
- Immutable memfs dirent.is_dir avoids regular rename ancestor locks; cycle
  predecessor identity checked before source ancestor lock. General directory
  namespace lock ordering remains a known correctness issue.
- Legacy rename now exact-link alias-aware, tracks pending tokens and updates
  full_path for same-view moves. Cross-directory different-share-root translation
  and directory descendants remain incomplete.

Last fixture-only corrections: query-directory ENOTDIR maps INVALID_PARAMETER
(existing documented behavior); name snapshot query uses supported normalized
name class0x30, not unsupported0x09. No model suppressions added. Build13 tree
is clean under git diff --check (including ext/specs); all user edits retained.


## SMB wave7 integration (2026-09-24, in progress)

User said continue. Three agents implemented: ordinary OPEN_IF and requested-W
OVERWRITE/OVERWRITE_IF/SUPERSEDE; same-view nested/cross-directory nonreplace
regular-file RENAME; atomic expected-FH stream removal plus accepted notification.
Root added typed compound OVERWRITE using existing VFS overwrite primitive (base
overwrite removes forks, individual stream preserves siblings) and actual-data
regression. No mutation finish-rejection tests: rollback still absent.

Build5 core groups passes; initial focused26 had only fixture issues corrected
(root stream finish helper must expect ESTALE, LIST_STREAMS includes unnamed fork,
stream fixture must remove its base before subsequent fixture reuses its name).
All expanded SMB probes passed. All five SMB profiles passed94traces each at
build5; all12 cross-protocol checks and Extended memfs rename lock test pass.
Logs /tmp/chimera-smb-wave7-{mbt,cross,memfslock,groups}5.log.

Read-only agent review caught CREATE long-basename copy overflow before backend
validation; post-admission failed OVERWRITE private ACCESS blocking later groups;
and stale overwritten-file notification path after external rename of pending
open. Runtime is fixing these before final validation. VFS agent advising failure
cleanup: preserve accepted-prefix semantics, no blanket group claim rollback.
Rename stored full_path separator mismatch already fixed to SMB backslashes.

Remaining restrictions: transient-W overwrite variants, caching/durable/EA/stream
CREATE and complex CLOSE; rename replacements/provisional sources/directories/
stream holders/cross-share views. Moving destination ancestor can stale full_path
while stable parent FH remains correct; legacy directory-descendant tracking and
general memfs directory lock ordering remain correctness gaps.


## SMB wave7 final validated checkpoint (2026-09-24)

Full build7 passes. Focused26 pass across tests6 (25pass + core fixture failure)
and groups7 (corrected core passes). All5 SMB profiles pass94traces each at
build6; cross12/12 pass build6; Extended memfs rename lock passes build5 (same
backend code). Final build7 changed only core fixture OPEN_CURRENT to owned OPEN
so subsequent groups can consume its handle. No production edits after build6.
Logs /tmp/chimera-smb-wave7-{build7,tests6,groups7,mbt6,cross6,memfslock5}.log.

Review findings addressed: parser rejects base component >255 before fixed-buffer
copy (stream suffix validated separately); overwrite notice snapshots execution
namespace path; failed CREATE private share reservation no longer blocks later
independent commands. New explicit VFS reserve_access_until(reserve,ready) marks
a reservation provisional until same-group CHECKPOINT runs unskipped OK and group
succeeds. Failure/cancel/skipped readiness retires only marked private reservation
while retaining owner/file storage until normal cleanup. Unmarked prefix, earlier
RESERVE/CLOSE journal edits, filesystem mutations preserved. Dynamic endpoint
validation follows group execution links, not physical index. All CREATE variants
become available only at final readiness; static/dynamic builders register marker.

Wire regression injects pre-mutation OVERWRITE and post-reservation COORD failure,
then independent OPEN→QUERY→CLOSE succeeds in same4-group compound, bytes intact.
Core tests cover errors, skips, cancellation, readonly finish retry, dynamic suffix,
earlier accepted claims/retire edits and malformed cross-group endpoint. No new
model suppressions or fake filesystem rollback.

Wave7 enabled OPEN_IF regular/directory; requested-W ordinary OVERWRITE/IF/
SUPERSEDE; nested/cross-directory same-view ordinary nonreplace RENAME; checked
stream DOC removal with accepted STREAM_NAME. Full conversion is NOT complete: see
current snapshot docs/reviews/smb-compound-current-status.md for boundaries and
known path/ancestor lock correctness risks. All agent work stopped source-ready;
root centrally built/tested and fixed fixture integration errors. No commits.


## SMB wave8 ongoing integration (2026-09-24)

Root continued with same three agents: VFS private ACCESS narrowing; runtime
EA-at-CREATE + stat/read-only overwrite; coverage provisional-source RENAME.
Root enabled ordinary non-DOC stream CLOSE (both stream/base DOC fences and
intent checks, typed retirement already covers both ACCESS owners) + exact3-group
QUERY/CLOSE/baseQUERY test with readonly finish rejection, accepted-only modified
notification, base DELETE admission released, stream content preserved. Coverage
added symmetric other-close-fence vs waiting OPEN/LOCK batch boundaries.

Build3 passes. Focused build2 tests25/26 passed; lifecycle fixture expected raw
EA record length instead of 4-byte wire padding. Agent fixed fixture only;
lifecycle3 now passes. Cross12/12 pass at build2.
BUT all5 SMB profiles at build2 failed: real stat-only OVERWRITE admission bug
zeros denied bits for stat lifetime, then adds transient W without restoring
requested share-denials. Earliest reproducer stepCore_500_0x7_4 state9: existing
c openedRW shareRWD; metadata-only OVERWRITE shareR+D incorrectly succeeds,
truncates despite existing writer. Runtime agent fixing transient claim W plus
original requested denied bits, then narrowing to lifetime0/0; no model changes.
Root tests/builds exclusively; no processes currently running at this note.
Logs /tmp/chimera-smb-wave8-{build3,tests2,lifecycle3,cross2,mbt2}.log.

Other wave8 code source-ready: typed NARROW_ACCESS uses append-only ACCESS
journal deltas, shadow conflict test plus canonical exclusions for same-compound
admission, actual rights held conservative until accepted publication; retries
discard deltas. ExtA prebuilt LIST/SET/REMOVE + owned overflow buffer >1024 with
request flag cleanup/duplicate context handling; prefix errors preserved. Provisional
RENAME late-binds actual FH, tries gates nonblocking and defers only untouched
rename on contention, releases dynamic gates before producer retry/cachewait.
Known leftover EA behavior inherited from legacy: initial xattr list snapshot
canonicalization does not track case variants newly introduced earlier in the same
input list (not yet fixed, not root failure). Full conversion remains unfinished.


## SMB wave8 validated checkpoint (2026-09-24)

Supersedes ongoing wave8 notes above. Full Debug+ASan build6 passes. Focused26/26
at build5, cross12/12 at build2, Extended memfs rename lock1/1 at build5, SMB five
profiles94traces each all pass at build6. Logs /tmp/chimera-smb-wave8-{build6,
tests5,cross2,memfslock5,mbt6}.log. Cross tests preceded SMB-only share-mask fix;
final build6 changes only replay ordering after focused tests. No commits.

Real stat-only overwrite bug fixed: destructive admission retains original
requested share-denied bits with transientW, then narrows to inert lifetime0/0.
Wire regressions cover existing writer/reader denied by new share mask, plus
existing holder denying transientW; reject before truncation, bytes intact.

Residual full-corpus failures at build5 were harness scheduling: after a lease
ACK the harness sent an independent overwrite before the earlier pending CREATE
finished. Model assumed sequential completion, actual competing transient SHARE
rights could legitimately fail either OPEN. Root added finish_unblocked_creates:
wait actual final reply only once all modeled ACK-required holders/lease peers
clear breaking. Existing result comparisons remain intact, no model changes/skips.
Failing stepLease_300_0x23_7 trace and all5 full profiles pass build6. Runtime agent
read-only audit found no blocker; non-ACK/empty-break parked creates intentionally
retain existing polling/FileId wait behavior.

Enabled wave8: append-only private NARROW_ACCESS; metadata/read-only overwrites;
CREATE EAs with owned >1024-byte inputs/duplicate cleanup; provisional-source
nonreplace regular RENAME; ordinary non-DOC stream CLOSE. Contracts and remaining
boundaries in docs/reviews/smb-compound-current-status.md. Still incomplete:
cache/lease grants, durable/reconnect/AppInstance, streamCREATE/createDOC,
unbuffered and non-OPEN reparse options; complex CLOSE; replacing/directory/
stream-holder rename, replacing hardlinks and SET_REPARSE_POINT. Known inherited
EA same-input case-map and namespace path/ancestor-lock concerns remain explicit.
Backend transactions/rollback remain deferred; no mutating finish-rejection tests.

Final independent read-only audits: VFS narrowing/CREATE lifecycle and provisional
RENAME/ordinary stream CLOSE/wait classification both found no new blocker.
All three workers idle at this checkpoint; no build/test processes remain.
Root and ext/specs git diff --check pass.


## SMB wave9 integration (2026-09-24)

User requested another SMB round. Root reused three agents, central builds/tests.
Runtime: unbuffered effective DesiredAccess + post-generic/MAX grant APPEND
filtering; non-OPEN RP options ordinary leaves coalesce, actualsymlink discovery
stops beforemutation and defers legacy. CREATE sequentialEA name map/tombstones.
VFS: memfs perFS rename topology rwlock; directory cross-parent moves exclusive,
regular/sameparent shared; every extra inode usestrylock, dropsall beforewait,
restarts FH/name/generation/cycle/nlink validation. Removedparents fail. Tests
force ancestor/parent/replacement contention, rebinding, orphan, independent
regular progress, opposing moves. General LOOKUP/READDIR(..) parent-child inverse
still remains independently; no claim allnamespace lockingfixed.
Coverage: SET_REPARSE cannot safely convert mechanically; successfullegacy swaps
handle but ACCESS/RANGE/namespace stayold. Cross-request sameFileId refs need
exclusive lifetime gate/versionedidentity before migration. Dedicated design
note docs/reviews/smb-set-reparse-compound-design.md. Landed bounded hardening:
retained oldFH + strict matchedREMOVE, unknownNFStype beforeunlink, missingFH/
reopenfailure returnerror notsuccess, no handle resurrection/CLOSE and DOCarming
only on successfulswap. Capability unsupportedfailsbeforemutation. Actual
partialeffects verifiedin faulttests, no fake rollback.

Root: SET_INFO private sequentialEA history; converted legacy ea_apply helper
LIST+SET/REMOVE loop to VFScompounds, ownedretainedhandle, privateattemptreset,
terminalonlycallback, finishretry commonadapter. FixedEA parseruint32 offsetwrap
using remaininglength checks, unit regressions. IndependentVFS reviewcaught
255-byte canonical scratchterminatoroverflow and 1024op capacitylimit on valid
~16KiB repeatedEA vectors; root fixedbuffer+1 and acceptedchunk adapter. Original
LISTsnapshot and full committedhistory survivechunks; retry restores current
chunkoffset/count only. SETpreflight capacitychecks beforemutation; runtime
EA CREATE budgetcheckpoint delayspipelineconstruction until all staticgroups
exist, checks2*EAcount+32refresh+64headroom, deferspre-mutation ifinsufficient.
Runtime alsofixed !PERSISTED legacyCREATE EAerror unreachabletreeopen/shareleak;
normal unhash+refs retirement, savedFID invalidated, successfulEAprefixpreserved.
PERSISTED failurecleanup stillneedsbackendrecordDELETE and remainsknown gap.

Build8 passes. Tests8 focused26/26; Extended6 EAparser+expandedmemfslock2/2pass.
MBTfiveprofiles +cross12runningatthisnote. Tests6 caught dynamicCREATE op_args
NULLduringcompletion, fixed by COORD/MKDIRflags inprepare; tests3 caughtroot
LISTop.status UNSETduringcallback (useoriginal *statusbool) andfixed. Build1/2/4/5
caughtfixtureinclude/macros +invalidENOMEMenum, allfixed. No testsuppression.
Large regressions:1100-entryCREATE,2044-entrySET with >4KiBnames across2chunks,
max250byteEAname repeated, case delete/recreate+prefixerror. No new mutating
finishrejection. Independentmemfs sourceauditclean asideknown other inversions.
Logs /tmp/chimera-smb-wave9-{build8,tests8,extended6,mbt8,cross8}.log.

Wave9 final: all5 SMBprofiles94traces each PASS; cross12/12 PASS onbuild8.
Combinedselected45CTestentriespass (26focused+2Extended+5SMBprofiles+12cross).
No builds/tests running. Root and ext/specs diffchecks clean. All agentsidle.
Current status doc is authoritative validatedwave9 snapshot; no commits made.

## 2026-09-24 SMB wave10 implementation and integration (validation pending)

User requested another SMB pass. Runtime converted bounded uncached regular-base
named-stream FILE_OPEN/CREATE/OPEN_IF: immutable parsed names, nontruncating base
OPEN, base DELETE ACCESS and typed OPEN_STREAM + stream ACCESS, both provisional
until common readiness checkpoint. Accepted surviving opens retain base_handle
through base ACCESS retirement (also fixed legacy stream success anchor lifetime).
Related I/O/CLOSE uses private stream/base owner aliases. claim_closed recognizes
base owner; DOC path overlays match stream base FH. Base+stream namespace publication
is atomic under registry lock, avoiding already-admitted foreign rename updating
half-published records. Unit covers renamed pending path and pair conflict.
Stream overwrite/EAs/directory-base/RP/caching/durable/DOC remain boundaries.
Existing-base reverse share denial defers before stream mutation. Integration
found disconnect DOC fence changed FILE_OPEN DELETE_PENDING to FILE_NOT_AVAILABLE;
bounded existing-base FILE_OPEN now defers before stream mutation on fence.
CREATE/OPEN_IF still reject fence, never replay an actual base creation.

Coverage converted unique legacy II/EX/BATCH oplock CLOSE. Ends VFS run at CLOSE,
preceding metadata coalesces. Explicit grant+file snapshot pin; read-only identity
check; no-DOC coordination defers on fence/newDOC to avoid recall-vs-DOC deadlock.
Accepted unhash/member removal, grant revoke+drain only after compound free/journal
drain. Shared RqLs/directory lease CLOSE remains boundary: grant coalesce refcount
precedes member attachment, needs atomic pending-member accounting. VFS exported
existing grant-pin helper, extended claim lifetime test, no cache journal needed
for this terminal-CLOSE slice.

Root persistent CREATE failure helper handles EA and legacy overwrite failure:
atomically unhash live or transfer parked registry owner under bucket→registry,
remove registry entry, invalidate savedFID, delete durable record in PUTFH+
DELETE_KEY_AT compound before original error completion. Final caller ref anchors
open until cleanup finish. Initial version leaked parked owner; VFS review caught
and root fixed. Fixture asserts actual record deletion OK (not ENOENT), successful
EA prefix preserved and exclusive reopen works, both live and deterministic park
between durable register and EA submission. Both cases pass focused3. OOM/backend
record deletion errors still logged best effort, no persistent repair queue yet.

Build1 passed, stream/persistent baseline smoke passed. Cache fixture initially
forgot enabling oplocks/leases (root fixed). Build2 raced namespace test typo
(agent corrected foreach API). Build3 passed, focused3 26/28 pass: new cache
shared-RH fixture break expectation under investigation; existing stream last
peer disconnect FILE_NOT_AVAILABLE described/fixed above. Extended3 2/2 and
cross3 12/12 pass. Await final fixture fix then rebuild/test all SMB. No commits,
no rollback injection after filesystem mutation; no model changes this wave.

Wave10 final validation: build4 full DebugASan PASS; focused4 all28 PASS;
SMB MBT4 all5 profiles94traces each PASS (~99sec); Extended3 all2 and cross3
all12 PASS (latter checks preceded final SMB-only source/fixture changes).
Total47 selected CTest entries pass. Final fixture fix requests RWH/full access
and asserts W before peer OPEN; RH legitimately survives share-compatible OPEN,
so there was no shared-grant production defect there. No model expectations or
suppression changes. Root and ext/specs diffchecks clean. All agents idle, no
builds/tests running. Authoritative docs/reviews/smb-compound-current-status.md
updated wave10 validated; source remains uncommitted. Main next conversions:
shared leases/CLOSE pending-member accounting; requested-grant CREATE; durable
CREATE/reconnect; specialized stream overwrite/EA/DOC; namespace identity gate
for reparse/replacing/directory rename. Backend transactions still not implemented.

## 2026-09-24 SMB wave11 stream overwrite and CREATE EAs

Next pass extends existing native uncached regular-base stream producer to
OVERWRITE/OVERWRITE_IF/SUPERSEDE and ExtA. No new VFS API. Base and stream OPEN
never truncate; OVERWRITE excludes CREATE on both, missing identities stay missing.
Typed OVERWRITE follows both ACCESS reservations/cache coordination, narrowing
restores requested lifetime rights. EA LIST/SET/REMOVE explicitly target retained
base_handle result, shared metadata semantics; both reservations remain provisional
through readiness. Same sequential case map, accepted-prefix errors and operation
capacity gate. Directory-base/reparse/caching/durable/DOC stream paths still boundaries.

New lifecycle wire tests exact one-compound CREATE/QUERY/CLOSE for overwrite,
new/missing base/fork actions, both-direction share denial and retained bytes,
base/sibling preservation, metadata/read-only narrowing and pre-OVERWRITE injected
error followed by independent OPEN within same submission. Stream ExtA tests base
visibility, case-preserving updates/delete-recreate and later invalid-EA prefix
with deny-all base+stream reservations withdrawn. No post-mutation finish rejection.
Build1/smoke1 pass; build2/28focused pass. Five94trace SMBprofiles, cross12 and
Extended2 validating now. Root owns changes/tests this wave, no agents spawned,
no commits or model expectation changes. Logs /tmp/chimera-smb-wave11-*.log.

Wave11 integration follow-up: all5 profiles94traces passed build2; cross12 and
Extended2 pass. Final source audit found old base DOS attrs must be checked before
OPEN_STREAM creates a missing fork/stamps shared attrs. Extracted shared pure
smb_create_overwrite_attrs_allowed; base completion checks READONLY/HIDDEN/SYSTEM
before stream mutation, normal post-open overwrite guard reuses same predicate.
Added metadata-only overwrite3dispositions×3attribute-bits regression: rejects,
missingstream remainsmissing, protectedbaseattrs preserved. Added1100-entry
streamEA budget test (:fork:$DATA rawname survives deferral +historyacrosschunks).
Build3 and all28focused pass. Final5profile rerun running in mbt3.log; cross2 and
Extended2 unchanged-source relevant checks remain valid. No backend/core changes.

Wave11 final: build3 DebugASan PASS; focused3 28/28 PASS; mbt3 all5profiles
94traces each PASS; cross2 12/12 and Extended2 2/2 PASS (before final SMB-only
DOSguard+fixture change). Total47 selected CTest entries pass. Current-status
doc updated validatedwave11. Diffchecks root/extspecs clean. No active builds/
tests, no commits. Remaining streams: directory-base, reparse options, requested
cache/durable/DOC. Shared lease CLOSE + requested-grant/durable CREATE and
complex namespace identity migration remain main broader conversion work.

## Wave12 four-agent pass in progress (2026-09-24)

User explicitly requested four agents for remaining SMB areas, then root combined
build/test/report. Root+3worker concurrency means launched create/close/namespace,
then directory/DOC as namespace/close completed; all four agents now launched.
No builds/tests by agents. No commits/reset/model suppression. Root owns testing,
CMake/shared integration. Current sources not yet built.

smb12_create: native opportunistic legacy II/EX/BATCH CREATE, ends batch to publish
accepted grant before suffix. Preallocated candidate, no-break/noalloc locked
admission may cap II/NONE, no postcommit command failure. RqLs/actualdurable and
OPEN_REQUIRING_OPLOCK remainfallback. DHnQ when guaranteeddeclined maycoalesce.
Reply snapshot aftergrantselection. Migrated all3legacycoalesce/acquire calls to
atomic membership APIs from closeagent. New create_cache probe+inspect registered
byroot. LegacyEAadapter fixture switches fromBATCH toRQLS toretain realfallback.

smb12_close completed: regular nonstream/nondirectory/nonDOC nondurable RqLs CLOSE
terminal compound. Atomic coalesce_member/acquire_member VFS APIs attach member
under samefilelock asgrantref; solves lastoldCLOSE vs pendingnewmember gap. New
prepared oplock publish helper forcreate. Root review requested capdirectlyII
(no hiddenRH grant mappedtoII); implemented. Root fixed grant_remove_member in
smb_internal.h to reanchor claim.op_handle to surviving member orNULL; otherwise
freed original handle address could be reused causing false self exemption.
Tests existing claim_access +cache_close fixture+inspect, no newregistration.

smb12_namespace: native nonreplace regularbase RENAME withstreamholders. Updates
bothstream/base namespaceparticipants; peerpath lock orderregistry→base→stream.
Readonly/noopretry and heldprivateOPEN +rename fixtures, hardlinkalias/crossparent/
chain andstreamDOC. Root reviewfound closedDOCowner pendingstreamdelete action
retainsold parent/name; agentresumed to repathcachedaction on acceptedrename before
publicpeerpathfilter (survivingpeer mayuseanotherhardlink). New helperownedbythis
agent in smb_doc_stream.[ch]; directoryagent informed. Docs smb-stream-holder-
rename-compound.md. Directory/replacing/reparseidentity boundariesunchanged.

smb12_doc_stream active: existingdirectorybase uncached namedstreams target;
actualattrs carrybase S_IFDIR andmarshalzerosEOF, so streamaware replymarshal
helper in smb_attr.h +legacy/native create/query_info/close callsites approved.
Agent must get CREATE ownershiphandoff fromcreateagent first. No VFSAPIexpected.
New smb2_directory_stream_probe registeredbyroot. DOC lifecycle progress may be
limited to namespaceagent pendingrecordfix; don'tclaim DOCfamilycomplete.

Root must wait allstable then build /tmp/chimera-compounds-build CCACHE_DIR=/tmp/
chimera-ccache. Run focusedregex plus vfs/claim_test (newgrantcore behavior), all5
SMB94trace profiles, cross12, Extended EA+rename-lock. ASAN_OPTIONS=detect_leaks=0.
Logs planned /tmp/chimera-smb-wave12-*.log. Existingcurrentstatus stillvalidated11
untilcombinedtestingdone. Keepcommentary60sec and finishallauthorizedwork.

Wave12 integration tests: build3 passes after fixture constant spelling and pointer
signedness fixes. Focused1 28/30 pass; new directory stream fails related WRITE
with INTERNAL_ERROR; rename fixture lacked named_streams config. Namespace agent
fixed config. Directory agent diagnosed producer I/O op in_handle remainedNULL,
VFS reopened PATH then rejected directory base type. smb_compound bind prepare
now explicitly assigns resolved producer READ/WRITE handle; root addedstateNULLguard.
Directory agent additionally fixed legacy reconnect STREAM type/size normalization
and added durable directory ADS reconnect fixture. Build4 pending.
All5 MBT profiles fail same new native caching grant behavior (17mismatches/profile,
modelNone vswireII and laterunexpectedbreak). CREATE agent investigating, owns
CREATE+cachefixture again. Do not suppress model. Cross1 12/12, Extended1 2/2,
Extended claim1 1/1 pass. claim_test registered only -C Extended, so focused30 not31.
All test processes done, no builds running at this checkpoint. Logs wave12.

Wave12 near-final: build5 full DebugASan PASS; focused2 all30 PASS. SMB MBT2 all5
profiles94traces PASS; finalMBT3 running after root locked legacy grant report
block. Extendedclaim1 +Extended1 two +cross1 twelve passedbuild3, laterchangesSMBonly.
Directory producer READ/WRITE binding fixed and directory probe passes incldurable
reconnect. Rename probe passes after configenable and respecting existing mixed
streamCLOSE/baseDOCfence boundary (fourstreamclosescoalesce, baseclosedseparately).
CREATE agentfixed17MBTmismatches: snapshot sameclient cache cap beforephase2
recall, intersectwithlivecap atacceptedpublish, resetperattempt. Addedall3overwrite
regression withsameclientRWHbreak/ACK andNONEgrant. Atomicattach nowpreinitializes
memberwiretag (LEASEorattemptedlegacy mode) soearlybreaknotwrongformat. Rootlocked
legacyreportcachingblock againstbreak; owncache locknestedremoved. Allsource stable.
New confirmedpreexistingresidual: legacyRqLsv2 initializes grant epoch AFTERcore
insertion withoutlock, canclobberconcurrentbreak/racingcoalescedgrant. Futurefixseed
clientepoch onlyfornewgrant insideatomiccoreacquisition, nevercoalescedgrant. Not
partofnewuniquelegacyoplock nativepath. Documentedcurrentstatus remainingwork.
FinalmustwaitMBT3done thenmarkdocsvalidatedwave12+50selectedentries, appendfinalmemory,
reportfourboundedprogressareas anddurable/sharedRqLsCREATE/complexnamespace/DOC
boundaries accurately. No commits. Diffchecks clean. Onlyactive session72916 MBT3.

Wave12 FINAL: MBT3 all5 profiles94traces PASS onbuild5; all50selectedCTestentries
pass (30focused+5MBT+12cross+3extended). Finalcurrentstatus validated12; regression
fixes retained, no model/trace suppressions. Allagentscomplete, nobuild/testactive.
No commits. Futurework priorities sharedRqLs/actualdurableCREATE (includingatomic
v2epochseed), specializedDOC/directory/cachedstreamCLOSE +mixedfenceboundaries,
replacing/directory/streamrename andSET_REPARSE identitymigration.

## Wave13 in progress (2026-09-24)
User requests another agent round on remainingSMB items. Spawned3agents:
smb13_create CREATE+VFSgrantcore/tests, fixatomicv2epochseed first then bounded
sharedlease/durableCREATE; smb13_close specializedCLOSE/DOC includingstreamDOC;
smb13_namespace complexrename/reparse nextsafe slice. Agents no builds/tests,
CMake/rootcommonruntimeheaders ownedroot, sourceownership explicit. Baselinewave12
all50selectedCTest pass, build /tmp/chimera-compounds-build DebugASan,
CCACHE_DIR=/tmp/chimera-ccache, ASAN_OPTIONS=detect_leaks=0. Rootmustintegrate/review
thenbuildtestall, fixfailures, reportexactconverted/residual andupdatememory/docs.
No backendrollback; no finishrejectionaftermutation. No commits/modelsuppression.

Wave13 source scopes chosen: CREATE native regular-base RqLs requested READ/NONE
including stronger same-key joins, higher requests/durable remain boundary;
CLOSE native nonDOC directoryleases +cachedstreams, terminal batch stillrequired;
namespace ReplaceIfExists when destinationabsent viaatomicNOREPLACE, EEXIST
fallback untouched toexistingreplacement lifecycle. Rootreview foundcandidate
publication gap in oldgrantacquire: claimvisible beforegrantlistdedup, breakerpin
could survive unconditionalfreeofracingcandidate. CREATEagentclosing withsinglelock
coalesce/admission/claim+grantpublication alongsideepochseed. RootwilladdExtended
enforce_test duecoretryacquire refactor. CLOSEagentstable3sourcefiles+designnote,
no newfixturetargets. Rootno builds/tests yet; other2agents active.

Wave13 integration: partialbuild1 cacheclose failed unusedstate in CREATErecallpoll;
rootremoved, build2 close+rename passes. Smoke1 fixturefailures fixed byagents:
rename replacementtarget setup beforeDOCwatch; directorychild wirepath usebackslash.
Build3 passes; smoke2 renamePASS, directoryCLOSE casesPASS but stream2ndsamekeyOPEN
INVALID_PARAMETER beforeOpenStream. Existing legacykeybinding checkedbaseFH.
CREATEagent fixing skipbasecheck +nonmutating streamlookup whenkeybound +actualstream
FHvalidation beforemutation; CLOSEagentadded wrongstream/basekeyreusetests and
missingstream4creatingdispositions noartifact checks. Rootalsofound nativeRqLs
missingfileidentitykeycheck (agentadding) andreplysnapshotgrantpointer UAF risk
CREATE→suffixCLOSE beforewireencode (agentadding ownedversion/grantsnapshot).
CREATEagentcorefix now single-lock coalesce/admission/claim+grantinsertion, epoch
seedprefirstvisibility. Corefixture32simultaneousfirstacquireraces+breakepochseed.
MBT1 running againstbuild3 binaries session56212; no rebuilduntilfinish. No other
tests active. Await CREATEfinalcheckpoint then fullbuild and51selectedtests
(30focused+5SMBprofiles94each+12cross+4extended inclenforce). No newtargets.

## Wave13 final validated checkpoint (2026-09-24)

Three agents completed; root integrated and tested. No active builds/tests or
agent work remains. No commits, model expectation edits, or trace suppressions.

- Native regular-file RqLs READ/NONE CREATE/OPEN/OPEN_IF requires NON_DIRECTORY,
  excludes parent-key, streams, overwrites and durable contexts. Same-key joins
  retain stronger existing grants, original version/epoch and break state.
  Accepted-only grant publication; CREATE still ends the VFS run. Reply version
  uses an owned snapshot so suffix CLOSE cannot free data needed for encoding.
- Grant core seeds initial epoch before visibility, never overwrites coalesced
  epochs, and atomically publishes claim+grant with same-key deduplication.
  Removes candidate-free-versus-breaker-pin race discovered in root review.
- Native cached-stream and directory-lease CLOSE, including shared membership,
  uses terminal acceptance and existing stream/base fences and ACCESS journals.
  DOC, durable/persistence, notifications and mixed-fence boundaries remain.
- ReplaceIfExists regular-base rename now coalesces when destination absent;
  atomic NOREPLACE EEXIST defers untouched command to legacy replacement after
  accepted prefix. Actual replacing/directory/stream rename and reparse remain.
- Legacy stream lease validation now checks stream FH, not base FH. Bound-key
  nonmutating OPEN_STREAM preflight reuses exact handle and rejects wrong-key
  creating dispositions before fork creation/metadata changes. Native lease
  checks retain different-file rejection and pre-create missing-name guard.

Validation: /tmp/chimera-compounds-build full Debug+ASan build4 PASS; focused
30/30, Extended4/4 (including enforce_test), cross12/12, SMB5 profiles x94 traces
PASS =51 selected CTest entries. Logs /tmp/chimera-smb-wave13-build4.log,
tests1.log, extended1.log, cross1.log, mbt2.log. Root/extspecs diffcheck clean.
Fixture fixes: child backslash paths, replacement target setup before DOC watcher.
Source failures exposed/fixed: stream same-key OPEN rejection, missing native key
identity checks, reply grant-pointer lifetime; core epoch/publication races fixed.

Authoritative docs/reviews/smb-compound-current-status.md is validated wave13;
new slice docs smb-read-lease-create-compound.md, smb-specialized-cache-close-
compound.md, smb-replace-if-exists-compound.md. Remaining correctness noted:
session-wide lease-key scans lack atomic client/key reservation across concurrent
files; legacy replacement onto same-inode hardlink is backend no-op but republishes
wrong source cached path (needs backend atomic no-op result). Earlier reparse
identity migration, durable-record cleanup fault repair, destination ancestor/share
path translation, and memfs dotdot lock ordering remain. Backend rollback absent.

## Wave14 in progress
User requested another round. Started smb14_create (higher RqLs native CREATE)
and smb14_close (nonpersisted durable CLOSE). New namespace spawn hit the thread
limit, so reactivated completed smb13_close as the namespace worker. Namespace
owns VFS rename APIs, request SDK fields/caps/version, compound rename plumbing,
and memfs/tests; target is actual regular-file replacement without participants,
using atomic MATCH_DEST_FH and explicit no-op outcome for hardlink path repair.
Root owns common SMB headers/runtime, CMake, review and all builds/tests.
Baseline wave13 full Debug/ASan build and 51 selected CTest entries pass.
Workers do not build/test/commit. Backend rollback remains absent.

CREATE and CLOSE source checkpoints ready; namespace still underway. CREATE
supports all regular nontruncating RqLs modes and fixes reply snapshot race by
copying private fields before visibility and cache fields under the file lock.
Key identity assessment: session-only scans miss sequential cross-session key
reuse as well as concurrent same-session scan/publication races; shared client/key
reservation must span native/legacy/stream/durable lifetimes. Lease ACK discovery
also scans current session only. CLOSE atomically detaches live/parked durable
ownership on acceptance; regular nonstream nonpersistent cases only.
Early build1 failed rename gate scratch static assertion after outcome pointer
was added. Namespace worker is fixing layout; no wave14 tests run yet.

Wave14 early checkpoint build2 full Debug/ASan PASS after rename gate context
packing fixed static assertion without enlarging scratch. Four CREATE/CLOSE/
lifecycle/durable probes PASS; five SMB profiles x94 traces PASS (mbt1).
CREATE fixture now covers outstanding-break RWH join and forceII/disabled
policy modes; reply snapshot race fixed. Root added rename outcome field to
smb_internal.h. Namespace native target-replacement work still in progress;
early tests are not final wave14 validation. No commits/model suppressions.

Wave14 namespace review rejected broad distinct-target replacement enablement:
ACCESS fencing only prevents admission while held. A legacy CREATE can acquire
an old target handle after the empty scan, pause before admission, then admit
and publish a stale destination path after replacement releases the fence.
Native pending_begin also needs matching path admission protection. Legacy lacks
prebackend pending registration; inode edit guards do not provide that globally.
Worker is retaining distinct occupied-target boundary and removing experimental
unused guards. Safe slice: source+destination matched same-inode alias NOOP,
legacy atomic outcome suppression of path updates/notifications, backend tests.
This is not a fully converted ReplaceIfExists lifecycle. Early cross12 and
Extended4 tests pass too. Final build/test still required after namespace edits.

Wave14 full build4 passed. Final initial run: cross12, Extended4 and SMB5x94
passed. Focused tests2: 28/30 passed; VFS compound_groups NOOP notification
regression exposed second publication path in vfs_notify synchronous gate.
Namespace worker added NOOP suppression there too. Rename probe forced-legacy
RENAME+QUERY expected two native groups, but only QUERY is native; root added
send_runs_groups and exact one-group assertion. Source review additionally marks
initial successful NOREPLACE move/path completion immediately, so cancellation
of optional replacement tail cannot lose accepted-prefix publication. Build5 and
retests still required. No test weakens actual NOOP notification assertion.

## Wave14 validated checkpoint
Full Debug/ASan build5 PASS. Final focused 30/30, Extended 4/4, cross-frontend 12/12,
and five SMB profiles with 94 traces each PASS: 51 selected CTest entries and
470 trace replays.
Logs /tmp/chimera-smb-wave14-{build5,tests3,extended3,cross3,mbt3}.log.
Targeted rerun smoke2 of both previously failing regressions also PASS.
Root and ext/specs diff checks clean. No commits/model suppressions.

Delivered: regular nontruncating NON_DIRECTORY RqLs modes 0..7 CREATE/OPEN/OPEN_IF,
accepted upgrades/caps and locked reply snapshot; regular nonstream nonpersistent
durable-v1/v2 terminal CLOSE with atomic live/parked owner retirement; native
same-inode alias ReplaceIfExists plus VFS MATCH_DEST_FH and UNKNOWN/MOVED/NOOP
outcomes (memfs, SDK version 4), preserving legacy cached source paths and suppressing
ordinary AND synchronous-gate no-op notifications. Initial moved-rename completion
now stages its accepted prefix before optional tail cancellation can skip it.
Dedicated SMB cancellation at that new tail remains a documented test gap.

Distinct-target replacement was deliberately NOT enabled: delayed legacy OPEN
admission lacks prebackend lifetime/path registration. Hintless/truncating/directory/
stream lease CREATE, parent keys, durable/reconnect/AppInstance CREATE, persisted
and complex DOC/notification CLOSE, complex namespace/reparse and terminal cache
batch boundaries remain. Global client/key reservation remains missing (current
session-only scan also misses sequential cross-session conflicts). Backend rollback
remains absent. Authoritative docs/reviews/smb-compound-current-status.md now records wave14;
slice reports: smb-regular-lease-create-compound.md, smb-specialized-cache-close-
compound.md, smb-replace-if-exists-compound.md.

## Wave15 started
User requested another round. Reused smb14_create, smb14_close, smb13_close
(namespace) workers. CREATE targets hintless/truncating regular leases; CLOSE
nonpersistent durable directory/stream lifecycle; namespace investigates next
bounded slice plus optional-tail cancellation regression. Suggested replacing
hardlink requests with absent targets, preserving distinct-target lifecycle
boundary. Root owns combined build/tests/review/common headers/runtime/docs.
Baseline wave14 build5 and 51 selected tests pass. No wave15 validation yet.

Wave15 source checkpoints: CREATE hintless/all-disposition regular leases and
CLOSE nonpersistent durable directory/stream support ready. Root review fixed
new preflight error ordering: missing hintless noncreating OPEN/OVERWRITE must
retain NAME_NOT_FOUND even if LeaseKey is bound elsewhere. Agent added regression.
Namespace production now accepts hardlink ReplaceIfExists via atomic no-replace
LINK, defers EEXIST before mutation, and maps rename cancellation to CANCELLED.
Its cancellation hook wraps private first-rename completion, avoiding use after
synchronous compound completion. Namespace fixture work still underway.

Root production-only build1 and CREATE/CLOSE/lifecycle fixture build2 PASS;
four smoke1 tests PASS. Broader wave15 mbt1/cross1/extended1 running on stable
production binaries. Final namespace fixture build and focused tests pending.
Logs /tmp/chimera-smb-wave15-*.log. No workers build/test/commit.

Wave15 integration update after user continued: all agent source checkpoints
preserved; no agents remain active. Namespace added runtime LINK root binding
from smb_doc_command_path/private producer view after root identified a cold-tree
construction-time FH risk. Full build3 failed fixture field name complete_private;
root corrected callback_private. Build4 passed, metadata probe passed, cancellation
probe exposed distinct prepare/completion contexts. Root preserved both contexts
with set_op_callbacks then set_op_prepare. Full build5 and both targeted smoke3
regressions pass. Final focused30, Extended4, cross12 pass; SMB mbt2 still running.
Namespace slice documented in smb-hardlink-replace-compound.md. Current-status
is wave15 integration pending final replay results. No new production edits after
build4; build5 only fixture correction. No model suppressions/commits.

## Wave15 validated checkpoint
Full Debug/ASan build5 PASS. Focused 30/30, Extended 4/4, cross-frontend 12/12,
five SMB profiles with 94 traces each PASS: 51 selected CTest entries and 470
trace replays. Logs /tmp/chimera-smb-wave15-{build5,tests1,extended2,cross2,mbt2}.log.
Build4 was the full integration rebuild; build5 includes the final fixture fix.
Root and ext/specs whitespace checks clean. No commits or model suppressions.

Delivered regular RqLs all six dispositions with/without NON_DIRECTORY, runtime
directory deferral and post-open type revalidation, correct missing noncreating
name/key error precedence; nonpersistent durable directory/stream CLOSE with exact
base ACCESS/handle identity validation and 12 lifecycle regressions; native
ReplaceIfExists hardlink when absent with atomic EEXIST fallback, cold-tree private
producer root binding, and dedicated accepted-rename/canceled-tail regression.
Cancellation fixture required callback_private field and preserving independently
wrapped prepare context; production rename maps EINTR to STATUS_CANCELLED.

All assigned slices completed and assessed. Distinct occupied-target rename/link,
directory/stream leases, parent keys, mandatory/create-DOC/durable/reconnect CREATE,
persisted/notify/stream-base DOC CLOSE, complex namespace/reparse, tentative cache
suffix state, client-wide key reservation and backend transactions still remain.
Stream DOC requires a private last-holder/deletion journal; current allocating
public retirement callbacks cannot be reused as retryable completion callouts.
Authoritative report is docs/reviews/smb-compound-current-status.md, wave15.
New reports: smb-hintless-overwrite-lease-create-compound.md and
smb-hardlink-replace-compound.md; specialized cache CLOSE doc has wave15 section.

## Wave16 started (2026-09-25)
User requested another round. Three workers: smb16_create converts bounded
SMB3 v2 directory leases; smb16_close targets persisted CLOSE after finding
pending notification ownership/thread affinity needs broader work; smb16_namespace
converts directory-rename child enumeration to retry-safe typed READDIR compounds
(prerequisite, not full directory rename conversion). Root adds typed CREATE
symlink/mknod NO_NOTIFY propagation and observer/cache regressions as a reparse
prerequisite. Workers do not build/test. Root build1 running; final integration,
assessment and validation pending. Backend rollback absent; no rejected mutation.

## Wave16 validated checkpoint
Full Debug+ASan build3 PASS; build4 final CLOSE fixture correction PASS. Focused
30/30, Extended4/4, cross12/12, SMB5 profiles x94 traces PASS: 51 selected CTest
entries, 470 SMB replays. Logs /tmp/chimera-smb-wave16-{build3,build4,tests1,
extended1,cross1,mbt1}.log. Production unchanged after build3; build4 only fixture.
Root/ext-specs diff checks clean. No commits/model suppressions/rollback injection.
All workers completed bounded assignments; root reviewed and integrated.

Delivered:
* Native SMB3/v2 directory lease CREATE/OPEN/OPEN_IF, explicit and hintless.
  Actual type/policy gates, preallocated accepted DIR_LEASE publisher, W masking,
  same-key join/upgrade/rearm, DIR_CONTENT recall; own-break pin recognizes dirs.
  Tests modes0..7, prefix/terminal grouping, held mkdir acceptance, safe read-only
  upgrade retry, wrong-key absent dir, child break/rearm, caps, v1/disabled/parent
  key fallback. SMB2 dialect fallback only source audited. Stream leases, parent
  keys, durable/DOC/mandatory CREATE still boundaries. Claim helper accepts DIR_LEASE.
* Persisted CLOSE typed RETIRE -> exact handle -> DELETE_KEY_AT -> CLOSE, scope
  prevents cancellation after retirement skipping deletion; accepted-only public
  teardown/logging. Deletion preserves legacy best-effort failure semantics;
  EIO leaves record and still needs repair/tombstone guarantee. Eleven real-record
  cases regular uncached/lease/BATCH/hintless, existing-directory FILE_OPEN,
  parked, cancel after RETIRE and DELETE, missing/EIO, warm directory reconnect.
  New persistent cases isolated in separate CA environment from rejection tests.
* Directory rename discovery uses typed streaming READDIR per page, private
  count/deny checkpoint reset, retained handle, accepted-only recall/progression.
  Initial-scan OOM/missing-FH denial bypass repaired. Five tests cover retry after
  child closes, terminal EIO/EINTR, surviving child, multipage accepted-prefix
  retention. This is a prerequisite, NOT native full directory rename.
* Root added mknod_at_flags/symlink_at_flags (old APIs delegate zero flags), typed
  name-based CREATE forwards namespace_flags, both ordinary and synchronous
  notify paths suppressed by NO_NOTIFY while auth/caches active. Flags fill
  request/gate padding; existing request field offsets unchanged, SDK stays4.
  Tests root/unprivileged success, denial, sync/ordinary watchers, held accepted
  finish and lookup identity. Reparse identity migration still NOT converted.

Integration: build2 const qualifier fixture failure fixed by root. Initial
cache_close probe exposed existing persistent CREATE bug: fresh directory mkdir
and named stream CREATE can advertise DURABLE_PERSISTENT but no persist_pid,
PERSISTED flag or backend record exists; registry entry->persistent false makes
persistent reconnect fail INVALID_PARAMETER. Test retained strict actual-record
checks, changed directory to precreate+FILE_OPEN, replaced stream cases with actual
persisted regular variants. No synthetic records used to claim stream coverage.
CLOSE generic stream path can consume a persisted stream, but production stream
persistence is currently broken/unverified prerequisite. This is a known gap.

Additional preexisting known race: CHANGE_NOTIFY may install after native CLOSE
eligibility snapshot, and accepted CLOSE does not reap late watch. Suggested
prerequisite: notify admission via doc_mutation_begin registry guard + synchronized
closed-FileId validation; defer on CLOSE fence; CLOSE recheck notify after fence
acquisition and defer untouched if appeared. Audit bucket/registry/watch order and
worker-affine parking/cleanup; not implemented here. Pending notify boundary alone
is NOT sufficient protection. Retain as high-priority next work.

Authoritative report docs/reviews/smb-compound-current-status.md now wave16
validated; new slice reports smb-directory-lease-create-compound.md,
smb-persisted-close-compound.md, smb-directory-rename-scan-compound.md; reparse
prerequisite doc updated. Remaining: stream leases, parent keys/specialized CREATE,
notify/legacy-lock/complex DOC CLOSE, occupied replacement, directory/stream rename,
reparse identity migration, terminal grant suffix, global lease-key reservation,
other backend capabilities/transaction support and listed correctness gaps.

## Wave17 started: two correctness gaps
User requested the last two issues from wave16 report. Reused smb16_create for
persistent grant truthfulness and VFS record-write failure propagation; reused
smb16_close for notify admission/lifetime against native and legacy CLOSE.
No worker builds/tests; root reviews/integrates. Persistence stream/fresh-dir
records lack full cold identity support: correct bounded fix declines PERSISTENT
without a proven record while preserving valid ordinary durable grants. Do not
claim full cold recovery. Notify design uses owning-thread admission timers on
existing parked-notify objects, explicit state refs, synchronized detach and
CLOSE fence recheck; no shared request async timer needed. Validation pending.

## Wave17 implementation and integration findings
Persistent grant truthfulness now requires actual successful record storage;
unsupported fresh-directory, directory OPEN_IF and stream routes decline
PERSISTENT but preserve eligible ordinary durable/warm reconnect. Default-KV
OPEN_AT writes only after handle/type/access admission, propagates PUT failure,
releases the acquired handle and preserves the successful filesystem prefix.
Handle-state r_created output captures that prefix for suppressed SMB notify;
module SDK is now5. Unpublished successful/ambiguous records delete via typed
cleanup compound + frontend retry adapter before terminal CREATE error.
Allocation/deletion failure repair/tombstone guarantee still absent. Full cold
identity recovery and generic OPEN_FH cached/inferred persistence remain gaps.

Notify now holds session authorization through watch attachment publication,
then namespace DOC guard -> open bucket -> state lock. Native CLOSE rechecks
after its fence; if a watch won first it defers untouched. Late watch admission
parks with owning-worker timer; no watch is installed under held fence. CANCEL,
disconnect and invalidated sessions terminate admission safely. State refs and
explicit tree pins survive logical teardown; every queued/already-ready request
is cleaned on its own worker. PreviousSessionId now detaches/flushes before
parking durables; recovered handles can attach fresh watches. Already admitted
SMB3 LOGOFF sibling-channel notifies still need separate flush/coverage. Full
notification-bearing CLOSE coalescing remains a deliberate boundary.

Root integration found/fixed: missing dynamic-symbol visibility in internal test
hooks; VFS OPEN_CURRENT owns cursor so test needs GETHANDLE before take_handle;
and REAL stale request union bug: previous r_open_file bypassed unpublished-key
cleanup. Request allocator and legacy handler now clear it; failed-PUT wire
mapping explicitly returns IO_DEVICE_ERROR. Smoke2 all4 PASS including actual
record cleanup, denied-DAC no-store, PreviousSessionId and warm fresh notify.
Build4 Debug+ASan PASS; focused31/31, extended4/4, cross12/12 PASS. SMB5 still
running at this checkpoint; final validation record follows. No rollback
injection after real namespace/KV mutation; no model suppressions changed.

## Wave17 final validated checkpoint
Full Debug+ASan build4 PASS. All52 selected CTest entries PASS: focused31,
extended4, cross12, SMB5 profiles x94 traces =470 successful trace replays.
Smoke2 all4 PASS (subset of focused). Logs /tmp/chimera-smb-wave17-{build4,
smoke2,tests1,extended1,cross1,mbt1}.log. Production unchanged after build4.
Root/ext-specs whitespace clean; no commits or model expectation changes.
Current authoritative report docs/reviews/smb-compound-current-status.md wave17;
slice reports smb-persistent-create-truthfulness.md and
smb-notify-close-admission.md. The two requested correctness gaps are repaired
with truthful grant downgrade and synchronized notify lifetime/admission; do
not claim full persistent cold recovery or fully native notification CLOSE.

Wave17 wrap-up source reassessment: distinguish native operation coverage from
wire-to-VFS coalescing; caching CREATE/CLOSE remain ends_batch and deadlock/session/
tree/capacity splits remain. Critical dependencies are prebackend pending-open
admission/stale-identity exclusion (occupied replacement/directory moves), private
cache/durable/DOC state + client-wide lease-key reservation (specialized CREATE
and suffix coalescing), and FileId identity migration (SET_REPARSE). Additional
explicit gap confirmed in smb_proc_lock.c: native LOCK supports local exact-range
owners; projected/legacy locks and their CLOSE still fall back. Typed stream
rename absent. Current-status report now includes wrap-up dependency assessment.

## Wave18 wrap-up fleet launched
User authorized plan and farm-out of remaining SMB areas. Running three lanes:
smb18_namespace (constructor admission + bounded distinct-target replacement),
smb18_create (client-wide lease-key reservation + specialized CREATE progress),
reused smb16_close (FileId identity migration + bounded SET_REPARSE). New reparse
spawn hit total thread limit, so reused idle worker. Root owns common headers/
runtime/CMake/build/test and validation. Plan is in smb-compound-agent-plan.md
wave18 section; later CLOSE/coalescing/directory-stream/projected-lock work is
dependency-gated, not claimed complete by this launch. No worker builds/tests.

Wave18 source correction to prior report: vfs_claim_range_is_local returns true
for every SMB2 claim without an existing backend token; projection currently
only applies to POSIX. Thus projected SMB lock mode is not a currently reachable
conversion blocker; legacy lock_entries migration remains. Do not spend a work
package implementing an imaginary active backend SMB projection mode.

## Wave18 implementation and integration findings
Three-lane fleet implemented first dependency batch, not whole SMB completion:
- namespace constructor reader/writer tokens now protect native occupied-target
  replacement of an unheld regular destination (strict memfs FH match/outcome).
  Live-target/directory/stream/occupied LINK and legacy delayed-admission race remain.
- client-wide LeaseKey reservations span unpublished/native/legacy constructors,
  active opens, durable park/warm reconnect, and final retirement. Same-client
  ACK searches all authorized sessions with tree memory pin. Bound keys force
  existing-only OPEN/MKDIR skip after lookup, preventing recreation after unlink.
- ParentLeaseKey native regular/directory CREATE enabled; caching CREATE still
  terminal. Fixture exposed key-only notification actor lacked proto/client key.
  Full-actor API now used by all direct SMB emitters (including deferred stream
  DOC origin snapshot). Legacy VFS rename/link/remove key-only emitters remain a
  correctness follow-up; do not claim every legacy ParentLeaseKey path repaired.
- Restricted SET_REPARSE standalone typed compound migrates canonical handle,
  ACCESS owner, file state and namespace only on accepted ready completion.
  Bucket-protected identity gate covers native/legacy resolution, direct CLOSE,
  replay/AppInstance pins. Stateful/nonregular cases still legacy; no wire-wide
  tentative rebind overlay. Request/session/tree pins and standalone CANCEL/
  disconnect tracking retain callbacks. Late cancel at read-only finish must
  suppress legacy mutation even if VFS cancel refuses while finishing.
- Review fixed bucket->trees ACK inversion, pin-before-FileId-resolution gap,
  canceled fallback mutation and cancellation-scope reservation retirement bug.
  Do not mark reparse reservation until ready using generic reserve_until: group
  EINTR after successful irreversible suffix can withdraw needed output. Here
  the standalone compound owns cleanup of untaken output with fences held.
- Unprivileged CHR/BLK SET now denies before unlink, fixing known destructive
  failure. Successful privileged device conversion remains untested; actual
  FIFO/socket/symlink migration and denial preservation covered.
- VFS CREATE callback now tolerates successful mutation returning NULL attrs;
  frontend reports missing identity and preserves actual accepted prefix.
- Initial SMB MBT showed two model defects: cross-session and parked durable
  same-client wrong-file key reuse was allowed. Verified ClientGuid/LeaseKey
  rule in MS-SMB2 lease-v2 handling. Root corrected live clientTag + parked owner
  membership before namespace effects/purge, added two deterministic model tests,
  passed 3 model selftest modules and regenerated all 94 traces. No trace edits,
  suppressions, or production weakening to satisfy stale expected results.
Validation final results will be appended after the active five-profile replay.

Wave18 regenerated MBT then exposed a real latent actor bug (durable trace
0x41_5 state215): a permitted nonlease DH2C reconnect moved BATCH open to another
ClientGuid, and common compound prepare rebuilt the I/O actor from the new
session. Its WRITE recalled its own retained BATCH grant. Root now uses canonical
ACCESS owner plus grant key bytes in primary and secondary-file preparation;
this keeps FileId RANGE identity (do not replace full owner with shared lease
grant owner). Worker updates legacy READ/WRITE and adds deterministic cross-client
BATCH reconnect with an exclusive range followed by READ/WRITE/unlock/no self
break. Final validation is pending these changes; earlier build6 alone is not
final proof.

Wave18 FINAL VALIDATED: full Debug+ASan build8 PASS; focused31 tests3 PASS,
extended4 extended2 PASS, cross12 cross2 PASS after VFS actor API, SMB5 mbt3 PASS
(94 regenerated traces/profile, 470 total). No production changes after build8.
Three SMB model selftest modules + regenerated94 passed model1. Targeted
create3/durable3 tests confirm cross-client retained RANGE/BATCH actor repair.
Logs /tmp/chimera-smb-wave18-{build8,tests3,extended2,cross2,mbt3,model1}.log.
Current inventory rewritten in docs/reviews/smb-compound-current-status.md;
plan remains in smb-compound-agent-plan.md. First three-lane wave done, queued
CLOSE/LOGOFF, tentative grant suffix coalescing, broader directory/stream/reparse,
legacy parent-key VFS plumbing, and persistence recovery/repair remain explicit.
Root and ext/specs diff --check pass. No commits made.

## Wave19 continuation launched
User requested continue. Reused smb16_close for native notify CLOSE/LOGOFF,
smb18_create for legacy VFS full-actor ParentLeaseKey plumbing, smb18_namespace
for bounded broader namespace/directory rename. Root owns shared headers/runtime,
VFS compound/request interfaces and all build/test/review. No worker tests/commits.
Wave18 build8 + 52 selected tests/470 SMB traces is baseline.

Wave19 integration findings (final validation still pending): actor-aware legacy
RENAME/LINK/matched REMOVE and bounded root-child directory rename implemented;
notification-bearing CLOSE accepted cleanup and sibling-channel LOGOFF watch
drain implemented. Root fixed signed LOGOFF aggregate reply snapshots and stale
suffix dispatch, including SESSION_SETUP treating an embedded snapshot as a real
channel-table handle. Deferred notification now pins both tree and session memory.
Prior watch deletion is snapshotted at cleanup cutoff so own-DOC delete cannot
override NOTIFY_CLEANUP. Full build4 passes; extended4, cross12 and SMB5 (470
traces) pass. Smoke1 caught incorrect public directory CLOSE grouping expectation
(fixture corrected, real boundary documented) and absent native CLOSE wire-CANCEL
routing (namespace agent implementing count1 CLOSE registry; close fixture now
holds real running COORDINATE before retirement). Do not call these final tests.

Independent actor audit found a remaining legacy LOCK issue: take_one sets
RANGE client_key=session_id; other old paths use current session instead of
canonical ACCESS owner. Native SMB locking normally masks this (projection is
POSIX-only), but native batch allocation fallback or retained lock_entries can
reach it and cause wrong conflicts through a same-lease peer, including after
cross-client nonlease durable reconnect. Minimal follow-up: canonical
chimera_smb_open_actor_owner for both legacy owners + break actor and a forced
fallback regression. Not fixed by wave19 ParentLeaseKey plumbing.

Wave19 FINAL VALIDATED: full Debug+ASan build9 PASS. Focused31 tests1,
extended4 extended2, cross12 cross2, SMB5 mbt2 (94 traces/profile, 470 total)
all PASS on that final build. Logs /tmp/chimera-smb-wave19-{build9,tests1,
extended2,cross2,mbt2}.log; standalone notify6 PASS too. ASan leak detection
disabled as usual. No wave19 model/trace edits or commits. Root and ext/specs
diff --check clean. Current inventory and all four wave19 slice reports updated.

Final scope: native notify-bearing CLOSE with accepted worker-owned cleanup;
session/tree pins for deferred secure notify; real multichannel LOGOFF watch
drain; per-wire LOGOFF reply snapshots and deleted-session/SETUP suffix rejection;
count1 native CLOSE wire-CANCEL registry with disconnect drain; full canonical
legacy RENAME/LINK/matched-REMOVE and CLOSE ParentLeaseKey actors; bounded native
same-parent root-child directory rename (global no-unrelated-open restriction).
Generic grouped cancellation and pre-submit waits remain excluded. Public
directory CLOSE after rename and directory DOC remain boundaries, now accurately
tested/documented; producer-relative directory OPEN/RENAME/QUERY/CLOSE coalesces.
Notify fixtures now synchronize exact nr_free completion on disconnect/TDIS and
explicitly close surviving deleted channels before fs teardown. Broader LOGOFF
open/claim retirement still follows session refcount lifetime; not solved here.
Other remaining work: general directory/stream/replacing namespace paths,
terminal cache/durable suffixes, specialized CREATE, stateful reparse, legacy
LOCK actor bug above, persistence recovery/repair, existing memfs '..' inversion.

## Wave20 in progress
User requested another pass with extra-high reasoning. Reused CREATE agent for
cached CREATE safe suffixes/private actor, namespace agent for root-sibling
directory admission with all RENAME/LINK reader tokens, LOCK agent for legacy
canonical actor+forced/natural fallback tests. Root owns runtime optional
private_suffix_eligible/private_actor_owner hooks, token lifecycle, effective
private peer paths, legacy namespace-wait cancel/disconnect integration, export
and CMake, and all build/tests. Preliminary production build1 passes. No full
wave20 validation yet. No backend mutation rejection/rollback simulation.

Wave20 integration: cached CREATE supports same-private-open related QUERY_INFO,
READ, FLUSH and RqLs WRITE, with private requested-key actor; CLOSE/legacy WRITE
and other lifecycle suffixes still split. Directory admission now allows exact
same-view root siblings, backed by all native/legacy RENAME+LINK reader tokens;
private helper inspects all acquired states via current-group path overlay.
Independent review caught FH-less COORDINATE placement (moved after pure
checkpoint+PUTHANDLE, before handler namespace ops) and legacy wait drain
dispatching suffix during disconnect. Root moved disconnect cutoff into generic
compound_advance and releases parsed WRITE/SESSION_SETUP payloads. Namespace
worker adding direct nondispatch test; CREATE entry exported only for interposition.
Build3 full PASS, focused_other1 29 PASS, create/cache+metadata+notify smoke PASS,
LOCK lock2 PASS after enabling durable fixture config (build4-lock). Rename new
failed-prefix fixture had incorrect expected success: first move genuinely
SHARING_VIOLATION due directory DELETE admission, second independent rename
success; worker correcting expectation without weakening production. Final full
wave20 rebuild/testing still pending. Source reports now include
smb-caching-create-private-suffix.md and smb-legacy-lock-canonical-owner.md.

Wave20 FINAL VALIDATED: full Debug+ASan build5 PASS; focused31 tests1,
extended4 extended1, cross12 cross1, SMB5 mbt1 (94 traces/profile, 470 total)
all PASS. Targeted CREATE/cache+LOCK+RENAME smoke2 also PASS on build5.
Logs /tmp/chimera-smb-wave20-{build5,tests1,extended1,cross1,mbt1,smoke2}.log.
ASAN_OPTIONS=detect_leaks=0. No production changes after build5, no model/trace
edits or commits. Root and ext/specs diff --check pass.

Exact delivered scope: safe related same-private-FileId cached CREATE suffixes
(QUERY/READ/FLUSH; RqLs additionally WRITE) with private requested-key actor;
directory rename allows exact same-view root siblings using native/legacy
RENAME+LINK reader admission; all acquired batch identities use private path
overlays; canonical legacy LOCK RANGE+recall actors with forced and natural
fallback, same-key and cross-client durable regressions; generic disconnect
suffix cutoff with direct legacy-CREATE nondispatch proof synchronized on
server completion. Reader wait supports CANCEL and teardown before FileId work.

Do not overclaim: cached CREATE/CLOSE still splits. Closed private slot currently
suppresses grant publication and returns NONE, losing same-key grant reply
mode/version/epoch; need reply-only accepted cache decision or private grant
journal preserving wire-order reply snapshots and final surviving membership.
Do not publish speculative live grants. Legacy lock_entries migration remains;
need atomic ownership/representation transfer, not release/reacquire or empty
range-owner construction. Directory rename remains memfs-only root-child,
same-parent, absent target, <=128 scan, no held children/deeper unrelated paths
or foreign views. Root symlink identity reasoning depends on current memfs
leaf-open behavior; future matched-FH backends need audit. The failed sibling
move fixture is SHARING_VIOLATION then independent directory SUCCESS, NOT a
successful earlier-public-move overlay test. Cross-protocol namespace races,
general namespace/reparse/durable/DOC, grouped cancellation, persistent repair
and old memfs '..' lock inversion remain as catalogued in current-status.

## Wave21: complete SMB LOCK/UNLOCK conversion (2026-09-26)
User requested the least difficult remaining area and a completion pass. Chose
legacy LOCK representation cleanup. Important reassessment: legacy lock_entries
have no persistence/recovery producer and are process-local; stopping all their
producers removes the need to invent live representation import for this source
conversion. Ordinary native LOCK already owns the full exact-range contract;
initial batch-allocation failure must return resources, not create legacy claims.
Root working directly; no new workers or commits. Removed old entry/list acquire,
unlock and completion queue; removed request/thread legacy fields and CLOSE gate.
Canonical cancellation reasons remain atomic; abort helper now bucket-protected
and never completes a request on the wrong worker. Tests changing forced fallback
to resource/no-effects checks and canonical owner inspection; adding atomic
multi-acquire rollback, partial multi-unlock retries, zero/end-byte geometry,
metadata/CLOSE coalescing and retained durable replay identity. Validation pending.
Wave20 source snapshots for this pass are in /tmp/chimera-smb-wave21-before.

Wave21 FINAL VALIDATED: full Debug+ASan build3 PASS; focused31 tests2,
extended4 extended2, cross12 cross2, SMB5 mbt2 (94 traces/profile, 470 total)
all PASS on build3. Independent Samba smb2.lock via temporary unshare network
namespace outside sandbox PASS: 23 success, 3 client platform skips (W2K8 NONE
bug, broken-Windows replay, CTDB-only), log torture2. Logs prefix
/tmp/chimera-smb-wave21-{build3,tests2,extended2,cross2,mbt2,torture2}.log.
ASAN_OPTIONS=detect_leaks=0; no production edits after build3, no model/trace edits
or commits. Root/ext/specs whitespace checks pass. New report:
docs/reviews/smb-compound-lock-completion.md; current-status inventory updated.

Local file-backed SMB LOCK/UNLOCK area COMPLETE: all legacy list producers,
entry/ticket code, resume queue/fields and native LOCK/CLOSE exclusions removed
(about 900 old production lines deleted). All actual ranges are journaled exact
owners. Allocation refusal reports resources without range or LockSequence
effects; preserve normal resolve/validation and related FileId inheritance.
Final review caught the need for that resolve, tested failed LOCK + related QUERY.
Canonical owner tests cover same-key peer, foreign client, cross-client durable
BATCH rehome; resource failure LOCK and UNLOCK; durable replay after allocation
failure; atomic batch conflict rollback; ordered partial unlock through EAGAIN;
exact zero/final-byte geometry; query/CLOSE with held ranges through retry.
Cancellation teardown helper holds open bucket and only atomically signals;
owning compound completes and releases references. Samba independently covers
cancel/close/tdis/logoff and durable/multichannel replay. No filesystem mutation
finish rejection. General grouped cancellation, cache CREATE/LOCK boundaries,
complex DOC/reparse and cold recovery remain in their own work areas.

Correct earlier memory's "legacy RANGE migration required": that was conditional
on keeping old producers. This binary cannot create/recover legacy entries, so
no in-process import contract is required. Do not reintroduce fallback claims
on allocation failure or claim cold lock recovery/live process migration solved.

2026-09-26 follow-up question about backend completion versus memory publication:
source review confirms claim reservation protection, NOT a universal atomic
backend/VFS/SMB publication boundary. RANGE acquisitions are linked provisionally
before finish; removals retain old public blockers, with private exclusions;
removed_by/busy/retire_journal protect edits and pins preserve storage. Publish
is per owner/file and access/range journals run sequentially before frontend cb.
SMB LockSequence snapshot is copied at smb_compound.c:390, checked privately in
smb_proc_lock.c:351, published later at smb_compound.c:542. Ordinary RANGE bind
permits multiple journals (pins are not mutual exclusion). Potential same-open
multichannel duplicate LOCK race: second worker reads old replay state before
first frontend publication, although the first range can already be accepted.
Not reproduced yet; don't call it verified runtime failure. Need deterministic
hold between range publication and SMB publication, plus concurrent-attempt
case. Conversion completion != proof of cross-layer serializability. Future
backend commit integration requires affected readers/editors to respect a
logical publication gate or validated commit version through accepted frontend
publication; callback ordering and lifetime pins alone do not provide that.

2026-09-26 checkpoint requested: user authorized committing all accumulated work
and pushing to the draft PR. Specs changes committed as 4e3bff6 and published
to chimera-nas/specs branch compounds-refinement. Parent checkpoint includes
that reachable submodule pin, all production/test changes, and review notes.
Existing draft PR #1692 uses compound-boilerplate, whose remote tip is
18035808f5aa53e05e1005893b5efb9edbd589da. It was independently force-updated:
local pre-checkpoint HEAD b54b6b32 and remote have 27/181 unique commits
(24 local commits are patch-equivalent), with substantial competing VFS and
frontend implementation changes. Do not force-push away that remote history
without explicit direction. Preserve this checkpoint as compounds-refinement
while resolving which implementation should occupy the existing draft PR.

## 2026-09-26 branch reconciliation history (superseded below)

User explicitly authorized preserving both diverged lines of work, pushing the
reconciled result to draft PR #1692 (`compound-boilerplate`), then rebasing that
PR on latest main including Windows. Worktree `/tmp/chimera-compound-reconcile`
(branch `compounds-reconciled`) starts from PR `18035808`; original worktree is
clean at checkpoint `21104a97`. Sources resolve the 106 cherry-pick conflicts,
but the index still needs staging and neither reconciliation commit nor PR push
has happened. Main rebase has not started. See
`docs/reviews/compound-branch-reconciliation.md` for decisions and provenance.
Reflog evidence supports a stale cached starting ref, not a remote rewrite time.
Debug build is `/tmp/chimera-reconcile-build`, ASAN_OPTIONS=detect_leaks=0.
CTest quick is 199 tests; extended includes the VFS and frontend focused probes.
Required netns configure/tests run outside sandbox. Latest build24 regenerates
JSON profiles: NLM upgradeIsNoop=false and NFS3/4 copyRange=supported. Earlier
standalone Quint profile edits alone do NOT change modern generated corpora.
Full quick run21 exposed shared causes across profiles; fixes/validation ongoing.

Reconciliation checkpoint before main rebase: Debug builds succeed; full quick
run still has failures in NLM blocking traces, all five SMB model profiles, and
POSIX-over-SMB. Do not report a clean suite. Fixed the NFS delegation fixture
which returned an empty filehandle and misread an unexecuted op as success.
NLM partial-unlock and partial-upgrade model self-tests pass (nine full-profile
tests); use an explicit EOF sentinel compatible with Quint Rust integer limits.

## 2026-09-26 main rebase and validation

The reconciliation checkpoint WAS pushed to PR #1692 as bc13021f. Older sections
above describe intermediate history. Integration worktree is
/tmp/chimera-compound-reconcile, branch compounds-reconciled. The PR history was
rebased onto main eb322605, including Windows, lock-stateid SETATTR and Cairn
metadata read-conflict tracking, and published with validation repairs as
989821ec. Two more main commits landed during final verification; the final
rebase targets c971e5a5 and retains its diskfs shutdown and claim-waiter fixes.
The original worktree now uses compound-boilerplate, tracking the PR branch.
Backup refs preserve original PR18035808,
checkpoint21104a97, reconciledbc13021f and the first main rebase.

Read docs/reviews/compound-branch-reconciliation.md for integration choices.
Main dependency pins: libevpl 8a3db7c7 and ndrzcc 451d11fc. Specs merge 8dfc579
combines main 7ac0dec with reconciliation b290aee. All 104 SMB selftests at 20
samples each and full corpus generation passed; the default 10,000 samples were
NOT completed. The user explicitly approved a specs PR. Specs 8dfc579 is pushed
and reachable in draft PR https://github.com/chimera-nas/specs/pull/33.
Use force-with-lease for root publication, checking the current remote SHA;
never overwrite an unexpected remote update.

Rebase regressions fixed: portable shared range arithmetic (carry bit; million-
case oracle passed); disjoint capability bits 30..37; optimized journal loop;
NFS exclusive-create verifier timestamps; filehandle OPEN authorization and
bound grants; lock-stateid SETATTR and retained I/O handle selection; SEEK from
write-only OPEN; pNFS missing GETHANDLE retention; nonregular OPEN error mapping;
SMB directory-stream READ type check; optimized fixture correctness; license
headers. Diskfs test helpers snapshot completion status before freeing the
compound, so an unrelated callback cannot change the value used to validate
whether outputs were initialized.

GCC Debug and Release builds pass, including after the final c971e5a5 rebase.
Full quick runs at 989821ec had 214 passed, 29 skipped and 30 failed out of 273
(same failing names; lease timing differs). These runs precede the last two
main commits. The final rebase passes 14 focused tests each in Debug/ASan and
Release: claims/journals, compound retries, SMB base and compound probes, NFS
lock replay, FUSE/POSIX locks, and diskfs model/smoke tests. Debug combines 13
passing checks from the main run with a separate smoke run. Seventeen earlier
range/locking/compound focused tests pass.
The additional final Debug leases_memfs_plain test timed out at 600 seconds
after reporting oplock-grant and missing lease-break mismatches; it remains
an unresolved failure. Logs: /tmp/chimera-latest-main-tests.log,
/tmp/chimera-latest-main-release-tests.log, and
/tmp/chimera-latest-main-diskfs-smoke.log.
NFS OPEN authorization, POSIX-over-NFSv4 batch/strict, five pNFS suites and three
SMB stream/reconnect probes pass. Earlier focused VFS, S3 and SDK checks pass.
make check was invoked and is NOT green: initial ClangDebug scan has 42 reports
not fully triaged, plus remaining model/probe failures. ClangRelease completed
after fixture repairs; the incremental final scan does not erase earlier reports.
No native Windows build performed. Syntax, SDK boundary, REUSE and copyright
checks pass. Some Linux/io_uring filehandle fixtures cannot run on this container's
backing filesystem; separate these aborts/skips from behavioral failures.
Remaining behavior includes NLM blocking, SMB grant/lease/identity/info, POSIX-
over-SMB, and remote NFSv3 RMDIR type-check mismatches. Do not claim conversion
complete or the LockSequence publication race fixed. Backend transactions remain
future work.

Submodule linked worktrees initially shared core.worktree settings; these were
repaired for libevpl/ndrzcc using extensions.worktreeConfig and per-worktree
core.worktree values. Both original and integration source trees were verified
clean before aligning them; do not mistake Git reading the wrong worktree for
user edits or discard changes to unrelated worktrees.

Final-main conflict resolution preserves main's acquire-or-queue blocker
recheck, with the compound admission fence checked under the same file mutex.
Fresh fenced requests return DENIED without parking; previously queued waiters
stay queued until release. Added test_waiter_admission_fence alongside main's
test_ack_before_enqueue and test_ack_before_requeue. All three pass in Debug
and Release.
Main's diskfs production fix is retained exactly. The intermediate published
989821ec tip is backed up at backup/compound-before-final-main-20260926.

## 2026-09-26 review of published PR a9e0efe0

Source review against main c971e5a5 and four fresh focused CTests are recorded
in docs/reviews/compound-pr-a9e0efe0-review.md. No production changes. Do not
equate executor routing with coalesced wire requests or finish-retry readiness.

Newly isolated integration regression: smb_proc_query_info.c:916 lost main's
assignment of r_fs_attrs.smb_fs_attributes via chimera_smb_fs_attributes. The
reply still emits that field at :1275. Native QUERY_INFO uses a private open
snapshot without a handle; restore from the actual command handle rather than
blindly copying main's expression. info_probe_memfs gets zero capability words
with streams both on and off.

Important remaining non-SMB gap: FUSE has 27 production raw compound_submit
sites, two shared frontend adapter sites (lock and commit), no direct retry.
Ordinary OPEN/CREATE/READDIR/etc return aggregate failure instead of replaying
finish EAGAIN. READDIR staging test manually resets; it does not exercise
production completion retry. Audit inputs/resets and use the adapter. Ordinary
SDK/POSIX, S3 and REST already use it. Several standalone NFS4 and legacy SMB
paths also use raw submit without finish retry.

NFSv4 proxy REMOVE ignores ISDIR/ISNOTDIR: current NFS3 remote model reproduces
both RMDIR(symlink) -> success and REMOVE(directory) -> success. Other remote
permission/attribute/statfs failures exist too; do not explain all by this flag.
Main already emits unrestricted proxy REMOVE, so do not call the missing type
enforcement a newly introduced PR regression. Main also did handle-only reparse
rebinding; the conversion repairs a slice but retains the stateful problem.
Stateful legacy SET_REPARSE still swaps only handle/flags, leaving old ACCESS,
RANGE and namespace identity; restricted native path excludes those stateful
opens. LockSequence cross-layer publication race remains source-level, without
a deterministic concurrent reproducer. Generic CLAIM/RECALL/CLOSE_DOC and
projected POSIX lock mutations/mandatory release remain explicitly nonretryable.
NLM pending/held/reaper lifecycle still requires a real journal conversion.

Backend integration contract concern: finish_result(OK) calls lock_attempt_accept
which can still fail on generation/cancel. Validate/freeze before backend commit,
then make accepted publication infallible under logical gates. VFS cache/notify
updates also remain pre-finish; deferred backend rollback alone cannot fix them.

Fresh tests all failed as expected: pnfs_memfs_remote, close_claim_probe,
lease_identity_probe_memfs, info_probe_memfs (6.34s total).
Log /tmp/chimera-pr-review-regressions.log. These reinforce, not supersede, the
prior 214-pass/29-skip/30-fail full quick results at 989821ec and final-main
14 focused passes per build plus lease-model timeout. Earlier wave green counts
do not describe the current merged tree/corpus. Review artifacts are uncommitted.

## September 26 PR regression/identity refinement (working tree)

- User prioritized confirmed regressions and identity, then asked why PR growth
  is so large. Production fixes are in the primary worktree and mirrored into
  `/tmp/chimera-compound-reconcile` for existing builds (same original HEAD).
- Capability planner now receives actual backend capabilities explicitly,
  including QUERY_INFO on private CREATE handles. Both streams knob probes pass.
- Embedded SMB ACCESS/cache CLOSE teardown uses atomic release_open again;
  close_claim_probe now passes for one and coalesced holders.
- Added same_cache for SMB ClientLeaseId compatibility; same_key/same_lease
  remain protocol+client-qualified, including RANGE I/O. Grant records/ACKs
  remain separate. Directory ParentLeaseKey suppression retains client scope.
  lease_identity_probe_memfs passes across clients and wire variants.
- Deleted unsafe SET_REPARSE handle-only callback fallback. Unsupported state
  (range/cache/DOC/streams/durable/resilient/peers/nonregular source) now fails
  before unlink. Supported migration updates open_flags with the new identity.
  IOCTL fixture now verifies preserved identities/bytes, range exclusion, and
  DOC behavior; existing retry/cancel/teardown/migration coverage still passes.
- NFSv4 proxy typed REMOVE uses LOOKUP + VERIFY/NVERIFY(type) in the same remote
  compound, proper NOTDIR/ISDIR mapping and pre-op attrs on failure. New quick
  remove_types_remote test passes, including dangling/directory symlinks.
  This does NOT solve concurrent remote rename vs removal: NFSv4 lacks atomic
  typed/matched REMOVE. Backend does not advertise REMOVE_MATCH_FH.
- Remote full NFS model still fails independent CREATE/permission/attrs cases.
  Unconfirmed LockSequence publication race, general FUSE finish retry, and
  backend transaction support remain outside this refinement.
- Size at a9e0efe0 vs main c971: +123589/-28129, net+95460. Tests+41564;
  VFS production+19241; SMB+16673; NFS+10993; other production-3093;
  docs+7199; other+2883. Production net+43814. Executor .c+.h adds12054,
  NFS4 adapter7067. Need deletion/consolidation pass, not just more conversion.
- Focused logs: `/tmp/chimera-pr-fixes-focused1.log` (3 original SMB regressions
  pass), focused3 (claim unit + extended IOCTL pass), focused4 (13 pass before
  new CMake registration), remove-types.log (new remote test passes), retry.log
  (extended VFS compound retry passes). Full required check completed at
  `/tmp/chimera-pr-fixes-check.log`; final results follow.
- Full GCC check result for this refinement: Release and Debug each 219 passed,
  29 skipped, 27 failed / 275. Previous baseline had 30 failures / 273; all three
  targeted SMB failures are repaired and no new failing test names appeared.
  The existing lease model still stalls at state267 in Debug and reports grant/
  break mismatches plus 600s timeouts in Release. Source/fixture changes are
  net-320 production lines and net+135 test lines. Root syntax, SDK include,
  REUSE and copyright checks pass (REUSE needed unsandboxed local IPC).
- Final make check exited2: Clang Debug40 / Release45 reports remain. The
  combined scan has42 unique file/message signatures versus40 in the saved
  baseline; the two additional signatures concern unchanged nfs_delay_retry_test.c
  and its included nfs4_slot.c (null slot-table path / parked-list leak).
  The extended delay_retry_test passes in both GCC Debug and Release; this
  does not by itself resolve those analyzer reports. No new warning signature
  points at modified production code. Full tests still have27 known failures.
  Code remains uncommitted; production files in both worktrees are identical.


## September 26 continued test repair (working tree)

User asked to continue fixing tests. No new agents, commits, or pushes in this
pass. Sources remain mirrored into /tmp/chimera-compound-reconcile for builds.

Confirmed fixes:
- SMB CMake had dropped SMB2_MBT_TRACE_DIRS, leaving encrypted311/ntlmv2/signed30
  batch tests with no input traces. Restore all five families' --trace-dir args.
- POSIX setup now unwinds before returning SKIP77 on an unsupported scratch
  filesystem, and drivers propagate77. server_destroy tolerates an initialized
  server whose worker pool was never started. Previously exit77 could abort on
  live threads, and clean unwind exposed a null-pool crash.
- /tmp is overlay; /worktrees/compounds is ext4 and supports passthrough handles.
  Use CHIMERA_MBT_SCRATCH and CHIMERA_TEST_ROOT both set to
  /worktrees/compounds/build/mbt-scratch. All ten previously failing POSIX
  linux/io_uring and NFS-over-passthrough cases pass there (55s). Overlay cases
  now skip cleanly instead of aborting. Do not count those as a backend fix.
- SMB CLOSE publishes empty-cache revocation before compound_free pumps ACCESS
  waiters. Cache retirement broadcasts owner-thread resume doorbells, and a
  newly registered reply wait rechecks locally to avoid a missed CLOSE wakeup.
- Native legacy-oplock publication recomputes the same-client lease cap from
  settled live state. The old pre-recall snapshot incorrectly forced NONE
  after the peer acknowledged NONE or closed. Updated overwrite_client_cap
  fixture expects LEVEL_II after ACK with a surviving peer open.
- Synchronous share-CLAIM denial fires OPEN_H_FORCE as the op's deny trigger,
  before a WAIT retry. The old final callback trigger was unreachable while
  WAIT parked and notified only the first blocking H holder. Removed the old
  duplicated trigger block. Queued denial does not repeat the trigger.
- An established RqLs lease at NONE must obey the sole-open WRITE-cache rule.
  Same-key ACCESS claims exempt only the requesting handle in that case;
  established nonzero granular caches retain their upgrade policy. This fixes
  lease 0x23_4 state146 wrongly rearming NONE to RWH and bumping its epoch.
  The claim unit's peer-open/cache fixture now supplies their common actual
  handle identity; all original assertions pass with that realistic identity.
- Legacy CREATE's successful OPEN attributes/access decisions are retained
  before a later share refusal can hand the handle to a claim-only retry.
  Otherwise peer CLOSE allowed the retry to succeed with r_attrs.mask=0 and
  CREATE reply aborted. leasePendingCloseRegression now passes.
- Abandoned parked legacy CREATE now completes CANCELLED after resource cleanup
  so the wire compound retires through the disconnected completion path. It
  previously returned with thread.live_compounds=1 forever; lease 0x23_6/_7
  replayed successfully but hung at server shutdown. Both now exit normally.
  Native recall settlement clears its retired park_fh marker as well.
- NLM confirmed ranges now preserve grant/replacement order: surviving split
  fragments stay at the original list position; granted pending reservations
  move to the tail at grant time. Removed duplicate LOCK's fast path so a
  re-lock uses the same admission/replacement semantics. FREE_ALL releases
  entries sequentially and pumps waiters each time, so this order changes
  which overlapping waiter is granted. All three previously failing
  stepBlocking_200_0x9_{0,1,2} traces now pass unchanged, with --paranoid.
  NLM's general direct-claim/journal/reaper conversion concerns remain.

Focused evidence: /tmp/chimera-tests-next-scratch-all.log (10 pass),
/tmp/chimera-tests-next-skip2.log (2 clean skips),
/tmp/chimera-tests-next-traces5.log (first8 lease traces pass),
/tmp/chimera-tests-next-smb-focused.log (38 SMB tests pass),
/tmp/chimera-tests-next-nlm-fixed2.log (3 NLM traces pass),
/tmp/chimera-tests-next-leases-fixed8.log (lease12 pass; earlier lease20 failure),
/tmp/chimera-tests-next-leases-fixed9.log (lease14/15/20 pass),
/tmp/chimera-tests-next-claim-fixed.log (269 claim assertions pass).
Extended journal/access/compound retry/NFS delay retry/SMB close/POSIX lock retry
also pass; initial combined extended log had the old handle-less claim fixture.
The initial full SMB attempt was superseded while stuck in the now-fixed
teardown; do not mistake its timeout/termination for the final code's result.

Required make syntax passes. Full make -k check running at
/tmp/chimera-tests-next-check.log, with ext4 scratch. Its first Release build
predated the claim-fixture handle correction, so that one test needs a rebuilt
Release rerun; Debug and later Clang builds will use the corrected fixture.
Final sweep results are to be recorded after completion. Remaining proxy issues
include NFSv4 GETATTR's stat-only request/parser (no statfs fields), remote
CREATE/type/permission semantics, and SMB-backed POSIX symlink behavior. Do not
claim the full tree is green based on the focused results above.


Additional finding from the ext4 sweep: six newly enabled NFSv4 passthrough
suites stalled on typeCoverageRegression step5 (READ anonymous FIFO). The
executor's open-intent flag treated every explicit handle as typechecked,
including PATH handles, although the later dispatch condition correctly
required a type check for PATH. It therefore opened the FIFO for data before
it could reject the type. Aligning the two conditions fixes the stalls;
regular data handles retain the existing descriptor-rights path. The directory
PATH-handle unit now checks EISDIR for READ/WRITE, the type-specific refusal
before opening, instead of the old generic flag-mismatch EINVAL.

Release reruns: both delegation passthrough suites pass; four ordinary/RDMA
linux/io_uring NFS4 suites now finish quickly but fail xattr operations with
STALE. Source cause: compound xattrs open PATH, while linux/io_uring use
fgetxattr/fsetxattr/flistxattr/fremovexattr on that descriptor. Those calls do
not support O_PATH. This needs a metadata-safe backend xattr access mechanism,
not blindly reopening FIFO/device handles for data. Log:
/tmp/chimera-tests-next-release-passthrough.log. The corrected claim test also
passes in that rerun. Nine functional failing test names currently remain:
three NFS3-over-NFS4 remote models, two SMB-backed POSIX models, four newly
unskipped NFS4 passthrough xattr models. Four new failures must be distinguished
from the five remaining out of the original27.

Final validation for this repair pass:
- Required `make -k check` completed with exit 2. Full log:
  /tmp/chimera-tests-next-check.log. GCC Debug/Release and Clang Debug/Release
  compile; syntax, REUSE and copyright checks pass. Static analysis and the
  remaining functional failures still prevent a green check.
- The full Release run reported 263 pass / 12 fail; it preceded the final FIFO and
  claim-fixture corrections. Rebuilt Release reruns pass the claim test and
  both delegation passthrough suites, with the four ordinary/RDMA passthrough
  suites completing with xattr failures instead of hanging.
- The full Debug run reported 265 pass / 10 fail; its VFS compound unit executable
  still expected EINVAL for the directory PATH-handle case. After rebuilding
  that unit, compound/compound_retry/claim_test/claim_journal/claim_access all
  pass in Debug and Release. Logs:
  /tmp/chimera-tests-next-debug-vfs-final.log and
  /tmp/chimera-tests-next-vfs-final2.log.
- Accounting for those targeted reruns, nine functional test names remain
  failing. 22 of 27 original failing names are cleared; 29 previously skipped
  tests now execute with ext4 scratch, exposing four additional failing names.
  Neither full CTest invocation skipped a whole test (individual unsupported
  trace capabilities still have their existing allowances). Do not describe
  this as one final full run with 266 passes: the final accounting includes the
  targeted reruns above. Ten former POSIX failures were scratch/skip handling,
  not ten backend correctness fixes.
- Clang logs have 41 unique path/message warning signatures versus 42 before this
  pass; no new signature, and the old NLM fragments-list null warning is gone.
  scan-build's retained-report summaries are 13 Debug / 14 Release; these are not
  comparable to the unique compiler warning count. Existing warnings remain
  untriaged; do not claim they are false positives or fixed.
- `make syntax` and `git diff --check` pass. All 25 modified source/build files
  are byte-identical between /worktrees/compounds and the integration source
  /tmp/chimera-compound-reconcile used for builds. No commit or push performed.

## September 27 architectural acceptance criterion: compound-only consumers

User explicitly reaffirmed that the final goal is to REMOVE the per-operation
north-facing VFS API. Every VFS consumer must submit filesystem operations
through compounds, including single-operation requests. Only a small, explicit
set of APIs inappropriate for compounds, such as resource release functions,
may remain outside that interface. Existing exceptions are not automatically
grandfathered into the final design.

Per-operation backend dispatch or executor implementation helpers may remain
private inside the VFS; their continued internal use does not justify exposing
them to consumers. Renaming vfs_procs.h to vfs_internal_procs.h is insufficient:
current NFS, SMB, S3 and FUSE production sources still include that header
(inclusion alone does not establish an actual per-op call). Removal of external
dependencies, consumer-facing declarations, fallback execution paths and
duplicated frontend implementations is part of completion, not optional cleanup
after declaring conversion finished. Single-command and multi-command execution
should use the same compound operation construction and protocol checks.
Enforce the final interface boundary in build/static checks once callers are
converted; keep release/lifecycle exceptions narrowly documented and justified.

## September 27 NFSv4 metadata API consolidation (working tree)

- ACCESS, ordinary GETATTR, READDIR and SECINFO now use the same native wire
  compound builder even when coalescing declines a run. The new metadata entry
  limits construction to one wire operation and uses the same operation checks,
  ownership, finish retry, response marshalling and accepted publication. It
  bypasses only the multi-op conservative reply budget; actual reply allocation
  checks remain. Removed the separate raw VFS callback chains, including the
  duplicate GETATTR delegation-query/combine path.
- SECINFO's standalone export-junction handling remains in the dispatcher. The
  shared builder rechecks possible junctions on execution/retry; a cached root
  comparison permits same-named entries under descendants, while unknown/root
  matches fail with DELAY. The namespace fixture covers descendant SECINFO with
  an export-name collision as well as export updates during finish.
- Synthetic named-attribute GETATTR and READDIR submit owned compounds and
  marshal only accepted results. READDIR gets base attrs plus stream records in
  one compound, keeps the synthetic protocol cursor, and uses the requested
  continuation cookie. Named-entry cookies now start at 3 rather than reserved
  cookies 1/2. The existing 64 KiB stream-list bound remains.
- Existing standalone PUTFH validation and OPENATTR now use the common finish
  retry adapter. They remain separate wire boundaries; this does not add a
  general synthetic cursor overlay or coalesced OPENATTR.
- NFSv4 ordinary calls to functions declared in vfs_internal_procs.h fell from
  28 to 14; all 14 remaining calls and the sole NFSv4 include of that header are
  in nfs4_proc_open.c (open_fh/open_at/lookup_at/open_stream/fsetattr). NFS4 LOCK
  still directly acquires claims via vfs_claim.h; DRC/recovery KV and other
  state/coordination boundaries remain. Do not call NFSv4 conversion complete.
- This pass reduces NFSv4 production by 516 physical lines (186 added / 702
  removed at this checkpoint), excluding tests and all earlier uncommitted work.

Debug build and 12 focused suites pass: v4.0/4.1/4.2 compound boundaries/retry,
delegation accepted/retry, namespace accepted/retry, metadata accepted/retry.
New metadata coverage checks ACCESS/GETATTR/READDIR forced through standalone
compound fallback by an oversized maxcount, suffix suppression on type error,
named-attribute type/size, paged entries/cookies and TOOSMALL. The fixture injects
finish EAGAIN into read-only metadata/stream compounds and verifies acceptance.
Logs: /tmp/chimera-nfs4-api-{metadata1,existing1,focused2}.log.

Previous /tmp/chimera-compound-reconcile and /tmp/chimera-reconcile-check trees
and logs no longer exist in the refreshed environment. New builds use the actual
working tree, build/Debug and build/Release; no source mirror is needed. Fresh
model corpus generation completed for all seven families. Both GCC builds pass,
and the same 12 focused suites pass in Release too (24 Debug/Release executions).
Release focused log: /tmp/chimera-nfs4-api-release-focused.log.

The required `make -k check` uses the fresh shared corpus, ext4 scratch at
build/mbt-scratch, and ASAN_OPTIONS=detect_leaks=0. Release reports 266 passed /
9 failed out of 275, matching the previously recorded failing names. Debug
reports 265 passed / 10 failed, with the same nine plus an intermittent FUSE
compound-lock teardown abort. Neither full run skips a whole test. Clang Debug
and Release compile but scan-build retains 42 / 46 findings; the combined log
has 41 unique path/message warning signatures. This matches the previous count,
but the previous logs are unavailable for an exact signature comparison. The
existing compound adapter combine/open_replay nullability warnings remain.
Formatting, SDK include, REUSE, copyright and git diff --check pass.
`make -k check` exits 2; do not describe the full check as passing. Full log:
/tmp/chimera-nfs4-api-check.log. No commit or push performed.

### Newly reproduced FUSE shutdown failure (outside NFSv4 pass)

`chimera/fuse/sim/compound_locks` passed once in isolation, then failed on the
11th run of CTest `--repeat until-fail:20`. The retained debug log ends with
`fuse thread destroyed with 1 active requests` at src/server/fuse/fuse.c:267;
NFS and SMB are disabled in this fixture. The final scenario deliberately
shuts down with a parked blocking lock after RELEASE. Stop cancels locks before
destroying the pool, but the cancellation/completion drain ordering needs
investigation; do not claim the passing rerun fixed it. No FUSE source was
changed in this NFSv4 pass. Evidence:
/tmp/chimera-nfs4-api-fuse-repeat.log,
/tmp/chimera-nfs4-api-fuse-abort.debug.log, and
/tmp/chimera-nfs4-api-debug-full-lasttest.log. The test-local debug artifact is
overwritten by subsequent reruns, so preserve the retained /tmp copy.

Code evidence for the likely teardown mechanism: fuse_server_stop calls
chimera_fuse_locks_shutdown, which calls synchronous lock_domain_shutdown and
schedules cancellation on the owning worker. Server destroy immediately stops
the worker pool. chimera_vfs_thread_drain (src/vfs/vfs.c:1484) waits only for
num_active_requests; neither parked lock attempts nor compounds themselves
participate in that counter. A drain/accounting contract for suspended compounds
is also needed before asynchronous backend finish is introduced. This remains
an investigation finding, not an implemented or verified shutdown fix.

## September 27 NFS delegated OPEN/API pass (working tree)

User requested another NFS pass, preserving the objective of removing the
north-facing per-operation API. No subagents, commit or push in this pass.

- CLAIM_DELEGATE_CUR and CLAIM_DELEG_CUR_FH now use the shared compound OPEN
  builder, including the single-operation fallback when reply budgeting declines
  coalescing. Removed their legacy OPEN dispatch branches and renewing stateid
  helper. Named claims LOOKUP, validate the delegation against the resolved FH,
  then OPEN that FH so a name replacement cannot redirect to an unchecked inode.
  The pure delegation snapshot checks client, object, version, access and
  revocation. Input failures remain compound checkpoints; reservation failure
  cannot silently revert to an unjournaled delegated OPEN.
- NFSv4.0 no longer breaks OPEN compounds simply because delegations are enabled.
  Grants run after accepted finish while client/owner reservations remain held.
  The final delegation (including owned WHO bytes) is copied into OPEN replay
  snapshots without overwriting a later owner operation. A repeated OPEN in the
  same wire compound receives the same grant; fresh-XID replay preserves it.
- New delegated UNCHECKED truncate testing found missing caller identity on the
  compound OPEN's internal SETATTR. That caused recall/revocation of the caller's
  own delegation. Truncate now supplies the admitted opener's actor identity.
- Named delegated CREATE remains supported: guarded/exclusive collision checks
  happen after identity validation; UNCHECKED size-zero truncation happens after
  access reservation. FH CREATE is invalid. Do not reject all named delegated
  CREATE: RFC 8881 section 18.16.3 explicitly permits it.
- NFSv4 ordinary raw VFS call sites decrease 14 -> 11, all still in
  nfs4_proc_open.c (open_fh 3, open_at 4, lookup_at 2, open_stream 1, fsetattr 1).
  Ordinary/cold-client OPEN fallback, named streams, legacy truncation and direct
  LOCK claims remain. NFSv3 has no ordinary raw VFS calls; removed 21 obsolete
  vfs_internal_procs.h includes. NFSv4 production delta for this pass alone:
  +269/-202, net +67; NFSv3 -21; VFS pNFS fix below is line-neutral.
- Broader testing exposed an obsolete pnfs_unsupported fixture: memfs now
  supports first-WRITE residency after main integration. Switched the negative
  fixture to Linux (no CAP_LAYOUT), using CHIMERA_TEST_ROOT/MBT scratch if set.
  This exposed actual data loss: VFS pNFS redirection materialized a DS backing
  for Linux, whose SETATTR does not persist PNFS_LAYOUT, so subsequent READ got
  local zeros. Both resolver entry points now honor CAP_LAYOUT via
  chimera_vfs_pnfs_io_possible, preserving local authoritative bytes.

Debug: all 26 selected NFS compound suites pass, including all three NFS3 backend
probes, namespace/proxy/delegation/pNFS/metadata, v4.0/4.1/4.2 boundaries and finish
retry, two new delegation-enabled v4.0 suites, actual read/write delegation replay
within a wire compound and across XIDs, and v4.0 named delegated-claim replay.
New delegated tests check both claim forms, current stateid READ/CLOSE, forced
single-op fallback, wrong object/stateid/version, guarded CREATE and valid/invalid
truncate. Retry fixture still deliberately excludes filesystem mutations;
these tests do not establish backend rollback. Log:
/tmp/chimera-nfs4-open-debug-focused.log. Release passes the same 26 suites:
/tmp/chimera-nfs4-open-release-focused.log.

Final required make -k check exits 2. Both Release and Debug report 266 passed /
9 failed out of 275; neither skips a whole test. Failing names exactly match the
nine baseline failures: pnfs_{memfs,diskfs,cairn}_remote, NFSv4
batch_{linux,io_uring,rdma_linux,rdma_io_uring}, POSIX batch_smb_memfs and
strict_smb. The previously reproduced intermittent FUSE shutdown abort did not
occur this time and remains unfixed. Both Clang builds compile; scan-build
reports two regenerated bugs in each build, but ccache reused previous analysis
compilations, so these counts are NOT a reduction from 42/46 baseline reports.
The emitted warnings retain all 41 baseline path/message signatures, with no
added signature occurrences. Formatting, SDK include boundary, REUSE, copyright
and final git diff --check pass. Full log: /tmp/chimera-nfs4-open-check.log.
No commit or push performed.

## September 27 remaining NFS OPEN conversion (working tree)

Converted the remaining ordinary OPEN fallback, named-stream OPEN and deferred
truncate paths to the shared VFS compound builder. Deleted the separate OPEN
installation/completion/callback chain, request fields and obsolete exported
helpers. This pass's NFS production delta, against
/tmp/chimera-nfs4-all-open-before, is +286/-1963 (net -1677 lines), excluding tests,
documentation and earlier changes. All 11 remaining ordinary per-operation VFS
calls are gone; NFS production has no vfs_internal_procs.h includes. Release,
claim/state/lifecycle helpers remain and are not counted as ordinary VFS ops.

- Standalone OPEN now uses the same reserved-owner journal, pure validation,
  replay, share admission, access union and accepted publication as coalesced
  OPEN. It binds cold v4.0 clients before building; there is no unjournaled
  fallback. Argument errors execute as checkpoints. Deferred truncation is an
  internal SETATTR after successful share reservation, carrying the opener's
  identity. Reservation failures preserve ACCESS/RESOURCE/client errors rather
  than collapsing everything into perpetual DELAY. Expired clients now report
  EXPIRED rather than being silently revived by the deleted OPEN path.
  A final review caught v4.0 principal ACCESS errors escaping before the owner
  journal. A standalone replay reservation now freezes that owner and reports
  the principal error as a mandatory execution checkpoint: no filesystem work,
  seqid consumption only after accepted finish, and correct error replay. The
  opt-in reservation API is explicitly restricted to this rejection/replay use;
  normal state reservations continue rejecting principal mismatches directly.
- Attribute-directory OPEN seeds the compound with the base FH, uses
  OPEN_STREAM, and shares the state journal. No stream create attributes are
  stamped onto the base inode, stream cinfo stays zero, and streams are not
  delegated. Existing UNCHECKED size-zero stream OPEN truncates the stream only
  after admission. A base stream-holder guard is acquired once at construction,
  retained across retries, transferred to a newly accepted stream state, and
  otherwise released at disposal. Repeatable operation callbacks do not publish
  it. Existing stream states can now be reserved/coalesced/closed by the journal;
  their original guard survives coalescing and drops at final CLOSE.
- Removing fallback exposed the former 128-existing-files-per-owner cap. Owner
  reservations now allocate their existing-file/child-pin arrays by population;
  compound state journals grow during construction and remain stable during
  execution/retry. Candidates remain bounded by wire operation count. A
  standalone OPEN freezes but does not build journals for unchanged child locks,
  avoiding unrelated coalescing limits. Coalesced lock-owner closure/range
  journal limits remain; this is not a claim to have removed all state limits.
- Added wire cases for forced standalone OPEN/replay/truncate, principal denial,
  held-lock reopen (coalesced and standalone), an owner with 130 open files,
  stream create/read/access union/close, denied and accepted stream truncation,
  guarded collision, and v4.0 stream create/reopen replay across fresh XIDs.
  Fault injection now admits nonmutating OPEN_STREAM and explicitly asserts a
  named stream's finish was rejected. Unit cases cover stream-holder lifetime,
  130 frozen child locks and expired-client error classification. Backend
  mutation rollback remains unimplemented and is not simulated by these tests.
- API conversion does not mean one VFS transaction per wire compound everywhere:
  the synthetic OPENATTR/attribute-directory boundary and reply-budget splits
  remain. Large connected lock sets may also require standalone dispatch.
  Owner reservation still scans/freezes the owner's existing state population;
  reducing that coordination cost is a separate design refinement.

The full sweep also exposed an intermittent SMB fixture abort in
chimera/server/smb/mbt/doc_compound_probe_memfs: expected_groups assertion at
smb2_doc_compound_probe.c:115 and submissions==1 at :230, after its disconnected
DOC waiter scenario. NFS is disabled in this fixture. An isolated Release
repeat passed eight times then failed on the ninth. The probe globally arms
all submissions, so late disconnected-client cleanup entering a later measured
window is a plausible cause, not yet proved or fixed. Preserve evidence in
/tmp/chimera-nfs4-all-open-check.log and
/tmp/chimera-nfs4-all-open-smb-doc-repeat.log. Do not relabel this as one of the
previous nine baseline failures or claim the broader suite is green.

Final verification for this pass:
- All 29 focused NFS CTest suites pass in Debug and Release, including the final
  principal-error replay regression. Logs:
  /tmp/chimera-nfs4-open-replay-debug-focused.log and
  /tmp/chimera-nfs4-open-replay-release-focused.log.
- The last owner-seqid correction was made during the full check's analysis
  phase, after its GCC test runs. Both GCC builds were rebuilt afterwards; in
  addition to the 29 focused suites, all 41 NFSv4 model suites were rerun in
  each build: 37 pass and the same four Linux/io_uring PATH-fd/xattr failures
  remain. Logs: /tmp/chimera-nfs4-open-replay-{debug,release}-mbt.log.
- The completed make -k check sweep reports 266 pass / 9 fail in both Debug
  and Release, with exactly the previous nine failing names and no whole-test
  skips. The SMB DOC probe abort from the first sweep did not recur in this
  sweep and remains unfixed. Full log:
  /tmp/chimera-nfs4-all-open-check-final.log. KVM suites were not run; CMake
  reported KVM disabled because oras is unavailable.
- Both Clang builds compile the final code. Final emitted diagnostics match
  all 41 baseline path/message signatures, with no added signature occurrences.
  Two new null-array warnings from the initial dynamic allocation shape were
  eliminated by using valid storage even for empty arrays and explicit counts.
  A targeted uncached state-file analysis confirmed only its existing file/FH
  warning remained. Final scan-build regenerated 4 reports in Debug and 5 in
  Release; ccache reuse still prevents interpreting those as total remaining
  analyzer findings. Targeted log: /tmp/chimera-nfs4-owner-scan-final.log.
- make syntax, the final syntax/SDK-boundary/REUSE/copyright checks, and
  git diff --check pass. Final hygiene log:
  /tmp/chimera-nfs4-open-final-hygiene.log. The final procedure-header symbol
  audit finds zero ordinary NFS VFS call sites and zero internal-procedure
  includes, excluding the explicitly permitted release API.
No commit or push performed.

## September 27 NFS attribute-directory coalescing (working tree)

This follows the remaining OPEN conversion above. NFS still has zero ordinary
per-operation VFS API calls. This pass removes additional dispatcher boundaries
and duplicate standalone compound implementations; it does not add backend
transaction begin/end hooks or rollback.

- OPENATTR now executes its configuration, backend-capability and regular-file
  checks inside the shared compound before any successor can run. PUTFH,
  GETFH, GETATTR, SAVEFH and RESTOREFH understand the synthetic attribute-directory
  cursor while the VFS cursor addresses its real base. OPENATTR -> stream OPEN
  -> READ/WRITE/CLOSE can share one VFS compound, including bases discovered by
  LOOKUP during execution. Multiple bases and stream OPENs are supported in the
  same compound. Synthetic saved cursors survive both coalescing and dispatcher
  boundaries. Operations lacking an attribute-directory encoding are explicitly
  excluded so they cannot accidentally address the base inode.
- Stream LOOKUP and REMOVE now join the shared encoder too, using OPEN_STREAM
  and REMOVE_STREAM. LOOKUP returns the real stream cursor; REMOVE preserves
  the synthetic directory and its existing zero/non-atomic change_info. The
  separate attribute-directory lookup/remove completion chains are deleted.
- The former single construction-time stream guard is replaced by explicit
  COORDINATE operations keyed by operation index and resolved base FH. Each
  guard is acquired once per such identity, retained across finish rejection,
  and selected by a repeatable pure completion callback. Only accepted state
  publication transfers ownership; disposal drops untransferred guards. The
  state journal retains the guard even when a later same-owner CLAIM_FH OPEN
  supplies the final handle. Replayed v4.0 OPEN skips acquiring a new guard.
- The attempt journal now records the protocol cursor after each successful
  wire operation, independently of internal VFS cursor moves. Accepted finish
  publishes it; retry resets it. This replaces completion-time special cases
  for failed namespace recalls and COPY/CLONE cursor walks. Ordinary OPENATTR,
  attribute-directory GETATTR, and all standalone PUTFH now reuse the same
  builder and gates; their duplicate callbacks and old staleness helper are
  removed. Standalone PUTFH submissions now appear in the shared trace, and
  boundary tests explicitly account for them.
- New FILEHANDLE/ACL attribute coverage caught an existing bug: the backend's
  base FH could win over the synthetic cursor when GETATTR requested FILEHANDLE.
  Synthetic attribute projection now clears that backend FH before marshalling,
  making GETATTR(FILEHANDLE) agree with GETFH. OPENATTR also rejects a base FH
  too long for the current wrapped-inner-FH limit (64 bytes including the
  8-byte marker), before allowing any suffix, instead of making an unwrappable
  synthetic handle. Increasing that wrapper limit is separate protocol work.
- Regression cases cover coalesced create/write, multiple resolved bases,
  saved/inherited synthetic cursors, stream CLAIM_FH identity, successful OPEN
  prefixes before failed suffixes, type/disabled-feature rejection before
  mutations, stream lookup/remove and standalone lookup fallback, synthetic
  TYPE/SIZE/FILEHANDLE/ACL, and v4.0 OPENATTR+stream create/reopen/replay. The
  metadata retry wrapper additionally requires three stream OPENs and three
  reads in one rejected finish, as well as the existing single-stream proof.
  The injector still excludes filesystem mutations; it does not model rollback.

The production delta against /tmp/chimera-nfs4-attrdir-before is +371/-557,
net -186 lines across seven production files, excluding tests/docs and earlier
work. Snapshots, detailed diff and diagnostic baseline are under /tmp with the
chimera-nfs4-attrdir prefix. No commit or push requested/performed.

Remaining named-attribute boundary: READDIR still uses its own LIST_STREAMS
compound and marshals its page after accepted completion. To coalesce it, move
cookie/verifier/page sizing and TOOSMALL checks into retryable execution
callbacks, with reply-arena reset, so they can veto a following mutation.
Namespace-root/export credential transitions, conservative reply/operation
budgets, connected lock-journal limits, and protocol lifecycle/pNFS cleanup
boundaries also remain. The previous intermittent FUSE shutdown and SMB DOC
probe failures remain unfixed unless subsequent records explicitly say otherwise.

Final validation:
- All 29 focused NFS suites pass on the final code/tests in both Debug and
  Release, including metadata, v4.0 replay, disabled OPENATTR, stream CLAIM_FH
  and explicit multi-base finish rejection. Logs:
  /tmp/chimera-nfs4-attrdir-focused-debug-final.log and
  /tmp/chimera-nfs4-attrdir-focused-release-final.log.
- The required make -k check sweep completed: 266 pass / 9 fail out of 275
  in EACH build, with the same nine previously recorded failing suites (three
  NFS remote-pNFS, four NFSv4 Linux/io_uring xattr cases, two SMB/POSIX symlink
  cases). There are no additional failing suites or whole-test skips. The
  known intermittent FUSE and SMB DOC aborts did not recur in this sweep and
  remain unfixed. Log: /tmp/chimera-nfs4-attrdir-check.log.
- Both Clang builds compile and emit the same 41 baseline path/message warning
  signatures, with no added occurrences. Analyzer exit status remains nonzero
  for existing findings; regenerated report counts remain subject to ccache
  reuse and are not a count of all remaining defects.
- make syntax, syntax/include-boundary/REUSE/copyright checks pass. The final
  internal-procedure declaration audit scans 76 function names and finds zero
  ordinary NFS call sites, excluding permitted release/handle-reference APIs.
  KVM tests were not registered because oras is unavailable.
- Final git diff --check passes. No commit or push performed.

## September 27 named-attribute READDIR coalescing (working tree)

This supersedes the named-attribute READDIR boundary recorded above. The shared
NFSv4 encoder now admits READDIR on synthetic attribute-directory cursors and
encodes base META open + GETATTR + LIST_STREAMS. No new VFS API was needed.
OPENATTR, READDIR, stream OPEN/READ/WRITE/CLOSE/REMOVE, saved cursors and following
operations can share one VFS compound. The duplicate standalone READDIR compound
and completion implementation are deleted; its single-operation fallback uses
the shared builder. Oversized reply-budget requests still deliberately split.

- The LIST_STREAMS execution gate validates records and stages the page before
  any successor executes. TOOSMALL, malformed records, change projection errors,
  invalid cookies and invalid attribute requests stop the sequence there.
  Argument checks moved from scanner boundaries into the operation prepare
  callback for both ordinary and named READDIR. Successful pages and EOF are
  private per-operation state; accepted finish publishes them, and whole-attempt
  reset rewinds the arena and clears the page marks before retry. Multiple pages
  and staged ACL GETATTR results coexist without overwriting earlier replies.
- Named-directory pagination no longer ignores cookieverf. Its opaque verifier
  hashes the base FH and ordered stream names/FHs, excluding sizes and record
  padding. A stale/nonmatching continuation returns NFS4ERR_NOT_SAME, matching
  RFC 8881 section 18.23.3 (https://www.rfc-editor.org/rfc/rfc8881.html#section-18.23.3).
  Reserved/out-of-range cookies return BAD_COOKIE. Cookie zero starts a fresh
  listing. Hashing/record validation continues beyond the returned page so a
  later malformed record cannot allow a mutation suffix to run. Missing named
  stream FHs fail IO instead of inheriting the base file's identity.
- The existing 64 KiB complete backend stream snapshot remains a limit. Memfs
  is currently the LIST_STREAMS implementation and returns a complete listing
  or ERANGE. A future backend returning a non-EOF partial snapshot is explicitly
  rejected with RESOURCE: positional cookies need a continuation-aware encoding
  before backend pagination can be supported. This pass does not provide backend
  transaction hooks or mutation rollback.
- Regressions cover empty and paginated lists, per-stream FILEHANDLE/ACL/SIZE,
  two pages plus ACL GETATTR/OPEN/READ in a rejected finish, v4.0 listing retries,
  invalid arguments/verifiers stopping CREATE, stale continuation stopping
  REMOVE, and pages before/after CREATE/WRITE/REMOVE in one wire/VFS compound.
  The finish injector now counts LIST_STREAMS operations actually completed;
  both metadata retry suites require a rejected two-list compound and a rejected
  unsuccessful list. Mutating compounds remain excluded from fake rollback.
- Removed the unused old READDIR entry-count estimator. Production delta for
  this pass: +135/-198, net -63 lines in nfs4_compound_vfs.c and
  nfs4_proc_readdir.c. Baseline snapshot:
  /tmp/chimera-nfs4-attrdir-readdir-before. No commit or push requested/performed.

Final validation:
- All 29 focused NFS suites pass in both Debug and Release after final cleanup,
  including the new v4.0 and v4.2 named-directory cases. Each v4.2 metadata suite
  now measures 44 complete wire/VFS spans. Logs:
  /tmp/chimera-nfs4-attrdir-readdir-focused-debug-final.log and
  /tmp/chimera-nfs4-attrdir-readdir-focused-release-final.log.
- Required make -k check completed with 266 passed / 9 failed out of 275 in EACH
  GCC build, exactly the previous nine failing suites: three remote-pNFS NFS
  suites, four Linux/io_uring NFSv4 PATH-fd/xattr suites, and two SMB/POSIX symlink
  suites. No additional failing suites or whole-test skips. The intermittent
  FUSE shutdown and SMB DOC failures did not recur; they remain unfixed.
  Log: /tmp/chimera-nfs4-attrdir-readdir-check.log.
- Both Clang builds compile. Diagnostic comparison against the prior snapshot
  has exactly the same 41 path/message signatures and occurrence counts, with
  no additions. Two reports were regenerated per build (ccache-dependent).
  make check remains nonzero for baseline tests and analyzer findings.
- Final syntax/include-boundary, REUSE, copyright and git diff checks pass.
  The internal-procedure reference audit scanned 76 names and found zero
  ordinary NFS call sites. KVM suites remain unregistered (oras unavailable).
- Initial new-test failures were test expectations, not production failures:
  four ACL-bearing entries needed a larger page than 4096, and standalone
  SAVEFH does not submit a VFS compound. Corrected tests pass. No commit/push.

## September 27 stream-list pagination (working tree)

User requested removal of the 64 KiB named-attribute stream-list limit. This
supersedes the complete-snapshot restriction in the prior READDIR record.

- LIST_STREAMS now carries an input/output verifier and a continuation cookie
  in every packed stream record. All in-tree compound callers and the internal
  backend adapter use the revised interface. The documented contract permits
  partial pages, supports resuming after any returned entry, and uses EBADCOOKIE
  for a stale verifier. A buffer too small for its first record still returns
  ERANGE; an earlier complete prefix succeeds with eof false.
- Memfs, the current stream-capable backend, honors continuation and returns
  bounded pages. Its verifier combines the base FH with a per-inode stream
  namespace generation, updated on stream creation, unlink and bulk removal
  during overwrite. Inode reuse resets the generation and changes the FH.
  Data writes/truncation and unrelated metadata changes do not invalidate it.
  The namespace lock protects verifier validation and page production together.
  Record fit calculations now include trailing alignment and initialize padding.
- NFS forwards the client's cookie/verifier into LIST_STREAMS and returns the
  backend cookies on the entries that actually fit its wire page. A backend page
  may contain more entries than the NFS reply. EOF reflects both limits, so a
  client resumes exactly after the last returned entry without omissions. The
  64 KiB allocation is now a page size, not a directory-size limit. Every READDIR
  retains its shared-compound execution gate, attempt-local staging, stop-before-
  mutation checks and accepted-finish publication. No new compound boundary.
- The previous frontend whole-snapshot name/FH hash is gone. Namespace edits
  now invalidate continuation even if a later edit restores the old membership.
  Invalid/reserved positions retain their NFS BAD_COOKIE handling; a stale
  verifier maps to NOT_SAME. A fresh cookie-zero request ignores its verifier.
- SMB's existing bounded stream-info query does not expose a continuation token.
  Both its shared and standalone paths now reject a non-EOF backend page instead
  of accidentally returning a successful truncated list. This preserves its
  bounded-query failure behavior; removing SMB's separate 4096-byte staging
  limit is not accomplished by this NFS pagination change. Its fallback default
  stream record is also fully initialized, including the FH length.
- New regressions create 300 names of 241 bytes each (>80 KiB of packed records),
  enumerate them to EOF, verify no duplicates/omissions and stable stream identity,
  reject a stale continuation before REMOVE, and continue after a data write.
  The retry fixture explicitly requires an actually completed non-EOF backend
  page to encounter finish rejection. VFS tests cover alignment at the first
  record, partial default-fork pages, continuation and stale-cookie mutation
  veto. SMB tests ensure an oversized result is not silently truncated.
- Production delta against /tmp/chimera-stream-pages-before: +148/-119, net +29
  lines. No backend transaction hooks or rollback are introduced. No commit/push.

Final validation:
- All 34 focused suites pass in BOTH Debug and Release, comprising the 29 NFS
  suites, three VFS compound suites and two SMB stream probes. The metadata
  feature suites each check 69 full wire/VFS spans. Logs:
  /tmp/chimera-stream-pages-focused-debug.log and
  /tmp/chimera-stream-pages-focused-release.log.
- Required make -k check completed: 266 passed / 9 failed of 275 in EACH GCC
  build, exactly the prior failing suites (three remote-pNFS, four Linux/io_uring
  NFSv4 PATH-fd/xattr, two SMB/POSIX symlink). No additional failing suites or
  whole-test skips. The known intermittent FUSE shutdown and SMB DOC failures
  did not recur and remain unfixed. Log: /tmp/chimera-stream-pages-check.log.
- Both Clang builds compile. The first Debug analysis found a new dead-store
  warning in the buffer-boundary test; an explicit assertion now checks that
  the saved LIST_STREAMS operation itself returned ERANGE. That target was
  rebuilt and passed CTest in both GCC builds, and reanalyzed in ClangDebug;
  the subsequent ClangRelease sweep included the corrected assertion. Final
  diagnostics have exactly the same 41 baseline path/message signatures and
  occurrence counts. The comparison replaces the superseded Debug test-file
  diagnostics with its reanalysis, rather than hiding an unresolved warning.
  Reanalysis log: /tmp/chimera-stream-pages-clang-debug-final.log; comparison:
  /tmp/chimera-stream-pages-final-diagnostics.py. Existing analyzer findings
  keep make check nonzero; report counts reflect fresh SDK-dependent analysis
  and ccache reuse, not a count of newly introduced defects.
- make syntax, final syntax/include-boundary, REUSE, copyright and diff checks
  pass. The NFS internal-procedure reference audit still finds zero ordinary
  calls across 76 names. KVM remains unregistered because oras is unavailable.
- Source snapshots are at /tmp/chimera-stream-pages-before. No commit/push.

## September 27 NFS4 shared-builder consolidation (working tree)

READLINK, VERIFY/NVERIFY, GETXATTR, SETXATTR, LISTXATTRS and REMOVEXATTR now
use chimera_nfs4_compound_metadata for standalone execution as well as the
shared multi-operation encoder. Removed six independent builders/completion
chains, the duplicate READLINK type gate, its unused declaration, and unused
VERIFY helper. Keep protocol argument validation and shared reply marshallers.
There are now 18 nfs4_proc_*.c files with independent builders, down from 24.
The broader namespace/state/pNFS boundaries from the previous assessment remain.

- Preserve READLINK's NOFOLLOW open intent and its non-symlink status.
- Use one pure xattr-name length validator in the handlers, scan and name
  staging; invalid lengths no longer allocate reply memory before rejection.
- Shared xattr operations now acquire ordinary inferred data handles rather
  than PATH handles, including after a metadata operation. This preserves the
  removed standalone builders' descriptor semantics and FIXES the four recorded
  NFS4 Linux/io_uring xattr failures (plain and RDMA variants).
- Unsupported operations on pseudo-root/named-attribute directory handles keep
  their previous STALE result. In particular, fallback must not silently unwrap
  an attribute-directory cursor and mutate xattrs on its base inode.
- Fifteen new measured requests force single-operation execution via an
  oversized READDIR suffix, or check xattrs following metadata in one compound.
  Cover successful and failed VERIFY/NVERIFY, READLINK/type rejection, xattr
  payload/list lifetime, CREATE collision, missing keys and TOOSMALL. Further
  checks cover invalid arguments stopping WRITE and synthetic cursors leaving
  the base untouched. Metadata suites now measure 84 complete wire/VFS spans.
- Finish injection now explicitly observes completed READLINK and GET/LISTXATTR
  operations; both metadata retry tests require these to encounter EAGAIN.
  Filesystem mutations remain excluded from synthetic rollback.
- Production delta against /tmp/chimera-nfs4-shared-builder-before: +74/-488,
  net -414 lines in eight production files. No commit or push.

Final validation:
- All 29 focused NFS suites pass in Debug and Release. Two initial Debug pNFS
  fixtures failed to mount the Linux backend on /tmp; rerunning those two with
  CHIMERA_TEST_ROOT=/worktrees/compounds/build/mbt-scratch passed. All Release
  focused tests used that scratch directory. Initial new-test failures were
  Python harness issues (xattr names decode as bytes, and pynfs rejected the
  deliberately invalid enum while packing); corrected assertions/packing pass.
- Required make -k check CTEST_PARALLEL=8: Release 270 passed / 5 failed of 275;
  Debug 269 passed / 6 failed of 275. Four previous NFS4 failures are fixed.
  The five carried-over failures are nfs/mbt/pnfs_{memfs,diskfs,cairn}_remote
  and posix/mbt/{batch_smb_memfs,strict_smb}. The known FUSE shutdown and SMB
  DOC intermittent failures did not occur in this sweep and remain unfixed.
- Newly observed independent correctness issue: Debug chimera/vfs/compound
  passes its assertions but LeakSanitizer reports 116408 bytes in six
  allocations. An isolated CTest rerun reproduces it. Five leaked operation
  snapshots originate at vfs_compound.c:8555: submit allocates original_ops
  again when an already-completed compound is submitted without disposing of
  the old snapshot/results. The io_owner re-submission test also leaves a
  632-byte cached open handle. This is a second submit after accepted completion,
  distinct from compound_retry after a rejected finish. Fix requires proper
  resubmission cleanup, not merely freeing the snapshot array. These VFS sources were unchanged in this
  pass, and the failing executable does not link the NFS server library. This
  issue remains unfixed; do not describe the overall checks as green or silently
  equate this sweep with the prior nine-failure baseline.
- Source audit still finds zero ordinary NFS calls across 76 internal VFS
  declarations (permitted release calls excluded). KVM not registered: oras
  unavailable. Final GCC builds include the comment cleanup after testing.

Logs: /tmp/chimera-nfs4-shared-builder-{check,focused-debug,focused-release,
pnfs-debug,targeted,vfs-debug}.log; final analysis comparison script:
/tmp/chimera-nfs4-shared-builder-diagnostics.py.

- Both Clang builds and corpus generation completed. Final diagnostics exactly
  match the 41 baseline path/message signatures and occurrence counts, with
  no additions or excess occurrences. ScanReport artifact counts are 42 Debug
  and 46 Release (affected by regenerated translation units/cache reuse).
  Existing diagnostics and the test failures above keep make check nonzero.
- Final syntax, SDK include-boundary, REUSE, copyright and git diff checks pass.
  The sandboxed standalone REUSE invocation could not bind its multiprocessing
  socket; the required full sweep and the separate unsandboxed license check
  both pass. Logs: /tmp/chimera-nfs4-shared-builder-{style-final,license-final,
  diagnostics}.txt/log as appropriate. No commit or push performed.

## September 27 VFS compound resubmission lifetime repair (working tree)

Follow-up to the NFS4 shared-builder pass's LeakSanitizer finding. A second
compound_submit now retains the original construction snapshot and runs the
same restart path as compound_retry, rather than allocating another snapshot
from already-executed operation structs. Restart releases the prior attempt's
owned results, restores original arguments/status/callback flags, resets groups
and cursors, and discards dynamic suffixes before attempt_reset and execution.

- Factored result-only teardown into one helper shared by compound_free and
  restart. Preserve build-owned paths, KV inputs, borrowed I/O/ACL inputs, and
  lock attempts. Reservations drain before producer/cursor handles. The common
  result helper also balances lock_file_state references.
- Resubmission observes the same replay barriers as retry: no running/canceled
  run, ownership transfer, published claim/access journal, nonretryable operation,
  or pending cross-thread cancel. Accepted borrowed CLOSE is also a barrier:
  its reference is owed to accepted teardown, so replay must not discard it or
  consume it again. Rejected-finish CLOSE remains retryable.
- An unsafe second submit reports aggregate EINVAL to the supplied terminal
  callback without executing operations/finish adapter. It preserves prior
  result ownership and finish status for proper teardown. Submitting an active
  compound is a programming error. These APIs still do not undo filesystem
  effects; ordinary operation EAGAIN does not establish backend rollback.
- Tests now require original gate/prepare inputs on re-execution, rerun prepare
  callbacks, rebuild grouped and ungrouped dynamic suffixes without growth,
  replace untaken READ references while preserving WRITE inputs, and refuse
  replay after taking a handle, publishing a range journal, or accepting an
  external CLOSE. Refusal preserves journal retirement and CLOSE release.
- The formerly failing Debug compound test now passes with leak checking, as
  do compound_retry and compound_groups. All six compound/claim suites pass
  in Release. Final validation is recorded below.
- Snapshot before this pass: /tmp/chimera-resubmit-before. Production delta
  is +33 net lines across vfs_compound.c and vfs_compound.h; no frontend
  operation conversion changes. No commit/push.

Final validation for the resubmission repair:
- All 35 focused extended suites pass in Debug and Release: six VFS compound/
  claim suites plus the 29 NFS compound/lifetime/delegation suites. Debug ASan
  no longer reports the 116408-byte resubmission leak. Release was rebuilt
  after final API comment changes before its focused sweep.
- Required make -k check CTEST_PARALLEL=8 completed all stages, exit 2 because
  of baseline failures/findings. Both quick sweeps: 270 passed / 5 failed of
  275. Failures remain nfs/mbt/pnfs_{memfs,diskfs,cairn}_remote and
  posix/mbt/{batch_smb_memfs,strict_smb}; Debug compound is now green. The known
  intermittent FUSE shutdown and SMB DOC failures did not recur here.
- Both Clang builds and model corpus generation completed. Diagnostics match
  exactly the prior 41 path/message signatures and 110 occurrences, with no
  additions/excess or missing/reduced occurrences. ScanReport artifact counts
  are 34 Debug and 35 Release; they differ with cache/regeneration and are not
  a count of newly introduced findings.
- make syntax, final syntax-check, VFS SDK boundary, REUSE, copyright and diff
  checks pass. KVM remains unregistered because oras is unavailable.
- Logs: /tmp/chimera-resubmit-check.log, /tmp/chimera-resubmit-focused-debug.log,
  /tmp/chimera-resubmit-focused-release.log, /tmp/chimera-resubmit-extended-debug.log,
  /tmp/chimera-resubmit-extended-release.log, /tmp/chimera-resubmit-style-final.log.
  Warning comparison: /tmp/chimera-resubmit-diagnostics.py and .txt.
- No commit or push. NFS shared-builder consolidation is still the next separate
  conversion task (COMMIT/CREATE/LINK first); the 18 independent builders and
  broader namespace/state/pNFS boundaries from the previous assessment remain.

## September 27 NFS4 COMMIT / CREATE / LINK shared builders (working tree)

COMMIT, CREATE and LINK now use chimera_nfs4_compound_metadata for their
single-operation fallbacks. Deleted their duplicate construction and completion
paths. The shared encoder handles accepted results, cursor publication and
finish retries for both coalesced and standalone execution. Production delta
against /tmp/chimera-nfs4-ccl-before: +104/-430, net -326 lines in five files.
Independent builder files fall from 18 to 15. No commit/push requested.

Preserved behavior and repairs:
- COMMIT retains NOFOLLOW on its preliminary metadata open, rejects nonregular
  objects before a potentially blocking data open, and reads flush pre_attr
  rather than the shared path's incorrect post-operation attr field.
- CREATE has one pure request validator shared by the scan and handler.
  Decode failures now become execution-checkpoint errors before any directory
  open/mutation; the old standalone path ignored unmarshalling errors. Rename
  map.open_input_status to map.input_status and reuse that attempt-invariant
  error field for OPEN and CREATE. Bad nanosecond timestamps return INVAL and
  create no name, including when the finish adapter rejects an attempt.
- CREATE's result callback now rejects absent/empty backend filehandles before
  the compound suffix can use the parent cursor as the created object.
- LINK preserves the explicit target-directory OPEN before LINK, including
  source-side lease recall in the VFS. Its single-op encoder can seed raw saved
  handles from sibling exports: different export IDs can share a filesystem,
  so the VFS decides EXDEV, not the encoder. Normal coalescing still stops at
  export credential transitions. Read-only saved exports still reject LINK.
- Synthetic current root/attribute-directory handles retain STALE. Saved
  synthetic LINK sources remain opaque and must never be unwrapped to link
  their base inode. The existing VFS mount-ID check returns XDEV for these
  sources; memfs inferred opens do not necessarily validate directory type,
  so XDEV also precedes NOTDIR for a regular target in this combination.
- Forty-eight additional measured wire requests exercise forced single-op and
  coalesced COMMIT/CREATE/LINK, six CREATE object types, device numbers, mode,
  applied attrset, change_info, SAVEFH/RESTOREFH, collisions, invalid timestamps,
  COMMIT type failures, error suffix stopping and cross-export hard links.
  Extra unmeasured checks cover synthetic cursors and read-only exports.
- The finish-retry fixture now injects EAGAIN on completed COMMIT and requires
  a nonzero commits counter. Successful CREATE/LINK remain excluded because
  the fixture has no backend mutation rollback.

The 15 remaining independent handler builders are ALLOCATE, CLONE, COPY,
DEALLOCATE, LOCKT, LOOKUP, LOOKUPP, READ, READ_PLUS, REMOVE, RENAME, SEEK,
SETATTR, WRITE and WRITE_SAME. They already use compounds; the remaining work
is sharing their builders and eliminating avoidable segmentation, alongside
namespace/export, bounded journal, state-retirement and pNFS boundaries.
RENAME remains the next suggested consolidation task.

Final validation:
- All 35 focused VFS/NFS extended suites pass in both Debug and Release,
  including the expanded metadata and finish-retry tests. Initial new-test
  failures were incorrect expectations (memfs symlink mode, synthetic-source
  XDEV ordering, and synchronous cursor-only fallback trace spans), corrected
  after comparing with the removed builders and VFS dispatch.
- Required make -k check CTEST_PARALLEL=8 completed all stages, exit 2 for the
  existing test failures and analyzer findings. Both quick sweeps pass 270/275;
  failures remain nfs/mbt/pnfs_{memfs,diskfs,cairn}_remote and
  posix/mbt/{batch_smb_memfs,strict_smb}. Known intermittent FUSE shutdown and
  SMB DOC failures did not recur and remain unfixed.
- Both Clang source builds and model corpus generations completed. Diagnostics
  exactly match all 41 baseline signatures and 110 occurrences: no additions,
  excess, missing or reduced occurrences. Each ScanReport has two generated
  reports; cache reuse changes that artifact count, not the warning baseline.
- make syntax, syntax-check, SDK include boundaries, REUSE, copyright and
  git diff --check pass. NFS source audit finds zero ordinary calls across 74
  internal VFS declarations after excluding permitted release APIs. KVM
  remains unregistered because oras is unavailable.
- Logs: /tmp/chimera-nfs4-ccl-{check,focused-debug,focused-release,targeted,
  build-debug,syntax}.log; diagnostic comparison script/output:
  /tmp/chimera-nfs4-ccl-diagnostics.py and .txt. Isolated production delta:
  /tmp/chimera-nfs4-ccl-production.patch. No commit or push performed.


## September 27 NFS4 remaining handler builders, three batches (working tree)

User asked to remove the remaining independent builders in successive batches.
Completed all 15 nfs4_proc_* handler files: RENAME/LOOKUP/LOOKUPP/LOCKT, then
READ/READ_PLUS/WRITE/SETATTR/ALLOCATE/DEALLOCATE/SEEK/WRITE_SAME, then COPY/CLONE/
REMOVE. All use chimera_nfs4_compound_single (renamed from _metadata), sharing
construction, pure execution callbacks, retry teardown and accepted publication
with coalesced runs. No nfs4_proc_*.c allocates a compound now; this removes 25
allocation sites from those 15 files. Relative snapshot:
/tmp/chimera-nfs4-all-builders-before. Production delta +365/-3972 = -3607 lines.
No commit/push requested or performed.

Semantics and correctness:
- RENAME keeps target-directory validation and delegation coordination. LINK,
  RENAME, COPY and CLONE single-operation fallbacks preserve raw saved handles
  across export IDs, letting the VFS decide cross-filesystem behavior. Synthetic
  saved handles remain opaque; credential transitions still split coalescing.
- LOOKUP/LOOKUPP retain namespace routing and reject missing backend result FHs
  before suffix execution. Audit caught an unconditional root-export LOOKUPP
  scanner refusal: ordinary child LOOKUPP returned DELAY after consolidation.
  Restricted that refusal to coalesced scanning; shared single entry is reached
  only after the handler resolves namespace routing. New regression fails on
  the pre-fix binary, passes after fix, including namespace finish retry.
- LOCKT preserves v4.0 client/grace validation, NOFOLLOW/type checks, range error
  order, owner/range deny results and accepted-only lease renewal.
- Shared I/O retains grace, NOFOLLOW metadata checks, stateid/FH/client/principal
  validation and request-input errors. SETATTR decode errors use a checkpoint
  and stay coalesced; updated four trace expectations previously requiring a
  refused build for malformed owner attributes. WRITE_SAME geometry validation
  avoids unsigned addition overflow.
- v4.0 I/O formerly renewed client leases during execution via acquire(). It now
  acquires without renewal and pins encountered clients; attempt reset clears
  renewal flags and accepted finish touches them. Pins remain across retries,
  release at teardown. v4.1+ renews only its authenticated session client.
- COPY uses the existing VFS generic read/write fallback, removing the duplicate
  frontend transfer loop. Endpoint stateid classes now fail at execution;
  CLONE retains capability checking even for zero-length requests.
- REMOVE's MDS phase uses the shared encoder. Real pNFS REMOVE still ends the
  wire run: its independent DS cleanup is best effort AFTER accepted MDS finish,
  and retries only finish EAGAIN (bounded), before resuming the wire suffix.
  Fixed stale backing-name construction to use the shared mount-id/file-id
  helper, matching both creation paths. New resident-memfs pNFS test verifies
  first WRITE creates a backing file, nonfinal-link REMOVE preserves it/data,
  and last-link REMOVE deletes it. Existing proxy pNFS tests did not cover this.

Coverage and remaining scope:
- Added forced-single wire tests for all 15 handlers, payloads/counts/stability,
  wrong-FH stateids, invalid attributes/ranges, change_info, cursors and errors;
  separate v4.0 READ/WRITE/SETATTR/LOCKT coverage. Namespace tests cover ordinary
  child LOOKUPP under a root export and descendant LOOKUP of an export name.
- Debug and Release focused VFS/NFS suites: 36/36 pass in each, including the
  new resident-pNFS test.
  Initial CLONE test used four bytes; backend requires aligned ranges, so fixed
  fixture to 4096 bytes. No assertion was relaxed to conceal a production fault.
- Remaining non-shared allocations are three namespace/export/root-resolution
  builders in nfs4_root.c and three pNFS builders in nfs4_pnfs.c (LAYOUTGET query,
  backing materialization, LAYOUTCOMMIT), plus accepted pNFS REMOVE cleanup in
  the shared encoder. These already use compounds but are separate orchestration
  work; do not claim the entire NFSv4 wire request is always one VFS transaction.
- Existing state-retirement/session/namespace/export and bounded reply/journal
  boundaries remain. Backend transactions and real mutation rollback remain out
  of scope; the synthetic finish fixture excludes successful mutations.
- Both final quick sweeps pass 270/275. Failures are the same existing
  nfs/mbt/pnfs_{memfs,diskfs,cairn}_remote and posix/mbt/{batch_smb_memfs,strict_smb}.
  Initial full-check Release ran before the LOOKUPP fix and had two extra root
  namespace MBT failures; a full Release rebuild and 275-test rerun confirmed
  both are fixed. Debug in the full check ran the final source.
- Full make -k check CTEST_PARALLEL=8 completed, exit 2 for the recorded tests
  and existing analyzer findings. Both Clang builds and all model generation
  stages finished. Diagnostics exactly match the saved baseline: 41 signatures,
  110 occurrences, no new/excess or reduced/missing entries. Each ScanReport
  has two reports. Final Release quick rerun is the authoritative Release
  result because the original sweep preceded the LOOKUPP correction.
- make syntax, final syntax-check, SDK include boundaries, REUSE, copyright
  and git diff --check pass. KVM remains unregistered because oras is absent.
  Existing intermittent FUSE shutdown/SMB DOC failures did not recur.
- Audited 74 ordinary internal VFS API names after stripping comments: zero
  direct calls anywhere in the NFS frontend.
- Logs: /tmp/chimera-nfs4-all-{check,debug-focused,namespace-before,
  namespace-build,batch1-tests,batch2-tests,batch3-tests,syntax}.log. Additional
  final logs: /tmp/chimera-nfs4-all-{release-focused,release-quick,
  final-release-build,final-boundaries}.log. Diagnostic comparison:
  /tmp/chimera-nfs4-all-diagnostics.py and .txt. Isolated production patch:
  /tmp/chimera-nfs4-all-production.patch.


## September 29 NFS4 LAYOUTCOMMIT consolidation (working tree)

User requested another NFS compound cleanup pass. LAYOUTCOMMIT now participates
in the shared encoder and uses chimera_nfs4_compound_single for fallback. Removed
its independent builder/gate/completion from nfs4_pnfs.c. Production delta against
/tmp/chimera-nfs4-layoutcommit-before: +135/-180 = -45 lines, across
nfs4_compound_vfs.c and nfs4_pnfs.c. No commit/push requested or performed.

- Pure execution authorization acquires the layout without lease renewal,
  validates the pinned session client, full identity, current execution FH,
  destruction, explicit sequence version and writable iomode, then drops the
  reference. Every backend retry redoes these checks. Seqid zero means current;
  old/future/open/anonymous/forged/returned stateids keep their distinct errors.
- Each attempt reads current size, then prepares a conditional SETATTR. Repeated
  commits in one run see preceding changes; reported high-water marks never
  shrink the file. Mtime updates and newsize reporting are preserved. The
  setattr_after_write flag avoids recalling the layout authorizing those writes.
  Empty updates use the executor's successful skip path. Wire arguments remain
  unchanged, and responses are published only after accepted finish.
- Reject overflow and invalid nanoseconds before metadata changes or suffixes.
  Pseudo-root and named-attribute directories cannot use a base file's layout.
  Feature-disabled and missing-FH paths retain their errors. The shared scanner
  retains export, synthetic-cursor, minor-version and reply-capacity boundaries.
- Tests cover coalesced before/after metadata, repeated commits, no shrink,
  mtime-only, standalone fallback, wrong client/FH, read-only grants, stateid
  versions, invalid input, stopped mutation suffixes and synthetic cursors.
  Disabled-pNFS tests cover coalesced, standalone and no-FH requests.
- Finish injection only admits unexecuted or skipped LAYOUTCOMMIT SETATTRs;
  applied size/mtime mutations remain excluded because the fixture has no
  rollback. A deterministic pending-finish gate returns the layout on another
  request, rejects finish, and proves the retry returns BAD_STATEID before its
  GETATTR suffix. Existing namespace rendezvous shares the fixture helper.
- Initial test issues: generated nfstime4 values require scalar comparison,
  not Python object equality. The proxy pNFS fixture lacks named streams, so
  the attrdir test uses resident memfs with named streams explicitly enabled
  and a real base-file layout; OPENATTR must succeed before LC is rejected.
  No production assertion was relaxed to hide either fixture issue.
- Debug and Release focused VFS/NFS suites pass 36/36 each; final disabled-pNFS
  additions also pass in both metadata variants. Both full quick sweeps pass
  270/275, with the same existing nfs/mbt/pnfs_{memfs,diskfs,cairn}_remote and
  posix/mbt/{batch_smb_memfs,strict_smb} failures.
- Full make -k check CTEST_PARALLEL=8 completed (exit 2 for the existing tests
  and analyzer findings). Both Clang builds and all model-generation steps
  finished; diagnostics exactly match the saved baseline: 41 signatures and
  110 occurrences, no new/excess or reduced/missing entries. Each ScanReport
  contains two reports. SMB model generation took 495s/476s; it was not hung.
  make syntax, syntax-check, SDK boundaries, REUSE, copyright and diff checks
  pass. KVM suites remain unregistered because oras is absent.
- Remaining specialized allocation sites: three root/export-resolution builders,
  two pNFS builders (LAYOUTGET query and backing materialization), plus accepted
  pNFS REMOVE cleanup. No nfs4_proc_* handler allocates its own compound. A fresh
  comment-stripped audit of vfs_internal_procs.h names finds zero ordinary
  direct VFS calls in the NFS frontend.
- LAYOUTGET still needs staged layout publication, retry-time authorization and
  consolidation of its conditional native-layout/backing-file work. Root export
  resolution needs export credential/snapshot semantics; simply sharing a small
  allocation helper would not coalesce those wire boundaries. State/session
  retirement and reply/journal capacity boundaries remain. Backend transaction
  rollback and cross-MDS/DS atomicity are not provided by this pass.
- Logs: /tmp/chimera-nfs4-lc-{syntax,build,targeted,disabled,focused-debug,
  focused-release,check}.log. Isolated production patch:
  /tmp/chimera-nfs4-lc-production.patch. Diagnostic comparison:
  /tmp/chimera-nfs4-lc-diagnostics.py and .txt.


## September 29 NFS4 root and export entry consolidation (working tree)

User asked to continue the remaining NFS work. Export-entry LOOKUP and real-root
PUTROOTFH/PUTPUBFH now call chimera_nfs4_compound_export, which selects the export
credential and uses the shared encoder for the entry and its same-export suffix.
Oversized replies use the same single-operation fallback. Removed the independent
export LOOKUP builder/completion in nfs4_root.c and PUTROOTFH's resolver callback.
Current production delta against /tmp/chimera-nfs4-root-before: +196/-113 = +83
lines. No commit/push requested or performed.

- The VFS seed is PUTROOT followed by LOOKUP_PATH (GETFH for an empty root
  path). LOOKUP keeps its original no-follow semantics; PUTROOTFH follows final
  symlinks and resolves afresh, preserving root-remount recovery. Failed real
  root resolution remains SERVERFAULT, never a synthetic-root fallback.
- An optional owned export snapshot fixes name/id/path/access/squash/anon ids/
  security policy. Pure prepare callbacks compare it under exports_lock before
  each wire operation on every attempt. A changed or removed export returns
  DELAY, forcing fresh client selection instead of replaying old credentials.
  Cache priming occurs only at accepted result publication, with a second
  locked snapshot comparison. Caller-owned paths are copied by the VFS.
- Export-entry LOOKUP copies live export records under lock before selecting
  policy. Crossing to another export/root remains a boundary because a VFS
  compound has one credential. Squash always derives from orig_cred.
- PUTROOTFH's deliberate WRONGSEC deferral is preserved. A disallowed flavor
  can install the root when the next operation handles security, but that root
  operation runs alone: otherwise a coalesced same-export PUTFH could bypass
  the normal security-flavor check. Tests retain the old signed root FH while
  reusing its export id with a Kerberos-only policy to exercise this case.
- Tests assert exact spans for root/public-root plus lookup/read, root after a
  file cursor, SAVEFH/SECINFO_NO_NAME/RESTOREFH, repeated roots, sibling entry,
  multi-component exports, single fallbacks, failed prefixes, read-only policy,
  security flavors, and squash/reset across two OPENs. Synthetic-root entry is
  measured separately in metadata tests. Real root cases include VFS path /,
  a final symlink and a missing path.
- The read-only finish fixture now admits namespace LOOKUP_PATHs and records
  them. A deterministic gate replaces an export while finish is pending, then
  rejects finish. Tests explicitly reuse the export id while changing its path
  or only its policy; retry rejects the old snapshot before GETFH. A root
  replacement case uses the same rendezvous. Successful mutations are still
  excluded; this does not implement backend rollback.
- Added accepted/retry NFSv4.0 namespace suites for PUTROOTFH/PUTPUBFH, entry
  suffixes, single fallback and failed-prefix retry. Initial v4.0 test had only
  successful requests and failed the fixture's required failed-prefix coverage;
  added the missing NOENT case, retaining the assertion. All six targeted
  namespace/metadata suites now pass. Initial existing focused suites passed
  36/36 before adding the two v4.0 suites. Final focused suites pass 38/38 in
  both Debug and Release. Final quick sweeps pass 270/275 in both builds,
  reproducing only pnfs_{memfs,diskfs,cairn}_remote, posix/batch_smb_memfs and
  posix/strict_smb. Clang diagnostics exactly match the saved baseline: 41
  signatures, 110 occurrences. The final incremental ClangDebug rebuild
  reports only the same two encoder diagnostics (combine/open_replay).
  Formatting, SDK boundary, licensing, copyright and diff checks pass. The
  licensing checker needed escalation for its Python multiprocessing socket.
- Final review added the common checkpoint to COPY/CLONE preparation so range
  operations also revalidate the selected export. Export lookup rejects a reused
  id whose path changed between name selection and snapshot copying. New root
  COPY tests exposed a needless build-time FH requirement in range restoration;
  NFSv4.0 OPEN replay had the same assumption. Both now use placeholders filled
  by their existing execution callbacks. Exact-span tests cover nonempty/EOF
  COPY and v4.0 OPEN/replay, including suffix data and restored handles.
- Release metadata retry initially failed an exact-span assertion after all
  wire checks passed. Reproduced with preserved logs: fixture stderr inserted
  an accepted record into the middle of the stdout submission tag at page 19.
  Changed fixture output to fwrite/fflush on the same stdout FILE as the server
  logger, sharing its buffer and stdio lock. Retained every trace assertion;
  20 consecutive reproducer runs and both final focused suites pass. Failed
  log: /tmp/chimera-compound-wire.MHutH8/chimera.log (lines 6294-6295).
- Remaining allocations: root-FH cold-cache resolution and pseudo-root READDIR
  enumeration in nfs4_root.c; LAYOUTGET query and backing materialization in
  nfs4_pnfs.c; shared main encoder and accepted pNFS REMOVE cleanup. Root cache
  miss orchestration and synthetic-root enumeration are still separate work.
  Other state/session retirement, export credential and capacity boundaries
  remain. No VFS public API extension was required for this pass.
- Logs: /tmp/chimera-nfs4-root-{build,initial,targeted,syntax,check}.log;
  focused-{debug,release}-final, quick-{debug,release}-final, late-targeted,
  metadata-stress-fixed, clang-debug-final, final-hygiene and diagnostics.txt
  under the same prefix. All model generation completed.
  Production-only patch: /tmp/chimera-nfs4-root-production.patch. Analyzer
  comparison script: /tmp/chimera-nfs4-root-diagnostics.py (same saved baseline
  as the LAYOUTCOMMIT pass). Full make -k check CTEST_PARALLEL=8 completed
  with exit 2 only for the recorded quick-test failures and existing analyzer
  findings. Final production corrections were rebuilt and fully retested in
  Debug/Release; ClangRelease included them and ClangDebug received a final
  incremental scan. KVM suites remain unregistered because oras is unavailable.


## September 29 NFS4 pseudo-root READDIR page consolidation (working tree)

User asked for another NFS batch. Pseudo-root READDIR now contributes a whole
response page to the shared encoder instead of allocating/submitting a separate
compound for every export. Removed the old asynchronous per-entry dispatch loop,
completion wrapper and public nfs4_root_readdir entry. No commit/push requested.
Production delta against /tmp/chimera-nfs4-pseudo-before: +271/-301 = -30 lines.

- nfs4_root_readdir_add appends PUTROOT/LOOKUP_PATH pairs and one final checkpoint
  to the caller's compound. It snapshots export names/paths/ids under the existing
  export-list lock, reserves page storage during construction, and bounds rows by
  maxcount, the arena's 8192-byte reserve and remaining VFS operation capacity.
  Rows that cannot be returned are not queried. Sparse requests retain the
  256-byte attribute allocation; broad requests reserve a larger bound. The
  256-byte page allowance, positional cookies, zero verifier and request
  credential used for pseudo-root listing are unchanged.
- Each lookup's prepare callback rechecks name/id/path under exports_lock on every
  attempt. Removed or repointed exports return DELAY, including replacement with
  the same explicit id. The helper sets the wire verification status explicitly:
  the generic errno mapping would otherwise turn EAGAIN into SERVERFAULT. Name
  and path strings remain owned through completion; no live export pointer escapes.
- Attribute callbacks write only private, preallocated reply buffers. Every
  successful retry overwrites all output fields; a failed page never publishes
  partial entries. The common finish handler owns retry, cleanup and accepted
  publication. The synthetic protocol current FH remains unchanged throughout,
  regardless of the last backing path resolved. No VFS public API changes.
- A new test exposed an existing missing-attributes case: LOOKUP_PATH on the
  physical VFS root has no final component to provide attrs. Empty normalized
  export paths now append OPEN/GETATTR, and attribute projection supplies the
  current FH when the backend's attrs omit it. Final symlinks retain no-follow
  semantics; ordinary multi-component export paths still use LOOKUP_PATH.
- Added four accepted/retry wire suites: compound_{v40,adoption}_pseudo_{accepted,
  retry}. They cover a 44-entry page in one VFS submission, signed alias handles,
  multi-component targets, final symlinks, physical root, small pages, EOF,
  reserved/out-of-range cookies, TOOSMALL, empty namespace, a successful lookup
  followed by a missing export, and suppression of a WRITE after failed READDIR.
  A pending-finish rendezvous replaces an export with the same id but a new path;
  retry returns DELAY before GETFH and a fresh request sees the new inode.
- A 444-export fixture exceeds the 128 KiB arena, verifies complete continuation
  without duplicate/missing names, and keeps the following GETFH intact. Pynfs
  decodes entry chains recursively and hit its default recursion limit; the
  fixture temporarily raises it to 4096, then restores it. The REST fixture uses
  single-component export names (REST rejects a nested export name with HTTP 400).
- Final review reproduced an ASan crash with broad attribute requests: the old
  256-byte buffer overflowed into the next preallocated entry. The attribute
  marshaller's size argument bounds ACL inclusion, not subsequent fixed fields.
  Construction now bounds fixed attributes by the requested bitmap (40 bytes
  each, with the larger signed-FH bound) plus the existing ACL allowance. All
  four wire suites enumerate broad-attribute pages and verify their continuation
  and trailing fields. Failure log: /tmp/chimera-nfs4-pseudo-broad.log; corrected
  4/4 run: /tmp/chimera-nfs4-pseudo-broad-fixed.log. No production edits after this
  correction; full make check was restarted to verify the final code.
- Existing POSIX pseudo-root/root-export tests on memfs and linux pass. Final
  focused Debug and Release suites each pass 46/46 (42 VFS/NFS suites plus four
  root integration tests). Full make -k check CTEST_PARALLEL=8 completed;
  Release's quick sweep passed 270/275 with the five known remote-pNFS and
  SMB/POSIX failures. Debug passed 269/275: those five plus the already-recorded
  intermittent FUSE compound-lock shutdown abort. Repetition reproduced it and
  the retained log again says "fuse thread destroyed with 1 active requests" at
  fuse.c:267. NFS is disabled in that fixture; this remains an unfixed teardown
  issue, not a newly established NFS regression. Evidence:
  /tmp/chimera-nfs4-pseudo-fuse-{lock-repeat.log,abort.debug.log}.
  Clang diagnostics exactly match the baseline: 41 signatures / 110 occurrences,
  with no new or missing messages. Both scan-build stages completed without new
  reports (ccache replays the existing warning output; do not claim the old
  findings were fixed). All model generation completed. make syntax, formatting,
  SDK boundaries, REUSE, copyright and git diff --check pass. Full check exits 2
  only for the reported test failures; no commit or push was performed.
- Remaining specialized allocation sites: cold root-FH resolution in nfs4_root.c,
  two LAYOUTGET builders in nfs4_pnfs.c, and accepted pNFS REMOVE cleanup, plus the
  common encoder. Positional cookie behavior across export-list mutations and the
  pseudo-root ACL allowance remain unchanged. Backend transaction
  rollback and cross-filesystem atomicity are outside this pass.
- Next-pass review target: the cold root resolver still snapshots the root export
  but validates only root_export_id before publishing its cache. Reusing that id
  with a changed path during resolution can satisfy the check; this path needs
  the common builder's snapshot revalidation and accepted publication rules.
  Its standalone completion also has no common finish-EAGAIN retry handling.
- Follow-up audit target: ordinary chimera_nfs4_readdir_entry_fill still uses
  a 256-byte fixed allowance plus a stored ACL's size. Check broad fixed-field
  requests there too; the shared marshaller does not enforce that total bound.
- Logs: /tmp/chimera-nfs4-pseudo-{build,initial,targeted,large,syntax,check,
  focused-debug,focused-release,broad,broad-fixed,correction-debug-build,
  correction-release-build}.log. The superseded check before the buffer fix is
  retained as /tmp/chimera-nfs4-pseudo-check-before-attr-fix.log. Final diagnostic
  comparison: /tmp/chimera-nfs4-pseudo-diagnostics.txt. Isolated production patch:
  /tmp/chimera-nfs4-pseudo-production.patch. Diagnostic comparison script:
  /tmp/chimera-nfs4-pseudo-diagnostics.py (same baseline as the preceding passes).

## September 29 ordinary NFS4 READDIR attribute sizing (working tree)

User asked to address the ordinary READDIR sizing concern from the pseudo-root
pass. The regression is confirmed and fixed. No commit/push requested.

- Before the fix, the new broad-attribute wire test failed decoding
  CHANGE_ATTR_TYPE: value 1852255537 (directory-name bytes) was returned as an enum.
  Ordinary entry storage reserved only 256 fixed bytes plus a stored ACL's bound;
  the marshaller could write beyond that allocation into subsequent entries.
  Reproducer: /tmp/chimera-nfs4-readdir-sizing-before.log.
- Moved the pseudo-root's fixed-attribute capacity calculation to the shared
  chimera_nfs4_attr_capacity helper in nfs4_attr.h. It bounds requested fixed
  fields (40 bytes each, with the larger wrapped-FH allowance) and accepts a
  caller-provided ACL capacity. Pseudo-root behavior is unchanged.
- Ordinary and named-attribute READDIR now reserve that safe bound plus the
  requested stored or mode-synthesized ACL. Unrequested ACLs allocate no extra
  storage. After encoding, the last opaque allocation is trimmed to its actual
  size with 8-byte arena alignment. maxcount is charged actual XDR entry bytes
  (24 fixed bytes plus padded name, returned bitmap, and encoded attributes),
  independently of temporary capacity and C struct sizes. The 8192-byte arena
  reserve and whole-attempt reset/publication rules remain intact.
- Production-only delta versus /tmp/chimera-nfs4-readdir-sizing-before:
  +57/-37 = +20 lines across nfs4_attr.h, nfs4_proc_readdir.c and nfs4_root.c.
  No VFS API change or new independent compound builder.
- Added eight accepted/retry wire suites: compound_{v40,adoption}_
  {readdir,readdir_linux}_{accepted,retry}, on memfs and Linux. They cover broad
  fixed fields with/without ACL, maximum numeric uid/gid strings, exact-fit
  maxcount and one-byte-short TOOSMALL, stored 25-ACE ACLs, unrequested ACLs,
  2048-byte page continuation, two READDIRs with intervening GETATTR in one
  compound, stopped mutation suffixes, and actual re-encoded response size.
  Linux free-space attributes can change while logs/builds write to the same
  filesystem; the equality comparison requests stable fields, while separate
  broad-request cases still encode the volatile fields.
- Expanded existing named-attribute READDIR tests to request broad word-0/1
  fields. The finish fixture explicitly allows read-only metadata for the new
  feature names and counts completed READDIR operations. The wrapper asserts
  both a two-page rejected compound and a failed READDIR prefix were retried.
  Mutations remain excluded from synthetic finish rejection.
- Targeted Debug run passed 15/15. Combined focused Debug and Release runs each
  passed 54/54. Both quick sweeps passed 270/275, with the same five persistent
  failures. The intermittent FUSE/SMB DOC aborts did not recur. Full
  make -k check CTEST_PARALLEL=8 completed with exit 2 for those test failures
  and two existing analyzer findings in each Clang configuration. Model
  generation, SDK boundaries, formatting, licensing and copyright checks pass.
  Baseline: five persistent quick-test failures (remote pNFS on memfs/diskfs/
  cairn, POSIX batch_smb_memfs and strict_smb), plus known intermittent FUSE
  shutdown / SMB DOC fixture aborts; 41 Clang diagnostic signatures and 110
  occurrences, exactly matched by this run with no new or removed diagnostics.
  make syntax and git diff --check pass.
- Logs: /tmp/chimera-nfs4-readdir-sizing-{before,configure,build,syntax,
  targeted-final,focused-debug,focused-release,check}.log. Production patch:
  /tmp/chimera-nfs4-readdir-sizing-production.patch. Diagnostic comparison:
  /tmp/chimera-nfs4-readdir-sizing-diagnostics.{py,txt}, using the same saved
  baseline.


## September 30 NFS4 cold root resolution (working tree)

User approved the cold-root resolver as the next NFS4 consolidation batch.
No commit/push requested. Implementation and verification are complete.

- Removed the independent nfs4_root_export_fh_resolve builder/context/completion.
  A cold cache now uses a namespace-prelude mode of nfs4_vfs_submit, with the
  common allocation, disposal, attempt reset and finish-retry handler. Root
  resolution and export entry share the path encoder and accepted cache
  publication helper. The prelude logs count=0 because it completes no wire op;
  LOOKUP/SECINFO/LOOKUPP resume only after it finishes. Namespace/credential
  selection is still a boundary, deliberately separate from this batch.
- The owned export snapshot is compared by id, name, path, access, squash,
  anonymous uid/gid and allowed security flavors before execution on each
  attempt and under exports_lock before accepted cache publication. Reusing
  an export id cannot prime the old root after path or policy replacement.
  A stale snapshot returns DELAY; LOOKUP/SECINFO no longer swallow that result
  and fall back to a physical entry. LOOKUPP also propagates it.
- Finish EAGAIN uses the shared bounded retry; a terminal finish error fails
  the pending wire op directly and stops its suffix. Ordinary resolution
  failures retain their namespace behavior. The current protocol FH, export,
  stateid and request credential stay intact across the prelude. Empty root
  paths also use the shared path, and final symlinks are still followed.
- Added four accepted/retry suites: compound_{v40,adoption}_cold_root_
  {accepted,retry}. Cold/warm junction LOOKUP, SECINFO, root and sibling
  LOOKUPP, missing root paths, the physical VFS root, symlinks and exact
  submission spans are covered. Pending-finish rendezvous replaces a root
  with the same id but a changed path, access or squash/anonymous identity.
  Both rejected and accepted finishes must reject the stale result; a fresh
  request must resolve the replacement. Mutation suffixes remain unexecuted.
  A terminal EIO finish is also tested. The fixture still excludes filesystem
  mutations from synthetic finish rejection.
- The new retry test against the old binary reproduced cold_root_lookup
  returning DELAY instead of completing the junction: the standalone resolver
  had not retried its rejected finish. Log:
  /tmp/chimera-nfs4-cold-root-before.log. An initial new test expected LOOKUPP
  from the symlink target itself to return its parent, but that object was now
  the namespace root and correctly returned NOENT; the test now starts at the
  distinct rootfs mount root. All eight cold-root/namespace suites pass.
- Production delta against /tmp/chimera-nfs4-cold-root-before is +154/-180,
  a net reduction of 26 lines across four files. No VFS API change.
- The first full check found a Release -Werror=maybe-uninitialized diagnostic
  for avail: the new prelude skipped its initialization. Moved the reply-budget
  initialization before both paths. Stopped only this task's check process
  group and restarted full verification; another user's check under
  /worktrees/extended-regress was left untouched. Earlier log:
  /tmp/chimera-nfs4-cold-root-check-before-release-fix.log.
- Final focused Debug and Release suites each pass 58/58, after the Release
  compiler correction. Full make -k check CTEST_PARALLEL=8 completed with
  exit 2 for the test failures and existing analyzer findings below.
  Release quick passed 269/275: the five persistent failures plus
  smb/mbt/batch_memfs_encrypted311, whose durable trace
  smb2Durable_stepDurable_300_0x41_3.itf.json reported an unexpected pending
  CREATE 'b' at state 295. Three consecutive isolated repeats passed without
  code changes; the extra failure is not reproduced or fixed. This fixture
  enables SMB only (smb2_mbt_common.h:1102), not the changed NFS root paths.
  Log: /tmp/chimera-nfs4-cold-root-smb-repeat.log. Debug quick passed
  270/275 with only the five persistent failures, including a pass of the
  encrypted-SMB case. The known intermittent FUSE/SMB DOC aborts did not recur.
- Both Clang builds completed with two existing encoder findings each. All
  diagnostics exactly match the baseline: 41 signatures / 110 occurrences,
  with no additions or removals. All model generation completed. make syntax,
  SDK boundaries, formatting, REUSE, copyright and git diff --check pass.
  KVM suites remain unregistered because oras is unavailable.
- Remaining specialized builders: two LAYOUTGET phases and accepted pNFS REMOVE
  backing cleanup, plus the common encoder. The cold-root prelude now shares
  lifecycle/encoding but still precedes namespace and credential selection.
  Broader junction/LOOKUPP coalescing and backend rollback remain separate work.
- Logs: /tmp/chimera-nfs4-cold-root-{before,configure,syntax,build,
  build-final,debug-rebuild,targeted,targeted2,targeted-final,focused-debug,
  focused-release,check}.log.
  Isolated production patch: /tmp/chimera-nfs4-cold-root-production.patch.
  Diagnostic comparison: /tmp/chimera-nfs4-cold-root-diagnostics.{py,txt}.


### NFS4 execution-time namespace coalescing (September 30, 2026)

- User asked to continue broader coalescing after cold-root consolidation.
  Ordinary LOOKUPP now stays in the common compound after LOOKUP, PUTFH and
  RESTOREFH, including when a real "/" export exists. LOOKUP/SECINFO names that
  coincide with exports no longer force a boundary when the execution cursor
  is a descendant. LOOKUPP at the namespace root returns NOENT in the compound.
- Construction snapshots the namespace-root policy and possible-junction
  classification. Pure prepare callbacks validate them and inspect the VFS
  cursor, at the first checkpoint and again at the actual namespace operation.
  A root resolved by earlier PUTROOTFH in the same attempt can be used privately
  without publishing its cache entry before finish. Changes require DELAY.
- Actual export junctions, mount-root parent crossings and cold root comparisons
  still need dispatcher credential/namespace selection. They defer that wire op
  and its entire suffix, then accept/publish only the successful prefix. A
  one-shot request flag dispatches the boundary directly to avoid re-coalescing
  it forever; an empty prefix resumes correctly. No new VFS API or builder.
- Important API contract: op_args only exposes the currently preparing op;
  it returns NULL for future ops during execution. The prepare callback uses
  op_skip for itself; the common completion gate uses op_edit to skip future
  slots. These skips and the private deferred marker reset on finish retry.
  Deferred callbacks must not publish a cursor, SECINFO consumption or replies.
- The accepted prefix publishes OPEN/owner journals and saved/current stateids
  before namespace dispatch resumes. Undispatched WRITE payloads remain owned
  by the request across disposal; terminal finish failures leave suffix release
  to the dispatcher's normal truncation sweep. Backend rollback is still future
  work; synthetic EAGAIN injection remains limited to read-only operations.
- Added accepted-boundary tracing. The test parser pairs each boundary with its
  preceding submission and checks the actual accepted prefix, while ordinary
  coalescing cases still require one submission. Rejected attempts cannot emit
  a boundary. Root-cache peek helper became unused and was removed. Production
  delta against /tmp/chimera-nfs4-namespace-coalescing-before: +151/-137, net +14
  lines across six files. Isolated patch:
  /tmp/chimera-nfs4-coalescing-production.patch.
- Extended the existing v4.0/v4.2 namespace suites with moving/nested/saved
  cursors, shadowed descendant names, version-specific SECINFO semantics,
  root-parent and non-directory vetoes, OPEN/SAVEFH/current-stateid publication,
  WRITE data on both sides of an actual junction, v4.0 owner replay, an empty
  deferred prefix, and controlled prefix finish EAGAIN/EIO. Initial focused
  Debug run passed 58/58; empty-prefix additions passed all four namespace
  variants. Final Debug/Release/full-check validation follows below.
- Remaining: cross-export credential changes, synthetic namespace operations,
  repeated root entry, budgets and other explicitly unsupported encodings can
  still split spans. Cold resolution shares lifecycle but remains a prelude.
  Specialized independent builders remain LAYOUTGET phases and accepted pNFS
  REMOVE backing cleanup. Backend compound transaction hooks/rollback remain
  outside this frontend pass.
- Final review found a warm-cache precedence bug in this pass: a PUTROOTFH
  that freshly resolved a replaced directory was compared against the older
  cached root handle. The new regression reproduced LOOKUPP returning OK and
  the physical parent instead of NOENT. The checkpoint now always prefers an
  earlier root-entry result from its own attempt over the shared cache. Tests
  replace the export directory twice without changing its configured path,
  covering both parent refusal and sibling-junction selection. Before-fix log:
  /tmp/chimera-nfs4-coalescing-stale-root-before.log.
- Final verification completed: focused Debug and Release each pass 58/58,
  including the stale-root-cache regressions and all namespace accepted/retry
  cases. Both quick suites pass 270/275. Only the five persistent failures
  remain: nfs/mbt/pnfs_{memfs,diskfs,cairn}_remote and
  posix/mbt/{batch_smb_memfs,strict_smb}. Their concrete mismatch classes match
  the previous cold-root log. No intermittent encrypted-SMB or FUSE failure
  appeared in this run.
- Full make -k check CTEST_PARALLEL=8 completed with exit 2 for those failures
  and existing analyzer findings. Clang warning messages/counts exactly match
  the baseline: 41 signatures, 110 occurrences, no additions or removals.
  HTML report counts differ: Debug 3, Release 4 (previously 2 each). The extra
  reports are test_nfs_persist.c:234 in both and nfs_nlm.c:236 in Release;
  both warnings were already present in the baseline logs. The other two
  reports are the existing encoder combine/open_replay findings. No new
  diagnostic is inferred from the changed report-file count.
- All model generation finished. make syntax, formatting, SDK include boundary,
  REUSE, copyright and git diff --check pass. Logs:
  /tmp/chimera-nfs4-coalescing-{syntax,build-final,focused-debug,focused-release,
  check,stale-root-before,empty-prefix}.log. Diagnostic comparison is saved at
  /tmp/chimera-nfs4-coalescing-diagnostics.{py,txt}.

### NFS4 same-export root re-entry coalescing (September 30, 2026)

- Latest user request: continue the conversion. This pass admits repeated
  PUTROOTFH/PUTPUBFH into the existing shared compound when they select the
  same export and effective credential. A span beginning with a file PUTFH can
  reset to that export's root and continue. Actual cross-export selection,
  synthetic roots and deferred WRONGSEC still use namespace dispatch.
- Capture the root export before scanning the prefix's policy gates. Compare
  its derived credential against the sequence credential, including active
  supplementary groups, origin and flags. A plan containing root entry checks
  the frozen policy at each wire-operation prepare (including the prefix) and
  again at the root path lookup. Changed identity/path/access/security requires
  DELAY; a finish retry cannot run its prefix with obsolete export policy.
- Each root entry encodes PUTROOT plus fresh configured-path resolution (or
  GETFH for the empty VFS path). It clears the current stateid/attribute-directory
  cursor while preserving saved FH/stateid. No warm root FH shortcut, new VFS
  API, or independent builder. The last successful root map is attempt-private,
  resets on finish retry, and supplies subsequent namespace comparisons. Only
  this last root result updates the shared cache after accepted finish: earlier
  roots may have been replaced by mutations inside the same accepted span.
- Shared v4.0/v4.2 namespace tests cover repeated public/root entry, file PUTFH,
  saved file and attribute-directory cursors, SECINFO consumption, v4.2 saved
  and cleared current stateids, physical VFS-root paths, failed root resolution,
  and RENAME/CREATE followed by a fresh second root entry and parent veto.
  Pending-finish replacements cover path, squash identity and security changes;
  exact traces require one span, and retry logs must show three path resolutions
  in one rejected attempt. Mutating spans are not subjected to synthetic EAGAIN
  because backend rollback is still outside this frontend pass.
- The new tests fail against the old binary solely on span assertions: eight
  operations split 3+3+2 instead of one span, in both v4.0 and v4.2.
  Log: /tmp/chimera-nfs4-root-coalescing-before.log. All 58 focused suites pass
  in both builds before the final snapshot-ordering refinement. Final checks
  will be recorded below. First full check was stopped only after verifying
  its stdout path, to restart against that last refinement.
- Turn snapshot: /tmp/chimera-nfs4-root-coalescing-before/.
  Logs: /tmp/chimera-nfs4-root-coalescing-{syntax,build-debug,build-release,
  namespace-debug,focused-debug,focused-release,check}.log.
- Remaining: real export/credential transitions; synthetic namespace boundaries;
  cold root resolution preludes; operation/reply budgets; LAYOUTGET's specialized
  phases and accepted pNFS REMOVE backing cleanup. Backend transaction hooks and
  rollback remain future work. This pass does not claim all NFS spans are 1:1.
- Final focused reruns after snapshot ordering pass 58/58 in both Debug and
  Release. Production delta: +85/-22, net +63 lines in nfs4_compound_vfs.c.
  Release quick passed 269/275: the five baseline failures plus
  smb/mbt/stream_probe_memfs. Its mode-3 last-peer disconnect check reopened a
  stream successfully (0) instead of OBJECT_NAME_NOT_FOUND (0xc0000034).
  Three consecutive isolated ctest reruns passed without code changes; this
  remains an unresolved intermittent observation. Rerun log:
  /tmp/chimera-nfs4-root-coalescing-stream-rerun.log.
- Debug quick initially passed 267/275. Besides the five baseline failures,
  s3/mbt/batch_diskfs aborted with libaio -28 (ENOSPC), posix/mbt/batch_io_uring
  first mismatched mkdir/write with errno 28 and then cascaded, and
  nfs/mbtdrc/batch_cairn aborted during the same disk-pressure period without
  an explicit cause in its ctest output. The filesystem was 99% full.
- Reclaimed about 8 GB from seven unused generated /tmp MBT image directories;
  verified no process cwd/root, FD, cmdline or memory-map references before
  deleting only those exact directories. Source, logs and other worktrees were
  preserved. The three affected Debug suites then passed (3/3, no code changes)
  in /tmp/chimera-nfs4-root-coalescing-space-rerun.log. Do not claim a diagnosed
  code fix for the otherwise unexplained NFS DRC abort.
- After normalizing generated inode values, persistent mismatch classes outside
  the ENOSPC-affected POSIX trace match the previous coalescing baseline.
  Comparison script/output:
  /tmp/chimera-nfs4-root-coalescing-failures.{py,txt}.
- Final make -k check CTEST_PARALLEL=8 completed with exit 2. All model generation,
  formatting, SDK include boundaries, REUSE and copyright checks passed. Both
  Clang builds emitted the two existing encoder HTML findings; warning content
  exactly matches the baseline: 41 signatures, 110 occurrences, no additions or
  removals. Diagnostic comparison:
  /tmp/chimera-nfs4-root-coalescing-diagnostics.{py,txt}.
- Final focused results: Debug 58/58, Release 58/58. Full quick results retain
  the raw 267/275 Debug and 269/275 Release counts above; the three extra Debug
  failures and the Release stream failure passed their isolated reruns. The
  five persistent pNFS/POSIX-over-SMB failures and existing analyzer findings
  remain unresolved. Do not describe the full check as green. Final
  git diff --check passed. Before the next large sweep, check available disk
  space: concurrent generated filesystem images can temporarily consume GBs.

### NFS4 inline cold-root comparison and virtual-root spans (October 1, 2026)

- User accepted the next cold/synthetic-root coalescing pass. Ordinary cold
  root comparisons now use conditional PUTROOT + LOOKUP_PATH/GETFH before the
  shared encoder's saved/current cursor seeds. Warm cache results skip both
  operations. Each attempt privately holds the root handle; only accepted
  finish can publish it under a matching locked export-policy snapshot.
- Actual export crossings still defer the wire operation/suffix for credential
  selection. No zero-wire root-resolution prelude is needed in these normal
  spans. Standalone cold resolution remains as a dispatcher/budget fallback;
  do not claim that helper was eliminated. A later wire root resolution takes
  cache-publication precedence over the early comparison probe.
- Failed root lookup leaves ordinary physical LOOKUP/SECINFO available; a
  mount-root LOOKUPP whose configured parent cannot resolve returns SERVERFAULT.
  Policy changes and root-probe EAGAIN map to DELAY, including before the first
  wire operation. Changed snapshots during accepted finish veto a deferred
  continuation while preserving its accepted prefix. Terminal finish failures
  publish neither the prefix nor cache. Retry resets all private root results.
- Virtual namespace runs now coalesce PUTROOTFH/PUTPUBFH, synthetic PUTFH,
  GETFH/GETATTR/ACCESS, SAVEFH/RESTOREFH, LOOKUPP parent refusal,
  SECINFO_NO_NAME, TEST_STATEID, and bounded pseudo-root READDIR pages.
  Protocol checkpoints never send the synthetic handle to a backend. READDIR
  may visit multiple real exports; current/saved protocol cursors remain virtual.
  GETATTR replies stage privately using existing root attributes and reply sizing.
  Installing a real root invalidates retried virtual prefixes. Named SECINFO,
  actual export selection, and unsupported virtual operations remain boundaries.
  A single virtual non-READDIR operation retains its direct protocol handler.
- Regression coverage: cold/warm comparisons, failed root views, saved cursors,
  snapshot path/security/squash replacement, accepted/rejected/terminal finish,
  virtual metadata/ACCESS, inherited and consumed saved cursors, parent errors,
  two virtual READDIR pages in one span, attribute-directory to pseudo-root
  transition, and real-root installation while virtual finish is pending.
  New cold tests also reproduce an existing DELAY loop when the configured
  root cannot resolve but an ordinary physical lookup should succeed.
- Old Release binary fails the new cold failed-view regression and virtual
  exact-span regressions (4/4 expected failures). During implementation, retry
  tests caught missing EAGAIN-to-DELAY mapping; final review caught an inherited
  attrdir flag on synthetic PUTFH. Both fixed, with regression coverage.
- Production delta: +230/-42, net +188 lines, entirely in the shared encoder;
  no new VFS API or independent builder. Snapshot and production patch:
  /tmp/chimera-nfs4-inline-root-before/ and
  /tmp/chimera-nfs4-inline-root-production.patch.
- Initial broad focused run hit a full filesystem; some trace logs were cut
  off and those results are invalid. Removed eight exact inactive generated
  NFS3/POSIX cairn test-image directories after checking process references,
  preserving logs/source and live work. Disk-pressure cleanup is not a code fix.
- Verification logs use
  /tmp/chimera-nfs4-inline-root-{syntax,build-debug,build-release,focused-debug,
  focused-release,cold-debug,pseudo-debug,before,check}.log.
- Final focused suites pass 58/58 in Debug and Release. Full quick Release
  passes 270/275; Debug passes 269/275. The five persistent remote-pNFS and
  POSIX-over-SMB failures retain their baseline mismatch signatures (no new
  normalized mismatch). Debug also reproduces the previously recorded FUSE
  compound-lock shutdown abort. Three isolated reruns passed; a subsequent
  until-fail:20 run reproduced it on attempt 12. Retained log again says "fuse thread destroyed
  with 1 active requests" at fuse.c:267, with the NFS server disabled. No FUSE
  code changed. Evidence: /tmp/chimera-nfs4-inline-root-fuse-{rerun,repeat}.log
  and /tmp/chimera-nfs4-inline-root-fuse-abort.debug.log. Passing reruns do not
  fix or dismiss the reproduced teardown issue.
- Full make -k check CTEST_PARALLEL=8 completed with exit 2 for the recorded
  test failures and existing analyzer findings. Both Clang stages finished all
  model generation and reported the same two encoder findings (combine and
  open_replay). Warning content/counts exactly match the baseline: 41 signatures,
  110 occurrences, no new/excess or missing messages. Comparison scripts and
  outputs are /tmp/chimera-nfs4-inline-root-{diagnostics,failures}.{py,txt}.
- make syntax, formatting checks, SDK include boundaries, REUSE, copyright and
  final git diff --check pass. Final quick counts remain Release 270/275 and
  Debug 269/275; focused suites remain 58/58 in each build. Full check is not
  green. No production edits followed focused validation, no commit or push.

## NFSv4 explicit export and credential transitions (2026-10-01)

- Current task: coalesce explicit export selections, preserving the retry/publication
  contract. Cross-export real PUTFH, PUTROOTFH/PUTPUBFH and RESTOREFH now use
  existing VFS operation groups inside one compound. Groups stop at the first
  error and share the compound finish/retry lifecycle; no new VFS API or builder.
- Deduplicated immutable export-policy snapshots own credentials derived afresh
  from orig_cred. Each operation maps to its current and saved export identity.
  Prepare validates policy snapshots; changed path/access/squash/security causes
  DELAY before the affected operation. Later disallowed PUTFH returns WRONGSEC
  in the same compound, preserving the successful prefix.
- VFS groups clear both cursors. Group entry explicitly reseeds SAVEFH from the
  attempt-private protocol handle before selecting the next current cursor.
  Attribute-directory markers stay in NFS; backend cursors receive their bases.
  Retry restores inherited saved FH and stateid journals. Accepted publication
  applies each result's identity, then the last successful cursor's identity.
  Delegation grants temporarily select their own OPEN's accepted identity.
- GETATTR staging, VERIFY/NVERIFY and READDIR entry marshalling take an explicit
  export id. OPEN's frontend ACL check uses the operation's frozen credential.
  Read-only checks execute under the frozen current/saved policies, including
  LINK/RENAME sources saved earlier within this same span.
- New shared v4.0/v4.2 namespace tests cover two distinct anonymous uid/gid
  mappings and restoration of the original uid/gid, ACCESS isolation, staged
  ACL/FILEHANDLE replies, VERIFY and READDIR signing, saved attrdir restoration,
  source/current ROFS with suppressed mutation suffixes, WRONGSEC prefix, and
  export squash/security/access replacement while a read-only finish is pending.
  Existing tests cover coalesced OPEN/stateid restoration across a real junction.
- The old Release binary fails the new span assertion: a 15-operation selection
  sequence uses five compounds; the new encoder uses one. Evidence:
  /tmp/chimera-nfs4-export-transitions-before.log.
- Test fixture fix: attach the finish observer before execution, then decide
  retry eligibility from executed operations at finish. Skipped/unexecuted
  mutation suffixes do not forbid retrying a read-only accepted prefix. Any
  executed mutation still forbids synthetic rejection; this supplies no backend
  rollback. Also fixed initial handling of an absent synthetic saved-export
  snapshot, which otherwise changed a legacy LINK XDEV answer to DELAY.
- Production delta before final validation: +267/-103, net +164 across six NFS
  files. Snapshot: /tmp/chimera-nfs4-export-transitions-before/.
  Logs use /tmp/chimera-nfs4-export-transitions-{syntax,build-debug,focused-debug,
  focused-release,namespace-debug,before,check}.log.
- Final verification: make syntax and focused Debug/Release each pass 58/58.
  Full quick Debug/Release each pass 270/275; the five persistent remote-pNFS
  and POSIX-over-SMB failures exactly match the baseline mismatch signatures.
  The known intermittent FUSE teardown issue did not recur in this sweep and
  remains unresolved. Both Clang stages finished all model generation and
  report the same two findings (combine and open_replay). Warning signatures
  and counts exactly match the baseline: 41 signatures, 110 occurrences,
  nothing added or missing. Formatting, SDK header boundaries, REUSE,
  copyright and final git diff --check pass. Full make -k check completed with
  exit 2 for those known failures and analyzer findings; the sweep is not green.
  Comparisons: /tmp/chimera-nfs4-export-transitions-{diagnostics,failures}.{py,txt}.
  No production edits followed focused validation; no commit or push.
- Remaining namespace boundaries: dynamic junction LOOKUP/SECINFO and mount-root
  LOOKUPP; real/synthetic-root transitions; deferred ROOT/PUB WRONGSEC semantics.
  Existing reply/operation budgets, specialized LAYOUTGET, accepted pNFS backing
  cleanup and protocol lifecycle paths remain. Backend transactions are future work.

## NFSv4 known-root junctions and named SECINFO (2026-10-02)

- Continuing namespace coalescing after explicit export/credential groups.
  Known-root LOOKUP junctions now select their frozen export identity in the
  shared compound, using existing VFS groups. ROOT/PUB and SAVE/RESTORE retain
  root provenance; restoring an older root after a newer ROOT does not imply
  that the restored cursor is the current namespace root. Execution validates
  the root handle, root policy, component mapping, target policy and security
  flavor before entering the export. Final symlinks remain unfollowed and a
  slash-only target path still returns NOENT, matching the previous handler.
- Named SECINFO now stays inline for real-root junctions discovered during
  execution and for synthetic roots. It advertises the frozen target policy
  without entering the export or resolving its backing path. Ordinary physical
  names advertise the current export. Conditional helper skipping preserves
  v4.0's current/saved directory and v4.1+'s consumed-current-FH semantics.
  Target policy/component snapshots are revalidated on retry; changed targets
  return DELAY before the suffix. Attempt-local selection resets on retry.
- Shared v4.0/v4.2 accepted/retry regressions cover repeated junction groups,
  signed handles for aliases, saved root identity, inaccessible/missing-backed
  SECINFO, ordinary names matching export names, final symlinks, slash-only
  targets, WRONGSEC mutation suppression, runtime SECINFO policy replacement,
  saved older-root provenance and missing synthetic SECINFO. Existing squash,
  OPEN and export replacement tests now assert the longer coalesced spans.
- Cold SECINFO finish-retry cases now include a failing VERIFY before CREATE:
  SECINFO no longer defers, so without that guard CREATE would actually run,
  correctly making the read-only rejection fixture ineligible. No mutation
  rollback is simulated or added.
- Production delta: +185/-19, net +166, only nfs4_compound_vfs.c. No new VFS API
  or independent builder. Snapshot /tmp/chimera-nfs4-junctions-before/ and patch
  /tmp/chimera-nfs4-junctions-production.patch. Old Release fails the new ROOT,
  LOOKUP, GETFH span: two compounds instead of one. Evidence in
  /tmp/chimera-nfs4-junctions-before.log.
- Final focused suites pass 58/58 in Debug and Release. Complete quick Release
  and the full Debug rerun each pass 270/275 with exactly the five preceding
  baseline failures and mismatch signatures. No new/missing normalized mismatch.
  Both Clang stages completed all model generation and reported exactly the
  same two encoder findings (combine and open_replay). Warning content/counts
  exactly match the baseline: 41 signatures, 110 occurrences, none new/missing.
  make syntax, formatting, SDK include boundaries, REUSE, copyright and final
  git diff --check pass. Full make -k check completed with exit 2 for the
  recorded failures/findings; it is not green. Comparisons are retained as
  /tmp/chimera-nfs4-junctions-{diagnostics,failures}.{py,txt}. The known intermittent
  FUSE teardown abort did not recur and remains unresolved. No production edits
  followed final focused validation; no commit or push.
  Logs use /tmp/chimera-nfs4-junctions-{syntax,focused-debug,focused-release,check}.log.
- First Debug quick sweep encountered transient ENOSPC/failed mkdtemp while
  another worktree ran ctest at -j48 on the same filesystem. Its stdout log is
  truncated; do not treat that sweep as valid final evidence. Preserved Testing
  logs in /tmp/chimera-nfs4-junctions-debug-first-sweep/. Five additional failed
  suites (NFS DRC io_uring/RDMA and SMB force-L2 plain/signed, replay plain) all
  passed in the completed full Debug rerun at -j4. Its complete evidence is at
  /tmp/chimera-nfs4-junctions-quick-debug-rerun.log. No files were manually
  removed: space recovered through normal test cleanup. Release quick passed
  270/275 with exactly the preceding baseline failures/mismatch signatures.
- Remaining namespace boundaries: arbitrary runtime junction LOOKUP (including
  inherited PUTFH and LOOKUPP-produced root cursors); mount-root LOOKUPP;
  real/synthetic transitions; deferred ROOT/PUB WRONGSEC. Dynamic credential
  selection cannot yet be represented by the construction-only VFS groups.
  Operation/reply budgets, specialized LAYOUTGET phases, accepted pNFS REMOVE
  cleanup, protocol lifecycle operations and future backend transactions remain.

## NFSv4 runtime-discovered junction LOOKUP (2026-10-03)

- Runtime junction LOOKUP now remains in the shared compound after inherited
  PUTFH, earlier namespace operations and RESTOREFH. Potential export names
  encode both the junction path and ordinary child lookup; attempt-private
  selection skips the unused helpers. Ordinary descendant names that happen
  to match an export retain the source export's credentials and identity.
- Added optional VFS group select_cred callout: after cursor reset and dependency
  checks, before any operation prepare, choose a borrowed immutable credential
  from earlier execution results. NULL uses the configured/default credential.
  Selection repeats on retry, cannot publish or transfer ownership, and leaves
  authorization errors to prepare. Existing group callers are zero-initialized.
  Groups retain one compound finish/retry lifecycle, without per-wire-op groups.
- NFS tracks current/saved identity in attempt-private state, restores both on
  retry, and resolves each operation's identity before policy checks. Group
  entry reseeds saved/current handles. Frozen source/target policies govern
  read-only checks, security flavors, squash credentials and signed replies.
  A first-operation runtime lookup keeps cold root resolution and inherited
  seeds in a prelude group inside the same compound. Entry-export runs can also
  resolve a cold root for later namespace comparisons.
- Shared v4.0/v4.2 regressions cover exact compound spans, distinct squash ids,
  saved identity restoration, ROFS/WRONGSEC mutation suppression, and target
  policy replacement at finish. A new retry test replaces the root's backing
  directory at the same configured path while read-only finish is pending:
  junction-to-physical and physical-to-junction retries return the correctly
  signed final handles. VFS tests cover selection from prior results, empty
  entry cursors, retry, default credentials and dependency-skipped groups.
- Cold LOOKUP retry fixtures now stop at a failing VERIFY before CREATE,
  because the formerly deferred suffix executes inline. Accepted finish preserves
  that operation error; rejected finish revalidates the changed snapshot. No
  synthetic rejection is allowed after an executed mutation; backend rollback
  remains future work. During development, tests caught use of GETFH attributes
  when checking the selected branch's LOOKUP result; that result gate is fixed.
- The old Release binary fails the cold inherited PUTFH/LOOKUP/GETFH exact-span
  assertion (two compounds instead of one). Focused final Debug and Release
  each pass 58/58. Full quick Debug and rebuilt Release each pass 270/275 with
  the five persistent remote-pNFS and POSIX-over-SMB failures. Final full quick
  reruns after the identity guard also pass 270/275 each, with exactly the
  baseline failures and mismatch signatures. No new/missing normalized mismatch.
- Full-check startup hit a stale Release CMAKE_MAKE_PROGRAM pointing to a
  deleted temporary Ninja wrapper. Reconfigured with /usr/bin/ninja and ran
  the complete Release build/quick suite separately. Focused results from the
  old binary are invalid and were replaced by a passing rebuilt run. An initial
  formatting failure was fixed; make syntax and syntax-check now pass.
- Clang found a new hypothetical NULL identity reaching junction path validation.
  Construction requires a target, but prepare now rejects its absence explicitly.
  Final targeted Debug analysis of the changed encoder reports only the two
  preceding findings (combine and open_replay); ClangRelease's full stage includes
  the guard and reports the same two encoder findings. Both full Clang stages
  completed all model generation. Replacing the initial Debug encoder diagnostics
  with its final rerun yields exactly the baseline 41 warning signatures and
  110 occurrences, with nothing new/excess or missing. The comparison script
  explicitly retains and replaces the initial three-finding result.
- Production delta: +280/-77, net +203 lines across the shared NFS encoder and
  VFS group API/executor. The executor adds seven lines; no independent builder.
- Snapshot: /tmp/chimera-nfs4-runtime-before/. Logs use
  /tmp/chimera-nfs4-runtime-{before,syntax,syntax-check,focused-debug,
  focused-release,check,release-check}.log. Patch and comparison evidence use
  /tmp/chimera-nfs4-runtime-production.patch and
  /tmp/chimera-nfs4-runtime-compare.{py,txt}.
- Final rerun logs are /tmp/chimera-nfs4-runtime-{debug-final,release-final,
  clang-debug-final}.log; focused logs above were overwritten by the final
  passing runs. make syntax, syntax-check, SDK include boundaries, REUSE,
  copyright and final git diff --check pass. Full make -k check completed
  with exit 2; its initial formatting/Release-cache failures were resolved by
  the separate checks/reruns, while known test and analyzer failures remain.
  The overall sweep is not green. The previously recorded intermittent FUSE
  teardown abort did not recur and remains unresolved. No production edits
  followed final focused validation. No commit or push.
- Remaining namespace boundaries: mount-root LOOKUPP; real/synthetic transitions;
  deferred ROOT/PUB WRONGSEC. Existing operation/reply budgets, specialized
  LAYOUTGET phases, accepted pNFS backing cleanup and protocol lifecycle paths
  also remain. No independent builder added; backend transaction hooks are
  still future work. No commit or push requested.

## NFSv4 full remaining-work census (2026-10-03)

- User challenged the repeated discovery of more conversion work. Short prior
  reports listed the next namespace tasks, not the complete remaining tail.
  Keep filesystem API adoption, shared-builder consolidation and wire-to-VFS
  finish/retry scope separate in future progress reports.
- Source audit recorded in docs/reviews/nfs4-compound-remaining-2026-10-03.md.
  Four NFS4 allocation/submission sites: shared encoder, accepted pNFS REMOVE
  cleanup, and two LAYOUTGET phases. Internal-procedure name scan found only
  the allowed release function. Shared whitelist recognizes 46 of 68 named
  dispatcher opcodes; all 22 others are accounted for in the report. Counts
  are not completion percentages or guarantees about every execution shape.
- Stable workstreams N4-01 namespace transitions; N4-02 state-owner binding and
  standalone state/claim fallback consolidation; N4-03 special-stateid admission
  after private CLOSE/CONFIRM/DOWNGRADE; N4-04 LAYOUTGET; N4-05 pNFS REMOVE batching;
  N4-06 operation/reply budgets; N4-07 error/protocol checkpoints; N4-08 protocol
  state retirement (DELEGRETURN/LAYOUTRETURN/FREE_STATEID/RELEASE_LOCKOWNER).
- Explicitly retain N4-02: v4.0 OPEN client binding can force a standalone OPEN;
  LOCK/LOCKU/CLOSE/CONFIRM/DOWNGRADE still have independent direct state mutation
  handlers when coalescing declines, even though common shapes are journaled.
  N4-04 completion callbacks honor aggregate finish errors but have no explicit
  server-side EAGAIN retry loop. Do not call either area complete.
- Session/replay admission, reference drops, transport/recovery maintenance and
  accepted DS cleanup have different scope from filesystem coalescing. State
  return operations are not automatically exempt merely because they are
  protocol-only: they require explicit boundary design to avoid recall deadlocks.
- Future passes should close named inventory subcases with exact-span and finish
  tests. No production edits or new test runs in this source-report-only pass.

## NFSv4 parallel completion pass (2026-10-03)

- User requested a dynamic fleet in this worktree, no worker builds/tests, then
  root integration and combined validation. Ownership was split across namespace,
  state/claim admission, pNFS, capacity/checkpoints, retirement, and mixed v4.0
  binding. Shared encoder/procs/common headers and VFS were integrated by one
  writer from frozen worker deltas. Workers also supplied source-only corrections
  and read-only integration reviews. No worker built, tested or formatted.
- Stable inventory updated in docs/reviews/nfs4-compound-remaining-2026-10-03.md.
  NFSv4 shared encoder now recognizes 56 dispatcher opcodes (51 switch cases
  plus five protocol predicates); the other twelve are client/session lifecycle.
  Explicit alloc/submit sites fell from four to two: shared encoder and accepted
  DS maintenance. Internal-procedure name scan finds only allowed release.
- NFS top-level production delta against the pre-fleet snapshot: 25 files,
  +3244/-3427 lines (net -183); excludes tests, VFS and SMB/FUSE follow-up fixes.
- N4-01: runtime mount-parent, synthetic/real cursor transitions, synthetic export
  lookup, conditional READDIR and ROOT/PUB security placement share attempts and
  frozen identity. Keep unsupported synthetic stateful shapes and the namespace
  fallback visible; do not report every possible cursor shape as complete.
- N4-02/03: removed independent direct state/claim handler implementations for
  OPEN/CLOSE/CONFIRM/DOWNGRADE/LOCK/LOCKU; standalone calls use shared admission.
  Per-operation v4.0 client pins, owner keys and accepted connection binding allow
  mixed clients in one run. Dynamic parent/range storage replaces standing-state
  scratch caps. Sparse/range/size I/O now carries private claim views and recall
  exclusions. CLONE destination is scoped; source still needs normal authorized
  state. COPY retains anonymous/scoped support.
- N4-04/05: LAYOUTGET native/materialized alternatives use one prebuilt sequence
  and accepted-only grant/device publication. Multiple pNFS REMOVEs share the
  namespace run, retaining victim identity/credentials for accepted DS cleanup.
  Cleanup remains an independent maintenance compound. External DS replacement
  atomicity and actual cross-backend rollback are future backend work.
- N4-06/07: separate 128 wire-map and 1024 helper limits; keep fitting prefixes;
  GETATTR uses actual compacted reply sizing before successors. Static/protocol
  errors and five pure response opcodes are shared checkpoints. Conservative
  READDIR/READ_PLUS/xattr bounds and the general reply floor still split runs.
- N4-08: DELEGRETURN/LAYOUTRETURN/FREE_STATEID/RELEASE_LOCKOWNER stage private
  retirement until accepted finish. Pending CB_GETATTR can coexist with return;
  combine publication revalidates lifetime. Wrong-client FREE and FILE layout
  return are rejected. Three same-object cases remain terminal DELAY in the same
  finish: returned-delegation namespace recall, layout grant/return then truncate,
  and layout return then regrant. FSID no-op, ALL ignoring layout type and partial
  layout return semantics are preexisting limitations, not closed by conversion.
- Integration/testing caught and fixed: delegation-disabled OPEN binding skip;
  pending-CB_GETATTR return rejection; saved synthetic LINK/RENAME error order;
  lone GETATTR/LAYOUTGET duplicate response floor; FREE swallowing principal
  admission failure; ALL retiring a retained but not-yet-staged fresh candidate
  on retry; grant reuse of an ALL-reserved layout destroyed by recall. Added
  identity, pending finish accept/retry/error, journal lifetime and exact-span
  coverage. Deliberately invalid SETXATTR enum tests bypass pynfs's client-side
  enum restriction. Old span assertions were updated only for verified larger
  runs, preserving protocol outcomes and suppressed-suffix checks.
- New anonymous WRITE_SAME wire case exposed memfs overwriting bytes outside a
  partial block with zeroes. Copy existing fragmented/shared block content before
  replacing the requested range; initialize holes with zeroes. New peer-claim and
  subsequent-unscoped VFS checks verify exclusions cannot escape their attempt.
- Full quick testing exposed SMB claim-rerun CREATE missing its truncate actor:
  only the initial opening gate had set it. The new correct anonymous admission
  rejected overwrite against its own deny-W. Bind actor/lease key when a truncate
  is built using a lent handle. Also release the separately owned seq_oh on generic
  retry-run error; the preceding failure leaked it and wedged shutdown. Existing
  durable and lease MBT traces reproduce both contexts. Preserve VFS admission.
- Optimized Release build fixes: IO_ADVISE rejects a missing state FH explicitly;
  device-cache insertion builds a local complete entry before assignment to avoid
  GCC's incorrect lock-subobject size inference. Tests include the shared layout
  constants header. No warning suppression or change to cache bounds.
- Removed direct-handler lock replay stub after handler removal; real wire tests
  cover replay/error order but not the old artificial GRACE-to-NO_GRACE policy
  toggle's exact call counts. No forced-revocation wire case for FREE was added;
  common ownership admission is source-audited. Finish rejection only targets
  read-only executed backend work; none of this proves backend mutation rollback.
- Intermediate SMB correction initially used op_edit during construction, which
  is allowed only inside an execution gate; changed it to op_args. The interim
  Release quick run was also invalidated by an overlapping shared-library rebuild
  (file-too-short loader errors). Preserve both logs as superseded; use only the
  later stable-binary runs. Debug's superseded run was interrupted before rebuilding.
- Focused final Debug and Release each pass 79/79. An intermediate stable full
  Release quick run passed 269/275: the prior five failures plus FUSE batch_memfs
  READDIR ESTALE on an old open/unlinked directory at trace step219. Three
  isolated repeats passed, but source review identified a concrete retention
  bug: OPENDIR's INFERRED|PATH flags could yield an unbacked descriptor. The
  non-root descriptor in the failing trace temporarily masked it by pinning
  the inode. This is not shared Debug/Release test storage or mount interference.
- A new root-only FUSE simulator regression deterministically failed with
  ESTALE after MKDIR/OPENDIR/RMDIR on the pre-fix Release binary. OPENDIR now
  uses READ_ONLY|DIRECTORY and keeps a backend reference through RELEASEDIR.
  READDIR to EOF and rewind after removal pass. All four quick and nine
  extended FUSE checks pass in both builds, including real kernel mounts;
  final targeted Clang FUSE analysis has no findings. This fixes the observed
  unlinked-directory ESTALE, not the separately recorded older teardown abort.
- Final Clang source diagnostics are 42 signatures/116 occurrences versus the
  prior 41/110: the sole added signature is delegation-retirement use-after-free,
  repeated in six compile units/configurations. Review confirms the reservation
  owns the extra reference across destroy; the analyzer assumes a lone reference.
  Existing NULL-file range-publication and the two encoder warnings remain.
  Final targeted SMB Debug analysis replaces that unit's earlier diagnostics
  and has the same five warnings; no new SMB warning from the retry correction.
- Final full Debug quick and final full Release quick each pass 270/275, with
  exactly the baseline five suites and normalized mismatch signatures, none
  added or missing: three remote-pNFS and two POSIX-over-SMB failures. Debug's
  full run precedes the FUSE-only correction and is supplemented by the final
  13 FUSE checks; Release's full run includes it. Final logs are
  quick-debug-stable.log and quick-release-fuse-final.log. Earlier Release
  quick-release-stable.log is superseded, preserved as failure evidence.
- Required make -k check completed with exit 2. Both full Clang stages and model
  generation completed. Intermediate compilation/test problems were corrected
  and validated separately; the baseline five tests and analyzer findings
  remain unresolved, so the overall sweep is not green. Final make syntax,
  syntax-check, SDK boundaries, REUSE, copyright and git diff --check pass.
  Final compare-final.txt retains/replaces the earlier SMB diagnostic unit
  and compares final full tests against the preceding baseline. No production
  edits followed final FUSE verification.
- Evidence: /tmp/chimera-nfs4-fleet-20261003/ (initial snapshots, ownership ledger,
  worker patches/reports, builds, focused/full tests and reviewed fixes). The
  layout arena case (60360-byte xattrs then compact GETATTR) returns RESOURCE on
  the pre-fix binary after a fitting LAYOUTGET, and passes on the fixed Release
  and Debug binaries. No commit or push requested or performed.

## 2026-10-03 — NFSv4 second dynamic fleet pass

User requested another review, parallel source-only changes, then centralized
build/test. Three workers handled synthetic identities, reply staging and layout
transitions; root handled namespace recall views and integration. All preexisting
worktree changes were preserved; no commit/push requested or performed.

Evidence: `/tmp/chimera-nfs4-fleet2-20261003/`, including immutable `before/`,
WORKFLOW.md, frozen scratch patches, worker reports and all failed/final logs.
Inventory: `docs/reviews/nfs4-compound-remaining-2026-10-03.md`.

Changes and verified regressions:
- Removed blanket synthetic scanner boundaries. State checks use protocol FHs,
  preventing attrdir DELEGRETURN/FILE LAYOUTRETURN from retiring base inode state.
  FILE LAYOUTRETURN rejects synthetic identities before COORDINATE needs a real
  backend cursor. Extended explicit/runtime/saved root and attrdir coverage.
- Removed obsolete deferred namespace dispatcher handoff, namespace_boundary,
  suffix skipping and split WRITE payload ownership; supported runtime junction
  selections already had prebuilt branches. Kept export snapshot fences/caps.
- Private DELEGRETURN now permits REMOVE, both RENAME endpoints and saved-source
  LINK in one span. Scoped VFS queries/recall honor pinned excluded claims; peers
  still block. Internal LINK/RENAME entrances carry copied actor + borrowed view
  through async DAC/request gates; SMB interposing probes follow new signatures.
  Cross-review caught missing LINK result-prepare. Runtime tests caught RENAME's
  second lower-VFS recall, fixed by stamping the view in namespace_ready without
  overwriting that readiness callback with generic operation_prepare.
- Multi-entry layout journal supports FILE/ALL return/regrant with fresh state
  identities, cancellation, and retained own-barrier counts. Returned/canceled
  layout -> truncate and truncate -> grant coalesce without exempting peer holds.
  UNCHECKED OPEN truncation now enters the same layout coordination as SETATTR;
  public peer recall waits, private return avoids self-recall, active private
  grant -> truncate remains terminal DELAY with bytes preserved.
- Actual READDIR/READ_PLUS/GETXATTR/LISTXATTRS replies stage before successors;
  synthetic-root pages use runtime compact marshalling and retry reset. Retained
  exact static scratch/fixed reply estimates and separately reserved transport
  array/view space. The first removal of the old floor exposed an actual TCP
  send failure after WRITE (TEST_STATEID transport-headroom regression); corrected
  with generated adapter's real 260-iovec reserve. Tight fitting xattr/layout
  response tests remain successful. RDMA shared staging additionally reserves
  cursor partitions (807 descriptors total on this build).
- Fixed test-only finish control race: a visible empty release file must remain
  pending rather than being interpreted as retry; older touch-only producers now
  write an explicit reject action. New layout test callback uses pynfs-required
  op_cb_layoutrecall name; unsupported layouttype test uses a valid enum.

Final focused suites: 101/101 Debug and 101/101 Release, logs
`focused-debug-verified.log`, `focused-release-verified.log`. Both final builds
and make syntax pass. Full required make -k check completed with exit 2: both quick suites are
270/275 with exactly the five baseline failed suites and normalized mismatch
signatures. Both Clang stages completed with the same 42 warning signatures /
116 occurrences, no additions or reductions. Syntax, SDK include boundaries,
REUSE and copyright validation pass. `compare-final.txt` records this comparison. Earlier failed runs are kept
and must not be substituted for final verified logs.

Residual inventory (keep bounded; do not call all compound correctness done):
- Intentional active unseen private layout grant -> truncate DELAY; contention
  and changed snapshots also reject without an independent filesystem builder.
- 128 wire maps / 1024 helpers, entry admission, fixed/construction/transport
  capacities still split. Twelve client/session admin opcodes stay outside the
  shared filesystem encoder; only explicit second submit site is accepted DS
  REMOVE maintenance. Backend begin/end/rollback remains separate work.
- N4-09 transport source findings: standalone TEST_STATEID/dispatcher RDMA
  headroom does not use new shared reserve; generated reply caps aggregate260
  iovecs but each READ permits256 without a wire-wide count; multiple RDMA READ
  zcopaque fields overwrite selected write-chunk vector. Need targeted tests
  and unified admission/compaction/transport work. No hardware reproduction
  claimed; quick-tier inproc RDMA coverage does not prove these edge cases.
- Existing FSID layout return no-op, ALL type filtering and range/iomode return
  gaps are protocol feature work. Native CLONE source admission still requires
  supported normal state; no general anonymous source API added.
- Finish-rejection fixtures only repeat read-only executed filesystem work;
  they do not prove backend mutation rollback or distributed transactions.

N4-09 classification follow-up: all three residual transport concerns predate
this round, verified against the before snapshot. Expanded mixed-operation
staging can increase their exposure. Inproc RDMA quick tests exercise write
chunks and the new conn->rdma reservation branch, but not worst-case fragmented
responses or reply chunks. No physical RDMA hardware claim.

## 2026-10-03 NFSv4 fleet round 3 — transport admission and multi-READ

User authorized another planned parallel round. Three source-only workers owned
arena admission, aggregate READ vectors, and libevpl/xdrzcc RDMA placement;
parent integrated, formatted, built and tested. No commits/pushes. Evidence and
snapshots: `/tmp/chimera-nfs4-fleet3-20261003/`; current inventory remains
`docs/reviews/nfs4-compound-remaining-2026-10-03.md`.

N4-09 implemented:
- New nfs4_reply.h centralizes256 READ /260 generated-reply vectors and TCP6240 /
  RDMA19368 arena bytes (64-bit platform). Shared, single, dispatcher fallback,
  initial resarray and standalone TEST_STATEID preserve the same reserve.
- READ completion admits actual niov+framing before successors; counters and
  first-READ selection live in attempt context, reset from accepted request on
  retry, publish only on accepted finish and persist across independent spans.
  An oversized first nonzero-capacity RDMA Write chunk rejects before suffix.
- libevpl nested xdrzcc consumes only first eligible zcopaque for Write chunk,
  including empty first READ. Later READs inline; decoder claims chunk once,
  validates/clips actual length, permits positioned Read roundup. RPC2 honors
  returned Write lengths with Reply chunks, rejects short/unsupported offers,
  and releases internally owned buffers on the new decoder rejection paths.
- Fully decoded COMPOUND requests lacking reply scratch return tiny RPC
  SYSTEM_ERR before NFS entry. Fully decoded WRITE references release on this
  path and trailing-garbage GARBAGE_ARGS. Full-capacity generated decode is
  retained; no partial uninitialized argarray is walked. Existing partial
  decode cleanup limitations remain separate.

Integration corrections: original fragmentation fixture edited completed op
storage and hit the VFS purity fingerprint. It now interposes internal READ
completion before the compound stores its results; no product contract change.
Read-only cross-review caught an introduced malformed-chunk destination leak;
fixed before final tests. Initial failed logs are retained and superseded.

Final focused114/114 pass Debug and Release;55/55 RPC transport suites pass in
both (the corrected-*.log files also include six vector tests, total61).
Focused selection adds actual TEST_STATEID arena test, six fragmented/retried
wire cases, three raw decoded-arena cases, and existing replay/persistence
checks. No physical RDMA test; inproc tests perform registered-memory placement.
Required make -k check completed exit2: both quick runs270/275 with exactly
five baseline failed suites and identical normalized mismatch signatures.
Both full Clang stages completed; a new rpc2 write_list initialization warning
was corrected by guarding the loop with write_chunk_present as well as segment
count. Final targeted analysis of rpc2.c reports no findings in both modes.
Replacing its pre-correction diagnostics yields unchanged42 signatures/116
occurrences, no additions/reductions. Final focused114, transport55 and NFS4
RDMA model1 rerun passes in both after guard correction. Evidence:
compare-final.txt, analysis-debug-final.log, analysis-release-final.log,
focused-*-verified.log, transport-*-verified.log, nfs-rdma-*-verified.log.
make syntax, final syntax/SDK checks, REUSE, copyright and diffcheck pass.
Full check is still not green because of the five baseline failures and
existing analyzer findings; do not relabel them as expected passes.

N4-10 remaining correctness work, not new independent filesystem builders:
- ca_maxresponsesize is checked only after VFS acceptance. Independent ctest
  diagnostic reproduced READ+WRITE returning REP_TOO_BIG at WRITE after data
  changed; log late-response-limit.log. This diagnostic expects the bug and
  must not be counted as a passing product regression.
- ca_maxresponsesize_cached checks SEQUENCE alone, then capture may silently
  discard oversized reply. RFC8881 2.10.6.4 requires cachethis TRUE be honored
  after successful SEQUENCE, including early size-error replies. Corrected
  misleading existing nfs4_session.c comment; functionality remains to fix.
- RDMA replay capture sees reduced inline message without first READ payload;
  need full logical capture and replay placement into current offered chunks.
  Simply disabling capture would violate requested caching.
- Undersized RDMA Reply chunks still fail at send after operations execute.
  Group with negotiated/cached wire-byte admission, not arena memory bounds.
- All-zero-length nonempty Write offers remain conflated with empty/no usable
  offer. General malformed-peer partial-decode ownership/Read-list complexity
  remain a separate transport audit.

No VFS production changes this round. Net production increase254 lines:
NFS7files +239/-81(net158), libevpl RPC +70/-27(net43), xdrzcc builtins
+77/-24(net53), excluding tests/CMake/docs. Nested submodule worktrees contain
source changes; no submodule commits were made.

## 2026-10-03 NFSv4 wire-response admission follow-up

User requested the next gap after fleet3. Implemented reply-size admission;
no new agents, commits or pushes this turn. Snapshot/log root:
`/tmp/chimera-nfs4-wire-budget-20261003/`. Inventory remains
`docs/reviews/nfs4-compound-remaining-2026-10-03.md` (N4-10).

- Removed the final success-to-REP_TOO_BIG rewrite, which could report failure
  after WRITE had already changed the file. The dispatcher accounts accepted
  wire results across spans; VFS attempts reset private counters on retry.
  Mutation success bounds are admitted before their first helper. Variable
  read-only results, including READ/READ_PLUS/GETATTR/READDIR/GETFH/xattrs,
  charge actual encoded bytes before successors. Leave a following error
  result, including SETATTR's mandatory empty bitmap.
- Checks include tag, fixed XDR fields, external READ bytes and RPC/security
  overhead. Both ca_maxresponsesize and requested cache limits apply. The
  original SEQUENCE session stays authoritative across CREATE_SESSION. Zero
  cached capacity rejects SEQUENCE without advancing its slot.
- Cache storage and session capacity are reserved before successful SEQUENCE.
  Allocation/quota failure restores the previous state word and cached answer.
  Capture transfers that buffer; optional failed shrink retains and charges its
  full capacity. A new replay-slot test covers capacity exhaustion, preservation
  of an old answer, retry, successful advance, and capacity accounting.
- libevpl exposes pre-dispatch RPC/security overhead and RDMA Reply capacity.
  The reduced Reply body excludes only the first eligible READ payload; the
  logical negotiated/cache budget includes it. Capacity sums are 64-bit.
- Exact-boundary tests exposed xdrzcc's fixed-opaque size bug (only padding was
  counted), plus missing odd fixed-opaque padding in encoding/decoding. Fixed
  both and bounded contiguous fixed-field decode. Unit coverage compares actual
  encoding lengths and checks aligned, odd, typedef, fragmented and truncated
  fields. This changes generated codecs beyond NFS; broad validation required.

Final focused194/194 pass in Debug and Release. Four new wire fixtures cover
v4.1/v4.2 accepted/finish-retried cases, exact fit, padding, WRITE/SETATTR
suppression, actual short READ/FH sizes, span transitions, cached-size error
replay with an accepted WRITE prefix, and zero cache limits. No backend mutation
rollback claim: finish-rejection fixtures only repeat read-only executed work.
Both full quick runs270/275: exactly the five known failures and identical
normalized mismatch signatures (three remote pNFS, two SMB suites).
Both full Clang stages completed: unchanged42 normalized warning signatures/
116 occurrences, no additions or reductions. Required make -k check completed
exit2. Initial syntax check caught unstable initializer alignment in the new
transport-budget test; changed to one field per line (formatting only), and
final syntax/SDK checks pass. REUSE, copyright and all three worktree diff checks
pass. The five baseline tests and existing analyzer findings still prevent a
green sweep. Final evidence: compare-final.txt, check.log, repository-final.log,
focused-debug-final.log and focused-release-final.log in the snapshot/log root.

Initial focused Linux failures used unsupported /tmp overlay handles; corrected
CHIMERA_TEST_ROOT=/worktrees/compounds/build/mbt-scratch reruns pass. Initial
wire test failed on its pynfs channel attribute spelling; exact-fit failures
then revealed the product XDR bug. Intermediate logs are retained separately.

Remaining N4-10: RDMA capture still omits selected Write-chunk READ data;
complete logical capture and replay into the retry's offered chunks remain.
All-zero nonempty Write offers and general partial-decode ownership/Read-list
complexity remain transport audits. No physical RDMA or malicious short-Reply
wire reproduction claimed; admission has unit plus transport/model regression
coverage. Mutation/GSS bounds are deliberately conservative. No VFS production
changes; net production +335 lines across NFS, RPC metadata and the generator
(excludes tests/docs/CMake; nested submodule edits remain uncommitted).


## 2026-10-04 NFSv4 complete RDMA replay follow-up

User requested the next gap. No agents/commits/pushes this turn. Snapshot and
logs: `/tmp/chimera-nfs4-rdma-replay-20261004/`. Scope is complete logical reply
capture plus first-READ placement using each retry's offered chunks.

Capture callback now receives the omitted Write chunk and a logical length
including its XDR padding. Shared NFS copy restores bytes/padding without a new
cache envelope. NFSv4 cached replay scans prefix results using the generated
codec (recycled scratch, no successful READ decode/ref acquisition), separates
the first READ into current Write targets, and sends through normal RPC/Reply/
GSS machinery. Empty first READ does not select a later one. Original cache
bytes and persistent-record format remain unchanged. Core checks Reply
capacity before issuing any Write/Reply RDMA IO. Cache-hit preparation failure
uses a new exported RPC SYSTEM_ERR helper; v3/v4.0 no longer fall through and
re-execute when replay preparation fails.

New live wire probes (minor0/1/2) use two READs plus WRITE and empty SETATTR
(the latter makes the v4.0 request DRC-eligible). Compare full logical replies;
change file bytes before retries and verify WRITE is not repeated. Vary odd/
empty first READ, original/retry chunk offers, distinct destinations, short
Write/Reply capacity, and stream/RDMA session transitions. Original destination
stays alive and is overwritten with a sentinel to detect stale remote targets.
Unit copy test covers fragmented omitted payload, padding, short output and
incomplete vectors. No physical RDMA test or backend rollback claim.

Initial fixtures failed: borrowed read-into data cannot be consumed directly
by re-marshalling (fixed with an owned clone); WRITE alone does not make v4.0
DRC-eligible (added SETATTR). Three wire probes then passed. Full build caught
missing EVPL_RPC2_API on new helper (fixed). Test array formatting is stable
with trailing commas. Final Debug and Release builds pass; broad focused
ctests pass231/231 in both (focused-*-final.log). Full quick suites273/278
in both; exactly the five baseline failed suites and identical normalized
mismatch signatures (tests-comparison.txt). The three new probes are also
in quick. Both full Clang stages completed: unchanged42 normalized warning
signatures/116 occurrences, no additions or reductions. Required make -k check
completed exit2, only the five baseline test failures and existing analyzer
findings. Syntax/SDK, REUSE, copyright and final root/libevpl/xdrzcc diff checks
pass. Evidence: compare-final.txt, check.log, focused-*-final.log and
build-*-verified.log. No new final failure/signature remains. Net production
+161 lines, excluding tests/docs/CMake; no codec or VFS production changes.

Remaining N4-10: all-zero nonempty Write offers and malformed-peer partial
decode ownership/inbound Read-list complexity. Existing finite admission and
backend transaction limits remain as recorded in the review. No new VFS
production changes in this pass.


## 2026-10-04 NFS transport ownership follow-up

User requested the next transport cases. No new agents, commit or push.
Evidence and starting snapshots: `/tmp/chimera-nfs4-transport-20261004/`.

Implemented: explicit RDMA Write segment count, preserving nonempty zero-byte
Write offers in XDR, NFS admission and replay; early bounded chunk metadata
before RPC arena views; checked nonzero Read position/size/address validation;
all program/procedure checks and allocations before Read submission; submission
reference and error draining to avoid premature request release/server abort.
Truncated/bad-version transport headers now reject safely and complete matching
client calls with decode error. Generated XDR wrappers and RPC adapters use
scoped clone ownership, including NFS post-decode reply admission; borrowed
RDMA chunks remain transport/caller owned. Truncated fragmented padding now
returns failure without reading past the vector array. See current review
`docs/reviews/nfs4-compound-remaining-2026-10-03.md` for the remaining inventory.

Regressions: raw absent/empty/all-zero Write offers; bad Read positions, totals,
address wrap, excessive counts (including near/exhausted decode arenas), valid
sixteen-segment reads, zero-length segments, invalid keys and mixed successful/
failed segments, unknown procedure before I/O, partial/bad-version reply headers,
late reply after failure; contiguous/vector decode refcount restoration after
truncation/padding/arena failure/trailing data; late bad result with owned and
borrowed chunks; NFSv4.0/v4.1/v4.2 partial WRITE argument decode failures.

Newly explicit remaining transport work: Position Zero / Long Call reception
and reconstruction of multiple positions; stricter returned Write/Reply segment
identity/count validation (Reply length sum still uint32_t); explicitly tracking
whether a successful result decoder claimed an offered nonempty chunk, instead
of inferring this solely from returned length. These are transport correctness/
feature work, not independent north-facing VFS operations. Physical RDMA/provider
cancellation coverage remains absent.

Final validation: complete Debug/Release builds pass; focused231/231 in each,
plus final targeted19/19 in each (RPC/RDMA, NFS decode/replay, NFS backend ownership).
Required make -k check completed exit2: both quick273/278, exactly the five baseline
failures and unchanged normalized mismatch signatures. Both complete Clang stages
match the baseline42 warning signatures/116 occurrences with no additions or
reductions; compare-final.txt records the comparison. Final fixture-only Clang
rebuilds report no bugs. Syntax (root/libevpl/xdrzcc/header), SDK, REUSE, copyright
and final diff checks pass. No new VFS production changes.

Initial failures retained in evidence: newly exposed/fixed padding overrun,
unknown NFS opcode incorrectly treated as undecodable (replaced by truncated
SETATTR), test linkage and fixture corrections. Final stable fixture formatting
uses host-order constants converted to wire order in a loop, and valid read keys
distinguish admission failures from provider errors. Disk exhaustion interrupted
an early Debug sweep and Release build; reclaimed disposable compiler cache and
old analyzer build trees, preserved diagnostics, then reran the affected checks.

## 2026-10-04 — NFSv4 conversion review restricted to PR regressions

User requested a fresh regression review, explicitly excluding unrelated
protocol/transport feature work. Reviewed the current dirty compound-boilerplate
worktree against PR base c971e5a5d1e423296640c163fdf5ca0adfe92ae7 (HEAD a9e0efe078).
Report: docs/reviews/nfs4-compound-regressions-2026-10-04.md; linked from the
remaining-work inventory. No production code changes in this review.

Two confirmed P2 fixes remain:

1. nfs4_reply.h:118-125 charges every OPEN except WANT_NO_DELEG for the largest
   optional delegation reply even with grants disabled. The pre-execution gate
   in nfs4_compound_vfs.c:7054 rejects a normal no-preference OPEN on a 256-byte
   session with REP_TOO_BIG although its measured successful reply is 160 bytes
   including SEQUENCE/tag/RPC framing. WANT_NO_DELEG succeeds with 168 bytes.
   Keep admission before mutation, but select a feasible per-OPEN response
   bound and retain any decision to decline through publication/retry. Existing
   Probe.open_op always supplies WANT_NO_DELEG and misses this default-flag case.
2. nfs4_compound_vfs.c:4174-4176 skips the grant/decline helper when a later CLOSE
   closes the private OPEN state. OPEN(WANT_NO_DELEG) alone returns NONE_EXT;
   the same OPEN followed by CLOSE(current) returns bare NONE. Preserve each
   OPEN's WANT-aware decline even when no grant is possible at final finish.
   Actual grants must still wait for accepted finish; keep replay consistent.
   RFC8881 section18.16.3 requires NONE_EXT for supported explicit WANT flags.
   Named-stream bare-decline behavior existed at base and is not this finding.

Both reproduced on v4.1/v4.2, Debug/Release, via eight observational CTests.
These diagnostics pass while displaying defective behavior; they are not
assertions that the bugs are fixed. Existing selected Debug tests passed103/103
with ^chimera/server/nfs/(compound_|open_owner|replay_slot|layout_barrier), using
CHIMERA_TEST_ROOT=/worktrees/compounds/build/mbt-scratch. Evidence and temporary
CTest registrations/scripts: /tmp/chimera-nfs4-regression-review-20261004/;
focused-debug.log, probe-matrix.log, probe.py, reply_size_probe.py. The initial
diagnostic had a fixture IndexError (pynfs strips SEQUENCE); corrected before
the recorded matrix. No full build/check or baseline binary run this turn.

Also traced private OPEN/LOCK state, claim views, retry reset, reservations,
retirement, namespace/credentials, successful-prefix gates, variable staging,
and accepted publication; no additional confirmed regression from this pass.
General RPC/RDMA descriptor validation, remote consumption bookkeeping and
Position Zero/Long Calls remain separate preexisting work, not PR blockers
without a demonstrated conversion dependency. Do not revive those as remaining
compound-conversion defects. Future backend rollback remains out of scope.

## 2026-10-04 — Fixed both NFSv4 OPEN review regressions

User authorized fixing the two findings above. Both are now fixed; the review
and remaining-work inventory link this resolution. No subagents/commit/push.

- Session OPEN pre-execution reply bounds cover the mandatory NONE/NONE_EXT
  response and applicable create attributes. Optional grants receive separate
  admission after accepted finish against the whole accepted response prefix,
  including its terminal error and lookahead error space. Successful grants
  retain their space across later grants/parked callbacks. v4.0 reserves the
  larger form because owner replay can already carry a delegation.
- A later CLOSE no longer skips the successful OPEN's grant/decline helper.
  The helper receives allow_grant=false for closed states or insufficient
  space, while still producing WANT-aware declines. NO_DELEG and CANCEL
  always report NOT_WANTED and CANCELLED respectively, including when grants
  are disabled. State/delegation publication remains after accepted finish.
- Extended wire-budget fixtures now cover disabled/enabled delegation grants,
  a warm callback path, v4.1/v4.2, finish retries, 256-byte reply/cache limits,
  cache replay, OPEN/CLOSE plus failed suffixes, two coalesced OPENs, and a
  real WRITE successor. New CLOSE assertion fails on the old binary.

Validation: final selected compound/state suites107/107 in Debug and Release,
including eight wire-budget variants. Required make -k check CTEST_PARALLEL=8
completed exit2. Debug quick273/278 has exactly the five known baseline failures.
Release quick272/278 adds leases_memfs_plain: trace
smb2Leases_stepLease_300_0x21_4, state45, unexpected lease break 1->0 epoch4.
That suite passed three consecutive reruns without changes; retain the original
failure as an observed transient, not as a green full sweep. All baseline
mismatch signatures remain unchanged. Both complete Clang stages match baseline
42 warning signatures/116 occurrences with no additions/reductions. Syntax,
SDK include checks, REUSE, copyright and diff checks pass.

Evidence: /tmp/chimera-nfs4-open-fixes-20261004/ includes before/ source snapshots,
per-file diffs, before-test.log, build logs, focused-debug.log,
focused-release.log, check.log, smb-lease-rerun.log, compare-final.txt. First
sandboxed build hit the configured REQUIRE_NETNS_TESTS capability check; reran
outside the sandbox successfully. No backend or unrelated SMB fixes in this pass.

## 2026-10-04 — Overall protocol and northside VFS API review

New current report: docs/reviews/compound-overall-review-2026-10-04.md. Reviewed
the dirty compound-boilerplate tree at a9e0efe078, with behavioral comparisons
to main c971e5a5. No production/test code changes. Do not revive superseded
NFSv4, multipart MOVE, S3 metadata-pagination, SMB capability/identity findings.

Stripped-comment production inventory finds exactly eight northside call sites
to seven functions declared in vfs_internal_procs.h, all SMB: five ordinary
filesystem sites (overwrite, disposition getattr/readdir, DOC open_fh and
matched remove), two recall_handle_lease sites (one already in COORDINATE), and
one pure dirent_match utility. No ordinary direct per-op call in NFS filesystem
handlers/MOUNT, S3, FUSE, SDK/POSIX or REST. NFSv4 has two allocation/submission
sites: shared encoder and accepted DS removal maintenance. NLM TEST is compound;
LOCK opens in a compound then directly acquires/publishes a live claim. NLM
pending/held/cancel/recovery lifecycle remains incomplete conversion.

API privacy is only nominal: 19 production frontend translation units include
vfs_internal_procs.h (10 S3, 1 FUSE, 8 SMB). Backend SDK include checker does not
enforce northside privacy. nm confirms low-level symbols still exported, and
the separately linked VFS root module imports getattr/open_fh; blindly hiding
all internal symbols would also break core linkage. Fifty frontend files include
vfs_release.h, transitively exposing open-cache and vfs_internal implementation.
Use opaque out-of-line retain/release, narrow control/query/helper headers and
an enforced northside allowlist. Existing release_handle wrapper declaration is
misplaced in internal_procs. Keep per-op implementation vocabulary inside VFS.

Priority findings: reproduced FUSE parked-lock shutdown abort again; unresolved
SMB LockSequence snapshot -> RANGE publish -> replay-cache publish serialization
concern (not runtime reproduced); SET_REPARSE support narrowed to eligible regular
placeholders on memfs (only in-tree backend advertising REMOVE_MATCH_FH), with
cached/durable/locked/stream/DOC/peer cases safely rejected. Unsafe identity-only
fallback is gone; safe rejection still narrows former supported behavior and
is not completion. FUSE has 27 raw submits versus 2 shared-retry submits and no
own retry. SMB has 41 raw versus 9 adapter submits; generic CLAIM/RECALL mark
attempts nonretryable. NFS raw submits have their own policy, not the same gap.

Remaining appropriate public facilities: admin/config/mount lifecycle; handle
and result lifetime; credentials/ACL/idmap/capability/root-FH helpers; independent
ACK/revoke/cancel/teardown; notify watches and invalidation ACK; narrow optional
cache-grant/accepted-publication hooks; pNFS control/query helpers. DOC setters,
raw acquisition/range mutation and cache internals are not blanket lifecycle
exemptions. NFS persistence makes 21 plain KV calls: either retain an explicit
protocol-state store interface or add typed default-KV operations. vfs_kv.h's
claim that keys cannot be compound ops contradicts existing typed *_KEY_AT ops.
Backend association/rollback/cache-notify publication/commit admission remain
deferred, but need a drain and infallible accepted-publication contract. FUSE/
POSIX typed locks remain dedicated one-lock compounds; projected mutation rejects
optimistic finish, and SEEK_END backend-only arbitration has cross-protocol limits.

Fresh existing Debug CTests: 133/134 passed. Failure:
chimera/fuse/sim/compound_locks, fatal fuse.c:267 "fuse thread destroyed with 1
active requests". Preserved current server diagnostic before reruns. Source:
FUSE cancels locks then worker destruction follows; VFS drain only counts backend
num_active_requests, not parked/finishing compounds. Need stop-admission/cancel/
drain while workers live; exact introduction relative to main remains unproven.
The prior full-check five persistent failures and analyzer findings are still
unresolved; no fresh full/Release/Windows/physical-RDMA run for this review.

Evidence: /tmp/chimera-overall-compound-review-20261004/ contains inventories,
tests.json, focused-debug.log, fuse_compound_locks.debug.log. git diff --check
passes. Detailed report records source locations, compatibility classification,
public/private API decisions and prioritized next work. No commit or push.

## 2026-10-05 — Review refinement: drain, SMB replay/API boundary, FUSE retry

Implemented the first two refinement priorities and ordinary FUSE retry in the
existing dirty compound-boilerplate worktree. No commit/push; backend transaction
implementation remains deferred. Evidence is in
/tmp/chimera-compound-refinement-20261005/.

- VFS drain now counts submitted compound attempts through parked execution,
  asynchronous finish, and terminal callback return, including inline retry and
  callback-side compound free. The counter drops using a saved thread pointer,
  never a potentially freed/recycled compound. A bounded drain wakeup is needed
  because evpl_continue can otherwise enter its kernel wait after the last timer
  callback. The new compound_retry regression reproduced that timer-only hang
  during development and now passes. FUSE compound_locks passed 50 consecutive
  runs; its previously reproduced shutdown abort is fixed by this accounting.
- SMB LockSequence is now reserved per open/bucket for the entire native batch,
  before DOC preflight or execution, across retry, until replay publication and
  journal teardown. Batch admission releases partial reservations before waiting
  (no opposite-order bucket deadlock). Other buckets and CLOSE remain runnable;
  waiters use existing interim/CANCEL handling. Private CREATE opens reserve
  their buckets before publication as well. A real authenticated two-channel
  resilient-open fixture holds finish, holds accepted completion before SMB
  publication, rejects/retries finish, and cancels an admission waiter. It also
  exercises unrelated buckets. With admission temporarily disabled, it
  deterministically reproduces the stale-replay failure. Restored test passed
  20 consecutive runs. This is now a reproduced-and-fixed concern, not merely
  the earlier source-review hypothesis.
- Converted the five direct SMB filesystem call sites: fallback overwrite is a
  compound; disposition validation combines GETATTR and bounded READDIR, skips
  enumeration after the readonly veto, and publishes only at accepted completion;
  last-close deletion combines parent resolution and matched REMOVE. Removed the
  obsolete callbacks and request fields, preserving actor and parent-lease skip.
- All 19 production frontend includes of vfs_internal_procs.h are gone. Recall
  coordination declarations moved to vfs_claim.h; pure directory matching to
  vfs_dirent.h; release_handle declaration to vfs_release.h. LINK_NO_NOTIFY lives
  with the other operation flags. The private header requires the VFS-only
  build definition; five explicit white-box fixtures receive it independently.
  check_vfs_northside.py also rejects private symbol references, private includes
  and frontend attempts to opt into the core API. It runs in make check and
  through CTest. Current audit: 71 private functions, zero production northside
  uses or includes. ELF exports are deliberately unchanged: the separate root
  VFS module still imports core operations.
- All ordinary FUSE submissions (including mount resolution) now use the shared
  finish-aware retry adapter. Existing streaming reset/publication keeps
  READDIRPLUS staging private. New real-wire regression covers LOOKUP, GETATTR,
  OPEN, READ, OPENDIR, READDIRPLUS, terminal retry exhaustion and rejected negative
  lookup. Rejection is injected only into read-only operations, not filesystem
  mutations lacking rollback. No direct FUSE compound submissions remain.

Validation so far: make syntax and git diff --check pass. Initial focused Debug
set passed 70/70. Final focused sets passed 24/24 in both Debug and Release,
including the new API checker. make check stopped on the five established quick
suite failures; make -k check completed every remaining required stage. Its
Release and Debug quick runs each passed 274/279, with exactly the three remote
pNFS and two POSIX-over-SMB failures. Normalized mismatch comparison against the
previous saved full-check log found no new signatures. Both Clang stages finished:
42 warning signatures / 116 occurrences, exactly matching the baseline, with no
new or increased warnings. Fresh scan-build report counts are 41 Debug / 45
Release; older runs generated fewer fresh reports because ccache replayed warning
text. Compare normalized compiler diagnostics, not just fresh HTML report counts.
REUSE lint and copyright-year checks pass. The full sweep remains red on those
existing tests/analyzer findings. Final comparison: compare-final.txt in the
evidence directory.

Still pending from the accepted refinement order: NLM acquisition/publication
and cancel/reap ownership integration; standalone SMB retry audit; broader SMB
cache/DOC/durable, namespace and SET_REPARSE coverage; out-of-line retain/release
and narrower lifecycle/control/query headers. vfs_release.h still transitively
exposes open-cache internals, so header/symbol enforcement above is not a claim
that the whole public control surface is already narrow. NLM still acquires a
live raw claim after its OPEN compound, and its interval-carving, pending ticket,
CANCEL and remote GRANTED paths must move together. Also audit pending OPEN
lifetime during client reaping: nfs_nlm_state.c currently only hands pending
entries back to their callback when file_state is already set (source concern,
not yet independently reproduced). Do not implement backend transactions in
this frontend pass.

### 2026-10-05 SMB refinement (current pass)

Evidence and the before snapshot: `/tmp/chimera-smb-remaining-20261005/`.
Detailed scope and remaining boundaries:
`docs/reviews/smb-compound-refinement-2026-10-05.md`.

- All production SMB submissions now use the shared finish-aware retry adapter.
  WRITE no longer reports per-op success after rejected finish. EA enumeration,
  rename discovery/mutation and fallback CREATE now reject tentative results
  before publication or handle transfer. Fallback CREATE resets early-attempt
  gate state, but its live generic CLAIM remains an executor replay barrier:
  finish rejection after CLAIM fails and cleans up instead of replaying it.
- CREATE-time DOC uses native admission/publication and coalesces safe related
  metadata/I/O suffixes. The wire fixture verifies CREATE + WRITE + READ is one
  submission with three groups, and rejected DOC attempts never arm deletion.
  A delete-only DOC opener must complete after notification, not wait for a
  lease ACK. The new unacknowledging RH-holder case reproduced a 20-second hang;
  notification-only coordination now passes (doc-notify-before/after.log).
  Data/cache requests still require coherence. DOC CREATE + CLOSE remains an
  acceptance boundary.
- Fallback rename's destination directory probe now shares the mutation run;
  transient claim/state refs are dropped before coordinate completion. Child
  recalls use explicit coordination. Preserve BOTH existing probe exemptions:
  POSIX mode and moving a directory into itself (backend must return EINVAL).
  Accidentally applying the probe there caused new mismatches/leaks during
  development; restoring the bypass removed both. The native builder also
  lacked the POSIX exemption: fixing it makes batch_smb_memfs and strict_smb
  pass (posix-smb7.log), eliminating two previously established failures.
- Diskfs/cairn now implement REMOVE_MATCH_FH with complete identity checks in
  their existing per-operation transactions, enabling ordinary SET_REPARSE.
  All three backend primitive tests passed in Debug and Release. This is not
  backend compound transaction support. Passthrough/proxy conditional unlink
  remains unsupported; do not reintroduce lookup-then-unlink as a substitute.
- New standalone retry wire test covers data/metadata/EA/directory/rename/flush,
  bounded exhaustion, early CREATE retries and terminal live-CLAIM rejection.
  Its fake transactional WRITE returns op success without changing storage,
  then rejects finish. Other mutation injections stop before dispatch; none
  claims that memfs rolls back filesystem effects.

Still incomplete: durable/AppInstance/recovery and some cache admission paths;
stateful SET_REPARSE identity journals and coalescing; unbounded directory-rename
recall/scan and cross-share/occupied-target boundaries; some DOC/cache/durable
CLOSE boundaries. Zero raw submissions is not full lifecycle replayability.
A further source-level lifetime audit should examine borrowed fallback handles
across concurrent native CLOSE and delayed finish/retry: SMB open refs preserve
object storage, whereas native CLOSE can clear/release open->handle. This was
not independently reproduced or changed in this pass; do not label it a fixed
or confirmed regression.

Final validation so far: make syntax, git diff --check and the northside API
check pass. The final make -k check CTEST_PARALLEL=8 log is check-complete.log;
do not use the earlier interrupted check-final.log. Release quick: 277/280,
only the three baseline remote pNFS failures. Debug quick: 274/280; in addition
to those three, mbtaux/batch_linux and mbt4/batch_deleg_{linux,io_uring} aborted
creating host fixtures with ENOSPC. All three passed serial rerun unchanged
(storage-rerun-debug.log). Both modes passed all 56 server SMB suites, including
the final standalone retry/notification fixture; both formerly failing POSIX
SMB suites also pass. Identity-scoped REMOVE passed all three backends in each
mode (remove-release.log, remove-debug-final.log). Both final Clang stages
completed: 42 normalized warning signatures / 116 occurrences, exactly the
baseline, with no new or increased warnings. The comparison found no new model
mismatch signatures. REUSE lint, copyright-year checks and include/API guards
pass. make check remains red on the established pNFS/analyzer findings and the
three recorded fixture-space aborts (which passed rerun). Final comparison:
compare-final.txt. No source changes followed the final check sweep.

### 2026-10-05 NLM compound conversion

Evidence, before snapshots and a task-only diff:
`/tmp/chimera-nlm-compound-20261005/`.
Detailed design and scope: `docs/reviews/nlm-compound-conversion-2026-10-05.md`.

- LOCK/NM_LOCK now submit PUTFH + OPEN_CURRENT + GETHANDLE + typed LOCK_CHANGE
  as one compound. UNLOCK borrows an accepted handle or uses PUTFH + local
  LOCK_CHANGE without opening a stale/unlinked object. TEST, LOCK and UNLOCK
  use the shared bounded finish-retry adapter. Raw claim acquisition,
  cancellation, interval carving/publication, and the frontend completion
  doorbell have been removed from NLM.
- Each NLM client owns a VFS lock domain. VFS owns canonical accepted intervals
  and pending attempts; NLM retains pending request sentinels and at most one
  backend handle anchor per canonical owner/file. Wire handles remain available
  for CANCEL identity and GRANTED payloads. Admission generation and the typed
  attempt are allocated before asynchronous OPEN, under the recovery mutex.
- CANCEL protects compound lifetime with the NLM registry mutex and invokes
  typed cancellation; it cannot remove a grant whose acceptance already won.
  FREE_ALL, SM_NOTIFY, disconnect cleanup and shutdown invalidate admissions
  and retire coverage. Pending entries always belong to terminal completion,
  fixing the old pending-OPEN reaping lifetime gap. Reaped live synchronous
  requests that have not sent BLOCKED get DENIED; disconnected request encodings
  are never reused by the lock callback. Worker teardown drains callbacks before
  destroying RPC/VFS resources. Multiple connections can keep client locks alive.
- Final replies, handle transfer, NSM monitoring and GRANTED scheduling follow
  journal acceptance. The explicit wait coordination hook may send BLOCKED once
  per logical RPC. Only rejected finish EAGAIN triggers a whole-compound retry;
  a lock conflict is an ordinary operation result. Nonblocking NLM continues
  to refuse cache recalls, and length overflow retains its saturating behavior.
- Compatibility discoveries: use carry-bit exclusive endpoints for 2^64 so
  unlock-to-EOF removes the final byte; release client ranges in acquisition
  order and pump between removals to preserve waiter ordering; serialize local
  same-owner publication by arbiter admission order, not doorbell order. Keep
  the existing POSIX projected-lock/unlock lane exempt from that last ordering.
- NM_LOCK is non-monitored, not necessarily nonblocking. It can send BLOCKED.
  During development the new recovery reply path sent a second NM_LOCK reply,
  corrupting the RPC request pool and causing runaway async-result logs. Both
  synchronous lock forms now discard their encoding after BLOCKED and suppress
  any later response on that RPC. The new wire probe covers this case; the full
  Debug memfs corpus also passes after the fix. Interrupted preliminary/check
  and reproduction logs are not final verification evidence.
- The first complete sweep added a Release analyzer warning in client cleanup:
  DL_DELETE's singleton branch followed by another iteration allowed an
  impossible list shape in the analyzer. Rebuilding the pending list directly
  preserves order, makes ownership explicit, and removes that diagnostic. A
  targeted Release analysis is clean, and both final focused suites pass after
  this change. The old full sweep is check-list-warning.log; the final sweep is
  check-complete.log. Production code is 702 lines smaller than the before
  snapshot (regression coverage counted separately).

The new quick-tier NLM compound wire probe covers retry/exhaustion, rejected and
partial unlock, final-byte geometry, held finish, CANCEL/recovery before submit
and during finish, synchronous recovery replies, one BLOCKED across retry,
waiter/publication order, NM_LOCK reaping, unlink then unlock and blocked shutdown.
Only local journals and bookkeeping opens are rejected by the fixture; it does
not claim backend filesystem rollback. Final focused Debug and Release runs
each pass 26/26 NLM auxiliary/shared-lock/FUSE/POSIX and VFS compound/claim unit
tests. The final make -k check CTEST_PARALLEL=8 sweep sets
SPECS_CORPUS_PREBUILT=ON and
SPECS_CORPUS_ROOT=/worktrees/compounds/build/Debug/specs-corpus for all builds;
this reuses the same locally generated corpus already used by the Debug/Release
tests while avoiding redundant generation in Clang. All replay suites remain
registered; no test/model/config exclusions are added.

Final verification: make syntax and git diff --check pass. Both Debug and Release
quick suites pass 278/281; the only failures are the established remote pNFS
memfs/diskfs/cairn suites. No fixture-space aborts occurred in this final sweep.
Both Clang stages completed, with exactly the baseline 42 normalized warning
signatures / 116 occurrences and no new or increased diagnostics. Ccache replayed
the warnings in the final sweep (no fresh HTML reports); the cleanup function
also passed a separate uncached Release clang --analyze run after its revision.
Do not interpret scan-build's empty report directory as zero baseline warnings.
Syntax, SDK/include and northside API guards, REUSE lint, and copyright checks
pass. make check remains red on the three baseline pNFS suites. Final logs:
check-complete.log, focused-{debug,release}-final.log, nlm-cleanup-analysis.log;
comparison: compare-final.txt. No production or test changes followed this sweep.

Remaining scope: CANCEL, recovery, reference release and shutdown are appropriate
control APIs. SHARE/UNSHARE retain their preexisting no-enforcement response;
DOS share enforcement is separate feature work. NLM range arbitration was already
local (backend projection only supported POSIX owners); backend NLM projection
and compound transactions remain future work. Typed local locking compounds
exclude unrelated filesystem mutations because cancellation/recovery can veto
publication during finish without backend rollback. Broader public-header
narrowing remains a separate recommendation, as do the SMB boundaries above.

## SMB handle lifetime follow-up (2026-10-05)

User asked to continue the remaining SMB items. This pass addresses the previously
source-only concern about fallback borrowers racing native CLOSE. No agents were
launched and no commit/push was requested. Before snapshots and evidence are in
`/tmp/chimera-smb-lifetime-20261005/`; review:
`docs/reviews/smb-handle-lifetime-2026-10-05.md`.

Confirmed with a signed SMB3 bound-channel wire probe: pause EA LISTXATTRS finish,
CLOSE its FileId on the other channel, resume, and the next GETXATTR compound
returns INTERNAL_ERROR (0xc00000e5) because open->handle was cleared. The final
probe still reproduces this against an isolated library rebuilt from the pre-pass
SMB source snapshot (`repro-final-baseline.log`), so this is no longer only a
source audit finding. Fallback CLOSE had the same borrowed-handle lifetime gap.

CLOSE now retires FileId/claims/DOC immediately but defers the original VFS handle
release until all SMB-open references and claim owners drain. The explicit
handle_close_deferred flag skips duplicate logical DOC at final retirement;
open_file_free releases the retained descriptor. Fallback release_doc consumes an
independent cache/synthetic reference when borrowers remain. Retaining the
original descriptor preserves canonical actor identity and protects multi-stage
compounds and finish retry without per-handler pins. Existing SET_REPARSE's
exclusive refcount gate prevents identity replacement while borrowers remain.
QUERY_DIRECTORY's redundant handle pin and field were removed. Production code
is 28 lines smaller than the pre-pass snapshot; new regression probes are separate.

The quick-tier handle_lifetime_probe covers native/fallback CLOSE, delayed EA,
READ, named streams, EAGAIN retry, two-handle/two-chunk COPYCHUNK after both
FileIds close, accepted READ across DOC, and rejection of fresh I/O on a closed
FileId. Only read-only compounds are rejected; no backend rollback is invented.
The existing DOC probe had an independent injection race: its global arm could
catch a delayed disconnected request from another session. Its hook now matches
SessionId and MessageId; coalescing/group-count assertions are unchanged. Both
probes passed 30 iterations each in both Debug and Release (120 passes total).

Extended smbtorture compound/compound_async/compound_find: memfs passes 3/3;
Linux fails 3/3. An isolated pre-pass SMB library reproduces all seven failure
signatures identically (`extended-{debug,baseline}.log`). Detailed logging
(`linux-cleanup-debug.log`) confirms fallback DOC removal returns ENOTSUP (95):
it unconditionally requests matched-FH removal and Linux lacks that capability.
This is a newly confirmed remaining PR regression, not caused by this batch.
Deletion/cleanup INTERNAL_ERROR leaves names behind, causing subsequent
OBJECT_NAME_COLLISION failures. Prioritize this next, preserving identity safety;
do not claim the Linux suites pass or silently remove the matching safeguard.
Add quick Linux DOC coverage: the existing SMB model/probe matrix mainly exercises
memfs and therefore missed this backend-capability gap.

Other remaining SMB items: durable reconnect/recovery and AppInstance lifecycle,
remaining cache admission and CLOSE batch boundaries, stateful SET_REPARSE and
following-command identity overlay, broader directory rename boundaries, and
command-scoped cancellation. Fallback CREATE's generic live CLAIM is still a
retry barrier; backend transactions remain deferred. Public-header narrowing and
three established remote pNFS failures also remain from the overall assessment.

Final verification for the SMB lifetime pass: `make syntax` and `git diff --check`
pass. The final `make -k check CTEST_PARALLEL=8` reuses the prebuilt Debug corpus
for all configurations, without excluding any suites. Debug and Release each
pass 279/282 quick tests; all 57 SMB tests pass in each. Only the three established
remote pNFS suites fail, with exactly the previous mismatch signatures. Both
Clang stages complete with exactly the baseline 42 normalized warning signatures
/ 116 occurrences; no new or increased diagnostics. Syntax, northside/include
API guards, REUSE, and copyright checks pass. `make check` remains red on those
three baseline suites. Final evidence: `check-complete.log`, `compare-final.txt`,
`probes-repeat-{debug,release}.log`; `check-before-fixture-fix.log` includes the
intermittent DOC injection failure and is not the final result. No production or
test code changed after the final sweep. The three Linux extended suite failures
above remain independently reproduced pre-pass failures and are not part of the
quick-tier result.

## Publication checkpoint (2026-10-06)

User requested committing and pushing accumulated work to draft Chimera PR1692,
updating its remaining-work description, then rebasing onto latest main.
The PR initially still pointed to a9e0efe0; all subsequent refinement was local.
Published dependency branches named compound-boilerplate with human Git identity:
- xdrzcc a7f0fb8a94230633110d4c6f8f1656275853daf2, draft PR22.
- libevpl c9a9a376e5bc2f8dab08f7788f5b3dd371e1caf7, draft PR207,
  including the XDR pin.
- Existing specs8dfc579 remains published through draft PR33.

The root checkpoint collects the accumulated source, tests and review documents.
PR description now distinguishes remaining SMB lifecycle/identity/cancellation
work, Linux matched-FH DOC failures, public-header cleanup and validation debt
from explicitly deferred backend transactions. Latest pre-rebase verification is
the October5 lifetime sweep above:279/282 quick in each build, all57 SMB pass,
three remote pNFS failures, unchanged42 analyzer signatures/116 occurrences.
Publication only removed extra EOF blank lines in two newly staged files; no
behavior changed. Staged/root/nested diff checks and the northside API guard
were rechecked. Rebase and its
validation follow publication; this checkpoint is not post-rebase evidence.

## 2026-10-06 NLM/FUSE follow-up after main rebase

Latest task: finish the remaining NLM/FUSE compound work. Ordinary operations
were already compound-routed; remaining useful work was in the shared local
lock executor. No frontend protocol handler rewrite was necessary.

- Local-only typed locks can now accompany cursor/path resolution, non-mutating
  OPEN_CURRENT, metadata queries and replayable CHECKPOINT callouts. One typed
  lock journal per compound remains intentional. Multiple journals can deadlock
  same-owner admission or partially publish across cancellation; filesystem
  mutations require the deferred backend transaction/joint publication work.
- Scope is validated before initial dispatch AND after prepare, including
  dynamically added operations. Old exact-four-op NLM shape validation let a
  prepare callback change OPEN flags without a scope check; dynamic filesystem
  suffixes could also bypass submit-only validation. New regression covers both.
- Moved lock attempt allocation/execution/accept/reset/free into guarded
  src/vfs/vfs_lock_internal.h. scripts/check_vfs_northside.py enforces both private
  operation headers and symbols; lock domain controls/builders remain public.
- Confirmed a file-state retention bug: after an accepted lock's compound was
  freed, mandatory owner retirement removed ranges but retained b->file until
  domain destruction. Admission now creates only generation tombstones;
  attempts acquire file state, and retirement/free share idle reference cleanup.
  Pending attempts and projected cleanup retain their pins. Tombstones remain
  until domain destruction to cover admission-before-worker-enqueue races.
- New quick test src/vfs/tests/vfs_lock_compound_test.c covers both FUSE/NLM owner
  identities, retirement after accepted finish, retirement with held finish,
  stale queued generations after file-state reclamation, composed metadata retry,
  dynamic suffix replay, and forbidden static/prepared/dynamic mutations.
- NLM CANCEL/recovery/disconnect and FUSE interrupt/close/shutdown remain mandatory
  controls. FUSE FLUSH's owner retirement belongs after terminal compound COMMIT
  (including terminal error), never in a retryable pre-finish callback. Existing
  SHARE/UNSHARE non-enforcement and projected NLM locks remain separate work.
- Focused Debug and Release: each 28 passed, one skip out of 29 selected. The skipped FUSE io_uring
  mounted lock test reports fuse.enable_uring=0. Regular mounted FUSE locks,
  five-backend NLM replay and POSIX/native NFS3 locks pass.
- Both Debug and Release quick: 286/289; only the three established remote
  pNFS failures. Both Clang stages retain exactly the prior 40 warning signatures / 112
  occurrences, with none added/increased. make check remains red on those
  analyzer reports and pNFS failures (same fsstat zero-capacity signatures).
  Both final builds and the new lock regression pass after final header review.
- Required make check initially caught an uncrustify non-idempotent nested
  ternary initializer in the new test. Moved that assignment out of the
  initializer; make syntax and syntax-check now pass. Production code unchanged.
- Disk pressure: removed old generated Clang trees (make check recreates them)
  and 2,253 nfs3_mbt_pt_* test fixture directories older than this task, retaining
  current-run fixtures and logs. Approximately 1.3GB of stale fixtures reclaimed.
- Preserved the public opaque lock-attempt forward declaration for the compound
  op structure; its callable executor API is private. Final source builds pass
  in Debug/Release; final compound_locks ctest reruns pass in both. Formatting,
  SDK/northside API guards, REUSE and copyright checks all pass.
- Logs: /tmp/chimera-nlm-fuse-check.log, focused-{debug,release}.log,
  final-build-{debug,release}.log, final-unit-{debug,release}.log, syntax-final.log
  (all with chimera-nlm-fuse- prefix); warning-compare.txt records the exact
  unchanged analyzer signature/occurrence counts. Follow-up publication targets
  draft PR #1692 on compound-boilerplate.
  Review: docs/reviews/nlm-fuse-compound-followup-2026-10-06.md.
