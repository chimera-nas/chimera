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
