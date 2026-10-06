# SMB compound conversion: cooperative agent plan

## Wave21 focused completion (2026-09-26)

User requested completing the least difficult remaining area. Root selected
LOCK/UNLOCK and worked directly: eliminate every legacy lock-entry producer,
use the existing compound exact RANGE owner throughout, return a resource error
on compound allocation failure, and remove obsolete completion/teardown queues.
No import API is required because the legacy list has no persistence/recovery
producer. Related FileId inheritance and accepted-only LockSequence publication
remain explicit resource-failure contracts. Regression scope and proof are in
[the LOCK completion report](smb-compound-lock-completion.md).

Validated: full Debug+ASan build3; all 52 selected CTest entries including 470
SMB trace replays; Samba smb2.lock 23 passed and three platform-specific skips.
No worker delegation, model edits or commits in this focused pass.

## Wave20 continuation (2026-09-25)

User requested another implementation/refinement pass. Reused existing workers:

- CREATE lane (`smb18_create`): supported same-private-open cached CREATE suffixes,
  private actor contract, wire regressions, and next cache-lifecycle design.
- Namespace lane (`smb18_namespace`): admit unrelated root-child handles during
  bounded directory rename by gating all native/legacy RENAME and LINK mutations;
  preserve private path overlays and deterministic contention/cancellation tests.
- LOCK lane (`smb16_close`): canonical legacy RANGE/recall identities, forced and
  natural fallback regressions, and retained-entry migration assessment.

Root owns shared runtime/header hooks, namespace admission cancellation/drain,
CMake/export integration, independent review, all builds/tests and memory. Workers
do not build/test, commit, or edit model expectations. Wave19 build9 and 52 tests
are the baseline. Backend transactional rollback remains out of scope.

Wave20 completed: all three lanes handed off sources and reports; root integrated
and reviewed the combined changes. Full Debug+ASan build5 and all 52 selected
tests passed, including 470 SMB trace replays. No wave20 model changes or commits.
Delivered scope and residual contracts are in
[the validated inventory](smb-compound-current-status.md).

## Wave19 continuation (2026-09-25)

User requested continuation after the validated wave18 checkpoint. Reusing three
workers; root owns all shared runtime/header integration, builds/tests, and final
review. Workers do not build, test, commit, or change model expectations.

| Lane | Owner and files | Deliverable |
| --- | --- | --- |
| Notification CLOSE / LOGOFF | `smb16_close`: CLOSE, notify, LOGOFF, notify fixtures | Accepted native notification cleanup; secure surviving sibling-channel LOGOFF cleanup |
| Legacy parent lease identity | `smb18_create`: VFS rename/link/remove and notify APIs, separate fixtures | Preserve full protocol/client/ParentLeaseKey actor through legacy namespace operations |
| Broader namespace conversion | `smb18_namespace`: namespace registry, SMB rename, rename fixtures | Safely bounded native directory rename or another justified namespace slice, with precise descendant/path rules |

Root alone edits smb_internal.h, smb_session.h, smb.c, smb_compound.[ch], shared
VFS request/compound structures and CMake. Rename call-site changes required by
the actor lane go through the namespace owner. Backend rollback is out of scope;
finish-rejection tests remain read-only/resource-only. Every handoff lists real
newly enabled paths, regressions, and residual boundaries. Validation uses the
wave18 Debug+ASan build and relevant focused, extended, cross-frontend and SMB MBT
checks. No library rebuild overlaps a test run.

Integration assigned the namespace worker temporary ownership of the shared
runtime/header and CANCEL handler for a bounded single-command native CLOSE
cancellation registry. Smoke testing exposed the missing wire route; the corrected
fixture holds an actually running coordinator before retirement. Root retained
all build/test ownership and reviewed the handoff. Grouped cancellation remains
outside this slice.

## Wave18 wrap-up fleet (2026-09-25)

This dispatch supersedes the historical first-wave assignments below. Root owns
shared SMB runtime/headers, CMake registration, integration, all builds/tests,
and final assessment. Three workers run concurrently in the shared worktree;
no worker builds, tests, commits, resets, or changes model expectations.

| Lane | Current agent / exclusive ownership | First deliverable |
| --- | --- | --- |
| Namespace admission | `smb18_namespace`: namespace registry, rename, namespace fixtures | Exclude delayed backend opens from publishing a replaced target; enable a bounded distinct-target replacement if the lifetime contract is complete |
| CREATE lifecycle | `smb18_create`: CREATE, durable helpers, lease registry, CREATE fixtures | Client-wide concurrent lease-key reservation and actual lifetime binding; integrate namespace constructor hooks; then a supported specialized CREATE slice |
| Identity migration | reused `smb16_close`: reparse, new rebind helpers, reparse fixtures | Cross-request FileId identity admission and a restricted typed SET_REPARSE sequence with accepted handle/claim/namespace migration |

First-wave handoff: all three implementation lanes and independent cross-review
are complete. Root integrated fixes for admission/identity races, cancellation,
lock ordering, ParentLeaseKey actor identity, and durable-reconnect I/O ownership.
Validation and the explicit residual inventory are maintained in
[smb-compound-current-status.md](smb-compound-current-status.md). The follow-on
packages below remain queued; they are not reported as completed by this batch.

Workers propose interfaces before consuming them. CREATE has one editor; the
namespace worker sends its constructor hooks to that owner. Root mediates
`smb_internal.h`, `smb.c`, `smb_compound.[ch]`, `smb_procs.h` and build wiring.
Namespace helper edits needed by reparse go through the namespace owner.

Follow-on packages queued behind the first combined validation gate:

1. Complete notification/DOC/legacy-lock CLOSE retirement, including sibling
   channel LOGOFF cleanup. Retain accepted-only protocol publication and cleanup
   on each owning worker.
2. Extend private cache/durable/DOC state so suffix commands consume tentative
   grants and retirement; remove terminal CREATE/CLOSE batching only after that
   state is represented. Preserve deadlock-avoidance splits until replaced by a
   tested ordering contract.
3. Directory/subtree rename and cross-share path publication; typed stream rename;
   retirement of legacy `lock_entries` state. SMB RANGE claims are local under
   current VFS projection policy; projected SMB locking is not an active gap.
   These packages depend on admission and backend capability contracts.
4. Independent coverage/lifetime audit, followed by root combined validation.

Acceptance for every package: source implementation plus regression fixtures,
explicit converted cases and still-disabled cases, lock/lifetime reasoning,
no pre-finish public effects in replayable callbacks, and no unused speculative
interfaces. A safe prerequisite is reported as a prerequisite, never as completed
conversion. Backend rollback is out of scope; tests must not reject finish after
real filesystem/KV mutation. Root checks worker claims against dispatch gates.

Validation uses the existing Debug+ASan build: targeted new/affected probes,
focused VFS/SMB suite, extended claim/namespace regressions, five SMB MBT wire
profiles, and cross-frontend regressions after shared VFS changes. Tests run only
after source handoff and a combined build; no library rebuild while tests run.

2026-09-23. Implementation authorized and underway. The coordinator owns builds,
tests and integration; progress and limitations are recorded in MEMORY.md and
the companion SMB runtime/coverage reports.

## Objective and scope

Map eligible SMB wire compounds to one VFS compound, with replayable frontend
checks, attempt-private protocol state and accepted-only result publication.
Convert every appropriate filesystem path, including ordinary one-command
requests. Retain explicit, justified boundaries for protocol control, long-lived
subscriptions, resource limits and unsupported transaction participant sets.
One compound per existing handler is an intermediate migration, not completion.

Backend transactions remain a separate project. This conversion must prepare
the frontend contract without pretending immediate backend effects can roll back.
Backend transaction begin/end/abort, transactional cross-filesystem coordination
and deferred VFS cache/notification publication are tracked prerequisites for
real mutating finish-rejection support, not silently added to this assignment.

## Cooperation rules

- The primary agent is coordinator, integration owner and sole build/test owner.
  Workers implement sources and necessary regressions, report findings, and do
  not build/test or commit unless explicitly reassigned that responsibility.
- There are currently four concurrency slots: coordinator plus three workers.
  The roles below are work packages, not a promise to run all agents at once.
  Reuse idle agents or assign the next package as dependencies finish.
- Use the shared worktree and an explicit file-ownership ledger. Do not use git
  reset/checkout to clean up the existing extensive uncommitted changes. No
  simultaneous edits to a file, including headers and test harnesses.
- The VFS foundation owner controls vfs_compound.[ch]; the SMB runtime owner
  controls smb.c, smb_internal.h, smb_procs.h and new common SMB compound files.
  During later waves the coordinator can take ownership of these interfaces.
  Other workers request interface changes from the owner; they do not patch a
  shared header opportunistically.
- The coordinator owns CMake registration and integration changes. Each worker
  adds separate test source files when possible; shared harness changes go
  through their designated owner.
- Every handoff states changed files, API dependencies, converted paths,
  remaining fallbacks, pre-finish side effects and new tests. A worker's "done"
  is implementation completion, not acceptance or proof of full conversion.
- Contract changes are communicated to every dependent worker before consumers
  change. Do not maintain competing private copies of common helpers.

## Contracts to settle before broad implementation

The foundation wave writes a short internal API contract, reviewed by the
coordinator, covering:

1. An SMB command builds a VFS operation group. Group failures preserve real
   operation/command status and can skip the remaining group while continuing
   appropriate later commands. VFS default stop-on-error behavior for other
   frontends stays unchanged. Group boundaries are not automatic savepoints.
2. Each handler has construction, replayable execution checks, attempt reset and
   accepted publication responsibilities. They need not each become a separate
   callback when unnecessary. No handler calls the old completion chain from a
   new compound completion wrapper.
3. One attempt-private SMB state journal resolves explicit/related FileIds and
   stages provisional opens, cursor changes, closes, disposition changes and
   per-command results. Later commands see earlier tentative results; public
   registries and transport output do not.
4. Existing open references, credentials and payloads are pinned for all retries.
   Bind provisional result handles without taking ownership before acceptance.
   Group context/credentials are immutable inputs, selected explicitly.
5. Ordinary operation failure, ordinary EAGAIN, accepted partial results,
   finish rejection, exhausted retry, disconnect and cancellation have distinct
   outcomes. Only finish rejection certifying abort permits replay.
6. Shared claim state is reserved/validated before backend commit; publication
   is preallocated and cannot discover a new admission failure afterward.
   Attempt reset rolls back only provisional state and retains accepted prefix
   semantics on ordinary command errors.
7. Coordination, asynchronous waits and control messages have explicit lifecycle
   rules. Pure callbacks cannot emit breaks, issue unrelated I/O, or wait.
   Accepted data/results, including READ RDMA writes, are not exposed early.
8. Each remaining split has a named reason and observable test coverage. No
   blanket fallback merely because a common opcode appears in the wire compound.

## Work packages and ownership

| Package | Main owned files/area | Deliverable and dependencies |
| --- | --- | --- |
| VFS execution foundation | vfs_compound.[ch], focused VFS tests | Operation groups, error continuation, group contexts, result/lifetime and cancellation seams; preserve existing frontend behavior |
| SMB runtime and journal | smb.c, smb_internal.h, smb_procs.h, new smb_compound.[ch] | Wire-to-group construction, journal/reset, related handles, one terminal reply path, fallback accounting; depends on foundation contract |
| Independent conformance and coverage | New SMB compound fixture/tests, conversion inventory | Baseline cases, source inventory, retry injector and side-effect audit; no ownership of production handlers |
| Shared claims and locks | vfs_claim*, vfs_lock*, new journal helpers | Compose provisional share/range/grant changes; batched locking, exact owner retirement and cancellation; foundation owner integrates compound entry points |
| Basic I/O and enumeration | smb_proc_read.c, write.c, flush.c, query_directory.c | Builders using explicit/provisional handles, retained WRITE/RDMA inputs, accepted READ output and enumeration cursor; depends on runtime contract |
| Metadata and information | smb_proc_query_info.c, set_info.c, set_info_rename.c, security.c, smb_attr.h, smb_ea.h | Attributes, security/EA queries and mutations, rename and disposition journal integration; stream cases reserved for next owner |
| CREATE/CLOSE and durable state | smb_proc_create.c, close.c, smb_sharemode.[ch], smb_durable.c | Provisional opens, create dispositions, destructive-open admission, close/deletion and replay/durable publication; depends on runtime and shared claims |
| Streams, reparse and range IOCTLs | smb_doc_stream.[ch], smb_proc_reparse.c, sparse.c, copychunk.c, copyoffload.c, ioctl.c; later stream sections of query_info/create | Missing typed stream operations and complete dependent paths; coordinated handoff from metadata/create owners |
| Async protocol integration | smb_async_interim.[ch], smb_proc_lock.c, cancel.c, change_notify.c, oplock_break.c, smb_notify.[ch]; later teardown portions of logoff/tree_disconnect/smb.c | LOCK translation, pending/cancel/disconnect lifecycle, lease coordination, notify boundaries and cleanup; depends on runtime and claims |

The table describes final ownership, not concurrent ownership. For example,
CREATE and stream-open changes are sequential handoffs; runtime and disconnect
edits to smb.c are owner-mediated. Compound executor changes for streams/claims
are integrated by its one owner, not edited by multiple workers.

## Execution waves with three worker slots

### Wave 1: establish the common execution model

Run VFS foundation, SMB runtime, and conformance/coverage workers concurrently.
The conformance worker audits the current behavior and writes fixtures while the
two foundation owners agree interfaces. Preserve a runnable migration path.

Coordinator gate: build both foundations, run focused VFS regressions and other
frontend compound tests, and validate an SMB sequence on an existing open with
both success and command-error continuation. Verify one VFS submission, private
results and bounded finish retry. Review the state-journal ownership contract
before distributing broad handler work.

### Wave 2: independent operation families and shared claims

Run shared claims, basic I/O/enumeration, and metadata/information workers.
Freeze the initial runtime API; route unavoidable changes through its owner.
The coordinator integrates small completed batches and runs the appropriate
tests. Metadata can finish ordinary cases while explicitly tracking disposition
publication until the claims/close contract lands.

Gate: converted handlers coexist in a single compound; failed commands do not
expose stale cursors or poison unrelated commands. Claimed conversion is tested
with finish rejection and subsequent operations, not just standalone requests.

### Wave 3: stateful operations and asynchronous paths

Run CREATE/CLOSE/durable, streams/reparse/range IOCTLs, and async protocol workers.
The claims foundation is available before this wave. Start stream/range work in
disjoint files; schedule access to CREATE/QUERY_INFO only after their owners
hand them off. Runtime and compound core changes are coordinator-owned here.

First milestone: CREATE -> WRITE -> QUERY_INFO -> CLOSE in one compound,
including newly created handles, ordinary command failure and safe injected
finish rejection. Then expand to leases, durable/replay, delete-on-close,
streams, multi-element locks and cancellation. Do not implement all of CREATE
in parallel with unreviewed ownership/publication semantics.

Gate: state visible to later commands reflects the attempt-private journal;
unrelated requests cannot consume provisional public handles or results.
Recall acknowledgements and cancellation can still make progress independently
of the operation waiting for them. Teardown cannot run a second completion.

### Wave 4: independent closure audit and integration fixes

Assign workers to three review lanes: filesystem coverage and wire mapping;
retry/publication/lifetime; protocol equivalence and missing tests. Prefer an
auditor who did not implement the family being reviewed. The coordinator owns
final builds/tests and assigns fixes back to their file owners.

Every direct VFS or claim call is classified as converted, deliberate control/
lifetime work, backend internals, or a concrete remaining gap. Also audit hidden
mutations in old completion functions, shared helpers, release/delete cleanup,
durable teardown and request-position updates. Counting direct calls alone is
insufficient. Remove obsolete callbacks/fallbacks only after replacements pass.

## Validation and acceptance

Root-owned validation proceeds from narrow to broad; no competing builds or
test servers from workers. Use the existing ASan build and outside-sandbox test
execution when network namespaces/KVM/backend fixtures require it.

- Unit-level VFS groups: continue after a failed group, preserve true errors,
  skip dependencies, execution versus finish errors, retry reset, cancellation
  and retained handles. Run NFS/FUSE/SDK/S3 compound regressions after shared
  executor or claim changes.
- Real SMB wire compounds: related and unrelated commands; explicit versus
  inherited FileIds; earlier CREATE failure; later command failure after a
  successful create; multiple handles and context transitions; provisional
  create/use/close; partial results and reply budgets.
- State and effects: share modes, leases/oplocks, durable/replay, delete-pending,
  rename identity, streams, directory restart/cursors, security/EA, sparse and
  copy IOCTLs, lock batches, async CANCEL/disconnect and close-versus-grant.
- Transport: plain, signed, encrypted, compressed and RDMA profiles supported
  by existing tests. No response or RDMA result data from a rejected attempt.
- Finish injection: transient and exhausted rejection; zero/multiple retries;
  ordinary operation EAGAIN never replays; exactly one terminal response; no
  double grants, shared-state publication, input consumption or payload loss.
  Until real backend rollback exists, reject mutating attempts before effects;
  do not describe such tests as proof of transactional rollback.
- Existing SMB probes and MBT traces across relevant backends, then selected
  smbtorture/pike cases and broader cross-protocol tests where shared claims
  changed. Keep the coverage matrix explicit; no blanket conformance claim.

Completion requires: every eligible request routed through compounds; supported
wire sequences coalesced rather than separately wrapped; all residual boundaries
documented and justified; no ordinary filesystem dispatch hidden in accepted
cleanup; retry contract reviewed independently; relevant tests passing without
new model suppressions masking regressions. Existing known limitations are
listed separately from introduced failures. If a gap remains, report it rather
than declaring a package complete because its assigned files were edited.

## First dispatch when implementation is authorized

Launch only three workers: VFS foundation, SMB runtime/journal, and independent
conformance/coverage. The primary agent coordinates the contract, owns integration
and runs tests. Once the first vertical slice passes, replace the worker tasks
with Wave 2. This keeps useful parallelism without distributing incompatible
assumptions across every handler at once.

## Wave12 assignments (2026-09-24)

User requested four agents and one combined coordinator build/test pass. Runtime
limit is root plus three active workers; fourth starts when a slot opens.

- `smb12_create`: caching/durable CREATE. Own CREATE and durable sources, lifecycle
  fixture. Target opportunistic preallocated legacy oplock publication at accepted
  terminal CREATE, plus requests whose caching/durable grant is declined.
- `smb12_close`: shared/specialized CLOSE. Own CLOSE, VFS claim/grant facilities,
  cache-close fixtures and new claim tests. Atomic membership variants eliminate
  coalesce-before-member linkage window; CREATE owner adapts its callsites.
- `smb12_namespace`: namespace operations. Own rename/reparse sources, namespace
  registry/bridge, rename fixture, and specifically rename/path portions of
  smb_doc_compound.c. Target nonreplacing regular-base rename with stream holders.
- `smb12_doc_stream`: existing directory-base named streams and reply normalization.
  Started after a worker checkpoint; CREATE ownership transferred from its owner.
  Added directory-stream disposition/I/O/EA/reconnect wire coverage. Root coordinated
  the small runtime producer-I/O binding correction discovered in testing.

Root owns common SMB runtime/shared headers, CMake, integration, all builds/tests,
status report and memory. Workers may request explicitly scoped shared edits.
No commits, tests, builds or model suppressions by workers. Pure/private callbacks,
explicit coordination and accepted-only protocol publication remain mandatory.
No finish rejection after executed mutation until real backend rollback exists.

## Wave13 assignments (2026-09-24)

- `smb13_create`: shared lease/durable CREATE; first move legacy v2 epoch seeding
  into atomic grant creation. Own CREATE, grant core/APIs/tests, create fixtures.
- `smb13_close`: specialized CLOSE/DOC. Own CLOSE, stream-DOC and CLOSE-specific
  DOC compound sections, close/doc fixtures. Coordinate grant APIs with CREATE.
- `smb13_namespace`: next safe complex namespace slice. Own rename/reparse,
  namespace registry and rename-specific DOC compound sections and fixtures.

Root owns common runtime/header edits, CMake, integration, combined build/tests,
source assessment, status and memory. Agents make source changes and targeted
regressions without builds/tests; callbacks stay private/read-only until accepted
finish, with explicit repeatable coordination. Actual mutation finish rejection
remains forbidden until backend rollback is available.

## Wave14 assignments (2026-09-24)

- `smb14_create`: higher requested regular-file RqLs modes and CREATE fixtures;
  owns CREATE plus VFS grant core/tests. Keep durable/key-reservation boundaries
  explicit unless their full lifetime contract is implemented.
- `smb14_close`: nonpersisted durable CLOSE and close fixtures; owns CLOSE/DOC
  lifecycle sections. Prove atomic live/parked ownership retirement at acceptance.
- Reused `smb13_close` agent as wave14 namespace worker because the thread limit
  prevented another spawn. Own rename/reparse/namespace and rename DOC sections,
  VFS rename/request/compound plumbing and memfs rename/tests. Target matched
  destination replacement without live participants and atomic no-op reporting.

Root owns shared SMB structs/runtime, CMake, review, combined build/tests and
status/memory. Workers do not build/test or commit. No model suppression and no
finish rejection after executed filesystem mutation without backend rollback.

## Wave15 assignments (2026-09-24)

Reused the three completed wave14 agents for the user's next parallel round.

- `smb14_create`: hintless regular RqLs and truncating lease CREATE using runtime
  type checks and existing typed overwrite/recall machinery; CREATE fixtures.
- `smb14_close`: nonpersistent durable directory and named-stream CLOSE, including
  base ACCESS/handle identity, grant lifetimes, parking and reconnect fixtures.
- `smb13_close` (namespace worker): next bounded namespace slice and a dedicated
  cancellation regression for the optional rename tail. Converted replacing
  hardlink requests with absent destinations and bound LINK roots at execution
  from the private producer's namespace view.

Root owns shared runtime/headers, CMake, integration review, all builds/tests,
status report and memory. Workers make source/fixture changes without builds,
tests, commits or model suppressions. No finish rejection after actual backend
mutation; pure callbacks and accepted publication remain mandatory.
