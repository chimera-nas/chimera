<!--
SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
SPDX-License-Identifier: Unlicense
-->

Review of draft PR #1692 at `a9e0efe078caeb7b2975e10b4cce7d4e44a2ebd8`,
against main `c971e5a5d1e423296640c163fdf5ca0adfe92ae7`, September 26, 2026.

The ordinary frontend filesystem conversion is extensive, but this is not yet
a complete conversion or a merge-ready PR. Three separate properties matter:
routing filesystem work through the executor, retaining a useful wire-request
boundary, and safely reconstructing an attempt after rejected finish. Coverage
of the first is much broader than coverage of the other two.

This pass reviewed frontend dispatch, eligibility/fallback decisions, retry
entry points, publication ordering, the relevant main changes, and existing
validation results. It also reran four focused failing CTests against the built
PR code. It is not a claim to have re-reviewed every changed line in the roughly
113,000 inserted frontend/VFS/client/POSIX source and test lines.

**Priority findings**

1. **P1: typed removal semantics are lost through the NFSv4 proxy. Confirmed
   runtime failure.** `nfs3_proc_rmdir.c:93` supplies REMOVE_ISDIR, and ordinary
   REMOVE supplies its corresponding non-directory restriction, but
   `src/vfs/nfs/nfs4_remove_at.c:134` emits an unrestricted NFSv4 REMOVE. The
   current remote NFSv3 model run reports RMDIR of a symlink succeeding instead
   of NOTDIR, and REMOVE of a directory succeeding instead of ISDIR. These are
   destructive successful operations, not just incorrect error translations.
   Main's proxy also emits the unrestricted REMOVE, so this is a retained
   backend correctness gap, not an established regression introduced by this PR.
   Enforce the requested type in the removal path; ensure the check addresses
   the same name/object that is removed, rather than adding an unprotected
   frontend lookup. The remote suite has additional permission/attribute/statfs
   mismatches that this single explanation does not account for.

2. **P1: stateful legacy SET_REPARSE still changes handle identity without
   migrating the associated state. Source-confirmed mismatch; this pass did not
   produce a new runtime reproducer.** The fallback callback at
   `src/server/smb/smb_proc_reparse.c:225` changes the open's handle and flags,
   but leaves its ACCESS/RANGE owners, share file state and namespace identity
   associated with the old inode. The new compound path migrates identity, but
   eligibility at line 446 excludes cached, durable, locked, stream and DOC
   opens, among others. Those are precisely the difficult cases still using the
   fallback. Following operations can therefore address the replacement while
   coordination still describes the removed object. Complete the identity
   migration contract, or reject unsupported replacement before unlinking.
   Main also rebound the handle alone; this is an incompletely repaired legacy
   lifecycle, not a newly introduced fallback behavior.

3. **P1: SMB LockSequence publication lacks a shared serialization boundary
   with RANGE publication. Concurrency concern, not a reproduced race.**
   `smb_compound.c:455` copies replay state under the open bucket mutex;
   `smb_proc_lock.c:384` decides whether an attempt is a replay from that private
   snapshot. `vfs_compound.c:4045` publishes RANGE state before entering frontend
   completion. `smb_compound.c:616` publishes LockSequence later, after command
   publication. `vfs_claim_journal.c:284` allows ordinary concurrent bindings to
   the same owner: its pins preserve lifetime, not exclusive execution. A second
   channel can use an old replay snapshot while the first attempt is executing
   or after its ranges have published. A deterministic two-channel test must
   exercise both intervals. Fix with a logical per-open/replay-bucket admission
   token or validated version spanning snapshot through frontend publication;
   holding a mutex only during either copy does not close the interval.

4. **P2: SMB filesystem capability reporting regressed during integration.
   Confirmed runtime failure and identified missing assignment.** Main's
   QUERY_INFO path computes `r_fs_attrs.smb_fs_attributes` with
   `chimera_smb_fs_attributes(handle->vfs_module->capabilities, named_streams)`.
   The converted planner at `smb_proc_query_info.c:916` only sets output_length.
   Its reply at line 1275 still serializes the unpopulated field. The current
   info probe gets zero instead of `0x080400cf` with streams enabled, and zero
   instead of `0x080000cf` without streams. Clients lose advertised features.
   Restore the capability snapshot using the actual resolved handle; native
   QUERY_INFO uses a private open snapshot whose handle is not populated, so
   copying main's expression verbatim into that planner would be insufficient.

5. **P2: ordinary FUSE operations are routed through compounds but do not use
   the finish-retry policy. Source-confirmed gap.** There are 27 production raw
   `chimera_vfs_compound_submit` call sites in FUSE, two shared-adapter sites
   (locking and commit/flush), and no direct compound_retry call. OPEN, for
   example, submits at `fuse_proc_io.c:219` and replies immediately on aggregate
   error at line 92. READDIR similarly replies on failure at
   `fuse_proc_dir.c:311`. A finish EAGAIN is exposed to the kernel rather than
   retried internally. This does not itself publish rejected results, but it
   falls short of transparent optimistic retry. Convert these submit policies
   after auditing reset and input lifetime. The READDIR staging test manually
   invokes reset; it does not prove production completion resubmits a rejected
   attempt. Several standalone NFSv4 and legacy SMB compounds have the same
   raw-submit policy and need a corresponding audit.

6. **Merge gate: SMB claim/lease behavior still fails current probes.** The
   close-claim probe returns cache `0x08` instead of its expected exclusive
   grant, both standalone and coalesced. The lease identity probe observes a
   break on a same-ClientLeaseId reopen and RWH becoming RH. These were rerun in
   this review; their root causes are not established here. The final rebase's
   lease model run also reported grant/missing-break mismatches and timed out.
   Do not change model expectations merely to obtain a green conversion run.

**Frontend coverage and actual remaining conversion work**

| Frontend | Current position | Remaining work |
| --- | --- | --- |
| SDK and POSIX | Ordinary filesystem requests use compounds and the shared retry adapter. Lock operations use the VFS lock domain. | Projected backend locks and mandatory release are deliberately nonretryable; mount/configuration/reference lifecycle remains separate. |
| S3 | Filesystem requests, metadata, multipart and cleanup use compounds and the retry adapter. | Bounded multipart/copy/publication stages have separate acceptance boundaries; these do not imply one transaction for an entire upload. No remaining direct ordinary filesystem dispatch was found. |
| FUSE | Ordinary filesystem operations, FLUSH/FSYNC and locks use compounds. | Finish retry is missing on most ordinary operations. Invalidation/coherence and reference teardown remain explicit lifecycles. |
| REST | Debug filesystem operations use compounds with finish retry. | Mount/control/configuration operations remain lifecycle APIs. No ordinary filesystem-routing omission found. |
| NFSv3 | Ordinary RPC filesystem operations use compounds, retry and rejected-result suppression. | NLM claim acquisition/publication remains outside the compound. DRC/NSM/recovery persistence is ancillary. Fix the proxy behavior independently of routing coverage. |
| NFSv4 | Broad native sequences, including session OPEN, I/O, LOCK/LOCKU, CLOSE, namespace and attributes. | Sessionless state operations, synthetic/export namespace, delegation lifecycle, pNFS orchestration, named attributes, reply-space fallbacks and remaining raw handlers. |
| SMB | Broad native command groups with private opens, ACCESS/RANGE journals and accepted publication. | Cache/durable/DOC lifecycle, general namespace replacement and identity migration, grouped cancellation, backend capability coverage, and persistent recovery/repair. |

S3's object chunk limit is the configured I/O size (default 128 KiB), capped at
1 MiB in `s3_compound.h:15`. Small GET/PUT includes initial lookup/open and data
in the first compound; larger transfers retain handles and submit subsequent
chunks. This is the intended bounded-request policy. Multipart stages and
cleanup do not need to become an unbounded in-memory compound.

The direct-call inventory was built from names declared in
`vfs_internal_procs.h`, excluding comments and tests. Excluding reference drops
and synchronous helpers, ordinary filesystem calls remain in NFSv4 ACCESS,
GETATTR, OPEN, READDIR and SECINFO handlers, plus SMB reparse, legacy close,
set-info and overwrite paths. No ordinary raw filesystem dispatch was found in
SDK/POSIX, FUSE, S3, REST or NFSv3 request handlers. This inventory is routing
evidence only; it does not measure coalescing or retry readiness.

**NFSv4 boundaries worth removing**

- `nfs4_compound_vfs.c:5637` requires a live session for LOCK/LOCKU and
  CLOSE/OPEN_CONFIRM/OPEN_DOWNGRADE. The sessionless v4.0 paths remain separate.
  Even journalable v4.0 OPEN is terminal because open_can_continue requires a
  live session at line 5765. With v4.0 delegations enabled, OPEN bypasses the
  native builder entirely at line 5757. Delegation claim OPEN variants beyond
  CLAIM_NULL/FH/PREVIOUS remain legacy as well.
- The native allowlist at line 771 excludes OPENATTR, DELEGRETURN and layout
  operations. OPENATTR's filesystem portion is a standalone compound;
  DELEGRETURN immediately releases live claim/state. Named-attribute directories
  are synthetic and excluded from cursor seeding. These need a protocol cursor
  and state overlay, rather than assuming that every current object is a VFS FH.
- Cross-export PUTFH/RESTOREFH requires credential transitions. Possible export
  junction LOOKUP/SECINFO stops the run. LOOKUPP after an earlier cursor move
  stops at line 5256, even when the runtime result could prove it remains inside
  the same export. Execution-time namespace callbacks can eliminate useful
  subsets without moving export policy into a backend.
- pNFS REMOVE explicitly stops native coalescing at line 5588. The standalone
  path accepts MDS removal and then issues a best-effort DS cleanup compound
  (`nfs4_proc_remove.c:247`). LAYOUTGET uses a query compound and sometimes a
  separate backing-materialization compound (`nfs4_pnfs.c:963`); LAYOUTCOMMIT
  is also standalone. Layout publication/return is not a journaled wire suffix.
  Multiple backends need an explicit participant/compensation policy; one VFS
  compound cannot by itself promise a distributed atomic commit.
- Reply-space checks and numerous protocol-validation checks still fall back
  to per-op dispatch. CHECKPOINT callbacks and reply-buffer budgeting can remove
  some of these. They are less important than stateful correctness boundaries.

**SMB boundaries worth removing**

- Cached CREATE only admits a restricted private suffix: related QUERY_INFO,
  READ, FLUSH, and WRITE for RqLs. CLOSE, LOCK, another CREATE and namespace
  changes stop the run (`smb_proc_create.c:7798`). Need private grant/member
  changes and immutable CREATE reply snapshots, including mode/version/epoch
  when a later CLOSE leaves no surviving open.
- CREATE eligibility at line 7843 still excludes stream leases, mandatory
  oplocks, create-time DOC, many durable/reconnect/AppInstance contexts and cold
  recovery. These are accepted-prefix legacy lifecycle paths, not unsupported
  wire commands. Cached/durable CLOSE is generally terminal; complex stream/base
  DOC and other unsupported delete lifecycle cases still fall back.
- RENAME remains backend- and shape-dependent. The native path requires atomic
  no-replace and source-FH matching (`smb_proc_set_info_rename.c:998`); occupied
  replacement also requires destination matching and an explicit outcome.
  Memfs supplies the full set; Linux's no-replace flag alone is insufficient.
  General directories, stream-itself rename, occupied LINK and targets with
  live/pending holders retain boundaries. The bounded directory path is a
  root-child/same-parent slice, with a 128-entry scan budget. Cross-share path
  translation, moved ancestors and descendant publication still need work.
- Restricted SET_REPARSE is a standalone typed compound, not a coalesced wire
  command. General replacement needs a private identity overlay consumed by
  following operations, in addition to fixing the stale-state fallback above.
- General cancellation is not group-scoped. `smb_compound.c:74` registers the
  native batch cancel path only for an isolated CLOSE. Existing LOCK and
  long-lived notify cancellation have their own paths. Extending whole-batch
  cancellation blindly would cancel unrelated commands in the same wire request.
- Cold persistent recovery lacks the complete live file/base/stream and
  namespace/claim lifecycle. Failed record cleanup remains best effort and needs
  repair/tombstones. Some of this predates conversion, but wrapping persistence
  in a compound does not establish recovery correctness.

**VFS contract work before attaching real backend transactions**

The existing callback/reset/owned-result design is a useful foundation. Native
NFSv4 and SMB stage substantial protocol state, and the shared frontend retry
adapter distinguishes finish EAGAIN from ordinary operation EAGAIN. Current
backend begin/end/abort and per-request compound association are explicitly
deferred; their absence is not an accidental omission from the agreed scope.

However, the following constraints need an explicit implementation contract:

- Generic CLAIM and RECALL publish live state and set nonretryable
  (`vfs_compound.c:8274` and `:8116`); CLOSE_DOC does likewise at line 7484.
  Active legacy SMB CREATE/rename and NFSv4 remove/rename paths use these.
  Replace normal request ownership changes with journals, and use explicit
  external coordination for recalls. Merely naming them compound operations
  does not meet the finish-retry requirement.
- NLM's raw acquire at `nfs_nlm.c:986` publishes pending-to-held state at the
  claim callback. Its UNLOCK and reaper cancellation handoff depend on that
  instant (`:1187`). The right conversion is a shared pending/held/retiring
  lifecycle and journal, not a side-effecting operation callback.
- Projected POSIX backend lock mutations reject an optimistic finish adapter
  before execution (`vfs_lock.c:724`). Mandatory release is also nonretryable
  (`:889`). This is a deliberate safe limitation requiring backend lock
  participation, not an unconverted fcntl syscall wrapper.
- VFS per-op completions still update global caches and emit notifications.
  REMOVE does so before compound finish (`vfs_proc_remove_at.c:61` and `:96`),
  as do other mutation paths. NO_NOTIFY flags cover selected frontend paths,
  not a universal transaction publication journal. Backend rollback cannot
  retract an emitted event or repair already-visible tentative cache entries.
- Successful backend finish is not yet a guaranteed infallible publication
  point. `vfs_compound.c:4022` calls lock_attempt_accept after finish succeeds;
  `vfs_lock.c:750` can still return EINTR on cancellation/generation change.
  A future committing backend cannot simply be plugged into this ordering.
  Validate/freeze all fallible frontend/VFS admission before commit, then publish
  under the logical gates through frontend acceptance. This is a future
  integration hazard, not evidence of a current backend transaction corruption.
- Transaction association must reach inferred opens, permission gates, walks,
  copy fallbacks, pNFS redirects and resource cleanup, not just the top-level
  typed op. The module request ABI currently has no general compound transaction
  identity, and the finish seam is not a replacement for that propagation.

**Validation and recommended order**

This review reran `pnfs_memfs_remote`, `close_claim_probe`,
`lease_identity_probe_memfs` and `info_probe_memfs`. All four failed in 6.34
seconds. Log: `/tmp/chimera-pr-review-regressions.log`. Builds came from the
final rebase; the published tip only adds documentation over that source.

Prior final-rebase Debug/Release builds and 14 focused checks in each
configuration passed. The complete quick suites were run at intermediate
published tip `989821ec`: 214 passed, 29 skipped, 30 failed each, before the
last two main commits. Clang analysis findings remain untriaged, native Windows
validation is absent, and filesystem-fixture aborts/skips must be kept distinct
from behavioral failures. Earlier wave reports of all tests passing describe
older trees/corpora and do not override this checkpoint.

First fix the confirmed main-integration and protocol regressions. Then close
the LockSequence publication gap and unsafe identity fallback. Add finish-retry
coverage to ordinary FUSE and audit raw standalone submissions. Continue SMB
cache/durable/DOC and namespace state journals; pursue NFSv4 sessionless state,
synthetic namespace and layout/delegation work as separate bounded efforts.
Define backend prepare/commit/publication and participant rules before beginning
the backend transaction implementation. Add a frontend direct-call allowlist
check: the current SDK include check protects backend headers, not the asserted
exclusive compound entrance for frontends.

No production code was changed in this review.


**September 26 refinement following this review**

The working-tree follow-up restores filesystem capability reporting using the
resolved handle (including a private CREATE producer), restores atomic embedded
ACCESS/cache release on CLOSE, and separates SMB object-store caching
compatibility from client-qualified lease/RANGE identity. Directory notification
suppression retains the existing client-qualified ParentLeaseKey contract.

SET_REPARSE's handle-only fallback is deleted. The full identity migration
continues to serve eligible regular placeholders; excluded state and nonregular
sources return NOT_SUPPORTED before filesystem mutation. The migrated handle's
open flags are refreshed too. Tests cover original identity/content preservation,
retained range exclusion, cache state, DOC behavior, peers, streams and resilience.

The NFSv4 proxy now places LOOKUP plus VERIFY/NVERIFY(type) before REMOVE in the
same remote compound, maps a mismatch to NOTDIR/ISDIR and preserves pre-op parent
attributes on failure. The focused remote test covers regular files, directories,
dangling and directory-target symlinks, preservation after rejection and successful
valid removals. This fixes the reproduced sequential type failures, **not atomic
identity enforcement against concurrent remote rename**: NFSv4 has no typed or
matched REMOVE and the proxy does not advertise REMOVE_MATCH_FH. The broader
remote model still fails on independent CREATE/permission/attribute behavior.

The LockSequence publication race remains an unconfirmed concurrency concern;
these fixes do not add the required replay serialization. FUSE retry policy and
backend transactional begin/finish remain separate outstanding work.

**PR size audit at the reviewed commit**

Against main, the PR adds 123,589 and removes 28,129 lines: **95,460 net lines**.
The split (including comments/blank lines) is tests +41,564, VFS production
+19,241, SMB production +16,673, NFS production +10,993, other production -3,093,
docs +7,199 and other support files +2,883. MEMORY.md alone accounts for 2,604.
Production therefore grows by 43,814 lines; tests/support explain only about
half the total growth.

The executor implementation/header add 12,054 lines; the NFSv4 compound adapter
adds 7,067. Additional ACCESS/RANGE journals, lock domains and SMB namespace,
cache and lifetime coordination implement contracts beyond a dispatch rewrite.
Some are necessary for retry safety, but the old/new frontend paths and repetitive
command construction also accumulate maintenance cost. Finishing the conversion
must include deleting superseded handlers and consolidating shared lifecycle
logic, followed by an audit of executor/API surface. No evidence supports a
promise of a near-zero final production delta. Ordinary frontends outside SMB/NFS
already shrink, showing that conversion alone does not require growth everywhere.

Full GCC validation after refinement: Release and Debug each report **219
passed, 29 skipped, 27 failed out of 275**. The previous baseline had 30
failures; the three targeted SMB failures are repaired and there are no newly
failing test names. The remaining failures include the pre-existing lease-model
stall/grant mismatches, remote NFS semantics, auxiliary NFS tests, unsupported
SMB batch wire profiles and POSIX backend/scratch-filesystem cases. Formatting,
SDK include, REUSE and copyright checks pass. This refinement is net **-320
production lines** and **+135 test lines**. Full log:
`/tmp/chimera-pr-fixes-check.log`.


Clang Debug/Release compile successfully but static analysis reports **40 / 45
findings**, so `make check` exits with failure. Two additional warning signatures
relative to the saved baseline occur in the unchanged NFS delay/retry test and
its included slot code (possible null slot-table dereference / parked-list leak).
That extended test passes in GCC Debug and Release; the reports remain to be
triaged. No new warning signature points at modified production code. The
working-tree code is uncommitted, and the integration checkout used for builds
was checked byte-for-byte against every changed source/build file.

**Subsequent test repair, September 26 (uncommitted)**

The next pass clears 22 of the 27 previously failing test names. SMB wire batch
registration now supplies its trace families. SMB lease handling now retires
empty caches before waking CREATE, rechecks settled cache rights, recalls all
foreign handle caches on share denial, retains successful OPEN attributes across
claim-only retries, and retires abandoned wire compounds at disconnect. NLM
preserves grant/replacement order when releasing ranges and waking waiters.
The existing model traces pass without changing their expected protocol results.

Ten former POSIX failures were scratch/skip handling: clean setup unwinding now
preserves skip 77, and ext4 scratch allows the passthrough tests to execute.
Enabling previously skipped coverage exposed an NFSv4 READ-on-FIFO stall.
The executor now type-checks borrowed PATH handles before opening for data;
both delegation passthrough suites pass. The four ordinary/RDMA passthrough
suites now complete but expose unsupported descriptor xattr access on O_PATH.

Nine functional failing test names remain after rebuilt targeted reruns:

- Three NFS3-over-NFS4 remote suites: CREATE/type/permission semantics and
  incomplete proxy attributes, including missing statfs fields.
- Four NFSv4 linux/io_uring ordinary/RDMA suites: compound xattrs use PATH
  handles, while the backends call descriptor xattr functions that reject them.
  Reopening special files for data would reintroduce the FIFO/device hazard.
- Two SMB-backed POSIX suites: symlink traversal and related errno/namespace
  mismatches.

Required `make -k check` still exits 2. Its initial Release result is 263 pass /
12 fail, superseded for the final FIFO and claim-fixture changes by targeted
reruns; Debug is 265 pass / 10 fail, with its stale VFS unit expectation corrected
and passing after rebuild. Neither full invocation skips a whole test. Final
VFS compound, retry, claim, journal and access tests pass in Debug and Release.
Full log: `/tmp/chimera-tests-next-check.log`; final targeted logs:
`/tmp/chimera-tests-next-release-passthrough.log`,
`/tmp/chimera-tests-next-vfs-final2.log`, and
`/tmp/chimera-tests-next-debug-vfs-final.log`.

Formatting, compilation, REUSE and copyright checks pass. Clang still reports
41 unique path/message warning signatures, down from 42; none are new in this
pass and the old NLM fragment-list warning is gone. These warnings have not all
been triaged. The integration checkout matches all 25 modified source/build
files byte-for-byte. Broader compound architecture gaps identified above remain.

**NFSv4 API consolidation, September 27 (uncommitted)**

The acceptance criterion is compound-only VFS consumers, including operations
that cannot be coalesced with neighboring wire commands. Removal of redundant
execution paths is part of conversion completion.

ACCESS, ordinary GETATTR, READDIR and SECINFO now share the native NFSv4
compound builder even when its conservative multi-operation reply budget
declines coalescing. Their raw VFS callback chains have been deleted, including
the duplicate GETATTR delegation-query/combine implementation. Synthetic
named-attribute GETATTR and READDIR use owned compounds and marshal accepted
results. Named-attribute enumeration now resumes from the requested cookie and
emits cookies starting at 3, fixing the old reserved-cookie/pagination errors.
The existing 64 KiB stream-list limit remains. Standalone PUTFH validation and
OPENATTR also use the common finish-retry adapter.

This pass removes **516 net NFSv4 production lines** (186 added / 702 removed),
excluding tests and prior uncommitted changes. Ordinary per-operation VFS call
sites fall from **28 to 14**. All remaining calls and the only NFSv4 include of
`vfs_internal_procs.h` are in `nfs4_proc_open.c`: open_fh, open_at, lookup_at,
open_stream and fsetattr. Completing OPEN's delegated, named-attribute and
other fallback cases through the shared state journal is the next substantial
deletion opportunity. Direct LOCK claim acquisition, state/coordination and
cross-export/synthetic namespace boundaries remain separate concerns; this is
not a completed NFSv4 conversion.

Twelve focused suites pass in **both Debug and Release**, covering v4.0/4.1/4.2
boundaries and finish retry, delegation, namespace handling and the new metadata
fallback/pagination cases. Retry injection rejects only eligible read-only
metadata attempts; it does not establish backend rollback for mutations.

Fresh builds now use `/worktrees/compounds/build` directly, with freshly
generated model traces and ext4 scratch. The required full `make -k check`
exits 2: Release reports **266 passed / 9 failed**, matching the previous failing
names; Debug reports **265 passed / 10 failed**, adding the FUSE shutdown abort
below. Neither run skips a whole test. Both Clang builds compile, but scan-build
reports **42 Debug / 46 Release findings**, with 41 unique path/message warning
signatures across the log. The prior log is no longer available for a precise
signature comparison. Formatting, SDK include, REUSE and copyright checks pass.
Full log: `/tmp/chimera-nfs4-api-check.log`; Release focused log:
`/tmp/chimera-nfs4-api-release-focused.log`.

The additional `chimera/fuse/sim/compound_locks` failure reproduced on the 11th
repetition after passing in isolation. Its fatal log is **"fuse thread destroyed
with 1 active requests"** at `src/server/fuse/fuse.c:267`; NFS is disabled in
the fixture. Its final scenario shuts down with a parked lock after RELEASE.
FUSE stop schedules lock cancellation before stopping the worker pool, while
`chimera_vfs_thread_drain` waits only for backend requests, not suspended local
lock compounds. This is a likely teardown mechanism and a broader lifecycle
gap to resolve before asynchronous backend finish. No shutdown fix is claimed.
Preserved evidence: `/tmp/chimera-nfs4-api-fuse-repeat.log` and
`/tmp/chimera-nfs4-api-fuse-abort.debug.log`.

**NFS delegated OPEN follow-up, September 27 (uncommitted)**

Named and filehandle delegation claims now execute through the shared OPEN
compound builder, including single-operation fallback. Validation is a pure
execution-time snapshot of the delegation's client, file identity, version,
access and revocation state. Named claims open the resolved filehandle after
validation. Their legacy dispatch branches are removed. A missing journal
reservation returns to the compound fallback rather than publishing through
legacy OPEN callbacks.

The delegation-enabled NFSv4.0 OPEN boundary is removed. Accepted completion
publishes grants and updates the owner replay snapshot while reservations remain
held, including across callback probing. Same-wire repeated OPENs and fresh-XID
replays retain the final grant and permission principal. The new truncate test
also exposed and fixed missing caller identity on internal OPEN SETATTR, which
otherwise recalled the caller's own delegation.

Ordinary raw NFSv4 VFS calls decrease **14 to 11**, all in `nfs4_proc_open.c`.
Ordinary/cold-client OPEN fallback, named streams and legacy truncate still need
consolidation; direct LOCK claim paths and the previously recorded transaction
boundaries remain. NFSv3 already has no ordinary raw VFS calls; its 21 stale
internal API includes are now deleted. This pass's NFS production delta is
**+46 net lines** (+67 NFSv4, -21 NFSv3), excluding tests and previous changes.

Testing uncovered a separate VFS pNFS regression: unsupported backends could
redirect their first write without being able to persist its layout mapping.
Linux then returned local zero bytes instead of the acknowledged data. Both
resolver entry points now require `CHIMERA_VFS_CAP_LAYOUT`. The unsupported
fixture uses Linux because main now supports first-write residency on memfs.
All **26 focused NFS suites pass in both Debug and Release**. This includes
NFS3 probes, all existing compound feature/adoption suites, both delegated claim
forms and fallback paths, invalid stateids/identity, truncate safety, v4.0
delegation replay and finish rejection. Mutation rollback is still outside the
finish-retry injector's coverage. Focused logs:
`/tmp/chimera-nfs4-open-debug-focused.log` and
`/tmp/chimera-nfs4-open-release-focused.log`.

The full required check remains red: **266 passed / 9 failed in each build**,
with exactly the previously recorded nine failing names and no whole-test skips.
The known intermittent FUSE shutdown abort did not recur; it remains unfixed.
Both Clang builds compile and emit the same 41 baseline warning signatures,
without added occurrences. Only two analyzer reports per build were regenerated
because of ccache reuse; this does not establish a reduction in findings.
Formatting, SDK include, REUSE, copyright and final diff checks pass. Full log:
`/tmp/chimera-nfs4-open-check.log`. No commit or push performed.

**Remaining NFS OPEN paths converted, September 27 (uncommitted)**

The last **11 ordinary NFS per-operation VFS calls are removed**. Ordinary
standalone/cold-client OPEN, named-stream OPEN and deferred truncation now use
the shared compound builder and owner journal. The duplicate OPEN state
installer and callback chains are deleted: this pass removes **1,677 net lines
of NFS production code** relative to the preceding pass.

Stream OPEN carries a request-lifetime base-file guard through finish retries
and transfers it only on accepted publication. Same-owner reopens preserve that
guard, and CLOSE releases it. Stream creation does not alter base-file metadata;
size-zero truncation follows share admission. v4.0 owner replay uses the same
journal, including fresh-XID named-stream create/reopen replay. Permanent
reservation errors now survive fallback; expired clients receive EXPIRED rather
than being implicitly revived by the deleted path.

The old fallback hid a 128-existing-files-per-owner limit. Existing-state and
child-pin arrays now scale with the owner's population; journal storage grows
only before submission. Standalone OPEN preserves held children without
journaling unchanged lock owners. Coalesced lock graph/range limits, synthetic
attribute-directory boundaries and reply-budget splits still remain. Lifecycle,
release and direct claim/state APIs also remain; zero ordinary per-op calls is
not a claim of universal one-wire-compound/one-VFS-transaction execution.

Additional regressions cover standalone OPEN and truncate, principal failure,
held-lock reopen, 130 files under one owner, stream access union, denied truncate,
base-file preservation, guard lifetime, 130 child pins and v4.0 stream replay.
The retry fixture explicitly verifies named-stream OPEN finish rejection while
continuing to exclude filesystem mutations.

The final replay review also caught a v4.0 principal-denied OPEN escaping before
owner-seqid journaling. It now reserves the owner for error/replay handling and
fails at a checkpoint before filesystem work. ACCESS consumes its seqid only
after accepted finish, a retransmission replays the error, and the next valid
OPEN advances the file stateid only once. The new wire regression covers that
sequence under normal and rejected finishes.

Final validation: **29 focused suites pass in both Debug and Release**. After
the final owner-seqid fix, both builds also reran all 41 NFSv4 model suites:
**37 passed / 4 failed**, exactly the known Linux/io_uring PATH-fd/xattr failures.
The full check's earlier GCC sweeps report **266 passed / 9 failed per build**,
matching the nine baseline failures. Final-code Clang builds succeed, with the
same 41 baseline diagnostic signatures and no added occurrences. Initial new
array warnings were resolved; regenerated report counts (4 Debug / 5 Release)
remain subject to ccache reuse. Final formatting, include-boundary, licensing,
copyright and diff checks pass. KVM suites were not exercised (oras unavailable).

The first full sweep additionally exposed an intermittent SMB DOC probe abort:
`doc_compound_probe_memfs` failed its global submission/group-count assertions
after a disconnected-client case, with NFS disabled. Isolated Release repetition
passed eight times and failed on the ninth. The subsequent full sweep passed
that probe; the intermittent issue remains unfixed. This is separate from the
nine baseline failures. Details and retained log paths are in MEMORY.md.

**NFS attribute-directory coalescing, September 27 (uncommitted)**

OPENATTR, stream OPEN/LOOKUP/REMOVE, and synthetic PUTFH/GETFH/GETATTR/SAVEFH/
RESTOREFH now use the shared encoder. A base resolved by an earlier LOOKUP,
OPENATTR, a stream OPEN and following I/O can execute in one VFS compound.
The encoder tracks the protocol cursor independently of temporary backend
cursor moves and publishes it after accepted finish. Multiple stream bases
are protected by memoized coordination guards; retryable callbacks select
those guards without reacquiring or publishing them. The state journal retains
a guard even when a later CLAIM_FH supplies the final handle.

Standalone PUTFH now uses the same builder too. The separate PUTFH, OPENATTR,
attribute-directory GETATTR, stream LOOKUP and stream REMOVE completion paths
are deleted: **186 net production lines removed** in this pass. New regression
coverage also found and fixed synthetic GETATTR(FILEHANDLE) returning the base
handle; it now agrees with GETFH. OPENATTR refuses a synthetic handle that
would exceed the existing wrapped inner-handle limit before executing a suffix.

Stream READDIR still has a dispatcher boundary. Its existing LIST_STREAMS
compound is retryable, but its page/cookie/verifier and TOOSMALL decisions must
move into execution callbacks before mutations can follow in the same VFS
compound. Export/namespace transitions, conservative budgets, connected lock
journal limits and protocol lifecycle/pNFS cleanup remain separate boundaries.
Zero ordinary per-operation VFS calls does not imply universal 1:1 wire/VFS
transaction mapping. Backend transaction hooks and mutation rollback are still
outside this pass.

Validation: **29 focused suites pass in both Debug and Release**, including
v4.0 replay and single-/multiple-base finish rejection. The full required check
completes at **266 passed / the same 9 failed in each build**; both Clang builds
emit the same 41 baseline diagnostic signatures with no added occurrences.
Formatting, SDK include, REUSE, copyright and diff checks pass. No additional
whole-test failure appeared. KVM suites remain unregistered (oras unavailable).
The earlier intermittent FUSE and SMB DOC probe issues remain unfixed. Detailed
coverage, remaining boundaries and retained logs are recorded in MEMORY.md.

**Named-attribute READDIR coalescing, September 27 (uncommitted)**

The READDIR boundary described above is now removed. Named-directory READDIR
uses the shared encoder's GETATTR/LIST_STREAMS sequence and an execution gate
that stages its page and rejects errors before any successor runs. It can share
one compound with OPENATTR, stream OPEN/I/O/CLOSE/REMOVE and further READDIRs.
Accepted finish publishes the pages; attempt reset discards all prior staging.
The separate standalone READDIR implementation and an unused sizing helper are
deleted: **63 net production lines removed**.

Continuation verifiers now represent ordered stream identities, preventing a
changed namespace from silently reinterpreting positional cookies. Record,
identity, pagination and page-size checks run before mutations. Regressions
cover multiple pages mixed with mutation, FILEHANDLE/ACL snapshots, invalid
continuations stopping mutation, and actual completed LIST_STREAMS operations
encountering finish rejection in v4.0 and v4.2. The retry fixture still excludes
filesystem mutations and therefore does not claim to simulate rollback.

The existing 64 KiB complete backend stream snapshot limit remains. Supporting
paginated backend LIST_STREAMS requires further work; a non-EOF backend snapshot
now fails explicitly. Conservative reply/operation budgets, namespace/export
transitions, connected lock-journal bounds and protocol lifecycle/pNFS cleanup
remain separate boundaries. Backend transaction hooks are still future work.

Validation: **29 focused NFS suites pass in both Debug and Release**. The full
required sweep reports **266 passed / 9 failed per build**, matching the nine
baseline failures exactly. Both Clang builds compile with the same 41 baseline
diagnostic signatures and occurrence counts. Formatting, SDK include boundary,
licensing, copyright and diff checks pass. KVM remains unregistered because oras
is unavailable. Full details and log paths are recorded in MEMORY.md.

**Stream-list pagination, September 27 (uncommitted)**

The 64 KiB total-list restriction described above is removed for NFS named
attributes. LIST_STREAMS now has per-entry continuation cookies and an input/
output verifier; memfs produces bounded pages and validates the stream namespace
generation while holding the base inode lock. NFS resumes after the
last entry that fits its reply, preserves EOF across both backend and wire-page
limits, and retains the shared compound's execution checks and retry staging.
No whole-directory snapshot or separate READDIR compound is required.

Regression coverage enumerates 300 long names (>80 KiB packed), checks
continuation after data writes, rejects stale namespace positions before a
mutation, and explicitly injects finish EAGAIN on a non-EOF backend page. VFS
buffer alignment/continuation cases and SMB partial-list rejection are covered.
SMB still has its separate fixed stream-info staging limit; it explicitly rejects
partial results rather than silently reporting a successful prefix. Production
code grows by 29 net lines for this pass. Transaction hooks remain future work.

Validation: **34 focused suites pass in both Debug and Release**, including the
large named-directory tests, VFS compound/retry/group tests and SMB stream probes.
The full required sweep remains **266 passed / 9 baseline failures per build**.
After fixing and rechecking one unused-index warning in the new VFS test, both
Clang configurations compile and final diagnostics match all 41 baseline
signatures and their occurrence counts. Formatting, SDK boundary, licensing,
copyright and diff checks pass. Details and logs are in MEMORY.md.


**Shared NFS4 builder consolidation, September 27 (uncommitted)**

READLINK, VERIFY/NVERIFY and all four xattr operations now use the shared encoder
for single-operation fallbacks. Six duplicate builders and completion paths are
removed: **414 net production lines deleted**. Eighteen operation-handler files
still have separate builders. Namespace/export transitions, bounded state
journals, protocol state retirement and pNFS orchestration remain separate work.

Preserving the standalone xattr builders' ordinary data opens also fixes the
shared encoder's PATH-descriptor mismatch on Linux/io_uring. All four previously
failing NFS4 xattr suites now pass. READLINK retains NOFOLLOW and its type gate;
invalid arguments and unsupported synthetic cursors retain their error behavior.
Fifteen new measured requests exercise forced standalone execution and metadata
followed by xattrs. Retry tests explicitly reject finishes containing completed
READLINK and GET/LISTXATTR; mutation rollback remains outside this fixture.

All **29 focused NFS suites pass in both builds**. The full sweep is **270/275
passing in Release**, **269/275 in Debug**. Five carried-over remote-pNFS and
SMB/POSIX failures remain. Debug additionally exposes a reproducible VFS-only
resubmission leak: repeated submit overwrites the old operation snapshot and
retains an open-handle reference (116408 bytes across six allocations). This is
a second submit after accepted completion, distinct from the finish-retry path.
The untouched VFS code does not depend on the changed NFS server handlers; the issue
is recorded separately and remains unfixed. Detailed evidence is in MEMORY.md.

Both Clang builds complete with the exact same 41 diagnostic signatures and
occurrence counts as the baseline. Formatting, SDK boundaries, licensing,
copyright and diff checks pass. The full check remains nonzero for the listed
tests and pre-existing analyzer diagnostics. No commit or push was performed.


**VFS compound resubmission lifetime repair, September 27 (uncommitted)**

The resubmission leak recorded above is fixed. A second submission retains the
original construction snapshot and shares retry's attempt reset, including
owned-result cleanup, original argument restoration, callback re-execution,
group/cursor reset and dynamic-suffix disposal. Result teardown is shared with
compound_free instead of maintaining another copy of the ownership rules.

Repeated submission cannot bypass retry's publication barriers. Taken outputs,
published journals, nonretryable operations and accepted borrowed CLOSEs prevent
re-execution; refusal preserves their accepted ownership for teardown. Pending
cross-thread cancellation also blocks restart. This does not provide backend
rollback or make an ordinary operation EAGAIN safe to retry automatically.

The Debug LeakSanitizer regression now passes. All **35 focused VFS/NFS extended
suites pass in both builds**. The full quick sweeps are now **270/275 passing in
both Debug and Release**, with only the five previously recorded remote-pNFS and
SMB/POSIX failures. Both Clang source builds have the exact baseline set of 41
warning signatures and 110 occurrences. Production code grows by 33 net lines.
Final check completion and logs are recorded in MEMORY.md. No commit or push.


**NFS4 COMMIT/CREATE/LINK consolidation, September 27 (uncommitted)**

COMMIT, CREATE and LINK now use the shared encoder even when dispatched alone.
Their duplicate builders/completions are removed: **326 net production lines
removed**, leaving 15 operation-handler files with independent compound builders.
RENAME is the next consolidation candidate; broader namespace/export, state and
pNFS boundaries remain.

The shared path retains COMMIT's NOFOLLOW/type gate and uses its pre-operation
attributes, preserves cross-export LINK when exports share a filesystem, and
keeps synthetic saved handles opaque. CREATE now rejects malformed attribute
values before mutation and a missing result filehandle before executing its
suffix. Forty-eight measured wire requests cover both execution modes and
relevant errors; finish injection now explicitly exercises completed COMMIT.
Successful namespace mutations are not replayed by the synthetic retry fixture.

All **35 focused VFS/NFS extended suites pass in both Debug and Release**.
Both full quick sweeps pass **270/275**, with the same five recorded remote-pNFS
and SMB/POSIX failures. Both Clang builds and model-generation stages complete;
diagnostics exactly match the baseline's 41 signatures and 110 occurrences.
Formatting, SDK boundaries, licensing, copyright and diff checks pass. The full
check remains nonzero for the existing tests and analyzer findings. Details and
logs are in MEMORY.md; no commit or push was performed.


**NFS4 remaining handler builders, three batches, September 27 (uncommitted)**

All 15 remaining `nfs4_proc_*` builders now use the shared encoder for both
coalesced and single-operation execution. The batches cover namespace/LOCKT,
data and sparse I/O/SETATTR, then COPY/CLONE/REMOVE. This removes **3,607 net
production lines** and all 25 compound-allocation sites from those handlers.
The common entry is now named `chimera_nfs4_compound_single`.

The conversion preserves operation-specific validation, cursor identity,
stateid authorization, delegation coordination and result ownership. v4.0 I/O
lease renewal is staged until accepted finish. SETATTR decode failures become
execution-checkpoint errors, so they no longer abandon the shared builder.
COPY uses the VFS transfer fallback instead of duplicating it in the frontend.
A root-export LOOKUPP refusal left over from the scanner caused a regression;
the new test reproduced it before the fix and passes afterward.

REMOVE's MDS phase shares the encoder, including pNFS. Its best-effort backing
cleanup remains a separate maintenance compound after accepted MDS finish and
before the wire suffix resumes. The backing filename now uses the same
mount-id/file-id helper as creation. A new real resident-pNFS test proves that
nonfinal-link removal preserves data and last-link removal deletes the backing.

**36 focused VFS/NFS suites pass in both Debug and Release.** Coverage now forces
single-operation execution of the removed builders, includes v4.0 I/O, checks
wrong-file stateids and error suffixes, and tests namespace routing under a
real root export. No ordinary north-facing VFS calls were found across the
74 internal API names audited. Both final quick sweeps pass **270/275**, with
only the five previously recorded remote-pNFS and SMB/POSIX failures. Both
Clang builds and model generation complete with exactly the baseline 41
diagnostic signatures and 110 occurrences. Formatting, SDK boundaries,
licensing, copyright and diff checks pass. Full check remains nonzero for
the recorded test failures and existing analyzer findings; logs are in MEMORY.md.

This completes the remaining per-handler builder consolidation, not all NFSv4
coalescing. Three root/export-resolution builders and three pNFS builders
(LAYOUTGET query, backing materialization and LAYOUTCOMMIT) remain outside the
shared encoder. State/session retirement, export credential changes and bounded
reply/journal capacity still impose boundaries. Backend rollback is not added;
the synthetic finish-retry fixture continues to exclude successful mutations.


**NFS4 LAYOUTCOMMIT consolidation, September 29 (uncommitted)**

LAYOUTCOMMIT now joins the shared NFS4 encoder and uses the same implementation
for standalone fallback. Its duplicate builder and callbacks are removed,
reducing production code by **45 net lines**. Authorization rechecks the live
writable layout, session identity, execution filehandle and stateid version on
every attempt without renewing leases. Conditional metadata updates use that
attempt's size, never shrink, and preserve mtime and newsize reply semantics.

Regression coverage checks ordering across multiple LAYOUTCOMMITs, fallback,
identity/version failures, malformed inputs, stopped suffixes, synthetic
cursors and disabled pNFS. A deterministic finish-retry test returns a layout
while finish is pending, then verifies that retry rejects its stale stateid
before the suffix. The fault injector excludes applied metadata mutations;
backend rollback is still outside this work.

**36 focused VFS/NFS suites pass in both Debug and Release.** Both quick sweeps
pass **270/275**, with the same five recorded remote-pNFS and SMB/POSIX failures.
Both Clang builds and model generation complete with exactly the baseline's
41 diagnostic signatures and 110 occurrences. Formatting, SDK boundaries,
licensing, copyright and diff checks pass. The full check remains nonzero for
the existing tests and analyzer findings; detailed results are in MEMORY.md.

Remaining independent builders are three root/export-resolution paths and two
LAYOUTGET paths (query and backing materialization), plus accepted
pNFS REMOVE cleanup. LAYOUTGET needs staged protocol publication and retry-time
authorization alongside its conditional storage work. Root/export conversion
must preserve export snapshots and credentials. No ordinary per-operation VFS
calls remain in the NFS frontend. This pass does not remove state/session or
capacity boundaries, or provide atomic transactions across the MDS and DS.


**NFS4 root/export entry consolidation, September 29 (uncommitted)**

Export-entry LOOKUP and real-root PUTROOTFH/PUTPUBFH now use the shared encoder
for the entry and its same-export suffix. The independent export LOOKUP builder
and PUTROOTFH resolver callback are removed. Single-operation fallback uses the
same construction, callbacks and retry lifecycle. Production grows by 83 net
lines, including export snapshot validation and accepted-only root-cache updates.

The compound fixes its credential from an owned export snapshot and rechecks
name/id/path/access/squash/anonymous IDs/security policy before each operation
on every attempt. Changed exports return DELAY for fresh selection. Cache
publication rechecks the snapshot under exports_lock after accepted finish.
PUTROOTFH still resolves afresh, follows symlinks, supports the physical VFS root,
and returns SERVERFAULT for an unresolved configured root. Its WRONGSEC deferral
runs the root operation alone so a same-export PUTFH cannot evade flavor checks.

New wire tests cover suffix coalescing, forced fallback, read-only and squash
policies, security deferral, synthetic-root entry, missing paths and symlinks.
Deterministic pending-finish tests replace exports using the same ID with changed
paths or policies and verify that retry rejects the old snapshot before its
suffix. Accepted/retry NFSv4.0 namespace suites extend the existing NFSv4.2
coverage. Final review also removed unnecessary build-time filehandle
requirements from COPY/CLONE restoration and v4.0 OPEN replay; new exact-span
tests cover those suffixes. Both focused suites pass 38/38. A reproduced test
logging race was fixed without weakening assertions (20 consecutive reproducer
runs pass). Final quick sweeps pass 270/275 in both builds with the same five
recorded failures. Clang diagnostics match the baseline (41 signatures, 110
occurrences); final incremental Debug analysis adds no new findings. Formatting,
SDK boundaries, licensing and copyright checks pass; all model generation
completed. The full check remains nonzero only for the recorded tests and
analyzer findings. Final production corrections were rebuilt, retested in both
configurations and analyzed.

Remaining independent builders cover root-FH cold-cache resolution, pseudo-root
READDIR enumeration, LAYOUTGET query and backing materialization, plus accepted
pNFS REMOVE cleanup. Cross-export credential changes, state/session retirement
and capacity still impose boundaries. No VFS public API extension was needed.


**NFS4 pseudo-root READDIR consolidation, September 29 (uncommitted)**

A pseudo-root directory page now runs in one shared VFS compound. The old
per-export compound allocation, submission and callback loop are removed;
production code decreases by 30 lines. Construction snapshots the export list
and reserves a bounded page. Pure callbacks revalidate each export's identity
and path and stage its attributes. The shared finish handler retries the whole
page, and publication preserves the synthetic root cursor.

Four new v4.0/v4.2 accepted/retry suites check exact submission spans, signed
alias handles, symlinks, multi-component paths, page/arena limits, cookies,
empty pages, failed prefixes and stopped mutations. A controlled export
replacement during finish proves retry rejects an obsolete path even when its
export id is reused. Coverage also exposed and fixed missing attributes for an
export of the physical VFS root. The 444-export arena test required increasing
the Python decoder's recursion limit; server limits and assertions are retained.

A broad-attribute regression reproduced an ASan crash from the previous 256-byte
entry allocation: the marshaller limits ACL inclusion but can still write fixed
fields beyond that size. The builder now reserves a bound for all requested
fixed fields plus the existing ACL allowance. The four new suites also exercise
these larger entries across page boundaries. Full verification was restarted
after this correction.

Focused Debug and Release runs each pass 46/46, including POSIX root tests on
memfs and linux. Release's quick sweep passed 270/275 with the same five recorded
failures; Debug passed 269/275 with those five plus the previously recorded FUSE
shutdown abort. Repeated CTest runs reproduced the same fatal active-request
check at fuse.c:267, in a fixture with NFS disabled. This remains unfixed. The
full repository check completed with exit 2 for those test failures. Both Clang
builds completed; warnings exactly match the baseline (41 signatures, 110
occurrences), including cached diagnostics. Formatting, SDK boundaries,
licensing and copyright checks pass, and all model generation completed.
Remaining independent
builders are cold root resolution, LAYOUTGET query/materialization and accepted
pNFS REMOVE cleanup. Pseudo-root positional cookies and the ACL allowance retain
their existing limitations; this pass adds no backend transaction rollback.

The next cold-root pass should also replace its id-only cache publication check:
a root export removed and recreated with the same id and a different path can
pass that check while an old resolution is in flight. That independent resolver
does not yet use the shared finish-retry handler or full snapshot validation.

**Ordinary NFS4 READDIR attribute sizing, September 29 (uncommitted)**

The suspected ordinary READDIR overflow is confirmed: a broad attribute request
returned directory-name bytes as an attribute value. Ordinary and named-attribute
entries now use the shared sizing bound extracted from pseudo-root READDIR,
including requested stored or synthesized ACLs. Unrequested ACLs consume no
extra storage. Encoding reclaims unused reservation space and charges actual
XDR entry bytes against maxcount, preserving the arena reserve and retry rules.
Production code grows by 20 net lines; no VFS API change is needed.

Eight new memfs/Linux, v4.0/v4.2 accepted/retry suites cover broad attributes,
full ACLs, exact-fit and one-byte-short budgets, continuation, multiple pages
with interleaved results, and stopped mutation suffixes. Existing named-attribute
suites now request broad attributes too. Combined focused Debug and Release runs
each pass 54/54. Both quick sweeps pass 270/275 with the same five persistent
failures; the known intermittent FUSE/SMB DOC aborts did not recur. The full
repository check completed with exit 2 for those failures and two existing
analyzer findings in each Clang configuration. Clang diagnostics exactly match
the baseline (41 signatures, 110 occurrences). Model generation, formatting,
SDK boundaries, licensing and copyright checks pass.


**NFS4 cold root resolution, September 30 (uncommitted)**

Cold namespace-root resolution now uses the common encoder's allocation,
attempt reset, finish retry and disposal. Its independent builder and completion
are removed. The shared path encoder handles both export entry and cold lookup,
including the physical VFS root and final symlinks. The namespace prelude retains
its caller's cursor and credential and completes no wire operation itself;
namespace/credential selection remains a boundary.

Owned export snapshots are revalidated before execution on every attempt and
under the export lock before cache publication. Path or policy replacement using
the same export id now yields DELAY, including when backend finish accepts the
old resolution. LOOKUP/SECINFO/LOOKUPP cannot swallow this failure and proceed
under an obsolete junction view. Terminal finish errors stop the pending wire
operation; finish EAGAIN uses the common bounded retry.

Four new v4.0/v4.2 accepted/retry suites cover cache behavior, namespace parents,
missing paths, symlinks, same-id replacement during pending finish, fresh cache
resolution, and stopped mutation suffixes. The old binary fails the new cold
junction retry case. All eight targeted cold-root/namespace suites pass, as do
all 58 focused suites in both Debug and Release. Production code decreases by
26 net lines. No VFS API change is needed.

Full repository verification completed. Debug quick passed 270/275 with the five
persistent failures. Release passed 269/275: those five plus an encrypted-SMB
durable trace reporting an unexpected pending CREATE. That extra failure passed
three consecutive isolated reruns without code changes, and passed in Debug;
it remains an unreproduced failure, not a claimed fix. Its fixture enables only
SMB. Both Clang builds report the existing two encoder findings, and diagnostics
exactly match the baseline (41 signatures, 110 occurrences). All model generation,
formatting, SDK boundaries, licensing and copyright checks pass. Full check
remains nonzero for the reported failures/findings. Remaining specialized builders
are LAYOUTGET's two phases and accepted pNFS REMOVE backing cleanup; broader
namespace coalescing and backend transactions remain separate work.


**NFS4 namespace coalescing, September 30 (uncommitted)**

LOOKUPP now remains in the shared compound after cursor changes and under a
real root export. LOOKUP and SECINFO whose names coincide with exports remain
coalesced when they address descendants. Pure execution checkpoints inspect the
actual cursor and validate namespace snapshots; LOOKUPP at the namespace root
returns NOENT without dispatcher fallback. A root resolved earlier in the same
attempt takes precedence over a stale cache, including directory replacement
at an unchanged export path.

Actual junctions, mount-root parent crossings and cold root comparisons defer
only the affected wire operation and suffix. Backend acceptance publishes the
prefix's state/owner journals, saved filehandle and current stateid before the
dispatcher selects the next credential. Deferred WRITE buffers stay with the
request. Finish retries reset both the deferral and conditional VFS skips;
terminal finish failure cannot publish a boundary or successful-prefix reply.
No VFS API or new independent builder is added. Obsolete construction checks and
the root-cache peek helper are removed: production delta is +151/-137, net +14.

The v4.0/v4.2 accepted/retry suites now cover moving and saved cursors, descendant
junction names, SECINFO cursor differences, parentless/non-directory failures,
OPEN/SAVEFH publication, WRITE data before and after a junction, v4.0 owner
replay, empty-prefix deferral, rejected/terminal prefix finish, and stale root
cache precedence. A regression reproduced the stale-cache parent error before
its fix. Submission tracing distinguishes planned spans from accepted namespace
prefixes and rejects repeated or unmatched boundary publication.

Remaining coalescing boundaries include actual cross-export credential changes,
cold root preludes, synthetic namespace handling, repeated root entry and
operation/reply budgets. LAYOUTGET's specialized phases and accepted pNFS REMOVE
backing cleanup remain separate builders. Backend transactions and rollback are
still future work.

Verification completed: focused suites pass 58/58 in both Debug and Release.
Both quick suites pass 270/275, with the same three remote-pNFS and two
POSIX-over-SMB failures. Clang diagnostics match the baseline exactly (41
signatures, 110 occurrences). Its HTML report count is now three in Debug and
four in Release: the additional persistence-test and NLM reports correspond to
warnings already present in the baseline logs. All model generation, formatting,
SDK boundaries, licensing and copyright checks pass. Full `make check` remains
nonzero for the known test failures and analyzer findings.

**NFS4 same-export root re-entry, September 30 (uncommitted)**

Repeated PUTROOTFH/PUTPUBFH and file-then-root sequences now remain in the shared
VFS compound when they retain the same export and credential. Every reset resolves
the configured path afresh. Pure prepares validate the root policy before the
prefix and root lookup; retry discards private root/cursor results. Saved handles,
attribute-directory handles and saved current stateids survive resets, while the
current stateid is cleared. Namespace comparisons use the last successful root
resolution, and only that result updates the shared cache after acceptance.
Earlier roots can have been replaced by mutations within the same compound.

The v4.0/v4.2 accepted/retry tests require one span for repeated roots, cover
consumed and saved cursors, and replace the root directory inside a compound.
Controlled finish rejection revalidates path, squash and security changes before
the prefix executes again. The retry fixture requires three root paths in one
rejected attempt. The old binary fails the exact-span regressions. Actual export
transitions, synthetic namespace work, cold root preludes, budgets, LAYOUTGET
phases and accepted pNFS backing cleanup remain boundaries. Backend rollback is
still future work. Production code grows by 63 net lines in the shared encoder;
there is no new VFS API or independent builder.

Focused verification passes 58/58 in both Debug and Release. Full `make check`
completed with exit 2: Release quick passed 269/275 and Debug 267/275. The five
persistent remote-pNFS/POSIX-over-SMB failures remain. The additional Release
SMB stream-disconnect failure passed three consecutive isolated reruns without
changes. During Debug, disk usage reached 99%: S3 diskfs and POSIX io_uring
reported ENOSPC, and an NFS DRC cairn test aborted without a diagnostic. All
three passed after reclaiming verified unused temporary test images. These
reruns do not establish a code fix for the otherwise unexplained abort or the
intermittent SMB result.

All model generation, formatting, SDK boundaries, licensing and copyright checks
pass. Clang diagnostics match the baseline exactly: 41 signatures and 110
occurrences; its HTML reports contain the two existing encoder findings in each
build. The full check remains red for the recorded tests and analyzer findings.

**NFS4 cold and synthetic root coalescing, October 1 (uncommitted)**

Cold root comparisons now execute inside the shared compound before its saved
and current cursor seeds. A warm cache skips those internal operations; a cold
lookup supplies an attempt-private root handle. Only accepted finish publishes
that handle under an unchanged export-policy snapshot. Root path, policy and
identity replacements cannot survive a rejected attempt, and a changed snapshot
at accepted finish prevents an obsolete namespace continuation. Actual export
crossings still accept their prefix and return to dispatch for credential
selection. Standalone resolution remains a fallback when dispatch or budgeting
prevents coalescing.

Synthetic-root metadata and cursor operations now use protocol checkpoints in
the same compound as bounded virtual READDIR pages. Multiple pages retain their
own staged replies, and their backend lookups do not change the synthetic
current/saved handles. GETATTR stages root attributes privately; ACCESS retains
the virtual directory's supported rights. SECINFO_NO_NAME consumption, parent
errors, inherited SAVEFH and transitions from attribute directories preserve
protocol behavior. Installing a real root while finish is pending invalidates
the entire retried virtual prefix. A lone virtual operation without backend work
still uses its direct protocol handler.

Accepted/retry tests cover both v4.0 and v4.2. The previous binary fails the new
synthetic span assertions and the ordinary physical lookup whose configured
root comparison cannot resolve. That latter case previously returned DELAY
instead of proceeding without a junction view. Production changes are confined
to the shared encoder: 230 lines added, 42 removed, 188 net. No VFS API or
independent builder was added. Final focused validation passes 58/58 in Debug
and 58/58 in Release.

Full `make check` completed with exit 2: Release quick passed 270/275 and Debug
269/275. The five persistent remote-pNFS/POSIX-over-SMB failures retain their
baseline mismatch signatures. Debug also reproduced the known FUSE lock-test
shutdown abort. Three isolated reruns passed, but repetition failed on attempt
12 with "fuse thread destroyed with 1 active requests"; NFS is disabled in that
fixture. This remains unresolved, with its failure log preserved.

Both Clang stages completed and report the same two encoder findings. Warning
signatures and counts exactly match the baseline (41 signatures, 110 occurrences).
All model generation, formatting, SDK boundaries, REUSE, copyright and final
`git diff --check` pass. No new analyzer diagnostic or mismatch signature was
introduced; the full check remains red for the recorded failures/findings.

Remaining boundaries: real export/credential changes; synthetic named SECINFO
and unsupported virtual operations; conservative operation/reply budgets and
associated cold-resolution fallbacks; LAYOUTGET's specialized phases; accepted
pNFS REMOVE backing cleanup; protocol state/lifecycle operations outside the
shared encoder. Backend transaction hooks and rollback remain future work.

### Explicit export selection and credential groups (2026-10-01)

Cross-export real PUTFH, PUTROOTFH/PUTPUBFH and RESTOREFH now remain in the shared
VFS compound. Existing operation groups select immutable credentials derived
from the original RPC credential. They share one finish/retry lifecycle and
stop at the first error. Because groups clear backend cursors, the encoder
explicitly rebinds its retry-private saved filehandle at each transition.

Each wire operation retains its export-policy snapshot. Prepare revalidates
that snapshot, enforces read-only policy for both current and saved endpoints,
and rejects disallowed PUTFH security flavors. Reply staging takes the operation's
export identity explicitly; accepted cursor publication and delegation grants
also use the matching identity. No shared request identity changes during
execution callbacks. Export replacement before a retry produces DELAY at the
affected operation without executing its suffix.

New v4.0/v4.2 tests exercise squash uid/gid restoration, access isolation, signed
GETATTR/VERIFY/READDIR results, saved attribute directories, read-only source and
destination errors, WRONGSEC prefix preservation, and policy replacement during
pending finish. A 15-operation case drops from five VFS compounds to one. The
finish fixture now evaluates executed operations, allowing rejection of read-only
prefixes whose planned mutation suffix never ran; it still cannot reject actual
backend mutations. Production delta is +267/-103, net +164, with no new VFS API.

Focused Debug and Release validation each pass 58/58. Full quick Debug and
Release each pass 270/275 with the same five baseline failures and mismatch
signatures. Both Clang stages finished and report the same two findings;
warning signatures and counts exactly match the baseline (41 signatures,
110 occurrences). All model generation, formatting, SDK header boundaries,
REUSE, copyright and final diff checks pass. Full `make check` completed with
exit 2 for the known failures/findings. The intermittent FUSE teardown failure
did not recur here and remains unresolved.

Remaining namespace boundaries are dynamic junction LOOKUP/SECINFO, mount-root
LOOKUPP, real/synthetic-root transitions, and deferred ROOT/PUB WRONGSEC.

### Known-root junction LOOKUP and named SECINFO (2026-10-02)

ROOT/PUB followed by a known export junction now stays in the shared compound,
including SAVE/RESTORE and later OPEN/data operations. The planner tracks which
root resolution produced each cursor and assigns the existing credential groups.
Prepare revalidates the resolved root, component mapping, export policy and
security flavor. A saved root from an older resolution is not assumed to be the
latest root; the regression replaces a root symlink within the compound and
confirms that restoring the older directory resolves its physical child.

Named SECINFO now handles real-root junctions discovered during execution and
synthetic roots in the same compound. It selects a frozen answer policy without
entering the target export or looking up its backing path. This preserves queries
for inaccessible exports and missing backing paths, ordinary descendant names,
v4.0 directory retention, and v4.1+ current-filehandle consumption. Rejected finish
clears attempt-local selection; changed target policy returns DELAY before the
retried suffix. Replies use the frozen policy, including SECINFO_NO_NAME.

Common v4.0/v4.2 accepted/retry tests cover repeated export groups, signed alias
handles, saved roots, WRONGSEC mutation suppression, symlink and empty-path
semantics, security queries and target policy replacement. Cold SECINFO retry
fixtures explicitly stop before their planned mutation: the suffix is now
reachable, so an executed CREATE would correctly prevent synthetic read-only
rejection. No backend rollback is simulated. The previous binary fails the new
ROOT/LOOKUP/GETFH span assertion, using two compounds instead of one.

Production changes are confined to the shared encoder: +185/-19, net +166 lines.
No additional VFS API or independent builder is introduced. Focused Debug and
Release suites pass 58/58 each. Quick Release and a complete Debug rerun each
pass 270/275 with exactly the five preceding baseline failures and mismatch
signatures. The first Debug sweep encountered temporary filesystem exhaustion
while another worktree ran high-parallelism tests; logs were truncated. All five
additional failed suites passed in the full rerun after space recovered. The
two Clang stages both completed model generation and reported the same two
encoder findings. Warning signatures and counts match the baseline exactly
(41 signatures, 110 occurrences). Formatting, SDK boundaries, REUSE, copyright
and final diff checks pass. Full `make check` completed with exit 2 and remains
red for the recorded failures/findings. The known intermittent FUSE teardown
abort did not recur and remains unresolved.

Remaining namespace boundaries are arbitrary runtime junction LOOKUP, including
inherited PUTFH and LOOKUPP-produced root cursors; mount-root LOOKUPP;
real/synthetic-root transitions; and deferred ROOT/PUB WRONGSEC. Existing VFS
groups select credentials during construction, so these runtime selections need
further executor work or a more general frontend encoding. Conservative budgets,
specialized LAYOUTGET phases, accepted pNFS REMOVE backing cleanup, and protocol
lifecycle boundaries also remain. Backend transaction hooks remain future work.

### Runtime-discovered junction LOOKUP (2026-10-03)

LOOKUP now selects an export junction during execution without ending the VFS
compound. This includes inherited PUTFH, namespace-produced root handles and
RESTOREFH. Potential export names encode conditional junction and physical-child
paths in the existing shared builder. Ordinary descendants retain the source
export identity even when their names match an export.

An optional VFS group credential callback runs after cursor reset and dependency
checks, before operation prepare. It chooses borrowed immutable credentials from
earlier execution results and repeats on every attempt. Its contract permits
only attempt-private frontend updates; authorization errors remain in prepare.
NFS propagates current and saved identities through subsequent operations,
revalidates frozen policies, and restores branch decisions and identities on
retry. Selection groups share the compound's single finish lifecycle.

Common v4.0/v4.2 tests cover exact spans, squash identity changes, saved identity,
ROFS/WRONGSEC suffix suppression, and policy replacement during pending finish.
A new retry regression replaces the root's backing directory at an unchanged
configured path, forcing both junction-to-physical and physical-to-junction
branch changes on retry. Final signed handles verify the selected identity.
VFS tests cover dependency skips and credentials selected from prior results.
Cold LOOKUP finish fixtures now guard their mutation suffix with a failing
VERIFY, since the suffix is no longer deferred. No backend rollback is simulated.

Focused Debug and Release suites pass 58/58 each. Full quick Debug and rebuilt
Release each pass 270/275 with the five persistent failures. A deleted temporary
Ninja wrapper initially prevented Release configuration; its cached path was
repaired and the full Release build and suite ran separately. Formatting passes
after an initial fix.
Clang's initial Debug stage found a new possible missing junction identity;
prepare now rejects it explicitly. Both focused suites and both complete quick
suites were rerun after that guard, with the same counts and baseline mismatch
signatures. Final targeted Debug analysis and the complete ClangRelease stage
report only the two preceding encoder findings. Both full Clang stages completed
all model generation. After replacing the initial Debug encoder diagnostics with
its final rerun, warning content and counts match the baseline exactly: 41
signatures, 110 occurrences, nothing new/excess or missing. SDK boundaries,
REUSE, copyright and final diff checks pass. Full `make check` completed with
exit 2 and remains red for the known test and analyzer failures. The intermittent
FUSE teardown abort did not recur and remains unresolved.
Production changes add 203 net lines across the shared encoder and VFS groups,
including seven executor lines. No independent builder was added.

Remaining namespace boundaries are mount-root LOOKUPP, real/synthetic-root
transitions and deferred ROOT/PUB WRONGSEC. Conservative operation/reply budgets,
specialized LAYOUTGET phases, accepted pNFS REMOVE backing cleanup and protocol
lifecycle boundaries remain. Backend transactions are future work.
