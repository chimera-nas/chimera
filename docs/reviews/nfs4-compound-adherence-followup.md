# NFSv4 compound adherence after refinement

See the [fourth pass](nfs4-compound-fourth-pass.md) for newer OPEN/CLOSE behavior.

Source review of the uncommitted `compounds-refinement` tree, 2026-09-22.
The inventory below records the review before the second implementation pass.
The implementation update at the end identifies addressed findings and current
limits; line references in the inventory refer to the reviewed source snapshot.
The later [third implementation pass](nfs4-compound-third-pass.md) adds READ_PLUS,
COPY/CLONE, and ACL inputs/GETATTR; those entries below are historical findings.

The server still submits supported runs and resumes the legacy dispatcher,
rather than carrying the whole filesystem-bearing wire request through one
VFS compound (`nfs4_proc_compound.c:259`). Most remaining boundaries reflect
adapter structure, not a fundamental requirement to finish the VFS compound.

## Rectifiable boundaries

1. **Stateid/current-FH decisions happen during construction.** READ/WRITE
   authorization is called while encoding (`nfs4_compound_vfs.c:3490`), and
   size SETATTR is authorized before submission (`:3229`). Cursor movement
   therefore forces a split (`:2747`, `:2787`, `:2854`); size SETATTR also
   supports only one such operation per run. Current-stateid use is rejected
   for SETATTR/range operations and for READ/WRITE outside the new OPEN suffix.
   Move these decisions to per-op preparation against the actual execution
   cursor and a private current/saved stateid context. Keep state references
   pinned safely without caching a retry-sensitive authorization decision.

2. **OPEN support has one private open record and a narrow suffix.** Context
   `:189` carries one reserved OPEN, and `:2249` allows only GETATTR/GETFH/
   READ/WRITE/COMMIT after it. Existing-owner/coalesced opens, v4.0 replay,
   exclusive and delegated opens, multiple OPENs, CLOSE, OPEN_DOWNGRADE and
   locks need per-operation state records plus an attempt-private view of
   pending protocol changes. CLOSE must become logically visible to later
   operations without destroying public state before acceptance. Ordinary
   successful-prefix semantics still apply if a subsequent op fails.
   Legacy OPEN still calls fallible install_state after accepted finish
   (`:668`) and can truncate after the VFS compound is disposed (`:1625`).
   That path is not ready for treating finish as the backend commit point.

3. **LOCKT is decided too late.** The actual claim probe is in result filling
   (`:830`); scanning sets may_fail_late (`:2637`) and excludes later mutations.
   Query at the operation's execution checkpoint, copy conflict data into
   attempt-private storage, and reject there. A changing answer on retry is
   valid. The comment that no earlier answer can exist overstates the problem;
   the relevant answer is the one at LOCKT execution.

4. **ACL conversion remains incomplete despite owned result ACLs.** GETATTR
   with ACL falls back because reply bounds still assume fixed non-ACL output
   (`:2533`); VERIFY/NVERIFY ACL comparison falls back (`:2343`). SETATTR and
   OPEN/CREATE ACL inputs are also excluded. Finish attribute input ownership,
   actual ACL encoding/comparison and bounded private result storage. Do not
   remove guards without replacing the lifetime/budget assumptions.

5. **Filesystem operations already expressible in VFS remain omitted.** The
   allow-list (`:233`) excludes READ_PLUS, COPY and CLONE, whose legacy handlers
   still call raw VFS functions. VFS compound primitives exist for all three;
   READ_PLUS can classify then conditionally read using the new callout model.
   COPY/CLONE additionally need correct two-object stateid/credential handling.
   Protocol-only state operations need ordered checkpoints/private state,
   not a fabricated filesystem operation for every wire opcode.

6. **Namespace and export state are not represented during execution.**
   Cross-export PUTFH and inherited SAVEFH/RESTOREFH are excluded (`:2352`,
   `:2433`) because one fixed credential serves the compound. Root junctions,
   LOOKUPP after cursor movement, pseudo-root and named-attribute FHs also
   cause fallback (`:2412`, `:3155`, `:3185`). Carry protocol namespace/export
   context alongside the execution cursor and provide per-op credential
   selection. Crossing exports/backends raises backend participant/atomicity
   questions, but does not itself require losing the frontend boundary.
   SECINFO ends every run (`:293`); consuming the current FH can instead be
   represented in private cursor state, allowing a later PUTFH to establish it.

7. **Delegations/pNFS disable broad classes of conversion.** Delegations
   enabled excludes OPEN (`:3101`) and session GETATTR (`:3171`); delegations/
   pNFS exclude REMOVE and RENAME, and pNFS excludes size SETATTR. Pure callouts
   cannot send recalls or CB_GETATTR requests. Add explicit asynchronous
   coordination steps with logical-request lifetimes/deduplication, capable
   of parking/resuming the same compound. Decide when to suspend or abort a
   backend transaction across such waits. This is substantive architecture
   work, unlike moving a synchronous validation check.

8. **Capacity and error representation still force artificial boundaries.**
   The NFS encoder caps its estimate at128 VFS ops (`:113`, `:2325`) while VFS
   supports1024. Conservative reply estimates reject the entire proposed run
   (`:3151`), and many ordinary validation failures are delegated to legacy
   handlers because VFS errno cannot express their NFS statuses. Use ordered
   frontend-status checkpoints and private result storage with execution-time
   space checks. Preserve real negotiated resource limits; avoid committing
   one partial run merely to rebuild the next due to adapter storage limits.

## Recommended order

First generalize per-op NFS status, execution-time current/saved FH/stateid
context and authorization. Use it to move LOCKT decisions before successors
and expand OPEN suffixes through SETATTR/range operations/CLOSE. Then generalize
OPEN records and provisional updates for coalescing, multiple OPENs, downgrade
and locks. ACL and READ_PLUS/COPY/CLONE conversions can follow independently.
Treat per-op export credentials and asynchronous delegation/pNFS coordination
as explicit larger steps.

Session/replay admission and final response publication can remain a logical
request envelope around the VFS compound. They need not become backend file
operations. Holding that envelope is distinct from splitting filesystem work
into separately accepted compounds.

Validation should assert compound boundaries, not just correct wire replies:
LOOKUP+READ, OPEN+SETATTR+WRITE+CLOSE, multiple OPENs with SAVEFH/RESTOREFH,
LOCKT denial before mutation, ACL operations, and finish-time rejection after
tentative protocol state changes. Existing passing protocol tests cannot prove
that these paths avoid legacy fallback.

## Second implementation pass

The follow-up now moves READ/WRITE, size SETATTR, and v4.2 range-operation
authorization into per-operation prepare callbacks. They inspect the actual VFS
execution filehandle, resolve current-stateid from attempt-private state, and
retain the NFS error privately if authorization fails. LOOKUP/PUTFH movement
therefore no longer forces a split before supported I/O. Multiple size-changing
SETATTR operations have independent inputs and checks. Delegation/layout stateids
still take the legacy path.

Current and saved stateids are tracked privately during execution, initialized
again on retry, and copied back only after accepted finish. Authorized handle
references are reacquired on each attempt and released on reset or final
cleanup. The VFS API now exposes the execution cursor to synchronous callbacks
and allows a prepare callback to have a different private context from an
existing result callback. A finish-EAGAIN unit test checks both callbacks and
the resolved cursor over two attempts with only one final publication.

The single reserved fresh-owner OPEN can now continue through SETATTR, SAVEFH,
RESTOREFH, ALLOCATE, DEALLOCATE, SEEK, and WRITE_SAME as well as its previous
GETATTR/GETFH/READ/WRITE/COMMIT suffix. It still excludes namespace movement after
OPEN, multiple OPENs, CLOSE, coalescing, v4.0 OPEN, exclusive OPEN, and delegation
publication. Legacy OPEN still has its previously documented post-finish work.

Session LOCKT now probes at its execution checkpoint, copies conflicting owner
data into attempt-private storage, and stops before later mutations on denial.
The old `may_fail_late` restrictions are removed. NFSv4.0 LOCKT deliberately
retains legacy dispatch: its clientid validation and lease renewal need an
explicit client pin and acceptance-time publication design.

ACL VERIFY/NVERIFY now stay in the compound and compare the owned ACL snapshot
using a dynamically sized scratch buffer. This also fixes comparisons of ACLs
too large for the old 4 KiB buffer. ACL GETATTR retains its guard: a maximum ACL
can exceed the entire reply buffer, so fixed reply estimates are insufficient.
ACL input ownership for SETATTR/CREATE/OPEN remains separate work.

Completion also handles VFS submission failure before any operation executes.
It reports the first wire operation's error and releases reservations without
asserting on unset operation results or publishing provisional state. This
allocation/build-failure branch was source-reviewed; allocation failure was
not injected through the NFS server.

Repository-owned pynfs wire tests now assert exactly one VFS submission for each
measured request, in addition to checking replies and file contents. They cover
LOOKUP/PUTFH followed by I/O, mismatched stateids stopping before mutation,
repeated size SETATTR, the expanded OPEN suffix and saved current-stateid,
LOCKT denial/continuation, and large-ACL VERIFY/NVERIFY. The test runs in both
v4.1 and v4.2, with 14 and 19 measured cases respectively.

Backend transaction begin/end, real rollback, cross-export credentials,
asynchronous delegation/pNFS coordination, READ_PLUS/COPY/CLONE, and broader
protocol-state reservations remain outside this implementation pass. Passing
wire and synthetic retry tests does not establish real backend rollback.

### Validation of the second pass

Debug+ASan build passed. Final CTest run selected 318 tests: 316 passed, zero
failed, and two skipped. This includes all three combined pynfs minor-version
suites, the two new wire-boundary suites, NFS and VFS unit tests, 224 SDK tests,
eight FUSE tests, 57 boto S3 tests, the Ceph S3 suite, and model probes. The
Linux/io_uring NFS3 probes skipped because the scratch filesystem lacks
`name_to_handle_at`; KVM coverage remains unavailable as documented in the
first-pass report. Shell/Python syntax checks and `git diff --check` passed.

Final build log: `/tmp/chimera-compounds-followup-build.log`.
Final regression log: `/tmp/chimera-compounds-followup-final-tests.log`.
The complete per-test output, including all 33 successful wire-boundary
assertions, is in `/tmp/chimera-compounds-build/Testing/Temporary/LastTest.log`.
All implementation changes remain uncommitted.
