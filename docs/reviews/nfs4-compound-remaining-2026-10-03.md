<!--
SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors

SPDX-License-Identifier: Unlicense
-->

# NFSv4 compound completion inventory — 2026-10-03

The [2026-10-04 regression review](nfs4-compound-regressions-2026-10-04.md)
and follow-up address two confirmed in-scope fixes: false OPEN reply-size rejection and
loss of the WANT-aware OPEN decline when a coalesced CLOSE retires its state.
These are distinct from the preexisting transport limitations listed below.

Updated after the 2026-10-04 transport ownership follow-up. Keep three milestones separate:
using VFS compounds for filesystem work, consolidating builders, and retaining
one finish/retry scope for a wire COMPOUND. The first two are substantially
complete; the last still has the explicit limits below. Opcode counts are not
completion percentages or guarantees about every execution shape.

The shared encoder recognizes 56 of the dispatcher's 68 named opcodes, up from
46. The remaining twelve are client/session administration, listed below.
There are now two explicit NFSv4 allocation/submission sites: the shared encoder
and accepted pNFS REMOVE backing cleanup. LAYOUTGET's two independent builders
are gone. The compound_single/compound_state wrappers use the shared encoder;
they do not contain alternative direct filesystem/state implementations.

## Stable workstreams

| ID | Implemented | Explicit remaining limits |
| --- | --- | --- |
| N4-01 Namespace transitions | Runtime mount-root LOOKUPP, synthetic-root LOOKUP into an export, cross-kind PUTFH/SAVEFH/RESTOREFH, conditional real/synthetic READDIR, and ROOT/PUB security error placement now share the enclosing attempt and frozen credentials. Synthetic state/error checkpoints now remain in the run, using the opaque protocol identity rather than the attribute directory's base inode. Removed the obsolete namespace-deferred handoff and its suffix/WRITE ownership bookkeeping. | Changed export snapshots still reject with DELAY; finite construction limits can still split runs. |
| N4-02 State-owner binding and fallback consolidation | OPEN/CLOSE/CONFIRM/DOWNGRADE/LOCK/LOCKU and IO_ADVISE use shared execution, including standalone fallback. Mixed v4.0 clients are pinned and checked per operation; accepted OPENs update connection binding in wire order. Lock-parent/range storage grows with standing state instead of silently truncating fixed scratch arrays. Delegation/layout IO_ADVISE validates client and FH. | Competing owner/state reservations may still return DELAY. Successful admission is not permission to publish before finish. Lifecycle/recovery admission stays separate. The removed direct-handler stub's exact GRACE-to-NO_GRACE switch/call-count scenario is not identical to the new wire replay coverage. |
| N4-03 Private state admission | Anonymous/bypass operations after private CLOSE/CONFIRM/DOWNGRADE can share the run. Size SETATTR, ALLOCATE/DEALLOCATE, WRITE_SAME, SEEK, READ_PLUS and CLONE destination carry the attempt's claim view. Full/flush recall and invalidation respect private claim exclusions. | Native CLONE source admission retains its existing caller-authorized-state assumption; this pass does not introduce a general anonymous source-side admission API. NFS CLONE requires a supported normal source state; COPY retains anonymous/scoped admission. |
| N4-04 LAYOUTGET | One shared encoding selects native or MDS/DS materialization, retains MDS identity, and journals layout slots, versions and device publication until acceptance. Retry reuses memoized coordination and resets private grant state. LAYOUTGET/current-stateid/GETDEVICEINFO suffixes coalesce. | Cross-backend atomicity and rollback require the planned backend transaction project. Materializing mutations are not covered by synthetic finish rejection. Return/regrant and truncate interactions have the N4-08 limits. |
| N4-05 pNFS REMOVE | Multiple ordinary REMOVEs coalesce. Accepted-prefix cleanup records retain each victim's MDS identity and credential; post-acceptance DS deletion retries independently. | DS deletion remains a maintenance compound after accepted namespace deletion; it cannot undo that deletion. A DS backend without conditional remove has a lookup/compare/remove fallback; atomic protection against unrelated DS replacement requires stronger backend support. |
| N4-06 Capacity | Wire maps are capped separately at 128; helper descriptors use the VFS's 1024 limit. The scanner retains a fitting prefix. GETATTR stages compacted actual results and rejects insufficient space before a successor executes; a lone GETATTR is not charged a second 8192-byte floor after producing its result. READDIR (ordinary, named and synthetic), READ_PLUS, GETXATTR and LISTXATTRS now stage actual results before successors execute. Static planning retains fixed result storage and construction scratch; transport array space is reserved separately. | Finite wire/helper capacity, initial dispatcher admission and remaining fixed estimates can split runs. Conservative mutation/security size bounds and the N4-10 transport edge cases below remain. |
| N4-07 Error/protocol checkpoints | Invalid names/attributes/options, malformed later FH, absent saved FH, minor/recovery checks and covered synthetic errors become terminal checkpoints with the successful prefix. GETDEVICEINFO, GETDEVICELIST, DELEGPURGE, LAYOUTSTATS and LAYOUTERROR can sit inside a filesystem run. Synthetic stateful errors now use shared checkpoints. | Lone protocol-only responses need no VFS compound. Unsupported protocol features remain unsupported. |
| N4-08 State retirement | DELEGRETURN, LAYOUTRETURN, FREE_STATEID and RELEASE_LOCKOWNER use private tombstones/reservations and accepted-only publication. Private later stateid uses reject retired identities. ALL layout return works without an FH. Concurrent pending CB_GETATTR can coexist with DELEGRETURN; stale callback publication is suppressed. Private delegation return followed by REMOVE/RENAME/LINK, FILE/ALL layout return followed by fresh grant, returned/canceled layouts followed by truncate, and truncate followed by grant now coalesce. UNCHECKED OPEN truncation now uses the same layout coordination as SETATTR. | An active private grant followed by truncate still returns terminal DELAY in the same finish. FSID layout return retains its preexisting no-op behavior; ALL still ignores requested layout type, and partial range/iomode return semantics are not implemented by this conversion. Revoked-delegation FREE ownership is checked in source and through common admission, but no dedicated forced-revocation wire fixture was added. |

The remaining causal N4-08 limit is an active private layout grant followed by
size-changing SETATTR or truncating OPEN of the same file. The client has not
received that grant and cannot return it while the server waits. The server
accepts the successful prefix and returns DELAY at the mutation, preserving
file contents. A return that cancels that grant removes this limit.

N4-09 is the transport-admission workstream. The third pass implements:

- One TCP/RDMA arena reservation shared by the encoder, single-operation
  paths, initial result-array allocation, dispatcher fallbacks and TEST_STATEID.
  Fully decoded requests that have already exhausted that headroom now return
  a small RPC error before entering the NFS handler; decoded WRITE references
  are released on this path and on fully decoded requests with trailing bytes.
- Aggregate READ vector admission before successor operations. Counters live
  in the attempt, reset on retry, and publish with the accepted prefix; they
  survive separate VFS spans in the same wire COMPOUND. A first READ exceeding
  its offered nonempty RDMA Write-chunk capacity also stops before successors.
- First-READ Write-chunk selection, one-time decoding, actual returned lengths
  for reduced replies, and ownership cleanup on the new length-rejection path.
  Later READ data stays inline or goes in the Reply chunk. This follows
  [RFC 8267 section 6.4.1](https://www.rfc-editor.org/rfc/rfc8267.html#section-6.4.1).
  Unsupported multi-chunk offers are rejected before dispatch.

The finite 260-vector output limit remains; the operation that would exceed it
returns RESOURCE before a following mutation can run. This pass uses admission,
not payload copying. Physical RDMA hardware has not been tested; in-process
RDMA exercises actual registered-memory placement and transport ownership.

N4-10 now admits complete logical response bytes during execution, separately
from arena storage and descriptor counts:

- The dispatcher accounts accepted results across spans. VFS attempts reset
  private byte counters on retry. Read-only variable results are measured
  before successors execute; mutations reserve a success bound before their
  first helper. Each successful operation also leaves space for the next
  operation's size error, including SETATTR's error bitmap. The unsafe final
  success-to-REP_TOO_BIG rewrite is removed.
- Admission includes the tag, fixed XDR fields, external READ data, RPC
  headers/verifiers and a conservative GSS wrapping bound. It checks both
  negotiated limits and the owning SEQUENCE session when a later
  CREATE_SESSION changes request binding. Requested caching with a zero-byte
  limit fails at SEQUENCE without advancing the slot.
- SEQUENCE reserves cache storage and session capacity before success.
  Capacity failure preserves the old slot sequence and cached reply; capture
  transfers the reserved buffer instead of allocating after mutations.
  Optional shrinking retains and accounts the original allocation on failure.
  Wire regressions replay a successful WRITE prefix plus REP_TOO_BIG_TO_CACHE
  byte-for-byte without re-executing the WRITE. See
  [RFC 8881 section 2.10.6.4](https://www.rfc-editor.org/rfc/rfc8881.html#section-2.10.6.4).
- libevpl exposes RPC/security overhead and the offered RDMA Reply capacity
  before dispatch. Admission checks the reduced Reply body separately from
  the complete logical response, retaining the first READ's placement across
  spans. This prevents a short Reply chunk from rejecting newly executed
  mutations at send time. Capacity arithmetic is widened to 64 bits.
- The new exact-boundary regressions exposed an existing xdrzcc bug: fixed
  opaque size estimates omitted the field bytes. Fixed opaque encoding and
  decoding also omitted odd-length padding. Both are corrected, with aligned,
  odd, typedef, fragmented and truncated-field coverage. These generator
  changes affect all generated RPC codecs, not just NFS.

The 2026-10-04 follow-up closes the RDMA replay gap:

- The capture contract describes the omitted Write payload and includes its
  padded size in the logical length. The NFS copy helper restores the payload
  and padding, so the cache still contains ordinary procedure-result XDR;
  no transport-specific cache format or persistence change is introduced.
  Existing v4.0 cache eligibility and size limits remain in effect.
- NFSv4 replay locates the first successful READ using the generated decoder
  for preceding result arms, then separates that payload into the retry's
  current Write offer. Empty first READs consume the offer; later READs stay
  in the reduced body. RPC framing, Reply placement and security wrapping
  continue through the ordinary send path.
- Both Write and Reply capacity are checked before posting any RDMA writes.
  An insufficient retry offer leaves the cached answer available for another
  retry. Cache-hit preparation failures send RPC SYSTEM_ERR rather than
  allowing the v3/v4.0 dispatchers to re-execute a cached mutation.
- New wire probes cover v4.0/v4.1/v4.2, two READs followed by WRITE/SETATTR,
  odd and empty first payloads, changed offers and destination addresses,
  short chunks, and stream/RDMA transitions for session replay. They compare
  the complete logical answer and verify that an intervening file overwrite
  survives each replay. Tests use in-process registered-memory RDMA, not
  physical hardware.

The next 2026-10-04 transport pass fixes the three identified cases:

- Write selection now retains the offered segment count independently of byte
  capacity. Nonempty lists of zero-length segments remain selected, including
  in NFS admission and cached replay. Only a zero segment count means an empty
  chunk, as defined in [RFC 8166 section 3.4.6](https://www.rfc-editor.org/rfc/rfc8166.html#section-3.4.6).
- Read-list complexity is bounded before allocating RPC views in the shared
  arena. The supported nonzero-position chunk requires one aligned position
  within the reduced procedure body, at most sixteen segments, checked total
  message size and non-wrapping remote address ranges. All RPC lookup and
  allocation checks precede reads. A submission reference prevents early
  completion from freeing the request during posting; failed reads drain and
  return ERR_CHUNK instead of aborting the server.
- Scoped XDR ownership tracking releases only acquired inline clones after a
  partial decode, trailing data, or NFS reply-space rejection. Borrowed RDMA
  references remain with the transport/caller, including when a later result
  fails after the first chunk was claimed. The fragmented padding reader now
  checks vector bounds and propagates truncated-padding failure. Truncated or
  unsupported-version RDMA headers are rejected before their fields are used;
  matching outstanding calls complete with a decode error.

N4-10 still has these explicitly separate transport items:

- Position Zero / Long Call receive support remains absent. Multiple distinct
  nonzero Read positions are rejected rather than reconstructed. Supporting
  Position Zero alongside the nonzero chunk is also part of the NFS baseline
  interoperability requirement in [RFC 8267 section 6.4.2](https://www.rfc-editor.org/rfc/rfc8267.html#section-6.4.2).
- Returned Write/Reply descriptors still need stricter matching against the
  advertised handles, addresses and counts. `evpl_rpc2_take_reply_chunk` still
  sums returned Reply lengths in 32 bits. The normal unused-Write and failed
  result-decode paths are covered, but a malicious peer claiming a nonempty
  Write payload for a successfully decoded result with no eligible data needs
  explicit claim tracking rather than the current length-based inference.
- Physical RDMA hardware and provider-specific disconnect/cancellation paths
  have not been exercised by this in-process transport pass.

These are preexisting correctness gaps exposed by the transport audit, not
new independent filesystem builders. The original diagnostic is retained
under `/tmp/chimera-nfs4-fleet3-20261003/`; the admission fixes, snapshots and
verification logs are under `/tmp/chimera-nfs4-wire-budget-20261003/`.
Successful mutation reservations and GSS overhead remain conservative bounds;
small actual READs and filehandles are charged at their actual encoded size.
RDMA admission has unit and transport/model regression coverage. The new
replay probes exercise short Write and Reply offers on the wire using
in-process RDMA; physical RDMA hardware has not been tested.

## Intentional boundaries

The twelve dispatcher opcodes outside the shared encoder are SEQUENCE,
SETCLIENTID, SETCLIENTID_CONFIRM, EXCHANGE_ID, CREATE_SESSION, DESTROY_SESSION,
DESTROY_CLIENTID, BIND_CONN_TO_SESSION, BACKCHANNEL_CTL, RECLAIM_COMPLETE, RENEW
and SET_SSV. They establish replay/session/client admission or administer
protocol state. Keep them outside filesystem replay unless a specific supported
mixed sequence justifies journal work.

Reference drops, connection/lease teardown, callback transport, recovery KV
operations, synchronous capability queries and accepted DS maintenance also
retain their appropriate independent lifecycles. Backend begin/end hooks,
rollback and distributed transaction guarantees remain the separate backend
project. Current finish-rejection fixtures only reject attempts whose executed
filesystem work is read-only; passing them does not prove mutation rollback.

## Source and regression anchors

- Shared planning, cursor/identity selection, reply bounds, terminal checkpoints,
  accepted publication and DS cleanup: [nfs4_compound_vfs.c](../../src/server/nfs/nfs4_compound_vfs.c).
- State/retirement admission and publication: [nfs4_state.c](../../src/server/nfs/nfs4_state.c).
- Shared native/materialized layouts: [nfs4_pnfs.c](../../src/server/nfs/nfs4_pnfs.c)
  and [nfs4_pnfs_compound.h](../../src/server/nfs/nfs4_pnfs_compound.h).
- Pure protocol result preparation: [nfs4_protocol.c](../../src/server/nfs/nfs4_protocol.c).
- Existing boundary/adoption/feature modules check wire outcomes and exact VFS
  spans. New checkpoint, multiclient and retirement modules additionally check
  suppressed suffixes, identity, and private/public state across pending,
  accepted, retried and terminally rejected finish.
- VFS claim/compound/clone tests check that an attempt's exclusions do not leak
  into later unscoped operations or suppress another client's conflicting claim.

## Previous-pass verification

All implementation workers finished before combined builds and tests began.
Follow-up workers supplied source-only fixes/reviews; integration, formatting,
builds and ctest execution were performed centrally.

- Debug and Release builds pass. The expanded NFS/VFS/POSIX focused selection
  passes 79/79 in each configuration, up from the previous 58-suite selection.
- Full quick runs pass 270/275 in each configuration, with exactly the previous
  five failed suites and normalized mismatch signatures: NFS remote pNFS on
  memfs, diskfs and cairn; POSIX batch_smb_memfs and strict_smb. These remain
  unresolved failures, not expected passes.
- Combined testing exposed and corrected SMB retry-run truncate identity and
  handle cleanup bugs, plus memfs partial-block WRITE_SAME corruption.
- An intermediate Release run exposed FUSE OPENDIR retaining only an unbacked
  path-cache descriptor. A new root-only simulator case deterministically
  reproduced READDIR ESTALE after removing the open directory. OPENDIR now
  retains a backend reference until RELEASEDIR. All 13 FUSE quick/extended
  checks pass in both configurations, including real mounts. The final full
  Release run includes this fix; Debug's full run precedes this FUSE-only fix
  and is supplemented by the final 13 FUSE checks.
- Both full Clang analysis stages completed, supplemented by targeted final
  analysis after SMB/FUSE corrections. Diagnostics total 42 normalized warning
  signatures / 116 occurrences, versus 41 / 110 before this pass. The sole
  additional signature concerns delegation retirement after destroy. Source
  review confirms that the retirement reservation retains an extra reference
  until all accesses finish; the reported path assumes only one reference.
  This is not a demonstrated runtime use-after-free, but the warning remains
  unsuppressed. Final FUSE analysis adds no findings.
- `make syntax`, syntax validation, SDK include boundaries, REUSE, copyright
  checks and `git diff --check` pass. The required `make -k check` completed
  with exit 2; subsequent builds and reruns corrected intermediate regressions,
  while the five baseline tests and static-analysis findings still prevent a
  green sweep.

Evidence and worker reports are retained in
`/tmp/chimera-nfs4-fleet-20261003/`. Final focused logs are
`focused-debug-final.log` and `focused-release-review.log`; full quick logs are
`quick-debug-stable.log` and `quick-release-fuse-final.log`. `compare-final.txt`
records the baseline comparison. Earlier failed and invalidated runs are kept
separately. NFS top-level production code decreased by 183 lines in this pass
(25 files, +3244/-3427), excluding tests, VFS and SMB/FUSE corrections.

## Second-pass implementation and verification

Three source-only workers handled layout transitions, variable replies and
synthetic identities. The parent implemented namespace recall views, integrated
shared-file patches, and ran all builds/tests. Workers then supplied source-only
fixes and cross-reviews for failures found by the combined run.

New regression modules cover synthetic identity/error checkpoints, layout
replacement/barriers and reply exhaustion. Existing retirement cases now cover
namespace mutations and unrelated holders. The SMB metadata and rename probes
exercise the internal view entrances after their signature updates.

Second-pass verification is complete:

- Debug and Release builds pass; focused suites pass **101/101 in each**.
  Logs: `focused-debug-verified.log`, `focused-release-verified.log`.
- Both full quick suites pass **270/275**, with exactly the five baseline
  failures and normalized mismatch signatures listed above.
- Both full Clang stages completed. Diagnostics are unchanged at **42 normalized
  signatures / 116 occurrences**; no added or removed warning occurrences.
- `make syntax`, syntax validation, SDK include boundaries, REUSE, copyright
  validation and `git diff --check` pass.
- Required `make -k check CTEST_PARALLEL=8` completed with exit 2. The five
  baseline test failures and existing analyzer findings still prevent a green
  sweep; they have not been reclassified as passes.
- Intermediate testing caught and corrected the missing LINK prepare, lower
  RENAME recall view, synthetic FILE return admission, omitted TCP transport
  reservation, and test-fixture issues. Final logs supersede those failed runs.
- The new finish-retry cases still reject only attempts with read-only executed
  filesystem work. No backend mutation rollback or physical RDMA coverage is
  claimed. Inproc RDMA quick suites exercise the shared RDMA reservation path,
  without establishing worst-case fragmented or multi-READ reply correctness.

Evidence and frozen worker reports: `/tmp/chimera-nfs4-fleet2-20261003/`.
`check.log` records the full sweep; `compare-final.txt` records the exact
baseline comparison. The NFS top-level production change in this round is
14 files, +839/-488 lines (net +351), excluding tests and VFS changes.

## Third-pass implementation and verification

Three source-only workers handled arena admission, READ vector accounting, and
RPC/RDMA placement. The parent merged shared files, formatted and built both
configurations, and ran all tests centrally. Follow-up review corrected the
fragmentation fixture's illegal mutation of completed VFS results and an
introduced decoder-error buffer leak before final focused verification.

The final focused selection passes **114/114 in Debug and Release**. RPC
transport suites pass **55/55 in both**, including multi-READ, empty-first,
odd-length, Reply-chunk, short-capacity and malformed-length cases. The latter
use in-process RDMA; physical RDMA was not tested. The final NFSv4 RDMA model batch also passes in each configuration.

Both full quick runs pass **270/275**, with exactly the five baseline failed
suites and normalized mismatch signatures. Both full Clang stages completed.
They exposed one new write-list initialization diagnostic; the loop now uses
the same presence guard as initialization. Subsequent analysis of that
translation unit reports no findings in either configuration. With those final
runs replacing its pre-correction diagnostics, the total is unchanged at
**42 normalized signatures / 116 occurrences**. All focused and transport
regressions above were rerun after the guard correction.

`make syntax`, final syntax/SDK-boundary checks, REUSE, copyright checks and
`git diff --check` pass. Required `make -k check CTEST_PARALLEL=8` completed with
exit 2: the five baseline tests and existing analyzer findings remain unresolved.
No new final analyzer warning or quick-test mismatch remains.

Evidence: `/tmp/chimera-nfs4-fleet3-20261003/`. Final focused logs are
`focused-debug-verified.log` and `focused-release-verified.log`;
`transport-debug-verified.log` and `transport-release-verified.log` record
the final 55 transport tests. `analysis-*-final.log` and `compare-final.txt`
record the final analyzer comparison; `check.log` retains the complete sweep. `late-response-limit.log` records an intentional
diagnostic reproduction of the remaining bug, not a passing product regression.

Production change against the round snapshot: NFS seven top-level files
+239/-81 lines (net +158); libevpl RPC +70/-27 (net +43); xdrzcc builtins
+77/-24 (net +53). Total net +254, excluding tests, CMake and documentation.
No VFS production changes were made this round. The transport changes are in
the existing libevpl and nested xdrzcc worktrees; no commits or pushes were made.

## Wire-response admission follow-up verification

Evidence: `/tmp/chimera-nfs4-wire-budget-20261003/`.

- Final combined focused tests: **194/194 in Debug and Release**, including
  NFS compound/replay, VFS, RPC transport and XDR suites. Four new wire cases
  cover minor versions 1/2 and accepted/retried execution. Tests require
  rejection before WRITE/SETATTR, exact fit and padding, actual short results,
  cross-span budgets, zero cache limits, and byte-for-byte replay of an accepted
  WRITE prefix followed by a cache-size error.
- Both full quick suites: **270/275**, with the same five baseline failures
  and identical normalized mismatch signatures.
- Both full Clang stages completed with **42 normalized warning signatures /
  116 occurrences**, identical to the previous final baseline. There are no
  added or removed warning occurrences. `compare-final.txt` records this
  comparison and the test-failure comparison.
- Required `make -k check CTEST_PARALLEL=8` completed with exit 2. Its initial
  formatting check caught unstable alignment in a test initializer; that
  formatting-only issue is corrected, and final syntax/SDK checks pass
  (`repository-final.log`). REUSE, copyright and final diff checks also pass.
  The five baseline test failures and existing analyzer findings still prevent
  a green sweep.
- Initial Linux-backend setup failures were corrected by using the supported
  worktree test filesystem. Initial exact-boundary failures exposed the fixed
  opaque generator defects described above; final regressions pass.
- No VFS production changes. Net production increase is 335 lines across NFS,
  libevpl metadata/admission and xdrzcc, excluding tests, build wiring and docs.

## RDMA replay follow-up verification — 2026-10-04

Evidence: `/tmp/chimera-nfs4-rdma-replay-20261004/`.

Complete Debug and Release builds pass. The expanded focused suites pass
**231/231 in each**, including compound, replay/DRC/GSS, RPC transport and XDR
coverage plus the three new wire probes. Logs are `focused-debug-final.log`
and `focused-release-final.log`. Both full quick suites pass **273/278**;
three new replay probes account for the increase from 275 total. The five
baseline failures and normalized mismatch signatures are unchanged.

Both full Clang stages completed with **42 normalized warning signatures /
116 occurrences**, identical to the previous final baseline, with no additions
or reductions. `compare-final.txt` records both analyzer and test comparisons.
`make syntax`, syntax/SDK boundary checks, REUSE, copyright and final diff checks
pass. Required `make -k check CTEST_PARALLEL=8` completed with exit 2 because
of the five baseline failed suites and existing analyzer findings; the full
sweep is still not green.

Initial failed fixtures and builds are retained separately: borrowed read-into
ownership, v4.0 cache eligibility, the new helper's export annotation, and
formatter alignment were corrected before the final verification above.

Net production change is +161 lines across NFS and libevpl, excluding tests,
build wiring and documentation. No VFS or XDR-generator production changes
were made in this pass.


## Transport ownership follow-up verification — 2026-10-04

Evidence: `/tmp/chimera-nfs4-transport-20261004/`.

Complete Debug and Release builds pass. Both focused suites pass **231/231**;
final targeted suites pass **19/19 each**, covering the corrected transport
fixture, NFS decode/replay and NFS backend ownership tests. The raw transport
fixture includes twenty-three offer/list cases, late result failures with owned
and borrowed destinations, and malformed reply headers followed by late replies.
Debug includes ASan and iovec ownership canaries. These use in-process RDMA.

Required `make -k check CTEST_PARALLEL=8` completed with exit 2. Both full quick
suites pass **273/278**, with exactly the five prior failed suites and identical
normalized mismatch signatures. Both full Clang stages completed with unchanged
**42 normalized warning signatures / 116 occurrences**; `compare-final.txt`
records no additions or reductions. Final fixture-only analyzer rebuilds report
no bugs in either mode. Syntax (including libevpl/xdrzcc), SDK boundary, REUSE,
copyright and final diff checks pass. Baseline failures still prevent a green
repository sweep.

Initial failures are retained separately: missing test linkage and fixture
corrections, the newly exposed/fixed fragmented-padding overrun, and one unstable
initializer layout. The final fixture has stable formatting and stronger valid-key
admission cases. Disk exhaustion interrupted an early Release build and Debug
sweep; disposable compiler cache and old analyzer build trees were reclaimed,
and the affected checks reran successfully. No source or diagnostic evidence was
removed during that recovery.
