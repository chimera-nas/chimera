<!--
SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors

SPDX-License-Identifier: Unlicense
-->

# Overall compound conversion review — 2026-10-04

Reviewed the current `compound-boilerplate` working tree, including its substantial
uncommitted changes, on top of `a9e0efe078`. Behavior comparisons below use main
`c971e5a5d1e423296640c163fdf5ca0adfe92ae7`. Older conversion inventories describe
earlier snapshots; this report supersedes their outstanding-work lists where they
conflict. This is a source/boundary audit and focused regression run, not a claim
to have re-reviewed every changed line or proved all concurrency interleavings.

Ordinary filesystem routing is close to complete outside SMB. NFSv4 now has one
shared encoder, and its filesystem handlers no longer use the per-operation VFS
API. SMB still has real bypasses and substantial valid-request fallback paths.
NLM acquisition, FUSE retry, shutdown accounting, and publication serialization
remain substantive work. Renaming the old header has not made the northside API
boundary private or enforced.

Three properties must be assessed separately: dispatch through a compound;
preserving the useful request/wire-compound boundary; and accepting/retrying
without exposing tentative state. The first does not establish the other two.

## Findings that need attention

### 1. FUSE shutdown can destroy a worker with an outstanding compound request

**High priority; reproduced again in this review.** The existing
`chimera/fuse/sim/compound_locks` CTest aborted with
`fuse thread destroyed with 1 active requests` at `fuse.c:267`. Its final scenario
parks a blocking lock, releases the open, and shuts down. This has recurred in
earlier passes; it is not an unexplained one-off result or a passing test.

`fuse_server_stop` initiates lock shutdown (`src/server/fuse/fuse.c:113`), and
`chimera_fuse_locks_shutdown` calls the synchronous domain shutdown entrance
(`fuse_proc_lock.c:201`). Canceled operations must still complete on their workers.
Server thread shutdown calls `chimera_vfs_thread_drain` before destroying protocol
state (`src/server/server.c:2584`), but that drain waits only for
`num_active_requests` (`src/vfs/vfs.c:1484`). Parked compound/lock continuations and
a compound waiting for a future finish handler are not necessarily outstanding
backend requests.

Add an explicit stop-admission/cancel/drain contract that keeps workers and
frontend resources alive through terminal compound completion. Count active
compounds, including their parked and finishing phases, rather than counting only
backend requests. Cancellation must first make progress possible; simply waiting
on an additional counter can deadlock on unresolved waits. The current abort is
confirmed; exact attribution to conversion versus the older FUSE shutdown path
has not been established by running main.

### 2. SMB LockSequence and range publication still lack common admission

**High-priority concurrency concern; not a reproduced race in this review.**
`smb_vfs_reset` snapshots `open->lock_seq_*` under the open bucket mutex
(`src/server/smb/smb_compound.c:431`). LOCK decides replay from that private
snapshot (`smb_proc_lock.c:365`). The VFS publishes RANGE state in
`chimera_vfs_compound_finish_result` (`src/vfs/vfs_compound.c:4061`), while the SMB
LockSequence cache is updated later in `smb_vfs_terminal` (`smb_compound.c:596`).
`chimera_vfs_claim_journal_bind` pins a normal owner without excluding another
normal journal (`vfs_claim_journal.c:284`).

Another channel can therefore snapshot the old replay state before the first
operation finishes, or enter between range publication and replay publication.
Two mutex-protected copies do not serialize that interval. Depending on the range
operation, a duplicate could be re-executed or return a conflict/missing-range
result instead of replaying the original answer. Existing sequential replay and
held-finish tests do not prove this interleaving safe.

Use a logical per-open/replay-bucket admission token, or validate a version while
holding an equivalent reservation from replay decision through accepted protocol
publication. Add a deterministic two-channel duplicate LOCK/UNLOCK test at both
intervals. This concern was raised in the September review and remains present.

### 3. SET_REPARSE now safely declines previously attempted operations

**Confirmed source-level compatibility narrowing, distinct from a stale-identity
bug.** The unsafe handle-only fallback has been removed. Current SET_REPARSE
requires `CHIMERA_VFS_CAP_REMOVE_MATCH_FH` (`smb_proc_reparse.c:559`), and only memfs
advertises that capability in the in-tree backend source. Linux/io_uring, diskfs,
cairn and the proxies therefore reject the handled NFS-style reparse replacements
before mutation. Main attempted remove/create without that requirement.

Even on memfs, `smb_set_reparse_native_eligible` (`:102`) and the subsequent
occupancy checks exclude cached, durable/resilient, locked, stream, DOC and other
stateful/peer-held cases. Replacing a nonregular source is also rejected. The
eligible operation runs as a standalone compound, not in the enclosing SMB batch.

Rejecting before unlink is safer than corrupting identity. It nevertheless means
the conversion does not yet preserve the former support envelope. Complete the
state migration and following-command identity overlay; provide a safe backend
replacement/matched-identity facility where feasible. Do not restore the old
unsafe fallback or simply drop the capability check. Treat any intentionally
unsupported cases as an explicit compatibility decision, not as completed work.
The memfs ioctl probe passes, including safe-rejection checks; this pass did not
run a new cross-backend SET_REPARSE reproducer.

### 4. Retry readiness is uneven even among compound-routed handlers

**Confirmed source gap; future optimistic-backend behavior, not proof of a
current nontransactional filesystem failure.** FUSE has 27 production raw-submit
sites and two shared retry-adapter sites (locks and COMMIT/FLUSH). For example,
OPEN reports aggregate errors directly (`fuse_proc_io.c:92,219`), as does READDIR
(`fuse_proc_dir.c:311,416`). A rejected finish EAGAIN becomes a kernel-visible
failure rather than transparent retry. READDIR's reset support alone does not
make its production submission retry.

SMB has 41 raw-submit sites versus nine shared-adapter sites. The native batch
uses the adapter, but many fallback helper compounds do not. Generic CLAIM and
RECALL operations also set `nonretryable` in the executor (`vfs_compound.c:8192,
8350`), and retry explicitly refuses those attempts (`:8443`). They can publish
live arbitration/recall state; wrapping a handler around them is not equivalent
to its journaled native path.

Audit immutable inputs/reset for each remaining FUSE/fallback path and use the
shared completion policy where safe. Replace normal request claim transitions
with journals and recalls with explicit repeatable coordination. Preserve the
distinction between operation EAGAIN and finish EAGAIN. NFSv3/NFSv4 implement
their own finish-aware retry, so their raw-submit counts are not the same gap.

### 5. The overall check is still not green

The most recent completed full check, before this read-only source review,
retains three NFS remote-pNFS failures (memfs/diskfs/cairn), two POSIX-over-SMB
failures (`batch_smb_memfs`, `strict_smb`), and analyzer findings. The proxy cases
include CREATE/permission/attribute/statfs semantics; the POSIX cases include
symlink traversal and namespace/errno discrepancies. These are current behavior
failures, not proof that each was introduced by this conversion. They still need
root-cause and baseline disposition before declaring behavioral parity.

The former false OPEN reply-size rejection and lost WANT-aware OPEN decline after
coalesced CLOSE are fixed and covered; do not list them as outstanding. Earlier
SMB filesystem-capability and lease-identity failures were also repaired. The
latest full Release check additionally saw an SMB lease failure which passed
three unchanged isolated repetitions; it remains an observed intermittent result.

## Conversion coverage by consumer

| Consumer | Current routing | Remaining conversion/composition work |
| --- | --- | --- |
| NFSv4 | Ordinary filesystem and state work uses the shared encoder, including namespace/credential transitions, named attributes, layouts and state returns. No direct ordinary per-op VFS calls found. | Finite wire/helper/reply limits may split runs. Private layout grant followed by same-file truncate returns DELAY rather than waiting on a grant the client has not received. Accepted pNFS REMOVE backing cleanup is a separate maintenance compound. Client/session establishment and recovery remain separate. |
| NFSv3 filesystem/MOUNT | Ordinary procedures and MOUNT path resolution are compound-routed. Filesystem procedures have finish-aware retry and suppress rejected results. | No ordinary procedure conversion omission found. Replay/recovery persistence is separate; NLM is not covered by this conclusion. |
| NLM | TEST is OPEN plus CLAIM_TEST in a compound. LOCK opens in a compound. | LOCK then calls direct `chimera_vfs_claim_acquire` and publishes its own pending/held state. UNLOCK/CANCEL/reaping use that lifecycle. Needs acquisition/publication integration, while retaining independent cancel/remote grant acknowledgements. |
| SMB | Broad native grouped executor with related FileIds, private opens, ACCESS/RANGE journals and accepted publication. | Remaining direct calls below; cached/durable/DOC lifecycle; broader rename/link/stream identity cases; SET_REPARSE; command-scoped cancellation; fallback retry. |
| S3 | GET/PUT, metadata, ACL/tagging, listing/deletion, copy, multipart and cleanup are compound-routed with shared retry. | Bounded streaming/copy/assembly stages intentionally have multiple acceptance scopes. Copy/assembly could batch more, but cannot silently promise upload-wide rollback. No direct ordinary VFS call found. |
| Client SDK / POSIX | Ordinary filesystem operations use compounds and shared retry. POSIX typed locking is compound-routed. | Typed lock compounds cannot mix with other filesystem operations; projected locks are not optimistic-transaction capable. Administrative and handle lifetime calls remain separate. |
| FUSE | Ordinary filesystem requests, mount bootstrap, FLUSH/FSYNC and locks use compounds. | Ordinary finish retry and parked-request shutdown above. Typed-lock composition limits. Coherence grants, invalidation and teardown are independent control operations. |
| REST | Debug filesystem endpoint uses compounds with shared retry. | No ordinary filesystem omission found. Mount/mkfs/configuration/users are administrative APIs. |
| fio / elbencho / cufile and other SDK users | Reach the filesystem through the client/POSIX layer; no direct internal per-op VFS call found in the production source inventory. | Inherit the client/POSIX behavior, rather than needing a separate per-op conversion. |

NFSv4 now has exactly two allocation/submission sites: the shared encoder
(`nfs4_compound_vfs.c:8240,9854`) and accepted DS cleanup (`:4117,4143`). Its encoder
recognizes 56 of the dispatcher's 68 named opcodes. The other twelve administer
client/session/replay state, not missing filesystem handlers. See the current
[NFS inventory](nfs4-compound-remaining-2026-10-03.md) for exact limits. Partial
layout-return semantics and remaining transport interoperability work are older
protocol limitations, not newly discovered compound-routing omissions.

S3's transfer/retry unit is configured I/O size, default 128 KiB and capped at
1 MiB (`s3_compound.h:15`). Small GET/PUT keeps lookup/open/data together; larger
requests retain accepted handles and advance by chunks. Multipart assembly now
uses nondestructive COPY or READ/WRITE, with at most 128 range operations or one
buffered read per attempt (`s3_multipart.c:2171`). The older destructive MOVE
finding is fixed. Metadata listing now follows cookies/grows buffers
(`s3_metadata.c:332`); the older single-page finding is fixed too. Independent
cleanup and final publication remain explicit. Previously accepted chunks and
HTTP bytes cannot be undone by retrying a later compound.

NLM's remaining acquisition is at `nfs_nlm.c:987`, after its OPEN compound
completion (`:1004`) and submission (`:1391`). The compound wrapper comment saying
the claim "cannot be one" describes the present implementation, not a permanent
architectural restriction. Pending admission, blocked/granted protocol messages,
cancellation and reclaim must be represented deliberately; an ordinary replayable
completion callback must not send NLM_GRANTED or publish held state.

FUSE/POSIX typed locks currently permit exactly one lock operation plus cursor
seeds (`vfs_compound.c:8610`). Projected mutation refuses an installed optimistic
finish adapter before effects (`vfs_lock.c:724`). Successful projected SEEK_END
moves the owner to backend-only arbitration (`:758`), so other protocols' local
claim queries lose its range visibility. These are explicit composition and
cross-protocol arbitration limits, not missing fcntl/FUSE dispatch wrappers.

## SMB work that could still be coalesced

- Cached CREATE allows a restricted related QUERY_INFO/READ/FLUSH suffix and
  WRITE for leases (`smb_proc_create.c:7785`). CLOSE, LOCK, another CREATE and
  namespace mutations need a private cache/member journal and immutable CREATE
  response snapshots, rather than publishing a grant to continue construction.
- CREATE with stream caching, create-time DOC, requiring-oplock and many durable,
  reconnect/AppInstance/cold-recovery contexts still bypasses the native batch
  (`smb_proc_create.c:7840`). Cached/durable CLOSE is generally terminal; complex
  stream/base DOC has its own fallback. These are supported request shapes which
  split execution, not wholly unimplemented opcodes.
- Native RENAME requires atomic no-replace and source identity matching
  (`smb_proc_set_info_rename.c:983`). Occupied replacement needs stronger outcome
  and destination guarantees. Only memfs advertises the complete matched-rename
  set. General descendant/cross-view directory moves, stream-itself rename,
  occupied LINK and held/pending targets retain boundaries. Existing bounded
  directory coalescing must not be mistaken for all directory moves.
- General native cancellation is not command-scoped. The batch cancel registry
  deliberately registers only isolated CLOSE (`smb_compound.c:71`); LOCK and
  notifications have separate paths. Extend cancellation without canceling
  unrelated SMB groups or discarding an accepted prefix.
- Persistent cold recovery and cleanup repair/tombstones remain incomplete.
  Unsupported persistent grants are declined, and record deletion failures are
  best effort. Their full support envelope needs a separate compatibility review;
  merely including durable KV operations in compounds does not establish recovery.

Protocol framing, IPC pipes, long-lived NOTIFY waits and session/tree changes
are reasonable separate boundaries. A fallback caused only by missing private
filesystem/state representation is a candidate for conversion; a boundary needed
to receive peer progress or change protocol lifetime needs an explicit policy.

## Making the per-operation north-facing API private

It is **not enforced today**. `vfs_internal_procs.h` has 74 callable declarations;
19 production frontend translation units still include it (ten S3, one FUSE,
eight SMB). No guard restricts its inclusion, and `check_vfs_sdk_includes.sh`
checks the backend SDK boundary, not the frontend boundary. The repository's
claim that the header is not includable north of VFS overstates current reality.
The internal header is not installed as part of the backend SDK; that is different
from preventing in-tree northside dependencies.

After stripping comments and excluding tests, all remaining northside references
to its functions are these eight sites, covering seven symbols:

| API | Caller / effect if unavailable | Disposition |
| --- | --- | --- |
| `chimera_vfs_overwrite` | `smb_proc_create.c:177`; fallback CREATE overwrite/truncate and stream cleanup | Convert to the existing typed OVERWRITE machinery, retaining share admission. |
| `chimera_vfs_getattr` | `smb_proc_set_info.c:1693`; fallback delete-disposition validation | Merge validation with its request compound. |
| `chimera_vfs_readdir` | `smb_proc_set_info.c:1860`; directory emptiness validation for disposition | Express dependent enumeration in that compound. |
| `chimera_vfs_open_fh` | `smb_proc_close.c:1571`; fallback DOC parent open | Convert accepted deletion cleanup/lifecycle to an explicit compound. |
| `chimera_vfs_remove_at_match_fh_actor` | `smb_proc_close.c:1637`; fallback matched DOC unlink | Same lifecycle conversion; preserve exact victim/actor identity. |
| `chimera_vfs_recall_handle_lease` | `smb_doc_compound.c:794` and `smb_proc_set_info.c:1796`; peer cache recall | First is already an explicit COORDINATE callout. Move the primitive to a narrow coordination interface or provide a typed coordination form; do not misclassify it as file I/O. |
| `chimera_vfs_dirent_match` | `smb_proc_query_directory.c:832`; pure directory-name filtering | Move the utility declaration to a public helper header. No compound needed. |

There are thus **five ordinary filesystem bypass sites**, two peer-coordination
sites, and one pure utility dependency. Banning the header immediately fails
nineteen includes; making its operation symbols uncallable also breaks the live
SMB paths above. S3/FUSE stale includes can be removed without converting another
filesystem handler, after retaining the proper helper/type includes.

Symbol privacy is a separate step. `nm -D` confirms the six non-utility symbols
above still exported by the current `libchimera_vfs.so`; implementations retain
`SYMBOL_EXPORT`. The VFS root module itself imports `chimera_vfs_getattr` and
`chimera_vfs_open_fh` across its shared-library boundary. Blindly hiding every
per-op symbol would break that internal module too. Keep an internal linkage
contract for VFS/root/tests, or change that linkage arrangement. Northside source
privacy does not require deleting the executor's per-op implementation vocabulary.

There is a second leak: `vfs_release.h` includes `vfs_open_cache.h`, which includes
`vfs_internal.h`. Fifty frontend files include release helpers, transitively
seeing allocation/dispatch/cache internals. SMB also includes the latter headers
directly. Make public release/retain functions out-of-line against opaque handles;
split helper queries and lifecycle hooks from cache implementation. An existing
out-of-line `chimera_vfs_release_handle` already exists (`vfs_proc_open.c:699`), but
its declaration is itself in the internal per-op header.

## What should remain public alongside compounds

"Public" here means supported for northside consumers, not an unrestricted
installed ABI for every internal structure.

| Facility | Needed by / appropriate scope |
| --- | --- |
| VFS/thread init, configuration, drain, destroy; mount/unmount, mkfs/rmfs | Server, REST and SDK control plane. Appropriate independent lifecycle APIs; strengthen drain as above. They are not ordinary inode transaction operations. |
| Opaque handle retain/release and owned-result disposal | All frontends and SDK; needed after taking compound results and for disconnect/dup/close cleanup. Appropriate. Retain/release need not expose open-cache shards or backend-private handle fields. |
| Credentials, ACL/SID/idmap helpers, root FH, capability and export/mount queries | Request authentication, translation and immutable admission inputs. Appropriate as narrow constructors/read-only queries. Avoid exporting module dispatch pointers or mutable mount-table internals just to read capabilities. |
| Claim/state references, cancellation, ACK/revoke/retire and lock-domain shutdown | Required so independent protocol returns, disconnects and recalls can unblock parked work. Appropriate lifecycle/control APIs. Request LOCK/UNLOCK/share mutations should use compound journals; mandatory teardown remains independently executable. |
| Optional caching-grant admission and accepted publication hooks | NFS delegation publication, FUSE grant rearming and SMB cache membership. Some standalone use is appropriate after acceptance or in an independent coherence lifecycle, with atomic arbitration and safe decline. Broad direct acquire/shrink/range-publish APIs must not become alternate request entrances. NLM acquisition remains a conversion gap; accepted NFS journal publication is not an ordinary I/O bypass. |
| Notify watch create/update/drain/destroy and invalidation ACK | SMB CHANGE_NOTIFY and FUSE coherence. Appropriate out of band; a long-lived watch must not hold a backend transaction open. Mutation event emission should belong to accepted VFS publication, or a tightly scoped accepted frontend hook, rather than arbitrary callbacks. |
| Default-store recovery/replay KV | NFS DRC, NSM and NFSv4 recovery still make 21 direct put/get/delete/search calls. Removing them now breaks persistence and recovery. A separate protocol-state store interface is reasonable. If the invariant is all storage through compounds, add typed default-KV operations or a dedicated store transaction API; keep autonomous startup/reaping lifecycle separate. |
| pNFS device/configuration queries, blob/FH helpers and maintenance scheduling | NFS and server setup. Appropriate control/query surface. GET_LAYOUT/materialization and DS filesystem cleanup already execute through compounds. Multiple backends still need a future participant policy. |
| Pure result/name/range/error helpers and value types | Attribute encoding, name matching, lock inputs, error translation, buffer ownership. Appropriate; they do not execute filesystem work. |

The current surface is broader than this table warrants. In particular, live
DOC setters/`release_doc`, cache shard access, synthetic-handle allocation, raw
claim list mutation and executor-private lock-attempt methods are not justified
merely by calling them "lifecycle." DOC can cause unlink and has ordering/identity
effects. Keep the necessary accepted retirement hooks while moving request
disposition/CLOSE work into compounds. SMB pipes need a protocol handle lifetime,
not unrestricted VFS allocator internals. Split `vfs_lock.h`'s explicitly
executor-private half into an internal header as well.

`vfs_kv.h`'s comment that keys cannot belong in compounds is already contradicted
by the typed PUT_KEY_AT/DELETE_KEY_AT/SEARCH_KEYS_AT operations
(`vfs_compound.h:501`). The important question is persistence ordering and backend
participation, not whether an operation uses the current FH. DRC records and
filesystem mutation on different stores are not atomic merely because both use
the compound API. State that limitation explicitly.

The backend finish-handler installation/result interface also belongs to the
executor/backend integration or testing surface, not general frontend authority
to declare a transaction committed. Frontends need submission, status, retry,
cancel, resource transfer and the defined pure/coordination callbacks.

## Deferred backend work that the frontend contract must accommodate

Begin/finish/abort, association of every nested request, and rollback remain the
explicitly deferred backend project. Do not expand this frontend review into an
unplanned backend transaction implementation. It does need a contract for:

- Transaction association through inferred opens, access gates, path walks,
  copy fallbacks and pNFS redirects, including multiple backend participants.
- Transaction-aware VFS caches and notifications. For example REMOVE still
  emits notify and updates the name cache in its per-op completion before
  compound finish (`vfs_proc_remove_at.c:61,96`). Frontend purity cannot undo that.
- An infallible accepted-publication phase. `compound_finish_result` can call
  `lock_attempt_accept` after successful finish, but the latter can still return
  EINTR after close/cancellation (`vfs_lock.c:745`). Prevalidate/freeze fallible
  admission before backend commit, then publish while its gates remain held.
- Logical serialization spanning backend acceptance and frontend journal
  application. Neither a later mutex nor keeping references alive alone prevents
  another request observing an inconsistent mixture; the SMB replay concern is
  a current example of that distinction.
- Cancellation/drain progress for compounds which hold no active backend request.

Current rejection fixtures validate frontend reconstruction and publication in
their covered cases. They do not prove rollback of actual backend mutations;
current backends cannot supply that guarantee. No test in this review attempted
to synthesize rollback after a real mutation.

## Suggested refinement order

1. Fix FUSE shutdown/drain and deterministically resolve the SMB LockSequence
   interleaving. Triage the existing integration failures without weakening
   expected results.
2. Convert the five SMB filesystem bypass sites, extract the recall and pure
   helper declarations, remove stale includes, and enforce a frontend include/
   symbol allowlist in CI. This makes the compound-only filesystem entrance real.
3. Complete FUSE ordinary retry and NLM acquisition/publication; audit standalone
   SMB retry rather than assuming the native adapter covers its fallbacks.
4. Broaden SMB cache/DOC/durable and namespace/SET_REPARSE cases, with an explicit
   backend support matrix and command-scoped cancellation tests.
5. Narrow the public lifecycle/control/query headers, including out-of-line
   retain/release, and make the recovery-KV exception an explicit design decision.
6. Before enabling optimistic backends, implement association, accepted cache/
   notify publication, commit/publication interlocks and real rollback tests.

## Validation and evidence

No production or test code changed in this review. Ran existing CTests against
the current Debug build, with network namespaces enabled where their fixtures
require them. **133/134 passed**: VFS compound/claim tests, NFS compound suites,
SMB compound/identity probes, S3 multipart profiles, SDK finish rejection, POSIX
lock retry and REST filesystem compound coverage. The failure was the FUSE lock
shutdown abort, whose server diagnostic was preserved before another run could
overwrite it. No fresh full check, Release, Windows or physical RDMA run was made.

Evidence: `/tmp/chimera-overall-compound-review-20261004/` contains the source call
and include inventories, CTest inventory, `focused-debug.log`, and
`fuse_compound_locks.debug.log`. The immediately preceding full check and baseline
comparison are in `/tmp/chimera-nfs4-open-fixes-20261004/`.
