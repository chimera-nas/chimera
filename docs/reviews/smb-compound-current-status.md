# SMB compound conversion checkpoint

Updated 2026-09-26, wave21 validated checkpoint. This snapshot supersedes older
progress sections in the wave-by-wave notes. Backend transactions and rollback
are not implemented.

## Enabled behavior

- Ordinary `FILE_CREATE`/`FILE_OPEN_IF` (including directories) and uncached `FILE_OPEN` can
  produce private handles consumed by later commands in the same VFS compound.
  Ordinary data OPEN performs live-cache recall through explicit coordination.
- Ordinary OVERWRITE/OVERWRITE_IF/SUPERSEDE use a typed OVERWRITE after
  access, type, DOS attributes, deletion state, share admission, and cache recall.
  Base overwrite removes named streams; overwriting a stream preserves siblings.
  Temporary write-sharing rights narrow privately after overwrite, so following
  commands see the final rights while other requests retain conservative admission
  until acceptance. Metadata-only overwrites retain requested share-denial bits
  during destructive-operation admission, then narrow to their inert lifetime claim.
- CREATE extended attributes use typed LIST/SET/REMOVE_XATTR in the same compound,
  with case-preserving lookup and accepted-prefix error handling. Owned input buffers
  support contexts beyond 1024 bytes within the existing transaction-size limit.
  Sequential EA updates preserve spelling, including new names and delete/recreate.
  Native CREATE and SET_INFO check available operation capacity before mutation;
  long lists use bounded accepted EA compounds with retained name history. The
  remaining legacy CREATE lifecycle also uses this compound EA adapter.
- Unbuffered CREATE/OPEN now coalesces, with APPEND removed from effective and
  granted access, including generic/MAXIMUM_ALLOWED expansion. Ordinary leaves
  with OPEN_REPARSE_POINT also coalesce for other dispositions; actual symlinks
  retain a pre-mutation discovery/legacy boundary.
- Parked durable eviction and complex sharing lifecycle are discovered before
  mutation/publication. The preceding prefix is accepted, temporary resources
  drain, and the affected command executes through the legacy path before native
  compound processing resumes. Rejected finish never starts that fallback.
- Ordinary I/O, attribute/EA/ACL operations, enumeration, local locks, sparse,
  copy/offload, several IOCTLs, ordinary CLOSE, and nonpersistent resilient CLOSE
  have compound handlers. Resiliency registry changes publish after acceptance.
- File-backed LOCK/UNLOCK is compound-only. All old lock-entry producers and
  their completion queue are removed; native CLOSE owns the same canonical RANGE
  token used by LOCK. Initial compound allocation failure reports resources
  without changing ranges or LockSequence state, while preserving FileId
  inheritance. Warm durable reconnect retains the canonical owner.
- Regular-file disposition/DOC uses all-file preflight fences and typed matched
  removal. Blocking LOCK and cache-recalling OPEN split from guarded DOC/rename
  runs to avoid waiting for a peer CLOSE blocked by those same fences.
- Nonreplacing regular-file RENAME of public or earlier-produced handles within supported
  share views coalesces on memfs, including nested and cross-directory moves.
  Typed VFS rename requires atomic NOREPLACE,
  expected-source-FH matching, and suppressed immediate notification. Accepted
  publication updates only the affected namespace link, preserving hardlink aliases.
  Preallocated view fences cover existing and newly arriving pending opens;
  unsupported foreign views defer before mutation.
  ReplaceIfExists also coalesces when the destination is absent. The native
  operation first uses atomic NOREPLACE. On EEXIST, a typed lookup recognizes
  a disappeared target or same-inode alias; a distinct unheld regular target can now use strict matched replacement under
  a constructor writer token and target fences. Live target holders or other
  unsupported state still defer before mutation. Same-inode
  hardlink aliases can complete natively using atomic source and destination
  matching plus an explicit backend NOOP outcome; neither path publication nor
  rename notification occurs for a no-op.
  Live stream holders no longer force a regular-base rename boundary. Accepted
  publication updates both namespace participants and any queued stream-DOC
  event path, including when the remaining holder uses another hardlink.
  Provisional sources bind their actual identity and acquire nonblocking gates during
  execution. Contention defers only the untouched rename after its producer prefix.
- Uncached named-stream OPEN/CREATE/OPEN_IF and OVERWRITE/OVERWRITE_IF/SUPERSEDE
  on regular files and existing directory bases coalesce with related I/O and CLOSE. The base opens without truncation, and base DELETE
  sharing plus stream SHARE reservations use one readiness endpoint. Both owners
  drain on failed admission; accepted filesystem prefixes remain intact.
  Stream opens retain a base handle through ACCESS retirement. Base and stream
  namespace records publish atomically with the latest accepted base path.
  Stream truncation is a typed OVERWRITE after both reservations and cache
  coordination; it preserves the base data and sibling streams. CREATE EAs use
  the retained base handle and the same sequential case-preserving EA pipeline,
  including accepted-prefix failures and pre-mutation capacity deferral.
- Ordinary nonstream CREATE can grant legacy II/EX/BATCH oplocks after accepted
  finish using a preallocated candidate and nonbreaking admission. Related
  QUERY_INFO, READ and FLUSH on its private FileId can share that compound.
  Legacy-oplock WRITE and cache-lifecycle consumers still require accepted
  publication before execution. Admission can cap the
  grant to II/NONE; mandatory-oplock requests remain legacy. Guaranteed-declined
  DHnQ requests can use the native path.
- Regular-file RqLs CREATE supports all six dispositions and all requested caching
  modes, with or without NON_DIRECTORY_FILE. Hintless type lookup and actual-handle
  revalidation defer unsupported directory lease policies before mutation. Truncation uses typed
  OVERWRITE after ACCESS admission and repeatable H/W recalls. Shared leases publish
  after accepted finish, including same-key joins retaining stronger existing
  rights, version/epoch and outstanding-break state. ParentLeaseKey contexts now participate natively; stream and durable
  contexts remain boundaries. CREATE
  admits related QUERY_INFO, READ, WRITE and FLUSH on the same private FileId
  before grant publication. The private actor retains LeaseKey identity so a
  same-key WRITE does not recall its existing grant. Unsupported suffixes,
  including CLOSE, LOCK, another CREATE and namespace mutation, still split.
  Lease reply
  version is snapshotted privately so suffix CLOSE cannot leave a dangling pointer.
  RH/RW/RWH use the existing repeatable H/W recall coordination; invalid W/H-only
  requests normalize to NONE and share policy can cap fresh grants to READ.
  Allowed same-key upgrades advance the epoch only at accepted publication.
- SMB3 lease-v2 directory CREATE/OPEN/OPEN_IF uses typed compounds for explicit
  DIRECTORY_FILE and hintless requests when directory leasing is enabled.
  Accepted publication uses DIR_LEASE, preserving directory-content recall,
  WRITE masking, same-key joins/upgrades and settled-break rearming. Invalid
  W/H-only requests grant NONE; share policy can cap to READ. CREATE remains
  bounded by the same private-suffix contract; ParentLeaseKey is supported,
  while stream and durable contexts remain boundaries.
- Legacy II/EX/BATCH, shared RqLs, directory-lease and cached-stream CLOSE accept preceding
  metadata in their compound and end that VFS run. FileId, membership and cache rights remain public until
  acceptance; member removal follows acceptance and grant teardown follows journal
  drain. Cached streams retain and retire both base and stream ACCESS owners.
  Fence contention or newly observed DOC defers the untouched CLOSE.
- Nonpersistent durable-v1/v2 CLOSE uses the terminal compound path for regular
  files, directory leases and named streams on regular or directory bases,
  including shared grants. Accepted publication atomically detaches the
  live tree owner or parked registry owner under bucket then registry locks.
  Request references prevent reconnect from rehoming the open during execution.
  Exactly one owner reference retires; cache teardown follows journal drain.
  Stream CLOSE validates the exact retained base handle, base FH and canonical
  base ACCESS claim before retirement. Both owners and the base handle survive
  held/rejected finish. Complex DOC remains a boundary.
- Persisted CLOSE orders typed claim retirement, durable-record DELETE_KEY_AT
  and exact-handle CLOSE in one cancellation scope. FileId/registry/grant
  publication remains accepted-only; cancellation after retirement drains the
  deletion and resource cleanup before reporting cancellation. Existing
  best-effort record-delete failure semantics remain, with accepted-only logging.
  Persisted CLOSE is terminal and retains complex-DOC boundaries.
- Notification-bearing CLOSE now uses native retirement. Accepted publication
  atomically detaches the watch and queues cleanup on each request's owning worker.
  A prior independent deletion retains DELETE_PENDING precedence; the closing
  handle's later DOC event cannot replace its own NOTIFY_CLEANUP response.
- SMB3 LOGOFF flushes admitted watches on surviving sibling channels. Notify
  requests retain session/tree memory through signed/encrypted cleanup. Every
  same-session slot in a LOGOFF packet retains its own reply context, while later
  commands, including SESSION_SETUP, cannot use those snapshots as live channels.
- Directory-rename contained-open discovery uses typed streaming READDIR
  compounds. Each page resets private collection on retry; recall and rename
  continuation require accepted finish. A bounded native path also coalesces
  same-parent moves of root-child directories, including chained peer queries
  and producer-related OPEN/RENAME/QUERY/CLOSE. Unrelated same-view root-child
  handles are allowed: all SMB RENAME/LINK mutations now take admission readers,
  and the directory writer excludes foreign constructors and mutators through
  acceptance. Effective private paths are checked alongside public participants.
  It still excludes deeper unrelated handles, foreign views, unresolved constructors, held children,
  occupied destinations and scans exceeding 128 entries. Closed subtrees survive.
  Public directory CLOSE still starts a separate admission run after rename.
- Hardlink ReplaceIfExists requests coalesce when the target is absent. Typed
  LINK always uses atomic no-replace semantics and suppresses immediate notify;
  EEXIST defers the untouched command to legacy replacement after its accepted
  prefix. Accepted native LINK emits its notification only after finish.
- Ordinary stream CLOSE coalesces query/close sequences, retires stream and base
  ACCESS owners together, and fences both identities against delete intent. Stream
  or base delete-on-close work remains a separate lifecycle boundary. Mixing
  stream CLOSE with preflight-DOC base CLOSE also splits the VFS run.
- Invisible pre-OPEN name records track accepted external renames without writing
  another request's private open. Binding validates the acquired inode; retry can
  discard a stale path update if the original name now resolves to another inode.

## Contract and correctness fixes

- Frontend prepare/completion callbacks remain private or read-only. Cache/input
  coordination is explicit and repeatable where necessary; filesystem mutation
  and tentative protocol publication are forbidden inside coordination callbacks.
- Dynamic grouped coordination is permitted only from completion callbacks.
  Memo identity includes operation position, callback, context, and resolved FH;
  contexts must remain valid through compound destruction. Cancellation scopes
  still prohibit dynamic insertion after irreversible resource retirement.
- Cleanup preserves producer handles until claim journals drain, and destroys
  consumers before their private CREATE slots. Deferred WRITE/RDMA input survives
  redispatch; skipped output buffers retain descriptor-local ownership.
- Namespace-admission waits park before FileId acquisition, retain session/tree
  context, and can be canceled or drained by their owning loop. The generic
  dispatcher now discards parsed suffixes on disconnect for legacy as well as
  native completion, including their owned WRITE and SESSION_SETUP input buffers.
- Plain disposition clear preserves CREATE delete-on-close mode for regular files
  and named streams. The Quint model now distinguishes mode from the shared
  deletion mark. The replay harness waits for a final asynchronous CREATE reply
  before encoding a request using its FileId. Once all modeled ACK-required
  blockers clear, it also waits for that final reply before advancing to an
  independent request. This preserves the trace ordering and actual reply checks.
- Memfs rename stabilizes directory ancestry under a topology lock. On inode
  contention it drops all inode locks before waiting, then revalidates identities,
  names and parent linkage. Independent regular renames retain parallelism.
  Deterministic tests cover contention, rebinding, orphan parents and opposing
  directory moves; this does not fix all other namespace-operation lock ordering.
- Shared grant acquisition now attaches the SMB member atomically with its grant
  reference under the file lock. Initial grant admission, claim/grant insertion
  and same-key coalescing are also atomic: no transient published candidate can
  be freed while a concurrent breaker holds a pin. Fresh v2 epoch seeding occurs
  before visibility and never overwrites a coalesced grant or break increment. Last-member CLOSE cannot revoke rights between
  those steps. Removing a member reanchors the grant to a surviving handle.
  Members carry their lease/oplock wire tag before attachment; legacy result
  publication takes the same lock as break callbacks. Native CREATE snapshots
  the same-client grant cap before destructive recall and intersects it with
  live admission at acceptance, preserving refusal across the ACK wait.
- Directory-stream replies use a stream-local type snapshot, retaining stream size
  and allocation while leaving backend type, access checks and shared metadata intact.
  The same correction applies to legacy durable reconnect replies. Related
  READ/WRITE explicitly bind the private producer data handle instead of reopening
  through a path cursor and incorrectly checking the directory base type.
- Native RqLs validates lease-key file identity and checks missing create names
  before mutation when the key is already bound. Legacy named streams validate
  against the stream FH instead of the base FH; a nonmutating preflight reuses
  the exact stream handle, preventing wrong-key rejection from creating a fork
  or stamping its base metadata.
- Native CREATE snapshots the private open before visibility, then captures final
  lease mode/epoch/flags under the file lock. Concurrent breaks cannot mix reply
  fields from different lease states.
- VFS rename reports UNKNOWN, MOVED or NOOP without changing existing callback
  signatures. Memfs atomically checks an expected occupied destination and reports
  successful no-ops; legacy SMB then preserves cached source paths and suppresses
  rename notifications. Other backends retain UNKNOWN until they implement this.
- VFS module SDK version is 5 for the changed request/handle-state descriptor
  contracts. The descriptor reports filesystem creation independently of record
  storage success.
- Name-based typed CREATE now propagates NO_NOTIFY for symlink and mknod as
  well as mkdir. Both ordinary and synchronous observer paths honor it;
  authorization and cache updates remain active. Existing standalone APIs
  retain their behavior. This supplies notification control for future reparse
  conversion; the restricted identity migration described below now uses it.
- Directory rename scan allocation/malformed-FH denial can no longer be cleared
  by a subsequent scan before the first recall wave. Each page retains its
  directory handle, restores private scan state on retry, and blocks mutation
  after terminal finish failure or cancellation.
- Stream DOC now uses atomic expected-FH matching under the memfs base inode
  lock; unsupported backends reject before mutation. STREAM_NAME notification
  publishes only after accepted successful removal, never for a replaced name.
- CREATE rejects oversized basenames before copying into fixed-size metadata.
  Overwrite notifications snapshot the execution-time namespace identity.
- CREATE explicitly marks ACCESS reservations provisional until a final readiness
  checkpoint succeeds. Failure, skipped readiness, or cancellation withdraws only
  that reservation before later groups run. Owner storage survives until normal
  teardown; earlier reservations, CLOSE journal edits, and filesystem effects remain
  intact. Unmarked VFS groups keep their existing prefix semantics.
- ACCESS narrowing uses append-only private journal changes, preserving earlier
  changes across CLOSE rollback and retry. Actual rights narrow only on accepted
  publication; waiter callbacks run after frontend publication.
- Dynamically acquired provisional-rename gates drain before retry re-enters a
  producer's cache waits. This releases attempt-owned coordination resources;
  it does not publish paths or roll back filesystem effects.
- EA parsing rejects relative-offset wraparound. Maximum-length canonical names
  fit their scratch buffers. Chunk retry restores the current chunk's starting
  offset/history, leaving accepted earlier chunks intact.
- Legacy CREATE EA/overwrite failures unhash the failed FileId and retire its
  references/claims. Persistent records are removed through a cleanup compound
  before completing the error. Atomic tree/registry detachment also transfers a
  parked durable owner's reference, avoiding a leak if another channel parks the
  open while its EA stage is pending. Successful filesystem prefixes are preserved.
- Persistent CREATE grants require successful recovery-record storage. Routes
  without records (fresh directories, directory OPEN_IF and named streams) decline
  PERSISTENT while preserving eligible ordinary durable grants. Default-KV OPEN_AT
  writes after handle authorization; storage failure releases the handle and
  reports an I/O error. Failed subsequent SMB admission or ambiguous PUT failure
  deletes the unpublished key through a cleanup compound before the error reply.
  Recycled and redispatched CREATE requests clear stale reply ownership before
  cleanup examines it. Successful filesystem prefixes remain intact.
- CHANGE_NOTIFY admission is serialized against native CLOSE fences and session
  invalidation. CLOSE rechecks for watches after acquiring its fence and defers
  untouched when needed. Parked requests retain watch state/tree memory and drain
  on their owning workers; pending admission supports CANCEL/disconnect/session
  cleanup. PreviousSessionId detaches and flushes watches before parking durable
  opens, permitting fresh notification admission after warm reconnect.
- SET_REPARSE_POINT on an ordinary unshared regular placeholder now uses a typed
  standalone compound: matched REMOVE, CREATE, OPEN, ACCESS reservation/retirement
  and accepted handle/file-state/namespace migration. An exclusive FileId gate
  excludes existing and arriving consumers; pinned request/session/tree state and
  CANCEL/disconnect tracking preserve lifetime. Unprivileged CHR/BLK requests
  fail before unlink, preserving the placeholder. Read-only fallback cannot start
  mutation after cancellation. Cached/durable/locked/DOC/stream or nonregular
  sources retain the guarded legacy path. This remains a wire batching boundary.
  See the [identity migration design](smb-set-reparse-compound-design.md).
- A client-wide LeaseKey registry reserves identity before native and legacy
  backend acquisition and survives live, parked and warm-reconnected opens until
  final retirement. Bound keys use existing-only backend operations, preventing
  name disappearance after lookup from creating a wrong-key object. Lease ACK
  lookup spans authorized sessions of the same client with pinned tree lifetime.
- Primary and secondary compound I/O actors retain the open's canonical ACCESS
  owner across durable reconnect, then add the cache grant's lease-key bytes.
  A permitted cross-client nonlease reconnect cannot recall its own BATCH grant
  or conflict with its retained RANGE owner. Legacy READ/WRITE use the same rule.
- ParentLeaseKey publication carries protocol and client identity in the new
  full-actor notification API. Native CREATE and all direct SMB event publishers
  use it, including deferred stream deletion's originating client. Same key bytes
  under another client no longer qualify for the exemption.

## Remaining work and risks

- Cross-layer publication needs a concurrency audit. RANGE publication happens
  in VFS finish_result before SMB terminal publication of LockSequence state;
  range-owner pins protect lifetime but do not serialize ordinary journal
  bindings. A second worker can snapshot stale replay state during that interval
  (or while the first attempt is executing). Source review identifies a potential
  duplicate/misclassified LOCK replay; a deterministic two-channel test holding
  this boundary is still needed. Passing sequential replay tests and completing
  the conversion do not establish atomic backend/VFS/frontend publication.
- CREATE stream leases, unsupported directory-lease policies,
  mandatory-oplock requests and durable/reconnect/AppInstance contexts,
  create-time DOC, and actual symlinks under non-OPEN OPEN_REPARSE_POINT dispositions
  retain legacy paths. Named-stream reparse options and
  specialized caching/durable/DOC opens remain boundaries. Oversized EA lists
  use the existing bounded compound adapter after pre-mutation deferral.
- CLOSE with stream/base DOC and unsupported DOC/POSIX-delete cases remains
  a lifecycle boundary. Caching CREATE splits before unsupported grant-lifecycle
  suffixes, and caching CLOSE remains terminal. Removing those boundaries requires
  private grant state and an accurate CREATE reply even when CLOSE leaves no open.
- Long-lived CHANGE_NOTIFY remains a frontend async lifecycle. LOGOFF now drains
  admitted watchers on sibling channels; broader non-durable open/claim teardown
  on surviving deleted channels still follows the existing session-refcount lifetime.
- Wire CANCEL now reaches an executing single-command native CLOSE. Cancellation
  during its retirement suffix drains cleanup; cancellation after finish begins
  preserves the accepted result. Grouped native cancellation and pre-submission
  input/preflight waits remain boundaries; canceling one command must not cancel
  unrelated commands in the same VFS batch.
- Occupied RENAME targets with live/pending protocol holders still defer, as do
  general directory and stream-itself RENAME and occupied-target LINK. The bounded native
  regular-target path now excludes all foreign constructors before backend
  acquisition. Broader legacy replacement still has a delayed-admission race;
  the new path does not claim to repair that fallback. Strict backend capabilities
  remain memfs-only.
- Legacy rename now updates exact-link public and pending peers, but a move across
  directories viewed through different share roots still lacks full relative-path
  translation. Directory-descendant path updates also need work.
- A destination ancestor moved after lookup can leave the share-relative path
  snapshot stale even when the exact parent FH and moved link remain correct.
  Parent/ancestor stabilization or path reconstruction remains necessary.
- Stateful legacy SET_REPARSE still swaps handle identity without migrating all
  ACCESS/RANGE/namespace state. The FileId gate is now present, but only the
  restricted typed slice migrates canonical identity. Full coalescing needs a
  tentative identity overlay consumed by following wire commands.
- Actor-aware legacy rename/link/matched-remove now retain client-qualified
  ParentLeaseKey exemptions through asynchronous permission gates. Existing
  exported APIs remain compatible; key-only notification conservatively recalls
  everyone. This fixes identity plumbing, not the remaining conversion boundaries.
- Backends without atomic rename outcomes retain the legacy same-inode no-op
  cached-path/notification gap. Memfs now reports it explicitly.
- Persistent stream/fresh-directory creation and complete cold recovery remain
  unfinished; unsupported routes now decline the persistent grant. Cold records
  lack exact file/base/stream identity and the full namespace/access/lease
  lifecycle, and cold DH2C does not satisfy fresh DH2Q grant processing. Generic
  OPEN_FH handle-state persistence also has cached/inferred bypasses; SMB does
  not use that API in the repaired paths.
- Failed CREATE durable-record deletion logs allocation/backend failures and
  returns the original CREATE error. A persistent repair queue or tombstone is
  still needed to guarantee removal after those faults; successful normal cleanup
  and parked-owner cleanup are now implemented. Persisted CLOSE preserves the
  same best-effort deletion policy and needs that recovery guarantee too.
- Existing on-disk duplicate case variants and incomplete initial EA LIST results
  remain outside the sequential-input case-map repair.
- **Known backend correctness issue:** memfs LOOKUP/READDIR of `..` can invert
  ordinary parent-to-child lookup/remove lock ordering independently of rename.
  The directory rename repair does not solve this general namespace issue.
- Protocol controls, long-lived notifications, and some session/tree changes are
  intentional boundaries. No test may simulate rollback by rejecting finish
  after actual namespace/data mutation on the current nontransactional backends.

## Wrap-up dependency assessment

The remaining work is concentrated in shared state and lifetime contracts:

1. Extend constructor admission to broader replacement and directory moves.
   The native bounded occupied-target path is converted, but descendant and
   cross-share-view path updates and legacy delayed publication still need work.
2. Build attempt-private cache/durable/DOC state. Client-wide lease-key admission
   and safe cached CREATE I/O suffixes are implemented. Removing the remaining
   caching CREATE/CLOSE boundaries needs grant decisions in wire order, immutable
   CREATE reply snapshots, and final surviving membership published at acceptance.
   A closed private slot currently suppresses grant publication and replies NONE;
   that loses an existing same-key grant's mode/version/epoch. A reply-only cache
   decision or cache journal must solve this without installing speculative live
   grants, including coordination with concurrent joins, upgrades and breaks.
3. Complete compound CLOSE for complex stream/base deletion;
   broaden cancellation and deleted-session resource teardown. Notification
   cleanup and sibling-channel LOGOFF watch cleanup are implemented.
4. Extend SET_REPARSE canonical identity migration to stateful opens and make
   following commands consume a private migrated identity. The first slice is a
   standalone compound and intentionally excludes those cases.
5. Supply typed stream rename and broaden strict backend capabilities. Projected
   SMB locks are not an active missing mode under the POSIX-only projection policy.
6. Complete persistent recovery/repair. The legacy RANGE representation and
   every producer are now removed, so in-process ownership migration is no longer
   required. This does not implement cold lock recovery. Canonical owner identity
   and ParentLeaseKey plumbing are implemented.

Protocol-only commands, IPC pipes, session/tree changes and long-lived notify
waits need an explicit boundary policy rather than artificial filesystem work.
The runtime also splits across effective session/tree changes, operation capacity,
and waits which could deadlock against held DOC fences. Removing a safety split
requires a replacement lifetime/ordering contract, not deleting an eligibility
check. Completion should be judged by remaining valid-request fallbacks and
unnecessary wire-to-VFS splits, not by raw standalone-call counts.

Frontend completion does not require implementing backend transactions in this
pass. End-to-end retry after actual mutation cannot be claimed until a backend
can roll it back; current held/rejected-finish tests deliberately avoid that claim.

## Validation

Wave21 full Debug+ASan build3 passes. All 52 selected CTest entries passed on
that build: 31 focused VFS/SMB checks, four extended checks, 12 cross-frontend
regressions, and five SMB profiles with 94 traces each (470 trace replays).
ASAN_OPTIONS=detect_leaks=0 was used. No production changes followed build3.
No model expectations or trace files were changed in this wave.

Independent Samba `smb2.lock` also passes against memfs in a temporary network
namespace: 23 cases passed, three client-selected skips (the Windows 2008
NONE-lock bug, broken-Windows replay behavior, and a CTDB-cluster-only test).
Cancellation by close/tree disconnect/logoff, async locks, range geometry,
zero-length locks, stacking, partial multi-unlock, durable/multichannel replay,
and the open/lock deadlock check all pass.

New wire coverage includes:

- Canonical range identity and handle anchors for single/multiple LOCK,
  same-key lease peer I/O, foreign-client conflict and cross-client durable
  BATCH reconnect retaining ranges.
- Forced initial batch-allocation refusal: failed LOCK acquires nothing,
  failed UNLOCK releases nothing, a related QUERY inherits the valid FileId,
  and durable LockSequence retry succeeds without a cached resource error.
- Atomic rollback after multi-acquire conflict, accepted prefixes on missing
  or malformed multi-unlock elements, exact zero-length/final-byte unlock,
  and finish retry of journaled claim changes.
- QUERY/CLOSE coalescing while ranges remain held, with resource-only finish
  rejection/retry and successful foreign I/O after retirement.

The legacy entry/list implementation and its completion queue are gone. The
prior import proposal is unnecessary because all producers were removed and
there is no persisted legacy representation to recover. Final review preserved
FileId inheritance on allocation failure and added a direct regression.
There was no finish rejection after actual namespace/data/KV mutation.
Root and ext/specs whitespace checks pass.

Logs: `/tmp/chimera-smb-wave21-{build3,tests2,extended2,cross2,mbt2,torture2}.log`.
Earlier wave18–20 validation and model corrections remain recorded in MEMORY.md.

Current slice reports: [LOCK/UNLOCK completion](smb-compound-lock-completion.md),
[cached CREATE suffixes](smb-caching-create-private-suffix.md),
[directory rename](smb-bounded-directory-rename.md),
[notification admission/lifetime](smb-notify-close-admission.md),
[native CLOSE cancellation](smb-native-close-cancellation.md), and
[legacy parent-lease actors](smb-parent-lease-actor-plumbing.md).
Earlier foundations: [constructor admission/replacement](smb-replacement-constructor-admission.md),
[client lease keys](smb-client-lease-key-admission.md),
[reparse identity migration](smb-set-reparse-compound-design.md), and
[persistent CREATE proof](smb-persistent-create-truthfulness.md).
