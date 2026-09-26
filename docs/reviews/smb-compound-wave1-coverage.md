# SMB compound conversion: independent Wave 1 coverage and handoff

2026-09-23. Source inventory and new fixture implementation; integration owner
builds and runs tests. This document does not claim the full SMB conversion is
complete. The Wave 1 runtime is developed concurrently and covers existing-open
QUERY_INFO/FileBasicInformation, ordinary-channel READ, and FLUSH only.

## A coverage distinction that changes the acceptance gate

`src/server/smb/tests/quint/smb2_mbt_replay.c:do_message` iterates the model's
command results and invokes `dispatch_cmd` separately for each command. Those
commands perform individual wire round trips. It does not build a packet with
multiple `NextCommand`-linked SMB headers. Model corpus success remains valuable
for individual command behavior, but cannot establish wire-to-VFS coalescing,
related-header inheritance, one aggregate response, or compound retry behavior.

The new `src/server/smb/tests/quint/smb2_compound_probe.c` constructs actual
multi-command packets and checks each response header, MessageId, status, chain
offset and data payload. It counts VFS submissions through executable symbol
interposition. The initial probe intentionally uses the plain transport profile;
it is not signed/encrypted compound coverage.

Implemented fixture cases, awaiting root-owned validation:

| Case | Required observation |
| --- | --- |
| Existing-open QUERY_INFO + READ + FLUSH | One VFS submission, three successful responses, exact READ bytes |
| QUERY_INFO + invalid explicit FileId + READ | Middle FILE_CLOSED preserved; independent final READ succeeds |
| Missing first FileId + related explicit valid FileId + related placeholder | All commands preserve FILE_CLOSED; a skipped command cannot seed inheritance |
| QUERY_INFO + related READ + related READ | Successful non-CREATE command seeds inherited FileId |
| Successful QUERY_INFO + failed related QUERY_INFO + related READ | An established inherited handle survives a subsequent command failure |
| One all-ones half of a related FileId | The complete previous FileId is inherited, including the other half |
| Held readonly finish followed by two rejected finishes | ECHO barriers make progress without publishing the compound response; exactly one final response after accepted retry |
| Readonly finish rejection through retry exhaustion | Every affected command reports INTERNAL_ERROR; no stale successful READ payload |
| READ operation EAGAIN + QUERY_INFO | Ordinary operation failure executes once; later independent group succeeds |
| Own exclusive lock + owner READ + other-open READ + QUERY_INFO | Owner self-exemption survives retained synthetic handles and finish retries; another open gets FILE_LOCK_CONFLICT; later QUERY executes |
| Exact unlock + other-open READ | Releasing the original range restores the other open's READ access |
| Held finish + TREE_DISCONNECT + TREE_CONNECT on the same connection | Old executed results retain their tree identity and data; replacement-tree I/O works before and after old acceptance; no duplicate response |

The held-finish injector returns asynchronously and is released by a one-shot
polling timer on the compound's owning VFS event loop. Executable interposition
of compound allocation records that loop; no production SMB symbol is exported
for the fixture. The timer waits for an explicit atomic client release flag.
ECHO barriers establish absence of early application responses before release;
no elapsed-time assumption determines acceptance.
Finish rejection is limited to readonly QUERY_INFO/READ runs. FLUSH is tested
only with accepted finish. This does not simulate filesystem rollback.

The new probe needs the ordinary SMB probe libraries, `dl`, and executable symbol
export (`ENABLE_EXPORTS` or `-Wl,--export-dynamic`). The coordinator owns CMake.

## Behavior the runtime must preserve

The current dispatcher does not globally poison a wire compound on an ordinary
command error. Related requests inherit context; unrelated requests clear the
previous related context. A failed CREATE that never established an open passes
its error to related followers. In contrast, failure after an established handle
does not automatically prevent later related commands using that handle.

`chimera_smb_open_file_resolve` has more effects than a lookup: it pins the open,
clears durable replay eligibility on a nonreplay access, and seeds saved FileId.
Its all-ones placeholder rule inherits the whole FileId if either half is
all-ones. Calling this helper unmodified from a replayable prepare callback would
publish replay state too early. The new runtime must separate pinning, private
inheritance and accepted publication.

Further real-wire coverage still needed after the initial slice:

- Failed CREATE followed by related operations, including a successful unrelated
  request after the failed related chain.
- CREATE/use/CLOSE of a provisional open in one VFS compound; cross-command
  staged position, rename, disposition and lock visibility.
- Mixed supported/legacy boundaries, explicit multiple opens, context/tree
  changes, and exhausting operation/reply budgets.
- Signed, encrypted and compressed compound packets. Existing standalone wire
  probes are not a substitute for signing each linked response correctly.
- Finish retry with READ minimum-count/EOF errors, failed first groups, payload
  retention, cancellation, disconnect and parked coordination.
- Durable replay, directory cursors, notifications and RDMA visibility before
  accepted finish. The initial probe observes network responses, not all shared
  server state.

## Remaining conversion inventory by owner

| Area | Files and remaining filesystem work | Publication hazards |
| --- | --- | --- |
| Basic I/O | `smb_proc_write.c`, remaining `smb_proc_read.c`, `smb_proc_flush.c` cases | WRITE updates attributes after data; channel sequence/replay state, read position, RDMA result writes and payload refs must follow acceptance |
| Enumeration | `smb_proc_query_directory.c` | Restart/reopen/index processing resets the public position before I/O; readdir callbacks advance position and output incrementally |
| Information/security/EA | `smb_proc_query_info.c`, `smb_proc_set_info.c`, `smb_proc_security.c`, `smb_ea.h` | Multiple queries/mutations and result marshalling; accepted notifications; disposition mutates public file/open state and invokes recall |
| Rename | `smb_proc_set_info_rename.c` | Namespace checks, opens and mutation must compose; open-name and delete-on-close identity updates cannot be early |
| CREATE | `smb_proc_create.c`, `smb_sharemode.c`, `smb_durable.c` | Provisional share/caching claims, destructive-open ordering, public open hash, grant members, durable/replay registries and response contexts |
| CLOSE/deletion | `smb_proc_close.c`, `smb_doc_stream.c` | CLOSE performs attributes, conditional identity-matched removal, lock/share/grant retirement and stream deletion; these cannot disappear into accepted cleanup |
| Locks | `smb_proc_lock.c` | Exact stacked ranges, all-or-nothing lock batches, prefix-preserving unlock batches, pending/cancel/grant, lock-sequence replay and lease invalidation |
| Streams | `smb_doc_stream.c`, stream portions of CREATE/QUERY_INFO | Missing typed stream open/list/remove forms; base-file delete reservation and stream-holder accounting |
| Reparse | `smb_proc_reparse.c` | Remove/mknod/open/readlink/getattr sequences replace objects and swap live handles |
| Sparse/ranges | `smb_proc_sparse.c`, `smb_proc_copychunk.c`, `smb_proc_copyoffload.c`, `smb_proc_ioctl.c` | Allocate/seek/copy/clone loops, source/destination claims, resume/offload tokens, chunk counters and partial results |
| Async/teardown | `smb_proc_cancel.c`, `smb_proc_change_notify.c`, `smb_proc_oplock_break.c`, `smb_notify.c`, session/tree/disconnect/durable cleanup | Pending ownership and exact terminal callback; recall acknowledgements must progress independently; mandatory teardown cannot be replayed as ordinary tentative work |

Named pipes and session/negotiate/transport control are not ordinary filesystem
conversion omissions. Long-lived notification registration should not hold a
backend filesystem transaction open for the subscription lifetime. Nevertheless,
cleanup triggered by those paths must be audited for hidden file mutations.

## Wave 2 shared-claim interface requirements

The existing POSIX/FUSE typed lock API explicitly requires a dedicated compound
with exactly one lock operation. Its range journal applies POSIX interval
replacement/coalescing semantics. SMB cannot simply call that API repeatedly:

1. **Exact range records and multiplicity.** SMB keeps independently acquired
   range records; identical/shared ranges can stack and unlock consumes a
   matching acquisition. Exclusive acquisition conflicts with the same handle's
   overlapping locks, while shared acquisition has different same-handle rules.
   Zero-length locks and ranges ending exactly at 2^64 remain valid. A normalized
   POSIX union loses information needed for RANGE_NOT_LOCKED and replay.
2. **Group-scoped batch outcome.** Lock acquisition validates the batch and
   rolls back its newly acquired prefix on conflict. Unlock preserves the
   successful removal prefix when a later element fails. Ordinary command error
   is not equivalent to aborting the entire VFS attempt. Provide a journal
   savepoint/rollback scope or equivalent explicit batch outcome, separate from
   operation-group error continuation.
3. **Attempt-private overlay.** Later commands in the same compound must observe
   tentative acquires/releases/share changes. Other requests must not consume
   them as accepted state. Provisional holds must still prevent conflicting
   admission and must be associated with the attempt for self-exemption.
4. **Precommit reservation and infallible publication.** Preallocate range nodes,
   grant members and deletion records before backend finish. Acquire/validate
   admission holds before destructive filesystem work. After accepted backend
   finish, publishing to shared claim/open tables must not introduce an
   allocation, stale-generation check or conflict that can newly fail.
5. **Share/grant composition.** Represent requested access/deny rights, held
   post-truncate rights, base-file stream deletion rights and grant membership
   explicitly. Cache lease recalls are external coordination; retry reruns pure
   validity checks without duplicating break messages or grant membership.
6. **Deletion ownership.** Stage disposition, last-holder selection, credential,
   parent/name and full object identity together. Perform identity-matched
   removal inside execution, not a fallible callback after transaction commit.
   Abort releases tentative ownership; close/disconnect generation cutoffs must
   prevent a pending attempt from publishing into a retired open.
7. **Backend projection restriction.** Legacy backend lock mutation cannot be
   optimistically retried after execution. Preserve the current reject-before-
   projection restriction or introduce explicit backend rollback capability;
   do not silently remove the typed-lock guard merely to allow mixed commands.
8. **Cancellation ownership.** Waiting claims retain compound/open lifetimes;
   cancellation settles the reservation exactly once on the owning worker.
   Retry resets attempt state, while mandatory disconnect cleanup retires it
   permanently and is not undone by a finish failure.

A practical first shared-claim slice is an exact local SMB range journal with
explicit batch mode (atomic acquire / prefix unlock), group-local checkpoints,
whole-attempt reset and prevalidated accepted publication. Follow it with share
reservation and deletion ownership overlays. Do not change existing POSIX range
semantics to accommodate SMB's acquisition multiplicity.

## Metadata conversion handoff

The metadata descriptor pass coalesces ordinary FILE/FILESYSTEM QUERY_INFO,
FILE_FULL_EA enumeration and mutations, basic attributes, allocation/EOF,
position/mode, and security descriptor query/set. Earlier QUERY responses keep
an owned scalar/path snapshot through wire emission so later position changes
or handle retirement cannot rewrite their results. EA loops dynamically append
operations within their command group. Security identity resolution uses an
explicit cache coordination operation that reruns on retry; descriptor decoding
and encoding remain replayable. Parent notifications and sticky write-time
policy publish only after accepted finish.

`tests/quint/smb2_metadata_compound_probe.c` sends real wire chains and asserts
one VFS submission, including position/query/read ordering, BASIC snapshots,
EOF/allocation sizes, and EA SET/QUERY followed by security QUERY. It does not
inject rejection after executed filesystem mutations. Root owns registration,
build and test results; this handoff is not a claim those checks have passed.

Namespace rename/hardlink, disposition and replacement reparse retain explicit
boundaries pending shared admission/delete ownership and live-handle replacement
publication. Stream enumeration and the ordinary filesystem IOCTL families are
assigned to the following pass. Legacy helper entry points remain for explicit
fallbacks and CREATE's existing helper uses; their mere textual presence is not
proof an ordinary converted wire command still takes that path.

## Stream and IOCTL conversion handoff

The following ordinary wire commands now route through common SMB descriptors
and share a VFS submission with eligible neighboring commands:

| Family | Compound execution |
| --- | --- |
| FILE_STREAM_INFORMATION | Typed LIST_STREAMS against the base object; GETATTR synthesis when streams are unavailable |
| GET_REPARSE_POINT | GETATTR, followed by READLINK for symbolic links; request-private response encoding |
| SET_SPARSE | GETATTR and SETATTR preserving other DOS flags |
| SET_ZERO_DATA | Typed ALLOCATE(DEALLOCATE), with pure mandatory range-lock checks |
| QUERY_ALLOCATED_RANGES | Dynamic alternating SEEK_DATA/SEEK_HOLE operations within the same command group |
| COPYCHUNK / COPYCHUNK_WRITE | Pinned source participant, source GETATTR, bounded dynamic COPY_RANGE operations; partial progress and limit-error bodies preserved |
| DUPLICATE_EXTENTS | Destination/source GETATTR, CLONE_RANGE and COPY_RANGE fallback where supported by prior behavior |
| OFFLOAD_READ / OFFLOAD_WRITE | GETATTR plus private token encoding; pinned source and clone/copy for token consumption |
| GET/SET_INTEGRITY_INFORMATION | Attempt-private per-open overlay; each query snapshots its own execution point |
| REQUEST_RESUME_KEY, CREATE_OR_GET_OBJECT_ID, FILE_LEVEL_TRIM, ENUMERATE_SNAPSHOTS | Pure checkpoint operations retaining protocol checks and response values |

Copy source participants do not change the destination's related-FileId
inheritance. Every data operation checks both mandatory ranges against the
compound's claim overlay. Actors preserve the original open identity even when
the operation uses a retained synthetic VFS handle. Native owned-copy/clone
cache admission is supplied by the corresponding VFS changes, rather than a
namespace recall that would apply the wrong cache-retention policy.

The stream delete-on-close runner now executes base open, stream open, identity
verification and stream removal in one typed compound. Its mandatory retirement
decision remains outside retry, and it still constitutes a separate cleanup
compound after legacy CLOSE/teardown. This is not a claim that ordinary stream
CLOSE has joined the original wire compound.

`tests/quint/smb2_ioctl_compound_probe.c` exercises actual wire chains and one
submission per chain for copies, offload, sparse/zero/ranges, integrity response
snapshots and matching base/stream enumeration. Root owns its build and test
results. Initial stream failures found by root were corrected: LIST_STREAMS
returns `buffer_count`, and OPEN_CURRENT needs GETHANDLE before its reference
can be used as a later handle producer.

### Remaining concrete boundaries in these owned handlers

- **SET_REPARSE_POINT:** replacing an inode also replaces the live open's VFS
  handle and delete-on-close identity. Share/access/cache claims must migrate to
  the replacement identity. This needs a provisional replacement slot visible
  to later commands, preallocated claim/delete ownership, and infallible
  accepted publication. The legacy path still performs direct filesystem calls;
  invoking its rebind callback after a compound would leave split state.
- **LMR_REQUEST_RESILIENCY:** no ordinary filesystem work, but its durable table
  insertion allocates an entry and hash storage. Coalescing needs a preallocated
  durable reservation and per-open resilient/timeout overlay. The current
  explicit control boundary remains.
- **SET_INFO rename, hardlink and disposition:** namespace admission, recalls,
  identity-matched deletion and public name/delete ownership are still tied to
  the legacy shared-state machinery. Typed stream primitives alone do not
  replace that journal contract.
- **CREATE helpers and stream holder retirement:** existing CREATE EA/stream
  helpers and public last-holder bookkeeping remain separate from the new
  ordinary descriptors. Conversion completeness depends on the CREATE/CLOSE
  owner, not on the absence of textual per-op calls in these utility helpers.
- **Protocol control:** pipe transceive, negotiate validation and network
  interface queries remain protocol/transport control paths, without a backend
  filesystem transaction to combine.

The independent lifetime pass also fixed accepted EA response ownership when a
connection drops before wire emission, and preserved accepted metadata
notifications for other watchers even if the initiating connection disconnects.
Per-open state publication remains subject to the core lifecycle checks.

## Follow-up: provisional CREATE audit and first namespace slice

The independent audit found that CREATE treated OPEN_CURRENT as a result-handle
producer and an attribute query. OPEN_CURRENT provides neither: its handle stays
in the cursor until GETHANDLE, and attributes need GETATTR. This caused empty-name
root stat-opens to dereference a null handle and made nested CREATE retain the
share-root parent identity. The CREATE owner added explicit GETHANDLE producers
and GETATTR for the root case; root owns regression validation.

Non-replacing FILE_LINK_INFORMATION now joins the common SET_INFO descriptor.
It saves the source, resolves and opens the destination parent, and executes
LINK inside the same VFS compound. The LINK primitive now accepts namespace
options and the original source actor identity. A new link_at_flags entry point
suppresses tentative FILE_ADDED notifications while the ordinary link_at entry
point preserves existing behavior. The frontend copies destination parent/name
and emits FILE_ADDED only after accepted finish, preserving ParentLeaseKey
self-exemption. Lookup errors and name collisions remain per-command failures;
independent following commands still run.

The metadata real-wire probe now exercises root and nested hardlinks with
interleaved link-count queries, reads through the alias, and verifies collision
and missing-parent failures followed by a successful query in one submission.
No executed namespace mutation is followed by injected finish rejection.
Root owns builds and execution; source handoff alone is not a passing result.

ReplaceIfExists hardlinks remain an explicit boundary. The exact range journal
does not yet reserve a replacement target's deny-delete admission and identity.
A snapshot-only share check would not protect the mutation against concurrent
opens or namespace replacement. Other remaining namespace work includes rename,
disposition, ordinary existing-open CLOSE and SET_REPARSE replacement. The LOCK
mapper was intentionally disabled pending canonical owner/retirement integration
at this review point; the presence of its descriptor was not wire conversion.

The metadata and IOCTL fixtures additionally assert that the submitted compound's
execution-group count equals the wire command count. One VFS submission alone
can give a false positive when a legacy prefix or suffix surrounds a single
converted command. Payload/status assertions and this group-count check are
both required for coalescing coverage.

## Namespace prerequisite handoff (not a RENAME conversion)

`src/server/smb/smb_namespace.[ch]` supplies a standalone namespace participant
and attempt-journal helper. The first production integration now preallocates
private CREATE slots, attaches accepted opens, detaches before pool reuse, and
uses independent membership alongside pending ACCESS admissions for ordinary
and stream DOC discovery. Surviving durable opens retain membership across
rehome. Final-reference teardown runs outside tree bucket locks. Production edit
acquisition, legacy mutation fencing, and compound RENAME dispatch remain
disabled; enabling only the rename descriptor would be unsafe.

The guard set is acquired atomically before execution; conflicting acquisition
holds no partial set. Guards last through retry and accepted publication. Path
changes reserve fixed event storage before the filesystem mutation, update only
the private overlay on operation success, and publish only after accepted finish.
Guards are keyed by file identity, while overlays and events distinguish each
hardlink by parent FH/name and share-root identity. Peer opens on another
hardlink are not repathed. A different share-root view is rejected before mutation
until an explicit path translation is supplied; directory descendant repathing
also remains outside the first slice.

Participants hold fixed path/DOC metadata independently of ACCESS claims. The
generic helper supports retained contexts; production membership is weak and
requires detach before physical recycle under the registry lock, avoiding a
permanent open-reference cycle. Production DOC discovery reads live open fields
under file locks; stored path/DOC snapshots are not yet authoritative after
legacy rename or disposition. The eventual guarded path must preserve closing
peers, hold a guard across DOC/path-consuming I/O, and defer physical recycle
until conflicting edits drain. These guard/finalization changes are prerequisites
for dispatch, not completed behavior.
The registry lock precedes file/cache locks; callbacks under it must not recurse,
allocate or perform I/O. The standalone helper fixture covers contention,
all-or-none acquisition, close during a reservation, rejected-attempt reset,
multiple peers, preserved DOC credentials, sequential renames, hardlink aliases,
and explicit cross-share rejection. Root owns registration and execution.

Required integration before dispatch can be enabled:

1. Validate production regular/stream/base membership and teardown under wire
   lifecycle tests, including reconnect/recovery and provisional retries.
2. Add guard-aware retirement for CLOSE, disconnect, TREE_DISCONNECT, AppInstance,
   durable expiry and failure unwinds. Cache acknowledgments and cache/share
   drain must remain independently runnable.
3. Make fixed participant path/DOC snapshots authoritative after every legacy
   mutation; current discovery intentionally uses live fields until this exists.
4. Make legacy rename, disposition, reparse replacement, destructive CREATE and
   every path-consuming cleanup obey the same exclusion. A global legacy guard
   is available for unresolved identities, but must not be acquired after holding
   a conflicting partial set or ACCESS edit lease.
5. Route later QUERY/WRITE/SET/CLOSE path reads through the private overlay and
   preserve per-command snapshots. Remove or preallocate the obsolete allocating
   legacy name-sharemode re-key operation; ordinary CREATE no longer registers it.
6. Preserve destination-parent lease probes and source recall identity with
   typed coordination; defer rename notifications until acceptance.

The cache's `set_delete_on_close` setter itself copies fixed buffers and does
not allocate. Separate shared pending-delete metadata does have ownership and
allocation concerns. The current VFS/backend rename interface has no atomic
NOREPLACE option: the legacy existence check followed by overwrite-capable
rename can race another creator. This existing gap is not solved by a frontend
path journal and must not be described as atomic no-replace behavior.

The public-CLOSE slice has a separate DOC-only admission fence, coupled to an
ACCESS insertion fence. Batch preflight acquires the complete known file set
before executing any VFS operation; contention releases the entire set and waits
on the owning event loop with no partial holds. Blocking LOCK and baseline CLOSE
paths with separate fences must be split from these batches. Multiple CLOSE
commands from one batch share identity. Disposition checks the fence under
registry/file/cache lock order; ordinary reads and cache acknowledgments remain
independent. Teardown must drain cache rights before waiting for guarded DOC/path
retirement, so a lease holder can satisfy recall by closing.

Reparse replacement is another explicit admission boundary: its legacy rebind
changes `open->handle` to the new inode without migrating the canonical ACCESS
file state or namespace identity. Public CLOSE admission must reject a current
handle/file-state FH mismatch until that migration is implemented.

## Disposition and DOC CLOSE implementation gate

`smb_doc_compound.[ch]` now contains the ordinary non-POSIX, nonstream disposition
and regular-file DOC CLOSE implementation. The runtime lazily allocates its batch
context only for participating commands. Intent and file-level pending state are
private across callbacks/retries; QUERY uses the staged value. A closing opener
copies the deleting opener's original credentials and namespace identity, and
conditional typed REMOVE requires backend match-FH capability. Parent lease
exemption belongs only to a closer carrying its own DOC intent. Notifications
publish after acceptance. Failed unlink still closes the SMB handle and reports
the legacy error; a strict identity mismatch is a distinct successful no-op.

Runtime activation now uses the VFS ACCESS insertion gate and static cancellation
scope. The gate blocks new and queued ACCESS admissions through last-peer
selection and accepted publication, while allowing same-batch admissions under a
stable private cookie. The mandatory CLOSE suffix is fully preallocated; after
successful atomic retirement, cancellation drains that suffix before returning.
No fallible dynamic append occurs after the retirement commit point.

The first integrated gate passed the new DOC wire probe but exposed a concurrent
CLOSE regression in the existing info probe: fail-fast fence conflicts returned
FILE_NOT_AVAILABLE. Source now implements whole-set preflight waiting. A second
review found legacy release_doc bypassed active fences and could overwrite staged
pending state; guarded legacy close and finalizer cleanup now serialize those
paths, with cache-rights drain before waiting. The focused gate below validates
the concurrency fix.

Legacy RENAME and SET_REPARSE now retain DOC fences across their callbacks and
release them through generic request completion; they remain legacy compounds
boundaries. The new `smb2_doc_compound_probe.c` checks real wire command counts,
cross-handle pending visibility/clearing, last close deletion, CREATE-DOC peer
ordering, shared cached-handle DOC clearing, waiting setters, disconnect of an
unexecuted waiting batch, and metadata-only retry/exhaustion. It deliberately
cannot inject finish rejection after REMOVE or CLOSE. The waiting/disconnect
extensions pass the integrated focused gate below.

### Validated DOC concurrency and CREATE mode distinction

The integrated focused gate now passes 25/25, including simultaneous DOC CLOSE,
whole-set preflight unwind, a waiting setter, and disconnect of a waiting batch.
Legacy CLOSE/finalizer cleanup releases cache rights before waiting for DOC/path
admission, preserves namespace participants through guarded retirement, and
avoids the former mixed-path pending-state overwrite.

CREATE's per-open FILE_DELETE_ON_CLOSE mode is distinct from the link's current
delete disposition. Clearing FileDispositionInformation does not cancel that
CREATE mode; CLOSE marks the link again. The logical open's identity and saved
credentials now supply its CREATE intent independently of VFS cache pointer
sharing. This follows [MS-FSA FileDispositionInformation](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/386d9ec5-e0f6-4853-b175-c05be01419e0),
[MS-FSA close processing](https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-fsa/d142c93a-72bc-4b05-9d96-8e00371c3308),
and [FILE_DISPOSITION_INFO](https://learn.microsoft.com/en-us/windows/win32/api/winbase/ns-winbase-file_disposition_info).
Two existing namespace MBT traces modeled own-clear as cancelling CREATE mode;
the new ordinary-file behavior exposed that model discrepancy. The model was
corrected, two model tests were added, and all five SMB profiles passed the
regenerated traces. Later ordinary data OPEN coverage exposed a distinct harness
issue: it consumed an ACK before the final asynchronous CREATE reply; the harness
now waits for that final reply. Stream ordinary-clear mode preservation and its
new regression also pass the focused gate.

### Bounded regular-file RENAME source checkpoint

The next descriptor implements existing regular files renamed within the share
root, without replacement. It shares the DOC batch's complete-file preflight
and ACCESS gate. Preallocated private path records resolve peer names by source
parent/name and preserve independent hardlink aliases and each share's path
prefix. QUERY name, SET metadata notifications and DOC intent consume this
private path. Accepted publication updates peers and matching cache DOC paths,
then emits rename notifications. Directory descendants, cross-directory moves,
stream holders and provisional source handles remain explicit boundaries.

The typed backend request requires both atomic NOREPLACE and matched source FH,
plus NO_NOTIFY until frontend acceptance. Memfs supports this strict combination;
Linux native NOREPLACE alone is insufficient for this frontend slice. The wire
fixture checks multiple renames and intervening name snapshots, independent
hardlink aliases, destination collision and later-command continuation, exact
same-link no-op, and renamed CREATE-DOC intent with a new inode at the old name.
The mapper is enabled. The build13 real-wire rename fixture and namespace
standalone test pass, including exact VFS group counts for converted wire
commands and the held-producer acceptance/retry cases below.

Named-stream DOC cleanup still verifies stream identity with OPEN_STREAM before
a separate REMOVE_STREAM. A compound alone does not make that check/use atomic:
strict matched REMOVE_STREAM is required to prevent deleting a rebound stream
name. Existing stream cleanup must not be described as identity-safe solely
because both operations now belong to a compound.

### Pending OPEN identity and legacy rename publication

The producer now registers an invisible name token before OPEN and binds its
result identity in explicit coordination afterward. Accepted external renames
update only that token under the namespace registry lock, never another attempt's
private open. Producer acceptance reads the token; rejection/retry preserves an
external update for the same original link/inode and discards it if the original
spelling opens a replacement inode. Pending tokens are excluded from public DOC
scans and detach without waiting on a namespace guard. The new wire fixture holds
a metadata OPEN before acceptance, performs two external renames, reuses the old
name, and checks both acceptance and a single readonly finish rejection by name
and inode identity.

Legacy rename publication also matches the old parent/name instead of repathing
all same-inode hardlink aliases. It updates namespace members and admitted legacy
opens under registry/file locks, preserves matching cached DOC credentials, and
carries path changes to invisible producers. Same-parent moves preserve each
share's prefix; cross-parent moves have exact full paths when both opens use the
same share-root view. Cross-parent moves across different share-root views still
need explicit path translation: parent/name identity is updated for safe DOC,
but the share-relative directory portion remains a known legacy limitation.
Directory descendant repathing and strict matched named-stream removal remain
separate unfinished namespace work. The build13 rename wire and namespace unit gates pass. The latter includes a
cross-parent, same-view pending-token move and hardlink-alias exclusion. Broader
legacy cross-share/directory behavior is not covered by those passing gates.

### Nested and cross-directory RENAME follow-up source checkpoint

The ordinary existing-file mapper now resolves destination parents with typed
LOOKUP_PATH, validates that the result is a directory, and saves the source
parent from the command's execution-time private path. Strict backend source-FH
matching and NOREPLACE remain mandatory. The path journal carries complete old
and new identities, so same-view cross-parent moves update peer paths, cached DOC
parent/name, pending producers, and later QUERY/CLOSE inputs. Accepted publication
emits separate old-name/new-name notifications for different parents.

A preallocated namespace view reservation prevents a new invisible producer from
introducing an unsupported foreign share view after validation. It is acquired
only in explicit coordination, checks existing pending producers, and includes
all earlier names in the same private rename chain. An unsupported foreign view
causes pre-mutation redispatch to the legacy boundary. Reservations do not wait
and are released before terminal ACCESS admission pumping. This does not claim
that legacy cross-share path translation or directory descendant repathing has
been solved.

The expanded wire fixture covers nested same-parent chains, cross-directory
moves back to the root, a missing target parent followed by independent success,
hardlink aliases, moved CREATE-DOC intent and old-name reuse, and held producer
acceptance/retry across a directory move. Namespace units cover admission and
rejection of late differing-view producers. This latest extension is source-ready
and awaits the root integration gate; the build13 passing gate predates it.

### Provisional-source RENAME follow-up source checkpoint

A regular-file CREATE or OPEN can now feed a later nonreplacing RENAME in the
same wire/VFS compound. Construction reserves an unresolved DOC/path slot keyed
by the producing open state. Its explicit execution coordination binds the
actual result FH to a retained file state and tries namespace/ACCESS admission
without waiting. Contention accepts the earlier prefix and redispatches the
untouched rename. Resolved type/capability checks retain unsupported cases as
boundaries, including directory sources and backends without strict source-FH
matching. Source parent and share-root inputs come from the producer's private
namespace snapshot, including a cold tree's unpublished root resolution.

Unlike fixed preflight fences, these late-acquired attempt resources are released
on reset before producer cache recalls can run again. This reset work only drops
coordination reservations/references; it does not publish a tentative path. The
runtime permits an earlier waiting producer but separates subsequent blocking
OPEN/LOCK commands after a dynamic rename admission. Ordinary preflight DOC
boundaries remain in place. Stream/base CLOSE and waiter boundaries are also
symmetric, avoiding a retained CLOSE fence across a later cache/lock wait.

The wire fixture adds cold CREATE-to-rename, fresh CREATE and existing data OPEN
followed by cross-directory rename/query/close, collision continuation, and a
readonly retry of OPEN plus a same-name skipped rename. It checks one submission
and one VFS command group per wire command. Finish rejection is injected only
when the rename was skipped and no filesystem mutation executed. The root wave8 integration gate passed; see
[smb-compound-current-status.md](smb-compound-current-status.md) for final test
results. Directory-ancestor movement and legacy cross-share full-path translation
remain explicit limitations.

## Wave9 SET_REPARSE identity audit and bounded hardening

SET_REPARSE_POINT is still an explicit compound boundary. Review found that its
legacy inode replacement swaps the VFS handle but leaves canonical ACCESS/file
state and namespace membership on the original inode. Safe conversion also
requires exclusive or versioned cross-request FileId identity admission; a
standalone compound alone cannot protect other consumers' borrowed claims.
The concrete prerequisite sequence and remaining success-path issues are in
[smb-set-reparse-compound-design.md](smb-set-reparse-compound-design.md).

This source checkpoint hardens the existing boundary: strict matched source
REMOVE refuses unsupported backends before mutation and preserves a different
object installed at the old name; unsupported NFS reparse types are rejected
before unlink; missing replacement FH/reopen failures return errors; logical
CLOSE cannot be undone by late handle rebinding. The registered IOCTL wire
fixture adds stale-name/content checks, unsupported-type failure plus independent
READ continuation, and missing-FH/reopen fault cases with explicit persisted
filesystem effects. This agent performed no builds/tests; root owns validation.

## Wave10 bounded oplock CLOSE source checkpoint

Ordinary existing-file CLOSE with a per-open legacy LEVEL_II, EXCLUSIVE, or
BATCH oplock now enters the native compound mapper when no DOC, stream,
durable/persistent, notify, or legacy-lock boundary applies. Canonical typed
ACCESS/RANGE retirement remains the existing private journal operation. The
builder retains the grant and file state without changing its membership,
rights, epoch, or break state. Pure retirement preparation rechecks the live
FileId and pinned grant identity.

This CLOSE terminates its VFS run: preceding eligible commands coalesce, but
subsequent commands begin only after acceptance and cache cleanup. This avoids
a later OPEN/WRITE waiting for cache rights that the same unfinished run must
release. Cache-bearing CLOSE uses late, nonblocking namespace coordination;
contention or newly discovered DOC intent defers the untouched CLOSE to the
legacy path, whose early cache revoke can satisfy an outstanding recall before
it waits for the namespace fence. It does not wait on a pre-submit DOC fence.

Accepted publication removes the winning FileId from lookup and detaches only
its grant member. After compound journal completion, descriptor release revokes
an empty grant, drains the closing open's cache ownership, and releases the
independent snapshot pin. A concurrent teardown that already won the logical
cutoff retains cleanup ownership. No CACHE journal/exclusion view is claimed;
CACHE stays conservative until acceptance.

Shared RqLs and directory leases remain explicit boundaries. Grant reference
counts include transient and ACCESS-lifetime pins, and a new same-key opener can
coalesce before attaching its SMB member. Empty-member-list revocation is not
sufficient to distinguish that pending opener. Their conversion needs atomic
member attachment or explicit pending-membership accounting, including a safe
last-holder protocol.

New sources `smb2_cache_close_compound_probe.c` and
`smb2_cache_close_inspect.c` verify real wire run/group counts, per-open oplock
variants, held-finish visibility, competing OPEN progress when accepted CLOSE
satisfies its break without an ACK, independent failure continuation, and the
shared-grant legacy boundary with surviving/last-member progress. Completed
CLOSE attempts are held then accepted; none are falsely rejected as though a
backend mutation had rolled back. Root owns registration, builds and tests.

## Wave12 shared-lease CLOSE source checkpoint

Ordinary regular-file RqLs lease CLOSE now uses the same terminal VFS run as
II/EX/BATCH CLOSE. Preceding metadata can coalesce; the run ends at CLOSE and
subsequent commands begin only after acceptance. Public FileId, grant members,
and cache rights stay intact until acceptance. Accepted removal detaches only
the closing member; after claim journals drain, empty grants lose their rights
and retained grant references drain. Surviving members keep their rights.
Directory leases, cached streams, durable/persistent handles, notify state,
legacy lock lists, and DOC combinations retain their previous boundaries.

The prior same-key CREATE race is closed by atomic VFS coalesce/acquire-member
APIs. A grant reference and fully initialized protocol member become visible
under the same file lock, including racing first acquisitions. CLOSE cannot
mistake an in-flight same-key attachment for an empty grant. Protocol membership
is independent of transient grant pins. Removing a member also reanchors the
grant's own-handle identity to a survivor, avoiding recycled-handle exemptions.

The existing cache-CLOSE wire fixture now holds acceptance with two shared lease
members, verifies their continued public membership, checks query/CLOSE coalescing
and suffix separation, and closes the final member while a peer waits on its
break. The inspection also checks the surviving grant's handle anchor. The VFS
ACCESS test pauses a joining thread immediately after either atomic acquisition
API, removes the old member, and verifies rights survive until the new member
leaves. Root combined validation passed: see smb-compound-current-status.md.

A separate accepted-publication helper supports preallocated opportunistic legacy
oplock CREATE: under the caller's file lock, it admits the requested mode or caps
to LEVEL_II/NONE without allocation, callbacks, recall, or waiter pumping. It never
retains an unadvertised RH-only legacy grant. Unit cases cover full BATCH,
LEVEL_II capping, and rejection without breaking a peer. The CREATE adapter owns
its protocol policy and publication ordering.
