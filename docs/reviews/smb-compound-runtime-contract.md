# SMB compound runtime contract

2026-09-24. Source handoff; the coordinator owns build and test acceptance.

The runtime coalesces each contiguous eligible SMB wire run into one VFS compound,
with an independent continue-on-error group per command. It supports normal and
RDMA READ/WRITE, FLUSH, directory enumeration, implemented QUERY/SET information
and security/EA classes, supported stream/reparse/copy/range IOCTL families, local
canonical LOCK/UNLOCK, ordinary disposition, bounded nonreplacing RENAME,
and the CREATE/CLOSE subsets described below.
Unsupported commands, named pipes, projected lock owners, parser errors, and
session/tree changes are explicit legacy boundaries. A failed FileId resolution
is an executable error group; it does not force a legacy fallback.

## Attempt and publication contract

Handler-owned descriptors in `smb_compound.h` supply immutable eligibility,
construction, replayable operation callbacks, accepted publication and cleanup.
`bound_eligible` gates capabilities that require a resolved/pinned handle.
`initialize` reserves command-private construction state. Builders may see a NULL
VFS handle for a preceding provisional CREATE; execution callbacks bind the actual
handle. `prepare`, `complete`, and `reset` may only change attempt-private state.
Dynamic dependent operations append to their current group without displacing
later command groups or invalidating physical operation indices.

An execution-discovered boundary is private control state, not a wire error.
The discovering operation must fail to skip its group tail; later group
checkpoints skip automatically. Accepted finish publishes only preceding groups,
then frees the VFS attempt, fences and pins before dispatching the boundary
command once through its legacy path. Rejected finish resets the cutoff and
re-evaluates discovery; exhausted or canceled finish never redispatches. Deferred
WRITE payloads and already-gathered RDMA input survive this transition. Consumer
descriptors release before their earlier private CREATE producer.

`gather_inputs` obtains RDMA input once before submission and retains it across
retries. Synchronous `publish` stages accepted replies and notifications without
fallible admission or filesystem dispatch. `publish_async` transfers accepted RDMA
output while the runtime retains result ownership; it may complete inline.
`release` runs at terminal completion after the VFS compound is freed and before
a legacy suffix executes. `reply_release` retains formatting snapshots and cleans
accepted reply buffers through actual wire formatting or transport drop.

Per-open overlays include position, mutable flags, channel sequence, replay-window
retirement, integrity settings and lock-sequence replay entries. Later commands
observe the same attempt overlay; only accepted dirty fields merge into live opens.
Private source participants for COPY operations retain independent open/handle pins
without altering target FileId inheritance. Related commands resolve inheritance
at execution time, preserving prior valid inheritance across ordinary failures.

Only finish EAGAIN enters the common bounded retry adapter (eight retries).
Operation failures keep preceding successful operations and continue later groups.
Rejected or exhausted finish publishes no replies, notifications or open overlays;
the affected commands receive INTERNAL_ERROR. Backend transaction rollback remains
a separate prerequisite for injecting finish rejection after filesystem mutations.

## Lifetime and cancellation

The runtime independently pins SMB opens and VFS handles. Synthetic handle copies
retain the original handle address as an identity-only claim actor so mandatory
locking self-exemption survives the copy. Open pins drop before a legacy suffix,
including CLOSE. Session/tree memory pins survive through wire formatting but do
not delay logical LOGOFF, TREE_DISCONNECT or disconnect cleanup. Generation checks
suppress output and later legacy dispatch on a disconnected/reused connection.
Accepted position/channel updates skip logically closed public opens.

Local LOCK uses canonical exact-range owners and the VFS journal, with explicit
legacy-compatible zero-length record semantics. Projected backends and mixed
legacy owner records remain whole-owner boundaries. Blocking waits are cancelable
without publishing tentative replay state. Generic asynchronous cancellation and
batch-wide tracing beyond the first dispatch span remain follow-up work.

## CREATE and CLOSE

Fresh regular and directory FILE_CREATE/FILE_OPEN_IF and ordinary FILE_OPEN use private open
slots. Root and nested directory metadata opens are included. Supported
contexts are MxAc, QFid, AlSi and SecD, with no requested caching grant. Cold share
root lookup runs inside the CREATE group and publishes its cached FH only after
acceptance. Cold persistent-CA registry recovery remains a legacy boundary.

A stable FileId and reply snapshot are allocated before submission. Parent lookup,
OPEN, attributes/access/type checks, and canonical RESERVE_ACCESS admission all
belong to the CREATE group. GETHANDLE captures owning results: OPEN_CURRENT alone
only changes the cursor and does not produce a handle or requested attributes.
Transient access retries and symlink error-payload recovery append inside the same
group. A successful reservation makes the slot available to later related commands.
PUTHANDLE_FROM preserves provisional ownership provenance through private CLOSE.

Data OPEN uses an actual data handle and explicit repeatable cache coordination:
OPEN_H recall precedes SHARE admission, OPEN_W follows it, and completion waits
for required acknowledgments. Retrying recomputes live cache input and uses the
claim state machine's idempotent invalidation; it does not repeat durable eviction.
Parked durable holders and complex failed sharing admission are discovered before
publication and become accepted-prefix legacy boundaries. Data OPEN runs are
separated from DOC preflight runs to avoid recalling a peer whose CLOSE waits for
those same fences. Invisible pending namespace tokens are registered before leaf
OPEN and bound before RESERVE, preserving externally accepted path changes through
provisional publication without exposing a FileId or touching another attempt.

Private CLOSE stages postquery attributes, atomic claim retirement and CLOSE in the same
compound. Accepted modified closes emit the deferred parent-directory modification
notice. CREATE followed by CLOSE never briefly exposes the open in a public table.
A surviving CREATE transfers its handle and canonical claim token only at accepted
publication. A prebuilt single-entry UTHASH table handles an empty bucket; occupied
bucket insertion temporarily disables expansion under its mutex, then restores
normal resizing. No fallible registry allocation occurs at publication.

Directory creation uses typed CREATE with notification suppression, followed by
OPEN_CURRENT, GETHANDLE and attributes. Accepted creation notifications describe
the successful namespace mutation even when a later admission step fails. No
finish-rejection test assumes filesystem mutation rollback on current backends.

Canonical ACCESS tokens own ordinary and stream-base SHARE claim storage. Legacy
admission, shrink, park and policy accesses use that storage. Each token owns a
file-state reference; the SMB open separately owns its share-file reference.
Provisional token aliases live only in attempt slots until accepted transfer.
Legacy CLOSE cuts off ACCESS and RANGE and polls retirement before DOC or response.
AppInstance force-close and parked durable purge/reap use an embedded one-shot
retirement timer, preserving claim anchors and the caller's final logical reference
until all other references and journal edits drain. Logical cutoff removes this
open's cache membership and revokes an empty grant's rights immediately, but retains
grant storage until journal anchors drain. Parked durable opens retain their
original tree memory/bucket-lock lifetime; reconnect waits for old-channel references
before changing tree ownership or reseeding refcounts.

Public CLOSE without caching, persistent/durable or stream state uses the same
compound; a resilient-only registration can be forgotten during publication.
Regular ordinary delete disposition and delete-on-close use a shared per-file
attempt overlay and a static matched-remove tail. All known DOC namespace and
ACCESS-insertion fences are acquired before execution; contention unwinds the
entire set before an owning-loop timer retries. Blocking LOCK and baseline
directory CLOSE are separate runs to avoid holding future-close fences while
waiting for another close. Fences survive finish retries. Pure queries see their
own overlay; competing requests see only accepted intent.

Atomic ACCESS/RANGE retirement is the logical-close commit point. Baseline
accepted cancellation after this point still unhashes the open; resource reference
consumption is tracked separately. DOC scopes cancellation through its preallocated
removal/CLOSE suffix. The accepted publisher unhashes the open and releases its
original resource reference; typed CLOSE consumes the exact independent retained
reference, explicitly rebound after optional POSTQUERY cursor upgrades.
Independent namespace membership preserves peer discovery after ACCESS unlink,
including logically closed peers still waiting to finish DOC. Legacy closes drop
cache rights before waiting on the same namespace/ACCESS fences, retain path and
handle identity until guarded cleanup, and keep asynchronous deletion tracked by
the retirement list. CREATE's per-open delete mode survives ordinary disposition
clear and uses its logical open path/credentials rather than a shared cache bit.

Shutdown drains all wire compounds, maintenance compounds and deferred open
retirements while the worker event loop and VFS remain available. Disconnect paths
complete abandoned parked requests, and parser rejection releases its wire context.

Other remaining CREATE boundaries are stream creation, lease/durable/AppInstance contexts
and delete-on-close. Parked durable eviction remains a discovered boundary pending
typed retirement/deletion; its legacy helper performs irreversible registry and
filesystem work and is not a replayable callback. Bounded RENAME covers existing
ordinary regular files, a nonreplacing root-directory rename within one parent,
and backends with strict matched NOREPLACE support. Per-command path snapshots
preserve rename/QUERY/CLOSE notification ordering inside the compound.

## Follow-up: open-if and overwrite

Regular and directory OPEN_IF now choose create versus existing-open behavior
inside the same group. The directory EEXIST arm opens the existing name; a
successful MKDIR retains its exact returned FH. CreateAction and namespace
notifications follow the actual outcome. Once creation has occurred, admission
failure is an ordinary accepted-prefix error, never legacy redispatch of the
mutating command. Existing OPEN/OPEN_IF enforce the READONLY DOS access fence.

Ordinary OVERWRITE, OVERWRITE_IF and SUPERSEDE use
nontruncating OPEN, access/DOS/type/delete-pending checks, SHARE reservation,
cache coordination, then typed OVERWRITE on the exact retained handle. This wraps
the existing overwrite primitive, preserving base named-stream removal. Only an
accepted successful mutation emits modification/size/attribute notifications.
After OVERWRITE, typed NARROW_ACCESS gives later commands the requested lifetime
SHARE view, including read-only and metadata-only truncating opens. Metadata-only
truncating requests still assert their requested deny bits during initial
admission, in both conflict directions; only their lifetime view becomes (0,0). Other clients
continue to see the conservative transient WRITE reservation until accepted
journal publication. No frontend claim mutation or admission runs at publication;
waiters pump only after the journal and frontend publication are complete.

The lifecycle wire fixture covers fresh/existing OPEN_IF and create actions,
native overwrite/query/close counts, real share and READONLY denials preserving
contents, successful truncation, and create-if-absent overwrite outcomes. The
coordinator owns acceptance of these new source changes and tests.

Each CREATE reservation is tied to its final readiness checkpoint. Failed
post-admission coordination or OVERWRITE withdraws that provisional reservation
before the next group, while retaining its storage until attempt cleanup. This
preserves later independent command admission without undoing accepted filesystem
prefix effects. The wire fixture injects both failures before mutation and checks
that a later conflicting OPEN succeeds in the same submission and bytes survive.
Overwrite notifications snapshot the effective pending namespace path at execution;
fresh creation notices keep the original creation identity. Oversized basenames
are rejected before fixed-size open/token construction, with explicit wire cases.


## Follow-up: CREATE EAs

ExtA input remains owned by the wire request across retries, parser rejection and
legacy redispatch. Lists over the inline 1024 bytes use owned overflow storage,
bounded by the existing maximum transaction size; duplicate ExtA replaces and
releases previous storage. The CREATE group lists existing names, applies each
EA through typed SETXATTR/REMOVEXATTR, then reaches readiness. Existing spelling
is preserved case-insensitively; deleting an absent EA succeeds. As with the
legacy implementation, ExtA applies to opened existing objects and follows
OVERWRITE when requested. Invalid later entries preserve earlier accepted EA
changes, fail the CREATE, and withdraw its provisional SHARE reservation. The
EA-bearing CREATE initially contributes only a budget checkpoint. Once all
static command groups exist, it checks capacity for the whole CREATE pipeline,
EA tail and bounded access-refresh suffix before appending them to its group and
performing any leaf mutation. Insufficient capacity becomes an accepted-prefix
boundary to the legacy adapter, which processes large EA lists in bounded VFS
compounds. No new notification is
introduced for EA changes where the existing legacy path emitted none.

Legacy CREATE failures during EA application or overwrite now use the shared
failed-open cleanup helper. It unhashes the unpublished FileId, removes durable
registry ownership, and deletes a persisted backend record in a bounded VFS
compound before dropping the final caller reference. Earlier accepted filesystem
changes remain. Concurrent durable parking still requires explicit ownership
transfer to avoid losing the registry-held open reference during failure cleanup.

Wire coverage includes >1024-byte values, duplicate context ownership,
CREATE/OPEN/OVERWRITE followed by same-compound EA QUERY, case spelling, absent
removal, and a later invalid entry preserving its earlier accepted prefix.
Unbuffered CREATE/OPEN uses an immutable effective access mask, removing APPEND
before classification/validation and again after generic or MAXIMUM_ALLOWED
expansion. An APPEND-only request fails before any filesystem operation; reported
granted rights and later WRITE checks use the filtered mask. Non-OPEN
OPEN_REPARSE_POINT dispositions coalesce for ordinary leaves. They stop at an
actual symlink before mutation and redispatch that command through legacy
follow/collision policy after the discovery prefix is accepted; FILE_OPEN retains
native NOFOLLOW. Caching/durable/AppInstance contexts retain their existing
specialized paths; the next stream slice is described below.

CREATE EA spelling now also consults successful earlier entries in the same
list. Earlier SET preserves its selected spelling; DELETE leaves a private
tombstone so recreation uses the new requested case. This state is cleared on
retry. Exact-group wire tests cover new-case updates, delete/recreate sequences,
unbuffered explicit/generic/MAX access, and reparse options on ordinary file and
directory names. A real symlink FILE_CREATE collision checks that both the link
and target data survive discovery/legacy redispatch unchanged.

## Follow-up: uncached named-stream producers

Ordinary FILE_OPEN, FILE_CREATE and FILE_OPEN_IF stream names now have a native
producer path on regular base files. Parsed wire names stay immutable: private
state separates the base name and stream name, including an optional `$DATA`
suffix. The group opens the base without truncation, checks access/delete state,
reserves base DELETE-sharing rights, opens the stream, then reserves stream SHARE
rights. Both provisional reservations share the final readiness endpoint so a
failed producer cannot block later independent commands. Related I/O and CLOSE
consume the private stream handle and both claim-owner aliases.

Accepted surviving opens take ownership of the base handle as an ACCESS actor
anchor, released only after base claim retirement. The base pending namespace
token tracks accepted external renames; accepted publication promotes that path
before attaching the stream identity. Base creation and stream creation have
separate accepted-prefix notifications, including when later admission fails.

Stream overwrite/supersede, EAs, reparse-point options, directory bases, caching,
durable/AppInstance contexts and delete-on-close remain specialized boundaries.
An existing base's reverse DELETE-sharing conflict retains legacy policy through
execution-discovered redispatch before stream mutation. Unsupported stream
backends still return their existing protocol error. New wire cases check exact
coalescing, surviving-handle ownership, collision cleanup and preserved base data.
Existing-base FILE_OPEN also redispatches if the base ACCESS admission is fenced:
legacy resolution can then observe the target stream's DELETE_PENDING or absence
during last-peer teardown. CREATE/OPEN_IF do not use this discovery boundary, so
an accepted base creation is never replayed to bypass a fence.

## Follow-up: stream overwrite and CREATE extended attributes

The regular-base uncached stream producer now accepts OVERWRITE, OVERWRITE_IF and
SUPERSEDE. Base OPEN and OPEN_STREAM remain nontruncating; OVERWRITE never sets
CREATE on either identity. Typed OVERWRITE runs only after base DELETE and stream
SHARE reservations and cache coordination. Existing private narrowing restores
requested lifetime rights before a later command in the compound executes.
The backend receives the stream handle, preserving unnamed data and sibling forks.

Stream CREATE EAs use the retained base handle explicitly, matching their shared
metadata semantics. They reuse sequential EA planning, canonical spelling and
accepted-prefix handling. Both ACCESS reservations remain provisional through
EA completion. Capacity checks still defer before any base/stream mutation when
the EA pipeline cannot fit, allowing the existing bounded compound adapter.

Directory-base streams, reparse options, requested caching, durable/reconnect and
DOC lifecycles remain boundaries. Existing-base FILE_OPEN can defer before stream
mutation on a base admission fence to preserve last-close DELETE_PENDING behavior.

## Wave12: accepted legacy caching grants (source handoff)

Ordinary nonstream CREATE/OPEN/overwrite can now request a unique legacy
LEVEL_II, EXCLUSIVE or BATCH oplock in the native pipeline. The command ends its
VFS run; preceding native commands still coalesce, and the following command
executes only after the selected grant has published. No tentative cache grant,
member, or FileId is visible during execution or finish retry. Existing cache
recall remains explicit, repeatable OPEN coordination.

Construction preallocates an unlinked grant candidate. After accepted finish,
under the canonical file-state lock, admission examines current claims and grants
the requested legacy level, LEVEL_II, or NONE without allocation, callbacks,
recall, waiter pumping, or filesystem I/O. The same lock protects the FileId hash,
grant/member attachment and canonical ACCESS own_cache backlink. Same-client
RqLs handle caching excludes a legacy grant; other same-client leases and forced
level-II shares cap it to READ. Legacy modes never retain an unadvertised HANDLE
bit after downgrading. The reply snapshot follows the accepted grant decision.
The open retains a distinct cache file-state pin until normal grant retirement.

OPEN_REQUIRING_OPLOCK stays legacy: a mandatory grant cannot fail after accepted
filesystem effects. RqLs, real durable/persistent/reconnect and AppInstance paths
also remain explicit boundaries. A DHnQ request that cannot obtain BATCH/HANDLE
caching can now be declined in the native pipeline; it has neither a durable
registry entry nor a create-guid replay identity. Legacy shared-lease CREATE now
uses the core's atomic grant-reference plus member-attachment APIs, including
racing first acquisition, to coordinate with native shared-lease CLOSE.

The new create-cache wire fixture verifies QUERY/CREATE and related QUERY/CLOSE
in two compounds, all three actual legacy grant levels, no early grant/member or
FileId while finish is held, a read-only OPEN finish retry, no-break II/NONE caps,
directory refusal, declined DHnQ, and a real durable grant retaining its legacy
boundary. The existing EA adapter fixture now explicitly uses RqLs so it continues
testing the legacy adapter after ordinary legacy oplocks become native. The
coordinator owns build/test acceptance; no mutation rollback is simulated.

Wave12 integration preserves the pre-recall same-client caching cap as private
attempt input. Legacy selection observes a same-client H lease before its
asynchronous overwrite break is acknowledged; accepted-only selection must not
forget that refusal after ACK reduces the lease to NONE. Phase-2 prepare snapshots
the cap, acceptance intersects it with current holders, and reset recomputes it
on retry. The wire fixture covers metadata-only OVERWRITE/OVERWRITE_IF/SUPERSEDE
against a same-client RWH lease and verifies NONE after the required break/ACK.
Legacy members also initialize their lease/legacy wire tag before atomic attach,
so a concurrent break cannot interpret a newly attached RqLs member as a legacy
oplock during the interval before CREATE records the grant result.
