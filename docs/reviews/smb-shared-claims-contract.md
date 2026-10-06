# SMB compound shared-claim contract

The range journal and ACCESS retirement journal reserve their edits during typed
VFS execution. Pure frontend prepare/complete callbacks inspect inputs or update
attempt-private results; they do not change public claims, send responses, or
retire protocol owners. Accepted publication must be infallible. Journals cannot
certify filesystem rollback; finish rejection is replayable only when the backend
actually rejected/aborted its transaction or the attempt had no filesystem writes.

## ACCESS admission fence

An explicit coordination step may acquire a per-file ACCESS insertion fence
before deciding last-close deletion. Acquisition is immediate under the file
mutex and holds a file reference, not the mutex, until release. Contending
cookies fail without acquiring anything; no request waits for another fence
while retaining partial admissions. Inert attribute-only ACCESS registrations
are covered. Already admitted claims remain visible to the DOC peer scan.

New direct insertion reports ordinary DENIED with `admission_fenced` set and
never starts recall, holder eviction, or a fresh parked acquire. Existing queued
ACCESS tickets remain queued, with requeue/release synchronized to avoid a lost
wakeup; releasing the last matching holder pumps them outside frontend locks.
Pure admission queries ignore the insertion barrier. Typed RESERVE temporarily
uses its admission cookie to admit a later operation from that same compound;
the SMB private pending-delete overlay still rejects inappropriate CREATE.
The cookie is removed from the admitted claim before frontend callbacks. DOC
uses an explicitly set stable batch-context cookie; a pooled compound address
must not remain a fence identity after compound_free.

Fence holders can persist across rejected attempts. Terminal release follows
accepted frontend publication or rejection cleanup, so a waiter cannot insert
between the last-close peer scan and namespace mutation. This is a control
reservation, not a claim that represents a public open or filesystem rollback.

## Exact range records

`vfs_claim_journal` preserves SMB acquisition multiplicity. An acquire batch is
atomic; exact unlock removes the oldest available matching offset/length and
retains a successful prefix on later error. Another journal's reserved removal
is unavailable until that attempt accepts or resets. If all matching accepted
rows are reserved by peers, unlock reports ordinary EBUSY / LOCK_NOT_GRANTED;
it does not wait, report RANGE_NOT_LOCKED, or trigger whole-compound retry.
Truly absent rows report ENOENT / RANGE_NOT_LOCKED. Raw finite `UINT64_MAX` length is distinct from
the VFS's to-EOF sentinel. Generic zero-byte records own no bytes; canonical SMB
owners explicitly opt into legacy interior-point compatibility until a protocol
oracle changes that observable behavior. POSIX and NFS range conventions are
unchanged. SMB ranges are local under the current projection contract; POSIX
backend-projected ownership is not accepted by this SMB journal.

`RANGE_BATCH` executes inside the compound. READ, WRITE and COPY_RANGE receive
private removal exclusions, and protocol mandatory-I/O checks use the same
compound journal view. Other requests see accepted old ranges plus provisional
new reservations. New reservations remain hidden from accepted GETLK results.

Blocking local acquisition polls on the owning VFS loop with 1–100 ms backoff;
SMB waits indefinitely until grant/cancel/owner retirement. An idle owner binding
is released while waiting to permit idle-owner retirement. Owner lifetime pins
are concurrent: disjoint operations on the same owner never wait for an internal
edit lease. Acquisitions reserve globally visible provisional records; removals
reserve individual records. Record references survive accepted publication until
journal completion, even if a concurrent journal already removes the record.
Earlier successful deltas retain their actual byte reservations while a later
command waits. Future backend transactions must account for these potentially
long resource holds and genuine byte-range dependency cycles; this layer does
not claim to abort filesystem transactions or detect those cycles.
`range_on_wait` is explicit protocol coordination and emits at most one interim
per request; its memo survives retries. The pure `range_is_canceled` predicate
observes request-lifetime atomic teardown state without cross-thread raw-pointer
queues. A canceled command reports ordinary EINTR, preserving accepted prefixes
and later independent groups. Global cancellation remains sticky.

Canonical SMB owner tokens must never mix with legacy per-open record lists.
Local LOCK commands use the token. Typed `RANGE_OWNER` allocates a token from
a provisional CREATE handle during execution. Pure callbacks bind its borrowed
attempt result; accepted surviving CREATE transfers ownership, while retry and
private CLOSE/free retire untransferred tokens before releasing handle anchors.
CREATE→LOCK→I/O→UNLOCK/CLOSE can therefore stay in one compound.
Existing opens with legacy records remain wholly
legacy until their records drain. CLOSE requests retirement and waits for actual
claim drain before reporting completion. Final open teardown and durable
preservation inspect canonical tokens as well as transitional legacy owners.

`RETIRE_OPEN_CLAIMS` reserves whole canonical RANGE-owner retirement together
with ordinary and base ACCESS retirement as one typed operation. RANGE staging
requires no other journal pin on that owner, snapshots up to 65,536 additional
exact records per attempt, and creates a reversible admission fence. Allocation
and overflow checks precede any fence or removal. A peer fence/pin conflict is
ordinary EBUSY, never a parked acquisition or finish rejection. Disjoint owners
remain independent. Same-attempt admission and mandatory I/O exclude all
retiring rows, including successful earlier LOCK commands; outsiders continue to
see the old reservations until acceptance.

If any of the three retirement admissions fails, typed dispatch withdraws only
reservations created by that operation. Earlier command deltas remain intact.
Accepted publication makes the range-owner cutoff permanent before protocol
unhash; completion releases the anchor-lifetime barrier. Attempt reset restores
old rows unless an independent external teardown also cut off the owner. This
facility does not roll back a later backend CLOSE error or namespace operation;
frontends must define those accepted-prefix semantics before enabling a path.

## Canonical ACCESS storage and retirement

`vfs_claim_access` allocates an owner token with its own ACCESS claim storage and
file-state reference before admission. Frontends route claim reads, park/shrink,
and grant-anchor updates through `access_owner_claim`; every retirement goes
through `access_owner_retire`, never a raw release of that storage. The frontend
keeps callback and handle anchors alive through retirement. No unprotected pointer
to a frontend-embedded share claim is staged in the journal.

`RETIRE_ACCESS` reserves an owner edit lease, preserving the authoritative old
claim while adding a private exclusion for subsequent RESERVE operations. An
external CLOSE/disconnect marks a cutoff immediately but defers actual unlink
until the journal accepts or resets. Accepted publish unlinks without allocation,
admission, protocol callbacks, or I/O. Frontend state publishes next; journal
completion releases pins and wakes waiters. A typed accepted CLOSE must not wait
for `is_retired` before compound free: the accepted unlink already happened, and
free is what releases its journal pin. Legacy CLOSE polls while retaining a token
reference. Timers must be one-shot when callbacks may free request storage.

`RESERVE_ACCESS` allocates the canonical token during typed execution and clones
an unlinked input template. `take_access_owner(cp, index, &file)` transfers both
the owner and the existing reservation file-state reference after acceptance;
the owner retains a separate file reference. A provisional slot may borrow the
operation result token during execution; rejected or unsuccessful attempts must
not leave it aliased in a public open after compound cleanup.

The ACCESS facility does not yet implement namespace serialization, atomic
NOREPLACE rename, delete-on-close intent publication, stream-holder retirement,
or atomic filesystem namespace removal. Those require explicit contracts;
retaining a SHARE claim is not a substitute for them.

## FH-routed recovery records

`SEARCH_KEYS_AT` now keeps bounded, owned pages of binary key/value results.
Inputs are immutable copied bounds. The continuation is the first unconsumed
key, so the next page resumes inclusively without skipping or duplicating a
record in a stable keyspace. Retry frees the rejected page and rebuilds it;
frontends publish recovered registry entries only after acceptance. Limits are
65,536 entries, 16 MiB of payload, and one continuation key up to 4 KiB. An
oversized first row returns ERANGE instead of an empty page with no progress.
These limits bound retained compound results, not a backend's internal scan
snapshot. Separate page compounds do not promise a single filesystem snapshot.
The routed primitive also checks request scratch including namespace overhead.

## Identity-matched removal

Typed REMOVE can set `remove_match_child_fh`, carry the expected child identity
in `arg_fh`, and inspect `remove_unmatched`. The strict primitive requires
`CAP_REMOVE_MATCH_FH`; only memfs currently advertises it. Other backends return
ENOTSUP before recall or dispatch. Existing legacy match wrappers still permit
unconditional fallback, an explicit backend correctness gap that must not be
mistaken for atomic identity checking in the new conversion.

`REMOVE_NO_NOTIFY` defers both ordinary lease/delete notifications and the
synchronous namespace event gate. Cache invalidation and child cache recall
remain in the typed operation; the frontend publishes removal events after
acceptance only when the operation actually removed the expected object.
The parent LeaseKey and actor are forwarded. `namespace_cred` is a borrowed,
immutable override limited to OPEN_CURRENT and REMOVE, including REMOVE's
implicit parent open; it does not change the group's credentials for later ops.

## Cancellation after a committed cleanup start

A construction-time `set_cancel_scope(start,end)` may declare one static
suffix inside a command group. Successful non-skipped start completion arms the
scope before its complete callback; cancellation is recorded but does not skip
the suffix through end. Earlier cancellation still prevents start dispatch.
Errors retain ordinary stop behavior, so optional DOC cleanup errors must be
recorded privately and explicitly normalized by the mapper. The scope cannot
cross groups, overlap another scope, or dynamically append cleanup operations.
It neither settles underlying asynchronous work nor implies rollback. Once end
completes, cancellation stops subsequent operations; canceled compounds cannot
retry. DOC uses this for admitted claim retirement through final handle CLOSE.

## Directory CREATE publication

Typed directory CREATE forwards `CHIMERA_VFS_MKDIR_NO_NOTIFY` to a flagged
mkdir primitive. Authorization, backend mkdir, and name/attribute cache updates
remain in normal execution, while synchronous and ordinary DIR_ADDED observer
events wait for frontend accepted publication. Legacy mkdir callers retain
flags-zero behavior. A held-finish regression verifies event suppression after
a real successful mkdir; it never injects rejection after that mutation.

## Producer CLOSE anchor lifetime

CLOSE invalidates a producer's output/cursor aliases immediately, but keeps the
owned handle reference in `closed_output_handle` until compound cleanup. RANGE
journals and ACCESS reservations may still borrow that handle as their actor
anchor before finish. Both rejected retry and accepted free drain all claims
and tokens before dropping closed producer references; external borrowed CLOSE
references retain their accepted-only release rule. A held-finish regression
checks handle reference counts across claim-only retry and accepted cleanup.


## Strict rename and accepted notification

Typed RENAME accepts NOREPLACE, MATCH_SOURCE_FH, and NO_NOTIFY flags in
`remove_flags`; the expected source identity is `arg_fh/arg_fh_len`. Existing
parent lease-key and operation-handle actor inputs retain their recall
exemptions. Strict capabilities are checked before authorization, recall, or
backend dispatch. Source mismatch is ESTALE, and any occupied destination is
EEXIST, including same-inode aliases and the same name. Frontends can handle
intentional identical-link no-ops before invoking this strict primitive.

Memfs compares the full source file handle and destination absence while
holding both namespace parent locks through mutation. Linux advertises native
renameat2 NOREPLACE only; it does not advertise atomic source identity matching.
Thus the strict SMB matched-source slice remains a boundary on Linux and all
other backends lacking the required capabilities. There is no lookup-then-rename
emulation advertised as atomic. NO_NOTIFY suppresses both synchronous and
ordinary rename observers while preserving cache invalidation and authorization.
The frontend publishes the event after acceptance. Real filesystem mutations
still cannot be rejected/retried without backend transaction support. SDK ABI
version 3 rejects old modules with incompatible compound namespace request layouts.

Regressions cover source rebinding at both public admission and direct backend
dispatch, occupied destinations, missing capability rejection, and held-finish
notification suppression after successful strict rename. The mutation case is
accepted, never subjected to a fabricated abort.

The memfs dirent now records immutable directory type at insertion, and rename
skips ancestor traversal for non-directories using that bit under the parent
lock. No additional child lock precedes directory-cycle detection. Cross-directory
directory moves now take a per-filesystem topology rwlock exclusively; other
renames share it. Every additional inode acquisition is nonblocking: contention
drops all inode locks, waits for that mutex alone, then restarts FH/dirent and
ancestry validation. The mount pins stable inode-block storage during that wait;
no dirent or generation snapshot survives the restart. Thus ancestor, second
parent, source and replacement-target contention cannot create a rename lock
cycle, and opposing directory moves cannot both pass the ancestry check. Removed
parent directories and incomplete ancestry walks are rejected before mutation.
The topology lock is released before completion callbacks. Ordinary independent
renames still run concurrently; data I/O does not take this lock.

mkdir initializes only a new directory edge. rmdir/replacement can detach only
empty directories, so cannot remove an ancestor of the locked live destination;
all updates of an existing directory's parent are under the exclusive rename
lock. This is a rename repair, not a general namespace-locking claim: inherited
LOOKUP/READDIR of `..` still acquire child then parent, and can invert ordinary
parent-to-child lookup/remove independently of rename.

The backend-private rename regression holds an ancestor mutex while renaming a
regular file, and holds the source directory mutex while rejecting a move into
its descendant. The latter also found an inherited cycle-check ordering bug:
the ancestry edge must compare source inode plus generation before taking that
ancestor mutex, rather than on the following traversal iteration. Both bounded
fixes are covered without production test hooks. The expanded backend-private
fixture observes real failed trylocks to force ancestor contention, source
rebinding, orphaned destination, and replacement-target contention; checks an
independent regular rename completes while another waits; and synchronizes two
opposing directory moves before topology-lock acquisition, requiring exactly
one move and one cycle rejection with correct parent links and link counts.


## Dynamic coordination identity

Grouped completion callbacks can append COORDINATE operations. Prepare, parked
coordination, finish, and active cancellation cleanup scopes cannot. The cached
result key includes physical index, callback function, private context identity,
and resolved FH. This prevents a rebuilt dynamic suffix from replaying a prior
phase's result when physical indices shift. Context storage and its semantic
identity must remain valid until the entire compound is freed, including when
an earlier dynamic descriptor is discarded on retry. The regression changes
function and context independently in reused slots, then verifies a third
attempt reuses the matching results without repeating coordination.

## Checked stream deletion

Stream delete-on-close now supplies the original complete stream FH to typed
REMOVE_STREAM. `CAP_REMOVE_STREAM_MATCH_FH` promises an atomic comparison with
unlink under the backend's stream namespace lock. Memfs implements that contract
under the base inode mutex. A rebound name returns ESTALE without touching the
replacement; a missing name returns ENOENT. Unsupported backends return ENOTSUP
before dispatch, with no lookup/remove emulation. Ordinary REMOVE_STREAM callers
retain the unchecked API and semantics. SDK version remains the unmerged ABI 3.

The SMB helper no longer opens and verifies the stream separately before removing
its name. Its pending metadata owns copied base/stream FHs, credentials, name,
and notification identity. Successful accepted deletion publishes STREAM_NAME
against the base name, with the saved ParentLeaseKey exemption. Mismatch and
missing-name outcomes publish nothing. This does not claim backend mutation
rollback: the held-finish mutation regression always accepts. Only the read-only
identity mismatch regression injects retry. The wire regression deterministically
rebinds the stream before checked dispatch and verifies replacement I/O survives;
a separate held finish verifies that CLOSE and its single notification wait for
acceptance. Existing CREATE delete-mode and plain-clear lifecycle cases remain.

## Failed producer admission cleanup

A frontend can explicitly mark RESERVE_ACCESS with
`chimera_vfs_compound_reserve_access_until(reservation, ready_checkpoint)`.
Both endpoints must belong to the same execution group, including a dynamically
appended suffix. The owner is private until the ready checkpoint completes
successfully without being skipped and the group succeeds. If those conditions
fail, the executor retires only that reservation before running later groups.
Ordinary VFS operation errors remain unchanged. Cancellation follows the same
group-exit cleanup.

The retired owner, file-state reference, and result storage remain alive until
normal journal-first compound teardown. Existing aliases therefore do not dangle;
accepted transfer is disabled by clearing claim_held. Unmarked successful prefix
reservations and previously staged CLOSE retirements are preserved. No filesystem
mutation is reversed. CREATE uses this opt-in so a failed overwrite or post-SHARE
recall cannot spuriously block a later independent OPEN in the same compound.

## Attempt-private ACCESS narrowing

Typed NARROW_ACCESS addresses only a successful RESERVE_ACCESS owned by this
compound. It stages subset-only used/denied bits after a successful overwrite,
without exposing an early public shrink. Subsequent reservations check the latest
private ACCESS row with the ordinary two-sided conflict predicate and exclude its
conservative public counterpart. This preserves owner exemptions and both share
conflict directions. Cache-grant conversion and sole-opener overlays remain a
separate scope.

The ACCESS journal stores an append-only change log. Multiple narrows compose;
a later retirement can be rewound without losing an earlier narrow. Every delta
pins its owner, and the owner edit lease remains until its final retained delta
is released. Reset restores the public pre-attempt rights. Accepted publication
applies only the final per-owner subset or retirement under the file lock;
waiters are pumped after frontend publication when the journal completes. A
failed producer's explicit cutoff suppresses its narrowed private row while
retaining claim/handle anchors through journal drainage. No filesystem rollback
is implied by these reservation changes.
