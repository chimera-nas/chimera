# Constructor admission and bounded occupied-target replacement

Wave18 implementation checkpoint. The coordinator owns the combined build and
test results; this report does not claim independent validation.

Regular-base ReplaceIfExists RENAME can now replace a distinct occupied regular
file within the original VFS compound when the destination has no protocol
holders or pending namespace users. A CREATE producer, the replacement, and its
QUERY/CLOSE suffix can stay in the same batch. Strict source and destination FH
matching and authoritative rename outcomes are required; currently this means
memfs. Existing same-inode aliases retain their atomic no-op handling.

## The constructor contract

All filesystem SMB CREATE entry paths register a constructor token before the
first backend handle acquisition, including parent resolution. Native CREATE
does this in explicit, repeatable COORDINATE; legacy CREATE does it before its
original handler body. The token spans private acquisition and admission until
accepted publication or cleanup. IPC CREATE does not participate. This covers
legacy stream, mkdir, durable and reconnect dispatch through the common entry.

An occupied-target replacement acquires a global writer token. It cannot pass a
foreign constructor, even one whose backend OPEN completed but has not yet
called the frontend or reserved ACCESS. Conversely, a later CREATE cannot
acquire an old backend identity under the writer. Native contention defers the
untouched command; legacy constructor contention polls on its owning worker
before backend work, with teardown cancellation. No registry mutex spans I/O.

Owners are wire compounds. Multiple writers and constructors from one wire
compound can compose. This is not permission to replace an earlier private
target open: the destination participant scan includes bound unpublished
producers as well as public and closing opens. A prior related CREATE of the
source can therefore compose; a prior destination CREATE holder still defers.

The global scope is conservative and can briefly delay unrelated CREATEs. A
future path-scoped admission scheme must cover unresolved parents, aliases,
stream/base identity and cross-share views before narrowing it. The current
token should not be treated as full namespace transaction isolation.

## Destination lifecycle and release

After acquiring constructor exclusion, RENAME acquires destination DOC and
ACCESS fences and checks registry then file-state locks, in that order. It
rejects public/pending/closing participants, protocol ACCESS/CACHE/RANGE claims,
pending claim acquisitions and break waiters, streams, and deletion state. The
VFS implicit I/O ACCESS row is not a protocol/path owner and is excluded from
that holder test. The backend's atomic MATCH_DEST_FH verifies that the looked-up
destination is still the inode being replaced.

No partial-fence wait is introduced. A conflict releases the destination
resources and defers before mutation. Successful resources survive through
accepted source path publication; command release drops ACCESS, DOC, file-state
reference, then constructor exclusion. Retry reset drops the same dynamic
resources, and target coordination runs on every attempt. Ordinary completion
callbacks only record private outcomes. Existing accepted-prefix publication
continues to preserve a successful initial NOREPLACE move if its optional suffix
is canceled. No test rejects finish after a real rename mutation.

The generic namespace journal also provides an allocation-free accepted identity
rebind, preserving path, participant context and DOC metadata. SET_REPARSE's
separate FileId/claim migration implementation supplies the required exclusion;
the helper alone does not make identity migration safe.

## Coverage added

The existing namespace unit test covers delayed-constructor/writer exclusion,
same-owner promotion and multiple writer lifetimes, failed admission with no
partial token, idempotent cleanup, bound pending identity discovery, and accepted
identity rebind metadata preservation.

The existing RENAME wire probe adds:

- Unheld distinct target with CREATE/RENAME/QUERY/CLOSE in one VFS compound.
- Destination appearance before initial atomic NOREPLACE now remaining in one
  compound when the new destination has no holders.
- Actual backend OPEN completion paused before frontend callbacks and ACCESS,
  for native and legacy mandatory-oplock CREATE. Replacement must decline its native
  distinct-target path while that constructor is invisible. The fixture holds
  legacy fallback before mutation and drains the opener before proceeding.
- A strict replacement held before mutation while native and legacy DOC CREATE
  arrive. Actual constructor-denial observation proves neither request acquired
  the old identity; after writer release both open and read the new contents.

Existing live-target fallback, old-handle data, source mismatch, same-inode no-op
retry, exact-link DOC, and accepted-prefix cancellation cases remain enabled.

## Remaining boundaries

Live occupied targets, pending constructors, cache/lock/deletion state, directories,
stream rename and occupied-target hardlinks still defer. This change does not
repair every legacy replacement race: the old legacy lifecycle can still
replace a target while a backend-acquired but not yet admitted constructor is
pending. The new native path never takes that shortcut. Completing the legacy
handoff requires writer admission before partial fences, with an existing
continuation or safe restart rather than a new broad sharing error. Native
strict identity matching protects against a destination changed before its
mutation; no general cross-protocol namespace transaction isolation is claimed.

Directory conversion still needs descendant repathing, translation for each
share root, ancestor stability or reconstruction, and atomic handoff from final
READDIR discovery to mutation. A global constructor writer alone does not update
descendant paths or prevent foreign directory moves. A coherent next slice is a
directory-specific private path journal, with all affected views reserved before
mutation and infallible accepted publication.

Typed stream rename remains a VFS prerequisite: define a name-to-name operation
on one base identity, explicit source/destination matching and replacement
outcome, and suppressed notification support. SMB then needs stream participant,
base-owner, pending stream-DOC and full-path publication in the same accepted
lifecycle. Mapping it to ordinary base RENAME would change the wrong namespace.

Occupied LINK conversion additionally needs an atomic matched destination
replacement contract; it cannot be implemented as REMOVE followed by LINK on
the current nontransactional backends.
