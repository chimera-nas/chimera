# Bounded native directory RENAME (wave20)

Wave20 expands admission to unrelated root-child handles and adds
namespace-mutator exclusion. Root's full Debug+ASan build5 and all 52 selected
tests passed, including the namespace unit and rename wire probes. See
[combined status](smb-compound-current-status.md) for validation logs.
Backend transactions/rollback remain out of scope.

## Enabled slice

A directory immediately below the share root can move to another name in that
same root in one VFS compound. The source may be a public FileId or an earlier
same-wire OPEN/CREATE result. Exact source peers receive the private path overlay,
so chained RENAME and QUERY_INFO consume the new spelling before accepted
publication. Related producer CLOSE can coalesce too; an ordinary public-directory
CLOSE currently starts a separate run because its fence admission is outside the
regular-file DOC journal. Closed subtree contents are preserved by the backend
rename; they require no frontend path rewriting.

This is deliberately a conservative, low-contention slice, not general native
directory rename:

- A global writer excludes native and legacy backend constructors and every
  RENAME/LINK mutator until accepted path publication and attempt cleanup. These
  shared reader tokens compose within the same wire batch; no mutex spans I/O.
- Every attached/pending/closing SMB namespace participant must have the same
  share view. Source-directory and share-root identities are allowed, as are
  exact root-child siblings: parent FH equals the root, and full path equals a
  nonempty basename other than `.`/`..`, with no separators. Deeper paths,
  foreign views, unresolved pending identities and invalid paths still defer.
  This replaces wave19's global ban on all unrelated handles. Mutation readers
  prevent an existing sibling from moving/linking into the directory after the
  snapshot, including through legacy stream/occupied-target operations.
- All acquired batch handles are checked separately using their current-group
  path overlay, including public handles and closed/failed private producers.
  Later unexecuted producers have no acquired identity yet; their real lookup
  sees the preceding move. A later directory move repeats admission and sees
  those producers. The public and private checks use one shared pure predicate.
- One typed READDIR with a 128-entry bound must reach EOF. Every immediate child
  must have a valid FH and no protocol ACCESS holder. This preserves the existing
  legacy immediate-child contained-open check for non-SMB holders without issuing
  recalls inside replayable callbacks. Large directories, backend scan errors and
  held children fall back to the existing paged/recalling path.
- The source parent and destination parent must both be the share root, and the
  source's private share-relative path must be a basename. Moving the share root
  itself is excluded. Nested/cross-parent moves need ancestor stabilization and
  descendant path journals; foreign share views need path translation.

The first mutation is atomic NOREPLACE + expected source identity. EEXIST always
falls back without namespace effects for a directory, even if ReplaceIfExists is
clear, preserving legacy directory-replacement rules. Occupied targets and
stream-itself renames remain boundaries. Directory notification uses the directory
rename class and publishes only after accepted finish.

## Lifecycle and limits

Native RENAME/LINK reader admission and directory writer admission are explicit
COORDINATE operations repeated each attempt. Readers run after the pure command
checkpoint and PUTHANDLE, before namespace discovery or handler mutation. Directory
writer admission precedes its execution-time source DOC/ACCESS coordination.
Contention never waits with partial fences: it defers the untouched command.
Readers and writers release on idempotent attempt reset/terminal cleanup.

Legacy SET_INFO RENAME/LINK acquires its reader before FileId resolution, parent
lookup, channel-sequence updates or source fences. Contention parks only a
context-pinned request on the owner-loop timer. CANCEL, disconnect, logoff and
tree teardown terminate that wait without backend work. Accepted legacy callbacks
publish before common request completion releases the reader. Disconnect stops
parsed suffix dispatch through the generic compound advance cutoff. The directory scan and path
callbacks only update private state; mutation/notification publication remains at
accepted completion. No new DOC operation eligibility was broadened.

The reader/writer barrier protects SMB acquisition and RENAME/LINK, not arbitrary NFS/POSIX external
namespace mutation. Cross-protocol changes between the contained-open snapshot and
rename remain the same broader VFS coordination limitation as the legacy scan.
This change does not claim to add a global cross-protocol namespace transaction.
Legacy live-target replacement still has its previously documented admission gap.

The enabled expected-source-FH backend is currently memfs. It returns the symlink
inode itself for leaf OPEN; ordinary SMB opens stop on symlinks, and explicit
reparse OPEN uses NOFOLLOW. A root basename cannot therefore conceal an acquired
descendant through transparent symlink following in this enabled slice. Legacy
non-OPEN reparse dispositions have backend-specific follow behavior elsewhere;
enabling matched directory rename on those backends requires auditing that
invariant or adding direct-link identity validation. Foreign share roots remain
excluded even if their display paths resemble root children.

## Coverage added

The namespace unit test covers root siblings (including closing/bound pending
rows), rejected descendants/unbound identities, same-identity pending source,
and foreign-view aliases. The rename wire probe covers:

1. Two coalesced directory renames with peer queries, followed by separate
   public closes, verifying a closed nested file's bytes survive and exactly two
   directory notifications, while an unrelated root file remains open.
2. Related directory OPEN -> RENAME -> QUERY -> CLOSE in one VFS compound.
3. Read-only exact-link rename finish EAGAIN/retry (no fake mutation rollback).
4. Held native directory rename versus a late old-subtree OPEN, proving the
   constructor is denied until accepted rename and then observes the missing path.
5. Earlier private child CREATE prevents native directory admission and reaches
   the legacy contained-open denial after accepted prefix publication.
6. Earlier private root-sibling CREATE + directory RENAME + QUERY coalesce.
7. A held directory writer excludes foreign regular RENAME, LINK and directory
   RENAME; their legacy admission sends PENDING and then observes the removed
   old ancestor after acceptance. Separate CANCEL and disconnect cases terminate
   those waits without moving their source.
8. A disconnected waiter with a legacy-only CREATE suffix proves no suffix
   handler entry, no backend open, and no created pathname. The fixture observes
   return from the canceled waiter's completion/compound advance before checking
   counters; it does not infer server teardown from client FIN delivery alone.
9. Failed-prefix isolation: moving a sibling into a directory held with DELETE
   access returns SHARING_VIOLATION at the parent probe; the independent directory
   rename still succeeds natively, and the sibling retains its original root
   path. This does not claim coverage of a successful earlier public move into
   that directory (the existing parent admission prevents that packet).

The existing directory scan retry/failure/cancel/recall fixtures continue to target
legacy streaming READDIR only, so a built-but-skipped native discovery descriptor
does not accidentally receive a legacy fault injector.

Legacy rename and its pause fixture now use the canonical actor-aware checked VFS
rename API; the fixture copies a held actor rather than retaining a stack pointer.
The parent deny probe and accepted native notification also use the canonical
open owner before overlaying ParentLeaseKey, preserving durable reconnect identity.
