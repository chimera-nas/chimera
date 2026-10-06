# NFSv4 fifth compound pass: remaining execution boundaries

This pass follows the user's request to keep removing NFSv4 compound boundaries.
It builds on the fourth pass; all changes remain uncommitted.
The [sixth pass](nfs4-compound-sixth-pass.md) supersedes the ordinary
same-owner/coalesced OPEN limitation recorded below.

## Addressed boundaries

Anonymous READ/WRITE and NFS READ_PLUS can now follow CLOSE in the same VFS
compound. COPY can likewise follow source and destination CLOSEs. NFS admission
uses its private closed-state view and includes denies from unpublished OPENs.
This also fixes anonymous READ, COPY, and size SETATTR bypassing a provisional
OPEN's deny reservation before that OPEN was published. Rejection occurs before
mutation and uses the protocol's LOCKED status.

The VFS receives a separate I/O admission view with optional ownership and
borrowed pinned exclusions. Anonymous I/O stays anonymous: it does not fabricate
an owner to bypass admission. Every unrelated NFS/SMB holder is still checked.
READ/WRITE requests and asynchronous permission gates copy owner values; the
exclusion array remains pinned by the compound. Shared implicit claims never
retain an exclusion pointer, and later requests perform their own checks. Each
request tests the rights it needs, rather than inheriting cached write rights
from a prior scoped operation.

COPY passes separate source and destination views through bounded streaming
read/write fallback. Native COPY currently has no equivalent admission gate,
so anonymous COPY now takes that fallback even without exclusions. Fully owned
COPY without exclusions keeps its native fast path. This is a performance
tradeoff for anonymous SDK/S3 callers. SMB COPYCHUNK and copy-offload fallback
callers are updated to pass the identities of their authorized endpoint opens.
The NFS proxy's consuming WRITE-buffer contract remains excluded from generic
streaming fallback.

Several existing public OPEN states under the same owner can now be reserved
for CLOSE by one compound. Owner-level group identity/count prevents competing
compounds and legacy OPEN from entering the reserved owner. Each state's
accepted or aborted completion is independent; releasing the first state leaves
no dangling owner pointer and retains exclusion until the last reservation.
Resources are still reserved before submission, held across retry, and published
or canceled after finish.

Saved VFS filehandles from an earlier run can be imported when they belong to
the same export and credential policy. Hidden cursor-only seed operations make
the inherited slot available to RESTOREFH, COPY/CLONE, LINK and RENAME. The first
wire operation's callback and result mapping account for these seeds. RESTOREFH
can also start a run when SECINFO left no current filehandle.

SECINFO no longer forces the end of a run. Its successful execution marks the
protocol current filehandle consumed; later operations requiring it fail with
NOFILEHANDLE before filesystem work. PUTFH or RESTOREFH can establish it again
inside the same compound. This private cursor state resets on retry and is
published only after accepted finish.

## Remaining limits

- Same-owner/coalesced OPEN, v4.0 replay/lease behavior, exclusive/delegated OPEN,
  OPEN_DOWNGRADE, and LOCK/LOCKU still need broader provisional state machinery.
- CLOSE with child lock states remains legacy. A second CLOSE of the identical
  state can still split during pre-reservation; different states under the same
  owner are now supported.
- Anonymous size SETATTR and the other range operations after CLOSE retain
  conservative boundaries pending their VFS admission-view plumbing. Metadata
  SETATTR now stays in the run. Native VFS READ_PLUS and CLONE have not gained
  a general admission-view interface; NFS READ_PLUS uses classifier plus READ.
- Cross-export credentials, namespace junctions, capacity/reply limits and
  asynchronous delegation/pNFS remain boundaries. Inherited saved handles are
  generalized only within one export, not across credential transitions.
- Legacy OPEN's fallible post-finish tail and v4.0 callback lease touches remain
  limitations of a universal retry contract. Backend transactions/rollback are
  still deferred. Synthetic finish rejection is restricted to read-only work.

## Validation

Debug+ASan builds passed without compiler warnings. All four wire variants
passed: 48 v4.1 and 77 v4.2 cases. Converted sequences require one tagged VFS
submission; the explicit TEST_STATEID fallback cases require precisely two
runs, with the entire inherited-saved-FH suffix in the second run. The finish
retry variants injected and accepted 32/42 read-only conflicts, including
successful prefixes followed by execution errors.

An OPEN setup after anonymous READ_PLUS initially returned DELAY while an
implicit cached read claim drained. Repeating that setup-only OPEN confirmed
normal transient break completion. The fixture now bounds that retry to three
seconds, forbids it for measured tags, and cannot hide compound splits or
re-execute measured mutations.

Focused VFS claim/enforce/compound/copy suites passed 4/4. New assertions cover
unrelated SMB denies, independent COPY endpoint views, anonymous native-capable
COPY without exclusions, asynchronous owner lifetime, and exclusion/cache
isolation. State/lifetime suites passed 2/2, including the grouped CLOSE
completion-order and accept/abort matrix and competing-group exclusion.
SMB FSCTL and both memfs/Linux IOCTL suites passed after preserving endpoint
identities in the three affected copy calls.

The final broad selection ran 326 CTest entries in 26.45 seconds: 324 passed,
two skipped, no failures. It included all three pynfs protocol suites, 224 SDK
tests, eight FUSE tests, 57 boto3 S3 tests, Ceph S3/model tests, NFS/VFS units,
and five SMB compatibility entries. The two Linux/io_uring NFS3 model probes
skipped because the scratch filesystem lacks name_to_handle_at support.
Python/shell syntax and git diff whitespace checks passed.

Logs:
- `/tmp/chimera-compounds-fifth-build.log`
- `/tmp/chimera-compounds-fifth-cursor-build.log`
- `/tmp/chimera-smb-copy-identity-build.log`
- `/tmp/chimera-compounds-fifth-final-boundaries.log`
- `/tmp/chimera-compounds-close-group-tests.log`
- `/tmp/chimera-anonymous-view-final-ctest.log`
- `/tmp/chimera-smb-copy-identity-ioctl.log`
- `/tmp/chimera-compounds-fifth-final-tests.log`

No production changes followed the broad run. KVM tests and real backend
transaction rollback remain outside this pass.
