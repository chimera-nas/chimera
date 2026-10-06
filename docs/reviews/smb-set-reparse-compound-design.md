<!--
SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
SPDX-License-Identifier: Unlicense
-->

# SET_REPARSE_POINT identity replacement

Status after the October 5 refinement: ordinary regular placeholders use a
standalone VFS compound with accepted identity migration. Unsupported stateful
opens and nonregular source objects return NOT_SUPPORTED **before unlink**.
The former handle-only fallback has been removed; it cannot safely preserve
ACCESS/RANGE ownership, namespace membership, cache identity or durable state.

## Supported contract

The replacement requires a live ordinary regular placeholder with a canonical
ACCESS owner and namespace participant, on a backend with REMOVE_MATCH_FH.
Memfs, diskfs and cairn provide that capability. Diskfs compares the complete
child FH under its namespace transaction locks; cairn validates the comparison
through its existing RocksDB transaction before completing a match or no-op.
This uses their existing operation transactions, not backend VFS-compound
transaction support. Linux/io_uring passthrough and proxy backends still lack
the required identity-conditional unlink; a lookup followed by an ordinary
unlink cannot safely substitute for it.

DOC, streams, durable/persistent/resilient state, RANGE ownership, cache grants,
notify state and peer namespace/ACCESS owners exclude this path. Unsupported
NFS special-file types and unprivileged CHR/BLK creation fail during preflight.

A bucket-protected identity token admits exactly the tree reference and SET's
resolver reference. New FileId consumers cannot pin the old generation while
SET owns that token. Existing consumers cause FILE_NOT_AVAILABLE without a
partial wait. Request context pins protect the tree/session storage throughout
asynchronous completion. TDIS and disconnect remain logical cutoffs.

The sequence holds namespace and ACCESS admission fences and executes:

1. GETATTR of the pinned source, requiring a regular file; parent PUTFH.
2. Exact-FH matched REMOVE with NO_NOTIFY.
3. SYMLINK or NODE CREATE with NO_NOTIFY.
4. OPEN_CURRENT and owning GETHANDLE.
5. Repeatable coordination acquiring the new identity's DOC/ACCESS fences.
6. RESERVE_ACCESS for a private canonical owner, RETIRE_ACCESS for the source,
   and a readiness checkpoint.

At accepted readiness, namespace membership, handle/open flags, ACCESS owner
and file-state reference move together under registry/bucket/file locks.
Publication allocates nothing and a CLOSED open is never revived. Old owner
storage is released only after the compound drains its retirement journal.
Notifications describe the accepted REMOVE/CREATE prefix, including a CREATE
that physically succeeded but returned an unusable file handle.

The cancellation scope starts at successful REMOVE and ends at readiness.
Cancellation after mutation cannot abandon a successful migration suffix.
Ordinary errors still terminate execution: without backend transactions,
REMOVE followed by CREATE is not atomic and completed filesystem effects are
not rolled back. A failed suffix can leave the original open attached to its
removed inode while the pathname is absent or contains the new object. The
failure is reported rather than pretending that a complete migration occurred.

## Verification

The IOCTL wire fixture checks source/target FH, canonical owner/file state and
namespace membership before and after acceptance, read-only finish retry,
concurrent consumers, held finish, cancellation before/after mutation,
FIFO/socket migration, device preflight denial, stale path identity, missing
FH/reopen failures, related CREATE -> SET -> GET, and TDIS/disconnect.

The review follow-up adds rejection tests for RANGE, lease, resilient, stream,
DOC and peer-open state. They check that the old FileId and pathname still
identify the original bytes, that a foreign writer still observes the retained
RANGE lock, and that CREATE-time DOC still removes the original object on CLOSE.
A second SET on a nonregular source is rejected and preserves that source.
The identity-scoped REMOVE fixture also runs against diskfs and cairn, checking
that a stale FH preserves the replacement name, reports an unmatched no-op,
and that the current FH permits removal.

## Remaining conversion work

SET remains a standalone boundary. Coalescing needs an execution-ordered
existing-open identity overlay: later commands must consume the new generation
while earlier results retain the old one, and retry must restore the initial
public generation. Expanding support for DOC, ranges, cache, streams or durable
opens needs their corresponding identity/publication journals. No excluded
case may fall back to changing only the handle.
