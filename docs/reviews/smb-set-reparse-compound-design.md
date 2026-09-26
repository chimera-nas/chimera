# SET_REPARSE_POINT: identity migration prerequisite

Status: wave9 correctness hardening, **not a completed compound conversion**.
GET_REPARSE_POINT remains compound-native. SET_REPARSE_POINT remains an explicit
legacy boundary. No unused compound descriptor or shared runtime migration API
has been added.

## Why a mechanical conversion is unsafe

The current implementation removes the opened object's directory entry, creates
a symlink/device/FIFO/socket at the same name, opens the new inode, then swaps
`open->handle`. This changes file identity, rather than just attributes.

The existing successful legacy path does not migrate `open->access_owner`,
`share_file_state`, RANGE ownership, or namespace participant identity. Those
still describe the removed inode while the handle describes the replacement.
Its DOC reservation and cached-handle state need their own migration. These are
known correctness issues still present after this wave's narrower hardening.

Compounds make the mismatch more visible: adjacent commands are constructed
using the old handle and ACCESS owner. The runtime has a producer handle slot
for a new CREATE, but no command-ordered identity replacement for an existing
FileId. Updating the public open from a prepare/complete callback would publish
before acceptance; modifying all constructed operations would also retarget
commands that precede the replacement.

A standalone SET compound is insufficient on its own. Another request can pin
the same SMB open while SET executes. Existing compound states borrow the
public ACCESS owner, and legacy handlers read the open's handle after resolving
the FileId. Putting the old owner/handle during accepted replacement can leave
those requests with dangling references or a handle and claim from different
identities. A single reference-count check is not exclusive admission.

## Landed bounded hardening

* Snapshot and retain the source handle through asynchronous parent open and
  deletion. Use the strict `remove_at_match_fh_flags` primitive, which requires
  `CHIMERA_VFS_CAP_REMOVE_MATCH_FH`. Unsupported backends fail with NOT_SUPPORTED
  before unlink. The parent primitive independently enforces its capability.
* If the source name now identifies another inode, return OBJECT_NAME_NOT_FOUND
  without creating or removing anything at that name. A lookup followed by an
  unconditional unlink is not a substitute for the backend identity check.
* Reject unsupported NFS special-file types before any filesystem operation.
  Previously the default case ran after the original entry was removed.
* Missing, empty, or oversized replacement FH and failure to reopen the new
  inode return INTERNAL_ERROR. They no longer report success with the orphaned
  original handle. The completed filesystem prefix is not rolled back.
* Rebinding checks the logical CLOSE cutoff under the open bucket and file
  locks. A CLOSE that already won is not undone by installing a new handle.

The existing IOCTL wire fixture now covers a stale opened inode whose pathname
was replaced by rename, preservation of the replacement's bytes, unsupported
NFS type content preservation and independent READ continuation, and real
successful symlink creation followed by injected missing-FH/reopen failures.
The fault cases explicitly verify that the new symlink still exists; no
mutation is followed by a fictitious finish rejection or rollback.

## Required bounded conversion contract

1. **Exclusive FileId identity admission.** Add a lifetime-aware rebind gate
   that prevents new identity consumers from pinning the old generation while
   the gate drains existing consumers. Waiting must hold no namespace/ACCESS
   fence whose release those consumers need. Cache acknowledgments, logical
   CLOSE, disconnect, and shutdown must still progress. Teardown cancels the
   migration or suppresses publication without reviving the open. Alternatively
   a complete versioned-open design can retain old handles/owners and bind every
   consumer to its generation; merely refcounting the old token is insufficient
   for legacy handlers reading mutable public fields.
2. **Start with a precise scope.** Initially support a live ordinary regular
   placeholder with no DOC, RANGE locks, cache grant, streams, durable state,
   or peer-open aliases. Recheck those restrictions under the relevant gates
   immediately before mutation. Unsupported cases defer before filesystem
   effects. An initial standalone compound is acceptable; it avoids introducing
   command-ordered existing-state rebinding at the same time.
3. **Typed replacement sequence.** Preallocate all frontend records, snapshot
   the exact source FH and parent/name, and acquire namespace plus ACCESS
   admission fences without partial-set waits. Run typed parent open, strict
   matched REMOVE, typed CREATE symlink/mknod, OPEN of the returned FH, new
   RESERVE_ACCESS, then old-owner retirement. New admission must be ready before
   retiring the old claim. Any partial prefix failure preserves the actual
   filesystem outcome and emits only the events for completed mutations.
   REMOVE followed by CREATE is not an atomic replace until backend transaction
   support exists; a competing creator may occupy the name between them.
4. **Typed notification control (VFS prerequisite implemented in wave16).**
   Name-based typed CREATE forwards `namespace_flags` for mkdir, symlink and
   mknod. NO_NOTIFY suppresses both ordinary and synchronous observer publication
   while preserving authorization and cache updates. The old standalone APIs
   retain their notification behavior. The future SMB conversion must select
   these flags and publish removal/addition after acceptance in execution order,
   including a removal-only prefix if creation failed. This does not implement
   the identity migration or make SET_REPARSE_POINT compound-native.
5. **Accepted identity migration.** Transfer the new handle and canonical
   ACCESS owner/file-state together; migrate the namespace participant identity
   using preallocated storage under the covering guard. Release the old identity
   only when all allowed consumers have drained. Publication must allocate
   nothing, perform no filesystem I/O, and remain safe if the open was logically
   closed or its tree/session retired. Failure paths release attempt-owned new
   resources without deleting successfully created filesystem objects.
6. **Later coalescing.** Add an existing-open identity overlay keyed by SMB
   state, with an execution-ordered `handle_from`/actor/ACCESS generation. Later
   commands consume the new generation; preceding commands keep their original
   results and identities. Reset restores the initial public generation and
   drops attempt resources before recalls can run again. DOC/range/cache/stream
   support follows only after their own migration contracts are implemented.

## Required future verification

Beyond basic SET+GET/query coalescing, test another request held on the old
identity, competing same-FileId operations, CLOSE/TDIS during replacement,
namespace/name rebinding, new ACCESS admission failure after CREATE, allocation
failure before mutation, and cancellation after each irreversible prefix.
Finish retry tests may reject readonly/pre-mutation attempts; completed mutation
retry needs genuine backend transaction rollback support.

## Wave18 implementation (pending integrated validation)

`SET_REPARSE_POINT` now takes a bucket-protected exclusive identity token before
asynchronous work. Admission requires exactly the tree reference plus the SET
resolver reference. The bucket lock makes that observation atomic with blocking
new normal/native FileId consumers; a busy request returns FILE_NOT_AVAILABLE
without retaining partial fences or waiting. Direct CLOSE, lease-key lookup and
CREATE replay/AppInstance discovery also honor the token. The AppInstance scan
can take its temporary reference under the claim lock, but must validate the
bucket-protected token after dropping that lock before using the old identity.
TDIS/disconnect remain logical cutoffs. Request-owned session/tree pins and a
credential snapshot keep delayed callbacks valid through wire cleanup. Context
pins are acquired before the first FileId resolution, because the open reference
alone does not preserve its tree's bucket storage. A late CANCEL is remembered
on the request even when the VFS is already finishing; a read-only nonregular
classification therefore cannot begin the destructive legacy fallback after
cancellation or a logical teardown cutoff.

The bounded typed path covers an ordinary regular placeholder without DOC,
streams, cache identity, durable/persistent/resilient state, range ownership,
notify state or peer namespace/ACCESS owners. Device CHR/BLK requests from a
non-root credential now fail ACCESS_DENIED in common preflight, before REMOVE on
either route; previously VFS rejected the later mknod only after deletion.
A cached VFS implicit-I/O ACCESS
row is not a protocol opener and does not exclude this path. It acquires the
namespace constructor writer token and source DOC/ACCESS fences before mutation;
existing foreign constructors cause a nonblocking failure. Its single standalone
compound is:

1. GETATTR regular-file classification, then parent PUTFH.
2. Atomic exact-FH REMOVE with NO_NOTIFY.
3. Typed SYMLINK or NODE CREATE with NO_NOTIFY (symlink, character/block device,
   FIFO and socket all use the same identity migration).
4. OPEN_CURRENT and owning GETHANDLE.
5. Repeatable explicit coordination acquiring the new identity's DOC/ACCESS
   fences, rejecting unexpected protocol owners.
6. RESERVE_ACCESS with a private canonical owner, RETIRE_ACCESS of the source,
   and a readiness checkpoint.

The new reservation remains compound-owned until accepted readiness; failed
suffixes leave it untransferred for compound cleanup. This standalone group does
not use `reserve_access_until`: that API withdraws reservations on group EINTR,
whereas cancellation after REMOVE must preserve a completed migration suffix.
The cancellation scope
starts at successful REMOVE and ends at readiness. A late cancellation cannot
leave a successfully migrated replacement with an unretired old claim; once the
whole suffix completes, SET reports success. Ordinary errors still terminate the
suffix and preserve irreversible filesystem prefixes. After accepted finish,
namespace FH, VFS handle, canonical ACCESS owner and file-state reference change
under registry/bucket/file locks without allocation or admission. Old owner
storage is put only after the compound drains its retirement journal. A CLOSED
open is never rebound. Notifications reflect accepted REMOVE/CREATE prefixes,
including CREATE that physically succeeded but lacked a usable returned FH.

The fixture extends `smb2_ioctl_compound_probe` with canonical owner/handle/file
and namespace-membership inspection before and after acceptance, read-only
finish-EAGAIN retry, held finish with new consumer/CLOSE rejection, an existing
consumer winning admission first, wire CANCEL before and after mutation,
FIFO/socket migration and CHR/BLK preflight denial preserving the placeholder,
related CREATE→SET→GET, and TDIS/disconnect during delayed CREATE
completion. Existing stale-path, unsupported-type, missing-FH and reopen-error
cases remain; failures never claim rollback and mutating finishes are accepted.
No tests or builds were run by the implementing agent; root integration owns
validation.

Remaining boundaries are explicit: this is a standalone VFS compound, not a
coalesced command with an existing-open identity overlay. Excluded state and
nonregular source types retain the gated legacy callback chain, whose successful
handle-only rebind still lacks claim/namespace migration. Cross-protocol
filesystem mutation between strict REMOVE and CREATE is not made transactional;
subsequent failure can leave the original open referring to its removed inode,
with its original claim and namespace identity, while the path is absent or
holds the physically created replacement. There is no synthetic rollback or
promise of repair in this patch. Broadening DOC, ranges, cache, streams or
durable state requires their corresponding identity/publication journals.

Privileged CHR/BLK creation shares the typed NODE builder, but successful device
migration is not asserted by the anonymous wire fixture. Its CHR/BLK cases
exercise the actual credential denial and prove that the placeholder survives.
