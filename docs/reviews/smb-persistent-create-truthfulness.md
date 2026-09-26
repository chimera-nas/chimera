# Persistent CREATE record proof

Wave17 validated checkpoint: full Debug+ASan build and all 52 selected CTest
entries pass; see [combined validation](smb-compound-current-status.md#validation).
This fixes false persistence claims. It does not finish persistent directory or
stream CREATE conversion or cold-recovery identity restoration.

## Corrected behavior

A persistent DH2Q grant now requires successful record storage, independently of
ID reservation and descriptor serialization. The successful handle-state OPEN_AT
continuation captures the record's backend FH and marks its storage proof. Only
that proof permits PERSISTENT in the wire grant, PERSISTED on the live open and a
persistent durable-registry entry.

Fresh directory MKDIR, existing-directory OPEN_IF through the MKDIR collision
branch, and named streams on file or directory bases currently bypass record
storage. They now decline PERSISTENT. Where a real BATCH/HANDLE cache grant makes
an ordinary durable handle eligible, they retain that ordinary durability and
warm reconnect; otherwise they decline the durable context. Regular-file and
existing-directory FILE_OPEN routes that actually write records retain persistence.

Default-KV handle-state persistence moves after acquired-handle type/access
checks. PUT failure now returns the error and releases the acquired handle; it
cannot report a successful persistent open. The filesystem prefix remains intact.
The VFS descriptor's r_created output records that prefix independently of storage
proof, allowing SMB to emit its suppressed creation event even on record failure.
The descriptor contract changes the module SDK to version 5.

A successful record can precede SMB share/type/access admission. Failed legacy
CREATE completion now deletes an unpublished record with a typed FH/DELETE_KEY
compound before sending its original error. A saved route also owns cleanup after
an ambiguous failed PUT; it is not proof of successful storage. Cleanup clears its
route/proof before dispatch and uses the frontend retry adapter. Existing failed
public-open cleanup clears the same fields so it cannot run this deletion twice.
Request allocation initializes the new discriminator fields before parse/control
errors can inspect a recycled request union. Allocation and legacy dispatch also
clear the reply-open pointer: a stale successful open otherwise bypassed failed
record cleanup. Storage EIO maps explicitly to STATUS_IO_DEVICE_ERROR. Accepted cleanup never unlinks the
created file. Allocation/deletion errors still need the previously documented
persistent repair/tombstone guarantee.

## Added coverage

The existing CREATE cache probe now inspects the actual KV record and live/registry
flags at completion, in addition to the wire response. Cases cover uncached and
cached fresh directories, existing-directory OPEN_IF, streams on files and streams
on directories; ordinary durable warm reconnect with original FileId and stream
data; explicit persistent reconnect rejection for downgraded ordinary durability;
base ACCESS-owner drainage; genuine persisted regular and FILE_OPEN directory
controls; PUT failure before storage; actual successful PUT followed by reported
EIO; and successful storage followed by SMB sharing denial. Failed PUT cases leave
the created name accessible through a subsequent exclusive open. No simulated
finish rejection follows filesystem or KV mutation.

The VFS compound-groups test also checks that an existing mode-000 file opened
by an unprivileged credential fails the post-open DAC check without storing its
handle-state key, and resets a reused descriptor's creation-outcome field.

## Remaining persistence work

The cold registry reconstructs only a limited record, including basename and
client/create identity. Cold claim removes that entry and asks CREATE processing
to reopen using request fields; it does not restore exact file/base/stream FHs,
share-relative namespace identity or the full access/lease lifecycle. The cold
reconnect retains its forced persistent ID, but its DH2C context does not satisfy
the fresh DH2Q persistence/grant path. This is not complete cold recovery, and
adding stream records alone would not make it so.

The generic open_fh_hs API also has a separate audited gap: cached/inferred opens
can skip handle-state persistence. SMB does not call that API in the paths changed
here. It must be repaired and tested before using it to add fresh-directory
persistence; this change deliberately does not route MKDIR through it.
