# S3 compound conversion implementation note

This records the S3 refinement following the review of `b54b6b32`. Backend
transactions and automatic transaction retries remain follow-up work. A VFS
compound groups the filesystem sequence today; it does not yet roll back
filesystem mutations when an operation fails.

## Request boundaries

`src/server/s3/s3_compound.h` builds the common request prefix: shared-root
PUTFH followed by bucket LOOKUP_PATH under the authenticated request credential.
`s3.c` copies the configured bucket path while holding the bucket-map lock, then
releases that lock before submission. Bucket lookup is part of the operation's
compound, so an earlier independent lookup cannot escape a future transaction.
The common error helper retains the distinction between a missing backing
bucket and a missing object.

| Request | Filesystem compound boundary |
| --- | --- |
| GET / HEAD object | Bucket and object lookup, validation, open, metadata enumeration/read, and first data read; HEAD omits data. |
| PUT object | Bucket lookup, parent creation/open, temporary object creation, metadata/tags, and first write. A small object's compound also publishes the object. |
| GetObjectAttributes / DeleteObject | Bucket lookup and the required object lookup or removal. |
| ACL / tagging | Bucket/object lookup plus mode or xattr operations. Listing xattrs can append dependent get/remove/set operations. |
| ListObjects / ListObjectVersions | Bucket lookup plus FIND. Collection is attempt-private; sorting, pagination, and XML emission follow acceptance. |
| CreateBucket | Bucket-root lookup and directory creation. The bucket map is updated after acceptance. |
| DeleteBucket | Lookup, FIND, removal of empty descendant directories through REMOVE_PATHS, and root removal. A nonempty bucket fails before removal; map removal follows acceptance. |
| HeadBucket | Bucket lookup verifies the backing path before the empty successful response. |
| DeleteObjects | One bucket lookup and up to 1000 removals. Per-key errors are recorded privately and normalized so later keys still execute. An aggregate failure remains a failed request. Trailing-slash keys use path-and-leaf removal so the marker is removed without deleting descendants. |
| CopyObject | Source/destination resolution, temporary destination creation, and the first transfer. Metadata and publication join the final transfer; clone can cover the whole object at once. |
| UploadPart / UploadPartCopy | Initial resolution/open/create and first transfer, followed by transfer compounds as needed. The multipart registry is updated after the final accepted transfer. |
| CompleteMultipartUpload | Assembly through transfer compounds, followed by metadata and object publication in the final compound. Cleanup of old parts also uses compounds. |
| Multipart create/abort/list controls | Bucket-validation compound before in-memory upload-table access or mutation and response emission. |
| ListBuckets | In-memory bucket-map enumeration; there is no backing-filesystem sequence to submit. |

The implementation is in `src/server/s3/s3_{get,put,metadata,bucket,list,delete,
delete_objects,acl,tagging,copy,multipart}.c`. Metadata append APIs replace the
former independently completing metadata callback chains.

## Streaming and retained payloads

The configured S3 `io_size` is the transfer/retry unit, with a 128 KiB default
and fallback for zero, capped at 1 MiB. GET/PUT objects up to that limit fit in
one compound, including zero-length objects. Larger GETs retain an accepted
open handle and issue subsequent bounded read compounds. Larger PUTs create
and write the first piece in the initial compound, issue subsequent write
compounds, and publish the completed temporary object in the final compound.
Copy and multipart transfer paths use the same bounded read/write unit while
retaining backend clone/copy/move optimizations. Failed or abandoned named
scratch files are removed through independent cleanup compounds with owned
credentials and copied inputs; cleanup does not retain the HTTP request.

PUT and UploadPart retain an owned decoded body buffer until the corresponding
compound is accepted. One extra byte distinguishes an exactly-limit object
from a larger stream without trusting Content-Length. HTTP consumption and
chunk decoding happen outside replayable callouts; an attempt always sees the
same retained decoded bytes and offset. The buffer advances only after
acceptance. Request references keep body storage, metadata inputs, and private
contexts alive through asynchronous completion and client disconnects.

GET transfers read iovec ownership to HTTP only after aggregate success. The
initial compound finishes before response headers or its first data are
published. Subsequent accepted chunks are irreversible output boundaries:
retrying a later chunk must not resend earlier accepted chunks.

## Callouts and future backend retries

Per-operation prepare/complete callbacks inspect results, validate types and
ranges, select backend capabilities, bind dependent arguments, append dependent
operations, and update private attempt state. They do not send HTTP output,
consume request input, publish bucket/upload-table changes, or advance accepted
stream offsets. FIND and xattr collectors reset their scratch state for each
attempt. Submitted inputs and retained payloads remain available until final
acceptance; dynamically appended suffixes are rebuilt after reset.

Final callbacks check the compound aggregate status before emitting protocol
results or transferring handles/iovecs. A later backend integration must finish
or abort the backend transaction before reaching that publication point.
Finish-time EAGAIN requires backend abort followed by
`chimera_vfs_compound_retry`, which restores submitted operation inputs,
discards dynamic suffixes, releases attempt results, and resets private state.
The retry API alone cannot undo filesystem effects. Ordinary operation errors
must preserve the intended successful-prefix/per-key semantics, rather than
being treated automatically as transaction conflicts.

Large requests deliberately span multiple accepted compounds; this work does
not promise a single backend transaction or snapshot across an entire large
transfer. PUT retains temporary-object publication semantics, but already
accepted transfer compounds cannot be rolled back by retrying a later chunk.

## Remaining transport limitation

An initial GET read failure is reported before HTTP headers. A failure in a
later large-GET chunk happens after fixed Content-Length and earlier bytes have
been sent. The current libevpl public HTTP interface exposes no request-abort
or stream-reset operation: the handler can release its handle and stop, but
cannot send a replacement status or reliably terminate that incomplete body.
Transport cancellation support is needed to handle this path completely.
Backend conflict retries, when added, must occur before publishing the failed
chunk. They cannot retract previously accepted HTTP bytes.

Validation results are maintained in the main refinement report; this note
describes the implementation and its intended retry contract.
