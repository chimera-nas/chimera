# Compound frontend refinement

Work performed on `compounds-refinement`, based on `b54b6b32`, following the
user's authorization to implement three parallel workstreams. Changes remain
uncommitted. The original findings are in
[the conversion review](compound-frontends-b54b6b32.md); this report does not
supersede findings outside the implemented scope.

## Implementation

### Completion cleanup

Straightforward NFS3, FUSE, and SDK completion adapters now consume compound
results directly instead of tail-calling the old per-operation completion
function. Public SDK callback signatures are preserved. Complex and shared
paths were left alone. NFS3 ACCESS retains the compound through ACL evaluation;
scalar-only consumers can copy their results before releasing it.

### VFS execution and OPEN

The VFS now has operation-specific prepare and complete callbacks, conditional
skip, argument binding from prior results, an attempt reset callback, and
dynamic suffix construction. Submitted operations are snapshotted, so retries
restore their inputs and rebuild the suffix. Callouts may change private
attempt state; they may not publish replies, protocol state, or consume HTTP
input. The header documents borrowed input and owned result lifetimes.

Execution status and finish status are separate. A finish handler can defer
acceptance, with immediate acceptance as the default. Rejected finish takes
precedence even if execution stopped on an operation error. This is a seam for
later backend integration, not a backend transaction implementation. Retrying
mutations is safe only after the backend has aborted their effects.

Other prerequisite fixes include stable growable operation storage, a
trampoline for synchronous completion, independent GETHANDLE references,
borrowed CLOSE release after acceptance, owned primary-attribute ACL snapshots,
separate OPEN input/applied attributes, and 64-bit write completion counts.

For a nondelegated, nonexclusive NFSv4.1 OPEN on an owner with no existing open
states, protocol state is reserved before execution and published after
acceptance. VFS share admission and conditional truncate run inside the same
compound. Supported dependent GETATTR, GETFH, current-stateid READ/WRITE, and
COMMIT operations can follow OPEN in that compound. Client/owner/state pins
protect both unpublished reservations and final publication.

A focused pynfs CSID3 trace confirms LOOKUP, OPEN, GETFH, and WRITE share one
VFS submission (`wire first=3 count=4 vfs_ops=8 reserved_open=1`). The following
CLOSE still uses the fallback dispatcher. This removes an important OPEN
boundary without claiming that all NFS wire compounds are now one VFS compound.

NFS finish rejection is checked before result publication. Finish-time EAGAIN
retries a pristine attempt with a bounded retry count. READDIR encoding resets
to the attempt's original buffer mark. Validation fixes also cover leading
PUTFH read-only policy, READ/WRITE grace checks, malformed SETATTR, and
VERIFY/NVERIFY masks; ACL VERIFY uses its faithful fallback path. Final guards
check explicit stateids against the current filehandle, retain fallback before
unsafe authorization across cursor moves, and prevent range mutations following
a late LOCKT denial.

### S3

All S3 filesystem execution now goes through compounds. This includes
GET/HEAD/PUT, copy and multipart, listing, buckets, ACLs, tags, and batch delete.
ListBuckets remains an in-memory map operation. Metadata helpers append to
their caller's compound rather than submitting independent chains.

Small GET/PUT objects use one compound. The threshold follows configured
`io_size`, defaults to 128 KiB, and is capped at 1 MiB. Larger requests resolve
and open with the first transfer, retain accepted handles, and use subsequent
bounded transfer compounds. PUT retains immutable decoded bytes through each
attempt and publishes its temporary object in the final compound. Publication
of HTTP output and bucket/upload-table changes occurs after acceptance.

Integration and cross-review fixes include max-1000 batch-delete stack growth,
named OPEN cursor propagation, bucket existence/error mapping, multipart
Abort/Complete reference accounting, and cleanup compounds for failed or
abandoned named scratch files. The old DeleteBucket nonempty-500 deviation was
retired; the response is now 409/BucketNotEmpty.

See [the S3 implementation note](s3-compound-conversion.md) for operation
boundaries, retained input/output lifetimes, and transport limitations.

## Validation

The build uses Debug+ASan in `/tmp/chimera-compounds-build`, pinned submodules,
and `/opt/s3-tests/.venv/bin/python`. Integration tests run outside the sandbox
for network namespaces, FUSE, and io_uring. The final broad run selected 316
CTest entries: 314 passed, zero failed, and two skipped for missing scratch
filesystem support for `name_to_handle_at` (Linux/io_uring NFS3 model probes).

| Coverage | Result |
| --- | --- |
| SDK, including NFS3-backed variants | 224/224 passed |
| FUSE, including actual mounts | 8/8 passed |
| S3 boto V4/V2 across configured backends | 57/57 passed |
| Ceph S3 suite | 214 internal cases passed |
| S3 in-process model probe | Passed |
| pynfs combined NFSv4.0/4.1/4.2 | All three suites passed |
| NFS state/protocol unit tests | 13/13 passed |
| NFS3 model probes | memfs/diskfs/cairn passed; Linux/io_uring skipped |
| VFS compound, ACL, claim and enforcement | 4/4 passed |

The pynfs suites reported 511, 137 and 152 passing cases respectively, with
unsupported/not-selected cases skipped by their configured suites. The CSID3
mapping trace was also run separately and passed. The final targeted NFS/core
rerun after the last stateid/ordering guards passed all 20 tests, including
all three pynfs suites (`/tmp/chimera-compounds-final-nfs.log`).

Broad-run output: `/tmp/chimera-compounds-final-tests.log`; OPEN trace:
`/tmp/chimera-compounds-open-mapping.log`. Build and formatting checks passed.

Added tests cover transfer threshold edges, zero-length objects, range reads,
overwrite truncation, metadata and tags, ACL changes, 1000-key mixed-result
deletion, bucket removal with more than 1024 empty directories, and malformed
streaming-upload scratch cleanup. VFS tests inject finish-time EAGAIN after
successful execution and after a failed-op prefix, check delayed ownership
transfer, and exercise borrowed CLOSE through retry. NFS state tests cover
unpublished lookup, competing admission, acceptance, abort, lease expiry, and
deferred client destruction.

## Remaining limits

- NFSv4.0 OPEN, owner coalescing, exclusive OPEN, delegations, multiple OPENs,
  CLOSE, and unsupported suffix operations still have fallback boundaries.
  General request-wide NFS compound mapping needs a broader private state
  overlay and replay/delegation coordination.
- Backend begin/end hooks, transaction association on backend requests,
  rollback, and production conflict retries remain follow-up work. Injected
  read-only finish failures validate the contract, not a real backend rollback.
- Large S3 transfers deliberately span accepted compounds. A permanent late
  GET failure cannot change already-sent headers, and libevpl lacks a public
  stream-abort/reset operation to reliably terminate that incomplete response.
- Existing empty-leaf representation of trailing-slash S3 keys remains
  backend dependent; this pass does not introduce a new on-disk key encoding.
- SMB2/REST conversion, complex FUSE READDIRPLUS grant publication, and other
  unresolved items from the original review are outside these three workstreams.
- KVM tests were not enabled because the image-fetch tool `oras` is absent.
  The external model-trace bundle was disabled; available in-process probes
  were used instead.
