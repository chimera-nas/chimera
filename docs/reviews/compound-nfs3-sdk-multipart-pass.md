# NFSv3, SDK/POSIX, and multipart compound follow-up

2026-09-23, uncommitted `compounds-refinement` worktree. This pass implements the
three follow-ups from the remaining-conversion audit. Three agents made source
changes only; the primary agent integrated shared VFS support, built the combined
tree, corrected integration errors, and owns the runtime validation below.

## Changes

- All 21 non-NULL NFSv3 filesystem procedures now submit one compound for their
  filesystem sequence. The fifteen newly converted procedures are SETATTR,
  LOOKUP, READ, WRITE, CREATE, MKDIR, SYMLINK, MKNOD, REMOVE, RMDIR, RENAME, LINK,
  READDIR, READDIRPLUS, and COMMIT. Protocol-only admission errors may reply
  without entering the VFS. ACCESS still uses its pure local permission helper.
- NFSv3 EXCLUSIVE verifier checks and SETATTR guards are operation callbacks.
  READDIR/PLUS marshal into a private, resettable RPC reply arena, preserving
  their wire budgets rather than imposing the generic 512-entry limit. Payload
  disposal and replies happen after completion. Finish EAGAIN retries are bounded
  to eight retries (nine attempts); exhaustion maps to NFS3ERR_JUKEBOX. Ordinary
  operation EAGAIN does not cause a successful prefix to be replayed.
- VFS compounds support per-operation object/before/after result masks and
  preserve supplied error-path attributes. This retains NFSv3 weak cache
  consistency data without changing existing callers' default masks. Auxiliary
  snapshots strip callback-scoped ACL pointers. READDIR passes through dot-entry
  flags. REMOVE can carry a resolved child FH for lease and silly-rename handling.
  OPEN-at preserves full-path input on path-based backends and current-directory
  handle semantics.
- SDK writev/writerv, read_into, readdir, open_at/mkdir_at/remove_at, lsetattr,
  and getacl now use compounds. Real-dirfd POSIX fchmodat/fchownat and utimensat
  also use compounds. The previous real-dirfd chmod path cleared the requested
  mode; the replacement retains it across OPEN and SETATTR. Direct ordinary
  filesystem dispatches no longer remain in these frontend directories; mount,
  administration, handle lifecycle, and lock/claim management remain separate.
- SDK read_into reads into private buffers and copies only accepted bytes into
  application destinations. This deliberately replaces direct destination/RDMA
  placement with a copy. SDK READDIR stages at most 512 entries per call, invokes
  application callbacks only after acceptance, and resumes after the last entry
  delivered when an application stops early. EOF requires delivery of all staged
  entries plus backend EOF.
- Multipart assembly no longer uses destructive MOVE_RANGE. Native COPY_RANGE
  and buffered READ/WRITE preserve uploaded parts through assembly and final
  publication, so a failed publication can retry from intact inputs. Existing
  bounded transfer compounds remain.
- S3 metadata reads/copies follow LISTXATTRS continuation cookies. Values and tag
  counts survive later pages, and dependent operations are appended after the
  final page. ERANGE grows non-paginating list requests from 16 KiB to a bounded
  1 MiB. Malformed names and nonprogressing continuation cookies fail instead of
  silently truncating the list. Existing 256-item metadata and compound limits
  remain.

## Validation

The final combined Debug/AddressSanitizer build passes (`-j12`; leak detection
was disabled for the runs). Final validation covered **543 distinct CTest cases:
527 passed, 16 skipped, zero failed**. Nine new focused regressions passed, then
518 of the 534 broader matrix cases passed. Twelve skips require a scratch
filesystem supporting `name_to_handle_at`; four fchownat cases intentionally
skip non-root credentials. These skips do not count as passes.

The broader matrix includes 223 client tests, 180 POSIX cases (including model
batches), native NFSv3 TCP/RDMA/GSS models, all eight S3 model tests, S3 boto3
operations, and existing VFS/FUSE/NFSv4 regression coverage. The SMB-backed POSIX
model passes all 53 traces after the handle-identity correction. Existing model
capability stops/deviation classifications remain; a passing batch is not a claim
of exhaustive protocol conformance.

Final evidence:

- `/tmp/chimera-conversion-build7.log`
- `/tmp/chimera-conversion-focused2.log` (9/9)
- `/tmp/chimera-conversion-matrix2.log` (518 passed, 16 skipped)
- `/tmp/chimera-conversion-final-LastTest.log` (detailed matrix output)
- `/tmp/chimera-conversion-final-dispatch-audit.json` (source inventory)

`git diff --check` passes. The final source inventory was rechecked after runtime
validation. All changes remain uncommitted.

New regressions cover real NFSv3 RPC/compound boundaries, EXCLUSIVE CREATE,
guarded SETATTR, READ payloads, READDIR/PLUS replay and FH results, finish-error
publication suppression, and bounded retry on memfs/diskfs/cairn. SDK regressions
cover vectored payloads, segmented and short reads, untouched destination tails,
zero-length reads, rejected-finish output suppression, and 600-entry directory
paging/early stop/dot entries. POSIX regressions cover real-dirfd metadata updates.
S3 tests force final multipart publication to fail, retry the same upload, and
compare every resulting byte on memfs and cairn. A focused metadata callback test
covers pagination, source binding, oversized lists, and continuation ordering.

Fault injectors are test-only. They reject read-only attempts (or a guard-vetoed
mutation); they do not implement or claim filesystem rollback.

## Completion assessment

The agent handoffs were not sufficient on their own: the combined build found
an incorrect READ ownership API call and a missing fixture header. Integration
also exposed live-handle identity loss in descriptor-relative SDK/POSIX calls.
The SMB-backed POSIX model found 18 errno mismatches across 11 traces: reopening
an unlinked non-directory descriptor by path answered ENOENT instead of ENOTDIR.
Binding the original handle directly to the compound operations fixed the suite.
A subsequent source audit found overlong names mapped to EINVAL during compound
construction. Builders now preserve ENAMETOOLONG; construction errors also survive
finish rejection/retry rather than allowing a shortened request to succeed.

After these corrections, the assigned conversion scope is complete:

| Assignment | Source assessment | Behavioral evidence |
|---|---|---|
| Multipart | No MOVE_RANGE assembly remains; all filesystem work stays compound-routed and failed publication preserves the upload inputs | Native-copy and buffered-path publication-failure/retry byte comparisons; multipart model suites |
| NFSv3 | Exactly 21 compound submission sites, one for each non-NULL filesystem RPC; no direct filesystem dispatch remains in handlers | Real RPC span/retry fixture on three backends; full native TCP/RDMA/GSS model suites and client regressions |
| SDK/POSIX | All specifically identified alternate entry points now dispatch compounds; ordinary direct filesystem calls are absent | SDK buffer/callback rejection tests; descriptor-relative tests; direct, NFS3, NFS4 and SMB-backed POSIX model suites |

This is an assessment of the requested assignments, not a claim that every
frontend in Chimera is converted. The source scan separately classifies pure
permission/status helpers, administration, handle lifetime, and lock/claim state.
Metadata helpers append operations to their caller's compound rather than
submitting a separate compound per helper. Temporary audit detail is retained in
`/tmp/chimera-conversion-final-dispatch-audit.json`.

## Remaining boundaries

Follow-up: [SDK/S3 retry and NFSv4 adoption](compound-sdk-s3-nfs4-followup.md)
implements the universal SDK/S3 finish-retry policy described as absent below.

This pass does not implement backend transaction hooks, rollback, or deferred VFS
cache/notification publication. NFSv3 now has a bounded finish-retry adapter;
SDK/POSIX and S3 still report finish failures through their existing completion
paths rather than supplying a universal automatic retry policy. Their new
publication staging keeps rejected SDK reads private.

The prior audit's SMB conversion, NFSv4 runtime fallbacks, optional REST debug
filesystem endpoints, and lock/lifecycle integration remain separate work. S3
large transfers intentionally use multiple bounded compounds; nondestructive
multipart assembly does not make an entire multi-compound request atomic.
