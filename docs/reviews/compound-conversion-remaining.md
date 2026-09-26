# Remaining frontend compound conversion

Reviewed the current, uncommitted `compounds-refinement` worktree on 2026-09-23,
after the correctness cleanup. This is a source and dispatch-path audit, not a
new runtime test campaign. No production code was changed for this review.
Earlier passing model tests establish behavior on their exercised paths; they
do not establish that those paths executed through VFS compounds.

Implementation follow-up: [NFSv3, SDK/POSIX, and multipart pass](compound-nfs3-sdk-multipart-pass.md)
supersedes the NFSv3/SDK conversion gaps and the two S3 correctness findings below.
The subsequent [SDK/S3 retry and NFSv4 adoption pass](compound-sdk-s3-nfs4-followup.md)
adds universal SDK/S3 retries and removes the proxy-COPY and ordinary root-export
LOOKUP/SECINFO exclusions. This document retains the original audit and its
then-current source anchors.

## Assessment

**Outside SMB, NFSv3 is the largest unconverted frontend.** SDK/POSIX alternate
entry points are the next broad gap. NFSv4 has extensive support but still
converts supported runs rather than necessarily the whole wire compound. S3 and
FUSE ordinary filesystem operations are substantially converted.

| Frontend | Current coverage | Remaining work |
|---|---|---|
| NFSv3 | 6 of 21 non-NULL procedures submit compounds | Convert the other 15, including all mutation and data-I/O procedures |
| NFSv4 | 42 opcodes recognized by the adapter, subject to runtime conditions | Namespace/export transitions, streams, pNFS control/removal, delegation OPEN variants, proxy COPY, fallback elimination |
| Client SDK / POSIX | Main path operations converted | Vectored writes, read-into, readdir, descriptor-relative and no-follow variants, ACL reads, direct POSIX metadata helpers |
| FUSE | Ordinary filesystem handlers use compounds | Lock state/FLUSH lifecycle integration; bootstrap lookup is separate |
| S3 | Filesystem operations are compound-routed | Refine server-side transfer boundaries; fix destructive multipart assembly and metadata pagination |
| SMB | Wire compound dispatcher only; no VFS compound submissions | Essentially the whole filesystem-facing conversion and publication contract |
| REST | Management operations; optional debug filesystem endpoint | Debug unlink/rename/link/chmod remain direct; management calls are separate lifecycle operations |

These are path classifications, not percentages of traffic or complete protocol
conformance. For example, recognizing an NFSv4 opcode does not mean all of its
stateid classes, exports, or argument variants remain in a VFS compound.

## 1. NFSv3: fifteen live procedures remain unconverted

Converted: **GETATTR, ACCESS, READLINK, FSSTAT, FSINFO, PATHCONF**.

Unconverted: **SETATTR, LOOKUP, READ, WRITE, CREATE, MKDIR, SYMLINK, MKNOD,
REMOVE, RMDIR, RENAME, LINK, READDIR, READDIRPLUS, COMMIT**.

These are active implementations, not unused legacy alternatives. Examples:

- READ directly opens the FH and dispatches READ from the open callback:
  [nfs3_proc_read.c:136](/worktrees/compounds/src/server/nfs/nfs3_proc_read.c:136),
  [nfs3_proc_read.c:74](/worktrees/compounds/src/server/nfs/nfs3_proc_read.c:74).
- CREATE directly opens the parent and then calls OPEN_AT:
  [nfs3_proc_create.c:260](/worktrees/compounds/src/server/nfs/nfs3_proc_create.c:260),
  [nfs3_proc_create.c:210](/worktrees/compounds/src/server/nfs/nfs3_proc_create.c:210).
- Guarded SETATTR is OPEN → GETATTR → frontend guard check → SETATTR:
  [nfs3_proc_setattr.c:203](/worktrees/compounds/src/server/nfs/nfs3_proc_setattr.c:203),
  [nfs3_proc_setattr.c:121](/worktrees/compounds/src/server/nfs/nfs3_proc_setattr.c:121).

The existing compound operations cover most required filesystem actions. The
conversion should be one RPC's filesystem sequence per VFS compound, with
protocol checks expressed as operation callbacks. Preserve weak-cache-consistency
attributes on success and failure, EXCLUSIVE CREATE verifier replay, credentials,
and SETATTR guard/error ordering.

There is also real publication work: WRITE currently releases payload and sends
its reply from the operation completion; READDIR and READDIRPLUS construct
results directly in the RPC reply arena during enumeration. Move final replies
and payload disposal after accepted finish, and make enumeration scratch state
resettable. See [nfs3_proc_write.c:36](/worktrees/compounds/src/server/nfs/nfs3_proc_write.c:36)
and [nfs3_proc_readdir.c:33](/worktrees/compounds/src/server/nfs/nfs3_proc_readdir.c:33).

## 2. SDK and POSIX: alternate paths bypass converted main operations

The public SDK still directly dispatches:

| API | Evidence | Conversion consideration |
|---|---|---|
| `writev`, `writerv` | [client_write.h:192](/worktrees/compounds/src/client/client_write.h:192), [client_write.h:212](/worktrees/compounds/src/client/client_write.h:212) | Use the same retained-payload compound path as ordinary write |
| `read_into` | [client_read_into.h:40](/worktrees/compounds/src/client/client_read_into.h:40) | Define destination-buffer ownership across attempts; add compound support or stage a READ and copy after acceptance |
| `readdir` | [client_readdir.h:58](/worktrees/compounds/src/client/client_readdir.h:58) | Stage bounded results and invoke application callbacks only after acceptance, preserving stop/cookie behavior |

`readdir` is particularly important: its backend entry callback directly invokes
an arbitrary application callback at
[client_readdir.h:30](/worktrees/compounds/src/client/client_readdir.h:30).
That callback cannot simply become a replayable compound operation callback.
`read_into` already places bytes in caller-owned memory during execution;
conversion must explicitly define when those bytes become observable rather
than assuming operation-struct reset restores them.

Live POSIX-facing SDK helpers also remain direct:

- `open_at`: [client_open.h:89](/worktrees/compounds/src/client/client_open.h:89),
  reached by `posix_openat.c:37`.
- `mkdir_at`: [client_mkdir.h:84](/worktrees/compounds/src/client/client_mkdir.h:84),
  reached by `posix_mkdirat.c:33`.
- `remove_at`: lookup and remove at
  [client_remove.h:155](/worktrees/compounds/src/client/client_remove.h:155),
  reached by `posix_unlinkat.c:37`.
- `lsetattr`: no-follow open then setattr at
  [client_setattr.h:173](/worktrees/compounds/src/client/client_setattr.h:173),
  reached by `posix_lchown.c:27`.
- ACL reads: lookup/open/getattr at
  [client_getacl.h:138](/worktrees/compounds/src/client/client_getacl.h:138),
  reached by `posix_getacl.c:36`.

The POSIX layer itself bypasses even these helpers in the real-dirfd branches
of [posix_fchmodat.c:95](/worktrees/compounds/src/posix/posix_fchmodat.c:95) and
[posix_fchownat.c:97](/worktrees/compounds/src/posix/posix_fchownat.c:97), and the
optional directory-GETATTR / lookup / open / setattr chain in
[posix_utimensat.c:307](/worktrees/compounds/src/posix/posix_utimensat.c:307).

Public path SETATTR, FSETATTR and STATFS are already converted. The direct
`chimera_dispatch_setattr_at` helper has no callers found in `src`; it is dead
helper cleanup rather than evidence of a live conversion gap. Handle release,
duplication, mount/mkfs and initialization are also not ordinary request gaps.

## 3. NFSv4: substantive remaining boundaries

The dispatcher submits an expressible run, completes it, and returns to legacy
handlers at unsupported boundaries, then tries again:
[nfs4_proc_compound.c:259](/worktrees/compounds/src/server/nfs/nfs4_proc_compound.c:259).
Thus one wire COMPOUND can still produce several independently accepted VFS
compounds plus direct filesystem operations.

### Namespace and export transitions

These are broader than isolated special operations:

- Pseudo-root and synthetic attribute-directory FHs cannot seed an ordinary run.
- Cross-export PUTFH/RESTOREFH split the run; it assumes one resolved export and
  credential context.
- LOOKUPP after a previous cursor move, or from a mount root, falls back.
- **With a `/` root export configured, any attempted run containing LOOKUP or
  LOOKUPP is refused**, even if the particular lookup does not cross a junction.

Evidence: [nfs4_compound_vfs.c:5209](/worktrees/compounds/src/server/nfs/nfs4_compound_vfs.c:5209),
[5274](/worktrees/compounds/src/server/nfs/nfs4_compound_vfs.c:5274),
[5841](/worktrees/compounds/src/server/nfs/nfs4_compound_vfs.c:5841),
[5918](/worktrees/compounds/src/server/nfs/nfs4_compound_vfs.c:5918).

These need execution-time protocol namespace resolution and explicit
export/credential transitions. Merely removing the guards would lose junction,
root-squash or namespace-parent semantics. They are not intrinsically reasons to
end the frontend compound; cross-backend participation still needs a defined
backend policy.

### OPEN and stateid variants

- Delegation claim OPEN forms remain outside the adapter; only CLAIM_NULL,
  CLAIM_FH and CLAIM_PREVIOUS enter it.
- **All v4.0 OPENs fall back when delegations are enabled**, not only OPENs that
  actually receive a delegation. The full owned replay response is now fixed,
  so the old missing-replay-storage explanation is no longer sufficient.
- Nonjournalable/reservation-fallback OPENs can still end a VFS run and execute
  a legacy completion tail afterward, including deferred truncate.
- Delegation/layout stateid classes on operations other than explicitly
  supported session READ/WRITE/READ_PLUS paths still trigger fallback. Even
  metadata-only SETATTR encounters the general stateid eligibility gate.
- After CLOSE, special/anonymous-stateid operations outside the implemented
  private exclusion view also split, including size SETATTR and range work.

Evidence: [nfs4_compound_vfs.c:5705](/worktrees/compounds/src/server/nfs/nfs4_compound_vfs.c:5705),
[5771](/worktrees/compounds/src/server/nfs/nfs4_compound_vfs.c:5771),
[3207](/worktrees/compounds/src/server/nfs/nfs4_compound_vfs.c:3207),
[5067](/worktrees/compounds/src/server/nfs/nfs4_compound_vfs.c:5067).

Extend the existing private journals, authorization snapshots and coordination
operations. Ordinary OPEN/coalescing, OPEN_CONFIRM, CLOSE, OPEN_DOWNGRADE, LOCK
and LOCKU already have compound support; they should not be described as wholly
unconverted.

### pNFS and named attributes

- LAYOUTGET still uses direct OPEN/backend layout acquisition.
- LAYOUTCOMMIT uses direct OPEN → GETATTR → conditional SETATTR.
- Every REMOVE falls back whenever pNFS is enabled, preserving the legacy MDS
  removal and subsequent DS cleanup path.
- OPENATTR and operations through synthetic attribute directories remain
  legacy, including named-stream open/removal.

Evidence: [nfs4_pnfs.c:859](/worktrees/compounds/src/server/nfs/nfs4_pnfs.c:859),
[1220](/worktrees/compounds/src/server/nfs/nfs4_pnfs.c:1220),
[nfs4_compound_vfs.c:5605](/worktrees/compounds/src/server/nfs/nfs4_compound_vfs.c:5605),
[nfs4_proc_open.c:1744](/worktrees/compounds/src/server/nfs/nfs4_proc_open.c:1744),
[nfs4_proc_remove.c:68](/worktrees/compounds/src/server/nfs/nfs4_proc_remove.c:68).

Add typed VFS operations where needed for layout acquisition and streams, and
journal protocol layout publication/retirement. Keep cross-backend cleanup in
an explicit accepted-completion path. Size SETATTR's pNFS recall barrier is
already integrated; that is separate from the unconverted LAYOUT operations.

### Proxy COPY and conservative fallback

COPY involving a scanned NFS-proxy current/saved FH is excluded at
[nfs4_compound_vfs.c:5467](/worktrees/compounds/src/server/nfs/nfs4_compound_vfs.c:5467).
Its comment cites consuming WRITE payloads, although the preceding correctness
pass added borrowed-payload cloning in
[nfs_write_payload.h:9](/worktrees/compounds/src/vfs/nfs/nfs_write_payload.h:9).
Re-audit this guard against the fixed ownership path and range authorization;
it is a strong candidate for removal with a proxy COPY compound/retry regression.
It is not safe to infer that removing the guard alone is sufficient.

Other splits come from 128 estimated VFS operations, conservative reply-space
estimation, reservation contention/capacity, and validation handed back to legacy
handlers. Most invalid-name/attribute/range decisions can become pure checkpoints
without breaking the enclosing sequence. See
[nfs4_compound_vfs.c:5198](/worktrees/compounds/src/server/nfs/nfs4_compound_vfs.c:5198)
and [5837](/worktrees/compounds/src/server/nfs/nfs4_compound_vfs.c:5837).
Keep resource budgets, but distinguish genuine capacity limits from avoidable
fallbacks; require tests to assert compound spans, not merely correct replies.

SEQUENCE and session/client establishment are protocol framing and naturally
outside filesystem execution. DELEGRETURN, LAYOUTRETURN, FREE_STATEID and
RELEASE_LOCKOWNER are different: their public state mutations could eventually
be journaled to avoid cutting a surrounding filesystem run, but must still
allow independent return/recall requests to unblock parked work.

## 4. S3: mostly converted, with a material assembly boundary problem

No direct filesystem-operation API calls remain in S3. Metadata, ACLs, tagging,
listing and deletion append their dependent operations inside compounds.
DeleteObjects includes up to 1,000 removals in one compound; ListBuckets is
in-memory; AbortMultipartUpload uses validation followed by independent cleanup.

GET/PUT use a configured transfer/retry unit, default 128 KiB and capped at 1 MiB.
Small objects fit in their initial compound; large objects retain an accepted
handle and use subsequent chunks. That matches the requested streaming design:
[s3_compound.h:15](/worktrees/compounds/src/server/s3/s3_compound.h:15).

CopyObject and UploadPartCopy normally accept one COPY_RANGE or buffered
READ/WRITE chunk at a time; whole-object CLONE is an exception. Multipart assembly
accepts at most 128 range operations or one buffered read per compound. These
server-side splits are transaction/retry-budget choices, not HTTP input/output
requirements. Coalescing more work is possible, but unlimited request-wide
transactions should not be assumed desirable. Evidence:
[s3_copy.c:400](/worktrees/compounds/src/server/s3/s3_copy.c:400),
[s3_multipart.c:1234](/worktrees/compounds/src/server/s3/s3_multipart.c:1234),
[s3_multipart.c:2182](/worktrees/compounds/src/server/s3/s3_multipart.c:2182).

**High-priority static correctness finding:** CompleteMultipartUpload prefers
MOVE_RANGE, which consumes the source parts, before the object is published.
After an earlier assembly compound is accepted, a later failure removes the
assembly scratch object and makes the upload retryable. The earlier source
bytes are already gone: memfs literally clears the source block pointers.
Per-compound backend rollback cannot restore an earlier accepted compound.

Evidence: mode selection at
[s3_multipart.c:2229](/worktrees/compounds/src/server/s3/s3_multipart.c:2229),
accepted progress at [2299](/worktrees/compounds/src/server/s3/s3_multipart.c:2299),
failure cleanup/retry state at [1963](/worktrees/compounds/src/server/s3/s3_multipart.c:1963)
and [1986](/worktrees/compounds/src/server/s3/s3_multipart.c:1986), destructive MOVE at
[memfs.c:5024](/worktrees/compounds/src/vfs/memfs/memfs.c:5024).

Use nondestructive copy/clone until publication, or introduce an assembly-wide
transaction/rollback strategy. A targeted regression should force a failure
after an accepted MOVE batch, retry Complete with the same parts, and verify
all bytes. This finding was not fault-injection reproduced during this review.

A smaller completeness issue exists inside the converted metadata helper:
[s3_metadata.c:419](/worktrees/compounds/src/server/s3/s3_metadata.c:419) requests
one 16 KiB LISTXATTRS page and
[321](/worktrees/compounds/src/server/s3/s3_metadata.c:321) never follows its
EOF/cookie. Enough other xattrs can leave S3 metadata/tags beyond that page.
Append pagination within the same compound, as tagging already does at
[s3_tagging.c:546](/worktrees/compounds/src/server/s3/s3_tagging.c:546).
This is not an unconverted request, but an incomplete dependent sequence.

The existing large-GET late-error transport limitation also remains: already
sent bytes cannot be retracted, and the current handler lacks a reliable HTTP
abort/reset operation. More compound coalescing does not solve that transport
problem.

## 5. FUSE, locking and ancillary entry points

FUSE ordinary I/O, namespace, attributes, xattrs and directory handlers submit
compounds. READDIRPLUS has resettable private staging and accepted-only node/grant
publication. There is no broad remaining filesystem handler conversion.

GETLK/SETLK/SETLKW/unlock still operate on claims directly:
[fuse_proc_lock.c:267](/worktrees/compounds/src/server/fuse/fuse_proc_lock.c:267),
[507](/worktrees/compounds/src/server/fuse/fuse_proc_lock.c:507).
FLUSH releases process locks before submitting its COMMIT compound:
[fuse_proc_io.c:560](/worktrees/compounds/src/server/fuse/fuse_proc_io.c:560).
These require a deliberate accepted-state/lifecycle contract if brought into
compound execution, not a wrapper around callbacks that publish lock state.
NFSv3's associated NLM service and POSIX lock machinery are analogous separate
claim state machines. Mount bootstrap lookups, coherence notifications/acks,
handle references and teardown are not ordinary per-request omissions.

The REST test-only `/api/v1/debug/fsop` endpoint still directly performs unlink,
rename, link, and lookup/open/chmod:
[rest_debug.c:225](/worktrees/compounds/src/server/rest/rest_debug.c:225).
Convert these if the invariant is that every filesystem entry point is compound
routed. REST mount/mkfs/config/user management is a separate administrative
lifecycle, not automatically part of an inode transaction.

## 6. SMB scope and shared work

SMB has **no native VFS compound submissions**. Its `chimera_smb_compound` stores
wire requests and related-operation IDs, then advances through individual
handlers: [smb_internal.h:1181](/worktrees/compounds/src/server/smb/smb_internal.h:1181),
[smb.c:1148](/worktrees/compounds/src/server/smb/smb.c:1148).
CREATE, READ/WRITE, FLUSH/CLOSE, queries, SET_INFO/security, streams/reparse,
copy/offload/sparse and deletion cleanup all need conversion where they perform
filesystem work.

This requires a wire-compound adapter, not independent one-op wrappers. It must
preserve SMB command continuation after errors while the VFS executor normally
stops at the first operation error; preserve related IDs privately; retain WRITE
inputs; and defer open/durable/lock publication, notifications, positions and
RDMA output until acceptance. Existing examples of per-command publication are
[smb_proc_create.c:1924](/worktrees/compounds/src/server/smb/smb_proc_create.c:1924),
[smb_proc_read.c:66](/worktrees/compounds/src/server/smb/smb_proc_read.c:66),
[smb_proc_write.c:55](/worktrees/compounds/src/server/smb/smb_proc_write.c:55).
Named pipes, session negotiation and cancellation require explicit protocol
boundaries rather than pretending they are filesystem operations.

Two shared items are distinct from frontend conversion:

1. **Finish-conflict retry policy.** NFSv4 explicitly checks finish status and
   retries EAGAIN before publication
   ([nfs4_compound_vfs.c:2999](/worktrees/compounds/src/server/nfs/nfs4_compound_vfs.c:2999)).
   Other converted frontends check aggregate errors but do not yet invoke
   `compound_retry`; a finish conflict currently becomes a final error/cleanup.
   Add common retry integration or equivalent per-frontend policy before enabling
   optimistic backends. Never retry an ordinary operation EAGAIN as proof of
   rollback. Lack of retry policy alone does not prove their operation callbacks
   are unsafe.
2. **Deferred VFS/backend transaction integration.** Requests still have no
   backend compound association and modules expose ordinary request dispatch;
   begin/finish/abort remain future work. Existing VFS operation completion also
   publishes cache and notification effects before aggregate finish, e.g.
   [vfs_proc_remove_at.c:61](/worktrees/compounds/src/vfs/vfs_proc_remove_at.c:61).
   Backend rollback alone cannot retract those external effects. Add transaction
   identity propagation through nested operations and accepted-only publication
   together with backend hooks. This was explicitly deferred, not evidence of an
   additional frontend handler left unconverted.

## Suggested order

1. Fix the destructive S3 multipart assembly boundary before relying on retry.
2. Convert all fifteen remaining NFSv3 procedures and the live SDK/POSIX variants;
   these are broad, relatively bounded gaps with existing compound primitives.
3. Remove the NFSv4 proxy COPY and v4.0 delegation OPEN boundaries after checking
   their now-improved ownership/replay foundations; convert pNFS REMOVE and
   LAYOUTCOMMIT's filesystem sequence.
4. Add shared stream/layout primitives and protocol-aware namespace/credential
   transitions; cover NFSv4 OPENATTR/LAYOUTGET and SMB together where useful.
5. Build SMB's wire-to-VFS adapter and private publication model; integrate lock
   lifecycles and eliminate remaining conservative NFSv4 fallbacks deliberately.
6. Before enabling transactional backends, complete common finish/retry policy,
   request association, and VFS cache/notification publication. Add fault tests
   proving rejected attempts have no externally published state.

For each conversion, test the actual VFS compound span and accepted publication,
not only protocol success. Existing MBT coverage should remain a semantic
regression net while these structural assertions establish adoption.
