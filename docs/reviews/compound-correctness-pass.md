# Compound correctness and model replay cleanup

Correctness cleanup on `compounds-refinement`, 2026-09-23. Changes remain
uncommitted, including the model updates inside `ext/specs`.

The previous broad regression set exercised protocol probes, but did not
register the generated model replay corpus. This pass generates the current
754-trace corpus and fixes the local CMake provider to honor an external trace
directory. Replay results must distinguish actual failures, optional capability
skips, and documented protocol/model differences; a green run with abandoned
traces is not evidence that the abandoned paths work.

The changes being validated include:

- NFSv4 LOCK admission parity with the legacy implementation; legacy LOCK and
  OPEN_CONFIRM replay ordering and consuming-error behavior; v4.0 SECINFO current
  filehandle preservation; ACCESS execute permission reporting.
- Native AUTH_UNIX OPEN_AT checks parent directory search permission before
  backend lookup, existing-entry errors or truncation, including inferred NFS3
  and compound CREATE. The old permission-bypass MBT tolerance is removed.
- Delegated GETATTR retains dirty size/change information, advances repeated
  reports only on accepted attempts, rejects incomplete callbacks, and handles
  delegation return during a pending query. A bounded per-file version journal
  preserves visible change values after return without hiding later backend
  updates. Entries survive for the server lifetime; restart persistence still
  requires backend metadata support.
- FUSE READDIRPLUS stages entries until compound finish accepts them. Node lookup
  counts and grants are then published once. Sync coherence replies use zero
  attribute and dentry TTL because the existing grant state has no incarnation
  token proving uninterrupted protection across a parked compound.
- SMB overwrite waits for share and cache admission before truncating the exact
  opened object. Rejected base-file and stream overwrites preserve bytes. The
  overwrite intent preserves stream deletion semantics and carries the requesting
  claim actor, so it does not recall its own coalesced lease.
- Retired SMB shares reject durable reconnects and release parked handles,
  including persistent warm registry records. EOF/allocation changes require
  the open's write-data grant. Legacy exclusive/batch oplocks count every
  distinct same-stream opener, including same-client and metadata-only opens;
  new write-caching leases also enforce the sole-opener rule. Admission uses
  the rights retained after an outstanding break, and deferred grant rescue
  cannot treat its own provisional cache claim as an established peer lease.
  Metadata-only lease requests cap their grant without recalling a peer. Size
  SETATTR carries the caller's claim identity through authorization and recalls;
  its legacy LEVEL_II oplock breaks, while a coherent same-key lease is spared.
- SET_INFO checks the open's granted rights before BASIC, disposition, rename,
  EA or size mutations. SACL changes require ACCESS_SYSTEM_SECURITY rather than
  treating WRITE_DAC as equivalent; an explicitly admitted security right is
  retained in GrantedAccess. Lease identity also needs both ClientGuid and key:
  strict replay exposed cross-client coalescing that suppressed required breaks.
  Parsed EA buffers are freed even on admission failure, and BASIC timestamp
  policy changes publish only after successful mutation. Disposition validates
  readonly files and nonempty directories before setting delete-pending state;
  unsupported DispositionEx flags return errors instead of being ignored.
  CREATE also resets pooled DOS attributes before adding ARCHIVE, preventing
  a preceding readonly request from making an unrelated new file readonly.
  Directory WRITE rejects before mutation, and rename to the same parent/name
  succeeds as a no-op after access admission.
- SMB deletion follows logical opens rather than backend cache-handle counts.
  The file or named stream remains delete-pending until its last relevant open
  retires. The pending action owns the original target identity, path and
  credentials; concurrent closes transfer it once. Cancellation, named-stream
  base reservations, normal CLOSE, transport teardown and durable cleanup use
  this ownership. Disposition validation and recall hold their backend handles
  independently of a concurrent CLOSE, including copied synthetic handles across
  the asynchronous GETATTR/READDIR chain. Tests include 24 simultaneous close
  scenarios, cancellation after the original opener closes, named-stream peers,
  abrupt final disconnect and rejected overwrite without data loss.
- Explicit DispositionEx POSIX deletion removes the link when the deleting
  handle closes, while existing peer handles retain access to the old object.
  The SMB VFS backend now uses this explicit capability for unlink/rmdir rather
  than relying on ordinary deferred deletion. Unsupported peers return an error
  (ENOTSUP for unsupported information class), and the temporary FID is closed
  even on failure. Tests recreate the same base/stream name before an old peer
  closes and verify both old data and replacement survival; sequential hardlink
  deletion is covered too. QUERY_INFO reports shared DeletePending, including
  surviving peers, rather than only the querying handle's own flag.
- NFS proxy WRITE clones retained input before outgoing XDR consumes it. Compound
  READ moves backend descriptors into caller-owned storage before asynchronous
  suffixes can outlive the upstream RPC reply. Explicit metadata cursors now
  receive the directory type check before READ/WRITE. Proxy GETATTR and namespace
  operations preserve upstream change attributes and before/after change values.
  Session I/O uses sequence zero with the retained state identity so coalesced
  OPENs do not invalidate older descriptors. Proxy SETATTR supplies requested
  pre/post attributes instead of leaving stale cache state.
  CLOSE also uses the current version of its retained identity; otherwise a
  concurrent upgrade could reject CLOSE with OLD_STATEID, orphan the remote
  state and cause repeated OPEN retries. Transient DELAY replies retry with a
  timer only when no successful mutating prefix would be repeated. Disconnect
  cancels that timer and completes once; released slots wake parked work.

pNFS now grants orchestrated flex layouts only when the MDS file already resides
on the configured authoritative NFS-proxy DS mount. A separate empty DS file,
or a one-time copy, cannot provide coherent subsequent MDS/DS writes and
truncates. Backends that merely store an opaque layout blob therefore return
LAYOUTUNAVAILABLE and do not advertise a supported layout type. Native backend
layout sources remain supported. This is a deliberate restriction of the
previous unsafe topology, not implementation of generic data migration.

The feature test reads pre-LAYOUTGET MDS data directly from the DS without
seeding a second file, checks writes in both directions, and checks truncate
visibility. Model pNFS clusters now export unique subtrees of the authoritative
DS mounts, alternating between configured devices and using memfs, diskfs, or
cairn storage. Async LAYOUTGET retains its client through completion.

Layout stateids now encode the server epoch consistently on grant, upgrade and
recall, and generic layout lookup acquires the reference it later releases.
LAYOUTCOMMIT validates the complete layout identity, reports actual size changes
and handles mtime-only requests. Its GETATTR/SETATTR extension remains vulnerable
to a concurrent extender; a backend atomic maximum-size operation is needed to
close that race. The sequential no-shrink test does not prove concurrent safety.

Validation uses the generated corpus as well as protocol probes. The corpus
contains 754 traces. SMB has 94 traces: the original lease seed is retained,
with eight additional traces restoring explicit RH-break coverage. Model
selftests, all generation batches and all 11 expanded coverage gates pass.

- Strict SMB: all 94 traces pass under plain, signed SMB 3.1.1, encrypted
  SMB 3.1.1, NTLMv2 and signed SMB 3.0 profiles: 470 trace executions, zero
  mismatches, skipped or abandoned traces, or recorded deviations. The final
  ordinary registered CTest run repeats all 470 executions successfully after
  removing the obsolete CD1/CD2/CD4 exclusions (build 21).
  The three additional security profiles are now registered in CTest alongside
  the existing plain and signed profiles. All 14 SMB probes, hardening and
  selected Samba regressions pass. These include
  access checks, rename, streams and delete-on-close permissions. SMB model replay
  uses memfs; no diskfs SMB model coverage is claimed.
- POSIX over SMB: all 53 traces pass, including the formerly failing open-file
  unlink followed by exclusive recreation. Other POSIX backend/proxy batches,
  all eight S3 MBTs and seven event-library, diskfs and FUSE model tests pass;
  unavailable host passthrough cases are capability skips.
- NFS/FUSE matrix: 44 passes and 22 host capability skips across 66 tests;
  1,729 complete trace executions, 191,138 operations/compounds, and 297 trace
  capability stops. Across ordinary and delegated runs, memfs/diskfs exercise
  140 of 142 distinct NFSv4 traces; the other two require a DELAY outcome where
  the server completes the recall operation. Cairn covers 132 of 142, with the
  additional eight stopping on unsupported READ_PLUS/CLONE capabilities.
- Native NFS3 after removal of the search-permission tolerance: seven
  TCP/RDMA/GSS configurations, 196 trace executions, 29,568 steps and 9,590
  attribute checks; zero attribute skips or capability declines. VFS enforcement,
  compound and ACL regressions pass.
- pNFS: three passing batches on memfs, diskfs and cairn; 18 complete traces /
  1,440 compounds per backend, six unsupported-xattr trace stops per backend.
  Completed traces still use the documented reconciliation rules below.
- NFS proxy model replay, DELAY retry lifecycle and four backend I/O regressions
  pass three repetitions each. Full v4.0 READ/WRITE delegation replay compares
  the complete OPEN reply and output filehandle, including owned ACE principal
  bytes. Finish-retry fixtures pass all 120 parallel normal/retry executions.
  Their intermittent assertion was a log race: an asynchronous logger split
  the final newline; writing each fixture record atomically fixed it.

The final broad regression selection (build 21) passes 333 tests with two
expected host-filehandle capability skips. All 16 focused tests pass, including
244 claim assertions and 36 hardening/lifetime assertions.

Detailed logs are under `/tmp/chimera-correctness-*.log`; the NFS trace audit is
`/tmp/chimera-nfs-final18-audit.json`. These groups overlap and must not be summed
into a unique test count. Final logs: `chimera-correctness-broad21.log`,
`chimera-correctness-focused21.log`, and `chimera-correctness-smb-registered21.log`.

Production backend compound transactions and rollback remain outside this
pass. Synthetic finish-EAGAIN tests exercise frontend retry and publication;
they do not prove rollback of backend mutations.

The SMB model corrections are separate changes in the dirty `ext/specs`
submodule. They synchronize coalesced lease members, retain an open's original
ClientGuid across reconnect, model SET_EOF cache breaks, and describe when a
share-denied batch CREATE waits for an acknowledgment. The driver retains
asynchronous CREATE responses and checks rejected-overwrite metadata through
an existing handle, avoiding an extra open that could itself park on a break.
The generator currently acknowledges outstanding breaks before issuing more
work; concurrent operations during breaks and parked compound suffixes remain
outside its coverage until pending commands are explicitly modeled. These
restrictions must not be mistaken for successful coverage of those schedules.
The sole-opener condition for a new exclusive grant is also distinguished from
the key checks for upgrading an existing granular lease; applying the former
to every upgrade produced incorrect expectations behind metadata-only opens.

Fatal SMB harness errors now flush diagnostics and terminate with their
original failing status without running global event-library cleanup beneath
live embedded-server threads. This removes a secondary teardown crash; it does
not convert timeouts or mismatches into passing outcomes.

Remaining boundaries are explicit: the generated SMB lease model does not yet
represent concurrent work during an outstanding break or a parked compound
suffix. Positive deletion, directory writes and self-renames are restored to the SMB
generator, with coverage gates requiring those branches. Namespace check/mutation atomicity, backend transaction rollback and
concurrent LAYOUTCOMMIT extension require deeper VFS/backend work. Delegated
v4.0 OPEN retains its existing protocol path, with its replay correctness fixed.

Coverage audit: successful NFS/POSIX batches run against their documented
reconciliation registries. In particular, each completed pNFS corpus accepts
symlink mode 0755, coarse file-type errors, and CREATE type-before-parent error
ordering; its six xattr traces stop early. Host passthrough suites could not run
here and retain limitations for relinking unlinked files and preserving
EXCLUSIVE-create verifiers in host timestamps. POSIX proxy tests retain client
API allowances such as unsupported real-directory-fd fstatat and error ordering.
POSIX over SMB skips operations the model denies by DAC because its mount
authenticates a single root identity (SD-DAC). It also reconciles root's retained
set-ID bits (SD-SETID) and special-file open error differences (SD-SPECIAL).
These results do not establish strict conformance outside the checked paths.

Deletion concurrency has bounded remaining limitations: a pending-delete check
and share-claim admission remain separate operations, and named-stream target
identity verification precedes a separate remove-stream operation. Base unlink
uses an expected-filehandle match. Closing those general namespace races needs
an atomic backend admission/mutation contract; the new sequential and concurrent
close tests do not establish that stronger guarantee.

SMB deletion still keeps one pending record per inode, so concurrent independent
dispositions on different hardlinks need per-link pending intents. The in-flight
action guard allows sequential deletion through another hardlink; it does not
establish independent concurrent intent ownership. NumberOfLinks continues to
report the backend count, so shared DeletePending correctness does not imply
complete Windows per-link metadata conformance.

To repeat the corpus checks after configuring `SPECS_TRACES_DIR` to the generated
trace directory, use the Extended configuration and retain verbose output:

```sh
ctest --test-dir /tmp/chimera-compounds-build -C Extended -V \
  -R 'chimera/server/smb/mbt/batch_memfs' -j5
ctest --test-dir /tmp/chimera-compounds-build -C Extended -V \
  -R 'chimera/posix/mbt/batch_smb_memfs'
```

These commands exercise the ordinary registry without an include-declined
override. The SMB reply and trace-exclusion registries are now empty.
