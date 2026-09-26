# FUSE/POSIX compound locking implementation

2026-09-23. User-approved implementation of
[fuse-posix-compound-locking-plan.md](fuse-posix-compound-locking-plan.md).
Source integration and validation are complete. Extended ext4 validation also
exposed and fixed a separate SIZE SETATTR handle-selection regression. Nothing committed or merged.

## Result and completion assessment

FUSE GETLK/SETLK/SETLKW/unlock and POSIX fcntl/lockf locking now use typed VFS
compounds. Frontends normalize arguments, pin request resources, and publish
terminal results. They no longer own interval surgery or grant-then-track nodes.
The shared lock domain owns admission, provisional ranges, a preallocated accepted
interval journal, waiting, cancellation, and semantic owner/file generations.
GETLK skips tentative ranges while admission continues to respect them.

FLUSH preserves locks through bounded COMMIT finish retries and releases them
once at terminal completion, including terminal errors. FSYNC does not release
locks. Close invalidates pending admissions before draining descriptor users;
mandatory owner cleanup is non-retryable and runs despite rejected finish.

The conversion required correctness work beyond wrapping dispatch: close/dup2
slot reservation, generation-checked descriptor waiters, serialization of crossed
dup2 calls before source pinning, checked range arithmetic, local GETLK PID
preservation, and backend denial/error propagation. FUSE interrupts marshal
completion to the request worker; retirement also removes a granted provisional
reservation whose finish is still pending.

Agents completed their assigned sources, then root reviewed integration and
built/tested the combined tree. Root corrections included published-only lock
queries/admission flags; NFS3 backend owner anchors; mixed-whence and cross-process
regressions; NFS4 tentative lock visibility; and test harness export/synchronization
fixes. Initial source handoff was not treated as validation.

## Backend interval correctness

Legacy immediate projection is distinct from optimistic transactions. Typed
mutations set CLAIM_REPLACE. Memfs preallocates exact same-owner replacements and
unlock fragments. Linux/io_uring keep an owner FD anchor and let fcntl perform
exact replacement, unlock, and EOF resolution. NFS3 keeps an owner anchor and
sends exact NLM geometry; anchor allocation precedes RPC, transport uncertainty
retains cleanup ownership, and only successful whole-owner release removes it.
Invalid EOF-relative unlocks are validated even without an existing anchor.

The NLM server also needed exact splitting and same-owner mode replacement. Its
LOCK path now reserves fragment storage before admission, including deferred
grants; UNLOCK allocation failure leaves old coverage intact. An exact-range
replay check now includes read/write mode. This fixes NLM interval semantics;
it does not convert NLM's existing pending/replay machinery to compounds.

## Explicit remaining boundaries

- Typed lock operations currently require one lock operation per compound,
  optionally with cursor setup. They cannot yet mix with filesystem mutations.
- Local lock publication supports rejected-finish retry. Immediate projected
  changes reject an installed finish adapter before mutation. Mandatory release
  drains once and remains non-retryable. Backend begin/end/rollback support is
  still future work.
- After a successful SEEK_END mutation, that owner/file uses backend-only range
  arbitration until close. This avoids stale absolute local coverage but means
  those locks are not fully represented for other node-local protocol arbiters.
  Linux resolves EOF through fcntl; NFS3 still uses GETATTR plus NLM and cannot
  provide atomic EOF-relative locking against concurrent resize.
- Owner generation tombstones remain until mount/client teardown. Empty buckets
  release inode-state references, but the small owner/file records are retained;
  reclaiming them safely needs explicit outstanding admission accounting.
- Persistent backend cleanup failure is returned after three shutdown attempts
  and logged at teardown. Remote failure cannot be turned into confirmed unlock;
  backend owner bookkeeping remains responsible until backend teardown.
- NLM retains its existing direct dispatch and pending-request races. The legacy
  claim_range_replace helper remains separate from the new preallocated journal;
  its bounded callback/spare handling was not redesigned here.

## Validation

ASan Debug integrated production builds passed (`/tmp/chimera-lock-build2.log`
and final production correction `/tmp/chimera-lock-build5.log`). Build6 contains
only the isolated SETATTR regression setup correction.
Build1 caught an allocation-error enum typo in root's NFS3 change. Builds3/4 only
correct test harnesses: isolate the directly included Linux module symbol, export
the FUSE interceptor, and synchronize explicitly with paused finish entry.

All 42 focused CTest selections pass after those fixture corrections. Coverage
includes real FUSE mounts; synthetic-wire retries/exhaustion, delayed unlock and
grant publication, interrupt/close/shutdown; POSIX descriptor cancellation/reuse
and crossed dup2; mandatory release despite rejected finish; memfs denial; exact
partial unlock/downgrade across independent processes and NFS3; mixed SEEK_SET/
SEEK_END ranges; and real Linux OFD operations without mount/handle prerequisites.
Logs: `/tmp/chimera-lock-focused{1,2,3}.log`; selection:
`/tmp/chimera-lock-focused.txt`. No regression was weakened to hide a production failure. ASan memory-error
checks are enabled; leak detection uses the existing detect_leaks=0 configuration.

The initial broad run passed 527 and skipped 16 of 543 further distinct selections
(`/tmp/chimera-lock-matrix1.log`). Rerunning twelve filesystem-related skips with
CHIMERA_MBT_SCRATCH=/build/test passed ten and exposed two POSIX/NFS3 model batches
with three failing data traces each (`/tmp/chimera-lock-ext4.log`). Four other skips
are nonroot fchownat variants requiring root credentials.

A three-operation trace prefix and GDB isolated the ext4 failures: create/open a
mode-000 file, read EOF, then allocate beyond EOF. The compound SETATTR path
downgrades a data handle to PATH, so ftruncate fails and the path fallback returns
EACCES. The model's existing allowed allocation error then leaves later size/data
expectations divergent. SIZE SETATTR now requires a data handle, and a direct VFS regression checks both
the resulting size and preservation of the data cursor. The minimal reproduction
and both complete NFS3 batches pass (`/tmp/chimera-lock-allocate-fixed.log`,
`/tmp/chimera-lock-ext4-fixed.log`); no model exception was added. The initial unit
test incorrectly targeted a directory and was corrected to create an independent
regular file without weakening either assertion. Evidence: `/tmp/chimera-lock-allocate-gdb2.log` and
`/tmp/chimera-lock-nfs3-allocate-prefix.itf.json`.

Final integrated run: **581 passed, 4 intentionally skipped, 0 failed** across
585 distinct selections, with ext4 scratch enabled. Logs:
`/tmp/chimera-lock-final.log` and immutable
`/tmp/chimera-lock-final-LastTest.log`; selection:
`/tmp/chimera-lock-all.txt`. The final log contains no AddressSanitizer errors.
The four skips are the nonroot fchownat cases listed above. Teardown diagnostics
identified three synthetic-owner fixture cleanup omissions: their earlier owner
anchors were left until after memfs removal. Those fixture-only omissions were corrected after the lock assertions, then all
nine affected registrations passed with zero unresolved-owner or ASan errors.
Logs: `/tmp/chimera-lock-cleanup.log` and
`/tmp/chimera-lock-cleanup-LastTest.log`; build7 changes tests only. Production
dirty-owner diagnostics are retained. Final whitespace checks pass.
