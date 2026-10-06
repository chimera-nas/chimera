<!--
SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
SPDX-License-Identifier: Unlicense
-->

# NLM compound conversion — 2026-10-05

NLM LOCK/NM_LOCK and UNLOCK now use the VFS local interval journal. TEST uses
its existing probe compound with finish-aware retry. The server no longer
acquires, cancels, publishes, carves, or releases raw range claims itself.
The conversion removes 702 production lines net, measured against the snapshot
at the start of this NLM pass; new regression coverage is counted separately.

## Execution and ownership

A LOCK builds PUTFH, OPEN_CURRENT, GETHANDLE, LOCK_CHANGE and submits once.
Admission records an immutable owner/file generation and allocates the typed
lock attempt before OPEN starts. The executor validates that its current file
matches that admission. OPEN retries and grant retries therefore remain one
logical request, and cancellation/recovery covers both phases.

UNLOCK borrows an accepted handle when available. Otherwise its compound uses
PUTFH and a local LOCK_CHANGE; it need not reopen a deleted backend object to
retire local coverage. Unknown and malformed-handle unlocks retain their
previous successful no-op behavior.

The VFS owns canonical accepted intervals and tentative replacement fragments.
NLM retains pending request entries and at most one backend handle anchor per
owner and canonical file identity. The wire handle remains available for CANCEL
matching and the remote GRANTED request. Handles transfer only after accepted
finish, and partial unlock, downgrade, and replacement all use the same VFS
journal as FUSE/POSIX. Existing NLM arbitration remains local: range projection
was already restricted to POSIX owners before this change.

The explicit wait callback can send BLOCKED once per logical request; retries
cannot resend it. Final replies, NSM monitoring and remote GRANTED scheduling
occur only after finish has been accepted. An operation conflict is not a
whole-compound retry. Only rejected finish EAGAIN uses the bounded common
frontend retry policy.

## Cancellation, recovery and teardown

CANCEL protects the compound pointer with the NLM registry mutex and calls the
VFS typed cancellation facility. It no longer steals a raw ticket or frees the
original RPC context. Cancellation may prevent an unfinished grant from being
published; it cannot remove an already accepted lock. CANCEL retains its
idempotent successful response in either race outcome.

FREE_ALL, SM_NOTIFY, last-connection cleanup and shutdown invalidate admission
generations and retire accepted coverage. Pending entries always belong to
their compound completion, including before OPEN returns. This fixes the old
path that could free a pending OPEN entry beneath its callback. A live
synchronous LOCK reaped before sending BLOCKED still receives DENIED. Disconnect
tracking prevents using its connection after the disconnect callback; other
connections may keep a client's blocked locks alive.

Worker teardown cancels pending operations and drains their terminal callbacks
before destroying RPC and VFS thread resources. Control cleanup remains a
lifecycle API, rather than an optimistic transaction that could restore locks
for a dead client.

## Compatibility fixes found by validation

- Client-wide cleanup must release accepted ranges in acquisition order,
  pumping waiters between releases. Removing all ranges before pumping changed
  which overlapping waiter won. Journal fragments retain acquisition order.
- Several same-owner grants can arrive before the home worker processes its
  doorbells. Their interval replacements must publish in arbiter admission
  order, including subsequent UNLOCK. Doorbell ordering cannot define this.
- Exclusive range endpoints need a carry bit for 2^64. Treating UINT64_MAX as
  that endpoint could leave the last byte locked after a to-EOF unlock.
- Non-monitored NM_LOCK can still block. Both synchronous LOCK forms must
  retire their reply encoding after BLOCKED; sending another response on
  cleanup corrupts the RPC request pool.
- UNLOCK must retain successful behavior for stale/no-longer-held files and
  must work after the locked file is unlinked.

## Validation

The new NLM wire probe covers finish retries and exhaustion, rejected unlock,
partial unlock, the final byte of the address space, held finish, cancellation
and recovery before submission and during finish, synchronous recovery replies,
one-time BLOCKED across retry, waiter ordering, unlink then unlock, and shutdown
with a blocked request. Its finish rejections affect only local journals and
bookkeeping opens; it makes no claim of backend filesystem rollback.

Both Debug and Release pass the 20 focused NLM/shared-lock/FUSE/POSIX suites
and the six VFS compound/claim unit tests. Both final quick sweeps pass 278/281;
only the three established remote pNFS suites fail. The two Clang stages retain
exactly the baseline 42 warning signatures / 116 occurrences, with none added
or increased. Formatting, include/API guards, REUSE and copyright checks pass.
`make check` remains red on the existing pNFS failures. The final sweep reuses
the worktree's generated corpus without dropping any replay tests.

The Release analyzer initially followed an impossible singleton-list deletion
into another iteration in client cleanup. Rebuilding the pending list directly
removes that ambiguity and preserves its order. The final focused suites and a
separate uncached Release analysis pass after that revision. Final scan-build
logs replay cached baseline diagnostics; empty fresh report directories are
not evidence that those baseline warnings have disappeared.

Full evidence is recorded in MEMORY.md. Logs and before/after snapshots are
under `/tmp/chimera-nlm-compound-20261005/`; `check-complete.log` and
`compare-final.txt` describe the final sweep.

## Scope remaining

NLM's ordinary implemented lock operations now enter through compounds. CANCEL,
client recovery, reference release and shutdown remain appropriate control APIs.
SHARE/UNSHARE still implement the preexisting no-enforcement response; adding
DOS share enforcement would be a separate feature. Backend compound transactions
and projected NLM locks remain future work. The typed local lock compound still
excludes unrelated filesystem mutations because cancellation/retirement can
veto local publication while finish is pending.
