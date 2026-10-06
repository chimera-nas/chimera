# Proposed FUSE/POSIX compound locking plan

2026-09-23. Design proposal, not an implementation or completed validation.
The background fuse_flush agent owns the narrow FLUSH correction separately.

## Scope and existing pieces

Make each GETLK/SETLK/SETLKW/unlock request a VFS compound. Keep frontend callbacks
limited to validation and attempt-private result construction. Put lock admission,
waiting, cancellation, and accepted range publication in the shared VFS claim
machinery. Existing compound RESERVE and claim_range_publish/retire APIs already
support local provisional claims and allocation-free accepted publication. The
NFSv4 interval journal supplies a precedent, but its owner/stateid/replay layer
must remain protocol-specific.

Current POSIX locking additionally projects locks into backend/OS arbitration.
Compound RESERVE explicitly rejects projected ranges today. SEEK_END lock geometry
is currently resolved by the backend, whose result exposes a token but not the
absolute granted interval. These are real shared-interface gaps.

## Proposed implementation sequence

1. Extract a protocol-neutral range journal. Key it by file identity and semantic
   owner (FUSE mount + kernel token; POSIX process/file owner, not descriptor
   lifetime). Preserve read/write modes, splits, replacements and to-EOF ranges.
   Preallocate all publication storage. Preserve old committed coverage until
   acceptance; tentative added coverage is an internal admission reservation.
   Publish range coverage and owner bookkeeping together, without allocation or
   failure, then wake waiters outside publication locks. Rejected attempts release
   only their own provisional resources and rebuild their private view.

2. Add typed compound lock-test/change operations using that journal. Inputs carry
   pinned handles, owner, range/whence, mode and waiting policy; outputs own conflict
   snapshots and reservation state. Frontends translate arguments and publish
   replies only after terminal completion. GETLK observations repeat on retry.
   A lock-conflict operation EAGAIN is not a rejected transaction and is not
   automatically retried. Normalize SEEK_CUR from the captured descriptor offset;
   preserve signed lengths, overflow checks, descriptor access checks and SEEK_END
   semantics.

3. Make waiting and cancellation explicit. A blocking operation parks through a
   VFS-owned ticket and resumes on its owning worker; it holds no frontend mutex
   or owner reservation that prevents the necessary unlock/close. Restart from a
   checked owner generation after contention where necessary. Preserve one terminal
   completion through grant/interrupt/teardown races. Close advances the owner/file
   generation and coordinates pending requests, so a pre-close queued grant cannot
   resurrect retired state; define admission ordering for concurrently new requests.
   Do not keep backend transactions open across unbounded waits: pre-admit before
   backend begin, or abort/yield/restart the attempt while retaining a safe ticket.

4. Convert FUSE local locks first, then POSIX local locking, sharing the same range
   engine. Replace grant-then-track windows and frontend range surgery. Preserve
   close-any-descriptor process-lock semantics, dup behavior, FUSE_INTERRUPT, and
   independent release progress. Mandatory close/teardown cleanup runs exactly
   once even on terminal commit errors; do not make it a replayable callback.

5. Route projected/backend locks through the same typed operations with explicit
   transaction capabilities. Existing immediate backend projection can be compound
   routed, but must not be advertised as supporting optimistic finish rejection.
   A retryable backend must support staged/abortable range changes (including
   replacement/unlock) and token cleanup, with ordered local/backend acceptance.
   Once a legacy backend unlocks or downgrades a range, another process may acquire
   it; restoring the old range is not a safe rollback strategy. Keep this capability
   boundary explicit until backend work provides the necessary contract. Preserve
   the current per-file backend FIFO and wait for accepted release completion.
   Extend backend results to include resolved absolute SEEK_END geometry, or retain
   an explicitly backend-only mode; GETATTR followed by LOCK is not an atomic
   substitute.

6. Validate each layer before declaring conversion complete: range split/merge and
   replacement properties; transient/exhausted finish rejection; no premature
   GETLK output, success reply, or unlock wakeup; operation EAGAIN without replay;
   blocking progress; cancellation versus grant; same-owner close versus pending
   acquisition; concurrent fd reuse/dup; cross-protocol local conflicts; and real
   cross-process backend contention, release ordering and SEEK_END behavior.
   Agents may prepare sources; root owns builds and integrated regression runs.

## Concrete correctness concerns to address in the conversion

- posix_lock_claims.c explicitly identifies a grant-before-track lifetime race:
  another carve can free a granted node before its granter tracks it.
- Its carve allocation-failure path retains excessive lock coverage and cannot
  report failure to the caller. Preallocation with propagated errors must replace
  silent partial behavior.
- FUSE release_owner drains granted locks but does not cancel pending same-owner
  acquisitions. The FLUSH agent confirmed a pending grant can publish after the
  release; owner generation and admission ordering belong in the locking pass.
- FUSE and POSIX GETLK differ in their treatment of cache claims. The shared typed
  test operation should report actual conflicting byte-range locks, with explicit
  frontend formatting, and distinguish backend errors from an unlocked answer.

The deliverable should distinguish compound routing from transactional capability.
Local locking can gain a complete retry/publication contract now. Backend-projected
lock atomicity remains dependent on backend cooperation; it cannot be obtained by
wrapping the existing acquire/release callbacks.
