# SMB LOCK/UNLOCK compound completion (wave21)

## Result

All supported file-backed SMB LOCK and UNLOCK operations use the compound exact
RANGE journal. The legacy entry/list implementation, its acquisition callbacks,
cross-thread completion queue, and its native LOCK/CLOSE exclusions are removed.
Acquired ranges stay in the canonical per-open owner through durable reconnect.
CLOSE retires that owner in its existing compound journal.

The dispatcher fallback now reports INSUFFICIENT_RESOURCES when it cannot allocate
the compound. It performs ordinary FileId resolution and protocol validation,
preserving the FileId inherited by a related suffix, but never acquires/releases
a range or publishes a LockSequence result. A retry can subsequently run through
the compound adapter. Named pipes have no filesystem byte ranges and are rejected
with INVALID_DEVICE_REQUEST after valid FileId resolution.

## Why an import facility was unnecessary

Wave20 correctly identified that simply deleting the legacy-entry eligibility
checks would let a native owner overlook live claims. It proposed representation
transfer while retaining the old producers. This pass removes every producer and
the representation itself together.

The legacy list was process-local. No persisted record or recovery routine
restores it, and warm durable reconnect retains the same in-memory open. A server
running this source therefore cannot contain legacy entries. No release/reacquire
window, speculative claim import, or new VFS migration API is needed. This does
not supply cross-version live process migration or cold lock recovery.

## Contract and lifetime

- Existing exact RANGE operations preserve atomic multi-acquire, ordered
  partial multi-unlock, duplicate shared acquisitions, zero-length compatibility,
  and final-byte ranges. Acquisition/removal and replay-cache changes are
  accepted through the compound journal and frontend private state.
- Canonical identity keeps the original client and per-FileId owner through
  durable reconnect, with the lease key used for cache-coherence exemption.
- Blocking waits remain owned by the executing compound. Wire CANCEL records
  a request-scoped reason; close/teardown records RANGE_NOT_LOCKED while holding
  the open's bucket lock. The owning worker observes cancellation, completes the
  journal, and releases the open pin. No raw request escapes to a legacy resume
  queue or completes on another worker.
- SMB range ownership is local under the current VFS projection policy, which
  projects POSIX claims only. Supporting projected SMB locks later requires an
  explicit compound contract; it is not a reason to keep an unreachable legacy
  implementation now.
- Disconnect cleanup still uses the existing canonical owner retirement
  lifecycle. Complex delete-on-close, cached CREATE/LOCK boundaries, general
  grouped cancellation, stateful reparse and cold persistence remain separate
  areas; this pass does not claim those conversions.

## Regression evidence

The LOCK wire probe now inspects accepted canonical claims instead of requiring
legacy entries. It verifies complete owner identity, lease key, handle anchor,
local/nonprovisional state and remaining claim counts. Coverage includes:

1. Single/multiple acquisitions and exact unlock, same-key peer I/O without
   self-recall, foreign-client conflict, and cross-client nonlease durable BATCH
   reconnect retaining all ranges.
2. Forced initial dispatcher allocation refusal: failed LOCK acquires nothing,
   failed UNLOCK releases nothing, a related QUERY still inherits the FileId,
   and a durable LockSequence retries successfully without a cached error.
3. Multi-acquire conflict rolls back its earlier reservation. Missing-range and
   malformed-element multi-unlock retain their accepted prefix, including after
   finish rejection/retry. Zero-length and final-byte ranges unlock exactly.
4. QUERY/CLOSE coalesces with held ranges and survives resource-only finish retry;
   a foreign writer succeeds after retirement. Existing provisional CREATE/LOCK,
   duplicate shared range, blocking grant, CANCEL and close cases remain.

Only journaled claim/read/resource attempts receive injected finish EAGAIN.
Tests never reject finish after actual filesystem or KV mutation. Independent
Samba `smb2.lock` testing covers replay, cancellation, teardown, geometry, stacking,
overlap and deadlock checks.

Full Debug+ASan build3 and all 52 selected CTest entries pass, including 470 SMB
trace replays. Samba passes 23 cases with three platform-specific skips. No
production changes followed build3; no model or trace edits were made. Logs:
`/tmp/chimera-smb-wave21-{build3,tests2,extended2,cross2,mbt2,torture2}.log`.
See [the current inventory](smb-compound-current-status.md) for the remaining areas.
