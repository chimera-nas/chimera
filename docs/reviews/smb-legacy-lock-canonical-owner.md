# Legacy SMB LOCK canonical ownership

Historical wave20 report. Wave21 removed this legacy representation and all its
producers, completing local file LOCK/UNLOCK conversion. The import prerequisite
below applies only if legacy producers are retained; it is superseded by
[the completion report](smb-compound-lock-completion.md).

Wave20 implementation validated by root's full Debug+ASan build5 and all 52
selected tests, including the expanded LOCK wire probe. See
[combined status](smb-compound-current-status.md) for validation logs.

## Defect and fix

The legacy multi-element LOCK helper built RANGE.client_key from SessionId.
ACCESS reservations, caching grants and native I/O instead use the canonical
open identity. Consequently a same-lease peer could conflict with a range that
should share its cache-coherence exemption, and a fresh range could incorrectly
conflict with the granting open's own caching lease.

The single-element path used the current session's client_key. A nonlease durable
BATCH open can reconnect under a different ClientGuid while retaining its original
ACCESS and RANGE identity, making that reconstruction wrong too. The recall actor
also rebuilt identity from the current session or replaced the entire owner with
the caching grant's identity.

Both legacy RANGE constructors and the recall actor now use
`chimera_smb_open_actor_owner(open)`. This preserves canonical protocol/client and
per-FileId owner halves, adding the current grant key for cache coherence. RANGE
claims retain their exact `op_handle` and persistent-FileId `policy_tag`. The
recall actor retains its own handle anchor. No protocol sequencing, geometry,
replay cache, cancellation or acquire/rollback behavior changes.

## Reachability and regression

These are reachable fallback paths, even though ordinary SMB LOCK now uses the
native adapter. `chimera_smb_vfs_compound_try` returns zero when the initial SMB
batch allocation fails; the ordinary dispatcher then calls `chimera_smb_lock`.
Once it has populated `open->lock_entries`, native LOCK bound eligibility declines
that handle. Native CLOSE likewise retains a legacy-entry boundary.

The existing `smb2_lock_compound_probe` now links `smb2_lock_inspect.c`. Its narrow
interposer returns zero for an explicitly armed standalone LOCK, reproducing the
real initial-allocation fallback dispatch without a production knob or backend
failure injection. A second mode calls real native admission and asserts its
natural refusal when legacy entries remain. The real wire parser, dispatcher,
legacy handler, claim engine and reply path all run.

The completion hook checks that the open still has legacy entries and no canonical
RANGE token, then verifies each actual RANGE claim's complete owner identity,
LeaseKey, handle anchor and policy tag against the canonical ACCESS identity.
It also checks exact remaining claim counts. New wire cases cover:

- A forced two-element acquisition under a coalesced RWH lease, same-key peer
  READ/WRITE, subsequent natural single-element fallback, no self lease break,
  and exact multi-element unlock.
- Nonlease single/multi-element acquisitions, own I/O success, another client's
  I/O conflict, and legacy CLOSE draining held entries so the peer can write.
- A nonlease durable BATCH open with a held legacy range, synchronous LOGOFF park,
  successful reconnect using a different ClientGuid, further multi- and
  single-element acquisitions, and own I/O across all ranges. The inspection
  explicitly requires canonical client_key to differ from the new session's
  client_key; this cannot silently reduce to a same-client reconnect test.

No finish rejection is injected into these legacy operations. Existing native
retry, provisional-owner, blocking/cancel and compound grouping cases remain.

## Remaining migration prerequisite

Removing the `lock_entries` exclusions is unsafe. The exact RANGE journal owns
its own acquisition records; it cannot retire or unlock arbitrary legacy claim
objects. Legacy entries also own file-state references, pending-acquire tickets
and callback storage. Creating an empty canonical owner alongside those entries
would make native UNLOCK miss them and native CLOSE leave their claims behind.
Releasing and reacquiring them introduces a conflict window and may wake another
waiter; it is not an atomic representation change or a safe rollback.

A bounded follow-up could stop creating new legacy entries by routing a legacy
LOCK dispatch through a standalone typed RANGE compound, with allocation failure
returning a resource error before effects. Handling already-retained entries still
needs an explicit VFS import/representation-transfer facility:

1. Obtain exclusive per-open lock-lifecycle admission, rejecting/defering while
   a parked legacy acquire, callback, concurrent LOCK/UNLOCK, CLOSE or teardown
   owns the open. An observation of refcounts or list emptiness is insufficient.
2. Allocate the canonical owner and all exact records before public changes.
   Preserve duplicate shared acquisitions, zero-length compatibility, exact
   unlock ordering, original owner identity, handle anchors and policy tags.
3. Under the file/owner locks, replace the linked legacy claim representation
   with equivalent owner records without releasing claims, pumping waiters or
   emitting cache recalls. Transfer frontend ownership once; no mixed list/token
   mutations can be allowed afterwards.
4. Only after this representation transfer may typed LOCK and
   RETIRE_OPEN_CLAIMS/CLOSE operate on the token. Validate waiting acquisitions,
   retries, partial multi-unlock errors, cancellation, durable rehome and teardown
   before removing the boundaries.

An alternative journal that explicitly owns legacy claim retirement could enable
CLOSE first, but still needs lifetime pins and accepted-only frontend list
publication; it does not solve general legacy UNLOCK migration. No migration or
CLOSE/runtime eligibility widening is included in this pass. SMB RANGE operations
are local today; backend projection is POSIX-only and is not the active reason
for these remaining SMB boundaries.
