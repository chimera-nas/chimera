# NFSv4 compound boundary inventory after the tenth pass

This is a source audit of the uncommitted conversion, including the new v4.0
owner replay journals and fresh OPEN/OPEN_CONFIRM work. It does not claim that
the entire wire COMPOUND is always one VFS compound, or that backend rollback
exists. Function names below are stable search anchors; line numbers will move
as the pass is refined. No production files were changed for this audit.

## Boundaries that need an explicit architectural extension

| Priority | Boundary and current behavior | Required extension |
| --- | --- | --- |
| P1 | A run has one credential and export. Later PUTFH and inherited RESTOREFH must stay in the same export. | Add an execution-time credential/export context transition, including security-flavor checks and squash policy, before allowing cross-export cursors in one compound. Same filesystem and same export are different tests; RENAME/LINK's filesystem restriction does not itself justify equating them. |
| P1 | Synthetic namespace and named-attribute handles are refused. PUTROOTFH/PUTPUBFH/OPENATTR are not in the encoder allow-list. Any LOOKUP or LOOKUPP run is refused when a `/` export is configured; LOOKUPP after a cursor move is also cut off. | Represent frontend namespace resolution in the operation stream, with private current/saved export and logical-handle state. A pure callback can select a synthetic result or backend lookup once the runtime cursor is known. Export-root parent and junction behavior must remain intact. |
| P1 | Delegation-enabled OPEN, delegation-sensitive GETATTR, and delegation/pNFS-sensitive REMOVE/RENAME remain legacy. Size SETATTR is refused when pNFS features are enabled. Delegation/layout stateids are outside the supported stateid classes; data-server READ/WRITE/READ_PLUS use the legacy authority model. | Introduce asynchronous coordination phases for recalls, CB_GETATTR/CB_NULL, and data-server cleanup. Sending a recall or changing delegation state is an external effect, so a repeatedly invoked pure callout alone is insufficient. Define which effects happen before the retryable attempt and which publish after acceptance. |
| P1 | Ordinary local range journals refuse projected backend tokens, callback-bearing/parked claims, and legacy overlapping mixed-mode intervals. NFS-proxy COPY also falls back. | Define reversible backend range-token ownership and normalize legacy interval history without guessing its mode. Unify proxy WRITE payload ownership before adopting the compound COPY transfer path. Local admission reservations do not establish backend transaction support. |
| P2 | v4.0 state operations other than LOCKT still depend on a connection's cached implicit session/client. A fresh connection without that binding cannot use these journals. A state belonging to a different client triggers legacy fallback during freezing. | Generalize LOCKT's client lookup/pinning to derive the correct client from each operation's clientid or acquired OPEN/LOCK state. Keep lease renewal after acceptance. Preserve existing sessionless and multiple-client connection behavior instead of treating the cached client as universal v4.0 authority. |

Source anchors: `chimera_nfs4_compound_try_vfs` scan and cursor seeding,
`nfs4_vfs_op_encodable`, `nfs4_vfs_stateid_encodable`, and
`nfs4_vfs_v40_open_journalable` in
[`nfs4_compound_vfs.c`](../../src/server/nfs/nfs4_compound_vfs.c);
connection binding in [`nfs4_proc_compound.c`](../../src/server/nfs/nfs4_proc_compound.c)
and [`nfs4_proc_setclientid_confirm.c`](../../src/server/nfs/nfs4_proc_setclientid_confirm.c);
LOCKT client reservations, execution checks and accepted lease renewal in
`chimera_nfs4_compound_try_vfs`, `nfs4_vfs_operation_prepare` and
`nfs4_vfs_compound_complete`;
`nfs_lock_range_journal_alloc` in
[`nfs4_state.c`](../../src/server/nfs/nfs4_state.c).

## Guards that can become pure execution checkpoints

These are conversion gaps rather than fundamental transaction boundaries.
Keeping them on the legacy path preserves existing behavior, but a failing
operation can still split a wire request after a successful VFS prefix.

- Invalid names, unsupported CREATE types, invalid attribute masks, READDIR
  cookie/maxcount checks, xattr name/option checks, invalid SEEK selectors,
  WRITE_SAME geometry, and overflowing ranges are rejected during scanning.
  Most depend only on immutable input and can set a private NFS status in a
  checkpoint, stopping later operations without ending the submitted compound.
  Preserve protocol error precedence and v4.0 consumed-error replay behavior.
- Grace checks for READ/WRITE/READ_PLUS and size SETATTR still split at
  scan time. Recovery checks are pure reads, and LOCK/OPEN/LOCKT demonstrate
  how to carry their protocol statuses at execution time. Grace state changing
  during an attempt needs a deliberate validation policy, not an implicit
  assumption that scan-time admission remains current forever.
- Anonymous size SETATTR, ALLOCATE/DEALLOCATE, SEEK and WRITE_SAME after a CLOSE
  or downgrade still encounter the `saw_close` guard. READ, WRITE, READ_PLUS
  and COPY already carry a private admission view. Extend the remaining VFS
  operations to consult the same private share/range view while preserving
  unrelated holders' conflicts. CLONE with special stateids remains excluded.
- A LOOKUPP after LOOKUP/RESTOREFH is refused because its source is unknown
  during scanning. Runtime cursor availability solves that timing issue;
  namespace-root/junction resolution remains the separate architectural work
  identified above.
- Busy or temporarily frozen owner groups fall back. A retry/wait result that
  retains the request's compound identity would be preferable to using legacy
  execution as the contention path once backend compounds are transactional.

The operation allow-list also excludes client/session administration,
RECLAIM_COMPLETE, FREE_STATEID, RELEASE_LOCKOWNER, delegation and layout
operations. Some have no filesystem work and need no fabricated VFS operation
on their own. When embedded between filesystem operations, however, retaining
one transaction requires a protocol-state checkpoint with a reversible private
update or an explicitly documented boundary. RECLAIM_COMPLETE, for example,
updates recovery records/counters and cannot be treated as an already-pure
validation callback.

## Capacity is currently a transaction boundary

`NFS4_VFS_COMPOUND_MAX_OPS` is 128 estimated VFS operations, while the generic
VFS supports 1024. A single wire operation can cost up to eight VFS operations.
The scan cuts a longer request into runs; conservative estimates can split it
before the actual generic limit is reached. Owner/state/connected-parent
arrays, original range counts, and reply storage introduce additional bounds.

Reply budgeting is intentionally conservative: the whole estimated result must
fit together with the response headroom floor. ACL GETATTR also performs a
dynamic staging check before allowing a suffix. Refused construction rewinds
the request arena before legacy dispatch. These are important memory-safety
properties; removing checks is not a valid way to obtain one-to-one mapping.

Before backend transaction support, decide whether limits mean a protocol
error before mutation, a growable bounded transaction representation, or an
explicitly non-atomic split. Silent splitting preserves present incremental
NFS behavior but gives the backend several transactions for one wire compound.
Track fallback reasons and submitted run counts so limits remain observable.

Source anchors: `NFS4_VFS_COMPOUND_MAX_OPS`, `nfs4_vfs_op_reply_bound`, scan
`vfs_ops`/`reply_bound`, `nfs4_vfs_getattr_complete`, and the `refuse` cleanup in
`nfs4_compound_vfs.c`; `NFS4_COMPOUND_OWNER_MAX_STATES` and
`NFS4_COMPOUND_MAX_ORIGINAL_RANGES` in `nfs4_state.h`.

## What is no longer a general boundary

Ordinary OPEN, CLOSE, OPEN_DOWNGRADE, LOCK and LOCKU use private owner/state
journals, including child-lock retirement and connected lock-owner parents.
The tenth pass adds v4.0 typed replay, fresh unconfirmed OPEN and OPEN_CONFIRM.
An OPEN replay skips its own work and restores its cached filehandle; it must
not skip an earlier successful prefix or later operations. Delegated replies
and legacy successful OPEN caches without a complete typed snapshot still
require fallback.

LOCKT now stays in the compound in all supported minor versions. Each v4.0
LOCKT resolves and pins its own wire clientid, including a client different
from the connection's cached client and requests on a sessionless connection.
Grace, length and client errors are execution checkpoints. Lease renewal waits
for accepted completion. The compound context also pins its cached client for
the complete attempt lifetime; session references alone are not relied upon
to protect the unified client.

EXCLUSIVE4/EXCLUSIVE4_1 and CLAIM_PREVIOUS are admitted by the current OPEN
encoder. Exclusive creates check their verifier before accepting a collision.
Reclaim OPEN/LOCK checks execute in the compound. Earlier notes describing
all exclusive/reclaim OPENs or all v4.0 replay as permanent boundaries are
therefore stale. These conversions do not make CREATE/WRITE/truncate safely
retryable on a backend that has not implemented rollback.

## Validation still needed before claiming recovery and fallback parity

The final tenth-pass wire runs passed all six normal/retry variants: 40
measured cases for v4.0, 129 for v4.1 and 167 for v4.2. Converted cases verify
the complete expected wire-to-VFS run mapping, including the new LOCKT paths.
These checks do not remove the remaining boundaries documented above.

The recovery unit tests repeat the grace/client eligibility gate and verify
that it leaves records, counters and client lifetime fields unchanged. Wire
tests cover outside-grace OPEN/LOCK rejection and suffix veto. They do not
establish successful persisted restart recovery end to end. Add a persisted
client restart/grace fixture exercising CLAIM_PREVIOUS, reclaim LOCK, duplicate
replay, RECLAIM_COMPLETE, and refusal after completion, with compound-run
assertions. Mutating finish-time EAGAIN tests require actual backend rollback.

The legacy LOCK/LOCKU fixes retain typed DENIED data and consume their early
OLD_STATEID/INVAL outcomes. Two residual parity risks deserve targeted tests:
legacy LOCK recovery errors still occur before owner-seqid classification, and
legacy OPEN_CONFIRM's OLD_STATEID/replay-op handling differs from the new
journal path. Sessionless connections and deliberately refused construction
must exercise these legacy branches; success on a bound, converted request
does not establish fallback parity.

Recommended next order: finish the v4.0 lifetime/replay tests; convert immutable
error and grace guards; extend anonymous admission coverage; add compound
credential and logical-namespace transitions; then design delegation/pNFS and
backend-token coordination. Independently settle capacity and successful
recovery semantics before treating a wire compound as one backend transaction.
