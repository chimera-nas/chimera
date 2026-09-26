# SMB wave13: bounded shared-key CREATE

Source scope (combined validation is recorded by the root integration pass):

- Ordinary regular-file CREATE, OPEN and OPEN_IF carrying RqLs READ or NONE now use native VFS command groups. The request must specify NON_DIRECTORY_FILE; named streams, parent-lease-key coordination, higher requested caching modes, truncation and durable contexts retain explicit boundaries.
- CREATE ends its VFS run. Only accepted publication attaches the new public member and ACCESS own_cache link; the suffix sees the accepted grant. The preallocated candidate is private across retries. A same-key request joins the existing grant, preserving its stronger rights, lease version, epoch and break state; a fresh READ grant may be declined without failing the accepted filesystem operation.
- Recall actors and ACCESS claims carry the LeaseKey. Coordination excludes the requester's own grant from ACK waits and timeout revocation, permitting same-key reopen during a break to report BREAK_IN_PROGRESS.
- Pure post-open identity validation preserves the lease-key-to-file check. A typed preflight LOOKUP rejects a missing CREATE/OPEN_IF name when the key is already bound, before namespace mutation. These checks retain the existing cross-request check/publication race; they are not a new session-wide atomic key reservation facility.
- CREATE response lease-version data is copied into attempt-owned storage. A related CLOSE can retire the live grant before wire encoding without leaving a dangling response pointer.

Core hardening:

- Atomic member acquisition accepts a fresh initial epoch. It is initialized before any claim visibility, and coalescing never overwrites the existing grant epoch with the requester epoch.
- Fresh grant admission publishes the claim and grant registry entry under one file lock. A racing same-key acquisition coalesces before candidate insertion. Previously a candidate could be visible to a break/pin before the losing creator unconditionally freed it during racing collapse.
- Generic claim admission and the old optional member_seed/member_seeded API remain available. The accepted RqLs publication helper performs no allocation, recall, waiter pumping or backend I/O.

Integration support for cached-stream CLOSE uncovered a legacy CREATE identity error: lease-key validation compared the base FH against a stream grant. The check now uses the actual stream FH. For an already-bound key, nonmutating OPEN_STREAM preflight rejects creating a different fork and reuses the resolved handle for valid same-fork operations, avoiding a reopen/truncate identity window. Different existing forks and the base file remain invalid key targets.

Regression coverage extends the existing create-cache and claim-access fixtures: v1/v2 READ grants; READ/NONE and opposite-version joins; stronger-grant preservation; wrong-file key rejection without creating missing names; held and rejected read-only OPEN finish with unchanged public membership/epoch; same-key reopen during an outstanding break; terminal grouping; concurrent first-acquire membership/epoch consistency; and seeded epoch visibility to the first break callback. No finish rejection is injected after filesystem mutation.

Remaining work includes native RH/RW/RWH requests, directory/stream lease CREATE, parent-key coordination, durable/reconnect CREATE, removal of terminal CREATE caching boundaries, and session-wide atomic LeaseKey identity reservation.

Coordinator validation: wave13 full Debug/ASan build and all 51 selected CTest
entries pass, including this slice's wire fixtures and five 94-trace SMB profiles.
See [current status](smb-compound-current-status.md) for scope and remaining work.
