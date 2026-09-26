# Client lease-key admission and parent-key CREATE

Wave18 implementation; root integration owns build and test results.

A lease key previously checked only published opens in the current session.
Two sessions of one client, or simultaneous unpublished CREATEs, could bind the
same key to different files. The new shared registry uses the same client_key
identity as VFS grants (ClientGuid-derived, with the existing zero-guid session
fallback) plus the complete 16-byte LeaseKey.

Before any backend acquisition a CREATE reserves its client/key entry. A fresh,
unbound key has one constructor; another constructor waits without namespace or
ACCESS fences. Once the exact FH is bound, constructors share its immutable
identity reservation, allowing same-key joins and pending replay classification
without serializing behind an existing lease break. Read-only identity callbacks
consult the reserved entry. Native accepted publication attaches a preallocated
binding reference to the open; legacy publication does the same before hash
visibility. No binding allocation or fallible work occurs during native accepted
publication. Lease state NONE still binds the key, while unsupported legacy
policies which decline a lease do not retain a binding.

The open owns that reference until final retirement, including durable parking,
warm reconnect, stream handles, failed post-admission CREATE, and tree teardown.
The last constructor/open reference removes the entry. Thus a concurrent CLOSE
cannot erase the identity while an already-admitted constructor is checking it.
The table lock never invokes callbacks or acquires namespace, VFS, or open-bucket
locks. Root also extended lease ACK discovery to matching-client sessions.

The same first native COORDINATE and legacy admission wrapper acquire the
namespace agent's constructor token. PUTFH of the synthetic root supplies the
COORDINATE input without opening backend handles; real share/path resolution
comes afterward. Contended native admission defers the untouched command.
Legacy admission pins its session/tree context and uses owning-worker interim
and timer cleanup. No token is held while waiting on another admission token.
Retry explicitly releases admission and reacquires through each-attempt
COORDINATE; terminal release/completion and request recycling drain all ownership.
Pending same-GUID DH2Q replay must retain its existing immediate sane/Windows
profile answer instead of waiting for the original request's break ACK.

The conversion slice enables flagged lease-v2 ParentLeaseKey contexts for the
already supported regular-file and directory RqLs CREATE paths. Private opens
retain the parsed parent key; accepted namespace notifications apply the same
parent-key exemption as legacy execution, and replies echo the key. A held
compound finish cannot publish those notifications. This does not remove the
terminal caching CREATE boundary or enable stream leases, durability, AppInstance,
mandatory oplocks, create-time DOC, or previously unsupported directory policies.

Regression sources extend create_cache_compound_probe/inspect:

- Two real same-client sessions contend while the first native CREATE finish is
  held. Server-worker observation proves the second reached key admission; after
  acceptance it rejects a different filename without creating it.
- Same-file cross-session join, independent-client reuse, last-CLOSE key reuse,
  and a lease ACK from a same-client session with no local grant member.
- Parked durable directory/stream bindings reject a new filename on the
  reconnecting session before warm reclaim, while existing persistence proof
  tests remain unchanged.
- Regular-file and directory child CREATE with ParentLeaseKey execute natively;
  held finish leaves directory grants untouched, accepted finish recalls the
  other directory lease but exempts the supplied parent key, and replies retain
  the exact key.

No build/tests were run by this worker. Root must validate combined changes,
including existing replay profiles, cancellation/disconnect, and retry coverage.
Full cold recovery, persistent-record repair, tentative grant state for suffix
coalescing, and native stream-lease preflight remain separate work.

Integration audit also closed lookup-to-create races for already-bound keys:
backend OPEN_AT is existing-only, and directory MKDIR is skipped. FILE_CREATE
still reports collision for an existing ordinary leaf (preserving symlink
handling); a vanished leaf on a create-capable disposition reports invalid key
reuse without creating a new inode. Native flags are rebuilt on each attempt.
A deterministic VFS test hook removes the exact leaf after successful preflight
and before delivering that result, covering regular/directory and native/legacy
OPEN_IF paths and checking that the name remains absent.

The parent-key regression exposed an existing VFS notification defect: the
key-only notification API constructs an actor with zero protocol/client identity,
which cannot satisfy the current full-identity key comparison. CREATE now calls
a new full-actor notification API with SMB protocol, originating client_key, and
ParentLeaseKey. The test uses identical parent-key bytes on two different clients:
only the originating client's grant is exempt. Root/namespace integration migrates
other SMB notification producers and their interposition fixtures as well.

Initial SMB replay failures also exposed model omissions rather than lost wire
behavior: durable trace 0x41_2 state14 reused clientTag0/key1 on c while a live b
lease still held that key in another session; trace 0x41_5 state183 reused
clientTag18/key1 on a while a parked durable c lease retained it. Both correctly
fail INVALID_PARAMETER with the shared registry. Root owns model analysis and
any correction; this worker did not change expectations or traces.

Root verified the model correction against MS-SMB2 lease-v2 handling:
https://learn.microsoft.com/en-us/openspecs/windows_protocols/ms-smb2/fc4f8879-f295-4995-b71e-21f309d8d7c8
Lease lookup uses ClientGuid and LeaseKey, independent of SessionId. The model
now checks live clientTag and parked owner membership before namespace effects
or parked purge. Deterministic model tests cover a wrong-file request across live
sessions and after durable parking, verifying no new name is created; the live
case also checks a same-file join. All three SMB model self-test modules pass,
and the complete 94-trace corpus was regenerated from the corrected model.
