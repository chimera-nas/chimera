# Directory and stream cache CLOSE compounds

Wave13 adds two bounded cases to the existing terminal caching CLOSE path:

- A nondurable directory open holding a directory lease, with no pending
  notification or delete intent.
- A nondurable named-stream open holding a legacy oplock or RqLs grant, with no
  delete intent on either the stream or its base.

Both use the cache grant's existing file identity. The grant, caching file state,
ACCESS owner, and retained input handle must agree. Directory flags require a
DIR_LEASE construct; streams keep their ordinary data-cache construct. A stream
also requires its independent base ACCESS owner and matching base file identity.

Construction pins the grant and its file state. Execution takes the existing
namespace fence; streams take both stream and base fences. The journal retires
both ACCESS owners together. Held finish leaves the public FileId, grant members,
cache rights, and stream base handle intact. Accepted publication unhashes the
FileId and detaches its grant member. After journal publication, release revokes
an empty grant and drains cache references. A surviving shared member keeps the
grant and its handle anchor.

Caching CLOSE still ends the VFS run, so later commands see accepted grant state.
Fence contention or newly observed delete intent defers before claim retirement;
the legacy close path can settle recalls before waiting on namespace cleanup.
Stream/base DOC, persisted and durable state, pending notifications, and legacy
lock entries remain deliberate boundaries. No stream-specific deletion is
implemented by this change.

The existing cache-close wire probe now covers held directory/shared-stream
closes, directory content recall, stream OPEN recall, legacy stream BATCH close,
both regular and directory stream bases, base ACCESS retirement, and the cached
stream DOC fallback. It does not reject finish after backend mutation. Root
integration owns building and running this regression with the full suite.

Coordinator validation: wave13 full Debug/ASan build and all 51 selected CTest
entries pass, including this slice's wire fixtures and five 94-trace SMB profiles.
See [current status](smb-compound-current-status.md) for scope and remaining work.


## Wave14: nonpersistent durable CLOSE

Regular, nonstream durable-v1/v2 opens now use the native CLOSE path when they
have no DOC work, persisted backend record, pending notification, or legacy
lock list. Both legacy oplocks and shared RqLs grants are supported. These
CLOSEs remain terminal: their preceding metadata commands coalesce and any
suffix runs after accepted registry and cache publication. Actual durable
CREATE/reconnect stays on its existing lifecycle path.

A resolved CLOSE pins its open, so a concurrent reconnect cannot rehome it.
Disconnect may still park that open while finish is held. Accepted CLOSE now
takes the original tree bucket and then the durable registry lock, removes the
matching live hash entry and/or parked registry entry, and transfers exactly
one owning reference to retirement. If a concurrent sweeper or legacy CLOSE
already owns retirement, the compound does not take a second ownership. The
same atomic detach fixes the existing nonpersistent resilient CLOSE case.
Registry entry destruction happens after both locks are released. Cached grant
membership still detaches only on acceptance, and cache teardown follows claim
journal drain.

The cache-close probe adds actual v1/v2 durable handles, a shared strong-lease
survivor, held-finish registry/member checks, and safe finish rejection/retry of
a metadata+CLOSE-only attempt. A deterministic disconnect transition parks the
open at finish, then checks registry removal and the parked tree pin's release
following acceptance. Every case checks stale reconnect rejection and a later
exclusive open to detect leaked access reservations. No backend mutation is
rolled back by these tests. Root integration owns build/test validation.

Persistent records, durable stream/directory CLOSE, all stream/base DOC work,
pending notifications, and removal of terminal cache/durable batching remain
outside this bounded slice.

Coordinator validation: wave14 full Debug/ASan build5 and all 51 selected CTest
entries pass, including five SMB profiles of 94 traces each. See the
[current status](smb-compound-current-status.md) for validation and remaining scope.

## Wave15: durable directory and stream CLOSE

The nonpersistent durable path now also admits directory leases and named
streams, on both regular-file and directory bases. Persistence, notification,
legacy-lock and stream/base-DOC boundaries remain. Cache/durable CLOSE still
ends the VFS run.

A named stream now validates its independent base handle, base file state and
canonical base ACCESS claim at construction and again before journal retirement.
The claim must reference that exact base handle and file identity. This applies
to ordinary and durable streams, including warm-reconnected opens. The owning
open retains the base handle until journal drain and final base-claim retirement;
retiring the stream handle alone cannot release that anchor. Existing paired
namespace fences stabilize both identities against new delete intent, and the
wave14 atomic live/parked owner transfer applies unchanged to these objects.
Directory caching is admitted only for the existing DIR_LEASE construct.

The existing cache-close probe now creates actual durable grants for directories
and both stream-base types, asserting the durable reply context and registry
entry. For each type it covers shared-grant survival, metadata/CLOSE-only retry,
deterministic parking before acceptance, and real transport disconnect followed
by warm reconnect on a new tree and native CLOSE. Stream cases include durable
v1/BATCH and v2/RqLs. At held finish both ACCESS owners and the base handle remain
public; a pure foreign DELETE admission probe still sees the base share-denial
claim. Accepted CLOSE drains the registry, both reservations and parked tree pin.
Subsequent deny-all base OPEN, stream-byte checks and directory-child OPEN check
cleanup and preservation. No finish rejection follows backend mutation. Combined
build/test validation belongs to the coordinator.

Stream/base DOC was reviewed but intentionally remains separate: its existing
retirement routine allocates/publishes pending deletion metadata and changes
public opener flags. Moving it into retryable completion callbacks would violate
the contract; conversion needs a private last-holder/deletion journal and typed
matched stream removal, with notifications and pending-state ownership published
only after acceptance.

Coordinator validation: wave15 full Debug/ASan build5 and all 51 selected CTest
entries pass, including five SMB profiles of 94 traces each. See the
[current status](smb-compound-current-status.md) for results and remaining scope.
