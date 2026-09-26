# ParentLeaseKey actor plumbing (wave 19)

Legacy RENAME, replacement LINK, and clean CLOSE delete-on-close passed only the
16-byte ParentLeaseKey into VFS completion. That lost the SMB protocol and client
identity required by `chimera_claim_owner_same_key`, so the mutating client's
own directory lease was recalled. Weakening key comparison would instead exempt
unrelated clients using the same bytes.

Actor-aware RENAME/LINK/matched-REMOVE entrypoints preserve existing exported
signatures and the SDK request layout. They copy the full actor through async
permission gates into the existing request `io_owner`. File recall retains that
canonical actor. Completion copies it and overlays ParentLeaseKey only for the
directory-content event. Missing actor or parent key grants no exemption. The
legacy key-only notify entrypoint remains exported and conservatively breaks all
leases; `notify_emit_actor` is the explicit client-scoped API.

The RENAME DAC gate now allocates its own context before any mutation: its two
resolved file handles plus copied actor exceed the fixed SDK scratch size. Other
gates retain their existing allocation scheme. Allocation failure reports ENOSPC
before dispatch.

Typed RENAME/LINK dispatch receives the same actor, while NO_NOTIFY continues to
suppress tentative notifications. Accepted SMB publishers remain responsible for
their event. SET_INFO size/allocation preparation and metadata publication also
retain the canonical open owner after a nonlease durable reconnect changes the
session's client identity.

Regression sources:

- `smb2_create_cache_compound_probe.c`: replacement LINK fallback and durable DOC
  fallback with equal parent-key bytes on two clients; mutator spared, peer
  recalled. These exercise real wire paths without injecting rejection after
  namespace mutation.
- `smb2_metadata_compound_probe.c`: LINK interposition follows the new actor API
  and owns its held actor copy across asynchronous completion.
- RENAME probe actor signature and legacy caller are maintained by the namespace
  lane; clean CLOSE caller is maintained by the CLOSE lane.

This is a correctness prerequisite, not conversion of replacement LINK or durable
DOC to native command spans. Existing fallback match semantics and backend
capability requirements remain unchanged. No worker build or tests were run;
combined root validation now passes in wave19 build9 and all 52 selected tests.
See [combined status](smb-compound-current-status.md).
