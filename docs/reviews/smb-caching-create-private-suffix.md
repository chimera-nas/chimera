# Caching CREATE: private suffix contract

Wave 20 allows a native caching CREATE to share its VFS compound with a bounded
suffix. Previously every caching CREATE ended the native run even when its
successor only queried or read the just-opened file.

## Supported sequence

The runtime consults the producer's `private_suffix_eligible` hook before
constructing each later command. For a caching CREATE, every admitted command
must use related-operation semantics and an all-ones persistent and volatile
FileId, selecting the same private produced open. QUERY_INFO, READ, and FLUSH
are admitted. RqLs lease CREATEs additionally admit WRITE. Ordinary command
eligibility and access checks still apply.

Noncaching CREATE retains its previous suffix eligibility. Another CREATE,
an unrelated command, a specific FileId, CLOSE, LOCK, namespace mutation,
or any other unsupported command ends the caching CREATE run. A legacy-oplock
CREATE also ends its run before WRITE. No unsupported command is initialized
or built speculatively before that boundary is chosen.

This admits one VFS compound for cached CREATE/QUERY/READ/FLUSH, and one for
RqLs CREATE/WRITE/READ/FLUSH. CREATE/QUERY/CLOSE still uses two compounds, with
QUERY now in the CREATE compound.

## Private state and publication

CREATE's candidate grant remains preallocated, unlinked storage. Tentative
execution never installs a grant, links a lease member, publishes a FileId, or
changes an existing same-key grant's mode/version/epoch. Existing repeatable
recall and admission coordination remains in place. Grant selection and
publication happen only after the VFS accepts the attempt.

The OPEN completion initializes the private ACCESS claim template before the
open becomes available to later groups. The producer's `private_actor_owner`
hook returns that template owner, including the requested LeaseKey. The public
open helper intentionally obtains its key from an installed grant and therefore
cannot describe a private same-key reopen. Keeping the key on private IO actors
prevents a suffix WRITE from recalling an already-public coherent same-key
lease. The key is an actor identity; it does not represent a published grant.

READ and QUERY do not depend on a granted cache mode. FLUSH only commits the
backend handle after its ordinary access checks, with no cache-state decision.
RqLs leases remain coherent with their own writes, including a READ-only grant.
Legacy LEVEL_II oplocks instead self-break on WRITE; their actual accepted grant
must be known before executing that suffix.

Failure in a supported suffix preserves ordinary SMB group semantics: a
successful CREATE prefix is published after acceptance, and a denied READ does
not erase the handle or prevent a later valid QUERY. A rejected read-only
attempt may repeat all private callbacks without exposing a provisional handle
or cache grant.

## Remaining CLOSE contract

CLOSE is deliberately still a boundary. The current closed-private-slot path
suppresses grant publication and leaves the CREATE reply at NONE. That is
insufficient for caching CREATE/CLOSE: a same-key reopen may join an existing
grant whose mode, lease version, and epoch must still be represented correctly
in the CREATE response, even when no new open survives acceptance.

Extending this slice requires a reply-only cache decision for a closed private
slot, or a private cache journal that expresses grant admission, joins/upgrades,
and member retirement in wire order. It must distinguish an otherwise-empty
grant from an existing same-key grant with surviving members; preserve the
CREATE reply's grant mode/version/epoch; and publish the final live membership
exactly once on acceptance. It must also define coordination against concurrent
grant changes without installing live speculative grants during a retryable
callback. Blanket reply NONE or temporarily publishing then removing a grant
does not satisfy that contract.

Legacy-oplock WRITE and namespace/LOCK suffixes likewise remain bounded until
their actual cache-mode or grant-lifecycle dependencies are represented.
Durable request/reconnect lifecycle eligibility is unchanged by this work.

## Regression sources

`smb2_create_cache_compound_probe.c` now asserts:

- Four groups in one submission for each legacy cache kind's
  CREATE/QUERY/READ/FLUSH, with a rejected read-only finish and successful retry.
- Three groups in one submission for a same-key RqLs CREATE/QUERY/READ retry,
  preserving the existing member count, grant state, and epoch until acceptance.
- Four groups in one submission for same-key RqLs CREATE/WRITE/READ/FLUSH,
  without a self-break or tentative member publication. This mutating case
  holds acceptance for inspection but does not inject finish rejection.
- An accepted CREATE prefix and later QUERY around an access-denied READ.
- A boundary before an unrelated specific-handle QUERY and before legacy
  LEVEL_II WRITE, retaining the latter's normal self-break behavior.
- Existing prefix-QUERY/CREATE/QUERY/CLOSE sequences now have groups 3+1 across
  two submissions, rather than the previous 2+2.

The existing `smb2_create_cache_inspect.c` checks public grant/member/FileId and
ACCESS-owner absence on held attempts. Cache-close probe sequences contain only
already-published opens, so their batch counts are unchanged. Root's wave20 full
Debug+ASan build5 and all 52 selected tests passed, including the CREATE/cache
probe. See [combined status](smb-compound-current-status.md) for validation logs.
