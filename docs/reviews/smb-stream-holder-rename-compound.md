# Base RENAME with live named-stream peers

Wave12 validated checkpoint; combined results are in smb-compound-current-status.md.

Nonreplacing regular-base RENAME no longer falls back merely because the base
has named-stream holders. The operating FileId must still name the regular base,
not a stream. Backend atomic NOREPLACE, source-FH matching and deferred
notification requirements remain unchanged. Directory and replacing renames are
still boundaries; so are cross-view moves without a usable path translation.

The existing private path journal already associates stream opens with the base
identity and exact parent/name link. Following stream queries therefore see the
renamed base path before acceptance, while unrelated hardlink aliases retain
their own paths. Accepted publication updates both the stream participant and
its base participant together under the namespace registry lock. Public stream
path access during validation/publication also takes the stream file lock when
it differs from the already-held base lock. Namespace writers retain the lock
order registry, base file, stream file, handle-cache shard. The legacy rename
publisher uses the same paired update and peer locking.

The existing base DOC/ACCESS fences stabilize path/delete-intent admission;
source rename share/cache arbitration remains in the typed VFS RENAME. A private
stream producer has an invisible pending base token rather than a public open.
An accepted concurrent rename updates that token only; stream CREATE later
promotes both identities and publishes its own fields. No prepare/completion
callback changes public namespace state, and notifications still publish after
accepted successful RENAME.

Added wire-fixture cases require one VFS compound for chained/cross-parent base
renames with stream queries, preserve stream data and hardlink aliases, verify
accepted paths in a later request, and coalesce ordinary stream closes. Base CLOSE remains a separate VFS run:
its preflight DOC admission cannot mix with stream CLOSE's late dual-identity
fences under the existing batching contract.
Additional cases exercise stream CREATE-DOC cleanup after base rename, a safe
same-name no-op finish retry, and a held existing-stream OPEN that accepts after
an external rename before another move and stream close. The held stream OPEN
only opens existing data, and its finish is accepted; no namespace mutation is
followed by an injected rejection.

This does not implement directory-descendant repathing, destination-ancestor
stabilization, replacing rename target identity migration, or SET_REPARSE
identity migration. Those require separate coordination and lifetime work.

A follow-up review also found deferred stream-DOC notification metadata left
behind by an already-closed DOC owner. Accepted native and legacy base rename now
repath that pending action under the stream file lock, requiring both base FH
identity and the exact old parent/name link. This update runs before filtering
the surviving public opener's link: the only remaining stream opener may use an
unmoved hardlink alias. Backend removal continues to use base/stream FH identity;
only its deferred event path changes. The fixture covers this sequence for native
nonreplace and legacy ReplaceIfExists rename and verifies the final STREAM_NAME
event's new basename and parent FH, while the surviving alias stays unchanged.
