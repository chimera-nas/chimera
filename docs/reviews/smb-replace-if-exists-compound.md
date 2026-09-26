# ReplaceIfExists regular-base RENAME

Wave13 implementation checkpoint; root owns combined build/test validation.

Setting ReplaceIfExists no longer unconditionally selects the legacy path.
Eligible regular-base renames first run the same typed, atomic NOREPLACE and
MATCH_SOURCE_FH operation as ordinary nonreplacing moves. When the destination
is absent at backend execution, the move, private path updates, related stream
queries, and later commands remain in the same VFS compound. This includes a
source created by an earlier command and chained/cross-parent moves.

An occupied destination returns EEXIST without changing either link. Only that
result, with ReplaceIfExists requested, defers the untouched command to the
existing replacement lifecycle after acceptance of the preceding prefix. There
is no speculative destination lookup that could turn a subsequent replacement
into an unguarded native operation. Source mismatch, access/sharing failures,
and other backend errors remain command errors; they do not trigger fallback.
The attempted native path change is discarded and publishes no notification.
The ordinary retry reset also discards it before a repeated readonly attempt.

The wire fixture now covers:

- One-compound absent-target moves with live stream holders, chained names,
  cross-parent moves, and a distinct hardlink alias.
- Related CREATE/RENAME/QUERY/CLOSE both with an absent target (one compound)
  and with an occupied target (accepted producer prefix, legacy replacement,
  then a native suffix). The latter verifies the old target FileId still reads
  its old bytes and that the failed native attempt emitted no rename event.
- Destination creation/removal after parent resolution, using a pause before
  checked RENAME dispatch and a second wire connection. Appearance takes the
  replacement boundary; disappearance completes the original native run.
- Strict source identity rejection after another inode replaces the opened
  pathname; the replacement's bytes remain intact and the requested destination
  is not created.
- Safe same-name ReplaceIfExists retry, without rejecting any filesystem
  mutation; same-inode hardlink fallback preserving both actual links/bytes.
- The older pending stream-DOC legacy test now creates a real replacement
  target so it continues to exercise legacy accepted path publication.

## Wave14: atomic same-inode outcomes

A ReplaceIfExists rename onto another hardlink of the same regular inode now
stays in the original VFS compound. After the initial NOREPLACE reports EEXIST,
typed LOOKUP resolves the destination. Matching source and destination FHs are
then immutable inputs to the backend's atomic checked rename. A mismatched or
missing destination returns ESTALE without changing either link. A successful
same-inode result is explicitly NOOP, so neither private nor accepted SMB path
publication runs and no rename notification is emitted.

The VFS adds MATCH_DEST_FH and authoritative rename outcome capabilities, with
memfs implementations. The result-aware entrypoint preserves the older callback
APIs. Every request/output begins UNKNOWN; unsupported backends do not claim an
atomic outcome. The module SDK version is now 4 for the request-layout change.
Legacy SMB rename also consumes the atomic result, repairing its same-inode
hardlink bug: the operating source FileId retains its original cached path,
and later DOC removes the original link rather than the alias. VFS notification
publication suppresses proven no-ops for legacy callers as well.

Tests cover native alias/query coalescing, safe no-op finish retry, native and
forced-legacy alias/DOC source-link preservation, and no rename notification.
VFS tests cover stale/missing destination identity, explicit capability refusal,
nonroot DAC-gated stale destination and successful matched replacement, atomic
no-op results, and preserved source/target names.

The initial successful NOREPLACE completion records its private path change
immediately, so cancellation of the optional replacement suffix cannot hide an
already completed move from accepted-prefix publication. Wave 15 adds a dedicated
wire regression that wraps the initial RENAME descriptor's private completion,
records its real successful move, then cancels the optional suffix while the VFS
executor still owns the callback frame. It checks STATUS_CANCELLED, the moved
namespace and file bytes, the source open's updated path, one accepted rename
notification, and subsequent DOC removal of the new link while an independent
hardlink survives. No finish rejection is injected after the filesystem mutation.
No-op suppression covers both synchronous VFS notification gates and the ordinary
rename completion publisher.

## Residual scope

Actual replacement of a distinct target inode remains legacy, including targets
that appear to have no current ACCESS claims. That observation is insufficient:
a legacy OPEN can acquire the old target's backend handle, pause before ACCESS
admission, and resume after replacement acceptance and fence release. It could
then publish the removed inode using the destination pathname. Native pending
OPEN tokens also require exclusion before backend handle acquisition, not just
an ACCESS barrier after it. Safe conversion needs a lifecycle/path reservation
covering both native and legacy constructors, or generation validation that
prevents delayed old-target publication. No unused speculative target-fence
mechanism from that investigation remains enabled.

Directory and stream-itself RENAME, directory descendants, cross-view translation,
destination-ancestor stability, and SET_REPARSE identity migration are unchanged.
Strict matched rename and authoritative outcome capabilities are currently memfs
only. Other backends retain their existing behavior and cannot use the new native
same-inode alias path without implementing the atomic contract.

Wave14 source checkpoint; root owns combined build/test validation. Wave13 prior
checkpoint passed all 51 selected CTest entries and five 94-trace SMB profiles.

Coordinator validation: wave14 full Debug/ASan build5 and all 51 selected CTest
entries pass, including five SMB profiles of 94 traces each. See the
[current status](smb-compound-current-status.md) for validation and remaining scope.

## Wave15 cancellation coverage

A dedicated SMB regression now cancels immediately after the first successful
typed rename completes, before its optional suffix. The accepted move preserves
cached source/alias paths, emits exactly one rename notification, and later DOC
removes the new source link while preserving an independent alias. The wire
operation reports STATUS_CANCELLED. This closes the wave14 test gap without
rejecting finish after a real mutation. Wave15 combined validation passes all
51 selected CTest entries; see the current-status report.
