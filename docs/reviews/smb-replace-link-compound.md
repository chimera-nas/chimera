# SMB ReplaceIfExists hardlink compound slice (wave 15)

SET_INFO FILE_LINK_INFORMATION now admits ReplaceIfExists into the typed LINK
compound path for ordinary eligible file opens. The initial VFS LINK always uses
replace=false. If the destination is absent, it links the source atomically and
continues adjacent native commands in the same compound. Root and nested target
parents are resolved by typed operations, and the source handle and claim owner
remain those of the command's admitted open.

An existing destination produces EEXIST without a namespace mutation. Completion
clears the staged notification and requests an accepted-prefix boundary before
redispatching that command through the existing legacy replacement policy. Only
a collision with wire ReplaceIfExists set triggers this boundary; ordinary
collisions and missing-parent errors retain their existing status. This also
preserves existing same-inode replacement behavior rather than inventing a new
no-op outcome. No distinct-target replacement admission has been enabled.

The LINK target root is now bound at execution from the command's namespace
view. In a related CREATE/LINK packet, this includes the producer's private root
identity; the cold tree cache is not published until acceptance. Construction
uses a valid placeholder FH, replaced by the prepare callback before PUTFH runs.
This also fixes the same cold-tree limitation for ordinary nonreplace LINK.

## Backend contract

The new slice relies on the existing ordinary LINK nonreplacement contract,
not on a backend silently honoring a new flag. Source inspection found:

- memfs checks replace=false and returns EEXIST under parent/source inode locks.
- diskfs's transactional destination check rejects EEXIST when replace=false.
- Linux and io_uring use linkat without an unlink step.
- NFS backends issue protocol LINK, which does not replace an existing name.
- The SMB backend encodes the requested replacement boolean, here false.

Callbacks only update attempt-private state. LINK suppresses its immediate VFS
notification; accepted publication emits the successful native link event. An
EEXIST attempt emits none before the legacy operation. Tests never inject finish
rejection after a real link or rename mutation, because backends still lack
transaction rollback.

## Regression coverage added

The existing metadata wire probe now covers cold-tree CREATE/LINK/QUERY/CLOSE in
one compound, with an assertion that CREATE really resolves the share root;
root and nested absent targets; occupied-target fallback with a related CREATE
producer; same-inode collision fallback; missing-parent status; and a competing
CREATE that installs the destination after typed parent resolution but before
LINK dispatch. The race asserts no early notification, exact native submission
groups before and after the boundary, one successful link event, prefix/suffix
link counts, source bytes through the replacement name, and unchanged bytes
through an already open old destination handle.

The rename wire probe also adds the wave 14 cancellation gap regression:
a successful initial NOREPLACE move followed by cancellation of its optional
replacement suffix must publish the accepted path and rename event, even though
the wire command reports STATUS_CANCELLED. Subsequent DOC removes that new path,
while an independent hardlink survives.

## Remaining namespace boundaries

An occupied distinct-target rename or LINK replacement still runs existing
legacy policy. Moving replacement fully into a native compound requires safe
participation by opens before backend acquisition or equivalent stale-identity
validation for both native and legacy CREATE. An empty claim scan plus ACCESS
fencing does not protect a legacy OPEN paused between backend acquisition and
claim admission. Directory/stream rename and the broader SET_REPARSE conversion
remain separate work.

Build and execution results are recorded by the integrating agent; this worker
did not run builds or tests.
