# SMB wave15: ReplaceIfExists hardlink compounds

A FILE_LINK_INFORMATION request with ReplaceIfExists can now stay in its native
VFS compound when the destination is absent. It uses the existing typed LINK with
replace=0, preserving the backend's atomic collision check. An EEXIST result has
no namespace effect: the frontend clears tentative notification state and defers
the untouched command, accepts the preceding prefix, then runs the existing
replacement lifecycle. Other failures retain their original error mapping.
Occupied same-inode aliases also retain that boundary; the implementation does
not infer a no-op from a racy lookup or change legacy link-count semantics.

LINK resolves its share root at execution from the command's namespace view.
This includes a related CREATE's private pending path, which can hold a root FH
before the tree cache is published. The root PUTFH uses a valid construction
placeholder, then a private prepare callback binds the actual view. Previously,
snapshotting the tree cache during construction could leave a cold-tree related
LINK with an empty root handle.

The backend receives NO_NOTIFY. The accepted frontend publisher emits FILE_ADDED
for a successful native link. A failed no-replace attempt emits nothing before
legacy fallback. Existing-target replacement remains a lifecycle boundary;
prebackend identity registration/stale-handle protection is still required for
safe conversion of that path.

Metadata fixture coverage includes related CREATE/LINK/QUERY/CLOSE on a cold tree,
a producer prefix followed by collision fallback, root and nested destinations,
link counts observed between operations, same-inode fallback, a real destination
created by another connection after parent resolution, preserved bytes through
an already-open replaced target, notification counts, and missing-parent errors.

The rename fixture also closes wave14's cancellation gap: it wraps the private
completion of the first successful typed rename, cancels the optional suffix,
and checks STATUS_CANCELLED, the accepted physical move, cached source and alias
paths, exactly one notification, and later DOC removal of the correct link.
The wrapper preserves separate prepare and completion contexts. This test accepts
the real mutation; it never simulates transaction rollback.

Combined validation is recorded in the current-status document.

Coordinator validation: wave15 full Debug/ASan build5 and all 51 selected CTest
entries pass, including five SMB profiles of 94 traces each. See the
[current status](smb-compound-current-status.md) for results and remaining scope.
