# Native single-command CLOSE cancellation

Validated in wave19 build9, including the corrected running-coordinator cancellation
fixture and the combined 52-test suite. See [combined status](smb-compound-current-status.md).

SMB2 CANCEL can now find a submitted native VFS batch containing exactly one
CLOSE. The connection owns an event-loop-only discovery list; it does not own
additional request storage or context references. Existing native batch pins
retain the wire requests, session snapshot, tree, open and backend resources.

Registration occurs immediately before compound submission, after input gathering
and DOC preflight. Completion unregisters before accepted publication or aggregate
reply generation. Lookup checks the connection generation, resolved session ID,
and exact MessageId (or a nonzero assigned AsyncId). It invokes VFS cancellation
and never dereferences the batch afterward because completion may be synchronous.

A CANCEL during running pre-retirement coordination stops CLOSE before resource
retirement. The CLOSE/notify fixture holds an actual COORDINATE, sends a real wire
CANCEL and an ECHO ordering barrier, verifies the watch remains attached, then
releases the operation and expects CANCELLED. Cancellation at the finish boundary
is too late: VFS ignores it and accepted CLOSE publishes its normal outcome. No
request flag converts that accepted result into an apparent rollback.

Disconnect drains by unlinking each entry before cancellation. Later completion
sees an already-unlinked batch and cannot touch a recycled connection's list.
Connection allocation initializes the head, and normal batch cleanup asserts that
no active registration remains. Generation checks reject stale lookup and catch
an unexpectedly retained link at unregistration.

Explicit remaining boundaries: batches containing multiple commands are not
registered because current VFS cancellation is batch-wide and could affect other
commands; input-gathering and DOC-preflight waits before submission are not
registered; this registry does not generalize native cancellation to other SMB
operations. A one-command CLOSE batch within a larger SMB wire compound cancels
only that batch; separately executed prefixes/suffixes remain outside its scope.
