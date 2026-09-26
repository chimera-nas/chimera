# CHANGE_NOTIFY admission and CLOSE ownership

Wave19 implementation validated by the root agent in build9 and the combined
52-test suite, including real signed/encrypted notification and LOGOFF cases.
See [combined status](smb-compound-current-status.md).

Previously native CLOSE tested the absence of notify_state only while building
its batch. CHANGE_NOTIFY could install a watch later, including while compound
finish was held, leaving accepted native CLOSE with an unretired watch or parked
request. Deferring that CLOSE to legacy was also insufficient: the old legacy
cleanup could send and free requests owned by another SMB worker, and detached
watch storage had no independent reference held by those requests.

## Admission ordering

CHANGE_NOTIFY now enters the namespace DOC mutation guard, then the open bucket.
It validates CLOSED and the retained handle before installing or finding a
watch. This excludes watch publication while native CLOSE holds its DOC fence.
Existing watch attachments now remain eligible for native CLOSE, including a
watch installed between construction and fence acquisition. Operation callbacks
leave the attachment, queue and cleanup state untouched. Only accepted retirement
unpublishes the FileId and transfers its attachment under the open bucket. Outside
that lock, publication marks cleanup and wakes each parked request's owner before
publishing any DOC removal event. A canceled/rejected attempt cannot close a watch;
an accepted retirement prefix still cleans it if a later operation is canceled.

A conflicting CHANGE_NOTIFY does not install a watch or touch its queue. A
standalone or final request moves into the existing parked-notify representation
and sends its usual interim. An owning-worker timer retries admission; accepted
CLOSE then causes FILE_CLOSED, while a canceled/failed CLOSE can allow ordinary
watch admission. SMB CANCEL and connection teardown claim this pending admission
through the existing parked-notify list, remove its timer and release its
references. No original SMB request remains live and no second interim/async
credit is charged when admission succeeds. A nonfinal compound notification
retains its existing INTERNAL_ERROR behavior for asynchronous completion.

Admission validates the owning session under sessions_lock and retains that lock
through attachment publication. The timer resolves its owning connection's
session handle on every retry; missing/deleted sessions complete with
NOTIFY_CLEANUP even while a CLOSE fence remains held. PreviousSessionId cleanup
detaches watch states under the tree buckets, then queues cleanup outside the
session/bucket locks, before parking durable opens. This ordering catches
already admitted durable watches and lets a later warm reconnect install a fresh
watch instead of inheriting a permanently closing state.

## Lifetime and worker ownership

The open bucket protects the notify_state attachment. Every active handler and
parked notify holds an independent state reference. Each parked request also pins
its tree's memory so logical TREE_DISCONNECT cannot recycle the bucket locks or
share before the owning worker releases its open reference. The handler pins its
request context before FileId resolution, then transfers independent tree and
session memory pins to the parked request outside the notify-state lock. The
session pin retains the nonce counter referenced by encrypted reply snapshots
even after the last channel logs off and the session is logically retired.

Logical CLOSE detaches the attachment under the bucket, marks the state closing
and queues every outstanding request to its owning worker. It never sends or
frees another worker's request. Each parked request retains its state directly,
so cleanup does not dereference the now-detached open attachment.

Already-ready requests are marked cleanup as well; every affected owner's
doorbell is rung. The doorbell handles cleanup/on_ready_queue under state->lock,
including requests already extracted into its private ready list. Final state
release destroys the VFS watch outside state->lock; VFS registry destruction
waits for an in-flight callback before the state mutex/storage is freed.

The admission lock order is sessions registry → namespace registry → open bucket
→ notify-state when required.
VFS watch create/update runs with no notify-state lock held. Queue transitions
use notify-state → ready-queue; replies, open-reference releases and state
destruction happen outside both. VFS emit's registry → notify-state callback
therefore cannot invert against watch destruction.

The same detach/refcount cleanup applies to legacy CLOSE, AppInstance revocation,
tree teardown and session cleanup. Notification-bearing CLOSE is no longer a
boundary by itself; unrelated cache/DOC/stream/legacy-lock restrictions still
apply. Notifications remain frontend lifecycle state published after accepted
VFS retirement, not backend mutations performed by replayable callbacks.

At the transition to closing, cleanup snapshots and consumes any already-present
VFS deleted flag. That prior independent deletion retains DELETE_PENDING once,
while an own-DOC removal emitted after cleanup cannot change its queued result.
This is necessary because request references keep the detached VFS watch alive
until all owning workers have sent their replies.

LOGOFF now snapshots/pins every matching parsed wire slot before freeing the
connection session handle. It marks the session deleted and flushes all admitted
watch attachments before completing the reply; owning workers retain their own
requests and secure snapshots. Dispatch rejects later commands inheriting the
logged-off snapshot, including SESSION_SETUP, without treating the embedded
snapshot as a connection-owned hash entry. No request memory is accessed after
completion. Broader sibling-channel open/claim retirement remains governed by
existing session lifetime rules; this change specifically closes the watch gap.

## Regression fixtures

The new notify-close probe adds deterministic real-wire cases for:

- Native CLOSE held at finish before CHANGE_NOTIFY, with accepted completion,
  safe read/resource-only EAGAIN retry, cancellation of pending admission and
  disconnect cleanup.
- A nonfinal compound notify waiting behind the CLOSE fence, with no watch,
  interim or parked request created.
- Watch installation after native CLOSE construction but before its fence;
  native execution retains that attachment until accepted cleanup returns
  NOTIFY_CLEANUP.
- Real PreviousSessionId invalidation with a durable directory's pending
  admission behind a held CLOSE fence, and separately an admitted watcher.
  Both finish NOTIFY_CLEANUP; the admitted case warm reconnects and successfully
  installs/cancels a fresh watch.

An additional internal lifecycle fixture uses two real connection-owning server
workers. It transfers only one parked request's state reference and queue
membership into the other's watch, preserving both real connection/open/tree
owners. It then invokes real CLOSE with the first request already ready and
asserts both cleanup replies plus freeing on the respective owning threads.
This tests cross-worker queue/lifetime behavior; it is not end-to-end SMB
multichannel binding coverage.

Wave19 adds typed-retirement assertions for pre-existing watches, held acceptance,
read/resource-only finish retry, cancellation before and after retirement,
disconnect and idle-watch cleanup. A watched empty-directory DOC CLOSE exercises the existing legacy lifecycle
and returns NOTIFY_CLEANUP; directory DOC remains outside native CLOSE. A separate
internal event-injection fixture checks deletion before and after cleanup's
cutoff (DELETE_PENDING versus NOTIFY_CLEANUP), without claiming a backend removal.

Additional wire cases perform real authenticated SMB3 channel binding, assert
that both channels queue on the same state from different server workers, and
exercise sibling TREE_DISCONNECT while native CLOSE finish is held. Signed
ECHO/LOGOFF/ECHO and ECHO/LOGOFF/SESSION_SETUP (related and unrelated suffixes)
cover aggregate reply and dispatch lifetime. Encrypted last-channel LOGOFF
asserts that cleanup sends while its session is logically retired but still
memory-pinned. These are additional to the internal queue-transfer fixture.
