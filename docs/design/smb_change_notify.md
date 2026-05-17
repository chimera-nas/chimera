# SMB2 CHANGE_NOTIFY in Chimera — Implementation Summary

## Feature

Adds full server-side support for SMB2 `CHANGE_NOTIFY` (MS-SMB2 §2.2.35 / §3.3.5.10) to the Chimera NAS daemon, including:

- Per-handle directory watches with `CompletionFilter` and `WATCH_TREE` semantics.
- Asynchronous "parked" requests: interim `STATUS_PENDING` response followed by a later final response when events arrive.
- `STATUS_NOTIFY_ENUM_DIR` overflow signaling per spec.
- Subtree (recursive) watch resolution via async reverse-path-lookup against the backend VFS.
- `SMB2_CANCEL` handling for outstanding notifies.
- Signing for async standalone responses (signed sessions must sign every reply, including the interim PENDING and the final body).

The work spans three layers:

1. **VFS notify subsystem** (`src/vfs/vfs_notify.{c,h}`): a new in-process pub/sub for filesystem events, with per-watch ring buffers, an event-emitter API consumed by `vfs_proc_*` operations, and an async resolver that maps descendant FHs back to relative paths for subtree watchers.
2. **SMB CHANGE_NOTIFY handler** (`src/server/smb/smb_proc_change_notify.c`, `smb_notify.{c,h}`): owns the SMB-side request lifecycle — parking, interim PENDING reply, callback-to-doorbell dispatch, final response with `FILE_NOTIFY_INFORMATION` records, cancel/close reaping, signing.
3. **Backend / call-site changes**: emit calls in mkdir/remove/rename VFS procs and the SMB create/write/set_info handlers; memfs and linux backends preserve `va_mode` on remove; libsmb2 wired in as a test-only dependency.

## Architecture

```
   producer side                       SMB side
   ─────────────                       ────────

   vfs_proc_{mkdir,remove,            chimera_smb_change_notify
   rename,...}                          │
        │                               ▼
        ▼                          state per open_file
   chimera_vfs_notify_emit            { watch*, pending* }
        │                               │
   ┌────┴────┐                          │
   ▼         ▼                          │
   exact     subtree                    │
   watches   (RPL resolve)              │
        │         │                     │
        │         ▼                     │
        │ sync_delegation_threads[0]    │
        │    chimera_vfs_getparent      │
        │         │                     │
        └────┬────┘                     │
             ▼                          │
        watch->callback                 │
   = chimera_smb_notify_callback        │
             │                          │
             ▼                          │
        thread->notify_ready  (per-SMB-thread queue)
             │                          │
             ▼                          │
        evpl_ring_doorbell ─────────────┘
                            (wakes SMB thread)
                                        │
                                        ▼
                               chimera_smb_notify_send_response
                                        │
                                        ▼
                                 wire: SMB2 async reply
```

### VFS notify (`src/vfs/vfs_notify.c`)

- `chimera_vfs_notify_init(struct chimera_vfs *)` — creates a sharded hash table of buckets (64 buckets, mutex per bucket), a mount-entries hash (per mount-id, holds subtree watches and the root FH cached at creation), an RPL cache, and a pending-event allocator.

- `chimera_vfs_notify_watch_create(notify, dir_fh, fh_len, filter_mask, watch_tree, callback, private_data)` — allocates a watch, inserts into the exact-watch bucket. If `watch_tree`, also links into the mount-entry's subtree list and creates the mount entry on first use.

- `chimera_vfs_notify_watch_update(notify, watch, filter_mask, watch_tree)` — atomically updates the filter mask (`__atomic_store_n` RELAXED, paired with `__atomic_load_n` in emit) and re-links between exact/subtree lists when `watch_tree` flips. When `watch_tree` flips, the ring is purged and `overflowed=1` is set to force a client rescan (paths/leaf-names of in-ring events would otherwise be ambiguous to a consumer with a different mode).

- `chimera_vfs_notify_watch_destroy(notify, watch)` — unlinks from bucket + (if subtree) from mount-entry list, both under their respective locks. Frees the watch. When the last subtree watch on a mount entry is removed, the entry itself is `HASH_DEL`'d and freed under `mount_entries_lock`.

- `chimera_vfs_notify_drain(watch, events_out, max, *overflowed)` — pulls events from the watch's ring buffer (32 entries, `CHIMERA_VFS_NOTIFY_RING_SIZE`), returning the overflowed flag from when enqueue saw a full ring or when an explicit `watch_overflow()` was called. Clears the overflow flag once the ring is empty.

- `chimera_vfs_notify_emit(notify, dir_fh, fh_len, action, name, name_len, old_name, old_name_len)` — the producer entry point. Three phases:
  1. Clamp `name_len` / `old_name_len` to `CHIMERA_VFS_NAME_MAX` defensively.
  2. **Exact watches** — under `bucket->lock`, iterate watches matching `dir_fh`, atomic-load `filter_mask`, if `(mask & action)` call `watch_enqueue` then `watch->callback`. The callback is fired *under the bucket lock* — this is the core lifetime invariant (see Locking below).
  3. **RPL cache invalidation** — on REMOVED / RENAMED, kill stale `parent → name → child` mappings in the cache.
  4. **Subtree watches** — under `mount_entries_lock`, look up the mount entry; if no subtree watches present, return. Special cases for subtree dispatch:
     - **`name_len == 0`** (cross-dir rename source-side, see Rename below) → overflow all subtree watches.
     - **Backend has no `CHIMERA_VFS_CAP_RPL`** → overflow all subtree watches.
     - **Pending-queue saturated** (`num_pending >= CHIMERA_VFS_NOTIFY_MAX_PENDING=256`) → overflow all subtree watches.
     - **`shutdown` flag set** → return silently (destroy is in progress).
     - **OOM on `alloc_pending`** → overflow all subtree watches.
     - Otherwise → allocate a `pending_event` and start async resolver.

- **Async resolver** (`chimera_vfs_notify_resolve`):
  - Walks `walk_fh → parent → grandparent → ...` toward the mount root.
  - At each step, all dereferences of the mount entry (subtree iteration AND the at-root check using `root_fh_len`/`root_fh`) happen inside the `mount_entries_lock` critical section, so the entry can be safely GC'd by `watch_destroy` when its last subtree watch is removed.
  - Skips subtree iteration at `depth==1` because that's the event's parent dir, already covered by the exact-watch dispatch.
  - Tries RPL cache first (`chimera_vfs_rpl_cache_lookup`); on hit, prepends the component synchronously and continues. On miss, calls async `chimera_vfs_getparent` via `sync_delegation_threads[0]` and returns; the callback resumes the walk.
  - Builds the relative path right-to-left in `pev->path_buf[CHIMERA_VFS_PATH_MAX]`.
  - At the mount root or on error/overflow/depth-cap, frees the pev and signals overflow to subtree watches on the mount where appropriate.

- **Delivery to a subtree watch** (`deliver_subtree_event`): if the accumulated `relpath_len > CHIMERA_VFS_NAME_MAX` (the ring entry's name buffer), overflow the watch instead of truncating; otherwise enqueue with the relative path.

- **Destroy**: sets a `shutdown` flag under `pending_lock`, then blocks (no timeout, logs every 5s) until `num_pending == 0`. New emits short-circuit the RPL pending path while shutdown is set; in-flight resolvers run to completion.

### Pending-event lifetime

- `chimera_vfs_notify_pending_event` is allocated from a free list (or `calloc`) under `pending_lock`. Holds the event metadata, the walk state (`walk_fh`, `depth`), the path buffer, and the leaf name.
- Freed (returned to the free list, `num_pending--`) on every terminal resolver outcome: root reached, ESTALE/EACCES, path-overflow, depth-cap, or successful delivery.
- Lifetime is bounded by the resolver chain: as long as a pev is alive, an async callback may still touch `notify->mount_entries`, `notify->rpl_cache`, `mount_entries_lock`. The destroy wait on `num_pending == 0` is the only safety against that.

### RPL cache (`src/vfs/vfs_rpl_cache.h`)

- 64-shard, RCU-readable forward+reverse hash table; entries hold `(child_fh, parent_fh, child_parent_name, fh_lens, rev_key)`. 30-second TTL.
- **Forward lookup** by `fh_hash` is O(1): sharded by `fwd_key = chimera_vfs_hash(child_fh, child_fh_len)`.
- **Invalidate** by `(parent_fh, name)` is O(num_shards): forward lookups are common (resolver hot path), invalidations are rare (rename/remove). The fwd-shard is computed from each candidate entry's `fwd_key`, so a single shard's eviction kills both indexes; the invalidator scans all reverse-key shards because it only knows `rev_key = parent_hash ^ name_hash`.
- Entries are freed via `call_rcu`. Readers use `urcu_memb_read_lock()`.

### SMB CHANGE_NOTIFY handler

`chimera_smb_change_notify(request)` in `smb_proc_change_notify.c`:

1. `chimera_smb_open_file_resolve` (refcnt++).
2. Validate it's a directory; reject `STATUS_INVALID_PARAMETER` otherwise.
3. Map the SMB `CompletionFilter` bits to a VFS event mask via `chimera_smb_map_completion_filter`.
4. If `open_file->notify_state` is NULL, create state and watch; otherwise call `chimera_vfs_notify_watch_update` to adopt the new filter / watch_tree (since a client can re-arm with different parameters).
5. **Under `state->lock`** (atomicity vs the VFS callback):
   - Reject duplicate-pending with `STATUS_INVALID_PARAMETER` (MS-SMB2 §3.3.5.10).
   - Drain the ring.
   - Re-filter drained events against the *current request's* filter (the watch's mask may have been broader when these were enqueued, e.g. before a `watch_update` narrowing).
   - If events or overflow → release lock, pre-fit-check the serialized size against the client's `OutputBufferLength` (treated strictly as a cap, including 0 = "no bytes allowed"), escalate to `STATUS_NOTIFY_ENUM_DIR` on truncation, write to `request->change_notify.events[]`, complete synchronously.
   - Otherwise → park.
6. Parking: reject if this CHANGE_NOTIFY is in a multi-command compound (`request->compound->num_requests > 1`) — the compound reply skips PENDING slots, which could confuse positional clients.
7. Allocate a `chimera_smb_notify_request` (`nr`); on OOM, complete with `SMB2_STATUS_INSUFFICIENT_RESOURCES` after releasing locks and the open_file resolve ref. Copy message_id, async_id, credit info, signing key, the per-request completion_filter, and the open_file pointer (which inherits the resolve()'s refcnt). Set `state->pending = nr` under `state->lock`, then push to `conn->parked_notifies` (single-threaded per conn). Send the interim `STATUS_PENDING` response as a standalone signed SMB2 message. Complete the original compound slot with `STATUS_PENDING` so the reply builder skips it (the interim already went out).

### Event delivery path

- VFS callback `chimera_smb_notify_callback` runs from inside `emit()` (under bucket/mount lock). It takes `state->lock`, nested `thread->notify_ready_lock`, links `nr` onto the per-SMB-thread ready queue, sets `nr->on_ready_queue=1`, and rings the thread's doorbell. Single attempt — if `pending` is NULL or already queued, no-op.

- The SMB thread's `evpl_doorbell` fires `chimera_smb_notify_doorbell_callback`, which drains `thread->notify_ready` and calls `chimera_smb_notify_send_response(nr)` for each.

- `send_response`:
  1. Take `state->lock`. If `state->pending != nr` (close/cancel claimed it), free `nr` and return.
  2. Drain the ring; on spurious wakeup leave `nr` parked.
  3. Re-filter drained events against `nr->completion_filter`. If all filtered out → re-park.
  4. Atomically claim: `state->pending = NULL`, release lock.
  5. Size the response buffer to `min(nr->output_buffer_length, CHIMERA_SMB_NOTIFY_MAX_RESP=64KB)` plus framing (with a 528-byte floor so a tiny request still has room for the body + padding). Build the SMB2 reply: header with PENDING-async flag, body with `StructureSize=9` + serialized `FILE_NOTIFY_INFORMATION` records. The serializer rolls back atomically if any event won't fit; the caller maps `consumed < nevents` to `STATUS_NOTIFY_ENUM_DIR` with empty body. The empty-body case emits the spec-mandated 1-byte zero padding (MS-SMB2 §2.2.36).
  6. Sign via `chimera_smb_sign_message` (standalone HMAC-SHA256 or AES-CMAC, per dialect) if `nr->signed_session`.
  7. `evpl_sendv(... EVPL_SEND_FLAG_TAKE_REF)`, unlink from `conn->parked_notifies`, free `nr` (releasing the held open_file ref).

### Cancel / Close paths

- `chimera_smb_notify_claim(nr)` is the atomic claim primitive: under `state->lock`, it clears `state->pending` if it equals `nr`, AND plucks `nr` from `thread->notify_ready` under the nested `notify_ready_lock`. Returns 1 if the caller now owns `nr`.

- `chimera_smb_notify_cancel(nr)` — claims, unlinks from `conn->parked_notifies`, sends signed `STATUS_CANCELLED` response, frees nr. Used by:
  - `chimera_smb_cancel` (SMB2_CANCEL handler, searches `conn->parked_notifies` by `async_id`).
  - `chimera_smb_notify_close` when the directory handle is closed.

- `chimera_smb_notify_drop(nr)` — claims, unlinks, frees. Sends nothing. Used by `chimera_smb_conn_free` on connection teardown (the bind is going away, no point sending a reply).

- `chimera_smb_notify_close(vfs_notify, state)` — peeks `state->pending`, cancels if present (releases `state->lock` first to satisfy the lock invariant; `cancel` re-acquires it briefly), then calls `chimera_vfs_notify_watch_destroy` (takes bucket/mount-entries locks), destroys `state->lock`, frees the state.

### Per-procedure emit call sites

- `vfs_proc_mkdir_at`: `DIR_ADDED` on parent.
- `vfs_proc_remove_at`: `DIR_REMOVED` or `FILE_REMOVED` on parent based on `S_ISDIR(r_removed_attr.va_mode)`. The completion handler strips `MASK_STAT` from `r_removed_attr.va_set_mask` after consuming `va_mode` so the downstream attr-cache insert (keyed by the removed object's FH, which on Linux is identical for all hardlinks to the same inode) does not pollute the cache with the pre-unlink `nlink` value.
- `vfs_proc_rename_at`:
  - Intra-dir: single `RENAMED` on the dir, carrying `(new_name, old_name)`. The SMB serializer expands this to a `FILE_ACTION_RENAMED_OLD_NAME` + `FILE_ACTION_RENAMED_NEW_NAME` pair.
  - Cross-dir: source dir gets `RENAMED(name=NULL, old_name=src)` (serializes as `RENAMED_OLD_NAME` only); dest dir gets `RENAMED(name=dst, old_name=NULL)` (serializes as `RENAMED_NEW_NAME` only). Matches Windows convention. The source-side `name_len==0` triggers a subtree overflow rather than running the resolver (which would otherwise build a leafless relpath).
- `smb_proc_create`: `DIR_ADDED` or `FILE_ADDED` based on `S_ISDIR(attr->va_mode)` of the post-create handle, on parent. Fires for `CREATE`, `OPEN_IF`, `OVERWRITE_IF`, `SUPERSEDE` (the create-capable dispositions; can yield spurious ADDED on OPEN_IF + existing file — documented limitation pending `create_action` plumbing through the VFS).
- `smb_proc_write`: `FILE_MODIFIED` on parent.
- `smb_proc_set_info` (non-rename path): `ATTRS_CHANGED` on parent.

### `FILE_NOTIFY_INFORMATION` serializer (`chimera_smb_notify_serialize_events`)

- Iterates input events, writes records via `chimera_smb_notify_write_record` which converts the UTF-8 name to UTF-16LE in place, computes the aligned record size, and chains `NextEntryOffset`.
- **Atomic events**: a snapshot of `(p, prev_entry)` is taken at the start of each event; if any record of a multi-part event (e.g. rename emits both OLD_NAME + NEW_NAME) fails to fit, the whole event is rolled back. Returns total bytes written and reports `*events_consumed`. The caller uses `consumed < nevents` as the trigger to escalate to `STATUS_NOTIFY_ENUM_DIR`.
- Skips name copies when `name_len == 0` (used by cross-dir-rename source/dest emits).

### Compound + status whitelist (`smb.c`)

- `chimera_smb_is_error_status` and `chimera_smb_status_should_abort` treat `STATUS_NOTIFY_ENUM_DIR (0x0000010C)` as success (severity 00 per NTSTATUS), so the compound reply emits the regular CHANGE_NOTIFY body, not an SMB2 error body.
- The compound reply builder skips any request with `status == STATUS_PENDING` — these are async-parked notifies (and SMB2_CANCEL, which has no response per spec). `chimera_smb_cancel_reply` was therefore dead code and has been deleted.
- `chimera_smb_sign_compound` skips PENDING slots (no header was written) and emits a defensive error if a non-PENDING slot has `SIGN` flag but no `session_handle`.

## Locking invariants

Documented in code; load-bearing for safety.

1. **Order:** `bucket->lock` / `mount_entries_lock` (registry-side) → `state->lock` (downstream) → `thread->notify_ready_lock`.
2. `chimera_vfs_notify_emit` fires `watch->callback` *while holding* the registry lock. The callback acquires `state->lock` next. This intentional inversion keeps the watch's `private_data` (the SMB notify state) alive across the callback: `watch_destroy` must wait for the registry lock, and therefore for any in-flight emit to finish.
3. **No code path** outside of `emit` may take `state->lock` and then `bucket->lock` / `mount_entries_lock` — that would AB-BA deadlock against an in-flight emit. `chimera_smb_notify_close` releases `state->lock` before calling `watch_destroy`; `chimera_vfs_notify_watch_update` does not take `state->lock` at all. Both have prominent block-comment annotations.
4. **`watch->filter_mask`** read/written outside the watch lock uses `__atomic_load_n` / `__atomic_store_n` (RELAXED ok because the SMB layer re-filters at response time).
5. **Connection / thread invariant**: a `chimera_smb_conn` is bound to exactly one SMB thread for its lifetime. All CHANGE_NOTIFY operations (park, callback-via-doorbell, cancel, drop, close, send_response) for that conn run on that thread's event loop and are therefore mutually serialized.

## Configuration / constants

- `CHIMERA_VFS_NOTIFY_RING_SIZE = 32` (per-watch event ring).
- `CHIMERA_VFS_NOTIFY_NUM_BUCKETS = 64` (exact-watch hash buckets).
- `CHIMERA_VFS_NOTIFY_MAX_PENDING = 256` (concurrent resolver pevs).
- `CHIMERA_VFS_NOTIFY_MAX_DEPTH = 64` (resolver hop cap).
- `CHIMERA_VFS_NAME_MAX = 256`, `CHIMERA_VFS_PATH_MAX = 4096`.
- `CHIMERA_SMB_NOTIFY_MAX_RESP = 65536` (hard cap on a single CHANGE_NOTIFY response buffer).
- RPL cache: 64 shards × 16 slots × 4 entries × 30s TTL.

## Tests

- **`chimera/vfs/notify_test`** (`src/vfs/tests/vfs_notify_test.c`, ~17 sub-tests, ~55 assertions, sub-second, ASAN clean): exact-watch enqueue/drain, rename event shape, filter mask, FH mismatch, ring overflow, callback delivery, `watch_update` filter and tree-flip transitions, RPL cache cross-shard invalidate, oversize name clamp, subtree on non-RPL backend overflows, subtree on max-pending overflows, file-vs-dir action discrimination, cross-dir rename split, cross-dir source-side `name_len==0` overflows subtree.

- **`chimera/server/smb/change_notify_test`** (`src/server/smb/tests/smb_change_notify_test.c`, 16 wire scenarios via libsmb2 against an in-process server, netns-isolated, ~3s, ASAN clean): create/unlink/rename/mkdir/rmdir notify, filter rejects mismatched events, duplicate CHANGE_NOTIFY rejection, tiny `OutputBufferLength` → `NOTIFY_ENUM_DIR`, `OutputBufferLength=0` → `NOTIFY_ENUM_DIR`, compounded CHANGE_NOTIFY rejected, DIR_NAME-only watch receives mkdir/rmdir, cross-dir rename source-side delivers `RENAMED_OLD_NAME` only, cross-dir rename destination-side delivers `RENAMED_NEW_NAME` only, CLOSE while parked → `STATUS_CANCELLED`, broad→narrow filter drops stale ring events.

- **Windows interop**: validated against Microsoft FileServer test suite's `test_change_notify.exe` (`10.65.175.24` → `10.65.181.198`): 11/11 BVT scenarios pass. NOTIFY_ENUM_DIR framing also verified at byte level via tcpdump + Wireshark dissection.

## Known limitations (documented in code)

- Spurious `FILE_ADDED` for `OPEN_IF`/`OVERWRITE_IF`/`SUPERSEDE` + pre-existing file — needs `create_action` plumbed through `chimera_vfs_open_at`.
- Subtree resolver funnels through `sync_delegation_threads[0]` — perf bottleneck under heavy subtree-watch + mutation load.
- `chimera_vfs_notify_destroy` blocks indefinitely if a delegation thread wedges (with progress logs); contract documented as "callers must quiesce frontends first."
- SMB 3.1.1 signing falls through to AES-CMAC-AES-128 instead of the preauth-integrity-derived key derivation (chimera doesn't yet implement 3.1.1 fully).
- Subtree `CHANGE_NOTIFY` on linux backend currently uses the coarse `!has_rpl` overflow path because the linux module doesn't advertise `CHIMERA_VFS_CAP_RPL`.
- `mount_entry->root_fh` and `has_rpl` are snapshots captured at first use; safe for Chimera's static mount config but stale if dynamic remount-in-place ever preserves the mount_id while changing the backing module.

## Files

```
 src/vfs/vfs_notify.{c,h}                         (new)
 src/vfs/vfs_rpl_cache.h                          (new)
 src/vfs/vfs_proc_getparent.c                     (new)
 src/vfs/vfs_proc_{mkdir,remove,rename}_at.c      (emit calls)
 src/vfs/vfs.{c,h}                                (capability bit, getparent plumbing)
 src/vfs/{memfs,linux}/*.c                        (preserve va_mode on remove)
 src/vfs/tests/vfs_notify_test.c                  (new, unit)

 src/server/smb/smb_notify.{c,h}                  (new)
 src/server/smb/smb_proc_change_notify.c          (new)
 src/server/smb/smb_proc_cancel.c                 (modified)
 src/server/smb/smb_proc_{create,close,write,
   set_info,ioctl}.c                              (emit calls + DOC fixes)
 src/server/smb/smb_signing.{c,h}                 (per-call MAC contexts +
                                                   chimera_smb_sign_message)
 src/server/smb/smb.c                             (compound dispatch + status
                                                   whitelist)
 src/server/smb/smb_internal.h                    (request/conn/thread additions)
 src/server/smb/smb_session.h                     (open_file->notify_state)
 src/server/smb/smb2.h                            (constants)
 src/server/smb/tests/smb_change_notify_test.c    (new, integration)

 ext/CMakeLists.txt                               (libsmb2 EXCLUDE_FROM_ALL)
```
