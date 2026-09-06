// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <pthread.h>
#include <uthash.h>
#include "vfs/vfs.h"
#include "sdk/vfs_fh.h"

/* Event types matching SMB2 CompletionFilter categories */
#define CHIMERA_VFS_NOTIFY_FILE_ADDED    0x0001
#define CHIMERA_VFS_NOTIFY_FILE_REMOVED  0x0002
#define CHIMERA_VFS_NOTIFY_FILE_MODIFIED 0x0004
#define CHIMERA_VFS_NOTIFY_DIR_ADDED     0x0008
#define CHIMERA_VFS_NOTIFY_DIR_REMOVED   0x0010
/* A rename of a FILE.  Split from the directory case because the two SMB2
 * name filters are: MS-FSCC scopes FILE_NOTIFY_CHANGE_FILE_NAME to file name
 * changes "including renaming" and _DIR_NAME to directory ones, so a client
 * watching directory names must not be woken because a file was renamed. */
#define CHIMERA_VFS_NOTIFY_RENAMED       0x0020
#define CHIMERA_VFS_NOTIFY_ATTRS_CHANGED 0x0040
#define CHIMERA_VFS_NOTIFY_SIZE_CHANGED  0x0080
/* Named-stream (alternate data stream) change classes, mapping to the
 * SMB2 FILE_NOTIFY_CHANGE_STREAM_{NAME,SIZE,WRITE} completion filters.
 * STREAM_NAME covers a stream being created/renamed/removed; STREAM_SIZE
 * a change to a stream's length; STREAM_WRITE a write into a stream's
 * data.  A write to a file's default ($DATA) fork touches both the
 * stream size and stream data, so the write path emits SIZE|WRITE. */
#define CHIMERA_VFS_NOTIFY_STREAM_NAME   0x0100
#define CHIMERA_VFS_NOTIFY_STREAM_SIZE   0x0200
#define CHIMERA_VFS_NOTIFY_STREAM_WRITE  0x0400
/* A rename of a DIRECTORY.  Raised instead of CHIMERA_VFS_NOTIFY_RENAMED when
 * the caller knows the renamed object is one (CHIMERA_VFS_RENAME_SRC_IS_DIR);
 * a caller that does not know raises RENAMED, which both name filters see --
 * the conservative answer for a protocol that cannot tell us. */
#define CHIMERA_VFS_NOTIFY_RENAMED_DIR   0x0800

#define CHIMERA_VFS_NOTIFY_RING_SIZE     32
#define CHIMERA_VFS_NOTIFY_NUM_BUCKETS   64
#define CHIMERA_VFS_NOTIFY_MAX_PENDING   256
#define CHIMERA_VFS_NOTIFY_MAX_DEPTH     64

struct chimera_vfs_notify_event {
    uint32_t action;           /* CHIMERA_VFS_NOTIFY_* */
    uint16_t name_len;
    uint16_t old_name_len;     /* for rename */
    char     name[CHIMERA_VFS_NAME_MAX];
    char     old_name[CHIMERA_VFS_NAME_MAX]; /* for rename */
};

struct chimera_vfs_notify_watch;
struct chimera_vfs_notify_gate;
struct chimera_vfs_request;

/* Callback: called when events are ready on a watch */
typedef void (*chimera_vfs_notify_callback_t)(
    struct chimera_vfs_notify_watch *watch,
    void                            *private_data);

/*
 * A synchronously-delivered namespace event: a mutation whose reply is being
 * withheld until every sync watcher acknowledges it has invalidated its
 * caches for the affected name(s).  Delivered on a malloc'd FIFO separate
 * from the ring so a burst can never drop one (a dropped gated event would
 * strand its gate until the deadline sweep).  The consumer drains the list
 * (chimera_vfs_notify_drain_sync), performs its invalidation for each event,
 * then acks it (chimera_vfs_notify_gate_ack) -- which frees the event and,
 * on the last outstanding ack, resumes the gated operation's completion.
 */
struct chimera_vfs_notify_sync_event {
    uint32_t                              action; /* CHIMERA_VFS_NOTIFY_* */
    uint16_t                              name_len;
    uint16_t                              old_name_len;
    char                                  name[CHIMERA_VFS_NAME_MAX];
    char                                  old_name[CHIMERA_VFS_NAME_MAX];
    struct chimera_vfs_notify_gate       *gate;
    struct chimera_vfs_notify_sync_event *next;
};

/* Watch on a directory */
struct chimera_vfs_notify_watch {
    uint8_t                               dir_fh[CHIMERA_VFS_FH_SIZE];
    uint16_t                              dir_fh_len;
    uint64_t                              dir_fh_hash;
    uint32_t                              filter_mask;
    int                                   watch_tree;

    /* Event ring buffer */
    struct chimera_vfs_notify_event       ring[CHIMERA_VFS_NOTIFY_RING_SIZE];
    int                                   ring_head; /* next write position */
    int                                   ring_count; /* number of pending events */
    int                                   overflowed;
    /* Set when the watched object itself was removed.  Drained via
     * chimera_vfs_notify_watch_take_deleted(); the SMB layer maps it to
     * STATUS_DELETE_PENDING on the pending CHANGE_NOTIFY. */
    int                                   deleted;

    chimera_vfs_notify_callback_t         callback;
    void                                 *private_data;

    /* Synchronous-coherence participation (chimera_vfs_notify_watch_set_sync):
     * namespace mutations under this directory complete only after this
     * watcher acks the sync events queued here, EXCEPT mutations whose
     * cred->origin matches `origin` (the watcher's own endpoint is natively
     * coherent with its own operations and gating on it would deadlock). */
    int                                   sync;
    const void                           *origin;
    struct chimera_vfs_notify_sync_event *sync_events;      /* FIFO, under lock */
    struct chimera_vfs_notify_sync_event *sync_events_tail;
    /* Number of gated namespace mutations from this watch's OWN origin
     * currently in flight under this directory (under the BUCKET lock;
     * marked at dispatch, cleared at completion, clamped at zero so a
     * watch recreated mid-operation cannot go negative and mask a later
     * mark).  While nonzero, PEERS' gates skip this watch: the origin's
     * kernel holds the directory lock for its own syscall, so a blocking
     * invalidation into it would deadlock the two mounts against each
     * other.  The skipped watcher converges through the ordinary async
     * emit instead -- sync coherence softens to microseconds-async exactly
     * when both mounts mutate one directory concurrently, where no
     * cross-mount ordering exists to preserve. */
    int                                   gated_inflight;

    /* Per-watch lock protects ring buffer state */
    pthread_mutex_t                       lock;

    /* Linkage within bucket (exact watches) */
    struct chimera_vfs_notify_watch      *next;

    /* Linkage within mount_entry subtree list */
    struct chimera_vfs_notify_watch      *subtree_next;
};

/* Tombstone of a just-removed object's FH.  Recorded by emit_delete so a
 * CHANGE_NOTIFY whose watch is armed *after* the delete already fired — a
 * cross-connection race where the deleting client does not wait for the
 * watcher's interim reply (smb2.notify.rmdir3/4) — still learns the object
 * is gone instead of parking on a watch that will never see another event.
 * Bounded ring per bucket; entries are honoured only within
 * CHIMERA_VFS_NOTIFY_TOMBSTONE_NS of the deletion so a later FH reuse cannot
 * spuriously report DELETE_PENDING. */
#define CHIMERA_VFS_NOTIFY_TOMBSTONE_COUNT 16
#define CHIMERA_VFS_NOTIFY_TOMBSTONE_NS    2000000000ULL /* 2 seconds */

struct chimera_vfs_notify_tombstone {
    uint8_t  fh[CHIMERA_VFS_FH_SIZE];
    uint16_t fh_len;
    uint64_t stamp;             /* chimera_vfs_now_ticks() at deletion; 0 = empty */
};

/* Bucket in the sharded exact-watch hash table */
struct chimera_vfs_notify_bucket {
    struct chimera_vfs_notify_watch    *watches;
    struct chimera_vfs_notify_tombstone tombstones[CHIMERA_VFS_NOTIFY_TOMBSTONE_COUNT];
    int                                 tombstone_next; /* ring write index */
    pthread_mutex_t                     lock;
};

/* Per-mount subtree watch registry */
struct chimera_vfs_notify_mount_entry {
    uint8_t                          mount_id[CHIMERA_VFS_MOUNT_ID_SIZE];
    uint8_t                          root_fh[CHIMERA_VFS_FH_SIZE];
    int                              root_fh_len;
    int                              has_rpl;
    struct chimera_vfs_notify_watch *subtree_watches;
    int                              num_subtree_watches;
    UT_hash_handle                   hh;
};

/* Pending event awaiting async RPL resolution */
struct chimera_vfs_notify_pending_event {
    uint32_t                                 action;
    uint8_t                                  dir_fh[CHIMERA_VFS_FH_SIZE];
    uint16_t                                 dir_fh_len;
    char                                     name[CHIMERA_VFS_NAME_MAX];
    uint16_t                                 name_len;
    char                                     old_name[CHIMERA_VFS_NAME_MAX];
    uint16_t                                 old_name_len;

    /* RPL walk state */
    uint8_t                                  walk_fh[CHIMERA_VFS_FH_SIZE];
    uint16_t                                 walk_fh_len;
    char                                     path_buf[CHIMERA_VFS_PATH_MAX]; /* relative path built bottom-up */
    int                                      path_offset; /* offset into path_buf where path starts */
    uint8_t                                  mount_id[CHIMERA_VFS_MOUNT_ID_SIZE];
    int                                      depth;

    struct chimera_vfs_notify               *notify;
    struct chimera_vfs_notify_pending_event *next;
};

/* Main notify subsystem */
struct chimera_vfs_notify {
    struct chimera_vfs_notify_bucket         buckets[CHIMERA_VFS_NOTIFY_NUM_BUCKETS];

    /* Subtree watch registry keyed by mount_id */
    struct chimera_vfs_notify_mount_entry   *mount_entries;
    pthread_mutex_t                          mount_entries_lock;

    /* RPL cache */
    struct chimera_vfs_rpl_cache            *rpl_cache;

    /* Pending RPL resolution queue */
    struct chimera_vfs_notify_pending_event *pending_events;
    struct chimera_vfs_notify_pending_event *free_events;
    int                                      num_pending;
    int                                      shutdown;     /* set during destroy to block new resolvers */
    pthread_mutex_t                          pending_lock;

    /* Completion gates: namespace mutations parked until every sync watcher
     * acks its invalidation (or the deadline sweep gives up on it).  The
     * sync-watch count is the dispatch-path fast gate: zero (no FUSE
     * coherence=sync mounts) means gate_install is a single atomic load. */
    pthread_mutex_t                          gates_lock;
    struct chimera_vfs_notify_gate          *gates;
    int                                      num_sync_watches; /* atomics */

    struct chimera_vfs                      *vfs;
};

/* Public API */

struct chimera_vfs_notify *
chimera_vfs_notify_init(
    struct chimera_vfs *vfs);

/*
 * Destroy the notify subsystem.
 *
 * LIFETIME CONTRACT: the caller MUST guarantee that no thread is
 * inside (or about to enter) chimera_vfs_notify_emit() when this is
 * called.  The shutdown flag set here blocks new async RPL resolver
 * chains from starting, and the function waits for any already-
 * dispatched resolvers to complete, but it does NOT serialize against
 * concurrent synchronous emit() calls touching bucket / mount-entry
 * state.  Production callers achieve this naturally by stopping all
 * frontends (NFS/SMB/S3 servers) before tearing down the VFS.  Tests
 * that need concurrent shutdown must arrange their own quiescence.
 */
void
chimera_vfs_notify_destroy(
    struct chimera_vfs_notify *notify);

struct chimera_vfs_notify_watch *
chimera_vfs_notify_watch_create(
    struct chimera_vfs_notify    *notify,
    const uint8_t                *dir_fh,
    uint16_t                      dir_fh_len,
    uint32_t                      filter_mask,
    int                           watch_tree,
    chimera_vfs_notify_callback_t callback,
    void                         *private_data);

void
chimera_vfs_notify_watch_destroy(
    struct chimera_vfs_notify       *notify,
    struct chimera_vfs_notify_watch *watch);

/* Update the per-watch filter mask and watch_tree mode in place.
 * Used when a new CHANGE_NOTIFY arrives on the same open with a
 * different CompletionFilter or WATCH_TREE flag — without this we
 * would either deliver too many events (filter ignored) or never
 * adjust subtree scope (watch_tree fixed at first request). */
void
chimera_vfs_notify_watch_update(
    struct chimera_vfs_notify       *notify,
    struct chimera_vfs_notify_watch *watch,
    uint32_t                         filter_mask,
    int                              watch_tree);

int
chimera_vfs_notify_drain(
    struct chimera_vfs_notify_watch *watch,
    struct chimera_vfs_notify_event *events,
    int                              max_events,
    int                             *overflowed);

/*
 * Mark a watch as overflowed so the next drain reports STATUS_NOTIFY_ENUM_DIR.
 * Used by the SMB layer when a set of drained events cannot be delivered in the
 * client's OutputBufferLength: the events are dropped and the client must
 * rescan, so the *next* CHANGE_NOTIFY on the handle must also report overflow
 * (MS-SMB2 / smb2.notify.valid-req).
 */
void
chimera_vfs_notify_mark_overflow(
    struct chimera_vfs_notify_watch *watch);

/*
 * Atomically read and clear a watch's "deleted" flag.  Returns non-zero if
 * the watched object had been removed since the last call.  The SMB layer
 * polls this alongside drain to complete a pending CHANGE_NOTIFY with
 * STATUS_DELETE_PENDING.
 */
int
chimera_vfs_notify_watch_take_deleted(
    struct chimera_vfs_notify_watch *watch);

void
chimera_vfs_notify_emit(
    struct chimera_vfs_notify *notify,
    const uint8_t             *dir_fh,
    uint16_t                   dir_fh_len,
    uint32_t                   action,
    const char                *name,
    uint16_t                   name_len,
    const char                *old_name,
    uint16_t                   old_name_len);

/* Like chimera_vfs_notify_emit, but the caller (the SMB layer) supplies a
 * ParentLeaseKey naming an SMB3 directory lease to spare from the
 * directory-lease break this fires: the mutating client's cached directory view
 * is coherent with the change it just made (MS-SMB2 dirlease self-exemption).
 * `has_skip` == false is exactly chimera_vfs_notify_emit (break every lease). */
void
chimera_vfs_notify_emit_lease(
    struct chimera_vfs_notify *notify,
    const uint8_t             *dir_fh,
    uint16_t                   dir_fh_len,
    uint32_t                   action,
    const char                *name,
    uint16_t                   name_len,
    const char                *old_name,
    uint16_t                   old_name_len,
    uint64_t                   skip_lo,
    uint64_t                   skip_hi,
    bool                       has_skip);

/* Deliver a CHANGE_NOTIFY event but do NOT break SMB3 directory leases.  For
 * the SMB create path when a create-capable disposition only opened an existing
 * file — change-notify fires (conservatively) but the directory's contents did
 * not change, so a directory read lease must not be recalled. */
void
chimera_vfs_notify_emit_nobreak(
    struct chimera_vfs_notify *notify,
    const uint8_t             *dir_fh,
    uint16_t                   dir_fh_len,
    uint32_t                   action,
    const char                *name,
    uint16_t                   name_len,
    const char                *old_name,
    uint16_t                   old_name_len);

/*
 * Signal that the object identified by `fh` was itself removed.  Marks every
 * exact watch on that FH deleted and fires its callback, so a pending
 * CHANGE_NOTIFY on a handle to the now-deleted directory completes with
 * STATUS_DELETE_PENDING instead of parking forever.
 */
void
chimera_vfs_notify_emit_delete(
    struct chimera_vfs_notify *notify,
    const uint8_t             *fh,
    uint16_t                   fh_len);

/*
 * The directory at `dir_fh` changed in ways the caller could not enumerate
 * (a compound recorded more mutations than its deferred-notify cap; see
 * chimera_vfs_notify_defer).  Break the directory's SMB3 leases and mark
 * every watch on it overflowed -- upstream that is STATUS_NOTIFY_ENUM_DIR,
 * the "too many changes, rescan" signal -- rather than delivering an
 * incomplete event list a client would trust.  Subtree watches on the same
 * mount are overflowed too (no per-event names exist to resolve relative
 * paths with), matching the coarse fallbacks emit_body already uses.
 */
void
chimera_vfs_notify_emit_overflow(
    struct chimera_vfs_notify *notify,
    const uint8_t             *dir_fh,
    uint16_t                   dir_fh_len);

/* ----------------------------------------------------------------
 * Synchronous coherence: sync watches and completion gates
 * ----------------------------------------------------------------
 *
 * A namespace mutation (create / remove / rename / link) under a directory
 * with SYNC watches does not complete back to its caller until every such
 * watcher (other than the mutation's own origin) has acknowledged the
 * event: the FUSE server uses this to guarantee that when a mutation
 * returns anywhere, every peer kernel's dentry for the affected name is
 * already invalidated.  The gate is installed transparently by
 * chimera_vfs_dispatch (vfs_notify_gate.c) after the backend completes and
 * before the protocol callback runs, so cross-protocol ordering holds for
 * every consumer without per-protocol changes.  A deadline sweep
 * (chimera_vfs_notify_gate_sweep, driven by the close thread's timer)
 * bounds the wait if a watcher wedges -- coherence degrades to the TTLs
 * for that operation rather than hanging it.
 */

/* How long a gated completion waits for watcher acks before proceeding
 * anyway.  Acks are normally microseconds (an in-kernel invalidation write);
 * the deadline exists for wedged consumers and kernel-side lock cycles
 * between mutually-gated mounts. */
#define CHIMERA_VFS_NOTIFY_GATE_TIMEOUT_MS 5000

/* Mark a watch as a sync watcher owned by `origin` (may be NULL, though a
 * NULL-origin sync watch gates even its own endpoint's mutations). */
void
chimera_vfs_notify_watch_set_sync(
    struct chimera_vfs_notify       *notify,
    struct chimera_vfs_notify_watch *watch,
    const void                      *origin);

/* Detach and return the watch's queued sync events (FIFO order).  Each
 * returned event MUST eventually be passed to chimera_vfs_notify_gate_ack
 * or its gate stalls until the deadline sweep. */
struct chimera_vfs_notify_sync_event *
chimera_vfs_notify_drain_sync(
    struct chimera_vfs_notify_watch *watch);

/* Acknowledge one sync event: the consumer has invalidated its caches for
 * the event's name(s).  Frees the event; the last outstanding ack resumes
 * the gated operation's completion on its owning thread. */
void
chimera_vfs_notify_gate_ack(
    struct chimera_vfs_notify            *notify,
    struct chimera_vfs_notify_sync_event *event);

/* Fire every gate past its deadline (called from the periodic close-thread
 * sweep). */
void
chimera_vfs_notify_gate_sweep(
    struct chimera_vfs_notify *notify);

/* vfs_notify_gate.c: install the completion gate on a namespace-mutating
 * request when sync watchers exist.  Called by chimera_vfs_dispatch; cheap
 * no-op when no sync watches are registered. */
void
chimera_vfs_notify_gate_install(
    struct chimera_vfs_request *request);
