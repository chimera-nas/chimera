// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>
#include <pthread.h>
#include <uthash.h>
#include "vfs/vfs.h"
#include "vfs_fh.h"

/* Event types matching SMB2 CompletionFilter categories */
#define CHIMERA_VFS_NOTIFY_FILE_ADDED    0x0001
#define CHIMERA_VFS_NOTIFY_FILE_REMOVED  0x0002
#define CHIMERA_VFS_NOTIFY_FILE_MODIFIED 0x0004
#define CHIMERA_VFS_NOTIFY_DIR_ADDED     0x0008
#define CHIMERA_VFS_NOTIFY_DIR_REMOVED   0x0010
#define CHIMERA_VFS_NOTIFY_RENAMED       0x0020
#define CHIMERA_VFS_NOTIFY_ATTRS_CHANGED 0x0040
#define CHIMERA_VFS_NOTIFY_SIZE_CHANGED  0x0080

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

/* Callback: called when events are ready on a watch */
typedef void (*chimera_vfs_notify_callback_t)(
    struct chimera_vfs_notify_watch *watch,
    void                            *private_data);

/* Watch on a directory */
struct chimera_vfs_notify_watch {
    uint8_t                          dir_fh[CHIMERA_VFS_FH_SIZE];
    uint16_t                         dir_fh_len;
    uint64_t                         dir_fh_hash;
    uint32_t                         filter_mask;
    int                              watch_tree;

    /* Event ring buffer */
    struct chimera_vfs_notify_event  ring[CHIMERA_VFS_NOTIFY_RING_SIZE];
    int                              ring_head;  /* next write position */
    int                              ring_count; /* number of pending events */
    int                              overflowed;

    chimera_vfs_notify_callback_t    callback;
    void                            *private_data;

    /* Per-watch lock protects ring buffer state */
    pthread_mutex_t                  lock;

    /* Linkage within bucket (exact watches) */
    struct chimera_vfs_notify_watch *next;

    /* Linkage within mount_entry subtree list */
    struct chimera_vfs_notify_watch *subtree_next;
};

/* Bucket in the sharded exact-watch hash table */
struct chimera_vfs_notify_bucket {
    struct chimera_vfs_notify_watch *watches;
    pthread_mutex_t                  lock;
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
