// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <stdint.h>

#include "evpl/evpl.h"
#include "vfs/vfs_pnfs.h"
#include "vfs/vfs_ref.h"
#include "vfs/sdk/vfs_cred.h"

/*
 * Bookkeeping for a file that lost its last link while something still
 * references it.
 *
 * The data of such a file must outlive its name until the last handle for it
 * leaves the open cache, and the open cache is the one place that sees every
 * handle: an attached one, a detached one, one still being opened, and an idle
 * one waiting on pending_close.  So the per-file count lives in the shard that
 * owns the file, under the shard lock every handle change already takes, and
 * needs no lock of its own.
 *
 * A record exists only for a file that was unlinked while referenced.  Every
 * handle of that file carries CHIMERA_VFS_OPEN_HANDLE_UNLINKED, which is what
 * the cache's hot paths test; a file without a record costs them one pointer
 * test (shard->unlinked) when a handle is created and one flag test on release.
 *
 * A handle of such a file is never closed on the spot.  It is handed to the
 * VFS close thread, which takes it out of the record's count and, for the last
 * one, owns the decision to purge the file.
 */

struct vfs_open_cache;
struct vfs_open_cache_shard;
struct chimera_vfs;
struct chimera_vfs_close_thread;
struct chimera_vfs_unlinked_work;
struct prometheus_counter_instance;
struct prometheus_gauge_instance;

enum chimera_vfs_unlinked_state {
    /* Handles may come and go; an open by file handle still succeeds. */
    CHIMERA_VFS_UNLINKED_TRACKED,
    /* The last handle has left the cache and the purge is running.  The
     * record stays until the backend close completes so that nothing can
     * open the file again in the meantime. */
    CHIMERA_VFS_UNLINKED_PURGING,
};

enum chimera_vfs_unlinked_event {
    CHIMERA_VFS_UNLINKED_EV_TRACKED,
    CHIMERA_VFS_UNLINKED_EV_RECALLED,
    CHIMERA_VFS_UNLINKED_EV_PURGED,
    CHIMERA_VFS_UNLINKED_EV_PURGE_FAILED,
    CHIMERA_VFS_UNLINKED_EV_RELINKED,
    CHIMERA_VFS_UNLINKED_EV_LEAKED_AT_SHUTDOWN,
    CHIMERA_VFS_UNLINKED_EV_NUM
};

/*
 * What the purge needs to know about a file that the open cache cannot tell it:
 * where the file's data lives and who unlinked it.  Captured by the unlink path
 * while it still has the file's attributes; a record made without it (the unit
 * tests that drive the cache directly) purges by closing the handles alone.
 */
struct chimera_vfs_unlinked_facts {
    /* The file has a backing file on a data server.  False for a placement
     * the layout attribute does not describe by name. */
    uint8_t                 has_backing;
    uint8_t                 deviceid[CHIMERA_VFS_DEVICEID_SIZE];
    /* The name LAYOUTGET and the write redirect created it under. */
    char                    backing_name[CHIMERA_VFS_PNFS_BACKING_NAME_MAX];
    /* The unlinking caller, which is the identity the delete runs as. */
    struct chimera_vfs_cred cred;
};

struct chimera_vfs_unlinked {
    struct chimera_vfs_unlinked      *prev;
    struct chimera_vfs_unlinked      *next;
    struct vfs_open_cache_shard      *shard;
    uint64_t                          fh_hash;
    uint16_t                          fh_len;
    uint8_t                           state;
    /* Every handle struct of the file that is still in the cache or detached
     * from it, or that has left it and not yet been taken out of the count by
     * the close thread.  PENDING and idle handles are included. */
    uint32_t                          handles;
    /* Sum over those handles of (opencnt - recallcnt): real opens, NFS open
     * state, NLM, the length of an NFSv3 operation. */
    uint32_t                          strong;
    /* Sum of recallcnt: references whose holder agreed to give them back. */
    uint32_t                          recallable;
    /* How many of those references have not been recalled yet.  Each
     * recallable reference is recalled at most once, so this is what tells the
     * release path whether reaching strong == 0 has anything left to do. */
    uint32_t                          unfired;
    /* A recall item for this record is on the close thread's inbox; keeps the
     * release path from queuing one per release. */
    uint8_t                           fire_queued;
    uint8_t                           have_facts;
    struct chimera_vfs_unlinked_facts facts;
    uint8_t                           fh[CHIMERA_VFS_FH_SIZE];
};

/* Per open cache: the close thread's inbox and the event counters.  Only the
 * file cache ever holds records, but the field is common to both caches so the
 * inline cache code does not have to know which one it is. */
/* One holder to call about one file, copied out of the cache so the call can be
 * made after every lock is dropped. */
struct chimera_vfs_recall_call {
    chimera_vfs_recall_fn fn;
    void                 *arg;
    uint16_t              fh_len;
    uint8_t               fh[CHIMERA_VFS_FH_SIZE];
};

struct chimera_vfs_unlinked_ctl {
    evpl_mutex_t                        lock;
    struct chimera_vfs_unlinked_work   *work_head;
    struct chimera_vfs_unlinked_work   *work_tail;
    struct evpl_doorbell               *doorbell;
    uint64_t                           *fence_issued;
    uint64_t                           *fence_completed;
    uint64_t                            events[CHIMERA_VFS_UNLINKED_EV_NUM];
    uint64_t                            alive;
    /* Recallable references held on the file cache right now, read without a
     * lock so umount and shutdown can skip their scans when there are none. */
    uint64_t                            live_recallables;
    struct prometheus_metrics          *metrics;
    struct prometheus_counter          *counter;
    struct prometheus_counter_series   *series[CHIMERA_VFS_UNLINKED_EV_NUM];
    struct prometheus_counter_instance *instance[CHIMERA_VFS_UNLINKED_EV_NUM];
    struct prometheus_gauge            *gauge;
    struct prometheus_gauge_series     *gauge_series;
    struct prometheus_gauge_instance   *gauge_instance;
};

void chimera_vfs_unlinked_ctl_init(
    struct chimera_vfs_unlinked_ctl *ctl,
    struct prometheus_metrics       *metrics);

void chimera_vfs_unlinked_ctl_destroy(
    struct chimera_vfs_unlinked_ctl *ctl);

/* Point the file cache's inbox at the close thread: its doorbell, and the
 * issued/completed fence umount waits on. */
void chimera_vfs_unlinked_attach_close_thread(
    struct vfs_open_cache           *cache,
    struct chimera_vfs_close_thread *close_thread);

void chimera_vfs_unlinked_event(
    struct chimera_vfs_unlinked_ctl *ctl,
    enum chimera_vfs_unlinked_event  event);

/* Drain the inbox on the close thread.  Returns the number of items taken, so
 * the shutdown drain can tell it has not finished. */
uint64_t chimera_vfs_unlinked_drain(
    struct chimera_vfs_close_thread *close_thread);

/* Free what a destroyed cache still holds. */
void chimera_vfs_unlinked_cache_destroy(
    struct vfs_open_cache *cache);

/*
 * Create the record for the file `fh` names, from the handles the open cache
 * holds for it right now (attached, detached, pending, idle), and set the
 * UNLINKED flag on each.  Creates nothing when the file has no handle in the
 * cache, or tracking is off.  Idempotent: a file that already has a record
 * keeps it.
 *
 * The unlink path calls this once the last link is gone.  It is exported so
 * that the unit tests can drive the open cache without an unlink.
 *
 * Returns CHIMERA_VFS_OK when a record exists on return, CHIMERA_VFS_ENOENT
 * when there was nothing to track, CHIMERA_VFS_EINVAL when tracking is off.
 */
SYMBOL_EXPORT enum chimera_vfs_error
chimera_vfs_unlinked_track(
    struct chimera_vfs *vfs,
    const uint8_t      *fh,
    uint16_t            fh_len);

/*
 * As chimera_vfs_unlinked_track, for the unlink path: also records `facts`, so
 * the purge rechecks the link count and deletes the backing file.  A file that
 * already has a record keeps its first facts.
 */
enum chimera_vfs_error
chimera_vfs_unlinked_track_facts(
    struct chimera_vfs                      *vfs,
    const uint8_t                           *fh,
    uint16_t                                 fh_len,
    const struct chimera_vfs_unlinked_facts *facts);

/*
 * As chimera_vfs_unlinked_track_facts, but makes a record only when the file has
 * a recallable reference on one of its handles.  The unlink path uses it for a
 * file with no layout attribute: such a file has no data of its own to purge,
 * and is worth tracking only because a holder has to be asked to give its
 * reference back.  Returns CHIMERA_VFS_ENOENT when it made nothing.
 */
enum chimera_vfs_error
chimera_vfs_unlinked_track_if_recallable(
    struct chimera_vfs                      *vfs,
    const uint8_t                           *fh,
    uint16_t                                 fh_len,
    const struct chimera_vfs_unlinked_facts *facts);

/* Recall every recallable reference on a file of the mount `mount_id`, reason
 * UMOUNT, from the close thread.  Safe to call again: a reference is recalled
 * at most once. */
void chimera_vfs_unlinked_recall_mount(
    struct chimera_vfs *vfs,
    const uint8_t      *mount_id);

/* Shutdown: forget every recallable reference still held, without recalling it,
 * and drop the handle references behind them so the handles can close.  The
 * holders' structs are left detached (r->handle is NULL), so a late release is
 * harmless.  Close thread only. */
void chimera_vfs_unlinked_abandon_recallable(
    struct chimera_vfs_close_thread *close_thread);

struct chimera_vfs_unlinked_info {
    int      exists;
    int      state;
    uint32_t handles;
    uint32_t strong;
    uint32_t recallable;
};

/* Test-only inspection and hooks. */
SYMBOL_EXPORT void
chimera_vfs_unlinked_test_info(
    struct chimera_vfs               *vfs,
    const uint8_t                    *fh,
    uint16_t                          fh_len,
    struct chimera_vfs_unlinked_info *info);

SYMBOL_EXPORT uint64_t
chimera_vfs_unlinked_test_event(
    struct chimera_vfs             *vfs,
    enum chimera_vfs_unlinked_event event);

/* Events counted over the life of the process, across every VFS instance, so a
 * test can read them after the instance that counted them has been destroyed. */
SYMBOL_EXPORT uint64_t
chimera_vfs_unlinked_test_total_event(
    enum chimera_vfs_unlinked_event event);

/* Called on the close thread just before the purge of a file whose last handle
 * has left the cache.  carrierless is set when that handle never had a backend
 * open (a pending open that failed). */
typedef void (*chimera_vfs_unlinked_test_purge_fn)(
    const uint8_t *fh,
    uint16_t       fh_len,
    int            carrierless,
    void          *arg);

SYMBOL_EXPORT void
chimera_vfs_unlinked_test_set_purge_hook(
    chimera_vfs_unlinked_test_purge_fn fn,
    void                              *arg);

/* The steps of the purge of an unlinked file, in the order the close thread
 * takes them.  A test hook sees each one just before it starts. */
enum chimera_vfs_unlinked_step {
    CHIMERA_VFS_UNLINKED_STEP_RECHECK,        /* link count read through the carrier */
    CHIMERA_VFS_UNLINKED_STEP_BACKING_DELETE, /* data-server backing file removed */
    CHIMERA_VFS_UNLINKED_STEP_BACKEND_CLOSE,  /* backend close of the carrier */
};

typedef void (*chimera_vfs_unlinked_test_step_fn)(
    enum chimera_vfs_unlinked_step step,
    const uint8_t                 *fh,
    uint16_t                       fh_len,
    void                          *arg);

SYMBOL_EXPORT void
chimera_vfs_unlinked_test_set_step_hook(
    chimera_vfs_unlinked_test_step_fn fn,
    void                             *arg);
