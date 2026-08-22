// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <utlist.h>

#include "vfs_notify.h"
#include "vfs_internal.h"
#include "vfs_rpl_cache.h"
#include "vfs_mount_table.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_claim.h"
#include "common/macros.h"

/* ----------------------------------------------------------------
 * Internal helpers
 * ---------------------------------------------------------------- */

static inline int
chimera_vfs_notify_bucket_index(uint64_t fh_hash)
{
    return fh_hash & (CHIMERA_VFS_NOTIFY_NUM_BUCKETS - 1);
} /* chimera_vfs_notify_bucket_index */

/*
 * Enqueue an event into a watch's ring buffer.
 * Takes watch->lock internally for thread safety.
 */
static inline void
chimera_vfs_notify_watch_enqueue(
    struct chimera_vfs_notify_watch *watch,
    uint32_t                         action,
    const char                      *name,
    uint16_t                         name_len,
    const char                      *old_name,
    uint16_t                         old_name_len)
{
    struct chimera_vfs_notify_event *ev;
    int                              idx;

    pthread_mutex_lock(&watch->lock);

    if (watch->ring_count >= CHIMERA_VFS_NOTIFY_RING_SIZE) {
        watch->overflowed = 1;
        pthread_mutex_unlock(&watch->lock);
        return;
    }

    idx = (watch->ring_head + watch->ring_count) & (CHIMERA_VFS_NOTIFY_RING_SIZE - 1);
    ev  = &watch->ring[idx];

    /* Clamp to the destination buffer size.  Names are bounded by
     * CHIMERA_VFS_NAME_MAX everywhere they originate, but enforce it
     * here so a malformed caller cannot overflow the ring entry. */
    if (name_len > CHIMERA_VFS_NAME_MAX) {
        name_len = CHIMERA_VFS_NAME_MAX;
    }
    if (old_name_len > CHIMERA_VFS_NAME_MAX) {
        old_name_len = CHIMERA_VFS_NAME_MAX;
    }

    ev->action       = action;
    ev->name_len     = name_len;
    ev->old_name_len = old_name_len;

    if (name_len) {
        memcpy(ev->name, name, name_len);
    }

    if (old_name_len && old_name) {
        memcpy(ev->old_name, old_name, old_name_len);
    }

    watch->ring_count++;

    pthread_mutex_unlock(&watch->lock);
} /* chimera_vfs_notify_watch_enqueue */

/*
 * Mark a watch as overflowed so the next drain reports it to the
 * consumer.  Used by the coarse subtree-fallback paths (no RPL, or
 * RPL pending-queue exhausted) where we know events happened
 * somewhere under the watched subtree but cannot compute the actual
 * relative path.  Overflow translates upstream to
 * STATUS_NOTIFY_ENUM_DIR which tells Windows to rescan — the
 * semantically correct signal.  The previous behaviour enqueued a
 * synthetic FILE_MODIFIED on "." which a client could legitimately
 * read as "the watched directory itself changed".
 */
static inline void
chimera_vfs_notify_watch_overflow(struct chimera_vfs_notify_watch *watch)
{
    pthread_mutex_lock(&watch->lock);
    watch->overflowed = 1;
    pthread_mutex_unlock(&watch->lock);
} /* chimera_vfs_notify_watch_overflow */

SYMBOL_EXPORT void
chimera_vfs_notify_mark_overflow(struct chimera_vfs_notify_watch *watch)
{
    chimera_vfs_notify_watch_overflow(watch);
} /* chimera_vfs_notify_mark_overflow */

/*
 * Look up or create a mount_entry for a given mount_id.
 * Caller must hold notify->mount_entries_lock.
 */
static struct chimera_vfs_notify_mount_entry *
chimera_vfs_notify_get_mount_entry(
    struct chimera_vfs_notify *notify,
    const uint8_t             *mount_id)
{
    struct chimera_vfs_notify_mount_entry *me = NULL;
    struct chimera_vfs_mount              *mount;

    HASH_FIND(hh, notify->mount_entries, mount_id, CHIMERA_VFS_MOUNT_ID_SIZE, me);

    if (!me) {
        me = calloc(1, sizeof(*me));
        memcpy(me->mount_id, mount_id, CHIMERA_VFS_MOUNT_ID_SIZE);

        /* Populate mount root FH and check if backend supports RPL.
         * Unit tests construct the notify subsystem with vfs==NULL —
         * exact-watch paths don't need it, and subtree watches in that
         * mode simply behave as if no mount entry were registered.
         *
         * NOTE: root_fh and has_rpl are SNAPSHOTS captured at first
         * use.  Chimera mounts are config-static today so this is
         * fine, but if dynamic remount-in-place is ever supported the
         * cached values can go stale.  In that case the mount_id
         * itself usually changes too (it's part of the FH), which
         * would force a new mount_entry to be created — but a remount
         * that preserves mount_id while changing root_fh or RPL
         * capability would not.  Document and revisit if it matters. */
        if (notify->vfs) {
            urcu_qsbr_read_lock();
            mount = chimera_vfs_mount_table_lookup(notify->vfs->mount_table, mount_id);
            if (mount) {
                memcpy(me->root_fh, mount->root_fh, mount->root_fh_len);
                me->root_fh_len = mount->root_fh_len;
                if (mount->module) {
                    me->has_rpl = (mount->module->capabilities & CHIMERA_VFS_CAP_RPL) != 0;
                }
            }
            urcu_qsbr_read_unlock();
        }

        HASH_ADD(hh, notify->mount_entries, mount_id, CHIMERA_VFS_MOUNT_ID_SIZE, me);
    }

    return me;
} /* chimera_vfs_notify_get_mount_entry */

/*
 * Allocate a pending event from the free list, or malloc one.
 * Caller must hold notify->pending_lock.
 */
static struct chimera_vfs_notify_pending_event *
chimera_vfs_notify_alloc_pending(struct chimera_vfs_notify *notify)
{
    struct chimera_vfs_notify_pending_event *ev;

    if (notify->free_events) {
        ev                  = notify->free_events;
        notify->free_events = ev->next;
    } else {
        ev = calloc(1, sizeof(*ev));
        if (!ev) {
            return NULL;
        }
    }

    ev->next = NULL;
    return ev;
} /* chimera_vfs_notify_alloc_pending */

/*
 * Return a pending event to the free list.
 */
static void
chimera_vfs_notify_free_pending(
    struct chimera_vfs_notify               *notify,
    struct chimera_vfs_notify_pending_event *ev)
{
    pthread_mutex_lock(&notify->pending_lock);
    ev->next            = notify->free_events;
    notify->free_events = ev;
    notify->num_pending--;
    pthread_mutex_unlock(&notify->pending_lock);
} /* chimera_vfs_notify_free_pending */

/* Forward declaration for the resolver chain */
static void chimera_vfs_notify_resolve(
    struct chimera_vfs_notify_pending_event *pev);

/*
 * Prepend a path component to the pending event's path buffer.
 * path_buf is built right-to-left: path_offset is the start index.
 */
static inline int
chimera_vfs_notify_path_prepend(
    struct chimera_vfs_notify_pending_event *pev,
    const char                              *component,
    int                                      component_len)
{
    int needed = component_len + 1; /* component + '/' separator */

    if (pev->path_offset < needed) {
        return -1; /* path too long */
    }

    pev->path_offset--;
    pev->path_buf[pev->path_offset] = '/';
    pev->path_offset               -= component_len;
    memcpy(&pev->path_buf[pev->path_offset], component, component_len);

    return 0;
} /* chimera_vfs_notify_path_prepend */

/*
 * Deliver a resolved subtree event to a watch.
 *
 * If the relative path exceeds CHIMERA_VFS_NAME_MAX (the size of a
 * ring entry's name buffer), we cannot deliver the full path — and
 * watch_enqueue would silently truncate it mid-component, producing
 * a malformed name that the SMB layer would then UTF-16-encode.
 * Signal overflow to the watch instead so the client rescans via
 * STATUS_NOTIFY_ENUM_DIR.
 */
static void
chimera_vfs_notify_deliver_subtree_event(
    struct chimera_vfs_notify_watch         *watch,
    struct chimera_vfs_notify_pending_event *pev)
{
    const char *relpath;
    int         relpath_len;
    char        old_path_buf[CHIMERA_VFS_NAME_MAX];
    const char *old_path     = pev->old_name;
    int         old_path_len = pev->old_name_len;

    relpath     = &pev->path_buf[pev->path_offset];
    relpath_len = CHIMERA_VFS_PATH_MAX - pev->path_offset;

    /* Same-directory renames carry an old_name that is just the leaf;
     * for subtree watches we must reattach the parent path so the
     * RENAMED_OLD_NAME record matches RENAMED_NEW_NAME's prefix. */
    if (old_path_len > 0 && (pev->action & CHIMERA_VFS_NOTIFY_RENAMED)) {
        int slash_idx = -1;
        for (int i = relpath_len - 1; i >= 0; i--) {
            if (relpath[i] == '/') {
                slash_idx = i;
                break;
            }
        }
        if (slash_idx >= 0) {
            int prefix_len = slash_idx + 1;
            if (prefix_len + old_path_len <= (int) sizeof(old_path_buf)) {
                memcpy(old_path_buf, relpath, prefix_len);
                memcpy(old_path_buf + prefix_len,
                       pev->old_name, pev->old_name_len);
                old_path     = old_path_buf;
                old_path_len = prefix_len + pev->old_name_len;
            }
        }
    }

    if (relpath_len > CHIMERA_VFS_NAME_MAX ||
        old_path_len > CHIMERA_VFS_NAME_MAX) {
        chimera_vfs_notify_watch_overflow(watch);
    } else {
        chimera_vfs_notify_watch_enqueue(watch,
                                         pev->action,
                                         relpath,
                                         (uint16_t) relpath_len,
                                         old_path,
                                         (uint16_t) old_path_len);
    }

    if (watch->callback) {
        watch->callback(watch, watch->private_data);
    }
} /* chimera_vfs_notify_deliver_subtree_event */

/*
 * Overflow every subtree watch on the given mount.  Used when the
 * resolver cannot continue (path_prepend exhausted, etc.) and we
 * cannot tell which specific watches would have matched.  Caller
 * must hold mount_entries_lock.
 */
static void
chimera_vfs_notify_overflow_all_subtree(struct chimera_vfs_notify_mount_entry *me)
{
    struct chimera_vfs_notify_watch *watch;

    if (!me) {
        return;
    }
    for (watch = me->subtree_watches; watch; watch = watch->subtree_next) {
        chimera_vfs_notify_watch_overflow(watch);
        if (watch->callback) {
            watch->callback(watch, watch->private_data);
        }
    }
} /* chimera_vfs_notify_overflow_all_subtree */

/* ----------------------------------------------------------------
 * Async RPL resolver
 * ---------------------------------------------------------------- */

/*
 * Callback from chimera_vfs_getparent() completion.
 */
static void
chimera_vfs_notify_resolve_getparent_cb(
    enum chimera_vfs_error error_code,
    const uint8_t         *parent_fh,
    uint16_t               parent_fh_len,
    const char            *name,
    uint16_t               name_len,
    void                  *private_data)
{
    struct chimera_vfs_notify_pending_event *pev    = private_data;
    struct chimera_vfs_notify               *notify = pev->notify;

    if (error_code != CHIMERA_VFS_OK) {
        /* getparent failed mid-walk (ESTALE, EACCES, the parent was
         * removed while we resolved, etc.).  We have a partial path
         * but no way to complete it — overflow all subtree watches on
         * this mount so the client rescans.  Better than silently
         * dropping the event. */
        struct chimera_vfs_notify_mount_entry *me;
        pthread_mutex_lock(&notify->mount_entries_lock);
        HASH_FIND(hh, notify->mount_entries, pev->mount_id,
                  CHIMERA_VFS_MOUNT_ID_SIZE, me);
        chimera_vfs_notify_overflow_all_subtree(me);
        pthread_mutex_unlock(&notify->mount_entries_lock);
        chimera_vfs_notify_free_pending(notify, pev);
        return;
    }

    /* Cache the result for future events.  This callback runs on the same
     * worker thread chimera_vfs_notify_resolve dispatched getparent on, so its
     * magazine is the correct thread-local pool to recycle from. */
    struct chimera_vfs_thread *rpl_thread = notify->vfs->num_sync_delegation_threads > 0 ?
        notify->vfs->sync_delegation_threads[0].vfs_thread :
        notify->vfs->close_thread.vfs_thread;

    chimera_vfs_rpl_cache_insert(rpl_thread, notify->rpl_cache,
                                 chimera_vfs_hash(pev->walk_fh, pev->walk_fh_len),
                                 pev->walk_fh,
                                 pev->walk_fh_len,
                                 parent_fh,
                                 parent_fh_len,
                                 chimera_vfs_hash(parent_fh, parent_fh_len),
                                 chimera_vfs_hash(name, name_len),
                                 name,
                                 name_len);

    /* Prepend this component to the path */
    if (chimera_vfs_notify_path_prepend(pev, name, name_len) < 0) {
        /* Accumulated path exceeded CHIMERA_VFS_PATH_MAX.  We can no
         * longer build a correct relative path, so overflow every
         * subtree watch on this mount.  Heavy-handed but always
         * correct: the client will rescan via NOTIFY_ENUM_DIR. */
        struct chimera_vfs_notify_mount_entry *me;
        pthread_mutex_lock(&notify->mount_entries_lock);
        HASH_FIND(hh, notify->mount_entries, pev->mount_id,
                  CHIMERA_VFS_MOUNT_ID_SIZE, me);
        chimera_vfs_notify_overflow_all_subtree(me);
        pthread_mutex_unlock(&notify->mount_entries_lock);
        chimera_vfs_notify_free_pending(notify, pev);
        return;
    }

    /* Update walk position */
    memcpy(pev->walk_fh, parent_fh, parent_fh_len);
    pev->walk_fh_len = parent_fh_len;

    /* Continue resolving */
    chimera_vfs_notify_resolve(pev);
} /* chimera_vfs_notify_resolve_getparent_cb */

/*
 * Core resolve loop.  Walks upward from walk_fh checking for subtree
 * watch matches at each level, using the RPL cache when possible and
 * falling back to async getparent calls on cache miss.
 */
static void
chimera_vfs_notify_resolve(struct chimera_vfs_notify_pending_event *pev)
{
    struct chimera_vfs_notify             *notify = pev->notify;
    struct chimera_vfs_notify_mount_entry *me;
    struct chimera_vfs_notify_watch       *watch;
    struct chimera_vfs_thread             *worker_thread;
    uint8_t                                r_parent_fh[CHIMERA_VFS_FH_SIZE];
    uint16_t                               r_parent_fh_len;
    char                                   r_name[CHIMERA_VFS_NAME_MAX];
    uint16_t                               r_name_len;
    int                                    rc;

    for (;;) {
        pev->depth++;

        if (pev->depth > CHIMERA_VFS_NOTIFY_MAX_DEPTH) {
            /* Walked too far without hitting the mount root — either a
             * cycle (shouldn't happen) or a pathologically deep tree.
             * Overflow all subtree watches on this mount so any client
             * that cares rescans, rather than silently dropping. */
            pthread_mutex_lock(&notify->mount_entries_lock);
            HASH_FIND(hh, notify->mount_entries, pev->mount_id,
                      CHIMERA_VFS_MOUNT_ID_SIZE, me);
            chimera_vfs_notify_overflow_all_subtree(me);
            pthread_mutex_unlock(&notify->mount_entries_lock);
            chimera_vfs_notify_free_pending(notify, pev);
            return;
        }

        /* Check if walk_fh matches any subtree watch AND whether we've
         * reached the mount root.  Both lookups happen under
         * mount_entries_lock and dereferences of `me` (subtree_watches,
         * root_fh, root_fh_len) stay under the lock — this is required
         * because watch_destroy may HASH_DEL+free a mount entry when
         * its last subtree watch is removed.
         *
         * Skip the subtree iteration at depth 1: walk_fh still equals
         * the event's parent dir fh, which the exact-watch path in
         * notify_emit has already delivered to.  Without this guard a
         * subtree watch placed directly on the event's parent dir
         * receives the same event twice.  The at-root check still
         * needs `me` though, so we always look it up. */
        {
            int at_root  = 0;
            int no_entry = 0;

            pthread_mutex_lock(&notify->mount_entries_lock);
            HASH_FIND(hh, notify->mount_entries, pev->mount_id,
                      CHIMERA_VFS_MOUNT_ID_SIZE, me);

            if (!me) {
                no_entry = 1;
            } else {
                if (pev->depth > 1) {
                    for (watch = me->subtree_watches; watch;
                         watch = watch->subtree_next) {
                        if (watch->dir_fh_len == pev->walk_fh_len &&
                            memcmp(watch->dir_fh, pev->walk_fh,
                                   pev->walk_fh_len) == 0) {
                            chimera_vfs_notify_deliver_subtree_event(watch, pev);
                        }
                    }
                }

                if (me->root_fh_len > 0 &&
                    pev->walk_fh_len == me->root_fh_len &&
                    memcmp(pev->walk_fh, me->root_fh, me->root_fh_len) == 0) {
                    at_root = 1;
                }
            }

            pthread_mutex_unlock(&notify->mount_entries_lock);

            if (no_entry || at_root) {
                chimera_vfs_notify_free_pending(notify, pev);
                return;
            }
        }

        /* Try RPL cache */
        rc = chimera_vfs_rpl_cache_lookup(notify->rpl_cache,
                                          chimera_vfs_hash(pev->walk_fh,
                                                           pev->walk_fh_len),
                                          pev->walk_fh,
                                          pev->walk_fh_len,
                                          r_parent_fh,
                                          &r_parent_fh_len,
                                          r_name,
                                          &r_name_len);

        if (rc == 0) {
            /* Cache hit — prepend and continue synchronously */
            if (chimera_vfs_notify_path_prepend(pev, r_name, r_name_len) < 0) {
                /* Path overflow — overflow all subtree watches on this
                 * mount so the client rescans.  Same rationale as the
                 * getparent_cb path. */
                pthread_mutex_lock(&notify->mount_entries_lock);
                HASH_FIND(hh, notify->mount_entries, pev->mount_id,
                          CHIMERA_VFS_MOUNT_ID_SIZE, me);
                chimera_vfs_notify_overflow_all_subtree(me);
                pthread_mutex_unlock(&notify->mount_entries_lock);
                chimera_vfs_notify_free_pending(notify, pev);
                return;
            }

            memcpy(pev->walk_fh, r_parent_fh, r_parent_fh_len);
            pev->walk_fh_len = r_parent_fh_len;
            continue;
        }

        /* Cache miss — issue async getparent.
         * The callback will continue the resolve loop.  Run it on the first
         * sync delegation thread when available, otherwise fall back to the
         * always-present close thread (the sync pool may be disabled). */
        worker_thread = notify->vfs->num_sync_delegation_threads > 0 ?
            notify->vfs->sync_delegation_threads[0].vfs_thread :
            notify->vfs->close_thread.vfs_thread;

        chimera_vfs_getparent(worker_thread,
                              NULL, /* cred — internal operation */
                              pev->walk_fh,
                              pev->walk_fh_len,
                              chimera_vfs_notify_resolve_getparent_cb,
                              pev);
        return;
    }
} /* chimera_vfs_notify_resolve */

/* ----------------------------------------------------------------
 * Public API
 * ---------------------------------------------------------------- */

SYMBOL_EXPORT struct chimera_vfs_notify *
chimera_vfs_notify_init(struct chimera_vfs *vfs)
{
    struct chimera_vfs_notify *notify;
    int                        i;

    notify      = calloc(1, sizeof(*notify));
    notify->vfs = vfs;

    for (i = 0; i < CHIMERA_VFS_NOTIFY_NUM_BUCKETS; i++) {
        notify->buckets[i].watches = NULL;
        pthread_mutex_init(&notify->buckets[i].lock, NULL);
    }

    pthread_mutex_init(&notify->mount_entries_lock, NULL);
    pthread_mutex_init(&notify->pending_lock, NULL);
    pthread_mutex_init(&notify->gates_lock, NULL);

    /* RPL cache: 64 shards, 16 slots/shard, 4 entries/slot, 30s TTL */
    notify->rpl_cache = chimera_vfs_rpl_cache_create(6, 4, 2, 30);

    return notify;
} /* chimera_vfs_notify_init */

SYMBOL_EXPORT void
chimera_vfs_notify_destroy(struct chimera_vfs_notify *notify)
{
    struct chimera_vfs_notify_mount_entry   *me, *me_tmp;
    struct chimera_vfs_notify_watch         *watch, *watch_tmp;
    struct chimera_vfs_notify_pending_event *pev, *pev_tmp;
    int                                      i;
    int                                      waits;

    if (!notify) {
        return;
    }

    /* Mark the subsystem as shutting down so no new RPL resolver chains
     * can start.  Existing in-flight chains observe a live state until
     * their pev is freed and num_pending drops to zero. */
    pthread_mutex_lock(&notify->pending_lock);
    notify->shutdown = 1;
    pthread_mutex_unlock(&notify->pending_lock);

    /* Block until all in-flight subtree resolvers (chimera_vfs_getparent
     * callbacks still in motion) have completed and freed their pev.
     * A callback that fires after we proceed touches freed
     * mount_entries / rpl_cache state, so we cannot proceed on timeout.
     * Bounded in practice by VFS I/O timeouts; log periodically if it
     * stalls so a stuck delegation thread is visible. */
    waits = 0;
    for (;;) {
        int n;
        pthread_mutex_lock(&notify->pending_lock);
        n = notify->num_pending;
        pthread_mutex_unlock(&notify->pending_lock);
        if (n == 0) {
            break;
        }
        usleep(100000);
        waits++;
        if ((waits % 50) == 0) {
            chimera_vfs_info(
                "notify_destroy: still %d RPL resolver(s) in flight after %d.%ds",
                n, waits / 10, (waits % 10) * 100);
        }
    }

    /* Free all watches from buckets.  Any sync events still queued are
     * teardown leftovers (their gated requests were completed or swept
     * before the frontends stopped); free them without acking. */
    for (i = 0; i < CHIMERA_VFS_NOTIFY_NUM_BUCKETS; i++) {
        watch = notify->buckets[i].watches;
        while (watch) {
            struct chimera_vfs_notify_sync_event *sev, *sev_tmp;

            watch_tmp = watch->next;
            sev       = watch->sync_events;
            while (sev) {
                sev_tmp = sev->next;
                free(sev);
                sev = sev_tmp;
            }
            pthread_mutex_destroy(&watch->lock);
            free(watch);
            watch = watch_tmp;
        }
        pthread_mutex_destroy(&notify->buckets[i].lock);
    }

    /* Free mount entries */
    HASH_ITER(hh, notify->mount_entries, me, me_tmp)
    {
        HASH_DEL(notify->mount_entries, me);
        free(me);
    }

    /* Free pending and free event lists */
    pev = notify->pending_events;
    while (pev) {
        pev_tmp = pev->next;
        free(pev);
        pev = pev_tmp;
    }

    pev = notify->free_events;
    while (pev) {
        pev_tmp = pev->next;
        free(pev);
        pev = pev_tmp;
    }

    pthread_mutex_destroy(&notify->mount_entries_lock);
    pthread_mutex_destroy(&notify->pending_lock);
    pthread_mutex_destroy(&notify->gates_lock);

    chimera_vfs_rpl_cache_destroy(notify->rpl_cache);

    free(notify);
} /* chimera_vfs_notify_destroy */

SYMBOL_EXPORT struct chimera_vfs_notify_watch *
chimera_vfs_notify_watch_create(
    struct chimera_vfs_notify    *notify,
    const uint8_t                *dir_fh,
    uint16_t                      dir_fh_len,
    uint32_t                      filter_mask,
    int                           watch_tree,
    chimera_vfs_notify_callback_t callback,
    void                         *private_data)
{
    struct chimera_vfs_notify_watch       *watch;
    struct chimera_vfs_notify_bucket      *bucket;
    struct chimera_vfs_notify_mount_entry *me;
    uint64_t                               fh_hash;
    int                                    bi;

    watch = calloc(1, sizeof(*watch));

    memcpy(watch->dir_fh, dir_fh, dir_fh_len);
    watch->dir_fh_len   = dir_fh_len;
    watch->dir_fh_hash  = chimera_vfs_hash(dir_fh, dir_fh_len);
    watch->filter_mask  = filter_mask;
    watch->watch_tree   = watch_tree;
    watch->callback     = callback;
    watch->private_data = private_data;
    watch->ring_head    = 0;
    watch->ring_count   = 0;
    watch->overflowed   = 0;
    pthread_mutex_init(&watch->lock, NULL);

    /* Insert into exact-watch bucket */
    fh_hash = watch->dir_fh_hash;
    bi      = chimera_vfs_notify_bucket_index(fh_hash);
    bucket  = &notify->buckets[bi];

    pthread_mutex_lock(&bucket->lock);
    /* If this object was removed in the brief window before this watch was
     * created (emit_delete ran first and found no watch to flag), inherit
     * that deletion now so the first drain reports STATUS_DELETE_PENDING
     * rather than parking forever.  The watch is not yet linked, so setting
     * deleted without watch->lock is safe — no other thread can see it. */
    for (int t = 0; t < CHIMERA_VFS_NOTIFY_TOMBSTONE_COUNT; t++) {
        struct chimera_vfs_notify_tombstone *ts = &bucket->tombstones[t];

        if (ts->stamp &&
            ts->fh_len == dir_fh_len &&
            memcmp(ts->fh, dir_fh, dir_fh_len) == 0 &&
            chimera_vfs_elapsed_ns(ts->stamp) < CHIMERA_VFS_NOTIFY_TOMBSTONE_NS) {
            watch->deleted = 1;
            break;
        }
    }
    watch->next     = bucket->watches;
    bucket->watches = watch;
    pthread_mutex_unlock(&bucket->lock);

    /* If subtree watch, also register in mount entry */
    if (watch_tree) {
        pthread_mutex_lock(&notify->mount_entries_lock);
        me = chimera_vfs_notify_get_mount_entry(notify,
                                                chimera_vfs_fh_mount_id(dir_fh));
        watch->subtree_next = me->subtree_watches;
        me->subtree_watches = watch;
        me->num_subtree_watches++;
        pthread_mutex_unlock(&notify->mount_entries_lock);
    }

    return watch;
} /* chimera_vfs_notify_watch_create */

/*
 * LOCK INVARIANT — DO NOT VIOLATE.
 *
 * This function takes (in order):
 *   1. watch->lock
 *   2. notify->mount_entries_lock
 *   3. watch->lock (again, briefly, to flip watch_tree)
 *
 * chimera_vfs_notify_emit holds bucket->lock or mount_entries_lock
 * while invoking watch->callback.  The callback (e.g.
 * chimera_smb_notify_callback) acquires the downstream
 * state->lock under that registry lock.  Therefore NO path
 * outside of emit may take a downstream lock (state->lock) and
 * then bucket->lock / mount_entries_lock — that would AB-BA
 * deadlock against an in-flight emit callback.
 *
 * watch_update is safe because it does NOT take state->lock at
 * all.  If you add code here that needs to coordinate with the
 * downstream consumer (e.g. notify state), refactor to drop the
 * mount_entries_lock first, or convert the callback dispatch in
 * emit to a deferred queue with state refcounting/RCU.  See the
 * lock-graph block comment above chimera_vfs_notify_emit.
 */
SYMBOL_EXPORT void
chimera_vfs_notify_watch_update(
    struct chimera_vfs_notify       *notify,
    struct chimera_vfs_notify_watch *watch,
    uint32_t                         filter_mask,
    int                              watch_tree)
{
    struct chimera_vfs_notify_mount_entry *me;
    int                                    old_tree;

    pthread_mutex_lock(&watch->lock);
    /* Paired with __atomic_load_n in emit's bucket-walk read.  The
     * lock makes the store visible under the bucket-walk; the atomic
     * load makes the read well-defined per C11. */
    __atomic_store_n(&watch->filter_mask, filter_mask, __ATOMIC_RELAXED);
    old_tree = watch->watch_tree;
    pthread_mutex_unlock(&watch->lock);

    if (old_tree == watch_tree) {
        return;
    }

    /* watch_tree flipped — purge any events queued under the old mode.
     * Subtree events carry a path-prefixed name ("sub/dir/file") while
     * exact-mode events carry a bare filename.  Delivering a stale path
     * to a non-tree consumer (or vice versa) would mislead the client,
     * and we have no per-event mode tag to filter selectively.  Force
     * the client to rescan via overflow semantics — but ONLY if there is
     * actually something queued (or an overflow already pending).  When
     * the ring is empty (the common case of a client re-arming a fresh
     * watch with a different recursion flag) signalling a spurious
     * overflow would make the very next CHANGE_NOTIFY complete
     * immediately with STATUS_NOTIFY_ENUM_DIR instead of parking
     * (smb2.notify.rec / mask-change). */
    pthread_mutex_lock(&watch->lock);
    if (watch->ring_count > 0 || watch->overflowed) {
        watch->ring_head  = 0;
        watch->ring_count = 0;
        watch->overflowed = 1;
    }
    pthread_mutex_unlock(&watch->lock);

    /* watch_tree flipped — relink in mount entries' subtree list. */
    pthread_mutex_lock(&notify->mount_entries_lock);

    HASH_FIND(hh, notify->mount_entries,
              chimera_vfs_fh_mount_id(watch->dir_fh),
              CHIMERA_VFS_MOUNT_ID_SIZE, me);

    if (old_tree && !watch_tree) {
        /* Remove from subtree list */
        if (me) {
            struct chimera_vfs_notify_watch **pp = &me->subtree_watches;
            while (*pp) {
                if (*pp == watch) {
                    *pp                 = watch->subtree_next;
                    watch->subtree_next = NULL;
                    me->num_subtree_watches--;
                    break;
                }
                pp = &(*pp)->subtree_next;
            }
        }
    } else if (!old_tree && watch_tree) {
        /* Add to subtree list */
        if (!me) {
            me = chimera_vfs_notify_get_mount_entry(
                notify, chimera_vfs_fh_mount_id(watch->dir_fh));
        }
        watch->subtree_next = me->subtree_watches;
        me->subtree_watches = watch;
        me->num_subtree_watches++;
    }

    pthread_mutex_lock(&watch->lock);
    watch->watch_tree = watch_tree;
    pthread_mutex_unlock(&watch->lock);

    pthread_mutex_unlock(&notify->mount_entries_lock);
} /* chimera_vfs_notify_watch_update */

SYMBOL_EXPORT void
chimera_vfs_notify_watch_destroy(
    struct chimera_vfs_notify       *notify,
    struct chimera_vfs_notify_watch *watch)
{
    struct chimera_vfs_notify_bucket      *bucket;
    struct chimera_vfs_notify_watch      **pp;
    struct chimera_vfs_notify_mount_entry *me;
    struct chimera_vfs_notify_sync_event  *sev, *sev_next;
    int                                    bi;

    /* Remove from exact-watch bucket */
    bi     = chimera_vfs_notify_bucket_index(watch->dir_fh_hash);
    bucket = &notify->buckets[bi];

    pthread_mutex_lock(&bucket->lock);
    pp = &bucket->watches;
    while (*pp) {
        if (*pp == watch) {
            *pp = watch->next;
            break;
        }
        pp = &(*pp)->next;
    }
    pthread_mutex_unlock(&bucket->lock);

    if (watch->sync) {
        __atomic_fetch_sub(&notify->num_sync_watches, 1, __ATOMIC_RELAXED);

        /* Ack any events the consumer never drained: the watcher is going
         * away (its kernel objects with it), so the gated mutations must
         * not be left stalled on it.  The watch is already unlinked, so no
         * new events can arrive. */
        sev                     = watch->sync_events;
        watch->sync_events      = NULL;
        watch->sync_events_tail = NULL;
        while (sev) {
            sev_next = sev->next;
            chimera_vfs_notify_gate_ack(notify, sev);
            sev = sev_next;
        }
    }

    /* Remove from subtree list if applicable */
    if (watch->watch_tree) {
        struct chimera_vfs_notify_mount_entry *me_to_free = NULL;

        pthread_mutex_lock(&notify->mount_entries_lock);
        HASH_FIND(hh, notify->mount_entries,
                  chimera_vfs_fh_mount_id(watch->dir_fh),
                  CHIMERA_VFS_MOUNT_ID_SIZE, me);
        if (me) {
            struct chimera_vfs_notify_watch **spp = &me->subtree_watches;
            while (*spp) {
                if (*spp == watch) {
                    *spp = watch->subtree_next;
                    me->num_subtree_watches--;
                    break;
                }
                spp = &(*spp)->subtree_next;
            }

            /* GC the mount_entry once the last subtree watch is gone.
             * Safe to free here because all resolver paths dereference
             * `me` only while holding mount_entries_lock (which we
             * hold).  Without this the entry would leak across the
             * daemon's lifetime as mounts came and went. */
            if (me->num_subtree_watches == 0) {
                HASH_DEL(notify->mount_entries, me);
                me_to_free = me;
            }
        }
        pthread_mutex_unlock(&notify->mount_entries_lock);

        free(me_to_free);
    }

    pthread_mutex_destroy(&watch->lock);
    free(watch);
} /* chimera_vfs_notify_watch_destroy */

SYMBOL_EXPORT int
chimera_vfs_notify_drain(
    struct chimera_vfs_notify_watch *watch,
    struct chimera_vfs_notify_event *events,
    int                              max_events,
    int                             *overflowed)
{
    int count = 0;
    int idx;

    pthread_mutex_lock(&watch->lock);

    *overflowed = watch->overflowed;

    while (count < max_events && watch->ring_count > 0) {
        idx              = watch->ring_head;
        events[count]    = watch->ring[idx];
        watch->ring_head = (idx + 1) & (CHIMERA_VFS_NOTIFY_RING_SIZE - 1);
        watch->ring_count--;
        count++;
    }

    if (watch->ring_count == 0) {
        watch->overflowed = 0;
    }

    pthread_mutex_unlock(&watch->lock);

    return count;
} /* chimera_vfs_notify_drain */

SYMBOL_EXPORT int
chimera_vfs_notify_watch_take_deleted(struct chimera_vfs_notify_watch *watch)
{
    int deleted;

    pthread_mutex_lock(&watch->lock);
    deleted        = watch->deleted;
    watch->deleted = 0;
    pthread_mutex_unlock(&watch->lock);

    return deleted;
} /* chimera_vfs_notify_watch_take_deleted */

/*
 * Lock invariants for the emit/destroy/callback dance:
 *
 *  - The watch->callback is invoked from inside emit while the bucket
 *    (or mount_entries) lock is held.  This is INTENTIONAL: holding
 *    the registry lock across the callback call ties the lifetime of
 *    the callback's `private_data` (e.g. an SMB notify_state) to the
 *    watch's presence in the registry.  watch_destroy must take the
 *    same bucket lock to unlink, so it waits for any in-flight emit
 *    (and its callback) to finish before freeing the watch — and by
 *    extension before notify_close frees the per-open state.
 *
 *  - For this to remain deadlock-free, NO code path that takes the
 *    callback's downstream locks (e.g. chimera_smb_notify_state.lock)
 *    may then acquire bucket->lock or mount_entries_lock.  Callers
 *    must take VFS-side locks first, downstream locks last.  Verify:
 *      change_notify handler:        state->lock  -> watch->lock     (no VFS-registry lock)
 *      smb_notify callback:          bucket/mount -> state->lock     (registry held first)
 *      smb_notify cancel/drop/close: state->lock  -> (no VFS-registry lock)
 *      smb_notify_close watch_destroy: takes bucket/mount alone after
 *                                      releasing state->lock
 *
 *  Adding any path that takes state->lock then bucket/mount lock will
 *  break this invariant.  An alternative — collecting callbacks to a
 *  temp list and invoking them after releasing the registry lock — is
 *  safer but requires state refcounting/RCU to keep private_data
 *  alive across the lock gap.  Switch to that model if a deadlock-
 *  inducing path is ever needed.
 */

static void
chimera_vfs_notify_emit_body(
    struct chimera_vfs_notify *notify,
    const uint8_t             *dir_fh,
    uint16_t                   dir_fh_len,
    uint32_t                   action,
    const char                *name,
    uint16_t                   name_len,
    const char                *old_name,
    uint16_t                   old_name_len)
{
    struct chimera_vfs_notify_bucket        *bucket;
    struct chimera_vfs_notify_watch         *watch;
    struct chimera_vfs_notify_mount_entry   *me;
    struct chimera_vfs_notify_pending_event *pev;
    uint64_t                                 fh_hash;
    int                                      bi;

    if (!notify) {
        return;
    }

    /* Clamp name lengths at the top.  Downstream code uses these as
     * memcpy sizes into bounded buffers (pev->name, pev->path_buf) and
     * subtracts name_len from CHIMERA_VFS_PATH_MAX to seed path_offset.
     * A caller passing a value > NAME_MAX would overflow pev->name; a
     * value > PATH_MAX would underflow path_offset.  Names this long
     * cannot be a single filename — re-enforce here so internal misuse
     * cannot corrupt memory. */
    if (name_len > CHIMERA_VFS_NAME_MAX) {
        name_len = CHIMERA_VFS_NAME_MAX;
    }
    if (old_name_len > CHIMERA_VFS_NAME_MAX) {
        old_name_len = CHIMERA_VFS_NAME_MAX;
    }

    fh_hash = chimera_vfs_hash(dir_fh, dir_fh_len);

    /* 1. Exact watches */
    bi     = chimera_vfs_notify_bucket_index(fh_hash);
    bucket = &notify->buckets[bi];

    pthread_mutex_lock(&bucket->lock);

    for (watch = bucket->watches; watch; watch = watch->next) {
        /* Read filter_mask atomically — watch_update writes it under
         * watch->lock, and we don't take watch->lock here.  Relaxed
         * order is enough: the SMB layer re-filters at response time
         * anyway, so a momentarily stale mask is benign. */
        uint32_t mask = __atomic_load_n(&watch->filter_mask, __ATOMIC_RELAXED);

        if (watch->dir_fh_len == dir_fh_len &&
            memcmp(watch->dir_fh, dir_fh, dir_fh_len) == 0 &&
            (mask & action)) {

            chimera_vfs_notify_watch_enqueue(watch, action,
                                             name, name_len,
                                             old_name, old_name_len);

            if (watch->callback) {
                watch->callback(watch, watch->private_data);
            }
        }
    }

    pthread_mutex_unlock(&bucket->lock);

    /* 2. RPL cache invalidation */
    if (notify->rpl_cache) {
        if (action & CHIMERA_VFS_NOTIFY_RENAMED) {
            chimera_vfs_rpl_cache_invalidate(notify->rpl_cache,
                                             fh_hash,
                                             dir_fh, dir_fh_len,
                                             chimera_vfs_hash(old_name, old_name_len),
                                             old_name, old_name_len);
        }

        if (action & (CHIMERA_VFS_NOTIFY_FILE_REMOVED | CHIMERA_VFS_NOTIFY_DIR_REMOVED)) {
            chimera_vfs_rpl_cache_invalidate(notify->rpl_cache,
                                             fh_hash,
                                             dir_fh, dir_fh_len,
                                             chimera_vfs_hash(name, name_len),
                                             name, name_len);
        }
    }

    /* 3. Subtree watches */
    pthread_mutex_lock(&notify->mount_entries_lock);

    HASH_FIND(hh, notify->mount_entries,
              chimera_vfs_fh_mount_id(dir_fh),
              CHIMERA_VFS_MOUNT_ID_SIZE, me);

    if (!me || me->num_subtree_watches == 0) {
        pthread_mutex_unlock(&notify->mount_entries_lock);
        return;
    }

    if (name_len == 0) {
        /* The event has no leaf name.  This happens for the source-
         * side emit of a cross-directory rename: the EXACT-watch path
         * above already delivered the OLD_NAME record correctly using
         * old_name, but the subtree resolver would build a leafless
         * relative path ("parent/parent/") that the SMB serializer
         * would then emit as a malformed NEW_NAME record.  Overflow
         * subtree watches instead so the client rescans; same
         * rationale as the !has_rpl / max-pending fallbacks. */
        for (watch = me->subtree_watches; watch; watch = watch->subtree_next) {
            if (watch->dir_fh_len == dir_fh_len &&
                memcmp(watch->dir_fh, dir_fh, dir_fh_len) == 0) {
                /* Already covered by the exact-watch dispatch above. */
                continue;
            }
            chimera_vfs_notify_watch_overflow(watch);
            if (watch->callback) {
                watch->callback(watch, watch->private_data);
            }
        }
        pthread_mutex_unlock(&notify->mount_entries_lock);
        return;
    }

    if (!me->has_rpl) {
        /* Backend can't reverse-path-lookup, so we cannot compute the
         * descendant's relative path.  Mark each subtree watch as
         * overflowed instead of enqueueing a synthetic event — the
         * consumer translates overflow to STATUS_NOTIFY_ENUM_DIR which
         * Windows correctly interprets as "rescan the directory".  A
         * synthetic FILE_MODIFIED on "." would be ambiguous: a client
         * could legitimately read it as "the watched directory itself
         * was modified" rather than "an unknown descendant changed".
         *
         * If the event is on the mount root, no subtree watch can be
         * an ancestor of the affected entry: subtree watches live at
         * or below the mount root, so the affected entry (mount-root/
         * leaf) is either AT a watch's root (the entry IS the watched
         * dir — not a descendant, not reportable) or is a sibling of a
         * watch ancestor.  Either way the event must not enqueue into
         * any subtree watch, and overflowing would wrongly trigger a
         * NOTIFY_ENUM_DIR rescan.  Mirror the RPL resolver's depth-1
         * at_root short-circuit and just return. */
        if (me->root_fh_len > 0 &&
            dir_fh_len == me->root_fh_len &&
            memcmp(dir_fh, me->root_fh, dir_fh_len) == 0) {
            pthread_mutex_unlock(&notify->mount_entries_lock);
            return;
        }

        for (watch = me->subtree_watches; watch; watch = watch->subtree_next) {
            /* Skip if the event is on the watched directory itself
             * (already handled by exact match above). */
            if (watch->dir_fh_len == dir_fh_len &&
                memcmp(watch->dir_fh, dir_fh, dir_fh_len) == 0) {
                continue;
            }

            chimera_vfs_notify_watch_overflow(watch);

            if (watch->callback) {
                watch->callback(watch, watch->private_data);
            }
        }

        pthread_mutex_unlock(&notify->mount_entries_lock);
        return;
    }

    pthread_mutex_unlock(&notify->mount_entries_lock);

    /* RPL path: queue for async resolution */
    pthread_mutex_lock(&notify->pending_lock);

    /* Refuse to start new resolvers once destroy() has begun.  Without
     * this the destroy wait loop could observe num_pending == 0 between
     * a callback completing and a new emit allocating the next pev,
     * race past the wait, and free state behind an in-flight resolver. */
    if (notify->shutdown) {
        pthread_mutex_unlock(&notify->pending_lock);
        return;
    }

    if (notify->num_pending >= CHIMERA_VFS_NOTIFY_MAX_PENDING) {
        /* Resolver capacity exhausted — we cannot enqueue another async
         * walk, so we cannot compute relative paths for the affected
         * subtree watches.  Same rationale as the !has_rpl branch
         * above: mark each watch overflowed so the consumer escalates
         * to STATUS_NOTIFY_ENUM_DIR rather than inventing a "."
         * MODIFIED record. */
        pthread_mutex_unlock(&notify->pending_lock);

        pthread_mutex_lock(&notify->mount_entries_lock);
        HASH_FIND(hh, notify->mount_entries,
                  chimera_vfs_fh_mount_id(dir_fh),
                  CHIMERA_VFS_MOUNT_ID_SIZE, me);
        if (me) {
            for (watch = me->subtree_watches; watch; watch = watch->subtree_next) {
                chimera_vfs_notify_watch_overflow(watch);
                if (watch->callback) {
                    watch->callback(watch, watch->private_data);
                }
            }
        }
        pthread_mutex_unlock(&notify->mount_entries_lock);
        return;
    }

    pev = chimera_vfs_notify_alloc_pending(notify);
    if (unlikely(!pev)) {
        /* OOM allocating a fresh pev.  Fall back to the coarse overflow
         * path so subtree watchers rescan rather than missing the event
         * entirely.  num_pending was not incremented. */
        pthread_mutex_unlock(&notify->pending_lock);

        pthread_mutex_lock(&notify->mount_entries_lock);
        HASH_FIND(hh, notify->mount_entries,
                  chimera_vfs_fh_mount_id(dir_fh),
                  CHIMERA_VFS_MOUNT_ID_SIZE, me);
        if (me) {
            for (watch = me->subtree_watches; watch; watch = watch->subtree_next) {
                if (watch->dir_fh_len == dir_fh_len &&
                    memcmp(watch->dir_fh, dir_fh, dir_fh_len) == 0) {
                    continue;
                }
                chimera_vfs_notify_watch_overflow(watch);
                if (watch->callback) {
                    watch->callback(watch, watch->private_data);
                }
            }
        }
        pthread_mutex_unlock(&notify->mount_entries_lock);
        return;
    }
    notify->num_pending++;
    pthread_mutex_unlock(&notify->pending_lock);

    pev->action       = action;
    pev->dir_fh_len   = dir_fh_len;
    pev->name_len     = name_len;
    pev->old_name_len = old_name_len;
    pev->walk_fh_len  = dir_fh_len;
    pev->depth        = 0;
    pev->notify       = notify;

    memcpy(pev->dir_fh, dir_fh, dir_fh_len);
    memcpy(pev->walk_fh, dir_fh, dir_fh_len);
    memcpy(pev->mount_id, chimera_vfs_fh_mount_id(dir_fh), CHIMERA_VFS_MOUNT_ID_SIZE);

    if (name_len && name) {
        memcpy(pev->name, name, name_len);
    }

    if (old_name_len && old_name) {
        memcpy(pev->old_name, old_name, old_name_len);
    }

    /* Initialize path_buf with the leaf name at the end.  Cross-dir
     * rename emits a "source-side" event with name_len==0; the
     * resolver still walks up the source parent's ancestors but
     * delivers a relpath without a leaf component, which is the best
     * partial signal we can give a subtree watcher in that case. */
    pev->path_offset = CHIMERA_VFS_PATH_MAX - name_len;
    if (name_len && name) {
        memcpy(&pev->path_buf[pev->path_offset], name, name_len);
    }

    /* Start the resolve chain */
    chimera_vfs_notify_resolve(pev);
} /* chimera_vfs_notify_emit_body */

/* A directory's contents/metadata just changed (a child added / removed /
 * renamed, or a child's attributes set).  Break every SMB3 directory lease on
 * that directory so a client caching its enumeration re-reads it — this is the
 * cross-protocol coherency point: an NFS or S3 mutation breaks an SMB client's
 * directory lease exactly as an SMB mutation does.  Run before taking any notify
 * registry lock (the break brackets the claim core's file lock entirely, so
 * there is no nesting against bucket/mount locks — preserving the lock-order
 * invariant documented above).  `has_skip` spares the lease named by a
 * ParentLeaseKey the mutating client supplied (self-exemption, via the trigger
 * engine's KEY circle); the no-skip wrapper (a leaseless mutator) passes a NULL
 * actor and breaks every directory lease. */
static inline void
chimera_vfs_notify_dir_lease_break(
    struct chimera_vfs_notify *notify,
    const uint8_t             *dir_fh,
    uint16_t                   dir_fh_len,
    uint64_t                   skip_lo,
    uint64_t                   skip_hi,
    bool                       has_skip)
{
    struct chimera_claim_actor actor;

    if (notify && notify->vfs && notify->vfs->vfs_state) {
        if (has_skip) {
            /* The ParentLeaseKey bytes ride in actor.owner.key; everything
             * else stays zero (a zero owner matches no real claim, so only
             * the KEY-circle exemption applies). */
            memset(&actor, 0, sizeof(actor));
            memcpy(actor.owner.key, &skip_lo, 8);
            memcpy(actor.owner.key + 8, &skip_hi, 8);
        }
        chimera_vfs_claim_invalidate(notify->vfs->vfs_state,
                                     dir_fh, dir_fh_len,
                                     chimera_vfs_hash(dir_fh, dir_fh_len),
                                     CHIMERA_TRIGGER_DIR_CONTENT,
                                     has_skip ? &actor : NULL,
                                     0);
    }
} /* chimera_vfs_notify_dir_lease_break */

SYMBOL_EXPORT void
chimera_vfs_notify_emit(
    struct chimera_vfs_notify *notify,
    const uint8_t             *dir_fh,
    uint16_t                   dir_fh_len,
    uint32_t                   action,
    const char                *name,
    uint16_t                   name_len,
    const char                *old_name,
    uint16_t                   old_name_len)
{
    chimera_vfs_notify_dir_lease_break(notify, dir_fh, dir_fh_len, 0, 0, false);
    chimera_vfs_notify_emit_body(notify, dir_fh, dir_fh_len, action,
                                 name, name_len, old_name, old_name_len);
} /* chimera_vfs_notify_emit */

SYMBOL_EXPORT void
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
    bool                       has_skip)
{
    chimera_vfs_notify_dir_lease_break(notify, dir_fh, dir_fh_len,
                                       skip_lo, skip_hi, has_skip);
    chimera_vfs_notify_emit_body(notify, dir_fh, dir_fh_len, action,
                                 name, name_len, old_name, old_name_len);
} /* chimera_vfs_notify_emit_lease */

SYMBOL_EXPORT void
chimera_vfs_notify_emit_nobreak(
    struct chimera_vfs_notify *notify,
    const uint8_t             *dir_fh,
    uint16_t                   dir_fh_len,
    uint32_t                   action,
    const char                *name,
    uint16_t                   name_len,
    const char                *old_name,
    uint16_t                   old_name_len)
{
    /* Deliver the CHANGE_NOTIFY event WITHOUT breaking directory leases.  Used
     * by the SMB create path when a create-capable disposition merely OPENED an
     * existing file (no directory-content change): change-notify still emits the
     * (conservative) FILE_ADDED, but a directory read lease must NOT break,
     * because nothing in the directory actually changed (a re-open of an
     * existing entry; smbtorture dirlease.rename re-opens the file before
     * renaming it). */
    chimera_vfs_notify_emit_body(notify, dir_fh, dir_fh_len, action,
                                 name, name_len, old_name, old_name_len);
} /* chimera_vfs_notify_emit_nobreak */

SYMBOL_EXPORT void
chimera_vfs_notify_emit_delete(
    struct chimera_vfs_notify *notify,
    const uint8_t             *fh,
    uint16_t                   fh_len)
{
    struct chimera_vfs_notify_bucket *bucket;
    struct chimera_vfs_notify_watch  *watch;
    uint64_t                          fh_hash;
    int                               bi;

    if (!notify) {
        return;
    }

    /* Only exact watches matter: CHANGE_NOTIFY is armed on a handle to the
     * object itself, so a watch keyed on the removed object's own FH is the
     * one that must learn it is gone.  (Subtree watchers on an ancestor are
     * handled by the regular DIR_REMOVED emit on the parent.) */
    fh_hash = chimera_vfs_hash(fh, fh_len);
    bi      = chimera_vfs_notify_bucket_index(fh_hash);
    bucket  = &notify->buckets[bi];

    pthread_mutex_lock(&bucket->lock);

    for (watch = bucket->watches; watch; watch = watch->next) {
        if (watch->dir_fh_len == fh_len &&
            memcmp(watch->dir_fh, fh, fh_len) == 0) {

            pthread_mutex_lock(&watch->lock);
            watch->deleted = 1;
            pthread_mutex_unlock(&watch->lock);

            if (watch->callback) {
                watch->callback(watch, watch->private_data);
            }
        }
    }

    /* Record a tombstone for this FH unconditionally — even when no watch
     * currently matches.  A CHANGE_NOTIFY racing this delete may arm its
     * watch a moment from now (the deleting client need not wait for the
     * watcher's interim reply); watch_create consults these tombstones so
     * that late-armed watch is born already-deleted instead of parking on
     * an object that will never emit another event. */
    {
        struct chimera_vfs_notify_tombstone *ts =
            &bucket->tombstones[bucket->tombstone_next];

        memcpy(ts->fh, fh, fh_len);
        ts->fh_len             = fh_len;
        ts->stamp              = chimera_vfs_now_ticks();
        bucket->tombstone_next =
            (bucket->tombstone_next + 1) % CHIMERA_VFS_NOTIFY_TOMBSTONE_COUNT;
    }

    pthread_mutex_unlock(&bucket->lock);
} /* chimera_vfs_notify_emit_delete */

/* ----------------------------------------------------------------
 * Synchronous coherence: sync watches and completion gates
 * ----------------------------------------------------------------
 *
 * See vfs_notify.h for the model.  A gate parks a namespace mutation's
 * COMPLETION (after the backend applied it, before the protocol callback
 * replies) until every sync watcher on the affected directory(ies) acks the
 * per-name invalidation events delivered to it — so when the mutating
 * caller sees its operation return, every sync watcher's caches for those
 * names are already gone.  The watcher whose origin matches the mutating
 * cred's origin is exempt: it is natively coherent with its own operation,
 * and its ack path can deadlock against the very syscall it would gate
 * (the kernel holds the parent's lock across the FUSE request while the
 * invalidation write needs that same lock).
 *
 * Gate lifecycle: refs = 1 (creation) + 1 per delivered event; pending =
 * 1 (the emitter's arm hold) + 1 per delivered event.  pending hitting
 * zero fires the gate exactly once (guarded by gate->request under
 * gates_lock); refs hitting zero frees it.  The deadline sweep fires
 * overdue gates the same way, and late acks then find request == NULL.
 */

struct chimera_vfs_notify_gate {
    int                             pending; /* atomics */
    int                             refs;    /* atomics */
    struct chimera_vfs_request     *request; /* under gates_lock; NULL = fired */
    uint64_t                        deadline;
    struct chimera_vfs_notify_gate *prev;
    struct chimera_vfs_notify_gate *next;
};

SYMBOL_EXPORT void
chimera_vfs_notify_watch_set_sync(
    struct chimera_vfs_notify       *notify,
    struct chimera_vfs_notify_watch *watch,
    const void                      *origin)
{
    pthread_mutex_lock(&watch->lock);
    watch->origin = origin;
    if (!watch->sync) {
        watch->sync = 1;
        __atomic_fetch_add(&notify->num_sync_watches, 1, __ATOMIC_RELAXED);
    }
    pthread_mutex_unlock(&watch->lock);
} /* chimera_vfs_notify_watch_set_sync */

SYMBOL_EXPORT struct chimera_vfs_notify_sync_event *
chimera_vfs_notify_drain_sync(struct chimera_vfs_notify_watch *watch)
{
    struct chimera_vfs_notify_sync_event *head;

    pthread_mutex_lock(&watch->lock);
    head                    = watch->sync_events;
    watch->sync_events      = NULL;
    watch->sync_events_tail = NULL;
    pthread_mutex_unlock(&watch->lock);

    return head;
} /* chimera_vfs_notify_drain_sync */

static void
chimera_vfs_notify_gate_unref(struct chimera_vfs_notify_gate *gate)
{
    if (__atomic_sub_fetch(&gate->refs, 1, __ATOMIC_ACQ_REL) == 0) {
        free(gate);
    }
} /* chimera_vfs_notify_gate_unref */

static void
chimera_vfs_notify_gate_mark(
    struct chimera_vfs_notify  *notify,
    struct chimera_vfs_request *request,
    int                         delta);

/* Detach the gated request, exactly once.  Returns it (for the caller to
 * resume) or NULL if the gate already fired (a late ack, or the sweep won).
 * Clears the winner's in-flight mark: the mark must survive the whole gate
 * wait -- the origin's kernel holds the directory lock until the reply this
 * fire unblocks, and a peer gating on a still-locked kernel is exactly the
 * cross-mount cycle the mark exists to break. */
static struct chimera_vfs_request *
chimera_vfs_notify_gate_fire(
    struct chimera_vfs_notify      *notify,
    struct chimera_vfs_notify_gate *gate)
{
    struct chimera_vfs_request *request;

    pthread_mutex_lock(&notify->gates_lock);
    request       = gate->request;
    gate->request = NULL;
    if (request) {
        DL_DELETE(notify->gates, gate);
    }
    pthread_mutex_unlock(&notify->gates_lock);

    if (request) {
        request->notify_gate = NULL;
        chimera_vfs_notify_gate_mark(notify, request, -1);
        /* Release the creation reference. */
        chimera_vfs_notify_gate_unref(gate);
    }

    return request;
} /* chimera_vfs_notify_gate_fire */

SYMBOL_EXPORT void
chimera_vfs_notify_gate_ack(
    struct chimera_vfs_notify            *notify,
    struct chimera_vfs_notify_sync_event *event)
{
    struct chimera_vfs_notify_gate *gate = event->gate;
    struct chimera_vfs_request     *request;

    free(event);

    if (__atomic_sub_fetch(&gate->pending, 1, __ATOMIC_ACQ_REL) == 0) {
        request = chimera_vfs_notify_gate_fire(notify, gate);
        if (request) {
            /* Resume the parked completion on its owning thread; the
             * drain loop routes notify_gate_resume to request->complete. */
            request->notify_gate_resume = 1;
            chimera_vfs_io_resume_post(request);
        }
    }

    chimera_vfs_notify_gate_unref(gate);
} /* chimera_vfs_notify_gate_ack */

SYMBOL_EXPORT void
chimera_vfs_notify_gate_sweep(struct chimera_vfs_notify *notify)
{
    struct chimera_vfs_notify_gate *gate, *next;
    struct chimera_vfs_request     *expired[16];
    int                             n = 0;
    uint64_t                        now;
    int                             i;

    if (!notify || !notify->gates) { /* unlocked peek: empty is the norm */
        return;
    }

    now = chimera_vfs_now_ticks();

    pthread_mutex_lock(&notify->gates_lock);
    for (gate = notify->gates; gate && n < 16; gate = next) {
        next = gate->next;
        if (gate->request && now >= gate->deadline) {
            expired[n++]  = gate->request;
            gate->request = NULL;
            DL_DELETE(notify->gates, gate);
            expired[n - 1]->notify_gate = NULL;
            chimera_vfs_notify_gate_unref(gate);
        }
    }
    pthread_mutex_unlock(&notify->gates_lock);

    for (i = 0; i < n; i++) {
        chimera_vfs_info("notify gate: sync watcher ack overdue; "
                         "completing gated op without it");
        chimera_vfs_notify_gate_mark(notify, expired[i], -1);
        expired[i]->notify_gate_resume = 1;
        chimera_vfs_io_resume_post(expired[i]);
    }
} /* chimera_vfs_notify_gate_sweep */

/* The parent directory(ies) a gated opcode mutates.  Shared by the
 * in-flight marking (install/completion) and must therefore be a pure
 * function of request fields that are stable from dispatch to
 * completion.  Returns the count (0-2). */
struct chimera_vfs_notify_gate_dir {
    const uint8_t *fh;
    uint16_t       fh_len;
};

static int
chimera_vfs_notify_gate_dirs(
    struct chimera_vfs_request         *request,
    struct chimera_vfs_notify_gate_dir *dirs)
{
    switch (request->opcode) {
        case CHIMERA_VFS_OP_REMOVE_AT:
            dirs[0].fh     = request->remove_at.handle->fh;
            dirs[0].fh_len = request->remove_at.handle->fh_len;
            return 1;
        case CHIMERA_VFS_OP_RENAME_AT:
            dirs[0].fh     = request->fh;
            dirs[0].fh_len = request->fh_len;
            dirs[1].fh     = request->rename_at.new_fh;
            dirs[1].fh_len = request->rename_at.new_fhlen;
            return 2;
        case CHIMERA_VFS_OP_LINK_AT:
            dirs[0].fh     = request->link_at.dir_fh;
            dirs[0].fh_len = request->link_at.dir_fhlen;
            return 1;
        case CHIMERA_VFS_OP_MKDIR_AT:
            dirs[0].fh     = request->mkdir_at.handle->fh;
            dirs[0].fh_len = request->mkdir_at.handle->fh_len;
            return 1;
        case CHIMERA_VFS_OP_MKNOD_AT:
            dirs[0].fh     = request->mknod_at.handle->fh;
            dirs[0].fh_len = request->mknod_at.handle->fh_len;
            return 1;
        case CHIMERA_VFS_OP_SYMLINK_AT:
            dirs[0].fh     = request->fh;
            dirs[0].fh_len = request->fh_len;
            return 1;
        case CHIMERA_VFS_OP_OPEN_AT:
            dirs[0].fh     = request->open_at.handle->fh;
            dirs[0].fh_len = request->open_at.handle->fh_len;
            return 1;
        default:
            return 0;
    } /* switch */
} /* chimera_vfs_notify_gate_dirs */

/* Mark (+1) / clear (-1) the mutating origin's own sync watches on the
 * affected directories.  See gated_inflight in vfs_notify.h: the mark is
 * what lets a concurrent peer's gate skip a watcher whose kernel is
 * holding the directory locked for this very operation. */
static void
chimera_vfs_notify_gate_mark(
    struct chimera_vfs_notify  *notify,
    struct chimera_vfs_request *request,
    int                         delta)
{
    struct chimera_vfs_notify_gate_dir dirs[2];
    struct chimera_vfs_notify_bucket  *bucket;
    struct chimera_vfs_notify_watch   *watch;
    const void                        *origin =
        request->cred ? request->cred->origin : NULL;
    int                                ndirs, i, bi;

    if (!origin) {
        /* Only origin-bearing endpoints (FUSE mounts) hold kernel locks a
         * blocking invalidation could deadlock against. */
        return;
    }

    ndirs = chimera_vfs_notify_gate_dirs(request, dirs);

    for (i = 0; i < ndirs; i++) {
        bi = chimera_vfs_notify_bucket_index(
            chimera_vfs_hash(dirs[i].fh, dirs[i].fh_len));
        bucket = &notify->buckets[bi];

        pthread_mutex_lock(&bucket->lock);
        for (watch = bucket->watches; watch; watch = watch->next) {
            if (watch->sync &&
                watch->origin == origin &&
                watch->dir_fh_len == dirs[i].fh_len &&
                memcmp(watch->dir_fh, dirs[i].fh, dirs[i].fh_len) == 0) {
                watch->gated_inflight += delta;
                if (watch->gated_inflight < 0) {
                    /* This watch was created after the op's install mark
                     * (the marked watch was destroyed mid-operation);
                     * absorb the unmatched clear. */
                    watch->gated_inflight = 0;
                }
            }
        }
        pthread_mutex_unlock(&bucket->lock);
    }
} /* chimera_vfs_notify_gate_mark */

/* Deliver one gated event to every sync watch on dir_fh (except the
 * mutation's own origin).  Each delivery raises the gate's pending/refs
 * before the watch callback can possibly ack it. */
static void
chimera_vfs_notify_emit_sync(
    struct chimera_vfs_notify      *notify,
    struct chimera_vfs_notify_gate *gate,
    const void                     *origin,
    const uint8_t                  *dir_fh,
    uint16_t                        dir_fh_len,
    uint32_t                        action,
    const char                     *name,
    uint16_t                        name_len,
    const char                     *old_name,
    uint16_t                        old_name_len)
{
    struct chimera_vfs_notify_bucket     *bucket;
    struct chimera_vfs_notify_watch      *watch;
    struct chimera_vfs_notify_sync_event *ev;
    uint64_t                              fh_hash;
    int                                   bi;

    if (name_len > CHIMERA_VFS_NAME_MAX) {
        name_len = CHIMERA_VFS_NAME_MAX;
    }
    if (old_name_len > CHIMERA_VFS_NAME_MAX) {
        old_name_len = CHIMERA_VFS_NAME_MAX;
    }

    fh_hash = chimera_vfs_hash(dir_fh, dir_fh_len);
    bi      = chimera_vfs_notify_bucket_index(fh_hash);
    bucket  = &notify->buckets[bi];

    pthread_mutex_lock(&bucket->lock);

    for (watch = bucket->watches; watch; watch = watch->next) {
        uint32_t mask = __atomic_load_n(&watch->filter_mask, __ATOMIC_RELAXED);

        if (!watch->sync ||
            watch->dir_fh_len != dir_fh_len ||
            memcmp(watch->dir_fh, dir_fh, dir_fh_len) != 0 ||
            !(mask & action)) {
            continue;
        }
        if (origin && watch->origin == origin) {
            continue;
        }
        /* This watcher's kernel is mid-mutation in the same directory: a
         * blocking invalidation into it would deadlock the two mounts
         * against each other (each kernel holds its own directory lock
         * while awaiting its gated reply).  Skip it -- the regular async
         * emit that follows the completion converges its view.  (Read
         * under the bucket lock, like the marks.) */
        if (watch->gated_inflight > 0) {
            continue;
        }

        ev = calloc(1, sizeof(*ev));
        if (!ev) {
            continue; /* degrade to the async emit for this watcher */
        }

        ev->action       = action;
        ev->name_len     = name_len;
        ev->old_name_len = old_name_len;
        if (name_len) {
            memcpy(ev->name, name, name_len);
        }
        if (old_name_len && old_name) {
            memcpy(ev->old_name, old_name, old_name_len);
        }
        ev->gate = gate;

        __atomic_fetch_add(&gate->pending, 1, __ATOMIC_ACQ_REL);
        __atomic_fetch_add(&gate->refs, 1, __ATOMIC_ACQ_REL);

        pthread_mutex_lock(&watch->lock);
        if (watch->sync_events_tail) {
            watch->sync_events_tail->next = ev;
        } else {
            watch->sync_events = ev;
        }
        watch->sync_events_tail = ev;
        pthread_mutex_unlock(&watch->lock);

        if (watch->callback) {
            watch->callback(watch, watch->private_data);
        }
    }

    pthread_mutex_unlock(&bucket->lock);
} /* chimera_vfs_notify_emit_sync */

/* The gated completion: runs in place of the proc's own completion handler
 * (swapped in by gate_install at dispatch), on the request's owning thread.
 * On success it delivers per-name sync events to the affected directories'
 * sync watchers and parks the real completion until they ack. */
static void
chimera_vfs_notify_gate_completion(struct chimera_vfs_request *request)
{
    struct chimera_vfs_notify      *notify = request->thread->vfs->vfs_notify;
    struct chimera_vfs_notify_gate *gate;
    const void                     *origin =
        request->cred ? request->cred->origin : NULL;

    /* Restore the real completion first: every exit below runs it, whether
     * inline (no watchers / failure) or via the gate resume path.  The
     * in-flight mark taken at install is cleared by gate_fire (the mark
     * must outlive the gate wait) or explicitly on the ungated exits. */
    request->complete = request->notify_saved_complete;

    if (request->status != CHIMERA_VFS_OK) {
        chimera_vfs_notify_gate_mark(notify, request, -1);
        request->complete(request);
        return;
    }

    gate = calloc(1, sizeof(*gate));
    if (!gate) {
        chimera_vfs_notify_gate_mark(notify, request, -1);
        request->complete(request);
        return;
    }

    gate->pending  = 1; /* the arm hold, released below */
    gate->refs     = 1;
    gate->request  = request;
    gate->deadline = chimera_vfs_now_ticks() +
        chimera_vfs_ns_to_ticks((uint64_t) CHIMERA_VFS_NOTIFY_GATE_TIMEOUT_MS *
                                1000000ULL);

    request->notify_gate = gate;

    pthread_mutex_lock(&notify->gates_lock);
    DL_APPEND(notify->gates, gate);
    pthread_mutex_unlock(&notify->gates_lock);

    switch (request->opcode) {
        case CHIMERA_VFS_OP_REMOVE_AT:
            if (!request->remove_at.r_unmatched) {
                uint32_t action = CHIMERA_VFS_NOTIFY_FILE_REMOVED;
                if ((request->remove_at.r_removed_attr.va_set_mask &
                     CHIMERA_VFS_ATTR_MODE) &&
                    S_ISDIR(request->remove_at.r_removed_attr.va_mode)) {
                    action = CHIMERA_VFS_NOTIFY_DIR_REMOVED;
                }
                chimera_vfs_notify_emit_sync(notify, gate, origin,
                                             request->remove_at.handle->fh,
                                             request->remove_at.handle->fh_len,
                                             action,
                                             request->remove_at.name,
                                             request->remove_at.namelen,
                                             NULL, 0);
            }
            break;
        case CHIMERA_VFS_OP_RENAME_AT:
        {
            int cross_dir = (request->fh_len != request->rename_at.new_fhlen) ||
                memcmp(request->fh, request->rename_at.new_fh,
                       request->fh_len) != 0;

            if (!cross_dir) {
                chimera_vfs_notify_emit_sync(notify, gate, origin,
                                             request->fh, request->fh_len,
                                             CHIMERA_VFS_NOTIFY_RENAMED,
                                             request->rename_at.new_name,
                                             request->rename_at.new_namelen,
                                             request->rename_at.name,
                                             request->rename_at.namelen);
            } else {
                chimera_vfs_notify_emit_sync(notify, gate, origin,
                                             request->fh, request->fh_len,
                                             CHIMERA_VFS_NOTIFY_RENAMED,
                                             NULL, 0,
                                             request->rename_at.name,
                                             request->rename_at.namelen);
                chimera_vfs_notify_emit_sync(notify, gate, origin,
                                             request->rename_at.new_fh,
                                             request->rename_at.new_fhlen,
                                             CHIMERA_VFS_NOTIFY_RENAMED,
                                             request->rename_at.new_name,
                                             request->rename_at.new_namelen,
                                             NULL, 0);
            }
            break;
        }
        case CHIMERA_VFS_OP_LINK_AT:
            chimera_vfs_notify_emit_sync(notify, gate, origin,
                                         request->link_at.dir_fh,
                                         request->link_at.dir_fhlen,
                                         CHIMERA_VFS_NOTIFY_FILE_ADDED,
                                         request->link_at.name,
                                         request->link_at.namelen,
                                         NULL, 0);
            break;
        case CHIMERA_VFS_OP_MKDIR_AT:
            chimera_vfs_notify_emit_sync(notify, gate, origin,
                                         request->mkdir_at.handle->fh,
                                         request->mkdir_at.handle->fh_len,
                                         CHIMERA_VFS_NOTIFY_DIR_ADDED,
                                         request->mkdir_at.name,
                                         request->mkdir_at.name_len,
                                         NULL, 0);
            break;
        case CHIMERA_VFS_OP_MKNOD_AT:
            chimera_vfs_notify_emit_sync(notify, gate, origin,
                                         request->mknod_at.handle->fh,
                                         request->mknod_at.handle->fh_len,
                                         CHIMERA_VFS_NOTIFY_FILE_ADDED,
                                         request->mknod_at.name,
                                         request->mknod_at.name_len,
                                         NULL, 0);
            break;
        case CHIMERA_VFS_OP_SYMLINK_AT:
            chimera_vfs_notify_emit_sync(notify, gate, origin,
                                         request->fh, request->fh_len,
                                         CHIMERA_VFS_NOTIFY_FILE_ADDED,
                                         request->symlink_at.name,
                                         request->symlink_at.namelen,
                                         NULL, 0);
            break;
        case CHIMERA_VFS_OP_OPEN_AT:
            if (request->open_at.r_created &&
                !(request->open_at.flags & CHIMERA_VFS_OPEN_NO_NOTIFY)) {
                chimera_vfs_notify_emit_sync(notify, gate, origin,
                                             request->open_at.handle->fh,
                                             request->open_at.handle->fh_len,
                                             CHIMERA_VFS_NOTIFY_FILE_ADDED,
                                             request->open_at.name,
                                             request->open_at.namelen,
                                             NULL, 0);
            }
            break;
        default:
            break;
    } /* switch */

    /* Release the arm hold; if nothing was delivered (or everything acked
     * already) the fire is ours and the completion proceeds inline. */
    if (__atomic_sub_fetch(&gate->pending, 1, __ATOMIC_ACQ_REL) == 0) {
        struct chimera_vfs_request *fired =
            chimera_vfs_notify_gate_fire(notify, gate);
        if (fired) {
            fired->complete(fired);
        }
    }
} /* chimera_vfs_notify_gate_completion */

SYMBOL_EXPORT void
chimera_vfs_notify_gate_install(struct chimera_vfs_request *request)
{
    struct chimera_vfs_notify *notify = request->thread->vfs->vfs_notify;

    if (!notify ||
        __atomic_load_n(&notify->num_sync_watches, __ATOMIC_RELAXED) == 0) {
        return;
    }

    if (request->notify_gate_wrapped) {
        return;
    }

    switch (request->opcode) {
        case CHIMERA_VFS_OP_REMOVE_AT:
        case CHIMERA_VFS_OP_RENAME_AT:
        case CHIMERA_VFS_OP_LINK_AT:
        case CHIMERA_VFS_OP_MKDIR_AT:
        case CHIMERA_VFS_OP_MKNOD_AT:
        case CHIMERA_VFS_OP_SYMLINK_AT:
        case CHIMERA_VFS_OP_OPEN_AT:
            break;
        default:
            return;
    } /* switch */

    request->notify_gate_wrapped   = 1;
    request->notify_saved_complete = request->complete;
    request->complete              = chimera_vfs_notify_gate_completion;

    /* Mark the origin's own watches on the affected directories for the
     * request's lifetime (cleared in the completion wrapper). */
    chimera_vfs_notify_gate_mark(notify, request, 1);
} /* chimera_vfs_notify_gate_install */
