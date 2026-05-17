// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include <stdlib.h>
#include <unistd.h>

#include "vfs_notify.h"
#include "vfs_internal.h"
#include "vfs_rpl_cache.h"
#include "vfs_mount_table.h"
#include "vfs/vfs_procs.h"
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
            urcu_memb_read_lock();
            mount = chimera_vfs_mount_table_lookup(notify->vfs->mount_table, mount_id);
            if (mount) {
                memcpy(me->root_fh, mount->root_fh, mount->root_fh_len);
                me->root_fh_len = mount->root_fh_len;
                if (mount->module) {
                    me->has_rpl = (mount->module->capabilities & CHIMERA_VFS_CAP_RPL) != 0;
                }
            }
            urcu_memb_read_unlock();
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

    /* Cache the result for future events */
    chimera_vfs_rpl_cache_insert(notify->rpl_cache,
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
         * The callback will continue the resolve loop. */
        chimera_vfs_getparent(notify->vfs->sync_delegation_threads[0].vfs_thread,
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

    /* Free all watches from buckets */
    for (i = 0; i < CHIMERA_VFS_NOTIFY_NUM_BUCKETS; i++) {
        watch = notify->buckets[i].watches;
        while (watch) {
            watch_tmp = watch->next;
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
     * the client to rescan via overflow semantics. */
    pthread_mutex_lock(&watch->lock);
    watch->ring_head  = 0;
    watch->ring_count = 0;
    watch->overflowed = 1;
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
         * was modified" rather than "an unknown descendant changed". */
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
} /* chimera_vfs_notify_emit */
