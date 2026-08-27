// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Kernel-cache coherence: make the FUSE kernel's caches track what the
 * other protocols do to the same files.
 *
 * Two mechanisms, one per kernel cache:
 *
 *  - Every open regular file holds a per-(mount, file) FUSE_GRANT claim
 *    (the claim core's revocable kernel read-cache construct, declined on
 *    contention rather than breaking a peer).  A write, setattr, remove,
 *    rename, or link from any other protocol -- or another FUSE mount --
 *    breaks it, and the break becomes a FUSE_NOTIFY_INVAL_INODE that drops
 *    the kernel's cached attributes and pages.  The mount's own I/O runs
 *    under the same owner identity (read_owned/write_owned), so it never
 *    invalidates its own kernel cache.
 *
 *  - Every directory the kernel holds dentries under gets a change-notify
 *    watch; namespace events (created/removed/renamed by any protocol)
 *    become FUSE_NOTIFY_INVAL_ENTRY / NOTIFY_DELETE for the exact names.
 *
 * Both notification writes can block inside the kernel until conflicting
 * page-cache activity settles, and the event-loop thread they would block
 * could be the very thread that must complete that activity.  So a
 * dedicated notifier thread owns every write to /dev/fuse that is not a
 * request reply; break callbacks and watch callbacks only enqueue.
 *
 * Locking: mount->grant_lock guards the grant and dirwatch tables and may
 * be held across claim-core/vfs_notify calls (it is never taken inside
 * them).  Break/revoke callbacks fire from arbitrary threads inside the
 * claim core and touch ONLY atomics + the notifier queue -- never
 * grant_lock -- which is what makes two mounts breaking each other's
 * grants deadlock-free.  The notifier's queue reference is counted in
 * grant->refcount, so a grant mid-break outlives its last RELEASE.
 */

#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>
#include <sys/uio.h>

#include "fuse_internal.h"
#include "vfs/vfs_claim.h"
#include "vfs/vfs_notify.h"

/* ------------------------------------------------------------------ */
/* Notifier thread                                                     */
/* ------------------------------------------------------------------ */

static void
chimera_fuse_notice_post(
    struct chimera_fuse_shared *shared,
    struct chimera_fuse_notice *notice)
{
    pthread_mutex_lock(&shared->notifier_lock);
    LL_APPEND(shared->notices, notice);
    pthread_cond_signal(&shared->notifier_cond);
    pthread_mutex_unlock(&shared->notifier_lock);
} /* chimera_fuse_notice_post */

/* One non-reply write to the mount's primary channel.  ENOENT means the
 * kernel no longer holds what we invalidated; ENODEV means the mount is
 * gone.  Both are fine. */
static void
chimera_fuse_notify_write(
    struct chimera_fuse_mount *mount,
    int                        code,
    const void                *arg,
    size_t                     arglen,
    const char                *name,
    size_t                     namelen)
{
    struct fuse_out_header hdr;
    struct iovec           iov[4];
    int                    niov = 0;
    char                   nul  = 0;
    ssize_t                rc;

    if (!mount->mounted || mount->dead) {
        return;
    }

    hdr.error  = code;
    hdr.unique = 0;
    hdr.len    = sizeof(hdr) + arglen;

    iov[niov].iov_base = &hdr;
    iov[niov].iov_len  = sizeof(hdr);
    niov++;

    iov[niov].iov_base = (void *) arg;
    iov[niov].iov_len  = arglen;
    niov++;

    if (name) {
        /* The kernel requires the name NUL-terminated on the wire. */
        iov[niov].iov_base = (void *) name;
        iov[niov].iov_len  = namelen;
        niov++;
        iov[niov].iov_base = &nul;
        iov[niov].iov_len  = 1;
        niov++;
        hdr.len += namelen + 1;
    }

    do {
        rc = writev(mount->channel_fds[0], iov, niov);
    } while (rc < 0 && errno == EINTR);

    if (rc < 0 && errno != ENOENT && errno != ENODEV && errno != ENOTCONN) {
        chimera_fuse_debug("fuse notify (code %d) failed: %s",
                           code, strerror(errno));
    }
} /* chimera_fuse_notify_write */

static void
chimera_fuse_inval_inode(
    struct chimera_fuse_mount *mount,
    uint64_t                   nodeid)
{
    struct fuse_notify_inval_inode_out out;

    memset(&out, 0, sizeof(out));
    out.ino = nodeid;
    out.off = 0;
    out.len = -1; /* whole file: attributes and every cached page */

    chimera_fuse_notify_write(mount, FUSE_NOTIFY_INVAL_INODE,
                              &out, sizeof(out), NULL, 0);
} /* chimera_fuse_inval_inode */

/* Attribute-only invalidation (off = -1): the kernel drops its cached
 * attributes but its page cache is left to AUTO_INVAL_DATA -- the next
 * cached read revalidates the (now invalid) attributes and the kernel
 * discards stale pages itself when mtime/size moved.  This is the ONLY
 * invalidation a grant break may use: a full-page invalidation blocks
 * inside the kernel on any in-flight write's page locks, and that write
 * may itself be parked on the very break being resolved (the cross-mount
 * write/write cycle) -- with the ack pipeline wedged behind it. */
static void
chimera_fuse_inval_attrs(
    struct chimera_fuse_mount *mount,
    uint64_t                   nodeid)
{
    struct fuse_notify_inval_inode_out out;

    memset(&out, 0, sizeof(out));
    out.ino = nodeid;
    out.off = -1;
    out.len = 0;

    chimera_fuse_notify_write(mount, FUSE_NOTIFY_INVAL_INODE,
                              &out, sizeof(out), NULL, 0);
} /* chimera_fuse_inval_attrs */

static void
chimera_fuse_inval_entry(
    struct chimera_fuse_mount *mount,
    uint64_t                   parent,
    const char                *name,
    size_t                     namelen)
{
    struct fuse_notify_inval_entry_out out;

    if (namelen == 0 || namelen >= CHIMERA_VFS_NAME_MAX) {
        return;
    }

    memset(&out, 0, sizeof(out));
    out.parent  = parent;
    out.namelen = namelen;

    chimera_fuse_notify_write(mount, FUSE_NOTIFY_INVAL_ENTRY,
                              &out, sizeof(out), name, namelen);
} /* chimera_fuse_inval_entry */

/* Resolve a broken grant: invalidate the kernel's view of the file, settle
 * the claim with the claim core, and drop the notifier's reference. */
static void
chimera_fuse_grant_resolve(
    struct chimera_fuse_shared *shared,
    struct chimera_fuse_grant  *grant)
{
    struct chimera_vfs_state  *state      = shared->vfs->vfs_state;
    struct chimera_fuse_mount *mount      = grant->mount;
    int                        free_grant = 0;

    chimera_fuse_inval_attrs(mount, grant->nodeid);

    if (!atomic_load(&grant->revoked)) {
        chimera_vfs_claim_ack(&grant->claim, 0);
    }

    chimera_vfs_claim_release(state, grant->file_state, &grant->claim);

    atomic_store(&grant->state, CHIMERA_FUSE_GRANT_BROKEN);

    pthread_mutex_lock(&mount->grant_lock);

    if (atomic_fetch_sub(&grant->refcount, 1) == 1) {
        HASH_DELETE(hh, mount->grants, grant);
        free_grant = 1;
    }

    pthread_mutex_unlock(&mount->grant_lock);

    if (free_grant) {
        chimera_vfs_state_put(state, grant->file_state);
        free(grant);
    }
} /* chimera_fuse_grant_resolve */

/* Drain one directory watch into per-name entry invalidations.  Sync
 * events (a gated mutation's completion is parked on our ack) drain
 * first: their invalidations must be on the wire -- including the
 * directory's own attribute refresh -- before the ack releases the
 * mutating caller. */
static void
chimera_fuse_dirwatch_drain(
    struct chimera_fuse_shared *shared,
    struct chimera_fuse_mount  *mount,
    uint64_t                    nodeid)
{
    struct chimera_vfs_notify            *notify = shared->vfs->vfs_notify;
    struct chimera_fuse_dirwatch         *dw;
    struct chimera_vfs_notify_event       events[16];
    struct chimera_vfs_notify_sync_event *sev, *sev_next;
    int                                   nevents, overflowed, i;
    int                                   touched = 0;

    pthread_mutex_lock(&mount->grant_lock);

    HASH_FIND(hh, mount->dirwatches, &nodeid, sizeof(nodeid), dw);

    if (!dw || !dw->watch) {
        pthread_mutex_unlock(&mount->grant_lock);
        return;
    }

    /* Re-arm BEFORE draining: an event landing after the drain must
     * queue a fresh notice or it would sit in the queue forever. */
    pthread_mutex_lock(&mount->dir_notifier_lock);
    dw->queued = 0;
    pthread_mutex_unlock(&mount->dir_notifier_lock);

    sev = chimera_vfs_notify_drain_sync(dw->watch);

    pthread_mutex_unlock(&mount->grant_lock);

    if (sev) {
        touched = 1;

        for (sev_next = sev; sev_next; sev_next = sev_next->next) {
            chimera_fuse_inval_entry(mount, nodeid,
                                     sev_next->name, sev_next->name_len);
            if (sev_next->old_name_len) {
                chimera_fuse_inval_entry(mount, nodeid,
                                         sev_next->old_name,
                                         sev_next->old_name_len);
            }
        }

        /* Refresh the directory's own attributes before acking: the gated
         * caller may stat the directory the moment its op returns. */
        chimera_fuse_inval_inode(mount, nodeid);

        while (sev) {
            sev_next = sev->next;
            chimera_vfs_notify_gate_ack(notify, sev);
            sev = sev_next;
        }
    }

    for (;;) {
        pthread_mutex_lock(&mount->grant_lock);

        HASH_FIND(hh, mount->dirwatches, &nodeid, sizeof(nodeid), dw);

        if (!dw || !dw->watch) {
            pthread_mutex_unlock(&mount->grant_lock);
            return;
        }

        nevents = chimera_vfs_notify_drain(dw->watch, events, 16, &overflowed);

        pthread_mutex_unlock(&mount->grant_lock);

        if (nevents > 0 || overflowed) {
            touched = 1;
        }

        for (i = 0; i < nevents; i++) {
            chimera_fuse_inval_entry(mount, nodeid,
                                     events[i].name, events[i].name_len);

            if (events[i].action & CHIMERA_VFS_NOTIFY_RENAMED &&
                events[i].old_name_len) {
                chimera_fuse_inval_entry(mount, nodeid,
                                         events[i].old_name,
                                         events[i].old_name_len);
            }
        }

        /* An overflowed ring lost names; the kernel's entry timeout is the
         * backstop for whatever was dropped. */

        if (nevents < 16) {
            break;
        }
    }

    /* The namespace change moved the directory's own mtime/size, and an
    * overflowed ring may have dropped names the entry timeout must now
    * cover -- either way, refresh the directory's cached attributes. */
    if (touched) {
        chimera_fuse_inval_inode(mount, nodeid);
    }
} /* chimera_fuse_dirwatch_drain */

static void *
chimera_fuse_notifier(void *arg)
{
    struct chimera_fuse_shared *shared = arg;
    struct chimera_fuse_notice *notice;

    pthread_mutex_lock(&shared->notifier_lock);

    for (;;) {
        while (!shared->notices && !shared->notifier_stop) {
            pthread_cond_wait(&shared->notifier_cond, &shared->notifier_lock);
        }

        if (!shared->notices) {
            /* Stopping, queue fully drained. */
            break;
        }

        notice          = shared->notices;
        shared->notices = notice->next;

        pthread_mutex_unlock(&shared->notifier_lock);

        switch (notice->type) {
            case CHIMERA_FUSE_NOTICE_INVAL_FILE:
                chimera_fuse_grant_resolve(shared, notice->grant);
                break;
            case CHIMERA_FUSE_NOTICE_DIR_EVENTS:
                /* Directory events run on the owning mount's own thread. */
                break;
        } /* switch */

        free(notice);

        pthread_mutex_lock(&shared->notifier_lock);
    }

    pthread_mutex_unlock(&shared->notifier_lock);

    return NULL;
} /* chimera_fuse_notifier */

/* Per-mount directory-event notifier.  An INVAL_ENTRY into this mount's
 * kernel can block on a directory lock held by one of this mount's own
 * in-flight namespace syscalls (which completes as soon as its server-side
 * acks flow).  Running each mount's directory events on the mount's own
 * thread keeps such a transient block from starving any other mount's
 * acks -- or the grant lane on the shared notifier, whose attribute-only
 * invalidations never block at all.  That lane separation is what keeps a
 * parked operation's ack pipeline live while an entry invalidation waits
 * out a kernel lock. */
static void *
chimera_fuse_dir_notifier(void *arg)
{
    struct chimera_fuse_mount  *mount = arg;
    struct chimera_fuse_notice *notice;

    pthread_mutex_lock(&mount->dir_notifier_lock);

    for (;;) {
        while (!mount->dir_notices && !mount->dir_notifier_stop) {
            pthread_cond_wait(&mount->dir_notifier_cond,
                              &mount->dir_notifier_lock);
        }

        if (!mount->dir_notices) {
            break; /* stopping, queue fully drained */
        }

        notice             = mount->dir_notices;
        mount->dir_notices = notice->next;

        pthread_mutex_unlock(&mount->dir_notifier_lock);

        chimera_fuse_dirwatch_drain(mount->shared, mount, notice->nodeid);

        free(notice);

        pthread_mutex_lock(&mount->dir_notifier_lock);
    }

    pthread_mutex_unlock(&mount->dir_notifier_lock);

    return NULL;
} /* chimera_fuse_dir_notifier */

void
chimera_fuse_notifier_start(struct chimera_fuse_shared *shared)
{
    int m;

    shared->notifier_stop = 0;
    pthread_create(&shared->notifier, NULL, chimera_fuse_notifier, shared);
    shared->notifier_running = 1;

    for (m = 0; m < shared->num_mounts; m++) {
        struct chimera_fuse_mount *mount = &shared->mounts[m];

        mount->dir_notifier_stop = 0;
        pthread_create(&mount->dir_notifier, NULL,
                       chimera_fuse_dir_notifier, mount);
        mount->dir_notifier_running = 1;
    }
} /* chimera_fuse_notifier_start */

void
chimera_fuse_notifier_stop(struct chimera_fuse_shared *shared)
{
    int m;

    for (m = 0; m < shared->num_mounts; m++) {
        struct chimera_fuse_mount *mount = &shared->mounts[m];

        if (!mount->dir_notifier_running) {
            continue;
        }

        pthread_mutex_lock(&mount->dir_notifier_lock);
        mount->dir_notifier_stop = 1;
        pthread_cond_signal(&mount->dir_notifier_cond);
        pthread_mutex_unlock(&mount->dir_notifier_lock);

        pthread_join(mount->dir_notifier, NULL);

        mount->dir_notifier_running = 0;
    }

    if (!shared->notifier_running) {
        return;
    }

    pthread_mutex_lock(&shared->notifier_lock);
    shared->notifier_stop = 1;
    pthread_cond_signal(&shared->notifier_cond);
    pthread_mutex_unlock(&shared->notifier_lock);

    pthread_join(shared->notifier, NULL);

    shared->notifier_running = 0;
} /* chimera_fuse_notifier_stop */

/* ------------------------------------------------------------------ */
/* Invalidation grants                                                 */
/* ------------------------------------------------------------------ */

/* Fires on the breaking protocol's thread, inside the claim core: atomics
 * and the notifier queue only. */
static void
chimera_fuse_grant_break_common(
    struct chimera_fuse_grant *grant,
    int                        revoked)
{
    struct chimera_fuse_notice *notice;
    int                         expected = CHIMERA_FUSE_GRANT_ACTIVE;

    if (revoked) {
        atomic_store(&grant->revoked, 1);
    }

    if (!atomic_compare_exchange_strong(&grant->state, &expected,
                                        CHIMERA_FUSE_GRANT_BREAKING)) {
        /* Already breaking (or broken): the queued resolution covers it. */
        return;
    }

    atomic_fetch_add(&grant->refcount, 1);

    notice = calloc(1, sizeof(*notice));

    notice->type  = CHIMERA_FUSE_NOTICE_INVAL_FILE;
    notice->mount = grant->mount;
    notice->grant = grant;

    chimera_fuse_notice_post(grant->mount->shared, notice);
} /* chimera_fuse_grant_break_common */

static void
chimera_fuse_grant_break_cb(
    struct chimera_vfs_claim *claim,
    uint8_t                   needed_mode,
    void                     *private_data)
{
    chimera_fuse_grant_break_common(private_data, 0);
} /* chimera_fuse_grant_break_cb */

static void
chimera_fuse_grant_revoked_cb(
    struct chimera_vfs_claim *claim,
    void                     *private_data)
{
    chimera_fuse_grant_break_common(private_data, 1);
} /* chimera_fuse_grant_revoked_cb */

/* Insert (or re-insert) the grant's claim.  Caller holds grant_lock. */
static int
chimera_fuse_grant_arm(
    struct chimera_vfs_state  *state,
    struct chimera_fuse_grant *grant)
{
    struct chimera_claim_owner    owner;
    enum chimera_vfs_claim_result result;

    /* CHIMERA_CONSTRUCT_FUSE_GRANT carries the DELEG_R-shaped deny rows and
     * the awaited-class break semantics coherence=sync needs: a conflicting
     * writer parks until this grant's break is ACKED (its advertised mode
     * survives break-begin), so "the write returned" implies "this kernel's
     * cache is gone".  Unlike a delegation it stays sweep-revocable at the
     * break deadline, which bounds the wait if the notifier ever wedges. */
    chimera_fuse_grant_owner(&owner, grant->mount, grant->fh_hash);

    chimera_vfs_claim_init_fuse_grant(&grant->claim, &owner);

    grant->claim.break_cb   = chimera_fuse_grant_break_cb;
    grant->claim.revoked_cb = chimera_fuse_grant_revoked_cb;
    grant->claim.cb_private = grant;

    atomic_store(&grant->revoked, 0);

    result = chimera_vfs_claim_try_acquire(state, grant->file_state,
                                           &grant->claim, NULL);

    if (result != CHIMERA_CLAIM_GRANTED) {
        /* Contention: decline to cache-track, like a declined delegation.
         * The kernel's attr/entry timeouts remain the backstop. */
        return -1;
    }

    atomic_store(&grant->state, CHIMERA_FUSE_GRANT_ACTIVE);

    return 0;
} /* chimera_fuse_grant_arm */

int
chimera_fuse_grant_ensure(
    struct chimera_fuse_thread *thread,
    struct chimera_fuse_mount  *mount,
    uint64_t                    nodeid,
    const uint8_t              *fh,
    uint32_t                    fh_len,
    uint64_t                    fh_hash)
{
    struct chimera_vfs_state  *state = thread->vfs_thread->vfs->vfs_state;
    struct chimera_fuse_grant *grant;
    int                        rc;

    pthread_mutex_lock(&mount->grant_lock);

    HASH_FIND(hh, mount->grants, &nodeid, sizeof(nodeid), grant);

    if (grant) {
        if (atomic_load(&grant->state) == CHIMERA_FUSE_GRANT_ACTIVE) {
            pthread_mutex_unlock(&mount->grant_lock);
            return CHIMERA_FUSE_COVER_HELD;
        }

        rc = CHIMERA_FUSE_COVER_NONE;
        if (atomic_load(&grant->state) == CHIMERA_FUSE_GRANT_BROKEN &&
            chimera_fuse_grant_arm(state, grant) == 0) {
            /* Re-armed a previously broken grant for this fresh touch. */
            rc = CHIMERA_FUSE_COVER_FRESH;
        }

        pthread_mutex_unlock(&mount->grant_lock);
        return rc;
    }

    grant = calloc(1, sizeof(*grant));

    grant->mount   = mount;
    grant->nodeid  = nodeid;
    grant->fh_hash = fh_hash;
    grant->fh_len  = fh_len;
    memcpy(grant->fh, fh, fh_len);
    /* The single long-lived reference belongs to the nodeid: it drops when
     * the kernel FORGETs the node and its caches are gone with it. */
    atomic_store(&grant->refcount, 1);
    atomic_store(&grant->state, CHIMERA_FUSE_GRANT_BROKEN);

    grant->file_state = chimera_vfs_state_get(state, grant->fh, grant->fh_len,
                                              grant->fh_hash, true);

    if (!grant->file_state) {
        pthread_mutex_unlock(&mount->grant_lock);
        free(grant);
        return CHIMERA_FUSE_COVER_NONE;
    }

    if (chimera_fuse_grant_arm(state, grant) != 0) {
        pthread_mutex_unlock(&mount->grant_lock);
        chimera_vfs_state_put(state, grant->file_state);
        free(grant);
        return CHIMERA_FUSE_COVER_NONE;
    }

    HASH_ADD(hh, mount->grants, nodeid, sizeof(grant->nodeid), grant);

    pthread_mutex_unlock(&mount->grant_lock);

    return CHIMERA_FUSE_COVER_FRESH;
} /* chimera_fuse_grant_ensure */

int
chimera_fuse_cover_touch(
    struct chimera_fuse_thread *thread,
    struct chimera_fuse_mount  *mount,
    uint64_t                    nodeid,
    const uint8_t              *fh,
    uint32_t                    fh_len)
{
    struct chimera_fuse_dirwatch *dw;
    struct chimera_fuse_grant    *grant;
    int                           have_grant;

    pthread_mutex_lock(&mount->grant_lock);
    HASH_FIND(hh, mount->grants, &nodeid, sizeof(nodeid), grant);
    have_grant = grant != NULL;
    if (!have_grant) {
        HASH_FIND(hh, mount->dirwatches, &nodeid, sizeof(nodeid), dw);
        if (dw) {
            pthread_mutex_unlock(&mount->grant_lock);
            return CHIMERA_FUSE_COVER_HELD;
        }
    }
    pthread_mutex_unlock(&mount->grant_lock);

    if (have_grant) {
        /* Known regular file: re-arm through the normal path (drops and
         * retakes grant_lock; the grant cannot vanish -- forgets come from
         * the kernel, which is mid-request on this node). */
        return chimera_fuse_grant_ensure(thread, mount, nodeid, fh, fh_len,
                                         chimera_fuse_fh_hash(fh, fh_len));
    }

    /* First touch: the node's type is unknown until the backend replies, so
     * coverage cannot begin yet.  The completion path arms it for next
     * time. */
    return CHIMERA_FUSE_COVER_NONE;
} /* chimera_fuse_cover_touch */

int
chimera_fuse_grant_active(
    struct chimera_fuse_mount *mount,
    uint64_t                   nodeid)
{
    struct chimera_fuse_grant *grant;
    int                        active;

    pthread_mutex_lock(&mount->grant_lock);
    HASH_FIND(hh, mount->grants, &nodeid, sizeof(nodeid), grant);
    active = grant && atomic_load(&grant->state) == CHIMERA_FUSE_GRANT_ACTIVE;
    pthread_mutex_unlock(&mount->grant_lock);

    return active;
} /* chimera_fuse_grant_active */

int
chimera_fuse_grant_open(
    struct chimera_fuse_thread     *thread,
    struct chimera_fuse_mount      *mount,
    uint64_t                        nodeid,
    struct chimera_vfs_open_handle *oh)
{
    return chimera_fuse_grant_ensure(thread, mount, nodeid,
                                     oh->fh, oh->fh_len, oh->fh_hash);
} /* chimera_fuse_grant_open */

void
chimera_fuse_grant_forget(
    struct chimera_fuse_mount *mount,
    struct chimera_vfs_state  *state,
    uint64_t                   nodeid)
{
    struct chimera_fuse_grant *grant;
    int                        free_grant = 0;

    pthread_mutex_lock(&mount->grant_lock);

    HASH_FIND(hh, mount->grants, &nodeid, sizeof(nodeid), grant);

    if (!grant) {
        pthread_mutex_unlock(&mount->grant_lock);
        return;
    }

    if (atomic_fetch_sub(&grant->refcount, 1) == 1) {
        /* A BREAKING grant cannot get here: the notifier holds its own
         * reference until it resolves. */
        if (atomic_load(&grant->state) == CHIMERA_FUSE_GRANT_ACTIVE) {
            chimera_vfs_claim_release(state, grant->file_state, &grant->claim);
        }

        HASH_DELETE(hh, mount->grants, grant);
        free_grant = 1;
    }

    pthread_mutex_unlock(&mount->grant_lock);

    if (free_grant) {
        chimera_vfs_state_put(state, grant->file_state);
        free(grant);
    }
} /* chimera_fuse_grant_forget */

/* ------------------------------------------------------------------ */
/* Directory watches                                                   */
/* ------------------------------------------------------------------ */

#define CHIMERA_FUSE_WATCH_MASK ( \
            CHIMERA_VFS_NOTIFY_FILE_ADDED | \
            CHIMERA_VFS_NOTIFY_FILE_REMOVED | \
            CHIMERA_VFS_NOTIFY_DIR_ADDED | \
            CHIMERA_VFS_NOTIFY_DIR_REMOVED | \
            CHIMERA_VFS_NOTIFY_RENAMED)

/* From any thread, under the notify bucket lock: enqueue-only, onto the
 * owning mount's directory-event thread. */
static void
chimera_fuse_dirwatch_cb(
    struct chimera_vfs_notify_watch *watch,
    void                            *private_data)
{
    struct chimera_fuse_dirwatch *dw    = private_data;
    struct chimera_fuse_mount    *mount = dw->mount;
    struct chimera_fuse_notice   *notice;

    pthread_mutex_lock(&mount->dir_notifier_lock);

    if (dw->queued) {
        pthread_mutex_unlock(&mount->dir_notifier_lock);
        return;
    }

    dw->queued = 1;

    notice = calloc(1, sizeof(*notice));

    notice->type   = CHIMERA_FUSE_NOTICE_DIR_EVENTS;
    notice->mount  = mount;
    notice->nodeid = dw->nodeid;

    LL_APPEND(mount->dir_notices, notice);
    pthread_cond_signal(&mount->dir_notifier_cond);

    pthread_mutex_unlock(&mount->dir_notifier_lock);
} /* chimera_fuse_dirwatch_cb */

int
chimera_fuse_watch_dir(
    struct chimera_fuse_thread *thread,
    struct chimera_fuse_mount  *mount,
    uint64_t                    nodeid,
    const uint8_t              *fh,
    uint32_t                    fh_len)
{
    struct chimera_vfs_notify    *notify = mount->shared->vfs->vfs_notify;
    struct chimera_fuse_dirwatch *dw;

    (void) thread;

    pthread_mutex_lock(&mount->grant_lock);

    HASH_FIND(hh, mount->dirwatches, &nodeid, sizeof(nodeid), dw);

    if (dw) {
        pthread_mutex_unlock(&mount->grant_lock);
        return CHIMERA_FUSE_COVER_HELD;
    }

    dw = calloc(1, sizeof(*dw));

    dw->nodeid = nodeid;
    dw->mount  = mount;

    dw->watch = chimera_vfs_notify_watch_create(notify, fh, fh_len,
                                                CHIMERA_FUSE_WATCH_MASK,
                                                0,
                                                chimera_fuse_dirwatch_cb, dw);

    if (!dw->watch) {
        pthread_mutex_unlock(&mount->grant_lock);
        free(dw);
        return CHIMERA_FUSE_COVER_NONE;
    }

    /* coherence=sync: namespace mutations under this directory gate their
     * completion on our invalidation ack.  The mount is the origin token, so
     * this kernel's own mutations (stamped in chimera_fuse_map_cred) never
     * gate on themselves. */
    if (mount->coherence_sync) {
        chimera_vfs_notify_watch_set_sync(notify, dw->watch, mount);
    }

    HASH_ADD(hh, mount->dirwatches, nodeid, sizeof(dw->nodeid), dw);

    pthread_mutex_unlock(&mount->grant_lock);

    return CHIMERA_FUSE_COVER_FRESH;
} /* chimera_fuse_watch_dir */

void
chimera_fuse_watch_forget(
    struct chimera_fuse_mount *mount,
    struct chimera_vfs        *vfs,
    uint64_t                   nodeid)
{
    struct chimera_fuse_dirwatch *dw;

    pthread_mutex_lock(&mount->grant_lock);

    HASH_FIND(hh, mount->dirwatches, &nodeid, sizeof(nodeid), dw);

    if (!dw) {
        pthread_mutex_unlock(&mount->grant_lock);
        return;
    }

    HASH_DELETE(hh, mount->dirwatches, dw);

    /* Destroy under grant_lock: the watch callback (which references dw)
     * runs under the notify bucket lock that destroy also serializes on,
     * so after this returns no callback can touch dw again. */
    chimera_vfs_notify_watch_destroy(vfs->vfs_notify, dw->watch);

    pthread_mutex_unlock(&mount->grant_lock);

    free(dw);
} /* chimera_fuse_watch_forget */

/* ------------------------------------------------------------------ */
/* Teardown                                                            */
/* ------------------------------------------------------------------ */

/* Runs at protocol destroy(), after the thread pool is gone: force-drop
 * whatever the sweeps could not (grants whose break was queued after the
 * notifier drained, watches for directories never forgotten). */
void
chimera_fuse_coherence_shutdown(
    struct chimera_fuse_shared *shared,
    struct chimera_fuse_mount  *mount)
{
    struct chimera_fuse_grant    *grant, *gtmp;
    struct chimera_fuse_dirwatch *dw, *dwtmp;

#ifndef __clang_analyzer__
    /* uthash blows clangs mind.  `state` lives inside the guard too: with the
     * body compiled out the analyser sees only a dead initialisation. */
    struct chimera_vfs_state     *state = shared->vfs->vfs_state;

    HASH_ITER(hh, mount->dirwatches, dw, dwtmp)
    {
        HASH_DELETE(hh, mount->dirwatches, dw);
        chimera_vfs_notify_watch_destroy(shared->vfs->vfs_notify, dw->watch);
        free(dw);
    }

    HASH_ITER(hh, mount->grants, grant, gtmp)
    {
        HASH_DELETE(hh, mount->grants, grant);

        if (atomic_load(&grant->state) == CHIMERA_FUSE_GRANT_ACTIVE) {
            chimera_vfs_claim_release(state, grant->file_state, &grant->claim);
        }

        chimera_vfs_state_put(state, grant->file_state);
        free(grant);
    }
#endif /* ifndef __clang_analyzer__ */
} /* chimera_fuse_coherence_shutdown */
