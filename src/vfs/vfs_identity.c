// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Asynchronous identity resolver -- see vfs_identity.h.
 *
 * A cache hit resolves synchronously on the calling thread (lock-free RCU
 * read).  A miss is queued to a small pool of worker pthreads that run the
 * registered, blocking miss handlers (NSS by default; the SMB server registers
 * a winbind handler), populate the user cache, then hand the result back to the
 * originating evpl thread by appending it to that thread's pending-identity
 * queue and ringing its doorbell.
 */

#define _GNU_SOURCE
#include <stdlib.h>
#include <string.h>
#include <pthread.h>
#include <pwd.h>
#include <grp.h>
#include <unistd.h>

#include "vfs.h"
#include "vfs_internal.h"
#include "vfs_user_cache.h"
#include "vfs_identity.h"
#include "common/macros.h"

struct chimera_vfs_identity_handler_entry {
    chimera_vfs_identity_handler               handler;
    void                                      *private_data;
    struct chimera_vfs_identity_handler_entry *next;
};

struct chimera_vfs_identity_request {
    struct chimera_vfs_identity_request *prev;
    struct chimera_vfs_identity_request *next;
    struct chimera_vfs_thread           *origin;
    enum chimera_vfs_identity_key        key;
    uint32_t                             id;
    char                                 name[CHIMERA_VFS_SID_MAX_LEN > 256 ?
                                              CHIMERA_VFS_SID_MAX_LEN : 256];
    int                                  found;
    struct chimera_vfs_user              result;
    chimera_vfs_identity_callback        callback;
    void                                *private_data;
};

struct chimera_vfs_identity {
    struct chimera_vfs                        *vfs;
    int                                        num_workers;
    pthread_t                                 *workers;
    pthread_mutex_t                            lock;
    pthread_cond_t                             cond;
    struct chimera_vfs_identity_request       *queue;
    int                                        shutdown;
    pthread_mutex_t                            handler_lock;
    struct chimera_vfs_identity_handler_entry *handlers;
};

/* Copy a cached user into a standalone result (so callbacks never hold an RCU
 * pointer). */
static inline void
chimera_vfs_identity_copy_user(
    struct chimera_vfs_user       *dst,
    const struct chimera_vfs_user *src)
{
    dst->uid   = src->uid;
    dst->gid   = src->gid;
    dst->ngids = src->ngids;
    memcpy(dst->gids, src->gids, src->ngids * sizeof(uint32_t));
    memcpy(dst->username, src->username, sizeof(dst->username));
    memcpy(dst->sid, src->sid, sizeof(dst->sid));
    dst->username_len = src->username_len;
} /* chimera_vfs_identity_copy_user */

/*
 * Synchronous cache probe.  Returns 1 and fills `out` on a hit, 0 on a miss.
 * Caller need not hold the RCU read lock -- it is taken here.
 */
static int
chimera_vfs_identity_cache_probe(
    struct chimera_vfs_user_cache *cache,
    enum chimera_vfs_identity_key  key,
    uint32_t                       id,
    const char                    *name,
    struct chimera_vfs_user       *out)
{
    const struct chimera_vfs_user *user  = NULL;
    int                            found = 0;

    urcu_qsbr_read_lock();

    switch (key) {
        case CHIMERA_VFS_IDENTITY_BY_UID:
            user = chimera_vfs_user_cache_lookup_by_uid(cache, id);
            break;
        case CHIMERA_VFS_IDENTITY_BY_NAME:
            user = chimera_vfs_user_cache_lookup_by_name(cache, name);
            break;
        case CHIMERA_VFS_IDENTITY_BY_SID:
            user = chimera_vfs_user_cache_lookup_by_sid(cache, name);
            break;
        case CHIMERA_VFS_IDENTITY_BY_GID:
        default:
            break;
    } /* switch */

    if (user) {
        chimera_vfs_identity_copy_user(out, user);
        found = 1;
    }

    urcu_qsbr_read_unlock();

    return found;
} /* chimera_vfs_identity_cache_probe */

/* Run the registered miss handlers in order until one resolves the key. */
static int
chimera_vfs_identity_run_handlers(
    struct chimera_vfs_identity         *identity,
    struct chimera_vfs_identity_request *req)
{
    struct chimera_vfs_identity_handler_entry *entry;
    int                                        rc = -1;

    pthread_mutex_lock(&identity->handler_lock);
    entry = identity->handlers;
    pthread_mutex_unlock(&identity->handler_lock);

    /* The handler list is only appended to at startup, so it is safe to walk
     * after grabbing the head. */
    while (entry) {
        memset(&req->result, 0, sizeof(req->result));
        if (entry->handler(req->key, req->id, req->name, &req->result,
                           entry->private_data) == 0) {
            rc = 0;
            break;
        }
        entry = entry->next;
    }

    return rc;
} /* chimera_vfs_identity_run_handlers */

static void *
chimera_vfs_identity_worker(void *arg)
{
    struct chimera_vfs_identity         *identity = arg;
    struct chimera_vfs_identity_request *req;

    /* Pure writer: this worker only runs miss handlers and populates the cache
     * (call_rcu + rcu_assign), never taking an RCU read lock -- the read-side
     * probe runs on the evpl threads.  So it is not registered as a QSBR
     * reader; registering it would put a thread that blocks in cond_wait and in
     * NSS/winbind resolution into the grace-period quorum and stall reclamation
     * process-wide. */
    pthread_mutex_lock(&identity->lock);

    while (1) {
        while (!identity->queue && !identity->shutdown) {
            pthread_cond_wait(&identity->cond, &identity->lock);
        }

        if (identity->shutdown && !identity->queue) {
            break;
        }

        req = identity->queue;
        DL_DELETE(identity->queue, req);

        pthread_mutex_unlock(&identity->lock);

        /* Blocking resolution happens here, off the event loop. */
        if (chimera_vfs_identity_run_handlers(identity, req) == 0) {
            req->found = 1;
            /* Populate the cache so future lookups for this identity are
             * synchronous (TTL-expiring, non-pinned). */
            chimera_vfs_user_cache_add(
                identity->vfs->vfs_user_cache,
                req->result.username[0] ? req->result.username : "",
                NULL, NULL,
                req->result.sid[0] ? req->result.sid : NULL,
                req->result.uid, req->result.gid,
                req->result.ngids, req->result.gids, 0);
        } else {
            req->found = 0;
        }

        /* Hand the completed job back to the originating evpl thread. */
        pthread_mutex_lock(&req->origin->lock);
        DL_APPEND(req->origin->pending_identity, req);
        pthread_mutex_unlock(&req->origin->lock);

        evpl_ring_doorbell(&req->origin->doorbell);

        pthread_mutex_lock(&identity->lock);
    }

    pthread_mutex_unlock(&identity->lock);

    return NULL;
} /* chimera_vfs_identity_worker */

/* ---- default NSS miss handler ------------------------------------------ */

static int
chimera_vfs_identity_nss_handler(
    enum chimera_vfs_identity_key key,
    uint32_t                      id,
    const char                   *name,
    struct chimera_vfs_user      *out,
    void                         *private_data)
{
    struct passwd pw, *res = NULL;
    char          buf[16384];
    int           rc;
    gid_t         grps[CHIMERA_VFS_CRED_MAX_GIDS];
    int           ng = CHIMERA_VFS_CRED_MAX_GIDS;
    int           i;

    (void) private_data;

    switch (key) {
        case CHIMERA_VFS_IDENTITY_BY_UID:
            rc = getpwuid_r(id, &pw, buf, sizeof(buf), &res);
            break;
        case CHIMERA_VFS_IDENTITY_BY_NAME:
            rc = getpwnam_r(name, &pw, buf, sizeof(buf), &res);
            break;
        default:
            /* NSS has no notion of a Windows SID, and a gid is not a user. */
            return -1;
    } /* switch */

    if (rc != 0 || res == NULL) {
        return -1;
    }

    out->uid = pw.pw_uid;
    out->gid = pw.pw_gid;
    strncpy(out->username, pw.pw_name, sizeof(out->username) - 1);
    out->username_len = (int) strlen(out->username);

    /* Supplementary groups (getgrouplist includes the primary gid; storing it
     * twice is harmless for membership checks). */
    if (getgrouplist(pw.pw_name, pw.pw_gid, grps, &ng) < 0) {
        ng = CHIMERA_VFS_CRED_MAX_GIDS;
    }
    if (ng > CHIMERA_VFS_CRED_MAX_GIDS) {
        ng = CHIMERA_VFS_CRED_MAX_GIDS;
    }
    out->ngids = ng;
    for (i = 0; i < ng; i++) {
        out->gids[i] = grps[i];
    }

    /* NSS supplies no SID; left empty so the algorithmic idmap is used. */
    return 0;
} /* chimera_vfs_identity_nss_handler */

static void
chimera_vfs_identity_add_handler(
    struct chimera_vfs_identity *identity,
    chimera_vfs_identity_handler handler,
    void                        *private_data)
{
    struct chimera_vfs_identity_handler_entry *entry, **pp;

    entry               = calloc(1, sizeof(*entry));
    entry->handler      = handler;
    entry->private_data = private_data;

    pthread_mutex_lock(&identity->handler_lock);
    /* Append to preserve registration order (NSS first, then winbind, ...). */
    pp = &identity->handlers;
    while (*pp) {
        pp = &(*pp)->next;
    }
    *pp = entry;
    pthread_mutex_unlock(&identity->handler_lock);
} /* chimera_vfs_identity_add_handler */

/* ---- lifecycle --------------------------------------------------------- */

SYMBOL_EXPORT SYMBOL_EXPORT struct chimera_vfs_identity *
chimera_vfs_identity_create(
    struct chimera_vfs *vfs,
    int                 num_workers)
{
    struct chimera_vfs_identity *identity;
    int                          i;

    if (num_workers < 1) {
        num_workers = 1;
    }

    identity              = calloc(1, sizeof(*identity));
    identity->vfs         = vfs;
    identity->num_workers = num_workers;
    identity->workers     = calloc(num_workers, sizeof(pthread_t));

    pthread_mutex_init(&identity->lock, NULL);
    pthread_cond_init(&identity->cond, NULL);
    pthread_mutex_init(&identity->handler_lock, NULL);

    /* The default local/NSS handler is always present and tried first.
     * (Registered directly on `identity`: vfs->identity is assigned only after
     * this function returns.) */
    chimera_vfs_identity_add_handler(identity, chimera_vfs_identity_nss_handler,
                                     NULL);

    for (i = 0; i < num_workers; i++) {
        pthread_create(&identity->workers[i], NULL,
                       chimera_vfs_identity_worker, identity);
    }

    return identity;
} /* chimera_vfs_identity_create */

SYMBOL_EXPORT void
chimera_vfs_identity_destroy(struct chimera_vfs_identity *identity)
{
    struct chimera_vfs_identity_handler_entry *entry, *next;
    struct chimera_vfs_identity_request       *req;
    int                                        i;

    pthread_mutex_lock(&identity->lock);
    identity->shutdown = 1;
    pthread_cond_broadcast(&identity->cond);
    pthread_mutex_unlock(&identity->lock);

    for (i = 0; i < identity->num_workers; i++) {
        pthread_join(identity->workers[i], NULL);
    }

    /* Any jobs still queued at shutdown are dropped (their callers are gone). */
    while (identity->queue) {
        req = identity->queue;
        DL_DELETE(identity->queue, req);
        free(req);
    }

    entry = identity->handlers;
    while (entry) {
        next = entry->next;
        free(entry);
        entry = next;
    }

    pthread_mutex_destroy(&identity->lock);
    pthread_cond_destroy(&identity->cond);
    pthread_mutex_destroy(&identity->handler_lock);

    free(identity->workers);
    free(identity);
} /* chimera_vfs_identity_destroy */

SYMBOL_EXPORT void
chimera_vfs_identity_register_handler(
    struct chimera_vfs          *vfs,
    chimera_vfs_identity_handler handler,
    void                        *private_data)
{
    chimera_vfs_identity_add_handler(vfs->identity, handler, private_data);
} /* chimera_vfs_identity_register_handler */

SYMBOL_EXPORT void
chimera_vfs_identity_resolve(
    struct chimera_vfs_thread    *thread,
    enum chimera_vfs_identity_key key,
    uint32_t                      id,
    const char                   *name,
    chimera_vfs_identity_callback callback,
    void                         *private_data)
{
    struct chimera_vfs_identity         *identity = thread->vfs->identity;
    struct chimera_vfs_user              hit;
    struct chimera_vfs_identity_request *req;

    /* Fast path: a cache hit resolves inline on this thread. */
    if (chimera_vfs_identity_cache_probe(thread->vfs->vfs_user_cache,
                                         key, id, name, &hit)) {
        callback(&hit, private_data);
        return;
    }

    /* Miss: dispatch to a worker and park (the callback fires on `thread`'s
     * evpl loop once the worker has resolved and cached the identity). */
    req               = calloc(1, sizeof(*req));
    req->origin       = thread;
    req->key          = key;
    req->id           = id;
    req->callback     = callback;
    req->private_data = private_data;
    if (name) {
        strncpy(req->name, name, sizeof(req->name) - 1);
    }

    pthread_mutex_lock(&identity->lock);
    DL_APPEND(identity->queue, req);
    pthread_cond_signal(&identity->cond);
    pthread_mutex_unlock(&identity->lock);
} /* chimera_vfs_identity_resolve */

SYMBOL_EXPORT int
chimera_vfs_identity_cached(
    struct chimera_vfs           *vfs,
    enum chimera_vfs_identity_key key,
    uint32_t                      id,
    const char                   *name)
{
    struct chimera_vfs_user scratch;

    return chimera_vfs_identity_cache_probe(vfs->vfs_user_cache, key, id, name,
                                            &scratch);
} /* chimera_vfs_identity_cached */

SYMBOL_EXPORT void
chimera_vfs_identity_thread_complete(struct chimera_vfs_thread *thread)
{
    struct chimera_vfs_identity_request *jobs, *req;

    pthread_mutex_lock(&thread->lock);
    jobs                     = thread->pending_identity;
    thread->pending_identity = NULL;
    pthread_mutex_unlock(&thread->lock);

    while (jobs) {
        req = jobs;
        DL_DELETE(jobs, req);
        req->callback(req->found ? &req->result : NULL, req->private_data);
        free(req);
    }
} /* chimera_vfs_identity_thread_complete */
