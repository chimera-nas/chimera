// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Out-of-line half of the RCU shim.  See chimera_rcu.h for the model; this
 * file holds the pieces that must exist exactly once (the thread-local retire
 * queue, the quiescence domain) and, in the fallback build, the reclaim thread
 * that turns a write-lock acquisition into a grace period.
 */

#include <stdlib.h>
#include <string.h>

#include "common/chimera_rcu.h"

SYMBOL_EXPORT __thread struct chimera_rcu_pending chimera_rcu_pending;

SYMBOL_EXPORT struct chimera_rcu_domain           chimera_rcu_global;

#ifdef CHIMERA_HAVE_URCU

SYMBOL_EXPORT void
chimera_rcu_domain_init(struct chimera_rcu_domain *domain)
{
    (void) domain;
} /* chimera_rcu_domain_init */

SYMBOL_EXPORT void
chimera_rcu_domain_destroy(struct chimera_rcu_domain *domain)
{
    (void) domain;
} /* chimera_rcu_domain_destroy */

SYMBOL_EXPORT void
chimera_rcu_register_thread(void)
{
    urcu_qsbr_register_thread();
} /* chimera_rcu_register_thread */

SYMBOL_EXPORT void
chimera_rcu_unregister_thread(void)
{
    urcu_qsbr_unregister_thread();
} /* chimera_rcu_unregister_thread */

SYMBOL_EXPORT void
chimera_rcu_quiescent(void)
{
    urcu_qsbr_quiescent_state();
} /* chimera_rcu_quiescent */

SYMBOL_EXPORT void
chimera_rcu_thread_offline(void)
{
    urcu_qsbr_thread_offline();
} /* chimera_rcu_thread_offline */

SYMBOL_EXPORT void
chimera_rcu_thread_online(void)
{
    urcu_qsbr_thread_online();
} /* chimera_rcu_thread_online */

SYMBOL_EXPORT void
chimera_rcu_retire(
    struct chimera_rcu_domain *domain,
    chimera_rcu_head          *head,
    chimera_rcu_cb             func)
{
    (void) domain;
    call_rcu(head, func);
} /* chimera_rcu_retire */

SYMBOL_EXPORT void
chimera_rcu_synchronize(struct chimera_rcu_domain *domain)
{
    (void) domain;
    urcu_qsbr_synchronize_rcu();
} /* chimera_rcu_synchronize */

SYMBOL_EXPORT void
chimera_rcu_barrier(void)
{
    rcu_barrier();
} /* chimera_rcu_barrier */

SYMBOL_EXPORT void
chimera_rcu_shutdown(void)
{
} /* chimera_rcu_shutdown */

SYMBOL_EXPORT void
chimera_rcu_pending_flush(void)
{
    unsigned i;

    for (i = 0; i < chimera_rcu_pending.n; i++) {
        call_rcu(chimera_rcu_pending.entry[i].head,
                 chimera_rcu_pending.entry[i].func);
    }

    chimera_rcu_pending.n = 0;
} /* chimera_rcu_pending_flush */

#else /* !CHIMERA_HAVE_URCU */

/*
 * A retire with no accompanying store cannot ping the write lock on the
 * calling thread: several of them run on event-loop threads that are inside a
 * read section of the very domain they are retiring against (a VFS module
 * retiring a filesystem holds the quiescence domain for the whole iteration).
 * So they are handed to a reclaim thread, which holds no read section and can
 * therefore take any domain's write lock.
 */
struct chimera_rcu_retired {
    struct chimera_rcu_retired *next;
    struct chimera_rcu_domain  *domain;
    chimera_rcu_head           *head;
    chimera_rcu_cb              func;
};

static struct {
    pthread_mutex_t             lock;
    pthread_cond_t              queued;   /* work arrived, or shutdown */
    pthread_cond_t              drained;  /* a batch completed */
    struct chimera_rcu_retired *head;
    struct chimera_rcu_retired *tail;
    uint64_t                    submitted;
    uint64_t                    completed;
    pthread_t                   thread;
    int                         running;
    int                         stopping;
} chimera_rcu_reclaim = {
    .lock    = PTHREAD_MUTEX_INITIALIZER,
    .queued  = PTHREAD_COND_INITIALIZER,
    .drained = PTHREAD_COND_INITIALIZER,
};

static __thread int chimera_rcu_is_online;

SYMBOL_EXPORT void
chimera_rcu_domain_init(struct chimera_rcu_domain *domain)
{
    pthread_rwlockattr_t attr;

    pthread_rwlockattr_init(&attr);

    /*
     * Writer preference.  A grace period here is a write-lock acquisition, and
     * with the default reader-preferring policy a busy cache shard can keep a
     * writer waiting indefinitely -- which would stall reclaim, not just delay
     * it, and grow the heap without bound.  The cost is that a thread must not
     * re-enter a read section it already holds, which nothing here does: the
     * inner cache lookups take no lock of their own and rely on the caller to
     * bracket them.
     */
#ifdef PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP
    pthread_rwlockattr_setkind_np(&attr,
                                  PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP);
#endif /* ifdef PTHREAD_RWLOCK_PREFER_WRITER_NONRECURSIVE_NP */

    pthread_rwlock_init(&domain->lock, &attr);
    pthread_rwlockattr_destroy(&attr);
} /* chimera_rcu_domain_init */

SYMBOL_EXPORT void
chimera_rcu_domain_destroy(struct chimera_rcu_domain *domain)
{
    pthread_rwlock_destroy(&domain->lock);
} /* chimera_rcu_domain_destroy */

static void __attribute__((constructor))
chimera_rcu_global_init(void)
{
    chimera_rcu_domain_init(&chimera_rcu_global);
} /* chimera_rcu_global_init */

SYMBOL_EXPORT void
chimera_rcu_synchronize(struct chimera_rcu_domain *domain)
{
    int online = chimera_rcu_is_online;

    /*
     * Waiting for a grace period from inside one is a deadlock, exactly as it
     * is under QSBR.  A caller that is online -- a teardown path running on an
     * event-loop thread -- goes offline for the wait, which is what
     * rcu_thread_offline() around a blocking call buys under urcu.
     */
    if (online) {
        chimera_rcu_thread_offline();
    }

    /*
     * The whole grace period: by the time the write lock is held, every reader
     * that entered before this call has left, and none can enter until it is
     * released.
     */
    pthread_rwlock_wrlock(&domain->lock);
    pthread_rwlock_unlock(&domain->lock);

    if (online) {
        chimera_rcu_thread_online();
    }
} /* chimera_rcu_synchronize */

static void *
chimera_rcu_reclaim_thread(void *arg)
{
    struct chimera_rcu_retired *batch, *item, *next;
    uint64_t                    done;

    (void) arg;

    while (1) {

        pthread_mutex_lock(&chimera_rcu_reclaim.lock);

        while (!chimera_rcu_reclaim.head && !chimera_rcu_reclaim.stopping) {
            pthread_cond_wait(&chimera_rcu_reclaim.queued,
                              &chimera_rcu_reclaim.lock);
        }

        if (!chimera_rcu_reclaim.head) {
            pthread_mutex_unlock(&chimera_rcu_reclaim.lock);
            break;
        }

        batch                    = chimera_rcu_reclaim.head;
        chimera_rcu_reclaim.head = NULL;
        chimera_rcu_reclaim.tail = NULL;

        pthread_mutex_unlock(&chimera_rcu_reclaim.lock);

        /*
         * One grace period per domain per batch, then the callbacks -- run
         * after the write lock is released, so a callback that takes a lock of
         * its own (the recycle pool's depot, say) can never order itself
         * against a domain write lock.
         */
        done = 0;

        for (item = batch; item; item = item->next) {
            struct chimera_rcu_retired *prior;
            int                         seen = 0;

            for (prior = batch; prior != item; prior = prior->next) {
                if (prior->domain == item->domain) {
                    seen = 1;
                    break;
                }
            }

            if (!seen) {
                chimera_rcu_synchronize(item->domain);
            }
        }

        for (item = batch; item; item = next) {
            next = item->next;
            item->func(item->head);
            free(item);
            done++;
        }

        pthread_mutex_lock(&chimera_rcu_reclaim.lock);
        chimera_rcu_reclaim.completed += done;
        pthread_cond_broadcast(&chimera_rcu_reclaim.drained);
        pthread_mutex_unlock(&chimera_rcu_reclaim.lock);
    }

    return NULL;
} /* chimera_rcu_reclaim_thread */

SYMBOL_EXPORT void
chimera_rcu_retire(
    struct chimera_rcu_domain *domain,
    chimera_rcu_head          *head,
    chimera_rcu_cb             func)
{
    struct chimera_rcu_retired *item;

    item = malloc(sizeof(*item));

    if (!item) {
        /*
         * Out of memory on a path that exists to prevent a use-after-free.
         * Take the grace period inline rather than free early; the caller may
         * block here, which is strictly better than the alternative.
         */
        chimera_rcu_synchronize(domain);
        func(head);
        return;
    }

    item->next   = NULL;
    item->domain = domain;
    item->head   = head;
    item->func   = func;

    pthread_mutex_lock(&chimera_rcu_reclaim.lock);

    if (!chimera_rcu_reclaim.running) {
        chimera_rcu_reclaim.running = 1;
        pthread_create(&chimera_rcu_reclaim.thread, NULL,
                       chimera_rcu_reclaim_thread, NULL);
    }

    if (chimera_rcu_reclaim.tail) {
        chimera_rcu_reclaim.tail->next = item;
    } else {
        chimera_rcu_reclaim.head = item;
    }
    chimera_rcu_reclaim.tail = item;
    chimera_rcu_reclaim.submitted++;

    pthread_cond_signal(&chimera_rcu_reclaim.queued);
    pthread_mutex_unlock(&chimera_rcu_reclaim.lock);
} /* chimera_rcu_retire */

SYMBOL_EXPORT void
chimera_rcu_barrier(void)
{
    uint64_t target;
    int      online = chimera_rcu_is_online;

    /* The reclaim thread needs the quiescence domain's write lock to finish
     * the batch we are waiting on; holding its read lock here would deadlock. */
    if (online) {
        chimera_rcu_thread_offline();
    }

    pthread_mutex_lock(&chimera_rcu_reclaim.lock);

    target = chimera_rcu_reclaim.submitted;

    while (chimera_rcu_reclaim.running &&
           chimera_rcu_reclaim.completed < target) {
        pthread_cond_wait(&chimera_rcu_reclaim.drained,
                          &chimera_rcu_reclaim.lock);
    }

    pthread_mutex_unlock(&chimera_rcu_reclaim.lock);

    if (online) {
        chimera_rcu_thread_online();
    }
} /* chimera_rcu_barrier */

SYMBOL_EXPORT void
chimera_rcu_shutdown(void)
{
    pthread_t thread;
    int       online = chimera_rcu_is_online;

    pthread_mutex_lock(&chimera_rcu_reclaim.lock);

    if (!chimera_rcu_reclaim.running) {
        pthread_mutex_unlock(&chimera_rcu_reclaim.lock);
        return;
    }

    thread                       = chimera_rcu_reclaim.thread;
    chimera_rcu_reclaim.stopping = 1;
    pthread_cond_broadcast(&chimera_rcu_reclaim.queued);
    pthread_mutex_unlock(&chimera_rcu_reclaim.lock);

    /* The reclaim thread drains what is queued before it stops, and that drain
     * needs the quiescence domain's write lock.  A caller still holding its
     * read lock would wait for a thread that is waiting for the caller, so go
     * offline across the join -- the same rule as the barrier above. */
    if (online) {
        chimera_rcu_thread_offline();
    }

    pthread_join(thread, NULL);

    if (online) {
        chimera_rcu_thread_online();
    }

    pthread_mutex_lock(&chimera_rcu_reclaim.lock);
    chimera_rcu_reclaim.running  = 0;
    chimera_rcu_reclaim.stopping = 0;
    pthread_mutex_unlock(&chimera_rcu_reclaim.lock);
} /* chimera_rcu_shutdown */

/*
 * Thread lifecycle.  "Online" means the thread holds the quiescence domain's
 * read lock, which it does for the span of an event-loop iteration; the evpl
 * loop hooks drop it at the iteration boundary and before the core wait, so a
 * write-lock ping completes in about one iteration -- the same shape as a QSBR
 * grace period, including the same hazard that a registered thread which stops
 * running its loop without unregistering will hold one up.
 */
SYMBOL_EXPORT void
chimera_rcu_thread_online(void)
{
    if (chimera_rcu_is_online) {
        return;
    }
    pthread_rwlock_rdlock(&chimera_rcu_global.lock);
    chimera_rcu_is_online = 1;
} /* chimera_rcu_thread_online */

SYMBOL_EXPORT void
chimera_rcu_thread_offline(void)
{
    if (!chimera_rcu_is_online) {
        return;
    }
    chimera_rcu_is_online = 0;
    pthread_rwlock_unlock(&chimera_rcu_global.lock);
} /* chimera_rcu_thread_offline */

SYMBOL_EXPORT void
chimera_rcu_quiescent(void)
{
    if (!chimera_rcu_is_online) {
        return;
    }
    pthread_rwlock_unlock(&chimera_rcu_global.lock);
    pthread_rwlock_rdlock(&chimera_rcu_global.lock);
} /* chimera_rcu_quiescent */

SYMBOL_EXPORT void
chimera_rcu_register_thread(void)
{
    chimera_rcu_thread_online();
} /* chimera_rcu_register_thread */

SYMBOL_EXPORT void
chimera_rcu_unregister_thread(void)
{
    chimera_rcu_thread_offline();
} /* chimera_rcu_unregister_thread */

SYMBOL_EXPORT void
chimera_rcu_pending_flush(void)
{
    unsigned i;

    /*
     * Each of these was displaced by a store made while this thread held the
     * domain's write lock, so the readers were already drained before the
     * store and none can have picked it up since.  Freeing it here -- just
     * after the write lock was dropped, outside the construct's own mutex --
     * needs no further grace period.
     */
    for (i = 0; i < chimera_rcu_pending.n; i++) {
        chimera_rcu_pending.entry[i].func(chimera_rcu_pending.entry[i].head);
    }

    chimera_rcu_pending.n = 0;
} /* chimera_rcu_pending_flush */

#endif /* CHIMERA_HAVE_URCU */
