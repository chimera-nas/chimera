// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

/*
 * RCU shim: liburcu when it is available, reader/writer locks when it is not.
 *
 * chimera's caches use RCU for two separable jobs, and only one of them needs
 * liburcu:
 *
 *   - Publication.  A new entry is stored into a slot with a release store
 *     while a per-shard mutex serializes writers.  That is plain C11 atomics;
 *     it works with or without urcu and is unchanged by this shim.
 *   - Reclaim.  The entry the store displaced cannot be freed until every
 *     reader that might still hold a pointer to it has finished.  That is the
 *     grace period, and it is the only thing liburcu is actually providing.
 *
 * Without liburcu the grace period has an exact and much duller definition:
 * acquire the write lock, release it.  Once that returns, every reader that
 * could have been holding the displaced pointer has left its critical section,
 * because acquiring the write lock is what drained them.
 *
 * So every construct that used to be covered by the one process-wide QSBR
 * grace period instead carries a `struct chimera_rcu_domain`, and the critical
 * section names the domain it is reading:
 *
 *      chimera_rcu_read_lock(&shard->rcu);      urcu: read section
 *      ...                                      else: rwlock read lock
 *      chimera_rcu_read_unlock(&shard->rcu);
 *
 * The domain pointer is ignored entirely in the urcu build -- the struct is a
 * single byte of padding there -- so a call site reads the same in both builds
 * and there is no #if anywhere but this header and its .c file.  Domains are
 * sized to whatever the readers actually contend on: per shard for the hot
 * caches, per table for the mount table, one per cache for the identity and
 * credential caches.  A single global domain would be correct too, but would
 * turn every cache lookup in the fallback into a bounce on one cache line.
 *
 * Mutations bracket themselves, and which bracket depends on what the writer
 * reads while it works:
 *
 *   - chimera_rcu_mutate_begin/end, where the writer also dereferences
 *     RCU-protected data -- the hot caches walk the entries already in a slot
 *     to pick a victim, and those may be mid-reclaim from an earlier
 *     displacement.  Under urcu that needs a genuine read section; in the
 *     fallback it is the write lock.
 *   - chimera_rcu_publish_begin/end, where the writer only publishes and
 *     retires under a lock of its own that serializes every mutation, so
 *     nothing it walks can be unlinked underneath it.  Under urcu that needs
 *     no read section at all -- and must not take one, because these run on
 *     threads that are deliberately never registered as QSBR readers (a
 *     thread parked in cond_timedwait must not sit in the grace-period
 *     quorum; see the expiry sweeps in vfs_user_cache.h and s3_cred_cache.h).
 *     In the fallback it is the same write lock, because reclaim still has to
 *     exclude readers.
 *
 * Neither is called write_lock: in the urcu build neither is one, and a reader
 * who believes otherwise will reason wrongly about what it excludes.
 *
 * Reclaim comes in two flavors:
 *
 *   - chimera_rcu_replace() publishes a pointer and hands the displaced object
 *     to the domain in one call, so a slot cannot be overwritten without
 *     saying what happens to what was there.  The victim is queued on a
 *     thread-local list and dispatched by chimera_rcu_mutate_end(), i.e. after
 *     the construct's own mutex has been dropped -- the same point the bare
 *     call_rcu() used to sit at, so no lock is held any longer than before.
 *     In the fallback the callback simply runs there: the write lock drained
 *     the readers before the store, so by the time it is released nothing can
 *     still be holding the victim.
 *   - chimera_rcu_retire() is for the objects that are freed after a grace
 *     period without any accompanying store -- a torn-down mount, a filesystem
 *     whose last op is still draining.  In the fallback it hands the object to
 *     a reclaim thread, which pings the domain's write lock and then runs the
 *     callback.  It never blocks the caller, because some of these run on
 *     event-loop threads that hold a read section of the domain they are
 *     retiring against.
 *
 * The coarsest domain, chimera_rcu_global, is quiescence rather than a data
 * structure: an event-loop thread holds its read lock for the whole of each
 * loop iteration and drops it at the iteration boundary and before sleeping,
 * driven by the same evpl loop hooks that drive quiescent states under QSBR.
 * A write-lock ping on it therefore means precisely what a QSBR grace period
 * means -- every registered thread has passed an iteration boundary -- which
 * is what the VFS modules rely on when they retire a filesystem that an
 * in-flight dispatch may still be inside.
 *
 * The one rule this imposes that QSBR did not: a thread must not enter a
 * mutate or publish region on a domain whose read side it already holds, and
 * must not re-enter a read section it already holds.  Under QSBR both are free
 * -- read_lock is a no-op and nesting costs nothing -- so nothing stopped a
 * caller from looking something up and mutating without leaving the lookup's
 * section.  Here that is a self-deadlock: the read lock is real, and the
 * domains are writer-preferring (they have to be, or a busy shard could stall
 * reclaim indefinitely rather than merely delay it).  Take what you need from
 * the read side, leave it, then mutate.
 */

#include <stddef.h>
#include <stdint.h>
#include <pthread.h>

#include "common/macros.h"

#ifdef CHIMERA_HAVE_URCU

#include <urcu/urcu-qsbr.h>

typedef struct rcu_head chimera_rcu_head;

/* Ignored in this build; present so call sites are identical either way. */
struct chimera_rcu_domain {
    char unused;
};

#else /* !CHIMERA_HAVE_URCU */

typedef struct chimera_rcu_head chimera_rcu_head;

struct chimera_rcu_head {
    chimera_rcu_head *next;
    void              (*func)(
        chimera_rcu_head *head);
};

struct chimera_rcu_domain {
    pthread_rwlock_t lock;
};

#endif /* CHIMERA_HAVE_URCU */

typedef void (*chimera_rcu_cb)(
    chimera_rcu_head *head);

/*
 * The quiescence domain.  Read-locked by every registered thread for the span
 * of an event-loop iteration; write-locked to wait for all of them to reach an
 * iteration boundary.  See the header comment.
 */
extern SYMBOL_EXPORT struct chimera_rcu_domain chimera_rcu_global;

SYMBOL_EXPORT void chimera_rcu_domain_init(
    struct chimera_rcu_domain *domain);

SYMBOL_EXPORT void chimera_rcu_domain_destroy(
    struct chimera_rcu_domain *domain);

/* Thread lifecycle.  Registered threads are the readers; the loop hooks below
 * are what let a grace period complete while they run. */
SYMBOL_EXPORT void chimera_rcu_register_thread(
    void);
SYMBOL_EXPORT void chimera_rcu_unregister_thread(
    void);
SYMBOL_EXPORT void chimera_rcu_quiescent(
    void);
SYMBOL_EXPORT void chimera_rcu_thread_offline(
    void);
SYMBOL_EXPORT void chimera_rcu_thread_online(
    void);

/* Retire `head` after a grace period on `domain`, with no accompanying store.
 * Never blocks the caller; safe from inside a read section of any domain. */
SYMBOL_EXPORT void chimera_rcu_retire(
    struct chimera_rcu_domain *domain,
    chimera_rcu_head          *head,
    chimera_rcu_cb             func);

/* Wait for one grace period on `domain`.  Must NOT be called from inside a
 * read section of that domain. */
SYMBOL_EXPORT void chimera_rcu_synchronize(
    struct chimera_rcu_domain *domain);

/* Wait until every chimera_rcu_retire() issued before this call has run. */
SYMBOL_EXPORT void chimera_rcu_barrier(
    void);

/* Stop the reclaim machinery.  Idempotent; drains before returning. */
SYMBOL_EXPORT void chimera_rcu_shutdown(
    void);

/*
 * Thread-local queue of objects displaced by chimera_rcu_replace() and not yet
 * handed on.  Defined in chimera_rcu.c so that the push stays inline (it runs
 * under the construct's mutex on the insert path) while remaining a single
 * object across the whole program.
 */
#define CHIMERA_RCU_PENDING_MAX 32

struct chimera_rcu_pending {
    unsigned n;
    struct {
        chimera_rcu_head *head;
        chimera_rcu_cb    func;
    } entry[CHIMERA_RCU_PENDING_MAX];
};

extern SYMBOL_EXPORT __thread struct chimera_rcu_pending chimera_rcu_pending;

SYMBOL_EXPORT void chimera_rcu_pending_flush(
    void);

static inline void
chimera_rcu_pend(
    chimera_rcu_head *head,
    chimera_rcu_cb    func)
{
    if (!head) {
        return;
    }

    if (chimera_rcu_pending.n == CHIMERA_RCU_PENDING_MAX) {
        /* Overflow: dispatching early is safe in both builds -- call_rcu may be
         * called anywhere, and in the fallback the victims are already
         * unreachable.  Only the bulk-retire loops ever reach this. */
        chimera_rcu_pending_flush();
    }

    chimera_rcu_pending.entry[chimera_rcu_pending.n].head = head;
    chimera_rcu_pending.entry[chimera_rcu_pending.n].func = func;
    chimera_rcu_pending.n++;
} /* chimera_rcu_pend */

#ifdef CHIMERA_HAVE_URCU

#define chimera_rcu_deref(p)     rcu_dereference(p)
#define chimera_rcu_assign(p, v) rcu_assign_pointer(p, v)

static inline void
chimera_rcu_read_lock(struct chimera_rcu_domain *domain)
{
    (void) domain;
    urcu_qsbr_read_lock();
} /* chimera_rcu_read_lock */

static inline void
chimera_rcu_read_unlock(struct chimera_rcu_domain *domain)
{
    (void) domain;
    urcu_qsbr_read_unlock();
} /* chimera_rcu_read_unlock */

static inline void
chimera_rcu_mutate_begin(struct chimera_rcu_domain *domain)
{
    (void) domain;
    urcu_qsbr_read_lock();
} /* chimera_rcu_mutate_begin */

static inline void
chimera_rcu_mutate_end(struct chimera_rcu_domain *domain)
{
    (void) domain;
    urcu_qsbr_read_unlock();
    if (chimera_rcu_pending.n) {
        chimera_rcu_pending_flush();
    }
} /* chimera_rcu_mutate_end */

static inline void
chimera_rcu_publish_begin(struct chimera_rcu_domain *domain)
{
    (void) domain;
} /* chimera_rcu_publish_begin */

static inline void
chimera_rcu_publish_end(struct chimera_rcu_domain *domain)
{
    (void) domain;
    if (chimera_rcu_pending.n) {
        chimera_rcu_pending_flush();
    }
} /* chimera_rcu_publish_end */

#else /* !CHIMERA_HAVE_URCU */

#define chimera_rcu_deref(p)     __atomic_load_n(&(p), __ATOMIC_ACQUIRE)
#define chimera_rcu_assign(p, v) __atomic_store_n(&(p), (v), __ATOMIC_RELEASE)

static inline void
chimera_rcu_read_lock(struct chimera_rcu_domain *domain)
{
    pthread_rwlock_rdlock(&domain->lock);
} /* chimera_rcu_read_lock */

static inline void
chimera_rcu_read_unlock(struct chimera_rcu_domain *domain)
{
    pthread_rwlock_unlock(&domain->lock);
} /* chimera_rcu_read_unlock */

static inline void
chimera_rcu_mutate_begin(struct chimera_rcu_domain *domain)
{
    pthread_rwlock_wrlock(&domain->lock);
} /* chimera_rcu_mutate_begin */

static inline void
chimera_rcu_mutate_end(struct chimera_rcu_domain *domain)
{
    pthread_rwlock_unlock(&domain->lock);
    if (chimera_rcu_pending.n) {
        chimera_rcu_pending_flush();
    }
} /* chimera_rcu_mutate_end */

static inline void
chimera_rcu_publish_begin(struct chimera_rcu_domain *domain)
{
    pthread_rwlock_wrlock(&domain->lock);
} /* chimera_rcu_publish_begin */

static inline void
chimera_rcu_publish_end(struct chimera_rcu_domain *domain)
{
    pthread_rwlock_unlock(&domain->lock);
    if (chimera_rcu_pending.n) {
        chimera_rcu_pending_flush();
    }
} /* chimera_rcu_publish_end */

#endif /* CHIMERA_HAVE_URCU */

/*
 * Publish `value` into `slot` and retire `head` (which may be NULL) with it.
 * Must be called inside a mutate or publish region on `domain`; the retire is
 * dispatched when that region ends.
 */
#define chimera_rcu_replace(domain, slot, value, head, func) \
        do {                                                     \
            (void) (domain);                                     \
            chimera_rcu_assign(slot, value);                     \
            chimera_rcu_pend(head, func);                        \
        } while (0)
