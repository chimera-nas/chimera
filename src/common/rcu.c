// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

/* Native QSBR. Readers retain references until their next quiescent state,
 * not merely until read_unlock. A callback batch waits for every reader that
 * was online at the start of its grace period to quiesce or go offline.
 * One process-wide worker executes callbacks in FIFO order. Unix uses liburcu
 * by default; CHIMERA_NATIVE_RCU enables this backend there for testing. */
#include "common/rcu.h"
#include "common/thread.h"
#include <assert.h>
#include <stdlib.h>
#include <stdint.h>

struct chimera_rcu_reader {
    struct chimera_rcu_reader *next;
    uint64_t epoch;
    int online;
    int depth;
};

static evpl_once_t init_once = EVPL_ONCE_INIT;
static evpl_mutex_t lock = EVPL_MUTEX_INITIALIZER;
static evpl_cond_t changed;
static evpl_native_thread_t worker;
static struct chimera_rcu_reader *readers;
static struct rcu_head *pending, **pending_tail = &pending;
static uint64_t epoch, submitted, completed;
static int stopping;
#ifdef _MSC_VER
static __declspec(thread) struct chimera_rcu_reader *self;
#else
static _Thread_local struct chimera_rcu_reader *self;
#endif

static int
readers_pending(uint64_t target)
{
    struct chimera_rcu_reader *reader;
    for (reader = readers; reader; reader = reader->next) {
        if (reader->online && reader->epoch < target) {
            return 1;
        }
    }
    return 0;
}

static void *
reclaim(void *arg)
{
    (void) arg;
    evpl_mutex_lock(&lock);
    for (;;) {
        struct rcu_head *batch;
        uint64_t target, through;
        while (!pending && !stopping) {
            evpl_cond_wait(&changed, &lock);
        }
        if (!pending && stopping) {
            break;
        }
        batch = pending;
        pending = NULL;
        pending_tail = &pending;
        through = submitted;
        target = ++epoch;
        while (readers_pending(target)) {
            evpl_cond_wait(&changed, &lock);
        }
        evpl_mutex_unlock(&lock);
        while (batch) {
            struct rcu_head *next = batch->next;
            batch->callback(batch);
            batch = next;
        }
        evpl_mutex_lock(&lock);
        completed = through;
        evpl_cond_broadcast(&changed);
    }
    evpl_mutex_unlock(&lock);
    return NULL;
}

static void
shutdown_worker(void)
{
    /* A callback may enqueue another callback after the barrier snapshot.
     * The exiting caller must stay offline while the worker drains that tail. */
    if (self) {
        chimera_rcu_unregister_thread();
    }
    chimera_rcu_barrier();
    evpl_mutex_lock(&lock);
    stopping = 1;
    evpl_cond_broadcast(&changed);
    evpl_mutex_unlock(&lock);
    evpl_native_thread_join(worker, NULL);
    evpl_cond_destroy(&changed);
}

static void
initialize(void)
{
    if (evpl_cond_init(&changed, NULL) ||
        evpl_native_thread_create(&worker, NULL, reclaim, NULL)) {
        abort();
    }
    if (atexit(shutdown_worker)) {
        abort();
    }
}

void
chimera_rcu_register_thread(void)
{
    assert(!self);
    evpl_once(&init_once, initialize);
    self = calloc(1, sizeof(*self));
    if (!self) {
        abort();
    }
    evpl_mutex_lock(&lock);
    self->epoch = epoch;
    self->online = 1;
    self->next = readers;
    readers = self;
    evpl_mutex_unlock(&lock);
}

void
chimera_rcu_unregister_thread(void)
{
    struct chimera_rcu_reader **link;
    assert(self && !self->depth);
    evpl_mutex_lock(&lock);
    for (link = &readers; *link != self; link = &(*link)->next) {
    }
    *link = self->next;
    evpl_cond_broadcast(&changed);
    evpl_mutex_unlock(&lock);
    free(self);
    self = NULL;
}

void
chimera_rcu_quiescent_state(void)
{
    assert(self && !self->depth);
    evpl_mutex_lock(&lock);
    self->epoch = epoch;
    evpl_cond_broadcast(&changed);
    evpl_mutex_unlock(&lock);
}

void
chimera_rcu_thread_offline(void)
{
    assert(self && !self->depth);
    evpl_mutex_lock(&lock);
    self->online = 0;
    evpl_cond_broadcast(&changed);
    evpl_mutex_unlock(&lock);
}

void
chimera_rcu_thread_online(void)
{
    assert(self && !self->depth);
    evpl_mutex_lock(&lock);
    self->epoch = epoch;
    self->online = 1;
    evpl_mutex_unlock(&lock);
}

void
chimera_rcu_read_lock(void)
{
    assert(self && self->online);
    self->depth++;
}

void
chimera_rcu_read_unlock(void)
{
    assert(self && self->depth);
    self->depth--;
}

void
chimera_call_rcu(struct rcu_head *head, void (*callback)(struct rcu_head *))
{
    evpl_once(&init_once, initialize);
    head->next = NULL;
    head->callback = callback;
    evpl_mutex_lock(&lock);
    *pending_tail = head;
    pending_tail = &head->next;
    submitted++;
    evpl_cond_broadcast(&changed);
    evpl_mutex_unlock(&lock);
}

void
chimera_rcu_barrier(void)
{
    uint64_t through;
    int online;
    evpl_once(&init_once, initialize);
    assert(!self || !self->depth);
    evpl_mutex_lock(&lock);
    through = submitted;
    online = self && self->online;
    if (online) {
        self->online = 0;
        evpl_cond_broadcast(&changed);
    }
    while (completed < through) {
        evpl_cond_wait(&changed, &lock);
    }
    if (online) {
        self->epoch = epoch;
        self->online = 1;
    }
    evpl_mutex_unlock(&lock);
}

static void
synchronized(struct rcu_head *head)
{
    (void) head;
}

void
chimera_synchronize_rcu(void)
{
    struct rcu_head head;
    chimera_call_rcu(&head, synchronized);
    chimera_rcu_barrier();
}
