// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#include "common/rcu.h"
#include "common/thread.h"
#include <assert.h>
#include <stdatomic.h>
#include <stdlib.h>

static evpl_mutex_t    lock = EVPL_MUTEX_INITIALIZER;
static evpl_cond_t     changed;
static int             ready, release_reader;
static atomic_int      callbacks;
static atomic_int      stop_reader;
static struct rcu_head first, second;

static void
after_second(struct rcu_head *head)
{
    (void) head;
    atomic_fetch_add(&callbacks, 1);
} /* after_second */

static void *
reporting_reader(void *arg)
{
    int offline = *(int *) arg;

    urcu_qsbr_register_thread();
    evpl_mutex_lock(&lock);
    ready = 1;
    evpl_cond_signal(&changed);
    evpl_mutex_unlock(&lock);
    while (!atomic_load(&stop_reader)) {
        if (offline) {
            urcu_qsbr_thread_offline();
            urcu_qsbr_thread_online();
        } else {
            urcu_qsbr_quiescent_state();
        }
    }
    urcu_qsbr_unregister_thread();
    return NULL;
} /* reporting_reader */

/* Each reporting path must let successive grace periods finish while the
 * reader remains registered. Unregistering must not rescue a missed wakeup. */
static void
test_reader_progress(int offline)
{
    evpl_native_thread_t thread;

    ready = 0;
    atomic_store(&stop_reader, 0);
    atomic_store(&callbacks, 0);
    assert(evpl_native_thread_create(&thread, NULL, reporting_reader, &offline) == 0);
    evpl_mutex_lock(&lock);
    while (!ready) {
        evpl_cond_wait(&changed, &lock);
    }
    evpl_mutex_unlock(&lock);
    for (int i = 0; i < 100; i++) {
        call_rcu(&second, after_second);
        rcu_barrier();
        assert(atomic_load(&callbacks) == i + 1);
    }
    atomic_store(&stop_reader, 1);
    assert(evpl_native_thread_join(thread, NULL) == 0);
} /* test_reader_progress */
static void
after_first(struct rcu_head *head)
{
    (void) head;
    atomic_fetch_add(&callbacks, 1);
    call_rcu(&second, after_second);
} /* after_first */
static void *
reader(void *arg)
{
    (void) arg;
    urcu_qsbr_register_thread();
    rcu_read_lock();
    rcu_read_unlock();
    /* QSBR still protects references after read_unlock. */
    evpl_mutex_lock(&lock);
    ready = 1;
    evpl_cond_broadcast(&changed);
    while (!release_reader) {
        evpl_cond_wait(&changed, &lock);
    }
    assert(atomic_load(&callbacks) == 0);
    evpl_mutex_unlock(&lock);
    urcu_qsbr_thread_offline();
    urcu_qsbr_thread_online();
    urcu_qsbr_quiescent_state();
    urcu_qsbr_unregister_thread();
    return NULL;
} /* reader */
static void
verify_shutdown(void)
{
    assert(atomic_load(&callbacks) == 2);
} /* verify_shutdown */
int
main(void)
{
    evpl_native_thread_t thread;

    assert(atexit(verify_shutdown) == 0);
    evpl_cond_init(&changed, NULL);
    test_reader_progress(0);
    test_reader_progress(1);
    for (int i = 0; i < 100; i++) {
        ready = release_reader = 0;
        atomic_store(&callbacks, 0);
        assert(evpl_native_thread_create(&thread, NULL, reader, NULL) == 0);
        evpl_mutex_lock(&lock);
        while (!ready) {
            evpl_cond_wait(&changed, &lock);
        }
        evpl_mutex_unlock(&lock);
        call_rcu(&first, after_first);
        chimera_thread_sleep_us(1000);
        assert(atomic_load(&callbacks) == 0);
        evpl_mutex_lock(&lock);
        release_reader = 1;
        evpl_cond_signal(&changed);
        evpl_mutex_unlock(&lock);
        assert(evpl_native_thread_join(thread, NULL) == 0);
        rcu_barrier();
        /* The first callback queues another callback while reclaiming. */
        rcu_barrier();
        assert(atomic_load(&callbacks) == 2);
    }
    urcu_qsbr_register_thread();
    synchronize_rcu();
    call_rcu(&second, after_second);
    rcu_barrier();
    urcu_qsbr_unregister_thread();
    assert(atomic_load(&callbacks) == 3);
    evpl_cond_destroy(&changed);
    atomic_store(&callbacks, 0);
    urcu_qsbr_register_thread();
    call_rcu(&first, after_first);
    /* Exercise shutdown with an online caller and a nested callback. */
    return 0;
} /* main */
