// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#include "common/chimera_rcu.h"
#include "common/thread.h"
#include <assert.h>
#include <stdatomic.h>
#include <stdlib.h>

static evpl_mutex_t     lock = EVPL_MUTEX_INITIALIZER;
static evpl_cond_t      changed;
static int              ready, release_reader;
static atomic_int       callbacks;
static atomic_int       stop_reader;
static chimera_rcu_head first, second;

static void
after_second(chimera_rcu_head *head)
{
    (void) head;
    atomic_fetch_add(&callbacks, 1);
} /* after_second */


static struct chimera_rcu_domain test_domain;

static void *
domain_reader(void *arg)
{
    (void) arg;
    chimera_rcu_read_lock(&test_domain);
    evpl_mutex_lock(&lock);
    ready++;
    evpl_cond_broadcast(&changed);
    while (!release_reader) {
        evpl_cond_wait(&changed, &lock);
    }
    evpl_mutex_unlock(&lock);
    chimera_rcu_read_unlock(&test_domain);
    return NULL;
} /* domain_reader */

/* Multiple readers must overlap, and retirement must wait for both. */
static void
test_domain_readers(void)
{
    evpl_native_thread_t threads[2];

    ready = release_reader = 0;
    atomic_store(&callbacks, 0);
    chimera_rcu_domain_init(&test_domain);
    for (int i = 0; i < 2; i++) {
        assert(evpl_native_thread_create(&threads[i], NULL, domain_reader, NULL) == 0);
    }
    evpl_mutex_lock(&lock);
    while (ready != 2) {
        evpl_cond_wait(&changed, &lock);
    }
    evpl_mutex_unlock(&lock);
    chimera_rcu_retire(&test_domain, &second, after_second);
    chimera_thread_sleep_us(1000);
    assert(atomic_load(&callbacks) == 0);
    evpl_mutex_lock(&lock);
    release_reader = 1;
    evpl_cond_broadcast(&changed);
    evpl_mutex_unlock(&lock);
    for (int i = 0; i < 2; i++) {
        assert(evpl_native_thread_join(threads[i], NULL) == 0);
    }
    chimera_rcu_barrier();
    assert(atomic_load(&callbacks) == 1);
    chimera_rcu_domain_destroy(&test_domain);
} /* test_domain_readers */

static void *
reporting_reader(void *arg)
{
    int offline = *(int *) arg;

    chimera_rcu_register_thread();
    evpl_mutex_lock(&lock);
    ready = 1;
    evpl_cond_signal(&changed);
    evpl_mutex_unlock(&lock);
    while (!atomic_load(&stop_reader)) {
        if (offline) {
            chimera_rcu_thread_offline();
            chimera_rcu_thread_online();
        } else {
            chimera_rcu_quiescent();
        }
    }
    chimera_rcu_unregister_thread();
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
        chimera_rcu_retire(&chimera_rcu_global, &second, after_second);
        chimera_rcu_barrier();
        assert(atomic_load(&callbacks) == i + 1);
    }
    atomic_store(&stop_reader, 1);
    assert(evpl_native_thread_join(thread, NULL) == 0);
} /* test_reader_progress */
static void
after_first(chimera_rcu_head *head)
{
    (void) head;
    atomic_fetch_add(&callbacks, 1);
    chimera_rcu_retire(&chimera_rcu_global, &second, after_second);
} /* after_first */
static void *
reader(void *arg)
{
    (void) arg;
    chimera_rcu_register_thread();
    /* Registration protects references through the iteration boundary. */
    evpl_mutex_lock(&lock);
    ready = 1;
    evpl_cond_broadcast(&changed);
    while (!release_reader) {
        evpl_cond_wait(&changed, &lock);
    }
    assert(atomic_load(&callbacks) == 0);
    evpl_mutex_unlock(&lock);
    chimera_rcu_thread_offline();
    chimera_rcu_thread_online();
    chimera_rcu_quiescent();
    chimera_rcu_unregister_thread();
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
    test_domain_readers();
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
        chimera_rcu_retire(&chimera_rcu_global, &first, after_first);
        chimera_thread_sleep_us(1000);
        assert(atomic_load(&callbacks) == 0);
        evpl_mutex_lock(&lock);
        release_reader = 1;
        evpl_cond_signal(&changed);
        evpl_mutex_unlock(&lock);
        assert(evpl_native_thread_join(thread, NULL) == 0);
        chimera_rcu_barrier();
        /* The first callback queues another callback while reclaiming. */
        chimera_rcu_barrier();
        assert(atomic_load(&callbacks) == 2);
    }
    chimera_rcu_register_thread();
    chimera_rcu_synchronize(&chimera_rcu_global);
    chimera_rcu_retire(&chimera_rcu_global, &second, after_second);
    chimera_rcu_barrier();
    chimera_rcu_unregister_thread();
    assert(atomic_load(&callbacks) == 3);
    evpl_cond_destroy(&changed);
    atomic_store(&callbacks, 0);
    chimera_rcu_register_thread();
    chimera_rcu_retire(&chimera_rcu_global, &first, after_first);
    /* Exercise shutdown with an online caller and a nested callback. */
    chimera_rcu_shutdown();
    chimera_rcu_unregister_thread();
    return 0;
} /* main */
