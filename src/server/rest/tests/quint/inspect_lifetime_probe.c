// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only

/* Exercise management-task ownership with a deliberately unresponsive worker.
 * Blocking belongs only to this synthetic protocol, never the collector. */
#include <stdlib.h>
#include <stdio.h>
#include <stdatomic.h>
#include "common/thread.h"
#include "server/server_inspect.h"
#include "server/protocol.h"

struct test_env;
struct worker {
    struct test_env                  *env;
    int                               index;
    struct evpl                      *evpl;
    struct chimera_inspect_endpoint  *endpoint;
    struct evpl_doorbell              control;
    struct evpl_doorbell_sender      *sender;
    struct chimera_inspect_collector *collector;
    struct chimera_server_protocol   *protocols[1];
    void                             *contexts[1];
};
struct test_env {
    struct chimera_server_inspect *inspect;
    struct worker                  workers[2];
    atomic_int                     phase, done, block, entered, callbacks;
};
static void
check(
    int         condition,
    const char *message)
{
    if (!condition) {
        fprintf(stderr, "inspection lifetime: %s\n", message); abort();
    }
} /* check */
static void
inspect(
    void                          *data,
    struct chimera_inspect_writer *writer)
{
    struct worker *worker = data;

    if (worker->index == 1) {
        atomic_store(&worker->env->entered, 1);
        while (atomic_load(&worker->env->block)) {
            chimera_thread_sleep_us(1000);
        }
    }
    if (atomic_load(&worker->env->phase) == 3) {
        for (unsigned int i = 0; i < 2000; i++) {
            struct chimera_inspect_connection row = { .service = CHIMERA_INSPECT_NFS };
            row.bind.serial = i + 1;
            writer->sample->scanned++;
            if (!writer->emit(writer, &row)) {
                break;
            }
        }
    }
} /* inspect */
static struct chimera_server_protocol protocol = { .inspect = inspect };
static void
complete(
    const struct chimera_inspect_result *result,
    void                                *data)
{
    struct worker   *worker = data;
    struct test_env *env    = worker->env;
    int              phase  = atomic_load(&env->phase);

    atomic_fetch_add(&env->callbacks, 1);
    check(phase != 1 && phase != 4, "cancelled collector never calls back");
    if (phase == 6) {
        check(result->responded == 0, "zero responding selected workers");
    } else if (phase == 2) {
        check(result->responded == 1 && result->present[0] && !result->present[1], "timeout reports missing worker");
        struct chimera_inspect_filter filter = { .connections = 1, .worker_id = -1 };
        for (int i = 0; i < 100; i++) {
            struct chimera_inspect_collector *other;
            check(chimera_server_inspect_collect(env->inspect, worker->evpl, &filter, complete, worker, &other) == 429,
                  "timeout keeps admission until stalled task releases");
        }
    } else {
        check(result->responded == 2, "both workers responded");
        check(result->samples[0].nrows + result->samples[1].nrows == CHIMERA_INSPECT_ROWS, "global row bound");
        check(result->samples[0].truncated && result->samples[1].truncated, "per-worker truncation explicit");
    }
    chimera_server_inspect_release(worker->collector);
    worker->collector = NULL;
    atomic_store(&env->done, phase);
} /* complete */
static void
command(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct worker   *worker = container_of(doorbell, struct worker, control);
    struct test_env *env    = worker->env;
    int              phase  = atomic_load(&env->phase);

    if (worker->index == 1 && phase == 7) {
        worker->endpoint = chimera_server_inspect_register(env->inspect, evpl, NULL,
                                                           1, worker->protocols, worker->contexts);
        atomic_store(&env->done, 7);
        return;
    }
    if (worker->index == 1 && phase == 5) {
        chimera_server_inspect_unregister(worker->endpoint);
        worker->endpoint = NULL;
        atomic_store(&env->done, 5);
        return;
    }
    struct chimera_inspect_filter filter = { 0 };
    filter.connections = 1;
    filter.worker_id   = phase == 6 ? 1 : -1;
    filter.services    = 1U << CHIMERA_INSPECT_NFS;
    filter.remote_port = -1;
    filter.local_port  = -1;
    int                           status = chimera_server_inspect_collect(env->inspect, evpl, &filter, complete, worker,
                                                                          &worker->collector);

    if (status == 429) {
        atomic_store(&env->done, -1); return;
    }
    check(status == 0, "collector starts");
    if (phase == 1 || phase == 4) {
        chimera_server_inspect_cancel(worker->collector);
        chimera_server_inspect_release(worker->collector);
        worker->collector = NULL;
        if (phase == 4) {
            chimera_server_inspect_unregister(worker->endpoint);
            worker->endpoint = NULL;
        } else {
            for (int i = 0; i < 100; i++) {
                struct chimera_inspect_collector *other;
                check(chimera_server_inspect_collect(env->inspect, evpl, &filter, complete, worker, &other) == 429,
                      "cancellation keeps admission before queued self-task runs");
            }
        }
        atomic_store(&env->done, phase);
    }
} /* command */
static void *
init(
    struct evpl *evpl,
    void        *data)
{
    struct test_env            *env = data;
    struct evpl_thread_identity identity;

    evpl_thread_get_identity(evpl, &identity);
    struct worker              *worker = &env->workers[identity.pool_index];
    worker->env          = env;
    worker->index        = identity.pool_index;
    worker->evpl         = evpl;
    worker->protocols[0] = &protocol;
    worker->contexts[0]  = worker;
    worker->endpoint     = chimera_server_inspect_register(env->inspect, evpl, NULL, 1, worker->protocols, worker->
                                                           contexts);
    evpl_add_doorbell(evpl, &worker->control, command);
    worker->sender = evpl_doorbell_sender(&worker->control);
    return worker;
} /* init */
static void
shutdown_worker(
    struct evpl *evpl,
    void        *data)
{
    struct worker *worker = data;

    if (worker->endpoint) {
        chimera_server_inspect_unregister(worker->endpoint);
    }
    evpl_remove_doorbell(evpl, &worker->control);
} /* shutdown_worker */
static void
await_value(
    atomic_int *value,
    int         expected)
{
    for (int i = 0; i < 10000; i++) {
        if (atomic_load(value) == expected) {
            return;
        }
        chimera_thread_sleep_us(1000);
    }
    check(0, "bounded test wait");
} /* await_value */
static void
start(
    struct test_env *env,
    int              phase)
{
    atomic_store(&env->phase, phase);
    for (;;) {
        atomic_store(&env->done, 0);
        evpl_doorbell_signal(env->workers[0].sender);
        for (int i = 0; i < 10000 && !atomic_load(&env->done); i++) {
            chimera_thread_sleep_us(1000);
        }
        if (atomic_load(&env->done) != -1) {
            break;
        }
        chimera_thread_sleep_us(1000);
    }
    check(atomic_load(&env->done) == phase, "phase completed");
} /* start */
int
main(void)
{
    struct test_env         env = { 0 };

    env.inspect = chimera_server_inspect_create(2);
    struct evpl_threadpool *pool = evpl_threadpool_create(NULL, 2, init, shutdown_worker, &env);
    atomic_store(&env.block, 1);
    start(&env, 1);
    await_value(&env.entered, 1);
    atomic_store(&env.block, 0);
    chimera_thread_sleep_us(10000);
    atomic_store(&env.block, 1);
    start(&env, 2);
    atomic_store(&env.block, 0);
    chimera_thread_sleep_us(10000);
    start(&env, 3);
    atomic_store(&env.phase, 5);
    atomic_store(&env.done, 0);
    evpl_doorbell_signal(env.workers[1].sender);
    await_value(&env.done, 5);
    start(&env, 6);
    atomic_store(&env.phase, 7);
    atomic_store(&env.done, 0);
    evpl_doorbell_signal(env.workers[1].sender);
    await_value(&env.done, 7);
    atomic_store(&env.entered, 0);
    atomic_store(&env.block, 1);
    start(&env, 4);
    await_value(&env.entered, 1);
    atomic_store(&env.block, 0);
    check(atomic_load(&env.callbacks) == 3, "exactly one callback per completed request");
    chimera_server_inspect_stop(env.inspect);
    evpl_threadpool_destroy(pool);
    for (int i = 0; i < 2; i++) {
        evpl_doorbell_sender_release(env.workers[i].sender);
    }
    chimera_server_inspect_destroy(env.inspect);
    return 0;
} /* main */
