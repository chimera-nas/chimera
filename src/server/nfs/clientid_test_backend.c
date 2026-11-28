// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * The "test" client ID allocator backend, built only with the tests.
 *
 * Type 1 IDs exactly as the local backend mints them, but each result is
 * completed from a separate worker thread after kwargs.delay_us microseconds.
 * That holds registration windows open (retransmit-while-pending tests) and
 * exercises the wrapper's cross-thread hand-back.
 *
 *   kwargs { version: 0, node_id, instance, generation,
 *            delay_us: 0..10000000 }
 */

#include <errno.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "clientid_allocator_internal.h"
#include "clientid_format.h"
#include "nfs_internal.h"

#define CLIENTID_TEST_MAX_DELAY_US 10000000ULL

struct clientid_test_item {
    struct clientid_test_item *next;
    struct clientid_req       *req;
    int                        status;
    uint64_t                   clientid;
    struct timespec            due;
};

struct clientid_test {
    struct clientid_local     *local;   /* first member: see get_stats */
    uint64_t                   delay_us;
    pthread_t                  worker;
    pthread_mutex_t            lock;
    pthread_cond_t             cond;
    struct clientid_test_item *head, *tail;
    bool                       stop;
};

static const uint8_t clientid_test_types[] = { CLIENTID_TYPE_SUPERVISOR };

/* The kwargs the local backend understands: everything but delay_us. */
static json_t *
clientid_test_local_kwargs(const json_t *kwargs)
{
    json_t *copy = json_copy((json_t *) kwargs);

    if (copy) {
        json_object_del(copy, "delay_us");
    }
    return copy;
} /* clientid_test_local_kwargs */

static int
clientid_test_supported(
    uint8_t       type,
    const json_t *kwargs)
{
    json_t    *local, *jd;
    json_int_t d;
    int        rc;

    jd = json_object_get(kwargs, "delay_us");
    if (jd) {
        if (!json_is_integer(jd)) {
            chimera_nfs_error("clientid allocator: kwargs \"delay_us\" must be an integer");
            return EINVAL;
        }
        d = json_integer_value(jd);
        if (d < 0 || (uint64_t) d > CLIENTID_TEST_MAX_DELAY_US) {
            chimera_nfs_error("clientid allocator: kwargs \"delay_us\" is out of range");
            return EINVAL;
        }
    }

    local = clientid_test_local_kwargs(kwargs);
    if (!local) {
        return ENOMEM;
    }
    rc = clientid_local_supported(type, local);
    json_decref(local);
    return rc;
} /* clientid_test_supported */

static void *
clientid_test_worker(void *arg)
{
    struct clientid_test      *t = arg;
    struct clientid_test_item *item;

    pthread_mutex_lock(&t->lock);
    for (;;) {
        while (!t->head && !t->stop) {
            pthread_cond_wait(&t->cond, &t->lock);
        }
        if (!t->head) {
            break;
        }
        item = t->head;
        if (pthread_cond_timedwait(&t->cond, &t->lock, &item->due) == 0 && !t->stop) {
            continue;   /* woken early: re-check the head */
        }
        t->head = item->next;
        if (!t->head) {
            t->tail = NULL;
        }
        pthread_mutex_unlock(&t->lock);

        clientid_allocator_complete(item->req, item->status, item->clientid);
        free(item);

        pthread_mutex_lock(&t->lock);
    }
    pthread_mutex_unlock(&t->lock);
    return NULL;
} /* clientid_test_worker */

static int
clientid_test_create(
    uint8_t       type,
    const json_t *kwargs,
    void        **state)
{
    struct clientid_test *t;
    json_t               *local;
    int                   rc;

    t = calloc(1, sizeof(*t));
    if (!t) {
        return ENOMEM;
    }
    local = clientid_test_local_kwargs(kwargs);
    if (!local) {
        free(t);
        return ENOMEM;
    }
    rc = clientid_local_create(type, local, &t->local);
    json_decref(local);
    if (rc) {
        free(t);
        return rc;
    }
    if (json_object_get(kwargs, "delay_us")) {
        t->delay_us = (uint64_t) json_integer_value(json_object_get(kwargs, "delay_us"));
    }
    pthread_mutex_init(&t->lock, NULL);
    pthread_cond_init(&t->cond, NULL);
    if (pthread_create(&t->worker, NULL, clientid_test_worker, t) != 0) {
        clientid_local_destroy(t->local);
        free(t);
        return ENOMEM;
    }
    *state = t;
    return 0;
} /* clientid_test_create */

static void
clientid_test_start(
    void                *state,
    struct clientid_req *req)
{
    struct clientid_test      *t    = state;
    struct clientid_test_item *item = calloc(1, sizeof(*item));

    if (!item) {
        clientid_allocator_complete(req, ENOMEM, 0);
        return;
    }
    item->req    = req;
    item->status = clientid_local_mint(t->local, &item->clientid);
    clock_gettime(CLOCK_REALTIME, &item->due);
    item->due.tv_sec  += (time_t) (t->delay_us / 1000000ULL);
    item->due.tv_nsec += (long) ((t->delay_us % 1000000ULL) * 1000ULL);
    if (item->due.tv_nsec >= 1000000000L) {
        item->due.tv_sec++;
        item->due.tv_nsec -= 1000000000L;
    }

    pthread_mutex_lock(&t->lock);
    if (t->tail) {
        t->tail->next = item;
    } else {
        t->head = item;
    }
    t->tail = item;
    pthread_cond_signal(&t->cond);
    pthread_mutex_unlock(&t->lock);
} /* clientid_test_start */

static void
clientid_test_destroy(void *state)
{
    struct clientid_test *t = state;

    pthread_mutex_lock(&t->lock);
    t->stop = true;
    pthread_cond_signal(&t->cond);
    pthread_mutex_unlock(&t->lock);
    pthread_join(t->worker, NULL);

    pthread_cond_destroy(&t->cond);
    pthread_mutex_destroy(&t->lock);
    clientid_local_destroy(t->local);
    free(t);
} /* clientid_test_destroy */

const struct clientid_backend_ops clientid_test_ops = {
    .name         = "test",
    .types        = clientid_test_types,
    .ntypes       = 1,
    .cross_thread = true,
    .secret_keys  = NULL,
    .supported    = clientid_test_supported,
    .create       = clientid_test_create,
    .start        = clientid_test_start,
    .destroy      = clientid_test_destroy,
};
