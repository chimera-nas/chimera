// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>

#include "posix_internal.h"
#include "evpl/evpl.h"
#include "vfs/vfs.h"

struct chimera_posix_client *chimera_posix_global;

void *
chimera_posix_worker_init(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_posix_client *posix  = private_data;
    int                          idx    = atomic_fetch_add(&posix->init_cursor, 1);
    struct chimera_posix_worker *worker = &posix->workers[idx];

    worker->parent = posix;
    worker->index  = idx;
    worker->evpl   = evpl;

    pthread_mutex_init(&worker->lock, NULL);
    evpl_add_doorbell(evpl, &worker->doorbell, chimera_posix_worker_doorbell);

    worker->client_thread = chimera_client_thread_init(evpl, posix->client);

    return worker;
} /* chimera_posix_worker_init */

void
chimera_posix_worker_shutdown(
    struct evpl *evpl,
    void        *private_data)
{
    struct chimera_posix_worker *worker = private_data;

    if (worker->client_thread) {
        chimera_client_thread_shutdown(evpl, worker->client_thread);
    }

    evpl_remove_doorbell(evpl, &worker->doorbell);
    pthread_mutex_destroy(&worker->lock);
} /* chimera_posix_worker_shutdown */

void
chimera_posix_worker_doorbell(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_posix_worker *worker = container_of(doorbell, struct chimera_posix_worker, doorbell);

    for (;;) {
        struct chimera_client_request *request;

        pthread_mutex_lock(&worker->lock);
        request = worker->pending_requests;
        if (request) {
            DL_DELETE(worker->pending_requests, request);
        }
        pthread_mutex_unlock(&worker->lock);

        if (!request) {
            break;
        }

        request->thread = worker->client_thread;
        request->sync_callback(worker->client_thread, request);
    }
} /* chimera_posix_worker_doorbell */

SYMBOL_EXPORT struct chimera_posix_client *
chimera_posix_init(
    const struct chimera_client_config *config,
    struct prometheus_metrics          *metrics)
{
    struct chimera_posix_client        *posix;
    struct chimera_client_config       *owned_config = NULL;
    const struct chimera_client_config *use_config;

    if (chimera_posix_global) {
        return chimera_posix_global;
    }

    posix = calloc(1, sizeof(*posix));

    if (!posix) {
        return NULL;
    }

    if (config) {
        use_config         = config;
        posix->owns_config = 0;
    } else {
        owned_config = chimera_client_config_init();
        if (!owned_config) {
            free(posix);
            return NULL;
        }
        use_config         = owned_config;
        posix->owns_config = 1;
    }

    posix->client = chimera_client_init(use_config, metrics);

    if (!posix->client) {
        if (owned_config) {
            free(owned_config);
        }
        free(posix);
        return NULL;
    }

    posix->nworkers = use_config->core_threads;
    posix->workers  = calloc(posix->nworkers, sizeof(*posix->workers));

    if (!posix->workers) {
        chimera_destroy(posix->client);
        free(posix);
        return NULL;
    }

    posix->max_fds = use_config->max_fds;
    posix->fds     = calloc(posix->max_fds, sizeof(*posix->fds));

    if (!posix->fds) {
        free(posix->workers);
        chimera_destroy(posix->client);
        free(posix);
        return NULL;
    }

    for (int i = 0; i < posix->max_fds; i++) {
        pthread_mutex_init(&posix->fds[i].lock, NULL);
        pthread_cond_init(&posix->fds[i].cond, NULL);
        posix->fds[i].handle        = NULL;
        posix->fds[i].offset        = 0;
        posix->fds[i].flags         = CHIMERA_POSIX_FD_CLOSED;
        posix->fds[i].refcnt        = 0;
        posix->fds[i].io_waiters    = 0;
        posix->fds[i].pending_close = 0;
        posix->fds[i].close_waiters = 0;

        if (i >= 3) {
            posix->fds[i].next = posix->free_list;
            posix->free_list   = &posix->fds[i];
        } else {
            posix->fds[i].next = NULL;
        }
    }

    pthread_mutex_init(&posix->fd_lock, NULL);
    atomic_init(&posix->next_worker, 0);
    atomic_init(&posix->init_cursor, 0);

    posix->pool = evpl_threadpool_create(
        NULL,
        posix->nworkers,
        chimera_posix_worker_init,
        chimera_posix_worker_shutdown,
        posix);

    if (!posix->pool) {
        for (int i = 0; i < posix->max_fds; i++) {
            pthread_mutex_destroy(&posix->fds[i].lock);
            pthread_cond_destroy(&posix->fds[i].cond);
        }
        free(posix->fds);
        pthread_mutex_destroy(&posix->fd_lock);
        free(posix->workers);
        chimera_destroy(posix->client);
        free(posix);
        return NULL;
    }

    chimera_posix_global = posix;

    return posix;
} /* chimera_posix_init */

SYMBOL_EXPORT void
chimera_posix_shutdown(void)
{
    struct chimera_posix_client *posix = chimera_posix_global;

    if (!posix) {
        return;
    }

    chimera_posix_global = NULL;

    if (posix->pool) {
        evpl_threadpool_destroy(posix->pool);
    }

    pthread_mutex_destroy(&posix->fd_lock);

    if (posix->fds) {
        for (int i = 0; i < posix->max_fds; i++) {
            pthread_mutex_destroy(&posix->fds[i].lock);
            pthread_cond_destroy(&posix->fds[i].cond);
        }
        free(posix->fds);
    }

    if (posix->workers) {
        free(posix->workers);
    }

    if (posix->client) {
        chimera_destroy(posix->client);
    }

    free(posix);
} /* chimera_posix_shutdown */
