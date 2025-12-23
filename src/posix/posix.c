// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>

#include "posix_internal.h"
#include "evpl/evpl.h"

SYMBOL_EXPORT struct chimera_posix_client *
chimera_posix_init(
    const struct chimera_client_config *config,
    struct prometheus_metrics          *metrics)
{
    struct chimera_posix_client       *posix;
    struct chimera_client_config      *owned_config = NULL;
    const struct chimera_client_config *use_config;

    if (chimera_posix_global) {
        return chimera_posix_global;
    }

    posix = calloc(1, sizeof(*posix));

    if (!posix) {
        return NULL;
    }

    if (config) {
        use_config = config;
        posix->owns_config = 0;
    } else {
        owned_config = chimera_client_config_init();
        if (!owned_config) {
            free(posix);
            return NULL;
        }
        use_config = owned_config;
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
        pthread_mutex_destroy(&posix->fd_lock);
        free(posix->workers);
        chimera_destroy(posix->client);
        free(posix);
        return NULL;
    }

    chimera_posix_global = posix;

    return posix;
}

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
        free(posix->fds);
    }

    if (posix->workers) {
        free(posix->workers);
    }

    if (posix->client) {
        chimera_destroy(posix->client);
    }

    free(posix);
}
