// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* chimera_server_init: the server with the REST API as its management plane.
 * This is the one place the server and REST meet, in a library above both, so
 * neither of them links the other back. */

#include "server/server.h"
#include "server_mgmt.h"
#include "rest/rest.h"

static void *
chimera_server_rest_init(
    const struct chimera_server_config *config,
    struct chimera_server              *server,
    struct chimera_vfs                 *vfs,
    struct prometheus_metrics          *metrics)
{
    return chimera_rest_init(config, server, vfs, metrics);
} /* chimera_server_rest_init */

static void
chimera_server_rest_start(void *data)
{
    chimera_rest_start(data);
} /* chimera_server_rest_start */

static void
chimera_server_rest_stop(void *data)
{
    chimera_rest_stop(data);
} /* chimera_server_rest_stop */

static void
chimera_server_rest_destroy(void *data)
{
    chimera_rest_destroy(data);
} /* chimera_server_rest_destroy */

static void *
chimera_server_rest_thread_init(
    struct evpl               *evpl,
    void                      *data,
    struct chimera_vfs_thread *vfs_thread)
{
    return chimera_rest_thread_init(evpl, data, vfs_thread);
} /* chimera_server_rest_thread_init */

static const struct chimera_server_mgmt chimera_server_rest_mgmt = {
    .init           = chimera_server_rest_init,
    .start          = chimera_server_rest_start,
    .stop           = chimera_server_rest_stop,
    .destroy        = chimera_server_rest_destroy,
    .thread_init    = chimera_server_rest_thread_init,
    .thread_destroy = chimera_rest_thread_destroy,
};

SYMBOL_EXPORT struct chimera_server *
chimera_server_init(
    const struct chimera_server_config *config,
    struct prometheus_metrics          *metrics)
{
    return chimera_server_init_with_mgmt(config, metrics, &chimera_server_rest_mgmt);
} /* chimera_server_init */
