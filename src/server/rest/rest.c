// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "evpl/evpl.h"
#include "evpl/evpl_http.h"
#include "common/macros.h"
#include "common/tcp_flavor.h"
#include "server/server.h"
#include "rest.h"
#include "rest_internal.h"
#include "rest_auth.h"

SYMBOL_EXPORT void
chimera_rest_send_json(
    struct evpl                 *evpl,
    struct chimera_rest_request *request,
    int                          status,
    json_t                      *obj)
{
    char *body = json_dumps(obj, JSON_COMPACT);

    json_decref(obj);
    if (!body) {
        chimera_rest_reply(request, 500, "application/json", "{}", 2);
        return;
    }
    chimera_rest_reply(request, status, "application/json", body, strlen(body));
    free(body);
} /* chimera_rest_send_json */

SYMBOL_EXPORT void
chimera_rest_send_error(
    struct evpl                 *evpl,
    struct chimera_rest_request *request,
    int                          status,
    const char                  *error,
    const char                  *message)
{
    json_t *obj = json_object();

    json_object_set_new(obj, "error", json_string(error));
    json_object_set_new(obj, "message", json_string(message));
    chimera_rest_send_json(evpl, request, status, obj);
} /* chimera_rest_send_error */

SYMBOL_EXPORT void
chimera_rest_send_json_response(
    struct evpl                 *evpl,
    struct chimera_rest_request *request,
    int                          status,
    const char                  *body)
{
    chimera_rest_reply(request, status, "application/json", body, strlen(body));
} /* chimera_rest_send_json_response */

SYMBOL_EXPORT struct chimera_rest_server *
chimera_rest_init(
    const struct chimera_server_config *config,
    struct chimera_server              *server,
    struct chimera_vfs                 *vfs,
    struct prometheus_metrics          *metrics)
{
    struct chimera_rest_server *rest;
    int                         http_port;
    int                         https_port;
    int                         module_count;

    http_port  = chimera_server_config_get_rest_http_port(config);
    https_port = chimera_server_config_get_rest_https_port(config);

    chimera_server_config_get_rest_modules(config, &module_count);
    if (http_port == 0 && https_port == 0) {
        chimera_rest_abort_if(module_count, "REST modules configured without a REST listener");
        chimera_rest_info("REST API disabled (no ports configured)");
        return NULL;
    }

    rest = calloc(1, sizeof(*rest));

    rest->http_port    = http_port;
    rest->https_port   = https_port;
    rest->flavor       = chimera_server_config_get_tcp_flavor(config);
    rest->server       = server;
    rest->auth_enabled = chimera_server_config_get_rest_auth_enabled(config);

    chimera_rest_modules_init(rest, config);

    chimera_rest_auth_init_secret(rest,
                                  chimera_server_config_get_state_dir(config));

    rest->winbind_enabled = chimera_server_config_get_smb_winbind_enabled(
        config);
    {
        const char *domain = chimera_server_config_get_smb_winbind_domain(
            config);
        if (domain) {
            strncpy(rest->winbind_domain, domain,
                    sizeof(rest->winbind_domain) - 1);
            rest->winbind_domain[sizeof(rest->winbind_domain) - 1] = '\0';
        }
    }

    if (http_port != 0) {
        /* The plain-HTTP listener follows the server's configured transport
         * flavor, so an in-process deployment (tests) reaches the whole REST
         * API over inproc without binding a port.  TLS has no inproc form, so
         * the HTTPS listener below stays a real socket regardless. */
        rest->http_endpoint = chimera_tcp_flavor_endpoint_create(rest->flavor,
                                                                 "0.0.0.0",
                                                                 http_port);
        rest->http_listener = evpl_listener_create();
        chimera_rest_info("REST API HTTP initialized on port %d", http_port);
    }

    if (https_port != 0) {
        rest->https_endpoint = evpl_endpoint_create("0.0.0.0", https_port);
        rest->https_listener = evpl_listener_create();
        chimera_rest_info("REST API HTTPS initialized on port %d", https_port);
    }

    return rest;
} /* chimera_rest_init */

SYMBOL_EXPORT void
chimera_rest_start(struct chimera_rest_server *rest)
{
    int rc;

    if (!rest) {
        return;
    }

    if (rest->http_listener) {
        rc = evpl_listen(rest->http_listener,
                         chimera_tcp_flavor_to_protocol(rest->flavor),
                         rest->http_endpoint);
        chimera_rest_abort_if(rc, "failed to listen for REST HTTP on port %d",
                              rest->http_port);
        chimera_rest_info("REST API HTTP server started on port %d",
                          rest->http_port);
    }

    if (rest->https_listener) {
        rc = evpl_listen(rest->https_listener, EVPL_STREAM_SOCKET_TLS,
                         rest->https_endpoint);
        chimera_rest_abort_if(rc, "failed to listen for REST HTTPS on port %d",
                              rest->https_port);
        chimera_rest_info("REST API HTTPS server started on port %d",
                          rest->https_port);
    }
} /* chimera_rest_start */

SYMBOL_EXPORT void
chimera_rest_stop(struct chimera_rest_server *rest)
{
    if (!rest) {
        return;
    }

    if (rest->http_listener) {
        evpl_listener_destroy(rest->http_listener);
    }

    if (rest->https_listener) {
        evpl_listener_destroy(rest->https_listener);
    }

    chimera_rest_info("REST API server stopped");
} /* chimera_rest_stop */

SYMBOL_EXPORT void
chimera_rest_destroy(struct chimera_rest_server *rest)
{
    if (!rest) {
        return;
    }

    if (rest->http_endpoint) {
        evpl_endpoint_close(rest->http_endpoint);
    }

    if (rest->https_endpoint) {
        evpl_endpoint_close(rest->https_endpoint);
    }

    chimera_rest_modules_destroy(rest);
    free(rest);
} /* chimera_rest_destroy */

SYMBOL_EXPORT void *
chimera_rest_thread_init(
    struct evpl                *evpl,
    struct chimera_rest_server *rest,
    struct chimera_vfs_thread  *vfs_thread)
{
    struct chimera_rest_thread *thread;

    if (!rest) {
        return NULL;
    }

    thread = calloc(1, sizeof(*thread));

    thread->evpl       = evpl;
    thread->shared     = rest;
    thread->vfs_thread = vfs_thread;
    thread->agent      = evpl_http_init(evpl);
    chimera_rest_requests_init(thread);
    chimera_rest_modules_thread_init(thread);

    if (rest->http_listener) {
        thread->http_server = evpl_http_attach(thread->agent, rest->http_listener,
                                               chimera_rest_dispatch, thread);
    }

    if (rest->https_listener) {
        thread->https_server = evpl_http_attach(thread->agent, rest->https_listener,
                                                chimera_rest_dispatch, thread);
    }

    return thread;
} /* chimera_rest_thread_init */

SYMBOL_EXPORT void
chimera_rest_thread_destroy(void *data)
{
    struct chimera_rest_thread *thread = data;

    if (!thread) {
        return;
    }

    chimera_rest_modules_thread_quiesce(thread);

    if (thread->http_server) {
        evpl_http_server_destroy(thread->agent, thread->http_server);
    }

    if (thread->https_server) {
        evpl_http_server_destroy(thread->agent, thread->https_server);
    }

    chimera_rest_requests_destroy(thread);
    chimera_rest_modules_thread_destroy(thread);
    evpl_http_destroy(thread->agent);
    free(thread);
} /* chimera_rest_thread_destroy */
