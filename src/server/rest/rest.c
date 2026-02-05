// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "evpl/evpl.h"
#include "evpl/evpl_http.h"
#include "common/macros.h"
#include "server/server.h"
#include "rest.h"
#include "rest_internal.h"

/* External handlers from rest_users.c */
void chimera_rest_handle_users_list(
    struct evpl *,
    struct evpl_http_request *,
    struct chimera_rest_thread *);
void chimera_rest_handle_users_get(
    struct evpl *,
    struct evpl_http_request *,
    struct chimera_rest_thread *,
    const char *);
void chimera_rest_handle_users_create(
    struct evpl *,
    struct evpl_http_request *,
    struct chimera_rest_thread *,
    const char *,
    int);
void chimera_rest_handle_users_delete(
    struct evpl *,
    struct evpl_http_request *,
    struct chimera_rest_thread *,
    const char *);

/* External handlers from rest_shares.c */
void chimera_rest_handle_exports_list(
    struct evpl *,
    struct evpl_http_request *,
    struct chimera_rest_thread *);
void chimera_rest_handle_exports_get(
    struct evpl *,
    struct evpl_http_request *,
    struct chimera_rest_thread *,
    const char *);
void chimera_rest_handle_exports_create(
    struct evpl *,
    struct evpl_http_request *,
    struct chimera_rest_thread *,
    const char *,
    int);
void chimera_rest_handle_exports_delete(
    struct evpl *,
    struct evpl_http_request *,
    struct chimera_rest_thread *,
    const char *);

void chimera_rest_handle_shares_list(
    struct evpl *,
    struct evpl_http_request *,
    struct chimera_rest_thread *);
void chimera_rest_handle_shares_get(
    struct evpl *,
    struct evpl_http_request *,
    struct chimera_rest_thread *,
    const char *);
void chimera_rest_handle_shares_create(
    struct evpl *,
    struct evpl_http_request *,
    struct chimera_rest_thread *,
    const char *,
    int);
void chimera_rest_handle_shares_delete(
    struct evpl *,
    struct evpl_http_request *,
    struct chimera_rest_thread *,
    const char *);

void chimera_rest_handle_buckets_list(
    struct evpl *,
    struct evpl_http_request *,
    struct chimera_rest_thread *);
void chimera_rest_handle_buckets_get(
    struct evpl *,
    struct evpl_http_request *,
    struct chimera_rest_thread *,
    const char *);
void chimera_rest_handle_buckets_create(
    struct evpl *,
    struct evpl_http_request *,
    struct chimera_rest_thread *,
    const char *,
    int);
void chimera_rest_handle_buckets_delete(
    struct evpl *,
    struct evpl_http_request *,
    struct chimera_rest_thread *,
    const char *);

/* External handlers from rest_swagger.c */
void chimera_rest_handle_swagger_ui(
    struct evpl *,
    struct evpl_http_request *);
void chimera_rest_handle_swagger_bundle_js(
    struct evpl *,
    struct evpl_http_request *);
void chimera_rest_handle_swagger_preset_js(
    struct evpl *,
    struct evpl_http_request *);
void chimera_rest_handle_swagger_css(
    struct evpl *,
    struct evpl_http_request *);
void chimera_rest_handle_openapi_json(
    struct evpl *,
    struct evpl_http_request *);

static void
chimera_rest_notify(
    struct evpl                *evpl,
    struct evpl_http_agent     *agent,
    struct evpl_http_request   *request,
    enum evpl_http_notify_type  notify_type,
    enum evpl_http_request_type request_type,
    const char                 *uri,
    void                       *notify_data,
    void                       *private_data)
{
    /* REST API requests are simple request/response, no streaming needed */
} /* chimera_rest_notify */

static void
chimera_rest_send_json_response(
    struct evpl              *evpl,
    struct evpl_http_request *request,
    int                       status,
    const char               *json_body)
{
    struct evpl_iovec iov;
    int               len = strlen(json_body);

    evpl_iovec_alloc(evpl, len, 0, 1, 0, &iov);
    memcpy(evpl_iovec_data(&iov), json_body, len);
    evpl_iovec_set_length(&iov, len);

    evpl_http_request_add_header(request, "Content-Type", "application/json");
    evpl_http_request_add_datav(request, &iov, 1);
    evpl_http_server_set_response_length(request, len);
    evpl_http_server_dispatch_default(request, status);
} /* chimera_rest_send_json_response */

static void
chimera_rest_handle_version(
    struct evpl              *evpl,
    struct evpl_http_request *request)
{
    char json_response[256];

    snprintf(json_response, sizeof(json_response),
             "{\"version\":\"%s\"}", CHIMERA_VERSION);

    chimera_rest_send_json_response(evpl, request, 200, json_response);
} /* chimera_rest_handle_version */

static void
chimera_rest_handle_not_found(
    struct evpl              *evpl,
    struct evpl_http_request *request)
{
    chimera_rest_send_json_response(evpl, request, 404,
                                    "{\"error\":\"Not Found\"}");
} /* chimera_rest_handle_not_found */

static void
chimera_rest_handle_method_not_allowed(
    struct evpl              *evpl,
    struct evpl_http_request *request)
{
    chimera_rest_send_json_response(evpl, request, 405,
                                    "{\"error\":\"Method Not Allowed\"}");
} /* chimera_rest_handle_method_not_allowed */

static int
chimera_rest_url_starts_with(
    const char *url,
    int         url_len,
    const char *prefix,
    int         prefix_len)
{
    if (url_len < prefix_len) {
        return 0;
    }
    return strncmp(url, prefix, prefix_len) == 0;
} /* chimera_rest_url_starts_with */

static void
chimera_rest_extract_path_param(
    const char *url,
    int         url_len,
    int         prefix_len,
    char       *param,
    int         param_size)
{
    int remaining = url_len - prefix_len;
    int copy_len  = remaining < param_size - 1 ? remaining : param_size - 1;

    if (remaining > 0) {
        strncpy(param, url + prefix_len, copy_len);
        param[copy_len] = '\0';
    } else {
        param[0] = '\0';
    }
} /* chimera_rest_extract_path_param */

static void
chimera_rest_dispatch(
    struct evpl                 *evpl,
    struct evpl_http_agent      *agent,
    struct evpl_http_request    *request,
    evpl_http_notify_callback_t *notify_callback,
    void                       **notify_data,
    void                        *private_data)
{
    struct chimera_rest_thread *thread = private_data;
    const char                 *url;
    int                         url_len;
    enum evpl_http_request_type req_type;
    char                        param[256];

    *notify_callback = chimera_rest_notify;
    *notify_data     = NULL;

    url      = evpl_http_request_url(request, &url_len);
    req_type = evpl_http_request_type(request);

    chimera_rest_debug("REST API request: %s %.*s",
                       evpl_http_request_type_to_string(request),
                       url_len, url);

    /* Version endpoint */
    if (url_len == 8 && strncmp(url, "/version", 8) == 0) {
        if (req_type == EVPL_HTTP_REQUEST_TYPE_GET) {
            chimera_rest_handle_version(evpl, request);
        } else {
            chimera_rest_handle_method_not_allowed(evpl, request);
        }
        return;
    }

    /* OpenAPI spec */
    if (url_len == 17 && strncmp(url, "/api/openapi.json", 17) == 0) {
        if (req_type == EVPL_HTTP_REQUEST_TYPE_GET) {
            chimera_rest_handle_openapi_json(evpl, request);
        } else {
            chimera_rest_handle_method_not_allowed(evpl, request);
        }
        return;
    }

    /* Swagger UI */
    if (url_len == 9 && strncmp(url, "/api/docs", 9) == 0) {
        if (req_type == EVPL_HTTP_REQUEST_TYPE_GET) {
            chimera_rest_handle_swagger_ui(evpl, request);
        } else {
            chimera_rest_handle_method_not_allowed(evpl, request);
        }
        return;
    }

    if (url_len == 10 && strncmp(url, "/api/docs/", 10) == 0) {
        if (req_type == EVPL_HTTP_REQUEST_TYPE_GET) {
            chimera_rest_handle_swagger_ui(evpl, request);
        } else {
            chimera_rest_handle_method_not_allowed(evpl, request);
        }
        return;
    }

    if (chimera_rest_url_starts_with(url, url_len,
                                     "/api/docs/swagger-ui-bundle.min.js",
                                     34)) {
        if (req_type == EVPL_HTTP_REQUEST_TYPE_GET) {
            chimera_rest_handle_swagger_bundle_js(evpl, request);
        } else {
            chimera_rest_handle_method_not_allowed(evpl, request);
        }
        return;
    }

    if (chimera_rest_url_starts_with(url, url_len,
                                     "/api/docs/swagger-ui-standalone-preset.min.js",
                                     45)) {
        if (req_type == EVPL_HTTP_REQUEST_TYPE_GET) {
            chimera_rest_handle_swagger_preset_js(evpl, request);
        } else {
            chimera_rest_handle_method_not_allowed(evpl, request);
        }
        return;
    }

    if (chimera_rest_url_starts_with(url, url_len,
                                     "/api/docs/swagger-ui.min.css", 28)) {
        if (req_type == EVPL_HTTP_REQUEST_TYPE_GET) {
            chimera_rest_handle_swagger_css(evpl, request);
        } else {
            chimera_rest_handle_method_not_allowed(evpl, request);
        }
        return;
    }

    /* Users API: /api/v1/users */
    if (url_len == 13 && strncmp(url, "/api/v1/users", 13) == 0) {
        if (req_type == EVPL_HTTP_REQUEST_TYPE_GET) {
            chimera_rest_handle_users_list(evpl, request, thread);
        } else if (req_type == EVPL_HTTP_REQUEST_TYPE_POST) {
            chimera_rest_handle_users_create(evpl, request, thread, NULL, 0);
        } else {
            chimera_rest_handle_method_not_allowed(evpl, request);
        }
        return;
    }

    if (chimera_rest_url_starts_with(url, url_len, "/api/v1/users/", 14)) {
        chimera_rest_extract_path_param(url, url_len, 14, param, sizeof(param));
        if (param[0] != '\0') {
            if (req_type == EVPL_HTTP_REQUEST_TYPE_GET) {
                chimera_rest_handle_users_get(evpl, request, thread, param);
            } else if (req_type == EVPL_HTTP_REQUEST_TYPE_DELETE) {
                chimera_rest_handle_users_delete(evpl, request, thread, param);
            } else {
                chimera_rest_handle_method_not_allowed(evpl, request);
            }
            return;
        }
    }

    /* Exports API: /api/v1/exports */
    if (url_len == 15 && strncmp(url, "/api/v1/exports", 15) == 0) {
        if (req_type == EVPL_HTTP_REQUEST_TYPE_GET) {
            chimera_rest_handle_exports_list(evpl, request, thread);
        } else if (req_type == EVPL_HTTP_REQUEST_TYPE_POST) {
            chimera_rest_handle_exports_create(evpl, request, thread, NULL, 0);
        } else {
            chimera_rest_handle_method_not_allowed(evpl, request);
        }
        return;
    }

    if (chimera_rest_url_starts_with(url, url_len, "/api/v1/exports/", 16)) {
        chimera_rest_extract_path_param(url, url_len, 16, param, sizeof(param));
        if (param[0] != '\0') {
            if (req_type == EVPL_HTTP_REQUEST_TYPE_GET) {
                chimera_rest_handle_exports_get(evpl, request, thread, param);
            } else if (req_type == EVPL_HTTP_REQUEST_TYPE_DELETE) {
                chimera_rest_handle_exports_delete(evpl, request, thread,
                                                   param);
            } else {
                chimera_rest_handle_method_not_allowed(evpl, request);
            }
            return;
        }
    }

    /* Shares API: /api/v1/shares */
    if (url_len == 14 && strncmp(url, "/api/v1/shares", 14) == 0) {
        if (req_type == EVPL_HTTP_REQUEST_TYPE_GET) {
            chimera_rest_handle_shares_list(evpl, request, thread);
        } else if (req_type == EVPL_HTTP_REQUEST_TYPE_POST) {
            chimera_rest_handle_shares_create(evpl, request, thread, NULL, 0);
        } else {
            chimera_rest_handle_method_not_allowed(evpl, request);
        }
        return;
    }

    if (chimera_rest_url_starts_with(url, url_len, "/api/v1/shares/", 15)) {
        chimera_rest_extract_path_param(url, url_len, 15, param, sizeof(param));
        if (param[0] != '\0') {
            if (req_type == EVPL_HTTP_REQUEST_TYPE_GET) {
                chimera_rest_handle_shares_get(evpl, request, thread, param);
            } else if (req_type == EVPL_HTTP_REQUEST_TYPE_DELETE) {
                chimera_rest_handle_shares_delete(evpl, request, thread, param);
            } else {
                chimera_rest_handle_method_not_allowed(evpl, request);
            }
            return;
        }
    }

    /* Buckets API: /api/v1/buckets */
    if (url_len == 15 && strncmp(url, "/api/v1/buckets", 15) == 0) {
        if (req_type == EVPL_HTTP_REQUEST_TYPE_GET) {
            chimera_rest_handle_buckets_list(evpl, request, thread);
        } else if (req_type == EVPL_HTTP_REQUEST_TYPE_POST) {
            chimera_rest_handle_buckets_create(evpl, request, thread, NULL, 0);
        } else {
            chimera_rest_handle_method_not_allowed(evpl, request);
        }
        return;
    }

    if (chimera_rest_url_starts_with(url, url_len, "/api/v1/buckets/", 16)) {
        chimera_rest_extract_path_param(url, url_len, 16, param, sizeof(param));
        if (param[0] != '\0') {
            if (req_type == EVPL_HTTP_REQUEST_TYPE_GET) {
                chimera_rest_handle_buckets_get(evpl, request, thread, param);
            } else if (req_type == EVPL_HTTP_REQUEST_TYPE_DELETE) {
                chimera_rest_handle_buckets_delete(evpl, request, thread,
                                                   param);
            } else {
                chimera_rest_handle_method_not_allowed(evpl, request);
            }
            return;
        }
    }

    chimera_rest_handle_not_found(evpl, request);
} /* chimera_rest_dispatch */

SYMBOL_EXPORT struct chimera_rest_server *
chimera_rest_init(
    const struct chimera_server_config *config,
    struct chimera_server              *server,
    struct chimera_vfs                 *vfs,
    struct prometheus_metrics          *metrics)
{
    struct chimera_rest_server *rest;
    int                         http_port;

    http_port = chimera_server_config_get_rest_http_port(config);

    if (http_port == 0) {
        chimera_rest_info("REST API disabled (http_port=0)");
        return NULL;
    }

    rest = calloc(1, sizeof(*rest));

    rest->http_port = http_port;
    rest->server    = server;
    rest->endpoint  = evpl_endpoint_create("0.0.0.0", http_port);
    rest->listener  = evpl_listener_create();

    chimera_rest_info("REST API initialized on port %d", http_port);

    return rest;
} /* chimera_rest_init */ /* chimera_rest_init */

SYMBOL_EXPORT void
chimera_rest_start(struct chimera_rest_server *rest)
{
    if (!rest) {
        return;
    }

    evpl_listen(rest->listener, EVPL_STREAM_SOCKET_TCP, rest->endpoint);
    chimera_rest_info("REST API server started");
} /* chimera_rest_start */

SYMBOL_EXPORT void
chimera_rest_stop(struct chimera_rest_server *rest)
{
    if (!rest) {
        return;
    }

    evpl_listener_destroy(rest->listener);
    chimera_rest_info("REST API server stopped");
} /* chimera_rest_stop */

SYMBOL_EXPORT void
chimera_rest_destroy(struct chimera_rest_server *rest)
{
    if (!rest) {
        return;
    }

    evpl_endpoint_close(rest->endpoint);
    free(rest);
} /* chimera_rest_destroy */

SYMBOL_EXPORT void *
chimera_rest_thread_init(
    struct evpl                *evpl,
    struct chimera_rest_server *rest)
{
    struct chimera_rest_thread *thread;

    if (!rest) {
        return NULL;
    }

    thread = calloc(1, sizeof(*thread));

    thread->evpl   = evpl;
    thread->shared = rest;
    thread->agent  = evpl_http_init(evpl);
    thread->server = evpl_http_attach(thread->agent, rest->listener,
                                      chimera_rest_dispatch, thread);

    return thread;
} /* chimera_rest_thread_init */

SYMBOL_EXPORT void
chimera_rest_thread_destroy(void *data)
{
    struct chimera_rest_thread *thread = data;

    if (!thread) {
        return;
    }

    evpl_http_server_destroy(thread->agent, thread->server);
    evpl_http_destroy(thread->agent);
    free(thread);
} /* chimera_rest_thread_destroy */
