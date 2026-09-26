// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdlib.h>
#include <ctype.h>
#include <string.h>
#include <strings.h>
#include "common/macros.h"
#include "rest_internal.h"
#include "rest_auth.h"

static void
request_release(struct chimera_rest_request *request)
{
    struct chimera_rest_header *header;
    unsigned int                i;

    if (--request->refs) {
        return;
    }
    request->thread->live_requests--;
    for (i = 0; i < request->num_params; i++) {
        free(request->param_names[i]);
        free(request->param_values[i]);
    }
    while ((header = request->headers)) {
        request->headers = header->next;
        free(header->name);
        free(header->value);
        free(header);
    }
    free(request->path);
    free(request->query);
    free(request->method);
    free(request->subject);
    free(request->body);
    free(request->response_type);
    free(request->response_body);
    free(request);
} /* request_release */

static void
reply_ready(
    struct evpl          *evpl,
    struct evpl_doorbell *doorbell)
{
    struct chimera_rest_thread  *thread = container_of(doorbell, struct chimera_rest_thread, replies);
    struct chimera_rest_request *request, *next;
    struct evpl_iovec            iov;
    size_t                       length;

    pthread_mutex_lock(&thread->reply_lock);
    request            = thread->reply_head;
    thread->reply_head = thread->reply_tail = NULL;
    pthread_mutex_unlock(&thread->reply_lock);
    while (request) {
        next = request->next_reply;
        if (request->http) {
            length = request->response_length;
            if (request->response_status == 204 || request->response_status == 304) {
                length = 0;
            }
            if (request->response_type) {
                evpl_http_request_add_header(request->http, "Content-Type", request->response_type);
            }
            if (request->allow[0]) {
                evpl_http_request_add_header(request->http, "Allow", request->allow);
            }
            if (length && strcmp(request->method, "HEAD") != 0) {
                evpl_iovec_alloc(evpl, length, 0, 1, 0, &iov);
                memcpy(evpl_iovec_data(&iov), request->response_body, length);
                evpl_iovec_set_length(&iov, length);
                evpl_http_request_add_datav(request->http, &iov, 1);
            }
            evpl_http_server_set_response_length(request->http, length);
            evpl_http_server_dispatch_default(request->http, request->response_status);
        }
        request_release(request); /* handler's ownership */
        request = next;
    }
} /* reply_ready */

SYMBOL_EXPORT void
chimera_rest_reply(
    struct chimera_rest_request *request,
    uint32_t                     status,
    const char                  *content_type,
    const void                  *body,
    size_t                       length)
{
    struct chimera_rest_thread *thread = request->thread;

    if (status < 200 || status > 599 || (length && !body) ||
        (content_type && (strchr(content_type, '\r') || strchr(content_type, '\n')))) {
        status       = 500;
        length       = 0;
        content_type = NULL;
    }
    request->response_status = status;
    request->response_length = length;
    request->response_type   = content_type ? strdup(content_type) : NULL;
    if (length) {
        request->response_body = malloc(length);
        chimera_rest_abort_if(!request->response_body, "REST response allocation failed");
        memcpy(request->response_body, body, length);
    }
    /* The outstanding handler reference pins the queue and doorbell through
     * this entire critical section, including ringing from a worker thread. */
    pthread_mutex_lock(&thread->reply_lock);
    if (thread->reply_tail) {
        thread->reply_tail->next_reply = request;
    } else {
        thread->reply_head = request;
    }
    thread->reply_tail = request;
    evpl_ring_doorbell(&thread->replies);
    pthread_mutex_unlock(&thread->reply_lock);
} /* chimera_rest_reply */

static const char * request_method(const struct chimera_rest_request *r) { return r->method; }
static const char * request_path(const struct chimera_rest_request *r) { return r->path; }
static const char * request_query(const struct chimera_rest_request *r) { return r->query; }
static const char * request_subject(const struct chimera_rest_request *r) { return r->subject; }
static int32_t request_cancelled(const struct chimera_rest_request *r) { return atomic_load(&r->cancelled); }

static const char *
request_param(
    const struct chimera_rest_request *r,
    const char                        *name)
{
    unsigned int i;

    for (i = 0; i < r->num_params; i++) {
        if (!strcmp(name, r->param_names[i])) {
            return r->param_values[i];
        }
    }
    return NULL;
} /* request_param */

static const char *
request_header(
    const struct chimera_rest_request *r,
    const char                        *name)
{
    struct chimera_rest_header *header;

    for (header = r->headers; header; header = header->next) {
        if (!strcasecmp(name, header->name)) {
            return header->value;
        }
    }
    return NULL;
} /* request_header */

static const void *
request_body(
    const struct chimera_rest_request *r,
    size_t                            *length)
{
    *length = r->body_length;
    return r->body;
} /* request_body */

SYMBOL_EXPORT const struct chimera_rest_host chimera_rest_host = {
    .abi_version = CHIMERA_REST_ABI_VERSION,
    .struct_size = sizeof(struct chimera_rest_host),
    .method      = request_method,
    .path        = request_path,
    .param       = request_param,
    .query       = request_query,
    .header      = request_header,
    .subject     = request_subject,
    .body        = request_body,
    .cancelled   = request_cancelled,
    .reply       = chimera_rest_reply,
};

static void
copy_header(
    const char *name,
    const char *value,
    void       *data)
{
    struct chimera_rest_request *request = data;
    struct chimera_rest_header  *header;

    if (!strcasecmp(name, "Authorization") || !strcasecmp(name, "Proxy-Authorization")) {
        return;
    }
    header = calloc(1, sizeof(*header));
    chimera_rest_abort_if(!header, "REST header allocation failed");
    header->name     = strdup(name);
    header->value    = strdup(value);
    header->next     = request->headers;
    request->headers = header;
} /* copy_header */

static void
rest_notify(
    struct evpl                *evpl,
    struct evpl_http_agent     *agent,
    struct evpl_http_request   *http,
    enum evpl_http_notify_type  type,
    enum evpl_http_request_type method,
    const char                 *uri,
    void                       *data,
    void                       *private_data)
{
    struct chimera_rest_request *request = data;
    struct chimera_rest_thread  *thread  = request->thread;
    struct evpl_iovec            iov[256];
    uint64_t                     avail;
    int                          n, i;
    size_t                       limit = request->route ? request->route->max_body : 0;

    if (type == EVPL_HTTP_NOTIFY_FAILED || type == EVPL_HTTP_NOTIFY_RESPONSE_COMPLETE) {
        request->http = NULL;
        atomic_store(&request->cancelled, 1);
        request_release(request); /* transport ownership */
        return;
    }
    if (type != EVPL_HTTP_NOTIFY_RECEIVE_DATA && type != EVPL_HTTP_NOTIFY_RECEIVE_COMPLETE) {
        return;
    }
    while ((avail = evpl_http_request_get_data_avail(http))) {
        n = evpl_http_request_get_datav(evpl, http, iov, avail > 256 ? 256 : avail);
        for (i = 0; i < n; i++) {
            if (!request->error_status) {
                if (iov[i].length > limit - request->body_length) {
                    request->error_status = 413;
                } else {
                    memcpy(request->body + request->body_length, iov[i].data, iov[i].length);
                    request->body_length += iov[i].length;
                }
            }
            evpl_iovec_release(evpl, &iov[i]);
        }
    }
    if (type != EVPL_HTTP_NOTIFY_RECEIVE_COMPLETE) {
        return;
    }
    request->refs++; /* handed to exactly one handler/reply */
    if (thread->stopping) {
        request->error_status = 503;
    }
    if (request->error_status || !request->route) {
        if (!request->error_status) {
            request->error_status = 404;
        }
        chimera_rest_send_error(evpl, request, request->error_status,
                                request->error_status == 401 ? "Unauthorized" :
                                request->error_status == 404 ? "Not Found" :
                                request->error_status == 405 ? "Method Not Allowed" :
                                request->error_status == 413 ? "Payload Too Large" : "Bad Request",
                                "REST request rejected");
        return;
    }
    request->route->handle(request, thread->module_state[request->module_index]);
} /* rest_notify */

void
chimera_rest_dispatch(
    struct evpl                 *evpl,
    struct evpl_http_agent      *agent,
    struct evpl_http_request    *http,
    evpl_http_notify_callback_t *notify_callback,
    void                       **notify_data,
    void                        *private_data)
{
    struct chimera_rest_thread        *thread  = private_data;
    struct chimera_rest_request       *request = calloc(1, sizeof(*request));
    struct chimera_rest_loaded_module *loaded;
    const struct chimera_rest_route   *route;
    struct chimera_rest_jwt_claims     claims;
    const char                        *url, *query;
    uint32_t                           i, j;
    int                                length;
    size_t                             prefix_length;

    chimera_rest_abort_if(!request, "REST request allocation failed");
    request->thread = thread;
    request->http   = http;
    request->refs   = 1;
    atomic_init(&request->cancelled, 0);
    thread->live_requests++;
    *notify_callback = rest_notify;
    *notify_data     = request;
    url              = evpl_http_request_url(http, &length);
    request->method  = strdup(evpl_http_request_type_to_string(http));
    for (i = 0; request->method[i]; i++) {
        request->method[i] = toupper((unsigned char) request->method[i]);
    }
    if (length > CHIMERA_REST_MAX_URI) {
        request->error_status = 414;
        return;
    }
    request->path = strndup(url, length);
    query         = strchr(request->path, '?');
    if (query) {
        request->query                       = strdup(query + 1);
        request->path[query - request->path] = '\0';
    } else {
        request->query = strdup("");
    }
    evpl_http_request_header_iterate(http, copy_header, request);
    request->error_status = 404;
    for (i = 0; i < thread->shared->num_modules; i++) {
        loaded        = &thread->shared->modules[i];
        prefix_length = strlen(loaded->prefix);
        if (strncmp(request->path, loaded->prefix, prefix_length) ||
            (request->path[prefix_length] && request->path[prefix_length] != '/')) {
            continue;
        }
        for (j = 0; j < loaded->module->num_routes; j++) {
            route = loaded->module->routes[j];
            if (!chimera_rest_match(route->path, request->path + prefix_length, NULL)) {
                continue;
            }
            request->error_status = 405;
            if (strcmp(route->method, request->method)) {
                if (strlen(request->allow) + strlen(route->method) + 3 < sizeof(request->allow)) {
                    if (request->allow[0]) {
                        strcat(request->allow, ", ");
                    }
                    strcat(request->allow, route->method);
                }
                continue;
            }
            request->allow[0]     = '\0';
            request->route        = route;
            request->module_index = i;
            request->error_status = 0;
            if (!chimera_rest_match(route->path, request->path + prefix_length, request)) {
                request->error_status = 400;
                return;
            }
            if (!(route->flags & CHIMERA_REST_PUBLIC) && thread->shared->auth_enabled) {
                if (chimera_rest_auth_check_request(thread->shared, http, &claims)) {
                    request->error_status = 401;
                    return;
                }
                request->subject = strdup(claims.sub);
            }
            request->body = malloc(route->max_body + 1);
            chimera_rest_abort_if(!request->body, "REST body allocation failed");
            return;
        }
    }
} /* chimera_rest_dispatch */

void
chimera_rest_requests_init(struct chimera_rest_thread *thread)
{
    pthread_mutex_init(&thread->reply_lock, NULL);
    evpl_add_doorbell(thread->evpl, &thread->replies, reply_ready);
} /* chimera_rest_requests_init */

void
chimera_rest_requests_destroy(struct chimera_rest_thread *thread)
{
    /* Closing the HTTP servers detached every transport pointer. The handler
     * reference still pins its wrapper, thread and module through late replies. */
    while (thread->live_requests) {
        evpl_continue(thread->evpl);
    }
    evpl_remove_doorbell(thread->evpl, &thread->replies);
    pthread_mutex_destroy(&thread->reply_lock);
} /* chimera_rest_requests_destroy */
