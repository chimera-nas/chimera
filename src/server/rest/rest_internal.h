// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <jansson.h>
#include <pthread.h>
#include <stdatomic.h>
#include "evpl/evpl.h"
#include "evpl/evpl_http.h"
#include "sdk/chimera_rest_sdk.h"

#include "common/logging.h"
#include "vfs/sdk/vfs_tcp_flavor.h"

struct chimera_server_config;
struct chimera_vfs_thread;
struct chimera_nfs_export;
struct evpl;
struct evpl_http_request;

#define chimera_rest_debug(...) chimera_debug("rest", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_rest_info(...)  chimera_info("rest", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_rest_error(...) chimera_error("rest", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_rest_abort(...) chimera_abort("rest", __FILE__, __LINE__, __VA_ARGS__)

#define chimera_rest_abort_if(cond, ...) \
        chimera_abort_if(cond, "rest", \
                         __FILE__, \
                         __LINE__, \
                         __VA_ARGS__)

struct chimera_rest_server {
    int                                http_port;
    int                                https_port;
    /* Transport flavor for the plain-HTTP listener (server.tcp_flavor).
     * inproc lets a test drive the API with no port bound. */
    enum chimera_tcp_flavor            flavor;
    struct evpl_endpoint              *http_endpoint;
    struct evpl_endpoint              *https_endpoint;
    struct evpl_listener              *http_listener;
    struct evpl_listener              *https_listener;
    struct chimera_server             *server;
    struct chimera_rest_loaded_module *modules;
    uint32_t                           num_modules;
    char                              *openapi;
    int                                auth_enabled;
    unsigned char                      jwt_secret[32];
    int                                winbind_enabled;
    char                               winbind_domain[256];
};

struct evpl;
struct evpl_http_request;

struct chimera_rest_thread {
    struct evpl                 *evpl;
    struct chimera_rest_server  *shared;
    struct evpl_http_agent      *agent;
    struct evpl_http_server     *http_server;
    struct evpl_http_server     *https_server;
    struct chimera_vfs_thread   *vfs_thread;
    void                       **module_state;
    struct evpl_doorbell         replies;
    pthread_mutex_t              reply_lock;
    struct chimera_rest_request *reply_head;
    struct chimera_rest_request *reply_tail;
    unsigned int                 live_requests;
    int                          stopping;
};

/**
 * Serialize a JSON object as a compact response body and dispatch it.
 *
 * Takes ownership of @obj and releases it (json_decref) before returning.
 *
 * @param evpl    Event loop for this thread
 * @param request HTTP request being responded to
 * @param status  HTTP status code to dispatch
 * @param obj     JSON object to serialize as the response body
 */
void
chimera_rest_send_json(
    struct evpl                 *evpl,
    struct chimera_rest_request *request,
    int                          status,
    json_t                      *obj);

/**
 * Dispatch a standard JSON error response of the form
 * {"error": <error>, "message": <message>}.
 *
 * @param evpl    Event loop for this thread
 * @param request HTTP request being responded to
 * @param status  HTTP status code to dispatch
 * @param error   Short error label (e.g. "Bad Request")
 * @param message Human-readable error description
 */
void
chimera_rest_send_error(
    struct evpl                 *evpl,
    struct chimera_rest_request *request,
    int                          status,
    const char                  *error,
    const char                  *message);

/**
 * Dispatch a pre-serialized JSON string as the response body.
 *
 * @param evpl      Event loop for this thread
 * @param request   HTTP request being responded to
 * @param status    HTTP status code to dispatch
 * @param json_body NUL-terminated JSON string to send as the body
 */
void
chimera_rest_send_json_response(
    struct evpl                 *evpl,
    struct chimera_rest_request *request,
    int                          status,
    const char                  *json_body);

/**
 * Populate every export field except the name (path, export_id, access
 * mode, squash, anon ids, and the sec restriction when one is set) into a
 * JSON object.  Shared between the export handlers (rest_exports.c), which
 * add a "name" field, and the config serializer (rest_config.c), which keys
 * its entries by name, so the two representations cannot drift apart.
 *
 * @param export Export to serialize
 * @param obj    JSON object to add the fields to
 */
void
chimera_rest_export_options_to_json(
    const struct chimera_nfs_export *export,
    json_t *obj);


#define CHIMERA_REST_MAX_PARAMS 8
#define CHIMERA_REST_MAX_URI    4096

struct chimera_rest_context {
    struct chimera_rest_thread *thread;
};

struct chimera_rest_loaded_module {
    const struct chimera_rest_module *module;
    void                             *handle;
    void                             *state;
    char                              prefix[96];
};

struct chimera_rest_header {
    char                       *name;
    char                       *value;
    struct chimera_rest_header *next;
};

struct chimera_rest_request {
    struct chimera_rest_thread      *thread;
    struct evpl_http_request        *http;
    const struct chimera_rest_route *route;
    uint32_t                         module_index;
    unsigned int                     refs;
    atomic_int                       cancelled;
    int                              error_status;
    char                             allow[64];
    char                            *path;
    char                            *query;
    char                            *method;
    char                            *subject;
    char                            *param_names[CHIMERA_REST_MAX_PARAMS];
    char                            *param_values[CHIMERA_REST_MAX_PARAMS];
    unsigned int                     num_params;
    struct chimera_rest_header      *headers;
    unsigned char                   *body;
    size_t                           body_length;
    uint32_t                         response_status;
    char                            *response_type;
    void                            *response_body;
    size_t                           response_length;
    struct chimera_rest_request     *next_reply;
};

extern const struct chimera_rest_host chimera_rest_host;

void chimera_rest_reply(
    struct chimera_rest_request *request,
    uint32_t                     status,
    const char                  *content_type,
    const void                  *body,
    size_t                       length);
void chimera_rest_dispatch(
    struct evpl                 *evpl,
    struct evpl_http_agent      *agent,
    struct evpl_http_request    *request,
    evpl_http_notify_callback_t *notify_callback,
    void                       **notify_data,
    void                        *private_data);
void chimera_rest_requests_init(
    struct chimera_rest_thread *thread);
void chimera_rest_requests_destroy(
    struct chimera_rest_thread *thread);
int chimera_rest_match(
    const char                  *pattern,
    const char                  *path,
    struct chimera_rest_request *request);
void chimera_rest_modules_init(
    struct chimera_rest_server         *rest,
    const struct chimera_server_config *config);
void chimera_rest_modules_destroy(
    struct chimera_rest_server *rest);
void chimera_rest_modules_thread_init(
    struct chimera_rest_thread *thread);
void chimera_rest_modules_thread_quiesce(
    struct chimera_rest_thread *thread);
void chimera_rest_modules_thread_destroy(
    struct chimera_rest_thread *thread);
