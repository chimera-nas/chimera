// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#pragma once

#include <jansson.h>

#include "common/logging.h"

struct chimera_vfs_thread;
struct evpl;
struct evpl_http_request;

#define chimera_rest_debug(...) chimera_debug("rest", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_rest_info(...)  chimera_info("rest", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_rest_error(...) chimera_error("rest", __FILE__, __LINE__, __VA_ARGS__)
#define chimera_rest_abort(...) chimera_abort("rest", __FILE__, __LINE__, __VA_ARGS__)

struct chimera_rest_server {
    int                    http_port;
    int                    https_port;
    struct evpl_endpoint  *http_endpoint;
    struct evpl_endpoint  *https_endpoint;
    struct evpl_listener  *http_listener;
    struct evpl_listener  *https_listener;
    struct chimera_server *server;
    int                    debug_fsops;
    int                    auth_enabled;
    unsigned char          jwt_secret[32];
    int                    winbind_enabled;
    char                   winbind_domain[256];
};

struct evpl;
struct evpl_http_request;

struct chimera_rest_thread {
    struct evpl                *evpl;
    struct chimera_rest_server *shared;
    struct evpl_http_agent     *agent;
    struct evpl_http_server    *http_server;
    struct evpl_http_server    *https_server;
    struct chimera_vfs_thread  *vfs_thread;
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
    struct evpl              *evpl,
    struct evpl_http_request *request,
    int                       status,
    json_t                   *obj);

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
    struct evpl              *evpl,
    struct evpl_http_request *request,
    int                       status,
    const char               *error,
    const char               *message);

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
    struct evpl              *evpl,
    struct evpl_http_request *request,
    int                       status,
    const char               *json_body);
