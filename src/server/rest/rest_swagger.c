// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "evpl/evpl.h"
#include "evpl/evpl_http.h"
#include "rest_internal.h"

/* These are defined in the generated swagger_embedded.c */
extern const unsigned char swagger_index_html[];
extern const unsigned int  swagger_index_html_len;
extern const unsigned char swagger_ui_bundle_min_js[];
extern const unsigned int  swagger_ui_bundle_min_js_len;
extern const unsigned char swagger_ui_standalone_preset_min_js[];
extern const unsigned int  swagger_ui_standalone_preset_min_js_len;
extern const unsigned char swagger_ui_min_css[];
extern const unsigned int  swagger_ui_min_css_len;
extern const unsigned char openapi_json[];
extern const unsigned int  openapi_json_len;

void
chimera_rest_handle_swagger_ui(
    struct evpl              *evpl,
    struct evpl_http_request *request)
{
    struct evpl_iovec iov;

    evpl_iovec_alloc(evpl, swagger_index_html_len, 0, 1, 0, &iov);
    memcpy(evpl_iovec_data(&iov), swagger_index_html, swagger_index_html_len);
    evpl_iovec_set_length(&iov, swagger_index_html_len);

    evpl_http_request_add_header(request, "Content-Type",
                                 "text/html; charset=utf-8");
    evpl_http_request_add_datav(request, &iov, 1);
    evpl_http_server_set_response_length(request, swagger_index_html_len);
    evpl_http_server_dispatch_default(request, 200);
} /* chimera_rest_handle_swagger_ui */

void
chimera_rest_handle_swagger_bundle_js(
    struct evpl              *evpl,
    struct evpl_http_request *request)
{
    struct evpl_iovec iov;

    evpl_iovec_alloc(evpl, swagger_ui_bundle_min_js_len, 0, 1, 0, &iov);
    memcpy(evpl_iovec_data(&iov), swagger_ui_bundle_min_js,
           swagger_ui_bundle_min_js_len);
    evpl_iovec_set_length(&iov, swagger_ui_bundle_min_js_len);

    evpl_http_request_add_header(request, "Content-Type",
                                 "application/javascript");
    evpl_http_request_add_datav(request, &iov, 1);
    evpl_http_server_set_response_length(request, swagger_ui_bundle_min_js_len);
    evpl_http_server_dispatch_default(request, 200);
} /* chimera_rest_handle_swagger_bundle_js */

void
chimera_rest_handle_swagger_preset_js(
    struct evpl              *evpl,
    struct evpl_http_request *request)
{
    struct evpl_iovec iov;

    evpl_iovec_alloc(evpl, swagger_ui_standalone_preset_min_js_len, 0, 1, 0,
                     &iov);
    memcpy(evpl_iovec_data(&iov), swagger_ui_standalone_preset_min_js,
           swagger_ui_standalone_preset_min_js_len);
    evpl_iovec_set_length(&iov, swagger_ui_standalone_preset_min_js_len);

    evpl_http_request_add_header(request, "Content-Type",
                                 "application/javascript");
    evpl_http_request_add_datav(request, &iov, 1);
    evpl_http_server_set_response_length(request,
                                         swagger_ui_standalone_preset_min_js_len
                                         );
    evpl_http_server_dispatch_default(request, 200);
} /* chimera_rest_handle_swagger_preset_js */

void
chimera_rest_handle_swagger_css(
    struct evpl              *evpl,
    struct evpl_http_request *request)
{
    struct evpl_iovec iov;

    evpl_iovec_alloc(evpl, swagger_ui_min_css_len, 0, 1, 0, &iov);
    memcpy(evpl_iovec_data(&iov), swagger_ui_min_css, swagger_ui_min_css_len);
    evpl_iovec_set_length(&iov, swagger_ui_min_css_len);

    evpl_http_request_add_header(request, "Content-Type", "text/css");
    evpl_http_request_add_datav(request, &iov, 1);
    evpl_http_server_set_response_length(request, swagger_ui_min_css_len);
    evpl_http_server_dispatch_default(request, 200);
} /* chimera_rest_handle_swagger_css */

void
chimera_rest_handle_openapi_json(
    struct evpl              *evpl,
    struct evpl_http_request *request)
{
    struct evpl_iovec iov;

    evpl_iovec_alloc(evpl, openapi_json_len, 0, 1, 0, &iov);
    memcpy(evpl_iovec_data(&iov), openapi_json, openapi_json_len);
    evpl_iovec_set_length(&iov, openapi_json_len);

    evpl_http_request_add_header(request, "Content-Type", "application/json");
    evpl_http_request_add_datav(request, &iov, 1);
    evpl_http_server_set_response_length(request, openapi_json_len);
    evpl_http_server_dispatch_default(request, 200);
} /* chimera_rest_handle_openapi_json */
