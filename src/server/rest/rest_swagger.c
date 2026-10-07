// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <string.h>
#include "rest_internal.h"
#include "rest_services.h"

extern const unsigned char swagger_index_html[];
extern const unsigned int  swagger_index_html_len;

static void
docs_ui(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_reply(request, 200, "text/html; charset=utf-8", swagger_index_html, swagger_index_html_len);
} /* docs_ui */
extern const unsigned char swagger_ui_bundle_min_js[];
extern const unsigned int  swagger_ui_bundle_min_js_len;

static void
docs_bundle(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_reply(request, 200, "application/javascript", swagger_ui_bundle_min_js, swagger_ui_bundle_min_js_len);
} /* docs_bundle */
extern const unsigned char swagger_ui_standalone_preset_min_js[];
extern const unsigned int  swagger_ui_standalone_preset_min_js_len;

static void
docs_preset(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_reply(request, 200, "application/javascript", swagger_ui_standalone_preset_min_js,
                       swagger_ui_standalone_preset_min_js_len);
} /* docs_preset */
extern const unsigned char swagger_ui_min_css[];
extern const unsigned int  swagger_ui_min_css_len;

static void
docs_css(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_reply(request, 200, "text/css", swagger_ui_min_css, swagger_ui_min_css_len);
} /* docs_css */

static void
docs_openapi(
    struct chimera_rest_request *request,
    void                        *state)
{
    const char *body = request->thread->shared->openapi;

    chimera_rest_reply(request, 200, "application/json", body, strlen(body));
} /* docs_openapi */

static const struct chimera_rest_route        docs_route_ui = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "GET",
    .path        = "/",
    .flags       = CHIMERA_REST_PUBLIC,
    .handle      = docs_ui,
};

static const struct chimera_rest_route        docs_route_bundle = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "GET",
    .path        = "/swagger-ui-bundle.min.js",
    .flags       = CHIMERA_REST_PUBLIC,
    .handle      = docs_bundle,
};

static const struct chimera_rest_route        docs_route_preset = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "GET",
    .path        = "/swagger-ui-standalone-preset.min.js",
    .flags       = CHIMERA_REST_PUBLIC,
    .handle      = docs_preset,
};

static const struct chimera_rest_route        docs_route_css = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "GET",
    .path        = "/swagger-ui.min.css",
    .flags       = CHIMERA_REST_PUBLIC,
    .handle      = docs_css,
};

static const struct chimera_rest_route        docs_route_openapi = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "GET",
    .path        = "/openapi.json",
    .flags       = CHIMERA_REST_PUBLIC,
    .handle      = docs_openapi,
};

static const struct chimera_rest_route        docs_route_index = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "GET",
    .path        = "",
    .flags       = CHIMERA_REST_PUBLIC,
    .handle      = docs_ui,
};

static const struct chimera_rest_route *const docs_routes[] = {
    &docs_route_ui,
    &docs_route_bundle,
    &docs_route_preset,
    &docs_route_css,
    &docs_route_openapi,
    &docs_route_index,
};
const struct chimera_rest_module              chimera_rest_docs_module = {
    .abi_version = CHIMERA_REST_ABI_VERSION,
    .struct_size = sizeof(struct chimera_rest_module),
    .name        = "docs",
    .api_version = 1,
    .routes      = docs_routes,
    .num_routes  = sizeof(docs_routes) / sizeof(docs_routes[0]),
};

CHIMERA_REST_EXPORT const struct chimera_rest_module *
chimera_rest_module_get_v1(void)
{
    return &chimera_rest_docs_module;
} /* chimera_rest_module_get_v1 */
