// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <string.h>
#include "rest_handlers.h"
#include "rest_auth.h"
#include "rest_services.h"

static void
core_version(
    struct chimera_rest_request *request,
    void                        *state)
{
    char body[256];

    snprintf(body, sizeof(body), "{\"version\":\"%s\"}", CHIMERA_VERSION);
    chimera_rest_reply(request, 200, "application/json", body, strlen(body));
} /* core_version */

static void
core_users_list(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_users_list(request->thread->evpl, request, request->thread);
} /* core_users_list */

static void
core_users_create(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_users_create(request->thread->evpl, request, request->thread, (const char *) request->body,
                                     request->body_length);
} /* core_users_create */

static void
core_users_get(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_users_get(request->thread->evpl, request, request->thread, chimera_rest_host.param(request,
                                                                                                           "username"));
} /* core_users_get */

static void
core_users_delete(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_users_delete(request->thread->evpl, request, request->thread, chimera_rest_host.param(request,
                                                                                                              "username"));
} /* core_users_delete */

static void
core_exports_list(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_exports_list(request->thread->evpl, request, request->thread);
} /* core_exports_list */

static void
core_exports_create(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_exports_create(request->thread->evpl, request, request->thread, (const char *) request->body,
                                       request->body_length);
} /* core_exports_create */

static void
core_exports_get(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_exports_get(request->thread->evpl, request, request->thread, chimera_rest_host.param(request,
                                                                                                             "name"));
} /* core_exports_get */

static void
core_exports_delete(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_exports_delete(request->thread->evpl, request, request->thread, chimera_rest_host.param(request,
                                                                                                                "name"))
    ;
} /* core_exports_delete */

static void
core_shares_list(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_shares_list(request->thread->evpl, request, request->thread);
} /* core_shares_list */

static void
core_shares_create(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_shares_create(request->thread->evpl, request, request->thread, (const char *) request->body,
                                      request->body_length);
} /* core_shares_create */

static void
core_shares_get(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_shares_get(request->thread->evpl, request, request->thread, chimera_rest_host.param(request,
                                                                                                            "name"));
} /* core_shares_get */

static void
core_shares_delete(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_shares_delete(request->thread->evpl, request, request->thread, chimera_rest_host.param(request,
                                                                                                               "name"));
} /* core_shares_delete */

static void
core_buckets_list(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_buckets_list(request->thread->evpl, request, request->thread);
} /* core_buckets_list */

static void
core_buckets_create(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_buckets_create(request->thread->evpl, request, request->thread, (const char *) request->body,
                                       request->body_length);
} /* core_buckets_create */

static void
core_buckets_get(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_buckets_get(request->thread->evpl, request, request->thread, chimera_rest_host.param(request,
                                                                                                             "name"));
} /* core_buckets_get */

static void
core_buckets_delete(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_buckets_delete(request->thread->evpl, request, request->thread, chimera_rest_host.param(request,
                                                                                                                "name"))
    ;
} /* core_buckets_delete */

static void
core_mounts_list(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_mounts_list(request->thread->evpl, request, request->thread);
} /* core_mounts_list */

static void
core_mounts_create(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_mounts_create(request->thread->evpl, request, request->thread, (const char *) request->body,
                                      request->body_length);
} /* core_mounts_create */

static void
core_mounts_get(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_mounts_get(request->thread->evpl, request, request->thread, chimera_rest_host.param(request,
                                                                                                            "name"));
} /* core_mounts_get */

static void
core_mounts_delete(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_mounts_delete(request->thread->evpl, request, request->thread, chimera_rest_host.param(request,
                                                                                                               "name"));
} /* core_mounts_delete */

static void
core_config(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_config(request->thread->evpl, request, request->thread);
} /* core_config */

static void
core_login(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_auth_login(request->thread->evpl, request, request->thread, (const char *) request->body,
                                   request->body_length);
} /* core_login */

static void
core_filesystems_create(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_filesystems_create(request->thread->evpl, request, request->thread, (const char *) request->body
                                           , request->body_length);
} /* core_filesystems_create */

static void
core_filesystems_delete(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_filesystems_delete(request->thread->evpl, request, request->thread, chimera_rest_host.param(
                                               request, "name"));
} /* core_filesystems_delete */

static const struct chimera_rest_route        route_version = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "GET",
    .path        = "/version",
    .flags       = CHIMERA_REST_PUBLIC,
    .max_body    = 0,
    .handle      = core_version,
};

static const struct chimera_rest_route        route_users_list = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "GET",
    .path        = "/users",
    .flags       = 0,
    .max_body    = 0,
    .handle      = core_users_list,
};

static const struct chimera_rest_route        route_users_create = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "POST",
    .path        = "/users",
    .flags       = 0,
    .max_body    = 65536,
    .handle      = core_users_create,
};

static const struct chimera_rest_route        route_users_get = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "GET",
    .path        = "/users/{username}", .
    flags        = 0,
    .max_body    = 0,
    .handle      = core_users_get,
};

static const struct chimera_rest_route        route_users_delete = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "DELETE",
    .path        = "/users/{username}", .
    flags        = 0,
    .max_body    = 0,
    .handle      = core_users_delete,
};

static const struct chimera_rest_route        route_exports_list = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "GET",
    .path        = "/exports",        .
    flags        = 0,
    .max_body    = 0,
    .handle      = core_exports_list,
};

static const struct chimera_rest_route        route_exports_create = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "POST",
    .path        = "/exports",
    .flags       = 0,
    .max_body    = 65536,
    .handle      = core_exports_create,
};

static const struct chimera_rest_route        route_exports_get = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "GET",
    .path        = "/exports/{name...}", .
    flags        = 0,
    .max_body    = 0,
    .handle      = core_exports_get,
};

static const struct chimera_rest_route        route_exports_delete = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "DELETE",
    .path        = "/exports/{name...}", .
    flags        = 0,
    .max_body    = 0,
    .handle      = core_exports_delete,
};

static const struct chimera_rest_route        route_shares_list = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "GET",
    .path        = "/shares",
    .flags       = 0,
    .max_body    = 0,
    .handle      = core_shares_list,
};

static const struct chimera_rest_route        route_shares_create = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "POST",
    .path        = "/shares",
    .flags       = 0,
    .max_body    = 65536,
    .handle      = core_shares_create,
};

static const struct chimera_rest_route        route_shares_get = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "GET",
    .path        = "/shares/{name...}", .
    flags        = 0,
    .max_body    = 0,
    .handle      = core_shares_get,
};

static const struct chimera_rest_route        route_shares_delete = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "DELETE",
    .path        = "/shares/{name...}", .
    flags        = 0,
    .max_body    = 0,
    .handle      = core_shares_delete,
};

static const struct chimera_rest_route        route_buckets_list = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "GET",
    .path        = "/buckets",        .
    flags        = 0,
    .max_body    = 0,
    .handle      = core_buckets_list,
};

static const struct chimera_rest_route        route_buckets_create = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "POST",
    .path        = "/buckets",
    .flags       = 0,
    .max_body    = 65536,
    .handle      = core_buckets_create,
};

static const struct chimera_rest_route        route_buckets_get = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "GET",
    .path        = "/buckets/{name...}", .
    flags        = 0,
    .max_body    = 0,
    .handle      = core_buckets_get,
};

static const struct chimera_rest_route        route_buckets_delete = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "DELETE",
    .path        = "/buckets/{name...}", .
    flags        = 0,
    .max_body    = 0,
    .handle      = core_buckets_delete,
};

static const struct chimera_rest_route        route_mounts_list = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "GET",
    .path        = "/mounts",
    .flags       = 0,
    .max_body    = 0,
    .handle      = core_mounts_list,
};

static const struct chimera_rest_route        route_mounts_create = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "POST",
    .path        = "/mounts",
    .flags       = 0,
    .max_body    = 65536,
    .handle      = core_mounts_create,
};

static const struct chimera_rest_route        route_mounts_get = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "GET",
    .path        = "/mounts/{name...}", .
    flags        = 0,
    .max_body    = 0,
    .handle      = core_mounts_get,
};

static const struct chimera_rest_route        route_mounts_delete = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "DELETE",
    .path        = "/mounts/{name...}", .
    flags        = 0,
    .max_body    = 0,
    .handle      = core_mounts_delete,
};

static const struct chimera_rest_route        route_config = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "GET",
    .path        = "/config",
    .flags       = 0,
    .max_body    = 0,
    .handle      = core_config,
};

static const struct chimera_rest_route        route_login = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "POST",
    .path        = "/auth/login",
    .flags       = CHIMERA_REST_PUBLIC,
    .max_body    = 65536,
    .handle      = core_login,
};

static const struct chimera_rest_route        route_filesystems_create = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "POST",
    .path        = "/filesystems",          .
    flags        = 0,
    .max_body    = 65536,
    .handle      = core_filesystems_create,
};

static const struct chimera_rest_route        route_filesystems_delete = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "DELETE",
    .path        = "/filesystems/{name...}",
    .flags       = 0,
    .max_body    = 0,
    .handle      = core_filesystems_delete,
};

static const struct chimera_rest_route *const core_routes[] = {
    &route_version,
    &route_users_list,
    &route_users_create,
    &route_users_get,
    &route_users_delete,
    &route_exports_list,
    &route_exports_create,
    &route_exports_get,
    &route_exports_delete,
    &route_shares_list,
    &route_shares_create,
    &route_shares_get,
    &route_shares_delete,
    &route_buckets_list,
    &route_buckets_create,
    &route_buckets_get,
    &route_buckets_delete,
    &route_mounts_list,
    &route_mounts_create,
    &route_mounts_get,
    &route_mounts_delete,
    &route_config,
    &route_login,
    &route_filesystems_create,
    &route_filesystems_delete,
};

const struct chimera_rest_module              chimera_rest_core_module = {
    .abi_version = CHIMERA_REST_ABI_VERSION,
    .struct_size = sizeof(struct chimera_rest_module),
    .name        = "core",
    .api_version = 1,
    .routes      = core_routes,
    .num_routes  = sizeof(core_routes) / sizeof
        (core_routes[0]),
};

CHIMERA_REST_EXPORT const struct chimera_rest_module *
chimera_rest_module_get_v1(void)
{
    return &chimera_rest_core_module;
} /* chimera_rest_module_get_v1 */
