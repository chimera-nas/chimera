// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <jansson.h>

#include "evpl/evpl.h"
#include "evpl/evpl_http.h"
#include "server/server.h"
#include "vfs/vfs.h"
#include "vfs/vfs_user_cache.h"
#include "rest_internal.h"

struct user_list_ctx {
    json_t *array;
};

static int
user_to_json_callback(
    const struct chimera_vfs_user *user,
    void                          *data)
{
    struct user_list_ctx *ctx = data;
    json_t               *obj;
    json_t               *gids_array;
    uint32_t              i;

    obj = json_object();
    json_object_set_new(obj, "username", json_string(user->username));
    json_object_set_new(obj, "uid", json_integer(user->uid));
    json_object_set_new(obj, "gid", json_integer(user->gid));
    json_object_set_new(obj, "pinned", json_boolean(user->pinned));

    gids_array = json_array();
    for (i = 0; i < user->ngids; i++) {
        json_array_append_new(gids_array, json_integer(user->gids[i]));
    }
    json_object_set_new(obj, "gids", gids_array);

    json_array_append_new(ctx->array, obj);

    return 0;
} /* user_to_json_callback */

void
chimera_rest_handle_users_list(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread)
{
    struct user_list_ctx ctx;
    char                *json_str;
    struct evpl_iovec    iov;
    int                  len;

    ctx.array = json_array();

    chimera_server_iterate_users(thread->shared->server,
                                 user_to_json_callback, &ctx);

    json_str = json_dumps(ctx.array, JSON_COMPACT);
    json_decref(ctx.array);

    len = strlen(json_str);
    evpl_iovec_alloc(evpl, len, 0, 1, 0, &iov);
    memcpy(evpl_iovec_data(&iov), json_str, len);
    evpl_iovec_set_length(&iov, len);
    free(json_str);

    evpl_http_request_add_header(request, "Content-Type", "application/json");
    evpl_http_request_add_datav(request, &iov, 1);
    evpl_http_server_set_response_length(request, len);
    evpl_http_server_dispatch_default(request, 200);
} /* chimera_rest_handle_users_list */

void
chimera_rest_handle_users_get(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *username)
{
    const struct chimera_vfs_user *user;
    json_t                        *obj;
    json_t                        *gids_array;
    char                          *json_str;
    struct evpl_iovec              iov;
    int                            len;
    uint32_t                       i;

    user = chimera_server_get_user(thread->shared->server, username);
    if (!user) {
        obj = json_object();
        json_object_set_new(obj, "error", json_string("Not Found"));
        json_object_set_new(obj, "message",
                            json_string("User does not exist"));
        json_str = json_dumps(obj, JSON_COMPACT);
        json_decref(obj);

        len = strlen(json_str);
        evpl_iovec_alloc(evpl, len, 0, 1, 0, &iov);
        memcpy(evpl_iovec_data(&iov), json_str, len);
        evpl_iovec_set_length(&iov, len);
        free(json_str);

        evpl_http_request_add_header(request, "Content-Type",
                                     "application/json");
        evpl_http_request_add_datav(request, &iov, 1);
        evpl_http_server_set_response_length(request, len);
        evpl_http_server_dispatch_default(request, 404);
        return;
    }

    obj = json_object();
    json_object_set_new(obj, "username", json_string(user->username));
    json_object_set_new(obj, "uid", json_integer(user->uid));
    json_object_set_new(obj, "gid", json_integer(user->gid));
    json_object_set_new(obj, "pinned", json_boolean(user->pinned));

    gids_array = json_array();
    for (i = 0; i < user->ngids; i++) {
        json_array_append_new(gids_array, json_integer(user->gids[i]));
    }
    json_object_set_new(obj, "gids", gids_array);

    json_str = json_dumps(obj, JSON_COMPACT);
    json_decref(obj);

    len = strlen(json_str);
    evpl_iovec_alloc(evpl, len, 0, 1, 0, &iov);
    memcpy(evpl_iovec_data(&iov), json_str, len);
    evpl_iovec_set_length(&iov, len);
    free(json_str);

    evpl_http_request_add_header(request, "Content-Type", "application/json");
    evpl_http_request_add_datav(request, &iov, 1);
    evpl_http_server_set_response_length(request, len);
    evpl_http_server_dispatch_default(request, 200);
} /* chimera_rest_handle_users_get */

void
chimera_rest_handle_users_create(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *body,
    int                         body_len)
{
    json_t           *root;
    json_error_t      error;
    const char       *username;
    const char       *password = NULL;
    json_int_t        uid, gid;
    json_t           *gids_array;
    uint32_t          gids[64];
    uint32_t          ngids = 0;
    int               rc;
    json_t           *obj;
    char             *json_str;
    struct evpl_iovec iov;
    int               len;
    size_t            i;

    root = json_loadb(body, body_len, 0, &error);
    if (!root) {
        obj = json_object();
        json_object_set_new(obj, "error", json_string("Bad Request"));
        json_object_set_new(obj, "message", json_string(error.text));
        json_str = json_dumps(obj, JSON_COMPACT);
        json_decref(obj);

        len = strlen(json_str);
        evpl_iovec_alloc(evpl, len, 0, 1, 0, &iov);
        memcpy(evpl_iovec_data(&iov), json_str, len);
        evpl_iovec_set_length(&iov, len);
        free(json_str);

        evpl_http_request_add_header(request, "Content-Type",
                                     "application/json");
        evpl_http_request_add_datav(request, &iov, 1);
        evpl_http_server_set_response_length(request, len);
        evpl_http_server_dispatch_default(request, 400);
        return;
    }

    username = json_string_value(json_object_get(root, "username"));
    if (!username) {
        json_decref(root);
        obj = json_object();
        json_object_set_new(obj, "error", json_string("Bad Request"));
        json_object_set_new(obj, "message",
                            json_string("Missing required field: username"));
        json_str = json_dumps(obj, JSON_COMPACT);
        json_decref(obj);

        len = strlen(json_str);
        evpl_iovec_alloc(evpl, len, 0, 1, 0, &iov);
        memcpy(evpl_iovec_data(&iov), json_str, len);
        evpl_iovec_set_length(&iov, len);
        free(json_str);

        evpl_http_request_add_header(request, "Content-Type",
                                     "application/json");
        evpl_http_request_add_datav(request, &iov, 1);
        evpl_http_server_set_response_length(request, len);
        evpl_http_server_dispatch_default(request, 400);
        return;
    }

    password = json_string_value(json_object_get(root, "password"));
    uid      = json_integer_value(json_object_get(root, "uid"));
    gid      = json_integer_value(json_object_get(root, "gid"));

    gids_array = json_object_get(root, "gids");
    if (gids_array && json_is_array(gids_array)) {
        ngids = json_array_size(gids_array);
        if (ngids > 64) {
            ngids = 64;
        }
        for (i = 0; i < ngids; i++) {
            gids[i] = json_integer_value(json_array_get(gids_array, i));
        }
    }

    rc = chimera_server_add_user(thread->shared->server, username, password,
                                 NULL, uid, gid, ngids, gids, 1);

    json_decref(root);

    if (rc != 0) {
        obj = json_object();
        json_object_set_new(obj, "error", json_string("Internal Server Error"));
        json_object_set_new(obj, "message",
                            json_string("Failed to create user"));
        json_str = json_dumps(obj, JSON_COMPACT);
        json_decref(obj);

        len = strlen(json_str);
        evpl_iovec_alloc(evpl, len, 0, 1, 0, &iov);
        memcpy(evpl_iovec_data(&iov), json_str, len);
        evpl_iovec_set_length(&iov, len);
        free(json_str);

        evpl_http_request_add_header(request, "Content-Type",
                                     "application/json");
        evpl_http_request_add_datav(request, &iov, 1);
        evpl_http_server_set_response_length(request, len);
        evpl_http_server_dispatch_default(request, 500);
        return;
    }

    obj = json_object();
    json_object_set_new(obj, "message", json_string("User created"));
    json_str = json_dumps(obj, JSON_COMPACT);
    json_decref(obj);

    len = strlen(json_str);
    evpl_iovec_alloc(evpl, len, 0, 1, 0, &iov);
    memcpy(evpl_iovec_data(&iov), json_str, len);
    evpl_iovec_set_length(&iov, len);
    free(json_str);

    evpl_http_request_add_header(request, "Content-Type", "application/json");
    evpl_http_request_add_datav(request, &iov, 1);
    evpl_http_server_set_response_length(request, len);
    evpl_http_server_dispatch_default(request, 201);
} /* chimera_rest_handle_users_create */

void
chimera_rest_handle_users_delete(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *username)
{
    int               rc;
    json_t           *obj;
    char             *json_str;
    struct evpl_iovec iov;
    int               len;

    rc = chimera_server_remove_user(thread->shared->server, username);

    if (rc != 0) {
        obj = json_object();
        json_object_set_new(obj, "error", json_string("Not Found"));
        json_object_set_new(obj, "message",
                            json_string("User does not exist"));
        json_str = json_dumps(obj, JSON_COMPACT);
        json_decref(obj);

        len = strlen(json_str);
        evpl_iovec_alloc(evpl, len, 0, 1, 0, &iov);
        memcpy(evpl_iovec_data(&iov), json_str, len);
        evpl_iovec_set_length(&iov, len);
        free(json_str);

        evpl_http_request_add_header(request, "Content-Type",
                                     "application/json");
        evpl_http_request_add_datav(request, &iov, 1);
        evpl_http_server_set_response_length(request, len);
        evpl_http_server_dispatch_default(request, 404);
        return;
    }

    evpl_http_server_dispatch_default(request, 204);
} /* chimera_rest_handle_users_delete */
