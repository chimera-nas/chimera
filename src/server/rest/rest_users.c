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

    ctx.array = json_array();

    chimera_server_iterate_users(thread->shared->server,
                                 user_to_json_callback, &ctx);

    chimera_rest_send_json(evpl, request, 200, ctx.array);
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
    uint32_t                       i;

    user = chimera_server_get_user(thread->shared->server, username);
    if (!user) {
        chimera_rest_send_error(evpl, request, 404, "Not Found",
                                "User does not exist");
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

    chimera_rest_send_json(evpl, request, 200, obj);
} /* chimera_rest_handle_users_get */

void
chimera_rest_handle_users_create(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *body,
    int                         body_len)
{
    json_t      *root;
    json_error_t error;
    const char  *username;
    const char  *password = NULL;
    const char  *smbpasswd_val;
    json_int_t   uid, gid;
    json_t      *gids_array;
    uint32_t     gids[64];
    uint32_t     ngids = 0;
    int          rc;
    json_t      *obj;
    size_t       i;

    root = json_loadb(body, body_len, 0, &error);
    if (!root) {
        chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                error.text);
        return;
    }

    username = json_string_value(json_object_get(root, "username"));
    if (!username) {
        json_decref(root);
        chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                "Missing required field: username");
        return;
    }

    if (chimera_server_get_user(thread->shared->server, username)) {
        json_decref(root);
        chimera_rest_send_error(evpl, request, 409, "Conflict",
                                "User with that username already exists");
        return;
    }

    password      = json_string_value(json_object_get(root, "password"));
    uid           = json_integer_value(json_object_get(root, "uid"));
    gid           = json_integer_value(json_object_get(root, "gid"));
    smbpasswd_val = json_string_value(json_object_get(root, "smbpasswd"));

    gids_array = json_object_get(root, "gids");
    if (gids_array && json_is_array(gids_array)) {
        ngids = json_array_size(gids_array);
        if (ngids > 64) {
            json_decref(root);
            chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                    "Too many supplementary groups (maximum 64)");
            return;
        }
        for (i = 0; i < ngids; i++) {
            gids[i] = json_integer_value(json_array_get(gids_array, i));
        }
    }

    rc = chimera_server_add_user(thread->shared->server, username,
                                 password, smbpasswd_val, NULL, uid, gid,
                                 ngids, gids, 1);

    json_decref(root);

    if (rc != 0) {
        chimera_rest_send_error(evpl, request, 500, "Internal Server Error",
                                "Failed to create user");
        return;
    }

    obj = json_object();
    json_object_set_new(obj, "message", json_string("User created"));
    chimera_rest_send_json(evpl, request, 201, obj);
} /* chimera_rest_handle_users_create */

void
chimera_rest_handle_users_delete(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *username)
{
    int rc;

    rc = chimera_server_remove_user(thread->shared->server, username);

    if (rc != 0) {
        chimera_rest_send_error(evpl, request, 404, "Not Found",
                                "User does not exist");
        return;
    }

    evpl_http_server_dispatch_default(request, 204);
} /* chimera_rest_handle_users_delete */
