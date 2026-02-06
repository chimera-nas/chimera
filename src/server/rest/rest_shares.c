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
#include "server/nfs/nfs.h"
#include "server/smb/smb.h"
#include "server/s3/s3.h"
#include "rest_internal.h"

/* ======================== NFS Exports ======================== */

struct export_list_ctx {
    json_t *array;
};

static int
export_to_json_callback(
    const struct chimera_nfs_export *export,
    void                            *data)
{
    struct export_list_ctx *ctx = data;
    json_t                 *obj;

    obj = json_object();
    json_object_set_new(obj, "name",
                        json_string(chimera_nfs_export_get_name(export)));
    json_object_set_new(obj, "path",
                        json_string(chimera_nfs_export_get_path(export)));

    json_array_append_new(ctx->array, obj);

    return 0;
} /* export_to_json_callback */

void
chimera_rest_handle_exports_list(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread)
{
    struct export_list_ctx ctx;
    char                  *json_str;
    struct evpl_iovec      iov;
    int                    len;

    ctx.array = json_array();

    chimera_server_iterate_exports(thread->shared->server,
                                   export_to_json_callback, &ctx);

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
} /* chimera_rest_handle_exports_list */

void
chimera_rest_handle_exports_get(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *name)
{
    const struct chimera_nfs_export *export;
    json_t                          *obj;
    char                            *json_str;
    struct evpl_iovec                iov;
    int                              len;

    export = chimera_server_get_export(thread->shared->server, name);
    if (!export) {
        obj = json_object();
        json_object_set_new(obj, "error", json_string("Not Found"));
        json_object_set_new(obj, "message",
                            json_string("Export does not exist"));
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
    json_object_set_new(obj, "name",
                        json_string(chimera_nfs_export_get_name(export)));
    json_object_set_new(obj, "path",
                        json_string(chimera_nfs_export_get_path(export)));

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
} /* chimera_rest_handle_exports_get */

void
chimera_rest_handle_exports_create(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *body,
    int                         body_len)
{
    json_t           *root;
    json_error_t      error;
    const char       *name;
    const char       *path;
    int               rc;
    json_t           *obj;
    char             *json_str;
    struct evpl_iovec iov;
    int               len;

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

    name = json_string_value(json_object_get(root, "name"));
    path = json_string_value(json_object_get(root, "path"));

    if (!name || !path) {
        json_decref(root);
        obj = json_object();
        json_object_set_new(obj, "error", json_string("Bad Request"));
        json_object_set_new(obj, "message",
                            json_string("Missing required fields: name, path"));
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

    rc = chimera_server_create_export(thread->shared->server, name, path);

    json_decref(root);

    if (rc != 0) {
        obj = json_object();
        json_object_set_new(obj, "error", json_string("Internal Server Error"));
        json_object_set_new(obj, "message",
                            json_string("Failed to create export"));
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
    json_object_set_new(obj, "message", json_string("Export created"));
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
} /* chimera_rest_handle_exports_create */

void
chimera_rest_handle_exports_delete(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *name)
{
    int               rc;
    json_t           *obj;
    char             *json_str;
    struct evpl_iovec iov;
    int               len;

    rc = chimera_server_remove_export(thread->shared->server, name);

    if (rc != 0) {
        obj = json_object();
        json_object_set_new(obj, "error", json_string("Not Found"));
        json_object_set_new(obj, "message",
                            json_string("Export does not exist"));
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
} /* chimera_rest_handle_exports_delete */

/* ======================== SMB Shares ======================== */

struct share_list_ctx {
    json_t *array;
};

static int
share_to_json_callback(
    const struct chimera_smb_share *share,
    void                           *data)
{
    struct share_list_ctx *ctx = data;
    json_t                *obj;

    obj = json_object();
    json_object_set_new(obj, "name",
                        json_string(chimera_smb_share_get_name(share)));
    json_object_set_new(obj, "path",
                        json_string(chimera_smb_share_get_path(share)));

    json_array_append_new(ctx->array, obj);

    return 0;
} /* share_to_json_callback */

void
chimera_rest_handle_shares_list(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread)
{
    struct share_list_ctx ctx;
    char                 *json_str;
    struct evpl_iovec     iov;
    int                   len;

    ctx.array = json_array();

    chimera_server_iterate_shares(thread->shared->server,
                                  share_to_json_callback, &ctx);

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
} /* chimera_rest_handle_shares_list */

void
chimera_rest_handle_shares_get(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *name)
{
    const struct chimera_smb_share *share;
    json_t                         *obj;
    char                           *json_str;
    struct evpl_iovec               iov;
    int                             len;

    share = chimera_server_get_share(thread->shared->server, name);
    if (!share) {
        obj = json_object();
        json_object_set_new(obj, "error", json_string("Not Found"));
        json_object_set_new(obj, "message",
                            json_string("Share does not exist"));
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
    json_object_set_new(obj, "name",
                        json_string(chimera_smb_share_get_name(share)));
    json_object_set_new(obj, "path",
                        json_string(chimera_smb_share_get_path(share)));

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
} /* chimera_rest_handle_shares_get */

void
chimera_rest_handle_shares_create(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *body,
    int                         body_len)
{
    json_t           *root;
    json_error_t      error;
    const char       *name;
    const char       *path;
    int               rc;
    json_t           *obj;
    char             *json_str;
    struct evpl_iovec iov;
    int               len;

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

    name = json_string_value(json_object_get(root, "name"));
    path = json_string_value(json_object_get(root, "path"));

    if (!name || !path) {
        json_decref(root);
        obj = json_object();
        json_object_set_new(obj, "error", json_string("Bad Request"));
        json_object_set_new(obj, "message",
                            json_string("Missing required fields: name, path"));
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

    rc = chimera_server_create_share(thread->shared->server, name, path);

    json_decref(root);

    if (rc != 0) {
        obj = json_object();
        json_object_set_new(obj, "error", json_string("Internal Server Error"));
        json_object_set_new(obj, "message",
                            json_string("Failed to create share"));
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
    json_object_set_new(obj, "message", json_string("Share created"));
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
} /* chimera_rest_handle_shares_create */

void
chimera_rest_handle_shares_delete(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *name)
{
    int               rc;
    json_t           *obj;
    char             *json_str;
    struct evpl_iovec iov;
    int               len;

    rc = chimera_server_remove_share(thread->shared->server, name);

    if (rc != 0) {
        obj = json_object();
        json_object_set_new(obj, "error", json_string("Not Found"));
        json_object_set_new(obj, "message",
                            json_string("Share does not exist"));
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
} /* chimera_rest_handle_shares_delete */

/* ======================== S3 Buckets ======================== */

struct bucket_list_ctx {
    json_t *array;
};

static int
bucket_to_json_callback(
    const struct s3_bucket *bucket,
    void                   *data)
{
    struct bucket_list_ctx *ctx = data;
    json_t                 *obj;

    obj = json_object();
    json_object_set_new(obj, "name",
                        json_string(chimera_s3_bucket_get_name(bucket)));
    json_object_set_new(obj, "path",
                        json_string(chimera_s3_bucket_get_path(bucket)));

    json_array_append_new(ctx->array, obj);

    return 0;
} /* bucket_to_json_callback */

void
chimera_rest_handle_buckets_list(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread)
{
    struct bucket_list_ctx ctx;
    char                  *json_str;
    struct evpl_iovec      iov;
    int                    len;

    ctx.array = json_array();

    chimera_server_iterate_buckets(thread->shared->server,
                                   bucket_to_json_callback, &ctx);

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
} /* chimera_rest_handle_buckets_list */

void
chimera_rest_handle_buckets_get(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *name)
{
    const struct s3_bucket *bucket;
    json_t                 *obj;
    char                   *json_str;
    struct evpl_iovec       iov;
    int                     len;

    bucket = chimera_server_get_bucket(thread->shared->server, name);
    if (!bucket) {
        obj = json_object();
        json_object_set_new(obj, "error", json_string("Not Found"));
        json_object_set_new(obj, "message",
                            json_string("Bucket does not exist"));
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
    json_object_set_new(obj, "name",
                        json_string(chimera_s3_bucket_get_name(bucket)));
    json_object_set_new(obj, "path",
                        json_string(chimera_s3_bucket_get_path(bucket)));

    chimera_server_release_bucket(thread->shared->server);

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
} /* chimera_rest_handle_buckets_get */

void
chimera_rest_handle_buckets_create(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *body,
    int                         body_len)
{
    json_t           *root;
    json_error_t      error;
    const char       *name;
    const char       *path;
    int               rc;
    json_t           *obj;
    char             *json_str;
    struct evpl_iovec iov;
    int               len;

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

    name = json_string_value(json_object_get(root, "name"));
    path = json_string_value(json_object_get(root, "path"));

    if (!name || !path) {
        json_decref(root);
        obj = json_object();
        json_object_set_new(obj, "error", json_string("Bad Request"));
        json_object_set_new(obj, "message",
                            json_string("Missing required fields: name, path"));
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

    rc = chimera_server_create_bucket(thread->shared->server, name, path);

    json_decref(root);

    if (rc != 0) {
        obj = json_object();
        json_object_set_new(obj, "error", json_string("Internal Server Error"));
        json_object_set_new(obj, "message",
                            json_string("Failed to create bucket"));
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
    json_object_set_new(obj, "message", json_string("Bucket created"));
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
} /* chimera_rest_handle_buckets_create */

void
chimera_rest_handle_buckets_delete(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *name)
{
    int               rc;
    json_t           *obj;
    char             *json_str;
    struct evpl_iovec iov;
    int               len;

    rc = chimera_server_remove_bucket(thread->shared->server, name);

    if (rc != 0) {
        obj = json_object();
        json_object_set_new(obj, "error", json_string("Not Found"));
        json_object_set_new(obj, "message",
                            json_string("Bucket does not exist"));
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
} /* chimera_rest_handle_buckets_delete */
