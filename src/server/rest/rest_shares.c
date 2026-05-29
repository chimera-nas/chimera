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

    ctx.array = json_array();

    chimera_server_iterate_exports(thread->shared->server,
                                   export_to_json_callback, &ctx);

    chimera_rest_send_json(evpl, request, 200, ctx.array);
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

    export = chimera_server_get_export(thread->shared->server, name);
    if (!export) {
        chimera_rest_send_error(evpl, request, 404, "Not Found",
                                "Export does not exist");
        return;
    }

    obj = json_object();
    json_object_set_new(obj, "name",
                        json_string(chimera_nfs_export_get_name(export)));
    json_object_set_new(obj, "path",
                        json_string(chimera_nfs_export_get_path(export)));

    chimera_rest_send_json(evpl, request, 200, obj);
} /* chimera_rest_handle_exports_get */

void
chimera_rest_handle_exports_create(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *body,
    int                         body_len)
{
    json_t      *root;
    json_error_t error;
    const char  *name;
    const char  *path;
    int          rc;
    json_t      *obj;

    root = json_loadb(body, body_len, 0, &error);
    if (!root) {
        chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                error.text);
        return;
    }

    name = json_string_value(json_object_get(root, "name"));
    path = json_string_value(json_object_get(root, "path"));

    if (!name || !path) {
        json_decref(root);
        chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                "Missing required fields: name, path");
        return;
    }

    rc = chimera_server_create_export(thread->shared->server, name, path);

    json_decref(root);

    if (rc != 0) {
        chimera_rest_send_error(evpl, request, 500, "Internal Server Error",
                                "Failed to create export");
        return;
    }

    obj = json_object();
    json_object_set_new(obj, "message", json_string("Export created"));
    chimera_rest_send_json(evpl, request, 201, obj);
} /* chimera_rest_handle_exports_create */

void
chimera_rest_handle_exports_delete(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *name)
{
    int rc;

    rc = chimera_server_remove_export(thread->shared->server, name);

    if (rc != 0) {
        chimera_rest_send_error(evpl, request, 404, "Not Found",
                                "Export does not exist");
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

    ctx.array = json_array();

    chimera_server_iterate_shares(thread->shared->server,
                                  share_to_json_callback, &ctx);

    chimera_rest_send_json(evpl, request, 200, ctx.array);
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

    share = chimera_server_get_share(thread->shared->server, name);
    if (!share) {
        chimera_rest_send_error(evpl, request, 404, "Not Found",
                                "Share does not exist");
        return;
    }

    obj = json_object();
    json_object_set_new(obj, "name",
                        json_string(chimera_smb_share_get_name(share)));
    json_object_set_new(obj, "path",
                        json_string(chimera_smb_share_get_path(share)));

    chimera_rest_send_json(evpl, request, 200, obj);
} /* chimera_rest_handle_shares_get */

void
chimera_rest_handle_shares_create(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *body,
    int                         body_len)
{
    json_t      *root;
    json_error_t error;
    const char  *name;
    const char  *path;
    int          rc;
    json_t      *obj;

    root = json_loadb(body, body_len, 0, &error);
    if (!root) {
        chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                error.text);
        return;
    }

    name = json_string_value(json_object_get(root, "name"));
    path = json_string_value(json_object_get(root, "path"));

    if (!name || !path) {
        json_decref(root);
        chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                "Missing required fields: name, path");
        return;
    }

    rc = chimera_server_create_share(thread->shared->server, name, path, 0);

    json_decref(root);

    if (rc != 0) {
        chimera_rest_send_error(evpl, request, 500, "Internal Server Error",
                                "Failed to create share");
        return;
    }

    obj = json_object();
    json_object_set_new(obj, "message", json_string("Share created"));
    chimera_rest_send_json(evpl, request, 201, obj);
} /* chimera_rest_handle_shares_create */

void
chimera_rest_handle_shares_delete(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *name)
{
    int rc;

    rc = chimera_server_remove_share(thread->shared->server, name);

    if (rc != 0) {
        chimera_rest_send_error(evpl, request, 404, "Not Found",
                                "Share does not exist");
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

    ctx.array = json_array();

    chimera_server_iterate_buckets(thread->shared->server,
                                   bucket_to_json_callback, &ctx);

    chimera_rest_send_json(evpl, request, 200, ctx.array);
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

    bucket = chimera_server_get_bucket(thread->shared->server, name);
    if (!bucket) {
        chimera_rest_send_error(evpl, request, 404, "Not Found",
                                "Bucket does not exist");
        return;
    }

    obj = json_object();
    json_object_set_new(obj, "name",
                        json_string(chimera_s3_bucket_get_name(bucket)));
    json_object_set_new(obj, "path",
                        json_string(chimera_s3_bucket_get_path(bucket)));

    chimera_server_release_bucket(thread->shared->server);

    chimera_rest_send_json(evpl, request, 200, obj);
} /* chimera_rest_handle_buckets_get */

void
chimera_rest_handle_buckets_create(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *body,
    int                         body_len)
{
    json_t      *root;
    json_error_t error;
    const char  *name;
    const char  *path;
    int          rc;
    json_t      *obj;

    root = json_loadb(body, body_len, 0, &error);
    if (!root) {
        chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                error.text);
        return;
    }

    name = json_string_value(json_object_get(root, "name"));
    path = json_string_value(json_object_get(root, "path"));

    if (!name || !path) {
        json_decref(root);
        chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                "Missing required fields: name, path");
        return;
    }

    rc = chimera_server_create_bucket(thread->shared->server, name, path);

    json_decref(root);

    if (rc != 0) {
        chimera_rest_send_error(evpl, request, 500, "Internal Server Error",
                                "Failed to create bucket");
        return;
    }

    obj = json_object();
    json_object_set_new(obj, "message", json_string("Bucket created"));
    chimera_rest_send_json(evpl, request, 201, obj);
} /* chimera_rest_handle_buckets_create */

void
chimera_rest_handle_buckets_delete(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *name)
{
    int rc;

    rc = chimera_server_remove_bucket(thread->shared->server, name);

    if (rc != 0) {
        chimera_rest_send_error(evpl, request, 404, "Not Found",
                                "Bucket does not exist");
        return;
    }

    evpl_http_server_dispatch_default(request, 204);
} /* chimera_rest_handle_buckets_delete */
