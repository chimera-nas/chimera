// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
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

/* Populate the per-export access options (mode, squash, anon ids) into obj. */
static void
export_options_to_json(
    const struct chimera_nfs_export *export,
    json_t                          *obj)
{
    json_object_set_new(obj, "options",
                        json_string(chimera_nfs_export_get_options(export) &
                                    CHIMERA_NFS_EXPORT_OPT_RO ? "ro" : "rw"));
    switch (chimera_nfs_export_get_squash(export)) {
        case CHIMERA_NFS_SQUASH_ALL:
            json_object_set_new(obj, "squash", json_string("all"));
            break;
        case CHIMERA_NFS_SQUASH_NONE:
            json_object_set_new(obj, "squash", json_string("none"));
            break;
        default:
            json_object_set_new(obj, "squash", json_string("root"));
            break;
    } /* switch */
    json_object_set_new(obj, "anonuid",
                        json_integer(chimera_nfs_export_get_anonuid(export)));
    json_object_set_new(obj, "anongid",
                        json_integer(chimera_nfs_export_get_anongid(export)));
} /* export_options_to_json */

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
    export_options_to_json(export, obj);

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
    export_options_to_json(export, obj);

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

    if (chimera_server_get_export(thread->shared->server, name)) {
        json_decref(root);
        chimera_rest_send_error(evpl, request, 409, "Conflict",
                                "Export with that name already exists");
        return;
    }

    rc = chimera_server_create_export(thread->shared->server, name, path);

    if (rc != 0) {
        json_decref(root);
        chimera_rest_send_error(evpl, request, 500, "Internal Server Error",
                                "Failed to create export");
        return;
    }

    /* Apply optional access options.  create_export seeded secure defaults
     * (root_squash, rw, configured anon); seed from those and override only the
     * fields present in the request body. */
    {
        const struct chimera_nfs_export *created =
            chimera_server_get_export(thread->shared->server, name);
        const char                      *opt_s     = json_string_value(json_object_get(root, "options"));
        const char                      *squash_s  = json_string_value(json_object_get(root, "squash"));
        json_t                          *anonuid_j = json_object_get(root, "anonuid");
        json_t                          *anongid_j = json_object_get(root, "anongid");
        uint32_t                         options   = chimera_nfs_export_get_options(created);
        uint32_t                         squash    = chimera_nfs_export_get_squash(created);
        uint32_t                         anonuid   = chimera_nfs_export_get_anonuid(created);
        uint32_t                         anongid   = chimera_nfs_export_get_anongid(created);

        if (opt_s) {
            if (strcasecmp(opt_s, "ro") == 0) {
                options = CHIMERA_NFS_EXPORT_OPT_RO;
            } else if (strcasecmp(opt_s, "rw") == 0) {
                options = CHIMERA_NFS_EXPORT_OPT_RW;
            }
        }
        if (squash_s) {
            if (strcasecmp(squash_s, "none") == 0 ||
                strcasecmp(squash_s, "no_root_squash") == 0) {
                squash = CHIMERA_NFS_SQUASH_NONE;
            } else if (strcasecmp(squash_s, "all") == 0 ||
                       strcasecmp(squash_s, "all_squash") == 0) {
                squash = CHIMERA_NFS_SQUASH_ALL;
            } else if (strcasecmp(squash_s, "root") == 0 ||
                       strcasecmp(squash_s, "root_squash") == 0) {
                squash = CHIMERA_NFS_SQUASH_ROOT;
            }
        }
        if (anonuid_j) {
            anonuid = (uint32_t) json_integer_value(anonuid_j);
        }
        if (anongid_j) {
            anongid = (uint32_t) json_integer_value(anongid_j);
        }

        chimera_server_export_set_options(thread->shared->server, name, options,
                                          squash, anonuid, anongid);
    }

    json_decref(root);

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

    if (chimera_server_get_share(thread->shared->server, name)) {
        json_decref(root);
        chimera_rest_send_error(evpl, request, 409, "Conflict",
                                "Share with that name already exists");
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
    json_t                 *root;
    json_error_t            error;
    const char             *name;
    const char             *path;
    const struct s3_bucket *existing;
    int                     rc;
    json_t                 *obj;

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

    /* chimera_server_get_bucket holds a read lock until released, so release it
     * unconditionally before acting on the result. */
    existing = chimera_server_get_bucket(thread->shared->server, name);
    chimera_server_release_bucket(thread->shared->server);
    if (existing) {
        json_decref(root);
        chimera_rest_send_error(evpl, request, 409, "Conflict",
                                "Bucket with that name already exists");
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
