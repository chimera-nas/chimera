// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only


#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <errno.h>
#include <jansson.h>

#include "evpl/evpl.h"
#include "evpl/evpl_http.h"
#include "server/server.h"
#include "server/nfs/nfs.h"
#include "rest_internal.h"


struct export_list_ctx {
    json_t *array;
};

/* Populate the per-export options (access mode, squash, anon ids) into obj. */
static void
export_options_to_json(
    const struct chimera_nfs_export *export,
    json_t                          *obj)
{
    json_object_set_new(obj, "access",
                        json_string(chimera_nfs_export_get_access(export) &
                                    CHIMERA_NFS_EXPORT_ACCESS_RO ? "ro" : "rw"));
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
    json_object_set_new(obj, "export_id",
                        json_integer(chimera_nfs_export_get_id(export)));
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
    json_object_set_new(obj, "export_id",
                        json_integer(chimera_nfs_export_get_id(export)));
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
    json_t      *exp_id_j;
    uint32_t     export_id = 0;
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

    /* Optional stable export id.  Validate the signed json value before any
     * unsigned cast so negatives and non-integers are rejected rather than
     * silently becoming auto-assignment (0) or wrapping into range. */
    exp_id_j = json_object_get(root, "export_id");
    if (exp_id_j) {
        json_int_t v = json_integer_value(exp_id_j);

        if (!json_is_integer(exp_id_j) ||
            v < 1 || v > CHIMERA_NFS_EXPORT_ID_MAX) {
            json_decref(root);
            chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                    "export_id must be an integer in range 1..65535");
            return;
        }
        export_id = (uint32_t) v;
    }

    /* The access mode field was renamed from "options" to "access".  Reject
     * the old key before creating anything: silently ignoring it would turn
     * a requested read-only export read-write. */
    if (json_object_get(root, "options")) {
        json_decref(root);
        chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                "\"options\" has been renamed to \"access\"");
        return;
    }

    if (chimera_server_get_export(thread->shared->server, name)) {
        json_decref(root);
        chimera_rest_send_error(evpl, request, 409, "Conflict",
                                "Export with that name already exists");
        return;
    }

    rc = chimera_server_create_export(thread->shared->server, name, path,
                                      export_id);

    if (rc == -EEXIST) {
        json_decref(root);
        chimera_rest_send_error(evpl, request, 409, "Conflict",
                                "export_id already in use");
        return;
    }

    if (rc == -EINVAL) {
        json_decref(root);
        chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                "export_id out of range");
        return;
    }

    if (rc == -ENOSPC) {
        json_decref(root);
        chimera_rest_send_error(evpl, request, 409, "Conflict",
                                "Export limit reached (server nfs_max_exports)");
        return;
    }

    if (rc != 0) {
        json_decref(root);
        chimera_rest_send_error(evpl, request, 500, "Internal Server Error",
                                "Failed to create export");
        return;
    }

    /* Apply optional export options.  create_export seeded the defaults
     * (rw, no squashing, configured anon); seed from those and override only
     * the fields present in the request body. */
    {
        const struct chimera_nfs_export *created =
            chimera_server_get_export(thread->shared->server, name);
        const char                      *access_s  = json_string_value(json_object_get(root, "access"));
        const char                      *squash_s  = json_string_value(json_object_get(root, "squash"));
        json_t                          *anonuid_j = json_object_get(root, "anonuid");
        json_t                          *anongid_j = json_object_get(root, "anongid");
        uint32_t                         access    = chimera_nfs_export_get_access(created);
        uint32_t                         squash    = chimera_nfs_export_get_squash(created);
        uint32_t                         anonuid   = chimera_nfs_export_get_anonuid(created);
        uint32_t                         anongid   = chimera_nfs_export_get_anongid(created);

        if (access_s) {
            if (strcasecmp(access_s, "ro") == 0) {
                access = CHIMERA_NFS_EXPORT_ACCESS_RO;
            } else if (strcasecmp(access_s, "rw") == 0) {
                access = CHIMERA_NFS_EXPORT_ACCESS_RW;
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

        chimera_server_export_set_options(thread->shared->server, name, access,
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
