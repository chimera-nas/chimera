// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only


#include <stdio.h>
#include <stdint.h>
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

/* Populate every export field except the name (path, export_id, access
 * mode, squash, anon ids, sec) into obj.  Shared with the config serializer
 * (rest_config.c), whose entries are keyed by export name, so the export
 * objects returned by /api/v1/exports and the exports section of
 * /api/v1/config cannot drift apart. */
void
chimera_rest_export_options_to_json(
    const struct chimera_nfs_export *export,
    json_t                          *obj)
{
    uint32_t sec_mask = chimera_nfs_export_get_sec(export);

    json_object_set_new(obj, "path",
                        json_string(chimera_nfs_export_get_path(export)));
    json_object_set_new(obj, "export_id",
                        json_integer(chimera_nfs_export_get_id(export)));
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

    /* Absent (mask 0) means any flavor is permitted, matching the config
     * file semantics; emit the restriction only when one is set. */
    if (sec_mask) {
        json_t *sec = json_array();

        if (sec_mask & CHIMERA_NFS_SEC_SYS) {
            json_array_append_new(sec, json_string("sys"));
        }
        if (sec_mask & CHIMERA_NFS_SEC_KRB5) {
            json_array_append_new(sec, json_string("krb5"));
        }
        if (sec_mask & CHIMERA_NFS_SEC_KRB5I) {
            json_array_append_new(sec, json_string("krb5i"));
        }
        if (sec_mask & CHIMERA_NFS_SEC_KRB5P) {
            json_array_append_new(sec, json_string("krb5p"));
        }
        json_object_set_new(obj, "sec", sec);
    }
} /* chimera_rest_export_options_to_json */

static int
export_to_json_callback(
    const struct chimera_nfs_export *export,
    void                            *data)
{
    struct export_list_ctx *ctx = data;
    json_t                 *obj = json_object();

    json_object_set_new(obj, "name",
                        json_string(chimera_nfs_export_get_name(export)));
    chimera_rest_export_options_to_json(export, obj);

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
    chimera_rest_export_options_to_json(export, obj);

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
    json_t                        *root;
    json_error_t                   error;
    const char                    *name;
    const char                    *path;
    json_t                        *exp_id_j;
    json_t                        *access_j;
    json_t                        *squash_j;
    json_t                        *anonuid_j;
    json_t                        *anongid_j;
    json_t                        *sec_j;
    uint32_t                       export_id = 0;
    struct chimera_nfs_export_opts opts      = { 0 };
    int                            rc;
    json_t                        *obj;

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

    /* Validate the optional export options before creating anything:
     * an unrecognized value silently falling back to the defaults would turn
     * a requested read-only or squashed export into an open one, and a
     * non-integer anonuid/anongid would map squashed callers to uid/gid 0. */
    access_j = json_object_get(root, "access");
    if (access_j) {
        const char *s = json_string_value(access_j);

        opts.has_access = 1;
        if (s && strcasecmp(s, "ro") == 0) {
            opts.access = CHIMERA_NFS_EXPORT_ACCESS_RO;
        } else if (s && strcasecmp(s, "rw") == 0) {
            opts.access = CHIMERA_NFS_EXPORT_ACCESS_RW;
        } else {
            json_decref(root);
            chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                    "access must be \"ro\" or \"rw\"");
            return;
        }
    }

    squash_j = json_object_get(root, "squash");
    if (squash_j) {
        const char *s = json_string_value(squash_j);

        opts.has_squash = 1;
        if (s && (strcasecmp(s, "none") == 0 ||
                  strcasecmp(s, "no_root_squash") == 0)) {
            opts.squash = CHIMERA_NFS_SQUASH_NONE;
        } else if (s && (strcasecmp(s, "all") == 0 ||
                         strcasecmp(s, "all_squash") == 0)) {
            opts.squash = CHIMERA_NFS_SQUASH_ALL;
        } else if (s && (strcasecmp(s, "root") == 0 ||
                         strcasecmp(s, "root_squash") == 0)) {
            opts.squash = CHIMERA_NFS_SQUASH_ROOT;
        } else {
            json_decref(root);
            chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                    "squash must be \"none\", \"root\", or \"all\"");
            return;
        }
    }

    anonuid_j = json_object_get(root, "anonuid");
    if (anonuid_j) {
        json_int_t v = json_integer_value(anonuid_j);

        if (!json_is_integer(anonuid_j) || v < 0 || v > UINT32_MAX) {
            json_decref(root);
            chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                    "anonuid must be an integer in range 0..4294967295");
            return;
        }
        opts.has_anonuid = 1;
        opts.anonuid     = (uint32_t) v;
    }

    anongid_j = json_object_get(root, "anongid");
    if (anongid_j) {
        json_int_t v = json_integer_value(anongid_j);

        if (!json_is_integer(anongid_j) || v < 0 || v > UINT32_MAX) {
            json_decref(root);
            chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                    "anongid must be an integer in range 0..4294967295");
            return;
        }
        opts.has_anongid = 1;
        opts.anongid     = (uint32_t) v;
    }

    /* Allowed security flavors: an optional array of
     * "sys"/"krb5"/"krb5i"/"krb5p" strings, matching the config file.  An
     * absent key or empty array permits any flavor.  Every malformed shape
     * is rejected: a typo'd flavor, non-string entry, or non-array value
     * silently dropped would leave the mask at 0 -- "any flavor allowed" --
     * turning a requested Kerberos-only export wide open to AUTH_SYS. */
    sec_j = json_object_get(root, "sec");
    if (sec_j) {
        size_t  si;
        json_t *flavor_j;

        if (!json_is_array(sec_j)) {
            json_decref(root);
            chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                    "sec must be an array of flavor strings");
            return;
        }

        opts.has_sec = 1;
        json_array_foreach(sec_j, si, flavor_j)
        {
            const char *f = json_string_value(flavor_j);

            if (f && (strcasecmp(f, "sys") == 0 ||
                      strcasecmp(f, "auth_sys") == 0)) {
                opts.sec_allowed |= CHIMERA_NFS_SEC_SYS;
            } else if (f && strcasecmp(f, "krb5") == 0) {
                opts.sec_allowed |= CHIMERA_NFS_SEC_KRB5;
            } else if (f && strcasecmp(f, "krb5i") == 0) {
                opts.sec_allowed |= CHIMERA_NFS_SEC_KRB5I;
            } else if (f && strcasecmp(f, "krb5p") == 0) {
                opts.sec_allowed |= CHIMERA_NFS_SEC_KRB5P;
            } else {
                json_decref(root);
                chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                        "sec flavors must be \"sys\", \"krb5\", "
                                        "\"krb5i\", or \"krb5p\"");
                return;
            }
        }
    }

    /* Create the export with the validated options applied atomically, so it
     * is never live half-configured.  Name and id uniqueness are decided
     * inside create_export under its lock: a handler-side check-then-create
     * would race a concurrent create/delete of the same name (REST handlers
     * run one per core thread). */
    rc = chimera_server_create_export(thread->shared->server, name, path,
                                      export_id, &opts);

    json_decref(root);

    if (rc == -EEXIST) {
        chimera_rest_send_error(evpl, request, 409, "Conflict",
                                "Export with that name already exists");
        return;
    }

    if (rc == -EADDRINUSE) {
        chimera_rest_send_error(evpl, request, 409, "Conflict",
                                "export_id already in use");
        return;
    }

    if (rc == -EINVAL) {
        chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                "export_id out of range");
        return;
    }

    if (rc == -ENOSPC) {
        chimera_rest_send_error(evpl, request, 409, "Conflict",
                                "Export limit reached (server nfs_max_exports)");
        return;
    }

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
