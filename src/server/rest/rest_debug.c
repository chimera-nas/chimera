// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Test-only debug endpoint: POST /api/v1/debug/fsop
 *
 * Performs a server-side filesystem mutation (unlink/rename/link/chmod) on a
 * path within an exported share, issued as a single VFS compound. The
 * VFS core recalls any outstanding delegation/oplock on the affected file as a
 * natural side effect of these metadata operations, which lets the pynfs
 * DELEG16-20 tests drive an "out-of-band" recall that chimera otherwise has no
 * way to simulate on an in-memory backend.
 *
 * This route is only reachable when the rest_debug_fsops config flag is set;
 * it is never enabled in production.
 *
 * Request body (JSON):
 *   { "op": "unlink", "path": "/share/foo" }
 *   { "op": "rename", "path": "/share/foo", "path2": "/share/bar" }
 *   { "op": "link",   "path": "/share/foo", "path2": "/share/foo-link" }
 *   { "op": "chmod",  "path": "/share/foo", "mode": 511 }
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <jansson.h>

#include "evpl/evpl.h"
#include "evpl/evpl_http.h"
#include "server/server.h"
#include "vfs/vfs.h"
#include "vfs/vfs_compound.h"
#include "common/compound_retry.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_cred.h"
#include "rest_internal.h"

struct rest_fsop_ctx {
    struct evpl              *evpl;
    struct evpl_http_request *request;
};

static void
rest_fsop_send_json(
    struct evpl              *evpl,
    struct evpl_http_request *request,
    int                       status,
    const char               *json_body)
{
    struct evpl_iovec iov;
    int               len = strlen(json_body);

    evpl_iovec_alloc(evpl, len, 0, 1, 0, &iov);
    memcpy(evpl_iovec_data(&iov), json_body, len);
    evpl_iovec_set_length(&iov, len);

    evpl_http_request_add_header(request, "Content-Type", "application/json");
    evpl_http_request_add_datav(request, &iov, 1);
    evpl_http_server_set_response_length(request, len);
    evpl_http_server_dispatch_default(request, status);
} /* rest_fsop_send_json */

/* Only terminal completion publishes the HTTP response. Rejected finishes
* replay through the common bounded adapter with compound-owned inputs. */
static void
rest_fsop_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct rest_fsop_ctx  *ctx        = private_data;
    enum chimera_vfs_error error_code = chimera_vfs_compound_status(compound);
    char                   response[128];

    /* The compound owns every intermediate handle, including chmod's open. */
    chimera_vfs_compound_free(compound);
    if (error_code == CHIMERA_VFS_OK) {
        rest_fsop_send_json(ctx->evpl, ctx->request, 200, "{\"status\":\"ok\"}");
    } else {
        snprintf(response, sizeof(response),
                 "{\"error\":\"fsop failed\",\"vfs_error\":%d}", error_code);
        rest_fsop_send_json(ctx->evpl, ctx->request, 500, response);
    }
    free(ctx);
} /* rest_fsop_complete */

void
chimera_rest_handle_debug_fsop(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *body,
    int                         body_len)
{
    json_t                      *root;
    json_error_t                 error;
    const char                  *op, *path, *path2 = NULL;
    const char                  *bad_request = NULL;
    json_t                      *mode_obj    = NULL;
    struct rest_fsop_ctx        *ctx;
    struct chimera_vfs_compound *compound;

    root = json_loadb(body, body_len, 0, &error);
    if (!root) {
        rest_fsop_send_json(evpl, request, 400,
                            "{\"error\":\"Bad Request\",\"message\":\"invalid JSON\"}");
        return;
    }

    op   = json_string_value(json_object_get(root, "op"));
    path = json_string_value(json_object_get(root, "path"));
    if (!op || !path || strlen(path) >= CHIMERA_VFS_PATH_MAX) {
        bad_request = "missing op or path";
    } else if (!strcmp(op, "rename") || !strcmp(op, "link")) {
        path2 = json_string_value(json_object_get(root, "path2"));
        if (!path2 || strlen(path2) >= CHIMERA_VFS_PATH_MAX) {
            bad_request = !strcmp(op, "rename") ? "rename requires path2" : "link requires path2";
        }
    } else if (!strcmp(op, "chmod")) {
        mode_obj = json_object_get(root, "mode");
        if (!json_is_integer(mode_obj)) {
            bad_request = "chmod requires integer mode";
        }
    } else if (strcmp(op, "unlink")) {
        bad_request = "unknown op";
    }
    if (bad_request) {
        char response[128];

        snprintf(response, sizeof(response),
                 "{\"error\":\"Bad Request\",\"message\":\"%s\"}", bad_request);
        json_decref(root);
        rest_fsop_send_json(evpl, request, 400, response);
        return;
    }

    ctx = calloc(1, sizeof(*ctx));
    if (!ctx) {
        json_decref(root);
        rest_fsop_send_json(evpl, request, 500, "{\"error\":\"out of memory\"}");
        return;
    }
    ctx->evpl    = evpl;
    ctx->request = request;
    compound     = chimera_vfs_compound_alloc(thread->vfs_thread, chimera_vfs_get_server_cred());
    chimera_vfs_compound_add_putroot(compound);

    if (!strcmp(op, "unlink")) {
        chimera_vfs_compound_add_remove_path(compound, path, strlen(path), 0);
    } else if (!strcmp(op, "rename")) {
        chimera_vfs_compound_add_rename_path(compound, path, strlen(path), path2, strlen(path2));
    } else if (!strcmp(op, "link")) {
        chimera_vfs_compound_add_link_path(compound, path, strlen(path), 0, path2, strlen(path2), 0);
    } else {
        struct chimera_vfs_attrs attr = { 0 };

        attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        attr.va_mode     = json_integer_value(mode_obj);
        chimera_vfs_compound_add_lookup_path(compound, path, strlen(path),
                                             CHIMERA_VFS_ATTR_FH, CHIMERA_VFS_LOOKUP_FOLLOW);
        chimera_vfs_compound_add_open_current(compound,
                                              CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH, 0);
        chimera_vfs_compound_add_setattr(compound, NULL, &attr, 0, 0);
    }

    /* Builders copy paths and attributes, so neither JSON nor stack storage
     * needs to survive asynchronous execution or finish retries. */
    json_decref(root);
    chimera_frontend_compound_submit(compound, rest_fsop_complete, ctx);
} /* chimera_rest_handle_debug_fsop */
