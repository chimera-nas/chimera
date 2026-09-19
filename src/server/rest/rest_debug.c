// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Test-only debug endpoint: POST /api/v1/debug/fsop
 *
 * Performs a server-side filesystem mutation (unlink/rename/link/chmod) on a
 * path within an exported share, issued directly through the VFS layer. The
 * VFS core recalls any outstanding delegation/oplock on the affected file as a
 * natural side effect of these metadata operations, which lets the pynfs
 * DELEG16-20 tests drive an "out-of-band" recall that chimera otherwise has no
 * way to simulate on an in-memory backend.
 *
 * Each op is one sequence rooted at PUTROOT and addressed by path: unlink is
 * REMOVE_PATH, rename RENAME_PATH, link LINK_PATH, and chmod resolves the
 * object (LOOKUP_PATH), opens it (OPEN_CURRENT) and applies the mode through
 * that open (SETATTR) -- the open belongs to the sequence and goes with it.
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
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_cred.h"
#include "rest_internal.h"

struct rest_fsop_ctx {
    struct evpl              *evpl;
    struct evpl_http_request *request;
    struct chimera_vfs_attrs  set_attr;
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

/*
 * The one completion for every op: maps the sequence's status to an HTTP
 * response and frees the context.  Nothing is read back from the ops, and
 * whatever the sequence opened (chmod's handle) is released with it.
 */
static void
rest_fsop_complete(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    struct rest_fsop_ctx  *ctx        = private_data;
    enum chimera_vfs_error error_code = chimera_vfs_compound_status(compound);
    char                   response[128];

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
    json_t                        *root;
    json_error_t                   error;
    const char                    *op;
    const char                    *path;
    const char                    *path2;
    json_t                        *mode_obj;
    struct rest_fsop_ctx          *ctx;
    struct chimera_vfs_compound   *compound;
    const struct chimera_vfs_cred *cred = chimera_vfs_get_server_cred();

    root = json_loadb(body, body_len, 0, &error);
    if (!root) {
        rest_fsop_send_json(evpl, request, 400,
                            "{\"error\":\"Bad Request\",\"message\":\"invalid JSON\"}");
        return;
    }

    op   = json_string_value(json_object_get(root, "op"));
    path = json_string_value(json_object_get(root, "path"));

    if (!op || !path || strlen(path) >= CHIMERA_VFS_PATH_MAX) {
        json_decref(root);
        rest_fsop_send_json(evpl, request, 400,
                            "{\"error\":\"Bad Request\",\"message\":\"missing op or path\"}");
        return;
    }

    ctx          = calloc(1, sizeof(*ctx));
    ctx->evpl    = evpl;
    ctx->request = request;

    /* The paths are copied by the adders, so the JSON document need only
     * outlive the build. */
    compound = chimera_vfs_compound_alloc(thread->vfs_thread, cred);

    chimera_vfs_compound_add_putroot(compound);

    if (strcmp(op, "unlink") == 0) {
        chimera_vfs_compound_add_remove_path(compound, path, strlen(path), 0);
    } else if (strcmp(op, "rename") == 0) {
        path2 = json_string_value(json_object_get(root, "path2"));
        if (!path2 || strlen(path2) >= CHIMERA_VFS_PATH_MAX) {
            chimera_vfs_compound_free(compound);
            json_decref(root);
            free(ctx);
            rest_fsop_send_json(evpl, request, 400,
                                "{\"error\":\"Bad Request\",\"message\":\"rename requires path2\"}");
            return;
        }
        chimera_vfs_compound_add_rename_path(compound,
                                             path, strlen(path),
                                             path2, strlen(path2));
    } else if (strcmp(op, "link") == 0) {
        path2 = json_string_value(json_object_get(root, "path2"));
        if (!path2 || strlen(path2) >= CHIMERA_VFS_PATH_MAX) {
            chimera_vfs_compound_free(compound);
            json_decref(root);
            free(ctx);
            rest_fsop_send_json(evpl, request, 400,
                                "{\"error\":\"Bad Request\",\"message\":\"link requires path2\"}");
            return;
        }
        chimera_vfs_compound_add_link_path(compound,
                                           path, strlen(path), 0,
                                           path2, strlen(path2), 0);
    } else if (strcmp(op, "chmod") == 0) {
        mode_obj = json_object_get(root, "mode");
        if (!json_is_integer(mode_obj)) {
            chimera_vfs_compound_free(compound);
            json_decref(root);
            free(ctx);
            rest_fsop_send_json(evpl, request, 400,
                                "{\"error\":\"Bad Request\",\"message\":\"chmod requires integer mode\"}");
            return;
        }

        ctx->set_attr.va_req_mask = 0;
        ctx->set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        ctx->set_attr.va_mode     = json_integer_value(mode_obj);

        chimera_vfs_compound_add_lookup_path(compound, path, strlen(path),
                                             CHIMERA_VFS_ATTR_FH,
                                             CHIMERA_VFS_LOOKUP_FOLLOW);
        chimera_vfs_compound_add_open_current(compound,
                                              CHIMERA_VFS_OPEN_INFERRED |
                                              CHIMERA_VFS_OPEN_PATH, 0);
        chimera_vfs_compound_add_setattr(compound, NULL, &ctx->set_attr, 0, 0);
    } else {
        chimera_vfs_compound_free(compound);
        json_decref(root);
        free(ctx);
        rest_fsop_send_json(evpl, request, 400,
                            "{\"error\":\"Bad Request\",\"message\":\"unknown op\"}");
        return;
    }

    json_decref(root);

    chimera_vfs_compound_submit(compound, rest_fsop_complete, ctx);
} /* chimera_rest_handle_debug_fsop */
