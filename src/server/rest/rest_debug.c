// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Test-only debug endpoint: POST /api/debug/v1/fsop
 *
 * Performs a server-side filesystem mutation (unlink/rename/link/chmod) on a
 * path within an exported share, issued directly through the VFS layer. The
 * VFS core recalls any outstanding delegation/oplock on the affected file as a
 * natural side effect of these metadata operations, which lets the pynfs
 * DELEG16-20 tests drive an "out-of-band" recall that chimera otherwise has no
 * way to simulate on an in-memory backend.
 *
 * This module is built only with tests and loaded explicitly by the suites
 * that need out-of-band mutations. It is not installed with the server.
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
#include "vfs/vfs_procs.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_cred.h"
#include "rest_internal.h"
#include "rest_services.h"

struct rest_fsop_ctx {
    struct evpl                    *evpl;
    struct chimera_rest_request    *request;
    struct chimera_vfs_thread      *vfs_thread;
    char                            path[CHIMERA_VFS_PATH_MAX];
    char                            path2[CHIMERA_VFS_PATH_MAX];
    uint64_t                        mode;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE + 16];
    int                             fh_len;
    struct chimera_vfs_attrs        set_attr;
    struct chimera_vfs_open_handle *handle;
};

static void
rest_fsop_send_json(
    struct evpl                 *evpl,
    struct chimera_rest_request *request,
    int                          status,
    const char                  *json_body)
{
    chimera_rest_send_json_response(evpl, request, status, json_body);
} /* rest_fsop_send_json */

/*
 * Terminal completion for ops that don't hold an open handle (unlink, rename,
 * link, and the final leg of chmod once the handle is released). Maps the VFS
 * status to an HTTP response and frees the context.
 */
static void
rest_fsop_finish(
    struct rest_fsop_ctx  *ctx,
    enum chimera_vfs_error error_code)
{
    char response[128];

    if (error_code == CHIMERA_VFS_OK) {
        rest_fsop_send_json(ctx->evpl, ctx->request, 200, "{\"status\":\"ok\"}");
    } else {
        snprintf(response, sizeof(response),
                 "{\"error\":\"fsop failed\",\"vfs_error\":%d}", error_code);
        rest_fsop_send_json(ctx->evpl, ctx->request, 500, response);
    }

    free(ctx);
} /* rest_fsop_finish */

static void
rest_fsop_remove_cb(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    rest_fsop_finish(private_data, error_code);
} /* rest_fsop_remove_cb */

static void
rest_fsop_rename_cb(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    rest_fsop_finish(private_data, error_code);
} /* rest_fsop_rename_cb */

static void
rest_fsop_link_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    rest_fsop_finish(private_data, error_code);
} /* rest_fsop_link_cb */

static void
rest_fsop_chmod_setattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct rest_fsop_ctx *ctx = private_data;

    chimera_vfs_release(ctx->vfs_thread, ctx->handle);
    rest_fsop_finish(ctx, error_code);
} /* rest_fsop_chmod_setattr_cb */

static void
rest_fsop_chmod_open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct rest_fsop_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        rest_fsop_finish(ctx, error_code);
        return;
    }

    ctx->handle = oh;

    ctx->set_attr.va_req_mask = 0;
    ctx->set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    ctx->set_attr.va_mode     = ctx->mode;

    chimera_vfs_setattr(ctx->vfs_thread, chimera_vfs_get_server_cred(),
                        ctx->handle, &ctx->set_attr, 0, 0,
                        rest_fsop_chmod_setattr_cb, ctx);
} /* rest_fsop_chmod_open_cb */

static void
rest_fsop_chmod_lookup_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct rest_fsop_ctx *ctx = private_data;

    if (error_code != CHIMERA_VFS_OK) {
        rest_fsop_finish(ctx, error_code);
        return;
    }

    memcpy(ctx->fh, attr->va_fh, attr->va_fh_len);
    ctx->fh_len = attr->va_fh_len;

    chimera_vfs_open_fh(ctx->vfs_thread, chimera_vfs_get_server_cred(),
                        ctx->fh, ctx->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH,
                        rest_fsop_chmod_open_cb, ctx);
} /* rest_fsop_chmod_lookup_cb */

void
chimera_rest_handle_debug_fsop(
    struct evpl                 *evpl,
    struct chimera_rest_request *request,
    struct chimera_rest_thread  *thread,
    const char                  *body,
    int                          body_len)
{
    json_t                        *root;
    json_error_t                   error;
    const char                    *op;
    const char                    *path;
    const char                    *path2;
    json_t                        *mode_obj;
    struct rest_fsop_ctx          *ctx;
    const struct chimera_vfs_cred *cred = chimera_vfs_get_server_cred();
    uint8_t                        root_fh[CHIMERA_VFS_FH_SIZE + 16];
    uint32_t                       root_fh_len;

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

    ctx             = calloc(1, sizeof(*ctx));
    ctx->evpl       = evpl;
    ctx->request    = request;
    ctx->vfs_thread = thread->vfs_thread;
    snprintf(ctx->path, sizeof(ctx->path), "%s", path);

    chimera_vfs_get_root_fh(root_fh, &root_fh_len);

    if (strcmp(op, "unlink") == 0) {
        chimera_vfs_remove(ctx->vfs_thread, cred, root_fh, root_fh_len,
                           ctx->path, strlen(ctx->path), 0,
                           rest_fsop_remove_cb, ctx);
    } else if (strcmp(op, "rename") == 0) {
        path2 = json_string_value(json_object_get(root, "path2"));
        if (!path2 || strlen(path2) >= CHIMERA_VFS_PATH_MAX) {
            json_decref(root);
            free(ctx);
            rest_fsop_send_json(evpl, request, 400,
                                "{\"error\":\"Bad Request\",\"message\":\"rename requires path2\"}");
            return;
        }
        snprintf(ctx->path2, sizeof(ctx->path2), "%s", path2);
        chimera_vfs_rename(ctx->vfs_thread, cred, root_fh, root_fh_len,
                           ctx->path, strlen(ctx->path),
                           ctx->path2, strlen(ctx->path2),
                           rest_fsop_rename_cb, ctx);
    } else if (strcmp(op, "link") == 0) {
        path2 = json_string_value(json_object_get(root, "path2"));
        if (!path2 || strlen(path2) >= CHIMERA_VFS_PATH_MAX) {
            json_decref(root);
            free(ctx);
            rest_fsop_send_json(evpl, request, 400,
                                "{\"error\":\"Bad Request\",\"message\":\"link requires path2\"}");
            return;
        }
        snprintf(ctx->path2, sizeof(ctx->path2), "%s", path2);
        chimera_vfs_link(ctx->vfs_thread, cred, root_fh, root_fh_len,
                         ctx->path, strlen(ctx->path), 0,
                         ctx->path2, strlen(ctx->path2),
                         0, 0, rest_fsop_link_cb, ctx);
    } else if (strcmp(op, "chmod") == 0) {
        mode_obj = json_object_get(root, "mode");
        if (!json_is_integer(mode_obj)) {
            json_decref(root);
            free(ctx);
            rest_fsop_send_json(evpl, request, 400,
                                "{\"error\":\"Bad Request\",\"message\":\"chmod requires integer mode\"}");
            return;
        }
        ctx->mode = json_integer_value(mode_obj);
        chimera_vfs_lookup(ctx->vfs_thread, cred, root_fh, root_fh_len,
                           ctx->path, strlen(ctx->path),
                           CHIMERA_VFS_ATTR_FH, CHIMERA_VFS_LOOKUP_FOLLOW,
                           rest_fsop_chmod_lookup_cb, ctx);
    } else {
        json_decref(root);
        free(ctx);
        rest_fsop_send_json(evpl, request, 400,
                            "{\"error\":\"Bad Request\",\"message\":\"unknown op\"}");
        return;
    }

    json_decref(root);
} /* chimera_rest_handle_debug_fsop */

static void
debug_fsop(
    struct chimera_rest_request *request,
    void                        *state)
{
    chimera_rest_handle_debug_fsop(request->thread->evpl, request, request->thread,
                                   (const char *) request->body, request->body_length);
} /* debug_fsop */

static const struct chimera_rest_route        debug_route = {
    .struct_size = sizeof(struct chimera_rest_route),
    .method      = "POST",
    .path        = "/fsop",
    .max_body    = CHIMERA_REST_MAX_BODY,
    .handle      = debug_fsop,
};
static const struct chimera_rest_route *const debug_routes[]            = { &debug_route };
const struct chimera_rest_module              chimera_rest_debug_module = {
    .abi_version = CHIMERA_REST_ABI_VERSION,
    .struct_size = sizeof(struct chimera_rest_module),
    .name        = "debug",
    .api_version = 1,
    .routes      = debug_routes,
    .num_routes  = 1,
};

CHIMERA_REST_EXPORT const struct chimera_rest_module *
chimera_rest_module_get_v1(void)
{
    return &chimera_rest_debug_module;
} /* chimera_rest_module_get_v1 */
