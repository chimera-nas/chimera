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
#include "vfs/sdk/vfs_error.h"
#include "vfs/vfs_procs.h"
#include "rest_internal.h"

/* ======================== Named filesystems ========================
 *
 * POST   /api/v1/filesystems                  {"module": .., "name": .., "options": ..}
 * DELETE /api/v1/filesystems/<module>/<name>
 *
 * Like the mounts API, the async VFS ops run on this REST thread's own VFS
 * thread and the HTTP reply is dispatched from the completion callback.
 */

struct fs_create_ctx {
    struct evpl              *evpl;
    struct evpl_http_request *request;
    json_t                   *root;
};

static void
fs_create_complete(
    struct chimera_vfs_thread *vfs_thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct fs_create_ctx *ctx = private_data;
    json_t               *obj;

    switch (status) {
        case CHIMERA_VFS_OK:
            obj = json_object();
            json_object_set_new(obj, "message", json_string("Filesystem created"));
            chimera_rest_send_json(ctx->evpl, ctx->request, 201, obj);
            break;
        case CHIMERA_VFS_EEXIST:
            chimera_rest_send_error(ctx->evpl, ctx->request, 409, "Conflict",
                                    "Filesystem with that name already exists");
            break;
        case CHIMERA_VFS_ENOTSUP:
            chimera_rest_send_error(ctx->evpl, ctx->request, 400, "Bad Request",
                                    "Module does not support named filesystems");
            break;
        case CHIMERA_VFS_ENOENT:
            chimera_rest_send_error(ctx->evpl, ctx->request, 404, "Not Found",
                                    "Module does not exist");
            break;
        case CHIMERA_VFS_EINVAL:
            chimera_rest_send_error(ctx->evpl, ctx->request, 400, "Bad Request",
                                    "Invalid filesystem name or options");
            break;
        default:
            chimera_rest_send_error(ctx->evpl, ctx->request, 500,
                                    "Internal Server Error",
                                    "Failed to create filesystem");
            break;
    } /* switch */

    json_decref(ctx->root);
    free(ctx);
} /* fs_create_complete */

void
chimera_rest_handle_filesystems_create(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *body,
    int                         body_len)
{
    json_t               *root;
    json_error_t          error;
    const char           *name;
    const char           *module;
    const char           *options;
    json_t               *options_json;
    struct fs_create_ctx *ctx;

    root = json_loadb(body, body_len, 0, &error);
    if (!root) {
        chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                error.text);
        return;
    }

    name         = json_string_value(json_object_get(root, "name"));
    module       = json_string_value(json_object_get(root, "module"));
    options_json = json_object_get(root, "options");
    options      = json_string_value(options_json);

    if (!name || !module) {
        json_decref(root);
        chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                "Missing required fields: name, module");
        return;
    }

    if (options_json && !options) {
        json_decref(root);
        chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                "Field 'options' must be a string");
        return;
    }

    if (options) {
        char errbuf[256];

        if (!chimera_vfs_mount_options_valid(options, errbuf, sizeof(errbuf))) {
            json_decref(root);
            chimera_rest_send_error(evpl, request, 400, "Bad Request", errbuf);
            return;
        }
    }

    /* chimera_vfs_mkfs stores the name/options pointers until the operation
     * completes, so keep the parsed JSON alive until the completion. */
    ctx          = malloc(sizeof(*ctx));
    ctx->evpl    = evpl;
    ctx->request = request;
    ctx->root    = root;

    chimera_vfs_mkfs(thread->vfs_thread, NULL, module, name, options,
                     fs_create_complete, ctx);
} /* chimera_rest_handle_filesystems_create */

struct fs_delete_ctx {
    struct evpl              *evpl;
    struct evpl_http_request *request;
    char                      module[64];
    char                      name[256];
};

static void
fs_delete_complete(
    struct chimera_vfs_thread *vfs_thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct fs_delete_ctx *ctx = private_data;

    switch (status) {
        case CHIMERA_VFS_OK:
            evpl_http_server_dispatch_default(ctx->request, 204);
            break;
        case CHIMERA_VFS_ENOENT:
            chimera_rest_send_error(ctx->evpl, ctx->request, 404, "Not Found",
                                    "Filesystem does not exist");
            break;
        case CHIMERA_VFS_EBUSY:
            chimera_rest_send_error(ctx->evpl, ctx->request, 409, "Conflict",
                                    "Filesystem has active mounts");
            break;
        case CHIMERA_VFS_ENOTSUP:
            chimera_rest_send_error(ctx->evpl, ctx->request, 400, "Bad Request",
                                    "Module does not support named filesystems");
            break;
        default:
            chimera_rest_send_error(ctx->evpl, ctx->request, 500,
                                    "Internal Server Error",
                                    "Failed to remove filesystem");
            break;
    } /* switch */

    free(ctx);
} /* fs_delete_complete */

void
chimera_rest_handle_filesystems_delete(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *param)
{
    struct fs_delete_ctx *ctx;
    const char           *slash = strchr(param, '/');

    /* The path parameter is <module>/<name>. */
    if (!slash || slash == param || !slash[1] || strchr(slash + 1, '/')) {
        chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                "Expected /api/v1/filesystems/<module>/<name>");
        return;
    }

    ctx          = malloc(sizeof(*ctx));
    ctx->evpl    = evpl;
    ctx->request = request;

    snprintf(ctx->module, sizeof(ctx->module), "%.*s",
             (int) (slash - param), param);
    snprintf(ctx->name, sizeof(ctx->name), "%s", slash + 1);

    chimera_vfs_rmfs(thread->vfs_thread, NULL, ctx->module, ctx->name,
                     fs_delete_complete, ctx);
} /* chimera_rest_handle_filesystems_delete */
