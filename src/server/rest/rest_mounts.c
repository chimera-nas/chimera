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
#include "vfs/vfs_error.h"
#include "vfs/vfs_procs.h"
#include "rest_internal.h"

/* ======================== VFS Mounts ======================== */

struct mount_list_ctx {
    json_t *array;
};

static int
mount_to_json_callback(
    const char *mount_path,
    const char *module_name,
    const char *module_path,
    void       *data)
{
    struct mount_list_ctx *ctx = data;
    json_t                *obj;

    /* The "root" pseudo-mount is a Chimera-internal entry that is not meant to
     * be managed by clients, so omit it from the listing. */
    if (strcmp(module_name, "root") == 0) {
        return 0;
    }

    obj = json_object();
    json_object_set_new(obj, "name", json_string(mount_path));
    json_object_set_new(obj, "module", json_string(module_name));
    json_object_set_new(obj, "path", json_string(module_path));

    json_array_append_new(ctx->array, obj);

    return 0;
} /* mount_to_json_callback */

void
chimera_rest_handle_mounts_list(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread)
{
    struct mount_list_ctx ctx;

    ctx.array = json_array();

    chimera_server_iterate_mounts(thread->shared->server,
                                  mount_to_json_callback, &ctx);

    chimera_rest_send_json(evpl, request, 200, ctx.array);
} /* chimera_rest_handle_mounts_list */

struct mount_get_ctx {
    const char *name;
    json_t     *result;
};

static int
mount_get_callback(
    const char *mount_path,
    const char *module_name,
    const char *module_path,
    void       *data)
{
    struct mount_get_ctx *ctx = data;

    /* The "root" pseudo-mount is a Chimera-internal entry and must not be
     * retrievable through the REST API. */
    if (strcmp(module_name, "root") == 0) {
        return 0;
    }

    if (strcmp(mount_path, ctx->name) != 0) {
        return 0;
    }

    ctx->result = json_object();
    json_object_set_new(ctx->result, "name", json_string(mount_path));
    json_object_set_new(ctx->result, "module", json_string(module_name));
    json_object_set_new(ctx->result, "path", json_string(module_path));

    return 1;
} /* mount_get_callback */

void
chimera_rest_handle_mounts_get(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *name)
{
    struct mount_get_ctx ctx;
    const char          *path = name;

    /* Mount paths are registered with leading slashes stripped, so normalize
     * the requested name the same way before matching. */
    while (*path == '/') {
        path++;
    }

    ctx.name   = path;
    ctx.result = NULL;

    chimera_server_iterate_mounts(thread->shared->server,
                                  mount_get_callback, &ctx);

    if (!ctx.result) {
        chimera_rest_send_error(evpl, request, 404, "Not Found",
                                "Mount does not exist");
        return;
    }

    chimera_rest_send_json(evpl, request, 200, ctx.result);
} /* chimera_rest_handle_mounts_get */

/*
 * Mount and umount are asynchronous VFS operations.  They are driven on this
 * REST thread's own VFS thread (thread->vfs_thread) rather than via the
 * blocking chimera_server_mount/unmount helpers, which spin up a throwaway
 * evpl and register a fresh VFS thread -- doing so from an already
 * RCU-registered REST worker aborts on a liburcu double-registration.  The
 * HTTP reply is dispatched from the completion callback.
 */

struct mount_exists_ctx {
    const char *name;
    int         found;
};

static int
mount_exists_callback(
    const char *mount_path,
    const char *module_name,
    const char *module_path,
    void       *data)
{
    struct mount_exists_ctx *ctx = data;

    if (strcmp(mount_path, ctx->name) == 0) {
        ctx->found = 1;
        return 1;
    }

    return 0;
} /* mount_exists_callback */

struct mount_create_ctx {
    struct evpl              *evpl;
    struct evpl_http_request *request;
    json_t                   *root;
};

static void
mount_create_complete(
    struct chimera_vfs_thread *vfs_thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct mount_create_ctx *ctx = private_data;
    json_t                  *obj;

    if (status == CHIMERA_VFS_OK) {
        obj = json_object();
        json_object_set_new(obj, "message", json_string("Mount created"));
        chimera_rest_send_json(ctx->evpl, ctx->request, 201, obj);
    } else {
        chimera_rest_send_error(ctx->evpl, ctx->request, 500,
                                "Internal Server Error",
                                "Failed to create mount");
    }

    json_decref(ctx->root);
    free(ctx);
} /* mount_create_complete */

void
chimera_rest_handle_mounts_create(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *body,
    int                         body_len)
{
    json_t                  *root;
    json_error_t             error;
    const char              *name;
    const char              *module;
    const char              *path;
    const char              *options;
    const char              *normalized;
    struct mount_create_ctx *ctx;
    struct mount_exists_ctx  exists_ctx;

    root = json_loadb(body, body_len, 0, &error);
    if (!root) {
        chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                error.text);
        return;
    }

    name    = json_string_value(json_object_get(root, "name"));
    module  = json_string_value(json_object_get(root, "module"));
    path    = json_string_value(json_object_get(root, "path"));
    options = json_string_value(json_object_get(root, "options"));

    if (!name || !module || !path) {
        json_decref(root);
        chimera_rest_send_error(evpl, request, 400, "Bad Request",
                                "Missing required fields: name, module, path");
        return;
    }

    /* Mount paths are registered with leading slashes stripped, so normalize
     * the requested name the same way before checking for an existing mount. */
    normalized = name;
    while (*normalized == '/') {
        normalized++;
    }

    exists_ctx.name  = normalized;
    exists_ctx.found = 0;

    chimera_server_iterate_mounts(thread->shared->server,
                                  mount_exists_callback, &exists_ctx);

    if (exists_ctx.found) {
        json_decref(root);
        chimera_rest_send_error(evpl, request, 409, "Conflict",
                                "Mount with that name already exists");
        return;
    }

    /* chimera_vfs_mount stores the name/path pointers until the operation
     * completes, so the parsed JSON (which owns those strings) is kept alive
     * in the context and released in the completion callback. */
    ctx          = malloc(sizeof(*ctx));
    ctx->evpl    = evpl;
    ctx->request = request;
    ctx->root    = root;

    chimera_vfs_mount(thread->vfs_thread, NULL, name, module, path, options,
                      mount_create_complete, ctx);
} /* chimera_rest_handle_mounts_create */

struct mount_delete_ctx {
    struct evpl              *evpl;
    struct evpl_http_request *request;
};

static void
mount_delete_complete(
    struct chimera_vfs_thread *vfs_thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct mount_delete_ctx *ctx = private_data;

    if (status == CHIMERA_VFS_ENOENT) {
        chimera_rest_send_error(ctx->evpl, ctx->request, 404, "Not Found",
                                "Mount does not exist");
    } else if (status != CHIMERA_VFS_OK) {
        chimera_rest_send_error(ctx->evpl, ctx->request, 500,
                                "Internal Server Error",
                                "Failed to delete mount");
    } else {
        evpl_http_server_dispatch_default(ctx->request, 204);
    }

    free(ctx);
} /* mount_delete_complete */

void
chimera_rest_handle_mounts_delete(
    struct evpl                *evpl,
    struct evpl_http_request   *request,
    struct chimera_rest_thread *thread,
    const char                 *name)
{
    struct mount_delete_ctx *ctx;

    if (chimera_server_mount_in_use(thread->shared->server, name)) {
        chimera_rest_send_error(evpl, request, 409, "Conflict",
                                "Mount is in use by a share, export, or bucket");
        return;
    }

    ctx          = malloc(sizeof(*ctx));
    ctx->evpl    = evpl;
    ctx->request = request;

    chimera_vfs_umount(thread->vfs_thread, NULL, name, mount_delete_complete,
                       ctx);
} /* chimera_rest_handle_mounts_delete */
