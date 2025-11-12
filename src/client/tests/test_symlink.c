// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdio.h>
#include <string.h>

#include "client/client.h"
#include "evpl/evpl.h"
#include "prometheus-c.h"
#include "vfs/vfs.h"

struct mount_ctx {
    int done;
    int status;
};

static void
mount_callback(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct mount_ctx *ctx = private_data;

    ctx->status = status;
    ctx->done   = 1;
} /* mount_callback */

static void
unmount_callback(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct mount_ctx *ctx = private_data;

    ctx->status = status;
    ctx->done   = 1;
} /* unmount_callback */

struct symlink_ctx {
    int done;
    int status;
};

static void
symlink_callback(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct symlink_ctx *ctx = private_data;

    ctx->status = status;
    ctx->done   = 1;
} /* symlink_callback */

static void
open_complete(
    struct chimera_client_thread   *client,
    enum chimera_vfs_error          status,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_vfs_open_handle **handle = private_data;

    *handle = oh;
} /* open_complete */

int
main(
    int    argc,
    char **argv)
{
    struct chimera_client          *client;
    struct chimera_client_config   *config;
    struct chimera_client_thread   *thread;
    struct evpl                    *evpl;
    struct chimera_vfs_open_handle *file_handle = NULL;
    struct prometheus_metrics      *metrics;
    struct mount_ctx                mount_ctx = { 0 };
    struct symlink_ctx              symlink_ctx = { 0 };

    chimera_log_init();

    metrics = prometheus_metrics_create(NULL, NULL, 0);

    ChimeraLogLevel = CHIMERA_LOG_DEBUG;

    evpl = evpl_create(NULL);

    config = chimera_client_config_init();

    client = chimera_client_init(config, metrics);

    thread = chimera_client_thread_init(evpl, client);

    chimera_mount(
        thread,
        "/memfs",
        "memfs",
        "/",
        mount_callback,
        &mount_ctx);

    while (!mount_ctx.done) {
        evpl_continue(evpl);
    }

    if (mount_ctx.status != 0) {
        fprintf(stderr, "Failed to mount test module\n");
        return 1;
    }

    file_handle = NULL;

    chimera_open(thread, "/memfs/testfile", 15, CHIMERA_VFS_OPEN_CREATE,
                 open_complete, &file_handle);

    while (!file_handle) {
        evpl_continue(evpl);
    }

    chimera_close(thread, file_handle);

    memset(&symlink_ctx, 0, sizeof(symlink_ctx));

    chimera_symlink(thread, "/memfs/symlink", 14, "/memfs/testfile", 15,
                    symlink_callback, &symlink_ctx);

    while (!symlink_ctx.done) {
        evpl_continue(evpl);
    }

    if (symlink_ctx.status != 0) {
        fprintf(stderr, "Failed to create symlink: %d\n", symlink_ctx.status);
        return 1;
    }

    fprintf(stderr, "Created symlink successfully\n");

    file_handle = NULL;

    chimera_open(thread, "/memfs/symlink", 14, 0, open_complete, &file_handle);

    while (!file_handle) {
        evpl_continue(evpl);
    }

    if (file_handle == NULL) {
        fprintf(stderr, "Failed to open symlink\n");
        return 1;
    }

    fprintf(stderr, "Opened symlink successfully\n");

    chimera_close(thread, file_handle);

    memset(&mount_ctx, 0, sizeof(mount_ctx));

    chimera_umount(thread, "/memfs", unmount_callback, &mount_ctx);

    while (!mount_ctx.done) {
        evpl_continue(evpl);
    }

    if (mount_ctx.status != 0) {
        fprintf(stderr, "Failed to unmount /memfs\n");
        return 1;
    }

    chimera_client_thread_shutdown(evpl, thread);

    chimera_destroy(client);

    prometheus_metrics_destroy(metrics);

    evpl_destroy(evpl);

    return 0;
} /* main */

