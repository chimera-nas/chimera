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

struct rename_ctx {
    int done;
    int status;
};

static void
rename_callback(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct rename_ctx *ctx = private_data;

    ctx->status = status;
    ctx->done   = 1;
} /* rename_callback */

struct open_ctx {
    int done;
    enum chimera_vfs_error status;
    struct chimera_vfs_open_handle *handle;
};

static void
open_complete(
    struct chimera_client_thread   *client,
    enum chimera_vfs_error          status,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct open_ctx *ctx = private_data;

    ctx->status = status;
    ctx->handle = oh;
    ctx->done   = 1;
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
    struct prometheus_metrics      *metrics;
    struct mount_ctx                mount_ctx = { 0 };
    struct rename_ctx                rename_ctx = { 0 };
    struct open_ctx                  open_ctx = { 0 };

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

    memset(&open_ctx, 0, sizeof(open_ctx));

    chimera_open(thread, "/memfs/testfile", 15, CHIMERA_VFS_OPEN_CREATE,
                 open_complete, &open_ctx);

    while (!open_ctx.done) {
        evpl_continue(evpl);
    }

    if (open_ctx.status != 0 || open_ctx.handle == NULL) {
        fprintf(stderr, "Failed to create test file\n");
        return 1;
    }

    chimera_close(thread, open_ctx.handle);

    memset(&rename_ctx, 0, sizeof(rename_ctx));

    chimera_rename(thread, "/memfs/testfile", 15, "/memfs/renamedfile", 17,
                   rename_callback, &rename_ctx);

    while (!rename_ctx.done) {
        evpl_continue(evpl);
    }

    if (rename_ctx.status != 0) {
        fprintf(stderr, "Failed to rename file: %d\n", rename_ctx.status);
        return 1;
    }

    fprintf(stderr, "Renamed file successfully\n");

    memset(&open_ctx, 0, sizeof(open_ctx));

    chimera_open(thread, "/memfs/testfile", 15, 0, open_complete, &open_ctx);

    while (!open_ctx.done) {
        evpl_continue(evpl);
    }

    if (open_ctx.status == 0 && open_ctx.handle != NULL) {
        fprintf(stderr, "Old file name still exists after rename\n");
        chimera_close(thread, open_ctx.handle);
        return 1;
    }

    memset(&open_ctx, 0, sizeof(open_ctx));

    chimera_open(thread, "/memfs/renamedfile", 17, 0, open_complete, &open_ctx);

    while (!open_ctx.done) {
        evpl_continue(evpl);
    }

    if (open_ctx.status != 0 || open_ctx.handle == NULL) {
        fprintf(stderr, "Failed to open renamed file\n");
        return 1;
    }

    fprintf(stderr, "Opened renamed file successfully\n");

    chimera_close(thread, open_ctx.handle);

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

