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

struct readlink_ctx {
    int done;
    int status;
    int targetlen;
    char target[CHIMERA_VFS_PATH_MAX];
};

static void
readlink_callback(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    const char                   *target,
    int                           targetlen,
    void                         *private_data)
{
    struct readlink_ctx *ctx = private_data;

    ctx->status    = status;
    ctx->targetlen = targetlen;
    if (target && targetlen > 0) {
        memcpy(ctx->target, target, targetlen);
        ctx->target[targetlen] = '\0';
    }
    ctx->done = 1;
} /* readlink_callback */

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
    struct symlink_ctx              symlink_ctx = { 0 };
    struct readlink_ctx              readlink_ctx = { 0 };
    struct open_ctx                  open_ctx = { 0 };
    char                             target[CHIMERA_VFS_PATH_MAX];

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

    memset(&readlink_ctx, 0, sizeof(readlink_ctx));

    chimera_readlink(thread, "/memfs/symlink", 14, target, sizeof(target),
                     readlink_callback, &readlink_ctx);

    while (!readlink_ctx.done) {
        evpl_continue(evpl);
    }

    if (readlink_ctx.status != 0) {
        fprintf(stderr, "Failed to readlink: %d\n", readlink_ctx.status);
        return 1;
    }

    if (readlink_ctx.targetlen != 15 || memcmp(readlink_ctx.target, "/memfs/testfile", 15) != 0) {
        fprintf(stderr, "Readlink returned wrong target: '%.*s' (expected '/memfs/testfile')\n",
                readlink_ctx.targetlen, readlink_ctx.target);
        return 1;
    }

    fprintf(stderr, "Readlink successful: '%.*s'\n", readlink_ctx.targetlen, readlink_ctx.target);

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

