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

struct stat_ctx {
    int done;
    int status;
    struct chimera_stat st;
};

static void
stat_callback(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    const struct chimera_stat    *st,
    void                         *private_data)
{
    struct stat_ctx *ctx = private_data;

    ctx->status = status;
    if (st) {
        ctx->st = *st;
    }
    ctx->done = 1;
} /* stat_callback */

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
    struct stat_ctx                 stat_ctx = { 0 };
    struct open_ctx                 open_ctx = { 0 };

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

    memset(&stat_ctx, 0, sizeof(stat_ctx));

    chimera_stat(thread, "/memfs/testfile", 15, stat_callback, &stat_ctx);

    while (!stat_ctx.done) {
        evpl_continue(evpl);
    }

    if (stat_ctx.status != 0) {
        fprintf(stderr, "Failed to stat file: %d\n", stat_ctx.status);
        return 1;
    }

    fprintf(stderr, "Stat successful:\n");
    fprintf(stderr, "  st_dev: %lu\n", (unsigned long)stat_ctx.st.st_dev);
    fprintf(stderr, "  st_ino: %lu\n", (unsigned long)stat_ctx.st.st_ino);
    fprintf(stderr, "  st_mode: %lo\n", (unsigned long)stat_ctx.st.st_mode);
    fprintf(stderr, "  st_nlink: %lu\n", (unsigned long)stat_ctx.st.st_nlink);
    fprintf(stderr, "  st_uid: %lu\n", (unsigned long)stat_ctx.st.st_uid);
    fprintf(stderr, "  st_gid: %lu\n", (unsigned long)stat_ctx.st.st_gid);
    fprintf(stderr, "  st_size: %lu\n", (unsigned long)stat_ctx.st.st_size);

    if (stat_ctx.st.st_ino == 0) {
        fprintf(stderr, "Warning: st_ino is 0\n");
    }

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

