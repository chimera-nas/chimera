// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "nfs_test_common.h"


struct mount_ctx {
    int status;
    int done;
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

struct mkdir_ctx {
    int status;
    int done;
};

static void
mkdir_complete(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct mkdir_ctx *ctx = private_data;

    ctx->status = status;

    ctx->done = 1;
} /* chimera_mkdir_complete */

int
main(
    int    argc,
    char **argv)
{
    struct test_env  env;
    struct mount_ctx ctx       = { 0 };
    struct mkdir_ctx mkdir_ctx = { 0 };

    nfs_test_init(&env, argv, argc);

    chimera_mount(env.client_thread, "mnt", "nfs", "127.0.0.1:/share", mount_callback, &ctx);

    while (!ctx.done) {
        evpl_continue(env.evpl);
    }

    if (ctx.status) {
        fprintf(stderr, "Failed to mount test module\n");
        exit(EXIT_FAILURE);
    }

    chimera_mkdir(env.client_thread, "mnt/testdir", 11, mkdir_complete, &mkdir_ctx);

    while (!mkdir_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (mkdir_ctx.status) {
        fprintf(stderr, "Failed to create directory\n");
        exit(EXIT_FAILURE);
    }

    /* try to make a directory with invalid parent */
    memset(&mkdir_ctx, 0, sizeof(mkdir_ctx));
    chimera_mkdir(env.client_thread, "mnt/invalid/testdir", 11, mkdir_complete, &mkdir_ctx);

    while (!mkdir_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (mkdir_ctx.status == 0 ) {
        fprintf(stderr, "Created directory with invalid parent\n");
        exit(EXIT_FAILURE);
    }

    memset(&ctx, 0, sizeof(ctx));

    chimera_umount(env.client_thread, "mnt", unmount_callback, &ctx);

    while (!ctx.done) {
        evpl_continue(env.evpl);
    }

    if (ctx.status) {
        fprintf(stderr, "Failed to unmount test module\n");
        exit(EXIT_FAILURE);
    }

    nfs_test_success(&env);
} /* main */