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

int
main(
    int    argc,
    char **argv)
{
    struct test_env  env;
    struct mount_ctx ctx = { 0 };

    nfs_test_init(&env, argv, argc);

    chimera_mount(env.client_thread, "mnt", "nfs", "127.0.0.1:/share", mount_callback, &ctx);

    while (!ctx.done) {
        evpl_continue(env.evpl);
    }

    if (ctx.status) {
        fprintf(stderr, "Failed to mount test module\n");
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

    fprintf(stderr, "Test successful\n");

    nfs_test_success(&env);
} /* main */