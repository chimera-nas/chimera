// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_test_common.h"

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
    int  done;
    int  status;
    int  targetlen;
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
    int                             done;
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

int
main(
    int    argc,
    char **argv)
{
    struct test_env     env;
    struct mount_ctx    mount_ctx    = { 0 };
    struct symlink_ctx  symlink_ctx  = { 0 };
    struct readlink_ctx readlink_ctx = { 0 };
    struct open_ctx     open_ctx     = { 0 };
    char                target[CHIMERA_VFS_PATH_MAX];

    client_test_init(&env, argv, argc);

    client_test_mount(&env, "/test", mount_callback, &mount_ctx);

    while (!mount_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (mount_ctx.status != 0) {
        fprintf(stderr, "Failed to mount test module\n");
        client_test_fail(&env);
    }

    memset(&open_ctx, 0, sizeof(open_ctx));

    chimera_open(env.client_thread, "/test/testfile", 14, CHIMERA_VFS_OPEN_CREATE,
                 open_complete, &open_ctx);

    while (!open_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (open_ctx.status != 0 || open_ctx.handle == NULL) {
        fprintf(stderr, "Failed to create test file\n");
        client_test_fail(&env);
    }

    chimera_close(env.client_thread, open_ctx.handle);

    memset(&symlink_ctx, 0, sizeof(symlink_ctx));

    chimera_symlink(env.client_thread, "/test/symlink", 13, "/test/testfile", 14,
                    symlink_callback, &symlink_ctx);

    while (!symlink_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (symlink_ctx.status != 0) {
        fprintf(stderr, "Failed to create symlink: %d\n", symlink_ctx.status);
        client_test_fail(&env);
    }

    fprintf(stderr, "Created symlink successfully\n");

    memset(&readlink_ctx, 0, sizeof(readlink_ctx));

    chimera_readlink(env.client_thread, "/test/symlink", 13, target, sizeof(target),
                     readlink_callback, &readlink_ctx);

    while (!readlink_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (readlink_ctx.status != 0) {
        fprintf(stderr, "Failed to readlink: %d\n", readlink_ctx.status);
        client_test_fail(&env);
    }

    if (readlink_ctx.targetlen != 14 || memcmp(readlink_ctx.target, "/test/testfile", 14) != 0) {
        fprintf(stderr, "Readlink returned wrong target: '%.*s' (expected '/test/testfile', got %d bytes)\n",
                readlink_ctx.targetlen, readlink_ctx.target, readlink_ctx.targetlen);
        client_test_fail(&env);
    }

    fprintf(stderr, "Readlink successful: '%.*s'\n", readlink_ctx.targetlen, readlink_ctx.target);

    memset(&mount_ctx, 0, sizeof(mount_ctx));

    chimera_umount(env.client_thread, "/test", unmount_callback, &mount_ctx);

    while (!mount_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (mount_ctx.status != 0) {
        fprintf(stderr, "Failed to unmount /test\n");
        client_test_fail(&env);
    }

    client_test_success(&env);

    return 0;
} /* main */

