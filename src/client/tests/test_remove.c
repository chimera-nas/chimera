// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_test_common.h"

struct remove_ctx {
    int done;
    int status;
};

static void
remove_callback(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct remove_ctx *ctx = private_data;

    ctx->status = status;
    ctx->done   = 1;
} /* remove_callback */

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
    struct test_env                env;
    struct mount_ctx                mount_ctx = { 0 };
    struct remove_ctx               remove_ctx = { 0 };
    struct open_ctx                 open_ctx = { 0 };

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

    memset(&remove_ctx, 0, sizeof(remove_ctx));

    chimera_remove(env.client_thread, "/test/testfile", 14, remove_callback, &remove_ctx);

    while (!remove_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (remove_ctx.status != 0) {
        fprintf(stderr, "Failed to remove file: %d\n", remove_ctx.status);
        client_test_fail(&env);
    }

    fprintf(stderr, "Removed file successfully\n");

    memset(&open_ctx, 0, sizeof(open_ctx));

    chimera_open(env.client_thread, "/test/testfile", 14, 0, open_complete, &open_ctx);

    while (!open_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (open_ctx.status == 0 && open_ctx.handle != NULL) {
        fprintf(stderr, "File still exists after remove\n");
        chimera_close(env.client_thread, open_ctx.handle);
        client_test_fail(&env);
    }

    fprintf(stderr, "File removed successfully (open failed as expected)\n");

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

