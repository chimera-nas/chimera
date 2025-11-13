// SPDX-FileCopyrightText: 2025 Ben Jarvis
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_test_common.h"

struct stat_ctx {
    int                 done;
    int                 status;
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
    struct test_env  env;
    struct mount_ctx mount_ctx = { 0 };
    struct stat_ctx  stat_ctx  = { 0 };
    struct open_ctx  open_ctx  = { 0 };

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

    memset(&stat_ctx, 0, sizeof(stat_ctx));

    chimera_stat(env.client_thread, "/test/testfile", 14, stat_callback, &stat_ctx);

    while (!stat_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (stat_ctx.status != 0) {
        fprintf(stderr, "Failed to stat file: %d\n", stat_ctx.status);
        client_test_fail(&env);
    }

    fprintf(stderr, "Stat successful:\n");
    fprintf(stderr, "  st_dev: %lu\n", (unsigned long) stat_ctx.st.st_dev);
    fprintf(stderr, "  st_ino: %lu\n", (unsigned long) stat_ctx.st.st_ino);
    fprintf(stderr, "  st_mode: %lo\n", (unsigned long) stat_ctx.st.st_mode);
    fprintf(stderr, "  st_nlink: %lu\n", (unsigned long) stat_ctx.st.st_nlink);
    fprintf(stderr, "  st_uid: %lu\n", (unsigned long) stat_ctx.st.st_uid);
    fprintf(stderr, "  st_gid: %lu\n", (unsigned long) stat_ctx.st.st_gid);
    fprintf(stderr, "  st_size: %lu\n", (unsigned long) stat_ctx.st.st_size);

    if (stat_ctx.st.st_ino == 0) {
        fprintf(stderr, "Warning: st_ino is 0\n");
    }

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

