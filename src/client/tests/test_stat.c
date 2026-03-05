// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
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

struct mkdir_ctx {
    int done;
    enum chimera_vfs_error status;
};

static void
mkdir_complete(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct mkdir_ctx *ctx = private_data;

    ctx->status = status;
    ctx->done   = 1;
} /* mkdir_complete */

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
    struct mkdir_ctx mkdir_ctx = { 0 };

    client_test_init(&env, argv, argc);

    client_test_mount(&env, "/test", mount_callback, &mount_ctx);

    while (!mount_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (mount_ctx.status != 0) {
        fprintf(stderr, "Failed to mount test module\n");
        client_test_fail(&env);
    }

    /* Create directory and verify ownership matches credential */
    chimera_mkdir(env.client_thread, "/test/testdir", 13, mkdir_complete, &mkdir_ctx);

    while (!mkdir_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (mkdir_ctx.status != 0) {
        fprintf(stderr, "Failed to mkdir /test/testdir: %d\n", mkdir_ctx.status);
        client_test_fail(&env);
    }

    chimera_stat(env.client_thread, "/test/testdir", 13, stat_callback, &stat_ctx);

    while (!stat_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (stat_ctx.status != 0) {
        fprintf(stderr, "Failed to stat /test/testdir: %d\n", stat_ctx.status);
        client_test_fail(&env);
    }

    if (stat_ctx.st.st_uid != env.cred.uid || stat_ctx.st.st_gid != env.cred.gid) {
        fprintf(stderr, "/test/testdir: expected uid=%u gid=%u, got uid=%lu gid=%lu\n",
                env.cred.uid, env.cred.gid,
                (unsigned long) stat_ctx.st.st_uid,
                (unsigned long) stat_ctx.st.st_gid);
        client_test_fail(&env);
    }

    fprintf(stderr, "/test/testdir: uid=%lu gid=%lu (ok)\n",
            (unsigned long) stat_ctx.st.st_uid,
            (unsigned long) stat_ctx.st.st_gid);

    /* Create file and verify ownership matches credential */
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

    if (stat_ctx.st.st_uid != env.cred.uid || stat_ctx.st.st_gid != env.cred.gid) {
        fprintf(stderr, "/test/testfile: expected uid=%u gid=%u, got uid=%lu gid=%lu\n",
                env.cred.uid, env.cred.gid,
                (unsigned long) stat_ctx.st.st_uid,
                (unsigned long) stat_ctx.st.st_gid);
        client_test_fail(&env);
    }

    fprintf(stderr, "/test/testfile: uid=%lu gid=%lu (ok)\n",
            (unsigned long) stat_ctx.st.st_uid,
            (unsigned long) stat_ctx.st.st_gid);

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

