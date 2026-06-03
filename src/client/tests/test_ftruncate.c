// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_test_common.h"

struct op_ctx {
    int done;
    enum chimera_vfs_error status;
};

struct fstat_ctx {
    int                 done;
    enum chimera_vfs_error status;
    struct chimera_stat st;
};

struct statfs_ctx {
    int                    done;
    enum chimera_vfs_error status;
    struct chimera_statvfs st;
};

struct open_ctx {
    int                             done;
    enum chimera_vfs_error status;
    struct chimera_vfs_open_handle *handle;
};

static void
op_callback(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct op_ctx *ctx = private_data;

    ctx->status = status;
    ctx->done   = 1;
} /* op_callback */

static void
fstat_callback(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    const struct chimera_stat    *st,
    void                         *private_data)
{
    struct fstat_ctx *ctx = private_data;

    ctx->status = status;
    if (st) {
        ctx->st = *st;
    }
    ctx->done = 1;
} /* fstat_callback */

static void
statfs_callback(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    const struct chimera_statvfs *st,
    void                         *private_data)
{
    struct statfs_ctx *ctx = private_data;

    ctx->status = status;
    if (st) {
        ctx->st = *st;
    }
    ctx->done = 1;
} /* statfs_callback */

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

int
main(
    int    argc,
    char **argv)
{
    struct test_env   env;
    struct mount_ctx  mount_ctx  = { 0 };
    struct open_ctx   open_ctx   = { 0 };
    struct op_ctx     op_ctx     = { 0 };
    struct fstat_ctx  fstat_ctx  = { 0 };
    struct statfs_ctx statfs_ctx = { 0 };
    const uint64_t    new_size   = 1048576; /* 1 MiB */

    client_test_init(&env, argv, argc);

    client_test_mount(&env, "/test", mount_callback, &mount_ctx);

    while (!mount_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (mount_ctx.status != 0) {
        fprintf(stderr, "Failed to mount test module\n");
        client_test_fail(&env);
    }

    /* Create a file. */
    chimera_open(env.client_thread, "/test/ftruncfile", 16, CHIMERA_VFS_OPEN_CREATE,
                 open_complete, &open_ctx);

    while (!open_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (open_ctx.status != 0 || open_ctx.handle == NULL) {
        fprintf(stderr, "Failed to create test file\n");
        client_test_fail(&env);
    }

    /* ftruncate to new_size, then fstat to verify the size took effect. */
    chimera_ftruncate(env.client_thread, open_ctx.handle, new_size, op_callback, &op_ctx);

    while (!op_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (op_ctx.status != 0) {
        fprintf(stderr, "ftruncate failed: %d\n", op_ctx.status);
        client_test_fail(&env);
    }

    chimera_fstat(env.client_thread, open_ctx.handle, fstat_callback, &fstat_ctx);

    while (!fstat_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (fstat_ctx.status != 0) {
        fprintf(stderr, "fstat after ftruncate failed: %d\n", fstat_ctx.status);
        client_test_fail(&env);
    }

    if (fstat_ctx.st.st_size != new_size) {
        fprintf(stderr, "ftruncate: expected size %lu, got %lu\n",
                (unsigned long) new_size, (unsigned long) fstat_ctx.st.st_size);
        client_test_fail(&env);
    }

    fprintf(stderr, "ftruncate: size=%lu (ok)\n", (unsigned long) fstat_ctx.st.st_size);

    /* commit (fsync) the file. */
    memset(&op_ctx, 0, sizeof(op_ctx));

    chimera_commit(env.client_thread, open_ctx.handle, op_callback, &op_ctx);

    while (!op_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (op_ctx.status != 0) {
        fprintf(stderr, "commit failed: %d\n", op_ctx.status);
        client_test_fail(&env);
    }

    fprintf(stderr, "commit: ok\n");

    /* fstatfs on the open handle. */
    chimera_fstatfs(env.client_thread, open_ctx.handle, statfs_callback, &statfs_ctx);

    while (!statfs_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (statfs_ctx.status != 0) {
        fprintf(stderr, "fstatfs failed: %d\n", statfs_ctx.status);
        client_test_fail(&env);
    }

    fprintf(stderr, "fstatfs: f_bsize=%lu f_blocks=%lu (ok)\n",
            (unsigned long) statfs_ctx.st.f_bsize,
            (unsigned long) statfs_ctx.st.f_blocks);

    chimera_close(env.client_thread, open_ctx.handle);

    client_test_success(&env);

    return 0;
} /* main */
