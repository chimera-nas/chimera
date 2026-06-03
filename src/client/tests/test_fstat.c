// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "client_test_common.h"

struct fstat_ctx {
    int                 done;
    int                 status;
    struct chimera_stat st;
};

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
    struct open_ctx  open_ctx  = { 0 };
    struct fstat_ctx fstat_ctx = { 0 };

    client_test_init(&env, argv, argc);

    client_test_mount(&env, "/test", mount_callback, &mount_ctx);

    while (!mount_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (mount_ctx.status != 0) {
        fprintf(stderr, "Failed to mount test module\n");
        client_test_fail(&env);
    }

    /* Create a file and keep the open handle for fstat. */
    chimera_open(env.client_thread, "/test/fstatfile", 15, CHIMERA_VFS_OPEN_CREATE,
                 open_complete, &open_ctx);

    while (!open_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (open_ctx.status != 0 || open_ctx.handle == NULL) {
        fprintf(stderr, "Failed to create test file\n");
        client_test_fail(&env);
    }

    /* fstat the open handle and verify the attributes. */
    chimera_fstat(env.client_thread, open_ctx.handle, fstat_callback, &fstat_ctx);

    while (!fstat_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (fstat_ctx.status != 0) {
        fprintf(stderr, "Failed to fstat open handle: %d\n", fstat_ctx.status);
        client_test_fail(&env);
    }

    if (fstat_ctx.st.st_uid != env.cred.uid || fstat_ctx.st.st_gid != env.cred.gid) {
        fprintf(stderr, "fstat: expected uid=%u gid=%u, got uid=%lu gid=%lu\n",
                env.cred.uid, env.cred.gid,
                (unsigned long) fstat_ctx.st.st_uid,
                (unsigned long) fstat_ctx.st.st_gid);
        client_test_fail(&env);
    }

    if (!S_ISREG(fstat_ctx.st.st_mode)) {
        fprintf(stderr, "fstat: expected a regular file, got mode 0%lo\n",
                (unsigned long) fstat_ctx.st.st_mode);
        client_test_fail(&env);
    }

    if (fstat_ctx.st.st_ino == 0) {
        fprintf(stderr, "Warning: fstat st_ino is 0\n");
    }

    fprintf(stderr, "fstat: uid=%lu gid=%lu mode=0%lo ino=%lu (ok)\n",
            (unsigned long) fstat_ctx.st.st_uid,
            (unsigned long) fstat_ctx.st.st_gid,
            (unsigned long) fstat_ctx.st.st_mode,
            (unsigned long) fstat_ctx.st.st_ino);

    chimera_close(env.client_thread, open_ctx.handle);

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
