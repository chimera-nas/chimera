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

struct open_at_ctx {
    int                             status;
    int                             done;
    struct chimera_vfs_open_handle *handle;
};

static void
open_at_complete(
    struct chimera_client_thread   *client,
    enum chimera_vfs_error          status,
    struct chimera_vfs_open_handle *handle,
    void                           *private_data)
{
    struct open_at_ctx *ctx = private_data;

    ctx->status = status;
    ctx->done   = 1;
    ctx->handle = handle;
} /* chimera_mkdir_complete */

struct write_ctx {
    int status;
    int done;
};

static void
write_callback(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct write_ctx *ctx = private_data;

    ctx->status = status;
    ctx->done   = 1;
} /* write_callback */

struct read_ctx {
    int status;
    int done;
};

static void
read_callback(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    struct evpl_iovec            *iov,
    int                           niov,
    void                         *private_data)
{
    struct read_ctx *ctx = private_data;
    int              i;

    ctx->status = status;
    ctx->done   = 1;

    for (i = 0; i < niov; i++) {
        evpl_iovec_release(&iov[i]);
    }
} /* read_callback */

int
main(
    int    argc,
    char **argv)
{
    struct test_env    env;
    struct mount_ctx   ctx         = { 0 };
    struct open_at_ctx open_at_ctx = { 0 };
    struct write_ctx   write_ctx   = { 0 };
    struct read_ctx    read_ctx    = { 0 };
    struct evpl_iovec  iov;


    nfs_test_init(&env, argv, argc);

    chimera_mount(env.client_thread, "mnt", "nfs", "127.0.0.1:/share", mount_callback, &ctx);

    while (!ctx.done) {
        evpl_continue(env.evpl);
    }

    if (ctx.status) {
        fprintf(stderr, "Failed to mount test module\n");
        exit(EXIT_FAILURE);
    }

    chimera_open(env.client_thread, "mnt/testfile", 12, CHIMERA_VFS_OPEN_CREATE, open_at_complete, &open_at_ctx);

    while (!open_at_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (open_at_ctx.status) {
        fprintf(stderr, "Failed to create file\n");
        exit(EXIT_FAILURE);
    }

    evpl_iovec_alloc(env.evpl, 13, 0, 1, &iov);
    memcpy(iov.data, "Hello, world!", 13);

    chimera_write(env.client_thread, open_at_ctx.handle, 0, 13, &iov, 1, write_callback, &write_ctx);

    while (!write_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (write_ctx.status) {
        fprintf(stderr, "Failed to write to file\n");
        exit(EXIT_FAILURE);
    }

    evpl_iovec_release(&iov);

    chimera_read(env.client_thread, open_at_ctx.handle, 0, 13, read_callback, &read_ctx);

    while (!read_ctx.done) {
        evpl_continue(env.evpl);
    }

    if (read_ctx.status) {
        fprintf(stderr, "Failed to read from file\n");
        exit(EXIT_FAILURE);
    }

    if (memcmp(iov.data, "Hello, world!", 13) != 0) {
        fprintf(stderr, "Read returned bad data\n");
        exit(EXIT_FAILURE);
    }

    chimera_close(env.client_thread, open_at_ctx.handle);

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