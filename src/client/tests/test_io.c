// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Exercises the SMB client data path + deep paths under a path-only mount:
 * mkdir a nested directory, create + write a file two levels deep, read it back
 * and verify the bytes, then readdir the parent and assert the entry shows up.
 */

#include <string.h>
#include <stdlib.h>
#include "client_test_common.h"

struct simple_ctx {
    int                             done;
    enum chimera_vfs_error          status;
    struct chimera_vfs_open_handle *handle;
    struct evpl                    *evpl;
    uint32_t                        rlen;
    char                            rbuf[4096];
    char                           *rdyn;     /* large-IO landing buffer (optional) */
    uint32_t                        rdyn_max;
};

static void
mount_cb(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct simple_ctx *ctx = private_data;

    ctx->status = status;
    ctx->done   = 1;
} /* mount_cb */

static void
open_cb(
    struct chimera_client_thread   *client,
    enum chimera_vfs_error          status,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct simple_ctx *ctx = private_data;

    ctx->status = status;
    ctx->handle = oh;
    ctx->done   = 1;
} /* open_cb */

static void
mkdir_cb(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct simple_ctx *ctx = private_data;

    ctx->status = status;
    ctx->done   = 1;
} /* mkdir_cb */

static void
write_cb(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct simple_ctx *ctx = private_data;

    ctx->status = status;
    ctx->done   = 1;
} /* write_cb */

static void
read_cb(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    struct evpl_iovec            *iov,
    int                           niov,
    void                         *private_data)
{
    struct simple_ctx *ctx = private_data;
    char              *dst = ctx->rdyn ? ctx->rdyn : ctx->rbuf;
    uint32_t           cap = ctx->rdyn ? ctx->rdyn_max : (uint32_t) sizeof(ctx->rbuf);
    uint32_t           off = 0;
    int                i;

    ctx->status = status;
    ctx->rlen   = 0;

    for (i = 0; i < niov; i++) {
        if (status == CHIMERA_VFS_OK) {
            uint32_t len = iov[i].length;

            if (off + len > cap) {
                len = cap - off;
            }
            memcpy(dst + off, iov[i].data, len);
            off += len;
        }
        evpl_iovec_release(ctx->evpl, &iov[i]);
    }
    ctx->rlen = off;
    ctx->done = 1;
} /* read_cb */

struct readdir_ctx {
    int done;
    enum chimera_vfs_error status;
    int found_b;
    int count;
};

static int
readdir_entry_cb(
    struct chimera_client_thread *thread,
    const struct chimera_dirent  *dirent,
    void                         *private_data)
{
    struct readdir_ctx *ctx = private_data;

    ctx->count++;
    if (dirent->namelen == 1 && dirent->name[0] == 'b') {
        ctx->found_b = 1;
    }
    return 0;
} /* readdir_entry_cb */

static void
readdir_complete_cb(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    uint64_t                      cookie,
    int                           eof,
    void                         *private_data)
{
    struct readdir_ctx *ctx = private_data;

    ctx->status = status;
    ctx->done   = 1;
} /* readdir_complete_cb */

#define RUN(ctx)       do { (ctx).done = 0; } while (0)
#define WAIT(env, ctx) do { while (!(ctx).done) { evpl_continue((env).evpl); } } while (0)

int
main(
    int    argc,
    char **argv)
{
    struct test_env    env;
    struct simple_ctx  c;
    struct readdir_ctx rc;
    const char        *payload = "the quick brown fox jumps over the lazy dog";
    uint32_t           plen    = (uint32_t) strlen(payload);

    client_test_init(&env, argv, argc);

    memset(&c, 0, sizeof(c));
    c.evpl = env.evpl;

    RUN(c);
    client_test_mount(&env, "/test", mount_cb, &c);
    WAIT(env, c);
    if (c.status != 0) {
        fprintf(stderr, "mount failed: %d\n", c.status);
        client_test_fail(&env);
    }

    /* Deep paths under a path-only mount: mkdir /test/a then /test/a/b. */
    RUN(c);
    chimera_mkdir(env.client_thread, "/test/a", 7, mkdir_cb, &c);
    WAIT(env, c);
    if (c.status != 0) {
        fprintf(stderr, "mkdir /test/a failed: %d\n", c.status);
        client_test_fail(&env);
    }

    RUN(c);
    chimera_mkdir(env.client_thread, "/test/a/b", 9, mkdir_cb, &c);
    WAIT(env, c);
    if (c.status != 0) {
        fprintf(stderr, "mkdir /test/a/b (deep) failed: %d\n", c.status);
        client_test_fail(&env);
    }

    /* Create a file two levels deep, write, read back, verify. */
    RUN(c);
    chimera_open(env.client_thread, "/test/a/b/file", 14, CHIMERA_VFS_OPEN_CREATE,
                 open_cb, &c);
    WAIT(env, c);
    if (c.status != 0 || !c.handle) {
        fprintf(stderr, "create deep file failed: %d\n", c.status);
        client_test_fail(&env);
    }

    struct chimera_vfs_open_handle *fh = c.handle;

    RUN(c);
    chimera_write(env.client_thread, fh, 0, plen, payload, write_cb, &c);
    WAIT(env, c);
    if (c.status != 0) {
        fprintf(stderr, "write failed: %d\n", c.status);
        client_test_fail(&env);
    }

    RUN(c);
    chimera_read(env.client_thread, fh, 0, plen, read_cb, &c);
    WAIT(env, c);
    if (c.status != 0 || c.rlen != plen || memcmp(c.rbuf, payload, plen) != 0) {
        fprintf(stderr, "read-back mismatch: status %d len %u/%u\n",
                c.status, c.rlen, plen);
        client_test_fail(&env);
    }
    fprintf(stderr, "deep-path write/read round-trip verified (%u bytes)\n", plen);

    chimera_close(env.client_thread, fh);

    /* Large IO: exercise read/write chunking (well above the 64 KiB write PDU
     * and not a chunk multiple, so the last chunk is partial). */
    {
        uint32_t big = 300000;
        char    *src = malloc(big);
        char    *dst = malloc(big);
        uint32_t k;

        for (k = 0; k < big; k++) {
            src[k] = (char) ((k * 2654435761u) >> 24);
        }

        RUN(c);
        chimera_open(env.client_thread, "/test/a/b/big", 13, CHIMERA_VFS_OPEN_CREATE,
                     open_cb, &c);
        WAIT(env, c);
        if (c.status != 0 || !c.handle) {
            fprintf(stderr, "create big file failed: %d\n", c.status);
            client_test_fail(&env);
        }
        fh = c.handle;

        RUN(c);
        chimera_write(env.client_thread, fh, 0, big, src, write_cb, &c);
        WAIT(env, c);
        if (c.status != 0) {
            fprintf(stderr, "big write failed: %d\n", c.status);
            client_test_fail(&env);
        }

        RUN(c);
        c.rdyn     = dst;
        c.rdyn_max = big;
        chimera_read(env.client_thread, fh, 0, big, read_cb, &c);
        WAIT(env, c);
        c.rdyn = NULL;
        if (c.status != 0 || c.rlen != big || memcmp(dst, src, big) != 0) {
            fprintf(stderr, "big read-back mismatch: status %d len %u/%u\n",
                    c.status, c.rlen, big);
            client_test_fail(&env);
        }
        fprintf(stderr, "large-IO chunked write/read round-trip verified (%u bytes)\n", big);

        chimera_close(env.client_thread, fh);
        free(src);
        free(dst);
    }

    /* readdir /test/a should list "b". */
    RUN(c);
    chimera_open(env.client_thread, "/test/a", 7, CHIMERA_VFS_OPEN_DIRECTORY,
                 open_cb, &c);
    WAIT(env, c);
    if (c.status != 0 || !c.handle) {
        fprintf(stderr, "open dir /test/a failed: %d\n", c.status);
        client_test_fail(&env);
    }

    memset(&rc, 0, sizeof(rc));
    chimera_readdir(env.client_thread, c.handle, 0,
                    readdir_entry_cb, readdir_complete_cb, &rc);
    while (!rc.done) {
        evpl_continue(env.evpl);
    }
    chimera_close(env.client_thread, c.handle);

    if (rc.status != 0 || !rc.found_b) {
        fprintf(stderr, "readdir of /test/a failed: status %d count %d found_b %d\n",
                rc.status, rc.count, rc.found_b);
        client_test_fail(&env);
    }
    fprintf(stderr, "readdir verified (%d entries, found 'b')\n", rc.count);

    RUN(c);
    chimera_umount(env.client_thread, "/test", mount_cb, &c);
    WAIT(env, c);

    client_test_success(&env);
    return 0;
} /* main */
