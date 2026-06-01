// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * End-to-end check of chimera_read_into(): write a known pattern to a memfs
 * file, read it back into a caller-provided evpl_iovec, and verify the bytes
 * landed in that buffer.  memfs advertises CAP_READ_PROVIDES_BUFFERS, so this
 * exercises the VFS-core scatter-copy fallback in chimera_vfs_read_into().
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "client/client.h"
#include "evpl/evpl.h"
#include "prometheus-c.h"

#define TEST_LEN 65536

struct op_ctx {
    int      done;
    int      status;
    uint32_t count;
};

static void
mount_callback(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct op_ctx *ctx = private_data;

    ctx->status = status;
    ctx->done   = 1;
} /* mount_callback */

static void
open_callback(
    struct chimera_client_thread   *client,
    enum chimera_vfs_error          status,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_vfs_open_handle **handle = private_data;

    *handle = oh;
} /* open_callback */

static void
write_callback(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct op_ctx *ctx = private_data;

    ctx->status = status;
    ctx->done   = 1;
} /* write_callback */

static void
read_into_callback(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    uint32_t                      count,
    uint32_t                      eof,
    void                         *private_data)
{
    struct op_ctx *ctx = private_data;

    ctx->status = status;
    ctx->count  = count;
    ctx->done   = 1;
} /* read_into_callback */

static void
wait_done(
    struct evpl   *evpl,
    struct op_ctx *ctx)
{
    while (!ctx->done) {
        evpl_continue(evpl);
    }
} /* wait_done */

int
main(
    int    argc,
    char **argv)
{
    struct chimera_client          *client;
    struct chimera_client_config   *config;
    struct chimera_client_thread   *thread;
    struct evpl                    *evpl;
    struct chimera_vfs_open_handle *file_handle = NULL;
    struct prometheus_metrics      *metrics;
    struct op_ctx                   ctx;
    struct chimera_vfs_cred         root_cred;
    char                           *pattern;
    struct evpl_iovec               wiov, riov[2];
    char                           *got;
    int                             niov;
    int                             i;

    chimera_log_init();

    metrics = prometheus_metrics_create(NULL, NULL, 0);

    evpl   = evpl_create(NULL);
    config = chimera_client_config_init();

    chimera_vfs_cred_init_unix(&root_cred, 0, 0, 0, NULL);
    client = chimera_client_init(config, &root_cred, metrics);
    thread = chimera_client_thread_init(evpl, client);

    ctx = (struct op_ctx) { 0 };
    chimera_mount(thread, "/memfs", "memfs", "/", NULL, mount_callback, &ctx);
    wait_done(evpl, &ctx);
    if (ctx.status != 0) {
        fprintf(stderr, "mount failed: %d\n", ctx.status);
        return 1;
    }

    chimera_open(thread, "/memfs/rifile", 13, CHIMERA_VFS_OPEN_CREATE,
                 open_callback, &file_handle);
    while (!file_handle) {
        evpl_continue(evpl);
    }

    /* Build a known pattern and write it through the zero-copy write path. */
    pattern = malloc(TEST_LEN);
    for (i = 0; i < TEST_LEN; i++) {
        pattern[i] = (char) (i * 31 + 7);
    }

    niov = evpl_iovec_alloc(evpl, TEST_LEN, 1, 1, 0, &wiov);
    if (niov != 1) {
        fprintf(stderr, "write iovec alloc failed\n");
        return 1;
    }
    memcpy(wiov.data, pattern, TEST_LEN);

    ctx = (struct op_ctx) { 0 };
    chimera_writerv(thread, file_handle, 0, TEST_LEN, &wiov, 1, write_callback, &ctx);
    wait_done(evpl, &ctx);
    if (ctx.status != 0) {
        fprintf(stderr, "write failed: %d\n", ctx.status);
        return 1;
    }

    /* Read it back into a caller-provided two-segment destination, so the
     * VFS-core scatter-copy spans more than one destination iovec. */
    for (i = 0; i < 2; i++) {
        niov = evpl_iovec_alloc(evpl, TEST_LEN / 2, 1, 1, 0, &riov[i]);
        if (niov != 1) {
            fprintf(stderr, "read iovec alloc failed\n");
            return 1;
        }
        memset(riov[i].data, 0, TEST_LEN / 2);
    }

    ctx = (struct op_ctx) { 0 };
    chimera_read_into(thread, file_handle, 0, TEST_LEN, riov, 2, read_into_callback, &ctx);
    wait_done(evpl, &ctx);
    if (ctx.status != 0) {
        fprintf(stderr, "read_into failed: %d\n", ctx.status);
        return 1;
    }

    if (ctx.count != TEST_LEN) {
        fprintf(stderr, "read_into short: got %u want %u\n", ctx.count, TEST_LEN);
        return 1;
    }

    /* Reassemble the two destination segments and compare to the pattern. */
    got = malloc(TEST_LEN);
    memcpy(got, riov[0].data, TEST_LEN / 2);
    memcpy(got + TEST_LEN / 2, riov[1].data, TEST_LEN / 2);

    if (memcmp(got, pattern, TEST_LEN) != 0) {
        fprintf(stderr, "read_into data mismatch\n");
        return 1;
    }

    fprintf(stderr, "read_into round-trip of %d bytes (2 dst segments) verified\n", TEST_LEN);

    evpl_iovec_release(evpl, &riov[0]);
    evpl_iovec_release(evpl, &riov[1]);
    chimera_close(thread, file_handle);
    free(got);
    free(pattern);

    chimera_client_thread_shutdown(evpl, thread);
    chimera_destroy(client);
    prometheus_metrics_destroy(metrics);
    evpl_destroy(evpl);

    return 0;
} /* main */
