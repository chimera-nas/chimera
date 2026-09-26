// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * End-to-end check of chimera_read_into(): write a known pattern to a memfs
 * file, read it back into a caller-provided evpl_iovec, and verify the bytes
 * landed in that buffer.  memfs advertises CAP_READ_PROVIDES_BUFFERS, so this
 * exercises accepted-only compound publication to segmented destinations.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>

#include "client/client.h"
#include "evpl/evpl.h"
#include "prometheus-c.h"

#define TEST_LEN        65536
#define DIRECTORY_FILES 600

extern void chimera_test_reject_finishes(
    int count) __attribute__((weak));
extern void chimera_test_operation_eagain(
    void) __attribute__((weak));
extern int chimera_test_finish_attempts(
    void) __attribute__((weak));
extern void chimera_test_finish_observer(
    void ( *fn )(void *),
    void   *arg) __attribute__((weak));

struct op_ctx {
    int      done;
    int      status;
    uint32_t count;
    uint32_t eof;
};

static void
mount_callback(
    struct chimera_client_thread *client,
    enum chimera_vfs_error        status,
    void                         *private_data)
{
    struct op_ctx *ctx = private_data;

    ctx->status = status;
    ctx->done++;
} /* mount_callback */

static void
open_callback(
    struct chimera_client_thread   *client,
    enum chimera_vfs_error          status,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct chimera_vfs_open_handle **handle = private_data;

    if (status != CHIMERA_VFS_OK) {
        fprintf(stderr, "open failed: %d\n", status);
        exit(1);
    }
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
    ctx->done++;
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
    ctx->eof    = eof;
    ctx->done++;
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

struct directory_ctx {
    struct op_ctx op;
    uint64_t      cookie;
    int           seen[DIRECTORY_FILES];
    int           files, dots, entries, stop_after, failed;
};

static void
observe_directory(void *arg)
{
    struct directory_ctx *ctx = arg;

    if (ctx->op.done || ctx->entries || ctx->cookie) {
        fprintf(stderr, "READDIR published before acceptance\n");
        abort();
    }
} /* observe_directory */

struct read_observer {
    struct op_ctx     *ctx;
    struct evpl_iovec *iov;
};

static void
observe_read(void *arg)
{
    struct read_observer *obs = arg;

    if (obs->ctx->done) {
        abort();
    }
    for (int segment = 0; segment < 2; segment++) {
        for (size_t i = 0; i < obs->iov[segment].length; i++) {
            if (((unsigned char *) obs->iov[segment].data)[i] != 0xa5) {
                fprintf(stderr, "READ_INTO published before acceptance\n");
                abort();
            }
        }
    }
} /* observe_read */

static int
directory_entry(
    struct chimera_client_thread *thread,
    const struct chimera_dirent  *entry,
    void                         *arg)
{
    struct directory_ctx *ctx = arg;

    ctx->entries++;
    if (!strcmp(entry->name, ".") || !strcmp(entry->name, "..")) {
        ctx->dots++;
    } else {
        int n = -1;
        if (sscanf(entry->name, "item-%d", &n) != 1 || n < 0 || n >= DIRECTORY_FILES) {
            ctx->failed = 1;
        } else if (ctx->seen[n]++) {
            ctx->failed = 1;
        } else {
            ctx->files++;
        }
    }
    return ctx->stop_after && ctx->entries >= ctx->stop_after;
} /* directory_entry */

static void
directory_complete(
    struct chimera_client_thread *thread,
    enum chimera_vfs_error        status,
    uint64_t                      cookie,
    int                           eof,
    void                         *arg)
{
    struct directory_ctx *ctx = arg;

    ctx->op.status = status;
    ctx->op.eof    = eof;
    ctx->op.done++;
    ctx->cookie = cookie;
} /* directory_complete */

static int
test_directory(
    struct chimera_client_thread *thread,
    struct evpl                  *evpl)
{
    struct op_ctx                   op     = { 0 };
    struct directory_ctx            dir    = { 0 };
    struct chimera_vfs_open_handle *handle = NULL;
    char                            path[64];

    chimera_mkdir(thread, "/memfs/pages", strlen("/memfs/pages"), mount_callback, &op);
    wait_done(evpl, &op);
    if (op.status) {
        return 1;
    }
    for (int n = 0; n < DIRECTORY_FILES; n++) {
        snprintf(path, sizeof(path), "/memfs/pages/item-%d", n);
        handle = NULL;
        chimera_open(thread, path, strlen(path), CHIMERA_VFS_OPEN_CREATE,
                     open_callback, &handle);
        while (!handle) {
            evpl_continue(evpl);
        }
        chimera_close(thread, handle);
    }
    handle = NULL;
    chimera_open(thread, "/memfs/pages", strlen("/memfs/pages"),
                 CHIMERA_VFS_OPEN_DIRECTORY | CHIMERA_VFS_OPEN_READ_ONLY, open_callback, &handle);
    while (!handle) {
        evpl_continue(evpl);
    }
    if (chimera_test_reject_finishes) {
        for (int reject = 2; reject <= 20; reject += 18) {
            chimera_test_reject_finishes(reject);
            chimera_test_finish_observer(observe_directory, &dir);
            chimera_readdir(thread, handle, 0, directory_entry, directory_complete, &dir);
            wait_done(evpl, &dir.op);
            chimera_test_finish_observer(NULL, NULL);
            if (dir.op.done != 1 || dir.failed ||
                chimera_test_finish_attempts() != (reject == 2 ? 3 : 9) ||
                (reject == 2 ? (dir.op.status || dir.entries != 512) :
                 (dir.op.status != CHIMERA_VFS_EAGAIN || dir.entries || dir.cookie))) {
                fprintf(stderr, "READDIR retry/publication contract failed\n");
                return 1;
            }
            memset(&dir, 0, sizeof(dir));
        }
    }
    dir.stop_after = 3;
    chimera_readdir(thread, handle, 0, directory_entry, directory_complete, &dir);
    wait_done(evpl, &dir.op);
    if (dir.op.status || dir.entries != 3 || dir.op.eof) {
        return 1;
    }
    dir.stop_after = 0;
    for (int page = 0; !dir.op.eof && page < DIRECTORY_FILES + 4; page++) {
        uint64_t before = dir.cookie;
        dir.op.done = 0;
        chimera_readdir(thread, handle, dir.cookie, directory_entry, directory_complete, &dir);
        wait_done(evpl, &dir.op);
        if (dir.op.status || dir.op.done != 1 || (!dir.op.eof && before == dir.cookie)) {
            fprintf(stderr, "READDIR failed or made no cookie progress\n");
            return 1;
        }
    }
    chimera_close(thread, handle);
    if (!dir.op.eof || dir.failed || dir.files != DIRECTORY_FILES || dir.dots != 2) {
        fprintf(stderr, "READDIR paging lost/duplicated entries: files=%d dots=%d failed=%d\n",
                dir.files, dir.dots, dir.failed);
        return 1;
    }
    return 0;
} /* test_directory */

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
    chimera_mkfs(thread, "memfs", "fs0", NULL, mount_callback, &ctx);
    wait_done(evpl, &ctx);
    if (ctx.status != 0) {
        fprintf(stderr, "mkfs failed: %d\n", ctx.status);
        return 1;
    }

    ctx = (struct op_ctx) { 0 };
    chimera_mount(thread, "/memfs", "memfs", "fs0", NULL, mount_callback, &ctx);
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
    if (chimera_test_reject_finishes) {
        chimera_test_reject_finishes(2);
    }
    chimera_writerv(thread, file_handle, 0, TEST_LEN, &wiov, 1, write_callback, &ctx);
    wait_done(evpl, &ctx);
    if (ctx.status != 0) {
        fprintf(stderr, "write failed: %d\n", ctx.status);
        return 1;
    }

    /* The copied writev path must preserve all segments until acceptance. */
    struct iovec pieces[3] = {
        { .iov_base = pattern,        .iov_len               = 7               },
        { .iov_base = pattern + 7,    .iov_len               = 1001            },
        { .iov_base = pattern + 1008, .iov_len               = TEST_LEN - 1008 }
    };
    ctx = (struct op_ctx) { 0 };
    if (chimera_test_reject_finishes) {
        chimera_test_reject_finishes(2);
    }
    chimera_writev(thread, file_handle, 0, TEST_LEN, pieces, 3, write_callback, &ctx);
    wait_done(evpl, &ctx);
    if (ctx.status || ctx.done != 1) {
        return 1;
    }

    if (chimera_test_reject_finishes) {
        if (chimera_test_finish_attempts() != 3) {
            return 1;
        }
        ctx = (struct op_ctx) { 0 };
        chimera_test_operation_eagain();
        chimera_writev(thread, file_handle, 0, TEST_LEN, pieces, 3, write_callback, &ctx);
        wait_done(evpl, &ctx);
        if (ctx.status != CHIMERA_VFS_EAGAIN || ctx.done != 1 || chimera_test_finish_attempts() != 1) {
            fprintf(stderr, "ordinary operation EAGAIN was retried\n");
            return 1;
        }
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

    if (chimera_test_reject_finishes) {
        for (int reject = 2; reject <= 20; reject += 18) {
            struct read_observer obs = { .ctx = &ctx, .iov = riov };
            ctx = (struct op_ctx) { 0 };
            memset(riov[0].data, 0xa5, TEST_LEN / 2);
            memset(riov[1].data, 0xa5, TEST_LEN / 2);
            chimera_test_reject_finishes(reject);
            chimera_test_finish_observer(observe_read, &obs);
            chimera_read_into(thread, file_handle, 0, TEST_LEN, riov, 2, read_into_callback, &ctx);
            wait_done(evpl, &ctx);
            chimera_test_finish_observer(NULL, NULL);
            if (ctx.done != 1 || chimera_test_finish_attempts() != (reject == 2 ? 3 : 9)) {
                return 1;
            }
            if (reject == 2) {
                if (ctx.status || ctx.count != TEST_LEN ||
                    memcmp(riov[0].data, pattern, TEST_LEN / 2) ||
                    memcmp(riov[1].data, pattern + TEST_LEN / 2, TEST_LEN / 2)) {
                    return 1;
                }
            } else {
                if (ctx.status != CHIMERA_VFS_EAGAIN || ctx.count) {
                    return 1;
                }
                ctx.done = 0;
                observe_read(&obs);
            }
        }
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

    /* A short EOF read changes exactly its returned bytes, including a
     * destination boundary; unused tail bytes remain the caller's sentinel. */
    struct evpl_iovec short_dest[2] = { riov[0], riov[1] };
    short_dest[0].length = 8;
    short_dest[1].length = 24;
    memset(riov[0].data, 0xa5, TEST_LEN / 2);
    memset(riov[1].data, 0xa5, TEST_LEN / 2);
    ctx = (struct op_ctx) { 0 };
    chimera_read_into(thread, file_handle, TEST_LEN - 9, 32, short_dest, 2,
                      read_into_callback, &ctx);
    wait_done(evpl, &ctx);
    if (ctx.status || ctx.done != 1 || ctx.count != 9 || !ctx.eof ||
        memcmp(riov[0].data, pattern + TEST_LEN - 9, 8) ||
        ((char *) riov[1].data)[0] != pattern[TEST_LEN - 1]) {
        return 1;
    }
    for (i = 1; i < 24; i++) {
        if (((unsigned char *) riov[1].data)[i] != 0xa5) {
            return 1;
        }
    }
    ctx = (struct op_ctx) { 0 };
    chimera_read_into(thread, file_handle, 0, 0, short_dest, 2, read_into_callback, &ctx);
    wait_done(evpl, &ctx);
    if (ctx.status || ctx.done != 1 || ctx.count) {
        return 1;
    }
    if (test_directory(thread, evpl)) {
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
