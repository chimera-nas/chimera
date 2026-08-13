// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Regression test for umount's completion reaching a caller that drives its
 * own event loop.
 *
 * umount is the one VFS operation that can finish from a timer rather than
 * from a backend completion: when a handle it does not own still references
 * the mount, it re-checks on a poll timer and eventually gives up with EBUSY.
 *
 * evpl_continue() runs the due timers and then waits for an fd event, and
 * waits indefinitely by default (wait_ms is -1).  So a caller spinning on
 * `while (!done) evpl_continue(evpl)` -- which is exactly what
 * chimera_server_unmount and the other synchronous wrappers in server.c do,
 * and what wait_done() below does -- would have `done` set from inside
 * evpl_continue and then block in it forever, never re-testing the flag.  The
 * umount would have completed and the caller would hang anyway.
 *
 * The test holds an open handle across the umount so the wait path is
 * genuinely taken, and is bounded by an alarm: before the fix this hangs
 * rather than failing, so a plain assert would never be reached.
 *
 * It then releases the handle and unmounts again, which must succeed -- so a
 * "fix" that merely made umount give up early would not pass either.
 */

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <signal.h>
#include <stdlib.h>
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_cred.h"
#include "vfs/sdk/vfs_error.h"
#include "common/logging.h"
#include "prometheus-c.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

/* Comfortably longer than the ~1s umount timeout, short enough that a hung
 * test fails the suite rather than stalling it. */
#define TEST_WATCHDOG_SECS 30

struct test_ctx {
    struct evpl                    *evpl;
    struct chimera_vfs_thread      *vfs_thread;
    enum chimera_vfs_error          status;
    int                             done;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;
    struct chimera_vfs_open_handle *handle;
};

static void
watchdog(int sig)
{
    (void) sig;
    fprintf(stderr,
            "FAIL: timed out -- umount completed but its caller never woke.\n"
            "      evpl_continue() ran the timer that finished the umount and\n"
            "      then blocked waiting for an fd event that never came.\n");
    _exit(1);
} /* watchdog */

static void
wait_done(struct test_ctx *ctx)
{
    while (!ctx->done) {
        evpl_continue(ctx->evpl);
    }
    ctx->done = 0;
} /* wait_done */

static void
mount_cb(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = status;
    ctx->done   = 1;
} /* mount_cb */

static void
umount_cb(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = status;
    ctx->done   = 1;
} /* umount_cb */

static void
lookup_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    if (error_code == CHIMERA_VFS_OK) {
        memcpy(ctx->fh, attr->va_fh, attr->va_fh_len);
        ctx->fh_len = attr->va_fh_len;
    }
    ctx->done = 1;
} /* lookup_cb */

static void
openfh_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->handle = oh;
    ctx->done   = 1;
} /* openfh_cb */

int
main(
    int    argc,
    char **argv)
{
    struct test_ctx               ctx = { 0 };
    struct chimera_vfs           *vfs;
    struct chimera_vfs_module_cfg module_cfgs[2];
    struct prometheus_metrics    *metrics;
    struct chimera_vfs_cred       cred;
    uint8_t                       root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                      root_fh_len;

    chimera_log_init();

    signal(SIGALRM, watchdog);
    alarm(TEST_WATCHDOG_SECS);

    chimera_vfs_cred_init_unix(&cred, 0, 0, 0, NULL);

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(metrics != NULL);

    memset(module_cfgs, 0, sizeof(module_cfgs));
    strncpy(module_cfgs[0].module_name, "memfs", sizeof(module_cfgs[0].module_name) - 1);
    strncpy(module_cfgs[1].module_name, "memkv", sizeof(module_cfgs[1].module_name) - 1);

    ctx.evpl = evpl_create(NULL);
    assert(ctx.evpl != NULL);

    vfs = chimera_vfs_init(0, 0, module_cfgs, 2, "memkv", 60, 1, 1, 0, metrics);
    assert(vfs != NULL);

    ctx.vfs_thread = chimera_vfs_thread_init(ctx.evpl, vfs);
    assert(ctx.vfs_thread != NULL);

    chimera_vfs_mkfs(ctx.vfs_thread, NULL, "memfs", "fs0", NULL,
                     mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_mount(ctx.vfs_thread, NULL, "/test", "memfs", "fs0", NULL,
                      mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    /* Take a handle on the mount root and keep it, so umount finds a
     * reference it cannot dispose of and has to wait on the poll timer. */
    chimera_vfs_get_root_fh(root_fh, &root_fh_len);
    chimera_vfs_lookup(ctx.vfs_thread, &cred, root_fh, root_fh_len, "test", 4,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                       lookup_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_open_fh(ctx.vfs_thread, &cred, ctx.fh, ctx.fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    /* The handle is still held, so this takes the wait path and ends in
     * EBUSY.  Reaching the assert at all is the point of the test. */
    chimera_vfs_umount(ctx.vfs_thread, NULL, "/test", umount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_EBUSY);
    TEST_PASS("umount of a referenced mount reports EBUSY to a caller driving evpl");

    /* With the reference gone the same call must succeed, so this cannot be
     * satisfied by an umount that simply refuses. */
    chimera_vfs_release(ctx.vfs_thread, ctx.handle);

    chimera_vfs_umount(ctx.vfs_thread, NULL, "/test", umount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    TEST_PASS("umount succeeds once the reference is dropped");

    /* A successful umount means every handle on the mount is gone, so the
     * filesystem is removable right away -- no retry, no waiting on a sweep.
     * That is the guarantee RMFS's mount-count test relies on. */
    chimera_vfs_rmfs(ctx.vfs_thread, NULL, "memfs", "fs0", mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    TEST_PASS("filesystem is removable immediately after umount returns");

    alarm(0);

    chimera_vfs_thread_destroy(ctx.vfs_thread);
    chimera_vfs_destroy(vfs);
    evpl_destroy(ctx.evpl);
    prometheus_metrics_destroy(metrics);

    fprintf(stderr, "All umount-wait tests passed\n");
    return 0;
} /* main */
