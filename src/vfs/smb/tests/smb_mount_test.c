// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * SMB2 client mount smoke test.
 *
 * Starts an in-process Chimera SMB *server* exporting a memfs share, then
 * drives the SMB2 *client* VFS module (vfs_smb) to MOUNT the share -- which
 * runs the full NEGOTIATE -> SESSION_SETUP (NTLMv2) -> TREE_CONNECT handshake
 * over a loopback TCP connection -- and then UMOUNT it (TREE_DISCONNECT ->
 * LOGOFF -> disconnect).  A successful mount proves the session was
 * established and torn down end to end.
 *
 * MOUNT and UMOUNT are driven on a single client evpl thread so the SMB
 * connection (which lives on that thread's evpl) stays valid across both.
 */

#include "common/logging.h"
#include "prometheus-c.h"
#include "server/server.h"
#include "common/test_users.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "evpl/evpl.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

struct op_ctx {
    int done;
    enum chimera_vfs_error status;
};

static void
mount_callback(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct op_ctx *ctx = private_data;

    (void) thread;
    ctx->status = status;
    ctx->done   = 1;
} /* mount_callback */

static void
umount_callback(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct op_ctx *ctx = private_data;

    (void) thread;
    ctx->status = status;
    ctx->done   = 1;
} /* umount_callback */

int
main(
    int    argc,
    char **argv)
{
    struct chimera_server        *server;
    struct chimera_server_config *config;
    struct prometheus_metrics    *metrics;
    struct chimera_vfs           *vfs;
    struct chimera_vfs_thread    *thread;
    struct evpl                  *evpl;
    struct op_ctx                 ctx;
    int                           rc;

    (void) argc;
    (void) argv;

    ChimeraLogLevel = CHIMERA_LOG_INFO;
    evpl_set_log_fn(chimera_vlog, chimera_log_flush);

    metrics = prometheus_metrics_create(NULL, NULL, 0);

    config = chimera_server_config_init();
    /* Register the SMB2 client VFS module (statically linked into chimera_vfs;
     * empty path => resolve the vfs_smb symbol, do not dlopen). */
    chimera_server_config_add_module(config, "smb", NULL, "");

    server = chimera_server_init(config, metrics);
    if (!server) {
        fprintf(stderr, "Failed to initialize server\n");
        return EXIT_FAILURE;
    }

    /* Export a memfs share over SMB. */
    if (chimera_server_mount(server, "share", "memfs", "/", NULL) != 0) {
        fprintf(stderr, "Failed to mount memfs share\n");
        return EXIT_FAILURE;
    }

    chimera_server_start(server);
    chimera_test_add_server_users(server);
    chimera_server_create_share(server, "share", "share", 0);

    /* Let the server's listener come up. */
    usleep(200000);

    /* ---- client side: mount the share through vfs_smb ---- */
    vfs    = chimera_server_get_vfs(server);
    evpl   = evpl_create(NULL);
    thread = chimera_vfs_thread_init(evpl, vfs);

    ctx.done = 0;
    chimera_vfs_mount(thread, NULL,
                      "smbclient",                 /* local mount name */
                      "smb",                       /* module */
                      "127.0.0.1:share",           /* host:share */
                      "user=myuser,password=mypassword,domain=WORKGROUP",
                      mount_callback, &ctx);

    while (!ctx.done) {
        evpl_continue(evpl);
    }

    if (ctx.status != CHIMERA_VFS_OK) {
        fprintf(stderr, "SMB mount failed: vfs error %d\n", ctx.status);
        rc = EXIT_FAILURE;
        goto out;
    }

    fprintf(stderr, "SMB mount established; tearing down\n");

    ctx.done = 0;
    chimera_vfs_umount(thread, NULL, "smbclient", umount_callback, &ctx);

    while (!ctx.done) {
        evpl_continue(evpl);
    }

    if (ctx.status != CHIMERA_VFS_OK) {
        fprintf(stderr, "SMB umount failed: vfs error %d\n", ctx.status);
        rc = EXIT_FAILURE;
        goto out;
    }

    fprintf(stderr, "SMB mount/umount smoke test PASSED\n");
    rc = EXIT_SUCCESS;

 out:
    chimera_vfs_thread_destroy(thread);
    evpl_destroy(evpl);
    chimera_server_destroy(server);
    prometheus_metrics_destroy(metrics);

    return rc;
} /* main */
