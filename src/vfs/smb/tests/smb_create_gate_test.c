// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * vfs_smb engine create gate test.
 *
 * vfs_smb is the real path-only (CHIMERA_VFS_CAP_FS_PATH_OP) backend: it
 * proxies to an SMB server and reports POSIX mode/uid/gid through the
 * modefromsid security descriptor.  This mounts an in-process SMB server's
 * memfs share through it and drives creating opens as a non-root AUTH_UNIX
 * caller, pinning that the engine's create gate (not the backend) decides:
 *
 *   S1  a mode-restricted parent refuses a create through a multi-component
 *       path (prefix-lookup leg) and a single-component one (parent = handle)
 *   S2  opening the parent up lets the same create succeed
 *   S3  a missing prefix surfaces ENOENT, neither EACCES nor a grant
 *   S4  a refused EXCLUSIVE create answers EEXIST for an existing name and
 *       EACCES for a missing one (the path-only existence probe)
 *   S5  what an AUTH_ATTR create does when the prefix is not traversable
 *   S6  a granted create in a setgid parent gets the parent's group --
 *       vfs_smb declares CREATE_GID_ENGINE, so the engine owes the
 *       inheritance, decided from the same parent fetch the gate used
 */

#include "common/logging.h"
#include "prometheus-c.h"
#include "server/server.h"
#include "common/test_users.h"
#include "vfs/sdk/chimera_vfs_sdk.h"
#include "vfs/vfs_procs.h"
#include "evpl/evpl.h"
#include "common/tcp_flavor.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <unistd.h>
#endif /* ifdef _WIN32 */

#include "vfs/vfs.h"
#include "vfs/vfs_release.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_cred.h"

#define UID_OWNER 1000
#define UID_A     2000

struct test_ctx {
    int                             done;
    enum chimera_vfs_error          status;
    struct evpl                    *evpl;
    struct chimera_vfs_thread      *thread;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;
    struct chimera_vfs_open_handle *handle;
    uint32_t                        gid;
    int                             gid_valid;
    int                             failures;
};

static void
wait_done(struct test_ctx *ctx)
{
    while (!ctx->done) {
        evpl_continue(ctx->evpl);
    }
    ctx->done = 0;
} /* wait_done */

static void
status_cb(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct test_ctx *ctx = private_data;

    (void) thread;
    ctx->status = status;
    ctx->done   = 1;
} /* status_cb */

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
lookup_gid_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status    = error_code;
    ctx->gid_valid = 0;
    if (error_code == CHIMERA_VFS_OK && attr &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_GID)) {
        ctx->gid       = attr->va_gid;
        ctx->gid_valid = 1;
    }
    ctx->done = 1;
} /* lookup_gid_cb */

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

static void
openat_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre,
    struct chimera_vfs_attrs       *dir_post,
    void                           *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->handle = oh;
    ctx->done   = 1;
} /* openat_cb */

static void
mkdir_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre,
    struct chimera_vfs_attrs *dir_post,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    if (error_code == CHIMERA_VFS_OK && attr &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        memcpy(ctx->fh, attr->va_fh, attr->va_fh_len);
        ctx->fh_len = attr->va_fh_len;
    }
    ctx->done = 1;
} /* mkdir_cb */

static void
setattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* setattr_cb */

static void
expect(
    struct test_ctx       *ctx,
    const char            *what,
    enum chimera_vfs_error got,
    enum chimera_vfs_error want)
{
    if (got != want) {
        fprintf(stderr, "  FAIL: %s: expected %d, got %d\n", what, want, got);
        ctx->failures++;
    } else {
        fprintf(stderr, "  PASS: %s\n", what);
    }
} /* expect */

/* Root mkdir of `name` under the mount root; leaves an open handle on it in
 * ctx->handle and returns it. */
static struct chimera_vfs_open_handle *
make_dir(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *root,
    struct chimera_vfs_open_handle *mnt,
    const char                     *name)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0755;
    chimera_vfs_mkdir_at(ctx->thread, root, mnt, name, strlen(name), &sattr,
                         CHIMERA_VFS_ATTR_FH, 0, 0, mkdir_cb, ctx);
    wait_done(ctx);
    if (ctx->status != CHIMERA_VFS_OK) {
        fprintf(stderr, "mkdir %s failed: %d\n", name, ctx->status);
        exit(EXIT_FAILURE);
    }
    chimera_vfs_open_fh(ctx->thread, root, ctx->fh, ctx->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, ctx);
    wait_done(ctx);
    if (ctx->status != CHIMERA_VFS_OK) {
        fprintf(stderr, "open %s failed: %d\n", name, ctx->status);
        exit(EXIT_FAILURE);
    }
    return ctx->handle;
} /* make_dir */

/* Root setattr of `dir`'s mode, owner and group. */
static void
set_mode(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *root,
    struct chimera_vfs_open_handle *dir,
    uint32_t                        mode,
    uint32_t                        uid)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
        CHIMERA_VFS_ATTR_GID;
    sattr.va_mode = mode;
    sattr.va_uid  = uid;
    sattr.va_gid  = uid;
    chimera_vfs_setattr(ctx->thread, root, dir, &sattr, 0, 0, setattr_cb, ctx);
    wait_done(ctx);
    if (ctx->status != CHIMERA_VFS_OK) {
        fprintf(stderr, "setattr failed: %d\n", ctx->status);
        exit(EXIT_FAILURE);
    }
} /* set_mode */

/* open_at of the whole in-mount path `path` as `cred`; a granted open is
 * released. */
static enum chimera_vfs_error
open_as(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *mnt,
    const char                     *path,
    unsigned int                    flags)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0644;
    ctx->handle       = NULL;
    chimera_vfs_open_at(ctx->thread, cred, mnt, path, strlen(path), flags,
                        &sattr, CHIMERA_VFS_ATTR_FH, 0, 0, openat_cb, ctx);
    wait_done(ctx);
    if (ctx->status == CHIMERA_VFS_OK && ctx->handle) {
        chimera_vfs_release(ctx->thread, ctx->handle);
    }
    return ctx->status;
} /* open_as */

int
main(
    int    argc,
    char **argv)
{
    struct chimera_server          *server;
    struct chimera_server_config   *config;
    struct prometheus_metrics      *metrics;
    struct chimera_vfs             *vfs;
    struct chimera_vfs_thread      *thread;
    struct evpl                    *evpl;
    struct test_ctx                 ctx;
    struct chimera_vfs_cred         root, unix_a, attr_a;
    struct chimera_vfs_open_handle *mnt, *d, *d2, *d3;
    uint8_t                         vroot_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        vroot_fh_len;
    uint8_t                         d3_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        d3_fh_len;
    int                             rc = EXIT_FAILURE;

    (void) argc;
    (void) argv;

    ChimeraLogLevel = CHIMERA_LOG_INFO;
    evpl_set_log_fn(chimera_vlog, chimera_log_flush);

    metrics = prometheus_metrics_create(NULL, NULL, 0);

    config = chimera_server_config_init();
    chimera_server_config_set_tcp_flavor(config, CHIMERA_TCP_FLAVOR_INPROC);
    chimera_server_config_set_smb_enabled(config, 1);
    /* Report and accept POSIX mode/uid/gid through the security descriptor,
     * which is what lets a mode-restricted directory exist over the proxy. */
    chimera_server_config_set_smb_mode_from_sid(config, 1);
    chimera_server_config_add_module(config, "smb", NULL, "");

    server = chimera_server_init(config, metrics);
    if (!server) {
        fprintf(stderr, "Failed to initialize server\n");
        return EXIT_FAILURE;
    }

    if (chimera_server_mkfs(server, "memfs", "fs0", NULL) != 0 ||
        chimera_server_mount(server, "share", "memfs", "fs0", NULL) != 0) {
        fprintf(stderr, "Failed to set up the memfs share\n");
        return EXIT_FAILURE;
    }

    chimera_server_start(server);
    chimera_test_add_server_users(server);
    chimera_server_create_share(server, "share", "share", 0);
    usleep(200000);

    vfs    = chimera_server_get_vfs(server);
    evpl   = evpl_create(NULL);
    thread = chimera_vfs_thread_init(evpl, vfs);

    memset(&ctx, 0, sizeof(ctx));
    ctx.evpl   = evpl;
    ctx.thread = thread;

    chimera_vfs_cred_init_unix(&root, 0, 0, 0, NULL);
    chimera_vfs_cred_init_unix(&unix_a, UID_A, UID_A, 0, NULL);
    chimera_vfs_cred_init_attr(&attr_a, UID_A, UID_A, 0, NULL);

    chimera_vfs_mount(thread, NULL, "/smbclient", "smb", "127.0.0.1:share",
                      "user=root,password=secret,domain=WORKGROUP",
                      status_cb, &ctx);
    wait_done(&ctx);
    if (ctx.status != CHIMERA_VFS_OK) {
        fprintf(stderr, "SMB mount failed: vfs error %d\n", ctx.status);
        goto out;
    }

    chimera_vfs_get_root_fh(vfs, vroot_fh, &vroot_fh_len);
    chimera_vfs_lookup(thread, &root, vroot_fh, vroot_fh_len, "smbclient", 9,
                       CHIMERA_VFS_ATTR_FH, 0,
                       lookup_cb, &ctx);
    wait_done(&ctx);
    if (ctx.status != CHIMERA_VFS_OK) {
        fprintf(stderr, "mount root lookup failed: %d\n", ctx.status);
        goto out;
    }
    chimera_vfs_open_fh(thread, &root, ctx.fh, ctx.fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, &ctx);
    wait_done(&ctx);
    if (ctx.status != CHIMERA_VFS_OK) {
        fprintf(stderr, "mount root open failed: %d\n", ctx.status);
        goto out;
    }
    mnt = ctx.handle;

    /* The share root is 0755 root-owned, whatever the backing filesystem's
     * default. */
    set_mode(&ctx, &root, mnt, 0755, 0);

    /* S1: a 0555 uid-1000 directory refuses the create, and so does the
     * 0755 root-owned share root for a single-component name. */
    d = make_dir(&ctx, &root, mnt, "d");
    set_mode(&ctx, &root, d, 0555, UID_OWNER);
    expect(&ctx, "S1 prefix-leg create in a 0555 parent refused",
           open_as(&ctx, &unix_a, mnt, "d/f",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY),
           CHIMERA_VFS_EACCES);
    expect(&ctx, "S1 single-component create under the 0755 root refused",
           open_as(&ctx, &unix_a, mnt, "x",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY),
           CHIMERA_VFS_EACCES);
    expect(&ctx, "S1 root still creates in the 0555 parent",
           open_as(&ctx, &root, mnt, "d/f_exists",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY),
           CHIMERA_VFS_OK);

    /* S4: the refused EXCLUSIVE create's existence probe. */
    expect(&ctx, "S4 refused exclusive create of an existing name is EEXIST",
           open_as(&ctx, &unix_a, mnt, "d/f_exists",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE |
                   CHIMERA_VFS_OPEN_READ_ONLY),
           CHIMERA_VFS_EEXIST);
    expect(&ctx, "S4 refused exclusive create of a missing name is EACCES",
           open_as(&ctx, &unix_a, mnt, "d/f_none",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE |
                   CHIMERA_VFS_OPEN_READ_ONLY),
           CHIMERA_VFS_EACCES);
    expect(&ctx, "S4 root lookup after the refused create finds nothing",
           open_as(&ctx, &root, mnt, "d/f_none", 0), CHIMERA_VFS_ENOENT);

    /* S3: a missing prefix surfaces the fetch error. */
    expect(&ctx, "S3 create under a missing prefix is ENOENT",
           open_as(&ctx, &unix_a, mnt, "missing/f",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY),
           CHIMERA_VFS_ENOENT);

    /* S2: open the parent up and the same create succeeds. */
    set_mode(&ctx, &root, d, 0777, UID_OWNER);
    expect(&ctx, "S2 prefix-leg create in a 0777 parent succeeds",
           open_as(&ctx, &unix_a, mnt, "d/f",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY),
           CHIMERA_VFS_OK);

    /* S5: AUTH_ATTR creates.  The first is the ordinary granted/refused
     * pair; the second reaches the parent through a prefix the caller may
     * not traverse (0700 owned by another uid). */
    expect(&ctx, "S5 AUTH_ATTR create in a 0777 parent",
           open_as(&ctx, &attr_a, mnt, "d/g",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY),
           CHIMERA_VFS_OK);
    set_mode(&ctx, &root, d, 0555, UID_OWNER);
    expect(&ctx, "S5 AUTH_ATTR create in a 0555 parent refused",
           open_as(&ctx, &attr_a, mnt, "d/h",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY),
           CHIMERA_VFS_EACCES);

    d2 = make_dir(&ctx, &root, mnt, "d2");
    {
        struct chimera_vfs_attrs sattr;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        sattr.va_mode     = 0777;
        chimera_vfs_mkdir_at(thread, &root, d2, "sub", 3, &sattr,
                             CHIMERA_VFS_ATTR_FH, 0, 0, mkdir_cb, &ctx);
        wait_done(&ctx);
        expect(&ctx, "S5 setup: root makes d2/sub", ctx.status,
               CHIMERA_VFS_OK);
    }
    set_mode(&ctx, &root, d2, 0700, UID_OWNER);
    /* A path-only backend resolves the whole prefix in one lookup against the
     * mount root, so the engine checks the final parent only, never the
     * intermediate components: an AUTH_ATTR caller does not owe traverse in
     * the SMB model, and an AUTH_UNIX caller's missing search on "d2" is not
     * seen either.  The AUTH_UNIX create therefore SUCCEEDS where POSIX owes
     * EACCES -- a known limit of the path-only leg, documented in
     * docs/vfs-module-sdk.md.  It is pinned here so that any change in the
     * answer -- enforcement arriving, above all -- surfaces. */
    expect(&ctx, "S5 AUTH_ATTR create past an untraversable prefix",
           open_as(&ctx, &attr_a, mnt, "d2/sub/f",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY),
           CHIMERA_VFS_OK);
    expect(&ctx, "S5 AUTH_UNIX create past an untraversable prefix "
           "(known limit: intermediate traverse unenforced)",
           open_as(&ctx, &unix_a, mnt, "d2/sub/f2",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY),
           CHIMERA_VFS_OK);
    chimera_vfs_release(thread, d2);

    /* S6: the engine's gid inheritance.  vfs_smb declares CREATE_GID_ENGINE,
     * so on a setgid parent the engine, not the backend, puts the parent's
     * group on the create -- for a gated caller, from the same parent fetch
     * the gate decided on.  The caller names no group of its own. */
    d3 = make_dir(&ctx, &root, mnt, "d3");
    memcpy(d3_fh, ctx.fh, ctx.fh_len);
    d3_fh_len = ctx.fh_len;
    set_mode(&ctx, &root, d3, 02777, UID_OWNER);
    expect(&ctx, "S6 gated create in a setgid parent succeeds",
           open_as(&ctx, &unix_a, mnt, "d3/sg",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY),
           CHIMERA_VFS_OK);
    chimera_vfs_lookup(thread, &root, d3_fh, d3_fh_len, "sg", 2,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_GID, 0,
                       lookup_gid_cb, &ctx);
    wait_done(&ctx);
    expect(&ctx, "S6 root finds the created file", ctx.status,
           CHIMERA_VFS_OK);
    if (!ctx.gid_valid || ctx.gid != UID_OWNER) {
        fprintf(stderr, "  FAIL: S6 created file carries gid %u (valid %d), "
                "not the setgid parent's %u\n",
                ctx.gid, ctx.gid_valid, UID_OWNER);
        ctx.failures++;
    } else {
        fprintf(stderr, "  PASS: S6 setgid parent's group inherited\n");
    }
    chimera_vfs_release(thread, d3);

    rc = ctx.failures ? EXIT_FAILURE : EXIT_SUCCESS;
    fprintf(stderr, "SMB create gate test %s\n", rc ? "FAILED" : "PASSED");

    chimera_vfs_release(thread, d);
    chimera_vfs_release(thread, mnt);
    chimera_vfs_umount(thread, NULL, "/smbclient", status_cb, &ctx);
    wait_done(&ctx);

 out:
    chimera_vfs_thread_destroy(thread);
    evpl_destroy(evpl);
    chimera_server_destroy(server);
    prometheus_metrics_destroy(metrics);

    return rc;
} /* main */
