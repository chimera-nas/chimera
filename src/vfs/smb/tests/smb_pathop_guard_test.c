// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Path-only parent resolution, driven from a non-root start handle.
 *
 * vfs_smb is the real path-only (CHIMERA_VFS_CAP_FS_PATH_OP) backend.  Every
 * client and POSIX entry point starts at the namespace root, where
 * chimera_vfs_path_op_whole() is false, so the whole-path arms of the path
 * procs are only reachable from a handle inside a path-only mount.  This mounts
 * an in-process SMB server's memfs share through vfs_smb at /smbclient and
 * issues each path proc from that mount's root handle, so the whole-path arm is
 * the one taken.
 *
 * The rows pin that a multi-component name has its parent resolved in the core
 * rather than handed to the backend whole.  The backend joins and collapses
 * "x/.." lexically without looking at x; the core requires the parent to
 * resolve as a real directory first.  So a ".." after a regular file, or after
 * a dangling symlink, must be refused, and nothing may be created, removed or
 * moved under the lexically collapsed name:
 *
 *   S1/S2  symlink  through a regular-file / dangling-symlink prefix
 *   M1     mknod    through a regular-file prefix
 *   R1     remove   through a regular-file prefix
 *   N1     rename   through a regular-file prefix
 *   O1     creating open through a regular-file prefix
 *   P1/P2  positive controls: a real directory parent, and ".." after one
 *   I1     informational: ".." after a symlink to a directory still collapses
 *          lexically (resolving it needs a per-component walk); recorded only
 *   L1     a whole path longer than the backend's path bound is
 *          ENAMETOOLONG, not a stale handle
 *   X1/X2  remove through a symlink into a non-path-only mount keeps the core's
 *          child-type check: the "nochk" module below deletes by name without
 *          checking the type itself, so only the core can refuse X1
 *
 * Each row prints one line, "ROW <id> PASS|FAIL|INFO status=<n> state=ok|bad",
 * and every row runs regardless of earlier failures.
 */

#include "common/logging.h"
#include "common/macros.h"
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
#include <sys/stat.h>
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <unistd.h>
#endif /* ifdef _WIN32 */

#include "vfs/vfs.h"
#include "vfs/vfs_release.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_cred.h"

extern struct chimera_vfs_module vfs_memfs;

/* ---- the nochk module ----------------------------------------------------
 *
 * memfs in every respect (a separate instance, on its own fh_magic) except
 * that, while `nochk_strip` is set, REMOVE_AT is forwarded with the
 * ISDIR/ISNOTDIR assertions cleared.  memfs then deletes whatever the name
 * resolves to -- the behaviour of a backend that cannot enforce the type
 * itself -- so the only thing that can refuse an unlink of a directory is the
 * core's child lookup. */

static int nochk_strip;

static void
nochk_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    if (nochk_strip && request->opcode == CHIMERA_VFS_OP_REMOVE_AT) {
        request->remove_at.flags &= ~(CHIMERA_VFS_REMOVE_ISDIR |
                                      CHIMERA_VFS_REMOVE_ISNOTDIR);
    }
    vfs_memfs.dispatch(request, private_data);
} /* nochk_dispatch */

/* Completed from vfs_memfs in main() before the server starts; found by the
 * VFS via dlsym("vfs_nochk"), which is why the test binary is linked with
 * ENABLE_EXPORTS. */
SYMBOL_EXPORT struct chimera_vfs_module vfs_nochk = {
    .sdk_version = CHIMERA_VFS_SDK_VERSION,
    .name        = "nochk",
    .fh_magic    = CHIMERA_VFS_FH_MAGIC_VENDOR0,
    .dispatch    = nochk_dispatch,
};

/* ---- harness ------------------------------------------------------------ */

struct test_ctx {
    int                             done;
    enum chimera_vfs_error          status;
    struct evpl                    *evpl;
    struct chimera_vfs_thread      *thread;
    const struct chimera_vfs_cred  *cred;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;
    uint32_t                        mode;
    struct chimera_vfs_open_handle *handle;
    char                            target[256];
    int                             targetlen;
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
    ctx->mode   = 0;
    if (error_code == CHIMERA_VFS_OK && attr) {
        if (attr->va_set_mask & CHIMERA_VFS_ATTR_FH) {
            memcpy(ctx->fh, attr->va_fh, attr->va_fh_len);
            ctx->fh_len = attr->va_fh_len;
        }
        if (attr->va_set_mask & CHIMERA_VFS_ATTR_MODE) {
            ctx->mode = attr->va_mode;
        }
    }
    ctx->done = 1;
} /* lookup_cb */

static void
attr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    (void) attr;
    ctx->status = error_code;
    ctx->done   = 1;
} /* attr_cb */

static void
err_cb(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* err_cb */

static void
open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct test_ctx *ctx = private_data;

    (void) attr;
    ctx->status = error_code;
    ctx->handle = (error_code == CHIMERA_VFS_OK) ? oh : NULL;
    ctx->done   = 1;
} /* open_cb */

static void
openfh_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->handle = (error_code == CHIMERA_VFS_OK) ? oh : NULL;
    ctx->done   = 1;
} /* openfh_cb */

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

    (void) set_attr;
    (void) dir_pre;
    (void) dir_post;
    ctx->status = error_code;
    if (error_code == CHIMERA_VFS_OK && attr &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        memcpy(ctx->fh, attr->va_fh, attr->va_fh_len);
        ctx->fh_len = attr->va_fh_len;
    }
    ctx->done = 1;
} /* mkdir_cb */

static void
readlink_cb(
    enum chimera_vfs_error    error_code,
    int                       targetlen,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    (void) attr;
    ctx->status    = error_code;
    ctx->targetlen = (error_code == CHIMERA_VFS_OK) ? targetlen : 0;
    ctx->done      = 1;
} /* readlink_cb */

/* Fixture step that must succeed: abort the test (not a row) otherwise. */
static void
must(
    struct test_ctx *ctx,
    const char      *what)
{
    if (ctx->status != CHIMERA_VFS_OK) {
        fprintf(stderr, "fixture %s failed: %d\n", what, ctx->status);
        exit(EXIT_FAILURE);
    }
} /* must */

/* Status of a no-follow lookup of `path` from the handle `fh`; the found
 * object's mode is left in ctx->mode. */
static enum chimera_vfs_error
probe(
    struct test_ctx *ctx,
    const uint8_t   *fh,
    uint32_t         fh_len,
    const char      *path)
{
    chimera_vfs_lookup(ctx->thread, ctx->cred, fh, fh_len, path, strlen(path),
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MODE, 0,
                       lookup_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* probe */

/* One asserted row: the op returned `want` and the server-side state check
 * (`state_ok`) holds.  The state check is the decisive half -- a refused op
 * that nevertheless acted on the collapsed name is a failure. */
static void
row(
    struct test_ctx       *ctx,
    const char            *id,
    enum chimera_vfs_error got,
    enum chimera_vfs_error want,
    int                    state_ok)
{
    int pass = (got == want) && state_ok;

    fprintf(stderr, "ROW %s %s status=%d want=%d state=%s\n", id,
            pass ? "PASS" : "FAIL", got, want, state_ok ? "ok" : "bad");
    if (!pass) {
        ctx->failures++;
    }
} /* row */

static void
row_info(
    const char            *id,
    enum chimera_vfs_error got,
    const char            *note)
{
    fprintf(stderr, "ROW %s INFO status=%d %s\n", id, got, note);
} /* row_info */

static enum chimera_vfs_error
do_symlink(
    struct test_ctx *ctx,
    const uint8_t   *fh,
    uint32_t         fh_len,
    const char      *path,
    const char      *target)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    chimera_vfs_symlink(ctx->thread, ctx->cred, fh, fh_len, path, strlen(path),
                        target, strlen(target), &sattr, 0, attr_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* do_symlink */

static enum chimera_vfs_error
do_mknod_fifo(
    struct test_ctx *ctx,
    const uint8_t   *fh,
    uint32_t         fh_len,
    const char      *path)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = S_IFIFO | 0644;
    chimera_vfs_mknod(ctx->thread, ctx->cred, fh, fh_len, path, strlen(path),
                      &sattr, 0, attr_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* do_mknod_fifo */

static enum chimera_vfs_error
do_remove(
    struct test_ctx *ctx,
    const uint8_t   *fh,
    uint32_t         fh_len,
    const char      *path,
    unsigned int     flags)
{
    chimera_vfs_remove(ctx->thread, ctx->cred, fh, fh_len, path, strlen(path),
                       flags, err_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* do_remove */

static enum chimera_vfs_error
do_rename(
    struct test_ctx *ctx,
    const uint8_t   *fh,
    uint32_t         fh_len,
    const char      *from,
    const char      *to)
{
    chimera_vfs_rename(ctx->thread, ctx->cred, fh, fh_len, from, strlen(from),
                       to, strlen(to), err_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* do_rename */

/* Creating open of `path`; a granted handle is released. */
static enum chimera_vfs_error
do_create(
    struct test_ctx *ctx,
    const uint8_t   *fh,
    uint32_t         fh_len,
    const char      *path)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0644;
    ctx->handle       = NULL;
    chimera_vfs_open(ctx->thread, ctx->cred, fh, fh_len, path, strlen(path),
                     CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY,
                     &sattr, CHIMERA_VFS_ATTR_FH, open_cb, ctx);
    wait_done(ctx);
    if (ctx->status == CHIMERA_VFS_OK && ctx->handle) {
        chimera_vfs_release(ctx->thread, ctx->handle);
        ctx->handle = NULL;
    }
    return ctx->status;
} /* do_create */

/* mkdir of `name` under the open directory `dir`; leaves the new directory
 * open in ctx->handle (its fh in ctx->fh) and returns the status. */
static enum chimera_vfs_error
do_mkdir_at(
    struct test_ctx                *ctx,
    struct chimera_vfs_open_handle *dir,
    const char                     *name)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0755;
    ctx->handle       = NULL;
    chimera_vfs_mkdir_at(ctx->thread, ctx->cred, dir, name, strlen(name),
                         &sattr, CHIMERA_VFS_ATTR_FH, 0, 0, mkdir_cb, ctx);
    wait_done(ctx);
    if (ctx->status != CHIMERA_VFS_OK) {
        return ctx->status;
    }
    chimera_vfs_open_fh(ctx->thread, ctx->cred, ctx->fh, ctx->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* do_mkdir_at */

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
    struct chimera_vfs_cred         root;
    struct chimera_vfs_open_handle *mnt, *dir, *m;
    uint8_t                         vroot_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        vroot_fh_len;
    uint8_t                         mnt_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        mnt_fh_len;
    char                            longpath[1300];
    enum chimera_vfs_error          st;
    int                             i, n, ok;
    int                             rc = EXIT_FAILURE;

    (void) argc;
    (void) argv;

    ChimeraLogLevel = CHIMERA_LOG_INFO;
    evpl_set_log_fn(chimera_vlog, chimera_log_flush);

    /* nochk is memfs with its own fh_magic, so it is a separate memfs
     * instance: same capabilities (not path-only) and lifecycle hooks. */
    vfs_nochk.capabilities   = vfs_memfs.capabilities;
    vfs_nochk.init           = vfs_memfs.init;
    vfs_nochk.destroy        = vfs_memfs.destroy;
    vfs_nochk.thread_init    = vfs_memfs.thread_init;
    vfs_nochk.thread_destroy = vfs_memfs.thread_destroy;

    metrics = prometheus_metrics_create(NULL, NULL, 0);

    config = chimera_server_config_init();
    chimera_server_config_set_tcp_flavor(config, CHIMERA_TCP_FLAVOR_INPROC);
    chimera_server_config_set_smb_enabled(config, 1);
    chimera_server_config_add_module(config, "smb", NULL, "");
    chimera_server_config_add_module(config, "nochk", NULL, "");

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

    chimera_vfs_cred_init_unix(&root, 0, 0, 0, NULL);

    memset(&ctx, 0, sizeof(ctx));
    ctx.evpl   = evpl;
    ctx.thread = thread;
    ctx.cred   = &root;

    chimera_vfs_mount(thread, NULL, "/smbclient", "smb", "127.0.0.1:share",
                      "user=root,password=secret,domain=WORKGROUP",
                      status_cb, &ctx);
    wait_done(&ctx);
    if (ctx.status != CHIMERA_VFS_OK) {
        fprintf(stderr, "SMB mount failed: vfs error %d\n", ctx.status);
        goto out;
    }

    chimera_vfs_mkfs(thread, NULL, "nochk", "fs1", NULL, status_cb, &ctx);
    wait_done(&ctx);
    must(&ctx, "nochk mkfs");
    chimera_vfs_mount(thread, NULL, "/m", "nochk", "fs1", NULL, status_cb, &ctx);
    wait_done(&ctx);
    must(&ctx, "nochk mount");

    chimera_vfs_get_root_fh(vfs, vroot_fh, &vroot_fh_len);

    /* The smb mount's own root handle: inside a path-only mount and not the
     * namespace root, so the path procs take their whole-path arm from it. */
    probe(&ctx, vroot_fh, vroot_fh_len, "smbclient");
    must(&ctx, "smbclient lookup");
    memcpy(mnt_fh, ctx.fh, ctx.fh_len);
    mnt_fh_len = ctx.fh_len;
    chimera_vfs_open_fh(thread, &root, mnt_fh, mnt_fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, &ctx);
    wait_done(&ctx);
    must(&ctx, "smbclient open");
    mnt = ctx.handle;

    /* Fixture, all single-component from the mount root: a regular file f, a
     * dangling symlink dl, directories d and d/e, a symlink lnk -> d/e, and
     * the victims of the remove and rename rows. */
    do_create(&ctx, mnt_fh, mnt_fh_len, "f");
    must(&ctx, "create f");
    do_create(&ctx, mnt_fh, mnt_fh_len, "victim");
    must(&ctx, "create victim");
    do_create(&ctx, mnt_fh, mnt_fh_len, "victim2");
    must(&ctx, "create victim2");
    do_symlink(&ctx, mnt_fh, mnt_fh_len, "dl", "missing");
    must(&ctx, "symlink dl");
    do_mkdir_at(&ctx, mnt, "d");
    must(&ctx, "mkdir d");
    dir = ctx.handle;
    do_mkdir_at(&ctx, dir, "e");
    must(&ctx, "mkdir d/e");
    chimera_vfs_release(thread, ctx.handle);
    chimera_vfs_release(thread, dir);
    do_symlink(&ctx, mnt_fh, mnt_fh_len, "lnk", "d/e");
    must(&ctx, "symlink lnk");

    /* S1: symlink through a regular-file prefix. */
    st = do_symlink(&ctx, mnt_fh, mnt_fh_len, "f/../s1", "t");
    row(&ctx, "S1", st, CHIMERA_VFS_ENOTDIR,
        probe(&ctx, mnt_fh, mnt_fh_len, "s1") == CHIMERA_VFS_ENOENT);

    /* S2: symlink through a dangling-symlink prefix. */
    st = do_symlink(&ctx, mnt_fh, mnt_fh_len, "dl/../s2", "t");
    row(&ctx, "S2", st, CHIMERA_VFS_ENOENT,
        probe(&ctx, mnt_fh, mnt_fh_len, "s2") == CHIMERA_VFS_ENOENT);

    /* M1: mknod through a regular-file prefix. */
    st = do_mknod_fifo(&ctx, mnt_fh, mnt_fh_len, "f/../p1");
    row(&ctx, "M1", st, CHIMERA_VFS_ENOTDIR,
        probe(&ctx, mnt_fh, mnt_fh_len, "p1") == CHIMERA_VFS_ENOENT);

    /* R1: remove through a regular-file prefix; victim must survive. */
    st = do_remove(&ctx, mnt_fh, mnt_fh_len, "f/../victim", 0);
    row(&ctx, "R1", st, CHIMERA_VFS_ENOTDIR,
        probe(&ctx, mnt_fh, mnt_fh_len, "victim") == CHIMERA_VFS_OK);

    /* N1: rename through a regular-file prefix; victim2 must stay put.  The
     * destination is a single component at the mount root, so the rename
     * involves no other directory the fixture holds open (an open directory
     * handle reserves DELETE, and a rename into it is a sharing violation --
     * which would refuse the op for a reason unrelated to the prefix). */
    st = do_rename(&ctx, mnt_fh, mnt_fh_len, "f/../victim2", "v2");
    ok = probe(&ctx, mnt_fh, mnt_fh_len, "victim2") == CHIMERA_VFS_OK;
    ok = ok && probe(&ctx, mnt_fh, mnt_fh_len, "v2") == CHIMERA_VFS_ENOENT;
    row(&ctx, "N1", st, CHIMERA_VFS_ENOTDIR, ok);

    /* O1: creating open through a regular-file prefix. */
    st = do_create(&ctx, mnt_fh, mnt_fh_len, "f/../o1");
    row(&ctx, "O1", st, CHIMERA_VFS_ENOTDIR,
        probe(&ctx, mnt_fh, mnt_fh_len, "o1") == CHIMERA_VFS_ENOENT);

    /* I1: ".." after a symlink to a directory.  The parent resolves as a
     * directory, but ".." is still collapsed lexically against the mount root
     * (POSIX would give d); recorded, not asserted. */
    st = do_symlink(&ctx, mnt_fh, mnt_fh_len, "lnk/../s3", "t");
    row_info("I1", st,
             probe(&ctx, mnt_fh, mnt_fh_len, "s3") == CHIMERA_VFS_OK ?
             "s3=root" :
             (probe(&ctx, mnt_fh, mnt_fh_len, "d/s3") == CHIMERA_VFS_OK ?
              "s3=d" : "s3=absent"));

    /* P1: a real directory parent; the link reads back its target. */
    st = do_symlink(&ctx, mnt_fh, mnt_fh_len, "d/s4", "t");
    ok = 0;
    if (probe(&ctx, mnt_fh, mnt_fh_len, "d/s4") == CHIMERA_VFS_OK &&
        S_ISLNK(ctx.mode)) {
        chimera_vfs_open_fh(thread, &root, ctx.fh, ctx.fh_len,
                            CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED |
                            CHIMERA_VFS_OPEN_NOFOLLOW,
                            openfh_cb, &ctx);
        wait_done(&ctx);
        if (ctx.status == CHIMERA_VFS_OK) {
            struct chimera_vfs_open_handle *lh = ctx.handle;

            chimera_vfs_readlink(thread, &root, lh, ctx.target,
                                 sizeof(ctx.target), 0, readlink_cb, &ctx);
            wait_done(&ctx);
            ok = ctx.status == CHIMERA_VFS_OK && ctx.targetlen == 1 &&
                ctx.target[0] == 't';
            chimera_vfs_release(thread, lh);
        }
    }
    row(&ctx, "P1", st, CHIMERA_VFS_OK, ok);

    /* P2: ".." after a real directory resolves (to the mount root). */
    st = do_symlink(&ctx, mnt_fh, mnt_fh_len, "d/../s5", "t");
    row(&ctx, "P2", st, CHIMERA_VFS_OK,
        probe(&ctx, mnt_fh, mnt_fh_len, "s5") == CHIMERA_VFS_OK);

    /* L1: a whole path longer than the backend's 1024-byte bound.  A
     * non-creating open keeps the whole-path arm, so the 1254-byte name (five
     * 250-byte components, each under NAME_MAX, the whole under PATH_MAX, so
     * the core passes it) reaches the backend's joiner, which must answer
     * ENAMETOOLONG -- a name-length problem, not a stale handle. */
    n = 0;
    for (i = 0; i < 5; i++) {
        if (i) {
            longpath[n++] = '/';
        }
        memset(longpath + n, 'a' + i, 250);
        n += 250;
    }
    longpath[n] = '\0';
    chimera_vfs_open(thread, &root, mnt_fh, mnt_fh_len, longpath, n,
                     CHIMERA_VFS_OPEN_READ_ONLY, NULL, CHIMERA_VFS_ATTR_FH,
                     open_cb, &ctx);
    wait_done(&ctx);
    st = ctx.status;
    if (st == CHIMERA_VFS_OK && ctx.handle) {
        chimera_vfs_release(thread, ctx.handle);
        ctx.handle = NULL;
    }
    row(&ctx, "L1", st, CHIMERA_VFS_ENAMETOOLONG, 1);

    /* X1/X2: lnk2 -> /m crosses (absolute target, resolved from the namespace
     * root) into the nochk mount, which will not refuse an unlink of a
     * directory itself. */
    probe(&ctx, vroot_fh, vroot_fh_len, "m");
    must(&ctx, "m lookup");
    chimera_vfs_open_fh(thread, &root, ctx.fh, ctx.fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, &ctx);
    wait_done(&ctx);
    must(&ctx, "m open");
    m = ctx.handle;
    do_mkdir_at(&ctx, m, "d");
    must(&ctx, "mkdir m/d");
    chimera_vfs_release(thread, ctx.handle);
    chimera_vfs_release(thread, m);
    do_symlink(&ctx, mnt_fh, mnt_fh_len, "lnk2", "/m");
    must(&ctx, "symlink lnk2");

    nochk_strip = 1;
    st          = do_remove(&ctx, mnt_fh, mnt_fh_len, "lnk2/d",
                            CHIMERA_VFS_REMOVE_ISNOTDIR);
    nochk_strip = 0;
    row(&ctx, "X1", st, CHIMERA_VFS_EISDIR,
        probe(&ctx, vroot_fh, vroot_fh_len, "m/d") == CHIMERA_VFS_OK);

    st = do_remove(&ctx, mnt_fh, mnt_fh_len, "lnk2/d", 0);
    row(&ctx, "X2", st, CHIMERA_VFS_OK,
        probe(&ctx, vroot_fh, vroot_fh_len, "m/d") == CHIMERA_VFS_ENOENT);

    rc = ctx.failures ? EXIT_FAILURE : EXIT_SUCCESS;
    fprintf(stderr, "SMB path-only guard test %s (%d failing rows)\n",
            rc ? "FAILED" : "PASSED", ctx.failures);

    chimera_vfs_release(thread, mnt);
    chimera_vfs_umount(thread, NULL, "/m", status_cb, &ctx);
    wait_done(&ctx);
    chimera_vfs_umount(thread, NULL, "/smbclient", status_cb, &ctx);
    wait_done(&ctx);

 out:
    chimera_vfs_thread_destroy(thread);
    evpl_destroy(evpl);
    chimera_server_destroy(server);
    prometheus_metrics_destroy(metrics);

    return rc;
} /* main */
