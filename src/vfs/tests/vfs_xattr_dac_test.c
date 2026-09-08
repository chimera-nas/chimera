// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * xattr DAC on the passthrough backends (linux, io_uring).
 *
 * These modules cache a privileged open_by_handle_at(2) descriptor on the
 * open handle and leave DAC to the kernel (CHIMERA_VFS_CAP_DELEGATES_DAC).
 * The kernel evaluates f*xattr(2) against the calling thread's fsuid at the
 * time of the call, so every xattr op must impersonate the caller
 * (chimera_setup_credential) or it runs as the server and grants a non-owner
 * whatever the mode bits deny.
 *
 * Kernel semantics for the user.* namespace (fs/xattr.c, xattr_permission):
 * set and remove need write access, get needs read access, list is not
 * permission-checked at all.  On a 0600 file owned by 1000, uid 2000 must
 * therefore be denied set/get/remove (EACCES) and allowed list, while uid
 * 1000 is allowed everything.
 *
 * Usage: vfs_xattr_dac_test <linux|io_uring>
 *
 * Exits 77 (ctest SKIP) when not root (setfsuid needs privilege) or when the
 * backend cannot serve the scratch directory (ENOTSUP mount, e.g. overlayfs
 * refuses name_to_handle_at).  The scratch root is $CHIMERA_MBT_SCRATCH,
 * defaulting to the working directory.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <limits.h>
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_cred.h"
#include "vfs/sdk/vfs_error.h"
#include "vfs/sdk/vfs_module.h"
#include "common/logging.h"
#include "prometheus-c.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)
#define SKIP_RC     77

#define XATTR_NAME  "user.chimera_dac_test"
#define XATTR_VALUE "value"

/* Evaluate `expr` (a status) and fail loudly unless it equals `expected`.
 * _exit rather than exit: the backend is still live, and a failure must
 * report as a failure, not as a crash in its atexit teardown. */
#define CHECK_STATUS(expr, expected)                                    \
        do {                                                            \
            enum chimera_vfs_error _st = (expr);                        \
            if (_st != (expected)) {                                    \
                fprintf(stderr, "FAIL: %s\n      expected %d, got %d\n", \
                        #expr, (int) (expected), (int) _st);            \
                _exit(1);                                               \
            }                                                           \
        } while (0)

struct test_ctx {
    int                             done;
    enum chimera_vfs_error          status;
    struct chimera_vfs             *vfs;
    struct chimera_vfs_thread      *vfs_thread;
    struct evpl                    *evpl;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;
    struct chimera_vfs_open_handle *handle;
    uint32_t                        value_len;
    char                            names[1024];
    uint32_t                        names_len;
    uint32_t                        count;
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
    if (error_code == CHIMERA_VFS_OK) {
        memcpy(ctx->fh, oh->fh, oh->fh_len);
        ctx->fh_len = oh->fh_len;
    }
    ctx->done = 1;
} /* openat_cb */

static void
get_xattr_cb(
    enum chimera_vfs_error error_code,
    uint32_t               value_len,
    void                  *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status    = error_code;
    ctx->value_len = value_len;
    ctx->done      = 1;
} /* get_xattr_cb */

static void
set_xattr_cb(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* set_xattr_cb */

static void
list_xattrs_cb(
    enum chimera_vfs_error error_code,
    const char            *names,
    uint32_t               names_len,
    uint32_t               count,
    uint32_t               eof,
    uint64_t               cookie,
    void                  *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status    = error_code;
    ctx->names_len = 0;
    ctx->count     = 0;
    if (error_code == CHIMERA_VFS_OK) {
        assert(names_len <= sizeof(ctx->names));
        if (names && names != ctx->names) {
            memcpy(ctx->names, names, names_len);
        }
        ctx->names_len = names_len;
        ctx->count     = count;
    }
    ctx->done = 1;
} /* list_xattrs_cb */

static void
remove_xattr_cb(
    enum chimera_vfs_error          error_code,
    const struct chimera_vfs_attrs *pre_attr,
    const struct chimera_vfs_attrs *post_attr,
    void                           *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* remove_xattr_cb */

/* Open `fh` as `cred` (access inferred); the handle is left in ctx->handle. */
static enum chimera_vfs_error
open_as(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *fh,
    uint32_t                       fh_len)
{
    chimera_vfs_open_fh(ctx->vfs_thread, cred, fh, fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* open_as */

/* Each *_as helper opens `fh` as `cred`, runs one xattr op through the
 * handle and releases it.  A denial at the open is reported as the result:
 * it is a denial of the op all the same. */

static enum chimera_vfs_error
set_xattr_as(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *fh,
    uint32_t                       fh_len,
    const char                    *name,
    const char                    *value)
{
    enum chimera_vfs_error st = open_as(ctx, cred, fh, fh_len);

    if (st != CHIMERA_VFS_OK) {
        return st;
    }

    chimera_vfs_set_xattr(ctx->vfs_thread, cred, ctx->handle,
                          CHIMERA_VFS_XATTR_EITHER, name, strlen(name),
                          value, strlen(value), set_xattr_cb, ctx);
    wait_done(ctx);
    st = ctx->status;

    chimera_vfs_release(ctx->vfs_thread, ctx->handle);
    return st;
} /* set_xattr_as */

static enum chimera_vfs_error
get_xattr_as(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *fh,
    uint32_t                       fh_len,
    const char                    *name,
    void                          *value,
    uint32_t                       value_maxlen)
{
    enum chimera_vfs_error st = open_as(ctx, cred, fh, fh_len);

    if (st != CHIMERA_VFS_OK) {
        return st;
    }

    chimera_vfs_get_xattr(ctx->vfs_thread, cred, ctx->handle, name,
                          strlen(name), value, value_maxlen, get_xattr_cb, ctx);
    wait_done(ctx);
    st = ctx->status;

    chimera_vfs_release(ctx->vfs_thread, ctx->handle);
    return st;
} /* get_xattr_as */

static enum chimera_vfs_error
list_xattrs_as(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *fh,
    uint32_t                       fh_len)
{
    enum chimera_vfs_error st = open_as(ctx, cred, fh, fh_len);

    if (st != CHIMERA_VFS_OK) {
        return st;
    }

    chimera_vfs_list_xattrs(ctx->vfs_thread, cred, ctx->handle, 0,
                            ctx->names, sizeof(ctx->names), list_xattrs_cb, ctx);
    wait_done(ctx);
    st = ctx->status;

    chimera_vfs_release(ctx->vfs_thread, ctx->handle);
    return st;
} /* list_xattrs_as */

static enum chimera_vfs_error
remove_xattr_as(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *fh,
    uint32_t                       fh_len,
    const char                    *name)
{
    enum chimera_vfs_error st = open_as(ctx, cred, fh, fh_len);

    if (st != CHIMERA_VFS_OK) {
        return st;
    }

    chimera_vfs_remove_xattr(ctx->vfs_thread, cred, ctx->handle, name,
                             strlen(name), remove_xattr_cb, ctx);
    wait_done(ctx);
    st = ctx->status;

    chimera_vfs_release(ctx->vfs_thread, ctx->handle);
    return st;
} /* remove_xattr_as */

/* Does the last list_xattrs result (back-to-back NUL-terminated names)
 * contain `name`? */
static int
listed(
    const struct test_ctx *ctx,
    const char            *name)
{
    const char *p   = ctx->names;
    const char *end = ctx->names + ctx->names_len;

    while (p < end) {
        if (strcmp(p, name) == 0) {
            return 1;
        }
        p += strlen(p) + 1;
    }
    return 0;
} /* listed */

int
main(
    int    argc,
    char **argv)
{
    struct test_ctx               ctx = { 0 };
    struct chimera_vfs_module_cfg module_cfgs[2];
    struct prometheus_metrics    *metrics;
    struct chimera_vfs_cred       root, owner, other;
    struct chimera_vfs_attrs      sattr;
    const char                   *backend = argc > 1 ? argv[1] : "linux";
    const char                   *scratch;
    char                          scratch_buf[PATH_MAX];
    char                          session_dir[PATH_MAX];
    char                          file_path[PATH_MAX + 8];
    char                          value[64];
    uint8_t                       root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                      root_fh_len;
    uint8_t                       file_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                      file_fh_len;

    if (strcmp(backend, "linux") != 0 && strcmp(backend, "io_uring") != 0) {
        fprintf(stderr, "usage: %s <linux|io_uring>\n", argv[0]);
        return 2;
    }

    if (geteuid() != 0) {
        fprintf(stderr, "SKIP: impersonation (setfsuid) needs root\n");
        return SKIP_RC;
    }

    chimera_log_init();

    scratch = getenv("CHIMERA_MBT_SCRATCH");
    if (!scratch || !*scratch) {
        scratch = realpath(".", scratch_buf);
        assert(scratch != NULL);
    }
    snprintf(session_dir, sizeof(session_dir), "%s/vfs_xattr_dac_XXXXXX",
             scratch);
    assert(mkdtemp(session_dir) != NULL);
    snprintf(file_path, sizeof(file_path), "%s/f", session_dir);

    chimera_vfs_cred_init_unix(&root, 0, 0, 0, NULL);
    chimera_vfs_cred_init_unix(&owner, 1000, 1000, 0, NULL);
    chimera_vfs_cred_init_unix(&other, 2000, 2000, 0, NULL);

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(metrics != NULL);

    memset(module_cfgs, 0, sizeof(module_cfgs));
    snprintf(module_cfgs[0].module_name, sizeof(module_cfgs[0].module_name),
             "%s", backend);
    snprintf(module_cfgs[1].module_name, sizeof(module_cfgs[1].module_name),
             "%s", "memkv");

    ctx.evpl = evpl_create(NULL);
    assert(ctx.evpl != NULL);

    ctx.vfs = chimera_vfs_init(0, 0, module_cfgs, 2, "memkv", 60, 1, 1, 0,
                               metrics);
    assert(ctx.vfs != NULL);

    ctx.vfs_thread = chimera_vfs_thread_init(ctx.evpl, ctx.vfs);
    assert(ctx.vfs_thread != NULL);

    chimera_vfs_mount(ctx.vfs_thread, NULL, "/test", backend, session_dir,
                      NULL, mount_cb, &ctx);
    wait_done(&ctx);
    if (ctx.status == CHIMERA_VFS_ENOTSUP) {
        fprintf(stderr, "SKIP: %s cannot serve %s (ENOTSUP)\n", backend,
                session_dir);
        rmdir(session_dir);
        /* _exit: skip the leak checker, this is a skip not a failure. */
        _exit(SKIP_RC);
    }
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_get_root_fh(root_fh, &root_fh_len);
    chimera_vfs_lookup(ctx.vfs_thread, &root, root_fh, root_fh_len, "test", 4,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                       lookup_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    memcpy(root_fh, ctx.fh, ctx.fh_len);
    root_fh_len = ctx.fh_len;

    /* Root creates a 0600 file owned by 1000:1000 (root runs as the server
     * identity, so the chown is privileged). */
    {
        struct chimera_vfs_open_handle *root_handle;

        assert(open_as(&ctx, &root, root_fh, root_fh_len) == CHIMERA_VFS_OK);
        root_handle = ctx.handle;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
            CHIMERA_VFS_ATTR_GID;
        sattr.va_mode = 0600;
        sattr.va_uid  = 1000;
        sattr.va_gid  = 1000;

        chimera_vfs_open_at(ctx.vfs_thread, &root, root_handle, "f", 1,
                            CHIMERA_VFS_OPEN_CREATE, &sattr, CHIMERA_VFS_ATTR_FH,
                            0, 0, openat_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_OK);
        memcpy(file_fh, ctx.fh, ctx.fh_len);
        file_fh_len = ctx.fh_len;

        chimera_vfs_release(ctx.vfs_thread, ctx.handle);
        chimera_vfs_release(ctx.vfs_thread, root_handle);
    }

    /* Non-owner first on every op, then the owner: a leaked impersonation
     * would surface as the owner (or root, below) failing. */

    /* set: write access. */
    CHECK_STATUS(set_xattr_as(&ctx, &other, file_fh, file_fh_len,
                              XATTR_NAME, XATTR_VALUE), CHIMERA_VFS_EACCES);
    CHECK_STATUS(set_xattr_as(&ctx, &owner, file_fh, file_fh_len,
                              XATTR_NAME, XATTR_VALUE), CHIMERA_VFS_OK);
    TEST_PASS("set_xattr: non-owner denied, owner allowed (0600)");

    /* get: read access. */
    CHECK_STATUS(get_xattr_as(&ctx, &other, file_fh, file_fh_len,
                              XATTR_NAME, value, sizeof(value)),
                 CHIMERA_VFS_EACCES);
    CHECK_STATUS(get_xattr_as(&ctx, &owner, file_fh, file_fh_len,
                              XATTR_NAME, value, sizeof(value)),
                 CHIMERA_VFS_OK);
    assert(ctx.value_len == strlen(XATTR_VALUE));
    assert(memcmp(value, XATTR_VALUE, ctx.value_len) == 0);
    TEST_PASS("get_xattr: non-owner denied, owner allowed (0600)");

    /* list: listxattr(2) is not permission-checked by the kernel, so both
     * succeed and see the attribute. */
    CHECK_STATUS(list_xattrs_as(&ctx, &other, file_fh, file_fh_len),
                 CHIMERA_VFS_OK);
    assert(listed(&ctx, XATTR_NAME));
    CHECK_STATUS(list_xattrs_as(&ctx, &owner, file_fh, file_fh_len),
                 CHIMERA_VFS_OK);
    assert(listed(&ctx, XATTR_NAME));
    TEST_PASS("list_xattrs: allowed for both (no kernel permission check)");

    /* remove: write access; the denied attempt must leave the attribute. */
    CHECK_STATUS(remove_xattr_as(&ctx, &other, file_fh, file_fh_len,
                                 XATTR_NAME), CHIMERA_VFS_EACCES);
    CHECK_STATUS(get_xattr_as(&ctx, &owner, file_fh, file_fh_len,
                              XATTR_NAME, value, sizeof(value)),
                 CHIMERA_VFS_OK);
    CHECK_STATUS(remove_xattr_as(&ctx, &owner, file_fh, file_fh_len,
                                 XATTR_NAME), CHIMERA_VFS_OK);
    CHECK_STATUS(get_xattr_as(&ctx, &owner, file_fh, file_fh_len,
                              XATTR_NAME, value, sizeof(value)),
                 CHIMERA_VFS_ENODATA);
    TEST_PASS("remove_xattr: non-owner denied, owner allowed (0600)");

    /* The server identity is restored after every impersonated op. */
    CHECK_STATUS(set_xattr_as(&ctx, &root, file_fh, file_fh_len,
                              XATTR_NAME, XATTR_VALUE), CHIMERA_VFS_OK);
    CHECK_STATUS(remove_xattr_as(&ctx, &root, file_fh, file_fh_len,
                                 XATTR_NAME), CHIMERA_VFS_OK);
    TEST_PASS("root: privilege restored after impersonated ops");

    chimera_vfs_umount(ctx.vfs_thread, NULL, "/test", mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_thread_destroy(ctx.vfs_thread);
    chimera_vfs_destroy(ctx.vfs);
    evpl_destroy(ctx.evpl);
    prometheus_metrics_destroy(metrics);

    unlink(file_path);
    rmdir(session_dir);

    fprintf(stderr, "All xattr DAC tests passed (%s)\n", backend);
    return 0;
} /* main */
