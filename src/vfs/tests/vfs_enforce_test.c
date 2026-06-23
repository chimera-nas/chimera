// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * End-to-end check that the VFS-layer access gate actually enforces the ACL on
 * an engine-authoritative backend (memfs): the owner of a 0600 file may
 * read/write/chmod it, and a non-owner is denied on read, write (EACCES) and
 * chmod (EPERM, ownership required) -- the cross-protocol-agnostic enforcement
 * the VFS gate provides.
 */

#include <stdio.h>
#include <string.h>
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_attrs.h"
#include "vfs/vfs_cred.h"
#include "vfs/vfs_error.h"
#include "common/logging.h"
#include "prometheus-c.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

struct test_ctx {
    int                             done;
    enum chimera_vfs_error          status;
    struct chimera_vfs             *vfs;
    struct chimera_vfs_thread      *vfs_thread;
    struct evpl                    *evpl;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;
    struct chimera_vfs_open_handle *handle;
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
read_cb(
    enum chimera_vfs_error    error_code,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* read_cb */

static void
write_cb(
    enum chimera_vfs_error    error_code,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* write_cb */

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
lookupat_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
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
} /* lookupat_cb */

static void
remove_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* remove_cb */

/* mkdir `name` under directory handle `dir` as `cred`; return the status. */
static enum chimera_vfs_error
mkdir_as(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir,
    const char                     *name)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0755;

    chimera_vfs_mkdir_at(ctx->vfs_thread, cred, dir, name, strlen(name),
                         &sattr, CHIMERA_VFS_ATTR_FH, 0, 0, mkdir_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* mkdir_as */

/* Create file `name` under directory handle `dir` as `cred`; return status. */
static enum chimera_vfs_error
create_as(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir,
    const char                     *name)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0644;

    chimera_vfs_open_at(ctx->vfs_thread, cred, dir, name, strlen(name),
                        CHIMERA_VFS_OPEN_CREATE, &sattr, CHIMERA_VFS_ATTR_FH,
                        0, 0, openat_cb, ctx);
    wait_done(ctx);
    if (ctx->status == CHIMERA_VFS_OK && ctx->handle) {
        chimera_vfs_release(ctx->vfs_thread, ctx->handle);
    }
    return ctx->status;
} /* create_as */

/* Look `name` up in directory handle `dir` as `cred`; return status. */
static enum chimera_vfs_error
lookup_as(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir,
    const char                     *name)
{
    chimera_vfs_lookup_at(ctx->vfs_thread, cred, dir, name, strlen(name),
                          CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                          lookupat_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* lookup_as */

/* Remove `name` (whose handle is `child_fh`) from `dir` as `cred`. */
static enum chimera_vfs_error
remove_as(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir,
    const char                     *name,
    const uint8_t                  *child_fh,
    uint32_t                        child_fh_len)
{
    chimera_vfs_remove_at(ctx->vfs_thread, cred, dir, name, strlen(name),
                          child_fh, child_fh_len, 0, 0, NULL, remove_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* remove_as */

/* Open a handle for `fh` as `cred`, run a read, return the resulting status. */
static enum chimera_vfs_error
read_as(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *fh,
    uint32_t                       fh_len)
{
    enum chimera_vfs_error st;

    chimera_vfs_open_fh(ctx->vfs_thread, cred, fh, fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);

    chimera_vfs_read(ctx->vfs_thread, cred, ctx->handle, 0, 0, NULL, 0, 0,
                     read_cb, ctx);
    wait_done(ctx);
    st = ctx->status;

    chimera_vfs_release(ctx->vfs_thread, ctx->handle);
    return st;
} /* read_as */

static enum chimera_vfs_error
write_as(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *fh,
    uint32_t                       fh_len)
{
    enum chimera_vfs_error st;

    chimera_vfs_open_fh(ctx->vfs_thread, cred, fh, fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);

    chimera_vfs_write(ctx->vfs_thread, cred, ctx->handle, 0, 0, 0, 0, 0,
                      NULL, 0, write_cb, ctx);
    wait_done(ctx);
    st = ctx->status;

    chimera_vfs_release(ctx->vfs_thread, ctx->handle);
    return st;
} /* write_as */

static enum chimera_vfs_error
chmod_as(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *fh,
    uint32_t                       fh_len,
    uint32_t                       mode)
{
    struct chimera_vfs_attrs sattr;
    enum chimera_vfs_error   st;

    chimera_vfs_open_fh(ctx->vfs_thread, cred, fh, fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = mode;

    chimera_vfs_setattr(ctx->vfs_thread, cred, ctx->handle, &sattr, 0, 0,
                        setattr_cb, ctx);
    wait_done(ctx);
    st = ctx->status;

    chimera_vfs_release(ctx->vfs_thread, ctx->handle);
    return st;
} /* chmod_as */

int
main(
    int    argc,
    char **argv)
{
    struct test_ctx               ctx = { 0 };
    struct chimera_vfs_module_cfg module_cfgs[2];
    struct prometheus_metrics    *metrics;
    struct chimera_vfs_cred       owner, other;
    struct chimera_vfs_attrs      sattr;
    uint8_t                       root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                      root_fh_len;
    uint8_t                       file_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                      file_fh_len;

    chimera_log_init();

    chimera_vfs_cred_init_unix(&owner, 1000, 1000, 0, NULL);
    chimera_vfs_cred_init_unix(&other, 2000, 2000, 0, NULL);

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(metrics != NULL);

    memset(module_cfgs, 0, sizeof(module_cfgs));
    strncpy(module_cfgs[0].module_name, "memfs", sizeof(module_cfgs[0].module_name) - 1);
    strncpy(module_cfgs[1].module_name, "memkv", sizeof(module_cfgs[1].module_name) - 1);

    ctx.evpl = evpl_create(NULL);
    assert(ctx.evpl != NULL);

    ctx.vfs = chimera_vfs_init(0, 0, module_cfgs, 2, "memkv", 60, 1, 1, 0, metrics);
    assert(ctx.vfs != NULL);

    ctx.vfs_thread = chimera_vfs_thread_init(ctx.evpl, ctx.vfs);
    assert(ctx.vfs_thread != NULL);

    /* Mount memfs into the namespace and resolve its root FH. */
    chimera_vfs_mount(ctx.vfs_thread, NULL, "/test", "memfs", "/", NULL,
                      mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_get_root_fh(root_fh, &root_fh_len);

    chimera_vfs_lookup(ctx.vfs_thread, &owner, root_fh, root_fh_len, "test", 4,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                       lookup_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    memcpy(root_fh, ctx.fh, ctx.fh_len);
    root_fh_len = ctx.fh_len;

    /* Open the root directory (as owner) and create a 0600 file owned by 1000. */
    chimera_vfs_open_fh(ctx.vfs_thread, &owner, root_fh, root_fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    {
        struct chimera_vfs_open_handle *root_handle = ctx.handle;

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID;
        sattr.va_mode     = 0600;
        sattr.va_uid      = 1000;
        sattr.va_gid      = 1000;

        chimera_vfs_open_at(ctx.vfs_thread, &owner, root_handle, "f", 1,
                            CHIMERA_VFS_OPEN_CREATE, &sattr, CHIMERA_VFS_ATTR_FH,
                            0, 0, openat_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_OK);
        memcpy(file_fh, ctx.fh, ctx.fh_len);
        file_fh_len = ctx.fh_len;

        chimera_vfs_release(ctx.vfs_thread, ctx.handle);
        chimera_vfs_release(ctx.vfs_thread, root_handle);
    }

    /* Owner (1000) holds read/write on a 0600 file; a non-owner (2000) does not. */
    assert(read_as(&ctx, &owner, file_fh, file_fh_len) == CHIMERA_VFS_OK);
    assert(read_as(&ctx, &other, file_fh, file_fh_len) == CHIMERA_VFS_EACCES);
    TEST_PASS("read: owner allowed, non-owner denied (0600)");

    assert(write_as(&ctx, &owner, file_fh, file_fh_len) == CHIMERA_VFS_OK);
    assert(write_as(&ctx, &other, file_fh, file_fh_len) == CHIMERA_VFS_EACCES);
    TEST_PASS("write: owner allowed, non-owner denied (0600)");

    /* chmod requires WRITE_ACL: the owner holds it implicitly, a non-owner does
     * not.  POSIX chmod(2) by a non-owner is EPERM (ownership required), not
     * EACCES. */
    assert(chmod_as(&ctx, &other, file_fh, file_fh_len, 0640) == CHIMERA_VFS_EPERM);
    assert(chmod_as(&ctx, &owner, file_fh, file_fh_len, 0640) == CHIMERA_VFS_OK);
    TEST_PASS("chmod: non-owner denied, owner allowed");

    /*
     * Namespace operations on a 0700 directory owned by 1000: the owner may
     * populate and traverse it, a non-owner (2000) is denied at every
     * namespace op -- the same VFS gate every protocol funnels through.
     */
    {
        struct chimera_vfs_open_handle *root_handle, *dir_handle;
        struct chimera_vfs_attrs        dattr;
        uint8_t                         dir_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                        dir_fh_len;
        uint8_t                         kid_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                        kid_fh_len;

        chimera_vfs_open_fh(ctx.vfs_thread, &owner, root_fh, root_fh_len,
                            CHIMERA_VFS_OPEN_INFERRED, openfh_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_OK);
        root_handle = ctx.handle;

        memset(&dattr, 0, sizeof(dattr));
        dattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        dattr.va_mode     = 0700;
        chimera_vfs_mkdir_at(ctx.vfs_thread, &owner, root_handle, "d", 1, &dattr,
                             CHIMERA_VFS_ATTR_FH, 0, 0, mkdir_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_OK);
        memcpy(dir_fh, ctx.fh, ctx.fh_len);
        dir_fh_len = ctx.fh_len;
        chimera_vfs_release(ctx.vfs_thread, root_handle);

        chimera_vfs_open_fh(ctx.vfs_thread, &owner, dir_fh, dir_fh_len,
                            CHIMERA_VFS_OPEN_INFERRED, openfh_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_OK);
        dir_handle = ctx.handle;

        assert(mkdir_as(&ctx, &other, dir_handle, "sub") == CHIMERA_VFS_EACCES);
        assert(mkdir_as(&ctx, &owner, dir_handle, "sub") == CHIMERA_VFS_OK);
        TEST_PASS("mkdir: non-owner denied, owner allowed (0700 dir)");

        /* Seed a file (as owner) for the lookup/remove checks below.  NOTE:
         * regular-file creation via open_at is intentionally not VFS-gated
         * (SMB applies its own create-access check; NFS file-create parent
         * enforcement is a documented follow-up), so we do not assert a
         * non-owner create is denied here. */
        assert(create_as(&ctx, &owner, dir_handle, "kid") == CHIMERA_VFS_OK);

        /* lookup needs EXECUTE (search) on the directory. */
        assert(lookup_as(&ctx, &other, dir_handle, "kid") == CHIMERA_VFS_EACCES);
        assert(lookup_as(&ctx, &owner, dir_handle, "kid") == CHIMERA_VFS_OK);
        memcpy(kid_fh, ctx.fh, ctx.fh_len);
        kid_fh_len = ctx.fh_len;
        TEST_PASS("lookup: non-owner denied, owner allowed (0700 dir)");

        assert(remove_as(&ctx, &other, dir_handle, "kid", kid_fh, kid_fh_len) ==
               CHIMERA_VFS_EACCES);
        assert(remove_as(&ctx, &owner, dir_handle, "kid", kid_fh, kid_fh_len) ==
               CHIMERA_VFS_OK);
        TEST_PASS("remove: non-owner denied, owner allowed (0700 dir)");

        chimera_vfs_release(ctx.vfs_thread, dir_handle);
    }

    chimera_vfs_thread_destroy(ctx.vfs_thread);
    chimera_vfs_destroy(ctx.vfs);
    evpl_destroy(ctx.evpl);
    prometheus_metrics_destroy(metrics);

    fprintf(stderr, "All VFS enforcement tests passed!\n");
    return 0;
} /* main */
