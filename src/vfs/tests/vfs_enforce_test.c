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
#include <sys/stat.h>
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

/* Set by readdir_entry_cb when the listing names "kid", with the entry's
 * va_set_mask and va_mode. */
static int      readdir_saw_kid;
static uint64_t readdir_kid_mask;
static uint32_t readdir_kid_mode;

static int
readdir_entry_cb(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    if (namelen == 3 && memcmp(name, "kid", 3) == 0) {
        readdir_saw_kid  = 1;
        readdir_kid_mask = attrs->va_set_mask;
        readdir_kid_mode = attrs->va_mode;
    }
    return 0;
} /* readdir_entry_cb */

static void
readdir_complete_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        eof,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* readdir_complete_cb */

/* List directory handle `dir` as `cred`, asking for `attr_mask` per entry,
 * with readdir `flags`.  Returns the status; a successful listing must name
 * "kid". */
static enum chimera_vfs_error
readdir_flags_as(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir,
    uint64_t                        attr_mask,
    uint32_t                        flags)
{
    readdir_saw_kid  = 0;
    readdir_kid_mask = 0;
    readdir_kid_mode = 0;
    chimera_vfs_readdir(ctx->vfs_thread, cred, dir, attr_mask, 0, 0, 0, flags,
                        NULL, 0, readdir_entry_cb, readdir_complete_cb, ctx);
    wait_done(ctx);
    assert(ctx->status != CHIMERA_VFS_OK || readdir_saw_kid);
    return ctx->status;
} /* readdir_flags_as */

static enum chimera_vfs_error
readdir_mask_as(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir,
    uint64_t                        attr_mask)
{
    return readdir_flags_as(ctx, cred, dir, attr_mask, 0);
} /* readdir_mask_as */

static enum chimera_vfs_error
readdir_as(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir)
{
    return readdir_mask_as(ctx, cred, dir, 0);
} /* readdir_as */

/* As readdir_as, through a stream whose read the caller bound at open (FUSE
 * after OPENDIR, the POSIX client on a directory opened for reading). */
static enum chimera_vfs_error
readdir_bound_as(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir)
{
    return readdir_flags_as(ctx, cred, dir, 0,
                            CHIMERA_VFS_READDIR_READ_BOUND);
} /* readdir_bound_as */

/* As readdir_flags_as with the attributes and handle READDIRPLUS asks for;
 * returns 1 if "kid" came with them, 0 if withheld.  Fails on anything else:
 * a withheld entry carries the mark, its inode number and its file type (a
 * regular file), and nothing more. */
static int
readdir_plus_flags(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir,
    uint32_t                        flags)
{
    assert(readdir_flags_as(ctx, cred, dir,
                            CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_FH,
                            flags) == CHIMERA_VFS_OK);
    if (readdir_kid_mask & CHIMERA_VFS_ATTR_WITHHELD) {
        assert((readdir_kid_mask & ~(CHIMERA_VFS_ATTR_WITHHELD |
                                     CHIMERA_VFS_ATTR_INUM)) == 0);
        assert(readdir_kid_mode == S_IFREG);
        return 0;
    }
    assert(readdir_kid_mask & CHIMERA_VFS_ATTR_FH);
    assert(readdir_kid_mask & CHIMERA_VFS_ATTR_MODE);
    return 1;
} /* readdir_plus_flags */

static int
readdir_plus_full(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir)
{
    return readdir_plus_flags(ctx, cred, dir, 0);
} /* readdir_plus_full */

static int
readdir_plus_bound(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir)
{
    return readdir_plus_flags(ctx, cred, dir, CHIMERA_VFS_READDIR_READ_BOUND);
} /* readdir_plus_bound */

static int find_entries;

static int
find_filter_cb(
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *attr,
    void                           *private_data)
{
    return 0;
} /* find_filter_cb */

static int
find_entry_cb(
    const char                     *path,
    int                             pathlen,
    const struct chimera_vfs_attrs *attr,
    void                           *private_data)
{
    find_entries++;
    return 0;
} /* find_entry_cb */

static void
find_complete_cb(
    enum chimera_vfs_error error_code,
    void                  *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* find_complete_cb */

/* Walk the tree under `fh` as `cred`, as S3 listing does; return the status. */
static enum chimera_vfs_error
find_as(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const void                    *fh,
    int                            fh_len)
{
    find_entries = 0;
    chimera_vfs_find(ctx->vfs_thread, cred, fh, fh_len,
                     CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_FH,
                     find_filter_cb, find_entry_cb, find_complete_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* find_as */

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
                          child_fh, child_fh_len, 0, 0, 0, NULL, remove_cb, ctx);
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

    /* Create a named filesystem, mount it into the namespace and resolve its
     * root FH. */
    chimera_vfs_mkfs(ctx.vfs_thread, NULL, "memfs", "fs0", NULL,
                     mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_mount(ctx.vfs_thread, NULL, "/test", "memfs", "fs0", NULL,
                      mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_get_root_fh(ctx.vfs_thread->vfs, root_fh, &root_fh_len);

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

        /* A component longer than {NAME_MAX} is rejected with ENAMETOOLONG
         * before dispatch -- lookup_at was the sole _at proc missing this bound,
         * letting an attacker-sized NFSv3 LOOKUP name overrun a passthrough
         * backend's fixed request buffer. */
        {
            char longname[CHIMERA_VFS_NAME_MAX + 32];

            memset(longname, 'a', sizeof(longname) - 1);
            longname[sizeof(longname) - 1] = '\0';
            assert(lookup_as(&ctx, &owner, dir_handle, longname) ==
                   CHIMERA_VFS_ENAMETOOLONG);
        }
        TEST_PASS("lookup: over-long component rejected (ENAMETOOLONG)");

        assert(remove_as(&ctx, &other, dir_handle, "kid", kid_fh, kid_fh_len) ==
               CHIMERA_VFS_EACCES);
        assert(remove_as(&ctx, &owner, dir_handle, "kid", kid_fh, kid_fh_len) ==
               CHIMERA_VFS_OK);
        TEST_PASS("remove: non-owner denied, owner allowed (0700 dir)");

        chimera_vfs_release(ctx.vfs_thread, dir_handle);
    }

    /*
     * The open-time access gate must authorize the open itself, not merely
     * the per-operation READ/WRITE that follows.  NFSv4 OPEN and SMB CREATE
     * bind I/O rights at open time, and a kernel NFS client defers write
     * authorization entirely to the server's OPEN -- so an open the mode
     * denies has to fail here, with nothing downstream to catch it.
     *
     * Each file is owned 1000:1000, so an open by 2000 lands in the "other"
     * class and only the low mode digit applies to it; the owner cells check
     * the same split from the other side.  Every cell uses a fresh name so no
     * open-handle or attribute cache entry from an earlier cell can satisfy
     * the next one.
     */
    {
        struct chimera_vfs_open_handle *root_handle;
        static const struct {
            const char  *name;
            uint32_t     mode;
            int          as_owner;
            unsigned int flags;
            enum chimera_vfs_error expect;
        }
        /* *INDENT-OFF* */
        cells[] = {
            { "g_oth_w_0000", 0000, 0, CHIMERA_VFS_OPEN_WRITE_ONLY, CHIMERA_VFS_EACCES },
            { "g_oth_r_0000", 0000, 0, CHIMERA_VFS_OPEN_READ_ONLY,  CHIMERA_VFS_EACCES },
            { "g_oth_w_0004", 0004, 0, CHIMERA_VFS_OPEN_WRITE_ONLY, CHIMERA_VFS_EACCES },
            { "g_oth_r_0004", 0004, 0, CHIMERA_VFS_OPEN_READ_ONLY,  CHIMERA_VFS_OK     },
            { "g_oth_w_0002", 0002, 0, CHIMERA_VFS_OPEN_WRITE_ONLY, CHIMERA_VFS_OK     },
            { "g_oth_r_0002", 0002, 0, CHIMERA_VFS_OPEN_READ_ONLY,  CHIMERA_VFS_EACCES },
            { "g_oth_w_0700", 0700, 0, CHIMERA_VFS_OPEN_WRITE_ONLY, CHIMERA_VFS_EACCES },
            { "g_own_w_0400", 0400, 1, CHIMERA_VFS_OPEN_WRITE_ONLY, CHIMERA_VFS_EACCES },
            { "g_own_r_0400", 0400, 1, CHIMERA_VFS_OPEN_READ_ONLY,  CHIMERA_VFS_OK     },
            { "g_own_w_0200", 0200, 1, CHIMERA_VFS_OPEN_WRITE_ONLY, CHIMERA_VFS_OK     },
        };
        /* *INDENT-ON* */
        unsigned int i;
        int          failures = 0;

        chimera_vfs_open_fh(ctx.vfs_thread, &owner, root_fh, root_fh_len,
                            CHIMERA_VFS_OPEN_INFERRED, openfh_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_OK);
        root_handle = ctx.handle;

        for (i = 0; i < sizeof(cells) / sizeof(cells[0]); i++) {
            const struct chimera_vfs_cred *actor = cells[i].as_owner ? &owner : &other;

            /* Create the cell's file as its owner, with the cell's mode.  A
             * fresh attrs struct per call: the backends rewrite the caller's
             * va_set_mask, so a reused struct no longer describes what the
             * next call is asking for. */
            memset(&sattr, 0, sizeof(sattr));
            sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
                CHIMERA_VFS_ATTR_GID;
            sattr.va_mode = cells[i].mode;
            sattr.va_uid  = 1000;
            sattr.va_gid  = 1000;

            chimera_vfs_open_at(ctx.vfs_thread, &owner, root_handle,
                                cells[i].name, strlen(cells[i].name),
                                CHIMERA_VFS_OPEN_CREATE, &sattr,
                                CHIMERA_VFS_ATTR_FH, 0, 0, openat_cb, &ctx);
            wait_done(&ctx);
            assert(ctx.status == CHIMERA_VFS_OK);
            chimera_vfs_release(ctx.vfs_thread, ctx.handle);

            /* Open it by name with the cell's data-access intent and no create
             * bit -- the shape an NFSv4 CLAIM_NULL / OPEN4_NOCREATE produces. */
            memset(&sattr, 0, sizeof(sattr));
            chimera_vfs_open_at(ctx.vfs_thread, actor, root_handle,
                                cells[i].name, strlen(cells[i].name),
                                cells[i].flags, &sattr,
                                CHIMERA_VFS_ATTR_FH, 0, 0, openat_cb, &ctx);
            wait_done(&ctx);

            if (ctx.status != cells[i].expect) {
                fprintf(stderr,
                        "  FAIL: open_at %s mode %04o as %s flags 0x%x: "
                        "expected %d, got %d\n",
                        cells[i].name, cells[i].mode,
                        cells[i].as_owner ? "owner" : "other",
                        cells[i].flags, cells[i].expect, ctx.status);
                failures++;
            }

            if (ctx.status == CHIMERA_VFS_OK && ctx.handle) {
                chimera_vfs_release(ctx.vfs_thread, ctx.handle);
            }
        }

        chimera_vfs_release(ctx.vfs_thread, root_handle);
        assert(failures == 0);
        TEST_PASS("open_at: mode gates data-access intent at open time");
    }

    /*
     * READDIR needs read (LIST_DIRECTORY) on the directory.  An enumeration
     * that does not say its read was bound at open -- every NFS READDIR,
     * since NFS has no directory open -- is judged against the directory's
     * current mode on every call, whatever grant other opens left on the
     * shared handle.  A stream opened for reading (FUSE OPENDIR, an O_RDONLY
     * POSIX opendir) says so with CHIMERA_VFS_READDIR_READ_BOUND, and POSIX
     * keeps a directory stream's rights from opendir(), so a later chmod does
     * not break it.  SMB (AUTH_ATTR) is exempt here: it checks the open's
     * granted access itself.  Search (EXECUTE) decides whether the entries'
     * attributes and handles come with the names, and is judged on every
     * call, bound stream or not.
     */
    {
        struct chimera_vfs_open_handle *root_handle, *dir_handle, *stream;
        struct chimera_vfs_open_handle *sub_handle;
        struct chimera_vfs_cred         smb_other;
        struct chimera_vfs_cred         root_cred;
        struct chimera_vfs_attrs        dattr;
        uint8_t                         dir_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                        dir_fh_len;
        uint8_t                         sub_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                        sub_fh_len;

        chimera_vfs_cred_init_attr(&smb_other, 2000, 2000, 0, NULL);
        chimera_vfs_cred_init_unix(&root_cred, 0, 0, 0, NULL);

        chimera_vfs_open_fh(ctx.vfs_thread, &owner, root_fh, root_fh_len,
                            CHIMERA_VFS_OPEN_INFERRED, openfh_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_OK);
        root_handle = ctx.handle;

        memset(&dattr, 0, sizeof(dattr));
        dattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        dattr.va_mode     = 0755;
        chimera_vfs_mkdir_at(ctx.vfs_thread, &owner, root_handle, "rd", 2,
                             &dattr, CHIMERA_VFS_ATTR_FH, 0, 0, mkdir_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_OK);
        memcpy(dir_fh, ctx.fh, ctx.fh_len);
        dir_fh_len = ctx.fh_len;

        /* A handle opened as an NFS READDIR opens it, and listed unbound. */
        chimera_vfs_open_fh(ctx.vfs_thread, &other, dir_fh, dir_fh_len,
                            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                            CHIMERA_VFS_OPEN_DIRECTORY, openfh_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_OK);
        dir_handle = ctx.handle;

        assert(create_as(&ctx, &owner, dir_handle, "kid") == CHIMERA_VFS_OK);

        assert(readdir_as(&ctx, &other, dir_handle) == CHIMERA_VFS_OK);
        assert(chmod_as(&ctx, &owner, dir_fh, dir_fh_len, 0700) == CHIMERA_VFS_OK);
        assert(readdir_as(&ctx, &other, dir_handle) == CHIMERA_VFS_EACCES);
        assert(readdir_as(&ctx, &owner, dir_handle) == CHIMERA_VFS_OK);
        assert(readdir_as(&ctx, &root_cred, dir_handle) == CHIMERA_VFS_OK);
        assert(chmod_as(&ctx, &owner, dir_fh, dir_fh_len, 0711) == CHIMERA_VFS_OK);
        assert(readdir_as(&ctx, &other, dir_handle) == CHIMERA_VFS_EACCES);
        assert(chmod_as(&ctx, &owner, dir_fh, dir_fh_len, 0755) == CHIMERA_VFS_OK);
        assert(readdir_as(&ctx, &other, dir_handle) == CHIMERA_VFS_OK);
        TEST_PASS("readdir: unbound READDIR judged against the current mode");

        /* opendir(): an O_RDONLY open checks read, and the stream it opens
         * keeps it -- a chmod afterwards leaves the stream listable, while a
         * fresh open is refused.  The open stamps its grant on the handle the
         * open cache shares with every open of this identity, here the very
         * handle the NFS-style READDIRs above use; that stamp must not exempt
         * them. */
        memset(&sattr, 0, sizeof(sattr));
        chimera_vfs_open_at(ctx.vfs_thread, &other, root_handle, "rd", 2,
                            CHIMERA_VFS_OPEN_READ_ONLY |
                            CHIMERA_VFS_OPEN_DIRECTORY, &sattr,
                            CHIMERA_VFS_ATTR_FH, 0, 0, openat_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_OK);
        stream = ctx.handle;
        assert(stream->granted_bound &&
               (stream->granted_access & CHIMERA_ACE_READ_DATA));

        assert(chmod_as(&ctx, &owner, dir_fh, dir_fh_len, 0700) == CHIMERA_VFS_OK);
        assert(readdir_bound_as(&ctx, &other, stream) == CHIMERA_VFS_OK);
        assert(readdir_as(&ctx, &other, stream) == CHIMERA_VFS_EACCES);
        assert(readdir_as(&ctx, &other, dir_handle) == CHIMERA_VFS_EACCES);
        TEST_PASS("readdir: a grant stamped on the shared handle exempts no unbound READDIR");
        chimera_vfs_release(ctx.vfs_thread, stream);

        memset(&sattr, 0, sizeof(sattr));
        chimera_vfs_open_at(ctx.vfs_thread, &other, root_handle, "rd", 2,
                            CHIMERA_VFS_OPEN_READ_ONLY |
                            CHIMERA_VFS_OPEN_DIRECTORY, &sattr,
                            CHIMERA_VFS_ATTR_FH, 0, 0, openat_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_EACCES);
        TEST_PASS("readdir: read bound at opendir survives a later chmod");

        /* FUSE binds read to its own stream after checking it at OPENDIR,
         * and opens the handle as NFS does, stamping nothing: the bound
         * stream lists, the same handle unbound does not. */
        assert(readdir_bound_as(&ctx, &other, dir_handle) == CHIMERA_VFS_OK);
        assert(readdir_as(&ctx, &other, dir_handle) == CHIMERA_VFS_EACCES);
        TEST_PASS("readdir: READ_BOUND is the caller's stream, not the handle's");

        /* Still 0700: SMB's handle-level check, not this gate, decides. */
        assert(readdir_as(&ctx, &smb_other, dir_handle) == CHIMERA_VFS_OK);
        TEST_PASS("readdir: AUTH_ATTR (SMB) caller not gated by the mode");

        /* Read without search (0744 for `other`, and 0644 for the owner,
         * whose rw- has no search either): the names, but each entry's
         * attributes and handle are withheld, as LOOKUP of it is refused.
         * A caller asking only for names and inode numbers is unaffected. */
        assert(chmod_as(&ctx, &owner, dir_fh, dir_fh_len, 0744) == CHIMERA_VFS_OK);
        assert(readdir_plus_full(&ctx, &other, dir_handle) == 0);
        assert(readdir_mask_as(&ctx, &other, dir_handle,
                               CHIMERA_VFS_ATTR_INUM) == CHIMERA_VFS_OK);
        assert(!(readdir_kid_mask & CHIMERA_VFS_ATTR_WITHHELD));
        assert(readdir_plus_full(&ctx, &owner, dir_handle) == 1);
        assert(readdir_plus_full(&ctx, &root_cred, dir_handle) == 1);
        assert(readdir_plus_full(&ctx, &smb_other, dir_handle) == 1);
        assert(chmod_as(&ctx, &owner, dir_fh, dir_fh_len, 0644) == CHIMERA_VFS_OK);
        assert(readdir_plus_full(&ctx, &owner, dir_handle) == 0);
        TEST_PASS("readdir: read without search withholds entry attributes");

        /* Search is never bound: a stream opened while the caller could
         * search loses the entries' attributes once it cannot, and gets
         * them back when it can again; its read stays bound. */
        assert(chmod_as(&ctx, &owner, dir_fh, dir_fh_len, 0755) == CHIMERA_VFS_OK);
        memset(&sattr, 0, sizeof(sattr));
        chimera_vfs_open_at(ctx.vfs_thread, &other, root_handle, "rd", 2,
                            CHIMERA_VFS_OPEN_READ_ONLY |
                            CHIMERA_VFS_OPEN_DIRECTORY, &sattr,
                            CHIMERA_VFS_ATTR_FH, 0, 0, openat_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_OK);
        stream = ctx.handle;
        assert(readdir_plus_bound(&ctx, &other, stream) == 1);
        assert(chmod_as(&ctx, &owner, dir_fh, dir_fh_len, 0744) == CHIMERA_VFS_OK);
        assert(readdir_plus_bound(&ctx, &other, stream) == 0);
        assert(chmod_as(&ctx, &owner, dir_fh, dir_fh_len, 0700) == CHIMERA_VFS_OK);
        assert(readdir_bound_as(&ctx, &other, stream) == CHIMERA_VFS_OK);
        assert(chmod_as(&ctx, &owner, dir_fh, dir_fh_len, 0755) == CHIMERA_VFS_OK);
        assert(readdir_plus_bound(&ctx, &other, stream) == 1);
        chimera_vfs_release(ctx.vfs_thread, stream);
        TEST_PASS("readdir: search judged on every call, even on a bound stream");

        /* find (S3 listing) fails the walk rather than report entries it
         * could not stat or descend into. */
        assert(find_as(&ctx, &other, dir_fh, dir_fh_len) == CHIMERA_VFS_OK);
        assert(find_entries >= 1);
        assert(chmod_as(&ctx, &owner, dir_fh, dir_fh_len, 0744) == CHIMERA_VFS_OK);
        assert(find_as(&ctx, &other, dir_fh, dir_fh_len) == CHIMERA_VFS_EACCES);
        assert(find_as(&ctx, &owner, dir_fh, dir_fh_len) == CHIMERA_VFS_OK);
        TEST_PASS("find: read without search fails the walk with EACCES");

        /* The same below the top: a subdirectory the caller may not list,
         * or may list but not search, fails the whole walk, after the
         * walk reached and listed the directories above it. */
        assert(chmod_as(&ctx, &owner, dir_fh, dir_fh_len, 0755) == CHIMERA_VFS_OK);
        assert(mkdir_as(&ctx, &owner, dir_handle, "sub") == CHIMERA_VFS_OK);
        memcpy(sub_fh, ctx.fh, ctx.fh_len);
        sub_fh_len = ctx.fh_len;
        chimera_vfs_open_fh(ctx.vfs_thread, &owner, sub_fh, sub_fh_len,
                            CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_PATH |
                            CHIMERA_VFS_OPEN_DIRECTORY, openfh_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_OK);
        sub_handle = ctx.handle;
        assert(create_as(&ctx, &owner, sub_handle, "f") == CHIMERA_VFS_OK);
        chimera_vfs_release(ctx.vfs_thread, sub_handle);
        assert(find_as(&ctx, &other, dir_fh, dir_fh_len) == CHIMERA_VFS_OK);
        assert(find_entries >= 3);
        assert(chmod_as(&ctx, &owner, sub_fh, sub_fh_len, 0700) == CHIMERA_VFS_OK);
        assert(find_as(&ctx, &other, dir_fh, dir_fh_len) == CHIMERA_VFS_EACCES);
        assert(find_as(&ctx, &owner, dir_fh, dir_fh_len) == CHIMERA_VFS_OK);
        assert(chmod_as(&ctx, &owner, sub_fh, sub_fh_len, 0744) == CHIMERA_VFS_OK);
        assert(find_as(&ctx, &other, dir_fh, dir_fh_len) == CHIMERA_VFS_EACCES);
        assert(find_as(&ctx, &owner, dir_fh, dir_fh_len) == CHIMERA_VFS_OK);
        assert(find_as(&ctx, &root_cred, dir_fh, dir_fh_len) == CHIMERA_VFS_OK);
        assert(chmod_as(&ctx, &owner, sub_fh, sub_fh_len, 0755) == CHIMERA_VFS_OK);
        assert(find_as(&ctx, &other, dir_fh, dir_fh_len) == CHIMERA_VFS_OK);
        TEST_PASS("find: a subdirectory without read or search fails the walk");

        chimera_vfs_release(ctx.vfs_thread, dir_handle);
        chimera_vfs_release(ctx.vfs_thread, root_handle);
    }

    /* Unmount and remove the filesystem to exercise the full lifecycle. */
    chimera_vfs_umount(ctx.vfs_thread, NULL, "/test", mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    /* umount does not return until every handle on the mount has been
     * closed and released, so the removal below succeeds immediately.  The
     * retry is kept as a backstop only. */
    for (int i = 0; i < 50; i++) {
        chimera_vfs_rmfs(ctx.vfs_thread, NULL, "memfs", "fs0", mount_cb, &ctx);
        wait_done(&ctx);
        if (ctx.status != CHIMERA_VFS_EBUSY) {
            break;
        }
        usleep(100000);
    }
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_thread_destroy(ctx.vfs_thread);
    chimera_vfs_destroy(ctx.vfs);
    evpl_destroy(ctx.evpl);
    prometheus_metrics_destroy(metrics);

    fprintf(stderr, "All VFS enforcement tests passed!\n");
    return 0;
} /* main */
