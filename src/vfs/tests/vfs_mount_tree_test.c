// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Mount points, as on Linux.
 *
 *   rootfs:   with nothing mounted at "/", the built-in rootfs is the root and
 *             grows the directories nested mount paths need ("/a/b", "/a/c").
 *   realroot: a filesystem mounted at "/" replaces the rootfs, and further
 *             mounts go on directories that exist inside it.
 *   nested:   a mount on a directory inside another (non-root) mount.
 *
 * Each checks the namespace from both directions -- lookups and readdir land
 * on the mounted root, ".." and getparent from a mount root land on the
 * directory holding the mount point -- and the refusals: a busy mount point
 * cannot be removed, renamed, mounted on again or have its filesystem
 * unmounted from under a submount, and a missing mount point on a real
 * filesystem is ENOENT.
 */

#include <stdio.h>
#include <string.h>
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_internal_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_mount_table.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_cred.h"
#include "vfs/sdk/vfs_error.h"
#include "common/logging.h"
#include "prometheus-c.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

#define MAX_ENTRIES 16

struct fh {
    uint8_t  fh[CHIMERA_VFS_FH_SIZE];
    uint32_t len;
};

struct test_ctx {
    int                             done;
    enum chimera_vfs_error          status;
    struct evpl                    *evpl;
    struct chimera_vfs             *vfs;
    struct chimera_vfs_thread      *thread;
    struct prometheus_metrics      *metrics;
    struct chimera_vfs_cred         cred;
    struct chimera_vfs_open_handle *handle;
    struct fh                       fh;
    char                            name[CHIMERA_VFS_NAME_MAX];
    int                             namelen;
    int                             num_entries;
    char                            entry_name[MAX_ENTRIES][64];
    struct fh                       entry_fh[MAX_ENTRIES];
};

static void
wait_done(struct test_ctx *ctx)
{
    while (!ctx->done) {
        evpl_continue(ctx->evpl);
    }
    ctx->done = 0;
} /* wait_done */

static int
fh_eq(
    const struct fh *a,
    const struct fh *b)
{
    return a->len == b->len && memcmp(a->fh, b->fh, a->len) == 0;
} /* fh_eq */

static void
status_cb(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = status;
    ctx->done   = 1;
} /* status_cb */

static enum chimera_vfs_error
do_mkfs(
    struct test_ctx *ctx,
    const char      *fsname)
{
    chimera_vfs_mkfs(ctx->thread, NULL, "memfs", fsname, NULL, status_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* do_mkfs */

static enum chimera_vfs_error
do_mount(
    struct test_ctx *ctx,
    const char      *path,
    const char      *fsname)
{
    chimera_vfs_mount(ctx->thread, NULL, path, "memfs", fsname, NULL, status_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* do_mount */

static enum chimera_vfs_error
do_umount(
    struct test_ctx *ctx,
    const char      *path)
{
    chimera_vfs_umount(ctx->thread, NULL, path, status_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* do_umount */

static void
lookup_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    if (error_code == CHIMERA_VFS_OK) {
        memcpy(ctx->fh.fh, attr->va_fh, attr->va_fh_len);
        ctx->fh.len = attr->va_fh_len;
    }
    ctx->done = 1;
} /* lookup_cb */

/* Resolve a namespace path from the root. */
static enum chimera_vfs_error
do_lookup(
    struct test_ctx *ctx,
    const char      *path,
    struct fh       *out)
{
    struct fh root;

    chimera_vfs_get_root_fh(ctx->vfs, root.fh, &root.len);
    chimera_vfs_lookup(ctx->thread, &ctx->cred, root.fh, root.len, path, strlen(path),
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                       lookup_cb, ctx);
    wait_done(ctx);
    if (ctx->status == CHIMERA_VFS_OK && out) {
        *out = ctx->fh;
    }
    return ctx->status;
} /* do_lookup */

static void
open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->handle = oh;
    ctx->done   = 1;
} /* open_cb */

static struct chimera_vfs_open_handle *
do_open_dir(
    struct test_ctx *ctx,
    const struct fh *fh)
{
    chimera_vfs_open_fh(ctx->thread, &ctx->cred, fh->fh, fh->len,
                        CHIMERA_VFS_OPEN_PATH | CHIMERA_VFS_OPEN_INFERRED |
                        CHIMERA_VFS_OPEN_DIRECTORY, open_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    return ctx->handle;
} /* do_open_dir */

static void
lookup_at_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    lookup_cb(error_code, attr, private_data);
} /* lookup_at_cb */

/* lookup_at(dir, name) */
static enum chimera_vfs_error
do_lookup_at(
    struct test_ctx *ctx,
    const struct fh *dir,
    const char      *name,
    struct fh       *out)
{
    struct chimera_vfs_open_handle *oh = do_open_dir(ctx, dir);

    chimera_vfs_lookup_at(ctx->thread, &ctx->cred, oh, name, strlen(name),
                          CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                          lookup_at_cb, ctx);
    wait_done(ctx);
    chimera_vfs_release(ctx->thread, oh);
    if (ctx->status == CHIMERA_VFS_OK && out) {
        *out = ctx->fh;
    }
    return ctx->status;
} /* do_lookup_at */

static void
mkdir_at_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* mkdir_at_cb */

static enum chimera_vfs_error
do_mkdir_at(
    struct test_ctx *ctx,
    const struct fh *dir,
    const char      *name)
{
    struct chimera_vfs_open_handle *oh = do_open_dir(ctx, dir);
    struct chimera_vfs_attrs        set_attr;

    memset(&set_attr, 0, sizeof(set_attr));
    set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    set_attr.va_mode     = 0755;

    chimera_vfs_mkdir_at(ctx->thread, &ctx->cred, oh, name, strlen(name), &set_attr,
                         CHIMERA_VFS_ATTR_FH, 0, 0, mkdir_at_cb, ctx);
    wait_done(ctx);
    chimera_vfs_release(ctx->thread, oh);
    return ctx->status;
} /* do_mkdir_at */

static void
remove_at_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* remove_at_cb */

static enum chimera_vfs_error
do_remove_at(
    struct test_ctx *ctx,
    const struct fh *dir,
    const char      *name)
{
    struct chimera_vfs_open_handle *oh = do_open_dir(ctx, dir);

    chimera_vfs_remove_at(ctx->thread, &ctx->cred, oh, name, strlen(name),
                          NULL, 0, 0, 0, 0, NULL, remove_at_cb, ctx);
    wait_done(ctx);
    chimera_vfs_release(ctx->thread, oh);
    return ctx->status;
} /* do_remove_at */

static void
rename_at_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *fromdir_pre_attr,
    struct chimera_vfs_attrs *fromdir_post_attr,
    struct chimera_vfs_attrs *todir_pre_attr,
    struct chimera_vfs_attrs *todir_post_attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* rename_at_cb */

static enum chimera_vfs_error
do_rename_at(
    struct test_ctx *ctx,
    const struct fh *dir,
    const char      *name,
    const char      *new_name)
{
    chimera_vfs_rename_at(ctx->thread, &ctx->cred, dir->fh, dir->len, name, strlen(name),
                          dir->fh, dir->len, new_name, strlen(new_name),
                          NULL, 0, 0, 0, 0, NULL, NULL, rename_at_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* do_rename_at */

static void
getparent_cb(
    enum chimera_vfs_error error_code,
    const uint8_t         *parent_fh,
    uint16_t               parent_fh_len,
    const char            *name,
    uint16_t               name_len,
    void                  *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    if (error_code == CHIMERA_VFS_OK) {
        memcpy(ctx->fh.fh, parent_fh, parent_fh_len);
        ctx->fh.len = parent_fh_len;
        memcpy(ctx->name, name, name_len);
        ctx->namelen = name_len;
    }
    ctx->done = 1;
} /* getparent_cb */

static int
readdir_entry_cb(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct test_ctx *ctx = arg;

    if ((namelen == 1 && name[0] == '.') ||
        (namelen == 2 && name[0] == '.' && name[1] == '.')) {
        return 0;
    }

    assert(ctx->num_entries < MAX_ENTRIES);
    snprintf(ctx->entry_name[ctx->num_entries], 64, "%.*s", namelen, name);
    assert(attrs->va_set_mask & CHIMERA_VFS_ATTR_FH);
    memcpy(ctx->entry_fh[ctx->num_entries].fh, attrs->va_fh, attrs->va_fh_len);
    ctx->entry_fh[ctx->num_entries].len = attrs->va_fh_len;
    ctx->num_entries++;
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

static void
do_readdir(
    struct test_ctx *ctx,
    const struct fh *dir)
{
    struct chimera_vfs_open_handle *oh = do_open_dir(ctx, dir);

    ctx->num_entries = 0;
    chimera_vfs_readdir(ctx->thread, &ctx->cred, oh, CHIMERA_VFS_ATTR_FH, 0, 0, 0, 0,
                        NULL, 0, readdir_entry_cb, readdir_complete_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    chimera_vfs_release(ctx->thread, oh);
} /* do_readdir */

/* The fh readdir reported for `name`, or NULL. */
static const struct fh *
entry(
    struct test_ctx *ctx,
    const char      *name)
{
    for (int i = 0; i < ctx->num_entries; i++) {
        if (!strcmp(ctx->entry_name[i], name)) {
            return &ctx->entry_fh[i];
        }
    }
    return NULL;
} /* entry */

static void
setup(struct test_ctx *ctx)
{
    struct chimera_vfs_module_cfg module_cfgs[2];

    memset(ctx, 0, sizeof(*ctx));
    chimera_vfs_cred_init_unix(&ctx->cred, 0, 0, 0, NULL);

    ctx->metrics = prometheus_metrics_create(NULL, NULL, 0);
    memset(module_cfgs, 0, sizeof(module_cfgs));
    strncpy(module_cfgs[0].module_name, "memfs", sizeof(module_cfgs[0].module_name) - 1);
    strncpy(module_cfgs[1].module_name, "memkv", sizeof(module_cfgs[1].module_name) - 1);

    ctx->evpl   = evpl_create(NULL);
    ctx->vfs    = chimera_vfs_init(0, 0, module_cfgs, 2, "memkv", 60, 1, 1, 0, ctx->metrics);
    ctx->thread = chimera_vfs_thread_init(ctx->evpl, ctx->vfs);

    for (int i = 0; i < 6; i++) {
        char name[8];

        snprintf(name, sizeof(name), "fs%d", i);
        assert(do_mkfs(ctx, name) == CHIMERA_VFS_OK);
    }
} /* setup */

static void
teardown(struct test_ctx *ctx)
{
    chimera_vfs_thread_destroy(ctx->thread);
    chimera_vfs_destroy(ctx->vfs);
    evpl_destroy(ctx->evpl);
    prometheus_metrics_destroy(ctx->metrics);
} /* teardown */

/* The fh of a mount's root, read from the mount table. */
static void
mount_root(
    struct test_ctx *ctx,
    const char      *path,
    struct fh       *out)
{
    struct chimera_vfs_mount *mount;

    mount = chimera_vfs_mount_table_find_exact(ctx->vfs->mount_table, path, strlen(path));
    assert(mount);
    memcpy(out->fh, mount->root_fh, mount->root_fh_len);
    out->len = mount->root_fh_len;
} /* mount_root */

static void
test_rootfs(void)
{
    struct test_ctx ctx;
    struct fh       root, a, ab, ac, x, xy, fh, b_root, c_root;

    setup(&ctx);

    chimera_vfs_get_root_fh(ctx.vfs, root.fh, &root.len);

    assert(do_mount(&ctx, "/a/b", "fs0") == CHIMERA_VFS_OK);
    assert(do_mount(&ctx, "/a/c/", "fs1") == CHIMERA_VFS_OK);
    assert(do_mount(&ctx, "x", "fs2") == CHIMERA_VFS_OK);
    mount_root(&ctx, "a/b", &b_root);
    mount_root(&ctx, "a/c", &c_root);

    /* Nothing was mounted at "/": the rootfs is still the root. */
    chimera_vfs_get_root_fh(ctx.vfs, fh.fh, &fh.len);
    assert(fh_eq(&fh, &root));

    /* Lookups cross into the mounts, and the mounts are distinct. */
    assert(do_lookup(&ctx, "a", &a) == CHIMERA_VFS_OK);
    assert(do_lookup(&ctx, "a/b", &ab) == CHIMERA_VFS_OK);
    assert(do_lookup(&ctx, "/a/c", &ac) == CHIMERA_VFS_OK);
    assert(fh_eq(&ab, &b_root));
    assert(fh_eq(&ac, &c_root));
    assert(do_mkdir_at(&ctx, &ab, "only_in_b") == CHIMERA_VFS_OK);
    assert(do_lookup(&ctx, "a/b/only_in_b", NULL) == CHIMERA_VFS_OK);
    assert(do_lookup(&ctx, "a/c/only_in_b", NULL) == CHIMERA_VFS_ENOENT);
    TEST_PASS("rootfs: nested mount paths resolve into their mounts");

    /* readdir of the rootfs directory presents the mounted roots. */
    do_readdir(&ctx, &a);
    assert(ctx.num_entries == 2);
    assert(entry(&ctx, "b") && fh_eq(entry(&ctx, "b"), &b_root));
    assert(entry(&ctx, "c") && fh_eq(entry(&ctx, "c"), &c_root));
    do_readdir(&ctx, &root);
    assert(ctx.num_entries == 2 && entry(&ctx, "a") && entry(&ctx, "x"));
    TEST_PASS("rootfs: readdir lists mount points as the mounted roots");

    /* ".." and getparent from a mount root lead back to the mount point's
     * directory; ".." path components resolve across mounts. */
    assert(do_lookup_at(&ctx, &ab, "..", &fh) == CHIMERA_VFS_OK);
    assert(fh_eq(&fh, &a));
    assert(do_lookup(&ctx, "a/b/../c", &fh) == CHIMERA_VFS_OK);
    assert(fh_eq(&fh, &c_root));
    chimera_vfs_getparent(ctx.thread, &ctx.cred, ab.fh, ab.len, getparent_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    assert(fh_eq(&ctx.fh, &a) && ctx.namelen == 1 && ctx.name[0] == 'b');
    TEST_PASS("rootfs: .. and getparent leave a mount for its mount point's directory");

    /* A mount the backend refuses leaves no mount point behind. */
    assert(do_mount(&ctx, "/q/r", "no_such_fs") != CHIMERA_VFS_OK);
    assert(do_lookup(&ctx, "q", NULL) == CHIMERA_VFS_ENOENT);
    assert(do_mount(&ctx, "/a/nope", "no_such_fs") != CHIMERA_VFS_OK);
    assert(do_lookup(&ctx, "a/nope", NULL) == CHIMERA_VFS_ENOENT);
    TEST_PASS("rootfs: a failed mount takes back the directories it made");

    /* The rootfs is read-only to everyone but the core. */
    assert(do_mkdir_at(&ctx, &a, "new") == CHIMERA_VFS_EROFS);

    /* A busy mount point stays put. */
    assert(do_remove_at(&ctx, &a, "b") == CHIMERA_VFS_EBUSY);
    assert(do_rename_at(&ctx, &a, "b", "z") == CHIMERA_VFS_EBUSY);
    assert(do_rename_at(&ctx, &a, "z", "b") == CHIMERA_VFS_EBUSY);
    assert(do_mount(&ctx, "/a/b", "fs3") == CHIMERA_VFS_EBUSY);
    TEST_PASS("rootfs: a busy mount point cannot be removed, renamed onto or remounted");

    /* A mount inside a real mount needs its mount point to exist there. */
    assert(do_mount(&ctx, "/x/y", "fs3") == CHIMERA_VFS_ENOENT);
    assert(do_lookup(&ctx, "x", &x) == CHIMERA_VFS_OK);
    assert(do_mkdir_at(&ctx, &x, "y") == CHIMERA_VFS_OK);
    assert(do_mount(&ctx, "/x/y", "fs3") == CHIMERA_VFS_OK);
    assert(do_lookup(&ctx, "x/y", &xy) == CHIMERA_VFS_OK);
    mount_root(&ctx, "x/y", &fh);
    assert(fh_eq(&xy, &fh));
    assert(do_lookup_at(&ctx, &xy, "..", &fh) == CHIMERA_VFS_OK);
    assert(fh_eq(&fh, &x));
    assert(do_umount(&ctx, "/x") == CHIMERA_VFS_EBUSY);
    assert(do_umount(&ctx, "/x/y") == CHIMERA_VFS_OK);
    assert(do_umount(&ctx, "/x") == CHIMERA_VFS_OK);
    TEST_PASS("nested: mount on a directory inside a mount, unmount innermost first");

    /* The rootfs cannot be unmounted.  A mount at "/" goes over it, hiding
     * the mounts on its directories until it is unmounted, as an overmount
     * does on Linux. */
    assert(do_umount(&ctx, "/") == CHIMERA_VFS_EBUSY);
    assert(do_mount(&ctx, "/", "fs4") == CHIMERA_VFS_OK);
    assert(do_lookup(&ctx, "a", NULL) == CHIMERA_VFS_ENOENT);
    assert(do_mount(&ctx, "/", "fs5") == CHIMERA_VFS_EBUSY);
    assert(do_umount(&ctx, "/") == CHIMERA_VFS_OK);
    chimera_vfs_get_root_fh(ctx.vfs, fh.fh, &fh.len);
    assert(fh_eq(&fh, &root));
    assert(do_lookup(&ctx, "a/b", &fh) == CHIMERA_VFS_OK && fh_eq(&fh, &b_root));
    TEST_PASS("rootfs: a mount over / hides the rootfs and its mounts until unmounted");

    /* The rootfs holds only what mount points need: unmounting drops the
     * directories no remaining mount uses. */
    assert(do_umount(&ctx, "/a/b") == CHIMERA_VFS_OK);
    assert(do_lookup(&ctx, "a/b", NULL) == CHIMERA_VFS_ENOENT);
    assert(do_lookup(&ctx, "a", NULL) == CHIMERA_VFS_OK);
    assert(do_umount(&ctx, "/a/c") == CHIMERA_VFS_OK);
    assert(do_lookup(&ctx, "a", NULL) == CHIMERA_VFS_ENOENT);
    do_readdir(&ctx, &root);
    assert(ctx.num_entries == 0);
    TEST_PASS("rootfs: unmount removes the mount point directories nothing else needs");

    teardown(&ctx);
} /* test_rootfs */

static void
test_realroot(void)
{
    struct test_ctx ctx;
    struct fh       rootfs_root, root, data, fh;

    setup(&ctx);

    chimera_vfs_get_root_fh(ctx.vfs, rootfs_root.fh, &rootfs_root.len);

    assert(do_mount(&ctx, "/", "fs0") == CHIMERA_VFS_OK);
    mount_root(&ctx, "", &root);
    chimera_vfs_get_root_fh(ctx.vfs, fh.fh, &fh.len);
    assert(fh_eq(&fh, &root));
    assert(!fh_eq(&fh, &rootfs_root));
    assert(do_mount(&ctx, "/", "fs1") == CHIMERA_VFS_EBUSY);
    TEST_PASS("realroot: a mount at / replaces the rootfs as the namespace root");

    /* Mount points on a real root must exist, like any real filesystem. */
    assert(do_mount(&ctx, "/data", "fs1") == CHIMERA_VFS_ENOENT);
    assert(do_mkdir_at(&ctx, &root, "data") == CHIMERA_VFS_OK);
    assert(do_mount(&ctx, "/data", "fs1") == CHIMERA_VFS_OK);
    assert(do_lookup(&ctx, "data", &data) == CHIMERA_VFS_OK);
    mount_root(&ctx, "data", &fh);
    assert(fh_eq(&data, &fh));
    assert(do_mkdir_at(&ctx, &data, "inner") == CHIMERA_VFS_OK);
    assert(do_lookup(&ctx, "/data/inner", NULL) == CHIMERA_VFS_OK);
    assert(do_lookup_at(&ctx, &data, "..", &fh) == CHIMERA_VFS_OK);
    assert(fh_eq(&fh, &root));
    do_readdir(&ctx, &root);
    assert(entry(&ctx, "data") && fh_eq(entry(&ctx, "data"), &data));
    TEST_PASS("realroot: mounts on directories of the root filesystem");

    assert(do_remove_at(&ctx, &root, "data") == CHIMERA_VFS_EBUSY);
    assert(do_umount(&ctx, "/") == CHIMERA_VFS_EBUSY);
    assert(do_umount(&ctx, "/data") == CHIMERA_VFS_OK);
    assert(do_umount(&ctx, "/") == CHIMERA_VFS_OK);
    chimera_vfs_get_root_fh(ctx.vfs, fh.fh, &fh.len);
    assert(fh_eq(&fh, &rootfs_root));
    TEST_PASS("realroot: unmounting / hands the namespace back to the rootfs");

    teardown(&ctx);
} /* test_realroot */

static void
detached_cb(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    status_cb(thread, status, private_data);
} /* detached_cb */

static void
test_detached(void)
{
    struct test_ctx ctx;
    struct fh       root;

    setup(&ctx);

    chimera_vfs_mount_detached(ctx.thread, NULL, "hidden", "memfs", "fs0", NULL,
                               detached_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    assert(chimera_vfs_mount_table_find_exact(ctx.vfs->mount_table, "hidden", 6));

    chimera_vfs_get_root_fh(ctx.vfs, root.fh, &root.len);
    do_readdir(&ctx, &root);
    assert(ctx.num_entries == 0);
    assert(do_lookup(&ctx, "hidden", NULL) == CHIMERA_VFS_ENOENT);
    assert(do_umount(&ctx, "hidden") == CHIMERA_VFS_OK);
    TEST_PASS("detached: a detached mount has no place in the namespace");

    teardown(&ctx);
} /* test_detached */

int
main(
    int    argc,
    char **argv)
{
    chimera_log_init();

    test_rootfs();
    test_realroot();
    test_detached();

    fprintf(stderr, "All mount tree tests passed\n");
    return 0;
} /* main */
