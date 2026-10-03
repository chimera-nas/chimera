// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * overlay: a union of two filesystems mounted in the namespace.
 *
 * Two memfs filesystems are mounted at /lower and /upper and an overlay of
 * them at /ovl.  The test reads through the overlay, modifies it, and checks
 * both what the overlay shows and what the layers hold: copy-up into the
 * upper layer, whiteouts over removed lower names, opaque directories,
 * merged-directory listings, the renames it supports and refuses, stable
 * handles across copy-up, and a read-only overlay without an upper layer.
 */

#include <stdio.h>
#include <string.h>
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
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
    chimera_vfs_mount(ctx->thread, &ctx->cred, path, "memfs", fsname, NULL, status_cb, ctx);
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
    struct chimera_vfs_module_cfg module_cfgs[3];

    memset(ctx, 0, sizeof(*ctx));
    chimera_vfs_cred_init_unix(&ctx->cred, 0, 0, 0, NULL);

    ctx->metrics = prometheus_metrics_create(NULL, NULL, 0);
    memset(module_cfgs, 0, sizeof(module_cfgs));
    strncpy(module_cfgs[0].module_name, "memfs", sizeof(module_cfgs[0].module_name) - 1);
    strncpy(module_cfgs[1].module_name, "memkv", sizeof(module_cfgs[1].module_name) - 1);
    strncpy(module_cfgs[2].module_name, "overlay", sizeof(module_cfgs[2].module_name) - 1);

    ctx->evpl   = evpl_create(NULL);
    ctx->vfs    = chimera_vfs_init(0, 0, module_cfgs, 3, "memkv", 60, 1, 1, 0, ctx->metrics);
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


#define DATA_MAX 256

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

static char read_buf[DATA_MAX + 1];
static int  read_len;

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
    read_len    = 0;
    if (error_code == CHIMERA_VFS_OK) {
        for (int i = 0; i < niov && read_len < (int) count; i++) {
            int len = evpl_iovec_length(&iov[i]);

            if (read_len + len > (int) count) {
                len = count - read_len;
            }
            memcpy(read_buf + read_len, evpl_iovec_data(&iov[i]), len);
            read_len += len;
        }
        evpl_iovecs_release(ctx->evpl, iov, niov);
    }
    read_buf[read_len] = '\0';
    ctx->done          = 1;
} /* read_cb */

static void
open_at_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dir_pre_attr,
    struct chimera_vfs_attrs       *dir_post_attr,
    void                           *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->handle = oh;
    if (error_code == CHIMERA_VFS_OK) {
        memcpy(ctx->fh.fh, attr->va_fh, attr->va_fh_len);
        ctx->fh.len = attr->va_fh_len;
    }
    ctx->done = 1;
} /* open_at_cb */

/* Open (optionally creating) `name` in directory `dir`; returns the handle. */
static enum chimera_vfs_error
do_open_at(
    struct test_ctx                 *ctx,
    const struct fh                 *dir,
    const char                      *name,
    unsigned int                     flags,
    struct chimera_vfs_open_handle **out)
{
    struct chimera_vfs_open_handle *oh = do_open_dir(ctx, dir);
    struct chimera_vfs_attrs        set_attr;

    memset(&set_attr, 0, sizeof(set_attr));
    set_attr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    set_attr.va_mode     = 0644;

    chimera_vfs_open_at(ctx->thread, &ctx->cred, oh, name, strlen(name), flags, &set_attr,
                        CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0, 0,
                        open_at_cb, ctx);
    wait_done(ctx);
    chimera_vfs_release(ctx->thread, oh);
    *out = ctx->status == CHIMERA_VFS_OK ? ctx->handle : NULL;
    return ctx->status;
} /* do_open_at */

/* Replace the contents of `name` in `dir` (created if absent) with `data`. */
static enum chimera_vfs_error
put_file(
    struct test_ctx *ctx,
    const struct fh *dir,
    const char      *name,
    const char      *data)
{
    struct chimera_vfs_open_handle *oh;
    struct evpl_iovec               iov[4];
    int niov, len = strlen(data);
    enum chimera_vfs_error          rc;

    rc = do_open_at(ctx, dir, name, CHIMERA_VFS_OPEN_CREATE, &oh);
    if (rc != CHIMERA_VFS_OK) {
        return rc;
    }

    niov = evpl_iovec_alloc(ctx->evpl, len, 0, 4, 0, iov);
    memcpy(evpl_iovec_data(&iov[0]), data, len);
    chimera_vfs_write(ctx->thread, &ctx->cred, oh, 0, len, CHIMERA_VFS_WRITE_FILESYNC,
                      0, 0, iov, niov, write_cb, ctx);
    wait_done(ctx);
    evpl_iovecs_release(ctx->evpl, iov, niov);
    rc = ctx->status;
    chimera_vfs_release(ctx->thread, oh);
    return rc;
} /* put_file */

/* The contents of namespace path `path` into read_buf. */
static enum chimera_vfs_error
get_file(
    struct test_ctx *ctx,
    const char      *path)
{
    struct chimera_vfs_open_handle *oh;
    struct evpl_iovec               iov[8];
    struct fh                       fh;

    if (do_lookup(ctx, path, &fh) != CHIMERA_VFS_OK) {
        return ctx->status;
    }

    chimera_vfs_open_fh(ctx->thread, &ctx->cred, fh.fh, fh.len,
                        CHIMERA_VFS_OPEN_READ_ONLY, open_cb, ctx);
    wait_done(ctx);
    if (ctx->status != CHIMERA_VFS_OK) {
        return ctx->status;
    }
    oh = ctx->handle;

    chimera_vfs_read(ctx->thread, &ctx->cred, oh, 0, DATA_MAX, iov, 8, 0, read_cb, ctx);
    wait_done(ctx);
    chimera_vfs_release(ctx->thread, oh);
    return ctx->status;
} /* get_file */

static void
test_overlay(void)
{
    struct test_ctx ctx;
    struct fh       lower, ovl, od, ldir, fh1, fh2, sub;

    setup(&ctx);

    assert(do_mount(&ctx, "/lower", "fs0") == CHIMERA_VFS_OK);
    assert(do_mount(&ctx, "/upper", "fs1") == CHIMERA_VFS_OK);

    /* Lower layer: d/{f,g,sub/x} */
    assert(do_lookup(&ctx, "lower", &lower) == CHIMERA_VFS_OK);
    assert(do_mkdir_at(&ctx, &lower, "d") == CHIMERA_VFS_OK);
    assert(do_lookup(&ctx, "lower/d", &ldir) == CHIMERA_VFS_OK);
    assert(put_file(&ctx, &ldir, "f", "lower-f") == CHIMERA_VFS_OK);
    assert(put_file(&ctx, &ldir, "g", "lower-g") == CHIMERA_VFS_OK);
    assert(do_mkdir_at(&ctx, &ldir, "sub") == CHIMERA_VFS_OK);
    assert(do_lookup(&ctx, "lower/d/sub", &sub) == CHIMERA_VFS_OK);
    assert(put_file(&ctx, &sub, "x", "lower-x") == CHIMERA_VFS_OK);

    chimera_vfs_mount(ctx.thread, &ctx.cred, "/ovl", "overlay", "",
                      "lowerdir=/lower,upperdir=/upper", status_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    /* Reads fall through to the lower layer. */
    assert(get_file(&ctx, "ovl/d/f") == CHIMERA_VFS_OK && !strcmp(read_buf, "lower-f"));
    assert(do_lookup(&ctx, "ovl/d", &od) == CHIMERA_VFS_OK);
    do_readdir(&ctx, &od);
    assert(ctx.num_entries == 3 && entry(&ctx, "f") && entry(&ctx, "g") &&
           entry(&ctx, "sub"));
    assert(do_lookup(&ctx, "upper/d", NULL) == CHIMERA_VFS_ENOENT);
    TEST_PASS("overlay: the lower layer shows through");

    /* Writing copies up: the overlay changes, the lower layer does not, and
     * the handle survives. */
    assert(do_lookup(&ctx, "ovl/d/f", &fh1) == CHIMERA_VFS_OK);
    assert(put_file(&ctx, &od, "f", "upper-f") == CHIMERA_VFS_OK);
    assert(get_file(&ctx, "ovl/d/f") == CHIMERA_VFS_OK && !strcmp(read_buf, "upper-f"));
    assert(get_file(&ctx, "lower/d/f") == CHIMERA_VFS_OK && !strcmp(read_buf, "lower-f"));
    assert(get_file(&ctx, "upper/d/f") == CHIMERA_VFS_OK && !strcmp(read_buf, "upper-f"));
    assert(do_lookup(&ctx, "ovl/d/f", &fh2) == CHIMERA_VFS_OK && fh_eq(&fh1, &fh2));
    TEST_PASS("overlay: writing copies up, handles are stable");

    /* New names go to the upper layer. */
    assert(put_file(&ctx, &od, "new", "new-data") == CHIMERA_VFS_OK);
    assert(get_file(&ctx, "upper/d/new") == CHIMERA_VFS_OK);
    assert(do_lookup(&ctx, "lower/d/new", NULL) == CHIMERA_VFS_ENOENT);

    /* Removing a lower name leaves a whiteout; it is hidden in listings. */
    assert(do_remove_at(&ctx, &od, "g") == CHIMERA_VFS_OK);
    assert(do_lookup(&ctx, "ovl/d/g", NULL) == CHIMERA_VFS_ENOENT);
    assert(get_file(&ctx, "lower/d/g") == CHIMERA_VFS_OK && !strcmp(read_buf, "lower-g"));
    assert(do_lookup(&ctx, "upper/d/.wh.g", NULL) == CHIMERA_VFS_OK);
    do_readdir(&ctx, &od);
    assert(ctx.num_entries == 3 && entry(&ctx, "f") && entry(&ctx, "new") &&
           entry(&ctx, "sub") && !entry(&ctx, "g"));
    TEST_PASS("overlay: removing a lower name whites it out");

    /* A directory made over the whiteout is opaque. */
    assert(do_mkdir_at(&ctx, &od, "g") == CHIMERA_VFS_OK);
    assert(do_lookup(&ctx, "ovl/d/g", &fh1) == CHIMERA_VFS_OK);
    do_readdir(&ctx, &fh1);
    assert(ctx.num_entries == 0);
    assert(do_lookup(&ctx, "upper/d/g/.wh..wh..opq", NULL) == CHIMERA_VFS_OK);
    assert(do_lookup(&ctx, "ovl/d/g/.wh..wh..opq", NULL) == CHIMERA_VFS_ENOENT);
    TEST_PASS("overlay: a directory over a whiteout is opaque");

    /* A merged directory is removable only once empty. */
    assert(do_remove_at(&ctx, &od, "sub") == CHIMERA_VFS_ENOTEMPTY);
    assert(do_lookup(&ctx, "ovl/d/sub", &fh1) == CHIMERA_VFS_OK);
    assert(do_remove_at(&ctx, &fh1, "x") == CHIMERA_VFS_OK);
    assert(do_remove_at(&ctx, &od, "sub") == CHIMERA_VFS_OK);
    assert(do_lookup(&ctx, "ovl/d/sub", NULL) == CHIMERA_VFS_ENOENT);
    assert(get_file(&ctx, "lower/d/sub/x") == CHIMERA_VFS_OK);
    TEST_PASS("overlay: removing a merged directory");

    /* Renames: a copied-up file moves and its old name stays hidden; a lower
     * directory cannot move. */
    assert(do_rename_at(&ctx, &od, "f", "f2") == CHIMERA_VFS_OK);
    assert(do_lookup(&ctx, "ovl/d/f", NULL) == CHIMERA_VFS_ENOENT);
    assert(get_file(&ctx, "ovl/d/f2") == CHIMERA_VFS_OK && !strcmp(read_buf, "upper-f"));
    assert(do_lookup(&ctx, "ovl", &ovl) == CHIMERA_VFS_OK);
    assert(do_rename_at(&ctx, &ovl, "d", "e") == CHIMERA_VFS_EXDEV);
    TEST_PASS("overlay: rename");

    /* ".." from the overlay root leaves the overlay for its mount point's
     * directory. */
    assert(do_lookup(&ctx, "ovl/..", &fh1) == CHIMERA_VFS_OK);
    chimera_vfs_get_root_fh(ctx.vfs, fh2.fh, &fh2.len);
    assert(fh_eq(&fh1, &fh2));

    /* A read-only overlay: no upper layer. */
    chimera_vfs_mount(ctx.thread, &ctx.cred, "/ro", "overlay", "", "lowerdir=/lower",
                      status_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    assert(get_file(&ctx, "ro/d/g") == CHIMERA_VFS_OK && !strcmp(read_buf, "lower-g"));
    assert(do_lookup(&ctx, "ro/d", &fh1) == CHIMERA_VFS_OK);
    assert(do_mkdir_at(&ctx, &fh1, "nope") == CHIMERA_VFS_EROFS);
    assert(put_file(&ctx, &fh1, "g", "x") == CHIMERA_VFS_EROFS);
    TEST_PASS("overlay: without an upperdir it is read-only");

    assert(do_umount(&ctx, "/ro") == CHIMERA_VFS_OK);
    assert(do_umount(&ctx, "/ovl") == CHIMERA_VFS_OK);
    assert(do_umount(&ctx, "/upper") == CHIMERA_VFS_OK);
    assert(do_umount(&ctx, "/lower") == CHIMERA_VFS_OK);

    teardown(&ctx);
} /* test_overlay */

int
main(
    int    argc,
    char **argv)
{
    chimera_log_init();

    test_overlay();

    fprintf(stderr, "All overlay tests passed\n");
    return 0;
} /* main */
