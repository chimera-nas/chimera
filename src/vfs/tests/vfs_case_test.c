// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Case-insensitive name resolution, parameterized by backend.
 *
 * SMB callers (CHIMERA_VFS_AUTH_ATTR credentials) resolve names the way
 * Windows does -- case-insensitively, preferring an exact match -- while NFS
 * and POSIX callers (AUTH_UNIX) keep exact matching.  Both kinds of caller
 * share one VFS name cache, so besides the backend's matching this checks that
 * a case-insensitive match never teaches an exact-match caller a spelling that
 * is not stored, that a negative entry cached for one spelling never hides a
 * case variant from an SMB caller, and that removing or renaming an entry by
 * another spelling leaves no stale entry under the stored one.
 *
 *     vfs_case_test <backend>
 *
 * where <backend> is memfs (default), diskfs_io_uring, diskfs_aio or cairn.
 */

#include "common/test_host.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#ifdef _WIN32
#include "common/platform.h"
#else  /* ifdef _WIN32 */
#include <unistd.h>
#endif /* ifdef _WIN32 */
#include <fcntl.h>
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

#define DEV_SIZE_BYTES (1024ULL * 1024ULL * 1024ULL) /* 1 GiB, sparse */

struct test_ctx {
    int                             done;
    enum chimera_vfs_error          status;
    struct chimera_vfs_thread      *vfs_thread;
    struct evpl                    *evpl;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;
    struct chimera_vfs_open_handle *handle;
};

static struct test_ctx         ctx;
static struct chimera_vfs_cred cred_unix, cred_smb;
static uint8_t                 dir_fh[CHIMERA_VFS_FH_SIZE];
static uint32_t                dir_fh_len;
static int                     failures;

static void
wait_done(void)
{
    while (!ctx.done) {
        evpl_continue(ctx.evpl);
    }
    ctx.done = 0;
} /* wait_done */

static void
mount_cb(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *private_data)
{
    ctx.status = status;
    ctx.done   = 1;
} /* mount_cb */

static void
lookup_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    ctx.status = error_code;
    if (error_code == CHIMERA_VFS_OK) {
        memcpy(ctx.fh, attr->va_fh, attr->va_fh_len);
        ctx.fh_len = attr->va_fh_len;
    }
    ctx.done = 1;
} /* lookup_cb */

static void
lookup_at_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    lookup_cb(error_code, attr, private_data);
} /* lookup_at_cb */

static void
openfh_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    void                           *private_data)
{
    ctx.status = error_code;
    ctx.handle = oh;
    ctx.done   = 1;
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
    ctx.status = error_code;
    ctx.handle = oh;
    if (error_code == CHIMERA_VFS_OK) {
        memcpy(ctx.fh, oh->fh, oh->fh_len);
        ctx.fh_len = oh->fh_len;
    }
    ctx.done = 1;
} /* openat_cb */

static void
mkdir_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    ctx.status = error_code;
    ctx.done   = 1;
} /* mkdir_cb */

static void
remove_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *pre_attr,
    struct chimera_vfs_attrs *post_attr,
    void                     *private_data)
{
    ctx.status = error_code;
    ctx.done   = 1;
} /* remove_cb */

static void
rename_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *fromdir_pre_attr,
    struct chimera_vfs_attrs *fromdir_post_attr,
    struct chimera_vfs_attrs *todir_pre_attr,
    struct chimera_vfs_attrs *todir_post_attr,
    void                     *private_data)
{
    ctx.status = error_code;
    ctx.done   = 1;
} /* rename_cb */

static void
link_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *r_attr,
    struct chimera_vfs_attrs *r_dir_pre_attr,
    struct chimera_vfs_attrs *r_dir_post_attr,
    void                     *private_data)
{
    ctx.status = error_code;
    ctx.done   = 1;
} /* link_cb */

static struct chimera_vfs_open_handle *
open_dir(void)
{
    chimera_vfs_open_fh(ctx.vfs_thread, &cred_unix, dir_fh, dir_fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, NULL);
    wait_done();
    assert(ctx.status == CHIMERA_VFS_OK);
    return ctx.handle;
} /* open_dir */

/* Look name up in the test directory; on success the fh lands in ctx.fh. */
static enum chimera_vfs_error
lookup(
    const struct chimera_vfs_cred *cred,
    const char                    *name)
{
    struct chimera_vfs_open_handle *dir = open_dir();

    chimera_vfs_lookup_at(ctx.vfs_thread, cred, dir, name, strlen(name),
                          CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                          lookup_at_cb, NULL);
    wait_done();
    chimera_vfs_release(ctx.vfs_thread, dir);
    return ctx.status;
} /* lookup */

static enum chimera_vfs_error
open_name(
    const struct chimera_vfs_cred *cred,
    const char                    *name,
    unsigned int                   flags)
{
    struct chimera_vfs_open_handle *dir = open_dir();
    struct chimera_vfs_attrs        sattr;
    enum chimera_vfs_error          status;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0644;

    chimera_vfs_open_at(ctx.vfs_thread, cred, dir, name, strlen(name),
                        flags, &sattr, CHIMERA_VFS_ATTR_FH, 0, 0,
                        openat_cb, NULL);
    wait_done();
    status = ctx.status;
    if (status == CHIMERA_VFS_OK) {
        chimera_vfs_release(ctx.vfs_thread, ctx.handle);
    }
    chimera_vfs_release(ctx.vfs_thread, dir);
    return status;
} /* open_name */

static enum chimera_vfs_error
mkdir_name(
    const struct chimera_vfs_cred *cred,
    const char                    *name)
{
    struct chimera_vfs_open_handle *dir = open_dir();
    struct chimera_vfs_attrs        sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0755;

    chimera_vfs_mkdir_at(ctx.vfs_thread, cred, dir, name, strlen(name), &sattr,
                         CHIMERA_VFS_ATTR_FH, 0, 0, mkdir_cb, NULL);
    wait_done();
    chimera_vfs_release(ctx.vfs_thread, dir);
    return ctx.status;
} /* mkdir_name */

static enum chimera_vfs_error
remove_name(
    const struct chimera_vfs_cred *cred,
    const char                    *name)
{
    struct chimera_vfs_open_handle *dir = open_dir();

    chimera_vfs_remove_at(ctx.vfs_thread, cred, dir, name, strlen(name),
                          NULL, 0, 0, 0, 0, NULL, remove_cb, NULL);
    wait_done();
    chimera_vfs_release(ctx.vfs_thread, dir);
    return ctx.status;
} /* remove_name */

static enum chimera_vfs_error
rename_name(
    const struct chimera_vfs_cred *cred,
    const char                    *from,
    const char                    *to)
{
    chimera_vfs_rename_at(ctx.vfs_thread, cred, dir_fh, dir_fh_len,
                          from, strlen(from), dir_fh, dir_fh_len,
                          to, strlen(to), NULL, 0, 0, 0, 0, NULL, NULL,
                          rename_cb, NULL);
    wait_done();
    return ctx.status;
} /* rename_name */

static enum chimera_vfs_error
link_name(
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *fh,
    uint32_t                       fh_len,
    const char                    *name,
    unsigned int                   replace)
{
    chimera_vfs_link_at(ctx.vfs_thread, cred, fh, fh_len, dir_fh, dir_fh_len,
                        name, strlen(name), replace, CHIMERA_VFS_ATTR_FH, 0, 0,
                        NULL, NULL, link_cb, NULL);
    wait_done();
    return ctx.status;
} /* link_name */

static void
expect(
    const char            *what,
    enum chimera_vfs_error got,
    enum chimera_vfs_error want)
{
    if (got != want) {
        fprintf(stderr, "FAIL: %s: status %d, expected %d\n", what, got, want);
        failures++;
    }
} /* expect */

/* The fh of the last successful lookup is fh/fh_len. */
static void
expect_fh(
    const char    *what,
    const uint8_t *fh,
    uint32_t       fh_len)
{
    if (ctx.fh_len != fh_len || memcmp(ctx.fh, fh, fh_len) != 0) {
        fprintf(stderr, "FAIL: %s: resolved to another object\n", what);
        failures++;
    }
} /* expect_fh */

static void
run_checks(void)
{
    uint8_t  foo_fh[CHIMERA_VFS_FH_SIZE], tgt_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t foo_fh_len, tgt_fh_len;

    /* An exact-match caller never sees another spelling, and an SMB caller's
     * case-insensitive hit is not cached under the spelling it asked for. */
    expect("create Foo.txt", open_name(&cred_unix, "Foo.txt", CHIMERA_VFS_OPEN_CREATE),
           CHIMERA_VFS_OK);
    memcpy(foo_fh, ctx.fh, ctx.fh_len);
    foo_fh_len = ctx.fh_len;

    expect("unix lookup foo.txt", lookup(&cred_unix, "foo.txt"), CHIMERA_VFS_ENOENT);
    expect("smb lookup foo.txt", lookup(&cred_smb, "foo.txt"), CHIMERA_VFS_OK);
    expect_fh("smb lookup foo.txt", foo_fh, foo_fh_len);
    expect("smb lookup FOO.TXT", lookup(&cred_smb, "FOO.TXT"), CHIMERA_VFS_OK);
    expect_fh("smb lookup FOO.TXT", foo_fh, foo_fh_len);
    expect("unix lookup FOO.TXT after smb", lookup(&cred_unix, "FOO.TXT"), CHIMERA_VFS_ENOENT);
    expect("unix lookup foo.txt after smb", lookup(&cred_unix, "foo.txt"), CHIMERA_VFS_ENOENT);
    expect("smb lookup foo.txt after unix ENOENT", lookup(&cred_smb, "foo.txt"), CHIMERA_VFS_OK);

    expect("smb open fOO.TXT", open_name(&cred_smb, "fOO.TXT", 0), CHIMERA_VFS_OK);
    expect_fh("smb open fOO.TXT", foo_fh, foo_fh_len);
    expect("unix lookup fOO.TXT after smb open", lookup(&cred_unix, "fOO.TXT"), CHIMERA_VFS_ENOENT);
    expect("smb exclusive create FOO.txt",
           open_name(&cred_smb, "FOO.txt", CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE),
           CHIMERA_VFS_EEXIST);
    expect("smb mkdir FOO.TXT", mkdir_name(&cred_smb, "FOO.TXT"), CHIMERA_VFS_EEXIST);

    /* An exact match wins over a case variant. */
    expect("unix create foo.txt", open_name(&cred_unix, "foo.txt", CHIMERA_VFS_OPEN_CREATE),
           CHIMERA_VFS_OK);
    expect("smb lookup Foo.txt", lookup(&cred_smb, "Foo.txt"), CHIMERA_VFS_OK);
    expect_fh("smb lookup Foo.txt", foo_fh, foo_fh_len);
    expect("unix remove foo.txt", remove_name(&cred_unix, "foo.txt"), CHIMERA_VFS_OK);

    /* Removing by another spelling removes the stored name, cached or not. */
    expect("unix lookup Foo.txt", lookup(&cred_unix, "Foo.txt"), CHIMERA_VFS_OK);
    expect("smb remove FOO.TXT", remove_name(&cred_smb, "FOO.TXT"), CHIMERA_VFS_OK);
    expect("unix lookup Foo.txt after remove", lookup(&cred_unix, "Foo.txt"), CHIMERA_VFS_ENOENT);
    expect("smb lookup foo.txt after remove", lookup(&cred_smb, "foo.txt"), CHIMERA_VFS_ENOENT);

    /* Renaming by another spelling moves the stored name. */
    expect("create Bar", open_name(&cred_unix, "Bar", CHIMERA_VFS_OPEN_CREATE), CHIMERA_VFS_OK);
    expect("unix lookup Bar", lookup(&cred_unix, "Bar"), CHIMERA_VFS_OK);
    expect("smb rename BAR -> baz", rename_name(&cred_smb, "BAR", "baz"), CHIMERA_VFS_OK);
    expect("unix lookup Bar after rename", lookup(&cred_unix, "Bar"), CHIMERA_VFS_ENOENT);
    expect("unix lookup baz after rename", lookup(&cred_unix, "baz"), CHIMERA_VFS_OK);

    /* A rename that only changes case renames the entry, not onto itself. */
    expect("create Qux", open_name(&cred_unix, "Qux", CHIMERA_VFS_OPEN_CREATE), CHIMERA_VFS_OK);
    expect("unix lookup Qux", lookup(&cred_unix, "Qux"), CHIMERA_VFS_OK);
    expect("smb rename qux -> QUX", rename_name(&cred_smb, "qux", "QUX"), CHIMERA_VFS_OK);
    expect("unix lookup Qux after case rename", lookup(&cred_unix, "Qux"), CHIMERA_VFS_ENOENT);
    expect("unix lookup QUX after case rename", lookup(&cred_unix, "QUX"), CHIMERA_VFS_OK);

    /* Renaming an entry onto its own stored name changes nothing, however the
     * source was spelled. */
    expect("create same", open_name(&cred_unix, "same", CHIMERA_VFS_OPEN_CREATE), CHIMERA_VFS_OK);
    expect("unix rename same -> same", rename_name(&cred_unix, "same", "same"), CHIMERA_VFS_OK);
    expect("smb rename SAME -> same", rename_name(&cred_smb, "SAME", "same"), CHIMERA_VFS_OK);
    expect("unix lookup same after self renames", lookup(&cred_unix, "same"), CHIMERA_VFS_OK);
    expect("unix lookup SAME after self renames", lookup(&cred_unix, "SAME"), CHIMERA_VFS_ENOENT);

    /* Renaming onto a case variant replaces it. */
    expect("create Tgt", open_name(&cred_unix, "Tgt", CHIMERA_VFS_OPEN_CREATE), CHIMERA_VFS_OK);
    expect("unix lookup Tgt", lookup(&cred_unix, "Tgt"), CHIMERA_VFS_OK);
    expect("smb rename baz -> TGT", rename_name(&cred_smb, "baz", "TGT"), CHIMERA_VFS_OK);
    expect("unix lookup Tgt after replace", lookup(&cred_unix, "Tgt"), CHIMERA_VFS_ENOENT);
    expect("unix lookup TGT after replace", lookup(&cred_unix, "TGT"), CHIMERA_VFS_OK);
    expect("smb lookup tgt after replace", lookup(&cred_smb, "tgt"), CHIMERA_VFS_OK);
    memcpy(tgt_fh, ctx.fh, ctx.fh_len);
    tgt_fh_len = ctx.fh_len;

    /* So does linking onto one, where the backend can replace by link. */
    expect("create Lnk", open_name(&cred_unix, "Lnk", CHIMERA_VFS_OPEN_CREATE), CHIMERA_VFS_OK);
    expect("unix lookup Lnk", lookup(&cred_unix, "Lnk"), CHIMERA_VFS_OK);
    expect("smb link onto LNK without replace", link_name(&cred_smb, tgt_fh, tgt_fh_len, "LNK", 0),
           CHIMERA_VFS_EEXIST);
    if (link_name(&cred_smb, tgt_fh, tgt_fh_len, "LNK", 1) == CHIMERA_VFS_OK) {
        expect("unix lookup Lnk after link replace", lookup(&cred_unix, "Lnk"), CHIMERA_VFS_ENOENT);
        expect("unix lookup LNK after link replace", lookup(&cred_unix, "LNK"), CHIMERA_VFS_OK);
        expect_fh("unix lookup LNK after link replace", tgt_fh, tgt_fh_len);
    } else {
        fprintf(stderr, "NOTE: backend does not replace by link\n");
    }
} /* run_checks */

/* Per-backend wiring, as vfs_zerorange_test does it. */
struct backend_spec {
    int         ncfg;
    const char *mount_module;
    char        mount_path[300];
    int         needs_mkfs;
};

static void
backend_configure(
    const char                    *backend,
    const char                    *session_dir,
    struct chimera_vfs_module_cfg *cfgs,
    struct backend_spec           *spec)
{
    char dev_path[300];
    char cfg[512];

    memset(spec, 0, sizeof(*spec));

    if (strcmp(backend, "memfs") == 0) {
        strncpy(cfgs[0].module_name, "memfs", sizeof(cfgs[0].module_name) - 1);
        spec->mount_module = "memfs";
        spec->needs_mkfs   = 1;
        snprintf(spec->mount_path, sizeof(spec->mount_path), "fs0");
    } else if (strcmp(backend, "cairn") == 0) {
        strncpy(cfgs[0].module_name, "cairn", sizeof(cfgs[0].module_name) - 1);
        snprintf(cfg, sizeof(cfg),
                 "{\"initialize\":true,\"path\":\"%s\"}", session_dir);
        strncpy(cfgs[0].config_data, cfg, sizeof(cfgs[0].config_data) - 1);
        spec->mount_module = "cairn";
        spec->needs_mkfs   = 1;
        snprintf(spec->mount_path, sizeof(spec->mount_path), "fs0");
    } else if (strcmp(backend, "diskfs_io_uring") == 0 ||
               strcmp(backend, "diskfs_aio") == 0) {
        const char *iotype = (strcmp(backend, "diskfs_aio") == 0) ? "libaio"
                                                                  : "io_uring";
        int         fd, rc;

        snprintf(dev_path, sizeof(dev_path), "%s/device-0.img", session_dir);
        fd = open(dev_path, O_CREAT | O_TRUNC | O_RDWR, 0644);
        assert(fd >= 0);
        rc = ftruncate(fd, (off_t) DEV_SIZE_BYTES);
        assert(rc == 0);
        close(fd);

        snprintf(cfg, sizeof(cfg),
                 "{\"initialize\":true,\"unsafe_async\":true,"
                 "\"intent_log_size\":67108864,"
                 "\"devices\":[{\"type\":\"%s\",\"size\":1,\"path\":\"%s\"}]}",
                 iotype, dev_path);
        strncpy(cfgs[0].module_name, "diskfs", sizeof(cfgs[0].module_name) - 1);
        strncpy(cfgs[0].config_data, cfg, sizeof(cfgs[0].config_data) - 1);
        spec->mount_module = "diskfs";
        spec->needs_mkfs   = 1;
        snprintf(spec->mount_path, sizeof(spec->mount_path), "fs0");
    } else {
        fprintf(stderr, "unknown backend: %s\n", backend);
        exit(2);
    }

    strncpy(cfgs[1].module_name, "memkv", sizeof(cfgs[1].module_name) - 1);
    spec->ncfg = 2;
} /* backend_configure */

int
main(
    int    argc,
    char **argv)
{
    struct chimera_vfs_module_cfg module_cfgs[2];
    struct backend_spec           spec;
    struct prometheus_metrics    *metrics;
    struct chimera_vfs           *vfs;
    const char                   *backend = argc > 1 ? argv[1] : "memfs";
    char                          tmpl[]  = "/tmp/vfs_case_XXXXXX";
    char                         *session_dir;

    chimera_log_init();
    chimera_vfs_cred_init_unix(&cred_unix, 0, 0, 0, NULL);
    chimera_vfs_cred_init_attr(&cred_smb, 0, 0, 0, NULL);

    session_dir = mkdtemp(tmpl);
    assert(session_dir != NULL);

    memset(module_cfgs, 0, sizeof(module_cfgs));
    backend_configure(backend, session_dir, module_cfgs, &spec);

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(metrics != NULL);

    ctx.evpl = evpl_create(NULL);
    assert(ctx.evpl != NULL);

    vfs = chimera_vfs_init(0, 0, module_cfgs, spec.ncfg, "memkv", 60, 1, 1, 0,
                           metrics);
    assert(vfs != NULL);

    ctx.vfs_thread = chimera_vfs_thread_init(ctx.evpl, vfs);
    assert(ctx.vfs_thread != NULL);

    if (spec.needs_mkfs) {
        chimera_vfs_mkfs(ctx.vfs_thread, NULL, spec.mount_module, "fs0", NULL,
                         mount_cb, NULL);
        wait_done();
        assert(ctx.status == CHIMERA_VFS_OK);
    }

    chimera_vfs_mount(ctx.vfs_thread, NULL, "/test", spec.mount_module,
                      spec.mount_path, NULL, mount_cb, NULL);
    wait_done();
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_get_root_fh(vfs, dir_fh, &dir_fh_len);
    chimera_vfs_lookup(ctx.vfs_thread, &cred_unix, dir_fh, dir_fh_len, "test", 4,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                       lookup_cb, NULL);
    wait_done();
    assert(ctx.status == CHIMERA_VFS_OK);
    memcpy(dir_fh, ctx.fh, ctx.fh_len);
    dir_fh_len = ctx.fh_len;

    run_checks();

    chimera_vfs_umount(ctx.vfs_thread, NULL, "/test", mount_cb, NULL);
    wait_done();
    assert(ctx.status == CHIMERA_VFS_OK);

    if (spec.needs_mkfs) {
        for (int i = 0; i < 50; i++) {
            chimera_vfs_rmfs(ctx.vfs_thread, NULL, spec.mount_module, "fs0",
                             mount_cb, NULL);
            wait_done();
            if (ctx.status != CHIMERA_VFS_EBUSY) {
                break;
            }
            usleep(100000);
        }
        assert(ctx.status == CHIMERA_VFS_OK);
    }

    chimera_vfs_thread_destroy(ctx.vfs_thread);
    chimera_vfs_destroy(vfs);
    evpl_destroy(ctx.evpl);
    prometheus_metrics_destroy(metrics);

    if (failures) {
        fprintf(stderr, "FAIL [%s]: %d check(s) failed\n", backend, failures);
        return 1;
    }

    fprintf(stderr, "PASS [%s]\n", backend);
    return 0;
} /* main */
