// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Backend-parameterized: on an engine-authoritative backend that stores the
 * canonical ACL (memfs, cairn, diskfs), the stored ACL -- not the mode bits
 * -- decides a non-root open, whichever route the open takes:
 *
 *   - open_at by name under a directory handle (NFSv4 CLAIM_NULL, openat(2));
 *   - chimera_vfs_open by path from the vfs root, which resolves the file
 *     with LOOKUP_AT (the client library and the POSIX layer);
 *   - chimera_vfs_open of the file's own handle, which reads it with GETATTR
 *     (NFSv4 CLAIM_FH).
 *
 * The file's ACL denies one user READ_DATA ahead of an EVERYONE@ grant,
 * which no mode can express: the mode alone admits that user, so a route
 * whose reply leaves out the ACL shows up as a grant.
 *
 *     vfs_open_acl_test <backend>
 *
 * where <backend> is memfs (default), cairn, diskfs_io_uring, or diskfs_aio.
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
#include "vfs/sdk/vfs_acl.h"
#include "common/logging.h"
#include "prometheus-c.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

#define DEV_SIZE_BYTES (1024ULL * 1024ULL * 1024ULL) /* 1 GiB, sparse */

#define UID_DENIED     2000 /* the ACL denies this user READ_DATA */
#define UID_ADMITTED   3000 /* EVERYONE@ admits this one */

struct test_ctx {
    int                             done;
    enum chimera_vfs_error          status;
    struct chimera_vfs             *vfs;
    struct chimera_vfs_thread      *vfs_thread;
    struct evpl                    *evpl;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;
    struct chimera_vfs_open_handle *handle;
    uint64_t                        got_mask;
    uint64_t                        got_mode;
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
open_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->handle = oh;
    ctx->done   = 1;
} /* open_cb */

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
getattr_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status   = error_code;
    ctx->got_mask = 0;
    if (error_code == CHIMERA_VFS_OK) {
        ctx->got_mask = attr->va_set_mask;
        ctx->got_mode = attr->va_mode;
    }
    ctx->done = 1;
} /* getattr_cb */

/* Per-backend wiring, mirroring vfs_acl_sid_test. */
struct backend_spec {
    int         ncfg;
    const char *mount_module;
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
    } else if (strcmp(backend, "cairn") == 0) {
        strncpy(cfgs[0].module_name, "cairn", sizeof(cfgs[0].module_name) - 1);
        snprintf(cfg, sizeof(cfg), "{\"initialize\":true,\"path\":\"%s\"}",
                 session_dir);
        strncpy(cfgs[0].config_data, cfg, sizeof(cfgs[0].config_data) - 1);
        spec->mount_module = "cairn";
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
    } else {
        fprintf(stderr, "unknown backend: %s\n", backend);
        exit(2);
    }

    strncpy(cfgs[1].module_name, "memkv", sizeof(cfgs[1].module_name) - 1);
    spec->ncfg = 2;
} /* backend_configure */

enum route {
    ROUTE_OPEN_AT,
    ROUTE_PATH,
    ROUTE_FH,
    ROUTE_COUNT
};

static const char *route_names[ROUTE_COUNT] = {
    "open_at by name", "open by path from the vfs root", "open of the handle"
};

/* Open the test file read-only as `cred` by `route`; return the status. */
static enum chimera_vfs_error
open_by(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    enum route                      route,
    struct chimera_vfs_open_handle *dir,
    const uint8_t                  *vroot_fh,
    uint32_t                        vroot_fh_len,
    const uint8_t                  *file_fh,
    uint32_t                        file_fh_len)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));

    switch (route) {
        case ROUTE_OPEN_AT:
            chimera_vfs_open_at(ctx->vfs_thread, cred, dir, "f", 1,
                                CHIMERA_VFS_OPEN_READ_ONLY, &sattr,
                                CHIMERA_VFS_ATTR_FH, 0, 0, openat_cb, ctx);
            break;
        case ROUTE_PATH:
            chimera_vfs_open(ctx->vfs_thread, cred, vroot_fh, vroot_fh_len,
                             "test/f", 6, CHIMERA_VFS_OPEN_READ_ONLY, &sattr,
                             CHIMERA_VFS_ATTR_FH, open_cb, ctx);
            break;
        default:
            chimera_vfs_open(ctx->vfs_thread, cred, file_fh, file_fh_len,
                             "", 0, CHIMERA_VFS_OPEN_READ_ONLY, &sattr,
                             CHIMERA_VFS_ATTR_FH, open_cb, ctx);
            break;
    } /* switch */
    wait_done(ctx);

    if (ctx->status == CHIMERA_VFS_OK && ctx->handle) {
        chimera_vfs_release(ctx->vfs_thread, ctx->handle);
    }
    return ctx->status;
} /* open_by */

int
main(
    int    argc,
    char **argv)
{
    struct test_ctx                 ctx = { 0 };
    struct chimera_vfs_module_cfg   module_cfgs[2];
    struct backend_spec             spec;
    struct prometheus_metrics      *metrics;
    struct chimera_vfs_cred         root_cred, denied_cred, admitted_cred;
    struct chimera_vfs_attrs        sattr;
    struct chimera_vfs_open_handle *dir;
    const char                     *backend = argc > 1 ? argv[1] : "memfs";
    char                            tmpl[]  = "/tmp/vfs_openacl_XXXXXX";
    char                           *session_dir;
    uint8_t                         vroot_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        vroot_fh_len;
    uint8_t                         file_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        file_fh_len;
    uint8_t                         acl_storage[sizeof(struct chimera_acl) +
                                                2 * sizeof(struct chimera_ace)];
    struct chimera_acl             *acl = (struct chimera_acl *) acl_storage;
    enum chimera_vfs_error          st;
    int                             route;
    int                             failures = 0;

    chimera_log_init();
    chimera_vfs_cred_init_unix(&root_cred, 0, 0, 0, NULL);
    chimera_vfs_cred_init_unix(&denied_cred, UID_DENIED, UID_DENIED, 0, NULL);
    chimera_vfs_cred_init_unix(&admitted_cred, UID_ADMITTED, UID_ADMITTED, 0,
                               NULL);

    session_dir = mkdtemp(tmpl);
    assert(session_dir != NULL);

    memset(module_cfgs, 0, sizeof(module_cfgs));
    backend_configure(backend, session_dir, module_cfgs, &spec);

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(metrics != NULL);

    ctx.evpl = evpl_create(NULL);
    assert(ctx.evpl != NULL);

    ctx.vfs = chimera_vfs_init(0, 0, module_cfgs, spec.ncfg, "memkv", 60, 1, 1, 0,
                               metrics);
    assert(ctx.vfs != NULL);

    ctx.vfs_thread = chimera_vfs_thread_init(ctx.evpl, ctx.vfs);
    assert(ctx.vfs_thread != NULL);

    chimera_vfs_mkfs(ctx.vfs_thread, NULL, spec.mount_module, "fs0", NULL,
                     mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_mount(ctx.vfs_thread, NULL, "/test", spec.mount_module, "fs0",
                      NULL, mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_get_root_fh(vroot_fh, &vroot_fh_len);
    chimera_vfs_lookup(ctx.vfs_thread, &root_cred, vroot_fh, vroot_fh_len,
                       "test", 4,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                       lookup_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_open_fh(ctx.vfs_thread, &root_cred, ctx.fh, ctx.fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    dir = ctx.handle;

    /* test/f: 0666, owned by 1000:1000. */
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
        CHIMERA_VFS_ATTR_GID;
    sattr.va_mode = 0666;
    sattr.va_uid  = 1000;
    sattr.va_gid  = 1000;
    chimera_vfs_open_at(ctx.vfs_thread, &root_cred, dir, "f", 1,
                        CHIMERA_VFS_OPEN_CREATE, &sattr, CHIMERA_VFS_ATTR_FH,
                        0, 0, openat_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    memcpy(file_fh, ctx.fh, ctx.fh_len);
    file_fh_len = ctx.fh_len;

    /* DENY UID_DENIED READ_DATA, then ALLOW EVERYONE@ READ_DATA|WRITE_DATA. */
    memset(acl_storage, 0, sizeof(acl_storage));
    acl->num_aces            = 2;
    acl->aces[0].type        = CHIMERA_ACE_DENIED;
    acl->aces[0].access_mask = CHIMERA_ACE_READ_DATA;
    acl->aces[0].who.type    = CHIMERA_PRINCIPAL_USER;
    acl->aces[0].who.id      = UID_DENIED;
    acl->aces[1].type        = CHIMERA_ACE_ALLOWED;
    acl->aces[1].access_mask = CHIMERA_ACE_READ_DATA | CHIMERA_ACE_WRITE_DATA;
    acl->aces[1].who.type    = CHIMERA_PRINCIPAL_SPECIAL;
    acl->aces[1].who.special = CHIMERA_WHO_EVERYONE;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_ACL;
    sattr.va_acl      = acl;
    chimera_vfs_setattr(ctx.vfs_thread, &root_cred, ctx.handle, &sattr, 0, 0,
                        setattr_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    /* The premise: whatever mode the backend derived from that ACL, it
     * admits UID_DENIED (other class) to read, so only the ACL refuses. */
    chimera_vfs_getattr(ctx.vfs_thread, &root_cred, ctx.handle,
                        CHIMERA_VFS_ATTR_MASK_STAT, getattr_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    assert(ctx.got_mask & CHIMERA_VFS_ATTR_MODE);
    assert(ctx.got_mode & S_IROTH);

    chimera_vfs_release(ctx.vfs_thread, ctx.handle);

    for (route = 0; route < ROUTE_COUNT; route++) {
        st = open_by(&ctx, &denied_cred, route, dir, vroot_fh, vroot_fh_len,
                     file_fh, file_fh_len);
        if (st != CHIMERA_VFS_EACCES) {
            fprintf(stderr, "  FAIL: %s on %s as the denied user: "
                    "expected %d, got %d\n",
                    route_names[route], backend, CHIMERA_VFS_EACCES, st);
            failures++;
        }

        st = open_by(&ctx, &admitted_cred, route, dir, vroot_fh, vroot_fh_len,
                     file_fh, file_fh_len);
        if (st != CHIMERA_VFS_OK) {
            fprintf(stderr, "  FAIL: %s on %s as the admitted user: "
                    "expected 0, got %d\n", route_names[route], backend, st);
            failures++;
        }
    }

    assert(failures == 0);
    TEST_PASS("the stored ACL decides a non-root open on every route");

    chimera_vfs_release(ctx.vfs_thread, dir);

    chimera_vfs_umount(ctx.vfs_thread, NULL, "/test", mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    for (int i = 0; i < 50; i++) {
        chimera_vfs_rmfs(ctx.vfs_thread, NULL, spec.mount_module, "fs0",
                         mount_cb, &ctx);
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

    if (chimera_test_remove_tree(session_dir) != 0) {
        fprintf(stderr, "warning: could not remove %s\n", session_dir);
    }

    fprintf(stderr, "All open-ACL tests passed on %s\n", backend);
    return 0;
} /* main */
