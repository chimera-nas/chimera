// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * The windows-identity ACL capability and its mount option.
 *
 * A mount runs with the windows-ACL behaviour when its module declares
 * CHIMERA_VFS_CAP_ACL_WINDOWS or its options carry "windows_acl".  Either
 * way the module must also be ACL_NATIVE: the option on a mode-only module
 * is a configuration error and the mount is refused.  A shim module,
 * "noaclnat", is memfs with CHIMERA_VFS_CAP_ACL_NATIVE cleared, standing in
 * for such a module.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#ifdef _WIN32
#include "common/platform.h"
#else /* ifdef _WIN32 */
#include <unistd.h>
#endif /* ifdef _WIN32 */
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/vfs_acl_windows.h"
#include "vfs/sdk/vfs_sid.h"
#include "vfs/vfs_internal.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_cred.h"
#include "vfs/sdk/vfs_error.h"
#include "vfs/sdk/vfs_fh_magic.h"
#include "vfs/sdk/vfs_module.h"
#include "vfs/sdk/vfs_request.h"
#include "common/logging.h"
#include "common/macros.h"
#include "prometheus-c.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

extern struct chimera_vfs_module vfs_memfs;

/* ---- the noaclnat shim -------------------------------------------------- */

static void
noaclnat_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    vfs_memfs.dispatch(request, private_data);
} /* noaclnat_dispatch */

/* Completed from vfs_memfs in main() before the VFS is initialized; found by
 * chimera_vfs_init via dlsym("vfs_noaclnat"), which is why the test binary
 * is linked with ENABLE_EXPORTS. */
SYMBOL_EXPORT struct chimera_vfs_module vfs_noaclnat = {
    .sdk_version = CHIMERA_VFS_SDK_VERSION,
    .name        = "noaclnat",
    .fh_magic    = CHIMERA_VFS_FH_MAGIC_VENDOR0,
    .dispatch    = noaclnat_dispatch,
};

/* ---- fixture ------------------------------------------------------------ */

struct test_ctx {
    int                             done;
    enum chimera_vfs_error          status;
    struct chimera_vfs             *vfs;
    struct chimera_vfs_thread      *vfs_thread;
    struct evpl                    *evpl;
    struct prometheus_metrics      *metrics;
    struct chimera_vfs_cred         root_cred;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;
    struct chimera_vfs_open_handle *handle;
};

/* ---- helpers ------------------------------------------------------------ */

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
make_fs(
    struct test_ctx *ctx,
    const char      *module,
    const char      *fsname)
{
    chimera_vfs_mkfs(ctx->vfs_thread, NULL, module, fsname, NULL,
                     mount_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
} /* make_fs */

/* Mount fsname of module at "/<name>" with the given options string and
 * return the mount's status. */
static enum chimera_vfs_error
mount_status(
    struct test_ctx *ctx,
    const char      *name,
    const char      *module,
    const char      *fsname,
    const char      *options)
{
    char path[64];

    snprintf(path, sizeof(path), "/%s", name);
    chimera_vfs_mount(ctx->vfs_thread, NULL, path, module, fsname, options,
                      mount_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* mount_status */

/* Resolve the root of the mount at "/<name>": fills ctx->fh/fh_len and opens
 * it into ctx->handle. */
static void
open_mount_root(
    struct test_ctx *ctx,
    const char      *name)
{
    uint8_t  root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t root_fh_len;

    chimera_vfs_get_root_fh(ctx->vfs, root_fh, &root_fh_len);
    chimera_vfs_lookup(ctx->vfs_thread, &ctx->root_cred, root_fh, root_fh_len,
                       name, strlen(name),
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                       lookup_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);

    chimera_vfs_open_fh(ctx->vfs_thread, &ctx->root_cred, ctx->fh, ctx->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
} /* open_mount_root */

/* The effective mount flags of the mount whose root is in ctx->fh. */
static uint64_t
mount_flags(struct test_ctx *ctx)
{
    struct chimera_vfs_mount_attrs attrs;
    int                            rc;

    rc = chimera_vfs_mount_table_lookup_attrs(ctx->vfs->mount_table, ctx->fh,
                                              &attrs);
    assert(rc == 0);
    return attrs.flags;
} /* mount_flags */

static enum chimera_vfs_error
setattr_mode_status(struct test_ctx *ctx)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0700;

    chimera_vfs_setattr(ctx->vfs_thread, &ctx->root_cred, ctx->handle, &sattr,
                        0, 0, setattr_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* setattr_mode_status */

/* ---- tests -------------------------------------------------------------- */

static void
test_mount_option_accepted(struct test_ctx *ctx)
{
    enum chimera_vfs_error st = mount_status(ctx, "acc", "memfs", "fs_acc",
                                             "windows_acl");

    assert(st == CHIMERA_VFS_OK);
    open_mount_root(ctx, "acc");
    assert((mount_flags(ctx) & CHIMERA_VFS_MOUNT_ATTR_WINDOWS_ACL) != 0);
    assert((mount_flags(ctx) & CHIMERA_VFS_MOUNT_ATTR_READONLY) == 0);
    chimera_vfs_release(ctx->vfs_thread, ctx->handle);
    TEST_PASS("windows_acl option mounts and sets the effective flag");
} /* test_mount_option_accepted */

static void
test_mount_option_absent(struct test_ctx *ctx)
{
    enum chimera_vfs_error st = mount_status(ctx, "plain", "memfs", "fs_plain",
                                             NULL);

    assert(st == CHIMERA_VFS_OK);
    open_mount_root(ctx, "plain");
    assert((mount_flags(ctx) & CHIMERA_VFS_MOUNT_ATTR_WINDOWS_ACL) == 0);
    chimera_vfs_release(ctx->vfs_thread, ctx->handle);
    TEST_PASS("no option leaves the flag clear");
} /* test_mount_option_absent */

static void
test_mount_option_combined(struct test_ctx *ctx)
{
    enum chimera_vfs_error st = mount_status(ctx, "comb", "memfs", "fs_comb",
                                             "ro,windows_acl");

    assert(st == CHIMERA_VFS_OK);
    open_mount_root(ctx, "comb");
    assert((mount_flags(ctx) & CHIMERA_VFS_MOUNT_ATTR_WINDOWS_ACL) != 0);
    assert((mount_flags(ctx) & CHIMERA_VFS_MOUNT_ATTR_READONLY) != 0);
    assert(setattr_mode_status(ctx) == CHIMERA_VFS_EROFS);
    chimera_vfs_release(ctx->vfs_thread, ctx->handle);
    TEST_PASS("ro,windows_acl parses both options");
} /* test_mount_option_combined */

static void
test_mount_option_rejected_without_acl_native(struct test_ctx *ctx)
{
    enum chimera_vfs_error st;

    /* Control: the shim mounts fine without the option. */
    st = mount_status(ctx, "shimok", "noaclnat", "fs_shim", NULL);
    assert(st == CHIMERA_VFS_OK);

    st = mount_status(ctx, "shimbad", "noaclnat", "fs_shim", "windows_acl");
    assert(st == CHIMERA_VFS_EINVAL);
    TEST_PASS("windows_acl on a module without ACL_NATIVE is refused");
} /* test_mount_option_rejected_without_acl_native */

/* ---- pure principal transforms ------------------------------------------ */

#define TEST_ACL_MAX 4

struct test_acl {
    struct chimera_acl hdr;
    struct chimera_ace aces[TEST_ACL_MAX];
};

static struct chimera_acl *
acl_init(
    struct test_acl *t,
    unsigned         n)
{
    memset(t, 0, sizeof(*t));
    t->hdr.num_aces = n;
    return &t->hdr;
} /* acl_init */

static struct chimera_principal
make_sid_principal(const char *str)
{
    struct chimera_principal p;

    memset(&p, 0, sizeof(p));
    p.type = CHIMERA_PRINCIPAL_SID;
    assert(chimera_sid_from_str(&p.sid, str) == 0);
    return p;
} /* make_sid_principal */

static struct chimera_principal
make_id_principal(
    uint8_t     type,
    uint32_t    id,
    const char *sid)
{
    struct chimera_principal p;

    memset(&p, 0, sizeof(p));
    p.type = type;
    p.id   = id;
    if (sid) {
        assert(chimera_sid_from_str(&p.sid, sid) == 0);
    }
    return p;
} /* make_id_principal */

static void
test_strip_native_sid(void)
{
    struct test_acl          t;
    struct chimera_acl      *acl = acl_init(&t, 1);
    struct chimera_principal expect;

    t.aces[0].who = make_id_principal(CHIMERA_PRINCIPAL_USER, 1000,
                                      "S-1-5-21-7-8-9-500");
    expect = make_sid_principal("S-1-5-21-7-8-9-500");

    assert(chimera_vfs_acl_windows_strip(acl) == 0);
    assert(memcmp(&t.aces[0].who, &expect, sizeof(expect)) == 0);
    TEST_PASS("strip: native SID becomes PRINCIPAL_SID");
} /* test_strip_native_sid */

static void
test_strip_numeric_only(void)
{
    struct test_acl          t;
    struct chimera_acl      *acl = acl_init(&t, 2);
    struct chimera_principal eu, eg;

    t.aces[0].who = make_id_principal(CHIMERA_PRINCIPAL_USER, 1000, NULL);
    t.aces[1].who = make_id_principal(CHIMERA_PRINCIPAL_GROUP, 5, NULL);
    eu            = make_sid_principal("S-1-5-88-1-1000");
    eg            = make_sid_principal("S-1-5-88-2-5");

    assert(chimera_vfs_acl_windows_strip(acl) == 2);
    assert(memcmp(&t.aces[0].who, &eu, sizeof(eu)) == 0);
    assert(memcmp(&t.aces[1].who, &eg, sizeof(eg)) == 0);
    TEST_PASS("strip: numeric-only principals get algorithmic SIDs");
} /* test_strip_numeric_only */

static void
test_strip_passthrough(void)
{
    struct test_acl          t;
    struct chimera_acl      *acl = acl_init(&t, 2);
    struct chimera_principal owner, opaque;

    memset(&owner, 0, sizeof(owner));
    owner.type    = CHIMERA_PRINCIPAL_SPECIAL;
    owner.special = CHIMERA_WHO_OWNER;
    opaque        = make_sid_principal("S-1-5-21-1-2-3-1234");
    t.aces[0].who = owner;
    t.aces[1].who = opaque;

    assert(chimera_vfs_acl_windows_strip(acl) == 0);
    assert(memcmp(&t.aces[0].who, &owner, sizeof(owner)) == 0);
    assert(memcmp(&t.aces[1].who, &opaque, sizeof(opaque)) == 0);
    TEST_PASS("strip: SPECIAL and PRINCIPAL_SID pass through");
} /* test_strip_passthrough */

static void
test_enrich_sync_algorithmic(void)
{
    struct test_acl          t;
    struct chimera_acl      *acl = acl_init(&t, 2);
    struct chimera_principal eu, eg;

    t.aces[0].who = make_sid_principal("S-1-5-88-1-1000");
    t.aces[1].who = make_sid_principal("S-1-5-88-2-5");
    /* A stale flag on the user ACE must be cleared, and the group's set. */
    t.aces[0].flags = CHIMERA_ACE_FLAG_IDENTIFIER_GROUP;
    eu              = make_id_principal(CHIMERA_PRINCIPAL_USER, 1000, NULL);
    eg              = make_id_principal(CHIMERA_PRINCIPAL_GROUP, 5, NULL);

    assert(chimera_vfs_acl_windows_enrich_sync(acl) == 0);
    assert(memcmp(&t.aces[0].who, &eu, sizeof(eu)) == 0);
    assert(memcmp(&t.aces[1].who, &eg, sizeof(eg)) == 0);
    assert(!(t.aces[0].flags & CHIMERA_ACE_FLAG_IDENTIFIER_GROUP));
    assert(t.aces[1].flags & CHIMERA_ACE_FLAG_IDENTIFIER_GROUP);
    TEST_PASS("enrich_sync: algorithmic SIDs decode to USER/GROUP");
} /* test_enrich_sync_algorithmic */

static void
test_enrich_sync_counts_native(void)
{
    struct test_acl          t;
    struct chimera_acl      *acl = acl_init(&t, 2);
    struct chimera_principal native, expect;

    native        = make_sid_principal("S-1-5-21-7-8-9-500");
    expect        = native;
    t.aces[0].who = native;
    t.aces[1].who = make_sid_principal("S-1-5-88-1-7");

    assert(chimera_vfs_acl_windows_enrich_sync(acl) == 1);
    assert(memcmp(&t.aces[0].who, &expect, sizeof(expect)) == 0);
    assert(t.aces[1].who.type == CHIMERA_PRINCIPAL_USER);
    TEST_PASS("enrich_sync: native SIDs untouched and counted");
} /* test_enrich_sync_counts_native */

static void
test_strip_enrich_roundtrip(void)
{
    struct test_acl     t, orig;
    struct chimera_acl *acl = acl_init(&t, 2);

    t.aces[0].who   = make_id_principal(CHIMERA_PRINCIPAL_USER, 1000, NULL);
    t.aces[1].who   = make_id_principal(CHIMERA_PRINCIPAL_GROUP, 5, NULL);
    t.aces[1].flags = CHIMERA_ACE_FLAG_IDENTIFIER_GROUP;
    memcpy(&orig, &t, sizeof(t));

    assert(chimera_vfs_acl_windows_strip(acl) == 2);
    assert(chimera_vfs_acl_windows_enrich_sync(acl) == 0);
    assert(memcmp(&orig, &t, sizeof(t)) == 0);
    TEST_PASS("strip then enrich_sync round-trips byte-for-byte");
} /* test_strip_enrich_roundtrip */

int
main(
    int    argc,
    char **argv)
{
    struct test_ctx               ctx = { 0 };
    struct chimera_vfs_module_cfg module_cfgs[3];

    chimera_log_init();

    test_strip_native_sid();
    test_strip_numeric_only();
    test_strip_passthrough();
    test_enrich_sync_algorithmic();
    test_enrich_sync_counts_native();
    test_strip_enrich_roundtrip();

    chimera_vfs_cred_init_unix(&ctx.root_cred, 0, 0, 0, NULL);

    /* The shim is memfs in every respect but the cleared ACL_NATIVE bit. */
    vfs_noaclnat.capabilities = vfs_memfs.capabilities &
        ~(uint64_t) CHIMERA_VFS_CAP_ACL_NATIVE;
    vfs_noaclnat.init           = vfs_memfs.init;
    vfs_noaclnat.destroy        = vfs_memfs.destroy;
    vfs_noaclnat.thread_init    = vfs_memfs.thread_init;
    vfs_noaclnat.thread_destroy = vfs_memfs.thread_destroy;

    ctx.metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(ctx.metrics != NULL);

    memset(module_cfgs, 0, sizeof(module_cfgs));
    strncpy(module_cfgs[0].module_name, "memfs", sizeof(module_cfgs[0].module_name) - 1);
    strncpy(module_cfgs[1].module_name, "noaclnat", sizeof(module_cfgs[1].module_name) - 1);
    strncpy(module_cfgs[2].module_name, "memkv", sizeof(module_cfgs[2].module_name) - 1);

    ctx.evpl = evpl_create(NULL);
    assert(ctx.evpl != NULL);

    ctx.vfs = chimera_vfs_init(0, 0, module_cfgs, 3, "memkv", 60, 1, 1, 0,
                               ctx.metrics);
    assert(ctx.vfs != NULL);

    ctx.vfs_thread = chimera_vfs_thread_init(ctx.evpl, ctx.vfs);
    assert(ctx.vfs_thread != NULL);

    make_fs(&ctx, "memfs", "fs_acc");
    make_fs(&ctx, "memfs", "fs_plain");
    make_fs(&ctx, "memfs", "fs_comb");
    make_fs(&ctx, "noaclnat", "fs_shim");

    test_mount_option_accepted(&ctx);
    test_mount_option_absent(&ctx);
    test_mount_option_combined(&ctx);
    test_mount_option_rejected_without_acl_native(&ctx);

    chimera_vfs_thread_destroy(ctx.vfs_thread);
    chimera_vfs_destroy(ctx.vfs);
    prometheus_metrics_destroy(ctx.metrics);
    evpl_destroy(ctx.evpl);

    fprintf(stderr, "vfs_acl_windows_test: all tests passed\n");
    return 0;
} /* main */
