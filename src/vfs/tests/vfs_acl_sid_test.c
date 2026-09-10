// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Backend-parameterized native-SID round trip through the VFS.
 *
 * On an engine-authoritative backend that stores the canonical ACL natively
 * (memfs, cairn, diskfs), an ACL whose principals carry native Windows SIDs
 * -- including an opaque CHIMERA_PRINCIPAL_SID the identity layer could not
 * map -- must come back byte-for-byte from getattr, and the owner/group SID
 * companions must persist alongside va_uid/va_gid.  A chown that does not
 * restate the owner SID must drop the stale one, and storing an owner SID on
 * an object with no explicit DACL must not manufacture an empty ACL (the
 * mode-derived DACL has to survive).
 *
 * The chown gate treats a SID as the companion of its numeric identity, so
 * a non-root leg checks who may attach, restate, replace or clear one.  A
 * SETATTR that shrinks a file and chowns it in one call must drop the stale
 * owner SID like any other chown.  And on the persistent backends everything
 * stored must come back byte for byte after a cold restart -- the in-memory
 * record mirrors are gone, so that is what proves the on-disk replay.
 *
 *     vfs_acl_sid_test <backend>
 *
 * where <backend> is memfs (default), cairn, diskfs_io_uring, or diskfs_aio.
 */

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
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
#include "vfs/sdk/vfs_acl.h"
#include "vfs/sdk/vfs_sid.h"
#include "common/logging.h"
#include "prometheus-c.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

#define DEV_SIZE_BYTES (1024ULL * 1024ULL * 1024ULL) /* 1 GiB, sparse */
#define TEST_MAX_ACES  8

#define SID_OWNER      "S-1-5-21-7-8-9-500"
#define SID_GROUP      "S-1-5-21-7-8-9-513"
#define SID_OPAQUE     "S-1-5-21-7-8-9-1001"

struct test_ctx {
    int                             done;
    enum chimera_vfs_error          status;
    struct chimera_vfs             *vfs;
    struct chimera_vfs_thread      *vfs_thread;
    struct evpl                    *evpl;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;
    struct chimera_vfs_open_handle *handle;

    /* getattr copy-out: the live attrs are only valid inside the callback */
    uint64_t                        got_mask;
    uint64_t                        got_uid;
    uint64_t                        got_gid;
    uint64_t                        got_mode;
    uint64_t                        got_size;
    struct chimera_sid              got_owner_sid;
    struct chimera_sid              got_group_sid;
    uint8_t                         got_acl_storage[sizeof(struct chimera_acl) +
                                                    TEST_MAX_ACES * sizeof(struct chimera_ace)];
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
    ctx->done   = 1;
} /* openat_cb */

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

    ctx->status = error_code;
    memset(&ctx->got_owner_sid, 0, sizeof(ctx->got_owner_sid));
    memset(&ctx->got_group_sid, 0, sizeof(ctx->got_group_sid));
    memset(ctx->got_acl_storage, 0, sizeof(ctx->got_acl_storage));
    ctx->got_mask = 0;

    if (error_code == CHIMERA_VFS_OK) {
        ctx->got_mask = attr->va_set_mask;
        ctx->got_uid  = attr->va_uid;
        ctx->got_gid  = attr->va_gid;
        ctx->got_mode = attr->va_mode;
        ctx->got_size = attr->va_size;
        if ((attr->va_set_mask & CHIMERA_VFS_ATTR_OWNER_SID) && attr->va_owner_sid) {
            ctx->got_owner_sid = *attr->va_owner_sid;
        }
        if ((attr->va_set_mask & CHIMERA_VFS_ATTR_GROUP_SID) && attr->va_group_sid) {
            ctx->got_group_sid = *attr->va_group_sid;
        }
        if ((attr->va_set_mask & CHIMERA_VFS_ATTR_ACL) && attr->va_acl &&
            attr->va_acl->num_aces <= TEST_MAX_ACES) {
            memcpy(ctx->got_acl_storage, attr->va_acl,
                   chimera_acl_size(attr->va_acl->num_aces));
        }
    }
    ctx->done = 1;
} /* getattr_cb */

static void
do_getattr(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred)
{
    chimera_vfs_getattr(ctx->vfs_thread, cred, ctx->handle,
                        CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL |
                        CHIMERA_VFS_ATTR_OWNER_SID | CHIMERA_VFS_ATTR_GROUP_SID,
                        getattr_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
} /* do_getattr */

static void
do_setattr(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    struct chimera_vfs_attrs      *sattr)
{
    chimera_vfs_setattr(ctx->vfs_thread, cred, ctx->handle, sattr, 0, 0,
                        setattr_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
} /* do_setattr */

/* Like do_setattr, but hands the status back so a gate refusal can be
 * asserted rather than aborting the test. */
static enum chimera_vfs_error
setattr_status(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    struct chimera_vfs_attrs      *sattr)
{
    chimera_vfs_setattr(ctx->vfs_thread, cred, ctx->handle, sattr, 0, 0,
                        setattr_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* setattr_status */

/* Look `name` up under `dir_fh` and open it; leaves the handle in ctx->handle. */
static void
open_child(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *dir_fh,
    uint32_t                       dir_fh_len,
    const char                    *name)
{
    chimera_vfs_lookup(ctx->vfs_thread, cred, dir_fh, dir_fh_len, name,
                       strlen(name),
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                       lookup_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);

    chimera_vfs_open_fh(ctx->vfs_thread, cred, ctx->fh, ctx->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
} /* open_child */

/* Create `name` (0644, uid/gid 1000) under `dir`; leaves the file open in
 * ctx->handle. */
static void
create_file(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir,
    const char                     *name)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
        CHIMERA_VFS_ATTR_GID;
    sattr.va_mode = 0644;
    sattr.va_uid  = 1000;
    sattr.va_gid  = 1000;

    chimera_vfs_open_at(ctx->vfs_thread, cred, dir, name, strlen(name),
                        CHIMERA_VFS_OPEN_CREATE, &sattr, CHIMERA_VFS_ATTR_FH,
                        0, 0, openat_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    assert(ctx->handle != NULL);
} /* create_file */

/* Per-backend wiring, mirroring vfs_zerorange_test. */
struct backend_spec {
    int         ncfg;
    const char *mount_module;
    char        mount_path[300];
    int         needs_mkfs;
    int         persistent; /* survives a cold restart of the VFS */
};

/* With `reuse` set the store created earlier in this run is opened as it is:
 * no device is (re)created and the module is not told to initialize it, so a
 * second chimera_vfs_init sees what the first one wrote. */
static void
backend_configure(
    const char                    *backend,
    const char                    *session_dir,
    struct chimera_vfs_module_cfg *cfgs,
    struct backend_spec           *spec,
    int                            reuse)
{
    const char *init = reuse ? "" : "\"initialize\":true,";
    char        dev_path[300];
    char        cfg[512];

    memset(spec, 0, sizeof(*spec));

    if (strcmp(backend, "memfs") == 0) {
        strncpy(cfgs[0].module_name, "memfs", sizeof(cfgs[0].module_name) - 1);
        spec->mount_module = "memfs";
        spec->needs_mkfs   = 1;
        snprintf(spec->mount_path, sizeof(spec->mount_path), "fs0");
    } else if (strcmp(backend, "cairn") == 0) {
        strncpy(cfgs[0].module_name, "cairn", sizeof(cfgs[0].module_name) - 1);
        snprintf(cfg, sizeof(cfg), "{%s\"path\":\"%s\"}", init, session_dir);
        strncpy(cfgs[0].config_data, cfg, sizeof(cfgs[0].config_data) - 1);
        spec->mount_module = "cairn";
        spec->needs_mkfs   = 1;
        spec->persistent   = 1;
        snprintf(spec->mount_path, sizeof(spec->mount_path), "fs0");
    } else if (strcmp(backend, "diskfs_io_uring") == 0 ||
               strcmp(backend, "diskfs_aio") == 0) {
        const char *iotype = (strcmp(backend, "diskfs_aio") == 0) ? "libaio"
                                                                  : "io_uring";

        snprintf(dev_path, sizeof(dev_path), "%s/device-0.img", session_dir);
        if (!reuse) {
            int fd = open(dev_path, O_CREAT | O_TRUNC | O_RDWR, 0644);
            int rc;

            assert(fd >= 0);
            rc = ftruncate(fd, (off_t) DEV_SIZE_BYTES);
            assert(rc == 0);
            close(fd);
        }

        snprintf(cfg, sizeof(cfg),
                 "{%s\"unsafe_async\":true,"
                 "\"intent_log_size\":67108864,"
                 "\"devices\":[{\"type\":\"%s\",\"size\":1,\"path\":\"%s\"}]}",
                 init, iotype, dev_path);
        strncpy(cfgs[0].module_name, "diskfs", sizeof(cfgs[0].module_name) - 1);
        strncpy(cfgs[0].config_data, cfg, sizeof(cfgs[0].config_data) - 1);
        spec->mount_module = "diskfs";
        spec->needs_mkfs   = 1;
        spec->persistent   = 1;
        snprintf(spec->mount_path, sizeof(spec->mount_path), "fs0");
    } else {
        fprintf(stderr, "unknown backend: %s\n", backend);
        exit(2);
    }

    strncpy(cfgs[1].module_name, "memkv", sizeof(cfgs[1].module_name) - 1);
    spec->ncfg = 2;
} /* backend_configure */

/* Mount fs0 at /test and open its root.  Fills root_fh / root_fh_len and
 * returns the root handle; used at start-up and again after the restart. */
static struct chimera_vfs_open_handle *
mount_root(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const struct backend_spec     *spec,
    uint8_t                       *root_fh,
    uint32_t                      *root_fh_len)
{
    chimera_vfs_mount(ctx->vfs_thread, NULL, "/test", spec->mount_module,
                      spec->mount_path, NULL, mount_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);

    chimera_vfs_get_root_fh(root_fh, root_fh_len);
    chimera_vfs_lookup(ctx->vfs_thread, cred, root_fh, *root_fh_len, "test", 4,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                       lookup_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    memcpy(root_fh, ctx->fh, ctx->fh_len);
    *root_fh_len = ctx->fh_len;

    chimera_vfs_open_fh(ctx->vfs_thread, cred, root_fh, *root_fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    return ctx->handle;
} /* mount_root */

/* Tear the VFS down and bring it back on the same store without initialising
 * it.  The inode caches and the in-memory record mirrors die with the old
 * instance, so whatever getattr reports afterwards was read back from the
 * device -- an in-process umount/mount would not prove that (diskfs keeps
 * its inode cache across umount). */
static void
cold_restart(
    struct test_ctx               *ctx,
    const char                    *backend,
    const char                    *session_dir,
    struct chimera_vfs_module_cfg *module_cfgs,
    struct backend_spec           *spec,
    struct prometheus_metrics    **metrics)
{
    chimera_vfs_thread_destroy(ctx->vfs_thread);
    chimera_vfs_destroy(ctx->vfs);
    prometheus_metrics_destroy(*metrics);

    memset(module_cfgs, 0, 2 * sizeof(*module_cfgs));
    backend_configure(backend, session_dir, module_cfgs, spec, 1 /* reuse */);

    *metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(*metrics != NULL);
    ctx->vfs = chimera_vfs_init(0, 0, module_cfgs, spec->ncfg, "memkv", 60, 1,
                                1, 0, *metrics);
    assert(ctx->vfs != NULL);
    ctx->vfs_thread = chimera_vfs_thread_init(ctx->evpl, ctx->vfs);
    assert(ctx->vfs_thread != NULL);
} /* cold_restart */

int
main(
    int    argc,
    char **argv)
{
    struct test_ctx                 ctx = { 0 };
    struct chimera_vfs_module_cfg   module_cfgs[2];
    struct backend_spec             spec;
    struct prometheus_metrics      *metrics;
    struct chimera_vfs_cred         cred;
    struct chimera_vfs_attrs        sattr;
    struct chimera_vfs_open_handle *root_handle;
    struct chimera_vfs_open_handle *f_handle;
    struct chimera_vfs_open_handle *g_handle;
    struct chimera_vfs_open_handle *h_handle;
    struct chimera_vfs_open_handle *t_handle;
    struct chimera_vfs_cred         owner_cred, other_cred;
    const char                     *backend = argc > 1 ? argv[1] : "memfs";
    char                            tmpl[]  = "/tmp/vfs_aclsid_XXXXXX";
    char                            rmcmd[400];
    char                           *session_dir;
    uint8_t                         root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        root_fh_len;
    struct chimera_sid              owner_sid, group_sid, opaque_sid;
    uint8_t                         acl_storage[sizeof(struct chimera_acl) +
                                                TEST_MAX_ACES * sizeof(struct chimera_ace)];
    struct chimera_acl             *acl     = (struct chimera_acl *) acl_storage;
    struct chimera_acl             *got_acl = (struct chimera_acl *) ctx.got_acl_storage;

    chimera_log_init();
    chimera_vfs_cred_init_unix(&cred, 0, 0, 0, NULL);
    chimera_vfs_cred_init_unix(&owner_cred, 1000, 1000, 0, NULL);
    chimera_vfs_cred_init_unix(&other_cred, 2000, 2000, 0, NULL);

    assert(chimera_sid_from_str(&owner_sid, SID_OWNER) == 0);
    assert(chimera_sid_from_str(&group_sid, SID_GROUP) == 0);
    assert(chimera_sid_from_str(&opaque_sid, SID_OPAQUE) == 0);

    session_dir = mkdtemp(tmpl);
    assert(session_dir != NULL);

    memset(module_cfgs, 0, sizeof(module_cfgs));
    backend_configure(backend, session_dir, module_cfgs, &spec, 0);

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(metrics != NULL);

    ctx.evpl = evpl_create(NULL);
    assert(ctx.evpl != NULL);

    ctx.vfs = chimera_vfs_init(0, 0, module_cfgs, spec.ncfg, "memkv", 60, 1, 1, 0,
                               metrics);
    assert(ctx.vfs != NULL);

    ctx.vfs_thread = chimera_vfs_thread_init(ctx.evpl, ctx.vfs);
    assert(ctx.vfs_thread != NULL);

    if (spec.needs_mkfs) {
        chimera_vfs_mkfs(ctx.vfs_thread, NULL, spec.mount_module, "fs0", NULL,
                         mount_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_OK);
    }

    root_handle = mount_root(&ctx, &cred, &spec, root_fh, &root_fh_len);

    create_file(&ctx, &cred, root_handle, "f");
    f_handle = ctx.handle;

    /* --- 1. an explicit DACL with SID-bearing principals + owner/group SIDs
     *        round-trips byte-for-byte --- */
    memset(acl_storage, 0, sizeof(acl_storage));
    acl->num_aces            = 2;
    acl->ctrl_flags          = CHIMERA_ACL_CTRL_PROTECTED;
    acl->aces[0].type        = CHIMERA_ACE_ALLOWED;
    acl->aces[0].flags       = 0;
    acl->aces[0].access_mask = CHIMERA_ACE_READ_DATA;
    acl->aces[0].who.type    = CHIMERA_PRINCIPAL_SID;
    acl->aces[0].who.sid     = opaque_sid;
    acl->aces[1].type        = CHIMERA_ACE_ALLOWED;
    acl->aces[1].flags       = 0;
    acl->aces[1].access_mask = CHIMERA_ACE_READ_DATA | CHIMERA_ACE_WRITE_DATA;
    acl->aces[1].who.type    = CHIMERA_PRINCIPAL_USER;
    acl->aces[1].who.id      = 1000;
    acl->aces[1].who.sid     = owner_sid;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_ACL |
        CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_OWNER_SID |
        CHIMERA_VFS_ATTR_GID | CHIMERA_VFS_ATTR_GROUP_SID;
    sattr.va_acl       = acl;
    sattr.va_uid       = 1000;
    sattr.va_owner_sid = &owner_sid;
    sattr.va_gid       = 1000;
    sattr.va_group_sid = &group_sid;
    ctx.handle         = f_handle;
    do_setattr(&ctx, &cred, &sattr);

    do_getattr(&ctx, &cred);
    assert(ctx.got_mask & CHIMERA_VFS_ATTR_ACL);
    assert(got_acl->num_aces == 2);
    assert(got_acl->ctrl_flags == CHIMERA_ACL_CTRL_PROTECTED);
    assert(memcmp(got_acl->aces, acl->aces, 2 * sizeof(struct chimera_ace)) == 0);
    assert(ctx.got_mask & CHIMERA_VFS_ATTR_OWNER_SID);
    assert(chimera_sid_equal(&ctx.got_owner_sid, &owner_sid));
    assert(ctx.got_mask & CHIMERA_VFS_ATTR_GROUP_SID);
    assert(chimera_sid_equal(&ctx.got_group_sid, &group_sid));
    assert(ctx.got_uid == 1000 && ctx.got_gid == 1000);
    TEST_PASS("SID-bearing ACL and owner/group SIDs round-trip via getattr");

    /* --- 2. a chown without a SID companion drops the stale owner SID and
     *        leaves the group SID and the DACL alone --- */
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_UID;
    sattr.va_uid      = 1001;
    do_setattr(&ctx, &cred, &sattr);

    do_getattr(&ctx, &cred);
    assert(ctx.got_uid == 1001);
    assert(!(ctx.got_mask & CHIMERA_VFS_ATTR_OWNER_SID));
    assert(!chimera_sid_present(&ctx.got_owner_sid));
    assert(ctx.got_mask & CHIMERA_VFS_ATTR_GROUP_SID);
    assert(chimera_sid_equal(&ctx.got_group_sid, &group_sid));
    assert(got_acl->num_aces == 2);
    assert(chimera_sid_equal(&got_acl->aces[0].who.sid, &opaque_sid));
    TEST_PASS("chown without a SID clears the owner SID only");

    /* --- 3. an explicit clear (GROUP_SID with no SID) removes the group SID --- */
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask  = CHIMERA_VFS_ATTR_GROUP_SID;
    sattr.va_group_sid = NULL;
    do_setattr(&ctx, &cred, &sattr);

    do_getattr(&ctx, &cred);
    assert(!(ctx.got_mask & CHIMERA_VFS_ATTR_GROUP_SID));
    assert(!(ctx.got_mask & CHIMERA_VFS_ATTR_OWNER_SID));
    assert(got_acl->num_aces == 2);
    TEST_PASS("explicit GROUP_SID clear removes the group SID");

    chimera_vfs_release(ctx.vfs_thread, f_handle);

    /* --- 4. owner SID on an object with no explicit DACL: the mode-derived
     *        DACL must survive (no empty ACL is manufactured) --- */
    create_file(&ctx, &cred, root_handle, "g");
    g_handle = ctx.handle;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask  = CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_OWNER_SID;
    sattr.va_uid       = 1000;
    sattr.va_owner_sid = &owner_sid;
    do_setattr(&ctx, &cred, &sattr);

    do_getattr(&ctx, &cred);
    assert(ctx.got_mask & CHIMERA_VFS_ATTR_OWNER_SID);
    assert(chimera_sid_equal(&ctx.got_owner_sid, &owner_sid));
    assert(ctx.got_mask & CHIMERA_VFS_ATTR_ACL);
    assert(got_acl->num_aces >= 3); /* mode-derived OWNER@/GROUP@/EVERYONE@ */
    assert(chimera_acl_to_mode(got_acl) == 0644);

    /* and a chmod on it still regenerates from mode, not from an empty ACL */
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0600;
    do_setattr(&ctx, &cred, &sattr);
    do_getattr(&ctx, &cred);
    assert((ctx.got_mode & 0777) == 0600);
    assert(got_acl->num_aces >= 3);
    assert(chimera_acl_to_mode(got_acl) == 0600);
    assert(ctx.got_mask & CHIMERA_VFS_ATTR_OWNER_SID);
    TEST_PASS("owner SID without an explicit DACL keeps the mode-derived ACL");

    chimera_vfs_release(ctx.vfs_thread, g_handle);

    /* --- 5. the chown gate treats a SID as the companion of its numeric
     *        identity: the owner may attach, restate or clear one but not
     *        replace it, and a non-owner may name one only with WRITE_OWNER
     *        and only alongside a uid it is taking to itself --- */
    create_file(&ctx, &cred, root_handle, "h");
    h_handle = ctx.handle;

    memset(acl_storage, 0, sizeof(acl_storage));
    acl->num_aces            = 1;
    acl->ctrl_flags          = CHIMERA_ACL_CTRL_PROTECTED;
    acl->aces[0].type        = CHIMERA_ACE_ALLOWED;
    acl->aces[0].flags       = 0;
    acl->aces[0].access_mask = CHIMERA_ACE_READ_DATA | CHIMERA_ACE_WRITE_DATA;
    acl->aces[0].who.type    = CHIMERA_PRINCIPAL_USER;
    acl->aces[0].who.id      = 1000;

    /* attach: the object has no SID yet, and the owner saves a DACL that
     * names its own SID alongside (the SMB AD flow) */
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask  = CHIMERA_VFS_ATTR_ACL | CHIMERA_VFS_ATTR_OWNER_SID;
    sattr.va_acl       = acl;
    sattr.va_owner_sid = &owner_sid;
    assert(setattr_status(&ctx, &owner_cred, &sattr) == CHIMERA_VFS_OK);
    do_getattr(&ctx, &cred);
    assert(ctx.got_uid == 1000);
    assert(ctx.got_mask & CHIMERA_VFS_ATTR_OWNER_SID);
    assert(chimera_sid_equal(&ctx.got_owner_sid, &owner_sid));

    /* restate: not a chown.  (sattr is rebuilt before every call: a backend
     * rewrites the caller's va_set_mask to what it applied.) */
    acl->aces[0].access_mask = CHIMERA_ACE_READ_DATA;
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask  = CHIMERA_VFS_ATTR_ACL | CHIMERA_VFS_ATTR_OWNER_SID;
    sattr.va_acl       = acl;
    sattr.va_owner_sid = &owner_sid;
    assert(setattr_status(&ctx, &owner_cred, &sattr) == CHIMERA_VFS_OK);

    /* replace: a different owner SID is an ownership change the owner may
     * not make; nothing of the refused set is applied */
    acl->aces[0].access_mask = CHIMERA_ACE_READ_DATA | CHIMERA_ACE_WRITE_DATA |
        CHIMERA_ACE_EXECUTE;
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask  = CHIMERA_VFS_ATTR_ACL | CHIMERA_VFS_ATTR_OWNER_SID;
    sattr.va_acl       = acl;
    sattr.va_owner_sid = &opaque_sid;
    assert(setattr_status(&ctx, &owner_cred, &sattr) == CHIMERA_VFS_EPERM);
    do_getattr(&ctx, &cred);
    assert(chimera_sid_equal(&ctx.got_owner_sid, &owner_sid));
    assert(got_acl->num_aces == 1);
    assert(got_acl->aces[0].access_mask == CHIMERA_ACE_READ_DATA);

    /* a non-owner without WRITE_OWNER may not name a SID at all -- this
     * used to be dispatched to the backend unchecked */
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask  = CHIMERA_VFS_ATTR_OWNER_SID;
    sattr.va_owner_sid = &opaque_sid;
    assert(setattr_status(&ctx, &other_cred, &sattr) == CHIMERA_VFS_EPERM);
    do_getattr(&ctx, &cred);
    assert(chimera_sid_equal(&ctx.got_owner_sid, &owner_sid));

    /* clear: permitted to the owner */
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask  = CHIMERA_VFS_ATTR_OWNER_SID;
    sattr.va_owner_sid = NULL;
    assert(setattr_status(&ctx, &owner_cred, &sattr) == CHIMERA_VFS_OK);
    do_getattr(&ctx, &cred);
    assert(!(ctx.got_mask & CHIMERA_VFS_ATTR_OWNER_SID));

    /* with WRITE_OWNER granted, the other user may take the object -- the
     * SID alongside its own uid, never on its own */
    memset(acl_storage, 0, sizeof(acl_storage));
    acl->num_aces            = 2;
    acl->ctrl_flags          = CHIMERA_ACL_CTRL_PROTECTED;
    acl->aces[0].type        = CHIMERA_ACE_ALLOWED;
    acl->aces[0].access_mask = CHIMERA_ACE_READ_DATA | CHIMERA_ACE_WRITE_DATA;
    acl->aces[0].who.type    = CHIMERA_PRINCIPAL_USER;
    acl->aces[0].who.id      = 1000;
    acl->aces[1].type        = CHIMERA_ACE_ALLOWED;
    acl->aces[1].access_mask = CHIMERA_ACE_WRITE_OWNER | CHIMERA_ACE_READ_ACL;
    acl->aces[1].who.type    = CHIMERA_PRINCIPAL_USER;
    acl->aces[1].who.id      = 2000;
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_ACL;
    sattr.va_acl      = acl;
    do_setattr(&ctx, &cred, &sattr);

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask  = CHIMERA_VFS_ATTR_OWNER_SID;
    sattr.va_owner_sid = &opaque_sid;
    assert(setattr_status(&ctx, &other_cred, &sattr) == CHIMERA_VFS_EPERM);

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask  = CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_OWNER_SID;
    sattr.va_uid       = 2000;
    sattr.va_owner_sid = &opaque_sid;
    assert(setattr_status(&ctx, &other_cred, &sattr) == CHIMERA_VFS_OK);
    do_getattr(&ctx, &cred);
    assert(ctx.got_uid == 2000);
    assert(chimera_sid_equal(&ctx.got_owner_sid, &opaque_sid));
    TEST_PASS("the chown gate governs the SID as the companion of its identity");

    /* --- 6. a shrinking truncate that also chowns drops the stale owner SID
     *        like any other chown, and one that also sets an ACL and a SID
     *        stores both (diskfs finished its truncate on a separate chain
     *        that skipped the record steps) --- */
    create_file(&ctx, &cred, root_handle, "t");
    t_handle = ctx.handle;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
    sattr.va_size     = 65536; /* grow first: a shrink needs a size to shrink */
    do_setattr(&ctx, &cred, &sattr);

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask  = CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_OWNER_SID;
    sattr.va_uid       = 1000;
    sattr.va_owner_sid = &owner_sid;
    do_setattr(&ctx, &cred, &sattr);
    do_getattr(&ctx, &cred);
    assert(chimera_sid_equal(&ctx.got_owner_sid, &owner_sid));

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_SIZE | CHIMERA_VFS_ATTR_UID;
    sattr.va_size     = 4096;
    sattr.va_uid      = 1001;
    do_setattr(&ctx, &cred, &sattr);
    do_getattr(&ctx, &cred);
    assert(ctx.got_size == 4096);
    assert(ctx.got_uid == 1001);
    assert(!(ctx.got_mask & CHIMERA_VFS_ATTR_OWNER_SID));
    assert(!chimera_sid_present(&ctx.got_owner_sid));

    memset(acl_storage, 0, sizeof(acl_storage));
    acl->num_aces            = 2;
    acl->ctrl_flags          = CHIMERA_ACL_CTRL_PROTECTED;
    acl->aces[0].type        = CHIMERA_ACE_ALLOWED;
    acl->aces[0].access_mask = CHIMERA_ACE_READ_DATA;
    acl->aces[0].who.type    = CHIMERA_PRINCIPAL_SID;
    acl->aces[0].who.sid     = opaque_sid;
    acl->aces[1].type        = CHIMERA_ACE_ALLOWED;
    acl->aces[1].access_mask = CHIMERA_ACE_READ_DATA | CHIMERA_ACE_WRITE_DATA;
    acl->aces[1].who.type    = CHIMERA_PRINCIPAL_USER;
    acl->aces[1].who.id      = 1000;
    acl->aces[1].who.sid     = owner_sid;
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_SIZE | CHIMERA_VFS_ATTR_ACL |
        CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_OWNER_SID;
    sattr.va_size      = 1024;
    sattr.va_acl       = acl;
    sattr.va_uid       = 1000;
    sattr.va_owner_sid = &owner_sid;
    do_setattr(&ctx, &cred, &sattr);
    do_getattr(&ctx, &cred);
    assert(ctx.got_size == 1024);
    assert(ctx.got_uid == 1000);
    assert(ctx.got_mask & CHIMERA_VFS_ATTR_ACL);
    assert(got_acl->num_aces == 2);
    assert(memcmp(got_acl->aces, acl->aces, 2 * sizeof(struct chimera_ace)) == 0);
    assert((ctx.got_mode & 0777) == (chimera_acl_to_mode(acl) & 0777));
    assert(chimera_sid_equal(&ctx.got_owner_sid, &owner_sid));
    TEST_PASS("a shrinking truncate keeps SID and ACL coherence");

    /* --- 7. everything stored comes back after a cold restart (memfs: after
     *        a plain umount/mount) --- */
    chimera_vfs_release(ctx.vfs_thread, h_handle);
    chimera_vfs_release(ctx.vfs_thread, t_handle);
    chimera_vfs_release(ctx.vfs_thread, root_handle);

    chimera_vfs_umount(ctx.vfs_thread, NULL, "/test", mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    if (spec.persistent) {
        cold_restart(&ctx, backend, session_dir, module_cfgs, &spec, &metrics);
    }
    root_handle = mount_root(&ctx, &cred, &spec, root_fh, &root_fh_len);

    /* f: two SID-bearing ACEs, chowned to 1001 (owner SID dropped), group
     * SID cleared explicitly */
    open_child(&ctx, &cred, root_fh, root_fh_len, "f");
    do_getattr(&ctx, &cred);
    assert(ctx.got_uid == 1001 && ctx.got_gid == 1000);
    assert(!(ctx.got_mask & CHIMERA_VFS_ATTR_OWNER_SID));
    assert(!(ctx.got_mask & CHIMERA_VFS_ATTR_GROUP_SID));
    assert(got_acl->num_aces == 2);
    assert(got_acl->ctrl_flags == CHIMERA_ACL_CTRL_PROTECTED);
    assert(got_acl->aces[0].who.type == CHIMERA_PRINCIPAL_SID);
    assert(chimera_sid_equal(&got_acl->aces[0].who.sid, &opaque_sid));
    assert(got_acl->aces[1].who.id == 1000);
    assert(chimera_sid_equal(&got_acl->aces[1].who.sid, &owner_sid));
    chimera_vfs_release(ctx.vfs_thread, ctx.handle);

    /* g: an owner SID with no ACL record -- the mode-derived DACL must still
     * be what comes back, so the SID record really is separate */
    open_child(&ctx, &cred, root_fh, root_fh_len, "g");
    do_getattr(&ctx, &cred);
    assert(chimera_sid_equal(&ctx.got_owner_sid, &owner_sid));
    assert(!(ctx.got_mask & CHIMERA_VFS_ATTR_GROUP_SID));
    assert((ctx.got_mode & 0777) == 0600);
    assert(got_acl->num_aces >= 3);
    assert(chimera_acl_to_mode(got_acl) == 0600);
    chimera_vfs_release(ctx.vfs_thread, ctx.handle);

    /* h: taken over by uid 2000 with the opaque SID as its companion */
    open_child(&ctx, &cred, root_fh, root_fh_len, "h");
    do_getattr(&ctx, &cred);
    assert(ctx.got_uid == 2000);
    assert(chimera_sid_equal(&ctx.got_owner_sid, &opaque_sid));
    assert(got_acl->num_aces == 2);
    chimera_vfs_release(ctx.vfs_thread, ctx.handle);

    /* t: the truncate-and-set state */
    open_child(&ctx, &cred, root_fh, root_fh_len, "t");
    do_getattr(&ctx, &cred);
    assert(ctx.got_size == 1024);
    assert(ctx.got_uid == 1000);
    assert(chimera_sid_equal(&ctx.got_owner_sid, &owner_sid));
    assert(got_acl->num_aces == 2);
    assert(memcmp(got_acl->aces, acl->aces, 2 * sizeof(struct chimera_ace)) == 0);
    chimera_vfs_release(ctx.vfs_thread, ctx.handle);
    TEST_PASS(spec.persistent ? "SIDs and ACLs survive a cold restart"
                              : "SIDs and ACLs survive umount and mount");

    chimera_vfs_release(ctx.vfs_thread, root_handle);

    chimera_vfs_umount(ctx.vfs_thread, NULL, "/test", mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    if (spec.needs_mkfs) {
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
    }

    chimera_vfs_thread_destroy(ctx.vfs_thread);
    chimera_vfs_destroy(ctx.vfs);
    evpl_destroy(ctx.evpl);
    prometheus_metrics_destroy(metrics);

    snprintf(rmcmd, sizeof(rmcmd), "rm -rf %s", session_dir);
    if (system(rmcmd) != 0) {
        fprintf(stderr, "warning: could not remove %s\n", session_dir);
    }

    fprintf(stderr, "All native-SID round-trip tests passed on %s\n", backend);
    return 0;
} /* main */
