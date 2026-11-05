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
 *
 * On such a mount the core resolves the SID-only principals a reply ACL
 * carries back to uid/gid before anything above the VFS sees them.  memfs
 * stores the ACL it is handed verbatim, so the tests store SID-only ACLs
 * directly and a scripted identity handler decides how each SID resolves.
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
#include "vfs/vfs_identity.h"
#include "vfs/vfs_acl_windows.h"
#include "vfs/sdk/vfs_sid.h"
#include "vfs/vfs_internal.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_cred.h"
#include "vfs/sdk/vfs_error.h"
#include "vfs/sdk/vfs_fh_magic.h"
#include "vfs/sdk/vfs_module.h"
#include "vfs/sdk/vfs_request.h"
#include "common/atomic.h"
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

/* ---- the memcnt shim ---------------------------------------------------- */

/* memfs in every respect, counting the CLOSEs dispatched from the test's own
 * thread (the close thread's cache sweeps run on a thread of their own and
 * are not counted), so a test can see a backend open being released. */
static struct chimera_vfs_thread       *memcnt_thread;
static int                              memcnt_closes;

static void
memcnt_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    if (request->opcode == CHIMERA_VFS_OP_CLOSE &&
        request->thread == memcnt_thread) {
        memcnt_closes++;
    }
    vfs_memfs.dispatch(request, private_data);
} /* memcnt_dispatch */

SYMBOL_EXPORT struct chimera_vfs_module vfs_memcnt = {
    .sdk_version = CHIMERA_VFS_SDK_VERSION,
    .name        = "memcnt",
    .fh_magic    = CHIMERA_VFS_FH_MAGIC_VENDOR1,
    .dispatch    = memcnt_dispatch,
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
    /* The windows_acl mount's root, and the last reply's set mask and ACL
     * (copied out of the callback, where alone va_acl is valid). */
    struct chimera_vfs_open_handle *wdir;
    uint64_t                        got_mask;
    struct chimera_acl             *got_acl;
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

/* ---- read-path enrichment: the scripted identity handler --------------- */

/*
 * Answers BY_SID from a table: each SID resolves to a user or a group, is
 * authoritatively unknown, or finds the "infrastructure" down, and every
 * consultation is counted.  It runs on the resolver's worker threads; the
 * table is only edited between tests, while no resolve is in flight.
 * Resolved and authoritatively-unknown SIDs are cached by the resolver, so
 * each test uses SIDs of its own.
 */
#define FIX_MAX 160

struct fixture_sid {
    char     sid[CHIMERA_VFS_SID_MAX_LEN];
    enum chimera_vfs_identity_status mode;
    int      is_group;
    uint32_t id;
    int      calls;
};

static struct fixture_sid fixture_sids[FIX_MAX];
static int                fixture_nsids;
static int                fixture_sid_calls; /* every BY_SID consultation */

static struct fixture_sid *
fixture_sid_add(
    const char                      *sid,
    enum chimera_vfs_identity_status mode,
    int                              is_group,
    uint32_t                         id)
{
    struct fixture_sid *f;

    assert(fixture_nsids < FIX_MAX);
    f = &fixture_sids[fixture_nsids++];
    snprintf(f->sid, sizeof(f->sid), "%s", sid);
    f->mode     = mode;
    f->is_group = is_group;
    f->id       = id;
    f->calls    = 0;
    return f;
} /* fixture_sid_add */

static int
fixture_calls(struct fixture_sid *f)
{
    return chimera_atomic_load_n(&f->calls, CHIMERA_MEMORY_RELAXED);
} /* fixture_calls */

static enum chimera_vfs_identity_status
fixture_sid_handler(
    enum chimera_vfs_identity_key       key,
    uint32_t                            id,
    const char                         *name,
    struct chimera_vfs_identity_result *out,
    void                               *private_data)
{
    (void) id;
    (void) private_data;

    if (key != CHIMERA_VFS_IDENTITY_BY_SID || !name) {
        return CHIMERA_VFS_IDENTITY_PASS;
    }

    chimera_atomic_fetch_add(&fixture_sid_calls, 1, CHIMERA_MEMORY_RELAXED);

    for (int i = 0; i < fixture_nsids; i++) {
        struct fixture_sid *f = &fixture_sids[i];

        if (strcmp(f->sid, name) != 0) {
            continue;
        }

        chimera_atomic_fetch_add(&f->calls, 1, CHIMERA_MEMORY_RELAXED);

        if (f->mode == CHIMERA_VFS_IDENTITY_OK) {
            if (f->is_group) {
                out->is_group  = 1;
                out->group.gid = f->id;
                snprintf(out->group.groupname, sizeof(out->group.groupname),
                         "waclg%u", f->id);
                out->group.groupname_len = (int) strlen(out->group.groupname);
                snprintf(out->group.sid, sizeof(out->group.sid), "%s", f->sid);
            } else {
                out->user.uid = f->id;
                out->user.gid = f->id;
                snprintf(out->user.username, sizeof(out->user.username),
                         "waclu%u", f->id);
                out->user.username_len = (int) strlen(out->user.username);
                snprintf(out->user.sid, sizeof(out->user.sid), "%s", f->sid);
            }
        }
        return f->mode;
    }

    return CHIMERA_VFS_IDENTITY_PASS;
} /* fixture_sid_handler */

/* ---- read-path enrichment: helpers -------------------------------------- */

#define WACL_MAX_ACES 160
#define WACL_ACL_SIZE (sizeof(struct chimera_acl) + \
                       WACL_MAX_ACES * sizeof(struct chimera_ace))

static void
getattr_acl_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status            = error_code;
    ctx->got_mask          = 0;
    ctx->got_acl->num_aces = 0;
    if (error_code == CHIMERA_VFS_OK && attr) {
        ctx->got_mask = attr->va_set_mask;
        if ((attr->va_set_mask & CHIMERA_VFS_ATTR_ACL) && attr->va_acl) {
            assert(attr->va_acl->num_aces <= WACL_MAX_ACES);
            memcpy(ctx->got_acl, attr->va_acl,
                   chimera_acl_size(attr->va_acl->num_aces));
        }
    }
    ctx->done = 1;
} /* getattr_acl_cb */

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
create_unlinked_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->handle = oh;
    ctx->done   = 1;
} /* create_unlinked_cb */

/* mkdir_at reply: the new directory's fh and (when it came back) its ACL. */
static void
mkdir_acl_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_pre_attr,
    struct chimera_vfs_attrs *dir_post_attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    if (error_code == CHIMERA_VFS_OK && attr &&
        (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        memcpy(ctx->fh, attr->va_fh, attr->va_fh_len);
        ctx->fh_len = attr->va_fh_len;
    }
    getattr_acl_cb(error_code, attr, private_data);
} /* mkdir_acl_cb */

/* Create (as root) file `name` under `dir` and return its open handle. */
static struct chimera_vfs_open_handle *
create_file(
    struct test_ctx                *ctx,
    struct chimera_vfs_open_handle *dir,
    const char                     *name)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0666;
    chimera_vfs_open_at(ctx->vfs_thread, &ctx->root_cred, dir, name,
                        (int) strlen(name), CHIMERA_VFS_OPEN_CREATE, &sattr,
                        CHIMERA_VFS_ATTR_FH, 0, 0, openat_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    return ctx->handle;
} /* create_file */

/* Store `acl` verbatim on `h` (as root). */
static void
set_acl(
    struct test_ctx                *ctx,
    struct chimera_vfs_open_handle *h,
    struct chimera_acl             *acl)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_ACL;
    sattr.va_acl      = acl;
    chimera_vfs_setattr(ctx->vfs_thread, &ctx->root_cred, h, &sattr, 0, 0,
                        setattr_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
} /* set_acl */

/* GETATTR h's ACL (as root) into ctx->got_acl and return the status.
 * *inline_done reports whether the reply arrived before chimera_vfs_getattr
 * returned: a warm (all cache-hit) enrichment never parks. */
static enum chimera_vfs_error
get_acl(
    struct test_ctx                *ctx,
    struct chimera_vfs_open_handle *h,
    int                            *inline_done)
{
    chimera_vfs_getattr(ctx->vfs_thread, &ctx->root_cred, h,
                        CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL,
                        getattr_acl_cb, ctx);
    if (inline_done) {
        *inline_done = ctx->done;
    }
    wait_done(ctx);
    return ctx->status;
} /* get_acl */

static struct chimera_acl *
acl_buf_init(
    void    *buf,
    unsigned n)
{
    struct chimera_acl *acl = (struct chimera_acl *) buf;

    assert(n <= WACL_MAX_ACES);
    memset(buf, 0, chimera_acl_size(n));
    acl->num_aces = (uint16_t) n;
    return acl;
} /* acl_buf_init */

static void
ace_set(
    struct chimera_ace      *ace,
    uint16_t                 type,
    uint32_t                 access_mask,
    uint16_t                 flags,
    struct chimera_principal who)
{
    ace->type        = type;
    ace->flags       = flags;
    ace->access_mask = access_mask;
    ace->who         = who;
} /* ace_set */

static struct chimera_principal
make_special_principal(uint8_t special)
{
    struct chimera_principal p;

    memset(&p, 0, sizeof(p));
    p.type    = CHIMERA_PRINCIPAL_SPECIAL;
    p.special = special;
    return p;
} /* make_special_principal */

static int
principal_eq(
    const struct chimera_principal *a,
    const struct chimera_principal *b)
{
    return memcmp(a, b, sizeof(*a)) == 0;
} /* principal_eq */

/* uint64_t-backed so the struct chimera_acl cast is aligned. */
static uint64_t acl_in_buf[(WACL_ACL_SIZE + 7) / 8];

/* ---- read-path enrichment: tests ---------------------------------------- */

static void
test_enrich_getattr_native(struct test_ctx *ctx)
{
    const char                     *sid = "S-1-5-21-40-41-42-4000";
    struct fixture_sid             *f   = fixture_sid_add(sid, CHIMERA_VFS_IDENTITY_OK, 0, 4000);
    struct chimera_vfs_open_handle *h   = create_file(ctx, ctx->wdir, "native");
    struct chimera_acl             *acl = acl_buf_init(acl_in_buf, 2);
    struct chimera_principal        expect;
    int                             inline_done;

    ace_set(&acl->aces[0], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA, 0,
            make_sid_principal(sid));
    ace_set(&acl->aces[1], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA, 0,
            make_special_principal(CHIMERA_WHO_OWNER));
    set_acl(ctx, h, acl);
    expect = make_id_principal(CHIMERA_PRINCIPAL_USER, 4000, sid);

    /* Cold: the SID misses the cache, so the reply parks on the resolver. */
    assert(get_acl(ctx, h, &inline_done) == CHIMERA_VFS_OK);
    assert(!inline_done);
    assert(ctx->got_mask & CHIMERA_VFS_ATTR_ACL);
    assert(ctx->got_acl->num_aces == 2);
    assert(principal_eq(&ctx->got_acl->aces[0].who, &expect));
    assert(!(ctx->got_acl->aces[0].flags & CHIMERA_ACE_FLAG_IDENTIFIER_GROUP));
    assert(ctx->got_acl->aces[1].who.type == CHIMERA_PRINCIPAL_SPECIAL);
    assert(fixture_calls(f) == 1);

    /* Warm: the cached identity resolves inline and the reply never parks. */
    assert(get_acl(ctx, h, &inline_done) == CHIMERA_VFS_OK);
    assert(inline_done);
    assert(principal_eq(&ctx->got_acl->aces[0].who, &expect));
    assert(fixture_calls(f) == 1);

    chimera_vfs_release(ctx->vfs_thread, h);
    TEST_PASS("enrich: getattr resolves a native SID to USER uid, SID kept");
} /* test_enrich_getattr_native */

static void
test_enrich_dedup(struct test_ctx *ctx)
{
    const char                     *sid = "S-1-5-21-40-41-42-4100";
    struct fixture_sid             *f   = fixture_sid_add(sid, CHIMERA_VFS_IDENTITY_OK, 0, 4100);
    struct chimera_vfs_open_handle *h   = create_file(ctx, ctx->wdir, "dedup");
    struct chimera_acl             *acl = acl_buf_init(acl_in_buf, 4);
    struct chimera_principal        expect;

    ace_set(&acl->aces[0], CHIMERA_ACE_DENIED, CHIMERA_ACE_WRITE_DATA, 0,
            make_sid_principal(sid));
    ace_set(&acl->aces[1], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA, 0,
            make_special_principal(CHIMERA_WHO_EVERYONE));
    ace_set(&acl->aces[2], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA, 0,
            make_sid_principal(sid));
    ace_set(&acl->aces[3], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_EXECUTE, 0,
            make_sid_principal(sid));
    set_acl(ctx, h, acl);
    expect = make_id_principal(CHIMERA_PRINCIPAL_USER, 4100, sid);

    assert(get_acl(ctx, h, NULL) == CHIMERA_VFS_OK);
    assert(ctx->got_acl->num_aces == 4);
    assert(principal_eq(&ctx->got_acl->aces[0].who, &expect));
    assert(principal_eq(&ctx->got_acl->aces[2].who, &expect));
    assert(principal_eq(&ctx->got_acl->aces[3].who, &expect));
    assert(ctx->got_acl->aces[1].who.type == CHIMERA_PRINCIPAL_SPECIAL);
    /* Three ACEs, one SID: one resolve, not three. */
    assert(fixture_calls(f) == 1);

    chimera_vfs_release(ctx->vfs_thread, h);
    TEST_PASS("enrich: one SID in three ACEs is resolved once, all rewritten");
} /* test_enrich_dedup */

#define MANY_ACES 128

static void
test_enrich_many(struct test_ctx *ctx)
{
    struct chimera_vfs_open_handle *h   = create_file(ctx, ctx->wdir, "many");
    struct chimera_acl             *acl = acl_buf_init(acl_in_buf, MANY_ACES);
    struct fixture_sid             *f[MANY_ACES];
    char                            sid[64];
    int                             inline_done;

    /* Users on even slots, groups on odd ones, every SID distinct. */
    for (int i = 0; i < MANY_ACES; i++) {
        snprintf(sid, sizeof(sid), "S-1-5-21-50-51-52-%d", 5000 + i);
        f[i] = fixture_sid_add(sid, CHIMERA_VFS_IDENTITY_OK, i & 1,
                               (uint32_t) (5000 + i));
        ace_set(&acl->aces[i], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA,
                (i & 1) ? 0 : CHIMERA_ACE_FLAG_IDENTIFIER_GROUP,
                make_sid_principal(sid));
    }
    set_acl(ctx, h, acl);

    assert(get_acl(ctx, h, &inline_done) == CHIMERA_VFS_OK);
    assert(!inline_done);
    assert(ctx->got_acl->num_aces == MANY_ACES);
    for (int i = 0; i < MANY_ACES; i++) {
        const struct chimera_ace *ace = &ctx->got_acl->aces[i];
        struct chimera_principal  expect;

        expect = make_id_principal((i & 1) ? CHIMERA_PRINCIPAL_GROUP :
                                   CHIMERA_PRINCIPAL_USER,
                                   (uint32_t) (5000 + i), f[i]->sid);
        assert(principal_eq(&ace->who, &expect));
        /* The (deliberately wrong) stored IDENTIFIER_GROUP flag is made to
         * agree with the resolved kind. */
        assert(!!(ace->flags & CHIMERA_ACE_FLAG_IDENTIFIER_GROUP) == (i & 1));
        assert(fixture_calls(f[i]) == 1);
    }

    chimera_vfs_release(ctx->vfs_thread, h);
    TEST_PASS("enrich: 128 distinct SIDs fan out and join");
} /* test_enrich_many */

static void
test_enrich_not_found_opaque(struct test_ctx *ctx)
{
    const char                     *gone   = "S-1-5-21-40-41-42-4200";
    const char                     *here   = "S-1-5-21-40-41-42-4201";
    struct fixture_sid             *f_gone = fixture_sid_add(gone, CHIMERA_VFS_IDENTITY_NOT_FOUND, 0, 0);
    struct fixture_sid             *f_here = fixture_sid_add(here, CHIMERA_VFS_IDENTITY_OK, 1, 4201);
    struct chimera_vfs_open_handle *h      = create_file(ctx, ctx->wdir, "notfound");
    struct chimera_acl             *acl    = acl_buf_init(acl_in_buf, 2);
    struct chimera_principal        opaque, expect;
    int                             inline_done;

    opaque = make_sid_principal(gone);
    ace_set(&acl->aces[0], CHIMERA_ACE_DENIED, CHIMERA_ACE_READ_DATA, 0, opaque);
    ace_set(&acl->aces[1], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA,
            CHIMERA_ACE_FLAG_IDENTIFIER_GROUP, make_sid_principal(here));
    set_acl(ctx, h, acl);
    expect = make_id_principal(CHIMERA_PRINCIPAL_GROUP, 4201, here);

    assert(get_acl(ctx, h, NULL) == CHIMERA_VFS_OK);
    assert(ctx->got_acl->num_aces == 2);
    assert(principal_eq(&ctx->got_acl->aces[0].who, &opaque));
    assert(principal_eq(&ctx->got_acl->aces[1].who, &expect));
    assert(fixture_calls(f_gone) == 1 && fixture_calls(f_here) == 1);

    /* Both answers are cached now (one of them negatively): inline. */
    assert(get_acl(ctx, h, &inline_done) == CHIMERA_VFS_OK);
    assert(inline_done);
    assert(principal_eq(&ctx->got_acl->aces[0].who, &opaque));
    assert(principal_eq(&ctx->got_acl->aces[1].who, &expect));
    assert(fixture_calls(f_gone) == 1 && fixture_calls(f_here) == 1);

    chimera_vfs_release(ctx->vfs_thread, h);
    TEST_PASS("enrich: an unknown SID stays opaque and the fetch succeeds");
} /* test_enrich_not_found_opaque */

static void
test_enrich_unavailable_fails(struct test_ctx *ctx)
{
    const char                     *down = "S-1-5-21-40-41-42-4300";
    const char                     *up   = "S-1-5-21-40-41-42-4301";
    struct fixture_sid             *fd   = fixture_sid_add(down, CHIMERA_VFS_IDENTITY_UNAVAILABLE, 0, 0);
    struct chimera_vfs_open_handle *h1   = create_file(ctx, ctx->wdir, "unavail1");
    struct chimera_vfs_open_handle *h2   = create_file(ctx, ctx->wdir, "unavail2");
    struct chimera_acl             *acl;

    fixture_sid_add(up, CHIMERA_VFS_IDENTITY_OK, 0, 4301);

    /* The only native SID cannot be resolved. */
    acl = acl_buf_init(acl_in_buf, 1);
    ace_set(&acl->aces[0], CHIMERA_ACE_DENIED, CHIMERA_ACE_READ_DATA, 0,
            make_sid_principal(down));
    set_acl(ctx, h1, acl);
    assert(get_acl(ctx, h1, NULL) == CHIMERA_VFS_EIO);

    /* Not cached: asked again on the next fetch, and it still fails. */
    assert(get_acl(ctx, h1, NULL) == CHIMERA_VFS_EIO);
    assert(fixture_calls(fd) == 2);

    /* One resolvable SID does not rescue a fetch another SID failed. */
    acl = acl_buf_init(acl_in_buf, 2);
    ace_set(&acl->aces[0], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA, 0,
            make_sid_principal(up));
    ace_set(&acl->aces[1], CHIMERA_ACE_DENIED, CHIMERA_ACE_READ_DATA, 0,
            make_sid_principal(down));
    set_acl(ctx, h2, acl);
    assert(get_acl(ctx, h2, NULL) == CHIMERA_VFS_EIO);

    chimera_vfs_release(ctx->vfs_thread, h1);
    chimera_vfs_release(ctx->vfs_thread, h2);
    TEST_PASS("enrich: an unavailable resolver fails the fetch with EIO");
} /* test_enrich_unavailable_fails */

static void
test_enrich_algorithmic_no_resolver(struct test_ctx *ctx)
{
    struct chimera_vfs_open_handle *h   = create_file(ctx, ctx->wdir, "algo");
    struct chimera_acl             *acl = acl_buf_init(acl_in_buf, 2);
    struct chimera_principal        eu, eg;
    int                             calls_before, inline_done;

    ace_set(&acl->aces[0], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA, 0,
            make_sid_principal("S-1-5-88-1-4400"));
    ace_set(&acl->aces[1], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA, 0,
            make_sid_principal("S-1-5-88-2-4401"));
    set_acl(ctx, h, acl);
    eu = make_id_principal(CHIMERA_PRINCIPAL_USER, 4400, NULL);
    eg = make_id_principal(CHIMERA_PRINCIPAL_GROUP, 4401, NULL);

    calls_before = chimera_atomic_load_n(&fixture_sid_calls, CHIMERA_MEMORY_RELAXED);
    assert(get_acl(ctx, h, &inline_done) == CHIMERA_VFS_OK);
    assert(inline_done);
    assert(principal_eq(&ctx->got_acl->aces[0].who, &eu));
    assert(principal_eq(&ctx->got_acl->aces[1].who, &eg));
    assert(ctx->got_acl->aces[1].flags & CHIMERA_ACE_FLAG_IDENTIFIER_GROUP);
    assert(chimera_atomic_load_n(&fixture_sid_calls, CHIMERA_MEMORY_RELAXED) ==
           calls_before);

    chimera_vfs_release(ctx->vfs_thread, h);
    TEST_PASS("enrich: algorithmic SIDs decode inline, resolver not asked");
} /* test_enrich_algorithmic_no_resolver */

static void
test_enrich_create_reply(struct test_ctx *ctx)
{
    const char                     *sid = "S-1-5-21-40-41-42-4500";
    struct fixture_sid             *f   = fixture_sid_add(sid, CHIMERA_VFS_IDENTITY_OK, 0, 4500);
    struct chimera_acl             *acl = acl_buf_init(acl_in_buf, 2);
    struct chimera_vfs_open_handle *parent;
    struct chimera_vfs_attrs        sattr;
    struct chimera_principal        expect;
    int                             enriched = 0;

    /* The parent: a directory whose inheritable ACE names a native SID. */
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0755;
    chimera_vfs_mkdir_at(ctx->vfs_thread, &ctx->root_cred, ctx->wdir, "inh", 3,
                         &sattr, CHIMERA_VFS_ATTR_FH, 0, 0, mkdir_acl_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    chimera_vfs_open_fh(ctx->vfs_thread, &ctx->root_cred, ctx->fh, ctx->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    parent = ctx->handle;

    ace_set(&acl->aces[0], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_MASK_ALL,
            CHIMERA_ACE_FLAG_FILE_INHERIT | CHIMERA_ACE_FLAG_DIR_INHERIT,
            make_special_principal(CHIMERA_WHO_OWNER));
    ace_set(&acl->aces[1], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA,
            CHIMERA_ACE_FLAG_FILE_INHERIT | CHIMERA_ACE_FLAG_DIR_INHERIT,
            make_sid_principal(sid));
    set_acl(ctx, parent, acl);
    expect = make_id_principal(CHIMERA_PRINCIPAL_USER, 4500, sid);

    /* The child's ACL comes back in the mkdir reply itself. */
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0755;
    chimera_vfs_mkdir_at(ctx->vfs_thread, &ctx->root_cred, parent, "child", 5,
                         &sattr,
                         CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT |
                         CHIMERA_VFS_ATTR_ACL,
                         0, 0, mkdir_acl_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    assert(ctx->got_mask & CHIMERA_VFS_ATTR_ACL);
    for (unsigned i = 0; i < ctx->got_acl->num_aces; i++) {
        const struct chimera_principal *who = &ctx->got_acl->aces[i].who;

        assert(who->type != CHIMERA_PRINCIPAL_SID);
        if (principal_eq(who, &expect)) {
            enriched++;
        }
    }
    /* The effective entry and its inherit-only continuation, one resolve. */
    assert(enriched == 2);
    assert(fixture_calls(f) == 1);

    chimera_vfs_release(ctx->vfs_thread, parent);
    TEST_PASS("enrich: a mkdir reply's inherited ACL is enriched");
} /* test_enrich_create_reply */

#define UID_DENIED   2000 /* the stored ACL denies this user READ_DATA */
#define UID_ADMITTED 3000 /* EVERYONE@ admits this one */

/* Open the windows_acl mount's file "gated" read-only as `cred` by one of
 * the three routes vfs_open_acl_test covers; return the status. */
static enum chimera_vfs_error
open_gated_by(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    int                            route,
    const uint8_t                 *file_fh,
    uint32_t                       file_fh_len)
{
    struct chimera_vfs_attrs sattr;
    uint8_t vroot_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t vroot_fh_len;

    memset(&sattr, 0, sizeof(sattr));
    ctx->handle = NULL;

    switch (route) {
        case 0:
            chimera_vfs_open_at(ctx->vfs_thread, cred, ctx->wdir, "gated", 5,
                                CHIMERA_VFS_OPEN_READ_ONLY, &sattr,
                                CHIMERA_VFS_ATTR_FH, 0, 0, openat_cb, ctx);
            break;
        case 1:
            chimera_vfs_get_root_fh(ctx->vfs, vroot_fh, &vroot_fh_len);
            chimera_vfs_open(ctx->vfs_thread, cred, vroot_fh, vroot_fh_len,
                             "wacl/gated", 10, CHIMERA_VFS_OPEN_READ_ONLY,
                             &sattr, CHIMERA_VFS_ATTR_FH, open_cb, ctx);
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
} /* open_gated_by */

static void
test_open_gated_on_enriched_acl(struct test_ctx *ctx)
{
    static const char *const        routes[3] = {
        "open_at by name", "open by path", "open of the handle"
    };
    const char                     *sid = "S-1-5-21-40-41-42-2000";
    struct chimera_vfs_open_handle *h;
    struct chimera_acl             *acl = acl_buf_init(acl_in_buf, 2);
    struct chimera_vfs_cred         denied, admitted;
    uint8_t                         file_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        file_fh_len;

    fixture_sid_add(sid, CHIMERA_VFS_IDENTITY_OK, 0, UID_DENIED);
    chimera_vfs_cred_init_unix(&denied, UID_DENIED, UID_DENIED, 0, NULL);
    chimera_vfs_cred_init_unix(&admitted, UID_ADMITTED, UID_ADMITTED, 0, NULL);

    h = create_file(ctx, ctx->wdir, "gated");
    memcpy(file_fh, h->fh, h->fh_len);
    file_fh_len = h->fh_len;

    /* DENY <SID of UID_DENIED> READ_DATA, then ALLOW EVERYONE@: no mode can
     * say this, and a stored SID-only DENY matches nobody unless enriched. */
    ace_set(&acl->aces[0], CHIMERA_ACE_DENIED, CHIMERA_ACE_READ_DATA, 0,
            make_sid_principal(sid));
    ace_set(&acl->aces[1], CHIMERA_ACE_ALLOWED,
            CHIMERA_ACE_READ_DATA | CHIMERA_ACE_WRITE_DATA, 0,
            make_special_principal(CHIMERA_WHO_EVERYONE));
    set_acl(ctx, h, acl);
    chimera_vfs_release(ctx->vfs_thread, h);

    /* The first route meets a cold SID (parks), the rest a warm one. */
    for (int route = 0; route < 3; route++) {
        enum chimera_vfs_error st;

        st = open_gated_by(ctx, &denied, route, file_fh, file_fh_len);
        if (st != CHIMERA_VFS_EACCES) {
            fprintf(stderr, "  FAIL: %s as the denied user: expected %d, "
                    "got %d\n", routes[route], CHIMERA_VFS_EACCES, st);
            assert(0);
        }
        st = open_gated_by(ctx, &admitted, route, file_fh, file_fh_len);
        if (st != CHIMERA_VFS_OK) {
            fprintf(stderr, "  FAIL: %s as the admitted user: expected 0, "
                    "got %d\n", routes[route], st);
            assert(0);
        }
    }

    TEST_PASS("enrich: a stored SID-only ACL decides a non-root open");
} /* test_open_gated_on_enriched_acl */

/* A GETATTR reply captured on its own, so two can be in flight at once. */
struct acl_reply {
    int                done;
    enum chimera_vfs_error status;
    struct chimera_sid owner_sid;
    struct test_acl    acl;
};

static void
acl_reply_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    void                     *private_data)
{
    struct acl_reply *r = private_data;

    r->status = error_code;
    memset(&r->owner_sid, 0, sizeof(r->owner_sid));
    r->acl.hdr.num_aces = 0;
    if (error_code == CHIMERA_VFS_OK && attr) {
        if ((attr->va_set_mask & CHIMERA_VFS_ATTR_ACL) && attr->va_acl) {
            assert(attr->va_acl->num_aces <= TEST_ACL_MAX);
            memcpy(&r->acl, attr->va_acl,
                   chimera_acl_size(attr->va_acl->num_aces));
        }
        if ((attr->va_set_mask & CHIMERA_VFS_ATTR_OWNER_SID) &&
            attr->va_owner_sid) {
            r->owner_sid = *attr->va_owner_sid;
        }
    }
    r->done = 1;
} /* acl_reply_cb */

static void
test_enrich_parked_scratch_reuse(struct test_ctx *ctx)
{
    const char                     *sid_a   = "S-1-5-21-40-41-42-4600";
    const char                     *sid_b   = "S-1-5-21-40-41-42-4601";
    const char                     *owner_a = "S-1-5-21-40-41-42-4610";
    const char                     *owner_b = "S-1-5-21-40-41-42-4611";
    struct chimera_vfs_open_handle *ha      = create_file(ctx, ctx->wdir, "scratch_a");
    struct chimera_vfs_open_handle *hb      = create_file(ctx, ctx->wdir, "scratch_b");
    struct chimera_vfs_attrs        sattr;
    struct chimera_sid              osid_a, osid_b;
    struct chimera_acl             *acl;
    struct chimera_principal        expect_a, expect_b;
    struct acl_reply                ra, rb;

    fixture_sid_add(sid_a, CHIMERA_VFS_IDENTITY_OK, 0, 4600);
    fixture_sid_add(sid_b, CHIMERA_VFS_IDENTITY_OK, 0, 4601);
    assert(chimera_sid_from_str(&osid_a, owner_a) == 0);
    assert(chimera_sid_from_str(&osid_b, owner_b) == 0);

    /* Different ACL sizes and owner SIDs, so a reply left pointing at the
     * module's scratch would show the other file's. */
    acl = acl_buf_init(acl_in_buf, 1);
    ace_set(&acl->aces[0], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA, 0,
            make_sid_principal(sid_a));
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask  = CHIMERA_VFS_ATTR_ACL | CHIMERA_VFS_ATTR_OWNER_SID;
    sattr.va_acl       = acl;
    sattr.va_owner_sid = &osid_a;
    chimera_vfs_setattr(ctx->vfs_thread, &ctx->root_cred, ha, &sattr, 0, 0,
                        setattr_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);

    acl = acl_buf_init(acl_in_buf, 2);
    ace_set(&acl->aces[0], CHIMERA_ACE_DENIED, CHIMERA_ACE_WRITE_DATA, 0,
            make_sid_principal(sid_b));
    ace_set(&acl->aces[1], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA, 0,
            make_special_principal(CHIMERA_WHO_EVERYONE));
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask  = CHIMERA_VFS_ATTR_ACL | CHIMERA_VFS_ATTR_OWNER_SID;
    sattr.va_acl       = acl;
    sattr.va_owner_sid = &osid_b;
    chimera_vfs_setattr(ctx->vfs_thread, &ctx->root_cred, hb, &sattr, 0, 0,
                        setattr_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);

    /* Both cold, both parked at once: B's reply reuses the scratch A's came
     * back in while A is still waiting on the resolver. */
    memset(&ra, 0, sizeof(ra));
    memset(&rb, 0, sizeof(rb));
    chimera_vfs_getattr(ctx->vfs_thread, &ctx->root_cred, ha,
                        CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL |
                        CHIMERA_VFS_ATTR_OWNER_SID, acl_reply_cb, &ra);
    chimera_vfs_getattr(ctx->vfs_thread, &ctx->root_cred, hb,
                        CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL |
                        CHIMERA_VFS_ATTR_OWNER_SID, acl_reply_cb, &rb);
    assert(!ra.done && !rb.done);
    while (!ra.done || !rb.done) {
        evpl_continue(ctx->evpl);
    }

    expect_a = make_id_principal(CHIMERA_PRINCIPAL_USER, 4600, sid_a);
    expect_b = make_id_principal(CHIMERA_PRINCIPAL_USER, 4601, sid_b);
    assert(ra.status == CHIMERA_VFS_OK && rb.status == CHIMERA_VFS_OK);
    assert(ra.acl.hdr.num_aces == 1);
    assert(principal_eq(&ra.acl.aces[0].who, &expect_a));
    assert(chimera_sid_equal(&ra.owner_sid, &osid_a));
    assert(rb.acl.hdr.num_aces == 2);
    assert(principal_eq(&rb.acl.aces[0].who, &expect_b));
    assert(chimera_sid_equal(&rb.owner_sid, &osid_b));

    chimera_vfs_release(ctx->vfs_thread, ha);
    chimera_vfs_release(ctx->vfs_thread, hb);
    TEST_PASS("enrich: a parked reply survives the module reusing its scratch");
} /* test_enrich_parked_scratch_reuse */

static void
test_enrich_unavailable_open_closes(struct test_ctx *ctx)
{
    const char                     *down = "S-1-5-21-40-41-42-4700";
    struct chimera_vfs_open_handle *dir, *h;
    struct chimera_acl             *acl = acl_buf_init(acl_in_buf, 2);
    struct chimera_vfs_cred         user;
    struct chimera_vfs_attrs        sattr;
    int                             closes_before;

    fixture_sid_add(down, CHIMERA_VFS_IDENTITY_UNAVAILABLE, 0, 0);
    chimera_vfs_cred_init_unix(&user, UID_ADMITTED, UID_ADMITTED, 0, NULL);

    assert(mount_status(ctx, "wcnt", "memcnt", "fs_wcnt", "windows_acl") ==
           CHIMERA_VFS_OK);
    open_mount_root(ctx, "wcnt");
    dir = ctx->handle;

    h = create_file(ctx, dir, "f");
    ace_set(&acl->aces[0], CHIMERA_ACE_DENIED, CHIMERA_ACE_READ_DATA, 0,
            make_sid_principal(down));
    ace_set(&acl->aces[1], CHIMERA_ACE_ALLOWED,
            CHIMERA_ACE_READ_DATA | CHIMERA_ACE_WRITE_DATA, 0,
            make_special_principal(CHIMERA_WHO_EVERYONE));
    set_acl(ctx, h, acl);
    chimera_vfs_release(ctx->vfs_thread, h);

    /* The backend opens the file, then the reply's ACL cannot be resolved:
     * the open fails EIO, and the backend open it made is closed rather than
     * stranded (no handle will ever wrap it). */
    memcnt_thread = ctx->vfs_thread;
    closes_before = memcnt_closes;
    memset(&sattr, 0, sizeof(sattr));
    chimera_vfs_open_at(ctx->vfs_thread, &user, dir, "f", 1,
                        CHIMERA_VFS_OPEN_READ_ONLY, &sattr,
                        CHIMERA_VFS_ATTR_FH, 0, 0, openat_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_EIO);
    assert(memcnt_closes == closes_before + 1);

    /* The same for create_unlinked (the S3 PUT path), here with a caller
     * that asks for the ACL.  memfs applies the create's ACL verbatim, so the
     * reply carries the unresolvable SID.  The failed create delivers no
     * handle -- none was cached, so nothing will close the backend open a
     * second time -- and the one it made is closed exactly once, now. */
    closes_before = memcnt_closes;
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_ACL;
    sattr.va_acl      = acl;
    chimera_vfs_create_unlinked(ctx->vfs_thread, &ctx->root_cred, dir->fh,
                                dir->fh_len, &sattr,
                                CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_ACL,
                                create_unlinked_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_EIO);
    assert(ctx->handle == NULL);
    assert(memcnt_closes == closes_before + 1);
    memcnt_thread = NULL;

    chimera_vfs_release(ctx->vfs_thread, dir);
    TEST_PASS("enrich: a failed open releases the backend open it made");
} /* test_enrich_unavailable_open_closes */

static void
test_enrich_off_without_flag(struct test_ctx *ctx)
{
    const char                     *sid = "S-1-5-21-40-41-42-4800";
    struct chimera_vfs_open_handle *dir, *h;
    struct chimera_acl             *acl = acl_buf_init(acl_in_buf, 1);
    struct chimera_principal        opaque;
    int                             calls_before, inline_done;

    fixture_sid_add(sid, CHIMERA_VFS_IDENTITY_OK, 0, 4800);

    /* The "plain" mount (memfs, no option) from test_mount_option_absent. */
    open_mount_root(ctx, "plain");
    dir = ctx->handle;
    h   = create_file(ctx, dir, "raw");

    opaque = make_sid_principal(sid);
    ace_set(&acl->aces[0], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA, 0, opaque);
    set_acl(ctx, h, acl);

    /* Whatever the module returns goes up verbatim: no resolve, no park. */
    calls_before = chimera_atomic_load_n(&fixture_sid_calls, CHIMERA_MEMORY_RELAXED);
    assert(get_acl(ctx, h, &inline_done) == CHIMERA_VFS_OK);
    assert(inline_done);
    assert(ctx->got_acl->num_aces == 1);
    assert(principal_eq(&ctx->got_acl->aces[0].who, &opaque));
    assert(chimera_atomic_load_n(&fixture_sid_calls, CHIMERA_MEMORY_RELAXED) ==
           calls_before);

    chimera_vfs_release(ctx->vfs_thread, h);
    chimera_vfs_release(ctx->vfs_thread, dir);
    TEST_PASS("enrich: a mount without the flag is left untouched");
} /* test_enrich_off_without_flag */

int
main(
    int    argc,
    char **argv)
{
    struct test_ctx               ctx = { 0 };
    struct chimera_vfs_module_cfg module_cfgs[4];

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

    /* So is memcnt, with nothing cleared: it only counts CLOSEs. */
    vfs_memcnt.capabilities   = vfs_memfs.capabilities;
    vfs_memcnt.init           = vfs_memfs.init;
    vfs_memcnt.destroy        = vfs_memfs.destroy;
    vfs_memcnt.thread_init    = vfs_memfs.thread_init;
    vfs_memcnt.thread_destroy = vfs_memfs.thread_destroy;

    ctx.metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(ctx.metrics != NULL);

    memset(module_cfgs, 0, sizeof(module_cfgs));
    strncpy(module_cfgs[0].module_name, "memfs", sizeof(module_cfgs[0].module_name) - 1);
    strncpy(module_cfgs[1].module_name, "noaclnat", sizeof(module_cfgs[1].module_name) - 1);
    strncpy(module_cfgs[2].module_name, "memcnt", sizeof(module_cfgs[2].module_name) - 1);
    strncpy(module_cfgs[3].module_name, "memkv", sizeof(module_cfgs[3].module_name) - 1);

    ctx.evpl = evpl_create(NULL);
    assert(ctx.evpl != NULL);

    ctx.vfs = chimera_vfs_init(0, 0, module_cfgs, 4, "memkv", 60, 1, 1, 0,
                               ctx.metrics);
    assert(ctx.vfs != NULL);

    ctx.vfs_thread = chimera_vfs_thread_init(ctx.evpl, ctx.vfs);
    assert(ctx.vfs_thread != NULL);

    chimera_vfs_identity_register_handler(ctx.vfs, fixture_sid_handler, NULL);

    make_fs(&ctx, "memfs", "fs_acc");
    make_fs(&ctx, "memfs", "fs_plain");
    make_fs(&ctx, "memfs", "fs_comb");
    make_fs(&ctx, "noaclnat", "fs_shim");
    make_fs(&ctx, "memfs", "fs_wacl");
    make_fs(&ctx, "memcnt", "fs_wcnt");

    test_mount_option_accepted(&ctx);
    test_mount_option_absent(&ctx);
    test_mount_option_combined(&ctx);
    test_mount_option_rejected_without_acl_native(&ctx);

    /* Read-path enrichment, on a memfs mount forced SID-only by the option. */
    ctx.got_acl = malloc(WACL_ACL_SIZE);
    assert(ctx.got_acl != NULL);
    assert(mount_status(&ctx, "wacl", "memfs", "fs_wacl", "windows_acl") ==
           CHIMERA_VFS_OK);
    open_mount_root(&ctx, "wacl");
    ctx.wdir = ctx.handle;

    test_enrich_getattr_native(&ctx);
    test_enrich_dedup(&ctx);
    test_enrich_many(&ctx);
    test_enrich_not_found_opaque(&ctx);
    test_enrich_unavailable_fails(&ctx);
    test_enrich_algorithmic_no_resolver(&ctx);
    test_enrich_create_reply(&ctx);
    test_open_gated_on_enriched_acl(&ctx);
    test_enrich_parked_scratch_reuse(&ctx);
    test_enrich_unavailable_open_closes(&ctx);
    test_enrich_off_without_flag(&ctx);

    chimera_vfs_release(ctx.vfs_thread, ctx.wdir);
    free(ctx.got_acl);

    chimera_vfs_thread_destroy(ctx.vfs_thread);
    chimera_vfs_destroy(ctx.vfs);
    prometheus_metrics_destroy(ctx.metrics);
    evpl_destroy(ctx.evpl);

    fprintf(stderr, "vfs_acl_windows_test: all tests passed\n");
    return 0;
} /* main */
