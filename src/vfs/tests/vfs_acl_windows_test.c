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
 * are not counted), so a test can see a backend open being released.  It also
 * keeps a copy of the last ACL an op's set_attr handed it, so a test can see
 * exactly what the module was asked to store. */
static struct chimera_vfs_thread      *memcnt_thread;
static int                             memcnt_closes;

#define MEMCNT_ACL_MAX 4

static struct {
    struct chimera_acl hdr;
    struct chimera_ace aces[MEMCNT_ACL_MAX];
} memcnt_acl;
static int                             memcnt_acl_seen;  /* ops that carried one */
/* ...and the set_attr and ACL pointers that op was handed. */
static const struct chimera_vfs_attrs *memcnt_set_attr_ptr;
static const struct chimera_acl       *memcnt_acl_ptr;

static struct chimera_vfs_attrs *
memcnt_set_attr(struct chimera_vfs_request *request)
{
    switch (request->opcode) {
        case CHIMERA_VFS_OP_SETATTR:
            return request->setattr.set_attr;
        case CHIMERA_VFS_OP_OPEN_AT:
            return request->open_at.set_attr;
        case CHIMERA_VFS_OP_MKDIR_AT:
            return request->mkdir_at.set_attr;
        case CHIMERA_VFS_OP_MKNOD_AT:
            return request->mknod_at.set_attr;
        case CHIMERA_VFS_OP_SYMLINK_AT:
            return request->symlink_at.set_attr;
        case CHIMERA_VFS_OP_CREATE_UNLINKED:
            return request->create_unlinked.set_attr;
        default:
            return NULL;
    } /* switch */
} /* memcnt_set_attr */

static void
memcnt_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    struct chimera_vfs_attrs *sa = memcnt_set_attr(request);

    if (request->opcode == CHIMERA_VFS_OP_CLOSE &&
        request->thread == memcnt_thread) {
        memcnt_closes++;
    }
    if (sa && (sa->va_set_mask & CHIMERA_VFS_ATTR_ACL) && sa->va_acl) {
        unsigned n = sa->va_acl->num_aces;

        memcnt_acl.hdr.num_aces   = sa->va_acl->num_aces;
        memcnt_acl.hdr.ctrl_flags = sa->va_acl->ctrl_flags;
        memcpy(memcnt_acl.aces, sa->va_acl->aces,
               (n < MEMCNT_ACL_MAX ? n : MEMCNT_ACL_MAX) * sizeof(struct chimera_ace));
        memcnt_set_attr_ptr = sa;
        memcnt_acl_ptr      = sa->va_acl;
        memcnt_acl_seen++;
    }
    vfs_memfs.dispatch(request, private_data);
} /* memcnt_dispatch */

SYMBOL_EXPORT struct chimera_vfs_module vfs_memcnt = {
    .sdk_version = CHIMERA_VFS_SDK_VERSION,
    .name        = "memcnt",
    .fh_magic    = CHIMERA_VFS_FH_MAGIC_VENDOR1,
    .dispatch    = memcnt_dispatch,
};

/* ---- the memblk shim ---------------------------------------------------- */

/* memfs in every respect, except that it declares READDIR blocking, so the
 * core bounces the entries through its collector (the module runs on a
 * delegation thread) and replays them to the caller from another thread, by
 * which time memfs's per-thread ACL scratch has moved on. */
SYMBOL_EXPORT struct chimera_vfs_module vfs_memblk = {
    .sdk_version  = CHIMERA_VFS_SDK_VERSION,
    .name         = "memblk",
    .fh_magic     = CHIMERA_VFS_FH_MAGIC_VENDOR2,
    .dispatch     = noaclnat_dispatch,
    .blocking_ops = CHIMERA_VFS_OP_BIT(CHIMERA_VFS_OP_READDIR),
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
    /* ...and that reply's uid/gid and owner/group SID companions (zero when
     * absent), and the set_attr pointer a setattr or create handed back. */
    uint64_t                        got_uid;
    uint64_t                        got_gid;
    struct chimera_sid              got_owner_sid;
    struct chimera_sid              got_group_sid;
    struct chimera_vfs_attrs       *got_set_attr;
    /* The windows_acl memcnt mount's root. */
    struct chimera_vfs_open_handle *wcnt;
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

    ctx->status       = error_code;
    ctx->got_set_attr = set_attr;
    ctx->done         = 1;
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

/*
 * The write direction, BY_UID / BY_GID, likewise from a table: each uid or
 * gid resolves to an identity carrying the native SID `sid`, is
 * authoritatively unknown, or finds the infrastructure down.  An id the
 * table does not name gets fixture_id_default -- PASS, so the NSS handler's
 * answer stands, unless a test takes the infrastructure down for every id.
 * The write path caches what it resolves (BY_UID positively, with the SID,
 * which the user cache also indexes BY_SID), so each test uses ids of its
 * own.
 */
#define FIX_ID_MAX 32

struct fixture_id {
    int                              is_group;
    uint32_t                         id;
    enum chimera_vfs_identity_status mode;
    char                             sid[CHIMERA_VFS_SID_MAX_LEN];
    int                              calls;
};

static struct fixture_id                fixture_ids[FIX_ID_MAX];
static int                              fixture_nids;
static int                              fixture_id_calls; /* every BY_UID/BY_GID consultation */
static enum chimera_vfs_identity_status fixture_id_default = CHIMERA_VFS_IDENTITY_PASS;

static struct fixture_id *
fixture_id_add(
    int                              is_group,
    uint32_t                         id,
    enum chimera_vfs_identity_status mode,
    const char                      *sid)
{
    struct fixture_id *f;

    assert(fixture_nids < FIX_ID_MAX);
    f = &fixture_ids[fixture_nids++];
    memset(f, 0, sizeof(*f));
    f->is_group = is_group;
    f->id       = id;
    f->mode     = mode;
    if (sid) {
        snprintf(f->sid, sizeof(f->sid), "%s", sid);
    }
    return f;
} /* fixture_id_add */

static int
fixture_id_calls_of(struct fixture_id *f)
{
    return chimera_atomic_load_n(&f->calls, CHIMERA_MEMORY_RELAXED);
} /* fixture_id_calls_of */

static int
fixture_id_calls_all(void)
{
    return chimera_atomic_load_n(&fixture_id_calls, CHIMERA_MEMORY_RELAXED);
} /* fixture_id_calls_all */

static enum chimera_vfs_identity_status
fixture_id_answer(
    int                                 is_group,
    uint32_t                            id,
    struct chimera_vfs_identity_result *out)
{
    chimera_atomic_fetch_add(&fixture_id_calls, 1, CHIMERA_MEMORY_RELAXED);

    for (int i = 0; i < fixture_nids; i++) {
        struct fixture_id *f = &fixture_ids[i];

        if (f->is_group != is_group || f->id != id) {
            continue;
        }

        chimera_atomic_fetch_add(&f->calls, 1, CHIMERA_MEMORY_RELAXED);

        if (f->mode == CHIMERA_VFS_IDENTITY_OK) {
            if (is_group) {
                out->is_group  = 1;
                out->group.gid = id;
                snprintf(out->group.groupname, sizeof(out->group.groupname),
                         "waclg%u", id);
                out->group.groupname_len = (int) strlen(out->group.groupname);
                snprintf(out->group.sid, sizeof(out->group.sid), "%s", f->sid);
            } else {
                out->user.uid = id;
                out->user.gid = id;
                snprintf(out->user.username, sizeof(out->user.username),
                         "waclu%u", id);
                out->user.username_len = (int) strlen(out->user.username);
                snprintf(out->user.sid, sizeof(out->user.sid), "%s", f->sid);
            }
        }
        return f->mode;
    }

    return fixture_id_default;
} /* fixture_id_answer */

static enum chimera_vfs_identity_status
fixture_sid_handler(
    enum chimera_vfs_identity_key       key,
    uint32_t                            id,
    const char                         *name,
    struct chimera_vfs_identity_result *out,
    void                               *private_data)
{
    (void) private_data;

    if (key == CHIMERA_VFS_IDENTITY_BY_UID || key == CHIMERA_VFS_IDENTITY_BY_GID) {
        return fixture_id_answer(key == CHIMERA_VFS_IDENTITY_BY_GID, id, out);
    }

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
    ctx->got_uid           = 0;
    ctx->got_gid           = 0;
    memset(&ctx->got_owner_sid, 0, sizeof(ctx->got_owner_sid));
    memset(&ctx->got_group_sid, 0, sizeof(ctx->got_group_sid));
    if (error_code == CHIMERA_VFS_OK && attr) {
        ctx->got_mask = attr->va_set_mask;
        if ((attr->va_set_mask & CHIMERA_VFS_ATTR_ACL) && attr->va_acl) {
            assert(attr->va_acl->num_aces <= WACL_MAX_ACES);
            memcpy(ctx->got_acl, attr->va_acl,
                   chimera_acl_size(attr->va_acl->num_aces));
        }
        if (attr->va_set_mask & CHIMERA_VFS_ATTR_UID) {
            ctx->got_uid = attr->va_uid;
        }
        if (attr->va_set_mask & CHIMERA_VFS_ATTR_GID) {
            ctx->got_gid = attr->va_gid;
        }
        if ((attr->va_set_mask & CHIMERA_VFS_ATTR_OWNER_SID) &&
            attr->va_owner_sid) {
            ctx->got_owner_sid = *attr->va_owner_sid;
        }
        if ((attr->va_set_mask & CHIMERA_VFS_ATTR_GROUP_SID) &&
            attr->va_group_sid) {
            ctx->got_group_sid = *attr->va_group_sid;
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

    ctx->status       = error_code;
    ctx->handle       = oh;
    ctx->got_set_attr = set_attr;
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
    ctx->got_set_attr = set_attr;
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

/* ---- write-path normalization ------------------------------------------- */

/* SETATTR `sattr` on `h` as `cred` and return the status; the set_attr the
 * completion handed back is left in ctx->got_set_attr.  *inline_done
 * reports whether it completed before chimera_vfs_setattr returned: a write
 * whose identities are all cached never parks. */
static enum chimera_vfs_error
setattr_status_inline(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *h,
    struct chimera_vfs_attrs       *sattr,
    int                            *inline_done)
{
    ctx->got_set_attr = NULL;
    chimera_vfs_setattr(ctx->vfs_thread, cred, h, sattr, 0, 0, setattr_cb, ctx);
    if (inline_done) {
        *inline_done = ctx->done;
    }
    wait_done(ctx);
    return ctx->status;
} /* setattr_status_inline */

static enum chimera_vfs_error
setattr_status(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *h,
    struct chimera_vfs_attrs       *sattr)
{
    return setattr_status_inline(ctx, cred, h, sattr, NULL);
} /* setattr_status */

/* Store `acl` on `h` as root and return the status. */
static enum chimera_vfs_error
set_acl_status_inline(
    struct test_ctx                *ctx,
    struct chimera_vfs_open_handle *h,
    struct chimera_acl             *acl,
    int                            *inline_done)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_ACL;
    sattr.va_acl      = acl;
    return setattr_status_inline(ctx, &ctx->root_cred, h, &sattr, inline_done);
} /* set_acl_status_inline */

static enum chimera_vfs_error
set_acl_status(
    struct test_ctx                *ctx,
    struct chimera_vfs_open_handle *h,
    struct chimera_acl             *acl)
{
    return set_acl_status_inline(ctx, h, acl, NULL);
} /* set_acl_status */

/* GETATTR h (as root) for the stat set and the SID companions. */
static enum chimera_vfs_error
get_owner(
    struct test_ctx                *ctx,
    struct chimera_vfs_open_handle *h)
{
    chimera_vfs_getattr(ctx->vfs_thread, &ctx->root_cred, h,
                        CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_OWNER_SID |
                        CHIMERA_VFS_ATTR_GROUP_SID,
                        getattr_acl_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* get_owner */

static struct chimera_sid
make_sid(const char *str)
{
    struct chimera_sid sid;

    assert(chimera_sid_from_str(&sid, str) == 0);
    return sid;
} /* make_sid */

static void
test_write_stores_sid_only(struct test_ctx *ctx)
{
    const char                     *sid = "S-1-5-21-60-61-62-6000";
    struct fixture_sid             *fs  = fixture_sid_add(sid, CHIMERA_VFS_IDENTITY_OK, 0, 6000);
    struct fixture_id              *fu  = fixture_id_add(0, 6000, CHIMERA_VFS_IDENTITY_OK, sid);
    struct chimera_vfs_open_handle *h   = create_file(ctx, ctx->wdir, "w_sidonly");
    struct chimera_acl             *acl = acl_buf_init(acl_in_buf, 1);
    struct chimera_principal        dual;

    /* uid 6000 <-> S, set with both halves of its identity. */
    dual = make_id_principal(CHIMERA_PRINCIPAL_USER, 6000, sid);
    ace_set(&acl->aces[0], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA, 0, dual);
    set_acl(ctx, h, acl);

    /* The principal came with its SID: there was nothing to resolve on the
     * way down, so neither direction of the identity has been asked about. */
    assert(fixture_id_calls_of(fu) == 0);
    assert(fixture_calls(fs) == 0);

    /* A dual-identity store would hand USER 6000 back without consulting the
     * resolver.  Stored SID-only, the read has to resolve S -- and while the
     * resolver is down it fails closed. */
    fs->mode = CHIMERA_VFS_IDENTITY_UNAVAILABLE;
    assert(get_acl(ctx, h, NULL) == CHIMERA_VFS_EIO);
    assert(fixture_calls(fs) == 1);

    fs->mode = CHIMERA_VFS_IDENTITY_OK;
    assert(get_acl(ctx, h, NULL) == CHIMERA_VFS_OK);
    assert(ctx->got_acl->num_aces == 1);
    assert(principal_eq(&ctx->got_acl->aces[0].who, &dual));
    assert(fixture_calls(fs) == 2);

    chimera_vfs_release(ctx->vfs_thread, h);
    TEST_PASS("write: a dual-identity principal is stored SID-only");
} /* test_write_stores_sid_only */

static void
test_write_numeric_algorithmic(struct test_ctx *ctx)
{
    struct fixture_id              *fu  = fixture_id_add(0, 6100, CHIMERA_VFS_IDENTITY_NOT_FOUND, NULL);
    struct chimera_vfs_open_handle *h   = create_file(ctx, ctx->wcnt, "w_numeric");
    struct chimera_acl             *acl = acl_buf_init(acl_in_buf, 1);
    struct chimera_principal        numeric, algo;
    int                             sid_calls, inline_done;

    numeric = make_id_principal(CHIMERA_PRINCIPAL_USER, 6100, NULL);
    algo    = make_sid_principal("S-1-5-88-1-6100");
    ace_set(&acl->aces[0], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA, 0, numeric);

    /* uid 6100 has no domain identity: the write still succeeds, once the
     * cold resolve has answered. */
    memcnt_acl_seen = 0;
    assert(set_acl_status_inline(ctx, h, acl, &inline_done) == CHIMERA_VFS_OK);
    assert(!inline_done);
    assert(fixture_id_calls_of(fu) == 1);

    /* The module was handed the lossless algorithmic SID, not the uid. */
    assert(memcnt_acl_seen == 1);
    assert(memcnt_acl.hdr.num_aces == 1);
    assert(principal_eq(&memcnt_acl.aces[0].who, &algo));

    /* The answer is cached negatively: the same write again is all inline. */
    memcnt_acl_seen = 0;
    assert(set_acl_status_inline(ctx, h, acl, &inline_done) == CHIMERA_VFS_OK);
    assert(inline_done);
    assert(fixture_id_calls_of(fu) == 1);
    assert(memcnt_acl_seen == 1);
    assert(principal_eq(&memcnt_acl.aces[0].who, &algo));

    /* It decodes back inline, without the resolver. */
    sid_calls = chimera_atomic_load_n(&fixture_sid_calls, CHIMERA_MEMORY_RELAXED);
    assert(get_acl(ctx, h, &inline_done) == CHIMERA_VFS_OK);
    assert(inline_done);
    assert(ctx->got_acl->num_aces == 1);
    assert(principal_eq(&ctx->got_acl->aces[0].who, &numeric));
    assert(chimera_atomic_load_n(&fixture_sid_calls, CHIMERA_MEMORY_RELAXED) ==
           sid_calls);

    chimera_vfs_release(ctx->vfs_thread, h);
    TEST_PASS("write: an unmappable uid round-trips through its algorithmic SID");
} /* test_write_numeric_algorithmic */

static void
test_write_unavailable_fails(struct test_ctx *ctx)
{
    struct fixture_id              *fu = fixture_id_add(0, 6200, CHIMERA_VFS_IDENTITY_UNAVAILABLE, NULL);
    struct chimera_vfs_open_handle *h  = create_file(ctx, ctx->wdir, "w_unavail");
    struct test_acl                 prev;
    struct chimera_acl             *acl;
    struct chimera_vfs_attrs        sattr;

    /* The ACL in place before the failed write. */
    acl = acl_buf_init(acl_in_buf, 2);
    ace_set(&acl->aces[0], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_MASK_ALL, 0,
            make_special_principal(CHIMERA_WHO_OWNER));
    ace_set(&acl->aces[1], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA, 0,
            make_special_principal(CHIMERA_WHO_EVERYONE));
    memcpy(&prev, acl, chimera_acl_size(2));
    set_acl(ctx, h, acl);

    /* uid 6200 cannot be resolved: the write fails closed rather than store
     * an algorithmic SID for a user who may really have a domain one. */
    acl = acl_buf_init(acl_in_buf, 1);
    ace_set(&acl->aces[0], CHIMERA_ACE_ALLOWED,
            CHIMERA_ACE_READ_DATA | CHIMERA_ACE_WRITE_DATA, 0,
            make_id_principal(CHIMERA_PRINCIPAL_USER, 6200, NULL));
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_ACL;
    sattr.va_acl      = acl;
    assert(setattr_status(ctx, &ctx->root_cred, h, &sattr) == CHIMERA_VFS_EIO);
    assert(fixture_id_calls_of(fu) == 1);
    /* Failed before dispatch, it still hands the caller its own set_attr. */
    assert(ctx->got_set_attr == &sattr);

    /* Nothing reached the module: the previous ACL is intact. */
    assert(get_acl(ctx, h, NULL) == CHIMERA_VFS_OK);
    assert(ctx->got_acl->num_aces == 2);
    assert(memcmp(ctx->got_acl->aces, prev.aces, 2 * sizeof(struct chimera_ace)) == 0);

    /* An outage is never cached: the next attempt asks again. */
    assert(set_acl_status(ctx, h, acl) == CHIMERA_VFS_EIO);
    assert(fixture_id_calls_of(fu) == 2);

    /* On a read-only windows_acl mount the write is refused EROFS, as on
     * any read-only mount, without the resolver being asked. */
    chimera_vfs_release(ctx->vfs_thread, h);
    open_mount_root(ctx, "comb");
    h = ctx->handle;
    assert(set_acl_status(ctx, h, acl) == CHIMERA_VFS_EROFS);
    assert(fixture_id_calls_of(fu) == 2);

    chimera_vfs_release(ctx->vfs_thread, h);
    TEST_PASS("write: an unavailable resolver fails the write with EIO");
} /* test_write_unavailable_fails */

static void
test_write_caller_buffer_const(struct test_ctx *ctx)
{
    const char                     *s_res = "S-1-5-21-60-61-62-6300";
    const char                     *s_own = "S-1-5-21-60-61-62-6301";
    struct chimera_vfs_open_handle *h     = create_file(ctx, ctx->wdir, "w_const");
    struct chimera_acl             *acl   = acl_buf_init(acl_in_buf, 3);
    struct chimera_vfs_attrs        sattr;
    struct test_acl                 snap;
    struct chimera_principal        e_res, e_own, e_grp;

    fixture_id_add(0, 6300, CHIMERA_VFS_IDENTITY_OK, s_res);
    fixture_sid_add(s_own, CHIMERA_VFS_IDENTITY_OK, 0, 6301);
    fixture_id_add(1, 6302, CHIMERA_VFS_IDENTITY_NOT_FOUND, NULL);

    /* Every kind of principal the normalization rewrites: one it resolves,
     * one it strips to its SID, one it encodes algorithmically. */
    ace_set(&acl->aces[0], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA, 0,
            make_id_principal(CHIMERA_PRINCIPAL_USER, 6300, NULL));
    ace_set(&acl->aces[1], CHIMERA_ACE_DENIED, CHIMERA_ACE_WRITE_DATA, 0,
            make_id_principal(CHIMERA_PRINCIPAL_USER, 6301, s_own));
    ace_set(&acl->aces[2], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA,
            CHIMERA_ACE_FLAG_IDENTIFIER_GROUP,
            make_id_principal(CHIMERA_PRINCIPAL_GROUP, 6302, NULL));
    memcpy(&snap, acl, chimera_acl_size(3));

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_ACL;
    sattr.va_acl      = acl;
    assert(setattr_status(ctx, &ctx->root_cred, h, &sattr) == CHIMERA_VFS_OK);

    /* The caller's buffer (the posix setacl path hands the core a const one)
     * is exactly as it was, and its own set_attr is what came back. */
    assert(memcmp(acl, &snap, chimera_acl_size(3)) == 0);
    assert(sattr.va_acl == acl);
    assert(ctx->got_set_attr == &sattr);

    /* What was stored is the normalized form, enriched back on the read. */
    e_res = make_id_principal(CHIMERA_PRINCIPAL_USER, 6300, s_res);
    e_own = make_id_principal(CHIMERA_PRINCIPAL_USER, 6301, s_own);
    e_grp = make_id_principal(CHIMERA_PRINCIPAL_GROUP, 6302, NULL);
    assert(get_acl(ctx, h, NULL) == CHIMERA_VFS_OK);
    assert(ctx->got_acl->num_aces == 3);
    assert(principal_eq(&ctx->got_acl->aces[0].who, &e_res));
    assert(principal_eq(&ctx->got_acl->aces[1].who, &e_own));
    assert(principal_eq(&ctx->got_acl->aces[2].who, &e_grp));

    chimera_vfs_release(ctx->vfs_thread, h);
    TEST_PASS("write: the caller's ACL buffer is never written");
} /* test_write_caller_buffer_const */

static void
test_chown_attaches_companion(struct test_ctx *ctx)
{
    const char                     *s_user  = "S-1-5-21-60-61-62-6400";
    const char                     *s_group = "S-1-5-21-60-61-62-6401";
    struct fixture_id              *fu      = fixture_id_add(0, 6400, CHIMERA_VFS_IDENTITY_OK, s_user);
    struct fixture_id              *fg      = fixture_id_add(1, 6401, CHIMERA_VFS_IDENTITY_OK, s_group);
    struct fixture_id              *fn      = fixture_id_add(0, 6402, CHIMERA_VFS_IDENTITY_NOT_FOUND, NULL);
    struct chimera_vfs_open_handle *h       = create_file(ctx, ctx->wdir, "w_chown");
    struct chimera_vfs_attrs        sattr;
    struct chimera_sid              e_user  = make_sid(s_user);
    struct chimera_sid              e_group = make_sid(s_group);

    /* A chown naming the ids alone. */
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID;
    sattr.va_uid      = 6400;
    sattr.va_gid      = 6401;
    assert(setattr_status(ctx, &ctx->root_cred, h, &sattr) == CHIMERA_VFS_OK);
    assert(fixture_id_calls_of(fu) == 1 && fixture_id_calls_of(fg) == 1);

    /* The engine attached the SIDs the ids resolve to (on its own copy). */
    assert(sattr.va_owner_sid == NULL && sattr.va_group_sid == NULL);
    assert(get_owner(ctx, h) == CHIMERA_VFS_OK);
    assert(ctx->got_uid == 6400 && ctx->got_gid == 6401);
    assert(chimera_sid_equal(&ctx->got_owner_sid, &e_user));
    assert(chimera_sid_equal(&ctx->got_group_sid, &e_group));

    /* A chown to a uid with no domain identity attaches nothing: the stored
     * owner SID no longer describes the owner and is cleared, as on any
     * mount.  (A cold resolve: the request parks and is dispatched once,
     * still handing back the caller's own set_attr.) */
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_UID;
    sattr.va_uid      = 6402;
    assert(setattr_status(ctx, &ctx->root_cred, h, &sattr) == CHIMERA_VFS_OK);
    assert(fixture_id_calls_of(fn) == 1);
    assert(ctx->got_set_attr == &sattr);
    assert(get_owner(ctx, h) == CHIMERA_VFS_OK);
    assert(ctx->got_uid == 6402);
    assert(!(ctx->got_mask & CHIMERA_VFS_ATTR_OWNER_SID));
    assert(chimera_sid_equal(&ctx->got_group_sid, &e_group));

    chimera_vfs_release(ctx->vfs_thread, h);
    TEST_PASS("write: a chown gets the owner and group SID companions");
} /* test_chown_attaches_companion */

static void
test_chown_stale_companion_no_eperm(struct test_ctx *ctx)
{
    const char                     *s_old = "S-1-5-21-60-61-62-6500";
    const char                     *s_new = "S-1-5-21-60-61-62-6501";
    struct fixture_id              *fu    = fixture_id_add(0, 6500, CHIMERA_VFS_IDENTITY_OK, s_old);
    struct chimera_vfs_open_handle *h     = create_file(ctx, ctx->wdir, "w_stale");
    struct chimera_vfs_attrs        sattr;
    struct chimera_vfs_cred         owner;
    struct chimera_sid              sid_old = make_sid(s_old);
    struct chimera_sid              sid_new = make_sid(s_new);

    chimera_vfs_cred_init_unix(&owner, 6500, 6500, 0, NULL);

    /* Root gives the file to uid 6500 with owner SID A, stated outright:
     * nothing is resolved, so the identity stays cold. */
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask  = CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_OWNER_SID;
    sattr.va_uid       = 6500;
    sattr.va_owner_sid = &sid_old;
    assert(setattr_status(ctx, &ctx->root_cred, h, &sattr) == CHIMERA_VFS_OK);
    assert(fixture_id_calls_of(fu) == 0);
    assert(get_owner(ctx, h) == CHIMERA_VFS_OK);
    assert(chimera_sid_equal(&ctx->got_owner_sid, &sid_old));

    /* The directory has since remapped uid 6500 to SID B. */
    snprintf(fu->sid, sizeof(fu->sid), "%s", s_new);

    /* A client naming B itself is replacing the stored owner SID: the gate's
     * owner-SID discipline refuses that to a non-root owner. */
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask  = CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_OWNER_SID;
    sattr.va_uid       = 6500;
    sattr.va_owner_sid = &sid_new;
    assert(setattr_status(ctx, &owner, h, &sattr) == CHIMERA_VFS_EPERM);

    /* The owner restating its uid alone passes the gate as sent; only then
     * does the engine attach the uid's current SID, so the stale companion
     * is refreshed instead of turning the chown into an EPERM. */
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_UID;
    sattr.va_uid      = 6500;
    assert(setattr_status(ctx, &owner, h, &sattr) == CHIMERA_VFS_OK);
    assert(fixture_id_calls_of(fu) == 1);
    assert(get_owner(ctx, h) == CHIMERA_VFS_OK);
    assert(ctx->got_uid == 6500);
    assert(chimera_sid_equal(&ctx->got_owner_sid, &sid_new));

    chimera_vfs_release(ctx->vfs_thread, h);
    TEST_PASS("write: a stale owner SID is refreshed after the gate, no EPERM");
} /* test_chown_stale_companion_no_eperm */

static void
test_specials_untouched(struct test_ctx *ctx)
{
    struct chimera_vfs_open_handle *h   = create_file(ctx, ctx->wcnt, "w_specials");
    struct chimera_acl             *acl = acl_buf_init(acl_in_buf, 3);
    struct chimera_vfs_attrs        sattr;
    struct test_acl                 snap;
    int                             id_calls, inline_done;

    ace_set(&acl->aces[0], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_MASK_ALL, 0,
            make_special_principal(CHIMERA_WHO_OWNER));
    ace_set(&acl->aces[1], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA,
            CHIMERA_ACE_FLAG_IDENTIFIER_GROUP,
            make_special_principal(CHIMERA_WHO_GROUP));
    ace_set(&acl->aces[2], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA, 0,
            make_special_principal(CHIMERA_WHO_EVERYONE));
    memcpy(&snap, acl, chimera_acl_size(3));

    /* With every uid and gid unresolvable, an ACL that names none of them
     * needs nothing from the resolver and is stored as given. */
    fixture_id_default = CHIMERA_VFS_IDENTITY_UNAVAILABLE;
    id_calls           = fixture_id_calls_all();
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_ACL;
    sattr.va_acl      = acl;
    memcnt_acl_seen   = 0;
    assert(setattr_status_inline(ctx, &ctx->root_cred, h, &sattr, &inline_done) ==
           CHIMERA_VFS_OK);
    assert(inline_done);
    assert(fixture_id_calls_all() == id_calls);
    fixture_id_default = CHIMERA_VFS_IDENTITY_PASS;

    /* Nothing to resolve or strip: the op went down untouched, carrying the
     * caller's own set_attr and ACL rather than a private copy. */
    assert(memcnt_acl_seen == 1);
    assert(memcnt_set_attr_ptr == &sattr && memcnt_acl_ptr == acl);

    assert(get_acl(ctx, h, &inline_done) == CHIMERA_VFS_OK);
    assert(inline_done);
    assert(ctx->got_acl->num_aces == 3);
    assert(memcmp(ctx->got_acl->aces, snap.aces, 3 * sizeof(struct chimera_ace)) == 0);

    chimera_vfs_release(ctx->vfs_thread, h);
    TEST_PASS("write: an ACL of special principals needs no resolution");
} /* test_specials_untouched */

/*
 * The creates, through open_at's own set_attr swap: a CREATE carrying a size
 * defers it (vfs_proc_open_at.c), so the module already sees a private copy
 * of the caller's set_attr before normalization copies that again.  Both
 * swaps must unwind to the caller's own buffer, and an unresolvable uid must
 * fail the create before the module is asked to make anything.
 */
static void
test_write_create_paths(struct test_ctx *ctx)
{
    const char              *s_res = "S-1-5-21-60-61-62-6600";
    struct fixture_id       *fu    = fixture_id_add(0, 6600, CHIMERA_VFS_IDENTITY_OK, s_res);
    struct fixture_id       *fd    = fixture_id_add(0, 6601, CHIMERA_VFS_IDENTITY_UNAVAILABLE, NULL);
    struct chimera_acl      *acl   = acl_buf_init(acl_in_buf, 3);
    struct chimera_vfs_attrs sattr;
    struct test_acl          snap;
    struct chimera_principal native, owner;
    struct chimera_sid       sid_res = make_sid(s_res);
    uint64_t                 mask;
    int                      inline_done;

    native = make_sid_principal(s_res);
    owner  = make_special_principal(CHIMERA_WHO_OWNER);
    ace_set(&acl->aces[0], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA, 0,
            make_id_principal(CHIMERA_PRINCIPAL_USER, 6600, NULL));
    ace_set(&acl->aces[1], CHIMERA_ACE_DENIED, CHIMERA_ACE_WRITE_DATA, 0,
            make_id_principal(CHIMERA_PRINCIPAL_USER, 6600, NULL));
    ace_set(&acl->aces[2], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_MASK_ALL, 0, owner);
    memcpy(&snap, acl, chimera_acl_size(3));

    /* open_at CREATE with a size: the deferred-truncate copy and the
     * normalized copy, one on top of the other.  uid 6600 is named by two
     * ACEs and by the create's own owner: one resolve answers all three. */
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_SIZE |
        CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_ACL;
    sattr.va_mode   = 0644;
    sattr.va_size   = 0;
    sattr.va_uid    = 6600;
    sattr.va_acl    = acl;
    mask            = sattr.va_set_mask;
    memcnt_acl_seen = 0;
    chimera_vfs_open_at(ctx->vfs_thread, &ctx->root_cred, ctx->wcnt, "w_create", 8,
                        CHIMERA_VFS_OPEN_CREATE, &sattr, CHIMERA_VFS_ATTR_FH,
                        0, 0, openat_cb, ctx);
    inline_done = ctx->done;
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    assert(!inline_done);
    assert(fixture_id_calls_of(fu) == 1);
    assert(memcnt_acl_seen == 1);
    assert(memcnt_acl.hdr.num_aces == 3);
    assert(principal_eq(&memcnt_acl.aces[0].who, &native));
    assert(principal_eq(&memcnt_acl.aces[1].who, &native));
    assert(principal_eq(&memcnt_acl.aces[2].who, &owner));
    assert(ctx->got_set_attr == &sattr);
    assert(sattr.va_set_mask == mask && sattr.va_acl == acl);
    assert(sattr.va_owner_sid == NULL);
    assert(memcmp(acl, &snap, chimera_acl_size(3)) == 0);
    assert(get_owner(ctx, ctx->handle) == CHIMERA_VFS_OK);
    assert(ctx->got_uid == 6600);
    assert(chimera_sid_equal(&ctx->got_owner_sid, &sid_res));
    chimera_vfs_release(ctx->vfs_thread, ctx->handle);

    /* mkdir_at: the identity is warm now, so this one never parks. */
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_ACL;
    sattr.va_mode     = 0755;
    sattr.va_acl      = acl;
    memcnt_acl_seen   = 0;
    chimera_vfs_mkdir_at(ctx->vfs_thread, &ctx->root_cred, ctx->wcnt, "w_mkdir", 7,
                         &sattr, CHIMERA_VFS_ATTR_FH, 0, 0, mkdir_acl_cb, ctx);
    inline_done = ctx->done;
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    assert(inline_done);
    assert(fixture_id_calls_of(fu) == 1);
    assert(memcnt_acl_seen == 1);
    assert(principal_eq(&memcnt_acl.aces[0].who, &native));
    assert(principal_eq(&memcnt_acl.aces[1].who, &native));
    assert(ctx->got_set_attr == &sattr);
    assert(memcmp(acl, &snap, chimera_acl_size(3)) == 0);

    /* The same CREATE-with-size on the now existing file: the open is
     * normalized like any other, and the deferred truncate it then issues
     * reuses open_at's private attrs for a size-only fsetattr of its own --
     * after both swaps have already handed the caller's buffer back. */
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_SIZE |
        CHIMERA_VFS_ATTR_ACL;
    sattr.va_mode   = 0644;
    sattr.va_size   = 0;
    sattr.va_acl    = acl;
    memcnt_acl_seen = 0;
    chimera_vfs_open_at(ctx->vfs_thread, &ctx->root_cred, ctx->wcnt, "w_create", 8,
                        CHIMERA_VFS_OPEN_CREATE, &sattr, CHIMERA_VFS_ATTR_FH,
                        0, 0, openat_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    assert(memcnt_acl_seen == 1);
    assert(principal_eq(&memcnt_acl.aces[0].who, &native));
    assert(ctx->got_set_attr == &sattr);
    assert(sattr.va_set_mask == (mask & ~(uint64_t) CHIMERA_VFS_ATTR_UID) &&
           sattr.va_acl == acl);
    assert(memcmp(acl, &snap, chimera_acl_size(3)) == 0);
    chimera_vfs_release(ctx->vfs_thread, ctx->handle);

    /* An unresolvable uid fails the create before it is dispatched: the
     * module never sees it and the name is not created. */
    acl = acl_buf_init(acl_in_buf, 1);
    ace_set(&acl->aces[0], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA, 0,
            make_id_principal(CHIMERA_PRINCIPAL_USER, 6601, NULL));
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_SIZE |
        CHIMERA_VFS_ATTR_ACL;
    sattr.va_mode   = 0644;
    sattr.va_acl    = acl;
    memcnt_acl_seen = 0;
    chimera_vfs_open_at(ctx->vfs_thread, &ctx->root_cred, ctx->wcnt, "w_nocreate", 10,
                        CHIMERA_VFS_OPEN_CREATE, &sattr, CHIMERA_VFS_ATTR_FH,
                        0, 0, openat_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_EIO);
    assert(fixture_id_calls_of(fd) == 1);
    assert(memcnt_acl_seen == 0);
    assert(ctx->got_set_attr == &sattr);

    chimera_vfs_lookup(ctx->vfs_thread, &ctx->root_cred, ctx->wcnt->fh,
                       ctx->wcnt->fh_len, "w_nocreate", 10,
                       CHIMERA_VFS_ATTR_FH, 0, lookup_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_ENOENT);

    TEST_PASS("write: creates normalize through open_at's own set_attr swap");
} /* test_write_create_paths */

/* ---- readdir: cache-only enrichment ------------------------------------- */

#define RD_MAX 16

struct rd_entry {
    char     name[64];
    uint64_t mask;
    int      has_acl;
    uint8_t  acl_buf[sizeof(struct chimera_acl) + 4 * sizeof(struct chimera_ace)];
};

struct rd_result {
    struct rd_entry entries[RD_MAX];
    int             n;
    int             done;
    enum chimera_vfs_error status;
};

static int
rd_entry_cb(
    uint64_t                        inum,
    uint64_t                        cookie,
    const char                     *name,
    int                             namelen,
    const struct chimera_vfs_attrs *attrs,
    void                           *arg)
{
    struct rd_result *r = arg;
    struct rd_entry  *e;

    assert(r->n < RD_MAX);
    e = &r->entries[r->n++];
    assert(namelen < (int) sizeof(e->name));
    memcpy(e->name, name, namelen);
    e->name[namelen] = '\0';
    e->mask          = attrs->va_set_mask;
    e->has_acl       = 0;
    if ((attrs->va_set_mask & CHIMERA_VFS_ATTR_ACL) && attrs->va_acl) {
        assert(attrs->va_acl->num_aces <= 4);
        memcpy(e->acl_buf, attrs->va_acl,
               chimera_acl_size(attrs->va_acl->num_aces));
        e->has_acl = 1;
    }
    return 0;
} /* rd_entry_cb */

static void
rd_complete_cb(
    enum chimera_vfs_error          error_code,
    struct chimera_vfs_open_handle *handle,
    uint64_t                        cookie,
    uint64_t                        verifier,
    uint32_t                        eof,
    struct chimera_vfs_attrs       *attr,
    void                           *private_data)
{
    struct rd_result *r = private_data;

    r->status = error_code;
    r->done   = 1;
} /* rd_complete_cb */

/* READDIR `dir` (as root) asking for the ACL; the whole pass completes
 * synchronously for memfs. */
static void
rd_run(
    struct test_ctx                *ctx,
    struct chimera_vfs_open_handle *dir,
    struct rd_result               *r)
{
    memset(r, 0, sizeof(*r));
    chimera_vfs_readdir(ctx->vfs_thread, &ctx->root_cred, dir,
                        CHIMERA_VFS_ATTR_MASK_STAT | CHIMERA_VFS_ATTR_ACL, 0,
                        0, 0, 0, NULL, 0, rd_entry_cb, rd_complete_cb, r);
    while (!r->done) {
        evpl_continue(ctx->evpl);
    }
    assert(r->status == CHIMERA_VFS_OK);
} /* rd_run */

static struct rd_entry *
rd_find(
    struct rd_result *r,
    const char       *name)
{
    for (int i = 0; i < r->n; i++) {
        if (strcmp(r->entries[i].name, name) == 0) {
            return &r->entries[i];
        }
    }
    return NULL;
} /* rd_find */

/* A fresh directory under the windows_acl mount root, opened. */
static struct chimera_vfs_open_handle *
rd_make_dir(
    struct test_ctx *ctx,
    const char      *name)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0755;
    chimera_vfs_mkdir_at(ctx->vfs_thread, &ctx->root_cred, ctx->wdir, name,
                         (int) strlen(name), &sattr, CHIMERA_VFS_ATTR_FH, 0, 0,
                         mkdir_acl_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    chimera_vfs_open_fh(ctx->vfs_thread, &ctx->root_cred, ctx->fh, ctx->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    return ctx->handle;
} /* rd_make_dir */

/* Create `name` in `dir` carrying a one-ACE ACL naming `sid`. */
static void
rd_make_file_sid(
    struct test_ctx                *ctx,
    struct chimera_vfs_open_handle *dir,
    const char                     *name,
    const char                     *sid)
{
    struct chimera_vfs_open_handle *h   = create_file(ctx, dir, name);
    struct chimera_acl             *acl = acl_buf_init(acl_in_buf, 1);

    ace_set(&acl->aces[0], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA, 0,
            make_sid_principal(sid));
    set_acl(ctx, h, acl);
    chimera_vfs_release(ctx->vfs_thread, h);
} /* rd_make_file_sid */

static void
test_readdir_cold_withholds_acl(struct test_ctx *ctx)
{
    const char                     *sid = "S-1-5-21-70-71-72-7001";
    struct fixture_sid             *f   = fixture_sid_add(sid, CHIMERA_VFS_IDENTITY_OK, 0, 7001);
    struct chimera_vfs_open_handle *dir = rd_make_dir(ctx, "rdcold");
    struct rd_result                r;
    struct rd_entry                *a, *b;
    struct chimera_principal        expect;
    int                             withheld;

    rd_make_file_sid(ctx, dir, "a", sid);
    rd_make_file_sid(ctx, dir, "b", sid);

    /* Cold cache: the entry listed first is delivered with its ACL
     * withheld.  The warm it fires may land before the other entry of the
     * same pass is reached, so that one is either withheld the same way or
     * already enriched; what it never is, is an ACL left partly resolved. */
    assert(!chimera_vfs_identity_cached(ctx->vfs, CHIMERA_VFS_IDENTITY_BY_SID,
                                        0, sid));
    rd_run(ctx, dir, &r);
    a = rd_find(&r, "a");
    b = rd_find(&r, "b");
    assert(a && b);
    assert(a->mask & CHIMERA_VFS_ATTR_MODE);
    expect   = make_id_principal(CHIMERA_PRINCIPAL_USER, 7001, sid);
    withheld = 0;
    for (struct rd_entry *e = a; e; e = (e == a) ? b : NULL) {
        if (e->has_acl) {
            assert(e->mask & CHIMERA_VFS_ATTR_ACL);
            assert(principal_eq(&((struct chimera_acl *) e->acl_buf)->aces[0].who,
                                &expect));
        } else {
            assert(!(e->mask & CHIMERA_VFS_ATTR_ACL));
            assert(e->mask & CHIMERA_VFS_ATTR_ACL_WITHHELD);
            withheld++;
        }
    }
    assert(withheld >= 1);

    /* The fire-and-forget warm resolves the SID in the background; wait
     * until its result is in the cache (the handler's call counter moves
     * before the worker publishes, so it is not a safe signal). */
    for (int i = 0; i < 2000 &&
         !chimera_vfs_identity_cached(ctx->vfs, CHIMERA_VFS_IDENTITY_BY_SID,
                                      0, sid); i++) {
        evpl_continue(ctx->evpl);
        usleep(1000);
    }
    assert(chimera_vfs_identity_cached(ctx->vfs, CHIMERA_VFS_IDENTITY_BY_SID,
                                       0, sid));

    /* Once the warm has landed in the cache, the same readdir enriches. */
    rd_run(ctx, dir, &r);
    a = rd_find(&r, "a");
    assert(a && a->has_acl);
    assert(principal_eq(&((struct chimera_acl *) a->acl_buf)->aces[0].who,
                        &expect));
    assert(fixture_calls(f) >= 1);

    chimera_vfs_release(ctx->vfs_thread, dir);
    TEST_PASS("readdir: a cold native SID withholds the ACL, one warm per SID");
} /* test_readdir_cold_withholds_acl */

static void
test_readdir_warm_enriches(struct test_ctx *ctx)
{
    const char                     *sid = "S-1-5-21-70-71-72-7002";
    struct fixture_sid             *f   = fixture_sid_add(sid, CHIMERA_VFS_IDENTITY_OK, 0, 7002);
    struct chimera_vfs_open_handle *dir = rd_make_dir(ctx, "rdwarm");
    struct chimera_vfs_open_handle *h;
    struct rd_result                r;
    struct rd_entry                *e;
    struct chimera_principal        expect;
    struct chimera_acl             *acl;

    rd_make_file_sid(ctx, dir, "w", sid);

    /* A getattr parks, resolves and warms the cache deterministically. */
    h = create_file(ctx, dir, "w");
    assert(get_acl(ctx, h, NULL) == CHIMERA_VFS_OK);
    assert(fixture_calls(f) == 1);
    chimera_vfs_release(ctx->vfs_thread, h);

    rd_run(ctx, dir, &r);
    e = rd_find(&r, "w");
    assert(e && e->has_acl && (e->mask & CHIMERA_VFS_ATTR_ACL));
    acl    = (struct chimera_acl *) e->acl_buf;
    expect = make_id_principal(CHIMERA_PRINCIPAL_USER, 7002, sid);
    assert(acl->num_aces == 1);
    assert(principal_eq(&acl->aces[0].who, &expect));
    assert(!(acl->aces[0].flags & CHIMERA_ACE_FLAG_IDENTIFIER_GROUP));
    assert(fixture_calls(f) == 1);

    chimera_vfs_release(ctx->vfs_thread, dir);
    TEST_PASS("readdir: a warm native SID enriches the entry's ACL");
} /* test_readdir_warm_enriches */

static void
test_readdir_algorithmic_passes(struct test_ctx *ctx)
{
    struct chimera_vfs_open_handle *dir = rd_make_dir(ctx, "rdalgo");
    struct chimera_vfs_open_handle *h   = create_file(ctx, dir, "g");
    struct chimera_acl             *acl = acl_buf_init(acl_in_buf, 2);
    struct rd_result                r;
    struct rd_entry                *e;
    struct chimera_principal        eu, eg;
    int                             calls = chimera_atomic_load_n(&fixture_sid_calls,
                                                                  CHIMERA_MEMORY_RELAXED);

    ace_set(&acl->aces[0], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA, 0,
            make_id_principal(CHIMERA_PRINCIPAL_USER, 7003, NULL));
    ace_set(&acl->aces[1], CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA,
            CHIMERA_ACE_FLAG_IDENTIFIER_GROUP,
            make_id_principal(CHIMERA_PRINCIPAL_GROUP, 7004, NULL));
    (void) chimera_vfs_acl_windows_strip(acl);
    set_acl(ctx, h, acl);
    chimera_vfs_release(ctx->vfs_thread, h);

    /* Cold, with no native SID to ask about: enriched inline. */
    rd_run(ctx, dir, &r);
    e = rd_find(&r, "g");
    assert(e && e->has_acl);
    acl = (struct chimera_acl *) e->acl_buf;
    eu  = make_id_principal(CHIMERA_PRINCIPAL_USER, 7003, NULL);
    eg  = make_id_principal(CHIMERA_PRINCIPAL_GROUP, 7004, NULL);
    assert(acl->num_aces == 2);
    assert(principal_eq(&acl->aces[0].who, &eu));
    assert(principal_eq(&acl->aces[1].who, &eg));
    assert(acl->aces[1].flags & CHIMERA_ACE_FLAG_IDENTIFIER_GROUP);
    assert(chimera_atomic_load_n(&fixture_sid_calls,
                                 CHIMERA_MEMORY_RELAXED) == calls);

    chimera_vfs_release(ctx->vfs_thread, dir);
    TEST_PASS("readdir: algorithmic-only ACLs enrich inline even cold");
} /* test_readdir_algorithmic_passes */

/* The blocking-bounce branch: the entries are collected on a delegation
 * thread and replayed later, so each must carry its own ACL by value, not a
 * pointer into the module's scratch (which holds the last entry's by then). */
static void
test_readdir_bounce_own_acl(struct test_ctx *ctx)
{
    const char                     *sid1 = "S-1-5-21-70-71-72-7101";
    const char                     *sid2 = "S-1-5-21-70-71-72-7102";
    struct fixture_sid             *f1   = fixture_sid_add(sid1, CHIMERA_VFS_IDENTITY_OK, 0, 7101);
    struct fixture_sid             *f2   = fixture_sid_add(sid2, CHIMERA_VFS_IDENTITY_OK, 0, 7102);
    struct chimera_vfs_open_handle *dir  = rd_make_dir(ctx, "rdbounce");
    struct chimera_vfs_open_handle *h;
    struct rd_result                r;
    struct rd_entry                *a, *b;
    struct chimera_principal        e1, e2;

    rd_make_file_sid(ctx, dir, "a", sid1);
    rd_make_file_sid(ctx, dir, "b", sid2);

    /* Warm both SIDs deterministically through getattr. */
    h = create_file(ctx, dir, "a");
    assert(get_acl(ctx, h, NULL) == CHIMERA_VFS_OK);
    chimera_vfs_release(ctx->vfs_thread, h);
    h = create_file(ctx, dir, "b");
    assert(get_acl(ctx, h, NULL) == CHIMERA_VFS_OK);
    chimera_vfs_release(ctx->vfs_thread, h);
    assert(fixture_calls(f1) == 1 && fixture_calls(f2) == 1);

    rd_run(ctx, dir, &r);
    a  = rd_find(&r, "a");
    b  = rd_find(&r, "b");
    e1 = make_id_principal(CHIMERA_PRINCIPAL_USER, 7101, sid1);
    e2 = make_id_principal(CHIMERA_PRINCIPAL_USER, 7102, sid2);
    assert(a && b && a->has_acl && b->has_acl);
    assert(principal_eq(&((struct chimera_acl *) a->acl_buf)->aces[0].who, &e1));
    assert(principal_eq(&((struct chimera_acl *) b->acl_buf)->aces[0].who, &e2));

    chimera_vfs_release(ctx->vfs_thread, dir);
    TEST_PASS("readdir: bounced entries each carry their own ACL");
} /* test_readdir_bounce_own_acl */

int
main(
    int    argc,
    char **argv)
{
    struct test_ctx               ctx = { 0 };
    struct chimera_vfs_module_cfg module_cfgs[5];

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

    /* And memblk: memfs with READDIR declared blocking. */
    vfs_memblk.capabilities   = vfs_memfs.capabilities;
    vfs_memblk.init           = vfs_memfs.init;
    vfs_memblk.destroy        = vfs_memfs.destroy;
    vfs_memblk.thread_init    = vfs_memfs.thread_init;
    vfs_memblk.thread_destroy = vfs_memfs.thread_destroy;

    ctx.metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(ctx.metrics != NULL);

    memset(module_cfgs, 0, sizeof(module_cfgs));
    strncpy(module_cfgs[0].module_name, "memfs", sizeof(module_cfgs[0].module_name) - 1);
    strncpy(module_cfgs[1].module_name, "noaclnat", sizeof(module_cfgs[1].module_name) - 1);
    strncpy(module_cfgs[2].module_name, "memcnt", sizeof(module_cfgs[2].module_name) - 1);
    strncpy(module_cfgs[3].module_name, "memkv", sizeof(module_cfgs[3].module_name) - 1);
    strncpy(module_cfgs[4].module_name, "memblk", sizeof(module_cfgs[4].module_name) - 1);

    ctx.evpl = evpl_create(NULL);
    assert(ctx.evpl != NULL);

    ctx.vfs = chimera_vfs_init(1, 0, module_cfgs, 5, "memkv", 60, 1, 1, 0,
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
    make_fs(&ctx, "memblk", "fs_wblk");

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

    test_readdir_cold_withholds_acl(&ctx);
    test_readdir_warm_enriches(&ctx);
    test_readdir_algorithmic_passes(&ctx);

    /* The blocking-bounce readdir branch, on a mount of its own. */
    assert(mount_status(&ctx, "wblk", "memblk", "fs_wblk", "windows_acl") ==
           CHIMERA_VFS_OK);
    open_mount_root(&ctx, "wblk");
    {
        struct chimera_vfs_open_handle *saved = ctx.wdir;

        ctx.wdir = ctx.handle;
        test_readdir_bounce_own_acl(&ctx);
        chimera_vfs_release(ctx.vfs_thread, ctx.wdir);
        ctx.wdir = saved;
    }

    /* Write-path normalization, on the same flagged mounts; the memcnt one
     * (mounted by test_enrich_unavailable_open_closes) also shows what the
     * module was handed. */
    open_mount_root(&ctx, "wcnt");
    ctx.wcnt = ctx.handle;

    test_write_stores_sid_only(&ctx);
    test_write_numeric_algorithmic(&ctx);
    test_write_unavailable_fails(&ctx);
    test_write_caller_buffer_const(&ctx);
    test_chown_attaches_companion(&ctx);
    test_chown_stale_companion_no_eperm(&ctx);
    test_specials_untouched(&ctx);
    test_write_create_paths(&ctx);

    chimera_vfs_release(ctx.vfs_thread, ctx.wcnt);
    chimera_vfs_release(ctx.vfs_thread, ctx.wdir);
    free(ctx.got_acl);

    chimera_vfs_thread_destroy(ctx.vfs_thread);
    chimera_vfs_destroy(ctx.vfs);
    prometheus_metrics_destroy(ctx.metrics);
    evpl_destroy(ctx.evpl);

    fprintf(stderr, "vfs_acl_windows_test: all tests passed\n");
    return 0;
} /* main */
