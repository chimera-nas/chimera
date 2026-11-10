// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Backend-parameterized: on an engine-DAC backend (memfs, cairn, diskfs) the
 * engine checks the parent directory of every creating open_at itself, for
 * POSIX (AUTH_UNIX: EXECUTE, then WRITE_DATA for the create) and SMB
 * (AUTH_ATTR: WRITE_DATA = ADD_FILE only) callers alike -- whether or not the
 * module checks the parent too.  A caller refused the create right is still
 * dispatched, without the create, so it may open an existing name; it never
 * creates one.
 *
 *     vfs_open_create_gate_test <backend>
 *
 * where <backend> is memfs (default), cairn, diskfs_io_uring, or diskfs_aio.
 */

#include "common/test_host.h"
#include <stdio.h>
#include <stddef.h>
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

#define UID_OWNER      1000 /* owns the parent directories */
#define UID_A          2000 /* the caller under test */
#define UID_B          3000 /* a second caller no ACE names */

/* A sized stand-in for struct chimera_acl, whose flexible array member MSVC
 * refuses to embed in another struct (C2229).  The hdr mirror keeps the real
 * header layout; the assert below pins the two together. */
#define TEST_ACL_MAX   4

struct test_acl {
    struct {
        uint16_t num_aces;
        uint16_t ctrl_flags;
    } hdr;
    struct chimera_ace aces[TEST_ACL_MAX];
};

_Static_assert(offsetof(struct test_acl, aces) ==
               offsetof(struct chimera_acl, aces),
               "test_acl must mirror the struct chimera_acl layout");

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
    uint64_t                        got_size;
    struct chimera_vfs_attrs       *got_set_attr;
    struct chimera_vfs_attrs       *want_set_attr;
};

/* Everything a case needs: the event loop, the mount root (held open) and
 * the callers. */
struct test_env {
    struct test_ctx                 ctx;
    const char                     *backend;
    struct chimera_vfs_open_handle *mnt;
    struct chimera_vfs_cred         root;       /* AUTH_UNIX uid 0 */
    struct chimera_vfs_cred         unix_a;     /* AUTH_UNIX UID_A */
    struct chimera_vfs_cred         attr_root;  /* AUTH_ATTR uid 0 */
    struct chimera_vfs_cred         attr_a;     /* AUTH_ATTR UID_A */
    struct chimera_vfs_cred         attr_b;     /* AUTH_ATTR UID_B */
    int                             failures;
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
lookupat_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status   = error_code;
    ctx->got_mask = 0;
    if (error_code == CHIMERA_VFS_OK) {
        ctx->got_mask = attr->va_set_mask;
        ctx->got_size = attr->va_size;
    }
    ctx->done = 1;
} /* lookupat_cb */

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

    ctx->status       = error_code;
    ctx->handle       = oh;
    ctx->got_set_attr = set_attr;
    ctx->done         = 1;
} /* openat_cb */

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

/* Per-backend wiring, mirroring vfs_open_acl_test. */
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

/* Record a mismatch; the run fails at the end, after every case has run. */
static void
expect(
    struct test_env       *env,
    const char            *test,
    const char            *what,
    enum chimera_vfs_error got,
    enum chimera_vfs_error want)
{
    if (got != want) {
        fprintf(stderr, "  FAIL: %s on %s: %s: expected %d, got %d\n",
                test, env->backend, what, want, got);
        env->failures++;
    }
} /* expect */

/* Root mkdir of a fresh parent directory `name` under the mount root; returns
 * an open handle on it. */
static struct chimera_vfs_open_handle *
parent_make(
    struct test_env *env,
    const char      *name)
{
    struct test_ctx         *ctx = &env->ctx;
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0755;
    chimera_vfs_mkdir_at(ctx->vfs_thread, &env->root, env->mnt, name,
                         strlen(name), &sattr, CHIMERA_VFS_ATTR_FH, 0, 0,
                         mkdir_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);

    chimera_vfs_open_fh(ctx->vfs_thread, &env->root, ctx->fh, ctx->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    return ctx->handle;
} /* parent_make */

/* Root setattr of `dir`'s mode and owner. */
static void
set_mode(
    struct test_env                *env,
    struct chimera_vfs_open_handle *dir,
    uint32_t                        mode)
{
    struct test_ctx         *ctx = &env->ctx;
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
        CHIMERA_VFS_ATTR_GID;
    sattr.va_mode = mode;
    sattr.va_uid  = UID_OWNER;
    sattr.va_gid  = UID_OWNER;
    chimera_vfs_setattr(ctx->vfs_thread, &env->root, dir, &sattr, 0, 0,
                        setattr_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
} /* set_mode */

/* Root setattr of the ACL in `acl`. */
static void
set_acl(
    struct test_env                *env,
    struct chimera_vfs_open_handle *dir,
    struct test_acl                *acl)
{
    struct test_ctx         *ctx = &env->ctx;
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_ACL;
    sattr.va_acl      = (struct chimera_acl *) acl;
    chimera_vfs_setattr(ctx->vfs_thread, &env->root, dir, &sattr, 0, 0,
                        setattr_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
} /* set_acl */

/* Append one ACE to `acl`: a numeric user when `uid` is not negative, else
 * EVERYONE@. */
static void
acl_add(
    struct test_acl *acl,
    uint16_t         type,
    uint32_t         mask,
    int64_t          uid)
{
    struct chimera_ace *ace;

    assert(acl->hdr.num_aces < TEST_ACL_MAX);
    ace = &acl->aces[acl->hdr.num_aces++];
    memset(ace, 0, sizeof(*ace));
    ace->type        = type;
    ace->access_mask = mask;
    if (uid >= 0) {
        ace->who.type = CHIMERA_PRINCIPAL_USER;
        ace->who.id   = (uint32_t) uid;
    } else {
        ace->who.type    = CHIMERA_PRINCIPAL_SPECIAL;
        ace->who.special = CHIMERA_WHO_EVERYONE;
    }
} /* acl_add */

/* open_at `name` in `dir` as `cred` with `flags`; a set_attr asking for size
 * 0 when `trunc` is set, an empty one otherwise.  Releases any handle and
 * returns the status. */
static enum chimera_vfs_error
open_as(
    struct test_env                *env,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir,
    const char                     *name,
    unsigned int                    flags,
    int                             trunc)
{
    struct test_ctx         *ctx = &env->ctx;
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    if (trunc) {
        sattr.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
        sattr.va_size     = 0;
    }
    ctx->handle        = NULL;
    ctx->want_set_attr = &sattr;
    chimera_vfs_open_at(ctx->vfs_thread, cred, dir, name, strlen(name), flags,
                        &sattr, CHIMERA_VFS_ATTR_FH, 0, 0, openat_cb, ctx);
    wait_done(ctx);
    if (ctx->status == CHIMERA_VFS_OK && ctx->handle) {
        chimera_vfs_release(ctx->vfs_thread, ctx->handle);
    }
    return ctx->status;
} /* open_as */

/* Root create of file `name` in `dir` with `mode` and `size`. */
static void
root_create(
    struct test_env                *env,
    struct chimera_vfs_open_handle *dir,
    const char                     *name,
    uint32_t                        mode,
    uint64_t                        size)
{
    struct test_ctx         *ctx = &env->ctx;
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = mode;
    if (size) {
        sattr.va_set_mask |= CHIMERA_VFS_ATTR_SIZE;
        sattr.va_size      = size;
    }
    chimera_vfs_open_at(ctx->vfs_thread, &env->root, dir, name, strlen(name),
                        CHIMERA_VFS_OPEN_CREATE, &sattr, CHIMERA_VFS_ATTR_FH,
                        0, 0, openat_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    chimera_vfs_release(ctx->vfs_thread, ctx->handle);
} /* root_create */

/* Root mkdir of `name` in `dir`. */
static void
root_mkdir(
    struct test_env                *env,
    struct chimera_vfs_open_handle *dir,
    const char                     *name)
{
    struct test_ctx         *ctx = &env->ctx;
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0777;
    chimera_vfs_mkdir_at(ctx->vfs_thread, &env->root, dir, name, strlen(name),
                         &sattr, CHIMERA_VFS_ATTR_FH, 0, 0, mkdir_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
} /* root_mkdir */

/* Root lookup of `name` in `dir`; the size lands in ctx.got_size. */
static enum chimera_vfs_error
root_lookup(
    struct test_env                *env,
    struct chimera_vfs_open_handle *dir,
    const char                     *name)
{
    struct test_ctx *ctx = &env->ctx;

    chimera_vfs_lookup_at(ctx->vfs_thread, &env->root, dir, name, strlen(name),
                          CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                          lookupat_cb, ctx);
    wait_done(ctx);
    return ctx->status;
} /* root_lookup */

/* U1: a DENY of ADD_FILE (WRITE_DATA) to the SMB caller refuses its create,
 * and nothing is created -- on every backend, including those whose own
 * open_at never checked an SMB create against the parent. */
static void
test_attr_deny_addfile(struct test_env *env)
{
    const char                     *t     = "test_attr_deny_addfile";
    int                             start = env->failures;
    struct chimera_vfs_open_handle *dir;
    struct test_acl                 acl;

    dir = parent_make(env, "u1");
    set_mode(env, dir, 0777);

    memset(&acl, 0, sizeof(acl));
    acl_add(&acl, CHIMERA_ACE_DENIED, CHIMERA_ACE_WRITE_DATA, UID_A);
    acl_add(&acl, CHIMERA_ACE_ALLOWED,
            CHIMERA_ACE_READ_DATA | CHIMERA_ACE_WRITE_DATA | CHIMERA_ACE_EXECUTE,
            -1);
    set_acl(env, dir, &acl);

    expect(env, t, "denied SMB create",
           open_as(env, &env->attr_a, dir, "f",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY, 0),
           CHIMERA_VFS_EACCES);
    expect(env, t, "root lookup after the denied create",
           root_lookup(env, dir, "f"), CHIMERA_VFS_ENOENT);

    /* Control: the DENY names one user; EVERYONE@ still admits another. */
    expect(env, t, "admitted SMB create",
           open_as(env, &env->attr_b, dir, "g",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY, 0),
           CHIMERA_VFS_OK);

    chimera_vfs_release(env->ctx.vfs_thread, dir);
    if (env->failures == start) {
        TEST_PASS(t);
    }
} /* test_attr_deny_addfile */

/* U2: a POSIX create needs write on the parent. */
static void
test_unix_create_no_write(struct test_env *env)
{
    const char                     *t     = "test_unix_create_no_write";
    int                             start = env->failures;
    struct chimera_vfs_open_handle *dir;

    dir = parent_make(env, "u2");
    set_mode(env, dir, 0555);

    expect(env, t, "create without write",
           open_as(env, &env->unix_a, dir, "f",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY, 0),
           CHIMERA_VFS_EACCES);

    chimera_vfs_release(env->ctx.vfs_thread, dir);
    if (env->failures == start) {
        TEST_PASS(t);
    }
} /* test_unix_create_no_write */

/* U3: ...and search: write without search is still refused. */
static void
test_unix_create_no_exec(struct test_env *env)
{
    const char                     *t     = "test_unix_create_no_exec";
    int                             start = env->failures;
    struct chimera_vfs_open_handle *dir;

    dir = parent_make(env, "u3");
    set_mode(env, dir, 0666);

    expect(env, t, "create without search",
           open_as(env, &env->unix_a, dir, "f",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY, 0),
           CHIMERA_VFS_EACCES);

    chimera_vfs_release(env->ctx.vfs_thread, dir);
    if (env->failures == start) {
        TEST_PASS(t);
    }
} /* test_unix_create_no_exec */

/* U4: a caller refused the create right still opens an existing name. */
static void
test_refused_create_existing_opens(struct test_env *env)
{
    const char                     *t     = "test_refused_create_existing_opens";
    int                             start = env->failures;
    struct chimera_vfs_open_handle *dir;

    dir = parent_make(env, "u4");
    root_create(env, dir, "f", 0666, 0);
    set_mode(env, dir, 0555);

    expect(env, t, "refused create of an existing name",
           open_as(env, &env->unix_a, dir, "f",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_READ_ONLY, 0),
           CHIMERA_VFS_OK);

    chimera_vfs_release(env->ctx.vfs_thread, dir);
    if (env->failures == start) {
        TEST_PASS(t);
    }
} /* test_refused_create_existing_opens */

/* U5: an exclusive create of an existing name collides, refused or not. */
static void
test_refused_create_exclusive_exists(struct test_env *env)
{
    const char                     *t     = "test_refused_create_exclusive_exists";
    int                             start = env->failures;
    struct chimera_vfs_open_handle *dir;

    dir = parent_make(env, "u5");
    root_create(env, dir, "f", 0666, 0);
    set_mode(env, dir, 0555);

    expect(env, t, "refused exclusive create of an existing name",
           open_as(env, &env->unix_a, dir, "f",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE |
                   CHIMERA_VFS_OPEN_READ_ONLY, 0),
           CHIMERA_VFS_EEXIST);

    chimera_vfs_release(env->ctx.vfs_thread, dir);
    if (env->failures == start) {
        TEST_PASS(t);
    }
} /* test_refused_create_exclusive_exists */

/* U6: an exclusive create of a missing name is refused the create. */
static void
test_refused_create_exclusive_missing(struct test_env *env)
{
    const char                     *t     = "test_refused_create_exclusive_missing";
    int                             start = env->failures;
    struct chimera_vfs_open_handle *dir;

    dir = parent_make(env, "u6");
    set_mode(env, dir, 0555);

    expect(env, t, "refused exclusive create of a missing name",
           open_as(env, &env->unix_a, dir, "g",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE, 0),
           CHIMERA_VFS_EACCES);
    expect(env, t, "root lookup after the refused create",
           root_lookup(env, dir, "g"), CHIMERA_VFS_ENOENT);

    chimera_vfs_release(env->ctx.vfs_thread, dir);
    if (env->failures == start) {
        TEST_PASS(t);
    }
} /* test_refused_create_exclusive_missing */

/* U7: a refused create of a missing name answers EACCES, not ENOENT. */
static void
test_refused_create_missing(struct test_env *env)
{
    const char                     *t     = "test_refused_create_missing";
    int                             start = env->failures;
    struct chimera_vfs_open_handle *dir;

    dir = parent_make(env, "u7");
    set_mode(env, dir, 0555);

    expect(env, t, "refused create of a missing name",
           open_as(env, &env->unix_a, dir, "g", CHIMERA_VFS_OPEN_CREATE, 0),
           CHIMERA_VFS_EACCES);
    expect(env, t, "root lookup after the refused create",
           root_lookup(env, dir, "g"), CHIMERA_VFS_ENOENT);

    chimera_vfs_release(env->ctx.vfs_thread, dir);
    if (env->failures == start) {
        TEST_PASS(t);
    }
} /* test_refused_create_missing */

/* U8: a granted create creates, for either flavor. */
static void
test_granted_create(struct test_env *env)
{
    const char                     *t     = "test_granted_create";
    int                             start = env->failures;
    struct chimera_vfs_open_handle *dir;

    dir = parent_make(env, "u8");
    set_mode(env, dir, 0777);

    expect(env, t, "POSIX create",
           open_as(env, &env->unix_a, dir, "f",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY, 0),
           CHIMERA_VFS_OK);
    expect(env, t, "root lookup of the POSIX create",
           root_lookup(env, dir, "f"), CHIMERA_VFS_OK);
    expect(env, t, "SMB create",
           open_as(env, &env->attr_a, dir, "g",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY, 0),
           CHIMERA_VFS_OK);
    expect(env, t, "root lookup of the SMB create",
           root_lookup(env, dir, "g"), CHIMERA_VFS_OK);

    chimera_vfs_release(env->ctx.vfs_thread, dir);
    if (env->failures == start) {
        TEST_PASS(t);
    }
} /* test_granted_create */

/* U9: a refused creator opening an existing writable file still gets the
 * size its set_attr asked for -- the deferred truncate survives the refusal. */
static void
test_refused_create_truncates_existing(struct test_env *env)
{
    const char                     *t     = "test_refused_create_truncates_existing";
    int                             start = env->failures;
    struct chimera_vfs_open_handle *dir;

    dir = parent_make(env, "u9");
    root_create(env, dir, "f", 0666, 5);
    set_mode(env, dir, 0555);

    /* The premise: the file has content to lose. */
    assert(root_lookup(env, dir, "f") == CHIMERA_VFS_OK);
    assert(env->ctx.got_mask & CHIMERA_VFS_ATTR_SIZE);
    assert(env->ctx.got_size == 5);

    expect(env, t, "refused create truncating an existing file",
           open_as(env, &env->unix_a, dir, "f",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY, 1),
           CHIMERA_VFS_OK);
    expect(env, t, "root lookup after the truncate",
           root_lookup(env, dir, "f"), CHIMERA_VFS_OK);
    if (!(env->ctx.got_mask & CHIMERA_VFS_ATTR_SIZE) ||
        env->ctx.got_size != 0) {
        fprintf(stderr, "  FAIL: %s on %s: size after the truncate: "
                "expected 0, got %llu\n", t, env->backend,
                (unsigned long long) env->ctx.got_size);
        env->failures++;
    }

    chimera_vfs_release(env->ctx.vfs_thread, dir);
    if (env->failures == start) {
        TEST_PASS(t);
    }
} /* test_refused_create_truncates_existing */

/* U10: a refused create of an existing directory for writing is EISDIR, as
 * the same open would be with the create right. */
static void
test_refused_create_existing_dir_eisdir(struct test_env *env)
{
    const char                     *t     = "test_refused_create_existing_dir_eisdir";
    int                             start = env->failures;
    struct chimera_vfs_open_handle *dir;

    dir = parent_make(env, "u10");
    root_mkdir(env, dir, "d");
    set_mode(env, dir, 0555);

    expect(env, t, "refused create of an existing directory",
           open_as(env, &env->unix_a, dir, "d",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY, 0),
           CHIMERA_VFS_EISDIR);

    chimera_vfs_release(env->ctx.vfs_thread, dir);
    if (env->failures == start) {
        TEST_PASS(t);
    }
} /* test_refused_create_existing_dir_eisdir */

/* U11: root is exempt, whatever the parent's mode and ACL say. */
static void
test_root_exempt(struct test_env *env)
{
    const char                     *t     = "test_root_exempt";
    int                             start = env->failures;
    struct chimera_vfs_open_handle *dir;
    struct test_acl                 acl;

    dir = parent_make(env, "u11");
    set_mode(env, dir, 0000);

    memset(&acl, 0, sizeof(acl));
    acl_add(&acl, CHIMERA_ACE_DENIED, CHIMERA_ACE_MASK_ALL, -1);
    set_acl(env, dir, &acl);

    expect(env, t, "POSIX root create",
           open_as(env, &env->root, dir, "f",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY, 0),
           CHIMERA_VFS_OK);
    expect(env, t, "SMB root create",
           open_as(env, &env->attr_root, dir, "g",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY, 0),
           CHIMERA_VFS_OK);

    chimera_vfs_release(env->ctx.vfs_thread, dir);
    if (env->failures == start) {
        TEST_PASS(t);
    }
} /* test_root_exempt */

/* U12: an open that cannot create is the module's to decide, as before --
 * search alone admits it. */
static void
test_noncreate_unchanged(struct test_env *env)
{
    const char                     *t     = "test_noncreate_unchanged";
    int                             start = env->failures;
    struct chimera_vfs_open_handle *dir;

    dir = parent_make(env, "u12");
    root_create(env, dir, "f", 0644, 0);
    set_mode(env, dir, 0111);

    expect(env, t, "open under a search-only parent",
           open_as(env, &env->unix_a, dir, "f", CHIMERA_VFS_OPEN_READ_ONLY, 0),
           CHIMERA_VFS_OK);

    chimera_vfs_release(env->ctx.vfs_thread, dir);
    if (env->failures == start) {
        TEST_PASS(t);
    }
} /* test_noncreate_unchanged */

/* U13: a POSIX caller who may not search the parent learns nothing about a
 * name in it: an existing name is refused exactly as a missing one would be,
 * and an exclusive create never reports the collision. */
static void
test_unix_no_exec_existing_no_leak(struct test_env *env)
{
    const char                     *t     = "test_unix_no_exec_existing_no_leak";
    int                             start = env->failures;
    struct chimera_vfs_open_handle *dir;

    dir = parent_make(env, "u13");
    root_create(env, dir, "f", 0666, 0);
    set_mode(env, dir, 0666);

    expect(env, t, "create of an existing name without search",
           open_as(env, &env->unix_a, dir, "f",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY, 0),
           CHIMERA_VFS_EACCES);
    expect(env, t, "exclusive create of an existing name without search",
           open_as(env, &env->unix_a, dir, "f",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE |
                   CHIMERA_VFS_OPEN_WRITE_ONLY, 0),
           CHIMERA_VFS_EACCES);

    chimera_vfs_release(env->ctx.vfs_thread, dir);
    if (env->failures == start) {
        TEST_PASS(t);
    }
} /* test_unix_no_exec_existing_no_leak */

/* U14: an SMB caller owes no traverse, so a directory granting it neither
 * ADD_FILE nor EXECUTE still lets it collide with an existing name, and open
 * one when the create it asked for is refused. */
static void
test_attr_exclusive_collides(struct test_env *env)
{
    const char                     *t     = "test_attr_exclusive_collides";
    int                             start = env->failures;
    struct chimera_vfs_open_handle *dir;
    struct test_acl                 acl;

    dir = parent_make(env, "u14");
    root_create(env, dir, "f", 0644, 0);
    set_mode(env, dir, 0777);

    memset(&acl, 0, sizeof(acl));
    acl_add(&acl, CHIMERA_ACE_DENIED, CHIMERA_ACE_WRITE_DATA, UID_A);
    acl_add(&acl, CHIMERA_ACE_ALLOWED, CHIMERA_ACE_READ_DATA, -1);
    set_acl(env, dir, &acl);

    expect(env, t, "refused SMB exclusive create of an existing name",
           open_as(env, &env->attr_a, dir, "f",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE |
                   CHIMERA_VFS_OPEN_READ_ONLY, 0),
           CHIMERA_VFS_EEXIST);
    expect(env, t, "refused SMB create of an existing name",
           open_as(env, &env->attr_a, dir, "f",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_READ_ONLY, 0),
           CHIMERA_VFS_OK);

    chimera_vfs_release(env->ctx.vfs_thread, dir);
    if (env->failures == start) {
        TEST_PASS(t);
    }
} /* test_attr_exclusive_collides */

/* The engine answered the open itself; the caller's set_attr must still come
 * back through the callback.  The NFSv3 and NFSv4 servers reuse it as the
 * scratch for the EXCLUSIVE verifier re-open, so a NULL here is a server
 * crash, not a cosmetic miss. */
static void
expect_set_attr_returned(
    struct test_env *env,
    const char      *test,
    const char      *what)
{
    struct test_ctx *ctx = &env->ctx;

    if (ctx->got_set_attr != ctx->want_set_attr) {
        fprintf(stderr, "  FAIL: %s on %s: %s: set_attr %p not the "
                "caller's %p\n",
                test, env->backend, what,
                (void *) ctx->got_set_attr, (void *) ctx->want_set_attr);
        env->failures++;
    }
} /* expect_set_attr_returned */

/* U15: a refusal the engine answers itself -- the exclusive-create probe's
 * EEXIST and EACCES, and the no-search EACCES -- still hands the caller's
 * set_attr back through the callback. */
static void
test_refusal_returns_set_attr(struct test_env *env)
{
    const char                     *t     = "test_refusal_returns_set_attr";
    int                             start = env->failures;
    struct chimera_vfs_open_handle *dir;

    dir = parent_make(env, "u15");
    root_create(env, dir, "f", 0666, 0);
    set_mode(env, dir, 0555);

    expect(env, t, "refused exclusive create of an existing name",
           open_as(env, &env->unix_a, dir, "f",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE |
                   CHIMERA_VFS_OPEN_READ_ONLY, 0),
           CHIMERA_VFS_EEXIST);
    expect_set_attr_returned(env, t, "set_attr after the EEXIST probe");

    expect(env, t, "refused exclusive create of a missing name",
           open_as(env, &env->unix_a, dir, "g",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE, 0),
           CHIMERA_VFS_EACCES);
    expect_set_attr_returned(env, t, "set_attr after the EACCES probe");

    set_mode(env, dir, 0666);
    expect(env, t, "exclusive create without search",
           open_as(env, &env->unix_a, dir, "f",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE |
                   CHIMERA_VFS_OPEN_WRITE_ONLY, 0),
           CHIMERA_VFS_EACCES);
    expect_set_attr_returned(env, t, "set_attr after the no-search refusal");

    chimera_vfs_release(env->ctx.vfs_thread, dir);
    if (env->failures == start) {
        TEST_PASS(t);
    }
} /* test_refusal_returns_set_attr */

/* U16: on an FH-relative backend the parent of `name` is the handle itself,
 * slash or no slash.  A name like "sub/x" must not have a writable
 * subdirectory stand in for the directory the caller cannot write: the gate
 * evaluates the handle, and nothing is created under either name. */
static void
test_slashed_name_checks_handle(struct test_env *env)
{
    const char                     *t     = "test_slashed_name_checks_handle";
    int                             start = env->failures;
    struct test_ctx                *ctx   = &env->ctx;
    struct chimera_vfs_open_handle *dir;
    struct chimera_vfs_open_handle *sub;

    dir = parent_make(env, "u16"); /* 0755, root-owned: no write for A */
    root_mkdir(env, dir, "sub");   /* 0777: writable by anyone */

    chimera_vfs_open_fh(ctx->vfs_thread, &env->root, ctx->fh, ctx->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    sub = ctx->handle;

    expect(env, t, "create of a slashed name in an unwritable directory",
           open_as(env, &env->unix_a, dir, "sub/x",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY, 0),
           CHIMERA_VFS_EACCES);
    expect(env, t, "root lookup of the literal slashed name",
           root_lookup(env, dir, "sub/x"), CHIMERA_VFS_ENOENT);
    expect(env, t, "root lookup inside the subdirectory",
           root_lookup(env, sub, "x"), CHIMERA_VFS_ENOENT);

    chimera_vfs_release(ctx->vfs_thread, sub);
    chimera_vfs_release(ctx->vfs_thread, dir);
    if (env->failures == start) {
        TEST_PASS(t);
    }
} /* test_slashed_name_checks_handle */

/* U17: only the engine says it has checked the parent.  A caller smuggling
 * the engine-internal bits in from outside gets them stripped at entry: the
 * module still refuses what the engine never checked, and a missing name is
 * still ENOENT, not the refused-create EACCES. */
static void
test_external_flags_stripped(struct test_env *env)
{
    const char                     *t     = "test_external_flags_stripped";
    int                             start = env->failures;
    struct chimera_vfs_open_handle *dir;

    dir = parent_make(env, "u17");
    root_create(env, dir, "f", 0666, 0);
    set_mode(env, dir, 0666); /* no search for anyone */

    expect(env, t, "non-create open smuggling PARENT_CHECKED",
           open_as(env, &env->unix_a, dir, "f",
                   CHIMERA_VFS_OPEN_PARENT_CHECKED |
                   CHIMERA_VFS_OPEN_READ_ONLY, 0),
           CHIMERA_VFS_EACCES);

    set_mode(env, dir, 0777);
    expect(env, t, "non-create open of a missing name smuggling "
           "CREATE_REFUSED",
           open_as(env, &env->unix_a, dir, "nosuch",
                   CHIMERA_VFS_OPEN_CREATE_REFUSED |
                   CHIMERA_VFS_OPEN_READ_ONLY, 0),
           CHIMERA_VFS_ENOENT);

    chimera_vfs_release(env->ctx.vfs_thread, dir);
    if (env->failures == start) {
        TEST_PASS(t);
    }
} /* test_external_flags_stripped */

/* U18: the deferred truncate is authorized against the existing file.  A
 * refused creator aiming O_CREAT|O_TRUNC at a file it may not write is
 * refused the open, and the content survives. */
static void
test_refused_create_no_truncate_readonly(struct test_env *env)
{
    const char                     *t =
        "test_refused_create_no_truncate_readonly";
    int                             start = env->failures;
    struct chimera_vfs_open_handle *dir;

    dir = parent_make(env, "u18");
    root_create(env, dir, "ro", 0444, 5);
    set_mode(env, dir, 0555);

    expect(env, t, "refused create truncating an unwritable file",
           open_as(env, &env->unix_a, dir, "ro",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY, 1),
           CHIMERA_VFS_EACCES);
    expect(env, t, "root lookup after the refused truncate",
           root_lookup(env, dir, "ro"), CHIMERA_VFS_OK);
    if (!(env->ctx.got_mask & CHIMERA_VFS_ATTR_SIZE) ||
        env->ctx.got_size != 5) {
        fprintf(stderr, "  FAIL: %s on %s: size after the refused truncate: "
                "expected 5, got %llu\n", t, env->backend,
                (unsigned long long) env->ctx.got_size);
        env->failures++;
    }

    chimera_vfs_release(env->ctx.vfs_thread, dir);
    if (env->failures == start) {
        TEST_PASS(t);
    }
} /* test_refused_create_no_truncate_readonly */

/* U19: the refused-exclusive existence probe asks the module for the literal
 * name, exactly what a dispatched create would have named.  It must not walk
 * the name as a path under the engine's uid-0 credential: that would answer
 * EEXIST for nested names the caller cannot traverse ("sub/x", "../f"),
 * turning the refusal into an existence oracle over the whole namespace. */
static void
test_excl_probe_no_path_walk(struct test_env *env)
{
    const char                     *t     = "test_excl_probe_no_path_walk";
    int                             start = env->failures;
    struct test_ctx                *ctx   = &env->ctx;
    struct chimera_vfs_open_handle *dir;
    struct chimera_vfs_open_handle *sub;

    dir = parent_make(env, "u19"); /* 0755, root-owned: A searches, no write */
    root_mkdir(env, dir, "sub");

    chimera_vfs_open_fh(ctx->vfs_thread, &env->root, ctx->fh, ctx->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    sub = ctx->handle;

    set_mode(env, sub, 0700);                    /* A may not search sub... */
    root_create(env, sub, "x", 0666, 0);         /* ...which holds x */
    root_create(env, env->mnt, "u19f", 0666, 0); /* a name ".." would reach */

    expect(env, t, "refused exclusive create of a nested existing name",
           open_as(env, &env->unix_a, dir, "sub/x",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE |
                   CHIMERA_VFS_OPEN_READ_ONLY, 0),
           CHIMERA_VFS_EACCES);
    expect_set_attr_returned(env, t, "set_attr after the literal-name probe");
    expect(env, t, "refused exclusive create of a dot-dot name",
           open_as(env, &env->unix_a, dir, "../u19f",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE |
                   CHIMERA_VFS_OPEN_READ_ONLY, 0),
           CHIMERA_VFS_EACCES);

    chimera_vfs_release(ctx->vfs_thread, sub);
    chimera_vfs_release(ctx->vfs_thread, dir);
    if (env->failures == start) {
        TEST_PASS(t);
    }
} /* test_excl_probe_no_path_walk */

/* R1: the built-in rootfs is an engine-DAC mount like any other.  A refused
 * exclusive create of a missing name is the gate's EACCES, ahead of the
 * module's EROFS (the order the mkdir gate set), because the probe answers
 * it without a dispatch.  A refused non-exclusive create IS dispatched, as a
 * plain open, and the read-only module refuses every OPEN_AT with EROFS --
 * the same answer a plain open of the name gets, so the refusal leaks
 * nothing.  An exclusive create of a mount-point name is the collision. */
static void
test_rootfs_gate_ordering(
    struct test_env                *env,
    struct chimera_vfs_open_handle *vroot)
{
    const char *t     = "test_rootfs_gate_ordering";
    int         start = env->failures;

    expect(env, t, "non-root exclusive create of a missing rootfs name",
           open_as(env, &env->unix_a, vroot, "nosuch",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE, 0),
           CHIMERA_VFS_EACCES);
    expect(env, t, "non-root create of a missing rootfs name",
           open_as(env, &env->unix_a, vroot, "nosuch",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY, 0),
           CHIMERA_VFS_EROFS);
    expect(env, t, "non-root exclusive create of a mount-point name",
           open_as(env, &env->unix_a, vroot, "test",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE |
                   CHIMERA_VFS_OPEN_READ_ONLY, 0),
           CHIMERA_VFS_EEXIST);

    if (env->failures == start) {
        TEST_PASS(t);
    }
} /* test_rootfs_gate_ordering */

/* R2: a gate-exempt caller's exclusive create of a mount-point name is
 * answered by the mount-cross redirect (the covered name opens as the mounted
 * root, which an exclusive create collides with), not the probe -- the one
 * other engine-answered EEXIST, and the same NFS servers reuse the set_attr
 * behind it. */
static void
test_mount_point_collision_returns_set_attr(
    struct test_env                *env,
    struct chimera_vfs_open_handle *vroot)
{
    const char *t     = "test_mount_point_collision_returns_set_attr";
    int         start = env->failures;

    expect(env, t, "root exclusive create of a mount-point name",
           open_as(env, &env->root, vroot, "test",
                   CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_EXCLUSIVE |
                   CHIMERA_VFS_OPEN_READ_ONLY, 0),
           CHIMERA_VFS_EEXIST);
    expect_set_attr_returned(env, t, "set_attr after the redirect collision");

    if (env->failures == start) {
        TEST_PASS(t);
    }
} /* test_mount_point_collision_returns_set_attr */

int
main(
    int    argc,
    char **argv)
{
    struct test_env               env;
    struct test_ctx              *ctx = &env.ctx;
    struct chimera_vfs_module_cfg module_cfgs[2];
    struct backend_spec           spec;
    struct prometheus_metrics    *metrics;
    char                          tmpl[] = "/tmp/vfs_opencg_XXXXXX";
    char                         *session_dir;
    uint8_t                       vroot_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                      vroot_fh_len;

    memset(&env, 0, sizeof(env));
    env.backend = argc > 1 ? argv[1] : "memfs";

    chimera_log_init();
    chimera_vfs_cred_init_unix(&env.root, 0, 0, 0, NULL);
    chimera_vfs_cred_init_unix(&env.unix_a, UID_A, UID_A, 0, NULL);
    chimera_vfs_cred_init_attr(&env.attr_root, 0, 0, 0, NULL);
    chimera_vfs_cred_init_attr(&env.attr_a, UID_A, UID_A, 0, NULL);
    chimera_vfs_cred_init_attr(&env.attr_b, UID_B, UID_B, 0, NULL);

    session_dir = mkdtemp(tmpl);
    assert(session_dir != NULL);

    memset(module_cfgs, 0, sizeof(module_cfgs));
    backend_configure(env.backend, session_dir, module_cfgs, &spec);

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(metrics != NULL);

    ctx->evpl = evpl_create(NULL);
    assert(ctx->evpl != NULL);

    ctx->vfs = chimera_vfs_init(0, 0, module_cfgs, spec.ncfg, "memkv", 60, 1, 1,
                                0, metrics);
    assert(ctx->vfs != NULL);

    ctx->vfs_thread = chimera_vfs_thread_init(ctx->evpl, ctx->vfs);
    assert(ctx->vfs_thread != NULL);

    chimera_vfs_mkfs(ctx->vfs_thread, NULL, spec.mount_module, "fs0", NULL,
                     mount_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);

    chimera_vfs_mount(ctx->vfs_thread, NULL, "/test", spec.mount_module, "fs0",
                      NULL, mount_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);

    chimera_vfs_get_root_fh(ctx->vfs_thread->vfs, vroot_fh, &vroot_fh_len);
    chimera_vfs_lookup(ctx->vfs_thread, &env.root, vroot_fh, vroot_fh_len,
                       "test", 4,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                       lookup_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);

    chimera_vfs_open_fh(ctx->vfs_thread, &env.root, ctx->fh, ctx->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    env.mnt = ctx->handle;

    test_attr_deny_addfile(&env);
    test_unix_create_no_write(&env);
    test_unix_create_no_exec(&env);
    test_refused_create_existing_opens(&env);
    test_refused_create_exclusive_exists(&env);
    test_refused_create_exclusive_missing(&env);
    test_refused_create_missing(&env);
    test_granted_create(&env);
    test_refused_create_truncates_existing(&env);
    test_refused_create_existing_dir_eisdir(&env);
    test_root_exempt(&env);
    test_noncreate_unchanged(&env);
    test_unix_no_exec_existing_no_leak(&env);
    test_attr_exclusive_collides(&env);
    test_refusal_returns_set_attr(&env);
    test_slashed_name_checks_handle(&env);
    test_external_flags_stripped(&env);
    test_refused_create_no_truncate_readonly(&env);
    test_excl_probe_no_path_walk(&env);

    /* The rootfs cases run while /test is still a mount point. */
    {
        struct chimera_vfs_open_handle *vroot;

        chimera_vfs_open_fh(ctx->vfs_thread, &env.root, vroot_fh,
                            vroot_fh_len, CHIMERA_VFS_OPEN_INFERRED,
                            openfh_cb, ctx);
        wait_done(ctx);
        assert(ctx->status == CHIMERA_VFS_OK);
        vroot = ctx->handle;
        test_rootfs_gate_ordering(&env, vroot);
        test_mount_point_collision_returns_set_attr(&env, vroot);
        chimera_vfs_release(ctx->vfs_thread, vroot);
    }

    chimera_vfs_release(ctx->vfs_thread, env.mnt);

    chimera_vfs_umount(ctx->vfs_thread, NULL, "/test", mount_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);

    for (int i = 0; i < 50; i++) {
        chimera_vfs_rmfs(ctx->vfs_thread, NULL, spec.mount_module, "fs0",
                         mount_cb, ctx);
        wait_done(ctx);
        if (ctx->status != CHIMERA_VFS_EBUSY) {
            break;
        }
        usleep(100000);
    }
    assert(ctx->status == CHIMERA_VFS_OK);

    chimera_vfs_thread_destroy(ctx->vfs_thread);
    chimera_vfs_destroy(ctx->vfs);
    evpl_destroy(ctx->evpl);
    prometheus_metrics_destroy(metrics);

    if (chimera_test_remove_tree(session_dir) != 0) {
        fprintf(stderr, "warning: could not remove %s\n", session_dir);
    }

    if (env.failures) {
        fprintf(stderr, "%d open create-gate check(s) failed on %s\n",
                env.failures, env.backend);
        return 1;
    }

    fprintf(stderr, "All open create-gate tests passed on %s\n", env.backend);
    return 0;
} /* main */
