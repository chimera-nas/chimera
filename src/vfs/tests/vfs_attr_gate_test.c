// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * The engine's access gates must fail CLOSED when a backend replies without
 * the attributes the gate evaluates (mode, uid, gid, and the native ACL on
 * an ACL-native backend).  A shim module, "attrstrip", is memfs in every
 * respect -- same capabilities, same fh_magic, so every memfs-encoded handle
 * routes through it -- except that, when armed, it clears chosen bits from
 * the r_attr of one opcode's replies: a backend that under-fills its reply.
 *
 * Each cell creates a fresh file (so no cached handle or stamped grant from
 * an earlier cell can answer for it), arms the shim, runs one operation as
 * one actor, disarms, and checks the status.  Controls run with the shim
 * disarmed and show what the correct answer is.  The VFS runs with its attr
 * and name caches disabled so every operation reaches the shim.
 */

#include <stdio.h>
#include <string.h>
#include <unistd.h>
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
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

/* ---- the attrstrip shim ------------------------------------------------ */

static uint32_t                  strip_opcode;
static uint64_t                  strip_mask;

static void
strip_set(
    uint32_t opcode,
    uint64_t mask)
{
    strip_opcode = opcode;
    strip_mask   = mask;
} /* strip_set */

/* The engine's completion callback is parked in the last bytes of the
 * request's plugin scratch page for the life of the dispatch.  memfs never
 * touches plugin_data, so the slot is the shim's own. */
#define ATTRSTRIP_SLOT(request)                                 \
        ((uint8_t *) (request)->plugin_data +                   \
         CHIMERA_VFS_PLUGIN_DATA_SIZE -                         \
         sizeof(chimera_vfs_complete_callback_t))

static void
attrstrip_complete(struct chimera_vfs_request *request)
{
    chimera_vfs_complete_callback_t complete;
    struct chimera_vfs_attrs       *r_attr = NULL;

    memcpy(&complete, ATTRSTRIP_SLOT(request), sizeof(complete));
    request->complete = complete;

    if (strip_mask && request->opcode == strip_opcode &&
        request->status == CHIMERA_VFS_OK) {
        switch (request->opcode) {
            case CHIMERA_VFS_OP_OPEN_AT:
                r_attr = &request->open_at.r_attr;
                break;
            case CHIMERA_VFS_OP_LOOKUP_AT:
                r_attr = &request->lookup_at.r_attr;
                break;
            case CHIMERA_VFS_OP_GETATTR:
                r_attr = &request->getattr.r_attr;
                break;
            default:
                break;
        } /* switch */
        if (r_attr) {
            r_attr->va_set_mask &= ~strip_mask;
        }
    }

    complete(request);
} /* attrstrip_complete */

static void
attrstrip_dispatch(
    struct chimera_vfs_request *request,
    void                       *private_data)
{
    memcpy(ATTRSTRIP_SLOT(request), &request->complete,
           sizeof(request->complete));
    request->complete = attrstrip_complete;
    vfs_memfs.dispatch(request, private_data);
} /* attrstrip_dispatch */

/* Completed from vfs_memfs in main() before the VFS is initialized; found by
 * chimera_vfs_init via dlsym("vfs_attrstrip"), which is why the test binary
 * is linked with ENABLE_EXPORTS. */
SYMBOL_EXPORT struct chimera_vfs_module vfs_attrstrip = {
    .sdk_version = CHIMERA_VFS_SDK_VERSION,
    .name        = "attrstrip",
    .fh_magic    = CHIMERA_VFS_FH_MAGIC_MEMFS,
    .dispatch    = attrstrip_dispatch,
};

/* ---- harness ------------------------------------------------------------ */

enum actor {
    ACTOR_ROOT,
    ACTOR_OWNER,
    ACTOR_OTHER,
    ACTOR_WHEEL,
    ACTOR_COUNT
};

static const char *actor_names[ACTOR_COUNT] = {
    "root", "owner", "other", "wheel"
};

struct open_cell {
    const char  *name;
    uint32_t     mode;                /* every cell's file is 1000:1000 */
    enum actor actor;
    unsigned int flags;
    uint32_t     strip_op;
    uint64_t     strip;               /* 0: shim disarmed (a control) */
    enum chimera_vfs_error expect;
};

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

/* Create `name` in `dir` as `cred` (root), owned by 1000:1000 with `mode`;
 * its fh is left in ctx->fh.  Always runs with the shim disarmed. */
static void
make_file(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir,
    const char                     *name,
    uint32_t                        mode)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
        CHIMERA_VFS_ATTR_GID;
    sattr.va_mode = mode;
    sattr.va_uid  = 1000;
    sattr.va_gid  = 1000;

    chimera_vfs_open_at(ctx->vfs_thread, cred, dir, name, strlen(name),
                        CHIMERA_VFS_OPEN_CREATE, &sattr, CHIMERA_VFS_ATTR_FH,
                        0, 0, openat_cb, ctx);
    wait_done(ctx);
    assert(ctx->status == CHIMERA_VFS_OK);
    chimera_vfs_release(ctx->vfs_thread, ctx->handle);
} /* make_file */

/* open_at `name` in `dir` as `cred` with `flags`; return the status. */
static enum chimera_vfs_error
open_at_as(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir,
    const char                     *name,
    unsigned int                    flags)
{
    struct chimera_vfs_attrs sattr;

    memset(&sattr, 0, sizeof(sattr));
    chimera_vfs_open_at(ctx->vfs_thread, cred, dir, name, strlen(name),
                        flags, &sattr, CHIMERA_VFS_ATTR_FH, 0, 0,
                        openat_cb, ctx);
    wait_done(ctx);
    if (ctx->status == CHIMERA_VFS_OK && ctx->handle) {
        chimera_vfs_release(ctx->vfs_thread, ctx->handle);
    }
    return ctx->status;
} /* open_at_as */

static int
check_cell(
    const char             *what,
    const struct open_cell *c,
    enum chimera_vfs_error  got)
{
    if (got == c->expect) {
        return 0;
    }
    fprintf(stderr,
            "  FAIL: %s %s mode %04o as %s flags 0x%x strip op %u mask 0x%llx: "
            "expected %d, got %d\n",
            what, c->name, c->mode, actor_names[c->actor], c->flags,
            c->strip_op, (unsigned long long) c->strip, c->expect, got);
    return 1;
} /* check_cell */

int
main(
    int    argc,
    char **argv)
{
    struct test_ctx                 ctx = { 0 };
    struct chimera_vfs_module_cfg   module_cfgs[2];
    struct prometheus_metrics      *metrics;
    struct chimera_vfs_cred         creds[ACTOR_COUNT];
    struct chimera_vfs_open_handle *mnt, *dir;
    struct chimera_vfs_attrs        sattr;
    uint8_t                         root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        root_fh_len;
    uint8_t                         dir_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        dir_fh_len;
    enum chimera_vfs_error          st;
    unsigned int                    i;
    int                             failures = 0;

    chimera_log_init();

    chimera_vfs_cred_init_unix(&creds[ACTOR_ROOT], 0, 0, 0, NULL);
    chimera_vfs_cred_init_unix(&creds[ACTOR_OWNER], 1000, 1000, 0, NULL);
    chimera_vfs_cred_init_unix(&creds[ACTOR_OTHER], 2000, 2000, 0, NULL);
    chimera_vfs_cred_init_unix(&creds[ACTOR_WHEEL], 3000, 0, 0, NULL);

    /* The shim is memfs in every respect but the reply rewrite: memfs's
     * capabilities (ACL_NATIVE, no DELEGATES_DAC -- the engine is the sole
     * DAC authority) and memfs's lifecycle hooks. */
    vfs_attrstrip.capabilities   = vfs_memfs.capabilities;
    vfs_attrstrip.init           = vfs_memfs.init;
    vfs_attrstrip.destroy        = vfs_memfs.destroy;
    vfs_attrstrip.thread_init    = vfs_memfs.thread_init;
    vfs_attrstrip.thread_destroy = vfs_memfs.thread_destroy;

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(metrics != NULL);

    memset(module_cfgs, 0, sizeof(module_cfgs));
    strncpy(module_cfgs[0].module_name, "attrstrip", sizeof(module_cfgs[0].module_name) - 1);
    strncpy(module_cfgs[1].module_name, "memkv", sizeof(module_cfgs[1].module_name) - 1);

    ctx.evpl = evpl_create(NULL);
    assert(ctx.evpl != NULL);

    /* attr_cache_enabled = 0, name_cache_enabled = 0: no cached (complete)
     * attrs may answer in place of a stripped reply. */
    ctx.vfs = chimera_vfs_init(0, 0, module_cfgs, 2, "memkv", 60, 0, 0, 0, metrics);
    assert(ctx.vfs != NULL);

    ctx.vfs_thread = chimera_vfs_thread_init(ctx.evpl, ctx.vfs);
    assert(ctx.vfs_thread != NULL);

    chimera_vfs_mkfs(ctx.vfs_thread, NULL, "attrstrip", "fs0", NULL,
                     mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_mount(ctx.vfs_thread, NULL, "/test", "attrstrip", "fs0", NULL,
                      mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_get_root_fh(root_fh, &root_fh_len);
    chimera_vfs_lookup(ctx.vfs_thread, &creds[ACTOR_ROOT], root_fh, root_fh_len,
                       "test", 4,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                       lookup_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_open_fh(ctx.vfs_thread, &creds[ACTOR_ROOT], ctx.fh, ctx.fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    mnt = ctx.handle;

    /* A world-writable directory for the cells, so every actor may create
    * and look up in it and only the file's own mode decides each cell. */
    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0777;
    chimera_vfs_mkdir_at(ctx.vfs_thread, &creds[ACTOR_ROOT], mnt, "cells", 5,
                         &sattr, CHIMERA_VFS_ATTR_FH, 0, 0, mkdir_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    memcpy(dir_fh, ctx.fh, ctx.fh_len);
    dir_fh_len = ctx.fh_len;
    chimera_vfs_release(ctx.vfs_thread, mnt);

    chimera_vfs_open_fh(ctx.vfs_thread, &creds[ACTOR_ROOT], dir_fh, dir_fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, openfh_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);
    dir = ctx.handle;

    /*
     * open_at by name, no create -- the shape of an NFSv4 CLAIM_NULL open
     * and of a POSIX openat(2).  The strip applies to the open_at reply.
     */
    {
        /* *INDENT-OFF* */
        static const struct open_cell cells[] = {
            /* Controls: shim disarmed, the gate answers as POSIX says. */
            { "c_ctl_0600",   0600, ACTOR_OTHER, CHIMERA_VFS_OPEN_WRITE_ONLY, 0, 0, CHIMERA_VFS_EACCES },
            { "c_ctl_0066",   0066, ACTOR_OWNER, CHIMERA_VFS_OPEN_WRITE_ONLY, 0, 0, CHIMERA_VFS_EACCES },
            { "c_ctl_0060",   0060, ACTOR_WHEEL, CHIMERA_VFS_OPEN_WRITE_ONLY, 0, 0, CHIMERA_VFS_EACCES },
            /* Each attr the gate reads, withheld in turn: refused, not granted. */
            { "c_mode_0600",  0600, ACTOR_OTHER, CHIMERA_VFS_OPEN_WRITE_ONLY, CHIMERA_VFS_OP_OPEN_AT, CHIMERA_VFS_ATTR_MODE, CHIMERA_VFS_EIO },
            { "c_uid_0066",   0066, ACTOR_OWNER, CHIMERA_VFS_OPEN_WRITE_ONLY, CHIMERA_VFS_OP_OPEN_AT, CHIMERA_VFS_ATTR_UID,  CHIMERA_VFS_EIO },
            { "c_gid_0060",   0060, ACTOR_WHEEL, CHIMERA_VFS_OPEN_WRITE_ONLY, CHIMERA_VFS_OP_OPEN_AT, CHIMERA_VFS_ATTR_GID,  CHIMERA_VFS_EIO },
            { "c_acl_0666",   0666, ACTOR_OTHER, CHIMERA_VFS_OPEN_READ_ONLY,  CHIMERA_VFS_OP_OPEN_AT, CHIMERA_VFS_ATTR_ACL,  CHIMERA_VFS_EIO },
            { "c_trunc_0600", 0600, ACTOR_OTHER, CHIMERA_VFS_OPEN_TRUNCATE,   CHIMERA_VFS_OP_OPEN_AT, CHIMERA_VFS_ATTR_MODE, CHIMERA_VFS_EIO },
            /* Opens the gate does not decide are unaffected by a short reply. */
            { "c_root_0600",  0600, ACTOR_ROOT,  CHIMERA_VFS_OPEN_WRITE_ONLY, CHIMERA_VFS_OP_OPEN_AT, CHIMERA_VFS_ATTR_MODE, CHIMERA_VFS_OK },
            { "c_path_0600",  0600, ACTOR_OTHER, CHIMERA_VFS_OPEN_PATH,       CHIMERA_VFS_OP_OPEN_AT, CHIMERA_VFS_ATTR_MODE, CHIMERA_VFS_OK },
            { "c_infer_0600", 0600, ACTOR_OTHER, CHIMERA_VFS_OPEN_INFERRED | CHIMERA_VFS_OPEN_WRITE_ONLY,
                                                                              CHIMERA_VFS_OP_OPEN_AT, CHIMERA_VFS_ATTR_MODE, CHIMERA_VFS_OK },
        };
        /* *INDENT-ON* */
        struct chimera_vfs_attrs cattr;

        for (i = 0; i < sizeof(cells) / sizeof(cells[0]); i++) {
            const struct open_cell *c = &cells[i];

            make_file(&ctx, &creds[ACTOR_ROOT], dir, c->name, c->mode);
            strip_set(c->strip_op, c->strip);
            st = open_at_as(&ctx, &creds[c->actor], dir, c->name, c->flags);
            strip_set(0, 0);
            failures += check_cell("open_at", c, st);
        }

        /* A file this very open creates needs no authorization, so a reply
         * without MODE must not fail it. */
        memset(&cattr, 0, sizeof(cattr));
        cattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        cattr.va_mode     = 0600;
        strip_set(CHIMERA_VFS_OP_OPEN_AT, CHIMERA_VFS_ATTR_MODE);
        chimera_vfs_open_at(ctx.vfs_thread, &creds[ACTOR_OTHER], dir, "c_new", 5,
                            CHIMERA_VFS_OPEN_CREATE | CHIMERA_VFS_OPEN_WRITE_ONLY,
                            &cattr, CHIMERA_VFS_ATTR_FH, 0, 0, openat_cb, &ctx);
        wait_done(&ctx);
        strip_set(0, 0);
        if (ctx.status != CHIMERA_VFS_OK) {
            fprintf(stderr, "  FAIL: open_at c_new (create, MODE stripped): "
                    "expected 0, got %d\n", ctx.status);
            failures++;
        } else {
            chimera_vfs_release(ctx.vfs_thread, ctx.handle);
        }

        assert(failures == 0);
        TEST_PASS("open_at: a reply without gate attrs is refused, never granted");
    }

    chimera_vfs_release(ctx.vfs_thread, dir);

    chimera_vfs_umount(ctx.vfs_thread, NULL, "/test", mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    for (i = 0; i < 50; i++) {
        chimera_vfs_rmfs(ctx.vfs_thread, NULL, "attrstrip", "fs0", mount_cb, &ctx);
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

    fprintf(stderr, "All VFS attr-gate tests passed!\n");
    return 0;
} /* main */
