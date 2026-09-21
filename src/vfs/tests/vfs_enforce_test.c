// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * End-to-end check that the VFS-layer access gate actually enforces the ACL on
 * an engine-authoritative backend (memfs): the owner of a 0600 file may
 * read/write/chmod it, and a non-owner is denied on read, write (EACCES) and
 * chmod (EPERM, ownership required) -- the cross-protocol-agnostic enforcement
 * the VFS gate provides.
 */

#include <stdio.h>
#include <string.h>
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_compound.h"
/* ...and the core's own per-op header, for the ONE thing below that is not a
 * sequence: the NAME_MAX bound lookup_at applies before it dispatches.  See
 * the over-long-component check in main(). */
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_cred.h"
#include "vfs/sdk/vfs_error.h"
#include "vfs/tests/compound_test_util.h"
#include "common/logging.h"
#include "prometheus-c.h"

#define TEST_PASS(name) fprintf(stderr, "  PASS: %s\n", name)

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

/* The one per-op completion left: see the over-long-component check. */
static void
lookupat_cb(
    enum chimera_vfs_error    error_code,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dir_attr,
    void                     *private_data)
{
    struct test_ctx *ctx = private_data;

    ctx->status = error_code;
    ctx->done   = 1;
} /* lookupat_cb */

/* mkdir `name` under directory handle `dir` as `cred`; return the status. */
static enum chimera_vfs_error
mkdir_as(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir,
    const char                     *name)
{
    struct chimera_vfs_compound *cp;
    struct chimera_vfs_attrs     sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0755;

    cp = chimera_vfs_compound_alloc(ctx->vfs_thread, cred);
    chimera_vfs_compound_add_puthandle(cp, dir, CHIMERA_VFS_OPEN_INFERRED);
    chimera_vfs_compound_add_create(cp, CHIMERA_VFS_COMPOUND_CREATE_DIR,
                                    name, (int) strlen(name), NULL, 0, &sattr,
                                    CHIMERA_VFS_ATTR_FH, 0, 0);

    ctx->status = compound_test_run(ctx->evpl, cp);
    chimera_vfs_compound_free(cp);

    return ctx->status;
} /* mkdir_as */

/* Create file `name` under directory handle `dir` as `cred`; return status. */
static enum chimera_vfs_error
create_as(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir,
    const char                     *name)
{
    struct chimera_vfs_compound *cp;
    struct chimera_vfs_attrs     sattr;

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = 0644;

    cp = chimera_vfs_compound_alloc(ctx->vfs_thread, cred);
    chimera_vfs_compound_add_puthandle(cp, dir, CHIMERA_VFS_OPEN_INFERRED);
    chimera_vfs_compound_add_open(cp, name, (int) strlen(name),
                                  CHIMERA_VFS_OPEN_CREATE, 0, &sattr,
                                  CHIMERA_VFS_ATTR_FH, 0, 0);

    ctx->status = compound_test_run(ctx->evpl, cp);
    /* The handle the OPEN produced is the compound's unless taken, and
     * nothing here wants it: free releases it. */
    chimera_vfs_compound_free(cp);

    return ctx->status;
} /* create_as */

/* Look `name` up in directory handle `dir` as `cred`; return status. */
static enum chimera_vfs_error
lookup_as(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir,
    const char                     *name)
{
    ctx->status = compound_test_lookup(ctx->vfs_thread, ctx->evpl, cred,
                                       dir->fh, dir->fh_len, name,
                                       ctx->fh, &ctx->fh_len);
    return ctx->status;
} /* lookup_as */

/* Remove `name` (whose handle is `child_fh`) from `dir` as `cred`.  The child
 * fh is the recall target remove_at would otherwise resolve for itself. */
static enum chimera_vfs_error
remove_as(
    struct test_ctx                *ctx,
    const struct chimera_vfs_cred  *cred,
    struct chimera_vfs_open_handle *dir,
    const char                     *name,
    const uint8_t                  *child_fh,
    uint32_t                        child_fh_len)
{
    struct chimera_vfs_compound *cp;
    int                          i_remove;

    cp = chimera_vfs_compound_alloc(ctx->vfs_thread, cred);
    chimera_vfs_compound_add_puthandle(cp, dir, CHIMERA_VFS_OPEN_INFERRED);
    i_remove = chimera_vfs_compound_add_remove(cp, name, (int) strlen(name),
                                               0, 0, 0);
    chimera_vfs_compound_op_set_remove_match(cp, (uint32_t) i_remove,
                                             child_fh, child_fh_len, 0, NULL);

    ctx->status = compound_test_run(ctx->evpl, cp);
    chimera_vfs_compound_free(cp);

    return ctx->status;
} /* remove_as */

/* Open a handle for `fh` as `cred`, run a read, return the resulting status.
 * The OPEN is expected to succeed for either credential -- the denial under
 * test is the READ's, not the open's -- so it is asserted separately. */
static enum chimera_vfs_error
read_as(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *fh,
    uint32_t                       fh_len)
{
    struct chimera_vfs_compound *cp;
    struct evpl_iovec            iov[1];

    assert(compound_test_open_fh(ctx->vfs_thread, ctx->evpl, cred, fh, fh_len,
                                 CHIMERA_VFS_OPEN_INFERRED,
                                 &ctx->handle) == CHIMERA_VFS_OK);

    /* A zero-byte read: it is the gate's answer that is under test, not the
     * data.  The descriptor array is the caller's to supply either way. */
    cp = chimera_vfs_compound_alloc(ctx->vfs_thread, cred);
    chimera_vfs_compound_add_read(cp, ctx->handle, 0, 0, iov, 1, 0, NULL,
                                  NULL, 0);

    ctx->status = compound_test_run(ctx->evpl, cp);
    chimera_vfs_compound_free(cp);

    chimera_vfs_release(ctx->vfs_thread, ctx->handle);
    return ctx->status;
} /* read_as */

static enum chimera_vfs_error
write_as(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *fh,
    uint32_t                       fh_len)
{
    struct chimera_vfs_compound *cp;

    assert(compound_test_open_fh(ctx->vfs_thread, ctx->evpl, cred, fh, fh_len,
                                 CHIMERA_VFS_OPEN_INFERRED,
                                 &ctx->handle) == CHIMERA_VFS_OK);

    cp = chimera_vfs_compound_alloc(ctx->vfs_thread, cred);
    chimera_vfs_compound_add_write(cp, ctx->handle, 0, 0, 0, NULL, 0, 0, 0,
                                   NULL);

    ctx->status = compound_test_run(ctx->evpl, cp);
    chimera_vfs_compound_free(cp);

    chimera_vfs_release(ctx->vfs_thread, ctx->handle);
    return ctx->status;
} /* write_as */

static enum chimera_vfs_error
chmod_as(
    struct test_ctx               *ctx,
    const struct chimera_vfs_cred *cred,
    const uint8_t                 *fh,
    uint32_t                       fh_len,
    uint32_t                       mode)
{
    struct chimera_vfs_compound *cp;
    struct chimera_vfs_attrs     sattr;

    assert(compound_test_open_fh(ctx->vfs_thread, ctx->evpl, cred, fh, fh_len,
                                 CHIMERA_VFS_OPEN_INFERRED,
                                 &ctx->handle) == CHIMERA_VFS_OK);

    memset(&sattr, 0, sizeof(sattr));
    sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sattr.va_mode     = mode;

    cp = chimera_vfs_compound_alloc(ctx->vfs_thread, cred);
    chimera_vfs_compound_add_setattr(cp, ctx->handle, &sattr, 0, 0);

    ctx->status = compound_test_run(ctx->evpl, cp);
    chimera_vfs_compound_free(cp);

    chimera_vfs_release(ctx->vfs_thread, ctx->handle);
    return ctx->status;
} /* chmod_as */

int
main(
    int    argc,
    char **argv)
{
    struct test_ctx               ctx = { 0 };
    struct chimera_vfs_module_cfg module_cfgs[2];
    struct prometheus_metrics    *metrics;
    struct chimera_vfs_cred       owner, other;
    struct chimera_vfs_attrs      sattr;
    uint8_t                       root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                      root_fh_len;
    uint8_t                       file_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                      file_fh_len;

    chimera_log_init();

    chimera_vfs_cred_init_unix(&owner, 1000, 1000, 0, NULL);
    chimera_vfs_cred_init_unix(&other, 2000, 2000, 0, NULL);

    metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(metrics != NULL);

    memset(module_cfgs, 0, sizeof(module_cfgs));
    strncpy(module_cfgs[0].module_name, "memfs", sizeof(module_cfgs[0].module_name) - 1);
    strncpy(module_cfgs[1].module_name, "memkv", sizeof(module_cfgs[1].module_name) - 1);

    ctx.evpl = evpl_create(NULL);
    assert(ctx.evpl != NULL);

    ctx.vfs = chimera_vfs_init(0, 0, module_cfgs, 2, "memkv", 60, 1, 1, 0, metrics);
    assert(ctx.vfs != NULL);

    ctx.vfs_thread = chimera_vfs_thread_init(ctx.evpl, ctx.vfs);
    assert(ctx.vfs_thread != NULL);

    /* Create a named filesystem, mount it into the namespace and resolve its
     * root FH. */
    chimera_vfs_mkfs(ctx.vfs_thread, NULL, "memfs", "fs0", NULL,
                     mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    chimera_vfs_mount(ctx.vfs_thread, NULL, "/test", "memfs", "fs0", NULL,
                      mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    assert(compound_test_mount_root(ctx.vfs_thread, ctx.evpl, &owner, "test",
                                    root_fh, &root_fh_len) == CHIMERA_VFS_OK);

    /* Open the root directory (as owner) and create a 0600 file owned by 1000. */
    {
        struct chimera_vfs_open_handle       *root_handle;
        struct chimera_vfs_compound          *cp;
        const struct chimera_vfs_compound_op *op;
        int                                   i_open;

        assert(compound_test_open_fh(ctx.vfs_thread, ctx.evpl, &owner,
                                     root_fh, root_fh_len,
                                     CHIMERA_VFS_OPEN_INFERRED,
                                     &root_handle) == CHIMERA_VFS_OK);

        memset(&sattr, 0, sizeof(sattr));
        sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID | CHIMERA_VFS_ATTR_GID;
        sattr.va_mode     = 0600;
        sattr.va_uid      = 1000;
        sattr.va_gid      = 1000;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &owner);
        chimera_vfs_compound_add_puthandle(cp, root_handle,
                                           CHIMERA_VFS_OPEN_INFERRED);
        i_open = chimera_vfs_compound_add_open(cp, "f", 1,
                                               CHIMERA_VFS_OPEN_CREATE, 0,
                                               &sattr, CHIMERA_VFS_ATTR_FH,
                                               0, 0);

        assert(compound_test_run(ctx.evpl, cp) == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, (uint32_t) i_open);
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_FH);
        memcpy(file_fh, op->attr.va_fh, op->attr.va_fh_len);
        file_fh_len = op->attr.va_fh_len;

        /* The OPEN's handle is the compound's: freeing it releases it. */
        chimera_vfs_compound_free(cp);
        chimera_vfs_release(ctx.vfs_thread, root_handle);
    }

    /* Owner (1000) holds read/write on a 0600 file; a non-owner (2000) does not. */
    assert(read_as(&ctx, &owner, file_fh, file_fh_len) == CHIMERA_VFS_OK);
    assert(read_as(&ctx, &other, file_fh, file_fh_len) == CHIMERA_VFS_EACCES);
    TEST_PASS("read: owner allowed, non-owner denied (0600)");

    assert(write_as(&ctx, &owner, file_fh, file_fh_len) == CHIMERA_VFS_OK);
    assert(write_as(&ctx, &other, file_fh, file_fh_len) == CHIMERA_VFS_EACCES);
    TEST_PASS("write: owner allowed, non-owner denied (0600)");

    /* chmod requires WRITE_ACL: the owner holds it implicitly, a non-owner does
     * not.  POSIX chmod(2) by a non-owner is EPERM (ownership required), not
     * EACCES. */
    assert(chmod_as(&ctx, &other, file_fh, file_fh_len, 0640) == CHIMERA_VFS_EPERM);
    assert(chmod_as(&ctx, &owner, file_fh, file_fh_len, 0640) == CHIMERA_VFS_OK);
    TEST_PASS("chmod: non-owner denied, owner allowed");

    /*
     * Namespace operations on a 0700 directory owned by 1000: the owner may
     * populate and traverse it, a non-owner (2000) is denied at every
     * namespace op -- the same VFS gate every protocol funnels through.
     */
    {
        struct chimera_vfs_open_handle *root_handle, *dir_handle;
        struct chimera_vfs_attrs        dattr;
        uint8_t                         dir_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                        dir_fh_len;
        uint8_t                         kid_fh[CHIMERA_VFS_FH_SIZE];
        uint32_t                        kid_fh_len;

        struct chimera_vfs_compound          *cp;
        const struct chimera_vfs_compound_op *op;
        int                                   i_mkdir;

        assert(compound_test_open_fh(ctx.vfs_thread, ctx.evpl, &owner,
                                     root_fh, root_fh_len,
                                     CHIMERA_VFS_OPEN_INFERRED,
                                     &root_handle) == CHIMERA_VFS_OK);

        memset(&dattr, 0, sizeof(dattr));
        dattr.va_set_mask = CHIMERA_VFS_ATTR_MODE;
        dattr.va_mode     = 0700;

        cp = chimera_vfs_compound_alloc(ctx.vfs_thread, &owner);
        chimera_vfs_compound_add_puthandle(cp, root_handle,
                                           CHIMERA_VFS_OPEN_INFERRED);
        i_mkdir = chimera_vfs_compound_add_create(cp,
                                                  CHIMERA_VFS_COMPOUND_CREATE_DIR,
                                                  "d", 1, NULL, 0, &dattr,
                                                  CHIMERA_VFS_ATTR_FH, 0, 0);

        assert(compound_test_run(ctx.evpl, cp) == CHIMERA_VFS_OK);

        op = chimera_vfs_compound_op(cp, (uint32_t) i_mkdir);
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_FH);
        memcpy(dir_fh, op->attr.va_fh, op->attr.va_fh_len);
        dir_fh_len = op->attr.va_fh_len;

        chimera_vfs_compound_free(cp);
        chimera_vfs_release(ctx.vfs_thread, root_handle);

        assert(compound_test_open_fh(ctx.vfs_thread, ctx.evpl, &owner,
                                     dir_fh, dir_fh_len,
                                     CHIMERA_VFS_OPEN_INFERRED,
                                     &dir_handle) == CHIMERA_VFS_OK);

        assert(mkdir_as(&ctx, &other, dir_handle, "sub") == CHIMERA_VFS_EACCES);
        assert(mkdir_as(&ctx, &owner, dir_handle, "sub") == CHIMERA_VFS_OK);
        TEST_PASS("mkdir: non-owner denied, owner allowed (0700 dir)");

        /* Seed a file (as owner) for the lookup/remove checks below.  NOTE:
         * regular-file creation via open_at is intentionally not VFS-gated
         * (SMB applies its own create-access check; NFS file-create parent
         * enforcement is a documented follow-up), so we do not assert a
         * non-owner create is denied here. */
        assert(create_as(&ctx, &owner, dir_handle, "kid") == CHIMERA_VFS_OK);

        /* lookup needs EXECUTE (search) on the directory. */
        assert(lookup_as(&ctx, &other, dir_handle, "kid") == CHIMERA_VFS_EACCES);
        assert(lookup_as(&ctx, &owner, dir_handle, "kid") == CHIMERA_VFS_OK);
        memcpy(kid_fh, ctx.fh, ctx.fh_len);
        kid_fh_len = ctx.fh_len;
        TEST_PASS("lookup: non-owner denied, owner allowed (0700 dir)");

        /* A component longer than {NAME_MAX} is rejected with ENAMETOOLONG
         * before dispatch -- lookup_at was the sole _at proc missing this bound,
         * letting an attacker-sized NFSv3 LOOKUP name overrun a passthrough
         * backend's fixed request buffer.
         *
         * DELIBERATELY the per-op call and not a sequence.  A sequence cannot
         * reach this bound: the LOOKUP adder refuses a name that will not fit
         * its op and the submission answers EINVAL, so the proc is never
         * dispatched and the check under test never runs.  The bound is the
         * core's own last line of defence, so it is tested where it lives.
         * lookupat_cb exists for this one call. */
        {
            char longname[CHIMERA_VFS_NAME_MAX + 32];

            memset(longname, 'a', sizeof(longname) - 1);
            longname[sizeof(longname) - 1] = '\0';

            chimera_vfs_lookup_at(ctx.vfs_thread, &owner, dir_handle,
                                  longname, (int) strlen(longname),
                                  CHIMERA_VFS_ATTR_FH |
                                  CHIMERA_VFS_ATTR_MASK_STAT, 0,
                                  lookupat_cb, &ctx);
            wait_done(&ctx);
            assert(ctx.status == CHIMERA_VFS_ENAMETOOLONG);

            /* And the sequence in front of it refuses to build one at all, so
             * no consumer can get that far. */
            assert(lookup_as(&ctx, &owner, dir_handle, longname) ==
                   CHIMERA_VFS_EINVAL);
        }
        TEST_PASS("lookup: over-long component rejected (ENAMETOOLONG)");

        assert(remove_as(&ctx, &other, dir_handle, "kid", kid_fh, kid_fh_len) ==
               CHIMERA_VFS_EACCES);
        assert(remove_as(&ctx, &owner, dir_handle, "kid", kid_fh, kid_fh_len) ==
               CHIMERA_VFS_OK);
        TEST_PASS("remove: non-owner denied, owner allowed (0700 dir)");

        chimera_vfs_release(ctx.vfs_thread, dir_handle);
    }

    /*
     * The open-time access gate must authorize the open itself, not merely
     * the per-operation READ/WRITE that follows.  NFSv4 OPEN and SMB CREATE
     * bind I/O rights at open time, and a kernel NFS client defers write
     * authorization entirely to the server's OPEN -- so an open the mode
     * denies has to fail here, with nothing downstream to catch it.
     *
     * Each file is owned 1000:1000, so an open by 2000 lands in the "other"
     * class and only the low mode digit applies to it; the owner cells check
     * the same split from the other side.  Every cell uses a fresh name so no
     * open-handle or attribute cache entry from an earlier cell can satisfy
     * the next one.
     */
    {
        struct chimera_vfs_open_handle *root_handle;
        static const struct {
            const char  *name;
            uint32_t     mode;
            int          as_owner;
            unsigned int flags;
            enum chimera_vfs_error expect;
        }
        /* *INDENT-OFF* */
        cells[] = {
            { "g_oth_w_0000", 0000, 0, CHIMERA_VFS_OPEN_WRITE_ONLY, CHIMERA_VFS_EACCES },
            { "g_oth_r_0000", 0000, 0, CHIMERA_VFS_OPEN_READ_ONLY,  CHIMERA_VFS_EACCES },
            { "g_oth_w_0004", 0004, 0, CHIMERA_VFS_OPEN_WRITE_ONLY, CHIMERA_VFS_EACCES },
            { "g_oth_r_0004", 0004, 0, CHIMERA_VFS_OPEN_READ_ONLY,  CHIMERA_VFS_OK     },
            { "g_oth_w_0002", 0002, 0, CHIMERA_VFS_OPEN_WRITE_ONLY, CHIMERA_VFS_OK     },
            { "g_oth_r_0002", 0002, 0, CHIMERA_VFS_OPEN_READ_ONLY,  CHIMERA_VFS_EACCES },
            { "g_oth_w_0700", 0700, 0, CHIMERA_VFS_OPEN_WRITE_ONLY, CHIMERA_VFS_EACCES },
            { "g_own_w_0400", 0400, 1, CHIMERA_VFS_OPEN_WRITE_ONLY, CHIMERA_VFS_EACCES },
            { "g_own_r_0400", 0400, 1, CHIMERA_VFS_OPEN_READ_ONLY,  CHIMERA_VFS_OK     },
            { "g_own_w_0200", 0200, 1, CHIMERA_VFS_OPEN_WRITE_ONLY, CHIMERA_VFS_OK     },
        };
        /* *INDENT-ON* */
        unsigned int i;
        int          failures = 0;

        chimera_vfs_open_fh(ctx.vfs_thread, &owner, root_fh, root_fh_len,
                            CHIMERA_VFS_OPEN_INFERRED, openfh_cb, &ctx);
        wait_done(&ctx);
        assert(ctx.status == CHIMERA_VFS_OK);
        root_handle = ctx.handle;

        for (i = 0; i < sizeof(cells) / sizeof(cells[0]); i++) {
            const struct chimera_vfs_cred *actor = cells[i].as_owner ? &owner : &other;

            /* Create the cell's file as its owner, with the cell's mode.  A
             * fresh attrs struct per call: the backends rewrite the caller's
             * va_set_mask, so a reused struct no longer describes what the
             * next call is asking for. */
            memset(&sattr, 0, sizeof(sattr));
            sattr.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
                CHIMERA_VFS_ATTR_GID;
            sattr.va_mode = cells[i].mode;
            sattr.va_uid  = 1000;
            sattr.va_gid  = 1000;

            chimera_vfs_open_at(ctx.vfs_thread, &owner, root_handle,
                                cells[i].name, strlen(cells[i].name),
                                CHIMERA_VFS_OPEN_CREATE, &sattr,
                                CHIMERA_VFS_ATTR_FH, 0, 0, openat_cb, &ctx);
            wait_done(&ctx);
            assert(ctx.status == CHIMERA_VFS_OK);
            chimera_vfs_release(ctx.vfs_thread, ctx.handle);

            /* Open it by name with the cell's data-access intent and no create
             * bit -- the shape an NFSv4 CLAIM_NULL / OPEN4_NOCREATE produces. */
            memset(&sattr, 0, sizeof(sattr));
            chimera_vfs_open_at(ctx.vfs_thread, actor, root_handle,
                                cells[i].name, strlen(cells[i].name),
                                cells[i].flags, &sattr,
                                CHIMERA_VFS_ATTR_FH, 0, 0, openat_cb, &ctx);
            wait_done(&ctx);

            if (ctx.status != cells[i].expect) {
                fprintf(stderr,
                        "  FAIL: open_at %s mode %04o as %s flags 0x%x: "
                        "expected %d, got %d\n",
                        cells[i].name, cells[i].mode,
                        cells[i].as_owner ? "owner" : "other",
                        cells[i].flags, cells[i].expect, ctx.status);
                failures++;
            }

            if (ctx.status == CHIMERA_VFS_OK && ctx.handle) {
                chimera_vfs_release(ctx.vfs_thread, ctx.handle);
            }
        }

        chimera_vfs_release(ctx.vfs_thread, root_handle);
        assert(failures == 0);
        TEST_PASS("open_at: mode gates data-access intent at open time");
    }

    /* Unmount and remove the filesystem to exercise the full lifecycle. */
    chimera_vfs_umount(ctx.vfs_thread, NULL, "/test", mount_cb, &ctx);
    wait_done(&ctx);
    assert(ctx.status == CHIMERA_VFS_OK);

    /* umount does not return until every handle on the mount has been
     * closed and released, so the removal below succeeds immediately.  The
     * retry is kept as a backstop only. */
    for (int i = 0; i < 50; i++) {
        chimera_vfs_rmfs(ctx.vfs_thread, NULL, "memfs", "fs0", mount_cb, &ctx);
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

    fprintf(stderr, "All VFS enforcement tests passed!\n");
    return 0;
} /* main */
