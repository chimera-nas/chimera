/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * In-process diskfs driver shared by the diskfs white-box tests: it mounts a
 * diskfs pool on one or more device FILES (the portable "pread" block backend,
 * so it runs off Linux too), drives VFS operations against it synchronously via
 * the evpl loop, and can tear the pool down cleanly OR as a crash (skipping the
 * clean-unmount finalize) and mount it back to run recovery.  It carries no
 * model of its own -- callers (the smoke test, the MBT replay harness) supply
 * the workload and the oracle.
 *
 * Everything is single-threaded and synchronous: each op pumps evpl until its
 * completion fires, so a caller can write straight-line code.
 *
 * The workload reaches diskfs the way a front end reaches it -- as a sequence
 * submitted to the VFS executor.  Every dh_* op below is one sequence, so the
 * backend sees exactly the dispatches a protocol server would make it see, and
 * the straight-line shape the callers want is compound_test_run() pumping the
 * loop until that sequence's single callback has fired.  The pool lifecycle
 * (mkfs / mount / umount / rmfs) is not a sequence and is not one here either.
 */

#ifndef DISKFS_TEST_HARNESS_H
#define DISKFS_TEST_HARNESS_H

#include "common/test_host.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#ifdef _WIN32
#include "common/platform.h"
#else // ifdef _WIN32
#include <unistd.h>
#endif // ifdef _WIN32
#include <fcntl.h>
/* These tests use assert() (and side-effecting calls inside it, as the other
 * in-process VFS tests do) as their oracle, so keep it live even in a Release /
 * NDEBUG build -- otherwise the checks vanish and the assert-only locals become
 * unused/uninitialized (-Werror).  Must precede <assert.h>, which (re)defines
 * assert from NDEBUG at each include. */
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "evpl/evpl_memory.h"
#include "vfs/vfs.h"
#include "vfs/vfs_compound.h"
/* The pool lifecycle -- mkfs, mount, umount, rmfs -- is not a sequence and
 * is not expressible as one.  It comes from the core's per-op header, which
 * is where those four still live. */
#include "vfs/vfs_release.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_cred.h"
#include "vfs/sdk/vfs_error.h"
#include "vfs/tests/compound_test_util.h"
#include "common/logging.h"
#include "prometheus-c.h"

#include "diskfs_test.h"

#define DH_MAX_DEV  8

/* Per-device JSON budget, and the whole-config buffer sized to the compile-time
 * worst case (every device present plus the fixed keys).  Sizing the output
 * buffer this way keeps GCC's -Wformat-truncation (which clang, and thus the
 * macOS build and the clang static-analysis gate, do not run) satisfied that the
 * config snprintf can never truncate. */
#define DH_DEV_JSON 200
#define DH_CFG_MAX  (DH_MAX_DEV * DH_DEV_JSON + 256)

struct dh {
    struct evpl                    *evpl;
    struct chimera_vfs             *vfs;
    struct chimera_vfs_thread      *thread;
    struct prometheus_metrics      *metrics;
    struct chimera_vfs_cred         cred;

    char                            dir[256];   /* session dir for device files */
    int                             ndev;
    uint64_t                        dev_bytes;
    uint64_t                        intent_log_bytes;
    uint32_t                        block_cache_blocks;
    int                             unsafe_async;

    /* per-op completion sync */
    int                             done;
    enum chimera_vfs_error          status;
    uint8_t                         fh[CHIMERA_VFS_FH_SIZE];
    uint32_t                        fh_len;
    uint64_t                        ino;        /* last lookup/getattr inode number */
    struct chimera_vfs_open_handle *handle;

    /* read scratch */
    uint8_t                        *readbuf;
    uint32_t                        readlen;

    /* misc op results */
    int                             auxlen;    /* readlink / get_xattr length */
    uint64_t                        seek_off;
    int                             seek_eof;
    char                            aux[512];  /* readlink target scratch */
};

/* ---- completion plumbing ------------------------------------------------- */

static inline void
dh_wait(struct dh *dh)
{
    while (!dh->done) {
        evpl_continue(dh->evpl);
    }
    dh->done = 0;
} /* dh_wait */

static inline void
dh_mount_cb(
    struct chimera_vfs_thread *thread,
    enum chimera_vfs_error     status,
    void                      *pd)
{
    struct dh *dh = pd;

    dh->status = status;
    dh->done   = 1;
} /* dh_mount_cb */

static inline void
dh_status_cb(
    enum chimera_vfs_error status,
    void                  *pd)
{
    struct dh *dh = pd;

    dh->status = status;
    dh->done   = 1;
} /* dh_status_cb */

/* ---- pool config / lifecycle --------------------------------------------- */

static inline void
dh_build_config(
    struct dh *dh,
    int        initialize,
    char      *out,
    size_t     outlen)
{
    char devs[DH_MAX_DEV * DH_DEV_JSON];
    int  n = 0;
    int  i;

    for (i = 0; i < dh->ndev; i++) {
        n += snprintf(devs + n, sizeof(devs) - n,
                      "%s{\"type\":\"pread\",\"size\":1,\"path\":\"%s/device-%d.img\"}",
                      i ? "," : "", dh->dir, i);
    }

    /* On a remount / recovery mount, "initialize" MUST be omitted entirely --
     * diskfs keys mkfs on the key's PRESENCE, not its value. */
    snprintf(out, outlen,
             "{%s\"unsafe_async\":%s,\"intent_log_size\":%lu,"
             "\"block_cache_blocks\":%u,\"devices\":[%s]}",
             initialize ? "\"initialize\":true," : "",
             dh->unsafe_async ? "true" : "false",
             (unsigned long) dh->intent_log_bytes,
             dh->block_cache_blocks,
             devs);
} /* dh_build_config */

/* Bring the pool up on dh->evpl: create the vfs + thread, mkfs on the initial
 * bringup, then mount "/test" -> "fs0".  initialize=1 formats; initialize=0 is a
 * remount of the existing device files (recovery when they were left !CLEAN). */
static inline void
dh_open_pool(
    struct dh *dh,
    int        initialize)
{
    struct chimera_vfs_module_cfg cfgs[2];
    char                          cfg[DH_CFG_MAX];

    memset(cfgs, 0, sizeof(cfgs));
    dh_build_config(dh, initialize, cfg, sizeof(cfg));
    strncpy(cfgs[0].module_name, "diskfs", sizeof(cfgs[0].module_name) - 1);
    strncpy(cfgs[0].config_data, cfg, sizeof(cfgs[0].config_data) - 1);
    strncpy(cfgs[1].module_name, "memkv", sizeof(cfgs[1].module_name) - 1);

    dh->vfs = chimera_vfs_init(0, 0, cfgs, 2, "memkv", 60, 1, 1, 0, dh->metrics);
    assert(dh->vfs != NULL);
    dh->thread = chimera_vfs_thread_init(dh->evpl, dh->vfs);
    assert(dh->thread != NULL);

    if (initialize) {
        chimera_vfs_mkfs(dh->thread, NULL, "diskfs", "fs0", NULL, dh_mount_cb, dh);
        dh_wait(dh);
        assert(dh->status == CHIMERA_VFS_OK);
    }

    chimera_vfs_mount(dh->thread, NULL, "/test", "diskfs", "fs0", NULL,
                      dh_mount_cb, dh);
    dh_wait(dh);
    assert(dh->status == CHIMERA_VFS_OK);
} /* dh_open_pool */

/* Create the device files + evpl + metrics + cred, then bring the pool up
 * fresh (mkfs).  ndev device files are each sized to dev_bytes. */
static inline void
dh_init(
    struct dh *dh,
    int        ndev,
    uint64_t   dev_bytes,
    uint64_t   intent_log_bytes,
    uint32_t   block_cache_blocks)
{
    char  tmpl[] = "/tmp/diskfs_mbt_XXXXXX";
    char *d;
    int   i;

    memset(dh, 0, sizeof(*dh));
    chimera_log_init();
    chimera_vfs_cred_init_unix(&dh->cred, 0, 0, 0, NULL);

    d = mkdtemp(tmpl);
    assert(d != NULL);
    snprintf(dh->dir, sizeof(dh->dir), "%s", d);

    dh->ndev               = ndev;
    dh->dev_bytes          = dev_bytes;
    dh->intent_log_bytes   = intent_log_bytes;
    dh->block_cache_blocks = block_cache_blocks;
    dh->unsafe_async       = 1;

    for (i = 0; i < ndev; i++) {
        char path[300];
        int  fd, rc;

        snprintf(path, sizeof(path), "%s/device-%d.img", dh->dir, i);
        fd = open(path, O_CREAT | O_TRUNC | O_RDWR, 0644);
        assert(fd >= 0);
        rc = ftruncate(fd, (off_t) dev_bytes);
        assert(rc == 0);
        close(fd);
    }

    dh->metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(dh->metrics != NULL);
    dh->evpl = evpl_create(NULL);
    assert(dh->evpl != NULL);

    dh_open_pool(dh, 1);
} /* dh_init */

/* Attach to an EXISTING device tree (a prior dh's dir), mounting it back
 * (recovery runs if it was left !clean, e.g. after a crash).  Used by the
 * fork/_exit crash probe: the parent attaches to the dir the crashed child
 * created.  Does not create or size any device file. */
static inline void
dh_init_attach(
    struct dh  *dh,
    const char *dir,
    int         ndev,
    uint64_t    dev_bytes,
    uint64_t    intent_log_bytes,
    uint32_t    block_cache_blocks)
{
    memset(dh, 0, sizeof(*dh));
    chimera_log_init();
    chimera_vfs_cred_init_unix(&dh->cred, 0, 0, 0, NULL);
    snprintf(dh->dir, sizeof(dh->dir), "%s", dir);
    dh->ndev               = ndev;
    dh->dev_bytes          = dev_bytes;
    dh->intent_log_bytes   = intent_log_bytes;
    dh->block_cache_blocks = block_cache_blocks;
    dh->unsafe_async       = 1;

    dh->metrics = prometheus_metrics_create(NULL, NULL, 0);
    assert(dh->metrics != NULL);
    dh->evpl = evpl_create(NULL);
    assert(dh->evpl != NULL);

    dh_open_pool(dh, 0);   /* mount existing (initialize omitted) => recovery */
} /* dh_init_attach */

/* Tear the pool down cleanly (umount + thread_destroy + vfs destroy), keeping
 * the evpl and device files so the caller can mount it back. */
static inline void
dh_close_pool_clean(struct dh *dh)
{
    chimera_vfs_umount(dh->thread, NULL, "/test", dh_mount_cb, dh);
    dh_wait(dh);
    assert(dh->status == CHIMERA_VFS_OK);
    chimera_vfs_thread_destroy(dh->thread);
    chimera_vfs_destroy(dh->vfs);
    dh->thread = NULL;
    dh->vfs    = NULL;
} /* dh_close_pool_clean */

/*
 * Tear the pool down as a CRASH: umount (drains handles so the open cache holds
 * no diskfs opens across teardown -- no clean-unmount finalize happens here),
 * thread_destroy (makes every acknowledged write durable in the intent log with
 * no use-after-free), then diskfs_test_crash (module teardown skipping the
 * free-map persist + CLEAN stamp).  The device files are left in the state a
 * crash leaves, so the next dh_open_pool(0) runs intent-log replay recovery.
 */
static inline void
dh_close_pool_crash(struct dh *dh)
{
    chimera_vfs_umount(dh->thread, NULL, "/test", dh_mount_cb, dh);
    dh_wait(dh);
    assert(dh->status == CHIMERA_VFS_OK);
    /* Arm the unsafe (crash) shutdown BEFORE any teardown runs, so every stage
     * -- the request thread's teardown, the reclaim workers, and the intent-log
     * threads -- takes its no-drain/no-flush/no-trim path and the on-disk log is
     * left intact for the next mount to replay. */
    diskfs_test_crash(dh->vfs);
    chimera_vfs_thread_destroy(dh->thread);
    chimera_vfs_destroy(dh->vfs);
    dh->thread = NULL;
    dh->vfs    = NULL;
} /* dh_close_pool_crash */

static inline void
dh_remount_clean(struct dh *dh)
{
    dh_close_pool_clean(dh);
    dh_open_pool(dh, 0);
} /* dh_remount_clean */

static inline void
dh_remount_crash(struct dh *dh)
{
    dh_close_pool_crash(dh);
    dh_open_pool(dh, 0);
} /* dh_remount_crash */

/* Final teardown: clean shutdown + free evpl/metrics + remove device files. */
static inline void
dh_fini(struct dh *dh)
{
    int i;

    if (dh->vfs) {
        dh_close_pool_clean(dh);
    }
    evpl_destroy(dh->evpl);
    prometheus_metrics_destroy(dh->metrics);
    for (i = 0; i < dh->ndev; i++) {
        char path[300];

        snprintf(path, sizeof(path), "%s/device-%d.img", dh->dir, i);
        unlink(path);
    }
    rmdir(dh->dir);
    if (dh->readbuf) {
        free(dh->readbuf);
    }
} /* dh_fini */

/* ---- ops ----------------------------------------------------------------- */

/*
 * Each op below is ONE sequence: allocate, append, run, read the result off
 * the op, free.  An op that acts on a handle the caller already holds names it
 * (the adder's `handle` argument, or chimera_vfs_compound_op_set_handle where
 * the adder has none), which is the executor's in_handle and is exactly what
 * the per-op call took.
 *
 * An op that resolves a NAME takes its directory from the current object, and
 * names it with PUTFH -- the NFS shape, and the one that leaves the trace this
 * harness exists to produce.  Lending the directory with PUTHANDLE instead
 * would be the closer translation of the *_at call it replaces, but these
 * handles are opened INFERRED rather than PATH|DIRECTORY, and a lent handle
 * that does not carry the directory bit costs the executor a GETATTR to ask
 * the object what it is.  A real caller lends a handle it opened AS a
 * directory and pays nothing; this one would pay once per namespace op, and
 * put a dispatch in the trace that no server makes.
 */

/* Root handle of the mounted fs (open handle on the "/test" mount root fh). */
static inline struct chimera_vfs_open_handle *
dh_root_handle(struct dh *dh)
{
    assert(compound_test_mount_root(dh->thread, dh->evpl, &dh->cred, "test",
                                    dh->fh, &dh->fh_len) == CHIMERA_VFS_OK);

    assert(compound_test_open_fh(dh->thread, dh->evpl, &dh->cred,
                                 dh->fh, dh->fh_len,
                                 CHIMERA_VFS_OPEN_INFERRED,
                                 &dh->handle) == CHIMERA_VFS_OK);

    return dh->handle;
} /* dh_root_handle */

/* Resolve name under parent fh; on success fills dh->fh / dh->fh_len / dh->ino. */
static inline enum chimera_vfs_error
dh_lookup(
    struct dh     *dh,
    const uint8_t *parent_fh,
    uint32_t       parent_fhlen,
    const char    *name)
{
    struct chimera_vfs_compound          *cp;
    const struct chimera_vfs_compound_op *op;
    int i_lookup;

    cp = chimera_vfs_compound_alloc(dh->thread, &dh->cred);
    chimera_vfs_compound_add_putfh(cp, parent_fh, (int) parent_fhlen);
    i_lookup = chimera_vfs_compound_add_lookup(cp, name, (int) strlen(name),
                                               CHIMERA_VFS_ATTR_FH |
                                               CHIMERA_VFS_ATTR_MASK_STAT, 0);

    dh->status = compound_test_run(dh->evpl, cp);

    if (dh->status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(cp, (uint32_t) i_lookup);
        if (op->attr.va_set_mask & CHIMERA_VFS_ATTR_FH) {
            memcpy(dh->fh, op->attr.va_fh, op->attr.va_fh_len);
            dh->fh_len = op->attr.va_fh_len;
        }
        if (op->attr.va_set_mask & CHIMERA_VFS_ATTR_INUM) {
            dh->ino = op->attr.va_ino;
        }
    }

    chimera_vfs_compound_free(cp);

    return dh->status;
} /* dh_lookup */

static inline struct chimera_vfs_open_handle *
dh_open_handle(
    struct dh     *dh,
    const uint8_t *fh,
    uint32_t       fhlen)
{
    dh->status = compound_test_open_fh(dh->thread, dh->evpl, &dh->cred,
                                       fh, fhlen, CHIMERA_VFS_OPEN_INFERRED,
                                       &dh->handle);

    return dh->status == CHIMERA_VFS_OK ? dh->handle : NULL;
} /* dh_open_handle */

static inline void
dh_release(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *h)
{
    chimera_vfs_release(dh->thread, h);
} /* dh_release */

/* mkdir; on success copies the new dir's fh into fh/fhlen. */
static inline enum chimera_vfs_error
dh_mkdir(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *dirh,
    const char                     *name,
    uint8_t                        *fh,
    uint32_t                       *fhlen)
{
    struct chimera_vfs_compound          *cp;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_attrs              sa;
    int i_mkdir;

    memset(&sa, 0, sizeof(sa));
    sa.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sa.va_mode     = 0755;

    cp = chimera_vfs_compound_alloc(dh->thread, &dh->cred);
    chimera_vfs_compound_add_putfh(cp, dirh->fh, (int) dirh->fh_len);
    i_mkdir = chimera_vfs_compound_add_create(cp,
                                              CHIMERA_VFS_COMPOUND_CREATE_DIR,
                                              name, (int) strlen(name),
                                              NULL, 0, &sa,
                                              CHIMERA_VFS_ATTR_FH, 0, 0);

    dh->status = compound_test_run(dh->evpl, cp);

    if (dh->status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(cp, (uint32_t) i_mkdir);
        if (op->attr.va_set_mask & CHIMERA_VFS_ATTR_FH) {
            memcpy(dh->fh, op->attr.va_fh, op->attr.va_fh_len);
            dh->fh_len = op->attr.va_fh_len;
        }
        if (fh) {
            memcpy(fh, dh->fh, dh->fh_len);
            *fhlen = dh->fh_len;
        }
    }

    chimera_vfs_compound_free(cp);

    return dh->status;
} /* dh_mkdir */

/* Create a regular file; if keep is non-NULL, keep the open handle there,
 * else release it.  On success copies the file's fh into fh/fhlen if given. */
static inline enum chimera_vfs_error
dh_create(
    struct dh                       *dh,
    struct chimera_vfs_open_handle  *dirh,
    const char                      *name,
    uint8_t                         *fh,
    uint32_t                        *fhlen,
    struct chimera_vfs_open_handle **keep)
{
    struct chimera_vfs_compound          *cp;
    const struct chimera_vfs_compound_op *op;
    struct chimera_vfs_open_handle       *oh;
    struct chimera_vfs_attrs              sa;
    int i_open;

    memset(&sa, 0, sizeof(sa));
    sa.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sa.va_mode     = 0644;

    cp = chimera_vfs_compound_alloc(dh->thread, &dh->cred);
    chimera_vfs_compound_add_putfh(cp, dirh->fh, (int) dirh->fh_len);
    i_open = chimera_vfs_compound_add_open(cp, name, (int) strlen(name),
                                           CHIMERA_VFS_OPEN_CREATE, 0, &sa,
                                           CHIMERA_VFS_ATTR_FH, 0, 0);

    dh->status = compound_test_run(dh->evpl, cp);

    if (dh->status != CHIMERA_VFS_OK) {
        chimera_vfs_compound_free(cp);
        return dh->status;
    }

    op = chimera_vfs_compound_op(cp, (uint32_t) i_open);
    if (op->attr.va_set_mask & CHIMERA_VFS_ATTR_FH) {
        memcpy(dh->fh, op->attr.va_fh, op->attr.va_fh_len);
        dh->fh_len = op->attr.va_fh_len;
    }

    oh         = chimera_vfs_compound_take_handle(cp, (uint32_t) i_open);
    dh->handle = oh;
    chimera_vfs_compound_free(cp);

    if (fh) {
        memcpy(fh, dh->fh, dh->fh_len);
        *fhlen = dh->fh_len;
    }
    if (keep) {
        *keep = oh;
    } else {
        dh_release(dh, oh);
    }

    return CHIMERA_VFS_OK;
} /* dh_create */

static inline enum chimera_vfs_error
dh_remove(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *dirh,
    const char                     *name)
{
    struct chimera_vfs_compound *cp;

    cp = chimera_vfs_compound_alloc(dh->thread, &dh->cred);
    chimera_vfs_compound_add_putfh(cp, dirh->fh, (int) dirh->fh_len);
    chimera_vfs_compound_add_remove(cp, name, (int) strlen(name), 0, 0, 0);

    dh->status = compound_test_run(dh->evpl, cp);

    chimera_vfs_compound_free(cp);

    return dh->status;
} /* dh_remove */

static inline enum chimera_vfs_error
dh_write(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *h,
    uint64_t                        off,
    uint32_t                        len,
    uint8_t                         byte)
{
    struct chimera_vfs_compound *cp;
    struct evpl_iovec            iov[16];
    int niov, i;

    niov = evpl_iovec_alloc(dh->evpl, len, 4096, 16, 0, iov);
    assert(niov > 0);
    for (i = 0; i < niov; i++) {
        memset(evpl_iovec_data(&iov[i]), byte, evpl_iovec_length(&iov[i]));
    }

    cp = chimera_vfs_compound_alloc(dh->thread, &dh->cred);
    chimera_vfs_compound_add_write(cp, h, off, len,
                                   CHIMERA_VFS_WRITE_FILESYNC, iov, niov,
                                   0, 0, NULL);

    dh->status = compound_test_run(dh->evpl, cp);

    chimera_vfs_compound_free(cp);
    evpl_iovecs_release(dh->evpl, iov, niov);

    return dh->status;
} /* dh_write */

/* Read [off,off+len) into buf (which must hold len bytes); holes read as 0. */
static inline enum chimera_vfs_error
dh_read(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *h,
    uint64_t                        off,
    uint32_t                        len,
    uint8_t                        *buf)
{
    struct chimera_vfs_compound          *cp;
    const struct chimera_vfs_compound_op *op;
    struct evpl_iovec                     iov[64];
    struct evpl_iovec                    *got;
    uint32_t copied = 0;
    int i_read, ngot, i;

    if (buf) {
        memset(buf, 0, len);
    }

    cp     = chimera_vfs_compound_alloc(dh->thread, &dh->cred);
    i_read = chimera_vfs_compound_add_read(cp, h, off, len, iov, 64, 0, NULL,
                                           NULL, 0);

    dh->status = compound_test_run(dh->evpl, cp);

    if (dh->status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(cp, (uint32_t) i_read);

        for (i = 0; i < op->niov && copied < len; i++) {
            uint32_t n = evpl_iovec_length(&op->iov[i]);

            if (n > len - copied) {
                n = len - copied;
            }
            if (buf) {
                memcpy(buf + copied, evpl_iovec_data(&op->iov[i]), n);
            }
            copied += n;
        }

        /* Take the data references before the compound is freed, and release
         * them here -- the copy above is all this harness wanted of them. */
        chimera_vfs_compound_take_iov(cp, (uint32_t) i_read, &got, &ngot);
        if (ngot) {
            evpl_iovecs_release(dh->evpl, got, ngot);
        }
    }

    chimera_vfs_compound_free(cp);

    return dh->status;
} /* dh_read */

static inline enum chimera_vfs_error
dh_truncate(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *h,
    uint64_t                        size)
{
    struct chimera_vfs_compound *cp;
    struct chimera_vfs_attrs     sa;

    memset(&sa, 0, sizeof(sa));
    sa.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
    sa.va_size     = size;

    cp = chimera_vfs_compound_alloc(dh->thread, &dh->cred);
    chimera_vfs_compound_add_setattr(cp, h, &sa, 0, 0);

    dh->status = compound_test_run(dh->evpl, cp);

    chimera_vfs_compound_free(cp);

    return dh->status;
} /* dh_truncate */

/* ---- extended ops: namespace / attr / clone / io variety ----------------- */

/* rename fromdir/name -> todir/new_name (fh-based). */
static inline enum chimera_vfs_error
dh_rename(
    struct dh     *dh,
    const uint8_t *fromdir_fh,
    uint32_t       fromdir_fhlen,
    const char    *name,
    const uint8_t *todir_fh,
    uint32_t       todir_fhlen,
    const char    *new_name)
{
    struct chimera_vfs_compound *cp;

    /* RENAME renames out of the SAVED object into the current one. */
    cp = chimera_vfs_compound_alloc(dh->thread, &dh->cred);
    chimera_vfs_compound_add_putfh(cp, fromdir_fh, (int) fromdir_fhlen);
    chimera_vfs_compound_add_savefh(cp);
    chimera_vfs_compound_add_putfh(cp, todir_fh, (int) todir_fhlen);
    chimera_vfs_compound_add_rename(cp, name, (int) strlen(name),
                                    new_name, (int) strlen(new_name), 0, 0, 0);

    dh->status = compound_test_run(dh->evpl, cp);

    chimera_vfs_compound_free(cp);

    return dh->status;
} /* dh_rename */

/* hardlink target_fh into dir_fh as name. */
static inline enum chimera_vfs_error
dh_link(
    struct dh     *dh,
    const uint8_t *target_fh,
    uint32_t       target_fhlen,
    const uint8_t *dir_fh,
    uint32_t       dir_fhlen,
    const char    *name)
{
    struct chimera_vfs_compound *cp;

    /* LINK links the SAVED object into the current one. */
    cp = chimera_vfs_compound_alloc(dh->thread, &dh->cred);
    chimera_vfs_compound_add_putfh(cp, target_fh, (int) target_fhlen);
    chimera_vfs_compound_add_savefh(cp);
    chimera_vfs_compound_add_putfh(cp, dir_fh, (int) dir_fhlen);
    chimera_vfs_compound_add_link(cp, name, (int) strlen(name), 0, 0, 0);

    dh->status = compound_test_run(dh->evpl, cp);

    chimera_vfs_compound_free(cp);

    return dh->status;
} /* dh_link */

/* create a symlink name -> target under dir handle; fills dh->fh with its fh. */
static inline enum chimera_vfs_error
dh_symlink(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *dirh,
    const char                     *name,
    const char                     *target)
{
    struct chimera_vfs_compound          *cp;
    const struct chimera_vfs_compound_op *op;
    int i_sym;

    cp = chimera_vfs_compound_alloc(dh->thread, &dh->cred);
    chimera_vfs_compound_add_putfh(cp, dirh->fh, (int) dirh->fh_len);
    i_sym = chimera_vfs_compound_add_create(cp,
                                            CHIMERA_VFS_COMPOUND_CREATE_SYMLINK,
                                            name, (int) strlen(name),
                                            target, (int) strlen(target), NULL,
                                            CHIMERA_VFS_ATTR_FH, 0, 0);

    dh->status = compound_test_run(dh->evpl, cp);

    if (dh->status == CHIMERA_VFS_OK) {
        op = chimera_vfs_compound_op(cp, (uint32_t) i_sym);
        if (op->attr.va_set_mask & CHIMERA_VFS_ATTR_FH) {
            memcpy(dh->fh, op->attr.va_fh, op->attr.va_fh_len);
            dh->fh_len = op->attr.va_fh_len;
        }
    }

    chimera_vfs_compound_free(cp);

    return dh->status;
} /* dh_symlink */

/* readlink an open symlink handle into dh->aux (dh->auxlen = length). */
static inline enum chimera_vfs_error
dh_readlink(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *h)
{
    struct chimera_vfs_compound          *cp;
    const struct chimera_vfs_compound_op *op;
    int i_rl;

    dh->auxlen = 0;

    cp   = chimera_vfs_compound_alloc(dh->thread, &dh->cred);
    i_rl = chimera_vfs_compound_add_readlink(cp);
    chimera_vfs_compound_op_set_handle(cp, (uint32_t) i_rl, h);

    dh->status = compound_test_run(dh->evpl, cp);

    if (dh->status == CHIMERA_VFS_OK) {
        op         = chimera_vfs_compound_op(cp, (uint32_t) i_rl);
        dh->auxlen = (int) op->target_len;
        if (dh->auxlen > (int) sizeof(dh->aux) - 1) {
            dh->auxlen = (int) sizeof(dh->aux) - 1;
        }
        memcpy(dh->aux, op->target, (size_t) dh->auxlen);
    }

    chimera_vfs_compound_free(cp);

    return dh->status;
} /* dh_readlink */

/* set metadata attrs (mode/uid/gid/atime/mtime) on an open handle. */
static inline enum chimera_vfs_error
dh_setattr_meta(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *h,
    uint32_t                        mode,
    uint32_t                        uid,
    uint32_t                        gid)
{
    struct chimera_vfs_compound *cp;
    struct chimera_vfs_attrs     sa;

    memset(&sa, 0, sizeof(sa));
    sa.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
        CHIMERA_VFS_ATTR_GID;
    sa.va_mode = mode;
    sa.va_uid  = uid;
    sa.va_gid  = gid;

    cp = chimera_vfs_compound_alloc(dh->thread, &dh->cred);
    chimera_vfs_compound_add_setattr(cp, h, &sa, 0, 0);

    dh->status = compound_test_run(dh->evpl, cp);

    chimera_vfs_compound_free(cp);

    return dh->status;
} /* dh_setattr_meta */

static inline enum chimera_vfs_error
dh_set_xattr(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *h,
    const char                     *name,
    const char                     *value)
{
    struct chimera_vfs_compound *cp;
    int i_sx;

    cp   = chimera_vfs_compound_alloc(dh->thread, &dh->cred);
    i_sx = chimera_vfs_compound_add_setxattr(cp, 0, name, (int) strlen(name),
                                             value, (uint32_t) strlen(value));
    chimera_vfs_compound_op_set_handle(cp, (uint32_t) i_sx, h);

    dh->status = compound_test_run(dh->evpl, cp);

    chimera_vfs_compound_free(cp);

    return dh->status;
} /* dh_set_xattr */

static inline enum chimera_vfs_error
dh_get_xattr(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *h,
    const char                     *name)
{
    struct chimera_vfs_compound          *cp;
    const struct chimera_vfs_compound_op *op;
    int i_gx;

    dh->auxlen = 0;

    cp   = chimera_vfs_compound_alloc(dh->thread, &dh->cred);
    i_gx = chimera_vfs_compound_add_getxattr(cp, name, (int) strlen(name), 256);
    chimera_vfs_compound_op_set_handle(cp, (uint32_t) i_gx, h);

    dh->status = compound_test_run(dh->evpl, cp);

    if (dh->status == CHIMERA_VFS_OK) {
        op         = chimera_vfs_compound_op(cp, (uint32_t) i_gx);
        dh->auxlen = (int) op->buffer_len;
    }

    chimera_vfs_compound_free(cp);

    return dh->status;
} /* dh_get_xattr */

static inline enum chimera_vfs_error
dh_remove_xattr(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *h,
    const char                     *name)
{
    struct chimera_vfs_compound *cp;
    int i_rx;

    cp   = chimera_vfs_compound_alloc(dh->thread, &dh->cred);
    i_rx = chimera_vfs_compound_add_removexattr(cp, name, (int) strlen(name));
    chimera_vfs_compound_op_set_handle(cp, (uint32_t) i_rx, h);

    dh->status = compound_test_run(dh->evpl, cp);

    chimera_vfs_compound_free(cp);

    return dh->status;
} /* dh_remove_xattr */

/* reflink [src_off,+len) of src into dst at dst_off. */
static inline enum chimera_vfs_error
dh_clone(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *src,
    uint64_t                        src_off,
    struct chimera_vfs_open_handle *dst,
    uint64_t                        dst_off,
    uint64_t                        len)
{
    struct chimera_vfs_compound *cp;

    cp = chimera_vfs_compound_alloc(dh->thread, &dh->cred);
    chimera_vfs_compound_add_clone_range(cp, src, src_off, dst, dst_off, len,
                                         0, 0);

    dh->status = compound_test_run(dh->evpl, cp);

    chimera_vfs_compound_free(cp);

    return dh->status;
} /* dh_clone */

/* allocate/deallocate a range (flags: 0 = allocate,
 * CHIMERA_VFS_ALLOCATE_DEALLOCATE = punch). */
static inline enum chimera_vfs_error
dh_allocate(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *h,
    uint64_t                        off,
    uint64_t                        len,
    uint32_t                        flags)
{
    struct chimera_vfs_compound *cp;

    cp = chimera_vfs_compound_alloc(dh->thread, &dh->cred);
    chimera_vfs_compound_add_allocate(cp, h, off, len, flags, 0, 0);

    dh->status = compound_test_run(dh->evpl, cp);

    chimera_vfs_compound_free(cp);

    return dh->status;
} /* dh_allocate */

/* SEEK_DATA/SEEK_HOLE (what) from offset; result in dh->seek_off/seek_eof. */
static inline enum chimera_vfs_error
dh_seek(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *h,
    uint64_t                        off,
    uint32_t                        what)
{
    struct chimera_vfs_compound          *cp;
    const struct chimera_vfs_compound_op *op;
    int i_seek;

    cp     = chimera_vfs_compound_alloc(dh->thread, &dh->cred);
    i_seek = chimera_vfs_compound_add_seek(cp, h, off, what);

    dh->status = compound_test_run(dh->evpl, cp);

    if (dh->status == CHIMERA_VFS_OK) {
        op           = chimera_vfs_compound_op(cp, (uint32_t) i_seek);
        dh->seek_off = op->seek_offset;
        dh->seek_eof = (int) op->seek_eof;
    }

    chimera_vfs_compound_free(cp);

    return dh->status;
} /* dh_seek */

#endif /* DISKFS_TEST_HARNESS_H */
