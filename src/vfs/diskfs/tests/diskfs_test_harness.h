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
 */

#ifndef DISKFS_TEST_HARNESS_H
#define DISKFS_TEST_HARNESS_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
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
#include "vfs/vfs_procs.h"
#include "vfs/vfs_release.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_cred.h"
#include "vfs/sdk/vfs_error.h"
#include "common/logging.h"
#include "prometheus-c.h"

#include "diskfs_test.h"

#define DH_MAX_DEV 8

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

static inline void
dh_lookup_cb(
    enum chimera_vfs_error    status,
    struct chimera_vfs_attrs *attr,
    void                     *pd)
{
    struct dh *dh = pd;

    dh->status = status;
    if (status == CHIMERA_VFS_OK) {
        if (attr->va_set_mask & CHIMERA_VFS_ATTR_FH) {
            memcpy(dh->fh, attr->va_fh, attr->va_fh_len);
            dh->fh_len = attr->va_fh_len;
        }
        if (attr->va_set_mask & CHIMERA_VFS_ATTR_INUM) {
            dh->ino = attr->va_ino;
        }
    }
    dh->done = 1;
} /* dh_lookup_cb */

static inline void
dh_openfh_cb(
    enum chimera_vfs_error          status,
    struct chimera_vfs_open_handle *oh,
    void                           *pd)
{
    struct dh *dh = pd;

    dh->status = status;
    dh->handle = oh;
    dh->done   = 1;
} /* dh_openfh_cb */

static inline void
dh_openat_cb(
    enum chimera_vfs_error          status,
    struct chimera_vfs_open_handle *oh,
    struct chimera_vfs_attrs       *set_attr,
    struct chimera_vfs_attrs       *attr,
    struct chimera_vfs_attrs       *dpre,
    struct chimera_vfs_attrs       *dpost,
    void                           *pd)
{
    struct dh *dh = pd;

    dh->status = status;
    dh->handle = oh;
    if (status == CHIMERA_VFS_OK && attr && (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        memcpy(dh->fh, attr->va_fh, attr->va_fh_len);
        dh->fh_len = attr->va_fh_len;
    }
    dh->done = 1;
} /* dh_openat_cb */

static inline void
dh_mkdirat_cb(
    enum chimera_vfs_error    status,
    struct chimera_vfs_attrs *set_attr,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dpre,
    struct chimera_vfs_attrs *dpost,
    void                     *pd)
{
    struct dh *dh = pd;

    dh->status = status;
    if (status == CHIMERA_VFS_OK && attr && (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        memcpy(dh->fh, attr->va_fh, attr->va_fh_len);
        dh->fh_len = attr->va_fh_len;
    }
    dh->done = 1;
} /* dh_mkdirat_cb */

static inline void
dh_removeat_cb(
    enum chimera_vfs_error    status,
    struct chimera_vfs_attrs *pre,
    struct chimera_vfs_attrs *post,
    void                     *pd)
{
    struct dh *dh = pd;

    dh->status = status;
    dh->done   = 1;
} /* dh_removeat_cb */

static inline void
dh_setattr_cb(
    enum chimera_vfs_error    status,
    struct chimera_vfs_attrs *pre,
    struct chimera_vfs_attrs *set,
    struct chimera_vfs_attrs *post,
    void                     *pd)
{
    struct dh *dh = pd;

    dh->status = status;
    dh->done   = 1;
} /* dh_setattr_cb */

static inline void
dh_write_cb(
    enum chimera_vfs_error    status,
    uint32_t                  length,
    uint32_t                  sync,
    struct chimera_vfs_attrs *pre,
    struct chimera_vfs_attrs *post,
    void                     *pd)
{
    struct dh *dh = pd;

    dh->status = status;
    dh->done   = 1;
} /* dh_write_cb */

static inline void
dh_read_cb(
    enum chimera_vfs_error    status,
    uint32_t                  count,
    uint32_t                  eof,
    struct evpl_iovec        *iov,
    int                       niov,
    struct chimera_vfs_attrs *attr,
    void                     *pd)
{
    struct dh *dh  = pd;
    uint32_t   off = 0;
    int        i;

    dh->status = status;
    if (status == CHIMERA_VFS_OK) {
        for (i = 0; i < niov && off < dh->readlen; i++) {
            uint32_t len = evpl_iovec_length(&iov[i]);

            if (len > dh->readlen - off) {
                len = dh->readlen - off;
            }
            if (dh->readbuf) {
                memcpy(dh->readbuf + off, evpl_iovec_data(&iov[i]), len);
            }
            off += len;
        }
        evpl_iovecs_release(dh->evpl, iov, niov);
    }
    dh->done = 1;
} /* dh_read_cb */

/* ---- pool config / lifecycle --------------------------------------------- */

static inline void
dh_build_config(
    struct dh *dh,
    int        initialize,
    char      *out,
    size_t     outlen)
{
    char devs[DH_MAX_DEV * 200];
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
    char                          cfg[1024];

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

/* Root handle of the mounted fs (open handle on the "/test" mount root fh). */
static inline struct chimera_vfs_open_handle *
dh_root_handle(struct dh *dh)
{
    uint8_t  root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t root_len;

    chimera_vfs_get_root_fh(root_fh, &root_len);
    chimera_vfs_lookup(dh->thread, &dh->cred, root_fh, root_len, "test", 4,
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                       dh_lookup_cb, dh);
    dh_wait(dh);
    assert(dh->status == CHIMERA_VFS_OK);

    chimera_vfs_open_fh(dh->thread, &dh->cred, dh->fh, dh->fh_len,
                        CHIMERA_VFS_OPEN_INFERRED, dh_openfh_cb, dh);
    dh_wait(dh);
    assert(dh->status == CHIMERA_VFS_OK);
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
    chimera_vfs_lookup(dh->thread, &dh->cred, parent_fh, parent_fhlen,
                       name, (int) strlen(name),
                       CHIMERA_VFS_ATTR_FH | CHIMERA_VFS_ATTR_MASK_STAT, 0,
                       dh_lookup_cb, dh);
    dh_wait(dh);
    return dh->status;
} /* dh_lookup */

static inline struct chimera_vfs_open_handle *
dh_open_handle(
    struct dh     *dh,
    const uint8_t *fh,
    uint32_t       fhlen)
{
    chimera_vfs_open_fh(dh->thread, &dh->cred, fh, fhlen,
                        CHIMERA_VFS_OPEN_INFERRED, dh_openfh_cb, dh);
    dh_wait(dh);
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
    struct chimera_vfs_attrs sa;

    memset(&sa, 0, sizeof(sa));
    sa.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sa.va_mode     = 0755;

    chimera_vfs_mkdir_at(dh->thread, &dh->cred, dirh, name, (int) strlen(name),
                         &sa, CHIMERA_VFS_ATTR_FH, 0, 0, dh_mkdirat_cb, dh);
    dh_wait(dh);
    if (dh->status == CHIMERA_VFS_OK && fh) {
        memcpy(fh, dh->fh, dh->fh_len);
        *fhlen = dh->fh_len;
    }
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
    struct chimera_vfs_attrs sa;

    memset(&sa, 0, sizeof(sa));
    sa.va_set_mask = CHIMERA_VFS_ATTR_MODE;
    sa.va_mode     = 0644;

    chimera_vfs_open_at(dh->thread, &dh->cred, dirh, name, (int) strlen(name),
                        CHIMERA_VFS_OPEN_CREATE, &sa, CHIMERA_VFS_ATTR_FH, 0, 0,
                        dh_openat_cb, dh);
    dh_wait(dh);
    if (dh->status != CHIMERA_VFS_OK) {
        return dh->status;
    }
    if (fh) {
        memcpy(fh, dh->fh, dh->fh_len);
        *fhlen = dh->fh_len;
    }
    if (keep) {
        *keep = dh->handle;
    } else {
        dh_release(dh, dh->handle);
    }
    return CHIMERA_VFS_OK;
} /* dh_create */

static inline enum chimera_vfs_error
dh_remove(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *dirh,
    const char                     *name)
{
    chimera_vfs_remove_at(dh->thread, &dh->cred, dirh, name, (int) strlen(name),
                          NULL, 0, 0, 0, 0, NULL, dh_removeat_cb, dh);
    dh_wait(dh);
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
    struct evpl_iovec iov[16];
    int niov, i;

    niov = evpl_iovec_alloc(dh->evpl, len, 4096, 16, 0, iov);
    assert(niov > 0);
    for (i = 0; i < niov; i++) {
        memset(evpl_iovec_data(&iov[i]), byte, evpl_iovec_length(&iov[i]));
    }
    chimera_vfs_write(dh->thread, &dh->cred, h, off, len,
                      CHIMERA_VFS_WRITE_FILESYNC, 0, 0, iov, niov, dh_write_cb, dh);
    dh_wait(dh);
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
    struct evpl_iovec iov[64];

    if (buf) {
        memset(buf, 0, len);
    }
    dh->readbuf = buf;
    dh->readlen = len;
    chimera_vfs_read(dh->thread, &dh->cred, h, off, len, iov, 64, 0, dh_read_cb, dh);
    dh_wait(dh);
    dh->readbuf = NULL;
    return dh->status;
} /* dh_read */

static inline enum chimera_vfs_error
dh_truncate(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *h,
    uint64_t                        size)
{
    struct chimera_vfs_attrs sa;

    memset(&sa, 0, sizeof(sa));
    sa.va_set_mask = CHIMERA_VFS_ATTR_SIZE;
    sa.va_size     = size;
    chimera_vfs_setattr(dh->thread, &dh->cred, h, &sa, 0, 0, dh_setattr_cb, dh);
    dh_wait(dh);
    return dh->status;
} /* dh_truncate */

/* ---- extended ops: namespace / attr / clone / io variety ----------------- */

static inline void
dh_symlinkat_cb(
    enum chimera_vfs_error    status,
    struct chimera_vfs_attrs *attr,
    struct chimera_vfs_attrs *dpre,
    struct chimera_vfs_attrs *dpost,
    void                     *pd)
{
    struct dh *dh = pd;

    dh->status = status;
    if (status == CHIMERA_VFS_OK && attr && (attr->va_set_mask & CHIMERA_VFS_ATTR_FH)) {
        memcpy(dh->fh, attr->va_fh, attr->va_fh_len);
        dh->fh_len = attr->va_fh_len;
    }
    dh->done = 1;
} /* dh_symlinkat_cb */

static inline void
dh_readlink_cb(
    enum chimera_vfs_error    status,
    int                       targetlen,
    struct chimera_vfs_attrs *attr,
    void                     *pd)
{
    struct dh *dh = pd;

    dh->status = status;
    dh->auxlen = targetlen;
    dh->done   = 1;
} /* dh_readlink_cb */

static inline void
dh_rename_cb(
    enum chimera_vfs_error    status,
    struct chimera_vfs_attrs *a,
    struct chimera_vfs_attrs *b,
    struct chimera_vfs_attrs *c,
    struct chimera_vfs_attrs *d,
    void                     *pd)
{
    struct dh *dh = pd;

    dh->status = status;
    dh->done   = 1;
} /* dh_rename_cb */

static inline void
dh_link_cb(
    enum chimera_vfs_error    status,
    struct chimera_vfs_attrs *a,
    struct chimera_vfs_attrs *b,
    struct chimera_vfs_attrs *c,
    void                     *pd)
{
    struct dh *dh = pd;

    dh->status = status;
    dh->done   = 1;
} /* dh_link_cb */

static inline void
dh_clone_cb(
    enum chimera_vfs_error    status,
    struct chimera_vfs_attrs *pre,
    struct chimera_vfs_attrs *post,
    void                     *pd)
{
    struct dh *dh = pd;

    dh->status = status;
    dh->done   = 1;
} /* dh_clone_cb */

static inline void
dh_seek_cb(
    enum chimera_vfs_error status,
    int                    eof,
    uint64_t               offset,
    void                  *pd)
{
    struct dh *dh = pd;

    dh->status   = status;
    dh->seek_eof = eof;
    dh->seek_off = offset;
    dh->done     = 1;
} /* dh_seek_cb */

static inline void
dh_xattr_status_cb(
    enum chimera_vfs_error          status,
    const struct chimera_vfs_attrs *pre,
    const struct chimera_vfs_attrs *post,
    void                           *pd)
{
    struct dh *dh = pd;

    dh->status = status;
    dh->done   = 1;
} /* dh_xattr_status_cb */

static inline void
dh_getxattr_cb(
    enum chimera_vfs_error status,
    uint32_t               value_len,
    void                  *pd)
{
    struct dh *dh = pd;

    dh->status = status;
    dh->auxlen = (int) value_len;
    dh->done   = 1;
} /* dh_getxattr_cb */

static inline void
dh_allocate_cb2(
    enum chimera_vfs_error    status,
    struct chimera_vfs_attrs *pre,
    struct chimera_vfs_attrs *post,
    void                     *pd)
{
    struct dh *dh = pd;

    dh->status = status;
    dh->done   = 1;
} /* dh_allocate_cb2 */

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
    chimera_vfs_rename_at(dh->thread, &dh->cred, fromdir_fh, fromdir_fhlen,
                          name, (int) strlen(name),
                          todir_fh, todir_fhlen, new_name, (int) strlen(new_name),
                          NULL, 0, 0, 0, 0, NULL, NULL, dh_rename_cb, dh);
    dh_wait(dh);
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
    chimera_vfs_link_at(dh->thread, &dh->cred, target_fh, target_fhlen,
                        dir_fh, dir_fhlen, name, (int) strlen(name), 0,
                        0, 0, 0, NULL, NULL, dh_link_cb, dh);
    dh_wait(dh);
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
    chimera_vfs_symlink_at(dh->thread, &dh->cred, dirh, name, (int) strlen(name),
                           target, (int) strlen(target), NULL,
                           CHIMERA_VFS_ATTR_FH, 0, 0, dh_symlinkat_cb, dh);
    dh_wait(dh);
    return dh->status;
} /* dh_symlink */

/* readlink an open symlink handle into dh->aux (dh->auxlen = length). */
static inline enum chimera_vfs_error
dh_readlink(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *h)
{
    dh->auxlen = 0;
    chimera_vfs_readlink(dh->thread, &dh->cred, h, dh->aux, sizeof(dh->aux) - 1,
                         0, dh_readlink_cb, dh);
    dh_wait(dh);
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
    struct chimera_vfs_attrs sa;

    memset(&sa, 0, sizeof(sa));
    sa.va_set_mask = CHIMERA_VFS_ATTR_MODE | CHIMERA_VFS_ATTR_UID |
        CHIMERA_VFS_ATTR_GID;
    sa.va_mode = mode;
    sa.va_uid  = uid;
    sa.va_gid  = gid;
    chimera_vfs_setattr(dh->thread, &dh->cred, h, &sa, 0, 0, dh_setattr_cb, dh);
    dh_wait(dh);
    return dh->status;
} /* dh_setattr_meta */

static inline enum chimera_vfs_error
dh_set_xattr(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *h,
    const char                     *name,
    const char                     *value)
{
    chimera_vfs_set_xattr(dh->thread, &dh->cred, h, 0, name, (uint32_t) strlen(name),
                          value, (uint32_t) strlen(value), dh_xattr_status_cb, dh);
    dh_wait(dh);
    return dh->status;
} /* dh_set_xattr */

static inline enum chimera_vfs_error
dh_get_xattr(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *h,
    const char                     *name)
{
    char buf[256];

    dh->auxlen = 0;
    chimera_vfs_get_xattr(dh->thread, &dh->cred, h, name, (uint32_t) strlen(name),
                          buf, sizeof(buf), dh_getxattr_cb, dh);
    dh_wait(dh);
    return dh->status;
} /* dh_get_xattr */

static inline enum chimera_vfs_error
dh_remove_xattr(
    struct dh                      *dh,
    struct chimera_vfs_open_handle *h,
    const char                     *name)
{
    chimera_vfs_remove_xattr(dh->thread, &dh->cred, h, name, (uint32_t) strlen(name),
                             dh_xattr_status_cb, dh);
    dh_wait(dh);
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
    chimera_vfs_clone_range(dh->thread, &dh->cred, src, src_off, dst, dst_off,
                            len, 0, 0, dh_clone_cb, dh);
    dh_wait(dh);
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
    chimera_vfs_allocate(dh->thread, &dh->cred, h, off, len, flags, 0, 0,
                         dh_allocate_cb2, dh);
    dh_wait(dh);
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
    chimera_vfs_seek(dh->thread, &dh->cred, h, off, what, dh_seek_cb, dh);
    dh_wait(dh);
    return dh->status;
} /* dh_seek */

#endif /* DISKFS_TEST_HARNESS_H */
