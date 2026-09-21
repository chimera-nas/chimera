/* SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
 *
 * SPDX-License-Identifier: LGPL-2.1-only
 *
 * Short sequences for the in-process VFS tests, run synchronously.
 *
 * Every test in src/vfs/tests and src/vfs/diskfs/tests drives the VFS the way
 * a front end does -- it builds a sequence and submits it -- rather than
 * reaching for the core's per-op entry points.  What these tests do NOT have
 * is a front end's event loop: they own the evpl loop themselves and want
 * straight-line code, so each helper here submits one sequence and pumps until
 * its single callback has fired.
 *
 * Three fixture shapes repeat in nearly every one of them -- resolve a share's
 * root file handle, resolve a name under a directory, open a file handle --
 * and are spelled once here.  Anything beyond that a test builds for itself
 * with compound_test_run(), which is the only part of this that is load
 * bearing: submit, pump, report the sequence's status.
 */

#ifndef CHIMERA_COMPOUND_TEST_UTIL_H
#define CHIMERA_COMPOUND_TEST_UTIL_H

#include <string.h>
/* These tests use assert() as their oracle, so keep it live even under NDEBUG.
 * Must precede <assert.h>, which (re)defines assert from NDEBUG at each
 * include. */
#undef NDEBUG
#include <assert.h>

#include "evpl/evpl.h"
#include "vfs/vfs.h"
#include "vfs/vfs_compound.h"
#include "vfs/sdk/vfs_attrs.h"
#include "vfs/sdk/vfs_cred.h"
#include "vfs/sdk/vfs_error.h"

static inline void
compound_test_done_cb(
    struct chimera_vfs_compound *compound,
    void                        *private_data)
{
    *(int *) private_data = 1;
} /* compound_test_done_cb */

/* Submit `cp` and pump `evpl` until its one callback has fired; returns the
 * sequence's status.  The caller still owns `cp` afterwards: it reads the
 * per-op results off it, takes whatever it means to keep, and frees it. */
static inline enum chimera_vfs_error
compound_test_run(
    struct evpl                 *evpl,
    struct chimera_vfs_compound *cp)
{
    int done = 0;

    chimera_vfs_compound_submit(cp, compound_test_done_cb, &done);

    while (!done) {
        evpl_continue(evpl);
    }

    return chimera_vfs_compound_status(cp);
} /* compound_test_run */

/* PUTFH(dir_fh); LOOKUP(name); GETFH -- the resolved object's file handle
 * lands in out_fh / out_fh_len (untouched unless the sequence succeeded). */
static inline enum chimera_vfs_error
compound_test_lookup(
    struct chimera_vfs_thread     *thread,
    struct evpl                   *evpl,
    const struct chimera_vfs_cred *cred,
    const void                    *dir_fh,
    uint32_t                       dir_fh_len,
    const char                    *name,
    uint8_t                       *out_fh,
    uint32_t                      *out_fh_len)
{
    struct chimera_vfs_compound          *cp;
    const struct chimera_vfs_compound_op *op;
    enum chimera_vfs_error                status;
    int                                   i_lookup;

    cp = chimera_vfs_compound_alloc(thread, cred);
    chimera_vfs_compound_add_putfh(cp, dir_fh, (int) dir_fh_len);
    i_lookup = chimera_vfs_compound_add_lookup(cp, name, (int) strlen(name),
                                               CHIMERA_VFS_ATTR_FH |
                                               CHIMERA_VFS_ATTR_MASK_STAT, 0);

    status = compound_test_run(evpl, cp);

    if (status == CHIMERA_VFS_OK && out_fh) {
        op = chimera_vfs_compound_op(cp, (uint32_t) i_lookup);
        assert(op->attr.va_set_mask & CHIMERA_VFS_ATTR_FH);
        memcpy(out_fh, op->attr.va_fh, op->attr.va_fh_len);
        *out_fh_len = op->attr.va_fh_len;
    }

    chimera_vfs_compound_free(cp);

    return status;
} /* compound_test_lookup */

/* The mount root of the share published at "/<share>": the namespace root's
 * file handle, then the one component that names the share. */
static inline enum chimera_vfs_error
compound_test_mount_root(
    struct chimera_vfs_thread     *thread,
    struct evpl                   *evpl,
    const struct chimera_vfs_cred *cred,
    const char                    *share,
    uint8_t                       *out_fh,
    uint32_t                      *out_fh_len)
{
    uint8_t  root_fh[CHIMERA_VFS_FH_SIZE];
    uint32_t root_fh_len;

    chimera_vfs_get_root_fh(root_fh, &root_fh_len);

    return compound_test_lookup(thread, evpl, cred, root_fh, root_fh_len,
                                share, out_fh, out_fh_len);
} /* compound_test_mount_root */

/* PUTFH(fh); OPEN_CURRENT(flags); GETHANDLE -- the open handle is TAKEN from
 * the sequence, so the caller releases it (chimera_vfs_release).  NULL is left
 * in *out_handle unless the sequence succeeded. */
static inline enum chimera_vfs_error
compound_test_open_fh(
    struct chimera_vfs_thread       *thread,
    struct evpl                     *evpl,
    const struct chimera_vfs_cred   *cred,
    const void                      *fh,
    uint32_t                         fh_len,
    unsigned int                     flags,
    struct chimera_vfs_open_handle **out_handle)
{
    struct chimera_vfs_compound *cp;
    enum chimera_vfs_error       status;
    int                          i_gh;

    *out_handle = NULL;

    cp = chimera_vfs_compound_alloc(thread, cred);
    chimera_vfs_compound_add_putfh(cp, fh, (int) fh_len);
    chimera_vfs_compound_add_open_current(cp, flags, 0);
    i_gh = chimera_vfs_compound_add_gethandle(cp);

    status = compound_test_run(evpl, cp);

    if (status == CHIMERA_VFS_OK) {
        *out_handle = chimera_vfs_compound_take_handle(cp, (uint32_t) i_gh);
        assert(*out_handle != NULL);
    }

    chimera_vfs_compound_free(cp);

    return status;
} /* compound_test_open_fh */

#endif /* CHIMERA_COMPOUND_TEST_UTIL_H */
