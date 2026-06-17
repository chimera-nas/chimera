// SPDX-FileCopyrightText: 2006-2012 Pawel Jakub Dawidek <pawel@dawidek.net>
// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: BSD-2-Clause
//
// Derived from the pjdfstest POSIX filesystem test suite
// (https://github.com/pjd/pjdfstest) by Pawel Jakub Dawidek.  These are C
// reimplementations of the upstream shell test cases, run against the Chimera
// POSIX client, and are distributed under pjdfstest's original 2-clause BSD
// license.

/* Ported from pjdfstest tests/rename/09.t: when the directory containing the
 * 'from' name is marked sticky (S_ISVTX), a non-super-user may rename the entry
 * only if it owns the sticky directory or the source object; otherwise EACCES
 * (or EPERM).  This is a representative subset of the upstream type x owner
 * matrix -- enough to cover every ownership outcome across a few object types. */
#include "../../pjd_common.h"
#include <sys/stat.h>

#define U 65534   /* the unprivileged actor */
#define O 65533   /* some other unprivileged owner */

static char *n0, *n1;   /* sticky source dir, world-writable dest dir */

/* Attempt, as user U, to rename a `type` object owned by file_uid out of the
 * sticky source dir n0 (owned by dir_uid) into the dest dir n1.  expect_ok
 * selects success vs an EACCES/EPERM denial. */
static void
try_one(
    enum pjd_ftype type,
    uid_t          dir_uid,
    uid_t          file_uid,
    int            expect_ok)
{
    char src[256], dst[256];

    snprintf(src, sizeof(src), "%s/f", n0);
    snprintf(dst, sizeof(dst), "%s/g", n1);

    EXPECT(0, pjd_chown(n0, dir_uid, dir_uid));
    EXPECT(0, pjd_create_file_owned(type, src, file_uid, file_uid));

    pjd_set_user(U, U);
    int e = pjd_rename(src, dst) < 0 ? errno : 0;
    pjd_set_root();

    if (expect_ok) {
        PJD_CHECK(e == 0, "rename(dir=%d,file=%d,%d) -> %s (want ok)",
                  dir_uid, file_uid, type, strerror(e));
    } else {
        PJD_CHECK(e == EACCES || e == EPERM,
                  "rename(dir=%d,file=%d,%d) -> %s (want EACCES/EPERM)",
                  dir_uid, file_uid, type, strerror(e));
    }

    /* Clean up whichever location the object ended in (as root). */
    if (pjd_remove_file(type, src) != 0) {
        (void) pjd_remove_file(type, dst);
    }
} /* try_one */

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *base = pjd_namegen();
    n0 = pjd_namegen();
    n1 = pjd_namegen();

    EXPECT(0, pjd_mkdir(base, 0755));
    pjd_cd(base);

    EXPECT(0, pjd_mkdir(n0, 0755));
    EXPECT(0, pjd_chmod(n0, 01777));   /* sticky source dir */
    EXPECT(0, pjd_mkdir(n1, 0777));    /* dest: world-writable, non-sticky */

    /* Owner matrix on a regular file: only "owns neither" is denied. */
    try_one(PJD_FT_REGULAR, U, U, 1);   /* owns dir and file       */
    try_one(PJD_FT_REGULAR, U, O, 1);   /* owns the sticky dir     */
    try_one(PJD_FT_REGULAR, O, U, 1);   /* owns the source object  */
    try_one(PJD_FT_REGULAR, O, O, 0);   /* owns neither -> denied  */

    /* The deny rule is independent of object type. */
    try_one(PJD_FT_FIFO, O, O, 0);
    try_one(PJD_FT_SYMLINK, O, O, 0);
    try_one(PJD_FT_DIR, O, O, 0);
    /* ...and an owned object of another type still renames. */
    try_one(PJD_FT_FIFO, O, U, 1);

    EXPECT(0, pjd_rmdir(n0));
    EXPECT(0, pjd_rmdir(n1));
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(base));
    return pjd_end();
} /* main */
