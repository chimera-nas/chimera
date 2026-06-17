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

/* Ported from pjdfstest tests/rename/10.t: when the 'to' name exists and the
 * directory containing it is marked sticky (S_ISVTX), a non-super-user may
 * replace it by rename only if it owns the sticky directory or the existing
 * 'to' object; otherwise EACCES (or EPERM).  Representative subset of the
 * upstream type x owner matrix. */
#include "../../pjd_common.h"
#include <sys/stat.h>

#define U 65534   /* the unprivileged actor */
#define O 65533   /* some other unprivileged owner */

static char *n0, *n1;   /* world-writable source dir, sticky dest dir */

/* As user U, rename a U-owned source object out of n0 over an existing `type`
 * 'to' object (owned by to_uid) in the sticky dest dir n1 (owned by dir_uid). */
static void
try_one(
    enum pjd_ftype type,
    uid_t          dir_uid,
    uid_t          to_uid,
    int            expect_ok)
{
    char src[256], dst[256];

    snprintf(src, sizeof(src), "%s/f", n0);
    snprintf(dst, sizeof(dst), "%s/g", n1);

    EXPECT(0, pjd_chown(n1, dir_uid, dir_uid));
    EXPECT(0, pjd_create_file_owned(type, src, U, U));        /* source: owned by U */
    EXPECT(0, pjd_create_file_owned(type, dst, to_uid, to_uid)); /* existing 'to'   */

    pjd_set_user(U, U);
    int e = pjd_rename(src, dst) < 0 ? errno : 0;
    pjd_set_root();

    if (expect_ok) {
        PJD_CHECK(e == 0, "rename(todir=%d,to=%d,%d) -> %s (want ok)",
                  dir_uid, to_uid, type, strerror(e));
    } else {
        PJD_CHECK(e == EACCES || e == EPERM,
                  "rename(todir=%d,to=%d,%d) -> %s (want EACCES/EPERM)",
                  dir_uid, to_uid, type, strerror(e));
    }

    /* Clean up: on success only the moved object remains at dst; on denial both
     * src and the original dst remain. */
    (void) pjd_remove_file(type, src);
    (void) pjd_remove_file(type, dst);
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

    EXPECT(0, pjd_mkdir(n0, 0777));    /* source: world-writable, non-sticky */
    EXPECT(0, pjd_mkdir(n1, 0755));
    EXPECT(0, pjd_chmod(n1, 01777));   /* sticky dest dir */

    /* Owner matrix on a regular 'to': only "owns neither" is denied. */
    try_one(PJD_FT_REGULAR, U, U, 1);   /* owns dest dir and 'to'   */
    try_one(PJD_FT_REGULAR, U, O, 1);   /* owns the sticky dest dir */
    try_one(PJD_FT_REGULAR, O, U, 1);   /* owns the 'to' object     */
    try_one(PJD_FT_REGULAR, O, O, 0);   /* owns neither -> denied   */

    /* Independent of the 'to' object type. */
    try_one(PJD_FT_FIFO, O, O, 0);
    try_one(PJD_FT_SYMLINK, O, O, 0);

    EXPECT(0, pjd_rmdir(n0));
    EXPECT(0, pjd_rmdir(n1));
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(base));
    return pjd_end();
} /* main */
