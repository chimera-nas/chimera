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

/* Ported from pjdfstest tests/rename/21.t: write access to a subdirectory is
 * required to move it to another directory.  Write permission on the containing
 * directory alone is enough to rename a plain file out of it; moving a
 * subdirectory may additionally require write on the subdirectory, which POSIX
 * leaves implementation-defined (accept 0 or EACCES for the dir-move cases). */
#include "../../pjd_common.h"

/* tolerant check: a result of 0 or `experr` both pass. */
static void
expect_ok_or(
    int         experr,
    int         rc,
    int         en,
    const char *what)
{
    int e = (rc < 0) ? en : 0;

    PJD_CHECK(e == 0 || e == experr, "%s -> %s", what, strerror(e));
} /* expect_ok_or */

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char *n0 = pjd_namegen(), *n1 = pjd_namegen();
    char *n2 = pjd_namegen(), *n3 = pjd_namegen();
    char  src[256], dst[256];

    EXPECT(0, pjd_mkdir(n2, 0777));
    EXPECT(0, pjd_mkdir(n3, 0777));

    /* Write permission on the containing directory (n2) is enough to rename a
     * subdirectory (n0) within it; renaming a directory may also require write
     * access to the directory itself, so accept 0 or EACCES. */
    snprintf(src, sizeof(src), "%s/%s", n2, n0);
    snprintf(dst, sizeof(dst), "%s/%s", n2, n1);
    EXPECT(0, pjd_mkdir(src, 0700));

    pjd_set_user(65534, 65534);
    {
        errno = 0;
        int rc = pjd_rename(src, dst); int en = errno;
        expect_ok_or(EACCES, rc, en, "rename subdir within same dir");
    }
    {
        errno = 0;
        int rc = pjd_rename(dst, src); int en = errno;
        expect_ok_or(EACCES, rc, en, "rename subdir back within same dir");
    }
    pjd_set_root();

    /* Moving a subdirectory out of n2 into n3: POSIX says write on n2 and n3
     * may be enough; accept 0 or EACCES. */
    snprintf(src, sizeof(src), "%s/%s", n2, n0);
    snprintf(dst, sizeof(dst), "%s/%s", n3, n1);
    pjd_set_user(65534, 65534);
    {
        errno = 0;
        int rc = pjd_rename(src, dst); int en = errno;
        expect_ok_or(EACCES, rc, en, "move subdir to other dir");
    }
    pjd_set_root();

    /* Clean up whichever of the two cases above actually moved the dir. */
    {
        errno = 0;
        int rc = pjd_rmdir(src); int en = errno;
        expect_ok_or(ENOENT, rc, en, "rmdir source subdir");
    }
    EXPECT(ENOENT, pjd_rmdir(src));
    {
        errno = 0;
        int rc = pjd_rmdir(dst); int en = errno;
        expect_ok_or(ENOENT, rc, en, "rmdir dest subdir");
    }
    EXPECT(ENOENT, pjd_rmdir(dst));

    /* Write permission on the containing directory (n2) IS enough to move a
     * plain file (n0) out of it into another writable directory (n3). */
    snprintf(src, sizeof(src), "%s/%s", n2, n0);
    snprintf(dst, sizeof(dst), "%s/%s", n3, n1);
    EXPECT(0, pjd_create(src, 0644));

    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_rename(src, dst));
    pjd_set_root();

    EXPECT(0, pjd_unlink(dst));
    EXPECT(ENOENT, pjd_unlink(src));

    EXPECT(0, pjd_rmdir(n3));
    EXPECT(0, pjd_rmdir(n2));

    return pjd_end();
} /* main */
