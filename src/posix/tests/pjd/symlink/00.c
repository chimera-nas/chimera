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

/* Ported from pjdfstest tests/symlink/00.t:
 * symlink creates symbolic links; lstat sees the link, stat follows to the
 * target, and the parent directory's mtime/ctime are updated. */

#include "../../pjd_common.h"

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char           *n0 = pjd_namegen();
    char           *n1 = pjd_namegen();
    char            n0n1[256];
    struct timespec t0, t1;
    struct stat     st;

    EXPECT(0, pjd_create(n0, 0644));
    EXPECT_EQ(S_IFREG, pjd_lstat_type(n0));
    EXPECT_EQ(0644, pjd_lstat_mode(n0));

    EXPECT(0, pjd_symlink(n0, n1));
    EXPECT_EQ(S_IFLNK, pjd_lstat_type(n1));
    EXPECT_EQ(S_IFREG, pjd_stat_type(n1));      /* follows to the regular file */
    EXPECT_EQ(0644, pjd_stat_mode(n1));

    EXPECT(0, pjd_unlink(n0));
    EXPECT(ENOENT, pjd_stat(n1, &st));          /* dangling now */
    EXPECT(0, pjd_unlink(n1));

    /* symlink updates the parent directory's mtime/ctime. */
    EXPECT(0, pjd_mkdir(n0, 0755));
    snprintf(n0n1, sizeof(n0n1), "%s/%s", n0, n1);
    pjd_stat_ctime(n0, &t0);
    pjd_settle();
    EXPECT(0, pjd_symlink("test", n0n1));
    pjd_stat_mtime(n0, &t1);
    PJD_CHECK(pjd_timespec_lt(&t0, &t1), "parent mtime advanced on symlink");
    pjd_stat_ctime(n0, &t1);
    PJD_CHECK(pjd_timespec_lt(&t0, &t1), "parent ctime advanced on symlink");
    EXPECT(0, pjd_unlink(n0n1));
    EXPECT(0, pjd_rmdir(n0));

    return pjd_end();
} /* main */
