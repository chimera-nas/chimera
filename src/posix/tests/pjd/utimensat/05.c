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

/* Ported from pjdfstest tests/utimensat/05.t: utimensat follows a final symlink
 * by default (setting times on the target) and operates on the symlink itself
 * when AT_SYMLINK_NOFOLLOW is given. */
#include "../../pjd_common.h"
#include <sys/stat.h>
#include <fcntl.h>

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char        *n0 = pjd_namegen(), *n1 = pjd_namegen(), *n2 = pjd_namegen();
    const time_t DATE1 = 1900000000, DATE2 = 1950000000;
    const time_t DATE3 = 1960000000, DATE4 = 1970000000;
    const time_t DATE5 = 1980000000, DATE6 = 1990000000;

    EXPECT(0, pjd_mkdir(n1, 0755));
    pjd_cd(n1);

    EXPECT(0, pjd_create(n0, 0644));
    EXPECT(0, pjd_symlink(n0, n2));

    /* Follow disabled by AT_SYMLINK_NOFOLLOW: times applied to the link n2;
     * the plain (follow) call on the regular file n0 applies to n0 itself. */
    EXPECT(0, pjd_utimensat(n0, DATE1, 0, DATE2, 0, 0));
    EXPECT(0, pjd_utimensat(n2, DATE3, 0, DATE4, 0, AT_SYMLINK_NOFOLLOW));
    EXPECT_EQ(DATE1, pjd_lstat_atime(n0));
    EXPECT_EQ(DATE2, pjd_lstat_mtime_sec(n0));
    EXPECT_EQ(DATE3, pjd_lstat_atime(n2));
    EXPECT_EQ(DATE4, pjd_lstat_mtime_sec(n2));

    /* Default (no flags): follow the symlink n2 and set times on its target n0. */
    EXPECT(0, pjd_utimensat(n2, DATE5, 0, DATE6, 0, 0));
    EXPECT_EQ(DATE5, pjd_lstat_atime(n0));
    EXPECT_EQ(DATE6, pjd_lstat_mtime_sec(n0));
    /* n2 (the link) must not have been touched by the follow call: its atime
     * must not be DATE5, and its mtime must still be DATE4. */
    PJD_CHECK(DATE5 != pjd_lstat_atime(n2), "link atime not set to DATE5");
    EXPECT_EQ(DATE4, pjd_lstat_mtime_sec(n2));

    EXPECT(0, pjd_unlink(n0));
    EXPECT(0, pjd_unlink(n2));

    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n1));
    return pjd_end();
} /* main */
