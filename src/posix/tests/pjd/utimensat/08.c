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

/* Ported from pjdfstest tests/utimensat/08.t: sub-second precision. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char           *n0 = pjd_namegen(), *n1 = pjd_namegen();
    struct timespec a, m;
    EXPECT(0, pjd_mkdir(n1, 0755));
    pjd_cd(n1);
    EXPECT(0, pjd_create(n0, 0644));
    EXPECT(0, pjd_utimensat(n0, 1900000000, 123456789, 1950000000, 987654321, 0));
    EXPECT(0, pjd_lstat_times(n0, &a, &m));
    EXPECT_EQ(1900000000, (long) a.tv_sec);
    EXPECT_EQ(123456789, (long) a.tv_nsec);
    EXPECT_EQ(1950000000, (long) m.tv_sec);
    EXPECT_EQ(987654321, (long) m.tv_nsec);
    EXPECT(0, pjd_unlink(n0));
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n1));
    return pjd_end();
} /* main */
