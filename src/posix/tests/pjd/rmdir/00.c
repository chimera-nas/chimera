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

/* Ported from pjdfstest tests/rmdir/00.t: rmdir removes directories and updates
 * the parent directory's mtime/ctime. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char           *n0 = pjd_namegen(), *n1 = pjd_namegen();
    char            n0n1[256];
    struct timespec t0, t1;
    EXPECT(0, pjd_mkdir(n0, 0755));
    EXPECT_EQ(S_IFDIR, pjd_lstat_type(n0));
    EXPECT(0, pjd_rmdir(n0));
    EXPECT_EQ(0, pjd_lstat_type(n0));   /* gone */

    EXPECT(0, pjd_mkdir(n0, 0755));
    snprintf(n0n1, sizeof(n0n1), "%s/%s", n0, n1);
    EXPECT(0, pjd_mkdir(n0n1, 0755));
    pjd_stat_ctime(n0, &t0);
    pjd_settle();
    EXPECT(0, pjd_rmdir(n0n1));
    pjd_stat_mtime(n0, &t1);
    PJD_CHECK(pjd_timespec_lt(&t0, &t1), "parent mtime advanced on rmdir");
    pjd_stat_ctime(n0, &t1);
    PJD_CHECK(pjd_timespec_lt(&t0, &t1), "parent ctime advanced on rmdir");
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
