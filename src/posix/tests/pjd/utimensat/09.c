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

/* Ported from pjdfstest tests/utimensat/09.t: y2038-compliant (timestamps past 2038). */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char        *n0 = pjd_namegen(), *n1 = pjd_namegen();
    const time_t BIG = 2200000000L;   /* ~2039-09-13 */
    EXPECT(0, pjd_mkdir(n1, 0755));
    pjd_cd(n1);
    EXPECT(0, pjd_create(n0, 0644));
    EXPECT(0, pjd_utimensat(n0, BIG, 0, BIG + 100, 0, 0));
    EXPECT_EQ(BIG, pjd_lstat_atime(n0));
    EXPECT_EQ(BIG + 100, pjd_lstat_mtime_sec(n0));
    EXPECT(0, pjd_unlink(n0));
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n1));
    return pjd_end();
} /* main */
