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

/* Ported from pjdfstest tests/open/13.t: EISDIR when opening a directory for writing. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen();
    EXPECT(0, pjd_mkdir(n0, 0755));
    EXPECT_EQ(0, pjd_open_e(n0, O_RDONLY, 0));
    EXPECT_EQ(EISDIR, pjd_open_e(n0, O_WRONLY, 0));
    EXPECT_EQ(EISDIR, pjd_open_e(n0, O_RDWR, 0));
    EXPECT_EQ(EISDIR, pjd_open_e(n0, O_RDONLY | O_TRUNC, 0));
    EXPECT_EQ(EISDIR, pjd_open_e(n0, O_WRONLY | O_TRUNC, 0));
    EXPECT_EQ(EISDIR, pjd_open_e(n0, O_RDWR | O_TRUNC, 0));
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
