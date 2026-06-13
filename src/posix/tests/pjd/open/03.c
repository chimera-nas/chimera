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

/* Ported from pjdfstest tests/open/03.t: ENAMETOOLONG if the whole path exceeds {PATH_MAX}. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *nx = pjd_dirgen_max();
    char  P[8192];
    snprintf(P, sizeof(P), "%sx", nx);
    pjd_mkdir_p(nx);
    EXPECT_EQ(0, pjd_open_e(nx, O_CREAT | O_RDONLY, 0644));
    EXPECT(0, pjd_unlink(nx));
    EXPECT_EQ(ENAMETOOLONG, pjd_open_e(P, O_CREAT | O_RDONLY, 0644));
    return pjd_end();
} /* main */
