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

/* Ported from pjdfstest tests/chmod/03.t:
 * chmod returns ENAMETOOLONG if an entire pathname exceeds {PATH_MAX}.
 * (Cleanup of the deep tree is unnecessary: the per-test backend is torn down
 * at teardown.) */

#include "../../pjd_common.h"

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char *nx = pjd_dirgen_max();
    char  nxx[8192];

    snprintf(nxx, sizeof(nxx), "%sx", nx);

    pjd_mkdir_p(nx);

    EXPECT(0, pjd_create(nx, 0644));
    EXPECT(0, pjd_chmod(nx, 0642));
    EXPECT_EQ(0642, pjd_stat_mode(nx));
    EXPECT(0, pjd_unlink(nx));
    EXPECT(ENAMETOOLONG, pjd_chmod(nxx, 0642));

    return pjd_end();
} /* main */
