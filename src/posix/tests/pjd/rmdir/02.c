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

/* Ported from pjdfstest tests/rmdir/02.t: ENAMETOOLONG if a component exceeds {NAME_MAX}. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *nx = pjd_namegen_max();
    char  nxx[512];
    snprintf(nxx, sizeof(nxx), "%sx", nx);
    EXPECT(0, pjd_mkdir(nx, 0755));
    EXPECT(0, pjd_rmdir(nx));
    EXPECT(ENAMETOOLONG, pjd_rmdir(nxx));
    return pjd_end();
} /* main */
