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

/* Ported from pjdfstest tests/rename/02.t: ENAMETOOLONG if an entire pathname exceeds {PATH_MAX}. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen();
    char *nx = pjd_dirgen_max();
    char  P[8192];
    snprintf(P, sizeof(P), "%sx", nx);
    pjd_mkdir_p(nx);
    EXPECT(0, pjd_create(n0, 0644));
    EXPECT(ENAMETOOLONG, pjd_rename(n0, P));
    EXPECT(ENAMETOOLONG, pjd_rename(P, n0));
    EXPECT(0, pjd_unlink(n0));
    return pjd_end();
} /* main */
