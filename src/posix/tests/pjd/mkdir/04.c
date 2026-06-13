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

/* Ported from pjdfstest tests/mkdir/04.t: ENOENT if a path-prefix component does not exist. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen();
    char  p[256];
    EXPECT(0, pjd_mkdir(n0, 0755));
    snprintf(p, sizeof(p), "%s/%s/test", n0, n1);
    EXPECT(ENOENT, pjd_mkdir(p, 0755));
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
