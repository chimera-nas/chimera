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

/* Ported from pjdfstest tests/rename/18.t: EINVAL when 'from' is an ancestor of 'to'. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen(), *n2 = pjd_namegen();
    char  a[256], b[512];
    EXPECT(0, pjd_mkdir(n0, 0755));
    snprintf(a, sizeof(a), "%s/%s", n0, n1);
    EXPECT(0, pjd_mkdir(a, 0755));
    EXPECT(EINVAL, pjd_rename(n0, a));
    snprintf(b, sizeof(b), "%s/%s/%s", n0, n1, n2);
    EXPECT(EINVAL, pjd_rename(n0, b));
    EXPECT(0, pjd_rmdir(a));
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
