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

/* Ported from pjdfstest tests/rmdir/12.t:
 * rmdir returns EINVAL if the last path component is '.', and EEXIST/ENOTEMPTY
 * if it is '..'. */

#include "../../pjd_common.h"

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char *n0 = pjd_namegen();
    char *n1 = pjd_namegen();
    char  n0n1[256], dot[300], dotdot[300];

    EXPECT(0, pjd_mkdir(n0, 0755));
    snprintf(n0n1, sizeof(n0n1), "%s/%s", n0, n1);
    EXPECT(0, pjd_mkdir(n0n1, 0755));

    snprintf(dot, sizeof(dot), "%s/.", n0n1);
    EXPECT(EINVAL, pjd_rmdir(dot));

    snprintf(dotdot, sizeof(dotdot), "%s/..", n0n1);
    {
        int rc = pjd_rmdir(dotdot);
        int e  = (rc < 0) ? errno : 0;
        PJD_CHECK(e == ENOTEMPTY || e == EEXIST,
                  "rmdir '..' -> %s (want ENOTEMPTY/EEXIST)", strerror(e));
    }

    EXPECT(0, pjd_rmdir(n0n1));
    EXPECT(0, pjd_rmdir(n0));

    return pjd_end();
} /* main */
