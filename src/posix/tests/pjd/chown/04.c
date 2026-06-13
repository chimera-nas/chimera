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

/* Ported from pjdfstest tests/chown/04.t:
 * chown returns ENOENT if the named file does not exist. */

#include "../../pjd_common.h"

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char *n0 = pjd_namegen();
    char *n1 = pjd_namegen();
    char *n2 = pjd_namegen();
    char  p1[256], p1t[512];

    EXPECT(0, pjd_mkdir(n0, 0755));

    snprintf(p1,  sizeof(p1),  "%s/%s", n0, n1);
    snprintf(p1t, sizeof(p1t), "%s/%s/test", n0, n1);

    EXPECT(ENOENT, pjd_chown(p1t, 65534, 65534));
    EXPECT(ENOENT, pjd_chown(p1, 65534, 65534));

    /* chown follows the (dangling) symlink -> ENOENT. */
    EXPECT(0, pjd_symlink(n2, p1));
    EXPECT(ENOENT, pjd_chown(p1, 65534, 65534));
    EXPECT(0, pjd_unlink(p1));

    EXPECT(0, pjd_rmdir(n0));

    return pjd_end();
} /* main */
