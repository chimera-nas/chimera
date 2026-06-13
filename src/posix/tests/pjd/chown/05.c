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

/* Ported from pjdfstest tests/chown/05.t:
 * chown returns EACCES when search permission is denied for a component of the
 * path prefix. */

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
    char  n1n2[256];

    EXPECT(0, pjd_mkdir(n0, 0755));
    pjd_cd(n0);

    EXPECT(0, pjd_mkdir(n1, 0755));
    EXPECT(0, pjd_chown(n1, 65534, 65534));

    snprintf(n1n2, sizeof(n1n2), "%s/%s", n1, n2);

    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_create(n1n2, 0644));
    EXPECT(0, pjd_chown(n1n2, 65534, 65534));
    pjd_set_root();

    /* Remove search permission on n1; non-owner can no longer traverse it. */
    EXPECT(0, pjd_chmod(n1, 0644));
    pjd_set_user(65534, 65534);
    EXPECT(EACCES, pjd_chown(n1n2, 65534, 65534));
    pjd_set_root();

    EXPECT(0, pjd_chmod(n1, 0755));
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_chown(n1n2, 65534, 65534));
    EXPECT(0, pjd_unlink(n1n2));
    pjd_set_root();

    EXPECT(0, pjd_rmdir(n1));
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n0));

    return pjd_end();
} /* main */
