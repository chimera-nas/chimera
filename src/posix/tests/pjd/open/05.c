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

/* Ported from pjdfstest tests/open/05.t: EACCES when search permission is denied for a path-prefix component. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen(), *n2 = pjd_namegen();
    char  P[256];
    EXPECT(0, pjd_mkdir(n0, 0755));
    pjd_cd(n0);
    EXPECT(0, pjd_mkdir(n1, 0755));
    EXPECT(0, pjd_chown(n1, 65534, 65534));
    snprintf(P, sizeof(P), "%s/%s", n1, n2);
    pjd_set_user(65534, 65534);
    EXPECT_EQ(0, pjd_open_e(P, O_RDONLY | O_CREAT, 0644));
    pjd_set_root();
    EXPECT(0, pjd_chmod(n1, 0644));
    pjd_set_user(65534, 65534);
    EXPECT_EQ(EACCES, pjd_open_e(P, O_RDONLY, 0));
    pjd_set_root();
    EXPECT(0, pjd_chmod(n1, 0755));
    EXPECT(0, pjd_unlink(P));
    EXPECT(0, pjd_rmdir(n1));
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
