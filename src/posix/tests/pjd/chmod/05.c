// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/chmod/05.t:
 * chmod returns EACCES when search permission is denied for a component of the
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
    EXPECT(0, pjd_chmod(n1n2, 0642));
    EXPECT_EQ(0642, pjd_stat_mode(n1n2));
    pjd_set_root();

    /* Remove search permission on n1; non-owner can no longer traverse it. */
    EXPECT(0, pjd_chmod(n1, 0644));
    pjd_set_user(65534, 65534);
    EXPECT(EACCES, pjd_chmod(n1n2, 0620));
    pjd_set_root();

    EXPECT(0, pjd_chmod(n1, 0755));
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_chmod(n1n2, 0420));
    EXPECT_EQ(0420, pjd_stat_mode(n1n2));
    EXPECT(0, pjd_unlink(n1n2));
    pjd_set_root();

    EXPECT(0, pjd_rmdir(n1));
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n0));

    return pjd_end();
} /* main */
