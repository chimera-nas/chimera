// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/utimensat/04.t: atime and mtime can be set independently. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen();
    EXPECT(0, pjd_mkdir(n1, 0755));
    pjd_cd(n1);
    EXPECT(0, pjd_create(n0, 0644));
    EXPECT(0, pjd_utimensat(n0, 1950000000, 0, 1900000000, 0, 0));   /* atime > mtime */
    EXPECT_EQ(1950000000, pjd_lstat_atime(n0));
    EXPECT_EQ(1900000000, pjd_lstat_mtime_sec(n0));
    EXPECT(0, pjd_utimensat(n0, 1900000000, 0, 1950000000, 0, 0));   /* mtime > atime */
    EXPECT_EQ(1900000000, pjd_lstat_atime(n0));
    EXPECT_EQ(1950000000, pjd_lstat_mtime_sec(n0));
    EXPECT(0, pjd_unlink(n0));
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n1));
    return pjd_end();
} /* main */
