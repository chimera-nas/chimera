// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/utimensat/02.t: UTIME_OMIT leaves that timestamp unchanged. */
#include "../../pjd_common.h"
#include <sys/stat.h>
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char        *n0 = pjd_namegen(), *n1 = pjd_namegen();
    const time_t DATE1 = 1900000000, DATE2 = 1950000000, DATE3 = 1920000000;
    EXPECT(0, pjd_mkdir(n1, 0755));
    pjd_cd(n1);
    EXPECT(0, pjd_create(n0, 0644));
    EXPECT(0, pjd_utimensat(n0, DATE1, 0, DATE2, 0, 0));
    /* Omit atime, change mtime. */
    EXPECT(0, pjd_utimensat(n0, 0, UTIME_OMIT, DATE3, 0, 0));
    EXPECT_EQ(DATE1, pjd_lstat_atime(n0));
    EXPECT_EQ(DATE3, pjd_lstat_mtime_sec(n0));
    /* Omit mtime, change atime. */
    EXPECT(0, pjd_utimensat(n0, DATE2, 0, 0, UTIME_OMIT, 0));
    EXPECT_EQ(DATE2, pjd_lstat_atime(n0));
    EXPECT_EQ(DATE3, pjd_lstat_mtime_sec(n0));
    EXPECT(0, pjd_unlink(n0));
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n1));
    return pjd_end();
} /* main */
