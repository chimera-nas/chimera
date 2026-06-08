// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/open/26.t: O_CREAT can create files with 0000 mode,
 * openable by the creator for read/write. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen();
    EXPECT_EQ(0, pjd_open_e(n0, O_CREAT | O_WRONLY, 0000));
    EXPECT_EQ(S_IFREG, pjd_lstat_type(n0));
    EXPECT_EQ(0, pjd_lstat_mode(n0));
    EXPECT(0, pjd_unlink(n0));
    EXPECT_EQ(0, pjd_open_e(n0, O_CREAT | O_RDWR, 0000));
    EXPECT_EQ(0, pjd_lstat_mode(n0));
    EXPECT(0, pjd_unlink(n0));
    EXPECT_EQ(0, pjd_open_e(n0, O_CREAT | O_RDONLY, 0000));
    EXPECT_EQ(0, pjd_lstat_mode(n0));
    EXPECT(0, pjd_unlink(n0));
    return pjd_end();
} /* main */
