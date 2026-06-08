// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/open/16.t: O_NOFOLLOW on a symlink target yields ELOOP (Linux). */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen();
    EXPECT(0, pjd_symlink(n0, n1));
    EXPECT_EQ(ELOOP, pjd_open_e(n1, O_RDONLY | O_CREAT | O_NOFOLLOW, 0644));
    EXPECT_EQ(ELOOP, pjd_open_e(n1, O_RDONLY | O_NOFOLLOW, 0));
    EXPECT_EQ(ELOOP, pjd_open_e(n1, O_WRONLY | O_NOFOLLOW, 0));
    EXPECT_EQ(ELOOP, pjd_open_e(n1, O_RDWR | O_NOFOLLOW, 0));
    EXPECT(0, pjd_unlink(n1));
    return pjd_end();
} /* main */
