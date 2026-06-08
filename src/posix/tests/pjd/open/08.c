// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/open/08.t: O_CREAT in a directory the caller cannot write -> EACCES. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen();
    EXPECT(0, pjd_mkdir(n0, 0755));
    pjd_cd(n0);
    /* cwd n0 is owned by root, mode 0755: an unprivileged caller cannot create. */
    pjd_set_user(65534, 65534);
    EXPECT_EQ(EACCES, pjd_open_e(n1, O_RDONLY | O_CREAT, 0644));
    pjd_set_root();
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
