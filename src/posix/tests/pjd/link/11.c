// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/link/11.t: EPERM if the source file is a directory. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen(), *n2 = pjd_namegen();
    EXPECT(0, pjd_mkdir(n0, 0755));
    EXPECT(EPERM, pjd_link(n0, n1));
    EXPECT(0, pjd_rmdir(n0));

    EXPECT(0, pjd_mkdir(n0, 0755));
    EXPECT(0, pjd_chown(n0, 65534, 65534));
    pjd_cd(n0);
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_mkdir(n1, 0755));
    EXPECT(EPERM, pjd_link(n1, n2));
    EXPECT(0, pjd_rmdir(n1));
    pjd_set_root();
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
