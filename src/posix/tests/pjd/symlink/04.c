// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/symlink/04.t: ENOENT if a name2 path-prefix component does not exist. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen();
    char  P[512];
    EXPECT(0, pjd_mkdir(n0, 0755));
    snprintf(P, sizeof(P), "%s/%s/test", n0, n1);
    EXPECT(ENOENT, pjd_symlink("test", P));
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
