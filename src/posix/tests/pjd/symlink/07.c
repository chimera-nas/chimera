// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/symlink/07.t: ELOOP on a symlink loop in the name2 pathname. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen();
    char  P[256];
    EXPECT(0, pjd_symlink(n0, n1));
    EXPECT(0, pjd_symlink(n1, n0));
    snprintf(P, sizeof(P), "%s/test", n0);
    EXPECT(ELOOP, pjd_symlink("test", P));
    snprintf(P, sizeof(P), "%s/test", n1);
    EXPECT(ELOOP, pjd_symlink("test", P));
    EXPECT(0, pjd_unlink(n0));
    EXPECT(0, pjd_unlink(n1));
    return pjd_end();
} /* main */
