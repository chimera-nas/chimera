// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/rename/11.t: ELOOP on a symlink loop in either pathname. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen();
    char  P0[256], P1[256];
    EXPECT(0, pjd_symlink(n0, n1));
    EXPECT(0, pjd_symlink(n1, n0));
    snprintf(P0, sizeof(P0), "%s/test", n0);
    snprintf(P1, sizeof(P1), "%s/test", n1);
    EXPECT(ELOOP, pjd_rename(P0, n0));
    EXPECT(ELOOP, pjd_rename(P1, n0));
    EXPECT(ELOOP, pjd_rename(n0, P0));
    EXPECT(ELOOP, pjd_rename(n0, P1));
    EXPECT(0, pjd_unlink(n0));
    EXPECT(0, pjd_unlink(n1));
    return pjd_end();
} /* main */
