// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/symlink/02.t: ENAMETOOLONG if a name2 component exceeds {NAME_MAX}. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *nx = pjd_namegen_max();
    char  P[512];
    snprintf(P, sizeof(P), "%sx", nx);
    EXPECT(ENAMETOOLONG, pjd_symlink("test", P));
    return pjd_end();
} /* main */
