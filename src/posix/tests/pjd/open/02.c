// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/open/02.t: ENAMETOOLONG if a component exceeds {NAME_MAX}. */
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
    EXPECT_EQ(0, pjd_open_e(nx, O_CREAT | O_RDONLY, 0644));
    EXPECT(0, pjd_unlink(nx));
    EXPECT_EQ(ENAMETOOLONG, pjd_open_e(P, O_CREAT | O_RDONLY, 0644));
    return pjd_end();
} /* main */
