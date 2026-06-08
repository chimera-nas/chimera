// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/chmod/02.t:
 * chmod returns ENAMETOOLONG if a pathname component exceeds {NAME_MAX}. */

#include "../../pjd_common.h"

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char *nx = pjd_namegen_max();
    char  nxx[512];

    snprintf(nxx, sizeof(nxx), "%sx", nx);

    EXPECT(0, pjd_create(nx, 0644));
    EXPECT(0, pjd_chmod(nx, 0620));
    EXPECT_EQ(0620, pjd_stat_mode(nx));
    EXPECT(0, pjd_unlink(nx));
    EXPECT(ENAMETOOLONG, pjd_chmod(nxx, 0620));

    return pjd_end();
} /* main */
