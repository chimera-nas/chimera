// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/mkdir/02.t: ENAMETOOLONG if a component exceeds {NAME_MAX}. */
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
    EXPECT(0, pjd_mkdir(nx, 0755));
    EXPECT_EQ(S_IFDIR, pjd_lstat_type(nx));
    EXPECT(0, pjd_rmdir(nx));
    EXPECT(ENAMETOOLONG, pjd_mkdir(nxx, 0755));
    return pjd_end();
} /* main */
