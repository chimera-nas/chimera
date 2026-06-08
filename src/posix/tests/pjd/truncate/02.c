// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/truncate/02.t:
 * truncate returns ENAMETOOLONG if a pathname component exceeds {NAME_MAX}. */

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
    EXPECT(0, pjd_truncate(nx, 123));
    EXPECT_EQ(123, pjd_stat_size(nx));
    EXPECT(0, pjd_unlink(nx));
    EXPECT(ENAMETOOLONG, pjd_truncate(nxx, 123));
    return pjd_end();
} /* main */
