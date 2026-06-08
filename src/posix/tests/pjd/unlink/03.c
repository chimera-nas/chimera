// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/unlink/03.t: ENAMETOOLONG if the whole path exceeds {PATH_MAX}. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *nx = pjd_dirgen_max();
    char  P[8192];
    snprintf(P, sizeof(P), "%sx", nx);
    pjd_mkdir_p(nx);
    EXPECT(0, pjd_create(nx, 0644));
    EXPECT(0, pjd_unlink(nx));
    EXPECT(ENAMETOOLONG, pjd_unlink(P));
    return pjd_end();
} /* main */
