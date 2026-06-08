// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/link/03.t: ENAMETOOLONG if an entire pathname exceeds {PATH_MAX}. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen();
    char *nx = pjd_dirgen_max();
    char  P[8192];
    snprintf(P, sizeof(P), "%sx", nx);
    pjd_mkdir_p(nx);
    EXPECT(0, pjd_create(n0, 0644));
    EXPECT(ENAMETOOLONG, pjd_link(n0, P));
    EXPECT(ENAMETOOLONG, pjd_link(P, n0));
    EXPECT(0, pjd_unlink(n0));
    return pjd_end();
} /* main */
