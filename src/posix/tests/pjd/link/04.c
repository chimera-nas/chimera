// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/link/04.t: ENOENT if a component of either path prefix does not exist. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen(), *n2 = pjd_namegen();
    char  src[256], P[512];
    EXPECT(0, pjd_mkdir(n0, 0755));
    snprintf(src, sizeof(src), "%s/%s", n0, n1);
    EXPECT(0, pjd_create(src, 0644));
    snprintf(P, sizeof(P), "%s/%s/test", n0, n2);
    EXPECT(ENOENT, pjd_link(src, P));
    EXPECT(ENOENT, pjd_link(P, src));
    EXPECT(0, pjd_unlink(src));
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
