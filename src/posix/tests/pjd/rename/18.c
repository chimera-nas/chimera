// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/rename/18.t: EINVAL when 'from' is an ancestor of 'to'. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen(), *n2 = pjd_namegen();
    char  a[256], b[512];
    EXPECT(0, pjd_mkdir(n0, 0755));
    snprintf(a, sizeof(a), "%s/%s", n0, n1);
    EXPECT(0, pjd_mkdir(a, 0755));
    EXPECT(EINVAL, pjd_rename(n0, a));
    snprintf(b, sizeof(b), "%s/%s/%s", n0, n1, n2);
    EXPECT(EINVAL, pjd_rename(n0, b));
    EXPECT(0, pjd_rmdir(a));
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
