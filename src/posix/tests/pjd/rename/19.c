// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/rename/19.t: EINVAL/EBUSY when renaming '.' or '..'. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen(), *n2 = pjd_namegen();
    char  dot[300], dotdot[300], a[256];
    EXPECT(0, pjd_mkdir(n0, 0755));
    snprintf(a, sizeof(a), "%s/%s", n0, n1);
    EXPECT(0, pjd_mkdir(a, 0755));
    snprintf(dot, sizeof(dot), "%s/.", a);
    snprintf(dotdot, sizeof(dotdot), "%s/..", a);
    int   rc = pjd_rename(dot, n2); int e = (rc < 0) ? errno : 0;
    PJD_CHECK(e == EINVAL || e == EBUSY, "rename '.' -> %s", strerror(e));
    rc = pjd_rename(dotdot, n2); e = (rc < 0) ? errno : 0;
    PJD_CHECK(e == EINVAL || e == EBUSY, "rename '..' -> %s", strerror(e));
    EXPECT(0, pjd_rmdir(a));
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
