// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/unlink/05.t: EACCES when search permission is denied for a path-prefix component. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen(), *n2 = pjd_namegen();
    char  P[256];
    EXPECT(0, pjd_mkdir(n0, 0755));
    pjd_cd(n0);
    EXPECT(0, pjd_mkdir(n1, 0755));
    EXPECT(0, pjd_chown(n1, 65534, 65534));
    snprintf(P, sizeof(P), "%s/%s", n1, n2);
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_create(P, 0644));
    pjd_set_root();
    EXPECT(0, pjd_chmod(n1, 0644));
    pjd_set_user(65534, 65534);
    EXPECT(EACCES, pjd_unlink(P));
    pjd_set_root();
    EXPECT(0, pjd_chmod(n1, 0755));
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_unlink(P));
    pjd_set_root();
    EXPECT(0, pjd_rmdir(n1));
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
