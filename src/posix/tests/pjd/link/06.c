// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/link/06.t:
 * link returns EACCES when a component of either path prefix denies search. */

#include "../../pjd_common.h"

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char *n0 = pjd_namegen();
    char *n1 = pjd_namegen();
    char *n2 = pjd_namegen();
    char *n3 = pjd_namegen();
    char  src[256], dst[256];

    EXPECT(0, pjd_mkdir(n0, 0755));
    pjd_cd(n0);

    EXPECT(0, pjd_mkdir(n1, 0755));
    EXPECT(0, pjd_chown(n1, 65534, 65534));

    snprintf(src, sizeof(src), "%s/%s", n1, n2);
    snprintf(dst, sizeof(dst), "%s/%s", n1, n3);

    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_create(src, 0644));
    EXPECT(0, pjd_link(src, dst));
    EXPECT(0, pjd_unlink(dst));
    pjd_set_root();

    /* No search permission on n1 -> EACCES for both source and dest prefix. */
    EXPECT(0, pjd_chmod(n1, 0644));
    pjd_set_user(65534, 65534);
    EXPECT(EACCES, pjd_link(src, dst));
    pjd_set_root();

    EXPECT(0, pjd_chmod(n1, 0755));
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_link(src, dst));
    EXPECT(0, pjd_unlink(dst));
    EXPECT(0, pjd_unlink(src));
    pjd_set_root();

    EXPECT(0, pjd_rmdir(n1));
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n0));

    return pjd_end();
} /* main */
