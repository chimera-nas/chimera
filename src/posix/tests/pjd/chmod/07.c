// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/chmod/07.t:
 * chmod returns EPERM if the effective user ID is neither the file owner nor
 * the super-user (also via a symlink to the target). */

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
    char  n1n2[256], n1n3[256];

    EXPECT(0, pjd_mkdir(n0, 0755));
    pjd_cd(n0);

    EXPECT(0, pjd_mkdir(n1, 0755));
    EXPECT(0, pjd_chown(n1, 65534, 65534));

    snprintf(n1n2, sizeof(n1n2), "%s/%s", n1, n2);
    snprintf(n1n3, sizeof(n1n3), "%s/%s", n1, n3);

    /* Owner (65534) may chmod; a different unprivileged user may not. */
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_create(n1n2, 0644));
    EXPECT(0, pjd_chmod(n1n2, 0642));
    pjd_set_root();
    EXPECT_EQ(0642, pjd_stat_mode(n1n2));

    pjd_set_user(65533, 65533);
    EXPECT(EPERM, pjd_chmod(n1n2, 0641));
    pjd_set_root();
    EXPECT_EQ(0642, pjd_stat_mode(n1n2));

    /* After chown to root, the former owner can no longer chmod. */
    EXPECT(0, pjd_chown(n1n2, 0, 0));
    pjd_set_user(65534, 65534);
    EXPECT(EPERM, pjd_chmod(n1n2, 0641));
    pjd_set_root();
    EXPECT_EQ(0642, pjd_stat_mode(n1n2));
    EXPECT(0, pjd_unlink(n1n2));

    /* Same, but chmod via a symlink to the target. */
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_create(n1n2, 0644));
    EXPECT(0, pjd_symlink(n2, n1n3));
    EXPECT(0, pjd_chmod(n1n3, 0642));
    pjd_set_root();
    EXPECT_EQ(0642, pjd_stat_mode(n1n2));
    EXPECT_EQ(65534, pjd_lstat_uid(n1n2));

    pjd_set_user(65533, 65533);
    EXPECT(EPERM, pjd_chmod(n1n3, 0641));
    pjd_set_root();
    EXPECT_EQ(0642, pjd_stat_mode(n1n2));

    EXPECT(0, pjd_chown(n1n3, 0, 0));
    pjd_set_user(65534, 65534);
    EXPECT(EPERM, pjd_chmod(n1n3, 0641));
    pjd_set_root();
    EXPECT_EQ(0642, pjd_stat_mode(n1n2));
    EXPECT_EQ(0, pjd_lstat_uid(n1n2));

    EXPECT(0, pjd_unlink(n1n2));
    EXPECT(0, pjd_unlink(n1n3));

    EXPECT(0, pjd_rmdir(n1));
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n0));

    return pjd_end();
} /* main */
