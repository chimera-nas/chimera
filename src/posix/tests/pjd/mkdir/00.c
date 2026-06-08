// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/mkdir/00.t:
 * mkdir creates directories - mode/umask application, uid/gid inheritance, and
 * new-dir / parent-dir timestamp updates. */

#include "../../pjd_common.h"

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char           *n0 = pjd_namegen();
    char           *n1 = pjd_namegen();
    struct timespec t0, t1;

    EXPECT(0, pjd_mkdir(n1, 0755));
    pjd_cd(n1);

    /* mode honored, with umask applied. */
    EXPECT(0, pjd_mkdir(n0, 0755));
    EXPECT_EQ(S_IFDIR, pjd_lstat_type(n0));
    EXPECT_EQ(0755, pjd_lstat_mode(n0));
    EXPECT(0, pjd_rmdir(n0));

    EXPECT(0, pjd_mkdir(n0, 0151));
    EXPECT_EQ(0151, pjd_lstat_mode(n0));
    EXPECT(0, pjd_rmdir(n0));

    chimera_posix_umask(0077);
    EXPECT(0, pjd_mkdir(n0, 0151));
    chimera_posix_umask(0);
    EXPECT_EQ(0100, pjd_lstat_mode(n0));
    EXPECT(0, pjd_rmdir(n0));

    chimera_posix_umask(0070);
    EXPECT(0, pjd_mkdir(n0, 0345));
    chimera_posix_umask(0);
    EXPECT_EQ(0305, pjd_lstat_mode(n0));
    EXPECT(0, pjd_rmdir(n0));

    chimera_posix_umask(0501);
    EXPECT(0, pjd_mkdir(n0, 0345));
    chimera_posix_umask(0);
    EXPECT_EQ(0244, pjd_lstat_mode(n0));
    EXPECT(0, pjd_rmdir(n0));

    /* New dir's uid = effective uid; gid = parent gid or effective gid. */
    EXPECT(0, pjd_chown(".", 65534, 65534));

    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_mkdir(n0, 0755));
    pjd_set_root();
    EXPECT_EQ(65534, pjd_lstat_uid(n0));
    EXPECT_EQ(65534, pjd_lstat_gid(n0));
    EXPECT(0, pjd_rmdir(n0));

    pjd_set_user(65534, 65533);
    EXPECT(0, pjd_mkdir(n0, 0755));
    pjd_set_root();
    EXPECT_EQ(65534, pjd_lstat_uid(n0));
    {
        long g = pjd_lstat_gid(n0);
        PJD_CHECK(g == 65533 || g == 65534, "new dir gid %ld in {65533,65534}", g);
    }
    EXPECT(0, pjd_rmdir(n0));

    EXPECT(0, pjd_chmod(".", 0777));
    pjd_set_user(65533, 65532);
    EXPECT(0, pjd_mkdir(n0, 0755));
    pjd_set_root();
    EXPECT_EQ(65533, pjd_lstat_uid(n0));
    {
        long g = pjd_lstat_gid(n0);
        PJD_CHECK(g == 65532 || g == 65534, "new dir gid %ld in {65532,65534}", g);
    }
    EXPECT(0, pjd_rmdir(n0));

    /* mkdir updates the parent's mtime/ctime and the new dir's timestamps. */
    EXPECT(0, pjd_chown(".", 0, 0));
    pjd_stat_ctime(".", &t0);
    pjd_settle();
    EXPECT(0, pjd_mkdir(n0, 0755));
    pjd_stat_mtime(".", &t1);
    PJD_CHECK(pjd_timespec_lt(&t0, &t1), "parent mtime advanced on mkdir");
    pjd_stat_ctime(".", &t1);
    PJD_CHECK(pjd_timespec_lt(&t0, &t1), "parent ctime advanced on mkdir");
    pjd_stat_mtime(n0, &t1);
    PJD_CHECK(pjd_timespec_lt(&t0, &t1), "new dir mtime set");
    EXPECT(0, pjd_rmdir(n0));

    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n1));

    return pjd_end();
} /* main */
