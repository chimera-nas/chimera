// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/mknod/00.t: creates FIFO files - mode/umask, uid/gid
 * inheritance, and new-node / parent-dir timestamp updates. */
#include "../../pjd_common.h"
static int mkf(
    const char *n,
    mode_t      m){ return pjd_mknod_fifo(n, m); }
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char           *n0 = pjd_namegen(), *n1 = pjd_namegen();
    struct timespec t0, t1;
    EXPECT(0, pjd_mkdir(n1, 0755));
    pjd_cd(n1);

    EXPECT(0, mkf(n0, 0755));
    EXPECT_EQ(S_IFIFO, pjd_lstat_type(n0));
    EXPECT_EQ(0755, pjd_lstat_mode(n0));
    EXPECT(0, pjd_unlink(n0));
    EXPECT(0, mkf(n0, 0151));
    EXPECT_EQ(0151, pjd_lstat_mode(n0));
    EXPECT(0, pjd_unlink(n0));
    chimera_posix_umask(0077); EXPECT(0, mkf(n0, 0151)); chimera_posix_umask(0);
    EXPECT_EQ(0100, pjd_lstat_mode(n0));
    EXPECT(0, pjd_unlink(n0));
    chimera_posix_umask(0070); EXPECT(0, mkf(n0, 0345)); chimera_posix_umask(0);
    EXPECT_EQ(0305, pjd_lstat_mode(n0));
    EXPECT(0, pjd_unlink(n0));
    chimera_posix_umask(0501); EXPECT(0, mkf(n0, 0345)); chimera_posix_umask(0);
    EXPECT_EQ(0244, pjd_lstat_mode(n0));
    EXPECT(0, pjd_unlink(n0));

    EXPECT(0, pjd_chown(".", 65534, 65534));
    pjd_set_user(65534, 65534); EXPECT(0, mkf(n0, 0755)); pjd_set_root();
    EXPECT_EQ(65534, pjd_lstat_uid(n0));
    EXPECT_EQ(65534, pjd_lstat_gid(n0));
    EXPECT(0, pjd_unlink(n0));
    pjd_set_user(65534, 65533); EXPECT(0, mkf(n0, 0755)); pjd_set_root();
    EXPECT_EQ(65534, pjd_lstat_uid(n0));
    { long g = pjd_lstat_gid(n0); PJD_CHECK(g == 65533 || g == 65534, "gid %ld", g); }
    EXPECT(0, pjd_unlink(n0));

    EXPECT(0, pjd_chown(".", 0, 0));
    pjd_stat_ctime(".", &t0);
    pjd_settle();
    EXPECT(0, mkf(n0, 0755));
    pjd_stat_mtime(".", &t1); PJD_CHECK(pjd_timespec_lt(&t0, &t1), "parent mtime");
    pjd_stat_ctime(".", &t1); PJD_CHECK(pjd_timespec_lt(&t0, &t1), "parent ctime");
    pjd_stat_mtime(n0, &t1);  PJD_CHECK(pjd_timespec_lt(&t0, &t1), "node mtime");
    EXPECT(0, pjd_unlink(n0));

    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n1));
    return pjd_end();
} /* main */
