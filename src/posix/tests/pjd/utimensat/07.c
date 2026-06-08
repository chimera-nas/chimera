// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/utimensat/07.t: setting explicit timestamps requires
 * ownership (or super-user); a non-owner gets EPERM even with write permission. */
#include "../../pjd_common.h"
#include <sys/stat.h>
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char        *n0 = pjd_namegen(), *n1 = pjd_namegen();
    const time_t D1 = 1900000000, D2 = 1950000000;
    EXPECT(0, pjd_mkdir(n1, 0755));
    pjd_cd(n1);
    EXPECT(0, pjd_create(n0, 0644));   /* owned root */

    /* Non-owner cannot set explicit times. */
    pjd_set_user(65534, 65534);
    EXPECT_EQ(EPERM, pjd_utimensat(n0, 0, UTIME_OMIT, D2, 0, 0) < 0 ? errno : 0);
    EXPECT_EQ(EPERM, pjd_utimensat(n0, D1, 0, 0, UTIME_OMIT, 0) < 0 ? errno : 0);
    EXPECT_EQ(EPERM, pjd_utimensat(n0, D1, 0, D2, 0, 0) < 0 ? errno : 0);
    pjd_set_root();

    /* Non-owner with write still cannot set explicit times. */
    EXPECT(0, pjd_chmod(n0, 0666));
    pjd_set_user(65534, 65534);
    EXPECT_EQ(EPERM, pjd_utimensat(n0, D1, 0, D2, 0, 0) < 0 ? errno : 0);
    pjd_set_root();

    /* Owner can. */
    EXPECT(0, pjd_chown(n0, 65534, 65534));
    EXPECT(0, pjd_chmod(n0, 0444));
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_utimensat(n0, D1, 0, D2, 0, 0));
    pjd_set_root();

    /* Super-user can. */
    EXPECT(0, pjd_chown(n0, 0, 0));
    EXPECT(0, pjd_utimensat(n0, D1, 0, D2, 0, 0));

    EXPECT(0, pjd_unlink(n0));
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n1));
    return pjd_end();
} /* main */
