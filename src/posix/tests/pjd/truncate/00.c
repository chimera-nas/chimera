// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/truncate/00.t:
 * truncate decreases/increases file size and updates ctime on success (but not
 * on a failed, permission-denied truncate). */

#include "../../pjd_common.h"

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char           *n0 = pjd_namegen();
    char           *n1 = pjd_namegen();
    struct timespec c1, c2;

    EXPECT(0, pjd_mkdir(n1, 0755));
    pjd_cd(n1);

    EXPECT(0, pjd_create(n0, 0644));
    EXPECT(0, pjd_truncate(n0, 1234567));
    EXPECT_EQ(1234567, pjd_stat_size(n0));
    EXPECT(0, pjd_truncate(n0, 567));
    EXPECT_EQ(567, pjd_stat_size(n0));
    EXPECT(0, pjd_unlink(n0));

    /* Grow from data, then shrink. */
    {
        int fd = pjd_open(n0, O_CREAT | O_WRONLY, 0644);
        PJD_CHECK(fd >= 0, "create n0 with data");
        if (fd >= 0) {
            char buf[12345];
            memset(buf, 'a', sizeof(buf));
            chimera_posix_write(fd, buf, sizeof(buf));
            chimera_posix_close(fd);
        }
    }
    EXPECT(0, pjd_truncate(n0, 23456));
    EXPECT_EQ(23456, pjd_stat_size(n0));
    EXPECT(0, pjd_truncate(n0, 1));
    EXPECT_EQ(1, pjd_stat_size(n0));
    EXPECT(0, pjd_unlink(n0));

    /* Successful truncate updates ctime. */
    EXPECT(0, pjd_create(n0, 0644));
    pjd_stat_ctime(n0, &c1);
    pjd_settle();
    EXPECT(0, pjd_truncate(n0, 123));
    pjd_stat_ctime(n0, &c2);
    PJD_CHECK(pjd_timespec_lt(&c1, &c2), "ctime advanced after truncate");
    EXPECT(0, pjd_unlink(n0));

    /* Unsuccessful truncate does not update ctime. */
    EXPECT(0, pjd_create(n0, 0644));
    pjd_stat_ctime(n0, &c1);
    pjd_settle();
    pjd_set_user(65534, 65534);
    EXPECT(EACCES, pjd_truncate(n0, 123));
    pjd_set_root();
    pjd_stat_ctime(n0, &c2);
    PJD_CHECK(c1.tv_sec == c2.tv_sec && c1.tv_nsec == c2.tv_nsec,
              "ctime unchanged after failed truncate");
    EXPECT(0, pjd_unlink(n0));

    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n1));

    return pjd_end();
} /* main */
