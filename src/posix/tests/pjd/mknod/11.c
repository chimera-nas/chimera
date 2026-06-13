// SPDX-FileCopyrightText: 2006-2012 Pawel Jakub Dawidek <pawel@dawidek.net>
// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: BSD-2-Clause
//
// Derived from the pjdfstest POSIX filesystem test suite
// (https://github.com/pjd/pjdfstest) by Pawel Jakub Dawidek.  These are C
// reimplementations of the upstream shell test cases, run against the Chimera
// POSIX client, and are distributed under pjdfstest's original 2-clause BSD
// license.

/* Ported from pjdfstest tests/mknod/11.t:
 * mknod creates character and block device files with the requested major/minor
 * numbers, returns EEXIST on a second create, and updates node/parent times. */

#include "../../pjd_common.h"

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char *n0 = pjd_namegen();
    char *n1 = pjd_namegen();

    EXPECT(0, pjd_mkdir(n1, 0755));
    pjd_cd(n1);

    struct {
        mode_t fmt;
        mode_t type_bits;
    } kinds[] = { { S_IFCHR, S_IFCHR }, { S_IFBLK, S_IFBLK } };

    for (int k = 0; k < 2; k++) {
        struct timespec t0, t1;

        EXPECT(0, pjd_mknod(n0, kinds[k].fmt | 0755, makedev(1, 2)));
        EXPECT_EQ(kinds[k].type_bits, pjd_lstat_type(n0));
        EXPECT_EQ(0755, pjd_lstat_mode(n0));
        EXPECT_EQ(1, pjd_lstat_major(n0));
        EXPECT_EQ(2, pjd_lstat_minor(n0));
        EXPECT(EEXIST, pjd_mknod(n0, kinds[k].fmt | 0777, makedev(3, 4)));
        EXPECT(0, pjd_unlink(n0));

        /* Timestamps: node + parent updated on successful mknod. */
        EXPECT(0, pjd_chown(".", 0, 0));
        pjd_stat_ctime(".", &t0);
        pjd_settle();
        EXPECT(0, pjd_mknod(n0, kinds[k].fmt | 0755, makedev(1, 2)));
        pjd_stat_mtime(".", &t1);
        PJD_CHECK(pjd_timespec_lt(&t0, &t1), "parent mtime advanced (kind %d)", k);
        pjd_stat_ctime(".", &t1);
        PJD_CHECK(pjd_timespec_lt(&t0, &t1), "parent ctime advanced (kind %d)", k);
        pjd_stat_mtime(n0, &t1);
        PJD_CHECK(pjd_timespec_lt(&t0, &t1), "node mtime set (kind %d)", k);
        EXPECT(0, pjd_unlink(n0));
    }

    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n1));

    return pjd_end();
} /* main */
