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

/* Ported from pjdfstest tests/utimensat/06.t: UTIME_NOW works for the owner, the
 * super-user, or anyone with write permission; otherwise EACCES. */
#include "../../pjd_common.h"
#include <sys/stat.h>
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen();
    EXPECT(0, pjd_mkdir(n1, 0755));
    pjd_cd(n1);
    EXPECT(0, pjd_create(n0, 0644));   /* owned root */

    /* Non-owner without write -> EACCES. */
    pjd_set_user(65534, 65534);
    EXPECT_EQ(EACCES, pjd_utimensat(n0, 0, UTIME_NOW, 0, UTIME_NOW, 0) < 0 ? errno : 0);
    pjd_set_root();

    /* Owner (even read-only mode) can touch. */
    EXPECT(0, pjd_chown(n0, 65534, 65534));
    EXPECT(0, pjd_chmod(n0, 0444));
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_utimensat(n0, 0, UTIME_NOW, 0, UTIME_NOW, 0));
    pjd_set_root();

    /* Super-user can touch. */
    EXPECT(0, pjd_utimensat(n0, 0, UTIME_OMIT, 0, UTIME_OMIT, 0));

    /* Non-owner with write permission can touch. */
    EXPECT(0, pjd_chown(n0, 0, 0));
    EXPECT(0, pjd_chmod(n0, 0666));
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_utimensat(n0, 0, UTIME_NOW, 0, UTIME_NOW, 0));
    pjd_set_root();

    EXPECT(0, pjd_unlink(n0));
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n1));
    return pjd_end();
} /* main */
