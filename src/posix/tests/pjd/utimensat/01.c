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

/* Ported from pjdfstest tests/utimensat/01.t: UTIME_NOW sets timestamps to ~now. */
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
    EXPECT(0, pjd_create(n0, 0644));
    EXPECT(0, pjd_utimensat(n0, 0, 0, 0, 0, 0));   /* baseline epoch */
    EXPECT(0, pjd_utimensat(n0, 0, UTIME_NOW, 0, UTIME_NOW, 0));
    long  now = (long) time(NULL);
    PJD_CHECK(pjd_lstat_atime(n0) > now - 300 && pjd_lstat_atime(n0) <= now + 5, "atime ~ now");
    PJD_CHECK(pjd_lstat_mtime_sec(n0) > now - 300 && pjd_lstat_mtime_sec(n0) <= now + 5, "mtime ~ now");
    EXPECT(0, pjd_unlink(n0));
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n1));
    return pjd_end();
} /* main */
