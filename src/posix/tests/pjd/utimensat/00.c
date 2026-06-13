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

/* Ported from pjdfstest tests/utimensat/00.t: utimensat sets atime/mtime to the
 * given explicit values on any type of file. */
#include "../../pjd_common.h"
#include <sys/stat.h>
static const enum pjd_ftype types[] = { PJD_FT_REGULAR, PJD_FT_DIR, PJD_FT_FIFO, PJD_FT_BLOCK, PJD_FT_CHAR,
                                        PJD_FT_SOCKET };
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char        *n0 = pjd_namegen(), *n1 = pjd_namegen();
    const time_t DATE1 = 1900000000, DATE2 = 1950000000;
    EXPECT(0, pjd_mkdir(n1, 0755));
    pjd_cd(n1);
    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        EXPECT(0, pjd_create_file(types[i], n0));
        EXPECT(0, pjd_utimensat(n0, DATE1, 0, DATE2, 0, 0));
        EXPECT_EQ(DATE1, pjd_lstat_atime(n0));
        EXPECT_EQ(DATE2, pjd_lstat_mtime_sec(n0));
        EXPECT(0, pjd_remove_file(types[i], n0));
    }
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n1));
    return pjd_end();
} /* main */
