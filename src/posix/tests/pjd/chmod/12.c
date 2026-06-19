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

/* Ported from pjdfstest tests/chmod/12.t:
 * "verify SUID/SGID bit behaviour" - when a non-privileged process writes to a
 * regular file the kernel clears the set-user-ID bit, and clears the
 * set-group-ID bit when the file is group-executable.  Each case opens the file
 * as uid/gid 65534, writes a byte, and checks both fstat (open fd) and stat
 * (path) report the privileged bits cleared. */

#include "../../pjd_common.h"

/* Permission/special bits (mode & 07777) of an open fd; -1 on error. */
static int
fstat_mode(int fd)
{
    struct stat st;

    if (chimera_posix_fstat(fd, &st) != 0) {
        return -1;
    }
    return st.st_mode & 07777;
} /* fstat_mode */

/* Create `name` with `mode`, open it as uid/gid 65534 with `flags`, write one
 * byte, and assert both fstat (fd) and stat (path) report `expect`. */
static void
killpriv_case(
    const char *name,
    mode_t      mode,
    int         flags,
    int         expect)
{
    int     fd;
    ssize_t w;

    EXPECT(0, pjd_create(name, mode));

    pjd_set_user(65534, 65534);
    fd = pjd_open(name, flags, 0);
    PJD_CHECK(fd >= 0, "open %s as uid 65534 (fd=%d)", name, fd);

    if (fd >= 0) {
        w = chimera_posix_write(fd, "x", 1);
        PJD_CHECK(w == 1, "write 1 byte to %s (rc=%ld)", name, (long) w);
        EXPECT_EQ(expect, fstat_mode(fd));
        chimera_posix_close(fd);
    }
    pjd_set_root();

    EXPECT_EQ(expect, pjd_stat_mode(name));
    EXPECT(0, pjd_unlink(name));
} /* killpriv_case */

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char *n0 = pjd_namegen();
    char *n2 = pjd_namegen();

    EXPECT(0, pjd_mkdir(n2, 0755));
    pjd_cd(n2);

    /* Writing to the file by non-owner clears the SUID. */
    killpriv_case(n0, 04777, O_WRONLY, 0777);

    /* Writing to the file by non-owner clears the SGID (group-executable). */
    killpriv_case(n0, 02777, O_RDWR, 0777);

    /* Writing to the file by non-owner clears both SUID and SGID. */
    killpriv_case(n0, 06777, O_RDWR, 0777);

    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n2));

    return pjd_end();
} /* main */
