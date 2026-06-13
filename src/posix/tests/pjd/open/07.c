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

/* Ported from pjdfstest tests/open/07.t: O_TRUNC requires write permission (EACCES otherwise). */
#include "../../pjd_common.h"
struct row { int mode; int uid; int gid; };
static const struct row rows[] = {
    { 0477, 65534, 65534 }, { 0747, 65533, 65534 }, { 0774, 65533, 65533 },
    { 0177, 65534, 65534 }, { 0717, 65533, 65534 }, { 0771, 65533, 65533 },
    { 0077, 65534, 65534 }, { 0707, 65533, 65534 }, { 0770, 65533, 65533 },
};
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen();
    EXPECT(0, pjd_mkdir(n0, 0755));
    EXPECT(0, pjd_chown(n0, 65534, 65534));
    pjd_cd(n0);
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_create(n1, 0644));
    /* Owner can write then read back one byte. */
    {
        int         fd = pjd_open(n1, O_WRONLY, 0);
        PJD_CHECK(fd >= 0, "open n1 O_WRONLY");
        chimera_posix_write(fd, "x", 1);
        struct stat stbuf;
        chimera_posix_fstat(fd, &stbuf);
        EXPECT_EQ(1, (long) stbuf.st_size);
        chimera_posix_close(fd);
    }
    pjd_set_root();
    for (unsigned i = 0; i < sizeof(rows) / sizeof(rows[0]); i++) {
        pjd_set_user(65534, 65534);
        EXPECT(0, pjd_chmod(n1, rows[i].mode));
        pjd_set_root();
        pjd_set_user(rows[i].uid, rows[i].gid);
        EXPECT_EQ(EACCES, pjd_open_e(n1, O_RDONLY | O_TRUNC, 0));
        pjd_set_root();
    }
    pjd_set_user(65534, 65534);
    EXPECT(0, pjd_unlink(n1));
    pjd_set_root();
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
