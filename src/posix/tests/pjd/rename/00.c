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

/* Ported from pjdfstest tests/rename/00.t:
 * rename changes a file's name while preserving its inode, mode and link count;
 * renaming one of several links and renaming directories both behave correctly. */

#include "../../pjd_common.h"

static const enum pjd_ftype types[] = {
    PJD_FT_REGULAR, PJD_FT_FIFO, PJD_FT_BLOCK, PJD_FT_CHAR, PJD_FT_SOCKET,
};

static void
check(
    const char *name,
    int         mode,
    long        inode,
    long        nlink)
{
    struct stat st;

    if (pjd_lstat(name, &st) != 0) {
        PJD_CHECK(0, "lstat %s", name);
        return;
    }
    if (mode >= 0) {
        EXPECT_EQ(mode, st.st_mode & 07777);
    }
    if (inode >= 0) {
        EXPECT_EQ(inode, (long) st.st_ino);
    }
    EXPECT_EQ(nlink, (long) st.st_nlink);
} /* check */

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char *n0 = pjd_namegen();
    char *n1 = pjd_namegen();
    char *n2 = pjd_namegen();
    char *n3 = pjd_namegen();

    EXPECT(0, pjd_mkdir(n3, 0755));
    pjd_cd(n3);

    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        struct stat junk;

        EXPECT(0, pjd_create_file(types[i], n0));
        EXPECT(0, pjd_chmod(n0, 0644));
        long        inode = pjd_lstat_inode(n0);
        check(n0, 0644, inode, 1);

        EXPECT(0, pjd_rename(n0, n1));
        EXPECT(ENOENT, pjd_lstat(n0, &junk));
        check(n1, 0644, inode, 1);

        EXPECT(0, pjd_link(n1, n0));
        check(n0, 0644, inode, 2);
        check(n1, 0644, inode, 2);

        EXPECT(0, pjd_rename(n1, n2));
        check(n0, 0644, inode, 2);
        EXPECT(ENOENT, pjd_lstat(n1, &junk));
        check(n2, 0644, inode, 2);

        EXPECT(0, pjd_unlink(n0));
        EXPECT(0, pjd_unlink(n2));
    }

    /* Directory rename preserves the inode. */
    {
        struct stat junk;
        EXPECT(0, pjd_mkdir(n0, 0755));
        EXPECT_EQ(S_IFDIR, pjd_lstat_type(n0));
        long        inode = pjd_lstat_inode(n0);
        EXPECT(0, pjd_rename(n0, n1));
        EXPECT(ENOENT, pjd_lstat(n0, &junk));
        EXPECT_EQ(S_IFDIR, pjd_lstat_type(n1));
        EXPECT_EQ(inode, pjd_lstat_inode(n1));
        EXPECT(0, pjd_rmdir(n1));
    }

    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n3));

    return pjd_end();
} /* main */
