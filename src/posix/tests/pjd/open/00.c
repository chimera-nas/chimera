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

/* Ported from pjdfstest tests/open/00.t:
 * "open opens (and eventually creates) a file" - mode/umask application, uid/gid
 * inheritance of a newly created file, and parent/child timestamp updates. */

#include "../../pjd_common.h"

static void
expect_open_create(
    const char *name,
    int         flags,
    mode_t      mode)
{
    int fd = pjd_open(name, flags, mode);

    pjd_record(fd >= 0, __FILE__, __LINE__, "open(%s) -> %d", name, fd);
    if (fd >= 0) {
        chimera_posix_close(fd);
    }
} /* expect_open_create */

int
main(
    int    argc,
    char **argv)
{
    struct timespec t0, t1;

    pjd_begin(argc, argv);

    char           *n0 = pjd_namegen();
    char           *n1 = pjd_namegen();

    EXPECT(0, pjd_mkdir(n1, 0755));
    pjd_cd(n1);

    /* O_CREAT applies the mode (umask 0 by default). */
    expect_open_create(n0, O_CREAT | O_WRONLY, 0755);
    EXPECT_EQ(S_IFREG, pjd_lstat_type(n0));
    EXPECT_EQ(0755, pjd_lstat_mode(n0));
    EXPECT(0, pjd_unlink(n0));

    expect_open_create(n0, O_CREAT | O_WRONLY, 0151);
    EXPECT_EQ(0151, pjd_lstat_mode(n0));
    EXPECT(0, pjd_unlink(n0));

    /* umask applied: 0151 & ~077 = 0100. */
    chimera_posix_umask(0077);
    expect_open_create(n0, O_CREAT | O_WRONLY, 0151);
    chimera_posix_umask(0);
    EXPECT_EQ(0100, pjd_lstat_mode(n0));
    EXPECT(0, pjd_unlink(n0));

    /* 0345 & ~070 = 0305. */
    chimera_posix_umask(0070);
    expect_open_create(n0, O_CREAT | O_WRONLY, 0345);
    chimera_posix_umask(0);
    EXPECT_EQ(0305, pjd_lstat_mode(n0));
    EXPECT(0, pjd_unlink(n0));

    /* 0345 & ~0501 = 0244. */
    chimera_posix_umask(0501);
    expect_open_create(n0, O_CREAT | O_WRONLY, 0345);
    chimera_posix_umask(0);
    EXPECT_EQ(0244, pjd_lstat_mode(n0));
    EXPECT(0, pjd_unlink(n0));

    /* New file's uid = effective uid; gid = parent gid or effective gid. */
    EXPECT(0, pjd_chown(".", 65534, 65534));

    pjd_set_user(65534, 65534);
    expect_open_create(n0, O_CREAT | O_WRONLY, 0644);
    pjd_set_root();
    EXPECT_EQ(65534, pjd_lstat_uid(n0));
    EXPECT_EQ(65534, pjd_lstat_gid(n0));
    EXPECT(0, pjd_unlink(n0));

    pjd_set_user(65534, 65533);
    expect_open_create(n0, O_CREAT | O_WRONLY, 0644);
    pjd_set_root();
    EXPECT_EQ(65534, pjd_lstat_uid(n0));
    {
        long g = pjd_lstat_gid(n0);
        PJD_CHECK(g == 65533 || g == 65534, "new file gid %ld in {65533,65534}", g);
    }
    EXPECT(0, pjd_unlink(n0));

    EXPECT(0, pjd_chmod(".", 0777));
    pjd_set_user(65533, 65532);
    expect_open_create(n0, O_CREAT | O_WRONLY, 0644);
    pjd_set_root();
    EXPECT_EQ(65533, pjd_lstat_uid(n0));
    {
        long g = pjd_lstat_gid(n0);
        PJD_CHECK(g == 65532 || g == 65534, "new file gid %ld in {65532,65534}", g);
    }
    EXPECT(0, pjd_unlink(n0));

    /* Creating a new file updates the parent directory's mtime and ctime. */
    EXPECT(0, pjd_chown(".", 0, 0));
    pjd_stat_ctime(".", &t0);
    pjd_settle();
    expect_open_create(n0, O_CREAT | O_WRONLY, 0644);
    pjd_stat_mtime(".", &t1);
    PJD_CHECK(pjd_timespec_lt(&t0, &t1), "parent mtime advanced on create");
    pjd_stat_ctime(".", &t1);
    PJD_CHECK(pjd_timespec_lt(&t0, &t1), "parent ctime advanced on create");
    EXPECT(0, pjd_unlink(n0));

    /* Opening an existing file does NOT update the parent's mtime/ctime. */
    EXPECT(0, pjd_create(n0, 0644));
    {
        struct timespec dm, dc, m, c;
        pjd_stat_mtime(".", &dm);
        pjd_stat_ctime(".", &dc);
        pjd_settle();
        expect_open_create(n0, O_CREAT | O_RDONLY, 0644);
        pjd_stat_mtime(".", &m);
        pjd_stat_ctime(".", &c);
        PJD_CHECK(dm.tv_sec == m.tv_sec && dm.tv_nsec == m.tv_nsec,
                  "parent mtime unchanged opening existing");
        PJD_CHECK(dc.tv_sec == c.tv_sec && dc.tv_nsec == c.tv_nsec,
                  "parent ctime unchanged opening existing");
    }
    EXPECT(0, pjd_unlink(n0));

    /* O_TRUNC on an existing file updates its mtime/ctime and zeroes size. */
    {
        int             fd = pjd_open(n0, O_CREAT | O_WRONLY, 0644);
        PJD_CHECK(fd >= 0, "create n0 for trunc test");
        if (fd >= 0) {
            chimera_posix_write(fd, "test\n", 5);
            chimera_posix_close(fd);
        }
        EXPECT_EQ(5, pjd_stat_size(n0));

        struct timespec m1, c1, m2, c2;
        pjd_stat_mtime(n0, &m1);
        pjd_stat_ctime(n0, &c1);
        pjd_settle();
        expect_open_create(n0, O_WRONLY | O_TRUNC, 0644);
        pjd_stat_mtime(n0, &m2);
        pjd_stat_ctime(n0, &c2);
        PJD_CHECK(pjd_timespec_lt(&m1, &m2), "mtime advanced on O_TRUNC");
        PJD_CHECK(pjd_timespec_lt(&c1, &c2), "ctime advanced on O_TRUNC");
        EXPECT_EQ(0, pjd_stat_size(n0));
        EXPECT(0, pjd_unlink(n0));
    }

    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n1));

    return pjd_end();
} /* main */
