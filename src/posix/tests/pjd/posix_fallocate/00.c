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

/* Ported from pjdfstest tests/posix_fallocate/00.t:
 * posix_fallocate(3) reserves space for [offset, offset+len) and grows the file
 * to offset+len when that exceeds the current size; it updates ctime on
 * success, fails with EINVAL when len is 0, and is governed by the
 * descriptor's access mode rather than the created file's mode bits. */

#include <stdlib.h>

#include "../../pjd_common.h"

/* open `name` with flags/mode, posix_fallocate(offset,len), return errno-or-0. */
static int
open_fallocate(
    const char *name,
    int         flags,
    mode_t      mode,
    off_t       offset,
    off_t       len)
{
    int fd = pjd_open(name, flags, mode);
    int rc;

    if (fd < 0) {
        return errno ? errno : EBADF;
    }
    rc = chimera_posix_fallocate(fd, offset, len);
    int e = (rc < 0) ? errno : 0;
    chimera_posix_close(fd);
    return e;
} /* open_fallocate */

/* Write `len` zero bytes into an existing file (stands in for the upstream
 * `dd if=/dev/random` that gives the file a non-zero starting size). */
static int
fill_file(
    const char *name,
    size_t      len)
{
    int     fd = pjd_open(name, O_RDWR, 0644);

    if (fd < 0) {
        return errno ? errno : EBADF;
    }

    char   *buf = calloc(1, len);
    ssize_t w   = chimera_posix_write(fd, buf, len);

    free(buf);
    chimera_posix_close(fd);
    return (w == (ssize_t) len) ? 0 : -1;
} /* fill_file */

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char           *n0 = pjd_namegen();
    char           *n1 = pjd_namegen();
    struct timespec c1, c2;
    int             r;

    /* open_fallocate()/fill_file() create files, so each must be evaluated
     * exactly once -- capture the result before asserting (EXPECT_EQ evaluates
     * its arguments more than once). */

    EXPECT(0, pjd_mkdir(n1, 0755));
    pjd_cd(n1);

    /* fallocate grows an empty file to len. */
    EXPECT(0, pjd_create(n0, 0644));
    r = open_fallocate(n0, O_RDWR, 0644, 0, 567);
    EXPECT_EQ(0, r);
    EXPECT_EQ(567, pjd_stat_size(n0));
    EXPECT(0, pjd_unlink(n0));

    /* fallocate past EOF grows size to offset+len. */
    EXPECT(0, pjd_create(n0, 0644));
    r = fill_file(n0, 12345);
    EXPECT_EQ(0, r);
    r = open_fallocate(n0, O_RDWR, 0644, 20000, 3456);
    EXPECT_EQ(0, r);
    EXPECT_EQ(23456, pjd_stat_size(n0));
    EXPECT(0, pjd_unlink(n0));

    /* successful posix_fallocate updates ctime. */
    EXPECT(0, pjd_create(n0, 0644));
    pjd_stat_ctime(n0, &c1);
    pjd_settle();
    r = open_fallocate(n0, O_RDWR, 0644, 0, 123);
    EXPECT_EQ(0, r);
    pjd_stat_ctime(n0, &c2);
    PJD_CHECK(pjd_timespec_lt(&c1, &c2), "ctime advanced after posix_fallocate");
    EXPECT(0, pjd_unlink(n0));

    /* unsuccessful posix_fallocate (len 0 -> EINVAL) does not update ctime. */
    EXPECT(0, pjd_create(n0, 0644));
    pjd_stat_ctime(n0, &c1);
    pjd_settle();
    r = open_fallocate(n0, O_WRONLY, 0644, 0, 0);
    EXPECT_EQ(EINVAL, r);
    pjd_stat_ctime(n0, &c2);
    PJD_CHECK(c1.tv_sec == c2.tv_sec && c1.tv_nsec == c2.tv_nsec,
              "ctime unchanged after failed posix_fallocate");
    EXPECT(0, pjd_unlink(n0));

    /* The file mode of a newly created file must not affect whether
     * posix_fallocate works -- only the descriptor's access mode does.
     * (https://bugs.freebsd.org/bugzilla/show_bug.cgi?id=154873) */
    r = open_fallocate(n0, O_CREAT | O_RDWR, 0, 0, 1);
    EXPECT_EQ(0, r);
    EXPECT(0, pjd_unlink(n0));
    EXPECT(0, pjd_chmod(".", 0777));
    pjd_set_user(65534, 65534);
    r = open_fallocate(n0, O_CREAT | O_RDWR, 0, 0, 1);
    EXPECT_EQ(0, r);
    pjd_set_root();
    EXPECT(0, pjd_unlink(n0));

    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n1));

    pjd_end();
    return 0;
} /* main */
