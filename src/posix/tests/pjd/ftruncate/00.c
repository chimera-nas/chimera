// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/ftruncate/00.t:
 * ftruncate decreases/increases file size on a writable descriptor, updates
 * ctime, fails with EINVAL on a read-only descriptor, and is governed by the
 * descriptor's access mode rather than the file's mode bits.
 *
 * (ftruncate/01-12,14 in pjdfstest are byte-for-byte copies of the path-based
 * truncate tests and are covered by the truncate category.) */

#include "../../pjd_common.h"

/* open `name` with flags, ftruncate to len, return ftruncate's errno-or-0. */
static int
open_ftruncate(
    const char *name,
    int         flags,
    off_t       len)
{
    int fd = pjd_open(name, flags, 0644);
    int rc;

    if (fd < 0) {
        return errno ? errno : EBADF;
    }
    rc = chimera_posix_ftruncate(fd, len);
    int e = (rc < 0) ? errno : 0;
    chimera_posix_close(fd);
    return e;
} /* open_ftruncate */

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
    EXPECT_EQ(0, open_ftruncate(n0, O_RDWR, 1234567));
    EXPECT_EQ(1234567, pjd_stat_size(n0));
    EXPECT_EQ(0, open_ftruncate(n0, O_WRONLY, 567));
    EXPECT_EQ(567, pjd_stat_size(n0));
    EXPECT(0, pjd_unlink(n0));

    /* Successful ftruncate updates ctime. */
    EXPECT(0, pjd_create(n0, 0644));
    pjd_stat_ctime(n0, &c1);
    pjd_settle();
    EXPECT_EQ(0, open_ftruncate(n0, O_RDWR, 123));
    pjd_stat_ctime(n0, &c2);
    PJD_CHECK(pjd_timespec_lt(&c1, &c2), "ctime advanced after ftruncate");
    EXPECT(0, pjd_unlink(n0));

    /* ftruncate on a read-only descriptor fails with EINVAL and leaves ctime. */
    EXPECT(0, pjd_create(n0, 0644));
    pjd_stat_ctime(n0, &c1);
    pjd_settle();
    pjd_set_user(65534, 65534);
    EXPECT_EQ(EINVAL, open_ftruncate(n0, O_RDONLY, 123));
    pjd_set_root();
    pjd_stat_ctime(n0, &c2);
    PJD_CHECK(c1.tv_sec == c2.tv_sec && c1.tv_nsec == c2.tv_nsec,
              "ctime unchanged after failed ftruncate");
    EXPECT(0, pjd_unlink(n0));

    /* The file mode does not affect ftruncate -- only the descriptor's access
     * mode does.  A freshly created mode-0 file opened O_RDWR can be ftruncated,
     * by its owner and by an unprivileged creator alike. */
    EXPECT_EQ(0, open_ftruncate(n0, O_CREAT | O_RDWR, 0));
    EXPECT(0, pjd_unlink(n0));

    EXPECT(0, pjd_chmod(".", 0777));
    pjd_set_user(65534, 65534);
    EXPECT_EQ(0, open_ftruncate(n0, O_CREAT | O_RDWR, 0));
    EXPECT(0, pjd_unlink(n0));
    pjd_set_root();

    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n1));

    return pjd_end();
} /* main */
