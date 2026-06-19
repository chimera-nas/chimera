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

/* Ported from pjdfstest tests/unlink/14.t: an open file is not freed by unlink;
 * stat/I/O via the open descriptor keep working and nlink drops to 0. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n2 = pjd_namegen();
    EXPECT(0, pjd_mkdir(n2, 0755));
    pjd_cd(n2);

    /* nlink of an open-but-unlinked file is 0.
     *
     * On NFSv3 the client silly-renames the still-open file to .nfsXXXX rather
     * than removing it (there is no server-side open-file state), so its link
     * count stays 1 until close -- the correct NFSv3 behavior, not a local
     * unlink.  Skip the nlink==0 assertion there but still exercise the path. */
    EXPECT(0, pjd_create(n0, 0644));
    {
        int         fd = pjd_open(n0, O_RDONLY, 0644);
        PJD_CHECK(fd >= 0, "open n0");
        EXPECT(0, pjd_unlink(n0));
        struct stat stbuf;
        EXPECT(0, chimera_posix_fstat(fd, &stbuf));
        if (!pjd_backend_is_nfs3()) {
            EXPECT_EQ(0, (long) stbuf.st_nlink);
        }
        chimera_posix_close(fd);
    }

    /* I/O to an open-but-unlinked file still works. */
    EXPECT(0, pjd_create(n0, 0644));
    {
        int     fd = pjd_open(n0, O_RDWR, 0644);
        char    buf[16];
        PJD_CHECK(fd >= 0, "open n0 rdwr");
        chimera_posix_write(fd, "Hello,_World!", 13);
        EXPECT(0, pjd_unlink(n0));
        ssize_t r = chimera_posix_pread(fd, buf, 13, 0);
        PJD_CHECK(r == 13 && memcmp(buf, "Hello,_World!", 13) == 0,
                  "pread after unlink returns data (%zd)", r);
        chimera_posix_close(fd);
    }

    pjd_cd("..");

    /* On NFSv3 the close above triggers removal of the silly-renamed .nfsXXXX
     * entry, but the client dispatches it asynchronously (close(2) does not
     * block on it here), so the directory may not be empty the instant rmdir
     * runs.  Poll briefly until the silly entry drains.  Local backends remove
     * the file synchronously and succeed on the first attempt. */
    if (pjd_backend_is_nfs3()) {
        int rc = -1;
        for (int i = 0; i < 100 && rc != 0; i++) {
            rc = pjd_rmdir(n2);
            if (rc != 0) {
                pjd_settle();
            }
        }
        EXPECT_EQ(0, rc);
    } else {
        EXPECT(0, pjd_rmdir(n2));
    }
    return pjd_end();
} /* main */
