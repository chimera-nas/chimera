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

/* Ported from pjdfstest tests/ftruncate/13.t:
 * ftruncate returns EINVAL if the length argument was less than 0. */

#include "../../pjd_common.h"

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char *n0 = pjd_namegen();
    int   fd;

    EXPECT(0, pjd_create(n0, 0644));

    fd = pjd_open(n0, O_RDWR, 0644);
    PJD_CHECK(fd >= 0, "open n0 O_RDWR");
    EXPECT(EINVAL, chimera_posix_ftruncate(fd, -1));
    if (fd >= 0) {
        chimera_posix_close(fd);
    }

    fd = pjd_open(n0, O_WRONLY, 0644);
    PJD_CHECK(fd >= 0, "open n0 O_WRONLY");
    EXPECT(EINVAL, chimera_posix_ftruncate(fd, -999999));
    if (fd >= 0) {
        chimera_posix_close(fd);
    }

    EXPECT(0, pjd_unlink(n0));

    return pjd_end();
} /* main */
