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

/* Ported from pjdfstest tests/open/23.t: open may return EINVAL for an illegal
 * combination of O_RDONLY/O_WRONLY/O_RDWR (accmode == 3).  POSIX leaves this
 * implementation-defined, so success is equally acceptable. */
#include "../../pjd_common.h"
#include <sys/stat.h>

static void
expect_open_accmode(
    const char *n0,
    int         flags)
{
    int e = pjd_open_e(n0, flags, 0);

    PJD_CHECK(e == 0 || e == EINVAL,
              "open accmode 0x%x -> %s (want 0 or EINVAL)", flags, strerror(e));
} /* expect_open_accmode */

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen();

    EXPECT(0, pjd_create(n0, 0644));
    expect_open_accmode(n0, O_RDONLY | O_RDWR);
    expect_open_accmode(n0, O_WRONLY | O_RDWR);
    expect_open_accmode(n0, O_RDONLY | O_WRONLY | O_RDWR);
    EXPECT(0, pjd_unlink(n0));
    return pjd_end();
} /* main */
