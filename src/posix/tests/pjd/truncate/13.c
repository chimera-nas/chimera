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

/* Ported from pjdfstest tests/truncate/13.t:
 * truncate returns EINVAL if the length argument was less than 0. */

#include "../../pjd_common.h"

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen();

    EXPECT(0, pjd_create(n0, 0644));
    EXPECT(EINVAL, pjd_truncate(n0, -1));
    EXPECT(EINVAL, pjd_truncate(n0, -999999));
    EXPECT(0, pjd_unlink(n0));
    return pjd_end();
} /* main */
