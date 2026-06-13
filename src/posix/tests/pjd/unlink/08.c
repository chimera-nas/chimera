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

/* Ported from pjdfstest tests/unlink/08.t: unlink of a directory.  POSIX permits
 * EPERM; Linux returns EISDIR; some systems allow it (0).  Accept any. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen();
    EXPECT(0, pjd_mkdir(n0, 0755));
    int   rc = pjd_unlink(n0);
    int   e  = (rc < 0) ? errno : 0;
    PJD_CHECK(e == 0 || e == EPERM || e == EISDIR, "unlink dir -> %s", strerror(e));
    int   rc2 = pjd_rmdir(n0);
    int   e2  = (rc2 < 0) ? errno : 0;
    PJD_CHECK(e2 == 0 || e2 == ENOENT, "rmdir dir -> %s", strerror(e2));
    return pjd_end();
} /* main */
