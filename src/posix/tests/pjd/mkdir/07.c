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

/* Ported from pjdfstest tests/mkdir/07.t: ELOOP on a symlink loop in the pathname. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen();
    char  n0t[256], n1t[256];
    EXPECT(0, pjd_symlink(n0, n1));
    EXPECT(0, pjd_symlink(n1, n0));
    snprintf(n0t, sizeof(n0t), "%s/test", n0);
    snprintf(n1t, sizeof(n1t), "%s/test", n1);
    EXPECT(ELOOP, pjd_mkdir(n0t, 0755));
    EXPECT(ELOOP, pjd_mkdir(n1t, 0755));
    EXPECT(0, pjd_unlink(n0));
    EXPECT(0, pjd_unlink(n1));
    return pjd_end();
} /* main */
