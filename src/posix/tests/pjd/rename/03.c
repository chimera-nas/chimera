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

/* Ported from pjdfstest tests/rename/03.t: ENOENT if a component of the 'from' path does not exist. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen(), *n2 = pjd_namegen();
    char  from[256], to[256];
    EXPECT(0, pjd_mkdir(n0, 0755));
    pjd_cd(n0);
    snprintf(from, sizeof(from), "%s/test", n1);   /* n1 doesn't exist */
    EXPECT(ENOENT, pjd_rename(from, n2));
    EXPECT(ENOENT, pjd_rename(n1, n2));            /* plain missing source */
    (void) to;
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
