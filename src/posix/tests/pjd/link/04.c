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

/* Ported from pjdfstest tests/link/04.t: ENOENT if a component of either path prefix does not exist. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen(), *n2 = pjd_namegen();
    char  src[256], P[512];
    EXPECT(0, pjd_mkdir(n0, 0755));
    snprintf(src, sizeof(src), "%s/%s", n0, n1);
    EXPECT(0, pjd_create(src, 0644));
    snprintf(P, sizeof(P), "%s/%s/test", n0, n2);
    EXPECT(ENOENT, pjd_link(src, P));
    EXPECT(ENOENT, pjd_link(P, src));
    EXPECT(0, pjd_unlink(src));
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
