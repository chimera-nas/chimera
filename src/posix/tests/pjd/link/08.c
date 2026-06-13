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

/* Ported from pjdfstest tests/link/08.t: ELOOP on a symlink loop in either pathname. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen(), *n2 = pjd_namegen();
    char  P0[256], P1[256];

    EXPECT(0, pjd_symlink(n0, n1));
    EXPECT(0, pjd_symlink(n1, n0));

    snprintf(P0, sizeof(P0), "%s/test", n0);
    snprintf(P1, sizeof(P1), "%s/test", n1);

    /* Loop on the source side. */
    EXPECT(ELOOP, pjd_link(P0, n2));
    EXPECT(ELOOP, pjd_link(P1, n2));

    /* Loop on the destination side (with a real, resolvable source). */
    EXPECT(0, pjd_create(n2, 0644));
    EXPECT(ELOOP, pjd_link(n2, P0));
    EXPECT(ELOOP, pjd_link(n2, P1));

    EXPECT(0, pjd_unlink(n0));
    EXPECT(0, pjd_unlink(n1));
    EXPECT(0, pjd_unlink(n2));
    return pjd_end();
} /* main */
