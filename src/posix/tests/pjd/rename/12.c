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

/* Ported from pjdfstest tests/rename/12.t: ENOTDIR if a component of either path prefix is not a directory. */
#include "../../pjd_common.h"
static const enum pjd_ftype types[] = { PJD_FT_REGULAR, PJD_FT_FIFO, PJD_FT_BLOCK, PJD_FT_CHAR, PJD_FT_SOCKET };
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen(), *n2 = pjd_namegen();
    char  src[256], P[512];
    EXPECT(0, pjd_mkdir(n0, 0755));
    pjd_cd(n0);
    EXPECT(0, pjd_mkdir(n1, 0755));
    snprintf(src, sizeof(src), "%s/%s", n1, n2);
    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        EXPECT(0, pjd_create_file(types[i], src));
        snprintf(P, sizeof(P), "%s/%s/test", n1, n2);
        EXPECT(ENOTDIR, pjd_rename(P, n2));   /* from prefix is a non-dir */
        EXPECT(ENOTDIR, pjd_rename(n1, P));   /* to prefix is a non-dir */
        EXPECT(0, pjd_unlink(src));
    }
    EXPECT(0, pjd_rmdir(n1));
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
