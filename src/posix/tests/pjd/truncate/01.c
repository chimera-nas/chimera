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

/* Ported from pjdfstest tests/truncate/01.t:
 * truncate returns ENOTDIR if a component of the path prefix is not a directory. */

#include "../../pjd_common.h"

static const enum pjd_ftype types[] = {
    PJD_FT_REGULAR, PJD_FT_FIFO, PJD_FT_BLOCK, PJD_FT_CHAR, PJD_FT_SOCKET,
};

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen();
    char *n1 = pjd_namegen();
    char  sub[256], deeper[512];

    EXPECT(0, pjd_mkdir(n0, 0755));
    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        snprintf(sub, sizeof(sub), "%s/%s", n0, n1);
        EXPECT(0, pjd_create_file(types[i], sub));
        snprintf(deeper, sizeof(deeper), "%s/%s/test", n0, n1);
        EXPECT(ENOTDIR, pjd_truncate(deeper, 123));
        EXPECT(0, pjd_unlink(sub));
    }
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
