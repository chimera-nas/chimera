// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/truncate/09.t:
 * truncate returns EISDIR if the named file is a directory. */

#include "../../pjd_common.h"

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen();

    EXPECT(0, pjd_mkdir(n0, 0755));
    EXPECT(EISDIR, pjd_truncate(n0, 123));
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
