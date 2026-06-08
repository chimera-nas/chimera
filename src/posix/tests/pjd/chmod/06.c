// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/chmod/06.t:
 * chmod returns ELOOP if too many symbolic links were encountered in
 * translating the pathname. */

#include "../../pjd_common.h"

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);

    char *n0 = pjd_namegen();
    char *n1 = pjd_namegen();
    char  n0t[256], n1t[256];

    /* Two symlinks pointing at each other form a loop. */
    EXPECT(0, pjd_symlink(n0, n1));
    EXPECT(0, pjd_symlink(n1, n0));

    EXPECT(ELOOP, pjd_chmod(n0, 0644));
    EXPECT(ELOOP, pjd_chmod(n1, 0644));

    snprintf(n0t, sizeof(n0t), "%s/test", n0);
    snprintf(n1t, sizeof(n1t), "%s/test", n1);
    EXPECT(ELOOP, pjd_chmod(n0t, 0644));
    EXPECT(ELOOP, pjd_chmod(n1t, 0644));

    EXPECT(0, pjd_unlink(n0));
    EXPECT(0, pjd_unlink(n1));

    return pjd_end();
} /* main */
