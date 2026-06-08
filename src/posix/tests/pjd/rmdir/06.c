// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/rmdir/06.t: EEXIST or ENOTEMPTY if the directory is not empty. */
#include "../../pjd_common.h"
static const enum pjd_ftype types[] = { PJD_FT_REGULAR, PJD_FT_DIR, PJD_FT_FIFO, PJD_FT_BLOCK, PJD_FT_CHAR,
                                        PJD_FT_SOCKET,  PJD_FT_SYMLINK };
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen();
    char  n0n1[256];
    EXPECT(0, pjd_mkdir(n0, 0755));
    snprintf(n0n1, sizeof(n0n1), "%s/%s", n0, n1);
    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        EXPECT(0, pjd_create_file(types[i], n0n1));
        int rc = pjd_rmdir(n0);
        int e  = (rc < 0) ? errno : 0;
        PJD_CHECK(e == EEXIST || e == ENOTEMPTY, "rmdir non-empty -> %s (want EEXIST/ENOTEMPTY)", strerror(e));
        EXPECT(0, pjd_remove_file(types[i], n0n1));
    }
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
