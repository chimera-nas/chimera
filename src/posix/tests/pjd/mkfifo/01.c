// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/mkfifo/01.t: ENOTDIR if a path-prefix component is not a directory. */
#include "../../pjd_common.h"
static const enum pjd_ftype types[] = { PJD_FT_REGULAR, PJD_FT_FIFO, PJD_FT_BLOCK, PJD_FT_CHAR, PJD_FT_SOCKET };
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen();
    char  sub[256], P[512];
    EXPECT(0, pjd_mkdir(n0, 0755));
    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        snprintf(sub, sizeof(sub), "%s/%s", n0, n1);
        EXPECT(0, pjd_create_file(types[i], sub));
        snprintf(P, sizeof(P), "%s/%s/test", n0, n1);
        EXPECT(ENOTDIR, pjd_mkfifo(P, 0644));
        EXPECT(0, pjd_unlink(sub));
    }
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
