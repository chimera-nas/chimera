// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/mkfifo/09.t: EEXIST if the named file exists (any type). */
#include "../../pjd_common.h"
static const enum pjd_ftype types[] = { PJD_FT_REGULAR, PJD_FT_DIR, PJD_FT_FIFO, PJD_FT_BLOCK, PJD_FT_CHAR,
                                        PJD_FT_SOCKET,  PJD_FT_SYMLINK };
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen();
    char  P[256];
    snprintf(P, sizeof(P), "%s", n0);
    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        EXPECT(0, pjd_create_file(types[i], n0));
        EXPECT(EEXIST, pjd_mkfifo(P, 0644));
        EXPECT(0, pjd_remove_file(types[i], n0));
    }
    return pjd_end();
} /* main */
