// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/open/22.t: EEXIST with O_CREAT|O_EXCL on an existing file (any type). */
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
    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        EXPECT(0, pjd_create_file(types[i], n0));
        EXPECT_EQ(EEXIST, pjd_open_e(n0, O_CREAT | O_EXCL, 0644));
        EXPECT(0, pjd_remove_file(types[i], n0));
    }
    return pjd_end();
} /* main */
