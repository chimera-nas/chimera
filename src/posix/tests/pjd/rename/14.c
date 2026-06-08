// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/rename/14.t: EISDIR when 'to' is a directory but 'from' is an existing non-directory. */
#include "../../pjd_common.h"
static const enum pjd_ftype types[] = { PJD_FT_REGULAR, PJD_FT_FIFO, PJD_FT_BLOCK, PJD_FT_CHAR, PJD_FT_SOCKET,
                                        PJD_FT_SYMLINK };
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen();
    EXPECT(0, pjd_mkdir(n0, 0755));
    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        EXPECT(0, pjd_create_file(types[i], n1));
        EXPECT(EISDIR, pjd_rename(n1, n0));
        EXPECT_EQ(S_IFDIR, pjd_lstat_type(n0));
        EXPECT(0, pjd_unlink(n1));
    }
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
