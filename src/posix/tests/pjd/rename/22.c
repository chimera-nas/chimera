// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/rename/22.t: rename updates the file's ctime. */
#include "../../pjd_common.h"
static const enum pjd_ftype types[] = { PJD_FT_REGULAR, PJD_FT_DIR, PJD_FT_FIFO, PJD_FT_BLOCK, PJD_FT_CHAR,
                                        PJD_FT_SOCKET };
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen();
    for (unsigned i = 0; i < sizeof(types) / sizeof(types[0]); i++) {
        struct timespec c1, c2;
        EXPECT(0, pjd_create_file(types[i], n0));
        pjd_lstat_ctime(n0, &c1);
        pjd_settle();
        EXPECT(0, pjd_rename(n0, n1));
        pjd_lstat_ctime(n1, &c2);
        PJD_CHECK(pjd_timespec_lt(&c1, &c2), "ctime advanced after rename (type %d)", types[i]);
        EXPECT(0, pjd_remove_file(types[i], n1));
    }
    return pjd_end();
} /* main */
