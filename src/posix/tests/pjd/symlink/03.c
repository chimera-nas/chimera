// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/symlink/03.t: ENAMETOOLONG if the whole name2 path, or
 * the symlink target (name1), exceeds {PATH_MAX}. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *nx = pjd_dirgen_max();
    char  P[8192];
    snprintf(P, sizeof(P), "%sx", nx);
    pjd_mkdir_p(nx);
    EXPECT(ENAMETOOLONG, pjd_symlink("test", P));
    /* Over-long symlink target (used verbatim, so it must itself exceed
     * {PATH_MAX} regardless of the harness cwd). */
    char *target = pjd_namegen_len((size_t) CHIMERA_VFS_PATH_MAX + 16);
    char *ln     = pjd_namegen();
    EXPECT(ENAMETOOLONG, pjd_symlink(target, ln));
    return pjd_end();
} /* main */
