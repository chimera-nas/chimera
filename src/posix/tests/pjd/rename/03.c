// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/rename/03.t: ENOENT if a component of the 'from' path does not exist. */
#include "../../pjd_common.h"
int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen(), *n1 = pjd_namegen(), *n2 = pjd_namegen();
    char  from[256], to[256];
    EXPECT(0, pjd_mkdir(n0, 0755));
    pjd_cd(n0);
    snprintf(from, sizeof(from), "%s/test", n1);   /* n1 doesn't exist */
    EXPECT(ENOENT, pjd_rename(from, n2));
    EXPECT(ENOENT, pjd_rename(n1, n2));            /* plain missing source */
    (void) to;
    pjd_cd("..");
    EXPECT(0, pjd_rmdir(n0));
    return pjd_end();
} /* main */
