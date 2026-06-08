// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Ported from pjdfstest tests/truncate/13.t:
 * truncate returns EINVAL if the length argument was less than 0. */

#include "../../pjd_common.h"

int
main(
    int    argc,
    char **argv)
{
    pjd_begin(argc, argv);
    char *n0 = pjd_namegen();

    EXPECT(0, pjd_create(n0, 0644));
    EXPECT(EINVAL, pjd_truncate(n0, -1));
    EXPECT(EINVAL, pjd_truncate(n0, -999999));
    EXPECT(0, pjd_unlink(n0));
    return pjd_end();
} /* main */
