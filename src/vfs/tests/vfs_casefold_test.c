// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/* Case-insensitive name matching (vfs/sdk/vfs_casefold.h). */

#include <stdio.h>
#include <string.h>

#include "vfs/sdk/vfs_casefold.h"

static int failures;

static void
check(
    const char *a,
    const char *b,
    int         want)
{
    int eq   = chimera_vfs_name_equal_ci(a, (int) strlen(a), b, (int) strlen(b));
    int hash = chimera_vfs_casefold_hash(a, (int) strlen(a)) ==
        chimera_vfs_casefold_hash(b, (int) strlen(b));

    if (eq != want || (want && !hash)) {
        fprintf(stderr, "FAIL: \"%s\" vs \"%s\": equal %d (want %d), hash match %d\n",
                a, b, eq, want, hash);
        failures++;
    }
} /* check */

int
main(void)
{
    check("MixedCase.txt", "mixedcase.TXT", 1);
    check("abc", "ABC", 1);
    check("abc", "abd", 0);
    check("abc", "abcd", 0);
    check("", "", 1);
    /* Latin-1 and beyond: Windows folds these too. */
    check("caf\xc3\xa9", "CAF\xc3\x89", 1);             /* é / É */
    check("\xc3\xbc" "ber", "\xc3\x9c" "BER", 1);       /* ü / Ü */
    check("\xd0\xbf\xd1\x80\xd0\xb8", "\xd0\x9f\xd0\xa0\xd0\x98", 1); /* при / ПРИ */
    check("\xcf\x83", "\xce\xa3", 1);                   /* σ / Σ */
    check("\xcf\x82", "\xce\xa3", 1);                   /* final ς / Σ */
    check("\xc3\xa9", "e", 0);                          /* é is not e */
    /* Different byte lengths that fold equal: ɐ (2 bytes) / Ɐ (3 bytes). */
    check("\xc9\x90", "\xe2\xb1\xaf", 1);
    /* Undecodable bytes compare as they are. */
    check("a\xff", "A\xff", 1);
    check("a\xff", "a\xfe", 0);
    check("\xc3", "\xc3", 1);
    check("\xc3", "\xc4", 0);

    if (failures) {
        return 1;
    }
    printf("casefold: all checks passed\n");
    return 0;
} /* main */
