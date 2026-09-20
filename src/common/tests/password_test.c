// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
// SPDX-License-Identifier: LGPL-2.1-only
#undef NDEBUG
#include <assert.h>
#include <string.h>

char * chimera_crypt_sha512(
    const char *,
    const char *,
    char *);

int
main(void)
{
    /* Shared with the REST authentication integration test. */
    const char *expected =
        "$6$testsalt$eBXKG..hXMuMyU2qJeRwFHrphEZTnovHazyD.YLjz/QKAbAvZj7z8MGdfCgwsM3n3k6pWpuGnuW/58UHKaWzL0";
    char        output[128];

    assert(chimera_crypt_sha512("adminpass", expected, output) == output);
    assert(!strcmp(output, expected));
    assert(chimera_crypt_sha512("incorrect", expected, output) == output);
    assert(strcmp(output, expected));
    assert(chimera_crypt_sha512("adminpass", "$6$rounds=oops$salt$", output) != output);
    assert(chimera_crypt_sha512("adminpass", "$1$salt$hash", output) != output);
    return 0;
} /* main */
