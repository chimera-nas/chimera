// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * NLM byte-range length conventions (nfs_nlm_range.h).
 *
 * Three spellings of "to end of file" meet in the NLM server: wire l_len 0
 * (with 0xffffffffffffffff accepted as an alias on input), the stored POSIX
 * length 0, and the VFS claim length UINT64_MAX.  These tests pin each
 * boundary crossing, and in particular that a to-EOF range is put back on
 * the wire as 0 whichever spelling the client used: Linux lockd decodes an
 * incoming range as end = off + len - 1 and only treats len == 0 as to-EOF,
 * so 0xffffffffffffffff from a nonzero offset describes the wrong range.
 */

#include <stdint.h>
#include <stdio.h>

#include "nfs_nlm_range.h"

/*
 * Use an explicit check rather than assert(): the static-analysis build compiles
 * with -DNDEBUG, which turns assert() into a no-op.  CHECK always evaluates.
 */
#define CHECK(cond)                                              \
        do {                                                     \
            if (!(cond)) {                                       \
                fprintf(stderr,                                  \
                        "test_nlm_range: FAILED at %s:%d: %s\n",  \
                        __FILE__, __LINE__, # cond);             \
                return 1;                                        \
            }                                                    \
        } while (0)

/* Input: both wire spellings of to-EOF store as POSIX 0; finite lengths,
 * including the one just below the sentinel, pass through. */
static int
test_wire_to_posix(void)
{
    CHECK(nlm_wire_len_to_posix(0) == 0);
    CHECK(nlm_wire_len_to_posix(UINT64_MAX) == 0);
    CHECK(nlm_wire_len_to_posix(4096) == 4096);
    CHECK(nlm_wire_len_to_posix(UINT64_MAX - 1) == UINT64_MAX - 1);
    return 0;
} /* test_wire_to_posix */

/* Stored -> claim core: POSIX 0 becomes the VFS to-EOF sentinel. */
static int
test_posix_to_vfs(void)
{
    CHECK(nlm_posix_len_to_vfs(0) == UINT64_MAX);
    CHECK(nlm_posix_len_to_vfs(4096) == 4096);
    CHECK(nlm_posix_len_to_vfs(UINT64_MAX - 1) == UINT64_MAX - 1);
    CHECK(nlm_vfs_len_to_posix(UINT64_MAX) == 0);
    CHECK(nlm_vfs_len_to_posix(4096) == 4096);
    CHECK(nlm_vfs_len_to_posix(nlm_posix_len_to_vfs(0)) == 0);
    CHECK(nlm_vfs_len_to_posix(nlm_posix_len_to_vfs(4096)) == 4096);
    return 0;
} /* test_posix_to_vfs */

/* Output: a to-EOF range is 0 on the wire from either internal form. */
static int
test_to_wire(void)
{
    CHECK(nlm_posix_len_to_wire(0) == 0);
    CHECK(nlm_posix_len_to_wire(4096) == 4096);
    CHECK(nlm_vfs_len_to_wire(UINT64_MAX) == 0);
    CHECK(nlm_vfs_len_to_wire(4096) == 4096);
    CHECK(nlm_vfs_len_to_wire(UINT64_MAX - 1) == UINT64_MAX - 1);
    return 0;
} /* test_to_wire */

/* The regression: however the client spelled to-EOF, both server-originated
 * paths (GRANTED from the stored entry, TEST from the VFS conflict) must
 * report l_len 0. */
static int
test_to_eof_round_trips(void)
{
    const uint64_t spellings[] = { 0, UINT64_MAX };
    unsigned       i;

    for (i = 0; i < sizeof(spellings) / sizeof(spellings[0]); i++) {
        uint64_t posix_len = nlm_wire_len_to_posix(spellings[i]);

        CHECK(nlm_posix_len_to_wire(posix_len) == 0);
        CHECK(nlm_vfs_len_to_wire(nlm_posix_len_to_vfs(posix_len)) == 0);
    }
    return 0;
} /* test_to_eof_round_trips */

int
main(void)
{
    if (test_wire_to_posix() ||
        test_posix_to_vfs() ||
        test_to_wire() ||
        test_to_eof_round_trips()) {
        return 1;
    }
    printf("test_nlm_range: all tests passed\n");
    return 0;
} /* main */
