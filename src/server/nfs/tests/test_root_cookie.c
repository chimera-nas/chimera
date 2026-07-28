// SPDX-FileCopyrightText: 2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Cookie policy for the NFSv4 pseudo-fs root READDIR (nfs4_root_cookie.h).
 *
 * args->cookie is a 64-bit nfs_cookie4 while the export walk's resume position
 * is an int, so a garbage cookie such as 0x80000002 once truncated to a
 * negative first_pos, never tripped the resume test, and restarted the listing
 * at position 0 -- the client got duplicate entries instead of
 * NFS4ERR_BAD_COOKIE.
 *
 * The reserved and garbage cookies are unreachable from chimera's own NFS
 * client: it clamps cookies below 3 to 0, synthesizes "." and ".." locally,
 * and never sends a cookie it did not receive from the server.  An
 * out-of-range cookie is reachable -- a client mid-walk when an export is
 * removed holds a cookie into a list that has since shrunk -- but not
 * deterministically.  Exercise the decision logic directly instead.
 */

#include <stdint.h>
#include <stdio.h>

#include "nfs4_root_cookie.h"

#define CHECK(cond)                                                 \
        do {                                                        \
            if (!(cond)) {                                          \
                fprintf(stderr,                                     \
                        "test_root_cookie: FAILED at %s:%d: %s\n",  \
                        __FILE__, __LINE__, # cond);                \
                return 1;                                           \
            }                                                       \
        } while (0)

/* Cookie 0 means "start of directory" whatever the export count. */
static int
test_start_of_directory(void)
{
    int first_pos = -1;

    CHECK(nfs4_root_readdir_cookie_first_pos(0, 4, &first_pos) == NFS4_OK);
    CHECK(first_pos == 0);

    first_pos = -1;
    CHECK(nfs4_root_readdir_cookie_first_pos(0, 0, &first_pos) == NFS4_OK);
    CHECK(first_pos == 0);

    return 0;
} /* test_start_of_directory */

/* 1 and 2 are reserved; the pseudo-root never emits them. */
static int
test_reserved_cookies_rejected(void)
{
    int first_pos = 7;

    CHECK(nfs4_root_readdir_cookie_first_pos(1, 4, &first_pos) ==
          NFS4ERR_BAD_COOKIE);
    CHECK(nfs4_root_readdir_cookie_first_pos(2, 4, &first_pos) ==
          NFS4ERR_BAD_COOKIE);

    /* first_pos is untouched on rejection. */
    CHECK(first_pos == 7);

    return 0;
} /* test_reserved_cookies_rejected */

/* Entry cookies are pos + 3, so cookie C resumes at C - 2. */
static int
test_resume_positions(void)
{
    int first_pos = -1;

    /* First entry's cookie: resume just past position 0. */
    CHECK(nfs4_root_readdir_cookie_first_pos(3, 4, &first_pos) == NFS4_OK);
    CHECK(first_pos == 1);

    /* Last entry's cookie with 4 exports: resume at end-of-directory. */
    CHECK(nfs4_root_readdir_cookie_first_pos(6, 4, &first_pos) == NFS4_OK);
    CHECK(first_pos == 4);

    return 0;
} /* test_resume_positions */

/* The encode side (nfs4_root_readdir_pos_cookie, used by nfs4_root_readdir to
 * stamp entry->cookie) and the decode side must stay inverses: resuming with
 * the cookie of position p restarts at p + 1, and every emitted cookie is
 * accepted.  They are separate functions, so pin the pairing here. */
static int
test_encode_decode_round_trip(void)
{
    const int num_exports = 8;
    int       pos, first_pos;

    for (pos = 0; pos < num_exports; pos++) {
        uint64_t cookie = nfs4_root_readdir_pos_cookie(pos);

        /* Never collides with the reserved values. */
        CHECK(cookie > 2);

        first_pos = -1;
        CHECK(nfs4_root_readdir_cookie_first_pos(cookie, num_exports,
                                                 &first_pos) == NFS4_OK);
        CHECK(first_pos == pos + 1);
    }

    /* One past the last emitted cookie is out of range. */
    first_pos = -1;
    CHECK(nfs4_root_readdir_cookie_first_pos(
              nfs4_root_readdir_pos_cookie(num_exports - 1) + 1,
              num_exports, &first_pos) == NFS4ERR_BAD_COOKIE);

    return 0;
} /* test_encode_decode_round_trip */

/* Anything above the last entry's cookie is garbage or stale. */
static int
test_out_of_range_rejected(void)
{
    int first_pos = -1;

    /* One past the last valid cookie for 4 exports. */
    CHECK(nfs4_root_readdir_cookie_first_pos(7, 4, &first_pos) ==
          NFS4ERR_BAD_COOKIE);

    /* Truncates to a negative int -- the original defect. */
    CHECK(nfs4_root_readdir_cookie_first_pos(0x80000002ULL, 4, &first_pos) ==
          NFS4ERR_BAD_COOKIE);

    CHECK(nfs4_root_readdir_cookie_first_pos(UINT64_MAX, 4, &first_pos) ==
          NFS4ERR_BAD_COOKIE);

    /* An empty export list accepts only the start-of-directory cookie. */
    CHECK(nfs4_root_readdir_cookie_first_pos(3, 0, &first_pos) ==
          NFS4ERR_BAD_COOKIE);

    return 0;
} /* test_out_of_range_rejected */

int
main(void)
{
    if (test_start_of_directory() ||
        test_reserved_cookies_rejected() ||
        test_resume_positions() ||
        test_encode_decode_round_trip() ||
        test_out_of_range_rejected()) {
        return 1;
    }

    printf("test_root_cookie: all cases passed\n");

    return 0;
} /* main */
