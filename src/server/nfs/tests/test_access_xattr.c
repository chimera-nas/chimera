// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

/*
 * Regression guard for the NFSv4.2 ACCESS handler: the RFC 8276 named-attribute
 * bits (XAREAD/XAWRITE/XALIST) must be advertised in `supported` on an
 * xattr-capable object and granted from the matching named-attr ACE bits.  The
 * original handler never advertised them, so a Linux client's pre-SETXATTR
 * ACCESS check came back "not supported" and setfattr failed with EACCES
 * without ever sending SETXATTR.  This exercises the pure decision logic
 * directly -- no kernel mount required, unlike the e2e test.
 */

#include <stdint.h>
#include <stdio.h>

#include "nfs4_access.h"

#define CHECK(cond)                                              \
        do {                                                     \
            if (!(cond)) {                                       \
                fprintf(stderr,                                  \
                        "test_access_xattr: FAILED at %s:%d: %s\n", \
                        __FILE__, __LINE__, # cond);             \
                return 1;                                        \
            }                                                    \
        } while (0)

#define XA_BITS (ACCESS4_XAREAD | ACCESS4_XAWRITE | ACCESS4_XALIST)

/* The xattr bits are meaningful only when the backend supports xattrs. */
static int
test_meaningful(void)
{
    uint32_t file_no  = chimera_nfs4_access_meaningful(0, 0);
    uint32_t file_yes = chimera_nfs4_access_meaningful(0, 1);
    uint32_t dir_yes  = chimera_nfs4_access_meaningful(1, 1);

    /* without xattr support, none of the xattr bits are advertised */
    CHECK((file_no & XA_BITS) == 0);
    /* a non-dir advertises EXECUTE, never LOOKUP/DELETE */
    CHECK(file_no & ACCESS4_EXECUTE);
    CHECK((file_no & (ACCESS4_LOOKUP | ACCESS4_DELETE)) == 0);

    /* with xattr support, all three xattr bits are advertised */
    CHECK((file_yes & XA_BITS) == XA_BITS);

    /* a directory advertises LOOKUP/DELETE, never EXECUTE, plus xattr bits */
    CHECK(dir_yes & ACCESS4_LOOKUP);
    CHECK(dir_yes & ACCESS4_DELETE);
    CHECK((dir_yes & ACCESS4_EXECUTE) == 0);
    CHECK((dir_yes & XA_BITS) == XA_BITS);
    return 0;
} /* test_meaningful */

/* Request bits map to the matching named-attr ACE bits. */
static int
test_to_mask(void)
{
    CHECK(chimera_nfs4_access4_to_mask(ACCESS4_XAREAD) ==
          CHIMERA_ACE_READ_NAMED_ATTRS);
    CHECK(chimera_nfs4_access4_to_mask(ACCESS4_XALIST) ==
          CHIMERA_ACE_READ_NAMED_ATTRS);
    CHECK(chimera_nfs4_access4_to_mask(ACCESS4_XAWRITE) ==
          CHIMERA_ACE_WRITE_NAMED_ATTRS);
    return 0;
} /* test_to_mask */

/* Granted ACE bits map back to allowed ACCESS4 bits, limited to requested. */
static int
test_from_granted(void)
{
    /* read-named-attrs granted -> XAREAD/XALIST allowed, XAWRITE not */
    CHECK(chimera_nfs4_access_from_granted(XA_BITS,
                                           CHIMERA_ACE_READ_NAMED_ATTRS) ==
          (ACCESS4_XAREAD | ACCESS4_XALIST));

    /* write-named-attrs granted -> only XAWRITE allowed */
    CHECK(chimera_nfs4_access_from_granted(XA_BITS,
                                           CHIMERA_ACE_WRITE_NAMED_ATTRS) ==
          ACCESS4_XAWRITE);

    /* both granted -> all three allowed */
    CHECK(chimera_nfs4_access_from_granted(XA_BITS,
                                           CHIMERA_ACE_READ_NAMED_ATTRS |
                                           CHIMERA_ACE_WRITE_NAMED_ATTRS) ==
          XA_BITS);

    /* a bit that was not requested is never returned, even if granted */
    CHECK((chimera_nfs4_access_from_granted(ACCESS4_XAREAD,
                                            CHIMERA_ACE_READ_NAMED_ATTRS |
                                            CHIMERA_ACE_WRITE_NAMED_ATTRS) &
           ACCESS4_XAWRITE) == 0);
    return 0;
} /* test_from_granted */

int
main(void)
{
    if (test_meaningful() || test_to_mask() || test_from_granted()) {
        return 1;
    }
    printf("test_access_xattr: all checks passed\n");
    return 0;
} /* main */
