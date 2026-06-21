// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <stdint.h>
#include <stdio.h>
#include <string.h>

#include "vfs/vfs_xattr_name.h"

/*
 * Use an explicit check rather than assert(): the static-analysis build compiles
 * with -DNDEBUG, which turns assert() into a no-op (and leaves any check-only
 * locals flagged as unused under -Werror).  CHECK always evaluates, so the test
 * is meaningful in every build mode.
 */
#define CHECK(cond)                                              \
        do {                                                     \
            if (!(cond)) {                                       \
                fprintf(stderr,                                  \
                        "test_xattr_name: FAILED at %s:%d: %s\n", \
                        __FILE__, __LINE__, # cond);             \
                return 1;                                        \
            }                                                    \
        } while (0)

/* chimera_vfs_xattr_is_user: only non-empty user.* names qualify. */
static int
test_is_user(void)
{
    CHECK(chimera_vfs_xattr_is_user("user.test", 9) == 1);
    CHECK(chimera_vfs_xattr_is_user("user.x", 6) == 1);

    /* bare key, no prefix */
    CHECK(chimera_vfs_xattr_is_user("test", 4) == 0);

    /* other namespaces are not user.* */
    CHECK(chimera_vfs_xattr_is_user("system.posix_acl_access", 23) == 0);
    CHECK(chimera_vfs_xattr_is_user("trusted.foo", 11) == 0);
    CHECK(chimera_vfs_xattr_is_user("security.selinux", 16) == 0);

    /* exact "user." with an empty key is not a valid user attribute */
    CHECK(chimera_vfs_xattr_is_user("user.", 5) == 0);
    return 0;
} /* test_is_user */

/* chimera_vfs_xattr_build_user: prepend "user." with length validation. */
static int
test_build_user(void)
{
    char buf[CHIMERA_VFS_XATTR_NAME_MAX + 1];
    char longkey[CHIMERA_VFS_XATTR_NAME_MAX + 1];

    /* normal key -> "user.test" */
    CHECK(chimera_vfs_xattr_build_user(buf, sizeof(buf), "test", 4) == 9);
    CHECK(memcmp(buf, "user.test", 9) == 0);

    /* empty key is rejected */
    CHECK(chimera_vfs_xattr_build_user(buf, sizeof(buf), "", 0) == -1);

    /* prefix + key may reach exactly XATTR_NAME_MAX (250 byte key) */
    memset(longkey, 'a', sizeof(longkey));
    CHECK(chimera_vfs_xattr_build_user(buf, sizeof(buf), longkey,
                                       CHIMERA_VFS_XATTR_NAME_MAX -
                                       CHIMERA_VFS_XATTR_USER_PREFIX_LEN) ==
          CHIMERA_VFS_XATTR_NAME_MAX);

    /* one byte past the limit is rejected */
    CHECK(chimera_vfs_xattr_build_user(buf, sizeof(buf), longkey,
                                       CHIMERA_VFS_XATTR_NAME_MAX -
                                       CHIMERA_VFS_XATTR_USER_PREFIX_LEN + 1) ==
          -1);

    /* a destination too small for the result is rejected */
    CHECK(chimera_vfs_xattr_build_user(buf, 5, "test", 4) == -1);
    return 0;
} /* test_build_user */

/*
 * Mirror the listxattrs strip/filter: walk a packed buffer of NUL-terminated
 * fully-qualified names, emit only user.* names with the prefix stripped.
 */
static int
test_list_strip_filter(void)
{
    /* "user.test\0system.foo\0user.other\0trusted.x\0" */
    static const char packed[] =
        "user.test\0"
        "system.foo\0"
        "user.other\0"
        "trusted.x";
    const char       *p     = packed;
    const char       *end   = packed + sizeof(packed) - 1;
    int               count = 0;

    while (p < end) {
        uint32_t namelen = strlen(p);

        if (chimera_vfs_xattr_is_user(p, namelen)) {
            const char *stripped = p + CHIMERA_VFS_XATTR_USER_PREFIX_LEN;
            uint32_t    striplen = namelen - CHIMERA_VFS_XATTR_USER_PREFIX_LEN;

            if (count == 0) {
                CHECK(striplen == 4 && memcmp(stripped, "test", 4) == 0);
            } else if (count == 1) {
                CHECK(striplen == 5 && memcmp(stripped, "other", 5) == 0);
            }
            count++;
        }
        p += namelen + 1;
    }

    /* only the two user.* names survive */
    CHECK(count == 2);
    return 0;
} /* test_list_strip_filter */

int
main(void)
{
    if (test_is_user() || test_build_user() || test_list_strip_filter()) {
        return 1;
    }
    printf("test_xattr_name: all checks passed\n");
    return 0;
} /* main */
