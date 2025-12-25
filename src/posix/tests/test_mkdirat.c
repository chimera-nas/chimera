// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <fcntl.h>
#include "posix_test_common.h"

#ifndef AT_FDCWD
#define AT_FDCWD -100
#endif

#ifndef AT_REMOVEDIR
#define AT_REMOVEDIR 0x200
#endif

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;
    struct stat           st;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing mkdirat...\n");

    // Test with AT_FDCWD
    rc = chimera_posix_mkdirat(AT_FDCWD, "/test/mkdirat_test", 0755);
    if (rc != 0) {
        fprintf(stderr, "mkdirat failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Verify it exists
    rc = chimera_posix_fstatat(AT_FDCWD, "/test/mkdirat_test", &st, 0);
    if (rc != 0) {
        fprintf(stderr, "fstatat failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (!S_ISDIR(st.st_mode)) {
        fprintf(stderr, "Expected directory\n");
        posix_test_fail(&env);
    }

    // Cleanup with unlinkat + AT_REMOVEDIR
    rc = chimera_posix_unlinkat(AT_FDCWD, "/test/mkdirat_test", AT_REMOVEDIR);
    if (rc != 0) {
        fprintf(stderr, "Failed to remove directory: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "mkdirat tests passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
}
