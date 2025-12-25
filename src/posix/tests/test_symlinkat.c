// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Tests symlinkat and readlinkat together

#include <fcntl.h>
#include "posix_test_common.h"

#ifndef AT_FDCWD
#define AT_FDCWD -100
#endif

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;
    char                  buf[256];
    ssize_t               len;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing symlinkat/readlinkat...\n");

    // Create a symlink
    rc = chimera_posix_symlinkat("/target/path", AT_FDCWD, "/test/symlink_test");
    if (rc != 0) {
        fprintf(stderr, "symlinkat failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Read it back
    len = chimera_posix_readlinkat(AT_FDCWD, "/test/symlink_test", buf, sizeof(buf));
    if (len < 0) {
        fprintf(stderr, "readlinkat failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    buf[len] = '\0';
    if (strcmp(buf, "/target/path") != 0) {
        fprintf(stderr, "Symlink content mismatch: got '%s'\n", buf);
        posix_test_fail(&env);
    }

    // Cleanup
    chimera_posix_unlinkat(AT_FDCWD, "/test/symlink_test", 0);

    fprintf(stderr, "symlinkat/readlinkat tests passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
}
