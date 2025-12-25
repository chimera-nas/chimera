// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

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
    int                   fd;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing openat...\n");

    // Test with AT_FDCWD and absolute path
    fd = chimera_posix_openat(AT_FDCWD, "/test/openat_test.txt", O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        fprintf(stderr, "openat with AT_FDCWD failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }
    chimera_posix_close(fd);

    // Test with AT_FDCWD and relative path
    fd = chimera_posix_openat(AT_FDCWD, "test/openat_test2.txt", O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        fprintf(stderr, "openat with relative path failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }
    chimera_posix_close(fd);

    // Cleanup
    rc = chimera_posix_unlinkat(AT_FDCWD, "/test/openat_test.txt", 0);
    if (rc != 0) {
        fprintf(stderr, "Failed to unlink openat_test.txt: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_unlinkat(AT_FDCWD, "/test/openat_test2.txt", 0);
    if (rc != 0) {
        fprintf(stderr, "Failed to unlink openat_test2.txt: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "openat tests passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
}
