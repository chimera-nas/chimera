// SPDX-FileCopyrightText: 2025-2026 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <fcntl.h>
#include "posix_test_common.h"

#ifndef AT_FDCWD
#define AT_FDCWD -100
#endif /* ifndef AT_FDCWD */

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

    fprintf(stderr, "Testing faccessat...\n");

    // Create a file and chmod to 0644 (no execute bits)
    fd = chimera_posix_openat(AT_FDCWD, "/test/access_test.txt", O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_fchmod(fd, 0644);
    if (rc != 0) {
        fprintf(stderr, "Failed to chmod test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    // Check access (F_OK = file exists)
    rc = chimera_posix_faccessat(AT_FDCWD, "/test/access_test.txt", F_OK, 0);
    if (rc != 0) {
        fprintf(stderr, "faccessat F_OK failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Check non-existent file
    rc = chimera_posix_faccessat(AT_FDCWD, "/test/nonexistent.txt", F_OK, 0);
    if (rc == 0) {
        fprintf(stderr, "faccessat should have failed for non-existent file\n");
        posix_test_fail(&env);
    }

    // Check R_OK on readable file (mode 0644, owner has read)
    rc = chimera_posix_faccessat(AT_FDCWD, "/test/access_test.txt", R_OK, 0);
    if (rc != 0) {
        fprintf(stderr, "faccessat R_OK failed on readable file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Check X_OK on non-executable file (mode 0644, no execute bits)
    rc = chimera_posix_faccessat(AT_FDCWD, "/test/access_test.txt", X_OK, 0);
    if (rc == 0) {
        fprintf(stderr, "faccessat X_OK should have failed for non-executable file\n");
        posix_test_fail(&env);
    }

    // Create an executable file and chmod to 0755, then verify X_OK succeeds
    fd = chimera_posix_openat(AT_FDCWD, "/test/access_exec.txt", O_CREAT | O_RDWR, 0755);
    if (fd < 0) {
        fprintf(stderr, "Failed to create executable test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_fchmod(fd, 0755);
    if (rc != 0) {
        fprintf(stderr, "Failed to chmod executable test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    rc = chimera_posix_faccessat(AT_FDCWD, "/test/access_exec.txt", X_OK, 0);
    if (rc != 0) {
        fprintf(stderr, "faccessat X_OK failed on executable file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    chimera_posix_unlinkat(AT_FDCWD, "/test/access_exec.txt", 0);

    // Cleanup
    chimera_posix_unlinkat(AT_FDCWD, "/test/access_test.txt", 0);

    fprintf(stderr, "faccessat tests passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
