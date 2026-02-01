// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include <sys/stat.h>
#include "posix_test_common.h"

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

    fprintf(stderr, "Testing mknod...\n");

    // Test 1: Create a FIFO (named pipe)
    fprintf(stderr, "  Test 1: Create FIFO\n");
    rc = chimera_posix_mknod("/test/test_fifo", S_IFIFO | 0644, 0);
    if (rc != 0) {
        if (errno == ENOTSUP || errno == EOPNOTSUPP) {
            fprintf(stderr, "mknod not supported by backend, skipping\n");
            rc = posix_test_umount();
            if (rc != 0) {
                fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
                posix_test_fail(&env);
            }
            posix_test_success(&env);
            return 0;
        }
        fprintf(stderr, "mknod FIFO failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Verify FIFO exists and has correct type
    rc = chimera_posix_stat("/test/test_fifo", &st);
    if (rc != 0) {
        fprintf(stderr, "stat FIFO failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (!S_ISFIFO(st.st_mode)) {
        fprintf(stderr, "Expected FIFO, got mode 0x%x\n", (unsigned int) st.st_mode);
        posix_test_fail(&env);
    }

    // Cleanup FIFO
    rc = chimera_posix_unlink("/test/test_fifo");
    if (rc != 0) {
        fprintf(stderr, "unlink FIFO failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Test 2: Create a socket
    fprintf(stderr, "  Test 2: Create socket\n");
    rc = chimera_posix_mknod("/test/test_sock", S_IFSOCK | 0644, 0);
    if (rc != 0) {
        fprintf(stderr, "mknod socket failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Verify socket exists and has correct type
    rc = chimera_posix_stat("/test/test_sock", &st);
    if (rc != 0) {
        fprintf(stderr, "stat socket failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (!S_ISSOCK(st.st_mode)) {
        fprintf(stderr, "Expected socket, got mode 0x%x\n", (unsigned int) st.st_mode);
        posix_test_fail(&env);
    }

    // Cleanup socket
    rc = chimera_posix_unlink("/test/test_sock");
    if (rc != 0) {
        fprintf(stderr, "unlink socket failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Test 3: Verify EEXIST when creating over existing file
    fprintf(stderr, "  Test 3: EEXIST on duplicate\n");
    rc = chimera_posix_mknod("/test/test_fifo2", S_IFIFO | 0644, 0);
    if (rc != 0) {
        fprintf(stderr, "mknod FIFO2 failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    rc = chimera_posix_mknod("/test/test_fifo2", S_IFIFO | 0644, 0);
    if (rc == 0) {
        fprintf(stderr, "mknod duplicate should have failed\n");
        posix_test_fail(&env);
    }

    if (errno != EEXIST) {
        fprintf(stderr, "Expected EEXIST, got %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Cleanup
    rc = chimera_posix_unlink("/test/test_fifo2");
    if (rc != 0) {
        fprintf(stderr, "unlink FIFO2 failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "mknod tests passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
