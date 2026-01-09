// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "posix_test_common.h"

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   fd;
    int                   rc;
    off_t                 pos;
    int64_t               pos64;
    char                  buf[64];
    ssize_t               nread;
    const char           *test_data = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
    size_t                data_len;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    data_len = strlen(test_data);

    // Create test file with known content
    fd = chimera_posix_open("/test/lseek_test", O_CREAT | O_RDWR | O_TRUNC, 0644);

    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (chimera_posix_write(fd, test_data, data_len) != (ssize_t) data_len) {
        fprintf(stderr, "Failed to write test data: %s\n", strerror(errno));
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    // Test SEEK_SET - seek to absolute position
    fprintf(stderr, "Testing SEEK_SET...\n");
    pos = chimera_posix_lseek(fd, 5, SEEK_SET);

    if (pos != 5) {
        fprintf(stderr, "SEEK_SET failed: expected 5, got %ld\n", (long) pos);
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    // Verify by reading
    nread = chimera_posix_read(fd, buf, 1);

    if (nread != 1 || buf[0] != 'F') {
        fprintf(stderr, "Read after SEEK_SET failed: expected 'F', got '%c'\n", buf[0]);
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    fprintf(stderr, "SEEK_SET passed\n");

    // Test SEEK_CUR - seek relative to current position
    fprintf(stderr, "Testing SEEK_CUR...\n");

    // Current position should be 6 (after reading 1 byte from position 5)
    pos = chimera_posix_lseek(fd, 3, SEEK_CUR);

    if (pos != 9) {
        fprintf(stderr, "SEEK_CUR failed: expected 9, got %ld\n", (long) pos);
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    // Verify by reading
    nread = chimera_posix_read(fd, buf, 1);

    if (nread != 1 || buf[0] != 'J') {
        fprintf(stderr, "Read after SEEK_CUR failed: expected 'J', got '%c'\n", buf[0]);
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    fprintf(stderr, "SEEK_CUR passed\n");

    // Test SEEK_CUR with negative offset
    fprintf(stderr, "Testing SEEK_CUR with negative offset...\n");
    pos = chimera_posix_lseek(fd, -5, SEEK_CUR);

    if (pos != 5) {
        fprintf(stderr, "SEEK_CUR negative failed: expected 5, got %ld\n", (long) pos);
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    nread = chimera_posix_read(fd, buf, 1);

    if (nread != 1 || buf[0] != 'F') {
        fprintf(stderr, "Read after SEEK_CUR negative failed: expected 'F', got '%c'\n", buf[0]);
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    fprintf(stderr, "SEEK_CUR negative passed\n");

    // Test SEEK_END - seek relative to end of file
    fprintf(stderr, "Testing SEEK_END...\n");
    pos = chimera_posix_lseek(fd, -5, SEEK_END);

    if (pos != (off_t) (data_len - 5)) {
        fprintf(stderr, "SEEK_END failed: expected %ld, got %ld\n",
                (long) (data_len - 5), (long) pos);
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    // Verify by reading
    nread  = chimera_posix_read(fd, buf, 5);
    buf[5] = '\0';

    if (nread != 5 || strcmp(buf, "VWXYZ") != 0) {
        fprintf(stderr, "Read after SEEK_END failed: expected 'VWXYZ', got '%s'\n", buf);
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    fprintf(stderr, "SEEK_END passed\n");

    // Test SEEK_SET to beginning
    fprintf(stderr, "Testing SEEK_SET to beginning...\n");
    pos = chimera_posix_lseek(fd, 0, SEEK_SET);

    if (pos != 0) {
        fprintf(stderr, "SEEK_SET to 0 failed: expected 0, got %ld\n", (long) pos);
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    nread = chimera_posix_read(fd, buf, 1);

    if (nread != 1 || buf[0] != 'A') {
        fprintf(stderr, "Read after SEEK_SET to 0 failed: expected 'A', got '%c'\n", buf[0]);
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    fprintf(stderr, "SEEK_SET to beginning passed\n");

    // Test lseek64
    fprintf(stderr, "Testing lseek64...\n");
    pos64 = chimera_posix_lseek64(fd, 10, SEEK_SET);

    if (pos64 != 10) {
        fprintf(stderr, "lseek64 failed: expected 10, got %lld\n", (long long) pos64);
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    nread = chimera_posix_read(fd, buf, 1);

    if (nread != 1 || buf[0] != 'K') {
        fprintf(stderr, "Read after lseek64 failed: expected 'K', got '%c'\n", buf[0]);
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    fprintf(stderr, "lseek64 passed\n");

    // Test invalid whence
    fprintf(stderr, "Testing invalid whence...\n");
    pos = chimera_posix_lseek(fd, 0, 999);

    if (pos != -1 || errno != EINVAL) {
        fprintf(stderr, "Invalid whence should have failed with EINVAL\n");
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    fprintf(stderr, "Invalid whence test passed\n");

    // Test seeking before beginning of file
    fprintf(stderr, "Testing seek before beginning...\n");
    pos = chimera_posix_lseek(fd, -100, SEEK_SET);

    if (pos != -1 || errno != EINVAL) {
        fprintf(stderr, "Seek before beginning should have failed with EINVAL\n");
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    fprintf(stderr, "Seek before beginning test passed\n");

    fprintf(stderr, "All lseek tests passed!\n");

    chimera_posix_close(fd);

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
