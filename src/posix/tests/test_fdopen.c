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
    CHIMERA_FILE         *fp;
    char                  buf[64];
    size_t                nread;
    const char           *test_data = "Hello, World!";
    size_t                data_len;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing fdopen...\n");

    data_len = strlen(test_data);

    // Create test file
    fd = chimera_posix_open("/test/fdopen_test", O_CREAT | O_RDWR | O_TRUNC, 0644);

    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Write some data
    if (chimera_posix_write(fd, test_data, data_len) != (ssize_t) data_len) {
        fprintf(stderr, "Failed to write test data: %s\n", strerror(errno));
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    // Seek to beginning
    if (chimera_posix_lseek(fd, 0, SEEK_SET) != 0) {
        fprintf(stderr, "lseek failed: %s\n", strerror(errno));
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    // Test fdopen
    fp = chimera_posix_fdopen(fd, "r+");

    if (!fp) {
        fprintf(stderr, "fdopen failed: %s\n", strerror(errno));
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    fprintf(stderr, "fdopen succeeded\n");

    // Verify fileno returns the original fd
    if (chimera_posix_fileno(fp) != fd) {
        fprintf(stderr, "fileno returned wrong fd: expected %d, got %d\n",
                fd, chimera_posix_fileno(fp));
        chimera_posix_fclose(fp);
        posix_test_fail(&env);
    }

    fprintf(stderr, "fileno returns correct fd\n");

    // Read using the FILE stream
    nread = chimera_posix_fread(buf, 1, data_len, fp);

    if (nread != data_len) {
        fprintf(stderr, "fread via fdopen'd stream failed: read %zu, expected %zu\n",
                nread, data_len);
        chimera_posix_fclose(fp);
        posix_test_fail(&env);
    }

    buf[nread] = '\0';

    if (strcmp(buf, test_data) != 0) {
        fprintf(stderr, "Data mismatch: expected '%s', got '%s'\n", test_data, buf);
        chimera_posix_fclose(fp);
        posix_test_fail(&env);
    }

    fprintf(stderr, "Read via fdopen'd stream succeeded\n");

    // Test fdopen with invalid fd
    fp = chimera_posix_fdopen(-1, "r");

    if (fp != NULL) {
        fprintf(stderr, "fdopen with invalid fd should have failed\n");
        chimera_posix_fclose(fp);
        posix_test_fail(&env);
    }

    if (errno != EBADF) {
        fprintf(stderr, "fdopen with invalid fd should set errno to EBADF, got %d\n", errno);
        posix_test_fail(&env);
    }

    fprintf(stderr, "fdopen with invalid fd correctly failed\n");

    // Close the stream (this also closes the fd)
    rc = chimera_posix_fclose(chimera_posix_fdopen(fd, "r"));

    fprintf(stderr, "fdopen test passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
