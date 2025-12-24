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
    struct stat           st;
    const char           *test_data = "Hello, World!";
    ssize_t               written;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fd = chimera_posix_open("/test/testfile", O_CREAT | O_RDWR, 0644);

    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    written = chimera_posix_write(fd, test_data, strlen(test_data));

    if (written != (ssize_t) strlen(test_data)) {
        fprintf(stderr, "Failed to write test data: %s\n", strerror(errno));
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    rc = chimera_posix_stat("/test/testfile", &st);

    if (rc != 0) {
        fprintf(stderr, "Failed to stat file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Stat successful:\n");
    fprintf(stderr, "  st_dev: %lu\n", (unsigned long) st.st_dev);
    fprintf(stderr, "  st_ino: %lu\n", (unsigned long) st.st_ino);
    fprintf(stderr, "  st_mode: %lo\n", (unsigned long) st.st_mode);
    fprintf(stderr, "  st_nlink: %lu\n", (unsigned long) st.st_nlink);
    fprintf(stderr, "  st_uid: %lu\n", (unsigned long) st.st_uid);
    fprintf(stderr, "  st_gid: %lu\n", (unsigned long) st.st_gid);
    fprintf(stderr, "  st_size: %lu\n", (unsigned long) st.st_size);

    if (st.st_size != (off_t) strlen(test_data)) {
        fprintf(stderr, "Wrong file size: expected %zu, got %lu\n",
                strlen(test_data), (unsigned long) st.st_size);
        posix_test_fail(&env);
    }

    if (!S_ISREG(st.st_mode)) {
        fprintf(stderr, "File is not a regular file\n");
        posix_test_fail(&env);
    }

    fprintf(stderr, "Stat verification passed\n");

    chimera_posix_close(fd);

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
