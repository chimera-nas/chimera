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
    struct stat           file_st;
    struct stat           link_st;
    const char           *test_data = "Hello, World!";

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing lstat...\n");

    // Create test file
    fd = chimera_posix_open("/test/lstat_file", O_CREAT | O_RDWR, 0644);

    if (fd < 0) {
        fprintf(stderr, "Failed to create test file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Write some data
    if (chimera_posix_write(fd, test_data, strlen(test_data)) != (ssize_t) strlen(test_data)) {
        fprintf(stderr, "Failed to write test data: %s\n", strerror(errno));
        chimera_posix_close(fd);
        posix_test_fail(&env);
    }

    chimera_posix_close(fd);

    // Get file stats
    rc = chimera_posix_stat("/test/lstat_file", &file_st);

    if (rc != 0) {
        fprintf(stderr, "Failed to stat file: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "File size: %lu, mode: %lo\n",
            (unsigned long) file_st.st_size,
            (unsigned long) file_st.st_mode);

    // Create symlink
    rc = chimera_posix_symlink("/test/lstat_file", "/test/lstat_link");

    if (rc != 0) {
        fprintf(stderr, "Failed to create symlink: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    fprintf(stderr, "Created symlink\n");

    // lstat on symlink should return symlink's stats
    rc = chimera_posix_lstat("/test/lstat_link", &link_st);

    if (rc != 0) {
        fprintf(stderr, "lstat on symlink failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Verify it's a symlink
    if (!S_ISLNK(link_st.st_mode)) {
        fprintf(stderr, "lstat did not return symlink mode: got %lo\n",
                (unsigned long) link_st.st_mode);
        posix_test_fail(&env);
    }

    fprintf(stderr, "lstat correctly identified symlink (mode: %lo)\n",
            (unsigned long) link_st.st_mode);

    // Symlink's inode should be different from file's inode
    if (link_st.st_ino == file_st.st_ino) {
        fprintf(stderr, "Warning: lstat returned same inode as file "
                "(may indicate symlink following)\n");
    } else {
        fprintf(stderr, "lstat returned different inode (file: %lu, link: %lu)\n",
                (unsigned long) file_st.st_ino,
                (unsigned long) link_st.st_ino);
    }

    // lstat on regular file should work too
    rc = chimera_posix_lstat("/test/lstat_file", &file_st);

    if (rc != 0) {
        fprintf(stderr, "lstat on regular file failed: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    if (!S_ISREG(file_st.st_mode)) {
        fprintf(stderr, "lstat on regular file returned wrong mode: %lo\n",
                (unsigned long) file_st.st_mode);
        posix_test_fail(&env);
    }

    fprintf(stderr, "lstat on regular file passed\n");

    fprintf(stderr, "lstat test passed\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
