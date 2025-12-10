// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "libsmb2_test_common.h"


#define TEST_DIR     "testdir"
#define TEST_FILE    "testdir\\test.txt"
#define TEST_CONTENT "Hello from libsmb2 test program!"

int
main(
    int    argc,
    char **argv)
{
    struct smb2fh      *fd;
    struct smb2dir     *dir;
    struct smb2dirent  *dirent;
    int                 rc;
    struct test_env     env;
    uint8_t             buffer[80];
    struct smb2_stat_64 stat;

    libsmb2_test_init(&env, argv, argc);

    rc = smb2_mkdir(env.ctx, TEST_DIR);

    if (rc < 0) {
        fprintf(stderr, "Failed to create directory: %s\n", smb2_get_error(env.ctx));
        libsmb2_test_fail(&env);
    }

    fd = smb2_open(env.ctx, TEST_FILE, O_WRONLY | O_CREAT);

    if (fd < 0) {
        fprintf(stderr, "Failed to open file: %s\n", smb2_get_error(env.ctx));
        libsmb2_test_fail(&env);
    }

    rc = smb2_write(env.ctx, fd, (uint8_t *) TEST_CONTENT, strlen(TEST_CONTENT));

    if (rc < 0) {
        fprintf(stderr, "Failed to write to file: %s\n", smb2_get_error(env.ctx));
        smb2_close(env.ctx, fd);
        libsmb2_test_fail(&env);
    }

    smb2_lseek(env.ctx, fd, 0, SEEK_SET, NULL);

    rc = smb2_read(env.ctx, fd, buffer, strlen(TEST_CONTENT));

    if (rc < 0) {
        fprintf(stderr, "Failed to read from file: %s\n", smb2_get_error(env.ctx));
        smb2_close(env.ctx, fd);
        libsmb2_test_fail(&env);
    }

    if (memcmp(buffer, TEST_CONTENT, strlen(TEST_CONTENT)) != 0) {
        fprintf(stderr, "Read content does not match written content\n");
        smb2_close(env.ctx, fd);
        libsmb2_test_fail(&env);
    }

    rc = smb2_fstat(env.ctx, fd, &stat);

    if (rc < 0) {
        fprintf(stderr, "Failed to stat file: %s\n", smb2_get_error(env.ctx));
        libsmb2_test_fail(&env);
    }

    rc = smb2_close(env.ctx, fd);

    dir = smb2_opendir(env.ctx, TEST_DIR);

    if (!dir) {
        fprintf(stderr, "Failed to open directory: %s\n", smb2_get_error(env.ctx));
        libsmb2_test_fail(&env);
    }

    while ((dirent = smb2_readdir(env.ctx, dir)) != NULL) {
        printf("dirent: %s\n", dirent->name);
    }

    smb2_closedir(env.ctx, dir);

    if (rc < 0) {
        fprintf(stderr, "Failed to close file: %s\n", smb2_get_error(env.ctx));
        libsmb2_test_fail(&env);
    }

    rc = smb2_unlink(env.ctx, TEST_FILE);

    if (rc < 0) {
        fprintf(stderr, "Failed to unlink file: %s\n", smb2_get_error(env.ctx));
        libsmb2_test_fail(&env);
    }

    rc = smb2_unlink(env.ctx, "/no/such/thing");

    if (rc == 0) {
        fprintf(stderr, "Unlink of non-existent file succeeded\n");
        libsmb2_test_fail(&env);
    }

    libsmb2_test_success(&env);

    return 0;
} /* main */
