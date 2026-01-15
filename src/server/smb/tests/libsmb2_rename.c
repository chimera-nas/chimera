// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "libsmb2_test_common.h"


#define TEST_DIR     "testdir"
#define TEST_DIR2    "testdir2"
#define TEST_FILE    "testdir\\test.txt"
#define TEST_RENAME1 "testdir\\rename1.txt"
#define TEST_RENAME2 "rename2.txt"
#define TEST_RENAME3 "testdir\\rename2.txt"
#define TEST_FILE2   "testdir\\file2.txt"
#define TEST_FILE3   "testdir\\file3.txt"
#define TEST_FILE4   "file4.txt"
#define TEST_FILE5   "testdir2\\file5.txt"

int
main(
    int    argc,
    char **argv)
{
    struct smb2fh  *fd;
    int             rc;
    struct test_env env;

    libsmb2_test_init(&env, argv, argc);

    /* Setup: Create test directory and initial file */
    rc = smb2_mkdir(env.ctx, TEST_DIR);
    if (rc < 0) {
        fprintf(stderr, "Failed to create directory: %s\n", smb2_get_error(env.ctx));
        libsmb2_test_fail(&env);
    }

    fd = smb2_open(env.ctx, TEST_FILE, O_WRONLY | O_CREAT);
    if (!fd) {
        fprintf(stderr, "Failed to create test file: %s\n", smb2_get_error(env.ctx));
        libsmb2_test_fail(&env);
    }
    smb2_close(env.ctx, fd);

    /* Test 1: Rename to non-existent directory should fail */
    rc = smb2_rename(env.ctx, TEST_FILE, "bogus\\path");
    if (rc == 0) {
        fprintf(stderr, "Rename to non-existent directory succeeded unexpectedly\n");
        libsmb2_test_fail(&env);
    }

    /* Test 2: Simple rename within same directory */
    rc = smb2_rename(env.ctx, TEST_FILE, TEST_RENAME1);
    if (rc < 0) {
        fprintf(stderr, "Failed to rename within directory: %s\n", smb2_get_error(env.ctx));
        libsmb2_test_fail(&env);
    }

    /* Verify file exists at new location */
    fd = smb2_open(env.ctx, TEST_RENAME1, O_RDONLY);
    if (!fd) {
        fprintf(stderr, "File not found after rename: %s\n", smb2_get_error(env.ctx));
        libsmb2_test_fail(&env);
    }
    smb2_close(env.ctx, fd);

    /* Test 3: Cross-directory rename (move to root) */
    rc = smb2_rename(env.ctx, TEST_RENAME1, TEST_RENAME2);
    if (rc < 0) {
        fprintf(stderr, "Failed to rename across directories: %s\n", smb2_get_error(env.ctx));
        libsmb2_test_fail(&env);
    }

    /* Verify file moved to root */
    fd = smb2_open(env.ctx, TEST_RENAME2, O_RDONLY);
    if (!fd) {
        fprintf(stderr, "File not found in new directory: %s\n", smb2_get_error(env.ctx));
        libsmb2_test_fail(&env);
    }
    smb2_close(env.ctx, fd);

    /* Test 4: Move file into existing directory using directory name as destination */
    rc = smb2_rename(env.ctx, TEST_RENAME2, TEST_DIR);
    if (rc < 0) {
        fprintf(stderr, "Failed to move file into directory: %s\n", smb2_get_error(env.ctx));
        libsmb2_test_fail(&env);
    }

    /* Verify it ended up as testdir/rename2.txt */
    fd = smb2_open(env.ctx, TEST_RENAME3, O_RDONLY);
    if (!fd) {
        fprintf(stderr, "File not found in target directory: %s\n", smb2_get_error(env.ctx));
        libsmb2_test_fail(&env);
    }
    smb2_close(env.ctx, fd);

    /* Test 5: Collision detection - rename to existing file without replace flag */
    /* Create two files */
    fd = smb2_open(env.ctx, TEST_FILE2, O_WRONLY | O_CREAT);
    if (!fd) {
        fprintf(stderr, "Failed to create file2: %s\n", smb2_get_error(env.ctx));
        libsmb2_test_fail(&env);
    }
    smb2_close(env.ctx, fd);

    fd = smb2_open(env.ctx, TEST_FILE3, O_WRONLY | O_CREAT);
    if (!fd) {
        fprintf(stderr, "Failed to create file3: %s\n", smb2_get_error(env.ctx));
        libsmb2_test_fail(&env);
    }
    smb2_close(env.ctx, fd);

    /* Try to rename file2 to file3 - should fail due to collision */
    rc = smb2_rename(env.ctx, TEST_FILE2, TEST_FILE3);
    if (rc == 0) {
        fprintf(stderr, "Rename to existing file succeeded unexpectedly (should fail)\n");
        libsmb2_test_fail(&env);
    }

    /* Cleanup */
    smb2_unlink(env.ctx, TEST_RENAME3);  /* File is now in testdir after test 4 */
    smb2_unlink(env.ctx, TEST_FILE2);
    smb2_unlink(env.ctx, TEST_FILE3);
    smb2_rmdir(env.ctx, TEST_DIR);

    libsmb2_test_success(&env);

    return 0;
} /* main */
