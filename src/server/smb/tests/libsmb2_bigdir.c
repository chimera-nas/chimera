// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

#include "libsmb2_test_common.h"


#define TEST_DIR     "testdir"
#define TEST_CONTENT "Hello from libsmb2 test program!"

int
main(
    int    argc,
    char **argv)
{
    //struct smb2fh  *fd;
    struct smb2dir *dir;
    //struct smb2dirent *dirent;
    int             rc;
    struct test_env env;

    //char            name[80];
    //int             i;

    libsmb2_test_init(&env, argv, argc);

    rc = smb2_mkdir(env.ctx, TEST_DIR);

    if (rc < 0) {
        fprintf(stderr, "Failed to create directory: %s\n", smb2_get_error(env.ctx));
        libsmb2_test_fail(&env);
    }

#if 0
    for (i = 0; i < 10000; i++) {
        snprintf(name, sizeof(name), "testdir\\test%d.txt", i);

        fd = smb2_open(env.ctx, name, O_WRONLY | O_CREAT);

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

        rc = smb2_close(env.ctx, fd);

        if (rc < 0) {
            fprintf(stderr, "Failed to close file: %s\n", smb2_get_error(env.ctx));
            libsmb2_test_fail(&env);
        }
    }
        #endif /* if 0 */

    dir = smb2_opendir(env.ctx, TEST_DIR);

    if (!dir) {
        fprintf(stderr, "Failed to open directory: %s\n", smb2_get_error(env.ctx));
        libsmb2_test_fail(&env);
    }

    #if 0
    i = 0;
    while ((dirent = smb2_readdir(env.ctx, dir)) != NULL) {
        printf("dirent: %s\n", dirent->name);
        i++;
    }

    if (i != 10000) {
        fprintf(stderr, "Expected 10000 dirents, got %d\n", i);
        libsmb2_test_fail(&env);
    }
    #endif /* if 0 */

    smb2_closedir(env.ctx, dir);

    libsmb2_test_success(&env);

    return 0;
} /* main */
