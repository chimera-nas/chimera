// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
// SPDX-FileCopyrightText: 2023 SUSE Linux Products GmbH
//
// SPDX-License-Identifier: GPL-2.0

/*
 * rewinddir-test - Test rewinddir() semantics
 *
 * This test verifies that after a rewinddir() call, readdir() returns
 * files that were created after the initial opendir() call.
 * This is required by POSIX.
 *
 * Originally from xfstests, ported to Chimera POSIX userspace API.
 */

#include "posix_test_common.h"
#include <dirent.h>

/*
 * Number of files we add to the test directory after calling opendir()
 * and before calling rewinddir().
 */
#define NUM_FILES 1000

int
main(
    int   argc,
    char *argv[])
{
    struct posix_test_env env;
    int                   file_counters[NUM_FILES] = { 0 };
    int                   dot_count                = 0;
    int                   dot_dot_count            = 0;
    struct dirent        *entry;
    CHIMERA_DIR          *dir      = NULL;
    char                 *dir_path = NULL;
    char                  file_path[512];
    int                   ret = 0;
    int                   i;
    int                   fd;

    posix_test_init(&env, argv, argc);

    if (posix_test_mount(&env) != 0) {
        fprintf(stderr, "Failed to mount test filesystem\n");
        posix_test_fail(&env);
    }

    fprintf(stderr, "Testing rewinddir() semantics on backend: %s\n", env.backend);

    dir_path = "/test/rewinddir_testdir";

    /* Create test directory */
    ret = chimera_posix_mkdir(dir_path, 0700);
    if (ret == -1) {
        fprintf(stderr, "Failed to create test directory: %s\n", strerror(errno));
        ret = errno;
        goto out;
    }

    /* Open the directory first, before creating any files */
    dir = chimera_posix_opendir(dir_path);
    if (dir == NULL) {
        fprintf(stderr, "Failed to open directory: %s\n", strerror(errno));
        ret = errno;
        goto out;
    }

    fprintf(stderr, "Creating %d files after opendir()...\n", NUM_FILES);

    /*
     * Now create all files inside the directory.
     * File names go from 1 to NUM_FILES, 0 is unused as it's the return
     * value for atoi() when an error happens.
     */
    for (i = 1; i <= NUM_FILES; i++) {
        snprintf(file_path, sizeof(file_path), "%s/%d", dir_path, i);
        fd = chimera_posix_open(file_path, O_CREAT | O_WRONLY, 0644);
        if (fd < 0) {
            fprintf(stderr, "Failed to create file number %d: %s\n", i, strerror(errno));
            ret = errno;
            goto out;
        }
        chimera_posix_close(fd);
    }

    fprintf(stderr, "Calling rewinddir()...\n");

    /*
     * Rewind the directory and read it.
     * POSIX requires that after a rewind, any new names added to the
     * directory after the opendir() call and before the rewinddir()
     * call, must be returned by readdir() calls.
     */
    chimera_posix_rewinddir(dir);

    fprintf(stderr, "Reading directory entries...\n");

    /*
     * readdir() returns NULL when it reaches the end of the directory or
     * when an error happens, so reset errno to 0 to distinguish between
     * both cases later.
     */
    errno = 0;
    while ((entry = chimera_posix_readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0) {
            dot_count++;
            continue;
        }
        if (strcmp(entry->d_name, "..") == 0) {
            dot_dot_count++;
            continue;
        }
        i = atoi(entry->d_name);
        if (i == 0) {
            fprintf(stderr, "Failed to parse name '%s' to integer\n", entry->d_name);
            ret = EINVAL;
            goto out;
        }
        if (i < 1 || i > NUM_FILES) {
            fprintf(stderr, "Unexpected file name '%s' (number %d out of range)\n",
                    entry->d_name, i);
            ret = EINVAL;
            goto out;
        }
        /* File names go from 1 to NUM_FILES, so subtract 1 for array index */
        file_counters[i - 1]++;
    }

    if (errno) {
        fprintf(stderr, "Failed to read directory: %s\n", strerror(errno));
        ret = errno;
        goto out;
    }

    /*
     * Now check that the readdir() calls returned every single file name
     * and without repeating any of them. If any name is missing or
     * repeated, don't exit immediately, so that we print a message for
     * all missing or repeated names.
     */
    fprintf(stderr, "Verifying file counts...\n");

    for (i = 0; i < NUM_FILES; i++) {
        if (file_counters[i] != 1) {
            fprintf(stderr, "ERROR: File name %d appeared %d times (expected 1)\n",
                    i + 1, file_counters[i]);
            ret = EINVAL;
        }
    }

    if (dot_count != 1) {
        fprintf(stderr, "ERROR: File name . appeared %d times (expected 1)\n", dot_count);
        ret = EINVAL;
    }
    if (dot_dot_count != 1) {
        fprintf(stderr, "ERROR: File name .. appeared %d times (expected 1)\n", dot_dot_count);
        ret = EINVAL;
    }

    if (ret == 0) {
        fprintf(stderr, "SUCCESS: All %d files found exactly once after rewinddir()\n", NUM_FILES);
    }

 out:
    if (dir != NULL) {
        chimera_posix_closedir(dir);
    }

    /* Cleanup: remove all created files */
    if (dir_path) {
        for (i = 1; i <= NUM_FILES; i++) {
            snprintf(file_path, sizeof(file_path), "%s/%d", dir_path, i);
            chimera_posix_unlink(file_path);
        }
        chimera_posix_rmdir(dir_path);
    }

    posix_test_umount();

    if (ret != 0) {
        posix_test_fail(&env);
    }

    posix_test_success(&env);
    return ret;
} /* main */
