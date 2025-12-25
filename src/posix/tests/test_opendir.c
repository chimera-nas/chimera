// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test for opendir, closedir, readdir, dirfd, rewinddir, seekdir, telldir, scandir

#include <dirent.h>
#include "posix_test_common.h"

static void
test_opendir_closedir(void)
{
    CHIMERA_DIR *dir;
    int          fd;

    fprintf(stderr, "Testing opendir/closedir/dirfd...\n");

    // Create a test directory
    if (chimera_posix_mkdir("/test/testdir", 0755) != 0) {
        fprintf(stderr, "Failed to create test directory: %s\n", strerror(errno));
        exit(1);
    }

    // Open the directory
    dir = chimera_posix_opendir("/test/testdir");
    if (!dir) {
        fprintf(stderr, "opendir failed: %s\n", strerror(errno));
        exit(1);
    }

    // Get the file descriptor
    fd = chimera_posix_dirfd(dir);
    if (fd < 0) {
        fprintf(stderr, "dirfd failed: %s\n", strerror(errno));
        exit(1);
    }

    fprintf(stderr, "Directory opened with fd=%d\n", fd);

    // Close the directory
    if (chimera_posix_closedir(dir) != 0) {
        fprintf(stderr, "closedir failed: %s\n", strerror(errno));
        exit(1);
    }

    // Try to open a non-existent directory
    dir = chimera_posix_opendir("/test/nonexistent");
    if (dir != NULL) {
        fprintf(stderr, "opendir should have failed for non-existent directory\n");
        exit(1);
    }

    fprintf(stderr, "opendir/closedir/dirfd tests passed\n");
} /* test_opendir_closedir */

static void
test_readdir(void)
{
    CHIMERA_DIR   *dir;
    struct dirent *entry;
    int            found_file1 = 0, found_file2 = 0, found_subdir = 0;
    int            count = 0;
    int            fd;

    fprintf(stderr, "Testing readdir...\n");

    // Create some files and subdirectories in testdir
    fd = chimera_posix_open("/test/testdir/file1.txt", O_CREAT | O_WRONLY, 0644);
    if (fd < 0) {
        fprintf(stderr, "Failed to create file1.txt: %s\n", strerror(errno));
        exit(1);
    }
    chimera_posix_close(fd);

    fd = chimera_posix_open("/test/testdir/file2.txt", O_CREAT | O_WRONLY, 0644);
    if (fd < 0) {
        fprintf(stderr, "Failed to create file2.txt: %s\n", strerror(errno));
        exit(1);
    }
    chimera_posix_close(fd);

    if (chimera_posix_mkdir("/test/testdir/subdir", 0755) != 0) {
        fprintf(stderr, "Failed to create subdir: %s\n", strerror(errno));
        exit(1);
    }

    // Open and read directory
    dir = chimera_posix_opendir("/test/testdir");
    if (!dir) {
        fprintf(stderr, "opendir failed: %s\n", strerror(errno));
        exit(1);
    }

    while ((entry = chimera_posix_readdir(dir)) != NULL) {
        fprintf(stderr, "  Entry: %s (ino=%lu)\n", entry->d_name, (unsigned long) entry->d_ino);
        count++;

        if (strcmp(entry->d_name, "file1.txt") == 0) {
            found_file1 = 1;
        } else if (strcmp(entry->d_name, "file2.txt") == 0) {
            found_file2 = 1;
        } else if (strcmp(entry->d_name, "subdir") == 0) {
            found_subdir = 1;
        }
    }

    chimera_posix_closedir(dir);

    if (!found_file1 || !found_file2 || !found_subdir) {
        fprintf(stderr, "readdir missing entries: file1=%d file2=%d subdir=%d\n",
                found_file1, found_file2, found_subdir);
        exit(1);
    }

    fprintf(stderr, "Found %d entries in directory\n", count);
    fprintf(stderr, "readdir tests passed\n");
} /* test_readdir */

static void
test_rewinddir(void)
{
    CHIMERA_DIR   *dir;
    struct dirent *entry;
    int            first_count = 0, second_count = 0;

    fprintf(stderr, "Testing rewinddir...\n");

    dir = chimera_posix_opendir("/test/testdir");
    if (!dir) {
        fprintf(stderr, "opendir failed: %s\n", strerror(errno));
        exit(1);
    }

    // Read all entries
    while ((entry = chimera_posix_readdir(dir)) != NULL) {
        first_count++;
    }

    // Rewind
    chimera_posix_rewinddir(dir);

    // Read all entries again
    while ((entry = chimera_posix_readdir(dir)) != NULL) {
        second_count++;
    }

    chimera_posix_closedir(dir);

    if (first_count != second_count) {
        fprintf(stderr, "rewinddir failed: first=%d second=%d\n", first_count, second_count);
        exit(1);
    }

    fprintf(stderr, "rewinddir tests passed (count=%d)\n", first_count);
} /* test_rewinddir */

static void
test_seekdir_telldir(void)
{
    CHIMERA_DIR   *dir;
    struct dirent *entry;
    long           pos;
    char           saved_name[256];

    fprintf(stderr, "Testing seekdir/telldir...\n");

    dir = chimera_posix_opendir("/test/testdir");
    if (!dir) {
        fprintf(stderr, "opendir failed: %s\n", strerror(errno));
        exit(1);
    }

    // Read first entry and save position
    entry = chimera_posix_readdir(dir);
    if (!entry) {
        fprintf(stderr, "First readdir failed\n");
        exit(1);
    }

    pos = chimera_posix_telldir(dir);
    fprintf(stderr, "After first entry, telldir=%ld\n", pos);

    // Read second entry and save its name
    entry = chimera_posix_readdir(dir);
    if (!entry) {
        fprintf(stderr, "Second readdir failed\n");
        exit(1);
    }
    strncpy(saved_name, entry->d_name, sizeof(saved_name) - 1);
    saved_name[sizeof(saved_name) - 1] = '\0';
    fprintf(stderr, "Second entry: %s\n", saved_name);

    // Seek back to saved position
    chimera_posix_seekdir(dir, pos);

    // Read entry - should be the same as the second entry we saved
    entry = chimera_posix_readdir(dir);
    if (!entry) {
        fprintf(stderr, "readdir after seekdir failed\n");
        exit(1);
    }

    if (strcmp(entry->d_name, saved_name) != 0) {
        fprintf(stderr, "seekdir/telldir mismatch: expected '%s', got '%s'\n",
                saved_name, entry->d_name);
        exit(1);
    }

    chimera_posix_closedir(dir);

    fprintf(stderr, "seekdir/telldir tests passed\n");
} /* test_seekdir_telldir */

static int
filter_txt_files(const struct dirent *entry)
{
    const char *name = entry->d_name;
    size_t      len  = strlen(name);

    if (len > 4 && strcmp(name + len - 4, ".txt") == 0) {
        return 1;
    }
    return 0;
} /* filter_txt_files */

static int
compare_entries(
    const struct dirent **a,
    const struct dirent **b)
{
    return strcmp((*a)->d_name, (*b)->d_name);
} /* compare_entries */

static void
test_scandir(void)
{
    struct dirent **namelist;
    int             n;

    fprintf(stderr, "Testing scandir...\n");

    // Scan with no filter, sorted
    n = chimera_posix_scandir("/test/testdir", &namelist, NULL, compare_entries);
    if (n < 0) {
        fprintf(stderr, "scandir failed: %s\n", strerror(errno));
        exit(1);
    }

    fprintf(stderr, "scandir found %d entries:\n", n);
    for (int i = 0; i < n; i++) {
        fprintf(stderr, "  %s\n", namelist[i]->d_name);
        free(namelist[i]);
    }
    free(namelist);

    // Scan with filter for .txt files
    n = chimera_posix_scandir("/test/testdir", &namelist, filter_txt_files, compare_entries);
    if (n < 0) {
        fprintf(stderr, "scandir with filter failed: %s\n", strerror(errno));
        exit(1);
    }

    fprintf(stderr, "scandir with .txt filter found %d entries:\n", n);
    if (n != 2) {
        fprintf(stderr, "Expected 2 .txt files, got %d\n", n);
        exit(1);
    }

    // Verify sorted order
    if (strcmp(namelist[0]->d_name, "file1.txt") != 0 ||
        strcmp(namelist[1]->d_name, "file2.txt") != 0) {
        fprintf(stderr, "scandir entries not in expected order\n");
        exit(1);
    }

    for (int i = 0; i < n; i++) {
        fprintf(stderr, "  %s\n", namelist[i]->d_name);
        free(namelist[i]);
    }
    free(namelist);

    fprintf(stderr, "scandir tests passed\n");
} /* test_scandir */

int
main(
    int    argc,
    char **argv)
{
    struct posix_test_env env;
    int                   rc;

    posix_test_init(&env, argv, argc);

    rc = posix_test_mount(&env);

    if (rc != 0) {
        fprintf(stderr, "Failed to mount test module: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    // Run all tests
    test_opendir_closedir();
    test_readdir();
    test_rewinddir();
    test_seekdir_telldir();
    test_scandir();

    fprintf(stderr, "All directory operation tests passed!\n");

    rc = posix_test_umount();

    if (rc != 0) {
        fprintf(stderr, "Failed to unmount /test: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    posix_test_success(&env);

    return 0;
} /* main */
