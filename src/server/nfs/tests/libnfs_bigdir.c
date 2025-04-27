#include "libnfs_test_common.h"
#include <stdlib.h>
#include <string.h>

#define NUM_FILES 10000
#define DIR_NAME  "/bigdir"

int
main(
    int    argc,
    char **argv)
{
    struct test_env   env;
    int               i;
    char              filename[256];
    struct nfsdir    *dir;
    struct nfsdirent *ent;
    int               file_count = 0;
    char             *seen_files;

    libnfs_test_init(&env, argv, argc);

    if (nfs_mount(env.nfs, "127.0.0.1", "/share") < 0) {
        fprintf(stderr, "Failed to mount NFS share: %s\n",
                nfs_get_error(env.nfs));
        libnfs_test_fail(&env);
    }

    // Create test directory
    if (nfs_mkdir(env.nfs, DIR_NAME) < 0) {
        fprintf(stderr, "Failed to create directory: %s\n",
                nfs_get_error(env.nfs));
        libnfs_test_fail(&env);
    }

    // Create a million empty files
    for (i = 0; i < NUM_FILES; i++) {
        struct nfsfh *fh;
        snprintf(filename, sizeof(filename), "%s/file%d", DIR_NAME, i);

        if (nfs_creat(env.nfs, filename, 0644, &fh) < 0) {
            fprintf(stderr, "Failed to create file %s: %s\n",
                    filename, nfs_get_error(env.nfs));
            libnfs_test_fail(&env);
        }
        nfs_close(env.nfs, fh);
    }

    // Allocate bitmap to track seen files
    seen_files = calloc(NUM_FILES, sizeof(char));
    if (seen_files == NULL) {
        fprintf(stderr, "Failed to allocate memory for file tracking\n");
        libnfs_test_fail(&env);
    }

    // Open and read directory
    if (nfs_opendir(env.nfs, DIR_NAME, &dir) < 0) {
        fprintf(stderr, "Failed to open directory: %s\n",
                nfs_get_error(env.nfs));
        free(seen_files);
        libnfs_test_fail(&env);
    }

    // Read all entries
    while ((ent = nfs_readdir(env.nfs, dir)) != NULL) {
        if (strcmp(ent->name, ".") == 0 || strcmp(ent->name, "..") == 0) {
            continue;
        }

        file_count++;

        // Extract file number and mark as seen
        if (sscanf(ent->name, "file%d", &i) == 1 && i >= 0 && i < NUM_FILES) {
            seen_files[i] = 1;
        } else {
            fprintf(stderr, "Invalid filename format: %s\n", ent->name);
            nfs_closedir(env.nfs, dir);
            free(seen_files);
            libnfs_test_fail(&env);
        }
    }

    nfs_closedir(env.nfs, dir);

    // Verify we saw exactly NUM_FILES files
    if (file_count != NUM_FILES) {
        fprintf(stderr, "Wrong number of files. Expected %d, got %d\n",
                NUM_FILES, file_count);
        free(seen_files);
        libnfs_test_fail(&env);
    }

    // Verify we saw every file exactly once
    for (i = 0; i < NUM_FILES; i++) {
        if (!seen_files[i]) {
            fprintf(stderr, "Missing file%d in directory listing\n", i);
            free(seen_files);
            libnfs_test_fail(&env);
        }
    }

    free(seen_files);
    nfs_umount(env.nfs);
    libnfs_test_success(&env);
} /* main */