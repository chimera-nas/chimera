// SPDX-FileCopyrightText: 2025 Chimera-NAS Project Contributors
//
// SPDX-License-Identifier: LGPL-2.1-only

// Test telldir and seekdir
// Based on cthon/special/telldir.c from Connectathon 2004
//
// Creates files, walks directory with telldir, then uses seekdir
// to verify cookies work correctly

#include "cthon_common.h"

static int debug = 0;
static int numfiles = 200;
static char *tdirname = "telldir-test";

typedef struct {
    int  inuse;
    long cookie;
    int  numfiles;
} file_info_t;

static file_info_t *file_info;

static void
alloc_file_info(int nfiles)
{
    file_info = calloc(nfiles, sizeof(file_info_t));
    if (file_info == NULL) {
        fprintf(stderr, "can't allocate file info array: %s\n", strerror(errno));
        exit(1);
    }
}

static void
save_file_info(int filenum, long cookie, int files_left)
{
    if (debug) {
        printf("\t%d 0x%lx %d\n", filenum, cookie, files_left);
    }

    file_info[filenum].inuse = 1;
    file_info[filenum].cookie = cookie;
    file_info[filenum].numfiles = files_left;
}

int
main(int argc, char **argv)
{
    struct posix_test_env env;
    int                   rc;
    int                   opt;
    char                  str[MAXPATHLEN];
    char                  filename[MAXPATHLEN];
    int                   i, fd;
    CHIMERA_DIR          *dp;
    struct dirent        *entry;
    int                   files_left;
    long                  cookie;

    cthon_Myname = "cthon_special_telldir";

    posix_test_init(&env, argv, argc);

    optind = 1;
    while ((opt = getopt(argc, argv, "hdb:n:")) != -1) {
        switch (opt) {
            case 'd': debug = 1; break;
            case 'b': break;
            case 'n': numfiles = atoi(optarg); break;
            default: break;
        }
    }

    rc = posix_test_mount(&env);
    if (rc != 0) {
        fprintf(stderr, "Failed to mount: %s\n", strerror(errno));
        posix_test_fail(&env);
    }

    cthon_testdir(NULL);

    fprintf(stdout, "%s: telldir/seekdir test (%d files)\n", cthon_Myname, numfiles);

    alloc_file_info(numfiles);

    // Create test directory
    snprintf(str, sizeof(str), "%s/%s", cthon_getcwd(), tdirname);
    if (chimera_posix_mkdir(str, 0777) < 0) {
        if (errno != EEXIST) {
            cthon_error("can't create %s", str);
            posix_test_fail(&env);
        }
    }

    // Create scratch files
    fprintf(stdout, "\tCreating %d files...\n", numfiles);
    for (i = 0; i < numfiles; i++) {
        snprintf(filename, sizeof(filename), "%s/%d", str, i);
        fd = chimera_posix_open(filename, O_CREAT | O_WRONLY, 0666);
        if (fd < 0) {
            cthon_error("can't create %s", filename);
            posix_test_fail(&env);
        }
        chimera_posix_close(fd);
    }

    // Open the directory
    dp = chimera_posix_opendir(str);
    if (dp == NULL) {
        cthon_error("can't open %s", str);
        posix_test_fail(&env);
    }

    // Walk directory and save telldir info
    fprintf(stdout, "\tWalking directory with telldir...\n");
    files_left = numfiles;
    while (files_left > 0) {
        int filenum;
        char *fname;

        cookie = chimera_posix_telldir(dp);
        if (cookie == -1) {
            fprintf(stderr, "\twarning: cookie = -1, errno=%d (%s)\n",
                    errno, strerror(errno));
        }

        errno = 0;
        entry = chimera_posix_readdir(dp);
        if (entry == NULL) {
            cthon_error("error reading %s: %s", str,
                        errno != 0 ? strerror(errno) : "premature EOF");
            chimera_posix_closedir(dp);
            posix_test_fail(&env);
        }

        fname = entry->d_name;
        if (strcmp(fname, ".") == 0 || strcmp(fname, "..") == 0)
            continue;

        filenum = atoi(fname);
        if (filenum < 0 || filenum >= numfiles) {
            fprintf(stderr, "\tWarning: unexpected filename: %s\n", fname);
            continue;
        }

        save_file_info(filenum, cookie, files_left);
        files_left--;
    }

    // Verify seekdir works correctly
    fprintf(stdout, "\tVerifying seekdir...\n");
    for (i = 0; i < numfiles; i++) {
        file_info_t *ip = &file_info[i];
        int files_found;
        int first_file = 1;

        if (!ip->inuse) {
            fprintf(stderr, "\tno information for file %d\n", i);
            chimera_posix_closedir(dp);
            posix_test_fail(&env);
        }

        chimera_posix_seekdir(dp, ip->cookie);

        // Count files starting at this cookie
        for (files_found = 0; files_found < ip->numfiles; files_found++) {
            errno = 0;
            entry = chimera_posix_readdir(dp);
            if (entry == NULL) {
                char *errmsg = errno != 0 ? strerror(errno) : NULL;

                fprintf(stderr, "\tentry for %d (cookie %ld):\n", i, ip->cookie);
                fprintf(stderr, "\texpected to find %d entries, only found %d\n",
                        ip->numfiles, files_found);
                if (errmsg)
                    fprintf(stderr, "\terror: %s\n", errmsg);
                chimera_posix_closedir(dp);
                posix_test_fail(&env);
            }

            if (first_file) {
                int file_read = atoi(entry->d_name);
                if (file_read != i) {
                    fprintf(stderr, "\texpected file %d at cookie %ld, found %s\n",
                            i, ip->cookie, entry->d_name);
                    chimera_posix_closedir(dp);
                    posix_test_fail(&env);
                }
            }
            first_file = 0;
        }
    }

    chimera_posix_closedir(dp);

    // Cleanup
    fprintf(stdout, "\tCleaning up...\n");
    for (i = 0; i < numfiles; i++) {
        snprintf(filename, sizeof(filename), "%s/%d", str, i);
        chimera_posix_unlink(filename);
    }
    chimera_posix_rmdir(str);
    free(file_info);

    fprintf(stdout, "\ttelldir/seekdir test succeeded\n");

    cthon_complete();
    posix_test_umount();
    posix_test_success(&env);
    return 0;
}
